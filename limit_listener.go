package pilosa

import (
	"fmt"
	"io"
	"net"
	"time"
)

var (
	ErrorTooManyConnections = errTooManyConnections{}
)

func NewBoundListener(l net.Listener, maxActive int) net.Listener {
	b := &boundListener{l, make(chan struct{}, maxActive)}
	for i := 0; i < maxActive; i++ {
		b.active <- struct{}{}
	}
	return b
}

type boundListener struct {
	net.Listener
	active chan struct{}
}

type boundConn struct {
	net.Conn
	active chan struct{}
}
type errTooManyConnections struct {
}

func (e errTooManyConnections) Error() string   { return "Too many connections" }
func (e errTooManyConnections) Timeout() bool   { return false }
func (e errTooManyConnections) Temporary() bool { return true }

func (l *boundListener) Accept() (net.Conn, error) {
	select {
	case <-l.active:
		c, err := l.Listener.Accept()
		if err != nil {
			l.active <- struct{}{}
			return nil, err
		}
		return &boundConn{c, l.active}, err
	default: //out of connections
		c, _ := l.Listener.Accept()
		l.writeError(c, ErrorTooManyConnections.Error())
		c.Close()
		return nil, ErrorTooManyConnections
	}
}

func (l *boundConn) Close() error {
	err := l.Conn.Close()
	l.active <- struct{}{}
	return err
}
func (l *boundListener) writeError(w io.Writer, msg string) {
	date := time.Now()
	var dst []byte
	dst = date.In(time.UTC).AppendFormat(dst, time.RFC1123)
	copy(dst[len(dst)-3:], "GMT")

	fmt.Fprintf(w, "HTTP/1.1 503 Service Unavailable\r\n"+
		"Connection: close\r\n"+
		"Server: %s\r\n"+
		"Date: %s\r\n"+
		"Content-Type: text/plain\r\n"+
		"Content-Length: %d\r\n"+
		"\r\n"+
		"%s",
		"Pilosa", dst, len(msg), msg)
}
