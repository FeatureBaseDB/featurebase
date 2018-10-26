package udproxy

import (
	"bytes"
	"io"
	"net"
	"sync"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

type Proxy struct {
	upstreamAddr *net.UDPAddr
	conn         *net.UDPConn

	drop     bool
	dropLock sync.Mutex

	quit chan struct{}
	eg   errgroup.Group
	// map from client address to upstream connection. We must maintain a
	// separate connection to upstream for each client connection so that we can
	// differentiate data sent back from upstream.
	upstreams map[*net.UDPAddr]*net.UDPConn
	// TODO - need to track a per-connection timeout so that "upstreams" doesn't
	// grow indefinitely.
}

func New(listenIP string, listenPort int, upstreamIP string, upstreamPort int) (*Proxy, error) {
	uc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP(listenIP), Port: listenPort})
	if err != nil {
		return nil, errors.Wrap(err, "listening")
	}
	p := &Proxy{
		conn:         uc,
		upstreamAddr: &net.UDPAddr{IP: net.ParseIP(upstreamIP), Port: upstreamPort},
		quit:         make(chan struct{}),
		upstreams:    make(map[*net.UDPAddr]*net.UDPConn),
	}
	if p.upstreamAddr.IP == nil {
		return nil, errors.Errorf("unable to parse upstream ip '%s'", upstreamIP)
	}
	p.eg.Go(p.run)
	return p, nil
}

func (p *Proxy) Drop() {
	// sleep to attempt to ensure all traffic that was supposed to pass, did
	// pass.
	time.Sleep(time.Millisecond * 3)
	p.dropLock.Lock()
	p.drop = true
	p.dropLock.Unlock()
}

func (p *Proxy) Undrop() {
	// this sleep is a cheap attempt to ensure everything that was supposed to
	// be dropped was.
	time.Sleep(time.Millisecond * 3)
	p.dropLock.Lock()
	p.drop = false
	p.dropLock.Unlock()
}

func (p *Proxy) dropping() bool {
	p.dropLock.Lock()
	d := p.drop
	p.dropLock.Unlock()
	return d
}

func (p *Proxy) run() error {
	buf := make([]byte, 65507)
	for {
		select {
		case <-p.quit:
			return nil
		default:
		}
		err := p.conn.SetReadDeadline(time.Now().Add(time.Millisecond))
		if err != nil {
			return errors.Wrap(err, "setting read deadline (run)")
		}
		n, addr, err := p.conn.ReadFromUDP(buf)
		if err, ok := err.(net.Error); ok && err.Timeout() {
			continue
		} else if err != nil {
			return errors.Wrap(err, "reading from udp conn")
		}
		upConn := p.upstreams[addr]
		if upConn == nil {
			p.upstreams[addr], err = net.DialUDP("udp", &net.UDPAddr{}, p.upstreamAddr)
			if err != nil {
				return errors.Wrap(err, "creating new connection to upstream")
			}
			p.eg.Go(func() error {
				return p.proxyBack(addr, p.upstreams[addr])
			})
			upConn = p.upstreams[addr]
		}
		if !p.dropping() {
			_, err = io.Copy(upConn, bytes.NewBuffer(buf[:n]))
			if err != nil {
				return errors.Wrap(err, "writing to upstream conn")
			}
		}
	}
}

func (p *Proxy) proxyBack(to *net.UDPAddr, from *net.UDPConn) error {
	buf := make([]byte, 65507)
	for {
		select {
		case <-p.quit:
			return nil
		default:
		}
		err := from.SetReadDeadline(time.Now().Add(time.Millisecond))
		if err != nil {
			return errors.Wrap(err, "setting read deadline (proxyBack)")
		}
		n, _, err := from.ReadFromUDP(buf)
		if err, ok := err.(net.Error); ok && err.Timeout() {
			continue
		} else if err != nil {
			return errors.Wrap(err, "reading from upstream")
		}
		if !p.dropping() {
			_, err = io.Copy(addrWriter{c: p.conn, a: to}, bytes.NewBuffer(buf[:n]))
			if err != nil {
				return errors.Wrap(err, "writing back to client")
			}
		}
	}
}

type addrWriter struct {
	c *net.UDPConn
	a *net.UDPAddr
}

func (a addrWriter) Write(b []byte) (n int, err error) {
	return a.c.WriteTo(b, a.a)
}

func (p *Proxy) Close() error {
	close(p.quit)
	return p.eg.Wait()
}
