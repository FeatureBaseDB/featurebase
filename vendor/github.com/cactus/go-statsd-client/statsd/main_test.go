package statsd

import (
	"bytes"
	"log"
	"net"
	"reflect"
	"testing"
	"time"
)

var statsdPacketTests = []struct {
	Prefix   string
	Method   string
	Stat     string
	Value    int64
	Rate     float32
	Expected string
}{
	{"test", "Gauge", "gauge", 1, 1.0, "test.gauge:1|g"},
	{"test", "Inc", "count", 1, 0.999999, "test.count:1|c|@0.999999"},
	{"test", "Inc", "count", 1, 1.0, "test.count:1|c"},
	{"test", "Dec", "count", 1, 1.0, "test.count:-1|c"},
	{"test", "Timing", "timing", 1, 1.0, "test.timing:1|ms"},
	{"", "Inc", "count", 1, 1.0, "count:1|c"},
	{"", "GaugeDelta", "gauge", 1, 1.0, "gauge:+1|g"},
	{"", "GaugeDelta", "gauge", -1, 1.0, "gauge:-1|g"},
}

func TestClient(t *testing.T) {
	l, err := newUDPListener("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	for _, tt := range statsdPacketTests {
		c, err := New(l.LocalAddr().String(), tt.Prefix)
		if err != nil {
			t.Fatal(err)
		}
		method := reflect.ValueOf(c).MethodByName(tt.Method)
		e := method.Call([]reflect.Value{
			reflect.ValueOf(tt.Stat),
			reflect.ValueOf(tt.Value),
			reflect.ValueOf(tt.Rate)})[0]
		errInter := e.Interface()
		if errInter != nil {
			t.Fatal(errInter.(error))
		}

		data := make([]byte, 128)
		_, _, err = l.ReadFrom(data)
		if err != nil {
			c.Close()
			t.Fatal(err)
		}

		data = bytes.TrimRight(data, "\x00")
		if bytes.Equal(data, []byte(tt.Expected)) != true {
			c.Close()
			t.Fatalf("%s got '%s' expected '%s'", tt.Method, data, tt.Expected)
		}
		c.Close()
	}
}

func TestNoopClient(t *testing.T) {
	l, err := newUDPListener("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	for _, tt := range statsdPacketTests {
		c, err := NewNoop(l.LocalAddr().String(), tt.Prefix)
		if err != nil {
			t.Fatal(err)
		}
		method := reflect.ValueOf(c).MethodByName(tt.Method)
		e := method.Call([]reflect.Value{
			reflect.ValueOf(tt.Stat),
			reflect.ValueOf(tt.Value),
			reflect.ValueOf(tt.Rate)})[0]
		errInter := e.Interface()
		if errInter != nil {
			t.Fatal(errInter.(error))
		}

		data := make([]byte, 128)
		n, _, err := l.ReadFrom(data)
		// this is expected to error, since there should
		// be no udp data sent, so the read will time out
		if err == nil || n != 0 {
			c.Close()
			t.Fatal(err)
		}
		c.Close()
	}
}

func newUDPListener(addr string) (*net.UDPConn, error) {
	l, err := net.ListenPacket("udp", addr)
	if err != nil {
		return nil, err
	}
	l.SetDeadline(time.Now().Add(100 * time.Millisecond))
	l.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	l.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
	return l.(*net.UDPConn), nil
}

func ExampleClient() {
	// first create a client
	client, err := Dial("127.0.0.1:8125", "test-client")
	// handle any errors
	if err != nil {
		log.Fatal(err)
	}
	// make sure to clean up
	defer client.Close()

	// Send a stat
	err = client.Inc("stat1", 42, 1.0)
	// handle any errors
	if err != nil {
		log.Printf("Error sending metric: %+v", err)
	}
}

func ExampleNoopClient() {
	// use interface so we can sub noop client if needed
	var client Statter
	var err error

	// first try to create a real client
	client, err = Dial("not-resolvable:8125", "test-client")
	// Lets say real client creation fails, but you don't care enough about
	// stats that you don't want your program to run. Just log an error and
	// make a NoopClient instead
	if err != nil {
		log.Println("Remote endpoint did not resolve. Disabling stats", err)
		client, err = NewNoop()
	}
	// make sure to clean up
	defer client.Close()

	// Send a stat
	err = client.Inc("stat1", 42, 1.0)
	// handle any errors
	if err != nil {
		log.Printf("Error sending metric: %+v", err)
	}
}
