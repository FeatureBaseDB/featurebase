package udproxy_test

import (
	"net"
	"testing"

	"github.com/pilosa/pilosa/internal/udproxy"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

func TestUDProxy(t *testing.T) {
	p, err := udproxy.New("127.0.0.1", 12345, "127.0.0.1", 12346)
	if err != nil {
		t.Fatalf("creating proxy: %v", err)
	}

	uc, err := net.ListenUDP("udp", &net.UDPAddr{Port: 12346})
	if err != nil {
		t.Fatalf("listening udp upstream: %v", err)
	}

	resp := make([]byte, 8)
	eg := errgroup.Group{}
	eg.Go(func() error {
		conn, err := net.DialUDP("udp", nil, &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 12345})
		if err != nil {
			t.Fatalf("connecting to proxy: %v", err)
		}
		_, err = conn.Write([]byte("hello!"))
		if err != nil {
			return errors.Wrap(err, "writing to proxy")
		}
		_, err = conn.Read(resp)
		if err != nil {
			return errors.Wrap(err, "reading from proxy")
		}
		p.Drop()
		_, err = conn.Write([]byte("hello2"))
		if err != nil {
			return errors.Wrap(err, "writing to dropping proxy")
		}
		p.Undrop()
		_, err = conn.Write([]byte("hello3"))
		if err != nil {
			return errors.Wrap(err, "writing to undropping proxy")
		}
		return nil
	})

	req := make([]byte, 10)
	_, addr, err := uc.ReadFrom(req)
	if err != nil {
		t.Fatalf("upstream reading from proxy: %v", err)
	}
	if string(req[:6]) != "hello!" {
		t.Fatalf("got unexpected request %s", req)
	}
	_, err = uc.WriteTo([]byte("goodbye"), addr)
	if err != nil {
		t.Fatalf("writing response: %v", err)
	}

	_, _, err = uc.ReadFrom(req)
	if err != nil {
		t.Fatalf("upstream reading from proxy: %v", err)
	}
	if string(req[:6]) != "hello3" {
		t.Fatalf("got unexpected request %s", req)
	}

	eg.Wait()
	if string(resp[:7]) != "goodbye" {
		t.Fatalf("got unexpected response '%v", resp)
	}

	err = p.Close()
	if err != nil {
		t.Fatalf("err closing proxy: '%v'", err)
	}
}

// TODO test dropping
