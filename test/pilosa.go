package test

import (
	"bytes"
	"io/ioutil"
	"net"
	"strconv"
	"testing"

	"github.com/pilosa/pilosa/server"
)

func MustNewRunningServer(t *testing.T) *server.Command {
	s := server.NewCommand(&bytes.Buffer{}, ioutil.Discard, ioutil.Discard)
	s.Config.Bind = ":0"
	port := strconv.Itoa(MustOpenPort(t))
	s.Config.GossipPort = port
	s.Config.GossipSeed = "localhost:" + port
	td, err := ioutil.TempDir("", "")
	if err != nil {
		t.Fatalf("error creating temp data directory: %v", err)
	}
	s.Config.DataDir = td
	err = s.Run()
	if err != nil {
		t.Fatalf("error running new pilosa server: %v", err)
	}
	return s
}

func MustOpenPort(t *testing.T) int {
	addr, err := net.ResolveTCPAddr("tcp", ":0")
	if err != nil {
		t.Fatalf("resolving new port addr: %v", err)
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		t.Fatalf("listening to get new port: %v", err)
	}
	defer func() {
		err := l.Close()
		if err != nil {
			t.Logf("error closing listener in MustOpenPort: %v", err)
		}
	}()
	return l.Addr().(*net.TCPAddr).Port
}
