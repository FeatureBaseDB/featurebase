package test

import (
	"bytes"
	"io/ioutil"
	"net"
	"strconv"
	"testing"

	"github.com/pilosa/pilosa/server"
	"github.com/pkg/errors"
)

func MustNewRunningServer(t *testing.T) *server.Command {
	s, err := newServer()
	if err != nil {
		t.Fatalf("getting new server: %v", err)
	}

	err = s.Run()
	if err != nil {
		t.Fatalf("running new pilosa server: %v", err)
	}
	return s
}

func newServer() (*server.Command, error) {
	s := server.NewCommand(&bytes.Buffer{}, ioutil.Discard, ioutil.Discard)

	port, err := findPort()
	if err != nil {
		return nil, errors.Wrap(err, "getting port")
	}
	s.Config.Bind = "localhost:" + strconv.Itoa(port)

	gport, err := findPort()
	if err != nil {
		return nil, errors.Wrap(err, "getting gossip port")
	}
	s.Config.GossipPort = strconv.Itoa(gport)

	s.Config.GossipSeed = "localhost:" + s.Config.GossipPort
	s.Config.Cluster.Type = "gossip"
	s.Config.Metric.Diagnostics = false
	td, err := ioutil.TempDir("", "")
	if err != nil {
		return nil, errors.Wrap(err, "temp dir")
	}
	s.Config.DataDir = td
	return s, nil
}

func findPort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", ":0")
	if err != nil {
		return 0, errors.Wrap(err, "resolving new port addr")
	}
	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, errors.Wrap(err, "listening to get new port")
	}
	port := l.Addr().(*net.TCPAddr).Port
	err = l.Close()
	if err != nil {
		return port, errors.Wrap(err, "closing listener")
	}
	return port, nil

}

func MustFindPort(t *testing.T) int {
	port, err := findPort()
	if err != nil {
		t.Fatalf("allocating new port: %v", err)
	}
	return port
}

type Cluster struct {
	Servers []*server.Command
}

func MustNewServerCluster(t *testing.T, size int) *Cluster {
	cluster, err := NewServerCluster(size)
	if err != nil {
		t.Fatalf("new cluster: %v", err)
	}
	return cluster
}

func NewServerCluster(size int) (cluster *Cluster, err error) {
	cluster = &Cluster{
		Servers: make([]*server.Command, size),
	}
	hosts := make([]string, size)
	for i := 0; i < size; i++ {
		s, err := newServer()
		if err != nil {
			return nil, errors.Wrap(err, "new server")
		}
		cluster.Servers[i] = s
		hosts[i] = s.Config.Bind
		s.Config.GossipSeed = cluster.Servers[0].Config.GossipSeed

	}

	for _, s := range cluster.Servers {
		s.Config.Cluster.Hosts = hosts
	}
	for i, s := range cluster.Servers {
		err := s.Run()
		if err != nil {
			for j := 0; j <= i; j++ {
				cluster.Servers[j].Close()
			}
			return nil, errors.Wrapf(err, "starting server %d of %d. Config: %#v", i+1, size, s.Config)
		}
	}

	return cluster, nil
}
