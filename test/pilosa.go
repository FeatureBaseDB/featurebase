package test

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/gossip"
	"github.com/pilosa/pilosa/server"
	"github.com/pkg/errors"
)

////////////////////////////////////////////////////////////////////////////////////
// Main represents a test wrapper for main.Main.
type Main struct {
	*server.Command

	Stdin  bytes.Buffer
	Stdout bytes.Buffer
	Stderr bytes.Buffer
}

// NewMain returns a new instance of Main with a temporary data directory and random port.
func NewMain() *Main {
	path, err := ioutil.TempDir("", "pilosa-")
	if err != nil {
		panic(err)
	}

	m := &Main{Command: server.NewCommand(os.Stdin, os.Stdout, os.Stderr)}
	m.Server.Network = *Network
	m.Config.DataDir = path
	m.Config.Bind = "localhost:0"
	m.Config.Cluster.Type = "static"
	m.Command.Stdin = &m.Stdin
	m.Command.Stdout = &m.Stdout
	m.Command.Stderr = &m.Stderr

	if testing.Verbose() {
		m.Command.Stdout = io.MultiWriter(os.Stdout, m.Command.Stdout)
		m.Command.Stderr = io.MultiWriter(os.Stderr, m.Command.Stderr)
	}

	return m
}

// Close closes the program and removes the underlying data directory.
func (m *Main) Close() error {
	defer os.RemoveAll(m.Config.DataDir)
	return m.Command.Close()
}

// RunWithTransport runs Main and returns the dynamically allocated gossip port.
func (m *Main) RunWithTransport(host string, bindPort int, joinSeed string, coordinator *pilosa.URI) (seed string, coord pilosa.URI, err error) {
	defer close(m.Started)

	m.Config.Cluster.Type = "gossip"

	/*
	   TEST:
	   - SetupServer (just static settings from config)
	   - OpenListener (sets Server.Name to use in gossip)
	   - NewTransport (gossip)
	   - SetupNetworking (does the gossip or static stuff) - uses Server.Name
	   - Open server

	   PRODUCTION:
	   - SetupServer (just static settings from config)
	   - SetupNetworking (does the gossip or static stuff) - calls NewTransport
	   - Open server - calls OpenListener
	*/

	// SetupServer
	err = m.SetupServer()
	if err != nil {
		return seed, coord, err
	}

	// Open server listener.
	// This is used to set Server.Name, which is used as the node
	// name for identifying a memberlist node.
	err = m.Server.OpenListener()
	if err != nil {
		return seed, coord, err
	}

	// Open gossip transport to use in SetupServer.
	transport, err := gossip.NewTransport(host, bindPort)
	if err != nil {
		return seed, coord, err
	}
	m.GossipTransport = transport

	if joinSeed != "" {
		m.Config.Gossip.Seed = joinSeed
	} else {
		m.Config.Gossip.Seed = transport.URI.String()
	}
	seed = m.Config.Gossip.Seed

	// SetupNetworking
	err = m.SetupNetworking()
	if err != nil {
		return seed, coord, err
	}

	if err = m.Server.BroadcastReceiver.Start(m.Server); err != nil {
		return seed, coord, err
	}

	if coordinator != nil {
		coord = *coordinator
	} else {
		coord = m.Server.URI
	}
	m.Server.Cluster.Coordinator = coord
	m.Server.Cluster.Static = false

	// Initialize server.
	err = m.Server.Open()
	if err != nil {
		return seed, coord, err
	}

	return seed, coord, nil
}

////////////////////////////////////////////////////////////////////////////////////

type Cluster struct {
	Servers []*Main
}

func MustNewServerCluster(t *testing.T, size int) *Cluster {
	cluster, err := NewServerCluster(size)
	if err != nil {
		t.Fatalf("new cluster: %v", err)
	}
	return cluster
}

func NewServerCluster(size int) (cluster *Cluster, err error) {
	if size == 0 {
		return nil, errors.New("cluster must contain at least one node")
	}

	cluster = &Cluster{
		Servers: make([]*Main, size),
	}

	gossipHost := "localhost"
	gossipPort := 0
	var gossipSeed string
	var coordinator pilosa.URI

	for i := 0; i < size; i++ {
		m := NewMain()

		gossipSeed, coordinator, err = m.RunWithTransport(gossipHost, gossipPort, gossipSeed, &coordinator)
		if err != nil {
			return nil, errors.Wrap(err, "RunWithTransport")
		}

		cluster.Servers[i] = m
	}

	return cluster, nil
}
