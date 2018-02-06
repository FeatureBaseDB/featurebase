package test

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
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

func NewMainArrayWithCluster(size int) []*Main {
	cluster, err := NewServerCluster(size)
	if err != nil {
		panic(err)
	}
	mainArray := make([]*Main, size)
	for i := 0; i < size; i++ {
		mainArray[i] = cluster.Servers[i]
	}
	return mainArray
}

// MustRunMain returns a new, running Main. Panic on error.
func MustRunMain() *Main {
	m := NewMain()
	m.Config.Metric.Diagnostics = false // Disable diagnostics.
	if err := m.Run(); err != nil {
		panic(err)
	}
	return m
}

// Close closes the program and removes the underlying data directory.
func (m *Main) Close() error {
	defer os.RemoveAll(m.Config.DataDir)
	return m.Command.Close()
}

// Reopen closes the program and reopens it.
func (m *Main) Reopen() error {
	if err := m.Command.Close(); err != nil {
		return err
	}

	// Create new main with the same config.
	config := m.Config
	m.Command = server.NewCommand(os.Stdin, os.Stdout, os.Stderr)
	m.Server.Network = *Network
	m.Config = config

	// Run new program.
	if err := m.Run(); err != nil {
		return err
	}
	return nil
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

// URL returns the base URL string for accessing the running program.
func (m *Main) URL() string { return "http://" + m.Server.Addr().String() }

// Client returns a client to connect to the program.
func (m *Main) Client() *pilosa.InternalHTTPClient {
	client, err := pilosa.NewInternalHTTPClient(m.Server.URI.HostPort(), pilosa.GetHTTPClient(nil))
	if err != nil {
		panic(err)
	}
	return client
}

// Query executes a query against the program through the HTTP API.
func (m *Main) Query(index, rawQuery, query string) (string, error) {
	resp := MustDo("POST", m.URL()+fmt.Sprintf("/index/%s/query?", index)+rawQuery, query)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("invalid status: %d, body=%s", resp.StatusCode, resp.Body)
	}
	return resp.Body, nil
}

// CreateDefinition.
func (m *Main) CreateDefinition(index, def, query string) (string, error) {
	resp := MustDo("POST", m.URL()+fmt.Sprintf("/index/%s/input-definition/%s", index, def), query)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("invalid status: %d, body=%s", resp.StatusCode, resp.Body)
	}
	return resp.Body, nil
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

// MustDo executes http.Do() with an http.NewRequest(). Panic on error.
func MustDo(method, urlStr string, body string) *httpResponse {
	req, err := http.NewRequest(method, urlStr, strings.NewReader(body))
	if err != nil {
		panic(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	buf, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}

	return &httpResponse{Response: resp, Body: string(buf)}
}

// httpResponse is a wrapper for http.Response that holds the Body as a string.
type httpResponse struct {
	*http.Response
	Body string
}
