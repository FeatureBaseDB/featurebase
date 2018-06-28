// Copyright 2017 Pilosa Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package test

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	gohttp "net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/pilosa/pilosa/http"
	"github.com/pilosa/pilosa/server"
	"github.com/pilosa/pilosa/toml"
	"github.com/pkg/errors"
)

////////////////////////////////////////////////////////////////////////////////////
// Main represents a test wrapper for server.Command.
type Main struct {
	*server.Command

	commandOptions []server.CommandOption

	Stdin  bytes.Buffer
	Stdout bytes.Buffer
	Stderr bytes.Buffer
}

func OptAntiEntropyInterval(dur time.Duration) server.CommandOption {
	return func(m *server.Command) error {
		m.Config.AntiEntropy.Interval = toml.Duration(dur)
		return nil
	}
}

func OptAllowedOrigins(origins []string) server.CommandOption {
	return func(m *server.Command) error {
		m.Config.Handler.AllowedOrigins = origins
		return nil
	}
}

// GossipAddress returns the address on which gossip is listening after a Main
// has been setup. Useful to pass as a seed to other nodes when creating and
// testing clusters.
func (m *Main) GossipAddress() string {
	return m.GossipTransport().URI.String()
}

// NewMain returns a new instance of Main with a temporary data directory and random port.
func NewMain(opts ...server.CommandOption) *Main {
	path, err := ioutil.TempDir("", "pilosa-")
	if err != nil {
		panic(err)
	}

	m := &Main{Command: server.NewCommand(os.Stdin, os.Stdout, os.Stderr, opts...), commandOptions: opts}
	m.Config.DataDir = path
	m.Config.Bind = "http://localhost:0"
	m.Config.Cluster.Disabled = true
	m.Command.Stdin = &m.Stdin
	m.Command.Stdout = &m.Stdout
	m.Command.Stderr = &m.Stderr

	err = m.SetupServer()
	if err != nil {
		panic(err)
	}

	if testing.Verbose() {
		m.Command.Stdout = io.MultiWriter(os.Stdout, m.Command.Stdout)
		m.Command.Stderr = io.MultiWriter(os.Stderr, m.Command.Stderr)
	}

	return m
}

// NewMainWithCluster returns a new instance of Main with clustering enabled.
func NewMainWithCluster(isCoordinator bool, opts ...server.CommandOption) *Main {
	m := NewMain(opts...)
	m.Config.Cluster.Disabled = false
	m.Config.Cluster.Coordinator = isCoordinator
	return m
}

// MustRunMainWithCluster ruturns a running array of *Main where
// all nodes are joined via memberlist (i.e. clustering enabled).
func MustRunMainWithCluster(t *testing.T, size int, opts ...[]server.CommandOption) []*Main {
	ma, err := runMainWithCluster(size, opts...)
	if err != nil {
		t.Fatalf("new main array with cluster: %v", err)
	}
	return ma
}

// runMainWithCluster runs an array of *Main where all nodes are
// joined via memberlist (i.e. clustering enabled).
func runMainWithCluster(size int, opts ...[]server.CommandOption) ([]*Main, error) {
	if size == 0 {
		return nil, errors.New("cluster must contain at least one node")
	}
	if len(opts) != size && len(opts) != 0 && len(opts) != 1 {
		return nil, errors.New("Slice of CommandOptions must be of length 0, 1, or equal to the number of cluster nodes")
	}

	mains := make([]*Main, size)
	var gossipSeeds = make([]string, size)
	for i := 0; i < size; i++ {
		var commandOpts []server.CommandOption
		if len(opts) > 0 {
			commandOpts = opts[i%len(opts)]
		}
		m := NewMainWithCluster(i == 0, commandOpts...)
		m.Config.Gossip.Port = "0"
		m.Config.Gossip.Seeds = gossipSeeds[:i]

		if err := m.Start(); err != nil {
			return nil, errors.Wrapf(err, "Starting server %d", i)
		}
		gossipSeeds[i] = m.GossipTransport().URI.String()
		mains[i] = m
	}

	return mains, nil
}

// MustRunMain returns a new, running Main. Panic on error.
func MustRunMain() *Main {
	m := NewMain()
	m.Config.Metric.Diagnostics = false // Disable diagnostics.
	if err := m.Start(); err != nil {
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
	config := m.Command.Config
	m.Command = server.NewCommand(os.Stdin, os.Stdout, os.Stderr, m.commandOptions...)
	m.Command.Config = config
	err := m.SetupServer()
	if err != nil {
		return errors.Wrap(err, "setting up server")
	}

	// Run new program.
	if err := m.Start(); err != nil {
		return err
	}
	return nil
}

// URL returns the base URL string for accessing the running program.
func (m *Main) URL() string { return m.Server.URI.String() }

// Client returns a client to connect to the program.
func (m *Main) Client() *http.InternalClient {
	client, err := http.NewInternalClient(m.Server.URI.HostPort(), http.GetHTTPClient(nil))
	if err != nil {
		panic(err)
	}
	return client
}

// Query executes a query against the program through the HTTP API.
func (m *Main) Query(index, rawQuery, query string) (string, error) {
	resp := MustDo("POST", m.URL()+fmt.Sprintf("/index/%s/query?", index)+rawQuery, query)
	if resp.StatusCode != gohttp.StatusOK {
		return "", fmt.Errorf("invalid status: %d, body=%s", resp.StatusCode, resp.Body)
	}
	return resp.Body, nil
}

func (m *Main) RecalculateCaches() error {
	resp := MustDo("POST", fmt.Sprintf("%s/recalculate-caches", m.URL()), "")
	if resp.StatusCode != 204 {
		return fmt.Errorf("invalid status: %d, body=%s", resp.StatusCode, resp.Body)
	}
	return nil
}

////////////////////////////////////////////////////////////////////////////////////

// MustDo executes http.Do() with an http.NewRequest(). Panic on error.
func MustDo(method, urlStr string, body string) *httpResponse {
	req, err := gohttp.NewRequest(
		method,
		urlStr,
		strings.NewReader(body),
	)

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := gohttp.DefaultClient.Do(req)
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
	*gohttp.Response
	Body string
}
