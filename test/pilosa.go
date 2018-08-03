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
	"github.com/pkg/errors"
)

////////////////////////////////////////////////////////////////////////////////////
// Command represents a test wrapper for server.Command.
type Command struct {
	*server.Command

	commandOptions []server.CommandOption

	stdin  bytes.Buffer
	stdout bytes.Buffer
	stderr bytes.Buffer
}

func OptAllowedOrigins(origins []string) server.CommandOption {
	return func(m *server.Command) error {
		m.Config.Handler.AllowedOrigins = origins
		return nil
	}
}

// newCommand returns a new instance of Main with a temporary data directory and random port.
func newCommand(opts ...server.CommandOption) *Command {
	path, err := ioutil.TempDir("", "pilosa-")
	if err != nil {
		panic(err)
	}

	// set aggressive close timeout by default to avoid hanging tests. This was
	// a problem with PDK tests which used go-pilosa as well. We put it at the
	// beginning of the option slice so that it can be overridden by user-passed
	// options.
	opts = append([]server.CommandOption{server.OptCommandCloseTimeout(time.Millisecond * 2)}, opts...)
	m := &Command{Command: server.NewCommand(os.Stdin, os.Stdout, os.Stderr, opts...), commandOptions: opts}
	m.Config.DataDir = path
	m.Config.Bind = "http://localhost:0"
	m.Config.Cluster.Disabled = true
	m.Command.Stdin = &m.stdin
	m.Command.Stdout = &m.stdout
	m.Command.Stderr = &m.stderr

	if testing.Verbose() {
		m.Command.Stdout = io.MultiWriter(os.Stdout, m.Command.Stdout)
		m.Command.Stderr = io.MultiWriter(os.Stderr, m.Command.Stderr)
	}

	return m
}

// NewCommandNode returns a new instance of Command with clustering enabled.
func NewCommandNode(isCoordinator bool, opts ...server.CommandOption) *Command {
	m := newCommand(opts...)
	m.Config.Cluster.Disabled = false
	m.Config.Cluster.Coordinator = isCoordinator
	return m
}

// MustRunCommand returns a new, running Main. Panic on error.
func MustRunCommand() *Command {
	m := newCommand()
	m.Config.Metric.Diagnostics = false // Disable diagnostics.
	if err := m.Start(); err != nil {
		panic(err)
	}
	return m
}

// GossipAddress returns the address on which gossip is listening after a Main
// has been setup. Useful to pass as a seed to other nodes when creating and
// testing clusters.
func (m *Command) GossipAddress() string {
	return m.GossipTransport().URI.String()
}

// Close closes the program and removes the underlying data directory.
func (m *Command) Close() error {
	defer os.RemoveAll(m.Config.DataDir)
	return m.Command.Close()
}

// Reopen closes the program and reopens it.
func (m *Command) Reopen() error {
	if err := m.Command.Close(); err != nil {
		return err
	}

	// Create new main with the same config.
	config := m.Command.Config
	m.Command = server.NewCommand(os.Stdin, os.Stdout, os.Stderr, m.commandOptions...)
	m.Command.Config = config

	// Run new program.
	return m.Start()
}

// URL returns the base URL string for accessing the running program.
func (m *Command) URL() string { return m.API.Node().URI.String() }

// Client returns a client to connect to the program.
func (m *Command) Client() *http.InternalClient {
	client, err := http.NewInternalClient(m.API.Node().URI.HostPort(), http.GetHTTPClient(nil))
	if err != nil {
		panic(err)
	}
	return client
}

// Query executes a query against the program through the HTTP API.
func (m *Command) Query(index, rawQuery, query string) (string, error) {
	resp := MustDo("POST", m.URL()+fmt.Sprintf("/index/%s/query?", index)+rawQuery, query)
	if resp.StatusCode != gohttp.StatusOK {
		return "", fmt.Errorf("invalid status: %d, body=%s", resp.StatusCode, resp.Body)
	}
	return resp.Body, nil
}

func (m *Command) RecalculateCaches() error {
	resp := MustDo("POST", fmt.Sprintf("%s/recalculate-caches", m.URL()), "")
	if resp.StatusCode != 204 {
		return fmt.Errorf("invalid status: %d, body=%s", resp.StatusCode, resp.Body)
	}
	return nil
}

// Cluster represents a Pilosa cluster (multiple Command instances)
type Cluster []*Command

// Start runs a Cluster
func (c Cluster) Start() error {
	var gossipSeeds = make([]string, len(c))
	for i, cc := range c {
		cc.Config.Gossip.Port = "0"
		cc.Config.Gossip.Seeds = gossipSeeds[:i]
		if err := cc.Start(); err != nil {
			return errors.Wrapf(err, "starting server %d", i)
		}
		gossipSeeds[i] = cc.GossipAddress()
	}
	return nil
}

// Stop stops a Cluster
func (c Cluster) Close() error {
	for i, cc := range c {
		if err := cc.Close(); err != nil {
			return errors.Wrapf(err, "stopping server %d", i)
		}
	}
	return nil
}

// MustNewCluster creates a new cluster
func MustNewCluster(t *testing.T, size int, opts ...[]server.CommandOption) Cluster {
	c, err := newCluster(size, opts...)
	if err != nil {
		t.Fatalf("new cluster: %v", err)
	}
	return c
}

// newCluster creates a new cluster
func newCluster(size int, opts ...[]server.CommandOption) (Cluster, error) {
	if size == 0 {
		return nil, errors.New("cluster must contain at least one node")
	}
	if len(opts) != size && len(opts) != 0 && len(opts) != 1 {
		return nil, errors.New("Slice of CommandOptions must be of length 0, 1, or equal to the number of cluster nodes")
	}

	cluster := make(Cluster, size)
	for i := 0; i < size; i++ {
		var commandOpts []server.CommandOption
		if len(opts) > 0 {
			commandOpts = opts[i%len(opts)]
		}
		m := NewCommandNode(i == 0, commandOpts...)
		cluster[i] = m
	}

	return cluster, nil
}

// runCluster creates and starts a new cluster
func runCluster(size int, opts ...[]server.CommandOption) (Cluster, error) {
	cluster, err := newCluster(size, opts...)
	if err != nil {
		return nil, errors.Wrap(err, "new cluster")
	}
	if err = cluster.Start(); err != nil {
		return nil, errors.Wrap(err, "starting cluster")
	}
	return cluster, nil
}

// MustRunCluster creates and starts a new cluster
func MustRunCluster(t *testing.T, size int, opts ...[]server.CommandOption) Cluster {
	c, err := runCluster(size, opts...)
	if err != nil {
		t.Fatalf("run cluster: %v", err)
	}
	return c
}

////////////////////////////////////////////////////////////////////////////////////

// MustDo executes http.Do() with an http.NewRequest(). Panic on error.
func MustDo(method, urlStr string, body string) *httpResponse {
	req, err := gohttp.NewRequest(
		method,
		urlStr,
		strings.NewReader(body),
	)
	if err != nil {
		panic(err)
	}

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
