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

package ctl

import (
	"bytes"
	"context"
	"fmt"
	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/pql"
	"io/ioutil"
	"net/http/httptest"
	"net/url"
	"testing"
)

func TestBackupCommand_FileRequired(t *testing.T) {
	buf := bytes.Buffer{}
	stdin, stdout, stderr := GetIO(buf)

	cm := NewBackupCommand(stdin, stdout, stderr)
	err := cm.Run(context.Background())
	if err.Error() != "output file required" {
		t.Fatalf("expect error: output file required, actual: %s", err)
	}

}

func TestBackupCommand_Run(t *testing.T) {

	buf := bytes.Buffer{}
	stdin, stdout, stderr := GetIO(buf)

	hldr := MustOpenHolder()
	defer hldr.Close()

	s := NewServer()
	defer s.Close()
	s.Handler.Host = s.Host()
	s.Handler.Cluster = NewCluster(1)
	s.Handler.Cluster.Nodes[0].Host = s.Host()
	s.Handler.Holder = hldr.Holder
	cm := NewBackupCommand(stdin, stdout, stderr)
	file, err := ioutil.TempFile("", "import.csv")

	cm.Index = "i"
	cm.Host = s.Host()
	cm.Frame = "f"
	cm.View = pilosa.ViewStandard
	cm.Path = file.Name()
	err = cm.Run(context.Background())
	if err != nil {
		t.Fatalf("Command not working, error: '%s'", err)
	}
}

// Server represents a test wrapper for httptest.Server.
type Server struct {
	*httptest.Server
	Handler *Handler
}

// NewServer returns a test server running on a random port.
func NewServer() *Server {
	s := &Server{
		Handler: NewHandler(),
	}
	s.Server = httptest.NewServer(s.Handler.Handler)

	// Update handler to use hostname.
	s.Handler.Host = s.Host()

	// Handler test messages can no-op.
	s.Handler.Broadcaster = pilosa.NopBroadcaster
	// Create a default cluster on the handler
	s.Handler.Cluster = NewCluster(1)
	s.Handler.Cluster.Nodes[0].Host = s.Host()

	return s
}

// Handler represents a test wrapper for pilosa.Handler.
type Handler struct {
	*pilosa.Handler
	Executor HandlerExecutor
}

// NewHandler returns a new instance of Handler.
func NewHandler() *Handler {
	h := &Handler{
		Handler: pilosa.NewHandler(),
	}
	h.Handler.Executor = &h.Executor
	h.Handler.LogOutput = ioutil.Discard

	// Handler test messages can no-op.
	h.Broadcaster = pilosa.NopBroadcaster

	return h
}

// HandlerExecutor is a mock implementing pilosa.Handler.Executor.
type HandlerExecutor struct {
	cluster   *pilosa.Cluster
	ExecuteFn func(ctx context.Context, index string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error)
}

func (c *HandlerExecutor) Cluster() *pilosa.Cluster { return c.cluster }

func (c *HandlerExecutor) Execute(ctx context.Context, index string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
	return c.ExecuteFn(ctx, index, query, slices, opt)
}

// Host returns the hostname of the running server.
func (s *Server) Host() string { return MustParseURLHost(s.URL) }

// MustParseURLHost parses rawurl and returns the hostname. Panic on error.
func MustParseURLHost(rawurl string) string {
	u, err := url.Parse(rawurl)
	if err != nil {
		panic(err)
	}
	return u.Host
}

func NewCluster(n int) *pilosa.Cluster {
	c := pilosa.NewCluster()
	c.ReplicaN = 1
	c.Hasher = NewModHasher()

	for i := 0; i < n; i++ {
		c.Nodes = append(c.Nodes, &pilosa.Node{
			Host: fmt.Sprintf("host%d", i),
		})
	}

	return c
}

// ModHasher represents a simple, mod-based hashing.
type ModHasher struct{}

// NewModHasher returns a new instance of ModHasher with n buckets.
func NewModHasher() *ModHasher { return &ModHasher{} }

func (*ModHasher) Hash(key uint64, n int) int { return int(key) % n }

// ConstHasher represents hash that always returns the same index.
type ConstHasher struct {
	i int
}

// NewConstHasher returns a new instance of ConstHasher that always returns i.
func NewConstHasher(i int) *ConstHasher { return &ConstHasher{i: i} }

func (h *ConstHasher) Hash(key uint64, n int) int { return h.i }

// Holder is a test wrapper for pilosa.Holder.
type Holder struct {
	*pilosa.Holder
	LogOutput bytes.Buffer
}

// NewHolder returns a new instance of Holder with a temporary path.
func NewHolder() *Holder {
	path, err := ioutil.TempDir("", "pilosa-")
	if err != nil {
		panic(err)
	}

	h := &Holder{Holder: pilosa.NewHolder()}
	h.Path = path
	h.Holder.LogOutput = &h.LogOutput

	return h
}

// MustOpenHolder creates and opens a holder at a temporary path. Panic on error.
func MustOpenHolder() *Holder {
	h := NewHolder()
	if err := h.Open(); err != nil {
		panic(err)
	}
	return h
}
