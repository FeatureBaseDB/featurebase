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
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	gohttp "net/http"
	"net/http/httptest"
	"net/url"

	"github.com/gogo/protobuf/proto"
	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/http"
	"github.com/pilosa/pilosa/internal"
	"github.com/pilosa/pilosa/pql"
)

// Handler represents a test wrapper for pilosa.Handler.
type Handler struct {
	*http.Handler
	Executor HandlerExecutor
}

// NewHandler returns a new instance of Handler.
func NewHandler(opts ...http.HandlerOption) (*Handler, error) {
	handler, err := http.NewHandler(opts...)
	if err != nil {
		return nil, err
	}
	h := &Handler{
		Handler: handler,
	}

	// Handler test messages can no-op.
	h.API.Broadcaster = pilosa.NopBroadcaster

	return h, nil
}

// MustNewHandler returns a new instance of Handler.
func MustNewHandler(opts ...http.HandlerOption) *Handler {
	h, err := NewHandler(opts...)
	if err != nil {
		panic(err)
	}
	return h
}

// HandlerExecutor is a mock implementing pilosa.Handler.Executor.
type HandlerExecutor struct {
	cluster   *pilosa.Cluster
	ExecuteFn func(ctx context.Context, index string, query *pql.Query, shards []uint64, opt *pilosa.ExecOptions) ([]interface{}, error)
}

func (c *HandlerExecutor) Cluster() *pilosa.Cluster { return c.cluster }

func (c *HandlerExecutor) Execute(ctx context.Context, index string, query *pql.Query, shards []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
	return c.ExecuteFn(ctx, index, query, shards, opt)
}

// Server represents a test wrapper for httptest.Server.
type Server struct {
	*httptest.Server
	Handler *Handler
}

// NewServer returns a test server running on a random port.
func NewServer() *Server {
	return &Server{}
	//handler, err := pilosa.NewHandler()
	//if err != nil {
	//	panic(err)
	//}
	//s := &Server{
	//	Handler: handler,
	//}
	//s.Server = httptest.NewServer(s.Handler.Handler)

	//// Handler test messages can no-op.
	//s.Handler.API.Broadcaster = pilosa.NopBroadcaster
	//// Create a default cluster on the handler
	//s.Handler.API.Cluster = NewCluster(1)
	//s.Handler.API.Cluster.Nodes[0].URI = s.HostURI()

	//return s
}

// LocalStatus exists so that test.Server implements StatusHandler.
func (s *Server) LocalStatus() (proto.Message, error) {
	return nil, nil
}

// ClusterStatus exists so that test.Server implements StatusHandler.
func (s *Server) ClusterStatus() (proto.Message, error) {
	id := "test-node"
	uri := pilosa.DefaultURI()
	node := &pilosa.Node{
		ID:  id,
		URI: *uri,
	}
	return &internal.ClusterStatus{
		ClusterID: "",
		State:     pilosa.ClusterStateNormal,
		Nodes:     pilosa.EncodeNodes([]*pilosa.Node{node}),
	}, nil
}

// HandleRemoteStatus just need to implement a nop to complete the Interface
func (s *Server) HandleRemoteStatus(pb proto.Message) error { return nil }

// Host returns the hostname of the running server.
func (s *Server) Host() string { return MustParseURLHost(s.URL) }

func (s *Server) HostURI() pilosa.URI {
	uri, err := pilosa.NewURIFromAddress(s.URL)
	if err != nil {
		panic(err)
	}
	return *uri
}

// MustParseURLHost parses rawurl and returns the hostname. Panic on error.
func MustParseURLHost(rawurl string) string {
	u, err := url.Parse(rawurl)
	if err != nil {
		panic(err)
	}
	return u.Host
}

// MustNewHTTPRequest creates a new HTTP request. Panic on error.
func MustNewHTTPRequest(method, urlStr string, body io.Reader) *gohttp.Request {
	req, err := gohttp.NewRequest(method, urlStr, body)
	req.Header.Add("Accept", "application/json")
	if err != nil {
		panic(err)
	}
	return req
}

// MustMarshalJSON marshals v to JSON. Panic on error.
func MustMarshalJSON(v interface{}) []byte {
	buf, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return buf
}

// MustReadAll reads a reader into a buffer and returns it. Panic on error.
func MustReadAll(r io.Reader) []byte {
	buf, err := ioutil.ReadAll(r)
	if err != nil {
		panic(err)
	}
	return buf
}
