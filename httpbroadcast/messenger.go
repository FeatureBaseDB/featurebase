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

package httpbroadcast

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"

	"golang.org/x/sync/errgroup"

	"github.com/gogo/protobuf/proto"
	"github.com/pilosa/pilosa"
)

// HTTPBroadcaster represents a NodeSet that broadcasts messages over HTTP.
type HTTPBroadcaster struct {
	server       *pilosa.Server
	internalPort string
}

// NewHTTPBroadcaster returns a new instance of HTTPBroadcaster.
func NewHTTPBroadcaster(s *pilosa.Server, internalPort string) *HTTPBroadcaster {
	return &HTTPBroadcaster{server: s, internalPort: internalPort}
}

// SendSync sends a protobuf message to all nodes simultaneously.
// It waits for all nodes to respond before the function returns (and returns any errors).
func (h *HTTPBroadcaster) SendSync(pb proto.Message) error {
	// Marshal the pb to []byte
	buf, err := pilosa.MarshalMessage(pb)
	if err != nil {
		return err
	}

	nodes, err := h.nodes()
	if err != nil {
		return err
	}

	var g errgroup.Group
	for _, n := range nodes {
		// Don't send the message to the local node.
		if n.Host == h.server.Host {
			continue
		}
		node := n
		g.Go(func() error {
			return h.sendNodeMessage(node, buf)
		})
	}
	return g.Wait()
}

// SendAsync exists to implement the Broadcaster interface, but just calls
// SendSync.
func (h *HTTPBroadcaster) SendAsync(pb proto.Message) error {
	return h.SendSync(pb)
}

func (h *HTTPBroadcaster) nodes() ([]*pilosa.Node, error) {
	if h.server == nil {
		return nil, errors.New("HTTPBroadcaster has no reference to Server")
	}
	nodeset, ok := h.server.Cluster.NodeSet.(*HTTPNodeSet)
	if !ok {
		return nil, errors.New("NodeSet cannot be caste to HTTPNodeSet")
	}
	return nodeset.Nodes(), nil
}

func (h *HTTPBroadcaster) sendNodeMessage(node *pilosa.Node, msg []byte) error {
	var client *http.Client
	client = http.DefaultClient

	// Create HTTP request.
	req, err := http.NewRequest("POST", (&url.URL{
		Scheme: "http",
		Host:   node.InternalHost,
	}).String(), bytes.NewReader(msg))

	// Require protobuf encoding.
	req.Header.Set("Content-Type", "application/x-protobuf")

	// Send request to remote node.
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Read response into buffer.
	body, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		return err
	}

	// Check status code.
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("invalid status sendNodeMessage: code=%d, err=%s, req=%v", resp.StatusCode, body, req)
	}

	return nil
}

// HTTPBroadcastReceiver unmarshals incoming messages over HTTP and passes them on to the handler.
type HTTPBroadcastReceiver struct {
	port      string
	handler   pilosa.BroadcastHandler
	logOutput io.Writer
}

// NewHTTPBroadcastReceiver returns a new instance of HTTPBroadcastReceiver.
func NewHTTPBroadcastReceiver(port string, logOutput io.Writer) *HTTPBroadcastReceiver {
	return &HTTPBroadcastReceiver{
		port:      port,
		logOutput: logOutput,
	}
}

// Start implements the BroadcastReceiver interface and starts listening for broadcast messages.
func (rec *HTTPBroadcastReceiver) Start(b pilosa.BroadcastHandler) error {
	rec.handler = b
	go func() {
		err := http.ListenAndServe(":"+rec.port, rec)
		if err != nil {
			fmt.Fprintf(rec.logOutput, "Error listening on %v for HTTPBroadcastReceiver: %v\n", ":"+rec.port, err)
		}
	}()
	return nil
}

func (rec *HTTPBroadcastReceiver) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Content-Type") != "application/x-protobuf" {
		http.Error(w, "Unsupported media type", http.StatusUnsupportedMediaType)
		return
	}

	// Read entire body.
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Unmarshal message to specific proto type.
	m, err := pilosa.UnmarshalMessage(body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := rec.handler.ReceiveMessage(m); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}

// HTTPNodeSet represents a NodeSet that broadcasts messages over HTTP.
type HTTPNodeSet struct {
	nodes []*pilosa.Node
}

// NewHTTPNodeSet returns a new instance of HTTPNodeSet.
func NewHTTPNodeSet() *HTTPNodeSet {
	return &HTTPNodeSet{}
}

// Nodes implements the NodeSet interface and returns a list of nodes in the cluster.
func (h *HTTPNodeSet) Nodes() []*pilosa.Node {
	return h.nodes
}

// Open implements the NodeSet interface to start network activity, but for a HTTPNodeSet it does nothing.
func (h *HTTPNodeSet) Open() error {
	return nil
}

// Join sets the NodeSet nodes to the slice of Nodes passed in.
func (h *HTTPNodeSet) Join(nodes []*pilosa.Node) error {
	h.nodes = nodes
	return nil
}
