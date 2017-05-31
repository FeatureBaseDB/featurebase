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

package pilosa

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pilosa/pilosa/internal"
)

// Default server settings.
const (
	DefaultAntiEntropyInterval = 10 * time.Minute
	DefaultPollingInterval     = 60 * time.Second
)

// Server represents a holder wrapped by a running HTTP server.
type Server struct {
	ln net.Listener

	// Close management.
	wg      sync.WaitGroup
	closing chan struct{}

	// Data storage and HTTP interface.
	Holder            *Holder
	Handler           *Handler
	Broadcaster       Broadcaster
	BroadcastReceiver BroadcastReceiver

	// Cluster configuration.
	// Host is replaced with actual host after opening if port is ":0".
	Host    string
	Cluster *Cluster

	// Background monitoring intervals.
	AntiEntropyInterval time.Duration
	PollingInterval     time.Duration

	// Misc options.
	MaxWritesPerRequest int

	LogOutput io.Writer
}

// NewServer returns a new instance of Server.
func NewServer() *Server {
	s := &Server{
		closing: make(chan struct{}),

		Holder:            NewHolder(),
		Handler:           NewHandler(),
		Broadcaster:       NopBroadcaster,
		BroadcastReceiver: NopBroadcastReceiver,

		//PluginRegistry: NewPluginRegistry(),

		AntiEntropyInterval: DefaultAntiEntropyInterval,
		PollingInterval:     DefaultPollingInterval,

		LogOutput: os.Stderr,
	}

	s.Handler.Holder = s.Holder

	return s
}

// Open opens and initializes the server.
func (s *Server) Open() error {
	// Require a port in the hostname.
	host, port, err := net.SplitHostPort(s.Host)
	if err != nil {
		return err
	} else if port == "" {
		port = DefaultPort
	}

	// Open HTTP listener to determine port (if specified as :0).
	ln, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return fmt.Errorf("net.Listen: %v", err)
	}
	s.ln = ln

	// Determine hostname based on listening port.
	s.Host = net.JoinHostPort(host, strconv.Itoa(s.ln.Addr().(*net.TCPAddr).Port))

	// Create local node if no cluster is specified.
	if len(s.Cluster.Nodes) == 0 {
		s.Cluster.Nodes = []*Node{{Host: s.Host}}
	}

	// Open holder.
	if err := s.Holder.Open(); err != nil {
		return fmt.Errorf("opening Holder: %v", err)
	}

	if err := s.BroadcastReceiver.Start(s); err != nil {
		return fmt.Errorf("starting BroadcastReceiver: %v", err)
	}

	// Open NodeSet communication
	if err := s.Cluster.NodeSet.Open(); err != nil {
		return fmt.Errorf("opening NodeSet: %v", err)
	}
	/*
		// Load plugins.
		if err := s.loadPlugins(); err != nil {
			return err
		}
	*/

	// Create executor for executing queries.
	e := NewExecutor()
	e.Holder = s.Holder
	e.Host = s.Host
	e.Cluster = s.Cluster
	e.MaxWritesPerRequest = s.MaxWritesPerRequest

	// Initialize HTTP handler.
	s.Handler.Broadcaster = s.Broadcaster
	s.Handler.StatusHandler = s
	s.Handler.Host = s.Host
	s.Handler.Cluster = s.Cluster
	s.Handler.Executor = e
	s.Handler.LogOutput = s.LogOutput

	// Initialize Holder.
	s.Holder.Broadcaster = s.Broadcaster
	s.Holder.LogOutput = s.LogOutput

	// Serve HTTP.
	go func() { http.Serve(ln, s.Handler) }()

	// Start background monitoring.
	s.wg.Add(2)
	go func() { defer s.wg.Done(); s.monitorAntiEntropy() }()
	go func() { defer s.wg.Done(); s.monitorMaxSlices() }()

	return nil
}

// Close closes the server and waits for it to shutdown.
func (s *Server) Close() error {
	// Notify goroutines to stop.
	close(s.closing)
	s.wg.Wait()

	if s.ln != nil {
		s.ln.Close()
	}
	if s.Holder != nil {
		s.Holder.Close()
	}

	return nil
}

// Addr returns the address of the listener.
func (s *Server) Addr() net.Addr {
	if s.ln == nil {
		return nil
	}
	return s.ln.Addr()
}

func (s *Server) logger() *log.Logger { return log.New(s.LogOutput, "", log.LstdFlags) }

func (s *Server) monitorAntiEntropy() {
	ticker := time.NewTicker(s.AntiEntropyInterval)
	defer ticker.Stop()

	s.logger().Printf("holder sync monitor initializing (%s interval)", s.AntiEntropyInterval)

	for {
		// Wait for tick or a close.
		select {
		case <-s.closing:
			return
		case <-ticker.C:
		}

		s.logger().Printf("holder sync beginning")

		// Initialize syncer with local holder and remote client.
		var syncer HolderSyncer
		syncer.Holder = s.Holder
		syncer.Host = s.Host
		syncer.Cluster = s.Cluster
		syncer.Closing = s.closing

		// Sync holders.
		if err := syncer.SyncHolder(); err != nil {
			s.logger().Printf("holder sync error: err=%s", err)
			continue
		}

		// Record successful sync in log.
		s.logger().Printf("holder sync complete")
	}
}

// monitorMaxSlices periodically pulls the highest slice from each node in the cluster.
func (s *Server) monitorMaxSlices() {
	// Ignore if only one node in the cluster.
	if len(s.Cluster.Nodes) <= 1 {
		return
	}

	ticker := time.NewTicker(s.PollingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.closing:
			return
		case <-ticker.C:
		}

		oldmaxslices := s.Holder.MaxSlices()
		for _, node := range s.Cluster.Nodes {
			if s.Host != node.Host {
				maxSlices, _ := checkMaxSlices(node.Host)
				for index, newmax := range maxSlices {
					// if we don't know about an index locally, log an error because
					// indexes should be created and synced prior to slice creation
					if localIndex := s.Holder.Index(index); localIndex != nil {
						if newmax > oldmaxslices[index] {
							oldmaxslices[index] = newmax
							localIndex.SetRemoteMaxSlice(newmax)
						}
					} else {
						s.logger().Printf("Local Index not found: %s", index)
					}
				}
			}
		}
	}
}

// ReceiveMessage represents an implementation of BroadcastHandler.
func (s *Server) ReceiveMessage(pb proto.Message) error {
	switch obj := pb.(type) {
	case *internal.CreateSliceMessage:
		idx := s.Holder.Index(obj.Index)
		if idx == nil {
			return fmt.Errorf("Local Index not found: %s", obj.Index)
		}
		if obj.IsInverse {
			idx.SetRemoteMaxInverseSlice(obj.Slice)
		} else {
			idx.SetRemoteMaxSlice(obj.Slice)
		}
	case *internal.CreateIndexMessage:
		opt := IndexOptions{
			ColumnLabel: obj.Meta.ColumnLabel,
			TimeQuantum: TimeQuantum(obj.Meta.TimeQuantum),
		}
		_, err := s.Holder.CreateIndex(obj.Index, opt)
		if err != nil {
			return err
		}
	case *internal.DeleteIndexMessage:
		if err := s.Holder.DeleteIndex(obj.Index); err != nil {
			return err
		}
	case *internal.CreateFrameMessage:
		index := s.Holder.Index(obj.Index)
		opt := FrameOptions{
			RowLabel:       obj.Meta.RowLabel,
			InverseEnabled: obj.Meta.InverseEnabled,
			CacheType:      obj.Meta.CacheType,
			CacheSize:      obj.Meta.CacheSize,
			TimeQuantum:    TimeQuantum(obj.Meta.TimeQuantum),
		}
		_, err := index.CreateFrame(obj.Frame, opt)
		if err != nil {
			return err
		}
	case *internal.DeleteFrameMessage:
		index := s.Holder.Index(obj.Index)
		if err := index.DeleteFrame(obj.Frame); err != nil {
			return err
		}
	}
	return nil
}

// LocalStatus returns the state of the local node as well as the
// holder (indexes/frames) according to the local node.
// In a gossip implementation, memberlist.Delegate.LocalState() uses this.
// Server implements StatusHandler.
func (s *Server) LocalStatus() (proto.Message, error) {
	if s.Holder == nil {
		return nil, errors.New("Server.Holder is nil")
	}

	ns := internal.NodeStatus{
		Host:    s.Host,
		State:   NodeStateUp,
		Indexes: encodeIndexes(s.Holder.Indexes()),
	}

	// Append Slice list per this Node's indexes
	for _, index := range ns.Indexes {
		index.Slices = s.Cluster.OwnsSlices(index.Name, index.MaxSlice, s.Host)
	}

	return &ns, nil
}

// ClusterStatus returns the NodeState for all nodes in the cluster.
func (s *Server) ClusterStatus() (proto.Message, error) {
	// Update local Node.state.
	ns, err := s.LocalStatus()
	if err != nil {
		return nil, err
	}
	node := s.Cluster.NodeByHost(s.Host)
	node.SetStatus(ns.(*internal.NodeStatus))

	// Update NodeState for all nodes.
	for host, nodeState := range s.Cluster.NodeStates() {
		// In a default configuration (or single-node) where a StaticNodeSet is used
		// then all nodes are marked as DOWN. At the very least, we should consider
		// the local node as UP.
		// TODO: we should be able to remove this check if/when cluster.Nodes and
		// cluster.NodeSet are unified.
		if host == s.Host {
			nodeState = NodeStateUp
		}
		node := s.Cluster.NodeByHost(host)
		node.SetState(nodeState)
	}

	return s.Cluster.Status(), nil
}

// HandleRemoteStatus receives incoming NodeState from remote nodes.
func (s *Server) HandleRemoteStatus(pb proto.Message) error {
	return s.mergeRemoteStatus(pb.(*internal.NodeStatus))
}

func (s *Server) mergeRemoteStatus(ns *internal.NodeStatus) error {
	// Update Node.state.
	node := s.Cluster.NodeByHost(ns.Host)
	node.SetStatus(ns)

	// Create indexes that don't exist.
	for _, index := range ns.Indexes {
		opt := IndexOptions{
			ColumnLabel: index.Meta.ColumnLabel,
			TimeQuantum: TimeQuantum(index.Meta.TimeQuantum),
		}
		idx, err := s.Holder.CreateIndexIfNotExists(index.Name, opt)
		if err != nil {
			return err
		}
		// Create frames that don't exist.
		for _, f := range index.Frames {
			opt := FrameOptions{
				RowLabel:    f.Meta.RowLabel,
				TimeQuantum: TimeQuantum(f.Meta.TimeQuantum),
				CacheSize:   f.Meta.CacheSize,
			}
			_, err := idx.CreateFrameIfNotExists(f.Name, opt)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func checkMaxSlices(hostport string) (map[string]uint64, error) {
	// Create HTTP request.
	req, err := http.NewRequest("GET", (&url.URL{
		Scheme: "http",
		Host:   hostport,
		Path:   "/slices/max",
	}).String(), nil)

	if err != nil {
		return nil, err
	}

	// Require protobuf encoding.
	req.Header.Set("Accept", "application/x-protobuf")
	req.Header.Set("Content-Type", "application/x-protobuf")

	// Send request to remote node.
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// Read response into buffer.
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	// Check status code.
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("invalid status checkMaxSlices: code=%d, err=%s, req=%v", resp.StatusCode, body, req)
	}

	// Decode response object.
	pb := internal.MaxSlicesResponse{}

	if err = proto.Unmarshal(body, &pb); err != nil {
		return nil, err
	}

	return pb.MaxSlices, nil
}

// StatusHandler specifies two methods which an object must implement to share
// state in the cluster. These are used by the GossipNodeSet to implement the
// LocalState and MergeRemoteState methods of memberlist.Delegate
type StatusHandler interface {
	LocalStatus() (proto.Message, error)
	ClusterStatus() (proto.Message, error)
	HandleRemoteStatus(proto.Message) error
}
