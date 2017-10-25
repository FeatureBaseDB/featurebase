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
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/CAFxX/gcnotifier"
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
	Network string
	URI     URI
	Cluster *Cluster

	// Background monitoring intervals.
	AntiEntropyInterval time.Duration
	PollingInterval     time.Duration
	MetricInterval      time.Duration

	// TLS configuration
	TLS *tls.Config

	// Misc options.
	MaxWritesPerRequest int

	LogOutput io.Writer

	defaultClient *http.Client
}

// NewServer returns a new instance of Server.
func NewServer() *Server {
	s := &Server{
		closing: make(chan struct{}),

		Holder:            NewHolder(),
		Handler:           NewHandler(),
		Broadcaster:       NopBroadcaster,
		BroadcastReceiver: NopBroadcastReceiver,

		Network: "tcp",

		AntiEntropyInterval: DefaultAntiEntropyInterval,
		PollingInterval:     DefaultPollingInterval,
		MetricInterval:      0,

		LogOutput: os.Stderr,
	}

	s.Handler.Holder = s.Holder

	return s
}

// Open opens and initializes the server.
func (s *Server) Open() error {
	var ln net.Listener
	var err error

	// If bind URI has the https scheme, enable TLS
	if s.URI.Scheme() == "https" && s.TLS != nil {
		ln, err = tls.Listen("tcp", s.URI.HostPort(), s.TLS)
		if err != nil {
			return err
		}
	} else if s.URI.Scheme() == "http" {
		// Open HTTP listener to determine port (if specified as :0).
		ln, err = net.Listen(s.Network, s.URI.HostPort())
		if err != nil {
			return fmt.Errorf("net.Listen: %v", err)
		}
	} else {
		return fmt.Errorf("unsupported scheme: %s", s.URI.Scheme())
	}

	s.ln = ln

	if s.URI.Port() == 0 {
		// If the port is 0, it is set automatically.
		// Find out automatically set port and update the host.
		s.URI.SetPort(uint16(s.ln.Addr().(*net.TCPAddr).Port))
	}

	// Set Cluster URI.
	s.Cluster.URI = s.URI

	/*
		// Create local node if no cluster is specified.
		if len(s.Cluster.Nodes) == 0 {
			s.Cluster.Nodes = []*Node{
				{Scheme: s.URI.Scheme(), Host: s.URI.HostPort()},
			}
		}

		// TODO: nodes aren't here yet. May need to merge new stats code anyway.
		for i, n := range s.Cluster.Nodes {
			if s.Cluster.NodeByHost(n.Host) != nil {
				s.Holder.Stats = s.Holder.Stats.WithTags(fmt.Sprintf("NodeID:%d", i))
			}
		}
	*/

	// Open holder.
	s.Holder.LogOutput = s.LogOutput
	if err := s.Holder.Open(); err != nil {
		return fmt.Errorf("opening Holder: %v", err)
	}

	// Start the BroadcastReceiver.
	if err := s.BroadcastReceiver.Start(s); err != nil {
		return fmt.Errorf("starting BroadcastReceiver: %v", err)
	}

	// Open Cluster management.
	if err := s.Cluster.Open(); err != nil {
		return fmt.Errorf("opening Cluster: %v", err)
	}

	// Create default HTTP client
	s.createDefaultClient()

	// Create executor for executing queries.
	e := NewExecutor(&ClientOptions{TLS: s.TLS})
	e.Holder = s.Holder
	e.URI = s.URI
	e.Cluster = s.Cluster
	e.MaxWritesPerRequest = s.MaxWritesPerRequest

	// Initialize HTTP handler.
	s.Handler.Broadcaster = s.Broadcaster
	s.Handler.StatusHandler = s
	s.Handler.URI = s.URI
	s.Handler.Cluster = s.Cluster
	s.Handler.Executor = e
	s.Handler.LogOutput = s.LogOutput

	// Initialize Holder.
	s.Holder.Broadcaster = s.Broadcaster

	// Serve HTTP.
	go func() {
		err := http.Serve(ln, s.Handler)
		if err != nil {
			s.Logger().Printf("HTTP handler terminated with error: %s\n", err)
		}
	}()

	/*
		// Start background monitoring.
		s.wg.Add(3)
		go func() { defer s.wg.Done(); s.monitorAntiEntropy() }()
		go func() { defer s.wg.Done(); s.monitorMaxSlices() }()
		go func() { defer s.wg.Done(); s.monitorRuntime() }()
	*/

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
	if s.Cluster != nil {
		s.Cluster.Close()
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

func (s *Server) Logger() *log.Logger { return log.New(s.LogOutput, "", log.LstdFlags) }

func (s *Server) monitorAntiEntropy() {
	t := time.Now()
	ticker := time.NewTicker(s.AntiEntropyInterval)
	defer ticker.Stop()

	s.Logger().Printf("holder sync monitor initializing (%s interval)", s.AntiEntropyInterval)

	for {
		// Wait for tick or a close.
		select {
		case <-s.closing:
			return
		case <-ticker.C:
			s.Holder.Stats.Count("AntiEntropy", 1, 1.0)
		}

		s.Logger().Printf("holder sync beginning")

		// Initialize syncer with local holder and remote client.
		var syncer HolderSyncer
		syncer.Holder = s.Holder
		syncer.URI = s.URI
		syncer.Cluster = s.Cluster
		syncer.Closing = s.closing
		syncer.ClientOptions = &ClientOptions{TLS: s.TLS}

		// Sync holders.
		if err := syncer.SyncHolder(); err != nil {
			s.Logger().Printf("holder sync error: err=%s", err)
			continue
		}

		// Record successful sync in log.
		s.Logger().Printf("holder sync complete")
	}
	dif := time.Since(t)
	s.Holder.Stats.Histogram("AntiEntropyDuration", float64(dif), 1.0)
}

// monitorMaxSlices periodically pulls the highest slice from each node in the cluster.
func (s *Server) monitorMaxSlices() {
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
			if s.URI != node.URI {
				maxSlices, _ := s.checkMaxSlices(node.URI)
				for index, newmax := range maxSlices {
					// if we don't know about an index locally, log an error because
					// indexes should be created and synced prior to slice creation
					if localIndex := s.Holder.Index(index); localIndex != nil {
						if newmax > oldmaxslices[index] {
							oldmaxslices[index] = newmax
							localIndex.SetRemoteMaxSlice(newmax)
						}
					} else {
						s.Logger().Printf("Local Index not found: %s", index)
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
		idx := s.Holder.Index(obj.Index)
		if idx == nil {
			return fmt.Errorf("Local Index not found: %s", obj.Index)
		}
		opt := decodeFrameOptions(obj.Meta)
		_, err := idx.CreateFrame(obj.Frame, *opt)
		if err != nil {
			return err
		}
	case *internal.DeleteFrameMessage:
		idx := s.Holder.Index(obj.Index)
		if err := idx.DeleteFrame(obj.Frame); err != nil {
			return err
		}
	case *internal.CreateInputDefinitionMessage:
		idx := s.Holder.Index(obj.Index)
		if idx == nil {
			return fmt.Errorf("Local Index not found: %s", obj.Index)
		}
		idx.CreateInputDefinition(obj.Definition)
	case *internal.DeleteInputDefinitionMessage:
		idx := s.Holder.Index(obj.Index)
		err := idx.DeleteInputDefinition(obj.Name)
		if err != nil {
			return err
		}
	case *internal.DeleteViewMessage:
		f := s.Holder.Frame(obj.Index, obj.Frame)
		if f == nil {
			return fmt.Errorf("Local Frame not found: %s", obj.Frame)
		}
		err := f.DeleteView(obj.View)
		if err != nil {
			return err
		}
	case *internal.ClusterStatus:
		err := s.Cluster.mergeClusterStatus(obj)
		if err != nil {
			return err
		}
	case *internal.ResizeInstruction:
		s.Cluster.followResizeInstruction(obj)
	case *internal.ResizeInstructionComplete:
		err := s.Cluster.MarkResizeInstructionComplete(obj)
		if err != nil {
			return err
		}
	}

	return nil
}

// State returns the cluster state according to this node.
func (s *Server) State() string {
	return s.Cluster.State
}

// LocalStatus returns the state of the local node as well as the
// holder (indexes/frames) according to the local node.
// In a gossip implementation, memberlist.Delegate.LocalState() uses this.
// Server implements StatusHandler.
func (s *Server) LocalStatus() (proto.Message, error) {
	if s.Cluster == nil {
		return nil, errors.New("Server.Cluster is nil")
	}
	if s.Holder == nil {
		return nil, errors.New("Server.Holder is nil")
	}

	ns := internal.NodeStatus{
		URI:     encodeURI(s.URI),
		State:   s.State(),
		Indexes: EncodeIndexes(s.Holder.Indexes()),
		URISet:  encodeURIs(s.Cluster.URISet()),
	}

	// TODO: get rid of this
	// Append Slice list per this Node's indexes
	for _, index := range ns.Indexes {
		index.Slices = s.Cluster.OwnsSlices(index.Name, index.MaxSlice, s.URI)
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
	localNode := s.Cluster.localNode()
	localNode.SetStatus(ns.(*internal.NodeStatus))

	return s.Cluster.Status(), nil
}

// HandleRemoteStatus receives incoming NodeState from remote nodes.
func (s *Server) HandleRemoteStatus(pb proto.Message) error {
	return s.mergeRemoteStatus(pb.(*internal.NodeStatus))
}

func (s *Server) mergeRemoteStatus(ns *internal.NodeStatus) error {

	// Ignore status updates from self.
	if s.URI == decodeURI(ns.URI) {
		return nil
	}
	fmt.Printf("mergeRemoteStatus on (%s) from (%s)\n", s.URI, ns.URI)

	// Update Node.state.
	// Node can be nil if a merge occurs (via gossip) before the coordinator has
	// a chance to broadcast the existence of the node.
	uri := decodeURI(ns.URI)
	if node := s.Cluster.NodeByURI(uri); node != nil {
		node.SetStatus(ns)
	}

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
			opt := decodeFrameOptions(f.Meta)
			_, err := idx.CreateFrameIfNotExists(f.Name, *opt)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *Server) checkMaxSlices(uri URI) (map[string]uint64, error) {
	// Create HTTP request.
	req, err := http.NewRequest("GET", (&url.URL{
		Scheme: uri.Scheme(),
		Host:   uri.HostPort(),
		Path:   "/slices/max",
	}).String(), nil)

	if err != nil {
		return nil, err
	}

	// Require protobuf encoding.
	req.Header.Set("Accept", "application/x-protobuf")
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("User-Agent", "pilosa/"+Version)

	resp, err := s.defaultClient.Do(req)
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

// monitorRuntime periodically polls the Go runtime metrics.
func (s *Server) monitorRuntime() {
	// Disable metrics when poll interval is zero
	if s.MetricInterval <= 0 {
		return
	}

	var m runtime.MemStats
	ticker := time.NewTicker(s.MetricInterval)
	defer ticker.Stop()

	gcn := gcnotifier.New()
	defer gcn.Close()

	s.Logger().Printf("runtime stats initializing (%s interval)", s.MetricInterval)

	for {
		// Wait for tick or a close.
		select {
		case <-s.closing:
			return
		case <-gcn.AfterGC():
			// GC just ran
			s.Holder.Stats.Count("garbage_collection", 1, 1.0)
		case <-ticker.C:
		}

		// Record the number of go routines
		s.Holder.Stats.Gauge("goroutines", float64(runtime.NumGoroutine()), 1.0)

		// Open File handles
		s.Holder.Stats.Gauge("OpenFiles", float64(CountOpenFiles()), 1.0)

		// Runtime memory metrics
		runtime.ReadMemStats(&m)
		s.Holder.Stats.Gauge("HeapAlloc", float64(m.HeapAlloc), 1.0)
		s.Holder.Stats.Gauge("HeapInuse", float64(m.HeapInuse), 1.0)
		s.Holder.Stats.Gauge("StackInuse", float64(m.StackInuse), 1.0)
		s.Holder.Stats.Gauge("Mallocs", float64(m.Mallocs), 1.0)
		s.Holder.Stats.Gauge("Frees", float64(m.Frees), 1.0)
	}
}

func (s *Server) createDefaultClient() {
	transport := &http.Transport{}
	if s.TLS != nil {
		transport.TLSClientConfig = s.TLS
	}
	s.defaultClient = &http.Client{Transport: transport}
}

// CountOpenFiles on opperating systems that support lsof
func CountOpenFiles() int {
	count := 0

	switch runtime.GOOS {
	case "darwin", "linux", "unix", "freebsd":
		// -b option avoid kernel blocks
		pid := os.Getpid()
		out, err := exec.Command("/bin/sh", "-c", fmt.Sprintf("lsof -b -p %v", pid)).Output()
		if err != nil {
			log.Fatal(err)
		}
		// only count lines with our pid, avoiding warning messages from -b
		lines := strings.Split(string(out), strconv.Itoa(pid))
		count = len(lines)
	case "windows":
		// TODO: count open file handles on windows
	default:

	}
	return count
}

// StatusHandler specifies the methods which an object must implement to share
// state in the cluster. These are used by the GossipNodeSet to implement the
// LocalState and MergeRemoteState methods of memberlist.Delegate
type StatusHandler interface {
	LocalStatus() (proto.Message, error)
	ClusterStatus() (proto.Message, error)
	HandleRemoteStatus(proto.Message) error
}
