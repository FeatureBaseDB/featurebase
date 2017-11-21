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
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/CAFxX/gcnotifier"
	"github.com/gogo/protobuf/proto"
	"github.com/pilosa/pilosa/diagnostics"
	"github.com/pilosa/pilosa/internal"
)

// Default server settings.
const (
	DefaultAntiEntropyInterval = 10 * time.Minute
	DefaultPollingInterval     = 60 * time.Second
	DefaultDiagnosticServer    = "https://diagnostics.pilosa.com/v0/diagnostics"
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
	Network     string
	URI         URI
	Cluster     *Cluster
	diagnostics *diagnostics.Diagnostics

	// Background monitoring intervals.
	AntiEntropyInterval time.Duration
	MetricInterval      time.Duration
	DiagnosticInterval  time.Duration

	// TLS configuration
	TLS *tls.Config

	// Misc options.
	MaxWritesPerRequest int

	LogOutput io.Writer

	defaultClient InternalClient
}

// NewServer returns a new instance of Server.
func NewServer() *Server {
	s := &Server{
		closing: make(chan struct{}),

		Holder:            NewHolder(),
		Handler:           NewHandler(),
		Broadcaster:       NopBroadcaster,
		BroadcastReceiver: NopBroadcastReceiver,
		diagnostics:       diagnostics.New(DefaultDiagnosticServer),

		Network: "tcp",

		AntiEntropyInterval: DefaultAntiEntropyInterval,
		MetricInterval:      0,
		DiagnosticInterval:  0,

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

	// Find the Node ID and append that tag to stats.
	for i, n := range s.Cluster.Nodes {
		if n.URI == s.URI {
			s.Holder.Stats = s.Holder.Stats.WithTags(fmt.Sprintf("NodeID:%d", i))
			break
		}
	}

	// Peek at the holder to determine if there is data on disk.
	// Don't actually load the data until after the Cluster
	// management starts.
	s.Holder.LogOutput = s.LogOutput
	s.Holder.Peek()

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

	s.Cluster.prefect = s.Handler

	// Initialize Holder.
	s.Holder.Broadcaster = s.Broadcaster

	// Serve HTTP.
	go func() {
		err := http.Serve(ln, s.Handler)
		if err != nil {
			s.Logger().Printf("HTTP handler terminated with error: %s\n", err)
		}
	}()

	// Start the BroadcastReceiver.
	if err := s.BroadcastReceiver.Start(s); err != nil {
		return fmt.Errorf("starting BroadcastReceiver: %v", err)
	}

	// Open Cluster management.
	if err := s.Cluster.Open(); err != nil {
		return fmt.Errorf("opening Cluster: %v", err)
	}

	// Open holder.
	if err := s.Holder.Open(); err != nil {
		return fmt.Errorf("opening Holder: %v", err)
	}
	s.Cluster.setNodeState(NodeStateReady)

	// Listen for joining nodes.
	// This needs to start after the Holder has opened so that nodes can join
	// the cluster without waiting for data to load on the coordinator. Before
	// this starts, the joins are queued up in the Cluster.joiningURIs buffered
	// channel.
	s.Cluster.ListenForJoins()

	// Start background monitoring.
	s.wg.Add(3)
	go func() { defer s.wg.Done(); s.monitorAntiEntropy() }()
	go func() { defer s.wg.Done(); s.monitorRuntime() }()
	go func() { defer s.wg.Done(); s.monitorDiagnostics() }()

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

// Logger returns a logger that writes to LogOutput
func (s *Server) Logger() *log.Logger { return log.New(s.LogOutput, "", log.LstdFlags) }

func (s *Server) monitorAntiEntropy() {
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
		t := time.Now()
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
		dif := time.Since(t)
		s.Holder.Stats.Histogram("AntiEntropyDuration", float64(dif), 1.0)
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
		err := s.Cluster.MergeClusterStatus(obj)
		if err != nil {
			return err
		}
	case *internal.ResizeInstruction:
		err := s.Cluster.FollowResizeInstruction(obj)
		if err != nil {
			return err
		}
	case *internal.ResizeInstructionComplete:
		err := s.Cluster.MarkResizeInstructionComplete(obj)
		if err != nil {
			return err
		}
	case *internal.SetCoordinatorMessage:
		s.Cluster.SetCoordinator(DecodeURI(obj.Old), DecodeURI(obj.New))
	case *internal.NodeStateMessage:
		err := s.Cluster.ReceiveNodeState(DecodeURI(obj.URI), obj.State)
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

// Server implements StatusHandler.
// LocalStatus is used to periodically sync information
// between nodes. Under normal conditions, nodes should
// remain in sync through Broadcast messages. For cases
// where a node fails to receive a Broadcast message, or
// when a new (empty) node needs to get in sync with the
// rest of the cluster, two things are shared via gossip:
// - MaxSlice/MaxInverseSlice by Index
// - Schema
// In a gossip implementation, memberlist.Delegate.LocalState() uses this.
func (s *Server) LocalStatus() (proto.Message, error) {
	if s.Cluster == nil {
		return nil, errors.New("Server.Cluster is nil")
	}
	if s.Holder == nil {
		return nil, errors.New("Server.Holder is nil")
	}

	ns := internal.NodeStatus{
		URI:       encodeURI(s.URI),
		MaxSlices: s.Holder.EncodeMaxSlices(),
		Schema:    s.Holder.EncodeSchema(),
	}

	return &ns, nil
}

// ClusterStatus returns the ClusterState and NodeSet for the cluster.
func (s *Server) ClusterStatus() (proto.Message, error) {
	return s.Cluster.Status(), nil
}

// HandleRemoteStatus receives incoming NodeStatus from remote nodes.
func (s *Server) HandleRemoteStatus(pb proto.Message) error {
	return s.mergeRemoteStatus(pb.(*internal.NodeStatus))
}

func (s *Server) mergeRemoteStatus(ns *internal.NodeStatus) error {
	// Ignore status updates from self.
	if s.URI == decodeURI(ns.URI) {
		return nil
	}

	// If this node is still STARTING, don't apply remote status.
	// There is an issue where starting up a cluster with existing
	// data will error on `flock: resource temporarily unavailable`.
	// This is because the ApplySchema creates/opens indexes before
	// Holder.Open() has run. When Holder.Open() runs later, the
	// fragment files are locked.
	// TODO: There is still a race condition where the coordinator
	// changes state to NORMAL, broadcasts that to the remote node,
	// the remote node receives a `NodeStatus` (with schema) before
	// running `Holder.Open()`. In that case, state would be NORMAL,
	// meaning this check wouldn't pass, and `Holder.Open()` still
	// hasn't run. We may need to track whether `Holder.Open()` has
	// run, and use that to determine if we bail here.
	if s.Cluster.State == ClusterStateStarting {
		return nil
	}

	// Sync schema.
	if err := s.Holder.ApplySchema(ns.Schema); err != nil {
		return err
	}

	// Sync maxSlices (standard).
	oldmaxslices := s.Holder.MaxSlices()
	for index, newMax := range ns.MaxSlices.Standard {
		localIndex := s.Holder.Index(index)
		// if we don't know about an index locally, log an error because
		// indexes should be created and synced prior to slice creation
		if localIndex == nil {
			s.Logger().Printf("Local Index not found: %s", index)
			continue
		}
		if newMax > oldmaxslices[index] {
			oldmaxslices[index] = newMax
			localIndex.SetRemoteMaxSlice(newMax)
		}
	}

	// Sync maxSlices (inverse).
	oldMaxInverseSlices := s.Holder.MaxInverseSlices()
	for index, newMaxInverse := range ns.MaxSlices.Inverse {
		localIndex := s.Holder.Index(index)
		// if we don't know about an index locally, log an error because
		// indexes should be created and synced prior to slice creation
		if localIndex == nil {
			s.Logger().Printf("Local Index not found: %s", index)
			continue
		}
		if newMaxInverse > oldMaxInverseSlices[index] {
			oldMaxInverseSlices[index] = newMaxInverse
			localIndex.SetRemoteMaxInverseSlice(newMaxInverse)
		}
	}

	return nil
}

// monitorDiagnostics periodically polls the the Pilosa Indexes for cluster info.
func (s *Server) monitorDiagnostics() {
	if s.DiagnosticInterval <= 0 {
		s.Logger().Printf("diagnostics disabled")
		return
	}

	s.diagnostics.SetLogger(s.LogOutput)
	s.diagnostics.SetVersion(Version)
	s.diagnostics.SetInterval(s.DiagnosticInterval)
	s.diagnostics.Open()
	s.diagnostics.Set("Host", s.URI.host)
	s.diagnostics.Set("Cluster", strings.Join(NodeSet(s.Cluster.NodeSet()).ToStrings(), ","))
	s.diagnostics.Set("NumNodes", len(s.Cluster.Nodes))
	s.diagnostics.Set("NumCPU", runtime.NumCPU())
	// TODO: unique cluster ID

	// Flush the diagnostics metrics at startup, then on each tick interval
	flush := func() {
		numFrames := 0
		numSlices := uint64(0)
		for _, index := range s.Holder.Indexes() {
			numSlices += index.MaxSlice() + 1
			for _, f := range index.Frames() {
				numFrames++
				if f.rangeEnabled {
					s.diagnostics.Set("BSIEnabled", true)
				}
				if f.timeQuantum != "" {
					s.diagnostics.Set("TimeQuantumEnabled", true)
				}
			}
		}

		s.diagnostics.Set("NumIndexes", len(s.Holder.Indexes()))
		s.diagnostics.Set("NumFrames", numFrames)
		s.diagnostics.Set("NumSlices", numSlices)
		s.diagnostics.Set("OpenFiles", CountOpenFiles())
		s.diagnostics.Set("GoRoutines", runtime.NumGoroutine())
		s.diagnostics.CheckVersion()
		s.diagnostics.Flush()
	}

	ticker := time.NewTicker(s.DiagnosticInterval)
	defer ticker.Stop()
	flush()
	for {
		// Wait for tick or a close.
		select {
		case <-s.closing:
			return
		case <-ticker.C:
			flush()
		}
	}
}

// monitorRuntime periodically polls the Go runtime metrics.
func (s *Server) monitorRuntime() {
	// Disable metrics when poll interval is zero.
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
			// GC just ran.
			s.Holder.Stats.Count("garbage_collection", 1, 1.0)
		case <-ticker.C:
		}

		// Record the number of go routines.
		s.Holder.Stats.Gauge("goroutines", float64(runtime.NumGoroutine()), 1.0)

		// Open File handles.
		s.Holder.Stats.Gauge("OpenFiles", float64(CountOpenFiles()), 1.0)

		// Runtime memory metrics.
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
	s.defaultClient = NewInternalHTTPClientFromURI(nil, &ClientOptions{TLS: s.TLS})
}

// CountOpenFiles on operating systems that support lsof.
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
// state in the cluster. These are used by the GossipMemberSet to implement the
// LocalState and MergeRemoteState methods of memberlist.Delegate
type StatusHandler interface {
	LocalStatus() (proto.Message, error)
	ClusterStatus() (proto.Message, error)
	HandleRemoteStatus(proto.Message) error
}
