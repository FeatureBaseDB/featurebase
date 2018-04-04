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
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pilosa/pilosa/internal"

	"golang.org/x/sync/errgroup"
)

// Default server settings.
const (
	DefaultAntiEntropyInterval = 10 * time.Minute
	DefaultDiagnosticServer    = "https://diagnostics.pilosa.com/v0/diagnostics"
)

// Ensure Server implements interfaces.
var _ Broadcaster = &Server{}
var _ BroadcastHandler = &Server{}
var _ StatusHandler = &Server{}

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
	Gossiper          Gossiper
	RemoteClient      *http.Client

	// Cluster configuration.
	Network     string
	NodeID      string
	URI         URI
	Cluster     *Cluster
	diagnostics *DiagnosticsCollector
	SystemInfo  SystemInfo

	GCNotifier GCNotifier

	NewAttrStore func(string) AttrStore

	// Background monitoring intervals.
	AntiEntropyInterval time.Duration
	MetricInterval      time.Duration
	DiagnosticInterval  time.Duration

	// TLS configuration
	TLS *tls.Config

	// Misc options.
	MaxWritesPerRequest int

	Logger Logger

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
		diagnostics:       NewDiagnosticsCollector(DefaultDiagnosticServer),
		SystemInfo:        NewNopSystemInfo(),

		Network: "tcp",

		GCNotifier: NopGCNotifier,

		NewAttrStore: NewNopAttrStore,

		AntiEntropyInterval: DefaultAntiEntropyInterval,
		MetricInterval:      0,
		DiagnosticInterval:  0,

		Logger: NopLogger,
	}

	s.Handler.Holder = s.Holder
	s.diagnostics.server = s
	return s
}

// Open opens and initializes the server.
func (s *Server) Open() error {
	s.Logger.Printf("open server")
	// s.ln can be configured prior to Open() via s.OpenListener().
	if s.ln == nil {
		if err := s.OpenListener(); err != nil {
			return err
		}
	}

	// Get or create NodeID.
	s.NodeID = s.LoadNodeID()

	// Set Cluster Node.
	node := &Node{
		ID:            s.NodeID,
		URI:           s.URI,
		IsCoordinator: s.Cluster.Coordinator == s.NodeID,
	}
	s.Cluster.Node = node

	// Append the NodeID tag to stats.
	s.Holder.Stats = s.Holder.Stats.WithTags(fmt.Sprintf("NodeID:%s", s.NodeID))

	// Peek at the holder to determine if there is data on disk.
	// Don't actually load the data until after the Cluster
	// management starts.
	s.Holder.Peek()

	// Create default HTTP client
	s.createDefaultClient(s.RemoteClient)

	// Create executor for executing queries.
	e := NewExecutor(s.RemoteClient)
	e.Holder = s.Holder
	e.Node = node
	e.Cluster = s.Cluster
	e.MaxWritesPerRequest = s.MaxWritesPerRequest

	// Cluster settings.
	s.Cluster.Broadcaster = s.Broadcaster
	s.Cluster.MaxWritesPerRequest = s.MaxWritesPerRequest

	// Initialize HTTP handler.
	s.Handler.Broadcaster = s.Broadcaster
	s.Handler.BroadcastHandler = s
	s.Handler.StatusHandler = s
	s.Handler.Node = node
	s.Handler.Cluster = s.Cluster
	s.Handler.Executor = e

	s.Cluster.prefect = s.Handler

	// Initialize Holder.
	s.Holder.Broadcaster = s.Broadcaster

	// Serve HTTP.
	go func() {
		err := http.Serve(s.ln, s.Handler)
		if err != nil {
			s.Logger.Printf("HTTP handler terminated with error: %s\n", err)
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
	if err := s.Cluster.SetNodeState(NodeStateReady); err != nil {
		return fmt.Errorf("setting nodeState: %v", err)
	}

	// Listen for joining nodes.
	// This needs to start after the Holder has opened so that nodes can join
	// the cluster without waiting for data to load on the coordinator. Before
	// this starts, the joins are queued up in the Cluster.joiningLeavingNodes
	// buffered channel.
	s.Cluster.ListenForJoins()

	// Start background monitoring.
	s.wg.Add(3)
	go func() { defer s.wg.Done(); s.monitorAntiEntropy() }()
	go func() { defer s.wg.Done(); s.monitorRuntime() }()
	go func() { defer s.wg.Done(); s.monitorDiagnostics() }()

	return nil
}

// OpenListener opens a listener for the Server.
func (s *Server) OpenListener() error {
	s.Logger.Printf("open server listener: %s", s.URI)
	if s.ln != nil {
		return fmt.Errorf("a listener already exists for server: %s", s.URI)
	}

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

// LoadNodeID gets NodeID from disk, or creates a new value.
// If server.NodeID is already set, a new ID is not created.
func (s *Server) LoadNodeID() string {
	if s.NodeID != "" {
		return s.NodeID
	}
	nodeID, err := s.Holder.loadNodeID()
	if err != nil {
		s.Logger.Printf("loading NodeID: %v", err)
		return s.NodeID
	}
	return nodeID
}

// Addr returns the address of the listener.
func (s *Server) Addr() net.Addr {
	if s.ln == nil {
		return nil
	}
	return s.ln.Addr()
}
func GetHTTPClient(t *tls.Config) *http.Client {
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		MaxIdleConns:          1000,
		MaxIdleConnsPerHost:   200,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}
	if t != nil {
		transport.TLSClientConfig = t
	}
	return &http.Client{Transport: transport}
}

func (s *Server) monitorAntiEntropy() {
	ticker := time.NewTicker(s.AntiEntropyInterval)
	defer ticker.Stop()

	s.Logger.Printf("holder sync monitor initializing (%s interval)", s.AntiEntropyInterval)

	for {
		// Wait for tick or a close.
		select {
		case <-s.closing:
			return
		case <-ticker.C:
			s.Holder.Stats.Count("AntiEntropy", 1, 1.0)
		}
		t := time.Now()
		s.Logger.Printf("holder sync beginning")

		// Initialize syncer with local holder and remote client.
		var syncer HolderSyncer
		syncer.Holder = s.Holder
		syncer.Node = s.Cluster.Node
		syncer.Cluster = s.Cluster
		syncer.Closing = s.closing
		syncer.RemoteClient = s.RemoteClient
		syncer.Stats = s.Holder.Stats.WithTags("HolderSyncer")

		// Sync holders.
		if err := syncer.SyncHolder(); err != nil {
			s.Logger.Printf("holder sync error: err=%s", err)
			continue
		}

		// Record successful sync in log.
		s.Logger.Printf("holder sync complete")
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
	case *internal.CreateFieldMessage:
		f := s.Holder.Frame(obj.Index, obj.Frame)
		field := decodeField(obj.Field)
		if err := f.CreateField(field); err != nil {
			return err
		}
	case *internal.DeleteFieldMessage:
		f := s.Holder.Frame(obj.Index, obj.Frame)
		if err := f.DeleteField(obj.Field); err != nil {
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
	case *internal.CreateViewMessage:
		f := s.Holder.Frame(obj.Index, obj.Frame)
		if f == nil {
			return fmt.Errorf("Local Frame not found: %s", obj.Frame)
		}
		_, _, err := f.createViewIfNotExistsBase(obj.View)
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
		s.Cluster.SetCoordinator(DecodeNode(obj.New))
	case *internal.UpdateCoordinatorMessage:
		s.Cluster.UpdateCoordinator(DecodeNode(obj.New))
	case *internal.NodeStateMessage:
		err := s.Cluster.ReceiveNodeState(obj.NodeID, obj.State)
		if err != nil {
			return err
		}
	case *internal.RecalculateCaches:
		s.Holder.RecalculateCaches()
	case *internal.NodeEventMessage:
		s.Cluster.ReceiveEvent(DecodeNodeEvent(obj))
	}

	return nil
}

// SendSync represents an implementation of Broadcaster.
func (s *Server) SendSync(pb proto.Message) error {
	var eg errgroup.Group
	for _, node := range s.Cluster.Nodes {
		s.Logger.Printf("SendSync to: %s", node.URI)
		// Don't forward the message to ourselves.
		if s.URI == node.URI {
			continue
		}

		ctx := context.WithValue(context.Background(), "uri", &node.URI)
		eg.Go(func() error {
			return s.defaultClient.SendMessage(ctx, pb)
		})
	}

	return eg.Wait()
}

// SendAsync represents an implementation of Broadcaster.
func (s *Server) SendAsync(pb proto.Message) error {
	return s.Gossiper.SendAsync(pb)
}

// SendTo represents an implementation of Broadcaster.
func (s *Server) SendTo(to *Node, pb proto.Message) error {
	s.Logger.Printf("SendTo: %s", to.URI)
	ctx := context.WithValue(context.Background(), "uri", &to.URI)
	return s.defaultClient.SendMessage(ctx, pb)
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
		Node:      EncodeNode(s.Cluster.Node),
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
	// Ignore NodeStatus messages until the cluster is in a Normal state.
	if s.Cluster.State() != ClusterStateNormal {
		return nil
	}

	go func() {
		// Make sure the holder has opened.
		<-s.Holder.opened

		err := s.mergeRemoteStatus(pb.(*internal.NodeStatus))
		if err != nil {
			s.Logger.Printf("merge remote status: %s", err)
		}
	}()

	return nil
}

func (s *Server) mergeRemoteStatus(ns *internal.NodeStatus) error {
	// Ignore status updates from self.
	if s.NodeID == DecodeNode(ns.Node).ID {
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
			s.Logger.Printf("Local Index not found: %s", index)
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
			s.Logger.Printf("Local Index not found: %s", index)
			continue
		}
		if newMaxInverse > oldMaxInverseSlices[index] {
			oldMaxInverseSlices[index] = newMaxInverse
			localIndex.SetRemoteMaxInverseSlice(newMaxInverse)
		}
	}

	return nil
}

// monitorDiagnostics periodically polls the Pilosa Indexes for cluster info.
func (s *Server) monitorDiagnostics() {
	// Do not send more than once a minute
	if s.DiagnosticInterval < time.Minute {
		s.Logger.Printf("diagnostics disabled")
		return
	} else {
		s.Logger.Printf("Pilosa is currently configured to send small diagnostics reports to our team every %v. More information here: https://www.pilosa.com/docs/latest/administration/#diagnostics", s.DiagnosticInterval)
	}

	s.diagnostics.Logger = s.Logger
	s.diagnostics.SetVersion(Version)
	s.diagnostics.Set("Host", s.URI.host)
	s.diagnostics.Set("Cluster", strings.Join(s.Cluster.NodeIDs(), ","))
	s.diagnostics.Set("NumNodes", len(s.Cluster.Nodes))
	s.diagnostics.Set("NumCPU", runtime.NumCPU())
	s.diagnostics.Set("NodeID", s.NodeID)
	s.diagnostics.Set("ClusterID", s.Cluster.ID)
	s.diagnostics.EnrichWithOSInfo()

	// Flush the diagnostics metrics at startup, then on each tick interval
	flush := func() {
		openFiles, err := CountOpenFiles()
		if err == nil {
			s.diagnostics.Set("OpenFiles", openFiles)
		}
		s.diagnostics.Set("GoRoutines", runtime.NumGoroutine())
		s.diagnostics.EnrichWithMemoryInfo()
		s.diagnostics.EnrichWithSchemaProperties()
		s.diagnostics.CheckVersion()
		err = s.diagnostics.Flush()
		if err != nil {
			s.Logger.Printf("Diagnostics error: %s", err)
		}
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

	defer s.GCNotifier.Close()

	s.Logger.Printf("runtime stats initializing (%s interval)", s.MetricInterval)

	for {
		// Wait for tick or a close.
		select {
		case <-s.closing:
			return
		case <-s.GCNotifier.AfterGC():
			// GC just ran.
			s.Holder.Stats.Count("garbage_collection", 1, 1.0)
		case <-ticker.C:
		}

		// Record the number of go routines.
		s.Holder.Stats.Gauge("goroutines", float64(runtime.NumGoroutine()), 1.0)

		openFiles, err := CountOpenFiles()
		// Open File handles.
		if err == nil {
			s.Holder.Stats.Gauge("OpenFiles", float64(openFiles), 1.0)
		}

		// Runtime memory metrics.
		runtime.ReadMemStats(&m)
		s.Holder.Stats.Gauge("HeapAlloc", float64(m.HeapAlloc), 1.0)
		s.Holder.Stats.Gauge("HeapInuse", float64(m.HeapInuse), 1.0)
		s.Holder.Stats.Gauge("StackInuse", float64(m.StackInuse), 1.0)
		s.Holder.Stats.Gauge("Mallocs", float64(m.Mallocs), 1.0)
		s.Holder.Stats.Gauge("Frees", float64(m.Frees), 1.0)
	}
}

func (s *Server) createDefaultClient(remoteClient *http.Client) {
	s.defaultClient = NewInternalHTTPClientFromURI(nil, remoteClient)
}

// CountOpenFiles on operating systems that support lsof.
func CountOpenFiles() (int, error) {
	switch runtime.GOOS {
	case "darwin", "linux", "unix", "freebsd":
		// -b option avoid kernel blocks
		pid := os.Getpid()
		out, err := exec.Command("/bin/sh", "-c", fmt.Sprintf("lsof -b -p %v", pid)).Output()
		if err != nil {
			return 0, fmt.Errorf("calling lsof: %s", err)
		}
		// only count lines with our pid, avoiding warning messages from -b
		lines := strings.Split(string(out), strconv.Itoa(pid))
		return len(lines), nil
	case "windows":
		// TODO: count open file handles on windows
		return 0, errors.New("CountOpenFiles() on Windows is not supported")
	default:
		return 0, errors.New("CountOpenFiles() on this OS is not supported")
	}
}

// StatusHandler specifies the methods which an object must implement to share
// state in the cluster. These are used by the GossipMemberSet to implement the
// LocalState and MergeRemoteState methods of memberlist.Delegate
type StatusHandler interface {
	LocalStatus() (proto.Message, error)
	ClusterStatus() (proto.Message, error)
	HandleRemoteStatus(proto.Message) error
}
