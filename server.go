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
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pilosa/pilosa/internal"
	"github.com/pkg/errors"

	"golang.org/x/sync/errgroup"
)

// Default server settings.
const (
	DefaultDiagnosticServer = "https://diagnostics.pilosa.com/v0/diagnostics"
)

// Ensure Server implements interfaces.
var _ Broadcaster = &Server{}
var _ BroadcastHandler = &Server{}
var _ StatusHandler = &Server{}

// Server represents a holder wrapped by a running HTTP server.
type Server struct {
	// Close management.
	wg      sync.WaitGroup
	closing chan struct{}

	// Internal
	Holder      *Holder
	Cluster     *Cluster
	diagnostics *DiagnosticsCollector
	executor    *Executor

	// External
	handler           *Handler
	Broadcaster       Broadcaster
	BroadcastReceiver BroadcastReceiver
	Gossiper          Gossiper

	systemInfo   SystemInfo
	gcNotifier   GCNotifier
	NewAttrStore func(string) AttrStore
	logger       Logger
	ln           net.Listener

	NodeID              string
	URI                 URI
	antiEntropyInterval time.Duration
	metricInterval      time.Duration
	diagnosticInterval  time.Duration
	maxWritesPerRequest int

	dataDir string
}

// ServerOption is a functional option type for pilosa.Server.
type ServerOption func(s *Server) error

func OptServerLogger(l Logger) ServerOption {
	return func(s *Server) error {
		s.logger = l
		return nil
	}
}

func OptServerReplicaN(n int) ServerOption {
	return func(s *Server) error {
		s.Cluster.ReplicaN = n
		return nil
	}
}

func OptServerDataDir(dir string) ServerOption {
	return func(s *Server) error {
		s.dataDir = dir
		return nil
	}
}

func OptServerAttrStoreFunc(af func(string) AttrStore) ServerOption {
	return func(s *Server) error {
		s.NewAttrStore = af
		s.Holder.NewAttrStore = af
		return nil
	}
}

func OptServerAntiEntropyInterval(interval time.Duration) ServerOption {
	return func(s *Server) error {
		s.antiEntropyInterval = interval
		return nil
	}
}

func OptServerLongQueryTime(dur time.Duration) ServerOption {
	return func(s *Server) error {
		s.Cluster.LongQueryTime = dur
		return nil
	}
}

func OptServerHandler(h *Handler) ServerOption {
	return func(s *Server) error {
		s.handler = h
		return nil
	}
}

func OptServerMaxWritesPerRequest(n int) ServerOption {
	return func(s *Server) error {
		s.maxWritesPerRequest = n
		return nil
	}
}

func OptServerMetricInterval(dur time.Duration) ServerOption {
	return func(s *Server) error {
		s.metricInterval = dur
		return nil
	}
}

func OptServerSystemInfo(si SystemInfo) ServerOption {
	return func(s *Server) error {
		s.systemInfo = si
		return nil
	}
}

func OptServerGCNotifier(gcn GCNotifier) ServerOption {
	return func(s *Server) error {
		s.gcNotifier = gcn
		return nil
	}
}

func OptServerInternalClient(c *http.Client) ServerOption {
	return func(s *Server) error {
		s.Cluster.internalClient = c
		return nil
	}
}

func OptServerStatsClient(sc StatsClient) ServerOption {
	return func(s *Server) error {
		s.Holder.Stats = sc
		return nil
	}
}

func OptServerDiagnosticsInterval(dur time.Duration) ServerOption {
	return func(s *Server) error {
		s.diagnosticInterval = dur
		return nil
	}
}

func OptServerListener(ln net.Listener) ServerOption {
	return func(s *Server) error {
		s.ln = ln

		return nil
	}
}

func OptServerURI(uri *URI) ServerOption {
	return func(s *Server) error {
		s.URI = *uri
		return nil
	}
}

// NewServer returns a new instance of Server.
func NewServer(opts ...ServerOption) (*Server, error) {
	s := &Server{
		closing:           make(chan struct{}),
		Cluster:           NewCluster(),
		Holder:            NewHolder(),
		handler:           NewHandler(),
		Broadcaster:       NopBroadcaster,
		BroadcastReceiver: NopBroadcastReceiver,
		diagnostics:       NewDiagnosticsCollector(DefaultDiagnosticServer),
		executor:          NewExecutor(),
		systemInfo:        NewNopSystemInfo(),

		gcNotifier: NopGCNotifier,

		NewAttrStore: NewNopAttrStore,

		antiEntropyInterval: time.Minute * 10,
		metricInterval:      0,
		diagnosticInterval:  0,

		logger: NopLogger,
	}
	s.diagnostics.server = s

	for _, opt := range opts {
		err := opt(s)
		if err != nil {
			return nil, errors.Wrap(err, "applying option")
		}
	}

	path, err := expandDirName(s.dataDir)
	if err != nil {
		return nil, err
	}

	s.Holder.Path = path
	s.Holder.Logger = s.logger
	s.Holder.Stats.SetLogger(s.logger)

	s.Cluster.Path = path
	s.Cluster.Logger = s.logger
	s.Cluster.Holder = s.Holder

	// update URI port with actual listener port. TODO this should probably be done outside of here.
	if s.URI.Port() == 0 {
		s.URI.SetPort(uint16(s.ln.Addr().(*net.TCPAddr).Port))
	}

	s.NodeID = s.LoadNodeID()

	node, err := NewNode(s.NodeID,
		OptNodeURI(&s.URI),
		OptNodeIsCoordinator(s.Cluster.Coordinator == s.NodeID),
		OptNodeLocalAPI(s.handler.API),
	)
	if err != nil {
		return nil, errors.Wrap(err, "creating node")
	}

	s.Cluster.Node = node
	s.Holder.Stats = s.Holder.Stats.WithTags(fmt.Sprintf("NodeID:%s", s.NodeID))

	s.executor.Holder = s.Holder
	s.executor.Node = node
	s.executor.Cluster = s.Cluster
	s.executor.MaxWritesPerRequest = s.maxWritesPerRequest
	s.handler.API.Executor = s.executor

	return s, nil
}

// Open opens and initializes the server.
func (s *Server) Open() error {
	s.logger.Printf("open server")
	// s.ln can be configured prior to Open() via s.OpenListener().
	if s.ln == nil {
		return errors.New("Must pass a listener option to NewServer")
	}

	// Log startup
	err := s.Holder.logStartup()
	if err != nil {
		log.Println(errors.Wrap(err, "logging startup"))
	}

	// Get or create NodeID.

	// Append the NodeID tag to stats.

	// Create default HTTP client

	// Create executor for executing queries.

	// Cluster settings.
	s.Cluster.Broadcaster = s.Broadcaster
	s.Cluster.MaxWritesPerRequest = s.maxWritesPerRequest

	// Initialize HTTP handler.
	s.handler.API.Holder = s.Holder
	s.handler.API.Broadcaster = s.Broadcaster
	s.handler.API.BroadcastHandler = s
	s.handler.API.StatusHandler = s
	s.handler.API.Cluster = s.Cluster

	// Initialize Holder.
	s.Holder.Broadcaster = s.Broadcaster

	// Serve HTTP.
	go func() {
		err := http.Serve(s.ln, s.handler)
		if err != nil {
			s.logger.Printf("HTTP handler terminated with error: %s\n", err)
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
		s.logger.Printf("loading NodeID: %v", err)
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

func (s *Server) monitorAntiEntropy() {
	ticker := time.NewTicker(s.antiEntropyInterval)
	defer ticker.Stop()

	s.logger.Printf("holder sync monitor initializing (%s interval)", s.antiEntropyInterval)

	for {
		// Wait for tick or a close.
		select {
		case <-s.closing:
			return
		case <-ticker.C:
			s.Holder.Stats.Count("AntiEntropy", 1, 1.0)
		}
		t := time.Now()
		s.logger.Printf("holder sync beginning")

		// Initialize syncer with local holder and remote client.
		var syncer HolderSyncer
		syncer.Holder = s.Holder
		syncer.Node = s.Cluster.Node
		syncer.Cluster = s.Cluster
		syncer.Closing = s.closing
		syncer.Stats = s.Holder.Stats.WithTags("HolderSyncer")

		// Sync holders.
		if err := syncer.SyncHolder(); err != nil {
			s.logger.Printf("holder sync error: err=%s", err)
			continue
		}

		// Record successful sync in log.
		s.logger.Printf("holder sync complete")
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
		opt := IndexOptions{}
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
		frame := s.Holder.Frame(obj.Index, obj.Frame)
		field := decodeField(obj.Field)
		if err := frame.CreateField(field); err != nil {
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
		node := node
		s.logger.Printf("SendSync to: %s", node.URI)
		// Don't forward the message to ourselves.
		if s.URI == node.URI {
			continue
		}

		eg.Go(func() error {
			return node.api.SendMessage(context.Background(), pb)
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
	s.logger.Printf("SendTo: %s", to.URI)
	return to.api.SendMessage(context.Background(), pb)
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
			s.logger.Printf("merge remote status: %s", err)
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
		return errors.Wrap(err, "applying schema")
	}

	// Sync maxSlices (standard).
	oldmaxslices := s.Holder.MaxSlices()
	for index, newMax := range ns.MaxSlices.Standard {
		localIndex := s.Holder.Index(index)
		// if we don't know about an index locally, log an error because
		// indexes should be created and synced prior to slice creation
		if localIndex == nil {
			s.logger.Printf("Local Index not found: %s", index)
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
			s.logger.Printf("Local Index not found: %s", index)
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
	if s.diagnosticInterval < time.Minute {
		s.logger.Printf("diagnostics disabled")
		return
	} else {
		s.logger.Printf("Pilosa is currently configured to send small diagnostics reports to our team every %v. More information here: https://www.pilosa.com/docs/latest/administration/#diagnostics", s.diagnosticInterval)
	}

	s.diagnostics.Logger = s.logger
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
			s.logger.Printf("Diagnostics error: %s", err)
		}
	}

	ticker := time.NewTicker(s.diagnosticInterval)
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
	if s.metricInterval <= 0 {
		return
	}

	var m runtime.MemStats
	ticker := time.NewTicker(s.metricInterval)
	defer ticker.Stop()

	defer s.gcNotifier.Close()

	s.logger.Printf("runtime stats initializing (%s interval)", s.metricInterval)

	for {
		// Wait for tick or a close.
		select {
		case <-s.closing:
			return
		case <-s.gcNotifier.AfterGC():
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

func expandDirName(path string) (string, error) {
	prefix := "~" + string(filepath.Separator)
	if strings.HasPrefix(path, prefix) {
		HomeDir := os.Getenv("HOME")
		if HomeDir == "" {
			return "", errors.New("data directory not specified and no home dir available")
		}
		return filepath.Join(HomeDir, strings.TrimPrefix(path, prefix)), nil
	}
	return path, nil
}
