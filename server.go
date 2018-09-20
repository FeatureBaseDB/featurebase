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
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pilosa/pilosa/roaring"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

// Default server settings.
const (
	defaultDiagnosticServer = "https://diagnostics.pilosa.com/v0/diagnostics"
)

// Ensure Server implements interfaces.
var _ broadcaster = &Server{}

// Server represents a holder wrapped by a running HTTP server.
type Server struct { // nolint: maligned
	// Close management.
	wg      sync.WaitGroup
	closing chan struct{}

	// Internal
	holder          *Holder
	cluster         *cluster
	diagnostics     *diagnosticsCollector
	executor        *executor
	hosts           []string
	clusterDisabled bool
	serializer      Serializer

	// External
	systemInfo SystemInfo
	gcNotifier GCNotifier
	logger     Logger

	nodeID              string
	uri                 URI
	antiEntropyInterval time.Duration
	metricInterval      time.Duration
	diagnosticInterval  time.Duration
	maxWritesPerRequest int
	isCoordinator       bool
	syncer              holderSyncer

	defaultClient InternalClient
	dataDir       string
}

// TODO: have this return an interface for Holder instead of concrete object?
func (s *Server) Holder() *Holder {
	return s.holder
}

// ServerOption is a functional option type for pilosa.Server
type ServerOption func(s *Server) error

func OptServerLogger(l Logger) ServerOption {
	return func(s *Server) error {
		s.logger = l
		return nil
	}
}

func OptServerReplicaN(n int) ServerOption {
	return func(s *Server) error {
		s.cluster.ReplicaN = n
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
		s.holder.NewAttrStore = af
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
		s.cluster.longQueryTime = dur
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

func OptServerInternalClient(c InternalClient) ServerOption {
	return func(s *Server) error {
		s.executor = newExecutor(optExecutorInternalQueryClient(c))
		s.defaultClient = c
		s.cluster.InternalClient = c
		return nil
	}
}

// DEPRECATED
func OptServerPrimaryTranslateStore(store TranslateStore) ServerOption {
	return func(s *Server) error {
		s.logger.Printf("DEPRECATED: OptServerPrimaryTranslateStore")
		return nil
	}
}

func OptServerPrimaryTranslateStoreFunc(tf func(interface{}) TranslateStore) ServerOption {
	return func(s *Server) error {
		s.holder.NewPrimaryTranslateStore = tf
		return nil
	}
}

func OptServerStatsClient(sc StatsClient) ServerOption {
	return func(s *Server) error {
		s.holder.Stats = sc
		return nil
	}
}

func OptServerDiagnosticsInterval(dur time.Duration) ServerOption {
	return func(s *Server) error {
		s.diagnosticInterval = dur
		return nil
	}
}

func OptServerURI(uri *URI) ServerOption {
	return func(s *Server) error {
		s.uri = *uri
		return nil
	}
}

// OptClusterDisabled tells the server whether to use a static cluster with the
// defined hosts. Mostly used for testing.
func OptServerClusterDisabled(disabled bool, hosts []string) ServerOption {
	return func(s *Server) error {
		s.hosts = hosts
		s.clusterDisabled = disabled
		return nil
	}
}

func OptServerSerializer(ser Serializer) ServerOption {
	return func(s *Server) error {
		s.serializer = ser
		return nil
	}
}

func OptServerIsCoordinator(is bool) ServerOption {
	return func(s *Server) error {
		s.isCoordinator = is
		return nil
	}
}

func OptServerNodeID(nodeID string) ServerOption {
	return func(s *Server) error {
		s.nodeID = nodeID
		return nil
	}
}

func OptServerClusterHasher(h Hasher) ServerOption {
	return func(s *Server) error {
		s.cluster.Hasher = h
		return nil
	}
}

func OptServerTranslateFileMapSize(mapSize int) ServerOption {
	return func(s *Server) error {
		s.holder.translateFile = NewTranslateFile(OptTranslateFileMapSize(mapSize))
		return nil
	}
}

// NewServer returns a new instance of Server.
func NewServer(opts ...ServerOption) (*Server, error) {
	s := &Server{
		closing:       make(chan struct{}),
		cluster:       newCluster(),
		holder:        NewHolder(),
		diagnostics:   newDiagnosticsCollector(defaultDiagnosticServer),
		systemInfo:    newNopSystemInfo(),
		defaultClient: nopInternalClient{},

		gcNotifier: NopGCNotifier,

		antiEntropyInterval: time.Minute * 10,
		metricInterval:      0,
		diagnosticInterval:  0,

		logger: NopLogger,
	}
	s.executor = newExecutor(optExecutorInternalQueryClient(s.defaultClient))
	s.cluster.InternalClient = s.defaultClient

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

	s.holder.Path = path
	s.holder.translateFile.Path = filepath.Join(path, ".keys")
	s.holder.Logger = s.logger
	s.holder.Stats.SetLogger(s.logger)

	s.cluster.Path = path
	s.cluster.logger = s.logger
	s.cluster.holder = s.holder

	// Get or create NodeID.
	s.nodeID = s.loadNodeID()
	if s.isCoordinator {
		s.cluster.Coordinator = s.nodeID
	}

	// Set Cluster Node.
	node := &Node{
		ID:            s.nodeID,
		URI:           s.uri,
		IsCoordinator: s.cluster.Coordinator == s.nodeID,
	}
	s.cluster.Node = node
	if s.clusterDisabled {
		err := s.cluster.setStatic(s.hosts)
		if err != nil {
			return nil, errors.Wrap(err, "setting cluster static")
		}
	}

	// Append the NodeID tag to stats.
	s.holder.Stats = s.holder.Stats.WithTags(fmt.Sprintf("NodeID:%s", s.nodeID))

	s.executor.Holder = s.holder
	s.executor.Node = node
	s.executor.Cluster = s.cluster
	s.executor.TranslateStore = s.holder.translateFile
	s.executor.MaxWritesPerRequest = s.maxWritesPerRequest
	s.cluster.broadcaster = s
	s.cluster.maxWritesPerRequest = s.maxWritesPerRequest
	s.holder.broadcaster = s

	err = s.cluster.setup()
	if err != nil {
		return nil, errors.Wrap(err, "setting up cluster")
	}

	return s, nil
}

// Open opens and initializes the server.
func (s *Server) Open() error {
	s.logger.Printf("open server")

	// Log startup
	err := s.holder.logStartup()
	if err != nil {
		log.Println(errors.Wrap(err, "logging startup"))
	}

	// Initialize id-key storage.
	if err := s.holder.translateFile.Open(); err != nil {
		return err
	}

	// Open Cluster management.
	if err := s.cluster.waitForStarted(); err != nil {
		return fmt.Errorf("opening Cluster: %v", err)
	}

	// Open holder.
	if err := s.holder.Open(); err != nil {
		return fmt.Errorf("opening Holder: %v", err)
	}
	if err := s.cluster.setNodeState(nodeStateReady); err != nil {
		return fmt.Errorf("setting nodeState: %v", err)
	}

	// Listen for joining nodes.
	// This needs to start after the Holder has opened so that nodes can join
	// the cluster without waiting for data to load on the coordinator. Before
	// this starts, the joins are queued up in the Cluster.joiningLeavingNodes
	// buffered channel.
	s.cluster.listenForJoins()

	s.syncer.Holder = s.holder
	s.syncer.Node = s.cluster.Node
	s.syncer.Cluster = s.cluster
	s.syncer.Closing = s.closing
	s.syncer.Stats = s.holder.Stats.WithTags("HolderSyncer")

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

	var errh error
	var errc error
	if s.cluster != nil {
		errc = s.cluster.close()
	}
	if s.holder != nil {
		errh = s.holder.Close()
	}
	// prefer to return holder error over cluster
	// error. This order is somewhat arbitrary. It would be better if we had
	// some way to combine all the errors, but probably not important enough to
	// warrant the extra complexity.
	if errh != nil {
		return errors.Wrap(errh, "closing holder")
	}
	return errors.Wrap(errc, "closing cluster")
}

// loadNodeID gets NodeID from disk, or creates a new value.
// If server.NodeID is already set, a new ID is not created.
func (s *Server) loadNodeID() string {
	if s.nodeID != "" {
		return s.nodeID
	}
	nodeID, err := s.holder.loadNodeID()
	if err != nil {
		s.logger.Printf("loading NodeID: %v", err)
		return s.nodeID
	}
	return nodeID
}

// SyncData manually invokes the anti entropy process which makes sure that this
// node has the data from all replicas across the cluster.
func (s *Server) SyncData() error {
	return errors.Wrap(s.syncer.SyncHolder(), "syncing holder")
}

func (s *Server) monitorAntiEntropy() {
	if s.antiEntropyInterval == 0 {
		return // anti entropy disabled
	}
	s.cluster.initializeAntiEntropy()

	ticker := time.NewTicker(s.antiEntropyInterval)
	defer ticker.Stop()

	s.logger.Printf("holder sync monitor initializing (%s interval)", s.antiEntropyInterval)

	// Initialize syncer with local holder and remote client.
	for {
		// Wait for tick or a close.
		select {
		case <-s.closing:
			return
		case <-s.cluster.abortAntiEntropyCh: // receive here so we don't block resizing
			continue
		case <-ticker.C:
			s.holder.Stats.Count("AntiEntropy", 1, 1.0)
		}
		t := time.Now()
		if s.cluster.State() == ClusterStateResizing {
			continue // don't launch anti-entropy during resize.
			// the cluster sets its state to resizing and *then* sends to
			// abortAntiEntropyCh before starting to resize
		}
		// Sync holders.
		s.logger.Printf("holder sync beginning")
		if err := s.syncer.SyncHolder(); err != nil {
			s.logger.Printf("holder sync error: err=%s", err)
			continue
		}

		// Record successful sync in log.
		s.logger.Printf("holder sync complete")
		dif := time.Since(t)
		s.holder.Stats.Histogram("AntiEntropyDuration", float64(dif), 1.0)

		// Drain tick channel since we just finished anti-entropy. If the AE
		// process took a long time, we don't want them to pile up on each
		// other.
		for {
			select {
			case <-ticker.C:
				continue
			default:
			}
			break
		}
	}
}

// receiveMessage represents an implementation of BroadcastHandler.
func (s *Server) receiveMessage(m Message) error {
	switch obj := m.(type) {
	case *CreateShardMessage:
		f := s.holder.Field(obj.Index, obj.Field)
		if f == nil {
			return fmt.Errorf("Local field not found: %s/%s", obj.Index, obj.Field)
		}
		f.addRemoteAvailableShards(roaring.NewBitmap(obj.Shard))
	case *CreateIndexMessage:
		opt := obj.Meta
		_, err := s.holder.CreateIndex(obj.Index, *opt)
		if err != nil {
			return err
		}
	case *DeleteIndexMessage:
		if err := s.holder.DeleteIndex(obj.Index); err != nil {
			return err
		}
	case *CreateFieldMessage:
		idx := s.holder.Index(obj.Index)
		if idx == nil {
			return fmt.Errorf("Local Index not found: %s", obj.Index)
		}
		opt := obj.Meta
		_, err := idx.createField(obj.Field, *opt)
		if err != nil {
			return err
		}
	case *DeleteFieldMessage:
		idx := s.holder.Index(obj.Index)
		if err := idx.DeleteField(obj.Field); err != nil {
			return err
		}
	case *CreateViewMessage:
		f := s.holder.Field(obj.Index, obj.Field)
		if f == nil {
			return fmt.Errorf("Local Field not found: %s", obj.Field)
		}
		_, _, err := f.createViewIfNotExistsBase(obj.View)
		if err != nil {
			return err
		}
	case *DeleteViewMessage:
		f := s.holder.Field(obj.Index, obj.Field)
		if f == nil {
			return fmt.Errorf("Local Field not found: %s", obj.Field)
		}
		err := f.deleteView(obj.View)
		if err != nil {
			return err
		}
	case *ClusterStatus:
		err := s.cluster.mergeClusterStatus(obj)
		if err != nil {
			return err
		}
	case *ResizeInstruction:
		err := s.cluster.followResizeInstruction(obj)
		if err != nil {
			return err
		}
	case *ResizeInstructionComplete:
		err := s.cluster.markResizeInstructionComplete(obj)
		if err != nil {
			return err
		}
	case *SetCoordinatorMessage:
		s.cluster.setCoordinator(obj.New)
	case *UpdateCoordinatorMessage:
		s.cluster.updateCoordinator(obj.New)
	case *NodeStateMessage:
		err := s.cluster.receiveNodeState(obj.NodeID, obj.State)
		if err != nil {
			return err
		}
	case *RecalculateCaches:
		s.holder.recalculateCaches()
	case *NodeEvent:
		s.cluster.ReceiveEvent(obj)
	case *NodeStatus:
		s.handleRemoteStatus(obj)
	}

	return nil
}

// SendSync represents an implementation of Broadcaster.
func (s *Server) SendSync(m Message) error {
	var eg errgroup.Group
	msg, err := s.serializer.Marshal(m)
	if err != nil {
		return fmt.Errorf("marshaling message: %v", err)
	}
	msg = append([]byte{getMessageType(m)}, msg...)
	for _, node := range s.cluster.nodes {
		node := node
		s.logger.Printf("SendSync to: %s", node.URI)
		// Don't forward the message to ourselves.
		if s.uri == node.URI {
			continue
		}

		eg.Go(func() error {
			return s.defaultClient.SendMessage(context.Background(), &node.URI, msg)
		})
	}

	return eg.Wait()
}

// SendAsync represents an implementation of Broadcaster.
func (s *Server) SendAsync(m Message) error {
	return ErrNotImplemented
}

// SendTo represents an implementation of Broadcaster.
func (s *Server) SendTo(to *Node, m Message) error {
	s.logger.Printf("SendTo: %s", to.URI)
	msg, err := s.serializer.Marshal(m)
	if err != nil {
		return fmt.Errorf("marshaling message: %v", err)
	}
	msg = append([]byte{getMessageType(m)}, msg...)
	return s.defaultClient.SendMessage(context.Background(), &to.URI, msg)
}

// node returns the pilosa.node object. It is used by membership protocols to
// get this node's name(ID), location(URI), and coordinator status.
func (s *Server) node() Node {
	return *s.cluster.Node
}

// handleRemoteStatus receives incoming NodeStatus from remote nodes.
func (s *Server) handleRemoteStatus(pb Message) {
	// Ignore NodeStatus messages until the cluster is in a Normal state.
	if s.cluster.State() != ClusterStateNormal {
		return
	}

	go func() {
		// Make sure the holder has opened.
		<-s.holder.opened

		err := s.mergeRemoteStatus(pb.(*NodeStatus))
		if err != nil {
			s.logger.Printf("merge remote status: %s", err)
		}
	}()
}

func (s *Server) mergeRemoteStatus(ns *NodeStatus) error {
	// Ignore status updates from self.
	if s.nodeID == ns.Node.ID {
		return nil
	}

	// Sync schema.
	if err := s.holder.applySchema(ns.Schema); err != nil {
		return errors.Wrap(err, "applying schema")
	}

	// Sync available shards.
	for _, is := range ns.Indexes {
		for _, fs := range is.Fields {
			f := s.holder.Field(is.Name, fs.Name)

			// if we don't know about an field locally, log a error because
			// fields should be created and synced prior to shard creation
			if f == nil {
				s.logger.Printf("Local Field not found: %s/%s", is.Name, fs.Name)
				continue
			}
			f.addRemoteAvailableShards(fs.AvailableShards)
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
	s.diagnostics.Set("Host", s.uri.Host)
	s.diagnostics.Set("Cluster", strings.Join(s.cluster.nodeIDs(), ","))
	s.diagnostics.Set("NumNodes", len(s.cluster.nodes))
	s.diagnostics.Set("NumCPU", runtime.NumCPU())
	s.diagnostics.Set("NodeID", s.nodeID)
	s.diagnostics.Set("ClusterID", s.cluster.id)
	s.diagnostics.EnrichWithOSInfo()

	// Flush the diagnostics metrics at startup, then on each tick interval
	flush := func() {
		openFiles, err := countOpenFiles()
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
			s.holder.Stats.Count("garbage_collection", 1, 1.0)
		case <-ticker.C:
		}

		// Record the number of go routines.
		s.holder.Stats.Gauge("goroutines", float64(runtime.NumGoroutine()), 1.0)

		openFiles, err := countOpenFiles()
		// Open File handles.
		if err == nil {
			s.holder.Stats.Gauge("OpenFiles", float64(openFiles), 1.0)
		}

		// Runtime memory metrics.
		runtime.ReadMemStats(&m)
		s.holder.Stats.Gauge("HeapAlloc", float64(m.HeapAlloc), 1.0)
		s.holder.Stats.Gauge("HeapInuse", float64(m.HeapInuse), 1.0)
		s.holder.Stats.Gauge("StackInuse", float64(m.StackInuse), 1.0)
		s.holder.Stats.Gauge("Mallocs", float64(m.Mallocs), 1.0)
		s.holder.Stats.Gauge("Frees", float64(m.Frees), 1.0)
	}
}

// countOpenFiles on operating systems that support lsof.
func countOpenFiles() (int, error) {
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
		return 0, errors.New("countOpenFiles() on Windows is not supported")
	default:
		return 0, errors.New("countOpenFiles() on this OS is not supported")
	}
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
