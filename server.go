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
	"encoding/json"
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

	uuid "github.com/satori/go.uuid"

	"github.com/molecula/featurebase/v2/disco"
	"github.com/molecula/featurebase/v2/logger"
	pnet "github.com/molecula/featurebase/v2/net"
	rbfcfg "github.com/molecula/featurebase/v2/rbf/cfg"
	"github.com/molecula/featurebase/v2/roaring"
	"github.com/molecula/featurebase/v2/sql2"
	"github.com/molecula/featurebase/v2/stats"
	"github.com/molecula/featurebase/v2/storage"
	"github.com/molecula/featurebase/v2/topology"
	"github.com/molecula/featurebase/v2/vprint"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"

	_ "github.com/lib/pq"
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
	holder           *Holder
	cluster          *cluster
	diagnostics      *diagnosticsCollector
	executor         *executor
	executorPoolSize int
	serializer       Serializer

	// Distributed Consensus
	disCo     disco.DisCo
	stator    disco.Stator
	metadator disco.Metadator
	resizer   disco.Resizer
	noder     topology.Noder
	sharder   disco.Sharder
	schemator disco.Schemator

	// External
	systemInfo    SystemInfo
	gcNotifier    GCNotifier
	logger        logger.Logger
	snapshotQueue SnapshotQueue

	nodeID              string
	uri                 pnet.URI
	grpcURI             pnet.URI
	antiEntropyInterval time.Duration
	metricInterval      time.Duration
	diagnosticInterval  time.Duration
	maxWritesPerRequest int
	confirmDownSleep    time.Duration
	confirmDownRetries  int
	syncer              holderSyncer
	maxQueryMemory      int64

	translationSyncer      TranslationSyncer
	resetTranslationSyncCh chan struct{}
	// HolderConfig stashes server options that are really Holder options.
	holderConfig *HolderConfig

	defaultClient InternalClient
	dataDir       string

	// Threshold for logging long-running queries
	longQueryTime      time.Duration
	queryHistoryLength int
}

// Holder returns the holder for server.
func (s *Server) Holder() *Holder {
	return s.holder
}

// ServerOption is a functional option type for pilosa.Server
type ServerOption func(s *Server) error

// OptServerLogger is a functional option on Server
// used to set the logger.
func OptServerLogger(l logger.Logger) ServerOption {
	return func(s *Server) error {
		s.logger = l
		s.holderConfig.Logger = l
		return nil
	}
}

// OptServerReplicaN is a functional option on Server
// used to set the number of replicas.
func OptServerReplicaN(n int) ServerOption {
	return func(s *Server) error {
		s.cluster.ReplicaN = n
		return nil
	}
}

// OptServerDataDir is a functional option on Server
// used to set the data directory.
func OptServerDataDir(dir string) ServerOption {
	return func(s *Server) error {
		s.dataDir = dir
		return nil
	}
}

// OptServerAntiEntropyInterval is a functional option on Server
// used to set the anti-entropy interval.
func OptServerAntiEntropyInterval(interval time.Duration) ServerOption {
	return func(s *Server) error {
		s.antiEntropyInterval = interval
		return nil
	}
}

// OptServerLongQueryTime is a functional option on Server
// used to set long query duration.
func OptServerLongQueryTime(dur time.Duration) ServerOption {
	return func(s *Server) error {
		s.longQueryTime = dur
		return nil
	}
}

// OptServerMaxWritesPerRequest is a functional option on Server
// used to set the maximum number of writes allowed per request.
func OptServerMaxWritesPerRequest(n int) ServerOption {
	return func(s *Server) error {
		s.maxWritesPerRequest = n
		return nil
	}
}

// OptServerMetricInterval is a functional option on Server
// used to set the interval between metric samples.
func OptServerMetricInterval(dur time.Duration) ServerOption {
	return func(s *Server) error {
		s.metricInterval = dur
		return nil
	}
}

// OptServerSystemInfo is a functional option on Server
// used to set the system information source.
func OptServerSystemInfo(si SystemInfo) ServerOption {
	return func(s *Server) error {
		s.systemInfo = si
		return nil
	}
}

// OptServerGCNotifier is a functional option on Server
// used to set the garbage collection notification source.
func OptServerGCNotifier(gcn GCNotifier) ServerOption {
	return func(s *Server) error {
		s.gcNotifier = gcn
		return nil
	}
}

// OptServerInternalClient is a functional option on Server
// used to set the implementation of InternalClient.
func OptServerInternalClient(c InternalClient) ServerOption {
	return func(s *Server) error {
		s.defaultClient = c
		s.cluster.InternalClient = c
		return nil
	}
}

func OptServerExecutorPoolSize(size int) ServerOption {
	return func(s *Server) error {
		s.executorPoolSize = size
		return nil
	}
}

// OptServerPrimaryTranslateStore has been deprecated.
func OptServerPrimaryTranslateStore(store TranslateStore) ServerOption {
	return func(s *Server) error {
		s.logger.Infof("DEPRECATED: OptServerPrimaryTranslateStore")
		return nil
	}
}

// OptServerStatsClient is a functional option on Server
// used to specify the stats client.
func OptServerStatsClient(sc stats.StatsClient) ServerOption {
	return func(s *Server) error {
		s.holderConfig.StatsClient = sc
		return nil
	}
}

// OptServerDiagnosticsInterval is a functional option on Server
// used to specify the duration between diagnostic checks.
func OptServerDiagnosticsInterval(dur time.Duration) ServerOption {
	return func(s *Server) error {
		s.diagnosticInterval = dur
		return nil
	}
}

// OptServerNodeDownRetries is a functional option on Server
// used to specify the retries and sleep duration for node down
// checks.
func OptServerNodeDownRetries(retries int, sleep time.Duration) ServerOption {
	return func(s *Server) error {
		s.confirmDownRetries = retries
		s.confirmDownSleep = sleep
		return nil
	}
}

// OptServerURI is a functional option on Server
// used to set the server URI.
func OptServerURI(uri *pnet.URI) ServerOption {
	return func(s *Server) error {
		s.uri = *uri
		return nil
	}
}

// OptServerGRPCURI is a functional option on Server
// used to set the server gRPC URI.
func OptServerGRPCURI(uri *pnet.URI) ServerOption {
	return func(s *Server) error {
		s.grpcURI = *uri
		return nil
	}
}

// OptServerClusterName sets the human-readable cluster name.
func OptServerClusterName(name string) ServerOption {
	return func(s *Server) error {
		s.cluster.Name = name
		return nil
	}
}

// OptServerSerializer is a functional option on Server
// used to set the serializer.
func OptServerSerializer(ser Serializer) ServerOption {
	return func(s *Server) error {
		s.serializer = ser
		return nil
	}
}

// OptServerNodeID is a functional option on Server
// used to set the server node ID.
func OptServerNodeID(nodeID string) ServerOption {
	return func(s *Server) error {
		s.nodeID = nodeID
		return nil
	}
}

// OptServerClusterHasher is a functional option on Server
// used to specify the consistent hash algorithm for data
// location within the cluster.
func OptServerClusterHasher(h topology.Hasher) ServerOption {
	return func(s *Server) error {
		s.cluster.Hasher = h
		return nil
	}
}

// OptServerOpenTranslateStore is a functional option on Server
// used to specify the translation data store type.
func OptServerOpenTranslateStore(fn OpenTranslateStoreFunc) ServerOption {
	return func(s *Server) error {
		s.holderConfig.OpenTranslateStore = fn
		return nil
	}
}

// OptServerOpenIDAllocator is a functional option on Server
// used to specify the ID allocator data store type.
// Except not really (because there's only one at this time).
func OptServerOpenIDAllocator(fn OpenIDAllocatorFunc) ServerOption {
	return func(s *Server) error {
		s.holderConfig.OpenIDAllocator = fn
		return nil
	}
}

// OptServerOpenTranslateReader is a functional option on Server
// used to specify the remote translation data reader.
func OptServerOpenTranslateReader(fn OpenTranslateReaderFunc) ServerOption {
	return func(s *Server) error {
		s.holderConfig.OpenTranslateReader = fn
		return nil
	}
}

// OptServerStorageConfig is a functional option on Server used to specify the
// transactional-storage backend to use, resulting in RoaringTx, RbfTx,
// BadgerTx, or a blueGreen* Tx being used for all Tx interface calls.
func OptServerStorageConfig(cfg *storage.Config) ServerOption {
	return func(s *Server) error {
		s.holderConfig.StorageConfig = cfg
		return nil
	}
}

// OptServerRowcacheOn is a functional option on Server
// used to turn on the row cache.
func OptServerRowcacheOn(rowcacheOn bool) ServerOption {
	return func(s *Server) error {
		s.holderConfig.RowcacheOn = rowcacheOn
		return nil
	}
}

// OptServerRBFConfig conveys the RBF flags to the Holder.
func OptServerRBFConfig(cfg *rbfcfg.Config) ServerOption {
	return func(s *Server) error {
		s.holderConfig.RBFConfig = cfg
		return nil
	}
}

// OptServerQueryHistoryLength is a functional option on Server
// used to specify the length of the query history buffer that maintains
// the information returned at /query-history.
func OptServerQueryHistoryLength(length int) ServerOption {
	return func(s *Server) error {
		s.queryHistoryLength = length
		return nil
	}
}

// OptServerMaxQueryMemory sets the memory used per Extract() and SELECT query.
func OptServerMaxQueryMemory(v int64) ServerOption {
	return func(s *Server) error {
		s.maxQueryMemory = v
		return nil
	}
}

// OptServerDisCo is a functional option on Server
// used to set the Distributed Consensus implementation.
func OptServerDisCo(disCo disco.DisCo,
	stator disco.Stator,
	metadator disco.Metadator,
	resizer disco.Resizer,
	noder topology.Noder,
	sharder disco.Sharder,
	schemator disco.Schemator) ServerOption {

	return func(s *Server) error {
		s.disCo = disCo
		s.stator = stator
		s.metadator = metadator
		s.resizer = resizer
		s.noder = noder
		s.sharder = sharder
		s.schemator = schemator
		return nil
	}
}

// OptServerLookupDB configures a connection to an external postgres database for ExternalLookup queries.
func OptServerLookupDB(dsn string) ServerOption {
	return func(s *Server) error {
		s.holderConfig.LookupDBDSN = dsn
		return nil
	}
}

// NewServer returns a new instance of Server.
func NewServer(opts ...ServerOption) (*Server, error) {
	cluster := newCluster()

	s := &Server{
		closing:       make(chan struct{}),
		cluster:       cluster,
		diagnostics:   newDiagnosticsCollector(defaultDiagnosticServer),
		systemInfo:    newNopSystemInfo(),
		defaultClient: nopInternalClient{},

		gcNotifier: NopGCNotifier,

		antiEntropyInterval: 0,
		metricInterval:      0,
		diagnosticInterval:  0,

		disCo:      disco.NopDisCo,
		stator:     disco.NopStator,
		metadator:  disco.NopMetadator,
		resizer:    disco.NopResizer,
		noder:      topology.NewEmptyLocalNoder(),
		sharder:    disco.NopSharder,
		schemator:  disco.NopSchemator,
		serializer: NopSerializer,

		confirmDownRetries: defaultConfirmDownRetries,
		confirmDownSleep:   defaultConfirmDownSleep,

		resetTranslationSyncCh: make(chan struct{}, 1),

		logger: logger.NopLogger,
	}
	s.cluster.InternalClient = s.defaultClient

	s.translationSyncer = newActiveTranslationSyncer(s.resetTranslationSyncCh)
	s.cluster.translationSyncer = s.translationSyncer

	s.diagnostics.server = s
	s.holderConfig = DefaultHolderConfig()
	s.holderConfig.TranslationSyncer = s.translationSyncer
	s.holderConfig.Logger = s.logger

	for _, opt := range opts {
		err := opt(s)
		if err != nil {
			return nil, errors.Wrap(err, "applying option")
		}
	}
	s.holderConfig.AntiEntropyInterval = s.antiEntropyInterval

	memTotal, err := s.systemInfo.MemTotal()
	if err != nil {
		return nil, errors.Wrap(err, "mem total")
	}

	// Default memory to 20% of total.
	maxQueryMemory := s.maxQueryMemory
	if maxQueryMemory == 0 {
		maxQueryMemory = int64(float64(memTotal) * .20)
	}

	// set up executor after server opts have been processed
	executorOpts := []executorOption{
		optExecutorInternalQueryClient(s.defaultClient),
		optExecutorMaxMemory(maxQueryMemory),
	}
	if s.executorPoolSize > 0 {
		executorOpts = append(executorOpts, optExecutorWorkerPoolSize(s.executorPoolSize))
	}
	s.executor = newExecutor(executorOpts...)

	path, err := expandDirName(s.dataDir)
	if err != nil {
		return nil, err
	}
	s.holder = NewHolder(path, s.holderConfig)
	s.holder.Stats.SetLogger(s.logger)
	s.holder.Logger.Infof("RowCacheOn: %v", s.holderConfig.RowcacheOn)
	cwd, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	s.holder.Logger.Infof("cwd: %v", cwd)
	s.holder.Logger.Infof("cmd line: %v", strings.Join(os.Args, " "))

	s.cluster.Path = path
	s.cluster.logger = s.logger
	s.cluster.holder = s.holder
	s.cluster.disCo = s.disCo
	s.cluster.stator = s.stator
	s.cluster.resizer = s.resizer
	s.cluster.noder = s.noder
	s.cluster.sharder = s.sharder

	// Append the NodeID tag to stats.
	s.holder.Stats = s.holder.Stats.WithTags(fmt.Sprintf("node_id:%s", s.nodeID))

	s.executor.Holder = s.holder
	s.executor.Cluster = s.cluster
	s.executor.MaxWritesPerRequest = s.maxWritesPerRequest
	s.cluster.broadcaster = s
	s.cluster.maxWritesPerRequest = s.maxWritesPerRequest
	s.cluster.confirmDownRetries = s.confirmDownRetries
	s.cluster.confirmDownSleep = s.confirmDownSleep
	s.holder.broadcaster = s
	s.holder.schemator = s.schemator
	s.holder.sharder = s.sharder
	s.holder.serializer = s.serializer
	return s, nil
}

func (s *Server) InternalClient() InternalClient {
	return s.defaultClient
}

func (s *Server) GRPCURI() pnet.URI {
	return s.grpcURI
}

// UpAndDown brings the server up minimally and shuts it down
// again; basically, it exists for testing holder open and close.
func (s *Server) UpAndDown() error {
	s.logger.Infof("open server. PID %v", os.Getpid())

	// Log startup
	err := s.holder.logStartup()
	if err != nil {
		log.Println(errors.Wrap(err, "logging startup"))
	}

	// Open holder.
	if err := s.holder.Open(); err != nil {
		return errors.Wrap(err, "opening Holder")
	}

	errh := s.holder.Close()
	if errh != nil {
		return errors.Wrap(errh, "closing holder")
	}

	return nil
}

// Open opens and initializes the server.
func (s *Server) Open() error {
	s.logger.Infof("open server. PID %v", os.Getpid())

	if s.holder.NeedsSnapshot() {
		// Start background monitoring.
		s.snapshotQueue = newSnapshotQueue(10, 2, s.logger)
	} else {
		s.snapshotQueue = defaultSnapshotQueue //TODO (twg) rethink this
	}

	// Log startup
	err := s.holder.logStartup()
	if err != nil {
		log.Println(errors.Wrap(err, "logging startup"))
	}

	// Start DisCo.
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	initState, err := s.disCo.Start(ctx)
	if err != nil {
		return errors.Wrap(err, "starting DisCo")
	}

	// Set node ID.
	s.nodeID = s.disCo.ID()

	node := &topology.Node{
		ID:        s.nodeID,
		URI:       s.uri,
		GRPCURI:   s.grpcURI,
		State:     disco.NodeStateUnknown,
		IsPrimary: s.IsPrimary(),
	}

	// Set metadata for this node.
	data, err := json.Marshal(node)
	if err != nil {
		return errors.Wrap(err, "marshaling json metadata")
	}
	if err := s.metadator.SetMetadata(context.Background(), data); err != nil {
		return errors.Wrap(err, "setting metadata")
	}

	s.cluster.Node = node
	s.executor.Node = node

	// Set up the holderSyncer.
	s.syncer.Holder = s.holder
	s.syncer.Node = node
	s.syncer.Cluster = s.cluster
	s.syncer.Closing = s.closing
	s.syncer.Stats = s.holder.Stats.WithTags("component:HolderSyncer")

	// Start background process listening for translation
	// sync resets.
	s.wg.Add(1)
	go func() { defer s.wg.Done(); s.monitorResetTranslationSync() }()
	go func() { _ = s.translationSyncer.Reset() }()

	// Open holder.
	func() {
		s.holder.startMsgsMu.Lock()
		defer s.holder.startMsgsMu.Unlock()

		s.holder.startMsgs = []Message{}
	}()
	if err := s.holder.Open(); err != nil {
		return errors.Wrap(err, "opening Holder")
	}
	// bring up the background tasks for the holder.
	s.holder.SnapshotQueue = s.snapshotQueue
	s.holder.Activate()
	// if we joined existing cluster then broadcast "resize on add" message
	if initState == disco.InitialClusterStateExisting {
		if err := s.cluster.addNode(s.nodeID); err != nil {
			return errors.Wrap(err, "adding a node to the existing cluster")
		}
	}

	if err := s.stator.Started(context.Background()); err != nil {
		return errors.Wrap(err, "setting nodeState")
	}

	s.wg.Add(3)
	go func() { defer s.wg.Done(); s.monitorAntiEntropy() }()
	go func() { defer s.wg.Done(); s.monitorRuntime() }()
	go func() { defer s.wg.Done(); s.monitorDiagnostics() }()

	toSend := func() []Message {
		s.holder.startMsgsMu.Lock()
		defer s.holder.startMsgsMu.Unlock()

		toSend := s.holder.startMsgs
		s.holder.startMsgs = nil
		return toSend
	}()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer cancel()
			select {
			case <-s.closing:
			case <-ctx.Done():
			}
		}()

		timer := time.NewTimer(0)
		defer timer.Stop()
		if !timer.Stop() {
			<-timer.C
		}
		for {
			state, err := s.stator.ClusterState(ctx)
			if err != nil {
				s.logger.Printf("failed to check cluster state: %v", err)
				timer.Reset(time.Second)
				select {
				case <-s.closing:
					return
				case <-timer.C:
					continue
				}
			}
			switch state {
			case disco.ClusterStateStarting, disco.ClusterStateUnknown, disco.ClusterStateDown:
				timer.Reset(time.Second)
				select {
				case <-s.closing:
					return
				case <-timer.C:
					continue
				}
			}
			break
		}

		start := time.Now()
		prevMsg := start
		s.logger.Printf("start initial cluster state sync")
		for i := range toSend {
			for {
				err := s.holder.broadcaster.SendSync(toSend[i])
				if err != nil {
					s.logger.Printf("failed to broadcast startup cluster message (trying again in a bit): %v", err)
					timer.Reset(time.Second)
					select {
					case <-s.closing:
						return
					case <-timer.C:
						continue
					}
				}
				break
			}

			if now := time.Now(); now.Sub(prevMsg) > time.Second {
				progressRatio := float64(i+1) / float64(len(toSend))
				remainingRatio := 1 - progressRatio
				timeRemaining := time.Duration(float64(now.Sub(prevMsg)) * (remainingRatio / progressRatio))
				s.logger.Printf("synced %d/%d messages (%.2f%% complete; %s remaining)", i+1, len(toSend), 100*progressRatio, timeRemaining)
				prevMsg = now
			}
		}
		s.logger.Printf("completed initial cluster state sync in %s", time.Since(start).String())
	}()

	return nil
}

// Close closes the server and waits for it to shutdown.
func (s *Server) Close() error {
	select {
	case <-s.closing:
		return nil
	default:
		errE := s.executor.Close()

		// Notify goroutines to stop.
		close(s.closing)
		s.wg.Wait()
		var errh, errd error
		var errhs error
		var errc error

		if s.cluster != nil {
			errc = s.cluster.close()
		}
		errhs = s.syncer.stopTranslationSync()
		if s.disCo != nil {
			errd = s.disCo.Close()
		}
		if s.holder != nil {
			errh = s.holder.Close()
		}
		if s.snapshotQueue != nil {
			s.holder.SnapshotQueue = nil
			s.snapshotQueue.Stop()
			s.snapshotQueue = nil
		}

		// prefer to return holder error over cluster
		// error. This order is somewhat arbitrary. It would be better if we had
		// some way to combine all the errors, but probably not important enough to
		// warrant the extra complexity.
		if errh != nil {
			return errors.Wrap(errh, "closing holder")
		}
		if errhs != nil {
			return errors.Wrap(errhs, "terminating holder translation sync")
		}
		if errc != nil {
			return errors.Wrap(errc, "closing cluster")
		}
		if errd != nil {
			return errors.Wrap(errd, "closing disco")
		}
		return errors.Wrap(errE, "closing executor")
	}
}

// NodeID returns the server's node id.
func (s *Server) NodeID() string { return s.nodeID }

// SyncData manually invokes the anti entropy process which makes sure that this
// node has the data from all replicas across the cluster.
func (s *Server) SyncData() error {
	return errors.Wrap(s.syncer.SyncHolder(), "syncing holder")
}

// monitorResetTranslationSync is a background process which
// listens for events indicating the need to reset the translation
// sync processes.
func (s *Server) monitorResetTranslationSync() {
	s.logger.Infof("holder translation sync monitor initializing")
	for {
		// Wait for a reset or a close.
		select {
		case <-s.closing:
			return
		case <-s.resetTranslationSyncCh:
			s.logger.Infof("holder translation sync beginning")
			s.wg.Add(1)
			go func() {
				// Obtaining this lock ensures that there is only
				// one instance of resetTranslationSync() running
				// at once.
				s.syncer.mu.Lock()
				defer s.syncer.mu.Unlock()
				defer s.wg.Done()
				if err := s.syncer.resetTranslationSync(); err != nil {
					s.logger.Errorf("holder translation sync error: err=%s", err)
				}
			}()
		}
	}
}

func (s *Server) monitorAntiEntropy() {
	if s.antiEntropyInterval == 0 || s.cluster.ReplicaN <= 1 {
		return // anti entropy disabled
	}
	s.cluster.initializeAntiEntropy()

	ticker := time.NewTicker(s.antiEntropyInterval)
	defer ticker.Stop()

	s.logger.Infof("holder sync monitor initializing (%s interval)", s.antiEntropyInterval)

	// Initialize syncer with local holder and remote client.
	for {
		// Wait for tick or a close.
		select {
		case <-s.closing:
			return
		case <-s.cluster.abortAntiEntropyCh: // receive here so we don't block resizing
			continue
		case <-ticker.C:
			s.holder.Stats.Count(MetricAntiEntropy, 1, 1.0)
		}
		t := time.Now()

		state, err := s.cluster.State()
		if err != nil {
			s.logger.Printf("cluster state error: err=%s", err)
			continue
		}

		if state == disco.ClusterStateResizing {
			continue // don't launch anti-entropy during resize.
			// the cluster sets its state to resizing and *then* sends to
			// abortAntiEntropyCh before starting to resize
		}

		// Sync holders.
		s.logger.Infof("holder sync beginning")
		s.cluster.muAntiEntropy.Lock()
		if err := s.syncer.SyncHolder(); err != nil {
			s.cluster.muAntiEntropy.Unlock()
			s.logger.Errorf("holder sync error: err=%s", err)
			continue
		}
		s.cluster.muAntiEntropy.Unlock()

		// Record successful sync in log.
		s.logger.Infof("holder sync complete")
		dif := time.Since(t)
		s.holder.Stats.Timing(MetricAntiEntropyDurationSeconds, dif, 1.0)

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
			return fmt.Errorf("local field not found: %s/%s", obj.Index, obj.Field)
		}
		if err := f.AddRemoteAvailableShards(roaring.NewBitmap(obj.Shard)); err != nil {
			return errors.Wrap(err, "adding remote available shards")
		}

	case *CreateIndexMessage:
		if _, err := s.holder.LoadIndex(obj.Index); err != nil {
			return err
		}

	case *DeleteIndexMessage:
		if err := s.holder.DeleteIndex(obj.Index); err != nil {
			return err
		}

	case *CreateFieldMessage:
		if _, err := s.holder.LoadField(obj.Index, obj.Field); err != nil {
			return err
		}

	case *DeleteFieldMessage:
		idx := s.holder.Index(obj.Index)
		if err := idx.DeleteField(obj.Field); err != nil {
			return err
		}

	case *DeleteAvailableShardMessage:
		f := s.holder.Field(obj.Index, obj.Field)
		if err := f.RemoveAvailableShard(obj.ShardID); err != nil {
			return err
		}

	case *CreateViewMessage:
		if _, err := s.holder.LoadView(obj.Index, obj.Field, obj.View); err != nil {
			return err
		}

	case *DeleteViewMessage:
		f := s.holder.Field(obj.Index, obj.Field)
		if f == nil {
			return fmt.Errorf("local field not found: %s", obj.Field)
		}
		err := f.deleteView(obj.View)
		if err != nil {
			return err
		}

	case *ResizeNodeMessage:
		switch obj.Action {
		case resizeJobActionRemove:
			if err := s.cluster.resizeNodeOnRemove(obj.NodeID); err != nil {
				return errors.Wrapf(err, "resizing node %s on remove %s", s.cluster.disCo.ID(), obj.NodeID)
			}

		case resizeJobActionAdd:
			if err := s.cluster.resizeNodeOnAdd(obj.NodeID); err != nil {
				return errors.Wrapf(err, "resizing node %s on remove %s", s.cluster.disCo.ID(), obj.NodeID)
			}

		default:
			return fmt.Errorf("incorrect resizing node action: %s", obj.Action)
		}

	case *ResizeInstruction:
		err := s.cluster.followResizeInstruction(context.Background(), obj)
		if err != nil {
			return err
		}

	case *ResizeAbortMessage:
		err := s.cluster.resizeAbort()
		if err != nil {
			return err
		}

	case *RecalculateCaches:
		s.holder.recalculateCaches()

	case *LoadSchemaMessage:
		err := s.holder.LoadSchema()
		if err != nil {
			return errors.Wrapf(err, "handling load schema message: %v", obj)
		}

	case *NodeStatus:
		s.handleRemoteStatus(obj)

	case *TransactionMessage:
		err := s.handleTransactionMessage(obj)
		if err != nil {
			return errors.Wrapf(err, "handling transaction message: %v", obj)
		}
	}

	return nil
}

func (s *Server) handleTransactionMessage(tm *TransactionMessage) error {
	mtrns := tm.Transaction // message transaction
	ctx := context.Background()
	switch tm.Action {
	case TRANSACTION_START:
		_, err := s.StartTransaction(ctx, mtrns.ID, mtrns.Timeout, mtrns.Exclusive, true)
		if err != nil {
			return errors.Wrap(err, "starting transaction locally")
		}
	case TRANSACTION_FINISH:
		_, err := s.FinishTransaction(ctx, mtrns.ID, true)
		if err != nil {
			return errors.Wrap(err, "finishing transaction locally")
		}
	case TRANSACTION_VALIDATE:
		trns, err := s.GetTransaction(ctx, mtrns.ID, true)
		if err != nil {
			return errors.Wrap(err, "getting local transaction to validate")
		}
		return CompareTransactions(mtrns, trns)
	default:
		return errors.Errorf("unknown transaction action: '%s'", tm.Action)
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

	for _, node := range s.cluster.Nodes() {
		node := node
		uri := node.URI // URI is a struct value

		// Don't forward the message to ourselves.
		if s.uri == uri {
			continue
		}

		eg.Go(func() error {
			return s.defaultClient.SendMessage(context.Background(), &uri, msg)
		})
	}

	return eg.Wait()
}

// SendAsync represents an implementation of Broadcaster.
func (s *Server) SendAsync(m Message) error {
	return ErrNotImplemented
}

// SendTo represents an implementation of Broadcaster.
func (s *Server) SendTo(node *topology.Node, m Message) error {
	msg, err := s.serializer.Marshal(m)
	if err != nil {
		return fmt.Errorf("marshaling message: %v", err)
	}
	msg = append([]byte{getMessageType(m)}, msg...)

	uri := node.URI // URI is a struct value

	return s.defaultClient.SendMessage(context.Background(), &uri, msg)
}

// node returns the pilosa.node object. It is used by membership protocols to
// get this node's name(ID), location(URI), and primary status.
func (s *Server) node() *topology.Node {
	return s.cluster.Node.Clone()
}

// handleRemoteStatus receives incoming NodeStatus from remote nodes.
func (s *Server) handleRemoteStatus(pb Message) {
	state, err := s.cluster.State()
	if err != nil {
		s.logger.Printf("getting cluster state: %s", err)
		return
	}

	// Ignore NodeStatus messages until the cluster is in a Normal state.
	if state != disco.ClusterStateNormal {
		return
	}

	go func() {
		// Make sure the holder has opened.
		s.holder.opened.Recv()

		err := s.mergeRemoteStatus(pb.(*NodeStatus))
		if err != nil {
			s.logger.Errorf("merge remote status: %s", err)
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

			// if we don't know about a field locally, log an error because
			// fields should be created and synced prior to shard creation
			if f == nil {
				s.logger.Errorf("local field not found: %s/%s", is.Name, fs.Name)
				continue
			}
			if err := f.AddRemoteAvailableShards(fs.AvailableShards); err != nil {
				return errors.Wrap(err, "adding remote available shards")
			}
		}
	}

	return nil
}

// IsPrimary returns if this node is primary right now or not.
func (s *Server) IsPrimary() bool {
	return s.nodeID == s.noder.PrimaryNodeID(s.cluster.Hasher)
}

// monitorDiagnostics periodically polls the Pilosa Indexes for cluster info.
func (s *Server) monitorDiagnostics() {
	// Do not send more than once a minute
	if s.diagnosticInterval < time.Minute {
		s.logger.Infof("diagnostics disabled")
		return
	}
	s.logger.Infof("Pilosa is currently configured to send small diagnostics reports to our team every %v. More information here: https://www.pilosa.com/docs/latest/administration/#diagnostics", s.diagnosticInterval)

	s.diagnostics.Logger = s.logger
	s.diagnostics.SetVersion(Version)
	s.diagnostics.Set("Host", s.uri.Host)
	s.diagnostics.Set("Cluster", strings.Join(s.cluster.nodeIDs(), ","))
	s.diagnostics.Set("NumNodes", len(s.cluster.noder.Nodes()))
	s.diagnostics.Set("NumCPU", runtime.NumCPU())
	s.diagnostics.Set("NodeID", s.nodeID)
	s.diagnostics.Set("ClusterID", s.cluster.id)
	s.diagnostics.EnrichWithCPUInfo()
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
		err = s.diagnostics.CheckVersion()
		if err != nil {
			s.logger.Errorf("can't check version: %v", err)
		}
		err = s.diagnostics.Flush()
		if err != nil {
			s.logger.Errorf("diagnostics error: %s", err)
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

	s.logger.Infof("runtime stats initializing (%s interval)", s.metricInterval)

	for {
		// Wait for tick or a close.
		select {
		case <-s.closing:
			return
		case <-s.gcNotifier.AfterGC():
			// GC just ran.
			s.holder.Stats.Count(MetricGarbageCollection, 1, 1.0)
		case <-ticker.C:
		}

		// Record the number of go routines.
		s.holder.Stats.Gauge(MetricGoroutines, float64(runtime.NumGoroutine()), 1.0)

		openFiles, err := countOpenFiles()
		// Open File handles.
		if err == nil {
			s.holder.Stats.Gauge(MetricOpenFiles, float64(openFiles), 1.0)
		}

		// Runtime memory metrics.
		runtime.ReadMemStats(&m)
		s.holder.Stats.Gauge(MetricHeapAlloc, float64(m.HeapAlloc), 1.0)
		s.holder.Stats.Gauge(MetricHeapInuse, float64(m.HeapInuse), 1.0)
		s.holder.Stats.Gauge(MetricStackInuse, float64(m.StackInuse), 1.0)
		s.holder.Stats.Gauge(MetricMallocs, float64(m.Mallocs), 1.0)
		s.holder.Stats.Gauge(MetricFrees, float64(m.Frees), 1.0)
	}
}

func (srv *Server) StartTransaction(ctx context.Context, id string, timeout time.Duration, exclusive bool, remote bool) (*Transaction, error) {
	snap := topology.NewClusterSnapshot(srv.cluster.noder, srv.cluster.Hasher, srv.cluster.partitionN)
	node := srv.node()
	if !remote && !snap.IsPrimaryFieldTranslationNode(node.ID) && len(srv.cluster.Nodes()) > 1 {
		return nil, ErrNodeNotPrimary
	}
	if remote && (snap.IsPrimaryFieldTranslationNode(node.ID) || len(srv.cluster.Nodes()) == 1) {
		return nil, errors.New("unexpected remote start call to primary or single node cluster")
	}

	if remote {
		return srv.holder.StartTransaction(ctx, id, timeout, exclusive)
	}

	// empty string id should generate an id
	if id == "" {
		id = uuid.NewV4().String()
	}
	trns, err := srv.holder.StartTransaction(ctx, id, timeout, exclusive)
	if err != nil {
		return trns, errors.Wrap(err, "starting transaction")
	}
	err = srv.SendSync(
		&TransactionMessage{
			Action:      TRANSACTION_START,
			Transaction: trns,
		})
	if err != nil {
		// try to clean up, but ignore errors
		_, errLocal := srv.holder.FinishTransaction(ctx, id)
		errBroadcast := srv.SendSync(
			&TransactionMessage{
				Action:      TRANSACTION_FINISH,
				Transaction: trns,
			},
		)
		if errLocal != nil || errBroadcast != nil {
			srv.logger.Errorf("error(s) while trying to clean up transaction which failed to start, local: %v, broadcast: %v",
				errLocal,
				errBroadcast,
			)
		}
		return trns, errors.Wrap(err, "broadcasting transaction start")
	}
	return trns, nil
}

func (srv *Server) FinishTransaction(ctx context.Context, id string, remote bool) (*Transaction, error) {
	snap := topology.NewClusterSnapshot(srv.cluster.noder, srv.cluster.Hasher, srv.cluster.partitionN)
	node := srv.node()
	if !remote && !snap.IsPrimaryFieldTranslationNode(node.ID) && len(srv.cluster.Nodes()) > 1 {
		return nil, ErrNodeNotPrimary
	}
	if remote && (snap.IsPrimaryFieldTranslationNode(node.ID) || len(srv.cluster.Nodes()) == 1) {
		return nil, errors.New("unexpected remote finish call to primary or single node cluster")
	}

	if remote {
		return srv.holder.FinishTransaction(ctx, id)
	}
	trns, err := srv.holder.FinishTransaction(ctx, id)
	if err != nil {
		return trns, errors.Wrap(err, "finishing transaction")
	}
	err = srv.SendSync(
		&TransactionMessage{
			Action:      TRANSACTION_FINISH,
			Transaction: trns,
		},
	)
	if err != nil {
		srv.logger.Errorf("error broadcasting transaction finish: %v", err)
		// TODO retry?
	}
	return trns, nil
}

func (srv *Server) Transactions(ctx context.Context) (map[string]*Transaction, error) {
	snap := topology.NewClusterSnapshot(srv.cluster.noder, srv.cluster.Hasher, srv.cluster.partitionN)
	node := srv.node()
	if !snap.IsPrimaryFieldTranslationNode(node.ID) && len(srv.cluster.Nodes()) > 1 {
		return nil, ErrNodeNotPrimary
	}

	return srv.holder.Transactions(ctx)
}

func (srv *Server) GetTransaction(ctx context.Context, id string, remote bool) (*Transaction, error) {
	snap := topology.NewClusterSnapshot(srv.cluster.noder, srv.cluster.Hasher, srv.cluster.partitionN)

	node := srv.node()
	if !remote && !snap.IsPrimaryFieldTranslationNode(node.ID) && len(srv.cluster.Nodes()) > 1 {
		return nil, ErrNodeNotPrimary
	}

	if remote && (snap.IsPrimaryFieldTranslationNode(node.ID) || len(srv.cluster.Nodes()) == 1) {
		return nil, errors.New("unexpected remote get call to primary or single node cluster")
	}

	trns, err := srv.holder.GetTransaction(ctx, id)
	if err != nil {
		return nil, errors.Wrap(err, "getting transaction")
	}

	// The way a client would find out that the exclusive transaction
	// it requested is active is by polling the GetTransaction
	// endpoint. Therefore, returning an active, exclusive
	// transaction, from here is what truly makes the transaction
	// "live". Before doing so, we want to make sure all nodes
	// agree. (in case other nodes have activity on this transaction
	// we're not aware of)
	if !remote && trns.Exclusive && trns.Active {
		err := srv.SendSync(
			&TransactionMessage{
				Action:      TRANSACTION_VALIDATE,
				Transaction: trns,
			},
		)
		if err != nil {
			return nil, errors.Wrap(err, "contacting remote hosts")
		}
		return trns, nil
	}
	return trns, nil
}

// PlanSQL parses and prepares a SQL statement.
func (s *Server) PlanSQL(ctx context.Context, q string) (*Stmt, error) {
	st, err := sql2.NewParser(strings.NewReader(q)).ParseStatement()
	if err != nil {
		return nil, err
	}
	vprint.VV("PLanning SQL: (%v)", q)
	return NewPlanner(s.executor).PlanStatement(ctx, st)
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
