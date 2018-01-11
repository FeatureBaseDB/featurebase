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
	"github.com/pilosa/pilosa/diagnostics"
	"github.com/pilosa/pilosa/internal"
	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"
)

// Default server settings.
const (
	DefaultAntiEntropyInterval = 10 * time.Minute
	DefaultPollingInterval     = 60 * time.Second
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
	// Host is replaced with actual host after opening if port is ":0".
	Network     string
	URI         *URI
	Cluster     *Cluster
	diagnostics *diagnostics.Diagnostics
	ClusterID   string

	// Background monitoring intervals.
	AntiEntropyInterval time.Duration
	PollingInterval     time.Duration
	MetricInterval      time.Duration
	DiagnosticInterval  time.Duration

	// TLS configuration
	TLS *tls.Config

	// Misc options.
	MaxWritesPerRequest int

	LogOutput io.Writer
	logger    *log.Logger

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
		PollingInterval:     DefaultPollingInterval,
		MetricInterval:      0,
		DiagnosticInterval:  0,

		LogOutput: os.Stderr,
	}
	s.logger = log.New(s.LogOutput, "", log.LstdFlags)

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

	// Create local node if no cluster is specified.
	if len(s.Cluster.Nodes) == 0 {
		s.Cluster.Nodes = []*Node{
			{Scheme: s.URI.Scheme(), Host: s.URI.HostPort()},
		}
	}

	for i, n := range s.Cluster.Nodes {
		if s.Cluster.NodeByHost(n.Host) != nil {
			s.Holder.Stats = s.Holder.Stats.WithTags(fmt.Sprintf("NodeID:%d", i))
		}
	}

	// Open holder.
	s.Holder.LogOutput = s.LogOutput
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

	// Create default HTTP client
	s.createDefaultClient(s.RemoteClient)

	// Create executor for executing queries.
	e := NewExecutor(s.RemoteClient)
	e.Holder = s.Holder
	e.Scheme = s.URI.Scheme()
	e.Host = s.URI.HostPort()
	e.Cluster = s.Cluster
	e.MaxWritesPerRequest = s.MaxWritesPerRequest
	s.Cluster.MaxWritesPerRequest = s.MaxWritesPerRequest

	// Initialize HTTP handler.
	s.Handler.Broadcaster = s.Broadcaster
	s.Handler.BroadcastHandler = s
	s.Handler.StatusHandler = s
	s.Handler.URI = s.URI
	s.Handler.Cluster = s.Cluster
	s.Handler.Executor = e
	s.Handler.LogOutput = s.LogOutput

	// Initialize Holder.
	s.Holder.Broadcaster = s.Broadcaster

	// Serve HTTP.
	go func() {
		server := &http.Server{Handler: s.Handler}
		go func() {
			<-s.closing
			server.Close()
		}()
		err := server.Serve(ln)
		if err != nil && err.Error() != "http: Server closed" {
			s.Logger().Printf("HTTP handler terminated with error: %s\n", err)
		}
	}()

	// load local ID
	if err := s.Holder.loadLocalID(); err != nil {
		s.Logger().Println(err)
	}

	if err := s.loadClusterID(); err != nil {
		s.Logger().Println(err)
	}

	// Start background monitoring.
	s.wg.Add(4)
	go func() { defer s.wg.Done(); s.monitorAntiEntropy() }()
	go func() { defer s.wg.Done(); s.monitorMaxSlices() }()
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

// Logger returns a logger that writes to LogOutput
func (s *Server) Logger() *log.Logger { return s.logger }

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
		syncer.RemoteClient = s.RemoteClient
		syncer.Stats = s.Holder.Stats.WithTags("HolderSyncer")

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
			if s.URI.HostPort() != node.Host {
				maxSlices, _ := s.checkMaxSlices(node.Scheme, node.Host)
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
		opt := FrameOptions{
			RowLabel:       obj.Meta.RowLabel,
			InverseEnabled: obj.Meta.InverseEnabled,
			RangeEnabled:   obj.Meta.RangeEnabled,
			CacheType:      obj.Meta.CacheType,
			CacheSize:      obj.Meta.CacheSize,
			TimeQuantum:    TimeQuantum(obj.Meta.TimeQuantum),
			Fields:         decodeFields(obj.Meta.Fields),
		}
		_, err := idx.CreateFrame(obj.Frame, opt)
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
	}
	return nil
}

// SendSync represents an implementation of Broadcaster.
func (s *Server) SendSync(pb proto.Message) error {
	var eg errgroup.Group
	for _, node := range s.Cluster.Nodes {
		uri, err := node.URI()
		if err != nil {
			return err
		}

		// Don't forward the message to ourselves.
		if *s.URI == *uri {
			continue
		}

		ctx := context.WithValue(context.Background(), "uri", uri)
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

// LocalStatus returns the state of the local node as well as the
// holder (indexes/frames) according to the local node.
// In a gossip implementation, memberlist.Delegate.LocalState() uses this.
// Server implements StatusHandler.
func (s *Server) LocalStatus() (proto.Message, error) {
	if s.Holder == nil {
		return nil, errors.New("Server.Holder is nil")
	}

	ns := internal.NodeStatus{
		Scheme:  s.URI.Scheme(),
		Host:    s.URI.HostPort(),
		State:   NodeStateUp,
		Indexes: EncodeIndexes(s.Holder.Indexes()),
	}

	// Append Slice list per this Node's indexes
	for _, index := range ns.Indexes {
		index.Slices = s.Cluster.OwnsSlices(index.Name, index.MaxSlice, s.URI.HostPort())
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
	node := s.Cluster.NodeByHost(s.URI.HostPort())
	node.SetStatus(ns.(*internal.NodeStatus))

	// Update NodeState for all nodes.
	for host, nodeState := range s.Cluster.NodeStates() {
		// In a default configuration (or single-node) where a StaticNodeSet is used
		// then all nodes are marked as DOWN. At the very least, we should consider
		// the local node as UP.
		// TODO: we should be able to remove this check if/when cluster.Nodes and
		// cluster.NodeSet are unified.
		if host == s.URI.HostPort() {
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

func (s *Server) checkMaxSlices(scheme string, hostPort string) (map[string]uint64, error) {
	// Create HTTP request.
	req, err := http.NewRequest("GET", (&url.URL{
		Scheme: scheme,
		Host:   hostPort,
		Path:   "/slices/max",
	}).String(), nil)

	if err != nil {
		return nil, err
	}

	// Require protobuf encoding.
	req.Header.Set("Accept", "application/x-protobuf")
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("User-Agent", "pilosa/"+Version)

	nodeURI, err := NewURIFromAddress(hostPort)
	if err != nil {
		return nil, err
	}
	nodeURI.SetScheme(scheme)
	ctx := context.WithValue(context.Background(), "uri", nodeURI)
	return s.defaultClient.MaxSliceByIndex(ctx)
}

// monitorDiagnostics periodically polls the Pilosa Indexes for cluster info.
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
	s.diagnostics.Set("Cluster", strings.Join(s.Cluster.NodeSetHosts(), ","))
	s.diagnostics.Set("NumNodes", len(s.Cluster.Nodes))
	s.diagnostics.Set("NumCPU", runtime.NumCPU())
	s.diagnostics.Set("LocalID", s.Holder.LocalID)
	s.diagnostics.Set("ClusterID", s.ClusterID)
	s.diagnostics.EnrichWithOSInfo()

	// Flush the diagnostics metrics at startup, then on each tick interval
	flush := func() {
		enrichDiagnosticsWithSchemaProperties(s.diagnostics, s.Holder)
		openFiles, err := CountOpenFiles()
		if err == nil {
			s.diagnostics.Set("OpenFiles", openFiles)
		}
		s.diagnostics.Set("GoRoutines", runtime.NumGoroutine())
		s.diagnostics.EnrichWithMemoryInfo()
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

func (s *Server) loadClusterID() error {
	// If this is the first node in the cluster, set the ClusterID to its ID
	node0URI, err := s.Cluster.Nodes[0].URI()
	if err == nil {
		if s.URI.Equals(node0URI) {
			s.ClusterID = s.Holder.LocalID
			return nil
		}
	} else {
		return err
	}
	if clusterID, err := s.defaultClient.NodeID(node0URI); err == nil {
		s.ClusterID = clusterID
		return nil
	} else {
		return err
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

// StatusHandler specifies two methods which an object must implement to share
// state in the cluster. These are used by the GossipNodeSet to implement the
// LocalState and MergeRemoteState methods of memberlist.Delegate
type StatusHandler interface {
	LocalStatus() (proto.Message, error)
	ClusterStatus() (proto.Message, error)
	HandleRemoteStatus(proto.Message) error
}

type diagnosticsFrameProperties struct {
	BSIFieldCount      int
	TimeQuantumEnabled bool
}

func enrichDiagnosticsWithSchemaProperties(d *diagnostics.Diagnostics, holder *Holder) {
	// NOTE: this function is not in the diagnostics package, since circular imports are not allowed.
	var numSlices uint64
	numFrames := 0
	numIndexes := 0
	bsiFieldCount := 0
	timeQuantumEnabled := false

	for _, index := range holder.Indexes() {
		numSlices += index.MaxSlice() + 1
		numIndexes += 1
		for _, frame := range index.Frames() {
			numFrames += 1
			if frame.rangeEnabled {
				if fields, err := frame.GetFields(); err == nil {
					bsiFieldCount += len(fields.Fields)
				}
			}
			if frame.TimeQuantum() != "" {
				timeQuantumEnabled = true
			}
		}
	}

	d.Set("NumIndexes", numIndexes)
	d.Set("NumFrames", numFrames)
	d.Set("NumSlices", numSlices)
	d.Set("BSIFieldCount", bsiFieldCount)
	d.Set("TimeQuantumEnabled", timeQuantumEnabled)
}
