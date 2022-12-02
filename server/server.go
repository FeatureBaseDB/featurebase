// Copyright 2021 Molecula Corp. All rights reserved.
//
// Package server contains the `pilosa server` subcommand which runs Pilosa
// itself. The purpose of this package is to define an easily tested Command
// object which handles interpreting configuration and setting up all the
// objects that Pilosa needs.

package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/molecula/featurebase/v3/dax"
	"github.com/molecula/featurebase/v3/systemlayer"
	"golang.org/x/sync/errgroup"

	pilosa "github.com/molecula/featurebase/v3"
	"github.com/molecula/featurebase/v3/authn"
	"github.com/molecula/featurebase/v3/authz"
	"github.com/molecula/featurebase/v3/batch"
	"github.com/molecula/featurebase/v3/boltdb"
	"github.com/molecula/featurebase/v3/dax/computer"
	"github.com/molecula/featurebase/v3/dax/computer/alpha"
	"github.com/molecula/featurebase/v3/encoding/proto"
	petcd "github.com/molecula/featurebase/v3/etcd"
	"github.com/molecula/featurebase/v3/gcnotify"
	"github.com/molecula/featurebase/v3/gopsutil"
	"github.com/molecula/featurebase/v3/logger"
	pnet "github.com/molecula/featurebase/v3/net"
	"github.com/molecula/featurebase/v3/prometheus"
	"github.com/molecula/featurebase/v3/sql3"
	"github.com/molecula/featurebase/v3/sql3/planner"
	"github.com/molecula/featurebase/v3/statik"
	"github.com/molecula/featurebase/v3/stats"
	"github.com/molecula/featurebase/v3/statsd"
	"github.com/molecula/featurebase/v3/syswrap"
	"github.com/molecula/featurebase/v3/testhook"
	"github.com/pelletier/go-toml"
	"github.com/pkg/errors"
)

type loggerLogger interface {
	logger.Logger
	Logger() *log.Logger
}

// Command represents the state of the pilosa server command.
type Command struct {
	Server *pilosa.Server

	// Configuration.
	Config *Config

	// Started will be closed once Command.Start is finished.
	Started chan struct{}
	// done will be closed when Command.Close() is called
	done chan struct{}

	logOutput      io.Writer
	queryLogOutput io.Writer
	logger         loggerLogger
	queryLogger    loggerLogger

	mds         pilosa.MDS
	writeLogger pilosa.WriteLogger
	snapshotter pilosa.Snapshotter

	Handler      pilosa.HandlerI
	httpHandler  http.Handler
	grpcServer   *grpcServer
	grpcLn       net.Listener
	API          *pilosa.API
	ln           net.Listener
	listenURI    *pnet.URI
	tlsConfig    *tls.Config
	closeTimeout time.Duration

	serverOptions []pilosa.ServerOption
	auth          *authn.Auth

	// isComputeNode is set to true if this node is running as a DAX compute
	// node.
	isComputeNode bool
}

// Logger returns the command's associated Logger to maintain CommandWithTLSSupport interface compatibility
func (cmd *Command) Logger() logger.Logger {
	return cmd.logger
}

type CommandOption func(c *Command) error

func OptCommandServerOptions(opts ...pilosa.ServerOption) CommandOption {
	return func(c *Command) error {
		c.serverOptions = append(c.serverOptions, opts...)
		return nil
	}
}

func OptCommandCloseTimeout(d time.Duration) CommandOption {
	return func(c *Command) error {
		c.closeTimeout = d
		return nil
	}
}

func OptCommandConfig(config *Config) CommandOption {
	return func(c *Command) error {
		defer c.Config.MustValidate()
		if c.Config != nil {
			c.Config.Etcd = config.Etcd
			c.Config.Auth = config.Auth
			c.Config.TLS = config.TLS
			c.Config.MDSAddress = config.MDSAddress
			c.Config.WriteLogger = config.WriteLogger
			return nil
		}
		c.Config = config
		return nil
	}
}

// OptCommandSetConfig was added because OptCommandConfig only sets a small
// sub-set of the config options (it doesn't seem to be used for anything but
// tests). We need a functional option which sets the full Config.
func OptCommandSetConfig(config *Config) CommandOption {
	return func(c *Command) error {
		defer c.Config.MustValidate()
		c.Config = config
		return nil
	}
}

// OptCommandInjections injects the interface implementations.
func OptCommandInjections(inj Injections) CommandOption {
	return func(c *Command) error {
		if inj.MDS != nil {
			c.mds = inj.MDS
		}
		if inj.WriteLogger != nil {
			c.writeLogger = inj.WriteLogger
		}
		if inj.Snapshotter != nil {
			c.snapshotter = inj.Snapshotter
		}
		c.isComputeNode = inj.IsComputeNode
		return nil
	}
}

type Injections struct {
	MDS           pilosa.MDS
	WriteLogger   pilosa.WriteLogger
	Snapshotter   pilosa.Snapshotter
	IsComputeNode bool
}

// NewCommand returns a new instance of Main.
func NewCommand(stderr io.Writer, opts ...CommandOption) *Command {
	c := &Command{
		Config: NewConfig(),

		logOutput: stderr,

		Started: make(chan struct{}),
		done:    make(chan struct{}),
	}

	for _, opt := range opts {
		err := opt(c)
		if err != nil {
			panic(err)
			// TODO: Return error instead of panic?
		}
	}

	return c
}

// defaultFileLimit is a suggested open file count limit for Pilosa to run with
const (
	defaultFileLimit = uint64(256 * 1024)
)

// we want to set resource limits *exactly once*, and then be able
// to report on whether or not that succeeded.
var setupResourceLimitsOnce sync.Once
var setupResourceLimitsErr error

// doSetupResourceLimits is the function which actually does the
// resource limit setup, possibly yielding an error. it's a Command
// method because it uses the command's logger, but is in fact
// expected to work globally.
func (m *Command) doSetupResourceLimits() error {
	oldLimit := &syscall.Rlimit{}

	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, oldLimit); err != nil {
		return fmt.Errorf("checking open file limit: %w", err)
	}
	// inherit existing limit
	targetFileLimit := defaultFileLimit
	if targetFileLimit > oldLimit.Max {
		m.logger.Warnf("open file maximum (%d) lower than suggested open files (%d)",
			oldLimit.Max, defaultFileLimit)
		targetFileLimit = oldLimit.Max
	}
	// If the soft limit is lower than the defaultFileLimit constant, we will try to change it.
	if oldLimit.Cur < targetFileLimit {
		newLimit := &syscall.Rlimit{
			Cur: targetFileLimit,
			Max: oldLimit.Max,
		}
		// Try to set the limit
		if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, newLimit); err != nil {
			return fmt.Errorf("setting open file limit: %w", err)
		}

		// Check the limit after setting it. OS may not obey Setrlimit call.
		if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, oldLimit); err != nil {
			return fmt.Errorf("checking open file limit: %w", err)
		} else {
			if oldLimit.Cur != targetFileLimit {
				m.logger.Warnf("tried to set open file limit to %d, but it is %d; see https://docs.featurebasedb.cloud/reference/hostsystem#operating-system-configuration", targetFileLimit, oldLimit.Cur)
			}
		}
	}
	// We don't have corresponding options for non-Linux right now, but probably should.
	if runtime.GOOS == "linux" {
		result, err := os.ReadFile("/proc/sys/vm/max_map_count")
		if err != nil {
			m.logger.Infof("Tried unsuccessfully to check system mmap limit: %w", err)
		} else {
			sysMmapLimit, err := strconv.ParseUint(strings.TrimSuffix(string(result), "\n"), 10, 64)
			if err != nil {
				m.logger.Infof("Tried unsuccessfully to check system mmap limit: %w", err)
			} else if m.Config.MaxMapCount > sysMmapLimit {
				m.logger.Warnf("Config max map limit (%v) is greater than current system limits (%v)", m.Config.MaxMapCount, sysMmapLimit)
			}
		}
	}
	return nil
}

// setupResourceLimits tries to set up resource limits, like mmap limits
// and open files, if that hasn't been done already, and returns an error
// if the attempt failed in a way that we didn't anticipate. Mere permission
// denied errors are not that concerning.
func (m *Command) setupResourceLimits() error {
	setupResourceLimitsOnce.Do(func() {
		setupResourceLimitsErr = m.doSetupResourceLimits()
	})
	return setupResourceLimitsErr
}

// StartNoServe starts the pilosa server, but doesn't serve on the http handler.
func (m *Command) StartNoServe() (err error) {
	// Seed random number generator
	rand.Seed(time.Now().UTC().UnixNano())

	// setupServer
	err = m.setupServer()
	if err != nil {
		return errors.Wrap(err, "setting up server")
	}
	err = m.setupResourceLimits()
	if err != nil {
		return errors.Wrap(err, "setting resource limits")
	}

	// Initialize server.
	if err = m.Server.Open(); err != nil {
		return errors.Wrap(err, "opening server")
	}

	return nil
}

// Register registers the node with the MDS service using whatever MDS
// implementation was injected during setup.
func (m *Command) Register() (err error) {
	if m.mds == nil {
		return errors.New("no MDS implementation with which to register")
	}

	node := &dax.Node{
		Address: dax.Address(m.Config.Advertise),
		RoleTypes: []dax.RoleType{
			dax.RoleTypeCompute,
			dax.RoleTypeTranslate,
		},
	}
	return m.mds.RegisterNode(context.Background(), node)
}

// CheckIn is called periodically to check in with the MDS service using
// whatever MDS implementation was injected during setup.
func (m *Command) CheckIn() (err error) {
	if m.mds == nil {
		return errors.New("no MDS implementation with which to check-in")
	}

	node := &dax.Node{
		Address: dax.Address(m.Config.Advertise),
		RoleTypes: []dax.RoleType{
			dax.RoleTypeCompute,
			dax.RoleTypeTranslate,
		},
	}
	return m.mds.CheckInNode(context.Background(), node)
}

// Start starts the pilosa server - it returns once the server is running.
func (m *Command) Start() (err error) {
	// Seed random number generator
	rand.Seed(time.Now().UTC().UnixNano())

	// setupServer
	err = m.setupServer()
	if err != nil {
		return errors.Wrap(err, "setting up server")
	}
	err = m.setupResourceLimits()
	if err != nil {
		return errors.Wrap(err, "setting resource limits")
	}

	// Initialize server.
	if err = m.Server.Open(); err != nil {
		return errors.Wrap(err, "opening server")
	}

	// Initialize HTTP.
	go func() {
		if err := m.Handler.Serve(); err != nil {
			m.logger.Errorf("handler serve error: %v", err)
		}
	}()
	m.logger.Printf("listening as %s\n", m.listenURI)

	// Initialize gRPC.
	go func() {
		if err := m.grpcServer.Serve(); err != nil {
			m.logger.Errorf("grpc server error: %v", err)
		}
	}()

	_ = testhook.Opened(pilosa.NewAuditor(), m, nil)
	close(m.Started)
	return nil
}

func (m *Command) UpAndDown() (err error) {
	// Seed random number generator
	rand.Seed(time.Now().UTC().UnixNano())

	// setupServer
	err = m.setupServer()
	if err != nil {
		return errors.Wrap(err, "setting up server")
	}
	m.logger.Infof("bringing server up and shutting it down immediately")

	go func() {
		err := m.Handler.Serve()
		if err != nil {
			m.logger.Errorf("handler serve error: %v", err)
		}
	}()

	// Bring the server up, and back down again.
	if err = m.Server.UpAndDown(); err != nil {
		return errors.Wrap(err, "bringing server up and down")
	}

	m.logger.Infof("teardown complete")

	return nil
}

// Wait waits for the server to be closed or interrupted.
func (m *Command) Wait() error {
	// First SIGKILL causes server to shut down gracefully.
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	select {
	case sig := <-c:
		m.logger.Infof("received signal '%s', gracefully shutting down...\n", sig.String())

		// Second signal causes a hard shutdown.
		go func() { <-c; os.Exit(1) }()
		return errors.Wrap(m.Close(), "closing command")
	case <-m.done:
		m.logger.Infof("server closed externally")
		return nil
	}
}

// setupServer uses the cluster configuration to set up this server.
func (m *Command) setupServer() error {
	runtime.SetBlockProfileRate(m.Config.Profile.BlockRate)
	runtime.SetMutexProfileFraction(m.Config.Profile.MutexFraction)

	_ = syswrap.SetMaxMapCount(m.Config.MaxMapCount)
	_ = syswrap.SetMaxFileCount(m.Config.MaxFileCount)

	err := m.setupLogger()
	if err != nil {
		return errors.Wrap(err, "setting up logger")
	}

	version := pilosa.VersionInfo(m.Config.Future.Rename)
	m.logger.Infof("%s", version)

	handleTrialDeadline(m.logger)

	// validateAddrs sets the appropriate values for Bind and Advertise
	// based on the inputs. It is not responsible for applying defaults, although
	// it does provide a non-zero port (10101) in the case where no port is specified.
	// The alternative would be to use port 0, which would choose a random port, but
	// currently that's not what we want.
	if err := m.Config.validateAddrs(context.Background()); err != nil {
		return errors.Wrap(err, "validating addresses")
	}

	uri, err := pilosa.AddressWithDefaults(m.Config.Bind)
	if err != nil {
		return errors.Wrap(err, "processing bind address")
	}

	grpcURI, err := pnet.NewURIFromAddress(m.Config.BindGRPC)
	if err != nil {
		return errors.Wrap(err, "processing bind grpc address")
	}
	if m.Config.GRPCListener == nil {
		// create gRPC listener
		m.grpcLn, err = net.Listen("tcp", grpcURI.HostPort())
		if err != nil {
			return errors.Wrap(err, "creating grpc listener")
		}
		// If grpc port is 0, get auto-allocated port from listener
		if grpcURI.Port == 0 {
			grpcURI.SetPort(uint16(m.grpcLn.Addr().(*net.TCPAddr).Port))
		}
	} else {
		m.grpcLn = m.Config.GRPCListener
	}

	// Setup TLS
	if uri.Scheme == "https" {
		m.tlsConfig, err = GetTLSConfig(&m.Config.TLS, m.logger)
		if err != nil {
			return errors.Wrap(err, "get tls config")
		}
	}

	diagnosticsInterval := time.Duration(0)
	if m.Config.Metric.Diagnostics {
		diagnosticsInterval = defaultDiagnosticsInterval
	}

	statsClient, err := newStatsClient(m.Config.Metric.Service, m.Config.Metric.Host, m.Config.Namespace())
	if err != nil {
		return errors.Wrap(err, "new stats client")
	}

	if m.Config.Listener == nil {
		m.ln, err = getListener(*uri, m.tlsConfig)
		if err != nil {
			return errors.Wrap(err, "getting listener")
		}
	} else {
		m.ln = m.Config.Listener
	}

	// If port is 0, get auto-allocated port from listener
	if uri.Port == 0 {
		uri.SetPort(uint16(m.ln.Addr().(*net.TCPAddr).Port))
	}

	// Save listenURI for later reference.
	m.listenURI = uri

	c := pilosa.GetHTTPClient(m.tlsConfig)

	// Get advertise address as uri.
	advertiseURI, err := pilosa.AddressWithDefaults(m.Config.Advertise)
	if err != nil {
		return errors.Wrap(err, "processing advertise address")
	}
	if advertiseURI.Port == 0 {
		advertiseURI.SetPort(uri.Port)
	}

	// Get grpc advertise address as uri.
	advertiseGRPCURI, err := pnet.NewURIFromAddress(m.Config.AdvertiseGRPC)
	if err != nil {
		return errors.Wrap(err, "processing grpc advertise address")
	}
	if advertiseGRPCURI.Port == 0 {
		advertiseGRPCURI.SetPort(grpcURI.Port)
	}

	// Primary store configuration is handled automatically now.
	if m.Config.Translation.PrimaryURL != "" {
		m.logger.Infof("DEPRECATED: The primary-url configuration option is no longer used.")
	}
	if m.Config.AntiEntropy.Interval != 0 {
		m.logger.Infof("DEPRECATED: The anti-entropy configuration option is no longer used.")
	}
	// Handle renamed and deprecated config parameter
	longQueryTime := m.Config.LongQueryTime
	if m.Config.Cluster.LongQueryTime >= 0 {
		longQueryTime = m.Config.Cluster.LongQueryTime
		m.logger.Infof("DEPRECATED: Configuration parameter cluster.long-query-time has been renamed to long-query-time")
	}

	// Use other config parameters to set Etcd parameters which we don't want to
	// expose in the user-facing config.
	//
	// Use cluster.name for etcd.cluster-name
	m.Config.Etcd.ClusterName = m.Config.Cluster.Name
	//
	// Use name for etcd.name
	m.Config.Etcd.Name = m.Config.Name
	// use the pilosa provided tls credentials if available
	m.Config.Etcd.TrustedCAFile = m.Config.TLS.CACertPath
	m.Config.Etcd.ClientCertFile = m.Config.TLS.CertificatePath
	m.Config.Etcd.ClientKeyFile = m.Config.TLS.CertificateKeyPath
	m.Config.Etcd.PeerCertFile = m.Config.TLS.CertificatePath
	m.Config.Etcd.PeerKeyFile = m.Config.TLS.CertificateKeyPath
	//
	// If an Etcd.Dir is not provided, nest a default under the pilosa data dir.
	if m.Config.Etcd.Dir == "" {
		path, err := expandDirName(m.Config.DataDir)
		if err != nil {
			return errors.Wrapf(err, "expanding directory name: %s", m.Config.DataDir)
		}
		m.Config.Etcd.Dir = filepath.Join(path, pilosa.DiscoDir)
	}

	// WriteLogger setup.
	var wlw computer.WriteLogWriter = computer.NewNopWriteLogWriter()
	var wlr computer.WriteLogReader = computer.NewNopWriteLogReader()
	if m.writeLogger != nil {
		alphaWriteLog := alpha.NewAlphaWriteLog(m.writeLogger)
		wlr = alphaWriteLog
		wlw = alphaWriteLog
	}

	// Snapshotter setup.
	var snap computer.SnapshotReadWriter = computer.NewNopSnapshotReadWriter()
	if m.snapshotter != nil {
		snap = alpha.NewAlphaSnapshot(m.snapshotter)
	}

	m.Config.Etcd.Id = m.Config.Name // TODO(twg) rethink this
	e := petcd.NewEtcd(m.Config.Etcd, m.logger, m.Config.Cluster.ReplicaN, version)

	executionPlannerFn := func(e pilosa.Executor, api *pilosa.API, sql string) sql3.CompilePlanner {
		fapi := &pilosa.FeatureBaseSchemaAPI{API: api}
		fsapi := &pilosa.FeatureBaseSystemAPI{API: api}
		fimp := &batch.FeaturebaseImporter{API: api}

		return planner.NewExecutionPlanner(e, fapi, fsapi, api, m.Server.SystemLayer, fimp, m.logger, sql)
	}

	serverOptions := []pilosa.ServerOption{
		pilosa.OptServerLongQueryTime(time.Duration(longQueryTime)),
		pilosa.OptServerDataDir(m.Config.DataDir),
		pilosa.OptServerReplicaN(m.Config.Cluster.ReplicaN),
		pilosa.OptServerMaxWritesPerRequest(m.Config.MaxWritesPerRequest),
		pilosa.OptServerMetricInterval(time.Duration(m.Config.Metric.PollInterval)),
		pilosa.OptServerDiagnosticsInterval(diagnosticsInterval),
		pilosa.OptServerExecutorPoolSize(m.Config.WorkerPoolSize),
		pilosa.OptServerOpenTranslateStore(boltdb.OpenTranslateStore),
		pilosa.OptServerOpenTranslateReader(pilosa.GetOpenTranslateReaderWithLockerFunc(c, &sync.Mutex{})),
		pilosa.OptServerOpenIDAllocator(pilosa.OpenIDAllocator),
		pilosa.OptServerLogger(m.logger),
		pilosa.OptServerQueryLogger(m.queryLogger),
		pilosa.OptServerSystemInfo(gopsutil.NewSystemInfo()),
		pilosa.OptServerGCNotifier(gcnotify.NewActiveGCNotifier()),
		pilosa.OptServerStatsClient(statsClient),
		pilosa.OptServerURI(advertiseURI),
		pilosa.OptServerGRPCURI(advertiseGRPCURI),
		pilosa.OptServerClusterName(m.Config.Cluster.Name),
		pilosa.OptServerSerializer(proto.Serializer{}),
		pilosa.OptServerStorageConfig(m.Config.Storage),
		pilosa.OptServerRBFConfig(m.Config.RBFConfig),
		pilosa.OptServerMaxQueryMemory(m.Config.MaxQueryMemory),
		pilosa.OptServerQueryHistoryLength(m.Config.QueryHistoryLength),
		pilosa.OptServerPartitionAssigner(m.Config.Cluster.PartitionToNodeAssignment),
		pilosa.OptServerDisCo(e, e, e, e),
		pilosa.OptServerExecutionPlannerFn(executionPlannerFn),
		pilosa.OptServerWriteLogReader(wlr),
		pilosa.OptServerWriteLogWriter(wlw),
		pilosa.OptServerSnapshotReadWriter(snap),
		pilosa.OptServerIsDataframeEnabled(m.Config.Dataframe.Enable),
	}

	if m.Config.LookupDBDSN != "" {
		serverOptions = append(serverOptions, pilosa.OptServerLookupDB(m.Config.LookupDBDSN))
	}

	serverOptions = append(serverOptions, m.serverOptions...)

	if m.Config.Auth.Enable {
		serverOptions = append(serverOptions, pilosa.OptServerInternalClient(pilosa.NewInternalClientFromURI(uri, c, pilosa.WithSecretKey(m.Config.Auth.SecretKey), pilosa.WithSerializer(proto.Serializer{}))))
	} else {
		serverOptions = append(serverOptions, pilosa.OptServerInternalClient(pilosa.NewInternalClientFromURI(uri, c, pilosa.WithSerializer(proto.Serializer{}))))
	}

	m.Server, err = pilosa.NewServer(serverOptions...)
	if err != nil {
		return errors.Wrap(err, "new server")
	}

	m.Server.SystemLayer = systemlayer.NewSystemLayer()

	m.API, err = pilosa.NewAPI(
		pilosa.OptAPIServer(m.Server),
		pilosa.OptAPIImportWorkerPoolSize(m.Config.ImportWorkerPoolSize),
		pilosa.OptAPIWriteLogReader(wlr),
		pilosa.OptAPIWriteLogWriter(wlw),
		pilosa.OptAPISnapshotter(snap),
		pilosa.OptAPIDirectiveWorkerPoolSize(m.Config.DirectiveWorkerPoolSize),
		pilosa.OptAPIIsComputeNode(m.isComputeNode),
	)
	if err != nil {
		return errors.Wrap(err, "new api")
	}
	// Tell server about its new API, which its client will need.
	m.Server.SetAPI(m.API)

	var p authz.GroupPermissions
	if m.Config.Auth.Enable {
		m.Config.MustValidateAuth()
		permsFile, err := os.Open(m.Config.Auth.PermissionsFile)
		if err != nil {
			return err
		}
		defer permsFile.Close()

		if err = p.ReadPermissionsFile(permsFile); err != nil {
			return err
		}

		ac := m.Config.Auth
		m.auth, err = authn.NewAuth(m.logger, ac.RedirectBaseURL, ac.Scopes, ac.AuthorizeURL, ac.TokenURL, ac.GroupEndpointURL, ac.LogoutURL, ac.ClientId, ac.ClientSecret, ac.SecretKey, ac.ConfiguredIPs)
		if err != nil {
			return errors.Wrap(err, "instantiating authN object")
		}

		err = m.setupQueryLogger()
		if err != nil {
			return errors.Wrap(err, "setting up queryLogger")
		}

		m.queryLogger.Infof("Starting Featurebase...")
		m.queryLogger.Infof("Group with admin level access: %v", p.Admin)
		m.queryLogger.Infof("Permissions: %+v", p.Permissions)
		if len(ac.ConfiguredIPs) > 0 {
			m.queryLogger.Infof("Configured IPs for allowed networks: %v", ac.ConfiguredIPs)
		}

		// TLS must be enabled if auth is
		if m.Config.TLS.CertificatePath == "" || m.Config.TLS.CertificateKeyPath == "" {
			return fmt.Errorf("transport layer security (TLS) is not configured properly. TLS is required when AuthN/Z is enabled, current configuration: %v", m.Config.TLS)
		}

	}

	m.grpcServer, err = NewGRPCServer(
		OptGRPCServerAPI(m.API),
		OptGRPCServerListener(m.grpcLn),
		OptGRPCServerTLSConfig(m.tlsConfig),
		OptGRPCServerLogger(m.logger),
		OptGRPCServerStats(statsClient),
		OptGRPCServerAuth(m.auth),
		OptGRPCServerPerm(&p),
		OptGRPCServerQueryLogger(m.queryLogger),
	)
	if err != nil {
		return errors.Wrap(err, "getting grpcServer")
	}

	hndlr, err := pilosa.NewHandler(
		pilosa.OptHandlerAllowedOrigins(m.Config.Handler.AllowedOrigins),
		pilosa.OptHandlerAPI(m.API),
		pilosa.OptHandlerLogger(m.logger),
		pilosa.OptHandlerQueryLogger(m.queryLogger),
		pilosa.OptHandlerFileSystem(&statik.FileSystem{}),
		pilosa.OptHandlerListener(m.ln, m.Config.Advertise),
		pilosa.OptHandlerCloseTimeout(m.closeTimeout),
		pilosa.OptHandlerMiddleware(m.grpcServer.middleware(m.Config.Handler.AllowedOrigins)),
		pilosa.OptHandlerAuthN(m.auth),
		pilosa.OptHandlerAuthZ(&p),
		pilosa.OptHandlerSerializer(proto.Serializer{}),
		pilosa.OptHandlerRoaringSerializer(proto.RoaringSerializer),
		pilosa.OptHandlerSQLEnabled(m.Config.SQL.EndpointEnabled),
	)
	if err != nil {
		return errors.Wrap(err, "new handler")
	}

	m.httpHandler = hndlr
	m.Handler = hndlr

	return nil
}

// HTTPHandler was added for the case where we want to get the full
// http.Handler, and not just those methods which satisfy the pilosa.HandlerI
// interface.
func (m *Command) HTTPHandler() http.Handler {
	return m.httpHandler
}

// setupLogger sets up the logger based on the configuration.
func (m *Command) setupLogger() error {
	var f *logger.FileWriter
	var err error
	if m.Config.LogPath != "" {
		f, err = logger.NewFileWriter(m.Config.LogPath)
		if err != nil {
			return errors.Wrap(err, "opening file")
		}
		m.logOutput = f
	}
	if m.Config.Verbose {
		m.logger = logger.NewVerboseLogger(m.logOutput)
	} else {
		m.logger = logger.NewStandardLogger(m.logOutput)
	}
	if m.Config.LogPath != "" {
		sighup := make(chan os.Signal, 1)
		signal.Notify(sighup, syscall.SIGHUP)
		go func() {
			for {
				// duplicate stderr onto log file
				err := m.dup(int(f.Fd()), int(os.Stderr.Fd()))
				if err != nil {
					m.logger.Errorf("syscall dup: %s\n", err.Error())
				}

				// reopen log file on SIGHUP
				<-sighup
				err = f.Reopen()
				if err != nil {
					m.logger.Infof("reopen: %s\n", err.Error())
				}
			}
		}()
	}
	return nil
}

func (m *Command) setupQueryLogger() error {
	var f *logger.FileWriter
	var err error

	if m.Config.Auth.QueryLogPath == "" {
		f, err = logger.NewFileWriterMode("queries/query.log", 0o600)
		if err != nil {
			return errors.Wrap(err, "opening file")
		}
	} else {
		f, err = logger.NewFileWriterMode(m.Config.Auth.QueryLogPath, 0o600)
		if err != nil {
			return errors.Wrap(err, "opening file")
		}
	}
	m.queryLogOutput = f

	m.queryLogger = logger.NewStandardLogger(m.queryLogOutput)

	sighup := make(chan os.Signal, 1)
	signal.Notify(sighup, syscall.SIGHUP)
	go func() {
		for range sighup {
			if err := f.Reopen(); err != nil {
				m.queryLogger.Infof("reopen: %s\n", err.Error())
			}
		}
	}()
	return nil
}

// Close shuts down the server.
func (m *Command) Close() error {
	select {
	case <-m.done:
		return nil
	default:
		eg := errgroup.Group{}
		m.grpcServer.Stop()
		eg.Go(m.Handler.Close)
		eg.Go(m.Server.Close)
		eg.Go(m.API.Close)
		if closer, ok := m.logOutput.(io.Closer); ok {
			// If closer is os.Stdout or os.Stderr, don't close it.
			if closer != os.Stdout && closer != os.Stderr {
				eg.Go(closer.Close)
			}
		}

		err := eg.Wait()
		_ = testhook.Closed(pilosa.NewAuditor(), m, nil)
		close(m.done)

		return errors.Wrap(err, "closing everything")
	}
}

// newStatsClient creates a stats client from the config
func newStatsClient(name string, host string, namespace string) (stats.StatsClient, error) {
	switch name {
	case "expvar":
		return stats.NewExpvarStatsClient(), nil
	case "statsd":
		return statsd.NewStatsClient(host, namespace)
	case "prometheus":
		return prometheus.NewPrometheusClient(
			prometheus.OptClientNamespace(namespace),
		)
	case "nop", "none":
		return stats.NopStatsClient, nil
	default:
		return nil, errors.Errorf("'%v' not a valid stats client, choose from [expvar, statsd, prometheus, none].", name)
	}
}

// getListener gets a net.Listener based on the config.
func getListener(uri pnet.URI, tlsconf *tls.Config) (ln net.Listener, err error) {
	// If bind URI has the https scheme, enable TLS
	if uri.Scheme == "https" && tlsconf != nil {
		ln, err = tls.Listen("tcp", uri.HostPort(), tlsconf)
		if err != nil {
			return nil, errors.Wrap(err, "tls.Listener")
		}
	} else if uri.Scheme == "http" {
		// Open HTTP listener to determine port (if specified as :0).
		ln, err = net.Listen("tcp", uri.HostPort())
		if err != nil {
			return nil, errors.Wrap(err, "net.Listen")
		}
	} else {
		return nil, errors.Errorf("unsupported scheme: %s", uri.Scheme)
	}

	return ln, nil
}

// ParseConfig parses s into a Config.
func ParseConfig(s string) (Config, error) {
	var c Config
	err := toml.Unmarshal([]byte(s), &c)
	return c, err
}

// expandDirName was copied from pilosa/server.go.
// TODO: consider centralizing this if we need this across packages.
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
