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
//
// Package server contains the `pilosa server` subcommand which runs Pilosa
// itself. The purpose of this package is to define an easily tested Command
// object which handles interpreting configuration and setting up all the
// objects that Pilosa needs.

package server

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/pelletier/go-toml"
	"github.com/pilosa/pilosa/v2"
	"github.com/pilosa/pilosa/v2/boltdb"
	"github.com/pilosa/pilosa/v2/encoding/proto"
	petcd "github.com/pilosa/pilosa/v2/etcd"
	"github.com/pilosa/pilosa/v2/gcnotify"
	"github.com/pilosa/pilosa/v2/gopsutil"
	"github.com/pilosa/pilosa/v2/gossip"
	"github.com/pilosa/pilosa/v2/http"
	"github.com/pilosa/pilosa/v2/logger"
	pnet "github.com/pilosa/pilosa/v2/net"
	"github.com/pilosa/pilosa/v2/prometheus"
	"github.com/pilosa/pilosa/v2/statik"
	"github.com/pilosa/pilosa/v2/stats"
	"github.com/pilosa/pilosa/v2/statsd"
	"github.com/pilosa/pilosa/v2/syswrap"
	"github.com/pilosa/pilosa/v2/test/port"
	"github.com/pilosa/pilosa/v2/testhook"
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

	// Gossip transport
	gossipTransport *gossip.Transport
	gossipMemberSet io.Closer

	// Standard input/output
	*pilosa.CmdIO

	// Started will be closed once Command.Start is finished.
	Started chan struct{}
	// done will be closed when Command.Close() is called
	done chan struct{}

	// Passed to the Gossip implementation.
	logOutput io.Writer
	logger    loggerLogger

	Handler      pilosa.Handler
	grpcServer   *grpcServer
	grpcLn       net.Listener
	API          *pilosa.API
	ln           net.Listener
	listenURI    *pnet.URI
	tlsConfig    *tls.Config
	closeTimeout time.Duration
	pgserver     *PostgresServer

	serverOptions []pilosa.ServerOption
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
			c.Config.DisCo = config.DisCo
			fmt.Printf("setting c.ConfigDisCo to '%#v'", config.DisCo)
			return nil
		}
		c.Config = config
		return nil
	}
}

// NewCommand returns a new instance of Main.
func NewCommand(stdin io.Reader, stdout, stderr io.Writer, opts ...CommandOption) *Command {
	c := &Command{
		Config: NewConfig(),

		CmdIO: pilosa.NewCmdIO(stdin, stdout, stderr),

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

// Start starts the pilosa server - it returns once the server is running.
func (m *Command) Start() (err error) {
	// Seed random number generator
	rand.Seed(time.Now().UTC().UnixNano())

	// SetupServer
	err = m.SetupServer()
	if err != nil {
		return errors.Wrap(err, "setting up server")
	}

	// Set up networking (i.e. gossip)
	// Gossip no longer unsed under etcd? time to turn it off here?
	err = m.setupNetworking()
	if err != nil {
		return errors.Wrap(err, "setting up networking")
	}

	go func() {
		err := m.Handler.Serve()
		if err != nil {
			m.logger.Printf("handler serve error: %v", err)
		}
	}()

	// Initialize server.
	if err = m.Server.Open(); err != nil {
		return errors.Wrap(err, "opening server")
	}

	m.logger.Printf("listening as %s\n", m.listenURI)
	go func() {
		if err := m.grpcServer.Serve(); err != nil {
			m.logger.Printf("grpc server error: %v", err)
		}
	}()

	// Initialize postgres.
	m.pgserver = nil
	if m.Config.Postgres.Bind != "" {
		var tlsConf *tls.Config
		if m.Config.Postgres.TLS.CertificatePath != "" {
			conf, err := GetTLSConfig(&m.Config.Postgres.TLS, m.logger.Logger())
			if err != nil {
				return errors.Wrap(err, "setting up postgres TLS")
			}
			tlsConf = conf
		}
		m.pgserver = NewPostgresServer(m.API, m.logger, tlsConf)
		m.pgserver.s.StartupTimeout = time.Duration(m.Config.Postgres.StartupTimeout)
		m.pgserver.s.ReadTimeout = time.Duration(m.Config.Postgres.ReadTimeout)
		m.pgserver.s.WriteTimeout = time.Duration(m.Config.Postgres.WriteTimeout)
		m.pgserver.s.MaxStartupSize = m.Config.Postgres.MaxStartupSize
		m.pgserver.s.ConnectionLimit = m.Config.Postgres.ConnectionLimit
		err := m.pgserver.Start(m.Config.Postgres.Bind)
		if err != nil {
			return errors.Wrap(err, "starting postgres")
		}
	}

	_ = testhook.Opened(pilosa.NewAuditor(), m, nil)
	close(m.Started)
	return nil
}

func (m *Command) UpAndDown() (err error) {
	// Seed random number generator
	rand.Seed(time.Now().UTC().UnixNano())

	// SetupServer
	err = m.SetupServer()
	if err != nil {
		return errors.Wrap(err, "setting up server")
	}

	// SetupNetworking (so we'll have profiling)
	err = m.setupNetworking()
	if err != nil {
		return errors.Wrap(err, "setting up networking")
	}
	go func() {
		err := m.Handler.Serve()
		if err != nil {
			m.logger.Printf("handler serve error: %v", err)
		}
	}()

	// Bring the server up, and back down again.
	if err = m.Server.UpAndDown(); err != nil {
		return errors.Wrap(err, "bringing server up and down")
	}

	m.logger.Printf("brought up and shut down again")

	return nil
}

// Wait waits for the server to be closed or interrupted.
func (m *Command) Wait() error {
	// First SIGKILL causes server to shut down gracefully.
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	select {
	case sig := <-c:
		m.logger.Printf("received signal '%s', gracefully shutting down...\n", sig.String())

		// Second signal causes a hard shutdown.
		go func() { <-c; os.Exit(1) }()
		return errors.Wrap(m.Close(), "closing command")
	case <-m.done:
		m.logger.Printf("server closed externally")
		return nil
	}
}

// SetupServer uses the cluster configuration to set up this server.
func (m *Command) SetupServer() error {
	runtime.SetBlockProfileRate(m.Config.Profile.BlockRate)
	runtime.SetMutexProfileFraction(m.Config.Profile.MutexFraction)

	_ = syswrap.SetMaxMapCount(m.Config.MaxMapCount)
	_ = syswrap.SetMaxFileCount(m.Config.MaxFileCount)

	err := m.setupLogger()
	if err != nil {
		return errors.Wrap(err, "setting up logger")
	}

	m.logger.Printf("%s", pilosa.VersionInfo())

	// If the pilosa command line uses -tx to override the
	// PILOSA_TXSRC env variable, then we must also correct
	// the environment, so that pilosa/txfactory.go can determine the
	// desired Tx engine. This enables "go test" testing in pilosa that
	// does not spin up a full server, while still respecting the pilosa
	// server's choice when run full in production.
	envTxsrc := os.Getenv("PILOSA_TXSRC")
	if m.Config.Txsrc == "" {
		// INVAR: No -tx flag on the command line.
		// We defer to the environment, and then the DefaultTxsrc
		if envTxsrc == "" {
			// no env variable requested either.
			m.Config.Txsrc = pilosa.DefaultTxsrc
		} else {
			// Tell the "regular" prod server what to use.
			m.Config.Txsrc = envTxsrc
		}
	}
	// INVAR: m.Config.Txsrc is valid and not "", but pilosa.DefaultTxsrc could be bad.
	txty := pilosa.MustTxsrcToTxtype(m.Config.Txsrc) // will panic on unknown Txsrc.
	os.Setenv("PILOSA_TXSRC", m.Config.Txsrc)
	m.logger.Printf("using Txsrc '%v'/%v", m.Config.Txsrc, txty)
	if len(txty) == 2 {
		m.logger.Printf("blue='%v' / green='%v'", txty[0], txty[1])
	}

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

	// create gRPC listener
	m.grpcLn, err = net.Listen("tcp", grpcURI.HostPort())
	if err != nil {
		return errors.Wrap(err, "creating grpc listener")
	}

	// If grpc port is 0, get auto-allocated port from listener
	if grpcURI.Port == 0 {
		grpcURI.SetPort(uint16(m.grpcLn.Addr().(*net.TCPAddr).Port))
	}

	// Setup TLS
	if uri.Scheme == "https" {
		m.tlsConfig, err = GetTLSConfig(&m.Config.TLS, m.logger.Logger())
		if err != nil {
			return errors.Wrap(err, "get tls config")
		}
	}

	diagnosticsInterval := time.Duration(0)
	if m.Config.Metric.Diagnostics {
		diagnosticsInterval = defaultDiagnosticsInterval
	}

	statsClient, err := newStatsClient(m.Config.Metric.Service, m.Config.Metric.Host)
	if err != nil {
		return errors.Wrap(err, "new stats client")
	}

	m.ln, err = getListener(*uri, m.tlsConfig)
	if err != nil {
		return errors.Wrap(err, "getting listener")
	}

	// If port is 0, get auto-allocated port from listener
	if uri.Port == 0 {
		uri.SetPort(uint16(m.ln.Addr().(*net.TCPAddr).Port))
	}

	// Save listenURI for later reference.
	m.listenURI = uri

	c := http.GetHTTPClient(m.tlsConfig)

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
		m.logger.Printf("DEPRECATED: The primary-url configuration option is no longer used.")
	}
	// Handle renamed and deprecated config parameter
	longQueryTime := m.Config.LongQueryTime
	if m.Config.Cluster.LongQueryTime >= 0 {
		longQueryTime = m.Config.Cluster.LongQueryTime
		m.logger.Printf("DEPRECATED: Configuration parameter cluster.long-query-time has been renamed to long-query-time")
	}

	// Set Coordinator.
	coordinatorOpt := pilosa.OptServerIsCoordinator(false)
	if m.Config.Cluster.Coordinator || len(m.Config.Gossip.Seeds) == 0 {
		coordinatorOpt = pilosa.OptServerIsCoordinator(true)
	}

	// If a DisCo.Dir is not provided, nest a default under the pilosa data dir.
	if m.Config.DisCo.Dir == "" {
		path, err := expandDirName(m.Config.DataDir)
		if err != nil {
			return errors.Wrapf(err, "expanding directory name: %s", m.Config.DataDir)
		}
		m.Config.DisCo.Dir = filepath.Join(path, pilosa.DefaultDiscoDir)
	}

	e := petcd.NewEtcd(m.Config.DisCo, m.Config.Cluster.ReplicaN)
	n := petcd.NewNoder(m.Config.DisCo, m.Config.Cluster.ReplicaN)
	discoOpt := pilosa.OptServerDisCo(e, e, e, e, n, e, e)

	serverOptions := []pilosa.ServerOption{
		pilosa.OptServerAntiEntropyInterval(time.Duration(m.Config.AntiEntropy.Interval)),
		pilosa.OptServerLongQueryTime(time.Duration(longQueryTime)),
		pilosa.OptServerDataDir(m.Config.DataDir),
		pilosa.OptServerReplicaN(m.Config.Cluster.ReplicaN),
		pilosa.OptServerMaxWritesPerRequest(m.Config.MaxWritesPerRequest),
		pilosa.OptServerMetricInterval(time.Duration(m.Config.Metric.PollInterval)),
		pilosa.OptServerDiagnosticsInterval(diagnosticsInterval),
		pilosa.OptServerExecutorPoolSize(m.Config.WorkerPoolSize),
		pilosa.OptServerOpenTranslateStore(boltdb.OpenTranslateStore),
		pilosa.OptServerOpenTranslateReader(http.GetOpenTranslateReaderWithLockerFunc(c, &sync.Mutex{})),
		pilosa.OptServerOpenIDAllocator(pilosa.OpenIDAllocator),
		pilosa.OptServerLogger(m.logger),
		pilosa.OptServerAttrStoreFunc(boltdb.NewAttrStore),
		pilosa.OptServerSystemInfo(gopsutil.NewSystemInfo()),
		pilosa.OptServerGCNotifier(gcnotify.NewActiveGCNotifier()),
		pilosa.OptServerStatsClient(statsClient),
		pilosa.OptServerURI(advertiseURI),
		pilosa.OptServerGRPCURI(advertiseGRPCURI),
		pilosa.OptServerInternalClient(http.NewInternalClientFromURI(uri, c)),
		pilosa.OptServerClusterDisabled(m.Config.Cluster.Disabled, m.Config.Cluster.Hosts),
		pilosa.OptServerClusterName(m.Config.Cluster.Name),
		pilosa.OptServerSerializer(proto.Serializer{}),
		pilosa.OptServerTxsrc(m.Config.Txsrc),
		pilosa.OptServerRowcacheOn(m.Config.RowcacheOn),
		pilosa.OptServerRBFConfig(m.Config.RBFConfig),
		pilosa.OptServerQueryHistoryLength(m.Config.QueryHistoryLength),
		coordinatorOpt,
		discoOpt,
	}

	serverOptions = append(serverOptions, m.serverOptions...)

	m.Server, err = pilosa.NewServer(serverOptions...)

	if err != nil {
		return errors.Wrap(err, "new server")
	}

	m.API, err = pilosa.NewAPI(
		pilosa.OptAPIServer(m.Server),
		pilosa.OptAPIImportWorkerPoolSize(m.Config.ImportWorkerPoolSize),
	)
	if err != nil {
		return errors.Wrap(err, "new api")
	}

	m.grpcServer, err = NewGRPCServer(
		OptGRPCServerAPI(m.API),
		OptGRPCServerListener(m.grpcLn),
		OptGRPCServerTLSConfig(m.tlsConfig),
		OptGRPCServerLogger(m.logger),
		OptGRPCServerStats(statsClient),
	)
	if err != nil {
		return errors.Wrap(err, "new grpc server")
	}

	m.Handler, err = http.NewHandler(
		http.OptHandlerAllowedOrigins(m.Config.Handler.AllowedOrigins),
		http.OptHandlerAPI(m.API),
		http.OptHandlerLogger(m.logger),
		http.OptHandlerFileSystem(&statik.FileSystem{}),
		http.OptHandlerListener(m.ln),
		http.OptHandlerCloseTimeout(m.closeTimeout),
		http.OptHandlerMiddleware(m.grpcServer.middleware(m.Config.Handler.AllowedOrigins)),
	)
	return errors.Wrap(err, "new handler")
}

// setupNetworking sets up internode communication based on the configuration.
func (m *Command) setupNetworking() error {
	if m.Config.Cluster.Disabled {
		return nil
	}

	gossipPort, err := strconv.Atoi(m.Config.Gossip.Port)
	if err != nil {
		return errors.Wrap(err, "parsing port")
	}

	// get the host portion of addr to use for binding
	gossipHost := m.listenURI.Host
	m.gossipTransport, err = gossip.NewTransport(gossipHost, gossipPort, m.logger.Logger())
	if err != nil && gossipPort >= 32768 {
		// In testing, we sometimes try to reuse an ephemeral port.
		// Which probably works. If it doesn't, this test will take
		// about a minute longer because we'll come back in from a
		// new port. See also the gossip config in gossip/gossip.go.
		// TODO: Maybe make that more configurable here.
		m.logger.Printf("ephemeral port %d already occupied, switching to :0 (%v)", gossipPort, err)
		if err := port.GetPort(func(p int) error {
			gossipPort = p
			m.Config.Gossip.Port = fmt.Sprintf(":%d", gossipPort)
			m.gossipTransport, err = gossip.NewTransport(gossipHost, gossipPort, m.logger.Logger())
			return err
		}, 10); err != nil {
			return errors.Wrap(err, "getting transport")
		}

	}

	gossipMemberSet, err := gossip.NewMemberSet(
		m.Config.Gossip,
		m.API,
		gossip.WithLogOutput(&filteredWriter{logOutput: m.logOutput, v: m.Config.Verbose}),
		gossip.WithPilosaLogger(m.logger),
		gossip.WithTransport(m.gossipTransport),
	)
	if err != nil {
		return errors.Wrap(err, "getting memberset")
	}
	m.gossipMemberSet = gossipMemberSet

	return errors.Wrap(gossipMemberSet.Open(), "opening gossip memberset")
}

// setupLogger sets up the logger based on the configuration.
func (m *Command) setupLogger() error {
	var f *logger.FileWriter
	var err error
	if m.Config.LogPath == "" {
		m.logOutput = m.Stderr
	} else {
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
					m.logger.Printf("syscall dup: %s\n", err.Error())
				}

				// reopen log file on SIGHUP
				<-sighup
				err = f.Reopen()
				if err != nil {
					m.logger.Printf("reopen: %s\n", err.Error())
				}
			}
		}()
	}
	return nil
}

// GossipTransport allows a caller to return the gossip transport created when
// setting up the GossipMemberSet. This is useful if one needs to determine the
// allocated ephemeral port programmatically. (usually used in tests)
func (m *Command) GossipTransport() *gossip.Transport {
	return m.gossipTransport
}

// Close shuts down the server.
func (m *Command) Close() error {
	defer close(m.done)
	eg := errgroup.Group{}
	m.grpcServer.Stop()
	eg.Go(m.Handler.Close)
	eg.Go(m.Server.Close)
	eg.Go(m.API.Close)
	eg.Go(m.pgserver.Close)
	if m.gossipMemberSet != nil {
		eg.Go(m.gossipMemberSet.Close)
	}
	if closer, ok := m.logOutput.(io.Closer); ok {
		// If closer is os.Stdout or os.Stderr, don't close it.
		if closer != os.Stdout && closer != os.Stderr {
			eg.Go(closer.Close)
		}
	}

	err := eg.Wait()
	_ = testhook.Closed(pilosa.NewAuditor(), m, nil)
	return errors.Wrap(err, "closing everything")
}

// newStatsClient creates a stats client from the config
func newStatsClient(name string, host string) (stats.StatsClient, error) {
	switch name {
	case "expvar":
		return stats.NewExpvarStatsClient(), nil
	case "statsd":
		return statsd.NewStatsClient(host)
	case "prometheus":
		return prometheus.NewPrometheusClient()
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

type filteredWriter struct {
	v         bool
	logOutput io.Writer
}

// Write forwards the write to logOutput if verbose is true, or it doesn't
// contain [DEBUG] or [INFO]. This implementation isn't technically correct
// since Write could be called with only part of a log line, but I don't think
// that actually happens, so until it becomes a problem, I don't think it's
// worth dealing with the extra complexity. (jaffee)
func (f *filteredWriter) Write(p []byte) (n int, err error) {
	if bytes.Contains(p, []byte("[DEBUG]")) || bytes.Contains(p, []byte("[INFO]")) {
		if f.v {
			return f.logOutput.Write(p)
		}
	} else {
		return f.logOutput.Write(p)
	}
	return len(p), nil
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
