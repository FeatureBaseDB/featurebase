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
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/pilosa/pilosa/v2"
	"github.com/pilosa/pilosa/v2/boltdb"
	"github.com/pilosa/pilosa/v2/encoding/proto"
	"github.com/pilosa/pilosa/v2/gcnotify"
	"github.com/pilosa/pilosa/v2/gopsutil"
	"github.com/pilosa/pilosa/v2/gossip"
	"github.com/pilosa/pilosa/v2/http"
	"github.com/pilosa/pilosa/v2/logger"
	"github.com/pilosa/pilosa/v2/prometheus"
	"github.com/pilosa/pilosa/v2/stats"
	"github.com/pilosa/pilosa/v2/statsd"
	"github.com/pilosa/pilosa/v2/syswrap"
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
	API          *pilosa.API
	ln           net.Listener
	listenURI    *pilosa.URI
	closeTimeout time.Duration

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
	defer close(m.Started)

	// Seed random number generator
	rand.Seed(time.Now().UTC().UnixNano())

	// SetupServer
	err = m.SetupServer()
	if err != nil {
		return errors.Wrap(err, "setting up server")
	}

	// SetupNetworking
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

	syswrap.SetMaxMapCount(m.Config.MaxMapCount)
	syswrap.SetMaxFileCount(m.Config.MaxFileCount)

	err := m.setupLogger()
	if err != nil {
		return errors.Wrap(err, "setting up logger")
	}

	productName := "Pilosa"
	if pilosa.EnterpriseEnabled {
		productName += " Enterprise"
	}
	m.logger.Printf("%s %s, build time %s\n", productName, pilosa.Version, pilosa.BuildTime)

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

	// Setup TLS
	var TLSConfig *tls.Config
	if uri.Scheme == "https" {
		TLSConfig, err = GetTLSConfig(&m.Config.TLS, m.logger.Logger())
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

	m.ln, err = getListener(*uri, TLSConfig)
	if err != nil {
		return errors.Wrap(err, "getting listener")
	}

	// If port is 0, get auto-allocated port from listener
	if uri.Port == 0 {
		uri.SetPort(uint16(m.ln.Addr().(*net.TCPAddr).Port))
	}

	// Save listenURI for later reference.
	m.listenURI = uri

	c := http.GetHTTPClient(TLSConfig)

	// Get advertise address as uri.
	advertiseURI, err := pilosa.AddressWithDefaults(m.Config.Advertise)
	if err != nil {
		return errors.Wrap(err, "processing advertise address")
	}
	if advertiseURI.Port == 0 {
		advertiseURI.SetPort(uri.Port)
	}

	// Primary store configuration is handled automatically now.
	if m.Config.Translation.PrimaryURL != "" {
		m.logger.Printf("DEPRECATED: The primary-url configuration option is no longer used.")
	}

	// Set Coordinator.
	coordinatorOpt := pilosa.OptServerIsCoordinator(false)
	if m.Config.Cluster.Coordinator || len(m.Config.Gossip.Seeds) == 0 {
		coordinatorOpt = pilosa.OptServerIsCoordinator(true)
	}

	serverOptions := []pilosa.ServerOption{
		pilosa.OptServerAntiEntropyInterval(time.Duration(m.Config.AntiEntropy.Interval)),
		pilosa.OptServerLongQueryTime(time.Duration(m.Config.Cluster.LongQueryTime)),
		pilosa.OptServerDataDir(m.Config.DataDir),
		pilosa.OptServerReplicaN(m.Config.Cluster.ReplicaN),
		pilosa.OptServerMaxWritesPerRequest(m.Config.MaxWritesPerRequest),
		pilosa.OptServerMetricInterval(time.Duration(m.Config.Metric.PollInterval)),
		pilosa.OptServerDiagnosticsInterval(diagnosticsInterval),
		pilosa.OptServerExecutorPoolSize(m.Config.WorkerPoolSize),
		pilosa.OptServerLogger(m.logger),
		pilosa.OptServerAttrStoreFunc(boltdb.NewAttrStore),
		pilosa.OptServerSystemInfo(gopsutil.NewSystemInfo()),
		pilosa.OptServerGCNotifier(gcnotify.NewActiveGCNotifier()),
		pilosa.OptServerStatsClient(statsClient),
		pilosa.OptServerURI(advertiseURI),
		pilosa.OptServerInternalClient(http.NewInternalClientFromURI(uri, c)),
		pilosa.OptServerPrimaryTranslateStoreFunc(http.NewTranslateStoreWithHTTPClient(c)),
		pilosa.OptServerClusterDisabled(m.Config.Cluster.Disabled, m.Config.Cluster.Hosts),
		pilosa.OptServerSerializer(proto.Serializer{}),
		coordinatorOpt,
	}

	if m.Config.Translation.MapSize > 0 {
		serverOptions = append(
			serverOptions,
			pilosa.OptServerTranslateFileMapSize(
				m.Config.Translation.MapSize,
			),
		)
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

	m.Handler, err = http.NewHandler(
		http.OptHandlerAllowedOrigins(m.Config.Handler.AllowedOrigins),
		http.OptHandlerAPI(m.API),
		http.OptHandlerLogger(m.logger),
		http.OptHandlerListener(m.ln),
		http.OptHandlerCloseTimeout(m.closeTimeout),
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
	if err != nil {
		return errors.Wrap(err, "getting transport")
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
	eg.Go(m.Handler.Close)
	eg.Go(m.Server.Close)
	eg.Go(m.API.Close)
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
		return nil, errors.Errorf("'%v' not a valid stats client, choose from [expvar, statsd, none].", name)
	}
}

// getListener gets a net.Listener based on the config.
func getListener(uri pilosa.URI, tlsconf *tls.Config) (ln net.Listener, err error) {
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
