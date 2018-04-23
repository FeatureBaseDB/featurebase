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

// Package server contains the `pilosa server` subcommand which runs Pilosa
// itself. The purpose of this package is to define an easily tested Command
// object which handles interpreting configuration and setting up all the
// objects that Pilosa needs.

package server

import (
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"crypto/tls"

	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/boltdb"
	"github.com/pilosa/pilosa/gcnotify"
	"github.com/pilosa/pilosa/gopsutil"
	"github.com/pilosa/pilosa/gossip"
	"github.com/pilosa/pilosa/statik"
	"github.com/pilosa/pilosa/statsd"
	"github.com/pkg/errors"
)

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
}

type loggerLogger interface {
	pilosa.Logger
	Logger() *log.Logger
}

// Command represents the state of the pilosa server command.
type Command struct {
	Server *pilosa.Server

	// Configuration.
	Config *Config

	// Gossip transport
	GossipTransport *gossip.Transport

	// Standard input/output
	*pilosa.CmdIO

	// Started will be closed once Command.Run is finished.
	Started chan struct{}
	// Done will be closed when Command.Close() is called
	Done chan struct{}

	// Passed to the Gossip implementation.
	logOutput io.Writer
	logger    loggerLogger
}

// NewCommand returns a new instance of Main.
func NewCommand(stdin io.Reader, stdout, stderr io.Writer) *Command {
	return &Command{
		Config: NewConfig(),

		CmdIO: pilosa.NewCmdIO(stdin, stdout, stderr),

		Started: make(chan struct{}),
		Done:    make(chan struct{}),
	}
}

// Run executes the pilosa server.
func (m *Command) Run(args ...string) (err error) { // TODO args WTF
	defer close(m.Started)
	prefix := "~" + string(filepath.Separator)
	if strings.HasPrefix(m.Config.DataDir, prefix) {
		HomeDir := os.Getenv("HOME")
		if HomeDir == "" {
			return errors.New("data directory not specified and no home dir available")
		}
		m.Config.DataDir = filepath.Join(HomeDir, strings.TrimPrefix(m.Config.DataDir, prefix))
	}

	// SetupServer
	err = m.SetupServer()
	if err != nil {
		return err
	}

	// SetupNetworking
	err = m.SetupNetworking()
	if err != nil {
		return err
	}

	// Initialize server.
	if err = m.Server.Open(); err != nil {
		return fmt.Errorf("server.Open: %v", err)
	}

	m.logger.Printf("Listening as %s\n", m.Server.URI)

	return nil
}

// Wait waits for the server to be closed or interrupted.
func (m *Command) Wait() error {
	// First SIGKILL causes server to shut down gracefully.
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	select {
	case sig := <-c:
		m.logger.Printf("Received %s; gracefully shutting down...\n", sig.String())

		// Second signal causes a hard shutdown.
		go func() { <-c; os.Exit(1) }()
		return errors.Wrap(m.Close(), "closing command")
	case <-m.Done:
		m.logger.Printf("Server closed externally")
		return nil
	}
}

// setupLogger sets up the logger based on the configuration.
func (m *Command) setupLogger() error {
	var err error
	if m.Config.LogPath == "" {
		m.logOutput = m.Stderr
	} else {
		m.logOutput, err = os.OpenFile(m.Config.LogPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
		if err != nil {
			return errors.Wrap(err, "opening file")
		}
	}

	if m.Config.Verbose {
		m.logger = pilosa.NewVerboseLogger(m.logOutput)
	} else {
		m.logger = pilosa.NewStandardLogger(m.logOutput)
	}
	return nil
}

// SetupServer uses the cluster configuration to set up this server.
func (m *Command) SetupServer() error {
	err := m.setupLogger()
	if err != nil {
		return errors.Wrap(err, "setting up logger")
	}

	handler := pilosa.NewHandler()
	handler.Logger = m.logger
	handler.FileSystem = &statik.FileSystem{}
	handler.API = pilosa.NewAPI()
	handler.API.Logger = m.logger

	uri, err := pilosa.AddressWithDefaults(m.Config.Bind)
	if err != nil {
		return errors.Wrap(err, "processing bind address")
	}

	// Setup TLS
	var TLSConfig *tls.Config
	if uri.Scheme() == "https" {
		if m.Config.TLS.CertificatePath == "" {
			return errors.New("certificate path is required for TLS sockets")
		}
		if m.Config.TLS.CertificateKeyPath == "" {
			return errors.New("certificate key path is required for TLS sockets")
		}
		cert, err := tls.LoadX509KeyPair(m.Config.TLS.CertificatePath, m.Config.TLS.CertificateKeyPath)
		if err != nil {
			return errors.Wrap(err, "load x509 key pair")
		}
		TLSConfig = &tls.Config{
			Certificates:       []tls.Certificate{cert},
			InsecureSkipVerify: m.Config.TLS.SkipVerify,
		}
	}

	diagnosticsInterval := time.Duration(0)
	if m.Config.Metric.Diagnostics {
		diagnosticsInterval = time.Duration(DefaultDiagnosticsInterval)
	}

	statsClient, err := NewStatsClient(m.Config.Metric.Service, m.Config.Metric.Host)
	if err != nil {
		return errors.Wrap(err, "new stats client")
	}

	ln, err := getListener(*uri, TLSConfig)
	if err != nil {
		return errors.Wrap(err, "getting listener")
	}

	c := GetHTTPClient(TLSConfig)
	handler.API.RemoteClient = c

	m.Server, err = pilosa.NewServer(
		pilosa.OptServerAntiEntropyInterval(time.Duration(m.Config.AntiEntropy.Interval)),
		pilosa.OptServerLongQueryTime(time.Duration(m.Config.Cluster.LongQueryTime)),
		pilosa.OptServerDataDir(m.Config.DataDir),
		pilosa.OptServerReplicaN(m.Config.Cluster.ReplicaN),
		pilosa.OptServerMaxWritesPerRequest(m.Config.MaxWritesPerRequest),
		pilosa.OptServerMetricInterval(time.Duration(m.Config.Metric.PollInterval)),
		pilosa.OptServerDiagnosticsInterval(diagnosticsInterval),

		pilosa.OptServerLogger(m.logger),
		pilosa.OptServerAttrStoreFunc(boltdb.NewAttrStore),
		pilosa.OptServerHandler(handler),
		pilosa.OptServerSystemInfo(gopsutil.NewSystemInfo()),
		pilosa.OptServerGCNotifier(gcnotify.NewActiveGCNotifier()),
		pilosa.OptServerStatsClient(statsClient),
		pilosa.OptServerListener(ln),
		pilosa.OptServerURI(uri),
		pilosa.OptServerRemoteClient(c),
	)

	return errors.Wrap(err, "new server")
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

// SetupNetworking sets up internode communication based on the configuration.
func (m *Command) SetupNetworking() error {

	m.Server.NodeID = m.Server.LoadNodeID()

	if m.Config.Cluster.Disabled {
		m.Server.Cluster.Static = true
		m.Server.Cluster.Coordinator = m.Server.NodeID
		for _, address := range m.Config.Cluster.Hosts {
			uri, err := pilosa.NewURIFromAddress(address)
			if err != nil {
				return err
			}
			m.Server.Cluster.Nodes = append(m.Server.Cluster.Nodes, &pilosa.Node{
				URI: *uri,
			})
		}

		m.Server.Broadcaster = pilosa.NopBroadcaster
		m.Server.Cluster.MemberSet = pilosa.NewStaticMemberSet(m.Server.Cluster.Nodes)
		m.Server.BroadcastReceiver = pilosa.NopBroadcastReceiver
		m.Server.Gossiper = pilosa.NopGossiper
		return nil
	}

	gossipPort, err := strconv.Atoi(m.Config.Gossip.Port)
	if err != nil {
		return err
	}

	// get the host portion of addr to use for binding
	gossipHost := m.Server.URI.Host()
	var transport *gossip.Transport
	if m.GossipTransport != nil {
		transport = m.GossipTransport
	} else {
		transport, err = gossip.NewTransport(gossipHost, gossipPort, m.logger.Logger())
		if err != nil {
			return err
		}
	}

	// Set Coordinator.
	if m.Config.Cluster.Coordinator || len(m.Config.Gossip.Seeds) == 0 {
		m.Server.Cluster.Coordinator = m.Server.NodeID
	}

	gossipEventReceiver := gossip.NewGossipEventReceiver(m.logger)
	m.Server.Cluster.EventReceiver = gossipEventReceiver
	gossipMemberSet, err := gossip.NewGossipMemberSet(m.Server.NodeID, m.Server.URI.Host(), m.Config.Gossip, gossipEventReceiver, m.Server, gossip.WithLogger(m.logger.Logger()), gossip.WithTransport(transport))
	if err != nil {
		return err
	}
	m.Server.Cluster.MemberSet = gossipMemberSet
	m.Server.Broadcaster = m.Server
	m.Server.BroadcastReceiver = gossipMemberSet
	m.Server.Gossiper = gossipMemberSet
	return nil
}

// Close shuts down the server.
func (m *Command) Close() error {
	var logErr error
	serveErr := m.Server.Close()
	if closer, ok := m.logOutput.(io.Closer); ok {
		logErr = closer.Close()
	}
	close(m.Done)
	if serveErr != nil && logErr != nil {
		return fmt.Errorf("closing server: '%v', closing logs: '%v'", serveErr, logErr)
	} else if logErr != nil {
		return logErr
	}
	return serveErr
}

// NewStatsClient creates a stats client from the config
func NewStatsClient(name string, host string) (pilosa.StatsClient, error) {
	switch name {
	case "expvar":
		return pilosa.NewExpvarStatsClient(), nil
	case "statsd":
		return statsd.NewStatsClient(host)
	case "nop", "none":
		return pilosa.NopStatsClient, nil
	default:
		return nil, errors.Errorf("'%v' not a valid stats client, choose from [expvar, statsd, none].")
	}
}

// getListener gets a net.Listener based on the config.
func getListener(uri pilosa.URI, tlsconf *tls.Config) (ln net.Listener, err error) {
	// If bind URI has the https scheme, enable TLS
	if uri.Scheme() == "https" && tlsconf != nil {
		ln, err = tls.Listen("tcp", uri.HostPort(), tlsconf)
		if err != nil {
			return nil, errors.Wrap(err, "tls.Listener")
		}
	} else if uri.Scheme() == "http" {
		// Open HTTP listener to determine port (if specified as :0).
		ln, err = net.Listen("tcp", uri.HostPort())
		if err != nil {
			return nil, errors.Wrap(err, "net.Listen")
		}
	} else {
		return nil, errors.Errorf("unsupported scheme: %s", uri.Scheme())
	}

	return ln, nil
}
