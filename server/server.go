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
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"crypto/tls"

	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/gcnotify"
	"github.com/pilosa/pilosa/gossip"
	"github.com/pilosa/pilosa/statik"
	"github.com/pilosa/pilosa/statsd"
)

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
}

const (
	// DefaultDataDir is the default data directory.
	DefaultDataDir = "~/.pilosa"
)

// Command represents the state of the pilosa server command.
type Command struct {
	Server *pilosa.Server

	// Configuration.
	Config *pilosa.Config

	// Profiling options.
	CPUProfile string
	CPUTime    time.Duration

	// Gossip transport
	GossipTransport *gossip.Transport

	// Standard input/output
	*pilosa.CmdIO

	// Started will be closed once Command.Run is finished.
	Started chan struct{}
	// Done will be closed when Command.Close() is called
	Done chan struct{}
}

// NewCommand returns a new instance of Main.
func NewCommand(stdin io.Reader, stdout, stderr io.Writer) *Command {
	return &Command{
		Server: pilosa.NewServer(),
		Config: pilosa.NewConfig(),

		CmdIO: pilosa.NewCmdIO(stdin, stdout, stderr),

		Started: make(chan struct{}),
		Done:    make(chan struct{}),
	}
}

// Run executes the pilosa server.
func (m *Command) Run(args ...string) (err error) {
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

	m.Server.Logger().Printf("Listening as %s\n", m.Server.URI)
	return nil
}

// SetupServer uses the cluster configuration to set up this server.
func (m *Command) SetupServer() error {
	err := m.Config.Validate()
	if err != nil {
		return err
	}

	uri, err := pilosa.AddressWithDefaults(m.Config.Bind)

	if err != nil {
		return err
	}
	m.Server.URI = *uri

	cluster := pilosa.NewCluster()
	cluster.ReplicaN = m.Config.Cluster.ReplicaN
	cluster.Holder = m.Server.Holder

	m.Server.Cluster = cluster

	// Setup logging output.
	m.Server.LogOutput, err = GetLogWriter(m.Config.LogPath, m.Stderr)
	if err != nil {
		return err
	}

	// Configure data directory (for Cluster .topology)
	m.Server.Cluster.Path = m.Config.DataDir

	// Configure holder.
	m.Server.Logger().Printf("Using data from: %s\n", m.Config.DataDir)
	m.Server.Holder.Path = m.Config.DataDir
	m.Server.MetricInterval = time.Duration(m.Config.Metric.PollInterval)
	if m.Config.Metric.Diagnostics {
		m.Server.DiagnosticInterval = time.Duration(DefaultDiagnosticsInterval)
	}
	m.Server.GCNotifier = gcnotify.NewActiveGCNotifier()
	m.Server.Holder.Stats, err = NewStatsClient(m.Config.Metric.Service, m.Config.Metric.Host)
	if err != nil {
		return err
	}

	m.Server.Holder.Stats.SetLogger(m.Server.LogOutput)

	// Copy configuration flags.
	m.Server.MaxWritesPerRequest = m.Config.MaxWritesPerRequest

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
			return err
		}
		m.Server.TLS = &tls.Config{
			Certificates:       []tls.Certificate{cert},
			InsecureSkipVerify: m.Config.TLS.SkipVerify,
		}

		// TODO Review this location

		TLSConfig = m.Server.TLS

	}
	c := pilosa.GetHTTPClient(TLSConfig)
	m.Server.RemoteClient = c
	m.Server.Handler.RemoteClient = c
	m.Server.Cluster.RemoteClient = c

	// Statik file system.
	m.Server.Handler.FileSystem = &statik.FileSystem{}

	// Default coordintor to port 0 when not specified so that coordinator
	// can be set to the value of server.URI after server binds to a port.
	// This would only be useful in a one-node cluster.
	coord := m.Config.Cluster.Coordinator
	if coord == "" {
		coord = ":0"
	}

	// Set the coordinator node.
	curi, err := pilosa.AddressWithDefaults(coord)
	if err != nil {
		return err
	}
	m.Server.Cluster.Coordinator = *curi

	// Set configuration options.
	m.Server.AntiEntropyInterval = time.Duration(m.Config.AntiEntropy.Interval)
	m.Server.Cluster.LongQueryTime = time.Duration(m.Config.Cluster.LongQueryTime)
	return nil
}

// SetupNetworking sets up internode communication based on the configuration.
func (m *Command) SetupNetworking() error {
	if m.Config.Cluster.Disabled {
		m.Server.Cluster.Static = true
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
		m.Server.Cluster.MemberSet = pilosa.NewStaticMemberSet()
		m.Server.BroadcastReceiver = pilosa.NopBroadcastReceiver
		m.Server.Gossiper = pilosa.NopGossiper
		err := m.Server.Cluster.MemberSet.(*pilosa.StaticMemberSet).Join(m.Server.Cluster.Nodes)
		if err != nil {
			return err
		}
		return nil
	}

	// Set internal port (string).
	gossipPortStr := pilosa.DefaultGossipPort
	if m.Config.Gossip.Port != "" {
		gossipPortStr = m.Config.Gossip.Port
	}

	gossipPort, err := strconv.Atoi(gossipPortStr)
	if err != nil {
		return err
	}

	// get the host portion of addr to use for binding
	gossipHost := m.Server.URI.Host()
	var transport *gossip.Transport
	if m.GossipTransport != nil {
		transport = m.GossipTransport
	} else {
		transport, err = gossip.NewTransport(gossipHost, gossipPort)
		if err != nil {
			return err
		}
	}

	m.Server.NodeID = m.Server.LoadNodeID()

	m.Server.Cluster.EventReceiver = gossip.NewGossipEventReceiver(m.Server.LogOutput)
	gossipMemberSet, err := gossip.NewGossipMemberSetWithTransport(m.Server.NodeID, m.Config, transport, m.Server)
	if err != nil {
		return err
	}
	m.Server.Cluster.MemberSet = gossipMemberSet
	m.Server.Broadcaster = m.Server
	m.Server.BroadcastReceiver = gossipMemberSet
	m.Server.Gossiper = gossipMemberSet
	return nil
}

// GetLogWriter opens a file for logging, or a default io.Writer (such as stderr) for an empty path.
func GetLogWriter(path string, defaultWriter io.Writer) (io.Writer, error) {
	// This is split out so it can be used in NewServeCmd as well as SetupServer
	if path == "" {
		return defaultWriter, nil
	} else {
		logFile, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0600)
		if err != nil {
			return nil, err
		}
		return logFile, nil
	}
}

// Close shuts down the server.
func (m *Command) Close() error {
	var logErr error
	serveErr := m.Server.Close()
	logOutput := m.Server.LogOutput
	if closer, ok := logOutput.(io.Closer); ok {
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
	default:
		return pilosa.NopStatsClient, nil
	}
}
