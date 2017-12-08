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

	"io/ioutil"

	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/gossip"
	"github.com/pilosa/pilosa/statsd"
)

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
}

const (
	// DefaultDataDir is the default data directory.
	DefaultDataDir = "~/.pilosa"

	// DefaultDiagnosticsInterval is the default sync frequency diagnostic metrics.
	DefaultDiagnosticsInterval = 1 * time.Hour
)

// Command represents the state of the pilosa server command.
type Command struct {
	Server *pilosa.Server

	// Configuration.
	Config *pilosa.Config

	// Profiling options.
	CPUProfile string
	CPUTime    time.Duration

	// Standard input/output
	*pilosa.CmdIO

	// running will be closed once Command.Run is finished.
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

	// Initialize server.
	if err = m.Server.Open(); err != nil {
		return fmt.Errorf("server.Open: %v", err)
	}

	m.Server.Logger().Printf("Listening as %s\n", m.Server.URI.Normalize())
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
	m.Server.URI = uri

	cluster := pilosa.NewCluster()
	cluster.ReplicaN = m.Config.Cluster.ReplicaN

	for _, address := range m.Config.Cluster.Hosts {
		uri, err := pilosa.NewURIFromAddress(address)
		if err != nil {
			return err
		}
		cluster.Nodes = append(cluster.Nodes, &pilosa.Node{
			Scheme: uri.Scheme(),
			Host:   uri.HostPort(),
		})
	}
	m.Server.Cluster = cluster

	// Setup logging output.
	m.Server.LogOutput, err = GetLogWriter(m.Config.LogPath, m.Stderr)
	if err != nil {
		return err
	}

	// Configure holder.
	m.Server.Logger().Printf("Using data from: %s\n", m.Config.DataDir)
	m.Server.Holder.Path = m.Config.DataDir
	m.Server.MetricInterval = time.Duration(m.Config.Metric.PollInterval)
	if m.Config.Metric.Diagnostics {
		m.Server.DiagnosticInterval = time.Duration(DefaultDiagnosticsInterval)
	}
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

	// Set internal port (string).
	gossipPortStr := pilosa.DefaultGossipPort
	// Config.GossipPort is deprecated, so Config.Gossip.Port has priority
	if m.Config.Gossip.Port != "" {
		gossipPortStr = m.Config.Gossip.Port
	} else if m.Config.GossipPort != "" {
		gossipPortStr = m.Config.GossipPort
	}

	switch m.Config.Cluster.Type {
	case pilosa.ClusterGossip:
		gossipPort, err := strconv.Atoi(gossipPortStr)
		if err != nil {
			return err
		}
		gossipSeed := pilosa.DefaultHost + ":" + pilosa.DefaultGossipPort
		// Config.GossipSeed is deprecated, so Config.Gossip.Seed has priority
		if m.Config.Gossip.Seed != "" {
			gossipSeed = m.Config.Gossip.Seed
		} else if m.Config.GossipSeed != "" {
			gossipSeed = m.Config.GossipSeed
		}

		var gossipKey []byte
		if m.Config.Gossip.Key != "" {
			gossipKey, err = ioutil.ReadFile(m.Config.Gossip.Key)
			if err != nil {
				return err
			}
		}

		// get the host portion of addr to use for binding
		gossipHost := uri.Host()
		gossipNodeSet, err := gossip.NewGossipNodeSet(uri.HostPort(), gossipHost, gossipPort, gossipSeed, m.Server, gossipKey)
		if err != nil {
			return err
		}
		m.Server.Cluster.NodeSet = gossipNodeSet
		m.Server.Broadcaster = m.Server
		m.Server.BroadcastReceiver = gossipNodeSet
		m.Server.Gossiper = gossipNodeSet
	case pilosa.ClusterStatic, pilosa.ClusterNone:
		m.Server.Broadcaster = pilosa.NopBroadcaster
		m.Server.Cluster.NodeSet = pilosa.NewStaticNodeSet()
		m.Server.BroadcastReceiver = pilosa.NopBroadcastReceiver
		m.Server.Gossiper = pilosa.NopGossiper
		err := m.Server.Cluster.NodeSet.(*pilosa.StaticNodeSet).Join(m.Server.Cluster.Nodes)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("'%v' is not a supported value for broadcaster type", m.Config.Cluster.Type)
	}

	// Set configuration options.
	m.Server.AntiEntropyInterval = time.Duration(m.Config.AntiEntropy.Interval)
	m.Server.Cluster.LongQueryTime = time.Duration(m.Config.Cluster.LongQueryTime)
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
