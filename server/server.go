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
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/gossip"
	"github.com/pilosa/pilosa/httpbroadcast"
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

	m.Server.Logger().Printf("Listening as http://%s\n", m.Server.Host)
	return nil
}

// SetupServer use the cluster configuration to setup this server
func (m *Command) SetupServer() error {
	err := m.Config.Validate()
	if err != nil {
		return err
	}

	cluster := pilosa.NewCluster()
	cluster.ReplicaN = m.Config.Cluster.ReplicaN

	for _, hostport := range m.Config.Cluster.Hosts {
		cluster.Nodes = append(cluster.Nodes, &pilosa.Node{Host: hostport})
	}
	// TODO: if InternalHosts is not provided then pilosa.Node.InternalHost is empty.
	// This will throw an error when trying to Broadcast messages over HTTP.
	// One option may be to fall back to using host from hostport + config.InternalPort.
	for i, internalhostport := range m.Config.Cluster.InternalHosts {
		cluster.Nodes[i].InternalHost = internalhostport
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
	m.Server.Holder.Stats, err = NewStatsClient(m.Config.Metric.Service, m.Config.Metric.Host)
	if err != nil {
		return err
	}

	m.Server.Holder.Stats.SetLogger(m.Server.LogOutput)

	// Copy configuration flags.
	m.Server.MaxWritesPerRequest = m.Config.MaxWritesPerRequest

	m.Server.Host, err = normalizeHost(m.Config.Bind)
	if err != nil {
		return err
	}

	// Set internal port (string).
	internalPortStr := pilosa.DefaultInternalPort
	if m.Config.InternalPort != "" {
		internalPortStr = m.Config.InternalPort
	}

	switch m.Config.Cluster.Type {
	case pilosa.ClusterHTTP:
		m.Server.Broadcaster = httpbroadcast.NewHTTPBroadcaster(m.Server, internalPortStr)
		m.Server.BroadcastReceiver = httpbroadcast.NewHTTPBroadcastReceiver(internalPortStr, m.Server.LogOutput)
		m.Server.Cluster.NodeSet = httpbroadcast.NewHTTPNodeSet()
		err := m.Server.Cluster.NodeSet.(*httpbroadcast.HTTPNodeSet).Join(m.Server.Cluster.Nodes)
		if err != nil {
			return err
		}
	case pilosa.ClusterGossip:
		gossipPort, err := strconv.Atoi(internalPortStr)
		if err != nil {
			return err
		}
		gossipSeed := pilosa.DefaultHost
		if m.Config.Cluster.GossipSeed != "" {
			gossipSeed = m.Config.Cluster.GossipSeed
		}
		// get the host portion of addr to use for binding
		gossipHost, _, err := net.SplitHostPort(m.Config.Bind)
		if err != nil {
			gossipHost = m.Config.Bind
		}
		gossipNodeSet := gossip.NewGossipNodeSet(m.Config.Bind, gossipHost, gossipPort, gossipSeed, m.Server)
		m.Server.Cluster.NodeSet = gossipNodeSet
		m.Server.Broadcaster = gossipNodeSet
		m.Server.BroadcastReceiver = gossipNodeSet
	case pilosa.ClusterStatic, pilosa.ClusterNone:
		m.Server.Broadcaster = pilosa.NopBroadcaster
		m.Server.Cluster.NodeSet = pilosa.NewStaticNodeSet()
		m.Server.BroadcastReceiver = pilosa.NopBroadcastReceiver
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

func normalizeHost(host string) (string, error) {
	if !strings.Contains(host, ":") {
		host = host + ":"
	} else if strings.Contains(host, "://") {
		if strings.HasPrefix(host, "http://") {
			host = host[7:]
		} else {
			return "", fmt.Errorf("invalid scheme or host: '%s'. use the format [http://]<host>:<port>", host)
		}
	}
	return host, nil
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
