package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/gogo/protobuf/proto"
	"github.com/umbel/pilosa"
	"github.com/umbel/pilosa/internal"
)

// Build holds the build information passed in at compile time.
var Build string

func init() {
	if Build == "" {
		Build = "v0.0.0"
	}

	rand.Seed(time.Now().UTC().UnixNano())
}

const (
	// DefaultDataDir is the default data directory.
	DefaultDataDir = "~/.pilosa"

	// DefaultHost is the default hostname and port to use.
	DefaultHost = "localhost:15000"

	// DefaultAntiEntropyInterval is the default interval to run AAE.
	DefaultAntiEntropyInterval = 10 * time.Minute
)

func main() {
	m := NewMain()
	fmt.Fprintf(m.Stderr, "Pilosa %s\n", Build)

	// Parse command line arguments.
	if err := m.ParseFlags(os.Args[1:]); err != nil {
		fmt.Fprintln(m.Stderr, err)
		os.Exit(2)
	}

	// Execute the program.
	if err := m.Run(); err != nil {
		fmt.Fprintln(m.Stderr, err)
		os.Exit(1)
	}

	// First SIGKILL causes server to shut down gracefully.
	// Second signal causes a hard shutdown.
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt)
	sig := <-c
	fmt.Fprintf(m.Stderr, "Received %s; gracefully shutting down...\n", sig.String())
	go func() { <-c; os.Exit(1) }()

	if err := m.Close(); err != nil {
		fmt.Fprintln(m.Stderr, err)
		os.Exit(1)
	}
}

// Main represents the main program execution.
type Main struct {
	index       *pilosa.Index
	ln          net.Listener
	ticker      *time.Ticker
	pollingSecs int

	// Close management.
	wg      sync.WaitGroup
	closing chan struct{}

	// Path to the configuration file.
	ConfigPath string

	// Configuration options.
	Config *Config

	// Cluster configuration shared by components
	Host    string
	Cluster *pilosa.Cluster

	// Profiling paths
	CPUProfile string

	// Standard input/output
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}

// NewMain returns a new instance of Main.
func NewMain() *Main {
	return &Main{
		closing: make(chan struct{}),

		Config: NewConfig(),
		Stdin:  os.Stdin,
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	}
}

// Addr returns the address of the listener.
func (m *Main) Addr() net.Addr {
	if m.ln == nil {
		return nil
	}
	return m.ln.Addr()
}

// Run executes the main program execution.
func (m *Main) Run(args ...string) error {
	// Notify user of config file.
	if m.ConfigPath != "" {
		fmt.Fprintf(m.Stdout, "Using config: %s\n", m.ConfigPath)
	}

	// Require a port in the hostname.
	host, port, err := net.SplitHostPort(m.Config.Host)
	if err != nil {
		return err
	} else if port == "" {
		return errors.New("port must be specified in config host")
	}

	// Set up profiling.
	if m.CPUProfile != "" {
		f, err := os.Create(m.CPUProfile)
		if err != nil {
			return err
		}

		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	// Open HTTP listener to determine port (if specified as :0).
	ln, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return err
	}
	m.ln = ln

	// Determine hostname based on listening port.
	m.Host = net.JoinHostPort(host, strconv.Itoa(m.ln.Addr().(*net.TCPAddr).Port))

	// Build cluster from config file. Create local host if none are specified.
	m.Cluster = m.Config.PilosaCluster()
	if len(m.Cluster.Nodes) == 0 {
		m.Cluster.Nodes = []*pilosa.Node{{
			Host: m.Host,
		}}
	}

	// Create index to store fragments.
	fmt.Fprintf(m.Stderr, "Using data from: %s\n", m.Config.DataDir)
	m.index = pilosa.NewIndex(m.Config.DataDir)
	if err := m.index.Open(); err != nil {
		return err
	}

	// Create executor for executing queries.
	e := pilosa.NewExecutor(m.index)
	e.Host = m.Host
	e.Cluster = m.Cluster

	// Initialize HTTP handler.
	h := pilosa.NewHandler()
	h.Index = m.index
	h.Host = m.Host
	h.Cluster = m.Cluster
	h.Executor = e
	h.LogOutput = m.Stderr

	// Serve HTTP.
	go func() { http.Serve(ln, h) }()

	// Start anti-entropy background workers.
	m.startAntiEntropyMonitors()

	// Sync up max slice if more than one node
	if len(m.Cluster.Nodes) > 1 {
		m.ticker = time.NewTicker(time.Second * time.Duration(m.pollingSecs))
		go func() {
			for range m.ticker.C {
				oldmax := m.index.SliceN()
				newmax := oldmax
				for _, node := range m.Cluster.Nodes {
					if m.Host != node.Host {
						newslice, _ := checkMaxSlice(node.Host)
						if newslice > newmax {
							newmax = newslice
						}
					}
				}
				if newmax > oldmax {
					m.index.SetMax(newmax)
				}
			}
		}()
	}

	fmt.Fprintf(m.Stderr, "Listening as http://%s\n", m.Host)

	return nil
}

func (m *Main) startAntiEntropyMonitors() {
	for _, node := range m.Cluster.Nodes {
		// Skip this node.
		if node.Host == m.Host {
			continue
		}

		m.wg.Add(1)
		go func(node *pilosa.Node) {
			defer m.wg.Done()
			m.monitorAntiEntropy(node)
		}(node)
	}
}

func (m *Main) monitorAntiEntropy(node *pilosa.Node) {
	ticker := time.NewTicker(time.Duration(m.Config.AntiEntropy.Interval))
	defer ticker.Stop()

	m.logger().Printf("index sync monitor initializing: host=%s", node.Host)

	for {
		// Wait for tick or a close.
		select {
		case <-m.closing:
			return
		case <-ticker.C:
		}

		m.logger().Printf("index sync beginning: host=%s", node.Host)

		// Set up remote client.
		client, err := pilosa.NewClient(node.Host)
		if err != nil {
			m.logger().Printf("anti-entropy client error: host=%s", node.Host)
			continue
		}

		// Initialize syncer with local index and remote client.
		var syncer pilosa.IndexSyncer
		syncer.Index = m.index
		syncer.Client = client

		// Sync indexes.
		if err := syncer.SyncIndex(); err != nil {
			m.logger().Printf("index sync error: host=%s, err=%s", node.Host, err)
			continue
		}

		// Record successful sync in log.
		m.logger().Printf("index sync complete: host=%s", node.Host)
	}
}

func checkMaxSlice(hostport string) (uint64, error) {
	// Create HTTP request.
	req, err := http.NewRequest("GET", (&url.URL{
		Scheme: "http",
		Host:   hostport,
		Path:   "/slices/max",
	}).String(), nil)

	if err != nil {
		return 0, err
	}

	// Require protobuf encoding.
	req.Header.Set("Accept", "application/x-protobuf")
	req.Header.Set("Content-Type", "application/x-protobuf")

	// Send request to remote node.
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	// Read response into buffer.
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}

	// Check status code.
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("invalid status: code=%d, err=%s", resp.StatusCode, body)
	}

	// Decode response object.
	pb := internal.SliceMaxResponse{}

	if err = proto.Unmarshal(body, &pb); err != nil {
		return 0, err
	}

	return *pb.SliceMax, nil

}

// Close shuts down the process.
func (m *Main) Close() error {
	// Notify goroutines to stop.
	close(m.closing)
	m.wg.Wait()

	if m.ticker != nil {
		m.ticker.Stop()
	}
	if m.ln != nil {
		m.ln.Close()
	}

	if m.index != nil {
		m.index.Close()
	}

	return nil
}

// ParseFlags parses command line flags from args.
func (m *Main) ParseFlags(args []string) error {
	fs := flag.NewFlagSet("pilosa", flag.ContinueOnError)
	fs.SetOutput(m.Stderr)
	fs.StringVar(&m.ConfigPath, "config", "", "config path")
	fs.StringVar(&m.CPUProfile, "cpuprofile", "", "write cpu profile to file")
	fs.IntVar(&m.pollingSecs, "pollingSecs", 60, "number of seconds to poll the cluster for maxslice")
	if err := fs.Parse(args); err != nil {
		return err
	}

	// Load config, if specified.
	if m.ConfigPath != "" {
		if _, err := toml.DecodeFile(m.ConfigPath, &m.Config); err != nil {
			return err
		}
	}

	// Use default data directory if one is not specified.
	if m.Config.DataDir == "" {
		m.Config.DataDir = DefaultDataDir
	}

	// Expand home directory.
	prefix := "~" + string(filepath.Separator)
	if strings.HasPrefix(m.Config.DataDir, prefix) {
		//	u, err := user.Current()
		HomeDir := os.Getenv("HOME")
		/*if err != nil {
			return err
		} else*/if HomeDir == "" {
			return errors.New("data directory not specified and no home dir available")
		}
		m.Config.DataDir = filepath.Join(HomeDir, strings.TrimPrefix(m.Config.DataDir, prefix))
	}

	return nil
}

func (m *Main) logger() *log.Logger { return log.New(m.Stderr, "", log.LstdFlags) }

// Config represents the configuration for the command.
type Config struct {
	DataDir string `toml:"data-dir"`
	Host    string `toml:"host"`

	Cluster struct {
		ReplicaN int           `toml:"replicas"`
		Nodes    []*ConfigNode `toml:"node"`
	} `toml:"cluster"`

	Plugins struct {
		Path string `toml:"path"`
	} `toml:"plugins"`

	AntiEntropy struct {
		Interval Duration `toml:"interval"`
	} `toml:"anti-entropy"`
}

type ConfigNode struct {
	Host string `toml:"host"`
}

// NewConfig returns an instance of Config with default options.
func NewConfig() *Config {
	c := &Config{
		Host: DefaultHost,
	}
	c.Cluster.ReplicaN = pilosa.DefaultReplicaN
	c.AntiEntropy.Interval = Duration(DefaultAntiEntropyInterval)
	return c
}

// PilosaCluster returns a new instance of pilosa.Cluster based on the config.
func (c *Config) PilosaCluster() *pilosa.Cluster {
	cluster := pilosa.NewCluster()
	cluster.ReplicaN = c.Cluster.ReplicaN

	for _, n := range c.Cluster.Nodes {
		cluster.Nodes = append(cluster.Nodes, &pilosa.Node{Host: n.Host})
	}

	return cluster
}

// Duration is a TOML wrapper type for time.Duration.
type Duration time.Duration

// String returns the string representation of the duration.
func (d Duration) String() string { return time.Duration(d).String() }

// UnmarshalText parses a TOML value into a duration value.
func (d *Duration) UnmarshalText(text []byte) error {
	v, err := time.ParseDuration(string(text))
	if err != nil {
		return err
	}

	*d = Duration(v)
	return nil
}
