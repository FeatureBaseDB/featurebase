package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/signal"
	"os/user"
	"path/filepath"
	"runtime/pprof"
	"strconv"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/umbel/pilosa"
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
	index *pilosa.Index
	ln    net.Listener

	// Path to the configuration file.
	ConfigPath string

	// Configuration options.
	Config *Config

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
	hostname := net.JoinHostPort(host, strconv.Itoa(m.ln.Addr().(*net.TCPAddr).Port))

	// Build cluster from config file. Create local host if none are specified.
	cluster := m.Config.PilosaCluster()
	if len(cluster.Nodes) == 0 {
		cluster.Nodes = []*pilosa.Node{{
			Host: hostname,
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
	e.Host = hostname
	e.Cluster = cluster

	// Initialize HTTP handler.
	h := pilosa.NewHandler()
	h.Index = m.index
	h.Host = hostname
	h.Cluster = cluster
	h.Executor = e
	h.LogOutput = m.Stderr

	// Serve HTTP.
	go func() { http.Serve(ln, h) }()

	fmt.Fprintf(m.Stderr, "Listening as http://%s\n", hostname)

	return nil
}

// Close shuts down the process.
func (m *Main) Close() error {
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
		u, err := user.Current()
		if err != nil {
			return err
		} else if u.HomeDir == "" {
			return errors.New("data directory not specified and no home dir available")
		}
		m.Config.DataDir = filepath.Join(u.HomeDir, strings.TrimPrefix(m.Config.DataDir, prefix))
	}

	return nil
}

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
