package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/pprof"
	"strconv"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/pilosa/pilosa"
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
)

func main() {
	m := NewMain()
	fmt.Fprintf(m.Stderr, "Pilosa %s\n", Build)

	// Load configuration from environment variables
	m.loadEnvConfig()

	// Parse command line arguments.
	if err := m.ParseFlags(os.Args[1:]); err != nil {
		fmt.Fprintln(m.Stderr, err)
		os.Exit(2)
	}

	// Start CPU profiling.
	if m.CPUProfile != "" {
		f, err := os.Create(m.CPUProfile)
		if err != nil {
			fmt.Fprintf(m.Stderr, "create cpu profile: %v", err)
			os.Exit(1)
		}
		defer f.Close()

		fmt.Fprintln(m.Stderr, "Starting cpu profile")
		pprof.StartCPUProfile(f)
		time.AfterFunc(m.CPUTime, func() {
			fmt.Fprintln(m.Stderr, "Stopping cpu profile")
			pprof.StopCPUProfile()
			f.Close()
		})
	}

	// Execute the program.
	if err := m.Run(); err != nil {
		fmt.Fprintln(m.Stderr, err)
		fmt.Fprintln(m.Stderr, "stopping profile")
		os.Exit(1)
	}

	// First SIGKILL causes server to shut down gracefully.
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt)
	sig := <-c
	fmt.Fprintf(m.Stderr, "Received %s; gracefully shutting down...\n", sig.String())

	// Second signal causes a hard shutdown.
	go func() { <-c; os.Exit(1) }()

	if err := m.Close(); err != nil {
		fmt.Fprintln(m.Stderr, err)
		os.Exit(1)
	}
}

// Main represents the main program execution.
type Main struct {
	Server *pilosa.Server

	// Configuration options.
	ConfigPath string
	Config     *pilosa.Config

	// Profiling options.
	CPUProfile string
	CPUTime    time.Duration

	// Standard input/output
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}

// NewMain returns a new instance of Main.
func NewMain() *Main {
	return &Main{
		Server: pilosa.NewServer(),
		Config: pilosa.NewConfig(),

		Stdin:  os.Stdin,
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	}
}

// Run executes the main program execution.
func (m *Main) Run(args ...string) error {
	// Notify user of config file.
	if m.ConfigPath != "" {
		fmt.Fprintf(m.Stdout, "Using config: %s\n", m.ConfigPath)
	}

	// Setup logging output.
	m.Server.LogOutput = m.Stderr

	// Configure index.
	fmt.Fprintf(m.Stderr, "Using data from: %s\n", m.Config.DataDir)
	m.Server.Index.Path = m.Config.DataDir
	m.Server.Index.Stats = pilosa.NewExpvarStatsClient()

	// Build cluster from config file.
	m.Server.Host = m.Config.Host
	m.Server.Cluster = m.Config.PilosaCluster()

	// Set configuration options.
	m.Server.AntiEntropyInterval = time.Duration(m.Config.AntiEntropy.Interval)

	// Initialize server.
	if err := m.Server.Open(); err != nil {
		return err
	}

	fmt.Fprintf(m.Stderr, "Listening as http://%s\n", m.Server.Host)

	return nil
}

// Close shuts down the server.
func (m *Main) Close() error {
	return m.Server.Close()
}

// ParseFlags parses command line flags from args.
func (m *Main) ParseFlags(args []string) error {
	fs := flag.NewFlagSet("pilosa", flag.ContinueOnError)
	fs.StringVar(&m.CPUProfile, "cpuprofile", "", "cpu profile")
	fs.DurationVar(&m.CPUTime, "cputime", 30*time.Second, "cpu profile duration")
	fs.StringVar(&m.ConfigPath, "config", "", "config path")
	fs.SetOutput(m.Stderr)
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

// loadEnvConfig loads configuration from environment variables
func (m *Main) loadEnvConfig() {
	envStringVar(&m.Config.DataDir, "DATA_DIR")
	envStringVar(&m.Config.Host, "HOST")
	envIntVar(&m.Config.Cluster.ReplicaN, "CLUSTER_REPLICAS")

	var clusterNodes string
	envStringVar(&clusterNodes, "CLUSTER_NODES")
	if clusterNodes != "" {
		for _, nodeAddr := range strings.Fields(clusterNodes) {
			node := &pilosa.ConfigNode{
				Host: nodeAddr,
			}
			m.Config.Cluster.Nodes = append(m.Config.Cluster.Nodes, node)
		}
	}
}

func envStringVar(v *string, key string) {
	value := os.Getenv(key)
	if value != "" {
		*v = value
	}
}

func envIntVar(v *int, key string) {
	value := os.Getenv(key)
	if value != "" {
		vi, err := strconv.Atoi(value)
		if err != nil {
			panic(err)
		}
		*v = vi
	}
}
