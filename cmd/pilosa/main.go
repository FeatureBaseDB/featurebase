package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"runtime/pprof"
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

	// Wait indefinitely.
	<-(chan struct{})(nil)
}

// Main represents the main program execution.
type Main struct {
	ln net.Listener

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

// Run executes the main program execution.
func (m *Main) Run(args ...string) error {
	logger := log.New(m.Stderr, "", log.LstdFlags)

	// Notify user of config file.
	if m.ConfigPath != "" {
		fmt.Fprintf(m.Stdout, "Using config: %s\n", m.ConfigPath)
	}

	// Require a port in the hostname.
	_, addr, err := net.SplitHostPort(m.Config.Host)
	if err != nil {
		return err
	} else if addr == "" {
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

	// Build cluster from config file.
	cluster := m.Config.PilosaCluster()

	// Create index to store fragments.
	index := pilosa.NewIndex()

	// Create executor for executing queries.
	e := pilosa.NewExecutor(index)
	e.Host = m.Config.Host
	e.Cluster = cluster

	// Initialize HTTP handler.
	h := pilosa.NewHandler()
	h.Executor = e
	h.LogOutput = m.Stderr

	// Open HTTP listener.
	ln, err := net.Listen("tcp", ":"+addr)
	if err != nil {
		return err
	}
	m.ln = ln

	// Serve HTTP.
	go func() { logger.Print(http.Serve(ln, h)) }()

	fmt.Fprintf(m.Stderr, "Listening on http://%s\n", ln.Addr().String())

	return nil
}

// Close shuts down the process.
func (m *Main) Close() error {
	if m.ln != nil {
		m.ln.Close()
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

	return nil
}
