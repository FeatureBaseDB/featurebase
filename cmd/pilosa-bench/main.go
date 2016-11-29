package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"

	"github.com/pilosa/pilosa"
)

// BasePort is the initial port used when generating a cluster.
const BasePort = 16000

// Default settings for command line arguments.
const (
	DefaultServerN  = 1
	DefaultReplicaN = 1
)

func main() {
	m := NewMain()

	// Parse command line arguments.
	if err := m.ParseFlags(os.Args[1:]); err == flag.ErrHelp {
		fmt.Fprintln(m.Stderr, m.Usage())
		os.Exit(1)
	} else if err != nil {
		fmt.Fprintln(m.Stderr, err)
		os.Exit(1)
	}

	// Execute the program.
	if err := m.Run(); err != nil {
		fmt.Fprintln(m.Stderr, err)
		os.Exit(1)
	}
}

// Main represents the main program execution.
type Main struct {
	ServerN  int
	ReplicaN int

	Verbose bool
	Work    bool

	// Standard input/output
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}

// NewMain returns a new instance of Main.
func NewMain() *Main {
	return &Main{
		ServerN:  DefaultServerN,
		ReplicaN: DefaultReplicaN,

		Stdin:  os.Stdin,
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	}
}

// Usage returns usage documentation.
func (m *Main) Usage() string {
	return `
pilosa-bench is a tool for testing and benchmarking pilosa clusters.

Usage:

	pilosa-bench [arguments]

The following arguments are available:

	-servers N
	    The number of servers to generate for the cluster.
	    Defaults to 1 server.

	-replicas N
	    Replication factor for data in the cluster.
	    Defaults to 1 replica.

	-v
	    Logs all server output to stderr in addition to server log.

	-work
	    Prints the temporary work directory and does not delete it
	    after the benchmark has completed.

`[1:]
}

// ParseFlags parses command line flags from args.
func (m *Main) ParseFlags(args []string) error {
	fs := flag.NewFlagSet("pilosa-bench", flag.ContinueOnError)
	fs.SetOutput(ioutil.Discard)
	fs.IntVar(&m.ServerN, "servers", DefaultServerN, "server count")
	fs.IntVar(&m.ReplicaN, "replicas", DefaultReplicaN, "replication factor")
	fs.BoolVar(&m.Verbose, "v", false, "verbose")
	fs.BoolVar(&m.Work, "work", false, "verbose")
	if err := fs.Parse(args); err != nil {
		return err
	}
	return nil
}

// Run executes the main program execution.
func (m *Main) Run(args ...string) error {
	// Validate arguments.
	if m.ServerN <= 0 {
		return errors.New("server count must be at least 1")
	} else if m.ReplicaN <= 0 {
		return errors.New("replication factor must be at least 1")
	} else if m.ReplicaN > m.ServerN {
		return errors.New("replication factor must be less than server count")
	}

	logger := m.logger()
	var logFiles []*os.File

	// Create a base temp path.
	path, err := ioutil.TempDir("", "pilosa-bench-")
	if err != nil {
		return err
	}
	if m.Work {
		fmt.Printf("WORK=%s\n\n", path)
	}

	// Build cluster configuration.
	cluster := pilosa.Cluster{
		ReplicaN: m.ReplicaN,
	}
	for i := 0; i < m.ServerN; i++ {
		cluster.Nodes = append(cluster.Nodes, &pilosa.Node{
			Host: fmt.Sprintf("localhost:%d", BasePort+i),
		})
	}

	// Build servers.
	servers := make([]*pilosa.Server, m.ServerN)
	for i := range servers {
		// Make server work directory.
		if err := os.MkdirAll(filepath.Join(path, strconv.Itoa(i)), 0777); err != nil {
			return err
		}

		// Build server.
		s := pilosa.NewServer()
		s.Host = fmt.Sprintf("localhost:%d", BasePort+i)
		s.Cluster = &cluster
		s.Index.Path = filepath.Join(path, strconv.Itoa(i), "data")

		// Create log file.
		f, err := os.Create(filepath.Join(path, strconv.Itoa(i), "log"))
		if err != nil {
			return err
		}
		logFiles = append(logFiles, f)

		// Set log and optionally write out to stderr as well.
		s.LogOutput = f
		if m.Verbose {
			s.LogOutput = io.MultiWriter(m.Stderr, s.LogOutput)
		}

		servers[i] = s
	}

	// Open all servers.
	for i, s := range servers {
		logger.Printf("starting server #%d: %s", i, s.Host)
		if err := s.Open(); err != nil {
			return err
		}
	}

	// FIXME(benbjohnson): Execute benchmark testing.

	// Close all servers.
	for i, s := range servers {
		logger.Printf("closing server #%d", i)
		if err := s.Close(); err != nil {
			logger.Printf("error closing server: %s", err)
		}
	}

	// Close all logs.
	for _, f := range logFiles {
		f.Close()
	}

	// If work flag is not set then delete all data & logs.
	if !m.Work {
		if err := os.RemoveAll(path); err != nil {
			return err
		}
	}

	return nil
}

// Close gracefully closes the program.
func (m *Main) Close() error { return nil }

func (m *Main) logger() *log.Logger { return log.New(m.Stderr, "", log.LstdFlags) }
