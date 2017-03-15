package server

import (
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pilosa/pilosa"
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

	// running will be closed once Command.Run is finished.
	Started chan struct{}
	// Done will be closed when Command.Close() is called
	Done chan struct{}
}

// NewMain returns a new instance of Main.
func NewCommand() *Command {
	return &Command{
		Server: pilosa.NewServer(),
		Config: pilosa.NewConfig(),

		Stdin:  os.Stdin,
		Stdout: os.Stdout,
		Stderr: os.Stderr,

		Started: make(chan struct{}),
		Done:    make(chan struct{}),
	}
}

// Run executes the pilosa server.
func (m *Command) Run(args ...string) error {
	defer close(m.Started)
	prefix := "~" + string(filepath.Separator)
	if strings.HasPrefix(m.Config.DataDir, prefix) {
		HomeDir := os.Getenv("HOME")
		if HomeDir == "" {
			return errors.New("data directory not specified and no home dir available")
		}
		m.Config.DataDir = filepath.Join(HomeDir, strings.TrimPrefix(m.Config.DataDir, prefix))
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
func (m *Command) Close() error {
	err := m.Server.Close()
	close(m.Done)
	return err
}
