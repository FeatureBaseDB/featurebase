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

// NewMain returns a new instance of Main.
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

	// Setup logging output.
	m.Server.LogOutput = m.Stderr

	// Configure index.
	fmt.Fprintf(m.Stderr, "Using data from: %s\n", m.Config.DataDir)
	m.Server.Index.Path = m.Config.DataDir
	m.Server.Index.Stats = pilosa.NewExpvarStatsClient()

	// Build cluster from config file.
	m.Server.Host, err = normalizeHost(m.Config.Host)
	if err != nil {
		return err
	}
	m.Server.Cluster = m.Config.PilosaCluster()

	// Setup Messenger.
	fmt.Fprintf(m.Stderr, "Using Messenger type: %s\n", m.Config.Cluster.MessengerType)
	m.Server.Messenger = m.Server.Cluster.NodeSet.(pilosa.Messenger)
	m.Server.Handler.Messenger = m.Server.Messenger
	m.Server.Index.Messenger = m.Server.Messenger

	// Set message handler.
	m.Server.Cluster.NodeSet.SetMessageHandler(m.Server.Index.HandleMessage)

	// Set configuration options.
	m.Server.AntiEntropyInterval = time.Duration(m.Config.AntiEntropy.Interval)

	// Initialize server.
	if err = m.Server.Open(); err != nil {
		return fmt.Errorf("server.Open: %v", err)
	}
	fmt.Fprintf(m.Stderr, "Listening as http://%s\n", m.Server.Host)
	return nil
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
	err := m.Server.Close()
	close(m.Done)
	return err
}
