package pilosa

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/umbel/pilosa/internal"
)

// Default server settings.
const (
	DefaultAntiEntropyInterval = 10 * time.Minute
	DefaultPollingInterval     = 60 * time.Second
)

// Server represents an index wrapped by a running HTTP server.
type Server struct {
	ln net.Listener

	// Close management.
	wg      sync.WaitGroup
	closing chan struct{}

	// Data storage and HTTP interface.
	Index   *Index
	Handler *Handler

	// Cluster configuration.
	// Host is replaced with actual host after opening if port is ":0".
	Host    string
	Cluster *Cluster

	// Background monitoring intervals.
	AntiEntropyInterval time.Duration
	PollingInterval     time.Duration

	LogOutput io.Writer
}

// NewServer returns a new instance of Server.
func NewServer() *Server {
	s := &Server{
		closing: make(chan struct{}),

		Index:   NewIndex(),
		Handler: NewHandler(),

		AntiEntropyInterval: DefaultAntiEntropyInterval,
		PollingInterval:     DefaultPollingInterval,

		LogOutput: os.Stderr,
	}

	s.Handler.Index = s.Index

	return s
}

// Open opens and initializes the server.
func (s *Server) Open() error {
	// Require a port in the hostname.
	host, port, err := net.SplitHostPort(s.Host)
	if err != nil {
		return err
	} else if port == "" {
		return errors.New("port must be specified in config host")
	}

	// Open HTTP listener to determine port (if specified as :0).
	ln, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return err
	}
	s.ln = ln

	// Determine hostname based on listening port.
	s.Host = net.JoinHostPort(host, strconv.Itoa(s.ln.Addr().(*net.TCPAddr).Port))

	// Create local node if no cluster is specified.
	if len(s.Cluster.Nodes) == 0 {
		s.Cluster.Nodes = []*Node{{Host: s.Host}}
	}

	// Open index.
	if err := s.Index.Open(); err != nil {
		return err
	}

	// Create executor for executing queries.
	e := NewExecutor()
	e.Index = s.Index
	e.Host = s.Host
	e.Cluster = s.Cluster

	// Initialize HTTP handler.
	s.Handler.Host = s.Host
	s.Handler.Cluster = s.Cluster
	s.Handler.Executor = e
	s.Handler.LogOutput = s.LogOutput

	// Serve HTTP.
	go func() { http.Serve(ln, s.Handler) }()

	// Start background monitoring.
	s.wg.Add(2)
	go func() { defer s.wg.Done(); s.monitorAntiEntropy() }()
	go func() { defer s.wg.Done(); s.monitorMaxSlices() }()

	return nil
}

// Close closes the server and waits for it to shutdown.
func (s *Server) Close() error {
	// Notify goroutines to stop.
	close(s.closing)
	s.wg.Wait()

	if s.ln != nil {
		s.ln.Close()
	}
	if s.Index != nil {
		s.Index.Close()
	}

	return nil
}

// Addr returns the address of the listener.
func (s *Server) Addr() net.Addr {
	if s.ln == nil {
		return nil
	}
	return s.ln.Addr()
}

func (s *Server) logger() *log.Logger { return log.New(s.LogOutput, "", log.LstdFlags) }

func (s *Server) monitorAntiEntropy() {
	ticker := time.NewTicker(s.AntiEntropyInterval)
	defer ticker.Stop()

	s.logger().Printf("index sync monitor initializing (%s interval)", s.AntiEntropyInterval)

	for {
		// Wait for tick or a close.
		select {
		case <-s.closing:
			return
		case <-ticker.C:
		}

		s.logger().Printf("index sync beginning")

		// Initialize syncer with local index and remote client.
		var syncer IndexSyncer
		syncer.Index = s.Index
		syncer.Host = s.Host
		syncer.Cluster = s.Cluster
		syncer.Closing = s.closing

		// Sync indexes.
		if err := syncer.SyncIndex(); err != nil {
			s.logger().Printf("index sync error: err=%s", err)
			continue
		}

		// Record successful sync in log.
		s.logger().Printf("index sync complete")
	}
}

// monitorMaxSlices periodically pulls the highest slice from each node in the cluster.
func (s *Server) monitorMaxSlices() {
	// Ignore if only one node in the cluster.
	if len(s.Cluster.Nodes) <= 1 {
		return
	}

	ticker := time.NewTicker(s.PollingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.closing:
			return
		case <-ticker.C:
		}

		oldmaxslices := s.Index.SliceNs()
		for _, node := range s.Cluster.Nodes {
			if s.Host != node.Host {
				maxSlices, _ := checkMaxSlices(node.Host)
				for db, newmax := range maxSlices {
					// we're not going to create a db locally if we don't know about it already
					// TODO: consider changing this so we DO create a db locally
					// do we want/need nodes to have empty files structures?
					if localdb := s.Index.DB(db); localdb != nil {
						if newmax > oldmaxslices[db] {
							oldmaxslices[db] = newmax
							localdb.SetRemoteMaxSlice(newmax)
						}
					}
				}
			}
		}
	}
}

func checkMaxSlices(hostport string) (MaxSlices, error) {
	// Create HTTP request.
	req, err := http.NewRequest("GET", (&url.URL{
		Scheme: "http",
		Host:   hostport,
		Path:   "/slices/max",
	}).String(), nil)

	if err != nil {
		return nil, err
	}

	// Require protobuf encoding.
	req.Header.Set("Accept", "application/x-protobuf")
	req.Header.Set("Content-Type", "application/x-protobuf")

	// Send request to remote node.
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// Read response into buffer.
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	// Check status code.
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("invalid status: code=%d, err=%s", resp.StatusCode, body)
	}

	// Decode response object.
	pb := internal.MaxSlicesResponse{}

	if err = proto.Unmarshal(body, &pb); err != nil {
		return nil, err
	}

	return pb.MaxSlices, nil

}
