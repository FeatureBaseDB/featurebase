// +build integration

package pilosa_test

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/umbel/pilosa"
)

// FIXME(jaffee): move all this cluster creation and interface code to a better place
type TestCluster interface {
	Hosts() []string
	Close() error
}

type cluster struct {
	hosts   []string
	servers []*pilosa.Server
	cluster *pilosa.Cluster
	path    string
}

func newTestCluster() *cluster {
	return &cluster{
		hosts:   make([]string, 0),
		servers: make([]*pilosa.Server, 0),
	}
}

func (c *cluster) Hosts() []string { return c.hosts }
func (c *cluster) Close() error {
	errs := ""
	for _, s := range c.servers {
		if err := s.Close(); err != nil {
			errs = errs + err.Error() + "; "
		}
	}
	if err := os.RemoveAll(c.path); err != nil {
		errs = errs + err.Error() + ";"
	}
	if errs != "" {
		return fmt.Errorf(errs)
	}
	return nil
}

func setupCluster() (TestCluster, error) {
	// FIXME(jaffee): add controls for configurable cluster setup via env vars or
	// build tags. For now I just stole benbjohsnon's code from pilosa-bench
	replicaN := 1
	serverN := 3
	BasePort := 19327

	testCluster := newTestCluster()

	path, err := ioutil.TempDir("", "pilosa-bench-")
	if err != nil {
		return testCluster, err
	}
	testCluster.path = path

	// Build cluster configuration.
	cluster := pilosa.NewCluster()
	cluster.ReplicaN = replicaN

	for i := 0; i < serverN; i++ {
		cluster.Nodes = append(cluster.Nodes, &pilosa.Node{
			Host: fmt.Sprintf("localhost:%d", BasePort+i),
		})
	}
	testCluster.cluster = cluster

	// Build servers.
	servers := make([]*pilosa.Server, serverN)
	for i := range servers {
		// Make server work directory.
		if err := os.MkdirAll(filepath.Join(path, strconv.Itoa(i)), 0777); err != nil {
			return testCluster, err
		}

		// Build server.
		s := pilosa.NewServer()
		s.Host = fmt.Sprintf("localhost:%d", BasePort+i)
		s.Cluster = cluster
		s.Index.Path = filepath.Join(path, strconv.Itoa(i), "data")

		// Create log file.
		f, err := os.Create(filepath.Join(path, strconv.Itoa(i), "log"))
		if err != nil {
			return testCluster, err
		}

		// Set log and optionally write out to stderr as well.
		s.LogOutput = f

		servers[i] = s
	}
	testCluster.servers = servers

	// Open all servers.
	for _, s := range servers {
		log.Printf("opening : %v", s)
		if err := s.Open(); err != nil {
			return testCluster, err
		}
	}

	hosts := make([]string, 0)
	for _, s := range servers {
		hosts = append(hosts, s.Host)
	}
	testCluster.hosts = hosts
	return testCluster, nil
}

func BenchmarkSetBitOps(b *testing.B) {
	tc, err := setupCluster()
	defer tc.Close()
	if err != nil {
		b.Fatal(err)
	}
	cli, err := pilosa.NewClient(tc.Hosts()[0])
	if err != nil {
		b.Fatal(err)
	}
	for n := 0; n < b.N; n++ {
		query := fmt.Sprintf("SetBit(%d, 'frame.n', %d)", n, n%10)
		cli.ExecuteQuery("2", query, true)
	}
}
