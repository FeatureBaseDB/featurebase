// creator contains code for standing up pilosa clusters
package creator

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"

	"github.com/pilosa/pilosa"
)

type Cluster interface {
	Hosts() []string
	Shutdown() error
	Logs() []io.Reader
}

type cluster struct {
	hosts   []string
	logs    []io.Reader
	servers []*pilosa.Server
	cluster *pilosa.Cluster
	path    string
}

func NewLocalCluster(replicaN, serverN int) (Cluster, error) {
	BasePort := 19327

	localCluster := &cluster{
		hosts:   make([]string, serverN),
		servers: make([]*pilosa.Server, serverN),
		logs:    make([]io.Reader, serverN),
	}
	path, err := ioutil.TempDir("", "pilosa-bench-")
	if err != nil {
		return localCluster, err
	}
	localCluster.path = path

	// Build cluster configuration.
	cluster := pilosa.NewCluster()
	cluster.ReplicaN = replicaN

	for i := 0; i < serverN; i++ {
		cluster.Nodes = append(cluster.Nodes, &pilosa.Node{
			Host: fmt.Sprintf("localhost:%d", BasePort+i),
		})
	}
	localCluster.cluster = cluster

	// Build servers.
	for i := range localCluster.servers {
		// Make server work directory.
		if err := os.MkdirAll(filepath.Join(path, strconv.Itoa(i)), 0777); err != nil {
			return localCluster, err
		}

		// Build server.
		s := pilosa.NewServer()
		s.Host = fmt.Sprintf("localhost:%d", BasePort+i)
		s.Cluster = cluster
		s.Index.Path = filepath.Join(path, strconv.Itoa(i), "data")

		// Create log stream
		localCluster.logs[i], s.LogOutput = io.Pipe()

		localCluster.servers[i] = s
	}

	// Open all servers.
	for i, s := range localCluster.servers {
		if err := s.Open(); err != nil {
			return localCluster, err
		}
		localCluster.hosts[i] = s.Host
	}

	return localCluster, nil
}

func (c *cluster) Hosts() []string   { return c.hosts }
func (c *cluster) Logs() []io.Reader { return c.logs }
func (c *cluster) Shutdown() error {
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
