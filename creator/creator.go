// creator contains code for standing up pilosa clusters
package creator

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"

	"github.com/pilosa/pilosa"
)

type Cluster interface {
	Hosts() []string
	Shutdown() error
}

type cluster struct {
	hosts   []string
	servers []*pilosa.Server
	cluster *pilosa.Cluster
	path    string
}

func NewLocalCluster(replicaN, serverN int) (Cluster, error) {
	BasePort := 19327

	localCluster := &cluster{
		hosts:   make([]string, 0),
		servers: make([]*pilosa.Server, 0),
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
	servers := make([]*pilosa.Server, serverN)
	for i := range servers {
		// Make server work directory.
		if err := os.MkdirAll(filepath.Join(path, strconv.Itoa(i)), 0777); err != nil {
			return localCluster, err
		}

		// Build server.
		s := pilosa.NewServer()
		s.Host = fmt.Sprintf("localhost:%d", BasePort+i)
		s.Cluster = cluster
		s.Index.Path = filepath.Join(path, strconv.Itoa(i), "data")

		// Create log file.
		f, err := os.Create(filepath.Join(path, strconv.Itoa(i), "log"))
		if err != nil {
			return localCluster, err
		}

		// Set log and optionally write out to stderr as well.
		s.LogOutput = f

		servers[i] = s
	}
	localCluster.servers = servers

	// Open all servers.
	for _, s := range servers {
		if err := s.Open(); err != nil {
			return localCluster, err
		}
	}

	hosts := make([]string, 0)
	for _, s := range servers {
		hosts = append(hosts, s.Host)
	}
	localCluster.hosts = hosts
	return localCluster, nil
}

func (c *cluster) Hosts() []string { return c.hosts }
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
