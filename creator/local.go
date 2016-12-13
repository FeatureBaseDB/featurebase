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

type LocalCluster struct {
	ReplicaN int
	ServerN  int
	hosts    []string
	logs     []io.Reader
	servers  []*pilosa.Server
	cluster  *pilosa.Cluster
	path     string
}

func (localCluster *LocalCluster) Start() error {
	BasePort := 19327

	localCluster.hosts = make([]string, localCluster.ServerN)
	localCluster.servers = make([]*pilosa.Server, localCluster.ServerN)
	localCluster.logs = make([]io.Reader, localCluster.ServerN)

	path, err := ioutil.TempDir("", "pilosa-bench-")
	if err != nil {
		return err
	}
	localCluster.path = path

	// Build cluster configuration.
	cluster := pilosa.NewCluster()
	cluster.ReplicaN = localCluster.ReplicaN

	for i := 0; i < localCluster.ServerN; i++ {
		cluster.Nodes = append(cluster.Nodes, &pilosa.Node{
			Host: fmt.Sprintf("localhost:%d", BasePort+i),
		})
	}
	localCluster.cluster = cluster

	// Build servers.
	for i := range localCluster.servers {
		// Make server work directory.
		if err := os.MkdirAll(filepath.Join(path, strconv.Itoa(i)), 0777); err != nil {
			return err
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
			return err
		}
		localCluster.hosts[i] = s.Host
	}

	return nil
}

func (c *LocalCluster) Hosts() []string   { return c.hosts }
func (c *LocalCluster) Logs() []io.Reader { return c.logs }
func (c *LocalCluster) Shutdown() error {
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
