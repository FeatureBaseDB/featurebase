package pilosa_test

import (
	"testing"

	"github.com/pilosa/pilosa"
)

func Test_NewConfig(t *testing.T) {
	c := pilosa.NewConfig()

	c.Cluster.Hosts = []string{c.Bind, "localhost:10102"}
	if err := c.Validate(); err != pilosa.ErrConfigClusterTypeMissing {
		t.Fatal(err)
	}

	c.Cluster.Type = "test"
	if err := c.Validate(); err != pilosa.ErrConfigClusterTypeInvalid {
		t.Fatal(err)
	}

	c.Cluster.Type = pilosa.ClusterHTTP
	if err := c.Validate(); err != pilosa.ErrConfigHostsMismatch {
		t.Fatal(err)
	}

	c.Cluster.InternalPort = pilosa.DefaultInternalPort
	c.Cluster.InternalHosts = []string{"localhost:14004", "localhost:14001"}
	if err := c.Validate(); err != pilosa.ErrConfigBroadcastPort {
		t.Fatal(err)
	}

	c.Cluster.InternalHosts = []string{"localhost:14000", "localhost:14001"}
	c.Bind = "localhost:1"
	// Check for bind addres in cluster hosts
	if err := c.Validate(); err != pilosa.ErrConfigHostsMissing {
		t.Fatal(err)
	}

	c.Bind = "localhost:10101"
	c.Cluster.ReplicaN = 3
	if err := c.Validate(); err != pilosa.ErrConfigReplicaNInvalid {
		t.Fatal(err)
	}

	c.Cluster.ReplicaN = 2
	c.Cluster.Type = pilosa.ClusterGossip
	c.Cluster.GossipSeed = "localhost:10101"
	if err := c.Validate(); err != pilosa.ErrConfigGossipSeed {
		t.Fatal(err)
	}
}
