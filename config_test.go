package pilosa_test

import (
	"reflect"
	"testing"
	"time"

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

	c.Cluster.GossipSeed = "localhost:14000"
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestDuration(t *testing.T) {
	d := pilosa.Duration(time.Second * 182)
	if d.String() != "3m2s" {
		t.Fatalf("Unexpected time Duration %s", d)
	}

	b := []byte{51, 109, 50, 115}
	v, _ := d.MarshalText()
	if !reflect.DeepEqual(b, v) {
		t.Fatalf("Unexpected marshalled value %v", v)
	}

	v, _ = d.MarshalTOML()
	if !reflect.DeepEqual(b, v) {
		t.Fatalf("Unexpected marshalled value %v", v)
	}

	err := d.UnmarshalText([]byte("5"))
	if err.Error() != "time: missing unit in duration 5" {
		t.Fatalf("expected time: missing unit in duration: %s", err)
	}

	err = d.UnmarshalText([]byte("3m2s"))
	v, _ = d.MarshalText()
	if !reflect.DeepEqual(b, v) {
		t.Fatalf("Unexpected marshalled value %v", v)
	}
}
