package pilosa_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/pilosa/pilosa"
)

func Test_NewConfig(t *testing.T) {
	c := pilosa.NewConfig()

	if c.Cluster.Disabled != pilosa.DefaultClusterDisabled {
		t.Fatalf("unexpected Cluster.Disabled: %v", c.Cluster.Disabled)
	}

	// Ensure that hosts can't be specificed on a non-disabled cluster.
	c.Cluster.Hosts = []string{c.Bind, "localhost:10102"}

	// Change cluster type from the default (gossip) to an invalid string.
	if err := c.Validate(); err != pilosa.ErrConfigClusterEnabledHosts {
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
