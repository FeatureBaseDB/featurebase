// Copyright 2017 Pilosa Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

	// Change cluster type from the default (gossip) to an invalid string.
	c.Cluster.Type = "invalid-type"
	if err := c.Validate(); err != pilosa.ErrConfigClusterTypeInvalid {
		t.Fatal(err)
	}

	// Change cluster type back to gossip.
	c.Cluster.Type = pilosa.ClusterGossip

	// Check for bind address in cluster hosts.
	c.Bind = "localhost:1"
	if err := c.Validate(); err != pilosa.ErrConfigHostsMissing {
		t.Fatal(err)
	}

	c.Bind = "localhost:10101"
	c.Cluster.ReplicaN = 2
	c.GossipSeed = "localhost:14000"
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
