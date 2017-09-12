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
	"math/rand"
	"reflect"
	"testing"
	"testing/quick"

	"github.com/davecgh/go-spew/spew"
	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/internal"
	"github.com/pilosa/pilosa/test"
)

// Ensure the cluster can fairly distribute partitions across the nodes.
func TestCluster_Owners(t *testing.T) {
	c := pilosa.Cluster{
		Nodes: []*pilosa.Node{
			{Host: "serverA:1000"},
			{Host: "serverB:1000"},
			{Host: "serverC:1000"},
		},
		Hasher:   test.NewModHasher(),
		ReplicaN: 2,
	}

	// Verify nodes are distributed.
	if a := c.PartitionNodes(0); !reflect.DeepEqual(a, []*pilosa.Node{c.Nodes[0], c.Nodes[1]}) {
		t.Fatalf("unexpected owners: %s", spew.Sdump(a))
	}

	// Verify nodes go around the ring.
	if a := c.PartitionNodes(2); !reflect.DeepEqual(a, []*pilosa.Node{c.Nodes[2], c.Nodes[0]}) {
		t.Fatalf("unexpected owners: %s", spew.Sdump(a))
	}
}

// Ensure the partitioner can assign a fragment to a partition.
func TestCluster_Partition(t *testing.T) {
	if err := quick.Check(func(index string, slice uint64, partitionN int) bool {
		c := pilosa.NewCluster()
		c.PartitionN = partitionN

		partitionID := c.Partition(index, slice)
		if partitionID < 0 || partitionID >= partitionN {
			t.Errorf("partition out of range: slice=%d, p=%d, n=%d", slice, partitionID, partitionN)
		}

		return true
	}, &quick.Config{
		Values: func(values []reflect.Value, rand *rand.Rand) {
			values[0], _ = quick.Value(reflect.TypeOf(""), rand)
			values[1] = reflect.ValueOf(uint64(rand.Uint32()))
			values[2] = reflect.ValueOf(rand.Intn(1000) + 1)
		},
	}); err != nil {
		t.Fatal(err)
	}
}

// Ensure the hasher can hash correctly.
func TestHasher(t *testing.T) {
	for _, tt := range []struct {
		key    uint64
		bucket []int
	}{
		// Generated from the reference C++ code
		{0, []int{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}},
		{1, []int{0, 0, 0, 0, 0, 0, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 17, 17}},
		{0xdeadbeef, []int{0, 1, 2, 3, 3, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 16, 16, 16}},
		{0x0ddc0ffeebadf00d, []int{0, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 15, 15, 15, 15}},
	} {
		for i, v := range tt.bucket {
			if got := pilosa.NewHasher().Hash(tt.key, i+1); got != v {
				t.Errorf("hash(%v,%v)=%v, want %v", tt.key, i+1, got, v)
			}
		}
	}
}

/* TODO travis: fix test
// Ensure cluster can compare its Nodes and Members
func TestCluster_NodeStates(t *testing.T) {
	c := pilosa.Cluster{
		Nodes: []*pilosa.Node{
			{Host: "serverA:1000"},
			{Host: "serverB:1000"},
			{Host: "serverC:1000"},
		},
		NodeSet: &pilosa.StaticNodeSet{},
	}

	err := c.NodeSet.(*pilosa.StaticNodeSet).Join([]*pilosa.Node{
		&pilosa.Node{Host: "serverA:1000"},
		&pilosa.Node{Host: "serverC:1000"},
		&pilosa.Node{Host: "serverD:1000"},
	})
	if err != nil {
		t.Fatalf("unexpected gossiper nodes: %s", err)
	}

	// Verify a DOWN node is reported, and extraneous nodes are ignored
	if a := c.NodeStates(); !reflect.DeepEqual(a, map[string]string{
		"serverA:1000": pilosa.NodeStateUp,
		"serverB:1000": pilosa.NodeStateDown,
		"serverC:1000": pilosa.NodeStateUp,
	}) {
		t.Fatalf("unexpected node state: %s", spew.Sdump(a))
	}
}
*/

// Ensure OwnsSlices can find the actual slice list for node and index.
func TestCluster_OwnsSlices(t *testing.T) {
	c := test.NewCluster(5)
	slices := c.OwnsSlices("test", 10, "host2")

	if !reflect.DeepEqual(slices, []uint64{0, 3, 6, 10}) {
		t.Fatalf("unexpected slices for node's index: %v", slices)
	}
}

func TestCluster_Nodes(t *testing.T) {

	nodes := []*pilosa.Node{
		&pilosa.Node{Host: "node0"},
		&pilosa.Node{Host: "node1"},
		&pilosa.Node{Host: "node2"},
	}

	t.Run("Hosts", func(t *testing.T) {
		actual := pilosa.Nodes(nodes).Hosts()
		expected := []string{"node0", "node1", "node2"}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("expected: %v, but got: %v", expected, actual)
		}
	})

	t.Run("Filter", func(t *testing.T) {
		actual := pilosa.Nodes(pilosa.Nodes(nodes).Filter(nodes[1])).Hosts()
		expected := []string{"node0", "node2"}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("expected: %v, but got: %v", expected, actual)
		}
	})

	t.Run("FilterHost", func(t *testing.T) {
		actual := pilosa.Nodes(pilosa.Nodes(nodes).FilterHost("node1")).Hosts()
		expected := []string{"node0", "node2"}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("expected: %v, but got: %v", expected, actual)
		}
	})

	t.Run("Contains", func(t *testing.T) {
		actualTrue := pilosa.Nodes(nodes).Contains(nodes[1])
		actualFalse := pilosa.Nodes(nodes).Contains(&pilosa.Node{})
		if !reflect.DeepEqual(actualTrue, true) {
			t.Errorf("expected: %v, but got: %v", true, actualTrue)
		}
		if !reflect.DeepEqual(actualFalse, false) {
			t.Errorf("expected: %v, but got: %v", false, actualTrue)
		}
	})

	t.Run("ContainsHost", func(t *testing.T) {
		actualTrue := pilosa.Nodes(nodes).ContainsHost("node1")
		actualFalse := pilosa.Nodes(nodes).ContainsHost("nodeX")
		if !reflect.DeepEqual(actualTrue, true) {
			t.Errorf("expected: %v, but got: %v", true, actualTrue)
		}
		if !reflect.DeepEqual(actualFalse, false) {
			t.Errorf("expected: %v, but got: %v", false, actualTrue)
		}
	})

	t.Run("Clone", func(t *testing.T) {
		clone := pilosa.Nodes(nodes).Clone()
		actual := pilosa.Nodes(clone).Hosts()
		expected := []string{"node0", "node1", "node2"}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("expected: %v, but got: %v", expected, actual)
		}
	})
}

func TestCluster_Coordinator(t *testing.T) {

	c1 := *pilosa.NewCluster()
	c1.Host = "host0:port0"
	c1.Coordinator = "host0:port0"
	c2 := *pilosa.NewCluster()
	c2.Host = "host1:port1"
	c2.Coordinator = "host0:port0"

	t.Run("IsCoordinator", func(t *testing.T) {
		if !c1.IsCoordinator() {
			t.Errorf("!IsCoordinator error: %v", c1.Host)
		} else if c2.IsCoordinator() {
			t.Errorf("IsCoordinator error: %v", c2.Host)
		}
	})
}

func TestCluster_Topology(t *testing.T) {
	c1 := test.NewCluster(1)

	t.Run("AddHost", func(t *testing.T) {
		err := c1.AddHost("abc")
		if err != nil {
			t.Fatal(err)
		}
		// add the same host.
		err = c1.AddHost("abc")
		if err != nil {
			t.Fatal(err)
		}
		err = c1.AddHost("xyz")
		if err != nil {
			t.Fatal(err)
		}

		actual := pilosa.Nodes(c1.Nodes).Hosts()
		expected := []string{"abc", "host0", "xyz"}

		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("expected: %v, but got: %v", expected, actual)
		}
	})

	t.Run("ContainsHost", func(t *testing.T) {
		if !c1.Topology.ContainsHost("abc") {
			t.Errorf("!ContainsHost error: %v", "abc")
		} else if c1.Topology.ContainsHost("invalidHost") {
			t.Errorf("ContainsHost error: %v", "invalidHost")
		}
	})
}

// Ensure DataDiff can generate the expected source map.
func TestCluster_Resize(t *testing.T) {
	// Given two clusters, ensure DataDiff can determine the sources of data
	// needed in order to respond to queries.
	t.Run("DataDiff", func(t *testing.T) {

		// Holder
		h1 := test.NewHolder()
		i, err := h1.CreateIndex("i", pilosa.IndexOptions{})
		if err != nil {
			t.Fatal(err)
		}
		f, err := i.CreateFrame("f", pilosa.FrameOptions{InverseEnabled: true})
		if err != nil {
			t.Fatal(err)
		}
		_, err = f.CreateViewIfNotExists("v")
		if err != nil {
			t.Fatal(err)
		}
		_, err = f.CreateViewIfNotExists("inverse")
		if err != nil {
			t.Fatal(err)
		}

		// Set max slices.
		i.SetRemoteMaxSlice(2)
		i.SetRemoteMaxInverseSlice(5)

		// Cluster 1
		c1 := test.NewCluster(3)
		c1.IndexReporter = h1
		c1.ReplicaN = 2

		// Cluster 2
		c2 := test.NewCluster(4)
		c2.ReplicaN = 2

		expected := map[string][]*internal.ResizeSource{
			"host0": []*internal.ResizeSource{
				{Host: "host1", Index: "i", Frame: "f", View: "inverse", Slice: 5},
			},
			"host1": []*internal.ResizeSource{
				{Host: "host2", Index: "i", Frame: "f", View: "v", Slice: 0},
				{Host: "host2", Index: "i", Frame: "f", View: "inverse", Slice: 0},
			},
			"host2": []*internal.ResizeSource{
				{Host: "host0", Index: "i", Frame: "f", View: "inverse", Slice: 3},
			},
			"host3": []*internal.ResizeSource{
				{Host: "host0", Index: "i", Frame: "f", View: "v", Slice: 1},
				{Host: "host1", Index: "i", Frame: "f", View: "v", Slice: 2},
				{Host: "host0", Index: "i", Frame: "f", View: "inverse", Slice: 1},
				{Host: "host1", Index: "i", Frame: "f", View: "inverse", Slice: 2},
				{Host: "host1", Index: "i", Frame: "f", View: "inverse", Slice: 5},
			},
		}

		actual := c1.DataDiff(c2, i)
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("expected: %v, but got: %v", expected, actual)
		}
	})
}
