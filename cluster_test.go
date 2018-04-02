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
	"bytes"
	"math/rand"
	"reflect"
	"testing"
	"testing/quick"

	"github.com/davecgh/go-spew/spew"
	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/test"
)

// Ensure the cluster can fairly distribute partitions across the nodes.
func TestCluster_Owners(t *testing.T) {
	c := pilosa.Cluster{
		Nodes: []*pilosa.Node{
			{URI: test.NewURIFromHostPort("serverA", 1000)},
			{URI: test.NewURIFromHostPort("serverB", 1000)},
			{URI: test.NewURIFromHostPort("serverC", 1000)},
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

// Ensure OwnsSlices can find the actual slice list for node and index.
func TestCluster_OwnsSlices(t *testing.T) {
	c := test.NewCluster(5)
	slices := c.OwnsSlices("test", 10, test.NewURIFromHostPort("host2", 0))

	if !reflect.DeepEqual(slices, []uint64{0, 3, 6, 10}) {
		t.Fatalf("unexpected slices for node's index: %v", slices)
	}
}

// Ensure ContainsSlices can find the actual slice list for node and index.
func TestCluster_ContainsSlices(t *testing.T) {
	c := test.NewCluster(5)
	c.ReplicaN = 3
	slices := c.ContainsSlices("test", 10, c.Nodes[2])

	if !reflect.DeepEqual(slices, []uint64{0, 2, 3, 5, 6, 9, 10}) {
		t.Fatalf("unexpected slices for node's index: %v", slices)
	}
}

func TestCluster_Nodes(t *testing.T) {
	uri0 := test.NewURIFromHostPort("node0", 0)
	uri1 := test.NewURIFromHostPort("node1", 0)
	uri2 := test.NewURIFromHostPort("node2", 0)
	uri3 := test.NewURIFromHostPort("node3", 0)

	node0 := &pilosa.Node{ID: "node0", URI: uri0}
	node1 := &pilosa.Node{ID: "node1", URI: uri1}
	node2 := &pilosa.Node{ID: "node2", URI: uri2}
	node3 := &pilosa.Node{ID: "node3", URI: uri3}

	nodes := []*pilosa.Node{node0, node1, node2}

	t.Run("NodeIDs", func(t *testing.T) {
		actual := pilosa.Nodes(nodes).IDs()
		expected := []string{node0.ID, node1.ID, node2.ID}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("expected: %v, but got: %v", expected, actual)
		}
	})

	t.Run("Filter", func(t *testing.T) {
		actual := pilosa.Nodes(pilosa.Nodes(nodes).Filter(nodes[1])).URIs()
		expected := []pilosa.URI{uri0, uri2}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("expected: %v, but got: %v", expected, actual)
		}
	})

	t.Run("FilterURI", func(t *testing.T) {
		actual := pilosa.Nodes(pilosa.Nodes(nodes).FilterURI(uri1)).URIs()
		expected := []pilosa.URI{uri0, uri2}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("expected: %v, but got: %v", expected, actual)
		}
	})

	t.Run("Contains", func(t *testing.T) {
		actualTrue := pilosa.Nodes(nodes).Contains(node1)
		actualFalse := pilosa.Nodes(nodes).Contains(node3)
		if !reflect.DeepEqual(actualTrue, true) {
			t.Errorf("expected: %v, but got: %v", true, actualTrue)
		}
		if !reflect.DeepEqual(actualFalse, false) {
			t.Errorf("expected: %v, but got: %v", false, actualTrue)
		}
	})

	t.Run("Clone", func(t *testing.T) {
		clone := pilosa.Nodes(nodes).Clone()
		actual := pilosa.Nodes(clone).URIs()
		expected := []pilosa.URI{uri0, uri1, uri2}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("expected: %v, but got: %v", expected, actual)
		}
	})
}

func TestCluster_Coordinator(t *testing.T) {
	uri1 := test.NewURIFromHostPort("node1", 0)
	uri2 := test.NewURIFromHostPort("node2", 0)

	node1 := &pilosa.Node{ID: "node1", URI: uri1}
	node2 := &pilosa.Node{ID: "node2", URI: uri2}

	c1 := *pilosa.NewCluster()
	c1.Node = node1
	c1.Coordinator = node1.ID
	c2 := *pilosa.NewCluster()
	c2.Node = node2
	c2.Coordinator = node1.ID

	t.Run("IsCoordinator", func(t *testing.T) {
		if !c1.IsCoordinator() {
			t.Errorf("!IsCoordinator error: %v", c1.Node)
		} else if c2.IsCoordinator() {
			t.Errorf("IsCoordinator error: %v", c2.Node)
		}
	})
}

func TestCluster_Topology(t *testing.T) {
	c1 := test.NewCluster(1) // automatically creates Node{ID: "node0"}

	uri0 := test.NewURIFromHostPort("host0", 0)
	uri1 := test.NewURIFromHostPort("host1", 0)
	uri2 := test.NewURIFromHostPort("host2", 0)
	invalid := test.NewURIFromHostPort("invalid", 0)

	node0 := &pilosa.Node{ID: "node0", URI: uri0}
	node1 := &pilosa.Node{ID: "node1", URI: uri1}
	node2 := &pilosa.Node{ID: "node2", URI: uri2}
	nodeinvalid := &pilosa.Node{ID: "nodeinvalid", URI: invalid}

	t.Run("AddNode", func(t *testing.T) {
		err := c1.AddNode(node1)
		if err != nil {
			t.Fatal(err)
		}
		// add the same host.
		err = c1.AddNode(node1)
		if err != nil {
			t.Fatal(err)
		}
		err = c1.AddNode(node2)
		if err != nil {
			t.Fatal(err)
		}

		actual := c1.NodeIDs()
		expected := []string{node0.ID, node1.ID, node2.ID}

		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("expected: %v, but got: %v", expected, actual)
		}
	})

	t.Run("ContainsID", func(t *testing.T) {
		if !c1.Topology.ContainsID(node1.ID) {
			t.Errorf("!ContainsHost error: %v", node1.ID)
		} else if c1.Topology.ContainsID(nodeinvalid.ID) {
			t.Errorf("ContainsHost error: %v", nodeinvalid.ID)
		}
	})
}

// Ensure that general cluster functionality works as expected.
func TestCluster_ResizeStates(t *testing.T) {

	t.Run("Single node, no data", func(t *testing.T) {
		tc := test.NewTestCluster(1)

		// Open TestCluster.
		if err := tc.Open(); err != nil {
			t.Fatal(err)
		}

		node := tc.Clusters[0]

		// Ensure that node comes up in state NORMAL.
		if node.State() != pilosa.ClusterStateNormal {
			t.Errorf("expected state: %v, but got: %v", pilosa.ClusterStateNormal, node.State())
		}

		expectedTop := &pilosa.Topology{
			NodeIDs: []string{node.Node.ID},
		}

		// Verify topology file.
		if !reflect.DeepEqual(node.Topology.NodeIDs, expectedTop.NodeIDs) {
			t.Errorf("expected topology: %v, but got: %v", expectedTop.NodeIDs, node.Topology.NodeIDs)
		}

		// Close TestCluster.
		if err := tc.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Single node, in topology", func(t *testing.T) {
		tc := test.NewTestCluster(0)
		tc.AddNode(false)

		node := tc.Clusters[0]

		// write topology to data file
		top := &pilosa.Topology{
			NodeIDs: []string{node.Node.ID},
		}
		tc.WriteTopology(node.Path, top)

		// Open TestCluster.
		if err := tc.Open(); err != nil {
			t.Fatal(err)
		}

		// Ensure that node comes up in state NORMAL.
		if node.State() != pilosa.ClusterStateNormal {
			t.Errorf("expected state: %v, but got: %v", pilosa.ClusterStateNormal, node.State())
		}

		// Close TestCluster.
		if err := tc.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Single node, not in topology", func(t *testing.T) {
		tc := test.NewTestCluster(0)
		tc.AddNode(false)

		node := tc.Clusters[0]

		// write topology to data file
		top := &pilosa.Topology{
			NodeIDs: []string{"some-other-host"},
		}
		tc.WriteTopology(node.Path, top)

		// Open TestCluster.
		expected := "considerTopology: coordinator node0 is not in topology: [some-other-host]"
		err := tc.Open()
		if err == nil || err.Error() != expected {
			t.Errorf("did not receive expected error: %s", expected)
		}

		// Close TestCluster.
		if err := tc.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Multiple nodes, no data", func(t *testing.T) {
		tc := test.NewTestCluster(0)
		tc.AddNode(false)

		// Open TestCluster.
		if err := tc.Open(); err != nil {
			t.Fatal(err)
		}

		tc.AddNode(false)

		node0 := tc.Clusters[0]
		node1 := tc.Clusters[1]

		// Ensure that nodes comes up in state NORMAL.
		if node0.State() != pilosa.ClusterStateNormal {
			t.Errorf("expected node0 state: %v, but got: %v", pilosa.ClusterStateNormal, node0.State())
		} else if node1.State() != pilosa.ClusterStateNormal {
			t.Errorf("expected node1 state: %v, but got: %v", pilosa.ClusterStateNormal, node1.State())
		}

		expectedTop := &pilosa.Topology{
			NodeIDs: []string{node0.Node.ID, node1.Node.ID},
		}

		// Verify topology file.
		if !reflect.DeepEqual(node0.Topology.NodeIDs, expectedTop.NodeIDs) {
			t.Errorf("expected node0 topology: %v, but got: %v", expectedTop.NodeIDs, node0.Topology.NodeIDs)
		} else if !reflect.DeepEqual(node1.Topology.NodeIDs, expectedTop.NodeIDs) {
			t.Errorf("expected node1 topology: %v, but got: %v", expectedTop.NodeIDs, node1.Topology.NodeIDs)
		}

		// Close TestCluster.
		if err := tc.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Multiple nodes, in/not in topology", func(t *testing.T) {
		tc := test.NewTestCluster(0)
		tc.AddNode(false)
		node0 := tc.Clusters[0]

		// write topology to data file
		top := &pilosa.Topology{
			NodeIDs: []string{"node0", "node2"},
		}
		tc.WriteTopology(node0.Path, top)

		// Open TestCluster.
		if err := tc.Open(); err != nil {
			t.Fatal(err)
		}

		// Ensure that node is in state STARTING before the other node joins.
		if node0.State() != pilosa.ClusterStateStarting {
			t.Errorf("expected node0 state: %v, but got: %v", pilosa.ClusterStateStarting, node0.State())
		}

		// Expect an error by adding a node not in the topology.
		expectedError := "host is not in topology: node1"
		err := tc.AddNode(false)
		if err == nil || err.Error() != expectedError {
			t.Errorf("did not receive expected error: %s", expectedError)
		}

		tc.AddNode(false)
		node2 := tc.Clusters[2]

		// Ensure that node comes up in state NORMAL.
		if node0.State() != pilosa.ClusterStateNormal {
			t.Errorf("expected node0 state: %v, but got: %v", pilosa.ClusterStateNormal, node0.State())
		} else if node2.State() != pilosa.ClusterStateNormal {
			t.Errorf("expected node1 state: %v, but got: %v", pilosa.ClusterStateNormal, node2.State())
		}

		// Close TestCluster.
		if err := tc.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Multiple nodes, with data", func(t *testing.T) {
		tc := test.NewTestCluster(0)
		tc.AddNode(false)
		node0 := tc.Clusters[0]

		// Open TestCluster.
		if err := tc.Open(); err != nil {
			t.Fatal(err)
		}

		// Add Bit Data to node0.
		if err := tc.CreateFrame("i", "f", pilosa.FrameOptions{}); err != nil {
			t.Fatal(err)
		}
		tc.SetBit("i", "f", "standard", 1, 101, nil)
		tc.SetBit("i", "f", "standard", 1, 1300000, nil)

		// Add Field Data to node0.
		if err := tc.CreateFrame("i", "fields", pilosa.FrameOptions{
			InverseEnabled: false,
			RangeEnabled:   true,
			//CacheType:      pilosa.CacheTypeNone,
			Fields: []*pilosa.Field{
				{
					Name: "fld0",
					Type: pilosa.FieldTypeInt,
					Min:  -100,
					Max:  100,
				},
			},
		}); err != nil {
			t.Fatal(err)
		}
		tc.SetFieldValue("i", "fields", 1, "fld0", -10)
		tc.SetFieldValue("i", "fields", 1, "fld0", 10)
		tc.SetFieldValue("i", "fields", 1300000, "fld0", -99)
		tc.SetFieldValue("i", "fields", 1300000, "fld0", 99)

		// Before starting the resize, get the CheckSum to use for
		// comparison later.
		node0Frame := node0.Holder.Frame("i", "f")
		node0View := node0Frame.View("standard")
		node0Fragment := node0View.Fragment(1)
		node0Checksum := node0Fragment.Checksum()

		node0Frame = node0.Holder.Frame("i", "fields")
		node0View = node0Frame.View("field_fld0")
		node0Fragment = node0View.Fragment(1)
		node0ChecksumFld := node0Fragment.Checksum()

		// AddNode needs to block until the resize process has completed.
		tc.AddNode(false)
		node1 := tc.Clusters[1]

		// Ensure that nodes come up in state NORMAL.
		if node0.State() != pilosa.ClusterStateNormal {
			t.Errorf("expected node0 state: %v, but got: %v", pilosa.ClusterStateNormal, node0.State())
		} else if node1.State() != pilosa.ClusterStateNormal {
			t.Errorf("expected node1 state: %v, but got: %v", pilosa.ClusterStateNormal, node1.State())
		}

		expectedTop := &pilosa.Topology{
			NodeIDs: []string{node0.Node.ID, node1.Node.ID},
		}

		// Verify topology file.
		if !reflect.DeepEqual(node0.Topology.NodeIDs, expectedTop.NodeIDs) {
			t.Errorf("expected node0 topology: %v, but got: %v", expectedTop.NodeIDs, node0.Topology.NodeIDs)
		} else if !reflect.DeepEqual(node1.Topology.NodeIDs, expectedTop.NodeIDs) {
			t.Errorf("expected node1 topology: %v, but got: %v", expectedTop.NodeIDs, node1.Topology.NodeIDs)
		}

		// Bits
		// Verify that node-1 contains the fragment (i/f/standard/1) transferred from node-0.
		node1Frame := node1.Holder.Frame("i", "f")
		node1View := node1Frame.View("standard")
		node1Fragment := node1View.Fragment(1)

		// Ensure checksums are the same.
		if chksum := node1Fragment.Checksum(); !bytes.Equal(chksum, node0Checksum) {
			t.Fatalf("expected standard view checksum to match: %x - %x", chksum, node0Checksum)
		}

		// Values
		// Verify that node-1 contains the fragment (i/fields/field_fld0/1) transferred from node-0.
		node1Frame = node1.Holder.Frame("i", "fields")
		node1View = node1Frame.View("field_fld0")
		node1Fragment = node1View.Fragment(1)

		// Ensure checksums are the same.
		if chksum := node1Fragment.Checksum(); !bytes.Equal(chksum, node0ChecksumFld) {
			t.Fatalf("expected checksum to match: %x - %x", chksum, node0ChecksumFld)
		}

		// Close TestCluster.
		if err := tc.Close(); err != nil {
			t.Fatal(err)
		}
	})
}

// Ensures that coordinator can be changed.
func TestCluster_UpdateCoordinator(t *testing.T) {
	t.Run("UpdateCoordinator", func(t *testing.T) {
		c := test.NewCluster(2)

		oldNode := c.Nodes[0]
		newNode := c.Nodes[1]

		// Update coordinator to the same value.
		if c.UpdateCoordinator(oldNode) {
			t.Errorf("did not expect coordinator to change")
		} else if c.Coordinator != oldNode.ID {
			t.Errorf("expected coordinator: %s, but got: %s", c.Coordinator, oldNode.URI)
		}

		// Update coordinator to a new value.
		if !c.UpdateCoordinator(newNode) {
			t.Errorf("expected coordinator to change")
		} else if c.Coordinator != newNode.ID {
			t.Errorf("expected coordinator: %s, but got: %s", c.Coordinator, newNode.URI)
		}
	})
}
