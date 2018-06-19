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

package pilosa

import (
	"bytes"
	"io/ioutil"
	"math/rand"
	"reflect"
	"strings"
	"testing"
	"testing/quick"

	"github.com/davecgh/go-spew/spew"
	"github.com/pilosa/pilosa/internal"
)

// Ensure that fragCombos creates the correct fragment mapping.
func TestFragCombos(t *testing.T) {

	uri0, err := NewURIFromAddress("host0")
	if err != nil {
		t.Fatal(err)
	}
	uri1, err := NewURIFromAddress("host1")
	if err != nil {
		t.Fatal(err)
	}

	node0 := &Node{ID: "node0", URI: *uri0}
	node1 := &Node{ID: "node1", URI: *uri1}

	c := NewCluster()
	c.addNodeBasicSorted(node0)
	c.addNodeBasicSorted(node1)

	tests := []struct {
		idx        string
		maxSlice   uint64
		fieldViews viewsByField
		expected   fragsByHost
	}{
		{
			idx:        "i",
			maxSlice:   uint64(2),
			fieldViews: viewsByField{"f": []string{"v1", "v2"}},
			expected: fragsByHost{
				"node0": []frag{{"f", "v1", uint64(0)}, {"f", "v2", uint64(0)}},
				"node1": []frag{{"f", "v1", uint64(1)}, {"f", "v2", uint64(1)}, {"f", "v1", uint64(2)}, {"f", "v2", uint64(2)}},
			},
		},
		{
			idx:        "foo",
			maxSlice:   uint64(3),
			fieldViews: viewsByField{"f": []string{"v0"}},
			expected: fragsByHost{
				"node0": []frag{{"f", "v0", uint64(1)}, {"f", "v0", uint64(2)}},
				"node1": []frag{{"f", "v0", uint64(0)}, {"f", "v0", uint64(3)}},
			},
		},
	}
	for _, test := range tests {

		actual := c.fragCombos(test.idx, test.maxSlice, test.fieldViews)
		if !reflect.DeepEqual(actual, test.expected) {
			t.Errorf("expected: %v, but got: %v", test.expected, actual)
		}

	}
}

// newIndexWithTempPath returns a new instance of Index.
func newIndexWithTempPath(name string) *Index {
	path, err := ioutil.TempDir("", "pilosa-index-")
	if err != nil {
		panic(err)
	}
	index, err := NewIndex(path, name)
	if err != nil {
		panic(err)
	}
	return index
}

// Ensure that fragSources creates the correct fragment mapping.
func TestFragSources(t *testing.T) {

	uri0, err := NewURIFromAddress("host0")
	if err != nil {
		t.Fatal(err)
	}
	uri1, err := NewURIFromAddress("host1")
	if err != nil {
		t.Fatal(err)
	}
	uri2, err := NewURIFromAddress("host2")
	if err != nil {
		t.Fatal(err)
	}
	uri3, err := NewURIFromAddress("host3")
	if err != nil {
		t.Fatal(err)
	}

	node0 := &Node{ID: "node0", URI: *uri0}
	node1 := &Node{ID: "node1", URI: *uri1}
	node2 := &Node{ID: "node2", URI: *uri2}
	node3 := &Node{ID: "node3", URI: *uri3}

	c1 := NewCluster()
	c1.ReplicaN = 1
	c1.addNodeBasicSorted(node0)
	c1.addNodeBasicSorted(node1)

	c2 := NewCluster()
	c2.ReplicaN = 1
	c2.addNodeBasicSorted(node0)
	c2.addNodeBasicSorted(node1)
	c2.addNodeBasicSorted(node2)

	c3 := NewCluster()
	c3.ReplicaN = 2
	c3.addNodeBasicSorted(node0)
	c3.addNodeBasicSorted(node1)

	c4 := NewCluster()
	c4.ReplicaN = 2
	c4.addNodeBasicSorted(node0)
	c4.addNodeBasicSorted(node1)
	c4.addNodeBasicSorted(node2)

	c5 := NewCluster()
	c5.ReplicaN = 2
	c5.addNodeBasicSorted(node0)
	c5.addNodeBasicSorted(node1)
	c5.addNodeBasicSorted(node2)
	c5.addNodeBasicSorted(node3)

	idx := newIndexWithTempPath("i")
	field, err := idx.CreateFieldIfNotExists("f", FieldOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = field.SetBit("standard", 1, 101, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = field.SetBit("standard", 1, 1300000, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = field.SetBit("standard", 1, 2600000, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = field.SetBit("standard", 1, 3900000, nil)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		from     *Cluster
		to       *Cluster
		idx      *Index
		expected map[string][]*internal.ResizeSource
		err      string
	}{
		{
			from: c1,
			to:   c2,
			idx:  idx,
			expected: map[string][]*internal.ResizeSource{
				"node0": []*internal.ResizeSource{},
				"node1": []*internal.ResizeSource{},
				"node2": []*internal.ResizeSource{
					{&internal.Node{"node0", &internal.URI{"http", "host0", 10101}, false}, "i", "f", "standard", uint64(0)},
					{&internal.Node{"node1", &internal.URI{"http", "host1", 10101}, false}, "i", "f", "standard", uint64(2)},
				},
			},
			err: "",
		},
		{
			from: c4,
			to:   c3,
			idx:  idx,
			expected: map[string][]*internal.ResizeSource{
				"node0": []*internal.ResizeSource{
					{&internal.Node{"node1", &internal.URI{"http", "host1", 10101}, false}, "i", "f", "standard", uint64(1)},
				},
				"node1": []*internal.ResizeSource{
					{&internal.Node{"node0", &internal.URI{"http", "host0", 10101}, false}, "i", "f", "standard", uint64(0)},
					{&internal.Node{"node0", &internal.URI{"http", "host0", 10101}, false}, "i", "f", "standard", uint64(2)},
				},
			},
			err: "",
		},
		{
			from: c5,
			to:   c4,
			idx:  idx,
			expected: map[string][]*internal.ResizeSource{
				"node0": []*internal.ResizeSource{
					{&internal.Node{"node2", &internal.URI{"http", "host2", 10101}, false}, "i", "f", "standard", uint64(0)},
					{&internal.Node{"node2", &internal.URI{"http", "host2", 10101}, false}, "i", "f", "standard", uint64(2)},
				},
				"node1": []*internal.ResizeSource{
					{&internal.Node{"node0", &internal.URI{"http", "host0", 10101}, false}, "i", "f", "standard", uint64(3)},
				},
				"node2": []*internal.ResizeSource{},
			},
			err: "",
		},
		{
			from:     c2,
			to:       c4,
			idx:      idx,
			expected: nil,
			err:      "clusters are the same size",
		},
		{
			from:     c1,
			to:       c5,
			idx:      idx,
			expected: nil,
			err:      "adding more than one node at a time is not supported",
		},
		{
			from:     c5,
			to:       c1,
			idx:      idx,
			expected: nil,
			err:      "removing more than one node at a time is not supported",
		},
	}
	for _, test := range tests {

		actual, err := (test.from).fragSources(test.to, test.idx)
		if test.err != "" {
			if !strings.Contains(err.Error(), test.err) {
				t.Fatalf("expected error: %s, got: %s", test.err, err.Error())
			}
		} else {
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(actual, test.expected) {
				t.Errorf("expected: %v, but got: %v", test.expected, actual)
			}
		}
	}
}

// Ensure that fragSources creates the correct fragment mapping.
func TestResizeJob(t *testing.T) {

	uri0, err := NewURIFromAddress("host0")
	if err != nil {
		t.Fatal(err)
	}
	uri1, err := NewURIFromAddress("host1")
	if err != nil {
		t.Fatal(err)
	}
	uri2, err := NewURIFromAddress("host2")
	if err != nil {
		t.Fatal(err)
	}

	node0 := &Node{ID: "node0", URI: *uri0}
	node1 := &Node{ID: "node1", URI: *uri1}
	node2 := &Node{ID: "node2", URI: *uri2}

	tests := []struct {
		existingNodes []*Node
		node          *Node
		action        string
		expectedIDs   map[string]bool
	}{
		{
			existingNodes: []*Node{node0, node1},
			node:          node2,
			action:        resizeJobActionAdd,
			expectedIDs:   map[string]bool{node0.ID: false, node1.ID: false, node2.ID: false},
		},
		{
			existingNodes: []*Node{node0, node1, node2},
			node:          node2,
			action:        resizeJobActionRemove,
			expectedIDs:   map[string]bool{node0.ID: false, node1.ID: false},
		},
	}
	for _, test := range tests {

		actual := newResizeJob(test.existingNodes, test.node, test.action)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(actual.IDs, test.expectedIDs) {
			t.Errorf("expected: %v, but got: %v", test.expectedIDs, actual.IDs)
		}
	}
}

// Ensure the cluster can fairly distribute partitions across the nodes.
func TestCluster_Owners(t *testing.T) {
	c := Cluster{
		Nodes: []*Node{
			{URI: NewTestURIFromHostPort("serverA", 1000)},
			{URI: NewTestURIFromHostPort("serverB", 1000)},
			{URI: NewTestURIFromHostPort("serverC", 1000)},
		},
		Hasher:   NewTestModHasher(),
		ReplicaN: 2,
	}

	// Verify nodes are distributed.
	if a := c.partitionNodes(0); !reflect.DeepEqual(a, []*Node{c.Nodes[0], c.Nodes[1]}) {
		t.Fatalf("unexpected owners: %s", spew.Sdump(a))
	}

	// Verify nodes go around the ring.
	if a := c.partitionNodes(2); !reflect.DeepEqual(a, []*Node{c.Nodes[2], c.Nodes[0]}) {
		t.Fatalf("unexpected owners: %s", spew.Sdump(a))
	}
}

// Ensure the partitioner can assign a fragment to a partition.
func TestCluster_Partition(t *testing.T) {
	if err := quick.Check(func(index string, slice uint64, partitionN int) bool {
		c := NewCluster()
		c.PartitionN = partitionN

		partitionID := c.partition(index, slice)
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
			if got := NewHasher().Hash(tt.key, i+1); got != v {
				t.Errorf("hash(%v,%v)=%v, want %v", tt.key, i+1, got, v)
			}
		}
	}
}

// Ensure ContainsSlices can find the actual slice list for node and index.
func TestCluster_ContainsSlices(t *testing.T) {
	c := NewTestCluster(5)
	c.ReplicaN = 3
	slices := c.containsSlices("test", 10, c.Nodes[2])

	if !reflect.DeepEqual(slices, []uint64{0, 2, 3, 5, 6, 9, 10}) {
		t.Fatalf("unexpected slices for node's index: %v", slices)
	}
}

func TestCluster_Nodes(t *testing.T) {
	uri0 := NewTestURIFromHostPort("node0", 0)
	uri1 := NewTestURIFromHostPort("node1", 0)
	uri2 := NewTestURIFromHostPort("node2", 0)
	uri3 := NewTestURIFromHostPort("node3", 0)

	node0 := &Node{ID: "node0", URI: uri0}
	node1 := &Node{ID: "node1", URI: uri1}
	node2 := &Node{ID: "node2", URI: uri2}
	node3 := &Node{ID: "node3", URI: uri3}

	nodes := []*Node{node0, node1, node2}

	t.Run("NodeIDs", func(t *testing.T) {
		actual := Nodes(nodes).IDs()
		expected := []string{node0.ID, node1.ID, node2.ID}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("expected: %v, but got: %v", expected, actual)
		}
	})

	t.Run("Filter", func(t *testing.T) {
		actual := Nodes(Nodes(nodes).Filter(nodes[1])).URIs()
		expected := []URI{uri0, uri2}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("expected: %v, but got: %v", expected, actual)
		}
	})

	t.Run("FilterURI", func(t *testing.T) {
		actual := Nodes(Nodes(nodes).FilterURI(uri1)).URIs()
		expected := []URI{uri0, uri2}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("expected: %v, but got: %v", expected, actual)
		}
	})

	t.Run("Contains", func(t *testing.T) {
		actualTrue := Nodes(nodes).Contains(node1)
		actualFalse := Nodes(nodes).Contains(node3)
		if !reflect.DeepEqual(actualTrue, true) {
			t.Errorf("expected: %v, but got: %v", true, actualTrue)
		}
		if !reflect.DeepEqual(actualFalse, false) {
			t.Errorf("expected: %v, but got: %v", false, actualTrue)
		}
	})

	t.Run("Clone", func(t *testing.T) {
		clone := Nodes(nodes).Clone()
		actual := Nodes(clone).URIs()
		expected := []URI{uri0, uri1, uri2}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("expected: %v, but got: %v", expected, actual)
		}
	})
}

// NEXT: move this test to internal and unexport IsCoordinator
func TestCluster_Coordinator(t *testing.T) {
	uri1 := NewTestURIFromHostPort("node1", 0)
	uri2 := NewTestURIFromHostPort("node2", 0)

	node1 := &Node{ID: "node1", URI: uri1}
	node2 := &Node{ID: "node2", URI: uri2}

	c1 := *NewCluster()
	c1.Node = node1
	c1.Coordinator = node1.ID
	c2 := *NewCluster()
	c2.Node = node2
	c2.Coordinator = node1.ID

	t.Run("IsCoordinator", func(t *testing.T) {
		if !c1.isCoordinator() {
			t.Errorf("!IsCoordinator error: %v", c1.Node)
		} else if c2.isCoordinator() {
			t.Errorf("IsCoordinator error: %v", c2.Node)
		}
	})
}

func TestCluster_Topology(t *testing.T) {
	c1 := NewTestCluster(1) // automatically creates Node{ID: "node0"}

	uri0 := NewTestURIFromHostPort("host0", 0)
	uri1 := NewTestURIFromHostPort("host1", 0)
	uri2 := NewTestURIFromHostPort("host2", 0)
	invalid := NewTestURIFromHostPort("invalid", 0)

	node0 := &Node{ID: "node0", URI: uri0}
	node1 := &Node{ID: "node1", URI: uri1}
	node2 := &Node{ID: "node2", URI: uri2}
	nodeinvalid := &Node{ID: "nodeinvalid", URI: invalid}

	t.Run("AddNode", func(t *testing.T) {
		err := c1.addNode(node1)
		if err != nil {
			t.Fatal(err)
		}
		// add the same host.
		err = c1.addNode(node1)
		if err != nil {
			t.Fatal(err)
		}
		err = c1.addNode(node2)
		if err != nil {
			t.Fatal(err)
		}

		actual := c1.nodeIDs()
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
		tc := NewClusterCluster(1)

		// Open TestCluster.
		if err := tc.Open(); err != nil {
			t.Fatal(err)
		}

		node := tc.Clusters[0]

		// Ensure that node comes up in state NORMAL.
		if node.State() != ClusterStateNormal {
			t.Errorf("expected state: %v, but got: %v", ClusterStateNormal, node.State())
		}

		expectedTop := &Topology{
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
		tc := NewClusterCluster(0)
		tc.AddNode(false)

		node := tc.Clusters[0]

		// write topology to data file
		top := &Topology{
			NodeIDs: []string{node.Node.ID},
		}
		tc.WriteTopology(node.Path, top)

		// Open TestCluster.
		if err := tc.Open(); err != nil {
			t.Fatal(err)
		}

		// Ensure that node comes up in state NORMAL.
		if node.State() != ClusterStateNormal {
			t.Errorf("expected state: %v, but got: %v", ClusterStateNormal, node.State())
		}

		// Close TestCluster.
		if err := tc.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Single node, not in topology", func(t *testing.T) {
		tc := NewClusterCluster(0)
		tc.AddNode(false)

		node := tc.Clusters[0]

		// write topology to data file
		top := &Topology{
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
		tc := NewClusterCluster(0)
		tc.AddNode(false)

		// Open TestCluster.
		if err := tc.Open(); err != nil {
			t.Fatal(err)
		}

		tc.AddNode(false)

		node0 := tc.Clusters[0]
		node1 := tc.Clusters[1]

		// Ensure that nodes comes up in state NORMAL.
		if node0.State() != ClusterStateNormal {
			t.Errorf("expected node0 state: %v, but got: %v", ClusterStateNormal, node0.State())
		} else if node1.State() != ClusterStateNormal {
			t.Errorf("expected node1 state: %v, but got: %v", ClusterStateNormal, node1.State())
		}

		expectedTop := &Topology{
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
		tc := NewClusterCluster(0)
		tc.AddNode(false)
		node0 := tc.Clusters[0]

		// write topology to data file
		top := &Topology{
			NodeIDs: []string{"node0", "node2"},
		}
		tc.WriteTopology(node0.Path, top)

		// Open TestCluster.
		if err := tc.Open(); err != nil {
			t.Fatal(err)
		}

		// Ensure that node is in state STARTING before the other node joins.
		if node0.State() != ClusterStateStarting {
			t.Errorf("expected node0 state: %v, but got: %v", ClusterStateStarting, node0.State())
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
		if node0.State() != ClusterStateNormal {
			t.Errorf("expected node0 state: %v, but got: %v", ClusterStateNormal, node0.State())
		} else if node2.State() != ClusterStateNormal {
			t.Errorf("expected node1 state: %v, but got: %v", ClusterStateNormal, node2.State())
		}

		// Close TestCluster.
		if err := tc.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Multiple nodes, with data", func(t *testing.T) {
		tc := NewClusterCluster(0)
		tc.AddNode(false)
		node0 := tc.Clusters[0]

		// Open TestCluster.
		if err := tc.Open(); err != nil {
			t.Fatal(err)
		}

		// Add Bit Data to node0.
		if err := tc.CreateField("i", "f", FieldOptions{}); err != nil {
			t.Fatal(err)
		}
		tc.SetBit("i", "f", "standard", 1, 101, nil)
		tc.SetBit("i", "f", "standard", 1, 1300000, nil)

		// Before starting the resize, get the CheckSum to use for
		// comparison later.
		node0Field := node0.Holder.Field("i", "f")
		node0View := node0Field.View("standard")
		node0Fragment := node0View.Fragment(1)
		node0Checksum := node0Fragment.Checksum()

		// AddNode needs to block until the resize process has completed.
		tc.AddNode(false)
		node1 := tc.Clusters[1]

		// Ensure that nodes come up in state NORMAL.
		if node0.State() != ClusterStateNormal {
			t.Errorf("expected node0 state: %v, but got: %v", ClusterStateNormal, node0.State())
		} else if node1.State() != ClusterStateNormal {
			t.Errorf("expected node1 state: %v, but got: %v", ClusterStateNormal, node1.State())
		}

		expectedTop := &Topology{
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
		node1Field := node1.Holder.Field("i", "f")
		node1View := node1Field.View("standard")
		node1Fragment := node1View.Fragment(1)

		// Ensure checksums are the same.
		if chksum := node1Fragment.Checksum(); !bytes.Equal(chksum, node0Checksum) {
			t.Fatalf("expected standard view checksum to match: %x - %x", chksum, node0Checksum)
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
		c := NewTestCluster(2)

		oldNode := c.Nodes[0]
		newNode := c.Nodes[1]

		// Update coordinator to the same value.
		if c.updateCoordinator(oldNode) {
			t.Errorf("did not expect coordinator to change")
		} else if c.Coordinator != oldNode.ID {
			t.Errorf("expected coordinator: %s, but got: %s", c.Coordinator, oldNode.URI)
		}

		// Update coordinator to a new value.
		if !c.updateCoordinator(newNode) {
			t.Errorf("expected coordinator to change")
		} else if c.Coordinator != newNode.ID {
			t.Errorf("expected coordinator: %s, but got: %s", c.Coordinator, newNode.URI)
		}
	})
}
