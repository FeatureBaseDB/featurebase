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

// Ensure that an empty cluster returns a valid (empty) NodeSet
func TestCluster_NodeSetHosts(t *testing.T) {

	c := pilosa.Cluster{}

	if h := c.NodeSetHosts(); !reflect.DeepEqual(h, []string{}) {
		t.Fatalf("unexpected slice of hosts: %s", h)
	}
}

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

// Ensure OwnsSlices can find the actual slice list for node and index
func TestCluster_OwnsSlices(t *testing.T) {
	c := test.NewCluster(5)
	slices := c.OwnsSlices("test", 10, "host2")

	if !reflect.DeepEqual(slices, []uint64{0, 3, 6, 10}) {
		t.Fatalf("unexpected slices for node's index: %v", slices)
	}
}
