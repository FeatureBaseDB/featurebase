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

func TestCluster_Nodes(t *testing.T) {
	uri0 := test.NewURIFromHostPort("node0", 0)
	uri1 := test.NewURIFromHostPort("node1", 0)
	uri2 := test.NewURIFromHostPort("node2", 0)
	uri3 := test.NewURIFromHostPort("node3", 0)

	nodes := []*pilosa.Node{
		{URI: uri0},
		{URI: uri1},
		{URI: uri2},
	}

	t.Run("URISet", func(t *testing.T) {
		actual := pilosa.Nodes(nodes).URIs()
		expected := []pilosa.URI{uri0, uri1, uri2}
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
		actualTrue := pilosa.Nodes(nodes).Contains(nodes[1])
		actualFalse := pilosa.Nodes(nodes).Contains(&pilosa.Node{})
		if !reflect.DeepEqual(actualTrue, true) {
			t.Errorf("expected: %v, but got: %v", true, actualTrue)
		}
		if !reflect.DeepEqual(actualFalse, false) {
			t.Errorf("expected: %v, but got: %v", false, actualTrue)
		}
	})

	t.Run("ContainsURI", func(t *testing.T) {
		actualTrue := pilosa.Nodes(nodes).ContainsURI(uri1)
		actualFalse := pilosa.Nodes(nodes).ContainsURI(uri3)
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

	c1 := *pilosa.NewCluster()
	c1.URI = uri1
	c1.Coordinator = uri1
	c2 := *pilosa.NewCluster()
	c2.URI = uri2
	c2.Coordinator = uri1

	t.Run("IsCoordinator", func(t *testing.T) {
		if !c1.IsCoordinator() {
			t.Errorf("!IsCoordinator error: %v", c1.URI)
		} else if c2.IsCoordinator() {
			t.Errorf("IsCoordinator error: %v", c2.URI)
		}
	})
}

func TestCluster_Topology(t *testing.T) {
	c1 := test.NewCluster(1)

	uri1 := test.NewURIFromHostPort("node1", 0)
	uri2 := test.NewURIFromHostPort("node2", 0)
	base := test.NewURIFromHostPort("host0", 0)
	invalid := test.NewURIFromHostPort("invalid", 0)

	t.Run("AddHost", func(t *testing.T) {
		err := c1.AddHost(uri1)
		if err != nil {
			t.Fatal(err)
		}
		// add the same host.
		err = c1.AddHost(uri1)
		if err != nil {
			t.Fatal(err)
		}
		err = c1.AddHost(uri2)
		if err != nil {
			t.Fatal(err)
		}

		actual := pilosa.Nodes(c1.Nodes).URIs()
		expected := []pilosa.URI{base, uri1, uri2}

		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("expected: %v, but got: %v", expected, actual)
		}
	})

	t.Run("ContainsURI", func(t *testing.T) {
		if !c1.Topology.ContainsURI(uri1) {
			t.Errorf("!ContainsHost error: %v", uri1)
		} else if c1.Topology.ContainsURI(invalid) {
			t.Errorf("ContainsHost error: %v", invalid)
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

		u0 := test.NewURIFromHostPort("host0", 0)
		u1 := test.NewURIFromHostPort("host1", 0)
		u2 := test.NewURIFromHostPort("host2", 0)
		u3 := test.NewURIFromHostPort("host3", 0)

		uri0 := u0.Encode()
		uri1 := u1.Encode()
		uri2 := u2.Encode()
		//uri3 := u3.Encode()

		expected := map[pilosa.URI][]*internal.ResizeSource{
			u0: []*internal.ResizeSource{
				{URI: uri1, Index: "i", Frame: "f", View: "inverse", Slice: 5},
			},
			u1: []*internal.ResizeSource{
				{URI: uri2, Index: "i", Frame: "f", View: "v", Slice: 0},
				{URI: uri2, Index: "i", Frame: "f", View: "inverse", Slice: 0},
			},
			u2: []*internal.ResizeSource{
				{URI: uri0, Index: "i", Frame: "f", View: "inverse", Slice: 3},
			},
			u3: []*internal.ResizeSource{
				{URI: uri0, Index: "i", Frame: "f", View: "v", Slice: 1},
				{URI: uri1, Index: "i", Frame: "f", View: "v", Slice: 2},
				{URI: uri0, Index: "i", Frame: "f", View: "inverse", Slice: 1},
				{URI: uri1, Index: "i", Frame: "f", View: "inverse", Slice: 2},
				{URI: uri1, Index: "i", Frame: "f", View: "inverse", Slice: 5},
			},
		}

		actual := c1.DataDiff(c2, i)
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("expected: %v, but got: %v", expected, actual)
		}
	})
}
