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
	"fmt"
	"math/rand"
	"net"
	"reflect"
	"strings"
	"testing"
	"testing/quick"
	"time"

	"github.com/davecgh/go-spew/spew"
	pnet "github.com/molecula/featurebase/v2/net"
	"github.com/molecula/featurebase/v2/roaring"
	"github.com/molecula/featurebase/v2/testhook"
	"github.com/molecula/featurebase/v2/topology"
	. "github.com/molecula/featurebase/v2/vprint" // nolint:staticcheck
	"github.com/pkg/errors"
)

// GlobalPortMap avoids many races and port conflicts when setting
// up ports for test clusters. Used for tests only.
var globalPortMap *GlobalPortMapper

func init() {
	globalPortMap = NewGlobalPortMapper(300)
}

// GlobalPortMapper maintains a pool of available ports by
// holding them open until GetPort() is called.
type GlobalPortMapper struct {
	availPorts map[int]net.Listener
}

// reserve n ports
func NewGlobalPortMapper(n int) (pm *GlobalPortMapper) {

	pm = &GlobalPortMapper{
		availPorts: make(map[int]net.Listener),
	}
	for i := 0; i < n; i++ {
		lsn, err := net.Listen("tcp", ":0")
		if err != nil {
			panic(errors.Wrap(err, "trying to listen on ephemeral port"))
		}
		r := lsn.Addr()
		port := r.(*net.TCPAddr).Port
		pm.availPorts[port] = lsn
	}
	return
}

func (pm *GlobalPortMapper) GetPort() (port int, err error) {
	for port, lsn := range pm.availPorts {
		lsn.Close()
		return port, nil
	}
	return -1, fmt.Errorf("no more ports available")
}

func (pm *GlobalPortMapper) MustGetPort() int {
	port, err := pm.GetPort()
	if err != nil {
		panic(err)
	}
	return port
}

// Ensure that fragCombos creates the correct fragment mapping.
func TestFragCombos(t *testing.T) {
	uri0, err := pnet.NewURIFromAddress("host0")
	if err != nil {
		t.Fatal(err)
	}
	uri1, err := pnet.NewURIFromAddress("host1")
	if err != nil {
		t.Fatal(err)
	}

	node0 := &topology.Node{ID: "node0", URI: *uri0}
	node1 := &topology.Node{ID: "node1", URI: *uri1}

	c := newCluster()
	c.addNodeBasicSorted(node0)
	c.addNodeBasicSorted(node1)

	tests := []struct {
		idx             string
		availableShards *roaring.Bitmap
		fieldViews      viewsByField
		expected        fragsByHost
	}{
		{
			idx:             "i",
			availableShards: roaring.NewBitmap(0, 1, 2),
			fieldViews:      viewsByField{"f": []string{"v1", "v2"}},
			expected: fragsByHost{
				"node0": []frag{{"f", "v1", uint64(0)}, {"f", "v2", uint64(0)}},
				"node1": []frag{{"f", "v1", uint64(1)}, {"f", "v2", uint64(1)}, {"f", "v1", uint64(2)}, {"f", "v2", uint64(2)}},
			},
		},
		{
			idx:             "foo",
			availableShards: roaring.NewBitmap(0, 1, 2, 3),
			fieldViews:      viewsByField{"f": []string{"v0"}},
			expected: fragsByHost{
				"node0": []frag{{"f", "v0", uint64(1)}, {"f", "v0", uint64(2)}},
				"node1": []frag{{"f", "v0", uint64(0)}, {"f", "v0", uint64(3)}},
			},
		},
	}
	for _, test := range tests {
		actual := c.fragCombos(test.idx, test.availableShards, test.fieldViews)
		if !reflect.DeepEqual(actual, test.expected) {
			t.Errorf("expected: %v, but got: %v", test.expected, actual)
		}

	}
}

// newHolderWithTempPath returns a new instance of Holder.
func newHolderWithTempPath(tb testing.TB, backend string) *Holder {
	path, err := testhook.TempDirInDir(tb, *TempDir, "pilosa-holder-")
	if err != nil {
		panic(err)
	}
	cfg := mustHolderConfig()
	cfg.StorageConfig.Backend = backend
	h := NewHolder(path, cfg)
	PanicOn(h.Open())
	testhook.Cleanup(tb, func() {
		h.Close()
	})
	return h
}

// newIndexWithTempPath returns a new instance of Index.
func newIndexWithTempPath(tb testing.TB, name string) *Index {
	path, err := testhook.TempDirInDir(tb, *TempDir, "pilosa-index-")
	if err != nil {
		panic(err)
	}
	cfg := DefaultHolderConfig()
	cfg.StorageConfig.FsyncEnabled = false
	cfg.RBFConfig.FsyncEnabled = false
	h := NewHolder(path, cfg)
	PanicOn(h.Open())
	index, err := h.CreateIndex(name, IndexOptions{})
	testhook.Cleanup(tb, func() {
		h.Close()
	})
	if err != nil {
		panic(err)
	}
	return index
}

// Ensure that fragSources creates the correct fragment mapping.
func TestFragSources(t *testing.T) {
	uri0, err := pnet.NewURIFromAddress("host0")
	if err != nil {
		t.Fatal(err)
	}
	uri1, err := pnet.NewURIFromAddress("host1")
	if err != nil {
		t.Fatal(err)
	}
	uri2, err := pnet.NewURIFromAddress("host2")
	if err != nil {
		t.Fatal(err)
	}
	uri3, err := pnet.NewURIFromAddress("host3")
	if err != nil {
		t.Fatal(err)
	}

	node0 := &topology.Node{ID: "node0", URI: *uri0}
	node1 := &topology.Node{ID: "node1", URI: *uri1}
	node2 := &topology.Node{ID: "node2", URI: *uri2}
	node3 := &topology.Node{ID: "node3", URI: *uri3}

	c1 := newCluster()
	c1.ReplicaN = 1
	c1.addNodeBasicSorted(node0)
	c1.addNodeBasicSorted(node1)

	c2 := newCluster()
	c2.ReplicaN = 1
	c2.addNodeBasicSorted(node0)
	c2.addNodeBasicSorted(node1)
	c2.addNodeBasicSorted(node2)

	c3 := newCluster()
	c3.ReplicaN = 2
	c3.addNodeBasicSorted(node0)
	c3.addNodeBasicSorted(node1)

	c4 := newCluster()
	c4.ReplicaN = 2
	c4.addNodeBasicSorted(node0)
	c4.addNodeBasicSorted(node1)
	c4.addNodeBasicSorted(node2)

	c5 := newCluster()
	c5.ReplicaN = 2
	c5.addNodeBasicSorted(node0)
	c5.addNodeBasicSorted(node1)
	c5.addNodeBasicSorted(node2)
	c5.addNodeBasicSorted(node3)

	idx := newIndexWithTempPath(t, "i")
	defer idx.Close()

	field, err := idx.CreateFieldIfNotExists("f", OptFieldTypeDefault())
	if err != nil {
		t.Fatal(err)
	}

	// Obtain transaction.
	var shard uint64
	tx := idx.holder.txf.NewTx(Txo{Write: writable, Index: idx, Shard: shard})
	defer tx.Rollback()

	_, err = field.SetBit(tx, 1, 101, nil)
	if err != nil {
		t.Fatal(err)
	}
	PanicOn(tx.Commit())

	shard = 1
	tx = idx.holder.txf.NewTx(Txo{Write: writable, Index: idx, Shard: shard})
	defer tx.Rollback()
	_, err = field.SetBit(tx, 1, ShardWidth*shard+1, nil)
	if err != nil {
		t.Fatal(err)
	}
	PanicOn(tx.Commit())

	shard = 2
	tx = idx.holder.txf.NewTx(Txo{Write: writable, Index: idx, Shard: shard})
	defer tx.Rollback()

	_, err = field.SetBit(tx, 1, ShardWidth*shard+1, nil)
	if err != nil {
		t.Fatal(err)
	}
	PanicOn(tx.Commit())

	shard = 3
	tx = idx.holder.txf.NewTx(Txo{Write: writable, Index: idx, Shard: shard})
	defer tx.Rollback()

	_, err = field.SetBit(tx, 1, ShardWidth*shard+1, nil)
	if err != nil {
		t.Fatal(err)
	}
	PanicOn(tx.Commit())

	tests := []struct {
		from     *cluster
		to       *cluster
		idx      *Index
		expected map[string][]*ResizeSource
		err      string
	}{
		{
			from: c1,
			to:   c2,
			idx:  idx,
			expected: map[string][]*ResizeSource{
				"node0": {},
				"node1": {},
				"node2": {
					{&topology.Node{ID: "node0", URI: pnet.URI{Scheme: "http", Host: "host0", Port: 10101}, IsPrimary: false}, "i", "f", "standard", uint64(0)},
					{&topology.Node{ID: "node1", URI: pnet.URI{Scheme: "http", Host: "host1", Port: 10101}, IsPrimary: false}, "i", "f", "standard", uint64(2)},
				},
			},
			err: "",
		},
		{
			from: c4,
			to:   c3,
			idx:  idx,
			expected: map[string][]*ResizeSource{
				"node0": {
					{&topology.Node{ID: "node1", URI: pnet.URI{Scheme: "http", Host: "host1", Port: 10101}, IsPrimary: false}, "i", "f", "standard", uint64(1)},
				},
				"node1": {
					{&topology.Node{ID: "node0", URI: pnet.URI{Scheme: "http", Host: "host0", Port: 10101}, IsPrimary: false}, "i", "f", "standard", uint64(0)},
					{&topology.Node{ID: "node0", URI: pnet.URI{Scheme: "http", Host: "host0", Port: 10101}, IsPrimary: false}, "i", "f", "standard", uint64(2)},
				},
			},
			err: "",
		},
		{
			from: c5,
			to:   c4,
			idx:  idx,
			expected: map[string][]*ResizeSource{
				"node0": {
					{&topology.Node{ID: "node2", URI: pnet.URI{Scheme: "http", Host: "host2", Port: 10101}, IsPrimary: false}, "i", "f", "standard", uint64(0)},
					{&topology.Node{ID: "node2", URI: pnet.URI{Scheme: "http", Host: "host2", Port: 10101}, IsPrimary: false}, "i", "f", "standard", uint64(2)},
				},
				"node1": {
					{&topology.Node{ID: "node0", URI: pnet.URI{Scheme: "http", Host: "host0", Port: 10101}, IsPrimary: false}, "i", "f", "standard", uint64(3)},
				},
				"node2": {},
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

	uri0, err := pnet.NewURIFromAddress("host0")
	if err != nil {
		t.Fatal(err)
	}
	uri1, err := pnet.NewURIFromAddress("host1")
	if err != nil {
		t.Fatal(err)
	}
	uri2, err := pnet.NewURIFromAddress("host2")
	if err != nil {
		t.Fatal(err)
	}

	node0 := &topology.Node{ID: "node0", URI: *uri0}
	node1 := &topology.Node{ID: "node1", URI: *uri1}
	node2 := &topology.Node{ID: "node2", URI: *uri2}

	tests := []struct {
		existingNodes []*topology.Node
		node          *topology.Node
		action        string
		expectedIDs   map[string]bool
	}{
		{
			existingNodes: []*topology.Node{node0, node1},
			node:          node2,
			action:        resizeJobActionAdd,
			expectedIDs:   map[string]bool{node0.ID: false, node1.ID: false, node2.ID: false},
		},
		{
			existingNodes: []*topology.Node{node0, node1, node2},
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
	c := cluster{
		noder: topology.NewLocalNoder([]*topology.Node{
			{URI: NewTestURIFromHostPort("serverA", 1000)},
			{URI: NewTestURIFromHostPort("serverB", 1000)},
			{URI: NewTestURIFromHostPort("serverC", 1000)},
		}),
		Hasher:   NewTestModHasher(),
		ReplicaN: 2,
	}

	cNodes := c.noder.Nodes()

	// Create a snapshot of the cluster to use for node/partition calculations.
	snap := topology.NewClusterSnapshot(c.noder, c.Hasher, c.ReplicaN)

	// Verify nodes are distributed.
	if a := snap.PartitionNodes(0); !reflect.DeepEqual(a, []*topology.Node{cNodes[0], cNodes[1]}) {
		t.Fatalf("unexpected owners: %s", spew.Sdump(a))
	}

	// Verify nodes go around the ring.
	if a := snap.PartitionNodes(2); !reflect.DeepEqual(a, []*topology.Node{cNodes[2], cNodes[0]}) {
		t.Fatalf("unexpected owners: %s", spew.Sdump(a))
	}
}

// Ensure the partitioner can assign a fragment to a partition.
func TestCluster_Partition(t *testing.T) {
	if err := quick.Check(func(index string, shard uint64, partitionN int) bool {
		c := newCluster()
		c.partitionN = partitionN

		partitionID := topology.ShardToShardPartition(index, shard, partitionN)
		if partitionID < 0 || partitionID >= partitionN {
			t.Errorf("partition out of range: shard=%d, p=%d, n=%d", shard, partitionID, partitionN)
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
			hasher := &topology.Jmphasher{}
			if got := hasher.Hash(tt.key, i+1); got != v {
				t.Errorf("hash(%v,%v)=%v, want %v", tt.key, i+1, got, v)
			}
		}
	}
}

// Ensure ContainsShards can find the actual shard list for node and index.
func TestCluster_ContainsShards(t *testing.T) {
	c := NewTestCluster(t, 5)
	c.ReplicaN = 3
	cNodes := c.noder.Nodes()

	// Create a snapshot of the cluster to use for node/partition calculations.
	snap := topology.NewClusterSnapshot(c.noder, c.Hasher, c.ReplicaN)

	shards := snap.ContainsShards("test", roaring.NewBitmap(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10), cNodes[2])

	if !reflect.DeepEqual(shards, []uint64{0, 2, 3, 5, 6, 9, 10}) {
		t.Fatalf("unexpected shars for node's index: %v", shards)
	}
}

func TestCluster_Nodes(t *testing.T) {
	const urisCount = 4
	var uris []pnet.URI
	arbitraryPorts := []int{17384, 17385, 17386, 17387}
	for i := 0; i < urisCount; i++ {
		uris = append(uris, NewTestURIFromHostPort(fmt.Sprintf("node%d", i), uint16(arbitraryPorts[i])))
	}

	node0 := &topology.Node{ID: "node0", URI: uris[0]}
	node1 := &topology.Node{ID: "node1", URI: uris[1]}
	node2 := &topology.Node{ID: "node2", URI: uris[2]}
	node3 := &topology.Node{ID: "node3", URI: uris[3]}

	nodes := []*topology.Node{node0, node1, node2}

	t.Run("NodeIDs", func(t *testing.T) {
		actual := topology.Nodes(nodes).IDs()
		expected := []string{node0.ID, node1.ID, node2.ID}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("expected: %v, but got: %v", expected, actual)
		}
	})

	t.Run("Filter", func(t *testing.T) {
		actual := topology.Nodes(topology.Nodes(nodes).Filter(nodes[1])).URIs()
		expected := []pnet.URI{uris[0], uris[2]}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("expected: %v, but got: %v", expected, actual)
		}
	})

	t.Run("FilterURI", func(t *testing.T) {
		actual := topology.Nodes(topology.Nodes(nodes).FilterURI(uris[1])).URIs()
		expected := []pnet.URI{uris[0], uris[2]}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("expected: %v, but got: %v", expected, actual)
		}
	})

	t.Run("Contains", func(t *testing.T) {
		actualTrue := topology.Nodes(nodes).Contains(node1)
		actualFalse := topology.Nodes(nodes).Contains(node3)
		if !reflect.DeepEqual(actualTrue, true) {
			t.Errorf("expected: %v, but got: %v", true, actualTrue)
		}
		if !reflect.DeepEqual(actualFalse, false) {
			t.Errorf("expected: %v, but got: %v", false, actualTrue)
		}
	})

	t.Run("Clone", func(t *testing.T) {
		clone := topology.Nodes(nodes).Clone()
		actual := topology.Nodes(clone).URIs()
		expected := []pnet.URI{uris[0], uris[1], uris[2]}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("expected: %v, but got: %v", expected, actual)
		}
	})
}

func TestCluster_PreviousNode(t *testing.T) {
	node0 := &topology.Node{ID: "node0"}
	node1 := &topology.Node{ID: "node1"}
	node2 := &topology.Node{ID: "node2"}

	t.Run("OneNode", func(t *testing.T) {
		c := newCluster()
		c.addNodeBasicSorted(node0)

		c.Node = node0
		if prev := c.unprotectedPreviousNode(); prev != nil {
			t.Errorf("expected: nil, but got: %v", prev)
		}
	})

	t.Run("TwoNode", func(t *testing.T) {
		c := newCluster()
		c.addNodeBasicSorted(node0)
		c.addNodeBasicSorted(node1)

		c.Node = node0
		if prev := c.unprotectedPreviousNode(); prev != node1 {
			t.Errorf("expected: node1, but got: %v", prev)
		}

		c.Node = node1
		if prev := c.unprotectedPreviousNode(); prev != node0 {
			t.Errorf("expected: node0, but got: %v", prev)
		}
	})

	t.Run("ThreeNode", func(t *testing.T) {
		c := newCluster()
		c.addNodeBasicSorted(node0)
		c.addNodeBasicSorted(node1)
		c.addNodeBasicSorted(node2)

		c.Node = node0
		if prev := c.unprotectedPreviousNode(); prev != node2 {
			t.Errorf("expected: node2, but got: %v", prev)
		}

		c.Node = node1
		if prev := c.unprotectedPreviousNode(); prev != node0 {
			t.Errorf("expected: node0, but got: %v", prev)
		}

		c.Node = node2
		if prev := c.unprotectedPreviousNode(); prev != node1 {
			t.Errorf("expected: node1, but got: %v", prev)
		}
	})
}

func TestAE(t *testing.T) {
	t.Run("AbortDoesn'tBlockUninitialized", func(t *testing.T) {
		c := newCluster()
		ch := make(chan struct{})
		go func() {
			c.abortAntiEntropy()
			close(ch)
		}()
		defer c.abortAntiEntropyQ() // avoid leaking a goroutine.
		select {
		case <-ch:
			return
		case <-time.After(time.Second):
			t.Fatalf("aborting anti entropy on a new cluster blocked")
		}
	})

	t.Run("AbortBlocksInitialized", func(t *testing.T) {
		c := newCluster()
		c.initializeAntiEntropy()

		ch := make(chan struct{})
		go func() {
			c.abortAntiEntropy()
			close(ch)
		}()
		defer c.abortAntiEntropyQ() // avoid leak of goroutine.
		select {
		case <-ch:
			t.Fatalf("aborting anti entropy on an initialized cluster didn't block")
		case <-time.After(time.Microsecond * 100):
		}
	})

	t.Run("AbortAntiEntropyQ", func(t *testing.T) {
		c := newCluster()
		c.initializeAntiEntropy()
		if c.abortAntiEntropyQ() {
			t.Fatalf("abortAntiEntropyQ should report false when abort not called")
		}
		go func() {
			for {
				if c.abortAntiEntropyQ() {
					break
				}
			}
		}()
		ch := make(chan struct{})
		go func() {
			c.abortAntiEntropy()
			close(ch)
		}()
		select {
		case <-ch:
		case <-time.After(time.Second):
			t.Fatalf("abort should not have blocked this long")
		}
	})
}
