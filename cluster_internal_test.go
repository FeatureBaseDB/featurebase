// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package pilosa

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"testing/quick"
	"time"

	"github.com/molecula/featurebase/v3/disco"
	pnet "github.com/molecula/featurebase/v3/net"
	"github.com/molecula/featurebase/v3/roaring"
)

// Ensure the cluster can fairly distribute partitions across the nodes.
func TestCluster_Owners(t *testing.T) {
	c := cluster{
		noder: disco.NewLocalNoder([]*disco.Node{
			{URI: NewTestURIFromHostPort("serverA", 1000)},
			{URI: NewTestURIFromHostPort("serverB", 1000)},
			{URI: NewTestURIFromHostPort("serverC", 1000)},
		}),
		Hasher:   &disco.Jmphasher{},
		ReplicaN: 2,
	}

	cNodes := c.noder.Nodes()

	// Create a snapshot of the cluster to use for node/partition calculations.
	snap := c.NewSnapshot()

	assigned := make(map[int]int)
	for i := 0; i < 256; i++ {
		nodes := snap.PartitionNodes(i)
		for _, node := range nodes {
			for j, n := range cNodes {
				if n == node {
					assigned[j]++
				}
			}
		}
	}
	expected := float64((256.0 * 2) / 3) // each partition is on two nodes, there's three nodes
	for k, v := range assigned {
		ratio := float64(v) / expected
		// Empirically, we expect 167/171/174
		if ratio < 0.97 || ratio > 1.03 {
			t.Fatalf("node %d has %d assigned partitions, expected about %.1f", k, v, expected)
		}
	}
}

// Ensure the partitioner can assign a fragment to a partition.
func TestCluster_Partition(t *testing.T) {
	if err := quick.Check(func(index string, shard uint64, partitionN int) bool {
		c := newCluster()
		c.partitionN = partitionN

		partitionID := disco.ShardToShardPartition(index, shard, partitionN)
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
			hasher := &disco.Jmphasher{}
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
	snap := c.NewSnapshot()

	availableShards := roaring.NewBitmap(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
	nodeCounts := make(map[uint64]int)
	for _, n := range cNodes {
		shards := snap.ContainsShards("test", availableShards, n)
		for _, shard := range shards {
			nodeCounts[shard]++
		}
	}
	for shard, count := range nodeCounts {
		if count != 3 {
			t.Fatalf("shard %d on %d nodes, expected 3", shard, count)
		}
	}
}

func TestCluster_Nodes(t *testing.T) {
	const urisCount = 4
	var uris []pnet.URI
	arbitraryPorts := []int{17384, 17385, 17386, 17387}
	for i := 0; i < urisCount; i++ {
		uris = append(uris, NewTestURIFromHostPort(fmt.Sprintf("node%d", i), uint16(arbitraryPorts[i])))
	}

	node0 := &disco.Node{ID: "node0", URI: uris[0]}
	node1 := &disco.Node{ID: "node1", URI: uris[1]}
	node2 := &disco.Node{ID: "node2", URI: uris[2]}
	node3 := &disco.Node{ID: "node3", URI: uris[3]}

	nodes := []*disco.Node{node0, node1, node2}

	t.Run("NodeIDs", func(t *testing.T) {
		actual := disco.Nodes(nodes).IDs()
		expected := []string{node0.ID, node1.ID, node2.ID}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("expected: %v, but got: %v", expected, actual)
		}
	})

	t.Run("Filter", func(t *testing.T) {
		actual := disco.Nodes(disco.Nodes(nodes).Filter(nodes[1])).URIs()
		expected := []pnet.URI{uris[0], uris[2]}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("expected: %v, but got: %v", expected, actual)
		}
	})

	t.Run("FilterURI", func(t *testing.T) {
		actual := disco.Nodes(disco.Nodes(nodes).FilterURI(uris[1])).URIs()
		expected := []pnet.URI{uris[0], uris[2]}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("expected: %v, but got: %v", expected, actual)
		}
	})

	t.Run("Contains", func(t *testing.T) {
		actualTrue := disco.Nodes(nodes).Contains(node1)
		actualFalse := disco.Nodes(nodes).Contains(node3)
		if !reflect.DeepEqual(actualTrue, true) {
			t.Errorf("expected: %v, but got: %v", true, actualTrue)
		}
		if !reflect.DeepEqual(actualFalse, false) {
			t.Errorf("expected: %v, but got: %v", false, actualTrue)
		}
	})

	t.Run("Clone", func(t *testing.T) {
		clone := disco.Nodes(nodes).Clone()
		actual := disco.Nodes(clone).URIs()
		expected := []pnet.URI{uris[0], uris[1], uris[2]}
		if !reflect.DeepEqual(actual, expected) {
			t.Errorf("expected: %v, but got: %v", expected, actual)
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
