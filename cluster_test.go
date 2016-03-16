package pilosa_test

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"testing/quick"

	"github.com/davecgh/go-spew/spew"
	"github.com/umbel/pilosa"
)

// Ensure the cluster can fairly distribute partitions across the nodes.
func TestCluster_Owners(t *testing.T) {
	c := pilosa.Cluster{
		Nodes: []*pilosa.Node{
			{Host: "serverA:1000"},
			{Host: "serverB:1000"},
			{Host: "serverC:1000"},
		},
		Hasher:   NewModHasher(),
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
	if err := quick.Check(func(slice uint64, partitionN int) bool {
		c := pilosa.NewCluster()
		c.PartitionN = partitionN

		partitionID := c.Partition(slice)
		if partitionID < 0 || partitionID > partitionN {
			t.Errorf("partition out of range: slice=%d, p=%d, n=%d", slice, partitionID, partitionN)
		}

		return true
	}, &quick.Config{
		Values: func(values []reflect.Value, rand *rand.Rand) {
			values[0] = reflect.ValueOf(uint64(rand.Uint32()))
			values[1] = reflect.ValueOf(rand.Intn(1000) + 1)
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

// NewCluster returns a cluster with n nodes and uses a mod-based hasher.
func NewCluster(n int) *pilosa.Cluster {
	c := pilosa.NewCluster()
	c.ReplicaN = 1
	c.Hasher = NewModHasher()

	for i := 0; i < n; i++ {
		c.Nodes = append(c.Nodes, &pilosa.Node{
			Host: fmt.Sprintf("host%d", i),
		})
	}

	return c
}

// ModHasher represents a simple, mod-based hashing.
type ModHasher struct{}

// NewModHasher returns a new instance of ModHasher with n buckets.
func NewModHasher() *ModHasher { return &ModHasher{} }

func (*ModHasher) Hash(key uint64, n int) int { return int(key) % n }

// ConstHasher represents hash that always returns the same index.
type ConstHasher struct {
	i int
}

// NewConstHasher returns a new instance of ConstHasher that always returns i.
func NewConstHasher(i int) *ConstHasher { return &ConstHasher{i: i} }

func (h *ConstHasher) Hash(key uint64, n int) int { return h.i }
