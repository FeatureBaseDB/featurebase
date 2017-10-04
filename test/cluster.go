package test

import (
	"fmt"

	"github.com/pilosa/pilosa"
)

// NewCluster returns a cluster with n nodes and uses a mod-based hasher.
func NewCluster(n int) *pilosa.Cluster {
	c := pilosa.NewCluster()
	c.ReplicaN = 1
	c.Hasher = NewModHasher()

	for i := 0; i < n; i++ {
		c.Nodes = append(c.Nodes, &pilosa.Node{
			Scheme: "http",
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
