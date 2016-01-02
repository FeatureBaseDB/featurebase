package pilosa

import (
	"encoding/binary"
	"hash/fnv"
)

const (
	// DefaultPartitionN is the default number of partitions in a cluster.
	DefaultPartitionN = 64

	// DefaultReplicaN is the default number of replicas per partition.
	DefaultReplicaN = 1
)

// Node represents a node in the cluster.
type Node struct {
	Host string
}

// Nodes represents a list of nodes.
type Nodes []*Node

// ContainsHost returns true if host matches on of the node's host.
func (a Nodes) ContainsHost(host string) bool {
	for _, n := range a {
		if n.Host == host {
			return true
		}
	}
	return false
}

// Hosts returns a list of all hostnames.
func (a Nodes) Hosts() []string {
	hosts := make([]string, len(a))
	for i, n := range a {
		hosts[i] = n.Host
	}
	return hosts
}

// Cluster represents a collection of nodes.
type Cluster struct {
	Nodes []*Node

	// Hashing algorithm used to assign partitions to nodes.
	Hasher Hasher

	// The number of partitions in the cluster.
	PartitionN int

	// The number of replicas a partition has.
	ReplicaN int
}

// NewCluster returns a new instance of Cluster with defaults.
func NewCluster() *Cluster {
	return &Cluster{
		Hasher:     &jmphasher{},
		PartitionN: DefaultPartitionN,
		ReplicaN:   DefaultReplicaN,
	}
}

// Partition returns the partition that a slice belongs to.
func (c *Cluster) Partition(slice uint64) int {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], slice)

	// Hash the bytes and mod by partition count.
	h := fnv.New64a()
	h.Write(buf[:])
	return int(h.Sum64() % uint64(c.PartitionN))
}

// SliceNodes returns a list of nodes that own a slice.
func (c *Cluster) SliceNodes(slice uint64) []*Node {
	return c.PartitionNodes(c.Partition(slice))
}

// OwnsSlice returns true if a host owns slice.
func (c *Cluster) OwnsSlice(host string, slice uint64) bool {
	return Nodes(c.SliceNodes(slice)).ContainsHost(host)
}

// PartitionNodes returns a list of nodes that own a partition.
func (c *Cluster) PartitionNodes(partitionID int) []*Node {
	// Default replica count to between one and the number of nodes.
	// The replica count can be zero if there are no nodes.
	replicaN := c.ReplicaN
	if replicaN > len(c.Nodes) {
		replicaN = len(c.Nodes)
	} else if replicaN == 0 {
		replicaN = 1
	}

	// Determine primary owner node.
	index := c.Hasher.Hash(uint64(partitionID), len(c.Nodes))

	// Collect nodes around the ring.
	nodes := make([]*Node, replicaN)
	for i := 0; i < replicaN; i++ {
		nodes[i] = c.Nodes[(index+i)%len(c.Nodes)]
	}

	return nodes
}

// Hasher represents an interface to hash integers into buckets.
type Hasher interface {
	// Hashes the key into a number between [0,N).
	Hash(key uint64, n int) int
}

// jmphasher represents an implementation of jmphash. Implements Hasher.
type jmphasher struct{}

// Hash returns the integer hash for the given key.
func (h *jmphasher) Hash(key uint64, n int) int {
	b, j := int64(-1), int64(0)
	for j < int64(n) {
		b = j
		key = key*uint64(2862933555777941757) + 1
		j = int64(float64(b+1) * (float64(int64(1)<<31) / float64((key>>33)+1)))
	}
	return int(b)
}
