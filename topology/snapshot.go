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

package topology

import (
	"encoding/binary"
	"hash/fnv"

	"github.com/pilosa/pilosa/v2/roaring"
	"github.com/pilosa/pilosa/v2/shardwidth"
)

const (
	// DefaultPartitionN is the default number of partitions in a cluster.
	DefaultPartitionN = 256

	// ShardWidth is the number of column IDs in a shard. It must be a power of 2 greater than or equal to 16.
	// shardWidthExponent = 20 // set in shardwidthNN.go files
	ShardWidth = 1 << shardwidth.Exponent
)

// ClusterSnapshot is a static representation of a cluster and its nodes. It is
// used to calculate things like partition location and data distribution.
type ClusterSnapshot struct {
	Nodes []*Node

	// Hashing algorithm used to assign partitions to nodes.
	Hasher Hasher

	// The number of partitions in the cluster.
	PartitionN int

	// The number of replicas a partition has.
	ReplicaN int
}

// NewClusterSnapshot returns a new instance of ClusterSnapshot.
func NewClusterSnapshot(noder Noder, hasher Hasher, replicas int) *ClusterSnapshot {
	nodes := noder.Nodes()

	// Make sure replica count doesn't exceed the number of nodes.
	nodeN := len(nodes)
	if replicas > nodeN {
		replicas = nodeN
	} else if replicas == 0 {
		replicas = 1
	}

	return &ClusterSnapshot{
		Nodes:      nodes,
		Hasher:     hasher,
		PartitionN: DefaultPartitionN,
		ReplicaN:   replicas,
	}
}

//////////////////////////////////////////////////////////////////////////////

// ShardToShardPartition returns the shard-partition that the given shard
// belongs to. NOTE: This is DIFFERENT from the key-partition.
func (c *ClusterSnapshot) ShardToShardPartition(index string, shard uint64) int {
	return dedupShardToShardPartition(index, shard, c.PartitionN)
}

// dedupShardToShardParition would ideally be called `shardToShardPartition`, but since
// we can't put this into it's own package yet (see the TODO below about import loops),
// that name conflicts with a function that already exists in the `pilosa` package.
func dedupShardToShardPartition(index string, shard uint64, partitionN int) int {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], shard)

	// Hash the bytes and mod by partition count.
	h := fnv.New64a()
	_, _ = h.Write([]byte(index))
	_, _ = h.Write(buf[:])
	return int(h.Sum64() % uint64(partitionN))
}

// IDToShardPartition returns the shard-partition that an id belongs to.
func (c *ClusterSnapshot) IDToShardPartition(index string, id uint64) int {
	return c.ShardToShardPartition(index, id/ShardWidth)
}

// KeyToKeyPartition returns the key-partition that the given key belongs to.
// NOTE: The key-partition is DIFFERENT from the shard-partition.
func (c *ClusterSnapshot) KeyToKeyPartition(index, key string) int {
	// Hash the bytes and mod by partition count.
	h := fnv.New64a()
	_, _ = h.Write([]byte(index))
	_, _ = h.Write([]byte(key))
	return int(h.Sum64() % uint64(c.PartitionN))
}

// ShardNodes returns a list of nodes that own a shard.
func (c *ClusterSnapshot) ShardNodes(index string, shard uint64) []*Node {
	return c.PartitionNodes(c.ShardToShardPartition(index, shard))
}

// OwnsShard returns true if a host owns a fragment.
func (c *ClusterSnapshot) OwnsShard(nodeID string, index string, shard uint64) bool {
	return Nodes(c.ShardNodes(index, shard)).ContainsID(nodeID)
}

// KeyNodes returns a list of nodes that own a key.
func (c *ClusterSnapshot) KeyNodes(index, key string) []*Node {
	return c.PartitionNodes(c.KeyToKeyPartition(index, key))
}

// PartitionNodes returns a list of nodes that own the given partition.
func (c *ClusterSnapshot) PartitionNodes(partitionID int) []*Node {
	// Determine primary owner node.
	nodeIndex := c.PrimaryNodeIndex(partitionID)
	if nodeIndex < 0 {
		// no nodes anyway
		return nil
	}
	// Collect nodes around the ring.
	nodes := make([]*Node, 0, c.ReplicaN)
	for i := 0; i < c.ReplicaN; i++ {
		nodes = append(nodes, c.Nodes[(nodeIndex+i)%len(c.Nodes)])
	}

	return nodes
}

// PrimaryFieldTranslationNode is the primary node responsible for translating
// field keys. The primary could be any node in the cluster, but we arbitrarily
// define it to be the node responsible for partition 0.
func (c *ClusterSnapshot) PrimaryFieldTranslationNode() *Node {
	// return c.PrimaryPartitionNode(0)
	for _, n := range c.Nodes {
		if n.IsCoordinator {
			return n
		}
	}
	return nil
}

// IsPrimaryFieldTranslationNode returns true if nodeID represents the primary
// node responsible for field translation.
func (c *ClusterSnapshot) IsPrimaryFieldTranslationNode(nodeID string) bool {
	// return c.PrimaryFieldTranslationNode().ID == nodeID
	for i := range c.Nodes {
		if c.Nodes[i].ID == nodeID && c.Nodes[i].IsCoordinator {
			return true
		}
	}
	return false
}

// PrimaryPartitionNode returns the primary node of the given partition.
func (c *ClusterSnapshot) PrimaryPartitionNode(partition int) *Node {
	if nodes := c.PartitionNodes(partition); len(nodes) > 0 {
		return nodes[0]
	}
	return nil
}

// IsPrimary returns true if the given node is the primary for the given
// partition.
func (c *ClusterSnapshot) IsPrimary(nodeID string, partition int) bool {
	primary := c.PrimaryNodeIndex(partition)
	return nodeID == c.Nodes[primary].ID
}

// PrimaryNodeIndex returns the index (position in the cluster) of the primary
// node for the given partition.
func (c *ClusterSnapshot) PrimaryNodeIndex(partition int) int {
	return c.Hasher.Hash(uint64(partition), len(c.Nodes))
}

// NonPrimaryReplicas returns the list of node IDs which are replicas for the
// given partition.
func (c *ClusterSnapshot) NonPrimaryReplicas(partition int) (nonPrimaryReplicas []string) {
	primary := c.PrimaryNodeIndex(partition)
	nodeN := len(c.Nodes)

	// Collect nodes around the ring.
	for i := 1; i < nodeN; i++ {
		node := c.Nodes[(primary+i)%nodeN]
		if i < c.ReplicaN {
			nonPrimaryReplicas = append(nonPrimaryReplicas, node.ID)
		}
	}
	return
}

// ReplicasForPrimary returns the map replicaNodeIDs[nodeID] which will have a
// true value for the primary nodeID, and false for others.
func (c *ClusterSnapshot) ReplicasForPrimary(primary int) (replicaNodeIDs, nonReplicas map[string]bool) {
	if primary < 0 {
		// no nodes anyway
		return
	}
	replicaNodeIDs = make(map[string]bool)
	nonReplicas = make(map[string]bool)

	nodeN := len(c.Nodes)

	// Collect nodes around the ring.
	for i := 0; i < nodeN; i++ {
		node := c.Nodes[(primary+i)%nodeN]
		if i < c.ReplicaN {
			// mark true if primary
			replicaNodeIDs[node.ID] = (i == 0)
		} else {
			nonReplicas[node.ID] = false
		}
	}
	return
}

// ContainsShards is like OwnsShards, but it includes replicas.
func (c *ClusterSnapshot) ContainsShards(index string, availableShards *roaring.Bitmap, node *Node) []uint64 {
	var shards []uint64
	_ = availableShards.ForEach(func(i uint64) error {
		p := c.ShardToShardPartition(index, i)
		// Determine the nodes for partition.
		nodes := c.PartitionNodes(p)
		for _, n := range nodes {
			if n.ID == node.ID {
				shards = append(shards, i)
			}
		}
		return nil
	})
	return shards
}

// TODO: update this comment
// The boltdb key translation stores are partitioned, designated by partitionIDs. These
// are shared between replicas, and one node is the primary for
// replication. So with 4 nodes and 3-way replication, each node has 3/4 of
// the translation stores on it.
func (c *ClusterSnapshot) PrimaryForColKeyTranslation(index, key string) (primary int) {
	partitionID := c.KeyToKeyPartition(index, key)
	return c.PrimaryNodeIndex(partitionID)
}

// TODO: update this comment
// should match cluster.go:1033 cluster.ownsShard(nodeID, index, shard)
// 	return Nodes(c.shardNodes(index, shard)).ContainsID(nodeID)
func (c *ClusterSnapshot) PrimaryForShardReplication(index string, shard uint64) int {
	n := len(c.Nodes)
	if n == 0 {
		return -1
	}
	partition := uint64(dedupShardToShardPartition(index, shard, c.PartitionN))
	nodeIndex := c.Hasher.Hash(partition, n)
	return nodeIndex
}

// PrimaryReplicaNode returns the node listed before the current node in Nodes().
// This is different than "previous node" as the first node always returns nil.
func (c *ClusterSnapshot) PrimaryReplicaNode(nodeID string) *Node {
	pos := c.nodePositionByID(nodeID)
	if pos <= 0 {
		return nil
	}
	return c.Nodes[pos-1]
}

// nodePositionByID returns the position of the node in slice c.Nodes.
func (c *ClusterSnapshot) nodePositionByID(nodeID string) int {
	return NodePositionByID(c.Nodes, nodeID)
}

// NodePositionByID returns the position of the node in slice nodes.
// TODO: this is exported because it's used in noder.go. Because that's the same
// package, it doesn't need to be exported, but ideally we could put this
// snapshot code into its own package. I tried to do that (by putting it into a
// package called `topology`), but that created an import loop. So what we
// really need to do is do a better job of creating sub-packages under pilosa
// (for things like `Noder` and `Nodes`).
func NodePositionByID(nodes []*Node, nodeID string) int {
	for i, n := range nodes {
		if n.ID == nodeID {
			return i
		}
	}
	return -1
}
