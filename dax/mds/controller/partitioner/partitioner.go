// Package partitioner provides the Partitioner type, which provides helper
// methods for determining partitions based on string keys.
package partitioner

import (
	"encoding/binary"
	"hash/fnv"

	"github.com/featurebasedb/featurebase/v3/dax"
)

// Partitioner encapsulates helper methods for determining partitions
type Partitioner struct{}

// NewPartitioner returns a new instance of Partitioner with default values.
func NewPartitioner() *Partitioner {
	return &Partitioner{}
}

// PartitionsForKeys returns a map of partitions to the list of strings which
// fall into that partition.
func (p *Partitioner) PartitionsForKeys(tkey dax.TableKey, partitionN int, keys ...string) map[dax.PartitionNum][]string {
	out := make(map[dax.PartitionNum][]string)

	for _, key := range keys {
		p := keyToPartition(tkey, partitionN, key)
		if _, found := out[p]; !found {
			out[p] = []string{}
		}
		out[p] = append(out[p], key)
	}

	return out
}

// keyToPartition returns the partition to which the given key belongs.
func keyToPartition(tkey dax.TableKey, partitionN int, key string) dax.PartitionNum {
	// Hash the bytes and mod by partition count.
	h := fnv.New64a()
	_, _ = h.Write([]byte(tkey))
	_, _ = h.Write([]byte(key))
	return dax.PartitionNum(h.Sum64() % uint64(partitionN))
}

// ShardToPartition returns the PartitionNum for the given shard.
func (p *Partitioner) ShardToPartition(tkey dax.TableKey, shard dax.ShardNum, partitionN int) dax.PartitionNum {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(shard))

	// Hash the bytes and mod by partition count.
	h := fnv.New64a()
	_, _ = h.Write([]byte(tkey))
	_, _ = h.Write(buf[:])
	return dax.PartitionNum(h.Sum64() % uint64(partitionN))
}
