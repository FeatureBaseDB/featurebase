// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package disco

// Hasher represents an interface to hash integers into buckets.
type Hasher interface {
	// Hashes the key into a number between [0,N).
	Hash(key uint64, n int) int
	Name() string
}

// Jmphasher represents an implementation of jmphash. Implements Hasher.
type Jmphasher struct{}

// Hash returns the integer hash for the given key.
func (h *Jmphasher) Hash(key uint64, n int) int {
	b, j := int64(-1), int64(0)
	for j < int64(n) {
		b = j
		key = key*uint64(2862933555777941757) + 1
		j = int64(float64(b+1) * (float64(int64(1)<<31) / float64((key>>33)+1)))
	}
	return int(b)
}

// Name returns the name of this hash.
func (h *Jmphasher) Name() string {
	return "jump-hash"
}

// PrimaryNode yields the node that would be selected as the primary from
// a list, for a given ID. It assumes the list is already in the
// expected order, as from Noder.Nodes().
func PrimaryNode(nodes []*Node, hasher Hasher) *Node {
	if len(nodes) == 0 {
		return nil
	}
	return nodes[hasher.Hash(0, len(nodes))]
}
