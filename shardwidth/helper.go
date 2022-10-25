// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package shardwidth

import (
	"math/bits"
)

// Exponent controls the size of each shard
//
// # Warnings
// - changing this value WILL corrupt any data sets created with a different value **
// - both server and client must be compiled with the same Exponent **
const Exponent = 20

// FindNextShard returns the index of the first item which is not in the
// same shard as i. The index it returns may be equal to the length of the
// haystack, indicatincg that the rest of the list is in the same shard.
func FindNextShard(i int, haystack []uint64) int {
	// compute the last thing that's in the same shard as haystack[i].
	if i >= len(haystack) {
		return i
	}
	// current shard:
	shard := (haystack[i] >> Exponent)
	// last value in shard:
	shardEnd := ((shard + 1) << Exponent) - 1
	j := i
	// We want to do a binary search of the haystack. For any length of
	// haystack, its topmost bit gives us a reasonable halfway point; it may
	// not actually be halfway, but the number of steps it'll take to search
	// it will be the same as if it were. sort.Search has interface overhead
	// and makes us sad.
	for incr := 1 << (bits.Len64(uint64(len(haystack) - i))); incr > 0; incr >>= 1 {
		if j+incr < len(haystack) {
			if haystack[j+incr] <= shardEnd {
				j += incr
			}
		}
	}
	// we've found the last item that is in the same shard as i, so...
	return j + 1
}

// FindShards finds the shards in a given haystack
func FindShards(haystack []uint64) (shards []uint64, endIndexes []int) {
	if len(haystack) == 0 {
		return nil, nil
	}
	index := 0
	// the steady state of this loop is that shards contains the current
	// shard, but not its ending index; each time we find a new ending
	// index, we record that index as the end for the current shard, and
	// the new shard, until we reach the end and append len(haystack)
	// as the last index.
	shards = []uint64{haystack[index] >> Exponent}
	index = FindNextShard(index, haystack)
	for index < len(haystack) {
		shards = append(shards, haystack[index]>>Exponent)
		endIndexes = append(endIndexes, index)
		index = FindNextShard(index, haystack)
	}
	endIndexes = append(endIndexes, index)
	return shards, endIndexes
}
