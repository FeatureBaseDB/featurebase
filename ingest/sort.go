// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package ingest

// "math/bits"

// HERE THERE BE DRAGONS

// This is some sorting logic Nia was experimenting with, which we aren't currently
// using, but which beat stdlib sort by a factor-of-several on at least some test
// data, so we aren't deleting it just yet.

// groupIDPairsByShard destructively groups ID pairs by shard.
// The returned slices reference the original pairs slice.
// func groupIDPairsByShard(pairs []IDPair) map[uint64][]IDPair {
// 	if len(pairs) == 0 {
// 		return nil
// 	}
//
// 	// Sort pairs by shard (in-place radix sort).
// 	// This may also change the order of pairs within a shard, but that should not matter.
// 	for {
// 		// Find the highest bit which needs to be sorted.
// 		var diffMask uint64
// 		prev := pairs[0].RecordID
// 		for _, v := range pairs[1:] {
// 			if v.RecordID < prev {
// 				diffMask |= v.RecordID ^ prev
// 			}
//
// 			prev = v.RecordID
// 		}
// 		diffLen := bits.Len64(diffMask)
// 		if diffLen <= shardwidth.Exponent {
// 			// The pairs are sorted by shard.
// 			break
// 		}
//
// 		// Select a right bit shift index such that the highest unsorted bit moves to the 128's place.
// 		shift := uint(diffLen) - 8
//
// 		// Create a mask that can be used to group values by sorted bits.
// 		sortedMask := ^uint64(0) << bits.Len64(diffMask)
//
// 		for i := 0; i < len(pairs); {
// 			// Select a group of pairs to sort.
// 			// While doing so, count the pairs within each bucket.
// 			j := i
// 			var buckets [256]struct {
// 				start, end uint
// 			}
// 			for group := pairs[i].RecordID & sortedMask; i < len(pairs) && pairs[i].RecordID&sortedMask == group; i++ {
// 				buckets[uint8(pairs[i].RecordID>>uint64(shift))].end++
// 			}
// 			group := pairs[j:i]
//
// 			// Assign indices within the group to the buckets.
// 			{
// 				var start uint
// 				for i := range buckets {
// 					bucket := &buckets[i]
// 					bucket.start = start
// 					bucket.end += start
// 					start = bucket.end
// 				}
// 			}
//
// 			// Split the group into the buckets.
// 			for i, b := range buckets {
// 				// There is no need to update the state of the current bucket - we will never reference it again after this.
// 				i := uint8(i)
// 				for j := b.start; j < b.end; j++ {
// 					// This inner loop may run quite a few times for the first few buckets, but no element will be moved more than twice per byte.
// 					for uint8(group[j].RecordID>>shift) != i {
// 						// This pair is in the wrong bucket.
// 						// Swap it into the correct bucket.
// 						dstBucket := &buckets[uint8(group[j].RecordID>>shift)]
// 						k := dstBucket.start
// 						dstBucket.start++
// 						group[j], group[k] = group[k], group[j]
// 					}
// 				}
// 			}
// 		}
// 	}
//
// 	// Split the pairs by shard.
// 	shards := make(map[uint64][]IDPair)
// 	for i := 0; i < len(pairs); {
// 		// Select the shard.
// 		shard := pairs[i].RecordID >> shardwidth.Exponent
//
// 		// Find all pairs in the shard.
// 		j := i
// 		incr := 1
// 		for i+incr < len(pairs) && pairs[i+incr].RecordID>>shardwidth.Exponent == shard {
// 			i += incr
// 			incr *= 2
// 		}
// 		for ; incr > 0; incr /= 2 {
// 			if i+incr < len(pairs) && pairs[i+incr].RecordID>>shardwidth.Exponent == shard {
// 				i += incr
// 			}
// 		}
// 		// that found us the last thing in this shard, so...
// 		i++
//
// 		// Add the shard, referencing the original slice.
// 		// This sets the cap so that we dont accidentally overwrite other shards data.
// 		shards[shard] = pairs[j:i:i]
// 	}
//
// 	return shards
// }

// pairsToZigZag converts a set of record-value pairs to Pilosa's zig-zag format.
// This assumes that all pairs are within the same shard.
// func pairsToZigZag(pairs []IDPair) []uint64 {
// 	const recordMask = (1 << shardwidth.Exponent) - 1
//
// 	dst := make([]uint64, len(pairs))
// 	for i, p := range pairs {
// 		dst[i] = (p.ID << shardwidth.Exponent) | (p.RecordID & recordMask)
// 	}
//
// 	return dst
// }

// radixSort64 sorts the data with radix-sort.
// The "shift" is the highest differing bit position, rounded down to a multiple of 8.
// If there are duplicates, this may change the duplicate count for some values.
// func radixSort64(data []uint64, shift uint) {
// 	if len(data) < 2 {
// 		return
// 	}
// 	if shift <= 8 {
// 		// The data falls into a 16-bit span, so the remaining digits can be sorted simultaneously with a bitmask.
// 		maskSort(data)
// 		return
// 	}
//
// 	// Count the values within each bucket.
// 	var buckets [256]struct {
// 		start, end uint
// 	}
// 	for _, v := range data {
// 		buckets[uint8(v>>shift)].end++
// 	}
//
// 	// Assign indices within the group to the buckets.
// 	{
// 		var start uint
// 		for i := range buckets {
// 			bucket := &buckets[i]
// 			bucket.start = start
// 			bucket.end += start
// 			start = bucket.end
// 		}
// 	}
//
// 	// Split the data into the buckets.
// 	var start uint
// 	for i, b := range buckets {
// 		// Replace misplaced values until the contents of the bucket all have the correct digit.
// 		i := uint8(i)
// 		for j := b.start; j < b.end; j++ {
// 			// This inner loop may run quite a few times for the first few buckets, but it will never run more than once-per-element-per-byte.
// 			for uint8(data[j]>>shift) != i {
// 				// This pair is in the wrong bucket.
// 				// Swap it into the correct bucket.
// 				dstBucket := &buckets[uint8(data[j]>>shift)]
// 				k := dstBucket.start
// 				dstBucket.start++
// 				data[j], data[k] = data[k], data[j]
// 			}
// 		}
//
// 		// Sort the contents of the bucket.
// 		data := data[start:b.end]
// 		switch {
// 		case len(data) < 64:
// 			// Use insertion-sort because the data is too small for a more complex algorithm to be efficient.
// 			for i := 0; i < len(data); i++ {
// 				for j := i; j > 0 && data[j-1] > data[j]; j-- {
// 					data[j-1], data[j] = data[j], data[j-1]
// 				}
// 			}
//
// 		default:
// 			// Sort the next byte recursively.
// 			radixSort64(data, shift-8)
// 		}
// 		start = b.end
// 	}
// }

// maskSort sorts integers using a bitmask.
// The values must all fall within one 16-bit span.
// If there are duplicates, this may change the duplicate count for some values.
// func maskSort(data []uint64) {
// 	if len(data) < 2 {
// 		return
// 	}
//
// 	base := data[0] &^ ((1 << 16) - 1)
//
// 	// Dump everything into the mask.
// 	var mask [(1 << 16) / 64]uint64
// 	for _, v := range data {
// 		mask[uint16(v)/64] |= 1 << (v % 64)
// 	}
//
// 	// Scan through the set bits in the mask.
// 	k := 0
// 	for i, w := range mask {
// 		for w != 0 {
// 			j := bits.TrailingZeros64(w)
// 			w &^= 1 << j
// 			data[k] = 64*uint64(i) + uint64(j) + base
// 			k++
// 		}
// 	}
//
// 	if k < len(data) {
// 		// Copy the ending value to fill up the rest of the space.
// 		// This happens once for each duplicate value.
// 		endVal := data[k-1]
// 		for k < len(data) {
// 			data[k] = endVal
// 			k++
// 		}
// 	}
// }
