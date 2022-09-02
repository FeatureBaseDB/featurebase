// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package roaring

import (
	"math"
	"sort"
)

// The following functions reimplement Roaring Bitmap methods, but done naively on
// uint64 slices. Most of these functions are inefficient, which is acceptable because
// this is purely for testing consistency with Roaring internal operations. Thus, the
// functions should be easily guaranteed to produce the correct results.

func sortSlice(slice []uint64) {
	sort.Slice(slice, func(i, j int) bool { return slice[i] < slice[j] })
}

// removeSliceDuplicates removes duplicate values
// in the slice and sorts the output.
func removeSliceDuplicates(slice []uint64) []uint64 {
	// just throw slice into a map and
	// get the values out again
	hash := make(map[uint64]bool)
	for _, val := range slice {
		hash[val] = true
	}
	unique := make([]uint64, 0)
	for key := range hash {
		unique = append(unique, key)
	}

	if len(unique) == 0 {
		return nil
	}
	sortSlice(unique)
	return unique
}

// intersect intersects two []uint64s, removing any duplicates
// and sorting the final output.
func intersectSlice(s1, s2 []uint64) []uint64 {
	// throw both slices in maps
	hash1 := make(map[uint64]bool)
	for _, val := range s1 {
		hash1[val] = true
	}
	hash2 := make(map[uint64]bool)
	for _, val := range s2 {
		hash2[val] = true
	}

	intersection := make([]uint64, 0)

	// look for keys from hash1 also in hash2
	for key := range hash1 {
		if _, found := hash2[key]; found {
			intersection = append(intersection, key)
		}
	}

	if len(intersection) == 0 {
		return nil
	}
	sortSlice(intersection)
	return intersection
}

// union unions two []uint64s and sorts the output.
func unionSlice(s1, s2 []uint64) []uint64 {
	// just dump both slices in a map
	// and get the values out again
	hash := make(map[uint64]bool)
	for _, val := range s1 {
		hash[val] = true
	}
	for _, val := range s2 {
		hash[val] = true
	}
	union := make([]uint64, 0)

	for key := range hash {
		union = append(union, key)
	}

	if len(union) == 0 {
		return nil
	}
	sortSlice(union)
	return union
}

// maxSlice returns the max in the slice.
func maxInSlice(slice []uint64) uint64 {
	if len(slice) == 0 {
		return 0
	}

	max := uint64(0)
	for _, val := range slice {
		if val > max {
			max = val
		}
	}
	return max
}

// differenceSlice returns a slice containing the values
// present in the first slice but not in the second.
func differenceSlice(s1, s2 []uint64) []uint64 {
	// throw s2 in a map, check if each value
	// in s1 is also in that map
	hash := make(map[uint64]bool)
	for _, val := range s2 {
		hash[val] = true
	}
	diff := make([]uint64, 0)
	for _, val := range s1 {
		if _, found := hash[val]; !found {
			diff = append(diff, val)
		}
	}
	// make sure duplicates in s1 are not added
	diff = removeSliceDuplicates(diff)
	return diff
}

// xorSlice returns an array containing the values
// present in exactly one of the two slices.
func xorSlice(s1, s2 []uint64) []uint64 {
	// throw both slices in maps
	hash1 := make(map[uint64]bool)
	for _, val := range s1 {
		hash1[val] = true
	}
	hash2 := make(map[uint64]bool)
	for _, val := range s2 {
		hash2[val] = true
	}

	xor := make([]uint64, 0)
	// add all values in hash1 not in hash2
	for key := range hash1 {
		if _, found := hash2[key]; !found {
			xor = append(xor, key)
		}
	}
	// add all values in hash2 not in hash1
	for key := range hash2 {
		if _, found := hash1[key]; !found {
			xor = append(xor, key)
		}
	}

	if len(xor) == 0 {
		return nil
	}
	sortSlice(xor)
	return xor
}

// shiftSlice adds n to each element and sorts the slice, but ignores any values that
// will cause an overflow. This does not modify the original slice, unlike the Roaring implementation.
func shiftSlice(slice []uint64, n int) []uint64 {
	shifted := make([]uint64, 0)
	for _, val := range slice {
		if uint64(n) <= math.MaxUint64-val {
			shifted = append(shifted, val+uint64(n))
		}
	}

	if len(shifted) == 0 {
		return nil
	}
	sortSlice(shifted)
	return shifted
}

// forEachSlice executes fn for each element in the slice.
func forEachInSlice(slice []uint64, fn func(uint64)) {
	for _, val := range slice {
		fn(val)
	}
}

// forEachRangeSlice executes fn for each element in slice that is in [start, end).
func forEachInRangeSlice(slice []uint64, start, end uint64, fn func(uint64)) {
	for _, val := range slice {
		if start <= val && val < end {
			fn(val)
		}
	}
}

// containedInSlice returns the index of the first instance of v and true
// if v is in slice and returns -1 and false otherwise.
func containedInSlice(slice []uint64, v uint64) (int, bool) {
	for idx := range slice {
		if v == slice[idx] {
			return idx, true
		}
	}
	return -1, false
}

// addNToSlice adds the contents of a to slice and returns the new slice and
// number of values successfully added. This somewhat mimics *Bitmap.DirectAddN
// and but does not modify slice in place, so it returns that new slice instead.
func addNToSlice(slice []uint64, a ...uint64) ([]uint64, int) {
	newSlice := make([]uint64, len(slice))
	copy(newSlice, slice)
	changed := 0

	for _, val := range a {
		if _, found := containedInSlice(newSlice, val); !found {
			newSlice = append(newSlice, val)
			changed++
		}
	}

	if len(newSlice) == 0 {
		return nil, changed
	}
	sortSlice(newSlice)
	return newSlice, changed
}

// removeNFromSlice removes the contents of a from slice and returns the new slice and
// number of values successfully removed. This somewhat mimics *Bitmap.DirectRemoveN
// and but does not modify slice in place, so it returns that new slice instead.
func removeNFromSlice(slice []uint64, a ...uint64) ([]uint64, int) {
	newSlice := make([]uint64, len(slice))
	copy(newSlice, slice)
	changed := 0

	for _, val := range a {
		if i, found := containedInSlice(newSlice, val); found {
			newSlice = append(newSlice[:i], newSlice[i+1:]...)
			changed++
		}
	}

	if len(newSlice) == 0 {
		return nil, changed
	}
	sortSlice(newSlice)
	return newSlice, changed
}

// countRangeSlice returns the number of values in slice that are in [start, end).
func countRangeSlice(slice []uint64, start, end uint64) uint64 {
	count := uint64(0)
	for _, val := range slice {
		if start <= val && val < end {
			count++
		}
	}
	return count
}

// rangeSlice returns a sorted slice of integers between [start, end).
func rangeSlice(slice []uint64, start, end uint64) []uint64 {
	newSlice := make([]uint64, 0)
	for _, val := range slice {
		if start <= val && val < end {
			newSlice = append(newSlice, val)
		}
	}

	if len(newSlice) == 0 {
		return nil
	}
	sortSlice(newSlice)
	return newSlice
}

// flipSplice returns a slice containing all numbers in [start, end]
// that are not in the original slice, as well as the numbers in the
// original slice not in [start, end].
func flipSlice(slice []uint64, start, end uint64) []uint64 {
	if start > end {
		sortSlice(slice)
		return slice
	}

	flipped := make([]uint64, 0)
	// add values in slice outside [start, end]
	hash := make(map[uint64]bool)
	for _, val := range slice {
		hash[val] = true
	}
	for val := range hash {
		if val < start || val > end {
			flipped = append(flipped, val)
		}
	}

	for i := start; i <= end; i++ {
		if _, found := containedInSlice(slice, i); !found {
			flipped = append(flipped, i)
		}
	}

	if len(flipped) == 0 {
		return nil
	}
	sortSlice(flipped)
	return flipped
}
