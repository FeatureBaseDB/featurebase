// Copyright 2021 Molecula Corp. All rights reserved.
//go:build gofuzz
// +build gofuzz

package roaring

import (
	"encoding/binary"
	"fmt"
	"reflect"
)

// FuzzBitmapUnmarshalBinary fuzz tests the unmarshaling of binary
// to both Pilosa and official roaring formats.
func FuzzBitmapUnmarshalBinary(data []byte) int {
	b := NewBitmap()
	err := b.UnmarshalBinary(data)
	if err != nil {
		return 0
	}
	return 1
}

// FuzzRoaringOps fuzz tests different operations on roaring bitmaps,
// comparing the results to a naive implementation of the operations
// on uint64 slices.
func FuzzRoaringOps(data []byte) int {
	// number of uint64s not to include
	const reserved = 4
	// flipping is too inefficient for large values of end - start
	// and will cause go-fuzz to hang if not controlled.
	const maxFlips = 1000000

	arr := bytesToUint64s(data)
	if len(arr) <= reserved {
		return 0
	}

	// start > end is possible. This will test correctness in unexpected conditions.
	start, end, split, rand := arr[0], arr[1], int(arr[2]), arr[3]

	// don't include start, end, split, rand in the slices
	if split < reserved {
		split = reserved
	}
	// ensure slice in bounds
	if split > len(arr) {
		split = len(arr)
	}
	// using removeSliceDuplicates guarantees that the slice inputs of the
	// following functions do not have duplicates and are sorted, just as
	// the Roaring Bitmap implementations are.
	s1 := removeSliceDuplicates(arr[reserved:split])
	s2 := removeSliceDuplicates(arr[split:])
	if len(s1) == 0 {
		s1 = nil
	}
	if len(s2) == 0 {
		s2 = nil
	}
	bm1 := NewBitmap(arr[reserved:split]...)
	bm2 := NewBitmap(arr[split:]...)

	expected := s1
	actual := bm1.Slice()
	if !reflect.DeepEqual(expected, actual) {
		panic(fmt.Sprintf("first slice:\n expected: %v\n got: %v", expected, actual))
	}

	expected = s2
	actual = bm2.Slice()
	if !reflect.DeepEqual(expected, actual) {
		panic(fmt.Sprintf("second slice:\n expected: %v\n got: %v", expected, actual))
	}
	// Pure functions

	expected = []uint64{maxInSlice(s1), maxInSlice(s2)}
	actual = []uint64{bm1.Max(), bm2.Max()}
	if !reflect.DeepEqual(expected, actual) {
		panic(fmt.Sprintf("max values:\n expected: %v\n got: %v", expected, actual))
	}

	expected = intersectSlice(s1, s2)
	actual = bm1.Intersect(bm2).Slice()
	if !reflect.DeepEqual(expected, actual) {
		panic(fmt.Sprintf("intersection:\n expected: %v\n got: %v", expected, actual))
	}

	expected = unionSlice(s1, s2)
	actual = bm1.Union(bm2).Slice()
	if !reflect.DeepEqual(expected, actual) {
		panic(fmt.Sprintf("union:\n expected: %v\n got: %v", expected, actual))
	}

	expected = differenceSlice(s1, s2)
	actual = bm1.Difference(bm2).Slice()
	if !reflect.DeepEqual(expected, actual) {
		panic(fmt.Sprintf("difference:\n expected: %v\n got: %v", expected, actual))
	}

	expected = xorSlice(s1, s2)
	actual = bm1.Xor(bm2).Slice()
	if !reflect.DeepEqual(expected, actual) {
		panic(fmt.Sprintf("XOR:\n expected: %v\n got: %v", expected, actual))
	}

	if (len(s1) > 0) != bm1.Any() {
		panic(fmt.Sprintf("any:\n %v has %v values but got %v which has %v values", s1, len(s1), bm1.Slice(), bm1.Any()))
	}
	if (len(s2) > 0) != bm2.Any() {
		panic(fmt.Sprintf("any:\n %v has %v values but got %v which has %v values", s2, len(s2), bm2.Slice(), bm2.Any()))
	}

	expected = []uint64{uint64(len(s1)), uint64(len(s2))}
	actual = []uint64{bm1.Count(), bm2.Count()}
	if !reflect.DeepEqual(expected, actual) {
		panic(fmt.Sprintf("count:\n expected: %v\n got: %v", expected, actual))
	}

	expect := countRangeSlice(s1, start, end)
	got := bm1.CountRange(start, end)
	if expect != got {
		panic(fmt.Sprintf("count range:\n count from %v to %v in slice %v and bitmap %v:\n expected %v got %v",
			start, end, s1, bm1.Slice(), expect, got))
	}
	expect = countRangeSlice(s2, start, end)
	got = bm2.CountRange(start, end)
	if expect != got {
		panic(fmt.Sprintf("count range:\n count from %v to %v in slice %v and bitmap %v:\n expected %v got %v",
			start, end, s2, bm2.Slice(), expect, got))
	}

	expected = rangeSlice(s1, start, end)
	actual = bm1.SliceRange(start, end)
	if !reflect.DeepEqual(expected, actual) {
		panic(fmt.Sprintf("slice range:\n from %v to %v in slice %v and bitmap %v:\n expected %v\n got %v",
			start, end, s1, bm1.Slice(), expected, actual))
	}
	expected = rangeSlice(s2, start, end)
	actual = bm2.SliceRange(start, end)
	if !reflect.DeepEqual(expected, actual) {
		panic(fmt.Sprintf("slice range:\n from %v to %v in slice %v and bitmap %v:\n expected %v\n got %v",
			start, end, s2, bm2.Slice(), expected, actual))
	}

	expect = uint64(len(intersectSlice(s1, s2)))
	got = bm1.IntersectionCount(bm2)
	if expect != got {
		panic(fmt.Sprintf("intersection count:\n expected %v got %v", expect, got))
	}

	_, found := containedInSlice(s1, rand)
	if found != bm1.Contains(rand) {
		panic(fmt.Sprintf("contains:\n %v contains %v: %v\n %v contains %v: %v", s1, rand, found,
			bm1.Slice(), rand, bm1.Contains(rand)))
	}
	_, found = containedInSlice(s2, rand)
	if found != bm2.Contains(rand) {
		panic(fmt.Sprintf("contains:\n %v contains %v: %v\n %v contains %v: %v", s2, rand, found,
			bm2.Slice(), rand, bm2.Contains(rand)))
	}

	if end-start < maxFlips {
		expected = flipSlice(s1, start, end)
		actual = bm1.Flip(start, end).Slice()
		if !reflect.DeepEqual(expected, actual) {
			panic(fmt.Sprintf("flip:\n from %v to %v in slice %v and bitmap %v\n expected %v\n got %v",
				start, end, s1, bm1.Slice(), expected, actual))
		}
		expected = flipSlice(s2, start, end)
		actual = bm2.Flip(start, end).Slice()
		if !reflect.DeepEqual(expected, actual) {
			panic(fmt.Sprintf("flip:\n from %v to %v in slice %v and bitmap %v\n expected %v\n got %v",
				start, end, s2, bm2.Slice(), expected, actual))
		}
	}

	expected = make([]uint64, 0)
	actual = make([]uint64, 0)
	forEachInSlice(s1, func(v uint64) { expected = append(expected, v) })
	bm1.ForEach(func(v uint64) error { actual = append(actual, v); return nil })
	if !reflect.DeepEqual(expected, actual) {
		panic(fmt.Sprintf("for each:\n expected %v\n got %v", expected, actual))
	}
	expected = make([]uint64, 0)
	actual = make([]uint64, 0)
	forEachInSlice(s2, func(v uint64) { expected = append(expected, v) })
	bm2.ForEach(func(v uint64) error { actual = append(actual, v); return nil })
	if !reflect.DeepEqual(expected, actual) {
		panic(fmt.Sprintf("for each:\n expected %v\n got %v", expected, actual))
	}

	expected = make([]uint64, 0)
	actual = make([]uint64, 0)
	forEachInRangeSlice(s1, start, end, func(v uint64) { expected = append(expected, v) })
	bm1.ForEachRange(start, end, func(v uint64) error { actual = append(actual, v); return nil })
	if !reflect.DeepEqual(expected, actual) {
		panic(fmt.Sprintf("for each in range:\n expected %v\n got %v", expected, actual))
	}
	expected = make([]uint64, 0)
	actual = make([]uint64, 0)
	forEachInRangeSlice(s2, start, end, func(v uint64) { expected = append(expected, v) })
	bm2.ForEachRange(start, end, func(v uint64) error { actual = append(actual, v); return nil })
	if !reflect.DeepEqual(expected, actual) {
		panic(fmt.Sprintf("for each in range:\n expected %v\n got %v", expected, actual))
	}

	// Impure functions
	// The following tests operations that mutate bitmaps.

	nbm1, nbm2 := bm1.Clone(), bm2.Clone()
	expected = shiftSlice(s1, 1)
	tempBM, _ := nbm1.Shift(1)
	actual = tempBM.Slice()
	if !reflect.DeepEqual(expected, actual) {
		panic(fmt.Sprintf("shift:\n in slice %v and bitmap %v \n expected %v\n got %v",
			s1, bm1.Slice(), expected, actual))
	}
	expected = shiftSlice(s2, 1)
	tempBM, _ = nbm2.Shift(1)
	actual = tempBM.Slice()
	if !reflect.DeepEqual(expected, actual) {
		panic(fmt.Sprintf("shift:\n in slice %v and bitmap %v \n expected %v\n got %v",
			s2, bm2.Slice(), expected, actual))
	}

	// reuse start and end as random values
	rand2 := start
	rand3 := end

	nbm1 = bm1.Clone()
	expected, echanged := addNToSlice(s1, rand)
	achanged := nbm1.DirectAddN(rand)
	actual = nbm1.Slice()
	if echanged != achanged || !reflect.DeepEqual(expected, actual) {
		panic(fmt.Sprintf("directAddN:\n adding %v in slice %v and bitmap %v \n expected %v and %v changed \n got %v and %v changed",
			rand, s1, bm1.Slice(), expected, echanged, actual, achanged))
	}
	nbm2 = bm2.Clone()
	expected, echanged = addNToSlice(s2, rand2, rand3)
	achanged = nbm2.DirectAddN(rand2, rand3)
	actual = nbm2.Slice()
	if echanged != achanged || !reflect.DeepEqual(expected, actual) {
		panic(fmt.Sprintf("directAddN:\n adding %v and %v in slice %v and bitmap %v \n expected %v and %v changed \n got %v and %v changed",
			rand2, rand3, s2, bm2.Slice(), expected, echanged, actual, achanged))
	}

	nbm1 = bm1.Clone()
	expected, echanged = removeNFromSlice(s1, rand2, rand3)
	achanged = nbm1.DirectRemoveN(rand2, rand3)
	actual = nbm1.Slice()
	if echanged != achanged || !reflect.DeepEqual(expected, actual) {
		panic(fmt.Sprintf("directRemoveN\n removing %v and %v in slice %v and bitmap %v \n expected %v and %v changed \n got %v and %v changed",
			rand2, rand3, s1, bm1.Slice(), expected, echanged, actual, achanged))
	}
	nbm2 = bm2.Clone()
	expected, echanged = removeNFromSlice(s2, rand)
	achanged = nbm2.DirectRemoveN(rand)
	actual = nbm2.Slice()
	if echanged != achanged || !reflect.DeepEqual(expected, actual) {
		panic(fmt.Sprintf("directRemoveN:\n removing %v in slice %v and bitmap %v \n expected %v and %v changed \n got %v and %v changed",
			rand, s2, bm2.Slice(), expected, echanged, actual, achanged))
	}

	nbm1, nbm2 = bm1.Clone(), bm2.Clone()
	expected = unionSlice(s1, s2)
	nbm1.UnionInPlace(nbm2)
	actual = nbm1.Slice()
	if !reflect.DeepEqual(expected, actual) {
		panic(fmt.Sprintf("union in place:\n expected %v\n got %v", expected, actual))
	}

	return 1
}

func bytesToUint64s(data []byte) []uint64 {
	const uint64Size = 8
	size := len(data) / uint64Size

	slice := make([]uint64, 0)
	for i := 0; i < size; i++ {
		offset := i * uint64Size
		num := binary.LittleEndian.Uint64(data[offset : offset+uint64Size])
		slice = append(slice, num)
	}
	return slice
}

// copy and paste the following to a main file to run.
// path should be the absolute path to the corpus.
// make sure filename is not already in the corpus.
func addSliceToCorpus(slice []uint64, filename, path string) {
	data := uint64sToBytes(slice)
	err := os.WriteFile(path+"/"+filename, data, 0750)
	if err != nil {
		fmt.Printf("could not write to file: %v\n", err)
	}
}

func uint64sToBytes(slice []uint64) []byte {
	const uint64Size = 8
	size := len(slice) * uint64Size

	data := make([]byte, size)
	for i := 0; i < len(slice); i++ {
		offset := i * uint64Size
		binary.LittleEndian.PutUint64(data[offset:offset+uint64Size], slice[i])
	}
	return data
}
