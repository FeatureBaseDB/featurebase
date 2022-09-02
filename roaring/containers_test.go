// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package roaring

import (
	"math/rand"
	"sort"
	"testing"
)

func TestContainersIterator(t *testing.T) {
	slc := NewFileBitmap().Containers
	testContainersIterator(slc, t)
}

func testContainersIterator(cs Containers, t *testing.T) {
	itr, found := cs.Iterator(0)
	if found {
		t.Fatalf("shouldn't have found 0 in empty btc")
	}
	if itr.Next() {
		t.Fatal("Next() should be false for empty btc")
	}

	cs.Put(1, NewContainerArray([]uint16{1}))
	cs.Put(2, NewContainerArray([]uint16{1, 2}))

	itr, found = cs.Iterator(0)
	if found {
		t.Fatalf("shouldn't have found 0")
	}

	if !itr.Next() {
		t.Fatalf("one should be next, but got false")
	}
	if key, val := itr.Value(); key != 1 || val.N() != 1 {
		t.Fatalf("Wrong k/v, exp: 1,1 got: %v,%v", key, val.N())
	}
	if !itr.Next() {
		t.Fatalf("two should be next, but got false")
	}
	if key, val := itr.Value(); key != 2 || val.N() != 2 {
		t.Fatalf("Wrong k/v, exp: 2,2 got: %v,%v", key, val.N())
	}

	if itr.Next() {
		t.Fatalf("itr should be done, but got true")
	}

	cs.Put(3, NewContainerArray([]uint16{1, 2, 3}))
	cs.Put(5, NewContainerArray([]uint16{1, 2, 3, 4, 5}))
	cs.Put(6, NewContainerArray([]uint16{1, 2, 3, 4, 5, 6}))

	itr, found = cs.Iterator(3)
	if !itr.Next() {
		t.Fatalf("3 should be next, but got false")
	}
	if !found {
		t.Fatalf("should have found 3")
	}
	if key, val := itr.Value(); key != 3 || val.N() != 3 {
		t.Fatalf("Wrong k/v, exp: 3,3 got: %v,%v", key, val.N())
	}
	if !itr.Next() {
		t.Fatalf("5 should be next, but got false")
	}
	if key, val := itr.Value(); key != 5 || val.N() != 5 {
		t.Fatalf("Wrong k/v, exp: 5,5 got: %v,%v", key, val.N())
	}

	itr, found = cs.Iterator(4)
	if found {
		t.Fatalf("shouldn't have found 4")
	}
	if !itr.Next() {
		t.Fatalf("5 should be next, but got false")
	}
	if key, val := itr.Value(); key != 5 || val.N() != 5 {
		t.Fatalf("Wrong k/v, exp: 5,5 got: %v,%v", key, val.N())
	}
	if !itr.Next() {
		t.Fatalf("6 should be next, but got false")
	}
	if key, val := itr.Value(); key != 6 || val.N() != 6 {
		t.Fatalf("Wrong k/v, exp: 6,6 got: %v,%v", key, val.N())
	}

	if itr.Next() {
		t.Fatalf("itr should be done, but got true")
	}
}

func TestSliceContainers(t *testing.T) {
	const size = 10
	n := size
	sc := newSliceContainers()

	// Add n keys
	for i := 0; i < n; i++ {
		key, set := uint64(i), []uint16{uint16(i)}
		sc.Put(key, NewContainerArray(set))
	}

	t.Run("Get n keys", func(t *testing.T) {
		for i := 0; i < n; i++ {
			key, set := uint64(i), []uint16{uint16(i)}
			c := sc.Get(key)
			if c == nil {
				t.Fatalf("Get(%d) returned nil container", key)
			}
			if len(c.data) > 0 { // happy linter
				if c.data[0] != set[0] {
					t.Fatalf("Get(%d): expected: %v, got: %v", key, set[0], c.data[0])
				}
			}
		}
	})

	t.Run("Last key/container", func(t *testing.T) {
		key, c := sc.Last()
		if key != uint64(n-1) || c.data[0] != uint16(n-1) {
			t.Fatalf("Last: expected: %v, got: %d, %v", n-1, key, c.data)
		}
	})

	// Remove odd keys
	for i := 1; i < size; i += 2 {
		key := uint64(i)
		sc.Remove(key)
		n--
	}

	t.Run("Try to Get removed containers", func(t *testing.T) {
		for i := 1; i < size; i += 2 {
			key := uint64(i)
			c := sc.Get(key)
			if c != nil {
				t.Fatalf("Get(for non existing key %d): found container: %v", key, c.data)
			}
		}

		// Test - Last key/container
		key, c := sc.Last()
		if key != uint64(size-2) || c.data[0] != uint16(size-2) {
			t.Fatalf("Last: expected: %v, got: %d, %v", size-2, key, c.data)
		}

		if sc.Size() != n {
			t.Fatalf("Size: expected: %d, got: %d", n, sc.Size())
		}
	})

	t.Run("Nil containers and repair them", func(t *testing.T) {
		// Remove half of even containers
		for i := range sc.containers {
			if i%2 == 0 {
				sc.containers[i] = nil
				n--
			}
		}
		sc.Repair()

		if sc.Size() != n {
			t.Fatalf("Size: expected: %d, got: %d", n, sc.Size())
		}

		for i, key := range sc.keys {
			if sc.containers[i] == nil {
				t.Fatalf("Found nil container for key: %d at index: %d", key, i)
			} else {
				if sc.containers[i].data[0] != uint16(key) {
					t.Fatalf("Invalid container data for key: %d at index: %d - expected: %d, got: %d",
						key, i, uint16(key), sc.containers[i].data[0],
					)
				}
			}
		}
	})
}

func genRun(r *rand.Rand) Interval16 {
gen:
	dat := r.Uint32()
	start, end := uint16(dat>>16), uint16(dat)
	if start > end {
		goto gen
	}
	return Interval16{start, end}
}

func splatRunNaive(into []uint64, from Interval16) {
	for v := int(from.Start); v <= int(from.Last); v++ {
		into[v/64] |= (uint64(1) << uint(v%64))
	}
}

func TestSplat(t *testing.T) {
	r := rand.New(rand.NewSource(42))
	for i := 0; i < 1024; i++ {
		run := genRun(r)

		var a, b [1024]uint64
		splatRunNaive(a[:], run)
		splatRun(&b, run)
		if a != b {
			t.Errorf("incorrect splat of run [%d, %d]", run.Start, run.Last)
		}
	}
}

func benchSplat(b *testing.B, run Interval16) {
	var buf [1024]uint64
	for i := 0; i < b.N; i++ {
		splatRun(&buf, run)
	}
}

func BenchmarkSplatSingle(b *testing.B)   { benchSplat(b, Interval16{42, 42}) }
func BenchmarkSplatPartword(b *testing.B) { benchSplat(b, Interval16{16, 31}) }
func BenchmarkSplatWord(b *testing.B)     { benchSplat(b, Interval16{16, 31}) }
func BenchmarkSplatEdges(b *testing.B)    { benchSplat(b, Interval16{15, 16}) }
func BenchmarkSplatMedium(b *testing.B)   { benchSplat(b, Interval16{13, 65}) }
func BenchmarkSplatAll(b *testing.B)      { benchSplat(b, Interval16{0, ^uint16(0)}) }

func TestRemakeContainersFrom(t *testing.T) {
	c := NewContainerArray([]uint16{})
	var containerValues []uint64
	inputs := make([]uint64, 0, 16000)
	var next uint64
	for i := 0; i < 16; i++ {
		for j := 0; j < 1000; j++ {
			inputs = append(inputs, next)
			next += uint64(i) + 12
		}
		if i == 8 {
			// ensure we skip a container's worth of stuff so we're verifying
			// that, at least once, the "next key" is not just key+1.
			next += 1 << 17
		}
	}
	// RemakeContainerFrom corrupts its inputs, so.
	original := make([]uint64, len(inputs))
	copy(original, inputs)
	output := make([]uint64, len(inputs))
	expectedTotal := len(inputs)
	total := 0
	var expectedNext uint64
	// count how often expectedNext isn't just "the next value of k". We
	// skipped a two-container-width hunk of values above, which should
	// always get us one aligned container of no values at all.
	skips := 0
	for k := uint64(0); k <= (next >> 16); k++ {
		containerValues, inputs, expectedNext = GetMatchingKeysFrom(inputs, k)
		if expectedNext == ^uint64(0) {
			if len(inputs) != 0 {
				t.Fatalf("expectedNext: got ^0 with %d inputs left, next %d",
					len(inputs), inputs[0])
			}
		} else if expectedNext != (k + 1) {
			skips++
		}
		c = RemakeContainerFrom(c, containerValues)
		vals := c.Slice()
		for _, v := range vals {
			output[total] = (k << 16) + uint64(v)
			total++
		}
	}
	if skips != 1 {
		t.Fatalf("expected one skip in sequence, got %d", skips)
	}
	if total != expectedTotal {
		t.Fatalf("expected %d things in containers, got %d", expectedTotal, total)
	}
	for i, v := range original {
		if v != output[i] {
			t.Fatalf("position %d in output: expected %d, got %d",
				i, v, output[i])
		}
	}
	// Now, reshuffle it!
	set := make(map[uint64]struct{}, len(original))
	for _, v := range original {
		set[v] = struct{}{}
	}
	inputs = inputs[:0]
	for k := range set {
		inputs = append(inputs, k)
	}
	// we now have an *unsorted* list. let's see whether that works.
	key := inputs[0] >> 16
	total = 0
	for len(inputs) > 0 {
		containerValues, inputs, expectedNext = GetMatchingKeysFrom(inputs, key)
		if expectedNext == ^uint64(0) {
			if len(inputs) != 0 {
				t.Fatalf("expectedNext: got ^0 with %d inputs left, next %d",
					len(inputs), inputs[0])
			}
		}
		c = RemakeContainerFrom(c, containerValues)
		vals := c.Slice()
		for _, v := range vals {
			output[total] = (key << 16) + uint64(v)
			total++
		}
		key = expectedNext
	}
	sort.Slice(output, func(i, j int) bool { return output[i] < output[j] })
	if total != expectedTotal {
		t.Fatalf("expected %d things in containers, got %d", expectedTotal, total)
	}
	for i, v := range original {
		if v != output[i] {
			t.Fatalf("position %d in output: expected %d, got %d",
				i, v, output[i])
		}
	}
}
