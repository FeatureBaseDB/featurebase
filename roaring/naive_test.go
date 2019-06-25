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

package roaring

import (
	"reflect"
	"testing"
)

func TestSortSlice(t *testing.T) {
	a := []uint64{1, 3, 2, 8, 5, 21, 13}
	sortSlice(a)
	if !reflect.DeepEqual(a, []uint64{1, 2, 3, 5, 8, 13, 21}) {
		t.Fatalf("unexpected sorting: %v", a)
	}
}

func TestRemoveSliceDuplicates(t *testing.T) {
	a := []uint64{2, 3, 2, 1, 2, 5, 8, 5, 13, 3, 2, 5, 144}
	a = removeSliceDuplicates(a)

	if !reflect.DeepEqual(a, []uint64{1, 2, 3, 5, 8, 13, 144}) {
		t.Fatalf("unexpected values: %v", a)
	}

	a = append(a, 21, 8, 3, 3, 5, 5, 1, 34, 21, 21)
	a = removeSliceDuplicates(a)

	if !reflect.DeepEqual(a, []uint64{1, 2, 3, 5, 8, 13, 21, 34, 144}) {
		t.Fatalf("unexpected values: %v", a)
	}
}

func TestIntersectSlice(t *testing.T) {
	a := []uint64{1, 4, 9, 5, 24, 13}
	b := []uint64{2, 1, 9, 5, 12}
	c := intersectSlice(a, b)

	if !reflect.DeepEqual(c, []uint64{1, 5, 9}) {
		t.Fatalf("unexpected values: %v", c)
	}
	if !reflect.DeepEqual(c, intersectSlice(b, a)) {
		t.Fatalf("unexpected values: %v", c)
	}
	if !reflect.DeepEqual(c, intersectSlice(c, a)) {
		t.Fatalf("unexpected values: %v", c)
	}
}

func TestUnionSlice(t *testing.T) {
	a := []uint64{1, 4, 9, 5, 24, 13}
	b := []uint64{2, 1, 9, 5, 12}
	c := unionSlice(a, b)

	if !reflect.DeepEqual(c, []uint64{1, 2, 4, 5, 9, 12, 13, 24}) {
		t.Fatalf("unexpected values: %v", c)
	}
	if !reflect.DeepEqual(c, unionSlice(b, a)) {
		t.Fatalf("unexpected values: %v", c)
	}
	if !reflect.DeepEqual(c, unionSlice(c, a)) {
		t.Fatalf("unexpected values: %v", c)
	}
}

func TestMaxInSlice(t *testing.T) {
	a := []uint64{1, 4, 9, 5, 24, 13}
	v := maxInSlice(a)
	if uint64(24) != v {
		t.Fatalf("expected %v, but got %v", uint64(24), v)
	}

	for i := uint64(1000); i <= uint64(100000); i++ {
		a = append(a, i)
		if v = maxInSlice(a); v != i {
			t.Fatalf("expected %v, but got %v", i, v)
		}
	}
}

func TestDifferenceSlice(t *testing.T) {
	a := []uint64{1, 4, 9, 5, 24, 13}
	b := []uint64{2, 1, 9, 5, 12}

	c := differenceSlice(a, b)
	if !reflect.DeepEqual(c, []uint64{4, 13, 24}) {
		t.Fatalf("unexpected values: %v", c)
	}

	c = differenceSlice(b, a)
	if !reflect.DeepEqual(c, []uint64{2, 12}) {
		t.Fatalf("unexpected values: %v", c)
	}

	c = differenceSlice(a, a)
	if !reflect.DeepEqual(c, []uint64(nil)) {
		t.Fatalf("unexpected values: %v", c)
	}
}

func TestXorSlice(t *testing.T) {
	a := []uint64{1, 4, 9, 5, 24, 13}
	b := []uint64{2, 1, 9, 5, 12}
	c := xorSlice(a, b)

	if !reflect.DeepEqual(c, []uint64{2, 4, 12, 13, 24}) {
		t.Fatalf("unexpected values: %v", c)
	}
	if !reflect.DeepEqual(c, xorSlice(b, a)) {
		t.Fatalf("unexpected values: %v", c)
	}
	if !reflect.DeepEqual(xorSlice(c, a), []uint64{1, 2, 5, 9, 12}) {
		t.Fatalf("unexpected values: %v", c)
	}
	if !reflect.DeepEqual(xorSlice(c, b), []uint64{1, 4, 5, 9, 13, 24}) {
		t.Fatalf("unexpected values: %v", c)
	}
}

func TestShiftSlice(t *testing.T) {
	a := []uint64{1, 4, 9, 5, 24, 13}

	c := shiftSlice(a, 12)
	if !reflect.DeepEqual(c, []uint64{13, 16, 17, 21, 25, 36}) {
		t.Fatalf("unexpected values: %v", c)
	}

	c = shiftSlice(a, 0)
	if !reflect.DeepEqual(c, []uint64{1, 4, 5, 9, 13, 24}) {
		t.Fatalf("unexpected values: %v", c)
	}
}

func TestForEachInSlice(t *testing.T) {
	a := []uint64{1, 4, 9, 5, 24, 13}
	c := make([]uint64, 0)

	forEachInSlice(a, func(v uint64) {
		c = append(c, v+1)
	})
	if !reflect.DeepEqual(c, []uint64{2, 5, 10, 6, 25, 14}) {
		t.Fatalf("unexpected values: %v", c)
	}
}

func TestForEachInRangeSlice(t *testing.T) {
	a := []uint64{1, 4, 9, 5, 24, 13}
	c := make([]uint64, 0)

	forEachInRangeSlice(a, uint64(3), uint64(12), func(v uint64) {
		c = append(c, v+1)
	})
	if !reflect.DeepEqual(c, []uint64{5, 10, 6}) {
		t.Fatalf("unexpected values: %v", c)
	}
}

func TestContainedInSlice(t *testing.T) {
	a := []uint64{1, 4, 9, 5, 24, 13}

	c := uint64(4)
	idx, found := containedInSlice(a, c)
	if !found {
		t.Fatalf("%v should be in %v", c, a)
	}
	if a[idx] != c {
		t.Fatalf("%v is not at position %v", c, idx)
	}

	c = uint64(12)
	idx, found = containedInSlice(a, c)
	if found {
		t.Fatalf("%v should not be in %v", c, a)
	}
	if idx != -1 {
		t.Fatalf("expected %v, got %v", -1, idx)
	}
}

func TestAddNToSlice(t *testing.T) {
	a := []uint64{1, 4, 9, 5, 24, 13}
	b := []uint64{2, 1, 9, 5, 12}

	c, changed := addNToSlice(a, b...)
	if !reflect.DeepEqual(c, []uint64{1, 2, 4, 5, 9, 12, 13, 24}) {
		t.Fatalf("unexpected values: %v", c)
	}
	if changed != 2 {
		t.Fatalf("changes expected %v, got %v", 2, changed)
	}

	c, changed = addNToSlice(b, a...)
	if !reflect.DeepEqual(c, []uint64{1, 2, 4, 5, 9, 12, 13, 24}) {
		t.Fatalf("%v and %v should be the same", c, a)
	}
	if changed != 3 {
		t.Fatalf("changes expected %v, got %v", 3, changed)
	}

	c, changed = addNToSlice(a, a...)
	if !reflect.DeepEqual(c, []uint64{1, 4, 5, 9, 13, 24}) {
		t.Fatalf("%v and %v should be the same", c, a)
	}
	if changed != 0 {
		t.Fatalf("changes expected %v, got %v", 0, changed)
	}
}

func TestRemoveNFromSlice(t *testing.T) {
	a := []uint64{1, 4, 9, 5, 24, 13}
	b := []uint64{2, 1, 9, 5, 12}

	c, changed := removeNFromSlice(a, b...)
	if !reflect.DeepEqual(c, []uint64{4, 13, 24}) {
		t.Fatalf("%v and %v should be the same", c, []uint64{4, 13, 24})
	}
	if changed != 3 {
		t.Fatalf("%v changes expected, got %v", 3, changed)
	}

	c, changed = removeNFromSlice(b, a...)
	if !reflect.DeepEqual(c, []uint64{2, 12}) {
		t.Fatalf("%v and %v should be the same", c, []uint64{2, 12})
	}
	if changed != 3 {
		t.Fatalf("%v changes expected, got %v", 3, changed)
	}

	c, changed = removeNFromSlice(a, a...)
	if !reflect.DeepEqual(c, []uint64(nil)) {
		t.Fatalf("%v and %v should be the same", c, a)
	}
	if changed != 6 {
		t.Fatalf("%v changes expected, got %v", 0, changed)
	}
}

func TestCountRangeSlice(t *testing.T) {
	a := []uint64{1, 4, 9, 5, 24, 13}

	c := countRangeSlice(a, uint64(3), uint64(12))
	if c != 3 {
		t.Fatalf("expected %v, got %v", 3, c)
	}

	c = countRangeSlice(a, uint64(0), uint64(25))
	if c != 6 {
		t.Fatalf("expected %v, got %v", 6, c)
	}

	c = countRangeSlice(a, uint64(12), uint64(4))
	if c != 0 {
		t.Fatalf("expected %v, got %v", 0, c)
	}
}

func TestRangeSlice(t *testing.T) {
	a := []uint64{1, 4, 9, 5, 24, 13}

	c := rangeSlice(a, uint64(3), uint64(12))
	if !reflect.DeepEqual(c, []uint64{4, 5, 9}) {
		t.Fatalf("unexpected values: %v", c)
	}

	c = rangeSlice(a, uint64(0), uint64(25))
	if !reflect.DeepEqual(c, []uint64{1, 4, 5, 9, 13, 24}) {
		t.Fatalf("unexpected values: %v", c)
	}

	c = rangeSlice(a, uint64(5), uint64(5))
	if !reflect.DeepEqual(c, []uint64(nil)) {
		t.Fatalf("unexpected values: %v", c)
	}
}

func TestFlipSlice(t *testing.T) {
	a := []uint64{1, 4, 9, 5, 24, 13}

	c := flipSlice(a, uint64(3), uint64(12))
	if !reflect.DeepEqual(c, []uint64{1, 3, 6, 7, 8, 10, 11, 12, 13, 24}) {
		t.Fatalf("unexpected values: %v", c)
	}

	c = flipSlice(a, uint64(13), uint64(12))
	if !reflect.DeepEqual(c, []uint64{1, 4, 5, 9, 13, 24}) {
		t.Fatalf("unexpected values: %v", c)
	}

	c = flipSlice(a, uint64(9), uint64(13))
	if !reflect.DeepEqual(c, []uint64{1, 4, 5, 10, 11, 12, 24}) {
		t.Fatalf("unexpected values: %v", c)
	}
}
