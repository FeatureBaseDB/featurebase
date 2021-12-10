package roaring

import (
	"math/rand"
	"reflect"
	"testing"
)

func TestSortSlice(t *testing.T) {
	tests := []struct{ a, expected []uint64 }{
		{
			a:        []uint64{1, 3, 2, 8, 5, 21, 13},
			expected: []uint64{1, 2, 3, 5, 8, 13, 21},
		},
	}

	for _, test := range tests {
		sortSlice(test.a)
		if !reflect.DeepEqual(test.a, test.expected) {
			t.Fatalf("unexpected sorting: %v", test.a)
		}
	}
}

func TestRemoveSliceDuplicates(t *testing.T) {
	tests := []struct{ a, expected []uint64 }{
		{
			a:        []uint64{2, 3, 2, 1, 2, 5, 8, 5, 13, 3, 2, 5, 144},
			expected: []uint64{1, 2, 3, 5, 8, 13, 144},
		},
		{
			a:        []uint64{2, 3, 2, 1, 2, 5, 8, 5, 13, 3, 2, 5, 144, 21, 8, 3, 3, 5, 5, 1, 34, 21, 21},
			expected: []uint64{1, 2, 3, 5, 8, 13, 21, 34, 144},
		},
	}

	for _, test := range tests {
		got := removeSliceDuplicates(test.a)
		if !reflect.DeepEqual(got, test.expected) {
			t.Fatalf("expected %v, got %v", test.expected, got)
		}
	}
}

func TestIntersectSlice(t *testing.T) {
	tests := []struct{ a, b, expected []uint64 }{
		{
			a:        []uint64{1, 4, 9, 5, 24, 13},
			b:        []uint64{2, 1, 9, 5, 12},
			expected: []uint64{1, 5, 9},
		},
		{
			a:        []uint64{2, 1, 9, 5, 12},
			b:        []uint64{1, 4, 9, 5, 24, 13},
			expected: []uint64{1, 5, 9},
		},
		{
			a:        []uint64{1, 4, 9, 5, 24, 13},
			b:        []uint64{1, 5, 9},
			expected: []uint64{1, 5, 9},
		},
	}

	for _, test := range tests {
		got := intersectSlice(test.a, test.b)
		if !reflect.DeepEqual(test.expected, got) {
			t.Fatalf("expected %v, got %v", test.expected, got)
		}
	}
}

func TestUnionSlice(t *testing.T) {
	tests := []struct{ a, b, expected []uint64 }{
		{
			a:        []uint64{1, 4, 9, 5, 24, 13},
			b:        []uint64{2, 1, 9, 5, 12},
			expected: []uint64{1, 2, 4, 5, 9, 12, 13, 24},
		},
		{
			a:        []uint64{2, 1, 9, 5, 12},
			b:        []uint64{1, 4, 9, 5, 24, 13},
			expected: []uint64{1, 2, 4, 5, 9, 12, 13, 24},
		},
		{
			a:        []uint64{1, 4, 9, 5, 24, 13},
			b:        []uint64{1, 5, 9},
			expected: []uint64{1, 4, 5, 9, 13, 24},
		},
	}

	for _, test := range tests {
		got := unionSlice(test.a, test.b)
		if !reflect.DeepEqual(test.expected, got) {
			t.Fatalf("expected %v, got %v", test.expected, got)
		}
	}
}

func TestMaxInSlice(t *testing.T) {
	// arbitrary, we just want to get the same values every time
	r := rand.New(rand.NewSource(23))
	a := []uint64{1, 4, 9, 5, 24, 13}
	v := maxInSlice(a)
	if uint64(24) != v {
		t.Fatalf("expected %v, but got %v", uint64(24), v)
	}

	for i := uint64(1000); i <= uint64(100000); i += uint64(r.Intn(35)) + 1 {
		a = append(a, i)
		if v = maxInSlice(a); v != i {
			t.Fatalf("expected %v, but got %v", i, v)
		}
	}
}

func TestDifferenceSlice(t *testing.T) {
	tests := []struct{ a, b, expected []uint64 }{
		{
			a:        []uint64{1, 4, 9, 5, 24, 13},
			b:        []uint64{2, 1, 9, 5, 12},
			expected: []uint64{4, 13, 24},
		},
		{
			a:        []uint64{2, 1, 9, 5, 12},
			b:        []uint64{1, 4, 9, 5, 24, 13},
			expected: []uint64{2, 12},
		},
		{
			a:        []uint64{1, 4, 9, 5, 24, 13},
			b:        []uint64{1, 4, 9, 5, 24, 13},
			expected: []uint64(nil),
		},
	}

	for _, test := range tests {
		got := differenceSlice(test.a, test.b)
		if !reflect.DeepEqual(test.expected, got) {
			t.Fatalf("expected %v, got %v", test.expected, got)
		}
	}
}

func TestXorSlice(t *testing.T) {
	tests := []struct{ a, b, expected []uint64 }{
		{
			a:        []uint64{1, 4, 9, 5, 24, 13},
			b:        []uint64{2, 1, 9, 5, 12},
			expected: []uint64{2, 4, 12, 13, 24},
		},
		{
			a:        []uint64{2, 1, 9, 5, 12},
			b:        []uint64{1, 4, 9, 5, 24, 13},
			expected: []uint64{2, 4, 12, 13, 24},
		},
		{
			a:        []uint64{2, 4, 12, 13, 24},
			b:        []uint64{1, 4, 9, 5, 24, 13},
			expected: []uint64{1, 2, 5, 9, 12},
		},
		{
			a:        []uint64{2, 4, 12, 13, 24},
			b:        []uint64{1, 2, 5, 9, 12},
			expected: []uint64{1, 4, 5, 9, 13, 24},
		},
	}

	for _, test := range tests {
		got := xorSlice(test.a, test.b)
		if !reflect.DeepEqual(test.expected, got) {
			t.Fatalf("expected %v, got %v", test.expected, got)
		}
	}
}

func TestShiftSlice(t *testing.T) {
	tests := []struct {
		a        []uint64
		shift    int
		expected []uint64
	}{
		{
			a:        []uint64{1, 4, 9, 5, 24, 13},
			shift:    12,
			expected: []uint64{13, 16, 17, 21, 25, 36},
		},
		{
			a:        []uint64{1, 4, 9, 5, 24, 13},
			shift:    0,
			expected: []uint64{1, 4, 5, 9, 13, 24},
		},
		{
			a:        []uint64{1, 4, 9, 5, 24, 13},
			shift:    1,
			expected: []uint64{2, 5, 6, 10, 14, 25},
		},
	}

	for _, test := range tests {
		got := shiftSlice(test.a, test.shift)

		if !reflect.DeepEqual(got, test.expected) {
			t.Fatalf("expected %v, got %v", test.expected, got)
		}
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
	tests := []struct {
		a     []uint64
		c     uint64
		index int
		found bool
	}{
		{
			a:     []uint64{1, 4, 9, 5, 24, 13},
			c:     uint64(4),
			index: 1,
			found: true,
		},
		{
			a:     []uint64{1, 4, 9, 5, 24, 13},
			c:     uint64(12),
			index: -1,
			found: false,
		},
	}

	for _, test := range tests {
		idx, found := containedInSlice(test.a, test.c)
		if found != test.found {
			t.Fatalf("expected value of found: %v, got %v", test.found, found)
		}
		if idx != test.index {
			t.Fatalf("expected index %v, got %v", test.index, idx)
		}
	}
}

func TestAddNToSlice(t *testing.T) {
	tests := []struct {
		a        []uint64
		b        []uint64
		expected []uint64
		changed  int
	}{
		{
			a:        []uint64{1, 4, 9, 5, 24, 13},
			b:        []uint64{2, 1, 9, 5, 12},
			expected: []uint64{1, 2, 4, 5, 9, 12, 13, 24},
			changed:  2,
		},
		{
			a:        []uint64{2, 1, 9, 5, 12},
			b:        []uint64{1, 4, 9, 5, 24, 13},
			expected: []uint64{1, 2, 4, 5, 9, 12, 13, 24},
			changed:  3,
		},
		{
			a:        []uint64{1, 4, 9, 5, 24, 13},
			b:        []uint64{1, 4, 9, 5, 24, 13},
			expected: []uint64{1, 4, 5, 9, 13, 24},
			changed:  0,
		},
	}

	for _, test := range tests {
		got, changed := addNToSlice(test.a, test.b...)
		if !reflect.DeepEqual(test.expected, got) {
			t.Fatalf("expected slices %v, got %v", test.expected, got)
		}
		if changed != test.changed {
			t.Fatalf("expected changed %v, got %v", test.changed, changed)
		}
	}
}

func TestRemoveNFromSlice(t *testing.T) {
	tests := []struct {
		a        []uint64
		b        []uint64
		expected []uint64
		changed  int
	}{
		{
			a:        []uint64{1, 4, 9, 5, 24, 13},
			b:        []uint64{2, 1, 9, 5, 12},
			expected: []uint64{4, 13, 24},
			changed:  3,
		},
		{
			a:        []uint64{2, 1, 9, 5, 12},
			b:        []uint64{1, 4, 9, 5, 24, 13},
			expected: []uint64{2, 12},
			changed:  3,
		},
		{
			a:        []uint64{1, 4, 9, 5, 24, 13},
			b:        []uint64{1, 4, 9, 5, 24, 13},
			expected: []uint64(nil),
			changed:  6,
		},
	}

	for _, test := range tests {
		got, changed := removeNFromSlice(test.a, test.b...)
		if !reflect.DeepEqual(test.expected, got) {
			t.Fatalf("expected slices %v, got %v", test.expected, got)
		}
		if changed != test.changed {
			t.Fatalf("expected changed %v, got %v", test.changed, changed)
		}
	}
}

func TestCountRangeSlice(t *testing.T) {
	tests := []struct {
		a        []uint64
		start    uint64
		end      uint64
		expected uint64
	}{
		{
			a:        []uint64{1, 4, 9, 5, 24, 13},
			start:    uint64(3),
			end:      uint64(12),
			expected: uint64(3),
		},
		{
			a:        []uint64{1, 4, 9, 5, 24, 13},
			start:    uint64(0),
			end:      uint64(25),
			expected: uint64(6),
		},
		{
			a:        []uint64{1, 4, 9, 5, 24, 13},
			start:    uint64(12),
			end:      uint64(4),
			expected: uint64(0),
		},
		{
			a:        []uint64{1, 4, 9, 5, 24, 13},
			start:    uint64(4),
			end:      uint64(4),
			expected: uint64(0),
		},
	}

	for _, test := range tests {
		got := countRangeSlice(test.a, test.start, test.end)
		if got != test.expected {
			t.Fatalf("expected %v, got %v", test.expected, got)
		}
	}
}

func TestRangeSlice(t *testing.T) {
	tests := []struct {
		a        []uint64
		start    uint64
		end      uint64
		expected []uint64
	}{
		{
			a:        []uint64{1, 4, 9, 5, 24, 13},
			start:    uint64(3),
			end:      uint64(12),
			expected: []uint64{4, 5, 9},
		},
		{
			a:        []uint64{1, 4, 9, 5, 24, 13},
			start:    uint64(0),
			end:      uint64(25),
			expected: []uint64{1, 4, 5, 9, 13, 24},
		},
		{
			a:        []uint64{1, 4, 9, 5, 24, 13},
			start:    uint64(5),
			end:      uint64(5),
			expected: []uint64(nil),
		},
	}

	for _, test := range tests {
		got := rangeSlice(test.a, test.start, test.end)
		if !reflect.DeepEqual(got, test.expected) {
			t.Fatalf("expected %v, got %v", test.expected, got)
		}
	}
}

func TestFlipSlice(t *testing.T) {
	tests := []struct {
		a        []uint64
		start    uint64
		end      uint64
		expected []uint64
	}{
		{
			a:        []uint64{1, 4, 9, 5, 24, 13},
			start:    uint64(3),
			end:      uint64(12),
			expected: []uint64{1, 3, 6, 7, 8, 10, 11, 12, 13, 24},
		},
		{
			a:        []uint64{1, 4, 9, 5, 24, 13},
			start:    uint64(13),
			end:      uint64(12),
			expected: []uint64{1, 4, 5, 9, 13, 24},
		},
		{
			a:        []uint64{1, 4, 9, 5, 24, 13},
			start:    uint64(9),
			end:      uint64(13),
			expected: []uint64{1, 4, 5, 10, 11, 12, 24},
		},
	}

	for _, test := range tests {
		got := flipSlice(test.a, test.start, test.end)
		if !reflect.DeepEqual(got, test.expected) {
			t.Fatalf("expected %v, got %v", test.expected, got)
		}
	}
}
