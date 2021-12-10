package generator

import (
	"math/rand"
	"sort"
)

// Uint64Slice generates between [0, n) random uint64 numbers between min and max.
func Uint64Slice(n int, min, max uint64, sorted bool, rand *rand.Rand) []uint64 {
	a := make([]uint64, rand.Intn(n))
	for i := range a {
		a[i] = min + uint64(rand.Int63n(int64(max-min)))
	}

	if sorted {
		sort.Sort(uint64Slice(a))
	}

	return a
}

// Uint64SetSlice returns the values in a uint64 set.
func Uint64SetSlice(m map[uint64]struct{}) []uint64 {
	a := make([]uint64, 0, len(m))
	for v := range m {
		a = append(a, v)
	}
	sort.Sort(uint64Slice(a))
	return a
}

// uint64Slice represents a sortable slice of uint64 numbers.
type uint64Slice []uint64

func (u uint64Slice) Swap(i, j int)      { u[i], u[j] = u[j], u[i] }
func (u uint64Slice) Len() int           { return len(u) }
func (u uint64Slice) Less(i, j int) bool { return u[i] < u[j] }
