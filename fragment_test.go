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

package pilosa_test

import (
	"bytes"
	"flag"
	"io/ioutil"
	"math"
	"os"
	"reflect"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/pilosa/pilosa"
)

// Test flags
var (
	FragmentPath = flag.String("fragment", "", "fragment path")
)

// SliceWidth is a helper reference to use when testing.
const SliceWidth = pilosa.SliceWidth

// Ensure a fragment can set a bit and retrieve it.
func TestFragment_SetBit(t *testing.T) {
	f := MustOpenFragment("i", "f", pilosa.ViewStandard, 0, "")
	defer f.Close()

	// Set bits on the fragment.
	if _, err := f.SetBit(120, 1); err != nil {
		t.Fatal(err)
	} else if _, err := f.SetBit(120, 6); err != nil {
		t.Fatal(err)
	} else if _, err := f.SetBit(121, 0); err != nil {
		t.Fatal(err)
	}

	// Verify counts on rows.
	if n := f.Row(120).Count(); n != 2 {
		t.Fatalf("unexpected count: %d", n)
	} else if n := f.Row(121).Count(); n != 1 {
		t.Fatalf("unexpected count: %d", n)
	}

	// Close and reopen the fragment & verify the data.
	if err := f.Reopen(); err != nil {
		t.Fatal(err)
	} else if n := f.Row(120).Count(); n != 2 {
		t.Fatalf("unexpected count (reopen): %d", n)
	} else if n := f.Row(121).Count(); n != 1 {
		t.Fatalf("unexpected count (reopen): %d", n)
	}
}

// Ensure a fragment can clear a set bit.
func TestFragment_ClearBit(t *testing.T) {
	f := MustOpenFragment("i", "f", pilosa.ViewStandard, 0, "")
	defer f.Close()

	// Set and then clear bits on the fragment.
	if _, err := f.SetBit(1000, 1); err != nil {
		t.Fatal(err)
	} else if _, err := f.SetBit(1000, 2); err != nil {
		t.Fatal(err)
	} else if _, err := f.ClearBit(1000, 1); err != nil {
		t.Fatal(err)
	}

	// Verify count on row.
	if n := f.Row(1000).Count(); n != 1 {
		t.Fatalf("unexpected count: %d", n)
	}

	// Close and reopen the fragment & verify the data.
	if err := f.Reopen(); err != nil {
		t.Fatal(err)
	} else if n := f.Row(1000).Count(); n != 1 {
		t.Fatalf("unexpected count (reopen): %d", n)
	}
}

// Ensure a fragment can snapshot correctly.
func TestFragment_Snapshot(t *testing.T) {
	f := MustOpenFragment("i", "f", pilosa.ViewStandard, 0, "")
	defer f.Close()

	// Set and then clear bits on the fragment.
	if _, err := f.SetBit(1000, 1); err != nil {
		t.Fatal(err)
	} else if _, err := f.SetBit(1000, 2); err != nil {
		t.Fatal(err)
	} else if _, err := f.ClearBit(1000, 1); err != nil {
		t.Fatal(err)
	}

	// Snapshot bitmap and verify data.
	if err := f.Snapshot(); err != nil {
		t.Fatal(err)
	} else if n := f.Row(1000).Count(); n != 1 {
		t.Fatalf("unexpected count: %d", n)
	}

	// Close and reopen the fragment & verify the data.
	if err := f.Reopen(); err != nil {
		t.Fatal(err)
	} else if n := f.Row(1000).Count(); n != 1 {
		t.Fatalf("unexpected count (reopen): %d", n)
	}
}

// Ensure a fragment can iterate over all bits in order.
func TestFragment_ForEachBit(t *testing.T) {
	f := MustOpenFragment("i", "f", pilosa.ViewStandard, 0, "")
	defer f.Close()

	// Set bits on the fragment.
	if _, err := f.SetBit(100, 20); err != nil {
		t.Fatal(err)
	} else if _, err := f.SetBit(2, 38); err != nil {
		t.Fatal(err)
	} else if _, err := f.SetBit(2, 37); err != nil {
		t.Fatal(err)
	}

	// Iterate over bits.
	var result [][2]uint64
	if err := f.ForEachBit(func(rowID, columnID uint64) error {
		result = append(result, [2]uint64{rowID, columnID})
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// Verify bits are correct.
	if !reflect.DeepEqual(result, [][2]uint64{{2, 37}, {2, 38}, {100, 20}}) {
		t.Fatalf("unexpected result: %#v", result)
	}
}

// Ensure a fragment can return the top n results.
func TestFragment_Top(t *testing.T) {
	f := MustOpenFragment("i", "f", pilosa.ViewStandard, 0, pilosa.CacheTypeRanked)
	defer f.Close()
	// Set bits on the rows 100, 101, & 102.
	f.MustSetBits(100, 1, 3, 200)
	f.MustSetBits(101, 1)
	f.MustSetBits(102, 1, 2)
	f.RecalculateCache()

	// Retrieve top rows.
	if pairs, err := f.Top(pilosa.TopOptions{N: 2}); err != nil {
		t.Fatal(err)
	} else if len(pairs) != 2 {
		t.Fatalf("unexpected count: %d", len(pairs))
	} else if pairs[0] != (pilosa.Pair{ID: 100, Count: 3}) {
		t.Fatalf("unexpected pair(0): %v", pairs[0])
	} else if pairs[1] != (pilosa.Pair{ID: 102, Count: 2}) {
		t.Fatalf("unexpected pair(1): %v", pairs[1])
	}
}

// Ensure a fragment can filter rows when retrieving the top n rows.
func TestFragment_Top_Filter(t *testing.T) {
	f := MustOpenFragment("i", "f", pilosa.ViewStandard, 0, pilosa.CacheTypeRanked)
	defer f.Close()

	// Set bits on the rows 100, 101, & 102.
	f.MustSetBits(100, 1, 3, 200)
	f.MustSetBits(101, 1)
	f.MustSetBits(102, 1, 2)
	f.RecalculateCache()
	// Assign attributes.
	f.RowAttrStore.SetAttrs(101, map[string]interface{}{"x": uint64(10)})
	f.RowAttrStore.SetAttrs(102, map[string]interface{}{"x": uint64(20)})

	// Retrieve top rows.
	if pairs, err := f.Top(pilosa.TopOptions{
		N:            2,
		FilterField:  "x",
		FilterValues: []interface{}{int64(10), int64(15), int64(20)},
	}); err != nil {
		t.Fatal(err)
	} else if len(pairs) != 2 {
		t.Fatalf("unexpected count: %d", len(pairs))
	} else if pairs[0] != (pilosa.Pair{ID: 102, Count: 2}) {
		t.Fatalf("unexpected pair(0): %v", pairs[0])
	} else if pairs[1] != (pilosa.Pair{ID: 101, Count: 1}) {
		t.Fatalf("unexpected pair(1): %v", pairs[1])
	}
}

// Ensure a fragment can return top rows that intersect with an input row.
func TestFragment_TopN_Intersect(t *testing.T) {
	f := MustOpenFragment("i", "f", pilosa.ViewStandard, 0, pilosa.CacheTypeRanked)
	defer f.Close()

	// Create an intersecting input row.
	src := pilosa.NewBitmap(1, 2, 3)

	// Set bits on various rows.
	f.MustSetBits(100, 1, 10, 11, 12)    // one intersection
	f.MustSetBits(101, 1, 2, 3, 4)       // three intersections
	f.MustSetBits(102, 1, 2, 4, 5, 6)    // two intersections
	f.MustSetBits(103, 1000, 1001, 1002) // no intersection
	f.RecalculateCache()

	// Retrieve top rows.
	if pairs, err := f.Top(pilosa.TopOptions{N: 3, Src: src}); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(pairs, []pilosa.Pair{
		{ID: 101, Count: 3},
		{ID: 102, Count: 2},
		{ID: 100, Count: 1},
	}) {
		t.Fatalf("unexpected pairs: %s", spew.Sdump(pairs))
	}
}

// Ensure a fragment can return top rows that have many bits set.
func TestFragment_TopN_Intersect_Large(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}

	f := MustOpenFragment("i", "f", pilosa.ViewStandard, 0, pilosa.CacheTypeRanked)
	defer f.Close()

	// Create an intersecting input row.
	src := pilosa.NewBitmap(
		980, 981, 982, 983, 984, 985, 986, 987, 988, 989,
		990, 991, 992, 993, 994, 995, 996, 997, 998, 999,
	)

	// Set bits on rows 0 - 999. Higher rows have higher bit counts.
	for i := uint64(0); i < 1000; i++ {
		for j := uint64(0); j < i; j++ {
			f.MustSetBits(i, j)
		}
	}
	f.RecalculateCache()

	// Retrieve top rows.
	if pairs, err := f.Top(pilosa.TopOptions{N: 10, Src: src}); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(pairs, []pilosa.Pair{
		{ID: 999, Count: 19},
		{ID: 998, Count: 18},
		{ID: 997, Count: 17},
		{ID: 996, Count: 16},
		{ID: 995, Count: 15},
		{ID: 994, Count: 14},
		{ID: 993, Count: 13},
		{ID: 992, Count: 12},
		{ID: 991, Count: 11},
		{ID: 990, Count: 10},
	}) {
		t.Fatalf("unexpected pairs: %s", spew.Sdump(pairs))
	}
}

// Ensure a fragment can return top rows when specified by ID.
func TestFragment_TopN_IDs(t *testing.T) {
	f := MustOpenFragment("i", "f", pilosa.ViewStandard, 0, pilosa.CacheTypeRanked)
	defer f.Close()

	// Set bits on various rows.
	f.MustSetBits(100, 1, 2, 3)
	f.MustSetBits(101, 4, 5, 6, 7)
	f.MustSetBits(102, 8, 9, 10, 11, 12)

	// Retrieve top rows.
	if pairs, err := f.Top(pilosa.TopOptions{RowIDs: []uint64{100, 101, 200}}); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(pairs, []pilosa.Pair{
		{ID: 101, Count: 4},
		{ID: 100, Count: 3},
	}) {
		t.Fatalf("unexpected pairs: %s", spew.Sdump(pairs))
	}
}

// Ensure the fragment cache limit works
func TestFragment_TopN_CacheSize(t *testing.T) {
	slice := uint64(0)
	cacheSize := uint32(3)

	// Create Index.
	index := MustOpenIndex()
	defer index.Close()

	// Create frame.
	frame, err := index.CreateFrameIfNotExists("f", pilosa.FrameOptions{CacheType: pilosa.CacheTypeRanked, CacheSize: cacheSize})
	if err != nil {
		t.Fatal(err)
	}

	// Create view.
	view, err := frame.CreateViewIfNotExists(pilosa.ViewStandard)
	if err != nil {
		t.Fatal(err)
	}

	// Create fragment.
	frag, err := view.CreateFragmentIfNotExists(slice)
	if err != nil {
		t.Fatal(err)
	}
	// Close the storage so we can re-open it without encountering a flock.
	frag.Close()

	f := &Fragment{
		Fragment:     frag,
		RowAttrStore: MustOpenAttrStore(),
	}
	f.Fragment.RowAttrStore = f.RowAttrStore.AttrStore
	if err := f.Open(); err != nil {
		panic(err)
	}
	defer f.Close()

	// Set bits on various rows.
	f.MustSetBits(100, 1, 2, 3)
	f.MustSetBits(101, 4, 5, 6, 7)
	f.MustSetBits(102, 8, 9, 10, 11, 12)
	f.MustSetBits(103, 8, 9, 10, 11, 12, 13)
	f.MustSetBits(104, 8, 9, 10, 11, 12, 13, 14)
	f.MustSetBits(105, 10, 11)

	f.RecalculateCache()

	p := []pilosa.Pair{
		{ID: 104, Count: 7},
		{ID: 103, Count: 6},
		{ID: 102, Count: 5},
	}

	// Retrieve top rows.
	if pairs, err := f.Top(pilosa.TopOptions{N: 5}); err != nil {
		t.Fatal(err)
	} else if len(pairs) > int(cacheSize) {
		t.Fatalf("TopN count cannot exceed cache size: %d", cacheSize)
	} else if pairs[0] != (pilosa.Pair{ID: 104, Count: 7}) {
		t.Fatalf("unexpected pair(0): %v", pairs)
	} else if !reflect.DeepEqual(pairs, p) {
		t.Fatalf("Invalid TopN result set: %s", spew.Sdump(pairs))
	}
}

// Ensure fragment can return a checksum for its blocks.
func TestFragment_Checksum(t *testing.T) {
	f := MustOpenFragment("i", "f", pilosa.ViewStandard, 0, "")
	defer f.Close()

	// Retrieve checksum and set bits.
	orig := f.Checksum()
	if _, err := f.SetBit(1, 200); err != nil {
		t.Fatal(err)
	} else if _, err := f.SetBit(pilosa.HashBlockSize*2, 200); err != nil {
		t.Fatal(err)
	}

	// Ensure new checksum is different.
	if chksum := f.Checksum(); bytes.Equal(chksum, orig) {
		t.Fatalf("expected checksum to change: %x", chksum, orig)
	}
}

// Ensure fragment can return a checksum for a given block.
func TestFragment_Blocks(t *testing.T) {
	f := MustOpenFragment("i", "f", pilosa.ViewStandard, 0, "")
	defer f.Close()

	// Retrieve initial checksum.
	var prev []pilosa.FragmentBlock

	// Set first bit.
	if _, err := f.SetBit(0, 0); err != nil {
		t.Fatal(err)
	}
	blocks := f.Blocks()
	if blocks[0].Checksum == nil {
		t.Fatalf("expected checksum: %x", blocks[0].Checksum)
	}
	prev = blocks

	// Set bit on different row.
	if _, err := f.SetBit(20, 0); err != nil {
		t.Fatal(err)
	}
	blocks = f.Blocks()
	if bytes.Equal(blocks[0].Checksum, prev[0].Checksum) {
		t.Fatalf("expected checksum to change: %x", blocks[0].Checksum)
	}
	prev = blocks

	// Set bit on different column.
	if _, err := f.SetBit(20, 100); err != nil {
		t.Fatal(err)
	}
	blocks = f.Blocks()
	if bytes.Equal(blocks[0].Checksum, prev[0].Checksum) {
		t.Fatalf("expected checksum to change: %x", blocks[0].Checksum)
	}
}

// Ensure fragment returns an empty checksum if no data exists for a block.
func TestFragment_Blocks_Empty(t *testing.T) {
	f := MustOpenFragment("i", "f", pilosa.ViewStandard, 0, "")
	defer f.Close()

	// Set bits on a different block.
	if _, err := f.SetBit(100, 1); err != nil {
		t.Fatal(err)
	}

	// Ensure checksum for block 1 is blank.
	if blocks := f.Blocks(); len(blocks) != 1 {
		t.Fatalf("unexpected block count: %d", len(blocks))
	} else if blocks[0].ID != 1 {
		t.Fatalf("unexpected block id: %d", blocks[0].ID)
	}
}

// Ensure a fragment's cache can be persisted between restarts.
func TestFragment_LRUCache_Persistence(t *testing.T) {
	f := MustOpenFragment("i", "f", pilosa.ViewStandard, 0, pilosa.CacheTypeLRU)
	defer f.Close()

	// Set bits on the fragment.
	for i := uint64(0); i < 1000; i++ {
		if _, err := f.SetBit(i, 0); err != nil {
			t.Fatal(err)
		}
	}

	// Verify correct cache type and size.
	if cache, ok := f.Cache().(*pilosa.LRUCache); !ok {
		t.Fatalf("unexpected cache: %T", f.Cache())
	} else if cache.Len() != 1000 {
		t.Fatalf("unexpected cache len: %d", cache.Len())
	}

	// Reopen the fragment.
	if err := f.Reopen(); err != nil {
		t.Fatal(err)
	}

	// Re-verify correct cache type and size.
	if cache, ok := f.Cache().(*pilosa.LRUCache); !ok {
		t.Fatalf("unexpected cache: %T", f.Cache())
	} else if cache.Len() != 1000 {
		t.Fatalf("unexpected cache len: %d", cache.Len())
	}
}

// Ensure a fragment's cache can be persisted between restarts.
func TestFragment_RankCache_Persistence(t *testing.T) {
	index := MustOpenIndex()
	defer index.Close()

	// Create frame.
	frame, err := index.CreateFrameIfNotExists("f", pilosa.FrameOptions{CacheType: pilosa.CacheTypeRanked})
	if err != nil {
		t.Fatal(err)
	}

	// Create view.
	view, err := frame.CreateViewIfNotExists(pilosa.ViewStandard)
	if err != nil {
		t.Fatal(err)
	}

	// Create fragment.
	f, err := view.CreateFragmentIfNotExists(0)
	if err != nil {
		t.Fatal(err)
	}

	// Set bits on the fragment.
	for i := uint64(0); i < 1000; i++ {
		if _, err := f.SetBit(i, 0); err != nil {
			t.Fatal(err)
		}
	}

	// Verify correct cache type and size.
	if cache, ok := f.Cache().(*pilosa.RankCache); !ok {
		t.Fatalf("unexpected cache: %T", f.Cache())
	} else if cache.Len() != 1000 {
		t.Fatalf("unexpected cache len: %d", cache.Len())
	}

	// Reopen the index.
	if err := index.Reopen(); err != nil {
		t.Fatal(err)
	}

	// Re-fetch fragment.
	f = index.Frame("f").View(pilosa.ViewStandard).Fragment(0)

	// Re-verify correct cache type and size.
	if cache, ok := f.Cache().(*pilosa.RankCache); !ok {
		t.Fatalf("unexpected cache: %T", f.Cache())
	} else if cache.Len() != 1000 {
		t.Fatalf("unexpected cache len: %d", cache.Len())
	}
}

// Ensure a fragment can be copied to another fragment.
func TestFragment_WriteTo_ReadFrom(t *testing.T) {
	f0 := MustOpenFragment("i", "f", pilosa.ViewStandard, 0, "")
	defer f0.Close()

	// Set and then clear bits on the fragment.
	if _, err := f0.SetBit(1000, 1); err != nil {
		t.Fatal(err)
	} else if _, err := f0.SetBit(1000, 2); err != nil {
		t.Fatal(err)
	} else if _, err := f0.ClearBit(1000, 1); err != nil {
		t.Fatal(err)
	}

	// Verify cache is populated.
	if n := f0.Cache().Len(); n != 1 {
		t.Fatalf("unexpected cache size: %d", n)
	}

	// Write fragment to a buffer.
	var buf bytes.Buffer
	wn, err := f0.WriteTo(&buf)
	if err != nil {
		t.Fatal(err)
	}

	// Read into another fragment.
	f1 := MustOpenFragment("i", "f", pilosa.ViewStandard, 0, "")
	if rn, err := f1.ReadFrom(&buf); err != nil {
		t.Fatal(err)
	} else if wn != rn {
		t.Fatalf("read/write byte count mismatch: wn=%d, rn=%d", wn, rn)
	}

	// Verify cache is in other fragment.
	if n := f1.Cache().Len(); n != 1 {
		t.Fatalf("unexpected cache size: %d", n)
	}

	// Verify data in other fragment.
	if a := f1.Row(1000).Bits(); !reflect.DeepEqual(a, []uint64{2}) {
		t.Fatalf("unexpected bits: %+v", a)
	}

	// Close and reopen the fragment & verify the data.
	if err := f1.Reopen(); err != nil {
		t.Fatal(err)
	} else if n := f1.Cache().Len(); n != 1 {
		t.Fatalf("unexpected cache size (reopen): %d", n)
	} else if a := f1.Row(1000).Bits(); !reflect.DeepEqual(a, []uint64{2}) {
		t.Fatalf("unexpected bits (reopen): %+v", a)
	}
}

func BenchmarkFragment_Blocks(b *testing.B) {
	if *FragmentPath == "" {
		b.Skip("no fragment specified")
	}

	// Open the fragment specified by the path.
	f := pilosa.NewFragment(*FragmentPath, "i", "f", pilosa.ViewStandard, 0)
	if err := f.Open(); err != nil {
		b.Fatal(err)
	}
	defer f.Close()

	// Reset timer and execute benchmark.
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if a := f.Blocks(); len(a) == 0 {
			b.Fatal("no blocks in fragment")
		}
	}
}

func BenchmarkFragment_IntersectionCount(b *testing.B) {
	f := MustOpenFragment("i", "f", pilosa.ViewStandard, 0, "")
	defer f.Close()
	f.MaxOpN = math.MaxInt32

	// Generate some intersecting data.
	for i := 0; i < 10000; i += 2 {
		if _, err := f.SetBit(1, uint64(i)); err != nil {
			b.Fatal(err)
		}
	}
	for i := 0; i < 10000; i += 3 {
		if _, err := f.SetBit(2, uint64(i)); err != nil {
			b.Fatal(err)
		}
	}

	// Snapshot to disk before benchmarking.
	if err := f.Snapshot(); err != nil {
		b.Fatal(err)
	}

	// Start benchmark
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if n := f.Row(1).IntersectionCount(f.Row(2)); n == 0 {
			b.Fatalf("unexpected count: %d", n)
		}
	}
}

// Fragment is a test wrapper for pilosa.Fragment.
type Fragment struct {
	*pilosa.Fragment
	RowAttrStore *AttrStore
}

// NewFragment returns a new instance of Fragment with a temporary path.
func NewFragment(index, frame, view string, slice uint64, cacheType string) *Fragment {
	file, err := ioutil.TempFile("", "pilosa-fragment-")
	if err != nil {
		panic(err)
	}
	file.Close()

	f := &Fragment{
		Fragment:     pilosa.NewFragment(file.Name(), index, frame, view, slice),
		RowAttrStore: MustOpenAttrStore(),
	}
	f.Fragment.CacheType = cacheType
	f.Fragment.RowAttrStore = f.RowAttrStore.AttrStore
	return f
}

// MustOpenFragment creates and opens an fragment at a temporary path. Panic on error.
func MustOpenFragment(index, frame, view string, slice uint64, cacheType string) *Fragment {
	if cacheType == "" {
		cacheType = pilosa.DefaultCacheType
	}
	f := NewFragment(index, frame, view, slice, cacheType)

	if err := f.Open(); err != nil {
		panic(err)
	}
	return f
}

// Close closes the fragment and removes all underlying data.
func (f *Fragment) Close() error {
	defer os.Remove(f.Path())
	defer os.Remove(f.CachePath())
	defer f.RowAttrStore.Close()
	return f.Fragment.Close()
}

// Reopen closes the fragment and reopens it as a new instance.
func (f *Fragment) Reopen() error {
	cacheType := f.Fragment.CacheType
	path := f.Path()
	if err := f.Fragment.Close(); err != nil {
		return err
	}

	f.Fragment = pilosa.NewFragment(path, f.Index(), f.Frame(), f.View(), f.Slice())
	f.Fragment.CacheType = cacheType
	f.Fragment.RowAttrStore = f.RowAttrStore.AttrStore
	if err := f.Open(); err != nil {
		return err
	}
	return nil
}

// MustSetBits sets bits on a row. Panic on error.
// This function does not accept a timestamp or quantum.
func (f *Fragment) MustSetBits(rowID uint64, columnIDs ...uint64) {
	for _, columnID := range columnIDs {
		if _, err := f.SetBit(rowID, columnID); err != nil {
			panic(err)
		}
	}
}

// MustClearBits clears bits on a row. Panic on error.
func (f *Fragment) MustClearBits(rowID uint64, columnIDs ...uint64) {
	for _, columnID := range columnIDs {
		if _, err := f.ClearBit(rowID, columnID); err != nil {
			panic(err)
		}
	}
}

// RowAttrStore provides simple storage for attributes.
type RowAttrStore struct {
	attrs map[uint64]map[string]interface{}
}

// NewRowAttrStore returns a new instance of RowAttrStore.
func NewRowAttrStore() *RowAttrStore {
	return &RowAttrStore{
		attrs: make(map[uint64]map[string]interface{}),
	}
}

// RowAttrs returns the attributes set to a row id.
func (s *RowAttrStore) RowAttrs(id uint64) (map[string]interface{}, error) {
	return s.attrs[id], nil
}

// SetRowAttrs assigns a set of attributes to a row id.
func (s *RowAttrStore) SetRowAttrs(id uint64, m map[string]interface{}) {
	s.attrs[id] = m
}

// GenerateImportFill generates a set of bits pairs that evenly fill a fragment chunk.
func GenerateImportFill(rowN int, pct float64) (rowIDs, columnIDs []uint64) {
	ipct := int(pct * 100)
	for i := 0; i < SliceWidth*rowN; i++ {
		if i%100 >= ipct {
			continue
		}

		rowIDs = append(rowIDs, uint64(i%SliceWidth))
		columnIDs = append(columnIDs, uint64(i/SliceWidth))
	}
	return
}

func TestFragment_Tanimoto(t *testing.T) {
	f := MustOpenFragment("i", "f", pilosa.ViewStandard, 0, pilosa.CacheTypeRanked)
	defer f.Close()

	src := pilosa.NewBitmap(1, 2, 3)

	// Set bits on the rows 100, 101, & 102.
	f.MustSetBits(100, 1, 3, 2, 200)
	f.MustSetBits(101, 1, 3)
	f.MustSetBits(102, 1, 2, 10, 12)
	f.RecalculateCache()

	if pairs, err := f.Top(pilosa.TopOptions{TanimotoThreshold: 50, Src: src}); err != nil {
		t.Fatal(err)
	} else if len(pairs) != 2 {
		t.Fatalf("unexpected count: %d", len(pairs))
	} else if pairs[0] != (pilosa.Pair{ID: 100, Count: 3}) {
		t.Fatalf("unexpected pair(0): %v", pairs[0])
	} else if pairs[1] != (pilosa.Pair{ID: 101, Count: 2}) {
		t.Fatalf("unexpected pair(1): %v", pairs[1])
	}
}

func TestFragment_Zero_Tanimoto(t *testing.T) {
	f := MustOpenFragment("i", "f", pilosa.ViewStandard, 0, pilosa.CacheTypeRanked)
	defer f.Close()

	src := pilosa.NewBitmap(1, 2, 3)

	// Set bits on the rows 100, 101, & 102.
	f.MustSetBits(100, 1, 3, 2, 200)
	f.MustSetBits(101, 1, 3)
	f.MustSetBits(102, 1, 2, 10, 12)
	f.RecalculateCache()

	if pairs, err := f.Top(pilosa.TopOptions{TanimotoThreshold: 0, Src: src}); err != nil {
		t.Fatal(err)
	} else if len(pairs) != 3 {
		t.Fatalf("unexpected count: %d", len(pairs))
	} else if pairs[0] != (pilosa.Pair{ID: 100, Count: 3}) {
		t.Fatalf("unexpected pair(0): %v", pairs[0])
	} else if pairs[1] != (pilosa.Pair{ID: 101, Count: 2}) {
		t.Fatalf("unexpected pair(1): %v", pairs[1])
	} else if pairs[2] != (pilosa.Pair{ID: 102, Count: 2}) {
		t.Fatalf("unexpected pair(1): %v", pairs[2])
	}
}
