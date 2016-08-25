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
	"github.com/umbel/pilosa"
)

// Test flags
var (
	FragmentPath = flag.String("fragment", "", "fragment path")
)

// SliceWidth is a helper reference to use when testing.
const SliceWidth = pilosa.SliceWidth

// Ensure a fragment can set a bit and retrieve it.
func TestFragment_SetBit(t *testing.T) {
	f := MustOpenFragment("d", "f", 0)
	defer f.Close()

	// Set bits on the fragment.
	if _, err := f.SetBit(120, 1, nil, 0); err != nil {
		t.Fatal(err)
	} else if _, err := f.SetBit(120, 6, nil, 0); err != nil {
		t.Fatal(err)
	} else if _, err := f.SetBit(121, 0, nil, 0); err != nil {
		t.Fatal(err)
	}

	// Verify counts on bitmaps.
	if n := f.Bitmap(120).Count(); n != 2 {
		t.Fatalf("unexpected count: %d", n)
	} else if n := f.Bitmap(121).Count(); n != 1 {
		t.Fatalf("unexpected count: %d", n)
	}

	// Close and reopen the fragment & verify the data.
	if err := f.Reopen(); err != nil {
		t.Fatal(err)
	} else if n := f.Bitmap(120).Count(); n != 2 {
		t.Fatalf("unexpected count (reopen): %d", n)
	} else if n := f.Bitmap(121).Count(); n != 1 {
		t.Fatalf("unexpected count (reopen): %d", n)
	}
}

// Ensure a fragment can clear a set bit.
func TestFragment_ClearBit(t *testing.T) {
	f := MustOpenFragment("d", "f", 0)
	defer f.Close()

	// Set and then clear bits on the fragment.
	if _, err := f.SetBit(1000, 1, nil, 0); err != nil {
		t.Fatal(err)
	} else if _, err := f.SetBit(1000, 2, nil, 0); err != nil {
		t.Fatal(err)
	} else if _, err := f.ClearBit(1000, 1); err != nil {
		t.Fatal(err)
	}

	// Verify count on bitmap.
	if n := f.Bitmap(1000).Count(); n != 1 {
		t.Fatalf("unexpected count: %d", n)
	}

	// Close and reopen the fragment & verify the data.
	if err := f.Reopen(); err != nil {
		t.Fatal(err)
	} else if n := f.Bitmap(1000).Count(); n != 1 {
		t.Fatalf("unexpected count (reopen): %d", n)
	}
}

// Ensure a fragment can snapshot correctly.
func TestFragment_Snapshot(t *testing.T) {
	f := MustOpenFragment("d", "f", 0)
	defer f.Close()

	// Set and then clear bits on the fragment.
	if _, err := f.SetBit(1000, 1, nil, 0); err != nil {
		t.Fatal(err)
	} else if _, err := f.SetBit(1000, 2, nil, 0); err != nil {
		t.Fatal(err)
	} else if _, err := f.ClearBit(1000, 1); err != nil {
		t.Fatal(err)
	}

	// Snapshot bitmap and verify data.
	if err := f.Snapshot(); err != nil {
		t.Fatal(err)
	} else if n := f.Bitmap(1000).Count(); n != 1 {
		t.Fatalf("unexpected count: %d", n)
	}

	// Close and reopen the fragment & verify the data.
	if err := f.Reopen(); err != nil {
		t.Fatal(err)
	} else if n := f.Bitmap(1000).Count(); n != 1 {
		t.Fatalf("unexpected count (reopen): %d", n)
	}
}

// Ensure a fragment can return the top n results.
func TestFragment_Top(t *testing.T) {
	f := MustOpenFragment("d", "f", 0)
	defer f.Close()

	// Set bits on the bitmaps 100, 101, & 102.
	f.MustSetBits(100, 1, 3, 200)
	f.MustSetBits(101, 1)
	f.MustSetBits(102, 1, 2)

	// Retrieve top bitmaps.
	if pairs, err := f.Top(pilosa.TopOptions{N: 2}); err != nil {
		t.Fatal(err)
	} else if len(pairs) != 2 {
		t.Fatalf("unexpected count: %d", len(pairs))
	} else if pairs[0] != (pilosa.Pair{Key: 100, Count: 3}) {
		t.Fatalf("unexpected pair(0): %v", pairs[0])
	} else if pairs[1] != (pilosa.Pair{Key: 102, Count: 2}) {
		t.Fatalf("unexpected pair(1): %v", pairs[1])
	}
}

// Ensure a fragment can filter bitmaps when retrieving the top n bitmaps.
func TestFragment_Top_Filter(t *testing.T) {
	f := MustOpenFragment("d", "f", 0)
	defer f.Close()

	// Set bits on the bitmaps 100, 101, & 102.
	f.MustSetBits(100, 1, 3, 200)
	f.MustSetBits(101, 1)
	f.MustSetBits(102, 1, 2)

	// Assign attributes.
	f.BitmapAttrStore.SetAttrs(101, map[string]interface{}{"x": uint64(10)})
	f.BitmapAttrStore.SetAttrs(102, map[string]interface{}{"x": uint64(20)})

	// Retrieve top bitmaps.
	if pairs, err := f.Top(pilosa.TopOptions{
		N:            2,
		FilterField:  "x",
		FilterValues: []interface{}{uint64(10), uint64(15), uint64(20)},
	}); err != nil {
		t.Fatal(err)
	} else if len(pairs) != 2 {
		t.Fatalf("unexpected count: %d", len(pairs))
	} else if pairs[0] != (pilosa.Pair{Key: 102, Count: 2}) {
		t.Fatalf("unexpected pair(0): %v", pairs[0])
	} else if pairs[1] != (pilosa.Pair{Key: 101, Count: 1}) {
		t.Fatalf("unexpected pair(1): %v", pairs[1])
	}
}

// Ensure a fragment can return top bitmaps that intersect with an input bitmap.
func TestFragment_TopN_Intersect(t *testing.T) {
	f := MustOpenFragment("d", "f", 0)
	defer f.Close()

	// Create an intersecting input bitmap.
	src := pilosa.NewBitmap(1, 2, 3)

	// Set bits on various bitmaps.
	f.MustSetBits(100, 1, 10, 11, 12)    // one intersection
	f.MustSetBits(101, 1, 2, 3, 4)       // three intersections
	f.MustSetBits(102, 1, 2, 4, 5, 6)    // two intersections
	f.MustSetBits(103, 1000, 1001, 1002) // no intersection

	// Retrieve top bitmaps.
	if pairs, err := f.Top(pilosa.TopOptions{N: 3, Src: src}); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(pairs, []pilosa.Pair{
		{Key: 101, Count: 3},
		{Key: 102, Count: 2},
		{Key: 100, Count: 1},
	}) {
		t.Fatalf("unexpected pairs: %s", spew.Sdump(pairs))
	}
}

// Ensure a fragment can return top bitmaps that have many bits set.
func TestFragment_TopN_Intersect_Large(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}

	f := MustOpenFragment("d", "f", 0)
	defer f.Close()

	// Create an intersecting input bitmap.
	src := pilosa.NewBitmap(
		980, 981, 982, 983, 984, 985, 986, 987, 988, 989,
		990, 991, 992, 993, 994, 995, 996, 997, 998, 999,
	)

	// Set bits on bitmaps 0 - 999. Higher bitmaps have higher bit counts.
	for i := uint64(0); i < 1000; i++ {
		for j := uint64(0); j < i; j++ {
			f.MustSetBits(i, j)
		}
	}

	// Retrieve top bitmaps.
	if pairs, err := f.Top(pilosa.TopOptions{N: 10, Src: src}); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(pairs, []pilosa.Pair{
		{Key: 999, Count: 19},
		{Key: 998, Count: 18},
		{Key: 997, Count: 17},
		{Key: 996, Count: 16},
		{Key: 995, Count: 15},
		{Key: 994, Count: 14},
		{Key: 993, Count: 13},
		{Key: 992, Count: 12},
		{Key: 991, Count: 11},
		{Key: 990, Count: 10},
	}) {
		t.Fatalf("unexpected pairs: %s", spew.Sdump(pairs))
	}
}

// Ensure a fragment can return top bitmaps when specified by ID.
func TestFragment_TopN_BitmapIDs(t *testing.T) {
	f := MustOpenFragment("d", "f", 0)
	defer f.Close()

	// Set bits on various bitmaps.
	f.MustSetBits(100, 1, 2, 3)
	f.MustSetBits(101, 4, 5, 6, 7)
	f.MustSetBits(102, 8, 9, 10, 11, 12)

	// Retrieve top bitmaps.
	if pairs, err := f.Top(pilosa.TopOptions{BitmapIDs: []uint64{100, 101, 200}}); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(pairs, []pilosa.Pair{
		{Key: 101, Count: 4},
		{Key: 100, Count: 3},
	}) {
		t.Fatalf("unexpected pairs: %s", spew.Sdump(pairs))
	}
}

// Ensure fragment can return a checksum for its blocks.
func TestFragment_Checksum(t *testing.T) {
	f := MustOpenFragment("d", "f", 0)
	defer f.Close()

	// Retrieve checksum and set bits.
	orig := f.Checksum()
	if _, err := f.SetBit(1, 200, nil, 0); err != nil {
		t.Fatal(err)
	} else if _, err := f.SetBit(pilosa.HashBlockSize*2, 200, nil, 0); err != nil {
		t.Fatal(err)
	}

	// Ensure new checksum is different.
	if chksum := f.Checksum(); bytes.Equal(chksum, orig) {
		t.Fatalf("expected checksum to change: %x", chksum, orig)
	}
}

// Ensure fragment can return a checksum for a given block.
func TestFragment_Blocks(t *testing.T) {
	f := MustOpenFragment("d", "f", 0)
	defer f.Close()

	// Retrieve initial checksum.
	var prev []pilosa.FragmentBlock

	// Set first bit.
	if _, err := f.SetBit(0, 0, nil, 0); err != nil {
		t.Fatal(err)
	}
	blocks := f.Blocks()
	if blocks[0].Checksum == nil {
		t.Fatalf("expected checksum: %x", blocks[0].Checksum)
	}
	prev = blocks

	// Set bit on different bitmap.
	if _, err := f.SetBit(20, 0, nil, 0); err != nil {
		t.Fatal(err)
	}
	blocks = f.Blocks()
	if bytes.Equal(blocks[0].Checksum, prev[0].Checksum) {
		t.Fatalf("expected checksum to change: %x", blocks[0].Checksum)
	}
	prev = blocks

	// Set bit on different profile.
	if _, err := f.SetBit(20, 100, nil, 0); err != nil {
		t.Fatal(err)
	}
	blocks = f.Blocks()
	if bytes.Equal(blocks[0].Checksum, prev[0].Checksum) {
		t.Fatalf("expected checksum to change: %x", blocks[0].Checksum)
	}
}

// Ensure fragment returns an empty checksum if no data exists for a block.
func TestFragment_Blocks_Empty(t *testing.T) {
	f := MustOpenFragment("d", "f", 0)
	defer f.Close()

	// Set bits on a different block.
	if _, err := f.SetBit(100, 1, nil, 0); err != nil {
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
	f := MustOpenFragment("d", "f", 0)
	defer f.Close()

	// Set bits on the fragment.
	for i := uint64(0); i < 1000; i++ {
		if _, err := f.SetBit(i, 0, nil, 0); err != nil {
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
	f := MustOpenFragment("d", "f.n", 0)
	defer f.Close()

	// Set bits on the fragment.
	for i := uint64(0); i < 1000; i++ {
		if _, err := f.SetBit(i, 0, nil, 0); err != nil {
			t.Fatal(err)
		}
	}

	// Verify correct cache type and size.
	if cache, ok := f.Cache().(*pilosa.RankCache); !ok {
		t.Fatalf("unexpected cache: %T", f.Cache())
	} else if cache.Len() != 1000 {
		t.Fatalf("unexpected cache len: %d", cache.Len())
	}

	// Reopen the fragment.
	if err := f.Reopen(); err != nil {
		t.Fatal(err)
	}

	// Re-verify correct cache type and size.
	if cache, ok := f.Cache().(*pilosa.RankCache); !ok {
		t.Fatalf("unexpected cache: %T", f.Cache())
	} else if cache.Len() != 1000 {
		t.Fatalf("unexpected cache len: %d", cache.Len())
	}
}

// Ensure a fragment can be copied to another fragment.
func TestFragment_WriteTo_ReadFrom(t *testing.T) {
	f0 := MustOpenFragment("d", "f", 0)
	defer f0.Close()

	// Set and then clear bits on the fragment.
	if _, err := f0.SetBit(1000, 1, nil, 0); err != nil {
		t.Fatal(err)
	} else if _, err := f0.SetBit(1000, 2, nil, 0); err != nil {
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
	f1 := MustOpenFragment("d", "f", 0)
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
	if a := f1.Bitmap(1000).Bits(); !reflect.DeepEqual(a, []uint64{2}) {
		t.Fatalf("unexpected bits: %+v", a)
	}

	// Close and reopen the fragment & verify the data.
	if err := f1.Reopen(); err != nil {
		t.Fatal(err)
	} else if n := f1.Cache().Len(); n != 1 {
		t.Fatalf("unexpected cache size (reopen): %d", n)
	} else if a := f1.Bitmap(1000).Bits(); !reflect.DeepEqual(a, []uint64{2}) {
		t.Fatalf("unexpected bits (reopen): %+v", a)
	}
}

/*
func BenchmarkFragment_BlockChecksum_Fill1(b *testing.B)  { benchmarkFragmentBlockChecksum(b, 0.01) }
func BenchmarkFragment_BlockChecksum_Fill10(b *testing.B) { benchmarkFragmentBlockChecksum(b, 0.10) }
func BenchmarkFragment_BlockChecksum_Fill50(b *testing.B) { benchmarkFragmentBlockChecksum(b, 0.50) }

func benchmarkFragmentBlockChecksum(b *testing.B, fillPercent float64) {
	f := MustOpenFragment("d", "f", 0)
	defer f.Close()

	// Fill fragment.
	bitmapIDs, profileIDs := GenerateImportFill(pilosa.HashBlockSize, fillPercent)
	if err := f.Import(bitmapIDs, profileIDs); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	// Calculate block checksum.
	for i := 0; i < b.N; i++ {
		f.InvalidateChecksums()

		if chksum := f.BlockChecksum(0); chksum == nil {
			b.Fatal("expected checksum")
		}
	}
}
*/

func BenchmarkFragment_Blocks(b *testing.B) {
	if *FragmentPath == "" {
		b.Skip("no fragment specified")
	}

	// Open the fragment specified by the path.
	f := pilosa.NewFragment(*FragmentPath, "d", "f", 0)
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
	f := MustOpenFragment("d", "f", 0)
	defer f.Close()
	f.MaxOpN = math.MaxInt32

	// Generate some intersecting data.
	for i := 0; i < 10000; i += 2 {
		if _, err := f.SetBit(1, uint64(i), nil, 0); err != nil {
			b.Fatal(err)
		}
	}
	for i := 0; i < 10000; i += 3 {
		if _, err := f.SetBit(2, uint64(i), nil, 0); err != nil {
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
		if n := f.Bitmap(1).IntersectionCount(f.Bitmap(2)); n == 0 {
			b.Fatalf("unexpected count: %d", n)
		}
	}
}

// Fragment is a test wrapper for pilosa.Fragment.
type Fragment struct {
	*pilosa.Fragment
	BitmapAttrStore *AttrStore
}

// NewFragment returns a new instance of Fragment with a temporary path.
func NewFragment(db, frame string, slice uint64) *Fragment {
	file, err := ioutil.TempFile("", "pilosa-fragment-")
	if err != nil {
		panic(err)
	}
	file.Close()

	f := &Fragment{
		Fragment:        pilosa.NewFragment(file.Name(), db, frame, slice),
		BitmapAttrStore: MustOpenAttrStore(),
	}
	f.Fragment.BitmapAttrStore = f.BitmapAttrStore.AttrStore
	return f
}

// MustOpenFragment creates and opens an fragment at a temporary path. Panic on error.
func MustOpenFragment(db, frame string, slice uint64) *Fragment {
	f := NewFragment(db, frame, slice)
	if err := f.Open(); err != nil {
		panic(err)
	}
	return f
}

// Close closes the fragment and removes all underlying data.
func (f *Fragment) Close() error {
	defer os.Remove(f.Path())
	defer os.Remove(f.CachePath())
	defer f.BitmapAttrStore.Close()
	return f.Fragment.Close()
}

// Reopen closes the fragment and reopens it as a new instance.
func (f *Fragment) Reopen() error {
	path := f.Path()
	if err := f.Fragment.Close(); err != nil {
		return err
	}

	f.Fragment = pilosa.NewFragment(path, f.DB(), f.Frame(), f.Slice())
	f.Fragment.BitmapAttrStore = f.BitmapAttrStore.AttrStore
	if err := f.Open(); err != nil {
		return err
	}
	return nil
}

// MustSetBits sets bits on a bitmap. Panic on error.
// This function does not accept a timestamp or quantum.
func (f *Fragment) MustSetBits(bitmapID uint64, profileIDs ...uint64) {
	for _, profileID := range profileIDs {
		if _, err := f.SetBit(bitmapID, profileID, nil, 0); err != nil {
			panic(err)
		}
	}
}

// MustClearBits clears bits on a bitmap. Panic on error.
func (f *Fragment) MustClearBits(bitmapID uint64, profileIDs ...uint64) {
	for _, profileID := range profileIDs {
		if _, err := f.ClearBit(bitmapID, profileID); err != nil {
			panic(err)
		}
	}
}

// BitmapAttrStore provides simple storage for attributes.
type BitmapAttrStore struct {
	attrs map[uint64]map[string]interface{}
}

// NewBitmapAttrStore returns a new instance of BitmapAttrStore.
func NewBitmapAttrStore() *BitmapAttrStore {
	return &BitmapAttrStore{
		attrs: make(map[uint64]map[string]interface{}),
	}
}

// BitmapAttrs returns the attributes set to a bitmap id.
func (s *BitmapAttrStore) BitmapAttrs(id uint64) (map[string]interface{}, error) {
	return s.attrs[id], nil
}

// SetBitmapAttrs assigns a set of attributes to a bitmap id.
func (s *BitmapAttrStore) SetBitmapAttrs(id uint64, m map[string]interface{}) {
	s.attrs[id] = m
}

// GenerateImportFill generates a set of bits pairs that evenly fill a fragment chunk.
func GenerateImportFill(bitmapN int, pct float64) (bitmapIDs, profileIDs []uint64) {
	ipct := int(pct * 100)
	for i := 0; i < SliceWidth*bitmapN; i++ {
		if i%100 >= ipct {
			continue
		}

		bitmapIDs = append(bitmapIDs, uint64(i%SliceWidth))
		profileIDs = append(profileIDs, uint64(i/SliceWidth))
	}
	return
}
