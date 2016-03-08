package roaring_test

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"sort"
	"testing"
	"testing/quick"

	"github.com/umbel/pilosa/roaring"
)

// Ensure an empty bitmap returns false if checking for existence.
func TestBitmap_Contains_Empty(t *testing.T) {
	if roaring.NewBitmap().Contains(1000) {
		t.Fatal("expected false")
	}
}

// Ensure an empty bitmap does nothing when removing an element.
func TestBitmap_Remove_Empty(t *testing.T) {
	roaring.NewBitmap().Remove(1000)
}

// Ensure a bitmap can return a slice of values.
func TestBitmap_Slice(t *testing.T) {
	if a := roaring.NewBitmap(1, 2, 3).Slice(); !reflect.DeepEqual(a, []uint64{1, 2, 3}) {
		t.Fatalf("unexpected slice: %+v", a)
	}
}

// Ensure an empty bitmap returns an empty slice of values.
func TestBitmap_Slice_Empty(t *testing.T) {
	if a := roaring.NewBitmap().Slice(); len(a) != 0 {
		t.Fatalf("unexpected slice: %+v", a)
	}
}

// Ensure a bitmap can return a slice of values within a range.
func TestBitmap_SliceRange(t *testing.T) {
	if a := roaring.NewBitmap(0, 1000001, 1000002, 1000003).SliceRange(1, 1000003); !reflect.DeepEqual(a, []uint64{1000001, 1000002}) {
		t.Fatalf("unexpected slice: %+v", a)
	}
}

// Ensure a bitmap can loop over a set of values.
func TestBitmap_ForEach(t *testing.T) {
	var a []uint64
	roaring.NewBitmap(1, 2, 3).ForEach(func(v uint64) {
		a = append(a, v)
	})
	if !reflect.DeepEqual(a, []uint64{1, 2, 3}) {
		t.Fatalf("unexpected values: %+v", a)
	}
}

// Ensure a bitmap can loop over a set of values in a range.
func TestBitmap_ForEachRange(t *testing.T) {
	var a []uint64
	roaring.NewBitmap(1, 2, 3, 4).ForEachRange(2, 4, func(v uint64) {
		a = append(a, v)
	})
	if !reflect.DeepEqual(a, []uint64{2, 3}) {
		t.Fatalf("unexpected values: %+v", a)
	}
}

func TestBitmap_Quick_Array1(t *testing.T)     { testBitmapQuick(t, 1000, 1000, 2000) }
func TestBitmap_Quick_Array2(t *testing.T)     { testBitmapQuick(t, 10000, 0, 1000) }
func TestBitmap_Quick_Bitmap1(t *testing.T)    { testBitmapQuick(t, 10000, 0, 10000) }
func TestBitmap_Quick_Bitmap2(t *testing.T)    { testBitmapQuick(t, 10000, 10000, 20000) }
func TestBitmap_Quick_LargeValue(t *testing.T) { testBitmapQuick(t, 10000, 0, math.MaxInt64) }

// Ensure a bitmap can perform basic operations on randomly generated values.
func testBitmapQuick(t *testing.T, n int, min, max uint64) {
	quick.Check(func(a []uint64) bool {
		bm := roaring.NewBitmap()
		m := make(map[uint64]struct{})

		// Add values to the bitmap and set.
		for _, v := range a {
			bm.Add(v)
			m[v] = struct{}{}
		}

		// Verify existence.
		for _, v := range a {
			// Check for individual value.
			if !bm.Contains(v) {
				t.Fatalf("expected bitmap to contain: %d", v)
			}

			// Check for next value (which may or may not exist).
			if _, ok := m[v+1]; bm.Contains(v+1) != ok {
				t.Fatalf("unexpected return from Contains(%d): %v", v+1, bm.Contains(v+1))
			}
		}

		// Verify slices are equal.
		if got, exp := bm.Slice(), uint64SetSlice(m); !reflect.DeepEqual(got, exp) {
			t.Fatalf("unexpected values:\n\ngot=%+v\n\nexp=%+v\n\n", got, exp)
		}

		// Remove all values in random order.
		for _, i := range rand.Perm(len(a)) {
			bm.Remove(a[i])
		}

		// Verify all values have been removed.
		if slice := bm.Slice(); len(slice) != 0 {
			t.Fatalf("expected no values, got: %+v", slice)
		}

		return true
	}, &quick.Config{
		Values: func(values []reflect.Value, rand *rand.Rand) {
			values[0] = reflect.ValueOf(GenerateUint64Slice(n, min, max, false, rand))
		},
	})
}

func TestBitmap_Marshal_Quick_Array1(t *testing.T)  { testBitmapMarshalQuick(t, 1000, 1000, 2000, false) }
func TestBitmap_Marshal_Quick_Array2(t *testing.T)  { testBitmapMarshalQuick(t, 10000, 0, 1000, false) }
func TestBitmap_Marshal_Quick_Bitmap1(t *testing.T) { testBitmapMarshalQuick(t, 10000, 0, 10000, false) }
func TestBitmap_Marshal_Quick_Bitmap2(t *testing.T) {
	testBitmapMarshalQuick(t, 10000, 10000, 20000, false)
}
func TestBitmap_Marshal_Quick_LargeValue(t *testing.T) {
	testBitmapMarshalQuick(t, 100, 0, math.MaxInt64, false)
}

func TestBitmap_Marshal_Quick_Bitmap_Sorted(t *testing.T) {
	testBitmapMarshalQuick(t, 10000, 0, 10000, true)
}

// Ensure a bitmap can be marshaled and unmarshaled.
func testBitmapMarshalQuick(t *testing.T, n int, min, max uint64, sorted bool) {
	if testing.Short() {
		t.Skip("short")
	}

	quick.Check(func(a0, a1 []uint64) bool {
		// Create bitmap with initial values set.
		bm := roaring.NewBitmap(a0...)

		set := make(map[uint64]struct{})
		for _, v := range a0 {
			set[v] = struct{}{}
		}

		// Write snapshot to buffer.
		var buf bytes.Buffer
		if n, err := bm.WriteTo(&buf); err != nil {
			t.Fatal(err)
		} else if n != int64(buf.Len()) {
			t.Fatalf("size mismatch: %d != %d", n, buf.Len())
		}

		// Set buffer as the writer for the ops log.
		bm.OpWriter = &buf

		// Add more values to bitmap.
		for _, v := range a1 {
			set[v] = struct{}{}
			if _, err := bm.Add(v); err != nil {
				t.Fatal(err)
			}

			// Extract buffer as a byte slice so it can be mapped.
			data := buf.Bytes()

			// Create new bitmap from ops log data.
			bm2 := roaring.NewBitmap()
			if err := bm2.UnmarshalBinary(data); err != nil {
				t.Fatal(err)
			}

			// Verify the original bitmap has the correct set of values.
			if exp, got := uint64SetSlice(set), bm.Slice(); !reflect.DeepEqual(exp, got) {
				t.Fatalf("mismatch: %s\n\nexp=%+v\n\ngot=%+v\n\n", diff(exp, got), exp, got)
			}

			// Verify the bitmap loaded with the ops log has the correct set of values.
			if exp, got := uint64SetSlice(set), bm2.Slice(); !reflect.DeepEqual(exp, got) {
				t.Fatalf("mismatch: %s\n\nexp=%+v\n\ngot=%+v\n\n", diff(exp, got), exp, got)
			}
		}

		return true
	}, &quick.Config{
		Values: func(values []reflect.Value, rand *rand.Rand) {
			values[0] = reflect.ValueOf(GenerateUint64Slice(n, min, max, sorted, rand))
			values[1] = reflect.ValueOf(GenerateUint64Slice(100, min, max, sorted, rand))
		},
	})
}

// GenerateUint64Slice generates between [0, n) random uint64 numbers between min and max.
func GenerateUint64Slice(n int, min, max uint64, sorted bool, rand *rand.Rand) []uint64 {
	a := make([]uint64, rand.Intn(n))
	for i := range a {
		a[i] = min + uint64(rand.Int63n(int64(max-min)))
	}

	if sorted {
		sort.Sort(uint64Slice(a))
	}

	return a
}

// uint64SetSlice returns the values in a uint64 set.
func uint64SetSlice(m map[uint64]struct{}) []uint64 {
	a := make([]uint64, 0, len(m))
	for v := range m {
		a = append(a, v)
	}
	sort.Sort(uint64Slice(a))
	return a
}

// uint64Slice represents a sortable slice of uint64 numbers.
type uint64Slice []uint64

func (p uint64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p uint64Slice) Len() int           { return len(p) }
func (p uint64Slice) Less(i, j int) bool { return p[i] < p[j] }

func diff(a, b []uint64) string {
	if len(a) != len(b) {
		return fmt.Sprintf("len: %d != %d", len(a), len(b))
	}
	for i := range a {
		if a[i] != b[i] {
			return fmt.Sprintf("index %d: %d != %d", i, a[i], b[i])
		}
	}
	return ""
}
