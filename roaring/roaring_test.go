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

func TestBitmap_Quick_Array1(t *testing.T)     { testBitmapQuick(t, 1000, 1000, 2000) }
func TestBitmap_Quick_Array2(t *testing.T)     { testBitmapQuick(t, 10000, 0, 1000) }
func TestBitmap_Quick_Bitmap1(t *testing.T)    { testBitmapQuick(t, 10000, 0, 10000) }
func TestBitmap_Quick_Bitmap2(t *testing.T)    { testBitmapQuick(t, 10000, 10000, 20000) }
func TestBitmap_Quick_LargeValue(t *testing.T) { testBitmapQuick(t, 10000, 0, math.MaxUint32) }

// Ensure a bitmap can perform basic operations on randomly generated values.
func testBitmapQuick(t *testing.T, n int, min, max uint32) {
	quick.Check(func(a []uint32) bool {
		bm := roaring.NewBitmap()
		m := make(map[uint32]struct{})

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
		if got, exp := bm.Slice(), uint32SetSlice(m); !reflect.DeepEqual(got, exp) {
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
			values[0] = reflect.ValueOf(GenerateUint32Slice(n, min, max, rand))
		},
	})
}

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
	if a := roaring.NewBitmap(1, 2, 3).Slice(); !reflect.DeepEqual(a, []uint32{1, 2, 3}) {
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
	if a := roaring.NewBitmap(0, 1000001, 1000002, 1000003).SliceRange(1, 1000003); !reflect.DeepEqual(a, []uint32{1000001, 1000002}) {
		t.Fatalf("unexpected slice: %+v", a)
	}
}

// Ensure a bitmap can loop over a set of values.
func TestBitmap_ForEach(t *testing.T) {
	var a []uint32
	roaring.NewBitmap(1, 2, 3).ForEach(func(v uint32) {
		a = append(a, v)
	})
	if !reflect.DeepEqual(a, []uint32{1, 2, 3}) {
		t.Fatalf("unexpected values: %+v", a)
	}
}

// Ensure a bitmap can loop over a set of values in a range.
func TestBitmap_ForEachRange(t *testing.T) {
	var a []uint32
	roaring.NewBitmap(1, 2, 3, 4).ForEachRange(2, 4, func(v uint32) {
		a = append(a, v)
	})
	if !reflect.DeepEqual(a, []uint32{2, 3}) {
		t.Fatalf("unexpected values: %+v", a)
	}
}

func TestBitmap_Marshal_Quick_Array1(t *testing.T)  { testBitmapMarshalQuick(t, 1000, 1000, 2000) }
func TestBitmap_Marshal_Quick_Array2(t *testing.T)  { testBitmapMarshalQuick(t, 10000, 0, 1000) }
func TestBitmap_Marshal_Quick_Bitmap1(t *testing.T) { testBitmapMarshalQuick(t, 10000, 0, 10000) }
func TestBitmap_Marshal_Quick_Bitmap2(t *testing.T) { testBitmapMarshalQuick(t, 10000, 10000, 20000) }
func TestBitmap_Marshal_Quick_LargeValue(t *testing.T) {
	testBitmapMarshalQuick(t, 100, 0, math.MaxUint32)
}

// Ensure a bitmap can be marshaled and unmarshaled.
func testBitmapMarshalQuick(t *testing.T, n int, min, max uint32) {
	quick.Check(func(a0, a1 []uint32) bool {
		println("=================================================")

		// Create bitmap with initial values set.
		bm := roaring.NewBitmap(a0...)

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
			if err := bm.Add(v); err != nil {
				t.Fatal(err)
			}

			// Create new bitmap from ops log data.
			bm2 := roaring.NewBitmap()
			if err := bm2.UnmarshalBinary(buf.Bytes()); err != nil {
				t.Fatal(err)
			}

			// Verify the two bitmaps match.
			if x, y := bm.Slice(), bm2.Slice(); !reflect.DeepEqual(x, y) {
				t.Fatalf("mismatch: %s\n\nbm1=%+v\n\nbm2=%+v\n\n", diff(x, y), x, y)
			}
		}

		return true
	}, &quick.Config{
		Values: func(values []reflect.Value, rand *rand.Rand) {
			values[0] = reflect.ValueOf(GenerateUint32Slice(n, min, max, rand))
			values[1] = reflect.ValueOf(GenerateUint32Slice(100, min, max, rand))
		},
	})
}

// GenerateUint32Slice generates between [0, n) random uint32 numbers between min and max.
func GenerateUint32Slice(n int, min, max uint32, rand *rand.Rand) []uint32 {
	a := make([]uint32, rand.Intn(n))
	for i := range a {
		a[i] = min + uint32(rand.Intn(int(max-min)))
	}
	return a
}

// uint32SetSlice returns the values in a uint32 set.
func uint32SetSlice(m map[uint32]struct{}) []uint32 {
	a := make([]uint32, 0, len(m))
	for v := range m {
		a = append(a, v)
	}
	sort.Sort(uint32Slice(a))
	return a
}

// uint32Slice represents a sortable slice of uint32 numbers.
type uint32Slice []uint32

func (p uint32Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p uint32Slice) Len() int           { return len(p) }
func (p uint32Slice) Less(i, j int) bool { return p[i] < p[j] }

func diff(a, b []uint32) string {
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
