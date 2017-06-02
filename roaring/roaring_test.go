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

package roaring_test

import (
	"fmt"
	"bytes"
	"math"
	"math/rand"
	"reflect"
	"sort"
	"testing"
	"testing/quick"

	"github.com/pilosa/pilosa/roaring"
	_ "github.com/pilosa/pilosa/test"
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

// Ensure bitmap can return the highest value.
func TestBitmap_Max(t *testing.T) {
	bm := roaring.NewBitmap()
	for i := uint64(1000); i <= 100000; i++ {
		bm.Add(i)

		if v := bm.Max(); v != i {
			t.Fatalf("max: got=%d; want=%d", v, i)
		}
	}
}

func TestBitmap_Intersection(t *testing.T) {
	bm0 := roaring.NewBitmap(0, 2683177)
	bm1 := roaring.NewBitmap()
	for i := uint64(628); i < 2683301; i++ {
		bm1.Add(i)
	}

	result := bm0.Intersect(bm1)
	if n := result.Count(); n != 1 {
		t.Fatalf("unexpected n: %d", n)
	}

}

func TestBitmap_Difference(t *testing.T) {
	bm0 := roaring.NewBitmap(0, 2683177)
	bm1 := roaring.NewBitmap()
	for i := uint64(628); i < 2683301; i++ {
		bm1.Add(i)
	}
	result := bm0.Difference(bm1)
	//expect to have just 0
	if n := result.Count(); n != 1 {
		t.Fatalf("unexpected n: %d", n)
	}
}

func TestBitmap_Union(t *testing.T) {
	bm0 := roaring.NewBitmap(0, 1000001, 1000002, 1000003)
	bm1 := roaring.NewBitmap(0, 50000, 1000001, 1000002)
	result := bm0.Union(bm1)
	if n := result.Count(); n != 5 {
		t.Fatalf("unexpected n: %d", n)
	}
}

func TestBitmap_Xor_ArrayArray(t *testing.T) {
	bm0 := roaring.NewBitmap(0, 1000001, 1000002, 1000003)
	bm1 := roaring.NewBitmap(0, 50000, 1000001, 1000002)
	result := bm0.Xor(bm1)
	if n := result.Count(); n != 2 {
		t.Fatalf("unexpected n: %d", n)
	}

	//equivalence array test
	result = result.Xor(result)
	if n := result.Count(); n > 0 {
		t.Fatalf("unexpected n: %d", n)
	}

}

//empty array test
func TestBitmap_Xor_Empty(t *testing.T) {
	bm1 := roaring.NewBitmap(0, 50000, 1000001, 1000002)
	empty := roaring.NewBitmap()
	result := bm1.Xor(empty)

	if n := result.Count(); n != 4 {
		t.Fatalf("unexpected n: %d", n)
	}
}
func TestBitmap_Xor_ArrayBitmap(t *testing.T) {
	bm0 := roaring.NewBitmap(1, 70, 200, 4097, 4098)
	bm1 := roaring.NewBitmap()
	for i := uint64(0); i < 10000; i += 2 {
		bm1.Add(i)
	}

	result := bm0.Xor(bm1)
	if n := result.Count(); n != 4999 {
		t.Fatalf("unexpected n: %d", n)
	}

	//equivalence bitmap test
	result = result.Xor(result)
	if n := result.Count(); n > 0 {
		t.Fatalf("unexpected n: %d", n)
	}

	empty := roaring.NewBitmap()
	result = bm1.Xor(empty)
	if n := result.Count(); n != 5000 {
		t.Fatalf("unexpected n: %d", n)
	}
}

func TestBitmap_Xor_BitmapBitmap(t *testing.T) {
	bm0 := roaring.NewBitmap()
	bm1 := roaring.NewBitmap()

	for i := uint64(0); i < 10000; i += 2 {
		bm1.Add(i)
	}

	for i := uint64(1); i < 10000; i += 2 {
		bm0.Add(i)
	}

	result := bm0.Xor(bm1)
	if n := result.Count(); n != 10000 {
		t.Fatalf("unexpected n: %d", n)
	}
}

// Ensure bitmap contents alternate.
func TestBitmap_Flip_Empty(t *testing.T) {
	bm := roaring.NewBitmap()
	results := bm.Flip(0, 10)
	if n := results.Count(); n != 11 {
		t.Fatalf("unexpected n: %d", n)
	}
	results = results.Flip(0, 10)
	if n := results.Count(); n != 0 {
		t.Fatalf("unexpected n: %d", n)
	}
}

// Test Subrange Flip should not affect bits outside of Range
func TestBitmap_Flip_Array(t *testing.T) {
	bm := roaring.NewBitmap(0, 1, 2, 3, 4, 8, 16, 32, 64, 128, 256, 512, 1024)
	results := bm.Flip(0, 4)
	if !reflect.DeepEqual(results.Slice(), []uint64{8, 16, 32, 64, 128, 256, 512, 1024}) {
		t.Fatalf("unexpected %v ", results.Slice())
	}
	results = results.Flip(0, 4)
	if !reflect.DeepEqual(results.Slice(), []uint64{0, 1, 2, 3, 4, 8, 16, 32, 64, 128, 256, 512, 1024}) {
		t.Fatalf("unexpected %v ", results.Slice())
	}

}

// Ensure  Flip works with underlying Bitmap container.
func TestBitmap_Flip_Bitmap(t *testing.T) {
	bm := roaring.NewBitmap()
	size := uint64(10000)
	for i := uint64(0); i < size; i += 2 {
		bm.Add(i)
	}
	results := bm.Flip(0, size-1)
	if n := results.Count(); n != size/2 {
		t.Fatalf("unexpected n: %d", n)
	}
	results = results.Flip(0, size-1) //flipping back should be the same
	if n := results.Count(); n != size/2 {
		t.Fatalf("unexpected n: %d", n)
	}
}

// Verify Flip works correctly with in different regions of bitmap, beginning, middle, and end.
func TestBitmap_Flip_After(t *testing.T) {
	bm := roaring.NewBitmap(0, 2, 4, 8)
	results := bm.Flip(9, 10)

	if !reflect.DeepEqual(results.Slice(), []uint64{0, 2, 4, 8, 9, 10}) {
		t.Fatalf("unexpected %v ", results.Slice())
	}
	results = results.Flip(0, 1)
	if !reflect.DeepEqual(results.Slice(), []uint64{1, 2, 4, 8, 9, 10}) {
		t.Fatalf("unexpected %v ", results.Slice())
	}
	results = results.Flip(4, 8)
	if !reflect.DeepEqual(results.Slice(), []uint64{1, 2, 5, 6, 7, 9, 10}) {
		t.Fatalf("unexpected %v ", results.Slice())
	}

}

// Ensure bitmap can return the number of intersecting bits in two bitmaps.
func TestBitmap_IntersectionCount_ArrayArray(t *testing.T) {
	bm0 := roaring.NewBitmap(0, 1000001, 1000002, 1000003)
	bm1 := roaring.NewBitmap(0, 50000, 1000001, 1000002)

	if n := bm0.IntersectionCount(bm1); n != 3 {
		t.Fatalf("unexpected n: %d", n)
	} else if n := bm1.IntersectionCount(bm0); n != 3 {
		t.Fatalf("unexpected n (reverse): %d", n)
	}
}

// Ensure bitmap can return the number of intersecting bits in two bitmaps.
func TestBitmap_IntersectionCount_ArrayBitmap(t *testing.T) {
	bm0 := roaring.NewBitmap(1, 70, 200, 4097, 4098)
	bm1 := roaring.NewBitmap()
	for i := uint64(0); i <= 10000; i += 2 {
		bm1.Add(i)
	}

	if n := bm0.IntersectionCount(bm1); n != 3 {
		t.Fatalf("unexpected n: %d", n)
	} else if n := bm1.IntersectionCount(bm0); n != 3 {
		t.Fatalf("unexpected n (reverse): %d", n)
	}
}

// Ensure bitmap can return the number of intersecting bits in two bitmaps.
func TestBitmap_IntersectionCount_BitmapBitmap(t *testing.T) {
	bm0 := roaring.NewBitmap()
	bm1 := roaring.NewBitmap()
	for i := uint64(0); i <= 10000; i += 2 {
		bm0.Add(i)
		bm1.Add(i + 1)
	}

	bm0.Add(1000)
	bm1.Add(1000)

	bm0.Add(2000)
	bm1.Add(2000)

	if n := bm0.IntersectionCount(bm1); n != 2 {
		t.Fatalf("unexpected n: %d", n)
	} else if n := bm1.IntersectionCount(bm0); n != 2 {
		t.Fatalf("unexpected n (reverse): %d", n)
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
		// If `got` is nil and `exp` has zero length, don't perform the DeepEqual
		// because when `a` is empty (`a = []uint64{}`) then `got` is a nil slice
		// while `exp` is an empty slice. Therefore they will not be considered equal.
		if got, exp := bm.Slice(), uint64SetSlice(m); !(got == nil && len(exp) == 0) && !reflect.DeepEqual(got, exp) {
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

// Ensure iterator can iterate over all the values on the bitmap.
func TestIterator(t *testing.T) {
	itr := roaring.NewBitmap(1, 2, 3).Iterator()
	itr.Seek(0)

	var a []uint64
	for v, eof := itr.Next(); !eof; v, eof = itr.Next() {
		a = append(a, v)
	}

	if !reflect.DeepEqual(a, []uint64{1, 2, 3}) {
		t.Fatalf("unexpected values: %+v", a)
	}
}

var benchmarkBitmapIntersectionCountData struct {
	a, b *roaring.Bitmap
}

func BenchmarkBitmap_IntersectionCount_ArrayBitmap(b *testing.B) {
	data := &benchmarkBitmapIntersectionCountData
	if data.a == nil {
		const max = (1 << 24) / 64

		// Build bitmap with array container.
		data.a = roaring.NewBitmap()
		for i, n := 0, rand.Intn(roaring.ArrayMaxSize); i < n; i++ {
			data.a.Add(uint64(rand.Intn(max)))
		}

		// Build bitmap with bitmap container.
		data.b = roaring.NewBitmap()
		for i, n := 0, roaring.ArrayMaxSize*2; i < n; i++ {
			data.b.Add(uint64(i * 3))
		}
	}

	// Reset timer & benchmark.
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data.a.IntersectionCount(data.b)
	}
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
