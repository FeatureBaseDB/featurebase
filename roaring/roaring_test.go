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
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"sort"
	"testing"
	"testing/quick"

	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/roaring"
	_ "github.com/pilosa/pilosa/test"
)

func TestBitmapClone(t *testing.T) {
	b := roaring.NewBitmap()
	for i := uint64(61000); i < 71000; i++ {
		b.Add(i)
	}
	c := b.Clone()
	if !reflect.DeepEqual(b, c) {
		t.Fatalf("Clone Objects not equal\n")
	}
	d := func() *roaring.Bitmap { //anybody know how to declare a nil value?
		return nil
	}()
	e := d.Clone()
	if e != nil {
		t.Fatalf("Clone nil Objects not equal\n")
	}
}

func TestContainerCount(t *testing.T) {
	b := roaring.NewBitmap(65535)

	if b.Count() != b.CountRange(0, 65546) {
		t.Fatalf("Count != CountRange\n")
	}
}

func TestCountRange(t *testing.T) {
	tests := []struct {
		name   string
		bitmap []uint64
		start  uint64
		end    uint64
		exp    uint64
	}{
		{
			name:   "j < 0 : 1",
			bitmap: []uint64{0, 1, 2, 3 * 65536},
			start:  0,
			end:    65536,
			exp:    3,
		},
		{
			name:   "i < 0 : 1",
			bitmap: []uint64{0, 1, 2, 2 * 65536, 3 * 65536},
			start:  65536,
			end:    3 * 65536,
			exp:    1,
		},
		{
			name:   "single-container-run",
			bitmap: []uint64{0, 2, 3, 4, 5, 2 * 65536, 3 * 65536},
			start:  2,
			end:    5,
			exp:    3,
		},
		{
			name:   "single-container-beg",
			bitmap: []uint64{1, 2, 3, 4, 5, 2 * 65536, 3 * 65536},
			start:  1,
			end:    4,
			exp:    3,
		},
		{
			name:   "partial-start",
			bitmap: []uint64{1, 2, 3, 4, 5, 2 * 65536, 3 * 65536},
			start:  5,
			end:    3 * 65536,
			exp:    2,
		},
		{
			name:   "partial-end",
			bitmap: []uint64{1, 2 * 65536, 3 * 65536, 3*65536 + 1, 3*65536 + 2},
			start:  0,
			end:    (3 * 65536) + 1,
			exp:    3,
		},
		{
			name:   "partial-both",
			bitmap: []uint64{65536, 65537, 65538, 2 * 65536, 2*65536 + 1, 2*65536 + 2},
			start:  65537,
			end:    (2 * 65536) + 1,
			exp:    3,
		},
		{
			name:   "partial-both-bookends",
			bitmap: []uint64{0, 65535, 65536, 65537, 65538, 2 * 65536, 2*65536 + 1, 2*65536 + 2, 3 * 65536},
			start:  65537,
			end:    (2 * 65536) + 1,
			exp:    3,
		},
		{
			name:   "empty-bookends",
			bitmap: []uint64{1, 65535, 5 * 65536, 5*65536 + 1},
			start:  65536,
			end:    5 * 65536,
			exp:    0,
		},
		{
			name:   "i not found, j found",
			bitmap: []uint64{1, 65535, 5 * 65536},
			start:  2 * 65535,
			end:    5*65536 + 1,
			exp:    1,
		},
		{
			name:   "i not found, j not found",
			bitmap: []uint64{1, 65535, 5 * 65536, 7 * 65536},
			start:  2 * 65535,
			end:    6 * 65536,
			exp:    1,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%s: %d to %d in '%v'", test.name, test.start, test.end, test.bitmap), func(t *testing.T) {
			b := roaring.NewBitmap(test.bitmap...)
			actual := b.CountRange(test.start, test.end)
			if actual != test.exp {
				t.Errorf("got: %d, exp: %d", actual, test.exp)
			}
		})
	}
}

func TestCheckBitmap(t *testing.T) {
	b := roaring.NewBitmap()
	x := 0
	for i := uint64(61000); i < 71000; i++ {
		x++
		b.Add(i)
	}
	for i := uint64(75000); i < 75100; i++ {
		x++
		b.Add(i)
	}
	err := b.Check()
	if err != nil {
		t.Fatalf("%v\n", err)
	}
}

func TestCheckArray(t *testing.T) {
	b := roaring.NewBitmap(0, 1, 10, 100, 1000, 10000, 90000, 100000)
	err := b.Check()
	if err != nil {
		t.Fatalf("%v\n", err)
	}
}

func TestCheckRun(t *testing.T) {
	b := roaring.NewBitmap(0, 1, 2, 3, 4, 5, 1000, 1001, 1002, 1003, 1004, 1005, 100000, 100001, 100002, 100003, 100004, 100005)
	b.Optimize() // convert to runs
	err := b.Check()
	if err != nil {
		t.Fatalf("%v\n", err)
	}
}
func TestCheckFullRun(t *testing.T) {
	b := roaring.NewBitmap()
	for i := uint64(0); i < 2097152; i++ {
		if i%16384 == 0 {
			b.Optimize() // convert to runs
		}
		b.Add(i)
	}
	err := b.Check()
	if err != nil {
		t.Fatalf("Before %v\n", err)
	}
	b.Optimize() // convert to runs
	err = b.Check()
	if err != nil {
		t.Fatalf("After %v\n", err)
	}
}

// Ensure that we can transition between runs and arrays when materializing the bitmap.
func TestContainerTransitions(t *testing.T) {
	// [run, run][array][run]
	b := roaring.NewBitmap(0, 1, 2, 3, 4, 5, 1000, 1001, 1002, 1003, 1004, 1005, 100000, 100001, 100002, 132000, 132001, 132002, 132003, 132004, 132005)
	b.Optimize() // convert to runs
	if !reflect.DeepEqual(b.Slice(), []uint64{0, 1, 2, 3, 4, 5, 1000, 1001, 1002, 1003, 1004, 1005, 100000, 100001, 100002, 132000, 132001, 132002, 132003, 132004, 132005}) {
		t.Fatalf("unexpected slice: %+v", b.Slice())
	}

	// Test the case where last and first bits of adjoining containers are set.
	// [run][array][run]
	b2 := roaring.NewBitmap(65531, 65532, 65533, 65534, 65535, 65536, 131071, 131072, 131073, 131074, 131075, 131076)
	b2.Optimize() // convert to runs
	if !reflect.DeepEqual(b2.Slice(), []uint64{65531, 65532, 65533, 65534, 65535, 65536, 131071, 131072, 131073, 131074, 131075, 131076}) {
		t.Fatalf("unexpected slice: %+v", b2.Slice())
	}
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
// TODO duplicate for all container types
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

// Ensure CountRange is correct even if rangekey is prior to inital container.
func TestBitmap_BitmapCountRangeEdgeCase(t *testing.T) {
	s := uint64(2009 * 1048576)
	e := uint64(2010 * 1048576)

	start := s + (39314024 % 1048576)
	bm0 := roaring.NewBitmap()
	for i := uint64(0); i < 65536; i++ {
		if (i+1)%4096 == 0 {
			start += 16384
		} else {
			start += 2
		}
		bm0.Add(start)
	}
	a := bm0.Count()
	r := bm0.CountRange(s, e)
	if a != r {
		t.Fatalf("Counts != CountRange %v %v", a, r)

	}
}

func TestBitmap_BitmapCountRange(t *testing.T) {
	bm0 := roaring.NewBitmap(0, 2683177)
	for i := uint64(628); i < 2683301; i++ {
		bm0.Add(i)
	}
	bm0.Add(2683307)
	if n := bm0.CountRange(1, 2683311); n != 2682674 {
		t.Fatalf("unexpected n: %d", n)
	}

	if n := bm0.CountRange(2683177, 2683310); n != 125 {
		t.Fatalf("unexpected n: %d", n)
	}

	if n := bm0.CountRange(2683301, 3000000); n != 1 {
		t.Fatalf("unexpected n: %d", n)
	}

	if n := bm0.CountRange(0, 1); n != 1 {
		t.Fatalf("unexpected n: %d", n)
	}

	// Test the case where the range is outside of the bitmap space.
	if n := bm0.CountRange(10000000, 10000001); n != 0 {
		t.Fatalf("unexpected n: %d", n)
	}
}

func TestBitmap_ArrayCountRange(t *testing.T) {
	bm0 := roaring.NewBitmap(0, 2683177, 2683313)
	if n := bm0.CountRange(1, 2683313); n != 1 {
		t.Fatalf("unexpected n: %d", n)
	}
}

func TestBitmap_RunCountRange(t *testing.T) {
	bm0 := roaring.NewBitmap(0, 1, 2, 3, 4, 5, 12, 13, 14, 15, 16, 17, 1000000, 1000002, 1000003, 1000004, 1000005, 1000006, 1000010, 1000011, 1000012, 1000013, 1000014)
	bm0.Optimize() // convert to runs
	if n := bm0.CountRange(15, 1000003); n != 5 {
		t.Fatalf("unexpected n: %d", n)
	}

	bm1 := roaring.NewBitmap(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17)
	bm1.Optimize() // convert to runs
	if n := bm1.CountRange(5, 12); n != 7 {
		t.Fatalf("unexpected n: %d", n)
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

func TestBitmap_Union1(t *testing.T) {
	bm0 := roaring.NewBitmap(0, 2683177)
	bm1 := roaring.NewBitmap()
	for i := uint64(628); i < 2683301; i++ {
		bm1.Add(i)
	}
	bm1.Add(4000000)

	result := bm0.Union(bm1)
	if n := result.Count(); n != 2682675 {
		t.Fatalf("unexpected n: %d", n)
	}
	bm := testBM()
	result = bm.Union(bm0)
	if n := result.Count(); n != 75009 {
		t.Fatalf("unexpected n: %d", n)
	}
	result = bm.Union(bm)
	if n := result.Count(); n != 75007 {
		t.Fatalf("unexpected n: %d", n)
	}

}

func TestBitmap_Intersection_Empty(t *testing.T) {
	bm0 := roaring.NewBitmap(0, 2683177)
	bm1 := roaring.NewBitmap()

	result := bm0.Intersect(bm1)
	if n := result.Count(); n != 0 {
		t.Fatalf("unexpected n: %d", n)
	}

}

func TestBitmap_IntersectArrayArray(t *testing.T) {
	bm0 := roaring.NewBitmap(0, 1, 2683, 5005)
	bm1 := roaring.NewBitmap(0, 2683, 2684, 5000)

	result := bm0.Intersect(bm1)
	if n := result.Count(); n != 2 {
		t.Fatalf("unexpected n: %d", n)
	}
}

func TestBitmap_IntersectBitmapBitmap(t *testing.T) {
	bm0 := roaring.NewBitmap()
	for i := uint64(0); i < 65536; i += 2 {
		bm0.Add(i)
	}

	bm1 := roaring.NewBitmap()
	for i := uint64(0); i < 65536; i += 3 {
		bm1.Add(i)
	}

	result := bm0.Intersect(bm1)
	if n := result.Count(); n != 10923 {
		t.Fatalf("unexpected n: %d", n)
	}
}

func TestBitmap_IntersectRunRun(t *testing.T) {
	// Intersect two runs that result in an array.
	bm0 := roaring.NewBitmap(0, 1, 2, 3, 4, 5, 10, 11, 12, 13, 14, 15)
	bm0.Optimize() // convert to runs
	bm1 := roaring.NewBitmap(5, 6, 7, 8, 9, 10, 11)
	bm1.Optimize() // convert to runs
	result := bm0.Intersect(bm1)
	if n := result.Count(); n != 3 {
		t.Fatalf("unexpected n: %d", n)
	}

	// Intersect two runs that result in a bitmap.
	bm2 := roaring.NewBitmap()
	runLen := uint64(25)
	spaceLen := uint64(8)
	offset := (runLen / 2) + spaceLen
	for i := uint64(0); i < (65536 - runLen - offset); i += (runLen + spaceLen) {
		for j := uint64(0); j < runLen; j++ {
			bm2.Add(offset + i + j)
		}
	}
	bm2.Optimize() // convert to runs
	bm3 := roaring.NewBitmap()
	runLen = uint64(32)
	spaceLen = uint64(1)
	for i := uint64(0); i < (65536 - runLen); i += (runLen + spaceLen) {
		for j := uint64(0); j < runLen; j++ {
			bm3.Add(i + j)
		}
	}
	bm3.Optimize() // convert to runs
	result = bm2.Intersect(bm3)
	if n := result.Count(); n != 47628 {
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
	if n := result.Count(); n != 1 {
		t.Fatalf("unexpected n: %d", n)
	}
}

func TestBitmap_Difference2(t *testing.T) {
	bm0 := roaring.NewBitmap(0, 1, 2, 131072, 262144, pilosa.SliceWidth+5, pilosa.SliceWidth+7)
	bm1 := roaring.NewBitmap(2, 3, 100000, 262144, 2*pilosa.SliceWidth+1)
	result := bm0.Difference(bm1)
	if !reflect.DeepEqual(result.Slice(), []uint64{0, 1, 131072, pilosa.SliceWidth + 5, pilosa.SliceWidth + 7}) {
		t.Fatalf("unexpected : %v", result.Slice())
	}
}

func TestBitmap_Difference_Empty(t *testing.T) {
	bm0 := roaring.NewBitmap(0, 2683177)
	bm1 := roaring.NewBitmap()
	result := bm0.Difference(bm1)
	if n := result.Count(); n != 2 {
		t.Fatalf("unexpected n: %d", n)
	}
}

func TestBitmap_DifferenceArrayArray(t *testing.T) {
	bm0 := roaring.NewBitmap(0, 4, 8, 12, 16, 20)
	bm1 := roaring.NewBitmap(1, 3, 6, 9, 12, 15, 18)
	result := bm0.Difference(bm1)
	if n := result.Count(); n != 5 {
		t.Fatalf("unexpected n: %d", n)
	}
}

func TestBitmap_DifferenceArrayRun(t *testing.T) {
	bm0 := roaring.NewBitmap(0, 4, 8, 12, 16, 20, 36, 40, 44)

	bm1 := roaring.NewBitmap(1, 2, 3, 4, 5, 6, 7, 8, 9, 30, 31, 32, 33, 34, 35, 36)
	bm1.Optimize() // convert to runs
	result := bm0.Difference(bm1)
	if n := result.Count(); n != 6 {
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

func TestBitmap_Xor(t *testing.T) {
	bm0 := testBM()
	bm1 := roaring.NewBitmap(0, 1, 2, 3)
	result := bm1.Xor(bm0)
	if n := result.Count(); n != 75011 {
		t.Fatalf("unexpected n: %d", n)
	}

	result = bm0.Xor(bm1)
	if n := result.Count(); n != 75011 {
		t.Fatalf("unexpected n: %d", n)
	}

	result = bm0.Xor(bm0)
	if n := result.Count(); n != 0 {
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
		t.Fatalf("test #1 unexpected n: %d", n)
	}

	result = bm1.Xor(bm0)
	if n := result.Count(); n != 4999 {
		t.Fatalf("test #2 unexpected n: %d", n)
	}

	//equivalence bitmap test
	result = result.Xor(result)
	if n := result.Count(); n > 0 {
		t.Fatalf("test 3 unexpected n: %d", n)
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
	bm0 := roaring.NewBitmap(0, 1, 1000001, 1000002, 1000003)
	bm1 := roaring.NewBitmap(0, 50000, 1000001, 1000002)

	if n := bm0.IntersectionCount(bm1); n != 3 {
		t.Fatalf("unexpected n: %d", n)
	} else if n := bm1.IntersectionCount(bm0); n != 3 {
		t.Fatalf("unexpected n (reverse): %d", n)
	}
}

// Ensure bitmap can return the number of intersecting bits in two bitmaps.
func TestBitmap_IntersectionCount_ArrayRun(t *testing.T) {
	bm0 := roaring.NewBitmap(0, 1000001, 1000002, 1000003)
	bm1 := roaring.NewBitmap(0, 1, 2, 3, 4, 5, 1000000, 1000002, 1000003, 1000004, 1000005, 1000006)
	bm1.Optimize() // convert to runs

	if n := bm0.IntersectionCount(bm1); n != 3 {
		t.Fatalf("unexpected n: %d", n)
	} else if n := bm1.IntersectionCount(bm0); n != 3 {
		t.Fatalf("unexpected n (reverse): %d", n)
	}
}

// Ensure bitmap can return the number of intersecting bits in two bitmaps.
func TestBitmap_IntersectionCount_RunRun(t *testing.T) {
	bm0 := roaring.NewBitmap(3, 4, 5, 6, 7, 8, 1000001, 1000002, 1000003, 1000004)
	bm0.Optimize() // convert to runs
	bm1 := roaring.NewBitmap(0, 1, 2, 3, 4, 5, 1000000, 1000002, 1000003, 1000004, 1000005, 1000006)
	bm1.Optimize() // convert to runs

	if n := bm0.IntersectionCount(bm1); n != 6 {
		t.Fatalf("unexpected n: %d", n)
	} else if n := bm1.IntersectionCount(bm0); n != 6 {
		t.Fatalf("unexpected n (reverse): %d", n)
	}
}

// Ensure bitmap can return the number of intersecting bits in two bitmaps.
func TestBitmap_IntersectionCount_BitmapRun(t *testing.T) {
	bm0 := roaring.NewBitmap()
	for i := uint64(3); i <= 1000006; i += 2 {
		bm0.Add(i)
	}
	bm1 := roaring.NewBitmap(0, 1, 2, 3, 4, 5, 1000000, 1000002, 1000003, 1000004, 1000005, 1000006)
	bm1.Optimize() // convert to runs

	if n := bm0.IntersectionCount(bm1); n != 4 {
		t.Fatalf("unexpected n: %d", n)
	} else if n := bm1.IntersectionCount(bm0); n != 4 {
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
func TestBitmap_IntersectionCount_Mixed(t *testing.T) {
	bm0 := testBM()
	bm1 := roaring.NewBitmap(0, 1, 2, 3, 4, 5, 6, 7, 9, 10, 65536)
	bm3 := roaring.NewBitmap(131072)

	if n := bm0.IntersectionCount(bm0); n != bm0.Count() {
		t.Fatalf("unexpected n: %d", n)
	}
	if n := bm0.IntersectionCount(bm1); n != 1 {
		t.Fatalf("unexpected n: %d", n)
	}
	if n := bm0.IntersectionCount(bm3); n != 1 {
		t.Fatalf("unexpected n: %d", n)
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
		manual_count := uint64(0)
		for _, v := range a {
			new_bit, _ := bm.Add(v)
			if new_bit {
				manual_count++
			}
			m[v] = struct{}{}
		}
		//check count
		if manual_count != bm.Count() {
			t.Fatalf("expected bitmap Add count to be: %d got: %d", manual_count, bm.Count())
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
			removed, _ := bm.Remove(a[i])
			if removed {
				manual_count--
			}
			//check count
			if manual_count != bm.Count() {
				t.Fatalf("expected bitmap Remove count to be: %d got: %d", manual_count, bm.Count())
			}
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

// TODO update for RLE

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
// TODO duplicate for all container types
func TestIterator(t *testing.T) {
	t.Run("bitmap", func(t *testing.T) {
		itr := roaring.NewBitmap(1, 2, 3).Iterator()
		itr.Seek(0)

		var a []uint64
		for v, eof := itr.Next(); !eof; v, eof = itr.Next() {
			a = append(a, v)
		}

		if !reflect.DeepEqual(a, []uint64{1, 2, 3}) {
			t.Fatalf("unexpected values: %+v", a)
		}
	})

	t.Run("run", func(t *testing.T) {
		bm1 := roaring.NewBitmap()
		for i := uint64(0); i < 11; i += 1 {
			bm1.Add(i)
		}
		bm1.Optimize()

		bm2 := roaring.NewBitmap()
		for i := uint64(0); i < 12; i += 1 {
			bm2.Add(i)
		}
		bm2.Optimize()

		for _, tt := range []struct {
			bm       *roaring.Bitmap
			expected []uint64
		}{
			{bm1, []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10}},
			{bm2, []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}},
			{bm1.Difference(bm2), []uint64{}},
			{bm2.Difference(bm1), []uint64{11}},
		} {
			itr := tt.bm.Iterator()
			itr.Seek(0)

			a := []uint64{}
			for v, eof := itr.Next(); !eof; v, eof = itr.Next() {
				a = append(a, v)
			}

			if !reflect.DeepEqual(a, tt.expected) {
				t.Fatalf("unexpected values: %#v %#v", a, tt.expected)
			}
		}
	})
}

// testBM creates a bitmap with 3 containers: array, bitmap, and run.
func testBM() *roaring.Bitmap {

	bm := roaring.NewBitmap()
	//the array
	for i := uint64(0); i < 1024; i += 4 {
		bm.Add((1 << 16) + i)
	}
	//the bitmap
	for i := uint64(0); i < 16384; i += 2 {
		bm.Add((2 << 16) + i)
	}
	//small run
	for i := uint64(0); i < 1024; i += 1 {
		bm.Add((3 << 16) + i)
	}
	//large run
	for i := uint64(0); i < 65535; i += 1 {
		bm.Add((4 << 16) + i)
	}
	bm.Optimize()
	//count 75007
	return bm
}

func TestBitmapOffsetRange(t *testing.T) {
	bm := testBM()

	bm1 := bm.OffsetRange(0, 0, 327680)
	if bm1.Count() != bm.Count() {
		t.Fatalf("Not Equal %d %d", bm1.Count(), bm.Count())
	}
	bm1 = bm.OffsetRange(0, 0, 131072)
	if bm1.Count() != 256 {
		t.Fatalf("Not Equal %d %d", bm1.Count(), 256)
	}

}
func TestBitmapContains(t *testing.T) {
	bm := testBM()

	//search for run value present
	if found := bm.Contains(3 << 16); !found {
		t.Fatalf("Test #1 Not Found %d ", 3<<16)
	}

	//search for value not present
	if found := bm.Contains((3 << 16) + 2048); found {
		t.Fatalf("Test #2 Found %d ", (3<<16)+2048)
	}
}

func TestBitmapBufIterator(t *testing.T) {

}

var benchmarkBitmapIntersectionCountData struct {
	a, b, r *roaring.Bitmap
}

func getBenchData() *struct{ a, b, r *roaring.Bitmap } {
	data := &benchmarkBitmapIntersectionCountData
	if data.a == nil {
		const max = (1 << 24) / 64

		// Build bitmap with array container.
		data.a = roaring.NewBitmap()
		for i, n := 0, 2*roaring.ArrayMaxSize/3; i < n; i++ {
			data.a.Add(uint64(rand.Intn(max)))
		}

		// Build bitmap with bitmap container.
		data.b = roaring.NewBitmap()
		for i, n := 0, MaxContainerVal/3; i < n; i++ {
			data.b.Add(uint64(i * 3))
		}

		// build bitmap with run container
		data.r = roaring.NewBitmap()
		for i, n := 0, MaxContainerVal; i < n; i++ {
			data.r.Add(uint64(i))
		}

	}
	return data
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

func TestBitmap_Intersect(t *testing.T) {
	bm0 := testBM()
	result := bm0.Intersect(bm0)
	if bm0.Count() != result.Count() {
		t.Fatalf("Counts do not match %d %d", bm0.Count(), result.Count())
	}
}

func BenchmarkBitmap_IntersectionCount_ArrayRun(b *testing.B) {
	data := getBenchData()
	// Reset timer & benchmark.
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data.a.IntersectionCount(data.r)
	}
}

func BenchmarkBitmap_IntersectionCount_BitmapRun(b *testing.B) {
	data := getBenchData()
	// Reset timer & benchmark.
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data.b.IntersectionCount(data.r)
	}
}

func BenchmarkBitmap_IntersectionCount_ArrayBitmap(b *testing.B) {
	data := getBenchData()
	// Reset timer & benchmark.
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data.a.IntersectionCount(data.b)
	}
}

const (
	NumRows         = uint64(10000)
	NumColums       = uint64(16)
	MaxContainerVal = 0xffff
)

func BenchmarkContainerLinear(b *testing.B) {

	for n := 0; n < b.N; n++ {
		bm := roaring.NewBitmap()
		for row := uint64(1); row < NumRows; row++ {
			for col := uint64(1); col < NumColums; col++ {
				bm.Add(row*pilosa.SliceWidth + (col * MaxContainerVal))
			}
		}
	}
}

func BenchmarkContainerReverse(b *testing.B) {
	for n := 0; n < b.N; n++ {
		bm := roaring.NewBitmap()
		for row := NumRows - 1; row >= 1; row-- {
			for col := NumColums - 1; col >= 1; col-- {
				bm.Add(row*pilosa.SliceWidth + (col * MaxContainerVal))
			}
		}
	}
}

func BenchmarkContainerColumn(b *testing.B) {
	for n := 0; n < b.N; n++ {
		bm := roaring.NewBitmap()
		for col := uint64(1); col < NumColums; col++ {
			for row := uint64(1); row < NumRows; row++ {
				bm.Add(row*pilosa.SliceWidth + (col * MaxContainerVal))
			}
		}
	}
}

func BenchmarkContainerOutsideIn(b *testing.B) {
	middle := NumRows / uint64(2)
	for n := 0; n < b.N; n++ {
		bm := roaring.NewBitmap()

		for col := uint64(1); col < NumColums; col++ {
			for row := uint64(1); row < middle; row++ {
				bm.Add(row*pilosa.SliceWidth + (col * MaxContainerVal))
				bm.Add((NumRows-row)*pilosa.SliceWidth + (col * MaxContainerVal))
			}
		}
	}
}

func BenchmarkContainerInsideOut(b *testing.B) {
	middle := NumRows / uint64(2)
	for n := 0; n < b.N; n++ {
		bm := roaring.NewBitmap()
		for col := uint64(1); col < NumColums; col++ {
			for row := uint64(1); row <= middle; row++ {
				bm.Add((middle+row)*pilosa.SliceWidth + (col * MaxContainerVal))
				bm.Add((middle-row)*pilosa.SliceWidth + (col * MaxContainerVal))
			}
		}
	}
}

func BenchmarkSliceAscending(b *testing.B) {
	for n := 0; n < b.N; n++ {
		bm := roaring.NewBitmap()
		for col := uint64(0); col < pilosa.SliceWidth; col++ {
			bm.Add(col)
		}
	}
}

func BenchmarkSliceDescending(b *testing.B) {
	for n := 0; n < b.N; n++ {
		bm := roaring.NewBitmap()
		for col := uint64(pilosa.SliceWidth); col > uint64(0); col-- {
			bm.Add(col)
		}
	}
}
