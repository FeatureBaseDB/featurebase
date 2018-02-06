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
	"fmt"
	"reflect"
	"testing"

	"github.com/pilosa/pilosa"
)

// Ensure a bitmap can be merged
func TestBitmap_Merge(t *testing.T) {
	tests := []struct {
		bm1 *pilosa.Bitmap
		bm2 *pilosa.Bitmap
		exp uint64
	}{
		{
			bm1: pilosa.NewBitmap(1, 2, 3, SliceWidth+1, 2*SliceWidth),
			bm2: pilosa.NewBitmap(3, 4, 5),
			exp: 7,
		},
		{
			bm1: pilosa.NewBitmap(),
			bm2: pilosa.NewBitmap(2, 66000, 70000, 70001, 70002, 70003, 70004),
			exp: 7,
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("#%d:", i), func(t *testing.T) {
			test.bm1.Merge(test.bm2)
			if cnt := test.bm1.Count(); cnt != test.exp {
				t.Fatalf("merged count %d is not %d", cnt, test.exp)
			}
			if length := len(test.bm1.Bits()); uint64(length) != test.exp {
				t.Fatalf("merged length %d is not %d", length, test.exp)
			}
		})
	}
}

// Ensure a bitmap can Xor'ed
func TestBitmap_Xor(t *testing.T) {
	bm1 := pilosa.NewBitmap(0, 1, SliceWidth)
	bm2 := pilosa.NewBitmap(0, 2*SliceWidth)
	exp := []uint64{1, SliceWidth, 2 * SliceWidth}

	res := bm1.Xor(bm2)
	if res.Count() != 3 {
		t.Fatalf("Test 1 Count after xor %d != 3\n", res.Count())
	}

	if !reflect.DeepEqual(res.Bits(), exp) {
		t.Fatalf("Test 2 Results %v != expected %v\n", res.Bits(), exp)
	}
	res = bm2.Xor(bm1)
	if res.Count() != 3 {
		t.Fatalf("Test 3 Count after xor %d != 3\n", res.Count())
	}
	if !reflect.DeepEqual(res.Bits(), exp) {
		t.Fatalf("Test 4 Results %v != expected %v\n", res.Bits(), exp)
	}

}
func TestBitmap_Union_Segment(t *testing.T) {
	bm1 := pilosa.NewBitmap(0, 1, SliceWidth)
	bm2 := pilosa.NewBitmap(0, 2*SliceWidth)
	exp := []uint64{0, 1, SliceWidth, 2 * SliceWidth}
	res := bm1.Union(bm2)

	if res.Count() != 4 {
		t.Fatalf("Test 1 Count after Union %d != 5\n", res.Count())
	}
	if !reflect.DeepEqual(res.Bits(), exp) {
		t.Fatalf("Test 2 Union Results %v != expected %v\n", res.Bits(), exp)
	}
	res = bm2.Union(bm1)
	if res.Count() != 4 {
		t.Fatalf("Test 3 Count after xor %d != 5\n", res.Count())
	}
	if !reflect.DeepEqual(res.Bits(), exp) {
		t.Fatalf("Test 2 Union Results %v != expected %v\n", res.Bits(), exp)
	}
}

func TestBitmap_Difference_Segment(t *testing.T) {
	bm1 := pilosa.NewBitmap(0, 1, SliceWidth)
	bm2 := pilosa.NewBitmap(0, 2*SliceWidth)
	exp := []uint64{1, SliceWidth}
	res := bm1.Difference(bm2)

	if res.Count() != 2 {
		t.Fatalf("Test 1 Count after Difference %d != 5\n", res.Count())
	}
	if !reflect.DeepEqual(res.Bits(), exp) {
		t.Fatalf("Test 2 Difference Results %v != expected %v\n", res.Bits(), exp)
	}
}
