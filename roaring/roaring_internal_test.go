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

import "testing"

func TestIntersectionCountArrayBitmap2(t *testing.T) {
	a, b := &container{}, &container{}
	tests := []struct {
		array  []uint32
		bitmap []uint64
		exp    uint64
	}{
		{
			array:  []uint32{0},
			bitmap: []uint64{1},
			exp:    1,
		},
		{
			array:  []uint32{0, 1},
			bitmap: []uint64{3},
			exp:    2,
		},
		{
			array:  []uint32{64, 128, 129, 2000},
			bitmap: []uint64{932421, 2},
			exp:    0,
		},
		{
			array:  []uint32{0, 65, 130, 195},
			bitmap: []uint64{255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255},
			exp:    4,
		},
		{
			array:  []uint32{63, 120, 543, 639, 12000},
			bitmap: []uint64{0x8000000000000000, 0, 0, 0, 0, 0, 0, 0, 0, 0x8000000000000000},
			exp:    2,
		},
	}

	for i, test := range tests {
		a.array = test.array
		b.bitmap = test.bitmap
		ret1 := intersectionCountArrayBitmapOld(a, b)
		ret2 := intersectionCountArrayBitmap(a, b)
		if ret1 != ret2 || ret2 != test.exp {
			t.Fatalf("test #%v intersectCountArrayBitmap fail orig: %v new: %v exp: %v", i, ret1, ret2, test.exp)
		}
	}
}
