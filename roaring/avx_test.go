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

import (
	"testing"
)

func BenchmarkBitmap_AVXUnion(b *testing.B) {
        a := &Container{
                bitmap : bitmapFull(),
                n : 65536,
                containerType : containerBitmap,
        }
        b.ResetTimer()
        for x := 0; x < b.N; x++ {
                _=And(a.bitmap, a.bitmap)
        }
}
	
func BenchmarkBitmap_Union(b *testing.B) {
        a := &Container{
                bitmap : bitmapFull(),
                n : 65536,
                containerType : containerBitmap,
        }
        b.ResetTimer()
        for x := 0; x < b.N; x++ {
                _=intersectBitmapBitmap(a, a)
        }
}


func TestAvx(t *testing.T) {
	a := getFullBitmap()
	b := getFullBitmap()
	c:=And(a,b)
	for i:=range a{
		if a[i] != c[1]{
			                t.Fatalf("Values don't match ")
		}
	}


}
