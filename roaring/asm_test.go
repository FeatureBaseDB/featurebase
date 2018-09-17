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

func BenchmarkBitmap_ASMIntersect(b *testing.B) {
	a:= bitmapFull()
	results:= make ([]uint64,bitmapN)
        b.ResetTimer()
        for x := 0; x < b.N; x++ {
                _=asmAnd(a, a,results)
        }
}

func BenchmarkBitmap_GOIntersect(b *testing.B) {
	a := bitmapFull()
	c := bitmapEvenBitsSet()
	results := make([]uint64, bitmapN)
	b.ResetTimer()
	for x := 0; x < b.N; x++ {
		_ = goAnd(a, c, results)
	}
}

func goAndNoBCE(a, b, c []uint64) (n int) {
	for i := 0; i < bitmapN; i++ {
		c[i] = a[i] & b[i]
		n += int(popcount(c[i]))
	}
	return n
}

func BenchmarkBitmap_GoAndNoBCE(b *testing.B) {
	a := bitmapFull()
	c := bitmapEvenBitsSet()
	results := make([]uint64, bitmapN)
	b.ResetTimer()
	for x := 0; x < b.N; x++ {
		_ = goAndNoBCE(a, c, results)
	}
}

func BenchmarkBitmap_GOIntersectUnroll4(b *testing.B) {
	a := bitmapFull()
	c := bitmapEvenBitsSet()
	results := make([]uint64, bitmapN)
	b.ResetTimer()
	for x := 0; x < b.N; x++ {
		_ = goAndUnroll4(a, c, results)
	}
}

func BenchmarkBitmap_GOIntersectUnroll8(b *testing.B) {
	a := bitmapFull()
	c := bitmapEvenBitsSet()
	results := make([]uint64, bitmapN)
	b.ResetTimer()
	for x := 0; x < b.N; x++ {
		_ = goAndUnroll8(a, c, results)
	}
}

func BenchmarkBitmap_GOIntersectUnroll16(b *testing.B) {
	a := bitmapFull()
	c := bitmapEvenBitsSet()
	results := make([]uint64, bitmapN)
	b.ResetTimer()
	for x := 0; x < b.N; x++ {
		_ = goAndUnroll16(a, c, results)
	}
}

func BenchmarkBitmap_GOIntersectUnroll1024(b *testing.B) {
	a := bitmapFull()
	c := bitmapEvenBitsSet()
	results := make([]uint64, bitmapN)
	b.ResetTimer()
	for x := 0; x < b.N; x++ {
		_ = goAndUnroll1024(a, c, results)
	}
}
