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

func TestBSFQ(t *testing.T) {
	result := BSFQ(2)
	if result != 1 {
		t.Fatalf("BSF INCORRECT: %d", result)
	}
}

func TestBSFQ_CompareGo(t *testing.T) {
	v := uint64(1)
	for i := 0; i < 64; i++ {
		if BSFQ(v) != trailingZeroN(v) {
			t.Fatalf("BSF INCORRECT: %d %d", BSFQ(v), trailingZeroN(v))
		}
		if v == 0 {
			v = 1
		} else {
			v *= 2
		}
	}
	/*
		if bsfq(0) != trailingZeroN(0) {
			fmt.Println(bsfq(0))
			t.Fatalf("BSF INCORRECT")
		}
	*/
}
func BenchmarkBSF(b *testing.B) {
	for i := 0; i < b.N; i++ {
		BSFQ(uint64(i))
	}
}

func BenchmarkTrailingZeroN(b *testing.B) {
	for i := 0; i < b.N; i++ {
		trailingZeroN(uint64(i))
	}
}

func BenchmarkPOPCNTQ(b *testing.B) {
	for i := 0; i < b.N; i++ {
		POPCNTQ(uint64(i))
	}
}

func BenchmarkPopcount(b *testing.B) {
	for i := 0; i < b.N; i++ {
		popcount(uint64(i))
	}
}

func BenchmarkPopcntAsm(b *testing.B) {
	// run the Fib function b.N times
	for n := 0; n < b.N; n++ {
		popcntAsm(0xdeadbeef)
	}
}

func BenchmarkPopcntGo(b *testing.B) {
	// run the Fib function b.N times
	for n := 0; n < b.N; n++ {
		popcntGo(0xdeadbeef)
	}
}

func getData() []uint64 {
	return []uint64{
		0xdeadbeef,
		0xdeadbeef,
		0xdeadbeef,
		0xdeadbeef,
		0xdeadbeef,
		0xdeadbeef,
		0xdeadbeef,
		0xdeadbeef,
		0xdeadbeef,
		0xdeadbeef,
		0xdeadbeef,
		0xdeadbeef,
		0xdeadbeef,
		0xdeadbeef,
		0xdeadbeef,
		0xdeadbeef,
		0xdeadbeef,
		0xdeadbeef,
		0xdeadbeef,
		0xdeadbeef,
		0xdeadbeef,
		0xdeadbeef,
		0xdeadbeef,
		0xdeadbeef,
		0xdeadbeef,
		0xdeadbeef,
		0xdeadbeef,
		0xdeadbeef,
		0xdeadbeef,
		0xdeadbeef,
		0xdeadbeef,
		0xdeadbeef,
	}
}

func BenchmarkPopcntSliceGo(b *testing.B) {
	d := getData()
	for n := 0; n < b.N; n++ {
		popcntSliceGo(d)
	}
}

func BenchmarkPopcntSliceAsm(b *testing.B) {
	d := getData()
	for n := 0; n < b.N; n++ {
		popcntSliceAsm(d)
	}
}

func BenchmarkPopcntSlice(b *testing.B) {
	d := getData()
	for n := 0; n < b.N; n++ {
		popcntSlice(d)
	}
}
