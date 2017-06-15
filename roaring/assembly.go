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


// bit population count, take from
// https://code.google.com/p/go/issues/detail?id=4988#c11
// credit: https://code.google.com/u/arnehormann/
func popcntGo(x uint64) (n uint64) {
	x -= (x >> 1) & 0x5555555555555555
	x = (x>>2)&0x3333333333333333 + x&0x3333333333333333
	x += x >> 4
	x &= 0x0f0f0f0f0f0f0f0f
	x *= 0x0101010101010101
	return x >> 56
}

func popcntSliceGo(s []uint64) uint64 {
	cnt := uint64(0)
	for _, x := range s {
		cnt += popcntGo(x)
	}
	return cnt
}

func popcntMaskSliceGo(s, m []uint64) uint64 {
	cnt := uint64(0)
	for i := range s {
		cnt += popcntGo(s[i] &^ m[i])
	}
	return cnt
}

func popcntAndSliceGo(s, m []uint64) uint64 {
	cnt := uint64(0)
	for i := range s {
		cnt += popcntGo(s[i] & m[i])
	}
	return cnt
}

func popcntOrSliceGo(s, m []uint64) uint64 {
	cnt := uint64(0)
	for i := range s {
		cnt += popcntGo(s[i] | m[i])
	}
	return cnt
}

func popcntXorSliceGo(s, m []uint64) uint64 {
	cnt := uint64(0)
	for i := range s {
		cnt += popcntGo(s[i] ^ m[i])
	}
	return cnt
}
