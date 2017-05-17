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

// +build !amd64

package roaring

func hasAsm() bool {return false}


func popcntSlice(s []uint64) uint64        { return popcntSliceGo(s) }
func popcntMaskSlice(s, m []uint64) uint64 { return popcntMaskSliceGo(s, m) }
func popcntAndSlice(s, m []uint64) uint64  { return popcntAndSliceGo(s, m) }
func popcntOrSlice(s, m []uint64) uint64   { return popcntOrSliceGo(s, m) }
func popcntXorSlice(s, m []uint64) uint64  { return popcntXorSliceGo(s, m) }
func popcnt(s uint64) uint64               { return popcntGo(s) }
