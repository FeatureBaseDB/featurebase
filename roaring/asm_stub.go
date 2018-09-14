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

// Package roaring implements roaring bitmaps with support for incremental changes.
package roaring

//go:noescape
func asmAnd(a, b, c []uint64) int
func asmOr(a, b, c []uint64) int
func asmXor(a, b, c []uint64) int
func asmAndN(a, b, c []uint64) int
