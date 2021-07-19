// Copyright 2020 Pilosa Corp.
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
	"fmt"
	"math"

	"github.com/molecula/featurebase/v2/shardwidth"
)

func (b *Bitmap) String() (r string) {
	r = "c("
	slc := b.Slice()
	width := 0
	s := ""
	for _, v := range slc {
		if width == 0 {
			s = fmt.Sprintf("%v", v)
		} else {
			s = fmt.Sprintf(", %v", v)
		}
		width += len(s)
		r += s
		if width > 70 {
			r += ",\n"
			width = 0
		}
	}
	if width == 0 && len(r) > 2 {
		r = r[:len(r)-2]
	}
	return r + ")"
}

// AsContainerMatrixString returns a string showing
// the matrix of rows in a shard, showing the count of hot (1) bits
// in each container.
func (b *Bitmap) AsContainerMatrixString() (r string) {
	slc := b.Slice()
	n := len(slc)
	max := slc[n-1]
	const rowWidthInContainerCount = 1 << (shardwidth.Exponent - 16) // - 16 because roaring.Container always holds 2^16 bits.

	sw := uint64(1 << shardwidth.Exponent)
	//fmt.Printf("sw = %v, shardwidth.Exponent = %v, rowWidthInContainerCount=%v\n", sw, shardwidth.Exponent, rowWidthInContainerCount)
	maxrow := uint64(math.Ceil(float64(max) / float64(sw)))
	if max == 0 {
		maxrow++
	}
	matrix := make([][]uint64, maxrow)
	for i := uint64(0); i < maxrow; i++ {
		matrix[i] = make([]uint64, rowWidthInContainerCount)
	}
	iter, _ := b.Containers.Iterator(0)
	for iter.Next() {
		k, v := iter.Value()
		j := k & keyMask
		i := (k << 16) >> shardwidth.Exponent
		matrix[i][j] = uint64(v.N())
	}
	r = "\n            "
	for j := 0; j < rowWidthInContainerCount; j++ {
		r += fmt.Sprintf("%-5v  ", j)
	}
	r += "\n"
	for i, row := range matrix {
		r += fmt.Sprintf("[row %05v] ", i)
		for j, col := range row {
			_ = j
			r += fmt.Sprintf("%-5v  ", col)
		}
		r += "\n"
	}
	return
}
