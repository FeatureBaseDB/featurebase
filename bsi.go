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

package pilosa

import "math/bits"

// bsiData contains BSI-structured data.
type bsiData []*Row

// insert a value for a column in the BSI data.
func (bsi *bsiData) insert(column uint64, value uint64) {
	data := *bsi
	for value != 0 {
		bit := bits.TrailingZeros64(value)
		value &^= 1 << bit

		for len(data) <= bit {
			data = append(data, NewRow())
		}

		data[bit].SetBit(column)
	}
	*bsi = data
}

// pivotDescending loops over nonzero BSI values in descending order.
// For each value, the provided function is called with the value and a slice of the associated columns.
func (bsi bsiData) pivotDescending(filter *Row, branch uint64, limit, offset *uint64, fn func(uint64, ...uint64)) {
	// This "pivot" algorithm works by treating the BSI data as a tree.
	// Each branch of this tree corresponds to a power-of-2-sized range of BSI values.
	// Each range is subdivided into 2 ranges of half size, which form lower branches.
	// Eventually, a range of width 1 cannot be subdivided and forms a leaf.
	// At each branch and leaf, there is a bitmap of all columns within the corresponding range.
	// The lower branches are formed as a difference or intersect of the upper branch's bitmap with the BSI bit that subdivides the range.
	// This function uses a depth-first search over this virtual tree.

	switch {
	case !filter.Any():
		// There are no remaining data.

	case offset != nil && *offset >= filter.Count():
		// Skip this entire branch.
		*offset -= filter.Count()

	case limit != nil && *limit == 0:
		// The limit has been reached.
		// No more data is necessary.

	case len(bsi) == 0:
		// This is a leaf node.
		cols := filter.Columns()
		if offset != nil {
			cols = cols[*offset:]
			*offset = 0
		}
		if limit != nil {
			if *limit < uint64(len(cols)) {
				cols = cols[:*limit]
			}
			*limit -= uint64(len(cols))
		}
		fn(branch, cols...)

	default:
		// Pivot over the highest bit.
		upperBranch, lowerBranch := branch|(1<<uint(len(bsi)-1)), branch
		splitBit := bsi[len(bsi)-1]
		lowerBits := bsi[:len(bsi)-1]
		lowerBits.pivotDescending(filter.Intersect(splitBit), upperBranch, limit, offset, fn)
		lowerBits.pivotDescending(filter.Difference(splitBit), lowerBranch, limit, offset, fn)
	}
}

/*
// distribution generates a BSI histogram for the input.
// TODO: I forgot what I was going to use this for.
// Could probbably use this for:
// - quartile queries
// - TopN on int
func (bsi bsiData) distribution(filter *Row) bsiData {
	var dist bsiData
	bsi.pivotDescending(filter, 0, nil, nil, func(count uint64, values ...uint64) {
		dist.insert(count, uint64(len(values)))
	})
	return dist
}
*/

// addBSI adds BSI values together.
func addBSI(x, y bsiData) bsiData {
	if len(x) > len(y) {
		x, y = y, x
	}
	carry := NewRow()
	out := make(bsiData, 0, len(y))
	for i, v := range x {
		out = append(out, v.Xor(y[i]).Xor(carry))
		carry = v.Intersect(y[i]).Union(v.Intersect(carry), y[i].Intersect(carry))
	}
	for _, v := range y[len(x):] {
		out = append(out, v.Xor(carry))
		carry = v.Intersect(carry)
	}
	if carry.Any() {
		out = append(out, carry)
	}
	return out
}
