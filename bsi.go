// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package pilosa

import (
	"math/bits"

	"github.com/featurebasedb/featurebase/v3/roaring"
)

// BSIData contains BSI-structured data.
type BSIData []*Row

// PivotDescending loops over nonzero BSI values in descending order.
// For each value, the provided function is called with the value and a slice of the associated columns.
// If limit or offset are not-nil, they will be applied.
// Applying a limit or offset may modify the pointed-to value.
func (bsi BSIData) PivotDescending(filter *Row, branch uint64, limit, offset *uint64, fn func(uint64, ...uint64)) {
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
		lowerBits.PivotDescending(filter.Intersect(splitBit), upperBranch, limit, offset, fn)
		lowerBits.PivotDescending(filter.Difference(splitBit), lowerBranch, limit, offset, fn)
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
	bsi.PivotDescending(filter, 0, nil, nil, func(count uint64, values ...uint64) {
		dist.insert(count, uint64(len(values)))
	})
	return dist
}
*/

var placeholderBitmap = roaring.NewBitmap()

// AddBSI adds two BSI bitmaps together.
// It does not handle sign and has no concept of overflow.
func AddBSI(x, y BSIData) BSIData {
	// Accumulate row segments.
	segments := make([][]RowSegment, len(x)+len(y))
	xsegs, ysegs := segments[:len(x)], segments[len(x):]
	for i, r := range x {
		xsegs[i] = r.Segments
	}
	for i, r := range y {
		ysegs[i] = r.Segments
	}

	var dst BSIData
	var xbitmaps, ybitmaps []*roaring.Bitmap
	for {
		// Find the next shard.
		next := ^uint64(0)
		for _, s := range segments {
			if len(s) == 0 {
				continue
			}
			shard := s[0].shard
			if shard < next {
				next = shard
			}
		}
		if next == ^uint64(0) {
			// There are no remaining shards.
			break
		}

		// Accumulate bitmaps for this shard.
		xbitmaps, ybitmaps = xbitmaps[:0], ybitmaps[:0]
		for i, segs := range xsegs {
			if len(segs) == 0 || segs[0].shard != next {
				continue
			}
			xsegs[i] = segs[1:]
			bm := segs[0].data
			if !bm.Any() {
				continue
			}
			for len(xbitmaps) < i {
				xbitmaps = append(xbitmaps, placeholderBitmap)
			}
			xbitmaps = append(xbitmaps, bm)
		}
		for i, segs := range ysegs {
			if len(segs) == 0 || segs[0].shard != next {
				continue
			}
			ysegs[i] = segs[1:]
			bm := segs[0].data
			if !bm.Any() {
				continue
			}
			for len(ybitmaps) < i {
				ybitmaps = append(ybitmaps, placeholderBitmap)
			}
			ybitmaps = append(ybitmaps, bm)
		}

		// Add the shard values together.
		var out []*roaring.Bitmap
		switch {
		case len(xbitmaps) == 0:
			// There are no values in x.
			out = ybitmaps
		case len(ybitmaps) == 0:
			// There are no values in y.
			out = xbitmaps
		default:
			out = roaring.Add(xbitmaps, ybitmaps)
		}

		// Convert the bitmaps to output segments.
		for i, b := range out {
			if !b.Any() {
				continue
			}
			for len(dst) <= i {
				dst = append(dst, NewRow())
			}
			dst[i].Segments = append(dst[i].Segments, RowSegment{
				shard:    next,
				writable: true,
				data:     b,
				n:        b.Count(),
			})
		}
	}

	return dst
}

// rowBuilder builds a row quickly from individual values.
// It is optimized for the case in which values are generated sequentially.
type rowBuilder struct {
	bm    *roaring.Bitmap
	mask  *[1024]uint64
	array []uint16
	key   uint64
	n     int32
}

// flushKey flushes the data at the current key to the bitmap.
func (b *rowBuilder) flushKey() {
	var c *roaring.Container
	switch {
	case b.mask != nil:
		c = roaring.NewContainerBitmapN(b.mask[:], b.n)
		b.mask = nil
	case len(b.array) > 0:
		c = roaring.NewContainerArrayCopy(b.array)
		b.array = b.array[:0]
	default:
		return
	}

	if b.bm == nil {
		b.bm = roaring.NewBitmap()
	}
	if old := b.bm.Containers.Get(b.key); old != nil {
		c = roaring.Union(c, old)
	}
	b.bm.Containers.Put(b.key, c)
}

// Add a value to the bitmap.
// Values must be added sequentially.
func (b *rowBuilder) Add(v uint64) {
	vkey := v / (1 << 16)
	if b.key != vkey {
		// This is a new key, so flush the old one.
		b.flushKey()
		b.key = vkey
	}

	if b.mask != nil {
		// Add to the mask.
		b.n += int32(1 &^ (b.mask[uint16(v)/64] >> (v % 64)))
		b.mask[uint16(v)/64] |= 1 << (v % 64)
		return
	}

	// Add to an array.
	b.array = append(b.array, uint16(v))
	if len(b.array) >= roaring.ArrayMaxSize {
		// The array is too big.
		// Convert it to a bitmask.
		m := [1024]uint64{}
		for _, v := range b.array {
			m[v/64] |= 1 << (v % 64)
		}
		b.n = int32(len(b.array))
		b.array = b.array[:0]
		b.mask = &m
	}
}

// Build a Row from stored data.
// This resets the builder.
func (b *rowBuilder) Build() *Row {
	// Flush the active key to the bitmap.
	b.flushKey()

	// Remove the bitmap and convert it to a Row.
	bm := b.bm
	b.bm = nil
	if bm == nil {
		return NewRow()
	}
	return NewRowFromBitmap(bm)
}

// bsiBuilder assembles BSI data.
// It is optimized for the case in which values are generated sequentially.
type bsiBuilder []rowBuilder

// Insert a value into the BSI data.
// Columns must be inserted sequentially, and duplicates are not allowed.
func (b *bsiBuilder) Insert(col, val uint64) {
	for val != 0 {
		i := bits.TrailingZeros64(val)
		val &^= 1 << i
		for len(*b) <= i {
			*b = append(*b, rowBuilder{})
		}
		(*b)[i].Add(col)
	}
}

// Build BSI data.
// This resets the builder.
func (b *bsiBuilder) Build() BSIData {
	builders := *b
	*b = builders[:0]
	rows := make(BSIData, len(builders))
	for i := range builders {
		rows[i] = builders[i].Build()
	}
	return rows
}
