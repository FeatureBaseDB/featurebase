// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package pilosa

import (
	"fmt"

	"github.com/featurebasedb/featurebase/v3/roaring"
)

// iterator is an interface for looping over row/column pairs.
type iterator interface {
	Seek(rowID, columnID uint64)
	Next() (rowID, columnID uint64, eof bool)
}

// bufIterator wraps an iterator to provide the ability to unread values.
type bufIterator struct {
	buf struct {
		rowID    uint64
		columnID uint64
		eof      bool
		full     bool
	}
	itr iterator
}

// newBufIterator returns a buffered iterator that wraps itr.
func newBufIterator(itr iterator) *bufIterator {
	return &bufIterator{itr: itr}
}

// Seek moves to the first pair equal to or greater than pseek/bseek.
func (itr *bufIterator) Seek(rowID, columnID uint64) {
	itr.buf.full = false
	itr.itr.Seek(rowID, columnID)
}

// Next returns the next pair in the row.
// If a value has been buffered then it is returned and the buffer is cleared.
func (itr *bufIterator) Next() (rowID, columnID uint64, eof bool) {
	if itr.buf.full {
		itr.buf.full = false
		return itr.buf.rowID, itr.buf.columnID, itr.buf.eof
	}

	// Read values onto buffer in case of unread.
	itr.buf.rowID, itr.buf.columnID, itr.buf.eof = itr.itr.Next()

	return itr.buf.rowID, itr.buf.columnID, itr.buf.eof
}

// Peek reads the next value but leaves it on the buffer.
func (itr *bufIterator) Peek() (rowID, columnID uint64, eof bool) {
	rowID, columnID, eof = itr.Next()
	itr.Unread()
	return
}

// Unread pushes previous pair on to the buffer.
// Panics if the buffer is already full.
func (itr *bufIterator) Unread() {
	if itr.buf.full {
		panic("pilosa.BufIterator: buffer full")
	}
	itr.buf.full = true
}

// sliceIterator iterates over a pair of row/column ID slices.
type sliceIterator struct {
	rowIDs    []uint64
	columnIDs []uint64

	i, n int
}

// newSliceIterator returns an iterator to iterate over a set of row/column ID pairs.
// Both slices MUST have an equal length. Otherwise the function will panic.
func newSliceIterator(rowIDs, columnIDs []uint64) *sliceIterator {
	if len(columnIDs) != len(rowIDs) {
		panic(fmt.Sprintf("pilosa.SliceIterator: pair length mismatch: %d != %d", len(rowIDs), len(columnIDs)))
	}

	return &sliceIterator{
		rowIDs:    rowIDs,
		columnIDs: columnIDs,

		n: len(rowIDs),
	}
}

// Seek moves the cursor to a given pair.
// If the pair is not found, the iterator seeks to the next pair.
func (itr *sliceIterator) Seek(bseek, pseek uint64) {
	for i := 0; i < itr.n; i++ {
		rowID := itr.rowIDs[i]
		columnID := itr.columnIDs[i]

		if (bseek == rowID && pseek <= columnID) || bseek < rowID {
			itr.i = i
			return
		}
	}

	// Seek to the end of the slice if all values are less than seek pair.
	itr.i = itr.n
}

// Next returns the next row/column ID pair.
func (itr *sliceIterator) Next() (rowID, columnID uint64, eof bool) {
	if itr.i >= itr.n {
		return 0, 0, true
	}

	rowID = itr.rowIDs[itr.i]
	columnID = itr.columnIDs[itr.i]

	itr.i++
	return rowID, columnID, false
}
