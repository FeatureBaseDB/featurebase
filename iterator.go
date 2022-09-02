// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package pilosa

import (
	"fmt"

	"github.com/molecula/featurebase/v3/roaring"
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

// limitIterator wraps an Iterator and limits it to a max column/row pair.
type limitIterator struct {
	itr         iterator
	maxRowID    uint64
	maxColumnID uint64

	eof bool
}

// newLimitIterator returns a new LimitIterator.
func newLimitIterator(itr iterator, maxRowID, maxColumnID uint64) *limitIterator { // nolint: unparam
	return &limitIterator{
		itr:         itr,
		maxRowID:    maxRowID,
		maxColumnID: maxColumnID,
	}
}

// Seek moves the underlying iterator to a column/row pair.
func (itr *limitIterator) Seek(rowID, columnID uint64) { itr.itr.Seek(rowID, columnID) }

// Next returns the next row/column ID pair.
// If the underlying iterator returns a pair higher than the max then EOF is returned.
func (itr *limitIterator) Next() (rowID, columnID uint64, eof bool) {
	// Always return EOF once it is reached by limit or the underlying iterator.
	if itr.eof {
		return 0, 0, true
	}

	// Retrieve pair from underlying iterator.
	// Mark as EOF if it is beyond the limit (or at EOF).
	rowID, columnID, eof = itr.itr.Next()
	if eof || rowID > itr.maxRowID || (rowID == itr.maxRowID && columnID > itr.maxColumnID) {
		itr.eof = true
		return 0, 0, true
	}

	return rowID, columnID, false
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

// roaringIterator converts a roaring.Iterator to output column/row pairs.
type roaringIterator struct {
	itr *roaring.Iterator
}

// newRoaringIterator returns a new iterator wrapping itr.
func newRoaringIterator(itr *roaring.Iterator) *roaringIterator {
	return &roaringIterator{itr: itr}
}

// Seek moves the cursor to a pair matching bseek/pseek.
// If the pair is not found then it moves to the next pair.
func (itr *roaringIterator) Seek(bseek, pseek uint64) {
	itr.itr.Seek((bseek * ShardWidth) + pseek)
}

// Next returns the next column/row ID pair.
func (itr *roaringIterator) Next() (rowID, columnID uint64, eof bool) {
	v, eof := itr.itr.Next()
	return v / ShardWidth, v % ShardWidth, eof
}
