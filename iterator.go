package pilosa

import (
	"fmt"

	"github.com/umbel/pilosa/roaring"
)

// Iterator is an interface for looping over bitmap/profile pairs.
type Iterator interface {
	Seek(bitmapID, profileID uint64)
	Next() (bitmapID, profileID uint64, eof bool)
}

// BufIterator wraps an iterator to provide the ability to unread values.
type BufIterator struct {
	buf struct {
		bitmapID  uint64
		profileID uint64
		eof       bool
		full      bool
	}
	itr Iterator
}

// NewBufIterator returns a buffered iterator that wraps itr.
func NewBufIterator(itr Iterator) *BufIterator {
	return &BufIterator{itr: itr}
}

// Seek moves to the first pair equal to or greater than pseek/bseek.
func (itr *BufIterator) Seek(bitmapID, profileID uint64) {
	itr.buf.full = false
	itr.itr.Seek(bitmapID, profileID)
}

// Next returns the next pair in the bitmap.
// If a value has been buffered then it is returned and the buffer is cleared.
func (itr *BufIterator) Next() (bitmapID, profileID uint64, eof bool) {
	if itr.buf.full {
		itr.buf.full = false
		return itr.buf.bitmapID, itr.buf.profileID, itr.buf.eof
	}

	// Read values onto buffer in case of unread.
	itr.buf.bitmapID, itr.buf.profileID, itr.buf.eof = itr.itr.Next()

	return itr.buf.bitmapID, itr.buf.profileID, itr.buf.eof
}

// Peek reads the next value but leaves it on the buffer.
func (itr *BufIterator) Peek() (bitmapID, profileID uint64, eof bool) {
	bitmapID, profileID, eof = itr.Next()
	itr.Unread()
	return
}

// Unread pushes previous pair on to the buffer.
// Panics if the buffer is already full.
func (itr *BufIterator) Unread() {
	if itr.buf.full {
		panic("pilosa.BufIterator: buffer full")
	}
	itr.buf.full = true
}

// LimitIterator wraps an Iterator and limits it to a max profile/bitmap pair.
type LimitIterator struct {
	itr          Iterator
	maxBitmapID  uint64
	maxProfileID uint64

	eof bool
}

// NewLimitIterator returns a new LimitIterator.
func NewLimitIterator(itr Iterator, maxBitmapID, maxProfileID uint64) *LimitIterator {
	return &LimitIterator{
		itr:          itr,
		maxBitmapID:  maxBitmapID,
		maxProfileID: maxProfileID,
	}
}

// Seek moves the underlying iterator to a profile/bitmap pair.
func (itr *LimitIterator) Seek(bitmapID, profileID uint64) { itr.itr.Seek(bitmapID, profileID) }

// Next returns the next bitmap/profile ID pair.
// If the underlying iterator returns a pair higher than the max then EOF is returned.
func (itr *LimitIterator) Next() (bitmapID, profileID uint64, eof bool) {
	// Always return EOF once it is reached by limit or the underlying iterator.
	if itr.eof {
		return 0, 0, true
	}

	// Retrieve pair from underlying iterator.
	// Mark as EOF if it is beyond the limit (or at EOF).
	bitmapID, profileID, eof = itr.itr.Next()
	if eof || bitmapID > itr.maxBitmapID || (bitmapID == itr.maxBitmapID && profileID > itr.maxProfileID) {
		itr.eof = true
		return 0, 0, true
	}

	return bitmapID, profileID, false
}

// SliceIterator iterates over a pair of bitmap/profile ID slices.
type SliceIterator struct {
	bitmapIDs  []uint64
	profileIDs []uint64

	i, n int
}

// NewSliceIterator returns an iterator to iterate over a set of bitmap/profile ID pairs.
// Both slices MUST have an equal length. Otherwise the function will panic.
func NewSliceIterator(bitmapIDs, profileIDs []uint64) *SliceIterator {
	if len(profileIDs) != len(bitmapIDs) {
		panic(fmt.Sprintf("pilosa.SliceIterator: pair length mismatch: %d != %d", len(bitmapIDs), len(profileIDs)))
	}

	return &SliceIterator{
		bitmapIDs:  bitmapIDs,
		profileIDs: profileIDs,

		n: len(bitmapIDs),
	}
}

// Seek moves the cursor to a given pair.
// If the pair is not found, the iterator seeks to the next pair.
func (itr *SliceIterator) Seek(bseek, pseek uint64) {
	for i := 0; i < itr.n; i++ {
		bitmapID := itr.bitmapIDs[i]
		profileID := itr.profileIDs[i]

		if (bseek == bitmapID && pseek <= profileID) || bseek < bitmapID {
			itr.i = i
			return
		}
	}

	// Seek to the end of the slice if all values are less than seek pair.
	itr.i = itr.n
}

// Next returns the next bitmap/profile ID pair.
func (itr *SliceIterator) Next() (bitmapID, profileID uint64, eof bool) {
	if itr.i >= itr.n {
		return 0, 0, true
	}

	bitmapID = itr.bitmapIDs[itr.i]
	profileID = itr.profileIDs[itr.i]

	itr.i++
	return bitmapID, profileID, false
}

// RoaringIterator converts a roaring.Iterator to output profile/bitmap pairs.
type RoaringIterator struct {
	itr *roaring.Iterator
}

// NewRoaringIterator returns a new iterator wrapping itr.
func NewRoaringIterator(itr *roaring.Iterator) *RoaringIterator {
	return &RoaringIterator{itr: itr}
}

// Seek moves the cursor to a pair matching bseek/pseek.
// If the pair is not found then it moves to the next pair.
func (itr *RoaringIterator) Seek(bseek, pseek uint64) {
	itr.itr.Seek((bseek * SliceWidth) + pseek)
}

// Next returns the next profile/bitmap ID pair.
func (itr *RoaringIterator) Next() (bitmapID, profileID uint64, eof bool) {
	v, eof := itr.itr.Next()
	return v / SliceWidth, v % SliceWidth, eof
}
