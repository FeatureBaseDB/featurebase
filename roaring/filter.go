// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package roaring

import (
	"fmt"
	"math"

	"github.com/pkg/errors"

	"github.com/featurebasedb/featurebase/v3/shardwidth"
)

// We want BitmapScanner to be accessible from both the pilosa package, and
// the rbf package. Pilosa imports rbf, so rbf can't import pilosa, but they
// both import roaring, and this package is closely tied to roaring structures
// like Containers and the key/container mapping, so it mostly makes sense for
// this to be here.
//
// Unfortunately, this really needs to be capable of being row-aware, which
// means it needs access to the shard width stuff, which roaring otherwise
// studiously avoids knowing about.
const (
	rowExponent = (shardwidth.Exponent - 16) // for instance, 20-16 = 4
	rowWidth    = 1 << rowExponent           // containers per row, for instance 1<<4 = 16
	keyMask     = (rowWidth - 1)             // a mask for offset within the row
	rowMask     = ^FilterKey(keyMask)        // a mask for the row bits, without converting them to a row ID
)

type FilterKey uint64

// FilterResult represents the results of a BitmapFilter considering a
// key, or data. The values are represented as exclusive upper bounds
// on a series of matches followed by a series of rejections. So for
// instance, if called on key 23, the result {YesKey: 23, NoKey: 24}
// indicates that key 23 is a "no" and 24 is unknown and will be the
// next to be Consider()ed.  This may seem confusing but it makes the
// math a lot easier to write. It can also report an error, which
// indicates that the entire operation should be stopped with that
// error.
type FilterResult struct {
	YesKey FilterKey // The lowest container key this filter is known NOT to match.
	NoKey  FilterKey // The highest container key after YesKey that this filter is known to not match.
	Err    error     // An error which should terminate processing.
}

// Row() computes the row number of a key.
func (f FilterKey) Row() uint64 {
	return uint64(f >> rowExponent)
}

// Add adds an offset to a key.
func (f FilterKey) Add(x uint64) FilterKey {
	return f + FilterKey(x)
}

// Sub determines the distance from o to f.
func (f FilterKey) Sub(o FilterKey) uint64 {
	return uint64(f - o)
}

// MatchReject just sets Yes and No appropriately.
func (f FilterKey) MatchReject(y, n FilterKey) FilterResult {
	return FilterResult{YesKey: y, NoKey: n}
}

func (f FilterKey) MatchOne() FilterResult {
	return FilterResult{YesKey: f + 1, NoKey: f + 1}
}

// NeedData() is only really meaningful for ConsiderKey, and indicates
// that a decision can't be made from the key alone.
func (f FilterKey) NeedData() FilterResult {
	return FilterResult{}
}

// Fail() reports a fatal error that should terminate processing.
func (f FilterKey) Fail(err error) FilterResult {
	return FilterResult{Err: err}
}

// Failf() is just like Errorf, etc
func (f FilterKey) Failf(msg string, args ...interface{}) FilterResult {
	return FilterResult{Err: fmt.Errorf(msg, args...)}
}

// MatchRow indicates that the current row matches the filter.
func (f FilterKey) MatchRow() FilterResult {
	return FilterResult{YesKey: (f & rowMask) + rowWidth}
}

// MatchOneRejectRow indicates that this item matched but no further
// items in this row can match.
func (f FilterKey) MatchOneRejectRow() FilterResult {
	return FilterResult{YesKey: f + 1, NoKey: (f & rowMask) + rowWidth}
}

// Reject rejects this item only.
func (f FilterKey) RejectOne() FilterResult {
	return FilterResult{NoKey: f + 1}
}

// Reject rejects N items.
func (f FilterKey) Reject(n uint64) FilterResult {
	return FilterResult{NoKey: f.Add(n)}
}

// RejectRow indicates that this entire row is rejected.
func (f FilterKey) RejectRow() FilterResult {
	return FilterResult{NoKey: (f & rowMask) + rowWidth}
}

// RejectUntil rejects everything up to the given key.
func (f FilterKey) RejectUntil(until FilterKey) FilterResult {
	return FilterResult{NoKey: until}
}

// RejectUntilRow rejects everything until the given row ID.
func (f FilterKey) RejectUntilRow(rowID uint64) FilterResult {
	return FilterResult{NoKey: FilterKey(rowID) << rowExponent}
}

// MatchRowUntilRow matches this row, then rejects everything else until
// the given row ID.
func (f FilterKey) MatchRowUntilRow(rowID uint64) FilterResult {
	// if rows are 16 wide, "yes" will be 16 minus our current position
	// within a row, and "no" will be the distance from the end of our
	// current row to the start of rowID, which is also the distance from
	// the beginning of our current row to the start of rowID-1.
	return FilterResult{
		YesKey: (f & rowMask) + rowWidth,
		NoKey:  FilterKey(rowID) << rowExponent,
	}
}

// RejectUntilOffset rejects this container, and any others until the given
// in-row offset.
func (f FilterKey) RejectUntilOffset(offset uint64) FilterResult {
	next := (f & rowMask).Add(offset)
	if next <= f {
		next += rowWidth
	}
	return FilterResult{NoKey: next}
}

// MatchUntilOffset matches the current container, then skips any other
// containers until the given offset.
func (f FilterKey) MatchOneUntilOffset(offset uint64) FilterResult {
	r := f.RejectUntilOffset(offset)
	r.YesKey = f + 1
	return r
}

// Done indicates that nothing can ever match.
func (f FilterKey) Done() FilterResult {
	return FilterResult{
		NoKey: ^FilterKey(0),
	}
}

// MatchRowAndDone matches this row and nothing after that.
func (f FilterKey) MatchRowAndDone() FilterResult {
	return FilterResult{
		YesKey: (f & rowMask) + rowWidth,
		NoKey:  ^FilterKey(0),
	}
}

// Match the current container, then skip any others until the same offset
// is reached again.
func (f FilterKey) MatchOneUntilSameOffset() FilterResult {
	return f.MatchOneUntilOffset(uint64(f) & keyMask)
}

// A BitmapFilter, given a series of key/data pairs, is considered to "match"
// some of those containers. Matching may be dependent on key values and
// cardinalities alone, or on the contents of the container.
//
// The ConsiderData function must not retain the container, or the data
// from the container; if it needs access to that information later, it needs
// to make a copy.
//
// Many filters are, by virtue of how they operate, able to predict their
// results on future keys. To accommodate this, and allow operations to
// avoid processing keys they don't need to process, the result of a filter
// operation can indicate not just whether a given key matches, but whether
// some upcoming keys will, or won't, match. If ConsiderKey yields a non-zero
// number of matches or non-matches for a given key, ConsiderData will not be
// called for that key.
//
// If multiple filters are combined, they are only called if their input is
// needed to determine a value.
type BitmapFilter interface {
	ConsiderKey(key FilterKey, n int32) FilterResult
	ConsiderData(key FilterKey, data *Container) FilterResult
}

// ContainerWriteback is the type for functions which can feed updated
// containers back to things from filters.
type ContainerWriteback func(key FilterKey, data *Container) error

// A BitmapRewriter is like a bitmap filter, but can modify the bitmap
// it's being called on during the iteration.
//
// After the last container is returned, ConsiderData will be called with
// an unspecified key and a nil container pointer, so the rewriter can
// write any trailing containers it has. A nil container passed to writeback
// implies a delete operation on the container. Writeback should only be
// called with keys greater than any previously given container key, and
// less than or equal to the current key. So for instance, if
// ConsiderData is called with key 3, and then with key 5, the call with
// key 5 may call writeback with key 4, and then key 5, but may not call
// it with keys 3 or lower, or 6 or higher. When the container provided to
// the call is nil, any monotonically increasing keys greater than the
// previous key are allowed. (If there was no previous key, 0 and higher
// are allowed.)
type BitmapRewriter interface {
	ConsiderKey(key FilterKey, n int32) FilterResult
	RewriteData(key FilterKey, data *Container, writeback ContainerWriteback) FilterResult
}

// BitmapColumnFilter is a BitmapFilter which checks for containers matching
// a given column within a row; thus, only the one container per row which
// matches the column needs to be evaluated, and it's evaluated as matching
// if it contains the relevant bit.
type BitmapColumnFilter struct {
	key, offset uint16
}

var _ BitmapFilter = &BitmapColumnFilter{}

func (f *BitmapColumnFilter) ConsiderKey(key FilterKey, n int32) FilterResult {
	if uint16(key&keyMask) != f.key {
		return key.RejectUntilOffset(uint64(f.key))
	}
	return key.NeedData()
}

func (f *BitmapColumnFilter) ConsiderData(key FilterKey, data *Container) FilterResult {
	if data.Contains(f.offset) {
		return key.MatchOneUntilSameOffset()
	}
	return key.RejectUntilOffset(uint64(f.key))
}

func NewBitmapColumnFilter(col uint64) BitmapFilter {
	return &BitmapColumnFilter{key: uint16((col >> 16) & keyMask), offset: uint16(col & 0xFFFF)}
}

// BitmapRowsFilter is a BitmapFilter which checks for containers that are
// in any of a provided list of rows. The row list should be sorted.
type BitmapRowsFilter struct {
	rows []uint64
	i    int
}

func (f *BitmapRowsFilter) ConsiderKey(key FilterKey, n int32) FilterResult {
	if f.i == -1 {
		return key.Done()
	}
	if n == 0 {
		return key.RejectOne()
	}
	row := uint64(key) >> rowExponent
	for f.rows[f.i] < row {
		f.i++
		if f.i >= len(f.rows) {
			f.i = -1
			return key.Done()
		}
	}
	if f.rows[f.i] > row {
		return key.RejectUntilRow(f.rows[f.i])
	}
	// rows[f.i] must be equal, so we should match this row, until the
	// next row, if there is a next row.
	if f.i+1 < len(f.rows) {
		return key.MatchRowUntilRow(f.rows[f.i+1])
	}
	return key.MatchRowAndDone()
}

func (f *BitmapRowsFilter) ConsiderData(key FilterKey, data *Container) FilterResult {
	return key.Fail(errors.New("bitmap rows filter should never consider data"))
}

func NewBitmapRowsFilter(rows []uint64) BitmapFilter {
	if len(rows) == 0 {
		return &BitmapRowsFilter{rows: rows, i: -1}
	}
	return &BitmapRowsFilter{rows: rows, i: 0}
}

// BitmapRowsUnion is a BitmapFilter which produces a union of all the
// rows listed in a []uint64.
type BitmapRowsUnion struct {
	c    []*Container
	rows []uint64
	i    int
}

func (f *BitmapRowsUnion) ConsiderKey(key FilterKey, n int32) FilterResult {
	if f.i == -1 {
		return key.Done()
	}
	if n == 0 {
		return key.RejectOne()
	}
	row := uint64(key) >> rowExponent
	for f.rows[f.i] < row {
		f.i++
		if f.i >= len(f.rows) {
			f.i = -1
			return key.Done()
		}
	}
	if f.rows[f.i] > row {
		return key.RejectUntilRow(f.rows[f.i])
	}
	// If we ran out of rows, we said we were done. If we're
	// waiting for a later row, we said to reject until then.
	// Therefore, we're on the current row, and need the data because
	// we're going to union it.
	return key.NeedData()
}

func (f *BitmapRowsUnion) ConsiderData(key FilterKey, data *Container) FilterResult {
	idx := key & keyMask
	f.c[idx] = f.c[idx].UnionInPlace(data)
	// UnionInPlace with nil will reuse the container. We don't want to reuse
	// the container, because ApplyFilter will overwrite it.
	if f.c[idx] == data {
		f.c[idx] = data.Clone()
	}
	return key.MatchOne()
}

// Yield the bitmap containing our results, adjusted for a particular shard
// if necessary (because we expect the results to correspond to our shard
// ID).
func (f *BitmapRowsUnion) Results(shard uint64) *Bitmap {
	b := NewSliceBitmap()
	for i, c := range f.c {
		// UnionInPlace might not have fixed count
		c.Repair()
		b.Containers.Put(uint64(i)+shard*rowWidth, c)
	}
	return b
}

// Reset the internal container buffer. You must use this before reusing a
// filter.
func (f *BitmapRowsUnion) Reset() {
	for i := range f.c {
		f.c[i] = nil
	}
}

// NewBitmapRowsUnion yields a BitmapRowsUnion which can give you the union
// of all containers matching a given row.
func NewBitmapRowsUnion(rows []uint64) *BitmapRowsUnion {
	if len(rows) == 0 {
		return &BitmapRowsUnion{rows: rows, i: -1, c: make([]*Container, rowWidth)}
	}
	return &BitmapRowsUnion{rows: rows, i: 0, c: make([]*Container, rowWidth)}
}

// BitmapRowFilterBase is a generic form of a row-aware wrapper; it
// handles making decisions about keys once you tell it a yesKey and noKey
// that it should be using, and makes callbacks per row.
type BitmapRowFilterBase struct {
	FilterResult
	callback func(row uint64) error
	lastRow  uint64
}

var _ BitmapFilter = &BitmapRowFilterBase{}

// DetermineByKey decides whether it can produce a meaningful FilterResult
// for a given key. This encapsulates all the logic for row callbacks and
// figuring out when to wrap a row.
func (b *BitmapRowFilterBase) DetermineByKey(key FilterKey) (FilterResult, bool) {
	if b.FilterResult.Err != nil {
		return b.FilterResult, true
	}
	row := key.Row()
	if b.YesKey <= key && b.NoKey > key {
		return key.RejectUntil(b.NoKey), true
	}
	if b.lastRow == row {
		return key.RejectRow(), true
	}
	// If we got here: Either b.noKey is less than key, or b.yesKey is
	// greater than key. If yesKey is greater, we match this row, and
	// possibly update to mark that we've said no through to the end
	// of this row.
	if b.YesKey > key {
		b.lastRow = row
		if b.callback != nil {
			err := b.callback(row)
			if err != nil {
				return key.Fail(err), true
			}
		}
		res := key.MatchOneRejectRow()
		// This is probably unnecessary, but the idea is, since
		// we've decided that we're rejecting everything up to the
		// end of this row, we want to be sure that a later call
		// doesn't produce a different answer.
		if b.NoKey < res.NoKey {
			b.NoKey = res.NoKey
		}
		// if our run of yes answers ends before the rejected row
		// ends, and our run of no answers extends beyond this row,
		// we can reject until then. note that we can't round that
		// up to a full row; if our inner filter were a column
		// filter, for instance, that only wanted to see the 7th
		// key in each row, we would want to reject up to that 7th
		// key, but then look at it.
		if b.YesKey <= res.NoKey && b.NoKey > res.NoKey {
			res.NoKey = b.NoKey
		}
		return res, true
	}
	// Both keys are <= key, err is nil, so this is basically a
	// NeedData.
	return b.FilterResult, false
}

// SetResult is a convenience function so that things embedding this
// can just call this instead of using a long series of dotted names.
// It returns the new result of DetermineByKey after this change.
func (b *BitmapRowFilterBase) SetResult(key FilterKey, result FilterResult) FilterResult {
	b.FilterResult = result
	result, _ = b.DetermineByKey(key)
	return result
}

// Without a sub-filter, we always-succeed; if we get a key that isn't
// already answered by our YesKey/NoKey/lastRow, we will match this key,
// reject the rest of the row, and update our keys accordingly. We'll
// also hit the callback, and return an error from it if appropriate.
func (b *BitmapRowFilterBase) ConsiderKey(key FilterKey, n int32) FilterResult {
	var done bool
	b.FilterResult, done = b.DetermineByKey(key)
	if done {
		return b.FilterResult
	}
	if n == 0 {
		return key.RejectOne()
	}
	b.FilterResult = key.MatchOneRejectRow()
	row := key.Row()
	b.lastRow = row
	if b.callback != nil {
		b.Err = b.callback(row)
	}
	return b.FilterResult
}

// This should probably never be reached?
func (b *BitmapRowFilterBase) ConsiderData(key FilterKey, data *Container) FilterResult {
	b.Err = errors.New("base iterator should never consider data")
	return b.FilterResult
}

func NewBitmapRowFilterBase(callback func(row uint64) error) *BitmapRowFilterBase {
	return &BitmapRowFilterBase{lastRow: ^uint64(0), callback: callback}
}

type BitmapRowLimitFilter struct {
	BitmapRowFilterBase
	limit uint64
}

var _ BitmapFilter = &BitmapRowLimitFilter{}

// Without a sub-filter, we always-succeed; if we get a key that isn't
// already answered by our YesKey/NoKey/lastRow, we will match the whole
// row.
func (b *BitmapRowLimitFilter) ConsiderKey(key FilterKey, n int32) FilterResult {
	var done bool
	b.FilterResult, done = b.DetermineByKey(key)
	if done {
		return b.FilterResult
	}
	if n == 0 {
		return key.RejectOne()
	}
	if b.limit > 0 {
		b.FilterResult = key.MatchRow()
		b.limit--
	} else {
		b.FilterResult = key.Done()
	}
	return b.FilterResult
}

// This should probably never be reached?
func (b *BitmapRowLimitFilter) ConsiderData(key FilterKey, data *Container) FilterResult {
	b.Err = errors.New("limit iterator should never consider data")
	return b.FilterResult
}

func NewBitmapRowLimitFilter(limit uint64) *BitmapRowLimitFilter {
	return &BitmapRowLimitFilter{BitmapRowFilterBase: *NewBitmapRowFilterBase(nil), limit: limit}
}

// BitmapRowFilterSingleFilter is a row iterator with a single
// filter, which is simpler than one with multiple filters where
// it coincidentally turns out that N==1.
type BitmapRowFilterSingleFilter struct {
	BitmapRowFilterBase
	filter BitmapFilter
}

func (b *BitmapRowFilterSingleFilter) ConsiderKey(key FilterKey, n int32) FilterResult {
	res, done := b.DetermineByKey(key)
	if done {
		return res
	}
	return b.SetResult(key, b.filter.ConsiderKey(key, n))
}

func (b *BitmapRowFilterSingleFilter) ConsiderData(key FilterKey, data *Container) FilterResult {
	// We already handled any consideration of the key above, in principle.
	b.FilterResult = b.filter.ConsiderData(key, data)
	if b.FilterResult.Err != nil {
		return b.FilterResult
	}
	res, done := b.DetermineByKey(key)
	if done {
		return res
	}
	// We could just return the res, which would say nothing, but I
	// think it should be a visible error if that happens.
	b.FilterResult.Err = errors.New("inner filter didn't make a decision")
	return b.FilterResult
}

func NewBitmapRowFilterSingleFilter(callback func(row uint64) error, filter BitmapFilter) *BitmapRowFilterSingleFilter {
	return &BitmapRowFilterSingleFilter{
		BitmapRowFilterBase: BitmapRowFilterBase{lastRow: ^uint64(0), callback: callback},
		filter:              filter,
	}
}

// BitmapRowFilterMultiFilter is a BitmapFilter which wraps other bitmap filters,
// calling a callback function once per row whenever it finds a container
// for which all the filters returned true.
type BitmapRowFilterMultiFilter struct {
	BitmapRowFilterBase
	filters         []BitmapFilter
	yesKeys, noKeys []FilterKey
	toDo            []int
}

func (b *BitmapRowFilterMultiFilter) ConsiderKey(key FilterKey, n int32) FilterResult {
	res, done := b.DetermineByKey(key)
	if done {
		return res
	}
	// highestNo: The highest No value that we have that isn't preceeded
	// by a relevant Yes.
	highestNo := key
	lowestYes := ^FilterKey(0)
	// The length of the "no" run after the lowest "yes"
	lowestYesNo := FilterKey(0)
	b.toDo = b.toDo[:0]
	// We scan for any no values that don't have an earlier yes that's
	// still greater than this key. If there are any, we can skip to
	// the highest such value immediately. We also build a todo list
	// of items for which we have neither a yes nor a no answer greater
	// than this key.
	for i, yk := range b.yesKeys {
		if yk > key {
			if yk < lowestYes {
				lowestYes = yk
				lowestYesNo = b.noKeys[i]
			}
			continue
		}
		nk := b.noKeys[i]
		if nk > highestNo {
			highestNo = nk
			continue
		}
		b.toDo = append(b.toDo, i)
	}
	// We have an unambiguous no, so we can set our internal state to
	// be aware that we have a No until then. We can unconditionally
	// return the result; it can't be not-done, because we just set
	// it to a known done state.
	if highestNo > key {
		return b.SetResult(key, key.RejectUntil(highestNo))
	}
	// Everything either has a yes value which is at least as high
	// as lowestYes, or is in f.toDo now. Now we call ConsiderKey
	// for everything in f.toDo, and accumulate a new list of the
	// values still don't know, using the same backing store.
	newToDo := b.toDo[:0]
	for _, filter := range b.toDo {
		result := b.filters[filter].ConsiderKey(key, n)
		if result.Err != nil {
			return key.Fail(result.Err)
		}
		yk, nk := result.YesKey, result.NoKey
		b.yesKeys[filter], b.noKeys[filter] = yk, nk
		if yk > key {
			if lowestYes == 0 || yk < lowestYes {
				lowestYes = yk
				lowestYesNo = nk
			}
			continue
		}
		if nk > highestNo {
			highestNo = nk
			continue
		}
		newToDo = append(newToDo, filter)
	}
	// Same logic as before; if we have a highestNo, we don't need more
	// information.
	if highestNo > key {
		return b.SetResult(key, key.RejectUntil(highestNo))
	}
	b.toDo = newToDo
	if len(b.toDo) > 0 {
		return key.NeedData()
	}
	// this shouldn't be possible
	if lowestYes <= key {
		return key.Failf("got lowest yes %d for key %d, this shouldn't happen", lowestYes, key)
	}
	// Flag that we have a definite Yes as far as the lowest yes, and a
	// definite No after that to the corresponding No.
	return b.SetResult(key, key.MatchReject(lowestYes, lowestYesNo))
}

// ConsiderData only gets called in cases where f.toDo had a list of filters
// for which we needed to get data to make a decision. That means that
// everything but the indexes in f.toDo must be a "yes" right now.
func (b *BitmapRowFilterMultiFilter) ConsiderData(key FilterKey, data *Container) FilterResult {
	res, done := b.DetermineByKey(key)
	if done {
		return res
	}
	highestNo := key
	for _, filter := range b.toDo {
		result := b.filters[filter].ConsiderData(key, data)
		if result.Err != nil {
			return key.Fail(result.Err)
		}
		yk, nk := result.YesKey, result.NoKey
		b.yesKeys[filter], b.noKeys[filter] = yk, nk
		if yk <= key && nk > highestNo {
			highestNo = nk
		}
	}
	if highestNo > key {
		return b.SetResult(key, key.RejectUntil(highestNo))
	}
	// if we got here, either something was buggy, or everything has a yes
	// > key.
	lowestYes := ^FilterKey(0)
	lowestYesNo := key
	for i, yk := range b.yesKeys {
		if yk < lowestYes {
			lowestYes = yk
			lowestYesNo = b.noKeys[i]
		}
	}
	// this shouldn't be possible
	if lowestYes <= key {
		return key.Failf("got lowest yes %d on data for key %d, this shouldn't happen", lowestYes, key)
	}
	return b.SetResult(key, key.MatchReject(lowestYes, lowestYesNo))
}

// BitmapBitmap filter builds a list of positions in the bitmap which
// match those in a provided bitmap. It is shard-agnostic; no matter what
// offsets the input bitmap's containers have, it matches them against
// corresponding keys.
type BitmapBitmapFilter struct {
	containers  []*Container
	nextOffsets []uint64
	callback    func(uint64) error
}

func (b *BitmapBitmapFilter) SetCallback(cb func(uint64) error) {
	b.callback = cb
}

func (b *BitmapBitmapFilter) ConsiderKey(key FilterKey, n int32) FilterResult {
	pos := key & keyMask
	if b.containers[pos] == nil || n == 0 {
		return key.RejectUntilOffset(b.nextOffsets[pos])
	}
	return key.NeedData()
}

func (b *BitmapBitmapFilter) ConsiderData(key FilterKey, data *Container) FilterResult {
	pos := key & keyMask
	base := uint64(key << 16)
	filter := b.containers[pos]
	if filter == nil {
		return key.RejectUntilOffset(b.nextOffsets[pos])
	}
	var lastErr error
	matched := false
	intersectionCallback(data, filter, func(v uint16) {
		matched = true
		err := b.callback(base + uint64(v))
		if err != nil {
			lastErr = err
		}
	})
	if lastErr != nil {
		return key.Fail(lastErr)
	}
	if !matched {
		return key.RejectUntilOffset(b.nextOffsets[pos])
	}
	return key.MatchOneUntilOffset(b.nextOffsets[pos])
}

// NewBitmapBitmapFilter creates a filter which can report all the positions
// within a bitmap which are set, and which have positions corresponding to
// the specified columns. It calls the provided callback function on
// each value it finds, terminating early if that returns an error.
//
// The input filter is assumed to represent one "row" of a shard's data,
// which is to say, a range of up to rowWidth consecutive containers starting
// at some multiple of rowWidth. We coerce that to the 0..rowWidth range
// because offset-within-row is what we care about.
func NewBitmapBitmapFilter(filter *Bitmap, callback func(uint64) error) *BitmapBitmapFilter {
	b := &BitmapBitmapFilter{
		callback:    callback,
		containers:  make([]*Container, rowWidth),
		nextOffsets: make([]uint64, rowWidth),
	}
	iter, _ := filter.Containers.Iterator(0)
	last := uint64(0)
	count := 0
	for iter.Next() {
		k, v := iter.Value()
		// Coerce container key into the 0-rowWidth range we'll be
		// using to compare against containers within each row.
		k = k & keyMask
		b.containers[k] = v
		last = k
		count++
	}
	// if there's only one container, we need to populate everything with
	// its position.
	if count == 1 {
		for i := range b.containers {
			b.nextOffsets[i] = last
		}
	} else {
		// Point each container at the offset of the next valid container.
		// With sparse bitmaps this will potentially make skipping faster.
		for i := range b.containers {
			if b.containers[i] != nil {
				for int(last) != i {
					b.nextOffsets[last] = uint64(i)
					last = (last + 1) % rowWidth
				}
			}
		}
	}
	return b
}

// BitmapRowFilterMultiFilter will call a
func NewBitmapRowFilterMultiFilter(callback func(row uint64) error, filters ...BitmapFilter) BitmapFilter {
	return &BitmapRowFilterMultiFilter{
		filters: filters,
		yesKeys: make([]FilterKey, len(filters)),
		noKeys:  make([]FilterKey, len(filters)),
		BitmapRowFilterBase: BitmapRowFilterBase{
			callback: callback,
			lastRow:  ^uint64(0),
		},
	}
}

// BitmapRowLister returns a pointer to a slice which it will populate when invoked
// as a bitmap filter.
func NewBitmapRowFilter(callback func(uint64) error, filters ...BitmapFilter) BitmapFilter {
	if len(filters) == 0 {
		return NewBitmapRowFilterBase(callback)
	}
	if len(filters) == 1 {
		return NewBitmapRowFilterSingleFilter(callback, filters[0])
	}
	return NewBitmapRowFilterMultiFilter(callback, filters...)
}

// BitmapRangeFilter limits filter operations to a specified range, and
// performs key or data callbacks.
//
// On seeing a key in its range:
// If the key callback is present, and returns true, match the key.
// Otherwise, if a data callback is present, request the data, and in the
// data handler, call the data callback, then match the single key.
// If neither is present, match the entire range at once.
type BitmapRangeFilter struct {
	min, max FilterKey
	kcb      func(FilterKey, int32) (bool, error)
	dcb      func(FilterKey, *Container) error
}

var _ BitmapFilter = &BitmapRangeFilter{}

func (b *BitmapRangeFilter) ConsiderKey(key FilterKey, n int32) FilterResult {
	if key >= b.max {
		return key.Done()
	}
	if key >= b.min {
		if b.kcb != nil {
			match, err := b.kcb(key, n)
			if err != nil {
				return key.Fail(err)
			}
			if match {
				return key.MatchOne()
			}
		}
		if b.dcb != nil {
			return key.NeedData()
		}
		return key.MatchReject(b.max, ^FilterKey(0))
	}
	return key.RejectUntil(b.min)
}

func (b *BitmapRangeFilter) ConsiderData(key FilterKey, data *Container) FilterResult {
	err := b.dcb(key, data)
	if err != nil {
		return key.Fail(err)
	}
	return key.MatchOne()
}

func NewBitmapRangeFilter(min, max FilterKey, keyCallback func(FilterKey, int32) (bool, error), dataCallback func(FilterKey, *Container) error) *BitmapRangeFilter {
	return &BitmapRangeFilter{min: min, max: max, kcb: keyCallback, dcb: dataCallback}
}

// BitmapMutexDupFilter is a filter which identifies cases where the same
// position has a bit set in more than one row.
//
// We keep a slice of the first value seen for every row, with ^0 as the
// default; when that's already set, things get appended to the entries in
// the map. At the end, for each entry in the map, we also add its first
// value to it. Thus, the map holds all the entries, but we're only using
// the map in the (hopefully rarer) cases where there's duplicate values.
//
// The slice is local-coordinates (first column 0), but the map is global
// coordinates (first column is whatever base was).
type BitmapMutexDupFilter struct {
	base    uint64              // the offset of 0 for this, used to accommodate shard offsets
	extra   map[uint64][]uint64 // extra values observed
	first   []uint64            // first values observed
	details bool
	limit   int
	done    bool      // if we have a limit, and we've hit it...
	highKey FilterKey // ... we can stop after this many containers.
}

var _ BitmapFilter = &BitmapMutexDupFilter{}

func NewBitmapMutexDupFilter(base uint64, details bool, limit int) *BitmapMutexDupFilter {
	filter := &BitmapMutexDupFilter{
		base:    base,
		extra:   map[uint64][]uint64{},
		first:   make([]uint64, 1<<shardwidth.Exponent),
		details: details,
		limit:   limit,
	}
	if filter.limit == 0 {
		// A limit of 0 is not a limit; set limit higher than possible number of
		// values we could have.
		filter.limit = 2 << shardwidth.Exponent
	}
	for i := range filter.first {
		filter.first[i] = ^uint64(0)
	}
	return filter
}

func (b *BitmapMutexDupFilter) ConsiderKey(key FilterKey, n int32) FilterResult {
	if n > 0 {
		return key.NeedData()
	}
	return key.RejectOne()
}

func (b *BitmapMutexDupFilter) ConsiderData(key FilterKey, data *Container) FilterResult {
	value, basePos := uint64(key)>>rowExponent, uint64(key&keyMask)<<16
	containerCallback(data, func(u uint16) {
		pos := basePos + uint64(u)
		if b.first[pos] != ^uint64(0) {
			if b.details {
				b.extra[pos+b.base] = append(b.extra[pos+b.base], value)
			} else {
				// no details, just annotate that it exists
				b.extra[pos+b.base] = []uint64{}
			}
		} else {
			b.first[pos] = value
		}
	})
	if len(b.extra) >= b.limit {
		if !b.done {
			// we note which container we found the last value we needed in.
			// We may still go over the limit, but we won't look at any *more*
			// containers in this row.
			//
			// We can't just abort early because the records we already found
			// could have more values.
			b.done = true
			b.highKey = key & keyMask
			return key.RejectRow()
		}
		if (key & keyMask) >= b.highKey {
			return key.RejectRow()
		}
	}
	return key.MatchOne()
}

// Report returns the set of duplicate values identified.
func (b *BitmapMutexDupFilter) Report() map[uint64][]uint64 {
	// copy values into extra, and remove them from first, so calling
	// Report() again won't cause double-appends. We only have to do
	// this if we've been asked for details; otherwise the list of
	// known positions is sufficient.
	if b.details {
		for k, v := range b.extra {
			kpos := k % (1 << shardwidth.Exponent)
			if b.first[kpos] != ^uint64(0) {
				v = append(v, 0)
				// prepend so the lowest value goes at the beginning
				copy(v[1:], v[:])
				v[0] = b.first[kpos]
				b.first[kpos] = ^uint64(0)
				b.extra[k] = v
			}
		}
	}
	return b.extra
}

// BitmapBitmapTrimmer is like BitmapBitmapFilter, but instead of calling
// a callback per bit found in the intersection, it calls a callback with the
// original raw container and the corresponding filter container, and also
// provides the writeback func it got from the bitmap. So for instance, to
// implement a "subtract these bits" function, you would difference-in-place
// the raw container with the filter container, then pass that to the writeback
// function.
//
// It's called a Trimmer because it won't add containers; it won't *add*
// containers. It calls the callback function for every container, whether or
// not it matches the filter; this allows an intersect-like filter to work
// too.
//
// Note, however, that the caller's ContainerWriteback function *may* create
// containers, even though the Trimmer won't have called RewriteData with those
// keys.
type BitmapBitmapTrimmer struct {
	containers  []*Container
	nextOffsets []uint64
	callback    func(key FilterKey, raw, filter *Container, writeback ContainerWriteback) error
}

var _ BitmapRewriter = &BitmapBitmapTrimmer{}

func (b *BitmapBitmapTrimmer) SetCallback(cb func(FilterKey, *Container, *Container, ContainerWriteback) error) {
	b.callback = cb
}

func (b *BitmapBitmapTrimmer) ConsiderKey(key FilterKey, n int32) FilterResult {
	pos := key & keyMask
	// If a trimmer wants to do something for a key, it *must* have something in
	// the filter slot for that key, otherwise we won't call it with the
	// corresponding existing data. For the mutex case, this is covered -- every
	// bit we have to write implies the corresponding filter bit being set.
	//
	// Note that, unlike BitmapBitmapFilter, we don't make assumptions about
	// what you are *doing* with the filter.
	if b.containers[pos] == nil || n == 0 {
		return key.RejectUntilOffset(b.nextOffsets[pos])
	}
	return key.NeedData()
}

func (b *BitmapBitmapTrimmer) RewriteData(key FilterKey, data *Container, writeback ContainerWriteback) FilterResult {
	pos := key & keyMask
	filter := b.containers[pos]
	err := b.callback(key, data, filter, writeback)
	if err != nil {
		return key.Fail(err)
	}
	return key.MatchOneUntilOffset(b.nextOffsets[pos])
}

// NewBitmapBitmapTrimmer creates a filter which calls a callback on every
// container in a bitmap, with corresponding elements from an initial filter
// bitmap. It does not call its callback for cases where there's no container
// in the original bitmap.
//
// The input filter is assumed to represent one "row" of a shard's data,
// which is to say, a range of up to rowWidth consecutive containers starting
// at some multiple of rowWidth. We coerce that to the 0..rowWidth range
// because offset-within-row is what we care about.
func NewBitmapBitmapTrimmer(filter *Bitmap, callback func(FilterKey, *Container, *Container, ContainerWriteback) error) *BitmapBitmapTrimmer {
	b := &BitmapBitmapTrimmer{
		callback:    callback,
		containers:  make([]*Container, rowWidth),
		nextOffsets: make([]uint64, rowWidth),
	}
	iter, _ := filter.Containers.Iterator(0)
	last := uint64(0)
	count := 0
	for iter.Next() {
		k, v := iter.Value()
		// Coerce container key into the 0-rowWidth range we'll be
		// using to compare against containers within each row.
		k = k & keyMask
		b.containers[k] = v
		last = k
		count++
	}
	// if there's only one container, we need to populate everything with
	// its position.
	if count == 1 {
		for i := range b.containers {
			b.nextOffsets[i] = last
		}
	} else {
		// Point each container at the offset of the next valid container.
		// With sparse bitmaps this will potentially make skipping faster.
		for i := range b.containers {
			if b.containers[i] != nil {
				for int(last) != i {
					b.nextOffsets[last] = uint64(i)
					last = (last + 1) % rowWidth
				}
			}
		}
	}
	return b
}

// ApplyFilterToIterator is a simplistic implementation that applies a bitmap
// filter to a ContainerIterator, returning an error if it encounters an error.
//
// This mostly exists for testing purposes; a Tx implementation where generating
// containers is expensive should almost certainly implement a better way to
// use filters which only generates data if it needs to.
func ApplyFilterToIterator(filter BitmapFilter, iter ContainerIterator) error {
	defer iter.Close()
	var until = uint64(0)
	for (until < ^uint64(0)) && iter.Next() {
		key, data := iter.Value()
		if key < until {
			continue
		}
		result := filter.ConsiderKey(FilterKey(key), data.N())
		if result.Err != nil {
			return result.Err
		}
		until = uint64(result.NoKey)
		if key < until {
			continue
		}
		result = filter.ConsiderData(FilterKey(key), data)
		if result.Err != nil {
			return result.Err
		}
		until = uint64(result.NoKey)
	}
	return nil
}

// BitmapBSICountFilter gives counts of values in each value-holding row
// of a BSI field, constrained by a filter. The first row of the data is
// taken to be an existence bit, which is intersected into the filter to
// constrain it, and the second is used as a sign bit. The rows after that
// are treated as value rows, and their counts of bits, overlapping with
// positive and negative bits in the sign rows, are returned to a callback
// function.
//
// The total counts of positions evaluated are returned with a row count
// of ^uint64(0) prior to row counts.
type BitmapBSICountFilter struct {
	containers  []*Container
	positive    []*Container
	negative    []*Container
	nextOffsets []uint64
	count       int32
	psum, nsum  uint64
}

func (b *BitmapBSICountFilter) Total() (count int32, total int64) {
	return b.count, int64(b.psum) - int64(b.nsum)
}

func (b *BitmapBSICountFilter) ConsiderKey(key FilterKey, n int32) FilterResult {
	pos := key & keyMask
	if b.containers[pos] == nil || n == 0 {
		return key.RejectUntilOffset(b.nextOffsets[pos])
	}
	return key.NeedData()
}

func (b *BitmapBSICountFilter) ConsiderData(key FilterKey, data *Container) FilterResult {
	pos := key & keyMask
	filter := b.containers[pos]
	if filter == nil {
		return key.RejectUntilOffset(b.nextOffsets[pos])
	}
	row := uint64(key >> rowExponent) // row count within the fragment
	// How do we translate the filter and existence bit into actionable things?
	// Assume the sign row is empty. We want positive values for anything in
	// the intersection of the filter and the positive bits. If the sign row
	// isn't empty, we want positive values for that intersection, less the
	// sign row, and negative for the intersection of the filter/positive and
	// the sign bits. So we can just stash the intermediate filter+existence
	// as positive, then split it up if we have sign bits, which we often don't.
	setup := false
	switch row {
	case 0: // existence bit
		b.positive[pos] = intersect(b.containers[pos], data)
		if b.positive[pos] == data {
			b.positive[pos] = b.positive[pos].Clone()
		}
		b.count += int32(b.positive[pos].N())
		setup = true
	case 1: // sign bit
		// split into negative/positive components. doesn't affect total
		// count.
		b.negative[pos] = intersect(b.positive[pos], data)
		if b.negative[pos] == data {
			b.negative[pos] = b.negative[pos].Clone()
		}
		b.positive[pos] = difference(b.positive[pos], data)
		setup = true
	}
	// if we were doing setup (first two rows), we're done
	if setup {
		return key.MatchOneUntilOffset(b.nextOffsets[pos])
	}
	// helpful reminder: a nil container is a valid empty container, and
	// intersectionCount knows this.
	pcount := intersectionCount(b.positive[pos], data)
	ncount := intersectionCount(b.negative[pos], data)
	b.psum += (uint64(pcount) << (row - 2))
	b.nsum += (uint64(ncount) << (row - 2))
	return key.MatchOneUntilOffset(b.nextOffsets[pos])
}

// NewBitmapBSICountFilter creates a BitmapBSICountFilter, used for tasks
// like computing the sum of a BSI field matching a given filter.
//
// The input filter is assumed to represent one "row" of a shard's data,
// which is to say, a range of up to rowWidth consecutive containers starting
// at some multiple of rowWidth. We coerce that to the 0..rowWidth range
// because offset-within-row is what we care about.
func NewBitmapBSICountFilter(filter *Bitmap) *BitmapBSICountFilter {
	containers := make([]*Container, rowWidth*3)
	b := &BitmapBSICountFilter{
		containers:  containers[:rowWidth],
		positive:    containers[rowWidth : rowWidth*2],
		negative:    containers[rowWidth*2 : rowWidth*3],
		nextOffsets: make([]uint64, rowWidth),
	}
	if filter == nil {
		for i := range b.containers {
			b.containers[i] = NewContainerRun([]Interval16{{Start: 0, Last: 65535}})
			b.nextOffsets[i] = uint64(i+1) % rowWidth
		}
		return b
	}
	count := 0
	iter, _ := filter.Containers.Iterator(0)
	last := uint64(0)
	for iter.Next() {
		k, v := iter.Value()
		// Coerce container key into the 0-rowWidth range we'll be
		// using to compare against containers within each row.
		k = k & keyMask
		b.containers[k] = v
		last = k
		count++
	}
	// if there's only one container, we need to populate everything with
	// its position.
	if count == 1 {
		for i := range b.containers {
			b.nextOffsets[i] = last
		}
	} else {
		// Point each container at the offset of the next valid container.
		// With sparse bitmaps this will potentially make skipping faster.
		for i := range b.containers {
			if b.containers[i] != nil {
				for int(last) != i {
					b.nextOffsets[last] = uint64(i)
					last = (last + 1) % rowWidth
				}
			}
		}
	}

	return b
}

// getNextFromIterator is a convenience function which calls Next and then Value
// on a ContainerIterator and changes the key to a FilterKey, and
// returns KEY_DONE if the iterator is done.
func getNextFromIterator(contIter ContainerIterator) (FilterKey, *Container) {
	if !contIter.Next() {
		return KEY_DONE, nil
	}
	key, val := contIter.Value()
	return FilterKey(key), val
}

// NewRepeatedRowIteratorFromBytes interprets "data" as a roaring
// bitmap and returns a ContainerIterator which will repeatedly return
// the first "row" of data as determined by shard width, but with
// increasing keys. It is essentially a conveniece wrapper around
// NewContainerIterator and NewRepeatedRowContainerIterator. It treats
// empty "data" as valid and returns a no-op iterator.
func NewRepeatedRowIteratorFromBytes(data []byte) (ContainerIterator, error) {
	iter, err := NewContainerIterator(data)
	if err != nil {
		return nil, errors.Wrap(err, "getting container iterator")
	}
	return NewRepeatedRowContainerIterator(iter), nil
}

// NewRepeatedRowContainerIterator returns a ContainerIterator which
// reads the first "row" of containers out of iter (as determined by
// shard width, so up to and including key 15 by default). It then
// returns a ContainerIterator which will repeatedly emit the
// containers in that row, but with increasing keys such that each
// time a particular container is emitted it has its key from the last
// time plus number of containers in a row (16 by default).
func NewRepeatedRowContainerIterator(iter ContainerIterator) *repeatedRowIterator {
	conts := getFirstRowAsContainers(iter)
	return &repeatedRowIterator{
		containers: conts,
		cur:        -1,
	}
}

type containerWithKey struct {
	*Container
	key FilterKey
}

type repeatedRowIterator struct {
	containers []containerWithKey
	row        uint64
	cur        int
}

func (r *repeatedRowIterator) Next() bool {
	r.cur++
	if r.cur >= len(r.containers) {
		r.cur = 0
		r.row++
	}
	return len(r.containers) > 0
}

func (r *repeatedRowIterator) Value() (uint64, *Container) {
	if len(r.containers) == 0 {
		return 0, nil
	}
	return r.row*rowWidth + uint64(r.containers[r.cur].key%rowWidth), r.containers[r.cur].Container
}

func (r *repeatedRowIterator) Close() {}

func getFirstRowAsContainers(citer ContainerIterator) []containerWithKey {
	citerContainers := make([]containerWithKey, 0)
	for citer.Next() {
		key, cont := citer.Value()
		if key >= rowWidth {
			continue
		}
		citerContainers = append(citerContainers, containerWithKey{Container: cont, key: FilterKey(key)})
	}
	return citerContainers
}

// NewClearAndSetRewriter instantiates a ClearAndSetRewriter
func NewClearAndSetRewriter(clear, set ContainerIterator) (*ClearAndSetRewriter, error) {
	curSetKey, curSet := getNextFromIterator(set)
	curClearKey, curClear := getNextFromIterator(clear)

	return &ClearAndSetRewriter{
		curSetKey:   curSetKey,
		curSet:      curSet,
		curClearKey: curClearKey,
		curClear:    curClear,
		clearIter:   clear,
		setIter:     set,
	}, nil
}

const KEY_DONE FilterKey = math.MaxUint64

// ClearAndSetRewriter is a BitmapRewriter which can operate on two
// ContainerIterators, clearing bits from one and setting bits from
// the other. It tries to do this pretty efficiently such that it
// doesn't look at the clear iterator unless there is actually a
// container that might need bits cleared, and it doesn't write a
// container unless bits have actually changed.
type ClearAndSetRewriter struct {
	curSetKey   FilterKey
	curSet      *Container
	curClearKey FilterKey
	curClear    *Container
	clearIter   ContainerIterator
	setIter     ContainerIterator
}

func (csr *ClearAndSetRewriter) nextClear() (FilterKey, *Container) {
	csr.curClearKey, csr.curClear = getNextFromIterator(csr.clearIter)
	return csr.curClearKey, csr.curClear
}
func (csr *ClearAndSetRewriter) nextSet() (FilterKey, *Container) {
	csr.curSetKey, csr.curSet = getNextFromIterator(csr.setIter)
	return csr.curSetKey, csr.curSet
}
func (csr *ClearAndSetRewriter) lowestKey() FilterKey {
	if csr.curSetKey < csr.curClearKey {
		return csr.curSetKey
	}
	return csr.curClearKey
}

func (csr *ClearAndSetRewriter) ConsiderKey(key FilterKey, n int32) FilterResult {
	if key < csr.lowestKey() {
		return key.RejectUntil(csr.lowestKey())
	}
	return key.NeedData()
}

// writeCurrent writes the current clear and set if they match the
// key. It takes some care to try to determine if any bits actually
// changed to avoid unnecessary writes.
func (csr *ClearAndSetRewriter) writeCurrent(key FilterKey, data *Container, writeback ContainerWriteback) error {
	if key == KEY_DONE {
		return nil
	}
	changed := false
	startN := data.N()
	if csr.curClearKey == key && csr.curSetKey == key {
		actualClear := csr.curClear.Difference(csr.curSet) // don't need to clear bits that we're going to set
		data = data.DifferenceInPlace(actualClear)
		clearedN := data.N()
		changed = clearedN != startN
		data = data.UnionInPlace(csr.curSet)
		data.Repair()
		setN := data.N()
		changed = changed || clearedN != setN
	} else if csr.curClearKey == key {
		data = data.DifferenceInPlace(csr.curClear)
		clearedN := data.N()
		changed = clearedN != startN
	} else if csr.curSetKey == key {
		data = data.UnionInPlace(csr.curSet)
		data.Repair()
		setN := data.N()
		changed = startN != setN
	}
	if changed {
		if err := writeback(key, data); err != nil {
			return err
		}
	}
	return nil
}

func (csr *ClearAndSetRewriter) RewriteData(key FilterKey, data *Container, writeback ContainerWriteback) FilterResult {
	if data == nil {
		key = KEY_DONE
	}

	// when we're called with a container, we need to process all the
	// sets up to that container that we haven't done yet. Then we
	// need to do any clears on that container, then any sets on that
	// container. Then we can reject until the lowest next container.
	// When we enter this function, curSet and curClear are the next
	// things we need to process. When we leave this function they
	// must be set to the next things we need to process.

	for ; csr.curSetKey < key; csr.nextSet() {
		if err := writeback(csr.curSetKey, csr.curSet); err != nil {
			return key.Fail(errors.Wrapf(err, "writing set container at %d", csr.curSetKey))
		}
	}

	// fast forward clears to this key
	for ; csr.curClearKey < key && key != KEY_DONE; csr.nextClear() {
	}
	err := csr.writeCurrent(key, data, writeback)
	if err != nil {
		return key.Fail(errors.Wrapf(err, "writing current key %d", key))
	}
	if csr.curSetKey == key {
		csr.nextSet()
	}
	if csr.curClearKey == key {
		csr.nextClear()
	}
	return key.RejectUntil(csr.lowestKey())
}
