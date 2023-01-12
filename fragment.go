// Copyright 2021 Molecula Corp. All rights reserved.
package pilosa

import (
	"archive/tar"
	"bytes"
	"container/heap"
	"context"
	"fmt"
	"io"
	"math"
	"math/bits"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/molecula/featurebase/v3/logger"
	"github.com/molecula/featurebase/v3/pb"
	"github.com/molecula/featurebase/v3/pql"
	"github.com/molecula/featurebase/v3/roaring"
	"github.com/molecula/featurebase/v3/shardwidth"
	"github.com/molecula/featurebase/v3/testhook"
	"github.com/molecula/featurebase/v3/tracing"
	"github.com/molecula/featurebase/v3/vprint"
	"github.com/pkg/errors"
)

const (
	// ShardWidth is the number of column IDs in a shard. It must be a power of 2 greater than or equal to 16.
	// shardWidthExponent = 20 // set in shardwidthNN.go files
	ShardWidth = 1 << shardwidth.Exponent

	// shardVsContainerExponent is the power of 2 of ShardWith minus the power
	// of two of roaring container width (which is 16).
	// 2^shardVsContainerExponent is the number of containers in a shard row.
	//
	// It is represented in this rather awkward way because calculating the row
	// which a given container is in means dividing by the number of rows per
	// container which is performantly expressed as a right shift by this
	// exponent.
	shardVsContainerExponent = shardwidth.Exponent - 16

	// width of roaring containers is 2^16
	containerWidth = 1 << 16

	// cacheExt is the file extension for persisted cache ids.
	cacheExt = ".cache"

	// HashBlockSize is the number of rows in a merkle hash block.
	HashBlockSize = 100

	// Row ids used for boolean fields.
	falseRowID = uint64(0)
	trueRowID  = uint64(1)

	// BSI bits used to check existence & sign.
	bsiExistsBit = 0
	bsiSignBit   = 1
	bsiOffsetBit = 2
)

func (f *fragment) index() string {
	return f.idx.name
}

func (f *fragment) field() string {
	return f.fld.name
}

func (f *fragment) view() string {
	return f._view.name
}
func (f *fragment) path() string {
	return filepath.Join(f._view.path, "fragments", strconv.FormatUint(f.shard, 10))
}

// fragment represents the intersection of a field and shard in an index.
type fragment struct {
	mu sync.RWMutex

	// We save 20GB worth strings on some data sets by not duplicating
	// the path, index, field, view strings on every fragment.
	// Instead assemble strings on demand in field(), view(), path(), index().
	fld   *Field
	_view *view

	shard uint64

	// idx cached to avoid repeatedly looking it up everywhere.
	idx *Index

	// parent holder
	holder *Holder

	// Cache for row counts.
	CacheType string // passed in by field

	// cache keeps a local rowid,count ranking:
	// telling us which is the most populated rows in that field.
	// Is only on "set fields" with rowCache enabled. So
	// BSI, mutex, bool fields do not have this.
	// Good: it Only has a string and a count, so cannot use Tx memory.
	cache cache

	CacheSize uint32

	// Cached checksums for each block.
	checksums map[int][]byte

	// Logger used for out-of-band log entries.
	Logger logger.Logger

	// mutexVector is used for mutex field types. It's checked for an
	// existing value (to clear) prior to setting a new value.
	mutexVector vector
}

// newFragment returns a new instance of fragment.
func newFragment(holder *Holder, idx *Index, fld *Field, vw *view, shard uint64) *fragment {
	checkIdx := holder.Index(idx.name)

	if checkIdx == nil {
		vprint.PanicOn(fmt.Sprintf("got nil idx back for '%v' from holder!", idx.Name()))
	}

	f := &fragment{
		_view: vw,
		fld:   fld,
		shard: shard,
		idx:   idx,

		CacheType: DefaultCacheType,
		CacheSize: DefaultCacheSize,

		holder: holder,
	}
	return f
}

// cachePath returns the path to the fragment's cache data.
func (f *fragment) cachePath() string { return f.path() + cacheExt }

func (f *fragment) bitDepth() (uint64, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	tx, err := f.holder.BeginTx(false, f.idx, f.shard)
	if err != nil {
		return 0, errors.Wrapf(err, "beginning new tx(false, %s, %d)", f.index(), f.shard)
	}
	defer tx.Rollback()

	maxRowID, _, err := f.maxRow(tx, nil)
	if err != nil {
		return 0, errors.Wrapf(err, "getting fragment max row id")
	}

	if maxRowID+1 > bsiOffsetBit {
		return maxRowID + 1 - bsiOffsetBit, nil
	}
	return 0, nil
}

type FragmentInfo struct {
	BitmapInfo roaring.BitmapInfo
}

func (f *fragment) Index() *Index {
	return f.holder.Index(f.index())
}

// Open opens the underlying storage.
func (f *fragment) Open() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := func() error {
		// Fill cache with rows persisted to disk.
		if err := f.openCache(); err != nil {
			return errors.Wrap(err, "opening cache")
		}

		// Clear checksums.
		f.checksums = make(map[int][]byte)
		return nil
	}(); err != nil {
		f.close()
		return err
	}

	_ = testhook.Opened(f.holder.Auditor, f, nil)
	return nil
}

// openCache initializes the cache from row ids persisted to disk.
func (f *fragment) openCache() error {
	// Determine cache type from field name.
	switch f.CacheType {
	case CacheTypeRanked:
		f.cache = NewRankCache(f.CacheSize)
	case CacheTypeLRU:
		f.cache = newLRUCache(f.CacheSize)
	case CacheTypeNone:
		f.cache = globalNopCache
		return nil
	default:
		return ErrInvalidCacheType
	}

	// Read cache data from disk.
	path := f.cachePath()
	buf, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("open cache: %s", err)
	}

	// Unmarshal cache data.
	var pb pb.Cache
	if err := proto.Unmarshal(buf, &pb); err != nil {
		f.holder.Logger.Errorf("error unmarshaling cache data, skipping: path=%s, err=%s", path, err)
		return nil
	}

	tx := f.idx.holder.txf.NewTx(Txo{Write: !writable, Index: f.idx, Fragment: f, Shard: f.shard})
	defer tx.Rollback()

	// Read in all rows by ID.
	// This will cause them to be added to the cache.
	for _, id := range pb.IDs {
		n, err := tx.CountRange(f.index(), f.field(), f.view(), f.shard, id*ShardWidth, (id+1)*ShardWidth)
		if err != nil {
			return errors.Wrap(err, "CountRange")
		}
		f.cache.BulkAdd(id, n)
	}
	f.cache.Invalidate()

	return nil
}

// Close flushes the underlying storage, closes the file and unlocks it.
func (f *fragment) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	defer func() {
		_ = testhook.Closed(f.holder.Auditor, f, nil)
	}()
	return f.close()
}

func (f *fragment) close() error {
	// Flush cache if closing gracefully.
	if err := f.flushCache(); err != nil {
		f.holder.Logger.Errorf("fragment: error flushing cache on close: err=%s, path=%s", err, f.path())
		return errors.Wrap(err, "flushing cache")
	}

	// Remove checksums.
	f.checksums = nil

	return nil
}

// mutexCheck checks for any entries in fragment which violate the mutex
// property of having only one value set for a given column ID.
func (f *fragment) mutexCheck(tx Tx, details bool, limit int) (map[uint64][]uint64, error) {
	dup := roaring.NewBitmapMutexDupFilter(f.shard<<shardwidth.Exponent, details, limit)
	err := tx.ApplyFilter(f.index(), f.field(), f.view(), f.shard, 0, dup)
	if err != nil {
		return nil, err
	}
	return dup.Report(), nil
}

// row returns a row by ID.
func (f *fragment) row(tx Tx, rowID uint64) (*Row, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.unprotectedRow(tx, rowID)
}

// mustRow returns a row by ID. Panic on error. Only used for testing.
func (f *fragment) mustRow(tx Tx, rowID uint64) *Row {
	row, err := f.row(tx, rowID)
	if err != nil {
		vprint.PanicOn(err)
	}
	return row
}

// unprotectedRow returns a row from the row cache if available or from storage
// (updating the cache).
func (f *fragment) unprotectedRow(tx Tx, rowID uint64) (*Row, error) {
	row, err := f.rowFromStorage(tx, rowID)
	if err != nil {
		return nil, err
	}
	return row, nil
}

// rowFromStorage clones a row data out of fragment storage and returns it as a
// Row object.
func (f *fragment) rowFromStorage(tx Tx, rowID uint64) (*Row, error) {
	// Only use a subset of the containers.
	// NOTE: The start & end ranges must be divisible by container width.
	//
	// Note that OffsetRange now returns a new bitmap which uses frozen
	// containers which will use copy-on-write semantics. The actual bitmap
	// and Containers object are new and not shared, but the containers are
	// shared.
	data, err := tx.OffsetRange(f.index(), f.field(), f.view(), f.shard, f.shard*ShardWidth, rowID*ShardWidth, (rowID+1)*ShardWidth)
	if err != nil {
		return nil, err
	}

	row := &Row{
		Segments: []RowSegment{{
			data:     data,
			shard:    f.shard,
			writable: true,
		}},
	}
	row.invalidateCount()

	return row, nil
}

// setBit sets a bit for a given column & row within the fragment.
// This updates both the on-disk storage and the in-cache bitmap.
func (f *fragment) setBit(tx Tx, rowID, columnID uint64) (changed bool, err error) {
	f.mu.Lock() // controls access to the file.
	defer f.mu.Unlock()

	doSetFunc := func() error {
		// handle mutux field type
		if f.mutexVector != nil {
			if err := f.handleMutex(tx, rowID, columnID); err != nil {
				return errors.Wrap(err, "handling mutex")
			}
		}
		changed, err = f.unprotectedSetBit(tx, rowID, columnID)
		return err
	}
	err = doSetFunc()
	return changed, err
}

// handleMutex will clear an existing row and store the new row
// in the vector.
func (f *fragment) handleMutex(tx Tx, rowID, columnID uint64) error {
	if existingRowID, found, err := f.mutexVector.Get(tx, columnID); err != nil {
		return errors.Wrap(err, "getting mutex vector data")
	} else if found && existingRowID != rowID {
		if _, err := f.unprotectedClearBit(tx, existingRowID, columnID); err != nil {
			return errors.Wrap(err, "clearing mutex value")
		}
	}
	return nil
}

// unprotectedSetBit TODO should be replaced by an invocation of importPositions with a single bit to set.
func (f *fragment) unprotectedSetBit(tx Tx, rowID, columnID uint64) (changed bool, err error) {

	// Determine the position of the bit in the storage.
	pos, err := f.pos(rowID, columnID)
	if err != nil {
		return false, errors.Wrap(err, "getting bit pos")
	}

	// Write to storage.
	changeCount := 0
	changeCount, err = tx.Add(f.index(), f.field(), f.view(), f.shard, pos)
	changed = changeCount > 0
	if err != nil {
		return false, errors.Wrap(err, "writing")
	}

	// Don't update the cache if nothing changed.
	if !changed {
		return changed, nil
	}

	// Invalidate block checksum.
	delete(f.checksums, int(rowID/HashBlockSize))

	// If we're using a cache, update it. Otherwise skip the
	// possibly-expensive count operation.
	if f.CacheType != CacheTypeNone {
		n, err := tx.CountRange(f.index(), f.field(), f.view(), f.shard, rowID*ShardWidth, (rowID+1)*ShardWidth)
		if err != nil {
			return false, err
		}
		f.cache.Add(rowID, n)
	}

	CounterSetBit.Inc()

	return changed, nil
}

// clearBit clears a bit for a given column & row within the fragment.
// This updates both the on-disk storage and the in-cache bitmap.
func (f *fragment) clearBit(tx Tx, rowID, columnID uint64) (changed bool, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.unprotectedClearBit(tx, rowID, columnID)
}

// unprotectedClearBit TODO should be replaced by an invocation of
// importPositions with a single bit to clear.
func (f *fragment) unprotectedClearBit(tx Tx, rowID, columnID uint64) (changed bool, err error) {
	changed = false
	// Determine the position of the bit in the storage.
	pos, err := f.pos(rowID, columnID)
	if err != nil {
		return false, errors.Wrap(err, "getting bit pos")
	}

	// Write to storage.
	changeCount := 0
	if changeCount, err = tx.Remove(f.index(), f.field(), f.view(), f.shard, pos); err != nil {
		return false, errors.Wrap(err, "writing")
	}

	// Don't update the cache if nothing changed.
	if changeCount <= 0 {
		return false, nil
	} else {
		changed = true
	}

	// Invalidate block checksum.
	delete(f.checksums, int(rowID/HashBlockSize))

	// If we're using a cache, update it. Otherwise skip the
	// possibly-expensive count operation.
	if f.CacheType != CacheTypeNone {
		n, err := tx.CountRange(f.index(), f.field(), f.view(), f.shard, rowID*ShardWidth, (rowID+1)*ShardWidth)
		if err != nil {
			return changed, err
		}
		f.cache.Add(rowID, n)
	}

	CounterClearBit.Inc()

	return changed, nil
}

// setRow replaces an existing row (specified by rowID) with the given
// Row. This updates both the on-disk storage and the in-cache bitmap.
func (f *fragment) setRow(tx Tx, row *Row, rowID uint64) (changed bool, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.unprotectedSetRow(tx, row, rowID)
}

func (f *fragment) unprotectedSetRow(tx Tx, row *Row, rowID uint64) (changed bool, err error) {
	// TODO: In order to return `changed`, we need to first compare
	// the existing row with the given row. Determine if the overhead
	// of this is worth having `changed`.
	// For now we will assume changed is always true.
	changed = true

	// First container of the row in storage.
	headContainerKey := rowID << shardVsContainerExponent

	// Remove every existing container in the row.
	for i := uint64(0); i < (1 << shardVsContainerExponent); i++ {
		if err := tx.RemoveContainer(f.index(), f.field(), f.view(), f.shard, headContainerKey+i); err != nil {
			return changed, err
		}
	}

	// From the given row, get the rowSegment for this shard.
	seg := row.segment(f.shard)
	if seg != nil {
		// Put each container from rowSegment to fragment storage.
		citer, _ := seg.data.Containers.Iterator(f.shard << shardVsContainerExponent)
		for citer.Next() {
			k, c := citer.Value()
			if err := tx.PutContainer(f.index(), f.field(), f.view(), f.shard, headContainerKey+(k%(1<<shardVsContainerExponent)), c); err != nil {
				return changed, err
			}
		}

		// Update the row in cache.
		if f.CacheType != CacheTypeNone {
			n, err := tx.CountRange(f.index(), f.field(), f.view(), f.shard, rowID*ShardWidth, (rowID+1)*ShardWidth)
			if err != nil {
				return changed, err
			}
			f.cache.BulkAdd(rowID, n)
		}
	} else {
		if f.CacheType != CacheTypeNone {
			f.cache.BulkAdd(rowID, 0)
		}
	}

	CounterSetRow.Inc()

	return changed, nil
}

// clearRow clears a row for a given rowID within the fragment.
// This updates both the on-disk storage and the in-cache bitmap.
func (f *fragment) clearRow(tx Tx, rowID uint64) (changed bool, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.unprotectedClearRow(tx, rowID)
}

func (f *fragment) unprotectedClearRow(tx Tx, rowID uint64) (changed bool, err error) {
	changed = false

	// First container of the row in storage.
	headContainerKey := rowID << shardVsContainerExponent

	// Remove every container in the row.
	for i := uint64(0); i < (1 << shardVsContainerExponent); i++ {
		k := headContainerKey + i
		// Technically we could bypass the Get() call and only
		// call Remove(), but the Get() gives us the ability
		// to return true if any existing data was removed.
		if cont, err := tx.Container(f.index(), f.field(), f.view(), f.shard, k); err != nil {
			return changed, err
		} else if cont != nil {
			if err := tx.RemoveContainer(f.index(), f.field(), f.view(), f.shard, k); err != nil {
				return changed, err
			}
			changed = true
		}
	}

	// Clear the row in cache.
	f.cache.Add(rowID, 0)

	return changed, nil
}

// clearBlock clears all rows for a given block.
// This updates both the on-disk storage and the in-cache bitmap.
func (f *fragment) clearBlock(tx Tx, block int) (changed bool, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	firstRow := uint64(block * HashBlockSize)
	err = func() error {
		var rowChanged bool
		for rowID := uint64(firstRow); rowID < firstRow+HashBlockSize; rowID++ {
			if chang, err := f.unprotectedClearRow(tx, rowID); err != nil {
				return errors.Wrapf(err, "clearing row: %d", rowID)
			} else if chang {
				rowChanged = true
			}
		}
		changed = rowChanged
		return nil
	}()
	return changed, err
}

func (f *fragment) bit(tx Tx, rowID, columnID uint64) (bool, error) {
	pos, err := f.pos(rowID, columnID)
	if err != nil {
		return false, err
	}
	return tx.Contains(f.index(), f.field(), f.view(), f.shard, pos)
}

// value uses a column of bits to read a multi-bit value.
func (f *fragment) value(tx Tx, columnID uint64, bitDepth uint64) (value int64, exists bool, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// If existence bit is unset then ignore remaining bits.
	if v, err := f.bit(tx, bsiExistsBit, columnID); err != nil {
		return 0, false, errors.Wrap(err, "getting existence bit")
	} else if !v {
		return 0, false, nil
	}

	// Compute other bits into a value.
	for i := uint64(0); i < bitDepth; i++ {
		if v, err := f.bit(tx, uint64(bsiOffsetBit+i), columnID); err != nil {
			return 0, false, errors.Wrapf(err, "getting value bit %d", i)
		} else if v {
			value |= (1 << i)
		}
	}

	// Negate if sign bit set.
	if v, err := f.bit(tx, bsiSignBit, columnID); err != nil {
		return 0, false, errors.Wrap(err, "getting sign bit")
	} else if v {
		value = -value
	}

	return value, true, nil
}

// clearValue uses a column of bits to clear a multi-bit value.
func (f *fragment) clearValue(tx Tx, columnID uint64, bitDepth uint64, value int64) (changed bool, err error) {
	return f.setValueBase(tx, columnID, bitDepth, value, true)
}

// setValue uses a column of bits to set a multi-bit value.
func (f *fragment) setValue(tx Tx, columnID uint64, bitDepth uint64, value int64) (changed bool, err error) {
	return f.setValueBase(tx, columnID, bitDepth, value, false)
}

func (f *fragment) positionsForValue(columnID uint64, bitDepth uint64, value int64, clear bool, toSet, toClear []uint64) ([]uint64, []uint64, error) {
	// Convert value to an unsigned representation.
	uvalue := uint64(value)
	if value < 0 {
		uvalue = uint64(-value)
	}

	// Mark value as set.
	if bit, err := f.pos(bsiExistsBit, columnID); err != nil {
		return toSet, toClear, errors.Wrap(err, "getting not-null pos")
	} else if clear {
		toClear = append(toClear, bit)
	} else {
		toSet = append(toSet, bit)
	}

	// Mark sign.
	if bit, err := f.pos(bsiSignBit, columnID); err != nil {
		return toSet, toClear, errors.Wrap(err, "getting sign pos")
	} else if value >= 0 || clear {
		toClear = append(toClear, bit)
	} else {
		toSet = append(toSet, bit)
	}

	for i := uint64(0); i < bitDepth; i++ {
		bit, err := f.pos(uint64(bsiOffsetBit+i), columnID)
		if err != nil {
			return toSet, toClear, errors.Wrap(err, "getting pos")
		}
		if uvalue&(1<<i) != 0 {
			toSet = append(toSet, bit)
		} else {
			toClear = append(toClear, bit)
		}
	}

	return toSet, toClear, nil
}

// TODO get rid of this and use positionsForValue to generate a single write op, and set that with importPositions.
func (f *fragment) setValueBase(tx Tx, columnID uint64, bitDepth uint64, value int64, clear bool) (changed bool, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	err = func() error {
		// Convert value to an unsigned representation.
		uvalue := uint64(value)
		if value < 0 {
			uvalue = uint64(-value)
		}

		for i := uint64(0); i < bitDepth; i++ {
			if uvalue&(1<<i) != 0 {
				if c, err := f.unprotectedSetBit(tx, uint64(bsiOffsetBit+i), columnID); err != nil {
					return err
				} else if c {
					changed = true
				}
			} else {
				if c, err := f.unprotectedClearBit(tx, uint64(bsiOffsetBit+i), columnID); err != nil {
					return err
				} else if c {
					changed = true
				}
			}
		}

		// Mark value as set (or cleared).
		if clear {
			if c, err := f.unprotectedClearBit(tx, uint64(bsiExistsBit), columnID); err != nil {
				return errors.Wrap(err, "clearing not-null")
			} else if c {
				changed = true
			}
		} else {
			if c, err := f.unprotectedSetBit(tx, uint64(bsiExistsBit), columnID); err != nil {
				return errors.Wrap(err, "marking not-null")
			} else if c {
				changed = true
			}
		}

		// Mark sign bit (or clear).
		if value >= 0 || clear {
			if c, err := f.unprotectedClearBit(tx, uint64(bsiSignBit), columnID); err != nil {
				return errors.Wrap(err, "clearing sign")
			} else if c {
				changed = true
			}
		} else {
			if c, err := f.unprotectedSetBit(tx, uint64(bsiSignBit), columnID); err != nil {
				return errors.Wrap(err, "marking sign")
			} else if c {
				changed = true
			}
		}

		return nil
	}()
	return changed, err
}

// sum returns the sum of a given bsiGroup as well as the number of columns involved.
// A bitmap can be passed in to optionally filter the computed columns.
func (f *fragment) sum(tx Tx, filter *Row, bitDepth uint64) (sum int64, count uint64, err error) {
	// If there's a provided filter, but it has no contents for this particular
	// shard, we're done and can return early. If there's no provided filter,
	// though, we want to run with no-filter, as opposed to an empty filter.
	var filterData *roaring.Bitmap
	if filter != nil {
		for _, seg := range filter.Segments {
			if seg.shard == f.shard {
				filterData = seg.data
				break
			}
		}
		// if filter is empty, we're done
		if filterData == nil {
			return 0, 0, nil
		}
	}
	bsiFilt := roaring.NewBitmapBSICountFilter(filterData)
	err = tx.ApplyFilter(f.index(), f.field(), f.view(), f.shard, 0, bsiFilt)
	if err != nil && err != io.EOF {
		return sum, count, errors.Wrap(err, "finding existing positions")
	}

	c32, sum := bsiFilt.Total()

	return sum, uint64(c32), nil
}

// min returns the min of a given bsiGroup as well as the number of columns involved.
// A bitmap can be passed in to optionally filter the computed columns.
func (f *fragment) min(tx Tx, filter *Row, bitDepth uint64) (min int64, count uint64, err error) {
	consider, err := f.row(tx, bsiExistsBit)
	if err != nil {
		return min, count, err
	} else if filter != nil {
		consider = consider.Intersect(filter)
	}

	// If there are no columns to consider, return early.
	if consider.Count() == 0 {
		return 0, 0, nil
	}

	// If we have negative values, we should find the highest unsigned value
	// from that set, then negate it, and return it. For example, if values
	// (-1, -2) exist, they are stored unsigned (1,2) with a negative sign bit
	// set. We take the highest of that set (2) and negate it and return it.
	if row, err := f.row(tx, bsiSignBit); err != nil {
		return min, count, err
	} else if row = row.Intersect(consider); row.Any() {
		min, count, err := f.maxUnsigned(tx, row, bitDepth)
		return -min, count, err
	}

	// Otherwise find lowest positive number.
	return f.minUnsigned(tx, consider, bitDepth)
}

// minUnsigned the lowest value without considering the sign bit. Filter is required.
func (f *fragment) minUnsigned(tx Tx, filter *Row, bitDepth uint64) (min int64, count uint64, err error) {
	count = filter.Count()
	for i := int(bitDepth - 1); i >= 0; i-- {
		row, err := f.row(tx, uint64(bsiOffsetBit+i))
		if err != nil {
			return min, count, err
		}
		row = filter.Difference(row)
		count = row.Count()
		if count > 0 {
			filter = row
		} else {
			min += (1 << uint(i))
			if i == 0 {
				count = filter.Count()
			}
		}
	}
	return min, count, nil
}

// max returns the max of a given bsiGroup as well as the number of columns involved.
// A bitmap can be passed in to optionally filter the computed columns.
func (f *fragment) max(tx Tx, filter *Row, bitDepth uint64) (max int64, count uint64, err error) {
	consider, err := f.row(tx, bsiExistsBit)
	if err != nil {
		return max, count, err
	} else if filter != nil {
		consider = consider.Intersect(filter)
	}

	// If there are no columns to consider, return early.
	if !consider.Any() {
		return 0, 0, nil
	}

	// Find lowest negative number w/o sign and negate, if no positives are available.
	row, err := f.row(tx, bsiSignBit)
	if err != nil {
		return max, count, err
	}
	pos := consider.Difference(row)
	if !pos.Any() {
		max, count, err = f.minUnsigned(tx, consider, bitDepth)
		return -max, count, err
	}

	// Otherwise find highest positive number.
	return f.maxUnsigned(tx, pos, bitDepth)
}

// maxUnsigned the highest value without considering the sign bit. Filter is required.
func (f *fragment) maxUnsigned(tx Tx, filter *Row, bitDepth uint64) (max int64, count uint64, err error) {
	count = filter.Count()
	for i := int(bitDepth - 1); i >= 0; i-- {
		row, err := f.row(tx, uint64(bsiOffsetBit+i))
		if err != nil {
			return max, count, err
		}
		row = row.Intersect(filter)

		count = row.Count()
		if count > 0 {
			max += (1 << uint(i))
			filter = row
		} else if i == 0 {
			count = filter.Count()
		}
	}
	return max, count, nil
}

// minRow returns minRowID of the rows in the filter and its count.
// if filter is nil, it returns fragment.minRowID, 1
// if fragment has no rows, it returns 0, 0
func (f *fragment) minRow(tx Tx, filter *Row) (uint64, uint64, error) {
	minRowID, hasRowID, err := f.minRowID(tx)
	if err != nil {
		return 0, 0, err
	}
	if hasRowID {
		if filter == nil {
			return minRowID, 1, nil
		}

		// Read last bit to determine max row.
		maxRowID, err := f.maxRowID(tx)
		if err != nil {
			return 0, 0, err
		}

		// iterate from min row ID and return the first that intersects with filter.
		for i := minRowID; i <= maxRowID; i++ {
			row, err := f.row(tx, i)
			if err != nil {
				return 0, 0, err
			}
			row = row.Intersect(filter)

			count := row.Count()
			if count > 0 {
				return i, count, nil
			}
		}
	}
	return 0, 0, nil
}

// maxRow returns maxRowID of the rows in the filter and its count.
// if filter is nil, it returns fragment.maxRowID, 1
// if fragment has no rows, it returns 0, 0
func (f *fragment) maxRow(tx Tx, filter *Row) (uint64, uint64, error) {
	minRowID, hasRowID, err := f.minRowID(tx)
	if err != nil {
		return 0, 0, err
	}
	if hasRowID {
		maxRowID, err := f.maxRowID(tx)
		if err != nil {
			return 0, 0, err
		}

		if filter == nil {
			return maxRowID, 1, nil
		}
		// iterate back from max row ID and return the first that intersects with filter.
		// TODO: implement reverse container iteration to improve performance here for sparse data. --Jaffee
		for i := maxRowID; i >= minRowID; i-- {
			row, err := f.row(tx, i)
			if err != nil {
				return 0, 0, err
			}
			row = row.Intersect(filter)

			count := row.Count()
			if count > 0 {
				return i, count, nil
			}
		}
	}
	return 0, 0, nil
}

// maxRowID determines the field's maxRowID value based
// on the contents of its storage, and sets the struct argument.
func (f *fragment) maxRowID(tx Tx) (_ uint64, err error) {
	max, err := tx.Max(f.index(), f.field(), f.view(), f.shard)
	if err != nil {
		return 0, err
	}
	return max / ShardWidth, nil
}

// rangeOp returns bitmaps with a bsiGroup value encoding matching the predicate.
func (f *fragment) rangeOp(tx Tx, op pql.Token, bitDepth uint64, predicate int64) (*Row, error) {
	switch op {
	case pql.EQ:
		return f.rangeEQ(tx, bitDepth, predicate)
	case pql.NEQ:
		return f.rangeNEQ(tx, bitDepth, predicate)
	case pql.LT, pql.LTE:
		return f.rangeLT(tx, bitDepth, predicate, op == pql.LTE)
	case pql.GT, pql.GTE:
		return f.rangeGT(tx, bitDepth, predicate, op == pql.GTE)
	default:
		return nil, ErrInvalidRangeOperation
	}
}

func absInt64(v int64) uint64 {
	switch {
	case v > 0:
		return uint64(v)
	case v == -9223372036854775808:
		return 9223372036854775808
	default:
		return uint64(-v)
	}
}

func (f *fragment) rangeEQ(tx Tx, bitDepth uint64, predicate int64) (*Row, error) {
	// Start with set of columns with values set.
	b, err := f.row(tx, bsiExistsBit)
	if err != nil {
		return nil, err
	}

	upredicate := absInt64(predicate)
	if uint64(bits.Len64(upredicate)) > bitDepth {
		// Predicate is out of range.
		return NewRow(), nil
	}

	// Filter to only positive/negative numbers.
	r, err := f.row(tx, bsiSignBit)
	if err != nil {
		return nil, err
	}
	if predicate < 0 {
		b = b.Intersect(r) // only negatives
	} else {
		b = b.Difference(r) // only positives
	}

	// Filter any bits that don't match the current bit value.
	for i := int(bitDepth - 1); i >= 0; i-- {
		row, err := f.row(tx, uint64(bsiOffsetBit+i))
		if err != nil {
			return nil, err
		}
		bit := (upredicate >> uint(i)) & 1

		if bit == 1 {
			b = b.Intersect(row)
		} else {
			b = b.Difference(row)
		}
	}

	return b, nil
}

func (f *fragment) rangeNEQ(tx Tx, bitDepth uint64, predicate int64) (*Row, error) {
	// Start with set of columns with values set.
	b, err := f.row(tx, bsiExistsBit)
	if err != nil {
		return nil, err
	}

	// Get the equal bitmap.
	eq, err := f.rangeEQ(tx, bitDepth, predicate)
	if err != nil {
		return nil, err
	}

	// Not-null minus the equal bitmap.
	b = b.Difference(eq)

	return b, nil
}

func (f *fragment) rangeLT(tx Tx, bitDepth uint64, predicate int64, allowEquality bool) (*Row, error) {
	if predicate == 1 && !allowEquality {
		predicate, allowEquality = 0, true
	}

	// Start with set of columns with values set.
	b, err := f.row(tx, bsiExistsBit)
	if err != nil {
		return nil, err
	}

	// Get the sign bit row.
	sign, err := f.row(tx, bsiSignBit)
	if err != nil {
		return nil, err
	}

	// Create predicate without sign bit.
	upredicate := absInt64(predicate)

	switch {
	case predicate == 0 && !allowEquality:
		// Match all negative integers.
		return b.Intersect(sign), nil
	case predicate == 0 && allowEquality:
		// Match all integers that are either negative or 0.
		zeroes, err := f.rangeEQ(tx, bitDepth, 0)
		if err != nil {
			return nil, err
		}
		return b.Intersect(sign).Union(zeroes), nil
	case predicate < 0:
		// Match all every negative number beyond the predicate.
		return f.rangeGTUnsigned(tx, b.Intersect(sign), bitDepth, upredicate, allowEquality)
	default:
		// Match positive numbers less than the predicate, and all negatives.
		pos, err := f.rangeLTUnsigned(tx, b.Difference(sign), bitDepth, upredicate, allowEquality)
		if err != nil {
			return nil, err
		}
		neg := b.Intersect(sign)
		return pos.Union(neg), nil
	}
}

// rangeLTUnsigned returns all bits LT/LTE the predicate without considering the sign bit.
func (f *fragment) rangeLTUnsigned(tx Tx, filter *Row, bitDepth uint64, predicate uint64, allowEquality bool) (*Row, error) {
	switch {
	case uint64(bits.Len64(predicate)) > bitDepth:
		fallthrough
	case predicate == (1<<bitDepth)-1 && allowEquality:
		// This query matches all possible values.
		return filter, nil
	case predicate == (1<<bitDepth)-1 && !allowEquality:
		// This query matches everything that is not (1<<bitDepth)-1.
		matches := NewRow()
		for i := uint64(0); i < bitDepth; i++ {
			row, err := f.row(tx, uint64(bsiOffsetBit+i))
			if err != nil {
				return nil, err
			}
			matches = matches.Union(filter.Difference(row))
		}
		return matches, nil
	case allowEquality:
		predicate++
	}

	// Compare intermediate bits.
	matched := NewRow()
	remaining := filter
	for i := int(bitDepth - 1); i >= 0 && predicate > 0 && remaining.Any(); i-- {
		row, err := f.row(tx, uint64(bsiOffsetBit+i))
		if err != nil {
			return nil, err
		}
		zeroes := remaining.Difference(row)
		switch (predicate >> uint(i)) & 1 {
		case 1:
			// Match everything with a zero bit here.
			matched = matched.Union(zeroes)
			predicate &^= 1 << uint(i)
		case 0:
			// Discard everything with a one bit here.
			remaining = zeroes
		}
	}

	return matched, nil
}

func (f *fragment) rangeGT(tx Tx, bitDepth uint64, predicate int64, allowEquality bool) (*Row, error) {
	if predicate == -1 && !allowEquality {
		predicate, allowEquality = 0, true
	}

	b, err := f.row(tx, bsiExistsBit)
	if err != nil {
		return nil, err
	}
	// Create predicate without sign bit.
	upredicate := absInt64(predicate)

	sign, err := f.row(tx, bsiSignBit)
	if err != nil {
		return nil, err
	}
	switch {
	case predicate == 0 && !allowEquality:
		// Match all positive numbers except zero.
		nonzero, err := f.rangeNEQ(tx, bitDepth, 0)
		if err != nil {
			return nil, err
		}
		b = nonzero
		fallthrough
	case predicate == 0 && allowEquality:
		// Match all positive numbers.
		return b.Difference(sign), nil
	case predicate >= 0:
		// Match all positive numbers greater than the predicate.
		return f.rangeGTUnsigned(tx, b.Difference(sign), bitDepth, upredicate, allowEquality)
	default:
		// Match all positives and greater negatives.
		neg, err := f.rangeLTUnsigned(tx, b.Intersect(sign), bitDepth, upredicate, allowEquality)
		if err != nil {
			return nil, err
		}
		pos := b.Difference(sign)
		return pos.Union(neg), nil
	}
}

func (f *fragment) rangeGTUnsigned(tx Tx, filter *Row, bitDepth uint64, predicate uint64, allowEquality bool) (*Row, error) {
prep:
	switch {
	case predicate == 0 && allowEquality:
		// This query matches all possible values.
		return filter, nil
	case predicate == 0 && !allowEquality:
		// This query matches everything that is not 0.
		matches := NewRow()
		for i := uint64(0); i < bitDepth; i++ {
			row, err := f.row(tx, uint64(bsiOffsetBit+i))
			if err != nil {
				return nil, err
			}
			matches = matches.Union(filter.Intersect(row))
		}
		return matches, nil
	case !allowEquality && uint64(bits.Len64(predicate)) > bitDepth:
		// The predicate is bigger than the BSI width, so nothing can be bigger.
		return NewRow(), nil
	case allowEquality:
		predicate--
		allowEquality = false
		goto prep
	}

	// Compare intermediate bits.
	matched := NewRow()
	remaining := filter
	predicate |= (^uint64(0)) << bitDepth
	for i := int(bitDepth - 1); i >= 0 && predicate < ^uint64(0) && remaining.Any(); i-- {
		row, err := f.row(tx, uint64(bsiOffsetBit+i))
		if err != nil {
			return nil, err
		}
		ones := remaining.Intersect(row)
		switch (predicate >> uint(i)) & 1 {
		case 1:
			// Discard everything with a zero bit here.
			remaining = ones
		case 0:
			// Match everything with a one bit here.
			matched = matched.Union(ones)
			predicate |= 1 << uint(i)
		}
	}

	return matched, nil
}

// notNull returns the exists row.
func (f *fragment) notNull(tx Tx) (*Row, error) {
	return f.row(tx, bsiExistsBit)
}

// rangeBetween returns bitmaps with a bsiGroup value encoding matching any value between predicateMin and predicateMax.
func (f *fragment) rangeBetween(tx Tx, bitDepth uint64, predicateMin, predicateMax int64) (*Row, error) {
	b, err := f.row(tx, bsiExistsBit)
	if err != nil {
		return nil, err
	}

	// Convert predicates to unsigned values.
	upredicateMin, upredicateMax := absInt64(predicateMin), absInt64(predicateMax)

	switch {
	case predicateMin == predicateMax:
		return f.rangeEQ(tx, bitDepth, predicateMin)
	case predicateMin >= 0:
		// Handle positive-only values.
		r, err := f.row(tx, bsiSignBit)
		if err != nil {
			return nil, err
		}
		return f.rangeBetweenUnsigned(tx, b.Difference(r), bitDepth, upredicateMin, upredicateMax)
	case predicateMax < 0:
		// Handle negative-only values. Swap unsigned min/max predicates.
		r, err := f.row(tx, bsiSignBit)
		if err != nil {
			return nil, err
		}
		return f.rangeBetweenUnsigned(tx, b.Intersect(r), bitDepth, upredicateMax, upredicateMin)
	default:
		// If predicate crosses positive/negative boundary then handle separately and union.
		r0, err := f.row(tx, bsiSignBit)
		if err != nil {
			return nil, err
		}
		pos, err := f.rangeLTUnsigned(tx, b.Difference(r0), bitDepth, upredicateMax, true)
		if err != nil {
			return nil, err
		}
		r1, err := f.row(tx, bsiSignBit)
		if err != nil {
			return nil, err
		}
		neg, err := f.rangeLTUnsigned(tx, b.Intersect(r1), bitDepth, upredicateMin, true)
		if err != nil {
			return nil, err
		}
		return pos.Union(neg), nil
	}
}

// rangeBetweenUnsigned returns BSI columns for a range of values. Disregards the sign bit.
func (f *fragment) rangeBetweenUnsigned(tx Tx, filter *Row, bitDepth uint64, predicateMin, predicateMax uint64) (*Row, error) {
	switch {
	case predicateMax > (1<<bitDepth)-1:
		// The upper bound cannot be violated.
		return f.rangeGTUnsigned(tx, filter, bitDepth, predicateMin, true)
	case predicateMin == 0:
		// The lower bound cannot be violated.
		return f.rangeLTUnsigned(tx, filter, bitDepth, predicateMax, true)
	}

	// Compare any upper bits which are equal.
	diffLen := bits.Len64(predicateMax ^ predicateMin)
	remaining := filter
	for i := int(bitDepth - 1); i >= diffLen; i-- {
		row, err := f.row(tx, uint64(bsiOffsetBit+i))
		if err != nil {
			return nil, err
		}
		switch (predicateMin >> uint(i)) & 1 {
		case 1:
			remaining = remaining.Intersect(row)
		case 0:
			remaining = remaining.Difference(row)
		}
	}

	// Clear the bits we just compared.
	equalMask := (^uint64(0)) << diffLen
	predicateMin &^= equalMask
	predicateMax &^= equalMask

	var err error
	remaining, err = f.rangeGTUnsigned(tx, remaining, uint64(diffLen), predicateMin, true)
	if err != nil {
		return nil, err
	}
	remaining, err = f.rangeLTUnsigned(tx, remaining, uint64(diffLen), predicateMax, true)
	if err != nil {
		return nil, err
	}
	return remaining, nil
}

// pos translates the row ID and column ID into a position in the storage bitmap.
func (f *fragment) pos(rowID, columnID uint64) (uint64, error) {
	// Return an error if the column ID is out of the range of the fragment's shard.
	minColumnID := f.shard * ShardWidth
	if columnID < minColumnID || columnID >= minColumnID+ShardWidth {
		return 0, errors.Errorf("column:%d out of bounds for shard %d", columnID, f.shard)
	}
	return pos(rowID, columnID), nil
}

// top returns the top rows from the fragment.
// If opt.Src is specified then only rows which intersect src are returned.
func (f *fragment) top(tx Tx, opt topOptions) ([]Pair, error) {
	// Retrieve pairs. If no row ids specified then return from cache.
	pairs, err := f.topBitmapPairs(tx, opt.RowIDs)
	if err != nil {
		return nil, err
	}

	// If row ids are provided, we don't want to truncate the result set
	if len(opt.RowIDs) > 0 {
		opt.N = 0
	}

	// Use `tanimotoThreshold > 0` to indicate whether or not we are considering Tanimoto.
	var tanimotoThreshold uint64
	var minTanimoto, maxTanimoto float64
	var srcCount uint64
	if opt.TanimotoThreshold > 0 && opt.Src != nil {
		tanimotoThreshold = opt.TanimotoThreshold
		srcCount = opt.Src.Count()
		minTanimoto = float64(srcCount*tanimotoThreshold) / 100
		maxTanimoto = float64(srcCount*100) / float64(tanimotoThreshold)
	}

	// Iterate over rankings and add to results until we have enough.
	results := &pairHeap{}
	for _, pair := range pairs {
		rowID, cnt := pair.ID, pair.Count

		// Ignore empty rows.
		if cnt == 0 {
			continue
		}

		// Check against either Tanimoto threshold or minimum threshold.
		if tanimotoThreshold > 0 {
			// Ignore counts outside of the Tanimoto min/max values.
			if float64(cnt) <= minTanimoto || float64(cnt) >= maxTanimoto {
				continue
			}
		} else {
			// Ignore counts less than MinThreshold.
			if cnt < opt.MinThreshold {
				continue
			}
		}

		// The initial n pairs should simply be added to the results.
		if opt.N == 0 || results.Len() < opt.N {
			// Calculate count and append.
			count := cnt
			if opt.Src != nil {
				r, err := f.row(tx, rowID)
				if err != nil {
					return nil, err
				}
				count = opt.Src.intersectionCount(r)
			}
			if count == 0 {
				continue
			}

			// Check against either Tanimoto threshold or minimum threshold.
			if tanimotoThreshold > 0 {
				tanimoto := math.Ceil(float64(count*100) / float64(cnt+srcCount-count))
				if tanimoto <= float64(tanimotoThreshold) {
					continue
				}
			} else {
				if count < opt.MinThreshold {
					continue
				}
			}

			heap.Push(results, Pair{ID: rowID, Count: count})

			// If we reach the requested number of pairs and we are not computing
			// intersections then simply exit. If we are intersecting then sort
			// and then only keep pairs that are higher than the lowest count.
			if opt.N > 0 && results.Len() == opt.N {
				if opt.Src == nil {
					break
				}
			}

			continue
		}

		// Retrieve the lowest count we have.
		// If it's too low then don't try finding anymore pairs.
		threshold := results.Pairs[0].Count

		// If the row doesn't have enough columns set before the intersection
		// then we can assume that any remaining rows also have a count too low.
		if threshold < opt.MinThreshold || cnt < threshold {
			break
		}

		// Calculate the intersecting column count and skip if it's below our
		// last row in our current result set.
		r, err := f.row(tx, rowID)
		if err != nil {
			return nil, err
		}
		count := opt.Src.intersectionCount(r)
		if count < threshold {
			continue
		}

		heap.Push(results, Pair{ID: rowID, Count: count})
	}

	//Pop first opt.N elements out of heap
	r := make(Pairs, results.Len())
	x := results.Len()
	i := 1
	for results.Len() > 0 {
		r[x-i] = heap.Pop(results).(Pair)
		i++
	}
	return r, nil
}

func (f *fragment) topBitmapPairs(tx Tx, rowIDs []uint64) ([]bitmapPair, error) {
	// Don't retrieve from storage if CacheTypeNone.
	if f.CacheType == CacheTypeNone {
		return f.cache.Top(), nil
	}
	// If no specific rows are requested, retrieve top rows.
	if len(rowIDs) == 0 {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.cache.Invalidate()
		return f.cache.Top(), nil
	}

	// Otherwise retrieve specific rows.
	pairs := make([]bitmapPair, 0, len(rowIDs))
	for _, rowID := range rowIDs {
		// Look up cache first, if available.
		if n := f.cache.Get(rowID); n > 0 {
			pairs = append(pairs, bitmapPair{
				ID:    rowID,
				Count: n,
			})
			continue
		}

		row, err := f.row(tx, rowID)
		if err != nil {
			return nil, err
		}
		if row.Count() > 0 {
			// Otherwise load from storage.
			pairs = append(pairs, bitmapPair{
				ID:    rowID,
				Count: row.Count(),
			})
		}
	}
	sortPairs := bitmapPairs(pairs)
	sort.Sort(&sortPairs)
	return pairs, nil
}

// topOptions represents options passed into the Top() function.
type topOptions struct {
	// Number of rows to return.
	N int

	// Bitmap to intersect with.
	Src *Row

	// Specific rows to filter against.
	RowIDs       []uint64
	MinThreshold uint64

	TanimotoThreshold uint64
}

// bulkImport bulk imports a set of bits.
// The cache is updated to reflect the new data.
func (f *fragment) bulkImport(tx Tx, rowIDs, columnIDs []uint64, options *ImportOptions) error {
	// Verify that there are an equal number of row ids and column ids.
	if len(rowIDs) != len(columnIDs) {
		return fmt.Errorf("mismatch of row/column len: %d != %d", len(rowIDs), len(columnIDs))
	}

	if f.mutexVector != nil && !options.Clear {
		return f.bulkImportMutex(tx, rowIDs, columnIDs, options)
	}
	return f.bulkImportStandard(tx, rowIDs, columnIDs, options)
}

// rowColumnSet is a sortable set of row and column IDs which
// correspond, allowing us to ensure that we produce values in
// a predictable order
type rowColumnSet struct {
	r []uint64
	c []uint64
}

func (r rowColumnSet) Len() int {
	return len(r.r)
}

func (r rowColumnSet) Swap(i, j int) {
	r.r[i], r.r[j] = r.r[j], r.r[i]
	r.c[i], r.c[j] = r.c[j], r.c[i]
}

// Sort by row ID first, column second, to sort by fragment position
func (r rowColumnSet) Less(i, j int) bool {
	if r.r[i] < r.r[j] {
		return true
	}
	if r.r[i] > r.r[j] {
		return false
	}
	return r.c[i] < r.c[j]
}

// bulkImportStandard performs a bulk import on a standard fragment. May mutate
// its rowIDs and columnIDs arguments.
func (f *fragment) bulkImportStandard(tx Tx, rowIDs, columnIDs []uint64, options *ImportOptions) (err error) {
	// rowSet maintains the set of rowIDs present in this import. It allows the
	// cache to be updated once per row, instead of once per bit. TODO: consider
	// sorting by rowID/columnID first and avoiding the map allocation here. (we
	// could reuse rowIDs to store the list of unique row IDs)
	rowSet := make(map[uint64]struct{})
	lastRowID := uint64(1 << 63)

	// It's possible for the ingest API to have already sorted things in
	// the row-first order we want for this import.
	if !options.fullySorted {
		sort.Sort(rowColumnSet{r: rowIDs, c: columnIDs})
	}
	// replace columnIDs with calculated positions to avoid allocation.
	prevRow, prevCol := ^uint64(0), ^uint64(0)
	next := 0
	for i := 0; i < len(columnIDs); i++ {
		rowID, columnID := rowIDs[i], columnIDs[i]
		if rowID == prevRow && columnID == prevCol {
			continue
		}
		prevRow, prevCol = rowID, columnID
		pos, err := f.pos(rowID, columnID)
		if err != nil {
			return err
		}
		columnIDs[next] = pos
		next++

		// Add row to rowSet.
		if rowID != lastRowID {
			lastRowID = rowID
			rowSet[rowID] = struct{}{}
		}
	}
	positions := columnIDs[:next]
	f.mu.Lock()
	defer f.mu.Unlock()
	if options.Clear {
		err = f.importPositions(tx, nil, positions, rowSet)
	} else {
		err = f.importPositions(tx, positions, nil, rowSet)
	}
	return errors.Wrap(err, "bulkImportStandard")
}

// parallelSlices provides a sort.Interface for corresponding slices of
// column and row values, and also allows pruning of duplicate values,
// meaning values for the same column, as you might want for a mutex.
//
// The slices should be parallel and of the same length.
type parallelSlices struct {
	cols, rows []uint64
}

// prune eliminates values which have the same column key and are
// adjacent in the slice. It doesn't handle non-adjacent keys, but
// does report whether it saw any. See fullPrune for what you probably
// want to be using.
func (p *parallelSlices) prune() (unsorted bool) {
	l := len(p.cols)
	if l == 0 {
		return
	}
	n := 0
	prev := p.cols[0]
	// At any point, n is the index of the last value we wrote
	// (we pretend we copied 0 to 0 before starting). If the next
	// value would have the same column ID, it should replace
	// the one we just wrote, otherwise we move n to point to a
	// new slot before writing. If there are no duplicates, n is
	// always equal to i. we don't check for this because skipping
	// those writes would require an extra branch...
	for i := 1; i < l; i++ {
		next := p.cols[i]
		if next < prev {
			unsorted = true
		}
		if next != prev {
			n++
		}
		prev = next
		p.cols[n] = p.cols[i]
		p.rows[n] = p.rows[i]
	}
	p.rows = p.rows[:n+1]
	p.cols = p.cols[:n+1]
	return unsorted
}

// fullPrune trims any adjacent values with identical column keys (and
// the corresponding row values), and if it notices that anything was unsorted,
// does a stable sort by column key and tries that again, ensuring that
// there's no items with the same column key. The last entry with a given
// column key wins.
func (p *parallelSlices) fullPrune() {
	if len(p.cols) == 0 {
		return
	}
	if len(p.rows) != len(p.cols) {
		vprint.PanicOn("parallelSlices must have same length for rows and columns")
	}
	unsorted := p.prune()
	if unsorted {

		// Q: why sort.Stable instead of sort.Sort?
		//
		// A: Because we need to ensure that the last entry
		// is the one that wins. The last entry is the most recent update, and
		// so the mutex field should reflect that one and not earlier
		// updates. Mutex fields are special in that only 1 bit can
		// be hot (set to 1), so the last update overrides all the others. We
		// exploit this to eliminate irrelevant earlier writes.
		//
		// So if the input columns were {1, 2, 1},
		// and input rows were          {3, 4, 5},
		// we need to be sure that we end up with cols:{1, 2}
		//                                        rows:{5, 4} // right, last update won.
		//                                and not rows:{3, 4} // wrong, first update won.
		//
		// illustrated: (dk = don't know state)
		//
		// the new, raw data before later updates "win":
		//
		//         col0  col1  col2
		// row3    dk    1     dk
		// row4    dk    dk    1
		// row5    dk    1     dk
		//
		// after last one wins, to be written to the backend:
		//
		//         col0  col1  col2
		// row3    dk    0     0
		// row4    dk    0     1
		// row5    dk    1     0
		//               ^
		//                \-- on col1. The last one won, b/c its a mutex all other rows go to 0 for that column.
		//
		// Which means we need a stable sort, ensuring that if two things
		// have the same column key, they stay in the same relative order,
		// and then the prune algorithm always keeps the last.
		sort.Stable(p)

		_ = p.prune()
	}
}

func (p *parallelSlices) Len() int {
	return len(p.cols)
}

func (p *parallelSlices) Less(i, j int) bool {
	return p.cols[i] < p.cols[j]
}

func (p parallelSlices) Swap(i, j int) {
	p.cols[i], p.cols[j] = p.cols[j], p.cols[i]
	p.rows[i], p.rows[j] = p.rows[j], p.rows[i]
}

// importPositions takes slices of positions within the fragment to set and
// clear in storage. One must also pass in the set of unique rows which are
// affected by the set and clear operations. It is unprotected (f.mu must be
// locked when calling it). No position should appear in both set and clear.
//
// importPositions tries to intelligently decide whether or not to do a full
// snapshot of the fragment or just do in-memory updates while appending
// operations to the op log.
func (f *fragment) importPositions(tx Tx, set, clear []uint64, rowSet map[uint64]struct{}) error {
	if len(set) > 0 {
		CounterImportingN.Add(float64(len(set)))

		// TODO benchmark Add/RemoveN behavior with sorted/unsorted positions
		changedN, err := tx.Add(f.index(), f.field(), f.view(), f.shard, set...)
		if err != nil {
			return errors.Wrap(err, "adding positions")
		}
		CounterImportedN.Add(float64(changedN))
	}

	if len(clear) > 0 {
		CounterClearingingN.Add(float64(len(clear)))
		changedN, err := tx.Remove(f.index(), f.field(), f.view(), f.shard, clear...)
		if err != nil {
			return errors.Wrap(err, "clearing positions")
		}
		CounterClearedN.Add(float64(changedN))
	}
	return f.updateCaching(tx, rowSet)
}

// updateCaching clears checksums for rows, and clears any existing TopN
// cache for them, and marks the cache for needing updates. I'm not sure
// that's correct. This was originally the tail end of importPositions, but
// we want to be able to access the same logic from elsewhere.
func (f *fragment) updateCaching(tx Tx, rowSet map[uint64]struct{}) error {
	// Update cache counts for all affected rows.
	for rowID := range rowSet {
		// Invalidate block checksum.
		delete(f.checksums, int(rowID/HashBlockSize))

		if f.CacheType != CacheTypeNone {
			start := rowID * ShardWidth
			end := (rowID + 1) * ShardWidth

			n, err := tx.CountRange(f.index(), f.field(), f.view(), f.shard, start, end)
			if err != nil {
				return errors.Wrap(err, "CountRange")
			}

			f.cache.BulkAdd(rowID, n)
		}
	}

	if f.CacheType != CacheTypeNone {
		f.cache.Invalidate()
	}
	return nil
}

// sliceDifference removes everything from original that's found in remove,
// updating the slice in place, and returns the compacted slice. The input
// sets should be sorted.
func sliceDifference(original, remove []uint64) []uint64 {
	if len(remove) == 0 {
		return original
	}
	rn := 0
	rv := remove[rn]
	on := 0
	ov := uint64(0)
	n := 0

	for on, ov = range original {
		for rv < ov {
			rn++
			if rn >= len(remove) {
				return append(original[:n], original[on:]...)
			}
			rv = remove[rn]
		}
		if rv != ov {
			original[n] = ov
			n++
		}
	}
	return original[:n]
}

// bulkImportMutex performs a bulk import on a fragment while ensuring
// mutex restrictions. Because the mutex requirements must be checked
// against storage, this method must acquire a write lock on the fragment
// during the entire process, and it handles every bit independently.
func (f *fragment) bulkImportMutex(tx Tx, rowIDs, columnIDs []uint64, options *ImportOptions) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// if ingest promises that this is "fully sorted", then we have been
	// promised that (1) there's no duplicate entries that need to be
	// pruned, (2) the input is sorted by row IDs and then column IDs,
	// meaning that we will generate positions in strictly sequential order.
	if !options.fullySorted {
		p := parallelSlices{cols: columnIDs, rows: rowIDs}
		p.fullPrune()
		columnIDs = p.cols
		rowIDs = p.rows
	}

	// create a mask of columns we care about
	columns := roaring.NewSliceBitmap(columnIDs...)

	// we now need to find existing rows for these bits.
	rowSet := make(map[uint64]struct{}, len(rowIDs))
	unsorted := false
	prev := uint64(0)
	for i := range rowIDs {
		rowID, columnID := rowIDs[i], columnIDs[i]
		rowSet[rowID] = struct{}{}
		pos, err := f.pos(rowID, columnID)
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("finding pos for row %d, col %d", rowID, columnID))
		}
		// positions are sorted by columns, but not by absolute
		// position. we might want them sorted, though.
		if pos < prev {
			if options.fullySorted {
				fmt.Printf("HELP! was promised fully sorted input, but previous position was %d, now generated %d\n",
					prev, pos)
			}
			unsorted = true
		}
		prev = pos
		rowIDs[i] = pos
	}
	toSet := rowIDs
	if unsorted {
		sort.Slice(toSet, func(i, j int) bool { return toSet[i] < toSet[j] })
	}

	nextKey := toSet[0] >> 16
	scratchContainer := roaring.NewContainerArray([]uint16{})
	rewriteExisting := roaring.NewBitmapBitmapTrimmer(columns, func(key roaring.FilterKey, data *roaring.Container, filter *roaring.Container, writeback roaring.ContainerWriteback) error {
		var inserting []uint64
		for roaring.FilterKey(nextKey) < key {
			thisKey := roaring.FilterKey(nextKey)
			inserting, toSet, nextKey = roaring.GetMatchingKeysFrom(toSet, nextKey)
			scratchContainer = roaring.RemakeContainerFrom(scratchContainer, inserting)

			err := writeback(thisKey, scratchContainer)
			if err != nil {
				return err
			}
		}
		// we only wanted to insert the data we had before the end, there's
		// no actual data here to modify.
		if data == nil {
			return nil
		}
		if roaring.FilterKey(nextKey) > key {
			// simple path: we only have to remove things from the filter,
			// if there are any.
			if filter.N() == 0 {
				return nil
			}
			existing := data.N()
			data = data.DifferenceInPlace(filter)
			if data.N() != existing {
				rowSet[key.Row()] = struct{}{}
				return writeback(key, data)
			}
			return nil
		}
		// nextKey has to be the same as key. we have values to insert, and
		// necessarily have a filter to remove which matches them. so we're
		// going to remove everything in the filter, then add all the values
		// we have to insert. but! in the case where a bit is already set,
		// and we remove it and re-add it, we don't want to count that.
		inserting, toSet, nextKey = roaring.GetMatchingKeysFrom(toSet, nextKey)

		existing := data.N()

		// so, we want to remove anything that's in the filter, *but*, if a
		// thing is in the filter, and we then add it back, that doesn't
		// count. but if a thing is in the filter, but *wasn't originally
		// there*, that counts. But we can't check that *after* we compute
		// the difference, so...
		reAdds := 0
		for _, v := range inserting {
			if filter.Contains(uint16(v)) && data.Contains(uint16(v)) {
				reAdds++
			}
		}
		data = data.DifferenceInPlace(filter)
		removes := int(existing - data.N())
		var changed bool
		adds := 0
		for _, v := range inserting {
			data, changed = data.Add(uint16(v))
			if changed {
				adds++
			}
		}
		// if we added more things than were being readded, or removed more
		// things than were being readded, we changed something.
		if adds > reAdds || removes > reAdds {
			rowSet[key.Row()] = struct{}{}
			return writeback(key, data)
		}
		return nil
	})

	err := tx.ApplyRewriter(f.index(), f.field(), f.view(), f.shard, 0, rewriteExisting)
	if err != nil {
		return err
	}
	return f.updateCaching(tx, rowSet)
}

// ClearRecords deletes all bits for the given records. It's basically
// the remove-only part of setting a mutex.
func (f *fragment) ClearRecords(tx Tx, recordIDs []uint64) (bool, error) {
	// create a mask of columns we care about
	columns := roaring.NewSliceBitmap(recordIDs...)
	return f.clearRecordsByBitmap(tx, columns)
}

func (f *fragment) clearRecordsByBitmap(tx Tx, columns *roaring.Bitmap) (changed bool, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.unprotectedClearRecordsByBitmap(tx, columns)
}

// clearRecordsByBitmap clears bits in a fragment that correspond to those
// positions within the bitmap.
func (f *fragment) unprotectedClearRecordsByBitmap(tx Tx, columns *roaring.Bitmap) (changed bool, err error) {
	rowSet := make(map[uint64]struct{})
	rewriteExisting := roaring.NewBitmapBitmapTrimmer(columns, func(key roaring.FilterKey, data *roaring.Container, filter *roaring.Container, writeback roaring.ContainerWriteback) error {
		if filter.N() == 0 {
			return nil
		}
		existing := data.N()
		// nothing to delete. this can't happen normally, but the rewriter calls
		// us with an empty data container when it's done.
		if existing == 0 {
			return nil
		}
		data = data.DifferenceInPlace(filter)
		if data.N() != existing {
			rowSet[key.Row()] = struct{}{}
			changed = true
			return writeback(key, data)
		}
		return nil
	})

	err = tx.ApplyRewriter(f.index(), f.field(), f.view(), f.shard, 0, rewriteExisting)
	if err != nil {
		return false, err
	}
	return changed, f.updateCaching(tx, rowSet)
}

// importValue bulk imports a set of range-encoded values.
func (f *fragment) importValue(tx Tx, columnIDs []uint64, values []int64, bitDepth uint64, clear bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Verify that there are an equal number of column ids and values.
	if len(columnIDs) != len(values) {
		return fmt.Errorf("mismatch of column/value len: %d != %d", len(columnIDs), len(values))
	}
	positionsByDepth := make([][]uint64, bitDepth+2)
	toSetByDepth := make([]int, bitDepth+2)
	toClearByDepth := make([]int, bitDepth+2)
	batchSize := len(columnIDs)
	if batchSize > 65536 {
		batchSize = 65536
	}
	for i := 0; i < int(bitDepth)+2; i++ {
		positionsByDepth[i] = make([]uint64, batchSize)
		toClearByDepth[i] = batchSize
	}

	row := 0
	columnID := uint64(0)
	value := int64(0)
	// arbitrarily set prev to be not equal to the first column ID
	// we will encounter.
	prev := columnIDs[len(columnIDs)-1] + 1
	for len(columnIDs) > 0 {
		downTo := len(columnIDs) - batchSize
		if downTo < 0 {
			downTo = 0
		}
		for i := range positionsByDepth {
			toSetByDepth[i] = 0
			toClearByDepth[i] = batchSize
		}
		for i := len(columnIDs) - 1; i >= downTo; i-- {
			columnID, value = columnIDs[i], values[i]
			columnID = columnID % ShardWidth
			if columnID == prev {
				continue
			}
			prev = columnID
			row = 0
			if clear {
				toClearByDepth[row]--
				positionsByDepth[row][toClearByDepth[row]] = columnID
			} else {
				positionsByDepth[row][toSetByDepth[row]] = columnID
				toSetByDepth[row]++
			}
			row++
			columnID += ShardWidth
			if value < 0 {
				positionsByDepth[row][toSetByDepth[row]] = columnID
				toSetByDepth[row]++
				value *= -1
			} else {
				toClearByDepth[row]--
				positionsByDepth[row][toClearByDepth[row]] = columnID
			}
			row++
			columnID += ShardWidth
			for j := 0; j < int(bitDepth); j++ {
				if value&1 != 0 {
					positionsByDepth[row][toSetByDepth[row]] = columnID
					toSetByDepth[row]++
				} else {
					toClearByDepth[row]--
					positionsByDepth[row][toClearByDepth[row]] = columnID
				}
				row++
				columnID += ShardWidth
				value >>= 1
			}
		}

		for i := range positionsByDepth {
			err := f.importPositions(tx, positionsByDepth[i][:toSetByDepth[i]], positionsByDepth[i][toClearByDepth[i]:], nil)
			if err != nil {
				return errors.Wrap(err, "importing positions")
			}
		}
		columnIDs = columnIDs[:downTo]
	}

	return nil
}

// importRoaring imports from the official roaring data format defined at
// https://github.com/RoaringBitmap/RoaringFormatSpec or from pilosa's version
// of the roaring format. The cache is updated to reflect the new data.
func (f *fragment) importRoaring(ctx context.Context, tx Tx, data []byte, clear bool) error {
	span, ctx := tracing.StartSpanFromContext(ctx, "fragment.importRoaring")
	defer span.Finish()

	rowSet, updateCache, err := f.doImportRoaring(ctx, tx, data, clear)
	if err != nil {
		return errors.Wrap(err, "doImportRoaring")
	}
	if updateCache {
		return f.updateCachePostImport(ctx, rowSet)
	}
	return nil
}

// ImportRoaringClearAndSet simply clears the bits in clear and sets the bits in set.
func (f *fragment) ImportRoaringClearAndSet(ctx context.Context, tx Tx, clear, set []byte) error {
	clearIter, err := roaring.NewContainerIterator(clear)
	if err != nil {
		return errors.Wrap(err, "getting clear iterator")
	}
	setIter, err := roaring.NewContainerIterator(set)
	if err != nil {
		return errors.Wrap(err, "getting set iterator")
	}

	rewriter, err := roaring.NewClearAndSetRewriter(clearIter, setIter)
	if err != nil {
		return errors.Wrap(err, "getting rewriter")
	}

	err = tx.ApplyRewriter(f.index(), f.field(), f.view(), f.shard, 0, rewriter)
	if err != nil {
		return fmt.Errorf("pilosa.ImportRoaringClearAndSet: %s", err)
	}
	if f.CacheType != CacheTypeNone {
		// TODO this may be quite a bit slower than the way
		// importRoaring does it as it tracks the number of bits
		// changed per row. We could do that, but I think it'd require
		// significant changes to the Rewriter API.
		f.mu.Lock()
		defer f.mu.Unlock()
		return f.rebuildRankCache(ctx, tx)
	}
	return nil
}

// ImportRoaringBSI interprets "clear" as a single row specifying
// records to be cleared, and "set" as specifying the values to be set
// which implies clearing any other values in those columns.
func (f *fragment) ImportRoaringBSI(ctx context.Context, tx Tx, clear, set []byte) error {
	// In this first block, we take the first row of clear as records
	// we want to unconditionally clear, and the first row of set as
	// records we also want to clear because they're going to get set
	// and Union the two together into a single clearing iterator.
	clearclearIter, err := roaring.NewRepeatedRowIteratorFromBytes(clear)
	if err != nil {
		return errors.Wrap(err, "getting clear iterator")
	}
	setClearIter, err := roaring.NewRepeatedRowIteratorFromBytes(set)
	if err != nil {
		return errors.Wrap(err, "getting set/clear iterator")
	}
	clearIter := roaring.NewUnionContainerIterator(clearclearIter, setClearIter)

	// Then we get the set iterator and create the rewriter.
	setIter, err := roaring.NewContainerIterator(set)
	if err != nil {
		return errors.Wrap(err, "getting set iterator")
	}
	rewriter, err := roaring.NewClearAndSetRewriter(clearIter, setIter)
	if err != nil {
		return errors.Wrap(err, "getting rewriter")
	}

	err = tx.ApplyRewriter(f.index(), f.field(), f.view(), f.shard, 0, rewriter)
	return errors.Wrap(err, "pilosa.ImportRoaringBSI: ")
}

// ImportRoaringSingleValued treats "clear" as a single row and clears
// all the columns specified, then sets all the bits in set. It's very
// similar to ImportRoaringBSI, but doesn't treate the first row of
// "set" as the existence row to also be cleared. Essentially it's for
// FieldTypeMutex.
func (f *fragment) ImportRoaringSingleValued(ctx context.Context, tx Tx, clear, set []byte) error {
	clearIter, err := roaring.NewRepeatedRowIteratorFromBytes(clear)
	if err != nil {
		return errors.Wrap(err, "getting cleariterator")
	}
	setIter, err := roaring.NewContainerIterator(set)
	if err != nil {
		return errors.Wrap(err, "getting set iterator")
	}

	rewriter, err := roaring.NewClearAndSetRewriter(clearIter, setIter)
	if err != nil {
		return errors.Wrap(err, "getting rewriter")
	}

	err = tx.ApplyRewriter(f.index(), f.field(), f.view(), f.shard, 0, rewriter)
	return errors.Wrap(err, "pilosa.ImportRoaringSingleValued: ")
}

func (f *fragment) doImportRoaring(ctx context.Context, tx Tx, data []byte, clear bool) (map[uint64]int, bool, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	rowSize := uint64(1 << shardVsContainerExponent)
	span, _ := tracing.StartSpanFromContext(ctx, "importRoaring.ImportRoaringBits")
	defer span.Finish()

	var rowSet map[uint64]int
	err := func() (err error) {
		var rit roaring.RoaringIterator
		rit, err = roaring.NewRoaringIterator(data)
		if err != nil {
			return err
		}

		_, rowSet, err = tx.ImportRoaringBits(f.index(), f.field(), f.view(), f.shard, rit, clear, true, rowSize)
		return err
	}()
	if err != nil {
		return nil, false, err
	}

	updateCache := f.CacheType != CacheTypeNone
	return rowSet, updateCache, err
}

func (f *fragment) updateCachePostImport(ctx context.Context, rowSet map[uint64]int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	anyChanged := false

	for rowID, changes := range rowSet {
		if changes == 0 {
			continue
		}
		anyChanged = true
		if changes < 0 {
			absChanges := uint64(-1 * changes)
			if absChanges <= f.cache.Get(rowID) {
				f.cache.BulkAdd(rowID, f.cache.Get(rowID)-absChanges)
			} else {
				f.cache.BulkAdd(rowID, 0)
			}
		} else {
			f.cache.BulkAdd(rowID, f.cache.Get(rowID)+uint64(changes))
		}
	}
	// we only set this if we need to update the cache
	if anyChanged {
		f.cache.Invalidate()
	}

	return nil
}

// importRoaringOverwrite overwrites the specified block with the provided data.
func (f *fragment) importRoaringOverwrite(ctx context.Context, tx Tx, data []byte, block int) error {
	// Clear the existing data from fragment block.
	if _, err := f.clearBlock(tx, block); err != nil {
		return errors.Wrapf(err, "clearing block: %d", block)
	}

	// Union the new block data with the fragment data.
	return f.importRoaring(ctx, tx, data, false)
}

// RecalculateCache rebuilds the cache regardless of invalidate time delay.
func (f *fragment) RecalculateCache() {
	f.mu.Lock()
	f.cache.Recalculate()
	f.mu.Unlock()
}

// FlushCache writes the cache data to disk.
func (f *fragment) FlushCache() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.flushCache()
}

func (f *fragment) rebuildRankCache(ctx context.Context, tx Tx) error {
	if f.CacheType != CacheTypeRanked {
		return nil // only rebuild ranked caches
	}

	f.cache.Clear()
	rows, err := f.unprotectedRows(ctx, tx, uint64(0))
	if err != nil {
		return err
	}
	for _, id := range rows {
		n, err := tx.CountRange(f.index(), f.field(), f.view(), f.shard, id*ShardWidth, (id+1)*ShardWidth)
		if err != nil {
			return errors.Wrap(err, "CountRange")
		}
		f.cache.BulkAdd(id, n)
	}
	f.cache.Invalidate()
	return nil
}

func (f *fragment) RebuildRankCache(ctx context.Context) error {
	if f.CacheType != CacheTypeRanked {
		return nil //only rebuild ranked caches
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	tx, err := f.holder.BeginTx(false, f.idx, f.shard)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	return f.rebuildRankCache(ctx, tx)
}

func (f *fragment) flushCache() error {
	if f.cache == nil {
		return nil
	}

	if f.CacheType == CacheTypeNone {
		return nil
	}

	// Retrieve a list of row ids from the cache.
	ids := f.cache.IDs()

	// Marshal cache data to bytes.
	buf, err := proto.Marshal(&pb.Cache{IDs: ids})
	if err != nil {
		return errors.Wrap(err, "marshalling")
	}

	if err := os.MkdirAll(filepath.Dir(f.cachePath()), 0750); err != nil {
		return errors.Wrap(err, "mkdir")
	}
	// Write to disk.
	if err := os.WriteFile(f.cachePath(), buf, 0600); err != nil {
		return errors.Wrap(err, "writing")
	}

	return nil
}

// WriteTo writes the fragment's data to w.
func (f *fragment) WriteTo(w io.Writer) (n int64, err error) {
	// Force cache flush.
	if err := f.FlushCache(); err != nil {
		return 0, errors.Wrap(err, "flushing cache")
	}

	// Write out data and cache to a tar archive.
	tw := tar.NewWriter(w)
	if err := f.writeStorageToArchive(tw); err != nil {
		return 0, fmt.Errorf("write storage: %s", err)
	}
	if err := f.writeCacheToArchive(tw); err != nil {
		return 0, fmt.Errorf("write cache: %s", err)
	}
	return 0, nil
}

// used in shipping the slices across the network for a resize.
func (f *fragment) writeStorageToArchive(tw *tar.Writer) error {

	tx := f.idx.holder.txf.NewTx(Txo{Write: !writable, Index: f.idx, Shard: f.shard})
	defer tx.Rollback()
	rbm, err := tx.RoaringBitmap(f.index(), f.field(), f.view(), f.shard)
	if err != nil {
		return errors.Wrap(err, "RoaringBitmapReader RoaringBitmap")
	}
	var buf bytes.Buffer
	sz, err := rbm.WriteTo(&buf)
	if err != nil {
		return errors.Wrap(err, "RoaringBitmapReader rbm.WriteTo(buf)")
	}

	// Write archive header.
	if err := tw.WriteHeader(&tar.Header{
		Name:    "data",
		Mode:    0600,
		Size:    sz,
		ModTime: time.Now(),
	}); err != nil {
		return errors.Wrap(err, "writing header")
	}

	// Copy the file up to the last known size.
	// This is done outside the lock because the storage format is append-only.
	if _, err := io.CopyN(tw, &buf, sz); err != nil {
		return errors.Wrap(err, "copying")
	}
	return nil
}

func (f *fragment) writeCacheToArchive(tw *tar.Writer) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Read cache into buffer.
	buf, err := os.ReadFile(f.cachePath())
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return errors.Wrap(err, "reading cache")
	}

	// Write archive header.
	if err := tw.WriteHeader(&tar.Header{
		Name:    "cache",
		Mode:    0600,
		Size:    int64(len(buf)),
		ModTime: time.Now(),
	}); err != nil {
		return errors.Wrap(err, "writing header")
	}

	// Write data to archive.
	if _, err := tw.Write(buf); err != nil {
		return errors.Wrap(err, "writing")
	}
	return nil
}

// ReadFrom reads a data file from r and loads it into the fragment.
func (f *fragment) ReadFrom(r io.Reader) (n int64, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	tr := tar.NewReader(r)
	for {
		// Read next tar header.
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			return 0, errors.Wrap(err, "opening")
		}

		// Process file based on file name.
		switch hdr.Name {
		case "data":
			tx := f.holder.txf.NewTx(Txo{Write: writable, Index: f.idx, Fragment: f, Shard: f.shard})
			defer tx.Rollback()
			if err := f.fillFragmentFromArchive(tx, tr); err != nil {
				return 0, errors.Wrap(err, "reading storage")
			}
			if err := tx.Commit(); err != nil {
				return 0, errors.Wrap(err, "Commit after tx.ReadFragmentFromArchive")
			}
		case "cache":
			if err := f.readCacheFromArchive(tr); err != nil {
				return 0, errors.Wrap(err, "reading cache")
			}
		default:
			return 0, fmt.Errorf("invalid fragment archive file: %s", hdr.Name)
		}
	}

	return 0, nil
}

// should be morally equivalent to fragment.readStorageFromArchive()
// below for RoaringTx, but also work on any Tx because it uses
// tx.ImportRoaringBits().
func (f *fragment) fillFragmentFromArchive(tx Tx, r io.Reader) error {

	// this is reading from inside a tarball, so definitely no need
	// to close it here.
	data, err := io.ReadAll(r)
	if err != nil {
		return errors.Wrap(err, "fillFragmentFromArchive io.ReadAll(r)")
	}
	if len(data) == 0 {
		return nil
	}

	// For reference, compare to what fragment.go:313 fragment.importStorage() does.

	clear := false
	log := false
	rowSize := uint64(0)
	itr, err := roaring.NewRoaringIterator(data)
	if err != nil {
		return errors.Wrap(err, "fillFragmentFromArchive NewRoaringIterator")
	}
	changed, rowSet, err := tx.ImportRoaringBits(f.index(), f.field(), f.view(), f.shard, itr, clear, log, rowSize)
	_, _ = changed, rowSet
	if err != nil {
		return errors.Wrap(err, "fillFragmentFromArchive ImportRoaringBits")
	}
	return nil
}

func (f *fragment) readCacheFromArchive(r io.Reader) error {
	// Slurp data from reader and write to disk.
	buf, err := io.ReadAll(r)
	if err != nil {
		return errors.Wrap(err, "reading")
	} else if err := os.WriteFile(f.cachePath(), buf, 0600); err != nil {
		return errors.Wrap(err, "writing")
	}

	// Re-open cache.
	if err := f.openCache(); err != nil {
		return errors.Wrap(err, "opening")
	}

	return nil
}

func (f *fragment) minRowID(tx Tx) (uint64, bool, error) {
	min, ok, err := tx.Min(f.index(), f.field(), f.view(), f.shard)
	return min / ShardWidth, ok, err
}

// rows returns all rows starting from 'start'. Filters will be applied in
// order. All filters must return true to include the row. Once a row is
// included, further containers in that row will be skipped. So, for a row to be
// included, there must be one container in that row where all filters return
// true. For a row to be skipped, at least one filter must return false for each
// container in that row (it need not be the same filter for each). Any filter
// returning done == true will cause processing to stop after all filters for
// this container have been processed. The rows accumulated up to this point
// (including this row if all filters passed) will be returned.
func (f *fragment) rows(ctx context.Context, tx Tx, start uint64, filters ...roaring.BitmapFilter) ([]uint64, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.unprotectedRows(ctx, tx, start, filters...)
}

// unprotectedRows calls rows without grabbing the mutex.
func (f *fragment) unprotectedRows(ctx context.Context, tx Tx, start uint64, filters ...roaring.BitmapFilter) ([]uint64, error) {
	var rows []uint64
	cb := func(row uint64) error {
		rows = append(rows, row)
		return nil
	}
	startKey := rowToKey(start)
	filter := roaring.NewBitmapRowFilter(cb, filters...)
	err := tx.ApplyFilter(f.index(), f.field(), f.view(), f.shard, startKey, filter)
	if err != nil {
		return nil, err
	} else {
		return rows, nil
	}
}

// unionRows yields the union of the given rows in this fragment
func (f *fragment) unionRows(ctx context.Context, tx Tx, rows []uint64) (*Row, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.unprotectedUnionRows(ctx, tx, rows)
}

// unprotectedRows calls rows without grabbing the mutex.
func (f *fragment) unprotectedUnionRows(ctx context.Context, tx Tx, rows []uint64) (*Row, error) {
	filter := roaring.NewBitmapRowsUnion(rows)
	err := tx.ApplyFilter(f.index(), f.field(), f.view(), f.shard, 0, filter)
	if err != nil {
		return nil, err
	} else {
		row := &Row{
			Segments: []RowSegment{{
				data:     filter.Results(f.shard),
				shard:    f.shard,
				writable: true,
			}},
		}
		row.invalidateCount()
		return row, nil
	}
}

type rowIterator interface {
	// TODO(kuba) linter suggests to use io.Seeker
	// Seek(offset int64, whence int) (int64, error)
	Seek(uint64)

	Next() (*Row, uint64, *int64, bool, error)
}

func (f *fragment) rowIterator(tx Tx, wrap bool, filters ...roaring.BitmapFilter) (rowIterator, error) {
	if strings.HasPrefix(f.view(), viewBSIGroupPrefix) {
		return f.intRowIterator(tx, wrap, filters...)
	}
	// viewStandard
	// TODO(kuba) - IMHO we should check if f.view() is viewStandard,
	// but because of testing the function returns set iterator as default one.
	return f.setRowIterator(tx, wrap, filters...)
}

type timeRowIterator struct {
	tx               Tx
	cur              int
	wrap             bool
	allRowIDs        []uint64
	rowIDToFragments map[uint64][]*fragment
}

func timeFragmentsRowIterator(fragments []*fragment, tx Tx, wrap bool, filters ...roaring.BitmapFilter) (rowIterator, error) {
	if len(fragments) == 0 {
		return nil, fmt.Errorf("there should be at least 1 fragment")
	} else if len(fragments) == 1 {
		return fragments[0].setRowIterator(tx, wrap, filters...)
	}

	it := &timeRowIterator{
		tx:   tx,
		cur:  0,
		wrap: wrap,
	}

	// create a sort of inverted index that maps each
	// rowID back to the fragments that have that rowID
	rowIDToFragments := make(map[uint64][]*fragment)
	for _, f := range fragments {
		rowIDs, err := f.rows(context.Background(), tx, 0, filters...)
		if err != nil {
			return nil, err
		}
		for _, rowID := range rowIDs {
			fs := append(rowIDToFragments[rowID], f)
			rowIDToFragments[rowID] = fs
		}
	}

	// if len(rowIDToFragments) == 0 what to do ??
	// ie all fragments returned empty rowIDs, is this possible
	// is this an error

	// collect all rowIDs from inverted index to a slice
	allRowIDs := make([]uint64, len(rowIDToFragments))
	i := 0
	for rowID := range rowIDToFragments {
		allRowIDs[i] = rowID
		i++
	}
	sort.Slice(allRowIDs, func(i, j int) bool { return allRowIDs[i] < allRowIDs[j] })

	it.rowIDToFragments = rowIDToFragments
	it.allRowIDs = allRowIDs

	return it, nil
}

func (it *timeRowIterator) Seek(rowID uint64) {
	idx := sort.Search(len(it.allRowIDs), func(i int) bool {
		return it.allRowIDs[i] >= rowID
	})
	it.cur = idx
}

func (it *timeRowIterator) Next() (r *Row, rowID uint64, _ *int64, wrapped bool, err error) {
	if it.cur >= len(it.allRowIDs) {
		if !it.wrap || len(it.allRowIDs) == 0 {
			return nil, 0, nil, true, nil
		}
		it.Seek(0)
		wrapped = true
	}

	// gather rows
	rowID = it.allRowIDs[it.cur]
	fragments := it.rowIDToFragments[rowID]
	rows := make([]*Row, 0, len(fragments))
	for _, fragment := range fragments {
		row, err := fragment.row(it.tx, rowID)
		if err != nil {
			return row, rowID, nil, wrapped, err
		}
		rows = append(rows, row)
	}

	// union rows
	r = rows[0].Union(rows[1:]...)

	it.cur++
	return r, rowID, nil, wrapped, nil
}

type intRowIterator struct {
	f      *fragment
	values int64Slice         // sorted slice of int values
	colIDs map[int64][]uint64 // [int value] -> [column IDs]
	cur    int                // current value index() (rowID)
	wrap   bool
}

func (f *fragment) intRowIterator(tx Tx, wrap bool, filters ...roaring.BitmapFilter) (rowIterator, error) {
	it := intRowIterator{
		f:      f,
		colIDs: make(map[int64][]uint64),
		cur:    0,
		wrap:   wrap,
	}

	// accumulator [column ID] -> [int value]
	acc := make(map[uint64]int64)

	f.mu.RLock()
	defer f.mu.RUnlock()
	callback := func(rid uint64) error {
		// skip exist(0) and sign(1) rows
		if rid == bsiExistsBit || rid == bsiSignBit {
			return nil
		}

		val := int64(1 << (rid - bsiOffsetBit))
		r, err := f.unprotectedRow(tx, rid)
		if err != nil {
			return err
		}
		for _, cid := range r.Columns() {
			acc[cid] |= val
		}
		return nil
	}
	if err := f.foreachRow(tx, filters, callback); err != nil {
		return nil, err
	}

	// apply exist and sign bits
	r0, err := f.unprotectedRow(tx, 0)
	if err != nil {
		return nil, err
	}
	allCols := r0.Columns()

	r1, err := f.unprotectedRow(tx, 1)
	if err != nil {
		return nil, err
	}
	signCols := r1.Columns()
	signIdx, signLen := 0, len(signCols)

	// all distinct values
	values := make(map[int64]struct{})
	for _, cid := range allCols {
		// apply sign bit
		if signIdx < signLen && cid == signCols[signIdx] {
			if tmp, ok := acc[cid]; ok {
				acc[cid] = -tmp
			}

			signIdx++
		}

		val := acc[cid]
		it.colIDs[val] = append(it.colIDs[val], cid)

		if _, ok := values[val]; !ok {
			it.values = append(it.values, val)
			values[val] = struct{}{}
		}
	}
	sort.Sort(it.values)

	return &it, nil
}

func (f *fragment) foreachRow(tx Tx, filters []roaring.BitmapFilter, fn func(rid uint64) error) error {
	filter := roaring.NewBitmapRowFilter(fn, filters...)
	err := tx.ApplyFilter(f.index(), f.field(), f.view(), f.shard, 0, filter)
	return errors.Wrap(err, "pilosa.foreachRow: ")
}

func (it *intRowIterator) Seek(rowID uint64) {
	idx := sort.Search(len(it.values), func(i int) bool {
		return it.values[i] >= it.values[rowID]
	})
	it.cur = idx
}

func (it *intRowIterator) Next() (r *Row, rowID uint64, value *int64, wrapped bool, err error) {
	if it.cur >= len(it.values) {
		if !it.wrap || len(it.values) == 0 {
			return nil, 0, nil, true, nil
		}
		wrapped = true
		it.cur = 0
	}
	if it.cur >= 0 {
		rowID = uint64(it.cur)
		value = &it.values[rowID]
		r = NewRow(it.colIDs[*value]...)
	}
	it.cur++
	return r, rowID, value, wrapped, nil
}

type setRowIterator struct {
	tx     Tx
	f      *fragment
	rowIDs []uint64
	cur    int
	wrap   bool
}

func (f *fragment) setRowIterator(tx Tx, wrap bool, filters ...roaring.BitmapFilter) (rowIterator, error) {
	rows, err := f.rows(context.Background(), tx, 0, filters...)
	if err != nil {
		return nil, err
	}
	return &setRowIterator{
		tx:     tx,
		f:      f,
		rowIDs: rows, // TODO: this may be memory intensive in high cardinality cases
		wrap:   wrap,
	}, nil
}

func (it *setRowIterator) Seek(rowID uint64) {
	idx := sort.Search(len(it.rowIDs), func(i int) bool {
		return it.rowIDs[i] >= rowID
	})
	it.cur = idx
}

func (it *setRowIterator) Next() (r *Row, rowID uint64, _ *int64, wrapped bool, err error) {
	if it.cur >= len(it.rowIDs) {
		if !it.wrap || len(it.rowIDs) == 0 {
			return nil, 0, nil, true, nil
		}
		it.Seek(0)
		wrapped = true
	}
	id := it.rowIDs[it.cur]
	r, err = it.f.row(it.tx, id)
	if err != nil {
		return r, rowID, nil, wrapped, err
	}

	rowID = id

	it.cur++
	return r, rowID, nil, wrapped, nil
}

// pos returns the row position of a row/column pair.
func pos(rowID, columnID uint64) uint64 {
	return (rowID * ShardWidth) + (columnID % ShardWidth)
}

// vector stores the mapping of colID to rowID.
// It's used for a mutex field type.
type vector interface {
	Get(tx Tx, colID uint64) (uint64, bool, error)
}

// rowsVector implements the vector interface by looking
// at row data as needed.
type rowsVector struct {
	f *fragment
}

// newRowsVector returns a rowsVector for a given fragment.
func newRowsVector(f *fragment) *rowsVector {
	return &rowsVector{
		f: f,
	}
}

// Get returns the rowID associated to the given colID.
// Additionally, it returns true if a value was found,
// otherwise it returns false. Ensure that you already
// have the mutex before calling this.
func (v *rowsVector) Get(tx Tx, colID uint64) (uint64, bool, error) {
	rows, err := v.f.unprotectedRows(context.Background(), tx, 0, roaring.NewBitmapColumnFilter(colID))
	if err != nil {
		return 0, false, err
	} else if len(rows) > 1 {
		return 0, false, errors.New("found multiple row values for column")
	} else if len(rows) == 1 {
		return rows[0], true, nil
	}
	return 0, false, nil
}

// rowToKey converts a Pilosa row ID to the key of the container which starts
// that row in the bitmap which represents this entire fragment. A fragment is
// all the rows within a shard within a field concatenated together.
func rowToKey(rowID uint64) (key uint64) {
	return rowID * (ShardWidth / containerWidth)
}

// boolVector implements the vector interface by looking
// at data in rows 0 and 1.
type boolVector struct {
	f *fragment
}

// newBoolVector returns a boolVector for a given fragment.
func newBoolVector(f *fragment) *boolVector {
	return &boolVector{
		f: f,
	}
}

// Get returns the rowID associated to the given colID.
// Additionally, it returns true if a value was found,
// otherwise it returns false. Ensure that you already
// have the fragment mutex before calling this.
func (v *boolVector) Get(tx Tx, colID uint64) (uint64, bool, error) {
	rows, err := v.f.unprotectedRows(context.Background(), tx, 0, roaring.NewBitmapColumnFilter(colID))
	if err != nil {
		return 0, false, err
	} else if len(rows) > 1 {
		return 0, false, errors.New("found multiple row values for column")
	} else if len(rows) == 1 {
		switch rows[0] {
		case falseRowID, trueRowID:
			return rows[0], true, nil
		default:
			return 0, false, errors.New("found non-boolean value")
		}
	}
	return 0, false, nil
}

// FormatQualifiedFragmentName generates a qualified name for the fragment to be used with Tx operations.
func FormatQualifiedFragmentName(index, field, view string, shard uint64) string {
	return fmt.Sprintf("%s\x00%s\x00%s\x00%d", index, field, view, shard)
}

// ParseQualifiedFragmentName parses a qualified name into its parts.
func ParseQualifiedFragmentName(name string) (index, field, view string, shard uint64, err error) {
	a := strings.Split(name, "\x00")
	if len(a) < 4 {
		return "", "", "", 0, fmt.Errorf("invalid qualified name: %q", name)
	}
	index, field, view = string(a[0]), string(a[1]), string(a[2])
	if shard, err = strconv.ParseUint(a[3], 10, 64); err != nil {
		return "", "", "", 0, fmt.Errorf("invalid qualified name: %q", name)
	}
	return index, field, view, shard, nil
}

type RowKV struct {
	RowID uint64      `json:"id"`
	Value interface{} `json:"value"`
}

func (r *RowKV) Compare(o RowKV, desc bool) (bool, bool) {
	switch val := r.Value.(type) {
	case string:
		if oVal, ok := o.Value.(string); ok {
			return desc != (val < oVal), true
		}
		return desc, false
	case bool:
		if oVal, ok := o.Value.(bool); ok {
			return desc != oVal, true
		}
		return desc, false
	case int64:
		if oVal, ok := o.Value.(int64); ok {
			return desc != (val < oVal), true
		}
		return desc, false
	default:
		return desc, false
	}
}

// sortBSIData, fetches the rows and seperates the positive and negetive values.
// these values and sorted seperately and appended
func (f *fragment) sortBsiData(tx Tx, filter *Row, bitDepth uint64, sort_desc bool) (*SortedRow, error) {
	consider, err := f.row(tx, bsiExistsBit)
	if err != nil {
		return nil, err
	} else if filter != nil {
		consider = consider.Intersect(filter)
	}
	row, err := f.row(tx, bsiSignBit)
	if err != nil {
		return nil, err
	}
	pos := consider.Difference(row)
	neg := consider.Difference(pos)

	var sortedRowIds []RowKV
	f.flattenRowValues(tx, &sortedRowIds, neg, bitDepth, -1)
	ok := true
	f.flattenRowValues(tx, &sortedRowIds, pos, bitDepth, 1)
	sort.SliceStable(sortedRowIds, func(i, j int) bool {
		if c, k := sortedRowIds[i].Compare(sortedRowIds[j], sort_desc); k {
			return c
		} else {
			ok = false
			return !k
		}
	})
	if !ok {
		return nil, errors.New("Couldn't compare field type for sorting")
	}

	return &SortedRow{
		Row:    consider,
		RowKVs: sortedRowIds,
	}, nil
}

func (f *fragment) flattenRowValues(tx Tx, sortedRowIds *[]RowKV, filter *Row, bitDepth uint64, sign int64) error {
	m := make(map[uint64]int64)
	for i := int(bitDepth - 1); i >= 0; i-- {
		row, err := f.row(tx, uint64(bsiOffsetBit+i))
		if err != nil {
			return err
		}
		row = row.Intersect(filter)

		for _, v := range row.Columns() {
			if val, ok := m[v]; ok {
				m[v] = val | (1 << i)
			} else {
				m[v] = (1 << i)
			}
		}

	}

	for k, v := range m {
		*sortedRowIds = append(*sortedRowIds, RowKV{
			RowID: k,
			Value: (v * sign),
		})
	}
	return nil
}
