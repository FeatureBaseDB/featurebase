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

package pilosa

import (
	"archive/tar"
	"bufio"
	"bytes"
	"container/heap"
	"context"
	"encoding/binary"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"github.com/cespare/xxhash"

	"math"

	"github.com/gogo/protobuf/proto"
	"github.com/pilosa/pilosa/internal"
	"github.com/pilosa/pilosa/pql"
	"github.com/pilosa/pilosa/roaring"
	"github.com/pkg/errors"
)

const (
	// SliceWidth is the number of column IDs in a slice.
	SliceWidth = 1048576

	// snapshotExt is the file extension used for an in-process snapshot.
	snapshotExt = ".snapshotting"

	// copyExt is the file extension used for the temp file used while copying.
	copyExt = ".copying"

	// cacheExt is the file extension for persisted cache ids.
	cacheExt = ".cache"

	// HashBlockSize is the number of rows in a merkle hash block.
	HashBlockSize = 100

	// defaultFragmentMaxOpN is the default value for Fragment.MaxOpN.
	defaultFragmentMaxOpN = 2000
)

// Fragment represents the intersection of a field and slice in an index.
type Fragment struct {
	mu sync.RWMutex

	// Composite identifiers
	index string
	field string
	view  string
	slice uint64

	// File-backed storage
	path        string
	file        *os.File
	storage     *roaring.Bitmap
	storageData []byte
	opN         int // number of ops since snapshot

	// Cache for row counts.
	CacheType string // passed in by field
	cache     Cache
	CacheSize uint32

	// Stats reporting.
	maxRowID uint64

	// Cache containing full rows (not just counts).
	rowCache BitmapCache

	// Cached checksums for each block.
	checksums map[int][]byte

	// Number of operations performed before performing a snapshot.
	// This limits the size of fragments on the heap and flushes them to disk
	// so that they can be mmapped and heap utilization can be kept low.
	MaxOpN int

	// Logger used for out-of-band log entries.
	Logger Logger

	// Row attribute storage.
	// This is set by the parent field unless overridden for testing.
	RowAttrStore AttrStore

	stats StatsClient
}

// NewFragment returns a new instance of Fragment.
func NewFragment(path, index, field, view string, slice uint64) *Fragment {
	return &Fragment{
		path:      path,
		index:     index,
		field:     field,
		view:      view,
		slice:     slice,
		CacheType: defaultCacheType,
		CacheSize: defaultCacheSize,

		Logger: NopLogger,
		MaxOpN: defaultFragmentMaxOpN,

		stats: NopStatsClient,
	}
}

// cachePath returns the path to the fragment's cache data.
func (f *Fragment) cachePath() string { return f.path + cacheExt }

// Open opens the underlying storage.
func (f *Fragment) Open() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := func() error {
		// Initialize storage in a function so we can close if anything goes wrong.
		if err := f.openStorage(); err != nil {
			return errors.Wrap(err, "opening storage")
		}

		// Fill cache with rows persisted to disk.
		if err := f.openCache(); err != nil {
			return errors.Wrap(err, "opening cache")
		}

		// Clear checksums.
		f.checksums = make(map[int][]byte)

		// Read last bit to determine max row.
		pos := f.storage.Max()
		f.maxRowID = pos / SliceWidth
		f.stats.Gauge("rows", float64(f.maxRowID), 1.0)

		return nil
	}(); err != nil {
		f.close()
		return err
	}

	return nil
}

// openStorage opens the storage bitmap.
func (f *Fragment) openStorage() error {
	// Create a roaring bitmap to serve as storage for the slice.
	if f.storage == nil {
		f.storage = roaring.NewFileBitmap()
	}
	// Open the data file to be mmap'd and used as an ops log.
	file, err := os.OpenFile(f.path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return fmt.Errorf("open file: %s", err)
	}
	f.file = file

	// Lock the underlying file.
	if err := syscall.Flock(int(f.file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return fmt.Errorf("flock: %s", err)
	}

	// If the file is empty then initialize it with an empty bitmap.
	fi, err := f.file.Stat()
	if err != nil {
		return errors.Wrap(err, "statting file before")
	} else if fi.Size() == 0 {
		bi := bufio.NewWriter(f.file)
		if _, err := f.storage.WriteTo(bi); err != nil {
			return fmt.Errorf("init storage file: %s", err)
		}
		bi.Flush()
		fi, err = f.file.Stat()
		if err != nil {
			return errors.Wrap(err, "statting file after")
		}
	}

	// Mmap the underlying file so it can be zero copied.
	storageData, err := syscall.Mmap(int(f.file.Fd()), 0, int(fi.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("mmap: %s", err)
	}
	f.storageData = storageData

	// Advise the kernel that the mmap is accessed randomly.
	if err := madvise(f.storageData, syscall.MADV_RANDOM); err != nil {
		return fmt.Errorf("madvise: %s", err)
	}

	// Attach the mmap file to the bitmap.
	data := f.storageData
	if err := f.storage.UnmarshalBinary(data); err != nil {
		return fmt.Errorf("unmarshal storage: file=%s, err=%s", f.file.Name(), err)
	}

	// Attach the file to the bitmap to act as a write-ahead log.
	f.storage.OpWriter = f.file
	f.rowCache = &SimpleCache{make(map[uint64]*Row)}

	return nil

}

// openCache initializes the cache from row ids persisted to disk.
func (f *Fragment) openCache() error {
	// Determine cache type from field name.
	switch f.CacheType {
	case CacheTypeRanked:
		f.cache = NewRankCache(f.CacheSize)
	case CacheTypeLRU:
		f.cache = NewLRUCache(f.CacheSize)
	case CacheTypeNone:
		f.cache = NewNopCache()
		return nil
	default:
		return ErrInvalidCacheType
	}

	// Read cache data from disk.
	path := f.cachePath()
	buf, err := ioutil.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("open cache: %s", err)
	}

	// Unmarshal cache data.
	var pb internal.Cache
	if err := proto.Unmarshal(buf, &pb); err != nil {
		f.Logger.Printf("error unmarshaling cache data, skipping: path=%s, err=%s", path, err)
		return nil
	}

	// Read in all rows by ID.
	// This will cause them to be added to the cache.
	for _, id := range pb.IDs {
		n := f.storage.CountRange(id*SliceWidth, (id+1)*SliceWidth)
		f.cache.BulkAdd(id, n)
	}
	f.cache.Invalidate()

	return nil
}

// Close flushes the underlying storage, closes the file and unlocks it.
func (f *Fragment) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.close()
}

func (f *Fragment) close() error {
	// Flush cache if closing gracefully.
	if err := f.flushCache(); err != nil {
		f.Logger.Printf("fragment: error flushing cache on close: err=%s, path=%s", err, f.path)
		return errors.Wrap(err, "flushing cache")
	}

	// Close underlying storage.
	if err := f.closeStorage(); err != nil {
		f.Logger.Printf("fragment: error closing storage: err=%s, path=%s", err, f.path)
		return errors.Wrap(err, "closing storage")
	}

	// Remove checksums.
	f.checksums = nil

	return nil
}

func (f *Fragment) closeStorage() error {
	// Clear the storage bitmap so it doesn't access the closed mmap.

	//f.storage = roaring.NewBitmap()

	// Unmap the file.
	if f.storageData != nil {
		if err := syscall.Munmap(f.storageData); err != nil {
			return fmt.Errorf("munmap: %s", err)
		}
		f.storageData = nil
	}

	// Flush file, unlock & close.
	if f.file != nil {
		if err := f.file.Sync(); err != nil {
			return fmt.Errorf("sync: %s", err)
		}
		if err := syscall.Flock(int(f.file.Fd()), syscall.LOCK_UN); err != nil {
			return fmt.Errorf("unlock: %s", err)
		}
		if err := f.file.Close(); err != nil {
			return fmt.Errorf("close file: %s", err)
		}
	}

	return nil
}

// row returns a row by ID.
func (f *Fragment) row(rowID uint64) *Row {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.unprotectedRow(rowID, true, true)
}

func (f *Fragment) unprotectedRow(rowID uint64, checkRowCache bool, updateRowCache bool) *Row {
	if checkRowCache {
		r, ok := f.rowCache.Fetch(rowID)
		if ok && r != nil {
			return r
		}
	}

	// Only use a subset of the containers.
	// NOTE: The start & end ranges must be divisible by
	data := f.storage.OffsetRange(f.slice*SliceWidth, rowID*SliceWidth, (rowID+1)*SliceWidth)

	// Reference bitmap subrange in storage.
	// We Clone() data because otherwise row will contains pointers to containers in storage.
	// This causes unexpected results when we cache the row and try to use it later.
	row := &Row{
		segments: []RowSegment{{
			data:     *data.Clone(),
			slice:    f.slice,
			writable: false,
		}},
	}
	row.InvalidateCount()

	if updateRowCache {
		f.rowCache.Add(rowID, row)
	}

	return row
}

// setBit sets a bit for a given column & row within the fragment.
// This updates both the on-disk storage and the in-cache bitmap.
func (f *Fragment) setBit(rowID, columnID uint64) (changed bool, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.unprotectedSetBit(rowID, columnID)
}

func (f *Fragment) unprotectedSetBit(rowID, columnID uint64) (changed bool, err error) {
	changed = false
	// Determine the position of the bit in the storage.
	pos, err := f.pos(rowID, columnID)
	if err != nil {
		return false, errors.Wrap(err, "getting bit pos")
	}

	// Write to storage.
	if changed, err = f.storage.Add(pos); err != nil {
		return false, errors.Wrap(err, "writing")
	}

	// Don't update the cache if nothing changed.
	if !changed {
		return changed, nil
	}

	// Invalidate block checksum.
	delete(f.checksums, int(rowID/HashBlockSize))

	// Increment number of operations until snapshot is required.
	if err := f.incrementOpN(); err != nil {
		return false, errors.Wrap(err, "incrementing")
	}

	// Get the row from row cache or fragment.storage.
	row := f.unprotectedRow(rowID, true, true)
	row.SetBit(columnID)

	// Update the cache.
	f.cache.Add(rowID, row.Count())

	f.stats.Count("setBit", 1, 0.001)

	// Update row count if they have increased.
	if rowID > f.maxRowID {
		f.maxRowID = rowID
		f.stats.Gauge("rows", float64(f.maxRowID), 1.0)
	}

	return changed, nil
}

// clearBit clears a bit for a given column & row within the fragment.
// This updates both the on-disk storage and the in-cache bitmap.
func (f *Fragment) clearBit(rowID, columnID uint64) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.unprotectedClearBit(rowID, columnID)
}

func (f *Fragment) unprotectedClearBit(rowID, columnID uint64) (changed bool, err error) {
	changed = false
	// Determine the position of the bit in the storage.
	pos, err := f.pos(rowID, columnID)
	if err != nil {
		return false, errors.Wrap(err, "getting bit pos")
	}

	// Write to storage.
	if changed, err = f.storage.Remove(pos); err != nil {
		return false, errors.Wrap(err, "writing")
	}

	// Don't update the cache if nothing changed.
	if !changed {
		return changed, nil
	}

	// Invalidate block checksum.
	delete(f.checksums, int(rowID/HashBlockSize))

	// Increment number of operations until snapshot is required.
	if err := f.incrementOpN(); err != nil {
		return false, errors.Wrap(err, "incrementing")
	}

	// Get the row from cache or fragment.storage.
	row := f.unprotectedRow(rowID, true, true)
	row.ClearBit(columnID)

	// Update the cache.
	f.cache.Add(rowID, row.Count())

	f.stats.Count("clearBit", 1, 1.0)

	return changed, nil
}

func (f *Fragment) bit(rowID, columnID uint64) (bool, error) {
	pos, err := f.pos(rowID, columnID)
	if err != nil {
		return false, err
	}
	return f.storage.Contains(pos), nil
}

// value uses a column of bits to read a multi-bit value.
func (f *Fragment) value(columnID uint64, bitDepth uint) (value uint64, exists bool, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// If existence bit is unset then ignore remaining bits.
	if v, err := f.bit(uint64(bitDepth), columnID); err != nil {
		return 0, false, errors.Wrap(err, "getting existence bit")
	} else if !v {
		return 0, false, nil
	}

	// Compute other bits into a value.
	for i := uint(0); i < bitDepth; i++ {
		if v, err := f.bit(uint64(i), columnID); err != nil {
			return 0, false, errors.Wrapf(err, "getting value bit %d", i)
		} else if v {
			value |= (1 << i)
		}
	}

	return value, true, nil
}

// setValue uses a column of bits to set a multi-bit value.
func (f *Fragment) setValue(columnID uint64, bitDepth uint, value uint64) (changed bool, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	for i := uint(0); i < bitDepth; i++ {
		if value&(1<<i) != 0 {
			if c, err := f.unprotectedSetBit(uint64(i), columnID); err != nil {
				return changed, err
			} else if c {
				changed = true
			}
		} else {
			if c, err := f.unprotectedClearBit(uint64(i), columnID); err != nil {
				return changed, err
			} else if c {
				changed = true
			}
		}
	}

	// Mark value as set.
	if c, err := f.unprotectedSetBit(uint64(bitDepth), columnID); err != nil {
		return changed, errors.Wrap(err, "marking not-null")
	} else if c {
		changed = true
	}

	return changed, nil
}

// importSetValue is a more efficient SetValue just for imports.
func (f *Fragment) importSetValue(columnID uint64, bitDepth uint, value uint64) (changed bool, err error) {

	for i := uint(0); i < bitDepth; i++ {
		if value&(1<<i) != 0 {
			bit, err := f.pos(uint64(i), columnID)
			if err != nil {
				return changed, errors.Wrap(err, "getting set pos")
			}
			if c, err := f.storage.Add(bit); err != nil {
				return changed, errors.Wrap(err, "adding")
			} else if c {
				changed = true
			}
		} else {
			bit, err := f.pos(uint64(i), columnID)
			if err != nil {
				return changed, errors.Wrap(err, "getting clear pos")
			}
			if c, err := f.storage.Remove(bit); err != nil {
				return changed, errors.Wrap(err, "removing")
			} else if c {
				changed = true
			}
		}
	}

	// Mark value as set.
	p, err := f.pos(uint64(bitDepth), columnID)
	if err != nil {
		return changed, errors.Wrap(err, "marking not-null")
	}
	if c, err := f.storage.Add(p); err != nil {
		return changed, errors.Wrap(err, "adding to storage")
	} else if c {
		changed = true
	}

	return changed, nil
}

// sum returns the sum of a given bsiGroup as well as the number of columns involved.
// A bitmap can be passed in to optionally filter the computed columns.
func (f *Fragment) sum(filter *Row, bitDepth uint) (sum, count uint64, err error) {
	// Compute count based on the existence row.
	row := f.row(uint64(bitDepth))
	if filter != nil {
		count = row.IntersectionCount(filter)
	} else {
		count = row.Count()
	}

	// Compute the sum based on the bit count of each row multiplied by the
	// place value of each row. For example, 10 bits in the 1's place plus
	// 4 bits in the 2's place plus 3 bits in the 4's place equals a total
	// sum of 30:
	//
	//   10*(2^0) + 4*(2^1) + 3*(2^2) = 30
	//
	for i := uint(0); i < bitDepth; i++ {
		row := f.row(uint64(i))
		cnt := uint64(0)
		if filter != nil {
			cnt = row.IntersectionCount(filter)
		} else {
			cnt = row.Count()
		}
		sum += (1 << i) * cnt
	}

	return sum, count, nil
}

// min returns the min of a given bsiGroup as well as the number of columns involved.
// A bitmap can be passed in to optionally filter the computed columns.
func (f *Fragment) min(filter *Row, bitDepth uint) (min, count uint64, err error) {

	consider := f.row(uint64(bitDepth))
	if filter != nil {
		consider = consider.Intersect(filter)
	}

	// If there are no columns to consider, return early.
	if consider.Count() == 0 {
		return 0, 0, nil
	}

	for i := bitDepth; i > uint(0); i-- {
		ii := i - 1 // allow for uint range: (bitDepth-1) to 0
		row := f.row(uint64(ii))

		x := consider.Difference(row)
		count = x.Count()
		if count > 0 {
			consider = x
		} else {
			min += (1 << ii)
			if ii == 0 {
				count = consider.Count()
			}
		}
	}

	return min, count, nil
}

// max returns the max of a given bsiGroup as well as the number of columns involved.
// A bitmap can be passed in to optionally filter the computed columns.
func (f *Fragment) max(filter *Row, bitDepth uint) (max, count uint64, err error) {

	consider := f.row(uint64(bitDepth))
	if filter != nil {
		consider = consider.Intersect(filter)
	}

	// If there are no columns to consider, return early.
	if consider.Count() == 0 {
		return 0, 0, nil
	}

	for i := bitDepth; i > uint(0); i-- {
		ii := i - 1 // allow for uint range: (bitDepth-1) to 0
		row := f.row(uint64(ii))

		x := row.Intersect(consider)
		count = x.Count()
		if count > 0 {
			max += (1 << ii)
			consider = x
		} else if ii == 0 {
			count = consider.Count()
		}
	}

	return max, count, nil
}

// rangeOp returns bitmaps with a bsiGroup value encoding matching the predicate.
func (f *Fragment) rangeOp(op pql.Token, bitDepth uint, predicate uint64) (*Row, error) {
	switch op {
	case pql.EQ:
		return f.rangeEQ(bitDepth, predicate)
	case pql.NEQ:
		return f.rangeNEQ(bitDepth, predicate)
	case pql.LT, pql.LTE:
		return f.rangeLT(bitDepth, predicate, op == pql.LTE)
	case pql.GT, pql.GTE:
		return f.rangeGT(bitDepth, predicate, op == pql.GTE)
	default:
		return nil, ErrInvalidRangeOperation
	}
}

func (f *Fragment) rangeEQ(bitDepth uint, predicate uint64) (*Row, error) {
	// Start with set of columns with values set.
	b := f.row(uint64(bitDepth))

	// Filter any bits that don't match the current bit value.
	for i := int(bitDepth - 1); i >= 0; i-- {
		row := f.row(uint64(i))
		bit := (predicate >> uint(i)) & 1

		if bit == 1 {
			b = b.Intersect(row)
		} else {
			b = b.Difference(row)
		}
	}

	return b, nil
}

func (f *Fragment) rangeNEQ(bitDepth uint, predicate uint64) (*Row, error) {
	// Start with set of columns with values set.
	b := f.row(uint64(bitDepth))

	// Get the equal bitmap.
	eq, err := f.rangeEQ(bitDepth, predicate)
	if err != nil {
		return nil, err
	}

	// Not-null minus the equal bitmap.
	b = b.Difference(eq)

	return b, nil
}

func (f *Fragment) rangeLT(bitDepth uint, predicate uint64, allowEquality bool) (*Row, error) {
	keep := NewRow()

	// Start with set of columns with values set.
	b := f.row(uint64(bitDepth))

	// Filter any bits that don't match the current bit value.
	leadingZeros := true
	for i := int(bitDepth - 1); i >= 0; i-- {
		row := f.row(uint64(i))
		bit := (predicate >> uint(i)) & 1

		// Remove any columns with higher bits set.
		if leadingZeros {
			if bit == 0 {
				b = b.Difference(row)
				continue
			} else {
				leadingZeros = false
			}
		}

		// Handle last bit differently.
		// If bit is zero then return only already kept columns.
		// If bit is one then remove any one columns.
		if i == 0 && !allowEquality {
			if bit == 0 {
				return keep, nil
			}
			return b.Difference(row.Difference(keep)), nil
		}

		// If bit is zero then remove all set columns not in excluded bitmap.
		if bit == 0 {
			b = b.Difference(row.Difference(keep))
			continue
		}

		// If bit is set then add columns for set bits to exclude.
		// Don't bother to compute this on the final iteration.
		if i > 0 {
			keep = keep.Union(b.Difference(row))
		}
	}

	return b, nil
}

func (f *Fragment) rangeGT(bitDepth uint, predicate uint64, allowEquality bool) (*Row, error) {
	b := f.row(uint64(bitDepth))
	keep := NewRow()

	// Filter any bits that don't match the current bit value.
	for i := int(bitDepth - 1); i >= 0; i-- {
		row := f.row(uint64(i))
		bit := (predicate >> uint(i)) & 1

		// Handle last bit differently.
		// If bit is one then return only already kept columns.
		// If bit is zero then remove any unset columns.
		if i == 0 && !allowEquality {
			if bit == 1 {
				return keep, nil
			}
			return b.Difference(b.Difference(row).Difference(keep)), nil
		}

		// If bit is set then remove all unset columns not already kept.
		if bit == 1 {
			b = b.Difference(b.Difference(row).Difference(keep))
			continue
		}

		// If bit is unset then add columns with set bit to keep.
		// Don't bother to compute this on the final iteration.
		if i > 0 {
			keep = keep.Union(b.Intersect(row))
		}
	}

	return b, nil
}

// notNull returns the not-null row (stored at bitDepth).
func (f *Fragment) notNull(bitDepth uint) (*Row, error) {
	return f.row(uint64(bitDepth)), nil
}

// rangeBetween returns bitmaps with a bsiGroup value encoding matching any value between predicateMin and predicateMax.
func (f *Fragment) rangeBetween(bitDepth uint, predicateMin, predicateMax uint64) (*Row, error) {
	b := f.row(uint64(bitDepth))
	keep1 := NewRow() // GTE
	keep2 := NewRow() // LTE

	// Filter any bits that don't match the current bit value.
	for i := int(bitDepth - 1); i >= 0; i-- {
		row := f.row(uint64(i))
		bit1 := (predicateMin >> uint(i)) & 1
		bit2 := (predicateMax >> uint(i)) & 1

		// GTE predicateMin
		// If bit is set then remove all unset columns not already kept.
		if bit1 == 1 {
			b = b.Difference(b.Difference(row).Difference(keep1))
		} else {
			// If bit is unset then add columns with set bit to keep.
			// Don't bother to compute this on the final iteration.
			if i > 0 {
				keep1 = keep1.Union(b.Intersect(row))
			}
		}

		// LTE predicateMin
		// If bit is zero then remove all set bits not in excluded bitmap.
		if bit2 == 0 {
			b = b.Difference(row.Difference(keep2))
		} else {
			// If bit is set then add columns for set bits to exclude.
			// Don't bother to compute this on the final iteration.
			if i > 0 {
				keep2 = keep2.Union(b.Difference(row))
			}
		}
	}

	return b, nil
}

// pos translates the row ID and column ID into a position in the storage bitmap.
func (f *Fragment) pos(rowID, columnID uint64) (uint64, error) {
	// Return an error if the column ID is out of the range of the fragment's slice.
	minColumnID := f.slice * SliceWidth
	if columnID < minColumnID || columnID >= minColumnID+SliceWidth {
		return 0, errors.New("column out of bounds")
	}
	return pos(rowID, columnID), nil
}

// forEachBit executes fn for every bit set in the fragment.
// Errors returned from fn are passed through.
func (f *Fragment) forEachBit(fn func(rowID, columnID uint64) error) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	var err error
	f.storage.ForEach(func(i uint64) {
		// Skip if an error has already occurred.
		if err != nil {
			return
		}

		// Invoke caller's function.
		err = fn(i/SliceWidth, (f.slice*SliceWidth)+(i%SliceWidth))
	})
	return err
}

// top returns the top rows from the fragment.
// If opt.Src is specified then only rows which intersect src are returned.
// If opt.FilterValues exist then the row attribute specified by field is matched.
func (f *Fragment) top(opt TopOptions) ([]Pair, error) {
	// Retrieve pairs. If no row ids specified then return from cache.
	pairs := f.topBitmapPairs(opt.RowIDs)

	// If row ids are provided, we don't want to truncate the result set
	if len(opt.RowIDs) > 0 {
		opt.N = 0
	}

	// Create a fast lookup of filter values.
	var filters map[interface{}]struct{}
	if opt.FilterName != "" && len(opt.FilterValues) > 0 {
		filters = make(map[interface{}]struct{})
		for _, v := range opt.FilterValues {
			filters[v] = struct{}{}
		}
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
	results := &PairHeap{}
	for _, pair := range pairs {
		rowID, cnt := pair.ID, pair.Count

		// Ignore empty rows.
		if cnt <= 0 {
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

		// Apply filter, if set.
		if filters != nil {
			attr, err := f.RowAttrStore.Attrs(rowID)
			if err != nil {
				return nil, errors.Wrap(err, "getting attrs")
			} else if attr == nil {
				continue
			} else if attrValue := attr[opt.FilterName]; attrValue == nil {
				continue
			} else if _, ok := filters[attrValue]; !ok {
				continue
			}
		}

		// The initial n pairs should simply be added to the results.
		if opt.N == 0 || results.Len() < opt.N {
			// Calculate count and append.
			count := cnt
			if opt.Src != nil {
				count = opt.Src.IntersectionCount(f.row(rowID))
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
		count := opt.Src.IntersectionCount(f.row(rowID))
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

func (f *Fragment) topBitmapPairs(rowIDs []uint64) []BitmapPair {
	// Don't retrieve from storage if CacheTypeNone.
	if f.CacheType == CacheTypeNone {
		return f.cache.Top()
	}
	// If no specific rows are requested, retrieve top rows.
	if len(rowIDs) == 0 {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.cache.Invalidate()
		return f.cache.Top()
	}

	// Otherwise retrieve specific rows.
	pairs := make([]BitmapPair, 0, len(rowIDs))
	for _, rowID := range rowIDs {
		// Look up cache first, if available.
		if n := f.cache.Get(rowID); n > 0 {
			pairs = append(pairs, BitmapPair{
				ID:    rowID,
				Count: n,
			})
			continue
		}

		row := f.row(rowID)
		if row.Count() > 0 {
			// Otherwise load from storage.
			pairs = append(pairs, BitmapPair{
				ID:    rowID,
				Count: row.Count(),
			})
		}
	}
	sort.Sort(BitmapPairs(pairs))
	return pairs
}

// TopOptions represents options passed into the Top() function.
type TopOptions struct {
	// Number of rows to return.
	N int

	// Bitmap to intersect with.
	Src *Row

	// Specific rows to filter against.
	RowIDs       []uint64
	MinThreshold uint64

	// Filter field name & values.
	FilterName        string
	FilterValues      []interface{}
	TanimotoThreshold uint64
}

// Checksum returns a checksum for the entire fragment.
// If two fragments have the same checksum then they have the same data.
func (f *Fragment) Checksum() []byte {
	h := xxhash.New()
	for _, block := range f.Blocks() {
		h.Write(block.Checksum)
	}
	return h.Sum(nil)
}

// InvalidateChecksums clears all cached block checksums.
func (f *Fragment) InvalidateChecksums() {
	f.mu.Lock()
	f.checksums = make(map[int][]byte)
	f.mu.Unlock()
}

// Blocks returns info for all blocks containing data.
func (f *Fragment) Blocks() []FragmentBlock {
	f.mu.Lock()
	defer f.mu.Unlock()

	var a []FragmentBlock

	// Initialize the iterator.
	itr := f.storage.Iterator()
	itr.Seek(0)

	// Initialize block hasher.
	h := newBlockHasher()

	// Iterate over each value in the fragment.
	v, eof := itr.Next()
	if eof {
		return nil
	}
	blockID := int(v / (HashBlockSize * SliceWidth))
	for {
		// Check for multiple block checksums in a row.
		if n := f.readContiguousChecksums(&a, blockID); n > 0 {
			itr.Seek(uint64(blockID+n) * HashBlockSize * SliceWidth)
			v, eof = itr.Next()
			if eof {
				break
			}
			blockID = int(v / (HashBlockSize * SliceWidth))
			continue
		}

		// Reset hasher.
		h.blockID = blockID
		h.Reset()

		// Read all values for the block.
		for ; ; v, eof = itr.Next() {
			// Once we hit the next block, save the value for the next iteration.
			blockID = int(v / (HashBlockSize * SliceWidth))
			if blockID != h.blockID || eof {
				break
			}

			h.WriteValue(v)
		}

		// Cache checksum.
		chksum := h.Sum()
		f.checksums[h.blockID] = chksum

		// Append block.
		a = append(a, FragmentBlock{
			ID:       h.blockID,
			Checksum: chksum,
		})

		// Exit if we're at the end.
		if eof {
			break
		}
	}

	return a
}

// readContiguousChecksums appends multiple checksums in a row and returns the count added.
func (f *Fragment) readContiguousChecksums(a *[]FragmentBlock, blockID int) (n int) {
	for i := 0; ; i++ {
		chksum := f.checksums[blockID+i]
		if chksum == nil {
			return i
		}

		*a = append(*a, FragmentBlock{
			ID:       blockID + i,
			Checksum: chksum,
		})
	}
}

// blockData returns bits in a block as row & column ID pairs.
func (f *Fragment) blockData(id int) (rowIDs, columnIDs []uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.storage.ForEachRange(uint64(id)*HashBlockSize*SliceWidth, (uint64(id)+1)*HashBlockSize*SliceWidth, func(i uint64) {
		rowIDs = append(rowIDs, i/SliceWidth)
		columnIDs = append(columnIDs, i%SliceWidth)
	})
	return
}

// mergeBlock compares the block's bits and computes a diff with another set of block bits.
// The state of a bit is determined by consensus from all blocks being considered.
//
// For example, if 3 blocks are compared and two have a set bit and one has a
// cleared bit then the bit is considered cleared. The function returns the
// diff per incoming block so that all can be in sync.
func (f *Fragment) mergeBlock(id int, data []pairSet) (sets, clears []pairSet, err error) {
	// Ensure that all pair sets are of equal length.
	for i := range data {
		if len(data[i].rowIDs) != len(data[i].columnIDs) {
			return nil, nil, fmt.Errorf("pair set mismatch(idx=%d): %d != %d", i, len(data[i].rowIDs), len(data[i].columnIDs))
		}
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	// Track sets and clears for all blocks (including local).
	sets = make([]pairSet, len(data)+1)
	clears = make([]pairSet, len(data)+1)

	// Limit upper row/column pair.
	maxRowID := uint64(id+1) * HashBlockSize
	maxColumnID := uint64(SliceWidth)

	// Create buffered iterator for local block.
	itrs := make([]*BufIterator, 1, len(data)+1)
	itrs[0] = NewBufIterator(
		NewLimitIterator(
			NewRoaringIterator(f.storage.Iterator()), maxRowID, maxColumnID,
		),
	)

	// Append buffered iterators for each incoming block.
	for i := range data {
		var itr Iterator = NewSliceIterator(data[i].rowIDs, data[i].columnIDs)
		itr = NewLimitIterator(itr, maxRowID, maxColumnID)
		itrs = append(itrs, NewBufIterator(itr))
	}

	// Seek to initial pair.
	for _, itr := range itrs {
		itr.Seek(uint64(id)*HashBlockSize, 0)
	}

	// Determine the number of blocks needed to meet consensus.
	// If there is an even split then a set is used.
	majorityN := (len(itrs) + 1) / 2

	// Iterate over all values in all iterators to determine differences.
	values := make([]bool, len(itrs))
	for {
		var min struct {
			rowID    uint64
			columnID uint64
		}

		// Find the lowest pair.
		var hasData bool
		for _, itr := range itrs {
			bid, pid, eof := itr.Peek()
			if eof { // no more data
				continue
			} else if !hasData { // first pair
				min.rowID, min.columnID, hasData = bid, pid, true
			} else if bid < min.rowID || (bid == min.rowID && pid < min.columnID) { // lower pair
				min.rowID, min.columnID = bid, pid
			}
		}

		// If all iterators are EOF then exit.
		if !hasData {
			break
		}

		// Determine consensus of point.
		var setN int
		for i, itr := range itrs {
			bid, pid, eof := itr.Next()

			values[i] = !eof && bid == min.rowID && pid == min.columnID
			if values[i] {
				setN++ // set
			} else {
				itr.Unread() // clear
			}
		}

		// Determine consensus value.
		newValue := setN >= majorityN

		// Add a diff for any node with a different value.
		for i := range itrs {
			// Value matches, ignore.
			if values[i] == newValue {
				continue
			}

			// Append to either the set or clear diff.
			if newValue {
				sets[i].rowIDs = append(sets[i].rowIDs, min.rowID)
				sets[i].columnIDs = append(sets[i].columnIDs, min.columnID)
			} else {
				clears[i].rowIDs = append(sets[i].rowIDs, min.rowID)
				clears[i].columnIDs = append(sets[i].columnIDs, min.columnID)
			}
		}
	}

	// Set local bits.
	for i := range sets[0].columnIDs {
		if _, err := f.unprotectedSetBit(sets[0].rowIDs[i], (f.slice*SliceWidth)+sets[0].columnIDs[i]); err != nil {
			return nil, nil, errors.Wrap(err, "setting")
		}
	}

	// Clear local bits.
	for i := range clears[0].columnIDs {
		if _, err := f.unprotectedClearBit(clears[0].rowIDs[i], (f.slice*SliceWidth)+clears[0].columnIDs[i]); err != nil {
			return nil, nil, errors.Wrap(err, "clearing")
		}
	}

	return sets[1:], clears[1:], nil
}

// bulkImport bulk imports a set of bits and then snapshots the storage.
// This does not affect the fragment's cache.
func (f *Fragment) bulkImport(rowIDs, columnIDs []uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	// Verify that there are an equal number of row ids and column ids.
	if len(rowIDs) != len(columnIDs) {
		return fmt.Errorf("mismatch of row/column len: %d != %d", len(rowIDs), len(columnIDs))
	}

	// Disconnect op writer so we don't append updates.
	f.storage.OpWriter = nil

	// Process every bit.
	// If an error occurs then reopen the storage.
	lastID := uint64(0)
	if err := func() error {
		set := make(map[uint64]struct{})
		for i := range rowIDs {
			rowID, columnID := rowIDs[i], columnIDs[i]

			// Determine the position of the bit in the storage.
			pos, err := f.pos(rowID, columnID)
			if err != nil {
				return errors.Wrap(err, "getting bit pos")
			}

			// Write to storage.
			_, err = f.storage.Add(pos)
			if err != nil {
				return errors.Wrap(err, "writing")
			}
			// Reduce the StatsD rate for high volume stats
			f.stats.Count("ImportBit", 1, 0.0001)
			// import optimization to avoid linear foreach calls
			// slight risk of concurrent cache counter being off but
			// no real danger
			if i == 0 || rowID != lastID {
				lastID = rowID
				set[rowID] = struct{}{}
			}

			// Invalidate block checksum.
			delete(f.checksums, int(rowID/HashBlockSize))
		}

		// Update cache counts for all rows.
		for rowID := range set {
			// Import should ALWAYS have row() load a new row from fragment.storage
			// because the row that's in rowCache hasn't been updated with
			// this import's data.
			f.cache.BulkAdd(rowID, f.unprotectedRow(rowID, false, false).Count())
		}

		f.cache.Invalidate()
		return nil
	}(); err != nil {
		_ = f.closeStorage()
		_ = f.openStorage()
		return err
	}

	// Write the storage to disk and reload.
	if err := f.snapshot(); err != nil {
		return errors.Wrap(err, "snapshotting")
	}

	return nil
}

// importValue bulk imports a set of range-encoded values.
func (f *Fragment) importValue(columnIDs, values []uint64, bitDepth uint) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	// Verify that there are an equal number of column ids and values.
	if len(columnIDs) != len(values) {
		return fmt.Errorf("mismatch of column/value len: %d != %d", len(columnIDs), len(values))
	}

	f.storage.OpWriter = nil
	// Process every value.
	// If an error occurs then reopen the storage.
	if err := func() error {
		for i := range columnIDs {
			columnID, value := columnIDs[i], values[i]

			_, err := f.importSetValue(columnID, bitDepth, value)
			if err != nil {
				return errors.Wrap(err, "setting")
			}
		}
		return nil
	}(); err != nil {
		_ = f.closeStorage()
		_ = f.openStorage()
		return err
	}
	if err := f.snapshot(); err != nil {
		return errors.Wrap(err, "snapshotting")
	}
	return nil
}

// incrementOpN increase the operation count by one.
// If the count exceeds the maximum allowed then a snapshot is performed.
func (f *Fragment) incrementOpN() error {
	f.opN++
	if f.opN <= f.MaxOpN {
		return nil
	}

	if err := f.snapshot(); err != nil {
		return fmt.Errorf("snapshot: %s", err)
	}
	return nil
}

// Snapshot writes the storage bitmap to disk and reopens it.
func (f *Fragment) Snapshot() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.snapshot()
}
func track(start time.Time, message string, stats StatsClient, logger Logger) {
	elapsed := time.Since(start)
	logger.Printf("%s took %s", message, elapsed)
	stats.Histogram("snapshot", elapsed.Seconds(), 1.0)
}

func (f *Fragment) snapshot() error {
	f.Logger.Printf("fragment: snapshotting %s/%s/%s/%d", f.index, f.field, f.view, f.slice)
	completeMessage := fmt.Sprintf("fragment: snapshot complete %s/%s/%s/%d", f.index, f.field, f.view, f.slice)
	start := time.Now()
	defer track(start, completeMessage, f.stats, f.Logger)

	// Create a temporary file to snapshot to.
	snapshotPath := f.path + snapshotExt
	file, err := os.Create(snapshotPath)
	if err != nil {
		return fmt.Errorf("create snapshot file: %s", err)
	}
	defer file.Close()

	// Write storage to snapshot.
	bw := bufio.NewWriter(file)
	if _, err := f.storage.WriteTo(bw); err != nil {
		return fmt.Errorf("snapshot write to: %s", err)
	}

	if err := bw.Flush(); err != nil {
		return fmt.Errorf("flush: %s", err)
	}

	// Close current storage.
	if err := f.closeStorage(); err != nil {
		return fmt.Errorf("close storage: %s", err)
	}

	// Move snapshot to data file location.
	if err := os.Rename(snapshotPath, f.path); err != nil {
		return fmt.Errorf("rename snapshot: %s", err)
	}

	// Reopen storage.
	if err := f.openStorage(); err != nil {
		return fmt.Errorf("open storage: %s", err)
	}

	// Reset operation count.
	f.opN = 0

	return nil
}

// RecalculateCache rebuilds the cache regardless of invalidate time delay.
func (f *Fragment) RecalculateCache() {
	f.mu.Lock()
	f.cache.Recalculate()
	f.mu.Unlock()
}

// FlushCache writes the cache data to disk.
func (f *Fragment) FlushCache() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.flushCache()
}

func (f *Fragment) flushCache() error {
	if f.cache == nil {
		return nil
	}

	if f.CacheType == CacheTypeNone {
		return nil
	}

	// Retrieve a list of row ids from the cache.
	ids := f.cache.IDs()

	// Marshal cache data to bytes.
	buf, err := proto.Marshal(&internal.Cache{IDs: ids})
	if err != nil {
		return errors.Wrap(err, "marshalling")
	}

	// Write to disk.
	if err := ioutil.WriteFile(f.cachePath(), buf, 0666); err != nil {
		return errors.Wrap(err, "writing")
	}

	return nil
}

// WriteTo writes the fragment's data to w.
func (f *Fragment) WriteTo(w io.Writer) (n int64, err error) {
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

func (f *Fragment) writeStorageToArchive(tw *tar.Writer) error {
	// Open separate file descriptor to read from.
	file, err := os.Open(f.path)
	if err != nil {
		return errors.Wrap(err, "opening file")
	}
	defer file.Close()

	// Retrieve the current file size under lock so we don't read
	// while an operation is appending to the end.
	var sz int64
	if err := func() error {
		f.mu.Lock()
		defer f.mu.Unlock()

		fi, err := file.Stat()
		if err != nil {
			return errors.Wrap(err, "statting")
		}
		sz = fi.Size()

		return nil
	}(); err != nil {
		return err
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
	if _, err := io.CopyN(tw, file, sz); err != nil {
		return errors.Wrap(err, "copying")
	}
	return nil
}

func (f *Fragment) writeCacheToArchive(tw *tar.Writer) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Read cache into buffer.
	buf, err := ioutil.ReadFile(f.cachePath())
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
func (f *Fragment) ReadFrom(r io.Reader) (n int64, err error) {
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
			if err := f.readStorageFromArchive(tr); err != nil {
				return 0, errors.Wrap(err, "reading storage")
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

func (f *Fragment) readStorageFromArchive(r io.Reader) error {
	// Create a temporary file to copy into.
	path := f.path + copyExt
	file, err := os.Create(path)
	if err != nil {
		return errors.Wrap(err, "creating directory")
	}
	defer file.Close()

	// Copy reader into temporary path.
	if _, err = io.Copy(file, r); err != nil {
		return errors.Wrap(err, "copying")
	}

	// Close current storage.
	if err := f.closeStorage(); err != nil {
		return errors.Wrap(err, "closing")
	}

	// Move snapshot to data file location.
	if err := os.Rename(path, f.path); err != nil {
		return errors.Wrap(err, "renaming")
	}

	// Reopen storage.
	if err := f.openStorage(); err != nil {
		return errors.Wrap(err, "opening")
	}

	return nil
}

func (f *Fragment) readCacheFromArchive(r io.Reader) error {
	// Slurp data from reader and write to disk.
	buf, err := ioutil.ReadAll(r)
	if err != nil {
		return errors.Wrap(err, "reading")
	} else if err := ioutil.WriteFile(f.cachePath(), buf, 0666); err != nil {
		return errors.Wrap(err, "writing")
	}

	// Re-open cache.
	if err := f.openCache(); err != nil {
		return errors.Wrap(err, "opening")
	}

	return nil
}

// FragmentBlock represents info about a subsection of the rows in a block.
// This is used for comparing data in remote blocks for active anti-entropy.
type FragmentBlock struct {
	ID       int    `json:"id"`
	Checksum []byte `json:"checksum"`
}

type blockHasher struct {
	blockID int
	buf     [8]byte
	hash    hash.Hash
}

func newBlockHasher() blockHasher {
	return blockHasher{
		blockID: -1,
		hash:    xxhash.New(),
	}
}
func (h *blockHasher) Reset() {
	h.hash.Reset()
}

func (h *blockHasher) Sum() []byte {
	return h.hash.Sum(nil)[:]
}

func (h *blockHasher) WriteValue(v uint64) {
	binary.BigEndian.PutUint64(h.buf[:], v)
	h.hash.Write(h.buf[:])
}

// FragmentSyncer syncs a local fragment to one on a remote host.
type FragmentSyncer struct {
	Fragment *Fragment

	Node    *Node
	Cluster *Cluster

	Closing <-chan struct{}
}

// isClosing returns true if the closing channel is closed.
func (s *FragmentSyncer) isClosing() bool {
	select {
	case <-s.Closing:
		return true
	default:
		return false
	}
}

// syncFragment compares checksums for the local and remote fragments and
// then merges any blocks which have differences.
func (s *FragmentSyncer) syncFragment() error {
	// Determine replica set.
	nodes := s.Cluster.sliceNodes(s.Fragment.index, s.Fragment.slice)
	if len(nodes) == 1 {
		return nil
	}

	// Create a set of blocks.
	blockSets := make([][]FragmentBlock, 0, len(nodes))
	for _, node := range nodes {
		// Read local blocks.
		if node.ID == s.Node.ID {
			b := s.Fragment.Blocks()
			blockSets = append(blockSets, b)
			continue
		}

		// Retrieve remote blocks.
		blocks, err := s.Cluster.InternalClient.FragmentBlocks(context.Background(), nil, s.Fragment.index, s.Fragment.field, s.Fragment.slice)
		if err != nil && err != ErrFragmentNotFound {
			return errors.Wrap(err, "getting blocks")
		}
		blockSets = append(blockSets, blocks)

		// Verify sync is not prematurely closing.
		if s.isClosing() {
			return nil
		}
	}

	// Iterate over all blocks and find differences.
	checksums := make([][]byte, len(nodes))
	for {
		// Find min block id.
		blockID := -1
		for _, blocks := range blockSets {
			if len(blocks) == 0 {
				continue
			} else if blockID == -1 || blocks[0].ID < blockID {
				blockID = blocks[0].ID
			}
		}

		// Exit loop if no blocks are left.
		if blockID == -1 {
			break
		}

		// Read the checksum for the current block.
		for i, blocks := range blockSets {
			// Clear checksum if the next block for the node doesn't match current ID.
			if len(blocks) == 0 || blocks[0].ID != blockID {
				checksums[i] = nil
				continue
			}

			// Otherwise set checksum and move forward.
			checksums[i] = blocks[0].Checksum
			blockSets[i] = blockSets[i][1:]
		}

		// Ignore if all the blocks on each node match.
		if byteSlicesEqual(checksums) {
			continue
		}
		// Synchronize block.
		if err := s.syncBlock(blockID); err != nil {
			return fmt.Errorf("sync block: id=%d, err=%s", blockID, err)
		}
		s.Fragment.stats.Count("BlockRepair", 1, 1.0)
	}

	return nil
}

// syncBlock sends and receives all rows for a given block.
// Returns an error if any remote hosts are unreachable.
func (s *FragmentSyncer) syncBlock(id int) error {
	f := s.Fragment

	// Read pairs from each remote block.
	var uris []*URI
	var pairSets []pairSet
	for _, node := range s.Cluster.sliceNodes(f.index, f.slice) {
		if s.Node.ID == node.ID {
			continue
		}

		// Verify sync is not prematurely closing.
		if s.isClosing() {
			return nil
		}

		uri := &node.URI
		uris = append(uris, uri)

		// Only sync the standard block.
		rowIDs, columnIDs, err := s.Cluster.InternalClient.BlockData(context.Background(), &node.URI, f.index, f.field, f.slice, id)
		if err != nil {
			return errors.Wrap(err, "getting block")
		}

		pairSets = append(pairSets, pairSet{
			columnIDs: columnIDs,
			rowIDs:    rowIDs,
		})
	}

	// Verify sync is not prematurely closing.
	if s.isClosing() {
		return nil
	}

	// Merge blocks together.
	sets, clears, err := f.mergeBlock(id, pairSets)
	if err != nil {
		return errors.Wrap(err, "merging")
	}

	// Write updates to remote blocks.
	for i := 0; i < len(uris); i++ {
		set, clear := sets[i], clears[i]
		count := 0

		// Ignore if there are no differences.
		if len(set.columnIDs) == 0 && len(clear.columnIDs) == 0 {
			continue
		}

		// Generate query with sets & clears, and group the requests to not exceed MaxWritesPerRequest.
		total := len(set.columnIDs) + len(clear.columnIDs)
		maxWrites := s.Cluster.MaxWritesPerRequest
		if maxWrites <= 0 {
			maxWrites = 5000
		}
		buffers := make([]bytes.Buffer, int(math.Ceil(float64(total)/float64(maxWrites))))

		// Only sync the standard block.
		for j := 0; j < len(set.columnIDs); j++ {
			fmt.Fprintf(&(buffers[count/maxWrites]), "SetBit(field=%q, row=%d, col=%d)\n", f.field, set.rowIDs[j], (f.slice*SliceWidth)+set.columnIDs[j])
			count++
		}
		for j := 0; j < len(clear.columnIDs); j++ {
			fmt.Fprintf(&(buffers[count/maxWrites]), "ClearBit(field=%q, row=%d, col=%d)\n", f.field, clear.rowIDs[j], (f.slice*SliceWidth)+clear.columnIDs[j])
			count++
		}

		// Iterate over the buffers.
		for k := 0; k < len(buffers); k++ {
			// Verify sync is not prematurely closing.
			if s.isClosing() {
				return nil
			}

			// Execute query.
			queryRequest := &internal.QueryRequest{
				Query:  buffers[k].String(),
				Remote: true,
			}
			_, err := s.Cluster.InternalClient.QueryNode(context.Background(), uris[i], f.index, queryRequest)
			if err != nil {
				return errors.Wrap(err, "executing")
			}
		}
	}

	return nil
}

func madvise(b []byte, advice int) (err error) {
	_, _, e1 := syscall.Syscall(syscall.SYS_MADVISE, uintptr(unsafe.Pointer(&b[0])), uintptr(len(b)), uintptr(advice))
	if e1 != 0 {
		err = e1
	}
	return
}

// pairSet is a list of equal length row and column id lists.
type pairSet struct {
	rowIDs    []uint64
	columnIDs []uint64
}

// byteSlicesEqual returns true if all slices are equal.
func byteSlicesEqual(a [][]byte) bool {
	if len(a) == 0 {
		return true
	}

	for _, v := range a[1:] {
		if !bytes.Equal(a[0], v) {
			return false
		}
	}
	return true
}

// pos returns the row position of a row/column pair.
func pos(rowID, columnID uint64) uint64 {
	return (rowID * SliceWidth) + (columnID % SliceWidth)
}
