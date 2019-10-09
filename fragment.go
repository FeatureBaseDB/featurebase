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
	"math"
	"os"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"github.com/cespare/xxhash"
	"github.com/gogo/protobuf/proto"
	"github.com/pilosa/pilosa/v2/internal"
	"github.com/pilosa/pilosa/v2/logger"
	"github.com/pilosa/pilosa/v2/pql"
	"github.com/pilosa/pilosa/v2/roaring"
	"github.com/pilosa/pilosa/v2/shardwidth"
	"github.com/pilosa/pilosa/v2/stats"
	"github.com/pilosa/pilosa/v2/syswrap"
	"github.com/pilosa/pilosa/v2/tracing"
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

	// snapshotExt is the file extension used for an in-process snapshot.
	snapshotExt = ".snapshotting"

	// copyExt is the file extension used for the temp file used while copying.
	copyExt = ".copying"

	// cacheExt is the file extension for persisted cache ids.
	cacheExt = ".cache"

	// tempExt is the file extension for temporary files.
	tempExt = ".temp"

	// HashBlockSize is the number of rows in a merkle hash block.
	HashBlockSize = 100

	// defaultFragmentMaxOpN is the default value for Fragment.MaxOpN.
	defaultFragmentMaxOpN = 10000

	// Row ids used for boolean fields.
	falseRowID = uint64(0)
	trueRowID  = uint64(1)

	// BSI bits used to check existence & sign.
	bsiExistsBit = 0
	bsiSignBit   = 1
	bsiOffsetBit = 2

	// Roaring bitmap flags.
	roaringFlagBSIv2 = 0x01 // indicates version using low bit for existence
)

// fragment represents the intersection of a field and shard in an index.
type fragment struct {
	mu sync.RWMutex

	// Composite identifiers
	index string
	field string
	view  string
	shard uint64

	// File-backed storage
	path               string
	flags              byte // user-defined flags passed to roaring
	file               *os.File
	storage            *roaring.Bitmap
	storageData        []byte
	totalOpN           int64 // total opN values
	totalOps           int64 // total ops (across all snapshots)
	opN                int   // number of ops since snapshot (may be approximate for imports)
	ops                int   // number of higher-level operations, as opposed to bit changes
	snapshotsRequested int   // number of times we've requested a snapshot
	snapshotsTaken     int   // number of actual snapshot operations
	snapshotting       bool  // set to true when requesting a snapshot, set to false after snapshot completes
	snapshotCond       sync.Cond
	snapshotDelays     int
	snapshotDelayTime  time.Duration

	// Cache for row counts.
	CacheType string // passed in by field
	cache     cache
	CacheSize uint32

	// Stats reporting.
	maxRowID uint64

	// Cache containing full rows (not just counts).
	rowCache bitmapCache

	// Cached checksums for each block.
	checksums map[int][]byte

	// Number of operations performed before performing a snapshot.
	// This limits the size of fragments on the heap and flushes them to disk
	// so that they can be mmapped and heap utilization can be kept low.
	MaxOpN int

	// Logger used for out-of-band log entries.
	Logger logger.Logger

	// Row attribute storage.
	// This is set by the parent field unless overridden for testing.
	RowAttrStore AttrStore

	// mutexVector is used for mutex field types. It's checked for an
	// existing value (to clear) prior to setting a new value.
	mutexVector vector

	stats stats.StatsClient

	snapshotQueue chan *fragment
}

// newFragment returns a new instance of Fragment.
func newFragment(path, index, field, view string, shard uint64, flags byte) *fragment {
	f := &fragment{
		path:      path,
		index:     index,
		field:     field,
		view:      view,
		shard:     shard,
		flags:     flags,
		CacheType: DefaultCacheType,
		CacheSize: DefaultCacheSize,

		Logger: logger.NopLogger,
		MaxOpN: defaultFragmentMaxOpN,

		stats: stats.NopStatsClient,
	}
	f.snapshotCond = sync.Cond{L: &f.mu}
	return f
}

// cachePath returns the path to the fragment's cache data.
func (f *fragment) cachePath() string { return f.path + cacheExt }

// newSnapshotQueue makes a new snapshot queue, of depth N, and spawns a
// goroutine for it.
func newSnapshotQueue(n int, w int, l logger.Logger) chan *fragment {
	ch := make(chan *fragment, n)
	for i := 0; i < w; i++ {
		go snapshotQueueWorker(ch, l)
	}
	return ch
}

func snapshotQueueWorker(snapshotQueue chan *fragment, l logger.Logger) {
	for f := range snapshotQueue {
		err := f.protectedSnapshot(true)
		if err != nil {
			l.Printf("snapshot error: %v", err)
		}
		f.snapshotCond.Broadcast()
	}
}

// enqueueSnapshot requests that the fragment be snapshotted at some point
// in the future, if this has not already been requested. Call this only when
// the mutex is held.
func (f *fragment) enqueueSnapshot() {
	f.snapshotsRequested++
	if f.snapshotting {
		return
	}
	f.snapshotting = true
	if f.snapshotQueue != nil {
		select {
		case f.snapshotQueue <- f:
		default:
			before := time.Now()
			// wait forever, but notice that we're waiting
			f.snapshotQueue <- f
			f.snapshotDelays++
			f.snapshotDelayTime += time.Since(before)
			if f.snapshotDelays >= 10 {
				f.Logger.Printf("snapshotting %s: last ten enqueue delays took %v", f.path, f.snapshotDelayTime)
				f.snapshotDelays = 0
				f.snapshotDelayTime = 0
			}
		}
	} else {
		// in testing, for instance, there may be no holder, thus no one
		// to handle these snapshots.
		err := f.snapshot()
		if err != nil {
			f.Logger.Printf("snapshot failed: %v", err)
		}
		f.snapshotting = false
		f.snapshotCond.Broadcast()
	}
}

// Open opens the underlying storage.
func (f *fragment) Open() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := func() error {
		// Initialize storage in a function so we can close if anything goes wrong.
		f.Logger.Debugf("open storage for index/field/view/fragment: %s/%s/%s/%d", f.index, f.field, f.view, f.shard)
		if err := f.openStorage(true); err != nil {
			return errors.Wrap(err, "opening storage")
		}

		// Fill cache with rows persisted to disk.
		f.Logger.Debugf("open cache for index/field/view/fragment: %s/%s/%s/%d", f.index, f.field, f.view, f.shard)
		if err := f.openCache(); err != nil {
			return errors.Wrap(err, "opening cache")
		}

		// Clear checksums.
		f.checksums = make(map[int][]byte)

		// Read last bit to determine max row.
		f.maxRowID = f.storage.Max() / ShardWidth
		f.stats.Gauge("rows", float64(f.maxRowID), 1.0)
		return nil
	}(); err != nil {
		f.close()
		return err
	}

	f.Logger.Debugf("successfully opened index/field/view/fragment: %s/%s/%s/%d", f.index, f.field, f.view, f.shard)
	return nil
}

func (f *fragment) reopen() (mustClose bool, err error) {
	if f.file == nil {
		// Open the data file to be mmap'd and used as an ops log.
		f.file, mustClose, err = syswrap.OpenFile(f.path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			return mustClose, fmt.Errorf("open file: %s", err)
		}
		f.storage.OpWriter = f.file
	}
	return mustClose, nil
}

// openStorage opens the storage bitmap. Usually you also want to read in
// the storage, but in the case where we just wrote that file, such as
// unprotectedWriteToFragment, we could also just... not. If we didn't
// have existing storage, we probably need to unmarshal the data. If the
// file we're asked to open is empty, we probably don't.
//
// If we already had mapped storage previously, we want to unmap that, and
// possibly remap it from the file, but we don't need a full unmarshal, just
// an update of mapped pointers.
//
// unmarshalData is somewhat overloaded. it tells us whether or not we
// need to actually create a bitmap from the data (if the data exists to
// do this from).
//
// usually unmarshalData is only set to false when we're in the middle of
// a snapshot, and unprotectedWriteToFragment just wrote the in-memory data
// out.
//
// If we have existing storage data, and we successfully get new data,
// we will unmap the existing storage data.
//
// This function's design is probably a problem -- it is trying to handle
// both cases where there was existing data before, and cases where we
// just wrote the data.
func (f *fragment) openStorage(unmarshalData bool) error {
	oldStorageData := f.storageData
	// there's a few places where we might encounter an error, but need
	// to continue past it through other error checks, before returning it.
	var lastError error

	// Create a roaring bitmap to serve as storage for the shard.
	if f.storage == nil {
		f.storage = roaring.NewFileBitmap()
		f.storage.Flags = f.flags
		// if we didn't actually have storage, we *do* need to
		// unmarshal this data in order to have any.
		unmarshalData = true
	}
	// Open the data file to be mmap'd and used as an ops log.
	file, mustClose, err := syswrap.OpenFile(f.path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return fmt.Errorf("open file: %s", err)
	}
	f.file = file
	if mustClose {
		defer f.safeClose()
	}

	// Lock the underlying file.
	if err := syscall.Flock(int(f.file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return fmt.Errorf("flock: %s", err)
	}

	// data is the data we would unmarshal from, if we're unmarshalling; it might
	// be obtained by calling ReadAll on a file.
	//
	// newStorageData is the data we should map things to. it is set only if
	// mmapped; if we didn't mmap (say, we couldn't), we won't want to unmap
	// the ioutil byte slice. (Theoretically, we shouldn't be using the mapped
	// flag in that case...)
	var data []byte
	var newStorageData []byte

	// If the file is empty then initialize it with an empty bitmap.
	fi, err := f.file.Stat()
	if err != nil {
		return errors.Wrap(err, "statting file before")
	} else if fi.Size() == 0 {
		bi := bufio.NewWriter(f.file)
		var err error
		if _, err = f.storage.WriteTo(bi); err != nil {
			return fmt.Errorf("init storage file: %s", err)
		}
		bi.Flush()
		_, err = f.file.Stat()
		if err != nil {
			return errors.Wrap(err, "statting file after")
		}
		// there's nothing here, we're not going to try to unmarshal it.
		unmarshalData = false
		f.rowCache = &simpleCache{make(map[uint64]*Row)}
	} else {
		// Mmap the underlying file so it can be zero copied.
		data, err = syswrap.Mmap(int(f.file.Fd()), 0, int(fi.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
		if err == syswrap.ErrMaxMapCountReached {
			f.Logger.Debugf("maximum number of maps reached, reading file instead")
			if unmarshalData {
				data, err = ioutil.ReadAll(file)
				if err != nil {
					return errors.Wrap(err, "failure file readall")
				}
			}
		} else if err != nil {
			return errors.Wrap(err, "mmap failed")
		} else {
			newStorageData = data
		}
	}

	if unmarshalData {
		f.storageData = newStorageData
		// We're about to either re-read the bitmap, or fail to do so
		// and unconditionally unmap the existing stuff. Either way, we
		// want to unmap the old storage data after we're done here, but
		// we can't unmap it yet because it's still live until sometime
		// later, but we can't unmap it later, because we could return
		// early... this is what defer is for.
		if oldStorageData != nil {
			defer func() {
				unmapErr := syswrap.Munmap(oldStorageData)
				if unmapErr != nil {
					f.Logger.Printf("unmap of old storage failed: %s", err)
				}
			}()
		}
		// set the preference for mapping based on whether the data's mmapped
		f.storage.PreferMapping(newStorageData != nil)
		// so we have a problem here: if this fails, it's unclear whether
		// *either* or *both* of old and new storage data might be in use.
		// So we call the thing that should unconditionally unmap both of them...
		if err := f.storage.UnmarshalBinary(data); err != nil {
			_, e2 := f.storage.RemapRoaringStorage(nil)
			if e2 != nil {
				return fmt.Errorf("unmarshal storage: file=%s, err=%s, clearing old mapping also failed: %v", f.file.Name(), err, e2)
			}
			return fmt.Errorf("unmarshal storage: file=%s, err=%s", f.file.Name(), err)
		}
		f.rowCache = &simpleCache{make(map[uint64]*Row)}
		f.ops, f.opN = f.storage.Ops()
	} else {
		// we're moving to new storage, so instead of using the OpN
		// derived from reading that storage, we notify the bitmap that
		// OpN is now effectively zero.
		f.opN = 0
		f.ops = 0
		f.storage.SetOps(0, 0)
		// if oldStorageData is nil, this just tries to unmap any bits that
		// are currently mapped. otherwise, it will point them at this
		// storage (if the containers match).
		var mappedAny bool
		mappedAny, lastError = f.storage.RemapRoaringStorage(newStorageData)
		if oldStorageData != nil {
			unmapErr := syswrap.Munmap(oldStorageData)
			if unmapErr != nil {
				f.Logger.Printf("unmap of old storage failed: %s", err)
			}
		}
		if mappedAny {
			// Advise the kernel that the mmap is accessed randomly.
			if err := madvise(newStorageData, syscall.MADV_RANDOM); err != nil {
				lastError = fmt.Errorf("madvise: %s", err)
			}
		} else {
			// if we did map data, but for some reason none of it got used
			// as backing store, we can unmap it, and set the slice to nil,
			// so we don't keep the now-invalid slice in f.storageData.
			if newStorageData != nil {
				unmapErr := syswrap.Munmap(newStorageData)
				if unmapErr != nil {
					lastError = fmt.Errorf("unmapping unused storage data: %s", err)
				}
				newStorageData = nil
			}
		}
		f.storageData = newStorageData
	}

	// Attach the file to the bitmap to act as a write-ahead log.
	f.storage.OpWriter = f.file

	return lastError
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
		n := f.storage.CountRange(id*ShardWidth, (id+1)*ShardWidth)
		f.cache.BulkAdd(id, n)
	}
	f.cache.Invalidate()

	return nil
}

// Close flushes the underlying storage, closes the file and unlocks it.
func (f *fragment) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for f.snapshotting {
		f.snapshotCond.Wait()
	}
	return f.close()
}

// awaitSnapshot lets us delay until the snapshot gets written, preventing tests
// from misleadingly showing amazingly fast performance because the snapshots they
// trigger haven't happened yet.
func (f *fragment) awaitSnapshot() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for f.snapshotting {
		f.snapshotCond.Wait()
	}
}

// unprotectedAwaitSnapshot assumes you already hold the lock, and waits for
// the snapshot fairy to come along.
func (f *fragment) unprotectedAwaitSnapshot() {
	for f.snapshotting {
		f.snapshotCond.Wait()
	}
}

func (f *fragment) close() error {
	// Flush cache if closing gracefully.
	if err := f.flushCache(); err != nil {
		f.Logger.Printf("fragment: error flushing cache on close: err=%s, path=%s", err, f.path)
		return errors.Wrap(err, "flushing cache")
	}

	// Close underlying storage.
	if err := f.closeStorage(true); err != nil {
		f.Logger.Printf("fragment: error closing storage: err=%s, path=%s", err, f.path)
		return errors.Wrap(err, "closing storage")
	}

	// Remove checksums.
	f.checksums = nil

	return nil
}

// safeClose is unprotected.
func (f *fragment) safeClose() error {
	// Flush file, unlock & close.
	if f.file != nil {
		if err := f.file.Sync(); err != nil {
			return fmt.Errorf("sync: %s", err)
		}
		if err := syscall.Flock(int(f.file.Fd()), syscall.LOCK_UN); err != nil {
			return fmt.Errorf("unlock: %s", err)
		}
		if err := syswrap.CloseFile(f.file); err != nil {
			return fmt.Errorf("close file: %s", err)
		}
	}
	f.file = nil
	f.storage.OpWriter = nil

	return nil
}

// closeStorage attempts to close storage, including unmapping the old
// storage if includeMap is true. This would normally make sense if you're
// expecting to be done using the fragment, or to reload it. But it's also
// okay to just leave stuff mmapped; you don't have to keep the file
// descriptor open. So in some cases, we'll just leave the old mmapping
// in place, rather than regenerating everything from the new file.
func (f *fragment) closeStorage(includeMap bool) error {
	// Clear the storage bitmap so it doesn't access the closed mmap.

	//f.storage = roaring.NewBitmap()

	// Unmap the file.
	if includeMap && f.storageData != nil {
		if err := syswrap.Munmap(f.storageData); err != nil {
			return fmt.Errorf("munmap: %s", err)
		}
		f.storageData = nil
	}

	if err := f.safeClose(); err != nil {
		return err
	}

	// opN is determined by how many bit set/clear operations are in the storage
	// write log, so once the storage is closed it should be 0. Opening new
	// storage will set opN appropriately.
	f.opN = 0

	return nil
}

// row returns a row by ID.
func (f *fragment) row(rowID uint64) *Row {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.unprotectedRow(rowID)
}

// unprotectedRow returns a row from the row cache if available or from storage
// (updating the cache).
func (f *fragment) unprotectedRow(rowID uint64) *Row {
	r, ok := f.rowCache.Fetch(rowID)
	if ok && r != nil {
		return r
	}

	row := f.rowFromStorage(rowID)
	f.rowCache.Add(rowID, row)
	return row
}

// rowFromStorage clones a row data out of fragment storage and returns it as a
// Row object.
func (f *fragment) rowFromStorage(rowID uint64) *Row {
	// Only use a subset of the containers.
	// NOTE: The start & end ranges must be divisible by container width.
	//
	// Note that OffsetRange now returns a new bitmap which uses frozen
	// containers which will use copy-on-write semantics. The actual bitmap
	// and Containers object are new and not shared, but the containers are
	// shared.
	data := f.storage.OffsetRange(f.shard*ShardWidth, rowID*ShardWidth, (rowID+1)*ShardWidth)

	row := &Row{
		segments: []rowSegment{{
			data:     data,
			shard:    f.shard,
			writable: true,
		}},
	}
	row.invalidateCount()

	return row
}

// setBit sets a bit for a given column & row within the fragment.
// This updates both the on-disk storage and the in-cache bitmap.
func (f *fragment) setBit(rowID, columnID uint64) (changed bool, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	mustClose, err := f.reopen()
	if err != nil {
		return false, errors.Wrap(err, "reopening")
	}
	if mustClose {
		defer f.safeClose()
	}

	// handle mutux field type
	if f.mutexVector != nil {
		if err := f.handleMutex(rowID, columnID); err != nil {
			return changed, errors.Wrap(err, "handling mutex")
		}
	}

	return f.unprotectedSetBit(rowID, columnID)
}

// handleMutex will clear an existing row and store the new row
// in the vector.
func (f *fragment) handleMutex(rowID, columnID uint64) error {
	if existingRowID, found, err := f.mutexVector.Get(columnID); err != nil {
		return errors.Wrap(err, "getting mutex vector data")
	} else if found && existingRowID != rowID {
		if _, err := f.unprotectedClearBit(existingRowID, columnID); err != nil {
			return errors.Wrap(err, "clearing mutex value")
		}
	}
	return nil
}

// unprotectedSetBit TODO should be replaced by an invocation of importPositions with a single bit to set.
func (f *fragment) unprotectedSetBit(rowID, columnID uint64) (changed bool, err error) {
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
	f.incrementOpN(1)

	// If we're using a cache, update it. Otherwise skip the
	// possibly-expensive count operation.
	if f.CacheType != CacheTypeNone {
		n := f.storage.CountRange(rowID*ShardWidth, (rowID+1)*ShardWidth)
		f.cache.Add(rowID, n)
	}
	// Drop the rowCache entry; it's wrong, and we don't want to force
	// a new copy if no one's reading it.
	f.rowCache.Add(rowID, nil)

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
func (f *fragment) clearBit(rowID, columnID uint64) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	mustClose, err := f.reopen()
	if err != nil {
		return false, errors.Wrap(err, "reopening")
	}
	if mustClose {
		defer f.safeClose()
	}
	return f.unprotectedClearBit(rowID, columnID)
}

// unprotectedClearBit TODO should be replaced by an invocation of
// importPositions with a single bit to clear.
func (f *fragment) unprotectedClearBit(rowID, columnID uint64) (changed bool, err error) {
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
	f.incrementOpN(1)

	// If we're using a cache, update it. Otherwise skip the
	// possibly-expensive count operation.
	if f.CacheType != CacheTypeNone {
		n := f.storage.CountRange(rowID*ShardWidth, (rowID+1)*ShardWidth)
		f.cache.Add(rowID, n)
	}
	// Drop the rowCache entry; it's wrong, and we don't want to force
	// a new copy if no one's reading it.
	f.rowCache.Add(rowID, nil)

	f.stats.Count("clearBit", 1, 1.0)

	return changed, nil
}

// setRow replaces an existing row (specified by rowID) with the given
// Row. This updates both the on-disk storage and the in-cache bitmap.
func (f *fragment) setRow(row *Row, rowID uint64) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	mustClose, err := f.reopen()
	if err != nil {
		return false, errors.Wrap(err, "reopening")
	}
	if mustClose {
		defer f.safeClose()
	}
	return f.unprotectedSetRow(row, rowID)
}

func (f *fragment) unprotectedSetRow(row *Row, rowID uint64) (changed bool, err error) {
	// TODO: In order to return `changed`, we need to first compare
	// the existing row with the given row. Determine if the overhead
	// of this is worth having `changed`.
	// For now we will assume changed is always true.
	changed = true

	// First container of the row in storage.
	headContainerKey := rowID << shardVsContainerExponent

	// Remove every existing container in the row.
	for i := uint64(0); i < (1 << shardVsContainerExponent); i++ {
		f.storage.Containers.Remove(headContainerKey + i)
	}

	// From the given row, get the rowSegment for this shard.
	seg := row.segment(f.shard)
	if seg == nil {
		return changed, nil
	}

	// Put each container from rowSegment to fragment storage.
	citer, _ := seg.data.Containers.Iterator(f.shard << shardVsContainerExponent)
	for citer.Next() {
		k, c := citer.Value()
		f.storage.Containers.Put(headContainerKey+(k%(1<<shardVsContainerExponent)), c)
	}

	// Update the row in cache.
	if f.CacheType != CacheTypeNone {
		n := f.storage.CountRange(rowID*ShardWidth, (rowID+1)*ShardWidth)
		f.cache.BulkAdd(rowID, n)
	}

	// invalidate rowCache for this row.
	f.rowCache.Add(rowID, nil)

	// Snapshot storage.
	f.enqueueSnapshot()
	f.stats.Count("setRow", 1, 1.0)

	return changed, nil
}

// ClearRow clears a row for a given rowID within the fragment.
// This updates both the on-disk storage and the in-cache bitmap.
func (f *fragment) clearRow(rowID uint64) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	mustClose, err := f.reopen()
	if err != nil {
		return false, errors.Wrap(err, "reopening")
	}
	if mustClose {
		defer f.safeClose()
	}
	return f.unprotectedClearRow(rowID)
}

func (f *fragment) unprotectedClearRow(rowID uint64) (changed bool, err error) {
	changed = false

	// First container of the row in storage.
	headContainerKey := rowID << shardVsContainerExponent

	// Remove every container in the row.
	for i := uint64(0); i < (1 << shardVsContainerExponent); i++ {
		k := headContainerKey + i
		// Technically we could bypass the Get() call and only
		// call Remove(), but the Get() gives us the ability
		// to return true if any existing data was removed.
		if cont := f.storage.Containers.Get(k); cont != nil {
			f.storage.Containers.Remove(k)
			changed = true
		}
	}

	// Clear the row in cache.
	f.cache.Add(rowID, 0)
	f.rowCache.Add(rowID, nil)

	// Snapshot storage.
	f.enqueueSnapshot()

	f.stats.Count("clearRow", 1, 1.0)

	return changed, nil
}

func (f *fragment) bit(rowID, columnID uint64) (bool, error) {
	pos, err := f.pos(rowID, columnID)
	if err != nil {
		return false, err
	}
	return f.storage.Contains(pos), nil
}

// value uses a column of bits to read a multi-bit value.
func (f *fragment) value(columnID uint64, bitDepth uint) (value int64, exists bool, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// If existence bit is unset then ignore remaining bits.
	if v, err := f.bit(bsiExistsBit, columnID); err != nil {
		return 0, false, errors.Wrap(err, "getting existence bit")
	} else if !v {
		return 0, false, nil
	}

	// Compute other bits into a value.
	for i := uint(0); i < bitDepth; i++ {
		if v, err := f.bit(uint64(bsiOffsetBit+i), columnID); err != nil {
			return 0, false, errors.Wrapf(err, "getting value bit %d", i)
		} else if v {
			value |= (1 << i)
		}
	}

	// Negate if sign bit set.
	if v, err := f.bit(bsiSignBit, columnID); err != nil {
		return 0, false, errors.Wrap(err, "getting sign bit")
	} else if v {
		value = -value
	}

	return value, true, nil
}

// clearValue uses a column of bits to clear a multi-bit value.
func (f *fragment) clearValue(columnID uint64, bitDepth uint, value int64) (changed bool, err error) {
	return f.setValueBase(columnID, bitDepth, value, true)
}

// setValue uses a column of bits to set a multi-bit value.
func (f *fragment) setValue(columnID uint64, bitDepth uint, value int64) (changed bool, err error) {
	return f.setValueBase(columnID, bitDepth, value, false)
}

func (f *fragment) positionsForValue(columnID uint64, bitDepth uint, value int64, clear bool, toSet, toClear []uint64) ([]uint64, []uint64, error) {
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

	for i := uint(0); i < bitDepth; i++ {
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
func (f *fragment) setValueBase(columnID uint64, bitDepth uint, value int64, clear bool) (changed bool, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	mustClose, err := f.reopen()
	if err != nil {
		return false, errors.Wrap(err, "reopening")
	}
	if mustClose {
		defer f.safeClose()
	}

	// Convert value to an unsigned representation.
	uvalue := uint64(value)
	if value < 0 {
		uvalue = uint64(-value)
	}

	for i := uint(0); i < bitDepth; i++ {
		if uvalue&(1<<i) != 0 {
			if c, err := f.unprotectedSetBit(uint64(bsiOffsetBit+i), columnID); err != nil {
				return changed, err
			} else if c {
				changed = true
			}
		} else {
			if c, err := f.unprotectedClearBit(uint64(bsiOffsetBit+i), columnID); err != nil {
				return changed, err
			} else if c {
				changed = true
			}
		}
	}

	// Mark value as set (or cleared).
	if clear {
		if c, err := f.unprotectedClearBit(uint64(bsiExistsBit), columnID); err != nil {
			return changed, errors.Wrap(err, "clearing not-null")
		} else if c {
			changed = true
		}
	} else {
		if c, err := f.unprotectedSetBit(uint64(bsiExistsBit), columnID); err != nil {
			return changed, errors.Wrap(err, "marking not-null")
		} else if c {
			changed = true
		}
	}

	// Mark sign bit (or clear).
	if value >= 0 || clear {
		if c, err := f.unprotectedClearBit(uint64(bsiSignBit), columnID); err != nil {
			return changed, errors.Wrap(err, "clearing sign")
		} else if c {
			changed = true
		}
	} else {
		if c, err := f.unprotectedSetBit(uint64(bsiSignBit), columnID); err != nil {
			return changed, errors.Wrap(err, "marking sign")
		} else if c {
			changed = true
		}
	}

	return changed, nil
}

// importSetValue is a more efficient SetValue just for imports.
func (f *fragment) importSetValue(columnID uint64, bitDepth uint, value int64, clear bool) (changed int, err error) { // nolint: unparam
	// Convert value to an unsigned representation.
	uvalue := uint64(value)
	if value < 0 {
		uvalue = uint64(-value)
	}

	for i := uint(0); i < bitDepth; i++ {
		bit, err := f.pos(uint64(bsiOffsetBit+i), columnID)
		if err != nil {
			return changed, errors.Wrap(err, "getting pos")
		}

		if uvalue&(1<<i) != 0 {
			if c, err := f.storage.Add(bit); err != nil {
				return changed, errors.Wrap(err, "adding")
			} else if c {
				changed++
			}
		} else {
			if c, err := f.storage.Remove(bit); err != nil {
				return changed, errors.Wrap(err, "removing")
			} else if c {
				changed++
			}
		}
	}

	// Mark value as set.
	if p, err := f.pos(uint64(bsiExistsBit), columnID); err != nil {
		return changed, errors.Wrap(err, "getting not-null pos")
	} else if clear {
		if c, err := f.storage.Remove(p); err != nil {
			return changed, errors.Wrap(err, "removing not-null from storage")
		} else if c {
			changed++
		}
	} else {
		if c, err := f.storage.Add(p); err != nil {
			return changed, errors.Wrap(err, "adding not-null to storage")
		} else if c {
			changed++
		}
	}

	// Mark sign bit.
	if p, err := f.pos(uint64(bsiSignBit), columnID); err != nil {
		return changed, errors.Wrap(err, "getting sign pos")
	} else if value >= 0 || clear {
		if c, err := f.storage.Remove(p); err != nil {
			return changed, errors.Wrap(err, "removing sign from storage")
		} else if c {
			changed++
		}
	} else {
		if c, err := f.storage.Add(p); err != nil {
			return changed, errors.Wrap(err, "adding sign to storage")
		} else if c {
			changed++
		}
	}

	return changed, nil
}

// sum returns the sum of a given bsiGroup as well as the number of columns involved.
// A bitmap can be passed in to optionally filter the computed columns.
func (f *fragment) sum(filter *Row, bitDepth uint) (sum int64, count uint64, err error) {
	// Compute count based on the existence row.
	consider := f.row(bsiExistsBit)
	if filter != nil {
		consider = consider.Intersect(filter)
	}
	count = consider.Count()

	// Determine positive & negative sets.
	nrow := f.row(bsiSignBit)
	prow := consider.Difference(nrow)

	// Compute the sum based on the bit count of each row multiplied by the
	// place value of each row. For example, 10 bits in the 1's place plus
	// 4 bits in the 2's place plus 3 bits in the 4's place equals a total
	// sum of 30:
	//
	//   10*(2^0) + 4*(2^1) + 3*(2^2) = 30
	//
	// Execute once for positive numbers and once for negative. Subtract the
	// negative sum from the positive sum.
	for i := uint(0); i < bitDepth; i++ {
		row := f.row(uint64(bsiOffsetBit + i))

		psum := int64((1 << i) * row.intersectionCount(prow))
		nsum := int64((1 << i) * row.intersectionCount(nrow))

		// Squash to reduce the possibility of overflow.
		sum += psum - nsum
	}

	return sum, count, nil
}

// min returns the min of a given bsiGroup as well as the number of columns involved.
// A bitmap can be passed in to optionally filter the computed columns.
func (f *fragment) min(filter *Row, bitDepth uint) (min int64, count uint64, err error) {
	consider := f.row(bsiExistsBit)
	if filter != nil {
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
	if row := f.row(bsiSignBit).Intersect(consider); row.Any() {
		min, count := f.maxUnsigned(row, bitDepth)
		return -min, count, nil
	}

	// Otherwise find lowest positive number.
	min, count = f.minUnsigned(consider, bitDepth)
	return min, count, nil
}

// minUnsigned the lowest value without considering the sign bit. Filter is required.
func (f *fragment) minUnsigned(filter *Row, bitDepth uint) (min int64, count uint64) {
	for i := int(bitDepth - 1); i >= 0; i-- {
		row := filter.Difference(f.row(uint64(bsiOffsetBit + i)))
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
	return min, count
}

// max returns the max of a given bsiGroup as well as the number of columns involved.
// A bitmap can be passed in to optionally filter the computed columns.
func (f *fragment) max(filter *Row, bitDepth uint) (max int64, count uint64, err error) {
	consider := f.row(bsiExistsBit)
	if filter != nil {
		consider = consider.Intersect(filter)
	}

	// If there are no columns to consider, return early.
	if !consider.Any() {
		return 0, 0, nil
	}

	// Find lowest negative number w/o sign and negate, if no positives are available.
	pos := consider.Difference(f.row(bsiSignBit))
	if !pos.Any() {
		max, count = f.minUnsigned(consider, bitDepth)
		return -max, count, nil
	}

	// Otherwise find highest positive number.
	max, count = f.maxUnsigned(pos, bitDepth)
	return max, count, nil
}

// maxUnsigned the highest value without considering the sign bit. Filter is required.
func (f *fragment) maxUnsigned(filter *Row, bitDepth uint) (max int64, count uint64) {
	for i := int(bitDepth - 1); i >= 0; i-- {
		row := f.row(uint64(bsiOffsetBit + i)).Intersect(filter)
		count = row.Count()
		if count > 0 {
			max += (1 << uint(i))
			filter = row
		} else if i == 0 {
			count = filter.Count()
		}
	}
	return max, count
}

// minRow returns minRowID of the rows in the filter and its count.
// if filter is nil, it returns fragment.minRowID, 1
// if fragment has no rows, it returns 0, 0
func (f *fragment) minRow(filter *Row) (uint64, uint64) {
	minRowID, hasRowID := f.minRowID()
	if hasRowID {
		if filter == nil {
			return minRowID, 1
		}
		// iterate from min row ID and return the first that intersects with filter.
		for i := minRowID; i <= f.maxRowID; i++ {
			row := f.row(i).Intersect(filter)
			count := row.Count()
			if count > 0 {
				return i, count
			}
		}
	}
	return 0, 0
}

// maxRow returns maxRowID of the rows in the filter and its count.
// if filter is nil, it returns fragment.maxRowID, 1
// if fragment has no rows, it returns 0, 0
func (f *fragment) maxRow(filter *Row) (uint64, uint64) {
	minRowID, hasRowID := f.minRowID()
	if hasRowID {
		if filter == nil {
			return f.maxRowID, 1
		}
		// iterate back from max row ID and return the first that intersects with filter.
		// TODO: implement reverse container iteration to improve performance here for sparse data. --Jaffee
		for i := f.maxRowID; i >= minRowID; i-- {
			row := f.row(i).Intersect(filter)
			count := row.Count()
			if count > 0 {
				return i, count
			}
		}
	}
	return 0, 0
}

// rangeOp returns bitmaps with a bsiGroup value encoding matching the predicate.
func (f *fragment) rangeOp(op pql.Token, bitDepth uint, predicate int64) (*Row, error) {
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

func (f *fragment) rangeEQ(bitDepth uint, predicate int64) (*Row, error) {
	// Start with set of columns with values set.
	b := f.row(bsiExistsBit)

	// Filter to only positive/negative numbers.
	upredicate := uint64(predicate)
	if predicate < 0 {
		upredicate = uint64(-predicate)
		b = b.Intersect(f.row(bsiSignBit)) // only negatives
	} else {
		b = b.Difference(f.row(bsiSignBit)) // only positives
	}

	// Filter any bits that don't match the current bit value.
	for i := int(bitDepth - 1); i >= 0; i-- {
		row := f.row(uint64(bsiOffsetBit + i))
		bit := (upredicate >> uint(i)) & 1

		if bit == 1 {
			b = b.Intersect(row)
		} else {
			b = b.Difference(row)
		}
	}

	return b, nil
}

func (f *fragment) rangeNEQ(bitDepth uint, predicate int64) (*Row, error) {
	// Start with set of columns with values set.
	b := f.row(bsiExistsBit)

	// Get the equal bitmap.
	eq, err := f.rangeEQ(bitDepth, predicate)
	if err != nil {
		return nil, err
	}

	// Not-null minus the equal bitmap.
	b = b.Difference(eq)

	return b, nil
}

func (f *fragment) rangeLT(bitDepth uint, predicate int64, allowEquality bool) (*Row, error) {
	// Start with set of columns with values set.
	b := f.row(bsiExistsBit)

	// Create predicate without sign bit.
	upredicate := uint64(predicate)
	if predicate < 0 {
		upredicate = uint64(-predicate)
	}

	// If predicate is positive, return all positives less than predicate and all negatives.
	if (predicate >= 0 && allowEquality) || (predicate >= -1 && !allowEquality) {
		pos, err := f.rangeLTUnsigned(b.Difference(f.row(bsiSignBit)), bitDepth, upredicate, allowEquality)
		if err != nil {
			return nil, err
		}
		neg := f.row(bsiSignBit)
		return neg.Union(pos), nil
	}

	// Otherwise if predicate is negative, return all negatives greater than upredicate.
	return f.rangeGTUnsigned(b.Intersect(f.row(bsiSignBit)), bitDepth, upredicate, allowEquality)
}

// rangeLTUnsigned returns all bits LT/LTE the predicate without considering the sign bit.
func (f *fragment) rangeLTUnsigned(filter *Row, bitDepth uint, predicate uint64, allowEquality bool) (*Row, error) {
	keep := NewRow()

	// Filter any bits that don't match the current bit value.
	leadingZeros := true
	for i := int(bitDepth - 1); i >= 0; i-- {
		row := f.row(uint64(bsiOffsetBit + i))
		bit := (predicate >> uint(i)) & 1

		// Remove any columns with higher bits set.
		if leadingZeros {
			if bit == 0 {
				filter = filter.Difference(row)
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
			return filter.Difference(row.Difference(keep)), nil
		}

		// If bit is zero then remove all set columns not in excluded bitmap.
		if bit == 0 {
			filter = filter.Difference(row.Difference(keep))
			continue
		}

		// If bit is set then add columns for set bits to exclude.
		// Don't bother to compute this on the final iteration.
		if i > 0 {
			keep = keep.Union(filter.Difference(row))
		}
	}

	return filter, nil
}

func (f *fragment) rangeGT(bitDepth uint, predicate int64, allowEquality bool) (*Row, error) {
	b := f.row(bsiExistsBit)

	// Create predicate without sign bit.
	upredicate := uint64(predicate)
	if predicate < 0 {
		upredicate = uint64(-predicate)
	}

	// If predicate is positive, return all positives greater than predicate.
	if (predicate >= 0 && allowEquality) || (predicate >= -1 && !allowEquality) {
		return f.rangeGTUnsigned(b.Difference(f.row(bsiSignBit)), bitDepth, upredicate, allowEquality)
	}

	// If predicate is negative, return all negatives less than than upredicate and all positives.
	neg, err := f.rangeLTUnsigned(b.Intersect(f.row(bsiSignBit)), bitDepth, upredicate, allowEquality)
	if err != nil {
		return nil, err
	}
	pos := b.Difference(f.row(bsiSignBit))
	return pos.Union(neg), nil
}

func (f *fragment) rangeGTUnsigned(filter *Row, bitDepth uint, predicate uint64, allowEquality bool) (*Row, error) {
	keep := NewRow()

	// Filter any bits that don't match the current bit value.
	for i := int(bitDepth - 1); i >= 0; i-- {
		row := f.row(uint64(bsiOffsetBit + i))
		bit := (predicate >> uint(i)) & 1

		// Handle last bit differently.
		// If bit is one then return only already kept columns.
		// If bit is zero then remove any unset columns.
		if i == 0 && !allowEquality {
			if bit == 1 {
				return keep, nil
			}
			return filter.Difference(filter.Difference(row).Difference(keep)), nil
		}

		// If bit is set then remove all unset columns not already kept.
		if bit == 1 {
			filter = filter.Difference(filter.Difference(row).Difference(keep))
			continue
		}

		// If bit is unset then add columns with set bit to keep.
		// Don't bother to compute this on the final iteration.
		if i > 0 {
			keep = keep.Union(filter.Intersect(row))
		}
	}

	return filter, nil
}

// notNull returns the exists row.
func (f *fragment) notNull() (*Row, error) {
	return f.row(bsiExistsBit), nil
}

// rangeBetween returns bitmaps with a bsiGroup value encoding matching any value between predicateMin and predicateMax.
func (f *fragment) rangeBetween(bitDepth uint, predicateMin, predicateMax int64) (*Row, error) {
	b := f.row(bsiExistsBit)

	// Convert predicates to unsigned values.
	upredicateMin, upredicateMax := uint64(predicateMin), uint64(predicateMax)
	if predicateMin < 0 {
		upredicateMin = uint64(-predicateMin)
	}
	if predicateMax < 0 {
		upredicateMax = uint64(-predicateMax)
	}

	// Handle positive-only values.
	if predicateMin >= 0 {
		return f.rangeBetweenUnsigned(b.Difference(f.row(bsiSignBit)), bitDepth, upredicateMin, upredicateMax)
	}

	// Handle negative-only values. Swap unsigned min/max predicates.
	if predicateMax < 0 {
		return f.rangeBetweenUnsigned(b.Intersect(f.row(bsiSignBit)), bitDepth, upredicateMax, upredicateMin)
	}

	// If predicate crosses positive/negative boundary then handle separately and union.
	pos, err := f.rangeLTUnsigned(b.Difference(f.row(bsiSignBit)), bitDepth, upredicateMax, true)
	if err != nil {
		return nil, err
	}
	neg, err := f.rangeLTUnsigned(b.Intersect(f.row(bsiSignBit)), bitDepth, upredicateMin, true)
	if err != nil {
		return nil, err
	}
	return pos.Union(neg), nil
}

// rangeBetweenUnsigned returns BSI columns for a range of values. Disregards the sign bit.
func (f *fragment) rangeBetweenUnsigned(filter *Row, bitDepth uint, predicateMin, predicateMax uint64) (*Row, error) {
	keep1 := NewRow() // GTE
	keep2 := NewRow() // LTE

	// Filter any bits that don't match the current bit value.
	for i := int(bitDepth - 1); i >= 0; i-- {
		row := f.row(uint64(bsiOffsetBit + i))
		bit1 := (predicateMin >> uint(i)) & 1
		bit2 := (predicateMax >> uint(i)) & 1

		// GTE predicateMin
		// If bit is set then remove all unset columns not already kept.
		if bit1 == 1 {
			filter = filter.Difference(filter.Difference(row).Difference(keep1))
		} else {
			// If bit is unset then add columns with set bit to keep.
			// Don't bother to compute this on the final iteration.
			if i > 0 {
				keep1 = keep1.Union(filter.Intersect(row))
			}
		}

		// LTE predicateMax
		// If bit is zero then remove all set bits not in excluded bitmap.
		if bit2 == 0 {
			filter = filter.Difference(row.Difference(keep2))
		} else {
			// If bit is set then add columns for set bits to exclude.
			// Don't bother to compute this on the final iteration.
			if i > 0 {
				keep2 = keep2.Union(filter.Difference(row))
			}
		}
	}

	return filter, nil
}

// pos translates the row ID and column ID into a position in the storage bitmap.
func (f *fragment) pos(rowID, columnID uint64) (uint64, error) {
	// Return an error if the column ID is out of the range of the fragment's shard.
	minColumnID := f.shard * ShardWidth
	if columnID < minColumnID || columnID >= minColumnID+ShardWidth {
		return 0, errors.Errorf("column:%d out of bounds", columnID)
	}
	return pos(rowID, columnID), nil
}

// forEachBit executes fn for every bit set in the fragment.
// Errors returned from fn are passed through.
func (f *fragment) forEachBit(fn func(rowID, columnID uint64) error) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	var err error
	f.storage.ForEach(func(i uint64) {
		// Skip if an error has already occurred.
		if err != nil {
			return
		}

		// Invoke caller's function.
		err = fn(i/ShardWidth, (f.shard*ShardWidth)+(i%ShardWidth))
	})
	return err
}

// top returns the top rows from the fragment.
// If opt.Src is specified then only rows which intersect src are returned.
// If opt.FilterValues exist then the row attribute specified by field is matched.
func (f *fragment) top(opt topOptions) ([]Pair, error) {
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
				count = opt.Src.intersectionCount(f.row(rowID))
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
		count := opt.Src.intersectionCount(f.row(rowID))
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

func (f *fragment) topBitmapPairs(rowIDs []uint64) []bitmapPair {
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

		row := f.row(rowID)
		if row.Count() > 0 {
			// Otherwise load from storage.
			pairs = append(pairs, bitmapPair{
				ID:    rowID,
				Count: row.Count(),
			})
		}
	}
	sort.Sort(bitmapPairs(pairs))
	return pairs
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

	// Filter field name & values.
	FilterName        string
	FilterValues      []interface{}
	TanimotoThreshold uint64
}

// Checksum returns a checksum for the entire fragment.
// If two fragments have the same checksum then they have the same data.
func (f *fragment) Checksum() []byte {
	h := xxhash.New()
	for _, block := range f.Blocks() {
		_, _ = h.Write(block.Checksum)
	}
	return h.Sum(nil)
}

// InvalidateChecksums clears all cached block checksums.
func (f *fragment) InvalidateChecksums() {
	f.mu.Lock()
	f.checksums = make(map[int][]byte)
	f.mu.Unlock()
}

// Blocks returns info for all blocks containing data.
func (f *fragment) Blocks() []FragmentBlock {
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
	blockID := int(v / (HashBlockSize * ShardWidth))
	for {
		// Check for multiple block checksums in a row.
		if n := f.readContiguousChecksums(&a, blockID); n > 0 {
			itr.Seek(uint64(blockID+n) * HashBlockSize * ShardWidth)
			v, eof = itr.Next()
			if eof {
				break
			}
			blockID = int(v / (HashBlockSize * ShardWidth))
			continue
		}

		// Reset hasher.
		h.blockID = blockID
		h.Reset()

		// Read all values for the block.
		for ; ; v, eof = itr.Next() {
			// Once we hit the next block, save the value for the next iteration.
			blockID = int(v / (HashBlockSize * ShardWidth))
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
func (f *fragment) readContiguousChecksums(a *[]FragmentBlock, blockID int) (n int) {
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
func (f *fragment) blockData(id int) (rowIDs, columnIDs []uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.storage.ForEachRange(uint64(id)*HashBlockSize*ShardWidth, (uint64(id)+1)*HashBlockSize*ShardWidth, func(i uint64) {
		rowIDs = append(rowIDs, i/ShardWidth)
		columnIDs = append(columnIDs, i%ShardWidth)
	})
	return rowIDs, columnIDs
}

// mergeBlock compares the block's bits and computes a diff with another set of block bits.
// The state of a bit is determined by consensus from all blocks being considered.
//
// For example, if 3 blocks are compared and two have a set bit and one has a
// cleared bit then the bit is considered cleared. The function returns the
// diff per incoming block so that all can be in sync.
func (f *fragment) mergeBlock(id int, data []pairSet) (sets, clears []pairSet, err error) {
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
	maxColumnID := uint64(ShardWidth)

	// Create buffered iterator for local block.
	itrs := make([]*bufIterator, 1, len(data)+1)
	itrs[0] = newBufIterator(
		newLimitIterator(
			newRoaringIterator(f.storage.Iterator()), maxRowID, maxColumnID,
		),
	)

	// Append buffered iterators for each incoming block.
	for i := range data {
		var itr iterator = newSliceIterator(data[i].rowIDs, data[i].columnIDs)
		itr = newLimitIterator(itr, maxRowID, maxColumnID)
		itrs = append(itrs, newBufIterator(itr))
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
		if _, err := f.unprotectedSetBit(sets[0].rowIDs[i], (f.shard*ShardWidth)+sets[0].columnIDs[i]); err != nil {
			return nil, nil, errors.Wrap(err, "setting")
		}
	}

	// Clear local bits.
	for i := range clears[0].columnIDs {
		if _, err := f.unprotectedClearBit(clears[0].rowIDs[i], (f.shard*ShardWidth)+clears[0].columnIDs[i]); err != nil {
			return nil, nil, errors.Wrap(err, "clearing")
		}
	}

	return sets[1:], clears[1:], nil
}

// bulkImport bulk imports a set of bits and then snapshots the storage.
// The cache is updated to reflect the new data.
func (f *fragment) bulkImport(rowIDs, columnIDs []uint64, options *ImportOptions) error {
	// Verify that there are an equal number of row ids and column ids.
	if len(rowIDs) != len(columnIDs) {
		return fmt.Errorf("mismatch of row/column len: %d != %d", len(rowIDs), len(columnIDs))
	}

	if f.mutexVector != nil && !options.Clear {
		return f.bulkImportMutex(rowIDs, columnIDs)
	}
	return f.bulkImportStandard(rowIDs, columnIDs, options)
}

// bulkImportStandard performs a bulk import on a standard fragment. May mutate
// its rowIDs and columnIDs arguments.
func (f *fragment) bulkImportStandard(rowIDs, columnIDs []uint64, options *ImportOptions) (err error) {
	// rowSet maintains the set of rowIDs present in this import. It allows the
	// cache to be updated once per row, instead of once per bit. TODO: consider
	// sorting by rowID/columnID first and avoiding the map allocation here. (we
	// could reuse rowIDs to store the list of unique row IDs)
	rowSet := make(map[uint64]struct{})
	lastRowID := uint64(1 << 63)

	// replace columnIDs with calculated positions to avoid allocation.
	for i := 0; i < len(columnIDs); i++ {
		rowID, columnID := rowIDs[i], columnIDs[i]
		pos, err := f.pos(rowID, columnID)
		if err != nil {
			return err
		}
		columnIDs[i] = pos

		// Add row to rowSet.
		if rowID != lastRowID {
			lastRowID = rowID
			rowSet[rowID] = struct{}{}
		}
	}
	positions := columnIDs
	f.mu.Lock()
	defer f.mu.Unlock()
	if options.Clear {
		err = f.importPositions(nil, positions, rowSet)
	} else {
		err = f.importPositions(positions, nil, rowSet)
	}
	return errors.Wrap(err, "bulkImportStandard")
}

// importPositions takes slices of positions within the fragment to set and
// clear in storage. One must also pass in the set of unique rows which are
// affected by the set and clear operations. It is unprotected (f.mu must be
// locked when calling it). No position should appear in both set and clear.
//
// importPositions tries to intelligently decide whether or not to do a full
// snapshot of the fragment or just do in-memory updates while appending
// operations to the op log.
func (f *fragment) importPositions(set, clear []uint64, rowSet map[uint64]struct{}) error {
	mustClose, err := f.reopen()
	if err != nil {
		return errors.Wrap(err, "reopening")
	}
	if mustClose {
		defer f.safeClose()
	}

	if len(set) > 0 {
		f.stats.Count("ImportingN", int64(len(set)), 1)
		changedN, err := f.storage.AddN(set...) // TODO benchmark Add/RemoveN behavior with sorted/unsorted positions
		if err != nil {
			return errors.Wrap(err, "adding positions")
		}
		f.stats.Count("ImportedN", int64(changedN), 1)
		f.incrementOpN(changedN)
	}

	if len(clear) > 0 {
		f.stats.Count("ClearingN", int64(len(clear)), 1)
		changedN, err := f.storage.RemoveN(clear...)
		if err != nil {
			return errors.Wrap(err, "clearing positions")
		}
		f.stats.Count("ClearedN", int64(changedN), 1)
		f.incrementOpN(changedN)
	}

	// Update cache counts for all affected rows.
	for rowID := range rowSet {
		// Invalidate block checksum.
		delete(f.checksums, int(rowID/HashBlockSize))

		if f.CacheType != CacheTypeNone {
			n := f.storage.CountRange(rowID*ShardWidth, (rowID+1)*ShardWidth)
			f.cache.BulkAdd(rowID, n)
		}

		f.rowCache.Add(rowID, nil)
	}

	if f.CacheType != CacheTypeNone {
		f.cache.Recalculate()
	}

	return nil
}

// bulkImportMutex performs a bulk import on a fragment while ensuring
// mutex restrictions. Because the mutex requirements must be checked
// against storage, this method must acquire a write lock on the fragment
// during the entire process, and it handles every bit independently.
func (f *fragment) bulkImportMutex(rowIDs, columnIDs []uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	rowSet := make(map[uint64]struct{})
	// we have to maintain which columns are getting bits set as a map so that
	// we don't end up setting multiple bits in the same column if a column is
	// repeated within the import.
	colSet := make(map[uint64]uint64)

	// Since each imported bit will at most set one bit and clear one bit, we
	// can reuse the rowIDs and columnIDs slices as the set and clear slice
	// arguments to importPositions. The set positions we'll get from the
	// colSet, but we maintain clearIdx as we loop through row and col ids so
	// that we know how many bits we need to clear and how far through columnIDs
	// we are.
	clearIdx := 0
	for i := range rowIDs {
		rowID, columnID := rowIDs[i], columnIDs[i]
		if existingRowID, found, err := f.mutexVector.Get(columnID); err != nil {
			return errors.Wrap(err, "getting mutex vector data")
		} else if found && existingRowID != rowID {
			// Determine the position of the bit in the storage.
			clearPos, err := f.pos(existingRowID, columnID)
			if err != nil {
				return err
			}
			columnIDs[clearIdx] = clearPos
			clearIdx++

			rowSet[existingRowID] = struct{}{}
		} else if found && existingRowID == rowID {
			continue
		}
		pos, err := f.pos(rowID, columnID)
		if err != nil {
			return err
		}
		colSet[columnID] = pos
		rowSet[rowID] = struct{}{}
	}

	// re-use rowIDs by populating positions to set from colSet.
	i := 0
	for _, pos := range colSet {
		rowIDs[i] = pos
		i++
	}
	toSet := rowIDs[:i]
	toClear := columnIDs[:clearIdx]

	return errors.Wrap(f.importPositions(toSet, toClear, rowSet), "importing positions")
}

func (f *fragment) importValueSmallWrite(columnIDs []uint64, values []int64, bitDepth uint, clear bool) error {
	// TODO figure out how to avoid re-allocating these each time. Probably
	// possible to store them on the fragment with a capacity based on
	// MaxOpN. For now, we know that the total number of bits to be
	// set+cleared is len(values)*(bitDepth+1), so we make each slice
	// slightly more than half of that to try to avoid reallocation.
	toSet := make([]uint64, 0, len(columnIDs)*int(bitDepth+1)*(5/8))
	toClear := make([]uint64, 0, len(columnIDs)*int(bitDepth+1)*(5/8))
	colSet := make(map[uint64]struct{}, len(columnIDs))

	if err := func() (err error) {
		for i := len(columnIDs) - 1; i >= 0; i-- {
			columnID, value := columnIDs[i], values[i]
			if _, ok := colSet[columnID]; ok {
				continue
			}

			colSet[columnID] = struct{}{}
			toSet, toClear, err = f.positionsForValue(columnID, bitDepth, value, clear, toSet, toClear)
			if err != nil {
				return errors.Wrap(err, "getting positions for value")
			}
		}
		return nil
	}(); err != nil {
		_ = f.closeStorage(true)
		_ = f.openStorage(true)
		return err
	}
	rowSet := make(map[uint64]struct{}, bitDepth+1)
	for i := uint(0); i < bitDepth+1; i++ {
		rowSet[uint64(i)] = struct{}{}
	}
	err := f.importPositions(toSet, toClear, rowSet)
	return errors.Wrap(err, "importing positions")
}

// importValue bulk imports a set of range-encoded values.
func (f *fragment) importValue(columnIDs []uint64, values []int64, bitDepth uint, clear bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Verify that there are an equal number of column ids and values.
	if len(columnIDs) != len(values) {
		return fmt.Errorf("mismatch of column/value len: %d != %d", len(columnIDs), len(values))
	}

	if len(columnIDs)*int(bitDepth+1)+f.opN < f.MaxOpN {
		return errors.Wrap(f.importValueSmallWrite(columnIDs, values, bitDepth, clear), "import small write")
	}

	// Process every value.
	// If an error occurs then reopen the storage.
	f.storage.OpWriter = nil
	totalChanges := 0
	if err := func() (err error) {
		for i := range columnIDs {
			columnID, value := columnIDs[i], values[i]
			changed, err := f.importSetValue(columnID, bitDepth, value, clear)
			if err != nil {
				return errors.Wrapf(err, "importSetValue")
			}
			totalChanges += changed
		}
		return nil
	}(); err != nil {
		_ = f.closeStorage(true)
		_ = f.openStorage(true)
		return err
	}
	// We don't actually care, except we want our stats to be accurate.
	f.incrementOpN(totalChanges)

	// in theory, this should probably have happened anyway, but if enough
	// of the bits matched existing bits, we'll be under our opN estimate, and
	// we want to ensure that the snapshot happens.
	f.enqueueSnapshot()
	f.unprotectedAwaitSnapshot()

	return nil
}

// importRoaring imports from the official roaring data format defined at
// https://github.com/RoaringBitmap/RoaringFormatSpec or from pilosa's version
// of the roaring format. The cache is updated to reflect the new data.
func (f *fragment) importRoaring(ctx context.Context, data []byte, clear bool) error {
	rowSize := uint64(1 << shardVsContainerExponent)
	span, ctx := tracing.StartSpanFromContext(ctx, "fragment.importRoaring")
	defer span.Finish()
	span, ctx = tracing.StartSpanFromContext(ctx, "importRoaring.AcquireFragmentLock")
	f.mu.Lock()
	defer f.mu.Unlock()
	span.Finish()
	span, ctx = tracing.StartSpanFromContext(ctx, "importRoaring.ImportRoaringBits")
	changed, rowSet, err := f.storage.ImportRoaringBits(data, clear, true, rowSize)
	span.Finish()
	if err != nil {
		return err
	}

	updateCache := f.CacheType != CacheTypeNone
	anyChanged := false

	for rowID, changes := range rowSet {
		if changes == 0 {
			continue
		}
		f.rowCache.Add(rowID, nil)
		if updateCache {
			anyChanged = true
			f.cache.BulkAdd(rowID, f.cache.Get(rowID)+uint64(changes))
		}
	}
	// we only set this if we need to update the cache
	if anyChanged {
		f.cache.Recalculate()
	}

	span, _ = tracing.StartSpanFromContext(ctx, "importRoaring.incrementOpN")
	f.incrementOpN(changed)
	span.Finish()
	return nil
}

// incrementOpN increase the operation count by one.
// If the count exceeds the maximum allowed then a snapshot is performed.
func (f *fragment) incrementOpN(changed int) {
	if changed <= 0 {
		return
	}
	f.opN += changed
	f.ops++
	if f.opN > f.MaxOpN {
		f.enqueueSnapshot()
	}
}

// Snapshot writes the storage bitmap to disk and reopens it. This may
// coexist with existing background-queue snapshotting; it does not remove
// things from the queue. You probably don't want to do this; use
// enqueueSnapshot/awaitSnapshot.
func (f *fragment) Snapshot() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.snapshot()
}

func track(start time.Time, message string, stats stats.StatsClient, logger logger.Logger) {
	elapsed := time.Since(start)
	logger.Printf("%s took %s", message, elapsed)
	stats.Histogram("snapshot", elapsed.Seconds(), 1.0)
}

// protectedSnapshot grabs the lock and unconditionally calls snapshot(). If
// fromQueue is true, the snapshotting state is also cleared.
func (f *fragment) protectedSnapshot(fromQueue bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	err := f.snapshot()
	if fromQueue {
		f.snapshotting = false
	}
	return err
}

// snapshot does the actual snapshot operation. it does not check or care
// about f.snapshotting.
func (f *fragment) snapshot() error {
	f.totalOpN += int64(f.opN)
	f.totalOps += int64(f.ops)
	f.snapshotsTaken++
	_, err := unprotectedWriteToFragment(f, f.storage)
	return err
}

// unprotectedWriteToFragment writes the fragment f with bm as the data. It is unprotected, and
// f.mu must be locked when calling it.
func unprotectedWriteToFragment(f *fragment, bm *roaring.Bitmap) (n int64, err error) { // nolint: interfacer
	completeMessage := fmt.Sprintf("fragment: snapshot complete %s/%s/%s/%d", f.index, f.field, f.view, f.shard)
	start := time.Now()
	defer track(start, completeMessage, f.stats, f.Logger)

	// Create a temporary file to snapshot to.
	snapshotPath := f.path + snapshotExt
	file, err := os.Create(snapshotPath)
	if err != nil {
		return n, fmt.Errorf("create snapshot file: %s", err)
	}
	defer file.Close()

	// Write storage to snapshot.
	bw := bufio.NewWriter(file)
	if n, err = bm.WriteTo(bw); err != nil {
		return n, fmt.Errorf("snapshot write to: %s", err)
	}

	if err := bw.Flush(); err != nil {
		return n, fmt.Errorf("flush: %s", err)
	}

	// Close current storage.
	if err := f.closeStorage(false); err != nil {
		return n, fmt.Errorf("close storage: %s", err)
	}

	// Move snapshot to data file location.
	if err := os.Rename(snapshotPath, f.path); err != nil {
		return n, fmt.Errorf("rename snapshot: %s", err)
	}

	// if we reloaded from the file, we'd end up with this bitmap
	// as our storage. so... let's use this bitmap. as our storage.
	f.storage = bm

	// Reopen storage.
	if err := f.openStorage(false); err != nil {
		return n, fmt.Errorf("open storage: %s", err)
	}

	// Reset operation count.
	f.opN = 0

	return n, nil
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

func (f *fragment) writeStorageToArchive(tw *tar.Writer) error {
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

func (f *fragment) writeCacheToArchive(tw *tar.Writer) error {
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

func (f *fragment) readStorageFromArchive(r io.Reader) error {
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
	if err := f.closeStorage(true); err != nil {
		return errors.Wrap(err, "closing")
	}

	// Move snapshot to data file location.
	if err := os.Rename(path, f.path); err != nil {
		return errors.Wrap(err, "renaming")
	}

	// Reopen storage.
	if err := f.openStorage(true); err != nil {
		return errors.Wrap(err, "opening")
	}

	return nil
}

func (f *fragment) readCacheFromArchive(r io.Reader) error {
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

func (f *fragment) minRowID() (uint64, bool) {
	min, ok := f.storage.Min()
	return min / ShardWidth, ok
}

// rowFilter is a function signature for controlling iteration over containers
// in a fragment. It will be invoked on each container found and returns two
// booleans. The first is whether the row this container is in should be
// included or skipped, and the second is whether to stop processing or
// continue.
type rowFilter func(rowID, key uint64, c *roaring.Container) (include, done bool)

// filterWithLimit returns a filter which will only allow a limited number of
// rows to be returned. It should be applied last so that it is only called (and
// therefore only updates its internal state) if the row is being included by
// every other filter.
func filterWithLimit(limit uint64) rowFilter {
	return func(rowID, key uint64, c *roaring.Container) (include, done bool) {
		if limit > 0 {
			limit--
			return true, false
		}
		return false, true
	}
}

func filterColumn(col uint64) rowFilter {
	return func(rowID, key uint64, c *roaring.Container) (include, done bool) {
		colID := col % ShardWidth
		colKey := ((rowID * ShardWidth) + colID) >> 16
		colVal := uint16(colID & 0xFFFF) // columnID within the container
		return colKey == key && c.Contains(colVal), false
	}
}

// TODO: this works, but it would be more performant if the fragment could seek
// to the next row in the rows list rather than asking the filter for each
// container serially. The container iterator would need to expose a seek
// method, and the rowFilter would need some way of communicating to
// fragment.rows what the next rowID to seek to is.
func filterWithRows(rows []uint64) rowFilter {
	loc := 0
	return func(rowID, key uint64, c *roaring.Container) (include, done bool) {
		if loc >= len(rows) {
			return false, true
		}
		i := sort.Search(len(rows[loc:]), func(i int) bool {
			return rows[loc+i] >= rowID
		})
		loc += i
		if loc >= len(rows) {
			return false, true
		}
		if rows[loc] == rowID {
			if loc == len(rows)-1 {
				done = true
			}
			return true, done
		}
		return false, false
	}
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
func (f *fragment) rows(start uint64, filters ...rowFilter) []uint64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.unprotectedRows(start, filters...)
}

// unprotectedRows calls rows without grabbing the mutex.
func (f *fragment) unprotectedRows(start uint64, filters ...rowFilter) []uint64 {
	startKey := rowToKey(start)
	i, _ := f.storage.Containers.Iterator(startKey)
	rows := make([]uint64, 0)
	var lastRow uint64 = math.MaxUint64

	// Loop over the existing containers.
	for i.Next() {
		key, c := i.Value()

		// virtual row for the current container
		vRow := key >> shardVsContainerExponent

		// skip dups
		if vRow == lastRow {
			continue
		}

		// apply filters
		addRow, done := true, false
		for _, filter := range filters {
			var d bool
			addRow, d = filter(vRow, key, c)
			done = done || d
			if !addRow {
				break
			}
		}
		if addRow {
			lastRow = vRow
			rows = append(rows, vRow)
		}
		if done {
			return rows
		}
	}
	return rows
}

// upgradeRoaringBSIv2 upgrades a fragment that contains old BSI formatting
// to a new BSI format (v2). The new format moves the "exists" bit to the
// beginning & adds a negative sign bit.
func upgradeRoaringBSIv2(f *fragment, bitDepth uint) (string, error) {
	// If flag set, already upgraded. Exit.
	if f.storage.Flags&roaringFlagBSIv2 == 1 {
		return "", nil
	}

	other := roaring.NewBitmap()
	other.Flags = roaringFlagBSIv2
	func() {
		f.mu.Lock()
		defer f.mu.Unlock()

		f.storage.ForEach(func(i uint64) {
			rowID, columnID := i/ShardWidth, (f.shard*ShardWidth)+(i%ShardWidth)
			if rowID == uint64(bitDepth) {
				_, _ = other.Add(pos(bsiExistsBit, columnID)) // move exists bit to beginning
			} else {
				_, _ = other.Add(pos(rowID+bsiOffsetBit, columnID)) // move other bits up
			}
		})
	}()

	// Create temporary file next to existing file.
	newPath := f.path + ".tmp"
	file, err := os.OpenFile(newPath, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return "", err
	}
	defer file.Close()

	// Write & flush to temporary file.
	if _, err := other.WriteTo(file); err != nil {
		return "", err
	} else if err := file.Sync(); err != nil {
		return "", err
	} else if err := file.Close(); err != nil {
		return "", err
	}

	return newPath, nil
}

type rowIterator struct {
	f      *fragment
	rowIDs []uint64
	cur    int
	wrap   bool
}

func (f *fragment) rowIterator(wrap bool, filters ...rowFilter) *rowIterator {
	return &rowIterator{
		f:      f,
		rowIDs: f.rows(0, filters...), // TODO: this may be memory intensive in high cardinality cases
		wrap:   wrap,
	}
}

func (ri *rowIterator) Seek(rowID uint64) {
	idx := sort.Search(len(ri.rowIDs), func(i int) bool {
		return ri.rowIDs[i] >= rowID
	})
	ri.cur = idx
}

func (ri *rowIterator) Next() (r *Row, rowID uint64, wrapped bool) {
	if ri.cur >= len(ri.rowIDs) {
		if !ri.wrap || len(ri.rowIDs) == 0 {
			return nil, 0, true
		}
		ri.Seek(0)
		wrapped = true
	}
	rowID = ri.rowIDs[ri.cur]
	r = ri.f.row(rowID)
	ri.cur++
	return r, rowID, wrapped
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
	_, _ = h.hash.Write(h.buf[:])
}

// fragmentSyncer syncs a local fragment to one on a remote host.
type fragmentSyncer struct {
	Fragment *fragment

	Node    *Node
	Cluster *cluster

	Closing <-chan struct{}
}

// isClosing returns true if the closing channel is closed.
func (s *fragmentSyncer) isClosing() bool {
	select {
	case <-s.Closing:
		return true
	default:
		return false
	}
}

// syncFragment compares checksums for the local and remote fragments and
// then merges any blocks which have differences.
func (s *fragmentSyncer) syncFragment() error {
	span, ctx := tracing.StartSpanFromContext(context.Background(), "FragmentSyncer.syncFragment")
	defer span.Finish()

	// Determine replica set.
	nodes := s.Cluster.shardNodes(s.Fragment.index, s.Fragment.shard)
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
		blocks, err := s.Cluster.InternalClient.FragmentBlocks(ctx, &node.URI, s.Fragment.index, s.Fragment.field, s.Fragment.view, s.Fragment.shard)
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
func (s *fragmentSyncer) syncBlock(id int) error {
	span, ctx := tracing.StartSpanFromContext(context.Background(), "FragmentSyncer.syncBlock")
	defer span.Finish()

	f := s.Fragment

	// Read pairs from each remote block.
	var uris []*URI
	var pairSets []pairSet
	for _, node := range s.Cluster.shardNodes(f.index, f.shard) {
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
		rowIDs, columnIDs, err := s.Cluster.InternalClient.BlockData(ctx, &node.URI, f.index, f.field, f.view, f.shard, id)
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

		// Handle Sets.
		if len(set.columnIDs) > 0 {
			setData, err := bitsToRoaringData(set)
			if err != nil {
				return errors.Wrap(err, "converting bits to roaring data (set)")
			}

			setReq := &ImportRoaringRequest{
				Clear: false,
				Views: map[string][]byte{cleanViewName(f.view): setData},
			}

			if err := s.Cluster.InternalClient.ImportRoaring(ctx, uris[i], f.index, f.field, f.shard, true, setReq); err != nil {
				return errors.Wrap(err, "sending roaring data (set)")
			}
		}

		// Handle Clears.
		if len(clear.columnIDs) > 0 {
			clearData, err := bitsToRoaringData(clear)
			if err != nil {
				return errors.Wrap(err, "converting bits to roaring data (clear)")
			}

			clearReq := &ImportRoaringRequest{
				Clear: true,
				Views: map[string][]byte{"": clearData},
			}

			if err := s.Cluster.InternalClient.ImportRoaring(ctx, uris[i], f.index, f.field, f.shard, true, clearReq); err != nil {
				return errors.Wrap(err, "sending roaring data (clear)")
			}
		}
	}

	return nil
}

// cleanViewName converts a viewname into the equivalent
// string required by the external api. Because views are
// not exposed externally, the conversion looks like this:
// "standard" -> ""
// "standard_YYYYMMDD" -> "YYYYMMDD"
// "other" -> "other" (there is currently not a use for this)
func cleanViewName(v string) string {
	viewPrefix := viewStandard + "_"
	if strings.HasPrefix(v, viewPrefix) {
		return v[len(viewPrefix):]
	} else if v == viewStandard {
		return ""
	}
	return v
}

// bitsToRoaringData converts a pairSet into a roaring.Bitmap
// which represents the data within a single shard.
func bitsToRoaringData(ps pairSet) ([]byte, error) {
	bmp := roaring.NewBitmap()
	for j := 0; j < len(ps.columnIDs); j++ {
		bmp.DirectAdd(ps.rowIDs[j]*ShardWidth + (ps.columnIDs[j] % ShardWidth))
	}

	var buf bytes.Buffer
	_, err := bmp.WriteTo(&buf)
	if err != nil {
		return nil, errors.Wrap(err, "writing to buffer")
	}

	return buf.Bytes(), nil
}

func madvise(b []byte, advice int) error { // nolint: unparam
	_, _, err := syscall.Syscall(syscall.SYS_MADVISE, uintptr(unsafe.Pointer(&b[0])), uintptr(len(b)), uintptr(advice))
	if err != 0 {
		return err
	}
	return nil
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
	return (rowID * ShardWidth) + (columnID % ShardWidth)
}

// vector stores the mapping of colID to rowID.
// It's used for a mutex field type.
type vector interface {
	Get(colID uint64) (uint64, bool, error)
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
func (v *rowsVector) Get(colID uint64) (uint64, bool, error) {
	rows := v.f.unprotectedRows(0, filterColumn(colID))
	if len(rows) > 1 {
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
func (v *boolVector) Get(colID uint64) (uint64, bool, error) {
	rows := v.f.unprotectedRows(0, filterColumn(colID))
	if len(rows) > 1 {
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
