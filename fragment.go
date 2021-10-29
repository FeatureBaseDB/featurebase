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
	"math/bits"
	"os"
	"path/filepath"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"github.com/cespare/xxhash"
	"github.com/gogo/protobuf/proto"
	"github.com/molecula/featurebase/v2/logger"
	pnet "github.com/molecula/featurebase/v2/net"
	"github.com/molecula/featurebase/v2/pb"
	"github.com/molecula/featurebase/v2/pql"
	"github.com/molecula/featurebase/v2/roaring"
	"github.com/molecula/featurebase/v2/shardwidth"
	"github.com/molecula/featurebase/v2/stats"
	"github.com/molecula/featurebase/v2/storage"
	"github.com/molecula/featurebase/v2/testhook"
	"github.com/molecula/featurebase/v2/topology"
	"github.com/molecula/featurebase/v2/tracing"
	. "github.com/molecula/featurebase/v2/vprint" // nolint:staticcheck
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

	// cacheExt is the file extension for persisted cache ids.
	cacheExt = ".cache"

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

// fragSpec saves a ton of duplicated strings for
// path, and index, field, view name strings.
type fragSpec struct {
	index    *Index
	field    *Field
	fieldstr string
	view     *view
}

func (f *fragment) index() string {
	return f.idx.name
}

func (f *fragment) field() string {
	// initialization of _field *Field can
	// go missing during tests that do incomplete setup.
	// Hence we must keep fieldstr as a backup.
	if f.fld == nil {
		return f.fieldstr
	} else {
		f.fieldstr = ""
	}
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
	fld      *Field
	fieldstr string
	_view    *view

	shard uint64

	// idx cached to avoid repeatedly looking it up everywhere.
	idx *Index

	// parent holder, used to find snapshot queue, etc.
	holder *Holder

	// debugging tool: addresses of current and previous maps
	prevdata, currdata struct{ from, to uintptr }

	// File-backed storage
	flags           byte // user-defined flags passed to roaring
	gen             generation
	storage         *roaring.Bitmap
	opN             int  // number of ops since snapshot (may be approximate for imports)
	ops             int  // number of higher-level operations, as opposed to bit changes
	snapshotPending bool // set to true when requesting a snapshot, set to false after snapshot completes
	snapshotCond    sync.Cond
	snapshotErr     error     // error yielded by the last snapshot operation
	snapshotStamp   time.Time // timestamp of last snapshot
	open            bool      // is this fragment actually open?

	// Cache for row counts.
	CacheType string // passed in by field

	// cache keeps a local rowid,count ranking:
	// telling us which is the most populated rows in that field.
	// Is only on "set fields" with rowCache enabled. So
	// BSI, mutex, bool fields do not have this.
	// Good: it Only has a string and a count, so cannot use Tx memory.
	cache cache

	CacheSize uint32

	// Cache containing full rows (not just counts).
	rowCache *simpleCache

	// Cached checksums for each block.
	checksums map[int][]byte

	// Number of operations performed before performing a snapshot.
	// This limits the size of fragments on the heap and flushes them to disk
	// so that they can be mmapped and heap utilization can be kept low.
	MaxOpN int

	// Logger used for out-of-band log entries.
	Logger logger.Logger

	// mutexVector is used for mutex field types. It's checked for an
	// existing value (to clear) prior to setting a new value.
	mutexVector vector

	stats stats.StatsClient

	bitmapInfo *roaring.BitmapInfo
}

// newFragment returns a new instance of fragment.
func newFragment(holder *Holder, spec fragSpec, shard uint64, flags byte) *fragment {
	idx := holder.Index(spec.index.name)

	if idx == nil {
		PanicOn(fmt.Sprintf("got nil idx back for '%v' from holder!", spec.index))
	}

	f := &fragment{
		_view:    spec.view,
		fieldstr: spec.fieldstr,
		fld:      spec.field,
		shard:    shard,
		flags:    flags,
		idx:      idx,

		CacheType: DefaultCacheType,
		CacheSize: DefaultCacheSize,

		holder: holder,
		MaxOpN: defaultFragmentMaxOpN,

		stats: stats.NopStatsClient,
	}
	f.snapshotCond = sync.Cond{L: &f.mu}
	return f
}

// cachePath returns the path to the fragment's cache data.
func (f *fragment) cachePath() string { return f.path() + cacheExt }

func (f *fragment) bitDepth() (uint64, error) {
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
	BitmapInfo     roaring.BitmapInfo
	BlockChecksums []FragmentBlock `json:"BlockChecksums,omitempty"`
}

func (f *fragment) Index() *Index {
	return f.holder.Index(f.index())
}

func (f *fragment) inspect(params InspectRequestParams) (fi FragmentInfo) {
	if f.bitmapInfo == nil {
		fi.BitmapInfo = f.storage.Info(params.Containers)
	} else {
		fi.BitmapInfo = *f.bitmapInfo
	}
	if params.Checksum {
		fi.BlockChecksums, _ = f.Blocks()
	}
	return fi
}

// Open opens the underlying storage.
func (f *fragment) Open() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := func() error {
		// Initialize storage in a function so we can close if anything goes wrong.
		f.holder.Logger.Debugf("open storage for index/field/view/fragment: %s/%s/%s/%d", f.index(), f.field(), f.view(), f.shard)
		if err := f.openStorage(true); err != nil {
			return errors.Wrap(err, "opening storage")
		}

		// Fill cache with rows persisted to disk.
		f.holder.Logger.Debugf("open cache for index/field/view/fragment: %s/%s/%s/%d", f.index(), f.field(), f.view(), f.shard)
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
	f.open = true

	_ = testhook.Opened(f.holder.Auditor, f, nil)
	f.holder.Logger.Debugf("successfully opened index/field/view/fragment: %s/%s/%s/%d", f.index(), f.field(), f.view(), f.shard)
	return nil
}

// emptyStorage is the common case for importStorage/applyStorage where they
// get no data. It tries to write the current storage to the provided file,
// which is assumed to be the file they didn't get any data from.
func (f *fragment) emptyStorage(file *os.File) (bool, error) {
	if f.holder.Opts.ReadOnly {
		return false, errors.New("can't flush/create storage for read-only holder")
	}
	// No data. We'll mark this for no mapping, clear any existing
	// mapped containers, and set the Source to nil. We also have no
	// ops.
	f.opN = 0
	f.ops = 0
	f.storage.SetOps(0, 0)

	f.storage.PreferMapping(false)
	_, err := f.storage.RemapRoaringStorage(nil)
	f.storage.SetSource(nil)
	if err != nil {
		return false, fmt.Errorf("applying/importing storage: no data, and clearing old mapping also failed: %v", err)
	}
	// Write the existing storage out to the file so it's
	// a valid Roaring file thereafter. nothing to unmarshal.
	// In the unlikely event that this happened even though we
	// had significant data, we're not mapping it, but that's
	// harmless even if it's not maximally efficient.
	bi := bufio.NewWriter(file)
	if _, err = f.storage.WriteTo(bi); err != nil {
		return false, fmt.Errorf("init storage file: %s", err)
	}
	bi.Flush()
	return false, nil
}

// importStorage attempts to import data from storage -- for instance,
// reading in a roaring bitmap from media.
func (f *fragment) importStorage(data []byte, file *os.File, newGen generation, mapped bool) (bool, error) {
	f.storage.PreferMapping(mapped)
	if len(data) == 0 {
		return f.emptyStorage(file)
	}

	// UnmarshalBinary will have remapped the storage to newGen if it
	// succeeded, or if it fails but the error is advisory-only. So we
	// optimistically set the source here, but if there's a non-advisory
	// error, we'll unmap it and then set the source to nil.
	f.storage.SetSource(newGen)
	if err := f.storage.UnmarshalBinary(data); err != nil {
		// roaring can report advisory-only errors...
		cause := errors.Cause(err)
		_, ok := cause.(roaring.AdvisoryError)
		if !ok {
			_, e2 := f.storage.RemapRoaringStorage(nil)
			f.storage.SetSource(nil)
			if e2 != nil {
				return false, fmt.Errorf("unmarshal storage: file=%s, err=%s, clearing old mapping also failed: %v", file.Name(), err, e2)
			}
			return false, fmt.Errorf("unmarshal storage: file=%s, err=%s", file.Name(), err)
		}
		f.holder.Logger.Warnf("unmarshal storage, file=%s, err=%v", file.Name(), err)
		trunc, ok := cause.(roaring.FileShouldBeTruncatedError)
		if ok && !f.holder.Opts.ReadOnly {
			// if the holder is ReadOnly, we silently ignore the "advisory"
			// error. This may be a bad idea.

			// generation code looks for a FileShouldBeTruncatedError
			return false, trunc
		}
	}
	f.ops, f.opN = f.storage.Ops()
	// For now, we assume that UnmarshalBinary will have mapped at least
	// one container if we told it the storage was mapped and it didn't
	// error out. This might be wrong in occasional trivial cases, but
	// it should be harmless.
	return mapped, nil
}

// applyStorage applies storage to a fragment that may already have
// usable data. For instance, this would try to remap existing containers
// to use a new storage as backing store.
func (f *fragment) applyStorage(data []byte, file *os.File, newGen generation, mapped bool) (bool, error) {
	if len(data) == 0 {
		// This shouldn't be used anyway in this path, but just in
		// case, we'll be explicit about it.
		f.storage.PreferMapping(false)
		if file != nil {
			fi, err := file.Stat()
			if err != nil {
				f.holder.Logger.Errorf("trying to apply new storage to existing bitmap, stat failed: %v", err)
			}
			if err == nil && fi != nil && fi.Size() == 0 {
				return f.emptyStorage(file)
			}
		}
		// if we can't be sure of that, we assume data is 0 because
		// we couldn't mmap it, and since all we'd be doing is remapping
		// our containers to use that storage *to take advantage of
		// mmap*, we'll just make sure our containers aren't pointing to
		// old storage and say "nope".
		_, _ = f.storage.RemapRoaringStorage(nil)
		f.storage.SetSource(nil)
		return false, nil
	}
	// Tell storage to prefer mapping if and only if we think the data
	// is mmapped and valid.
	f.storage.PreferMapping(mapped)
	// RemapRoaringStorage will fix any mapped containers to point either
	// to the provided data (if PreferMapping was called with true and
	// data is provided and there's a corresponding container) or to
	// allocated storage, so when it's done, there's nothing in it that
	// is mapped to anything *other than* the provided data.
	mapped, err := f.storage.RemapRoaringStorage(data)
	if err != nil {
		// OOPS! something went wrong, we don't know why, we can't
		// sanely recover from that.
		_, _ = f.storage.RemapRoaringStorage(nil)
		mapped = false
		f.storage.SetSource(nil)
	} else {
		f.storage.SetSource(newGen)
	}
	return mapped, err
}

func (f *fragment) inspectStorage(data []byte, file *os.File, newGen generation, mapped bool) (didMap bool, err error) {
	f.bitmapInfo = &roaring.BitmapInfo{}
	f.storage, didMap, err = roaring.InspectBinary(data, mapped, f.bitmapInfo)
	return didMap, err
}

// openStorage opens the storage bitmap.
//
// This has been massively reworked recently, and now hands a lot of
// file management off to the generation object and the Done method
// of that object. Similarly, the bitmap mapping/remapping
// logic is now mostly in importStorage (reading in a bitmap) and applyStorage
// (remapping an existing bitmap to match a new backing store).
func (f *fragment) openStorage(unmarshalData bool) error {

	useRowCache := storage.RowCacheEnabled()
	if !f.idx.NeedsSnapshot() {
		f.gen = &NopGeneration{}
		if useRowCache {
			f.rowCache = newSimpleCache()
		}
		f.currdata = struct{ from, to uintptr }{}
		f.prevdata = f.currdata
		return nil // openStorage becomes a noop under RBF, Badger, etc.
	}

	// Create a roaring bitmap to serve as storage for the shard.
	if f.storage == nil {
		f.storage = roaring.NewFileBitmap()
		f.storage.Flags = f.flags
		// if we didn't actually have storage, we *do* need to
		// unmarshal this data in order to have any.
		unmarshalData = true
	}
	if useRowCache {
		f.rowCache = newSimpleCache()
	}
	var storageOp func([]byte, *os.File, generation, bool) (bool, error)
	if f.holder.Opts.Inspect {
		// note that this will unmarshal even if we already have
		// storage; when Inspect is on for a holder, we actually want
		// to be able to report this.
		storageOp = f.inspectStorage
	} else {
		if unmarshalData {
			storageOp = f.importStorage
		} else {
			storageOp = f.applyStorage
		}
	}
	var err error
	f.gen, err = newGeneration(f.gen, f.path(), unmarshalData, storageOp, f.holder.Logger)
	if f.gen != nil {
		scratchData := f.gen.Bytes()
		f.prevdata = f.currdata
		var scratchAddrs struct{ from, to uintptr }
		if scratchData != nil {
			scratchAddrs.from = uintptr(unsafe.Pointer(&scratchData[0]))
			scratchAddrs.to = scratchAddrs.from + uintptr(len(scratchData))
		}
		f.currdata = scratchAddrs
	}
	if generationDebug {
		// We might have already done this anyway, if we think we
		// mapped stuff, but when debugging we want to do it
		// unconditionally, because the test cases otherwise won't
		// exercise this code well.
		f.storage.SetSource(f.gen)
	}
	return err
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
	for f.snapshotPending {
		f.snapshotCond.Wait()
	}
	// Note: snapshots won't progress on a closed fragment, so we
	// wait until after a possible pending snapshot to close.
	f.open = false
	return f.close()
}

func (f *fragment) close() error {
	// Flush cache if closing gracefully.
	if err := f.flushCache(); err != nil {
		f.holder.Logger.Errorf("fragment: error flushing cache on close: err=%s, path=%s", err, f.path())
		return errors.Wrap(err, "flushing cache")
	}

	// Close underlying storage.
	if err := f.closeStorage(); err != nil {
		f.holder.Logger.Errorf("fragment: error closing storage: err=%s, path=%s", err, f.path())
		return errors.Wrap(err, "closing storage")
	}

	// Remove checksums.
	f.checksums = nil

	return nil
}

// closeStorage marks the current generation as done. It is not necessary
// to call this before openStorage.
func (f *fragment) closeStorage() error {
	// opN is determined by how many bit set/clear operations are in the storage
	// write log, so once the storage is closed it should be 0. Opening new
	// storage will set opN appropriately.
	f.opN = 0

	if f.gen != nil {
		f.gen.Done()
	}
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
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.unprotectedRow(tx, rowID)
}

// mustRow returns a row by ID. Panic on error. Only used for testing.
func (f *fragment) mustRow(tx Tx, rowID uint64) *Row {
	row, err := f.row(tx, rowID)
	if err != nil {
		PanicOn(err)
	}
	return row
}

// unprotectedRow returns a row from the row cache if available or from storage
// (updating the cache).
func (f *fragment) unprotectedRow(tx Tx, rowID uint64) (*Row, error) {

	useRowCache := storage.RowCacheEnabled()
	if useRowCache {
		if f.rowCache == nil {
			f.rowCache = newSimpleCache()
		}
		r, ok := f.rowCache.Fetch(rowID)
		if ok && r != nil {
			return r, nil
		}
	}
	row, err := f.rowFromStorage(tx, rowID)
	if err != nil {
		return nil, err
	}
	if useRowCache {
		f.rowCache.Add(rowID, row)
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
		segments: []rowSegment{{
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
	var wp *io.Writer
	if f.storage != nil {
		wp = &f.storage.OpWriter
	}
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
	// avoid crashing when f.gen is nil
	if f.gen != nil {
		err = f.gen.Transaction(wp, doSetFunc)
	} else {
		if tx.Type() == RoaringTxn {
			return changed, errors.New("internal error: f.gen was nil and tx.Type is RoaringTxn - should never happen under roaring b/c storage should be open")
		}
		// else transactional backend. Just do it.
		err = doSetFunc()
	}
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

	// Increment number of operations until snapshot is required.
	f.incrementOpN(1)

	// If we're using a cache, update it. Otherwise skip the
	// possibly-expensive count operation.
	if f.CacheType != CacheTypeNone {
		n, err := tx.CountRange(f.index(), f.field(), f.view(), f.shard, rowID*ShardWidth, (rowID+1)*ShardWidth)
		if err != nil {
			return false, err
		}
		f.cache.Add(rowID, n)
	}
	// Drop the rowCache entry; it's wrong, and we don't want to force
	// a new copy if no one's reading it.
	if storage.RowCacheEnabled() && f.rowCache != nil {
		f.rowCache.Add(rowID, nil)
	}

	f.stats.Count(MetricSetBit, 1, 1.0)

	return changed, nil
}

// clearBit clears a bit for a given column & row within the fragment.
// This updates both the on-disk storage and the in-cache bitmap.
func (f *fragment) clearBit(tx Tx, rowID, columnID uint64) (changed bool, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var wp *io.Writer
	if f.storage != nil {
		wp = &f.storage.OpWriter
	}
	err = f.gen.Transaction(wp, func() error {
		changed, err = f.unprotectedClearBit(tx, rowID, columnID)
		return err
	})
	return changed, err
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

	// Increment number of operations until snapshot is required.
	f.incrementOpN(1)

	// If we're using a cache, update it. Otherwise skip the
	// possibly-expensive count operation.
	if f.CacheType != CacheTypeNone {
		n, err := tx.CountRange(f.index(), f.field(), f.view(), f.shard, rowID*ShardWidth, (rowID+1)*ShardWidth)
		if err != nil {
			return changed, err
		}
		f.cache.Add(rowID, n)
	}
	// Drop the rowCache entry; it's wrong, and we don't want to force
	// a new copy if no one's reading it.
	if storage.RowCacheEnabled() && f.rowCache != nil {
		f.rowCache.Add(rowID, nil)
	}

	f.stats.Count(MetricClearBit, 1, 1.0)

	return changed, nil
}

// setRow replaces an existing row (specified by rowID) with the given
// Row. This updates both the on-disk storage and the in-cache bitmap.
func (f *fragment) setRow(tx Tx, row *Row, rowID uint64) (changed bool, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var wp *io.Writer
	if f.storage != nil {
		wp = &f.storage.OpWriter
	}
	err = f.gen.Transaction(wp, func() error {
		changed, err = f.unprotectedSetRow(tx, row, rowID)
		return err
	})
	return changed, err
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

	// invalidate rowCache for this row.
	if storage.RowCacheEnabled() && f.rowCache != nil {
		f.rowCache.Add(rowID, nil)
	}

	// Snapshot storage.
	f.holder.SnapshotQueue.Enqueue(f)
	f.stats.Count("setRow", 1, 1.0)

	return changed, nil
}

// clearRow clears a row for a given rowID within the fragment.
// This updates both the on-disk storage and the in-cache bitmap.
func (f *fragment) clearRow(tx Tx, rowID uint64) (changed bool, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var wp *io.Writer
	if f.storage != nil {
		wp = &f.storage.OpWriter
	}
	err = f.gen.Transaction(wp, func() error {
		changed, err = f.unprotectedClearRow(tx, rowID)
		return err
	})
	return changed, err
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
	if storage.RowCacheEnabled() && f.rowCache != nil {
		f.rowCache.Add(rowID, nil)
	}

	// Snapshot storage.
	f.holder.SnapshotQueue.Enqueue(f)

	return changed, nil
}

// unprotectedClearBlock clears all rows for a given block.
// This updates both the on-disk storage and the in-cache bitmap.
func (f *fragment) unprotectedClearBlock(tx Tx, block int) (changed bool, err error) {
	firstRow := uint64(block * HashBlockSize)
	var wp *io.Writer
	if f.storage != nil {
		wp = &f.storage.OpWriter
	}
	err = f.gen.Transaction(wp, func() error {
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
	})
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
func (f *fragment) setValueBase(txOrig Tx, columnID uint64, bitDepth uint64, value int64, clear bool) (changed bool, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	tx := txOrig
	if NilInside(tx) {
		tx = f.idx.holder.txf.NewTx(Txo{Write: writable, Index: f.idx, Fragment: f, Shard: f.shard})
		defer func() {
			if err == nil {
				PanicOn(tx.Commit())
			} else {
				tx.Rollback()
			}
		}()
	}

	var wp *io.Writer
	if f.storage != nil {
		wp = &f.storage.OpWriter
	}
	err = f.gen.Transaction(wp, func() error {
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
	})
	return changed, err
}

// sum returns the sum of a given bsiGroup as well as the number of columns involved.
// A bitmap can be passed in to optionally filter the computed columns.
func (f *fragment) sum(tx Tx, filter *Row, bitDepth uint64) (sum int64, count uint64, err error) {
	// Compute count based on the existence row.
	consider, err := f.row(tx, bsiExistsBit)
	if err != nil {
		return sum, count, err
	} else if filter != nil {
		consider = consider.Intersect(filter)
	}
	count = consider.Count()

	// Get negative set
	nrow, err := f.row(tx, bsiSignBit)
	if err != nil {
		return sum, count, err
	}

	// Filter negative set
	nrow = consider.Intersect(nrow)

	// Get postive set
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
	for i := uint64(0); i < bitDepth; i++ {
		row, err := f.row(tx, uint64(bsiOffsetBit+i))
		if err != nil {
			return sum, count, err
		}

		psum := int64((1 << i) * row.intersectionCount(prow))
		nsum := int64((1 << i) * row.intersectionCount(nrow))

		// Squash to reduce the possibility of overflow.
		sum += psum - nsum
	}

	return sum, count, nil
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

// forEachBit executes fn for every bit set in the fragment.
// Errors returned from fn are passed through.
func (f *fragment) forEachBit(tx Tx, fn func(rowID, columnID uint64) error) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return tx.ForEach(f.index(), f.field(), f.view(), f.shard, func(i uint64) error {
		return fn(i/ShardWidth, (f.shard*ShardWidth)+(i%ShardWidth))
	})
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

// Checksum returns a checksum for the entire fragment.
// If two fragments have the same checksum then they have the same data.
func (f *fragment) Checksum() ([]byte, error) {
	h := xxhash.New()

	blocks, err := f.Blocks()
	if err != nil {
		return nil, err
	}

	for _, block := range blocks {
		_, _ = h.Write(block.Checksum)
	}
	return h.Sum(nil), nil
}

// InvalidateChecksums clears all cached block checksums.
func (f *fragment) InvalidateChecksums() {
	f.mu.Lock()
	f.checksums = make(map[int][]byte)
	f.mu.Unlock()
}

// Blocks returns info for all blocks containing data.
func (f *fragment) Blocks() ([]FragmentBlock, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	var a []FragmentBlock

	idx := f.holder.Index(f.index())
	if idx == nil {
		err := fmt.Errorf("index() was nil in fragment.Blocks(): f.index()='%v'", f.index())
		PanicOn(err)
		return nil, err
	}
	tx := idx.holder.txf.NewTx(Txo{Write: !writable, Index: idx, Fragment: f, Shard: f.shard})
	defer tx.Rollback()
	// no Commit below, b/c is read-only.

	itr := tx.NewTxIterator(f.index(), f.field(), f.view(), f.shard)
	defer itr.Close()

	itr.Seek(0)

	// Initialize block hasher.
	h := newBlockHasher()

	// Iterate over each value in the fragment.
	v, eof := itr.Next()
	if eof {
		return nil, nil
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
		f.checksums[h.blockID] = chksum // the only place checksums is added to.

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

	return a, nil
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
func (f *fragment) blockData(id int) (rowIDs, columnIDs []uint64, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	idx := f.holder.Index(f.index())
	tx := idx.holder.txf.NewTx(Txo{Write: !writable, Index: idx, Shard: f.shard})
	defer tx.Rollback()
	// readonly, so no Commit()

	if err := tx.ForEachRange(f.index(), f.field(), f.view(), f.shard, uint64(id)*HashBlockSize*ShardWidth, (uint64(id)+1)*HashBlockSize*ShardWidth, func(i uint64) error {
		rowIDs = append(rowIDs, i/ShardWidth)
		columnIDs = append(columnIDs, i%ShardWidth)
		return nil
	}); err != nil {
		return nil, nil, err
	}
	return rowIDs, columnIDs, nil
}

// mergeBlock compares the block's bits and computes a diff with another set of block bits.
// The state of a bit is determined by consensus from all blocks being considered.
//
// For example, if 3 blocks are compared and two have a set bit and one has a
// cleared bit then the bit is considered set. The function returns the
// diff per incoming block so that all can be in sync.
func (f *fragment) mergeBlock(tx Tx, id int, data []pairSet) (sets, clears []pairSet, err error) {
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
	maxRowID := (uint64(id+1) * HashBlockSize) - 1
	maxColumnID := uint64(ShardWidth) - 1

	// Create buffered iterator for local block.
	bm, err := tx.RoaringBitmap(f.index(), f.field(), f.view(), f.shard)
	if err != nil {
		return nil, nil, err
	}
	itrs := make([]*bufIterator, 1, len(data)+1)
	itrs[0] = newBufIterator(
		newLimitIterator(
			newRoaringIterator(bm.Iterator()), maxRowID, maxColumnID,
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
				clears[i].rowIDs = append(clears[i].rowIDs, min.rowID)
				clears[i].columnIDs = append(clears[i].columnIDs, min.columnID)
			}
		}
	}

	rowSet := make(map[uint64]struct{}, len(sets[0].columnIDs))
	// compute positions directly, replacing columnIDs with the computed
	// positions
	for i := range sets[0].columnIDs {
		rowSet[sets[0].rowIDs[i]] = struct{}{}
		sets[0].columnIDs[i] += sets[0].rowIDs[i] * ShardWidth
	}
	for i := range clears[0].columnIDs {
		rowSet[clears[0].rowIDs[i]] = struct{}{}
		clears[0].columnIDs[i] += clears[0].rowIDs[i] * ShardWidth
	}
	err = f.importPositions(tx, sets[0].columnIDs, clears[0].columnIDs, rowSet)

	return sets[1:], clears[1:], err
}

// bulkImport bulk imports a set of bits and then snapshots the storage.
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
		PanicOn("parallelSlices must have same length for rows and columns")
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
	//tx.AddN()
	var wp *io.Writer
	if f.storage != nil {
		wp = &f.storage.OpWriter
	}
	useRowCache := storage.RowCacheEnabled()

	doFunc := func() error {
		if len(set) > 0 {
			f.stats.Count(MetricImportingN, int64(len(set)), 1)

			// TODO benchmark Add/RemoveN behavior with sorted/unsorted positions
			changedN, err := tx.Add(f.index(), f.field(), f.view(), f.shard, set...)
			if err != nil {
				return errors.Wrap(err, "adding positions")
			}
			f.stats.Count(MetricImportedN, int64(changedN), 1)
			f.incrementOpN(changedN)
		}

		if len(clear) > 0 {
			f.stats.Count(MetricClearingN, int64(len(clear)), 1)
			changedN, err := tx.Remove(f.index(), f.field(), f.view(), f.shard, clear...)
			if err != nil {
				return errors.Wrap(err, "clearing positions")
			}
			f.stats.Count(MetricClearedN, int64(changedN), 1)
			f.incrementOpN(changedN)
		}

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
			if useRowCache && f.rowCache != nil {
				f.rowCache.Add(rowID, nil)
			}
		}

		if f.CacheType != CacheTypeNone {
			f.cache.Invalidate()
		}
		return nil
	}
	var err error
	if f.gen != nil {
		err = f.gen.Transaction(wp, doFunc)
	} else {
		if tx.Type() == RoaringTxn {
			return errors.New("internal error: f.gen was nil and tx.Type is RoaringTxn - should never happen under roaring b/c storage should be open")
		}
		err = doFunc()
	}

	if err != nil {
		// we got an error. it's possible that the error indicates that something went wrong.
		mappedIn, mappedOut, unmappedIn, errs, e2 := f.storage.SanityCheckMapping(f.currdata.from, f.currdata.to)
		if errs != 0 {
			f.holder.Logger.Errorf("transaction failed on %s. storage has %d mapped in range, %d mapped out of range, %d unmapped in range, %d errors total, last %v",
				f.path(), mappedIn, mappedOut, unmappedIn, errs, e2)
			if f.prevdata.from != f.currdata.from {
				mappedIn, mappedOut, unmappedIn, errs, e2 = f.storage.SanityCheckMapping(f.prevdata.from, f.prevdata.to)
				f.holder.Logger.Errorf("with previous map, storage would have %d mapped in range, %d mapped out of range, %d unmapped in range, %d errors total, last %v",
					mappedIn, mappedOut, unmappedIn, errs, e2)
			}
		}
	}
	return err
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
	// we'll reuse the row IDs as the values to clear, if any.
	toClear := columnIDs[:0]
	callback := func(pos uint64) error {
		toClear = append(toClear, pos)
		rowID := pos / ShardWidth
		rowSet[rowID] = struct{}{}
		return nil
	}
	findExisting := roaring.NewBitmapBitmapFilter(columns, callback)
	err := tx.ApplyFilter(f.index(), f.field(), f.view(), f.shard, 0, findExisting)
	if err != nil {
		return errors.Wrap(err, "finding existing positions")
	}
	// if we're clearing things, anything being set that is being cleared
	// should not be cleared
	if len(toClear) > 0 {
		toClear = sliceDifference(toClear, toSet)
	}
	return errors.Wrap(f.importPositions(tx, toSet, toClear, rowSet), "importing positions")
}

// ClearRecords deletes all bits for the given records. It's basically
// the remove-only part of setting a mutex.
func (f *fragment) ClearRecords(tx Tx, recordIDs []uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// create a mask of columns we care about
	columns := roaring.NewSliceBitmap(recordIDs...)

	// we now need to find existing rows for these bits.
	rowSet := make(map[uint64]struct{})
	var toClear []uint64
	callback := func(pos uint64) error {
		toClear = append(toClear, pos)
		rowID := pos / ShardWidth
		rowSet[rowID] = struct{}{}
		return nil
	}
	findExisting := roaring.NewBitmapBitmapFilter(columns, callback)
	err := tx.ApplyFilter(f.index(), f.field(), f.view(), f.shard, 0, findExisting)
	if err != nil {
		return errors.Wrap(err, "finding existing positions")
	}
	return errors.Wrap(f.importPositions(tx, nil, toClear, rowSet), "clearing records")
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
	span, ctx = tracing.StartSpanFromContext(ctx, "importRoaring.AcquireFragmentLock")
	f.mu.Lock()
	defer f.mu.Unlock()
	span.Finish()

	return f.unprotectedImportRoaring(ctx, tx, data, clear)
}

func (f *fragment) unprotectedImportRoaring(ctx context.Context, tx Tx, data []byte, clear bool) error {
	rowSize := uint64(1 << shardVsContainerExponent)
	span, ctx := tracing.StartSpanFromContext(ctx, "importRoaring.ImportRoaringBits")

	useRowCache := storage.RowCacheEnabled()
	var changed int
	var rowSet map[uint64]int
	var wp *io.Writer
	if f.storage != nil {
		wp = &f.storage.OpWriter
	}
	err := f.gen.Transaction(wp, func() (err error) {
		var rit roaring.RoaringIterator
		rit, err = roaring.NewRoaringIterator(data)
		if err != nil {
			return err
		}

		changed, rowSet, err = tx.ImportRoaringBits(f.index(), f.field(), f.view(), f.shard, rit, clear, true, rowSize)
		return err
	})

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
		if useRowCache && f.rowCache != nil {
			f.rowCache.Add(rowID, nil)
		}
		if updateCache {
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
	}
	// we only set this if we need to update the cache
	if anyChanged {
		f.cache.Invalidate()
	}

	span, _ = tracing.StartSpanFromContext(ctx, "importRoaring.incrementOpN")

	f.incrementOpN(changed)

	span.Finish()
	return nil
}

// importRoaringOverwrite overwrites the specified block with the provided data.
func (f *fragment) importRoaringOverwrite(ctx context.Context, tx Tx, data []byte, block int) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Clear the existing data from fragment block.
	if _, err := f.unprotectedClearBlock(tx, block); err != nil {
		return errors.Wrapf(err, "clearing block: %d", block)
	}

	// Union the new block data with the fragment data.
	return f.unprotectedImportRoaring(ctx, tx, data, false)
}

// incrementOpN increase the operation count by one.
// If the count exceeds the maximum allowed then a snapshot is performed.
func (f *fragment) incrementOpN(changed int) {
	if changed <= 0 {
		return
	}
	// don't count opN or ops if our index doesn't want snapshots
	if !f.idx.NeedsSnapshot() {
		return
	}
	f.opN += changed
	f.ops++
	if f.opN > f.MaxOpN {
		f.holder.SnapshotQueue.Enqueue(f)
	}
}

// Snapshot writes the storage bitmap to disk and reopens it. This may
// coexist with existing background-queue snapshotting; it does not remove
// things from the queue. You probably don't want to do this; use
// the snapshotQueue's Enqueue/Await.
func (f *fragment) Snapshot() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.snapshot()
}

func track(start time.Time, message string, stats stats.StatsClient, logger logger.Logger) {
	elapsed := time.Since(start)
	logger.Debugf("%s took %s", message, elapsed)
	stats.Timing(MetricSnapshotDurationSeconds, elapsed, 1.0)
}

// snapshot does the actual snapshot operation. it does not check or care
// about f.snapshotPending.
func (f *fragment) snapshot() (err error) {
	if !f.idx.NeedsSnapshot() {
		return nil
	}
	if !f.open {
		return errors.New("snapshot request on closed fragment")
	}
	wouldPanic := debug.SetPanicOnFault(true)
	defer func() {
		debug.SetPanicOnFault(wouldPanic)
		if r := recover(); r != nil {
			if e2, ok := r.(error); ok {
				err = e2
				// special case: if we caught a page fault, we diagnose that directly. sadly,
				// we can't see the actual values that were used to generate this, probably.
				if e2.Error() == "runtime error: invalid memory address or nil pointer dereference" {
					mappedIn, mappedOut, unmappedIn, errs, _ := f.storage.SanityCheckMapping(f.currdata.from, f.currdata.to)
					f.holder.Logger.Errorf("transaction failed on %s. storage has %d mapped in range, %d mapped out of range, %d unmapped in range, %d errors total",
						f.path(), mappedIn, mappedOut, unmappedIn, errs)
				}
			} else {
				err = fmt.Errorf("non-error PanicOn: %v", r)
			}
		}
	}()
	_, err = unprotectedWriteToFragment(f, f.storage)
	if err == nil {
		f.snapshotStamp = time.Now()
	}
	return err
}

// unprotectedWriteToFragment writes the fragment f with bm as the data. It is unprotected, and
// f.mu must be locked when calling it.
func unprotectedWriteToFragment(f *fragment, bm *roaring.Bitmap) (n int64, err error) { // nolint: interfacer
	completeMessage := fmt.Sprintf("fragment: snapshot complete %s/%s/%s/%d", f.index(), f.field(), f.view(), f.shard)
	start := time.Now()
	defer track(start, completeMessage, f.stats, f.holder.Logger)

	// Create a temporary file to snapshot to.
	snapshotPath := f.path() + snapshotExt
	file, err := os.Create(snapshotPath)
	if err != nil {
		return n, fmt.Errorf("create snapshot file: %s", err)
	}
	// No deferred close, because we want to close it sooner than the
	// end of this function.

	// Write storage to snapshot.
	bw := bufio.NewWriter(file)
	if n, err = bm.WriteTo(bw); err != nil {
		file.Close()
		return n, fmt.Errorf("snapshot write to: %s", err)
	}

	if err := bw.Flush(); err != nil {
		file.Close()
		return n, fmt.Errorf("flush: %s", err)
	}

	// we close the file here so we don't still have it open when trying
	// to open it in a moment.
	file.Close()

	// Move snapshot to data file location.
	if err := os.Rename(snapshotPath, f.path()); err != nil {
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

	if err := os.MkdirAll(filepath.Dir(f.cachePath()), 0777); err != nil {
		return errors.Wrap(err, "mkdir")
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
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return errors.Wrap(err, "fillFragmentFromArchive ioutil.ReadAll(r)")
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

// blockToRoaringData converts a fragment block into a roaring.Bitmap
// which represents a portion of the data within a single shard.
// TODO: it seems like we should be able to get the
// block data as roaring without having to go through
// this rows/columns step.
func (f *fragment) blockToRoaringData(block int) ([]byte, error) {
	rowIDs, columnIDs, err := f.blockData(block)
	if err != nil {
		return nil, err
	}
	return bitsToRoaringData(pairSet{
		columnIDs: columnIDs,
		rowIDs:    rowIDs,
	})
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

	if storage.RowCacheEnabled() {
		// needs a write lock since it will update the f.rowCache
		f.mu.Lock()
		defer f.mu.Unlock()
	} else {
		f.mu.RLock()
		defer f.mu.RUnlock()
	}
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
	return tx.ApplyFilter(f.index(), f.field(), f.view(), f.shard, 0, filter)
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

	Node    *topology.Node
	Cluster *cluster

	// FieldType helps determine which method of syncing to use.
	FieldType string

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

	// Create a snapshot of the cluster to use for node/partition calculations.
	snap := topology.NewClusterSnapshot(s.Cluster.noder, s.Cluster.Hasher, s.Cluster.ReplicaN)

	// Determine replica set.
	nodes := snap.ShardNodes(s.Fragment.index(), s.Fragment.shard)
	if len(nodes) == 1 {
		return nil
	}

	// This is here solely to prevent unnecessary work;
	// if this node isn't the primary replica, there's no need
	// to continue processing int/decimal fields.
	if nodes[0].ID != s.Node.ID {
		switch s.FieldType {
		case FieldTypeInt, FieldTypeDecimal, FieldTypeTimestamp:
			return nil
		}
	}

	// Create a set of blocks.
	blockSets := make([][]FragmentBlock, 0, len(nodes))
	for _, node := range nodes {
		// Read local blocks.
		if node.ID == s.Node.ID {
			b, err := s.Fragment.Blocks() // comes from Tx store, creates its own Tx.
			if err != nil {
				return err
			}
			blockSets = append(blockSets, b)
			continue
		}

		// Retrieve remote blocks.
		blocks, err := s.Cluster.InternalClient.FragmentBlocks(ctx, &node.URI, s.Fragment.index(), s.Fragment.field(), s.Fragment.view(), s.Fragment.shard)
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

		// If we've gotten here, it means that the block differs
		// between nodes. If this particular fragment is part of an
		// `int` or `decimal` field, then instead of using a consensus
		// to determine which bits to update, we consider the primary
		// replica to be correct, and overwrite the non-primary replicas
		// with the primary's data.

		s.Fragment.holder.Logger.Debugf("sync block from primary: index='%v' field='%v' view='%v' shard='%v' id=%d", s.Fragment.index(), s.Fragment.field(), s.Fragment.view(), s.Fragment.shard, blockID)

		switch s.FieldType {
		case FieldTypeInt, FieldTypeDecimal, FieldTypeTimestamp:
			// Synchronize block from the primary replica.
			if err := s.syncBlockFromPrimary(blockID); err != nil {
				return fmt.Errorf("sync block from primary: id=%d, err=%s", blockID, err)
			}
			s.Fragment.stats.CountWithCustomTags(MetricBlockRepair, 1, 1.0, []string{"primary:true"})
		default:
			// Synchronize block.
			if err := s.syncBlock(blockID); err != nil {
				return fmt.Errorf("sync block: id=%d, err=%s", blockID, err)
			}
			s.Fragment.stats.CountWithCustomTags(MetricBlockRepair, 1, 1.0, []string{"primary:false"})
		}
	}

	return nil
}

// syncBlockFromPrimary sends all rows for a given block
// from the primary replica to non-primary replicas.
// Since this is pushing updates out to replicas, it only
// runs on the primary replica.
// Returns an error if any remote hosts are unreachable.
func (s *fragmentSyncer) syncBlockFromPrimary(id int) error {
	span, ctx := tracing.StartSpanFromContext(context.Background(), "FragmentSyncer.syncBlockFromPrimary")
	defer span.Finish()

	f := s.Fragment

	// Create a snapshot of the cluster to use for node/partition calculations.
	snap := topology.NewClusterSnapshot(s.Cluster.noder, s.Cluster.Hasher, s.Cluster.ReplicaN)

	// Determine replica set. Return early if this is not
	// the primary node.
	nodes := snap.ShardNodes(f.index(), f.shard)
	if s.Node.ID != nodes[0].ID {
		f.holder.Logger.Debugf("non-primary replica expecting sync from primary: %s, index=%s, field=%s, shard=%d", nodes[0].ID, f.index(), f.field(), f.shard)
		return nil
	}

	// Get the local block represented as roaring data.
	localData, err := f.blockToRoaringData(id)
	if err != nil {
		return errors.Wrap(err, "converting block to roaring data")
	}

	// Verify sync is not prematurely closing.
	if s.isClosing() {
		return nil
	}

	// Create the overwrite request to be sent to non-primary replicas.
	overwriteReq := &ImportRoaringRequest{
		Action: RequestActionOverwrite,
		Block:  id,
		Views:  map[string][]byte{cleanViewName(f.view()): localData},
	}

	// Write updates to remote blocks.
	for _, node := range nodes {
		if s.Node.ID == node.ID {
			continue
		}

		uri := &node.URI
		if err := s.Cluster.InternalClient.ImportRoaring(ctx, uri, f.index(), f.field(), f.shard, true, overwriteReq); err != nil {
			return errors.Wrap(err, "sending roaring data (overwrite)")
		}
	}

	return nil
}

// syncBlock sends and receives all rows for a given block.
// Returns an error if any remote hosts are unreachable.
func (s *fragmentSyncer) syncBlock(id int) error {
	span, ctx := tracing.StartSpanFromContext(context.Background(), "FragmentSyncer.syncBlock")
	defer span.Finish()

	f := s.Fragment

	// Create a snapshot of the cluster to use for node/partition calculations.
	snap := topology.NewClusterSnapshot(s.Cluster.noder, s.Cluster.Hasher, s.Cluster.ReplicaN)

	// Read pairs from each remote block.
	var uris []*pnet.URI
	var pairSets []pairSet
	for _, node := range snap.ShardNodes(f.index(), f.shard) {
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
		// Does a remote fetch
		rowIDs, columnIDs, err := s.Cluster.InternalClient.BlockData(ctx, &node.URI, f.index(), f.field(), f.view(), f.shard, id)
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

	idx := f.holder.Index(f.index())
	tx := idx.holder.txf.NewTx(Txo{Write: writable, Index: idx, Shard: f.shard})
	defer tx.Rollback()

	// Merge blocks together.
	sets, clears, err := f.mergeBlock(tx, id, pairSets)
	if err != nil {
		return errors.Wrap(err, "merging")
	}

	// no safeCopy needed here. We are not leaking data outside the tx, because
	// sets and clears only contain columnIDs.

	err = tx.Commit()
	if err != nil {
		return err
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
				Action: RequestActionSet,
				Views:  map[string][]byte{cleanViewName(f.view()): setData},
			}

			if err := s.Cluster.InternalClient.ImportRoaring(ctx, uris[i], f.index(), f.field(), f.shard, true, setReq); err != nil {
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
				Action: RequestActionClear,
				Views:  map[string][]byte{cleanViewName(f.view()): clearData},
			}

			if err := s.Cluster.InternalClient.ImportRoaring(ctx, uris[i], f.index(), f.field(), f.shard, true, clearReq); err != nil {
				return errors.Wrap(err, "sending roaring data (clear)")
			}
		}
	}

	return nil
}

// cleanViewName converts a view name into the equivalent
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
