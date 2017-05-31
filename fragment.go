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
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"math"

	"github.com/gogo/protobuf/proto"
	"github.com/pilosa/pilosa/internal"
	"github.com/pilosa/pilosa/roaring"
)

const (
	// SliceWidth is the number of column IDs in a slice.
	SliceWidth = 1048576

	// SnapshotExt is the file extension used for an in-process snapshot.
	SnapshotExt = ".snapshotting"

	// CopyExt is the file extension used for the temp file used while copying.
	CopyExt = ".copying"

	// CacheExt is the file extension for persisted cache ids.
	CacheExt = ".cache"

	// HashBlockSize is the number of rows in a merkle hash block.
	HashBlockSize = 100
)

const (
	// DefaultFragmentMaxOpN is the default value for Fragment.MaxOpN.
	DefaultFragmentMaxOpN = 2000
)

// Fragment represents the intersection of a frame and slice in an index.
type Fragment struct {
	mu sync.Mutex

	// Composite identifiers
	index string
	frame string
	view  string
	slice uint64

	// File-backed storage
	path        string
	file        *os.File
	storage     *roaring.Bitmap
	storageData []byte
	opN         int // number of ops since snapshot

	// Cache for row counts.
	CacheType string // passed in by frame
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

	// Writer used for out-of-band log entries.
	LogOutput io.Writer

	// Row attribute storage.
	// This is set by the parent frame unless overridden for testing.
	RowAttrStore *AttrStore

	stats StatsClient
}

// NewFragment returns a new instance of Fragment.
func NewFragment(path, index, frame, view string, slice uint64) *Fragment {
	return &Fragment{
		path:      path,
		index:     index,
		frame:     frame,
		view:      view,
		slice:     slice,
		CacheType: DefaultCacheType,
		CacheSize: DefaultCacheSize,

		LogOutput: ioutil.Discard,
		MaxOpN:    DefaultFragmentMaxOpN,

		stats: NopStatsClient,
	}
}

// Path returns the path the fragment was initialized with.
func (f *Fragment) Path() string { return f.path }

// CachePath returns the path to the fragment's cache data.
func (f *Fragment) CachePath() string { return f.path + CacheExt }

// Index returns the index that the fragment was initialized with.
func (f *Fragment) Index() string { return f.index }

// Frame returns the frame the fragment was initialized with.
func (f *Fragment) Frame() string { return f.frame }

// View returns the view the fragment was initialized with.
func (f *Fragment) View() string { return f.view }

// Slice returns the slice the fragment was initialized with.
func (f *Fragment) Slice() uint64 { return f.slice }

// Cache returns the fragment's cache.
// This is not safe for concurrent use.
func (f *Fragment) Cache() Cache { return f.cache }

// Open opens the underlying storage.
func (f *Fragment) Open() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := func() error {
		// Initialize storage in a function so we can close if anything goes wrong.
		if err := f.openStorage(); err != nil {
			return err
		}

		// Fill cache with rows persisted to disk.
		if err := f.openCache(); err != nil {
			return err
		}

		// Clear checksums.
		f.checksums = make(map[int][]byte)

		// Read last bit to determine max row.
		pos := f.storage.Max()
		f.maxRowID = pos / SliceWidth
		f.stats.Gauge("rows", float64(f.maxRowID))

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
	f.storage = roaring.NewBitmap()

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
		return err
	} else if fi.Size() == 0 {
		if _, err := f.storage.WriteTo(f.file); err != nil {
			return fmt.Errorf("init storage file: %s", err)
		}

		fi, err = f.file.Stat()
		if err != nil {
			return err
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
	f.rowCache = &SimpleCache{make(map[uint64]*Bitmap)}

	return nil

}

// openCache initializes the cache from row ids persisted to disk.
func (f *Fragment) openCache() error {
	// Determine cache type from frame name.
	switch f.CacheType {
	case CacheTypeRanked:
		f.cache = NewRankCache(f.CacheSize)
	case CacheTypeLRU:
		f.cache = NewLRUCache(f.CacheSize)
	default:
		return ErrInvalidCacheType
	}

	// Read cache data from disk.
	path := f.CachePath()
	buf, err := ioutil.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return fmt.Errorf("open cache: %s", err)
	}

	// Unmarshal cache data.
	var pb internal.Cache
	if err := proto.Unmarshal(buf, &pb); err != nil {
		log.Printf("error unmarshaling cache data, skipping: path=%s, err=%s", path, err)
		return nil
	}

	// Read in all rows by ID.
	// This will cause them to be added to the cache.
	for _, id := range pb.IDs {
		//n := f.storage.CountRange(id*SliceWidth, (id+1)*SliceWidth)
		n := f.row(id, true, true).Count()
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
		f.logger().Printf("fragment: error flushing cache on close: err=%s, path=%s", err, f.path)
	}

	// Close underlying storage.
	if err := f.closeStorage(); err != nil {
		f.logger().Printf("fragment: error closing storage: err=%s, path=%s", err, f.path)
	}

	// Remove checksums.
	f.checksums = nil

	return nil
}

func (f *Fragment) closeStorage() error {
	// Clear the storage bitmap so it doesn't access the closed mmap.
	f.storage = roaring.NewBitmap()

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

// logger returns a logger instance for the fragment.nt.
func (f *Fragment) logger() *log.Logger { return log.New(f.LogOutput, "", log.LstdFlags) }

// Row returns a row by ID.
func (f *Fragment) Row(rowID uint64) *Bitmap {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.row(rowID, true, true)
}

func (f *Fragment) row(rowID uint64, checkRowCache bool, updateRowCache bool) *Bitmap {
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
	// We Clone() data because otherwise bm will contains pointers to containers in storage.
	// This causes unexpected results when we cache the row and try to use it later.
	bm := &Bitmap{
		segments: []BitmapSegment{{
			data:     *data.Clone(),
			slice:    f.slice,
			writable: false,
		}},
	}
	bm.InvalidateCount()

	if updateRowCache {
		f.rowCache.Add(rowID, bm)
	}

	return bm
}

// SetBit sets a bit for a given column & row within the fragment.
// This updates both the on-disk storage and the in-cache bitmap.
func (f *Fragment) SetBit(rowID, columnID uint64) (changed bool, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.setBit(rowID, columnID)
}

func (f *Fragment) setBit(rowID, columnID uint64) (changed bool, err error) {
	changed = false
	// Determine the position of the bit in the storage.
	pos, err := f.pos(rowID, columnID)
	if err != nil {
		return false, err
	}

	// Write to storage.
	if changed, err = f.storage.Add(pos); err != nil {
		return false, err
	}

	// Don't update the cache if nothing changed.
	if !changed {
		return changed, nil
	}

	// Invalidate block checksum.
	delete(f.checksums, int(rowID/HashBlockSize))

	// Increment number of operations until snapshot is required.
	if err := f.incrementOpN(); err != nil {
		return false, err
	}

	// Get the row from row cache or fragment.storage.
	bm := f.row(rowID, true, true)
	bm.SetBit(columnID)

	// Update the cache.
	f.cache.Add(rowID, bm.Count())

	f.stats.Count("setN", 1)

	// Update row count if they have increased.
	if rowID > f.maxRowID {
		f.maxRowID = rowID
		f.stats.Gauge("rows", float64(f.maxRowID))
	}

	return changed, nil
}

// ClearBit clears a bit for a given column & row within the fragment.
// This updates both the on-disk storage and the in-cache bitmap.
func (f *Fragment) ClearBit(rowID, columnID uint64) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.clearBit(rowID, columnID)
}

func (f *Fragment) clearBit(rowID, columnID uint64) (changed bool, err error) {
	changed = false
	// Determine the position of the bit in the storage.
	pos, err := f.pos(rowID, columnID)
	if err != nil {
		return false, err
	}

	// Write to storage.
	if changed, err = f.storage.Remove(pos); err != nil {
		return false, err
	}

	// Don't update the cache if nothing changed.
	if !changed {
		return changed, nil
	}

	// Invalidate block checksum.
	delete(f.checksums, int(rowID/HashBlockSize))

	// Increment number of operations until snapshot is required.
	if err := f.incrementOpN(); err != nil {
		return false, err
	}

	// Get the row from cache or fragment.storage.
	bm := f.row(rowID, true, true)
	bm.ClearBit(columnID)

	// Update the cache.
	f.cache.Add(rowID, bm.Count())

	f.stats.Count("clearN", 1)

	return changed, nil
}

// pos translates the row ID and column ID into a position in the storage bitmap.
func (f *Fragment) pos(rowID, columnID uint64) (uint64, error) {
	// Return an error if the column ID is out of the range of the fragment's slice.
	minColumnID := f.slice * SliceWidth
	if columnID < minColumnID || columnID >= minColumnID+SliceWidth {
		return 0, errors.New("column out of bounds")
	}
	return Pos(rowID, columnID), nil
}

// ForEachBit executes fn for every bit set in the fragment.
// Errors returned from fn are passed through.
func (f *Fragment) ForEachBit(fn func(rowID, columnID uint64) error) error {
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

// Top returns the top rows from the fragment.
// If opt.Src is specified then only rows which intersect src are returned.
// If opt.FilterValues exist then the row attribute specified by field is matched.
func (f *Fragment) Top(opt TopOptions) ([]Pair, error) {
	// Retrieve pairs. If no row ids specified then return from cache.
	pairs := f.topBitmapPairs(opt.RowIDs)

	// If row ids are provided, we don't want to truncate the result set
	if len(opt.RowIDs) > 0 {
		opt.N = 0
	}

	// Create a fast lookup of filter values.
	var filters map[interface{}]struct{}
	if opt.FilterField != "" && len(opt.FilterValues) > 0 {
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
				return nil, err
			} else if attr == nil {
				continue
			} else if attrValue := attr[opt.FilterField]; attrValue == nil {
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
				count = opt.Src.IntersectionCount(f.Row(rowID))
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

		// If the row doesn't have enough bits set before the intersection
		// then we can assume that any remaining rows also have a count too low.
		if threshold < opt.MinThreshold || cnt < threshold {
			break
		}

		// Calculate the intersecting bit count and skip if it's below our
		// last row in our current result set.
		count := opt.Src.IntersectionCount(f.Row(rowID))
		if count < threshold {
			continue
		}

		heap.Push(results, Pair{ID: rowID, Count: count})
	}

	//Pop first opt.N elements out of heap
	r := make(Pairs, results.Len(), results.Len())
	x := results.Len()
	i := 1
	for results.Len() > 0 {
		r[x-i] = heap.Pop(results).(Pair)
		i++
	}
	return r, nil
}

func (f *Fragment) topBitmapPairs(rowIDs []uint64) []BitmapPair {
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

		bm := f.Row(rowID)
		if bm.Count() > 0 {
			// Otherwise load from storage.
			pairs = append(pairs, BitmapPair{
				ID:    rowID,
				Count: bm.Count(),
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
	Src *Bitmap

	// Specific rows to filter against.
	RowIDs       []uint64
	MinThreshold uint64

	// Filter field name & values.
	FilterField       string
	FilterValues      []interface{}
	TanimotoThreshold uint64
}

// Checksum returns a checksum for the entire fragment.
// If two fragments have the same checksum then they have the same data.
func (f *Fragment) Checksum() []byte {
	h := sha1.New()
	for _, block := range f.Blocks() {
		h.Write(block.Checksum)
	}
	return h.Sum(nil)
}

// BlockN returns the number of blocks in the fragment.
func (f *Fragment) BlockN() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return int(f.storage.Max() / (HashBlockSize * SliceWidth))
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

// BlockData returns bits in a block as row & column ID pairs.
func (f *Fragment) BlockData(id int) (rowIDs, columnIDs []uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.storage.ForEachRange(uint64(id)*HashBlockSize*SliceWidth, (uint64(id)+1)*HashBlockSize*SliceWidth, func(i uint64) {
		rowIDs = append(rowIDs, i/SliceWidth)
		columnIDs = append(columnIDs, i%SliceWidth)
	})
	return
}

// MergeBlock compares the block's bits and computes a diff with another set of block bits.
// The state of a bit is determined by consensus from all blocks being considered.
//
// For example, if 3 blocks are compared and two have a set bit and one has a
// cleared bit then the bit is considered cleared. The function returns the
// diff per incoming block so that all can be in sync.
func (f *Fragment) MergeBlock(id int, data []PairSet) (sets, clears []PairSet, err error) {
	// Ensure that all pair sets are of equal length.
	for i := range data {
		if len(data[i].RowIDs) != len(data[i].ColumnIDs) {
			return nil, nil, fmt.Errorf("pair set mismatch(idx=%d): %d != %d", i, len(data[i].RowIDs), len(data[i].ColumnIDs))
		}
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	// Track sets and clears for all blocks (including local).
	sets = make([]PairSet, len(data)+1)
	clears = make([]PairSet, len(data)+1)

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
		var itr Iterator = NewSliceIterator(data[i].RowIDs, data[i].ColumnIDs)
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
				sets[i].RowIDs = append(sets[i].RowIDs, min.rowID)
				sets[i].ColumnIDs = append(sets[i].ColumnIDs, min.columnID)
			} else {
				clears[i].RowIDs = append(sets[i].RowIDs, min.rowID)
				clears[i].ColumnIDs = append(sets[i].ColumnIDs, min.columnID)
			}
		}
	}

	// Set local bits.
	for i := range sets[0].ColumnIDs {
		if _, err := f.setBit(sets[0].RowIDs[i], (f.Slice()*SliceWidth)+sets[0].ColumnIDs[i]); err != nil {
			return nil, nil, err
		}
	}

	// Clear local bits.
	for i := range clears[0].ColumnIDs {
		if _, err := f.clearBit(clears[0].RowIDs[i], (f.Slice()*SliceWidth)+clears[0].ColumnIDs[i]); err != nil {
			return nil, nil, err
		}
	}

	return sets[1:], clears[1:], nil
}

// Import bulk imports a set of bits and then snapshots the storage.
// This does not affect the fragment's cache.
func (f *Fragment) Import(rowIDs, columnIDs []uint64) error {
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
				return err
			}

			// Write to storage.
			_, err = f.storage.Add(pos)
			if err != nil {
				return err
			}

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
			// Import should ALWAYS have row() load a new bm from fragment.storage
			// because the row that's in rowCache hasn't been updated with
			// this import's data.
			f.cache.BulkAdd(rowID, f.row(rowID, false, false).Count())
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
		return err
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

func track(start time.Time, name string, logger *log.Logger) {
	elapsed := time.Since(start)
	logger.Printf("%s took %s", name, elapsed)
}

func (f *Fragment) snapshot() error {
	logger := f.logger()
	logger.Printf("fragment: snapshotting %s/%s/%s/%d", f.index, f.frame, f.view, f.slice)
	defer track(time.Now(), fmt.Sprintf("fragment: snapshot complete %s/%s/%s/%d", f.index, f.frame, f.view, f.slice), logger)

	// Create a temporary file to snapshot to.
	snapshotPath := f.path + SnapshotExt
	file, err := os.Create(snapshotPath)
	if err != nil {
		return fmt.Errorf("create snapshot file: %s", err)
	}
	defer file.Close()

	// Write storage to snapshot.
	bw := bufio.NewWriter(file)
	if _, err := f.storage.WriteTo(bw); err != nil {
		return fmt.Errorf("snapshot write to: %s", err)
	} else if err := bw.Flush(); err != nil {
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

	// Retrieve a list of row ids from the cache.
	ids := f.cache.IDs()

	// Marshal cache data to bytes.
	buf, err := proto.Marshal(&internal.Cache{IDs: ids})
	if err != nil {
		return err
	}

	// Write to disk.
	if err := ioutil.WriteFile(f.CachePath(), buf, 0666); err != nil {
		return err
	}

	return nil
}

// WriteTo writes the fragment's data to w.
func (f *Fragment) WriteTo(w io.Writer) (n int64, err error) {
	// Force cache flush.
	if err := f.FlushCache(); err != nil {
		return 0, err
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
		return err
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
			return err
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
		return err
	}

	// Copy the file up to the last known size.
	// This is done outside the lock because the storage format is append-only.
	if _, err := io.CopyN(tw, file, sz); err != nil {
		return err
	}
	return nil
}

func (f *Fragment) writeCacheToArchive(tw *tar.Writer) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Read cache into buffer.
	buf, err := ioutil.ReadFile(f.CachePath())
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}

	// Write archive header.
	if err := tw.WriteHeader(&tar.Header{
		Name:    "cache",
		Mode:    0600,
		Size:    int64(len(buf)),
		ModTime: time.Now(),
	}); err != nil {
		return err
	}

	// Write data to archive.
	if _, err := tw.Write(buf); err != nil {
		return err
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
			return 0, err
		}

		// Process file based on file name.
		switch hdr.Name {
		case "data":
			if err := f.readStorageFromArchive(tr); err != nil {
				return 0, err
			}
		case "cache":
			if err := f.readCacheFromArchive(tr); err != nil {
				return 0, err
			}
		default:
			return 0, fmt.Errorf("invalid fragment archive file: %s", hdr.Name)
		}
	}

	return 0, nil
}

func (f *Fragment) readStorageFromArchive(r io.Reader) error {
	// Create a temporary file to copy into.
	path := f.path + CopyExt
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	// Copy reader into temporary path.
	if _, err = io.Copy(file, r); err != nil {
		return err
	}

	// Close current storage.
	if err := f.closeStorage(); err != nil {
		return err
	}

	// Move snapshot to data file location.
	if err := os.Rename(path, f.path); err != nil {
		return err
	}

	// Reopen storage.
	if err := f.openStorage(); err != nil {
		return err
	}

	return nil
}

func (f *Fragment) readCacheFromArchive(r io.Reader) error {
	// Slurp data from reader and write to disk.
	buf, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	} else if err := ioutil.WriteFile(f.CachePath(), buf, 0666); err != nil {
		return err
	}

	// Re-open cache.
	if err := f.openCache(); err != nil {
		return err
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
		hash:    sha1.New(),
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

	Host    string
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

// SyncFragment compares checksums for the local and remote fragments and
// then merges any blocks which have differences.
func (s *FragmentSyncer) SyncFragment() error {
	// Determine replica set.
	nodes := s.Cluster.FragmentNodes(s.Fragment.Index(), s.Fragment.Slice())
	if len(nodes) == 1 {
		return nil
	}

	// Create a set of blocks.
	blockSets := make([][]FragmentBlock, 0, len(nodes))
	for _, node := range nodes {
		// Read local blocks.
		if node.Host == s.Host {
			b := s.Fragment.Blocks()
			blockSets = append(blockSets, b)
			continue
		}

		// Retrieve remote blocks.
		client, err := NewClient(node.Host)
		if err != nil {
			return err
		}
		blocks, err := client.FragmentBlocks(context.Background(), s.Fragment.Index(), s.Fragment.Frame(), s.Fragment.View(), s.Fragment.Slice())
		if err != nil && err != ErrFragmentNotFound {
			return err
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
	}

	return nil
}

// syncBlock sends and receives all rows for a given block.
// Returns an error if any remote hosts are unreachable.
func (s *FragmentSyncer) syncBlock(id int) error {
	f := s.Fragment

	// Read pairs from each remote block.
	var pairSets []PairSet
	var clients []*Client
	for _, node := range s.Cluster.FragmentNodes(f.Index(), f.Slice()) {
		if s.Host == node.Host {
			continue
		}

		// Verify sync is not prematurely closing.
		if s.isClosing() {
			return nil
		}

		client, err := NewClient(node.Host)
		if err != nil {
			return err
		}
		clients = append(clients, client)

		// Only sync the standard block.
		rowIDs, columnIDs, err := client.BlockData(context.Background(), f.Index(), f.Frame(), ViewStandard, f.Slice(), id)
		if err != nil {
			return err
		}

		pairSets = append(pairSets, PairSet{
			ColumnIDs: columnIDs,
			RowIDs:    rowIDs,
		})
	}

	// Verify sync is not prematurely closing.
	if s.isClosing() {
		return nil
	}

	// Merge blocks together.
	sets, clears, err := f.MergeBlock(id, pairSets)
	if err != nil {
		return err
	}

	// Write updates to remote blocks.
	for i := 0; i < len(clients); i++ {
		set, clear := sets[i], clears[i]

		// Ignore if there are no differences.
		if len(set.ColumnIDs) == 0 && len(clear.ColumnIDs) == 0 {
			continue
		}

		// Generate query with sets & clears.
		var buf bytes.Buffer

		// Only sync the standard block.
		for j := 0; j < len(set.ColumnIDs); j++ {
			fmt.Fprintf(&buf, "SetBit(frame=%q, rowID=%d, columnID=%d)\n", f.Frame(), set.RowIDs[j], (f.Slice()*SliceWidth)+set.ColumnIDs[j])
		}
		for j := 0; j < len(clear.ColumnIDs); j++ {
			fmt.Fprintf(&buf, "ClearBit(frame=%q, rowID=%d, columnID=%d)\n", f.Frame(), clear.RowIDs[j], (f.Slice()*SliceWidth)+clear.ColumnIDs[j])
		}

		// Verify sync is not prematurely closing.
		if s.isClosing() {
			return nil
		}

		// Execute query.
		_, err := clients[i].ExecuteQuery(context.Background(), f.Index(), buf.String(), false)
		if err != nil {
			return err
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

// PairSet is a list of equal length row and column id lists.
type PairSet struct {
	RowIDs    []uint64
	ColumnIDs []uint64
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

// Pos returns the row position of a row/column pair.
func Pos(rowID, columnID uint64) uint64 {
	return (rowID * SliceWidth) + (columnID % SliceWidth)
}
