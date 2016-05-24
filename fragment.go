package pilosa

import (
	"archive/tar"
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"github.com/gogo/protobuf/proto"
	"github.com/umbel/pilosa/internal"
	"github.com/umbel/pilosa/roaring"
)

const (
	// SliceWidth is the number of profile IDs in a slice.
	SliceWidth = 65536

	// SnapshotExt is the file extension used for an in-process snapshot.
	SnapshotExt = ".snapshotting"

	// CopyExt is the file extension used for the temp file used while copying.
	CopyExt = ".copying"

	// CacheExt is the file extension for persisted cache ids.
	CacheExt = ".cache"

	// MinThreshold is the lowest count to use in a Top-N operation when
	// looking for additional bitmap/count pairs.
	MinThreshold = 10

	// HashBlockSize is the number of bitmaps in a merkle hash block.
	HashBlockSize = 100
)

const (
	// DefaultCacheFlushInterval is the default value for Fragment.CacheFlushInterval.
	DefaultCacheFlushInterval = 1 * time.Minute

	// DefaultFragmentMaxOpN is the default value for Fragment.MaxOpN.
	DefaultFragmentMaxOpN = 1000
)

// Fragment represents the intersection of a frame and slice in a database.
type Fragment struct {
	mu sync.Mutex

	// Composite identifiers
	db    string
	frame string
	slice uint64

	// File-backed storage
	path        string
	file        *os.File
	storage     *roaring.Bitmap
	storageData []byte
	opN         int // number of ops since snapshot

	// Bitmap cache.
	cache Cache

	// Cached checksums for each block.
	checksums map[int][]byte

	// Close management
	wg      sync.WaitGroup
	closing chan struct{}

	// The interval at which the cached bitmap ids are persisted to disk.
	CacheFlushInterval time.Duration

	// Number of operations performed before performing a snapshot.
	// This limits the size of fragments on the heap and flushes them to disk
	// so that they can be mmapped and heap utilization can be kept low.
	MaxOpN int

	// Writer used for out-of-band log entries.
	LogOutput io.Writer

	// Bitmap attribute storage.
	// This is set by the parent frame unless overridden for testing.
	BitmapAttrStore *AttrStore
}

// NewFragment returns a new instance of Fragment.
func NewFragment(path, db, frame string, slice uint64) *Fragment {
	return &Fragment{
		path:    path,
		db:      db,
		frame:   frame,
		slice:   slice,
		closing: make(chan struct{}, 0),

		LogOutput:          os.Stderr,
		CacheFlushInterval: DefaultCacheFlushInterval,
		MaxOpN:             DefaultFragmentMaxOpN,
	}
}

// Path returns the path the fragment was initialized with.
func (f *Fragment) Path() string { return f.path }

// CachePath returns the path to the fragment's cache data.
func (f *Fragment) CachePath() string { return f.path + CacheExt }

// DB returns the database the fragment was initialized with.
func (f *Fragment) DB() string { return f.db }

// Frame returns the frame the fragment was initialized with.
func (f *Fragment) Frame() string { return f.frame }

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

		// Fill cache with bitmaps persisted to disk.
		if err := f.openCache(); err != nil {
			return err
		}

		// Clear checksums.
		f.checksums = make(map[int][]byte)

		// Periodically flush cache.
		f.wg.Add(1)
		go func() { defer f.wg.Done(); f.monitorCacheFlush() }()

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
	data := (*[0x7FFFFFFF]byte)(unsafe.Pointer(&f.storageData[0]))[:fi.Size()]
	if err := f.storage.UnmarshalBinary(data); err != nil {
		return fmt.Errorf("unmarshal storage: file=%s, err=%s", f.file.Name(), err)
	}

	// Attach the file to the bitmap to act as a write-ahead log.
	f.storage.OpWriter = f.file

	return nil

}

// openCache initializes the cache from bitmap ids persisted to disk.
func (f *Fragment) openCache() error {
	// Determine cache type from frame name.
	if strings.HasSuffix(f.frame, FrameSuffixRank) {
		c := NewRankCache()
		c.ThresholdLength = 50000
		c.ThresholdIndex = 45000
		f.cache = c
	} else {
		f.cache = NewLRUCache(50000)
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

	// Read in all bitmaps by ID.
	// This will cause them to be added to the cache.
	for _, bitmapID := range pb.GetBitmapIDs() {
		f.bitmap(bitmapID)
	}

	return nil
}

// Close flushes the underlying storage, closes the file and unlocks it.
func (f *Fragment) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.close()
}

func (f *Fragment) close() error {
	// Notify goroutines of closing and wait for completion.
	close(f.closing)
	f.mu.Unlock()
	f.wg.Wait()
	f.mu.Lock()

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

// Bitmap returns a bitmap by ID.
func (f *Fragment) Bitmap(bitmapID uint64) *Bitmap {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.bitmap(bitmapID)
}

func (f *Fragment) bitmap(bitmapID uint64) *Bitmap {
	// Read bitmap from storage.
	bm := NewBitmap()
	f.storage.ForEachRange(bitmapID*SliceWidth, (bitmapID+1)*SliceWidth, func(i uint64) {
		profileID := (f.slice * SliceWidth) + (i % SliceWidth)
		bm.SetBit(profileID)
	})

	// Update cache.
	f.cache.Add(bitmapID, bm.Count())

	return bm
}

// SetBit sets a bit for a given profile & bitmap within the fragment.
// This updates both the on-disk storage and the in-cache bitmap.
func (f *Fragment) SetBit(bitmapID, profileID uint64, t *time.Time, q TimeQuantum) (changed bool, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Set time bits if this is a time-frame and a timestamp is specified.
	if strings.HasSuffix(f.frame, FrameSuffixTime) && t != nil {
		return f.setTimeBit(bitmapID, profileID, *t, q)
	}

	return f.setBit(bitmapID, profileID)
}

func (f *Fragment) setBit(bitmapID, profileID uint64) (changed bool, bool error) {
	// Determine the position of the bit in the storage.
	changed = false
	pos, err := f.pos(bitmapID, profileID)
	if err != nil {
		return false, err
	}

	// Write to storage.
	if changed, err = f.storage.Add(pos); err != nil {
		return false, err
	}

	// Invalidate block checksum.
	delete(f.checksums, int(bitmapID/HashBlockSize))

	// If the number of operations exceeds the limit then snapshot.
	if err := f.incrementOpN(); err != nil {
		return false, err
	}

	// Update the cache.
	if f.bitmap(bitmapID).SetBit(profileID) {
		changed = true
	}

	return changed, nil
}

func (f *Fragment) setTimeBit(bitmapID, profileID uint64, t time.Time, q TimeQuantum) (changed bool, err error) {
	for _, timeID := range TimeIDsFromQuantum(q, t, bitmapID) {
		if v, err := f.setBit(timeID, profileID); err != nil {
			return changed, fmt.Errorf("set time bit: t=%s, q=%s, err=%s", t, q, err)
		} else if v {
			changed = true
		}
	}
	return changed, nil
}

// ClearBit clears a bit for a given profile & bitmap within the fragment.
// This updates both the on-disk storage and the in-cache bitmap.
func (f *Fragment) ClearBit(bitmapID, profileID uint64) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.clearBit(bitmapID, profileID)
}

func (f *Fragment) clearBit(bitmapID, profileID uint64) (bool, error) {
	// Determine the position of the bit in the storage.
	pos, err := f.pos(bitmapID, profileID)
	if err != nil {
		return false, err
	}

	// Write to storage.
	changed, err := f.storage.Remove(pos)
	if err != nil {
		return false, err
	}

	// Invalidate block checksum.
	delete(f.checksums, int(bitmapID/HashBlockSize))

	// Increment number of operations until snapshot is required.
	if err := f.incrementOpN(); err != nil {
		return false, err
	}

	// Update the cache.
	if f.bitmap(bitmapID).ClearBit(profileID) {
		return true, nil
	}

	return changed, nil
}

// pos translates the bitmap ID and profile ID into a position in the storage bitmap.
func (f *Fragment) pos(bitmapID, profileID uint64) (uint64, error) {
	// Return an error if the profile ID is out of the range of the fragment's slice.
	minProfileID := f.slice * SliceWidth
	if profileID < minProfileID || profileID >= minProfileID+SliceWidth {
		return 0, errors.New("profile out of bounds")
	}

	return (bitmapID * SliceWidth) + (profileID % SliceWidth), nil
}

// Top returns the top bitmaps from the fragment.
// If opt.Src is specified then only bitmaps which intersect src are returned.
// If opt.FilterValues exist then the bitmap attribute specified by field is matched.
func (f *Fragment) Top(opt TopOptions) ([]Pair, error) {
	// Retrieve pairs. If no bitmap ids specified then return from cache.
	pairs := f.topBitmapPairs(opt.BitmapIDs)

	// Create a fast lookup of filter values.
	var filters map[interface{}]struct{}
	if opt.FilterField != "" && len(opt.FilterValues) > 0 {
		filters = make(map[interface{}]struct{})
		for _, v := range opt.FilterValues {
			filters[v] = struct{}{}
		}
	}

	// Iterate over rankings and add to results until we have enough.
	results := make([]Pair, 0, opt.N)
	for _, pair := range pairs {
		bitmapID, n := pair.ID, pair.Count

		// Ignore empty bitmaps.
		if n <= 0 {
			continue
		}

		// Apply filter, if set.
		if filters != nil {
			attr, err := f.BitmapAttrStore.Attrs(bitmapID)
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
		if opt.N == 0 || len(results) < opt.N {
			// Calculate count and append.
			count := n
			if opt.Src != nil {
				count = opt.Src.IntersectionCount(f.Bitmap(bitmapID))
			}
			if count == 0 {
				continue
			}
			results = append(results, Pair{Key: bitmapID, Count: count})

			// If we reach the requested number of pairs and we are not computing
			// intersections then simply exit. If we are intersecting then sort
			// and then only keep pairs that are higher than the lowest count.
			if opt.N > 0 && len(results) == opt.N {
				if opt.Src == nil {
					break
				}
				sort.Sort(Pairs(results))
			}
			continue
		}

		// Retrieve the lowest count we have.
		// If it's too low then don't try finding anymore pairs.
		threshold := results[len(results)-1].Count
		if threshold < MinThreshold {
			break
		}

		// If the bitmap doesn't have enough bits set before the intersection
		// then we can assume that any remaing bitmaps also have a count too low.
		if n < threshold {
			break
		}

		// Calculate the intersecting bit count and skip if it's below our
		// last bitmap in our current result set.
		count := opt.Src.IntersectionCount(f.Bitmap(bitmapID))
		if count < threshold {
			continue
		}

		// Swap out the last pair for this new count.
		results[len(results)-1] = Pair{Key: bitmapID, Count: count}

		// If it's count is also higher than the second to last item then resort.
		if len(results) >= 2 && count > results[len(results)-2].Count {
			sort.Sort(Pairs(results))
		}
	}

	sort.Sort(Pairs(results))
	return results, nil
}

func (f *Fragment) topBitmapPairs(bitmapIDs []uint64) []BitmapPair {
	// If no specific bitmaps are requested, retrieve top bitmaps.
	if len(bitmapIDs) == 0 {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.cache.Invalidate()
		return f.cache.Top()
	}

	// Otherwise retrieve specific bitmaps.
	pairs := make([]BitmapPair, len(bitmapIDs))
	for i, bitmapID := range bitmapIDs {
		pairs[i] = BitmapPair{
			ID:    bitmapID,
			Count: f.Bitmap(bitmapID).Count(),
		}
	}
	return pairs
}

// TopOptions represents options passed into the Top() function.
type TopOptions struct {
	// Number of bitmaps to return.
	N int

	// Bitmap to intersect with.
	Src *Bitmap

	// Specific bitmaps to filter against.
	BitmapIDs []uint64

	// Filter field name & values.
	FilterField  string
	FilterValues []interface{}
}

func (f *Fragment) Range(bitmapID uint64, start, end time.Time) *Bitmap {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Retrieve a list of bitmap ids for a given time range.
	bitmapIDs := TimeIDsFromRange(start, end, bitmapID)
	if len(bitmapIDs) == 0 {
		return NewBitmap()
	}

	// Union all bitmap ids from the time range.
	bm := f.bitmap(bitmapIDs[0])
	for _, id := range bitmapIDs[1:] {
		bm = bm.Union(f.bitmap(id))
	}
	return bm
}

// Checksum returns a checksum for the entire fragment.
// If two fragments have the same checksum then they have the same data.
func (f *Fragment) Checksum() []byte {
	h := sha1.New()
	for i, blockN := 0, f.BlockN(); i < blockN; i++ {
		h.Write(f.BlockChecksum(i))
	}
	return h.Sum(nil)
}

// BlockN returns the number of blocks in the fragment.
func (f *Fragment) BlockN() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return int(f.storage.Max() / (HashBlockSize * SliceWidth))
}

// BlockChecksum returns the checksum for a single block in the fragment.
// Returns nil if there is no data for the block.
func (f *Fragment) BlockChecksum(i int) []byte {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Use the cached checksum, if available.
	if chksum, ok := f.checksums[i]; ok {
		return chksum
	}

	// Otherwise calculate the checksum from the data on disk.
	h := sha1.New()
	var written bool
	f.storage.ForEachRange(uint64(i)*HashBlockSize*SliceWidth, (uint64(i)+1)*HashBlockSize*SliceWidth, func(i uint64) {
		// Write value to the hash.
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], i)
		h.Write(buf[:])

		// Mark the block has having data.
		written = true
	})

	// If no data was written then return a nil checksum.
	if !written {
		return nil
	}

	// Cache checksum for later use.
	chksum := h.Sum(nil)[:]
	f.checksums[i] = chksum

	return chksum
}

// InvalidateChecksums clears all cached block checksums.
func (f *Fragment) InvalidateChecksums() {
	f.mu.Lock()
	f.checksums = make(map[int][]byte)
	f.mu.Unlock()
}

// Blocks returns info for all blocks containing data.
func (f *Fragment) Blocks() []FragmentBlock {
	var a []FragmentBlock
	for i, blockN := 0, f.BlockN(); i <= blockN; i++ {
		chksum := f.BlockChecksum(i)
		if chksum == nil {
			continue
		}

		a = append(a, FragmentBlock{
			ID:       i,
			Checksum: chksum,
		})
	}
	return a
}

// BlockData returns bits in a block as bitmap & profile ID pairs.
func (f *Fragment) BlockData(id int) (bitmapIDs, profileIDs []uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.storage.ForEachRange(uint64(id)*HashBlockSize*SliceWidth, (uint64(id)+1)*HashBlockSize*SliceWidth, func(i uint64) {
		bitmapIDs = append(bitmapIDs, i/SliceWidth)
		profileIDs = append(profileIDs, i%SliceWidth)
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
		if len(data[i].BitmapIDs) != len(data[i].ProfileIDs) {
			return nil, nil, fmt.Errorf("pair set mismatch(idx=%d): %d != %d", i, len(data[i].BitmapIDs), len(data[i].ProfileIDs))
		}
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	// Track sets and clears for all blocks (including local).
	sets = make([]PairSet, len(data)+1)
	clears = make([]PairSet, len(data)+1)

	// Limit upper bitmap/profile pair.
	maxBitmapID := uint64(id+1) * HashBlockSize
	maxProfileID := uint64(SliceWidth)

	// Create buffered iterator for local block.
	itrs := make([]*BufIterator, 1, len(data)+1)
	itrs[0] = NewBufIterator(
		NewLimitIterator(
			NewRoaringIterator(f.storage.Iterator()), maxBitmapID, maxProfileID,
		),
	)

	// Append buffered iterators for each incoming block.
	for i := range data {
		var itr Iterator = NewSliceIterator(data[i].BitmapIDs, data[i].ProfileIDs)
		itr = NewLimitIterator(itr, maxBitmapID, maxProfileID)
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
			bitmapID  uint64
			profileID uint64
		}

		// Find the lowest pair.
		var hasData bool
		for _, itr := range itrs {
			bid, pid, eof := itr.Peek()
			if eof { // no more data
				continue
			} else if !hasData { // first pair
				min.bitmapID, min.profileID, hasData = bid, pid, true
			} else if bid < min.bitmapID || (bid == min.bitmapID && pid < min.profileID) { // lower pair
				min.bitmapID, min.profileID = bid, pid
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

			values[i] = !eof && bid == min.bitmapID && pid == min.profileID
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
				sets[i].BitmapIDs = append(sets[i].BitmapIDs, min.bitmapID)
				sets[i].ProfileIDs = append(sets[i].ProfileIDs, min.profileID)
			} else {
				clears[i].BitmapIDs = append(sets[i].BitmapIDs, min.bitmapID)
				clears[i].ProfileIDs = append(sets[i].ProfileIDs, min.profileID)
			}
		}
	}

	// Set local bits.
	for i := range sets[0].ProfileIDs {
		if _, err := f.setBit(sets[0].BitmapIDs[i], (f.Slice()*SliceWidth)+sets[0].ProfileIDs[i]); err != nil {
			return nil, nil, err
		}
	}

	// Clear local bits.
	for i := range clears[0].ProfileIDs {
		if _, err := f.clearBit(clears[0].BitmapIDs[i], (f.Slice()*SliceWidth)+clears[0].ProfileIDs[i]); err != nil {
			return nil, nil, err
		}
	}

	return sets[1:], clears[1:], nil
}

// Import bulk imports a set of bits and then snapshots the storage.
// This does not affect the fragment's cache.
func (f *Fragment) Import(bitmapIDs, profileIDs []uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Verify that there are an equal number of bitmap ids and profile ids.
	if len(bitmapIDs) != len(profileIDs) {
		return fmt.Errorf("mismatch of bitmap and profile len: %d != %d", len(bitmapIDs), len(profileIDs))
	}

	// Disconnect op writer so we don't append updates.
	f.storage.OpWriter = nil

	// Process every bit.
	// If an error occurs then reopen the storage.
	if err := func() error {
		for i := range bitmapIDs {
			bitmapID, profileID := bitmapIDs[i], profileIDs[i]

			// Determine the position of the bit in the storage.
			pos, err := f.pos(bitmapID, profileID)
			if err != nil {
				return err
			}

			// Write to storage.
			if _, err := f.storage.Add(pos); err != nil {
				return err
			}

			// Invalidate block checksum.
			delete(f.checksums, int(bitmapID/HashBlockSize))

			// Update the cache.
			f.bitmap(bitmapID).SetBit(profileID)
		}
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

func (f *Fragment) snapshot() error {
	logger := f.logger()
	logger.Printf("fragment: snapshotting %s/%s/%d", f.db, f.frame, f.slice)

	// Create a temporary file to snapshot to.
	snapshotPath := f.path + SnapshotExt
	file, err := os.Create(snapshotPath)
	if err != nil {
		return fmt.Errorf("create snapshot file: %s", err)
	}
	defer file.Close()

	// Write storage to snapshot.
	if _, err := f.storage.WriteTo(file); err != nil {
		return fmt.Errorf("snapshot write to: %s", err)
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

// monitorCacheFlush periodically flushes the cache to disk.
// This is run in a goroutine.
func (f *Fragment) monitorCacheFlush() {
	ticker := time.NewTicker(f.CacheFlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-f.closing:
			return
		case <-ticker.C:
			if err := f.FlushCache(); err != nil {
				f.logger().Printf("error flushing cache: err=%s, path=%s", err, f.CachePath())
			}
		}
	}
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

	// Retrieve a list of bitmap ids from the cache.
	bitmapIDs := f.cache.BitmapIDs()

	// Marshal cache data to bytes.
	buf, err := proto.Marshal(&internal.Cache{
		BitmapIDs: bitmapIDs,
	})
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

// FragmentBlock represents info about a subsection of the bitmaps in a block.
// This is used for comparing data in remote blocks for active anti-entropy.
type FragmentBlock struct {
	ID       int    `json:"id"`
	Checksum []byte `json:"checksum"`
}

// FragmentSyncer syncs a local fragment to one on a remote host.
type FragmentSyncer struct {
	Fragment *Fragment

	Host    string
	Cluster *Cluster
}

// SyncFragment compares checksums for the local and remote fragments and
// then merges any blocks which have differences.
func (s *FragmentSyncer) SyncFragment() error {
	// Determine replica set.
	nodes := s.Cluster.SliceNodes(s.Fragment.Slice())

	// Create a set of blocks.
	blockSets := make([][]FragmentBlock, 0, len(nodes))
	for _, node := range nodes {
		// Read local blocks.
		if node.Host == s.Host {
			blockSets = append(blockSets, s.Fragment.Blocks())
			continue
		}

		// Retrieve remote blocks.
		client, err := NewClient(node.Host)
		if err != nil {
			return err
		}
		blocks, err := client.FragmentBlocks(s.Fragment.DB(), s.Fragment.Frame(), s.Fragment.Slice())
		if err != nil && err != ErrFragmentNotFound {
			return err
		}
		blockSets = append(blockSets, blocks)
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

// syncBlock sends and receives all bitmaps for a given block.
// Returns an error if any remote hosts are unreachable.
func (s *FragmentSyncer) syncBlock(id int) error {
	f := s.Fragment

	// Read pairs from each remote block.
	var pairSets []PairSet
	var clients []*Client
	for _, node := range s.Cluster.SliceNodes(f.Slice()) {
		if s.Host == node.Host {
			continue
		}

		client, err := NewClient(node.Host)
		if err != nil {
			return err
		}
		clients = append(clients, client)

		bitmapIDs, profileIDs, err := client.BlockData(f.DB(), f.Frame(), f.Slice(), id)
		if err != nil {
			return err
		}

		pairSets = append(pairSets, PairSet{
			ProfileIDs: profileIDs,
			BitmapIDs:  bitmapIDs,
		})
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
		if len(set.ProfileIDs) == 0 && len(clear.ProfileIDs) == 0 {
			continue
		}

		// Generate query with sets & clears.
		var buf bytes.Buffer
		for j := 0; j < len(set.ProfileIDs); j++ {
			fmt.Fprintf(&buf, "SetBit(frame=%q, id=%d, profileID=%d)\n", f.Frame(), set.BitmapIDs[j], (f.Slice()*SliceWidth)+set.ProfileIDs[j])
		}
		for j := 0; j < len(clear.ProfileIDs); j++ {
			fmt.Fprintf(&buf, "ClearBit(frame=%q, id=%d, profileID=%d)\n", f.Frame(), clear.BitmapIDs[j], (f.Slice()*SliceWidth)+clear.ProfileIDs[j])
		}

		// Execute query.
		_, err := clients[i].ExecuteQuery(f.DB(), buf.String(), false)
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

// PairSet is a list of equal length bitmap and profile id lists.
type PairSet struct {
	BitmapIDs  []uint64
	ProfileIDs []uint64
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
