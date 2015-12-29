package pilosa

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"github.com/umbel/pilosa/roaring"
)

// SliceWidth is the number of profile IDs in a slice.
const SliceWidth = 65536

// SnapshotExt is the file extension used for an in-process snapshot.
const SnapshotExt = ".snapshotting"

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

	// Bitmap cache.
	cache Cache
}

// NewFragment returns a new instance of Fragment.
func NewFragment(path, db, frame string, slice uint64) *Fragment {
	f := &Fragment{
		path:  path,
		db:    db,
		frame: frame,
		slice: slice,
	}

	// Determine cache type from frame name.
	if strings.HasSuffix(frame, ".n") {
		c := NewRankCache()
		c.ThresholdLength = 50000
		c.ThresholdIndex = 45000
		f.cache = c
	} else {
		f.cache = NewLRUCache(50000)
	}

	return f
}

// Open opens the underlying storage.
func (f *Fragment) Open() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Initialize storage in a function so we can close if anything goes wrong.
	if err := f.openStorage(); err != nil {
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

// Close flushes the underlying storage, closes the file and unlocks it.
func (f *Fragment) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.close()
}

func (f *Fragment) close() error {
	if err := f.closeStorage(); err != nil {
		return err
	}
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

// Path returns the path the fragment was initialized with.
func (f *Fragment) Path() string { return f.path }

// DB returns the database the fragment was initialized with.
func (f *Fragment) DB() string { return f.db }

// Frame returns the frame the fragment was initialized with.
func (f *Fragment) Frame() string { return f.frame }

// Slice returns the slice the fragment was initialized with.
func (f *Fragment) Slice() uint64 { return f.slice }

// Bitmap returns a bitmap by ID.
func (f *Fragment) Bitmap(bitmapID uint64) *Bitmap {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.bitmap(bitmapID)
}

func (f *Fragment) bitmap(bitmapID uint64) *Bitmap {
	// Read from cache.
	if bm := f.cache.Get(bitmapID); bm != nil {
		return bm
	}

	// Read bitmap from storage.
	bm := NewBitmap()
	f.storage.ForEachRange(uint32(bitmapID)*SliceWidth, uint32(bitmapID+1)*SliceWidth, func(i uint32) {
		profileID := (f.slice * SliceWidth) + (uint64(i) % SliceWidth)
		bm.setBit(profileID)
	})

	// Add to the cache.
	f.cache.Add(bitmapID, 0 /*filter*/, bm)

	return bm
}

func (f *Fragment) TopNAll(n int, categories []uint64) []Pair {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cache.Invalidate()

	// Create a set of categories.
	m := make(map[uint64]struct{})
	for _, v := range categories {
		m[v] = struct{}{}
	}

	// Iterate over rankings and add to results until we have enough.
	var results []Pair
	for _, pair := range f.cache.Pairs() {
		// Skip if categories are specified but category is not found.
		if _, ok := m[pair.category]; (len(categories) > 0 && !ok) || pair.Count <= 0 {
			continue
		}

		// Append pair.
		results = append(results, pair)

		// Exit when we have enough pairs.
		if len(results) >= n {
			break
		}
	}
	return results
}

// SetBit sets a bit for a given profile & bitmap within the fragment.
// This updates both the on-disk storage and the in-cache bitmap.
func (f *Fragment) SetBit(bitmapID, profileID uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.setBit(bitmapID, profileID)
}

func (f *Fragment) setBit(bitmapID, profileID uint64) error {
	// Determine the position of the bit in the storage.
	pos, err := f.pos(bitmapID, profileID)
	if err != nil {
		return err
	}

	// Write to storage.
	if err := f.storage.Add(pos); err != nil {
		return err
	}

	// Update the cache.
	f.bitmap(bitmapID).setBit(profileID)

	return nil
}

// ClearBit clears a bit for a given profile & bitmap within the fragment.
// This updates both the on-disk storage and the in-cache bitmap.
func (f *Fragment) ClearBit(bitmapID, profileID uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Determine the position of the bit in the storage.
	pos, err := f.pos(bitmapID, profileID)
	if err != nil {
		return err
	}

	// Write to storage.
	if err := f.storage.Remove(pos); err != nil {
		return err
	}

	// Update the cache.
	f.bitmap(bitmapID).clearBit(profileID)

	return nil
}

// pos translates the bitmap ID and profile ID into a position in the storage bitmap.
func (f *Fragment) pos(bitmapID, profileID uint64) (uint32, error) {
	// Return an error if the profile ID is out of the range of the fragment's slice.
	minProfileID := f.slice * SliceWidth
	if profileID < minProfileID || profileID >= minProfileID+SliceWidth {
		return 0, errors.New("profile out of bounds")
	}

	return uint32((bitmapID * SliceWidth) + (profileID % SliceWidth)), nil
}

func (f *Fragment) TopN(src *Bitmap, n int, categories []uint64) []Pair {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Resort rank, if necessary.
	f.cache.Invalidate()

	// Create a set of categories.
	set := make(map[uint64]struct{})
	for _, v := range categories {
		set[v] = struct{}{}
	}

	var results []Pair
	var x int
	breakout := 1000

	// Iterate over rankings.
	rankings := f.cache.Pairs()
	for i, pair := range rankings {
		// Skip if category not found.
		if len(set) > 0 {
			if _, ok := set[pair.category]; !ok {
				continue
			}
		}

		// Only append if there are intersecting bits with source bitmap.
		bc := src.IntersectionCount(pair.bitmap)
		if bc > 0 {
			results = append(results, Pair{
				Key:      pair.Key,
				Count:    bc,
				category: pair.category,
			})
		}
		x = i

		// Exit when we have enough.
		if len(results) > n {
			break
		}
	}

	// Sort results by ranking.
	sort.Sort(Pairs(results))

	if len(results) < n {
		return results
	}

	end := len(results) - 1
	o := results[end]
	threshold := o.Count

	if threshold <= 10 {
		return results
	}

	results = append(results, o)
	for i := x + 1; i < len(rankings); i++ {
		o = rankings[i]

		if len(set) > 0 {
			if _, ok := set[o.category]; !ok {
				continue
			}
		}

		// Need something to do with the size of initial bitmap
		if len(results) > breakout || o.Count < threshold {
			break
		}

		bc := src.IntersectionCount(o.bitmap)
		if bc > threshold {
			if results[end-1].Count > bc {
				results[end] = Pair{Key: o.Key, Count: bc, category: o.category}
				threshold = bc
			} else {
				results[end+1] = Pair{Key: o.Key, Count: bc, category: o.category}
				sort.Sort(Pairs(results))
				threshold = results[end].Count
			}
		}
	}

	return results[:end]
}

/*
func (f *Fragment) TopFill(args FillArgs) ([]Pair, error) {
	result := make([]Pair, 0)
	for _, id := range args.Bitmaps {
		if _, ok := f.cache.Get(id); !ok {
			continue
		}

		if args.Handle == 0 {
			if bm := f.Bitmap(id); bm != nil && bm.Count() > 0 {
				result = append(result, Pair{Key: id, Count: bm.Count()})
			}
			continue
		}

		res := f.Intersect([]uint64{args.Handle, id})
		if res == nil {
			continue
		}

		if bc := res.BitCount(); bc > 0 {
			result = append(result, Pair{Key: id, Count: bc})
		}
	}
	return result, nil
}
*/

func (f *Fragment) Range(bitmapID uint64, start, end time.Time) *Bitmap {
	f.mu.Lock()
	defer f.mu.Unlock()

	bitmapIDs := GetRange(start, end, bitmapID)
	if len(bitmapIDs) == 0 {
		return NewBitmap()
	}

	result := f.bitmap(bitmapIDs[0])
	for _, id := range bitmapIDs[1:] {
		result = result.Union(f.bitmap(id))
	}
	return result
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
			// Determine the position of the bit in the storage.
			pos, err := f.pos(bitmapIDs[i], profileIDs[i])
			if err != nil {
				return err
			}

			// Write to storage.
			if err := f.storage.Add(pos); err != nil {
				return err
			}
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

// Snapshot writes the storage bitmap to disk and reopens it.
func (f *Fragment) Snapshot() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.snapshot()
}

func (f *Fragment) snapshot() error {
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

	return nil
}

func madvise(b []byte, advice int) (err error) {
	_, _, e1 := syscall.Syscall(syscall.SYS_MADVISE, uintptr(unsafe.Pointer(&b[0])), uintptr(len(b)), uintptr(advice))
	if e1 != 0 {
		err = e1
	}
	return
}
