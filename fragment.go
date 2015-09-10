package pilosa

import (
	"strings"
	"sync"
	"time"

	log "github.com/cihub/seelog"
	"github.com/golang/groupcache/lru"
)

type Fragment struct {
	mu    sync.Mutex
	id    SUUID
	slice int
	impl  Pilosa
	cache *lru.Cache

	// Provide an autoincrementing index for bitmap handles.
	seq uint64

	// Stats for how many messages have been processed.
	stats FragmentStats
}

func NewFragment(id SUUID, db string, slice int, frame string) *Fragment {
	storage := NewStorage(Backend, StorageOptions{
		DB:          db,
		Slice:       slice,
		Frame:       frame,
		FragmentID:  id,
		LevelDBPath: LevelDBPath,
	})

	var impl Pilosa
	if strings.HasSuffix(frame, ".n") {
		impl = NewBrand(db, frame, slice, storage, 50000, 45000, 100)
	} else {
		impl = NewGeneral(db, frame, slice, storage)
	}

	return &Fragment{
		id:    id,
		cache: lru.New(50000),
		impl:  impl,
		slice: slice,
	}
}

func (f *Fragment) Bitmap(bh BitmapHandle) (*Bitmap, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.bitmap(bh)
}

func (f *Fragment) bitmap(bh BitmapHandle) (*Bitmap, bool) {
	bm, ok := f.cache.Get(bh)
	if ok && bm != nil {
		return bm.(*Bitmap), ok
	}
	return NewBitmap(), false //cache fail
}

func (f *Fragment) exists(bitmapID uint64) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.impl.Exists(bitmapID)
}

func (f *Fragment) TopNAll(n int, categories []uint64) []Pair {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.impl.TopNAll(n, categories)
}

func (f *Fragment) TopN(bitmap BitmapHandle, n int, categories []uint64) []Pair {
	f.mu.Lock()
	defer f.mu.Unlock()

	bm, ok := f.cache.Get(bitmap)
	if ok {
		return f.impl.TopN(bm.(*Bitmap), n, categories)
	}
	return nil
}

func (f *Fragment) NewHandle(bitmapID uint64) BitmapHandle {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.allocHandle(f.impl.Get(bitmapID))
}

func (f *Fragment) AllocHandle(bm *Bitmap) BitmapHandle {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.allocHandle(bm)
}

func (f *Fragment) allocHandle(bm *Bitmap) BitmapHandle {
	handle := f.nextHandle()
	f.cache.Add(handle, bm)
	return handle
}

func (f *Fragment) nextHandle() BitmapHandle {
	millis := uint64(time.Now().UTC().UnixNano())
	id := millis << (64 - 41)
	id |= uint64(f.slice) << (64 - 41 - 13)
	id |= f.seq % 1024
	f.seq += 1
	return BitmapHandle(id)
}

func (f *Fragment) Union(bitmaps []BitmapHandle) BitmapHandle {
	f.mu.Lock()
	defer f.mu.Unlock()

	result := NewBitmap()
	for i, id := range bitmaps {
		bm, _ := f.bitmap(id)
		if i == 0 {
			result = bm
		} else {
			result = result.Union(bm)
		}
	}
	return f.allocHandle(result)
}

func (f *Fragment) build_time_range_bitmap(bitmapID uint64, start, end time.Time) BitmapHandle {
	f.mu.Lock()
	defer f.mu.Unlock()

	result := NewBitmap()
	for i, bid := range GetRange(start, end, bitmapID) {
		bm := f.impl.Get(bid)
		if i == 0 {
			result = bm
		} else {
			result = result.Union(bm)
		}
	}
	return f.AllocHandle(result)
}

func (f *Fragment) Intersect(bitmaps []BitmapHandle) BitmapHandle {
	f.mu.Lock()
	defer f.mu.Unlock()

	var result *Bitmap
	for i, id := range bitmaps {
		bm, _ := f.bitmap(id)
		if i == 0 {
			result = bm.Clone()
		} else {
			result = result.Intersection(bm)
		}
	}
	return f.allocHandle(result)
}

func (f *Fragment) Difference(bitmaps []BitmapHandle) BitmapHandle {
	f.mu.Lock()
	defer f.mu.Unlock()

	result := NewBitmap()
	for i, id := range bitmaps {
		bm, _ := f.bitmap(id)
		if i == 0 {
			result = bm
		} else {
			result = result.Difference(bm)
		}
	}
	return f.allocHandle(result)
}

func (f *Fragment) Persist() {
	f.mu.Lock()
	defer f.mu.Unlock()

	err := f.impl.Persist()
	if err != nil {
		log.Warn("Error saving:", err)
	}
}

func (f *Fragment) Load() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.impl.Load(f)
}

// FragmentStats represents in-memory stats for a single fragment.
type FragmentStats struct {
	// Messages processed by the fragment
	ProcessN    uint64
	ProcessTime time.Duration
}

type Pilosa interface {
	Get(id uint64) *Bitmap
	SetBit(id uint64, bit_pos uint64, filter uint64) bool
	ClearBit(id uint64, bit_pos uint64) bool
	TopN(b *Bitmap, n int, categories []uint64) []Pair
	TopNAll(n int, categories []uint64) []Pair
	Clear() bool
	Store(bitmapID uint64, bm *Bitmap, filter uint64) error
	Stats() interface{}
	Persist() error
	Load(fragment *Fragment)
	Exists(id uint64) bool
}
