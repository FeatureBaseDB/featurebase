package pilosa

import (
	"encoding/json"
	"io"
	"sort"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/groupcache/lru"
	"github.com/umbel/pilosa/internal"
)

// Cache represents a cache for bitmaps.
type Cache interface {
	io.WriterTo
	io.ReaderFrom

	Add(bitmapID, category uint64, bm *Bitmap)
	Get(bitmapID uint64) *Bitmap
	Len() int

	// Updates the cache, if necessary.
	Invalidate()

	// Returns a list of all key/count pairs.
	Pairs() []Pair
}

// LRUCache represents a least recently used Cache implemenation.
type LRUCache struct {
	cache *lru.Cache
	keys  map[uint64]struct{}
}

// NewLRUCache returns a new instance of LRUCache.
func NewLRUCache(maxEntries int) *LRUCache {
	c := &LRUCache{
		cache: lru.New(maxEntries),
		keys:  make(map[uint64]struct{}),
	}
	c.cache.OnEvicted = c.onEvicted
	return c
}

// Get returns a bitmap with a given id.
func (c *LRUCache) Add(bitmapID, category uint64, bm *Bitmap) {
	c.cache.Add(bitmapID, bm)
	c.keys[bitmapID] = struct{}{}
}

// Get returns a bitmap with a given id.
func (c *LRUCache) Get(bitmapID uint64) *Bitmap {
	bm, ok := c.cache.Get(bitmapID)
	if !ok {
		return nil
	}
	return bm.(*Bitmap)
}

// Len returns the number of items in the cache.
func (c *LRUCache) Len() int { return c.cache.Len() }

// Invalidate is a no-op.
func (c *LRUCache) Invalidate() {}

// Pairs returns all key/count pairs in the cache.
func (c *LRUCache) Pairs() []Pair {
	a := make([]Pair, 0, len(c.keys))
	for k := range c.keys {
		a = append(a, Pair{
			Key:   k,
			Count: c.Get(k).Count(),
		})
	}
	return a
}

// WriteTo writes the cache to w.
func (c *LRUCache) WriteTo(w io.Writer) (n int64, err error) {
	// Write keys to slice.
	a := make([]uint64, 0, len(c.keys))
	for k := range c.keys {
		a = append(a, k)
	}

	// Encode to file as array of keys.
	if err := json.NewEncoder(w).Encode(a); err != nil {
		return 0, err
	}
	return 0, nil
}

// ReadFrom read from r into the cache.
func (c *LRUCache) ReadFrom(r io.Reader) (n int64, err error) {
	var keys []uint64
	if err := json.NewDecoder(r).Decode(&keys); err != nil {
		return 0, err
	}
	panic("FIXME: TODO")
}

func (c *LRUCache) onEvicted(key lru.Key, _ interface{}) {
	delete(c.keys, key.(uint64))
}

// Ensure LRUCache implements Cache.
var _ Cache = &LRUCache{}

// RankCache represents a cache with sorted entries.
type RankCache struct {
	entries  map[uint64]*Pair
	rankings []Pair // cached, ordered list

	updateN    int
	updateTime time.Time

	ThresholdLength int
	ThresholdIndex  int
	ThresholdValue  uint64
}

// NewRankCache returns a new instance of RankCache.
func NewRankCache() *RankCache {
	return &RankCache{
		entries: make(map[uint64]*Pair),
	}
}

// Get returns a bitmap with a given id.
func (c *RankCache) Add(bitmapID, category uint64, bm *Bitmap) {
	// Ignore if the bit count on the bitmap is below the threshold.
	if bm.Count() < c.ThresholdValue {
		return
	}

	// Add to cache.
	c.entries[bitmapID] = &Pair{
		Key:      bitmapID,
		Count:    bm.Count(),
		bitmap:   bm,
		category: category,
	}

	// If size is larger than the threshold then trim it.
	if len(c.entries) > c.ThresholdLength {
		c.update()
		for k, entry := range c.entries {
			if entry.bitmap.Count() <= c.ThresholdValue {
				delete(c.entries, k)
			}
		}
	}
}

// Get returns a bitmap with a given id.
func (c *RankCache) Get(bitmapID uint64) *Bitmap {
	entry, ok := c.entries[bitmapID]
	if !ok {
		return nil
	}
	return entry.bitmap
}

// Len returns the number of items in the cache.
func (c *RankCache) Len() int { return len(c.entries) }

// Invalidate reorders the entries, if necessary.
func (c *RankCache) Invalidate() {
	// Update if there aren't many items or it hasn't been updated recently.
	if len(c.rankings) < 50 || (c.updateN > 0 && time.Since(c.updateTime) > 5*time.Minute) {
		c.update()
	}
}

// update reorders the entries by rank.
func (c *RankCache) update() {
	// Convert cache to a sorted list.
	list := make([]Pair, 0, len(c.entries))
	for k, item := range c.entries {
		list = append(list, Pair{
			Key:      k,
			Count:    item.bitmap.Count(),
			bitmap:   item.bitmap,
			category: item.category,
		})
	}
	sort.Sort(Pairs(list))

	// Store the count of the item at the threshold index.
	c.rankings = list
	if len(c.rankings) > c.ThresholdIndex {
		c.ThresholdValue = list[c.ThresholdIndex].bitmap.Count()
	} else {
		c.ThresholdValue = 1
	}

	// Reset counters.
	c.updateTime, c.updateN = time.Now(), 0
}

// Pairs returns an ordered list of key/count pairs.
func (c *RankCache) Pairs() []Pair { return c.rankings }

// WriteTo writes the cache to w.
func (c *RankCache) WriteTo(w io.Writer) (n int64, err error) {
	panic("FIXME: TODO")
}

// ReadFrom read from r into the cache.
func (c *RankCache) ReadFrom(r io.Reader) (n int64, err error) {
	panic("FIXME: TODO")
}

// Ensure RankCache implements Cache.
var _ Cache = &RankCache{}

type Pair struct {
	Key   uint64 `json:"key"`
	Count uint64 `json:"count"`

	bitmap   *Bitmap
	category uint64
}

func encodePair(p Pair) *internal.Pair {
	return &internal.Pair{
		Key:   proto.Uint64(p.Key),
		Count: proto.Uint64(p.Count),
	}
}

func decodePair(pb *internal.Pair) Pair {
	return Pair{
		Key:   pb.GetKey(),
		Count: pb.GetCount(),
	}
}

type Pairs []Pair

func (p Pairs) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p Pairs) Len() int           { return len(p) }
func (p Pairs) Less(i, j int) bool { return p[i].Count > p[j].Count }

func encodePairs(a Pairs) []*internal.Pair {
	other := make([]*internal.Pair, len(a))
	for i := range a {
		other[i] = encodePair(a[i])
	}
	return other
}

func decodePairs(a []*internal.Pair) []Pair {
	other := make([]Pair, len(a))
	for i := range a {
		other[i] = decodePair(a[i])
	}
	return other
}
