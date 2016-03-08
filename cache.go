package pilosa

import (
	"io"
	"sort"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/groupcache/lru"
	"github.com/umbel/pilosa/internal"
)

// Cache represents a cache for bitmaps.
type Cache interface {
	Add(bitmapID uint64, bm *Bitmap)
	Get(bitmapID uint64) *Bitmap
	Len() int

	// Returns a list of all bitmap IDs.
	BitmapIDs() []uint64

	// Updates the cache, if necessary.
	Invalidate()

	// Returns an ordered list of the top ranked bitmaps.
	Top() []BitmapPair
}

// LRUCache represents a least recently used Cache implemenation.
type LRUCache struct {
	cache   *lru.Cache
	bitmaps map[uint64]*Bitmap
}

// NewLRUCache returns a new instance of LRUCache.
func NewLRUCache(maxEntries int) *LRUCache {
	c := &LRUCache{
		cache:   lru.New(maxEntries),
		bitmaps: make(map[uint64]*Bitmap),
	}
	c.cache.OnEvicted = c.onEvicted
	return c
}

// Add adds a bitmap to the cache.
func (c *LRUCache) Add(bitmapID uint64, bm *Bitmap) {
	c.cache.Add(bitmapID, bm)
	c.bitmaps[bitmapID] = bm
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

// BitmapIDs returns a list of all bitmap IDs in the cache.
func (c *LRUCache) BitmapIDs() []uint64 {
	a := make([]uint64, 0, len(c.bitmaps))
	for id := range c.bitmaps {
		a = append(a, id)
	}
	sort.Sort(uint64Slice(a))
	return a
}

// Top returns all bitmaps in the cache.
func (c *LRUCache) Top() []BitmapPair {
	a := make([]BitmapPair, 0, len(c.bitmaps))
	for id, bm := range c.bitmaps {
		a = append(a, BitmapPair{
			ID:     id,
			Bitmap: bm,
		})
	}
	sort.Sort(BitmapPairs(a))
	return a
}

func (c *LRUCache) onEvicted(key lru.Key, _ interface{}) { delete(c.bitmaps, key.(uint64)) }

// Ensure LRUCache implements Cache.
var _ Cache = &LRUCache{}

// RankCache represents a cache with sorted entries.
type RankCache struct {
	entries  map[uint64]*Bitmap
	rankings []BitmapPair // cached, ordered list

	updateN    int
	updateTime time.Time

	ThresholdLength int
	ThresholdIndex  int
	ThresholdValue  uint64
}

// NewRankCache returns a new instance of RankCache.
func NewRankCache() *RankCache {
	return &RankCache{
		entries: make(map[uint64]*Bitmap),
	}
}

// Add adds a bitmap to the cache.
func (c *RankCache) Add(bitmapID uint64, bm *Bitmap) {
	// Ignore if the bit count on the bitmap is below the threshold.
	if bm.Count() < c.ThresholdValue {
		return
	}

	// Add to cache.
	c.entries[bitmapID] = bm

	// If size is larger than the threshold then trim it.
	if len(c.entries) > c.ThresholdLength {
		c.update()
		for id, bm := range c.entries {
			if bm.Count() <= c.ThresholdValue {
				delete(c.entries, id)
			}
		}
	}
}

// Get returns a bitmap with a given id.
func (c *RankCache) Get(bitmapID uint64) *Bitmap { return c.entries[bitmapID] }

// Len returns the number of items in the cache.
func (c *RankCache) Len() int { return len(c.entries) }

// BitmapIDs returns a list of all bitmap IDs in the cache.
func (c *RankCache) BitmapIDs() []uint64 {
	a := make([]uint64, 0, len(c.entries))
	for id := range c.entries {
		a = append(a, id)
	}
	sort.Sort(uint64Slice(a))
	return a
}

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
	rankings := make([]BitmapPair, 0, len(c.entries))
	for id, bm := range c.entries {
		rankings = append(rankings, BitmapPair{
			ID:     id,
			Bitmap: bm,
		})
	}
	sort.Sort(BitmapPairs(rankings))

	// Store the count of the item at the threshold index.
	c.rankings = rankings
	if len(c.rankings) > c.ThresholdIndex {
		c.ThresholdValue = rankings[c.ThresholdIndex].Bitmap.Count()
	} else {
		c.ThresholdValue = 1
	}

	// Reset counters.
	c.updateTime, c.updateN = time.Now(), 0
}

// Top returns an ordered list of bitmaps.
func (c *RankCache) Top() []BitmapPair { return c.rankings }

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

// BitmapPair represents a bitmap with an associated identifier.
type BitmapPair struct {
	ID     uint64
	Bitmap *Bitmap
}

// BitmapPairs is a sortable list of BitmapPair objects.
type BitmapPairs []BitmapPair

func (p BitmapPairs) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p BitmapPairs) Len() int           { return len(p) }
func (p BitmapPairs) Less(i, j int) bool { return p[i].Bitmap.Count() > p[j].Bitmap.Count() }

type Pair struct {
	Key   uint64 `json:"key"`
	Count uint64 `json:"count"`
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

// Add merges other into p and returns a new slice.
func (p Pairs) Add(other []Pair) []Pair {
	// Create lookup of key/counts.
	m := make(map[uint64]uint64, len(p))
	for _, pair := range p {
		m[pair.Key] = pair.Count
	}

	// Add/merge from other.
	for _, pair := range other {
		m[pair.Key] += pair.Count
	}

	// Convert back to slice.
	a := make([]Pair, 0, len(m))
	for k, v := range m {
		a = append(a, Pair{Key: k, Count: v})
	}
	return a
}

// Keys returns a slice of all keys in p.
func (p Pairs) Keys() []uint64 {
	a := make([]uint64, len(p))
	for i := range p {
		a[i] = p[i].Key
	}
	return a
}

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

// uint64Slice represents a sortable slice of uint64 numbers.
type uint64Slice []uint64

func (p uint64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p uint64Slice) Len() int           { return len(p) }
func (p uint64Slice) Less(i, j int) bool { return p[i] < p[j] }

// merge combines p and other to a unique sorted set of values.
// p and other must both have unique sets and be sorted.
func (p uint64Slice) merge(other []uint64) []uint64 {
	ret := make([]uint64, 0, len(p))

	i, j := 0, 0
	for i < len(p) && j < len(other) {
		a, b := p[i], other[j]
		if a == b {
			ret = append(ret, a)
			i, j = i+1, j+1
		} else if a < b {
			ret = append(ret, a)
			i++
		} else {
			ret = append(ret, b)
			j++
		}
	}

	if i < len(p) {
		ret = append(ret, p[i:]...)
	} else if j < len(other) {
		ret = append(ret, other[j:]...)
	}

	return ret
}
