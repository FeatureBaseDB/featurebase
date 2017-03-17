package pilosa

import (
	"bytes"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/golang/groupcache/lru"
	"github.com/pilosa/pilosa/internal"
)

// Cache represents a cache for bitmap counts.
type Cache interface {
	Add(bitmapID uint64, n uint64)
	BulkAdd(bitmapID uint64, n uint64)
	Get(bitmapID uint64) uint64
	Len() int

	// Returns a list of all bitmap IDs.
	BitmapIDs() []uint64

	// Updates the cache, if necessary.
	Invalidate()

	// Rebuilds the cache
	Recalculate()

	// Returns an ordered list of the top ranked bitmaps.
	Top() []BitmapPair
}

// LRUCache represents a least recently used Cache implemenation.
type LRUCache struct {
	cache  *lru.Cache
	counts map[uint64]uint64
}

// NewLRUCache returns a new instance of LRUCache.
func NewLRUCache(maxEntries int) *LRUCache {
	c := &LRUCache{
		cache:  lru.New(maxEntries),
		counts: make(map[uint64]uint64),
	}
	c.cache.OnEvicted = c.onEvicted
	return c
}

func (c *LRUCache) BulkAdd(bitmapID, n uint64) {
	c.Add(bitmapID, n)
}

// Add adds a bitmap to the cache.
func (c *LRUCache) Add(bitmapID, n uint64) {
	c.cache.Add(bitmapID, n)
	c.counts[bitmapID] = n
}

// Get returns a bitmap with a given id.
func (c *LRUCache) Get(bitmapID uint64) uint64 {
	n, _ := c.cache.Get(bitmapID)
	nn, _ := n.(uint64)
	return nn
}

// Len returns the number of items in the cache.
func (c *LRUCache) Len() int { return c.cache.Len() }

// Invalidate is a no-op.
func (c *LRUCache) Invalidate() {}

// Recalculate is a no-op.
func (c *LRUCache) Recalculate() {}

// BitmapIDs returns a list of all bitmap IDs in the cache.
func (c *LRUCache) BitmapIDs() []uint64 {
	a := make([]uint64, 0, len(c.counts))
	for id := range c.counts {
		a = append(a, id)
	}
	sort.Sort(uint64Slice(a))
	return a
}

// Top returns all counts in the cache.
func (c *LRUCache) Top() []BitmapPair {
	a := make([]BitmapPair, 0, len(c.counts))
	for id, n := range c.counts {
		a = append(a, BitmapPair{
			ID:    id,
			Count: uint64(n),
		})
	}
	sort.Sort(BitmapPairs(a))
	return a
}

func (c *LRUCache) onEvicted(key lru.Key, _ interface{}) { delete(c.counts, key.(uint64)) }

// Ensure LRUCache implements Cache.
var _ Cache = &LRUCache{}

// RankCache represents a cache with sorted entries.
type RankCache struct {
	mu       sync.Mutex
	entries  map[uint64]uint64
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
		entries: make(map[uint64]uint64),
	}
}

// Add adds a bitmap to the cache.
func (c *RankCache) Add(bitmapID uint64, n uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Ignore if the bit count on the bitmap is below the threshold.
	if n < c.ThresholdValue {
		return
	}

	c.entries[bitmapID] = n

	c.invalidate()
}

// BulkAdd adds a bitmap to the cache unsorted. You should Invalidate after completion.
func (c *RankCache) BulkAdd(bitmapID uint64, n uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if n < c.ThresholdValue {
		return
	}

	c.entries[bitmapID] = n
}

// Get returns a bitmap with a given id.
func (c *RankCache) Get(bitmapID uint64) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.entries[bitmapID]
}

// Len returns the number of items in the cache.
func (c *RankCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}

// BitmapIDs returns a list of all bitmap IDs in the cache.
func (c *RankCache) BitmapIDs() []uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	a := make([]uint64, 0, len(c.entries))
	for id := range c.entries {
		a = append(a, id)
	}
	sort.Sort(uint64Slice(a))
	return a
}

// Invalidate recalculates the the entries by rank.
func (c *RankCache) Invalidate() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.invalidate()
}

func (c *RankCache) Recalculate() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.recalculate()
}

func (c *RankCache) invalidate() {
	// Don't invalidate more than once every X seconds.
	// TODO: consider making this configurable.
	if time.Now().Sub(c.updateTime).Seconds() < 10 {
		return
	}
	c.recalculate()
}

func (c *RankCache) recalculate() {
	// Convert cache to a sorted list.
	rankings := make([]BitmapPair, 0, len(c.entries))
	for id, cnt := range c.entries {
		rankings = append(rankings, BitmapPair{
			ID:    id,
			Count: cnt,
		})
	}
	sort.Sort(BitmapPairs(rankings))

	// Store the count of the item at the threshold index.
	c.rankings = rankings
	if len(c.rankings) > c.ThresholdIndex {
		c.ThresholdValue = rankings[c.ThresholdIndex].Count
		c.rankings = c.rankings[0:c.ThresholdIndex]
	} else {
		c.ThresholdValue = 1
	}

	// Reset counters.
	c.updateTime, c.updateN = time.Now(), 0

	// If size is larger than the threshold then trim it.
	if len(c.entries) > c.ThresholdLength {
		for id, cnt := range c.entries {
			if cnt <= c.ThresholdValue {
				delete(c.entries, id)
			}
		}
	}
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
	ID    uint64
	Count uint64
}

// BitmapPairs is a sortable list of BitmapPair objects.
type BitmapPairs []BitmapPair

func (p BitmapPairs) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p BitmapPairs) Len() int           { return len(p) }
func (p BitmapPairs) Less(i, j int) bool { return p[i].Count > p[j].Count }

// Pair holds a bitmap id and its count.
type Pair struct {
	ID    uint64 `json:"id"`
	Count uint64 `json:"count"`
}

func encodePair(p Pair) *internal.Pair {
	return &internal.Pair{
		Key:   p.ID,
		Count: p.Count,
	}
}

func decodePair(pb *internal.Pair) Pair {
	return Pair{
		ID:    pb.Key,
		Count: pb.Count,
	}
}

// Pairs is a sortable slice of Pair objects.
type Pairs []Pair

func (p Pairs) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p Pairs) Len() int           { return len(p) }
func (p Pairs) Less(i, j int) bool { return p[i].Count > p[j].Count }

// PairHeap is a heap implementation over a group of Pairs.
type PairHeap struct {
	Pairs
}

func (p PairHeap) Less(i, j int) bool { return p.Pairs[i].Count < p.Pairs[j].Count }

func (h *Pairs) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(Pair))
}

func (h *Pairs) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// Add merges other into p and returns a new slice.
func (p Pairs) Add(other []Pair) []Pair {
	// Create lookup of key/counts.
	m := make(map[uint64]uint64, len(p))
	for _, pair := range p {
		m[pair.ID] = pair.Count
	}

	// Add/merge from other.
	for _, pair := range other {
		m[pair.ID] += pair.Count
	}

	// Convert back to slice.
	a := make([]Pair, 0, len(m))
	for k, v := range m {
		a = append(a, Pair{ID: k, Count: v})
	}
	return a
}

// Keys returns a slice of all keys in p.
func (p Pairs) Keys() []uint64 {
	a := make([]uint64, len(p))
	for i := range p {
		a[i] = p[i].ID
	}
	return a
}

func (p Pairs) String() string {
	var buf bytes.Buffer
	buf.WriteString("Pairs(")
	for i := range p {
		fmt.Fprintf(&buf, "%d/%d", p[i].ID, p[i].Count)
		if i < len(p)-1 {
			buf.WriteString(", ")
		}
	}
	buf.WriteString(")")
	return buf.String()
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

// BitmapCache provides an interface for caching full bitmaps.
type BitmapCache interface {
	Fetch(id uint64) (*Bitmap, bool)
	Add(id uint64, b *Bitmap)
}

// SimpleCache implements BitmapCache
// it is meant to be a short-lived cache for cases where writes are continuing to access
// the same bit within a short time frame (i.e. good for write-heavy loads)
// A read-heavy use case would cause the cache to get bigger, potentially causing the
// node to run out of memory.
type SimpleCache struct {
	cache map[uint64]*Bitmap
}

// Fetch retrieves the bitmap at the id in the cache.
func (s *SimpleCache) Fetch(id uint64) (*Bitmap, bool) {
	m, ok := s.cache[id]
	return m, ok
}

// Add adds the bitmap to the cache, keyed on the id.
func (s *SimpleCache) Add(id uint64, b *Bitmap) {
	s.cache[id] = b
}
