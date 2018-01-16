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
	"bytes"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/golang/groupcache/lru"
	"github.com/pilosa/pilosa/internal"
)

const (
	// ThresholdFactor is used to calculate the threshold for new items entering the cache
	ThresholdFactor = 1.1
)

// Cache represents a cache of counts.
type Cache interface {
	Add(id uint64, n uint64)
	BulkAdd(id uint64, n uint64)
	Get(id uint64) uint64
	Len() int

	// Returns a list of all IDs.
	IDs() []uint64

	// Updates the cache, if necessary.
	Invalidate()

	// Rebuilds the cache
	Recalculate()

	// Returns an ordered list of the top ranked bitmaps.
	Top() []BitmapPair

	// SetStats defines the stats client used in the cache.
	SetStats(s StatsClient)
}

// LRUCache represents a least recently used Cache implementation.
type LRUCache struct {
	cache  *lru.Cache
	counts map[uint64]uint64
	stats  StatsClient
}

// NewLRUCache returns a new instance of LRUCache.
func NewLRUCache(maxEntries uint32) *LRUCache {
	c := &LRUCache{
		cache:  lru.New(int(maxEntries)),
		counts: make(map[uint64]uint64),
		stats:  NopStatsClient,
	}
	c.cache.OnEvicted = c.onEvicted
	return c
}

// BulkAdd adds a count to the cache unsorted. You should Invalidate after completion.
func (c *LRUCache) BulkAdd(id, n uint64) {
	c.Add(id, n)
}

// Add adds a count to the cache.
func (c *LRUCache) Add(id, n uint64) {
	c.cache.Add(id, n)
	c.counts[id] = n
}

// Get returns a count for a given id.
func (c *LRUCache) Get(id uint64) uint64 {
	n, _ := c.cache.Get(id)
	nn, _ := n.(uint64)
	return nn
}

// Len returns the number of items in the cache.
func (c *LRUCache) Len() int { return c.cache.Len() }

// Invalidate is a no-op.
func (c *LRUCache) Invalidate() {}

// Recalculate is a no-op.
func (c *LRUCache) Recalculate() {}

// IDs returns a list of all IDs in the cache.
func (c *LRUCache) IDs() []uint64 {
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

// SetStats defines the stats client used in the cache.
func (c *LRUCache) SetStats(s StatsClient) {
	c.stats = s
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

	// maxEntries is the user defined size of the cache
	maxEntries uint32

	// thresholdBuffer is used the calculate the lowest cached threshold value
	// This threshold determines what new items are added to the cache
	thresholdBuffer int

	// thresholdValue is the value of the last item in the cache
	thresholdValue uint64

	stats StatsClient
}

// NewRankCache returns a new instance of RankCache.
func NewRankCache(maxEntries uint32) *RankCache {
	return &RankCache{
		maxEntries:      maxEntries,
		thresholdBuffer: int(ThresholdFactor * float64(maxEntries)),
		entries:         make(map[uint64]uint64),
		stats:           NopStatsClient,
	}
}

// Add adds a count to the cache.
func (c *RankCache) Add(id uint64, n uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// Ignore if the bit count is below the threshold.
	if n < c.thresholdValue {
		return
	}

	c.entries[id] = n

	c.invalidate()
}

// BulkAdd adds a count to the cache unsorted. You should Invalidate after completion.
func (c *RankCache) BulkAdd(id uint64, n uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if n < c.thresholdValue {
		return
	}

	c.entries[id] = n
}

// Get returns a count for a given id.
func (c *RankCache) Get(id uint64) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.entries[id]
}

// Len returns the number of items in the cache.
func (c *RankCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}

// IDs returns a list of all IDs in the cache.
func (c *RankCache) IDs() []uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	a := make([]uint64, 0, len(c.entries))
	for id := range c.entries {
		a = append(a, id)
	}
	sort.Sort(uint64Slice(a))
	return a
}

// Invalidate recalculates the entries by rank.
func (c *RankCache) Invalidate() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.invalidate()
}

// Recalculate rebuilds the cache.
func (c *RankCache) Recalculate() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stats.Count("cache.recalculate", 1, 1.0)
	c.recalculate()
}

func (c *RankCache) invalidate() {
	// Don't invalidate more than once every X seconds.
	// TODO: consider making this configurable.
	if time.Now().Sub(c.updateTime).Seconds() < 10 {
		return
	}
	c.stats.Count("cache.invalidate", 1, 1.0)
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
	length := len(c.rankings)
	c.stats.Gauge("RankCache", float64(length), 1.0)

	var removeItems []BitmapPair // cached, ordered list
	if length > int(c.maxEntries) {
		c.thresholdValue = rankings[c.maxEntries].Count
		removeItems = c.rankings[c.maxEntries:]
		c.rankings = c.rankings[0:c.maxEntries]
	} else {
		c.thresholdValue = 1
	}

	// Reset counters.
	c.updateTime, c.updateN = time.Now(), 0

	// If size is larger than the threshold then trim it.
	if len(c.entries) > c.thresholdBuffer {
		c.stats.Count("cache.threshold", 1, 1.0)
		for _, pair := range removeItems {
			delete(c.entries, pair.ID)
		}
	}
}

// SetStats defines the stats client used in the cache.
func (c *RankCache) SetStats(s StatsClient) {
	c.stats = s
}

// Top returns an ordered list of pairs.
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

// BitmapPair represents a id/count pair with an associated identifier.
type BitmapPair struct {
	ID    uint64
	Count uint64
}

// BitmapPairs is a sortable list of BitmapPair objects.
type BitmapPairs []BitmapPair

func (p BitmapPairs) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p BitmapPairs) Len() int           { return len(p) }
func (p BitmapPairs) Less(i, j int) bool { return p[i].Count > p[j].Count }

// Pair holds an id/count pair.
type Pair struct {
	ID    uint64 `json:"id"`
	Key   string `json:"key,omitempty"`
	Count uint64 `json:"count"`
}

func encodePair(p Pair) *internal.Pair {
	return &internal.Pair{
		ID:    p.ID,
		Key:   p.Key,
		Count: p.Count,
	}
}

func decodePair(pb *internal.Pair) Pair {
	return Pair{
		ID:    pb.ID,
		Key:   pb.Key,
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

// Less implemets the Sort interface.
// reports whether the element with index i should sort before the element with index j.
func (p PairHeap) Less(i, j int) bool { return p.Pairs[i].Count < p.Pairs[j].Count }

// Push appends the element onto the Pair slice.
func (p *Pairs) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*p = append(*p, x.(Pair))
}

// Pop removes the minimum element from the Pair slice.
func (p *Pairs) Pop() interface{} {
	old := *p
	n := len(old)
	x := old[n-1]
	*p = old[0 : n-1]
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

// NopCache represents a no-op Cache implementation.
type NopCache struct {
	stats StatsClient
}

// Ensure NopCache implements Cache.
var _ Cache = &NopCache{}

// NewNopCache returns a new instance of NopCache.
func NewNopCache() *NopCache {
	return &NopCache{
		stats: NopStatsClient,
	}
}

func (c *NopCache) Add(id uint64, n uint64)     {}
func (c *NopCache) BulkAdd(id uint64, n uint64) {}
func (c *NopCache) Get(id uint64) uint64        { return 0 }
func (c *NopCache) IDs() []uint64               { return make([]uint64, 0, 0) }

func (c *NopCache) Invalidate() {}
func (c *NopCache) Len() int    { return 0 }
func (c *NopCache) Recalculate() {
}
func (c *NopCache) SetStats(s StatsClient) {
	c.stats = s
}

func (c *NopCache) Top() []BitmapPair {
	return []BitmapPair{}
}
