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
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/molecula/featurebase/v2/lru"
	pb "github.com/molecula/featurebase/v2/proto"
	"github.com/molecula/featurebase/v2/stats"
	"github.com/pkg/errors"
)

const (
	// thresholdFactor is used to calculate the threshold for new items entering the cache
	thresholdFactor = 1.1
)

// cache represents a cache of counts.
type cache interface {
	Add(id uint64, n uint64)
	BulkAdd(id uint64, n uint64)
	Get(id uint64) uint64
	Len() int

	// Returns a list of all IDs.
	IDs() []uint64

	// Soft ask for the cache to be rebuilt - may not if it has been done recently.
	Invalidate()

	// Rebuilds the cache.
	Recalculate()

	// Returns an ordered list of the top ranked bitmaps.
	Top() []bitmapPair

	// SetStats defines the stats client used in the cache.
	SetStats(s stats.StatsClient)
}

// lruCache represents a least recently used Cache implementation.
type lruCache struct {
	cache  *lru.Cache
	counts map[uint64]uint64
	stats  stats.StatsClient
}

// newLRUCache returns a new instance of LRUCache.
func newLRUCache(maxEntries uint32) *lruCache {
	c := &lruCache{
		cache:  lru.New(int(maxEntries)),
		counts: make(map[uint64]uint64),
		stats:  stats.NopStatsClient,
	}
	c.cache.OnEvicted = c.onEvicted
	return c
}

// BulkAdd adds a count to the cache unsorted. You should Invalidate after completion.
func (c *lruCache) BulkAdd(id, n uint64) {
	c.Add(id, n)
}

// Add adds a count to the cache.
func (c *lruCache) Add(id, n uint64) {
	c.cache.Add(id, n)
	c.counts[id] = n
}

// Get returns a count for a given id.
func (c *lruCache) Get(id uint64) uint64 {
	n, _ := c.cache.Get(id)
	nn, _ := n.(uint64)
	return nn
}

// Len returns the number of items in the cache.
func (c *lruCache) Len() int { return c.cache.Len() }

// Invalidate is a no-op.
func (c *lruCache) Invalidate() {}

// Recalculate is a no-op.
func (c *lruCache) Recalculate() {}

// IDs returns a list of all IDs in the cache.
func (c *lruCache) IDs() []uint64 {
	a := make([]uint64, 0, len(c.counts))
	for id := range c.counts {
		a = append(a, id)
	}
	sort.Sort(uint64Slice(a))
	return a
}

// Top returns all counts in the cache.
func (c *lruCache) Top() []bitmapPair {
	a := make([]bitmapPair, 0, len(c.counts))
	for id, n := range c.counts {
		a = append(a, bitmapPair{
			ID:    id,
			Count: n,
		})
	}
	pairs := bitmapPairs(a)
	sort.Sort(&pairs)
	return a
}

// SetStats defines the stats client used in the cache.
func (c *lruCache) SetStats(s stats.StatsClient) {
	c.stats = s
}

func (c *lruCache) onEvicted(key lru.Key, _ interface{}) { delete(c.counts, key.(uint64)) }

// Ensure LRUCache implements Cache.
var _ cache = &lruCache{}

// rankCache represents a cache with sorted entries.
type rankCache struct {
	mu           sync.Mutex
	entries      map[uint64]uint64
	rankings     bitmapPairs // cached, ordered list
	rankingsRead bool
	dirty        bool

	updateN    int
	updateTime time.Time

	// maxEntries is the user defined size of the cache
	maxEntries uint32

	// thresholdBuffer is used the calculate the lowest cached threshold value
	// This threshold determines what new items are added to the cache
	thresholdBuffer int

	// thresholdValue is the value of the last item in the cache
	thresholdValue uint64

	stats stats.StatsClient
}

// NewRankCache returns a new instance of RankCache.
func NewRankCache(maxEntries uint32) *rankCache {
	return &rankCache{
		maxEntries:      maxEntries,
		thresholdBuffer: int(thresholdFactor * float64(maxEntries)),
		entries:         make(map[uint64]uint64),
		stats:           stats.NopStatsClient,
	}
}

// Add adds a count to the cache.
func (c *rankCache) Add(id uint64, n uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Flag the cache as dirty.
	// This forces recalculation if top is called before the cache is recalculated.
	c.dirty = true

	// Ignore if the column count is below the threshold,
	// unless the count is 0, which is effectively used
	// to clear the cache value.
	if n < c.thresholdValue && n > 0 {
		delete(c.entries, id)
		return
	}

	c.entries[id] = n

	c.invalidate()
}

// BulkAdd adds a count to the cache unsorted. You should Invalidate after completion.
func (c *rankCache) BulkAdd(id uint64, n uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Flag the cache as dirty.
	// This forces recalculation if top is called before the cache is recalculated.
	c.dirty = true

	if n < c.thresholdValue {
		delete(c.entries, id)
		return
	}

	c.entries[id] = n
}

// Get returns a count for a given id.
func (c *rankCache) Get(id uint64) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.entries[id]
}

// Len returns the number of items in the cache.
func (c *rankCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}

// IDs returns a list of all IDs in the cache.
func (c *rankCache) IDs() []uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.entries) == 0 {
		return nil
	}
	ids := make([]uint64, 0, len(c.entries))
	for id := range c.entries {
		ids = append(ids, id)
	}
	sort.Sort(uint64Slice(ids))
	return ids
}

// Invalidate recalculates the entries by rank.
func (c *rankCache) Invalidate() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.invalidate()
}

// Recalculate rebuilds the cache.
func (c *rankCache) Recalculate() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stats.Count(MetricRecalculateCache, 1, 1.0)
	c.recalculate()
}

func (c *rankCache) invalidate() {
	// Don't invalidate more than once every X seconds.
	// TODO: consider making this configurable.
	if time.Since(c.updateTime).Seconds() < 10 {
		// Skipping recalculation means that the ranked cache's growth is unbounded.
		// This is somewhat necessary for now since recalculation is not cheap.
		// The cache will remain flagged as dirty and will be recalculated if Top is called.
		// This may cause unexpected memory growth, so record it in metrics for debugging purposes.
		c.stats.Count(MetricInvalidateCacheSkipped, 1, 1.0)
		// Ensure that we're marked as dirty even if we weren't otherwise.
		c.dirty = true
		return
	}
	c.stats.Count(MetricInvalidateCache, 1, 1.0)
	c.recalculate()
}

func (c *rankCache) recalculate() {
	if c.rankingsRead {
		c.rankings = nil
		c.rankingsRead = false
	}

	// Convert cache to a sorted list.
	rankings := c.rankings[:0]
	if cap(rankings) < len(c.entries) {
		rankings = make([]bitmapPair, 0, len(c.entries))
	}
	for id, cnt := range c.entries {
		rankings = append(rankings, bitmapPair{
			ID:    id,
			Count: cnt,
		})
	}
	c.rankings = rankings
	sort.Sort(&c.rankings)

	// Store the count of the item at the threshold index.
	length := len(c.rankings)
	c.stats.Gauge(MetricRankCacheLength, float64(length), 1.0)

	var removeItems []bitmapPair // cached, ordered list
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
		c.stats.Count(MetricCacheThresholdReached, 1, 1.0)
		for _, pair := range removeItems {
			delete(c.entries, pair.ID)
		}
	}

	// The cache is no longer dirty.
	c.dirty = false
}

// SetStats defines the stats client used in the cache.
func (c *rankCache) SetStats(s stats.StatsClient) {
	c.stats = s
}

// Top returns an ordered list of pairs.
func (c *rankCache) Top() []bitmapPair {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.dirty {
		// The cache is dirty, so we need to recalculate it to get a consistent view.
		c.stats.Count(MetricReadDirtyCache, 1, 1.0)
		c.recalculate()
	}

	c.rankingsRead = true
	return c.rankings
}

// WriteTo writes the cache to w.
func (c *rankCache) WriteTo(w io.Writer) (n int64, err error) {
	panic("FIXME: TODO")
}

// ReadFrom read from r into the cache.
func (c *rankCache) ReadFrom(r io.Reader) (n int64, err error) {
	panic("FIXME: TODO")
}

// Ensure RankCache implements Cache.
var _ cache = &rankCache{}

// bitmapPair represents a id/count pair with an associated identifier.
type bitmapPair struct {
	ID    uint64
	Count uint64
}

// bitmapPairs is a sortable list of BitmapPair objects.
type bitmapPairs []bitmapPair

func (p *bitmapPairs) Swap(i, j int)      { (*p)[i], (*p)[j] = (*p)[j], (*p)[i] }
func (p *bitmapPairs) Len() int           { return len(*p) }
func (p *bitmapPairs) Less(i, j int) bool { return (*p)[i].Count > (*p)[j].Count }

// Pair holds an id/count pair.
type Pair struct {
	ID    uint64 `json:"id"`
	Key   string `json:"key"`
	Count uint64 `json:"count"`
}

// PairField is a Pair with its associated field.
type PairField struct {
	Pair  Pair
	Field string
}

func (p PairField) Clone() (r PairField) {
	return PairField{
		Pair:  p.Pair,
		Field: p.Field,
	}
}

// ToTable implements the ToTabler interface.
func (p PairField) ToTable() (*pb.TableResponse, error) {
	return pb.RowsToTable(p, 1)
}

// ToRows implements the ToRowser interface.
func (p PairField) ToRows(callback func(*pb.RowResponse) error) error {
	if p.Pair.Key != "" {
		return callback(&pb.RowResponse{
			Headers: []*pb.ColumnInfo{
				{Name: p.Field, Datatype: "string"},
				{Name: "count", Datatype: "uint64"},
			},
			Columns: []*pb.ColumnResponse{
				{ColumnVal: &pb.ColumnResponse_StringVal{StringVal: p.Pair.Key}},
				{ColumnVal: &pb.ColumnResponse_Uint64Val{Uint64Val: p.Pair.Count}},
			},
		})
	} else {
		return callback(&pb.RowResponse{
			Headers: []*pb.ColumnInfo{
				{Name: p.Field, Datatype: "uint64"},
				{Name: "count", Datatype: "uint64"},
			},
			Columns: []*pb.ColumnResponse{
				{ColumnVal: &pb.ColumnResponse_Uint64Val{Uint64Val: p.Pair.ID}},
				{ColumnVal: &pb.ColumnResponse_Uint64Val{Uint64Val: p.Pair.Count}},
			},
		})
	}
}

// MarshalJSON marshals PairField into a JSON-encoded byte slice,
// excluding `Field`.
func (p PairField) MarshalJSON() ([]byte, error) {
	return json.Marshal(p.Pair)
}

// Pairs is a sortable slice of Pair objects.
type Pairs []Pair

func (p Pairs) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p Pairs) Len() int           { return len(p) }
func (p Pairs) Less(i, j int) bool { return p[i].Count > p[j].Count }

// pairHeap is a heap implementation over a group of Pairs.
type pairHeap struct {
	Pairs
}

// Less implemets the Sort interface.
// reports whether the element with index i should sort before the element with index j.
func (p pairHeap) Less(i, j int) bool { return p.Pairs[i].Count < p.Pairs[j].Count }

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

// PairsField is a Pairs object with its associated field.
type PairsField struct {
	Pairs []Pair
	Field string
}

func (p *PairsField) Clone() (r *PairsField) {
	r = &PairsField{
		Pairs: make([]Pair, len(p.Pairs)),
		Field: p.Field,
	}
	copy(r.Pairs, p.Pairs)
	return
}

// ToTable implements the ToTabler interface.
func (p *PairsField) ToTable() (*pb.TableResponse, error) {
	return pb.RowsToTable(p, len(p.Pairs))
}

// ToRows implements the ToRowser interface.
func (p *PairsField) ToRows(callback func(*pb.RowResponse) error) error {
	// Determine if the ID has string keys.
	var stringKeys bool
	if len(p.Pairs) > 0 {
		if p.Pairs[0].Key != "" {
			stringKeys = true
		}
	}

	dtype := "uint64"
	if stringKeys {
		dtype = "string"
	}
	ci := []*pb.ColumnInfo{
		{Name: p.Field, Datatype: dtype},
		{Name: "count", Datatype: "uint64"},
	}
	for _, pair := range p.Pairs {
		if stringKeys {
			if err := callback(&pb.RowResponse{
				Headers: ci,
				Columns: []*pb.ColumnResponse{
					{ColumnVal: &pb.ColumnResponse_StringVal{StringVal: pair.Key}},
					{ColumnVal: &pb.ColumnResponse_Uint64Val{Uint64Val: uint64(pair.Count)}},
				}}); err != nil {
				return errors.Wrap(err, "calling callback")
			}
		} else {
			if err := callback(&pb.RowResponse{
				Headers: ci,
				Columns: []*pb.ColumnResponse{
					{ColumnVal: &pb.ColumnResponse_Uint64Val{Uint64Val: uint64(pair.ID)}},
					{ColumnVal: &pb.ColumnResponse_Uint64Val{Uint64Val: uint64(pair.Count)}},
				}}); err != nil {
				return errors.Wrap(err, "calling callback")
			}
		}
		ci = nil //only send on the first
	}
	return nil
}

// MarshalJSON marshals PairsField into a JSON-encoded byte slice,
// excluding `Field`.
func (p PairsField) MarshalJSON() ([]byte, error) {
	return json.Marshal(p.Pairs)
}

// int64Slice represents a sortable slice of int64 numbers.
type int64Slice []int64

func (p int64Slice) Len() int           { return len(p) }
func (p int64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p int64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// uint64Slice represents a sortable slice of uint64 numbers.
type uint64Slice []uint64

func (p uint64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p uint64Slice) Len() int           { return len(p) }
func (p uint64Slice) Less(i, j int) bool { return p[i] < p[j] }

// simpleCache implements a bitmap Rowcache.
// it is meant to be a short-lived cache for cases where writes are continuing to access
// the same row within a short time frame (i.e. good for write-heavy loads)
// A read-heavy use case would cause the cache to get bigger, potentially causing the
// node to run out of memory.
type simpleCache struct {
	cache map[uint64]*Row
}

// Fetch retrieves the bitmap at the id in the cache.
func (s *simpleCache) Fetch(id uint64) (*Row, bool) {
	m, ok := s.cache[id]
	return m, ok
}

func newSimpleCache() *simpleCache {
	return &simpleCache{
		cache: make(map[uint64]*Row),
	}
}

// Add adds the bitmap to the cache, keyed on the id. A nil row means
// deleting the row from the cache.
func (s *simpleCache) Add(id uint64, b *Row) {
	if b != nil {
		s.cache[id] = b
	} else {
		delete(s.cache, id)
	}
}

// nopCache represents a no-op Cache implementation.
type nopCache struct {
	stats stats.StatsClient
}

// Ensure NopCache implements Cache.
var globalNopCache cache = nopCache{
	stats: stats.NopStatsClient,
}

func (c nopCache) Add(uint64, uint64)     {}
func (c nopCache) BulkAdd(uint64, uint64) {}
func (c nopCache) Get(uint64) uint64      { return 0 }
func (c nopCache) IDs() []uint64          { return []uint64{} }

func (c nopCache) Invalidate()                {}
func (c nopCache) Len() int                   { return 0 }
func (c nopCache) Recalculate()               {}
func (c nopCache) SetStats(stats.StatsClient) {}

func (c nopCache) Top() []bitmapPair {
	return []bitmapPair{}
}
