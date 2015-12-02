package pilosa

import (
	"sort"
	"strings"
	"sync"
	"time"
)

// SliceWidth is the number of profile IDs in a slice.
const SliceWidth = 65536

// Fragment represents the intersection of a frame and slice in a database.
type Fragment struct {
	mu sync.Mutex

	// Composite identifiers
	db    string
	frame string
	slice uint64

	// Bitmap cache.
	cache Cache
}

// NewFragment returns a new instance of Fragment.
func NewFragment(db, frame string, slice uint64) *Fragment {
	f := &Fragment{
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

// Bitmap returns a bitmap by ID.
func (f *Fragment) Bitmap(bitmapID uint64) *Bitmap {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.bitmap(bitmapID)
}

func (f *Fragment) bitmap(bitmapID uint64) *Bitmap {
	// Read from cache.
	if bm, ok := f.cache.Get(bitmapID); ok {
		return bm
	}

	// Read from storage engine.
	// bm, filter := f.storage.Fetch(bitmapID, f.db, f.frame, f.slice)

	bm := NewBitmap()
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
