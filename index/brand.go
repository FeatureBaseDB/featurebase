package index

import "sort"

type Pair struct {
	Key, Count uint64
}
type Rank struct {
	*Pair
	bitmap IBitmap
}

type RankList []*Rank

func (p RankList) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p RankList) Len() int           { return len(p) }
func (p RankList) Less(i, j int) bool { return p[i].Count > p[j].Count }

type Brand struct {
	bitmap_cache     map[uint64]*Rank
	db               string
	slice            int
	storage          Storage
	rankings         RankList
	rank_counter     int
	threshold_value  uint64
	threshold_length int
	threshold_idx    int
	skip             int
}

func NewBrand(db string, slice int, s Storage, threshold_len int, threshold int, skipp int) *Brand {
	f := new(Brand)
	f.storage = s
	f.slice = slice
	f.db = db
	f.rank_counter = 0
	f.threshold_value = 0
	f.threshold_length = threshold_len
	f.threshold_idx = threshold
	f.skip = skipp
	f.Clear() //alloc the cache
	return f

}
func (self *Brand) Clear() bool {
	self.bitmap_cache = make(map[uint64]*Rank)
	return true
}
func (self *Brand) Get(bitmap_id uint64) IBitmap {
	bm, ok := self.bitmap_cache[bitmap_id]
	if ok {
		return bm.bitmap
	}
	//I should fetch the category here..need to come up with a good source
	b := self.storage.Fetch(bitmap_id, self.db, self.slice)

	//Need to figure out wether to cache or not...

	self.cache_it(b, bitmap_id)

	return b
}

func (self *Brand) cache_it(bm IBitmap, bitmap_id uint64) {
	if bm.Count() >= self.threshold_value {
		self.bitmap_cache[bitmap_id] = &Rank{&Pair{bitmap_id, bm.Count()}, bm}
		if len(self.bitmap_cache) > self.threshold_length {
			self.trim()
		}
	}
}
func (self *Brand) trim() {
	for k, item := range self.bitmap_cache {
		if item.bitmap.Count() < self.threshold_value {
			delete(self.bitmap_cache, k)
		}
	}

}

func (self *Brand) SetBit(bitmap_id uint64, bit_pos uint64) bool {
	bm := self.Get(bitmap_id)
	change, chunk, address := SetBit(bm, bit_pos)
	if change {
		val := chunk.Value.Block[address.BlockIndex]
		self.storage.StoreBlock(int64(bitmap_id), self.db, self.slice, int64(address.ChunkKey), int32(address.BlockIndex), int64(val))
		self.storage.StoreBlock(int64(bitmap_id), self.db, self.slice, COUNTER_KEY, 0, int64(bm.Count()))
		if bm.Count() >= self.threshold_value {

			self.Rank() //need to optimize this
		}
	}
	return change
}

func (self *Brand) Rank() {
	if self.rank_counter <= 0 {
		self.rank_counter = self.skip
	} else {
		self.rank_counter -= 1
		return //skip
	}

	var list RankList
	for k, item := range self.bitmap_cache {
		if item.bitmap.Count() > 50 {
			list = append(list, &Rank{&Pair{k, item.bitmap.Count()}, item.bitmap})
		}
	}
	sort.Sort(list)
	self.rankings = list
	if len(list) > self.threshold_idx {
		item := list[self.threshold_idx]
		self.threshold_value = item.bitmap.Count()
	} else {
		self.threshold_value = 0
	}
}

func packagePairs(r RankList) []Pair {
	res := make([]Pair, r.Len())
	//for i := 0; i < r.Len(); i++ {
	for i, v := range r {
		res[i] = Pair{v.Key, v.Count}
	}
	return res
}

func (self *Brand) Stats() interface{} {
	total := uint64(0)
	i := uint64(0)
	bit_total := uint64(0)
	for _, v := range self.bitmap_cache {
		total += uint64(v.bitmap.Len()) * uint64(256)
		i += 1
		bit_total += v.Count
	}
	avg_bytes := uint64(0)
	avg_bits := uint64(0)
	if i > 0 {
		avg_bytes = total / i
		avg_bits = bit_total / i
	}

	stats := map[string]interface{}{
		"total size of cache in bytes":       total,
		"number of bitmaps":                  len(self.bitmap_cache),
		"avg size of bitmap in space(bytes)": avg_bytes,
		"avg size of bitmap in bits":         avg_bits,
		"rank counter":                       self.rank_counter,
		"threshold_value":                    self.threshold_value,
		"threshold_length":                   self.threshold_length,
		"threshold_idx":                      self.threshold_idx,
		"skip":                               self.skip}
	return stats
}
func (self *Brand) Store(bitmap_id uint64, bm IBitmap) {
	//oldbm:=self.Get(bitmap_id)
	//nbm = Union(oldbm, bm)
	self.storage.Store(int64(bitmap_id), self.db, self.slice, bm.(*Bitmap))
	self.cache_it(bm, bitmap_id)
}

func (self *Brand) TopN(src_bitmap IBitmap, n int) []Pair {
	breakout := 500
	var (
		o       *Rank
		results RankList
	)
	counter := 0
	x := 0

	//needCat := category.Size() > 0
	for i, pair := range self.rankings {
		/*
			        if needCat {
					    if !category.Contains(pair.category) {
					        continue
					    }
					}
		*/
		if counter > n {
			break
		}
		bm := Intersection(src_bitmap, pair.bitmap)
		bc := BitCount(bm)
		if bc > 0 {
			results = append(results, &Rank{&Pair{pair.Key, bc}, bm})
			counter = counter + 1
		}
		x = i
	}

	sort.Sort(results)
	if counter < n {
		return packagePairs(results)
	}
	end := len(results) - 1
	o = results[end]
	current_threshold := o.Count
	if current_threshold <= 10 {
		return packagePairs(results)
	}
	results = append(results, o)

	for i := x; i < len(self.rankings); i++ {
		counter = counter + 1
		o = self.rankings[i]
		/*
		   if needCat {
		       if !category.Contains(o.category) {
		           continue
		       }
		   } else
		*/
		if i > breakout {
			break
		}

		if o.Count < current_threshold { //done
			break

		}

		abitmap := Intersection(src_bitmap, o.bitmap)
		bc := BitCount(abitmap)

		if bc > current_threshold {
			if results[end].Count == current_threshold {
				results[end] = &Rank{&Pair{o.Key, bc}, abitmap}
			} else {
				results[end+1] = &Rank{&Pair{o.Key, bc}, abitmap}
				sort.Sort(results)
			}
			current_threshold = bc
		}
	}
	return packagePairs(results)
}
