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
	bitmap_cache map[uint64]*Rank
	db           string
	slice        int
	storage      Storage
	rankings     RankList
}

func NewBrand(db string, slice int, s Storage) *Brand {
	f := new(Brand)
	f.bitmap_cache = make(map[uint64]*Rank)
	f.storage = s
	f.slice = slice
	f.db = db
	return f

}

func (self *Brand) Get(bitmap_id uint64) IBitmap {
	bm, ok := self.bitmap_cache[bitmap_id]
	if ok {
		return bm.bitmap
	}
	//I should fetch the category here..need to come up with a good source
	b := self.storage.Fetch(bitmap_id, self.db, self.slice)
	self.bitmap_cache[bitmap_id] = &Rank{&Pair{bitmap_id, b.Count()}, b}
	return b
}

func (self *Brand) SetBit(bitmap_id uint64, bit_pos uint64) bool {
	bm := self.Get(bitmap_id)
	change, chunk, address := SetBit(bm, bit_pos)
	if change {
		val := chunk.Value.Block[address.BlockIndex]
		self.storage.StoreBlock(int64(bitmap_id), self.db, self.slice, int64(address.ChunkKey), int32(address.BlockIndex), int64(val))
		self.storage.StoreBlock(int64(bitmap_id), self.db, self.slice, COUNTER_KEY, 0, int64(bm.Count()))

		self.Rank() //need to optimize this
	}
	return change
}

func (self *Brand) Rank() {
	var list RankList
	for k, item := range self.bitmap_cache {
		if item.bitmap.Count() > 50 {
			list = append(list, &Rank{&Pair{k, item.bitmap.Count()}, item.bitmap})
		}
	}
	sort.Sort(list)
	self.rankings = list
}

func packagePairs(r RankList) []Pair {
	res := make([]Pair, r.Len())
	//for i := 0; i < r.Len(); i++ {
	for i, v := range r {
		res[i] = Pair{v.Key, v.Count}
	}
	return res
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
