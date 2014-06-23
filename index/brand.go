package index

import (
	"time"

	_ "github.com/go-sql-driver/mysql"
)

import (
	"encoding/json"
	"fmt"
	"log"
	"pilosa/config"
	"pilosa/util"
	"sort"
)

type Pair struct {
	Key, Count uint64
}
type Rank struct {
	*Pair
	bitmap   IBitmap
	category uint64
}

type RankList []*Rank

func (p RankList) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p RankList) Len() int           { return len(p) }
func (p RankList) Less(i, j int) bool { return p[i].Count > p[j].Count }

type Brand struct {
	bitmap_cache     map[uint64]*Rank
	db               string
	frame            string
	slice            int
	storage          Storage
	rankings         RankList
	rank_count       int
	threshold_value  uint64
	threshold_length int
	threshold_idx    int
	skip             int
	rank_time        time.Time
}

func NewBrand(db string, frame string, slice int, s Storage, threshold_len int, threshold int, skipp int) *Brand {
	f := new(Brand)
	f.storage = s
	f.frame = frame
	f.slice = slice
	f.db = db
	f.rank_count = 0
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
	b, filter := self.storage.Fetch(bitmap_id, self.db, self.frame, self.slice)
	self.cache_it(b, bitmap_id, filter)
	return b
}

func (self *Brand) GetFilter(bitmap_id, filter uint64) IBitmap {
	b, _ := self.storage.Fetch(bitmap_id, self.db, self.frame, self.slice)
	self.cache_it(b, bitmap_id, filter)
	return b
}

func (self *Brand) cache_it(bm IBitmap, bitmap_id uint64, category uint64) {
	if bm.Count() >= self.threshold_value {
		self.bitmap_cache[bitmap_id] = &Rank{&Pair{bitmap_id, bm.Count()}, bm, category}
		if len(self.bitmap_cache) > self.threshold_length {
			log.Printf("RANK: %d %d %d", len(self.bitmap_cache), self.threshold_length, self.threshold_value)
			self.Rank()
			self.trim()
		}
	}
}
func (self *Brand) trim() {
	for k, item := range self.bitmap_cache {
		if item.bitmap.Count() <= self.threshold_value {
			delete(self.bitmap_cache, k)
		}
	}
	log.Printf("TRIM: %d %d", len(self.bitmap_cache), self.threshold_length)

}

func (self *Brand) SetBit(bitmap_id uint64, bit_pos uint64, filter uint64) bool {
	bm1, ok := self.bitmap_cache[bitmap_id]
	var bm IBitmap
	if ok {
		bm = bm1.bitmap
	} else {
		bm = self.GetFilter(bitmap_id, filter) //aways overwrites what is in cass filter type
	}
	change, chunk, address := SetBit(bm, bit_pos)
	if change {
		val := chunk.Value.Block[address.BlockIndex]
		self.storage.BeginBatch()
		self.storage.StoreBlock(int64(bitmap_id), self.db, self.frame, self.slice, filter, int64(address.ChunkKey), int32(address.BlockIndex), int64(val))
		self.storage.StoreBlock(int64(bitmap_id), self.db, self.frame, self.slice, filter, COUNTER_KEY, 0, int64(bm.Count()))
		self.storage.EndBatch()
		self.rank_count++
	}
	return change
}

func (self *Brand) Rank() {
	start := time.Now()
	var list RankList
	for k, item := range self.bitmap_cache {
		//		if item.bitmap.Count() > 50 {
		list = append(list, &Rank{&Pair{k, item.bitmap.Count()}, item.bitmap, item.category})
		//		}
	}
	sort.Sort(list)
	self.rankings = list
	if len(list) > self.threshold_idx {
		item := list[self.threshold_idx]
		self.threshold_value = item.bitmap.Count()
	} else {
		self.threshold_value = 1
	}

	//dump(self.rankings, 10)
	self.rank_count = 0
	delta := time.Since(start)
	util.SendTimer("brand_Rank", delta.Nanoseconds())
	self.rank_time = start

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
		"rank counter":                       self.rank_count,
		"threshold_value":                    self.threshold_value,
		"threshold_length":                   self.threshold_length,
		"threshold_idx":                      self.threshold_idx,
		"skip":                               self.skip}
	return stats
}
func (self *Brand) Store(bitmap_id uint64, bm IBitmap, filter uint64) {
	self.storage.Store(int64(bitmap_id), self.db, self.frame, self.slice, filter, bm.(*Bitmap))
	self.cache_it(bm, bitmap_id, filter)
}

func (self *Brand) TopN(src_bitmap IBitmap, n int, categories []uint64) []Pair {
	if len(self.rankings) < 50 {
		self.Rank()
	} else if self.rank_count > 0 {
		last := time.Since(self.rank_time) * time.Second
		if last > 60 {
			self.Rank()
		}
	}

	is := NewIntSet()
	for _, v := range categories {
		is.Add(v)

	}

	test := self.TopNCat(src_bitmap, n, is)
	return test
}
func dump(r RankList, n int) {
	for i, v := range r {
		log.Println(i, v)
		if i > n {
			return
		}
	}
}
func (self *Brand) TopNCat(src_bitmap IBitmap, n int, category *IntSet) []Pair {
	breakout := 500
	var (
		o       *Rank
		results RankList
	)
	counter := 0
	x := 0

	needCat := category.Size() > 0
	for i, pair := range self.rankings {

		if needCat {
			if !category.Contains(pair.category) {
				continue
			}
		}

		if counter > n {
			break
		}
		bm := Intersection(src_bitmap, pair.bitmap)
		bc := BitCount(bm)
		if bc > 0 {
			results = append(results, &Rank{&Pair{pair.Key, bc}, bm, pair.category})
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

	for i := x + 1; i < len(self.rankings); i++ {
		counter = counter + 1
		o = self.rankings[i]

		if needCat {
			if !category.Contains(o.category) {
				continue
			}
		}

		if i > breakout {
			break
		}

		//if o.Count < current_threshold { //done
		//need something to do with the size of initianl bitmap
		if o.Count < current_threshold { //done
			break

		}

		abitmap := Intersection(src_bitmap, o.bitmap)
		bc := BitCount(abitmap)

		if bc > current_threshold {
			if results[end-1].Count > bc {
				results[end] = &Rank{&Pair{o.Key, bc}, abitmap, o.category}
				current_threshold = bc
			} else {
				results[end+1] = &Rank{&Pair{o.Key, bc}, abitmap, o.category}
				sort.Sort(results)
				o = results[end]
				current_threshold = o.Count
			}
		}
	}
	return packagePairs(results[:end])
}
func (self *Brand) getFileName() string {
	base := config.GetString("fragment_base")
	if base == "" {
		base = "."
	}

	return fmt.Sprintf("%s/%s.%s.%d.json", base, self.db, self.frame, self.slice)
}

func (self *Brand) Persist() error {
	log.Println("Brand Persist:", self.getFileName())
	self.storage.FlushBatch()
	asize := len(self.bitmap_cache)

	if asize == 0 {
		log.Println("Nothing to save ", self.getFileName())
		return nil
	}
	w, err := util.Create(self.getFileName())
	if err != nil {
		log.Println("Error opening outfile ", self.getFileName())
		log.Println(err)
		return err
	}
	defer w.Close()
	defer self.storage.Close()

	var list RankList
	for k, item := range self.bitmap_cache {
		list = append(list, &Rank{&Pair{k, item.bitmap.Count()}, item.bitmap, item.category})
	}

	sort.Sort(list)

	results := make([]uint64, asize)
	i := 0
	for _, k := range list { //   map[uint64]*Rank
		results[i] = k.Key
		i += 1
	}

	encoder := json.NewEncoder(w)
	return encoder.Encode(results)
}

func (self *Brand) Load(requestChan chan Command, f *Fragment) {
	log.Println("Brand Load")
	r, err := util.Open(self.getFileName())
	if err != nil {
		log.Println("NO Brand Init File:", self.getFileName())
		return
	}
	dec := json.NewDecoder(r)
	var keys []uint64
	if err := dec.Decode(&keys); err != nil {
		return
		//log.Println("Bad mojo")
	}
	for _, k := range keys {
		request := NewLoadRequest(k)
		requestChan <- request
		request.Response()

	}
}
