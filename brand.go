package pilosa

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"

	log "github.com/cihub/seelog"
	"github.com/umbel/pilosa/statsd"
)

var FragmentBase string

var globalLock *sync.Mutex

func init() {
	globalLock = new(sync.Mutex)
}

type Pair struct {
	Key, Count uint64
}
type Rank struct {
	*Pair
	bitmap   *Bitmap
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

func (b *Brand) Clear() bool {
	b.bitmap_cache = make(map[uint64]*Rank)
	return true
}

func (b *Brand) Exists(bitmap_id uint64) bool {
	_, ok := b.bitmap_cache[bitmap_id]
	return ok
}

func (b *Brand) Get(bitmap_id uint64) *Bitmap {
	if bm, ok := b.bitmap_cache[bitmap_id]; ok {
		return bm.bitmap
	}

	// I should fetch the category here..need to come up with a good source
	bm, filter := b.storage.Fetch(bitmap_id, b.db, b.frame, b.slice)
	b.cache_it(bm, bitmap_id, filter)
	return bm
}

func (b *Brand) Get_nocache(bitmap_id uint64) (*Bitmap, uint64) {
	if bm, ok := b.bitmap_cache[bitmap_id]; ok {
		return bm.bitmap, bm.category
	}

	//I should fetch the category here..need to come up with a good source
	return b.storage.Fetch(bitmap_id, b.db, b.frame, b.slice)
}

func (b *Brand) GetFilter(bitmap_id, filter uint64) *Bitmap {
	bm, old_filter := b.storage.Fetch(bitmap_id, b.db, b.frame, b.slice)
	if filter == 0 {
		filter = old_filter
	}
	b.cache_it(bm, bitmap_id, filter)
	return bm
}

func (b *Brand) cache_it(bm *Bitmap, bitmap_id uint64, category uint64) {
	if bm.Count() >= b.threshold_value {
		b.bitmap_cache[bitmap_id] = &Rank{&Pair{bitmap_id, bm.Count()}, bm, category}
		if len(b.bitmap_cache) > b.threshold_length {
			log.Info("RANK:", len(b.bitmap_cache), b.threshold_length, b.threshold_value)
			b.Rank()
			b.trim()
		}
	}
}

func (b *Brand) trim() {
	for k, item := range b.bitmap_cache {
		if item.bitmap.Count() <= b.threshold_value {
			delete(b.bitmap_cache, k)
		}
	}
	log.Info("TRIM:", len(b.bitmap_cache), b.threshold_length)
}

func (b *Brand) SetBit(bitmap_id uint64, bit_pos uint64, filter uint64) bool {
	bm1, ok := b.bitmap_cache[bitmap_id]
	var bm *Bitmap
	if ok {
		bm = bm1.bitmap
	} else {
		bm = b.GetFilter(bitmap_id, filter) //aways overwrites what is in cass filter type
	}
	change, chunk, address := bm.SetBit(bit_pos)
	if change {
		val := chunk.Value[address.BlockIndex]
		b.storage.StoreBit(bitmap_id, b.db, b.frame, b.slice, filter, address.ChunkKey, int32(address.BlockIndex), val, bm.Count())
		b.rank_count++
	}
	return change
}

func (b *Brand) Rank() {
	start := time.Now()
	var list RankList
	for k, item := range b.bitmap_cache {
		list = append(list, &Rank{&Pair{k, item.bitmap.Count()}, item.bitmap, item.category})
	}
	sort.Sort(list)
	b.rankings = list
	if len(list) > b.threshold_idx {
		item := list[b.threshold_idx]
		b.threshold_value = item.bitmap.Count()
	} else {
		b.threshold_value = 1
	}

	b.rank_count = 0
	delta := time.Since(start)
	statsd.SendTimer("brand_Rank", delta.Nanoseconds())
	b.rank_time = start
}

func packagePairs(r RankList) []Pair {
	res := make([]Pair, r.Len())
	for i, v := range r {
		res[i] = Pair{v.Key, v.Count}
	}
	return res
}

func (b *Brand) Stats() interface{} {
	total := uint64(0)
	i := uint64(0)
	bit_total := uint64(0)
	for _, v := range b.bitmap_cache {
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
		"number of bitmaps":                  len(b.bitmap_cache),
		"avg size of bitmap in space(bytes)": avg_bytes,
		"avg size of bitmap in bits":         avg_bits,
		"rank counter":                       b.rank_count,
		"threshold_value":                    b.threshold_value,
		"threshold_length":                   b.threshold_length,
		"threshold_idx":                      b.threshold_idx,
		"skip":                               b.skip}
	return stats
}
func (b *Brand) Store(bitmap_id uint64, bm *Bitmap, filter uint64) {
	b.storage.Store(bitmap_id, b.db, b.frame, b.slice, filter, bm)
	b.cache_it(bm, bitmap_id, filter)
}
func (b *Brand) checkRank() {
	if len(b.rankings) < 50 {
		b.Rank()
		return
	}

	if b.rank_count > 0 {
		last := time.Since(b.rank_time) * time.Second
		if last > 60*5 {
			b.Rank()
		}
	}
}
func (b *Brand) TopN(src_bitmap *Bitmap, n int, categories []uint64) []Pair {
	b.checkRank()
	set := make(map[uint64]struct{})
	for _, v := range categories {
		set[v] = struct{}{}
	}

	return b.TopNCat(src_bitmap, n, set)
}

func dump(r RankList, n int) {
	for i, v := range r {
		log.Info(i, v)
		if i > n {
			return
		}
	}
}

func (b *Brand) TopNAll(n int, categories []uint64) []Pair {
	log.Trace("TopNAll")

	b.checkRank()
	results := make([]Pair, 0, 0)

	set := make(map[uint64]struct{})
	needCat := false
	for _, v := range categories {
		set[v] = struct{}{}
		needCat = true
	}

	count := 0
	for _, pair := range b.rankings {
		if needCat {
			if _, ok := set[pair.category]; !ok {
				continue
			}
		}

		if count >= n {
			break
		}
		if pair.Count > 0 {
			results = append(results, Pair{pair.Key, pair.Count})
		}
		count++
	}
	return results
}

func (b *Brand) TopNCat(src_bitmap *Bitmap, n int, set map[uint64]struct{}) []Pair {
	breakout := 1000
	var (
		o       *Rank
		results RankList
	)
	counter := 0
	x := 0

	needCat := (len(set) > 0)
	for i, pair := range b.rankings {
		if needCat {
			if _, ok := set[pair.category]; !ok {
				continue
			}
		}

		if counter > n {
			break
		}
		bc := src_bitmap.IntersectionCount(pair.bitmap)
		if bc > 0 {
			results = append(results, &Rank{&Pair{pair.Key, bc}, nil, pair.category})
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

	for i := x + 1; i < len(b.rankings); i++ {
		o = b.rankings[i]

		if needCat {
			if _, ok := set[o.category]; !ok {
				continue
			}
			counter = counter + 1
		} else {
			counter = counter + 1
		}

		if counter > breakout {
			break
		}

		//if o.Count < current_threshold { //done
		//need something to do with the size of initianl bitmap
		if o.Count < current_threshold { //done
			break

		}

		bc := src_bitmap.IntersectionCount(o.bitmap)

		if bc > current_threshold {
			if results[end-1].Count > bc {
				results[end] = &Rank{&Pair{o.Key, bc}, nil, o.category}
				current_threshold = bc
			} else {
				results[end+1] = &Rank{&Pair{o.Key, bc}, nil, o.category}
				sort.Sort(results)
				o = results[end]
				current_threshold = o.Count
			}
		}
	}
	return packagePairs(results[:end])
}
func (b *Brand) getFileName() string {
	base := FragmentBase
	if base == "" {
		base = "."
	}

	return fmt.Sprintf("%s/%s.%s.%d.json", base, b.db, b.frame, b.slice)
}

func (b *Brand) Persist() error {
	log.Info("Brand Persist:", b.getFileName())
	b.storage.Flush()
	asize := len(b.bitmap_cache)

	if asize == 0 {
		log.Warn("Nothing to save ", b.getFileName())
		return nil
	}
	w, err := createFile(b.getFileName())
	if err != nil {
		log.Warn("Error opening outfile ", b.getFileName())
		log.Warn(err)
		return err
	}
	defer w.Close()
	defer b.storage.Close()

	var list RankList
	for k, item := range b.bitmap_cache {
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

func (b *Brand) Load(requestChan chan Command, f *Fragment) {
	log.Warn("Brand Load")
	time.Sleep(time.Duration(rand.Intn(32)) * time.Second) //trying to avoid mass cassandra hit
	r, err := openFile(b.getFileName())
	if err != nil {
		log.Warn("NO Brand Init File:", b.getFileName())
		return
	}
	dec := json.NewDecoder(r)
	var keys []uint64
	if err := dec.Decode(&keys); err != nil {
		return
	}
	globalLock.Lock()
	defer globalLock.Unlock()
	// probaly need to get a etcd lock too someday
	for _, k := range keys {
		request := NewLoadRequest(k)
		requestChan <- request
		request.Response()
		time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond) //trying to avoid mass cassandra hit

	}
}

func (b *Brand) ClearBit(bitmap_id uint64, bit_pos uint64) bool {
	log.Trace("ClearBit", bitmap_id, bit_pos)
	bm1, ok := b.bitmap_cache[bitmap_id]
	var bm *Bitmap
	filter := uint64(0)
	if ok {
		bm = bm1.bitmap
		filter = bm1.category
	} else {
		bm, filter = b.Get_nocache(bitmap_id)
		if bm.Count() == 0 {
			return false //nothing to unset
		}
	}

	changed, chunk, address := bm.ClearBit(bit_pos)
	if changed {
		val := chunk.Value[address.BlockIndex]
		if val == 0 {
			b.storage.RemoveBit(bitmap_id, b.db, b.frame, b.slice, filter, address.ChunkKey, int32(address.BlockIndex), bm.Count())
		} else {
			b.storage.StoreBit(bitmap_id, b.db, b.frame, b.slice, filter, address.ChunkKey, int32(address.BlockIndex), val, bm.Count())
		}
		b.rank_count++
	}

	return changed
}
