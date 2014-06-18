package index

import (
	"encoding/json"
	"fmt"
	"log"
	"pilosa/config"
	"pilosa/util"

	"github.com/golang/groupcache/lru"
)

type General struct {
	bitmap_cache *lru.Cache
	keys         map[uint64]interface{}
	db           string
	frame        string
	slice        int
	storage      Storage
}

func NewGeneral(db string, frame string, slice int, s Storage) *General {
	f := new(General)
	f.storage = s
	f.frame = frame
	f.slice = slice
	f.db = db
	f.Clear()
	f.keys = make(map[uint64]interface{})
	//f.bitmap_cache = lru.New(10000)
	return f

}
func (self *General) Clear() bool {
	self.bitmap_cache = lru.New(10000)
	self.bitmap_cache.OnEvicted = self.OnEvicted
	return true
}

func (self *General) Get(bitmap_id uint64) IBitmap {
	bm, ok := self.bitmap_cache.Get(bitmap_id)
	if ok {
		return bm.(*Bitmap)
	}
	bm, _ = self.storage.Fetch(bitmap_id, self.db, self.frame, self.slice)
	self.bitmap_cache.Add(bitmap_id, bm)
	self.keys[bitmap_id] = 0
	return bm.(*Bitmap)
}
func (self *General) SetBit(bitmap_id uint64, bit_pos uint64, filter uint64) bool {
	bm := self.Get(bitmap_id)
	change, chunk, address := SetBit(bm, bit_pos)
	if change {
		val := chunk.Value.Block[address.BlockIndex]
		self.storage.BeginBatch()
		self.storage.StoreBlock(int64(bitmap_id), self.db, self.frame, self.slice, filter, int64(address.ChunkKey), int32(address.BlockIndex), int64(val))
		self.storage.StoreBlock(int64(bitmap_id), self.db, self.frame, self.slice, filter, COUNTER_KEY, 0, int64(bm.Count()))
		self.storage.EndBatch()

	}
	return change
}
func (self *General) TopN(b IBitmap, n int, categories []uint64) []Pair {
	var empty []Pair

	return empty
}
func (self *General) Store(bitmap_id uint64, bm IBitmap, filter uint64) {
	//oldbm:=self.Get(bitmap_id)
	//nbm = Union(oldbm, bm)
	self.storage.Store(int64(bitmap_id), self.db, self.frame, self.slice, filter, bm.(*Bitmap))
	self.bitmap_cache.Add(bitmap_id, bm)
	self.keys[bitmap_id] = 0
}
func (self *General) OnEvicted(key lru.Key, value interface{}) {
	delete(self.keys, key.(uint64))
}

func (self *General) Stats() interface{} {

	stats := map[string]interface{}{
		"total size of cache in items": self.bitmap_cache.Len()}
	return stats
}

func (self *General) getFileName() string {
	base := config.GetString("fragment_base")
	if base == "" {
		base = "."
	}
	return fmt.Sprintf("%s/%s.%s.%d", base, self.db, self.frame, self.slice)
}

func (self *General) Persist() error {
	log.Println("General Persist")
	w, err := util.Create(self.getFileName())
	if err != nil {
		log.Println("Error saving:", err)
		return err
	}
	defer w.Close()
	defer self.storage.Close()

	results := make([]uint64, len(self.keys))
	i := 0
	for k, _ := range self.keys { //   map[uint64]*Rank
		results[i] = k
		i += 1
	}

	encoder := json.NewEncoder(w)
	return encoder.Encode(results)
}

func (self *General) Load(requestChan chan Command, f *Fragment) {
	log.Println("General Load")
	r, err := util.Open(self.getFileName())
	if err != nil {
		log.Println("NO General Init File:", self.getFileName())
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
