package index

import "github.com/golang/groupcache/lru"

type General struct {
	bitmap_cache *lru.Cache
	db           string
	slice        int
	storage      Storage
}

func NewGeneral(db string, slice int, s Storage) *General {
	f := new(General)
	f.storage = s
	f.slice = slice
	f.db = db
	f.Clear()
	//f.bitmap_cache = lru.New(10000)
	return f

}
func (self *General) Clear() bool {
	self.bitmap_cache = lru.New(10000)
	return true
}

func (self *General) Get(bitmap_id uint64) IBitmap {
	bm, ok := self.bitmap_cache.Get(bitmap_id)
	if ok {
		return bm.(*Bitmap)
	}
	bm = self.storage.Fetch(bitmap_id, self.db, self.slice)
	self.bitmap_cache.Add(bitmap_id, bm)
	return bm.(*Bitmap)
}
func (self *General) SetBit(bitmap_id uint64, bit_pos uint64) bool {
	bm := self.Get(bitmap_id)
	change, chunk, address := SetBit(bm, bit_pos)
	if change {
		val := chunk.Value.Block[address.BlockIndex]
		self.storage.StoreBlock(int64(bitmap_id), self.db, self.slice, int64(address.ChunkKey), int32(address.BlockIndex), int64(val))
		self.storage.StoreBlock(int64(bitmap_id), self.db, self.slice, COUNTER_KEY, 0, int64(bm.Count()))

	}
	return change
}
func (self *General) TopN(b IBitmap, n int) []Pair {

	return nil
}
func (self *General) Store(bitmap_id uint64, bm IBitmap) {
	//oldbm:=self.Get(bitmap_id)
	//nbm = Union(oldbm, bm)
	self.storage.Store(int64(bitmap_id), self.db, self.slice, bm.(*Bitmap))
	self.bitmap_cache.Add(bitmap_id, bm)
}
