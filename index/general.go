package index

import (
	"github.com/golang/groupcache/lru"
)

type General struct {
	bitmap_cache *lru.Cache
	db    string
    slice int
    storage      Storage
}

func NewGeneral(db string, slice int, s Storage) *General {
	f := new(General)
	f.bitmap_cache = lru.New(10000)
	f.storage = s
    f.slice = slice
    f.db = db
	return f

}

func (f *General) Union(bitmaps[]uint64) IBitmap {
    result := NewBitmap()
    for i,id:= range bitmaps{
        bm:=f.Get(id)
        if i == 0{
            result = bm
        } else {
            result = Union(result,bm)
        }
    }
    return result 
}

func (f *General) Intersect(bitmaps[]uint64) IBitmap{
    result := NewBitmap()
    for i,id:= range bitmaps{
        bm:=f.Get(id)
        if i == 0{
            result = bm
        } else {
            result = Intersection(result,bm)
        }
    }
    return result 
}

func (f *General) Get(bitmap_id uint64) IBitmap {
	bm, ok := f.bitmap_cache.Get(bitmap_id)
	if ok {
		return bm.(*Bitmap)
	}
	bm = f.storage.Fetch(bitmap_id, f.db,f.slice)
	f.bitmap_cache.Add(bitmap_id, bm)
	return bm.(*Bitmap)
}

