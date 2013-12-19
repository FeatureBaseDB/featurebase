package index

// #cgo  CFLAGS:-mpopcnt

import "fmt"

type MemoryStorage struct {
	db map[string]*Bitmap
}

func NewMemoryStorage() Storage {
	//	log.Println("Hello")
	obj := new(MemoryStorage)
	obj.db = make(map[string]*Bitmap)

	return obj
}

func (c *MemoryStorage) Fetch(bitmap_id uint64, db string, slice int) IBitmap {
	//	log.Println("hello")

	key := fmt.Sprintf("%d:%s:%d", bitmap_id, db, slice)
	bitmap, found := c.db[key]
	if !found {
		bitmap = CreateRBBitmap().(*Bitmap)
		c.db[key] = bitmap
	}
	return bitmap
}

func (c *MemoryStorage) Store(bitmap_id int64, db string, slice int, bitmap *Bitmap) error {
	//only use the cache and throw away everything
	return nil
}

func (c *MemoryStorage) StoreBlock(bitmap_id int64, db string, slice int, chunk_key int64, block_index int32, block int64) error {
	//only use the cache and throw away everything

	return nil
}
