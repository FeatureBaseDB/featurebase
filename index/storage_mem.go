package index

// #cgo  CFLAGS:-mpopcnt

import (
	//	"log"
	"fmt"
)

type MemoryStorage struct {
	db map[string]*Bitmap
}

func NewMemoryStorage() Storage {
	obj := new(MemoryStorage)
	obj.db = make(map[string]*Bitmap)

	return obj
}

func (c *MemoryStorage) Fetch(bitmap_id uint64, db string, slice int) IBitmap {
	key := fmt.Sprintf("%d:%s:%d", bitmap_id, db, slice)
	bitmap, found := c.db[key]
	if !found {
		bitmap = CreateRBBitmap().(*Bitmap)
		c.db[key] = bitmap
	}
	return bitmap
}

func (c *MemoryStorage) Store(bitmap_id int64, db string, slice int, bitmap *Bitmap) error {
	key := fmt.Sprintf("%d:%s:%d", bitmap_id, db, slice)
	c.db[key] = bitmap
	return nil
}

func (c *MemoryStorage) StoreBlock(bitmap_id int64, db string, slice int, chunk_key int64, block_index int32, block int64) error {
	bm := c.Fetch(uint64(bitmap_id), db, slice)
	node := GetChunk(bm, uint64(chunk_key))
	if node == nil {
		node = &Chunk{uint64(chunk_key), BlockArray{}}
		bm.AddChunk(node)
	}
	node.Value.Block[block_index] = uint64(block)

	return nil
}
