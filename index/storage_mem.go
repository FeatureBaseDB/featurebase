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

func (c *MemoryStorage) BeginBatch() {
}
func (c *MemoryStorage) Close() {
}
func (c *MemoryStorage) EndBatch() {
}
func (c *MemoryStorage) FlushBatch() {
}
func (c *MemoryStorage) Fetch(bitmap_id uint64, db string, frame string, slice int) (IBitmap, uint64) {
	//	log.Println("hello")

	key := fmt.Sprintf("%d:%s:%s:%d", bitmap_id, db, frame, slice)
	bitmap, found := c.db[key]
	if !found {
		bitmap = CreateRBBitmap().(*Bitmap)
		c.db[key] = bitmap
	}
	return bitmap, 0
}

func (c *MemoryStorage) Store(bitmap_id uint64, db string, frame string, slice int, filter uint64, bitmap *Bitmap) error {
	//only use the cache and throw away everything
	return nil
}

func (c *MemoryStorage) StoreBlock(bitmap_id uint64, db string, frame string, slice int, filter uint64, chunk_key uint64, block_index int32, block uint64) error {
	//only use the cache and throw away everything

	return nil
}
