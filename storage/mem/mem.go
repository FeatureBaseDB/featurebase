package mem

import (
	"fmt"

	"github.com/umbel/pilosa"
)

func init() {
	pilosa.RegisterStorage("memory",
		func(opt pilosa.StorageOptions) pilosa.Storage {
			return NewStorage()
		},
	)
}

// Storage represents in-memory bitmap storage.
type Storage struct {
	db map[string]*pilosa.Bitmap
}

// NewStorage returns a new instance of Storage.
func NewStorage() *Storage {
	return &Storage{
		db: make(map[string]*pilosa.Bitmap),
	}
}

// Open initializes the storage.
func (c *Storage) Open() error { return nil }

// Close shuts down the storage.
func (c *Storage) Close() error { return nil }

// FlushBatch flushes the batch to disk. This is a no-op for in-memory storage.
func (c *Storage) Flush() {}

// Fetch retrieves a bitmap by id.
func (c *Storage) Fetch(id uint64, db, frame string, slice int) (*pilosa.Bitmap, uint64) {
	key := fmt.Sprintf("%d:%s:%s:%d", id, db, frame, slice)

	// Find bitmap by key.
	b, ok := c.db[key]
	if ok {
		return b, 0
	}

	// If the bitmap doesn't exist then create a new one.
	b = pilosa.NewBitmap()
	c.db[key] = b
	return b, 0
}

// Store saves a bitmap to storage.
// This is a no-op for in-memory storage because changes are stored in the cache.
func (c *Storage) Store(id uint64, db, frame string, slice int, filter uint64, b *pilosa.Bitmap) error {
	return nil
}

// StoreBlock saves a block to storage.
// This is a no-op for in-memory storage because changes are stored in the cache.
func (c *Storage) StoreBlock(id uint64, db, frame string, slice int, filter uint64, chunk_key uint64, block_index int32, block uint64) error {
	return nil
}

// RemoveBlock deletes a block from bitmap.
// This is a no-op for in-memory storage because changes are stored in the cache.
func (self *Storage) RemoveBlock(id uint64, db string, frame string, slice int, chunk uint64, block_index int32) {
}

// StoreBit sets a bit in a bitmap.
// This is a no-op for in-memory storage because changes are stored in the cache.
func (self *Storage) StoreBit(id uint64, db string, frame string, slice int, filter uint64, bchunk uint64, block_index int32, bblock, count uint64) {
}

// RemoveBit unsets a bit in a bitmap.
// This is a no-op for in-memory storage because changes are stored in the cache.
func (self *Storage) RemoveBit(id uint64, db string, frame string, slice int, filter uint64, chunk uint64, block_index int32, count uint64) {
}
