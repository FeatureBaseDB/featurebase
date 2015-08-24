package index

type Storage interface {
	Fetch(bitmap_id uint64, db string, frame string, slice int) (*Bitmap, uint64)
	Store(id uint64, db string, frame string, slice int, filter uint64, bitmap *Bitmap) error
	StoreBlock(id uint64, db string, frame string, slice int, filter uint64, chunk uint64, block_index int32, block uint64) error
	StoreBit(bid uint64, db string, frame string, slice int, filter uint64, chunk uint64, block_index int32, block, count uint64)
	RemoveBit(id uint64, db string, frame string, slice int, filter uint64, chunk uint64, block_index int32, count uint64)
	RemoveBlock(id uint64, db string, frame string, slice int, chunk uint64, block_index int32)
	BeginBatch()
	EndBatch()
	FlushBatch()
	Close()
}
