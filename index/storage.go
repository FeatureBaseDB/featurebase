package index

type Storage interface {
	Fetch(bitmap_id uint64, db string, frame string, slice int) (IBitmap, uint64)
	Store(id uint64, db string, frame string, slice int, filter uint64, bitmap *Bitmap) error
	StoreBlock(id uint64, db string, frame string, slice int, filter uint64, chunk uint64, block_index int32, block uint64) error
	StoreBit(bid uint64, db string, frame string, slice int, filter uint64, bchunk uint64, block_index int32, bblock, count uint64)
	BeginBatch()
	EndBatch()
	FlushBatch()
	Close()
}
