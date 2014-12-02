package index

type Storage interface {
	Fetch(bitmap_id uint64, db string, frame string, slice int) (IBitmap, uint64)
	Store(id uint64, db string, frame string, slice int, filter uint64, bitmap *Bitmap) error
	StoreBlock(id uint64, db string, frame string, slice int, filter uint64, chunk uint64, block_index int32, block uint64) error
	BeginBatch()
	EndBatch()
	FlushBatch()
	Close()
}
