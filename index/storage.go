package index

type Storage interface {
	Fetch(bitmap_id uint64, db string, frame string, slice int) (IBitmap, uint64)
	Store(id int64, db string, frame string, slice int, filter uint64, bitmap *Bitmap) error
	StoreBlock(id int64, db string, frame string, slice int, filter uint64, chunk int64, block_index int32, block int64) error
	BeginBatch()
	EndBatch()
	FlushBatch()
	Close()
}
