package index

type Storage interface {
	Fetch(bitmap_id uint64, db string, frame string, slice int) (IBitmap, int)
	Store(id int64, db string, frame string, slice int, filter int, bitmap *Bitmap) error
	StoreBlock(id int64, db string, frame string, slice int, filter int, chunk int64, block_index int32, block int64) error
}
