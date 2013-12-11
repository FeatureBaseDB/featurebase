package index

type Storage interface {
	Fetch(bitmap_id uint64, db string, slice int) IBitmap
	Store(id int64, db string, slice int, bitmap *Bitmap) error
	StoreBlock(id int64, db string, slice int, chunk int64, block_index int32, block int64) error
}
