package index

type Storage interface {
	Fetch(bitmap_id uint64, shard int32) IBitmap
	Store(id int64, shard_key int32, bitmap *Bitmap) error
	StoreBlock(id int64, shard_key int32, chunk int64, block_index int32, block int64) error
}
