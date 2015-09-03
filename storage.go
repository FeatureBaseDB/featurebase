package pilosa

// Storage represents
type Storage interface {
	Open() error
	Close() error
	Flush()

	Fetch(bitmap_id uint64, db, frame string, slice int) (*Bitmap, uint64)
	Store(id uint64, db, frame string, slice int, filter uint64, bitmap *Bitmap) error

	StoreBlock(id uint64, db, frame string, slice int, filter uint64, chunk uint64, block_index int32, block uint64) error
	RemoveBlock(id uint64, db, frame string, slice int, chunk uint64, block_index int32)

	StoreBit(bid uint64, db, frame string, slice int, filter uint64, chunk uint64, block_index int32, block, count uint64)
	RemoveBit(id uint64, db, frame string, slice int, filter uint64, chunk uint64, block_index int32, count uint64)
}

var storageFns = make(map[string]NewStorageFunc)

// NewStorageFunc represents a function that instantiates a new storage engine.
type NewStorageFunc func(opt StorageOptions) Storage

// RegisterStorage registers a storage engine.
func RegisterStorage(name string, fn NewStorageFunc) {
	if storageFns[name] != nil {
		panic("storage engine already registered: " + name)
	}
	storageFns[name] = fn
}

// NewStorage returns a new Storage instance by name.
func NewStorage(name string, opt StorageOptions) Storage {
	fn := storageFns[name]
	if fn == nil {
		panic("storage type not registered: " + name)
	}
	return fn(opt)
}

// StorageOptions represents the options passed to the storage engine.
type StorageOptions struct {
	DB         string
	Slice      int
	Frame      string
	FragmentID SUUID

	LevelDBPath string
}
