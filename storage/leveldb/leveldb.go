package leveldb

import (
	"bytes"
	"encoding/binary"
	"path/filepath"
	"strconv"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	. "github.com/syndtr/goleveldb/leveldb/util"
	"github.com/umbel/pilosa"
	"github.com/umbel/pilosa/statsd"
)

func init() {
	pilosa.RegisterStorage("leveldb",
		func(opt pilosa.StorageOptions) pilosa.Storage {
			return NewStorage(opt)
		},
	)
}

//go get github.com/syndtr/goleveldb/leveldb

// Storage represents a LevelDB-backed storage engine.
type Storage struct {
	path string
	db   *leveldb.DB

	batch     *leveldb.Batch
	batchTime time.Time
	batchN    int
}

// NewStorage returns a new instance of Storage.
func NewStorage(opt pilosa.StorageOptions) *Storage {
	path := filepath.Join(
		opt.LevelDBPath,
		opt.DB,
		strconv.Itoa(opt.Slice),
		opt.Frame,
		pilosa.SUUID_to_Hex(opt.FragmentID),
	)

	return &Storage{path: path}
}

// Path returns the path the storage was initialized with.
func (s *Storage) Path() string { return s.path }

// Open opens and initializes the storage.
func (s *Storage) Open() error {
	db, err := leveldb.OpenFile(s.path, nil)
	if err != nil {
		return err
	}
	s.db = db
	s.batchTime = time.Now().Add(-time.Hour)

	return nil
}

// Close flushes and closes the storage.
func (s *Storage) Close() error {
	s.Flush()
	return s.db.Close()
}

func (s *Storage) Fetch(bitmapID uint64, db string, frame string, slice int) (*pilosa.Bitmap, uint64) {
	bm := pilosa.NewBitmap()

	// Begin benchmark.
	start := time.Now()

	// Create an iterator on the database.
	iter := s.db.NewIterator(&Range{
		Start: marshalKey(bitmapID, 0, 0),
		Limit: marshalKey(bitmapID+1, 0, 0),
	}, nil)
	defer iter.Release()

	// Iterate over blocks in database and create bitmap.
	var chunk *pilosa.Chunk
	var filter, block, count uint64
	lastKey := uint64(pilosa.CounterMask)

	for iter.Next() {
		_, key, idx := unmarshalKey(iter.Key())
		block, filter = unmarshalValue(iter.Value())
		if key != pilosa.CounterMask {
			if key != lastKey {
				chunk = &pilosa.Chunk{key, pilosa.NewBlocks()}
				bm.AddChunk(chunk)
			}
			chunk.Value[idx] = block
		} else {
			count = block
		}

		lastKey = key
	}

	statsd.SendTimer("leveldb_storage_Fetch", time.Since(start).Nanoseconds())

	// Set bit count on bitmap.
	bm.SetCount(uint64(count))

	return bm, uint64(filter)
}

// beginBatch starts a new batch if one is not already started.
func (s *Storage) beginBatch() {
	if s.batch == nil {
		s.batch = &leveldb.Batch{}
	}
	s.batchN++
}

func (s *Storage) endBatch() {
	if s.batch == nil {
		return
	}

	start := time.Now()
	if time.Since(s.batchTime) > 15*time.Second || s.batchN > 300 {
		s.Flush()
	}
	statsd.SendTimer("leveldb_storage_EndBatch", time.Since(start).Nanoseconds())
}

func (s *Storage) Flush() {
	start := time.Now()

	// Flush the batch if one exists.
	if s.batch != nil {
		s.db.Write(s.batch, nil)
	}

	// Clear batch and reset timer and count.
	s.batch = nil
	s.batchTime = time.Now()
	s.batchN = 0

	statsd.SendTimer("leveldb_storage_FlushBatch", time.Since(start).Nanoseconds())
}

// Store saves a bitmap to storage.
func (s *Storage) Store(bitmapID uint64, db string, frame string, slice int, filter uint64, bm *pilosa.Bitmap) error {
	s.beginBatch()

	for itr := bm.ChunkIterator(); !itr.Limit(); itr = itr.Next() {
		for idx, block := range itr.Item().Value {
			if block != 0 {
				s.StoreBlock(bitmapID, db, frame, slice, filter, itr.Item().Key, int32(idx), block)
			}
		}
	}

	s.StoreBlock(bitmapID, db, frame, slice, filter, pilosa.CounterMask, 0, bm.BitCount())

	s.endBatch()
	return nil
}

// StoreBlock saves a block to storage.
func (s *Storage) StoreBlock(bitmapID uint64, db string, frame string, slice int, filter uint64, chunk uint64, index int32, block uint64) error {
	start := time.Now()
	s.batch.Put(marshalKey(bitmapID, chunk, uint8(index)), marshalValue(block, filter))
	statsd.SendTimer("leveldb_storage_StoreBlock", time.Since(start).Nanoseconds())
	return nil
}

// RemoveBlock deletes a block from storage. This is a no-op in the LevelDB store.
func (s *Storage) RemoveBlock(bitmapID uint64, db string, frame string, slice int, chunk uint64, blockIndex int32) {
}

// StoreBit sets a bit in storage.
func (s *Storage) StoreBit(bitmapID uint64, db string, frame string, slice int, filter uint64, bchunk uint64, blockIndex int32, bblock, count uint64) {
	s.beginBatch()
	s.StoreBlock(bitmapID, db, frame, slice, filter, bchunk, blockIndex, bblock)
	s.StoreBlock(bitmapID, db, frame, slice, filter, pilosa.CounterMask, 0, count)
	s.endBatch()
}

// RemoveBlock unsets a bit in storage. This is a no-op in the LevelDB store.
func (s *Storage) RemoveBit(bitmapID uint64, db string, frame string, slice int, filter uint64, bchunk uint64, blockIndex int32, count uint64) {
}

// marshalKey encodes the id, chunk key, & block index to a byte slice.
func marshalKey(id, key uint64, index uint8) []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, id)
	binary.Write(buf, binary.LittleEndian, key)
	binary.Write(buf, binary.LittleEndian, index)
	return buf.Bytes()
}

// marshalValue encodes the block number and filter to a byte slice.
func marshalValue(block, filter uint64) []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, block)
	binary.Write(buf, binary.LittleEndian, filter)
	return buf.Bytes()
}

// unmarshalKey decodes the id, chunk key, & block index from a byte slice.
func unmarshalKey(v []byte) (id, key uint64, index uint8) {
	buf := bytes.NewReader(v)
	binary.Read(buf, binary.LittleEndian, &id)
	binary.Read(buf, binary.LittleEndian, &key)
	binary.Read(buf, binary.LittleEndian, &index)
	return
}

// unmarshalValue decodes the block number and filter from a byte slice.
func unmarshalValue(v []byte) (block, filter uint64) {
	buf := bytes.NewReader(v)
	binary.Read(buf, binary.LittleEndian, &block)
	binary.Read(buf, binary.LittleEndian, &filter)
	return
}
