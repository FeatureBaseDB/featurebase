package index

// #cgo  CFLAGS:-mpopcnt

import (
	"bytes"
	"encoding/binary"
	"log"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	. "github.com/syndtr/goleveldb/leveldb/util"
	"github.com/umbel/pilosa/util"
)

type LevelDBStorage struct {
	db            *leveldb.DB
	batch         *leveldb.Batch
	batch_time    time.Time
	batch_counter int
}

//go get github.com/syndtr/goleveldb/leveldb
func NewLevelDBStorage(file_path string) Storage {
	obj := new(LevelDBStorage)
	db, _ := leveldb.OpenFile(file_path, nil)
	obj.db = db
	obj.batch = nil
	obj.batch_counter = 0
	obj.batch_time = time.Now().Add(-time.Hour)
	return obj
}
func encodeKey(id, chunk_key uint64, block_index uint8) []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, id)
	binary.Write(buf, binary.LittleEndian, chunk_key)
	binary.Write(buf, binary.LittleEndian, block_index)
	return buf.Bytes()
}

func encodeValue(block, filter uint64) []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, block)
	binary.Write(buf, binary.LittleEndian, filter)
	return buf.Bytes()
}

func decodeKey(key []byte) (uint64, uint64, uint8) {
	var (
		id, chunk_key uint64
		block_index   uint8
	)
	buf := bytes.NewReader(key)
	binary.Read(buf, binary.LittleEndian, &id)
	binary.Read(buf, binary.LittleEndian, &chunk_key)
	binary.Read(buf, binary.LittleEndian, &block_index)
	return id, chunk_key, block_index
}

func decodeValue(value []byte) (uint64, uint64) {
	var (
		block, filter uint64
	)
	buf := bytes.NewReader(value)
	binary.Read(buf, binary.LittleEndian, &block)
	binary.Read(buf, binary.LittleEndian, &filter)
	return block, filter
}

func (self *LevelDBStorage) Fetch(bitmap_id uint64, db string, frame string, slice int) (IBitmap, uint64) {
	start := time.Now()
	var (
		chunk                   *Chunk
		filter, block, last_key uint64
	)

	bitmap := CreateRBBitmap()
	count := uint64(0)
	start_key := encodeKey(bitmap_id, 0, 0)
	limit_key := encodeKey(bitmap_id+1, 0, 0)
	iter := self.db.NewIterator(&Range{Start: start_key, Limit: limit_key}, nil)
	last_key = COUNTERMASK
	for iter.Next() {
		_, chunk_key, block_index := decodeKey(iter.Key())
		block, filter = decodeValue(iter.Value())
		if chunk_key != COUNTERMASK {
			if chunk_key != last_key {
				chunk = &Chunk{chunk_key, BlockArray{make([]uint64, 32, 32)}}
				bitmap.AddChunk(chunk)
			}
			chunk.Value.Block[block_index] = block

		} else {
			count = block
		}
		last_key = chunk_key
	}
	iter.Release()
	//err = iter.Error()

	delta := time.Since(start)
	util.SendTimer("leveldb_storage_Fetch", delta.Nanoseconds())
	bitmap.SetCount(uint64(count))
	return bitmap, uint64(filter)
}

func (self *LevelDBStorage) BeginBatch() {
	if self.batch == nil {
		self.batch = new(leveldb.Batch)
	}
	self.batch_counter++
}
func (self *LevelDBStorage) runBatch(batch *leveldb.Batch) {
	if batch != nil {
		self.db.Write(batch, nil)
	}
}
func (self *LevelDBStorage) FlushBatch() {
	start := time.Now()
	self.runBatch(self.batch) //maybe this is crazy but i'll give it a whirl
	self.batch = nil
	self.batch_time = time.Now()
	self.batch_counter = 0
	delta := time.Since(start)
	util.SendTimer("leveldb_storage_FlushBatch", delta.Nanoseconds())
}
func (self *LevelDBStorage) EndBatch() {
	start := time.Now()
	if self.batch != nil {
		last := time.Since(self.batch_time)
		if last*time.Second > 15 {
			self.FlushBatch()
		} else if self.batch_counter > 300 {
			self.FlushBatch()
		}
	} else {
		log.Println("NIL BATCH")
	}
	delta := time.Since(start)
	util.SendTimer("leveldb_storage_EndBatch", delta.Nanoseconds())

}

func (self *LevelDBStorage) Store(id uint64, db string, frame string, slice int, filter uint64, bitmap *Bitmap) error {
	self.BeginBatch()
	for i := bitmap.Min(); !i.Limit(); i = i.Next() {
		var chunk = i.Item()
		for idx, block := range chunk.Value.Block {
			block_index := int32(idx)
			if block != 0 {
				self.StoreBlock(id, db, frame, slice, filter, chunk.Key, block_index, block)
			}
		}
	}
	cnt := BitCount(bitmap)

	self.StoreBlock(id, db, frame, slice, filter, COUNTERMASK, 0, cnt)
	self.EndBatch()
	return nil
}

func (self *LevelDBStorage) StoreBlock(id uint64, db string, frame string, slice int, filter uint64, chunk uint64, block_index int32, block uint64) error {
	if self.batch == nil {
		panic("NIL BATCH")
	}
	start := time.Now()
	self.batch.Put(encodeKey(id, chunk, uint8(block_index)), encodeValue(block, filter))
	delta := time.Since(start)
	util.SendTimer("leveldb_storage_StoreBlock", delta.Nanoseconds())
	return nil
}

func (self *LevelDBStorage) RemoveBlock(id uint64, db string, frame string, slice int, chunk uint64, block_index int32) {
}

func (self *LevelDBStorage) RemoveBit(bid uint64, db string, frame string, slice int, filter uint64, bchunk uint64, block_index int32, count uint64) {
}

func (self *LevelDBStorage) Close() {
	self.FlushBatch()
	self.db.Close()
}

func (self *LevelDBStorage) StoreBit(bid uint64, db string, frame string, slice int, filter uint64, bchunk uint64, block_index int32, bblock, count uint64) {
	self.BeginBatch()
	self.StoreBlock(bid, db, frame, slice, filter, bchunk, block_index, bblock)
	self.StoreBlock(bid, db, frame, slice, filter, COUNTERMASK, 0, count)
	self.EndBatch()

}
