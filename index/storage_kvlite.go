package index

// #cgo  CFLAGS:-mpopcnt

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"

	"github.com/steveyen/gkvlite"
)

type KVStorage struct {
	db         *gkvlite.Store
	cc         *gkvlite.Collection
	evic_count int
}

func NewKVStorage(path string, slice int, db string) (Storage, error) {
	//	log.Println("Hello")
	obj := new(KVStorage)

	base_path := fmt.Sprintf("%s/%s", path, db)
	os.MkdirAll(base_path, 0755)

	f, err := os.Create(fmt.Sprintf("%s/%d.kvlite", base_path, slice))
	s, err := gkvlite.NewStore(f)
	obj.cc = s.SetCollection(db, nil)

	obj.db = s

	return obj, err
}

func (self *KVStorage) Fetch(bitmap_id uint64, db string, slice int) IBitmap {

	var (
		chunk *Chunk
	)
	last_key := COUNTER_KEY
	marker := COUNTER_KEY
	count := uint64(0)

	key, _ := toKeyBytes(int64(bitmap_id), slice, 0, 0)

	bitmap := CreateRBBitmap()

	self.cc.VisitItemsAscend(key, true, func(i *gkvlite.Item) bool {
		bid, _, chunk_key, block_index, _ := fromKeyBytes(i.Key)
		if bid != int64(bitmap_id) {
			return false
		}
		block, _ := binary.Uvarint(i.Val) //just need to cast as a block
		if chunk_key != marker {
			if chunk_key != last_key {
				chunk = &Chunk{uint64(chunk_key), BlockArray{}}
				bitmap.AddChunk(chunk)
			}
			chunk.Value.Block[block_index] = block

		} else {
			count = block
		}
		last_key = chunk_key
		return true
	})
	bitmap.SetCount(uint64(count))
	return bitmap
}

func (self *KVStorage) Store(bitmap_id int64, db string, slice int, bitmap *Bitmap) error {
	for i := bitmap.Min(); !i.Limit(); i = i.Next() {
		var chunk = i.Item()
		for idx, block := range chunk.Value.Block {
			block_index := int32(idx)
			iblock := int64(block)
			if iblock != 0 {
				self.StoreBlock(bitmap_id, db, slice, int64(chunk.Key), block_index, iblock)
			}
		}
	}
	cnt := int64(BitCount(bitmap))

	//var dumb = COUNTERMASK
	//COUNTER_KEY := int64(dumb)

	self.StoreBlock(bitmap_id, db, slice, COUNTER_KEY, 0, cnt)
	self.db.Flush()
	return nil

}
func Bytes(src int64) []byte {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, src)
	if err != nil {
		fmt.Println("binary.Write failed:", err)
	}
	return buf.Bytes()
}
func fromKeyBytes(src []byte) (bitmap_id int64, slice int, chunk_key int64, block_index int32, err error) {
	buf := bytes.NewReader(src)
	err = binary.Read(buf, binary.LittleEndian, &bitmap_id)
	err = binary.Read(buf, binary.LittleEndian, &slice)
	err = binary.Read(buf, binary.LittleEndian, &chunk_key)
	err = binary.Read(buf, binary.LittleEndian, &block_index)
	return
}

func toKeyBytes(bitmap_id int64, slice int, chunk_key int64, block_index int32) ([]byte, error) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, bitmap_id)
	err = binary.Write(buf, binary.LittleEndian, slice)
	err = binary.Write(buf, binary.LittleEndian, chunk_key)
	err = binary.Write(buf, binary.LittleEndian, block_index)

	return buf.Bytes(), err
}

func (self *KVStorage) StoreBlock(bitmap_id int64, db string, slice int, chunk_key int64, block_index int32, block int64) error {
	//key := fmt.Sprintf("%d%d:%d:%d", bitmap_id, slice, chunk_key, block_index)
	key, _ := toKeyBytes(bitmap_id, slice, chunk_key, block_index)
	value := Bytes(block)

	self.cc.Set(key, value)
	self.evic_count += 1
	if self.evic_count > 10000 {
		self.cc.EvictSomeItems()
		self.evic_count = 0
	}
	return nil
}
