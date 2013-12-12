package index

// #cgo  CFLAGS:-mpopcnt

import (
	"log"
	"tux21b.org/v1/gocql"
)

type CassandraStorage struct {
	db *gocql.Session
}

func BuildSchema() {
	/*
	   "CREATE KEYSPACE IF NOT EXISTS hotbox WITH strategy_class = SimpleStrategy AND strategy_options:replication_factor = 1"
	   "CREATE TABLE IF NOT EXISTS bitmap ( bitmap_id bigint, db varchar, slice int, ChunkKey bigint,   BlockIndex int,   block bigint,    PRIMARY KEY ((bitmap_id, db, slice),ChunkKey,BlockIndex) )
	   "
	*/

}
func NewCassStorage() Storage {
	obj := new(CassandraStorage)
	cluster := gocql.NewCluster("127.0.0.1")
	cluster.Keyspace = "hotbox"
	cluster.Consistency = gocql.Quorum
	//cluster.ProtoVersion = 1
	// cluster.CQLVersion = "3.0.0"
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}

	err = session.Query("USE hotbox").Exec()
	if err != nil {
	}
	obj.db = session
	return obj
}

func (c *CassandraStorage) Fetch(bitmap_id uint64, db string, slice int) IBitmap {
	var dumb = COUNTERMASK
	last_key := int64(dumb)
	marker := int64(dumb)
	var id = int64(bitmap_id)

	var (
		chunk            *Chunk
		chunk_key, block int64
		block_index      uint32
		s8               uint8
	)
	log.Println("FETCHING ", bitmap_id, db, slice)

	bitmap := CreateRBBitmap()
	iter := c.db.Query("SELECT Chunkkey,BlockIndex,block FROM bitmap WHERE bitmap_id=? AND db=? AND slice=? ", id, db, slice).Iter()
	count := int64(0)
	for iter.Scan(&chunk_key, &block_index, &block) {
		s8 = uint8(block_index)
		if chunk_key != marker {
			if chunk_key != last_key {
				chunk = &Chunk{uint64(chunk_key), BlockArray{}}
				bitmap.AddChunk(chunk)
			}
			chunk.Value.Block[s8] = uint64(block)

		} else {
			count = block
		}
		last_key = chunk_key

	}
	bitmap.SetCount(uint64(count))
	return bitmap
}

func (c *CassandraStorage) Store(id int64, db string, slice int, bitmap *Bitmap) error {
	for i := bitmap.Min(); !i.Limit(); i = i.Next() {
		var chunk = i.Item()
		for idx, block := range chunk.Value.Block {
			block_index := int32(idx)
			iblock := int64(block)
			if iblock != 0 {
				c.StoreBlock(id, db, slice, int64(chunk.Key), block_index, iblock)
			}
		}
	}
	cnt := int64(BitCount(bitmap))

	var dumb = COUNTERMASK
	COUNTER_KEY := int64(dumb)

	c.StoreBlock(id, db, slice, COUNTER_KEY, 0, cnt)
	return nil
}

func (c *CassandraStorage) StoreBlock(id int64, db string, slice int, chunk int64, block_index int32, block int64) error {

	if err := c.db.Query(`INSERT INTO bitmap (bitmap_id, db, slice , ChunkKey, BlockIndex,block) VALUES (?,?,?, ?,?,?);`, id, db, slice, chunk, block_index, block).Exec(); err != nil {
		log.Println(err)
		log.Println("INSERT ", id, chunk, block_index)
		return err
	}
	return nil
}
