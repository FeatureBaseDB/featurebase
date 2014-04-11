package index

// #cgo  CFLAGS:-mpopcnt

import (
	"fmt"
	"log"
	"pilosa/util"

	"time"

	"tux21b.org/v1/gocql"
)

type CassandraStorage struct {
	db            *gocql.Session
	batch         *gocql.Batch
	stmt          string
	batch_time    time.Time
	batch_counter int
}

func BuildSchema() {
	/*
			   "CREATE KEYSPACE IF NOT EXISTS hotbox WITH strategy_class = SimpleStrategy AND strategy_options:replication_factor = 1"
		       create keyspace if not exists hotbox with replication = { 'class': 'SimpleStrategy', 'replication_factor' : 1} and durable_writes = true;
			   CREATE TABLE IF NOT EXISTS bitmap ( bitmap_id bigint, db varchar, frame varchar, slice int, filter int, ChunkKey bigint,   BlockIndex int,   block bigint,    PRIMARY KEY ((bitmap_id, db, frame,slice),ChunkKey,BlockIndex) )
			   "
	*/

}
func NewCassStorage(hosts []string, keyspace string) Storage {
	obj := new(CassandraStorage)
	//cluster := gocql.NewCluster("127.0.0.1")
	cluster := gocql.NewCluster(hosts...)
	cluster.Keyspace = keyspace
	//cluster.Consistency = gocql.Quorum
	cluster.Consistency = gocql.One
	//cluster.ProtoVersion = 1
	// cluster.CQLVersion = "3.0.0"
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}

	err = session.Query(fmt.Sprintf("USE %s", keyspace)).Exec()
	if err != nil {
	}
	obj.db = session
	obj.stmt = `INSERT INTO bitmap ( bitmap_id, db, frame, slice , filter, ChunkKey, BlockIndex, block) VALUES (?,?,?,?,?,?,?,?);`
	obj.batch = nil
	obj.batch_counter = 0
	return obj
}

func (c *CassandraStorage) Close() {
}
func (c *CassandraStorage) Fetch(bitmap_id uint64, db string, frame string, slice int) (IBitmap, uint64) {
	var dumb = COUNTERMASK
	last_key := int64(dumb)
	marker := int64(dumb)
	var id = int64(bitmap_id)
	start := time.Now()
	var (
		chunk            *Chunk
		chunk_key, block int64
		block_index      uint32
		s8               uint8
		filter           int
	)

	bitmap := CreateRBBitmap()
	iter := c.db.Query("SELECT filter,Chunkkey,BlockIndex,block FROM bitmap WHERE bitmap_id=? AND db=? AND frame=? AND slice=? ", id, db, frame, slice).Iter()
	count := int64(0)

	for iter.Scan(&filter, &chunk_key, &block_index, &block) {
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
	delta := time.Since(start)
	util.SendTimer("cassandra_storage_Fetch", delta.Nanoseconds())
	bitmap.SetCount(uint64(count))
	return bitmap, uint64(filter)
}
func (self *CassandraStorage) BeginBatch() {
	if self.batch == nil {
		self.batch = gocql.NewBatch(gocql.LoggedBatch)
	}
	self.batch_counter++
}
func (self *CassandraStorage) runBatch(batch *gocql.Batch) {
	if batch != nil {
		self.db.ExecuteBatch(batch)
	}
}
func (self *CassandraStorage) FlushBatch() {
	start := time.Now()
	go self.runBatch(self.batch) //maybe this is crazy but i'll give it a whirl
	self.batch = nil
	self.batch_time = time.Now()
	self.batch_counter = 0
	delta := time.Since(start)
	util.SendTimer("cassandra_storage_FlushBatch", delta.Nanoseconds())
}
func (self *CassandraStorage) EndBatch() {
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
	util.SendTimer("cassandra_storage_EndBatch", delta.Nanoseconds())

}

func (self *CassandraStorage) Store(id int64, db string, frame string, slice int, filter uint64, bitmap *Bitmap) error {
	self.BeginBatch()
	for i := bitmap.Min(); !i.Limit(); i = i.Next() {
		var chunk = i.Item()
		for idx, block := range chunk.Value.Block {
			block_index := int32(idx)
			iblock := int64(block)
			if iblock != 0 {
				self.StoreBlock(id, db, frame, slice, filter, int64(chunk.Key), block_index, iblock)
			}
		}
	}
	cnt := int64(BitCount(bitmap))

	var dumb = COUNTERMASK
	COUNTER_KEY := int64(dumb)

	self.StoreBlock(id, db, frame, slice, filter, COUNTER_KEY, 0, cnt)
	self.EndBatch()
	return nil
}

func (self *CassandraStorage) StoreBlock(id int64, db string, frame string, slice int, filter uint64, chunk int64, block_index int32, block int64) error {
	if self.batch == nil {
		panic("NIL BATCH")
	}
	start := time.Now()
	self.batch.Query(self.stmt, id, db, frame, slice, int(filter), chunk, block_index, block)
	delta := time.Since(start)
	util.SendTimer("cassandra_storage_StoreBlock", delta.Nanoseconds())
	return nil
}
