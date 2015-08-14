package index

// #cgo  CFLAGS:-mpopcnt

import (
	"time"

	log "github.com/cihub/seelog"
	"github.com/gocql/gocql"
	"github.com/umbel/pilosa/util"
)

// DefaultStorageHosts are the hosts that stores the data.
var DefaultStorageHosts = [...]string{"localhost"}

// DefaultStorageKeyspace is the default keyspace used for storage.
const DefaultStorageKeyspace = "pilosa"

const DefaultCassandraTimeWindow = 5 * time.Second
const DefaultCassandraMaxSizeBatch = 15

var StorageHosts = DefaultStorageHosts[:]
var StorageKeyspace = DefaultStorageKeyspace
var CassandraTimeWindow = DefaultCassandraTimeWindow
var CassandraMaxSizeBatch = DefaultCassandraMaxSizeBatch

type CassandraStorage struct {
	db                    *gocql.Session
	batch                 *gocql.Batch
	stmt                  string
	dstmt                 string
	batch_time            time.Time
	batch_counter         int
	cass_time_window_secs float64
	cass_flush_size       int
}

var cluster *gocql.ClusterConfig
var session *gocql.Session

func SetupCassandra() {
	var err error
	hosts := StorageHosts
	keyspace := StorageKeyspace
	cluster = gocql.NewCluster(hosts...)
	cluster.Keyspace = keyspace
	cluster.Consistency = gocql.One
	cluster.Timeout = 5 * time.Second
	cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: 10}
	session, err = cluster.CreateSession()
	if err != nil {
		log.Warn(err)
	}
}

func BuildSchema() {
	/*
					   "CREATE KEYSPACE IF NOT EXISTS pilosa WITH strategy_class = SimpleStrategy AND strategy_options:replication_factor = 1"
				       create keyspace if not exists pilosa with replication = {'class': 'SimpleStrategy', 'replication_factor' : 1} and durable_writes = true;
				       CREATE KEYSPACE pilosa WITH replication = {'class': 'NetworkTopologyStrategy', 'pilpang': '2'}  AND durable_writes = true;

		  CREATE TABLE IF NOT EXISTS bitmap (bitmap_id bigint, db varchar, frame varchar, slice int, filter int, chunkkey bigint, blockindex int, block bigint, PRIMARY KEY ((bitmap_id, db, frame, slice), chunkkey, blockindex) )
					   "
	*/

}

func NewCassStorage() Storage {
	obj := new(CassandraStorage)
	// cluster.CQLVersion = "3.0.0"
	/*session, err := cluster.CreateSession()
	if err != nil {
		log.Warn(err)
	}
	*/

	obj.db = session
	obj.stmt = `INSERT INTO bitmap ( bitmap_id, db, frame, slice , filter, ChunkKey, BlockIndex, block) VALUES (?,?,?,?,?,?,?,?)  USING timestamp ?;`
	obj.dstmt = `DELETE FROM bitmap USING TIMESTAMP ? WHERE bitmap_id=? AND db=? AND frame=? AND slice=? AND chunkkey=? AND blockindex=?`
	obj.batch = nil
	obj.batch_time = time.Now()
	obj.batch_counter = 0
	obj.cass_time_window_secs = float64(CassandraTimeWindow.Seconds())
	obj.cass_flush_size = CassandraMaxSizeBatch
	return obj
}

func (c *CassandraStorage) Close() {
}
func (c *CassandraStorage) Fetch(bitmap_id uint64, db string, frame string, slice int) (IBitmap, uint64) {
	var dumb = COUNTERMASK
	last_key := int64(dumb)
	marker := int64(dumb)
	var id = util.Uint64ToInt64(bitmap_id)
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
				chunk = &Chunk{uint64(chunk_key), BlockArray{make([]uint64, 32, 32)}}
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
	util.SendInc("cassandra_storage_Read")
	bitmap.SetCount(uint64(count))
	return bitmap, uint64(filter)
}
func (self *CassandraStorage) BeginBatch() {
	if self.batch == nil {
		self.batch = gocql.NewBatch(gocql.UnloggedBatch)
	}
	self.batch_counter++
}
func (self *CassandraStorage) runBatch(batch *gocql.Batch) {
	if batch != nil {
		err := self.db.ExecuteBatch(batch)
		if err != nil {
			log.Warn("Batch ERROR", err)
		}
	}
}
func (self *CassandraStorage) FlushBatch() {
	start := time.Now()
	self.runBatch(self.batch) //maybe this is crazy but i'll give it a whirl
	self.batch = nil
	self.batch_time = time.Now()
	self.batch_counter = 0
	delta := time.Since(start)
	util.SendTimer("cassandra_storage_FlushBatch", delta.Nanoseconds())
}
func (self *CassandraStorage) EndBatch() {
	start := time.Now()
	if self.batch != nil {
		self.FlushBatch()
	} else {
		log.Warn("NIL BATCH")
	}
	delta := time.Since(start)
	util.SendTimer("cassandra_storage_EndBatch", delta.Nanoseconds())
	util.SendInc("cassandra_storage_Write")

}

func (self *CassandraStorage) Store(id uint64, db string, frame string, slice int, filter uint64, bitmap *Bitmap) error {
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

func (self *CassandraStorage) StoreBlock(bid uint64, db string, frame string, slice int, filter uint64, bchunk uint64, block_index int32, bblock uint64) error {
	id := util.Uint64ToInt64(bid) //these fucntions ignore overflow
	block := util.Uint64ToInt64(bblock)
	chunk := util.Uint64ToInt64(bchunk)

	if self.batch == nil {
		self.BeginBatch()
	}
	start := time.Now()
	self.batch.Query(self.stmt, id, db, frame, slice, int(filter), chunk, block_index, block, start.UnixNano())
	delta := time.Since(start)
	util.SendTimer("cassandra_storage_StoreBlock", delta.Nanoseconds())
	return nil
}

func (self *CassandraStorage) StoreBit(bid uint64, db string, frame string, slice int, filter uint64, chunk uint64, block_index int32, val, count uint64) {
	self.BeginBatch()
	self.StoreBlock(bid, db, frame, slice, filter, chunk, block_index, val)
	self.StoreBlock(bid, db, frame, slice, filter, COUNTERMASK, 0, count)
	self.EndBatch()
}

func (self *CassandraStorage) RemoveBit(id uint64, db string, frame string, slice int, filter uint64, chunk uint64, block_index int32, count uint64) {
	log.Trace("RemoveBit", id, db, frame, slice, chunk, block_index)
	self.BeginBatch()
	self.RemoveBlock(id, db, frame, slice, chunk, block_index)
	self.StoreBlock(id, db, frame, slice, filter, COUNTERMASK, 0, count)
	self.EndBatch()
}

func (self *CassandraStorage) RemoveBlock(bid uint64, db string, frame string, slice int, bchunk uint64, block_index int32) {
	log.Trace("RemoveBBlock", bid, db, frame, slice, bchunk, block_index)
	id := util.Uint64ToInt64(bid) //these fucntions ignore overflow
	chunk := util.Uint64ToInt64(bchunk)

	if self.batch == nil {
		self.BeginBatch()
	}
	start := time.Now()

	self.batch.Query(self.dstmt, start.UnixNano(), id, db, frame, slice, chunk, block_index)
	delta := time.Since(start)
	util.SendTimer("cassandra_storage_DeleteBlock", delta.Nanoseconds())
}
