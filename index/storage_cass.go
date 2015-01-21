package index

// #cgo  CFLAGS:-mpopcnt

import (
	"log"
	"pilosa/config"
	"pilosa/util"

	//"sync/atomic"
	"time"

	"github.com/gocql/gocql"
)

type CassandraStorage struct {
	db                    *gocql.Session
	batch                 *gocql.Batch
	stmt                  string
	batch_time            time.Time
	batch_counter         int
	cass_time_window_secs float64
	cass_flush_size       int
	cass_queue            CassQueue
}

var cluster *gocql.ClusterConfig

func init() {
	hosts := config.GetStringArrayDefault("cassandra_hosts", []string{"localhost"})
	keyspace := config.GetStringDefault("cassandra_keyspace", "pilosa")
	cluster = gocql.NewCluster(hosts...)
	cluster.Keyspace = keyspace
	cluster.Consistency = gocql.One
	cluster.Timeout = 5 * time.Second
	cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: 10}
}

func BuildSchema() {
	/*
			   "CREATE KEYSPACE IF NOT EXISTS pilosa WITH strategy_class = SimpleStrategy AND strategy_options:replication_factor = 1"
		       create keyspace if not exists pilosa with replication = {'class': 'SimpleStrategy', 'replication_factor' : 1} and durable_writes = true;
			   CREATE TABLE IF NOT EXISTS bitmap (bitmap_id bigint, db varchar, frame varchar, slice int, filter int, chunkkey bigint, blockindex int, block bigint, PRIMARY KEY ((bitmap_id, db, frame, slice), chunkkey, blockindex) )
			   "
	*/

}
func NewCassStorage() Storage {
	obj := new(CassandraStorage)
	// cluster.CQLVersion = "3.0.0"
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}

	obj.db = session
	obj.stmt = `INSERT INTO bitmap ( bitmap_id, db, frame, slice , filter, ChunkKey, BlockIndex, block) VALUES (?,?,?,?,?,?,?,?)  USING timestamp ?;`
	obj.batch = nil
	obj.batch_time = time.Now()
	obj.batch_counter = 0
	obj.cass_time_window_secs = float64(config.GetIntDefault("cassandra_time_window_secs", 5))
	obj.cass_flush_size = config.GetIntDefault("cassandra_max_size_batch", 15)
	obj.cass_queue = NewCassQueue()
	go obj.asyncStore()
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
		go self.db.ExecuteBatch(batch)
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
		/*
			last := time.Since(self.batch_time)
			if last.Seconds() > self.cass_time_window_secs {
				self.FlushBatch()
			} else if self.batch_counter > self.cass_flush_size {
				self.FlushBatch()
			}
		*/
	} else {
		log.Println("NIL BATCH")
	}
	delta := time.Since(start)
	util.SendTimer("cassandra_storage_EndBatch", delta.Nanoseconds())

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

type CassRecord struct {
	bitmap_id   uint64
	db          string
	frame       string
	slice       int
	filter      uint64
	chunk       uint64
	block_index int32
	val         uint64
	count       uint64
}

func (self *CassandraStorage) asyncStore() {
	for {
		rec, term := self.cass_queue.Pop()
		if term {
			break
		}
		self.BeginBatch()
		self.StoreBlock(rec.bitmap_id, rec.db, rec.frame, rec.slice, rec.filter, rec.chunk, rec.block_index, rec.val)
		self.StoreBlock(rec.bitmap_id, rec.db, rec.frame, rec.slice, rec.filter, COUNTERMASK, 0, rec.count)
		self.EndBatch()
	}
}

func (self *CassandraStorage) StoreBit(bid uint64, db string, frame string, slice int, filter uint64, bchunk uint64, block_index int32, bblock, count uint64) {
	rec := CassRecord{bid, db, frame, slice, filter, bchunk, block_index, bblock, count}
	self.cass_queue.Push(rec)
}

type CassQueue struct {
	size   int64
	buffer chan CassRecord
}

func NewCassQueue() CassQueue {
	//return CassQueue{0, make(chan CassRecord, 4096)}
	return CassQueue{0, make(chan CassRecord)}
}

func (self *CassQueue) Push(rec CassRecord) {
	//	atomic.AddInt64(&self.size, 1)
	self.buffer <- rec
}
func (self *CassQueue) Pop() (CassRecord, bool) {
	ret := <-self.buffer
	//	atomic.AddInt64(&self.size, -1)
	return ret, false
}
