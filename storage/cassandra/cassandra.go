package cassandra

import (
	"time"

	log "github.com/cihub/seelog"
	"github.com/gocql/gocql"
	"github.com/umbel/pilosa"
	"github.com/umbel/pilosa/statsd"
)

func init() {
	pilosa.RegisterStorage("cassandra",
		func(opt pilosa.StorageOptions) pilosa.Storage {
			return NewStorage(opt)
		},
	)
}

// CREATE KEYSPACE IF NOT EXISTS pilosa WITH strategy_class = SimpleStrategy AND strategy_options:replication_factor = 1"
// create keyspace if not exists pilosa with replication = {'class': 'SimpleStrategy', 'replication_factor' : 1} and durable_writes = true;
// CREATE KEYSPACE pilosa WITH replication = {'class': 'NetworkTopologyStrategy', 'pilpang': '2'}  AND durable_writes = true;
// CREATE TABLE IF NOT EXISTS bitmap (bitmap_id bigint, db varchar, frame varchar, slice int, filter int, chunkkey bigint, blockindex int, block bigint, PRIMARY KEY ((bitmap_id, db, frame, slice), chunkkey, blockindex) )

// DefaultHosts are the default hosts in the cassandra cluster.
var DefaultHosts = []string{"localhost"}

const (
	// DefaultKeyspace is the default keyspace used in cassandra.
	DefaultKeyspace = "pilosa"

	// DefaultFlushInterval is the default maximum time between flushes.
	DefaultFlushInterval = 5 * time.Second

	// DefaultFlushThreshold is the default maximum number of items to batch.
	DefaultFlushThreshold = 15
)

// Storage represents Cassandra-backed storage for bitmaps.
type Storage struct {
	session *gocql.Session

	batch     *gocql.Batch
	batchTime time.Time
	batchN    int

	FlushInterval  time.Duration
	FlushThreshold int

	Hosts    []string
	Keyspace string
}

// NewStorage returns a new, uninitialized instance of Storage.
func NewStorage(opt pilosa.StorageOptions) *Storage {
	return &Storage{
		batchTime: time.Now(),

		FlushInterval:  DefaultFlushInterval,
		FlushThreshold: DefaultFlushThreshold,

		Hosts:    DefaultHosts,
		Keyspace: DefaultKeyspace,
	}
}

// Open opens the connection to the cassandra cluster.
func (s *Storage) Open() error {
	// Create cluster configuration.
	config := gocql.NewCluster(s.Hosts...)
	config.Keyspace = s.Keyspace
	config.Consistency = gocql.One
	config.Timeout = 5 * time.Second
	config.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: 10}

	// Connect to cassandra.
	session, err := config.CreateSession()
	if err != nil {
		return err
	}
	s.session = session

	return nil
}

// Close closes the connection to the cassandra cluster.
func (s *Storage) Close() error {
	if s.session != nil {
		s.Flush()
		s.session.Close()
	}

	return nil
}

// Fetch returns a bitmap by ID.
func (s *Storage) Fetch(bitmapID uint64, db string, frame string, slice int) (*pilosa.Bitmap, uint64) {
	bm := pilosa.NewBitmap()

	// Start benchmark.
	start := time.Now()

	// Create iterator over bitmap.
	itr := s.session.Query("SELECT filter, Chunkkey, BlockIndex, block FROM bitmap WHERE bitmap_id=? AND db=? AND frame=? AND slice=? ",
		u64toi64(bitmapID), db, frame, slice).Iter()

	// Iterate over chunks and materialize bitmap object.
	var chunk *pilosa.Chunk
	var chunkKey, block, count int64
	var blockIndex uint32
	var filter int
	lastKey := int64(-1)

	for itr.Scan(&filter, &chunkKey, &blockIndex, &block) {
		if chunkKey != int64(-1) {
			if chunkKey != lastKey {
				chunk = &pilosa.Chunk{uint64(chunkKey), pilosa.NewBlocks()}
				bm.AddChunk(chunk)
			}
			chunk.Value[uint8(blockIndex)] = uint64(block)
		} else {
			count = block
		}
		lastKey = chunkKey
	}

	statsd.SendTimer("cassandra_storage_Fetch", time.Since(start).Nanoseconds())
	statsd.SendInc("cassandra_storage_Read")

	// Set total bits set.
	bm.SetCount(uint64(count))
	return bm, uint64(filter)
}

// beginBatch starts a batch if one is not already in progress.
func (s *Storage) beginBatch() {
	if s.batch == nil {
		s.batch = gocql.NewBatch(gocql.UnloggedBatch)
	}
	s.batchN++
}

// endBatch flushes a batch if one is in progress.
func (s *Storage) endBatch() {
	if s.batch == nil {
		return
	}

	start := time.Now()
	s.Flush()
	statsd.SendTimer("cassandra_storage_EndBatch", time.Since(start).Nanoseconds())
	statsd.SendInc("cassandra_storage_Write")
}

// Flush flushes the current batch to storage.
func (s *Storage) Flush() {
	start := time.Now()

	// If batch exists then flush it.
	if s.batch != nil {
		if err := s.session.ExecuteBatch(s.batch); err != nil {
			log.Warn("Batch ERROR: ", err)
		}
	}

	// Clear batch and threshold time and count.
	s.batch = nil
	s.batchTime = time.Now()
	s.batchN = 0

	statsd.SendTimer("cassandra_storage_FlushBatch", time.Since(start).Nanoseconds())
}

// Store saves a bitmap to storage.
func (s *Storage) Store(id uint64, db string, frame string, slice int, filter uint64, bm *pilosa.Bitmap) error {
	s.beginBatch()

	for i := bm.ChunkIterator(); !i.Limit(); i = i.Next() {
		var chunk = i.Item()
		for idx, block := range chunk.Value {
			if block != 0 {
				s.StoreBlock(id, db, frame, slice, filter, chunk.Key, int32(idx), block)
			}
		}
	}

	s.StoreBlock(id, db, frame, slice, filter, pilosa.CounterMask, 0, bm.BitCount())
	s.endBatch()
	return nil
}

// StoreBlock saves a block to storage.
func (s *Storage) StoreBlock(bid uint64, db string, frame string, slice int, filter uint64, bchunk uint64, blockIndex int32, bblock uint64) error {
	id := u64toi64(bid) // these functions ignore overflow
	block := u64toi64(bblock)
	chunk := u64toi64(bchunk)

	if s.batch == nil {
		s.beginBatch()
	}

	start := time.Now()
	s.batch.Query(`INSERT INTO bitmap ( bitmap_id, db, frame, slice , filter, ChunkKey, BlockIndex, block) VALUES (?,?,?,?,?,?,?,?)  USING timestamp ?;`,
		id, db, frame, slice, int(filter), chunk, blockIndex, block, start.UnixNano())
	statsd.SendTimer("cassandra_storage_StoreBlock", time.Since(start).Nanoseconds())

	return nil
}

// RemoveBlock deletes a block from storage.
func (s *Storage) RemoveBlock(bid uint64, db string, frame string, slice int, bchunk uint64, blockIndex int32) {
	log.Trace("RemoveBlock", bid, db, frame, slice, bchunk, blockIndex)

	id := u64toi64(bid) //these fucntions ignore overflow
	chunk := u64toi64(bchunk)

	if s.batch == nil {
		s.beginBatch()
	}

	startTime := time.Now()

	s.batch.Query(`DELETE FROM bitmap USING TIMESTAMP ? WHERE bitmap_id=? AND db=? AND frame=? AND slice=? AND chunkkey=? AND blockindex=?`,
		startTime.UnixNano(), id, db, frame, slice, chunk, blockIndex)

	statsd.SendTimer("cassandra_storage_DeleteBlock", time.Since(startTime).Nanoseconds())
}

func (s *Storage) StoreBit(bid uint64, db string, frame string, slice int, filter uint64, chunk uint64, blockIndex int32, val, count uint64) {
	s.beginBatch()
	s.StoreBlock(bid, db, frame, slice, filter, chunk, blockIndex, val)
	s.StoreBlock(bid, db, frame, slice, filter, pilosa.CounterMask, 0, count)
	s.endBatch()
}

func (s *Storage) RemoveBit(id uint64, db string, frame string, slice int, filter uint64, chunk uint64, blockIndex int32, count uint64) {
	log.Trace("RemoveBit", id, db, frame, slice, chunk, blockIndex)
	s.beginBatch()
	s.RemoveBlock(id, db, frame, slice, chunk, blockIndex)
	s.StoreBlock(id, db, frame, slice, filter, pilosa.CounterMask, 0, count)
	s.endBatch()
}

func u64toi64(v uint64) int64 { return int64(v) }
