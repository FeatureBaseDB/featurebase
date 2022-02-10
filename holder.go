// Copyright 2021 Molecula Corp. All rights reserved.
package pilosa

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/molecula/featurebase/v3/disco"
	"github.com/molecula/featurebase/v3/logger"
	rbfcfg "github.com/molecula/featurebase/v3/rbf/cfg"
	"github.com/molecula/featurebase/v3/roaring"
	"github.com/molecula/featurebase/v3/stats"
	"github.com/molecula/featurebase/v3/storage"
	"github.com/molecula/featurebase/v3/testhook"
	"github.com/molecula/featurebase/v3/topology"
	"github.com/molecula/featurebase/v3/vprint"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

const (
	// defaultCacheFlushInterval is the default value for Fragment.CacheFlushInterval.
	defaultCacheFlushInterval = 1 * time.Minute

	// existenceFieldName is the name of the internal field used to store existence values.
	existenceFieldName = "_exists"

	// DiscoDir is the default data directory used by the disco implementation.
	DiscoDir = "disco"

	// IndexesDir is the default indexes directory used by the holder.
	IndexesDir = "indexes"

	// FieldsDir is the default fields directory used by each index.
	FieldsDir = "fields"
)

func init() {
	// needed to get the most I/O throughtpu.
	runtime.GOMAXPROCS(runtime.NumCPU())

	// For performance tuning, leave these readily available:
	// CPUProfileForDur(time.Minute, "server.cpu.pprof")
	// MemProfileForDur(2*time.Minute, "server.mem.pprof")
}

// Holder represents a container for indexes.
type Holder struct {
	mu sync.RWMutex

	// our configuration
	cfg *HolderConfig

	// Partition count used by translation.
	partitionN int

	// opened channel is closed once Open() completes.
	opened lockedChan

	broadcaster broadcaster
	schemator   disco.Schemator
	sharder     disco.Sharder
	serializer  Serializer

	// executor, which we use only to get access to its worker pool
	executor *executor

	// Close management
	wg      sync.WaitGroup
	closing chan struct{}

	// Stats
	Stats stats.StatsClient

	// Data directory path.
	path string

	// The interval at which the cached row ids are persisted to disk.
	cacheFlushInterval time.Duration

	Logger logger.Logger

	// Instantiates new translation stores
	OpenTranslateStore  OpenTranslateStoreFunc
	OpenTranslateReader OpenTranslateReaderFunc

	// Func to open whatever implementation of transaction store we're using.
	OpenTransactionStore OpenTransactionStoreFunc

	// Func to open the ID allocator.
	OpenIDAllocator func(string, bool) (*idAllocator, error)

	// transactionManager
	transactionManager *TransactionManager

	translationSyncer TranslationSyncer

	ida *idAllocator

	// Queue of fields (having a foreign index) which have
	// opened before their foreign index has opened.
	foreignIndexFields   []*Field
	foreignIndexFieldsMu sync.Mutex

	// Queue of messages to broadcast in bulk when the cluster comes up.
	// This is wrong, but. . . yeah.
	startMsgs   []Message
	startMsgsMu sync.Mutex

	// opening is set to true while Holder is opening.
	// It's used to determine if foreign index application
	// needs to be queued and completed after all indexes
	// have opened.
	opening bool

	Opts HolderOpts

	Auditor testhook.Auditor

	txf *TxFactory

	lookupDB *sql.DB

	// a separate lock out for indexes, to avoid the deadlock/race dilema
	// on holding mu.
	imu     sync.RWMutex
	indexes map[string]*Index
}

// HolderOpts holds information about the holder which other things might want
// to look up later while using the holder.
type HolderOpts struct {
	// StorageBackend controls the tx/storage engine we instatiate. Set by
	// server.go OptServerStorageConfig
	StorageBackend string
}

func (h *Holder) StartTransaction(ctx context.Context, id string, timeout time.Duration, exclusive bool) (*Transaction, error) {
	return h.transactionManager.Start(ctx, id, timeout, exclusive)
}

func (h *Holder) FinishTransaction(ctx context.Context, id string) (*Transaction, error) {
	return h.transactionManager.Finish(ctx, id)
}

func (h *Holder) Transactions(ctx context.Context) (map[string]*Transaction, error) {
	return h.transactionManager.List(ctx)
}

func (h *Holder) GetTransaction(ctx context.Context, id string) (*Transaction, error) {
	return h.transactionManager.Get(ctx, id)
}

// lockedChan looks a little ridiculous admittedly, but exists for good reason.
// The channel within is used (for example) to signal to other goroutines when
// the Holder has finished opening (via closing the channel). However, it is
// possible for the holder to be closed and then reopened, but a channel which
// is closed cannot be re-opened. We must create a new channel - this creates a
// data race with any goroutine which might be accessing the channel. To ensure
// that there is no data race on the value of the channel itself, we wrap any
// operation on it with an RWMutex so that we can guarantee that nothing is
// trying to listen on it when it gets swapped.
type lockedChan struct {
	ch chan struct{}
	mu sync.RWMutex
}

func (lc *lockedChan) Close() {
	lc.mu.RLock()
	close(lc.ch)
	lc.mu.RUnlock()
}

func (lc *lockedChan) Recv() {
	lc.mu.RLock()
	<-lc.ch
	lc.mu.RUnlock()
}

// HolderConfig holds configuration details that need to be set up at
// initial holder creation. NewHolder takes a *HolderConfig, which can be
// nil. Use DefaultHolderConfig to get a default-valued HolderConfig you
// can then alter.
type HolderConfig struct {
	PartitionN           int
	OpenTranslateStore   OpenTranslateStoreFunc
	OpenTranslateReader  OpenTranslateReaderFunc
	OpenTransactionStore OpenTransactionStoreFunc
	OpenIDAllocator      OpenIDAllocatorFunc
	TranslationSyncer    TranslationSyncer
	Serializer           Serializer
	Schemator            disco.Schemator
	Sharder              disco.Sharder
	CacheFlushInterval   time.Duration
	StatsClient          stats.StatsClient
	Logger               logger.Logger

	StorageConfig       *storage.Config
	RBFConfig           *rbfcfg.Config
	AntiEntropyInterval time.Duration

	LookupDBDSN string
}

func DefaultHolderConfig() *HolderConfig {
	return &HolderConfig{
		PartitionN:           topology.DefaultPartitionN,
		OpenTranslateStore:   OpenInMemTranslateStore,
		OpenTranslateReader:  nil,
		OpenTransactionStore: OpenInMemTransactionStore,
		OpenIDAllocator:      func(string, bool) (*idAllocator, error) { return &idAllocator{}, nil },
		TranslationSyncer:    NopTranslationSyncer,
		Serializer:           GobSerializer,
		Schemator:            disco.InMemSchemator,
		Sharder:              disco.InMemSharder,
		CacheFlushInterval:   defaultCacheFlushInterval,
		StatsClient:          stats.NopStatsClient,
		Logger:               logger.NopLogger,
		StorageConfig:        storage.NewDefaultConfig(),
		RBFConfig:            rbfcfg.NewDefaultConfig(),
	}
}

// NewHolder returns a new instance of Holder for the given path.
func NewHolder(path string, cfg *HolderConfig) *Holder {
	if cfg == nil {
		cfg = DefaultHolderConfig()
	}
	if cfg.StorageConfig == nil {
		cfg.StorageConfig = storage.NewDefaultConfig()
	}
	if cfg.RBFConfig == nil {
		cfg.RBFConfig = rbfcfg.NewDefaultConfig()
	}

	h := &Holder{
		cfg:     cfg,
		closing: make(chan struct{}),

		opened: lockedChan{ch: make(chan struct{})},

		broadcaster: NopBroadcaster,

		partitionN:           cfg.PartitionN,
		Stats:                cfg.StatsClient,
		cacheFlushInterval:   cfg.CacheFlushInterval,
		OpenTranslateStore:   cfg.OpenTranslateStore,
		OpenTranslateReader:  cfg.OpenTranslateReader,
		OpenTransactionStore: cfg.OpenTransactionStore,
		OpenIDAllocator:      cfg.OpenIDAllocator,
		translationSyncer:    cfg.TranslationSyncer,
		serializer:           cfg.Serializer,
		sharder:              cfg.Sharder,
		schemator:            cfg.Schemator,
		Logger:               cfg.Logger,
		Opts:                 HolderOpts{StorageBackend: cfg.StorageConfig.Backend},

		Auditor: NewAuditor(),

		path: path,

		indexes: make(map[string]*Index),
	}

	txf, err := NewTxFactory(cfg.StorageConfig.Backend, h.IndexesPath(), h)
	vprint.PanicOn(err)
	h.txf = txf

	_ = testhook.Created(h.Auditor, h, nil)
	return h
}

// Path returns the path directory the holder was created with.
func (h *Holder) Path() string {
	return h.path
}

// IndexesPath returns the path of the indexes directory.
func (h *Holder) IndexesPath() string {
	return filepath.Join(h.path, IndexesDir)
}

// Open initializes the root data directory for the holder.
func (h *Holder) Open() error {
	h.opening = true
	defer func() { h.opening = false }()

	if h.txf == nil {
		txf, err := NewTxFactory(h.cfg.StorageConfig.Backend, h.IndexesPath(), h)
		if err != nil {
			return errors.Wrap(err, "Holder.Open NewTxFactory()")
		}
		h.txf = txf
	}

	// Reset closing in case Holder is being reopened.
	h.closing = make(chan struct{})

	h.Logger.Printf("open holder path: %s", h.path)
	if err := os.MkdirAll(h.IndexesPath(), 0777); err != nil {
		return errors.Wrap(err, "creating directory")
	}

	tstore, err := h.OpenTransactionStore(h.path)
	if err != nil {
		return errors.Wrap(err, "opening transaction store")
	}
	h.transactionManager = NewTransactionManager(tstore)
	h.transactionManager.Log = h.Logger

	// Open ID allocator.
	h.ida, err = h.OpenIDAllocator(filepath.Join(h.path, "idalloc.db"), h.cfg.StorageConfig.FsyncEnabled)
	if err != nil {
		return errors.Wrap(err, "opening ID allocator")
	}

	// Load schema from etcd.
	schema, err := h.schemator.Schema(context.Background())
	if err != nil {
		return errors.Wrap(err, "getting schema")
	}

	// Open path to read all index directories.
	f, err := os.Open(h.IndexesPath())
	if err != nil {
		return errors.Wrap(err, "opening directory")
	}
	defer f.Close()

	fis, err := f.Readdir(0)
	if err != nil {
		return errors.Wrap(err, "reading directory")
	}

	for _, fi := range fis {
		// Skip files or hidden directories.
		if !fi.IsDir() || strings.HasPrefix(fi.Name(), ".") {
			continue
		}

		// Only continue with indexes which are present in schema.
		idx, ok := schema[fi.Name()]
		if !ok {
			continue
		}

		// decode the CreateIndexMessage from the schema data in order to
		// get its metadata, such as CreateAt.
		cim, err := decodeCreateIndexMessage(h.serializer, idx.Data)
		if err != nil {
			return errors.Wrap(err, "decoding create index message")
		}

		h.Logger.Printf("opening index: %s", filepath.Base(fi.Name()))

		index, err := h.newIndex(h.IndexPath(filepath.Base(fi.Name())), filepath.Base(fi.Name()))
		if errors.Cause(err) == ErrName {
			h.Logger.Errorf("opening index: %s, err=%s", fi.Name(), err)
			continue
		} else if err != nil {
			return errors.Wrap(err, "opening index")
		}

		// Since we don't have createdAt stored on disk within the data
		// directory, we need to populate it from the etcd schema data.
		// TODO: we may no longer need the createdAt value stored in memory on
		// the index struct; it may only be needed in the schema return value
		// from the API, which already comes from etcd. In that case, this logic
		// could be removed, and the createdAt on the index struct could be
		// removed.
		index.createdAt = cim.CreatedAt

		err = index.OpenWithSchema(idx)
		if err != nil {
			_ = h.txf.Close()
			if err == ErrName {
				h.Logger.Errorf("opening index: %s, err=%s", index.Name(), err)
				continue
			}
			return fmt.Errorf("open index: name=%s, err=%s", index.Name(), err)
		}
		h.addIndex(index)
	}

	// If any fields were opened before their foreign index
	// was opened, it's safe to process those now since all index
	// opens have completed by this point.
	if err := h.processForeignIndexFields(); err != nil {
		return errors.Wrap(err, "processing foreign index fields")
	}

	h.Stats.Open()

	h.opened.Close()

	_ = testhook.Opened(h.Auditor, h, nil)

	if err := h.txf.Open(); err != nil {
		return errors.Wrap(err, "Holder.Open h.txf.Open()")
	}

	if h.cfg.LookupDBDSN != "" {
		h.Logger.Printf("connecting to lookup database")

		db, err := sql.Open("postgres", h.cfg.LookupDBDSN)
		if err != nil {
			return errors.Wrap(err, "connecting to lookup database")
		}
		if err := db.Ping(); err != nil {
			return errors.Wrap(err, "pinging lookup database")
		}

		h.Logger.Printf("connection to lookup database succeeded, connection stats: %+v", db.Stats())

		h.lookupDB = db
	}

	h.Logger.Printf("open holder: complete")

	return nil

}

func (h *Holder) sendOrSpool(msg Message) error {
	if h.maybeSpool(msg) {
		return nil
	}

	return h.broadcaster.SendSync(msg)
}

func (h *Holder) maybeSpool(msg Message) bool {
	h.startMsgsMu.Lock()
	defer h.startMsgsMu.Unlock()

	if h.startMsgs == nil {
		// Startup is done.
		return false
	}

	h.startMsgs = append(h.startMsgs, msg)
	return true
}

// Activate runs the background tasks relevant to keeping a holder in
// a stable state, such as flushing caches. This is separate from
// opening because, while a server would nearly always want to do
// this, other use cases (like consistency checks of a data directory)
// need to avoid it even getting started.
func (h *Holder) Activate() {
	// Periodically flush cache.
	h.wg.Add(1)
	go func() { defer h.wg.Done(); h.monitorCacheFlush() }()
}

// checkForeignIndex is a check before applying a foreign
// index to a field; if the index is not yet available,
// (because holder is still opening and may not have opened
// the index yet), this method queues it up to be processed
// once all indexes have been opened.
func (h *Holder) checkForeignIndex(f *Field) error {
	if h.opening {
		if fi := h.Index(f.options.ForeignIndex); fi == nil {
			h.foreignIndexFieldsMu.Lock()
			defer h.foreignIndexFieldsMu.Unlock()
			h.foreignIndexFields = append(h.foreignIndexFields, f)
			return nil
		}
	}
	return f.applyForeignIndex()
}

// processForeignIndexFields applies a foreign index to any
// fields which were opened before their foreign index.
func (h *Holder) processForeignIndexFields() error {
	for _, f := range h.foreignIndexFields {
		if err := f.applyForeignIndex(); err != nil {
			return errors.Wrap(err, "applying foreign index")
		}
	}
	h.foreignIndexFields = h.foreignIndexFields[:0] // reset
	return nil
}

// Close closes all open fragments.
func (h *Holder) Close() error {
	if h == nil {
		return nil
	}

	if globalUseStatTx {
		fmt.Printf("%v\n", globalCallStats.report())
	}

	h.Stats.Close()

	// Notify goroutines of closing and wait for completion.
	close(h.closing)
	h.wg.Wait()
	for _, index := range h.Indexes() {
		if err := index.Close(); err != nil {
			return errors.Wrap(err, "closing index")
		}
	}
	if err := h.txf.Close(); err != nil {
		return errors.Wrap(err, "holder.Txf.Close()")
	}
	if err := h.ida.Close(); err != nil {
		return errors.Wrap(err, "closing ID allocator")
	}

	// Reset opened in case Holder needs to be reopened.
	h.txf = nil
	h.opened.mu.Lock()
	h.opened.ch = make(chan struct{})
	h.opened.mu.Unlock()

	if h.lookupDB != nil {
		err := h.lookupDB.Close()
		if err != nil {
			return errors.Wrap(err, "closing DB")
		}
		h.lookupDB = nil
	}

	_ = testhook.Closed(h.Auditor, h, nil)

	return nil
}

// HasData returns true if Holder contains at least one index.
// This is used to determine if the rebalancing of data is necessary
// when a node joins the cluster.
func (h *Holder) HasData() (bool, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if len(h.Indexes()) > 0 {
		return true, nil
	}
	// Open path to read all index directories.
	if _, err := os.Stat(h.IndexesPath()); os.IsNotExist(err) {
		return false, nil
	} else if err != nil {
		return false, errors.Wrap(err, "statting data dir")
	}

	f, err := os.Open(h.IndexesPath())
	if err != nil {
		return false, errors.Wrap(err, "opening data dir")
	}
	defer f.Close()

	fis, err := f.Readdir(0)
	if err != nil {
		return false, errors.Wrap(err, "reading data dir")
	}

	for _, fi := range fis {
		if !fi.IsDir() {
			continue
		}
		return true, nil
	}
	return false, nil
}

// availableShardsByIndex returns a bitmap of all shards by indexes.
func (h *Holder) availableShardsByIndex() map[string]*roaring.Bitmap {
	m := make(map[string]*roaring.Bitmap)
	for _, index := range h.Indexes() {
		m[index.Name()] = index.AvailableShards(includeRemote)
	}
	return m
}

// Schema returns schema information for all indexes, fields, and views.
func (h *Holder) Schema() ([]*IndexInfo, error) {
	return h.schema(context.TODO(), true)
}

// limitedSchema returns schema information for all indexes and fields.
func (h *Holder) limitedSchema() ([]*IndexInfo, error) {
	return h.schema(context.TODO(), false)
}

func (h *Holder) schema(ctx context.Context, includeViews bool) ([]*IndexInfo, error) {
	schema, err := h.schemator.Schema(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "getting schema via schemator")
	}

	a := make([]*IndexInfo, 0, len(schema))

	for _, index := range schema {
		cim, err := decodeCreateIndexMessage(h.serializer, index.Data)
		if err != nil {
			return nil, errors.Wrap(err, "decoding CreateIndexMessage")
		}

		di := &IndexInfo{
			Name:       cim.Index,
			CreatedAt:  cim.CreatedAt,
			Options:    cim.Meta,
			ShardWidth: ShardWidth,
			Fields:     make([]*FieldInfo, 0, len(index.Fields)),
		}
		for fieldName, field := range index.Fields {
			if fieldName == existenceFieldName {
				continue
			}
			cfm, err := decodeCreateFieldMessage(h.serializer, field.Data)
			if err != nil {
				return nil, errors.Wrap(err, "decoding CreateFieldMessage")
			}
			fi := &FieldInfo{
				Name:      cfm.Field,
				CreatedAt: cfm.CreatedAt,
				Options:   *cfm.Meta,
			}
			if includeViews {
				for viewName := range field.Views {
					fi.Views = append(fi.Views, &ViewInfo{Name: viewName})
				}
				sort.Sort(viewInfoSlice(fi.Views))
			}
			di.Fields = append(di.Fields, fi)
		}
		sort.Sort(fieldInfoSlice(di.Fields))
		a = append(a, di)
	}
	sort.Sort(indexInfoSlice(a))
	return a, nil
}

// applySchema applies an internal Schema to Holder.
func (h *Holder) applySchema(schema *Schema) error {
	// Create indexes.
	// We use h.CreateIndex() instead of h.CreateIndexIfNotExists() because we
	// want to limit the use of this method for now to only new indexes.
	for _, i := range schema.Indexes {
		idx, err := h.CreateIndex(i.Name, i.Options)
		if err != nil {
			return errors.Wrap(err, "creating index")
		}

		// Create fields that don't exist.
		for _, f := range i.Fields {
			fld, err := idx.CreateFieldIfNotExistsWithOptions(f.Name, &f.Options)
			if err != nil {
				return errors.Wrap(err, "creating field")
			}

			// Create views that don't exist.
			for _, v := range f.Views {
				_, err := fld.createViewIfNotExists(v.Name)
				if err != nil {
					return errors.Wrap(err, "creating view")
				}
			}
		}
	}

	// Send the load schema message to all nodes.
	if err := h.sendOrSpool(&LoadSchemaMessage{}); err != nil {
		return errors.Wrap(err, "sending LoadSchemaMessage")
	}

	return nil
}

// IndexPath returns the path where a given index is stored.
func (h *Holder) IndexPath(name string) string {
	return filepath.Join(h.IndexesPath(), name)
}

// Index returns the index by name.
func (h *Holder) Index(name string) (idx *Index) {
	h.imu.RLock()
	idx = h.indexes[name]
	h.imu.RUnlock()
	return
}

// Indexes returns a list of all indexes in the holder.
func (h *Holder) Indexes() []*Index {
	h.imu.RLock()
	// sizing and copying has to be done under the lock to avoid
	// a logical race with a deletion/addition to indexes.
	cp := make([]*Index, 0, len(h.indexes))
	for _, idx := range h.indexes {
		cp = append(cp, idx)
	}
	h.imu.RUnlock()
	sort.Sort(indexSlice(cp))
	return cp
}

// CreateIndex creates an index.
// An error is returned if the index already exists.
func (h *Holder) CreateIndex(name string, opt IndexOptions) (*Index, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Ensure index doesn't already exist.
	if h.Index(name) != nil {
		return nil, newConflictError(ErrIndexExists)
	}

	cim := &CreateIndexMessage{
		Index:     name,
		CreatedAt: timestamp(),
		Meta:      opt,
	}

	// Create the index in etcd as the system of record.
	if err := h.persistIndex(context.Background(), cim); err != nil {
		return nil, errors.Wrap(err, "persisting index")
	}

	return h.createIndex(cim, false)
}

// LoadSchemaMessage is an internal message used to inform a node to load the
// latest schema from etcd.
type LoadSchemaMessage struct{}

// LoadSchema creates all indexes based on the information stored in schemator.
// It does not return an error if an index already exists. The thinking is that
// this method will load all indexes that don't already exist. We likely want to
// revisit this; for example, we might want to confirm that the createdAt
// timestamps on each of the indexes matches the value in etcd.
func (h *Holder) LoadSchema() error {
	h.mu.Lock()
	defer h.mu.Unlock()

	return h.loadSchema()
}

// LoadIndex creates an index based on the information stored in schemator.
// An error is returned if the index already exists.
func (h *Holder) LoadIndex(name string) (*Index, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Ensure index doesn't already exist.
	if h.Index(name) != nil {
		return nil, newConflictError(ErrIndexExists)
	}
	return h.loadIndex(name)
}

// LoadField creates a field based on the information stored in schemator.
// An error is returned if the field already exists.
func (h *Holder) LoadField(index, field string) (*Field, error) {
	// Ensure field doesn't already exist.
	if h.Field(index, field) != nil {
		return nil, newConflictError(ErrFieldExists)
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	return h.loadField(index, field)
}

// LoadView creates a view based on the information stored in schemator. Unlike
// index and field, it is not considered an error if the view already exists.
func (h *Holder) LoadView(index, field, view string) (*view, error) {
	// If the view already exists, just return with it here.
	if v := h.view(index, field, view); v != nil {
		return v, nil
	}

	return h.loadView(index, field, view)
}

// CreateIndexAndBroadcast creates an index locally, then broadcasts the
// creation to other nodes so they can create locally as well. An error is
// returned if the index already exists.
func (h *Holder) CreateIndexAndBroadcast(ctx context.Context, cim *CreateIndexMessage) (*Index, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Ensure index doesn't already exist.
	if h.Index(cim.Index) != nil {
		return nil, newConflictError(ErrIndexExists)
	}

	// Create the index in etcd as the system of record.
	if err := h.persistIndex(ctx, cim); err != nil {
		return nil, errors.Wrap(err, "persisting index")
	}

	return h.createIndex(cim, true)
}

// CreateIndexIfNotExists returns an index by name.
// The index is created if it does not already exist.
func (h *Holder) CreateIndexIfNotExists(name string, opt IndexOptions) (*Index, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	cim := &CreateIndexMessage{
		Index:     name,
		CreatedAt: timestamp(),
		Meta:      opt,
	}

	// Create the index in etcd as the system of record.
	err := h.persistIndex(context.Background(), cim)
	if err != nil && errors.Cause(err) != disco.ErrIndexExists {
		return nil, errors.Wrap(err, "persisting index")
	}

	if index := h.Index(name); index != nil {
		return index, nil
	}

	// It may happen that index is not in memory, but it's already in etcd,
	// then we need to create it locally.
	return h.createIndex(cim, false)
}

// persistIndex stores the index information in etcd.
func (h *Holder) persistIndex(ctx context.Context, cim *CreateIndexMessage) error {
	if cim.Index == "" {
		return ErrIndexRequired
	}

	if err := ValidateName(cim.Index); err != nil {
		return errors.Wrap(err, "validating name")
	}

	if b, err := h.serializer.Marshal(cim); err != nil {
		return errors.Wrap(err, "marshaling")
	} else if err := h.schemator.CreateIndex(ctx, cim.Index, b); err != nil {
		return errors.Wrapf(err, "writing index to disco: %s", cim.Index)
	}
	return nil
}

func (h *Holder) createIndex(cim *CreateIndexMessage, broadcast bool) (*Index, error) {
	if cim.Index == "" {
		return nil, errors.New("index name required")
	}

	// Otherwise create a new index.
	index, err := h.newIndex(h.IndexPath(cim.Index), cim.Index)
	if err != nil {
		return nil, errors.Wrap(err, "creating")
	}

	index.keys = cim.Meta.Keys
	index.trackExistence = cim.Meta.TrackExistence
	index.createdAt = cim.CreatedAt

	if err = index.Open(); err != nil {
		return nil, errors.Wrap(err, "opening")
	}

	// Update options.
	h.addIndex(index)

	if broadcast {
		// Send the create index message to all nodes.
		if err := h.broadcaster.SendSync(cim); err != nil {
			return nil, errors.Wrap(err, "sending CreateIndex message")
		}
	}

	// Since this is a new index, we need to kick off
	// its translation sync.
	if err := h.translationSyncer.Reset(); err != nil {
		return nil, errors.Wrap(err, "resetting translation sync")
	}

	return index, nil
}

func (h *Holder) loadSchema() error {
	schema, err := h.schemator.Schema(context.TODO())
	if err != nil {
		return errors.Wrap(err, "getting schema")
	}

	// TODO: This is kind of inefficient because we're ignoring the index.Data
	// and field.Data values, which contains the index and field information,
	// and only using the map key to call loadIndex() and loadField(). These
	// make another call to schemator to get the same index and field
	// information that we already have in the map. It probably makes sense to
	// either copy the parts of the loadIndex and loadField methods here (like
	// decodeCreateIndexMessage) or split loadIndex and loadField into smaller
	// methods that we could reuse here.
	for indexName, index := range schema {
		_, err := h.loadIndex(indexName)
		if err != nil {
			return errors.Wrap(err, "loading index")
		}
		for fieldName, field := range index.Fields {
			_, err := h.loadField(indexName, fieldName)
			if err != nil {
				return errors.Wrap(err, "loading field")
			}
			for viewName := range field.Views {
				_, err := h.loadView(indexName, fieldName, viewName)
				if err != nil {
					return errors.Wrap(err, "loading view")
				}
			}
		}
	}

	return nil
}

func (h *Holder) loadIndex(indexName string) (*Index, error) {
	b, err := h.schemator.Index(context.TODO(), indexName)
	if err != nil {
		return nil, errors.Wrapf(err, "getting index: %s", indexName)
	}

	cim, err := decodeCreateIndexMessage(h.serializer, b)
	if err != nil {
		return nil, errors.Wrap(err, "decoding CreateIndexMessage")
	}

	return h.createIndex(cim, false)
}

func (h *Holder) loadField(indexName, fieldName string) (*Field, error) {
	b, err := h.schemator.Field(context.TODO(), indexName, fieldName)
	if err != nil {
		return nil, errors.Wrapf(err, "getting field: %s/%s", indexName, fieldName)
	}

	// Get index.
	idx := h.Index(indexName)
	if idx == nil {
		return nil, errors.Errorf("local index not found: %s", indexName)
	}

	cfm, err := decodeCreateFieldMessage(h.serializer, b)
	if err != nil {
		return nil, errors.Wrap(err, "decoding CreateFieldMessage")
	}

	return idx.createFieldIfNotExists(cfm)
}

func (h *Holder) loadView(indexName, fieldName, viewName string) (*view, error) {
	b, err := h.schemator.View(context.Background(), indexName, fieldName, viewName)
	if err != nil {
		return nil, errors.Wrapf(err, "getting view: %s/%s/%s", indexName, fieldName, viewName)
	} else if !b {
		return nil, errors.Wrapf(err, "tried to load a nonexistent view: %s/%s/%s", indexName, fieldName, viewName)
	}

	// Get field.
	fld := h.Field(indexName, fieldName)
	if fld == nil {
		return nil, errors.Errorf("local field not found: %s/%s", indexName, fieldName)
	}

	return fld.createViewIfNotExists(viewName)
}

func (h *Holder) newIndex(path, name string) (*Index, error) {
	index, err := NewIndex(h, path, name)
	if err != nil {
		return nil, err
	}
	index.Stats = h.Stats.WithTags(fmt.Sprintf("index:%s", index.Name()))
	index.broadcaster = h.broadcaster
	index.serializer = h.serializer
	index.Schemator = h.schemator
	index.OpenTranslateStore = h.OpenTranslateStore
	index.translationSyncer = h.translationSyncer
	return index, nil
}

// DeleteIndex removes an index from the holder.
func (h *Holder) DeleteIndex(name string) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Confirm index exists.
	index := h.Index(name)
	if index == nil {
		return newNotFoundError(ErrIndexNotFound, name)
	}

	// Close index.
	if err := index.Close(); err != nil {
		return errors.Wrap(err, "closing")
	}

	// remove any backing store.
	if err := h.txf.DeleteIndex(name); err != nil {
		return errors.Wrap(err, "index.Txf.DeleteIndex")
	}

	// Delete index directory.
	if err := os.RemoveAll(h.IndexPath(name)); err != nil {
		return errors.Wrap(err, "removing directory")
	}

	// Remove reference.
	h.deleteIndex(name)

	// Delete the index from etcd as the system of record.
	if err := h.schemator.DeleteIndex(context.TODO(), name); err != nil {
		return errors.Wrapf(err, "deleting index from etcd: %s", name)
	}

	// I'm not sure if calling Reset() here is necessary
	// since closing the index stops its translation
	// sync processes.
	return h.translationSyncer.Reset()
}

func (h *Holder) deleteIndex(index string) {
	h.imu.Lock()
	delete(h.indexes, index)
	h.imu.Unlock()
}

// Field returns the field for an index and name.
func (h *Holder) Field(index, name string) *Field {
	idx := h.Index(index)
	if idx == nil {
		return nil
	}
	return idx.Field(name)
}

// view returns the view for an index, field, and name.
func (h *Holder) view(index, field, name string) *view {
	f := h.Field(index, field)
	if f == nil {
		return nil
	}
	return f.view(name)
}

// fragment returns the fragment for an index, field & shard.
func (h *Holder) fragment(index, field, view string, shard uint64) *fragment {
	v := h.view(index, field, view)
	if v == nil {
		return nil
	}
	return v.Fragment(shard)
}

// monitorCacheFlush periodically flushes all fragment caches sequentially.
// This is run in a goroutine.
func (h *Holder) monitorCacheFlush() {
	ticker := time.NewTicker(h.cacheFlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-h.closing:
			return
		case <-ticker.C:
			h.flushCaches()
		}
	}
}

func (h *Holder) flushCaches() {
	for _, index := range h.Indexes() {
		for _, field := range index.Fields() {
			for _, view := range field.views() {
				for _, fragment := range view.allFragments() {
					select {
					case <-h.closing:
						return
					default:
					}

					if err := fragment.FlushCache(); err != nil {
						h.Logger.Errorf("flushing cache: err=%s, path=%s", err, fragment.cachePath())
					}
				}
			}
		}
	}
}

// recalculateCaches recalculates caches on every index in the holder. This is
// probably not practical to call in real-world workloads, but makes writing
// integration tests much eaiser, since one doesn't have to wait 10 seconds
// after setting bits to get expected response.
// This is mostly unnecessary now, as caches will automatically recalculate on read.
// However, a user may explicitly request calculation, in which case we should not defer it.
func (h *Holder) recalculateCaches() {
	for _, index := range h.Indexes() {
		index.recalculateCaches()
	}
}

// Log startup time and version to $DATA_DIR/.startup.log
func (h *Holder) logStartup() error {
	RFC3339NanoFixedWidth := "2006-01-02T15:04:05.000000 07:00"
	time := time.Now().Format(RFC3339NanoFixedWidth)
	logLine := fmt.Sprintf("%s\t%s\n", time, Version)

	if err := os.MkdirAll(h.path, 0777); err != nil {
		return errors.Wrap(err, "creating data directory")
	}

	f, err := os.OpenFile(h.path+"/startup.log", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return errors.Wrap(err, "opening startup log")
	}

	defer f.Close()

	if _, err = f.WriteString(logLine); err != nil {
		return errors.Wrap(err, "writing startup log")
	}

	return nil
}

// holderSyncer is an active anti-entropy tool that compares the local holder
// with a remote holder based on block checksums and resolves differences.
type holderSyncer struct {
	mu sync.Mutex

	Holder *Holder

	Node    *topology.Node
	Cluster *cluster

	// Translation sync handling.
	readers                     []TranslateEntryReader
	readersMu                   sync.Mutex
	pendingReaders              int
	stopInitializeReplicationCh chan struct{}

	syncers errgroup.Group

	// Stats
	Stats stats.StatsClient

	// Signals that the sync should stop.
	Closing <-chan struct{}
}

// IsClosing returns true if the syncer has been asked to close.
func (s *holderSyncer) IsClosing() bool {
	if s.Cluster.abortAntiEntropyQ() {
		return true
	}
	select {
	case <-s.Closing:
		return true
	default:
		return false
	}
}

// SyncHolder compares the holder on host with the local holder and resolves differences.
func (s *holderSyncer) SyncHolder() error {
	s.mu.Lock() // only allow one instance of SyncHolder to be running at a time
	defer s.mu.Unlock()
	ti := time.Now()

	// Create a snapshot of the cluster to use for node/partition calculations.
	snap := topology.NewClusterSnapshot(s.Cluster.noder, s.Cluster.Hasher, s.Cluster.ReplicaN)

	schema, err := s.Holder.Schema()
	if err != nil {
		return errors.Wrap(err, "getting schema")
	}

	// Iterate over schema in sorted order.
	for _, di := range schema {
		// Verify syncer has not closed.
		if s.IsClosing() {
			return nil
		}

		tf := time.Now()
		for _, fi := range di.Fields {
			// Verify syncer has not closed.
			if s.IsClosing() {
				return nil
			}

			for _, vi := range fi.Views {
				// Verify syncer has not closed.
				if s.IsClosing() {
					return nil
				}

				itr := s.Holder.Index(di.Name).AvailableShards(includeRemote).Iterator()
				itr.Seek(0)
				for shard, eof := itr.Next(); !eof; shard, eof = itr.Next() {
					// Ignore shards that this host doesn't own.
					if !snap.OwnsShard(s.Node.ID, di.Name, shard) {
						continue
					}

					// Verify syncer has not closed.
					if s.IsClosing() {
						return nil
					}

					// Sync fragment if own it.
					if err := s.syncFragment(di.Name, fi.Name, vi.Name, shard); err != nil {
						return fmt.Errorf("fragment sync error: index=%s, field=%s, view=%s, shard=%d, err=%s", di.Name, fi.Name, vi.Name, shard, err)
					}
				}
			}
			s.Stats.Timing(MetricSyncFieldDurationSeconds, time.Since(tf), 1.0)
			tf = time.Now() // reset tf
		}
		s.Stats.Timing(MetricSyncIndexDurationSeconds, time.Since(ti), 1.0)
		ti = time.Now() // reset ti
	}

	return nil
}

// syncFragment synchronizes a fragment with the rest of the cluster.
func (s *holderSyncer) syncFragment(index, field, view string, shard uint64) error {
	// Retrieve local field.
	f := s.Holder.Field(index, field)
	if f == nil {
		return newNotFoundError(ErrFieldNotFound, field)
	}

	// Ensure view exists locally.
	v, err := f.createViewIfNotExists(view)
	if err != nil {
		return errors.Wrap(err, "creating view")
	}

	// Ensure fragment exists locally.
	frag, err := v.CreateFragmentIfNotExists(shard)
	if err != nil {
		return errors.Wrap(err, "creating fragment")
	}

	// Sync fragments together.
	fs := fragmentSyncer{
		Fragment:  frag,
		Node:      s.Node,
		Cluster:   s.Cluster,
		FieldType: f.Type(),
		Closing:   s.Closing,
	}
	if err := fs.syncFragment(); err != nil {
		return errors.Wrap(err, "syncing fragment")
	}

	return nil
}

// resetTranslationSync reinitializes streaming sync of translation data.
func (s *holderSyncer) resetTranslationSync() error {
	if s.stopInitializeReplicationCh == nil {
		// suppose stopTranslationSync[S] holds the lock s.readersMu and tries
		// to send a signal to s.stopInitializeRepliationCh. If
		// s.stopInitializeReplicationCh is unbufferd and
		// s.initializeReplication[I] is trying to acquire s.readersMu, this
		// will result in a deadlock.
		// Hence, the channel is buffered to prevent this scenario. Once
		// [S] releases the lock, [I] will acquire it, however, the value
		// of s.pendingReaders will be -1, at this point [I] should not
		// attempt to add any more readers since those readers will be 'stale'
		// [I] should also drain the channel to prevent the next invocation
		// of [I] receiving a stop signal that was meant for the current one
		// This is needlessly complicated and is the result of me running
		// into various deadlocks while trying to fix handling of column
		// key replication.
		s.stopInitializeReplicationCh = make(chan struct{})
	}
	// Stop existing streams.
	if err := s.stopTranslationSync(); err != nil {
		return errors.Wrap(err, "stop translation sync")
	}

	// Create a snapshot of the cluster to use for node/partition calculations.
	snap := topology.NewClusterSnapshot(s.Cluster.noder, s.Cluster.Hasher, s.Cluster.ReplicaN)

	// Set read-only flag for all translation stores.
	s.setTranslateReadOnlyFlags(snap)

	if err := s.initializeReplication(snap); err != nil {
		return errors.Wrap(err, "initializing translation replication")
	}
	return nil

}

////////////////////////////////////////////////////////////

// TranslationSyncer provides an interface allowing a function
// to notify the server that an action has occurred which requires
// the translation sync process to be reset. In general, this
// includes anything which modifies schema (add/remove index, etc),
// or anything that changes the cluster topology (add/remove node).
// I originally considered leveraging the broadcaster since that was
// already in place and provides similar event messages, but the
// broadcaster is really meant for notifiying other nodes, while
// this is more akin to an internal message bus. In fact, I think
// a future iteration on this may be to make it more generic so
// it can act as an internal message bus where one of the messages
// being published is "translationSyncReset".
type TranslationSyncer interface {
	Reset() error
}

// NopTranslationSyncer represents a translationSyncer that doesn't do anything.
var NopTranslationSyncer TranslationSyncer = &nopTranslationSyncer{}

type nopTranslationSyncer struct{}

// Reset is a no-op implementation of translationSyncer Reset method.
func (nopTranslationSyncer) Reset() error { return nil }

// activeTranslationSyncer represents a translationSyncer that resets
// the server's translation syncer.
type activeTranslationSyncer struct {
	ch chan struct{}
}

// newActiveTranslationSyncer returns a new instance of activeTranslationSyncer.
func newActiveTranslationSyncer(ch chan struct{}) *activeTranslationSyncer {
	return &activeTranslationSyncer{
		ch: ch,
	}
}

// Reset resets the server's translation syncer.
func (a *activeTranslationSyncer) Reset() error {
	// just in case some other part of the code has fired
	// off a translation sync and it hasn't been received yet
	// therefore we don't want to block on send since a.ch is
	// (for now) unbuffered. One translationSync is as good as
	// another
	select {
	case a.ch <- struct{}{}:
	default:
	}
	return nil
}

////////////////////////////////////////////////////////////

// stopTranslationSync closes and waits for all outstanding translation readers
// to complete. This should be called before reconnecting to the cluster in case
// of a cluster resize or schema change.
func (s *holderSyncer) stopTranslationSync() error {
	s.readersMu.Lock()
	defer func() {
		s.readers = nil // will be populated by initializeReplication
		s.readersMu.Unlock()
	}()
	// send signal to stop initializing more readers
	if s.pendingReaders > 0 {
		s.pendingReaders = -1
		close(s.stopInitializeReplicationCh)
		s.stopInitializeReplicationCh = make(chan struct{})
	}
	var g errgroup.Group
	for i := range s.readers {
		rd := s.readers[i]
		g.Go(func() error {
			return rd.Close()
		})
	}
	g.Go(s.syncers.Wait)
	return g.Wait()
}

// setTranslateReadOnlyFlags updates all translation stores to enable or disable
// writing new translation keys. Index stores are writable if the node owns the
// partition. Field stores are writable if the node is the primary.
func (s *holderSyncer) setTranslateReadOnlyFlags(snap *topology.ClusterSnapshot) {
	s.Cluster.mu.RLock()
	isPrimaryFieldTranslator := snap.IsPrimaryFieldTranslationNode(s.Cluster.Node.ID)

	for _, index := range s.Holder.Indexes() {
		// There is a race condition here:
		// if Indexes() returns idx1, and then in another
		// process, holder.DeleteIndex(idx1) is called,
		// then the next step trying to get TranslateStore(partitionID)
		// for an index that is closed (and therefore its transateStores
		// no longer exist) will fail with a nil pointer error.
		// For now, I just checked that the translateStore hasn't been
		// set to nil before trying to use it, but another option may
		// be to prevent the translateStores from being zeroed out
		// while this process is active. Checking for nil as we do
		// really obviates the need for the RLock around the for loop.

		// Obtain a read lock on index to prevent Index.Close() from
		// destroying the Index.translateStores map before this is
		// done using it.
		//
		// Update: there was another path down to Index.Close(), so
		// we shrink to lock to be inside index.TranslateStore() now.
		for partitionID := 0; partitionID < snap.PartitionN; partitionID++ {
			primary := snap.PrimaryPartitionNode(partitionID)
			isPrimary := primary != nil && s.Node.ID == primary.ID

			if ts := index.TranslateStore(partitionID); ts != nil {
				ts.SetReadOnly(!isPrimary)
			}
		}

		for _, field := range index.Fields() {
			field.TranslateStore().SetReadOnly(!isPrimaryFieldTranslator)
		}
	}
	s.Cluster.mu.RUnlock()
}

// initializeReplication builds a map of nodes for which we need to replicate
// any key translation, whether that's field keys (every node replicates these
// from the primary) or index keys (only the replica nodes for each partition
// replicate these from whichever node is primary for that partition).
func (s *holderSyncer) initializeReplication(snap *topology.ClusterSnapshot) error {
	nodeMaps := make(map[string]TranslateOffsetMap)
	if snap.ReplicaN > 1 {
		if err := s.populateIndexReplication(nodeMaps, snap); err != nil {
			return err
		}
	}
	if err := s.populateFieldReplication(nodeMaps, snap); err != nil {
		return err
	}

	// filter out empty nodes
	nodes := make(map[*topology.Node]bool)
	for _, node := range snap.Nodes {
		m := nodeMaps[node.ID]
		if !m.Empty() {
			nodes[node] = true
		}
	}

	// connect to remote nodes and set up readers
	readersCh := make(chan TranslateEntryReader, len(nodes))
	ctx, cancelAddingMoreReaders := context.WithCancel(context.Background())
	defer cancelAddingMoreReaders()
	s.readersMu.Lock()
	s.pendingReaders = len(nodes)
	s.readersMu.Unlock()
	go func() {
		for {
			for node := range nodes {
				// check if ctx cancelled
				// this means there was a signal sent to stop further init
				// of readers
				select {
				case <-s.Closing:
					return
				case <-ctx.Done():
					close(readersCh)
					return
				default:
				}
				// connect to remote node
				m := nodeMaps[node.ID]
				rd, err := s.Holder.OpenTranslateReader(context.Background(), node.URI.String(), m)
				if err != nil {
					continue
				}
				readersCh <- rd
				delete(nodes, node)
			}
			if len(nodes) == 0 {
				close(readersCh)
				return
			}
			time.Sleep(10 * time.Second)

		}
	}()

	for {
		select {
		case <-s.Closing:
			cancelAddingMoreReaders()
			return nil
		case <-s.stopInitializeReplicationCh:
			return nil
		case rd, ok := <-readersCh:
			// all translate readers have been launched, hence channel is
			// closed
			if !ok {
				return nil
			}
			s.readersMu.Lock()
			// [S] has been initiated and acquired the lock first
			// at this point we should close the reader we've recieved rather
			// than start replication on it.
			// [S] should have already closed all the rest
			// of the reads if they were still in action.
			// we are also draining the channel since the signal for stopping
			// further replication was meant for us
			if s.pendingReaders == -1 {
				rd.Close()
				cancelAddingMoreReaders()
			drain:
				for {
					select {
					case <-s.stopInitializeReplicationCh:
					default:
						break drain
					}
				}
				s.readersMu.Unlock()
				return nil
			}
			s.pendingReaders--
			s.readers = append(s.readers, rd)
			s.syncers.Go(func() error {
				defer rd.Close()
				s.readBothTranslateReader(rd, snap)
				return nil
			})
			s.readersMu.Unlock()
		}
	}
}

// populateFieldReplication populates a map from node IDs to TranslateOffsetMaps
// to record that we need to translate fields which have key translation
// from the primary node.
func (s *holderSyncer) populateFieldReplication(nodeMaps map[string]TranslateOffsetMap, snap *topology.ClusterSnapshot) error {
	// Set up field translation
	if !snap.IsPrimaryFieldTranslationNode(s.Cluster.Node.ID) {
		primaryID := snap.PrimaryFieldTranslationNode().ID
		// Build a map of field key offsets to stream from.
		m := nodeMaps[primaryID]
		if m == nil {
			m = make(TranslateOffsetMap)
			nodeMaps[primaryID] = m
		}
		for _, index := range s.Holder.Indexes() {
			for _, field := range index.Fields() {
				store := field.TranslateStore()
				// I think right now this is supposed to be impossible;
				// we use an InMemTranslateStore by default even if
				// no translate store is being used or attempted.
				if store == nil {
					return fmt.Errorf("no translate store for field %q/%q", index.Name(), field.Name())
				}
				offset, err := store.MaxID()
				if err != nil {
					return errors.Wrapf(err, "cannot determine max id for %q/%q", index.Name(), field.Name())
				}
				m.SetFieldOffset(index.Name(), field.Name(), offset)
			}
		}
	}
	return nil
}

// populateIndexReplication populates a map of node IDs to TranslateOffsetMaps
// to record which nodes we need to replicate index key translation for.
// That means nodes which are the primary for a partition that we're a
// non-primary replica for.
func (s *holderSyncer) populateIndexReplication(nodeMaps map[string]TranslateOffsetMap, snap *topology.ClusterSnapshot) error {
	for _, node := range snap.Nodes {
		if node.ID == s.Node.ID {
			continue
		}

		// Build a map of partition offsets to stream from.
		m := make(TranslateOffsetMap)
		for _, index := range s.Holder.Indexes() {
			if !index.Keys() {
				continue
			}
			for partitionID := 0; partitionID < snap.PartitionN; partitionID++ {
				partitionNodes := snap.PartitionNodes(partitionID)
				isPrimary := partitionNodes[0].ID == node.ID                          // remote is primary?
				isReplica := topology.Nodes(partitionNodes[1:]).ContainsID(s.Node.ID) // local is replica?
				if !isPrimary || !isReplica {
					continue
				}

				store := index.TranslateStore(partitionID)
				if store == nil {
					return fmt.Errorf("no store available for index %q, partition %d", index.Name(), partitionID)
				}
				offset, err := store.MaxID()
				if err != nil {
					return errors.Wrapf(err, "cannot determine max id for %q", index.Name())
				}
				m.SetIndexPartitionOffset(index.Name(), partitionID, offset)
			}
		}

		// Skip if no replication required.
		if len(m) == 0 {
			continue
		}
		nodeMaps[node.ID] = m
	}
	return nil
}

// readBothTranslateReader reads key translation for field keys or
// index keys from a remote node. Both field and index keys may be sent,
// the distinction is that field keys have a non-empty field name.
func (s *holderSyncer) readBothTranslateReader(rd TranslateEntryReader, snap *topology.ClusterSnapshot) {
	for {
		var entry TranslateEntry
		if err := rd.ReadEntry(&entry); err != nil {
			s.Holder.Logger.Errorf("cannot read translate entry: %s", err)
			return
		}

		var store TranslateStore
		if entry.Field != "" {
			// Find appropriate store.
			f := s.Holder.Field(entry.Index, entry.Field)
			if f == nil {
				s.Holder.Logger.Errorf("field not found: %s/%s", entry.Index, entry.Field)
				return
			}
			store = f.TranslateStore()
			if store == nil {
				s.Holder.Logger.Errorf("no translate store suitable for index %q, field %q, key %q", entry.Index, entry.Field, entry.Key)
				return
			}
		} else {
			// Find appropriate store.
			idx := s.Holder.Index(entry.Index)
			if idx == nil {
				s.Holder.Logger.Errorf("index not found: %q", entry.Index)
				return
			}
			store = idx.TranslateStore(snap.KeyToKeyPartition(entry.Index, entry.Key))
			if store == nil {
				s.Holder.Logger.Errorf("no translate store suitable for index %q, key %q", entry.Index, entry.Key)
				return
			}
		}
		// Apply replication to store.
		if err := store.ForceSet(entry.ID, entry.Key); err != nil {
			s.Holder.Logger.Errorf("cannot force set field translation data: %d=%q", entry.ID, entry.Key)
			return
		}
	}
}

func uint64InSlice(i uint64, s []uint64) bool {
	for _, o := range s {
		if i == o {
			return true
		}
	}
	return false
}

// used by Index.openFields(), enabling Tx / Txf by telling
// the holder about its own indexes.
func (h *Holder) addIndex(idx *Index) {
	h.imu.Lock()
	h.indexes[idx.name] = idx
	h.imu.Unlock()
}

func (h *Holder) Txf() *TxFactory {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.txf
}

// BeginTx starts a transaction on the holder. The index and shard
// must be specified.
func (h *Holder) BeginTx(writable bool, idx *Index, shard uint64) (Tx, error) {
	return h.txf.NewTx(Txo{Write: writable, Index: idx, Shard: shard}), nil
}

func decodeCreateIndexMessage(ser Serializer, b []byte) (*CreateIndexMessage, error) {
	var cim CreateIndexMessage
	if err := ser.Unmarshal(b, &cim); err != nil {
		return nil, errors.Wrap(err, "unmarshaling")
	}
	return &cim, nil
}

func decodeCreateFieldMessage(ser Serializer, b []byte) (*CreateFieldMessage, error) {
	var cfm CreateFieldMessage
	if err := ser.Unmarshal(b, &cfm); err != nil {
		return nil, errors.Wrap(err, "unmarshaling")
	}
	return &cfm, nil
}
