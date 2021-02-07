// Copyright 2017 Pilosa Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pilosa

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/pilosa/pilosa/v2/logger"
	rbfcfg "github.com/pilosa/pilosa/v2/rbf/cfg"
	"github.com/pilosa/pilosa/v2/roaring"
	"github.com/pilosa/pilosa/v2/stats"
	"github.com/pilosa/pilosa/v2/storage"
	"github.com/pilosa/pilosa/v2/testhook"
	"github.com/pilosa/pilosa/v2/topology"
	"github.com/pilosa/pilosa/v2/tracing"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
	"golang.org/x/sync/errgroup"
)

const (
	// defaultCacheFlushInterval is the default value for Fragment.CacheFlushInterval.
	defaultCacheFlushInterval = 1 * time.Minute

	// fileLimit is the maximum open file limit (ulimit -n) to automatically set.
	fileLimit = 262144 // (512^2)

	// existenceFieldName is the name of the internal field used to store existence values.
	existenceFieldName = "_exists"

	// DefaultDiscoDir is the default data directory used by the disco implementation.
	DefaultDiscoDir = ".disco"
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

	NewAttrStore func(string) AttrStore

	// Close management
	wg      sync.WaitGroup
	closing chan struct{}

	// Stats
	Stats stats.StatsClient

	// Data directory path.
	path string

	// The interval at which the cached row ids are persisted to disk.
	cacheFlushInterval time.Duration

	Logger        logger.Logger
	SnapshotQueue SnapshotQueue

	// Instantiates new translation stores
	OpenTranslateStore  OpenTranslateStoreFunc
	OpenTranslateReader OpenTranslateReaderFunc

	// Func to open whatever implementation of transaction store we're using.
	OpenTransactionStore OpenTransactionStoreFunc

	// Func to open the ID allocator.
	OpenIDAllocator func(string) (*idAllocator, error)

	// transactionManager
	transactionManager *TransactionManager

	translationSyncer TranslationSyncer

	ida *idAllocator

	// Queue of fields (having a foreign index) which have
	// opened before their foreign index has opened.
	foreignIndexFields []*Field

	// opening is set to true while Holder is opening.
	// It's used to determine if foreign index application
	// needs to be queued and completed after all indexes
	// have opened.
	opening bool

	Opts HolderOpts

	Auditor testhook.Auditor

	txf *TxFactory

	// a separate lock out for indexes, to avoid the deadlock/race dilema
	// on holding mu.
	imu     sync.RWMutex
	indexes map[string]*Index
}

// HolderOpts holds information about the holder which other things might want
// to look up later while using the holder.
type HolderOpts struct {
	// ReadOnly indicates that this holder's contents should not produce
	// disk writes under any circumstances. It must be set before Open
	// is called, and changing it is not supported.
	ReadOnly bool
	// If Inspect is set, we'll try to obtain additional information
	// about fragments when opening them.
	Inspect bool

	// StorageBackend controls the tx/storage engine we instatiate. Set by
	// server.go OptServerStorageConfig
	StorageBackend string

	// RowcacheOn, if true, turns on the row cache for all storage backends.
	RowcacheOn bool
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
	CacheFlushInterval   time.Duration
	StatsClient          stats.StatsClient
	NewAttrStore         func(string) AttrStore
	Logger               logger.Logger
	RowcacheOn           bool

	StorageConfig       *storage.Config
	RBFConfig           *rbfcfg.Config
	AntiEntropyInterval time.Duration
}

func DefaultHolderConfig() *HolderConfig {
	return &HolderConfig{
		PartitionN:           topology.DefaultPartitionN,
		OpenTranslateStore:   OpenInMemTranslateStore,
		OpenTranslateReader:  nil,
		OpenTransactionStore: OpenInMemTransactionStore,
		OpenIDAllocator:      func(string) (*idAllocator, error) { return &idAllocator{}, nil },
		TranslationSyncer:    NopTranslationSyncer,
		CacheFlushInterval:   defaultCacheFlushInterval,
		StatsClient:          stats.NopStatsClient,
		NewAttrStore:         newNopAttrStore,
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
		NewAttrStore:         cfg.NewAttrStore,
		cacheFlushInterval:   cfg.CacheFlushInterval,
		OpenTranslateStore:   cfg.OpenTranslateStore,
		OpenTranslateReader:  cfg.OpenTranslateReader,
		OpenTransactionStore: cfg.OpenTransactionStore,
		OpenIDAllocator:      cfg.OpenIDAllocator,
		translationSyncer:    cfg.TranslationSyncer,
		Logger:               cfg.Logger,
		Opts:                 HolderOpts{StorageBackend: cfg.StorageConfig.Backend, RowcacheOn: cfg.RowcacheOn},

		SnapshotQueue: defaultSnapshotQueue,

		Auditor: NewAuditor(),

		path: path,

		indexes: make(map[string]*Index),
	}

	storage.SetRowCacheOn(cfg.RowcacheOn)

	txf, err := NewTxFactory(cfg.StorageConfig.Backend, path, h)
	panicOn(err)
	h.txf = txf
	h.txf.blueGreenOffIfRunningBlueGreen()

	_ = testhook.Created(h.Auditor, h, nil)
	return h
}

// Path() returns the path directory the holder was created with.
func (h *Holder) Path() string {
	return h.path
}

type HolderInfo struct {
	FragmentInfo  map[string]FragmentInfo
	FragmentNames []string
}

type regexpList []*regexp.Regexp

func newRegexpList(regexes string) (results regexpList, err error) {
	if regexes == "" {
		return nil, nil
	}
	for _, sub := range strings.Split(regexes, ",") {
		re, err := regexp.Compile(sub)
		if err != nil {
			return nil, err
		}
		results = append(results, re)
	}
	return results, nil
}

func (rl regexpList) Match(haystack string) bool {
	if rl == nil {
		return true
	}
	for _, re := range rl {
		if re.MatchString(haystack) {
			return true
		}
	}
	return false
}

// shardRange represents a series of shards
type shardRange struct {
	min, max uint64
}

type shardRangeList []shardRange

func newShardRangeList(shards string) (results shardRangeList, err error) {
	if shards == "" {
		return nil, nil
	}
	for _, sub := range strings.Split(shards, ",") {
		var sr shardRange
		minMax := strings.Split(sub, "-")
		if len(minMax) > 2 {
			return nil, fmt.Errorf("invalid range %q", sub)
		}
		sr.min, err = strconv.ParseUint(minMax[0], 10, 64)
		if err != nil {
			return nil, err
		}
		sr.max = sr.min
		if len(minMax) == 2 {
			sr.max, err = strconv.ParseUint(minMax[0], 10, 64)
			if err != nil {
				return nil, err
			}
		}
		if sr.max < sr.min {
			return nil, fmt.Errorf("invalid range %q: max < min", sub)
		}
		results = append(results, sr)
	}
	return results, nil
}

func (sl shardRangeList) Match(shard uint64) bool {
	if sl == nil {
		return true
	}
	for _, sr := range sl {
		if shard >= sr.min && shard <= sr.max {
			return true
		}
	}
	return false
}

// HolderFilter represents something that potentially filters out
// parts of a holder, indicating whether or not to process them,
// or recurse into them. It is permissible to recurse a thing
// without processing it, or process it without recursing it.
// For instance, something looking to accumulate statistics
// about views might return (true, false) from CheckView,
// while a fragment scanning operation would return (false, true)
// from everything above CheckFrag.
type HolderFilter interface {
	CheckIndex(iname string) (process bool, recurse bool)
	CheckField(iname, fname string) (process bool, recurse bool)
	CheckView(iname, fname, vname string) (process bool, recurse bool)
	CheckFragment(iname, fname, vname string, shard uint64) (process bool)
}

// HolderFilterAll is a placeholder type which always returns true for the
// check functions. You can embed it to make a HolderOperator which processes
// everything.
type HolderFilterAll struct{}

func (HolderFilterAll) CheckIndex(string) (bool, bool) {
	return true, true
}

func (HolderFilterAll) CheckField(string, string) (bool, bool) {
	return true, true
}

func (HolderFilterAll) CheckView(string, string, string) (bool, bool) {
	return true, true
}

func (HolderFilterAll) CheckFragment(string, string, string, uint64) bool {
	return true
}

// HolderProcessNone is a placeholder type which does nothing for the
// process functions. You can embed it to make a HolderOperator which
// does nothing, or embed it and provide your own ProcessFragment to
// do just that.
type HolderProcessNone struct{}

func (HolderProcessNone) ProcessIndex(*Index) error {
	return nil
}

func (HolderProcessNone) ProcessField(*Field) error {
	return nil
}

func (HolderProcessNone) ProcessView(*view) error {
	return nil
}

func (HolderProcessNone) ProcessFragment(*fragment) error {
	return nil
}

// HolderProcess represents something that has operations which can be
// performed on indexes, fields, views, and/or fragments.
type HolderProcess interface {
	ProcessIndex(*Index) error
	ProcessField(*Field) error
	ProcessView(*view) error
	ProcessFragment(*fragment) error
}

// HolderOperator is both a filter and a process. This is the general
// form of "I want to do something to some part of a holder."
type HolderOperator interface {
	HolderFilter
	HolderProcess
}

var _ HolderOperator = (*holderInspector)(nil)

type HolderFilterParams struct {
	Indexes string
	Fields  string
	Views   string
	Shards  string
}

type holderFilterFull struct {
	HolderFilterParams
	indexRegexps regexpList
	fieldRegexps regexpList
	viewRegexps  regexpList
	shardRanges  shardRangeList
}

type inspectRequestFull struct {
	HolderFilter
	params InspectRequestParams
}

func (i *holderFilterFull) CheckIndex(iname string) (process, recurse bool) {
	return true, i.indexRegexps.Match(iname)
}

func (i *holderFilterFull) CheckField(iname, fname string) (process, recurse bool) {
	return true, i.fieldRegexps.Match(fname)
}

func (i *holderFilterFull) CheckView(iname, fname, vname string) (process, recurse bool) {
	return true, i.viewRegexps.Match(vname)
}

func (i *holderFilterFull) CheckFragment(iname, fname, vname string, shard uint64) (process bool) {
	return i.shardRanges.Match(shard)
}

func NewHolderFilter(params HolderFilterParams) (result HolderFilter, err error) {
	filter := &holderFilterFull{
		HolderFilterParams: params,
	}
	filter.indexRegexps, err = newRegexpList(params.Indexes)
	if err != nil {
		return nil, err
	}
	filter.fieldRegexps, err = newRegexpList(params.Fields)
	if err != nil {
		return nil, err
	}
	filter.viewRegexps, err = newRegexpList(params.Views)
	if err != nil {
		return nil, err
	}
	filter.shardRanges, err = newShardRangeList(params.Shards)
	if err != nil {
		return nil, err
	}
	return filter, nil
}

func expandInspectRequest(req *InspectRequest) (*inspectRequestFull, error) {
	filter, err := NewHolderFilter(req.HolderFilterParams)
	if err != nil {
		return nil, err
	}
	irf := &inspectRequestFull{
		HolderFilter: filter,
		params:       req.InspectRequestParams,
	}
	return irf, nil
}

type holderInspector struct {
	*inspectRequestFull
	pathParts [3]string
	path      string
	hi        *HolderInfo
}

func (h *holderInspector) ProcessIndex(i *Index) error {
	h.pathParts[0] = i.name
	return nil
}

func (h *holderInspector) ProcessField(f *Field) error {
	h.pathParts[1] = f.name
	return nil
}

func (h *holderInspector) ProcessView(v *view) error {
	h.pathParts[2] = v.name
	h.path = strings.Join(h.pathParts[:], "/")
	return nil
}

func (h *holderInspector) ProcessFragment(f *fragment) error {
	path := h.path + "/" + strconv.FormatUint(f.shard, 10)
	h.hi.FragmentInfo[path] = f.inspect(h.inspectRequestFull.params)
	h.hi.FragmentNames = append(h.hi.FragmentNames, path)
	return nil
}

func (h *Holder) Inspect(ctx context.Context, req *InspectRequest) (*HolderInfo, error) {
	fullReq, err := expandInspectRequest(req)
	if err != nil {
		return nil, err
	}
	inspector := &holderInspector{
		inspectRequestFull: fullReq,
		hi: &HolderInfo{
			FragmentInfo: make(map[string]FragmentInfo),
		},
	}
	err = h.Process(ctx, inspector)
	sort.Strings(inspector.hi.FragmentNames)
	return inspector.hi, err
}

// Open initializes the root data directory for the holder.
func (h *Holder) Open() error {

	h.opening = true
	defer func() { h.opening = false }()

	if h.txf == nil {
		txf, err := NewTxFactory(h.cfg.StorageConfig.Backend, h.path, h)
		if err != nil {
			return errors.Wrap(err, "Holder.Open NewTxFactory()")
		}
		h.txf = txf
	}

	h.txf.blueGreenOffIfRunningBlueGreen()

	// Reset closing in case Holder is being reopened.
	h.closing = make(chan struct{})

	h.setFileLimit()

	h.Logger.Printf("open holder path: %s", h.path)
	if err := os.MkdirAll(h.path, 0777); err != nil {
		return errors.Wrap(err, "creating directory")
	}

	// Verify that we are not trying to open with v1 translation data.
	if ok, err := h.hasV1TranslateKeysFile(); err != nil {
		return errors.Wrap(err, "verify v1 translation file")
	} else if !ok {
		return ErrCannotOpenV1TranslateFile
	}

	tstore, err := h.OpenTransactionStore(h.path)
	if err != nil {
		return errors.Wrap(err, "opening transaction store")
	}
	h.transactionManager = NewTransactionManager(tstore)
	h.transactionManager.Log = h.Logger

	// Open ID allocator.
	h.ida, err = h.OpenIDAllocator(filepath.Join(h.path, "idalloc.db"))
	if err != nil {
		return errors.Wrap(err, "opening ID allocator")
	}

	// Open path to read all index directories.
	f, err := os.Open(h.path)
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
		// Skip embedded db files too.
		if h.txf.IsTxDatabasePath(fi.Name()) {
			continue
		}

		h.Logger.Printf("opening index: %s", filepath.Base(fi.Name()))

		index, err := h.newIndex(h.IndexPath(filepath.Base(fi.Name())), filepath.Base(fi.Name()))
		if errors.Cause(err) == ErrName {
			h.Logger.Printf("ERROR opening index: %s, err=%s", fi.Name(), err)
			continue
		} else if err != nil {
			return errors.Wrap(err, "opening index")
		}

		if h.isPrimary() {
			index.createdAt = timestamp()
			err = index.OpenWithTimestamp()
		} else {
			err = index.Open()
		}
		if err != nil {
			_ = h.txf.Close()
			if err == ErrName {
				h.Logger.Printf("ERROR opening index: %s, err=%s", index.Name(), err)
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

	// under blue_green, we must sync blue from green before we turn on checking.
	if err := h.txf.green2blue(h); err != nil {
		return errors.Wrap(err, "Holder.Open h.txf.green2blue(h)")
	}

	h.txf.blueGreenOnIfRunningBlueGreen()

	h.Logger.Printf("open holder: complete")

	return nil

}

// Activate runs the background tasks relevant to keeping a holder in a stable
// state, such as scanning it for needed snapshots, or flushing caches. This
// is separate from opening because, while a server would nearly always want
// to do this, other use cases (like consistency checks of a data directory)
// need to avoid it even getting started.
func (h *Holder) Activate() {
	// Periodically flush cache.
	h.wg.Add(2)
	go func() { defer h.wg.Done(); h.monitorCacheFlush() }()
	go func() { defer h.wg.Done(); h.SnapshotQueue.ScanHolder(h, h.closing) }()
}

// checkForeignIndex is a check before applying a foreign
// index to a field; if the index is not yet available,
// (because holder is still opening and may not have opened
// the index yet), this method queues it up to be processed
// once all indexes have been opened.
func (h *Holder) checkForeignIndex(f *Field) error {
	if h.opening {
		if fi := h.Index(f.options.ForeignIndex); fi == nil {
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
	if h.txf != nil && h.txf.blueGreenReg != nil {
		h.txf.blueGreenReg.Close()
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
	if h.SnapshotQueue != nil {
		h.SnapshotQueue.Stop()
		h.SnapshotQueue = nil
	}

	_ = testhook.Closed(h.Auditor, h, nil)

	return nil
}

func (h *Holder) NeedsSnapshot() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return h.txf.NeedsSnapshot()
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
	if _, err := os.Stat(h.path); os.IsNotExist(err) {
		return false, nil
	} else if err != nil {
		return false, errors.Wrap(err, "statting data dir")
	}

	f, err := os.Open(h.path)
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
		// Skip embedded db files too.
		if h.txf.IsTxDatabasePath(fi.Name()) {
			continue
		}

		// Skip DisCo data directory.
		if fi.Name() == DefaultDiscoDir {
			continue
		}

		return true, nil
	}
	return false, nil
}

// hasV1TranslateKeysFile returns true if a v1 translation data file exists on disk.
func (h *Holder) hasV1TranslateKeysFile() (bool, error) {
	if _, err := os.Stat(filepath.Join(h.path, ".keys")); os.IsNotExist(err) {
		return true, nil
	} else if err != nil {
		return false, err
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
	var a []*IndexInfo
	for _, index := range h.Indexes() {
		di := &IndexInfo{
			Name:      index.Name(),
			CreatedAt: index.CreatedAt(),
			Options:   index.Options(),
		}
		for _, field := range index.Fields() {
			fi := &FieldInfo{
				Name:      field.Name(),
				CreatedAt: field.CreatedAt(),
				Options:   field.Options(),
			}
			for _, view := range field.views() {
				fi.Views = append(fi.Views, &ViewInfo{Name: view.name})
			}
			sort.Sort(viewInfoSlice(fi.Views))
			di.Fields = append(di.Fields, fi)
		}
		sort.Sort(fieldInfoSlice(di.Fields))
		a = append(a, di)
	}
	sort.Sort(indexInfoSlice(a))
	return a, nil
}

// limitedSchema returns schema information for all indexes and fields.
func (h *Holder) limitedSchema() ([]*IndexInfo, error) {
	var a []*IndexInfo
	for _, index := range h.Indexes() {
		di := &IndexInfo{
			Name:       index.Name(),
			CreatedAt:  index.CreatedAt(),
			Options:    index.Options(),
			ShardWidth: ShardWidth,
			Fields:     make([]*FieldInfo, 0, len(index.Fields())),
		}
		for _, field := range index.Fields() {
			if strings.HasPrefix(field.name, "_") {
				continue
			}
			fi := &FieldInfo{
				Name:      field.Name(),
				CreatedAt: field.CreatedAt(),
				Options:   field.Options(),
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
	// Create indexes that don't exist.
	for _, i := range schema.Indexes {
		idx, err := h.CreateIndexIfNotExists(i.Name, i.Options)
		if err != nil {
			return errors.Wrap(err, "creating index")
		}
		if i.CreatedAt != 0 {
			idx.mu.Lock()
			idx.createdAt = i.CreatedAt
			idx.mu.Unlock()
		}

		// Create fields that don't exist.
		for _, f := range i.Fields {
			fld, err := idx.createFieldIfNotExists(f.Name, &f.Options)
			if err != nil {
				return errors.Wrap(err, "creating field")
			}
			if f.CreatedAt != 0 {
				fld.mu.Lock()
				fld.createdAt = f.CreatedAt
				fld.mu.Unlock()
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
	return nil
}

// IndexPath returns the path where a given index is stored.
func (h *Holder) IndexPath(name string) string {
	return filepath.Join(h.path, name)
}

// HolderPathFromIndexPath is
// used by test/index.go:71 in test.Index.Reopen() to get the right
// path into a test Holder that doesn't know its own proper path.
// If the Holder changes index paths to being something other than
// holderPath + "/" + indexName, this will need adjusting too.
func (h *Holder) HolderPathFromIndexPath(indexPath, indexName string) string {
	n := len(indexPath)
	hpath2 := indexPath[:n-(len(indexName)+1)]
	return hpath2
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
	return h.createIndex(name, opt)
}

// CreateIndexIfNotExists returns an index by name.
// The index is created if it does not already exist.
func (h *Holder) CreateIndexIfNotExists(name string, opt IndexOptions) (*Index, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Return index if it exists.
	if index := h.Index(name); index != nil {
		return index, nil
	}
	return h.createIndex(name, opt)
}

func (h *Holder) createIndex(name string, opt IndexOptions) (*Index, error) {
	if name == "" {
		return nil, errors.New("index name required")
	}

	// Otherwise create a new index.
	index, err := h.newIndex(h.IndexPath(name), name)
	if err != nil {
		return nil, errors.Wrap(err, "creating")
	}

	index.keys = opt.Keys
	index.trackExistence = opt.TrackExistence

	if err = index.Open(); err != nil {
		return nil, errors.Wrap(err, "opening")
	}
	if err = index.saveMeta(); err != nil {
		return nil, errors.Wrap(err, "meta")
	}

	// Update options.
	h.addIndex(index)

	// Since this is a new index, we need to kick off
	// its translation sync.
	if err := h.translationSyncer.Reset(); err != nil {
		return nil, errors.Wrap(err, "resetting translation sync")
	}

	return index, nil
}

func (h *Holder) newIndex(path, name string) (*Index, error) {
	index, err := NewIndex(h, path, name)
	if err != nil {
		return nil, err
	}
	index.Stats = h.Stats.WithTags(fmt.Sprintf("index:%s", index.Name()))
	index.broadcaster = h.broadcaster
	index.newAttrStore = h.NewAttrStore
	index.columnAttrs = h.NewAttrStore(filepath.Join(index.path, ".data"))
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
						h.Logger.Printf("ERROR flushing cache: err=%s, path=%s", err, fragment.cachePath())
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

// TODO: this needs to be removed
func (h *Holder) isPrimary() bool {
	if s, ok := h.broadcaster.(*Server); ok {
		return s.IsPrimary()
	}
	return false
}

// setFileLimit attempts to set the open file limit to the FileLimit constant defined above.
func (h *Holder) setFileLimit() {
	oldLimit := &syscall.Rlimit{}
	newLimit := &syscall.Rlimit{}

	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, oldLimit); err != nil {
		h.Logger.Printf("ERROR checking open file limit: %s", err)
		return
	}
	// If the soft limit is lower than the FileLimit constant, we will try to change it.
	if oldLimit.Cur < fileLimit {
		newLimit.Cur = fileLimit
		// If the hard limit is not high enough, we will try to change it too.
		if oldLimit.Max < fileLimit {
			newLimit.Max = fileLimit
		} else {
			newLimit.Max = oldLimit.Max
		}

		// Try to set the limit
		if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, newLimit); err != nil {
			// If we just tried to change the hard limit and failed, we probably don't have permission. Let's try again without setting the hard limit.
			if newLimit.Max > oldLimit.Max {
				newLimit.Max = oldLimit.Max
				// Obviously the hard limit cannot be higher than the soft limit.
				if newLimit.Cur >= newLimit.Max {
					newLimit.Cur = newLimit.Max
				}
				// Try setting again with lowered Max (hard limit)
				if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, newLimit); err != nil {
					h.Logger.Printf("ERROR setting open file limit: %s", err)
				}
				// If we weren't trying to change the hard limit, let the user know something is wrong.
			} else {
				h.Logger.Printf("ERROR setting open file limit: %s", err)
			}
		}

		// Check the limit after setting it. OS may not obey Setrlimit call.
		if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, oldLimit); err != nil {
			h.Logger.Printf("ERROR checking open file limit: %s", err)
		} else {
			if oldLimit.Cur < fileLimit {
				h.Logger.Printf("WARNING: Tried to set open file limit to %d, but it is %d. You may consider running \"sudo ulimit -n %d\" before starting Pilosa to avoid \"too many open files\" error. See https://www.pilosa.com/docs/latest/administration/#open-file-limits for more information.", fileLimit, oldLimit.Cur, fileLimit)
			}
		}
	}
}

func (h *Holder) LoadNodeID() (string, error) {
	idPath := path.Join(h.path, ".id")
	h.Logger.Printf("load NodeID: %s", idPath)
	if err := os.MkdirAll(h.path, 0777); err != nil {
		return "", errors.Wrap(err, "creating directory")
	}

	nodeIDBytes, err := ioutil.ReadFile(idPath)
	if err == nil {
		nodeid := strings.TrimSpace(string(nodeIDBytes))
		h.Logger.Printf("I am NodeID: %s", nodeid)
		return nodeid, nil
	}
	if !os.IsNotExist(err) {
		return "", errors.Wrap(err, "reading file")
	}
	nodeID := uuid.NewV4().String()
	err = ioutil.WriteFile(idPath, []byte(nodeID), 0600)
	if err != nil {
		return "", errors.Wrap(err, "writing file")
	}
	h.Logger.Printf("I am NodeID: %s", nodeID)
	return nodeID, nil
}

// Log startup time and version to $DATA_DIR/.startup.log
func (h *Holder) logStartup() error {
	RFC3339NanoFixedWidth := "2006-01-02T15:04:05.000000 07:00"
	time := time.Now().Format(RFC3339NanoFixedWidth)
	logLine := fmt.Sprintf("%s\t%s\n", time, Version)

	f, err := os.OpenFile(h.path+"/.startup.log", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
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
	readers []TranslateEntryReader

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

		// Sync index column attributes.
		if err := s.syncIndex(di.Name); err != nil {
			return fmt.Errorf("index sync error: index=%s, err=%s", di.Name, err)
		}

		tf := time.Now()
		for _, fi := range di.Fields {
			// Verify syncer has not closed.
			if s.IsClosing() {
				return nil
			}

			// Sync field row attributes.
			if err := s.syncField(di.Name, fi.Name); err != nil {
				return fmt.Errorf("field sync error: index=%s, field=%s, err=%s", di.Name, fi.Name, err)
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

// syncIndex synchronizes index attributes with the rest of the cluster.
func (s *holderSyncer) syncIndex(index string) error {
	span, ctx := tracing.StartSpanFromContext(context.Background(), "HolderSyncer.syncIndex")
	defer span.Finish()

	// Retrieve index reference.
	idx := s.Holder.Index(index)
	if idx == nil {
		return nil
	}
	indexTag := fmt.Sprintf("index:%s", index)

	// Read block checksums.
	blks, err := idx.ColumnAttrStore().Blocks()
	if err != nil {
		return errors.Wrap(err, "getting blocks")
	}
	s.Stats.CountWithCustomTags(MetricColumnAttrStoreBlocks, int64(len(blks)), 1.0, []string{indexTag})

	// Sync with every other host.
	for _, node := range topology.Nodes(s.Cluster.noder.Nodes()).FilterID(s.Node.ID) {
		// Retrieve attributes from differing blocks.
		// Skip update and recomputation if no attributes have changed.
		m, err := s.Cluster.InternalClient.ColumnAttrDiff(ctx, &node.URI, index, blks)
		if err != nil {
			return errors.Wrap(err, "getting differing blocks")
		} else if len(m) == 0 {
			continue
		}
		s.Stats.CountWithCustomTags(MetricColumnAttrDiff, int64(len(m)), 1.0, []string{indexTag, node.ID})

		// Update local copy.
		if err := idx.ColumnAttrStore().SetBulkAttrs(m); err != nil {
			return errors.Wrap(err, "setting attrs")
		}

		// Recompute blocks.
		blks, err = idx.ColumnAttrStore().Blocks()
		if err != nil {
			return errors.Wrap(err, "recomputing blocks")
		}
	}

	return nil
}

// syncField synchronizes field attributes with the rest of the cluster.
func (s *holderSyncer) syncField(index, name string) error {
	span, ctx := tracing.StartSpanFromContext(context.Background(), "HolderSyncer.syncField")
	defer span.Finish()

	// Retrieve field reference.
	f := s.Holder.Field(index, name)
	if f == nil {
		return nil
	}
	indexTag := fmt.Sprintf("index:%s", index)
	fieldTag := fmt.Sprintf("field:%s", name)

	// Read block checksums.
	blks, err := f.RowAttrStore().Blocks()
	if err != nil {
		return errors.Wrap(err, "getting blocks")
	}
	s.Stats.CountWithCustomTags(MetricRowAttrStoreBlocks, int64(len(blks)), 1.0, []string{indexTag, fieldTag})

	// Sync with every other host.
	for _, node := range topology.Nodes(s.Cluster.noder.Nodes()).FilterID(s.Node.ID) {
		// Retrieve attributes from differing blocks.
		// Skip update and recomputation if no attributes have changed.
		m, err := s.Cluster.InternalClient.RowAttrDiff(ctx, &node.URI, index, name, blks)
		if errors.Cause(err) == ErrFieldNotFound {
			continue // field not created remotely yet, skip
		} else if err != nil {
			return errors.Wrap(err, "getting differing blocks")
		} else if len(m) == 0 {
			continue
		}
		s.Stats.CountWithCustomTags(MetricRowAttrDiff, int64(len(m)), 1.0, []string{indexTag, fieldTag, node.ID})

		// Update local copy.
		if err := f.RowAttrStore().SetBulkAttrs(m); err != nil {
			return errors.Wrap(err, "setting attrs")
		}

		// Recompute blocks.
		blks, err = f.RowAttrStore().Blocks()
		if err != nil {
			return errors.Wrap(err, "recomputing blocks")
		}
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
	// Stop existing streams.
	if err := s.stopTranslationSync(); err != nil {
		return errors.Wrap(err, "stop translation sync")
	}

	// Create a snapshot of the cluster to use for node/partition calculations.
	snap := topology.NewClusterSnapshot(s.Cluster.noder, s.Cluster.Hasher, s.Cluster.ReplicaN)

	// Set read-only flag for all translation stores.
	s.setTranslateReadOnlyFlags(snap)

	// Connect to each node that has a primary for which we are a replica.
	if err := s.initializeIndexTranslateReplication(snap); err != nil {
		return errors.Wrap(err, "initialize index translate replication")
	}

	// Connect to coordinator to stream field data.
	if err := s.initializeFieldTranslateReplication(snap); err != nil {
		return errors.Wrap(err, "initialize field translate replication")
	}
	return nil
}

////////////////////////////////////////////////////////////

// translationSyncer provides an interface allowing a function
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
	a.ch <- struct{}{}
	return nil
}

////////////////////////////////////////////////////////////

// stopTranslationSync closes and waits for all outstanding translation readers
// to complete. This should be called before reconnecting to the cluster in case
// of a cluster resize or schema change.
func (s *holderSyncer) stopTranslationSync() error {
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
// partition. Field stores are writable if the node is the coordinator.
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

// initializeIndexTranslateReplication connects to each node that is the
// primary for a partition that we are a replica of.
func (s *holderSyncer) initializeIndexTranslateReplication(snap *topology.ClusterSnapshot) error {
	for _, node := range snap.Nodes {
		// Skip local node.
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

		// Connect to remote node and begin streaming.
		rd, err := s.Holder.OpenTranslateReader(context.Background(), node.URI.String(), m)
		if err != nil {
			return err
		}
		s.readers = append(s.readers, rd)

		s.syncers.Go(func() error {
			defer rd.Close()
			s.readIndexTranslateReader(rd)
			return nil
		})
	}

	return nil
}

// initializeFieldTranslateReplication connects the coordinator to stream field data.
func (s *holderSyncer) initializeFieldTranslateReplication(snap *topology.ClusterSnapshot) error {
	// Skip if coordinator.
	if !snap.IsPrimaryFieldTranslationNode(s.Cluster.Node.ID) {
		return nil
	}

	// Build a map of partition offsets to stream from.
	m := make(TranslateOffsetMap)
	for _, index := range s.Holder.Indexes() {
		for _, field := range index.Fields() {
			store := field.TranslateStore()
			offset, err := store.MaxID()
			if err != nil {
				return errors.Wrapf(err, "cannot determine max id for %q/%q", index.Name(), field.Name())
			}
			m.SetFieldOffset(index.Name(), field.Name(), offset)
		}
	}

	// Skip if no replication required.
	if len(m) == 0 {
		return nil
	}

	// Connect to primary and begin streaming.
	primary := snap.PrimaryFieldTranslationNode()
	rd, err := s.Holder.OpenTranslateReader(context.Background(), primary.URI.String(), m)
	if err != nil {
		return err
	}
	s.readers = append(s.readers, rd)

	s.syncers.Go(func() error {
		defer rd.Close()
		s.readFieldTranslateReader(rd)
		return nil
	})
	return nil
}

func (s *holderSyncer) readIndexTranslateReader(rd TranslateEntryReader) {
	// Create a snapshot of the cluster to use for node/partition calculations.
	snap := topology.NewClusterSnapshot(s.Cluster.noder, s.Cluster.Hasher, s.Cluster.ReplicaN)

	for {
		var entry TranslateEntry
		if err := rd.ReadEntry(&entry); err != nil {
			s.Holder.Logger.Printf("cannot read index translate entry: %s", err)
			return
		}

		// Find appropriate store.
		idx := s.Holder.Index(entry.Index)
		if idx == nil {
			s.Holder.Logger.Printf("index not found: %q", entry.Index)
			return
		}

		// Apply replication to store.
		store := idx.TranslateStore(snap.KeyToKeyPartition(entry.Index, entry.Key))
		if err := store.ForceSet(entry.ID, entry.Key); err != nil {
			s.Holder.Logger.Printf("cannot force set index translation data: %d=%q", entry.ID, entry.Key)
			return
		}
	}
}

func (s *holderSyncer) readFieldTranslateReader(rd TranslateEntryReader) {
	for {
		var entry TranslateEntry
		if err := rd.ReadEntry(&entry); err != nil {
			s.Holder.Logger.Printf("cannot read field translate entry: %s", err)
			return
		}

		// Find appropriate store.
		f := s.Holder.Field(entry.Index, entry.Field)
		if f == nil {
			s.Holder.Logger.Printf("field not found: %s/%s", entry.Index, entry.Field)
			return
		}

		// Apply replication to store.
		store := f.TranslateStore()
		if err := store.ForceSet(entry.ID, entry.Key); err != nil {
			s.Holder.Logger.Printf("cannot force set field translation data: %d=%q", entry.ID, entry.Key)
			return
		}
	}
}

// holderCleaner removes fragments and data files that are no longer used.
type holderCleaner struct {
	Node *topology.Node

	Holder  *Holder
	Cluster *cluster

	// Signals that the sync should stop.
	Closing <-chan struct{}
}

// TODO: this is here to satisfy the linter since holderCleaner was removed from
// the gossip implementation of removeNode. But presumably we will use it once
// we have ported over the etcd implementation.
var _ holderCleaner

// IsClosing returns true if the cleaner has been marked to close.
func (c *holderCleaner) IsClosing() bool {
	select {
	case <-c.Closing:
		return true
	default:
		return false
	}
}

// CleanHolder compares the holder with the cluster state and removes
// any unnecessary fragments and files.
func (c *holderCleaner) CleanHolder() error {
	// Create a snapshot of the cluster to use for node/partition calculations.
	snap := topology.NewClusterSnapshot(c.Cluster.noder, c.Cluster.Hasher, c.Cluster.ReplicaN)

	for _, index := range c.Holder.Indexes() {
		// Verify cleaner has not closed.
		if c.IsClosing() {
			return nil
		}

		// Get the fragments that node is responsible for (based on hash(index, node)).
		containedShards := snap.ContainsShards(index.Name(), index.AvailableShards(includeRemote), c.Node)

		// Get the fragments registered in memory.
		for _, field := range index.Fields() {
			// deletedShards is used to track which shards for the field
			// were deleted. Any shards that get deleted from this node
			// get added to remoteAvailableShards. This is done because
			// the CleanHolder process is cleaning up shards which got
			// moved to other nodes. Because those shards still exist
			// (just no longer on this particular node), this node still
			// needs to consider each of them as an available shard in
			// the cluster.
			var deletedShards []uint64
			for _, view := range field.views() {
				for _, fragment := range view.allFragments() {
					fragShard := fragment.shard
					// Ignore fragments that should be present.
					if uint64InSlice(fragShard, containedShards) {
						continue
					}
					// Delete fragment.
					if err := view.deleteFragment(fragShard); err != nil {
						return errors.Wrap(err, "deleting fragment")
					}
					deletedShards = append(deletedShards, fragShard)
				}
			}
			if len(deletedShards) > 0 {
				if err := field.AddRemoteAvailableShards(roaring.NewBitmap(deletedShards...)); err != nil {
					return errors.Wrap(err, "adding remote available shards")
				}
			}
		}
	}
	return nil
}

func uint64InSlice(i uint64, s []uint64) bool {
	for _, o := range s {
		if i == o {
			return true
		}
	}
	return false
}

// Process loops through a holder based on the Check functions in op, calling
// the Process functions in op when indicated.
func (h *Holder) Process(ctx context.Context, op HolderOperator) (err error) {
	var fieldNames, viewNames []string
	var fragNums []uint64

	indexes := h.Indexes()
	for _, idx := range indexes {
		if err = ctx.Err(); err != nil {
			return err
		}
		if idx == nil {
			continue
		}
		indexName := idx.name
		process, recurse := op.CheckIndex(indexName)
		if !process && !recurse {
			continue
		}

		if err = ctx.Err(); err != nil {
			return err
		}
		if process {
			err = op.ProcessIndex(idx)
			if err != nil {
				return err
			}
		}
		if !recurse {
			continue
		}
		fieldNames = fieldNames[:0]
		idx.mu.Lock()
		for fieldName := range idx.fields {
			fieldNames = append(fieldNames, fieldName)
		}
		idx.mu.Unlock()
		for _, fieldName := range fieldNames {
			if err = ctx.Err(); err != nil {
				return err
			}
			process, recurse := op.CheckField(idx.name, fieldName)
			if !process && !recurse {
				continue
			}
			idx.mu.Lock()
			field := idx.fields[fieldName]
			idx.mu.Unlock()
			if field == nil {
				continue
			}
			if err = ctx.Err(); err != nil {
				return err
			}
			if process {
				err = op.ProcessField(field)
				if err != nil {
					return err
				}
			}
			if !recurse {
				continue
			}
			viewNames = viewNames[:0]
			field.mu.Lock()
			for viewName := range field.viewMap {
				viewNames = append(viewNames, viewName)
			}
			field.mu.Unlock()
			for _, viewName := range viewNames {
				if err = ctx.Err(); err != nil {
					return err
				}
				process, recurse := op.CheckView(indexName, fieldName, viewName)
				if !process && !recurse {
					continue
				}
				field.mu.Lock()
				view := field.viewMap[viewName]
				field.mu.Unlock()
				if view == nil {
					continue
				}
				if err = ctx.Err(); err != nil {
					return err
				}
				if process {
					err = op.ProcessView(view)
					if err != nil {
						return err
					}
				}
				if !recurse {
					continue
				}
				fragNums := fragNums[:0]
				view.mu.Lock()
				for fragNum := range view.fragments {
					fragNums = append(fragNums, fragNum)
				}
				view.mu.Unlock()
				for _, fragNum := range fragNums {
					if err = ctx.Err(); err != nil {
						return err
					}
					process := op.CheckFragment(indexName, fieldName, viewName, fragNum)
					if !process {
						continue
					}
					view.mu.Lock()
					frag := view.fragments[fragNum]
					view.mu.Unlock()
					err = op.ProcessFragment(frag)
					if err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

// used by Index.openFields(), enabling Tx / Txf by telling
// the holder about its own indexes.
func (h *Holder) addIndex(idx *Index) {
	h.imu.Lock()
	h.indexes[idx.name] = idx
	h.imu.Unlock()
}

func (h *Holder) DumpAllShards() {
	h.mu.RLock()
	defer h.mu.RUnlock()
	h.txf.dbPerShard.DumpAll()
}

func (h *Holder) Txf() *TxFactory {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.txf
}

// Begin starts a transaction on the holder. The index and shard
// must be specified.
func (h *Holder) BeginTx(writable bool, idx *Index, shard uint64) (Tx, error) {
	return h.txf.NewTx(Txo{Write: writable, Index: idx, Shard: shard}), nil
}

func (h *Holder) HasRoaringData() (has bool, err error) {

	idxs := h.Indexes()
	for _, idx := range idxs {
		paths, err := listFilesUnderDir(idx.path, false, "", true)
		if err != nil {
			return false, errors.Wrap(err, "HasRoaringData listFilesUnderDir")
		}
		index := idx.name

		for _, relpath := range paths {
			field, view, shard, err := fragmentSpecFromRoaringPath(relpath)
			if err != nil {
				continue // ignore .meta paths
			}
			abspath := idx.path + sep + relpath

			hasData, err := roaringFragmentHasData(abspath, index, field, view, shard)
			if err != nil {
				return false, errors.Wrap(err, "HasRoaringData roaringFragmentHasData")
			}
			if hasData {
				return true, nil
			}
		}
	}
	return
}
