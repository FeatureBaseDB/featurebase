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
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/pilosa/pilosa/logger"
	"github.com/pilosa/pilosa/roaring"
	"github.com/pilosa/pilosa/stats"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

const (
	// defaultCacheFlushInterval is the default value for Fragment.CacheFlushInterval.
	defaultCacheFlushInterval = 1 * time.Minute

	// fileLimit is the maximum open file limit (ulimit -n) to automatically set.
	fileLimit = 262144 // (512^2)

	// existenceFieldName is the name of the internal field used to store existence values.
	existenceFieldName = "exists"
)

// Holder represents a container for indexes.
type Holder struct {
	mu sync.RWMutex

	// Indexes by name.
	indexes map[string]*Index

	// Key/ID translation
	translateFile            *TranslateFile
	NewPrimaryTranslateStore func(interface{}) TranslateStore

	// opened channel is closed once Open() completes.
	opened chan struct{}

	broadcaster broadcaster

	NewAttrStore func(string) AttrStore

	// Close management
	wg      sync.WaitGroup
	closing chan struct{}

	// Stats
	Stats stats.StatsClient

	// Data directory path.
	Path string

	// The interval at which the cached row ids are persisted to disk.
	cacheFlushInterval time.Duration

	Logger logger.Logger
}

// NewHolder returns a new instance of Holder.
func NewHolder() *Holder {
	return &Holder{
		indexes: make(map[string]*Index),
		closing: make(chan struct{}),

		opened: make(chan struct{}),

		translateFile:            NewTranslateFile(),
		NewPrimaryTranslateStore: newNopTranslateStore,

		broadcaster: NopBroadcaster,
		Stats:       stats.NopStatsClient,

		NewAttrStore: newNopAttrStore,

		cacheFlushInterval: defaultCacheFlushInterval,

		Logger: logger.NopLogger,
	}
}

// Open initializes the root data directory for the holder.
func (h *Holder) Open() error {
	// Reset closing in case Holder is being reopened.
	h.closing = make(chan struct{})

	h.setFileLimit()

	h.Logger.Printf("open holder path: %s", h.Path)
	if err := os.MkdirAll(h.Path, 0777); err != nil {
		return errors.Wrap(err, "creating directory")
	}

	// Open path to read all index directories.
	f, err := os.Open(h.Path)
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

		h.Logger.Printf("opening index: %s", filepath.Base(fi.Name()))

		index, err := h.newIndex(h.IndexPath(filepath.Base(fi.Name())), filepath.Base(fi.Name()))
		if errors.Cause(err) == ErrName {
			h.Logger.Printf("ERROR opening index: %s, err=%s", fi.Name(), err)
			continue
		} else if err != nil {
			return errors.Wrap(err, "opening index")
		}
		if err := index.Open(); err != nil {
			if err == ErrName {
				h.Logger.Printf("ERROR opening index: %s, err=%s", index.Name(), err)
				continue
			}
			return fmt.Errorf("open index: name=%s, err=%s", index.Name(), err)
		}
		h.mu.Lock()
		h.indexes[index.Name()] = index
		h.mu.Unlock()
	}
	h.Logger.Printf("open holder: complete")

	// Periodically flush cache.
	h.wg.Add(1)
	go func() { defer h.wg.Done(); h.monitorCacheFlush() }()

	h.Stats.Open()

	close(h.opened)
	return nil
}

// Close closes all open fragments.
func (h *Holder) Close() error {
	h.Stats.Close()

	// Notify goroutines of closing and wait for completion.
	close(h.closing)
	h.wg.Wait()

	for _, index := range h.indexes {
		if err := index.Close(); err != nil {
			return errors.Wrap(err, "closing index")
		}
	}

	if h.translateFile != nil {
		if err := h.translateFile.Close(); err != nil {
			return err
		}
	}

	// Reset opened in case Holder needs to be reopened.
	h.opened = make(chan struct{})

	return nil
}

// HasData returns true if Holder contains at least one index.
// This is used to determine if the rebalancing of data is necessary
// when a node joins the cluster.
func (h *Holder) HasData() (bool, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.indexes) > 0 {
		return true, nil
	}
	// Open path to read all index directories.
	if _, err := os.Stat(h.Path); os.IsNotExist(err) {
		return false, nil
	} else if err != nil {
		return false, errors.Wrap(err, "statting data dir")
	}

	f, err := os.Open(h.Path)
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
		m[index.Name()] = index.AvailableShards()
	}
	return m
}

// Schema returns schema information for all indexes, fields, and views.
func (h *Holder) Schema() []*IndexInfo {
	var a []*IndexInfo
	for _, index := range h.Indexes() {
		di := &IndexInfo{Name: index.Name()}
		for _, field := range index.Fields() {
			fi := &FieldInfo{Name: field.Name(), Options: field.Options()}
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
	return a
}

// limitedSchema returns schema information for all indexes and fields.
func (h *Holder) limitedSchema() []*IndexInfo {
	var a []*IndexInfo
	for _, index := range h.Indexes() {
		di := &IndexInfo{Name: index.Name(), Options: index.Options()}
		for _, field := range index.Fields() {
			fi := &FieldInfo{Name: field.Name(), Options: field.Options()}
			di.Fields = append(di.Fields, fi)
		}
		sort.Sort(fieldInfoSlice(di.Fields))
		a = append(a, di)
	}
	sort.Sort(indexInfoSlice(a))
	return a
}

// applySchema applies an internal Schema to Holder.
func (h *Holder) applySchema(schema *Schema) error {
	// Create indexes that don't exist.
	for _, index := range schema.Indexes {
		idx, err := h.CreateIndexIfNotExists(index.Name, index.Options)
		if err != nil {
			return errors.Wrap(err, "creating index")
		}
		// Create fields that don't exist.
		for _, f := range index.Fields {
			field, err := idx.createFieldIfNotExists(f.Name, f.Options)
			if err != nil {
				return errors.Wrap(err, "creating field")
			}
			// Create views that don't exist.
			for _, v := range f.Views {
				_, err := field.createViewIfNotExists(v.Name)
				if err != nil {
					return errors.Wrap(err, "creating view")
				}
			}
		}
	}
	return nil
}

// IndexPath returns the path where a given index is stored.
func (h *Holder) IndexPath(name string) string { return filepath.Join(h.Path, name) }

// Index returns the index by name.
func (h *Holder) Index(name string) *Index {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.index(name)
}

func (h *Holder) index(name string) *Index { return h.indexes[name] }

// Indexes returns a list of all indexes in the holder.
func (h *Holder) Indexes() []*Index {
	h.mu.RLock()
	a := make([]*Index, 0, len(h.indexes))
	for _, index := range h.indexes {
		a = append(a, index)
	}
	h.mu.RUnlock()

	sort.Sort(indexSlice(a))
	return a
}

// CreateIndex creates an index.
// An error is returned if the index already exists.
func (h *Holder) CreateIndex(name string, opt IndexOptions) (*Index, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Ensure index doesn't already exist.
	if h.indexes[name] != nil {
		return nil, newConflictError(ErrIndexExists)
	}
	return h.createIndex(name, opt)
}

// CreateIndexIfNotExists returns an index by name.
// The index is created if it does not already exist.
func (h *Holder) CreateIndexIfNotExists(name string, opt IndexOptions) (*Index, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Find index in cache first.
	if index := h.indexes[name]; index != nil {
		return index, nil
	}

	return h.createIndex(name, opt)
}

func (h *Holder) createIndex(name string, opt IndexOptions) (*Index, error) {
	if name == "" {
		return nil, errors.New("index name required")
	}

	// Return index if it exists.
	if index := h.index(name); index != nil {
		return index, nil
	}

	// Otherwise create a new index.
	index, err := h.newIndex(h.IndexPath(name), name)
	if err != nil {
		return nil, errors.Wrap(err, "creating")
	}

	index.keys = opt.Keys
	index.trackExistence = opt.TrackExistence

	if err := index.Open(); err != nil {
		return nil, errors.Wrap(err, "opening")
	} else if err := index.saveMeta(); err != nil {
		return nil, errors.Wrap(err, "meta")
	}

	// Update options.
	h.indexes[index.Name()] = index

	return index, nil
}

func (h *Holder) newIndex(path, name string) (*Index, error) {
	index, err := NewIndex(path, name)
	if err != nil {
		return nil, err
	}
	index.logger = h.Logger
	index.Stats = h.Stats.WithTags(fmt.Sprintf("index:%s", index.Name()))
	index.broadcaster = h.broadcaster
	index.newAttrStore = h.NewAttrStore
	index.columnAttrs = h.NewAttrStore(filepath.Join(index.path, ".data"))
	return index, nil
}

// DeleteIndex removes an index from the holder.
func (h *Holder) DeleteIndex(name string) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Confirm index exists.
	index := h.index(name)
	if index == nil {
		return newNotFoundError(ErrIndexNotFound)
	}

	// Close index.
	if err := index.Close(); err != nil {
		return errors.Wrap(err, "closing")
	}

	// Delete index directory.
	if err := os.RemoveAll(h.IndexPath(name)); err != nil {
		return errors.Wrap(err, "removing directory")
	}

	// Remove reference.
	delete(h.indexes, name)

	return nil
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
						h.Logger.Printf("error flushing cache: err=%s, path=%s", err, fragment.cachePath())
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
func (h *Holder) recalculateCaches() {
	for _, index := range h.Indexes() {
		index.recalculateCaches()
	}
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
				h.Logger.Printf("WARNING: Tried to set open file limit to %d, but it is %d. You may consider running \"sudo ulimit -n %d\" before starting Pilosa to avoid \"too many open files\" error. See https://www.pilosa.com/docs/administration/#open-file-limits for more information.", fileLimit, oldLimit.Cur, fileLimit)
			}
		}
	}
}

func (h *Holder) loadNodeID() (string, error) {
	idPath := path.Join(h.Path, ".id")
	nodeID := ""
	h.Logger.Printf("load NodeID: %s", idPath)
	if err := os.MkdirAll(h.Path, 0777); err != nil {
		return "", errors.Wrap(err, "creating directory")
	}

	nodeIDBytes, err := ioutil.ReadFile(idPath)
	if err == nil {
		nodeID = strings.TrimSpace(string(nodeIDBytes))
	} else if os.IsNotExist(err) {
		nodeID = uuid.NewV4().String()
		err = ioutil.WriteFile(idPath, []byte(nodeID), 0600)
		if err != nil {
			return "", errors.Wrap(err, "writing file")
		}
	} else if err != nil {
		return "", errors.Wrap(err, "reading file")
	}

	return nodeID, nil
}

// Log startup time and version to $DATA_DIR/.startup.log
func (h *Holder) logStartup() error {
	time, err := time.Now().MarshalText()
	if err != nil {
		return errors.Wrap(err, "creating timestamp")
	}
	logLine := fmt.Sprintf("%s\t%s\n", time, Version)

	f, err := os.OpenFile(h.Path+"/.startup.log", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return errors.Wrap(err, "opening startup log")
	}

	defer f.Close()

	if _, err = f.WriteString(logLine); err != nil {
		return errors.Wrap(err, "writing startup log")
	}

	return nil
}

func (h *Holder) setPrimaryTranslateStore(node *Node) {
	var nodeID string
	if node != nil {
		nodeID = node.ID
	}
	ts := h.NewPrimaryTranslateStore(node)
	h.translateFile.SetPrimaryStore(nodeID, ts)
}

// holderSyncer is an active anti-entropy tool that compares the local holder
// with a remote holder based on block checksums and resolves differences.
type holderSyncer struct {
	mu sync.Mutex

	Holder *Holder

	Node    *Node
	Cluster *cluster

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
	// Iterate over schema in sorted order.
	for _, di := range s.Holder.Schema() {
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

				itr := s.Holder.Index(di.Name).AvailableShards().Iterator()
				itr.Seek(0)
				for shard, eof := itr.Next(); !eof; shard, eof = itr.Next() {
					// Ignore shards that this host doesn't own.
					if !s.Cluster.ownsShard(s.Node.ID, di.Name, shard) {
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
			s.Stats.Histogram("syncField", float64(time.Since(tf)), 1.0)
			tf = time.Now() // reset tf
		}
		s.Stats.Histogram("syncIndex", float64(time.Since(ti)), 1.0)
		ti = time.Now() // reset ti
	}

	return nil
}

// syncIndex synchronizes index attributes with the rest of the cluster.
func (s *holderSyncer) syncIndex(index string) error {
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
	s.Stats.CountWithCustomTags("ColumnAttrStoreBlocks", int64(len(blks)), 1.0, []string{indexTag})

	// Sync with every other host.
	for _, node := range Nodes(s.Cluster.nodes).FilterID(s.Node.ID) {
		// Retrieve attributes from differing blocks.
		// Skip update and recomputation if no attributes have changed.
		m, err := s.Cluster.InternalClient.ColumnAttrDiff(context.Background(), &node.URI, index, blks)
		if err != nil {
			return errors.Wrap(err, "getting differing blocks")
		} else if len(m) == 0 {
			continue
		}
		s.Stats.CountWithCustomTags("ColumnAttrDiff", int64(len(m)), 1.0, []string{indexTag, node.ID})

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
	s.Stats.CountWithCustomTags("RowAttrStoreBlocks", int64(len(blks)), 1.0, []string{indexTag, fieldTag})

	// Sync with every other host.
	for _, node := range Nodes(s.Cluster.nodes).FilterID(s.Node.ID) {
		// Retrieve attributes from differing blocks.
		// Skip update and recomputation if no attributes have changed.
		m, err := s.Cluster.InternalClient.RowAttrDiff(context.Background(), &node.URI, index, name, blks)
		if err == ErrFieldNotFound {
			continue // field not created remotely yet, skip
		} else if err != nil {
			return errors.Wrap(err, "getting differing blocks")
		} else if len(m) == 0 {
			continue
		}
		s.Stats.CountWithCustomTags("RowAttrDiff", int64(len(m)), 1.0, []string{indexTag, fieldTag, node.ID})

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
		return ErrFieldNotFound
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
		Fragment: frag,
		Node:     s.Node,
		Cluster:  s.Cluster,
		Closing:  s.Closing,
	}
	if err := fs.syncFragment(); err != nil {
		return errors.Wrap(err, "syncing fragment")
	}

	return nil
}

// holderCleaner removes fragments and data files that are no longer used.
type holderCleaner struct {
	Node *Node

	Holder  *Holder
	Cluster *cluster

	// Signals that the sync should stop.
	Closing <-chan struct{}
}

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
	for _, index := range c.Holder.Indexes() {
		// Verify cleaner has not closed.
		if c.IsClosing() {
			return nil
		}

		// Get the fragments that node is responsible for (based on hash(index, node)).
		containedShards := c.Cluster.containsShards(index.Name(), index.AvailableShards(), c.Node)

		// Get the fragments registered in memory.
		for _, field := range index.Fields() {
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
