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
	"net/http"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/pilosa/pilosa/internal"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

const (
	// DefaultCacheFlushInterval is the default value for Fragment.CacheFlushInterval.
	DefaultCacheFlushInterval = 1 * time.Minute

	// FileLimit is the maximum open file limit (ulimit -n) to automatically set.
	FileLimit = 262144 // (512^2)
)

// Holder represents a container for indexes.
type Holder struct {
	mu sync.RWMutex

	// Indexes by name.
	indexes map[string]*Index

	// opened channel is closed once Open() completes.
	opened chan struct{}

	Broadcaster Broadcaster

	NewAttrStore func(string) AttrStore

	// Close management
	wg      sync.WaitGroup
	closing chan struct{}

	// Stats
	Stats StatsClient

	// Data directory path.
	Path string

	// The interval at which the cached row ids are persisted to disk.
	CacheFlushInterval time.Duration

	Logger Logger
}

// NewHolder returns a new instance of Holder.
func NewHolder() *Holder {
	return &Holder{
		indexes: make(map[string]*Index),
		closing: make(chan struct{}, 0),

		opened: make(chan struct{}),

		Broadcaster: NopBroadcaster,
		Stats:       NopStatsClient,

		NewAttrStore: NewNopAttrStore,

		CacheFlushInterval: DefaultCacheFlushInterval,

		Logger: NopLogger,
	}
}

// Open initializes the root data directory for the holder.
func (h *Holder) Open() error {
	h.setFileLimit()

	h.Logger.Printf("open holder path: %s", h.Path)
	if err := os.MkdirAll(h.Path, 0777); err != nil {
		return err
	}

	// Open path to read all index directories.
	f, err := os.Open(h.Path)
	if err != nil {
		return err
	}
	defer f.Close()

	fis, err := f.Readdir(0)
	if err != nil {
		return err
	}

	for _, fi := range fis {
		if !fi.IsDir() {
			continue
		}

		h.Logger.Printf("opening index: %s", filepath.Base(fi.Name()))

		index, err := h.newIndex(h.IndexPath(filepath.Base(fi.Name())), filepath.Base(fi.Name()))
		if err == ErrName {
			h.Logger.Printf("ERROR opening index: %s, err=%s", fi.Name(), err)
			continue
		} else if err != nil {
			return err
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
			return err
		}
	}
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

// MaxSlices returns MaxSlice map for all indexes.
func (h *Holder) MaxSlices() map[string]uint64 {
	a := make(map[string]uint64)
	for _, index := range h.Indexes() {
		a[index.Name()] = index.MaxSlice()
	}
	return a
}

// MaxInverseSlices returns MaxInverseSlice map for all indexes.
func (h *Holder) MaxInverseSlices() map[string]uint64 {
	a := make(map[string]uint64)
	for _, index := range h.Indexes() {
		a[index.Name()] = index.MaxInverseSlice()
	}
	return a
}

// Schema returns schema information for all indexes, frames, and views.
func (h *Holder) Schema() []*IndexInfo {
	var a []*IndexInfo
	for _, index := range h.Indexes() {
		di := &IndexInfo{Name: index.Name()}
		for _, frame := range index.Frames() {
			fi := &FrameInfo{Name: frame.Name(), Options: frame.Options()}
			for _, view := range frame.Views() {
				fi.Views = append(fi.Views, &ViewInfo{Name: view.Name()})
			}
			sort.Sort(viewInfoSlice(fi.Views))
			di.Frames = append(di.Frames, fi)
		}
		sort.Sort(frameInfoSlice(di.Frames))
		a = append(a, di)
	}
	sort.Sort(indexInfoSlice(a))
	return a
}

// ApplySchema applies an internal Schema to Holder.
func (h *Holder) ApplySchema(schema *internal.Schema) error {
	// Create indexes that don't exist.
	for _, index := range schema.Indexes {
		opt := IndexOptions{}
		idx, err := h.CreateIndexIfNotExists(index.Name, opt)
		if err != nil {
			return err
		}
		// Create frames that don't exist.
		for _, f := range index.Frames {
			opt := decodeFrameOptions(f.Meta)
			frame, err := idx.CreateFrameIfNotExists(f.Name, *opt)
			if err != nil {
				return err
			}
			// Create views that don't exist.
			for _, v := range f.Views {
				_, err := frame.CreateViewIfNotExists(v)
				if err != nil {
					return err
				}
			}
		}
		// TODO: Create inputDefinitions that don't exist.
	}
	return nil
}

// EncodeMaxSlices creates and internal representation of max slices.
func (h *Holder) EncodeMaxSlices() *internal.MaxSlices {
	return &internal.MaxSlices{
		Standard: h.MaxSlices(),
		Inverse:  h.MaxInverseSlices(),
	}
}

// EncodeSchema creates an internal representation of schema.
func (h *Holder) EncodeSchema() *internal.Schema {
	return &internal.Schema{
		Indexes: EncodeIndexes(h.Indexes()),
	}
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
		return nil, ErrIndexExists
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
		return nil, err
	}

	if err := index.Open(); err != nil {
		return nil, err
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
	index.Logger = h.Logger
	index.Stats = h.Stats.WithTags(fmt.Sprintf("index:%s", index.Name()))
	index.broadcaster = h.Broadcaster
	index.NewAttrStore = h.NewAttrStore
	index.columnAttrStore = h.NewAttrStore(filepath.Join(index.path, ".data"))
	return index, nil
}

// DeleteIndex removes an index from the holder.
func (h *Holder) DeleteIndex(name string) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Ignore if index doesn't exist.
	index := h.index(name)
	if index == nil {
		return nil
	}

	// Close index.
	if err := index.Close(); err != nil {
		return err
	}

	// Delete index directory.
	if err := os.RemoveAll(h.IndexPath(name)); err != nil {
		return err
	}

	// Remove reference.
	delete(h.indexes, name)

	return nil
}

// Frame returns the frame for an index and name.
func (h *Holder) Frame(index, name string) *Frame {
	idx := h.Index(index)
	if idx == nil {
		return nil
	}
	return idx.Frame(name)
}

// View returns the view for an index, frame, and name.
func (h *Holder) View(index, frame, name string) *View {
	f := h.Frame(index, frame)
	if f == nil {
		return nil
	}
	return f.View(name)
}

// Fragment returns the fragment for an index, frame & slice.
func (h *Holder) Fragment(index, frame, view string, slice uint64) *Fragment {
	v := h.View(index, frame, view)
	if v == nil {
		return nil
	}
	return v.Fragment(slice)
}

// monitorCacheFlush periodically flushes all fragment caches sequentially.
// This is run in a goroutine.
func (h *Holder) monitorCacheFlush() {
	ticker := time.NewTicker(h.CacheFlushInterval)
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
		for _, frame := range index.Frames() {
			for _, view := range frame.Views() {
				for _, fragment := range view.Fragments() {
					select {
					case <-h.closing:
						return
					default:
					}

					if err := fragment.FlushCache(); err != nil {
						h.Logger.Printf("error flushing cache: err=%s, path=%s", err, fragment.CachePath())
					}
				}
			}
		}
	}
}

// RecalculateCaches recalculates caches on every index in the holder. This is
// probably not practical to call in real-world workloads, but makes writing
// integration tests much eaiser, since one doesn't have to wait 10 seconds
// after setting bits to get expected response.
func (h *Holder) RecalculateCaches() {
	for _, index := range h.Indexes() {
		index.RecalculateCaches()
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
	if oldLimit.Cur < FileLimit {
		newLimit.Cur = FileLimit
		// If the hard limit is not high enough, we will try to change it too.
		if oldLimit.Max < FileLimit {
			newLimit.Max = FileLimit
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
			if oldLimit.Cur < FileLimit {
				h.Logger.Printf("WARNING: Tried to set open file limit to %d, but it is %d. You may consider running \"sudo ulimit -n %d\" before starting Pilosa to avoid \"too many open files\" error. See https://www.pilosa.com/docs/administration/#open-file-limits for more information.", FileLimit, oldLimit.Cur, FileLimit)
			}
		}
	}
}

func (h *Holder) loadNodeID() (string, error) {
	idPath := path.Join(h.Path, ".id")
	nodeID := ""
	h.Logger.Printf("load NodeID: %s", idPath)
	if err := os.MkdirAll(h.Path, 0777); err != nil {
		return "", err
	}

	nodeIDBytes, err := ioutil.ReadFile(idPath)
	if err == nil {
		nodeID = strings.TrimSpace(string(nodeIDBytes))
	} else if os.IsNotExist(err) {
		nodeID = uuid.NewV4().String()
		err = ioutil.WriteFile(idPath, []byte(nodeID), 0600)
		if err != nil {
			return "", err
		}
	} else if err != nil {
		return "", err
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

// HolderSyncer is an active anti-entropy tool that compares the local holder
// with a remote holder based on block checksums and resolves differences.
type HolderSyncer struct {
	Holder *Holder

	Node         *Node
	Cluster      *Cluster
	RemoteClient *http.Client

	// Stats
	Stats StatsClient

	// Signals that the sync should stop.
	Closing <-chan struct{}
}

// IsClosing returns true if the syncer has been marked to close.
func (s *HolderSyncer) IsClosing() bool {
	select {
	case <-s.Closing:
		return true
	default:
		return false
	}
}

// SyncHolder compares the holder on host with the local holder and resolves differences.
func (s *HolderSyncer) SyncHolder() error {
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
		for _, fi := range di.Frames {
			// Verify syncer has not closed.
			if s.IsClosing() {
				return nil
			}

			// Sync frame row attributes.
			if err := s.syncFrame(di.Name, fi.Name); err != nil {
				return fmt.Errorf("frame sync error: index=%s, frame=%s, err=%s", di.Name, fi.Name, err)
			}

			for _, vi := range fi.Views {
				// Verify syncer has not closed.
				if s.IsClosing() {
					return nil
				}

				for slice := uint64(0); slice <= s.Holder.Index(di.Name).MaxSlice(); slice++ {
					// Ignore slices that this host doesn't own.
					if !s.Cluster.OwnsSlice(s.Node.ID, di.Name, slice) {
						continue
					}

					// Verify syncer has not closed.
					if s.IsClosing() {
						return nil
					}

					// Sync fragment if own it.
					if err := s.syncFragment(di.Name, fi.Name, vi.Name, slice); err != nil {
						return fmt.Errorf("fragment sync error: index=%s, frame=%s, slice=%d, err=%s", di.Name, fi.Name, slice, err)
					}
				}
			}
			s.Stats.Histogram("syncFrame", float64(time.Since(tf)), 1.0)
			tf = time.Now() // reset tf
		}
		s.Stats.Histogram("syncIndex", float64(time.Since(ti)), 1.0)
		ti = time.Now() // reset ti
	}

	return nil
}

// syncIndex synchronizes index attributes with the rest of the cluster.
func (s *HolderSyncer) syncIndex(index string) error {
	// Retrieve index reference.
	idx := s.Holder.Index(index)
	if idx == nil {
		return nil
	}
	indexTag := fmt.Sprintf("index:%s", index)

	// Read block checksums.
	blks, err := idx.ColumnAttrStore().Blocks()
	if err != nil {
		return err
	}
	s.Stats.CountWithCustomTags("ColumnAttrStoreBlocks", int64(len(blks)), 1.0, []string{indexTag})

	// Sync with every other host.
	for _, node := range Nodes(s.Cluster.Nodes).FilterID(s.Node.ID) {
		client := NewInternalHTTPClientFromURI(&node.URI, s.RemoteClient)

		// Retrieve attributes from differing blocks.
		// Skip update and recomputation if no attributes have changed.
		m, err := client.ColumnAttrDiff(context.Background(), index, blks)
		if err != nil {
			return err
		} else if len(m) == 0 {
			continue
		}
		s.Stats.CountWithCustomTags("ColumnAttrDiff", int64(len(m)), 1.0, []string{indexTag, node.ID})

		// Update local copy.
		if err := idx.ColumnAttrStore().SetBulkAttrs(m); err != nil {
			return err
		}

		// Recompute blocks.
		blks, err = idx.ColumnAttrStore().Blocks()
		if err != nil {
			return err
		}
	}

	return nil
}

// syncFrame synchronizes frame attributes with the rest of the cluster.
func (s *HolderSyncer) syncFrame(index, name string) error {
	// Retrieve frame reference.
	f := s.Holder.Frame(index, name)
	if f == nil {
		return nil
	}
	indexTag := fmt.Sprintf("index:%s", index)
	frameTag := fmt.Sprintf("frame:%s", name)

	// Read block checksums.
	blks, err := f.RowAttrStore().Blocks()
	if err != nil {
		return err
	}
	s.Stats.CountWithCustomTags("RowAttrStoreBlocks", int64(len(blks)), 1.0, []string{indexTag, frameTag})

	// Sync with every other host.
	for _, node := range Nodes(s.Cluster.Nodes).FilterID(s.Node.ID) {
		client := NewInternalHTTPClientFromURI(&node.URI, s.RemoteClient)

		// Retrieve attributes from differing blocks.
		// Skip update and recomputation if no attributes have changed.
		m, err := client.RowAttrDiff(context.Background(), index, name, blks)
		if err == ErrFrameNotFound {
			continue // frame not created remotely yet, skip
		} else if err != nil {
			return err
		} else if len(m) == 0 {
			continue
		}
		s.Stats.CountWithCustomTags("RowAttrDiff", int64(len(m)), 1.0, []string{indexTag, frameTag, node.ID})

		// Update local copy.
		if err := f.RowAttrStore().SetBulkAttrs(m); err != nil {
			return err
		}

		// Recompute blocks.
		blks, err = f.RowAttrStore().Blocks()
		if err != nil {
			return err
		}
	}

	return nil
}

// syncFragment synchronizes a fragment with the rest of the cluster.
func (s *HolderSyncer) syncFragment(index, frame, view string, slice uint64) error {
	// Retrieve local frame.
	f := s.Holder.Frame(index, frame)
	if f == nil {
		return ErrFrameNotFound
	}

	// Ensure view exists locally.
	v, err := f.CreateViewIfNotExists(view)
	if err != nil {
		return err
	}

	// Ensure fragment exists locally.
	frag, err := v.CreateFragmentIfNotExists(slice)
	if err != nil {
		return err
	}

	// Sync fragments together.
	fs := FragmentSyncer{
		Fragment:     frag,
		Node:         s.Node,
		Cluster:      s.Cluster,
		Closing:      s.Closing,
		RemoteClient: s.RemoteClient,
	}
	if err := fs.SyncFragment(); err != nil {
		return err
	}

	return nil
}

// HolderCleaner removes fragments and data files that are no longer used.
type HolderCleaner struct {
	Node *Node

	Holder  *Holder
	Cluster *Cluster

	// Signals that the sync should stop.
	Closing <-chan struct{}
}

// IsClosing returns true if the cleaner has been marked to close.
func (c *HolderCleaner) IsClosing() bool {
	select {
	case <-c.Closing:
		return true
	default:
		return false
	}
}

// CleanHolder compares the holder with the cluster state and removes
// any unnecessary fragments and files.
func (c *HolderCleaner) CleanHolder() error {
	for _, index := range c.Holder.Indexes() {
		// Verify cleaner has not closed.
		if c.IsClosing() {
			return nil
		}

		// Get the fragments that node is responsible for (based on hash(index, node)).
		containedSlices := c.Cluster.ContainsSlices(index.Name(), index.MaxSlice(), c.Node)

		// Get the fragments registered in memory.
		for _, frame := range index.Frames() {
			for _, view := range frame.Views() {
				for _, fragment := range view.Fragments() {
					fragSlice := fragment.Slice()
					// Ignore fragments that should be present.
					if uint64InSlice(fragSlice, containedSlices) {
						continue
					}
					// Delete fragment.
					if err := view.DeleteFragment(fragSlice); err != nil {
						return err
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
