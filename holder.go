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
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// DefaultCacheFlushInterval is the default value for Fragment.CacheFlushInterval.
const DefaultCacheFlushInterval = 1 * time.Minute

// Holder represents a container for indexes.
type Holder struct {
	mu sync.Mutex

	// Indexes by name.
	indexes map[string]*Index

	Broadcaster Broadcaster
	// Close management
	wg      sync.WaitGroup
	closing chan struct{}

	// Stats
	Stats StatsClient

	// Data directory path.
	Path string

	// The interval at which the cached row ids are persisted to disk.
	CacheFlushInterval time.Duration

	LogOutput io.Writer
}

// NewHolder returns a new instance of Holder.
func NewHolder() *Holder {
	return &Holder{
		indexes: make(map[string]*Index),
		closing: make(chan struct{}, 0),

		Broadcaster: NopBroadcaster,
		Stats:       NopStatsClient,

		CacheFlushInterval: DefaultCacheFlushInterval,

		LogOutput: os.Stderr,
	}
}

// Open initializes the root data directory for the holder.
func (h *Holder) Open() error {
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

		h.logger().Printf("opening index: %s", filepath.Base(fi.Name()))

		index, err := h.newIndex(h.IndexPath(filepath.Base(fi.Name())), filepath.Base(fi.Name()))
		if err == ErrName {
			h.logger().Printf("ERROR opening index: %s, err=%s", fi.Name(), err)
			continue
		} else if err != nil {
			return err
		}
		if err := index.Open(); err != nil {
			if err == ErrName {
				h.logger().Printf("ERROR opening index: %s, err=%s", index.Name(), err)
				continue
			}
			return fmt.Errorf("open index: name=%s, err=%s", index.Name(), err)
		}
		h.indexes[index.Name()] = index
	}

	// Periodically flush cache.
	h.wg.Add(1)
	go func() { defer h.wg.Done(); h.monitorCacheFlush() }()

	return nil
}

// Close closes all open fragments.
func (h *Holder) Close() error {
	// Notify goroutines of closing and wait for completion.
	close(h.closing)
	h.wg.Wait()

	for _, index := range h.indexes {
		index.Close()
	}
	return nil
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

// Schema returns schema data for all indexes and frames.
func (h *Holder) Schema() []*IndexInfo {
	var a []*IndexInfo
	for _, index := range h.Indexes() {
		di := &IndexInfo{Name: index.Name()}
		for _, frame := range index.Frames() {
			fi := &FrameInfo{Name: frame.Name()}
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

// IndexPath returns the path where a given index is stored.
func (h *Holder) IndexPath(name string) string { return filepath.Join(h.Path, name) }

// Index returns the index by name.
func (h *Holder) Index(name string) *Index {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.index(name)
}

func (h *Holder) index(name string) *Index { return h.indexes[name] }

// Indexes returns a list of all indexes in the holder.
func (h *Holder) Indexes() []*Index {
	h.mu.Lock()
	defer h.mu.Unlock()

	a := make([]*Index, 0, len(h.indexes))
	for _, index := range h.indexes {
		a = append(a, index)
	}
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
	index.SetColumnLabel(opt.ColumnLabel)
	index.SetTimeQuantum(opt.TimeQuantum)

	h.indexes[index.Name()] = index

	return index, nil
}

func (h *Holder) newIndex(path, name string) (*Index, error) {
	index, err := NewIndex(path, name)
	if err != nil {
		return nil, err
	}
	index.LogOutput = h.LogOutput
	index.Stats = h.Stats.WithTags(fmt.Sprintf("index:%s", index.Name()))
	index.broadcaster = h.Broadcaster
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
						h.logger().Printf("error flushing cache: err=%s, path=%s", err, fragment.CachePath())
					}
				}
			}
		}
	}
}

func (h *Holder) logger() *log.Logger { return log.New(h.LogOutput, "", log.LstdFlags) }

// HolderSyncer is an active anti-entropy tool that compares the local holder
// with a remote holder based on block checksums and resolves differences.
type HolderSyncer struct {
	Holder *Holder

	Host    string
	Cluster *Cluster

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
					if !s.Cluster.OwnsFragment(s.Host, di.Name, slice) {
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
		}
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

	// Read block checksums.
	blks, err := idx.ColumnAttrStore().Blocks()
	if err != nil {
		return err
	}

	// Sync with every other host.
	for _, node := range Nodes(s.Cluster.Nodes).FilterHost(s.Host) {
		client, err := NewClient(node.Host)
		if err != nil {
			return err
		}

		// Retrieve attributes from differing blocks.
		// Skip update and recomputation if no attributes have changed.
		m, err := client.ColumnAttrDiff(context.Background(), index, blks)
		if err != nil {
			return err
		} else if len(m) == 0 {
			continue
		}

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
	// Retrieve index reference.
	f := s.Holder.Frame(index, name)
	if f == nil {
		return nil
	}

	// Read block checksums.
	blks, err := f.RowAttrStore().Blocks()
	if err != nil {
		return err
	}

	// Sync with every other host.
	for _, node := range Nodes(s.Cluster.Nodes).FilterHost(s.Host) {
		client, err := NewClient(node.Host)
		if err != nil {
			return err
		}

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
		Fragment: frag,
		Host:     s.Host,
		Cluster:  s.Cluster,
		Closing:  s.Closing,
	}
	if err := fs.SyncFragment(); err != nil {
		return err
	}

	return nil
}
