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
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pilosa/pilosa/internal"
)

// Default frame settings.
const (
	DefaultRowLabel       = "rowID"
	DefaultCacheType      = CacheTypeRanked
	DefaultInverseEnabled = false

	// Default ranked frame cache
	DefaultCacheSize = 50000
)

// Frame represents a container for views.
type Frame struct {
	mu          sync.Mutex
	path        string
	index       string
	name        string
	timeQuantum TimeQuantum

	views map[string]*View

	// Row attribute storage and cache
	rowAttrStore *AttrStore

	broadcaster Broadcaster
	Stats       StatsClient

	// Frame settings.
	rowLabel       string
	cacheType      string
	inverseEnabled bool

	// Cache size for ranked frames
	cacheSize uint32

	LogOutput io.Writer
}

// NewFrame returns a new instance of frame.
func NewFrame(path, index, name string) (*Frame, error) {
	err := ValidateName(name)
	if err != nil {
		return nil, err
	}

	return &Frame{
		path:  path,
		index: index,
		name:  name,

		views:        make(map[string]*View),
		rowAttrStore: NewAttrStore(filepath.Join(path, ".data")),

		broadcaster: NopBroadcaster,
		Stats:       NopStatsClient,

		rowLabel:       DefaultRowLabel,
		inverseEnabled: DefaultInverseEnabled,
		cacheType:      DefaultCacheType,
		cacheSize:      DefaultCacheSize,

		LogOutput: ioutil.Discard,
	}, nil
}

// Name returns the name the frame was initialized with.
func (f *Frame) Name() string { return f.name }

// Index returns the index name the frame was initialized with.
func (f *Frame) Index() string { return f.index }

// Path returns the path the frame was initialized with.
func (f *Frame) Path() string { return f.path }

// RowAttrStore returns the attribute storage.
func (f *Frame) RowAttrStore() *AttrStore { return f.rowAttrStore }

// MaxSlice returns the max slice in the frame.
func (f *Frame) MaxSlice() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()

	view := f.views[ViewStandard]
	if view == nil {
		return 0
	}
	return view.MaxSlice()
}

// MaxInverseSlice returns the max inverse slice in the frame.
func (f *Frame) MaxInverseSlice() uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()

	view := f.views[ViewInverse]
	if view == nil {
		return 0
	}
	return view.MaxSlice()
}

// SetRowLabel sets the row labels. Persists to meta file on update.
func (f *Frame) SetRowLabel(v string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Ignore if no change occurred.
	if v == "" || f.rowLabel == v {
		return nil
	}

	// Make sure rowLabel is valid name
	err := ValidateLabel(v)
	if err != nil {
		return err
	}

	// Persist meta data to disk on change.
	f.rowLabel = v
	if err := f.saveMeta(); err != nil {
		return err
	}

	return nil
}

// RowLabel returns the row label.
func (f *Frame) RowLabel() string {
	f.mu.Lock()
	v := f.rowLabel
	f.mu.Unlock()
	return v
}

// CacheType returns the caching mode for the frame.
func (f *Frame) CacheType() string {
	return f.cacheType
}

// InverseEnabled returns true if an inverse view is available.
func (f *Frame) InverseEnabled() bool {
	return f.inverseEnabled
}

// SetCacheSize sets the cache size for ranked fames. Persists to meta file on update.
// defaults to DefaultCacheSize 50000
func (f *Frame) SetCacheSize(v uint32) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Ignore if no change occurred.
	if v == 0 || f.cacheSize == v {
		return nil
	}

	// Persist meta data to disk on change.
	f.cacheSize = v
	if err := f.saveMeta(); err != nil {
		return err
	}

	return nil
}

// CacheSize returns the ranked frame cache size.
func (f *Frame) CacheSize() uint32 {
	f.mu.Lock()
	v := f.cacheSize
	f.mu.Unlock()
	return v
}

// Options returns all options for this frame.
func (f *Frame) Options() FrameOptions {
	f.mu.Lock()
	opt := FrameOptions{
		RowLabel:       f.rowLabel,
		InverseEnabled: f.inverseEnabled,
		CacheType:      f.cacheType,
		CacheSize:      f.cacheSize,
		TimeQuantum:    f.timeQuantum,
	}
	f.mu.Unlock()
	return opt
}

// Open opens and initializes the frame.
func (f *Frame) Open() error {
	if err := func() error {
		// Ensure the frame's path exists.
		if err := os.MkdirAll(f.path, 0777); err != nil {
			return err
		}

		if err := f.loadMeta(); err != nil {
			return err
		}

		if err := f.openViews(); err != nil {
			return err
		}

		if err := f.rowAttrStore.Open(); err != nil {
			return err
		}

		return nil
	}(); err != nil {
		f.Close()
		return err
	}

	return nil
}

// openViews opens and initializes the views inside the frame.
func (f *Frame) openViews() error {
	file, err := os.Open(filepath.Join(f.path, "views"))
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}
	defer file.Close()

	fis, err := file.Readdir(0)
	if err != nil {
		return err
	}

	for _, fi := range fis {
		if !fi.IsDir() {
			continue
		}

		name := filepath.Base(fi.Name())
		view := f.newView(f.ViewPath(name), name)
		if err := view.Open(); err != nil {
			return fmt.Errorf("open view: view=%s, err=%s", view.Name(), err)
		}
		view.RowAttrStore = f.rowAttrStore
		f.views[view.Name()] = view
	}

	return nil
}

// loadMeta reads meta data for the frame, if any.
func (f *Frame) loadMeta() error {
	var pb internal.FrameMeta

	// Read data from meta file.
	buf, err := ioutil.ReadFile(filepath.Join(f.path, ".meta"))
	if os.IsNotExist(err) {
		f.timeQuantum = ""
		f.rowLabel = DefaultRowLabel
		f.cacheType = DefaultCacheType
		f.inverseEnabled = DefaultInverseEnabled
		f.cacheSize = DefaultCacheSize
		return nil
	} else if err != nil {
		return err
	} else {
		if err := proto.Unmarshal(buf, &pb); err != nil {
			return err
		}
	}

	// Copy metadata fields.
	f.timeQuantum = TimeQuantum(pb.TimeQuantum)
	f.rowLabel = pb.RowLabel
	f.inverseEnabled = pb.InverseEnabled
	f.cacheSize = pb.CacheSize

	// Copy cache type.
	f.cacheType = pb.CacheType
	if f.cacheType == "" {
		f.cacheType = DefaultCacheType
	}

	return nil
}

// saveMeta writes meta data for the frame.
func (f *Frame) saveMeta() error {
	// Marshal metadata.
	buf, err := proto.Marshal(&internal.FrameMeta{
		RowLabel:       f.rowLabel,
		InverseEnabled: f.inverseEnabled,
		CacheType:      f.cacheType,
		CacheSize:      f.cacheSize,
		TimeQuantum:    string(f.timeQuantum),
	})
	if err != nil {
		return err
	}

	// Write to meta file.
	if err := ioutil.WriteFile(filepath.Join(f.path, ".meta"), buf, 0666); err != nil {
		return err
	}

	return nil
}

// Close closes the frame and its views.
func (f *Frame) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Close the attribute store.
	if f.rowAttrStore != nil {
		_ = f.rowAttrStore.Close()
	}

	// Close all views.
	for _, view := range f.views {
		_ = view.Close()
	}
	f.views = make(map[string]*View)

	return nil
}

// TimeQuantum returns the time quantum for the frame.
func (f *Frame) TimeQuantum() TimeQuantum {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.timeQuantum
}

// SetTimeQuantum sets the time quantum for the frame.
func (f *Frame) SetTimeQuantum(q TimeQuantum) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Validate input.
	if !q.Valid() {
		return ErrInvalidTimeQuantum
	}

	// Update value on frame.
	f.timeQuantum = q

	// Persist meta data to disk.
	if err := f.saveMeta(); err != nil {
		return err
	}

	return nil
}

// ViewPath returns the path to a view in the frame.
func (f *Frame) ViewPath(name string) string {
	return filepath.Join(f.path, "views", name)
}

// View returns a view in the frame by name.
func (f *Frame) View(name string) *View {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.view(name)
}

func (f *Frame) view(name string) *View { return f.views[name] }

// Views returns a list of all views in the frame.
func (f *Frame) Views() []*View {
	f.mu.Lock()
	defer f.mu.Unlock()

	other := make([]*View, 0, len(f.views))
	for _, view := range f.views {
		other = append(other, view)
	}
	return other
}

// CreateViewIfNotExists returns the named view, creating it if necessary.
func (f *Frame) CreateViewIfNotExists(name string) (*View, error) {
	// Don't create inverse views if they are not enabled.
	if !f.InverseEnabled() && IsInverseView(name) {
		return nil, ErrFrameInverseDisabled
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if view := f.views[name]; view != nil {
		return view, nil
	}

	view := f.newView(f.ViewPath(name), name)
	if err := view.Open(); err != nil {
		return nil, err
	}
	view.RowAttrStore = f.rowAttrStore
	f.views[view.Name()] = view

	return view, nil
}

func (f *Frame) newView(path, name string) *View {
	view := NewView(path, f.index, f.name, name, f.cacheSize)
	view.cacheType = f.cacheType
	view.LogOutput = f.LogOutput
	view.RowAttrStore = f.rowAttrStore
	view.stats = f.Stats.WithTags(fmt.Sprintf("view:%s", name))
	view.broadcaster = f.broadcaster
	return view
}

// SetBit sets a bit on a view within the frame.
func (f *Frame) SetBit(name string, rowID, colID uint64, t *time.Time) (changed bool, err error) {
	// Validate view name.
	if !IsValidView(name) {
		return false, ErrInvalidView
	}

	// Retrieve view. Exit if it doesn't exist.
	view, err := f.CreateViewIfNotExists(name)
	if err != nil {
		return changed, err
	}

	// Set non-time bit.
	if v, err := view.SetBit(rowID, colID); err != nil {
		return changed, err
	} else if v {
		changed = v
	}

	// Exit early if no timestamp is specified.
	if t == nil {
		return changed, nil
	}

	// If a timestamp is specified then set bits across all views for the quantum.
	for _, subname := range ViewsByTime(name, *t, f.TimeQuantum()) {
		view, err := f.CreateViewIfNotExists(subname)
		if err != nil {
			return changed, err
		}

		if c, err := view.SetBit(rowID, colID); err != nil {
			return changed, err
		} else if c {
			changed = true
		}
	}

	return changed, nil
}

// ClearBit clears a bit within the frame.
func (f *Frame) ClearBit(name string, rowID, colID uint64, t *time.Time) (changed bool, err error) {
	// Validate view name.
	if !IsValidView(name) {
		return false, ErrInvalidView
	}

	// Retrieve view. Exit if it doesn't exist.
	view, err := f.CreateViewIfNotExists(name)
	if err != nil {
		return changed, err
	}

	// Clear non-time bit.
	if v, err := view.ClearBit(rowID, colID); err != nil {
		return changed, err
	} else if v {
		changed = v
	}

	// Exit early if no timestamp is specified.
	if t == nil {
		return changed, nil
	}

	// If a timestamp is specified then clear bits across all views for the quantum.
	for _, subname := range ViewsByTime(name, *t, f.TimeQuantum()) {
		view, err := f.CreateViewIfNotExists(subname)
		if err != nil {
			return changed, err
		}

		if c, err := view.ClearBit(rowID, colID); err != nil {
			return changed, err
		} else if c {
			changed = true
		}
	}

	return changed, nil
}

// Import bulk imports data.
func (f *Frame) Import(rowIDs, columnIDs []uint64, timestamps []*time.Time) error {
	// Determine quantum if timestamps are set.
	q := f.TimeQuantum()
	if hasTime(timestamps) && q == "" {
		return errors.New("time quantum not set in either index or frame")
	}

	// Split import data by fragment.
	dataByFragment := make(map[importKey]importData)
	for i := range rowIDs {
		rowID, columnID, timestamp := rowIDs[i], columnIDs[i], timestamps[i]

		var standard, inverse []string
		if timestamp == nil {
			standard = []string{ViewStandard}
			inverse = []string{ViewInverse}
		} else {
			standard = ViewsByTime(ViewStandard, *timestamp, q)
			// In order to match the logic of `SetBit()`, we want bits
			// with timestamps to write to both time and standard views.
			standard = append(standard, ViewStandard)
			inverse = ViewsByTime(ViewInverse, *timestamp, q)
		}

		// Attach bit to each standard view.
		for _, name := range standard {
			key := importKey{View: name, Slice: columnID / SliceWidth}
			data := dataByFragment[key]
			data.RowIDs = append(data.RowIDs, rowID)
			data.ColumnIDs = append(data.ColumnIDs, columnID)
			dataByFragment[key] = data
		}

		if f.inverseEnabled {
			// Attach reversed bits to each inverse view.
			for _, name := range inverse {
				key := importKey{View: name, Slice: rowID / SliceWidth}
				data := dataByFragment[key]
				data.RowIDs = append(data.RowIDs, columnID)    // reversed
				data.ColumnIDs = append(data.ColumnIDs, rowID) // reversed
				dataByFragment[key] = data
			}
		}
	}

	// Import into each fragment.
	for key, data := range dataByFragment {
		// Skip inverse data if inverse is not enabled.
		if !f.inverseEnabled && IsInverseView(key.View) {
			continue
		}

		// Re-sort data for inverse views.
		if IsInverseView(key.View) {
			sort.Sort(importBitSet{
				rowIDs:    data.RowIDs,
				columnIDs: data.ColumnIDs,
			})
		}

		view, err := f.CreateViewIfNotExists(key.View)
		if err != nil {
			return err
		}

		frag, err := view.CreateFragmentIfNotExists(key.Slice)
		if err != nil {
			return err
		}

		if err := frag.Import(data.RowIDs, data.ColumnIDs); err != nil {
			return err
		}
	}

	return nil
}

// encodeFrames converts a into its internal representation.
func encodeFrames(a []*Frame) []*internal.Frame {
	other := make([]*internal.Frame, len(a))
	for i := range a {
		other[i] = encodeFrame(a[i])
	}
	return other
}

// encodeFrame converts f into its internal representation.
func encodeFrame(f *Frame) *internal.Frame {
	return &internal.Frame{
		Name: f.name,
		Meta: &internal.FrameMeta{
			RowLabel:       f.rowLabel,
			InverseEnabled: f.inverseEnabled,
			CacheType:      f.cacheType,
			CacheSize:      f.cacheSize,
			TimeQuantum:    string(f.timeQuantum),
		},
	}
}

type frameSlice []*Frame

func (p frameSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p frameSlice) Len() int           { return len(p) }
func (p frameSlice) Less(i, j int) bool { return p[i].Name() < p[j].Name() }

// FrameInfo represents schema information for a frame.
type FrameInfo struct {
	Name  string      `json:"name"`
	Views []*ViewInfo `json:"views,omitempty"`
}

type frameInfoSlice []*FrameInfo

func (p frameInfoSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p frameInfoSlice) Len() int           { return len(p) }
func (p frameInfoSlice) Less(i, j int) bool { return p[i].Name < p[j].Name }

// FrameOptions represents options to set when initializing a frame.
type FrameOptions struct {
	RowLabel       string      `json:"rowLabel,omitempty"`
	InverseEnabled bool        `json:"inverseEnabled,omitempty"`
	CacheType      string      `json:"cacheType,omitempty"`
	CacheSize      uint32      `json:"cacheSize,omitempty"`
	TimeQuantum    TimeQuantum `json:"timeQuantum,omitempty"`
}

// Encode converts o into its internal representation.
func (o *FrameOptions) Encode() *internal.FrameMeta {
	return &internal.FrameMeta{
		RowLabel:       o.RowLabel,
		InverseEnabled: o.InverseEnabled,
		CacheType:      o.CacheType,
		CacheSize:      o.CacheSize,
		TimeQuantum:    string(o.TimeQuantum),
	}
}

// importBitSet represents slices of row and column ids.
// This is used to sort data during import.
type importBitSet struct {
	rowIDs, columnIDs []uint64
}

func (p importBitSet) Swap(i, j int) {
	p.rowIDs[i], p.rowIDs[j] = p.rowIDs[j], p.rowIDs[i]
	p.columnIDs[i], p.columnIDs[j] = p.columnIDs[j], p.columnIDs[i]
}
func (p importBitSet) Len() int           { return len(p.rowIDs) }
func (p importBitSet) Less(i, j int) bool { return p.rowIDs[i] < p.rowIDs[j] }

// Cache types.
const (
	CacheTypeLRU    = "lru"
	CacheTypeRanked = "ranked"
)

// IsValidCacheType returns true if v is a valid cache type.
func IsValidCacheType(v string) bool {
	switch v {
	case CacheTypeLRU, CacheTypeRanked:
		return true
	default:
		return false
	}
}
