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
	"github.com/pilosa/pilosa/pql"
)

// Default frame settings.
const (
	DefaultRowLabel       = "rowID"
	DefaultCacheType      = CacheTypeRanked
	DefaultInverseEnabled = false
	DefaultRangeEnabled   = false

	// Default ranked frame cache
	DefaultCacheSize = 50000
)

// Frame represents a container for views.
type Frame struct {
	mu          sync.RWMutex
	path        string
	index       string
	name        string
	timeQuantum TimeQuantum
	schema      *FrameSchema

	views map[string]*View

	// Row attribute storage and cache
	rowAttrStore *AttrStore

	broadcaster Broadcaster
	Stats       StatsClient

	// Frame settings.
	rowLabel       string
	cacheType      string
	inverseEnabled bool
	rangeEnabled   bool

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
		path:   path,
		index:  index,
		name:   name,
		schema: &FrameSchema{},

		views:        make(map[string]*View),
		rowAttrStore: NewAttrStore(filepath.Join(path, ".data")),

		broadcaster: NopBroadcaster,
		Stats:       NopStatsClient,

		rowLabel:       DefaultRowLabel,
		inverseEnabled: DefaultInverseEnabled,
		rangeEnabled:   DefaultRangeEnabled,
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
	f.mu.RLock()
	defer f.mu.RUnlock()

	var max uint64
	for _, view := range f.views {
		if view.name == ViewInverse {
			continue
		} else if viewMaxSlice := view.MaxSlice(); viewMaxSlice > max {
			max = viewMaxSlice
		}
	}
	return max
}

// MaxInverseSlice returns the max inverse slice in the frame.
func (f *Frame) MaxInverseSlice() uint64 {
	f.mu.RLock()
	defer f.mu.RUnlock()

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
	f.mu.RLock()
	v := f.rowLabel
	f.mu.RUnlock()
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

// RangeEnabled returns true if range fields can be stored on this frame.
func (f *Frame) RangeEnabled() bool {
	return f.rangeEnabled
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
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.options()
}

func (f *Frame) options() FrameOptions {
	return FrameOptions{
		RowLabel:       f.rowLabel,
		InverseEnabled: f.inverseEnabled,
		RangeEnabled:   f.rangeEnabled,
		CacheType:      f.cacheType,
		CacheSize:      f.cacheSize,
		TimeQuantum:    f.timeQuantum,
		Fields:         f.schema.Fields,
	}
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
		} else if err := f.loadSchema(); err != nil {
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
		f.rangeEnabled = DefaultRangeEnabled
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
	f.rangeEnabled = pb.RangeEnabled
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
	fo := f.options()
	buf, err := proto.Marshal(fo.Encode())
	if err != nil {
		return err
	}

	// Write to meta file.
	if err := ioutil.WriteFile(filepath.Join(f.path, ".meta"), buf, 0666); err != nil {
		return err
	}

	return nil
}

// loadSchema reads the schema for the frame.
func (f *Frame) loadSchema() error {
	buf, err := ioutil.ReadFile(filepath.Join(f.path, ".schema"))
	if os.IsNotExist(err) {
		f.schema = &FrameSchema{}
		return nil
	} else if err != nil {
		return err
	}

	var pb internal.FrameSchema
	if err := proto.Unmarshal(buf, &pb); err != nil {
		return err
	}
	f.schema = decodeFrameSchema(&pb)

	return nil
}

// saveSchema writes the current schema to disk.
func (f *Frame) saveSchema() error {
	if buf, err := proto.Marshal(encodeFrameSchema(f.schema)); err != nil {
		return err
	} else if err := ioutil.WriteFile(filepath.Join(f.path, ".schema"), buf, 0666); err != nil {
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
		if err := view.Close(); err != nil {
			return err
		}
	}
	f.views = make(map[string]*View)

	return nil
}

// Schema returns the frame's current schema.
func (f *Frame) Schema() *FrameSchema {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.schema
}

// Field returns a field from the schema by name.
func (f *Frame) Field(name string) *Field {
	for _, field := range f.Schema().Fields {
		if field.Name == name {
			return field
		}
	}
	return nil
}

// CreateField creates a new field on the schema.
func (f *Frame) CreateField(field *Field) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Ensure frame supports fields.
	if !f.RangeEnabled() {
		return ErrFrameFieldsNotAllowed
	}

	// Copy schema and append field.
	schema := f.schema.Clone()
	if err := schema.AddField(field); err != nil {
		return err
	}
	f.schema = schema
	f.saveSchema()
	return nil
}

// GetFields returns a list of all the fields in the frame.
func (f *Frame) GetFields() (*FrameSchema, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	// Ensure the frame supports fields.
	if !f.RangeEnabled() {
		return nil, ErrFrameFieldsNotAllowed
	}

	err := f.loadSchema()
	if err != nil {
		return nil, err
	}

	return f.schema, nil
}

// DeleteField deletes an existing field on the schema.
func (f *Frame) DeleteField(name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Ensure frame supports fields.
	if !f.RangeEnabled() {
		return ErrFrameFieldsNotAllowed
	}

	// Copy schema and remove field.
	schema := f.schema.Clone()
	if err := schema.DeleteField(name); err != nil {
		return err
	}
	f.schema = schema

	// Remove views.
	viewName := ViewFieldPrefix + name
	if view := f.views[viewName]; view != nil {
		delete(f.views, viewName)

		if err := view.Close(); err != nil {
			return err
		} else if err := os.RemoveAll(view.Path()); err != nil {
			return err
		}
	}

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
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.view(name)
}

func (f *Frame) view(name string) *View { return f.views[name] }

// Views returns a list of all views in the frame.
func (f *Frame) Views() []*View {
	f.mu.RLock()
	defer f.mu.RUnlock()

	other := make([]*View, 0, len(f.views))
	for _, view := range f.views {
		other = append(other, view)
	}
	return other
}

// RecalculateCaches recalculates caches on every view in the frame.
func (f *Frame) RecalculateCaches() {
	for _, view := range f.Views() {
		view.RecalculateCaches()
	}
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

// DeleteView removes the view from the frame.
func (f *Frame) DeleteView(name string) error {
	view := f.views[name]
	if view == nil {
		return ErrInvalidView
	}

	// Close data files before deletion.
	if err := view.Close(); err != nil {
		return err
	}

	// Delete view directory.
	if err := os.RemoveAll(view.Path()); err != nil {
		return err
	}

	delete(f.views, name)

	return nil
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

// FieldValue reads a field value for a column.
func (f *Frame) FieldValue(columnID uint64, name string) (value int64, exists bool, err error) {
	field := f.Field(name)
	if field == nil {
		return 0, false, ErrFieldNotFound
	}

	// Fetch target view.
	view := f.View(ViewFieldPrefix + name)
	if view == nil {
		return 0, false, nil
	}

	v, exists, err := view.FieldValue(columnID, field.BitDepth())
	if err != nil {
		return 0, false, err
	} else if !exists {
		return 0, false, nil
	}
	return int64(v) + field.Min, true, nil
}

// SetFieldValue sets a field value for a column.
func (f *Frame) SetFieldValue(columnID uint64, name string, value int64) (changed bool, err error) {
	// Fetch field and validate value.
	field := f.Field(name)
	if field == nil {
		return false, ErrFieldNotFound
	} else if value < field.Min {
		return false, ErrFieldValueTooLow
	} else if value > field.Max {
		return false, ErrFieldValueTooHigh
	}

	// Fetch target view.
	view, err := f.CreateViewIfNotExists(ViewFieldPrefix + name)
	if err != nil {
		return false, err
	}

	// Determine base value to store.
	baseValue := uint64(value - field.Min)

	return view.SetFieldValue(columnID, field.BitDepth(), baseValue)
}

// FieldSum returns the sum and count for a field.
// An optional filtering bitmap can be provided.
func (f *Frame) FieldSum(filter *Bitmap, name string) (sum, count int64, err error) {
	field := f.Field(name)
	if field == nil {
		return 0, 0, ErrFieldNotFound
	}

	view := f.View(ViewFieldPrefix + name)
	if view == nil {
		return 0, 0, nil
	}

	vsum, vcount, err := view.FieldSum(filter, field.BitDepth())
	if err != nil {
		return 0, 0, err
	}
	return int64(vsum) + (int64(vcount) * field.Min), int64(vcount), nil
}

func (f *Frame) FieldRange(name string, op pql.Token, predicate int64) (*Bitmap, error) {
	// Retrieve and validate field.
	field := f.Field(name)
	if field == nil {
		return nil, ErrFieldNotFound
	} else if predicate < field.Min || predicate > field.Max {
		return nil, nil
	}

	// Retrieve field's view.
	view := f.View(ViewFieldPrefix + name)
	if view == nil {
		return nil, nil
	}

	baseValue, outOfRange := field.BaseValue(op, predicate)
	if outOfRange {
		return NewBitmap(), nil
	}

	return view.FieldRange(op, field.BitDepth(), baseValue)
}

func (f *Frame) FieldRangeBetween(name string, predicateMin, predicateMax int64) (*Bitmap, error) {
	// Retrieve and validate field.
	field := f.Field(name)
	if field == nil {
		return nil, ErrFieldNotFound
	} else if predicateMin > predicateMax {
		return nil, ErrInvalidBetweenValue
	}

	// Retrieve field's view.
	view := f.View(ViewFieldPrefix + name)
	if view == nil {
		return nil, nil
	}

	baseValueMin, baseValueMax, outOfRange := field.BaseValueBetween(predicateMin, predicateMax)
	if outOfRange {
		return NewBitmap(), nil
	}

	return view.FieldRangeBetween(field.BitDepth(), baseValueMin, baseValueMax)
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

// ImportValue bulk imports range-encoded value data.
func (f *Frame) ImportValue(fieldName string, columnIDs []uint64, values []int64) error {
	// Verify that this frame is range-encoded.
	if !f.RangeEnabled() {
		return fmt.Errorf("Frame not RangeEnabled: %s", f.name)
	}

	viewName := ViewFieldPrefix + fieldName
	// Get the field so we know bitDepth.
	field := f.Field(fieldName)
	if field == nil {
		return fmt.Errorf("Field does not exist: %s", fieldName)
	}

	// Split import data by fragment.
	dataByFragment := make(map[importKey]importValueData)
	for i := range columnIDs {
		columnID, value := columnIDs[i], values[i]
		if int64(value) > field.Max {
			return fmt.Errorf("%v, columnID=%v, value=%v", ErrFieldValueTooHigh, columnID, value)
		} else if int64(value) < field.Min {
			return fmt.Errorf("%v, columnID=%v, value=%v", ErrFieldValueTooLow, columnID, value)
		}

		// Attach value to each field view.
		for _, name := range []string{viewName} {
			key := importKey{View: name, Slice: columnID / SliceWidth}
			data := dataByFragment[key]
			data.ColumnIDs = append(data.ColumnIDs, columnID)
			data.Values = append(data.Values, value)
			dataByFragment[key] = data
		}
	}

	// Import into each fragment.
	for key, data := range dataByFragment {

		// The view must already exist (i.e. we can't create it)
		// because we need to know bitDepth (based on min/max value).
		view, err := f.CreateViewIfNotExists(key.View)
		if err != nil {
			return err
		}

		frag, err := view.CreateFragmentIfNotExists(key.Slice)
		if err != nil {
			return err
		}

		baseValues := make([]uint64, len(data.Values))
		for i, value := range data.Values {
			baseValues[i] = uint64(value - field.Min)
		}

		if err := frag.ImportValue(data.ColumnIDs, baseValues, field.BitDepth()); err != nil {
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
	fo := f.options()
	return &internal.Frame{
		Name: f.name,
		Meta: fo.Encode(),
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
	RangeEnabled   bool        `json:"rangeEnabled,omitempty"`
	CacheType      string      `json:"cacheType,omitempty"`
	CacheSize      uint32      `json:"cacheSize,omitempty"`
	TimeQuantum    TimeQuantum `json:"timeQuantum,omitempty"`
	Fields         []*Field    `json:"fields,omitempty"`
}

// Encode converts o into its internal representation.
func (o *FrameOptions) Encode() *internal.FrameMeta {
	return &internal.FrameMeta{
		RowLabel:       o.RowLabel,
		InverseEnabled: o.InverseEnabled,
		RangeEnabled:   o.RangeEnabled,
		CacheType:      o.CacheType,
		CacheSize:      o.CacheSize,
		TimeQuantum:    string(o.TimeQuantum),
		Fields:         encodeFields(o.Fields),
	}
}

// FrameSchema represents the list of fields on a frame.
type FrameSchema struct {
	Fields []*Field
}

// Clone returns a copy of s.
func (s *FrameSchema) Clone() *FrameSchema {
	other := &FrameSchema{Fields: make([]*Field, len(s.Fields))}
	copy(other.Fields, s.Fields)
	return other
}

// HasField returns true if a field exists on the schema.
func (s *FrameSchema) HasField(name string) bool {
	for _, f := range s.Fields {
		if f.Name == name {
			return true
		}
	}
	return false
}

// AddField adds a single field to the schema.
func (s *FrameSchema) AddField(field *Field) error {
	if err := ValidateField(field); err != nil {
		return err
	} else if s.HasField(field.Name) {
		return ErrFieldExists
	}

	// Add field to list.
	s.Fields = append(s.Fields, field)

	// Sort fields by name.
	sort.Slice(s.Fields, func(i, j int) bool {
		return s.Fields[i].Name < s.Fields[j].Name
	})

	return nil
}

// DeleteField removes a single field from the schema.
func (s *FrameSchema) DeleteField(name string) error {
	for i, field := range s.Fields {
		if field.Name == name {
			copy(s.Fields[i:], s.Fields[i+1:])
			s.Fields, s.Fields[len(s.Fields)-1] = s.Fields[:len(s.Fields)-1], nil
			return nil
		}
	}
	return ErrFieldNotFound
}

func encodeFrameSchema(schema *FrameSchema) *internal.FrameSchema {
	if schema == nil {
		return nil
	}
	return &internal.FrameSchema{
		Fields: encodeFields(schema.Fields),
	}
}

func decodeFrameSchema(schema *internal.FrameSchema) *FrameSchema {
	if schema == nil {
		return nil
	}
	return &FrameSchema{
		Fields: decodeFields(schema.Fields),
	}
}

// List of field data types.
const (
	FieldTypeInt = "int"
)

func IsValidFieldType(v string) bool {
	switch v {
	case FieldTypeInt:
		return true
	default:
		return false
	}
}

// Field represents a range field on a frame.
type Field struct {
	Name string `json:"name,omitempty"`
	Type string `json:"type,omitempty"`
	Min  int64  `json:"min,omitempty"`
	Max  int64  `json:"max,omitempty"`
}

// BitDepth returns the number of bits required to store a value between min & max.
func (f *Field) BitDepth() uint {
	for i := uint(0); i < 63; i++ {
		if f.Max-f.Min < (1 << i) {
			return i
		}
	}
	return 63
}

// BaseValue adjusts the value to align with the range for Field for a certain
// operation type.
// Note: There is an edge case for GT and LT where this returns a baseValue
// that does not fully encompass the range.
// ex: Field.Min = 0, Field.Max = 1023
// BaseValue(LT, 2000) returns 1023, which will perform "LT 1023" and effectively
// exclude any columns with value = 1023.
// Note that in this case (because the range uses the full BitDepth 0 to 1023),
// we can't simply return 1024.
// In order to make this work, we effectively need to change the operator to LTE.
// Executor.executeFieldRangeSlice() takes this into account and returns
// `frag.FieldNotNull(field.BitDepth())` in such instances.
func (f *Field) BaseValue(op pql.Token, value int64) (baseValue uint64, outOfRange bool) {
	if op == pql.GT || op == pql.GTE {
		if value > f.Max {
			return baseValue, true
		} else if value > f.Min {
			baseValue = uint64(value - f.Min)
		}
	} else if op == pql.LT || op == pql.LTE {
		if value < f.Min {
			return baseValue, true
		} else if value > f.Max {
			baseValue = uint64(f.Max - f.Min)
		} else {
			baseValue = uint64(value - f.Min)
		}
	} else if op == pql.EQ || op == pql.NEQ {
		if value < f.Min || value > f.Max {
			return baseValue, true
		}
		baseValue = uint64(value - f.Min)
	}
	return baseValue, false
}

// BaseValueBetween adjusts the min/max value to align with the range for Field.
func (f *Field) BaseValueBetween(min, max int64) (baseValueMin, baseValueMax uint64, outOfRange bool) {
	if max < f.Min || min > f.Max {
		return baseValueMin, baseValueMax, true
	}
	// Adjust min/max to range.
	if min > f.Min {
		baseValueMin = uint64(min - f.Min)
	}
	// Make sure the high value of the BETWEEN does not exceed BitDepth.
	if max > f.Max {
		baseValueMax = uint64(f.Max - f.Min)
	} else if max > f.Min {
		baseValueMax = uint64(max - f.Min)
	}
	return baseValueMin, baseValueMax, false
}

func ValidateField(f *Field) error {
	if f.Name == "" {
		return ErrFieldNameRequired
	} else if !IsValidFieldType(f.Type) {
		return ErrInvalidFieldType
	} else if f.Min > f.Max {
		return ErrInvalidFieldRange
	}
	return nil
}

func encodeFields(a []*Field) []*internal.Field {
	if len(a) == 0 {
		return nil
	}
	other := make([]*internal.Field, len(a))
	for i := range a {
		other[i] = encodeField(a[i])
	}
	return other
}

func decodeFields(a []*internal.Field) []*Field {
	if len(a) == 0 {
		return nil
	}
	other := make([]*Field, len(a))
	for i := range a {
		other[i] = decodeField(a[i])
	}
	return other
}

func encodeField(f *Field) *internal.Field {
	if f == nil {
		return nil
	}
	return &internal.Field{
		Name: f.Name,
		Type: f.Type,
		Min:  int64(f.Min),
		Max:  int64(f.Max),
	}
}

func decodeField(f *internal.Field) *Field {
	if f == nil {
		return nil
	}
	return &Field{
		Name: f.Name,
		Type: f.Type,
		Min:  f.Min,
		Max:  f.Max,
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
	CacheTypeNone   = "none"
)

// IsValidCacheType returns true if v is a valid cache type.
func IsValidCacheType(v string) bool {
	switch v {
	case CacheTypeLRU, CacheTypeRanked, CacheTypeNone:
		return true
	default:
		return false
	}
}
