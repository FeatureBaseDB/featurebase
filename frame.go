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
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pilosa/pilosa/internal"
	"github.com/pilosa/pilosa/pql"
	"github.com/pkg/errors"
)

// Default frame settings.
const (
	DefaultCacheType = CacheTypeRanked

	// Default ranked frame cache
	DefaultCacheSize = 50000
)

// Frame represents a container for views.
type Frame struct {
	mu    sync.RWMutex
	path  string
	index string
	name  string

	views map[string]*View

	// Row attribute storage and cache
	rowAttrStore AttrStore

	broadcaster Broadcaster
	Stats       StatsClient

	// Frame options.
	cacheType   string
	cacheSize   uint32
	timeQuantum TimeQuantum
	fields      []*oField

	Logger Logger
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

		views: make(map[string]*View),

		rowAttrStore: NopAttrStore,

		broadcaster: NopBroadcaster,
		Stats:       NopStatsClient,

		cacheType: DefaultCacheType,
		cacheSize: DefaultCacheSize,
		//timeQuantum
		//fields

		Logger: NopLogger,
	}, nil
}

// Name returns the name the frame was initialized with.
func (f *Frame) Name() string { return f.name }

// Index returns the index name the frame was initialized with.
func (f *Frame) Index() string { return f.index }

// Path returns the path the frame was initialized with.
func (f *Frame) Path() string { return f.path }

// RowAttrStore returns the attribute storage.
func (f *Frame) RowAttrStore() AttrStore { return f.rowAttrStore }

// MaxSlice returns the max slice in the frame.
func (f *Frame) MaxSlice() uint64 {
	f.mu.RLock()
	defer f.mu.RUnlock()

	var max uint64
	for _, view := range f.views {
		if viewMaxSlice := view.MaxSlice(); viewMaxSlice > max {
			max = viewMaxSlice
		}
	}
	return max
}

// CacheType returns the caching mode for the frame.
func (f *Frame) CacheType() string {
	return f.cacheType
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
		return errors.Wrap(err, "saving")
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
		CacheType:   f.cacheType,
		CacheSize:   f.cacheSize,
		TimeQuantum: f.timeQuantum,
		Fields:      f.fields,
	}
}

// Open opens and initializes the frame.
func (f *Frame) Open() error {
	if err := func() error {
		// Ensure the frame's path exists.
		if err := os.MkdirAll(f.path, 0777); err != nil {
			return errors.Wrap(err, "creating frame dir")
		}

		if err := f.loadMeta(); err != nil {
			return errors.Wrap(err, "loading meta")
		}

		if err := f.openViews(); err != nil {
			return errors.Wrap(err, "opening views")
		}

		if err := f.rowAttrStore.Open(); err != nil {
			return errors.Wrap(err, "opening attrstore")
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
		return errors.Wrap(err, "opening view directory")
	}
	defer file.Close()

	fis, err := file.Readdir(0)
	if err != nil {
		return errors.Wrap(err, "reading directory")
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
		f.cacheType = DefaultCacheType
		f.cacheSize = DefaultCacheSize
		f.timeQuantum = ""
		//f.fields
		return nil
	} else if err != nil {
		return errors.Wrap(err, "reading meta")
	} else {
		if err := proto.Unmarshal(buf, &pb); err != nil {
			return errors.Wrap(err, "unmarshaling")
		}
	}

	// Copy metadata fields.
	f.cacheType = pb.CacheType
	if f.cacheType == "" {
		f.cacheType = DefaultCacheType
	}
	f.cacheSize = pb.CacheSize
	f.timeQuantum = TimeQuantum(pb.TimeQuantum)
	f.fields = decodeFields(pb.Fields)

	return nil
}

// saveMeta writes meta data for the frame.
func (f *Frame) saveMeta() error {
	// Marshal metadata.
	fo := f.options()
	buf, err := proto.Marshal(fo.Encode())
	if err != nil {
		return errors.Wrap(err, "marshaling")
	}

	// Write to meta file.
	if err := ioutil.WriteFile(filepath.Join(f.path, ".meta"), buf, 0666); err != nil {
		return errors.Wrap(err, "writing meta")
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

// Field returns a field by name.
func (f *Frame) Field(name string) *oField {
	f.mu.RLock()
	defer f.mu.RUnlock()
	for _, field := range f.fields {
		if field.Name == name {
			return field
		}
	}
	return nil
}

// Fields returns the fields on the frame.
func (f *Frame) Fields() []*oField {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.fields
}

// HasField returns true if a field exists on the frame.
func (f *Frame) HasField(name string) bool {
	for _, fld := range f.fields {
		if fld.Name == name {
			return true
		}
	}
	return false
}

// CreateField creates a new field on the frame.
func (f *Frame) CreateField(field *oField) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Append field.
	if err := f.addField(field); err != nil {
		return err
	}
	f.saveMeta()
	return nil
}

// addField adds a single field to fields.
func (f *Frame) addField(field *oField) error {
	if err := ValidateField(field); err != nil {
		return errors.Wrap(err, "validating field")
	} else if f.HasField(field.Name) {
		return ErrFieldExists
	}

	// Add field to list.
	f.fields = append(f.fields, field)

	// Sort fields by name.
	sort.Slice(f.fields, func(i, j int) bool {
		return f.fields[i].Name < f.fields[j].Name
	})

	return nil
}

// GetFields returns a list of all the fields in the frame.
func (f *Frame) GetFields() ([]*oField, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	err := f.loadMeta()
	if err != nil {
		return nil, errors.Wrap(err, "loading meta")
	}

	return f.fields, nil
}

// DeleteField deletes an existing field on the schema.
func (f *Frame) DeleteField(name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Remove field.
	if err := f.deleteField(name); err != nil {
		return err
	}

	// Remove views.
	viewName := ViewFieldPrefix + name
	if view := f.views[viewName]; view != nil {
		delete(f.views, viewName)

		if err := view.Close(); err != nil {
			return errors.Wrap(err, "closing view")
		} else if err := os.RemoveAll(view.Path()); err != nil {
			return errors.Wrap(err, "deleting directory")
		}
	}

	return nil
}

// deleteField removes a single field from fields.
func (f *Frame) deleteField(name string) error {
	for i, field := range f.fields {
		if field.Name == name {
			copy(f.fields[i:], f.fields[i+1:])
			f.fields, f.fields[len(f.fields)-1] = f.fields[:len(f.fields)-1], nil
			return nil
		}
	}
	return ErrFieldNotFound
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
		return errors.Wrap(err, "saving meta")
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

// viewNames returns a list of all views (as a string) in the frame.
func (f *Frame) viewNames() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	other := make([]string, 0, len(f.views))
	for viewName, _ := range f.views {
		other = append(other, viewName)
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
// Additionally, a CreateViewMessage is sent to the cluster.
func (f *Frame) CreateViewIfNotExists(name string) (*View, error) {

	view, created, err := f.createViewIfNotExistsBase(name)
	if err != nil {
		return nil, err
	}

	if created {
		// Broadcast view creation to the cluster.
		err = f.broadcaster.SendSync(
			&internal.CreateViewMessage{
				Index: f.index,
				Frame: f.name,
				View:  name,
			})
		if err != nil {
			return nil, errors.Wrap(err, "sending CreateView message")
		}
	}

	return view, nil
}

// createViewIfNotExistsBase returns the named view, creating it if necessary.
// The returned bool indicates whether the view was created or not.
func (f *Frame) createViewIfNotExistsBase(name string) (*View, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if view := f.views[name]; view != nil {
		return view, false, nil
	}

	view := f.newView(f.ViewPath(name), name)

	if err := view.Open(); err != nil {
		return nil, false, errors.Wrap(err, "opening view")
	}
	view.RowAttrStore = f.rowAttrStore
	f.views[view.Name()] = view

	return view, true, nil
}

func (f *Frame) newView(path, name string) *View {
	view := NewView(path, f.index, f.name, name, f.cacheSize)
	view.cacheType = f.cacheType
	view.Logger = f.Logger
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
		return errors.Wrap(err, "closing view")
	}

	// Delete view directory.
	if err := os.RemoveAll(view.Path()); err != nil {
		return errors.Wrap(err, "deleting directory")
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
		return changed, errors.Wrap(err, "creating view")
	}

	// Set non-time bit.
	if v, err := view.SetBit(rowID, colID); err != nil {
		return changed, errors.Wrap(err, "setting on view")
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
			return changed, errors.Wrapf(err, "creating view %s", subname)
		}

		if c, err := view.SetBit(rowID, colID); err != nil {
			return changed, errors.Wrapf(err, "setting on view %s", subname)
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
		return changed, errors.Wrap(err, "creating view")
	}

	// Clear non-time bit.
	if v, err := view.ClearBit(rowID, colID); err != nil {
		return changed, errors.Wrap(err, "setting on view")
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
			return changed, errors.Wrapf(err, "creating view %s", subname)
		}

		if c, err := view.ClearBit(rowID, colID); err != nil {
			return changed, errors.Wrapf(err, "setting on view %s", subname)
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
		return false, errors.Wrap(err, "creating view")
	}

	// Determine base value to store.
	baseValue := uint64(value - field.Min)

	return view.SetFieldValue(columnID, field.BitDepth(), baseValue)
}

// FieldSum returns the sum and count for a field.
// An optional filtering row can be provided.
func (f *Frame) FieldSum(filter *Row, name string) (sum, count int64, err error) {
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

// FieldMin returns the min for a field.
// An optional filtering row can be provided.
func (f *Frame) FieldMin(filter *Row, name string) (min, count int64, err error) {
	field := f.Field(name)
	if field == nil {
		return 0, 0, ErrFieldNotFound
	}

	view := f.View(ViewFieldPrefix + name)
	if view == nil {
		return 0, 0, nil
	}

	vmin, vcount, err := view.FieldMin(filter, field.BitDepth())
	if err != nil {
		return 0, 0, err
	}
	return int64(vmin) + field.Min, int64(vcount), nil
}

// FieldMax returns the max for a field.
// An optional filtering row can be provided.
func (f *Frame) FieldMax(filter *Row, name string) (max, count int64, err error) {
	field := f.Field(name)
	if field == nil {
		return 0, 0, ErrFieldNotFound
	}

	view := f.View(ViewFieldPrefix + name)
	if view == nil {
		return 0, 0, nil
	}

	vmax, vcount, err := view.FieldMax(filter, field.BitDepth())
	if err != nil {
		return 0, 0, err
	}
	return int64(vmax) + field.Min, int64(vcount), nil
}

func (f *Frame) FieldRange(name string, op pql.Token, predicate int64) (*Row, error) {
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
		return NewRow(), nil
	}

	return view.FieldRange(op, field.BitDepth(), baseValue)
}

func (f *Frame) FieldRangeBetween(name string, predicateMin, predicateMax int64) (*Row, error) {
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
		return NewRow(), nil
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
		rowID, columnID := rowIDs[i], columnIDs[i]
		var timestamp *time.Time
		if len(timestamps) > i {
			timestamp = timestamps[i]
		}

		var standard []string
		if timestamp == nil {
			standard = []string{ViewStandard}
		} else {
			standard = ViewsByTime(ViewStandard, *timestamp, q)
			// In order to match the logic of `SetBit()`, we want bits
			// with timestamps to write to both time and standard views.
			standard = append(standard, ViewStandard)
		}

		// Attach bit to each standard view.
		for _, name := range standard {
			key := importKey{View: name, Slice: columnID / SliceWidth}
			data := dataByFragment[key]
			data.RowIDs = append(data.RowIDs, rowID)
			data.ColumnIDs = append(data.ColumnIDs, columnID)
			dataByFragment[key] = data
		}
	}

	// Import into each fragment.
	for key, data := range dataByFragment {
		view, err := f.CreateViewIfNotExists(key.View)
		if err != nil {
			return errors.Wrap(err, "creating view")
		}

		frag, err := view.CreateFragmentIfNotExists(key.Slice)
		if err != nil {
			return errors.Wrap(err, "creating view")
		}

		if err := frag.Import(data.RowIDs, data.ColumnIDs); err != nil {
			return err
		}
	}

	return nil
}

// ImportValue bulk imports range-encoded value data.
func (f *Frame) ImportValue(fieldName string, columnIDs []uint64, values []int64) error {
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
			return errors.Wrap(err, "creating view")
		}

		frag, err := view.CreateFragmentIfNotExists(key.Slice)
		if err != nil {
			return errors.Wrap(err, "creating fragment")
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
		Name:  f.name,
		Meta:  fo.Encode(),
		Views: f.viewNames(),
	}
}

type frameSlice []*Frame

func (p frameSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p frameSlice) Len() int           { return len(p) }
func (p frameSlice) Less(i, j int) bool { return p[i].Name() < p[j].Name() }

// FrameInfo represents schema information for a frame.
type FrameInfo struct {
	Name    string       `json:"name"`
	Options FrameOptions `json:"options"`
	Views   []*ViewInfo  `json:"views,omitempty"`
}

type frameInfoSlice []*FrameInfo

func (p frameInfoSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p frameInfoSlice) Len() int           { return len(p) }
func (p frameInfoSlice) Less(i, j int) bool { return p[i].Name < p[j].Name }

// FrameOptions represents options to set when initializing a frame.
type FrameOptions struct {
	CacheType   string      `json:"cacheType,omitempty"`
	CacheSize   uint32      `json:"cacheSize,omitempty"`
	TimeQuantum TimeQuantum `json:"timeQuantum,omitempty"`
	Fields      []*oField   `json:"fields,omitempty"`
}

// Encode converts o into its internal representation.
func (o *FrameOptions) Encode() *internal.FrameMeta {
	return encodeFrameOptions(o)
}

func encodeFrameOptions(o *FrameOptions) *internal.FrameMeta {
	if o == nil {
		return nil
	}
	return &internal.FrameMeta{
		CacheType:   o.CacheType,
		CacheSize:   o.CacheSize,
		TimeQuantum: string(o.TimeQuantum),
		Fields:      encodeFields(o.Fields),
	}
}

func decodeFrameOptions(options *internal.FrameMeta) *FrameOptions {
	if options == nil {
		return nil
	}
	return &FrameOptions{
		CacheType:   options.CacheType,
		CacheSize:   options.CacheSize,
		TimeQuantum: TimeQuantum(options.TimeQuantum),
		Fields:      decodeFields(options.Fields),
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

// oField represents a range field on a frame.
type oField struct {
	Name string `json:"name,omitempty"`
	Type string `json:"type,omitempty"`
	Min  int64  `json:"min,omitempty"`
	Max  int64  `json:"max,omitempty"`
}

// BitDepth returns the number of bits required to store a value between min & max.
func (f *oField) BitDepth() uint {
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
func (f *oField) BaseValue(op pql.Token, value int64) (baseValue uint64, outOfRange bool) {
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
func (f *oField) BaseValueBetween(min, max int64) (baseValueMin, baseValueMax uint64, outOfRange bool) {
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

func ValidateField(f *oField) error {
	if f.Name == "" {
		return ErrFieldNameRequired
	} else if !IsValidFieldType(f.Type) {
		return ErrInvalidFieldType
	} else if f.Min > f.Max {
		return ErrInvalidFieldRange
	}
	return nil
}

func encodeFields(a []*oField) []*internal.Field {
	if len(a) == 0 {
		return nil
	}
	other := make([]*internal.Field, len(a))
	for i := range a {
		other[i] = encodeField(a[i])
	}
	return other
}

func decodeFields(a []*internal.Field) []*oField {
	if len(a) == 0 {
		return nil
	}
	other := make([]*oField, len(a))
	for i := range a {
		other[i] = decodeField(a[i])
	}
	return other
}

func encodeField(f *oField) *internal.Field {
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

func decodeField(f *internal.Field) *oField {
	if f == nil {
		return nil
	}
	return &oField{
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
