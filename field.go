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

// Default field settings.
const (
	DefaultFieldType = FieldTypeSet

	defaultCacheType = CacheTypeRanked

	// Default ranked field cache
	defaultCacheSize = 50000
)

// Field types.
const (
	FieldTypeSet  = "set"
	FieldTypeInt  = "int"
	FieldTypeTime = "time"
)

// Field represents a container for views.
type Field struct {
	mu    sync.RWMutex
	path  string
	index string
	name  string

	views map[string]*View

	// Row attribute storage and cache
	rowAttrStore AttrStore

	broadcaster Broadcaster
	Stats       StatsClient

	// Field options.
	options FieldOptions

	bsiGroups []*bsiGroup

	Logger Logger
}

// FieldOption is a functional option type for pilosa.Fielde.
type FieldOption func(f *Field) error

// TODO: break these out into separate Options (not a FieldOptions object)
func OptFieldFieldOptions(o FieldOptions) FieldOption {
	return func(f *Field) error {
		f.options = o
		return nil
	}
}

// NewField returns a new instance of field.
func NewField(path, index, name string, opts ...FieldOption) (*Field, error) {
	err := validateName(name)
	if err != nil {
		return nil, err
	}

	f := &Field{
		path:  path,
		index: index,
		name:  name,

		views: make(map[string]*View),

		rowAttrStore: NopAttrStore,

		broadcaster: NopBroadcaster,
		Stats:       NopStatsClient,

		options: FieldOptions{
			Type:      DefaultFieldType,
			CacheType: defaultCacheType,
			CacheSize: defaultCacheSize,
		},

		Logger: NopLogger,
	}

	for _, opt := range opts {
		err := opt(f)
		if err != nil {
			return nil, errors.Wrap(err, "applying option")
		}
	}

	return f, nil
}

// Name returns the name the field was initialized with.
func (f *Field) Name() string { return f.name }

// Index returns the index name the field was initialized with.
func (f *Field) Index() string { return f.index }

// Path returns the path the field was initialized with.
func (f *Field) Path() string { return f.path }

// RowAttrStore returns the attribute storage.
func (f *Field) RowAttrStore() AttrStore { return f.rowAttrStore }

// MaxSlice returns the max slice in the field.
func (f *Field) MaxSlice() uint64 {
	f.mu.RLock()
	defer f.mu.RUnlock()

	var max uint64
	for _, view := range f.views {
		if viewMaxSlice := view.calculateMaxSlice(); viewMaxSlice > max {
			max = viewMaxSlice
		}
	}
	return max
}

// Type returns the field type.
func (f *Field) Type() string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.options.Type
}

// CacheType returns the caching mode for the field.
func (f *Field) CacheType() string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.options.CacheType
}

// SetCacheSize sets the cache size for ranked fames. Persists to meta file on update.
// defaults to DefaultCacheSize 50000
func (f *Field) SetCacheSize(v uint32) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Ignore if no change occurred.
	if v == 0 || f.options.CacheSize == v {
		return nil
	}

	// Persist meta data to disk on change.
	f.options.CacheSize = v
	if err := f.saveMeta(); err != nil {
		return errors.Wrap(err, "saving")
	}

	return nil
}

// CacheSize returns the ranked field cache size.
func (f *Field) CacheSize() uint32 {
	f.mu.RLock()
	v := f.options.CacheSize
	f.mu.RUnlock()
	return v
}

// Options returns all options for this field.
func (f *Field) Options() FieldOptions {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.options
}

// Open opens and initializes the field.
func (f *Field) Open() error {
	if err := func() error {
		// Ensure the field's path exists.
		if err := os.MkdirAll(f.path, 0777); err != nil {
			return errors.Wrap(err, "creating field dir")
		}

		if err := f.loadMeta(); err != nil {
			return errors.Wrap(err, "loading meta")
		}

		// Apply the field options loaded from meta.
		if err := f.applyOptions(f.options); err != nil {
			return errors.Wrap(err, "applying options")
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

// openViews opens and initializes the views inside the field.
func (f *Field) openViews() error {
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
		if err := view.open(); err != nil {
			return fmt.Errorf("opening view: view=%s, err=%s", view.name, err)
		}
		view.RowAttrStore = f.rowAttrStore
		f.views[view.name] = view
	}

	return nil
}

// loadMeta reads meta data for the field, if any.
func (f *Field) loadMeta() error {
	var pb internal.FieldOptions

	// Read data from meta file.
	buf, err := ioutil.ReadFile(filepath.Join(f.path, ".meta"))
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return errors.Wrap(err, "reading meta")
	} else {
		if err := proto.Unmarshal(buf, &pb); err != nil {
			return errors.Wrap(err, "unmarshaling")
		}
	}

	// Copy metadata fields.
	f.options.Type = pb.Type
	f.options.CacheType = pb.CacheType
	f.options.CacheSize = pb.CacheSize
	f.options.Min = pb.Min
	f.options.Max = pb.Max
	f.options.TimeQuantum = TimeQuantum(pb.TimeQuantum)
	f.options.Keys = pb.Keys

	return nil
}

// saveMeta writes meta data for the field.
func (f *Field) saveMeta() error {
	// Marshal metadata.
	fo := f.options
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

// applyOptions configures the field based on opt.
func (f *Field) applyOptions(opt FieldOptions) error {
	switch opt.Type {
	case FieldTypeSet, "":
		f.options.Type = FieldTypeSet
		if opt.CacheType != "" {
			f.options.CacheType = opt.CacheType
		}
		if opt.CacheSize != 0 {
			f.options.CacheSize = opt.CacheSize
		}
		f.options.Min = 0
		f.options.Max = 0
		f.options.TimeQuantum = ""
		f.options.Keys = opt.Keys
	case FieldTypeInt:
		f.options.Type = opt.Type
		f.options.CacheType = CacheTypeNone
		f.options.CacheSize = 0
		f.options.Min = opt.Min
		f.options.Max = opt.Max
		f.options.TimeQuantum = ""
		f.options.Keys = opt.Keys

		// Create new bsiGroup.
		bsig := &bsiGroup{
			Name: f.name,
			Type: bsiGroupTypeInt,
			Min:  opt.Min,
			Max:  opt.Max,
		}
		// Validate bsiGroup.
		if err := bsig.validate(); err != nil {
			return err
		}
		if err := f.createBSIGroup(bsig); err != nil {
			return errors.Wrap(err, "creating bsigroup")
		}
	case FieldTypeTime:
		f.options.Type = opt.Type
		f.options.CacheType = CacheTypeNone
		f.options.CacheSize = 0
		f.options.Min = 0
		f.options.Max = 0
		f.options.Keys = opt.Keys
		// Set the time quantum.
		if err := f.SetTimeQuantum(opt.TimeQuantum); err != nil {
			f.Close()
			return errors.Wrap(err, "setting time quantum")
		}
	default:
		return errors.New("invalid field type")
	}

	return nil
}

// Close closes the field and its views.
func (f *Field) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Close the attribute store.
	if f.rowAttrStore != nil {
		_ = f.rowAttrStore.Close()
	}

	// Close all views.
	for _, view := range f.views {
		if err := view.close(); err != nil {
			return err
		}
	}
	f.views = make(map[string]*View)

	return nil
}

// Keys returns true if the field uses string keys.
func (f *Field) Keys() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.options.Keys
}

// bsiGroup returns a bsiGroup by name.
func (f *Field) bsiGroup(name string) *bsiGroup {
	f.mu.RLock()
	defer f.mu.RUnlock()
	for _, bsig := range f.bsiGroups {
		if bsig.Name == name {
			return bsig
		}
	}
	return nil
}

// hasBSIGroup returns true if a bsiGroup exists on the field.
func (f *Field) hasBSIGroup(name string) bool {
	for _, bsig := range f.bsiGroups {
		if bsig.Name == name {
			return true
		}
	}
	return false
}

// createBSIGroup creates a new bsiGroup on the field.
func (f *Field) createBSIGroup(bsig *bsiGroup) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Append bsiGroup.
	if err := f.addBSIGroup(bsig); err != nil {
		return err
	}
	f.saveMeta()
	return nil
}

// addBSIGroup adds a single bsiGroup to bsiGroups.
func (f *Field) addBSIGroup(bsig *bsiGroup) error {
	if err := bsig.validate(); err != nil {
		return errors.Wrap(err, "validating bsigroup")
	} else if f.hasBSIGroup(bsig.Name) {
		return ErrBSIGroupExists
	}

	// Add bsiGroup to list.
	f.bsiGroups = append(f.bsiGroups, bsig)

	// Sort bsiGroups by name.
	sort.Slice(f.bsiGroups, func(i, j int) bool {
		return f.bsiGroups[i].Name < f.bsiGroups[j].Name
	})

	return nil
}

// deleteBSIGroupAndView deletes an existing bsiGroup on the schema.
func (f *Field) deleteBSIGroupAndView(name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Remove bsiGroup.
	if err := f.deleteBSIGroup(name); err != nil {
		return err
	}

	// Remove views.
	viewName := viewBSIGroupPrefix + name
	if view := f.views[viewName]; view != nil {
		delete(f.views, viewName)

		if err := view.close(); err != nil {
			return errors.Wrap(err, "closing view")
		} else if err := os.RemoveAll(view.path); err != nil {
			return errors.Wrap(err, "deleting directory")
		}
	}

	return nil
}

// deleteBSIGroup removes a single bsiGroup from bsiGroups.
func (f *Field) deleteBSIGroup(name string) error {
	for i, bsig := range f.bsiGroups {
		if bsig.Name == name {
			copy(f.bsiGroups[i:], f.bsiGroups[i+1:])
			f.bsiGroups, f.bsiGroups[len(f.bsiGroups)-1] = f.bsiGroups[:len(f.bsiGroups)-1], nil
			return nil
		}
	}
	return ErrBSIGroupNotFound
}

// TimeQuantum returns the time quantum for the field.
func (f *Field) TimeQuantum() TimeQuantum {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.options.TimeQuantum
}

// SetTimeQuantum sets the time quantum for the field.
func (f *Field) SetTimeQuantum(q TimeQuantum) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Validate input.
	if !q.Valid() {
		return ErrInvalidTimeQuantum
	}

	// Update value on field.
	f.options.TimeQuantum = q

	// Persist meta data to disk.
	if err := f.saveMeta(); err != nil {
		return errors.Wrap(err, "saving meta")
	}

	return nil
}

// ViewPath returns the path to a view in the field.
func (f *Field) ViewPath(name string) string {
	return filepath.Join(f.path, "views", name)
}

// View returns a view in the field by name.
func (f *Field) View(name string) *View {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.view(name)
}

func (f *Field) view(name string) *View { return f.views[name] }

// Views returns a list of all views in the field.
func (f *Field) Views() []*View {
	f.mu.RLock()
	defer f.mu.RUnlock()

	other := make([]*View, 0, len(f.views))
	for _, view := range f.views {
		other = append(other, view)
	}
	return other
}

// viewNames returns a list of all views (as a string) in the field.
func (f *Field) viewNames() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	other := make([]string, 0, len(f.views))
	for viewName, _ := range f.views {
		other = append(other, viewName)
	}
	return other
}

// RecalculateCaches recalculates caches on every view in the field.
func (f *Field) RecalculateCaches() {
	for _, view := range f.Views() {
		view.recalculateCaches()
	}
}

// CreateViewIfNotExists returns the named view, creating it if necessary.
// Additionally, a CreateViewMessage is sent to the cluster.
func (f *Field) CreateViewIfNotExists(name string) (*View, error) {

	view, created, err := f.createViewIfNotExistsBase(name)
	if err != nil {
		return nil, err
	}

	if created {
		// Broadcast view creation to the cluster.
		err = f.broadcaster.SendSync(
			&internal.CreateViewMessage{
				Index: f.index,
				Field: f.name,
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
func (f *Field) createViewIfNotExistsBase(name string) (*View, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if view := f.views[name]; view != nil {
		return view, false, nil
	}

	view := f.newView(f.ViewPath(name), name)

	if err := view.open(); err != nil {
		return nil, false, errors.Wrap(err, "opening view")
	}
	view.RowAttrStore = f.rowAttrStore
	f.views[view.name] = view

	return view, true, nil
}

func (f *Field) newView(path, name string) *View {
	view := NewView(path, f.index, f.name, name, f.options.CacheSize)
	view.cacheType = f.options.CacheType
	view.Logger = f.Logger
	view.RowAttrStore = f.rowAttrStore
	view.stats = f.Stats.WithTags(fmt.Sprintf("view:%s", name))
	view.broadcaster = f.broadcaster
	return view
}

// DeleteView removes the view from the field.
func (f *Field) DeleteView(name string) error {
	view := f.views[name]
	if view == nil {
		return ErrInvalidView
	}

	// Close data files before deletion.
	if err := view.close(); err != nil {
		return errors.Wrap(err, "closing view")
	}

	// Delete view directory.
	if err := os.RemoveAll(view.path); err != nil {
		return errors.Wrap(err, "deleting directory")
	}

	delete(f.views, name)

	return nil
}

// Row returns a row of the standard view.
func (f *Field) Row(rowID uint64) (*Row, error) {
	if f.Type() != FieldTypeSet {
		return nil, errors.Errorf("row method unsupported for field type: %s", f.Type())
	}
	view := f.View(ViewStandard)
	if view == nil {
		return nil, ErrInvalidView
	}
	return view.row(rowID), nil
}

// ViewRow returns a row for a view and slice.
// TODO: unexport this with views (it's only used in tests).
func (f *Field) ViewRow(viewName string, rowID uint64) (*Row, error) {
	view := f.View(viewName)
	if view == nil {
		return nil, ErrInvalidView
	}
	return view.row(rowID), nil
}

// SetBit sets a bit on a view within the field.
func (f *Field) SetBit(name string, rowID, colID uint64, t *time.Time) (changed bool, err error) {
	// Validate view name.
	if !isValidView(name) {
		return false, ErrInvalidView
	}

	// Retrieve view. Exit if it doesn't exist.
	view, err := f.CreateViewIfNotExists(name)
	if err != nil {
		return changed, errors.Wrap(err, "creating view")
	}

	// Set non-time bit.
	if v, err := view.setBit(rowID, colID); err != nil {
		return changed, errors.Wrap(err, "setting on view")
	} else if v {
		changed = v
	}

	// Exit early if no timestamp is specified.
	if t == nil {
		return changed, nil
	}

	// If a timestamp is specified then set bits across all views for the quantum.
	for _, subname := range viewsByTime(name, *t, f.TimeQuantum()) {
		view, err := f.CreateViewIfNotExists(subname)
		if err != nil {
			return changed, errors.Wrapf(err, "creating view %s", subname)
		}

		if c, err := view.setBit(rowID, colID); err != nil {
			return changed, errors.Wrapf(err, "setting on view %s", subname)
		} else if c {
			changed = true
		}
	}

	return changed, nil
}

// ClearBit clears a bit within the field.
func (f *Field) ClearBit(name string, rowID, colID uint64, t *time.Time) (changed bool, err error) {
	// Validate view name.
	if !isValidView(name) {
		return false, ErrInvalidView
	}

	// Retrieve view. Exit if it doesn't exist.
	view, err := f.CreateViewIfNotExists(name)
	if err != nil {
		return changed, errors.Wrap(err, "creating view")
	}

	// Clear non-time bit.
	if v, err := view.clearBit(rowID, colID); err != nil {
		return changed, errors.Wrap(err, "clearing on view")
	} else if v {
		changed = v
	}

	// Exit early if no timestamp is specified.
	if t == nil {
		return changed, nil
	}

	// If a timestamp is specified then clear bits across all views for the quantum.
	for _, subname := range viewsByTime(name, *t, f.TimeQuantum()) {
		view, err := f.CreateViewIfNotExists(subname)
		if err != nil {
			return changed, errors.Wrapf(err, "creating view %s", subname)
		}

		if c, err := view.clearBit(rowID, colID); err != nil {
			return changed, errors.Wrapf(err, "clearing on view %s", subname)
		} else if c {
			changed = true
		}
	}

	return changed, nil
}

// Value reads a field value for a column.
func (f *Field) Value(columnID uint64) (value int64, exists bool, err error) {
	bsig := f.bsiGroup(f.name)
	if bsig == nil {
		return 0, false, ErrBSIGroupNotFound
	}

	// Fetch target view.
	view := f.View(viewBSIGroupPrefix + f.name)
	if view == nil {
		return 0, false, nil
	}

	v, exists, err := view.value(columnID, bsig.BitDepth())
	if err != nil {
		return 0, false, err
	} else if !exists {
		return 0, false, nil
	}
	return int64(v) + bsig.Min, true, nil
}

// SetValue sets a field value for a column.
func (f *Field) SetValue(columnID uint64, value int64) (changed bool, err error) {
	// Fetch bsiGroup and validate value.
	bsig := f.bsiGroup(f.name)
	if bsig == nil {
		return false, ErrBSIGroupNotFound
	} else if value < bsig.Min {
		return false, ErrBSIGroupValueTooLow
	} else if value > bsig.Max {
		return false, ErrBSIGroupValueTooHigh
	}

	// Fetch target view.
	view, err := f.CreateViewIfNotExists(viewBSIGroupPrefix + f.name)
	if err != nil {
		return false, errors.Wrap(err, "creating view")
	}

	// Determine base value to store.
	baseValue := uint64(value - bsig.Min)

	return view.setValue(columnID, bsig.BitDepth(), baseValue)
}

// Sum returns the sum and count for a field.
// An optional filtering row can be provided.
func (f *Field) Sum(filter *Row, name string) (sum, count int64, err error) {
	bsig := f.bsiGroup(name)
	if bsig == nil {
		return 0, 0, ErrBSIGroupNotFound
	}

	view := f.View(viewBSIGroupPrefix + name)
	if view == nil {
		return 0, 0, nil
	}

	vsum, vcount, err := view.sum(filter, bsig.BitDepth())
	if err != nil {
		return 0, 0, err
	}
	return int64(vsum) + (int64(vcount) * bsig.Min), int64(vcount), nil
}

// Min returns the min for a field.
// An optional filtering row can be provided.
func (f *Field) Min(filter *Row, name string) (min, count int64, err error) {
	bsig := f.bsiGroup(name)
	if bsig == nil {
		return 0, 0, ErrBSIGroupNotFound
	}

	view := f.View(viewBSIGroupPrefix + name)
	if view == nil {
		return 0, 0, nil
	}

	vmin, vcount, err := view.min(filter, bsig.BitDepth())
	if err != nil {
		return 0, 0, err
	}
	return int64(vmin) + bsig.Min, int64(vcount), nil
}

// Max returns the max for a field.
// An optional filtering row can be provided.
func (f *Field) Max(filter *Row, name string) (max, count int64, err error) {
	bsig := f.bsiGroup(name)
	if bsig == nil {
		return 0, 0, ErrBSIGroupNotFound
	}

	view := f.View(viewBSIGroupPrefix + name)
	if view == nil {
		return 0, 0, nil
	}

	vmax, vcount, err := view.max(filter, bsig.BitDepth())
	if err != nil {
		return 0, 0, err
	}
	return int64(vmax) + bsig.Min, int64(vcount), nil
}

func (f *Field) Range(name string, op pql.Token, predicate int64) (*Row, error) {
	// Retrieve and validate bsiGroup.
	bsig := f.bsiGroup(name)
	if bsig == nil {
		return nil, ErrBSIGroupNotFound
	} else if predicate < bsig.Min || predicate > bsig.Max {
		return nil, nil
	}

	// Retrieve bsiGroup's view.
	view := f.View(viewBSIGroupPrefix + name)
	if view == nil {
		return nil, nil
	}

	baseValue, outOfRange := bsig.baseValue(op, predicate)
	if outOfRange {
		return NewRow(), nil
	}

	return view.rangeOp(op, bsig.BitDepth(), baseValue)
}

func (f *Field) RangeBetween(name string, predicateMin, predicateMax int64) (*Row, error) {
	// Retrieve and validate bsiGroup.
	bsig := f.bsiGroup(name)
	if bsig == nil {
		return nil, ErrBSIGroupNotFound
	} else if predicateMin > predicateMax {
		return nil, ErrInvalidBetweenValue
	}

	// Retrieve bsiGroup's view.
	view := f.View(viewBSIGroupPrefix + name)
	if view == nil {
		return nil, nil
	}

	baseValueMin, baseValueMax, outOfRange := bsig.baseValueBetween(predicateMin, predicateMax)
	if outOfRange {
		return NewRow(), nil
	}

	return view.rangeBetween(bsig.BitDepth(), baseValueMin, baseValueMax)
}

// Import bulk imports data.
func (f *Field) Import(rowIDs, columnIDs []uint64, timestamps []*time.Time) error {
	// Determine quantum if timestamps are set.
	q := f.TimeQuantum()
	if hasTime(timestamps) && q == "" {
		return errors.New("time quantum not set in field")
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
			standard = viewsByTime(ViewStandard, *timestamp, q)
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

		if err := frag.bulkImport(data.RowIDs, data.ColumnIDs); err != nil {
			return err
		}
	}

	return nil
}

// ImportValue bulk imports range-encoded value data.
func (f *Field) ImportValue(columnIDs []uint64, values []int64) error {
	viewName := viewBSIGroupPrefix + f.name
	// Get the bsiGroup so we know bitDepth.
	bsig := f.bsiGroup(f.name)
	if bsig == nil {
		return errors.Wrap(ErrBSIGroupNotFound, f.name)
	}

	// Split import data by fragment.
	dataByFragment := make(map[importKey]importValueData)
	for i := range columnIDs {
		columnID, value := columnIDs[i], values[i]
		if int64(value) > bsig.Max {
			return fmt.Errorf("%v, columnID=%v, value=%v", ErrBSIGroupValueTooHigh, columnID, value)
		} else if int64(value) < bsig.Min {
			return fmt.Errorf("%v, columnID=%v, value=%v", ErrBSIGroupValueTooLow, columnID, value)
		}

		// Attach value to each bsiGroup view.
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
			baseValues[i] = uint64(value - bsig.Min)
		}

		if err := frag.importValue(data.ColumnIDs, baseValues, bsig.BitDepth()); err != nil {
			return err
		}
	}

	return nil
}

// encodeFields converts a into its internal representation.
func encodeFields(a []*Field) []*internal.Field {
	other := make([]*internal.Field, len(a))
	for i := range a {
		other[i] = encodeField(a[i])
	}
	return other
}

// encodeField converts f into its internal representation.
func encodeField(f *Field) *internal.Field {
	fo := f.options
	return &internal.Field{
		Name:  f.name,
		Meta:  fo.Encode(),
		Views: f.viewNames(),
	}
}

type fieldSlice []*Field

func (p fieldSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p fieldSlice) Len() int           { return len(p) }
func (p fieldSlice) Less(i, j int) bool { return p[i].Name() < p[j].Name() }

// FieldInfo represents schema information for a field.
type FieldInfo struct {
	Name    string       `json:"name"`
	Options FieldOptions `json:"options"`
	Views   []*ViewInfo  `json:"views,omitempty"`
}

type fieldInfoSlice []*FieldInfo

func (p fieldInfoSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p fieldInfoSlice) Len() int           { return len(p) }
func (p fieldInfoSlice) Less(i, j int) bool { return p[i].Name < p[j].Name }

// FieldOptions represents options to set when initializing a field.
type FieldOptions struct {
	Type        string      `json:"type,omitempty"`
	CacheType   string      `json:"cacheType,omitempty"`
	CacheSize   uint32      `json:"cacheSize,omitempty"`
	Min         int64       `json:"min,omitempty"`
	Max         int64       `json:"max,omitempty"`
	TimeQuantum TimeQuantum `json:"timeQuantum,omitempty"`
	Keys        bool        `json:"keys,omitempty"`
}

// Validate ensures that FieldOption values are valid.
func (o *FieldOptions) Validate() error {
	switch o.Type {
	case FieldTypeSet, "":
		// TODO: cacheType, cacheSize validation
	case FieldTypeInt:
		if o.Min > o.Max {
			return ErrInvalidBSIGroupRange
		}
	case FieldTypeTime:
		if o.TimeQuantum == "" || !o.TimeQuantum.Valid() {
			return ErrInvalidTimeQuantum
		}
	default:
		return errors.New("invalid field type")
	}
	return nil
}

// Encode converts o into its internal representation.
func (o *FieldOptions) Encode() *internal.FieldOptions {
	return encodeFieldOptions(o)
}

func encodeFieldOptions(o *FieldOptions) *internal.FieldOptions {
	if o == nil {
		return nil
	}
	return &internal.FieldOptions{
		Type:        o.Type,
		CacheType:   o.CacheType,
		CacheSize:   o.CacheSize,
		Min:         o.Min,
		Max:         o.Max,
		TimeQuantum: string(o.TimeQuantum),
		Keys:        o.Keys,
	}
}

func decodeFieldOptions(options *internal.FieldOptions) *FieldOptions {
	if options == nil {
		return nil
	}
	return &FieldOptions{
		Type:        options.Type,
		CacheType:   options.CacheType,
		CacheSize:   options.CacheSize,
		Min:         options.Min,
		Max:         options.Max,
		TimeQuantum: TimeQuantum(options.TimeQuantum),
		Keys:        options.Keys,
	}
}

// List of bsiGroup types.
const (
	bsiGroupTypeInt = "int"
)

func isValidBSIGroupType(v string) bool {
	switch v {
	case bsiGroupTypeInt:
		return true
	default:
		return false
	}
}

// bsiGroup represents a group of range-encoded rows on a field.
type bsiGroup struct {
	Name string `json:"name,omitempty"`
	Type string `json:"type,omitempty"`
	Min  int64  `json:"min,omitempty"`
	Max  int64  `json:"max,omitempty"`
}

// BitDepth returns the number of bits required to store a value between min & max.
func (b *bsiGroup) BitDepth() uint {
	for i := uint(0); i < 63; i++ {
		if b.Max-b.Min < (1 << i) {
			return i
		}
	}
	return 63
}

// baseValue adjusts the value to align with the range for Field for a certain
// operation type.
// Note: There is an edge case for GT and LT where this returns a baseValue
// that does not fully encompass the range.
// ex: Field.Min = 0, Field.Max = 1023
// baseValue(LT, 2000) returns 1023, which will perform "LT 1023" and effectively
// exclude any columns with value = 1023.
// Note that in this case (because the range uses the full BitDepth 0 to 1023),
// we can't simply return 1024.
// In order to make this work, we effectively need to change the operator to LTE.
// Executor.executeBSIGroupRangeSlice() takes this into account and returns
// `frag.FieldNotNull(bsig.BitDepth())` in such instances.
func (b *bsiGroup) baseValue(op pql.Token, value int64) (baseValue uint64, outOfRange bool) {
	if op == pql.GT || op == pql.GTE {
		if value > b.Max {
			return baseValue, true
		} else if value > b.Min {
			baseValue = uint64(value - b.Min)
		}
	} else if op == pql.LT || op == pql.LTE {
		if value < b.Min {
			return baseValue, true
		} else if value > b.Max {
			baseValue = uint64(b.Max - b.Min)
		} else {
			baseValue = uint64(value - b.Min)
		}
	} else if op == pql.EQ || op == pql.NEQ {
		if value < b.Min || value > b.Max {
			return baseValue, true
		}
		baseValue = uint64(value - b.Min)
	}
	return baseValue, false
}

// baseValueBetween adjusts the min/max value to align with the range for Field.
func (b *bsiGroup) baseValueBetween(min, max int64) (baseValueMin, baseValueMax uint64, outOfRange bool) {
	if max < b.Min || min > b.Max {
		return baseValueMin, baseValueMax, true
	}
	// Adjust min/max to range.
	if min > b.Min {
		baseValueMin = uint64(min - b.Min)
	}
	// Make sure the high value of the BETWEEN does not exceed BitDepth.
	if max > b.Max {
		baseValueMax = uint64(b.Max - b.Min)
	} else if max > b.Min {
		baseValueMax = uint64(max - b.Min)
	}
	return baseValueMin, baseValueMax, false
}

func (b *bsiGroup) validate() error {
	if b.Name == "" {
		return ErrBSIGroupNameRequired
	} else if !isValidBSIGroupType(b.Type) {
		return ErrInvalidBSIGroupType
	} else if b.Min > b.Max {
		return ErrInvalidBSIGroupRange
	}
	return nil
}

func encodeBSIGroup(b *bsiGroup) *internal.BSIGroup {
	if b == nil {
		return nil
	}
	return &internal.BSIGroup{
		Name: b.Name,
		Type: b.Type,
		Min:  int64(b.Min),
		Max:  int64(b.Max),
	}
}

func decodeBSIGroup(b *internal.BSIGroup) *bsiGroup {
	if b == nil {
		return nil
	}
	return &bsiGroup{
		Name: b.Name,
		Type: b.Type,
		Min:  b.Min,
		Max:  b.Max,
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

// isValidCacheType returns true if v is a valid cache type.
func isValidCacheType(v string) bool {
	switch v {
	case CacheTypeLRU, CacheTypeRanked, CacheTypeNone:
		return true
	default:
		return false
	}
}
