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
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pilosa/pilosa/v2/internal"
	"github.com/pilosa/pilosa/v2/logger"
	"github.com/pilosa/pilosa/v2/pql"
	"github.com/pilosa/pilosa/v2/roaring"
	"github.com/pilosa/pilosa/v2/stats"
	"github.com/pilosa/pilosa/v2/tracing"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

// Default field settings.
const (
	DefaultFieldType = FieldTypeSet

	DefaultCacheType = CacheTypeRanked

	// Default ranked field cache
	DefaultCacheSize = 50000

	bitsPerWord = 32 << (^uint(0) >> 63) // either 32 or 64
	maxInt      = 1<<(bitsPerWord-1) - 1 // either 1<<31 - 1 or 1<<63 - 1

)

// Field types.
const (
	FieldTypeSet   = "set"
	FieldTypeInt   = "int"
	FieldTypeTime  = "time"
	FieldTypeMutex = "mutex"
	FieldTypeBool  = "bool"
)

// Field represents a container for views.
type Field struct {
	mu    sync.RWMutex
	path  string
	index string
	name  string

	viewMap map[string]*view

	// Row attribute storage and cache
	rowAttrStore AttrStore

	// Key/ID translation store.
	translateStore TranslateStore

	broadcaster broadcaster
	Stats       stats.StatsClient

	// Field options.
	options FieldOptions

	bsiGroups []*bsiGroup

	// Shards with data on any node in the cluster, according to this node.
	remoteAvailableShards *roaring.Bitmap

	logger logger.Logger

	snapshotQueue chan *fragment

	// Instantiates new translation store on open.
	OpenTranslateStore OpenTranslateStoreFunc
}

// FieldOption is a functional option type for pilosa.fieldOptions.
type FieldOption func(fo *FieldOptions) error

// OptFieldKeys is a functional option on FieldOptions
// used to specify whether keys are used for this field.
func OptFieldKeys() FieldOption {
	return func(fo *FieldOptions) error {
		fo.Keys = true
		return nil
	}
}

// OptFieldTypeDefault is a functional option on FieldOptions
// used to set the field type and cache setting to the default values.
func OptFieldTypeDefault() FieldOption {
	return func(fo *FieldOptions) error {
		if fo.Type != "" {
			return errors.Errorf("field type is already set to: %s", fo.Type)
		}
		fo.Type = FieldTypeSet
		fo.CacheType = DefaultCacheType
		fo.CacheSize = DefaultCacheSize
		return nil
	}
}

// OptFieldTypeSet is a functional option on FieldOptions
// used to specify the field as being type `set` and to
// provide any respective configuration values.
func OptFieldTypeSet(cacheType string, cacheSize uint32) FieldOption {
	return func(fo *FieldOptions) error {
		if fo.Type != "" {
			return errors.Errorf("field type is already set to: %s", fo.Type)
		}
		fo.Type = FieldTypeSet
		fo.CacheType = cacheType
		fo.CacheSize = cacheSize
		return nil
	}
}

// OptFieldTypeInt is a functional option on FieldOptions
// used to specify the field as being type `int` and to
// provide any respective configuration values.
func OptFieldTypeInt(min, max int64) FieldOption {
	return func(fo *FieldOptions) error {
		if fo.Type != "" {
			return errors.Errorf("field type is already set to: %s", fo.Type)
		}
		if min > max {
			return errors.New("int field min cannot be greater than max")
		}
		fo.Type = FieldTypeInt
		fo.Min = min
		fo.Max = max
		fo.Base = bsiBase(min, max)
		return nil
	}
}

// OptFieldTypeTime is a functional option on FieldOptions
// used to specify the field as being type `time` and to
// provide any respective configuration values.
// Pass true to skip creation of the standard view.
func OptFieldTypeTime(timeQuantum TimeQuantum, opt ...bool) FieldOption {
	return func(fo *FieldOptions) error {
		if fo.Type != "" {
			return errors.Errorf("field type is already set to: %s", fo.Type)
		}
		if !timeQuantum.Valid() {
			return ErrInvalidTimeQuantum
		}
		fo.Type = FieldTypeTime
		fo.TimeQuantum = timeQuantum
		fo.NoStandardView = len(opt) >= 1 && opt[0]
		return nil
	}
}

// OptFieldTypeMutex is a functional option on FieldOptions
// used to specify the field as being type `mutex` and to
// provide any respective configuration values.
func OptFieldTypeMutex(cacheType string, cacheSize uint32) FieldOption {
	return func(fo *FieldOptions) error {
		if fo.Type != "" {
			return errors.Errorf("field type is already set to: %s", fo.Type)
		}
		fo.Type = FieldTypeMutex
		fo.CacheType = cacheType
		fo.CacheSize = cacheSize
		return nil
	}
}

// OptFieldTypeBool is a functional option on FieldOptions
// used to specify the field as being type `bool` and to
// provide any respective configuration values.
func OptFieldTypeBool() FieldOption {
	return func(fo *FieldOptions) error {
		if fo.Type != "" {
			return errors.Errorf("field type is already set to: %s", fo.Type)
		}
		fo.Type = FieldTypeBool
		return nil
	}
}

// NewField returns a new instance of field.
func NewField(path, index, name string, opts FieldOption) (*Field, error) {
	err := validateName(name)
	if err != nil {
		return nil, errors.Wrap(err, "validating name")
	}

	return newField(path, index, name, opts)
}

// newField returns a new instance of field (without name validation).
func newField(path, index, name string, opts FieldOption) (*Field, error) {
	// Apply functional option.
	fo := FieldOptions{}
	err := opts(&fo)
	if err != nil {
		return nil, errors.Wrap(err, "applying option")
	}

	f := &Field{
		path:  path,
		index: index,
		name:  name,

		viewMap: make(map[string]*view),

		rowAttrStore: nopStore,

		broadcaster: NopBroadcaster,
		Stats:       stats.NopStatsClient,

		options: applyDefaultOptions(fo),

		remoteAvailableShards: roaring.NewBitmap(),

		logger: logger.NopLogger,

		OpenTranslateStore: OpenInMemTranslateStore,
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

// TranslateStore returns the underlying translation store for the field.
func (f *Field) TranslateStore() TranslateStore { return f.translateStore }

// AvailableShards returns a bitmap of shards that contain data.
func (f *Field) AvailableShards() *roaring.Bitmap {
	f.mu.RLock()
	defer f.mu.RUnlock()

	b := f.remoteAvailableShards.Clone()
	for _, view := range f.viewMap {
		b = b.Union(view.availableShards())
	}
	return b
}

// AddRemoteAvailableShards merges the set of available shards into the current known set
// and saves the set to a file.
func (f *Field) AddRemoteAvailableShards(b *roaring.Bitmap) error {
	f.mergeRemoteAvailableShards(b)
	// Save the updated bitmap to the data store.
	return f.saveAvailableShards()
}

// mergeRemoteAvailableShards merges the set of available shards into the current known set.
func (f *Field) mergeRemoteAvailableShards(b *roaring.Bitmap) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.remoteAvailableShards = f.remoteAvailableShards.Union(b)
}

// loadAvailableShards reads remoteAvailableShards data for the field, if any.
func (f *Field) loadAvailableShards() error {
	bm := roaring.NewBitmap()
	// Read data from meta file.
	path := filepath.Join(f.path, ".available.shards")
	buf, err := ioutil.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return errors.Wrap(err, "reading available shards")
	} else {
		if err := bm.UnmarshalBinary(buf); err != nil {
			return errors.Wrap(err, "unmarshaling")
		}
	}
	// Merge bitmap from file into field.
	f.mergeRemoteAvailableShards(bm)

	return nil
}

// saveAvailableShards writes remoteAvailableShards data for the field.
func (f *Field) saveAvailableShards() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.unprotectedSaveAvailableShards()
}

func (f *Field) unprotectedSaveAvailableShards() error {
	path := filepath.Join(f.path, ".available.shards")
	// Create a temporary file to save to.
	tempPath := path + tempExt

	// Open or create file.
	file, err := os.OpenFile(tempPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return errors.Wrap(err, "opening temporary available shards file")
	}
	defer file.Close()

	// Write available shards to file.
	bw := bufio.NewWriter(file)
	if _, err = f.remoteAvailableShards.WriteTo(bw); err != nil {
		return errors.Wrap(err, "writing bitmap to buffer")
	}
	bw.Flush()

	// Move snapshot to data file location.
	if err := os.Rename(tempPath, path); err != nil {
		return fmt.Errorf("rename snapshot: %s", err)
	}

	return nil
}

// RemoveAvailableShard removes a shard from the bitmap cache.
//
// NOTE: This can be overridden on the next sync so all nodes should be updated.
func (f *Field) RemoveAvailableShard(v uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	b := f.remoteAvailableShards.Clone()
	if _, err := b.Remove(v); err != nil {
		return err
	}
	f.remoteAvailableShards = b

	return f.unprotectedSaveAvailableShards()
}

// Type returns the field type.
func (f *Field) Type() string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.options.Type
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
	if err := func() (err error) {
		// Ensure the field's path exists.
		f.logger.Debugf("ensure field path exists: %s", f.path)
		if err := os.MkdirAll(f.path, 0777); err != nil {
			return errors.Wrap(err, "creating field dir")
		}

		f.logger.Debugf("load meta file for index/field: %s/%s", f.index, f.name)
		if err := f.loadMeta(); err != nil {
			return errors.Wrap(err, "loading meta")
		}

		f.logger.Debugf("load available shards for index/field: %s/%s", f.index, f.name)
		if err := f.loadAvailableShards(); err != nil {
			return errors.Wrap(err, "loading available shards")
		}

		// Apply the field options loaded from meta.
		f.logger.Debugf("apply options for index/field: %s/%s", f.index, f.name)
		if err := f.applyOptions(f.options); err != nil {
			return errors.Wrap(err, "applying options")
		}

		f.logger.Debugf("open views for index/field: %s/%s", f.index, f.name)
		if err := f.openViews(); err != nil {
			return errors.Wrap(err, "opening views")
		}

		f.logger.Debugf("open row attribute store for index/field: %s/%s", f.index, f.name)
		if err := f.rowAttrStore.Open(); err != nil {
			return errors.Wrap(err, "opening attrstore")
		}

		// Instantiate & open translation store.
		if f.translateStore, err = f.OpenTranslateStore(filepath.Join(f.path, "keys"), f.index, f.name); err != nil {
			return errors.Wrap(err, "opening translate store")
		}

		return nil
	}(); err != nil {
		f.Close()
		return err
	}

	f.logger.Debugf("successfully opened field index/field: %s/%s", f.index, f.name)
	return nil
}

var fieldQueue = make(chan struct{}, 16)

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
	eg, ctx := errgroup.WithContext(context.Background())
	var mu sync.Mutex

fileLoop:
	for _, loopFi := range fis {
		select {
		case <-ctx.Done():
			break fileLoop
		default:
			fi := loopFi
			if !fi.IsDir() {
				continue
			}
			fieldQueue <- struct{}{}
			eg.Go(func() error {
				defer func() {
					<-fieldQueue
				}()
				name := filepath.Base(fi.Name())
				f.logger.Debugf("open index/field/view: %s/%s/%s", f.index, f.name, fi.Name())
				view := f.newView(f.viewPath(name), name)
				if err := view.open(); err != nil {
					return fmt.Errorf("opening view: view=%s, err=%s", view.name, err)
				}

				// Automatically upgrade BSI v1 fragments if they exist & reopen view.
				if bsig := f.bsiGroup(f.name); bsig != nil {
					if ok, err := upgradeViewBSIv2(view, bsig.BitDepth); err != nil {
						return errors.Wrap(err, "upgrade view bsi v2")
					} else if ok {
						if err := view.close(); err != nil {
							return errors.Wrap(err, "closing upgraded view")
						}
						view = f.newView(f.viewPath(name), name)
						if err := view.open(); err != nil {
							return fmt.Errorf("re-opening view: view=%s, err=%s", view.name, err)
						}
					}
				}

				view.rowAttrStore = f.rowAttrStore
				f.logger.Debugf("add index/field/view to field.viewMap: %s/%s/%s", f.index, f.name, view.name)
				mu.Lock()
				f.viewMap[view.name] = view
				mu.Unlock()
				return nil
			})
		}
	}

	return eg.Wait()
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

	// Initialize "base" to "min" when upgrading from v1 BSI format.
	if pb.BitDepth == 0 {
		pb.Base = bsiBase(pb.Min, pb.Max)
		pb.BitDepth = uint64(bitDepthInt64(pb.Max - pb.Min))
		if pb.BitDepth == 0 {
			pb.BitDepth = 1
		}
	}

	// Copy metadata fields.
	f.options.Type = pb.Type
	f.options.CacheType = pb.CacheType
	f.options.CacheSize = pb.CacheSize
	f.options.Min = pb.Min
	f.options.Max = pb.Max
	f.options.Base = pb.Base
	f.options.BitDepth = uint(pb.BitDepth)
	f.options.TimeQuantum = TimeQuantum(pb.TimeQuantum)
	f.options.Keys = pb.Keys
	f.options.NoStandardView = pb.NoStandardView

	return nil
}

// saveMeta writes meta data for the field.
func (f *Field) saveMeta() error {
	path := filepath.Join(f.path, ".meta")
	// Create a temporary file to marshal to.
	tempPath := f.path + tempExt

	// Marshal metadata.
	fo := f.options
	buf, err := proto.Marshal(fo.encode())
	if err != nil {
		return errors.Wrap(err, "marshaling")
	}

	// Write to meta file.
	if err := ioutil.WriteFile(tempPath, buf, 0666); err != nil {
		return errors.Wrap(err, "writing meta")
	}

	// Move temp file to data file location.
	if err := os.Rename(tempPath, path); err != nil {
		return fmt.Errorf("rename temp: %s", err)
	}

	return nil
}

// applyOptions configures the field based on opt.
func (f *Field) applyOptions(opt FieldOptions) error {
	switch opt.Type {
	case FieldTypeSet, FieldTypeMutex, "":
		fldType := opt.Type
		if fldType == "" {
			fldType = FieldTypeSet
		}
		f.options.Type = fldType
		if opt.CacheType != "" {
			f.options.CacheType = opt.CacheType
		}
		if opt.CacheType == CacheTypeNone {
			f.options.CacheSize = 0
		} else if opt.CacheSize != 0 {
			f.options.CacheSize = opt.CacheSize
		}
		f.options.Min = 0
		f.options.Max = 0
		f.options.Base = 0
		f.options.BitDepth = 0
		f.options.TimeQuantum = ""
		f.options.Keys = opt.Keys
	case FieldTypeInt:
		f.options.Type = opt.Type
		f.options.CacheType = CacheTypeNone
		f.options.CacheSize = 0
		f.options.Min = opt.Min
		f.options.Max = opt.Max
		f.options.Base = opt.Base
		f.options.BitDepth = opt.BitDepth
		f.options.TimeQuantum = ""
		f.options.Keys = opt.Keys

		// Create new bsiGroup.
		bsig := &bsiGroup{
			Name:     f.name,
			Type:     bsiGroupTypeInt,
			Min:      opt.Min,
			Max:      opt.Max,
			Base:     opt.Base,
			BitDepth: opt.BitDepth,
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
		f.options.Base = 0
		f.options.BitDepth = 0
		f.options.Keys = opt.Keys
		f.options.NoStandardView = opt.NoStandardView
		// Set the time quantum.
		if err := f.setTimeQuantum(opt.TimeQuantum); err != nil {
			f.Close()
			return errors.Wrap(err, "setting time quantum")
		}
	case FieldTypeBool:
		f.options.Type = FieldTypeBool
		f.options.CacheType = CacheTypeNone
		f.options.CacheSize = 0
		f.options.Min = 0
		f.options.Max = 0
		f.options.Base = 0
		f.options.BitDepth = 0
		f.options.TimeQuantum = ""
		f.options.Keys = false
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
	for _, view := range f.viewMap {
		if err := view.close(); err != nil {
			return err
		}
	}
	f.viewMap = make(map[string]*view)

	if f.translateStore != nil {
		if err := f.translateStore.Close(); err != nil {
			return err
		}
	}

	return nil
}

// keys returns true if the field uses string keys.
func (f *Field) keys() bool {
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
	if err := f.saveMeta(); err != nil {
		return errors.Wrap(err, "saving")
	}
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

// TimeQuantum returns the time quantum for the field.
func (f *Field) TimeQuantum() TimeQuantum {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.options.TimeQuantum
}

// setTimeQuantum sets the time quantum for the field.
func (f *Field) setTimeQuantum(q TimeQuantum) error {
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

// RowTime gets the row at the particular time with the granularity specified by
// the quantum.
func (f *Field) RowTime(rowID uint64, time time.Time, quantum string) (*Row, error) {
	if !TimeQuantum(quantum).Valid() {
		return nil, ErrInvalidTimeQuantum
	}
	viewname := viewsByTime(viewStandard, time, TimeQuantum(quantum[len(quantum)-1:]))[0]
	view := f.view(viewname)
	if view == nil {
		return nil, errors.Errorf("view with quantum %v not found.", quantum)
	}
	return view.row(rowID), nil
}

// viewPath returns the path to a view in the field.
func (f *Field) viewPath(name string) string {
	return filepath.Join(f.path, "views", name)
}

// view returns a view in the field by name.
func (f *Field) view(name string) *view {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.unprotectedView(name)
}

func (f *Field) unprotectedView(name string) *view { return f.viewMap[name] }

// views returns a list of all views in the field.
func (f *Field) views() []*view {
	f.mu.RLock()
	defer f.mu.RUnlock()

	other := make([]*view, 0, len(f.viewMap))
	for _, view := range f.viewMap {
		other = append(other, view)
	}
	return other
}

// recalculateCaches recalculates caches on every view in the field.
func (f *Field) recalculateCaches() {
	for _, view := range f.views() {
		view.recalculateCaches()
	}
}

// createViewIfNotExists returns the named view, creating it if necessary.
// Additionally, a CreateViewMessage is sent to the cluster.
func (f *Field) createViewIfNotExists(name string) (*view, error) {
	view, created, err := f.createViewIfNotExistsBase(name)
	if err != nil {
		return nil, err
	}

	if created {
		// Broadcast view creation to the cluster.
		err = f.broadcaster.SendSync(
			&CreateViewMessage{
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
func (f *Field) createViewIfNotExistsBase(name string) (*view, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if view := f.viewMap[name]; view != nil {
		return view, false, nil
	}
	view := f.newView(f.viewPath(name), name)

	if err := view.open(); err != nil {
		return nil, false, errors.Wrap(err, "opening view")
	}
	view.rowAttrStore = f.rowAttrStore
	f.viewMap[view.name] = view

	return view, true, nil
}

func (f *Field) newView(path, name string) *view {
	view := newView(path, f.index, f.name, name, f.options)
	view.logger = f.logger
	view.rowAttrStore = f.rowAttrStore
	view.stats = f.Stats
	view.broadcaster = f.broadcaster
	view.snapshotQueue = f.snapshotQueue
	return view
}

// deleteView removes the view from the field.
func (f *Field) deleteView(name string) error {
	view := f.viewMap[name]
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

	delete(f.viewMap, name)

	return nil
}

// Row returns a row of the standard view.
// It seems this method is only being used by the test
// package, and the fact that it's only allowed on
// `set` fields is odd. This may be considered for
// deprecation in a future version.
func (f *Field) Row(rowID uint64) (*Row, error) {
	if f.Type() != FieldTypeSet {
		return nil, errors.Errorf("row method unsupported for field type: %s", f.Type())
	}
	view := f.view(viewStandard)
	if view == nil {
		return nil, ErrInvalidView
	}
	return view.row(rowID), nil
}

// SetBit sets a bit on a view within the field.
func (f *Field) SetBit(rowID, colID uint64, t *time.Time) (changed bool, err error) {
	viewName := viewStandard
	if !f.options.NoStandardView {
		// Retrieve view. Exit if it doesn't exist.
		view, err := f.createViewIfNotExists(viewName)
		if err != nil {
			return changed, errors.Wrap(err, "creating view")
		}

		// Set non-time bit.
		if v, err := view.setBit(rowID, colID); err != nil {
			return changed, errors.Wrap(err, "setting on view")
		} else if v {
			changed = v
		}
	}

	// Exit early if no timestamp is specified.
	if t == nil {
		return changed, nil
	}

	// If a timestamp is specified then set bits across all views for the quantum.
	for _, subname := range viewsByTime(viewName, *t, f.TimeQuantum()) {
		view, err := f.createViewIfNotExists(subname)
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
func (f *Field) ClearBit(rowID, colID uint64) (changed bool, err error) {
	viewName := viewStandard

	// Retrieve view. Exit if it doesn't exist.
	view, present := f.viewMap[viewName]
	if !present {
		return changed, errors.Wrap(err, "clearing missing view")

	}

	// Clear non-time bit.
	if v, err := view.clearBit(rowID, colID); err != nil {
		return changed, errors.Wrap(err, "clearing on view")
	} else if v {
		changed = v
	}
	if len(f.viewMap) == 1 { // assuming no time views
		return changed, nil
	}
	lastViewNameSize := 0
	level := 0
	skipAbove := maxInt
	for _, view := range f.allTimeViewsSortedByQuantum() {
		if lastViewNameSize < len(view.name) {
			level++
		} else if lastViewNameSize > len(view.name) {
			level--
		}
		if level < skipAbove {
			if changed, err = view.clearBit(rowID, colID); err != nil {
				return changed, errors.Wrapf(err, "clearing on view %s", view.name)
			}
			if !changed {
				skipAbove = level + 1
			} else {
				skipAbove = maxInt
			}
		}
		lastViewNameSize = len(view.name)
	}

	return changed, nil
}

func groupCompare(a, b string, offset int) (lt, eq bool) {
	if len(a) > offset {
		a = a[:offset]
	}
	if len(b) > offset {
		b = b[:offset]
	}
	v := strings.Compare(a, b)
	return v < 0, v == 0
}

func (f *Field) allTimeViewsSortedByQuantum() (me []*view) {
	me = make([]*view, len(f.viewMap))
	prefix := viewStandard + "_"
	offset := len(viewStandard) + 1
	i := 0
	for _, v := range f.viewMap {
		if len(v.name) > offset && strings.Compare(v.name[:offset], prefix) == 0 { // skip non-time views
			me[i] = v
			i++
		}
	}
	me = me[:i]
	year := strings.Index(me[0].name, "_") + 4
	month := year + 2
	day := month + 2
	sort.Slice(me, func(i, j int) (lt bool) {
		var eq bool
		// group by quantum from year to hour
		if lt, eq = groupCompare(me[i].name, me[j].name, year); eq {
			if lt, eq = groupCompare(me[i].name, me[j].name, month); eq {
				if lt, eq = groupCompare(me[i].name, me[j].name, day); eq {
					lt = strings.Compare(me[i].name, me[j].name) > 0
				}
			}
		}
		return lt
	})
	return me
}

// Value reads a field value for a column.
func (f *Field) Value(columnID uint64) (value int64, exists bool, err error) {
	bsig := f.bsiGroup(f.name)
	if bsig == nil {
		return 0, false, ErrBSIGroupNotFound
	}

	// Fetch target view.
	view := f.view(viewBSIGroupPrefix + f.name)
	if view == nil {
		return 0, false, nil
	}

	v, exists, err := view.value(columnID, bsig.BitDepth)
	if err != nil {
		return 0, false, err
	} else if !exists {
		return 0, false, nil
	}
	return int64(v) + bsig.Base, true, nil
}

// SetValue sets a field value for a column.
func (f *Field) SetValue(columnID uint64, value int64) (changed bool, err error) {
	// Fetch bsiGroup & validate min/max.
	bsig := f.bsiGroup(f.name)
	if bsig == nil {
		return false, ErrBSIGroupNotFound
	} else if value < bsig.Min {
		return false, ErrBSIGroupValueTooLow
	} else if value > bsig.Max {
		return false, ErrBSIGroupValueTooHigh
	}

	// Determine base value to store.
	baseValue := int64(value - bsig.Base)
	requiredBitDepth := bitDepthInt64(baseValue)

	// Increase bit depth value if the unsigned value is greater.
	if requiredBitDepth > bsig.BitDepth {
		if err := func() error {
			f.mu.Lock()
			defer f.mu.Unlock()

			uvalue := uint64(baseValue)
			if value < 0 {
				uvalue = uint64(-baseValue)
			}
			bitDepth := bitDepth(uvalue)

			bsig.BitDepth = bitDepth
			f.options.BitDepth = bitDepth
			return f.saveMeta()
		}(); err != nil {
			return false, errors.Wrap(err, "increasing bsi max")
		}
	}

	// Fetch target view.
	view, err := f.createViewIfNotExists(viewBSIGroupPrefix + f.name)
	if err != nil {
		return false, errors.Wrap(err, "creating view")
	}

	return view.setValue(columnID, bsig.BitDepth, baseValue)
}

// Sum returns the sum and count for a field.
// An optional filtering row can be provided.
func (f *Field) Sum(filter *Row, name string) (sum, count int64, err error) {
	bsig := f.bsiGroup(name)
	if bsig == nil {
		return 0, 0, ErrBSIGroupNotFound
	}

	view := f.view(viewBSIGroupPrefix + name)
	if view == nil {
		return 0, 0, nil
	}

	vsum, vcount, err := view.sum(filter, bsig.BitDepth)
	if err != nil {
		return 0, 0, err
	}
	return int64(vsum) + (int64(vcount) * bsig.Base), int64(vcount), nil
}

// Min returns the min for a field.
// An optional filtering row can be provided.
func (f *Field) Min(filter *Row, name string) (min, count int64, err error) {
	bsig := f.bsiGroup(name)
	if bsig == nil {
		return 0, 0, ErrBSIGroupNotFound
	}

	view := f.view(viewBSIGroupPrefix + name)
	if view == nil {
		return 0, 0, nil
	}

	vmin, vcount, err := view.min(filter, bsig.BitDepth)
	if err != nil {
		return 0, 0, err
	}
	return int64(vmin) + bsig.Base, int64(vcount), nil
}

// Max returns the max for a field.
// An optional filtering row can be provided.
func (f *Field) Max(filter *Row, name string) (max, count int64, err error) {
	bsig := f.bsiGroup(name)
	if bsig == nil {
		return 0, 0, ErrBSIGroupNotFound
	}

	view := f.view(viewBSIGroupPrefix + name)
	if view == nil {
		return 0, 0, nil
	}

	vmax, vcount, err := view.max(filter, bsig.BitDepth)
	if err != nil {
		return 0, 0, err
	}
	return int64(vmax) + bsig.Base, int64(vcount), nil
}

// Range performs a conditional operation on Field.
func (f *Field) Range(name string, op pql.Token, predicate int64) (*Row, error) {
	// Retrieve and validate bsiGroup.
	bsig := f.bsiGroup(name)
	if bsig == nil {
		return nil, ErrBSIGroupNotFound
	} else if predicate < bsig.Min || predicate > bsig.Max {
		return nil, nil
	}

	// Retrieve bsiGroup's view.
	view := f.view(viewBSIGroupPrefix + name)
	if view == nil {
		return nil, nil
	}

	baseValue, outOfRange := bsig.baseValue(op, predicate)
	if outOfRange {
		return NewRow(), nil
	}

	return view.rangeOp(op, bsig.BitDepth, baseValue)
}

// Import bulk imports data.
func (f *Field) Import(rowIDs, columnIDs []uint64, timestamps []*time.Time, opts ...ImportOption) error {

	// Set up import options.
	options := &ImportOptions{}
	for _, opt := range opts {
		err := opt(options)
		if err != nil {
			return errors.Wrap(err, "applying option")
		}
	}

	// Determine quantum if timestamps are set.
	q := f.TimeQuantum()
	if hasTime(timestamps) {
		if q == "" {
			return errors.New("time quantum not set in field")
		} else if options.Clear {
			return errors.New("import clear is not supported with timestamps")
		}
	}

	fieldType := f.Type()

	// Split import data by fragment.
	dataByFragment := make(map[importKey]importData)
	for i := range rowIDs {
		rowID, columnID := rowIDs[i], columnIDs[i]

		// Bool-specific data validation.
		if fieldType == FieldTypeBool && rowID > 1 {
			return errors.New("bool field imports only support values 0 and 1")
		}

		var timestamp *time.Time
		if len(timestamps) > i {
			timestamp = timestamps[i]
		}

		var standard []string
		if timestamp == nil {
			standard = []string{viewStandard}
		} else {
			standard = viewsByTime(viewStandard, *timestamp, q)
			if !f.options.NoStandardView {
				// In order to match the logic of `SetBit()`, we want bits
				// with timestamps to write to both time and standard views.
				standard = append(standard, viewStandard)
			}
		}

		// Attach bit to each standard view.
		for _, name := range standard {
			key := importKey{View: name, Shard: columnID / ShardWidth}
			data := dataByFragment[key]
			data.RowIDs = append(data.RowIDs, rowID)
			data.ColumnIDs = append(data.ColumnIDs, columnID)
			dataByFragment[key] = data
		}
	}

	// Import into each fragment.
	for key, data := range dataByFragment {
		view, err := f.createViewIfNotExists(key.View)
		if err != nil {
			return errors.Wrap(err, "creating view")
		}

		frag, err := view.CreateFragmentIfNotExists(key.Shard)
		if err != nil {
			return errors.Wrap(err, "creating fragment")
		}

		if err := frag.bulkImport(data.RowIDs, data.ColumnIDs, options); err != nil {
			return err
		}
	}

	return nil
}

// importValue bulk imports range-encoded value data.
func (f *Field) importValue(columnIDs []uint64, values []int64, options *ImportOptions) error {
	viewName := viewBSIGroupPrefix + f.name
	// Get the bsiGroup so we know bitDepth.
	bsig := f.bsiGroup(f.name)
	if bsig == nil {
		return errors.Wrap(ErrBSIGroupNotFound, f.name)
	}

	// We want to determine the required bit depth, in case the field doesn't
	// have as many bits currently as would be needed to represent these values,
	// but only if the values are in-range for the field.
	var min, max int64
	if len(values) > 0 {
		min, max = values[0], values[0]
	}

	// Split import data by fragment.
	dataByFragment := make(map[importKey]importValueData)
	for i := range columnIDs {
		columnID, value := columnIDs[i], values[i]
		if value > bsig.Max {
			return fmt.Errorf("%v, columnID=%v, value=%v", ErrBSIGroupValueTooHigh, columnID, value)
		} else if value < bsig.Min {
			return fmt.Errorf("%v, columnID=%v, value=%v", ErrBSIGroupValueTooLow, columnID, value)
		}
		if value > max {
			max = value
		}
		if value < min {
			min = value
		}

		// Attach value to each bsiGroup view.
		for _, name := range []string{viewName} {
			key := importKey{View: name, Shard: columnID / ShardWidth}
			data := dataByFragment[key]
			data.ColumnIDs = append(data.ColumnIDs, columnID)
			data.Values = append(data.Values, value)
			dataByFragment[key] = data
		}
	}

	// Determine the highest bit depth required by the min & max.
	requiredDepth := bitDepthInt64(min - bsig.Base)
	if v := bitDepthInt64(max - bsig.Base); v > requiredDepth {
		requiredDepth = v
	}
	// Increase bit depth if required.
	if requiredDepth > bsig.BitDepth {
		if err := func() error {
			f.mu.Lock()
			defer f.mu.Unlock()
			bsig.BitDepth = requiredDepth
			f.options.BitDepth = requiredDepth
			return f.saveMeta()
		}(); err != nil {
			return errors.Wrap(err, "increasing bsi bit depth")
		}
	} else {
		requiredDepth = bsig.BitDepth
	}

	// Import into each fragment.
	for key, data := range dataByFragment {
		// The view must already exist (i.e. we can't create it)
		// because we need to know bitDepth (based on min/max value).
		view, err := f.createViewIfNotExists(key.View)
		if err != nil {
			return errors.Wrap(err, "creating view")
		}

		frag, err := view.CreateFragmentIfNotExists(key.Shard)
		if err != nil {
			return errors.Wrap(err, "creating fragment")
		}

		baseValues := make([]int64, len(data.Values))
		for i, value := range data.Values {
			baseValues[i] = value - bsig.Base
		}

		if err := frag.importValue(data.ColumnIDs, baseValues, requiredDepth, options.Clear); err != nil {
			return err
		}
	}

	return nil
}

func (f *Field) importRoaring(ctx context.Context, data []byte, shard uint64, viewName string, clear bool) error {
	span, ctx := tracing.StartSpanFromContext(ctx, "Field.importRoaring")
	defer span.Finish()

	if viewName == "" {
		viewName = viewStandard
	}
	span.LogKV("view", viewName, "bytes", len(data), "shard", shard)
	view, err := f.createViewIfNotExists(viewName)
	if err != nil {
		return errors.Wrap(err, "creating view")
	}

	frag, err := view.CreateFragmentIfNotExists(shard)
	if err != nil {
		return errors.Wrap(err, "creating fragment")
	}

	if err := frag.importRoaring(ctx, data, clear); err != nil {
		return err
	}

	return nil
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
	Base           int64       `json:"base,omitempty"`
	BitDepth       uint        `json:"bitDepth,omitempty"`
	Min            int64       `json:"min,omitempty"`
	Max            int64       `json:"max,omitempty"`
	Keys           bool        `json:"keys"`
	NoStandardView bool        `json:"noStandardView,omitempty"`
	CacheSize      uint32      `json:"cacheSize,omitempty"`
	CacheType      string      `json:"cacheType,omitempty"`
	Type           string      `json:"type,omitempty"`
	TimeQuantum    TimeQuantum `json:"timeQuantum,omitempty"`
}

// applyDefaultOptions returns a new FieldOptions object
// with default values if o does not contain a valid type.
func applyDefaultOptions(o FieldOptions) FieldOptions {
	if o.Type == "" {
		return FieldOptions{
			Type:      DefaultFieldType,
			CacheType: DefaultCacheType,
			CacheSize: DefaultCacheSize,
		}
	}
	return o
}

// encode converts o into its internal representation.
func (o *FieldOptions) encode() *internal.FieldOptions {
	return encodeFieldOptions(o)
}

func encodeFieldOptions(o *FieldOptions) *internal.FieldOptions {
	if o == nil {
		return nil
	}
	return &internal.FieldOptions{
		Type:           o.Type,
		CacheType:      o.CacheType,
		CacheSize:      o.CacheSize,
		Base:           o.Base,
		BitDepth:       uint64(o.BitDepth),
		Min:            o.Min,
		Max:            o.Max,
		TimeQuantum:    string(o.TimeQuantum),
		Keys:           o.Keys,
		NoStandardView: o.NoStandardView,
	}
}

// MarshalJSON marshals FieldOptions to JSON such that
// only those attributes associated to the field type
// are included.
func (o *FieldOptions) MarshalJSON() ([]byte, error) {
	switch o.Type {
	case FieldTypeSet:
		return json.Marshal(struct {
			Type      string `json:"type"`
			CacheType string `json:"cacheType"`
			CacheSize uint32 `json:"cacheSize"`
			Keys      bool   `json:"keys"`
		}{
			o.Type,
			o.CacheType,
			o.CacheSize,
			o.Keys,
		})
	case FieldTypeInt:
		return json.Marshal(struct {
			Type     string `json:"type"`
			Base     int64  `json:"base"`
			BitDepth uint   `json:"bitDepth"`
			Min      int64  `json:"min"`
			Max      int64  `json:"max"`
			Keys     bool   `json:"keys"`
		}{
			o.Type,
			o.Base,
			o.BitDepth,
			o.Min,
			o.Max,
			o.Keys,
		})
	case FieldTypeTime:
		return json.Marshal(struct {
			Type           string      `json:"type"`
			TimeQuantum    TimeQuantum `json:"timeQuantum"`
			Keys           bool        `json:"keys"`
			NoStandardView bool        `json:"noStandardView"`
		}{
			o.Type,
			o.TimeQuantum,
			o.Keys,
			o.NoStandardView,
		})
	case FieldTypeMutex:
		return json.Marshal(struct {
			Type      string `json:"type"`
			CacheType string `json:"cacheType"`
			CacheSize uint32 `json:"cacheSize"`
			Keys      bool   `json:"keys"`
		}{
			o.Type,
			o.CacheType,
			o.CacheSize,
			o.Keys,
		})
	case FieldTypeBool:
		return json.Marshal(struct {
			Type string `json:"type"`
		}{
			o.Type,
		})
	}
	return nil, errors.New("invalid field type")
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

// bsiBase is a helper function used to determine the default value
// for base. Because base is not exposed as a field option argument,
// it defaults to min, max, or 0 depending on the min/max range.
func bsiBase(min, max int64) int64 {
	if min > 0 {
		return min
	} else if max < 0 {
		return max
	}
	return 0
}

// bsiGroup represents a group of range-encoded rows on a field.
type bsiGroup struct {
	Name     string `json:"name,omitempty"`
	Type     string `json:"type,omitempty"`
	Min      int64  `json:"min,omitempty"`
	Max      int64  `json:"max,omitempty"`
	Base     int64  `json:"base,omitempty"`
	BitDepth uint   `json:"bitDepth,omitempty"`
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
// Executor.executeBSIGroupRangeShard() takes this into account and returns
// `frag.FieldNotNull(bsig.BitDepth())` in such instances.
func (b *bsiGroup) baseValue(op pql.Token, value int64) (baseValue int64, outOfRange bool) {
	min, max := b.bitDepthMin(), b.bitDepthMax()

	if op == pql.GT || op == pql.GTE {
		if value > max {
			return baseValue, true
		} else if value > min {
			baseValue = int64(value - b.Base)
		}
	} else if op == pql.LT || op == pql.LTE {
		if value < min {
			return baseValue, true
		} else if value > max {
			baseValue = int64(max - b.Base)
		} else {
			baseValue = int64(value - b.Base)
		}
	} else if op == pql.EQ || op == pql.NEQ {
		if value < min || value > max {
			return baseValue, true
		}
		baseValue = int64(value - b.Base)
	}
	return baseValue, false
}

// baseValueBetween adjusts the min/max value to align with the range for Field.
func (b *bsiGroup) baseValueBetween(lo, hi int64) (baseValueLo, baseValueHi int64, outOfRange bool) {
	min, max := b.bitDepthMin(), b.bitDepthMax()
	if hi < min || lo > max {
		return 0, 0, true
	}

	// Limit lo/hi to possible bit range.
	if lo < min {
		lo = min
	}
	if hi > max {
		hi = max
	}
	return lo - b.Base, hi - b.Base, false
}

func (b *bsiGroup) validate() error {
	if b.Name == "" {
		return ErrBSIGroupNameRequired
	} else if !isValidBSIGroupType(b.Type) {
		return ErrInvalidBSIGroupType
	}
	return nil
}

// bitDepthMin returns the minimum value possible for the current bit depth.
func (b *bsiGroup) bitDepthMin() int64 {
	return b.Base - (1 << b.BitDepth) + 1
}

// bitDepthMax returns the maximum value possible for the current bit depth.
func (b *bsiGroup) bitDepthMax() int64 {
	return b.Base + (1 << b.BitDepth) - 1
}

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

// bitDepth returns the number of bits required to store a value.
func bitDepth(v uint64) uint {
	for i := uint(0); i < 63; i++ {
		if v < (1 << i) {
			return i
		}
	}
	return 63
}

// bitDepthInt64 returns the required bit depth for abs(v).
func bitDepthInt64(v int64) uint {
	if v < 0 {
		return bitDepth(uint64(-v))
	}
	return bitDepth(uint64(v))
}
