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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"math/bits"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pilosa/pilosa/v2/internal"
	"github.com/pilosa/pilosa/v2/pql"
	"github.com/pilosa/pilosa/v2/roaring"
	"github.com/pilosa/pilosa/v2/stats"
	"github.com/pilosa/pilosa/v2/testhook"
	"github.com/pilosa/pilosa/v2/tracing"
	"github.com/pkg/errors"
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
	FieldTypeSet     = "set"
	FieldTypeInt     = "int"
	FieldTypeTime    = "time"
	FieldTypeMutex   = "mutex"
	FieldTypeBool    = "bool"
	FieldTypeDecimal = "decimal"
)

type protected struct {
	mu       sync.Mutex
	duration time.Duration
}

func (p *protected) Set(d time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.duration = d
}
func (p *protected) Get() time.Duration {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.duration
}

var availableShardFileFlushDuration = &protected{
	duration: 5 * time.Second,
}

// Field represents a container for views.
type Field struct {
	mu            sync.RWMutex
	createdAt     int64
	path          string
	index         string
	name          string
	qualifiedName string

	idx *Index

	viewMap map[string]*view

	// Row attribute storage and cache
	rowAttrStore AttrStore

	broadcaster broadcaster
	Stats       stats.StatsClient

	// Field options.
	options FieldOptions

	// finalOptions is used with a final call to applyOptions.
	// The initial call to applyOptions is made with options
	// loaded from the meta file on disk (in the case when
	// a field is being re-opened). If the field creator calls
	// setOptions before calling Open(), then those options
	// will be held in finalOptions, and applied instead of
	// those from the meta file.
	finalOptions *FieldOptions

	bsiGroups []*bsiGroup

	// Shards with data on any node in the cluster, according to this node.
	remoteAvailableShards *roaring.Bitmap

	translateStore TranslateStore

	// Instantiates new translation stores
	OpenTranslateStore OpenTranslateStoreFunc

	// Used for looking up a foreign index.
	holder *Holder

	// Stores whether or not the field has keys enabled.
	// This is most helpful for cases where the keys are
	// based on a foreign index; this prevents having to
	// call holder.index.Keys() every time.
	usesKeys bool

	// Synchronization primitives needed for async writing of
	// the remoteAvailableShards
	availableShardChan chan []byte
	doneChan           chan struct{}
	wg                 sync.WaitGroup
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

// OptFieldForeignIndex marks this field as a foreign key to another
// index. That is, the values of this field should be interpreted as
// referencing records (Pilosa columns) in another index. TODO explain
// where/how this is used by Pilosa.
func OptFieldForeignIndex(index string) FieldOption {
	return func(fo *FieldOptions) error {
		fo.ForeignIndex = index
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
		fo.Min = pql.NewDecimal(min, 0)
		fo.Max = pql.NewDecimal(max, 0)
		fo.Base = bsiBase(min, max)
		return nil
	}
}

// OptFieldTypeDecimal is a functional option for creating a `decimal` field.
// Unless we decide to expand the range of supported values, `scale` is
// restricted to the range [0,19]. This supports anything from:
//
// scale = 0:
// min: -9223372036854775808.
// max:  9223372036854775807.
//
// to:
//
// scale = 19:
// min: -0.9223372036854775808
// max:  0.9223372036854775807
//
// While it's possible to support scale values outside of this range,
// the coverage for those scales are no longer continuous. For example,
//
// scale = -2:
// min : [-922337203685477580800, -100]
// GAPs: [-99, -1], [-199, -101] ... [-922337203685477580799, -922337203685477580701]
//       0
// max : [100, 922337203685477580700]
// GAPs: [1, 99], [101, 199] ... [922337203685477580601, 922337203685477580699]
//
// An alternative to this gap strategy would be to scale the supported range
// to a continuous 64-bit space (which is not unreasonable using bsiGroup.Base).
// The issue with this approach is that we would need to know which direction
// to favor. For example, there are two possible ranges for `scale = -2`:
//
// min : [-922337203685477580800, -922337203685477580800+(2^64)]
// max : [922337203685477580700-(2^64), 922337203685477580700]
//
func OptFieldTypeDecimal(scale int64, minmax ...pql.Decimal) FieldOption {
	return func(fo *FieldOptions) error {
		if fo.Type != "" {
			return errors.Errorf("can't set field type to 'decimal', already set to: %s", fo.Type)
		}
		if scale < 0 || scale > 19 {
			return errors.Errorf("scale values outside the range [0,19] are not supported: %d", scale)
		}

		fo.Min, fo.Max = pql.MinMax(scale)
		if len(minmax) == 2 {
			min := minmax[0]
			max := minmax[1]
			if !min.IsValid() || !max.IsValid() {
				return errors.Errorf("min/max range %s-%s is not supported", min, max)
			} else if !min.SupportedByScale(scale) || !max.SupportedByScale(scale) {
				return errors.Errorf("min/max range %s-%s is not supported by scale %d", min, max, scale)
			} else if min.GreaterThan(max) {
				return errors.Errorf("decimal field min cannot be greater than max, got %s, %s", min, max)
			}
			fo.Min = min
			fo.Max = max
		} else if len(minmax) > 2 {
			return errors.Errorf("unknown extra parameters beyond min and max: %v", minmax)
		} else if len(minmax) == 1 {
			min := minmax[0]
			if !min.IsValid() {
				return errors.Errorf("min %s is not supported", min)
			} else if !min.SupportedByScale(scale) {
				return errors.Errorf("min %s is not supported by scale %d", min, scale)
			}
			fo.Min = min
		}
		fo.Type = FieldTypeDecimal
		fo.Base = bsiBase(fo.Min.ToInt64(scale), fo.Max.ToInt64(scale))
		fo.Scale = scale
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
// NOTE: This function is only used in tests, which is why
// it only takes a single `FieldOption` (the assumption being
// that it's of the type `OptFieldType*`). This means
// this function couldn't be used to set, for example,
// `FieldOptions.Keys`.
func NewField(holder *Holder, path, index, name string, opts FieldOption) (*Field, error) {
	err := validateName(name)
	if err != nil {
		return nil, errors.Wrap(err, "validating name")
	}

	return newField(holder, path, index, name, opts)
}

// newField returns a new instance of field (without name validation).
func newField(holder *Holder, path, index, name string, opts FieldOption) (*Field, error) {
	// Apply functional option.
	fo := FieldOptions{}
	err := opts(&fo)
	if err != nil {
		return nil, errors.Wrap(err, "applying option")
	}

	f := &Field{
		path:          path,
		index:         index,
		name:          name,
		qualifiedName: FormatQualifiedFieldName(index, name),

		viewMap: make(map[string]*view),

		rowAttrStore: nopStore,

		broadcaster: NopBroadcaster,
		Stats:       stats.NopStatsClient,

		options: *applyDefaultOptions(&fo),

		remoteAvailableShards: roaring.NewBitmap(),

		holder: holder,

		OpenTranslateStore: OpenInMemTranslateStore,
	}

	return f, nil
}

// Name returns the name the field was initialized with.
func (f *Field) Name() string { return f.name }

// CreatedAt is an timestamp for a specific version of field.
func (f *Field) CreatedAt() int64 {
	f.mu.RLock()
	defer f.mu.RUnlock()

	return f.createdAt
}

// Index returns the index name the field was initialized with.
func (f *Field) Index() string { return f.index }

// Path returns the path the field was initialized with.
func (f *Field) Path() string { return f.path }

// TranslateStorePath returns the translation database path for the field.
func (f *Field) TranslateStorePath() string {
	return filepath.Join(f.path, "keys")
}

// TranslateStore returns the field's translation store.
func (f *Field) TranslateStore() TranslateStore {
	return f.translateStore
}

// RowAttrStore returns the attribute storage.
func (f *Field) RowAttrStore() AttrStore { return f.rowAttrStore }

// AvailableShards returns a bitmap of shards that contain data.
func (f *Field) AvailableShards(localOnly bool) *roaring.Bitmap {
	f.mu.RLock()
	defer f.mu.RUnlock()

	var b *roaring.Bitmap
	if localOnly {
		b = roaring.NewBitmap()
	} else {
		b = f.remoteAvailableShards.Clone()
	}
	for _, view := range f.viewMap {
		//b.Union(view.availableShards())
		b.UnionInPlace(view.availableShards())
	}
	return b
}

// LocalAvailableShards returns a bitmap of shards that contain data, but
// only from the local node. This prevents txfactory from making
// db-per-shard for remote shards.
func (f *Field) LocalAvailableShards() *roaring.Bitmap {
	f.mu.RLock()
	defer f.mu.RUnlock()

	b := roaring.NewBitmap()
	for _, view := range f.viewMap {
		//b.Union(view.availableShards())
		b.UnionInPlace(view.availableShards())
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
	// Read data from meta file.
	path := filepath.Join(f.path, ".available.shards")
	buf, err := ioutil.ReadFile(path)
	// doesn't exist: this is fine
	if os.IsNotExist(err) {
		return nil
	}
	// some other problem:
	if err != nil {
		f.holder.Logger.Printf("available shards file present but unreadable, discarding: %v", err)
		err = os.Remove(path)
		if err != nil {
			return errors.Wrap(err, "deleting corrupt available shards list")
		}
		return nil
	}
	bm := roaring.NewBitmap()
	if err = bm.UnmarshalBinary(buf); err != nil {
		f.holder.Logger.Printf("available shards file corrupt, discarding: %v", err)
		err = os.Remove(path)
		if err != nil {
			return errors.Wrap(err, "deleting corrupt available shards list")
		}
		return nil
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
	var buf bytes.Buffer
	if _, err := f.remoteAvailableShards.WriteTo(&buf); err != nil {
		return errors.Wrap(err, "rendering available shards ")
	}
	f.availableShardChan <- buf.Bytes()
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
		f.holder.Logger.Debugf("ensure field path exists: %s", f.path)
		if err := os.MkdirAll(f.path, 0777); err != nil {
			return errors.Wrap(err, "creating field dir")
		}

		f.holder.Logger.Debugf("load meta file for index/field: %s/%s", f.index, f.name)
		if err := f.loadMeta(); err != nil {
			return errors.Wrap(err, "loading meta")
		}

		f.holder.Logger.Debugf("load available shards for index/field: %s/%s", f.index, f.name)

		if err := f.loadAvailableShards(); err != nil {
			return errors.Wrap(err, "loading available shards")
		}

		// If options were provided using setOptions(), then
		// use those instead of the options from the meta file.
		if f.finalOptions != nil {
			f.options = *f.finalOptions
		}

		// Apply the field options loaded from meta (or set via setOptions()).
		f.holder.Logger.Debugf("apply options for index/field: %s/%s", f.index, f.name)
		if err := f.applyOptions(f.options); err != nil {
			return errors.Wrap(err, "applying options")
		}

		f.holder.Logger.Debugf("open views for index/field: %s/%s", f.index, f.name)
		if err := f.openViews(); err != nil {
			return errors.Wrap(err, "opening views")
		}

		f.holder.Logger.Debugf("open row attribute store for index/field: %s/%s", f.index, f.name)
		if err := f.rowAttrStore.Open(); err != nil {
			return errors.Wrap(err, "opening attrstore")
		}

		// Apply the field-specific translateStore.
		if err := f.applyTranslateStore(); err != nil {
			return errors.Wrap(err, "applying translate store")
		}

		// If the field has a foreign index, make sure the index
		// exists.
		if f.options.ForeignIndex != "" {
			if err := f.holder.checkForeignIndex(f); err != nil {
				return errors.Wrap(err, "checking foreign index")
			}
		}
		f.availableShardChan = make(chan []byte)
		f.doneChan = make(chan struct{})
		f.wg.Add(1)
		go f.writeAvailableShards()
		return nil
	}(); err != nil {
		f.Close()
		return err
	}

	_ = testhook.Opened(f.holder.Auditor, f, nil)
	f.holder.Logger.Debugf("successfully opened field index/field: %s/%s", f.index, f.name)
	return nil
}

func blockingWriteAvailableShards(fieldPath string, availableShardBytes []byte) {
	path := filepath.Join(fieldPath, ".available.shards")

	// Create a temporary file to save to.
	tempPath := path + tempExt
	err := ioutil.WriteFile(tempPath, availableShardBytes, 0666)
	if err != nil {
		log.Println("failed to write ", tempPath)
		return
	}

	// Move snapshot to data file location.
	if err := os.Rename(tempPath, path); err != nil {
		log.Printf("rename snapshot: %s", err)
	}
}
func nonBlockingWriteAvailableShards(fieldPath string, availableShardBytes []byte, done chan bool) {
	if len(availableShardBytes) == 0 {
		return
	}
	go func() {
		blockingWriteAvailableShards(fieldPath, availableShardBytes)
		done <- true
	}()
}

func (f *Field) writeAvailableShards() {
	defer f.wg.Done()
	ticker := time.NewTicker(availableShardFileFlushDuration.Get())
	var data []byte
	tracker := make(chan bool)
	writing := false

	for alive := true; alive; {
		select {
		case newdata := <-f.availableShardChan:
			data = newdata
		case <-ticker.C:
			if len(data) > 0 {
				if !writing {
					writing = true
					nonBlockingWriteAvailableShards(f.path, data, tracker)
					data = nil
				}
			}
		case <-tracker:
			writing = false
		case <-f.doneChan:
			if writing { //wait to writing is complete
				<-tracker
			}
			if len(data) > 0 {
				blockingWriteAvailableShards(f.path, data)
			}
			alive = false
		}
	}
	ticker.Stop()
}

// applyTranslateStore opens the configured translate store.
func (f *Field) applyTranslateStore() error {
	// Instantiate & open translation store.
	var err error
	f.translateStore, err = f.OpenTranslateStore(f.TranslateStorePath(), f.index, f.name, -1, -1)
	if err != nil {
		return errors.Wrap(err, "opening field translate store")
	}
	f.usesKeys = f.options.Keys

	// In the case where the field has a foreign index, set
	// the usesKeys value accordingly.
	if foreignIndexName := f.ForeignIndex(); foreignIndexName != "" {
		if foreignIndex := f.holder.Index(foreignIndexName); foreignIndex != nil {
			f.usesKeys = foreignIndex.Keys()
		}
	}
	return nil
}

// applyForeignIndex used to set the field's translateStore to
// that of the foreign index, but since moving to partitioned
// translate stores on indexes, that doesn't happen anymore.
// So now all this method does is check that the foreign index
// actually exists. If we decided this was unnecessary (which
// it kind of is), we could remove the field.holder and all
// the logic which does this check on holder open after all
// indexes have opened.
func (f *Field) applyForeignIndex() error {
	foreignIndex := f.holder.Index(f.options.ForeignIndex)
	if foreignIndex == nil {
		return errors.Wrapf(ErrForeignIndexNotFound, "%s", f.options.ForeignIndex)
	}
	f.usesKeys = foreignIndex.Keys()
	return nil
}

// ForeignIndex returns the foreign index name attached to the field.
// Returns blank string if no foreign index exists.
func (f *Field) ForeignIndex() string {
	return f.options.ForeignIndex
}

// openViews opens and initializes the views inside the field.
func (f *Field) openViews() error {

	view2shards := f.idx.fieldView2shard.getViewsForField(f.name)
	if view2shards == nil {
		// no data
		return nil
	}

	for name, shardset := range view2shards {

		view := f.newView(f.viewPath(name), name)
		if err := view.openWithShardSet(shardset); err != nil {
			return fmt.Errorf("opening view: view=%s, err=%s", view.name, err)
		}

		if f.holder.txf.TxType() == RoaringTxn {
			// Automatically upgrade BSI v1 fragments if they exist & reopen view.
			if bsig := f.bsiGroup(f.name); bsig != nil {
				if ok, err := upgradeViewBSIv2(view, bsig.BitDepth); err != nil {
					return errors.Wrap(err, "upgrade view bsi v2")
				} else if ok {
					if err := view.close(); err != nil {
						return errors.Wrap(err, "closing upgraded view")
					}
					view = f.newView(f.viewPath(name), name)
					if err := view.openWithShardSet(shardset); err != nil {
						return fmt.Errorf("re-opening view: view=%s, err=%s", view.name, err)
					}
				}
			}
		}

		view.rowAttrStore = f.rowAttrStore
		f.holder.Logger.Debugf("add index/field/view to field.viewMap: %s/%s/%s", f.index, f.name, view.name)
		f.viewMap[view.name] = view
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

	// Since pb.Min and pb.Max were changed to pql.Decimal,
	// and since they now have a different protobuf field
	// number, an existing meta file may have values in the
	// old min/max fields which need to be converted to
	// pql.Decimal.
	// TODO: we can remove the OldMin/OldMax once we're
	// confident no one is still using the older version.
	var min pql.Decimal
	if pb.Min != nil {
		min = pql.NewDecimal(pb.Min.Value, pb.Min.Scale)
	} else {
		min = pql.NewDecimal(pb.OldMin, pb.Scale)
	}
	var max pql.Decimal
	if pb.Max != nil {
		max = pql.NewDecimal(pb.Max.Value, pb.Max.Scale)
	} else {
		max = pql.NewDecimal(pb.OldMax, pb.Scale)
	}

	// Initialize "base" to "min" when upgrading from v1 BSI format.
	if pb.BitDepth == 0 {
		minInt64, maxInt64 := min.ToInt64(0), max.ToInt64(0)
		pb.Base = bsiBase(minInt64, maxInt64)
		pb.BitDepth = uint64(bitDepthInt64(maxInt64 - minInt64))
		if pb.BitDepth == 0 {
			pb.BitDepth = 1
		}
	}

	// Copy metadata fields.
	f.options.Type = pb.Type
	f.options.CacheType = pb.CacheType
	f.options.CacheSize = pb.CacheSize
	f.options.Min = min
	f.options.Max = max
	f.options.Base = pb.Base
	f.options.Scale = pb.Scale
	f.options.BitDepth = uint(pb.BitDepth)
	f.options.TimeQuantum = TimeQuantum(pb.TimeQuantum)
	f.options.Keys = pb.Keys
	f.options.NoStandardView = pb.NoStandardView
	f.options.ForeignIndex = pb.ForeignIndex

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

// setOptions saves options for final application during Open().
func (f *Field) setOptions(opts *FieldOptions) {
	f.finalOptions = applyDefaultOptions(opts)
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
		f.options.Min = pql.Decimal{}
		f.options.Max = pql.Decimal{}
		f.options.Base = 0
		f.options.BitDepth = 0
		f.options.TimeQuantum = ""
		f.options.Keys = opt.Keys
		f.options.ForeignIndex = opt.ForeignIndex
	case FieldTypeInt, FieldTypeDecimal:
		f.options.Type = opt.Type
		f.options.CacheType = CacheTypeNone
		f.options.CacheSize = 0
		f.options.Min = opt.Min
		f.options.Max = opt.Max
		f.options.Base = opt.Base
		f.options.Scale = opt.Scale
		f.options.BitDepth = opt.BitDepth
		f.options.TimeQuantum = ""
		f.options.Keys = opt.Keys
		f.options.ForeignIndex = opt.ForeignIndex

		// Create new bsiGroup.
		bsig := &bsiGroup{
			Name:     f.name,
			Type:     bsiGroupTypeInt,
			Min:      opt.Min.ToInt64(opt.Scale),
			Max:      opt.Max.ToInt64(opt.Scale),
			Base:     opt.Base,
			Scale:    opt.Scale,
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
		f.options.Min = pql.Decimal{}
		f.options.Max = pql.Decimal{}
		f.options.Base = 0
		f.options.BitDepth = 0
		f.options.Keys = opt.Keys
		f.options.NoStandardView = opt.NoStandardView
		// Set the time quantum.
		if err := f.setTimeQuantum(opt.TimeQuantum); err != nil {
			f.Close()
			return errors.Wrap(err, "setting time quantum")
		}
		f.options.ForeignIndex = opt.ForeignIndex
	case FieldTypeBool:
		f.options.Type = FieldTypeBool
		f.options.CacheType = CacheTypeNone
		f.options.CacheSize = 0
		f.options.Min = pql.Decimal{}
		f.options.Max = pql.Decimal{}
		f.options.Base = 0
		f.options.BitDepth = 0
		f.options.TimeQuantum = ""
		f.options.Keys = false
		f.options.ForeignIndex = ""
	default:
		return errors.New("invalid field type")
	}

	return nil
}

// Close closes the field and its views.
func (f *Field) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	defer func() {
		_ = testhook.Closed(f.holder.Auditor, f, nil)
	}()
	// Shutdown the available shards writer
	if f.doneChan != nil {
		close(f.doneChan)
		f.wg.Wait()
		close(f.availableShardChan)
		f.availableShardChan = nil
		f.doneChan = nil
	}
	// Close the attribute store.
	if f.rowAttrStore != nil {
		_ = f.rowAttrStore.Close()
	}

	// Close field translation store.
	if f.translateStore != nil {
		if err := f.translateStore.Close(); err != nil {
			return err
		}
	}

	// Close all views.
	for _, view := range f.viewMap {
		if err := view.close(); err != nil {
			return err
		}
	}
	f.viewMap = make(map[string]*view)

	return nil
}

// Keys returns true if the field uses string keys.
func (f *Field) Keys() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.usesKeys
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
func (f *Field) RowTime(tx Tx, rowID uint64, time time.Time, quantum string) (*Row, error) {
	if !TimeQuantum(quantum).Valid() {
		return nil, ErrInvalidTimeQuantum
	}
	viewname := viewsByTime(viewStandard, time, TimeQuantum(quantum[len(quantum)-1:]))[0]
	view := f.view(viewname)
	if view == nil {
		return nil, errors.Errorf("view with quantum %v not found.", quantum)
	}

	return view.row(tx, rowID)
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

	if err := view.openEmpty(); err != nil {
		return nil, false, errors.Wrap(err, "opening view")
	}
	view.rowAttrStore = f.rowAttrStore
	f.viewMap[view.name] = view

	return view, true, nil
}

func (f *Field) newView(path, name string) *view {
	view := newView(f.holder, path, f.index, f.name, name, f.options)
	view.idx = f.idx
	view.rowAttrStore = f.rowAttrStore
	view.stats = f.Stats
	view.broadcaster = f.broadcaster
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
// `set`,`mutex`, and `bool` fields is odd. This may
// be considered for deprecation in a future version.
func (f *Field) Row(tx Tx, rowID uint64) (*Row, error) {
	switch f.Type() {
	case FieldTypeSet, FieldTypeMutex, FieldTypeBool:
		view := f.view(viewStandard)
		if view == nil {
			return nil, ErrInvalidView
		}
		return view.row(tx, rowID)
	default:
		return nil, errors.Errorf("row method unsupported for field type: %s", f.Type())
	}
}

// mutexCheck performs a sanity-check on the available fragments for a
// field. The return is map[column]map[shard][]values for collisions only.
func (f *Field) MutexCheck(ctx context.Context, qcx *Qcx, details bool, limit int) (map[uint64]map[uint64][]uint64, error) {
	if f.Type() != FieldTypeMutex {
		return nil, errors.New("mutex check only valid for mutex fields")
	}

	// Rather than deferring the unlock, we grab the standard view
	// from the field's viewMap and unlock immediately. This avoids
	// holding the rlock for a potentially long time which blocks any
	// write lock, and pending write locks block other read locks.
	f.mu.RLock()
	standard := f.viewMap[viewStandard]
	f.mu.RUnlock()
	if standard == nil {
		// no standard view present means we've never needed to create it,
		// so it has no bits set, so it has no extra bits set.
		return nil, nil
	}
	return standard.mutexCheck(ctx, qcx, details, limit)
}

// SetBit sets a bit on a view within the field.
func (f *Field) SetBit(tx Tx, rowID, colID uint64, t *time.Time) (changed bool, err error) {
	viewName := viewStandard
	if !f.options.NoStandardView {
		// Retrieve view. Exit if it doesn't exist.
		view, err := f.createViewIfNotExists(viewName)
		if err != nil {
			return changed, errors.Wrap(err, "creating view")
		}

		// Set non-time bit.
		if v, err := view.setBit(tx, rowID, colID); err != nil {
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

		if c, err := view.setBit(tx, rowID, colID); err != nil {
			return changed, errors.Wrapf(err, "setting on view %s", subname)
		} else if c {
			changed = true
		}
	}

	return changed, nil
}

// ClearBit clears a bit within the field.
func (f *Field) ClearBit(tx Tx, rowID, colID uint64) (changed bool, err error) {
	viewName := viewStandard

	// Retrieve view. Exit if it doesn't exist.
	view, present := f.viewMap[viewName]
	if !present {
		return false, errors.Wrap(err, "clearing missing view")
	}

	// Clear non-time bit.
	if v, err := view.clearBit(tx, rowID, colID); err != nil {
		return false, errors.Wrap(err, "clearing on view")
	} else if v {
		changed = changed || v
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
			cleared, err := view.clearBit(tx, rowID, colID)
			changed = changed || cleared
			if err != nil {
				return changed, errors.Wrapf(err, "clearing on view %s", view.name)
			}
			if !cleared {
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

// StringValue reads an integer field value for a column, and converts
// it to a string based on a foreign index string key.
func (f *Field) StringValue(tx Tx, columnID uint64) (value string, exists bool, err error) {
	bsig := f.bsiGroup(f.name)
	if bsig == nil {
		return value, false, ErrBSIGroupNotFound
	}

	val, exists, err := f.Value(tx, columnID)
	if exists {
		value, err = f.translateStore.TranslateID(uint64(val))
	}
	return value, exists, err
}

// Value reads a field value for a column.
func (f *Field) Value(tx Tx, columnID uint64) (value int64, exists bool, err error) {
	bsig := f.bsiGroup(f.name)
	if bsig == nil {
		return 0, false, ErrBSIGroupNotFound
	}

	// Fetch target view.
	view := f.view(viewBSIGroupPrefix + f.name)
	if view == nil {
		return 0, false, nil
	}

	v, exists, err := view.value(tx, columnID, bsig.BitDepth)
	if err != nil {
		return 0, false, err
	} else if !exists {
		return 0, false, nil
	}
	return int64(v) + bsig.Base, true, nil
}

// SetValue sets a field value for a column.
func (f *Field) SetValue(tx Tx, columnID uint64, value int64) (changed bool, err error) {
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
	if view.holder == nil {
		panic("view.holder should not be nil")
	}
	if view.idx == nil {
		panic("view.idx should not be nil")
	}
	view.holder.addIndex(view.idx)

	return view.setValue(tx, columnID, bsig.BitDepth, baseValue)
}

// ClearValue removes a field value for a column.
func (f *Field) ClearValue(tx Tx, columnID uint64) (changed bool, err error) {
	bsig := f.bsiGroup(f.name)
	if bsig == nil {
		return false, ErrBSIGroupNotFound
	}
	// Fetch target view.
	view := f.view(viewBSIGroupPrefix + f.name)
	if view == nil {
		return false, nil
	}
	value, exists, err := view.value(tx, columnID, bsig.BitDepth)
	if err != nil {
		return false, err
	}
	if exists {
		return view.clearValue(tx, columnID, bsig.BitDepth, value)
	}
	return false, nil
}

func (f *Field) MaxForShard(tx Tx, shard uint64, filter *Row) (ValCount, error) {
	bsig := f.bsiGroup(f.name)
	if bsig == nil {
		return ValCount{}, ErrBSIGroupNotFound
	}

	view := f.view(viewBSIGroupPrefix + f.name)
	if view == nil {
		return ValCount{}, nil
	}

	fragment := view.Fragment(shard)
	if fragment == nil {
		return ValCount{}, nil
	}

	var localTx Tx
	if NilInside(tx) {
		localTx = f.holder.txf.NewTx(Txo{Write: !writable, Index: f.idx, Fragment: fragment, Shard: fragment.shard})
		defer localTx.Rollback()
	} else {
		localTx = tx
	}

	max, cnt, err := fragment.max(localTx, filter, bsig.BitDepth)
	if err != nil {
		return ValCount{}, errors.Wrap(err, "calling fragment.max")
	}

	valCount := ValCount{Count: int64(cnt)}

	if f.Options().Type == FieldTypeDecimal {
		dec := pql.NewDecimal(max+bsig.Base, bsig.Scale)
		valCount.DecimalVal = &dec
	} else {
		valCount.Val = max + bsig.Base
	}

	return valCount, nil
}

// MinForShard returns the minimum value which appears in this shard
// (this field must be an Int or Decimal field). It also returns the
// number of times the minimum value appears.
func (f *Field) MinForShard(tx Tx, shard uint64, filter *Row) (ValCount, error) {
	bsig := f.bsiGroup(f.name)
	if bsig == nil {
		return ValCount{}, ErrBSIGroupNotFound
	}

	view := f.view(viewBSIGroupPrefix + f.name)
	if view == nil {
		return ValCount{}, nil
	}

	fragment := view.Fragment(shard)
	if fragment == nil {
		return ValCount{}, nil
	}

	var localTx Tx
	if NilInside(tx) {
		localTx = f.idx.holder.txf.NewTx(Txo{Write: !writable, Index: f.idx, Fragment: fragment, Shard: fragment.shard})
		defer localTx.Rollback()
	} else {
		localTx = tx
	}

	min, cnt, err := fragment.min(localTx, filter, bsig.BitDepth)
	if err != nil {
		return ValCount{}, errors.Wrap(err, "calling fragment.min")
	}

	valCount := ValCount{Count: int64(cnt)}

	if f.Options().Type == FieldTypeDecimal {
		dec := pql.NewDecimal(min+bsig.Base, bsig.Scale)
		valCount.DecimalVal = &dec
	} else {
		valCount.Val = min + bsig.Base
	}

	return valCount, nil
}

// Range performs a conditional operation on Field.
func (f *Field) Range(qcx *Qcx, name string, op pql.Token, predicate int64) (*Row, error) {
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

	return view.rangeOp(qcx, op, bsig.BitDepth, baseValue)
}

// Import bulk imports data.
func (f *Field) Import(qcx *Qcx, rowIDs, columnIDs []uint64, timestamps []*time.Time, opts ...ImportOption) (err0 error) {

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

		tx, finisher, err := qcx.GetTx(Txo{Write: true, Index: frag.idx, Fragment: frag, Shard: frag.shard})
		if err != nil {
			return errors.Wrap(err, "qcx.GetTx")
		}

		err1 := frag.bulkImport(tx, data.RowIDs, data.ColumnIDs, options)
		if err1 != nil {
			finisher(&err1)
			return err1
		}
		finisher(nil)
	}
	return nil
}

func (f *Field) importFloatValue(qcx *Qcx, columnIDs []uint64, values []float64, options *ImportOptions) error {
	// convert values to int64 values based on scale
	ivalues := make([]int64, len(values))
	bsig := f.bsiGroup(f.name)
	if bsig == nil {
		return errors.Wrap(ErrBSIGroupNotFound, f.name)
	}
	mult := math.Pow10(int(bsig.Scale))
	for i, fval := range values {
		ivalues[i] = int64(fval * mult)
	}
	// then call importValue
	return f.importValue(qcx, columnIDs, ivalues, options)
}

// importValue bulk imports range-encoded value data.
func (f *Field) importValue(qcx *Qcx, columnIDs []uint64, values []int64, options *ImportOptions) (err0 error) {
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
	if err := func() error {
		f.mu.Lock()
		defer f.mu.Unlock()
		bitDepth := bsig.BitDepth
		if requiredDepth > bitDepth {
			bsig.BitDepth = requiredDepth
			f.options.BitDepth = requiredDepth
			return f.saveMeta()
		} else {
			requiredDepth = bitDepth
		}
		return nil
	}(); err != nil {
		return errors.Wrap(err, "increasing bsi bit depth")
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

		// now we know which shard we discovered.
		tx, finisher, err := qcx.GetTx(Txo{Write: writable, Index: f.idx, Shard: frag.shard})
		if err != nil {
			return err
		}
		// by deferring, even though we are in loop, we get en-mass commit at once if they all succeed,
		// or en-mass rollback if any fail.
		defer finisher(&err0)

		if err = frag.importValue(tx, data.ColumnIDs, baseValues, requiredDepth, options.Clear); err != nil {
			return err
		}
	}

	return nil
}

func (f *Field) importRoaring(ctx context.Context, tx Tx, data []byte, shard uint64, viewName string, clear bool) error {
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
	if err := frag.importRoaring(ctx, tx, data, clear); err != nil {
		return err
	}

	return nil
}

func (f *Field) GetIndex() *Index {
	return f.idx
}

func (f *Field) importRoaringOverwrite(ctx context.Context, tx Tx, data []byte, shard uint64, viewName string, block int) error {
	span, ctx := tracing.StartSpanFromContext(ctx, "Field.importRoaringOverwrite")
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
	if err := frag.importRoaringOverwrite(ctx, tx, data, block); err != nil {
		return err
	}

	// If field is int or decimal, then we need to update field.options.BitDepth
	// and bsiGroup.BitDepth based on the imported data.
	switch f.Options().Type {
	case FieldTypeInt, FieldTypeDecimal:
		frag.mu.Lock()
		maxRowID, _, err := frag.maxRow(tx, nil)
		frag.mu.Unlock()
		if err != nil {
			return err
		}

		var bitDepth uint
		if maxRowID+1 > bsiOffsetBit {
			bitDepth = uint(maxRowID + 1 - bsiOffsetBit)
		}

		bsig := f.bsiGroup(f.name)

		f.mu.Lock()
		defer f.mu.Unlock()
		if bitDepth > f.options.BitDepth {
			f.options.BitDepth = bitDepth
		}
		if bsig != nil {
			bsig.BitDepth = bitDepth
		}
	}

	return nil
}

type fieldSlice []*Field

func (p fieldSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p fieldSlice) Len() int           { return len(p) }
func (p fieldSlice) Less(i, j int) bool { return p[i].Name() < p[j].Name() }

// FieldInfo represents schema information for a field.
type FieldInfo struct {
	Name      string       `json:"name"`
	CreatedAt int64        `json:"createdAt,omitempty"`
	Options   FieldOptions `json:"options"`
	Views     []*ViewInfo  `json:"views,omitempty"`
}

type fieldInfoSlice []*FieldInfo

func (p fieldInfoSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p fieldInfoSlice) Len() int           { return len(p) }
func (p fieldInfoSlice) Less(i, j int) bool { return p[i].Name < p[j].Name }

// FieldOptions represents options to set when initializing a field.
type FieldOptions struct {
	Base           int64       `json:"base,omitempty"`
	BitDepth       uint        `json:"bitDepth,omitempty"`
	Min            pql.Decimal `json:"min,omitempty"`
	Max            pql.Decimal `json:"max,omitempty"`
	Scale          int64       `json:"scale,omitempty"`
	Keys           bool        `json:"keys"`
	NoStandardView bool        `json:"noStandardView,omitempty"`
	CacheSize      uint32      `json:"cacheSize,omitempty"`
	CacheType      string      `json:"cacheType,omitempty"`
	Type           string      `json:"type,omitempty"`
	TimeQuantum    TimeQuantum `json:"timeQuantum,omitempty"`
	ForeignIndex   string      `json:"foreignIndex"`
}

// newFieldOptions returns a new instance of FieldOptions
// with applied and validated functional options.
func newFieldOptions(opts ...FieldOption) (*FieldOptions, error) {
	fo := FieldOptions{}
	for _, opt := range opts {
		err := opt(&fo)
		if err != nil {
			return nil, err
		}
	}

	if fo.Keys {
		switch fo.Type {
		case FieldTypeInt:
			return nil, ErrIntFieldWithKeys

		case FieldTypeDecimal:
			return nil, ErrDecimalFieldWithKeys
		}
	}

	return &fo, nil
}

// applyDefaultOptions updates FieldOptions with the default
// values if o does not contain a valid type.
func applyDefaultOptions(o *FieldOptions) *FieldOptions {
	if o.Type == "" {
		o.Type = DefaultFieldType
		o.CacheType = DefaultCacheType
		o.CacheSize = DefaultCacheSize
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
		Scale:          o.Scale,
		BitDepth:       uint64(o.BitDepth),
		Min:            &internal.Decimal{Value: o.Min.Value, Scale: o.Min.Scale},
		Max:            &internal.Decimal{Value: o.Max.Value, Scale: o.Max.Scale},
		TimeQuantum:    string(o.TimeQuantum),
		Keys:           o.Keys,
		NoStandardView: o.NoStandardView,
		ForeignIndex:   o.ForeignIndex,
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
			Type         string      `json:"type"`
			Base         int64       `json:"base"`
			BitDepth     uint        `json:"bitDepth"`
			Min          pql.Decimal `json:"min"`
			Max          pql.Decimal `json:"max"`
			Keys         bool        `json:"keys"`
			ForeignIndex string      `json:"foreignIndex"`
		}{
			o.Type,
			o.Base,
			o.BitDepth,
			o.Min,
			o.Max,
			o.Keys,
			o.ForeignIndex,
		})
	case FieldTypeDecimal:
		return json.Marshal(struct {
			Type     string      `json:"type"`
			Base     int64       `json:"base"`
			Scale    int64       `json:"scale"`
			BitDepth uint        `json:"bitDepth"`
			Min      pql.Decimal `json:"min"`
			Max      pql.Decimal `json:"max"`
			Keys     bool        `json:"keys"`
		}{
			o.Type,
			o.Base,
			o.Scale,
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
	Scale    int64  `json:"scale,omitempty"`
	BitDepth uint   `json:"bitDepth,omitempty"`
}

// baseValue adjusts the value to align with the range for Field for a certain
// operation type.
// Note: There is an edge case for GT and LT where this returns a baseValue
// that does not fully encompass the range.
// ex: Field.Min = 0, Field.Max = 1023
// baseValue(LT, 2000) returns 1023, which will perform "LT 1023" and effectively
// exclude any columns with value = 1023.
func (b *bsiGroup) baseValue(op pql.Token, value int64) (baseValue int64, outOfRange bool) {
	min, max := b.bitDepthMin(), b.bitDepthMax()

	if op == pql.GT || op == pql.GTE {
		if value > max {
			return baseValue, true
		} else if value < min {
			baseValue = int64(min - b.Base)
			// Address edge case noted in comments above.
			if op == pql.GT {
				baseValue--
			}
		} else {
			baseValue = int64(value - b.Base)
		}
	} else if op == pql.LT || op == pql.LTE {
		if value < min {
			return baseValue, true
		} else if value > max {
			baseValue = int64(max - b.Base)
			// Address edge case noted in comments above.
			if op == pql.LT {
				baseValue++
			}
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
	if hi < min || lo > max || hi < lo {
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
	return uint(bits.Len64(v))
}

// bitDepthInt64 returns the required bit depth for abs(v).
func bitDepthInt64(v int64) uint {
	if v < 0 {
		return bitDepth(uint64(-v))
	}
	return bitDepth(uint64(v))
}

// FormatQualifiedFieldName generates a qualified name for the field to be used with Tx operations.
func FormatQualifiedFieldName(index, field string) string {
	return fmt.Sprintf("%s\x00%s\x00", index, field)
}
