// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package pilosa

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/bits"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/molecula/featurebase/v3/pql"
	"github.com/molecula/featurebase/v3/roaring"
	"github.com/molecula/featurebase/v3/stats"
	"github.com/molecula/featurebase/v3/testhook"
	"github.com/molecula/featurebase/v3/tracing"
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
	FieldTypeSet       = "set"
	FieldTypeInt       = "int"
	FieldTypeTime      = "time"
	FieldTypeMutex     = "mutex"
	FieldTypeBool      = "bool"
	FieldTypeDecimal   = "decimal"
	FieldTypeTimestamp = "timestamp"
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

	broadcaster broadcaster
	Stats       stats.StatsClient
	serializer  Serializer

	// Field options.
	options FieldOptions

	bsiGroups []*bsiGroup

	// Shards with data on any node in the cluster, according to this node.
	remoteAvailableShardsMu sync.Mutex
	remoteAvailableShards   *roaring.Bitmap

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
	availableShardChan chan struct{}
	wg                 sync.WaitGroup

	// track whether we're shutting down
	closing chan struct{}
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

// OptFieldTypeTimestamp is a functional option on FieldOptions
// used to specify the field as being type `timestamp` and to
// provide any respective configuration values.
func OptFieldTypeTimestamp(epoch time.Time, timeUnit string) FieldOption {
	return func(fo *FieldOptions) error {
		if fo.Type != "" {
			return errors.Errorf("field type is already set to: %s", fo.Type)
		}

		minTime := MinTimestamp
		maxTime := MaxTimestamp

		var base, minInt, maxInt int64
		switch timeUnit {
		case TimeUnitSeconds:
			base = epoch.Unix()
			minInt = minTime.Unix() - base
			maxInt = maxTime.Unix() - base
		case TimeUnitMilliseconds:
			base = epoch.UnixMilli()
			minInt = minTime.UnixMilli() - base
			maxInt = maxTime.UnixMilli() - base
		case TimeUnitMicroseconds, TimeUnitUSeconds:
			base = epoch.UnixMicro()
			minInt = minTime.UnixMicro() - base
			maxInt = maxTime.UnixMicro() - base
		case TimeUnitNanoseconds:
			// Note: For nano, the min and max values are also the min and max integer
			// values we support. Also, keep in mind that MinNano is a negative
			// number. So if base is positive and we do MinNano - base...it would increase minInt
			// beyond what we support. This isn't an issue with larger granularities.
			base = epoch.UnixNano()
			if base > 0 {
				maxInt = MaxTimestampNano.UnixNano() - base
				minInt = MinTimestampNano.UnixNano()
			} else {
				maxInt = MaxTimestampNano.UnixNano()
				minInt = MinTimestampNano.UnixNano() - base
			}
			minTime = MinTimestampNano
			maxTime = MaxTimestampNano
		default:
			return errors.Errorf("invalid time unit: '%q'", fo.TimeUnit)
		}

		if err := CheckEpochOutOfRange(epoch, minTime, maxTime); err != nil {
			return err
		}

		fo.Type = FieldTypeTimestamp
		fo.TimeUnit = timeUnit
		fo.Base = base
		fo.Min = pql.NewDecimal(minInt, 0)
		fo.Max = pql.NewDecimal(maxInt, 0)

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
//
//	0
//
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
func OptFieldTypeTime(timeQuantum TimeQuantum, ttl string, opt ...bool) FieldOption {
	return func(fo *FieldOptions) error {
		if fo.Type != "" {
			return errors.Errorf("field type is already set to: %s", fo.Type)
		}
		if !timeQuantum.Valid() {
			return ErrInvalidTimeQuantum
		}
		fo.Type = FieldTypeTime
		fo.TimeQuantum = timeQuantum
		ttlParsed, err := time.ParseDuration(ttl)
		if err != nil {
			return errors.Errorf("cannot parse ttl: %s", ttl)
		}
		if ttlParsed < 0 {
			return errors.Errorf("ttl can't be negative: %s", ttl)
		}
		fo.TTL = ttlParsed
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

		broadcaster: NopBroadcaster,
		Stats:       stats.NopStatsClient,
		serializer:  NopSerializer,

		options: applyDefaultOptions(&fo),

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

// AvailableShards returns a bitmap of shards that contain data.
func (f *Field) AvailableShards(localOnly bool) *roaring.Bitmap {
	f.mu.RLock()
	defer f.mu.RUnlock()
	f.remoteAvailableShardsMu.Lock()
	defer f.remoteAvailableShardsMu.Unlock()

	var b *roaring.Bitmap
	if localOnly {
		b = roaring.NewBitmap()
	} else {
		b = f.remoteAvailableShards.Clone()
	}
	for viewname, view := range f.viewMap {
		availableShards := view.availableShards()
		if availableShards == nil || availableShards.Containers == nil {
			f.holder.Logger.Warnf("empty available shards for view: %s on field %s available shards: %v", viewname, f.name, availableShards)
			continue
		}
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
	f.remoteAvailableShardsMu.Lock()
	defer f.remoteAvailableShardsMu.Unlock()
	f.remoteAvailableShards = f.remoteAvailableShards.Union(b)
}

// loadAvailableShards reads remoteAvailableShards data for the field, if any.
func (f *Field) loadAvailableShards() error {
	shards, err := f.holder.sharder.Shards(context.Background(), f.index, f.name)
	if err != nil {
		return errors.Wrap(err, "loading available shards")
	}

	bm := roaring.NewBitmap()
	for _, s := range shards {
		b := roaring.NewBitmap()
		if err = b.UnmarshalBinary(s); err != nil {
			return errors.Wrap(err, "available shards corrupt")
		}
		bm.UnionInPlace(b)
	}
	// Merge bitmap from file into field.
	f.mergeRemoteAvailableShards(bm)

	return nil
}

// saveAvailableShards writes remoteAvailableShards data for the field.
func (f *Field) saveAvailableShards() error {
	select {
	case f.availableShardChan <- struct{}{}:
	default:
	}
	return nil
}

// RemoveAvailableShard removes a shard from the bitmap cache.
//
// NOTE: This can be overridden on the next sync so all nodes should be updated.
func (f *Field) RemoveAvailableShard(v uint64) error {
	f.remoteAvailableShardsMu.Lock()
	defer f.remoteAvailableShardsMu.Unlock()

	b := f.remoteAvailableShards.Clone()
	if _, err := b.Remove(v); err != nil {
		return err
	}
	f.remoteAvailableShards = b

	return f.saveAvailableShards()
}

// Type returns the field type.
func (f *Field) Type() string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.options.Type
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
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := func() (err error) {
		// Ensure the field's path exists.
		f.holder.Logger.Debugf("ensure field path exists: %s", f.path)
		if err := os.MkdirAll(f.path, 0750); err != nil {
			return errors.Wrap(err, "creating field dir")
		}

		f.holder.Logger.Debugf("load available shards for index/field: %s/%s", f.index, f.name)

		if err := f.loadAvailableShards(); err != nil {
			return errors.Wrap(err, "loading available shards")
		}

		// Apply the field options loaded from etcd (or set via setOptions()).
		f.holder.Logger.Debugf("apply options for index/field: %s/%s", f.index, f.name)
		if err := f.applyOptions(f.options); err != nil {
			return errors.Wrap(err, "applying options")
		}

		f.holder.Logger.Debugf("open views for index/field: %s/%s", f.index, f.name)
		if err := f.openViews(); err != nil {
			return errors.Wrap(err, "opening views")
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

		f.availableShardChan = make(chan struct{}, 1)
		f.wg.Add(1)
		go f.writeAvailableShards()
		return nil
	}(); err != nil {
		f.unprotectedClose()
		return err
	}
	f.closing = make(chan struct{})

	_ = testhook.Opened(f.holder.Auditor, f, nil)
	f.holder.Logger.Debugf("successfully opened field index/field: %s/%s", f.index, f.name)
	return nil
}

func (f *Field) protectedRemoteAvailableShards() *roaring.Bitmap {
	f.remoteAvailableShardsMu.Lock()
	defer f.remoteAvailableShardsMu.Unlock()

	f.remoteAvailableShards.Optimize()
	return f.remoteAvailableShards.Clone()
}

func (f *Field) flushAvailableShards(ctx context.Context) {
	shards := f.protectedRemoteAvailableShards()
	var buf bytes.Buffer
	if _, err := shards.WriteTo(&buf); err != nil {
		f.holder.Logger.Errorf("writting available shards: %v", err)
		return
	}

	if err := f.holder.sharder.SetShards(ctx, f.index, f.name, buf.Bytes()); err != nil {
		f.holder.Logger.Errorf("setting available shards: %v", err)
	}
}

func (f *Field) writeAvailableShards() {
	defer f.wg.Done()

	interval := availableShardFileFlushDuration.Get()
	timer := time.NewTimer(interval)
	defer timer.Stop()

	for range f.availableShardChan {
		// Available shards have been updated.

		// Wait a bit so that we batch writes.
	timerWait:
		for {
			select {
			case _, ok := <-f.availableShardChan:
				if !ok {
					// The server is shutting down.
					// Do the write immediately.
					timer.Stop()
					break timerWait
				}

			case <-timer.C:
				// We have waited long enough.
				break timerWait
			}
		}

		// Set the timer for the next flush.
		timer.Reset(interval)

		// Actually write the shards.
		f.flushAvailableShards(context.Background())
	}
}

// applyTranslateStore opens the configured translate store.
func (f *Field) applyTranslateStore() error {
	// Instantiate & open translation store.
	var err error
	f.translateStore, err = f.OpenTranslateStore(f.TranslateStorePath(), f.index, f.name, -1, -1, f.holder.cfg.StorageConfig.FsyncEnabled)
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

// TTL returns the ttl of the field.
func (f *Field) TTL() time.Duration {
	return f.options.TTL
}

func (f *Field) bitDepth() (uint64, error) {
	var maxBitDepth uint64

	view2shards := f.idx.fieldView2shard.getViewsForField(f.name)
	for name, shardset := range view2shards {
		view := f.view(name)
		if view == nil {
			continue
		}

		bd, err := view.bitDepth(shardset.shards())
		if err != nil {
			return 0, errors.Wrapf(err, "getting view(%s) bit depth", name)
		}
		if bd > maxBitDepth {
			maxBitDepth = bd
		}
	}

	return maxBitDepth, nil
}

// cacheBitDepth is used by Index.setFieldBitDepths() to updated the in-memory
// bitDepth values for each field and its bsiGroup.
func (f *Field) cacheBitDepth(bd uint64) error {
	// Get the assocated bsiGroup so that its bitDepth can be updated as well.
	bsig := f.bsiGroup(f.name)

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.options.BitDepth < bd {
		f.options.BitDepth = bd
	}

	if bsig != nil && bsig.BitDepth < bd {
		bsig.BitDepth = bd
	}

	return nil
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

		f.holder.Logger.Debugf("add index/field/view to field.viewMap: %s/%s/%s", f.index, f.name, view.name)
		f.viewMap[view.name] = view
	}
	return nil
}

// setOptions saves options for final application during Open().
func (f *Field) setOptions(opts *FieldOptions) {
	f.options = applyDefaultOptions(opts)
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
		f.options.TTL = 0
		f.options.Keys = opt.Keys
		f.options.ForeignIndex = opt.ForeignIndex
	case FieldTypeInt, FieldTypeDecimal, FieldTypeTimestamp:
		f.options.Type = opt.Type
		f.options.CacheType = CacheTypeNone
		f.options.CacheSize = 0
		f.options.Min = opt.Min
		f.options.Max = opt.Max
		f.options.Base = opt.Base
		f.options.Scale = opt.Scale
		f.options.BitDepth = opt.BitDepth
		f.options.TimeUnit = opt.TimeUnit
		f.options.TimeQuantum = ""
		f.options.TTL = 0
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
			TimeUnit: opt.TimeUnit,
			BitDepth: opt.BitDepth,
		}
		// Validate and create bsiGroup.
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
		// Validate the time quantum.
		if !opt.TimeQuantum.Valid() {
			return ErrInvalidTimeQuantum
		}
		f.options.TimeQuantum = opt.TimeQuantum
		f.options.TTL = opt.TTL
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
		f.options.TTL = 0
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
	return f.unprotectedClose()
}

// unprotectedClose is the actual closing part of the operation, without the
// locking.
func (f *Field) unprotectedClose() error {
	if f.closing != nil {
		select {
		case <-f.closing:
			// already closed. prevent double-close
			return errors.New("double close of field")
		default:
		}
		close(f.closing)
	}
	defer func() {
		_ = testhook.Closed(f.holder.Auditor, f, nil)
	}()
	// Shutdown the available shards writer
	if f.availableShardChan != nil {
		close(f.availableShardChan)
		f.wg.Wait()
		f.availableShardChan = nil
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

func (f *Field) flushCaches() {
	// look up the close channel so if we somehow end up living until the
	// field gets reopened, we don't have a data race, but correctly detect
	// that the old one is closed.
	f.mu.RLock()
	closing := f.closing
	f.mu.RUnlock()
	for _, v := range f.views() {
		select {
		case <-closing:
			return
		default:
			v.flushCaches()
		}
	}
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
	// Append bsiGroup.
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

// viewsByTimeRange is a wrapper on the non-method viewsByTimeRange,
// which computes views for a specific field for a given time
// range. The difference is that, it can return "standard" if from/to
// are not set and can automatically coerce from/to times to match the
// actual range present in the field.
func (f *Field) viewsByTimeRange(from, to time.Time) (views []string, err error) {
	// if field is not a time field, return an error
	// If we can't find time views at all, and standard view is available,
	// yield standard view
	// if we can't find time views, and standard view is disabled,
	// yield union of all views
	// yield "standard" if from and to were both not set and there is a
	// standard view.
	q := f.TimeQuantum()
	if q == "" {
		return nil, fmt.Errorf("field %s is not a time-field, 'from' and 'to' are not valid options for this field type", f.name)
	}

	if from.IsZero() && to.IsZero() && !f.options.NoStandardView {
		return []string{viewStandard}, nil
	}

	// Get min/max based on existing views.
	var vs []string
	for _, v := range f.views() {
		vs = append(vs, v.name)
	}
	min, max := minMaxViews(vs, q)

	// If min/max are empty, there were no time views.
	if min == "" || max == "" {
		return []string{}, nil
	}

	// Convert min/max from string to time.Time.
	minTime, err := timeOfView(min, false)
	if err != nil {
		return nil, errors.Wrapf(err, "getting min time from view: %s", min)
	}
	if from.IsZero() || from.Before(minTime) {
		from = minTime
	}

	maxTime, err := timeOfView(max, true)
	if err != nil {
		return nil, errors.Wrapf(err, "getting max time from view: %s", max)
	}
	if to.IsZero() || to.After(maxTime) {
		to = maxTime
	}
	return viewsByTimeRange(viewStandard, from, to, q), nil
}

// RowTime gets the row at the particular time with the granularity specified by
// the quantum.
func (f *Field) RowTime(qcx *Qcx, rowID uint64, time time.Time, quantum string) (*Row, error) {
	if !TimeQuantum(quantum).Valid() {
		return nil, ErrInvalidTimeQuantum
	}
	viewname := viewByTimeUnit(viewStandard, time, rune(quantum[len(quantum)-1]))
	view := f.view(viewname)
	if view == nil {
		return nil, errors.Errorf("view with quantum %v not found.", quantum)
	}

	return view.row(qcx, rowID)
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
	cvm := &CreateViewMessage{
		Index: f.index,
		Field: f.name,
		View:  name,
	}

	// call this base method to isolate the mu.Lock and ensure we aren't holding
	// the lock while calling SendSync below.
	view, created, err := f.createViewIfNotExistsBase(cvm)
	if err != nil {
		return nil, err
	}

	if created {
		// Broadcast view creation to the cluster.
		err := f.holder.sendOrSpool(cvm)
		if err != nil {
			return nil, errors.Wrap(err, "sending CreateView message")
		}
	}

	return view, nil
}

// createViewIfNotExistsBase returns the named view, creating it if necessary.
// One purpose of isolating this method from createViewIfNotExists() is that we
// need to enforce the mu.Lock on everything in this method, but we can't be
// holding the lock when broadcasting the CreateViewMessage view
// broadcaster.SendSync(); calling that SendSync() while holding the lock can
// result in a deadlock waiting on the remote node to give up its lock obtained
// by performing the same action. The returned bool indicates whether the view
// was created or not.
func (f *Field) createViewIfNotExistsBase(cvm *CreateViewMessage) (*view, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// If we already have this view, we can probably assume etcd already
	// has it.
	if view := f.viewMap[cvm.View]; view != nil && !view.isClosing() {
		return view, false, nil
	}

	// Create the view in etcd as the system of record.
	// Don't persist views related to the existence field.
	if f.name != existenceFieldName {
		if err := f.persistView(context.Background(), cvm); err != nil {
			return nil, false, errors.Wrap(err, "persisting view")
		}
	}
	view := f.newView(f.viewPath(cvm.View), cvm.View)

	if err := view.openEmpty(); err != nil {
		return nil, false, errors.Wrap(err, "opening view")
	}
	f.viewMap[view.name] = view

	return view, true, nil
}

func (f *Field) newView(path, name string) *view {
	view := newView(f.holder, path, f.index, f.name, name, f.options)
	view.idx = f.idx
	view.fld = f
	view.stats = f.Stats
	view.broadcaster = f.broadcaster
	return view
}

// deleteView removes the view from the field.
func (f *Field) deleteView(name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	view := f.viewMap[name]
	if view == nil {
		return ErrInvalidView
	}

	// Delete the view from etcd as the system of record.
	if err := f.holder.Schemator.DeleteView(context.TODO(), f.index, f.name, name); err != nil {
		return errors.Wrapf(err, "deleting view from etcd: %s/%s/%s", f.index, f.name, name)
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
func (f *Field) Row(qcx *Qcx, rowID uint64) (*Row, error) {
	switch f.Type() {
	case FieldTypeSet, FieldTypeMutex, FieldTypeBool:
		view := f.view(viewStandard)
		if view == nil {
			return nil, ErrInvalidView
		}
		return view.row(qcx, rowID)
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
func (f *Field) SetBit(qcx *Qcx, rowID, colID uint64, t *time.Time) (changed bool, err error) {
	viewName := viewStandard
	if !f.options.NoStandardView {
		// Retrieve view. Exit if it doesn't exist.
		view, err := f.createViewIfNotExists(viewName)
		if err != nil {
			return changed, errors.Wrap(err, "creating view")
		}

		// Set non-time bit.
		if v, err := view.setBit(qcx, rowID, colID); err != nil {
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

		if c, err := view.setBit(qcx, rowID, colID); err != nil {
			return changed, errors.Wrapf(err, "setting on view %s", subname)
		} else if c {
			changed = true
		}
	}

	return changed, nil
}

// ClearBit clears a bit within the field.
func (f *Field) ClearBit(qcx *Qcx, rowID, colID uint64) (changed bool, err error) {
	viewName := viewStandard

	// Retrieve view. Exit if it doesn't exist.
	view, present := f.viewMap[viewName]
	if !present {
		return false, errors.Wrap(err, "clearing missing view")
	}

	// Clear non-time bit.
	if v, err := view.clearBit(qcx, rowID, colID); err != nil {
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
			cleared, err := view.clearBit(qcx, rowID, colID)
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

// ClearBits clears all bits corresponding to the given record IDs in standard
// or BSI views. It does not delete bits from time quantum views.
func (f *Field) ClearBits(tx Tx, shard uint64, recordIDs ...uint64) error {
	bsig := f.bsiGroup(f.name)
	var v *view
	if bsig != nil {
		// looks like we're a BSI field?
		v = f.view(viewBSIGroupPrefix + f.name)
	} else {
		v = f.view(viewStandard)
	}
	// it's fine if we never actually created the view, that means the
	// bits are all clear!
	if v == nil {
		return nil
	}
	frag := v.Fragment(shard)
	if frag == nil {
		return nil
	}
	_, err := frag.ClearRecords(tx, recordIDs)
	return err
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
func (f *Field) StringValue(qcx *Qcx, columnID uint64) (value string, exists bool, err error) {
	bsig := f.bsiGroup(f.name)
	if bsig == nil {
		return value, false, ErrBSIGroupNotFound
	}

	val, exists, err := f.Value(qcx, columnID)
	if exists {
		value, err = f.translateStore.TranslateID(uint64(val))
	}
	return value, exists, err
}

// Value reads a field value for a column.
func (f *Field) Value(qcx *Qcx, columnID uint64) (value int64, exists bool, err error) {
	bsig := f.bsiGroup(f.name)
	if bsig == nil {
		return 0, false, ErrBSIGroupNotFound
	}

	// Fetch target view.
	view := f.view(viewBSIGroupPrefix + f.name)
	if view == nil {
		return 0, false, nil
	}

	v, exists, err := view.value(qcx, columnID, bsig.BitDepth)
	if err != nil {
		return 0, false, err
	} else if !exists {
		return 0, false, nil
	}
	return int64(v) + bsig.Base, true, nil
}

// SetValue sets a field value for a column.
func (f *Field) SetValue(qcx *Qcx, columnID uint64, value int64) (changed bool, err error) {
	// Fetch bsiGroup & validate min/max.
	bsig := f.bsiGroup(f.name)
	if bsig == nil {
		return false, ErrBSIGroupNotFound
	}

	// Determine base value to store.
	baseValue := int64(value - bsig.Base)
	//Timestamp expects incoming value to already be relative to epoch
	if f.Type() == FieldTypeTimestamp {
		value = baseValue
	}
	if value < bsig.Min {
		return false, errors.Wrapf(ErrBSIGroupValueTooLow, "index = %v, field = %v, column ID = %v, value %v is smaller than min allowed %v", f.index, f.name, columnID, value, bsig.Min)
	} else if value > bsig.Max {
		return false, errors.Wrapf(ErrBSIGroupValueTooHigh, "index = %v, field = %v, column ID = %v, value %v is larger than max allowed %v", f.index, f.name, columnID, value, bsig.Max)
	}

	requiredBitDepth := bitDepthInt64(baseValue)

	// Increase bit depth value if the unsigned value is greater.
	if requiredBitDepth > bsig.BitDepth {
		uvalue := uint64(baseValue)
		if value < 0 {
			uvalue = uint64(-baseValue)
		}
		bitDepth := bitDepth(uvalue)

		f.mu.Lock()
		bsig.BitDepth = bitDepth
		f.options.BitDepth = bitDepth
		f.mu.Unlock()
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

	return view.setValue(qcx, columnID, bsig.BitDepth, baseValue)
}

// ClearValue removes a field value for a column.
func (f *Field) ClearValue(qcx *Qcx, columnID uint64) (changed bool, err error) {
	bsig := f.bsiGroup(f.name)
	if bsig == nil {
		return false, ErrBSIGroupNotFound
	}
	// Fetch target view.
	view := f.view(viewBSIGroupPrefix + f.name)
	if view == nil {
		return false, nil
	}
	value, exists, err := view.value(qcx, columnID, bsig.BitDepth)
	if err != nil {
		return false, err
	}
	if exists {
		return view.clearValue(qcx, columnID, bsig.BitDepth, value)
	}
	return false, nil
}

func (f *Field) MaxForShard(qcx *Qcx, shard uint64, filter *Row) (ValCount, error) {
	tx, finisher, err := qcx.GetTx(Txo{Write: true, Index: f.idx, Shard: shard})
	defer finisher(&err)
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

	max, cnt, err := fragment.max(tx, filter, bsig.BitDepth)
	if err != nil {
		return ValCount{}, errors.Wrap(err, "calling fragment.max")
	}

	v, err := f.valCountize(max, cnt, bsig)
	return v, err
}

// MinForShard returns the minimum value which appears in this shard
// (this field must be an Int or Decimal field). It also returns the
// number of times the minimum value appears.
func (f *Field) MinForShard(qcx *Qcx, shard uint64, filter *Row) (ValCount, error) {
	tx, finisher, err := qcx.GetTx(Txo{Write: true, Index: f.idx, Shard: shard})
	defer finisher(&err)
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

	min, cnt, err := fragment.min(tx, filter, bsig.BitDepth)
	if err != nil {
		return ValCount{}, errors.Wrap(err, "calling fragment.min")
	}

	v, err := f.valCountize(min, cnt, bsig)
	return v, err
}

// valCountize takes the "raw" value and count we get from the
// fragment and calculates the cooked values for this field
// (timestamping, decimaling, or just adding in the base). It always
// includes the int64 "Val\" value to make comparisons easier in the
// executor (at time of writing, Percentile takes advantage of this,
// but we might be able to simplify logic in other places as well).
func (f *Field) valCountize(val int64, cnt uint64, bsig *bsiGroup) (ValCount, error) {
	if bsig == nil {
		bsig = f.bsiGroup(f.name)
		if bsig == nil {
			return ValCount{}, ErrBSIGroupNotFound
		}

	}
	valCount := ValCount{Count: int64(cnt)}

	if f.Options().Type == FieldTypeDecimal {
		dec := pql.NewDecimal(val+bsig.Base, bsig.Scale)
		valCount.DecimalVal = &dec
	} else if f.Options().Type == FieldTypeTimestamp {
		ts, err := ValToTimestamp(f.options.TimeUnit, val+bsig.Base)
		if err != nil {
			return ValCount{}, errors.Wrap(err, "translating value to timestamp")
		}
		valCount.TimestampVal = ts
		// valCount.TimestampVal = time.Unix(0, (val+bsig.Base)*TimeUnitNanos(f.options.TimeUnit)).UTC()
	}

	valCount.Val = val + bsig.Base
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
func (f *Field) Import(qcx *Qcx, rowIDs, columnIDs []uint64, timestamps []int64, shard uint64, options *ImportOptions) (err0 error) {
	// Determine quantum if timestamps are set.
	q := f.TimeQuantum()
	if len(timestamps) > 0 {
		if q == "" {
			return errors.New("time quantum not set in field")
		} else if options.Clear {
			return errors.New("import clear is not supported with timestamps")
		}
	} else {
		// short path: if we don't have any timestamps, we only need
		// to write to exactly one view, which is always viewStandard,
		// and *every* bit goes into that view, and we already verified that
		// everything is in the same shard, so we can skip most of this.
		fieldType := f.Type()
		if fieldType == FieldTypeBool {
			for _, rowID := range rowIDs {
				if rowID > 1 {
					return errors.New("bool field imports only support values 0 and 1")
				}
			}
		}
		tx, finisher, err := qcx.GetTx(Txo{Write: true, Index: f.idx, Shard: shard})
		if err != nil {
			return errors.Wrap(err, "qcx.GetTx")
		}
		var err1 error
		defer finisher(&err1)
		view, err := f.createViewIfNotExists(viewStandard)
		if err != nil {
			return errors.Wrapf(err, "creating view %s", viewStandard)
		}

		frag, err := view.CreateFragmentIfNotExists(shard)
		if err != nil {
			return errors.Wrap(err, "creating fragment")
		}

		err1 = frag.bulkImport(tx, rowIDs, columnIDs, options)
		return err1
	}

	fieldType := f.Type()

	// Split import data by fragment.
	views := make(map[string]*importData)
	var timeStringBuf []byte
	var timeViews [][]byte
	if len(q) > 0 {
		// We're supporting time quantums, so we need to store bits in a
		// number of views for every entry with a timestamp. We want to compute
		// time quantum view names for whatever combination of YMDH views
		// we have. But we don't want to allocate four strings per entry, or
		// recompute and recreate the entire string. We know that only the
		// YYYYMMDDHH part of the string changes over time.
		timeStringBuf = make([]byte, len(viewStandard)+11)
		copy(timeStringBuf, []byte(viewStandard))
		copy(timeStringBuf[len(viewStandard):], []byte("_YYYYMMDDHH"))
		// Now we have a buffer that contains
		// `standard_YYYYMMDDHH`. We also need storage space to hold several
		// slice headers, one per entry in q. These will hold the view names
		// corresponding to each letter in q.
		timeViews = make([][]byte, len(q))
	}
	// This helper function records that a given column/row pair is relevant
	// to a specific view. We use a map lookup for the strings, but do the
	// actual operations using a slice so we're only writing each map entry
	// once, not once on every update.
	see := func(name []byte, columnID uint64, rowID uint64) {
		var ok bool
		var data *importData
		if data, ok = views[string(name)]; !ok {
			data = &importData{}
			views[string(name)] = data
		}
		data.RowIDs = append(data.RowIDs, rowID)
		data.ColumnIDs = append(data.ColumnIDs, columnID)
	}
	for i := range rowIDs {
		rowID, columnID := rowIDs[i], columnIDs[i]

		// Bool-specific data validation.
		if fieldType == FieldTypeBool && rowID > 1 {
			return errors.New("bool field imports only support values 0 and 1")
		}

		hasTime := len(timestamps) > i && timestamps[i] != 0

		// attach bit to standard view unless we have a timestamp and
		// have the NoStandardView option set
		if !hasTime || !f.options.NoStandardView {
			see([]byte(viewStandard), columnID, rowID)
		}
		if hasTime {
			// attach bit to all the views for this timestamp. note that the
			// `timeViews` slice gets resliced and reused by this process, so
			// we don't have to allocate millions of tiny slices of slice headers.
			timeViews = viewsByTimeInto(timeStringBuf, timeViews, time.Unix(0, timestamps[i]).UTC(), q)
			for _, v := range timeViews {
				see(v, columnID, rowID)
			}
		}
	}
	tx, finisher, err := qcx.GetTx(Txo{Write: true, Index: f.idx, Shard: shard})
	if err != nil {
		return errors.Wrap(err, "qcx.GetTx")
	}
	var err1 error
	defer finisher(&err1)
	for viewName, data := range views {
		view, err := f.createViewIfNotExists(viewName)
		if err != nil {
			return errors.Wrapf(err, "creating view %s", viewName)
		}

		frag, err := view.CreateFragmentIfNotExists(shard)
		if err != nil {
			return errors.Wrap(err, "creating fragment")
		}

		err1 = frag.bulkImport(tx, data.RowIDs, data.ColumnIDs, options)
		if err1 != nil {
			return err1
		}
	}
	return nil
}

// importFloatValue imports floating point values. In current usage, this
// should only ever be called with data for a single shard; the API calls
// around this are splitting it up per shard.
func (f *Field) importFloatValue(qcx *Qcx, columnIDs []uint64, values []float64, shard uint64, options *ImportOptions) error {
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
	return f.importValue(qcx, columnIDs, ivalues, shard, options)
}

// importTimestampValue imports timestamp values. In current usage, this
// should only ever be called with data for a single shard; the API calls
// around this are splitting it up per shard.
func (f *Field) importTimestampValue(qcx *Qcx, columnIDs []uint64, values []time.Time, shard uint64, options *ImportOptions) error {
	ivalues := make([]int64, len(values))
	bsig := f.bsiGroup(f.name)
	if bsig == nil {
		return errors.Wrap(ErrBSIGroupNotFound, f.name)
	}

	for i, t := range values {
		ivalues[i] = TimestampToVal(f.options.TimeUnit, t)
	}
	return f.importValue(qcx, columnIDs, ivalues, shard, options)
}

// importValue bulk imports range-encoded value data. This function should
// only be called with data for a single shard; the API calls that wrap
// this handle splitting the data up per-shard.
func (f *Field) importValue(qcx *Qcx, columnIDs []uint64, values []int64, shard uint64, options *ImportOptions) (err0 error) {
	// no data to import
	if len(columnIDs) == 0 {
		return nil
	}
	if len(values) != len(columnIDs) {
		return fmt.Errorf("importValue: mismatch between column IDs and values: %d != %d", len(columnIDs), len(values))
	}
	viewName := viewBSIGroupPrefix + f.name
	// Get the bsiGroup so we know bitDepth.
	bsig := f.bsiGroup(f.name)
	if bsig == nil {
		return errors.Wrap(ErrBSIGroupNotFound, f.name)
	}

	// We want to determine the required bit depth, in case the field doesn't
	// have as many bits currently as would be needed to represent these values,
	// but only if the values are in-range for the field.
	min, max := values[0], values[0]

	// Check for minimum/maximum in case we need to expand the field's
	// stated bit depth.
	for i := range columnIDs {
		columnID, value := columnIDs[i], values[i]
		if value > bsig.Max {
			return errors.Wrapf(ErrBSIGroupValueTooHigh, "index = %v, field = %v, column ID = %v, value %v is larger than max allowed %v", f.index, f.name, columnID, value, bsig.Max)
		} else if value < bsig.Min {
			return errors.Wrapf(ErrBSIGroupValueTooLow, "index = %v, field = %v, column ID = %v, value %v is smaller than min allowed %v", f.index, f.name, columnID, value, bsig.Min)
		}
		if value > max {
			max = value
		}
		if value < min {
			min = value
		}

	}

	// Timestamps differ from other BSI fields in that integer representations
	// of timestamps are already relative to the epoch (base).
	// So a user may set an epoch to 2022-03-01 as the start of a race
	// and import finishing times in seconds.
	// Timestamps ingested as timestamps are of course absolute, but by the time
	// we get here it would be a relative integer.
	if f.Type() != FieldTypeTimestamp {
		min -= bsig.Base
		max -= bsig.Base
	}

	// Determine the highest bit depth required by the min & max.
	requiredDepth := bitDepthInt64(min)
	if v := bitDepthInt64(max); v > requiredDepth {
		requiredDepth = v
	}
	// Increase bit depth if required.
	f.mu.Lock()
	bitDepth := bsig.BitDepth
	if requiredDepth > bitDepth {
		bsig.BitDepth = requiredDepth
		f.options.BitDepth = requiredDepth
	} else {
		requiredDepth = bitDepth
	}
	f.mu.Unlock()

	if columnIDs[0]/ShardWidth != shard {
		return fmt.Errorf("requested import for shard %d, got record ID for shard %d", shard, columnIDs[0]/ShardWidth)
	}

	view, err := f.createViewIfNotExists(viewName)
	if err != nil {
		return errors.Wrap(err, "creating view")
	}

	frag, err := view.CreateFragmentIfNotExists(shard)
	if err != nil {
		return errors.Wrap(err, "creating fragment")
	}

	if bsig.Base != 0 {
		for i, v := range values {
			// for Timestamps, values are already relative to their base (epoch)
			// for other types (IntFields), values need to be subtracted from their base (either Min or Max)
			if f.Type() == FieldTypeTimestamp {
				values[i] = v
			} else {
				values[i] = v - bsig.Base
			}
		}
	}

	// now we know which shard we discovered.
	tx, finisher, err := qcx.GetTx(Txo{Write: writable, Index: f.idx, Shard: frag.shard})
	if err != nil {
		return err
	}
	// defer the finisher, so it will check the error returned and
	// possibly rollback.
	defer finisher(&err0)

	return frag.importValue(tx, columnIDs, values, requiredDepth, options.Clear)
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

	// If field is int, decimal, or timestamp, then we need to update
	// field.options.BitDepth and bsiGroup.BitDepth based on the imported data.
	switch f.Options().Type {
	case FieldTypeInt, FieldTypeDecimal, FieldTypeTimestamp:
		frag.mu.Lock()
		maxRowID, _, err := frag.maxRow(tx, nil)
		frag.mu.Unlock()
		if err != nil {
			return err
		}

		var bitDepth uint64
		if maxRowID+1 > bsiOffsetBit {
			bitDepth = uint64(maxRowID + 1 - bsiOffsetBit)
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
	Name        string       `json:"name"`
	CreatedAt   int64        `json:"createdAt,omitempty"`
	Options     FieldOptions `json:"options"`
	Cardinality *uint64      `json:"cardinality,omitempty"`
	Views       []*ViewInfo  `json:"views,omitempty"`
}

type fieldInfoSlice []*FieldInfo

func (p fieldInfoSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p fieldInfoSlice) Len() int           { return len(p) }
func (p fieldInfoSlice) Less(i, j int) bool { return p[i].Name < p[j].Name }

// FieldOptions represents options to set when initializing a field.
type FieldOptions struct {
	Base           int64         `json:"base,omitempty"`
	BitDepth       uint64        `json:"bitDepth,omitempty"`
	Min            pql.Decimal   `json:"min,omitempty"`
	Max            pql.Decimal   `json:"max,omitempty"`
	Scale          int64         `json:"scale,omitempty"`
	Keys           bool          `json:"keys"`
	NoStandardView bool          `json:"noStandardView,omitempty"`
	CacheSize      uint32        `json:"cacheSize,omitempty"`
	CacheType      string        `json:"cacheType,omitempty"`
	Type           string        `json:"type,omitempty"`
	TimeUnit       string        `json:"timeUnit,omitempty"`
	TimeQuantum    TimeQuantum   `json:"timeQuantum,omitempty"`
	ForeignIndex   string        `json:"foreignIndex"`
	TTL            time.Duration `json:"ttl,omitempty"`
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

		case FieldTypeTimestamp:
			return nil, ErrTimestampFieldWithKeys
		}
	}

	return &fo, nil
}

// applyDefaultOptions updates FieldOptions with the default
// values if o does not contain a valid type.
func applyDefaultOptions(o *FieldOptions) FieldOptions {
	if o == nil {
		o = &FieldOptions{}
	}
	if o.Type == "" {
		o.Type = DefaultFieldType
		o.CacheType = DefaultCacheType
		o.CacheSize = DefaultCacheSize
	}
	return *o
}

// MarshalJSON marshals FieldOptions to JSON such that
// only those attributes associated to the field type
// are included.
func (o *FieldOptions) MarshalJSON() ([]byte, error) {
	switch o.Type {
	case FieldTypeSet, "":
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
			BitDepth     uint64      `json:"bitDepth"`
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
			BitDepth uint64      `json:"bitDepth"`
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
	case FieldTypeTimestamp:
		epoch, err := ValToTimestamp(o.TimeUnit, o.Base)
		if err != nil {
			return nil, errors.Wrap(err, "translating val to timestamp")
		}

		return json.Marshal(struct {
			Type     string      `json:"type"`
			Epoch    time.Time   `json:"epoch"`
			BitDepth uint64      `json:"bitDepth"`
			Min      pql.Decimal `json:"min"`
			Max      pql.Decimal `json:"max"`
			TimeUnit string      `json:"timeUnit"`
		}{
			o.Type,
			epoch,
			o.BitDepth,
			o.Min,
			o.Max,
			o.TimeUnit,
		})
	case FieldTypeTime:
		return json.Marshal(struct {
			Type           string        `json:"type"`
			TimeQuantum    TimeQuantum   `json:"timeQuantum"`
			Keys           bool          `json:"keys"`
			NoStandardView bool          `json:"noStandardView"`
			TTL            time.Duration `json:"ttl"`
		}{
			o.Type,
			o.TimeQuantum,
			o.Keys,
			o.NoStandardView,
			o.TTL,
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
	return nil, errors.Errorf("invalid field type: '%s'", o.Type)
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
	TimeUnit string `json:"timeUnit,omitempty"`
	BitDepth uint64 `json:"bitDepth,omitempty"`
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
func bitDepth(v uint64) uint64 {
	return uint64(bits.Len64(v))
}

// bitDepthInt64 returns the required bit depth for abs(v).
func bitDepthInt64(v int64) uint64 {
	if v < 0 {
		return bitDepth(uint64(-v))
	}
	return bitDepth(uint64(v))
}

// FormatQualifiedFieldName generates a qualified name for the field to be used with Tx operations.
func FormatQualifiedFieldName(index, field string) string {
	return fmt.Sprintf("%s\x00%s\x00", index, field)
}

// persistView stores the view information in etcd.
func (f *Field) persistView(ctx context.Context, cvm *CreateViewMessage) error {
	if cvm.Index == "" {
		return ErrIndexRequired
	} else if cvm.Field == "" {
		return ErrFieldRequired
	} else if cvm.View == "" {
		return ErrViewRequired
	}

	return f.holder.Schemator.CreateView(ctx, cvm.Index, cvm.Field, cvm.View)
}

// Timestamp field ranges.
var (
	DefaultEpoch     = time.Unix(0, 0).UTC()            // 1970-01-01T00:00:00Z
	MinTimestampNano = time.Unix(-1<<32, 0).UTC()       // 1833-11-24T17:31:44Z
	MaxTimestampNano = time.Unix(1<<32, 0).UTC()        // 2106-02-07T06:28:16Z
	MinTimestamp     = time.Unix(-62135596799, 0).UTC() // 0001-01-01T00:00:01Z
	MaxTimestamp     = time.Unix(253402300799, 0).UTC() // 9999-12-31T23:59:59Z
)

// Constants related to timestamp.
const (
	TimeUnitSeconds      = "s"
	TimeUnitMilliseconds = "ms"
	TimeUnitMicroseconds = "µs"
	TimeUnitUSeconds     = "us"
	TimeUnitNanoseconds  = "ns"
)

// IsValidTimeUnit returns true if unit is valid.
func IsValidTimeUnit(unit string) bool {
	switch unit {
	case TimeUnitSeconds, TimeUnitMilliseconds, TimeUnitMicroseconds, TimeUnitUSeconds, TimeUnitNanoseconds:
		return true
	default:
		return false
	}
}

// TimeUnitNanos returns the number of nanoseconds in unit.
func TimeUnitNanos(unit string) int64 {
	switch unit {
	case TimeUnitSeconds:
		return int64(time.Second)
	case TimeUnitMilliseconds:
		return int64(time.Millisecond)
	case TimeUnitMicroseconds, TimeUnitUSeconds:
		return int64(time.Microsecond)
	default:
		return int64(time.Nanosecond)
	}
}

// CheckEpochOutOfRange checks if the epoch is after max or before min
func CheckEpochOutOfRange(epoch, min, max time.Time) error {
	if epoch.After(max) || epoch.Before(min) {
		return errors.Errorf("custom epoch too far from Unix epoch: %s", epoch)
	}
	return nil
}

func (f *Field) SortShardRow(tx Tx, shard uint64, filter *Row, sort_desc bool) (*SortedRow, error) {
	bsig := f.bsiGroup(f.name)
	if bsig == nil {
		return nil, errors.New("bsig is nil")
	}

	view := f.view(viewBSIGroupPrefix + f.name)
	if view == nil {
		return nil, errors.New("view is nil")
	}

	fragment := view.Fragment(shard)
	if fragment == nil {
		return nil, errors.New("fragment is nil")
	}

	return fragment.sortBsiData(tx, filter, bsig.BitDepth, sort_desc)
}
