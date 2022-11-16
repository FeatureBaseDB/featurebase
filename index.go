// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package pilosa

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/molecula/featurebase/v3/dax"
	"github.com/molecula/featurebase/v3/disco"
	"github.com/molecula/featurebase/v3/pql"
	"github.com/molecula/featurebase/v3/roaring"
	"github.com/molecula/featurebase/v3/stats"
	"github.com/molecula/featurebase/v3/testhook"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

// Index represents a container for fields.
type Index struct {
	mu          sync.RWMutex
	createdAt   int64
	owner       string
	description string

	path          string
	name          string
	qualifiedName string
	keys          bool // use string keys

	// Existence tracking.
	trackExistence bool
	existenceFld   *Field

	// Fields by name.
	fields map[string]*Field

	broadcaster broadcaster
	serializer  Serializer
	Stats       stats.StatsClient

	// Passed to field for foreign-index lookup.
	holder *Holder

	// Per-partition translation stores
	translatePartitions dax.VersionedPartitions
	translateStores     map[int]TranslateStore

	translationSyncer TranslationSyncer

	// Instantiates new translation stores
	OpenTranslateStore OpenTranslateStoreFunc

	// track the subset of shards available to our views
	fieldView2shard *FieldView2Shards

	// indicate that we're closing and should wrap up and not allow new actions
	closing chan struct{}
}

// NewIndex returns an existing (but possibly empty) instance of
// Index at path. It will not erase any prior content.
func NewIndex(holder *Holder, path, name string) (*Index, error) {

	// Emulate what the spf13/cobra does, letting env vars override
	// the defaults, because we may be under a simple "go test" run where
	// not all that command line machinery has been spun up.

	err := ValidateName(name)
	if err != nil {
		return nil, errors.Wrap(err, "validating name")
	}

	idx := &Index{
		path:   path,
		name:   name,
		fields: make(map[string]*Field),

		broadcaster:    NopBroadcaster,
		Stats:          stats.NopStatsClient,
		holder:         holder,
		trackExistence: true,

		serializer: NopSerializer,

		translateStores: make(map[int]TranslateStore),

		translationSyncer: NopTranslationSyncer,

		OpenTranslateStore: OpenInMemTranslateStore,
	}
	return idx, nil
}

func (i *Index) NewTx(txo Txo) Tx {
	return i.holder.txf.NewTx(txo)
}

// CreatedAt is an timestamp for a specific version of an index.
func (i *Index) CreatedAt() int64 {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.createdAt
}

// Name returns name of the index.
func (i *Index) Name() string { return i.name }

// Holder yields this index's Holder.
func (i *Index) Holder() *Holder { return i.holder }

// QualifiedName returns the qualified name of the index.
func (i *Index) QualifiedName() string { return i.qualifiedName }

// Path returns the path the index was initialized with.
func (i *Index) Path() string {
	return i.path
}

// FieldsPath returns the path of the fields directory.
func (i *Index) FieldsPath() string {
	return filepath.Join(i.path, FieldsDir)
}

// TranslateStorePath returns the translation database path for a partition.
func (i *Index) TranslateStorePath(partitionID int) string {
	return filepath.Join(i.path, translateStoreDir, strconv.Itoa(partitionID))
}

// TranslateStore returns the translation store for a given partition.
func (i *Index) TranslateStore(partitionID int) TranslateStore {
	i.mu.RLock() // avoid race with Index.Close() doing i.translateStores = make(map[int]TranslateStore)
	defer i.mu.RUnlock()
	return i.translateStores[partitionID]
}

// Keys returns true if the index uses string keys.
func (i *Index) Keys() bool { return i.keys }

// Options returns all options for this index.
func (i *Index) Options() IndexOptions {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.options()
}

func (i *Index) options() IndexOptions {
	return IndexOptions{
		Description:    i.description,
		Keys:           i.keys,
		TrackExistence: i.trackExistence,
	}
}

// Open opens and initializes the index.
func (i *Index) Open() error {
	return i.open(nil)
}

// OpenWithSchema opens the index and uses the provided schema to verify that
// the index's fields are expected.
func (i *Index) OpenWithSchema(idx *disco.Index) error {
	if idx == nil {
		return ErrInvalidSchema
	}

	// decode the CreateIndexMessage from the schema data in order to
	// get its metadata.
	cim, err := decodeCreateIndexMessage(i.serializer, idx.Data)
	if err != nil {
		return errors.Wrap(err, "decoding create index message")
	}
	i.createdAt = cim.CreatedAt
	i.trackExistence = cim.Meta.TrackExistence
	i.keys = cim.Meta.Keys

	return i.open(idx)
}

// open opens the index with an optional schema (disco.Index). If a schema is
// provided, it will apply the metadata from the schema to the index, and then
// open all fields found in the schema. If a schema is not provided, the
// metadata for the index is not changed from its existing value, and fields are
// not validated against the schema as they are opened.
func (i *Index) open(idx *disco.Index) (err error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	// Ensure the path exists.
	i.holder.Logger.Debugf("ensure index path exists: %s", i.FieldsPath())
	if err := os.MkdirAll(i.FieldsPath(), 0750); err != nil {
		return errors.Wrap(err, "creating directory")
	}
	i.closing = make(chan struct{})
	// fmt.Printf("new channel %p for index %p\n", i.closing, i)

	// we don't want to open *all* the views for each shard, since
	// most are empty when we are doing time quantums. It slows
	// down startup dramatically. So we ask for the meta data
	// of what fields/views/shards are present with data up front.
	fieldView2shard, err := i.holder.txf.GetFieldView2ShardsMapForIndex(i)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("i.holder.txf.GetFieldView2ShardsMapForIndex('%v')", i.name))
	}
	i.fieldView2shard = fieldView2shard

	// Add index to a map in holder. Used by openFields.
	i.holder.addIndex(i)

	i.holder.Logger.Debugf("open fields for index: %s", i.name)
	if err := i.openFields(idx); err != nil {
		return errors.Wrap(err, "opening fields")
	}

	// Set bit depths.
	// This is called in Index.open() (as opposed to Field.Open()) because the
	// Field.bitDepth() method uses a transaction which relies on the index and
	// its entry for the field in the Index.field map. If we try to set a
	// field's BitDepth in Field.Open(), which itself might be inside the
	// Index.openField() loop, then the field has not yet been added to the
	// Index.field map. I think it would be better if Field.bitDepth didn't rely
	// on its index at all, but perhaps with transactions that not possible. I
	// don't know.
	if err := i.setFieldBitDepths(); err != nil {
		return errors.Wrap(err, "setting field bitDepths")
	}

	if i.trackExistence {
		if err := i.openExistenceField(); err != nil {
			return errors.Wrap(err, "opening existence field")
		}
	}

	if i.keys {
		i.holder.Logger.Debugf("open translate store for index: %s", i.name)

		var g errgroup.Group
		var mu sync.Mutex
		// TODO(tlt): this for loop doesn't work because if we assign a
		// translate partition to this node later (after the table has been
		// created with a sub-set of translatePartitions), then the new
		// TranslateStores don't get initialized. For now I just put it back so
		// it opens a TranslateStore for every partition no matter what, but we
		// really need to have the ApplyDirective logic able to initialize any
		// TranslateStore which doesn't already exist (and perhaps shut down any
		// that are to be removed).
		//
		// for _, partition := range i.translatePartitions {
		//	  partitionID := int(partition.Num)
		//
		//
		// TODO(tlt): instead of i.holder.partitionN, we need to use
		// len(i.translatePartitions), or actually we need to know the
		// keypartitions for the qtbl (i don't think we can rely on the length
		// of this slice) but that will only apply here... we need to go through
		// all the code and see where these are being used:
		// - i.holder.partitionN
		// - DefaultPartitionN
		//
		//
		for partitionID := 0; partitionID < i.holder.partitionN; partitionID++ {
			partitionID := partitionID

			g.Go(func() error {
				store, err := i.OpenTranslateStore(i.TranslateStorePath(partitionID), i.name, "", partitionID, i.holder.partitionN, i.holder.cfg.StorageConfig.FsyncEnabled)
				if err != nil {
					return errors.Wrapf(err, "opening index translate store: partition=%d", partitionID)
				}

				mu.Lock()
				defer mu.Unlock()
				i.translateStores[partitionID] = store
				return nil
			})
		}
		if err := g.Wait(); err != nil {
			return err
		}
	}

	_ = testhook.Opened(i.holder.Auditor, i, nil)
	return nil
}

var indexQueue = make(chan struct{}, 8)

// openFields opens and initializes the fields inside the index.
func (i *Index) openFields(idx *disco.Index) error {
	eg, ctx := errgroup.WithContext(context.Background())
	var mu sync.Mutex

	if idx == nil {
		return nil
	}
fileLoop:
	for fname, fld := range idx.Fields {
		lfname := fname
		select {
		case <-ctx.Done():
			break fileLoop
		default:
			// Decode the CreateFieldMessage from the schema data in order to
			// get its metadata.
			cfm, err := decodeCreateFieldMessage(i.holder.serializer, fld.Data)
			if err != nil {
				return errors.Wrap(err, "decoding create field message")
			}

			indexQueue <- struct{}{}
			eg.Go(func() error {
				defer func() {
					<-indexQueue
				}()
				i.holder.Logger.Debugf("open field: %s", lfname)

				_, err := i.openField(&mu, cfm, lfname)
				if err != nil {
					return errors.Wrap(err, "opening field")
				}

				return nil
			})
		}
	}

	err := eg.Wait()
	if err != nil {
		// Close any fields which got opened, since the overall
		// index won't be open.
		for n, f := range i.fields {
			f.Close()
			delete(i.fields, n)
		}
	}
	return err
}

// openField opens the field directory, initializes the field, and adds it to
// the in-memory map of fields maintained by Index.
func (i *Index) openField(mu *sync.Mutex, cfm *CreateFieldMessage, file string) (*Field, error) {
	mu.Lock()
	fld, err := i.newField(i.fieldPath(filepath.Base(file)), filepath.Base(file))
	mu.Unlock()
	if err != nil {
		return nil, errors.Wrapf(ErrName, "'%s'", file)
	}

	// Pass holder through to the field for use in looking
	// up a foreign index.
	fld.holder = i.holder

	fld.createdAt = cfm.CreatedAt
	fld.options = applyDefaultOptions(cfm.Meta)

	// open the views we have data for.
	if err := fld.Open(); err != nil {
		return nil, fmt.Errorf("open field: name=%s, err=%s", fld.Name(), err)
	}

	i.holder.Logger.Debugf("add field to index.fields: %s", file)
	mu.Lock()
	i.fields[fld.Name()] = fld
	mu.Unlock()

	return fld, nil
}

// openExistenceField gets or creates the existence field and associates it to the index.
func (i *Index) openExistenceField() error {
	cfm := &CreateFieldMessage{
		Index:     i.name,
		Field:     existenceFieldName,
		Owner:     "",
		CreatedAt: 0,
		Meta:      &FieldOptions{Type: FieldTypeSet, CacheType: CacheTypeNone, CacheSize: 0},
	}

	// First try opening the existence field from disk. If it doesn't already
	// exist on disk, then we fall through to the code path which creates it.
	var mu sync.Mutex
	fld, err := i.openField(&mu, cfm, existenceFieldName)
	if err == nil {
		i.existenceFld = fld
		return nil
	} else if errors.Cause(err) != ErrName {
		return errors.Wrap(err, "opening existence file")
	}

	// If we have gotten here, it means that we couldn't successfully open the
	// existence field from disk, so we need to create it.

	f, err := i.createFieldIfNotExists(cfm)
	if err != nil {
		return errors.Wrap(err, "creating existence field")
	}
	i.existenceFld = f
	return nil
}

// setFieldBitDepths sets the BitDepth for all int and decimal fields in the index.
func (i *Index) setFieldBitDepths() error {
	for name, f := range i.fields {
		switch f.Type() {
		case FieldTypeInt, FieldTypeDecimal, FieldTypeTimestamp:
			// pass
		default:
			continue
		}
		bd, err := f.bitDepth()
		if err != nil {
			return errors.Wrapf(err, "getting bit depth for field: %s", name)
		}
		if err := f.cacheBitDepth(bd); err != nil {
			return errors.Wrapf(err, "caching field bitDepth: %d", bd)
		}
	}
	return nil
}

// Close closes the index and its fields.
func (i *Index) Close() error {
	i.mu.Lock()
	defer i.mu.Unlock()
	// flag that we're trying to shut down
	if i.closing != nil {
		select {
		case <-i.closing:
			// already closed. prevent double-close
			return errors.New("double close of index")
		default:
		}
		close(i.closing)
	}
	defer func() {
		_ = testhook.Closed(i.holder.Auditor, i, nil)
	}()

	err := i.holder.txf.CloseIndex(i)
	if err != nil {
		return errors.Wrap(err, "closing index")
	}

	// Close partitioned translation stores.
	for _, store := range i.translateStores {
		if err := store.Close(); err != nil {
			return errors.Wrap(err, "closing translation store")
		}
	}
	i.translateStores = make(map[int]TranslateStore)

	// Close all fields.
	for _, f := range i.fields {
		if err := f.Close(); err != nil {
			return errors.Wrap(err, "closing field")
		}
	}
	i.fields = make(map[string]*Field)

	return nil
}

func (i *Index) flushCaches() {
	// look up the close channel so if we somehow end up living until the
	// index gets reopened, we don't have a data race, but correctly detect
	// that the old one is closed.
	i.mu.RLock()
	closing := i.closing
	i.mu.RUnlock()
	for _, field := range i.Fields() {
		select {
		case <-closing:
			return
		default:
			field.flushCaches()
		}
	}
}

// make it clear what the Index.AvailableShards() calls are trying to obtain.
const includeRemote = false

// AvailableShards returns a bitmap of all shards with data in the index.
func (i *Index) AvailableShards(localOnly bool) *roaring.Bitmap {
	if i == nil {
		return roaring.NewBitmap()
	}

	i.mu.RLock()
	defer i.mu.RUnlock()

	b := roaring.NewBitmap()
	for _, f := range i.fields {
		//b.Union(f.AvailableShards(localOnly))
		b.UnionInPlace(f.AvailableShards(localOnly))
	}

	i.Stats.Gauge(MetricMaxShard, float64(b.Max()), 1.0)
	return b
}

// Begin starts a transaction on a shard of the index.
func (i *Index) BeginTx(writable bool, shard uint64) (Tx, error) {
	return i.holder.txf.NewTx(Txo{Write: writable, Index: i, Shard: shard}), nil
}

// fieldPath returns the path to a field in the index.
func (i *Index) fieldPath(name string) string { return filepath.Join(i.FieldsPath(), name) }

// Field returns a field in the index by name.
func (i *Index) Field(name string) *Field {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.field(name)
}

func (i *Index) field(name string) *Field {
	return i.fields[name]
}

// Fields returns a list of all fields in the index.
func (i *Index) Fields() []*Field {
	i.mu.RLock()
	defer i.mu.RUnlock()

	a := make([]*Field, 0, len(i.fields))
	for _, f := range i.fields {
		a = append(a, f)
	}
	sort.Sort(fieldSlice(a))

	return a
}

// existenceField returns the internal field used to track column existence.
func (i *Index) existenceField() *Field {
	i.mu.RLock()
	defer i.mu.RUnlock()

	return i.existenceFld
}

// recalculateCaches recalculates caches on every field in the index.
func (i *Index) recalculateCaches() {
	for _, field := range i.Fields() {
		field.recalculateCaches()
	}
}

// CreateField creates a field.
func (i *Index) CreateField(name string, requestUserID string, opts ...FieldOption) (*Field, error) {
	err := ValidateName(name)
	if err != nil {
		return nil, errors.Wrap(err, "validating name")
	}

	// Grab lock, check for field existing, release lock. We don't want
	// to stay holding the lock, but we might care about the ErrFieldExists
	// part of this.
	err = func() error {
		i.mu.Lock()
		defer i.mu.Unlock()

		// Ensure field doesn't already exist.
		if i.fields[name] != nil {
			return newConflictError(ErrFieldExists)
		}
		return nil
	}()
	if err != nil {
		return nil, err
	}

	// Apply and validate functional options.
	fo, err := newFieldOptions(opts...)
	if err != nil {
		return nil, errors.Wrap(err, "applying option")
	}

	ts := timestamp()
	cfm := &CreateFieldMessage{
		Index:     i.name,
		Field:     name,
		CreatedAt: ts,
		Owner:     requestUserID,
		Meta:      fo,
	}

	// Create the field in etcd as the system of record. We do this without
	// the lock held because it can take an arbitrary amount of time...
	if err := i.persistField(context.Background(), cfm); errors.Cause(err) == ErrFieldExists {
		return nil, newConflictError(ErrFieldExists)
	} else if err != nil {
		return nil, errors.Wrap(err, "persisting field")
	}

	// This is identical to the previous check, because we could get super
	// unlucky and have the persist-field thing happen, and somehow the field
	// gets created, before we get to run again, and the specific nature of
	// the error can matter to the backend.
	i.mu.Lock()
	defer i.mu.Unlock()

	// Ensure field doesn't already exist.
	if i.fields[name] != nil {
		return nil, newConflictError(ErrFieldExists)
	}

	// Actually do the internal bookkeeping.
	return i.createField(cfm)
}

// CreateFieldIfNotExists creates a field with the given options if it doesn't exist.
func (i *Index) CreateFieldIfNotExists(name string, requestUserID string, opts ...FieldOption) (*Field, error) {
	err := ValidateName(name)
	if err != nil {
		return nil, errors.Wrap(err, "validating name")
	}

	i.mu.Lock()
	defer i.mu.Unlock()

	// Find field in cache first.
	if f := i.fields[name]; f != nil {
		return f, nil
	}

	// Apply and validate functional options.
	fo, err := newFieldOptions(opts...)
	if err != nil {
		return nil, errors.Wrap(err, "applying option")
	}

	ts := timestamp()
	cfm := &CreateFieldMessage{
		Index:     i.name,
		Field:     name,
		CreatedAt: ts,
		Owner:     requestUserID,
		Meta:      fo,
	}

	// Create the field in etcd as the system of record.
	if err := i.persistField(context.Background(), cfm); err != nil && errors.Cause(err) != ErrFieldExists {
		// There is a case where the index is not in memory, but it is in
		// persistent storage. In that case, this will return an "index exists"
		// error, which in that case should return the index. TODO: We may need
		// to allow for that in the future.
		return nil, errors.Wrap(err, "persisting field")
	}

	return i.createField(cfm)
}

// CreateFieldIfNotExistsWithOptions is a method which I created because I
// needed the functionality of CreateFieldIfNotExists, but instead of taking
// function options, taking a *FieldOptions struct. TODO: This should
// definintely be refactored so we don't have these virtually equivalent
// methods, but I'm puttin this here for now just to see if it works.
func (i *Index) CreateFieldIfNotExistsWithOptions(name string, requestUserID string, opt *FieldOptions) (*Field, error) {
	err := ValidateName(name)
	if err != nil {
		return nil, errors.Wrap(err, "validating name")
	}

	i.mu.Lock()
	defer i.mu.Unlock()

	// Find field in cache first.
	if f := i.fields[name]; f != nil {
		return f, nil
	}
	if opt != nil && (opt.Type == FieldTypeInt || opt.Type == FieldTypeTimestamp) {
		min, max := pql.MinMax(0)
		// ensure the provided bounds are valid
		zero := big.NewInt(0)
		maxv := opt.Max.Value()
		if maxv.Cmp(zero) == 0 {
			opt.Max = max
		} else if max.LessThan(opt.Max) {
			opt.Max = max
		}
		minv := opt.Min.Value()
		if minv.Cmp(zero) == 0 {
			opt.Min = min
		} else if min.GreaterThan(opt.Min) {
			opt.Min = min
		}
	}
	// added for backward compatablity with old schemas
	if opt != nil && opt.Type == FieldTypeDecimal {
		min, max := pql.MinMax(opt.Scale)
		zero := big.NewInt(0)

		// ensure the provided bounds are valid
		maxv := opt.Max.Value()
		if maxv.Cmp(zero) == 0 {
			opt.Max = max
		} else if max.LessThan(opt.Max) {
			opt.Max = max
		}

		minv := opt.Min.Value()
		if minv.Cmp(zero) == 0 {
			opt.Min = min
		} else if min.GreaterThan(opt.Min) {
			opt.Min = min
		}
	}

	ts := timestamp()
	cfm := &CreateFieldMessage{
		Index:     i.name,
		Field:     name,
		CreatedAt: ts,
		Owner:     requestUserID,
		Meta:      opt,
	}

	// Create the field in etcd as the system of record.
	if err := i.persistField(context.Background(), cfm); err != nil && errors.Cause(err) != ErrFieldExists {
		// There is a case where the index is not in memory, but it is in
		// persistent storage. In that case, this will return an "index exists"
		// error, which in that case should return the index. TODO: We may need
		// to allow for that in the future.
		return nil, errors.Wrap(err, "persisting field")
	}

	return i.createField(cfm)
}

// persistField stores the field information in etcd.
func (i *Index) persistField(ctx context.Context, cfm *CreateFieldMessage) error {
	if cfm.Index == "" {
		return ErrIndexRequired
	} else if cfm.Field == "" {
		return ErrFieldRequired
	}

	if err := ValidateName(cfm.Field); err != nil {
		return errors.Wrap(err, "validating name")
	}

	if b, err := i.serializer.Marshal(cfm); err != nil {
		return errors.Wrap(err, "marshaling field")
	} else if err := i.holder.Schemator.CreateField(ctx, cfm.Index, cfm.Field, b); errors.Cause(err) == disco.ErrFieldExists {
		return ErrFieldExists
	} else if err != nil {
		return errors.Wrapf(err, "writing field to disco: %s/%s", cfm.Index, cfm.Field)
	}
	return nil
}

func (i *Index) persistUpdateField(ctx context.Context, cfm *CreateFieldMessage) error {
	if cfm.Index == "" {
		return ErrIndexRequired
	} else if cfm.Field == "" {
		return ErrFieldRequired
	}

	if b, err := i.serializer.Marshal(cfm); err != nil {
		return errors.Wrap(err, "marshaling field")
	} else if err := i.holder.Schemator.UpdateField(ctx, cfm.Index, cfm.Field, b); errors.Cause(err) == disco.ErrFieldDoesNotExist {
		return ErrFieldNotFound
	} else if err != nil {
		return errors.Wrapf(err, "writing field to disco: %s/%s", cfm.Index, cfm.Field)
	}
	return nil
}

func (i *Index) UpdateField(ctx context.Context, name string, requestUserID string, update FieldUpdate) (*CreateFieldMessage, error) {
	// Get field from etcd
	buf, err := i.holder.Schemator.Field(ctx, i.name, name)
	if err != nil {
		return nil, errors.Wrapf(err, "getting field '%s' from etcd", name)
	}
	cfm, err := decodeCreateFieldMessage(i.holder.serializer, buf)
	if err != nil {
		return nil, errors.Wrap(err, "decoding CreateFieldMessage")
	} else if cfm == nil {
		return nil, errors.New("got nil CreateFieldMessage when decoding")
	}

	// Handle the options we know how to update, or error.
	switch update.Option {
	case "TTL", "ttl":
		if cfm.Meta.Type != FieldTypeTime {
			return nil, NewBadRequestError(errors.Errorf("can only add TTL to a 'time' type field, not '%s'", cfm.Meta.Type))
		}
		dur, err := time.ParseDuration(update.Value)
		if err != nil {
			return nil, NewBadRequestError(errors.Wrap(err, "parsing duration"))
		}
		if dur < 0 {
			return nil, NewBadRequestError(errors.Errorf("ttl can't be negative: '%s'", update.Value))
		}
		cfm.Meta.TTL = dur
	case "noStandardView":
		if cfm.Meta.Type != FieldTypeTime {
			return nil, NewBadRequestError(errors.Errorf("can only update 'noStandardView' on a 'time' type field, not '%s'", cfm.Meta.Type))
		}
		boolValue, err := strconv.ParseBool(update.Value)
		if err != nil {
			return nil, NewBadRequestError(errors.Errorf("invalid value for noStandardView: '%s'", update.Value))
		}
		cfm.Meta.NoStandardView = boolValue
	default:
		return nil, NewBadRequestError(errors.Errorf("updates for option '%s' are not supported", update.Option))
	}

	// Persist the updated field to etcd.
	if err := i.persistUpdateField(ctx, cfm); err != nil {
		return nil, errors.Wrap(err, "persisting updated field")
	}

	i.mu.Lock()
	defer i.mu.Unlock()

	return cfm, nil
}

func (i *Index) UpdateFieldLocal(cfm *CreateFieldMessage, update FieldUpdate) error {
	// Update local structures. This assumes we don't need to do
	// anything else... which is fine for TTL specifically, but I'm
	// not sure about other things, so be aware when adding new update
	// abilities.
	i.mu.Lock()
	defer i.mu.Unlock()
	field := i.field(cfm.Field)
	if field == nil {
		return errors.Errorf("field '%s' not found locally", cfm.Field)
	}
	if err := field.applyOptions(*cfm.Meta); err != nil {
		return errors.Wrap(err, "updating local field options")
	}

	return nil

}

// createFieldIfNotExists creates the field if it does not already exist in the
// in-memory index structure. This is not related to whether or not the field
// exists in etcd.
func (i *Index) createFieldIfNotExists(cfm *CreateFieldMessage) (*Field, error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	// Find field in cache first.
	if f := i.fields[cfm.Field]; f != nil {
		return f, nil
	}

	return i.createField(cfm)
}

// createField does the internal field creation logic, creating the in-memory
// data structure, and kicking translation sync if appropriate. It does not
// notify other nodes; that's done from the API's initial CreateField call
// now.
func (i *Index) createField(cfm *CreateFieldMessage) (*Field, error) {
	opt := cfm.Meta
	if opt == nil {
		opt = &FieldOptions{}
	}

	// TODO: can we do a general FieldOption validation here instead of just cache type?
	if cfm.Field == "" {
		return nil, errors.New("field name required")
	} else if opt.CacheType != "" && !isValidCacheType(opt.CacheType) {
		return nil, ErrInvalidCacheType
	}

	// Initialize field.
	f, err := i.newField(i.fieldPath(cfm.Field), cfm.Field)
	if err != nil {
		return nil, errors.Wrap(err, "initializing")
	}
	f.createdAt = cfm.CreatedAt
	f.owner = cfm.Owner

	// Pass holder through to the field for use in looking
	// up a foreign index.
	f.holder = i.holder

	f.setOptions(opt)

	// Open field.
	if err := f.Open(); err != nil {
		return nil, errors.Wrap(err, "opening")
	}

	// Add to index's field lookup.
	i.fields[cfm.Field] = f

	// enable Txf to find the index in field_test.go TestField_SetValue
	f.idx = i

	// Kick off the field's translation sync process.
	if err := i.translationSyncer.Reset(); err != nil {
		return nil, errors.Wrap(err, "resetting translation syncer")
	}

	return f, nil
}

func (i *Index) newField(path, name string) (*Field, error) {
	f, err := newField(i.holder, path, i.name, name, OptFieldTypeDefault())
	if err != nil {
		return nil, err
	}
	f.idx = i
	f.Stats = i.Stats
	f.broadcaster = i.broadcaster
	f.serializer = i.serializer
	f.OpenTranslateStore = i.OpenTranslateStore
	return f, nil
}

// DeleteField removes a field from the index.
func (i *Index) DeleteField(name string) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	// Disallow deleting the existence field.
	if name == existenceFieldName {
		return newNotFoundError(ErrFieldNotFound, existenceFieldName)
	}

	// Confirm field exists.
	f := i.field(name)
	if f == nil {
		return newNotFoundError(ErrFieldNotFound, name)
	}

	// Delete the field from etcd as the system of record.
	if err := i.holder.Schemator.DeleteField(context.TODO(), i.name, name); err != nil {
		return errors.Wrapf(err, "deleting field from etcd: %s/%s", i.name, name)
	}

	// Close field.
	if err := f.Close(); err != nil {
		return errors.Wrap(err, "closing")
	}

	if err := i.holder.txf.DeleteFieldFromStore(i.name, name, i.fieldPath(name)); err != nil {
		return errors.Wrap(err, "Txf.DeleteFieldFromStore")
	}

	// Remove reference.
	delete(i.fields, name)

	// remove shard metadata for field
	i.fieldView2shard.removeField(name)
	return i.translationSyncer.Reset()
}

// SetTranslatePartitions sets the cached value: translatePartitions.
//
// There's already logic in api_directive.go which creates a new index with
// partitions. This particular function is used when the index already exists on
// the node, but we get a Directive which changes its partition list. In that
// case, we need to update this cached value. Really, this is kind of hacky and
// we need to revisit the ApplyDirective logic so that it's more intuitive with
// respect to index.translatePartitions.
func (i *Index) SetTranslatePartitions(tp dax.VersionedPartitions) {
	i.mu.Lock()
	defer i.mu.Unlock()

	i.translatePartitions = tp
}

type indexSlice []*Index

func (p indexSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p indexSlice) Len() int           { return len(p) }
func (p indexSlice) Less(i, j int) bool { return p[i].Name() < p[j].Name() }

// IndexInfo represents schema information for an index.
type IndexInfo struct {
	Name           string       `json:"name"`
	CreatedAt      int64        `json:"createdAt,omitempty"`
	UpdatedAt      int64        `json:"updatedAt"`
	Owner          string       `json:"owner"`
	LastUpdateUser string       `json:"lastUpdatedUser"`
	Options        IndexOptions `json:"options"`
	Fields         []*FieldInfo `json:"fields"`
	ShardWidth     uint64       `json:"shardWidth"`
}

// Field returns the FieldInfo the provided field name. If the field does not
// exist, it returns nil
func (ii *IndexInfo) Field(name string) *FieldInfo {
	for _, fld := range ii.Fields {
		if fld.Name == name {
			return fld
		}
	}
	return nil
}

type indexInfoSlice []*IndexInfo

func (p indexInfoSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p indexInfoSlice) Len() int           { return len(p) }
func (p indexInfoSlice) Less(i, j int) bool { return p[i].Name < p[j].Name }

// IndexOptions represents options to set when initializing an index.
type IndexOptions struct {
	Keys           bool   `json:"keys"`
	TrackExistence bool   `json:"trackExistence"`
	PartitionN     int    `json:"partitionN"`
	Description    string `json:"description"`
}

type importData struct {
	RowIDs    []uint64
	ColumnIDs []uint64
}

// FormatQualifiedIndexName generates a qualified name for the index to be used with Tx operations.
func FormatQualifiedIndexName(index string) string {
	return fmt.Sprintf("%s\x00", index)
}
