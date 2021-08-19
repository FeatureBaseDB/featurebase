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
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"

	"github.com/molecula/featurebase/v2/disco"
	"github.com/molecula/featurebase/v2/roaring"
	"github.com/molecula/featurebase/v2/stats"
	"github.com/molecula/featurebase/v2/testhook"
	. "github.com/molecula/featurebase/v2/vprint" // nolint:staticcheck
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

// Index represents a container for fields.
type Index struct {
	mu            sync.RWMutex
	createdAt     int64
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
	Schemator   disco.Schemator
	serializer  Serializer
	Stats       stats.StatsClient

	// Passed to field for foreign-index lookup.
	holder *Holder

	// Per-partition translation stores
	translateStores map[int]TranslateStore

	translationSyncer TranslationSyncer

	// Instantiates new translation stores
	OpenTranslateStore OpenTranslateStoreFunc

	// track the subset of shards available to our views
	fieldView2shard *FieldView2Shards
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

		Schemator:  disco.InMemSchemator,
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

func (i *Index) NeedsSnapshot() bool {
	return i.holder.txf.NeedsSnapshot()
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
	// Ensure the path exists.
	i.holder.Logger.Debugf("ensure index path exists: %s", i.FieldsPath())
	if err := os.MkdirAll(i.FieldsPath(), 0777); err != nil {
		return errors.Wrap(err, "creating directory")
	}

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
		for partitionID := 0; partitionID < i.holder.partitionN; partitionID++ {
			partitionID := partitionID

			g.Go(func() error {
				store, err := i.OpenTranslateStore(i.TranslateStorePath(partitionID), i.name, "", partitionID, i.holder.partitionN)
				if err != nil {
					return errors.Wrapf(err, "opening index translate store: partition=%d", partitionID)
				}

				mu.Lock()
				defer mu.Unlock()

				i.mu.Lock()
				defer i.mu.Unlock()
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
	f, err := os.Open(i.FieldsPath())
	if err != nil {
		return errors.Wrap(err, "opening fields directory")
	}
	defer f.Close()

	fis, err := f.Readdir(0)
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

			var cfm *CreateFieldMessage = &CreateFieldMessage{}
			var err error

			// Only continue with fields which are present in the provided,
			// non-nil index schema. The reason we have to check for idx != nil
			// here is because there are tests which call index.Open without
			// having a disco.Index available.
			if idx != nil {
				fld, ok := idx.Fields[fi.Name()]
				if !ok {
					continue
				}

				// Decode the CreateFieldMessage from the schema data in order to
				// get its metadata.
				cfm, err = decodeCreateFieldMessage(i.holder.serializer, fld.Data)
				if err != nil {
					return errors.Wrap(err, "decoding create field message")
				}
			}

			indexQueue <- struct{}{}
			eg.Go(func() error {
				defer func() {
					<-indexQueue
				}()
				i.holder.Logger.Debugf("open field: %s", fi.Name())

				_, err := i.openField(&mu, cfm, fi.Name())
				if err != nil {
					return errors.Wrap(err, "opening field")
				}

				return nil
			})
		}
	}
	err = eg.Wait()
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
	i.mu.Lock()
	i.fields[fld.Name()] = fld
	i.mu.Unlock()

	return fld, nil
}

// openExistenceField gets or creates the existence field and associates it to the index.
func (i *Index) openExistenceField() error {
	cfm := &CreateFieldMessage{
		Index:     i.name,
		Field:     existenceFieldName,
		CreatedAt: 0,
		Meta:      &FieldOptions{CacheType: CacheTypeNone, CacheSize: 0},
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

// make it clear what the Index.AvailableShards() calls are trying to obtain.
const includeRemote = false
const localOnly = true

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
func (i *Index) CreateField(name string, opts ...FieldOption) (*Field, error) {
	err := ValidateName(name)
	if err != nil {
		return nil, errors.Wrap(err, "validating name")
	}

	i.mu.Lock()
	defer i.mu.Unlock()

	// Ensure field doesn't already exist.
	if i.fields[name] != nil {
		return nil, newConflictError(ErrFieldExists)
	}

	// Apply and validate functional options.
	fo, err := newFieldOptions(opts...)
	if err != nil {
		return nil, errors.Wrap(err, "applying option")
	}

	cfm := &CreateFieldMessage{
		Index:     i.name,
		Field:     name,
		CreatedAt: timestamp(),
		Meta:      fo,
	}

	// Create the field in etcd as the system of record.
	if err := i.persistField(context.Background(), cfm); err != nil {
		return nil, errors.Wrap(err, "persisting field")
	}

	return i.createField(cfm, false)
}

// CreateFieldAndBroadcast creates a field locally, then broadcasts the
// creation to other nodes so they can create locally as well. An error is
// returned if the field already exists.
func (i *Index) CreateFieldAndBroadcast(cfm *CreateFieldMessage) (*Field, error) {
	err := ValidateName(cfm.Field)
	if err != nil {
		return nil, errors.Wrap(err, "validating name")
	}

	i.mu.Lock()
	defer i.mu.Unlock()

	// Ensure field doesn't already exist.
	if i.fields[cfm.Field] != nil {
		return nil, newConflictError(ErrFieldExists)
	}

	// Create the field in etcd as the system of record.
	if err := i.persistField(context.Background(), cfm); err != nil {
		return nil, errors.Wrap(err, "persisting field")
	}

	return i.createField(cfm, true)
}

// CreateFieldIfNotExists creates a field with the given options if it doesn't exist.
func (i *Index) CreateFieldIfNotExists(name string, opts ...FieldOption) (*Field, error) {
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

	cfm := &CreateFieldMessage{
		Index:     i.name,
		Field:     name,
		CreatedAt: timestamp(),
		Meta:      fo,
	}

	// Create the field in etcd as the system of record.
	if err := i.persistField(context.Background(), cfm); err != nil {
		// There is a case where the index is not in memory, but it is in
		// persistent storage. In that case, this will return an "index exists"
		// error, which in that case should return the index. TODO: We may need
		// to allow for that in the future.
		return nil, errors.Wrap(err, "persisting field")
	}

	return i.createField(cfm, false)
}

// CreateFieldIfNotExistsWithOptions is a method which I created because I
// needed the functionality of CreateFieldIfNotExists, but instead of taking
// function options, taking a *FieldOptions struct. TODO: This should
// definintely be refactored so we don't have these virtually equivalent
// methods, but I'm puttin this here for now just to see if it works.
func (i *Index) CreateFieldIfNotExistsWithOptions(name string, opt *FieldOptions) (*Field, error) {
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

	cfm := &CreateFieldMessage{
		Index:     i.name,
		Field:     name,
		CreatedAt: timestamp(),
		Meta:      opt,
	}

	// Create the field in etcd as the system of record.
	if err := i.persistField(context.Background(), cfm); err != nil {
		// There is a case where the index is not in memory, but it is in
		// persistent storage. In that case, this will return an "index exists"
		// error, which in that case should return the index. TODO: We may need
		// to allow for that in the future.
		return nil, errors.Wrap(err, "persisting field")
	}

	return i.createField(cfm, false)
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
		return errors.Wrap(err, "marshaling")
	} else if err := i.Schemator.CreateField(ctx, cfm.Index, cfm.Field, b); err != nil {
		return errors.Wrapf(err, "writing field to disco: %s/%s", cfm.Index, cfm.Field)
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

	return i.createField(cfm, false)
}

// createField, in addition to creating a new Field, calls Field.Open which
// potentially aquires a lock on Index. So until/unless we refactor the
// Index.createField() function call path, we cannot call Index.createField
// while holding an Index lock.
func (i *Index) createField(cfm *CreateFieldMessage, broadcast bool) (*Field, error) {
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

	if broadcast {
		// Send the create field message to all nodes.
		if err := i.holder.sendOrSpool(cfm); err != nil {
			return nil, errors.Wrap(err, "sending CreateField message")
		}
	}

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
	f.schemator = i.Schemator
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

	// Close field.
	if err := f.Close(); err != nil {
		return errors.Wrap(err, "closing")
	}

	if err := i.holder.txf.DeleteFieldFromStore(i.name, name, i.fieldPath(name)); err != nil {
		return errors.Wrap(err, "Txf.DeleteFieldFromStore")
	}

	// Remove reference.
	delete(i.fields, name)

	// Delete the field from etcd as the system of record.
	if err := i.Schemator.DeleteField(context.TODO(), i.name, name); err != nil {
		return errors.Wrapf(err, "deleting field from etcd: %s/%s", i.name, name)
	}

	return i.translationSyncer.Reset()
}

type indexSlice []*Index

func (p indexSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p indexSlice) Len() int           { return len(p) }
func (p indexSlice) Less(i, j int) bool { return p[i].Name() < p[j].Name() }

// IndexInfo represents schema information for an index.
type IndexInfo struct {
	Name       string       `json:"name"`
	CreatedAt  int64        `json:"createdAt,omitempty"`
	Options    IndexOptions `json:"options"`
	Fields     []*FieldInfo `json:"fields"`
	ShardWidth uint64       `json:"shardWidth"`
}

type indexInfoSlice []*IndexInfo

func (p indexInfoSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p indexInfoSlice) Len() int           { return len(p) }
func (p indexInfoSlice) Less(i, j int) bool { return p[i].Name < p[j].Name }

// IndexOptions represents options to set when initializing an index.
type IndexOptions struct {
	Keys           bool `json:"keys"`
	TrackExistence bool `json:"trackExistence"`
}

type importData struct {
	RowIDs    []uint64
	ColumnIDs []uint64
}

// FormatQualifiedIndexName generates a qualified name for the index to be used with Tx operations.
func FormatQualifiedIndexName(index string) string {
	return fmt.Sprintf("%s\x00", index)
}

// Dump prints to stdout the contents of the roaring Containers
// stored in idx. Mostly for debugging.
func (i *Index) Dump(label string) {
	fileline := FileLine(2)
	fmt.Printf("\n%v Dump: %v\n\n", fileline, label)
	i.holder.txf.dbPerShard.DumpAll()
}

func (i *Index) Txf() *TxFactory {
	return i.holder.txf
}
