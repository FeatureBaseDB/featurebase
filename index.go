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
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pilosa/pilosa/v2/internal"
	"github.com/pilosa/pilosa/v2/logger"
	"github.com/pilosa/pilosa/v2/roaring"
	"github.com/pilosa/pilosa/v2/stats"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

// Index represents a container for fields.
type Index struct {
	mu   sync.RWMutex
	path string
	name string
	keys bool // use string keys

	// Existence tracking.
	trackExistence bool
	existenceFld   *Field

	// Partitions used by translation.
	partitionN int

	// Fields by name.
	fields map[string]*Field

	newAttrStore func(string) AttrStore

	// Column attribute storage and cache.
	columnAttrs AttrStore

	broadcaster broadcaster
	Stats       stats.StatsClient

	logger        logger.Logger
	snapshotQueue snapshotQueue

	// Passed to field for foreign-index lookup.
	holder *Holder

	// Per-partition translation stores
	translateStores map[int]TranslateStore

	// Instantiates new translation stores
	OpenTranslateStore OpenTranslateStoreFunc
}

// NewIndex returns a new instance of Index.
func NewIndex(path, name string, partitionN int) (*Index, error) {
	err := validateName(name)
	if err != nil {
		return nil, errors.Wrap(err, "validating name")
	}

	return &Index{
		path:       path,
		name:       name,
		partitionN: partitionN,
		fields:     make(map[string]*Field),

		newAttrStore: newNopAttrStore,
		columnAttrs:  nopStore,

		broadcaster:    NopBroadcaster,
		Stats:          stats.NopStatsClient,
		logger:         logger.NopLogger,
		trackExistence: true,

		translateStores: make(map[int]TranslateStore),

		OpenTranslateStore: OpenInMemTranslateStore,
	}, nil
}

// Name returns name of the index.
func (i *Index) Name() string { return i.name }

// Path returns the path the index was initialized with.
func (i *Index) Path() string { return i.path }

// TranslateStorePath returns the translation database path for a partition.
func (i *Index) TranslateStorePath(partitionID int) string {
	return filepath.Join(i.path, translateStoreDir, strconv.Itoa(partitionID))
}

// TranslateStore returns the translation store for a given partition.
func (i *Index) TranslateStore(partitionID int) TranslateStore {
	return i.translateStores[partitionID]
}

// Keys returns true if the index uses string keys.
func (i *Index) Keys() bool { return i.keys }

// ColumnAttrStore returns the storage for column attributes.
func (i *Index) ColumnAttrStore() AttrStore { return i.columnAttrs }

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
func (i *Index) Open() (err error) {
	// Ensure the path exists.
	i.logger.Debugf("ensure index path exists: %s", i.path)
	if err := os.MkdirAll(i.path, 0777); err != nil {
		return errors.Wrap(err, "creating directory")
	}

	// Read meta file.
	i.logger.Debugf("load meta file for index: %s", i.name)
	if err := i.loadMeta(); err != nil {
		return errors.Wrap(err, "loading meta file")
	}

	i.logger.Debugf("open fields for index: %s", i.name)
	if err := i.openFields(); err != nil {
		return errors.Wrap(err, "opening fields")
	}

	if i.trackExistence {
		if err := i.openExistenceField(); err != nil {
			return errors.Wrap(err, "opening existence field")
		}
	}

	if err := i.columnAttrs.Open(); err != nil {
		return errors.Wrap(err, "opening attrstore")
	}

	i.logger.Debugf("open translate store for index: %s", i.name)

	var g errgroup.Group
	var mu sync.Mutex
	for partitionID := 0; partitionID < i.partitionN; partitionID++ {
		partitionID := partitionID

		g.Go(func() error {
			store, err := i.OpenTranslateStore(i.TranslateStorePath(partitionID), i.name, "", partitionID, i.partitionN)
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

	return nil
}

var indexQueue = make(chan struct{}, 8)

// openFields opens and initializes the fields inside the index.
func (i *Index) openFields() error {
	f, err := os.Open(i.path)
	if err != nil {
		return errors.Wrap(err, "opening directory")
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
			indexQueue <- struct{}{}
			eg.Go(func() error {
				defer func() {
					<-indexQueue
				}()
				i.logger.Debugf("open field: %s", fi.Name())
				mu.Lock()
				fld, err := i.newField(i.fieldPath(filepath.Base(fi.Name())), filepath.Base(fi.Name()))
				mu.Unlock()
				if err != nil {
					return errors.Wrapf(ErrName, "'%s'", fi.Name())
				}

				// Pass holder through to the field for use in looking
				// up a foreign index.
				fld.holder = i.holder

				if err := fld.Open(); err != nil {
					return fmt.Errorf("open field: name=%s, err=%s", fld.Name(), err)
				}
				i.logger.Debugf("add field to index.fields: %s", fi.Name())
				mu.Lock()
				i.fields[fld.Name()] = fld
				mu.Unlock()
				return nil
			})
		}
	}
	return eg.Wait()
}

// openExistenceField gets or creates the existence field and associates it to the index.
func (i *Index) openExistenceField() error {
	f, err := i.createFieldIfNotExists(existenceFieldName, &FieldOptions{CacheType: CacheTypeNone, CacheSize: 0})
	if err != nil {
		return errors.Wrap(err, "creating existence field")
	}
	i.existenceFld = f
	return nil
}

// loadMeta reads meta data for the index, if any.
func (i *Index) loadMeta() error {
	var pb internal.IndexMeta

	// Read data from meta file.
	buf, err := ioutil.ReadFile(filepath.Join(i.path, ".meta"))
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return errors.Wrap(err, "reading")
	} else {
		if err := proto.Unmarshal(buf, &pb); err != nil {
			return errors.Wrap(err, "unmarshalling")
		}
	}

	// Copy metadata fields.
	i.keys = pb.Keys
	i.trackExistence = pb.TrackExistence

	return nil
}

// saveMeta writes meta data for the index.
func (i *Index) saveMeta() error {
	// Marshal metadata.
	buf, err := proto.Marshal(&internal.IndexMeta{
		Keys:           i.keys,
		TrackExistence: i.trackExistence,
	})
	if err != nil {
		return errors.Wrap(err, "marshalling")
	}

	// Write to meta file.
	if err := ioutil.WriteFile(filepath.Join(i.path, ".meta"), buf, 0666); err != nil {
		return errors.Wrap(err, "writing")
	}

	return nil
}

// Close closes the index and its fields.
func (i *Index) Close() error {
	i.mu.Lock()
	defer i.mu.Unlock()

	// Close the attribute store.
	i.columnAttrs.Close()

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

// AvailableShards returns a bitmap of all shards with data in the index.
func (i *Index) AvailableShards() *roaring.Bitmap {
	if i == nil {
		return roaring.NewBitmap()
	}

	i.mu.RLock()
	defer i.mu.RUnlock()

	b := roaring.NewBitmap()
	for _, f := range i.fields {
		b = b.Union(f.AvailableShards())
	}

	i.Stats.Gauge("maxShard", float64(b.Max()), 1.0)
	return b
}

// fieldPath returns the path to a field in the index.
func (i *Index) fieldPath(name string) string { return filepath.Join(i.path, name) }

// Field returns a field in the index by name.
func (i *Index) Field(name string) *Field {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.field(name)
}

func (i *Index) field(name string) *Field { return i.fields[name] }

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
	err := validateName(name)
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

	return i.createField(name, fo)
}

// CreateFieldIfNotExists creates a field with the given options if it doesn't exist.
func (i *Index) CreateFieldIfNotExists(name string, opts ...FieldOption) (*Field, error) {
	err := validateName(name)
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

	return i.createField(name, fo)
}

func (i *Index) createFieldIfNotExists(name string, opt *FieldOptions) (*Field, error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	// Find field in cache first.
	if f := i.fields[name]; f != nil {
		return f, nil
	}

	return i.createField(name, opt)
}

func (i *Index) createField(name string, opt *FieldOptions) (*Field, error) {
	if name == "" {
		return nil, errors.New("field name required")
	} else if opt.CacheType != "" && !isValidCacheType(opt.CacheType) {
		return nil, ErrInvalidCacheType
	}

	// Initialize field.
	f, err := i.newField(i.fieldPath(name), name)
	if err != nil {
		return nil, errors.Wrap(err, "initializing")
	}

	// Pass holder through to the field for use in looking
	// up a foreign index.
	f.holder = i.holder

	f.setOptions(opt)

	// Open field.
	if err := f.Open(); err != nil {
		return nil, errors.Wrap(err, "opening")
	}

	if err := f.saveMeta(); err != nil {
		f.Close()
		return nil, errors.Wrap(err, "saving meta")
	}

	// Add to index's field lookup.
	i.fields[name] = f

	return f, nil
}

func (i *Index) newField(path, name string) (*Field, error) {
	f, err := newField(path, i.name, name, OptFieldTypeDefault())
	if err != nil {
		return nil, err
	}
	f.logger = i.logger
	f.Stats = i.Stats
	f.broadcaster = i.broadcaster
	f.rowAttrStore = i.newAttrStore(filepath.Join(f.path, ".data"))
	if i.snapshotQueue != nil {
		f.snapshotQueue = i.snapshotQueue
	}
	f.OpenTranslateStore = i.OpenTranslateStore
	return f, nil
}

// DeleteField removes a field from the index.
func (i *Index) DeleteField(name string) error {
	i.mu.Lock()
	defer i.mu.Unlock()

	// Confirm field exists.
	f := i.field(name)
	if f == nil {
		return newNotFoundError(ErrFieldNotFound)
	}

	// Close field.
	if err := f.Close(); err != nil {
		return errors.Wrap(err, "closing")
	}

	// Delete field directory.
	if err := os.RemoveAll(i.fieldPath(name)); err != nil {
		return errors.Wrap(err, "removing directory")
	}

	// If the field being deleted is the existence field,
	// turn off existence tracking on the index.
	if name == existenceFieldName {
		i.trackExistence = false
		i.existenceFld = nil

		// Update meta data on disk.
		if err := i.saveMeta(); err != nil {
			return errors.Wrap(err, "saving existence meta data")
		}
	}

	// Remove reference.
	delete(i.fields, name)

	return nil
}

type indexSlice []*Index

func (p indexSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p indexSlice) Len() int           { return len(p) }
func (p indexSlice) Less(i, j int) bool { return p[i].Name() < p[j].Name() }

// IndexInfo represents schema information for an index.
type IndexInfo struct {
	Name       string       `json:"name"`
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

// hasTime returns true if a contains a non-nil time.
func hasTime(a []*time.Time) bool {
	for _, t := range a {
		if t != nil {
			return true
		}
	}
	return false
}

type importKey struct {
	View  string
	Shard uint64
}

type importData struct {
	RowIDs    []uint64
	ColumnIDs []uint64
}

type importValueData struct {
	ColumnIDs []uint64
	Values    []int64
}
