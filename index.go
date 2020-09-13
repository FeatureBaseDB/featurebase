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
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/pilosa/pilosa/v2/hash"
	"github.com/pilosa/pilosa/v2/internal"
	"github.com/pilosa/pilosa/v2/roaring"
	"github.com/pilosa/pilosa/v2/stats"
	"github.com/pilosa/pilosa/v2/testhook"
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

	newAttrStore func(string) AttrStore

	// Column attribute storage and cache.
	columnAttrs AttrStore

	broadcaster broadcaster
	Stats       stats.StatsClient

	// Passed to field for foreign-index lookup.
	holder *Holder

	// Per-partition translation stores
	translateStores map[int]TranslateStore

	translationSyncer TranslationSyncer

	// Instantiates new translation stores
	OpenTranslateStore OpenTranslateStoreFunc
}

// NewIndex returns an existing (but possibly empty) instance of
// Index at path. It will not erase any prior content.
func NewIndex(holder *Holder, path, name string) (*Index, error) {

	// Emulate what the spf13/cobra does, letting env vars override
	// the defaults, because we may be under a simple "go test" run where
	// not all that command line machinery has been spun up.

	err := validateName(name)
	if err != nil {
		return nil, errors.Wrap(err, "validating name")
	}

	idx := &Index{
		path:   path,
		name:   name,
		fields: make(map[string]*Field),

		newAttrStore: newNopAttrStore,
		columnAttrs:  nopStore,

		broadcaster:    NopBroadcaster,
		Stats:          stats.NopStatsClient,
		holder:         holder,
		trackExistence: true,

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
func (i *Index) Open() error {
	return i.open(false)
}

// OpenWithTimestamp opens and initializes the index and set a new CreatedAt timestamp for fields.
func (i *Index) OpenWithTimestamp() error { return i.open(true) }

func (i *Index) open(withTimestamp bool) (err error) {
	// Ensure the path exists.
	i.holder.Logger.Debugf("ensure index path exists: %s", i.path)
	if err := os.MkdirAll(i.path, 0777); err != nil {
		return errors.Wrap(err, "creating directory")
	}

	// Read meta file.
	i.holder.Logger.Debugf("load meta file for index: %s", i.name)
	if err := i.loadMeta(); err != nil {
		return errors.Wrap(err, "loading meta file")
	}

	i.holder.Logger.Debugf("open fields for index: %s", i.name)
	if err := i.openFields(withTimestamp); err != nil {
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

	_ = testhook.Opened(i.holder.Auditor, i, nil)
	return nil
}

var indexQueue = make(chan struct{}, 8)

// openFields opens and initializes the fields inside the index.
func (i *Index) openFields(withTimestamp bool) error {
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
			// Skip embedded db files too.
			if i.holder.txf.IsTxDatabasePath(fi.Name()) {
				continue
			}

			indexQueue <- struct{}{}
			eg.Go(func() error {
				defer func() {
					<-indexQueue
				}()
				i.holder.Logger.Debugf("open field: %s", fi.Name())

				mu.Lock()

				// goroutine safe
				i.holder.addIndex(i)

				fld, err := i.newField(i.fieldPath(filepath.Base(fi.Name())), filepath.Base(fi.Name()))
				if withTimestamp {
					fld.createdAt = timestamp()
				}
				mu.Unlock()
				if err != nil {
					return errors.Wrapf(ErrName, "'%s'", fi.Name())
				}

				// Pass holder through to the field for use in looking
				// up a foreign index.
				fld.holder = i.holder

				// open all the views
				if err := fld.Open(); err != nil {
					return fmt.Errorf("open field: name=%s, err=%s", fld.Name(), err)
				}
				i.holder.Logger.Debugf("add field to index.fields: %s", fi.Name())
				i.mu.Lock()
				i.fields[fld.Name()] = fld
				i.mu.Unlock()
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
	// TrackExistence is by default true
	pb := &internal.IndexMeta{TrackExistence: true}

	// Read data from meta file.
	buf, err := ioutil.ReadFile(filepath.Join(i.path, ".meta"))
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return errors.Wrap(err, "reading")
	} else {
		if err := proto.Unmarshal(buf, pb); err != nil {
			return errors.Wrap(err, "unmarshalling")
		}
	}

	// Copy metadata fields.
	if pb == nil {
		i.trackExistence = true
	} else {
		i.trackExistence = pb.TrackExistence
	}
	i.keys = pb.GetKeys()

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
	defer func() {
		_ = testhook.Closed(i.holder.Auditor, i, nil)
	}()

	err := i.holder.txf.CloseIndex(i)
	if err != nil {
		return errors.Wrap(err, "closing index")
	}

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
	f.rowAttrStore = i.newAttrStore(filepath.Join(f.path, ".data"))
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
		return newNotFoundError(ErrFieldNotFound, name)
	}

	// Close field.
	if err := f.Close(); err != nil {
		return errors.Wrap(err, "closing")
	}

	if err := i.holder.txf.DeleteFieldFromStore(i.name, name, i.fieldPath(name)); err != nil {
		return errors.Wrap(err, "Txf.DeleteFieldFromStore")
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

// FormatQualifiedIndexName generates a qualified name for the index to be used with Tx operations.
func FormatQualifiedIndexName(index string) string {
	return fmt.Sprintf("%s\x00", index)
}

// Dump prints to stdout the contents of the roaring Containers
// stored in idx. Mostly for debugging.
func (idx *Index) Dump(label string) {
	fileline := FileLine(2)
	fmt.Printf("\n%v Dump: %v\n\n", fileline, label)
	idx.holder.txf.dbPerShard.DumpAll()
}

func (idx *Index) SliceOfShards(field, view, viewPath string) (sliceOfShards []uint64, err error) {

	// SliceOfShards is based on view.openFragments()
	// If we go to a database per shard then index will need this, or
	// something like it, to read database files/directories
	// and figure out what all the shards are so that a view
	// can open its fragments.

	file, err := os.Open(filepath.Join(viewPath, "fragments"))
	if os.IsNotExist(err) {
		return
	} else if err != nil {
		return nil, errors.Wrap(err, "opening fragments directory")
	}
	defer file.Close()

	fis, err := file.Readdir(0)
	if err != nil {
		return nil, errors.Wrap(err, "reading fragments directory")
	}

	for _, fi := range fis {
		if fi.IsDir() {
			continue
		}
		// Parse filename into integer.
		shard, err := strconv.ParseUint(filepath.Base(fi.Name()), 10, 64)
		if err != nil {
			idx.holder.Logger.Debugf("WARNING: couldn't use non-integer file as shard in index/field/view %s/%s/%s: %s", idx.name, field, view, fi.Name())
			continue
		}
		sliceOfShards = append(sliceOfShards, shard)
	}
	return
}

type AllTranslatorSummary struct {
	Sums []*TranslatorSummary
}

func NewAllTranslatorSummary() *AllTranslatorSummary {
	return &AllTranslatorSummary{}
}
func (ats *AllTranslatorSummary) Append(b *AllTranslatorSummary) {
	ats.Sums = append(ats.Sums, b.Sums...)
}

func (ats *AllTranslatorSummary) Sort() {
	// return sorted by index then PartitionID then Field
	sort.Slice(ats.Sums, func(i, j int) bool {
		a := ats.Sums[i]
		b := ats.Sums[j]
		if a.Index < b.Index {
			return true
		}
		if a.Index > b.Index {
			return false
		}
		// INVAR: a.Index == b.Index
		if a.PartitionID < b.PartitionID {
			return true
		}
		if a.PartitionID > b.PartitionID {
			return false
		}
		return a.Field < b.Field
	})
}

// sums is only guaranteed to be sorted by (index, PartitionID, field) iff err returns nil
func (i *Index) ComputeTranslatorSummary(verbose bool) (ats *AllTranslatorSummary, err error) {
	i.mu.RLock()
	defer i.mu.RUnlock()

	ats = &AllTranslatorSummary{}

	fmt.Printf("\nindex: %v\n=================\n", i.name)
	for _, fld := range i.fields {
		sum, err := fld.translateStore.ComputeTranslatorSummary()
		if err != nil {
			return ats, err
		}
		sum.Field = fld.name
		sum.Index = i.Name()
		sum.Checksum = hash.Blake3sum16([]byte(fmt.Sprintf("%v/%v/%v", sum.Checksum, fld.name, i.Name())))

		if verbose {
			fmt.Printf("row blake3-%v keyN: %5v idN: %5v field: '%v'\n", sum.Checksum, sum.KeyCount, sum.IDCount, fld.name)
		}
		ats.Sums = append(ats.Sums, sum)
	}

	fmt.Printf("====================\n")

	for partitionID, store := range i.translateStores {
		sum, err := store.ComputeTranslatorSummary()
		if err != nil {
			return ats, err
		}
		if sum == nil {
			// probably one of the Noop stores
			continue
		}
		sum.PartitionID = partitionID
		sum.Index = i.Name()

		sum.Checksum = hash.Blake3sum16([]byte(fmt.Sprintf("%v/%v/%v", sum.Checksum, partitionID, i.Name())))
		if verbose {
			fmt.Printf("col blake3-%v keyN: %10v idN: %10v paritionID: %03v \n", sum.Checksum, sum.KeyCount, sum.IDCount, partitionID)
		}
		ats.Sums = append(ats.Sums, sum)
	}
	return
}

func (idx *Index) WriteFragmentChecksums(w io.Writer, showBits, showOps bool) {
	paths, err := listFilesUnderDir(idx.path, false, "", true)
	panicOn(err)
	index := idx.name
	n := 0
	for _, relpath := range paths {
		field, view, shard, err := fragmentSpecFromRoaringPath(relpath)
		if err != nil {
			continue // ignore .meta paths
		}
		abspath := idx.path + sep + relpath

		checksum, hotbits := RoaringFragmentChecksum(abspath, index, field, view, shard)
		fmt.Fprintf(w, "frg blake3-%v field: '%v' view: '%v' shard: %3v hotbits: %10v\n", checksum, field, view, shard, hotbits)
		n++
	}
	if n == 0 {
		fmt.Fprintf(w, "empty index '%v'", idx.path)
	}
}

func (idx *Index) Txf() *TxFactory {
	return idx.holder.txf
}
