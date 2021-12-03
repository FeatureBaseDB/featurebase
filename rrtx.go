// Copyright 2020 Pilosa Corp.
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
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/molecula/featurebase/v2/roaring"
	txkey "github.com/molecula/featurebase/v2/short_txkey"
	"github.com/molecula/featurebase/v2/storage"

	"github.com/molecula/featurebase/v2/vprint"
	"github.com/pkg/errors"
)

// RoaringTx represents a fake transaction object for Roaring storage.
type RoaringTx struct {
	write    bool
	Index    *Index
	Field    *Field
	fragment *fragment
	o        Txo
	sn       int64 // serial number

	done bool
	mu   sync.Mutex // protect done as it changes state

	w *RoaringWrapper
}

func (tx *RoaringTx) Type() string {
	return RoaringTxn
}

// based on view.openFragments()
func roaringMapOfShards(optionalViewPath string) (shardMap map[uint64]bool, err error) {

	shardMap = make(map[uint64]bool)

	path := filepath.Join(optionalViewPath, "fragments")
	file, err := os.Open(path)
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
		//vv("rrtx next fi = '%v'", fi.Name())
		if fi.IsDir() {
			continue
		}
		name := fi.Name()
		if strings.HasSuffix(name, ".cache") {
			continue
		}

		// Parse filename into integer.
		shard, err := strconv.ParseUint(filepath.Base(name), 10, 64)
		if err != nil {
			//vv("WARNING: couldn't use non-integer file as shard in index/field/view %s/%s/%s: %s", index, field, view, fi.Name())
			//panic(fmt.Sprintf("WARNING: couldn't use non-integer file as shard in index/field/view %s/%s/%s: %s", index, field, view, fi.Name()))
			//tx.Index.holder.Logger.Debugf("WARNING: couldn't use non-integer file as shard in index/field/view %s/%s/%s: %s", index, field, view, fi.Name())
			continue
		}
		shardMap[shard] = true
	}
	return
}

// NewTxIterator returns a *roaring.Iterator that MUST have Close() called on it BEFORE
// the transaction Commits or Rollsback.
func (tx *RoaringTx) NewTxIterator(index, field, view string, shard uint64) *roaring.Iterator {
	b, err := tx.bitmap(index, field, view, shard)
	vprint.PanicOn(err)
	return b.Iterator()
}

// ImportRoaringBits return values changed and rowSet will be inaccurate if
// the data []byte is supplied. This mimics the traditional roaring-per-file
// and should be faster.
func (tx *RoaringTx) ImportRoaringBits(index, field, view string, shard uint64, rit roaring.RoaringIterator, clear bool, log bool, rowSize uint64) (changed int, rowSet map[uint64]int, err error) {
	f, err := tx.getFragment(index, field, view, shard)
	if err != nil {
		return 0, nil, err
	}

	changed, rowSet, err = f.storage.ImportRoaringRawIterator(rit, clear, true, rowSize)
	return
}

func (c *RoaringTx) ApplyFilter(index, field, view string, shard uint64, ckey uint64, filter roaring.BitmapFilter) (err error) {
	return GenericApplyFilter(c, index, field, view, shard, ckey, filter)
}

// Rollback
func (tx *RoaringTx) Rollback() {
	tx.w.CleanupTx(tx)
}

// Commit
func (tx *RoaringTx) Commit() error {
	tx.w.CleanupTx(tx)
	return nil
}

func (tx *RoaringTx) RoaringBitmap(index, field, view string, shard uint64) (*roaring.Bitmap, error) {
	return tx.bitmap(index, field, view, shard)
}

func (tx *RoaringTx) Container(index, field, view string, shard uint64, key uint64) (*roaring.Container, error) {
	b, err := tx.bitmap(index, field, view, shard)
	if err != nil {
		return nil, err
	}
	return b.Containers.Get(key), nil
}

func (tx *RoaringTx) PutContainer(index, field, view string, shard uint64, key uint64, c *roaring.Container) error {
	b, err := tx.bitmap(index, field, view, shard)
	if err != nil {
		return err
	}
	b.Containers.Put(key, c)
	return nil
}

func (tx *RoaringTx) RemoveContainer(index, field, view string, shard uint64, key uint64) error {
	b, err := tx.bitmap(index, field, view, shard)
	if err != nil {
		return err
	}
	b.Containers.Remove(key)
	return nil
}

func (tx *RoaringTx) Add(index, field, view string, shard uint64, a ...uint64) (changeCount int, err error) {
	//vv("RoaringTx.Add(index='%v', shard='%v') stack=\n%v", index, shard, stack())
	b, err := tx.bitmap(index, field, view, shard)
	if err != nil {
		return 0, err
	}
	// Note: do not replace b.AddN() with b.DirectAddN().
	// DirectAddN() does not do op-log operations inside roaring, so the
	// on-disk representation no longer matches the in-memory operations.
	count, err := b.AddN(a...)
	return count, err
}

func (tx *RoaringTx) Remove(index, field, view string, shard uint64, a ...uint64) (changeCount int, err error) {
	b, err := tx.bitmap(index, field, view, shard)
	if err != nil {
		return 0, err
	}
	return b.RemoveN(a...)
}

func (tx *RoaringTx) Contains(index, field, view string, shard uint64, v uint64) (exists bool, err error) {
	b, err := tx.bitmap(index, field, view, shard)
	if err != nil {
		return false, err
	}
	return b.Contains(v), nil
}

func (tx *RoaringTx) ContainerIterator(index, field, view string, shard uint64, key uint64) (citer roaring.ContainerIterator, found bool, err error) {
	b, err := tx.bitmap(index, field, view, shard)
	if err != nil {
		return nil, false, errors.Wrap(err, "getting bitmap")
	}
	//vv("b bitmap back from bitmap(index='%v', field='%v', view='%v', shard='%v')='%#v'", index, field, view, shard, b.Slice())
	citer, found = b.Containers.Iterator(key)
	return citer, found, nil
}

func (tx *RoaringTx) ForEach(index, field, view string, shard uint64, fn func(i uint64) error) error {
	b, err := tx.bitmap(index, field, view, shard)
	if err != nil {
		return err
	}
	return b.ForEach(fn)
}

func (tx *RoaringTx) ForEachRange(index, field, view string, shard uint64, start, end uint64, fn func(uint64) error) error {
	b, err := tx.bitmap(index, field, view, shard)
	if err != nil {
		return err
	}
	return b.ForEachRange(start, end, fn)
}

func (tx *RoaringTx) Count(index, field, view string, shard uint64) (uint64, error) {
	b, err := tx.bitmap(index, field, view, shard)
	if err != nil {
		return 0, err
	}
	return b.Count(), nil
}

func (tx *RoaringTx) Max(index, field, view string, shard uint64) (uint64, error) {
	b, err := tx.bitmap(index, field, view, shard)
	if err != nil {
		return 0, err
	}
	return b.Max(), nil
}

func (tx *RoaringTx) Min(index, field, view string, shard uint64) (uint64, bool, error) {
	b, err := tx.bitmap(index, field, view, shard)
	if err != nil {
		return 0, false, err
	}
	v, ok := b.Min()
	return v, ok, nil
}

func (tx *RoaringTx) CountRange(index, field, view string, shard uint64, start, end uint64) (uint64, error) {
	b, err := tx.bitmap(index, field, view, shard)
	if err != nil {
		return 0, err
	}
	return b.CountRange(start, end), nil
}

func (tx *RoaringTx) OffsetRange(index, field, view string, shard uint64, offset, start, end uint64) (*roaring.Bitmap, error) {
	b, err := tx.bitmap(index, field, view, shard)
	if err != nil {
		return nil, err
	}
	return b.OffsetRange(offset, start, end), nil
}

// getFragment is used by IncrementOpN() and by bitmap()
func (tx *RoaringTx) getFragment(index, field, view string, shard uint64) (*fragment, error) {

	// If a fragment is attached, always use it. Since it was set at Tx creation,
	// it is highly likely to be correct.
	if tx.fragment != nil {
		// but still a basic sanity check.
		if tx.fragment.index() != index ||
			tx.fragment.field() != field ||
			tx.fragment.view() != view ||
			tx.fragment.shard != shard {

			// still insist that index and shard match, since that is the current scope of all Tx.
			if tx.fragment.index() != index ||
				tx.fragment.shard != shard {
				panic(fmt.Sprintf("different fragment cached vs requested. index='%v', field='%v'; view='%v'; shard='%v'; tx.fragment='%#v'", index, field, view, shard, tx.fragment))
			}
			// cannot use this fragment.
			tx.fragment = nil

		} else {
			return tx.fragment, nil
		}
	}

	// If a field is attached, start from there.
	// Otherwise look up the field from the index.
	f := tx.Field

	if f == nil {
		// we cannot assume that the tx.Index that we "started" on is the same
		// as the index we are being queried; it might be foreign: TestExecutor_ForeignIndex
		// So go through the holder
		idx := tx.Index.holder.Index(index)
		if idx == nil {
			// only thing we can try is the cached index, and hope we aren't being asked for a foreign index.
			f = tx.Index.Field(field)
			if f == nil {
				return nil, newNotFoundError(ErrFieldNotFound, field)
			}
		} else {
			if f = idx.Field(field); f == nil {
				return nil, newNotFoundError(ErrFieldNotFound, field)
			}
		}
	}
	// INVAR: f is not nil.

	v := f.view(view)
	if v == nil {
		return nil, errors.Wrapf(ViewNotFound, "getting %s", view)
	}

	frag := v.Fragment(shard)

	if frag == nil {
		return nil, errors.Wrapf(FragmentNotFound, "field:%q, view:%q, shard:%d", field, view, shard)
	}

	// Note: we cannot cache frag into tx.fragment.
	// Empirically, it breaks 245 top-level pilosa tests.
	// tx.fragment = frag // breaks the world.

	return frag, nil
}

const ViewNotFound = Error("view not found")
const FragmentNotFound = Error("fragment not found")

func (tx *RoaringTx) bitmap(index, field, view string, shard uint64) (*roaring.Bitmap, error) {
	frag, err := tx.getFragment(index, field, view, shard)
	if err != nil {
		return nil, errors.Wrap(err, "getFragment")
	}
	return frag.storage, nil
}

func roaringGetFieldView2Shards(idx *Index) (vs *FieldView2Shards, err error) {
	vs = NewFieldView2Shards()

	// A) open the index directory
	f, err := os.Open(idx.FieldsPath())
	if err != nil {
		return nil, errors.Wrap(err, "opening directory")
	}
	defer f.Close()

	fieldFIs, err := f.Readdir(0)
	if err != nil {
		return nil, errors.Wrap(err, "reading directory")
	}

	//vv("roaringGetFieldView2Shards A) opened index path '%v'", idx.path)

	// B) read the name of each field under the index
	for _, loopFieldFi := range fieldFIs {
		fieldFI := loopFieldFi
		if !fieldFI.IsDir() {
			continue
		}
		field := fieldFI.Name()

		//vv("roaringGetFieldView2Shards B) on field '%v'", field)

		fieldPath := filepath.Join(idx.FieldsPath(), field)

		viewsDir := filepath.Join(fieldPath, "views")
		file, err := os.Open(viewsDir)
		if os.IsNotExist(err) {
			//return nil
			continue
		} else if err != nil {
			return nil, errors.Wrapf(err, "opening view directory '%v'", viewsDir)
		}
		defer file.Close()

		// C) read the name of each view under the field

		viewFIs, err := file.Readdir(0)
		if err != nil {
			return nil, errors.Wrapf(err, "reading views directory '%v'", viewsDir)
		}
		for _, viewFI := range viewFIs {

			if !viewFI.IsDir() {
				continue
			}
			view := viewFI.Name()
			roaringViewPath := filepath.Join(viewsDir, view)

			shardMap, err := roaringMapOfShards(roaringViewPath)
			if err != nil {
				return nil, errors.Wrapf(err, "reading view path directory '%v'", roaringViewPath)
			}
			if len(shardMap) == 0 {
				//vv("roaringGetFieldView2Shards C) SAVED SPACE! field '%v' view '%v' had no shards", field, view)
				continue
			}

			ss := newShardSetFromMap(shardMap)
			fv := txkey.FieldView{Field: field, View: view}
			vs.addViewShardSet(fv, ss)

			//vv("roaringGetFieldView2Shards C) added field '%v' view '%v' with shards '%#v'", field, view, ss.shards)
		}
	}
	return
}

// inefficient for roaring. Instead use the roaringGetFieldView2Shards() above.
func (tx *RoaringTx) GetSortedFieldViewList(idx *Index, shard uint64) (fvs []txkey.FieldView, err error) {

	// A) open the index directory
	f, err := os.Open(idx.FieldsPath())
	if err != nil {
		return nil, errors.Wrap(err, "opening directory")
	}
	defer f.Close()

	fieldFIs, err := f.Readdir(0)
	if err != nil {
		return nil, errors.Wrap(err, "reading directory")
	}

	//vv("A) shard %v, opened index path '%v'", shard, idx.path)

	// B) read the name of each field under the index
	for _, loopFieldFi := range fieldFIs {
		fieldFI := loopFieldFi
		if !fieldFI.IsDir() {
			continue
		}
		field := fieldFI.Name()

		//vv("B) on field '%v'", field)

		fieldPath := filepath.Join(idx.FieldsPath(), field)

		viewsDir := filepath.Join(fieldPath, "views")
		file, err := os.Open(viewsDir)
		if os.IsNotExist(err) {
			//return nil
			continue
		} else if err != nil {
			return nil, errors.Wrapf(err, "opening view directory '%v'", viewsDir)
		}
		defer file.Close()

		// C) read the name of each view under the field

		viewFIs, err := file.Readdir(0)
		if err != nil {
			return nil, errors.Wrapf(err, "reading views directory '%v'", viewsDir)
		}
		for _, viewFI := range viewFIs {

			if !viewFI.IsDir() {
				continue
			}
			view := viewFI.Name()
			roaringViewPath := filepath.Join(viewsDir, view)

			shardMap, err := roaringMapOfShards(roaringViewPath)
			if err != nil {
				return nil, errors.Wrapf(err, "reading view path directory '%v'", roaringViewPath)
			}
			if len(shardMap) == 0 {
				continue
			}

			// once we know we have data for this shard!
			if shardMap[shard] {
				fv := txkey.FieldView{Field: field, View: view}
				//vv("C) adding fv '%#v'", fv)
				fvs = append(fvs, fv)
			}
		}
	}
	// directory stuff isn't returned in sorted order, we must sort.
	sort.Slice(fvs, func(i, j int) bool {
		if fvs[i].Field < fvs[j].Field {
			return true
		}
		if fvs[i].Field > fvs[j].Field {
			return false
		}
		return fvs[i].View < fvs[j].View
	})
	return
}

func (tx *RoaringTx) GetFieldSizeBytes(index, field string) (uint64, error) {
	return 0, nil
}

//////// registrar and wrapper machinery

// roaringRegistrar mirrors the machinery expected
// for all backends for the roaring files approach.
//
type roaringRegistrar struct {
	mu sync.Mutex
	mp map[*RoaringWrapper]bool

	path2db map[string]*RoaringWrapper
}

func (r *roaringRegistrar) Size() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	nmp := len(r.mp)
	npa := len(r.path2db)
	if nmp != npa {
		panic(fmt.Sprintf("nmp=%v, vs npa=%v", nmp, npa))
	}
	return nmp
}

var globalRoaringReg *roaringRegistrar = newRoaringRegistrar()

func newRoaringRegistrar() *roaringRegistrar {
	return &roaringRegistrar{
		mp:      make(map[*RoaringWrapper]bool),
		path2db: make(map[string]*RoaringWrapper),
	}
}

func (r *roaringRegistrar) unprotectedRegister(w *RoaringWrapper) {
	r.mp[w] = true
	r.path2db[w.path] = w
}

// unregister removes w from r
func (r *roaringRegistrar) unregister(w *RoaringWrapper) {
	r.mu.Lock()
	delete(r.mp, w)
	delete(r.path2db, w.path)
	r.mu.Unlock()
}

// openRoaringDB will check the registry and make a new instance only
// if one does not exist for its path0. Otherwise it returns
// the existing instance.
func (r *roaringRegistrar) OpenDBWrapper(path string, doAllocZero bool, _ *storage.Config) (DBWrapper, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	w, ok := r.path2db[path]
	if ok {
		return w, nil
	}
	// otherwise, make a new roaring and store it in globalRoaringReg
	w = &RoaringWrapper{
		reg:  r,
		path: path,
	}
	r.unprotectedRegister(w)

	return w, nil
}

func (w *RoaringWrapper) SetHolder(h *Holder) {
	w.h = h
}

func (w *RoaringWrapper) Path() string {
	return w.path
}

func (w *RoaringWrapper) HasData() (has bool, err error) {
	return w.h.HasRoaringData()
}

func (w *RoaringWrapper) CleanupTx(tx Tx) {
	r := tx.(*RoaringTx)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.done {
		return
	}
	r.done = true
}

func (w *RoaringWrapper) OpenListString() (r string) {
	return "RoaringWrapper.OpenListString() not yet implemented"
}

func (w *RoaringWrapper) CloseDB() error {
	return errors.New("CloseDB not supported in roaring")
}
func (w *RoaringWrapper) OpenDB() error {
	return errors.New("OpenDB not supported in roaring")
}

// statically confirm that RoaringTx satisfies the Tx interface.
var _ Tx = (*RoaringTx)(nil)

// RoaringWrapper provides the NewTx() method.
type RoaringWrapper struct {
	muDb sync.Mutex

	path string

	h *Holder

	reg *roaringRegistrar

	// make RoaringWrapper.Close() idempotent, avoiding panic on double Close()
	closed bool
}

var globalNextTxSnRoaring int64

func (w *RoaringWrapper) NewTx(write bool, initialIndexName string, o Txo) (tx Tx, err error) {

	sn := atomic.AddInt64(&globalNextTxSnRoaring, 1)
	return &RoaringTx{
		write:    o.Write,
		Field:    o.Field,
		Index:    o.Index,
		fragment: o.Fragment,
		o:        o,
		sn:       sn,
		w:        w,
	}, nil
}

// Close shuts down the Roaring database.
func (w *RoaringWrapper) Close() (err error) {
	w.muDb.Lock()
	defer w.muDb.Unlock()
	if !w.closed {
		w.reg.unregister(w)
		w.closed = true
	}
	return nil
}

func (w *RoaringWrapper) IsClosed() (closed bool) {
	w.muDb.Lock()
	closed = w.closed
	w.muDb.Unlock()
	return
}

func (w *RoaringWrapper) DeleteField(index, field, fieldPath string) error {
	//vv("RoaringWrapper.DeleteField(index = '%v', field = '%v', fieldPath = '%v'", index, field, fieldPath)

	// match txn sn count vs lmdb/etc.
	atomic.AddInt64(&globalNextTxSnRoaring, 1)

	err := os.RemoveAll(fieldPath)
	if err != nil {
		return errors.Wrap(err, "removing directory")
	}
	return nil
}

func (w *RoaringWrapper) DeleteFragment(index, field, view string, shard uint64, frag interface{}) error {

	// match txn sn count vs lmdb/etc.
	atomic.AddInt64(&globalNextTxSnRoaring, 1)

	fragment, ok := frag.(*fragment)
	if !ok {
		return fmt.Errorf("RoaringStore.DeleteFragment must get frag of type *fragment, but got '%T'", frag)
	}

	// Delete fragment file.
	if err := os.Remove(fragment.path()); err != nil {
		return errors.Wrap(err, "deleting fragment file")
	}

	// Delete fragment cache file.
	if err := os.Remove(fragment.cachePath()); err != nil {
		return errors.Wrap(err, fmt.Sprintf("no cache file to delete for shard %d", fragment.shard))
	}
	return nil
}
