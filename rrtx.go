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
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/pilosa/pilosa/v2/roaring"
	"github.com/pkg/errors"
)

// MultiTx implements the transaction interface to combine multiple transactions.
type MultiTx struct {
	mu       sync.Mutex
	writable bool
	holder   *Holder
	index    *Index
	txs      map[multiTxKey]Tx
}

// NewMultiTx returns a new instance of MultiTx for a Holder.
func NewMultiTx(writable bool, holder *Holder) *MultiTx {
	return &MultiTx{
		writable: writable,
		holder:   holder,
		txs:      make(map[multiTxKey]Tx),
	}
}

// NewMultiTxWithIndex returns a new instance of MultiTx for a single index.
func NewMultiTxWithIndex(writable bool, index *Index) *MultiTx {
	return &MultiTx{
		writable: writable,
		index:    index,
		txs:      make(map[multiTxKey]Tx),
	}
}

var _ Tx = (*MultiTx)(nil)

func (mtx *MultiTx) Type() string {
	return RoaringTxn
}

// debugging, what does this Tx see as its database?
func (mtx *MultiTx) Dump() {
	mtx.mu.Lock()
	defer mtx.mu.Unlock()
	if len(mtx.txs) == 0 {
		return
	}
	for _, tx := range mtx.txs {
		tx.Dump()
		return
	}
}

func (mtx *MultiTx) SliceOfShards(index, field, view, optionalViewPath string) (sliceOfShards []uint64, err error) {
	tx, err := mtx.txNoShard(index)
	panicOn(err)
	return tx.SliceOfShards(index, field, view, optionalViewPath)
}

func (mtx *MultiTx) UseRowCache() bool {
	return true
}

func (mtx *MultiTx) RoaringBitmapReader(index, field, view string, shard uint64, fragmentPathForRoaring string) (r io.ReadCloser, sz int64, err error) {
	tx, err := mtx.tx(index, shard)
	panicOn(err)
	return tx.RoaringBitmapReader(index, field, view, shard, fragmentPathForRoaring)
}

func (mtx *MultiTx) NewTxIterator(index, field, view string, shard uint64) *roaring.Iterator {
	tx, err := mtx.tx(index, shard)
	panicOn(err)
	return tx.NewTxIterator(index, field, view, shard)
}

// Readonly is true if the transaction is not read-and-write, but only doing reads.
func (mtx *MultiTx) Readonly() bool {
	return !mtx.writable
}

func (mtx *MultiTx) Pointer() string {
	return fmt.Sprintf("%p", mtx)
}

func (mtx *MultiTx) ImportRoaringBits(index, field, view string, shard uint64, rit roaring.RoaringIterator, clear bool, log bool, rowSize uint64, data []byte) (changed int, rowSet map[uint64]int, err error) {
	tx, err := mtx.tx(index, shard)
	panicOn(err)
	return tx.ImportRoaringBits(index, field, view, shard, rit, clear, log, rowSize, data)
}

func (mtx *MultiTx) IncrementOpN(index, field, view string, shard uint64, changedN int) {
	tx, err := mtx.tx(index, shard)
	panicOn(err)
	tx.IncrementOpN(index, field, view, shard, changedN)
}

// Rollback rolls back all underlying transactions.
func (mtx *MultiTx) Rollback() {
	for _, tx := range mtx.txs {
		tx.Rollback()
	}
}

// Commit commits all underlying transactions.
func (mtx *MultiTx) Commit() (err error) {
	for _, tx := range mtx.txs {
		if e := tx.Commit(); e != nil && err == nil {
			err = e
		}
	}
	return err
}

func (mtx *MultiTx) RoaringBitmap(index, field, view string, shard uint64) (*roaring.Bitmap, error) {
	tx, err := mtx.tx(index, shard)
	if err != nil {
		return nil, err
	}
	return tx.RoaringBitmap(index, field, view, shard)
}

func (mtx *MultiTx) Container(index, field, view string, shard uint64, key uint64) (*roaring.Container, error) {
	tx, err := mtx.tx(index, shard)
	if err != nil {
		return nil, err
	}
	return tx.Container(index, field, view, shard, key)
}

func (mtx *MultiTx) PutContainer(index, field, view string, shard uint64, key uint64, c *roaring.Container) error {
	tx, err := mtx.tx(index, shard)
	if err != nil {
		return err
	}
	return tx.PutContainer(index, field, view, shard, key, c)
}

func (mtx *MultiTx) RemoveContainer(index, field, view string, shard uint64, key uint64) error {
	tx, err := mtx.tx(index, shard)
	if err != nil {
		return err
	}
	return tx.RemoveContainer(index, field, view, shard, key)
}

func (mtx *MultiTx) Add(index, field, view string, shard uint64, batched bool, a ...uint64) (changeCount int, err error) {
	tx, err := mtx.tx(index, shard)
	if err != nil {
		return 0, err
	}
	return tx.Add(index, field, view, shard, batched, a...)
}

func (mtx *MultiTx) Remove(index, field, view string, shard uint64, a ...uint64) (changeCount int, err error) {
	tx, err := mtx.tx(index, shard)
	if err != nil {
		return 0, err
	}
	return tx.Remove(index, field, view, shard, a...)
}

func (mtx *MultiTx) Contains(index, field, view string, shard uint64, v uint64) (exists bool, err error) {
	tx, err := mtx.tx(index, shard)
	if err != nil {
		return false, err
	}
	return tx.Contains(index, field, view, shard, v)
}

func (mtx *MultiTx) ContainerIterator(index, field, view string, shard uint64, key uint64) (citer roaring.ContainerIterator, found bool, err error) {
	tx, err := mtx.tx(index, shard)
	if err != nil {
		return nil, false, err
	}
	return tx.ContainerIterator(index, field, view, shard, key)
}

func (mtx *MultiTx) ForEach(index, field, view string, shard uint64, fn func(i uint64) error) error {
	tx, err := mtx.tx(index, shard)
	if err != nil {
		return err
	}
	return tx.ForEach(index, field, view, shard, fn)
}

func (mtx *MultiTx) ForEachRange(index, field, view string, shard uint64, start, end uint64, fn func(uint64) error) error {
	tx, err := mtx.tx(index, shard)
	if err != nil {
		return err
	}
	return tx.ForEachRange(index, field, view, shard, start, end, fn)
}

func (mtx *MultiTx) Count(index, field, view string, shard uint64) (uint64, error) {
	tx, err := mtx.tx(index, shard)
	if err != nil {
		return 0, err
	}
	return tx.Count(index, field, view, shard)
}

func (mtx *MultiTx) Max(index, field, view string, shard uint64) (uint64, error) {
	tx, err := mtx.tx(index, shard)
	if err != nil {
		return 0, err
	}
	return tx.Max(index, field, view, shard)
}

func (mtx *MultiTx) Min(index, field, view string, shard uint64) (uint64, bool, error) {
	tx, err := mtx.tx(index, shard)
	if err != nil {
		return 0, false, err
	}
	return tx.Min(index, field, view, shard)
}

func (mtx *MultiTx) UnionInPlace(index, field, view string, shard uint64, others ...*roaring.Bitmap) error {
	tx, err := mtx.tx(index, shard)
	if err != nil {
		return err
	}
	return tx.UnionInPlace(index, field, view, shard, others...)
}

func (mtx *MultiTx) CountRange(index, field, view string, shard uint64, start, end uint64) (uint64, error) {
	tx, err := mtx.tx(index, shard)
	if err != nil {
		return 0, err
	}
	return tx.CountRange(index, field, view, shard, start, end)
}

func (mtx *MultiTx) OffsetRange(index, field, view string, shard uint64, offset, start, end uint64) (*roaring.Bitmap, error) {
	tx, err := mtx.tx(index, shard)
	if err != nil {
		return nil, err
	}
	return tx.OffsetRange(index, field, view, shard, offset, start, end)
}

// tx returns a transaction by index/shard. Reuses transaction if already open.
// Otherwise begins a new transaction.
func (mtx *MultiTx) tx(index string, shard uint64) (_ Tx, err error) {
	mtx.mu.Lock()
	defer mtx.mu.Unlock()

	mkey := multiTxKey{index: index, shard: shard, write: mtx.writable}

	// Lookup transaction from cache.
	tx := mtx.txs[mkey]
	if tx != nil {
		return tx, nil
	}

	// If transaction doesn't exist, lookup the index.
	idx := mtx.index
	if mtx.holder != nil {
		if idx = mtx.holder.Index(index); idx == nil {
			return nil, ErrIndexNotFound
		}
	}

	// Begin tranaction & cache it.
	if tx, err = idx.BeginTx(mtx.writable, shard); err != nil {
		return nil, err
	}
	mtx.txs[mkey] = tx

	return tx, nil
}

// version of the above for SliceOfShards(), where we don't have a shard.
func (mtx *MultiTx) txNoShard(index string) (_ Tx, err error) {
	mtx.mu.Lock()
	defer mtx.mu.Unlock()

	// Lookup transaction from cache.
	for _, tx := range mtx.txs {
		if tx.(*RoaringTx).Index.name == index {
			return tx, nil
		}
	}
	panic(fmt.Sprintf("no prior RoaringTx available, looking up index='%v'", index))
}

type multiTxKey struct {
	index string
	shard uint64
	write bool
}

// RoaringTx represents a fake transaction object for Roaring storage.
type RoaringTx struct {
	write    bool
	Index    *Index
	Field    *Field
	fragment *fragment
}

func (tx *RoaringTx) Type() string {
	return RoaringTxn
}

func (tx *RoaringTx) Dump() {
	fmt.Printf("%v\n", tx.Index.StringifiedRoaringKeys())
}

func (tx *RoaringTx) UseRowCache() bool {
	return true
}

func (tx *RoaringTx) SliceOfShards(index, field, view, optionalViewPath string) (sliceOfShards []uint64, err error) {

	// SliceOfShards is based on view.openFragments()

	file, err := os.Open(filepath.Join(optionalViewPath, "fragments"))
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
			//AlwaysPrintf("WARNING: couldn't use non-integer file as shard in index/field/view %s/%s/%s: %s", index, field, view, fi.Name())
			//v.holder.Logger.Debugf("WARNING: couldn't use non-integer file as shard in index/field/view %s/%s/%s: %s", v.index, v.field, v.name, fi.Name())
			continue
		}
		sliceOfShards = append(sliceOfShards, shard)
	}
	return
}

func (tx *RoaringTx) Pointer() string {
	return fmt.Sprintf("%p", tx)
}

// NewTxIterator returns a *roaring.Iterator that MUST have Close() called on it BEFORE
// the transaction Commits or Rollsback.
func (tx *RoaringTx) NewTxIterator(index, field, view string, shard uint64) *roaring.Iterator {
	b, err := tx.bitmap(index, field, view, shard)
	panicOn(err)
	return b.Iterator()
}

// ImportRoaringBits return values changed and rowSet will be inaccurate if
// the data []byte is supplied. This mimics the traditional roaring-per-file
// and should be faster.
func (tx *RoaringTx) ImportRoaringBits(index, field, view string, shard uint64, rit roaring.RoaringIterator, clear bool, log bool, rowSize uint64, data []byte) (changed int, rowSet map[uint64]int, err error) {
	f, err := tx.getFragment(index, field, view, shard)
	if err != nil {
		return 0, nil, err
	}
	if len(data) > 0 {
		// changed and rowSet are ignored anyway when len(data) > 0;
		// when we are called from fragment.fillFragmentFromArchive()
		// which is the only place the data []byte is supplied.
		// blueGreenTx also turns off the checks in this case.
		return 0, nil, f.readStorageFromArchive(bytes.NewBuffer(data))
	}

	changed, rowSet, err = f.storage.ImportRoaringRawIterator(rit, clear, true, rowSize)
	return
}

func (tx *RoaringTx) Readonly() bool {
	return !tx.write
}

func (tx *RoaringTx) IncrementOpN(index, field, view string, shard uint64, changedN int) {
	frag, err := tx.getFragment(index, field, view, shard)
	panicOn(err)
	frag.incrementOpN(changedN)
}

// Rollback is a no-op as Roaring does not support transactions.
func (tx *RoaringTx) Rollback() {}

// Commit is a no-op as Roaring does not support transactions.
func (tx *RoaringTx) Commit() error {
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

func (tx *RoaringTx) Add(index, field, view string, shard uint64, batched bool, a ...uint64) (changeCount int, err error) {
	b, err := tx.bitmap(index, field, view, shard)
	if err != nil {
		return 0, err
	}
	if !batched {
		changed, err := b.Add(a...)
		if changed {
			return 1, err
		}
		return 0, err
	}

	// Note: do not replace b.AddN() with b.DirectAddN().
	// DirectAddN() does not do op-log operations inside roaring
	// This creates a problem because RoaringTx needs the op-log
	// to know when to flush the fragment to disk.
	count, err := b.AddN(a...) // AddN does oplog batches. needed to keep op-log up to date.

	return count, err
}

func (tx *RoaringTx) Remove(index, field, view string, shard uint64, a ...uint64) (changeCount int, err error) {
	b, err := tx.bitmap(index, field, view, shard)
	if err != nil {
		return 0, err
	}
	changed, err := b.Remove(a...) // green TestFragment_Bug_Q2DoubleDelete
	panicOn(err)
	if changed {
		return 1, err
	} else {
		return 0, err
	}

	// Note: don't replace b.Remove(a...) with b.RemoveN(a...) or
	// with b.DirectRemoveN(a...). If you do, you'll see
	// TestFragment_Bug_Q2DoubleDelete go red.
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
		return nil, false, err
	}
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

func (tx *RoaringTx) UnionInPlace(index, field, view string, shard uint64, others ...*roaring.Bitmap) error {
	b, err := tx.bitmap(index, field, view, shard)
	if err != nil {
		return err
	}
	b.UnionInPlace(others...)
	return nil
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
		if tx.fragment.index != index ||
			tx.fragment.field != field ||
			tx.fragment.view != view ||
			tx.fragment.shard != shard {
			panic(fmt.Sprintf("different fragment cached vs requested. tx.fragment='%#v', index='%v', field='%v'; view='%v'; shard='%v'", tx.fragment, index, field, view, shard))
		}
		return tx.fragment, nil
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
				return nil, ErrFieldNotFound
			}
		} else {
			if f = idx.Field(field); f == nil {
				return nil, ErrFieldNotFound
			}
		}
	}
	// INVAR: f is not nil.

	v := f.view(view)
	if v == nil {
		return nil, errors.Errorf("view not found: %q", view)
	}

	frag := v.Fragment(shard)

	if frag == nil {
		return nil, fmt.Errorf("fragment not found: %q / %q / %d", field, view, shard)
	}

	// Note: we cannot cache frag into tx.fragment.
	// Empirically, it breaks 245 top-level pilosa tests.
	// tx.fragment = frag // breaks the world.

	return frag, nil
}

func (tx *RoaringTx) bitmap(index, field, view string, shard uint64) (*roaring.Bitmap, error) {
	frag, err := tx.getFragment(index, field, view, shard)
	if err != nil {
		return nil, err
	}
	return frag.storage, nil
}

type RoaringStore struct{}

func NewRoaringStore() *RoaringStore {
	return &RoaringStore{}
}

func (db *RoaringStore) Close() error {
	return nil
}

func (db *RoaringStore) DeleteField(index, field, fieldPath string) error {

	// under blue-green badger_roaring, the directory will not be found, b/c badger will have
	// already done the os.RemoveAll().	BUT, RemoveAll returns nil error in this case. Docs:
	// "If the path does not exist, RemoveAll returns nil (no error)"
	err := os.RemoveAll(fieldPath)
	if err != nil {
		return errors.Wrap(err, "removing directory")
	}
	return nil
}

// frag should be passed by any RoaringTx user, but for RBF/Badger it can be nil.
func (db *RoaringStore) DeleteFragment(index, field, view string, shard uint64, frag interface{}) error {

	fragment, ok := frag.(*fragment)
	if !ok {
		return fmt.Errorf("RoaringStore.DeleteFragment must get frag of type *fragment, but got '%T'", frag)
	}

	// Close data files before deletion.
	if err := fragment.Close(); err != nil {
		return errors.Wrap(err, "closing fragment")
	}

	// Delete fragment file.
	if err := os.Remove(fragment.path); err != nil {
		return errors.Wrap(err, "deleting fragment file")
	}

	// Delete fragment cache file.
	if err := os.Remove(fragment.cachePath()); err != nil {
		return errors.Wrap(err, fmt.Sprintf("no cache file to delete for shard %d", fragment.shard))
	}
	return nil
}

func (tx *RoaringTx) RoaringBitmapReader(index, field, view string, shard uint64, fragmentPathForRoaring string) (r io.ReadCloser, sz int64, err error) {
	file, err := os.Open(fragmentPathForRoaring) // open the fragment file
	if err != nil {
		return nil, -1, err
	}
	fi, err := file.Stat()
	if err != nil {
		return nil, -1, errors.Wrap(err, "statting")
	}
	sz = fi.Size()
	r = file
	return
}
