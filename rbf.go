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
	"io"
	"math"
	"os"
	"strings"
	"sync"

	"github.com/molecula/featurebase/v2/rbf"
	rbfcfg "github.com/molecula/featurebase/v2/rbf/cfg"
	"github.com/molecula/featurebase/v2/roaring"
	txkey "github.com/molecula/featurebase/v2/short_txkey"
	"github.com/molecula/featurebase/v2/storage"

	"github.com/molecula/featurebase/v2/vprint"
	"github.com/pkg/errors"
)

// RbfDBWrapper wraps an *rbf.DB
type RbfDBWrapper struct {
	path string
	db   *rbf.DB
	cfg  *rbfcfg.Config
	reg  *rbfDBRegistrar
	muDb sync.Mutex

	openTx map[*RBFTx]bool

	// make Close() idempotent, avoiding panic on double Close()
	closed bool

	//DeleteEmptyContainer bool // needed for roaring compat?

	doAllocZero bool
}

func (w *RbfDBWrapper) Path() string {
	return w.path
}

func (w *RbfDBWrapper) SetHolder(h *Holder) {
	// don't need it at the moment
	//w.h = h
}

func (w *RbfDBWrapper) CleanupTx(tx Tx) {
	r := tx.(*RBFTx)
	r.mu.Lock()
	if r.done {
		r.mu.Unlock()
		return
	}
	r.done = true
	r.mu.Unlock()

	// try not to hold r.mu while locking w.muDb
	w.muDb.Lock()

	delete(w.openTx, r)

	w.muDb.Unlock()
}

// rbfDBRegistrar also allows opening the same path twice to
// result in sharing the same open database handle, and
// thus the same transactional guarantees.
//
type rbfDBRegistrar struct {
	mu sync.Mutex
	mp map[*RbfDBWrapper]bool

	path2db map[string]*RbfDBWrapper

	rbfConfig *rbfcfg.Config
}

func (r *rbfDBRegistrar) SetRBFConfig(cfg *rbfcfg.Config) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rbfConfig = cfg
}

func (r *rbfDBRegistrar) Size() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	nmp := len(r.mp)
	npa := len(r.path2db)
	if nmp != npa {
		panic(fmt.Sprintf("nmp=%v, vs npa=%v", nmp, npa))
	}
	return nmp
}

var globalRbfDBReg *rbfDBRegistrar = newRbfDBRegistrar()

func newRbfDBRegistrar() *rbfDBRegistrar {
	return &rbfDBRegistrar{
		mp:      make(map[*RbfDBWrapper]bool),
		path2db: make(map[string]*RbfDBWrapper),
	}
}

// register each rbf.DB created, so we dedup and can
// can clean them up. This is called by OpenDBWrapper() while
// holding the r.mu.Lock, since it needs to atomically
// check the registry and make a new instance only
// if one does not exist for its path, and otherwise
// return the existing instance.
func (r *rbfDBRegistrar) unprotectedRegister(w *RbfDBWrapper) {
	r.mp[w] = true
	r.path2db[w.path] = w
}

// unregister removes w from r
func (r *rbfDBRegistrar) unregister(w *RbfDBWrapper) {
	r.mu.Lock()
	delete(r.mp, w)
	delete(r.path2db, w.path)
	r.mu.Unlock()
}

// OpenDBWrapper opens the database in the path directory
// without deleting any prior content. Any
// database directory will have the "-rbf" suffix.
//
// OpenDBWrapper will check the registry and make a new instance only
// if one does not exist for its path. Otherwise it returns
// the existing instance. This insures only one RbfDBWrapper
// per bpath in this pilosa node.
func (r *rbfDBRegistrar) OpenDBWrapper(path string, doAllocZero bool, cfg *storage.Config) (DBWrapper, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	w, ok := r.path2db[path]
	if ok {
		// creates the effect of having only one DB open per pilosa node.
		return w, nil
	}
	if r.rbfConfig == nil {
		r.rbfConfig = rbfcfg.NewDefaultConfig()
		r.rbfConfig.DoAllocZero = doAllocZero
		r.rbfConfig.FsyncEnabled = cfg.FsyncEnabled
	}
	db := rbf.NewDB(path, r.rbfConfig)

	w = &RbfDBWrapper{
		reg:         r,
		path:        path,
		db:          db,
		doAllocZero: doAllocZero,
		openTx:      make(map[*RBFTx]bool),
		cfg:         r.rbfConfig,
	}
	r.unprotectedRegister(w)

	err := db.Open()
	if err != nil {
		panic(fmt.Sprintf("cannot open rbfDB at path '%v': '%v'", path, err))
	}
	return w, nil
}

type RBFTx struct {
	// initialIndex is only a debugging aid. Transactions
	// can cross indexes. It can be left empty without consequence.
	initialIndex string
	tx           *rbf.Tx
	o            Txo
	Db           *RbfDBWrapper

	done bool
	mu   sync.Mutex // protect done as it changes state
}

func (tx *RBFTx) DBPath() string {
	return tx.tx.DBPath()
}

func (tx *RBFTx) Type() string {
	return RBFTxn
}

func (tx *RBFTx) Rollback() {
	tx.tx.Rollback()
	tx.Db.CleanupTx(tx)
}

func (tx *RBFTx) Commit() (err error) {
	err = tx.tx.Commit()
	tx.Db.CleanupTx(tx)
	return err
}

func (tx *RBFTx) RoaringBitmap(index, field, view string, shard uint64) (*roaring.Bitmap, error) {
	return tx.tx.RoaringBitmap(rbfName(index, field, view, shard))
}

func (tx *RBFTx) Container(index, field, view string, shard uint64, key uint64) (*roaring.Container, error) {
	return tx.tx.Container(rbfName(index, field, view, shard), key)
}

func (tx *RBFTx) PutContainer(index, field, view string, shard uint64, key uint64, c *roaring.Container) error {
	return tx.tx.PutContainer(rbfName(index, field, view, shard), key, c)
}

func (tx *RBFTx) RemoveContainer(index, field, view string, shard uint64, key uint64) error {
	return tx.tx.RemoveContainer(rbfName(index, field, view, shard), key)
}

// Add sets all the a bits hot in the specified fragment.
func (tx *RBFTx) Add(index, field, view string, shard uint64, a ...uint64) (changeCount int, err error) {
	return tx.addOrRemove(index, field, view, shard, false, a...)
}

// Remove clears all the specified a bits in the chosen fragment.
func (tx *RBFTx) Remove(index, field, view string, shard uint64, a ...uint64) (changeCount int, err error) {
	return tx.addOrRemove(index, field, view, shard, true, a...)
}

// sortedParanoia is a flag to enable a check for unsorted inputs to addOrRemove,
// which is expensive in practice and only really useful occasionally.
const sortedParanoia = false

func (tx *RBFTx) addOrRemove(index, field, view string, shard uint64, remove bool, a ...uint64) (changeCount int, err error) {
	if len(a) == 0 {
		return 0, nil
	}
	name := rbfName(index, field, view, shard)
	// this special case can/should possibly go away, except that it
	// turns out to be by far the most common case, and we need to know
	// there's at least two items to simplify the check-sorted thing.
	if len(a) == 1 {
		hi, lo := highbits(a[0]), lowbits(a[0])
		rc, err := tx.tx.Container(name, hi)
		if err != nil {
			return 0, errors.Wrap(err, "failed to retrieve container")
		}
		if remove {
			if rc.N() == 0 {
				return 0, nil
			}
			rc1, chng := rc.Remove(lo)
			if !chng {
				return 0, nil
			}
			if rc1.N() == 0 {
				err = tx.tx.RemoveContainer(name, hi)
			} else {
				err = tx.tx.PutContainer(name, hi, rc1)
			}
			if err != nil {
				return 0, err
			}
			return 1, nil
		} else {
			rc2, chng := rc.Add(lo)
			if !chng {
				return 0, nil
			}
			err = tx.tx.PutContainer(name, hi, rc2)
			if err != nil {
				return 0, err
			}
			return 1, nil
		}
	}

	var lastHi uint64 = math.MaxUint64 // highbits is always less than this starter.
	var rc *roaring.Container
	var hi uint64
	var lo uint16

	// we can accept sorted either ascending or descending.
	sign := a[1] - a[0]
	prev := a[0] - sign
	sign >>= 63
	for i, v := range a {
		// This check is noticably expensive (a few percent in some
		// use cases) and as long as it passes occasionally it's probably
		// not important to run it all the time, and anyway panic is
		// not a good choice outside of testing.
		if sortedParanoia {
			if (v-prev)>>63 != sign {
				explain := fmt.Sprintf("addOrRemove: %d < %d != %d < %d", v, prev, a[1], a[0])
				panic(explain)
			}
			if v == prev {
				explain := fmt.Sprintf("addOrRemove: %d twice", v)
				panic(explain)
			}
		}
		prev = v
		hi, lo = highbits(v), lowbits(v)
		if hi != lastHi {
			// either first time through, or changed to a different container.
			// do we need put the last updated container now?
			if i > 0 {
				// not first time through, write what we got.
				if remove && (rc == nil || rc.N() == 0) {
					err = tx.tx.RemoveContainer(name, lastHi)
					if err != nil {
						return 0, errors.Wrap(err, "failed to remove container")
					}
				} else {
					err = tx.tx.PutContainer(name, lastHi, rc)
					if err != nil {
						return 0, errors.Wrap(err, "failed to put container")
					}
				}
			}
			// get the next container
			rc, err = tx.tx.Container(name, hi)
			if err != nil {
				return 0, errors.Wrap(err, "failed to retrieve container")
			}
		} // else same container, keep adding bits to rct.
		chng := false
		// rc can be nil before, and nil after, in both Remove/Add below.
		// The roaring container add() and remove() methods handle this.
		if remove {
			rc, chng = rc.Remove(lo)
		} else {
			rc, chng = rc.Add(lo)
		}
		if chng {
			changeCount++
		}
		lastHi = hi
	}
	// write the last updates.
	if remove {
		if rc == nil || rc.N() == 0 {
			err = tx.tx.RemoveContainer(name, hi)
			if err != nil {
				return 0, errors.Wrap(err, "failed to remove container")
			}
		} else {
			err = tx.tx.PutContainer(name, hi, rc)
			if err != nil {
				return 0, errors.Wrap(err, "failed to put container")
			}
		}
	} else {
		if rc == nil || rc.N() == 0 {
			panic("there should be no way to have an empty bitmap AFTER an Add() operation")
		}
		err = tx.tx.PutContainer(name, hi, rc)
		if err != nil {
			return 0, errors.Wrap(err, "failed to put container")
		}
	}
	return
}

func (tx *RBFTx) Contains(index, field, view string, shard uint64, v uint64) (exists bool, err error) {
	return tx.tx.Contains(rbfName(index, field, view, shard), v)
}

func (tx *RBFTx) ContainerIterator(index, field, view string, shard uint64, key uint64) (citer roaring.ContainerIterator, found bool, err error) {
	return tx.tx.ContainerIterator(rbfName(index, field, view, shard), key)
}

func (tx *RBFTx) ForEach(index, field, view string, shard uint64, fn func(i uint64) error) error {
	return tx.tx.ForEach(rbfName(index, field, view, shard), fn)
}

func (tx *RBFTx) ForEachRange(index, field, view string, shard uint64, start, end uint64, fn func(uint64) error) error {
	return tx.tx.ForEachRange(rbfName(index, field, view, shard), start, end, fn)
}

func (tx *RBFTx) Count(index, field, view string, shard uint64) (uint64, error) {
	return tx.tx.Count(rbfName(index, field, view, shard))
}

func (tx *RBFTx) Max(index, field, view string, shard uint64) (uint64, error) {
	return tx.tx.Max(rbfName(index, field, view, shard))
}

func (tx *RBFTx) Min(index, field, view string, shard uint64) (uint64, bool, error) {
	return tx.tx.Min(rbfName(index, field, view, shard))
}

// CountRange returns the count of hot bits in the start, end range on the fragment.
// roaring.countRange counts the number of bits set between [start, end).
func (tx *RBFTx) CountRange(index, field, view string, shard uint64, start, end uint64) (n uint64, err error) {
	return tx.tx.CountRange(rbfName(index, field, view, shard), start, end)
}

func (tx *RBFTx) OffsetRange(index, field, view string, shard uint64, offset, start, end uint64) (*roaring.Bitmap, error) {
	return tx.tx.OffsetRange(rbfName(index, field, view, shard), offset, start, end)
}

func (tx *RBFTx) ImportRoaringBits(index, field, view string, shard uint64, rit roaring.RoaringIterator, clear bool, log bool, rowSize uint64) (changed int, rowSet map[uint64]int, err error) {
	return tx.tx.ImportRoaringBits(rbfName(index, field, view, shard), rit, clear, log, rowSize)
}

func (tx *RBFTx) NewTxIterator(index, field, view string, shard uint64) *roaring.Iterator {
	b, err := tx.RoaringBitmap(index, field, view, shard)
	vprint.PanicOn(err)
	return b.Iterator()
}

func (tx *RBFTx) ApplyFilter(index, field, view string, shard uint64, ckey uint64, filter roaring.BitmapFilter) (err error) {
	return tx.tx.ApplyFilter(rbfName(index, field, view, shard), ckey, filter)
}

func (tx *RBFTx) GetSortedFieldViewList(idx *Index, shard uint64) (fvs []txkey.FieldView, err error) {
	return tx.tx.GetSortedFieldViewList()
}

func (tx *RBFTx) GetFieldSizeBytes(index, field string) (uint64, error) {
	return tx.tx.GetSizeBytesWithPrefix(string(txkey.FieldPrefix(index, field)))
}

// SnapshotReader returns a reader that provides a snapshot of the current database.
func (tx *RBFTx) SnapshotReader() (io.Reader, error) {
	return tx.tx.SnapshotReader()
}

// rbfName returns a NULL-separated key used for identifying bitmap maps in RBF.
func rbfName(index, field, view string, shard uint64) string {
	return string(txkey.Prefix(index, field, view, shard))
}

// rbfFieldPrefix returns a prefix for field keys in RBF.
func rbfFieldPrefix(index, field string) string {
	//return fmt.Sprintf("%s\x00%s\x00", index, field)
	return string(txkey.FieldPrefix(index, field))
}

func (w *RbfDBWrapper) HasData() (has bool, err error) {
	w.muDb.Lock()
	defer w.muDb.Unlock()
	return w.db.HasData(false) // false => any prior attempt at write means we "have data"
}

func (w *RbfDBWrapper) DeleteField(index, field, fieldPath string) error {
	w.muDb.Lock()
	defer w.muDb.Unlock()

	if err := os.RemoveAll(fieldPath); err != nil {
		return errors.Wrap(err, "removing directory")
	}

	tx, err := w.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := tx.DeleteBitmapsWithPrefix(rbfFieldPrefix(index, field)); err != nil {
		return err
	}
	return tx.Commit()
}

func (w *RbfDBWrapper) DeleteIndex(indexName string) error {

	if strings.Contains(indexName, "'") {
		return fmt.Errorf("error: bad indexName `%v` in RbfDBWrapper.DeleteIndex() call: indexName cannot contain apostrophes/single quotes", indexName)
	}
	prefix := txkey.IndexOnlyPrefix(indexName)

	w.muDb.Lock()
	defer w.muDb.Unlock()

	tx, err := w.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := tx.DeleteBitmapsWithPrefix(string(prefix)); err != nil {
		return err
	}
	return tx.Commit()
}

func (w *RbfDBWrapper) Close() error {
	w.muDb.Lock()
	defer w.muDb.Unlock()
	if !w.closed {
		w.reg.unregister(w)
		w.closed = true
	}
	return w.db.Close()
}

// needed to handle the special case on reload, the close method unregisters the wrapper and all that is
// required is the backing file get reloaded

func (w *RbfDBWrapper) CloseDB() error {
	w.muDb.Lock()
	defer w.muDb.Unlock()
	w.closed = true
	return w.db.Close()
}
func (w *RbfDBWrapper) OpenDB() error {
	w.muDb.Lock()
	defer w.muDb.Unlock()
	err := w.db.Open()
	if err != nil {
		return err
	}
	w.closed = false
	return nil
}

func (w *RbfDBWrapper) NewTx(write bool, initialIndex string, o Txo) (_ Tx, err error) {
	tx, err := w.db.Begin(write)
	if err != nil {
		return nil, err
	}

	rtx := &RBFTx{
		tx:           tx,
		initialIndex: initialIndex,
		o:            o,
		Db:           w,
	}

	w.muDb.Lock()
	w.openTx[rtx] = true
	w.muDb.Unlock()

	return rtx, nil
}

func (w *RbfDBWrapper) DeleteFragment(index, field, view string, shard uint64, frag interface{}) error {
	tx, err := w.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	err = tx.DeleteBitmapsWithPrefix(rbfName(index, field, view, shard))
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (w *RbfDBWrapper) OpenListString() (r string) {
	return "rbf OpenListString not implemented yet"
}
