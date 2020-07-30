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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/pilosa/pilosa/v2/rbf"
	"github.com/pilosa/pilosa/v2/roaring"
	"github.com/pkg/errors"
)

// batch operations want Tx.Add(batched=doBatch), while bit-at-a-time want Tx.Add(batched=!doBatched)
// Used in Tx.Add() to get consistency between RoaringTx and other Tx implementations on
// the changeCount returned.
const doBatched = false // must be false, do not change this without adjusting the Add() implementations correspondingly.

// writable initializes Tx that update, use !writable for read-only.
const writable = true

// Tx providers offer transactional storage for high-level roaring.Bitmaps and
// low-level roaring.Containers.
//
// The common 4-tuple of (index, field, view, shard) jointly specify a fragment.
// A fragment conceptually holds one roaring.Bitmap.
//
// Within the fragment, the ckey or container-key is the uint64 that specifies
// the high 48-bits of the roaring.Bitmap 64-bit space.
// The ckey is used to retreive a specific roaring.Container that
// is either a run, array, or raw-bitmap. The roaring.Container is the
// low 16-bits of the roaring.Bitmap space. Its size is at most
// 8KB (2^16 bits / (8 bits / byte) == 8192 bytes).
//
// The grain of the transaction is guaranteed to be at least at the shard
// within one index. Therefore updates to the any of the fields within
// the same shard will be atomically visible only once the transaction commits.
// Reads from another, concurrently open, transaction will not see updates
// that have not been committed.
type Tx interface {

	// Type returns "roaring", "rbf", "badger", "badger_roaring", or one of the other
	// blue-green Tx types at the top of txfactory.go
	Type() string

	// Rollback must be called the end of read-only transactions. Either
	// Rollback or Commit must be called at the end of writable transactions.
	// It is safe to call Rollback multiple times, but it must be
	// called at least once to release resources. Any Rollback after
	// a Commit is ignored, so 'defer tx.Rollback()' should be commonly
	// written after starting a new transaction.
	//
	// If there is an error during internal Rollback processing,
	// this would be quite serious, and the underlying storage is
	// expected to panic. Hence there is no explicit error returned
	// from Rollback that needs to be checked.
	Rollback()

	// Commit makes the updates in the Tx visible to subsequent transactions.
	Commit() error

	// Readonly returns the flag this transaction was created with
	// during NewTx. If the transaction is writable, it will return false.
	Readonly() bool

	// UseRowCache is used by fragment.go unprotectedRow() to determine
	// dynamically at runtime if RoaringTx
	// are in use, which for continuity want to continue to use the
	// rowCache, or if other storage engines (RBF, Badger) are in
	// use, which will mean that the bitmap data stored by the
	// rowCache can disappear as it is un-mmap-ed, causing crashes.
	UseRowCache() bool

	// IncrementOpN updates internal statistics with the changedN provided.
	IncrementOpN(index, field, view string, shard uint64, changedN int)

	// Pointer gives us a memory address for the underlying
	// transaction for debugging.
	// It is public because we use it in roaring to report invalid
	// container memory access outside of a transaction.
	Pointer() string

	// NewTxIterator returns it, a *roaring.Iterator whose it.Next() will
	// successively return each uint64 stored in the conceptual roaring.Bitmap
	// for the specified fragment.
	NewTxIterator(index, field, view string, shard uint64) (it *roaring.Iterator)

	// ContainerIterator loops over the containers in the conceptual
	// roaring.Bitmap for the specified fragment.
	// Calling Next() on the returned roaring.ContainerIterator gives
	// you a roaring.Container that is either run, array, or raw bitmap.
	// Return value 'found' is true when the ckey container was present.
	// ckey of 0 gives all containers (in the fragment).
	//
	// ContainerIterator must not have side-effects. blueGreenTx will
	// call it at the very beginning of commit to verify db contents.
	//
	ContainerIterator(index, field, view string, shard uint64, ckey uint64) (citer roaring.ContainerIterator, found bool, err error)

	// RoaringBitmap retreives the roaring.Bitmap for the entire shard.
	RoaringBitmap(index, field, view string, shard uint64) (*roaring.Bitmap, error)

	// Container returns the roaring.Container for the given ckey
	// (container-key or highbits), in the chosen fragment.
	Container(index, field, view string, shard uint64, ckey uint64) (*roaring.Container, error)

	// PutContainer stores c under the given ckey (container-key), in the specified fragment.
	PutContainer(index, field, view string, shard uint64, ckey uint64, c *roaring.Container) error

	// RemoveContainer deletes the roaring.Container under the given ckey (container-key),
	// in the specified fragment.
	RemoveContainer(index, field, view string, shard uint64, ckey uint64) error

	// Add adds the 'a' bits to the specified fragment.
	//
	// Using batched=true allows efficient bulk-import.
	//
	// Notes on the RoaringTx implementation:
	// If the batched flag is true, then the roaring.Bitmap.AddN() is used, which does oplog batches.
	// If the batched flag is false, then the roaring.Bitmap.Add() is used, which does simple opTypeAdd single adds.
	//
	// Beware: if batched is true, then changeCount will only ever be 0 or 1,
	// because it calls roaring.Add().
	// If batched is false, we call roaring.DirectAddN() and then changeCount
	// will be accurate if the changeCount is greater than 0.
	//
	// Hence: only ever call Add(batched=false) if changeCount is expected to be 0 or 1.
	// Or, must use Add(batched=true) if changeCount can be > 1.
	//
	Add(index, field, view string, shard uint64, batched bool, a ...uint64) (changeCount int, err error)

	// Remove removes the 'a' values from the Bitmap for the fragment.
	Remove(index, field, view string, shard uint64, a ...uint64) (changeCount int, err error)

	// Contains tests if the uint64 v is stored in the fragment's Bitmap.
	Contains(index, field, view string, shard uint64, v uint64) (exists bool, err error)

	// ForEach
	ForEach(index, field, view string, shard uint64, fn func(i uint64) error) error

	// ForEachRange
	ForEachRange(index, field, view string, shard uint64, start, end uint64, fn func(uint64) error) error

	// Count
	Count(index, field, view string, shard uint64) (uint64, error)

	// Max
	Max(index, field, view string, shard uint64) (uint64, error)

	// Min
	Min(index, field, view string, shard uint64) (uint64, bool, error)

	// UnionInPlace
	UnionInPlace(index, field, view string, shard uint64, others ...*roaring.Bitmap) error

	// CountRange
	CountRange(index, field, view string, shard uint64, start, end uint64) (uint64, error)

	// OffsetRange
	OffsetRange(index, field, view string, shard uint64, offset, start, end uint64) (*roaring.Bitmap, error)

	// ImportRoaringBits does efficient bulk import using rit, a roaring.RoaringIterator.
	//
	// See the roaring package for details of the RoaringIterator.
	//
	// If clear is true, the bits from rit are cleared, otherwise they are set in the
	// specifed fragment.
	//
	// The data argument can be nil, its ignored for RBF/BadgerTx. It is supplied to
	// RoaringTx.ImportRoaringBits() in fragment.go fragment.fillFragmentFromArchive()
	// to do the traditional fragment.readStorageFromArchive() which
	// does some in memory field/view/fragment metadata updates.
	// It makes blueGreenTx testing viable too.
	//
	// ImportRoaringBits return values changed and rowSet may be inaccurate if
	// the data []byte is supplied (the RoaringTx implementation neglects this for speed).
	ImportRoaringBits(index, field, view string, shard uint64, rit roaring.RoaringIterator, clear bool, log bool, rowSize uint64, data []byte) (changed int, rowSet map[uint64]int, err error)

	RoaringBitmapReader(index, field, view string, shard uint64, fragmentPathForRoaring string) (r io.ReadCloser, sz int64, err error)

	// SliceOfShards returns all of the shards for the specified index, field, view triple.
	// Use within pilosa supposes a new read-only transaction was created just
	// for the SliceOfShards() call. The legacy RoaringTx version is the only
	// one that needs optionalViewPath; any other Tx implementation can ignore that.
	SliceOfShards(index, field, view, optionalViewPath string) (sliceOfShards []uint64, err error)

	// Dump is for debugging, what does this Tx see as its database?
	Dump()
}

// TxStore has operations that will create and commit multiple
// Tx on a backing store.
type TxStore interface {

	// DeleteFragment deletes all the containers in a fragment.
	//
	// This is not in a Tx because it will often do too many deletes for a single
	// transaction, and clients would be suprised to find their Tx had already
	// been commited and they are getting an error on double-Commit.
	// Instead each TxStore implementation creates and commits as many
	// transactions as needed.
	//
	// Argument frag should be passed by any RoaringTx user, but for RBF/Badger it can be nil.
	// If not nil, it must be of type *fragment. If frag is supplied, then
	// index must be equal to frag.index, field equal to frag.field, view equal
	// to frag.view, and shard equal to frag.shard.
	//
	DeleteFragment(index, field, view string, shard uint64, frag interface{}) error

	DeleteField(index, field string) error

	// Close shuts down the database.
	Close() error
}

// RawRoaringData used by ImportRoaringBits.
// must be consumable by roaring.newRoaringIterator()
type RawRoaringData struct {
	data []byte
}

func (rr *RawRoaringData) Iterator() (roaring.RoaringIterator, error) {
	return roaring.NewRoaringIterator(rr.data)
}

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
		//panic(fmt.Sprintf("fragment not found: %q / %q / %d", field, view, shard))
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

type RBFTx struct {
	index string
	tx    *rbf.Tx
}

func (tx *RBFTx) Type() string {
	return RBFTxn
}

func (tx *RBFTx) Rollback() {
	tx.tx.Rollback()
}

func (tx *RBFTx) Commit() error {
	return tx.tx.Commit()
}

func (tx *RBFTx) RoaringBitmap(index, field, view string, shard uint64) (*roaring.Bitmap, error) {
	return tx.tx.RoaringBitmap(rbfName(field, view, shard))
}

func (tx *RBFTx) Container(index, field, view string, shard uint64, key uint64) (*roaring.Container, error) {
	return tx.tx.Container(rbfName(field, view, shard), key)
}

func (tx *RBFTx) PutContainer(index, field, view string, shard uint64, key uint64, c *roaring.Container) error {
	return tx.tx.PutContainer(rbfName(field, view, shard), key, c)
}

func (tx *RBFTx) RemoveContainer(index, field, view string, shard uint64, key uint64) error {
	return tx.tx.RemoveContainer(rbfName(field, view, shard), key)
}

func (tx *RBFTx) Add(index, field, view string, shard uint64, batched bool, a ...uint64) (changeCount int, err error) {
	return tx.tx.Add(rbfName(field, view, shard), a...)
}

func (tx *RBFTx) Remove(index, field, view string, shard uint64, a ...uint64) (changeCount int, err error) {
	return tx.tx.Remove(rbfName(field, view, shard), a...)
}

func (tx *RBFTx) Contains(index, field, view string, shard uint64, v uint64) (exists bool, err error) {
	return tx.tx.Contains(rbfName(field, view, shard), v)
}

func (tx *RBFTx) ContainerIterator(index, field, view string, shard uint64, key uint64) (citer roaring.ContainerIterator, found bool, err error) {
	return tx.tx.ContainerIterator(rbfName(field, view, shard), key)
}

func (tx *RBFTx) ForEach(index, field, view string, shard uint64, fn func(i uint64) error) error {
	return tx.tx.ForEach(rbfName(field, view, shard), fn)
}

func (tx *RBFTx) ForEachRange(index, field, view string, shard uint64, start, end uint64, fn func(uint64) error) error {
	return tx.tx.ForEachRange(rbfName(field, view, shard), start, end, fn)
}

func (tx *RBFTx) Count(index, field, view string, shard uint64) (uint64, error) {
	return tx.tx.Count(rbfName(field, view, shard))
}

func (tx *RBFTx) Max(index, field, view string, shard uint64) (uint64, error) {
	return tx.tx.Max(rbfName(field, view, shard))
}

func (tx *RBFTx) Min(index, field, view string, shard uint64) (uint64, bool, error) {
	return tx.tx.Min(rbfName(field, view, shard))
}

func (tx *RBFTx) UnionInPlace(index, field, view string, shard uint64, others ...*roaring.Bitmap) error {
	return tx.tx.UnionInPlace(rbfName(field, view, shard), others...)
}

func (tx *RBFTx) CountRange(index, field, view string, shard uint64, start, end uint64) (uint64, error) {
	return tx.tx.CountRange(rbfName(field, view, shard), start, end)
}

func (tx *RBFTx) OffsetRange(index, field, view string, shard uint64, offset, start, end uint64) (*roaring.Bitmap, error) {
	return tx.tx.OffsetRange(rbfName(field, view, shard), offset, start, end)
}

func (tx *RBFTx) IncrementOpN(index, field, view string, shard uint64, changedN int) {}

func (tx *RBFTx) ImportRoaringBits(index, field, view string, shard uint64, rit roaring.RoaringIterator, clear bool, log bool, rowSize uint64, data []byte) (changed int, rowSet map[uint64]int, err error) {
	// TODO: Implement RBFTX.ImportRoaringBits"
	return 0, make(map[uint64]int), nil
}

func (tx *RBFTx) RoaringBitmapReader(index, field, view string, shard uint64, fragmentPathForRoaring string) (r io.ReadCloser, sz int64, err error) {
	panic("TODO: Implement RBFTx.RoaringBitmapReader()")
}

func (tx *RBFTx) SliceOfShards(index, field, view, optionalViewPath string) (sliceOfShards []uint64, err error) {
	prefix := rbfFieldViewPrefix(field, view)

	names, err := tx.tx.BitmapNames()
	if err != nil {
		return nil, err
	}

	// Iterate over shard names and collect shards from matching field/view prefix.
	for _, name := range names {
		if !strings.HasPrefix(name, prefix) {
			continue
		}

		s := strings.TrimPrefix(name, prefix)
		shard, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return nil, errors.Wrap(err, "parse shard id from rbf key")
		}
		sliceOfShards = append(sliceOfShards, shard)
	}
	return sliceOfShards, nil
}

func (tx *RBFTx) NewTxIterator(index, field, view string, shard uint64) *roaring.Iterator {
	b, err := tx.RoaringBitmap(index, field, view, shard)
	panicOn(err)
	return b.Iterator()
}

func (tx *RBFTx) Pointer() string {
	return fmt.Sprintf("%p", tx)
}

func (tx *RBFTx) Dump() {
	tx.tx.Dump(tx.index)
}

// Readonly is true if the transaction is not read-and-write, but only doing reads.
func (tx *RBFTx) Readonly() bool {
	return !tx.tx.Writable()
}

func (tx *RBFTx) UseRowCache() bool {
	return false
}

// rbfName returns a NULL-separated key used for identifying bitmap maps in RBF.
func rbfName(field, view string, shard uint64) string {
	return fmt.Sprintf("%s\x00%s\x00%d", field, view, shard)
}

// rbfFieldViewPrefix returns a NULL-separated prefix for keys in RBF.
func rbfFieldViewPrefix(field, view string) string {
	return fmt.Sprintf("%s\x00%s\x00", field, view)
}
