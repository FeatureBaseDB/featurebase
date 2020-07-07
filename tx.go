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
	"sync"

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
	// See the roaring package for details of the RoaringIterator.
	// If clear is true, the bits from rit are cleared, otherwise they are set in the
	// specifed fragment.
	ImportRoaringBits(index, field, view string, shard uint64, rit roaring.RoaringIterator, clear bool, log bool, rowSize uint64) (changed int, rowSet map[uint64]int, err error)
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

func (mtx *MultiTx) UseRowCache() bool {
	return true
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

func (tx *MultiTx) ImportRoaringBits(index, field, view string, shard uint64, rit roaring.RoaringIterator, clear bool, log bool, rowSize uint64) (changed int, rowSet map[uint64]int, err error) {
	panic("not done")
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

func (tx *RoaringTx) UseRowCache() bool {
	return true
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

func (tx *RoaringTx) ImportRoaringBits(index, field, view string, shard uint64, rit roaring.RoaringIterator, clear bool, log bool, rowSize uint64) (changed int, rowSet map[uint64]int, err error) {
	b, err := tx.bitmap(index, field, view, shard)
	panicOn(err)
	if err != nil {
		return 0, nil, err
	}
	return b.ImportRoaringRawIterator(rit, clear, true, rowSize)
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

	// If a fragment is attached, always use it.
	if tx.fragment != nil {
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
		panic(fmt.Sprintf("fragment not found: %q / %q / %d", field, view, shard))
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
