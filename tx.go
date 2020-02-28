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

type Tx interface {
	Rollback() error
	Commit() error

	RoaringBitmap(index, field, view string, shard uint64) (*roaring.Bitmap, error)

	Container(index, field, view string, shard uint64, key uint64) (*roaring.Container, error)
	PutContainer(index, field, view string, shard uint64, key uint64, c *roaring.Container) error
	RemoveContainer(index, field, view string, shard uint64, key uint64) error

	Add(index, field, view string, shard uint64, a ...uint64) (changed bool, err error)
	Remove(index, field, view string, shard uint64, a ...uint64) (changed bool, err error)
	Contains(index, field, view string, shard uint64, v uint64) (exists bool, err error)

	ContainerIterator(index, field, view string, shard uint64, key uint64) (citer roaring.ContainerIterator, found bool, err error)
	ForEach(index, field, view string, shard uint64, fn func(i uint64) error) error
	ForEachRange(index, field, view string, shard uint64, start, end uint64, fn func(uint64) error) error

	Count(index, field, view string, shard uint64) (uint64, error)
	Max(index, field, view string, shard uint64) (uint64, error)
	Min(index, field, view string, shard uint64) (uint64, bool, error)
	UnionInPlace(index, field, view string, shard uint64, others ...*roaring.Bitmap) error
	CountRange(index, field, view string, shard uint64, start, end uint64) (uint64, error)
	OffsetRange(index, field, view string, shard uint64, offset, start, end uint64) (*roaring.Bitmap, error)
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

// Rollback rolls back all underlying transactions.
func (mtx *MultiTx) Rollback() (err error) {
	for _, tx := range mtx.txs {
		if e := tx.Rollback(); e != nil && err == nil {
			err = e
		}
	}
	return err
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

func (mtx *MultiTx) Add(index, field, view string, shard uint64, a ...uint64) (changed bool, err error) {
	tx, err := mtx.tx(index, shard)
	if err != nil {
		return false, err
	}
	return tx.Add(index, field, view, shard, a...)
}

func (mtx *MultiTx) Remove(index, field, view string, shard uint64, a ...uint64) (changed bool, err error) {
	tx, err := mtx.tx(index, shard)
	if err != nil {
		return false, err
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

	// Lookup transaction from cache.
	tx := mtx.txs[multiTxKey{index, shard}]
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
	if tx, err = idx.Begin(mtx.writable, shard); err != nil {
		return nil, err
	}
	mtx.txs[multiTxKey{index, shard}] = tx

	return tx, nil
}

type multiTxKey struct {
	index string
	shard uint64
}

// RoaringTx represents a fake transaction object for Roaring storage.
type RoaringTx struct {
	Index    *Index
	Field    *Field
	fragment *fragment
}

// Rollback is a no-op as Roaring does not support transactions.
func (tx *RoaringTx) Rollback() error {
	return nil
}

// Commit is a no-op as Roaring does not support transactions.
func (tx *RoaringTx) Commit() error {
	return nil
}

func (tx *RoaringTx) RoaringBitmap(index, field, view string, shard uint64) (*roaring.Bitmap, error) {
	return tx.bitmap(field, view, shard)
}

func (tx *RoaringTx) Container(index, field, view string, shard uint64, key uint64) (*roaring.Container, error) {
	b, err := tx.bitmap(field, view, shard)
	if err != nil {
		return nil, err
	}
	return b.Containers.Get(key), nil
}

func (tx *RoaringTx) PutContainer(index, field, view string, shard uint64, key uint64, c *roaring.Container) error {
	b, err := tx.bitmap(field, view, shard)
	if err != nil {
		return err
	}
	b.Containers.Put(key, c)
	return nil
}

func (tx *RoaringTx) RemoveContainer(index, field, view string, shard uint64, key uint64) error {
	b, err := tx.bitmap(field, view, shard)
	if err != nil {
		return err
	}
	b.Containers.Remove(key)
	return nil
}

func (tx *RoaringTx) Add(index, field, view string, shard uint64, a ...uint64) (changed bool, err error) {
	b, err := tx.bitmap(field, view, shard)
	if err != nil {
		return false, err
	}
	return b.Add(a...)
}

func (tx *RoaringTx) Remove(index, field, view string, shard uint64, a ...uint64) (changed bool, err error) {
	b, err := tx.bitmap(field, view, shard)
	if err != nil {
		return false, err
	}
	return b.Remove(a...)
}

func (tx *RoaringTx) Contains(index, field, view string, shard uint64, v uint64) (exists bool, err error) {
	b, err := tx.bitmap(field, view, shard)
	if err != nil {
		return false, err
	}
	return b.Contains(v), nil
}

func (tx *RoaringTx) ContainerIterator(index, field, view string, shard uint64, key uint64) (citer roaring.ContainerIterator, found bool, err error) {
	b, err := tx.bitmap(field, view, shard)
	if err != nil {
		return nil, false, err
	}
	citer, found = b.Containers.Iterator(key)
	return citer, found, nil
}

func (tx *RoaringTx) ForEach(index, field, view string, shard uint64, fn func(i uint64) error) error {
	b, err := tx.bitmap(field, view, shard)
	if err != nil {
		return err
	}
	return b.ForEach(fn)
}

func (tx *RoaringTx) ForEachRange(index, field, view string, shard uint64, start, end uint64, fn func(uint64) error) error {
	b, err := tx.bitmap(field, view, shard)
	if err != nil {
		return err
	}
	return b.ForEachRange(start, end, fn)
}

func (tx *RoaringTx) Count(index, field, view string, shard uint64) (uint64, error) {
	b, err := tx.bitmap(field, view, shard)
	if err != nil {
		return 0, err
	}
	return b.Count(), nil
}

func (tx *RoaringTx) Max(index, field, view string, shard uint64) (uint64, error) {
	b, err := tx.bitmap(field, view, shard)
	if err != nil {
		return 0, err
	}
	return b.Max(), nil
}

func (tx *RoaringTx) Min(index, field, view string, shard uint64) (uint64, bool, error) {
	b, err := tx.bitmap(field, view, shard)
	if err != nil {
		return 0, false, err
	}
	v, ok := b.Min()
	return v, ok, nil
}

func (tx *RoaringTx) UnionInPlace(index, field, view string, shard uint64, others ...*roaring.Bitmap) error {
	b, err := tx.bitmap(field, view, shard)
	if err != nil {
		return err
	}
	b.UnionInPlace(others...)
	return nil
}

func (tx *RoaringTx) CountRange(index, field, view string, shard uint64, start, end uint64) (uint64, error) {
	b, err := tx.bitmap(field, view, shard)
	if err != nil {
		return 0, err
	}
	return b.CountRange(start, end), nil
}

func (tx *RoaringTx) OffsetRange(index, field, view string, shard uint64, offset, start, end uint64) (*roaring.Bitmap, error) {
	b, err := tx.bitmap(field, view, shard)
	if err != nil {
		return nil, err
	}
	return b.OffsetRange(offset, start, end), nil
}

func (tx *RoaringTx) bitmap(field, view string, shard uint64) (*roaring.Bitmap, error) {
	// If a fragment is attached, always use it.
	if tx.fragment != nil {
		return tx.fragment.storage, nil
	}

	// If a field is attached, start from there.
	// Otherwise look up the field from the index.
	f := tx.Field
	if f == nil {
		if f = tx.Index.Field(field); f == nil {
			return nil, ErrFieldNotFound
		}
	}

	v := f.view(view)
	if v == nil {
		return nil, errors.Errorf("view not found: %q", view)
	}

	frag := v.Fragment(shard)
	if frag == nil {
		panic(fmt.Sprintf("fragment not found: %q / %q / %d", field, view, shard))
	}
	return frag.storage, nil
}
