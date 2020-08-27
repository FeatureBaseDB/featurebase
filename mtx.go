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
	"io"
	"sync"

	"github.com/pilosa/pilosa/v2/roaring"
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

type multiTxKey struct {
	index string
	shard uint64
	write bool
}

func (mtx *MultiTx) Type() string {
	return mtx.index.Txf.TxType()
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
			return nil, newNotFoundError(ErrIndexNotFound, index)
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
	panic(fmt.Sprintf("txNoShard: no prior tx in MultiTx available, looking up index='%v'", index))
}
