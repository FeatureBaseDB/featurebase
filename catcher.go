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

	"github.com/molecula/featurebase/v2/roaring"
	txkey "github.com/molecula/featurebase/v2/short_txkey"
	. "github.com/molecula/featurebase/v2/vprint"
)

// catcher is useful to report error locations with a
// Stack dump before the complexity
// of the executor_test swallows up
// the location of a PanicOn.
type catcherTx struct {
	b Tx
}

func newCatcherTx(b Tx) *catcherTx {
	return &catcherTx{b: b}
}

func init() {
	// keep golangci-lint happy
	_ = newCatcherTx
}

var _ Tx = (*catcherTx)(nil)

func (c *catcherTx) IncrementOpN(index, field, view string, shard uint64, changedN int) {
	c.b.IncrementOpN(index, field, view, shard, changedN)
}

func (c *catcherTx) NewTxIterator(index, field, view string, shard uint64) *roaring.Iterator {
	return c.b.NewTxIterator(index, field, view, shard)
}

func (c *catcherTx) ImportRoaringBits(index, field, view string, shard uint64, rit roaring.RoaringIterator, clear bool, log bool, rowSize uint64, data []byte) (changed int, rowSet map[uint64]int, err error) {
	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see ImportRoaringBits() PanicOn '%v' at '%v'", r, Stack())
			PanicOn(r)
		}
	}()
	return c.b.ImportRoaringBits(index, field, view, shard, rit, clear, log, rowSize, data)
}

func (c *catcherTx) Dump(short bool, shard uint64) {
	c.b.Dump(short, shard)
}

func (c *catcherTx) Readonly() bool {
	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see Readonly() PanicOn '%v' at '%v'", r, Stack())
			PanicOn(r)
		}
	}()
	return c.b.Readonly()
}

func (tx *catcherTx) Pointer() string {
	return fmt.Sprintf("%p", tx)
}

func (c *catcherTx) Rollback() {
	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see Rollback() PanicOn '%v' at '%v'", r, Stack())
			PanicOn(r)
		}
	}()
	c.b.Rollback()
}

func (c *catcherTx) Commit() error {

	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see Commit() PanicOn '%v' at '%v'", r, Stack())
			PanicOn(r)
		}
	}()
	return c.b.Commit()
}

func (c *catcherTx) RoaringBitmap(index, field, view string, shard uint64) (*roaring.Bitmap, error) {

	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see RoaringBitmap() PanicOn '%v' at '%v'", r, Stack())
			PanicOn(r)
		}
	}()
	return c.b.RoaringBitmap(index, field, view, shard)
}

func (c *catcherTx) Container(index, field, view string, shard uint64, key uint64) (ct *roaring.Container, err error) {

	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see Container() PanicOn '%v' at '%v'", r, Stack())
			PanicOn(r)
		}
	}()
	return c.b.Container(index, field, view, shard, key)
}

func (c *catcherTx) PutContainer(index, field, view string, shard uint64, key uint64, rc *roaring.Container) error {

	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see PutContainer() PanicOn '%v' at '%v'", r, Stack())
			PanicOn(r)
		}
	}()
	return c.b.PutContainer(index, field, view, shard, key, rc)
}

func (c *catcherTx) RemoveContainer(index, field, view string, shard uint64, key uint64) error {

	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see RemoveContainer() PanicOn '%v' at '%v'", r, Stack())
			PanicOn(r)
		}
	}()
	return c.b.RemoveContainer(index, field, view, shard, key)
}

func (c *catcherTx) UseRowCache() bool {
	return c.b.UseRowCache()
}

func (c *catcherTx) IsDone() bool {
	return c.b.IsDone()
}

func (c *catcherTx) Add(index, field, view string, shard uint64, a ...uint64) (changeCount int, err error) {

	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see Add() PanicOn '%v' at '%v'", r, Stack())
			PanicOn(r)
		}
	}()
	return c.b.Add(index, field, view, shard, a...)
}

func (c *catcherTx) Remove(index, field, view string, shard uint64, a ...uint64) (changeCount int, err error) {

	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see Remove() PanicOn '%v' at '%v'", r, Stack())
			PanicOn(r)
		}
	}()
	return c.b.Remove(index, field, view, shard, a...)
}

func (c *catcherTx) Contains(index, field, view string, shard uint64, key uint64) (exists bool, err error) {

	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see Contains() PanicOn '%v' at '%v'", r, Stack())
			PanicOn(r)
		}
	}()
	return c.b.Contains(index, field, view, shard, key)
}

func (c *catcherTx) ContainerIterator(index, field, view string, shard uint64, firstRoaringContainerKey uint64) (citer roaring.ContainerIterator, found bool, err error) {

	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see ContainerIterator() PanicOn '%v' at '%v'", r, Stack())
			PanicOn(r)
		}
	}()
	return c.b.ContainerIterator(index, field, view, shard, firstRoaringContainerKey)
}

func (c *catcherTx) ForEach(index, field, view string, shard uint64, fn func(i uint64) error) error {

	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see ForEach() PanicOn '%v' at '%v'", r, Stack())
			PanicOn(r)
		}
	}()
	return c.b.ForEach(index, field, view, shard, fn)
}

func (c *catcherTx) ForEachRange(index, field, view string, shard uint64, start, end uint64, fn func(uint64) error) error {

	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see ForEachRange() PanicOn '%v' at '%v'", r, Stack())
			PanicOn(r)
		}
	}()
	return c.b.ForEachRange(index, field, view, shard, start, end, fn)
}

func (c *catcherTx) Count(index, field, view string, shard uint64) (uint64, error) {

	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see Count() PanicOn '%v' at '%v'", r, Stack())
			PanicOn(r)
		}
	}()
	return c.b.Count(index, field, view, shard)
}

func (c *catcherTx) Max(index, field, view string, shard uint64) (uint64, error) {

	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see Max() PanicOn '%v' at '%v'", r, Stack())
			PanicOn(r)
		}
	}()
	return c.b.Max(index, field, view, shard)
}

func (c *catcherTx) Min(index, field, view string, shard uint64) (uint64, bool, error) {

	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see Min() PanicOn '%v' at '%v'", r, Stack())
			PanicOn(r)
		}
	}()
	return c.b.Min(index, field, view, shard)
}

func (c *catcherTx) UnionInPlace(index, field, view string, shard uint64, others ...*roaring.Bitmap) error {

	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see UnionInPlace() PanicOn '%v' at '%v'", r, Stack())
			PanicOn(r)
		}
	}()
	return c.b.UnionInPlace(index, field, view, shard, others...)
}

func (c *catcherTx) CountRange(index, field, view string, shard uint64, start, end uint64) (n uint64, err error) {

	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see CountRange() PanicOn '%v' at '%v'", r, Stack())
			PanicOn(r)
		}
	}()
	return c.b.CountRange(index, field, view, shard, start, end)
}

func (c *catcherTx) OffsetRange(index, field, view string, shard, offset, start, end uint64) (other *roaring.Bitmap, err error) {

	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see OffsetRange() PanicOn '%v' at '%v'", r, Stack())
			PanicOn(r)
		}
	}()
	return c.b.OffsetRange(index, field, view, shard, offset, start, end)
}

func (c *catcherTx) RoaringBitmapReader(index, field, view string, shard uint64, fragmentPathForRoaring string) (r io.ReadCloser, sz int64, err error) {
	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see RoaringBitmapReader() PanicOn '%v' at '%v'", r, Stack())
			PanicOn(r)
		}
	}()
	return c.b.RoaringBitmapReader(index, field, view, shard, fragmentPathForRoaring)
}

func (c *catcherTx) Type() string {
	return c.b.Type()
}

func (c *catcherTx) Group() *TxGroup {
	return c.b.Group()
}

func (c *catcherTx) Options() Txo {
	return c.b.Options()
}

// Sn retreives the serial number of the Tx.
func (c *catcherTx) Sn() int64 {
	return c.b.Sn()
}

func (c *catcherTx) ApplyFilter(index, field, view string, shard uint64, ckey uint64, filter roaring.BitmapFilter) (err error) {
	return GenericApplyFilter(c, index, field, view, shard, ckey, filter)
}

func (c *catcherTx) GetSortedFieldViewList(idx *Index, shard uint64) (fvs []txkey.FieldView, err error) {
	return c.b.GetSortedFieldViewList(idx, shard)
}

func (tx *catcherTx) GetFieldSizeBytes(index, field string) (uint64, error) {
	return 0, nil
}
