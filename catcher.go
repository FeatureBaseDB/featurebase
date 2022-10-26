// Copyright 2021 Molecula Corp. All rights reserved.
package pilosa

import (
	"github.com/molecula/featurebase/v3/roaring"
	txkey "github.com/molecula/featurebase/v3/short_txkey"
	"github.com/molecula/featurebase/v3/vprint"
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

func (c *catcherTx) ImportRoaringBits(index, field, view string, shard uint64, rit roaring.RoaringIterator, clear bool, log bool, rowSize uint64) (changed int, rowSet map[uint64]int, err error) {
	defer func() {
		if r := recover(); r != nil {
			vprint.AlwaysPrintf("see ImportRoaringBits() PanicOn '%v' at '%v'", r, vprint.Stack())
			vprint.PanicOn(r)
		}
	}()
	return c.b.ImportRoaringBits(index, field, view, shard, rit, clear, log, rowSize)
}

func (c *catcherTx) Rollback() {
	defer func() {
		if r := recover(); r != nil {
			vprint.AlwaysPrintf("see Rollback() PanicOn '%v' at '%v'", r, vprint.Stack())
			vprint.PanicOn(r)
		}
	}()
	c.b.Rollback()
}

func (c *catcherTx) Commit() error {

	defer func() {
		if r := recover(); r != nil {
			vprint.AlwaysPrintf("see Commit() PanicOn '%v' at '%v'", r, vprint.Stack())
			vprint.PanicOn(r)
		}
	}()
	return c.b.Commit()
}

func (c *catcherTx) RoaringBitmap(index, field, view string, shard uint64) (*roaring.Bitmap, error) {

	defer func() {
		if r := recover(); r != nil {
			vprint.AlwaysPrintf("see RoaringBitmap() PanicOn '%v' at '%v'", r, vprint.Stack())
			vprint.PanicOn(r)
		}
	}()
	return c.b.RoaringBitmap(index, field, view, shard)
}

func (c *catcherTx) Container(index, field, view string, shard uint64, key uint64) (ct *roaring.Container, err error) {

	defer func() {
		if r := recover(); r != nil {
			vprint.AlwaysPrintf("see Container() PanicOn '%v' at '%v'", r, vprint.Stack())
			vprint.PanicOn(r)
		}
	}()
	return c.b.Container(index, field, view, shard, key)
}

func (c *catcherTx) PutContainer(index, field, view string, shard uint64, key uint64, rc *roaring.Container) error {

	defer func() {
		if r := recover(); r != nil {
			vprint.AlwaysPrintf("see PutContainer() PanicOn '%v' at '%v'", r, vprint.Stack())
			vprint.PanicOn(r)
		}
	}()
	return c.b.PutContainer(index, field, view, shard, key, rc)
}

func (c *catcherTx) RemoveContainer(index, field, view string, shard uint64, key uint64) error {

	defer func() {
		if r := recover(); r != nil {
			vprint.AlwaysPrintf("see RemoveContainer() PanicOn '%v' at '%v'", r, vprint.Stack())
			vprint.PanicOn(r)
		}
	}()
	return c.b.RemoveContainer(index, field, view, shard, key)
}

func (c *catcherTx) Add(index, field, view string, shard uint64, a ...uint64) (changeCount int, err error) {

	defer func() {
		if r := recover(); r != nil {
			vprint.AlwaysPrintf("see Add() PanicOn '%v' at '%v'", r, vprint.Stack())
			vprint.PanicOn(r)
		}
	}()
	return c.b.Add(index, field, view, shard, a...)
}

func (c *catcherTx) Remove(index, field, view string, shard uint64, a ...uint64) (changeCount int, err error) {

	defer func() {
		if r := recover(); r != nil {
			vprint.AlwaysPrintf("see Remove() PanicOn '%v' at '%v'", r, vprint.Stack())
			vprint.PanicOn(r)
		}
	}()
	return c.b.Remove(index, field, view, shard, a...)
}

func (c *catcherTx) Contains(index, field, view string, shard uint64, key uint64) (exists bool, err error) {

	defer func() {
		if r := recover(); r != nil {
			vprint.AlwaysPrintf("see Contains() PanicOn '%v' at '%v'", r, vprint.Stack())
			vprint.PanicOn(r)
		}
	}()
	return c.b.Contains(index, field, view, shard, key)
}

func (c *catcherTx) ContainerIterator(index, field, view string, shard uint64, firstRoaringContainerKey uint64) (citer roaring.ContainerIterator, found bool, err error) {

	defer func() {
		if r := recover(); r != nil {
			vprint.AlwaysPrintf("see ContainerIterator() PanicOn '%v' at '%v'", r, vprint.Stack())
			vprint.PanicOn(r)
		}
	}()
	return c.b.ContainerIterator(index, field, view, shard, firstRoaringContainerKey)
}

func (c *catcherTx) Count(index, field, view string, shard uint64) (uint64, error) {

	defer func() {
		if r := recover(); r != nil {
			vprint.AlwaysPrintf("see Count() PanicOn '%v' at '%v'", r, vprint.Stack())
			vprint.PanicOn(r)
		}
	}()
	return c.b.Count(index, field, view, shard)
}

func (c *catcherTx) Max(index, field, view string, shard uint64) (uint64, error) {

	defer func() {
		if r := recover(); r != nil {
			vprint.AlwaysPrintf("see Max() PanicOn '%v' at '%v'", r, vprint.Stack())
			vprint.PanicOn(r)
		}
	}()
	return c.b.Max(index, field, view, shard)
}

func (c *catcherTx) Min(index, field, view string, shard uint64) (uint64, bool, error) {

	defer func() {
		if r := recover(); r != nil {
			vprint.AlwaysPrintf("see Min() PanicOn '%v' at '%v'", r, vprint.Stack())
			vprint.PanicOn(r)
		}
	}()
	return c.b.Min(index, field, view, shard)
}

func (c *catcherTx) CountRange(index, field, view string, shard uint64, start, end uint64) (n uint64, err error) {

	defer func() {
		if r := recover(); r != nil {
			vprint.AlwaysPrintf("see CountRange() PanicOn '%v' at '%v'", r, vprint.Stack())
			vprint.PanicOn(r)
		}
	}()
	return c.b.CountRange(index, field, view, shard, start, end)
}

func (c *catcherTx) OffsetRange(index, field, view string, shard, offset, start, end uint64) (other *roaring.Bitmap, err error) {

	defer func() {
		if r := recover(); r != nil {
			vprint.AlwaysPrintf("see OffsetRange() PanicOn '%v' at '%v'", r, vprint.Stack())
			vprint.PanicOn(r)
		}
	}()
	return c.b.OffsetRange(index, field, view, shard, offset, start, end)
}

func (c *catcherTx) Type() string {
	return c.b.Type()
}

func (c *catcherTx) ApplyFilter(index, field, view string, shard uint64, ckey uint64, filter roaring.BitmapFilter) (err error) {
	return GenericApplyFilter(c, index, field, view, shard, ckey, filter)
}

func (c *catcherTx) ApplyRewriter(index, field, view string, shard uint64, ckey uint64, filter roaring.BitmapRewriter) (err error) {
	return c.b.ApplyRewriter(index, field, view, shard, ckey, filter)
}

func (c *catcherTx) GetSortedFieldViewList(idx *Index, shard uint64) (fvs []txkey.FieldView, err error) {
	return c.b.GetSortedFieldViewList(idx, shard)
}

func (tx *catcherTx) GetFieldSizeBytes(index, field string) (uint64, error) {
	return 0, nil
}
