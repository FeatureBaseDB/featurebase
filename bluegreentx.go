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

	"github.com/pilosa/pilosa/v2/roaring"
)

// blueGreenTx runs two Tx together and notices differences in their output.
// By convention, the 'b' Tx is the output that is returned to caller.
type blueGreenTx struct {
	a Tx
	b Tx // b's output is returned

	idx *Index
}

func newBlueGreenTx(a, b Tx, idx *Index) *blueGreenTx {
	return &blueGreenTx{a: a, b: b, idx: idx}
}

var _ = newBlueGreenTx // keep linter happy

var _ Tx = (*blueGreenTx)(nil)

func (c *blueGreenTx) Readonly() bool {
	a := c.a.Readonly()
	b := c.b.Readonly()
	if a != b {
		panic(fmt.Sprintf("a=%v, but b =%v", a, b))
	}
	return b
}

func (c *blueGreenTx) NewTxIterator(index, field, view string, shard uint64) *roaring.Iterator {
	return c.b.NewTxIterator(index, field, view, shard)
}

func (c *blueGreenTx) Pointer() string {
	return fmt.Sprintf("%p", c)
}

func (c *blueGreenTx) IncrementOpN(index, field, view string, shard uint64, changedN int) {
	c.a.IncrementOpN(index, field, view, shard, changedN)
	c.b.IncrementOpN(index, field, view, shard, changedN)
}

func (c *blueGreenTx) Rollback() {
	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see Rollback() panic '%v' at '%v'", r, stack())
			panic(r)
		}
	}()
	c.a.Rollback()
	c.b.Rollback()
}

func (c *blueGreenTx) Commit() error {
	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see Commit() panic '%v' at '%v'", r, stack())
			panic(r)
		}
	}()
	errA := c.a.Commit()
	_ = errA
	errB := c.b.Commit()

	compareErrors(errA, errB)
	return errB
}

func (c *blueGreenTx) RoaringBitmap(index, field, view string, shard uint64) (*roaring.Bitmap, error) {
	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see RoaringBitmap() panic '%v' at '%v'", r, stack())
			panic(r)
		}
	}()
	a, errA := c.a.RoaringBitmap(index, field, view, shard)
	_, _ = a, errA
	b, errB := c.b.RoaringBitmap(index, field, view, shard)
	compareErrors(errA, errB)
	return b, errB
}

func (c *blueGreenTx) Container(index, field, view string, shard uint64, key uint64) (ct *roaring.Container, err error) {
	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see Container() panic '%v' at '%v'", r, stack())
			panic(r)
		}
	}()
	a, errA := c.a.Container(index, field, view, shard, key)
	b, errB := c.b.Container(index, field, view, shard, key)
	compareErrors(errA, errB)
	err = a.BitwiseCompare(b)
	panicOn(err)

	return b, errB
}

func (c *blueGreenTx) PutContainer(index, field, view string, shard uint64, key uint64, rc *roaring.Container) error {
	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see PutContainer() panic '%v' at '%v'", r, stack())
			panic(r)
		}
	}()
	errA := c.a.PutContainer(index, field, view, shard, key, rc)
	errB := c.b.PutContainer(index, field, view, shard, key, rc)
	compareErrors(errA, errB)

	/*  draft idea of how to check the full databases afterwards:
	hashA := c.a.RootHashString()
	hashB := c.b.RootHashString()
	if hashA != hashB {
		panic(fmt.Sprintf("hashA = '%v' but hashB = '%v'", hashA, hashB))
	}
	*/
	return errB
}

func (c *blueGreenTx) ImportRoaringBits(index, field, view string, shard uint64, rit roaring.RoaringIterator, clear bool, log bool, rowSize uint64) (changed int, rowSet map[uint64]int, err error) {
	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see ImportRoaringBits() panic '%v' at '%v'", r, stack())
			panic(r)
		}
	}()

	// remember where the iterator started, so we can replay it a second time.
	rit2 := rit.Clone()

	changedA, rowSetA, errA := c.a.ImportRoaringBits(index, field, view, shard, rit, clear, log, rowSize)

	changedB, rowSetB, errB := c.b.ImportRoaringBits(index, field, view, shard, rit2, clear, log, rowSize)

	if changedA != changedB {
		panic(fmt.Sprintf("changedA = %v, but changedB = %v", changedA, changedB))
	}
	if len(rowSetA) != len(rowSetB) {
		panic(fmt.Sprintf("rowSetA = %#v, but rowSetB = %#v", rowSetA, rowSetB))
	}
	for k, va := range rowSetA {
		vb, ok := rowSetB[k]
		if !ok {
			panic(fmt.Sprintf("diff on key '%v': present in rowSetA, but not in rowSet B. rowSetA = %#v, but rowSetB = %#v", k, rowSetA, rowSetB))
		}
		if va != vb {
			panic(fmt.Sprintf("diff on key '%v', rowSetA has value '%v', but rowSetB has value '%v'", k, va, vb))
		}
	}

	compareErrors(errA, errB)

	//compareDatabases(c.a, c.b)
	return changedB, rowSetB, errB
}

/* // TODO: get a database-wide checksum working
func compareDatabases(a, b Tx) {

	index, field, view, shard := "i", "f", "v", uint64(0)

	ha, errA := a.WholeDatabaseBlake3Hash(index, field, view, shard)
	panicOn(errA)
	hb, errB := b.WholeDatabaseBlake3Hash(index, field, view, shard)
	panicOn(errB)

	if ha != hb {
		panic(fmt.Sprintf("a.WholeDatabaseBlake3Hash(%T) = '%v' but b.WholeDatabaseBlake3Hash(%T) = '%v'", a, ha, b, hb))
	}
}
*/

func (c *blueGreenTx) RemoveContainer(index, field, view string, shard uint64, key uint64) error {
	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see RemoveContainer() panic '%v' at '%v'", r, stack())
			panic(r)
		}
	}()
	errA := c.a.RemoveContainer(index, field, view, shard, key)
	errB := c.b.RemoveContainer(index, field, view, shard, key)
	compareErrors(errA, errB)
	return errB
}

func (c *blueGreenTx) UseRowCache() bool {
	return c.b.UseRowCache()
}

func (c *blueGreenTx) Add(index, field, view string, shard uint64, batched bool, a ...uint64) (changeCount int, err error) {
	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see Add() panic '%v' for index='%v', field='%v', view='%v', shard='%v' at '%v'", r, index, field, view, shard, stack())
			panic(r)
		}
	}()

	// must copy a before calling Add(), since RoaringTx.Add() uses roaring.DirectAddN()
	// which modifies the input array a.
	a2 := make([]uint64, len(a))
	copy(a2, a)

	ach, errA := c.a.Add(index, field, view, shard, batched, a...)
	_, _ = ach, errA

	bch, errB := c.b.Add(index, field, view, shard, batched, a2...)

	if ach != bch {
		panic(fmt.Sprintf("Add() difference, ach=%v, but bch=%v; errA='%v'; errB='%v'", ach, bch, errA, errB))
	}
	compareErrors(errA, errB)
	return bch, errB
}

func compareErrors(errA, errB error) {
	switch {
	case errA == nil && errB == nil:
		// OK
	case errA == nil:
		panic(fmt.Sprintf("errA is nil, but errB = %#v", errB))
	case errB == nil:
		panic(fmt.Sprintf("errB is nil, but errA = %#v", errA))
	default:
		ae := errA.Error()
		be := errB.Error()
		if ae != be {
			panic(fmt.Sprintf("errA is '%v', but errB is '%v'", ae, be))
		}
	}
}

func (c *blueGreenTx) Remove(index, field, view string, shard uint64, a ...uint64) (changeCount int, err error) {
	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see Remove() panic '%v' at '%v'", r, stack())
			panic(r)
		}
	}()
	ach, errA := c.a.Remove(index, field, view, shard, a...)
	_, _ = ach, errA
	bch, errB := c.b.Remove(index, field, view, shard, a...)
	compareErrors(errA, errB)
	return bch, errB
}

func (c *blueGreenTx) Contains(index, field, view string, shard uint64, key uint64) (exists bool, err error) {
	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see Contains() panic '%v' at '%v'", r, stack())
			panic(r)
		}
	}()
	ax, errA := c.a.Contains(index, field, view, shard, key)
	_, _ = ax, errA
	bx, errB := c.b.Contains(index, field, view, shard, key)

	compareErrors(errA, errB)
	return bx, errB
}

func (c *blueGreenTx) ContainerIterator(index, field, view string, shard uint64, firstRoaringContainerKey uint64) (citer roaring.ContainerIterator, found bool, err error) {
	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see ContainerIterator() panic '%v' at '%v'", r, stack())
			panic(r)
		}
	}()
	// TODO: need to return a blueGreenIterator too, that does close/next operations on both A and B.
	ait, afound, errA := c.a.ContainerIterator(index, field, view, shard, firstRoaringContainerKey)
	_, _, _ = ait, afound, errA
	bit, bfound, errB := c.b.ContainerIterator(index, field, view, shard, firstRoaringContainerKey)

	compareErrors(errA, errB)
	return bit, bfound, errB
}

func (c *blueGreenTx) ForEach(index, field, view string, shard uint64, fn func(i uint64) error) error {
	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see ForEach() panic '%v' at '%v'", r, stack())
			panic(r)
		}
	}()
	errA := c.a.ForEach(index, field, view, shard, fn)
	_ = errA
	errB := c.b.ForEach(index, field, view, shard, fn)
	_ = errB

	compareErrors(errA, errB)
	return errB
}

func (c *blueGreenTx) ForEachRange(index, field, view string, shard uint64, start, end uint64, fn func(uint64) error) error {
	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see ForEachRange() panic '%v' at '%v'", r, stack())
			panic(r)
		}
	}()
	errA := c.a.ForEachRange(index, field, view, shard, start, end, fn)
	_ = errA
	errB := c.b.ForEachRange(index, field, view, shard, start, end, fn)
	_ = errB

	compareErrors(errA, errB)
	return errB
}

func (c *blueGreenTx) Count(index, field, view string, shard uint64) (uint64, error) {
	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see Count() panic '%v' at '%v'", r, stack())
			panic(r)
		}
	}()
	a, errA := c.a.Count(index, field, view, shard)
	_, _ = a, errA
	b, errB := c.b.Count(index, field, view, shard)
	_, _ = b, errB

	compareErrors(errA, errB)
	return b, errB
}

func (c *blueGreenTx) Max(index, field, view string, shard uint64) (uint64, error) {
	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see Max() panic '%v' at '%v'", r, stack())
			panic(r)
		}
	}()
	a, errA := c.a.Max(index, field, view, shard)
	_, _ = a, errA
	b, errB := c.b.Max(index, field, view, shard)
	_, _ = b, errB

	compareErrors(errA, errB)
	return b, errB
}

func (c *blueGreenTx) Min(index, field, view string, shard uint64) (uint64, bool, error) {
	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see Min() panic '%v' at '%v'", r, stack())
			panic(r)
		}
	}()
	amin, afound, errA := c.a.Min(index, field, view, shard)
	_, _, _ = amin, afound, errA
	bmin, bfound, errB := c.b.Min(index, field, view, shard)
	_, _, _ = bmin, bfound, errB

	compareErrors(errA, errB)
	return bmin, bfound, errB
}

func (c *blueGreenTx) UnionInPlace(index, field, view string, shard uint64, others ...*roaring.Bitmap) error {
	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see UnionInPlace() panic '%v' at '%v'", r, stack())
			panic(r)
		}
	}()
	errA := c.a.UnionInPlace(index, field, view, shard, others...)
	errB := c.b.UnionInPlace(index, field, view, shard, others...)
	compareErrors(errA, errB)
	return errB
}

func (c *blueGreenTx) CountRange(index, field, view string, shard uint64, start, end uint64) (n uint64, err error) {
	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see CountRange() panic '%v' at '%v'", r, stack())
			panic(r)
		}
	}()
	a, errA := c.a.CountRange(index, field, view, shard, start, end)
	b, errB := c.b.CountRange(index, field, view, shard, start, end)

	if a != b {
		panic(fmt.Sprintf("a = %v, but b = %v", a, b))
	}

	compareErrors(errA, errB)
	return b, errB
}

func (c *blueGreenTx) OffsetRange(index, field, view string, shard, offset, start, end uint64) (other *roaring.Bitmap, err error) {
	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see OffsetRange() panic '%v' at '%v'", r, stack())
			panic(r)
		}
	}()
	a, errA := c.a.OffsetRange(index, field, view, shard, offset, start, end)
	b, errB := c.b.OffsetRange(index, field, view, shard, offset, start, end)

	err = roaringBitmapDiff(a, b)
	panicOn(err)
	compareErrors(errA, errB)
	return b, errB
}
