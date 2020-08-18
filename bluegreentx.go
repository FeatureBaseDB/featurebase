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
	"reflect"
	"sort"
	"sync"

	"github.com/pilosa/pilosa/v2/roaring"
)

// blueGreenTx runs two Tx together and notices differences in their output.
// By convention, the 'b' Tx is the output that is returned to caller.
//
// Warning: DATA RACES are expected if RoaringTx is one side of the Tx pair.
// The checkDatabase() call will do reads of the fragments at Commit/Rollback,
// while the snapshotqueue may be doing writes.
//
// Do not run with go test -race and expect it to be race free.
//
type blueGreenTx struct {
	a Tx
	b Tx // b's output is returned

	as string
	bs string

	idx *Index

	checker              blueGreenChecker
	mu                   sync.Mutex
	rollbackOrCommitDone bool
}

func newBlueGreenTx(a, b Tx, idx *Index) *blueGreenTx {
	as := a.Type()
	bs := b.Type()
	c := &blueGreenTx{a: a, b: b, idx: idx, as: as, bs: bs}
	c.checker.c = c
	return c
}

var _ = newBlueGreenTx // keep linter happy

var _ Tx = (*blueGreenTx)(nil)

func (c *blueGreenTx) Type() string {
	return c.a.Type() + "_" + c.b.Type()
}

var blueGreenTxDumpMut sync.Mutex

func (c *blueGreenTx) Dump() {
	blueGreenTxDumpMut.Lock()
	defer blueGreenTxDumpMut.Unlock()
	fmt.Printf("%v blueGreenTx.Dump ============== \n", FileLine(2))
	fmt.Printf("A(%v) Dump:\n", c.as)
	c.a.Dump()
	fmt.Printf("B(%v) Dump:\n", c.bs)
	c.b.Dump()
}

func (c *blueGreenTx) Readonly() bool {
	a := c.a.Readonly()
	b := c.b.Readonly()
	if a != b {
		panic(fmt.Sprintf("a=%v, but b =%v", a, b))
	}
	return b
}

func (c *blueGreenTx) NewTxIterator(index, field, view string, shard uint64) *roaring.Iterator {
	c.checker.see(index, field, view, shard)
	// can't really do simultaneous iteration on A and B, so punt and
	// just give back B.
	return c.b.NewTxIterator(index, field, view, shard)
}

func (c *blueGreenTx) Pointer() string {
	return fmt.Sprintf("%p", c)
}

func (c *blueGreenTx) IncrementOpN(index, field, view string, shard uint64, changedN int) {
	c.checker.see(index, field, view, shard)
	c.a.IncrementOpN(index, field, view, shard, changedN)
	c.b.IncrementOpN(index, field, view, shard, changedN)
}

// compareTxState is called for the first Commit or Rollback a blueGreenTx sees.
func (c *blueGreenTx) compareTxState(index, field, view string, shard uint64) {
	here := fmt.Sprintf("%v/%v/%v/%v", index, field, view, shard)
	aIter, aFound, aErr := c.a.ContainerIterator(index, field, view, shard, 0)
	bIter, bFound, bErr := c.b.ContainerIterator(index, field, view, shard, 0)
	if aErr == nil || aIter != nil {
		defer aIter.Close()
	}
	if bErr == nil || bIter != nil {
		defer bIter.Close()
	}

	if aFound != bFound {
		c.Dump()
		panic(fmt.Sprintf("compareTxState[%v]: A(%v) ContainerIterator had aFound=%v, but B(%v) had bFound=%v; at '%v'", here, c.as, aFound, c.bs, bFound, stack()))
	}

	if aErr != nil || bErr != nil {
		if aErr != nil && bErr != nil {
			c.Dump()
			panic(fmt.Sprintf("compareTxState[%v]: A(%v) reported err '%v'; B(%v) reported err '%v' at %v", here, c.as, aErr, c.bs, bErr, stack()))
		}
		if aErr != nil {
			c.Dump()
			panic(fmt.Sprintf("compareTxState[%v]: A(%v) reported err %v at %v; but B(%v) did not", here, c.as, aErr, c.bs, stack()))
		}
		if bErr != nil {
			c.Dump()
			panic(fmt.Sprintf("compareTxState[%v]: B(%v) reported err %v at %v; but A(%v) did not", here, c.bs, bErr, c.as, stack()))
		}
	}
	for aIter.Next() {
		aKey, aValue := aIter.Value()

		if !bIter.Next() {
			c.Dump()
			panic(fmt.Sprintf("compareTxState[%v]: A(%v) found key %v, B(%v) didn't, at %v", here, c.as, aKey, c.bs, stack()))
		}
		bKey, bValue := bIter.Value()
		if bKey != aKey {
			//vv("6960 really ought to be present index='%v',field='%v';view='%v';shard='%v'; isIn=%v", index, field, view, shard, c.isIn(index, field, view, shard, 456130566))
			AlwaysPrintf("problem in caller %v", Caller(2))
			c.Dump()
			panic(fmt.Sprintf("compareTxState[%v]: A(%v) found key %v, B(%v) found %v, at %v", here, c.as, aKey, c.bs, bKey, stack()))
		}
		if err := aValue.BitwiseCompare(bValue); err != nil {
			c.Dump()
			//vv("compareTxState[%v]: key %v differs: %v;  A=%v; B=%v; at stack=%v", here, aKey, err, c.as, c.bs, stack())
			panic(fmt.Sprintf("compareTxState[%v]: key %v differs: %v;  A=%v; B=%v; at stack=%v", here, aKey, err, c.as, c.bs, stack()))
		}
	}
	// end checking everything in A, but does B have more?
	if bIter.Next() {
		AlwaysPrintf("bIter has more than it should. problem in caller %v", Caller(2))
		c.Dump()
		bKey, _ := bIter.Value()
		//vv("compareTxState[%v]: B(%v) found key %v, A(%v) didn't, at %v", here, c.bs, bKey, c.as, stack())
		panic(fmt.Sprintf("compareTxState[%v]: B(%v) found key %v, A(%v) didn't, at %v", here, c.bs, bKey, c.as, stack()))
	}
}

func (c *blueGreenTx) checkDatabase() {
	c.checker.mu.Lock()
	defer c.checker.mu.Unlock()

	// seen() returns nil on 2nd or any further call,
	// so only the first Commit() or Rollback() does this.
	for index, fields := range c.checker.seen() {
		for field, views := range fields {
			for view, shards := range views {
				for shard := range shards {
					c.compareTxState(index, field, view, shard)
				}
			}
		}
	}
}

func (c *blueGreenTx) Rollback() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.rollbackOrCommitDone {
		return // avoid using discarded tx for Dump, which will panic.
	}
	c.rollbackOrCommitDone = true

	c.checkDatabase()
	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see Rollback() panic '%v' at '%v'", r, stack())
			panic(r)
		}
	}()
	//vv("blueGreenTx.Rollback() about to call (%v) a.Rollback()", c.as)
	c.a.Rollback()
	//vv("blueGreenTx.Rollback() about to call (%v) b.Rollback()", c.bs)
	c.b.Rollback()
	//vv("blueGreenTx.Rollback() done. bgtx p=%p", c)
}

func (c *blueGreenTx) Commit() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// for rbf 6930 debug stuff:
	//in := c.isIn("i", "x", "standard", 0, 456130566)
	//fmt.Printf("blueGreenTx.Commit() called. bgtx p=%p; in rbf=%v\n", c, in[0])
	if c.rollbackOrCommitDone {
		return nil
	}
	//vv("blueGreenTx.Commit() called. bgtx p=%p", c)
	c.rollbackOrCommitDone = true
	c.checkDatabase()
	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see Commit() panic '%v' at '%v'", r, stack())
			panic(r)
		}
	}()
	errA := c.a.Commit()
	_ = errA
	errB := c.b.Commit()

	/*
		tx2, err := c.idx.Txf.rbfDB.NewRBFTx(false, "", nil)
		panicOn(err)
		inRbf, err := tx2.Contains("i", "x", "standard", 0, 456130566)
		panicOn(err)
		fmt.Printf("AFTER commits happened, blueGreenTx.Commit() called. bgtx p=%p; in rbf=%v\n", c, inRbf)
	*/
	compareErrors(errA, errB)
	return errB
}

func (c *blueGreenTx) RoaringBitmap(index, field, view string, shard uint64) (*roaring.Bitmap, error) {
	c.checker.see(index, field, view, shard)
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

	slcA := a.Slice()
	slcB := b.Slice()
	if !reflect.DeepEqual(slcA, slcB) {
		panic("blueGreenTx.RoaringBitmap() returning different roaring.Bitmaps!")
	}

	return b, errB
}

func (c *blueGreenTx) Container(index, field, view string, shard uint64, key uint64) (ct *roaring.Container, err error) {
	c.checker.see(index, field, view, shard)
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
	c.checker.see(index, field, view, shard)
	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see PutContainer() panic '%v' at '%v'", r, stack())
			panic(r)
		}
	}()
	errA := c.a.PutContainer(index, field, view, shard, key, rc)
	errB := c.b.PutContainer(index, field, view, shard, key, rc)
	compareErrors(errA, errB)

	return errB
}

func (c *blueGreenTx) ImportRoaringBits(index, field, view string, shard uint64, rit roaring.RoaringIterator, clear bool, log bool, rowSize uint64, data []byte) (changed int, rowSet map[uint64]int, err error) {
	c.checker.see(index, field, view, shard)

	// these are the first port of call for debugging, so we leave them in.
	// ================== begin save comments.
	//c.checkDatabase()
	////vv("got past database check at TOP of ImportRoaringBits")
	//c.Dump()
	////vv("done with top dump; clear=%v", clear)
	// ==================   end save comments.
	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see ImportRoaringBits() panic '%v' at '%v'", r, stack())
			panic(r)
		}
	}()

	// remember where the iterator started, so we can replay it a second time.
	rit2 := rit.Clone()
	panicOn(err)

	changedA, rowSetA, errA := c.a.ImportRoaringBits(index, field, view, shard, rit, clear, log, rowSize, data)
	changedB, rowSetB, errB := c.b.ImportRoaringBits(index, field, view, shard, rit2, clear, log, rowSize, data)

	if len(data) == 0 {
		// okay to check! otherwise we are in the fragment.fillFragmentFromArchive
		// case where we know that RoaringTx.ImportRoaringBits changed and rowSet will
		// be inaccurate.
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
	}
	compareErrors(errA, errB)
	c.checkDatabase()
	return changedB, rowSetB, errB
}

func (c *blueGreenTx) RemoveContainer(index, field, view string, shard uint64, key uint64) error {
	c.checker.see(index, field, view, shard)
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
	// avoid cross-talk between our two implementations
	// by never allowing either to use the row cache.
	return false
}

var _ = (&blueGreenTx{}).isIn // happy linter

func (c *blueGreenTx) isIn(index, field, view string, shard uint64, ckey uint64) (r []bool) {
	r = make([]bool, 2)
	inA, errA := c.a.Contains(index, field, view, shard, ckey)
	panicOn(errA)
	inB, errB := c.b.Contains(index, field, view, shard, ckey)
	panicOn(errB)
	r[0] = inA
	r[1] = inB
	return
}

func (c *blueGreenTx) Add(index, field, view string, shard uint64, batched bool, a ...uint64) (changeCount int, err error) {
	c.checker.see(index, field, view, shard)
	//vv("blueGreenTx) Add(index=%v, field=%v, view=%v, shard=%v", index, field, view, shard)
	defer func() {
		// rbf 6960 debug code:
		/*
			in := c.isIn("i", "x", "standard", 0, 456130566)
			if in[0] || in[1] {
				vv("first time 6960 present isIn=%v; bgtx p=%p stack=\n%v", in, c, stack())
			}
		*/
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
	c.checker.see(index, field, view, shard)
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
	c.checker.see(index, field, view, shard)
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
	c.checker.see(index, field, view, shard)
	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see ContainerIterator() panic '%v' at '%v'", r, stack())
			panic(r)
		}
	}()

	ait, afound, errA := c.a.ContainerIterator(index, field, view, shard, firstRoaringContainerKey)
	_, _, _ = ait, afound, errA

	bit, bfound, errB := c.b.ContainerIterator(index, field, view, shard, firstRoaringContainerKey)

	compareErrors(errA, errB)
	// INVAR: errA == errB, so only need to check one.
	if errB != nil {
		// RoaringTx can return an iterator and an error, so be sure Close it we have it.
		if ait != nil {
			ait.Close()
		}
		if bit != nil {
			bit.Close()
		}
		return nil, bfound, errB
	}
	// INVAR: errA == errB == nil
	bgi := NewBlueGreenIterator(c, ait, bit)
	return bgi, bfound, errB
}

func NewBlueGreenIterator(tx *blueGreenTx, ait, bit roaring.ContainerIterator) *blueGreenIterator {
	return &blueGreenIterator{
		tx:  tx,
		as:  tx.as,
		bs:  tx.bs,
		ait: ait,
		bit: bit,
	}
}

type blueGreenIterator struct {
	tx *blueGreenTx
	as string
	bs string

	ait roaring.ContainerIterator
	bit roaring.ContainerIterator
}

func (bgi *blueGreenIterator) Next() bool {
	na := bgi.ait.Next()
	nb := bgi.bit.Next()
	if na != nb {
		panic(fmt.Sprintf("na=%v(%v) != nb(%v)=%v", na, bgi.as, bgi.bs, nb))
	}
	return nb
}

func (bgi *blueGreenIterator) Value() (uint64, *roaring.Container) {
	ka, ca := bgi.ait.Value()
	kb, cb := bgi.bit.Value()
	if ka != kb {
		panic(fmt.Sprintf("ka=%v != kb=%v", ka, kb))
	}
	err := ca.BitwiseCompare(cb)
	panicOn(err)
	return kb, cb
}
func (bgi *blueGreenIterator) Close() {
	bgi.ait.Close()
	bgi.bit.Close()
}

// ForEach is read-only on the database, and so we only pass through to B.
// Avoids the side-effects of calling fn too many times.
func (c *blueGreenTx) ForEach(index, field, view string, shard uint64, fn func(i uint64) error) error {
	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see ForEach() panic '%v' at '%v'", r, stack())
			panic(r)
		}
	}()
	return c.b.ForEach(index, field, view, shard, fn)

}

// ForEachRange cannot change the database, and we also can't control
// the side effects of the fn() calls. So we only pass through to B, not A.
// No checker.see() is needed as well, because we are read-only.
func (c *blueGreenTx) ForEachRange(index, field, view string, shard uint64, start, end uint64, fn func(uint64) error) error {

	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see ForEachRange() panic '%v' at '%v'", r, stack())
			panic(r)
		}
	}()

	// calling fn will have side effects; can only call it the right number of times.
	// so can't do this.
	//	errA := c.a.ForEachRange(index, field, view, shard, start, end, fn)
	return c.b.ForEachRange(index, field, view, shard, start, end, fn)
}

func (c *blueGreenTx) Count(index, field, view string, shard uint64) (uint64, error) {
	c.checker.see(index, field, view, shard)
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
	c.checker.see(index, field, view, shard)
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
	c.checker.see(index, field, view, shard)
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
	c.checker.see(index, field, view, shard)
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
	c.checker.see(index, field, view, shard)
	defer func() {
		if r := recover(); r != nil {
			c.Dump()
			AlwaysPrintf("see CountRange() panic '%v' at '%v'", r, stack())
			panic(r)
		}
	}()
	a, errA := c.a.CountRange(index, field, view, shard, start, end)
	b, errB := c.b.CountRange(index, field, view, shard, start, end)

	if a != b {
		panic(fmt.Sprintf("a(%v) = %v, but b(%v) = %v", c.as, a, c.bs, b))
	}

	compareErrors(errA, errB)
	return b, errB
}

func (c *blueGreenTx) OffsetRange(index, field, view string, shard, offset, start, end uint64) (other *roaring.Bitmap, err error) {
	c.checker.see(index, field, view, shard)
	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see OffsetRange() panic '%v' at '%v'", r, stack())
			panic(r)
		}
	}()
	a, errA := c.a.OffsetRange(index, field, view, shard, offset, start, end)
	b, errB := c.b.OffsetRange(index, field, view, shard, offset, start, end)

	err = roaringBitmapDiff(a, b)
	if err != nil {
		c.Dump()
		panicOn(err)
	}
	compareErrors(errA, errB)

	//vv("end of blue-green OffsetRange, dump:")
	//c.Dump()

	return b, errB
}

func (c *blueGreenTx) RoaringBitmapReader(index, field, view string, shard uint64, fragmentPathForRoaring string) (r io.ReadCloser, sz int64, err error) {
	c.checker.see(index, field, view, shard)
	defer func() {
		if r := recover(); r != nil {
			c.Dump()
			AlwaysPrintf("see RoaringBitmapReader() panic '%v' at '%v'", r, stack())
			panic(r)
		}
	}()

	rcA, szA, errA := c.a.RoaringBitmapReader(index, field, view, shard, fragmentPathForRoaring)
	rcB, szB, errB := c.b.RoaringBitmapReader(index, field, view, shard, fragmentPathForRoaring)

	compareErrors(errA, errB)

	// We are seeing Roaring vs Badger size differences on
	// server/ test TestClusterResize_AddNode/ContinuousShards,
	// so turn off the szA vs szB checks and MutliReaderB use. But keep them if we want to
	// check RBF vs Badger for byte-for-byte compatiblity (we
	// suspect the ops log or optimized bitmaps are accounting for the difference).
	sizeMustMatch := false
	if sizeMustMatch {
		if szA != szB {
			panic(fmt.Sprintf("szA(%v) = %v, but szB(%v) = %v;  fragmentPathForRoaring='%v'", c.as, szA, c.bs, szB, fragmentPathForRoaring))
		}
		return &MultiReaderB{a: rcA, b: rcB}, szB, errB
	} else {
		// one db won't get data if we do
		//return &MultiReaderB{a: rcA, b: rcB, allowSizeVariation: true}, szB, errB
		_, _ = szA, errA
		rcA.Close()
		return rcB, szB, errB
	}
}

func (c *blueGreenTx) SliceOfShards(index, field, view, optionalViewPath string) (sliceOfShards []uint64, err error) {
	// doesn't change state, so we don't really need see() call here. And we don't have a single shard for it.
	//c.checker.see(index, field, view, shard) // don't have shard.
	defer func() {
		if r := recover(); r != nil {
			c.Dump()
			AlwaysPrintf("see SliceOfShards() panic '%v' at '%v'", r, stack())
			panic(r)
		}
	}()
	slcA, errA := c.a.SliceOfShards(index, field, view, optionalViewPath)
	slcB, errB := c.b.SliceOfShards(index, field, view, optionalViewPath)
	compareErrors(errA, errB)

	// sort order may be different, and that's ok.
	cpa := append([]uint64{}, slcA...)
	cpb := append([]uint64{}, slcB...)
	sort.Slice(cpa, func(i, j int) bool { return cpa[i] < cpa[j] })
	sort.Slice(cpb, func(i, j int) bool { return cpb[i] < cpb[j] })

	if !reflect.DeepEqual(cpa, cpb) {
		// report the first difference
		ma := make(map[uint64]bool)
		for _, ka := range slcA {
			ma[ka] = true
		}
		for _, kb := range slcB {
			if !ma[kb] {
				//vv("blueGreenTx SliceOfShards diference! B(%v) had shard %v, but A(%v) did not. cpa='%#v'; cpb='%#v'; in the SliceOfShards returned slice.", c.bs, kb, c.as, cpa, cpb)
				c.Dump()
				panic(fmt.Sprintf("blueGreenTx SliceOfShards diference! B(%v) had shard %v, but A(%v) did not. cpa='%#v'; cpb='%#v'; in the SliceOfShards returned slice.", c.bs, kb, c.as, cpa, cpb))
			}
			delete(ma, kb)
		}
		if len(ma) != 0 {
			for firstDifference := range ma {
				panic(fmt.Sprintf("blueGreenTx SliceOfShards diference! A(%v) had %v, but B(%v) did not. cpa='%#v'; cpb='%#v'; in the SliceOfShards returned slice.", c.as, firstDifference, c.bs, cpa, cpb))
			}
		}
		panic(fmt.Sprintf("blueGreenTx SliceOfShards diference \n slcA(%v)='%#v';\n slcB(%v)='%#v';\n", c.as, cpa, c.bs, cpb))
	}
	return slcB, errB
}

// MultiReaderB is returned by RoaringBitmapReader. It verifies
// that identical byte streams are read from its two members.
type MultiReaderB struct {
	a io.ReadCloser
	b io.ReadCloser

	allowSizeVariation bool
}

// Read implements the standard io.Reader method. It panics
// if "a" and "b" have even one byte different in their reads.
func (m *MultiReaderB) Read(p []byte) (nB int, errB error) {
	nB, errB = m.b.Read(p)
	p2 := make([]byte, nB)

	// read (and discard after comparing for equality) the exact same amount from A.

	// ReadAtLeast reads from r into buf until it has read at least
	// min bytes. It returns the number of bytes copied and an error
	// if fewer bytes were read. The error is EOF only if no bytes
	// were read. If an EOF happens after reading fewer than min bytes,
	// ReadAtLeast returns ErrUnexpectedEOF. If min is greater than
	// the length of buf, ReadAtLeast returns ErrShortBuffer. On
	// return, n >= min if and only if err == nil. If r returns
	// an error having read at least min bytes, the error is dropped.
	nA, errA := io.ReadAtLeast(m.a, p2, nB)

	if !m.allowSizeVariation {
		if errA == io.ErrUnexpectedEOF {
			panic(fmt.Sprintf("MultiReaderB got ErrUnexpectedEOF: read %v bytes from B, but could only read %v bytes for A", nB, nA))
		}
		if nA != nB {
			panic(fmt.Sprintf("MultiReaderB read %v bytes from B, but could only read %v bytes for A", nB, nA))
		}
		cmp := bytes.Compare(p[:nB], p2[:nB])
		if cmp != 0 {
			panic(fmt.Sprintf("MultiReaderB reads p and p2 (cmp= %v) differed.", cmp))
		}
	}
	return
}

func (m *MultiReaderB) Close() error {
	m.a.Close()
	return m.b.Close()
}

// blueGreenChecker is used
type blueGreenChecker struct {
	visited map[string]map[string]map[string]map[uint64]struct{}

	c *blueGreenTx

	// lock mu when using visited.
	// otherwise concurrent map writes on TestAPI_Import/RowIDColumnKey
	mu sync.Mutex
}

// see would mark a thing as seen.
func (b *blueGreenChecker) see(index, field, view string, shard uint64) {
	// keep this next Printf. Useful to see the sequence of Tx operations.
	//fmt.Printf("blueGreenTx.%v on index='%v'\n", Caller(1), index)

	// is ckey 6960 present in i/x/standard/0 ?
	////vv("6960 present index='%v',field='%v';view='%v';shard='%v'; isIn=%v", index, field, view, shard, b.c.isIn(index, field, view, shard, 456130566))

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.visited == nil {
		b.visited = make(map[string]map[string]map[string]map[uint64]struct{})
	}
	var visitedIdx map[string]map[string]map[uint64]struct{}
	var visitedField map[string]map[uint64]struct{}
	var visitedView map[uint64]struct{}

	if visitedIdx = b.visited[index]; visitedIdx == nil {
		visitedIdx = make(map[string]map[string]map[uint64]struct{})
		b.visited[index] = visitedIdx
	}
	if visitedField = visitedIdx[field]; visitedField == nil {
		visitedField = make(map[string]map[uint64]struct{})
		visitedIdx[field] = visitedField
	}
	if visitedView = visitedField[view]; visitedView == nil {
		visitedView = make(map[uint64]struct{})
		visitedField[view] = visitedView
	}
	visitedView[shard] = struct{}{}
}

// seen reports the things it has seen, exactly once so
// that Rollback can be called after Commit without repeating
// the check.
func (b *blueGreenChecker) seen() map[string]map[string]map[string]map[uint64]struct{} {
	return b.visited
}
