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
	"sync"

	"github.com/molecula/featurebase/v2/roaring"
	txkey "github.com/molecula/featurebase/v2/short_txkey"
	. "github.com/molecula/featurebase/v2/vprint"
)

// blueGreenTx runs two Tx together and notices differences in their output.
// By convention, the 'b' Tx is the output that is returned to caller.
//
// Warning: DATA RACES are expected if RoaringTx is one side of the Tx pair.
// The checkDatabase() call will do reads of the fragments at Commit/Rollback,
// while the snapshotqueue may be doing writes.
//
// Do not run with go test -race and expect it to be race free with RoaringTx
// on one arm.
//
// Note: using the dbshard.go DBShard.mut RWMutex to begin and end
// both the A and B transactions atomically, we support a single importer and
// lots of readers running under blue-green transactions. Two writers a.k.a. two
// github ingests at once will deadlock eventually, but I think that may be asking
// for more than we want to test under blue-green, as it would require a bunch of
// test-only internal executor logic that could mess with the production path.
// So, for now, a limitation on blue green tests is that they be single
// writer/single importer going at once.
//
type blueGreenTx struct {
	a Tx
	b Tx // b's output is returned

	o  Txo
	as string
	bs string

	types      []txtype
	hasRoaring bool

	// roaring will not create as many Tx (they are
	// psuedo Tx anyway), espcially when deleting
	// files. Return the non-roaring Sn if
	// possible, by referencing useSnA.
	useSnA bool

	idx *Index

	checker              blueGreenChecker
	mu                   sync.Mutex
	rollbackOrCommitDone bool

	txf *TxFactory

	short bool // short Dump or long

	FullDump bool // else quieter, don't attemp Dump() if false.
}

// blueGreenRegistry is used to force checking of (read) transactions
// before writes happen, if roaring is on one of the A/B branches.
// Because roaring won't have an MVCC view of the world. Writes to
// roaring will show up, while writes to the DB won't show up on
// readTx that have already started.
type blueGreenRegistry struct {
	mu         sync.Mutex
	m          map[int64]*blueGreenTx
	types      []txtype
	hasRoaring bool
}

// if we have raoring in the mix we cannot expect reads
// to match up, but otherwise do.
func newBlueGreenReg(types []txtype) *blueGreenRegistry {

	hasRoaring := false
	if types[0] == roaringTxn || types[1] == roaringTxn {
		hasRoaring = true
	}
	return &blueGreenRegistry{
		m:          make(map[int64]*blueGreenTx),
		types:      types,
		hasRoaring: hasRoaring,
	}
}

// add remembers the tx so we can check that
// all tx were finished before Close().
func (b *blueGreenRegistry) add(c *blueGreenTx) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if c.useSnA {
		b.m[c.a.Sn()] = c
	} else {
		b.m[c.b.Sn()] = c
	}
}

func (b *blueGreenRegistry) finishedTx(tx *blueGreenTx) {
	b.mu.Lock()
	defer b.mu.Unlock()
	sn := tx.Sn()
	delete(b.m, sn)
	//vv("blueGreenRegistry deleted _sn_ %v", sn)

	// Note that a tx.o.dbs.Cleanup(tx) call should not be needed,
	// because the individual tx will call cleanup themselves.
}

func (b *blueGreenRegistry) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.m) > 0 {
		PanicOn(fmt.Sprintf("still have open/unchecked blueGreenTx: '%#v'", b.m))
		//AlwaysPrintf("still have unchecked blueGreenTx: '%#v'", b.m)
	}
}

func (txf *TxFactory) newBlueGreenTx(a, b Tx, idx *Index, o Txo) *blueGreenTx {
	as := a.Type()
	bs := b.Type()
	c := &blueGreenTx{a: a,
		b:          b,
		idx:        idx,
		as:         as,
		bs:         bs,
		txf:        txf,
		types:      txf.types,
		hasRoaring: txf.blueGreenReg.hasRoaring,
		short:      true,
	}

	if c.types[1] == roaringTxn {
		c.useSnA = true
	}
	//vv("newBlueGreenTx with a.sn=%v with o.Shard=%v", c.Sn(), int(o.Shard))

	c.checker.c = c
	c.o = o
	txf.blueGreenReg.add(c)
	return c
}

var _ Tx = (*blueGreenTx)(nil)

func (c *blueGreenTx) Type() string {
	return c.a.Type() + "_" + c.b.Type()
}

var blueGreenTxDumpMut sync.Mutex

func (c *blueGreenTx) Dump(short bool, shard uint64) {

	if !c.FullDump {
		return
	}

	blueGreenTxDumpMut.Lock()
	defer blueGreenTxDumpMut.Unlock()
	fmt.Printf("%v blueGreenTx.Dump ============== \n", FileLine(2))
	fmt.Printf("A(%v) Dump:\n", c.as)
	c.a.Dump(short, shard)
	fmt.Printf("B(%v) Dump:\n", c.bs)
	c.b.Dump(short, shard)

	if !short {
		fmt.Printf("dbPerShard.DumpAll(): idx=%p\n", c.idx)
		c.idx.holder.txf.dbPerShard.DumpAll()
	}
}

func (c *blueGreenTx) Readonly() bool {
	a := c.a.Readonly()
	b := c.b.Readonly()
	if a != b {
		PanicOn(fmt.Sprintf("Readonly difference, a=%v, but b =%v", a, b))
	}
	return b
}

// for now we just return B's list, since this is involved in
// holder Open which can happen before any blue-green is done;
// in fact this is instrumental in setting up the sync from
// green to blue.
func (c *blueGreenTx) GetSortedFieldViewList(idx *Index, shard uint64) (fvB []txkey.FieldView, errB error) {
	fvB, errB = c.b.GetSortedFieldViewList(idx, shard)
	return
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
	if c.o.blueGreenOff {
		return
	}
	here := fmt.Sprintf("%v/%v/%v/%v", index, field, view, shard)
	//vv("compareTxState here = '%v', _sn_ %v gid=%v", here, c.Sn(), curGID())
	aIter, aFound, aErr := c.a.ContainerIterator(index, field, view, shard, 0)
	bIter, bFound, bErr := c.b.ContainerIterator(index, field, view, shard, 0)
	if aErr == nil || aIter != nil {
		defer aIter.Close()
	}
	if bErr == nil || bIter != nil {
		defer bIter.Close()
	}

	if aFound != bFound {
		c.Dump(c.short, shard)
		PanicOn(fmt.Sprintf("compareTxState[%v]: A(%v) ContainerIterator had aFound=%v, but B(%v) had bFound=%v; at '%v'", here, c.as, aFound, c.bs, bFound, Stack()))
	}

	if aErr != nil || bErr != nil {
		if aErr != nil && bErr != nil {
			c.Dump(c.short, shard)
			PanicOn(fmt.Sprintf("compareTxState[%v]: A(%v) reported err '%v'; B(%v) reported err '%v' at %v", here, c.as, aErr, c.bs, bErr, Stack()))
		}
		if aErr != nil {
			c.Dump(c.short, shard)
			PanicOn(fmt.Sprintf("compareTxState[%v]: A(%v) reported err %v at %v; but B(%v) did not", here, c.as, aErr, c.bs, Stack()))
		}
		if bErr != nil {
			c.Dump(c.short, shard)
			PanicOn(fmt.Sprintf("compareTxState[%v]: B(%v) reported err %v at %v; but A(%v) did not", here, c.bs, bErr, c.as, Stack()))
		}
	}
	for aIter.Next() {
		aKey, aValue := aIter.Value()

		if !bIter.Next() {
			AlwaysPrintf("compareTxState[%v]: A(%v) found key %v, B(%v) didn't, dump to follow, Stack=\n %v\n\n and here is dump:", here, c.as, aKey, c.bs, Stack())
			c.Dump(c.short, shard)
			PanicOn(fmt.Sprintf("compareTxState[%v]: A(%v) found key %v, B(%v) didn't, at %v", here, c.as, aKey, c.bs, Stack()))
		}
		bKey, bValue := bIter.Value()
		if bKey != aKey {
			AlwaysPrintf("problem in caller %v", Caller(2))
			c.Dump(c.short, shard)
			PanicOn(fmt.Sprintf("compareTxState[%v]: A(%v) found key %v, B(%v) found %v, at %v", here, c.as, aKey, c.bs, bKey, Stack()))
		}
		if err := aValue.BitwiseCompare(bValue); err != nil {
			c.Dump(c.short, shard)
			//vv("compareTxState[%v]: key %v differs: %v;  A=%v; B=%v; at Stack=%v", here, aKey, err, c.as, c.bs, Stack())
			PanicOn(fmt.Sprintf("compareTxState[%v]: key %v differs: %v;  A=%v; B=%v; at Stack=%v", here, aKey, err, c.as, c.bs, Stack()))
		}
		//vv("successfully matched aKey(%v)='%v' and bKey(%v)='%v'", c.as, aKey, c.bs, bKey)
	}
	// end checking everything in A, but does B have more?
	if bIter.Next() {
		AlwaysPrintf("bIter has more than it should. problem in caller %v. _sn_ %v", Caller(2), c.Sn())
		c.Dump(c.short, shard)
		bKey, _ := bIter.Value()
		PanicOn(fmt.Sprintf("compareTxState[%v]: B(%v) found key %v, A(%v) didn't, (a.sn=%v) (b.sn=%v) at %v", here, c.bs, bKey, c.as, c.a.Sn(), c.b.Sn(), Stack()))
	}
	//vv("done without problem. compareTxState here = '%v', _sn_ %v gid=%v", here, c.Sn(), curGID())
}

func (c *blueGreenTx) checkDatabase() {
	if c.o.blueGreenOff {
		return
	}
	if c.hasRoaring && !c.o.Write {
		// With roaring on one arm, we only check the we are A/B
		// consistent after every write.
		//
		// Ideally reads can only see that consitent state, and don't need
		// to be checked themselves-- but we do try if both A and B
		// are transactional. Sketch of proof by induction that
		// write checking should, theoretically, suffice:
		// Starting with zero data, if we have agreement in both A/B
		// database state after each write, then
		// because there is only ever a single
		// writer (for LMDB/RBF), we should always have the same
		// data state between A and B as long as every prior
		// A/B check of the serialized writes suceeded.
		//
		// This avoids a key problem we discovered when A/B checking reads
		// with roaring on one arm.
		// The MVCC of the transactional engines means that reads that
		// start before a write commit will look very different
		// when comparing to roaring's non-transactional state.
		return
	}

	c.checker.mu.Lock()
	defer c.checker.mu.Unlock()
	if c.checker.checkDone {
		return // idemopotent. checkDatabase can be called twice. Only the first does the checks.
	}
	c.checker.checkDone = true

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

func (c *blueGreenTx) IsDone() bool {
	return c.b.IsDone()
}

func (c *blueGreenTx) Rollback() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.rollbackOrCommitDone {
		return // avoid using discarded tx for Dump, which will PanicOn.
	}
	c.rollbackOrCommitDone = true

	if c.o.Write {
		c.checkDatabase()
	}
	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see Rollback() PanicOn '%v' at '%v'", r, Stack())
			PanicOn(r)
		}
	}()
	//vv("blueGreenTx.Rollback() about to call (%v) a.Rollback()", c.as)
	c.a.Rollback()
	//vv("blueGreenTx.Rollback() about to call (%v) b.Rollback()", c.bs)
	c.b.Rollback()
	//vv("blueGreenTx.Rollback() done. bgtx p=%p", c)

	c.txf.blueGreenReg.finishedTx(c)
}

func (c *blueGreenTx) Commit() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.rollbackOrCommitDone {
		return nil
	}
	//vv("blueGreenTx.Commit() called. bgtx p=%p", c)
	c.rollbackOrCommitDone = true
	if c.o.Write {
		if !c.o.blueGreenOff {
			c.checkDatabase()
		}
	}
	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see Commit() PanicOn '%v' at '%v'", r, Stack())
			PanicOn(r)
		}
	}()
	errA := c.a.Commit()
	_ = errA
	errB := c.b.Commit()

	compareErrors(errA, errB)
	c.txf.blueGreenReg.finishedTx(c)
	return errB
}

func (c *blueGreenTx) RoaringBitmap(index, field, view string, shard uint64) (*roaring.Bitmap, error) {
	c.checker.see(index, field, view, shard)
	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see RoaringBitmap() PanicOn '%v' at '%v'", r, Stack())
			PanicOn(r)
		}
	}()
	a, errA := c.a.RoaringBitmap(index, field, view, shard)
	_, _ = a, errA
	b, errB := c.b.RoaringBitmap(index, field, view, shard)
	if !c.o.blueGreenOff {
		compareErrors(errA, errB)

		slcA := a.Slice()
		slcB := b.Slice()
		if !reflect.DeepEqual(slcA, slcB) {
			PanicOn("blueGreenTx.RoaringBitmap() returning different roaring.Bitmaps!")
		}
	}
	return b, errB
}

func (c *blueGreenTx) Container(index, field, view string, shard uint64, key uint64) (ct *roaring.Container, err error) {
	c.checker.see(index, field, view, shard)
	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see Container() PanicOn '%v' at '%v'", r, Stack())
			PanicOn(r)
		}
	}()
	a, errA := c.a.Container(index, field, view, shard, key)
	b, errB := c.b.Container(index, field, view, shard, key)

	if !c.o.blueGreenOff {
		compareErrors(errA, errB)
		err = a.BitwiseCompare(b)
		PanicOn(err)
	}
	return b, errB
}

func (c *blueGreenTx) PutContainer(index, field, view string, shard uint64, key uint64, rc *roaring.Container) error {
	c.checker.see(index, field, view, shard)
	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see PutContainer() PanicOn '%v' at '%v'", r, Stack())
			PanicOn(r)
		}
	}()
	errA := c.a.PutContainer(index, field, view, shard, key, rc)
	errB := c.b.PutContainer(index, field, view, shard, key, rc)

	if !c.o.blueGreenOff {
		compareErrors(errA, errB)
	}
	return errB
}

func (c *blueGreenTx) ImportRoaringBits(index, field, view string, shard uint64, rit roaring.RoaringIterator, clear bool, log bool, rowSize uint64, data []byte) (changed int, rowSet map[uint64]int, err error) {
	c.checker.see(index, field, view, shard)

	// these are the first port of call for debugging, so we leave them in.
	// ================== begin save comments.
	//c.checkDatabase()
	////vv("got past database check at TOP of ImportRoaringBits")
	//c.Dump(c.short, shard)
	////vv("done with top dump; clear=%v", clear)
	// ==================   end save comments.
	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see ImportRoaringBits() PanicOn '%v' at '%v'", r, Stack())
			PanicOn(r)
		}
	}()

	// remember where the iterator started, so we can replay it a second time.
	rit2 := rit.Clone()
	PanicOn(err)

	changedA, rowSetA, errA := c.a.ImportRoaringBits(index, field, view, shard, rit, clear, log, rowSize, data)
	changedB, rowSetB, errB := c.b.ImportRoaringBits(index, field, view, shard, rit2, clear, log, rowSize, data)

	if !c.o.blueGreenOff {

		if len(data) == 0 {
			// okay to check! otherwise we are in the fragment.fillFragmentFromArchive
			// case where we know that RoaringTx.ImportRoaringBits changed and rowSet will
			// be inaccurate.
			if changedA != changedB {
				PanicOn(fmt.Sprintf("changedA = %v, but changedB = %v", changedA, changedB))
			}
			if len(rowSetA) != len(rowSetB) {
				PanicOn(fmt.Sprintf("rowSetA = %#v, but rowSetB = %#v", rowSetA, rowSetB))
			}
			for k, va := range rowSetA {
				vb, ok := rowSetB[k]
				if !ok {
					PanicOn(fmt.Sprintf("diff on key '%v': present in rowSetA, but not in rowSet B. rowSetA = %#v, but rowSetB = %#v", k, rowSetA, rowSetB))
				}
				if va != vb {
					PanicOn(fmt.Sprintf("diff on key '%v', rowSetA has value '%v', but rowSetB has value '%v'", k, va, vb))
				}
			}
		}
		compareErrors(errA, errB)
		c.checkDatabase()
	}
	return changedB, rowSetB, errB
}

func (c *blueGreenTx) RemoveContainer(index, field, view string, shard uint64, key uint64) error {
	c.checker.see(index, field, view, shard)
	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see RemoveContainer() PanicOn '%v' at '%v'", r, Stack())
			PanicOn(r)
		}
	}()
	errA := c.a.RemoveContainer(index, field, view, shard, key)
	errB := c.b.RemoveContainer(index, field, view, shard, key)

	if !c.o.blueGreenOff {
		compareErrors(errA, errB)
	}
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
	PanicOn(errA)
	inB, errB := c.b.Contains(index, field, view, shard, ckey)
	PanicOn(errB)
	r[0] = inA
	r[1] = inB
	return
}

func (c *blueGreenTx) Add(index, field, view string, shard uint64, a ...uint64) (changeCount int, err error) {
	c.checker.see(index, field, view, shard)
	//vv("blueGreenTx) Add(index=%v, field=%v, view=%v, shard=%v", index, field, view, shard)
	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see Add() PanicOn '%v' for index='%v', field='%v', view='%v', shard='%v' at '%v'", r, index, field, view, shard, Stack())
			PanicOn(r)
		}
	}()

	// must copy a before calling Add(), since RoaringTx.Add() uses roaring.DirectAddN()
	// which modifies the input array a.
	a2 := make([]uint64, len(a))
	copy(a2, a)

	ach, errA := c.a.Add(index, field, view, shard, a...)
	_, _ = ach, errA

	bch, errB := c.b.Add(index, field, view, shard, a2...)

	if !c.o.blueGreenOff {

		if ach != bch {
			PanicOn(fmt.Sprintf("Add() difference, ach=%v, but bch=%v; errA='%v'; errB='%v'", ach, bch, errA, errB))
		}
		compareErrors(errA, errB)
	}

	return bch, errB
}

func compareErrors(errA, errB error) {
	switch {
	case errA == nil && errB == nil:
		// OK
	case errA == nil:
		PanicOn(fmt.Sprintf("errA is nil, but errB = %#v", errB))
	case errB == nil:
		PanicOn(fmt.Sprintf("errB is nil, but errA = %#v", errA))
	default:
		ae := errA.Error()
		be := errB.Error()
		if ae != be {
			PanicOn(fmt.Sprintf("errA is '%v', but errB is '%v'", ae, be))
		}
	}
}

func (c *blueGreenTx) Remove(index, field, view string, shard uint64, a ...uint64) (changeCount int, err error) {
	c.checker.see(index, field, view, shard)
	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see Remove() PanicOn '%v' at '%v'", r, Stack())
			PanicOn(r)
		}
	}()
	ach, errA := c.a.Remove(index, field, view, shard, a...)
	_, _ = ach, errA
	bch, errB := c.b.Remove(index, field, view, shard, a...)

	if !c.o.blueGreenOff {
		compareErrors(errA, errB)
	}
	return bch, errB
}

func (c *blueGreenTx) Contains(index, field, view string, shard uint64, key uint64) (exists bool, err error) {
	c.checker.see(index, field, view, shard)
	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see Contains() PanicOn '%v' at '%v'", r, Stack())
			PanicOn(r)
		}
	}()
	ax, errA := c.a.Contains(index, field, view, shard, key)
	_, _ = ax, errA
	bx, errB := c.b.Contains(index, field, view, shard, key)

	if !c.o.blueGreenOff {
		compareErrors(errA, errB)
	}
	return bx, errB
}

func (c *blueGreenTx) ContainerIterator(index, field, view string, shard uint64, firstRoaringContainerKey uint64) (citer roaring.ContainerIterator, found bool, err error) {
	c.checker.see(index, field, view, shard)
	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see ContainerIterator() PanicOn '%v' at '%v'", r, Stack())
			PanicOn(r)
		}
	}()

	ait, afound, errA := c.a.ContainerIterator(index, field, view, shard, firstRoaringContainerKey)
	_, _, _ = ait, afound, errA

	bit, bfound, errB := c.b.ContainerIterator(index, field, view, shard, firstRoaringContainerKey)

	if !c.o.blueGreenOff {
		compareErrors(errA, errB)
	}

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
	if errA != nil {
		// RoaringTx can return an iterator and an error, so be sure Close it we have it.
		if ait != nil {
			ait.Close()
		}
	}

	// INVAR: errA == errB == nil
	bgi := NewBlueGreenIterator(c, ait, bit)
	return bgi, bfound, errB
}

func (tx *blueGreenTx) GetFieldSizeBytes(index, field string) (uint64, error) {
	return 0, nil
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
		PanicOn(fmt.Sprintf("na=%v(%v) != nb(%v)=%v", na, bgi.as, bgi.bs, nb))
	}
	return nb
}

func (bgi *blueGreenIterator) Value() (uint64, *roaring.Container) {
	ka, ca := bgi.ait.Value()
	kb, cb := bgi.bit.Value()

	if !bgi.tx.o.blueGreenOff {
		if ka != kb {
			PanicOn(fmt.Sprintf("ka=%v != kb=%v", ka, kb))
		}
		err := ca.BitwiseCompare(cb)
		PanicOn(err)
	}
	return kb, cb
}
func (bgi *blueGreenIterator) Close() {
	bgi.ait.Close()
	bgi.bit.Close()
}

// ForEach is read-only on the database, and so we only pass through to B.
// Avoids the side-effects of calling fn too many times, which can cause serious false alarms.
func (c *blueGreenTx) ForEach(index, field, view string, shard uint64, fn func(i uint64) error) error {
	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see ForEach() PanicOn '%v' at '%v'", r, Stack())
			PanicOn(r)
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
			AlwaysPrintf("see ForEachRange() PanicOn '%v' at '%v'", r, Stack())
			PanicOn(r)
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
			AlwaysPrintf("see Count() PanicOn '%v' at '%v'", r, Stack())
			PanicOn(r)
		}
	}()
	a, errA := c.a.Count(index, field, view, shard)
	_, _ = a, errA
	b, errB := c.b.Count(index, field, view, shard)
	_, _ = b, errB

	if !c.o.blueGreenOff {
		compareErrors(errA, errB)
	}
	return b, errB
}

func (c *blueGreenTx) Max(index, field, view string, shard uint64) (uint64, error) {
	c.checker.see(index, field, view, shard)
	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see Max() PanicOn '%v' at '%v'", r, Stack())
			PanicOn(r)
		}
	}()
	a, errA := c.a.Max(index, field, view, shard)
	_, _ = a, errA
	b, errB := c.b.Max(index, field, view, shard)
	_, _ = b, errB

	if !c.o.blueGreenOff {
		compareErrors(errA, errB)
	}
	return b, errB
}

func (c *blueGreenTx) Min(index, field, view string, shard uint64) (uint64, bool, error) {
	c.checker.see(index, field, view, shard)
	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see Min() PanicOn '%v' at '%v'", r, Stack())
			PanicOn(r)
		}
	}()
	amin, afound, errA := c.a.Min(index, field, view, shard)
	_, _, _ = amin, afound, errA
	bmin, bfound, errB := c.b.Min(index, field, view, shard)
	_, _, _ = bmin, bfound, errB

	if !c.o.blueGreenOff {
		compareErrors(errA, errB)
	}
	return bmin, bfound, errB
}

func (c *blueGreenTx) UnionInPlace(index, field, view string, shard uint64, others ...*roaring.Bitmap) error {
	c.checker.see(index, field, view, shard)
	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see UnionInPlace() PanicOn '%v' at '%v'", r, Stack())
			PanicOn(r)
		}
	}()
	errA := c.a.UnionInPlace(index, field, view, shard, others...)
	errB := c.b.UnionInPlace(index, field, view, shard, others...)
	if !c.o.blueGreenOff {
		compareErrors(errA, errB)
	}
	return errB
}

func (c *blueGreenTx) CountRange(index, field, view string, shard uint64, start, end uint64) (n uint64, err error) {
	c.checker.see(index, field, view, shard)
	defer func() {
		if r := recover(); r != nil {
			c.Dump(c.short, shard)
			AlwaysPrintf("see CountRange() PanicOn '%v' at '%v'", r, Stack())
			PanicOn(r)
		}
	}()
	a, errA := c.a.CountRange(index, field, view, shard, start, end)
	b, errB := c.b.CountRange(index, field, view, shard, start, end)

	if !c.o.blueGreenOff {
		if a != b {
			PanicOn(fmt.Sprintf("a(%v) = %v, but b(%v) = %v", c.as, a, c.bs, b))
		}

		compareErrors(errA, errB)
	}
	return b, errB
}

func (c *blueGreenTx) OffsetRange(index, field, view string, shard, offset, start, end uint64) (other *roaring.Bitmap, err error) {
	c.checker.see(index, field, view, shard)
	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see OffsetRange() on _sn_ %v, PanicOn '%v' at '%v'", c.Sn(), r, Stack())
			PanicOn(r)
		}
	}()
	a, errA := c.a.OffsetRange(index, field, view, shard, offset, start, end)
	b, errB := c.b.OffsetRange(index, field, view, shard, offset, start, end)

	if !c.o.blueGreenOff {

		err = roaringBitmapDiff(a, b)
		if err != nil {
			c.Dump(false, shard)
			PanicOn(fmt.Errorf("on _sn_ %v OffsetRange(index='%v', field='%v', view='%v', shard='%v', offset: %v start: %v, end: %v) err: %v", c.Sn(), index, field, view, int(shard), offset, start, end, err))
		}
		compareErrors(errA, errB)
	}

	return b, errB
}

func (c *blueGreenTx) RoaringBitmapReader(index, field, view string, shard uint64, fragmentPathForRoaring string) (r io.ReadCloser, sz int64, err error) {
	c.checker.see(index, field, view, shard)
	defer func() {
		if r := recover(); r != nil {
			c.Dump(c.short, shard)
			AlwaysPrintf("see RoaringBitmapReader() PanicOn '%v' at '%v'", r, Stack())
			PanicOn(r)
		}
	}()

	rcA, szA, errA := c.a.RoaringBitmapReader(index, field, view, shard, fragmentPathForRoaring)
	rcB, szB, errB := c.b.RoaringBitmapReader(index, field, view, shard, fragmentPathForRoaring)

	if !c.o.blueGreenOff {
		compareErrors(errA, errB)
	}

	// We are seeing Roaring vs Badger size differences on
	// server/ test TestClusterResize_AddNode/ContinuousShards,
	// so turn off the szA vs szB checks and MutliReaderB use. But keep them if we want to
	// check RBF vs Badger for byte-for-byte compatiblity (we
	// suspect the ops log or optimized bitmaps are accounting for the difference).
	sizeMustMatch := false // !c.hasRoaring
	if c.o.blueGreenOff {
		sizeMustMatch = false
	}
	if sizeMustMatch {
		if szA != szB {
			PanicOn(fmt.Sprintf("szA(%v) = %v, but szB(%v) = %v;  fragmentPathForRoaring='%v'", c.as, szA, c.bs, szB, fragmentPathForRoaring))
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

func (c *blueGreenTx) Group() *TxGroup {
	return c.b.Group()
}

func (c *blueGreenTx) Options() Txo {
	return c.b.Options()
}

// Sn retreives the serial number of the Tx.
func (c *blueGreenTx) Sn() int64 {
	asn := c.a.Sn()
	bsn := c.b.Sn()

	if c.useSnA {
		return asn
	}
	return bsn
}

func (c *blueGreenTx) ApplyFilter(index, field, view string, shard uint64, ckey uint64, filter roaring.BitmapFilter) (err error) {
	return GenericApplyFilter(c, index, field, view, shard, ckey, filter)
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
			PanicOn(fmt.Sprintf("MultiReaderB got ErrUnexpectedEOF: read %v bytes from B, but could only read %v bytes for A", nB, nA))
		}
		if nA != nB {
			PanicOn(fmt.Sprintf("MultiReaderB read %v bytes from B, but could only read %v bytes for A", nB, nA))
		}
		cmp := bytes.Compare(p[:nB], p2[:nB])
		if cmp != 0 {
			PanicOn(fmt.Sprintf("MultiReaderB reads p and p2 (cmp= %v) differed.", cmp))
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

	checkDone bool
}

// see would mark a thing as seen.
func (b *blueGreenChecker) see(index, field, view string, shard uint64) {
	// keep this next Printf. Useful to see the sequence of Tx operations.
	//fmt.Printf("blueGreenTx.%v on index='%v' shard=%v\n", Caller(1), index, shard)

	if !b.c.o.Write {
		return
	}

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
