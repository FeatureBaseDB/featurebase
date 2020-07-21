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
	"reflect"
	"sort"

	"github.com/pilosa/pilosa/v2/roaring"
)

// blueGreenTx runs two Tx together and notices differences in their output.
// By convention, the 'b' Tx is the output that is returned to caller.
type blueGreenTx struct {
	a Tx
	b Tx // b's output is returned

	idx *Index

	checker blueGreenChecker
}

func newBlueGreenTx(a, b Tx, idx *Index) *blueGreenTx {
	return &blueGreenTx{a: a, b: b, idx: idx}
}

var _ = newBlueGreenTx // keep linter happy

var _ Tx = (*blueGreenTx)(nil)

func (c *blueGreenTx) Type() string {
	return c.a.Type() + "_" + c.b.Type()
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

func (c *blueGreenTx) compareTxState(index, field, view string, shard uint64) {
	here := fmt.Sprintf("%v/%v/%v/%v", index, field, view, shard)
	aIter, aFound, aErr := c.a.ContainerIterator(index, field, view, shard, 0)
	bIter, bFound, bErr := c.b.ContainerIterator(index, field, view, shard, 0)

	if aFound != bFound {
		panic(fmt.Sprintf("compareTxState[%v]: A ContainerIterator had aFound=%v, but B had bFound=%v; at '%v'", here, aFound, bFound, stack()))
	}

	if aErr == nil {
		defer aIter.Close()
	}
	if bErr == nil {
		defer bIter.Close()
	}
	if aErr != nil || bErr != nil {
		if aErr != nil && bErr != nil {
			panic(fmt.Sprintf("compareTxState[%v]: A reported err '%v'; B reported err '%v' at %v", here, aErr, bErr, stack()))
		}
		if aErr != nil {
			panic(fmt.Sprintf("compareTxState[%v]: A reported err %v at %v; but B did not", here, aErr, stack()))
		}
		if bErr != nil {
			panic(fmt.Sprintf("compareTxState[%v]: B reported err %v at %v; but A did not", here, bErr, stack()))
		}
	}
	for aIter.Next() {
		aKey, aValue := aIter.Value()
		if !bIter.Next() {
			panic(fmt.Sprintf("compareTxState[%v]: A found key %v, B didn't, at %v", here, aKey, stack()))
		}
		bKey, bValue := bIter.Value()
		if bKey != aKey {
			panic(fmt.Sprintf("compareTxState[%v]: A found key %v, B found %v, at %v", here, aKey, bKey, stack()))
		}
		if err := aValue.BitwiseCompare(bValue); err != nil {
			panic(fmt.Sprintf("compareTxState[%v]: key %v differs: %v at %v", here, aKey, err, stack()))
		}
	}
	// end checking everything in A, but does B have more?
	if bIter.Next() {
		bKey, _ := bIter.Value()
		panic(fmt.Sprintf("compareTxState[%v]: B found key %v, A didn't, at %v", here, bKey, stack()))
	}
}

func (c *blueGreenTx) checkDatabase() {
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
	c.checkDatabase()
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
	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see ImportRoaringBits() panic '%v' at '%v'", r, stack())
			panic(r)
		}
	}()

	// remember where the iterator started, so we can replay it a second time.
	rit2 := rit.Clone()

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
	return c.b.UseRowCache()
}

func (c *blueGreenTx) Add(index, field, view string, shard uint64, batched bool, a ...uint64) (changeCount int, err error) {
	c.checker.see(index, field, view, shard)
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
	// TODO: need to return a blueGreenIterator too, that does close/next operations on both A and B.
	ait, afound, errA := c.a.ContainerIterator(index, field, view, shard, firstRoaringContainerKey)
	_, _, _ = ait, afound, errA
	bit, bfound, errB := c.b.ContainerIterator(index, field, view, shard, firstRoaringContainerKey)

	compareErrors(errA, errB)
	if errA != nil {
		ait.Close() // don't leak it.
	}
	return bit, bfound, errB
}

func (c *blueGreenTx) ForEach(index, field, view string, shard uint64, fn func(i uint64) error) error {
	c.checker.see(index, field, view, shard)
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
	c.checker.see(index, field, view, shard)
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
	panicOn(err)
	compareErrors(errA, errB)
	return b, errB
}

func (c *blueGreenTx) RoaringBitmapReader(index, field, view string, shard uint64, fragmentPathForRoaring string) (r io.ReadCloser, sz int64, err error) {
	c.checker.see(index, field, view, shard)
	defer func() {
		if r := recover(); r != nil {
			AlwaysPrintf("see OffsetRange() panic '%v' at '%v'", r, stack())
			panic(r)
		}
	}()

	rcA, szA, errA := c.a.RoaringBitmapReader(index, field, view, shard, fragmentPathForRoaring)
	rcB, szB, errB := c.b.RoaringBitmapReader(index, field, view, shard, fragmentPathForRoaring)
	if szA != szB {
		panic(fmt.Sprintf("szA = %v, but szB = %v", szA, szB))
	}
	compareErrors(errA, errB)
	return &MultiReaderB{a: rcA, b: rcB}, szB, errB
}

func (c *blueGreenTx) SliceOfShards(index, field, view, optionalViewPath string) (sliceOfShards []uint64, err error) {
	// doesn't change state, so we don't really need see() call here. And we don't have a single shard for it.
	//c.checker.see(index, field, view, shard) // don't have shard.
	defer func() {
		if r := recover(); r != nil {
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
				panic(fmt.Sprintf("blueGreenTx SliceOfShards diference! B had %v, but A did not; in the SliceOfShards returned slice.", kb))
			}
			delete(ma, kb)
		}
		if len(ma) != 0 {
			for _, firstDifference := range ma {
				panic(fmt.Sprintf("blueGreenTx SliceOfShards diference! A had %v, but B did not; in the SliceOfShards returned slice.", firstDifference))
			}
		}
		panic(fmt.Sprintf("blueGreenTx SliceOfShards diference \n slcA='%#v';\n slcB='%#v';\n", cpa, cpb))
	}
	return slcB, errB
}

type MultiReaderB struct {
	a io.ReadCloser
	b io.ReadCloser
}

// TODO(jea): test this for accuracy/correctness.
func (m *MultiReaderB) Read(p []byte) (nB int, errB error) {
	nB, errB = m.b.Read(p)
	p2 := make([]byte, nB)
	// discard the exact same amount from A
	// ReadAtLeast reads from r into buf until it has read at least
	// min bytes. It returns the number of bytes copied and an error
	// if fewer bytes were read. The error is EOF only if no bytes
	// were read. If an EOF happens after reading fewer than min bytes,
	// ReadAtLeast returns ErrUnexpectedEOF. If min is greater than
	// the length of buf, ReadAtLeast returns ErrShortBuffer. On
	// return, n >= min if and only if err == nil. If r returns
	// an error having read at least min bytes, the error is dropped.
	nA, errA := io.ReadAtLeast(m.a, p2, nB)
	if errA == io.ErrUnexpectedEOF {
		panic(fmt.Sprintf("MultiReaderB got ErrUnexpectedEOF: read %v bytes from B, but could only read %v bytes for A", nB, nA))
	}
	if nA != nB {
		panic(fmt.Sprintf("MultiReaderB read %v bytes from B, but could only read %v bytes for A", nB, nA))
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
	done    bool
}

// see would mark a thing as seen.
func (b *blueGreenChecker) see(index, field, view string, shard uint64) {
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
	if b.done {
		return nil
	}
	b.done = true
	return b.visited
}
