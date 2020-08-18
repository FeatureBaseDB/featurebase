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

// explanation of build tags:
//
// badgerdb builds but won't run in 32-bit 386 world, as of 2020 July 20.
// See https://github.com/dgraph-io/badger/issues/1384 for any progress.
// What we see is that the value-log allocations immediately run out of
// memory. So we turn off 386 with a build tag to keep the .circleci happy.
//
// gendebug_test will have a TestMain if build tag generationdebug is on,
// so we avoid conflicting with that debug scenario.

// +build !386
// +build !generationdebug

package pilosa

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"testing"

	"github.com/dgraph-io/badger/v2"
	"github.com/pilosa/pilosa/v2/roaring"
)

var _ = &roaring.Bitmap{}

// helpers, each runs their own new txn, and commits if a change/delete
// was made. The txn is rolled back if it is just viewing the data.

func badgerDBMustHaveBitvalue(dbwrap *BadgerDBWrapper, index, field, view string, shard uint64, bitvalue uint64) {

	tx := dbwrap.NewBadgerTx(!writable, index, nil)
	defer tx.Rollback()
	exists, err := tx.Contains(index, field, view, shard, bitvalue)
	panicOn(err)
	if !exists {
		panic(fmt.Sprintf("ARG bitvalue '%v' was NOT SET!!!", bitvalue))
	}

	tx.Rollback()
}

func badgerDBMustNotHaveBitvalue(dbwrap *BadgerDBWrapper, index, field, view string, shard uint64, bitvalue uint64) {

	tx := dbwrap.NewBadgerTx(!writable, index, nil)
	defer tx.Rollback()
	exists, err := tx.Contains(index, field, view, shard, bitvalue)
	panicOn(err)
	if exists {
		panic(fmt.Sprintf("ARG bitvalue '%v' WAS SET but should not have been.!!!", bitvalue))
	}
	tx.Rollback()
}

func badgerDBMustSetBitvalue(dbwrap *BadgerDBWrapper, index, field, view string, shard uint64, putme uint64) {
	tx := dbwrap.NewBadgerTx(writable, index, nil)

	// add a bit
	changed, err := tx.Add(index, field, view, shard, doBatched, putme)
	if changed != 1 {
		panic("should have 1 bit changed")
	}
	panicOn(err)

	exists, err := tx.Contains(index, field, view, shard, putme)
	panicOn(err)
	if !exists {
		panic("ARG putme was NOT SET!!!")
	}
	panicOn(tx.Commit())
}

func badgerDBMustDeleteBitvalueContainer(dbwrap *BadgerDBWrapper, index, field, view string, shard uint64, putme uint64) {
	tx := dbwrap.NewBadgerTx(writable, index, nil)
	hi := highbits(putme)
	panicOn(tx.RemoveContainer(index, field, view, shard, hi))
	panicOn(tx.Commit())
}

func badgerDBMustDeleteBitvalue(dbwrap *BadgerDBWrapper, index, field, view string, shard uint64, putme uint64) {
	tx := dbwrap.NewBadgerTx(writable, index, nil)
	_, err := tx.Remove(index, field, view, shard, putme)
	panicOn(err)
	panicOn(tx.Commit())
}

func mustOpenEmptyBadgerWrapper(path string) (w *BadgerDBWrapper, cleaner func()) {
	var err error
	fn := badgerPath(path)
	panicOn(os.RemoveAll(fn))
	w, err = globalBadgerReg.openBadgerDBWrapper(path)
	panicOn(err)

	// verify it is empty
	allkeys := w.StringifiedBadgerKeys(nil)
	if allkeys != "" {
		panic(fmt.Sprintf("freshly created database was not empty! had keys:'%v'", allkeys))
	}

	return w, func() {
		w.Close() // stop any started background GC goroutine.
		os.RemoveAll(fn)
	}
}

// end of helper utilities
//////////////////////////

//////////////////////////
// begin Tx method tests

func TestBadger_DeleteFragment(t *testing.T) {

	// setup
	dbwrap, clean := mustOpenEmptyBadgerWrapper("TestBadger_DeleteFragment")
	defer clean()
	defer dbwrap.Close()
	index, field, view, shard0 := "i", "f", "v", uint64(0)
	tx := dbwrap.NewBadgerTx(writable, index, nil)

	shard1 := uint64(1)

	bits := []uint64{0, 3, 1 << 16, 1<<16 + 3, 8 << 16}
	shards := []uint64{shard0, shard1}
	for _, s := range shards {
		for _, v := range bits {
			changed, err := tx.Add(index, field, view, s, doBatched, v)
			if changed <= 0 {
				panic("should have changed")
			}
			panicOn(err)
		}
	}

	for _, s := range shards {
		for _, v := range bits {
			exists, err := tx.Contains(index, field, view, s, v)
			panicOn(err)
			if !exists {
				panic("ARG bitvalue was NOT SET!!!")
			}
		}
	}
	err := tx.Commit()
	panicOn(err)

	// end of setup

	survivor := shard0
	victim := shard1
	err = dbwrap.DeleteFragment(index, field, view, victim, nil)
	panicOn(err)

	tx = dbwrap.NewBadgerTx(!writable, index, nil)
	defer tx.Rollback()

	for _, s := range shards {
		for _, v := range bits {
			exists, err := tx.Contains(index, field, view, s, v)
			panicOn(err)
			if s == survivor {
				if !exists {
					panic(fmt.Sprintf("ARG survivor died : bit %v", v))
				}
			} else if s == victim { // victim, should have been deleted
				if exists {
					panic(fmt.Sprintf("ARG victim lived : bit %v", v))
				}
			}
		}
	}
}

func TestBadger_Max_on_many_containers(t *testing.T) {
	dbwrap, clean := mustOpenEmptyBadgerWrapper("TestBadger_Max_on_many_containers")
	defer clean()
	defer dbwrap.Close()
	index, field, view, shard := "i", "f", "v", uint64(0)

	putmeValues := []uint64{0, 2 << 16, 4 << 16}

	for _, putme := range putmeValues {
		badgerDBMustNotHaveBitvalue(dbwrap, index, field, view, shard, putme)
		badgerDBMustSetBitvalue(dbwrap, index, field, view, shard, putme)
		badgerDBMustHaveBitvalue(dbwrap, index, field, view, shard, putme)
	}

	tx := dbwrap.NewBadgerTx(!writable, index, nil)
	defer tx.Rollback()

	max, err := tx.Max(index, field, view, shard)
	panicOn(err)
	expected := putmeValues[len(putmeValues)-1]
	if max != expected {
		panic(fmt.Sprintf("expected Max() of %v but got max=%v", expected, max))
	}
}

// and the rest

func TestBadger_SetBitmap(t *testing.T) {
	dbwrap, clean := mustOpenEmptyBadgerWrapper("TestBadger_SetBitmap")
	defer clean()
	defer dbwrap.Close()
	index, field, view, shard := "i", "f", "v", uint64(0)
	tx := dbwrap.NewBadgerTx(writable, index, nil)
	bitvalue := uint64(0)
	changed, err := tx.Add(index, field, view, shard, doBatched, bitvalue)
	if changed <= 0 {
		panic("should have changed")
	}
	panicOn(err)

	exists, err := tx.Contains(index, field, view, shard, bitvalue)
	panicOn(err)
	if !exists {
		panic("ARG bitvalue was NOT SET!!!")
	}

	err = tx.Commit()
	panicOn(err)

	//
	// commited, so should be visible outside the txn
	//

	tx2 := dbwrap.NewBadgerTx(!writable, index, nil)
	exists, err = tx2.Contains(index, field, view, shard, bitvalue)
	panicOn(err)
	if !exists {
		panic("ARG bitvalue was NOT SET!!! on tx2")
	}

	n, err := tx2.Count(index, field, view, shard)
	panicOn(err)
	if n != 1 {
		panic(fmt.Sprintf("should have Count 1; instead n = %v", n))
	}
	tx2.Rollback()
}

func TestBadger_OffsetRange(t *testing.T) {
	dbwrap, clean := mustOpenEmptyBadgerWrapper("TestBadger_OffsetRange")
	defer clean()
	defer dbwrap.Close()
	index, field, view, shard := "i", "f", "v", uint64(0)
	tx := dbwrap.NewBadgerTx(writable, index, nil)

	bitvalue := uint64(1 << 20)
	changed, err := tx.Add(index, field, view, shard, doBatched, bitvalue)
	if changed <= 0 {
		panic("should have changed")
	}
	panicOn(err)

	bitvalue2 := uint64(1<<20 + 1)
	changed, err = tx.Add(index, field, view, shard, doBatched, bitvalue2)
	if changed <= 0 {
		panic("should have changed")
	}
	panicOn(err)

	exists, err := tx.Contains(index, field, view, shard, bitvalue)
	panicOn(err)
	if !exists {
		panic("ARG bitvalue was NOT SET!!!")
	}
	exists, err = tx.Contains(index, field, view, shard, bitvalue2)
	panicOn(err)
	if !exists {
		panic("ARG bitvalue2 was NOT SET!!!")
	}

	err = tx.Commit()
	panicOn(err)

	offset := uint64(0 << 20)
	start := uint64(0 << 16)
	endx := bitvalue + 1<<16

	tx2 := dbwrap.NewBadgerTx(!writable, index, nil)
	rbm2, err := tx2.OffsetRange(index, field, view, shard, offset, start, endx)
	panicOn(err)
	tx2.Rollback()

	// should see our 1M value
	s2 := bitmapAsString(rbm2)
	expect2 := "c(1048576, 1048577)"
	if s2 != expect2 {
		panic(fmt.Sprintf("s2='%v', but expected '%v'", s2, expect2))
	}

	// now offset by 2M
	offset = uint64(2 << 20)
	tx3 := dbwrap.NewBadgerTx(!writable, index, nil)
	rbm3, err := tx3.OffsetRange(index, field, view, shard, offset, start, endx)
	panicOn(err)
	tx3.Rollback()

	//expect to see 3M == 3145728
	s3 := bitmapAsString(rbm3)
	expect3 := "c(3145728, 3145729)"

	if s3 != expect3 {
		panic(fmt.Sprintf("s3='%v', but expected '%v'", s3, expect3))
	}
}

func TestBadger_Count_on_many_containers(t *testing.T) {
	dbwrap, clean := mustOpenEmptyBadgerWrapper("TestBadger_Count_on_many_containers")
	defer clean()
	defer dbwrap.Close()
	index, field, view, shard := "i", "f", "v", uint64(0)

	putmeValues := []uint64{0, 2 << 16, 4 << 16}

	for _, putme := range putmeValues {
		badgerDBMustNotHaveBitvalue(dbwrap, index, field, view, shard, putme)
		badgerDBMustSetBitvalue(dbwrap, index, field, view, shard, putme)
		badgerDBMustHaveBitvalue(dbwrap, index, field, view, shard, putme)
	}

	tx := dbwrap.NewBadgerTx(writable, index, nil)
	defer tx.Rollback()

	n, err := tx.Count(index, field, view, shard)
	panicOn(err)
	if int(n) != len(putmeValues) {
		panic(fmt.Sprintf("expected Count of %v but got n=%v", len(putmeValues), n))
	}
}

func TestBadger_Count_dense_containers(t *testing.T) {
	dbwrap, clean := mustOpenEmptyBadgerWrapper("TestBadger_Count_dense_containers")
	defer clean()
	defer dbwrap.Close()
	index, field, view, shard := "i", "f", "v", uint64(0)

	tx := dbwrap.NewBadgerTx(writable, index, nil)

	expected := 0
	// can't do more than about 100k writes per badger txn by default, so
	// have to keep this kind of small.
	// (See maxBatchCount:104857, maxBatchSize:10066329).
	for i := uint64(0); i < (1<<16)+2; i += 2 {
		changed, err := tx.Add(index, field, view, shard, doBatched, i)
		panicOn(err)
		if changed <= 0 {
			panic("wat? should have changed")
		}
		expected++
	}
	defer tx.Rollback()

	n, err := tx.Count(index, field, view, shard)
	panicOn(err)
	if int(n) != expected {
		panic(fmt.Sprintf("expected Count of %v but got n=%v", expected, n))
	}
}

func TestBadger_ContainerIterator_on_empty(t *testing.T) {
	// iterate on empty container, should not find anything.
	dbwrap, clean := mustOpenEmptyBadgerWrapper("TestBadger_ContainerIterator")
	defer clean()
	defer dbwrap.Close()
	index, field, view, shard := "i", "f", "v", uint64(0)
	tx := dbwrap.NewBadgerTx(!writable, index, nil)
	defer tx.Rollback()
	bitvalue := uint64(0)
	citer, found, err := tx.ContainerIterator(index, field, view, shard, bitvalue)
	panicOn(err)
	defer citer.Close()
	if found {
		panic("should not have found anything")
	}
	panicOn(err)
}

func TestBadger_ContainerIterator_on_one_bit(t *testing.T) {
	// set one bit, iterate.
	dbwrap, clean := mustOpenEmptyBadgerWrapper("TestBadger_ContainerIterator_on_one_bit")
	defer clean()
	defer dbwrap.Close()
	index, field, view, shard := "i", "f", "v", uint64(0)
	tx := dbwrap.NewBadgerTx(writable, index, nil)
	defer tx.Rollback()

	bitvalue := uint64(42)

	// add a bit
	changed, err := tx.Add(index, field, view, shard, doBatched, bitvalue)
	if changed <= 0 {
		panic("should have changed")
	}
	panicOn(err)

	exists, err := tx.Contains(index, field, view, shard, bitvalue)
	panicOn(err)
	if !exists {
		panic("ARG bitvalue was NOT SET!!!")
	}

	// same Tx, continues in use.

	citer, found, err := tx.ContainerIterator(index, field, view, shard, highbits(bitvalue))
	if !found {
		panic("ContainerIterator did not find the 42 bit")
	}
	panicOn(err)
	defer citer.Close()

	loopCount := 0
	for citer.Next() {
		key, container := citer.Value()
		if key != 0 {
			panic("42 should have had key 0")
		}
		if container == nil {
			panic("container was nil")
		}
		if container.N() != 1 {
			panic("put a bit in, but size of container was not 1")
		}
		if !container.Contains(lowbits(bitvalue)) {
			panic("container did not have our bitvalue!")
		}
		loopCount++
		if loopCount > 0 { // happier linter
			break
		}
	}
	if loopCount != 1 {
		panic("ContainerIterator did not return a citer that scanned our set bit")
	}
}

func TestBadger_ContainerIterator_on_one_bit_fail_to_find(t *testing.T) {
	dbwrap, clean := mustOpenEmptyBadgerWrapper("TestBadger_ContainerIterator_on_one_bit")
	defer clean()
	defer dbwrap.Close()
	index, field, view, shard := "i", "f", "v", uint64(0)
	tx := dbwrap.NewBadgerTx(writable, index, nil)
	defer tx.Rollback()

	putme := uint64(1<<16) + 3 // in the key:1 container
	searchme := putme + 1

	// add a bit
	changed, err := tx.Add(index, field, view, shard, doBatched, putme)
	if changed <= 0 {
		panic("should have changed")
	}
	panicOn(err)

	exists, err := tx.Contains(index, field, view, shard, putme)
	panicOn(err)
	if !exists {
		panic("ARG putme was NOT SET!!!")
	}

	// same Tx, continues in use.

	citer, found, err := tx.ContainerIterator(index, field, view, shard, highbits(searchme))
	if !found {
		panic("ContainerIterator did not find the searchme")
	}
	defer citer.Close()
	loopCount := 0
	for citer.Next() {
		key, container := citer.Value()
		if key != 1 {
			panic("Containeriterator searching for highbits(searchme) should not have had a bit")
		}
		if container == nil {
			panic("container was nil")
		}
		if container.N() != 1 {
			panic("put a bit in, but size of container was not 1")
		}
		if container.Contains(lowbits(searchme)) {
			panic("container should have putme but not our searchme!")
		}
		loopCount++
		// only want first pass. keep linter happy by avoiding raw break
		if loopCount > 0 {
			break
		}
	}
	panicOn(err)
}

func TestBadger_ContainerIterator_empty_iteration_loop(t *testing.T) {
	dbwrap, clean := mustOpenEmptyBadgerWrapper("TestBadger_ContainerIterator_empty_iteration_loop")
	defer clean()
	defer dbwrap.Close()
	index, field, view, shard := "i", "f", "v", uint64(0)
	tx := dbwrap.NewBadgerTx(writable, index, nil)
	defer tx.Rollback()

	putme := uint64(1<<16) + 3  // in the key:1 container
	searchme := uint64(1 << 17) // in the next container, key:2

	// add a bit
	changed, err := tx.Add(index, field, view, shard, doBatched, putme)
	if changed <= 0 {
		panic("should have changed")
	}
	panicOn(err)

	exists, err := tx.Contains(index, field, view, shard, putme)
	panicOn(err)
	if !exists {
		panic("ARG putme was NOT SET!!!")
	}

	// same Tx, continues in use.

	citer, found, err := tx.ContainerIterator(index, field, view, shard, highbits(searchme))
	panicOn(err)
	if found {
		panic("ContainerIterator found the searchme, when it should not have")
	}
	defer citer.Close()
	if citer.Next() {
		panic("expected no looping, 0 iterations, b/c started searchme past our data in putme")
	}

	// expect to see a blow up from the citer.Value() call, verify that we do.
	func() {
		defer func() {
			r := recover()
			if r == nil {
				panic("expected a panic from citer.Value() in this case")
			}
		}()
		citer.Value() // should panic
	}()

}

func TestBadger_ForEach_on_one_bit(t *testing.T) {

	dbwrap, clean := mustOpenEmptyBadgerWrapper("TestBadger_ContainerIterator_on_one_bit")
	defer clean()
	defer dbwrap.Close()
	index, field, view, shard := "i", "f", "v", uint64(0)
	tx := dbwrap.NewBadgerTx(writable, index, nil)
	defer tx.Rollback()

	bitvalue := uint64(42)

	// add a bit
	changed, err := tx.Add(index, field, view, shard, doBatched, bitvalue)
	if changed <= 0 {
		panic("should have changed")
	}
	panicOn(err)

	exists, err := tx.Contains(index, field, view, shard, bitvalue)
	panicOn(err)
	if !exists {
		panic("ARG bitvalue was NOT SET!!!")
	}

	// same Tx, continues in use.
	count := 0
	err = tx.ForEach(index, field, view, shard, func(v uint64) error {
		if v != bitvalue {
			panic(fmt.Sprintf("bitvalue corrupt got %v want %v", v, bitvalue))
		}
		count += 1
		return nil
	})
	panicOn(err)
	if count != 1 {
		panic(fmt.Sprintf("Expected single iteration got %v ", count))
	}
}

func TestBadger_RemoveContainer_one_bit_test(t *testing.T) {

	dbwrap, clean := mustOpenEmptyBadgerWrapper("TestBadger_RemoveContainer_one_bit_test")
	defer clean()
	defer dbwrap.Close()

	index, field, view, shard := "i", "f", "v", uint64(0)

	putmeValues := []uint64{0, 13, 77, 1511}

	for _, putme := range putmeValues {

		// a) delete of whole container in a seperate txn. Commit should establish the deletion.

		badgerDBMustNotHaveBitvalue(dbwrap, index, field, view, shard, putme)
		badgerDBMustSetBitvalue(dbwrap, index, field, view, shard, putme)
		badgerDBMustHaveBitvalue(dbwrap, index, field, view, shard, putme)
		badgerDBMustDeleteBitvalueContainer(dbwrap, index, field, view, shard, putme)
		badgerDBMustNotHaveBitvalue(dbwrap, index, field, view, shard, putme)

		// b) deletion + rollback on the txn should restore the deleted bit

		badgerDBMustSetBitvalue(dbwrap, index, field, view, shard, putme)
		badgerDBMustHaveBitvalue(dbwrap, index, field, view, shard, putme)

		// delete, but rollback instead of commit
		tx := dbwrap.NewBadgerTx(writable, index, nil)
		hi := highbits(putme)
		panicOn(tx.RemoveContainer(index, field, view, shard, hi))
		tx.Rollback()

		// verify that the rollback undid the deletion.
		badgerDBMustHaveBitvalue(dbwrap, index, field, view, shard, putme)

		// c) within one Tx, after delete it should be gone as viewed within the txn.
		tx = dbwrap.NewBadgerTx(writable, index, nil)
		hi = highbits(putme)

		exists, err := tx.Contains(index, field, view, shard, putme)
		panicOn(err)
		if !exists {
			panic(fmt.Sprintf("ARG putme '%v' was NOT SET!!!", putme))
		}

		panicOn(tx.RemoveContainer(index, field, view, shard, hi))

		exists, err = tx.Contains(index, field, view, shard, putme)
		panicOn(err)
		if exists {
			panic(fmt.Sprintf("ARG putme '%v' was SET even after RemoveContiner in this txn.", putme))
		}

		tx.Rollback()

		// verify that the rollback undid the deletion.
		badgerDBMustHaveBitvalue(dbwrap, index, field, view, shard, putme)
		// leave with clean slate
		badgerDBMustDeleteBitvalueContainer(dbwrap, index, field, view, shard, putme)
	}
}

func TestBadger_Remove_one_bit_test(t *testing.T) {

	dbwrap, clean := mustOpenEmptyBadgerWrapper("TestBadger_Remove_one_bit_test")
	defer clean()
	defer dbwrap.Close()

	index, field, view, shard := "i", "f", "v", uint64(0)

	putmeValues := []uint64{0, 13, 77, 1511}

	for _, putme := range putmeValues {

		// a) delete of whole container in a seperate txn. Commit should establish the deletion.

		badgerDBMustNotHaveBitvalue(dbwrap, index, field, view, shard, putme)
		badgerDBMustSetBitvalue(dbwrap, index, field, view, shard, putme)
		badgerDBMustHaveBitvalue(dbwrap, index, field, view, shard, putme)
		badgerDBMustDeleteBitvalue(dbwrap, index, field, view, shard, putme)
		badgerDBMustNotHaveBitvalue(dbwrap, index, field, view, shard, putme)

		// b) deletion + rollback on the txn should restore the deleted bit

		badgerDBMustSetBitvalue(dbwrap, index, field, view, shard, putme)
		badgerDBMustHaveBitvalue(dbwrap, index, field, view, shard, putme)

		// delete, but rollback instead of commit
		tx := dbwrap.NewBadgerTx(writable, index, nil)
		hi, lo := highbits(putme), lowbits(putme)
		_, _ = hi, lo
		_, err := tx.Remove(index, field, view, shard, hi)
		panicOn(err)
		tx.Rollback()

		// verify that the rollback undid the deletion.
		badgerDBMustHaveBitvalue(dbwrap, index, field, view, shard, putme)

		// c) within one Tx, after delete it should be gone as viewed within the txn.
		tx = dbwrap.NewBadgerTx(writable, index, nil)

		exists, err := tx.Contains(index, field, view, shard, putme)
		panicOn(err)
		if !exists {
			panic(fmt.Sprintf("ARG putme '%v' was NOT SET!!!", putme))
		}

		mustRemove(tx.Remove(index, field, view, shard, putme))

		exists, err = tx.Contains(index, field, view, shard, putme)
		panicOn(err)
		if exists {
			panic(fmt.Sprintf("ARG putme '%v' was SET even after Remove in this txn.", putme))
		}

		tx.Rollback()

		// verify that the rollback undid the deletion.
		badgerDBMustHaveBitvalue(dbwrap, index, field, view, shard, putme)
		// leave with clean slate
		badgerDBMustDeleteBitvalueContainer(dbwrap, index, field, view, shard, putme)
	}
}

func TestBadger_reverse_badger_iterator(t *testing.T) {

	// sanity check our understanding of Seek()-ing on reverse iterators.
	dbwrap, clean := mustOpenEmptyBadgerWrapper("TestBadger_reverse_badger_iterator")
	defer clean()
	defer dbwrap.Close()

	// add 0, 1, 2 to badgerdb as keys (and same value).
	err := dbwrap.db.Update(func(txn *badger.Txn) error {
		for i := 0; i < 3; i++ {
			kv := []byte(fmt.Sprintf("a:%v", i))
			err := txn.Set(kv, kv)
			panicOn(err)
		}
		return nil
	})
	panicOn(err)

	tx := dbwrap.db.NewTransaction(!writable)

	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false // else by default, pre-fetches the 1st 100 values, which would be slow.
	opts.Reverse = true
	it := tx.NewIterator(opts)
	it.Rewind()
	if !it.Valid() {
		panic("invalid reversed iterator?")
	}
	a := []byte("a:3")
	it.Seek(a)
	if !it.Valid() {
		panic("invalid reversed iterator after seek")
	}

	it.Next()
	if !it.Valid() {
		panic("invalid reversed iterator after seek and next")
	}
	item := it.Item()

	err = item.Value(func(val []byte) error {
		// This func with val would only be called if item.Value encounters no error.
		if string(val) != "a:1" {
			panic(fmt.Sprintf("we are in trouble, should have gotten 'a:1' but instead got '%v'", string(val)))
		}
		return nil
	})
	panicOn(err)
}

func TestBadger_Min_on_many_containers(t *testing.T) {
	dbwrap, clean := mustOpenEmptyBadgerWrapper("TestBadger_Min_on_many_containers")
	defer clean()
	defer dbwrap.Close()
	index, field, view, shard := "i", "f", "v", uint64(0)

	// verify no containers flag works
	tx := dbwrap.NewBadgerTx(!writable, index, nil)
	min, containersExist, err := tx.Min(index, field, view, shard)
	_ = min
	panicOn(err)
	if containersExist {
		panic("no containers should exist")
	}
	tx.Rollback()

	putmeValues := []uint64{3, 2 << 16, 4 << 16}

	for _, putme := range putmeValues {
		badgerDBMustNotHaveBitvalue(dbwrap, index, field, view, shard, putme)
		badgerDBMustSetBitvalue(dbwrap, index, field, view, shard, putme)
		badgerDBMustHaveBitvalue(dbwrap, index, field, view, shard, putme)
	}

	tx = dbwrap.NewBadgerTx(!writable, index, nil)
	defer tx.Rollback()

	min, containersExist, err = tx.Min(index, field, view, shard)
	panicOn(err)
	if !containersExist {
		panic("containers should exist")
	}
	expected := putmeValues[0]
	if min != expected {
		panic(fmt.Sprintf("expected Min() of %v but got min=%v", expected, min))
	}
}

func TestBadger_CountRange_on_many_containers(t *testing.T) {
	dbwrap, clean := mustOpenEmptyBadgerWrapper("TestBadger_CountRange_on_many_containers")
	defer clean()
	defer dbwrap.Close()
	index, field, view, shard := "i", "f", "v", uint64(0)

	// verify no containers flag works
	tx := dbwrap.NewBadgerTx(!writable, index, nil)
	n, err := tx.CountRange(index, field, view, shard, 0, math.MaxUint64)
	panicOn(err)
	if n != 0 {
		panic("no containers should exist")
	}
	tx.Rollback()

	putmeValues := []uint64{3, 2 << 16, 4 << 16}

	for _, putme := range putmeValues {
		badgerDBMustNotHaveBitvalue(dbwrap, index, field, view, shard, putme)
		badgerDBMustSetBitvalue(dbwrap, index, field, view, shard, putme)
		badgerDBMustHaveBitvalue(dbwrap, index, field, view, shard, putme)
	}

	tx = dbwrap.NewBadgerTx(!writable, index, nil)
	defer tx.Rollback()

	n, err = tx.CountRange(index, field, view, shard, 0, math.MaxUint64)
	panicOn(err)
	if n == 0 {
		panic("containers should exist")
	}
	expected := uint64(len(putmeValues))
	if n != expected {
		panic(fmt.Sprintf("expected CountRange() of %v but got n=%v", expected, n))
	}
}

func TestBadger_CountRange_middle_container(t *testing.T) {
	dbwrap, clean := mustOpenEmptyBadgerWrapper("TestBadger_CountRange_middle_container")
	defer clean()
	defer dbwrap.Close()
	index, field, view, shard := "i", "f", "v", uint64(0)

	putmeValues := []uint64{3, 2 << 16, 4 << 16}

	for _, putme := range putmeValues {
		badgerDBMustNotHaveBitvalue(dbwrap, index, field, view, shard, putme)
		badgerDBMustSetBitvalue(dbwrap, index, field, view, shard, putme)
		badgerDBMustHaveBitvalue(dbwrap, index, field, view, shard, putme)
	}

	tx := dbwrap.NewBadgerTx(!writable, index, nil)
	defer tx.Rollback()

	// pick out just the middle container with the 1 bit set on it.
	n, err := tx.CountRange(index, field, view, shard, 4, (2<<16)+1)
	panicOn(err)
	if n != 1 {
		panic("middle 1 bit container should exist")
	}
}

func TestBadger_CountRange_many_middle_container(t *testing.T) {
	dbwrap, clean := mustOpenEmptyBadgerWrapper("TestBadger_CountRange_many_middle_container")
	defer clean()
	defer dbwrap.Close()
	index, field, view, shard := "i", "f", "v", uint64(0)

	putmeValues := []uint64{3, 2 << 16, 4 << 16}

	for _, putme := range putmeValues {
		badgerDBMustNotHaveBitvalue(dbwrap, index, field, view, shard, putme)
		badgerDBMustSetBitvalue(dbwrap, index, field, view, shard, putme)
		badgerDBMustHaveBitvalue(dbwrap, index, field, view, shard, putme)
	}

	tx := dbwrap.NewBadgerTx(!writable, index, nil)
	defer tx.Rollback()

	// get them all
	n, err := tx.CountRange(index, field, view, shard, 0, (4<<16)+1)
	panicOn(err)
	if n != 3 {
		panic("count should have been all 3 bits")
	}
}

func TestBadger_UnionInPlace(t *testing.T) {

	dbwrap, clean := mustOpenEmptyBadgerWrapper("TestBadger_UnionInPlace")
	defer clean()
	defer dbwrap.Close()
	index, field, view, shard := "i", "f", "v", uint64(0)

	putmeValues := []uint64{3, 2 << 16}

	others := roaring.NewBitmap()
	others2 := roaring.NewBitmap()
	others3 := roaring.NewBitmap()
	// populate others with putmeValues +1 into others

	for _, putme := range putmeValues {
		badgerDBMustNotHaveBitvalue(dbwrap, index, field, view, shard, putme)
		badgerDBMustSetBitvalue(dbwrap, index, field, view, shard, putme)
		badgerDBMustHaveBitvalue(dbwrap, index, field, view, shard, putme)
	}

	tx2 := dbwrap.NewBadgerTx(!writable, index, nil)
	n, err := tx2.Count(index, field, view, shard)
	panicOn(err)
	if n != 2 {
		panic("should have 2 bits set")
	}
	tx2.Rollback()

	for _, putme := range putmeValues {
		mustAddR(others.Add(putme)) // should not change count, b/c putme already in the rbm
		mustAddR(others.Add(putme + 1))
		mustAddR(others2.Add(putme + 2))
	}
	mustAddR(others3.Add(4 << 16)) // outside the 2<<16 container

	tx := dbwrap.NewBadgerTx(writable, index, nil)
	defer tx.Rollback()
	err = tx.UnionInPlace(index, field, view, shard, others, others2, others3)
	panicOn(err)

	// end game, check we got the union.
	rbm, err := tx.RoaringBitmap(index, field, view, shard)
	panicOn(err)
	n = rbm.Count()
	if n != 7 {
		panic("should have a total 3 + 3 +1 = 7 bits set on the containers")
	}
}

func TestBadger_RoaringBitmap(t *testing.T) {

	dbwrap, clean := mustOpenEmptyBadgerWrapper("TestBadger_RoaringBitmap")
	defer clean()
	defer dbwrap.Close()
	index, field, view, shard := "i", "f", "v", uint64(0)

	expected := uint64(3)
	putme := expected
	badgerDBMustSetBitvalue(dbwrap, index, field, view, shard, putme)

	tx := dbwrap.NewBadgerTx(!writable, index, nil)
	defer tx.Rollback()

	rbm, err := tx.RoaringBitmap(index, field, view, shard)
	panicOn(err)

	slc := rbm.Slice()
	if slc[0] != uint64(expected) {
		panic(fmt.Sprintf("should have gotten %v back", expected))
	}
}

func TestBadger_reverse_badger_iterator_and_prefix_valid(t *testing.T) {

	// does a reverse iterator and ValidForPrefix behave like we expect it too?
	dbwrap, clean := mustOpenEmptyBadgerWrapper("TestBadger_reverse_badger_iterator_and_prefix_valid")
	defer clean()
	defer dbwrap.Close()

	// add 0, 1, 2 to badgerdb as keys (and same value).
	err := dbwrap.db.Update(func(txn *badger.Txn) error {
		for _, prefix := range []string{"a", "b", "c"} {
			for i := 0; i < 3; i++ {
				kv := []byte(fmt.Sprintf("%v:%v", prefix, i))
				err := txn.Set(kv, kv)
				panicOn(err)
			}
		}
		return nil
	})
	panicOn(err)

	tx := dbwrap.NewBadgerTx(!writable, "no-index-avail", nil)

	prefix := []byte("b:")
	it := NewBadgerIterator(tx, prefix)

	if !it.it.Valid() {
		panic("why is underlying badger it not valid here?")
	}
	results := ""
	for it.Next() {
		item := it.it.Item()
		sk := string(item.Key())
		results += sk + ", "
	}
	expected := `b:0, b:1, b:2, `
	if results != expected {
		panic(fmt.Sprintf("observed: '%v' but expected: '%v'", results, expected))
	}
	it.Close()

	// now reversed
	seekto := []byte("c:")
	rit := NewBadgerReverseIterator(tx, prefix, seekto) // Seeks("b:") goes to b:0
	defer rit.Close()

	if !rit.it.Valid() {
		panic("why is underlying badger it not valid here?")
	}
	results = ""
	for rit.Next() {
		item := rit.it.Item()
		sk := string(item.Key())
		results += sk + ", "
	}
	expected = `b:2, b:1, b:0, `
	if results != expected {
		panic(fmt.Sprintf("observed: '%v' but expected: '%v'", results, expected))
	}
	rit.Close()
}

func TestBadger_just_reverse_badger_iterator_and_prefix_valid(t *testing.T) {

	// does a reverse iterator and ValidForPrefix behave like we expect it too?
	dbwrap, clean := mustOpenEmptyBadgerWrapper("TestBadger_reverse_badger_iterator_and_prefix_valid")
	defer clean()
	defer dbwrap.Close()

	// add 0, 1, 2 to badgerdb as keys (and same value).
	err := dbwrap.db.Update(func(txn *badger.Txn) error {
		for _, prefix := range []string{"a", "b", "c"} {
			for i := 0; i < 3; i++ {
				kv := []byte(fmt.Sprintf("%v:%v", prefix, i))
				err := txn.Set(kv, kv)
				panicOn(err)
			}
		}
		return nil
	})
	panicOn(err)

	tx := dbwrap.NewBadgerTx(!writable, "no-index-avail", nil)

	seekto := []byte("c:")
	prefix := []byte("b:")
	// now reversed
	rit := NewBadgerReverseIterator(tx, prefix, seekto) // Seeks("b:") goes to b:0
	defer rit.Close()

	if !rit.it.Valid() {
		panic("why is underlying badger rit not valid here?")
	}
	results := ""
	for rit.Next() {
		item := rit.it.Item()
		sk := string(item.Key())
		results += sk + ", "
	}
	expected := `b:2, b:1, b:0, `
	if results != expected {
		panic(fmt.Sprintf("observed: '%v' but expected: '%v'", results, expected))
	}
	rit.Close()
}

func TestBadger_ImportRoaringBits(t *testing.T) {

	dbwrap, clean := mustOpenEmptyBadgerWrapper("TestBadger_ImportRoaringBits")
	defer clean()
	defer dbwrap.Close()
	index, field, view, shard := "i", "f", "v", uint64(0)
	tx := dbwrap.NewBadgerTx(writable, index, nil)
	defer tx.Rollback()
	tx.DeleteEmptyContainer = true // traditional badger Tx behavior, but not Roaring.

	//bitvalue := uint64(42)

	// get some roaring bits, get an itr RoaringIterator from them
	rowSize := uint64(0)
	//bits := []uint64{0}
	bits := []uint64{0, 2, 5, 1<<16 + 1, 2 << 16}
	data := getTestBitmapAsRawRoaring(bits...)
	itr, err := roaring.NewRoaringIterator(data)
	panicOn(err)
	clear := false
	logme := false

	changed, rowSet, err := tx.ImportRoaringBits(index, field, view, shard, itr, clear, logme, rowSize, nil)
	_ = rowSet
	if changed != len(bits) {
		panic(fmt.Sprintf("should have changed %v bits: changed='%v', rowSet='%#v', err='%v'", len(bits), changed, rowSet, err))
	}
	panicOn(err)

	for _, v := range bits {
		exists, err := tx.Contains(index, field, view, shard, v)
		panicOn(err)
		if !exists {
			panic(fmt.Sprintf("ARG bitvalue was NOT SET!!! '%v'", v))
		}
	}

	// now test the union in place with the same set gives no change.

	changed, rowSet, err = tx.ImportRoaringBits(index, field, view, shard, itr, clear, logme, rowSize, nil)
	_ = rowSet
	if changed != 0 {
		panic(fmt.Sprintf("should have not changed any bits on the second import, but we see changed='%v', rowSet='%#v', err='%v'", changed, rowSet, err))
	}
	panicOn(err)

	for _, v := range bits {
		exists, err := tx.Contains(index, field, view, shard, v)
		panicOn(err)
		if !exists {
			panic(fmt.Sprintf("ARG bitvalue was NOT SET!!! '%v'", v))
		}
	}

	// now test the clear path
	clear = true

	for _, v := range bits {
		// clear 1 bit at a time
		data := getTestBitmapAsRawRoaring(v)
		itr, err := roaring.NewRoaringIterator(data)
		panicOn(err)

		changed, rowSet, err := tx.ImportRoaringBits(index, field, view, shard, itr, clear, logme, rowSize, nil)
		_ = rowSet
		if changed != 1 {
			panic(fmt.Sprintf("should have changed 1 bit: '%v', rowSet='%#v', err='%v'", changed, rowSet, err))
		}
		panicOn(err)
	}
	n, err := tx.Count(index, field, view, shard)
	panicOn(err)
	if n != 0 {
		panic(fmt.Sprintf("n = %v not zero so the clearbits didn't happen!", n))
	}
	allkeys := stringifiedBadgerKeysTx(tx)

	// should have no keys
	if allkeys != "" {
		panic("badger should have no keys now")
	}
}

func TestBadger_ImportRoaringBits_set_nonoverlapping_bits(t *testing.T) {

	dbwrap, clean := mustOpenEmptyBadgerWrapper("TestBadger_ImportRoaringBits_set_nonoverlapping_bits")
	defer clean()
	defer dbwrap.Close()
	index, field, view, shard := "i", "f", "v", uint64(0)
	tx := dbwrap.NewBadgerTx(writable, index, nil)
	defer tx.Rollback()

	// get some roaring bits, get an itr RoaringIterator from them
	rowSize := uint64(0)
	//bits := []uint64{0}
	bits := []uint64{0, 2, 1 << 16, 1<<16 + 2}
	data := getTestBitmapAsRawRoaring(bits...)
	itr, err := roaring.NewRoaringIterator(data)
	panicOn(err)

	bits2 := []uint64{1, 2, 3, 1<<16 + 1, 1<<16 + 2, 1<<16 + 3} //, 5, 1<<16 + 1, 2 << 16}
	data2 := getTestBitmapAsRawRoaring(bits2...)
	itr2, err := roaring.NewRoaringIterator(data2)
	panicOn(err)

	clear := false
	logme := false

	changed, rowSet, err := tx.ImportRoaringBits(index, field, view, shard, itr, clear, logme, rowSize, nil)
	_ = rowSet
	if changed != len(bits) {
		panic(fmt.Sprintf("should have changed %v bits: changed='%v', rowSet='%#v', err='%v'", len(bits), changed, rowSet, err))
	}
	panicOn(err)

	for _, v := range bits {
		exists, err := tx.Contains(index, field, view, shard, v)
		panicOn(err)
		if !exists {
			panic(fmt.Sprintf("ARG bitvalue was NOT SET!!! '%v'", v))
		}
	}

	// now import the 2nd, overlapping set and set them.

	changed, rowSet, err = tx.ImportRoaringBits(index, field, view, shard, itr2, clear, logme, rowSize, nil)
	_ = rowSet
	if changed != 4 {
		panic(fmt.Sprintf("should have changed 2 bits: the 1 and the 3, but we see changed='%v', rowSet='%#v', err='%v'", changed, rowSet, err))
	}
	panicOn(err)
}

func TestBadger_ImportRoaringBits_clear_nonoverlapping_bits(t *testing.T) {

	dbwrap, clean := mustOpenEmptyBadgerWrapper("TestBadger_ImportRoaringBits_clear_nonoverlapping_bits")
	defer clean()
	defer dbwrap.Close()
	index, field, view, shard := "i", "f", "v", uint64(0)
	tx := dbwrap.NewBadgerTx(writable, index, nil)
	defer tx.Rollback()

	// get some roaring bits, get an itr RoaringIterator from them
	rowSize := uint64(0)
	//bits := []uint64{0}
	bits := []uint64{0, 2, 1 << 16, 1<<16 + 2} //, 5, 1<<16 + 1, 2 << 16}
	data := getTestBitmapAsRawRoaring(bits...)
	itr, err := roaring.NewRoaringIterator(data)
	panicOn(err)

	bits2 := []uint64{1, 2, 3, 1<<16 + 1, 1<<16 + 2, 1<<16 + 3} //, 5, 1<<16 + 1, 2 << 16}
	data2 := getTestBitmapAsRawRoaring(bits2...)
	itr2, err := roaring.NewRoaringIterator(data2)
	panicOn(err)

	clear := false
	logme := false

	changed, rowSet, err := tx.ImportRoaringBits(index, field, view, shard, itr, clear, logme, rowSize, nil)
	_ = rowSet
	if changed != len(bits) {
		panic(fmt.Sprintf("should have changed %v bits: changed='%v', rowSet='%#v', err='%v'", len(bits), changed, rowSet, err))
	}
	panicOn(err)

	for _, v := range bits {
		exists, err := tx.Contains(index, field, view, shard, v)
		panicOn(err)
		if !exists {
			panic(fmt.Sprintf("ARG bitvalue was NOT SET!!! '%v'", v))
		}
	}

	// now import the 2nd overlapping set and clear them.
	clear = true

	changed, rowSet, err = tx.ImportRoaringBits(index, field, view, shard, itr2, clear, logme, rowSize, nil)
	_ = rowSet
	if changed != 2 {
		panic(fmt.Sprintf("should have changed 1 bit: the 2, but we see changed='%v', rowSet='%#v', err='%v'", changed, rowSet, err))
	}
	panicOn(err)

	n, err := tx.Count(index, field, view, shard)
	panicOn(err)
	if n != 2 { // just the 0 and the 1<<16 bits should be left set.
		panic(fmt.Sprintf("n = %v not 2 so the clearbits didn't happen!", n))
	}

}

func getTestBitmapAsRawRoaring(bitsToSet ...uint64) []byte {
	b := roaring.NewBitmap()
	changed := b.DirectAddN(bitsToSet...)
	n := len(bitsToSet)
	if changed != n {
		panic(fmt.Sprintf("changed=%v but bitsToSet len = %v", changed, n))
	}
	buf := bytes.NewBuffer(make([]byte, 0, 100000))
	_, err := b.WriteTo(buf)
	if err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func TestBadger_AutoCommit(t *testing.T) {

	// setup
	dbwrap, clean := mustOpenEmptyBadgerWrapper("TestBadger_AutoCommit")
	defer clean()
	defer dbwrap.Close()

	index, field, view, shard := "i", "f", "v", uint64(0)
	tx := dbwrap.NewBadgerTx(writable, index, nil)

	// if we go over 100K writes, we should autocommit
	// rather than panic.
	for v := 0; v < 133444; v++ {
		changed, err := tx.Add(index, field, view, shard, doBatched, uint64(v))
		if changed <= 0 {
			panic("should have changed")
		}
		panicOn(err)
	}

	err := tx.Commit()
	panicOn(err)
}

func TestBadger_BigWritesAvoidTxnTooLargeWithAutoCommit(t *testing.T) {

	// setup
	dbwrap, clean := mustOpenEmptyBadgerWrapper("TestBadger_BigWritesAvoidTxnTooLargeWithAutoCommit")
	defer clean()
	defer dbwrap.Close()

	index, field, view, shard := "i", "f", "v", uint64(0)
	tx := dbwrap.NewBadgerTx(writable, index, nil)

	containerKey := uint64(0)
	// setup
	bits := make([]uint64, 1024)
	n := 0
	for i := range bits {
		bits[i] = ^uint64(0)
		n += 64
	}
	rc := roaring.NewContainerBitmap(n, bits)

	// if we go over 100K big writes, we should autocommit
	// rather than panic.
	for v := 0; v < 133444; v++ {
		containerKey++
		err := tx.PutContainer(index, field, view, shard, containerKey, rc)
		panicOn(err)
	}

	err := tx.Commit()
	panicOn(err)
}

func TestBadger_DeleteIndex(t *testing.T) {

	// setup
	dbwrap, clean := mustOpenEmptyBadgerWrapper("TestBadger_DeleteIndex")
	defer clean()
	defer dbwrap.Close()
	index, field, view, shard := "i", "f", "v", uint64(0)
	tx := dbwrap.NewBadgerTx(writable, index, nil)
	bitvalue := uint64(777)
	bits := []uint64{0, 3, 1 << 16, 1<<16 + 3, 8 << 16}
	for _, v := range bits {
		changed, err := tx.Add(index, field, view, shard, doBatched, v)
		if changed <= 0 {
			panic("should have changed")
		}
		panicOn(err)
	}

	index2 := "i2" // should not be deleted, even though it shares a prefix with 'i'
	changed, err := tx.Add(index2, field, view, shard, doBatched, bitvalue)
	if changed <= 0 {
		panic("should have changed")
	}
	panicOn(err)

	for _, v := range bits {
		exists, err := tx.Contains(index, field, view, shard, v)
		panicOn(err)
		if !exists {
			panic("ARG bitvalue was NOT SET!!!")
		}
	}
	exists, err := tx.Contains(index2, field, view, shard, bitvalue)
	panicOn(err)
	if !exists {
		panic("ARG bitvalue was NOT SET!!! on index2")
	}
	err = tx.Commit()
	panicOn(err)

	// end of setup
	err = dbwrap.DeleteIndex(index)
	panicOn(err)

	tx = dbwrap.NewBadgerTx(!writable, index2, nil)
	defer tx.Rollback()
	exists, err = tx.Contains(index2, field, view, shard, bitvalue)
	panicOn(err)
	if !exists {
		panic(fmt.Sprintf("after delete of '%v', another index '%v' was gone too?!?", index, index2))
	}

	for _, v := range bits {
		exists, err = tx.Contains(index, field, view, shard, v)
		panicOn(err)
		if exists {
			allkeys := stringifiedBadgerKeysTx(tx)
			panic(fmt.Sprintf("after delete of index '%v', bit v=%v was not gone?!?; allkeys='%v'", index, v, allkeys))
		}
	}
}

func TestBadger_DeleteIndex_over100k(t *testing.T) {

	// setup
	dbwrap, clean := mustOpenEmptyBadgerWrapper("TestBadger_DeleteIndex_over100k")
	defer clean()
	defer dbwrap.Close()
	index, field, view, shard := "i", "f", "v", uint64(0)
	tx := dbwrap.NewBadgerTx(writable, index, nil)
	bitvalue := uint64(777)
	limit := uint64(100002) // default batch size in DeleteIndex is 100k keys per delete transaction.
	//limit := uint64(101)
	for v := uint64(1); v < limit; v++ {
		// shift by << 16 to get into a different shard
		changed, err := tx.Add(index, field, view, shard, doBatched, v<<16)
		if changed <= 0 {
			panic("should have changed")
		}
		panicOn(err)
		if v%100000 == 0 {
			panicOn(tx.Commit())
			tx = dbwrap.NewBadgerTx(writable, index, nil)
		}
	}

	index2 := "i2" // should not be deleted, even though it shares a prefix with 'i'
	changed, err := tx.Add(index2, field, view, shard, doBatched, bitvalue)
	if changed <= 0 {
		panic("should have changed")
	}
	panicOn(err)
	err = tx.Commit()
	panicOn(err)

	// end of setup
	err = dbwrap.DeleteIndex(index)
	panicOn(err)

	tx = dbwrap.NewBadgerTx(!writable, index2, nil)
	defer tx.Rollback()
	exists, err := tx.Contains(index2, field, view, shard, bitvalue)
	panicOn(err)
	if !exists {
		panic(fmt.Sprintf("after delete of '%v', another index '%v' was gone too?!?", index, index2))
	}

	for v := uint64(0); v < limit; v++ {
		exists, err = tx.Contains(index, field, view, shard, v<<16)
		panicOn(err)
		if exists {
			allkeys := stringifiedBadgerKeysTx(tx)
			panic(fmt.Sprintf("after delete of index '%v', bit v=%v was not gone?!?; allkeys='%v'", index, v, allkeys))
		}
	}
}

func TestBitmapDiff(t *testing.T) {
	a := roaring.NewBitmap()
	b := roaring.NewBitmap()
	err := roaringBitmapDiff(a, b)
	panicOn(err)
	err = roaringBitmapDiff(b, a)
	panicOn(err)

	a = roaring.NewBitmap(0)
	err = roaringBitmapDiff(a, b)
	if err == nil {
		panic("diff should have been noticed")
	}
	err = roaringBitmapDiff(b, a)
	if err == nil {
		panic("diff should have been noticed")
	}

	b = roaring.NewBitmap(0)
	err = roaringBitmapDiff(a, b)
	panicOn(err)
	err = roaringBitmapDiff(b, a)
	panicOn(err)

	a = roaring.NewBitmap()

	err = roaringBitmapDiff(a, b)
	if err == nil {
		panic("diff should have been noticed")
	}
	err = roaringBitmapDiff(b, a)
	if err == nil {
		panic("diff should have been noticed")
	}

	a = roaring.NewBitmap(1)

	err = roaringBitmapDiff(a, b)
	if err == nil {
		panic("diff should have been noticed")
	}
	err = roaringBitmapDiff(b, a)
	if err == nil {
		panic("diff should have been noticed")
	}

	b = roaring.NewBitmap(1, 2)
	a = roaring.NewBitmap(0, 1)

	err = roaringBitmapDiff(a, b)
	if err == nil {
		panic("diff should have been noticed")
	}
	err = roaringBitmapDiff(b, a)
	if err == nil {
		panic("diff should have been noticed")
	}

	b = roaring.NewBitmap(1, 2, 3)
	a = roaring.NewBitmap(1, 2)

	err = roaringBitmapDiff(a, b)
	if err == nil {
		panic("diff should have been noticed")
	}
	err = roaringBitmapDiff(b, a)
	if err == nil {
		panic("diff should have been noticed")
	}
}

// mustAddR is a helper for calling roaring.Container.Add() in tests to
// keep the linter happy that we are checking the error.
func mustAddR(changed bool, err error) {
	panicOn(err)
}

// mustRemove is a helper for calling Tx.Remove() in tests to
// keep the linter happy that we are checking the error.
func mustRemove(changeCount int, err error) {
	panicOn(err)
}

func TestBadger_SliceOfShards(t *testing.T) {

	dbwrap, clean := mustOpenEmptyBadgerWrapper("TestBadger_SliceOfShards")
	defer clean()
	defer dbwrap.Close()
	index, field, view := "i", "f", "v"
	shards := []uint64{0, 1, 2, 3, 1000001, 2000001}
	putme := uint64(179)
	for _, shard := range shards {
		badgerDBMustSetBitvalue(dbwrap, index, field, view, shard, putme)
	}
	tx := dbwrap.NewBadgerTx(!writable, index, nil)
	defer tx.Rollback()

	slc, err := tx.SliceOfShards(index, field, view, "")
	panicOn(err)
	for i := range shards {
		if shards[i] != slc[i] {
			panic(fmt.Sprintf("expected at i=%v that slc[i]=%v = shards[i]=%v", i, slc[i], shards[i]))
		}
	}
}

// Benchmark performance of setValue for BSI ranges.
func BenchmarkBadger_Write(b *testing.B) {

	dbwrap, clean := mustOpenEmptyBadgerWrapper("BenchmarkBadger_Write")
	//defer clean()
	_ = clean
	defer dbwrap.Close()

	putmeValues := []uint64{3, 2 << 16}
	index, field, view, shard := "i", "f", "v", uint64(0)

	for _, putme := range putmeValues {
		badgerDBMustSetBitvalue(dbwrap, index, field, view, shard, putme)
	}
	/*

		dbwrap, clean := mustOpenEmptyBadgerWrapper("BenchmarkBadger_Write")
		defer clean()
		defer dbwrap.Close()
		index, field, view, shard := "i", "f", "v", uint64(0)
		tx := dbwrap.NewBadgerTx(writable, index, nil)

		bitvalue := uint64(1 << 20)
		changed, err := tx.Add(index, field, view, shard, doBatched, bitvalue)
		if changed <= 0 {
			panic("should have changed")
		}
		panicOn(err)

		bitvalue2 := uint64(1<<20 + 1)
		changed, err = tx.Add(index, field, view, shard, doBatched, bitvalue2)
		if changed <= 0 {
			panic("should have changed")
		}
		panicOn(err)

		exists, err := tx.Contains(index, field, view, shard, bitvalue)
		panicOn(err)
		if !exists {
			panic("ARG bitvalue was NOT SET!!!")
		}
		exists, err = tx.Contains(index, field, view, shard, bitvalue2)
		panicOn(err)
		if !exists {
			panic("ARG bitvalue2 was NOT SET!!!")
		}

		err = tx.Commit()
		panicOn(err)

		offset := uint64(0 << 20)
		start := uint64(0 << 16)
		endx := bitvalue + 1<<16

		tx2 := dbwrap.NewBadgerTx(!writable, index, nil)
		rbm2, err := tx2.OffsetRange(index, field, view, shard, offset, start, endx)
		panicOn(err)
		tx2.Rollback()

		// should see our 1M value
		s2 := bitmapAsString(rbm2)
		expect2 := "c(1048576, 1048577)"
		if s2 != expect2 {
			panic(fmt.Sprintf("s2='%v', but expected '%v'", s2, expect2))
		}

		// now offset by 2M
		offset = uint64(2 << 20)
		tx3 := dbwrap.NewBadgerTx(!writable, index, nil)
		rbm3, err := tx3.OffsetRange(index, field, view, shard, offset, start, endx)
		panicOn(err)
		tx3.Rollback()

		//expect to see 3M == 3145728
		s3 := bitmapAsString(rbm3)
		expect3 := "c(3145728, 3145729)"

		if s3 != expect3 {
			panic(fmt.Sprintf("s3='%v', but expected '%v'", s3, expect3))
		}
	*/
}

func reportTestBadgersNeedingClose() {
	globalBadgerReg.mu.Lock()
	defer globalBadgerReg.mu.Unlock()
	n := len(globalBadgerReg.mp)
	if n > 0 {
		AlwaysPrintf("*** these badgers are still open (n=%v):", n)
		i := 0
		for w := range globalBadgerReg.mp {
			AlwaysPrintf("i=%v, w p=%p stack:\n%v\n\n", i, w, w.startStack)
			i++
		}
	}
}

var _ = reportTestBadgersNeedingClose // happy linter

func TestMain(m *testing.M) {
	ret := m.Run()
	//reportTestBadgersNeedingClose()
	os.Exit(ret)
}

/*
func TestBadger_ConflictWriteWriteResolution(t *testing.T) {

	// 1) when do we get write-write conflicts (probably different goroutines) but
	// can we get them on different keys?

	// 2) does having a lock registry that insures we are only ever writing
	// different keys at once avoid write-write conflicts?

	// 3) how should write-write conflicts be resolved?
	//    presumably just retying the write?

	// setup
	dbwrap, clean := mustOpenEmptyBadgerWrapper("TestBadger_ConflictWriteWriteResolution")
	defer clean()
	defer dbwrap.Close()

	concur := 10
	//bkey := []byte("a")
	//by := []byte("value-for-a")

	// read-loop:
	for i := 0; i < concur*2; i++ {
		go func() {
			tx := dbwrap.NewBadgerTx(!writable, "", nil)

			for j := 0; true; j++ {

				bkey := []byte(fmt.Sprintf("key-for-a j=%v", j))
				//by := []byte(fmt.Sprintf("value-for-a j=%v", -1))

				_, err := tx.tx.Get(bkey)
				if err != badger.ErrKeyNotFound {
					panicOn(err)
				}

				if j%10 == 0 {
					vv("committing after 10, gid=%v", curGID())
					tx.Rollback()
					tx = dbwrap.NewBadgerTx(!writable, "", nil)
				}

			}
		}()
	}

	// write-loop:
	for i := 0; i < concur; i++ {
		go func() {
			tx := dbwrap.NewBadgerTx(writable, "", nil)

			for j := 0; true; j++ {

				bkey := []byte(fmt.Sprintf("key-for-a j=%v", j))
				by := []byte(fmt.Sprintf("value-for-a j=%v", j))

				entry := badger.NewEntry(bkey, by)
				err := tx.tx.SetEntry(entry)
				panicOn(err)

				if j%5 == 0 {
					panicOn(tx.tx.Delete(bkey))
				}

				_, err = tx.tx.Get(bkey)
				if err != badger.ErrKeyNotFound {
					panicOn(err)
				}

				if j%10 == 0 {
					///vv("committing after 10, gid=%v", curGID())
					err = tx.tx.Commit()
					panicOn(err)
					tx = dbwrap.NewBadgerTx(writable, "", nil)
				}

			}
		}()
	}
	select {}
}
*/
