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

// +build amd64

package pilosa

import (
	"fmt"
	"math"
	"os"
	"testing"

	"github.com/pilosa/pilosa/v2/roaring"
)

// helpers, each runs their own new txn, and commits if a change/delete
// was made. The txn is rolled back if it is just viewing the data.

func LMDBMustHaveBitvalue(dbwrap *LMDBWrapper, index, field, view string, shard uint64, bitvalue uint64) {

	tx, _ := dbwrap.NewTx(!writable, index, Txo{})
	defer tx.Rollback()
	exists, err := tx.Contains(index, field, view, shard, bitvalue)
	panicOn(err)
	if !exists {
		panic(fmt.Sprintf("ARG bitvalue '%v' was NOT SET!!!", bitvalue))
	}

	tx.Rollback()
}

func LMDBMustNotHaveBitvalue(dbwrap *LMDBWrapper, index, field, view string, shard uint64, bitvalue uint64) {

	tx, _ := dbwrap.NewTx(!writable, index, Txo{})
	defer tx.Rollback()
	exists, err := tx.Contains(index, field, view, shard, bitvalue)
	panicOn(err)
	if exists {
		panic(fmt.Sprintf("ARG bitvalue '%v' WAS SET but should not have been.!!!", bitvalue))
	}
	tx.Rollback()
}

func LMDBMustSetBitvalue(dbwrap *LMDBWrapper, index, field, view string, shard uint64, putme uint64) {
	tx, _ := dbwrap.NewTx(writable, index, Txo{})

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

func LMDBMustDeleteBitvalueContainer(dbwrap *LMDBWrapper, index, field, view string, shard uint64, putme uint64) {
	tx, _ := dbwrap.NewTx(writable, index, Txo{})
	hi := highbits(putme)
	panicOn(tx.RemoveContainer(index, field, view, shard, hi))
	panicOn(tx.Commit())
}

func LMDBMustDeleteBitvalue(dbwrap *LMDBWrapper, index, field, view string, shard uint64, putme uint64) {
	tx, _ := dbwrap.NewTx(writable, index, Txo{})
	_, err := tx.Remove(index, field, view, shard, putme)
	panicOn(err)
	panicOn(tx.Commit())
}

func mustOpenEmptyLMDBWrapper(path string) (w *LMDBWrapper, cleaner func()) {
	var err error
	fn := lmdbPath(path)
	panicOn(os.RemoveAll(fn))
	ww, err := globalLMDBReg.OpenDBWrapper(fn, DetectMemAccessPastTx, nil)
	panicOn(err)
	w = ww.(*LMDBWrapper)

	// verify it is empty
	allkeys := w.StringifiedLMDBKeys(nil, false)
	if allkeys != "<empty lmdb database>" {
		panic(fmt.Sprintf("freshly created database was not empty! had keys:'%v'", allkeys))
	}

	return w, func() {
		w.Close()
		panicOn(os.RemoveAll(fn))
	}
}

// end of helper utilities
//////////////////////////

//////////////////////////
// begin Tx method tests

func TestLMDB_DeleteFragment(t *testing.T) {

	// setup
	dbwrap, clean := mustOpenEmptyLMDBWrapper("TestLmdb_DeleteFragment")
	defer clean()
	defer dbwrap.Close()
	index, field, view, shard0 := "i", "f", "v", uint64(0)
	tx, _ := dbwrap.NewTx(writable, index, Txo{})

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

	tx, _ = dbwrap.NewTx(!writable, index, Txo{})
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

func TestLMDB_Max_on_many_containers(t *testing.T) {
	path := "TestLMDB_Max_on_many_containers"
	dbwrap, clean := mustOpenEmptyLMDBWrapper(path)

	defer clean()
	defer dbwrap.Close()
	index, field, view := "i", "f", "v"

	// 099
	// 101
	// 199
	// 300
	// 399
	//
	// find max in [300,400) and get 399
	// find max in [000,100) and get 099
	// find max in [100,200) and get 199
	// find max in [400,500) and get nothing back
	// find max in [200,300) and get nothing back

	shards := []int{99, 101, 199, 300, 399}

	for _, sh := range shards {
		shard := uint64(sh)
		for _, pm := range shards {
			putme := uint64(pm)
			if putme > shard {
				continue
			}
			LMDBMustNotHaveBitvalue(dbwrap, index, field, view, shard, putme)
			LMDBMustSetBitvalue(dbwrap, index, field, view, shard, putme)
			LMDBMustHaveBitvalue(dbwrap, index, field, view, shard, putme)
		}
	}

	tx, _ := dbwrap.NewTx(!writable, index, Txo{})
	defer tx.Rollback()

	for _, shard := range shards {
		max, err := tx.Max(index, field, view, uint64(shard))
		panicOn(err)
		if max != uint64(shard) {
			panic(fmt.Sprintf("expected max (%v) to be == shard = %v", max, shard))
		}
	}

	// check for not found
	max, err := tx.Max(index, field, view, uint64(200))
	panicOn(err)
	if max != 0 {
		panic("expected not found to give 0 max back with nil err")
	}
	max, err = tx.Max(index, field, view, uint64(400))
	panicOn(err)
	if max != 0 {
		panic("expected not found to give 0 max back with nil err")
	}

}

// and the rest

func TestLMDB_SetBitmap(t *testing.T) {
	dbwrap, clean := mustOpenEmptyLMDBWrapper("TestLMDB_SetBitmap")
	defer clean()
	defer dbwrap.Close()
	index, field, view, shard := "i", "f", "v", uint64(0)
	tx, _ := dbwrap.NewTx(writable, index, Txo{})
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

	tx2, _ := dbwrap.NewTx(!writable, index, Txo{})
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

func TestLMDB_OffsetRange(t *testing.T) {
	dbwrap, clean := mustOpenEmptyLMDBWrapper("TestLMDB_OffsetRange")
	defer clean()
	defer dbwrap.Close()
	index, field, view, shard := "i", "f", "v", uint64(0)
	tx, _ := dbwrap.NewTx(writable, index, Txo{})

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

	tx2, _ := dbwrap.NewTx(!writable, index, Txo{})
	rbm2, err := tx2.OffsetRange(index, field, view, shard, offset, start, endx)
	panicOn(err)
	tx2.Rollback()

	// should see our 1M value
	s2 := BitmapAsString(rbm2)
	expect2 := "c(1048576, 1048577)"
	if s2 != expect2 {
		panic(fmt.Sprintf("s2='%v', but expected '%v'", s2, expect2))
	}

	// now offset by 2M
	offset = uint64(2 << 20)
	tx3, _ := dbwrap.NewTx(!writable, index, Txo{})
	rbm3, err := tx3.OffsetRange(index, field, view, shard, offset, start, endx)
	panicOn(err)
	tx3.Rollback()

	//expect to see 3M == 3145728
	s3 := BitmapAsString(rbm3)
	expect3 := "c(3145728, 3145729)"

	if s3 != expect3 {
		panic(fmt.Sprintf("s3='%v', but expected '%v'", s3, expect3))
	}
}

func TestLMDB_Count_on_many_containers(t *testing.T) {
	dbwrap, clean := mustOpenEmptyLMDBWrapper("TestLMDB_Count_on_many_containers")
	defer clean()
	defer dbwrap.Close()
	index, field, view, shard := "i", "f", "v", uint64(0)

	putmeValues := []uint64{0, 2 << 16, 4 << 16}

	for _, putme := range putmeValues {
		LMDBMustNotHaveBitvalue(dbwrap, index, field, view, shard, putme)
		LMDBMustSetBitvalue(dbwrap, index, field, view, shard, putme)
		LMDBMustHaveBitvalue(dbwrap, index, field, view, shard, putme)
	}

	tx, _ := dbwrap.NewTx(writable, index, Txo{})
	defer tx.Rollback()

	n, err := tx.Count(index, field, view, shard)
	panicOn(err)
	if int(n) != len(putmeValues) {
		panic(fmt.Sprintf("expected Count of %v but got n=%v", len(putmeValues), n))
	}
}

func TestLMDB_Count_dense_containers(t *testing.T) {
	dbwrap, clean := mustOpenEmptyLMDBWrapper("TestLMDB_Count_dense_containers")
	defer clean()
	defer dbwrap.Close()
	index, field, view, shard := "i", "f", "v", uint64(0)

	tx, _ := dbwrap.NewTx(writable, index, Txo{})

	expected := 0
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

func TestLMDB_ContainerIterator_on_empty(t *testing.T) {
	// iterate on empty container, should not find anything.
	dbwrap, clean := mustOpenEmptyLMDBWrapper("TestLMDB_ContainerIterator")
	defer clean()
	defer dbwrap.Close()
	index, field, view, shard := "i", "f", "v", uint64(0)
	tx, _ := dbwrap.NewTx(!writable, index, Txo{})
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

func TestLMDB_ContainerIterator_on_one_bit(t *testing.T) {
	// set one bit, iterate.
	dbwrap, clean := mustOpenEmptyLMDBWrapper("TestLMDB_ContainerIterator_on_one_bit")
	defer clean()
	defer dbwrap.Close()
	index, field, view, shard := "i", "f", "v", uint64(0)
	tx, _ := dbwrap.NewTx(writable, index, Txo{})
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

func TestLMDB_ContainerIterator_on_one_bit_fail_to_find(t *testing.T) {
	dbwrap, clean := mustOpenEmptyLMDBWrapper("TestLMDB_ContainerIterator_on_one_bit")
	defer clean()
	defer dbwrap.Close()
	index, field, view, shard := "i", "f", "v", uint64(0)
	tx, _ := dbwrap.NewTx(writable, index, Txo{})
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

func TestLMDB_ContainerIterator_empty_iteration_loop(t *testing.T) {
	dbwrap, clean := mustOpenEmptyLMDBWrapper("TestLMDB_ContainerIterator_empty_iteration_loop")
	defer clean()
	defer dbwrap.Close()
	index, field, view, shard := "i", "f", "v", uint64(0)
	tx, _ := dbwrap.NewTx(writable, index, Txo{})
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

func TestLMDB_ForEach_on_one_bit(t *testing.T) {

	dbwrap, clean := mustOpenEmptyLMDBWrapper("TestLMDB_ContainerIterator_on_one_bit")
	defer clean()
	defer dbwrap.Close()
	index, field, view, shard := "i", "f", "v", uint64(0)
	tx, _ := dbwrap.NewTx(writable, index, Txo{})
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

func TestLMDB_RemoveContainer_one_bit_test(t *testing.T) {

	dbwrap, clean := mustOpenEmptyLMDBWrapper("TestLMDB_RemoveContainer_one_bit_test")
	defer clean()
	defer dbwrap.Close()

	index, field, view, shard := "i", "f", "v", uint64(0)

	putmeValues := []uint64{0, 13, 77, 1511}

	for _, putme := range putmeValues {

		// a) delete of whole container in a seperate txn. Commit should establish the deletion.

		LMDBMustNotHaveBitvalue(dbwrap, index, field, view, shard, putme)
		LMDBMustSetBitvalue(dbwrap, index, field, view, shard, putme)
		LMDBMustHaveBitvalue(dbwrap, index, field, view, shard, putme)
		LMDBMustDeleteBitvalueContainer(dbwrap, index, field, view, shard, putme)
		LMDBMustNotHaveBitvalue(dbwrap, index, field, view, shard, putme)

		// b) deletion + rollback on the txn should restore the deleted bit

		LMDBMustSetBitvalue(dbwrap, index, field, view, shard, putme)
		LMDBMustHaveBitvalue(dbwrap, index, field, view, shard, putme)

		// delete, but rollback instead of commit
		tx, _ := dbwrap.NewTx(writable, index, Txo{})
		hi := highbits(putme)
		panicOn(tx.RemoveContainer(index, field, view, shard, hi))
		tx.Rollback()

		// verify that the rollback undid the deletion.
		LMDBMustHaveBitvalue(dbwrap, index, field, view, shard, putme)

		// c) within one Tx, after delete it should be gone as viewed within the txn.
		tx, _ = dbwrap.NewTx(writable, index, Txo{})
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
		LMDBMustHaveBitvalue(dbwrap, index, field, view, shard, putme)
		// leave with clean slate
		LMDBMustDeleteBitvalueContainer(dbwrap, index, field, view, shard, putme)
	}
}

func TestLMDB_Remove_one_bit_test(t *testing.T) {

	dbwrap, clean := mustOpenEmptyLMDBWrapper("TestLMDB_Remove_one_bit_test")
	defer clean()
	defer dbwrap.Close()

	index, field, view, shard := "i", "f", "v", uint64(0)

	putmeValues := []uint64{0, 13, 77, 1511}

	for _, putme := range putmeValues {

		// a) delete of whole container in a seperate txn. Commit should establish the deletion.

		LMDBMustNotHaveBitvalue(dbwrap, index, field, view, shard, putme)
		LMDBMustSetBitvalue(dbwrap, index, field, view, shard, putme)
		LMDBMustHaveBitvalue(dbwrap, index, field, view, shard, putme)
		LMDBMustDeleteBitvalue(dbwrap, index, field, view, shard, putme)
		LMDBMustNotHaveBitvalue(dbwrap, index, field, view, shard, putme)

		// b) deletion + rollback on the txn should restore the deleted bit

		LMDBMustSetBitvalue(dbwrap, index, field, view, shard, putme)
		LMDBMustHaveBitvalue(dbwrap, index, field, view, shard, putme)

		// delete, but rollback instead of commit
		tx, _ := dbwrap.NewTx(writable, index, Txo{})
		hi, lo := highbits(putme), lowbits(putme)
		_, _ = hi, lo
		_, err := tx.Remove(index, field, view, shard, hi)
		panicOn(err)
		tx.Rollback()

		// verify that the rollback undid the deletion.
		LMDBMustHaveBitvalue(dbwrap, index, field, view, shard, putme)

		// c) within one Tx, after delete it should be gone as viewed within the txn.
		tx, _ = dbwrap.NewTx(writable, index, Txo{})

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
		LMDBMustHaveBitvalue(dbwrap, index, field, view, shard, putme)
		// leave with clean slate
		LMDBMustDeleteBitvalueContainer(dbwrap, index, field, view, shard, putme)
	}
}

func TestLMDB_Min_on_many_containers(t *testing.T) {
	dbwrap, clean := mustOpenEmptyLMDBWrapper("TestLMDB_Min_on_many_containers")
	defer clean()
	defer dbwrap.Close()
	index, field, view, shard := "i", "f", "v", uint64(0)

	// verify no containers flag works
	tx, _ := dbwrap.NewTx(!writable, index, Txo{})
	min, containersExist, err := tx.Min(index, field, view, shard)
	_ = min
	panicOn(err)
	if containersExist {
		panic("no containers should exist")
	}
	tx.Rollback()

	putmeValues := []uint64{3, 2 << 16, 4 << 16}

	for _, putme := range putmeValues {
		LMDBMustNotHaveBitvalue(dbwrap, index, field, view, shard, putme)
		LMDBMustSetBitvalue(dbwrap, index, field, view, shard, putme)
		LMDBMustHaveBitvalue(dbwrap, index, field, view, shard, putme)
	}

	tx, _ = dbwrap.NewTx(!writable, index, Txo{})
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

func TestLMDB_CountRange_on_many_containers(t *testing.T) {
	dbwrap, clean := mustOpenEmptyLMDBWrapper("TestLMDB_CountRange_on_many_containers")
	defer clean()
	defer dbwrap.Close()
	index, field, view, shard := "i", "f", "v", uint64(0)

	// verify no containers flag works
	tx, _ := dbwrap.NewTx(!writable, index, Txo{})
	n, err := tx.CountRange(index, field, view, shard, 0, math.MaxUint64)
	panicOn(err)
	if n != 0 {
		panic("no containers should exist")
	}
	tx.Rollback()

	putmeValues := []uint64{3, 2 << 16, 4 << 16}

	for _, putme := range putmeValues {
		LMDBMustNotHaveBitvalue(dbwrap, index, field, view, shard, putme)
		LMDBMustSetBitvalue(dbwrap, index, field, view, shard, putme)
		LMDBMustHaveBitvalue(dbwrap, index, field, view, shard, putme)
	}

	tx, _ = dbwrap.NewTx(!writable, index, Txo{})
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

func TestLMDB_CountRange_middle_container(t *testing.T) {
	dbwrap, clean := mustOpenEmptyLMDBWrapper("TestLMDB_CountRange_middle_container")
	defer clean()
	defer dbwrap.Close()
	index, field, view, shard := "i", "f", "v", uint64(0)

	putmeValues := []uint64{3, 2 << 16, 4 << 16}

	for _, putme := range putmeValues {
		LMDBMustNotHaveBitvalue(dbwrap, index, field, view, shard, putme)
		LMDBMustSetBitvalue(dbwrap, index, field, view, shard, putme)
		LMDBMustHaveBitvalue(dbwrap, index, field, view, shard, putme)
	}

	tx, _ := dbwrap.NewTx(!writable, index, Txo{})
	defer tx.Rollback()

	// pick out just the middle container with the 1 bit set on it.
	n, err := tx.CountRange(index, field, view, shard, 4, (2<<16)+1)
	panicOn(err)
	if n != 1 {
		panic("middle 1 bit container should exist")
	}
}

func TestLMDB_CountRange_many_middle_container(t *testing.T) {
	dbwrap, clean := mustOpenEmptyLMDBWrapper("TestLMDB_CountRange_many_middle_container")
	defer clean()
	defer dbwrap.Close()
	index, field, view, shard := "i", "f", "v", uint64(0)

	putmeValues := []uint64{3, 2 << 16, 4 << 16}

	for _, putme := range putmeValues {
		LMDBMustNotHaveBitvalue(dbwrap, index, field, view, shard, putme)
		LMDBMustSetBitvalue(dbwrap, index, field, view, shard, putme)
		LMDBMustHaveBitvalue(dbwrap, index, field, view, shard, putme)
	}

	tx, _ := dbwrap.NewTx(!writable, index, Txo{})
	defer tx.Rollback()

	// get them all
	n, err := tx.CountRange(index, field, view, shard, 0, (4<<16)+1)
	panicOn(err)
	if n != 3 {
		panic("count should have been all 3 bits")
	}
}

func TestLMDB_UnionInPlace(t *testing.T) {

	dbwrap, clean := mustOpenEmptyLMDBWrapper("TestLMDB_UnionInPlace")
	defer clean()
	defer dbwrap.Close()
	index, field, view, shard := "i", "f", "v", uint64(0)

	putmeValues := []uint64{3, 2 << 16}

	others := roaring.NewBitmap()
	others2 := roaring.NewBitmap()
	others3 := roaring.NewBitmap()
	// populate others with putmeValues +1 into others

	for _, putme := range putmeValues {
		LMDBMustNotHaveBitvalue(dbwrap, index, field, view, shard, putme)
		LMDBMustSetBitvalue(dbwrap, index, field, view, shard, putme)
		LMDBMustHaveBitvalue(dbwrap, index, field, view, shard, putme)
	}

	tx2, _ := dbwrap.NewTx(!writable, index, Txo{})
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

	tx, _ := dbwrap.NewTx(writable, index, Txo{})
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

func TestLMDB_RoaringBitmap(t *testing.T) {

	dbwrap, clean := mustOpenEmptyLMDBWrapper("TestLMDB_RoaringBitmap")
	defer clean()
	defer dbwrap.Close()
	index, field, view, shard := "i", "f", "v", uint64(0)

	expected := uint64(3)
	putme := expected
	LMDBMustSetBitvalue(dbwrap, index, field, view, shard, putme)

	tx, _ := dbwrap.NewTx(!writable, index, Txo{})
	defer tx.Rollback()

	rbm, err := tx.RoaringBitmap(index, field, view, shard)
	panicOn(err)

	slc := rbm.Slice()
	if slc[0] != uint64(expected) {
		panic(fmt.Sprintf("should have gotten %v back", expected))
	}
}

func TestLMDB_ImportRoaringBits(t *testing.T) {

	dbwrap, clean := mustOpenEmptyLMDBWrapper("TestLMDB_ImportRoaringBits")
	defer clean()
	defer dbwrap.Close()
	index, field, view, shard := "i", "f", "v", uint64(0)
	tx, _ := dbwrap.NewTx(writable, index, Txo{})
	defer tx.Rollback()
	tx.(*LMDBTx).DeleteEmptyContainer = true // match Roaring

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
	allkeys := stringifiedLMDBKeysTx(tx.(*LMDBTx), false)

	// should have no keys
	if allkeys != "<empty lmdb database>" {
		panic("lmdb should have no keys now")
	}
}

func TestLMDB_ImportRoaringBits_set_nonoverlapping_bits(t *testing.T) {

	dbwrap, clean := mustOpenEmptyLMDBWrapper("TestLMDB_ImportRoaringBits_set_nonoverlapping_bits")
	defer clean()
	defer dbwrap.Close()
	index, field, view, shard := "i", "f", "v", uint64(0)
	tx, _ := dbwrap.NewTx(writable, index, Txo{})
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

func TestLMDB_ImportRoaringBits_clear_nonoverlapping_bits(t *testing.T) {

	dbwrap, clean := mustOpenEmptyLMDBWrapper("TestLMDB_ImportRoaringBits_clear_nonoverlapping_bits")
	defer clean()
	defer dbwrap.Close()
	index, field, view, shard := "i", "f", "v", uint64(0)
	tx, _ := dbwrap.NewTx(writable, index, Txo{})
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

func TestLMDB_DeleteIndex(t *testing.T) {

	// setup
	dbwrap, clean := mustOpenEmptyLMDBWrapper("TestLMDB_DeleteIndex")
	defer clean()
	defer dbwrap.Close()
	index, field, view, shard := "i", "f", "v", uint64(0)
	tx, _ := dbwrap.NewTx(writable, index, Txo{})
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

	tx, _ = dbwrap.NewTx(!writable, index2, Txo{})
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
			allkeys := stringifiedLMDBKeysTx(tx.(*LMDBTx), false)
			panic(fmt.Sprintf("after delete of index '%v', bit v=%v was not gone?!?; allkeys='%v'", index, v, allkeys))
		}
	}
}

func TestLMDB_DeleteIndex_over100k(t *testing.T) {

	// setup
	dbwrap, clean := mustOpenEmptyLMDBWrapper("TestLMDB_DeleteIndex_over100k")
	defer clean()
	defer dbwrap.Close()
	index, field, view, shard := "i", "f", "v", uint64(0)
	tx, _ := dbwrap.NewTx(writable, index, Txo{})
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
			tx, _ = dbwrap.NewTx(writable, index, Txo{})
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

	tx, _ = dbwrap.NewTx(!writable, index2, Txo{})
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
			allkeys := stringifiedLMDBKeysTx(tx.(*LMDBTx), false)
			panic(fmt.Sprintf("after delete of index '%v', bit v=%v was not gone?!?; allkeys='%v'", index, v, allkeys))
		}
	}
}

func TestLMDB_SliceOfShards(t *testing.T) {

	dbwrap, clean := mustOpenEmptyLMDBWrapper("TestLMDB_SliceOfShards")
	defer clean()
	defer dbwrap.Close()
	index, field, view := "i", "f", "v"
	shards := []uint64{0, 1, 2, 3, 1000001, 2000001}
	putme := uint64(179)
	for _, shard := range shards {
		LMDBMustSetBitvalue(dbwrap, index, field, view, shard, putme)
	}
	tx, _ := dbwrap.NewTx(!writable, index, Txo{})
	defer tx.Rollback()

	slc, err := tx.SliceOfShards(index, field, view, "")
	panicOn(err)
	for i := range shards {
		if shards[i] != slc[i] {
			panic(fmt.Sprintf("expected at i=%v that slc[i]=%v = shards[i]=%v", i, slc[i], shards[i]))
		}
	}
}

func TestLMDB_HasData(t *testing.T) {

	db, clean := mustOpenEmptyLMDBWrapper("TestLMDB_SliceOfShards")
	defer clean()
	defer db.Close()

	// HasData should start out false.
	hasAnything, err := db.HasData()
	if err != nil {
		t.Fatal(err)
	}
	if hasAnything {
		t.Fatalf("HasData reported existing data on an empty database")
	}

	// check that HasData sees a committed record.

	index, field, view, shard, putme := "i", "f", "v", uint64(123), uint64(42)
	LMDBMustSetBitvalue(db, index, field, view, shard, putme)

	// HasData(false) should now report data
	hasAnything, err = db.HasData()
	if err != nil {
		t.Fatal(err)
	}
	if !hasAnything {
		t.Fatalf("HasData() reported no data on a database that has bits written to it")
	}
}
