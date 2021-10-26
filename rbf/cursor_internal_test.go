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

package rbf

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/molecula/featurebase/v2/roaring"
	. "github.com/molecula/featurebase/v2/vprint" // nolint:staticcheck
)

func getRoaringIter(bitsToSet ...uint64) roaring.RoaringIterator {

	b := roaring.NewBitmap()
	changed := b.DirectAddN(bitsToSet...)
	n := len(bitsToSet)
	if changed != n {
		PanicOn(fmt.Sprintf("changed=%v but bitsToSet len = %v", changed, n))
	}
	buf := bytes.NewBuffer(make([]byte, 0, 100000))
	_, err := b.WriteTo(buf)
	if err != nil {
		PanicOn(err)
	}
	itr, err := roaring.NewRoaringIterator(buf.Bytes())
	PanicOn(err)
	return itr
}

func TestCursor_RoaringImport(t *testing.T) {

	itr := getRoaringIter([]uint64{1}...)

	db := testHelperMustOpenNewDB(t)
	defer MustCloseDB(t, db)

	index := "i"
	field := "f"
	view := "v"
	shard := uint64(0)
	name := rbfName(index, field, view, shard)
	clear := false
	rowSize := uint64(0)

	tx := MustBegin(t, db, true)
	defer tx.Rollback()

	changed, rowSet, err := tx.ImportRoaringBits(name, itr, clear, false, rowSize)
	PanicOn(err) // writeTheTailOfLeafCellsFromIter has to be AFTER any pre-existing data. ckey=0 was found already in db.
	_ = rowSet
	if changed != 1 {
		t.Fatalf("expected 1 changed, got %v", changed)
	}
}

func TestCursor_RoaringImport_clear_bits(t *testing.T) {

	itr := getRoaringIter([]uint64{1}...)

	db := testHelperMustOpenNewDB(t)
	defer MustCloseDB(t, db)

	index := "i"
	field := "f"
	view := "v"
	shard := uint64(0)
	name := rbfName(index, field, view, shard)
	clear := false
	rowSize := uint64(0)

	tx := MustBegin(t, db, true)
	defer tx.Rollback()

	changed, rowSet, err := tx.ImportRoaringBits(name, itr, clear, false, rowSize)
	PanicOn(err)
	_ = rowSet
	if changed != 1 {
		t.Fatalf("expected 1 changed, got %v", changed)
	}

	// now clear
	clear = true
	itr2 := getRoaringIter([]uint64{1}...)

	changed, rowSet, err = tx.ImportRoaringBits(name, itr2, clear, false, rowSize)
	PanicOn(err)
	_ = rowSet
	if changed != 1 {
		t.Fatalf("expected 1 changed on clear true, got %v", changed)
	}
}

func TestCursor_RoaringImport_two_leaves(t *testing.T) {

	// make enough for 2 leaves, so then we'll have
	// to make a branch too.

	want := make([]uint64, 0, ArrayMaxSize)

	for x := uint64(0); x < 6000; x += 2 {
		want = append(want, x)
	}

	for x := uint64(0); x < 6000; x += 2 {
		want = append(want, x+ShardWidth)
	}

	itr := getRoaringIter(want...)

	db := testHelperMustOpenNewDB(t)
	defer MustCloseDB(t, db)

	index := "i"
	field := "f"
	view := "v"
	shard := uint64(0)
	name := rbfName(index, field, view, shard)
	clear := false
	rowSize := uint64(0)

	tx := MustBegin(t, db, true)
	defer tx.Rollback()

	changed, rowSet, err := tx.ImportRoaringBits(name, itr, clear, false, rowSize)
	PanicOn(err) // writeTheTailOfLeafCellsFromIter has to be AFTER any pre-existing data. ckey=0 was found already in db.
	_ = rowSet
	if changed != 6000 {
		t.Fatalf("expected 6000 bits changed, got %v", changed)
	}
}

func TestCursor_RoaringImport_many_leaves_manual_split(t *testing.T) {

	// does the right split:

	// make enough for so many leaves that we have to
	// make a branch too. so 513 or more cells, because
	// 512 16-byte branchCells should
	//biggerFactor := 2
	NbranchCells := int(maxBranchCellsPerPage) + 1 // * biggerFactor

	want := make([]uint64, 0, ArrayMaxSize)

	expectedBitsChanged := 0
	m := make(map[int]bool)
	for i := 0; i < NbranchCells; i++ {
		for x := 0; x < 6000; x += 2 {
			rowID := i
			columnID := x
			value := (rowID * ShardWidth) + (columnID % ShardWidth)
			want = append(want, uint64(value)) // x+uint64(i)*ShardWidth)
			m[value] = true
			expectedBitsChanged++
		}
	}

	itr := getRoaringIter(want[:len(want)-3000]...)
	itr2 := getRoaringIter(want[len(want)-3000:]...)

	db := testHelperMustOpenNewDB(t)
	defer MustCloseDB(t, db)

	index := "i"
	field := "f"
	view := "v"
	shard := uint64(0)
	name := rbfName(index, field, view, shard)
	clear := false
	rowSize := uint64(0)

	tx := MustBegin(t, db, true)
	defer tx.Rollback()

	//vv("DONE WITH Add()")
	changed, _, err := tx.ImportRoaringBits(name, itr, clear, false, rowSize)
	PanicOn(err) // writeTheTailOfLeafCellsFromIter has to be AFTER any pre-existing data. ckey=0 was found already in db.
	if changed != expectedBitsChanged-3000 {
		t.Fatalf("expected %v bits changed, got %v", expectedBitsChanged-3000, changed)
	}
	//vv("changed on set is %v", changed)

	//vv("about to do itr2, that starts with key %v", itr2.ContainerKeys()[0])
	changed, _, err = tx.ImportRoaringBits(name, itr2, clear, false, rowSize)
	PanicOn(err)
	if changed != 3000 {
		t.Fatalf("expected %v bits changed, got %v", 3000, changed)
	}

	// now clear
	clear = true
	//itr3 := getRoaringIter(want[len(want)-3000:]...)
	itr3 := getRoaringIter(want...)

	changed, _, err = tx.ImportRoaringBits(name, itr3, clear, false, rowSize)
	PanicOn(err)
	if changed != expectedBitsChanged {
		//     cursor_internal_test.go:235: expected 2,724,000 bits changed, got 2,721,000
		t.Fatalf("expected %v bits changed, got %v", expectedBitsChanged, changed)
	}
}

func TestCursor_RoaringImport_auto_many_leaves(t *testing.T) {

	// make enough for so many leaves that we have to
	// make a branch too. so 513 or more cells, because
	// 512 16-byte branchCells should
	//biggerFactor := 2
	NbranchCells := int(maxBranchCellsPerPage) + 1 // * biggerFactor

	want := make([]uint64, 0, ArrayMaxSize)

	expectedBitsChanged := 0
	m := make(map[int]bool)
	for i := 0; i < NbranchCells; i++ {
		for x := 0; x < 6000; x += 2 {
			rowID := i
			columnID := x
			value := (rowID * ShardWidth) + (columnID % ShardWidth)
			want = append(want, uint64(value)) // x+uint64(i)*ShardWidth)
			m[value] = true
			expectedBitsChanged++
		}
	}

	itr := getRoaringIter(want...)

	db := testHelperMustOpenNewDB(t)
	defer MustCloseDB(t, db)

	index := "i"
	field := "f"
	view := "v"
	shard := uint64(0)
	name := rbfName(index, field, view, shard)
	clear := false
	rowSize := uint64(0)

	tx := MustBegin(t, db, true)
	defer tx.Rollback()
	//vv("DONE WITH Add()")

	changed, _, err := tx.ImportRoaringBits(name, itr, clear, false, rowSize)
	PanicOn(err) // writeTheTailOfLeafCellsFromIter has to be AFTER any pre-existing data. ckey=0 was found already in db.
	if changed != expectedBitsChanged {
		t.Fatalf("expected %v bits changed, got %v", expectedBitsChanged, changed)
	}

	// now clear
	clear = true
	itr2 := getRoaringIter(want...)

	changed, _, err = tx.ImportRoaringBits(name, itr2, clear, false, rowSize)
	PanicOn(err)
	if changed != expectedBitsChanged {
		t.Fatalf("expected %v bits changed, got %v", expectedBitsChanged, changed)
	}
}

func TestCursor_putBranchCellsHandlesLotsOfNewBranchesAtTheRoot(t *testing.T) {

	const maxBranchCells = 2
	prev := globalBranchFillPct
	defer func() {
		globalBranchFillPct = prev
	}()
	// force there to be so many branch cells that the root cannot handle
	// them without making extra branch levels.
	globalBranchFillPct = float64(maxBranchCells+1) / float64(maxBranchCellsPerPage)

	biggerFactor := 3 //4 // int(maxBranchCellsPerPage) + 1
	_ = biggerFactor
	NbranchCells := 10 // (int(maxBranchCellsPerPage) + 1) * biggerFactor

	want := make([]uint64, 0, ArrayMaxSize)

	expectedBitsChanged := 0
	m := make(map[int]bool)
	for i := 0; i < NbranchCells; i++ {
		//if i%10000 == 0 {
		//vv("i = %v, NbranchCells = %v", i, NbranchCells)
		//}
		for x := 0; x < 6000; x += 2 {
			rowID := i
			columnID := x
			value := (rowID * ShardWidth) + (columnID % ShardWidth)
			want = append(want, uint64(value)) // x+uint64(i)*ShardWidth)
			m[value] = true
			expectedBitsChanged++
		}
	}

	itr := getRoaringIter(want...)

	db := testHelperMustOpenNewDB(t)
	defer MustCloseDB(t, db)

	index := "i"
	field := "f"
	view := "v"
	shard := uint64(0)
	name := rbfName(index, field, view, shard)

	tx := MustBegin(t, db, true)
	defer tx.Rollback()

	if err := tx.CreateBitmap(name); err != nil {
		t.Fatal(err)
	}
	c, err := tx.cursor(name)
	PanicOn(err)

	ikeys := itr.ContainerKeys()

	for _, ckey := range ikeys {
		_, err := c.Seek(ckey)
		PanicOn(err)
		break // after the first seek
	}

	var leafcells []leafCell
	for ckey, ct := itr.NextContainer(); ct != nil; ckey, ct = itr.NextContainer() {
		newN := int(ct.N())
		if newN == 0 {
			continue
		}
		lc := ConvertToLeafArgs(ckey, ct)
		leafcells = append(leafcells, lc)
	} // end ckey loop
	// INVAR: leafcells is ready to go

	groups := splitLeafCells(leafcells)

	var branches []branchCell

	for i, group := range groups {
		_ = i
		// First page should overwrite the original.
		// Subsequent pages should allocate new pages.
		branch := branchCell{LeftKey: group[0].Key}

		branch.ChildPgno, err = c.tx.allocatePgno()
		PanicOn(err)

		branches = append(branches, branch)
		//vv("on group i=%v of %v, group[0].Key = %v; branch.ChildPgno=%v; branch.Key=%v", i, len(groups.slc), int(group[0].Key), (branch.ChildPgno), int(branch.Key))

		var buf [PageSize]byte
		// Write child page.
		writePageNo(buf[:], branch.ChildPgno)
		writeFlags(buf[:], PageTypeLeaf)
		writeCellN(buf[:], len(group))

		offset := dataOffset(len(group))
		for j, cell := range group {
			writeLeafCell(buf[:], j, offset, cell)
			offset += align8(cell.Size())
		}

		err = c.tx.writePage(buf[:])
		PanicOn(err)
	}

	//vv("branches ckeys = '%#v'", keysFromParents(branches))

	err = c.putBranchCells(0, branches)
	PanicOn(err)
}

func TestCursor_incrementally_add_pages_and_view_them(t *testing.T) {

	const maxBranchCells = 2
	prev := globalBranchFillPct
	defer func() {
		globalBranchFillPct = prev
	}()
	// force there to be so many branch cells that the root cannot handle
	// them without making extra branch levels.
	globalBranchFillPct = float64(maxBranchCells+1) / float64(maxBranchCellsPerPage)

	NbranchCells := 10

	want := make([]uint64, 0, ArrayMaxSize)

	expectedBitsChanged := 0
	m := make(map[int]bool)
	for i := 0; i < NbranchCells; i++ {
		for x := 0; x < 6000; x += 2 {
			rowID := i
			columnID := x
			value := (rowID * ShardWidth) + (columnID % ShardWidth)
			want = append(want, uint64(value)) // x+uint64(i)*ShardWidth)
			m[value] = true
			expectedBitsChanged++
		}
	}

	db := testHelperMustOpenNewDB(t)
	defer MustCloseDB(t, db)

	index := "i"
	field := "f"
	view := "v"
	shard := uint64(0)
	name := rbfName(index, field, view, shard)
	clear := false
	rowSize := uint64(0)

	tx := MustBegin(t, db, true)
	defer tx.Rollback()

	for i := 0; i < NbranchCells; i++ {

		itr := getRoaringIter(want[i*3000 : (i+1)*3000]...)

		changed, _, err := tx.ImportRoaringBits(name, itr, clear, false, rowSize)
		PanicOn(err) // writeTheTailOfLeafCellsFromIter has to be AFTER any pre-existing data. ckey=0 was found already in db.
		if changed != 3000 {
			t.Fatalf("expected %v bits changed, got %v", 3000, changed)
		}
	}

	// now clear
	clear = true

	for i := 0; i < NbranchCells; i++ {

		itr := getRoaringIter(want[i*3000 : (i+1)*3000]...)

		changed, _, err := tx.ImportRoaringBits(name, itr, clear, false, rowSize)
		PanicOn(err) // writeTheTailOfLeafCellsFromIter has to be AFTER any pre-existing data. ckey=0 was found already in db.
		if changed != 3000 {
			t.Fatalf("expected %v bits changed, got %v", 3000, changed)
		}
	}
}

// Useful for understanding the split patterns. This
// is how the README.md split pattern docs were obtained.
func TestCursor_from_B_to_C(t *testing.T) {

	const maxBranchCells = 2
	prev := globalBranchFillPct
	defer func() {
		globalBranchFillPct = prev
	}()
	// force there to be so many branch cells that the root cannot handle
	// them without making extra branch levels.
	globalBranchFillPct = float64(maxBranchCells+1) / float64(maxBranchCellsPerPage)

	NbranchCells := 3

	want := make([]uint64, 0, ArrayMaxSize)

	expectedBitsChanged := 0
	m := make(map[int]bool)
	for i := 0; i < NbranchCells; i++ {
		for x := 0; x < 6000; x += 2 {
			rowID := i
			columnID := x
			value := (rowID * ShardWidth) + (columnID % ShardWidth)
			want = append(want, uint64(value)) // x+uint64(i)*ShardWidth)
			m[value] = true
			expectedBitsChanged++
		}
	}

	db := testHelperMustOpenNewDB(t)
	defer MustCloseDB(t, db)

	index := "i"
	field := "f"
	view := "v"
	shard := uint64(0)
	name := rbfName(index, field, view, shard)
	clear := false
	rowSize := uint64(0)

	tx := MustBegin(t, db, true)
	defer tx.Rollback()
	itr := getRoaringIter(want[:6000]...)

	changed, _, err := tx.ImportRoaringBits(name, itr, clear, false, rowSize)
	PanicOn(err) // writeTheTailOfLeafCellsFromIter has to be AFTER any pre-existing data. ckey=0 was found already in db.
	if changed != 6000 {
		t.Fatalf("expected %v bits changed, got %v", 6000, changed)
	}

	//vv("STARTING TO ADD C")
	itr = getRoaringIter(want[6000:9000]...)

	changed, _, err = tx.ImportRoaringBits(name, itr, clear, false, rowSize)
	PanicOn(err) // writeTheTailOfLeafCellsFromIter has to be AFTER any pre-existing data. ckey=0 was found already in db.
	if changed != 3000 {
		t.Fatalf("expected %v bits changed, got %v", 3000, changed)
	}

	// now clear
	clear = true

	for i := 0; i < NbranchCells; i++ {

		itr := getRoaringIter(want[i*3000 : (i+1)*3000]...)

		changed, _, err := tx.ImportRoaringBits(name, itr, clear, false, rowSize)
		PanicOn(err) // writeTheTailOfLeafCellsFromIter has to be AFTER any pre-existing data. ckey=0 was found already in db.
		if changed != 3000 {
			t.Fatalf("expected %v bits changed, got %v", 3000, changed) // failing here got 0
		}
	}
}
