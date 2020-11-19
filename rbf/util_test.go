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
	"fmt"
	"github.com/pilosa/pilosa/v2/roaring"
	"io"
)

// util_test adds reusable utilities for testing.
// Here we catch BitN and ElemN mis-settings by
// scanning all data under an rbf-root (logically equivalent
// to a single roaring.Bitmap with multiple rows).

// verify that BitN and ElemN are correct.
func (c_orig *Cursor) DebugSlowCheckAllPages() {

	// work with a totally new Cursor, so we don't impact our current cursor
	// so any test using the cursor isn't disturbed.
	c2 := Cursor{tx: c_orig.tx}
	c2.stack.elems[0] = c_orig.stack.elems[0]
	err := c2.First()
	if err != nil {
		if err == io.EOF {
			// ok, can be empty
			return
		} else {
			panic(err)
		}
	}

	checkElemNBitN(c2.tx, 0)
}

// checkElemNBitN recursively writes the tree representation starting from a given page to STDERR.
func checkElemNBitN(tx *Tx, pgno uint32) {
	page, err := tx.readPage(pgno)
	if err != nil {
		panic(err)
	}

	if IsMetaPage(page) {
		visitor := func(pgno uint32, records []*RootRecord) {
			for _, record := range records {
				checkElemNBitN(tx, record.Pgno)
			}
		}
		Walk(tx, readMetaRootRecordPageNo(page), visitor)
		return
	}

	// Handle each type of page
	switch typ := readFlags(page); typ {
	case PageTypeBranch:
		for i, n := 0, readCellN(page); i < n; i++ {
			cell := readBranchCell(page, i)
			if cell.Flags&uint32(ContainerTypeBitmap) == 0 { // leaf/branch child page
				checkElemNBitN(tx, cell.Pgno)
			}
			// else is a bitmap
		}
	case PageTypeLeaf:
		cellCheckElemNBitN(tx, page)
	}
}

func cellCheckElemNBitN(tx *Tx, b []byte) {
	pgno := readPageNo(b)
	if pgno == Magic32() {
		// skip meta page
		return
	}

	flags := readFlags(b)
	cellN := readCellN(b)

	switch {
	case flags&PageTypeLeaf != 0:
		for i := 0; i < cellN; i++ {
			cell := readLeafCell(b, i)
			verifyElemNBitN(tx, cell)
		}
	}
}
func verifyElemNBitN(tx *Tx, lc leafCell) {

	obsElemN := int32(-1)
	obsBitN := int32(-1)

	typ := ""
	var c *roaring.Container
	switch lc.Type {
	case ContainerTypeArray:
		typ = "array"
		a := toArray16(lc.Data)
		c = roaring.NewContainerArray(a)
		obsBitN = c.N()
		obsElemN = int32(len(a))

	case ContainerTypeRLE:
		typ = "rle"
		a := toInterval16(lc.Data)
		c = roaring.NewContainerRun(a)
		obsBitN = c.N()
		obsElemN = int32(len(a))

	case ContainerTypeBitmap:
		panic("should never get an actual bitmap!")

	case ContainerTypeBitmapPtr:
		typ = "bitmap_ptr"
		_, bm, _ := tx.leafCellBitmap(toPgno(lc.Data))
		c = roaring.NewContainerBitmap(-1, bm)
		obsBitN = int32(c.N())
		obsElemN = 0 // by definition, should always see 0 for raw bitmaps's ElemN
	}
	if lc.BitN != int(obsBitN) {
		panic(fmt.Sprintf("lc.BitN(%v) != obsBitN(%v); typ='%v'", lc.BitN, obsBitN, typ))
	}
	if lc.ElemN != int(obsElemN) {
		panic(fmt.Sprintf("lc.ElemN(%v) != obsElemN(%v); typ='%v'", lc.ElemN, obsElemN, typ))
	}
}
