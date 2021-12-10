// Copyright 2021 Molecula Corp. All rights reserved.
package rbf

import (
	"fmt"
	"io"
	"os"
	"testing"

	rbfcfg "github.com/molecula/featurebase/v2/rbf/cfg"
	"github.com/molecula/featurebase/v2/roaring"
	"github.com/molecula/featurebase/v2/testhook"
)

// util_test adds reusable utilities for testing.
// Here we catch BitN and ElemN mis-settings by
// scanning all data under an rbf-root (logically equivalent
// to a single roaring.Bitmap with multiple rows).

var _ = keysFromParents // linter happy

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
	page, _, err := tx.readPage(pgno)
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
				checkElemNBitN(tx, cell.ChildPgno)
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

func testHelperMustOpenNewDB(tb testing.TB, cfg ...*rbfcfg.Config) *DB {
	tb.Helper()

	path, err := testhook.TempDir(tb, "rbfdb")
	if err != nil {
		panic(err)
	}

	var cfg0 *rbfcfg.Config
	if len(cfg) > 0 {
		cfg0 = cfg[0]
	}
	db := NewDB(path, cfg0)

	if err := db.Open(); err != nil {
		tb.Fatal(err)
	}
	return db
}

// MustCloseDB closes db. On error, fail test.
// This function also also performs an integrity check on the DB.
func MustCloseDB(tb testing.TB, db *DB) {
	tb.Helper()
	if err := db.Check(); err != nil && err != ErrClosed {
		tb.Fatal(err)
	} else if n := db.TxN(); n != 0 {
		tb.Fatalf("db still has %d active transactions; must closed before closing db", n)
	} else if err := db.Close(); err != nil && err != ErrClosed {
		tb.Fatal(err)
	} else if err := os.RemoveAll(db.Path); err != nil {
		tb.Fatal(err)
	}
}

// MustBegin returns a new transaction or fails.
func MustBegin(tb testing.TB, db *DB, writable bool) *Tx {
	tb.Helper()
	tx, err := db.Begin(writable)
	if err != nil {
		tb.Fatal(err)
	}
	return tx
}
