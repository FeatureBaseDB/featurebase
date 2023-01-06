// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package rbf

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"os"

	"github.com/featurebasedb/featurebase/v3/roaring"
	"github.com/pkg/errors"
)

// probably should just implement the container interface
// but for now i'll do it
func (c *Cursor) Rows() ([]uint64, error) {
	shardVsContainerExponent := uint(4) // needs constant exported from roaring package
	rows := make([]uint64, 0)
	if err := c.First(); err != nil {
		if err == io.EOF { // root leaf with no elements
			return rows, nil
		}
		return nil, errors.Wrap(err, "rows")
	}
	var err error
	var lastRow uint64 = math.MaxUint64
	for {
		err := c.Next()
		if err != nil {
			break
		}

		elem := &c.stack.elems[c.stack.top]
		leafPage, _, err := c.tx.readPage(elem.pgno)
		if err != nil {
			return nil, err
		}
		cell := readLeafCell(leafPage, elem.index)

		vRow := cell.Key >> shardVsContainerExponent
		if vRow == lastRow {
			continue
		}
		rows = append(rows, vRow)
		lastRow = vRow
	}
	return rows, err
}

func (tx *Tx) FieldViews() []string {
	records, _ := tx.RootRecords()
	a := make([]string, 0, records.Len())
	for itr := records.Iterator(); !itr.Done(); {
		name, _, _ := itr.Next()
		a = append(a, name)
	}
	return a
}

func (c *Cursor) DumpKeys() {
	if err := c.First(); err != nil {
		// ignoring errors for this debug function
		return
	}
	for {
		err := c.Next()
		if err == io.EOF {
			break
		}
		elem := &c.stack.elems[c.stack.top]
		leafPage, _, err := c.tx.readPage(elem.pgno)
		if err != nil {
			return
		}
		cell := readLeafCell(leafPage, elem.index)
		fmt.Println("key", cell.Key)
	}
}

func (c *Cursor) DumpStack() {
	fmt.Println("STACK")
	for i := c.stack.top; i >= 0; i-- {
		fmt.Printf("%+v\n", c.stack.elems[i])
	}
	fmt.Println()
}

func (c *Cursor) Dump(name string) {
	writer, _ := os.Create(name)
	defer writer.Close()
	bufStdout := bufio.NewWriter(writer)
	fmt.Fprintf(bufStdout, "digraph RBF{\n")
	fmt.Fprintf(bufStdout, "rankdir=\"LR\"\n")

	fmt.Fprintf(bufStdout, "node [shape=record height=.1]\n")
	Dumpdot(c.tx, 0, " ", bufStdout)
	fmt.Fprintf(bufStdout, "\n}")
	bufStdout.Flush()
}

func (c *Cursor) Row(shard, rowID uint64) (*roaring.Bitmap, error) {
	base := rowID * ShardWidth

	offset := uint64(shard * ShardWidth)
	off := highbits(offset)
	hi0, hi1 := highbits(base), highbits((rowID+1)*ShardWidth)
	c.stack.top = 0
	ok, err := c.Seek(hi0)
	if err != nil {
		return nil, err
	}
	if !ok {
		elem := &c.stack.elems[c.stack.top]
		leafPage, _, err := c.tx.readPage(elem.pgno)
		if err != nil {
			return nil, err
		}
		n := readCellN(leafPage)
		if elem.index >= n {
			if err := c.goNextPage(); err != nil {
				return nil, errors.Wrap(err, "row")
			}
		}
	}
	other := roaring.NewSliceBitmap()
	for {
		err := c.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}

		elem := &c.stack.elems[c.stack.top]
		leafPage, _, err := c.tx.readPage(elem.pgno)
		if err != nil {
			return nil, err
		}
		cell := readLeafCell(leafPage, elem.index)
		if cell.Key >= hi1 {
			break
		}
		other.Containers.Put(off+(cell.Key-hi0), toContainer(cell, c.tx))
	}
	return other, nil
}

// CurrentPageType returns the type of the container currently pointed to by cursor used in testing
// sometimes the cursor needs to be positions prior to this call with First/Last etc.
func (c *Cursor) CurrentPageType() ContainerType {
	elem := &c.stack.elems[c.stack.top]
	leafPage, _, _ := c.tx.readPage(elem.pgno)
	cell := readLeafCell(leafPage, elem.index)
	return cell.Type
}

func intoContainer(l leafCell, tx *Tx, replacing *roaring.Container, target []byte) (c *roaring.Container) {
	if len(l.Data) == 0 {
		return nil
	}
	orig := l.Data
	var cpMaybe []byte
	var mapped bool
	if tx.db.cfg.DoAllocZero {
		// make a copy so no one will see corrupted data
		// or mmapped data that may disappear.
		cpMaybe = target[:len(orig)]
		copy(cpMaybe, orig)
		mapped = false
	} else {
		// not a copy
		cpMaybe = orig
		mapped = true
	}
	switch l.Type {
	case ContainerTypeArray:
		c = roaring.RemakeContainerArray(replacing, toArray16(cpMaybe))
	case ContainerTypeBitmapPtr:
		_, bm, _ := tx.leafCellBitmap(toPgno(cpMaybe))
		cloneMaybe := bm
		c = roaring.RemakeContainerBitmapN(replacing, cloneMaybe, int32(l.BitN))
	case ContainerTypeBitmap:
		c = roaring.RemakeContainerBitmapN(replacing, toArray64(cpMaybe), int32(l.BitN))
	case ContainerTypeRLE:
		c = roaring.RemakeContainerRunN(replacing, toInterval16(cpMaybe), int32(l.BitN))
	}
	// Note: If the "roaringparanoia" build tag isn't set, this
	// should be optimized away entirely. Otherwise it's moderately
	// expensive.
	c.CheckN()
	c.SetMapped(mapped)
	return c
}

// intoWritableContainer always uses the provided target for a copy of
// the container's contents, so the container can be modified safely.
func intoWritableContainer(l leafCell, tx *Tx, replacing *roaring.Container, target []byte) (c *roaring.Container, err error) {
	if len(l.Data) == 0 {
		return nil, nil
	}
	orig := l.Data
	target = target[:len(orig)]
	copy(target, orig)
	switch l.Type {
	case ContainerTypeArray:
		c = roaring.RemakeContainerArray(replacing, toArray16(target))
	case ContainerTypeBitmapPtr:
		pgno := toPgno(target)
		target = target[:PageSize] // reslice back to full size
		_, bm, err := tx.leafCellBitmapInto(pgno, target)
		if err != nil {
			return nil, fmt.Errorf("intoContainer: %s", err)
		}
		c = roaring.RemakeContainerBitmapN(replacing, bm, int32(l.BitN))
	case ContainerTypeBitmap:
		c = roaring.RemakeContainerBitmapN(replacing, toArray64(target), int32(l.BitN))
	case ContainerTypeRLE:
		c = roaring.RemakeContainerRunN(replacing, toInterval16(target), int32(l.BitN))
	}
	// Note: If the "roaringparanoia" build tag isn't set, this
	// should be optimized away entirely. Otherwise it's moderately
	// expensive.
	c.CheckN()
	c.SetMapped(false)
	return c, nil
}

func toContainer(l leafCell, tx *Tx) (c *roaring.Container) {
	if len(l.Data) == 0 {
		return nil
	}
	orig := l.Data
	var cpMaybe []byte
	var mapped bool
	if tx.db.cfg.DoAllocZero {
		// make a copy, otherwise someone could see corrupted data
		// or mmapped data that may disappear.
		cpMaybe = make([]byte, len(orig))
		copy(cpMaybe, orig)
		mapped = false
	} else {
		// not a copy
		cpMaybe = orig
		mapped = true
	}
	switch l.Type {
	case ContainerTypeArray:
		c = roaring.NewContainerArray(toArray16(cpMaybe))
	case ContainerTypeBitmapPtr:
		_, bm, _ := tx.leafCellBitmap(toPgno(cpMaybe))
		cloneMaybe := bm
		c = roaring.NewContainerBitmap(l.BitN, cloneMaybe)
	case ContainerTypeBitmap:
		c = roaring.NewContainerBitmap(l.BitN, toArray64(cpMaybe))
	case ContainerTypeRLE:
		c = roaring.NewContainerRunN(toInterval16(cpMaybe), int32(l.BitN))
	}
	// Note: If the "roaringparanoia" build tag isn't set, this
	// should be optimized away entirely. Otherwise it's moderately
	// expensive.
	c.CheckN()
	c.SetMapped(mapped)
	return c
}

type Nodetype int

const (
	Branch Nodetype = iota
	Leaf
	Bitmap
)

type Walker interface {
	Visitor(pgno uint32, records []*RootRecord)
	VisitRoot(pgno uint32, name string)
	Visit(pgno uint32, n Nodetype)
}

func WalkPage(tx *Tx, pgno uint32, walker Walker) {
	page, _, err := tx.readPage(pgno)
	if err != nil {
		panic(err)
	}

	if IsMetaPage(page) {
		Walk(tx, readMetaRootRecordPageNo(page), walker.Visitor)
		return
	}

	// Handle
	switch typ := readFlags(page); typ {
	case PageTypeBranch:
		walker.Visit(pgno, Branch)
		for i, n := 0, readCellN(page); i < n; i++ {
			cell := readBranchCell(page, i)
			WalkPage(tx, cell.ChildPgno, walker)
		}
	case PageTypeLeaf:
		walker.Visit(pgno, Leaf)
	default:
		panic(err)
	}
}
