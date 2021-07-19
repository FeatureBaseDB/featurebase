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
	"bufio"
	"fmt"
	"io"
	"math"
	"os"
	"unsafe"

	"github.com/molecula/featurebase/v2/roaring"
	"github.com/molecula/featurebase/v2/storage"
	"github.com/pkg/errors"
)

//probably should just implement the container interface
// but for now i'll do it
func (c *Cursor) Rows() ([]uint64, error) {
	shardVsContainerExponent := uint(4) //needs constant exported from roaring package
	rows := make([]uint64, 0)
	if err := c.First(); err != nil {
		if err == io.EOF { //root leaf with no elements
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
		name, _ := itr.Next()
		a = append(a, name.(string))
	}
	return a
}

func (c *Cursor) DumpKeys() {
	if err := c.First(); err != nil {
		//ignoring errors for this debug function
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
	dumpdot(c.tx, 0, " ", bufStdout)
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
	if storage.EnableRowCache() || tx.db.cfg.DoAllocZero {
		// make a copy, otherwise the rowCache will see corrupted data
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
		if storage.EnableRowCache() {
			cloneMaybe = (*[1024]uint64)(unsafe.Pointer(&target[0]))[:1024]
			copy(cloneMaybe, bm)
		}
		c = roaring.RemakeContainerBitmap(replacing, cloneMaybe)
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

func toContainer(l leafCell, tx *Tx) (c *roaring.Container) {

	if len(l.Data) == 0 {
		return nil
	}
	orig := l.Data
	var cpMaybe []byte
	var mapped bool
	if storage.EnableRowCache() || tx.db.cfg.DoAllocZero {
		// make a copy, otherwise the rowCache will see corrupted data
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
		if storage.EnableRowCache() {
			cloneMaybe = make([]uint64, len(bm))
			copy(cloneMaybe, bm)
		}
		c = roaring.NewContainerBitmap(-1, cloneMaybe)
	case ContainerTypeBitmap:
		c = roaring.NewContainerBitmap(-1, toArray64(cpMaybe))
	case ContainerTypeRLE:
		c = roaring.NewContainerRun(toInterval16(cpMaybe))
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
