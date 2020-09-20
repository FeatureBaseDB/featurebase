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

	"github.com/pilosa/pilosa/v2/roaring"
	"github.com/pkg/errors"
)

// if enableRowCache, then we must not return mmap-ed memory
// directly, but only a copy.
const EnableRowCache = true

// DoAllocZero means we copy mmap read data and
// wipe it afterwards to catch retention of data
// past Tx.Rollback which was a big problem.
// This should be set by NewDBWithAllocZero and never changed
// afterwards in order to avoid a data race.
var DoAllocZero bool

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
		cell := c.cell()
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
	r, _ := tx.RootRecords()
	res := make([]string, len(r))
	for i := range r {
		res[i] = r[i].Name
	}
	return res
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
		cell := c.cell()
		fmt.Println("key", cell.Key)
	}
}
func (c *Cursor) DumpStack() {
	fmt.Println("STACK")
	for i := c.stack.index; i >= 0; i-- {
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
	c.stack.index = 0
	ok, err := c.Seek(hi0)
	if err != nil {
		return nil, err
	}
	if !ok {
		elem := &c.stack.elems[c.stack.index]
		n := readCellN(c.leafPage)
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

		cell := c.cell()
		if cell.Key >= hi1 {
			break
		}
		other.Containers.Put(off+(cell.Key-hi0), toContainer(cell, c.tx))
	}
	return other, nil
}

// CurrentPageType returns the type of the container currently pointed to by cursor used in testing
// sometimes the cursor needs to be positions prior to this call with First/Last etc.
func (c *Cursor) CurrentPageType() int {
	cell := c.cell()
	return cell.Type
}

func toContainer(l leafCell, tx *Tx) (c *roaring.Container) {

	if len(l.Data) == 0 {
		return nil
	}
	orig := l.Data
	var cpMaybe []byte
	var mapped bool
	if EnableRowCache || DoAllocZero {
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
		if EnableRowCache {
			cloneMaybe = make([]uint64, len(bm))
			copy(cloneMaybe, bm)
		}
		c = roaring.NewContainerBitmap(l.N, cloneMaybe)
	case ContainerTypeBitmap:
		c = roaring.NewContainerBitmap(l.N, toArray64(cpMaybe))
	case ContainerTypeRLE:
		c = roaring.NewContainerRun(toInterval16(cpMaybe))
	}
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

func Page(tx *Tx, pgno uint32, walker Walker) {
	page, err := tx.readPage(pgno)
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
			Page(tx, cell.Pgno, walker)
		}
	case PageTypeLeaf:
		walker.Visit(pgno, Leaf)
	default:
		panic(err)
	}
}
