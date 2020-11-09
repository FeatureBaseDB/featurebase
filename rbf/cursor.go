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
	"encoding/binary"
	"fmt"
	"io"
	"math/bits"
	"sort"
	"unsafe"

	"github.com/pilosa/pilosa/v2/roaring"
	"github.com/pkg/errors"
)

const (
	BitmapN = (1 << 16) / 64
)

type Cursor struct {
	tx       *Tx
	buffered bool

	// buffers
	leafPage  []byte
	array     [ArrayMaxSize + 1]uint16
	rle       [RLEMaxSize + 1]roaring.Interval16
	leafCells [PageSize / 8]leafCell

	stack struct {
		index int
		elems [32]stackElem
	}
}

func runAdd(runs []roaring.Interval16, v uint16) ([]roaring.Interval16, bool) {
	i := sort.Search(len(runs),
		func(i int) bool { return runs[i].Last >= v })

	if i == len(runs) {
		i--
	}

	iv := runs[i]
	if v >= iv.Start && iv.Last >= v {
		return nil, false
	}

	if iv.Last < v {
		if iv.Last == v-1 {
			runs[i].Last++
		} else {
			runs = append(runs, roaring.Interval16{Start: v, Last: v})
		}
	} else if v+1 == iv.Start {
		// combining two intervals
		if i > 0 && runs[i-1].Last == v-1 {
			runs[i-1].Last = iv.Last
			runs = append(runs[:i], runs[i+1:]...)
			//TODO check if to big
			return runs, true
		}
		// just before an interval
		runs[i].Start--
	} else if i > 0 && v-1 == runs[i-1].Last {
		// just after an interval
		runs[i-1].Last++
	} else {
		// alone
		newIv := roaring.Interval16{Start: v, Last: v}
		runs = append(runs[:i], append([]roaring.Interval16{newIv}, runs[i:]...)...)
	}
	return runs, true
}
func checkRun(runs []roaring.Interval16, bitN int, key uint64) leafCell {
	if len(runs) >= RLEMaxSize {
		//convertToBitmap
		bitmap := make([]uint64, BitmapN)
		for _, iv := range runs {
			w1, w2 := iv.Start/64, iv.Last/64
			b1, b2 := iv.Start&63, iv.Last&63
			// a mask for everything under bit X looks like
			// (1 << x) - 1. Say b1 is 4; our mask will want
			// to have the bottom 4 bits be zero, so we shift
			// left 4, getting 10000, then subtract 1, and
			// get 01111, which is the mask to *remove*.
			m1 := (uint64(1) << b1) - 1
			// inclusive mask: same thing, then shift left 1 and
			// or in 1. So for 4, we'd get 011111, which is the
			// mask to *keep*.
			m2 := (((uint64(1) << b2) - 1) << 1) | 1
			if w1 == w2 {
				// If we only had bit 4 in the range, this would
				// end up being 011111 &^ 01111, or 010000.
				bitmap[w1] |= (m2 &^ m1)
				continue
			}
			// for w2, the "To" field, we want to set the bottom N
			// bits. For w1, the "From" word, we want to set all *but*
			// the bottom N bits.
			bitmap[w2] |= m2
			bitmap[w1] |= ^m1
			words := bitmap[w1+1 : w2]
			// set every bit between them
			for i := range words {
				words[i] = ^uint64(0)
			}
		}
		n := uint64(0)
		for _, v := range bitmap {
			n += popcount(v)
		}

		return leafCell{Key: key, N: int(n), BitN: int(n), Type: ContainerTypeBitmap, Data: fromArray64(bitmap)}
	}
	return leafCell{Key: key, N: len(runs), BitN: int(bitN + 1), Type: ContainerTypeRLE, Data: fromInterval16(runs)}
}

// Add sets a bit on the underlying bitmap.
func (c *Cursor) Add(v uint64) (changed bool, err error) {
	hi, lo := highbits(v), lowbits(v)
	// Move cursor to the key of the container.
	// Insert new container if it doesn't exist.
	if exact, err := c.Seek(hi); err != nil {
		return false, err
	} else if !exact {
		return true, c.putLeafCell(leafCell{Key: hi, Type: ContainerTypeArray, N: 1, BitN: 1, Data: fromArray16([]uint16{lo})})
	}

	// If the container exists and bit is not set then update the page.
	cell := c.cell()
	switch cell.Type {
	case ContainerTypeArray:
		// Exit if value exists in array container.
		a := toArray16(cell.Data)
		i, ok := arrayIndex(a, lo)
		if ok {
			return false, nil
		}

		// Copy container data and insert new value.
		other := c.array[:len(a)+1]
		copy(other, a[:i])
		other[i] = lo
		copy(other[i+1:], a[i:])
		return true, c.putLeafCell(leafCell{Key: cell.Key, Type: ContainerTypeArray, N: len(other), BitN: cell.BitN + 1, Data: fromArray16(other)})

	case ContainerTypeRLE:
		runs := toInterval16(cell.Data)
		//TODO Look at this again with fresh eyes
		copy(c.rle[:], runs)
		run, added := runAdd(c.rle[:len(runs)], lo)
		if added {
			leaf := checkRun(run, cell.BitN, cell.Key)
			return true, c.putLeafCell(leaf)
		}
		return false, nil
	case ContainerTypeBitmapPtr:
		// Exit if bit set in bitmap container.
		pgno, bm, err := c.tx.leafCellBitmap(toPgno(cell.Data))

		if err != nil {
			return false, errors.Wrap(err, "cursor.Add")
		}

		a := cloneArray64(bm)
		if a[lo/64]&(1<<uint64(lo%64)) != 0 {
			return false, nil
		}

		// Insert new value and rewrite page.
		a[lo/64] |= 1 << uint64(lo%64)
		if err := c.tx.writeBitmapPage(pgno, fromArray64(a)); err != nil {
			return false, err
		}
		// TODO(bbj): Update parent cell with new BitN.

		return true, nil
	default:
		return false, fmt.Errorf("rbf.Cursor.Add(): invalid container type: %d", cell.Type)
	}
}

// Remove unsets a bit on the underlying bitmap.
func (c *Cursor) Remove(v uint64) (changed bool, err error) {
	hi, lo := highbits(v), lowbits(v)

	// Move cursor to the key of the container.
	// Exit if container does not exist.
	if exact, err := c.Seek(hi); err != nil {
		return false, err
	} else if !exact {
		return false, nil
	}

	// If the container exists and bit is not set then update the page.
	cell := c.cell()
	switch cell.Type {
	case ContainerTypeArray:
		// Exit if value does not exists in array container.
		a := toArray16(cell.Data)
		i, ok := arrayIndex(a, lo)
		if !ok {
			return false, nil
		} else if len(a) == 1 {
			return true, c.deleteLeafCell(cell.Key)
		}

		// Copy container data and remove new value.
		other := make([]uint16, len(a)-1)
		copy(other[:i], a[:i])
		copy(other[i:], a[i+1:])
		return true, c.putLeafCell(leafCell{Key: cell.Key, Type: ContainerTypeArray, N: len(other), BitN: cell.BitN - 1, Data: fromArray16(other)})

	case ContainerTypeRLE:
		r := toInterval16(cell.Data)
		i, contains := roaring.BinSearchRuns(lo, r)
		if !contains {
			return false, nil
		}
		copy(c.rle[:], r)
		runs := c.rle[:len(r)]

		if lo == runs[i].Last && lo == runs[i].Start {
			runs = append(runs[:i], runs[i+1:]...)
		} else if lo == runs[i].Last {
			runs[i].Last--
		} else if lo == c.rle[i].Start {
			runs[i].Start++
		} else if lo > runs[i].Start {
			last := runs[i].Last
			runs[i].Last = lo - 1
			runs = append(runs, roaring.Interval16{})
			copy(runs[i+2:], runs[i+1:])
			runs[i+1] = roaring.Interval16{Start: lo + 1, Last: last}
		}
		if len(runs) == 0 {
			return true, c.deleteLeafCell(cell.Key)
		}
		return true, c.putLeafCell(leafCell{Key: cell.Key, Type: ContainerTypeRLE, N: len(runs), Data: fromInterval16(runs)})
	case ContainerTypeBitmapPtr:
		pgno, bm, err := c.tx.leafCellBitmap(toPgno(cell.Data))
		if err != nil {
			return false, errors.Wrap(err, "cursor.add")
		}
		a := cloneArray64(bm)
		if a[lo/64]&(1<<uint64(lo%64)) == 0 {
			return false, nil
		}

		// TODO(3736): Handle shrinking bitmap container to an array container.

		// Clear bit and rewrite page.
		a[lo/64] &^= 1 << uint64(lo%64)
		if err := c.tx.writeBitmapPage(pgno, fromArray64(a)); err != nil {
			return false, err
		}

		// TODO(bbj): Update parent cell to decrement BitN.
		return true, nil
	default:
		return false, fmt.Errorf("rbf.Cursor.Add(): invalid container type: %d", cell.Type)
	}
}

// Contains returns true if a bit is set on the underlying bitmap.
func (c *Cursor) Contains(v uint64) (exists bool, err error) {
	hi, lo := highbits(v), lowbits(v)

	// Move cursor to the key of the container.
	if exact, err := c.Seek(hi); err != nil {
		return false, err
	} else if !exact {
		return false, nil
	}

	// If the container exists then check for low bits existence.
	cell := c.cell()
	switch cell.Type {
	case ContainerTypeArray:
		a := toArray16(cell.Data)
		_, ok := arrayIndex(a, lo)
		return ok, nil

	case ContainerTypeRLE:
		a := toInterval16(cell.Data)
		i := int32(sort.Search(len(a),
			func(i int) bool { return a[i].Last >= lo }))
		if i < int32(len(a)) {
			return (lo >= a[i].Start) && (lo <= a[i].Last), nil
		}
		return false, nil
	case ContainerTypeBitmapPtr:
		_, a, err := c.tx.leafCellBitmap(toPgno(cell.Data))
		if err != nil {
			return false, errors.Wrap(err, "cursor.Contains")
		}
		return a[lo/64]&(1<<uint64(lo%64)) != 0, err
	default:
		return false, fmt.Errorf("rbf.Cursor.Contains(): invalid container type: %d", cell.Type)
	}
}
func fromPgno(val uint32) []byte {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, val)
	//	return (*[4]byte)(unsafe.Pointer(&val))[:]
	return buf
}
func toPgno(val []byte) uint32 {
	return binary.LittleEndian.Uint32(val)
}
func (c *Cursor) putLeafCell(in leafCell) (err error) {
	leafPage := c.leafPage

	cells := readLeafCells(leafPage, c.leafCells[:])
	elem := &c.stack.elems[c.stack.index]
	cell := in
	if elem.index >= len(cells) || c.Key() != cell.Key {
		//new cell
		if in.Type == ContainerTypeBitmap {
			//allocated bitmap()
			bitmapPgno, err := c.tx.allocate()
			if err != nil {
				return err
			}
			cell.Data = fromPgno(bitmapPgno)
			cell.Type = ContainerTypeBitmapPtr
		}
		// Shift cells over if this is an insertion.
		cells = append(cells, leafCell{})
		copy(cells[elem.index+1:], cells[elem.index:])

	} else {
		if in.Type == ContainerTypeBitmap {
			cell = cells[elem.index]
			if cell.Type != ContainerTypeBitmapPtr {
				bitmapPgno, err := c.tx.allocate()
				if err != nil {
					return errors.Wrap(err, "cursor.putLeafCell")
				}
				cell.Type = ContainerTypeBitmapPtr
				cell.Data = fromPgno(bitmapPgno)
			}
		}
	}

	if in.Type == ContainerTypeArray && in.N > ArrayMaxSize {
		//convert to bitmap
		in.Type = ContainerTypeBitmap
		a := make([]uint64, PageSize/8)
		for _, v := range toArray16(in.Data) {
			a[v/64] |= 1 << uint64(v%64)
		}
		in.Data = fromArray64(a)
		cell.Type = ContainerTypeBitmapPtr
		bitmapPgno, err := c.tx.allocate()
		if err != nil {
			return err
		}
		cell.Data = fromPgno(bitmapPgno)
	}

	cells[elem.index] = cell

	// Split into multiple pages if page size is exceeded.
	groups := [][]leafCell{cells}
	sz := leafCellsPageSize(cells)
	if sz >= PageSize {
		groups = splitLeafCells(cells)
	}

	// Write each group to a separate page.
	newRoot := (len(groups) > 1) && (c.stack.index == 0)
	var parents []branchCell
	origPgno := elem.pgno
	// newRoot if split occured and bottom of the stack
	for i, group := range groups {
		// First page should overwrite the original.
		// Subsequent pages should allocate new pages.
		parent := branchCell{Key: group[0].Key} //<<< this is the key spot for making sure that key is correct
		if i == 0 && !newRoot {
			parent.Pgno = origPgno
		} else {
			if parent.Pgno, err = c.tx.allocate(); err != nil {
				return fmt.Errorf("cannot allocate leaf: %w", err)
			}
		}

		// if the cell is a bitmap write out its page
		if in.Type == ContainerTypeBitmap {
			var bm [PageSize]byte
			copy(bm[:], fromArray64(toArray64(in.Data)))
			if err = c.tx.writeBitmapPage(toPgno(cell.Data), bm[:]); err != nil {
				return errors.Wrap(err, "putLeafCell writing bitmap page")
			}
		}
		var buf [PageSize]byte
		// Write cells to page.
		writePageNo(buf[:], parent.Pgno)
		writeFlags(buf[:], PageTypeLeaf)
		writeCellN(buf[:], len(group))

		offset := dataOffset(len(group))
		for j, cell := range group {
			writeLeafCell(buf[:], j, offset, cell)
			offset += align8(cell.Size())
		}

		if err := c.tx.writePage(buf[:]); err != nil {
			return err
		}

		parents = append(parents, parent)
	}

	// TODO(BBJ): Update page in buffer & cursor stack.

	// If this is not a split then exit now.
	if len(groups) == 1 {
		return nil
	}

	// Initialize a new root if we are currently the root page.
	if c.stack.index == 0 {
		assert(newRoot) // leaf write must be root when stack at root
		return c.writeRoot(origPgno, parents)
	}
	assert(!newRoot) // leaf write must NOT be root when stack not at root

	// Otherwise update existing parent.
	return c.putBranchCells(c.stack.index-1, parents)
}

// deleteLeafCell removes a cell from the currently positioned page & index.
func (c *Cursor) deleteLeafCell(key uint64) (err error) {
	cells := readLeafCells(c.leafPage, c.leafCells[:])
	elem := &c.stack.elems[c.stack.index]
	oldPageKey := cells[0].Key
	cell := c.cell()
	if cell.Type == ContainerTypeBitmapPtr {
		if err := c.tx.deallocate(toPgno(cell.Data)); err != nil {
			return err
		}
	}

	// If no more cells exist and we have a parent, remove from parent.
	if c.stack.index > 0 && len(cells) == 1 {
		if err := c.tx.deallocate(elem.pgno); err != nil {
			return err
		}
		return c.deleteBranchCell(c.stack.index-1, cells[0].Key)
	}

	// Remove matching cell from list.
	copy(cells[elem.index:], cells[elem.index+1:])
	cells[len(cells)-1] = leafCell{}
	cells = cells[:len(cells)-1]

	// Write cells to page.
	buf := make([]byte, PageSize)
	writePageNo(buf[:], elem.pgno)
	writeFlags(buf[:], PageTypeLeaf)
	writeCellN(buf[:], len(cells))

	offset := dataOffset(len(cells))
	for j, cell := range cells {
		writeLeafCell(buf[:], j, offset, cell)
		offset += align8(cell.Size())
	}
	if err := c.tx.writePage(buf[:]); err != nil {
		return err
	}

	// Update the parent's reference key if it's changed.
	if c.stack.index > 0 && oldPageKey != cells[0].Key {
		return c.updateBranchCell(c.stack.index-1, cells[0].Key)
	}
	return nil
}

// putBranchCells updates a branch page with one or more cells.
func (c *Cursor) putBranchCells(stackIndex int, newCells []branchCell) (err error) {
	elem := &c.stack.elems[stackIndex]

	// Read branch page from disk. The current buffer is the leaf page.
	page, err := c.tx.readPage(elem.pgno)
	if err != nil {
		return err
	}

	cells := readBranchCells(page)

	// Update current cell & insert additional cells after it.
	cells[elem.index] = newCells[0]
	if len(newCells) > 1 {
		cells = append(cells, make([]branchCell, len(newCells)-1)...)
		copy(cells[elem.index+len(newCells):], cells[elem.index+1:])
		copy(cells[elem.index+1:], newCells[1:])
	}

	// Split into multiple pages if page size is exceeded.
	groups := [][]branchCell{cells}
	if branchCellsPageSize(cells) > PageSize {
		groups = splitBranchCells(cells)
	}

	// Write each group to a separate page.
	var parents []branchCell
	origPgno := readPageNo(page)
	newRoot := len(groups) > 1 && stackIndex == 0
	for i, group := range groups {
		// First page should overwrite the original.
		// Subsequent pages should allocate new pages.
		parent := branchCell{Key: group[0].Key}
		if i == 0 && !newRoot {
			parent.Pgno = origPgno
		} else {
			if parent.Pgno, err = c.tx.allocate(); err != nil {
				return fmt.Errorf("cannot allocate branch: %w", err)
			}
		}
		parents = append(parents, parent)

		// Write cells to page.
		var buf [PageSize]byte
		writePageNo(buf[:], parents[i].Pgno)
		writeFlags(buf[:], PageTypeBranch)
		writeCellN(buf[:], len(group))

		offset := dataOffset(len(group))
		for j, cell := range group {
			writeBranchCell(buf[:], j, offset, cell)
			offset += align8(branchCellSize)
		}

		if err := c.tx.writePage(buf[:]); err != nil {
			return err
		}
	}

	// TODO(BBJ): Check if key on page changes and update parent if so.

	// TODO(BBJ): Update page in buffer & cursor stack.

	// If this is not a split, then exit now.
	if len(groups) == 1 {
		return nil
	}

	// Initialize a new root if we are currently the root page.
	if stackIndex == 0 {
		assert(newRoot) // branch write must be root when stack at root
		return c.writeRoot(origPgno, parents)
	}
	assert(!newRoot) // branch write must NOT be root when stack at root

	// Otherwise update existing parent.
	return c.putBranchCells(stackIndex-1, parents)
}

// updateBranchCell updates the key for cell in the branch.
func (c *Cursor) updateBranchCell(stackIndex int, newKey uint64) (err error) {
	elem := &c.stack.elems[stackIndex]

	// Read branch page from disk. The current buffer is the leaf page.
	page, err := c.tx.readPage(elem.pgno)
	if err != nil {
		return err
	}
	cells := readBranchCells(page)
	oldPageKey := cells[0].Key

	// Update key in branch cell.
	cells[elem.index].Key = newKey

	// Write cells to page.
	var buf [PageSize]byte
	writePageNo(buf[:], elem.pgno)
	writeFlags(buf[:], PageTypeBranch)
	writeCellN(buf[:], len(cells))

	offset := dataOffset(len(cells))
	for j, cell := range cells {
		writeBranchCell(buf[:], j, offset, cell)
		offset += align8(branchCellSize)
	}
	if err := c.tx.writePage(buf[:]); err != nil {
		return err
	}

	if stackIndex > 0 && oldPageKey != cells[0].Key {
		return c.updateBranchCell(stackIndex-1, cells[0].Key)
	}
	return nil
}

// deleteBranchCell removes a cell from a branch page.
func (c *Cursor) deleteBranchCell(stackIndex int, key uint64) (err error) {
	elem := &c.stack.elems[stackIndex]

	// Read branch page from disk. The current buffer is the leaf page.
	page, err := c.tx.readPage(elem.pgno)
	if err != nil {
		return err
	}
	cells := readBranchCells(page)
	oldPageKey := cells[0].Key

	// Remove cell from branch.
	copy(cells[elem.index:], cells[elem.index+1:])
	cells[len(cells)-1] = branchCell{}
	cells = cells[:len(cells)-1]

	// If the root only has one node, replace it with its child.
	if stackIndex == 0 && len(cells) == 1 {
		target, err := c.tx.readPage(cells[0].Pgno)
		if err != nil {
			return err
		}

		buf := make([]byte, PageSize)
		copy(buf, target)
		writePageNo(buf[:], elem.pgno)

		if err := c.tx.deallocate(cells[0].Pgno); err != nil {
			return err
		}
		return c.tx.writePage(buf[:])
	}

	// Write cells to page.
	var buf [PageSize]byte
	writePageNo(buf[:], elem.pgno)
	writeFlags(buf[:], PageTypeBranch)
	writeCellN(buf[:], len(cells))

	offset := dataOffset(len(cells))
	for j, cell := range cells {
		writeBranchCell(buf[:], j, offset, cell)
		offset += align8(branchCellSize)
	}
	if err := c.tx.writePage(buf[:]); err != nil {
		return err
	}

	if stackIndex > 0 && len(cells) > 0 && oldPageKey != cells[0].Key {
		return c.updateBranchCell(stackIndex-1, cells[0].Key)
	}
	return nil
}

// writeRoot writes a new branch page at the root with the given cells.
func (c *Cursor) writeRoot(pgno uint32, cells []branchCell) error {
	var buf [PageSize]byte
	writePageNo(buf[:], pgno)
	writeFlags(buf[:], PageTypeBranch)
	writeCellN(buf[:], len(cells))

	offset := dataOffset(len(cells))
	for i := range cells {
		writeBranchCell(buf[:], i, offset, cells[i])
		offset += align8(branchCellSize)
	}
	return c.tx.writePage(buf[:])
}

// splitLeafCells splits cells into roughly equal parts. It's a naive
// implementation that splits cells whenever a page is 60% full.
func splitLeafCells(cells []leafCell) [][]leafCell {
	slices := make([][]leafCell, 1, 2)

	var dataSize int
	for _, cell := range cells {
		if cell.Type == ContainerTypeBitmap {
			panic("no! all ContainerTypeBitmap should be ContainerTypeBitmapPtr by now")
		}
		// Determine number of cells on current slice & cell size.
		cellN := len(slices[len(slices)-1])
		sz := align8(leafCellHeaderSize + len(cell.Data))

		// If there is at least one cell on the slice & we've exceeded
		// half a page then create a new group of cells.
		if cellN != 0 && (dataOffset(cellN+1)+dataSize+sz) > (PageSize*60)/100 {
			slices, dataSize = append(slices, nil), 0
		} else if cellN != 0 && cell.Type == ContainerTypeArray && cell.N > ArrayMaxSize {
			slices, dataSize = append(slices, nil), 0
			sz = PageSize
		}

		// Append to current slice & increase total cell data size.
		slices[len(slices)-1] = append(slices[len(slices)-1], cell)
		dataSize += sz
	}

	return slices
}

// splitBranchCells splits cells into roughly equal parts. It's a naive
// implementation that splits cells whenever a page is 60% full.
func splitBranchCells(cells []branchCell) [][]branchCell {
	slices := make([][]branchCell, 1, 2)

	var dataSize int
	for _, cell := range cells {
		// Determine number of cells on current slice & cell size.
		cellN := len(slices[len(slices)-1])
		sz := align8(branchCellSize)

		// If there is at least one cell on the slice & we've exceeded
		// half a page then create a new group of cells.
		if cellN != 0 && (dataOffset(cellN+1)+dataSize+sz) > (PageSize*60)/100 {
			slices, dataSize = append(slices, nil), 0
		}

		// Append to current slice & increase total cell data size.
		slices[len(slices)-1] = append(slices[len(slices)-1], cell)
		dataSize += sz
	}

	return slices
}

// Key returns the key that the cursor is currently positioned over.
func (c *Cursor) Key() uint64 {
	elem := &c.stack.elems[c.stack.index]
	offset := readCellOffset(c.leafPage, elem.index)
	return *(*uint64)(unsafe.Pointer(&c.leafPage[offset]))
}

func (c *Cursor) cell() leafCell {
	elem := &c.stack.elems[c.stack.index]
	return readLeafCell(c.leafPage[:], elem.index)
}

// First moves to the first element of the btree.
func (c *Cursor) First() error {
	c.buffered = true

	for c.stack.index = 0; ; c.stack.index++ {
		elem := &c.stack.elems[c.stack.index]

		buf, err := c.tx.readPage(elem.pgno)
		if err != nil {
			return err
		}

		switch typ := readFlags(buf); typ {
		case PageTypeBranch:
			elem.index = 0

			// Read cell pgno into the next stack level.
			cell := readBranchCell(buf, elem.index)

			c.stack.elems[c.stack.index+1] = stackElem{
				pgno: cell.Pgno,
				key:  cell.Key,
			}

		case PageTypeLeaf:
			c.leafPage = buf
			elem.index = 0
			if readCellN(buf) == 0 {
				return io.EOF // root leaf with no elements
			}
			return nil
		default:
			return fmt.Errorf("rbf.Cursor.First(): invalid page type: pgno=%d type=%d", elem.pgno, typ)
		}
	}
}

// Last moves to the last element of the btree.
func (c *Cursor) Last() error {
	// c.stack.elems[0].pgno = c.root
	c.buffered = true

	for c.stack.index = 0; ; c.stack.index++ {
		elem := &c.stack.elems[c.stack.index]

		buf, err := c.tx.readPage(elem.pgno)
		if err != nil {
			return err
		}

		switch typ := readFlags(buf); typ {
		case PageTypeBranch:
			elem.index = readCellN(buf) - 1

			// Read cell pgno into the next stack level.
			cell := readBranchCell(buf, elem.index)
			c.stack.elems[c.stack.index+1] = stackElem{
				pgno: cell.Pgno,
				key:  cell.Key,
			}

		case PageTypeLeaf:
			elem.index = readCellN(buf) - 1
			c.leafPage = buf
			if readCellN(buf) == 0 {
				return io.EOF // root leaf with no elements
			}
			return nil
		default:
			return fmt.Errorf("rbf.Cursor.Last(): invalid page type: pgno=%d type=%d", elem.pgno, typ)
		}
	}
}

// Seek moves to the specified container of the btree.
// If the container does not exist then it moves to the next container after the key.
func (c *Cursor) Seek(key uint64) (exact bool, err error) {
	// c.stack.elems[0].pgno = c.bitmap.root
	c.buffered = true
	for c.stack.index = 0; ; c.stack.index++ {
		elem := &c.stack.elems[c.stack.index]
		assert(elem.pgno != 0) // cursor should never point to page zero (meta)

		buf, err := c.tx.readPage(elem.pgno)
		if err != nil {
			return false, err
		}

		switch typ := readFlags(buf); typ {
		case PageTypeBranch:
			n := readCellN(buf)
			index, xact := search(n, func(i int) int {
				if v := readBranchCellKey(buf, i); key == v {
					return 0
				} else if key < v {
					return -1
				}
				return 1
			})
			//if not found (xact) the cell
			if !xact && index > 0 {
				index--
			}
			elem.index = index

			// Read cell pgno into the next stack level.

			cell := readBranchCell(buf, elem.index)

			c.stack.elems[c.stack.index+1] = stackElem{
				pgno: cell.Pgno,
				key:  cell.Key,
			}

		case PageTypeLeaf:
			n := readCellN(buf)
			index, xact := search(n, func(i int) int {
				if v := readLeafCellKey(buf, i); key == v {
					return 0
				} else if key < v {
					return -1
				}
				return 1
			})
			elem.index = index
			c.leafPage = buf
			return xact, nil

		default:
			return false, fmt.Errorf("rbf.Cursor.Seek(): invalid page type: pgno=%d type=%d", elem.pgno, typ)
		}
	}
}

// Next moves to the next element of the btree. Returns EOF if no more elements exist.
func (c *Cursor) Next() error {
	if c.buffered {
		c.buffered = false

		// Move to next available element if we are past the last cell in the page.
		if elem := &c.stack.elems[c.stack.index]; elem.index >= readCellN(c.leafPage) {
			return c.goNextPage()
		}
		return nil
	}

	// Move forward to the next leaf element if available.
	if elem := &c.stack.elems[c.stack.index]; elem.index < readCellN(c.leafPage)-1 {
		elem.index++
		return nil
	}
	return c.goNextPage()
}

// Prev moves to the previous element of the btree.
func (c *Cursor) Prev() error {
	if c.buffered {
		c.buffered = false
		return nil
	}

	// Move forward to the next leaf element if available.
	if elem := &c.stack.elems[c.stack.index]; elem.index > 0 {
		elem.index--
		return nil
	}

	// Move up the stack until we can move forward one element.
	for c.stack.index--; c.stack.index >= 0; c.stack.index-- {
		elem := &c.stack.elems[c.stack.index]
		if elem.index > 0 {
			elem.index--
			break
		}
	}

	// No more elements, return EOF.
	if c.stack.index == -1 {
		c.stack.index = 0
		return io.EOF
	}

	// Traverse back down the stack to find the first element in each page.
	for ; ; c.stack.index++ {
		elem := &c.stack.elems[c.stack.index]

		buf, err := c.tx.readPage(elem.pgno)
		if err != nil {
			return err
		}

		switch typ := readFlags(buf); typ {
		case PageTypeBranch:
			cell := readBranchCell(buf, elem.index)

			c.stack.elems[c.stack.index+1] = stackElem{
				pgno: cell.Pgno,
				key:  cell.Key,
			}

		case PageTypeLeaf:
			elem.index = readCellN(buf) - 1
			c.leafPage = buf
			return nil
		default:
			return fmt.Errorf("rbf.Cursor.Prev(): invalid page type: pgno=%d type=%d", elem.pgno, typ)
		}
	}
}

// Union performs a bitwise OR operation on row and a given row id in the bitmap.
func (c *Cursor) Union(rowID uint64, row []uint64) error {
	base := rowID * ShardWidth

	if _, err := c.Seek(base >> 16); err != nil {
		return err
	}
	for {
		err := c.Next()
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}

		cell := c.cell()
		key := cell.Key << 16
		if key >= base+ShardWidth {
			return nil
		}
		offset := key - base
		switch cell.Type {
		case ContainerTypeArray:
			for _, v := range toArray16(cell.Data) {
				row[(offset+uint64(v))/64] |= 1 << uint64(v%64)
			}
		case ContainerTypeRLE:
			panic("TODO(BBJ): rbf.Bitmap.Union() RLE support")
		case ContainerTypeBitmapPtr:
			_, bm, err := c.tx.leafCellBitmap(toPgno(cell.Data))
			if err != nil {
				return errors.Wrap(err, "union")
			}
			for i, v := range bm {
				row[(offset/64)+uint64(i)] |= v
			}
		default:
			return fmt.Errorf("rbf.Bitmap.Union(): invalid container type: %d", cell.Type)
		}
	}
}

// Intersect performs a bitwise AND operation on row and a given row id in the bitmap.
func (c *Cursor) Intersect(rowID uint64, row []uint64) error {
	base := rowID * ShardWidth
	c.stack.index = 0

	keyExists := make([]bool, ShardWidth/(1<<16))

	if _, err := c.Seek(base >> 16); err != nil {
		return err
	}
	for {
		err := c.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		cell := c.cell()
		key := cell.Key << 16
		if key >= base+ShardWidth {
			return nil
		}
		offset := key - base

		keyExists[offset/(1<<16)] = true

		switch cell.Type {
		case ContainerTypeArray:
			for i, v := range cell.Bitmap(c.tx) {
				row[(offset/64)+uint64(i)] &= v
			}
		case ContainerTypeRLE:
			panic("TODO(BBJ): rbf.Bitmap.Intersect() RLE support")
		case ContainerTypeBitmapPtr:
			_, bm, err := c.tx.leafCellBitmap(toPgno(cell.Data))
			if err != nil {
				return errors.Wrap(err, "cursor.Intersect")
			}
			for i, v := range bm {
				row[(offset/64)+uint64(i)] &= v
			}
		default:
			return fmt.Errorf("rbf.Bitmap.Intersect(): invalid container type: %d", cell.Type)
		}
	}

	// Clear any missing keys.
	for i, ok := range keyExists {
		if ok {
			continue
		}
		for j := 0; j < (1 << 16); j += 64 {
			row[((i*(1<<16))+j)/64] = 0
		}
	}
	return nil
}

// Values returns the values for the container the cursor is currently pointing to.
func (c *Cursor) Values() []uint16 {
	elem := &c.stack.elems[c.stack.index]
	if readCellN(c.leafPage[:]) == 0 {
		return nil
	}
	cell := readLeafCell(c.leafPage[:], elem.index)
	return cell.Values(c.tx)
}

// stackElem represents a single element on the cursor stack.
type stackElem struct {
	pgno  uint32 // current page number
	index int    // cell index
	key   uint64 // element key
}

func (c *Cursor) goNextPage() error {
	for c.stack.index--; c.stack.index >= 0; c.stack.index-- {
		elem := &c.stack.elems[c.stack.index]
		if buf, err := c.tx.readPage(elem.pgno); err != nil {
			return err
		} else if n := readCellN(buf); elem.index+1 < n {
			elem.index++
			break
		}
	}

	// No more elements, return EOF.
	if c.stack.index == -1 {
		c.stack.index = 0
		return io.EOF
	}

	// Traverse back down the stack to find the first element in each page.
	for ; ; c.stack.index++ {
		elem := &c.stack.elems[c.stack.index]
		buf, err := c.tx.readPage(elem.pgno)
		if err != nil {
			return err
		}

		switch typ := readFlags(buf); typ {
		case PageTypeBranch:
			cell := readBranchCell(buf, elem.index)
			c.stack.elems[c.stack.index+1] = stackElem{
				pgno: cell.Pgno,
				key:  cell.Key,
			}
		case PageTypeLeaf:
			elem.index = 0
			c.leafPage = buf
			return nil
		default:
			return fmt.Errorf("rbf.Cursor.Next(): invalid page type: pgno=%d type=%d", elem.pgno, typ)
		}
	}
}

func ConvertToLeafArgs(key uint64, c *roaring.Container) (result leafCell) {
	result.Key = key
	result.N = int(c.N())
	result.BitN = int(c.N())
	result.Type = ContainerTypeNone
	if c.N() == 0 {
		return
	}
	switch roaring.ContainerType(c) {
	case 1: //array
		a := roaring.AsArray(c)
		if len(a) > ArrayMaxSize {
			roaring.ConvertArrayToBitmap(c)
			result.Type = ContainerTypeBitmap
			result.Data = fromArray64(roaring.AsBitmap(c))
			return
		}
		result.Type = ContainerTypeArray
		result.Data = fromArray16(a)
		return
	case 2: //bitmap
		result.Type = ContainerTypeBitmap
		result.Data = fromArray64(roaring.AsBitmap(c))
		return
	case 3: //run
		r := roaring.AsRuns(c)
		if len(r) > RLEMaxSize {
			roaring.ConvertRunToBitmap(c)
			result.Type = ContainerTypeBitmap
			result.Data = fromArray64(roaring.AsBitmap(c))
			return
		}
		result.N = len(r) //note RBF N is number of containers
		result.Type = ContainerTypeRLE
		result.Data = fromInterval16(r)
		return
	}
	return
}

func (c *Cursor) merge(key uint64, data *roaring.Container) (bool, error) {
	cell := c.cell()
	var container *roaring.Container
	switch cell.Type {
	case ContainerTypeArray:
		d := toArray16(cell.Data)
		container = roaring.NewContainerArray(d)
	case ContainerTypeBitmapPtr:
		_, d, err := c.tx.leafCellBitmap(toPgno(cell.Data))
		if err != nil {
			return false, errors.Wrap(err, "cursor.merge")
		}
		container = roaring.NewContainerBitmap(-1, d)
	case ContainerTypeRLE:
		d := toInterval16(cell.Data)
		container = roaring.NewContainerRun(d)
	}
	res := roaring.Union(data, container)
	if res.N() != data.N() {
		leaf := ConvertToLeafArgs(key, res)
		err := c.putLeafCell(leaf)
		return true, err
	}

	return false, nil
}

func (c *Cursor) AddRoaring(bm *roaring.Bitmap) (changed bool, err error) {
	itr, _ := bm.Containers.Iterator(0)
	for itr.Next() {
		hi, cont := itr.Value()
		leaf := ConvertToLeafArgs(hi, cont)
		if leaf.N == 0 {
			continue
		}
		// Move cursor to the key of the container.
		// Insert new container if it doesn't exist.
		if exact, err := c.Seek(hi); err != nil {
			return false, err
		} else if !exact {
			err = c.putLeafCell(leaf)
			if err != nil {
				return false, err
			}
			changed = true
			continue
		}
		// If the container exists and bit is not set then update the page.
		u, err := c.merge(hi, cont)
		if err != nil {
			return false, err
		}
		if u {
			changed = true
		}
	}
	return changed, nil
}

func popcount(x uint64) uint64 {
	return uint64(bits.OnesCount64(x))
}

func (c *Cursor) RemoveRoaring(bm *roaring.Bitmap) (changed bool, err error) {
	itr, _ := bm.Containers.Iterator(0)
	for itr.Next() {
		hi, cont := itr.Value()
		if cont.N() == 0 {
			continue
		}
		// Move cursor to the key of the container.
		// Insert new container if it doesn't exist.
		if exact, err := c.Seek(hi); err != nil {
			return false, err
		} else if exact {
			f, err := c.difference(hi, cont)
			if err != nil {
				return f, err
			}
			if f {
				changed = true
			}
		}
	}
	return
}

func (c *Cursor) difference(key uint64, data *roaring.Container) (bool, error) {
	cell := c.cell()
	var container *roaring.Container
	switch cell.Type {
	case ContainerTypeArray:
		d := toArray16(cell.Data)
		container = roaring.NewContainerArray(d)
	case ContainerTypeBitmapPtr:
		_, d, err := c.tx.leafCellBitmap(toPgno(cell.Data))
		if err != nil {
			return false, errors.Wrap(err, "cursor.difference")
		}
		container = roaring.NewContainerBitmap(-1, d)
	case ContainerTypeRLE:
		d := toInterval16(cell.Data)
		container = roaring.NewContainerRun(d)
	}

	res := roaring.Difference(container, data)
	if res == nil {
		return true, c.deleteLeafCell(cell.Key)
	}

	if res.N() != container.N() {
		leaf := ConvertToLeafArgs(key, res)
		err := c.putLeafCell(leaf)
		return true, err
	}

	return false, nil
}
