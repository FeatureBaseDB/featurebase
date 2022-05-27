// Copyright 2021 Molecula Corp. All rights reserved.
package rbf

import (
	"encoding/binary"
	"fmt"
	"io"
	"sort"
	"unsafe"

	"github.com/molecula/featurebase/v3/roaring"
	"github.com/pkg/errors"
)

const (
	BitmapN = (1 << 16) / 64
)

// Cursor represents an object for traversing forward and backward in the
// keyspace. Users should initialize a cursor with either First(), Last(), or
// Seek() and then move forward or backward using Next() or Prev(), respectively.
//
// If you mutate the b-tree that a cursor is attached to then you'll need to
// re-initialize the cursor. This may be changed in the future though.
//
// Cursors can be reused in some cases; if you're keeping a cursor around,
// be sure to nil out the tx, or it will hold a reference to it.
type Cursor struct {
	tx       *Tx
	buffered bool // if true, Next() and Prev() do not move the cursor position

	// buffers
	array     [ArrayMaxSize + 1]uint16
	rle       [RLEMaxSize + 1]roaring.Interval16
	leafCells [PageSize / 8]leafCell

	// stack holds branches
	stack searchStack
}

type searchStack struct {
	top   int // current depth in elems
	elems [32]stackElem
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
			//TODO check if too big
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

// maybeConvertOversizedRunToBitmap is only called by Cursor.Add().
// bitN must be the correct new bit count.
func maybeConvertOversizedRunToBitmap(runs []roaring.Interval16, bitN int, key uint64) leafCell {
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

		// note that ElemN should be left 0 for ContainerTypeBitmap.
		return leafCell{Key: key, BitN: int(bitN), Type: ContainerTypeBitmap, Data: fromArray64(bitmap)}
	}
	return leafCell{Key: key, ElemN: len(runs), BitN: int(bitN), Type: ContainerTypeRLE, Data: fromInterval16(runs)}
}

// Add sets a bit on the underlying bitmap.
// If changed is true, this cursor will need to be reinitialized before use.
func (c *Cursor) Add(v uint64) (changed bool, err error) {
	hi, lo := highbits(v), lowbits(v)
	// Move cursor to the key of the container.
	// Insert new container if it doesn't exist.
	if exact, err := c.Seek(hi); err != nil {
		return false, err
	} else if !exact {
		return true, c.putLeafCell(leafCell{Key: hi, Type: ContainerTypeArray, ElemN: 1, BitN: 1, Data: fromArray16([]uint16{lo})})
	}

	// If the container exists and bit is not set then update the page.
	elem := &c.stack.elems[c.stack.top]
	leafPage, _, err := c.tx.readPage(elem.pgno)
	if err != nil {
		return false, err
	}
	cell := readLeafCell(leafPage, elem.index)

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
		return true, c.putLeafCell(leafCell{Key: cell.Key, Type: ContainerTypeArray, ElemN: len(other), BitN: cell.BitN + 1, Data: fromArray16(other)})

	case ContainerTypeRLE:
		runs := toInterval16(cell.Data)
		copy(c.rle[:], runs)
		run, added := runAdd(c.rle[:len(runs)], lo)
		if added {
			leaf := maybeConvertOversizedRunToBitmap(run, cell.BitN+1, cell.Key)
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
		// Update with new BitN.
		cell.BitN++
		return true, c.putLeafCell(cell)
	default:
		return false, fmt.Errorf("rbf.Cursor.Add(): invalid container type: %d", cell.Type)
	}
}

// Remove unsets a bit on the underlying bitmap.
// If changed is true, the cursor will need to be reinitialized before use.
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
	elem := &c.stack.elems[c.stack.top]
	leafPage, _, err := c.tx.readPage(elem.pgno)
	if err != nil {
		return false, err
	}
	cell := readLeafCell(leafPage, elem.index)

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
		other := c.array[:len(a)-1]
		copy(other[:i], a[:i])
		copy(other[i:], a[i+1:])
		return true, c.putLeafCell(leafCell{Key: cell.Key, Type: ContainerTypeArray, ElemN: len(other), BitN: cell.BitN - 1, Data: fromArray16(other)})

	case ContainerTypeRLE:
		r := toInterval16(cell.Data)
		i, contains := roaring.BinSearchRuns(lo, r)
		if !contains {
			return false, nil
		}
		// INVAR: lo is in run[i]
		copy(c.rle[:], r)
		runs := c.rle[:len(r)]

		if lo == runs[i].Last && lo == runs[i].Start {
			runs = append(runs[:i], runs[i+1:]...)
		} else if lo == runs[i].Last {
			runs[i].Last--
		} else if lo == c.rle[i].Start {
			runs[i].Start++
		} else if lo > runs[i].Start {
			// INVAR: Start < lo < Last.
			// We remove lo, so split into two runs:
			last := runs[i].Last
			runs[i].Last = lo - 1
			// INVAR: runs[:i] is correct, but still need to insert the new interval at i+1.
			runs = append(runs, roaring.Interval16{})
			// copy the tail first
			copy(runs[i+2:], runs[i+1:])
			// overwrite with the new interval.
			runs[i+1] = roaring.Interval16{Start: lo + 1, Last: last}
		}
		if len(runs) == 0 {
			return true, c.deleteLeafCell(cell.Key)
		}
		return true, c.putLeafCell(leafCell{Key: cell.Key, Type: ContainerTypeRLE, ElemN: len(runs), BitN: cell.BitN - 1, Data: fromInterval16(runs)})

	case ContainerTypeBitmapPtr:
		pgno, bm, err := c.tx.leafCellBitmap(toPgno(cell.Data))
		if err != nil {
			return false, errors.Wrap(err, "cursor.add")
		}
		a := cloneArray64(bm)
		if a[lo/64]&(1<<uint64(lo%64)) == 0 {
			// not present.
			return false, nil
		}

		// clear the bit
		a[lo/64] &^= 1 << uint64(lo%64)
		cell.BitN--

		if cell.BitN == 0 {
			if err := c.tx.freePgno(pgno); err != nil {
				return false, err
			}
			return true, c.deleteLeafCell(cell.Key)
		}

		// shrink if we've gotten small.
		if cell.BitN <= ArrayMaxSize {
			cbm := roaring.NewContainerBitmap(cell.BitN, a)
			// convert to array
			cbm = roaring.Optimize(cbm)

			leafCell1 := ConvertToLeafArgs(cell.Key, cbm)
			// ConvertToLeafArgs returns leafCell1 with BitN and ElemN updated.
			return true, c.putLeafCell(leafCell1)
		}

		// rewrite page, still as a bitmap.
		if err := c.tx.writeBitmapPage(pgno, fromArray64(a)); err != nil {
			return false, err
		}
		return true, c.putLeafCell(cell)

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
	elem := &c.stack.elems[c.stack.top]
	leafPage, _, err := c.tx.readPage(elem.pgno)
	if err != nil {
		return false, err
	}
	cell := readLeafCell(leafPage, elem.index)

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

// putLeafCell inserts or updates a cell into the currently position leaf page
// in the stack. If in.Key exists in the page, the cell is updated. Otherwise,
// cell is inserted.
//
// This method has two paths. First, a fast path is used if we know the insert
// will not cause the page to grow past the max page size. This path simply
// shifts bytes around in the page to make room for the new cell. The second
// path is the slow path where a page may split into two pages. This causes the
// entire page to be deserialized so that pages can more easily be split.
//
// The slow path may cause a cascade of changes to parent pages as a page split
// creates a new entry in the parent. If that entry overflows the parent then
// the parent branch page will split which could cascade up to the root. If the
// root page splits, a new branch page is created but retains the original root
// page number so that the root records do not need to be updated.
func (c *Cursor) putLeafCell(in leafCell) (err error) {
	elem := &c.stack.elems[c.stack.top]
	leafPage, isHeap, err := c.tx.readPage(elem.pgno) // the last read leaf page
	if err != nil {
		return err
	}
	cellN := readCellN(leafPage)

	// Determine if the insert/update will overflow the page.
	// If it doesn't then we can do an optimized write where we don't deserialize.
	isInsert := elem.index >= cellN || pageKeyAt(leafPage, elem.index) != in.Key
	newEstPageSize := leafPageSize(leafPage)
	if isInsert {
		newEstPageSize += in.Size() + leafCellIndexElemSize
	} else {
		newEstPageSize += in.Size() - len(readLeafCellBytesAtOffset(leafPage, readCellOffset(leafPage, elem.index)))
	}

	// Use the fast path if we are not splitting pages and the container types are the same.
	useFast := newEstPageSize+16 <= PageSize
	if useFast && !isInsert {
		if prev := readLeafCell(leafPage, elem.index); prev.Type != in.Type {
			useFast = false
		}
	}

	// Use an optimized routine to insert the leaf cell if we won't overflow.
	// We pad the estimate with 16 bytes because we do 8-byte alignment of
	// both the cell and the index.
	if useFast {
		return c.putLeafCellFast(in, isInsert)
	}

	cells := readLeafCells(leafPage, c.leafCells[:])
	cell := in
	if isInsert {
		//new cell
		if in.Type == ContainerTypeBitmap {
			//allocated bitmap()
			bitmapPgno, err := c.tx.allocatePgno()
			if err != nil {
				return err
			}
			cell.Data = fromPgno(bitmapPgno)
			cell.Type = ContainerTypeBitmapPtr
			cell.BitN = in.BitN
			cell.ElemN = in.ElemN
		}
		// Shift cells over if this is an insertion.
		cells = append(cells, leafCell{})
		copy(cells[elem.index+1:], cells[elem.index:])

	} else {
		// FB-1239: Free bitmap page if replaced container is a bitmap pointer.
		prev := cells[elem.index]
		if prev.Type == ContainerTypeBitmapPtr {
			if in.Type == ContainerTypeBitmapPtr {
				if toPgno(in.Data) != toPgno(prev.Data) { // bptr-to-bptr with different bitmap pages
					if err := c.tx.freePgno(toPgno(prev.Data)); err != nil {
						return err
					}
				}
			} else if in.Type != ContainerTypeBitmap {
				if err := c.tx.freePgno(toPgno(prev.Data)); err != nil {
					return err
				}
			}
		}

		if in.Type == ContainerTypeBitmap {
			cell = cells[elem.index]
			if cell.Type != ContainerTypeBitmapPtr {
				bitmapPgno, err := c.tx.allocatePgno()
				if err != nil {
					return errors.Wrap(err, "cursor.putLeafCell")
				}
				cell.Type = ContainerTypeBitmapPtr
				cell.Data = fromPgno(bitmapPgno)
				cell.ElemN = in.ElemN
			}
			// update the BitN regardless
			cell.BitN = in.BitN
		}
	}

	if in.Type == ContainerTypeArray && in.ElemN > ArrayMaxSize {
		//convert to bitmap
		in.Type = ContainerTypeBitmap
		a := make([]uint64, PageSize/8)
		for _, v := range toArray16(in.Data) {
			a[v/64] |= 1 << uint64(v%64)
		}
		in.Data = fromArray64(a)
		cell.Type = ContainerTypeBitmapPtr
		bitmapPgno, err := c.tx.allocatePgno()
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
	newRoot := (len(groups) > 1) && (c.stack.top == 0)
	var parents []branchCell
	origPgno := elem.pgno
	// newRoot if split occured and bottom of the stack
	for i, group := range groups {
		// First page should overwrite the original.
		// Subsequent pages should allocate new pages.
		parent := branchCell{LeftKey: group[0].Key} //<<< this is the key spot for making sure that key is correct
		if i == 0 && !newRoot {
			parent.ChildPgno = origPgno
		} else {
			if parent.ChildPgno, err = c.tx.allocatePgno(); err != nil {
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
		writePageNo(buf[:], parent.ChildPgno)
		writeFlags(buf[:], PageTypeLeaf)
		writeCellN(buf[:], len(group))

		offset := dataOffset(len(group))
		x := 0
		for j, cell := range group {
			writeLeafCell(buf[:], j, offset, cell)
			offset += align8(cell.Size())
			x++
		}

		if err := c.tx.writePage(buf[:]); err != nil {
			return err
		}

		parents = append(parents, parent)
	}

	// Free the source page once we've finished with it if it is on heap.
	if isHeap {
		freePage(leafPage)
	}

	// TODO(BBJ): Update page in buffer & cursor stack.

	// If this is not a split then exit now.
	if len(groups) == 1 {
		return nil
	}

	// Initialize a new root if we are currently the root page.
	if c.stack.top == 0 {
		assert(newRoot) // leaf write must be root when stack at root
		return c.writeRoot(origPgno, parents)
	}
	assert(!newRoot) // leaf write must NOT be root when stack not at root

	// Otherwise update existing parent.
	return c.putBranchCells(c.stack.top-1, parents)
}

// putLeafCellFast quickly insert or updates a cell on a leaf page.
// It works by shifting bytes around instead of deserializing. This must not overflow.
func (c *Cursor) putLeafCellFast(in leafCell, isInsert bool) (err error) {
	elem := &c.stack.elems[c.stack.top]
	src, isHeap, err := c.tx.readPage(elem.pgno)
	if err != nil {
		return err
	}
	srcCellN := readCellN(src)

	// Determine the cell count of the new page.
	dstCellN := srcCellN
	if isInsert {
		dstCellN++
	}

	// Write page header.
	dst := allocPage()
	writePageNo(dst, readPageNo(src))
	writeFlags(dst, PageTypeLeaf)
	writeCellN(dst, dstCellN)

	// Copy data before index.
	offset := dataOffset(dstCellN)

	if elem.index > 0 {
		srcStart := dataOffset(srcCellN)
		srcEnd := readCellEndingOffset(src, elem.index-1)
		shiftN := offset - srcStart
		copy(dst[offset:], src[srcStart:srcEnd])
		offset += align8(srcEnd - srcStart)

		// Rewrite initial index slots by position moved.
		for i := 0; i < elem.index; i++ {
			writeCellOffset(dst, i, readCellOffset(src, i)+shiftN)
		}
	}

	// Insert new row.
	writeLeafCell(dst[:], elem.index, offset, in)
	offset += align8(in.Size())

	// Copy data after inserted element.
	if (isInsert && elem.index < srcCellN) || (!isInsert && elem.index < srcCellN-1) {
		var srcStart int
		if isInsert {
			srcStart = readCellOffset(src, elem.index)
		} else {
			srcStart = readCellOffset(src, elem.index+1)
		}
		srcEnd := readCellEndingOffset(src, srcCellN-1)
		copy(dst[offset:], src[srcStart:srcEnd])

		// Rewrite ending index slots by position moved.
		shiftN := offset - srcStart
		for i := elem.index + 1; i < dstCellN; i++ {
			srci := i
			if isInsert {
				srci--
			}
			writeCellOffset(dst, i, readCellOffset(src, srci)+shiftN)
		}
	}

	// Write new page to dirty page cache.
	if err := c.tx.writePage(dst); err != nil {
		return err
	}

	// Free page if on heap.
	if isHeap {
		freePage(src)
	}
	return nil
}

// deleteLeafCell removes a cell from the currently positioned page & index.
// If the removal causes the leaf page to have no more elements then its entry
// is removed from the parent. If the removal changes the first entry in the
// leaf page then the entry will be updated in the parent branch page.
func (c *Cursor) deleteLeafCell(key uint64) (err error) {
	elem := &c.stack.elems[c.stack.top]
	leafPage, _, err := c.tx.readPage(elem.pgno)
	if err != nil {
		return err
	}
	cells := readLeafCells(leafPage, c.leafCells[:])
	oldPageKey := cells[0].Key
	cell := readLeafCell(leafPage, elem.index)

	if cell.Type == ContainerTypeBitmapPtr {
		if err := c.tx.freePgno(toPgno(cell.Data)); err != nil {
			return err
		}
	}

	// If no more cells exist and we have a parent, remove from parent.
	if c.stack.top > 0 && len(cells) == 1 {
		if err := c.tx.freePgno(elem.pgno); err != nil {
			return err
		}
		return c.deleteBranchCell(c.stack.top-1, cells[0].Key)
	}

	// Remove matching cell from list.
	copy(cells[elem.index:], cells[elem.index+1:])
	cells[len(cells)-1] = leafCell{}
	cells = cells[:len(cells)-1]
	// Write cells to page.
	buf := allocPage()
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
	if c.stack.top > 0 && oldPageKey != cells[0].Key {
		return c.updateBranchCell(c.stack.top-1, cells[0].Key)
	}
	return nil
}

// putBranchCells updates a branch page with one or more cells. If the cells
// cause the branch page to split then a new entry will be created in the
// parent page. If this branch page is a root then a new root will be created.
func (c *Cursor) putBranchCells(stackIndex int, newCells []branchCell) (err error) {
	elem := &c.stack.elems[stackIndex]

	// Read branch page from disk. The current buffer is the leaf page.
	page, _, err := c.tx.readPage(elem.pgno)
	if err != nil {
		return err
	}

	cells := readBranchCells(page)

	if len(cells) == 0 {
		cells = make([]branchCell, 1)
	}

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
		parent := branchCell{LeftKey: group[0].LeftKey}
		if i == 0 && !newRoot {
			parent.ChildPgno = origPgno
		} else {
			if parent.ChildPgno, err = c.tx.allocatePgno(); err != nil {
				return fmt.Errorf("cannot allocate branch: %w", err)
			}
		}
		parents = append(parents, parent)

		// Write cells to page.
		var buf [PageSize]byte
		writePageNo(buf[:], parents[i].ChildPgno)
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

// updateBranchCell updates the key for cell in the branch. Branch elements
// are fixed size so this cannot cause a page split.
func (c *Cursor) updateBranchCell(stackIndex int, newKey uint64) (err error) {
	elem := &c.stack.elems[stackIndex]

	// Read branch page from disk. The current buffer is the leaf page.
	page, _, err := c.tx.readPage(elem.pgno)
	if err != nil {
		return err
	}
	cells := readBranchCells(page)
	oldPageKey := cells[0].LeftKey

	// Update key in branch cell.
	cells[elem.index].LeftKey = newKey

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

	if stackIndex > 0 && oldPageKey != cells[0].LeftKey {
		return c.updateBranchCell(stackIndex-1, cells[0].LeftKey)
	}
	return nil
}

// deleteBranchCell removes a cell from a branch page. If no more elements
// exist in a branch page then it is removed. If an empty branch page is the
// root page then it is converted to an empty leaf page as branch pages are not
// allowed to be empty.
func (c *Cursor) deleteBranchCell(stackIndex int, key uint64) (err error) {
	elem := &c.stack.elems[stackIndex]

	// Read branch page from disk. The current buffer is the leaf page.
	page, _, err := c.tx.readPage(elem.pgno)
	if err != nil {
		return err
	}
	cells := readBranchCells(page)
	oldPageKey := cells[0].LeftKey

	// Remove cell from branch.
	copy(cells[elem.index:], cells[elem.index+1:])
	cells[len(cells)-1] = branchCell{}
	cells = cells[:len(cells)-1]

	// Branches are not allowed to have zero element so we must remove the page
	// or, in the case of the root page, convert to a leaf page.
	if len(cells) == 0 {
		// If this is the root page, convert to leaf page.
		if stackIndex == 0 {
			var buf [PageSize]byte
			writePageNo(buf[:], elem.pgno)
			writeFlags(buf[:], PageTypeLeaf)
			writeCellN(buf[:], len(cells))
			return c.tx.writePage(buf[:])
		}

		// If this is a non-root page, free and remove from parent.
		if err := c.tx.freePgno(elem.pgno); err != nil {
			return err
		}
		return c.deleteBranchCell(stackIndex-1, oldPageKey)
	}

	// If the root only has one node, replace it with its child.
	if stackIndex == 0 && len(cells) == 1 {
		target, _, err := c.tx.readPage(cells[0].ChildPgno)
		if err != nil {
			return err
		}

		buf := allocPage()
		copy(buf, target)
		writePageNo(buf[:], elem.pgno)

		if err := c.tx.freePgno(cells[0].ChildPgno); err != nil {
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

	assert(readCellN(buf[:]) > 0) // must have at least one cell

	if err := c.tx.writePage(buf[:]); err != nil {
		return err
	}

	if stackIndex > 0 && len(cells) > 0 && oldPageKey != cells[0].LeftKey {
		return c.updateBranchCell(stackIndex-1, cells[0].LeftKey)
	}
	return nil
}

// writeRoot writes a new branch page at the root with the given cells.
// The root of a b-tree should always stay the same, even in the case of a
// split. This allows us to not rewrite the root record for the b-tree.
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
		thresh := int(float64(PageSize) * globalBranchFillPct)
		if cellN != 0 && (dataOffset(cellN+1)+dataSize+sz) > thresh {
			slices, dataSize = append(slices, nil), 0
		} else if cellN != 0 && cell.Type == ContainerTypeArray && cell.ElemN > ArrayMaxSize {
			slices, dataSize = append(slices, nil), 0
			sz = PageSize
		}

		// Append to current slice & increase total cell data size.
		slices[len(slices)-1] = append(slices[len(slices)-1], cell)
		dataSize += sz
	}

	return slices
}

var globalBranchFillPct = 0.60

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

		thresh := int(float64(PageSize) * globalBranchFillPct)
		if cellN != 0 && (dataOffset(cellN+1)+dataSize+sz) > thresh {
			slices, dataSize = append(slices, nil), 0
		}

		// Append to current slice & increase total cell data size.
		slices[len(slices)-1] = append(slices[len(slices)-1], cell)
		dataSize += sz
	}

	return slices
}

// pageKeyAt returns the key at the given index of the page.
func pageKeyAt(page []byte, index int) uint64 {
	offset := readCellOffset(page, index)
	return *(*uint64)(unsafe.Pointer(&page[offset]))
}

// First moves to the first element of the btree. Returns io.EOF if no elements
// exist. The first call to Next() will not move the position but subsequent
// calls will move the position forward until it reaches the end and return io.EOF.
func (c *Cursor) First() error {
	c.buffered = true

	for c.stack.top = 0; ; c.stack.top++ {
		elem := &c.stack.elems[c.stack.top]

		buf, _, err := c.tx.readPage(elem.pgno)
		if err != nil {
			return err
		}

		switch typ := readFlags(buf); typ {
		case PageTypeBranch:
			elem.index = 0

			if n := readCellN(buf); elem.index >= n { // branch cell index must less than cell count
				return fmt.Errorf("branch cell index out of range: pgno=%d i=%d n=%d", elem.pgno, elem.index, n)
			}

			// Read cell pgno into the next stack level.
			cell := readBranchCell(buf, elem.index)

			c.stack.elems[c.stack.top+1] = stackElem{
				pgno: cell.ChildPgno,
				key:  cell.LeftKey,
			}

		case PageTypeLeaf:
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

	for c.stack.top = 0; ; c.stack.top++ {
		elem := &c.stack.elems[c.stack.top]

		buf, _, err := c.tx.readPage(elem.pgno)
		if err != nil {
			return err
		}

		switch typ := readFlags(buf); typ {
		case PageTypeBranch:
			elem.index = readCellN(buf) - 1

			// Read cell pgno into the next stack level.
			cell := readBranchCell(buf, elem.index)
			c.stack.elems[c.stack.top+1] = stackElem{
				pgno: cell.ChildPgno,
				key:  cell.LeftKey,
			}

		case PageTypeLeaf:
			elem.index = readCellN(buf) - 1
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
// TODO: what happens if there are no more containers?!?!
func (c *Cursor) Seek(key uint64) (exact bool, err error) {
	// c.stack.elems[0].pgno = c.bitmap.root
	c.buffered = true
	for c.stack.top = 0; ; c.stack.top++ {
		elem := &c.stack.elems[c.stack.top]
		assert(elem.pgno != 0) // cursor should never point to page zero (meta)

		buf, _, err := c.tx.readPage(elem.pgno)
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

			c.stack.elems[c.stack.top+1] = stackElem{
				pgno: cell.ChildPgno,
				key:  cell.LeftKey,
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
			return xact, nil

		default:
			return false, fmt.Errorf("rbf.Cursor.Seek(): invalid page type: pgno=%d type=%d", elem.pgno, typ)
		}
	}
}

// Next moves to the next element of the btree. Returns EOF if no more elements exist.
func (c *Cursor) Next() error {
	elem := &c.stack.elems[c.stack.top]
	leafPage, _, err := c.tx.readPage(elem.pgno)
	if err != nil {
		return err
	}

	if c.buffered {
		c.buffered = false

		// Move to next available element if we are past the last cell in the page.
		if elem.index >= readCellN(leafPage) {
			return c.goNextPage()
		}
		return nil
	}

	// Move forward to the next leaf element if available.
	if elem.index < readCellN(leafPage)-1 {
		elem.index++
		return nil
	}
	return c.goNextPage()
}

// Prev moves to the previous element of the btree. Returns io.EOF if at the
// beginning of the b-tree.
func (c *Cursor) Prev() error {
	if c.buffered {
		c.buffered = false
		return nil
	}

	// Move forward to the next leaf element if available.
	if elem := &c.stack.elems[c.stack.top]; elem.index > 0 {
		elem.index--
		return nil
	}

	// Move up the stack until we can move forward one element.
	for c.stack.top--; c.stack.top >= 0; c.stack.top-- {
		elem := &c.stack.elems[c.stack.top]
		if elem.index > 0 {
			elem.index--
			break
		}
	}

	// No more elements, return EOF.
	if c.stack.top == -1 {
		c.stack.top = 0
		return io.EOF
	}

	// Traverse back down the stack to find the first element in each page.
	for ; ; c.stack.top++ {
		elem := &c.stack.elems[c.stack.top]

		buf, _, err := c.tx.readPage(elem.pgno)
		if err != nil {
			return err
		}

		switch typ := readFlags(buf); typ {
		case PageTypeBranch:
			cell := readBranchCell(buf, elem.index)

			c.stack.elems[c.stack.top+1] = stackElem{
				pgno: cell.ChildPgno,
				key:  cell.LeftKey,
			}

		case PageTypeLeaf:
			elem.index = readCellN(buf) - 1
			return nil
		default:
			return fmt.Errorf("rbf.Cursor.Prev(): invalid page type: pgno=%d type=%d", elem.pgno, typ)
		}
	}
}

// Key returns the key for the container the cursor is currently pointing to.
func (c *Cursor) Key() uint64 {
	elem := &c.stack.elems[c.stack.top]
	leafPage, _, _ := c.tx.readPage(elem.pgno)
	if readCellN(leafPage[:]) == 0 {
		return 0
	}
	return readLeafCellKey(leafPage, elem.index)
}

// Values returns the values for the container the cursor is currently pointing to.
func (c *Cursor) Values() []uint16 {
	elem := &c.stack.elems[c.stack.top]
	leafPage, _, _ := c.tx.readPage(elem.pgno)
	if readCellN(leafPage[:]) == 0 {
		return nil
	}
	cell := readLeafCell(leafPage, elem.index)
	return cell.Values(c.tx)
}

// stackElem represents a single element on the cursor stack.
type stackElem struct {
	pgno  uint32 // current page number
	index int    // cell index
	key   uint64 // element key
}

func (se *stackElem) equal(se2 *stackElem) bool {
	if se.pgno != se2.pgno {
		return false
	}
	if se.index != se2.index {
		return false
	}
	if se.key != se2.key {
		return false
	}
	return true
}

func (se *stackElem) String() string {
	return fmt.Sprintf("stackElem{pgno:%v  index:%v  key:%v}", int(se.pgno) /*,tx.pageTypeDesc(se.pgno)*/, se.index, int(se.key))
}

func (se *stackElem) clear() {
	se.pgno = 0
	se.index = 0
	se.key = 0
}

// suppress unused warnings - these can be useful for debugging
var _ = (&stackElem{}).clear
var _ = (&stackElem{}).String
var _ = (&stackElem{}).equal

// goNextPage moves up the stack until an element has additional elements
// available after the current position. It then traverses back down the stack
// to position at the next leaf page element.
func (c *Cursor) goNextPage() error {
	for c.stack.top--; c.stack.top >= 0; c.stack.top-- {
		elem := &c.stack.elems[c.stack.top]
		if buf, _, err := c.tx.readPage(elem.pgno); err != nil {
			return err
		} else if n := readCellN(buf); elem.index+1 < n {
			elem.index++
			break
		}
	}

	// No more elements, return EOF.
	if c.stack.top == -1 {
		c.stack.top = 0
		return io.EOF
	}

	// Traverse back down the stack to find the first element in each page.
	for ; ; c.stack.top++ {
		elem := &c.stack.elems[c.stack.top]
		buf, _, err := c.tx.readPage(elem.pgno)
		if err != nil {
			return err
		}

		switch typ := readFlags(buf); typ {
		case PageTypeBranch:
			cell := readBranchCell(buf, elem.index)
			c.stack.elems[c.stack.top+1] = stackElem{
				pgno: cell.ChildPgno,
				key:  cell.LeftKey,
			}
		case PageTypeLeaf:
			elem.index = 0
			return nil
		default:
			return fmt.Errorf("rbf.Cursor.Next(): invalid page type: pgno=%d type=%d", elem.pgno, typ)
		}
	}
}

func ConvertToLeafArgs(key uint64, c *roaring.Container) (result leafCell) {
	result.Key = key
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
			// result.ElemN is 0 or undefined for bitmap
			return
		}
		result.Type = ContainerTypeArray
		result.Data = fromArray16(a)
		result.ElemN = int(c.N())
		return
	case 2: //bitmap
		result.Type = ContainerTypeBitmap
		result.Data = fromArray64(roaring.AsBitmap(c))
		// result.ElemN is 0 or undefined for bitmap
		return
	case 3: //run
		r := roaring.AsRuns(c)
		if len(r) > RLEMaxSize {
			roaring.ConvertRunToBitmap(c)
			result.Type = ContainerTypeBitmap
			result.Data = fromArray64(roaring.AsBitmap(c))
			// result.ElemN is 0 or undefined for bitmap

			return
		}
		result.ElemN = len(r) // ElemN is the number of runs
		result.Type = ContainerTypeRLE
		result.Data = fromInterval16(r)
		return
	}
	return
}

func (c *Cursor) merge(key uint64, data *roaring.Container) (bool, error) {
	elem := &c.stack.elems[c.stack.top]
	leafPage, _, err := c.tx.readPage(elem.pgno)
	if err != nil {
		return false, err
	}
	cell := readLeafCell(leafPage, elem.index)

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
		if leaf.BitN == 0 {
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
	elem := &c.stack.elems[c.stack.top]
	leafPage, _, err := c.tx.readPage(elem.pgno)
	if err != nil {
		return false, err
	}
	cell := readLeafCell(leafPage, elem.index)

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
	if res.N() == 0 {
		return true, c.deleteLeafCell(cell.Key)
	}

	if res.N() != container.N() {
		leaf := ConvertToLeafArgs(key, res)
		err := c.putLeafCell(leaf)
		return true, err
	}

	return false, nil
}

// Close closes a cursor out, invalidating it for future use, and puts it
// back in a pool to reduce allocations. Contrast with unpooledClose, which
// you probably shouldn't use.
func (c *Cursor) Close() {
	assert(c != nil)
	c.unpooledClose()
	cursorSyncPool.Put(c)
}

// unpooledClose is the part of a close operation which does not put the
// cursor back in the pool. It is useful only for cases where we're reusing
// a cursor, such as a Db's freelistCursor.
func (c *Cursor) unpooledClose() {
	c.tx = nil
}

func keysFromParents(parents []branchCell) (ckeys []int) {
	for _, par := range parents {
		ckeys = append(ckeys, int(par.LeftKey))
	}
	return
}

var _ = (&Cursor{}).showCursorStack

func (c *Cursor) showCursorStack() (r string) {
	r = fmt.Sprintf("top = %v\n", c.stack.top)
	for i := 0; i <= c.stack.top; i++ {
		r += fmt.Sprintf("  [%02v] %v\n", i, c.stack.elems[i].String())
	}
	return
}
