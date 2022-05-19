// Copyright 2021 Molecula Corp. All rights reserved.
// Package rbf implements the roaring b-tree file format.
package rbf

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sync"
	"time"
	"unsafe"

	"github.com/benbjohnson/immutable"
	"github.com/molecula/featurebase/v3/roaring"
	"github.com/molecula/featurebase/v3/shardwidth"
	"github.com/molecula/featurebase/v3/vprint"
)

const (
	// Magic is the first 4 bytes of the RBF file.
	Magic = "\xFFRBF"

	// PageSize is the fixed size for every database page.
	PageSize = 8192

	// ShardWidth represents the number of bits per shard.
	ShardWidth = 1 << shardwidth.Exponent

	// RowValueMask masks the low bits for a row.
	RowValueMask = ShardWidth - 1

	// ArrayMaxSize represents the maximum size of array containers.
	// This is sligtly less than roaring to accommodate the page header.
	ArrayMaxSize = 4079

	// RLEMaxSize represents the maximum size of run length encoded containers.
	RLEMaxSize = 2039
)

const maxBranchCellsPerPage = int((PageSize - branchPageHeaderSize) / (branchCellIndexElemSize + unsafe.Sizeof(branchCell{})))

// Page types.
const (
	PageTypeRootRecord   = 1
	PageTypeLeaf         = 2
	PageTypeBranch       = 4
	PageTypeBitmapHeader = 8  // Only used by the WAL for marking next page
	PageTypeBitmap       = 16 // Only used internally when walking the b-tree
)

// Meta commit/rollback flags.
const (
	MetaPageFlagCommit   = 1
	MetaPageFlagRollback = 2
)

type ContainerType int

// Container types.
const (
	ContainerTypeNone ContainerType = iota
	ContainerTypeArray
	ContainerTypeRLE
	ContainerTypeBitmap
	ContainerTypeBitmapPtr
)

// ContainerTypeString returns a string representation of the container type.
func (typ ContainerType) String() string {
	switch typ {
	case ContainerTypeNone:
		return "none"
	case ContainerTypeArray:
		return "array"
	case ContainerTypeRLE:
		return "rle"
	case ContainerTypeBitmap:
		return "bitmap"
	case ContainerTypeBitmapPtr:
		return "bitmap-ptr"
	default:
		return fmt.Sprintf("unknown<%d>", typ)
	}
}

const (
	rootRecordPageHeaderSize = 12
	rootRecordHeaderSize     = 4 + 2     // pgno, len(name)
	leafCellHeaderSize       = 8 + 4 + 6 // key, type, count
	leafPageHeaderSize       = 4 + 4 + 2 // pgno, flags, cell n
	leafCellIndexElemSize    = 2
	branchPageHeaderSize     = 4 + 4 + 2 // pgno, flags, cell n
	branchCellSize           = 8 + 4 + 4 // key, flags, pgno
	branchCellIndexElemSize  = 2
)

var (
	ErrTxClosed           = errors.New("transaction closed")
	ErrTxNotWritable      = errors.New("transaction not writable")
	ErrBitmapNameRequired = errors.New("bitmap name required")
	ErrBitmapNotFound     = errors.New("bitmap not found")
	ErrBitmapExists       = errors.New("bitmap already exists")
	ErrTxTooLarge         = errors.New("rbf tx too large")
)

// Debug is just a temporary flag used for debugging.
var Debug bool

// Magic32 returns the magic bytes as a big endian encoded uint32.
func Magic32() uint32 {
	return binary.BigEndian.Uint32([]byte(Magic))
}

// Meta page helpers

// IsMetaPage returns  true if page is a meta page.
func IsMetaPage(page []byte) bool {
	return bytes.Equal(readMetaMagic(page), []byte(Magic))
}

func readMetaMagic(page []byte) []byte { return page[0:4] }
func writeMetaMagic(page []byte)       { copy(page, Magic) }

func readMetaPageN(page []byte) uint32     { return binary.BigEndian.Uint32(page[8:]) }
func writeMetaPageN(page []byte, n uint32) { binary.BigEndian.PutUint32(page[8:], n) }

func readMetaWALID(page []byte) int64         { return int64(binary.BigEndian.Uint64(page[12:])) }
func writeMetaWALID(page []byte, walID int64) { binary.BigEndian.PutUint64(page[12:], uint64(walID)) }

func readMetaRootRecordPageNo(page []byte) uint32        { return binary.BigEndian.Uint32(page[20:]) }
func writeMetaRootRecordPageNo(page []byte, pgno uint32) { binary.BigEndian.PutUint32(page[20:], pgno) }

func readMetaFreelistPageNo(page []byte) uint32        { return binary.BigEndian.Uint32(page[24:]) }
func writeMetaFreelistPageNo(page []byte, pgno uint32) { binary.BigEndian.PutUint32(page[24:], pgno) }

/* lint
func readMetaChecksum(page []byte) uint32 {
	return binary.BigEndian.Uint32(page[PageSize-4 : PageSize])
}
func writeMetaChecksum(page []byte, chksum uint32) {
	binary.BigEndian.PutUint32(page[PageSize-4:PageSize], chksum)
}
*/

// Root record page helpers

func WalkRootRecordPages(page []byte) uint32 { return binary.BigEndian.Uint32(page[8:]) }
func writeRootRecordOverflowPgno(page []byte, pgno uint32) {
	binary.BigEndian.PutUint32(page[8:], pgno)
}

func readRootRecords(page []byte) (records []*RootRecord, err error) {
	for data := page[rootRecordPageHeaderSize:]; ; {
		var rec *RootRecord
		if rec, data, err = ReadRootRecord(data); err != nil {
			return records, err
		} else if rec == nil {
			return records, nil
		}
		records = append(records, rec)
	}
}

// writeRootRecords is only called by tx.go Tx.writeRootRecordPages().
// We can return io.ErrShortBuffer in err. If we still have records
// to write that don't fit on page, remain will point to the next
// record that hasn't yet been written.
func writeRootRecords(page []byte, itr *immutable.SortedMapIterator) (err error) {
	data := page[rootRecordPageHeaderSize:]

	for !itr.Done() {
		name, pgno := itr.Next()

		data, err = WriteRootRecord(data, &RootRecord{Name: name.(string), Pgno: pgno.(uint32)})
		if err != nil {
			itr.Seek(name)
			return err
		}
	}
	return nil
}

// Branch & leaf page helpers

func readPageNo(page []byte) uint32     { return binary.BigEndian.Uint32(page[0:4]) }
func writePageNo(page []byte, v uint32) { binary.BigEndian.PutUint32(page[0:4], v) }

func readFlags(page []byte) uint32     { return binary.BigEndian.Uint32(page[4:8]) }
func writeFlags(page []byte, v uint32) { binary.BigEndian.PutUint32(page[4:8], v) }

func readCellN(page []byte) int     { return int(binary.BigEndian.Uint16(page[8:10])) }
func writeCellN(page []byte, v int) { binary.BigEndian.PutUint16(page[8:10], uint16(v)) }

func readCellOffset(page []byte, i int) int {
	return int(binary.BigEndian.Uint16(page[10+(i*2):]))
}

func writeCellOffset(page []byte, i int, v int) {
	binary.BigEndian.PutUint16(page[10+(i*2):], uint16(v))
}

// readCellEndingOffset returns the last byte position of the i-th cell.
func readCellEndingOffset(page []byte, i int) int {
	offset := readCellOffset(page, i)
	return offset + len(readLeafCellBytesAtOffset(page, offset))
}

func dataOffset(n int) int {
	return align8(10 + (n * 2))
}

func IsBitmapHeader(page []byte) bool {
	return readFlags(page) == PageTypeBitmapHeader
}

type RootRecord struct {
	Name string
	Pgno uint32
}

// ReadRootRecord reads the page number & name for a root record.
// If there is not enough space or the pgno is zero then a nil record is returned.
// Returns the remaining buffer.
func ReadRootRecord(data []byte) (rec *RootRecord, remaining []byte, err error) {
	// Ensure there is enough space to read the pgno & name length.
	if len(data) < rootRecordHeaderSize {
		return nil, data, nil
	}

	// Read root page number.
	rec = &RootRecord{}
	rec.Pgno = binary.BigEndian.Uint32(data)
	if rec.Pgno == 0 {
		return nil, data, nil
	}
	data = data[4:]

	// Read name length.
	sz := int(binary.BigEndian.Uint16(data))
	data = data[2:]
	if len(data) < sz {
		return nil, data, fmt.Errorf("short root record buffer")
	}

	// Read name and allocate as string on heap.
	rec.Name, data = string(data[:sz]), data[sz:]

	return rec, data, nil
}

// WriteRootRecord writes a root record with the pgno & name.
// Returns io.ErrShortBuffer if there is not enough space.
func WriteRootRecord(data []byte, rec *RootRecord) (remaining []byte, err error) {
	// Ensure record data is valid.
	if rec == nil {
		return data, fmt.Errorf("root record required")
	} else if rec.Name == "" {
		return data, fmt.Errorf("root record name required")
	} else if rec.Pgno == 0 {
		return data, fmt.Errorf("invalid root record pgno: %d", rec.Pgno)
	}

	// Ensure there is enough space to write the full record.
	if len(data) < rootRecordHeaderSize+len(rec.Name) {
		return data, io.ErrShortBuffer
	}

	// Write root page number.
	binary.BigEndian.PutUint32(data, rec.Pgno)
	data = data[4:]

	// Write name length.
	binary.BigEndian.PutUint16(data, uint16(len(rec.Name)))
	data = data[2:]

	// Write name.
	copy(data, rec.Name)
	data = data[len(rec.Name):]

	return data, nil
}

func align8(offset int) int {
	if offset%8 == 0 {
		return offset
	}
	return offset + (8 - (offset & 0x7))
}

// leafCell represents a leaf cell.
type leafCell struct {
	Key  uint64
	Type ContainerType

	// ElemN is the number of "things" in Data:
	//  for an array container the number of integers in the array.
	//  for an RLE, number of intervals.
	// ElemN is undefined or 0 for ContainerTypeBitmap
	ElemN int

	BitN int
	Data []byte
}

// Size returns the size of the leaf cell, in bytes.
func (c *leafCell) Size() int {
	return leafCellHeaderSize + len(c.Data)
}

// Bitmap returns a bitmap representation of the cell data.
func (c *leafCell) Bitmap(tx *Tx) []uint64 {
	switch c.Type {
	case ContainerTypeArray:
		buf := make([]uint64, PageSize/8)
		for _, v := range toArray16(c.Data) {
			buf[v/64] |= 1 << uint64(v%64)
		}
		return buf
	case ContainerTypeRLE:
		buf := make([]uint64, PageSize/8)
		for _, iv := range toInterval16(c.Data) {
			w1, w2 := iv.Start/64, iv.Last/64
			b1, b2 := iv.Start&63, iv.Last&63
			m1 := (uint64(1) << b1) - 1
			m2 := (((uint64(1) << b2) - 1) << 1) | 1
			if w1 == w2 {
				buf[w1] |= (m2 &^ m1)
				continue
			}
			buf[w2] |= m2
			buf[w1] |= ^m1
			words := buf[w1+1 : w2]
			for i := range words {
				words[i] = ^uint64(0)
			}
		}
		return buf
	case ContainerTypeBitmapPtr:
		_, bm, _ := tx.leafCellBitmap(toPgno(c.Data))
		return bm
	default:
		vprint.PanicOn(fmt.Errorf("invalid container type: %d", c.Type))
	}
	return nil
}

// Values returns a slice of 16-bit values from a container.
func (c *leafCell) Values(tx *Tx) []uint16 {
	switch c.Type {
	case ContainerTypeArray:
		return toArray16(c.Data)
	case ContainerTypeRLE:
		a := make([]uint16, c.BitN)
		n := int32(0)
		for _, r := range toInterval16(c.Data) {
			for v := int(r.Start); v <= int(r.Last); v++ {
				a[n] = uint16(v)
				n++
			}
		}
		a = a[:n]
		return a
	case ContainerTypeBitmapPtr:
		_, bm, _ := tx.leafCellBitmap(toPgno(c.Data))
		return bitmapValues(bm)
	case ContainerTypeNone:
		return []uint16{}
	default:
		vprint.PanicOn(fmt.Errorf("invalid container type: %d", c.Type))
	}
	return nil
}

func bitmapValues(bm []uint64) []uint16 {
	a := make([]uint16, 0, BitmapN*64)
	for i, v := range bm {
		for j := uint(0); j < 64; j++ {
			if v&(1<<j) != 0 {
				a = append(a, (uint16(i)*64)+uint16(j))
			}
		}
	}
	return a
}

// firstValue the first value from the container.
func (c *leafCell) firstValue(tx *Tx) uint16 {
	switch c.Type {
	case ContainerTypeArray:
		a := toArray16(c.Data)
		return a[0]
	case ContainerTypeRLE:
		r := toInterval16(c.Data)
		return r[0].Start
	case ContainerTypeBitmapPtr:
		_, slc, err := tx.leafCellBitmap(toPgno(c.Data))
		vprint.PanicOn(err)
		for i, v := range slc {
			for j := uint(0); j < 64; j++ {
				if v&(1<<j) != 0 {
					return (uint16(i) * 64) + uint16(j)
				}
			}
		}
		vprint.PanicOn(fmt.Errorf("rbf.leafCell.firstValue(): no values set in bitmap container: key=%d", c.Key))
	default:
		vprint.PanicOn(fmt.Errorf("invalid container type: %d", c.Type))
	}

	return 0
}

// helper for lastValue()
func (c *leafCell) lastValueFromBitmap(a []uint64) uint16 {
	for i := len(a) - 1; i >= 0; i-- {
		for j := 63; j >= 0; j-- {
			if a[i]&(1<<j) != 0 {
				return (uint16(i) * 64) + uint16(j)
			}
		}
	}
	vprint.PanicOn(fmt.Errorf("rbf.leafCell.lastValueFromBitmap(): no values set in bitmap container: key=%d", c.Key))
	return 0
}

// lastValue the last value from the container.
func (c *leafCell) lastValue(tx *Tx) uint16 {
	switch c.Type {
	case ContainerTypeArray:
		a := toArray16(c.Data)
		return a[len(a)-1]
	case ContainerTypeRLE:
		r := toInterval16(c.Data)
		return r[len(r)-1].Last

	case ContainerTypeBitmap:
		a := toArray64(c.Data)
		return c.lastValueFromBitmap(a)

	case ContainerTypeBitmapPtr:
		_, a, err := tx.leafCellBitmap(toPgno(c.Data))
		vprint.PanicOn(err)
		return c.lastValueFromBitmap(a)
	default:
		vprint.PanicOn(fmt.Errorf("invalid container type: %d", c.Type))
	}
	return 0
}

// countRange returns the bit count within the given range.
// We have to take int32 rather than uint16 because the interval is [start, end),
// and otherwise we have no way to ask to count the entire container (the
// high bit will be missed).
func (c *leafCell) countRange(tx *Tx, start, end int32) (n int) {
	// If the full range is being queried, simply use the precalculated count.
	if start == 0 && end > math.MaxUint16 {
		return c.BitN
	}

	switch c.Type {
	case ContainerTypeArray:
		return int(roaring.ArrayCountRange(toArray16(c.Data), start, end))
	case ContainerTypeRLE:
		return int(roaring.RunCountRange(toInterval16(c.Data), start, end))
	case ContainerTypeBitmap:
		return int(roaring.BitmapCountRange(toArray64(c.Data), start, end))
	case ContainerTypeBitmapPtr:
		_, a, err := tx.leafCellBitmap(toPgno(c.Data))
		vprint.PanicOn(err)
		return int(roaring.BitmapCountRange(a, start, end))
	default:
		vprint.PanicOn(fmt.Errorf("invalid container type: %d", c.Type))
	}
	return
}

func readLeafCellKey(page []byte, i int) uint64 {
	offset := readCellOffset(page, i)
	assert(offset < len(page)) // cell read beyond page size
	return *(*uint64)(unsafe.Pointer(&page[offset]))
}

func readLeafCell(page []byte, i int) leafCell {
	assert(i < readCellN(page)) // cell index exceeds cell count
	offset := readCellOffset(page, i)

	buf := page[offset:]

	var cell leafCell
	cell.Key = *(*uint64)(unsafe.Pointer(&buf[0]))
	cell.Type = ContainerType(*(*uint32)(unsafe.Pointer(&buf[8])))
	cell.ElemN = int(*(*uint16)(unsafe.Pointer(&buf[12])))
	cell.BitN = int(*(*uint32)(unsafe.Pointer(&buf[14])))

	switch cell.Type {
	case ContainerTypeArray:
		cell.Data = buf[leafCellHeaderSize : leafCellHeaderSize+(cell.ElemN*2)]
	case ContainerTypeRLE:
		cell.Data = buf[leafCellHeaderSize : leafCellHeaderSize+(cell.ElemN*4)]
	case ContainerTypeBitmapPtr:
		cell.Data = buf[leafCellHeaderSize : leafCellHeaderSize+4]
	default:
	}

	return cell
}

func readLeafCellInto(cell *leafCell, page []byte, i int) {
	assert(i < readCellN(page)) // cell index exceeds cell count
	offset := readCellOffset(page, i)

	buf := page[offset:]

	cell.Key = *(*uint64)(unsafe.Pointer(&buf[0]))
	cell.Type = ContainerType(*(*uint32)(unsafe.Pointer(&buf[8])))
	cell.ElemN = int(*(*uint16)(unsafe.Pointer(&buf[12])))
	cell.BitN = int(*(*uint32)(unsafe.Pointer(&buf[14])))

	switch cell.Type {
	case ContainerTypeArray:
		cell.Data = buf[leafCellHeaderSize : leafCellHeaderSize+(cell.ElemN*2)]
	case ContainerTypeRLE:
		cell.Data = buf[leafCellHeaderSize : leafCellHeaderSize+(cell.ElemN*4)]
	case ContainerTypeBitmapPtr:
		cell.Data = buf[leafCellHeaderSize : leafCellHeaderSize+4]
	default:
		cell.Data = nil
	}
}

func readLeafCells(page []byte, buf []leafCell) []leafCell {
	n := readCellN(page)
	cells := buf[:n]
	for i := 0; i < n; i++ {
		cells[i] = readLeafCell(page, i)
	}
	return cells
}

func readLeafCellBytesAtOffset(page []byte, offset int) []byte {
	buf := page[offset:]
	typ := ContainerType(*(*uint32)(unsafe.Pointer(&buf[8])))
	n := int(*(*uint16)(unsafe.Pointer(&buf[12])))

	switch typ {
	case ContainerTypeArray:
		return buf[:leafCellHeaderSize+(n*2)]
	case ContainerTypeRLE:
		return buf[:leafCellHeaderSize+(n*4)]
	case ContainerTypeBitmapPtr:
		return buf[:leafCellHeaderSize+4]
	default:
		vprint.PanicOn(fmt.Errorf("invalid cell type: %d", typ))
	}
	return nil
}

// leafPageSize returns the number of bytes used on a leaf page.
func leafPageSize(page []byte) int {
	cellN := readCellN(page)
	if cellN == 0 {
		return leafPageHeaderSize
	}

	// Determine the offset & size of the last element.
	offset := readCellOffset(page, cellN-1)
	return offset + len(readLeafCellBytesAtOffset(page, offset))
}

// leafCellsPageSize returns the total page size required to hold cells.
func leafCellsPageSize(cells []leafCell) int {
	sz := dataOffset(len(cells))
	for i := range cells {
		sz += align8(cells[i].Size())
	}
	return sz
}

func writeLeafCell(page []byte, i, offset int, cell leafCell) {
	writeCellOffset(page, i, offset)
	*(*uint64)(unsafe.Pointer(&page[offset])) = cell.Key
	*(*uint32)(unsafe.Pointer(&page[offset+8])) = uint32(cell.Type)
	*(*uint16)(unsafe.Pointer(&page[offset+12])) = uint16(cell.ElemN)
	*(*uint32)(unsafe.Pointer(&page[offset+14])) = uint32(cell.BitN)
	assert(offset+leafCellHeaderSize+len(cell.Data) <= PageSize) // leaf cell write extends beyond page
	copy(page[offset+leafCellHeaderSize:], cell.Data)
}

// branchCell represents a branch cell.
type branchCell struct {
	LeftKey   uint64 // smallest key on ChildPgno
	Flags     uint32
	ChildPgno uint32
}

// branchCellsPageSize returns the total page size required to hold cells.
func branchCellsPageSize(cells []branchCell) int {
	sz := dataOffset(len(cells))
	for range cells {
		sz += align8(branchCellSize)
	}
	return sz
}

func readBranchCellKey(page []byte, i int) uint64 {
	offset := readCellOffset(page, i)
	return *(*uint64)(unsafe.Pointer(&page[offset]))
}

func readBranchCell(page []byte, i int) branchCell {
	assert(i >= 0)              // branch cell index must be zero or greater
	assert(i < readCellN(page)) // branch cell index must less than cell count

	offset := readCellOffset(page, i)
	var cell branchCell
	cell.LeftKey = *(*uint64)(unsafe.Pointer(&page[offset]))
	cell.Flags = *(*uint32)(unsafe.Pointer(&page[offset+8]))
	cell.ChildPgno = *(*uint32)(unsafe.Pointer(&page[offset+12]))
	return cell
}

func readBranchCells(page []byte) []branchCell {
	n := readCellN(page)
	cells := make([]branchCell, n, n+1)
	for i := 0; i < n; i++ {
		cells[i] = readBranchCell(page, i)
	}
	return cells
}

func writeBranchCell(page []byte, i, offset int, cell branchCell) {
	writeCellOffset(page, i, offset)
	*(*uint64)(unsafe.Pointer(&page[offset+0])) = cell.LeftKey
	*(*uint32)(unsafe.Pointer(&page[offset+8])) = uint32(cell.Flags)
	*(*uint32)(unsafe.Pointer(&page[offset+12])) = uint32(cell.ChildPgno)
}

func highbits(v uint64) uint64 { return v >> 16 }
func lowbits(v uint64) uint16  { return uint16(v & 0xFFFF) }

// search implements a binary search similar to sort.Search(), however,
// it returns the position as well as whether an exact match was made.
//
// The return value from f should be -1 for less than, 0 for equal, and 1 for
// greater than.
func search(n int, f func(int) int) (index int, exact bool) {
	i, j := 0, n
	for i < j {
		h := int(uint(i+j) >> 1)
		if cmp := f(h); cmp == 0 {
			return h, true
		} else if cmp > 0 {
			i = h + 1
		} else {
			j = h
		}
	}
	return i, false
}

func Pagedump(b []byte, indent string, writer io.Writer) {
	if writer == nil {
		writer = os.Stderr
	}

	pgno := readPageNo(b)
	if pgno == Magic32() {
		fmt.Fprintf(writer, "==META\n")
		return
	}

	flags := readFlags(b)
	cellN := readCellN(b)

	// NOTE(BBJ): There's no way to tell if a page is a bitmap container with
	// the page alone so this will output !PAGE for bitmap pages & invalid pages.
	switch {
	case flags&PageTypeLeaf != 0:
		fmt.Fprintf(writer, "==LEAF pgno=%d flags=%d n=%d\n", pgno, flags, cellN)
		for i := 0; i < cellN; i++ {
			cell := readLeafCell(b, i)
			switch cell.Type {
			case ContainerTypeArray:
				//fmt.Fprintf(os.Stderr, "[%d]: key=%d type=array n=%d elems=%v\n", i, cell.Key, cell.N, toArray16(cell.Data))
				fmt.Fprintf(writer, "%s[%d]: key=%d type=array BitN=%d \n", indent, i, cell.Key, cell.BitN)
			case ContainerTypeRLE:
				fmt.Fprintf(writer, "%s[%d]: key=%d type=rle BitN=%d\n", indent, i, cell.Key, cell.BitN)
			case ContainerTypeBitmapPtr:
				fmt.Fprintf(writer, "%s[%d]: key=%d type=bitmap BitN=%d\n", indent, i, cell.Key, cell.BitN)
			default:
				fmt.Fprintf(writer, "%s[%d]: key=%d type=unknown<%d> BitN=%d\n", indent, i, cell.Key, cell.Type, cell.BitN)
			}
		}
	case flags&PageTypeBranch != 0:
		fmt.Fprintf(writer, "==BRANCH pgno=%d flags=%d n=%d\n", pgno, flags, cellN)
		for i := 0; i < cellN; i++ {
			cell := readBranchCell(b, i)
			fmt.Fprintf(writer, "[%d]: key=%d flags=%d pgno=%d\n", i, cell.LeftKey, cell.Flags, cell.ChildPgno)
		}
	default:
		fmt.Fprintf(writer, "==!PAGE %d flags=%d\n", pgno, flags)
	}
}

func Walk(tx *Tx, pgno uint32, v func(uint32, []*RootRecord)) {
	for pgno := readMetaRootRecordPageNo(tx.meta[:]); pgno != 0; {
		page, _, err := tx.readPage(pgno)
		if err != nil {
			vprint.PanicOn(err)
		}

		// Read all records on the page.
		a, err := readRootRecords(page)
		if err != nil {
			vprint.PanicOn(err)
		}
		v(pgno, a)
		// Read next overflow page number.
		pgno = WalkRootRecordPages(page)
	}
}

func assert(condition bool) {
	if !condition {
		vprint.PanicOn(fmt.Errorf("assertion failed"))
	}
}

// RowValues returns a list of integer values from a row bitmap.
func RowValues(b []uint64) []uint64 {
	a := make([]uint64, 0)
	for i, v := range b {
		for j := uint(0); j < 64; j++ {
			if v&(1<<j) != 0 {
				a = append(a, (uint64(i)*64)+uint64(j))
			}
		}
	}
	return a
}

// func caller(skip int) string {
// 	_, file, line, _ := runtime.Caller(skip + 1)
// 	return fmt.Sprintf("%s:%d", file, line)
// }

func (db *DB) fsync(f *os.File) error {
	if !db.cfg.FsyncEnabled {
		return nil
	}
	return f.Sync()
}

func (db *DB) fsyncWAL(f *os.File) error {
	if !db.cfg.FsyncEnabled || !db.cfg.FsyncWALEnabled {
		return nil // no sync if either fsync flag is disabled
	}
	return f.Sync()
}

// uint32Hasher implements Hasher for uint32 keys.
type uint32Hasher struct{}

// Hash returns a hash for key.
func (h *uint32Hasher) Hash(key uint32) uint32 {
	return hashUint64(uint64(key))
}

// hashUint64 returns a 32-bit hash for a 64-bit value.
func hashUint64(value uint64) uint32 {
	hash := value
	for value > 0xffffffff {
		value /= 0xffffffff
		hash ^= value
	}
	return uint32(hash)
}

// Metric is a simple, internal metric for check duration of operations.
type Metric struct {
	name     string
	interval int // reporting interval

	mu sync.Mutex
	d  time.Duration // total duration
	n  int           // total count
}

func NewMetric(name string, interval int) Metric {
	assert(interval > 0)
	return Metric{name: name, interval: interval}
}

func (m *Metric) Inc(d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.d += d
	m.n++

	if m.n != 0 && m.n%m.interval == 0 {
		fmt.Printf("metric:%10s avg=%dns\n", m.name, int(m.d)/m.n)
	}
}

// ErrorList represents a list of errors.
type ErrorList []error

// Err returns the list if it contains errors. Otherwise returns nil.
func (a ErrorList) Err() error {
	if len(a) > 0 {
		return a
	}
	return nil
}

func (a ErrorList) Error() string {
	switch len(a) {
	case 0:
		return "no errors"
	case 1:
		return a[0].Error()
	}
	return fmt.Sprintf("%s (and %d more errors)", a[0], len(a)-1)
}

func (a ErrorList) FullError() string {
	if len(a) == 0 {
		return ""
	}

	var buf bytes.Buffer
	for _, err := range a {
		fmt.Fprintln(&buf, err)
	}
	return buf.String()
}

// Append appends an error to the list. If err is an ErrorList then all errors are appended.
func (a *ErrorList) Append(err error) {
	switch err := err.(type) {
	case ErrorList:
		*a = append(*a, err...)
	default:
		*a = append(*a, err)
	}
}
