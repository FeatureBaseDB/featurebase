// Package rbf implements the roaring b-tree file format.
package rbf

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"unsafe"

	"github.com/pilosa/pilosa/v2/shardwidth"
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
	ArrayMaxSize = 4080

	// RLEMaxSize represents the maximum size of run length encoded containers.
	RLEMaxSize = 2040
)

// Page types.
const (
	PageTypeRootRecord   = 1
	PageTypeLeaf         = 2
	PageTypeBranch       = 4
	PageTypeBitmapHeader = 8 // Only used by the WAL for marking next page
)

// Meta commit/rollback flags.
const (
	MetaPageFlagCommit   = 1
	MetaPageFlagRollback = 2
)

// Container types.
const (
	ContainerTypeNone = iota
	ContainerTypeArray
	ContainerTypeRLE
	ContainerTypeBitmap
)

const (
	rootRecordPageHeaderSize = 12
	rootRecordHeaderSize     = 4 + 2     // pgno, len(name)
	leafCellHeaderSize       = 8 + 4 + 4 // key, type, count
	branchCellSize           = 8 + 4 + 4 // key, flags, pgno
)

var (
	ErrTxClosed           = errors.New("transaction closed")
	ErrTxNotWritable      = errors.New("transaction not writable")
	ErrBitmapNameRequired = errors.New("bitmap name required")
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

func writeRootRecords(page []byte, records []*RootRecord) (remaining []*RootRecord, err error) {
	data := page[rootRecordPageHeaderSize:]
	for i, rec := range records {
		if data, err = WriteRootRecord(data, rec); err == io.ErrShortBuffer {
			return records[i:], nil
		} else if err != nil {
			return records[i:], err
		}
	}
	return nil, nil
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
	Type int
	N    int
	Data []byte
}
type leafArgs leafCell

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
	case ContainerTypeBitmap:
		_, bm, _ := tx.leafCellBitmap(toPgno(c.Data))
		return bm
	default:
		panic(fmt.Sprintf("invalid container type: %d", c.Type))
	}
}

// Values returns a slice of 16-bit values from a container.
func (c *leafCell) Values(tx *Tx) []uint16 {
	switch c.Type {
	case ContainerTypeArray:
		return toArray16(c.Data)
	case ContainerTypeRLE:
		//a := make([]uint16, c.N)
		a := make([]uint16, ArrayMaxSize)
		n := int32(0)
		for _, r := range toInterval16(c.Data) {
			for v := int(r.Start); v <= int(r.Last); v++ {
				a[n] = uint16(v)
				n++
			}
		}
		a = a[:n]
		return a
	case ContainerTypeBitmap:
		a := make([]uint16, 0, BitmapN*64)
		_, bm, _ := tx.leafCellBitmap(toPgno(c.Data))
		for i, v := range bm {
			for j := uint(0); j < 64; j++ {
				if v&(1<<j) != 0 {
					a = append(a, (uint16(i)*64)+uint16(j))
				}
			}
		}
		return a
	case ContainerTypeNone:
		return []uint16{}
	default:
		panic(fmt.Sprintf("invalid container type: %d", c.Type))
	}
}

// firstValue the first value from the container.
func (c *leafCell) firstValue() uint16 {
	switch c.Type {
	case ContainerTypeArray:
		a := toArray16(c.Data)
		return a[0]
	case ContainerTypeRLE:
		r := toInterval16(c.Data)
		return r[0].Start
	case ContainerTypeBitmap:
		for i, v := range toArray64(c.Data) {
			for j := uint(0); j < 64; j++ {
				if v&(1<<j) != 0 {
					return (uint16(i) * 64) + uint16(j)
				}
			}
		}
		panic(fmt.Sprintf("rbf.leafCell.firstValue(): no values set in bitmap container: key=%d", c.Key))
	default:
		panic(fmt.Sprintf("invalid container type: %d", c.Type))
	}
}

func readLeafCellKey(page []byte, i int) uint64 {
	offset := readCellOffset(page, i)
	return *(*uint64)(unsafe.Pointer(&page[offset]))
}

func readLeafCell(page []byte, i int) leafCell {
	offset := readCellOffset(page, i)
	buf := page[offset:]

	var cell leafCell
	cell.Key = *(*uint64)(unsafe.Pointer(&buf[0]))
	cell.Type = int(*(*uint32)(unsafe.Pointer(&buf[8])))
	cell.N = int(*(*uint32)(unsafe.Pointer(&buf[12])))

	switch cell.Type {
	case ContainerTypeArray:
		cell.Data = buf[16 : 16+(cell.N*2)]
	case ContainerTypeRLE:
		cell.Data = buf[16 : 16+(cell.N*4)]
	case ContainerTypeBitmap:
		cell.Data = buf[16 : 16+4]
	default:
	}

	return cell
}

func readLeafCells(page []byte, buf []leafCell) []leafCell {
	n := readCellN(page)
	cells := buf[:n]
	for i := 0; i < n; i++ {
		cells[i] = readLeafCell(page, i)
	}
	return cells
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
	*(*uint32)(unsafe.Pointer(&page[offset+12])) = uint32(cell.N)
	assert(offset+16+len(cell.Data) <= PageSize)
	copy(page[offset+16:], cell.Data)
}

// branchCell represents a branch cell.
type branchCell struct {
	Key   uint64
	Flags uint32
	Pgno  uint32
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
	assert(i >= 0)

	offset := readCellOffset(page, i)
	var cell branchCell
	cell.Key = *(*uint64)(unsafe.Pointer(&page[offset]))
	cell.Flags = *(*uint32)(unsafe.Pointer(&page[offset+8]))
	cell.Pgno = *(*uint32)(unsafe.Pointer(&page[offset+12]))
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
	*(*uint64)(unsafe.Pointer(&page[offset+0])) = cell.Key
	*(*uint32)(unsafe.Pointer(&page[offset+8])) = uint32(cell.Flags)
	*(*uint32)(unsafe.Pointer(&page[offset+12])) = uint32(cell.Pgno)
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

/* lint
func itohex(v int) string { return fmt.Sprintf("0x%x", v) }

func hexdump(b []byte) { println(hex.Dump(b)) }
*/

func pagedumpi(b []byte, indent string, writer io.Writer) {
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
		for i := 0; i < cellN; i++ {
			cell := readLeafCell(b, i)
			switch cell.Type {
			case ContainerTypeArray:
				//fmt.Fprintf(os.Stderr, "[%d]: key=%d type=array n=%d elems=%v\n", i, cell.Key, cell.N, toArray16(cell.Data))
				fmt.Fprintf(writer, "%s[%d]: key=%d type=array n=%d \n", indent, i, cell.Key, cell.N)
			case ContainerTypeRLE:
				fmt.Fprintf(writer, "%s[%d]: key=%d type=rle n=%d\n", indent, i, cell.Key, cell.N)
			case ContainerTypeBitmap:
				fmt.Fprintf(writer, "%s[%d]: key=%d type=bitmap n=%d\n", indent, i, cell.Key, cell.N)
			default:
				fmt.Fprintf(writer, "%s[%d]: key=%d type=unknown<%d> n=%d\n", indent, i, cell.Key, cell.Type, cell.N)
			}
		}
	case flags&PageTypeBranch != 0:
		fmt.Fprintf(writer, "==BRANCH pgno=%d flags=%d n=%d\n", pgno, flags, cellN)
		for i := 0; i < cellN; i++ {
			cell := readBranchCell(b, i)
			fmt.Fprintf(writer, "[%d]: key=%d flags=%d pgno=%d\n", i, cell.Key, cell.Flags, cell.Pgno)
		}
	default:
		fmt.Fprintf(writer, "==!PAGE %d flags=%d\n", pgno, flags)
	}
}

func Walk(tx *Tx, pgno uint32, v func(uint32, []*RootRecord)) {
	for pgno := readMetaRootRecordPageNo(tx.meta[:]); pgno != 0; {
		page, err := tx.readPage(pgno)
		if err != nil {
			panic(err)
		}

		// Read all records on the page.
		a, err := readRootRecords(page)
		if err != nil {
			panic(err)
		}
		v(pgno, a)
		// Read next overflow page number.
		pgno = WalkRootRecordPages(page)
	}
}

// treedump recursively writes the tree representation starting from a given page to STDERR.
func treedump(tx *Tx, pgno uint32, indent string, writer io.Writer) {
	page, err := tx.readPage(pgno)
	if err != nil {
		panic(err)
	}

	if IsMetaPage(page) {
		fmt.Fprintf(writer, "META(%d)\n", pgno)
		fmt.Fprintf(writer, "└── <FREELIST>\n")
		//treedump(tx, readMetaFreelistPageNo(page), indent+"    ")

		visitor := func(pgno uint32, records []*RootRecord) {
			fmt.Fprintf(writer, "└── ROOT RECORD(%d): n=%d\n", pgno, len(records))
			for _, record := range records {
				fmt.Fprintf(writer, "└── ROOT(%q) %d\n", record.Name, record.Pgno)
				treedump(tx, record.Pgno, indent+"    ", writer)

			}
		}
		rrdump(tx, readMetaRootRecordPageNo(page), visitor)

		return
	}

	// Handle
	switch typ := readFlags(page); typ {
	case PageTypeBranch:
		fmt.Fprintf(writer, "%s BRANCH(%d) n=%d\n", fmtindent(indent), pgno, readCellN(page))

		for i, n := 0, readCellN(page); i < n; i++ {
			cell := readBranchCell(page, i)
			if cell.Flags&ContainerTypeBitmap == 0 { // leaf/branch child page
				treedump(tx, cell.Pgno, "    "+indent, writer)
			} else {
				fmt.Fprintf(writer, "%s BITMAP(%d)\n", fmtindent("    "+indent), cell.Pgno)
			}
		}
	case PageTypeLeaf:
		fmt.Fprintf(writer, "%s LEAF(%d) n=%d\n", fmtindent(indent), pgno, readCellN(page))
		pagedumpi(page, fmtindent("    "+indent), writer)
	default:
		panic(err)
	}
}

func rrdump(tx *Tx, pgno uint32, v func(uint32, []*RootRecord)) {
	for pgno := readMetaRootRecordPageNo(tx.meta[:]); pgno != 0; {
		page, err := tx.readPage(pgno)
		if err != nil {
			panic(err)
		}

		// Read all records on the page.
		a, err := readRootRecords(page)
		if err != nil {
			panic(err)
		}
		v(pgno, a)
		// Read next overflow page number.
		pgno = WalkRootRecordPages(page)
	}
}

func fmtindent(s string) string {
	if s == "" {
		return ""
	}
	return s + "└──"
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

func onpanic(fn func()) {
	if r := recover(); r != nil {
		fn()
	}
}

func assert(condition bool) {
	if !condition {
		panic("assertion failed")
	}
}
