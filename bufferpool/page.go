package bufferpool

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
)

const PAGE_SIZE int64 = 8192

const INVALID_PAGE int64 = -1

const PAGE_TYPE_BTREE_OVERFLOW = 8
const PAGE_TYPE_BTREE_HEADER = 9
const PAGE_TYPE_BTREE_INTERNAL = 10
const PAGE_TYPE_BTREE_LEAF = 11
const PAGE_TYPE_HASH_TABLE = 12

// SLOTTED PAGE
// page size 8192 bytes
// byte aligned, big endian

// |====================================================|
// | offset | length        |                           |
// |----------------------------------------------------|
// | header                                             |
// |====================================================|
// | 0      | 8             |  pageNumber (int64)       |
// | 8      | 2             |  pageType (int16)         |
// | 10     | 2             |  slotCount (int16)        |
// | 12     | 2             |  localDepth (int16)       | // used only for hash tables
// | 14     | 2             |  freeSpaceOffset (int16)  |
// | 16     | 8             |  prevPointer (int64)      |
// | 24     | 8             |  nextPointer (int64)      |
// | 32	    | 4             |  checksum (int32)			|
// | 36     | 16            |  pageLSN (int64)			|
// |====================================================|
// | <start of slot array 1..slotCount>                 |
// |----------------------------------------------------|
// | 52     | slotcount     |  slot entry is int16      |
// |        |  * slotwidth  |   values (payloadOffset,  |
// |        |  * #slots     |   payloadLength)          |
// |----------------------------------------------------|
// | <free space>                                       |
// |----------------------------------------------------|
// | <payload starting at freeSpaceOffset>              |
// | see below...                                       |
// |====================================================|

// == internal payload chunk ==
// keyLength (int16)
// keyBytes
// ptrValue (int64) (page number)

// == leaf payload chunk ==
// keyLength (int16)
// keyBytes
// flags int8
// overflowPtr (int64) [optional]
// rowPayloadTotalLen (int32) [optional]
// rowPayloadChunkLen (int16)
// rowPayloadChunkBytes

// == row payload bytes ==
// writeTID (int64)
// schemaVersion (int16)
// redoPtr (int64)
// fieldOffsets (one for each field, int16, FF is null)
//   offsets point to:
// *fieldData (one for each field)
//   	valueLen (int32) only used for variable length types
//		valueBytes

const PAGE_NUMBER_OFFSET = 0        // offset 0, length 8, end 8
const PAGE_TYPE_OFFSET = 8          // offset 8, length 2, end 10
const PAGE_SLOT_COUNT_OFFSET = 10   // offset 10, length 2, end 12
const PAGE_LOCAL_DEPTH_OFFSET = 12  // offset 12, length 2, end 14
const PAGE_FREE_SPACE_OFFSET = 14   // offset 14, length 2, end 16
const PAGE_PREV_POINTER_OFFSET = 16 // offset 16, length 8, end 24
const PAGE_NEXT_POINTER_OFFSET = 24 // offset 24, length 8, end 32
const PAGE_CHECKSUM = 32            // offset 32, length 4, end 36
const PAGE_LSN = 36                 // offset 36, length 16, end 52
const PAGE_SLOTS_START_OFFSET = 52  // offset 52

// PAGE_SLOT_LENGTH is the size of the page slot offset value.
const PAGE_SLOT_LENGTH = 2

// 1k is the max key size for now
// we can make this bigger later with overflow
const MAX_KEY_ON_PAGE_SIZE = 1024

// 768 bytes is the max payload on a page before we overflow
// this gives us 1792 bytes for key and payload
// available space on a page after header is 8144 ish
// this gives us room for 4 key/value @ 2036 bytes per page, so
// there is a little bit of wiggle room
const MAX_PAYLOAD_CHUNK_SIZE_ON_PAGE = 768

type PageLatchState int

const (
	None PageLatchState = iota
	Read
	Write
)

// Page represents various types of data page on disk and in memory
type Page struct {
	mu         sync.RWMutex
	latchState PageLatchState
	id         PageID
	pinCount   int
	isDirty    bool
	data       [PAGE_SIZE]byte
}

func NewPage(pageID PageID, pinCount int) *Page {
	return &Page{
		id:         pageID,
		latchState: None,
		pinCount:   pinCount,
		isDirty:    false,
		data:       [PAGE_SIZE]byte{},
	}
}

func (p *Page) TakeReadLatch() {
	p.mu.RLock()
	p.latchState = Read
}

func (p *Page) ReleaseReadLatch() {
	p.mu.RUnlock()
	p.latchState = None
}

func (p *Page) TakeWriteLatch() {
	p.mu.Lock()
	p.latchState = Write
}

func (p *Page) ReleaseWriteLatch() {
	p.mu.Unlock()
	p.latchState = None
}

func (p *Page) ReleaseAnyLatch() {
	if p.latchState == Read {
		p.ReleaseReadLatch()
	} else if p.latchState == Write {
		p.ReleaseWriteLatch()
	}
}

func (p *Page) LatchState() PageLatchState {
	return p.latchState
}

// routines to read and write from page header information
func (p *Page) WritePageNumber(pageNumber int64) {
	p.id.Page = pageNumber
	binary.BigEndian.PutUint64(p.data[PAGE_NUMBER_OFFSET:], uint64(pageNumber))
	p.isDirty = true
}

func (p *Page) ReadPageNumber() int64 {
	return int64(binary.BigEndian.Uint32(p.data[PAGE_NUMBER_OFFSET:]))
}

func (p *Page) WritePageType(pageType int16) {
	binary.BigEndian.PutUint16(p.data[PAGE_TYPE_OFFSET:], uint16(pageType))
	p.isDirty = true
}

func (p *Page) ReadPageType() int16 {
	return int16(binary.BigEndian.Uint16(p.data[PAGE_TYPE_OFFSET:]))
}

func (p *Page) WriteSlotCount(slotCount int16) {
	binary.BigEndian.PutUint16(p.data[PAGE_SLOT_COUNT_OFFSET:], uint16(slotCount))
	p.isDirty = true
}

func (p *Page) ReadSlotCount() int16 {
	return int16(binary.BigEndian.Uint16(p.data[PAGE_SLOT_COUNT_OFFSET:]))
}

func (p *Page) WriteLocalDepth(localDepth int16) {
	binary.BigEndian.PutUint16(p.data[PAGE_LOCAL_DEPTH_OFFSET:], uint16(localDepth))
	p.isDirty = true
}

func (p *Page) ReadLocalDepth() int16 {
	return int16(binary.BigEndian.Uint16(p.data[PAGE_LOCAL_DEPTH_OFFSET:]))
}

func (p *Page) WriteFreeSpaceOffset(offset int16) {
	binary.BigEndian.PutUint16(p.data[PAGE_FREE_SPACE_OFFSET:], uint16(offset))
	p.isDirty = true
}

func (p *Page) ReadFreeSpaceOffset() int16 {
	return int16(binary.BigEndian.Uint16(p.data[PAGE_FREE_SPACE_OFFSET:]))
}

func (p *Page) WritePrevPointer(prevPointer PageID) {
	binary.BigEndian.PutUint64(p.data[PAGE_PREV_POINTER_OFFSET:], uint64(prevPointer.Page))
	p.isDirty = true
}

func (p *Page) ReadPrevPointer() PageID {
	page := int64(binary.BigEndian.Uint64(p.data[PAGE_PREV_POINTER_OFFSET:]))
	return PageID{ObjectID: p.id.ObjectID, Shard: p.id.Shard, Page: page}
}

func (p *Page) WriteNextPointer(nextPointer PageID) {
	binary.BigEndian.PutUint64(p.data[PAGE_NEXT_POINTER_OFFSET:], uint64(nextPointer.Page))
	p.isDirty = true
}

func (p *Page) ReadNextPointer() PageID {
	page := int64(binary.BigEndian.Uint64(p.data[PAGE_NEXT_POINTER_OFFSET:]))
	return PageID{ObjectID: p.id.ObjectID, Shard: p.id.Shard, Page: page}
}

// read slots from pages
func (p *Page) ReadPageSlot(slot int16) PageSlot {
	offset := PAGE_SLOTS_START_OFFSET + PAGE_SLOT_LENGTH*slot
	keyOffset := int16(binary.BigEndian.Uint16(p.data[offset:]))
	return PageSlot{
		PayloadOffset: keyOffset,
	}
}

func (p *Page) WritePageSlot(slot int16, value PageSlot) {
	offset := PAGE_SLOTS_START_OFFSET + PAGE_SLOT_LENGTH*slot
	binary.BigEndian.PutUint16(p.data[offset:], uint16(value.PayloadOffset))
}

// TODO(pok) deprecate this once we get b+tree working, this
// is just used in hash table right now
func (p *Page) PutKeyValueInPageSlot(slotNumber int16, keyBytes []byte, payloadBytes []byte) error {
	freeSpaceOffset := p.ReadFreeSpaceOffset()

	keyLength := len(keyBytes)
	payloadChunkLength := len(payloadBytes)

	// check for overflow
	if payloadChunkLength > MAX_PAYLOAD_CHUNK_SIZE_ON_PAGE {
		return errors.New("overflow")
	}

	totalPayloadSize := p.ComputeLeafPayloadTotalLength(keyLength, payloadChunkLength)

	// check to make sure we don't blow free space
	if p.FreeSpaceOnPage() < int16(totalPayloadSize) {
		return errors.New("page is full")
	}

	// compute the new free space offset
	freeSpaceOffset -= int16(totalPayloadSize)
	offset := freeSpaceOffset

	// write the header
	offset = p.WriteLeafPagePayloadHeader(offset, int16(keyLength), keyBytes, 0, 0, int32(payloadChunkLength))

	// write the payload
	p.WriteLeafPagePayloadBytes(offset, int16(payloadChunkLength), payloadBytes)

	// update the free space offset
	p.WriteFreeSpaceOffset(int16(freeSpaceOffset))

	// make a slot
	slot := PageSlot{
		PayloadOffset: freeSpaceOffset,
	}
	// write the slot
	p.WritePageSlot(slotNumber, slot)

	return nil
}

func (p *Page) ComputeInternalPayloadTotalLength(keyLength int) int32 {
	l := /*keyLength*/ 2 + keyLength + /*ptrValue*/ 8
	return int32(l)
}

func (p *Page) WriteInternalPagePayload(offset int16, keyLen int16, keyBytes []byte, ptrValue int64) {
	binary.BigEndian.PutUint16(p.data[offset:], uint16(keyLen))
	offset += 2
	copy(p.data[offset:], keyBytes)
	offset += keyLen
	binary.BigEndian.PutUint64(p.data[offset:], uint64(ptrValue))
	p.isDirty = true
}

func (p *Page) ComputeLeafPayloadTotalLength(keyLength int, payloadLength int) int32 {
	l := /*keyLength*/ 2 + keyLength + /*flags*/ 1 + /*payLoadLength*/ 2 + payloadLength
	if l > MAX_PAYLOAD_CHUNK_SIZE_ON_PAGE {
		l += 8 + 4 // add overflow ptr and total payload size
	}
	return int32(l)
}

func (p *Page) WriteLeafPagePayloadHeader(offset int16, keyLen int16, keyBytes []byte, flags int8, overflowPtr int64, payloadTotalLen int32) int16 {
	binary.BigEndian.PutUint16(p.data[offset:], uint16(keyLen))
	offset += 2
	copy(p.data[offset:], keyBytes)
	offset += keyLen
	p.data[offset] = byte(flags)
	offset += 1
	// if we are overflowing write the overflow ptr
	if flags == 1 {
		binary.BigEndian.PutUint64(p.data[offset:], uint64(overflowPtr))
		offset += 8
		// total length
		binary.BigEndian.PutUint32(p.data[offset:], uint32(payloadTotalLen))
		offset += 4
	}
	p.isDirty = true
	return offset
}

func (p *Page) WriteLeafPagePayloadBytes(offset int16, payloadChunkLength int16, payloadChunkBytes []byte) {
	// chunk length
	binary.BigEndian.PutUint16(p.data[offset:], uint16(payloadChunkLength))
	offset += 2
	// now copy the payload bytes
	copy(p.data[offset:], payloadChunkBytes)
	p.isDirty = true
}

func (p *Page) ReadLeafPagePayloadBytes(offset int16) (int16, []byte) {
	chunkLen := int16(binary.BigEndian.Uint16(p.data[offset:]))
	offset += 2
	chunkBytes := make([]byte, chunkLen)
	copy(chunkBytes, p.data[offset:offset+chunkLen])
	return chunkLen, chunkBytes
}

func (p *Page) WriteInternalPageChunk(offset int16, chunk InternalPageChunk) {
	binary.BigEndian.PutUint16(p.data[offset:], uint16(chunk.KeyLength))
	offset += 2
	copy(p.data[offset:], chunk.KeyBytes)
	offset += int16(len(chunk.KeyBytes))
	binary.BigEndian.PutUint64(p.data[offset:], uint64(chunk.PtrValue))
	p.isDirty = true
}

func (p *Page) FreeSpaceOnPage() int16 {
	freeSpaceOffset := p.ReadFreeSpaceOffset()
	freespace := freeSpaceOffset - (p.ReadSlotCount()*PAGE_SLOT_LENGTH + PAGE_SLOT_LENGTH + PAGE_SLOTS_START_OFFSET)
	return freespace
}

func (p *Page) CopyPageTo(page *Page) {
	// copy everything but pageNumber & pageType
	offset := int64(PAGE_SLOT_COUNT_OFFSET)
	copy(page.data[offset:], p.data[offset:offset+PAGE_SIZE-offset])
}

func (p *Page) PinCount() int {
	return p.pinCount
}

func (p *Page) ID() PageID {
	return p.id
}

func (p *Page) DecPinCount() {
	if p.pinCount > 0 {
		p.pinCount--
	}
}

func (pg *Page) Dump(label string) {
	indent := 0
	if len(label) > 0 {
		fmt.Printf("%s%s:\n", fmt.Sprintf("%*s", indent, ""), label)
		indent += 4
	}
	pageType := pg.ReadPageType()
	fmt.Printf("%sPAGE(%d) pageType: %d slotCount: %d, freeSpaceOffset: %d, freeSpaceOnPage: %d, prevPtr: %d, nextPtr: %d\n", fmt.Sprintf("%*s", indent, ""), pg.ID(), pageType, pg.ReadSlotCount(), pg.ReadFreeSpaceOffset(), pg.FreeSpaceOnPage(), pg.ReadPrevPointer(), pg.ReadNextPointer())
	fmt.Printf("%sKEYS: -->\n", fmt.Sprintf("%*s", indent, ""))
	indent += 4

	// get the keys off the page
	keys := make([]int, 0)
	pointers := make([]PageID, 0)
	iter := NewPageSlotIterator(pg, 0)
	for {
		ps := iter.Next()
		if ps == nil {
			break
		}
		pl := ps.KeyPayload(pg)
		keys = append(keys, int(pl.KeyAsInt(pg)))
		if pageType == PAGE_TYPE_BTREE_INTERNAL {
			ipl := ps.InternalPayload(pg)
			pointers = append(pointers, ipl.ValueAsPagePointer(pg))
		}
	}

	if pageType == PAGE_TYPE_BTREE_LEAF {
		for _, key := range keys {
			fmt.Printf("%s(%d)\n", fmt.Sprintf("%*s", indent, ""), key)
		}
	} else {
		for idx, key := range keys {
			ptr := pointers[idx]
			fmt.Printf("%s(%d, %d)\n", fmt.Sprintf("%*s", indent, ""), key, ptr)
		}
		ptr := pg.ReadNextPointer()
		fmt.Printf("%s(-->, %d)\n", fmt.Sprintf("%*s", indent, ""), ptr)
	}

}

type KeyPayload struct {
	BaseOffset int16
}

func (p *KeyPayload) KeyAsInt(page *Page) int32 {
	return int32(binary.BigEndian.Uint32(page.data[p.BaseOffset+2:]))
}

func (p *KeyPayload) KeyBytes(page *Page) []byte {
	offset := p.BaseOffset
	keyLen := int16(binary.BigEndian.Uint16(page.data[offset:]))
	offset += 2
	result := make([]byte, keyLen)
	copy(result, page.data[offset:offset+keyLen])
	return result
}

type InternalPayload struct {
	BaseOffset int16
}

func (p *InternalPayload) ptrOffset(page *Page) int16 {
	offset := p.BaseOffset
	keyLen := int16(binary.BigEndian.Uint16(page.data[offset:]))
	offset += 2 + keyLen
	return offset
}

func (p *InternalPayload) ValueAsPagePointer(page *Page) PageID {
	offset := p.ptrOffset(page)
	pageid := int64(binary.BigEndian.Uint64(page.data[offset:]))
	return PageID{ObjectID: page.id.ObjectID, Shard: page.id.Shard, Page: pageid}
}

func (p *InternalPayload) PutPagePointer(page *Page, pagePtr PageID) error {
	offset := p.ptrOffset(page)
	binary.BigEndian.PutUint64(page.data[offset:], uint64(pagePtr.Page))
	page.isDirty = true
	return nil
}

func (l *InternalPayload) InternalPageChunk(page *Page) InternalPageChunk {
	offset := l.BaseOffset
	keyLen := int16(binary.BigEndian.Uint16(page.data[offset:]))
	offset += 2
	keyBytes := make([]byte, keyLen)
	copy(keyBytes, page.data[offset:offset+keyLen])
	offset += keyLen
	ptrValue := int64(binary.BigEndian.Uint64(page.data[offset:]))
	return InternalPageChunk{
		KeyLength: keyLen,
		KeyBytes:  keyBytes,
		PtrValue:  ptrValue,
	}
}

type LeafPayload struct {
	BaseOffset int16
}

func (l *LeafPayload) valueOffset(page *Page) int16 {
	offset := l.BaseOffset
	keyLen := int16(binary.BigEndian.Uint16(page.data[offset:]))
	offset += 2 + keyLen
	return offset
}

func (l *LeafPayload) ValueLength(page *Page) int32 {
	offset := l.valueOffset(page)
	valueLen := int32(binary.BigEndian.Uint32(page.data[offset:]))
	return valueLen
}

// this wil fail in overflow
func (l *LeafPayload) ValueAsBytes(page *Page) []byte {
	offset := l.valueOffset(page)
	valueLen := int32(binary.BigEndian.Uint32(page.data[offset:]))
	offset += 4
	result := make([]byte, valueLen)
	copy(result, page.data[offset:int32(offset)+valueLen])
	return result
}

func (l *LeafPayload) GetPayloadReader(page *Page) LeafPagePayLoadReader {
	offset := l.BaseOffset
	keyLen := int16(binary.BigEndian.Uint16(page.data[offset:]))
	offset += 2
	keyBytes := make([]byte, keyLen)
	copy(keyBytes, page.data[offset:offset+keyLen])
	offset += keyLen
	flags := int8(page.data[offset])
	offset += 1
	overflowPtr := int64(0)
	payloadTotalLength := int32(0)
	if flags == 1 {
		overflowPtr = int64(binary.BigEndian.Uint64(page.data[offset:]))
		offset += 8
		payloadTotalLength = int32(binary.BigEndian.Uint32(page.data[offset:]))
		offset += 4
	}
	valueLen := int16(binary.BigEndian.Uint16(page.data[offset:]))
	offset += 2
	if payloadTotalLength == 0 {
		payloadTotalLength = int32(valueLen)
	}
	valueBytes := make([]byte, valueLen)
	copy(valueBytes, page.data[offset:int16(offset)+valueLen])
	return LeafPagePayLoadReader{
		KeyLength:          keyLen,
		KeyBytes:           keyBytes,
		Flags:              flags,
		OverflowPtr:        overflowPtr,
		PayloadTotalLength: payloadTotalLength,
		PayloadChunkLength: valueLen,
		PayloadChunkBytes:  valueBytes,
	}
}

type PageSlot struct {
	PayloadOffset int16
}

func (s *PageSlot) KeyPayload(page *Page) KeyPayload {
	return KeyPayload{BaseOffset: s.PayloadOffset}
}

func (s *PageSlot) InternalPayload(page *Page) InternalPayload {
	return InternalPayload{BaseOffset: s.PayloadOffset}
}

func (s *PageSlot) LeafPayload(page *Page) LeafPayload {
	return LeafPayload{BaseOffset: s.PayloadOffset}
}

type LeafPagePayLoadReader struct {
	KeyLength          int16
	KeyBytes           []byte
	Flags              int8
	OverflowPtr        int64
	PayloadTotalLength int32
	PayloadChunkLength int16
	PayloadChunkBytes  []byte
}

func (pc *LeafPagePayLoadReader) Length() int32 {
	l := /*keyLength*/ 2 + pc.KeyLength + /*flags*/ 1 + /*payLoadLength*/ 2 + pc.PayloadChunkLength
	if pc.Flags == 1 {
		l += 8 + 4 // add overflow ptr and total payload size
	}
	return int32(l)
}

type InternalPageChunk struct {
	KeyLength int16
	KeyBytes  []byte
	PtrValue  int64
}

func (pc *InternalPageChunk) Length() int {
	return 2 + len(pc.KeyBytes) + 8
}

type PageSlotIterator struct {
	page      *Page
	slotCount int16
	cursor    int16
}

func NewPageSlotIterator(page *Page, fromSlot int16) *PageSlotIterator {
	i := &PageSlotIterator{
		page:      page,
		slotCount: page.ReadSlotCount(),
		cursor:    fromSlot,
	}
	return i
}

func (i *PageSlotIterator) Next() *PageSlot {
	if i.cursor < i.slotCount {
		s := i.page.ReadPageSlot(i.cursor)
		i.cursor++
		return &s
	}
	return nil
}

func (i *PageSlotIterator) Cursor() int16 {
	return i.cursor
}
