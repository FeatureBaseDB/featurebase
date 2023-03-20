package bufferpool

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const PAGE_SIZE int = 8192

const INVALID_PAGE int = -1

const PAGE_TYPE_BTREE_INTERNAL = 10
const PAGE_TYPE_BTREE_LEAF = 11
const PAGE_TYPE_HASH_TABLE = 12

// PAGE
// page size 8192 bytes
// byte aligned, big endian

// |====================================================|
// | offset | length        |                           |
// |----------------------------------------------------|
// | header                                             |
// |====================================================|
// | 0      | 4             |  pageNumber (int32)       |
// | 4      | 2             |  pageType (int16)         |
// | 6      | 2             |  slotCount (int16)        |
// | 8      | 2             |  localDepth (int16)       |
// | 10     | 2             |  freeSpaceOffset (int16)  |
// | 12     | 4             |  prevPointer (int32)      |
// | 16     | 4             |  nextPointer (int32)      |
// |====================================================|
// | <start of slot array 1..slotCount>                 |
// |----------------------------------------------------|
// | 20     | slotcount     |  slot entry is 2 int16    |
// |        |  * slotwidth  |   values (payloadOffset,  |
// |        |  * #slots     |   payloadLength)          |
// |----------------------------------------------------|
// | <free space>                                       |
// |----------------------------------------------------|
// | <payload starting at freeSpaceOffset>              |
// |  payload entries are keylength (int16), key bytes, |
// |  payload length (int32), payload bytes             |
// |====================================================|

const PAGE_NUMBER_OFFSET = 0        // offset 0, length 4, end 4
const PAGE_TYPE_OFFSET = 4          // offset 4, length 2, end 6
const PAGE_SLOT_COUNT_OFFSET = 6    // offset 6, length 2, end 8
const PAGE_LOCAL_DEPTH_OFFSET = 8   // offset 8, length 2, end 10
const PAGE_FREE_SPACE_OFFSET = 10   // offset 10, length 2, end 12
const PAGE_PREV_POINTER_OFFSET = 12 // offset 12, length 4, end 16
const PAGE_NEXT_POINTER_OFFSET = 16 // offset 16, length 4, end 20
const PAGE_SLOTS_START_OFFSET = 20  // offset 20

// PAGE_SLOT_LENGTH is the size of the page slot key/value.
//
// key offset int16    	//offset 0, length 2, end 2
// value offset int16	    //offset 2, length 2, end 4
const PAGE_SLOT_LENGTH = 4

// Page represents a page on disk
type Page struct {
	id       PageID
	pinCount int
	isDirty  bool
	data     [PAGE_SIZE]byte
}

type PageSlot struct {
	KeyOffset   int16
	ValueOffset int16
}

func (s *PageSlot) KeyBytes(page *Page) []byte {
	offset := s.KeyOffset
	keyLen := int16(binary.BigEndian.Uint16(page.data[offset:]))
	offset += 2
	result := make([]byte, keyLen)
	copy(result, page.data[offset:offset+keyLen])
	return result
}

func (s *PageSlot) KeyAsInt(page *Page) int32 {
	return int32(binary.BigEndian.Uint32(page.data[s.KeyOffset+2:]))
}

func (s *PageSlot) ValueBytes(page *Page) []byte {
	offset := s.ValueOffset
	valueLen := int32(binary.BigEndian.Uint32(page.data[offset:]))
	offset += 4
	result := make([]byte, valueLen)
	copy(result, page.data[offset:int32(offset)+valueLen])
	return result
}

func (s *PageSlot) ValueAsPagePointer(page *Page) int32 {
	return int32(binary.BigEndian.Uint32(page.data[s.ValueOffset+4:]))
}

type PageChunk struct {
	KeyLength int16
	KeyBytes  []byte
	// TODO(pok) ValueBytes can be up to int32 long
	// this requires an overflow page mechanism, that is not implemented
	// yet, so be aware of this when storing stuff...
	ValueLength int32
	ValueBytes  []byte
}

func (pc *PageChunk) Length() int {
	return 2 + len(pc.KeyBytes) + 4 + len(pc.ValueBytes)
}

func (pc *PageChunk) ComputeKeyOffset(pageOffset int) int {
	return pageOffset
}

func (pc *PageChunk) ComputeValueOffset(pageOffset int) int {
	return pageOffset + 2 + len(pc.KeyBytes)
}

func (p *Page) WritePageNumber(pageNumber int32) {
	p.id = PageID(pageNumber)
	binary.BigEndian.PutUint32(p.data[PAGE_NUMBER_OFFSET:], uint32(pageNumber))
	p.isDirty = true
}

func (p *Page) ReadPageNumber() int {
	return int(binary.BigEndian.Uint32(p.data[PAGE_NUMBER_OFFSET:]))
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

func (p *Page) ReadSlot(slot int16) PageSlot {
	offset := PAGE_SLOTS_START_OFFSET + PAGE_SLOT_LENGTH*slot
	keyOffset := int16(binary.BigEndian.Uint16(p.data[offset:]))
	offset += 2
	valueOffset := int16(binary.BigEndian.Uint16(p.data[offset:]))
	return PageSlot{
		KeyOffset:   keyOffset,
		ValueOffset: valueOffset,
	}
}

func (p *Page) WriteSlot(slot int16, value PageSlot) {
	offset := PAGE_SLOTS_START_OFFSET + PAGE_SLOT_LENGTH*slot
	binary.BigEndian.PutUint16(p.data[offset:], uint16(value.KeyOffset))
	offset += 2
	binary.BigEndian.PutUint16(p.data[offset:], uint16(value.ValueOffset))
}

func (p *Page) WriteFreeSpaceOffset(offset int16) {
	binary.BigEndian.PutUint16(p.data[PAGE_FREE_SPACE_OFFSET:], uint16(offset))
	p.isDirty = true
}

func (p *Page) ReadFreeSpaceOffset() int16 {
	return int16(binary.BigEndian.Uint16(p.data[PAGE_FREE_SPACE_OFFSET:]))
}

func (p *Page) WritePrevPointer(prevPointer int32) {
	binary.BigEndian.PutUint32(p.data[PAGE_PREV_POINTER_OFFSET:], uint32(prevPointer))
	p.isDirty = true
}

func (p *Page) ReadPrevPointer() int {
	return int(binary.BigEndian.Uint32(p.data[PAGE_PREV_POINTER_OFFSET:]))
}

func (p *Page) WriteNextPointer(nextPointer int32) {
	binary.BigEndian.PutUint32(p.data[PAGE_NEXT_POINTER_OFFSET:], uint32(nextPointer))
	p.isDirty = true
}

func (p *Page) ReadNextPointer() int {
	return int(binary.BigEndian.Uint32(p.data[PAGE_NEXT_POINTER_OFFSET:]))
}

func (p *Page) WriteChunk(offset int16, chunk PageChunk) {
	binary.BigEndian.PutUint16(p.data[offset:], uint16(chunk.KeyLength))
	offset += 2
	copy(p.data[offset:], chunk.KeyBytes)
	offset += int16(len(chunk.KeyBytes))
	binary.BigEndian.PutUint32(p.data[offset:], uint32(chunk.ValueLength))
	offset += 4
	copy(p.data[offset:], chunk.ValueBytes)
	p.isDirty = true
}

func (p *Page) ReadChunk(offset int16) PageChunk {
	keyLen := int16(binary.BigEndian.Uint16(p.data[offset:]))
	offset += 2
	keyBytes := make([]byte, keyLen)
	copy(keyBytes, p.data[offset:offset+keyLen])
	offset += keyLen
	valueLen := int32(binary.BigEndian.Uint32(p.data[offset:]))
	offset += 4
	valueBytes := make([]byte, valueLen)
	copy(valueBytes, p.data[offset:int32(offset)+valueLen])
	return PageChunk{
		KeyLength:   keyLen,
		KeyBytes:    keyBytes,
		ValueLength: valueLen,
		ValueBytes:  valueBytes,
	}
}

func (p *Page) FreeSpace() int16 {
	freeSpaceOffset := p.ReadFreeSpaceOffset()
	freespace := freeSpaceOffset - (p.ReadSlotCount()*PAGE_SLOT_LENGTH + PAGE_SLOT_LENGTH + PAGE_SLOTS_START_OFFSET)
	return freespace
}

func (p *Page) WriteKeyValueInSlot(slotNumber int16, key []byte, value []byte) error {
	freeSpaceOffset := p.ReadFreeSpaceOffset()

	// build a chunk
	chunk := PageChunk{
		KeyLength:   int16(len(key)),
		KeyBytes:    key,
		ValueLength: int32(len(value)),
		ValueBytes:  value,
	}

	// compute the new free space offset
	freeSpaceOffset -= int16(chunk.Length())

	// check we won't blow free space on page
	slotCount := p.ReadSlotCount()
	slotEndOffset := slotCount*PAGE_SLOT_LENGTH + PAGE_SLOT_LENGTH + PAGE_SLOTS_START_OFFSET

	// DEBUG!!
	//fmt.Printf("freeSpaceOffset: %d, slotCount: %d, slotCount*4 + 4 + 20: %d, freeSpace: %d\n", freeSpaceOffset, slotCount, slotEndOffset, freeSpaceOffset-slotEndOffset)

	if freeSpaceOffset-slotEndOffset <= 0 {
		return errors.New("page is full")
	}

	keyOffset := chunk.ComputeKeyOffset(int(freeSpaceOffset))
	valueOffset := chunk.ComputeValueOffset(int(freeSpaceOffset))

	p.WriteChunk(freeSpaceOffset, chunk)

	// update the free space offset
	p.WriteFreeSpaceOffset(int16(freeSpaceOffset))

	// make a slot
	slot := PageSlot{
		KeyOffset:   int16(keyOffset),
		ValueOffset: int16(valueOffset),
	}
	// write the slot
	p.WriteSlot(slotNumber, slot)

	return nil
}

func (p *Page) WritePage(page *Page) {
	// copy everything but pageNumber & pageType
	offset := PAGE_SLOT_COUNT_OFFSET
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
		s := i.page.ReadSlot(i.cursor)
		i.cursor++
		return &s
	}
	return nil
}

func (i *PageSlotIterator) Cursor() int16 {
	return i.cursor
}

func (pg *Page) Dump(label string) {
	indent := 0
	if len(label) > 0 {
		fmt.Printf("%s%s:\n", fmt.Sprintf("%*s", indent, ""), label)
		indent += 4
	}
	pageType := pg.ReadPageType()
	fmt.Printf("%sPAGE(%d) pageType: %d slotCount: %d, prevPtr: %d, nextPtr: %d\n", fmt.Sprintf("%*s", indent, ""), pg.ID(), pageType, pg.ReadSlotCount(), pg.ReadPrevPointer(), pg.ReadNextPointer())
	fmt.Printf("%sKEYS: -->\n", fmt.Sprintf("%*s", indent, ""))
	indent += 4

	// get the keys off the page
	keys := make([]int, 0)
	pointers := make([]int, 0)
	iter := NewPageSlotIterator(pg, 0)
	for {
		ps := iter.Next()
		if ps == nil {
			break
		}
		keys = append(keys, int(ps.KeyAsInt(pg)))
		if pageType == /*nodeTypeInternal*/ 10 {
			pointers = append(pointers, int(ps.ValueAsPagePointer(pg)))
		}
	}

	if pageType == /*nodeTypeLeaf*/ 11 {
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
