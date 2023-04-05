package extendiblehash

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/featurebasedb/featurebase/v3/bufferpool"
)

// ExtendibleHashTable is an extendible hash table implementation backed by a buffer
// pool
type ExtendibleHashTable struct {
	directory   []bufferpool.PageID
	globalDepth uint
	keysPerPage int
	bufferPool  *bufferpool.BufferPool
}

// NewExtendibleHashTable creates a new ExtendibleHashTable
func NewExtendibleHashTable(keyLength int, valueLength int, bufferPool *bufferpool.BufferPool) (*ExtendibleHashTable, error) {
	bytesPerKV := /* keyLength */ 2 + keyLength + /* valueLength */ 2 + valueLength + bufferpool.PAGE_SLOT_LENGTH
	keysPerPage := int(bufferpool.PAGE_SIZE-bufferpool.PAGE_SLOTS_START_OFFSET) / bytesPerKV

	//create the root page
	page, err := bufferPool.NewPage(0, 0)
	if err != nil {
		return nil, err
	}
	page.WritePageType(bufferpool.PAGE_TYPE_HASH_TABLE)
	bufferPool.FlushPage(page.ID())

	return &ExtendibleHashTable{
		globalDepth: 0,
		directory:   make([]bufferpool.PageID, 1),
		keysPerPage: keysPerPage,
		bufferPool:  bufferPool,
	}, nil
}

// Get gets a key from the hash table. It returns the value, a bool set to true if the key is
// found (false if the key is not found) or an error.
func (e *ExtendibleHashTable) Get(key []byte) ([]byte, bool, error) {
	pageId, err := e.getPageID(key)
	if err != nil {
		return []byte{}, false, err
	}

	page, err := e.bufferPool.FetchPage(pageId)
	if err != nil {
		return []byte{}, false, err
	}
	defer e.bufferPool.UnpinPage(page.ID())

	index, found := e.findKey(page, key)
	if found {
		slot := page.ReadPageSlot(int16(index))
		lpl := slot.HashPayload(page)
		rdr := lpl.GetPayloadReader(page)
		return rdr.PayloadChunkBytes, true, nil
	}
	return []byte{}, false, nil
}

// Put puts a key/value pair into the hash table. It returns an error if one occurs.
func (e *ExtendibleHashTable) Put(key, value []byte) error {
	pageID, err := e.getPageID(key)
	if err != nil {
		return err
	}
	page, err := e.bufferPool.FetchPage(pageID)
	if err != nil {
		return err
	}
	defer e.bufferPool.UnpinPage(page.ID())

	full := int(page.ReadSlotCount()) >= e.keysPerPage
	err = e.putKeyValue(page, key, value)
	if err != nil {
		return err
	}

	if full {
		err = e.splitOnKey(page, key)
		if err != nil {
			return err
		}
	}
	return nil
}

// Close cleans up the hash table after its use.
func (e *ExtendibleHashTable) Close() {
	e.bufferPool.Close()
}

func (e *ExtendibleHashTable) hashFunction(k Hashable) int {
	hashResult := k.Hash() & ((1 << e.globalDepth) - 1)
	return int(hashResult)
}

func (e *ExtendibleHashTable) getPageID(key []byte) (bufferpool.PageID, error) {
	hash := e.hashFunction(Key(key))
	if hash > len(e.directory)-1 {
		return bufferpool.PageID{ObjectID: 0, Shard: 0, Page: bufferpool.INVALID_PAGE}, fmt.Errorf("hash (%d) out of the directory array bounds (%d)", hash, len(e.directory))
	}
	id := e.directory[hash]
	return bufferpool.PageID(id), nil
}

func (e *ExtendibleHashTable) findKey(page *bufferpool.Page, key []byte) (int, bool) {
	minIndex := 0
	onePastMaxIndex := int(page.ReadSlotCount())

	for onePastMaxIndex != minIndex {
		index := (minIndex + onePastMaxIndex) / 2
		s := page.ReadPageSlot(int16(index))
		pl := s.KeyPayload(page)
		keyAtIndex := pl.KeyBytes(page)

		if bytes.Equal(keyAtIndex, key) {
			return index, true
		}
		if bytes.Compare(key, keyAtIndex) < 0 {
			onePastMaxIndex = index
		} else {
			minIndex = index + 1
		}
	}
	return minIndex, false
}

func (e *ExtendibleHashTable) splitOnKey(page *bufferpool.Page, key []byte) error {
	if uint(page.ReadLocalDepth()) == e.globalDepth {

		e.directory = append(e.directory, e.directory...)
		e.globalDepth++
	}

	// scratch page for left
	p0 := e.bufferPool.ScratchPage()
	p0.WritePageNumber(page.ID().Page)
	p0.WritePageType(bufferpool.PAGE_TYPE_HASH_TABLE)

	// allocate new page for split
	p1, err := e.bufferPool.NewPage(0, 0)
	if err != nil {
		return err
	}
	defer e.bufferPool.UnpinPage(p1.ID())
	p1.WritePageType(bufferpool.PAGE_TYPE_HASH_TABLE)

	// update local depths
	newLocalDepth := page.ReadLocalDepth() + 1
	p0.WriteLocalDepth(newLocalDepth)
	p1.WriteLocalDepth(newLocalDepth)

	ld := page.ReadLocalDepth()
	hiBit := uint64(1 << ld)

	it := bufferpool.NewPageSlotIterator(page, 0)
	for {
		slot := it.Next()
		if slot == nil {
			break
		}
		lpl := slot.HashPayload(page)
		rdr := lpl.GetPayloadReader(page)
		keyBytes := rdr.KeyBytes
		k := string(keyBytes)
		h := Key(k).Hash()

		if h&hiBit > 0 {
			sc := p1.ReadSlotCount()
			e.putKeyValueInPageSlot(p1, sc, keyBytes, rdr.PayloadChunkBytes)
			// update the slot count
			p1.WriteSlotCount(int16(sc + 1))

		} else {
			sc := p0.ReadSlotCount()
			e.putKeyValueInPageSlot(p0, sc, keyBytes, rdr.PayloadChunkBytes)
			// update the slot count
			p0.WriteSlotCount(int16(sc + 1))
		}
	}
	for j := Key(key).Hash() & (hiBit - 1); j < uint64(len(e.directory)); j += hiBit {
		if j&hiBit > 0 {
			e.directory[j] = p1.ID()
		} else {
			e.directory[j] = p0.ID()
		}
	}

	// copy p1 back into page
	p0.CopyPageTo(page)

	return nil
}

func (e *ExtendibleHashTable) cleanPage(page *bufferpool.Page) error {
	// 1. make a scratch page
	// 2. iterate the slots on this page and copy data
	// 3. put the scratch page back over this one

	scratchPage := e.bufferPool.ScratchPage()
	scratchPage.WritePageType(bufferpool.PAGE_TYPE_BTREE_LEAF)
	scratchPage.WritePageNumber(page.ID().Page)
	freeSpaceOffset := scratchPage.ReadFreeSpaceOffset()

	slotCount := int(page.ReadSlotCount())

	for i := 0; i < slotCount; i++ {

		// read the slot from the page
		s := page.ReadPageSlot(int16(i))
		// read the chunk from the page
		lbl := s.HashPayload(page)
		c := lbl.GetPayloadReader(page)

		// mod the freespace offset based on the size of the chunk
		freeSpaceOffset -= int16(c.Length())
		// update the values in the slot
		s.PayloadOffset = freeSpaceOffset

		// write the chunk
		scratchPage.WriteHashPagePayloadBytes(freeSpaceOffset, c.KeyLength, c.KeyBytes, c.PayloadChunkLength, c.PayloadChunkBytes)
		//write the slot
		scratchPage.WritePageSlot(int16(i), s)

		// update the free space offset
		scratchPage.WriteFreeSpaceOffset(int16(freeSpaceOffset))
	}
	// set the new slotcounts
	scratchPage.WriteSlotCount(int16(slotCount))

	scratchPage.CopyPageTo(page)

	return nil
}

func (e *ExtendibleHashTable) keyValueWillFit(page *bufferpool.Page, key, value []byte) bool {
	// will this k/v fit on the page?
	payloadLen := page.ComputeHashPayloadTotalLength(len(key), len(value))
	fs := page.FreeSpaceOnPage()
	return fs > int16(payloadLen)
}

func (e *ExtendibleHashTable) putKeyValue(page *bufferpool.Page, key, value []byte) error {
	if !e.keyValueWillFit(page, key, value) {
		// try to garbage collect the page first
		err := e.cleanPage(page)
		if err != nil {
			return err
		}
	}

	//find the key
	newIndex, found := e.findKey(page, []byte(key))
	// get the slot count
	slotCount := int(page.ReadSlotCount())
	if found {
		// we found the key, so we will update the value
		err := e.putKeyValueInPageSlot(page, int16(newIndex), []byte(key), []byte(value))
		if err != nil {
			return err
		}
	} else {
		// move the slots over if needed
		for j := slotCount; j > newIndex; j-- {
			sl := page.ReadPageSlot(int16(j - 1))
			page.WritePageSlot(int16(j), sl)
		}
		err := e.putKeyValueInPageSlot(page, int16(newIndex), []byte(key), []byte(value))
		if err != nil {
			return err
		}
		// update the slot count
		page.WriteSlotCount(int16(slotCount + 1))
	}
	return nil
}

func (e *ExtendibleHashTable) putKeyValueInPageSlot(page *bufferpool.Page, slotNumber int16, keyBytes []byte, payloadBytes []byte) error {
	freeSpaceOffset := page.ReadFreeSpaceOffset()

	keyLength := len(keyBytes)
	payloadChunkLength := len(payloadBytes)

	// check for overflow
	if payloadChunkLength > bufferpool.MAX_PAYLOAD_CHUNK_SIZE_ON_PAGE {
		return errors.New("overflow")
	}

	totalPayloadSize := page.ComputeHashPayloadTotalLength(keyLength, payloadChunkLength)

	// check to make sure we don't blow free space
	if page.FreeSpaceOnPage() < int16(totalPayloadSize) {
		return errors.New("page is full")
	}

	// compute the new free space offset
	freeSpaceOffset -= int16(totalPayloadSize)

	// write the payload
	page.WriteHashPagePayloadBytes(freeSpaceOffset, int16(keyLength), keyBytes, int16(payloadChunkLength), payloadBytes)

	// update the free space offset
	page.WriteFreeSpaceOffset(int16(freeSpaceOffset))

	// make a slot
	slot := bufferpool.PageSlot{
		PayloadOffset: freeSpaceOffset,
	}
	// write the slot
	page.WritePageSlot(slotNumber, slot)

	return nil
}
