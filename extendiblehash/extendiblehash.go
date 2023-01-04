package extendiblehash

import (
	"bytes"
	"fmt"

	"github.com/molecula/featurebase/v3/bufferpool"
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
	bytesPerKV := keyLength + valueLength + bufferpool.PAGE_SLOT_LENGTH
	keysPerPage := (bufferpool.PAGE_SIZE - bufferpool.PAGE_SLOTS_START_OFFSET) / bytesPerKV

	//create the root page
	page, err := bufferPool.NewPage()
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
		slot := page.ReadSlot(int16(index))
		return slot.ValueBytes(page), true, nil
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
		return 0, fmt.Errorf("hash (%d) out of the directory array bounds (%d)", hash, len(e.directory))
	}
	id := e.directory[hash]
	return bufferpool.PageID(id), nil
}

func (e *ExtendibleHashTable) findKey(page *bufferpool.Page, key []byte) (int, bool) {
	minIndex := 0
	onePastMaxIndex := int(page.ReadSlotCount())

	for onePastMaxIndex != minIndex {
		index := (minIndex + onePastMaxIndex) / 2
		s := page.ReadSlot(int16(index))
		keyAtIndex := s.KeyBytes(page)

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
	p0.WritePageNumber(int32(page.ID()))
	p0.WritePageType(bufferpool.PAGE_TYPE_HASH_TABLE)

	// allocate new page for split
	p1, err := e.bufferPool.NewPage()
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
		keyBytes := slot.KeyBytes(page)
		k := string(keyBytes)
		h := Key(k).Hash()

		if h&hiBit > 0 {
			sc := p1.ReadSlotCount()
			p1.WriteKeyValueInSlot(sc, keyBytes, slot.ValueBytes(page))
			// update the slot count
			p1.WriteSlotCount(int16(sc + 1))

		} else {
			sc := p0.ReadSlotCount()
			p0.WriteKeyValueInSlot(sc, keyBytes, slot.ValueBytes(page))
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
	p0.WritePage(page)

	return nil
}

func (e *ExtendibleHashTable) cleanPage(page *bufferpool.Page) error {
	scratch := e.bufferPool.ScratchPage()
	// copy page number
	scratch.WritePageNumber(int32(page.ID()))
	// set the page type
	scratch.WritePageType(bufferpool.PAGE_TYPE_HASH_TABLE)
	// copy local depth
	scratch.WriteLocalDepth(page.ReadLocalDepth())

	// copy slots from page to scratch
	si := bufferpool.NewPageSlotIterator(page, 0)
	for {
		slot := si.Next()
		if slot == nil {
			break
		}
		scratch.WriteKeyValueInSlot(si.Cursor(), slot.KeyBytes(page), slot.ValueBytes(page))
	}

	// update the slot count
	scratch.WriteSlotCount(page.ReadSlotCount())

	// write scratch back to page
	scratch.WritePage(page)
	return nil
}

func (e *ExtendibleHashTable) keyValueWillFit(page *bufferpool.Page, key, value []byte) bool {
	// will this k/v fit on the page?
	slotLen := 4                          // we need 2 len words for the slot
	chunkLen := 6 + len(key) + len(value) // int16 len + int32 len + len of respective []byte
	fs := page.FreeSpace()
	return fs > (int16(slotLen) + int16(chunkLen))
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
		err := page.WriteKeyValueInSlot(int16(newIndex), []byte(key), []byte(value))
		if err != nil {
			return err
		}
	} else {
		// TODO(pok) should check in WriteSlot() to see if we are out space too...

		// TODO(pok) we should move all the slots in one fell swoop, because,... performance
		// move all the slots after where we are going to insert
		for j := slotCount; j > newIndex; j-- {
			sl := page.ReadSlot(int16(j - 1))
			page.WriteSlot(int16(j), sl)
		}
		err := page.WriteKeyValueInSlot(int16(newIndex), []byte(key), []byte(value))
		if err != nil {
			return err
		}
		// update the slot count
		page.WriteSlotCount(int16(slotCount + 1))
	}
	return nil
}
