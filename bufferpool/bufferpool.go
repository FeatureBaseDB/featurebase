package bufferpool

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
)

// FrameID is the type for frame id
type FrameID int

// PageID is the type for page id
type PageID struct {
	ObjectID int32
	Shard    int32
	Page     int64
}

func (p PageID) Bytes() []byte {
	var valueBuf bytes.Buffer
	v := make([]byte, 4)
	binary.BigEndian.PutUint32(v, uint32(p.ObjectID))
	valueBuf.Write(v)
	binary.BigEndian.PutUint32(v, uint32(p.Shard))
	valueBuf.Write(v)
	vp := make([]byte, 8)
	binary.BigEndian.PutUint64(vp, uint64(p.Page))
	valueBuf.Write(vp)
	return valueBuf.Bytes()
}

var pageSyncPool = sync.Pool{
	New: func() any {
		pg := new(Page)
		pg.latchState = None
		pg.id = PageID{0, 0, INVALID_PAGE}
		pg.isDirty = false
		pg.pinCount = 0
		return pg
	},
}

// BufferPool represents a buffer pool of pages
type BufferPool struct {

	// the underlying storage
	diskManager DiskManager

	// the actual frames in the buffer pool
	frames []*Page
	// the list of free frames
	freeList []FrameID

	framesMu sync.RWMutex

	// the replacer that will elect replacements when buffer pool is full
	replacer *ClockReplacer

	// the map of frames to page ids to frame ids
	// frame ids are the offset into frames (above)
	// if you ask the pool for page 1:1:673, this will know at
	// what offset in pages page 1:1:673 will exist (or not)
	pageTable   map[PageID]FrameID
	pageTableMu sync.RWMutex
}

// TODO(pok) implement a lazy writer
//  * if free list is 'low' then
//		* increase size of cache if there is physical memory available
//		* write out old pages and boot them from the cache to increase free list

// NewBufferPool returns a buffer pool
func NewBufferPool(maxSize int, diskManager DiskManager) *BufferPool {
	freeList := make([]FrameID, 0)
	pages := make([]*Page, maxSize)
	for i := 0; i < maxSize; i++ {
		frameNumber := FrameID(i)
		freeList = append(freeList, frameNumber)
	}
	clockReplacer := NewClockReplacer(maxSize)
	return &BufferPool{
		diskManager: diskManager,
		frames:      pages,
		replacer:    clockReplacer,
		freeList:    freeList,
		pageTable:   make(map[PageID]FrameID),
	}
}

// Dumps all the pages in the buffer pool
func (b *BufferPool) Dump() {
	fmt.Println()
	fmt.Printf("------------------------------------------------------------------------------------------\n")
	fmt.Printf("BUFFER POOL\n")
	for _, p := range b.frames {
		if p != nil {
			p.Dump("")
		}
	}
	fmt.Printf("------------------------------------------------------------------------------------------\n")
	fmt.Println()
}

// FetchPage fetches the requested page from the buffer pool.
func (b *BufferPool) FetchPage(pageID PageID) (*Page, error) {
	b.pageTableMu.Lock()
	defer b.pageTableMu.Unlock()

	b.framesMu.RLock()
	// if it is in buffer pool already then just return it
	if frameID, ok := b.pageTable[pageID]; ok {
		page := b.frames[frameID]
		page.pinCount++
		b.replacer.Pin(frameID)
		b.framesMu.RUnlock()
		return page, nil
	}

	// we will need to write to frames, so unlock and take write lock
	b.framesMu.RUnlock()
	b.framesMu.Lock()
	defer b.framesMu.Unlock()

	// not in the buffer pool so try the free list or
	// the replacer will vote a page off the island
	frameID, isFromFreeList, err := b.getFrameID()
	if err != nil {
		b.framesMu.RUnlock()
		return nil, err
	}

	if !isFromFreeList {
		// if it didn't come from the freelist then
		// remove page from current frame, writing it out if dirty
		currentPage := b.frames[frameID]
		if currentPage != nil {
			if currentPage.isDirty {
				b.diskManager.WritePage(currentPage)
			}
			delete(b.pageTable, currentPage.id)
		}
	}

	// if we got to here, sorry, have to do an I/O
	page, err := b.diskManager.ReadPage(pageID)
	if err != nil {
		return nil, err
	}
	page.pinCount = 1
	b.pageTable[pageID] = frameID
	pageSyncPool.Put(b.frames[frameID])
	b.frames[frameID] = page
	b.replacer.Pin(frameID)

	return page, nil
}

// UnpinPage unpins the target page from the buffer pool
func (b *BufferPool) UnpinPage(pageID PageID) error {
	b.framesMu.RLock()
	b.pageTableMu.RLock()
	defer b.framesMu.RUnlock()
	defer b.pageTableMu.RUnlock()

	if frameID, ok := b.pageTable[pageID]; ok {
		page := b.frames[frameID]
		page.DecPinCount()

		if page.pinCount <= 0 {
			b.replacer.Unpin(frameID)
		}
		return nil
	}

	return errors.New("could not find page")
}

// FlushPage Flushes the target page to disk
// This shold not be called during normal operation
func (b *BufferPool) FlushPage(pageID PageID) bool {
	b.framesMu.RLock()
	b.pageTableMu.RLock()
	defer b.framesMu.RUnlock()
	defer b.pageTableMu.RUnlock()

	if frameID, ok := b.pageTable[pageID]; ok {
		page := b.frames[frameID]
		page.DecPinCount()

		b.diskManager.WritePage(page)
		page.isDirty = false

		return true
	}
	return false
}

// NewPage allocates a new page in the buffer pool with the disk manager help
func (b *BufferPool) NewPage(fileID int32, shard int32) (*Page, error) {
	b.framesMu.Lock()
	b.pageTableMu.Lock()
	defer b.framesMu.Unlock()
	defer b.pageTableMu.Unlock()

	// get a free frame
	frameID, isFromFreeList, err := b.getFrameID()
	if err != nil {
		return nil, err
	}

	if !isFromFreeList {
		// remove page from current frame
		currentPage := b.frames[frameID]
		if currentPage != nil {
			if currentPage.isDirty {
				b.diskManager.WritePage(currentPage)
			}

			delete(b.pageTable, currentPage.id)
		}
	}

	// allocates new page
	pageID, err := b.diskManager.AllocatePage(fileID, shard)
	if err != nil {
		return nil, err
	}
	page := NewPage(pageID, 1)
	page.WritePageNumber(pageID.Page)
	page.WriteFreeSpaceOffset(int16(PAGE_SIZE))
	page.WriteNextPointer(PageID{0, 0, INVALID_PAGE})
	page.WritePrevPointer(PageID{0, 0, INVALID_PAGE})

	// update the frame table
	b.pageTable[pageID] = frameID
	pageSyncPool.Put(b.frames[frameID])
	b.frames[frameID] = page

	return page, nil
}

// ScratchPage returns a page outside the buffer pool - do not use if you intend the page
// to be in the buffer pool (use NewPage() for that)
// ScratchPage is intended to be used in cases where you need the Page primitives
// and will copy the scratch page back over a real page later
func (b *BufferPool) ScratchPage() *Page {
	page := &Page{
		id:       PageID{0, 0, INVALID_PAGE},
		pinCount: 0,
		isDirty:  false,
		data:     [PAGE_SIZE]byte{},
	}
	page.WritePageNumber(INVALID_PAGE)
	page.WriteFreeSpaceOffset(int16(PAGE_SIZE))
	page.WriteNextPointer(PageID{0, 0, INVALID_PAGE})
	page.WritePrevPointer(PageID{0, 0, INVALID_PAGE})
	return page
}

// DeletePage deletes a page from the buffer pool
func (b *BufferPool) DeletePage(pageID PageID) error {
	b.framesMu.Lock()
	b.pageTableMu.Lock()
	defer b.framesMu.Unlock()
	defer b.pageTableMu.Unlock()

	var frameID FrameID
	var ok bool
	if frameID, ok = b.pageTable[pageID]; !ok {
		return nil
	}

	page := b.frames[frameID]

	if page.pinCount > 0 {
		return errors.New("pin count greater than 0")
	}
	delete(b.pageTable, page.id)
	b.replacer.Pin(frameID)
	b.diskManager.DeallocatePage(pageID)

	b.freeList = append(b.freeList, frameID)

	return nil
}

func (b *BufferPool) getFrameID() (FrameID, bool, error) {
	if len(b.freeList) > 0 {
		frameID, newFreeList := b.freeList[0], b.freeList[1:]
		b.freeList = newFreeList
		return frameID, true, nil
	}

	victim, err := b.replacer.Victim()
	return victim, false, err
}

// Close closes the buffer pool
func (b *BufferPool) Close() {
	b.diskManager.Close()
}
