package bufferpool

import (
	"errors"
	"fmt"
	"sync"
)

// FrameID is the type for frame id
type FrameID int

// PageID is the type for page id
type PageID int

var pageSyncPool = sync.Pool{
	New: func() any {
		pg := new(Page)
		pg.id = PageID(INVALID_PAGE)
		pg.isDirty = false
		pg.pinCount = 0
		return pg
	},
}

// BufferPool represents a buffer pool of pages
type BufferPool struct {
	// the underlying storage
	diskManager DiskManager
	// the actual pages in the buffer pool
	pages []*Page
	// the replacer that will elect replacements when buffer pool is full
	replacer *ClockReplacer
	// the list of free frames
	freeList []FrameID
	// the map of frames to page ids to frame ids
	// frame ids are the offset into pages
	// if you ask the pool for page 673, this will know at
	// what offset in pages page 673 will exist
	pageTable map[PageID]FrameID
}

// TODO(pok) implement a lazy writer
//  * if free list is 'low' then
//		* increase size of cache if there is physical memory available
//		* write out old pages and boot them from the cache to increase free list

// TODO(pok) implement a checkpoint that scans the pool and writes out dirty pages every
// minute or so

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
		pages:       pages,
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
	for _, p := range b.pages {
		if p != nil {
			p.Dump("")
		}
	}
	fmt.Printf("------------------------------------------------------------------------------------------\n")
	fmt.Println()
}

// FetchPage fetches the requested page from the buffer pool.
func (b *BufferPool) FetchPage(pageID PageID) (*Page, error) {
	// if it is in buffer pool already then just return it
	if frameID, ok := b.pageTable[pageID]; ok {
		page := b.pages[frameID]
		page.pinCount++
		b.replacer.Pin(frameID)
		return page, nil
	}

	// not in the buffer pool so try the free list or
	// the replacer will vote a page off the island
	frameID, isFromFreeList, err := b.getFrameID()
	if err != nil {
		return nil, err
	}

	if !isFromFreeList {
		// if it didn't come from the freelist then
		// remove page from current frame, writing it out if dirty
		currentPage := b.pages[frameID]
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
	pageSyncPool.Put(b.pages[frameID])
	b.pages[frameID] = page
	b.replacer.Pin(frameID)

	return page, nil
}

// UnpinPage unpins the target page from the buffer pool
func (b *BufferPool) UnpinPage(pageID PageID) error {
	if frameID, ok := b.pageTable[pageID]; ok {
		page := b.pages[frameID]
		page.DecPinCount()

		if page.pinCount <= 0 {
			b.replacer.Unpin(frameID)
		}
		return nil
	}

	return errors.New("could not find page")
}

// FlushPage Flushes the target page to disk
func (b *BufferPool) FlushPage(pageID PageID) bool {
	if frameID, ok := b.pageTable[pageID]; ok {
		page := b.pages[frameID]
		page.DecPinCount()

		b.diskManager.WritePage(page)
		page.isDirty = false

		return true
	}
	return false
}

// NewPage allocates a new page in the buffer pool with the disk manager help
func (b *BufferPool) NewPage() (*Page, error) {
	// get a free frame
	frameID, isFromFreeList, err := b.getFrameID()
	if err != nil {
		return nil, err
	}

	if !isFromFreeList {
		// remove page from current frame
		currentPage := b.pages[frameID]
		if currentPage != nil {
			if currentPage.isDirty {
				b.diskManager.WritePage(currentPage)
			}

			delete(b.pageTable, currentPage.id)
		}
	}

	// allocates new page
	pageID, err := b.diskManager.AllocatePage()
	if err != nil {
		return nil, err
	}
	page := &Page{pageID, 1, false, [PAGE_SIZE]byte{}}
	page.WritePageNumber(int32(pageID))
	page.WriteFreeSpaceOffset(int16(PAGE_SIZE))
	page.WriteNextPointer(int32(INVALID_PAGE))
	page.WritePrevPointer(int32(INVALID_PAGE))

	// update the frame table
	b.pageTable[pageID] = frameID
	pageSyncPool.Put(b.pages[frameID])
	b.pages[frameID] = page

	return page, nil
}

// ScratchPage returns a page outside the buffer pool - do not use if you intend the page
// to be in the buffer pool (use NewPage() for that)
// ScratchPage is intended to be used in cases where you need the Page primitives
// and will copy the scratch page back over a real page later
func (b *BufferPool) ScratchPage() *Page {
	page := &Page{
		id:       PageID(INVALID_PAGE),
		pinCount: 0,
		isDirty:  false,
		data:     [PAGE_SIZE]byte{},
	}
	page.WritePageNumber(int32(INVALID_PAGE))
	page.WriteFreeSpaceOffset(int16(PAGE_SIZE))
	page.WriteNextPointer(int32(INVALID_PAGE))
	page.WritePrevPointer(int32(INVALID_PAGE))
	return page
}

// DeletePage deletes a page from the buffer pool
func (b *BufferPool) DeletePage(pageID PageID) error {
	var frameID FrameID
	var ok bool
	if frameID, ok = b.pageTable[pageID]; !ok {
		return nil
	}

	page := b.pages[frameID]

	if page.pinCount > 0 {
		return errors.New("pin count greater than 0")
	}
	delete(b.pageTable, page.id)
	b.replacer.Pin(frameID)
	b.diskManager.DeallocatePage(pageID)

	b.freeList = append(b.freeList, frameID)

	return nil
}

// FlushAllpages flushes all the pages in the buffer pool to disk
// Yeah, never call this unless you know what you are doing
func (b *BufferPool) FlushAllpages() {
	for pageID := range b.pageTable {
		b.FlushPage(pageID)
	}
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

// OnDiskSize exposes the on disk size of the backing store
// behind this buffer pool
func (b *BufferPool) OnDiskSize() int64 {
	return b.diskManager.FileSize()
}

// Close closes the buffer pool
func (b *BufferPool) Close() {
	b.diskManager.Close()
}
