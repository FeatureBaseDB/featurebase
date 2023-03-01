// Copyright 2023 Molecula Corp. All rights reserved.
package bufferpool

import (
	"fmt"
	"os"
	"sync"

	"github.com/pkg/errors"
)

type FileShardID struct {
	ObjectId int32
	Shard    int32
}

// OnDiskDiskManager is a on disk implementation for a DiskManager interface
type OnDiskDiskManager struct {
	mu sync.Mutex

	files map[FileShardID]*os.File
}

// NewInMemDiskSpillingDiskManager returns a in-memory version of disk manager
func NewOnDiskDiskManager() *OnDiskDiskManager {
	dm := &OnDiskDiskManager{
		files: make(map[FileShardID]*os.File),
	}
	return dm
}

func (d *OnDiskDiskManager) CreateOrOpenShard(objectId int32, shard int32, dataFile string) error {
	// serialize access here
	d.mu.Lock()
	defer d.mu.Unlock()

	fileID := FileShardID{objectId, shard}

	// do we have the file already open?
	_, ok := d.files[fileID]
	if ok {
		return nil
	}

	var fd *os.File

	// see if the file for this shard exists
	_, err := os.Stat(dataFile)
	if err != nil {
		//create file
		fd, err = os.OpenFile(dataFile, os.O_RDWR|os.O_CREATE, 0o600)
		if err != nil {
			return fmt.Errorf("open file: %w", err)
		}
		// write the root page
		err = d.writeRootPage(fd, objectId, shard)
		if err != nil {
			return err
		}
	} else {
		// open or create the file
		fd, err = os.OpenFile(dataFile, os.O_RDWR, 0o600)
		if err != nil {
			return fmt.Errorf("open file: %w", err)
		}
	}
	d.files[fileID] = fd

	return nil
}

func (d *OnDiskDiskManager) writeRootPage(fd *os.File, objectId int32, shard int32) error {
	headerPage := NewPage(PageID{objectId, shard, 0}, 0)
	headerPage.WritePageNumber(0)
	headerPage.WriteFreeSpaceOffset(int16(PAGE_SIZE))
	headerPage.WriteNextPointer(PageID{0, 0, INVALID_PAGE})
	headerPage.WritePrevPointer(PageID{0, 0, INVALID_PAGE})
	headerPage.WritePageType(PAGE_TYPE_BTREE_HEADER)

	// write a slot that points to the initial root page
	rootPageID := PageID{objectId, shard, 1}

	// get the free space offset
	freeSpaceOffset := headerPage.ReadFreeSpaceOffset()

	keyBytes := []byte{0, 0, 0, 0}

	// build a chunk
	chunk := InternalPageChunk{
		KeyLength: int16(len(keyBytes)),
		KeyBytes:  keyBytes,
		PtrValue:  1,
	}

	// compute the new free space offset
	freeSpaceOffset -= int16(chunk.Length())

	headerPage.WriteInternalPageChunk(freeSpaceOffset, chunk)

	// update the free space offset
	headerPage.WriteFreeSpaceOffset(int16(freeSpaceOffset))

	// make a slot
	slot := PageSlot{
		PayloadOffset: freeSpaceOffset,
	}
	// write the slot
	headerPage.WritePageSlot(0, slot)

	// update the slot count
	headerPage.WriteSlotCount(int16(1))

	_, err := fd.WriteAt(headerPage.data[:], 0)
	if err != nil {
		return err
	}

	rootPage := NewPage(rootPageID, 0)
	rootPage.WritePageNumber(1)
	rootPage.WriteFreeSpaceOffset(int16(PAGE_SIZE))
	rootPage.WriteNextPointer(PageID{0, 0, INVALID_PAGE})
	rootPage.WritePrevPointer(PageID{0, 0, INVALID_PAGE})
	rootPage.WritePageType(PAGE_TYPE_BTREE_LEAF)

	_, err = fd.WriteAt(rootPage.data[:], PAGE_SIZE)
	if err != nil {
		return err
	}

	return nil
}

// ReadPage reads a page from disk
func (d *OnDiskDiskManager) ReadPage(pageID PageID) (*Page, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	// do we have the file open
	fileID := FileShardID{pageID.ObjectID, pageID.Shard}

	fd, ok := d.files[fileID]
	if !ok {
		return nil, errors.Errorf("file id '%d' not open", pageID.ObjectID)
	}

	info, err := os.Stat(fd.Name())
	if err != nil {
		return nil, err
	}

	numPages := info.Size() / int64(PAGE_SIZE)

	// check we're not asking for page out of range
	if pageID.Page < 0 || pageID.Page >= numPages {
		return nil, errors.Errorf("requested page number '%d' out of range", pageID.Page)
	}
	offset := pageID.Page * int64(PAGE_SIZE)

	var page = pageSyncPool.Get().(*Page)
	// we have to do this stupid check because if -cpuprofile is set for go test, this
	// the previous line return a weird nil-ish thing...
	if page == (*Page)(nil) {
		page = pageSyncPool.New().(*Page)
	}
	page.id = pageID

	// do the read
	_, err = fd.ReadAt(page.data[:], offset)
	if err != nil {
		return nil, err
	}
	return page, nil
}

// WritePage writes a page in memory to pages
func (d *OnDiskDiskManager) WritePage(page *Page) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// do we have the file open
	fileID := FileShardID{page.id.ObjectID, page.id.Shard}

	fd, ok := d.files[fileID]
	if !ok {
		return errors.Errorf("file id '%d' not open", page.id.ObjectID)
	}

	info, err := os.Stat(fd.Name())
	if err != nil {
		return err
	}

	numPages := info.Size() / int64(PAGE_SIZE)

	// check we're not asking for page out of range
	if page.id.Page < 0 || page.id.Page >= numPages {
		return errors.Errorf("requested page number '%d' out of range", page.id.Page)
	}
	offset := page.id.Page * int64(PAGE_SIZE)

	// do the write
	_, err = fd.WriteAt(page.data[:], offset)
	if err != nil {
		return err
	}
	// yeah, nah
	// err = d.fd.Sync()
	// if err != nil {
	// 	return err
	// }
	return nil
}

// AllocatePage allocates a page and returns the page number
func (d *OnDiskDiskManager) AllocatePage(objectId int32, shard int32) (PageID, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	// do we have the file open
	fileID := FileShardID{objectId, shard}

	fd, ok := d.files[fileID]
	if !ok {
		return PageID{objectId, shard, INVALID_PAGE}, errors.Errorf("file id '%d' not open", objectId)
	}

	info, err := os.Stat(fd.Name())
	if err != nil {
		return PageID{objectId, shard, INVALID_PAGE}, err
	}

	numPages := info.Size() / int64(PAGE_SIZE)
	numPages = numPages + 1

	pageID := PageID{objectId, shard, numPages - 1}
	size := numPages * PAGE_SIZE
	_, err = fd.WriteAt([]byte{0}, size-1)
	if err != nil {
		return PageID{objectId, shard, INVALID_PAGE}, err
	}
	return pageID, nil
}

// DeallocatePage removes page from disk
func (d *OnDiskDiskManager) DeallocatePage(pageID PageID) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// nothing to do right now
	return nil
}

func (d *OnDiskDiskManager) FileSize(objectId int32, shard int32) int64 {
	// TODO(pok) return correct file size
	return 0
}

func (d *OnDiskDiskManager) Close() {
	d.mu.Lock()
	defer d.mu.Unlock()

	for _, fd := range d.files {
		fd.Close()
	}
	// empty the efiles
	d.files = make(map[FileShardID]*os.File, 0)
}
