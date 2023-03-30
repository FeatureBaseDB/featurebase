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

// TupleStoreDiskManager is a on disk implementation for a DiskManager interface
type TupleStoreDiskManager struct {
	mu sync.Mutex

	files map[FileShardID]*os.File
}

// NewTupleStoreDiskManager returns a disk manager for t-store btrees
func NewTupleStoreDiskManager() *TupleStoreDiskManager {
	dm := &TupleStoreDiskManager{
		files: make(map[FileShardID]*os.File),
	}
	return dm
}

func (d *TupleStoreDiskManager) CreateOrOpenShard(objectId int32, shard int32, dataFile string) error {
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
		err = d.bootstrapTStoreFile(fd, objectId, shard)
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

func (d *TupleStoreDiskManager) writePageIDToSlot(page *Page, slotNumber int16, key byte, ptr int64) {
	// get the free space offset
	freeSpaceOffset := page.ReadFreeSpaceOffset()
	keyBytes := []byte{0, 0, 0, key}
	// build a chunk
	chunk := InternalPageChunk{
		KeyLength: int16(len(keyBytes)),
		KeyBytes:  keyBytes,
		PtrValue:  ptr,
	}
	// compute the new free space offset
	freeSpaceOffset -= int16(chunk.Length())
	page.WriteInternalPageChunk(freeSpaceOffset, chunk)
	// update the free space offset
	page.WriteFreeSpaceOffset(int16(freeSpaceOffset))

	// make a slot
	slot := PageSlot{
		PayloadOffset: freeSpaceOffset,
	}
	// write the slot
	page.WritePageSlot(slotNumber, slot)

	// update the slot count
	slotCount := page.ReadSlotCount()
	slotCount += 1
	page.WriteSlotCount(slotCount)
}

func (d *TupleStoreDiskManager) bootstrapTStoreFile(fd *os.File, objectId int32, shard int32) error {
	// make a new header page
	headerPage := NewPage(PageID{objectId, shard, 0}, 0)
	headerPage.WritePageNumber(0)
	headerPage.WriteFreeSpaceOffset(int16(PAGE_SIZE))
	headerPage.WriteNextPointer(PageID{0, 0, INVALID_PAGE})
	headerPage.WritePrevPointer(PageID{0, 0, INVALID_PAGE})
	headerPage.WritePageType(PAGE_TYPE_BTREE_HEADER)

	// write the pages ids for the data and schema btrees into the right slots
	dataRootPageID := PageID{objectId, shard, 1}
	schemaRootPageID := PageID{objectId, shard, 2}

	d.writePageIDToSlot(headerPage, 0, 0, dataRootPageID.Page)
	d.writePageIDToSlot(headerPage, 1, 1, schemaRootPageID.Page)

	fileOffset := int64(0)

	// write the header page
	_, err := fd.WriteAt(headerPage.data[:], fileOffset)
	if err != nil {
		return err
	}

	// write the data root page
	fileOffset += PAGE_SIZE
	dataRootPage := NewPage(dataRootPageID, 0)
	dataRootPage.WritePageNumber(dataRootPageID.Page)
	dataRootPage.WriteFreeSpaceOffset(int16(PAGE_SIZE))
	dataRootPage.WriteNextPointer(PageID{0, 0, INVALID_PAGE})
	dataRootPage.WritePrevPointer(PageID{0, 0, INVALID_PAGE})
	dataRootPage.WritePageType(PAGE_TYPE_BTREE_LEAF)

	_, err = fd.WriteAt(dataRootPage.data[:], fileOffset)
	if err != nil {
		return err
	}

	// write the schema root page
	fileOffset += PAGE_SIZE
	schemaRootPage := NewPage(schemaRootPageID, 0)
	schemaRootPage.WritePageNumber(dataRootPageID.Page)
	schemaRootPage.WriteFreeSpaceOffset(int16(PAGE_SIZE))
	schemaRootPage.WriteNextPointer(PageID{0, 0, INVALID_PAGE})
	schemaRootPage.WritePrevPointer(PageID{0, 0, INVALID_PAGE})
	schemaRootPage.WritePageType(PAGE_TYPE_BTREE_LEAF)

	_, err = fd.WriteAt(schemaRootPage.data[:], fileOffset)
	if err != nil {
		return err
	}
	return nil
}

// ReadPage reads a page from disk
func (d *TupleStoreDiskManager) ReadPage(pageID PageID) (*Page, error) {
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
func (d *TupleStoreDiskManager) WritePage(page *Page) error {
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
func (d *TupleStoreDiskManager) AllocatePage(objectId int32, shard int32) (PageID, error) {
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
func (d *TupleStoreDiskManager) DeallocatePage(pageID PageID) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// nothing to do right now
	return nil
}

func (d *TupleStoreDiskManager) FileSize(objectId int32, shard int32) int64 {
	// TODO(pok) return correct file size
	return 0
}

func (d *TupleStoreDiskManager) Close() {
	d.mu.Lock()
	defer d.mu.Unlock()

	for _, fd := range d.files {
		fd.Close()
	}
	// empty the efiles
	d.files = make(map[FileShardID]*os.File, 0)
}
