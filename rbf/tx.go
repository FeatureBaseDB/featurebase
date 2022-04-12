// Copyright 2021 Molecula Corp. All rights reserved.
package rbf

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"sync"

	"github.com/benbjohnson/immutable"
	"github.com/molecula/featurebase/v3/roaring"
	txkey "github.com/molecula/featurebase/v3/short_txkey"
	"github.com/molecula/featurebase/v3/vprint"
)

var _ = txkey.ToString

// Tx represents a transaction.
type Tx struct {
	mu          sync.RWMutex
	db          *DB                  // parent db
	meta        [PageSize]byte       // copy of current meta page
	walID       int64                // max WAL ID at start of tx
	walPageN    int                  // wal page count
	rootRecords *immutable.SortedMap // read-only cache of root records

	// pageMap holds WAL pages that have not yet been transferred
	// into the database pages. So it can be empty, if the whole previous
	// WAL has been checkpointed back into the database.
	pageMap  *PageMap // mapping of database pages to WAL IDs
	writable bool     // if true, tx can write

	dirtyPages       map[uint32][]byte // updated pages in this tx
	dirtyBitmapPages map[uint32][]byte // updated bitmap pages in this tx

	// If Rollback() has already completed, don't do it again.
	// Note db == nil means that commit has already been done.
	rollbackDone bool

	// DeleteEmptyContainer lets us by default match the roaring
	// behavior where an existing container has all its bits cleared
	// but still sticks around in the database.
	DeleteEmptyContainer bool

	// It is possible for a modification of the free list to cause a page to
	// be allocated or deallocated, which would modify the free list.
	//
	// For the case where pages need to be allocated during free list
	// changes, we can trivially just allocate new pages and not use the
	// free list. That's the simple case...
	modifyingFreelist bool
	// But removals can't be deferred/not-done like that. If a free list
	// change causes us to deallocate a page (such as if we're *allocating*
	// a page, which causes it to be *removed* from the free list), we really
	// do need to record that, but if we try to do it during the update
	// process, things could go horribly wrong. So, we have a transient list
	// of page numbers which have been deallocated, but it happened *during*
	// the modification of the free list. The top-level modification then
	// processes them on its way out, using a defer. During the processing
	// of this list, we *still* have the flag set, and we make a new list
	// while processing the list, so if somehow a pending add to the list
	// manages to trigger a *deallocation* (which I don't think should be
	// happening), we'll process that one after the current list is processed.
	pendingFreelistAdds []uint32

	// DEBUG
	stack []byte
}

func (tx *Tx) DBPath() string {
	return tx.db.Path
}

// Writable returns true if the transaction can mutate data.
func (tx *Tx) Writable() bool {
	return tx.writable
}

// dirty returns true if any pages have been updated in this tx.
func (tx *Tx) dirty() bool {
	return tx.dirtyN() != 0
}

// dirtyN returns the number of dirty pages.
func (tx *Tx) dirtyN() int {
	return len(tx.dirtyPages) + len(tx.dirtyBitmapPages)
}

// PageN returns the number of pages in the database as seen by this transaction.
func (tx *Tx) PageN() int {
	return int(readMetaPageN(tx.meta[:]))
}

// Commit completes the transaction and persists data changes.
func (tx *Tx) Commit() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.db == nil {
		return ErrTxClosed
	}

	// Remove any free pages off the end of the file and update the size.
	if err := tx.truncateFreelist(); err != nil {
		return err
	}

	// If any pages have been written, ensure we write a new meta page with
	// the commit flag to mark the end of the transaction.
	if tx.dirty() {
		if err := tx.flush(); err != nil {
			return err
		}
		// future plan: after checkpoint is moved to background
		// or not every removeTx, then we can move the
		// tx.db.rootRecords = tx.rootRecords into removeTx().
		//
		// ... or maybe not: let's do that part here, and then removeTx
		// may or may not start a checkpoint, possibly asynchronously.
		//
		// avoid race detector firing on a write race here
		// vs the read of rootRecords at db.Begin(), then release
		// the lock, because we need removeTx to grab the lock to
		// work, but if it wants to checkpoint, it wants to be able to return
		// to us here and still be holding the lock.
		tx.db.mu.Lock()
		tx.db.rootRecords = tx.rootRecords
		tx.db.pageMap = tx.pageMap
		tx.db.walPageN = tx.walPageN
		tx.db.mu.Unlock()
	}

	tx.db.mu.Lock()
	defer tx.db.mu.Unlock()
	// Disconnect transaction from DB.
	return tx.db.removeTx(tx)
}

// truncateFreelist removes any free pages off the end of the file and updates
// the size of the database. This allows the data file to be resized on checkpoint.
func (tx *Tx) truncateFreelist() error {
	for {
		if truncated, err := tx.truncateLastFreePage(); err != nil {
			return err
		} else if !truncated {
			return nil // no more free pages at end of file, exit
		}
	}
}

// truncateLastFreePage removes the last page from the file if it is a free page.
// The page count is then decremented to move the high water mark to remove the page.
// Returns true if a page was removed, otherwise returns false.
func (tx *Tx) truncateLastFreePage() (truncated bool, outErr error) {
	tx.modifyingFreelist = true
	defer tx.freelistCleanup(&outErr)

	c := tx.db.getFreelistCursor(tx)
	if err := c.Last(); err == io.EOF {
		return false, nil
	} else if err != nil {
		return false, err
	}

	elem := &c.stack.elems[c.stack.top]
	leafPage, _, err := c.tx.readPage(elem.pgno)
	if err != nil {
		return false, err
	}
	cell := readLeafCell(leafPage, elem.index)

	// If page number is not the last page then exit.
	pgno := uint32((cell.Key << 16) | uint64(cell.lastValue(tx)))
	pageN := readMetaPageN(tx.meta[:])
	if pgno < pageN-1 {
		return false, nil
	}

	// Otherwise remove it from the freelist.
	if changed, err := c.Remove(uint64(pgno)); err != nil {
		return false, err
	} else if !changed {
		vprint.PanicOn(fmt.Sprintf("tx.Tx.truncateLastFreePage(): double alloc: %d", pgno))
	}

	// Decrement the page count in the database.
	writeMetaPageN(tx.meta[:], pageN-1)

	return true, nil
}

func (tx *Tx) Rollback() { tx.rollback(false) }

func (tx *Tx) rollback(hasDBLock bool) {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	// allow Rollback to be called more than once.
	if tx.rollbackDone {
		return
	}
	tx.rollbackDone = true
	if tx.db == nil {
		// Commit already done.
		return
	}

	// Disconnect transaction from DB.
	if !hasDBLock {
		tx.db.mu.Lock()
		defer tx.db.mu.Unlock()
	}
	vprint.PanicOn(tx.db.removeTx(tx))
}

// Root returns the root page number for a bitmap. Returns 0 if the bitmap does not exist.
func (tx *Tx) Root(name string) (uint32, error) {
	tx.mu.RLock()
	defer tx.mu.RUnlock()
	return tx.root(name)
}

func (tx *Tx) root(name string) (uint32, error) {
	records, err := tx.RootRecords()
	if err != nil {
		return 0, err
	}

	pgno, ok := records.Get(name)
	if !ok {
		return 0, ErrBitmapNotFound
	}
	return pgno.(uint32), nil
}

// BitmapNames returns a list of all bitmap names.
func (tx *Tx) BitmapNames() ([]string, error) {
	tx.mu.RLock()
	defer tx.mu.RUnlock()

	if tx.db == nil {
		return nil, ErrTxClosed
	}

	// Read list of root records.
	records, err := tx.RootRecords()
	if err != nil {
		return nil, err
	}

	a := make([]string, 0, records.Len())
	for itr := records.Iterator(); !itr.Done(); {
		k, _ := itr.Next()
		a = append(a, k.(string))
	}
	return a, nil
}

// BitmapExist returns true if bitmap exists.
func (tx *Tx) BitmapExists(name string) (bool, error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return tx.bitmapExists(name)
}

func (tx *Tx) bitmapExists(name string) (bool, error) {
	if tx.db == nil {
		return false, ErrTxClosed
	} else if name == "" {
		return false, ErrBitmapNameRequired
	}

	// Read root records and find entry for bitmap.
	records, err := tx.RootRecords()
	if err != nil {
		return false, err
	}
	_, ok := records.Get(name)
	return ok, nil
}

// CreateBitmap creates a new empty bitmap with the given name.
// Returns an error if the bitmap already exists.
func (tx *Tx) CreateBitmap(name string) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return tx.createBitmap(name)
}

func (tx *Tx) createBitmap(name string) error {
	if tx.db == nil {
		return ErrTxClosed
	} else if !tx.writable {
		return ErrTxNotWritable
	} else if name == "" {
		return ErrBitmapNameRequired
	}

	// Read list of root records.
	records, err := tx.RootRecords()
	if err != nil {
		return err
	}

	// Find btree by name. Exit if already exists.
	if _, ok := records.Get(name); ok {
		return ErrBitmapExists
	}

	// Allocate new root page.
	pgno, err := tx.allocatePgno()
	if err != nil {
		return err
	}

	// Write root page.
	page := allocPage()
	writePageNo(page, pgno)
	writeFlags(page, PageTypeLeaf)
	writeCellN(page, 0)
	if err := tx.writePage(page); err != nil {
		return err
	}

	// Insert into correct index.
	records = records.Set(name, pgno)
	if err := tx.writeRootRecordPages(records); err != nil {
		return fmt.Errorf("write bitmaps: %w", err)
	}

	return nil
}

// CreateBitmapIfNotExists creates a new empty bitmap with the given name.
// This is a no-op if the bitmap already exists.
func (tx *Tx) CreateBitmapIfNotExists(name string) error {
	if err := tx.CreateBitmap(name); err != nil && err != ErrBitmapExists {
		return err
	}
	return nil
}

func (tx *Tx) createBitmapIfNotExists(name string) error {
	if err := tx.createBitmap(name); err != nil && err != ErrBitmapExists {
		return err
	}
	return nil
}

// DeleteBitmap removes a bitmap with the given name.
// Returns an error if the bitmap does not exist.
func (tx *Tx) DeleteBitmap(name string) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.db == nil {
		return ErrTxClosed
	} else if !tx.writable {
		return ErrTxNotWritable
	} else if name == "" {
		return ErrBitmapNameRequired
	}

	// Read list of root records.
	records, err := tx.RootRecords()
	if err != nil {
		return err
	}

	// Find btree by name. Exit if it doesn't exist.
	pgno, ok := records.Get(name)
	if !ok {
		return fmt.Errorf("bitmap does not exist: %q", name)
	}

	// Deallocate all pages in the tree.
	if err := tx.deallocateTree(pgno.(uint32)); err != nil {
		return err
	}

	// Delete from record list & rewrite record pages.
	records = records.Delete(name)
	if err := tx.writeRootRecordPages(records); err != nil {
		return fmt.Errorf("write bitmaps: %w", err)
	}
	tx.rootRecords = records
	return nil
}

// DeleteBitmapsWithPrefix removes all bitmaps with a given prefix.
func (tx *Tx) DeleteBitmapsWithPrefix(prefix string) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.db == nil {
		return ErrTxClosed
	} else if !tx.writable {
		return ErrTxNotWritable
	}

	// Read list of root records.
	records, err := tx.RootRecords()
	if err != nil {
		return err
	}

	for itr := records.Iterator(); !itr.Done(); {
		name, pgno := itr.Next()

		// Skip bitmaps without matching prefix.
		if !strings.HasPrefix(name.(string), prefix) {
			continue
		}
		// Deallocate all pages in the tree.
		if err := tx.deallocateTree(pgno.(uint32)); err != nil {
			return err
		}

		records = records.Delete(name.(string))
	}

	// Rewrite record pages.
	if err := tx.writeRootRecordPages(records); err != nil {
		return fmt.Errorf("write bitmaps: %w", err)
	}
	tx.rootRecords = records
	return nil
}

// RenameBitmap updates the name of an existing bitmap.
// Returns an error if the bitmap does not exist.
func (tx *Tx) RenameBitmap(oldname, newname string) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.db == nil {
		return ErrTxClosed
	} else if !tx.writable {
		return ErrTxNotWritable
	} else if oldname == "" || newname == "" {
		return ErrBitmapNameRequired
	}

	// Read list of root records.
	records, err := tx.RootRecords()
	if err != nil {
		return err
	}

	// Find btree by name. Exit if it doesn't exist.
	pgno, ok := records.Get(oldname)
	if !ok {
		return fmt.Errorf("bitmap does not exist: %q", oldname)
	}

	// Update record name & rewrite record pages.
	records = records.Delete(oldname)
	records = records.Set(newname, pgno)
	if err := tx.writeRootRecordPages(records); err != nil {
		return fmt.Errorf("write bitmaps: %w", err)
	}

	return nil
}

// RootRecords returns a list of root records.
func (tx *Tx) RootRecords() (records *immutable.SortedMap, err error) {
	if tx.rootRecords != nil {
		return tx.rootRecords, nil
	}

	records = immutable.NewSortedMap(nil)
	for pgno := readMetaRootRecordPageNo(tx.meta[:]); pgno != 0; {
		page, _, err := tx.readPage(pgno)
		if err != nil {
			return nil, err
		}

		// Read all records on the page.
		a, err := readRootRecords(page)
		if err != nil {
			return nil, err
		}
		for _, rec := range a {
			records = records.Set(rec.Name, rec.Pgno)
		}

		// Read next overflow page number.
		pgno = WalkRootRecordPages(page)
	}

	// Cache result
	tx.rootRecords = records
	return records, nil
}

// writeRootRecordPages writes a list of root record pages.
func (tx *Tx) writeRootRecordPages(records *immutable.SortedMap) (err error) {

	// Release all existing root record pages.
	for pgno := readMetaRootRecordPageNo(tx.meta[:]); pgno != 0; {
		page, _, err := tx.readPage(pgno)
		if err != nil {
			return err
		}

		err = tx.freePgno(pgno)
		if err != nil {
			return err
		}
		pgno = WalkRootRecordPages(page)
	}

	// Exit early if no records exist.
	if records.Len() == 0 {
		writeMetaRootRecordPageNo(tx.meta[:], 0)
		return nil
	}

	// Allocate initial root record page.
	pgno, err := tx.allocatePgno()
	if err != nil {
		return err
	}
	writeMetaRootRecordPageNo(tx.meta[:], pgno)

	// Write new root record pages.
	for itr := records.Iterator(); !itr.Done(); {
		// Initialize page & write as many records as will fit.
		page := allocPage()
		writePageNo(page, pgno)
		writeFlags(page, PageTypeRootRecord)

		if err := writeRootRecords(page, itr); err == io.ErrShortBuffer {
			// Allocate next pgno and write overflow if we have remaining records.
			if pgno, err = tx.allocatePgno(); err != nil {
				return err
			}
			writeRootRecordOverflowPgno(page, pgno)
		} else if err != nil {
			return err
		}

		// Write page to disk.
		if err := tx.writePage(page); err != nil {
			return err
		}
	}

	// Update cache records.
	tx.rootRecords = records

	return nil
}

// Add sets a given bit on the bitmap.
func (tx *Tx) Add(name string, a ...uint64) (changeCount int, err error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.db == nil {
		return 0, ErrTxClosed
	} else if !tx.writable {
		return 0, ErrTxNotWritable
	} else if name == "" {
		return 0, ErrBitmapNameRequired
	}

	if err := tx.createBitmapIfNotExists(name); err != nil {
		return 0, err
	}

	c, err := tx.cursor(name)
	if err != nil {
		return 0, err
	}
	defer c.Close()

	for _, v := range a {
		if vchanged, err := c.Add(v); err != nil {
			return changeCount, err
		} else if vchanged {
			changeCount++
		}
	}
	return changeCount, nil
}

// Remove unsets a given bit on the bitmap.
func (tx *Tx) Remove(name string, a ...uint64) (changeCount int, err error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.db == nil {
		return 0, ErrTxClosed
	} else if !tx.writable {
		return 0, ErrTxNotWritable
	} else if name == "" {
		return 0, ErrBitmapNameRequired
	}

	c, err := tx.cursor(name)
	if err == ErrBitmapNotFound {
		return 0, nil
	} else if err != nil {
		return 0, err
	}
	defer c.Close()

	for _, v := range a {
		if vchanged, err := c.Remove(v); err != nil {
			return changeCount, err
		} else if vchanged {
			changeCount++
		}
	}
	return changeCount, nil
}

// Contains returns true if the given bit is set on the bitmap.
func (tx *Tx) Contains(name string, v uint64) (bool, error) {
	tx.mu.RLock()
	defer tx.mu.RUnlock()

	if tx.db == nil {
		return false, ErrTxClosed
	} else if name == "" {
		return false, ErrBitmapNameRequired
	}

	c, err := tx.cursor(name)
	if err == ErrBitmapNotFound {
		return false, nil
	} else if err != nil {
		return false, err
	}
	defer c.Close()

	return c.Contains(v)
}

// Depth returns the depth of the b-tree for a bitmap.
func (tx *Tx) Depth(name string) (int, error) {
	tx.mu.RLock()
	defer tx.mu.RUnlock()

	if tx.db == nil {
		return 0, ErrTxClosed
	} else if name == "" {
		return 0, ErrBitmapNameRequired
	}

	c, err := tx.cursor(name)
	if err == ErrBitmapNotFound {
		return 0, nil
	} else if err != nil {
		return 0, err
	}
	defer c.Close()

	if err := c.First(); err != nil {
		return 0, err
	}
	return c.stack.top + 1, nil
}

// Cursor returns an instance of a cursor this bitmap.
func (tx *Tx) Cursor(name string) (*Cursor, error) {
	tx.mu.RLock()
	defer tx.mu.RUnlock()
	return tx.cursor(name)
}

func (tx *Tx) cursor(name string) (*Cursor, error) {
	if tx.db == nil {
		return nil, ErrTxClosed
	} else if name == "" {
		return nil, ErrBitmapNameRequired
	}

	root, err := tx.root(name)
	if err != nil {
		return nil, err
	}

	c := tx.db.getCursor(tx)
	c.stack.top = 0
	c.stack.elems[0] = stackElem{pgno: root}
	return c, nil
}

// RoaringBitmap returns a bitmap as a Roaring bitmap.
func (tx *Tx) RoaringBitmap(name string) (*roaring.Bitmap, error) {
	tx.mu.RLock()
	defer tx.mu.RUnlock()

	if tx.db == nil {
		return nil, ErrTxClosed
	} else if name == "" {
		return nil, ErrBitmapNameRequired
	}

	c, err := tx.cursor(name)
	if err == ErrBitmapNotFound {
		return roaring.NewSliceBitmap(), nil
	} else if err != nil {
		return nil, err
	}
	defer c.Close()

	other := roaring.NewSliceBitmap()
	if err := c.First(); err == io.EOF {
		return other, nil
	} else if err != nil {
		return nil, err
	}

	for {
		if err := c.Next(); err == io.EOF {
			return other, nil
		} else if err != nil {
			return nil, err
		}

		elem := &c.stack.elems[c.stack.top]
		leafPage, _, err := c.tx.readPage(elem.pgno)
		if err != nil {
			return nil, err
		}
		cell := readLeafCell(leafPage, elem.index)
		other.Containers.Put(cell.Key, toContainer(cell, tx))
	}
}

// Container returns a Roaring container by key.
func (tx *Tx) Container(name string, key uint64) (*roaring.Container, error) {
	tx.mu.RLock()
	defer tx.mu.RUnlock()
	return tx.container(name, key)
}

func (tx *Tx) container(name string, key uint64) (*roaring.Container, error) {
	if tx.db == nil {
		return nil, ErrTxClosed
	} else if name == "" {
		return nil, ErrBitmapNameRequired
	}

	c, err := tx.cursor(name)
	if err == ErrBitmapNotFound {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	defer c.Close()

	if exact, err := c.Seek(key); err != nil || !exact {
		return nil, err
	}

	elem := &c.stack.elems[c.stack.top]
	leafPage, _, err := c.tx.readPage(elem.pgno)
	if err != nil {
		return nil, err
	}
	cell := readLeafCell(leafPage, elem.index)

	return toContainer(cell, tx), nil
}

// PutContainer inserts a container into a bitmap. Overwrites if key already exists.
func (tx *Tx) PutContainer(name string, key uint64, ct *roaring.Container) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	return tx.putContainer(name, key, ct)
}

func (tx *Tx) putContainer(name string, key uint64, ct *roaring.Container) error {
	if tx.DeleteEmptyContainer && ct.N() == 0 {
		return tx.removeContainer(name, key)
	}

	cell := ConvertToLeafArgs(key, ct)

	if err := tx.createBitmapIfNotExists(name); err != nil {
		return err
	}

	c, err := tx.cursor(name)
	if err != nil {
		return err
	}
	defer c.Close()

	if _, err := c.Seek(cell.Key); err != nil {
		return err
	}

	return c.putLeafCell(cell)
}

func (tx *Tx) putContainerWithCursor(cur *Cursor, key uint64, ct *roaring.Container) error {
	if tx.DeleteEmptyContainer && ct.N() == 0 {
		if exact, err := cur.Seek(key); err != nil || !exact {
			return err
		}
		return cur.deleteLeafCell(key)
	}
	return cur.putLeafCell(ConvertToLeafArgs(key, ct))
}

// RemoveContainer removes a container from the bitmap by key.
func (tx *Tx) RemoveContainer(name string, key uint64) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return tx.removeContainer(name, key)
}

func (tx *Tx) removeContainer(name string, key uint64) error {
	c, err := tx.cursor(name)
	if err == ErrBitmapNotFound {
		return nil
	} else if err != nil {
		return err
	}
	defer c.Close()

	if exact, err := c.Seek(key); err != nil || !exact {
		return err
	}
	return c.deleteLeafCell(key)
}

// Check verifies the integrity of the database.
func (tx *Tx) Check() error {
	tx.mu.RLock()
	defer tx.mu.RUnlock()

	if tx.db == nil {
		return ErrTxClosed
	}

	var errorList ErrorList
	if err := tx.checkPageAllocations(); err != nil {
		errorList.Append(err)
	}
	return errorList.Err()
}

func (tx *Tx) checkPage(pgno, parent, typ uint32) error {
	switch typ {
	case PageTypeBranch:
		return tx.checkBranchPage(pgno, parent, typ)
	default:
		return nil
	}
}

func (tx *Tx) checkBranchPage(pgno, parent, typ uint32) error {
	page, _, err := tx.readPage(pgno)
	if err != nil {
		return err
	}

	if readCellN(page) == 0 {
		return fmt.Errorf("branch page %d is empty", pgno)
	}
	return nil
}

// checkPageAllocations ensures that all pages are either in-use or on the freelist.
func (tx *Tx) checkPageAllocations() error {
	var errorList ErrorList
	freePageSet, err := tx.freePageSet()
	if err != nil {
		errorList.Append(err)
	}

	inusePageSet, err := tx.inusePageSet()
	if err != nil {
		errorList.Append(err)
	}

	// Iterate over all pages and ensure they are either in-use or free.
	// They should not be BOTH in-use or free or NEITHER in-use or free.
	pageN := readMetaPageN(tx.meta[:])
	for pgno := uint32(1); pgno < pageN; pgno++ {
		_, isInuse := inusePageSet[pgno]
		_, isFree := freePageSet[pgno]

		if isInuse && isFree {
			errorList.Append(fmt.Errorf("page in-use & free: pgno=%d", pgno))
			continue
		}

		if !isInuse && !isFree {
			errorList.Append(fmt.Errorf("page not in-use & not free: pgno=%d", pgno))
			continue
		}
	}

	return errorList.Err()
}

// freePageSet returns the set of pages in the freelist.
func (tx *Tx) freePageSet() (map[uint32]struct{}, error) {
	var errorList ErrorList

	m := make(map[uint32]struct{})
	c := Cursor{tx: tx}
	c.stack.elems[0] = stackElem{pgno: readMetaFreelistPageNo(tx.meta[:])}
	if err := c.First(); err == io.EOF {
		return m, nil
	} else if err != nil {
		return m, err
	}

	for {
		if err := c.Next(); err == io.EOF {
			return m, errorList.Err()
		} else if err != nil {
			errorList.Append(err)
			return m, errorList.Err()
		}

		elem := &c.stack.elems[c.stack.top]
		leafPage, _, err := c.tx.readPage(elem.pgno)
		if err != nil {
			errorList.Append(fmt.Errorf("cannot read free page: pgno=%d err=%w", elem.pgno, err))
			continue
		}

		cell := readLeafCell(leafPage, elem.index)
		for _, v := range cell.Values(tx) {
			pgno := uint32((cell.Key << 16) | uint64(v))
			m[pgno] = struct{}{}
		}
	}
}

// inusePageSet returns the set of pages in use by the root records or b-trees.
func (tx *Tx) inusePageSet() (map[uint32]struct{}, error) {
	var errorList ErrorList
	m := make(map[uint32]struct{})
	m[0] = struct{}{} // meta page

	// Traverse root record linked list and mark each page as in-use.
	for pgno := readMetaRootRecordPageNo(tx.meta[:]); pgno != 0; {
		m[pgno] = struct{}{}

		page, _, err := tx.readPage(pgno)
		if err != nil {
			errorList.Append(err)
			break
		}
		pgno = WalkRootRecordPages(page)
	}

	// Traverse freelist and mark pages as in-use.
	if err := tx.walkTree(readMetaFreelistPageNo(tx.meta[:]), 0, func(pgno, parent, typ uint32, err error) error {
		if err != nil {
			errorList.Append(err)
			return nil
		}

		m[pgno] = struct{}{}
		if err := tx.checkPage(pgno, parent, typ); err != nil {
			errorList.Append(err)
		}
		return nil
	}); err != nil {
		return m, err
	}

	// Traverse every b-tree and mark pages as in-use.
	records, err := tx.RootRecords()
	if err != nil {
		errorList.Append(err)
	} else {
		for itr := records.Iterator(); !itr.Done(); {
			_, pgno := itr.Next()

			if err := tx.walkTree(pgno.(uint32), 0, func(pgno, parent, typ uint32, err error) error {
				if err != nil {
					errorList.Append(err)
				}

				m[pgno] = struct{}{}
				if err := tx.checkPage(pgno, parent, typ); err != nil {
					errorList.Append(err)
				}
				return nil
			}); err != nil {
				return m, err
			}
		}
	}

	return m, errorList.Err()
}

// GetSizeBytesWithPrefix returns the size of bitmaps with a given key prefix.
func (tx *Tx) GetSizeBytesWithPrefix(prefix string) (n uint64, err error) {
	records, err := tx.RootRecords()
	if err != nil {
		return 0, err
	}

	// Loop over each bitmap in the database.
	for itr := records.Iterator(); !itr.Done(); {
		name, pgno := itr.Next()

		// Skip over any bitmaps that don't have a matching prefix.
		if !strings.HasPrefix(name.(string), prefix) {
			continue
		}

		// Traverse the bitmap's b-tree and count the bytes for each page.
		if err := tx.walkTree(pgno.(uint32), 0, func(pgno, parent, typ uint32, err error) error {
			n += PageSize
			return err
		}); err != nil {
			return 0, err
		}
	}
	return n, nil
}

// walkTree recursively iterates over a page and all its children.
func (tx *Tx) walkTree(pgno, parent uint32, fn func(pgno, parent, typ uint32, err error) error) error {
	// Read page and iterate over children.
	page, _, err := tx.readPage(pgno)
	if err != nil {
		return fn(pgno, parent, 0, fmt.Errorf("cannot read page: pgno=%d parent=%d err=%s", pgno, parent, err))
	}

	switch typ := readFlags(page); typ {
	case PageTypeBranch:
		if err := fn(pgno, parent, typ, nil); err != nil {
			return err
		}

		for i, n := 0, readCellN(page); i < n; i++ {
			cell := readBranchCell(page, i)
			if err := tx.walkTree(cell.ChildPgno, pgno, fn); err != nil {
				return err
			}
		}
		return nil

	case PageTypeLeaf:
		if err := fn(pgno, parent, typ, nil); err != nil {
			return err
		}

		// Execute callback only for bitmap pages pointed to by this leaf.
		for i, n := 0, readCellN(page); i < n; i++ {
			if cell := readLeafCell(page, i); cell.Type == ContainerTypeBitmapPtr {
				if err := fn(toPgno(cell.Data), pgno, PageTypeBitmap, nil); err != nil {
					return err
				}
			}
		}
		return nil

	default:
		return fn(pgno, parent, typ, fmt.Errorf("invalid page type: pgno=%d parent=%d type=%d", pgno, parent, typ))
	}
}

// freelistCleanup handles things which we need to add to the free list,
// because they became free during the process of modifying the free list.
// It also marks us as done modifying the free list. Expected usage is that
// you set modifyingFreelist to true, then defer this.
//
// if you are modifying the free list, we can't further change the free
// list during that modification. for page allocations, we can just skip
// the free list check. for deallocations, though, we do need to mark them
// as freed at some point. so, if we're modifying the free list when
// a new freePgno happens, we stash the new pages in here, then apply
// them afterwards. so far as i know, this can actually only happen
// during an allocate, when we're removing entries from the free list, and
// the add path doesn't ever trigger it. so, when we remove entries from
// the free list, it's possible that doing so frees up pages that were
// part of the free list, and we then add them. but we don't have to worry
// about that removing things from the free list, because the add logic
// already just uses new pages rather than trying to use the free list
// when it knows the free list is involved.
//
// Because this is expected to be used in a defer, instead of returning an
// error, it will set the error it got the address of to a new error if it
// encounters one and there wasn't one already.
func (tx *Tx) freelistCleanup(outErr *error) {
	defer func() {
		// no matter what, we're done with this after this, but we still
		// want it set *while* we do this so nothing we do will have side
		// effects that collide with what we're doing.
		tx.modifyingFreelist = false
	}()
	if len(tx.pendingFreelistAdds) == 0 {
		return
	}
	c := tx.db.getFreelistCursor(tx)
	for len(tx.pendingFreelistAdds) > 0 {
		var pass []uint32
		pass, tx.pendingFreelistAdds = tx.pendingFreelistAdds, nil
		for _, pgno := range pass {
			if changed, err := c.Add(uint64(pgno)); err != nil {
				if outErr != nil && *outErr == nil {
					*outErr = err
				}
				return
			} else if !changed {
				vprint.PanicOn(fmt.Sprintf("rbf.Tx.freelistCleanup(): double free: %d", pass))
			}
		}
	}
}

// allocatePgno returns a page number for a new available page. This page may be
// pulled from the free list or, if no free pages are available, it will be
// created by extending the file size.
//
// allocatePgno uses the freelist cursor (a shared db-wide thing), and sets
// the "modifyingFreelist" flag while it's running. If for some reason a
// modification to the freelist would require a new allocation or free,
// allocations always just create a new page, and frees are processed later
// by a separate call through a deferred tx.freelistCleanup().
func (tx *Tx) allocatePgno() (_ uint32, outErr error) {
	if tx.modifyingFreelist {
		return tx.allocateNewPgno(), nil
	}
	// this serves as a precaution against double-use of the freelist cursor
	// used database-wide. we don't have actual synchronization here because
	// only one write Tx should exist at once and it's not safe to use its
	// write-capable ops concurrently anyway.
	tx.modifyingFreelist = true
	defer tx.freelistCleanup(&outErr)
	c := tx.db.getFreelistCursor(tx)
	if err := c.First(); err == io.EOF {
		return tx.allocateNewPgno(), nil
	} else if err != nil {
		return 0, err
	}

	elem := &c.stack.elems[c.stack.top]
	leafPage, _, err := c.tx.readPage(elem.pgno)
	if err != nil {
		return 0, err
	}
	cell := readLeafCell(leafPage, elem.index)

	v := cell.firstValue(tx)

	pgno := uint32((cell.Key << 16) | uint64(v))

	if changed, err := c.Remove(uint64(pgno)); err != nil {
		return 0, err
	} else if !changed {
		vprint.PanicOn(fmt.Sprintf("tx.Tx.allocatePgno(): double alloc: %d", pgno))
	}
	return pgno, nil
}

// allocateNewPgno requests a new page unconditionally, ignoring the free list.
func (tx *Tx) allocateNewPgno() uint32 {
	// Increment the total page count by one and return the last page.
	pgno := readMetaPageN(tx.meta[:])
	writeMetaPageN(tx.meta[:], pgno+1)
	return pgno
}

// deallocate releases a page number to the freelist.
func (tx *Tx) freePgno(pgno uint32) (outErr error) {
	delete(tx.dirtyPages, pgno)
	delete(tx.dirtyBitmapPages, pgno)

	if tx.modifyingFreelist {
		tx.pendingFreelistAdds = append(tx.pendingFreelistAdds, pgno)
		return nil
	}
	c := tx.db.getFreelistCursor(tx)

	tx.modifyingFreelist = true
	defer tx.freelistCleanup(&outErr)
	if changed, err := c.Add(uint64(pgno)); err != nil {
		return err
	} else if !changed {
		vprint.PanicOn(fmt.Sprintf("rbf.Tx.freePgno(): double free: %d", pgno))
	}
	return nil
}

// deallocateTree recursively all pages in a btree.
func (tx *Tx) deallocateTree(pgno uint32) error {
	page, _, err := tx.readPage(pgno)
	if err != nil {
		return err
	}

	switch typ := readFlags(page); typ {
	case PageTypeBranch:
		for i, n := 0, readCellN(page); i < n; i++ {
			cell := readBranchCell(page, i)
			if err := tx.deallocateTree(cell.ChildPgno); err != nil {
				return err
			}
		}
		return tx.freePgno(pgno)

	case PageTypeLeaf:
		for i, n := 0, readCellN(page); i < n; i++ {
			if cell := readLeafCell(page, i); cell.Type == ContainerTypeBitmapPtr {
				if err := tx.freePgno(toPgno(cell.Data)); err != nil {
					return err
				}
			}
		}
		return tx.freePgno(pgno)
	default:
		return fmt.Errorf("rbf.Tx.deallocateTree(): invalid page type: pgno=%d type=%d", pgno, typ)
	}
}

func (tx *Tx) readPage(pgno uint32) (_ []byte, isHeap bool, err error) {
	// Meta page is always cached on the transaction.
	if pgno == 0 {
		return tx.meta[:], false, nil
	}

	// Verify page number requested is within current size of database.
	pageN := readMetaPageN(tx.meta[:])
	if pgno >= pageN {
		return nil, false, fmt.Errorf("rbf: page read out of bounds: pgno=%d max=%d", pgno, pageN-1)
	}

	// Check if page has been updated in this tx.
	if tx.writable {
		if page := tx.dirtyPages[pgno]; page != nil {
			return page, true, nil
		} else if page := tx.dirtyBitmapPages[pgno]; page != nil {
			return page, true, nil
		}
	}

	// Check if page is remapped in WAL.
	if walID, ok := tx.pageMap.Get(pgno); ok {
		buf, err := tx.db.readWALPageByID(walID)
		return buf, false, err
	}

	// Otherwise read directly from DB.
	buf, err := tx.db.readDBPage(pgno)
	return buf, false, err
}

func (tx *Tx) writePage(page []byte) error {
	tx.dirtyPages[readPageNo(page)] = page
	return tx.checkTxSize()
}

func (tx *Tx) writeBitmapPage(pgno uint32, page []byte) error {
	tx.dirtyBitmapPages[pgno] = page
	return tx.checkTxSize()
}

func (tx *Tx) checkTxSize() error {
	pageN := tx.walPageN + len(tx.dirtyPages) + (len(tx.dirtyBitmapPages) * 2)
	if pageN*PageSize >= len(tx.db.wal) {
		return ErrTxTooLarge
	}
	return nil
}

func (tx *Tx) AddRoaring(name string, bm *roaring.Bitmap) (changed bool, err error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if err := tx.createBitmapIfNotExists(name); err != nil {
		return false, err
	}

	c, err := tx.cursor(name)
	if err != nil {
		return false, err
	}
	defer c.Close()

	return c.AddRoaring(bm)
}

func (tx *Tx) leafCellBitmap(pgno uint32) (uint32, []uint64, error) {
	page, _, err := tx.readPage(pgno)
	if err != nil {
		return 0, nil, err
	}
	return pgno, toArray64(page), err
}

// leafCellBitmapInto copies the bitmap into provided space
func (tx *Tx) leafCellBitmapInto(pgno uint32, into []byte) (uint32, []uint64, error) {
	page, _, err := tx.readPage(pgno)
	if err != nil {
		return 0, nil, err
	}
	copy(into, page)
	return pgno, toArray64(into), err
}

func (tx *Tx) ContainerIterator(name string, key uint64) (citer roaring.ContainerIterator, found bool, err error) {
	tx.mu.RLock()
	defer tx.mu.RUnlock()

	c, err := tx.cursor(name)
	if err == ErrBitmapNotFound {
		return &emptyContainerIterator{}, false, nil // nothing available.
	} else if err != nil {
		return nil, false, err
	}

	exact, err := c.Seek(key)
	if err != nil {
		return nil, false, err
	}
	return &containerIterator{cursor: c}, exact, nil
}

// Shared pool for container filters, used because they contain cursors
// which are large.
var containerFilterPool = &sync.Pool{}

// getContainerFilter generates a containerFilter, which may actually secretly
// be used for rewriting; the data structures are similar enough that sharing
// a pool for both types seems advantageous.
func getContainerFilter(c *Cursor, name string, filter roaring.BitmapFilter, rewriter roaring.BitmapRewriter, tx *Tx) *containerFilter {
	existing := containerFilterPool.Get()
	if existing == nil {
		return &containerFilter{cursor: c, name: name, filter: filter, rewriter: rewriter, tx: tx}
	}
	f := existing.(*containerFilter)
	f.cursor = c
	f.name = name
	f.filter = filter
	f.rewriter = rewriter
	f.tx = tx
	return f
}

func (tx *Tx) ApplyRewriter(name string, key uint64, rewriter roaring.BitmapRewriter) (err error) {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	// Unlike a Filter, Rewriter makes sense to apply to an empty bitmap.
	if err = tx.createBitmapIfNotExists(name); err != nil {
		return err
	}

	c, err := tx.cursor(name)
	if err == ErrBitmapNotFound {
		return nil // nothing available.
	} else if err != nil {
		return err
	}

	_, err = c.Seek(key)
	if err != nil {
		return err
	}
	f := getContainerFilter(c, name, nil, rewriter, tx)
	defer f.Close()
	return f.ApplyRewriter()
}

func (tx *Tx) ApplyFilter(name string, key uint64, filter roaring.BitmapFilter) (err error) {
	tx.mu.RLock()
	defer tx.mu.RUnlock()

	c, err := tx.cursor(name)
	if err == ErrBitmapNotFound {
		return nil // nothing available.
	} else if err != nil {
		return err
	}

	_, err = c.Seek(key)
	if err != nil {
		return err
	}
	f := getContainerFilter(c, name, filter, nil, tx)
	defer f.Close()
	return f.ApplyFilter()
}

func (tx *Tx) ForEach(name string, fn func(i uint64) error) error {
	return tx.ForEachRange(name, 0, math.MaxUint64, fn)
}

func (tx *Tx) ForEachRange(name string, start, end uint64, fn func(uint64) error) error {
	tx.mu.RLock()
	defer tx.mu.RUnlock()

	c, err := tx.cursor(name)
	if err == ErrBitmapNotFound {
		return nil
	} else if err != nil {
		return err
	}
	defer c.Close()

	if _, err := c.Seek(highbits(start)); err != nil {
		return err
	}

	for {
		if err := c.Next(); err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}

		elem := &c.stack.elems[c.stack.top]
		leafPage, _, err := c.tx.readPage(elem.pgno)
		if err != nil {
			return err
		}
		cell := readLeafCell(leafPage, elem.index)

		switch cell.Type {
		case ContainerTypeArray:
			for _, lo := range toArray16(cell.Data) {
				v := cell.Key<<16 | uint64(lo)
				if v < start {
					continue
				} else if v > end {
					return nil
				} else if err := fn(v); err != nil {
					return err
				}
			}
		case ContainerTypeRLE:
			for _, r := range toInterval16(cell.Data) {
				for lo := int(r.Start); lo <= int(r.Last); lo++ {
					v := cell.Key<<16 | uint64(lo)
					if v < start {
						continue
					} else if v > end {
						return nil
					} else if err := fn(v); err != nil {
						return err
					}
				}
			}
		case ContainerTypeBitmap:
			for i, bits := range toArray64(cell.Data) {
				for j := uint(0); j < 64; j++ {
					if bits&(1<<j) == 0 {
						continue
					}

					v := cell.Key<<16 | (uint64(i) * 64) | uint64(j)
					if v < start {
						continue
					} else if v > end {
						return nil
					} else if err := fn(v); err != nil {
						return err
					}
				}
			}
		case ContainerTypeBitmapPtr:
			_, bm, err := c.tx.leafCellBitmap(toPgno(cell.Data))
			if err != nil {
				return err
			}
			for i, bits := range bm {
				for j := uint(0); j < 64; j++ {
					if bits&(1<<j) == 0 {
						continue
					}

					v := cell.Key<<16 | (uint64(i) * 64) | uint64(j)
					if v < start {
						continue
					} else if v > end {
						return nil
					} else if err := fn(v); err != nil {
						return err
					}
				}
			}
		default:
			vprint.PanicOn(fmt.Sprintf("invalid container type: %d", cell.Type))
		}
	}
}

func (tx *Tx) Count(name string) (uint64, error) {
	tx.mu.RLock()
	defer tx.mu.RUnlock()

	c, err := tx.cursor(name)
	if err == ErrBitmapNotFound {
		return 0, nil
	} else if err != nil {
		return 0, err
	}
	defer c.Close()

	if err := c.First(); err == io.EOF {
		return 0, nil
	} else if err != nil {
		return 0, err
	}

	var n uint64
	for {
		if err := c.Next(); err == io.EOF {
			break
		} else if err != nil {
			return 0, err
		}

		elem := &c.stack.elems[c.stack.top]
		leafPage, _, err := c.tx.readPage(elem.pgno)
		if err != nil {
			return 0, err
		}
		cell := readLeafCell(leafPage, elem.index)

		n += uint64(cell.BitN)
	}
	return n, nil
}

func (tx *Tx) Max(name string) (uint64, error) {
	tx.mu.RLock()
	defer tx.mu.RUnlock()

	c, err := tx.cursor(name)
	if err == ErrBitmapNotFound {
		return 0, nil
	} else if err != nil {
		return 0, err
	}
	defer c.Close()

	if err := c.Last(); err == io.EOF {
		return 0, nil
	} else if err != nil {
		return 0, err
	}

	elem := &c.stack.elems[c.stack.top]
	leafPage, _, err := c.tx.readPage(elem.pgno)
	if err != nil {
		return 0, err
	}
	cell := readLeafCell(leafPage, elem.index)

	return uint64((cell.Key << 16) | uint64(cell.lastValue(tx))), nil
}

func (tx *Tx) Min(name string) (uint64, bool, error) {
	tx.mu.RLock()
	defer tx.mu.RUnlock()

	c, err := tx.cursor(name)
	if err == ErrBitmapNotFound {
		return 0, false, nil
	} else if err != nil {
		return 0, false, err
	}
	defer c.Close()

	if err := c.First(); err == io.EOF {
		return 0, false, nil
	} else if err != nil {
		return 0, false, err
	}

	elem := &c.stack.elems[c.stack.top]
	leafPage, _, err := c.tx.readPage(elem.pgno)
	if err != nil {
		return 0, false, err
	}
	cell := readLeafCell(leafPage, elem.index)

	return uint64((cell.Key << 16) | uint64(cell.firstValue(tx))), true, nil
}

// roaring.countRange counts the number of bits set between [start, end).
func (tx *Tx) CountRange(name string, start, end uint64) (uint64, error) {
	tx.mu.RLock()
	defer tx.mu.RUnlock()

	if start >= end {
		return 0, nil
	}

	skey := highbits(start)
	ekey := highbits(end)
	ebits := int32(lowbits(end))

	csr, err := tx.cursor(name)
	if err == ErrBitmapNotFound {
		return 0, nil
	} else if err != nil {
		return 0, err
	}
	defer csr.Close()

	exact, err := csr.Seek(skey)
	_ = exact
	if err == io.EOF {
		return 0, nil
	} else if err != nil {
		return 0, err
	}
	var n uint64
	for {
		if err := csr.Next(); err == io.EOF {
			break
		} else if err != nil {
			return 0, err
		}

		elem := &csr.stack.elems[csr.stack.top]
		leafPage, _, err := csr.tx.readPage(elem.pgno)
		if err != nil {
			return 0, err
		}
		c := readLeafCell(leafPage, elem.index)

		k := c.Key
		if k > ekey {
			break
		}

		// If range is entirely in one container then just count that range.
		if skey == ekey {
			return uint64(c.countRange(tx, int32(lowbits(start)), ebits)), nil
		}
		// INVAR: skey < ekey

		// k > ekey handles the case when start > end and where start and end
		// are in different containers. Same container case is already handled above.
		if k > ekey {
			break
		}
		if k == skey {
			n += uint64(c.countRange(tx, int32(lowbits(start)), roaring.MaxContainerVal+1))
			continue
		}
		if k < ekey {
			n += uint64(c.BitN)
			continue
		}
		if k == ekey && ebits > 0 {
			n += uint64(c.countRange(tx, 0, ebits))
			break
		}
	}
	return n, nil
}

func (tx *Tx) OffsetRange(name string, offset, start, endx uint64) (*roaring.Bitmap, error) {
	if lowbits(offset) != 0 {
		vprint.PanicOn("offset must not contain low bits")
	} else if lowbits(start) != 0 {
		vprint.PanicOn("range start must not contain low bits")
	} else if lowbits(endx) != 0 {
		vprint.PanicOn("range endx must not contain low bits")
	}

	tx.mu.RLock()
	defer tx.mu.RUnlock()

	c, err := tx.cursor(name)
	if err == ErrBitmapNotFound {
		return roaring.NewSliceBitmap(), nil
	} else if err != nil {
		return nil, err
	}
	defer c.Close()

	other := roaring.NewSliceBitmap()
	off := highbits(offset)
	hi0, hi1 := highbits(start), highbits(endx)

	if _, err := c.Seek(hi0); err == io.EOF {
		return other, nil
	} else if err != nil {
		return nil, err
	}

	for {
		if err := c.Next(); err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}

		elem := &c.stack.elems[c.stack.top]
		leafPage, _, err := c.tx.readPage(elem.pgno)
		if err != nil {
			return nil, err
		}
		cell := readLeafCell(leafPage, elem.index)
		ckey := cell.Key

		// >= hi1 is correct b/c endx cannot have any lowbits set.
		if ckey >= hi1 {
			break
		}
		other.Containers.Put(off+(ckey-hi0), toContainer(cell, tx))
	}
	return other, nil
}

// containerFilter is like ContainerIterator, but implements ApplyFilter and
// also ApplyRewriter, depending on which is provided to it.
type containerFilter struct {
	cursor   *Cursor
	name     string
	filter   roaring.BitmapFilter
	rewriter roaring.BitmapRewriter
	tx       *Tx
	header   roaring.Container
	body     [8192]byte
}

func (s *containerFilter) Close() {
	s.cursor.Close()
	s.cursor = nil
	containerFilterPool.Put(s)
}

func (s *containerFilter) ApplyFilter() (err error) {
	var minKey roaring.FilterKey
	var cell leafCell
	if s.filter == nil {
		return errors.New("can't apply filter without a filter")
	}
	for err := s.cursor.Next(); err == nil; err = s.cursor.Next() {
		elem := &s.cursor.stack.elems[s.cursor.stack.top]
		leafPage, _, _ := s.cursor.tx.readPage(elem.pgno)
		readLeafCellInto(&cell, leafPage, elem.index)
		key := roaring.FilterKey(cell.Key)
		if key < minKey {
			continue
		}
		res := s.filter.ConsiderKey(key, int32(cell.BitN))
		if res.Err != nil {
			return res.Err
		}
		if res.YesKey <= key && res.NoKey <= key {
			data := intoContainer(cell, s.cursor.tx, &s.header, s.body[:])
			res = s.filter.ConsiderData(key, data)
			if res.Err != nil {
				return res.Err
			}
		}
		minKey = res.NoKey
		if minKey > key+1 {
			_, err := s.cursor.Seek(uint64(minKey))
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *containerFilter) ApplyRewriter() (err error) {
	var minKey roaring.FilterKey
	var cell leafCell
	if s.rewriter == nil {
		return errors.New("can't apply rewriter without a rewriter")
	}
	var dirty bool
	var key roaring.FilterKey
	var writeback roaring.ContainerWriteback = func(updateKey roaring.FilterKey, data *roaring.Container) (err error) {
		dirty = true
		var exact bool
		if updateKey != key {
			exact, err = s.cursor.Seek(uint64(updateKey))
			if err != nil {
				return err
			}
		} else {
			exact = true
		}
		if data.N() == 0 {
			if exact {
				err = s.cursor.deleteLeafCell(uint64(updateKey))
				key = ^roaring.FilterKey(0)
			}
			// if we don't delete, we aren't changing our situation at all
		} else {
			cell = ConvertToLeafArgs(uint64(updateKey), data)
			err = s.cursor.putLeafCell(cell)
			key = ^roaring.FilterKey(0)
		}
		return err
	}
	for err := s.cursor.Next(); err == nil; err = s.cursor.Next() {
		elem := &s.cursor.stack.elems[s.cursor.stack.top]
		leafPage, _, _ := s.cursor.tx.readPage(elem.pgno)
		readLeafCellInto(&cell, leafPage, elem.index)
		key = roaring.FilterKey(cell.Key)
		if key < minKey {
			continue
		}
		res := s.rewriter.ConsiderKey(key, int32(cell.BitN))
		if res.Err != nil {
			return res.Err
		}
		if res.YesKey <= key && res.NoKey <= key {
			data := intoWritableContainer(cell, s.cursor.tx, &s.header, s.body[:])
			res = s.rewriter.RewriteData(key, data, writeback)
			if res.Err != nil {
				return res.Err
			}
		}
		minKey = res.NoKey
		// if the callback did any writing, we need to reset our cursor,
		// and if the next key is far away, we should also reset our cursor.
		//
		// In practice the "key+64" probably comes out to "we've been told
		// we're done".
		if dirty || minKey > (key+64) {
			dirty = false
			_, err := s.cursor.Seek(uint64(minKey))
			if err != nil {
				return err
			}
		}
	}
	// notify rewriter that we're done, telling them the last key we
	// processed.
	res := s.rewriter.RewriteData(^roaring.FilterKey(0), nil, writeback)
	return res.Err
}

// containerIterator wraps Cursor to implement roaring.ContainerIterator.
type containerIterator struct {
	cursor *Cursor
}

// Close must be called when the client is done
// with the containerIterator so that the internal
// Cursor can be recycled.
func (itr *containerIterator) Close() {
	itr.cursor.Close()
}

// Next moves the iterator to the next container.
func (itr *containerIterator) Next() bool {
	err := itr.cursor.Next()
	return err == nil
}

// Value returns the current key & container.
func (itr *containerIterator) Value() (uint64, *roaring.Container) {
	elem := &itr.cursor.stack.elems[itr.cursor.stack.top]
	leafPage, _, _ := itr.cursor.tx.readPage(elem.pgno)
	cell := readLeafCell(leafPage, elem.index)
	return cell.Key, toContainer(cell, itr.cursor.tx)
}

// always returns false for Next()
type emptyContainerIterator struct{}

func (si *emptyContainerIterator) Close() {}

func (si *emptyContainerIterator) Next() bool {
	return false
}
func (si *emptyContainerIterator) Value() (uint64, *roaring.Container) {
	vprint.PanicOn("emptyContainerIterator never has any Values")
	return 0, nil
}

func (tx *Tx) ImportRoaringBits(name string, itr roaring.RoaringIterator, clear bool, log bool, rowSize uint64) (changed int, rowSet map[uint64]int, err error) {

	// begin write boilerplate
	if tx.db == nil {
		err = ErrTxClosed
		return
	} else if !tx.writable {
		err = ErrTxNotWritable
		return
	} else if name == "" {
		err = ErrBitmapNameRequired
		return
	}

	tx.mu.Lock()
	defer tx.mu.Unlock()

	if err = tx.createBitmapIfNotExists(name); err != nil {
		return
	}
	// end write boilerplate

	n := itr.Len()
	if n == 0 {
		return
	}
	rowSet = make(map[uint64]int)

	var currRow uint64

	cur, err := tx.cursor(name)
	if err != nil {
		return changed, rowSet, err
	}
	defer cur.Close()

	for itrKey, synthC := itr.NextContainer(); synthC != nil; itrKey, synthC = itr.NextContainer() {

		if rowSize != 0 {
			currRow = itrKey / rowSize
		}
		nsynth := int(synthC.N())
		if nsynth == 0 {
			continue
		}
		// INVAR: nsynth > 0

		// Find existing container, if any.
		var oldC *roaring.Container
		if exact, err := cur.Seek(itrKey); err != nil {
			return changed, rowSet, err
		} else if exact {
			elem := &cur.stack.elems[cur.stack.top]
			leafPage, _, err := cur.tx.readPage(elem.pgno)
			if err != nil {
				return changed, rowSet, err
			}
			cell := readLeafCell(leafPage, elem.index)
			oldC = toContainer(cell, tx)
		}

		if oldC == nil || oldC.N() == 0 {
			// no container at the itrKey in badger (or all zero container).
			if clear {
				// changed of 0 and empty rowSet is perfect, no need to change the defaults.
				continue
			} else {
				changed += nsynth
				rowSet[currRow] += nsynth

				if err := tx.putContainerWithCursor(cur, itrKey, synthC); err != nil {
					return changed, rowSet, err
				}
				continue
			}
		}

		if clear {

			existN := oldC.N() // number of bits set in the old container
			newC := oldC.Difference(synthC)

			// update rowSet and changes
			if newC.N() == existN {
				// INVAR: do changed need adjusting? nope. same bit count,
				// so no change could have happened.
				continue
			} else {
				changes := int(existN - newC.N())
				changed += changes
				rowSet[currRow] -= changes
				err = tx.putContainerWithCursor(cur, itrKey, newC)
				if err != nil {
					return
				}
				continue
			}
		} else {
			// setting bits

			existN := oldC.N()
			if existN == roaring.MaxContainerVal+1 {
				// completely full container already, set will do nothing. so changed of 0 default is perfect.
				continue
			}
			if existN == 0 {
				// can nsynth be zero? No, because of the continue/invariant above where nsynth > 0
				changed += nsynth
				rowSet[currRow] += nsynth
				err = tx.putContainerWithCursor(cur, itrKey, synthC)
				if err != nil {
					return
				}
				continue
			}

			newC := roaring.Union(oldC, synthC) // UnionInPlace was giving us crashes on overly large containers.

			if roaring.ContainerType(newC) == roaring.ContainerBitmap {
				newC.Repair() // update the bit-count so .n is valid. b/c UnionInPlace doesn't update it.
			}
			if newC.N() != existN {
				changes := int(newC.N() - existN)
				changed += changes
				rowSet[currRow] += changes

				err = tx.putContainerWithCursor(cur, itrKey, newC)
				if err != nil {
					vprint.PanicOn(err)
					return
				}
				continue
			}
		}
	}
	return
}

// flush writes the dirty pages & meta page to the WAL.
func (tx *Tx) flush() error {
	w := bufio.NewWriterSize(tx.db.walFile, 65536)

	// Write non-bitmap pages to WAL.
	for _, pgno := range dirtyPageMapKeys(tx.dirtyPages) {
		walID, err := tx.writeToWAL(w, tx.dirtyPages[pgno])
		if err != nil {
			return fmt.Errorf("write page to wal: %w", err)
		}
		tx.pageMap = tx.pageMap.Set(pgno, walID)
	}

	// Write bitmap headers & pages to WAL.
	//
	// We need to write a bitmap header before each such page. We only allocate
	// one header, and we reuse it, because each write is flushing it out to
	// disk, and it doesn't get stored in-memory.
	var hdr []byte
	if len(tx.dirtyBitmapPages) > 0 {
		hdr = allocPage()
	}
	for _, pgno := range dirtyPageMapKeys(tx.dirtyBitmapPages) {
		// Write header page.
		writePageNo(hdr[:], pgno)
		writeFlags(hdr[:], PageTypeBitmapHeader)
		if _, err := tx.writeToWAL(w, hdr); err != nil {
			return fmt.Errorf("write bitmap header page to wal: %w", err)
		}

		// Write bitmap page.
		walID, err := tx.writeToWAL(w, tx.dirtyBitmapPages[pgno])
		if err != nil {
			return fmt.Errorf("write bitmap page to wal: %w", err)
		}
		tx.pageMap = tx.pageMap.Set(pgno, walID)
	}

	// Write meta page to WAL.
	walID, err := tx.writeToWAL(w, tx.meta[:])
	if err != nil {
		return fmt.Errorf("write meta page to wal: %w", err)
	}
	tx.pageMap = tx.pageMap.Set(uint32(0), walID)

	// Flush & sync WAL.
	if err := w.Flush(); err != nil {
		return fmt.Errorf("flush wal: %w", err)
	} else if err := tx.db.fsyncWAL(tx.db.walFile); err != nil {
		return fmt.Errorf("sync wal: %w", err)
	}

	return nil
}

func (tx *Tx) writeToWAL(w io.Writer, page []byte) (walID int64, err error) {
	// Determine next WAL ID from cached meta page.
	walID = readMetaWALID(tx.meta[:]) + 1

	// Update WAL ID on cached meta page.
	writeMetaWALID(tx.meta[:], walID)

	// Append to WAL and increment WAL size.
	if _, err := w.Write(page); err != nil {
		return 0, err
	}
	tx.walPageN++

	return walID, nil
}

// Pages returns meta & record data for a list of pages.
func (tx *Tx) Pages(pgnos []uint32) ([]Page, error) {
	// Read page info for all pages in the database.
	infos, err := tx.PageInfos()
	if err != nil {
		return nil, err
	}

	// Loop over each requested page number and extract additional data.
	var pages []Page
	for _, pgno := range pgnos {
		buf, _, err := tx.readPage(pgno)
		if err != nil {
			return nil, err
		}

		switch info := infos[pgno].(type) {
		case *MetaPageInfo:
			pages = append(pages, &MetaPage{MetaPageInfo: info})

		case *RootRecordPageInfo:
			records, err := readRootRecords(buf)
			if err != nil {
				return nil, err
			}
			pages = append(pages, &RootRecordPage{RootRecordPageInfo: info, Records: records})

		case *LeafPageInfo:
			page := &LeafPage{LeafPageInfo: info}
			cells := make([]leafCell, page.CellN)
			for _, cell := range readLeafCells(buf, cells) {
				other := &LeafCell{
					Key:  cell.Key,
					Type: cell.Type,
				}

				switch cell.Type {
				case ContainerTypeArray, ContainerTypeRLE:
					other.Values = cell.Values(tx)
				case ContainerTypeBitmapPtr:
					other.Pgno = toPgno(cell.Data)
				}

				page.Cells = append(page.Cells, other)
			}
			pages = append(pages, page)

		case *BranchPageInfo:
			page := &BranchPage{BranchPageInfo: info}
			for _, cell := range readBranchCells(buf) {
				page.Cells = append(page.Cells, &BranchCell{
					Key:   cell.LeftKey,
					Flags: cell.Flags,
					Pgno:  cell.ChildPgno,
				})
			}
			pages = append(pages, page)

		case *BitmapPageInfo:
			pages = append(pages, &BitmapPage{
				BitmapPageInfo: info,
				Values:         bitmapValues(toArray64(buf)),
			})

		case *FreePageInfo:
			pages = append(pages, &FreePage{FreePageInfo: info})

		default:
			vprint.PanicOn(fmt.Sprintf("invalid page info type %T", info))
		}
	}

	return pages, nil
}

// PageInfos returns meta data about all pages in the database.
func (tx *Tx) PageInfos() ([]PageInfo, error) {
	var errorList ErrorList

	infos := make([]PageInfo, tx.PageN())

	// Read meta page info.
	metaInfo, err := tx.metaPageInfo()
	if err != nil {
		return nil, err
	}
	infos[0] = metaInfo

	// Traverse root record linked list.
	for pgno := metaInfo.RootRecordPageNo; pgno != 0; {
		info, err := tx.rootRecordPageInfo(pgno)
		if err != nil {
			errorList.Append(err)
			break
		}
		infos[pgno] = info
		pgno = info.Next
	}

	// Traverse freelist and mark pages as in-use.
	if err := tx.walkPageInfo(infos, metaInfo.FreelistPageNo, "freelist"); err != nil {
		errorList.Append(err)
	}

	// Traverse every b-tree and mark pages as in-use.
	records, err := tx.RootRecords()
	if err != nil {
		errorList.Append(err)
	} else {
		for itr := records.Iterator(); !itr.Done(); {
			name, pgno := itr.Next()

			if err := tx.walkPageInfo(infos, pgno.(uint32), name.(string)); err != nil {
				errorList.Append(err)
			}
		}
	}

	// Build page info objects for each free page.
	freePageSet, err := tx.freePageSet()
	if err != nil {
		errorList.Append(err)
	} else {
		for pgno := range freePageSet {
			infos[pgno] = &FreePageInfo{Pgno: pgno}
		}
	}

	return infos, errorList.Err()
}

// metaPageInfo returns page metadata for the meta page.
func (tx *Tx) metaPageInfo() (*MetaPageInfo, error) {
	buf, _, err := tx.readPage(0)
	if err != nil {
		return nil, err
	}

	return &MetaPageInfo{
		Pgno:             0,
		Magic:            readMetaMagic(buf),
		PageN:            readMetaPageN(buf),
		WALID:            readMetaWALID(buf),
		RootRecordPageNo: readMetaRootRecordPageNo(buf),
		FreelistPageNo:   readMetaFreelistPageNo(buf),
	}, nil
}

// rootRecordPageInfo returns page metadata for a root record page.
func (tx *Tx) rootRecordPageInfo(pgno uint32) (*RootRecordPageInfo, error) {
	buf, _, err := tx.readPage(pgno)
	if err != nil {
		return nil, err
	}

	return &RootRecordPageInfo{
		Pgno: pgno,
		Next: WalkRootRecordPages(buf),
	}, nil
}

func (tx *Tx) walkPageInfo(infos []PageInfo, root uint32, name string) error {
	var errorList ErrorList

	if err := tx.walkTree(root, 0, func(pgno, parent, typ uint32, err error) error {
		if err != nil {
			errorList.Append(err)
			return nil
		}

		buf, _, err := tx.readPage(pgno)
		if err != nil {
			errorList.Append(fmt.Errorf("cannot read page: pgno=%d parent=%d typ=%d err=%d", pgno, parent, typ, err))
			return nil
		}

		switch typ {
		case PageTypeLeaf:
			infos[pgno] = &LeafPageInfo{
				Pgno:   pgno,
				Parent: parent,
				Tree:   name,
				Flags:  readFlags(buf),
				CellN:  readCellN(buf),
			}
		case PageTypeBranch:
			infos[pgno] = &BranchPageInfo{
				Pgno:   pgno,
				Parent: parent,
				Tree:   name,
				Flags:  readFlags(buf),
				CellN:  readCellN(buf),
			}
		case PageTypeBitmap:
			infos[pgno] = &BitmapPageInfo{
				Pgno:   pgno,
				Parent: parent,
				Tree:   name,
			}
		}

		return nil
	}); err != nil {
		errorList.Append(err)
	}

	return errorList.Err()
}

// PageData returns the raw page data for a single page.
func (tx *Tx) PageData(pgno uint32) ([]byte, error) {
	buf, _, err := tx.readPage(pgno)
	return buf, err
}

func (tx *Tx) GetSortedFieldViewList() (fvs []txkey.FieldView, _ error) {
	records, err := tx.RootRecords()
	if err != nil {
		return nil, err
	}
	it := records.Iterator()
	for !it.Done() {
		k, _ := it.Next()
		root := k.(string)
		fv := txkey.FieldViewFromPrefix([]byte(root))
		fvs = append(fvs, fv)
	}
	return
}

func (tx *Tx) DebugInfo() *TxDebugInfo {
	return &TxDebugInfo{
		Ptr:      fmt.Sprintf("%p", tx),
		Writable: tx.writable,
		Stack:    string(tx.stack),
	}
}

type TxDebugInfo struct {
	Ptr      string `json:"ptr"`
	Writable bool   `json:"writable"`
	Stack    string `json:"stack,omitempty"`
}

// SnapshotReader returns a reader that provides a snapshot for the current database state.
func (tx *Tx) SnapshotReader() (io.Reader, error) {
	if tx.db == nil {
		return nil, ErrTxClosed
	}
	return &snapshotReader{tx: tx}, nil
}

type snapshotReader struct {
	tx   *Tx
	pgno uint32
}

func (r *snapshotReader) Read(p []byte) (n int, err error) {
	// Exit if we are past the end of the database.
	if r.pgno >= readMetaPageN(r.tx.meta[:]) {
		return 0, io.EOF
	}

	// Otherwise look up the page data from mmap or page cache and copy it out.
	buf, _, err := r.tx.readPage(r.pgno)
	if err != nil {
		return 0, err
	} else if len(p) < len(buf) {
		return 0, io.ErrShortBuffer
	}
	copy(p, buf)

	// Increment the page number.
	r.pgno++

	return len(buf), nil
}

type PageInfo interface {
	pageInfo()
}

func (*MetaPageInfo) pageInfo()       {}
func (*RootRecordPageInfo) pageInfo() {}
func (*LeafPageInfo) pageInfo()       {}
func (*BranchPageInfo) pageInfo()     {}
func (*BitmapPageInfo) pageInfo()     {}
func (*FreePageInfo) pageInfo()       {}

type MetaPageInfo struct {
	Pgno             uint32
	Magic            []byte
	PageN            uint32
	WALID            int64
	RootRecordPageNo uint32
	FreelistPageNo   uint32
}

type RootRecordPageInfo struct {
	Pgno uint32
	Next uint32
}

type LeafPageInfo struct {
	Pgno   uint32
	Parent uint32
	Tree   string
	Flags  uint32
	CellN  int
}

type BranchPageInfo struct {
	Pgno   uint32
	Parent uint32
	Tree   string
	Flags  uint32
	CellN  int
}

type BitmapPageInfo struct {
	Pgno   uint32
	Parent uint32
	Tree   string
}

type FreePageInfo struct {
	Pgno uint32
}

type Page interface {
	page()
}

func (*MetaPage) page()       {}
func (*RootRecordPage) page() {}
func (*LeafPage) page()       {}
func (*BranchPage) page()     {}
func (*BitmapPage) page()     {}
func (*FreePage) page()       {}

type MetaPage struct {
	*MetaPageInfo
}

type RootRecordPage struct {
	*RootRecordPageInfo
	Records []*RootRecord
}

type LeafPage struct {
	*LeafPageInfo
	Cells []*LeafCell
}

// LeafCell represents a leaf cell in the public API.
type LeafCell struct {
	Key    uint64
	Type   ContainerType
	Pgno   uint32   // bitmap pointer only
	Values []uint16 // array & rle containers only
}

type BranchPage struct {
	*BranchPageInfo
	Cells []*BranchCell
}

// BranchCell represents a branch cell in the public API.
type BranchCell struct {
	Key   uint64
	Flags uint32
	Pgno  uint32
}

type BitmapPage struct {
	*BitmapPageInfo
	Values []uint16
}

type FreePage struct {
	*FreePageInfo
}

// dirtyPageMapKeys returns a sorted slice slice of keys for a dirty page map.
func dirtyPageMapKeys(m map[uint32][]byte) []uint32 {
	a := make([]uint32, 0, len(m))
	for k := range m {
		a = append(a, k)
	}
	sort.Sort(uint32Slice(a))
	return a
}

type uint32Slice []uint32

func (p uint32Slice) Len() int           { return len(p) }
func (p uint32Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p uint32Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
