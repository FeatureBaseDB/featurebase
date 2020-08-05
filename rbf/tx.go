// Copyright 2017 Pilosa Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package rbf

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/benbjohnson/immutable"
	"github.com/pilosa/pilosa/v2/roaring"
)

// Tx represents a transaction.
type Tx struct {
	mu       sync.RWMutex
	db       *DB            // parent db
	meta     [PageSize]byte // copy of current meta page
	walID    int64          // max WAL ID at start of tx
	pageMap  *immutable.Map // mapping of database pages to WAL IDs
	writable bool           // if true, tx can write
	dirty    bool           // if true, changes have been made

	// If Rollback() has already completed, don't do it again.
	// Note db == nil means that commit has already been done.
	rollbackDone bool

	// DeleteEmptyContainer lets us by default match the roaring
	// behavior where an existing container has all its bits cleared
	// but still sticks around in the database.
	DeleteEmptyContainer bool
}

// Writable returns true if the transaction can mutate data.
func (tx *Tx) Writable() bool {
	return tx.writable
}

// Commit completes the transaction and persists data changes.
func (tx *Tx) Commit() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.db == nil {
		return ErrTxClosed
	}

	// If any pages have been written, ensure we write a new meta page with
	// the commit flag to mark the end of the transaction.
	if tx.dirty {
		if err := tx.writeMetaPage(MetaPageFlagCommit); err != nil {
			return err
		} else if err := tx.db.SyncWAL(); err != nil {
			return err
		}
		tx.db.pageMap = tx.pageMap
	}

	if err := tx.db.checkpoint(); err != nil {
		return err
	}

	// Disconnect transaction from DB.
	return tx.db.removeTx(tx)
}

func (tx *Tx) Rollback() {
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

	// TODO(bbj): Invalidate DB if rollback fails. Possibly attempt reopen?

	// If any pages have been written, ensure we write a new meta page with
	// the rollback flag to mark the end of the transaction. This allows us to
	// discard pages in the transaction during playback of the WAL on open.
	if tx.dirty {
		if err := tx.writeMetaPage(MetaPageFlagRollback); err != nil {
			panic(err)
		} else if err := tx.db.SyncWAL(); err != nil {
			panic(err)
		}
	}

	// turn on these error checks! we see
	// panic: cannot find segment containing WAL page: 1
	// when running go test -v
	// TestCursor_FirstNext_Quick/6
	//
	//panicOn(tx.db.checkpoint())
	//panicOn(tx.db.removeTx(tx))

	_ = tx.db.checkpoint()

	// Disconnect transaction from DB.
	_ = tx.db.removeTx(tx)
}

// Root returns the root page number for a bitmap. Returns 0 if the bitmap does not exist.
func (tx *Tx) Root(name string) (uint32, error) {
	tx.mu.RLock()
	defer tx.mu.RUnlock()
	return tx.root(name)
}

func (tx *Tx) root(name string) (uint32, error) {
	records, err := tx.rootRecords()
	if err != nil {
		return 0, err
	}

	i := sort.Search(len(records), func(i int) bool { return records[i].Name >= name })
	if i >= len(records) || records[i].Name != name {
		return 0, ErrBitmapNotFound
	}
	return records[i].Pgno, nil
}

// BitmapNames returns a list of all bitmap names.
func (tx *Tx) BitmapNames() ([]string, error) {
	tx.mu.RLock()
	defer tx.mu.RUnlock()

	if tx.db == nil {
		return nil, ErrTxClosed
	}

	// Read list of root records.
	records, err := tx.rootRecords()
	if err != nil {
		return nil, err
	}

	// Convert to a list of strings.
	names := make([]string, len(records))
	for i := range records {
		names[i] = records[i].Name
	}
	return names, nil
}

// CreateBitmap creates a new empty bitmap with the given name.
// Returns an error if the bitmap already exists.
func (tx *Tx) CreateBitmap(name string) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	return tx.createBitmap(name)
}

func (tx *Tx) createBitmap(name string) error {
	//vv("createBitmap(name='%v'", name)

	if tx.db == nil {
		return ErrTxClosed
	} else if !tx.writable {
		return ErrTxNotWritable
	} else if name == "" {
		return ErrBitmapNameRequired
	}

	// Read list of root records.
	records, err := tx.rootRecords()
	if err != nil {
		return err
	}

	// Find btree by name. Exit if already exists.
	index := sort.Search(len(records), func(i int) bool { return records[i].Name >= name })
	if index < len(records) && records[index].Name == name {
		return ErrBitmapExists
	}
	//fmt.Println("CREATE BITMAP", name, index)

	// Allocate new root page.
	pgno, err := tx.allocate()
	//fmt.Println("CREATE BITMAP @ PGNO", pgno)
	if err != nil {
		return err
	}

	// Write root page.
	page := make([]byte, PageSize)
	writePageNo(page, pgno)
	writeFlags(page, PageTypeLeaf)
	writeCellN(page, 0)
	if err := tx.writePage(page); err != nil {
		return err
	}

	// Insert into correct index.
	records = append(records, nil)
	copy(records[index+1:], records[index:])
	records[index] = &RootRecord{Name: name, Pgno: pgno}
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

/*
func dump(r []*RootRecord) {
	for _, i := range r {
		fmt.Println("RECORD", i.Name, i.Pgno)
	}

}
*/

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
	records, err := tx.rootRecords()
	if err != nil {
		return err
	}

	// Find btree by name. Exit if it doesn't exist.
	index := sort.Search(len(records), func(i int) bool { return records[i].Name >= name })
	if index >= len(records) || records[index].Name != name {
		return fmt.Errorf("bitmap does not exist: %q", name)
	}
	pgno := records[index].Pgno

	// Deallocate all pages in the tree.
	if err := tx.deallocateTree(pgno); err != nil {
		return err
	}

	// Delete from record list & rewrite record pages.
	records = append(records[:index], records[index+1:]...)
	if err := tx.writeRootRecordPages(records); err != nil {
		return fmt.Errorf("write bitmaps: %w", err)
	}

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
	records, err := tx.rootRecords()
	if err != nil {
		return err
	}

	for i := 0; i < len(records); i++ {
		record := records[i]

		// Skip bitmaps without matching prefix.
		if !strings.HasPrefix(record.Name, prefix) {
			continue
		}

		// Deallocate all pages in the tree.
		if err := tx.deallocateTree(record.Pgno); err != nil {
			return err
		}

		// Delete from record list.
		records = append(records[:i], records[i+1:]...)
		i--
	}

	// Rewrite record pages.
	if err := tx.writeRootRecordPages(records); err != nil {
		return fmt.Errorf("write bitmaps: %w", err)
	}

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
	records, err := tx.rootRecords()
	if err != nil {
		return err
	}

	// Find btree by name. Exit if it doesn't exist.
	index := sort.Search(len(records), func(i int) bool { return records[i].Name >= oldname })
	if index >= len(records) || records[index].Name != oldname {
		return fmt.Errorf("bitmap does not exist: %q", oldname)
	}

	// Update record name & rewrite record pages.
	records[index].Name = newname
	if err := tx.writeRootRecordPages(records); err != nil {
		return fmt.Errorf("write bitmaps: %w", err)
	}

	return nil
}

// rootRecords returns a list of root records.
func (tx *Tx) rootRecords() ([]*RootRecord, error) {
	var records []*RootRecord
	for pgno := readMetaRootRecordPageNo(tx.meta[:]); pgno != 0; {
		page, err := tx.readPage(pgno)
		if err != nil {
			return nil, err
		}

		// Read all records on the page.
		a, err := readRootRecords(page)
		if err != nil {
			return nil, err
		}
		records = append(records, a...)

		// Read next overflow page number.
		pgno = WalkRootRecordPages(page)
	}
	return records, nil
}

// writeRootRecordPages writes a list of root record pages.
func (tx *Tx) writeRootRecordPages(records []*RootRecord) (err error) {
	// Release all existing root record pages.
	for pgno := readMetaRootRecordPageNo(tx.meta[:]); pgno != 0; {
		page, err := tx.readPage(pgno)
		if err != nil {
			return err
		}

		err = tx.deallocate(pgno)
		if err != nil {
			return err
		}
		pgno = WalkRootRecordPages(page)
	}

	// Exit early if no records exist.
	if len(records) == 0 {
		writeMetaRootRecordPageNo(tx.meta[:], 0)
		return nil
	}

	// Ensure records are in sorted order.
	sort.Slice(records, func(i, j int) bool { return records[i].Name < records[j].Name })

	// Allocate initial root record page.
	pgno, err := tx.allocate()
	if err != nil {
		return err
	}
	writeMetaRootRecordPageNo(tx.meta[:], pgno)

	// Write new root record pages.
	for i := 0; len(records) != 0; i++ {
		// Initialize page & write as many records as will fit.
		page := make([]byte, PageSize)
		writePageNo(page, pgno)
		writeFlags(page, PageTypeRootRecord)
		if records, err = writeRootRecords(page, records); err != nil {
			return err
		}

		// Allocate next and write overflow if we have remaining records.
		if len(records) != 0 {
			if pgno, err = tx.allocate(); err != nil {
				return err
			}
			writeRootRecordOverflowPgno(page, pgno)
		}

		// Write page to disk.
		if err := tx.writePage(page); err != nil {
			return err
		}
	}

	return nil
}

// Add sets a given bit on the bitmap.
func (tx *Tx) Add(name string, a ...uint64) (changeCount int, err error) {
	//vv("rbf Tx.Add(a='%#v')", a)

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
	if err != nil {
		return 0, err
	} else if c == nil {
		return 0, nil
	}
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
	if err != nil {
		return false, err
	} else if c == nil {
		return false, nil
	}
	return c.Contains(v)
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
	if err == ErrBitmapNotFound {
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	c := Cursor{tx: tx}
	c.stack.elems[0] = stackElem{pgno: root}
	return &c, nil
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
	if err != nil {
		return nil, err
	} else if c == nil {
		return roaring.NewSliceBitmap(), nil
	}

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

		cell := c.cell()
		other.Containers.Put(cell.Key, toContainer(cell, tx))
	}
}

// Container returns a Roaring container by key.
func (tx *Tx) Container(name string, key uint64) (*roaring.Container, error) {
	tx.mu.RLock()
	defer tx.mu.RUnlock()

	if tx.db == nil {
		return nil, ErrTxClosed
	} else if name == "" {
		return nil, ErrBitmapNameRequired
	}

	c, err := tx.cursor(name)
	if err != nil {
		return nil, err
	} else if c == nil {
		return nil, err
	} else if exact, err := c.Seek(key); err != nil || !exact {
		return nil, err
	}
	return toContainer(c.cell(), tx), nil
}

// PutContainer inserts a container into a bitmap. Overwrites if key already exists.
func (tx *Tx) PutContainer(name string, key uint64, ct *roaring.Container) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if ct.N() == 0 {
		return nil
	}
	cell := ConvertToLeafArgs(key, ct)

	if err := tx.createBitmapIfNotExists(name); err != nil {
		return err
	}

	c, err := tx.cursor(name)
	if err != nil {
		return err
	} else if _, err := c.Seek(cell.Key); err != nil {
		return err
	}
	return c.putLeafCell(cell)
}

// RemoveContainer removes a container from the bitmap by key.
func (tx *Tx) RemoveContainer(name string, key uint64) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	c, err := tx.cursor(name)
	if err != nil {
		return err
	} else if c == nil {
		return nil
	} else if exact, err := c.Seek(key); err != nil || !exact {
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

	if err := tx.checkPageAllocations(); err != nil {
		return fmt.Errorf("page allocations: %w", err)
	}
	return nil
}

// checkPageAllocations ensures that all pages are either in-use or on the freelist.
func (tx *Tx) checkPageAllocations() error {
	freePageSet, err := tx.freePageSet()
	if err != nil {
		return err
	}

	inusePageSet, err := tx.inusePageSet()
	if err != nil {
		return err
	}

	// Iterate over all pages and ensure they are either in-use or free.
	// They should not be BOTH in-use or free or NEITHER in-use or free.
	pageN := readMetaPageN(tx.meta[:])
	for pgno := uint32(1); pgno < pageN; pgno++ {
		_, isInuse := inusePageSet[pgno]
		_, isFree := freePageSet[pgno]

		if isInuse && isFree {
			return fmt.Errorf("page in-use & free: pgno=%d", pgno)
		} else if !isInuse && !isFree {
			page, _ := tx.readPage(pgno)
			flags := readFlags(page)
			if flags == PageTypeBranch || flags == PageTypeLeaf {

				return fmt.Errorf("page not in-use & not free: pgno=%d", pgno)
			}
			//assuming its a bitmap so its ok TODO ben?
			return nil
		}
	}

	return nil
}

// freePageSet returns the set of pages in the freelist.
func (tx *Tx) freePageSet() (map[uint32]struct{}, error) {
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
			return m, nil
		} else if err != nil {
			return m, err
		}

		cell := c.cell()
		for _, v := range cell.Values(tx) {
			pgno := uint32((cell.Key << 16) & uint64(v))
			m[pgno] = struct{}{}
		}
	}
}

// inusePageSet returns the set of pages in use by the root records or b-trees.
func (tx *Tx) inusePageSet() (map[uint32]struct{}, error) {
	m := make(map[uint32]struct{})
	m[0] = struct{}{} // meta page

	// Traverse root record linked list and mark each page as in-use.
	for pgno := readMetaRootRecordPageNo(tx.meta[:]); pgno != 0; {
		m[pgno] = struct{}{}

		page, err := tx.readPage(pgno)
		if err != nil {
			return nil, err
		}
		pgno = WalkRootRecordPages(page)
	}

	// Traverse freelist and mark pages as in-use.
	if err := tx.walkTree(readMetaFreelistPageNo(tx.meta[:]), func(pgno uint32) error {
		m[pgno] = struct{}{}
		return nil
	}); err != nil {
		return m, err
	}

	// Traverse every b-tree and mark pages as in-use.
	records, err := tx.rootRecords()
	if err != nil {
		return m, err
	}
	for _, record := range records {
		if err := tx.walkTree(record.Pgno, func(pgno uint32) error {
			m[pgno] = struct{}{}
			return nil
		}); err != nil {
			return m, err
		}
	}

	return m, nil
}

// walkTree recursively iterates over a page and all its children.
func (tx *Tx) walkTree(pgno uint32, fn func(uint32) error) error {
	// Execute callback.
	if err := fn(pgno); err != nil {
		return err
	}

	// Read page and iterate over children.
	page, err := tx.readPage(pgno)
	if err != nil {
		return err
	}

	switch typ := readFlags(page); typ {
	case PageTypeBranch:
		for i, n := 0, readCellN(page); i < n; i++ {
			cell := readBranchCell(page, i)
			if err := tx.walkTree(cell.Pgno, fn); err != nil {
				return err
			}
		}
		return nil
	case PageTypeLeaf:
		return nil
	default:
		return fmt.Errorf("rbf.Tx.forEachTreePage(): invalid page type: pgno=%d type=%d", pgno, typ)
	}
}

// allocate returns a page number for a new available page. This page may be
// pulled from the free list or, if no free pages are available, it will be
// created by extending the file size.
func (tx *Tx) allocate() (uint32, error) {
	// Attempt to find page in freelist.
	pgno, err := tx.nextFreelistPageNo()

	if err != nil {
		return 0, err
	} else if pgno != 0 {
		c := Cursor{tx: tx}
		c.stack.elems[0] = stackElem{pgno: readMetaFreelistPageNo(tx.meta[:])}
		if changed, err := c.Remove(uint64(pgno)); err != nil {
			return 0, err
		} else if !changed {
			panic(fmt.Sprintf("tx.Tx.allocate(): double alloc: %d", pgno))
		}
		return pgno, nil
	}

	// Increment the total page count by one and return the last page.
	pgno = readMetaPageN(tx.meta[:])
	writeMetaPageN(tx.meta[:], pgno+1)
	return pgno, nil
}

func (tx *Tx) nextFreelistPageNo() (uint32, error) {
	c := Cursor{tx: tx}
	c.stack.elems[0] = stackElem{pgno: readMetaFreelistPageNo(tx.meta[:])}
	if err := c.First(); err == io.EOF {
		return 0, nil
	} else if err != nil {
		return 0, err
	}

	cell := c.cell()
	v := cell.firstValue()

	pgno := uint32((cell.Key << 16) | uint64(v))
	return pgno, nil
}

// deallocate releases a page number to the freelist.
func (tx *Tx) deallocate(pgno uint32) error {
	c := Cursor{tx: tx}
	c.stack.elems[0] = stackElem{pgno: readMetaFreelistPageNo(tx.meta[:])}

	if changed, err := c.Add(uint64(pgno)); err != nil {
		return err
	} else if !changed {
		panic(fmt.Sprintf("rbf.Tx.deallocate(): double free: %d", pgno))
	}
	return nil
}

// deallocateTree recursively all pages in a btree.
func (tx *Tx) deallocateTree(pgno uint32) error {
	page, err := tx.readPage(pgno)
	if err != nil {
		return err
	}

	switch typ := readFlags(page); typ {
	case PageTypeBranch:
		for i, n := 0, readCellN(page); i < n; i++ {
			cell := readBranchCell(page, i)
			if err := tx.deallocateTree(cell.Pgno); err != nil {
				return err
			}
		}
		return nil

	case PageTypeLeaf:
		return tx.deallocate(pgno)
	default:
		return fmt.Errorf("rbf.Tx.deallocateTree(): invalid page type: pgno=%d type=%d", pgno, typ)
	}
}

func (tx *Tx) readPage(pgno uint32) ([]byte, error) {
	//	fmt.Println("readPage", pgno)
	// Meta page is always cached on the transaction.
	if pgno == 0 {
		return tx.meta[:], nil
	}

	pageN := readMetaPageN(tx.meta[:])
	if pgno > pageN {
		return nil, fmt.Errorf("rbf: page read out of bounds: pgno=%d max=%d", pgno, pageN)
	}
	return tx.db.readPage(tx.pageMap, pgno)
}

func (tx *Tx) writePage(page []byte) error {
	//	fmt.Println("writePage", readPageNo(page))
	// Write page to WAL and obtain position in WAL.
	walID, err := tx.db.writeWALPage(page, false)
	if err != nil {
		return err
	}

	// Mark transaction as dirty so we write a meta page on commit/rollback.
	tx.dirty = true

	// Update page map with WAL position.
	tx.pageMap = tx.pageMap.Set(readPageNo(page), walID)
	return nil
}

func (tx *Tx) writeBitmapPage(pgno uint32, page []byte) error {
	// Write bitmap to WAL and obtain WAL position of the actual page data (not the prefix page).
	walID, err := tx.db.writeBitmapPage(pgno, page)
	if err != nil {
		return err
	}

	// Mark transaction as dirty so we write a meta page on commit/rollback.
	tx.dirty = true

	// Update page map with WAL position.
	tx.pageMap = tx.pageMap.Set(pgno, walID)
	return nil
}

func (tx *Tx) writeMetaPage(flag uint32) error {
	// Set meta flags.
	writeFlags(tx.meta[:], flag)

	// Write page to WAL and obtain position in WAL.
	walID, err := tx.db.writeWALPage(tx.meta[:], true)
	if err != nil {
		return err
	}
	tx.pageMap = tx.pageMap.Set(uint32(0), walID)

	return nil
}

func (tx *Tx) AddRoaring(name string, bm *roaring.Bitmap) (changed bool, err error) {
	tx.mu.RLock()
	defer tx.mu.RUnlock()

	if err := tx.createBitmapIfNotExists(name); err != nil {
		return false, err
	}

	c, err := tx.cursor(name)
	if err != nil {
		return false, err
	}
	return c.AddRoaring(bm)
}

func (tx *Tx) leafCellBitmap(pgno uint32) (uint32, []uint64, error) {
	page, err := tx.readPage(pgno)
	if err != nil {
		return 0, nil, err
	}
	return pgno, toArray64(page), err
}

func (tx *Tx) ContainerIterator(name string, key uint64) (citer roaring.ContainerIterator, found bool, err error) {
	tx.mu.RLock()
	defer tx.mu.RUnlock()

	c, err := tx.cursor(name)
	if c == nil && err == nil {
		return &emptyContainerIterator{}, false, nil // nothing available.
	} else if err != nil {
		return nil, false, err
	}

	// INVAR: c is not nil

	if _, err := c.Seek(key); err != nil {
		return nil, false, err
	}
	return &containerIterator{cursor: c}, true, nil
}

func (tx *Tx) ForEach(name string, fn func(i uint64) error) error {
	return tx.ForEachRange(name, 0, math.MaxUint64, fn)
}

func (tx *Tx) ForEachRange(name string, start, end uint64, fn func(uint64) error) error {
	tx.mu.RLock()
	defer tx.mu.RUnlock()

	c, err := tx.cursor(name)
	if err != nil {
		return err
	} else if c == nil {
		return nil
	} else if _, err := c.Seek(highbits(start)); err != nil {
		return err
	}

	for {
		if err := c.Next(); err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}

		switch cell := c.cell(); cell.Type {
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
					if bits&(1<<j) != 0 {
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
			panic(fmt.Sprintf("invalid container type: %d", cell.Type))
		}
	}
}

func (tx *Tx) Count(name string) (uint64, error) {
	tx.mu.RLock()
	defer tx.mu.RUnlock()

	c, err := tx.cursor(name)
	if err != nil {
		return 0, err
	} else if c == nil {
		return 0, nil
	} else if err := c.First(); err != nil {
		return 0, err
	}

	var n uint64
	for {
		if err := c.Next(); err == io.EOF {
			break
		} else if err != nil {
			return 0, err
		}

		n += uint64(c.cell().BitN)
	}
	return n, nil
}

func (tx *Tx) Max(name string) (uint64, error) {
	tx.mu.RLock()
	defer tx.mu.RUnlock()

	c, err := tx.cursor(name)
	if err != nil {
		return 0, err
	} else if c == nil {
		return 0, nil
	} else if err := c.Last(); err == io.EOF {
		return 0, nil
	} else if err != nil {
		return 0, err
	}

	cell := c.cell()
	return uint64((cell.Key << 16) | uint64(cell.lastValue())), nil
}

func (tx *Tx) Min(name string) (uint64, bool, error) {
	tx.mu.RLock()
	defer tx.mu.RUnlock()

	c, err := tx.cursor(name)
	if err != nil {
		return 0, false, err
	} else if c == nil {
		return 0, false, nil
	} else if err := c.First(); err == io.EOF {
		return 0, false, nil
	} else if err != nil {
		return 0, false, err
	}

	cell := c.cell()
	return uint64((cell.Key << 16) | uint64(cell.firstValue())), true, nil
}

func (tx *Tx) UnionInPlace(name string, others ...*roaring.Bitmap) error {
	panic("TODO")
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

	csr, err := tx.cursor(name)
	if err != nil {
		return 0, err
	} else if csr == nil {
		return 0, nil
	}

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

		c := csr.cell()
		k := c.Key
		if k > ekey {
			break
		}

		// If range is entirely in one container then just count that range.
		if skey == ekey {
			return uint64(c.countRange(int32(lowbits(start)), int32(lowbits(end)))), nil
		}
		// INVAR: skey < ekey

		// k > ekey handles the case when start > end and where start and end
		// are in different containers. Same container case is already handled above.
		if k > ekey {
			break
		}
		if k == skey {
			n += uint64(c.countRange(int32(lowbits(start)), roaring.MaxContainerVal+1))
			continue
		}
		if k < ekey {
			n += uint64(c.BitN)
			continue
		}
		if k == ekey {
			n += uint64(c.countRange(0, int32(lowbits(end))))
			break
		}
	}
	return n, nil
}

func (tx *Tx) OffsetRange(name string, offset, start, endx uint64) (*roaring.Bitmap, error) {
	if lowbits(offset) != 0 {
		panic("offset must not contain low bits")
	} else if lowbits(start) != 0 {
		panic("range start must not contain low bits")
	} else if lowbits(endx) != 0 {
		panic("range endx must not contain low bits")
	}

	tx.mu.RLock()
	defer tx.mu.RUnlock()

	c, err := tx.cursor(name)
	if err != nil {
		return nil, err
	}

	other := roaring.NewSliceBitmap()
	off := highbits(offset)
	hi0, hi1 := highbits(start), highbits(endx)

	if c == nil {
		// bitmap not found. Match what roaring does and return nil in this case.
		return other, nil
	}

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

		cell := c.cell()
		ckey := cell.Key

		// >= hi1 is correct b/c endx cannot have any lowbits set.
		if ckey >= hi1 {
			break
		}
		other.Containers.Put(off+(ckey-hi0), toContainer(cell, tx))
	}
	return other, nil
}

// containerIterator wraps Cursor to implement roaring.ContainerIterator.
type containerIterator struct {
	cursor *Cursor
}

// Close is a no-op. It exists to implement the roaring.ContainerIterator interface.
func (itr *containerIterator) Close() {}

// Next moves the iterator to the next container.
func (itr *containerIterator) Next() bool {
	err := itr.cursor.Next()
	return err == nil
}

// Value returns the current key & container.
func (itr *containerIterator) Value() (uint64, *roaring.Container) {
	cell := itr.cursor.cell()
	return cell.Key, toContainer(cell, itr.cursor.tx)
}

// always returns false for Next()
type emptyContainerIterator struct{}

func (si *emptyContainerIterator) Close() {}

func (si *emptyContainerIterator) Next() bool {
	return false
}
func (si *emptyContainerIterator) Value() (uint64, *roaring.Container) {
	panic("emptyContainerIterator never has any Values")
}

func (tx *Tx) Dump(index string) {
	fmt.Println(tx.DumpString(index))
}
func (tx *Tx) DumpString(index string) (r string) {

	r = "allkeys:[\n"

	// grab root records, for a list of bitmaps.
	records, err := tx.rootRecords()
	panicOn(err)
	n := 0
	for _, rr := range records {
		c, err := tx.cursor(rr.Name)
		panicOn(err)
		err = c.First() // First will rewind to beginning.
		if err == io.EOF {
			r += "<empty bitmap>"
			n++
			continue
		}
		panicOn(err)
		for {
			err := c.Next()
			if err == io.EOF {
				break
			}
			panicOn(err)
			cell := c.cell()
			ckey := cell.Key
			ct := toContainer(cell, tx)

			s := stringOfCkeyCt(ckey, ct, rr.Name, index)
			r += s
			n++
		}
	}
	if n == 0 {
		return ""
	}
	// note that we can have a bitmap present, but it can be empty
	r += "]\n   all-in-blake3:" + blake3sum16([]byte(r)) + "\n"

	return "rbf-" + r
}

func containerToBytes(ct *roaring.Container) []byte {
	ty := roaring.ContainerType(ct)
	switch ty {
	case containerNil:
		panic("nil container")
	case containerArray:
		return fromArray16(roaring.AsArray(ct))
	case containerBitmap:
		return fromArray64(roaring.AsBitmap(ct))
	case containerRun:
		return fromInterval16(roaring.AsRuns(ct))
	}
	panic(fmt.Sprintf("unknown container type '%v'", int(ty)))
}

func badgerKey(index, field, view string, shard uint64, roaringContainerKey uint64) []byte {
	// The %020d which adds zero padding up to 20 runes is required to
	// allow the textual sort to accurately
	// reflect a numeric sort order. This is because, as a string,
	// math.MaxUint64 is 20 bytes long.
	// Example of such a badgerKey with a container-key that is math.MaxUint64:
	// ...........................................12345678901234567890
	// idx:'i';fld:'f';vw:'standard';shd:'1';ckey@18446744073709551615

	prefix := badgerPrefix(index, field, view, shard)
	ckey := []byte(fmt.Sprintf("%020d", roaringContainerKey))
	bkey := append(prefix, ckey...)
	MustValidateKey(bkey)
	return bkey
}

// badgerPrefix returns everything from badgerKey up to and
// including the '@' fune in a badger key. The prefix excludes the roaring container key itself.
// NB must be kept in sync with badgerKey() and badgerKeyExtractContainerKey().
func badgerPrefix(index, field, view string, shard uint64) []byte {
	return []byte(fmt.Sprintf("idx:'%v';fld:'%v';vw:'%v';shd:'%020v';ckey@", index, field, view, shard))
}

// MustValidatekey will panic on a bad badgerKey with an informative message.
func MustValidateKey(bkey []byte) {
	n := len(bkey)
	if n < 56 {
		panic(fmt.Sprintf("bkey too short min size is 56 but we see %v in '%v'", n, string(bkey)))
	}
	beforeCkey := bkey[n-26 : n-20]
	if !bytes.Equal(beforeCkey, ckeyPartExpected) {
		panic(fmt.Sprintf(`bkey did not have expected ";ckey@" at 26 bytes from the end of the bkey '%v'; instead had '%v'`, string(bkey), string(beforeCkey)))
	}
}

func bitmapAsString(rbm *roaring.Bitmap) (r string) {
	r = "c("
	slc := rbm.Slice()
	width := 0
	s := ""
	for _, v := range slc {
		if width == 0 {
			s = fmt.Sprintf("%v", v)
		} else {
			s = fmt.Sprintf(", %v", v)
		}
		width += len(s)
		r += s
		if width > 70 {
			r += ",\n"
			width = 0
		}
	}
	if width == 0 && len(r) > 2 {
		r = r[:len(r)-2]
	}
	return r + ")"
}

// should really be exported from the pilosa/roaring package so we don't get out of sync...
const (
	containerNil    byte = iota // no container
	containerArray              // slice of bit position values
	containerBitmap             // slice of 1024 uint64s
	containerRun                // container of run-encoded bits
)

var ckeyPartExpected = []byte(";ckey@")

func invName(rbfName string) (field, view string, shard uint64) {
	s := strings.Split(rbfName, "\x00")
	if len(s) != 3 {
		panic("should have 3 parts")
	}
	field = s[0]
	view = s[1]
	var err error
	shard, err = strconv.ParseUint(s[2], 10, 64)
	panicOn(err)
	return
}

func stringOfCkeyCt(ckey uint64, ct *roaring.Container, rrName, index string) (s string) {

	by := containerToBytes(ct)
	hash := blake3sum16(by)

	cts := roaring.NewSliceContainers()
	cts.Put(ckey, ct)
	rbm := &roaring.Bitmap{Containers: cts}
	srbm := bitmapAsString(rbm)

	field, view, shard := invName(rrName)
	bkey := string(badgerKey(index, field, view, shard, ckey))

	s = fmt.Sprintf("%v -> %v (%v hot)\n", bkey, hash, ct.N())
	s += "          ......." + srbm + "\n"
	return
}

func (tx *Tx) ImportRoaringBits(name string, itr roaring.RoaringIterator, clear bool, log bool, rowSize uint64, data []byte) (changed int, rowSet map[uint64]int, err error) {

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

	var oldC *roaring.Container
	for itrKey, synthC := itr.NextContainer(); synthC != nil; itrKey, synthC = itr.NextContainer() {
		if rowSize != 0 {
			currRow = itrKey / rowSize
		}
		nsynth := int(synthC.N())
		if nsynth == 0 {
			continue
		}
		// INVAR: nsynth > 0

		oldC, err = tx.Container(name, itrKey)
		panicOn(err)
		if err != nil {
			return
		}

		if oldC == nil || oldC.N() == 0 {
			// no container at the itrKey in badger (or all zero container).
			if clear {
				// changed of 0 and empty rowSet is perfect, no need to change the defaults.
				continue
			} else {

				changed += nsynth
				rowSet[currRow] += nsynth

				err = tx.PutContainer(name, itrKey, synthC)
				if err != nil {
					return
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

				if tx.DeleteEmptyContainer && newC.N() == 0 {
					err = tx.RemoveContainer(name, itrKey)
					if err != nil {
						return
					}
					continue
				}
				err = tx.PutContainer(name, itrKey, newC)
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
				err = tx.PutContainer(name, itrKey, synthC)
				if err != nil {
					return
				}
				continue
			}

			newC := oldC.UnionInPlace(synthC)

			if roaring.ContainerType(newC) == containerBitmap {
				newC.Repair() // update the bit-count so .n is valid. b/c UnionInPlace doesn't update it.
			}
			if newC.N() != existN {
				changes := int(newC.N() - existN)
				changed += changes
				rowSet[currRow] += changes

				err = tx.PutContainer(name, itrKey, newC)
				if err != nil {
					panicOn(err)
					return
				}
				continue
			}
		}
	}
	return
}
