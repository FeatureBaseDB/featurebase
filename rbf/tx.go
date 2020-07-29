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
	"fmt"
	"io"
	"math"
	"sort"
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

	// TODO(bbj): Invalidate DB if rollback fails. Possibly attempt reopen?

	if tx.db == nil {
		return
	}

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

	_ = tx.db.checkpoint() // TODO: Check error

	// Disconnect transaction from DB.
	_ = tx.db.removeTx(tx) // TODO: Check error
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
func (tx *Tx) PutContainer(name string, key uint64, cont *roaring.Container) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	cell := ConvertToLeafArgs(key, cont)
	if cell.BitN == 0 {
		return nil
	}

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
	if err != nil {
		// TODO(bbj): Don't return error if bitmap is simply not found?
		return nil, false, err
	} else if c == nil {
		return nil, false, nil
	} else if err := c.First(); err != nil {
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

func (tx *Tx) CountRange(name string, start, end uint64) (uint64, error) {
	tx.mu.RLock()
	defer tx.mu.RUnlock()

	c, err := tx.cursor(name)
	if err != nil {
		return 0, err
	} else if c == nil {
		return 0, nil
	}

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

		cell := c.cell()
		if cell.Key > highbits(end) {
			break
		}

		if cell.Key == highbits(start) {
			n += uint64(cell.countRange(lowbits(start), math.MaxUint16))
		} else if cell.Key == highbits(end) {
			n += uint64(cell.countRange(0, lowbits(end)))
		} else {
			n += uint64(cell.BitN)
		}
	}
	return n, nil
}

func (tx *Tx) OffsetRange(name string, offset, start, end uint64) (*roaring.Bitmap, error) {
	tx.mu.RLock()
	defer tx.mu.RUnlock()

	b, err := tx.RoaringBitmap(name)
	if err != nil {
		return nil, err
	}
	return b.OffsetRange(offset, start, end), nil
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
	return err != nil
}

// Value returns the current key & container.
func (itr *containerIterator) Value() (uint64, *roaring.Container) {
	cell := itr.cursor.cell()
	return cell.Key, toContainer(cell, itr.cursor.tx)
}
