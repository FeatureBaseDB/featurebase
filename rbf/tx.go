package rbf

import (
	"fmt"
	"io"
	"sort"

	"github.com/benbjohnson/immutable"
	"github.com/pilosa/pilosa/v2/roaring"
)

// Tx represents a transaction.
type Tx struct {
	db       *DB            // parent db
	meta     [PageSize]byte // copy of current meta page
	walID    int64          // max WAL ID at start of tx
	pageMap  *immutable.Map // mapping of database pages to WAL IDs
	writable bool           // if true, tx can write
	dirty    bool           // if true, changes have been made
}

// Commit completes the transaction and persists data changes.
func (tx *Tx) Commit() error {
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
	tx.db.removeTx(tx)

	return nil
}

func (tx *Tx) Rollback() error {
	if tx.db == nil {
		return ErrTxClosed
	}

	// If any pages have been written, ensure we write a new meta page with
	// the rollback flag to mark the end of the transaction. This allows us to
	// discard pages in the transaction during playback of the WAL on open.
	if tx.dirty {
		if err := tx.writeMetaPage(MetaPageFlagRollback); err != nil {
			return err
		} else if err := tx.db.SyncWAL(); err != nil {
			return err
		}
	}

	if err := tx.db.checkpoint(); err != nil {
		return err
	}

	// Disconnect transaction from DB.
	tx.db.removeTx(tx)

	return nil
}

// Root returns the root page number for a bitmap. Returns 0 if the bitmap does not exist.
func (tx *Tx) Root(name string) (uint32, error) {
	records, err := tx.rootRecords()
	if err != nil {
		return 0, err
	}

	i := sort.Search(len(records), func(i int) bool { return records[i].Name >= name })
	if i >= len(records) || records[i].Name != name {
		return 0, fmt.Errorf("bitmap not found: %q", name)
	}
	return records[i].Pgno, nil
}

// CreateBitmap creates a new empty bitmap with the given name.
// Returns an error if the bitmap already exists.
func (tx *Tx) CreateBitmap(name string) error {
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
		return fmt.Errorf("bitmap already exists: %q", name)
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
func dump(r []*RootRecord) {
	for _, i := range r {
		fmt.Println("RECORD", i.Name, i.Pgno)
	}

}

// DeleteBitmap removes a bitmap with the given name.
// Returns an error if the bitmap does not exist.
func (tx *Tx) DeleteBitmap(name string) error {
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

// RenameBitmap updates the name of an existing bitmap.
// Returns an error if the bitmap does not exist.
func (tx *Tx) RenameBitmap(oldname, newname string) error {
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
		pgno = readRootRecordOverflowPgno(page)
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

		tx.deallocate(pgno)
		pgno = readRootRecordOverflowPgno(page)
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
func (tx *Tx) Add(name string, a ...uint64) (changed bool, err error) {
	if tx.db == nil {
		return false, ErrTxClosed
	} else if !tx.writable {
		return false, ErrTxNotWritable
	} else if name == "" {
		return false, ErrBitmapNameRequired
	}

	c, err := tx.Cursor(name)
	if err != nil {
		return false, err
	}
	for _, v := range a {
		if vchanged, err := c.Add(v); err != nil {
			return changed, err
		} else if vchanged {
			changed = true
		}
	}
	return changed, nil
}

// Remove unsets a given bit on the bitmap.
func (tx *Tx) Remove(name string, a ...uint64) (changed bool, err error) {
	if tx.db == nil {
		return false, ErrTxClosed
	} else if !tx.writable {
		return false, ErrTxNotWritable
	} else if name == "" {
		return false, ErrBitmapNameRequired
	}

	c, err := tx.Cursor(name)
	if err != nil {
		return false, err
	}
	for _, v := range a {
		if vchanged, err := c.Remove(v); err != nil {
			return changed, err
		} else if vchanged {
			changed = true
		}
	}
	return changed, nil
}

// Contains returns true if the given bit is set on the bitmap.
func (tx *Tx) Contains(name string, v uint64) (bool, error) {
	if tx.db == nil {
		return false, ErrTxClosed
	} else if name == "" {
		return false, ErrBitmapNameRequired
	}

	c, err := tx.Cursor(name)
	if err != nil {
		return false, err
	}
	return c.Contains(v)
}

// Cursor returns an instance of a cursor this bitmap.
func (tx *Tx) Cursor(name string) (*Cursor, error) {
	if tx.db == nil {
		return nil, ErrTxClosed
	} else if name == "" {
		return nil, ErrBitmapNameRequired
	}

	root, err := tx.Root(name)
	if err != nil {
		return nil, err
	}

	c := Cursor{tx: tx}
	c.stack.elems[0] = stackElem{pgno: root}
	return &c, nil
}

// Check verifies the integrity of the database.
func (tx *Tx) Check() error {
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
		pgno = readRootRecordOverflowPgno(page)
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
			if cell.Flags&ContainerTypeBitmap != 0 { // bitmap cell (cannot traverse into)
				if err := fn(cell.Pgno); err != nil {
					return err
				}
			} else {
				if err := tx.walkTree(cell.Pgno, fn); err != nil {
					return err
				}
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
			if cell.Flags&ContainerTypeBitmap == 0 { // leaf/branch child page
				if err := tx.deallocateTree(cell.Pgno); err != nil {
					return err
				}
			} else {
				if err := tx.deallocate(cell.Pgno); err != nil { // bitmap child page
					return err
				}
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
	c, err := tx.Cursor(name)
	if err != nil {
		return false, err
	}
	return c.AddRoaring(bm)
}
