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
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/benbjohnson/immutable"
	rbfcfg "github.com/molecula/featurebase/v2/rbf/cfg"
	"github.com/molecula/featurebase/v2/syswrap"
)

var (
	ErrClosed = errors.New("rbf: database closed")
)

// shared cursor pool across all DB instances.
// Cursors are returned on Cursor.Close().
var cursorSyncPool = &sync.Pool{
	New: func() interface{} {
		return &Cursor{}
	},
}

// DB options like 	MaxSize, FsyncEnabled, DoAllocZero
// can be set before calling DB.Open().
type DB struct {
	cfg rbfcfg.Config

	data        []byte               // database mmap
	file        *os.File             // database file descriptor
	rootRecords *immutable.SortedMap // cached root records
	pageMap     *PageMap             // pgno-to-WALID mapping
	txs         map[*Tx]struct{}     // active transactions
	opened      bool                 // true if open

	wal      []byte   // wal mmap
	walFile  *os.File // wal file descriptor
	walPageN int      // wal page count

	mu       sync.RWMutex // general mutex
	rwmu     sync.Mutex   // mutex for restricting single writer
	haltCond *sync.Cond   // condition for resuming txs after checkpoint

	// Path represents the path to the database file.
	Path string
}

// NewDB returns a new instance of DB.
// If cfg is nil we will use the rbfcfg.DefaultConfig().
func NewDB(path string, cfg *rbfcfg.Config) *DB {
	if cfg == nil {
		cfg = rbfcfg.NewDefaultConfig()
	}
	db := &DB{
		cfg:     *cfg,
		txs:     make(map[*Tx]struct{}),
		pageMap: NewPageMap(),
		Path:    path,
	}
	db.haltCond = sync.NewCond(&db.mu)

	return db
}

// DataPath returns the path to the data file for the DB.
func (db *DB) DataPath() string {
	return filepath.Join(db.Path, "data")
}

// WALPath returns the path to the WAL file.
func (db *DB) WALPath() string {
	return filepath.Join(db.Path, "wal")
}

func CreateDirIfNotExist(path string) {
	dir := filepath.Dir(path)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		err = os.MkdirAll(dir, 0755)
		if err != nil {
			panic(err)
		}
	}
}

// TxN returns the number of active transactions.
func (db *DB) TxN() int {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return len(db.txs)
}

// Open opens a database with the file specified in Path.
// Creates a new file if one does not already exist.
func (db *DB) Open() (err error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := os.MkdirAll(db.Path, 0755); err != nil {
		return err
	} else if db.file, err = os.OpenFile(db.DataPath(), os.O_WRONLY|os.O_CREATE, 0666); err != nil {
		return fmt.Errorf("open file: %w", err)
	}

	// Open read-only database mmap.
	if f, err := os.OpenFile(db.DataPath(), os.O_RDONLY, 0666); err != nil {
		return fmt.Errorf("open mmap file: %w", err)
	} else if db.data, err = syswrap.Mmap(int(f.Fd()), 0, int(db.cfg.MaxSize), syscall.PROT_READ, syscall.MAP_SHARED); err != nil {
		f.Close()
		return fmt.Errorf("open mmap file: %w", err)
	} else if err := f.Close(); err != nil {
		return fmt.Errorf("cannot close mmap file: %w", err)
	}

	// Initialize file if it is too small.
	if fi, err := db.file.Stat(); err != nil {
		return fmt.Errorf("stat: %w", err)
	} else if fi.Size() < PageSize {
		if err := db.init(); err != nil {
			return fmt.Errorf("init: %w", err)
		}
	}

	// TODO(BBJ): Obtain advisory lock on file.

	db.opened = true

	// Open write-ahead log & checkpoint to the end since no transactions are open.
	if err := db.openWAL(); err != nil {
		return fmt.Errorf("wal open: %w", err)
	} else if err := db.checkpoint(); err != nil {
		return fmt.Errorf("checkpoint: %w", err)
	}

	return nil
}

func (db *DB) openWAL() (err error) {
	// Open WAL file writer.
	if db.walFile, err = os.OpenFile(db.WALPath(), os.O_WRONLY|os.O_CREATE, 0666); err != nil {
		return fmt.Errorf("open wal file: %w", err)
	}

	// Open read-only mmap.
	if f, err := os.OpenFile(db.WALPath(), os.O_RDONLY, 0666); err != nil {
		return fmt.Errorf("open wal mmap file: %w", err)
	} else if db.wal, err = syswrap.Mmap(int(f.Fd()), 0, int(db.cfg.MaxWALSize), syscall.PROT_READ, syscall.MAP_SHARED); err != nil {
		f.Close()
		return fmt.Errorf("map wal mmap file: %w", err)
	} else if err := f.Close(); err != nil {
		return fmt.Errorf("cannot close wal mmap file: %w", err)
	}

	// Determine the number of whole pages in the WAL.
	var pageN int
	if fi, err := db.walFile.Stat(); err != nil {
		return fmt.Errorf("wal stat: %w", err)
	} else {
		pageN = int(fi.Size() / PageSize)
	}

	// Read backwards through the WAL to find the last valid meta page.
	for ; pageN > 0; pageN-- {
		if page, err := db.readWALPageAt(pageN - 1); err != nil {
			return err
		} else if IsMetaPage(page) {
			break
		}
	}

	// Truncate WAL to the last valid meta page.
	if err := db.walFile.Truncate(int64(pageN * PageSize)); err != nil {
		return fmt.Errorf("wal truncate: %w", err)
	} else if _, err := db.walFile.Seek(int64(pageN*PageSize), io.SeekStart); err != nil {
		return fmt.Errorf("wal seek: %w", err)
	}
	db.walPageN = pageN

	return nil
}

// checkpoint moves all WAL pages to the main DB file.
// Must be called by a write transaction while under db.mu lock.
func (db *DB) checkpoint() error {
	if !db.opened {
		return nil
	} else if len(db.txs) > 0 {
		return nil // skip if transactions open
	}

	for i := 0; i < db.walPageN; i++ {
		page, err := db.readWALPageAt(i)
		if err != nil {
			return err
		}

		// Determine page number. Meta pages are always on zero & bitmap
		// headers specify the page number of the next page in the WAL.
		// All other pages have their page number in the page data.
		var pgno uint32
		if IsBitmapHeader(page) {
			pgno = readPageNo(page)
			if page, err = db.readWALPageAt(i + 1); err != nil {
				return err
			}
			i++ // bitmaps in WAL are two pages
		} else if !IsMetaPage(page) {
			pgno = readPageNo(page)
		}

		// Write data to the data file.
		if err := db.writeDBPage(pgno, page); err != nil {
			return err
		}
	}

	// Ensure database file is synced and then truncate the WAL file.
	if err := db.fsync(db.file); err != nil {
		return fmt.Errorf("db file sync: %w", err)
	} else if err := db.walFile.Truncate(0); err != nil {
		return fmt.Errorf("truncate wal file: %w", err)
	} else if err := db.fsync(db.walFile); err != nil {
		return fmt.Errorf("wal file sync: %w", err)
	} else if _, err := db.walFile.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek wal file: %w", err)
	}
	db.walPageN = 0
	db.pageMap = NewPageMap()

	// Notify halted tranactions that the WAL has been checkpointed.
	db.haltCond.Broadcast()

	return nil
}

// Close closes the database.
func (db *DB) Close() (err error) {
	// TODO(bbj): Add wait group to hang until last Tx is complete.

	// Wait for writer lock.
	db.rwmu.Lock()
	defer db.rwmu.Unlock()

	db.mu.Lock()
	defer db.mu.Unlock()

	db.opened = false

	// Close mmap handle.
	if db.data != nil {
		if e := syswrap.Munmap(db.data); e != nil && err == nil {
			err = e
		}
		db.data = nil
	}

	// Close writer handler.
	if db.file != nil {
		err = db.file.Sync()
		if err != nil {
			return
		}
		if e := db.file.Close(); e != nil && err == nil {
			err = e
		}
		db.file = nil
	}

	// Close WAL mmap handle.
	if db.wal != nil {
		if e := syswrap.Munmap(db.wal); e != nil && err == nil {
			err = e
		}
		db.wal = nil
	}

	// Close wal writer handler.
	if db.walFile != nil {
		if e := db.walFile.Close(); e != nil && err == nil {
			err = e
		}
		db.walFile = nil
	}

	return err
}

// HasData with requireOneHotBit=false returns
// hasAnyRecords true if any record has been stored,
// even if the value for that bitmap record turned out to have
// no bits hot (be all zeroes).
//
// In this case, we are taking the attempted storage
// of any named bitmap into the database as evidence
// that the db is in use, and we return hasAnyRecords true.
//
// Conversely, if requireOneHotBit is true, then a
// database consisting of only a named bitmap with
// an all zeroes (no bits hot)
// will return hasAnyRecords false. We must find at
// least a single hot bit inside the db
// in order to return hasAnyRecords true.
//
// HasData is used by backend migration and blue/green checks.
//
// If there is a disk error we return (false, error), so always
// check the error before deciding if hasAnyRecords is valid.
//
// We will internally create and rollback a read-only
// transaction to answer this query.
func (db *DB) HasData(requireOneHotBit bool) (hasAnyRecords bool, err error) {

	// Read a list of all bitmaps in Tx.
	tx, err := db.Begin(false)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	records, err := tx.RootRecords()
	if err != nil {
		return false, err
	}
	// Loop over each bitmap and attempt to move to the first cell.
	// If we can move to a cell then we have at least one record.

	for itr := records.Iterator(); !itr.Done(); {
		name, _ := itr.Next()
		// Fetch cursor for bitmap.
		cur, err := tx.Cursor(name.(string))
		if err != nil {
			return false, err
		}
		defer cur.Close()

		if !requireOneHotBit {
			return true, nil
		}
		// INVAR: requireOneHotBit true

		// Check if we can move to the first cell.
		if err := cur.First(); err == io.EOF {
			continue // no data in bitmap
		} else if err != nil {
			return false, err
		}
		return true, nil
	}
	return false, nil
}

// Size returns the size of the database & WAL, in bytes.
func (db *DB) Size() (int64, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	fi, err := os.Stat(db.Path)
	if err != nil {
		return 0, err
	}
	return db.walSize() + fi.Size(), nil
}

// WALSize returns the size of the WAL, in bytes.
func (db *DB) WALSize() int64 {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.walSize()
}

func (db *DB) walSize() int64 {
	return int64(db.walPageN * PageSize)
}

// init initializes a new database file.
func (db *DB) init() error {
	if err := db.initMetaPage(); err != nil {
		return fmt.Errorf("meta: %w", err)
	} else if err := db.initRootRecordPage(); err != nil {
		return fmt.Errorf("root record page: %w", err)
	} else if err := db.initFreelistPage(); err != nil {
		return fmt.Errorf("freelist page: %w", err)
	}
	return nil
}

// initMetaPage initializes the meta page.
func (db *DB) initMetaPage() error {

	page := make([]byte, PageSize)
	writeMetaMagic(page)
	writeMetaPageN(page, 3)
	writeMetaRootRecordPageNo(page, 1)
	writeMetaFreelistPageNo(page, 2)
	_, err := db.file.WriteAt(page, 0*PageSize)
	return err
}

// initRootRecordPage initializes the initial root record page.
func (db *DB) initRootRecordPage() error {

	page := make([]byte, PageSize)
	writePageNo(page, 1)
	writeFlags(page, PageTypeRootRecord)
	_, err := db.file.WriteAt(page, 1*PageSize)
	return err
}

// initFreelistPage initializes the initial freelist btree page.
func (db *DB) initFreelistPage() error {

	page := make([]byte, PageSize)
	writePageNo(page, 2)
	writeFlags(page, PageTypeLeaf)
	_, err := db.file.WriteAt(page, 2*PageSize)
	return err
}

// Begin starts a new transaction.
func (db *DB) Begin(writable bool) (_ *Tx, err error) {
	// Ensure only one writable transaction at a time.
	if writable {
		db.rwmu.Lock()
	}

	// This local function is called at exit points that occur before we can
	// call Rollback() which would normally release these locks.
	cleanup := func() {
		if writable {
			db.rwmu.Unlock()
		}
	}

	db.mu.Lock()
	// note: We cannot defer db.mu.Unlock() here because
	// we call tx.Rollback() before if db.readMetaPage
	// returns an error, and thus we will deadlock against
	// ourselves when the Rollback tries to acquire the db.mu.
	// This is why db.mu.Unlock() is done manually below.

	if !db.opened {
		cleanup()
		db.mu.Unlock()
		return nil, ErrClosed
	}

	// Wait for WAL size to be below threshold.
	for int64(db.walPageN*PageSize) > db.cfg.MaxWALCheckpointSize {
		db.haltCond.Wait()
	}

	tx := &Tx{
		db:          db,
		rootRecords: db.rootRecords,
		pageMap:     db.pageMap,
		walPageN:    db.walPageN,
		writable:    writable,

		DeleteEmptyContainer: true,
	}

	if writable {
		tx.dirtyPages = make(map[uint32][]byte)
		tx.dirtyBitmapPages = make(map[uint32][]byte)
	}

	// Copy meta page into transaction's buffer.
	// This page is only written at the end of a dirty transaction.
	page, err := db.readMetaPage()
	if err != nil {
		// we will deadlock in tx.Rollback()
		// on db.mu.Lock unless we manually db.mu.Unlock first.
		db.mu.Unlock()
		tx.Rollback()
		return nil, err
	}
	copy(tx.meta[:], page)

	// Attach starting WAL ID to transaction.
	tx.walID = readMetaWALID(tx.meta[:])

	// Track transaction with the DB.
	db.txs[tx] = struct{}{}

	// If no root records are cached, build the cache the first time.
	// Normally the cache is updated by successful write transactions but
	// this avoids recomputing the cache if there are no write txs for a while.
	if db.rootRecords == nil {
		if db.rootRecords, err = tx.RootRecords(); err != nil {
			db.mu.Unlock()
			tx.Rollback()
			return nil, err
		}
	}

	db.mu.Unlock()
	return tx, nil
}

// removeTx removes an active transaction from the database.
func (db *DB) removeTx(tx *Tx) error {
	// Release writer lock if tx is writable.
	if tx.writable {
		tx.db.rwmu.Unlock()
	}

	delete(tx.db.txs, tx)

	// Disassociate from db.
	tx.db = nil

	// Write pages from WAL to DB.
	// TODO(bbj): Move this to an async goroutine.
	if len(db.txs) == 0 && db.walSize() > db.cfg.MinWALCheckpointSize {
		if err := db.checkpoint(); err != nil {
			return fmt.Errorf("checkpoint: %w", err)
		}
	}

	return nil
}

// Check performs an integrity check.
func (db *DB) Check() error {

	tx, err := db.Begin(false)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	return tx.Check()
}

// writeDBPage writes a page to the data file.
func (db *DB) writeDBPage(pgno uint32, page []byte) error {
	_, err := db.file.WriteAt(page, int64(pgno)*PageSize)
	return err
}

func (db *DB) readDBPage(pgno uint32) ([]byte, error) {
	offset := int64(pgno) * PageSize
	return db.data[offset : offset+PageSize], nil
}

// baseWALID returns the WAL ID stored in the database file meta page.
func (db *DB) baseWALID() int64 {
	return readMetaWALID(db.data)
}

// readWALPageByID reads a WAL page by WAL ID.
func (db *DB) readWALPageByID(id int64) ([]byte, error) {
	return db.readWALPageAt(int(id - db.baseWALID() - 1))
}

// readWALPageAt reads the i-th page in the WAL file.
func (db *DB) readWALPageAt(i int) ([]byte, error) {
	offset := int64(i) * PageSize
	return db.wal[offset : offset+PageSize], nil
}

func (db *DB) readMetaPage() ([]byte, error) {
	if walID, ok := db.pageMap.Get(uint32(0)); ok {
		return db.readWALPageByID(walID)
	}
	return db.readDBPage(0)
}

func (db *DB) getCursor(tx *Tx) *Cursor {
	c := cursorSyncPool.Get().(*Cursor)
	c.tx = tx
	return c
}

// Shared pool for in-memory database pages.
// These are used before being flushed to disk.
var pagePool = &sync.Pool{
	New: func() interface{} {
		page := make([]byte, PageSize)
		return &page
	},
}

func allocPage() []byte {
	page := pagePool.Get().(*[]byte)
	return *page
}

func freePage(page []byte) {
	pagePool.Put(&page)
}
