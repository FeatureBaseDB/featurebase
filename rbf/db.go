// Copyright 2021 Molecula Corp. All rights reserved.
package rbf

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime/debug"
	"sort"
	"sync"
	"syscall"
	"unsafe"

	"github.com/benbjohnson/immutable"
	"github.com/molecula/featurebase/v2/logger"
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

// txWaiter is a representation of "i need to wait for txs to complete".
// it is created with a function, and will run that function, with the db
// lock held, at some point after every Tx that was open when it was created
// has closed. WARNING: A txWaiter may hold db.rwmu.
type txWaiter struct {
	ready     chan struct{}
	waitingOn map[*Tx]struct{}
	callback  func()
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
	logger      logger.Logger        // for diagnostics from async things

	wal       []byte   // wal mmap
	walFile   *os.File // wal file descriptor
	walPageN  int      // wal page count
	baseWALID int64    // WAL ID of first page

	mu       sync.RWMutex // general mutex
	rwmu     sync.Mutex   // mutex for restricting single writer
	haltCond *sync.Cond   // condition for resuming txs after checkpoint

	txWaiters []*txWaiter // things waiting for Txs to close

	isDead error // this database died in an unrecoverable way, error out opens

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
		logger:  cfg.Logger,
	}
	if db.logger == nil {
		// default to writing to stdout if not told otherwise
		db.logger = logger.NewStandardLogger(os.Stderr)
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
	} else {
		// checkpoint wants to hold the rwmu lock.
		db.rwmu.Lock()
		if err := db.checkpoint(); err != nil {
			return fmt.Errorf("startup checkpoint: %w", err)
		}
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
	var fileSize int64
	if fi, err := db.walFile.Stat(); err != nil {
		return fmt.Errorf("wal stat: %w", err)
	} else {
		fileSize = fi.Size()
		pageN = int(fileSize / PageSize)
	}

	// Read backwards through the WAL to find the last valid meta page.
	for ; pageN > 0; pageN-- {
		if page, err := db.readWALPageAt(pageN - 1); err != nil {
			return err
		} else if IsMetaPage(page) {
			// We now face a challenge. Probably this is a meta page.
			// But consider a sequence of pages written which gets
			// interrupted right before the meta page is written.
			// If the last page is a bitmap page, it could LOOK LIKE a meta
			// page. So we have to check the page before it. If that page
			// is a bitmap header, then actually this is a bitmap page, right?
			// If that page doesn't exist, of course, we're fine, except
			// for the philosophical question of why we wrote a meta page
			// when no pages had changed.
			if pageN > 1 {
				if page, err = db.readWALPageAt(pageN - 2); err != nil {
					return err
				}
				if IsBitmapHeader(page) {
					// But wait!
					// What if this *is* a meta page, and the page before it is
					// actually a *bitmap page* that looks like a bitmap header? And
					// so on.
					//
					// Rather than try to resolve this, in this insanely unlikely
					// situation, we read from the beginning which allows us to
					// always know what we're seeing, because every bitmap page
					// comes *after* a bitmap header page, and thus, we know when
					// we might be seeing one.
					pageN, err = db.methodicalWALPageN(pageN)
					if err != nil {
						return err
					}
				}
			}
			break
		}
	}
	if fileSize != int64(pageN*PageSize) {
		if err := db.walFile.Truncate(int64(pageN * PageSize)); err != nil {
			return fmt.Errorf("wal truncate: %w", err)
		}
	}
	if _, err := db.walFile.Seek(int64(pageN*PageSize), io.SeekStart); err != nil {
		return fmt.Errorf("wal seek: %w", err)
	}
	db.walPageN = pageN
	db.baseWALID = readMetaWALID(db.data)

	return nil
}

// methodicalWALPageN tries to determine the last meta page in a very reliable
// but slow way. This handles the theoretical but hard to imagine creating
// edge case where we have a bitmap page which happens to look like a meta
// page, and the write got interrupted before the meta page got written.
func (db *DB) methodicalWALPageN(pageN int) (lastMeta int, err error) {
	for i := 0; i < pageN; i++ {
		var page []byte
		if page, err = db.readWALPageAt(i); err != nil {
			return -1, err
		}
		switch {
		case IsMetaPage(page):
			lastMeta = i
		case IsBitmapHeader(page):
			// skip the bitmap page, which we can't usefully evaluate
			i++
		}
	}
	return lastMeta, nil
}

// Checkpoint performs a manual checkpoint. This is not necessary except for tests.
func (db *DB) Checkpoint() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.rwmu.Lock()
	return db.checkpoint()
}

// checkpoint moves all WAL pages to the main DB file. Must be called
// while holding both db.mu and db.rwmu. Should release db.rwmu, but not
// db.mu.
func (db *DB) checkpoint() (err error) {
	// if we don't spin off a possible async waiter, we should release the
	// write lock, if we do, that will release it.
	releaseLock := true
	defer func() {
		if releaseLock {
			db.rwmu.Unlock()
		}
	}()
	if !db.opened {
		return nil
	}

	// Check if there are any WAL pages, if not do nothing as
	// checkpointing and calling fsync can be very expensive even if
	// there are no writes.
	if db.walPageN == 0 {
		return nil
	}
	// wake up things waiting on haltCond when we're done, even if we fail.
	// Otherwise, we deadlock with them all stuck waiting on that forever.
	defer func() {
		if err != nil && db.isDead == nil {
			db.isDead = err
		}
		db.haltCond.Broadcast()
	}()

	// Copy the pages from the WAL back to the database outside of the lock.
	if err := func() error {
		db.mu.Unlock() // This is intentionally reversed so run w/o lock
		defer db.mu.Lock()

		var page []byte
		// We might have either a *PageMap or just the file. If we have the file,
		// building the PageMap is fairly expensive because it's fancy and immutable.
		// If we have the PageMap *or* some other map, that's two different things
		// to iterate. If we have the PageMap, building a map from it is relatively
		// cheap, so we'll do it that way.
		pages := make(map[uint32]int)

		if db.pageMap.size == 0 {
			// you'd think we're done, but actually this PROBABLY means that
			// this is initial startup, and we haven't read the file yet. We scan
			// the file for pages, because it turns out most of them probably
			// got overwritten.
			for i := 0; i < db.walPageN; i++ {
				page, err = db.readWALPageAt(i)
				if err != nil {
					return fmt.Errorf("reading WAL page %d: %w", i, err)
				}

				// Determine page number. Meta pages are always on zero & bitmap
				// headers specify the page number of the next page in the WAL.
				// All other pages have their page number in the page data.
				var pgno uint32
				if IsBitmapHeader(page) {
					pgno = readPageNo(page)
					if i+1 < db.walPageN {
						if page, err = db.readWALPageAt(i + 1); err != nil {
							return err
						}
					} else {
						return fmt.Errorf("last page of WAL file (%d) is bitmap header", i)
					}
					i++ // bitmaps in WAL are two pages
				} else if !IsMetaPage(page) {
					pgno = readPageNo(page)
				}
				// record where in the file we have this page
				pages[pgno] = i
			}
		} else {
			itr := db.pageMap.Iterator()
			itr.First()
			for k, v, ok := itr.Next(); ok; k, v, ok = itr.Next() {
				pages[k] = int(v - db.baseWALID - 1)
			}
		}

		for pgno, walID := range pages {
			page, err = db.readWALPageAt(walID)
			if err != nil {
				return fmt.Errorf("reading page %d [page number %d]: %v", walID, pgno, err)
			}

			// Write data to the data file.
			if err = db.writeDBPage(pgno, page); err != nil {
				return fmt.Errorf("writing page %d: %v", pgno, err)
			}
		}

		// Ensure database file is synced and then truncate the WAL file.
		if err = db.fsync(db.file); err != nil {
			return fmt.Errorf("db file sync: %w", err)
		}

		return nil
	}(); err != nil {
		return err
	}

	// now we've updated the file. There are existing transactions that are still
	// using the WAL, though. So we wait for them to terminate before we unlock
	// the rwmu and update the metadata about the WAL.
	releaseLock = false
	db.walPageN = 0
	db.pageMap = NewPageMap()

	db.afterCurrentTx(func() {
		defer db.rwmu.Unlock()
		db.baseWALID = readMetaWALID(db.data)
		db.mu.Unlock()
		defer db.mu.Lock()

		if err = db.walFile.Truncate(0); err != nil {
			db.logger.Errorf("truncate wal file: %w", err)
		} else if err = db.fsync(db.walFile); err != nil {
			db.logger.Errorf("wal file sync: %w", err)
		} else if _, err = db.walFile.Seek(0, io.SeekStart); err != nil {
			db.logger.Errorf("seek wal file: %w", err)
		}

	})

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
		err = db.fsync(db.file)
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
// HasData is used by backend migration.
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

	page := allocPage()
	writeMetaMagic(page)
	writeMetaPageN(page, 3)
	writeMetaRootRecordPageNo(page, 1)
	writeMetaFreelistPageNo(page, 2)
	_, err := db.file.WriteAt(page, 0*PageSize)
	return err
}

// initRootRecordPage initializes the initial root record page.
func (db *DB) initRootRecordPage() error {

	page := allocPage()
	writePageNo(page, 1)
	writeFlags(page, PageTypeRootRecord)
	_, err := db.file.WriteAt(page, 1*PageSize)
	return err
}

// initFreelistPage initializes the initial freelist btree page.
func (db *DB) initFreelistPage() error {

	page := allocPage()
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
	defer db.mu.Unlock()

	if !db.opened {
		cleanup()
		return nil, ErrClosed
	}
	if db.isDead != nil {
		err := db.isDead
		cleanup()
		return nil, err
	}

	// Wait for WAL size to be below threshold, if we're going to write.
	// Reads don't care.
	if writable {
		for int64(db.walPageN*PageSize) > db.cfg.MaxWALCheckpointSize {
			if db.isDead != nil {
				err := db.isDead
				cleanup()
				return nil, err
			}
			// This implicitly releases db.mu.Lock and comes back with it
			// held again.
			db.haltCond.Wait()
		}
	}

	tx := &Tx{
		db:          db,
		rootRecords: db.rootRecords,
		pageMap:     db.pageMap,
		walPageN:    db.walPageN,
		writable:    writable,
		stack:       debug.Stack(), // DEBUG

		DeleteEmptyContainer: true,
	}
	defer func() {
		if err != nil {
			tx.rollback(true)
		}
	}()

	if writable {
		tx.dirtyPages = make(map[uint32][]byte)
		tx.dirtyBitmapPages = make(map[uint32][]byte)
	}

	// Copy meta page into transaction's buffer.
	// This page is only written at the end of a dirty transaction.
	page, err := db.readMetaPage()
	if err != nil {
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
			return nil, err
		}
	}

	return tx, nil
}

// afterCurrentTx produces runs the provided callback, with the db lock
// held, after all current Tx terminate. It should be called with the db
// lock held.
func (db *DB) afterCurrentTx(callback func()) {
	if len(db.txs) == 0 {
		callback()
		return
	}
	txw := &txWaiter{}
	txw.ready = make(chan struct{})
	txw.callback = callback
	txw.waitingOn = make(map[*Tx]struct{}, len(db.txs))
	for k := range db.txs {
		txw.waitingOn[k] = struct{}{}
	}
	db.txWaiters = append(db.txWaiters, txw)
	go func() {
		<-txw.ready
		db.mu.Lock()
		defer db.mu.Unlock()
		txw.callback()
	}()
	return
}

// removeTx removes an active transaction from the database. it obtains
// the db lock, and currently drops it, but will later possibly be leaving
// it retained by an asynchronous op that wants to happen before we start
// running new tx.
func (db *DB) removeTx(tx *Tx) error {
	// We might want to trigger a checkpoint. Only for writable
	// transactions, and only when either there's nothing else open or we
	// really need to.
	checkpoint := false
	if tx.writable {
		walSize := db.walSize()
		if walSize > db.cfg.MinWALCheckpointSize {
			// Might be a good time for a checkpoint. We'll do a checkpoint
			// if we're the only transaction, or if we have to.
			if len(db.txs) == 1 || walSize > db.cfg.MaxWALCheckpointSize {
				checkpoint = true
			}
		}
		// During checkpointing, we'll be preventing writes, but allowing reads.
		if !checkpoint {
			tx.db.rwmu.Unlock()
		}
	}
	// remove ourselves from the list of transactions the db is keeping.
	delete(tx.db.txs, tx)
	for i := 0; i < len(tx.db.txWaiters); i++ {
		txw := tx.db.txWaiters[i]
		// in practice this probably never matters, but theoretically the
		// goroutine that's waiting on the condition variable may
		// not have performed its first test on len(txw.waitingOn) yet.
		delete(txw.waitingOn, tx)
		// let it know we're done. we've still got db.mu.lock, so it won't
		// happen just yet, but it'll be able to continue.
		if len(txw.waitingOn) == 0 {
			// remove us from the db's list
			copy(db.txWaiters[i:], db.txWaiters[i+1:])
			db.txWaiters = db.txWaiters[:len(db.txWaiters)-1]
			close(txw.ready)
			// decrement i so we don't skip an entry we just copied in to [i]
			i--
		}
	}

	// Disassociate from db.
	tx.db = nil

	if checkpoint {
		// We need to run a checkpoint. This can be semi-asynchronous.
		// It needs to wait until every existing transaction has finished,
		// because every existing transaction could want to look up pages
		// which are in the database before our operations, but which should
		// now be in the WAL. We want them to use the WAL instead.
		db.afterCurrentTx(func() {
			// We still hold db.rwmu here. checkpoint unlocks it when it's
			// ready.
			if err := db.checkpoint(); err != nil {
				db.logger.Errorf("async checkpoint: %v", err)
			}
		})
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

// readWALPageByID reads a WAL page by WAL ID.
func (db *DB) readWALPageByID(id int64) ([]byte, error) {
	return db.readWALPageAt(int(id - db.baseWALID - 1))
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

func (db *DB) DebugInfo() *DebugInfo {
	info := &DebugInfo{Path: db.Path}
	for tx := range db.txs {
		info.Txs = append(info.Txs, tx.DebugInfo())
	}
	sort.Slice(info.Txs, func(i, j int) bool { return info.Txs[i].Ptr < info.Txs[j].Ptr })
	return info
}

type DebugInfo struct {
	Path string         `json:"path"`
	Txs  []*TxDebugInfo `json:"txs"`
}

// Shared pool for in-memory database pages.
// These are used before being flushed to disk.
var pagePool = &sync.Pool{}

func allocPage() []byte {
	existing := pagePool.Get()
	if existing == nil {
		return make([]byte, PageSize)
	}
	// zero the existing page before returning it
	page := existing.(*[PageSize]byte)[:]
	for i := range page {
		page[i] = 0
	}
	return page
}

func freePage(page []byte) {
	data := (*[PageSize]byte)(unsafe.Pointer(&page[0]))
	pagePool.Put(data)
}
