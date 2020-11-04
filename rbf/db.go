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
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/benbjohnson/immutable"
	"github.com/pilosa/pilosa/v2/syswrap"

	rbfcfg "github.com/pilosa/pilosa/v2/rbf/cfg"
)

var (
	ErrClosed = errors.New("rbf: database closed")
)

// DB options like 	MaxSize, FsyncEnabled, DoAllocZero
// can be set before calling DB.Open().
type DB struct {
	cfg rbfcfg.Config

	data        []byte           // mmap data
	file        *os.File         // file descriptor
	rootRecords []*RootRecord    // cached root records
	pageMap     *immutable.Map   // pgno-to-WALID mapping
	txs         map[*Tx]struct{} // active transactions
	opened      bool             // true if open

	wcache   []byte       // wal write cache
	segments []WALSegment // write-ahead log

	mu     sync.RWMutex // general mutex
	rwmu   sync.Mutex   // mutex for restricting single writer
	exclmu sync.RWMutex // mutex for locking out everyone but a single writer

	// Path represents the path to the database file.
	Path string

	lastCheckpoint time.Time
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
		pageMap: immutable.NewMap(&uint32Hasher{}),
		wcache:  make([]byte, cfg.MaxWALSegmentFileSize+PageSize),
		Path:    path,
	}
	return db
}

// DataPath returns the path to the data file for the DB.
func (db *DB) DataPath() string {
	return filepath.Join(db.Path, "data")
}

// WALPath returns the path to the WAL directory.
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

	// Open read-only mmap.
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

	// Ensure WAL directory exists.
	if err := os.MkdirAll(db.WALPath(), 0777); err != nil {
		return fmt.Errorf("create wal dir: %w", err)
	}

	db.opened = true

	// Open write-ahead log & checkpoint to the end since no transactions are open.
	if err := db.openWALSegments(); err != nil {
		return fmt.Errorf("wal open: %w", err)
	} else if err := db.checkpoint(true); err != nil {
		return fmt.Errorf("checkpoint: %w", err)
	}

	return nil
}

func (db *DB) openWALSegments() error {
	fis, err := ioutil.ReadDir(db.WALPath())
	if err != nil {
		return fmt.Errorf("read dir: %w", err)
	}

	// Open all WAL segments.
	for _, fi := range fis {
		if filepath.Ext(fi.Name()) != ".wal" {
			continue
		}

		segment := db.NewWALSegment(filepath.Join(db.WALPath(), fi.Name()))
		if err := segment.Open(); err != nil {
			_ = db.closeWALSegments()
			return err
		}
		db.segments = append(db.segments, segment)
	}

	// Truncate everything after the last successful meta page.
	if walID, err := findLastWALMetaPage(db.segments); err != nil {
		return err
	} else if db.segments, err = db.truncateWALAfter(db.segments, walID); err != nil {
		return err
	}

	return nil
}

// updateWALSegment updates or adds a segment.
func (db *DB) updateWALSegment(s WALSegment) {
	segments := make([]WALSegment, len(db.segments), len(db.segments)+1)
	copy(segments, db.segments)

	// Find the matching segment using the path.
	segment := walSegmentByPath(segments, s.Path)

	// Update existing segment if it already exists.
	// Otherwise append segment to the end.
	if segment != nil {
		*segment = s
	} else {
		assert(len(segments) == 0 || segments[len(segments)-1].MinWALID < s.MinWALID)
		segments = append(segments, s)
	}

	// Replace DB segment list.
	db.segments = segments
}

// checkpoint moves WAL segments to the main DB file.
// Must be called by a write transaction while under db.mu lock.
func (db *DB) checkpoint(exclusive bool) error {
	if !db.opened {
		return nil
	} else if len(db.txs) > 0 {
		return nil // skip if transactions open
	}

	// Determine last checkpointed WAL ID.
	page, err := db.readDBPage(0)
	if err != nil {
		return err
	}
	walID := readMetaWALID(page)
	// INVAR: walID represents everything already in the DB, and
	// any wal page k > walID is in the WAL not the DB.

	// Loop over each transaction

	// We could be looking at a recovery. When there is no new
	// meta page further down in the WAL, then there was power
	// failure or the process was killed. So we have to search
	// and find the next meta page, if present.
	//
	// Read ahead to the next meta page, if present, in the WAL. If we
	// we find it, then we must ensure the pages between
	// [walID, next_meta_page.walID] are committed.
	// If there is NOT another meta page after, then those writes get
	// rolled back.
	walID++
	for {
		// Determine last page of transaction.
		metaWALID, err := findNextWALMetaPage(db.segments, walID)
		if err == ErrNoMetaFound {
			break
		} else if err != nil {
			return err
		}

		// Loop over pages in the transaction.
		for ; walID <= metaWALID; walID++ {
			page, err := readWALPage(db.segments, walID)
			if err != nil {
				return err
			}
			isBitmapHeader := IsBitmapHeader(page)

			// Determine page number. Meta pages are always on zero & bitmap
			// headers specify the page number of the next page in the WAL.
			// All other pages have their page number in the page data.
			var pgno uint32
			if isBitmapHeader {
				pgno, walID = readPageNo(page), walID+1 // skip next page
			} else if !IsMetaPage(page) {
				pgno = readPageNo(page)
			}

			// Ensure we actually read the bitmap data in when we checkpoint.
			// NOTE: The walID variable is incremented above in the pgno check.
			if isBitmapHeader {
				if page, err = readWALPage(db.segments, walID); err != nil {
					return err
				}
			}

			// TODO: address this problem: if we write a WAL meta page to database page 0 before fsyncing
			// the transactions updates from the WAL into the DB, then (upon
			// power failure in the middle of a fsync), the meta page might
			// get updated before all of the databases pages that included the changes
			// that the meta page represents. The only way to have a strict
			// ordering that the meta page is updated only after the other
			// pages is to fsync it in a 2nd fsync that follows the
			// the first. SSDs and HDs both exhibit these "unsynchronized writes".
			// reference https://www.usenix.org/system/files/conference/fast13/fast13-final80.pdf
			//
			// needed pattern:
			// 1) write  tx-content pages;
			// 2) fsync the tx-content pages;
			// 3) write meta page;
			// 4) fsync the meta page.

			// The OS can also be inserting fsyncs at any point (e.g. due to memory pressure)
			// and so we have to be certain that the meta page is written after a separate fsync.

			// Write page data into main db file.
			if err := db.writeDBPage(pgno, page); err != nil {
				return err
			}
		}
	}

	// Ensure WAL pages are fully copied & synced to DB file.
	if err := db.fsync(db.file); err != nil {
		return fmt.Errorf("db file sync: %w", err)
	}
	db.pageMap = immutable.NewMap(&uint32Hasher{})

	// Remove WAL segments that have been checkpointed.
	for _, segment := range db.segments {
		if err := segment.Close(); err != nil {
			return err
		} else if err := os.Remove(segment.Path); err != nil {
			return err
		}
	}
	db.segments = nil

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
		if e := db.file.Close(); e != nil && err == nil {
			err = e
		}
		db.file = nil
	}

	if e := db.closeWALSegments(); e != nil && err == nil {
		err = e
	}

	return err
}

// closeWALSegments closes the WAL and all its segments.
func (db *DB) closeWALSegments() (err error) {
	for _, s := range db.segments {
		if e := s.Close(); e != nil && err == nil {
			err = e
		}
	}
	db.segments = nil
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
	for _, record := range records {
		// Fetch cursor for bitmap.
		cur, err := tx.Cursor(record.Name)
		if err != nil {
			return false, err
		}
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
	return walSize(db.segments) + fi.Size(), nil
}

// WALSize returns the size of all WAL segments, in bytes.
func (db *DB) WALSize() int64 {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return walSize(db.segments)
}

// WALSegments returns the WAL segments currently on the DB.
func (db *DB) WALSegments() []WALSegment {
	db.mu.RLock()
	defer db.mu.RUnlock()
	other := make([]WALSegment, len(db.segments))
	copy(other, db.segments)
	return other
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
	return db.begin(writable, false)
}

// BeginWithExclusiveLock starts a new transaction with an exclusive lock.
//
// This waits for all read transactions to finish and disallows any other
// transactions on the database. All WAL writes are flushed to disk and page
// writes during this transaction are written directly to the database file.
//
// Note that because page writes are direct, write failures can corrupt the
// database. This should only be used during bulk loading of data.
func (db *DB) BeginWithExclusiveLock() (_ *Tx, err error) {
	return db.begin(true, true)
}

func (db *DB) begin(writable, exclusive bool) (_ *Tx, err error) {
	if exclusive {
		assert(writable) // exclusive transactions must be writable
	}

	if exclusive {
		db.exclmu.Lock()
	} else {
		db.exclmu.RLock()
	}

	// Ensure only one writable transaction at a time.
	if writable {
		db.rwmu.Lock()
	}

	// This local function is called at exit points that occur before we can
	// call Rollback() which would normally release these locks.
	cleanup := func() {
		if exclusive {
			db.exclmu.Unlock()
		} else {
			db.exclmu.RUnlock()
		}

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

	// Flush all WAL writes to disk before an exclusive writer so that we can
	// work directly with the on-disk database.
	if exclusive {
		if err := db.checkpoint(true); err != nil {
			cleanup()
			db.mu.Unlock()
			return nil, err
		}
	}

	tx := &Tx{
		db:          db,
		rootRecords: db.rootRecords,
		pageMap:     db.pageMap,
		writable:    writable,
		exclusive:   exclusive,

		DeleteEmptyContainer: true,
	}
	if writable {
		tx.wcache = db.wcache[:0]
	}

	// Copy list of WAL segments so they can be altered by the tx.
	// Add last segment to the list of segments that will be updated/added.
	if len(db.segments) != 0 {
		tx.segments = make([]WALSegment, len(db.segments))
		copy(tx.segments, db.segments)
		tx.updatedSegmentPaths = []string{tx.segments[len(tx.segments)-1].Path}
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

	db.mu.Unlock()
	return tx, nil
}

// removeTx removes an active transaction from the database.
func (db *DB) removeTx(tx *Tx) error {
	if tx.exclusive {
		db.exclmu.Unlock()
	} else {
		db.exclmu.RUnlock()
	}

	// Release writer lock if tx is writable.
	if tx.writable {
		tx.db.rwmu.Unlock()
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	delete(tx.db.txs, tx)

	// Write pages from WAL to DB.
	// TODO(bbj): Move this to an async goroutine.
	// TODO(jea): Make the time-based checkpointing work at all, and update the
	// comment in cfg/cfg.go for CheckpointEveryDur. Seems that
	// wal.go readWALPage() can receive a request for a walID that
	// comes before the segments it is passed if we do not
	// checkpoint eagerly.
	if tx.writable {
		if db.cfg.CheckpointEveryDur == 0 || time.Since(db.lastCheckpoint) > db.cfg.CheckpointEveryDur {
			if err := db.checkpoint(false); err != nil {
				return fmt.Errorf("checkpoint: %w", err)
			}
			db.lastCheckpoint = time.Now()
		}
	}

	// Disassociate from db.
	tx.db = nil

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

func (db *DB) readMetaPage() ([]byte, error) {
	if walID, ok := db.pageMap.Get(uint32(0)); ok {
		return readWALPage(db.segments, walID.(int64))
	}
	return db.readDBPage(0)
}
