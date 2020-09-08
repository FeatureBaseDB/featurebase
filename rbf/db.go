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

	"github.com/benbjohnson/immutable"
	"github.com/pilosa/pilosa/v2/syswrap"
)

var (
	ErrClosed = errors.New("rbf: database closed")
)

const (
	// Maximum size of a single WAL segment.
	// May exceed by one page if last page is a bitmap header + bitmap.
	MaxWALSegmentFileSize = 10 * (1 << 20)
)

type DB struct {
	data        []byte           // mmap data
	file        *os.File         // file descriptor
	segments    []*WALSegment    // write-ahead log
	rootRecords []*RootRecord    // cached root records
	pageMap     *immutable.Map   // pgno-to-WALID mapping
	txs         map[*Tx]struct{} // active transactions
	opened      bool             // true if open

	mu     sync.RWMutex // general mutex
	rwmu   sync.Mutex   // mutex for restricting single writer
	exclmu sync.RWMutex // mutex for locking out everyone but a single writer

	// Path represents the path to the database file.
	Path string

	// The maximum allowed database size. Required by mmap.
	MaxSize int64
}

// NewDB returns a new instance of DB.
func NewDB(path string) *DB {
	db := &DB{
		txs:     make(map[*Tx]struct{}),
		pageMap: immutable.NewMap(&uint32Hasher{}),
		Path:    path,
		MaxSize: DefaultMaxSize,
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
	} else if db.data, err = syswrap.Mmap(int(f.Fd()), 0, int(db.MaxSize), syscall.PROT_READ, syscall.MAP_SHARED); err != nil {
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

		segment := NewWALSegment(filepath.Join(db.WALPath(), fi.Name()))
		if err := segment.Open(); err != nil {
			_ = db.closeWALSegments()
			return err
		}
		db.segments = append(db.segments, segment)
	}

	// Truncate everything after the last successful meta page.
	if walID, err := db.findLastWALMetaPage(); err != nil {
		return err
	} else if err := db.truncateWALAfter(walID); err != nil {
		return err
	}

	return nil
}

// checkpoint copies pages from WAL segments into the main DB file. This can
// only copy pages that aren't in use by an active transaction. The page map
// is rebuilt as well for all WAL pages still in use.
//
// If exclusive is true, all WAL writes are flushed to disk.
func (db *DB) checkpoint(exclusive bool) error {
	if !db.opened {
		return nil
	}

	// Determine last checkpointed WAL ID.
	page, err := db.readPage(nil, 0)
	if err != nil {
		return err
	}
	walID := readMetaWALID(page)
	maxCheckpointedWALID := walID

	// Determine the high water mark for WAL pages that can be copied.
	minActiveWALID := db.minActiveWALID()

	// Loop over each transaction
	walID++
	pageMap := immutable.NewMap(&uint32Hasher{})
	for {
		// Determine last page of transaction.
		metaWALID, err := db.findNextWALMetaPage(walID)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		// Loop over pages in the transaction.
		for ; walID <= metaWALID; walID++ {
			canCheckpoint := exclusive || minActiveWALID == 0 || walID <= minActiveWALID

			page, err := db.readWALPage(walID)
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

			// If we can no longer checkpoint, map the page number to the WAL page.
			if !canCheckpoint {
				pageMap = pageMap.Set(pgno, walID)
				continue
			}

			// Ensure we actually read the bitmap data in when we checkpoint.
			// NOTE: The walID variable is incremented above in the pgno check.
			if isBitmapHeader {
				if page, err = db.readWALPage(walID); err != nil {
					return err
				}
			}

			// Write page data into main db file.
			if err := db.writePage(pgno, page); err != nil {
				return err
			}

			// Track highest WALID that has been checkpointed back to disk.
			if IsMetaPage(page) {
				maxCheckpointedWALID = walID
			}
		}
	}

	// Remove WAL segments that have been checkpointed.
	if maxCheckpointedWALID != 0 {
		for len(db.segments) > 0 {
			segment := db.segments[0]
			if segment.MaxWALID() > maxCheckpointedWALID {
				break
			}

			segpath := segment.Path()
			if err := segment.Close(); err != nil {
				return err
			} else if err := os.Remove(segpath); err != nil {
				return err
			}
			db.segments, db.segments[0] = db.segments[1:], nil
		}
	}

	// Ensure all segments are flushed and there is no remapped pages.
	if exclusive {
		assert(len(db.segments) == 0)
		assert(pageMap.Len() == 0)
	}

	db.pageMap = pageMap
	return nil
}

// truncateWALAfter removes all pages in the WAL after walID.
func (db *DB) truncateWALAfter(walID int64) error {
	for i := len(db.segments) - 1; i >= 0; i-- {
		segment := db.segments[i]
		if segment.MaxWALID() <= walID {
			break
		}

		// Drop entire segment if all pages are after WAL ID.
		if walID < segment.MinWALID() {
			if err := segment.Close(); err != nil {
				return err
			} else if err := os.Remove(segment.Path()); err != nil {
				return err
			}
			db.segments, db.segments[i] = db.segments[:len(db.segments)-1], nil
			continue
		}

		// If we only remove some of the WAL pages then truncate and exit
		// since segments before this will retain all their pages.
		return segment.TruncateAfter(walID)
	}
	return nil
}

func (db *DB) findNextWALMetaPage(walID int64) (metaWALID int64, err error) {
	maxWALID := db.maxWALID()

	for ; walID <= maxWALID; walID++ {
		// Read page data from WAL and return if it is a meta page.
		page, err := db.readWALPage(walID)
		if err != nil {
			return walID, err
		} else if IsMetaPage(page) {
			return walID, nil
		}

		// Skip over next page if this is a bitmap header.
		if IsBitmapHeader(page) {
			walID++
		}
	}

	return -1, io.EOF
}

func (db *DB) findLastWALMetaPage() (walID int64, err error) {
	if len(db.segments) == 0 {
		return 0, nil
	}

	var maxMetaWALID int64
	maxWALID := db.maxWALID()
	for walID := db.minWALID(); walID <= maxWALID; walID++ {
		if page, err := db.readWALPage(walID); err != nil {
			return walID, err
		} else if IsBitmapHeader(page) {
			walID++ // skip next page for bitmap headers
		} else if IsMetaPage(page) {
			maxMetaWALID = walID // save max meta WAL ID
		}
	}
	return maxMetaWALID, nil
}

// minActiveWALID returns the lowest WAL ID in use by any active transaction.
// Returns 0 if no transactions are active.
func (db *DB) minActiveWALID() int64 {
	var walID int64
	for tx := range db.txs {
		if walID == 0 || walID > tx.walID {
			walID = tx.walID
		}
	}
	return walID
}

// ActiveWALSegment returns the most recent WAL segment.
func (db *DB) ActiveWALSegment() *WALSegment {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.activeWALSegment()
}

func (db *DB) activeWALSegment() *WALSegment {
	if len(db.segments) == 0 {
		return nil
	}
	return db.segments[len(db.segments)-1]
}

// MinWALID returns the lowest WAL ID available in the WAL.
func (db *DB) MinWALID() int64 {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.minWALID()
}

func (db *DB) minWALID() int64 {
	if len(db.segments) == 0 {
		return 0
	}
	return db.segments[0].MinWALID()
}

// MaxWALID returns the highest WAL ID available in the WAL.
func (db *DB) MaxWALID() int64 {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.maxWALID()
}

func (db *DB) maxWALID() int64 {

	if len(db.segments) == 0 {
		return 0
	}
	s := db.segments[len(db.segments)-1]
	return s.MaxWALID()
}

// WALPageN returns the number of pages across all segments.
func (db *DB) WALPageN() int64 {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var n int64
	for _, s := range db.segments {
		n += int64(s.PageN())
	}
	return n
}

// SyncWAL flushes the active segment to disk.
func (db *DB) SyncWAL() error {
	if s := db.ActiveWALSegment(); s != nil {
		return s.Sync()
	}
	return nil
}

// readWALPage reads a single page at the given WAL ID.
func (db *DB) readWALPage(walID int64) ([]byte, error) {
	// TODO(BBJ): Binary search for segment.
	for _, s := range db.segments {
		if walID >= s.MinWALID() && walID <= s.MaxWALID() {
			return s.ReadWALPage(walID)
		}
	}
	return nil, fmt.Errorf("cannot find segment containing WAL page: %d", walID)
}

func (db *DB) writeWALPage(page []byte, isMeta bool) (walID int64, err error) {

	if err := db.ensureWritableWALSegment(); err != nil {
		return 0, err
	}
	return db.activeWALSegment().WriteWALPage(page, isMeta)
}

func (db *DB) writeBitmapPage(pgno uint32, page []byte) (walID int64, err error) {

	if err := db.ensureWritableWALSegment(); err != nil {
		return 0, err
	}

	// Write header page for next bitmap page.
	buf := make([]byte, PageSize)
	writePageNo(buf[:], pgno)
	writeFlags(buf[:], PageTypeBitmapHeader)
	// TODO(BBJ): Write checksum.
	if _, err := db.activeWALSegment().WriteWALPage(buf, false); err != nil {
		return 0, fmt.Errorf("write bitmap header: %w", err)
	}

	// Write the bitmap page and return its WALID.
	return db.activeWALSegment().WriteWALPage(page, false)
}

func (db *DB) ensureWritableWALSegment() error {
	if s := db.activeWALSegment(); s != nil && s.Size() < MaxWALSegmentFileSize {
		return nil
	}
	return db.addWALSegment()
}

// addWALSegment appends a new, writable segment and closing an existing segments for write.
func (db *DB) addWALSegment() error {

	// If we have a current active WAL segment then close it and start the
	// next segment from the next WAL ID. If there is no existing WAL segments,
	// read the last checkpointed WAL ID from the DB and start after that.
	var base int64
	if s := db.activeWALSegment(); s != nil {
		base = s.MaxWALID() + 1
		if err := s.CloseForWrite(); err != nil {
			return err
		}
	} else {
		page, err := db.readPage(db.pageMap, 0)
		if err != nil {
			return err
		}
		base = readMetaWALID(page) + 1
	}

	// Create new segment file.
	s := NewWALSegment(filepath.Join(db.WALPath(), FormatWALSegmentPath(base)))
	if err := s.Open(); err != nil {
		return fmt.Errorf("add wal segment: %w", err)
	}
	db.segments = append(db.segments, s)

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
	return err
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

// WALSize returns the size of all WAL segments, in bytes.
func (db *DB) WALSize() int64 {

	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.walSize()
}

func (db *DB) walSize() int64 {

	var sz int64
	for _, s := range db.segments {
		sz += s.Size()
	}
	return sz
}

// WALSegments returns the WAL segments currently on the DB.
// This should only be used for debugging & testing purposes.
func (db *DB) WALSegments() []*WALSegment {

	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.segments
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
	defer db.mu.Unlock()

	if !db.opened {
		cleanup()
		return nil, ErrClosed
	}

	// Flush all WAL writes to disk before an exclusive writer so that we can
	// work directly with the on-disk database.
	if exclusive {
		if err := db.checkpoint(true); err != nil {
			cleanup()
			return nil, err
		}
	}

	tx := &Tx{
		db:          db,
		rootRecords: db.rootRecords,
		pageMap:     db.pageMap,
		writable:    writable,
		exclusive:   exclusive,
	}

	// Copy meta page into transaction's buffer.
	// This page is only written at the end of a dirty transaction.
	page, err := db.readPage(db.pageMap, 0)
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	copy(tx.meta[:], page)

	// Attach starting WAL ID to transaction.
	tx.walID = readMetaWALID(tx.meta[:])

	// Track transaction with the DB.
	db.txs[tx] = struct{}{}

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

	// Write pages from WAL to DB.
	// TODO(bbj): Move this to an async goroutine.
	if tx.writable {
		if err := db.checkpoint(false); err != nil {
			return err
		}
	}

	delete(tx.db.txs, tx)

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

// writePage writes a page to the data file.
func (db *DB) writePage(pgno uint32, page []byte) error {

	_, err := db.file.WriteAt(page, int64(pgno)*PageSize)
	return err
}

func (db *DB) readPage(pageMap *immutable.Map, pgno uint32) ([]byte, error) {
	// Check if page is currently in WAL.
	if pageMap != nil {
		if walID, ok := pageMap.Get(pgno); ok {
			return db.readWALPage(walID.(int64))
		}
	}

	// Otherwise read from the data file.
	offset := int64(pgno) * PageSize
	return db.data[offset : offset+PageSize], nil
}
