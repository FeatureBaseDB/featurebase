// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package boltdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	pilosa "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/roaring"
	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"

	"runtime/pprof"
)

var _ = pprof.StartCPUProfile

var (
	// ErrTranslateStoreClosed is returned when reading from an TranslateEntryReader
	// and the underlying store is closed.
	ErrTranslateStoreClosed = errors.New("boltdb: translate store closing")

	// ErrTranslateKeyNotFound is returned when translating key
	// and the underlying store returns an empty set
	ErrTranslateKeyNotFound = errors.New("boltdb: translating key returned empty set")

	bucketKeys = []byte("keys")
	bucketIDs  = []byte("ids")
	bucketFree = []byte("free")
	freeKey    = []byte("free")
)

const (
	// snapshotExt is the file extension used for an in-process snapshot.
	snapshotExt = ".snapshotting"

	errFmtTranslateBucketNotFound = "boltdb: translate bucket '%s' not found"
)

// OpenTranslateStore opens and initializes a boltdb translation store.
func OpenTranslateStore(path, index, field string, partitionID, partitionN int, fsyncEnabled bool) (pilosa.TranslateStore, error) {
	s := NewTranslateStore(index, field, partitionID, partitionN, fsyncEnabled)
	s.Path = path
	if err := s.Open(); err != nil {
		return nil, err
	}
	return s, nil
}

// Ensure type implements interface.
var _ pilosa.TranslateStore = &TranslateStore{}

// TranslateStore is an on-disk storage engine for translating string-to-uint64 values.
// An empty string will be converted into the sentinel byte slice:
// var emptyKey = []byte{
// 	0x00, 0x00, 0x00,
// 	0x4d, 0x54, 0x4d, 0x54, // MTMT
// 	0x00,
// 	0xc2, 0xa0, // NO-BREAK SPACE
// 	0x00,
// }
type TranslateStore struct {
	mu sync.RWMutex
	db *bolt.DB

	index       string
	field       string
	partitionID int
	partitionN  int

	once    sync.Once
	closing chan struct{}

	readOnly     bool
	fsyncEnabled bool
	writeNotify  chan struct{}

	// File path to database file.
	Path string
}

// NewTranslateStore returns a new instance of TranslateStore.
func NewTranslateStore(index, field string, partitionID, partitionN int, fsyncEnabled bool) *TranslateStore {
	return &TranslateStore{
		index:        index,
		field:        field,
		partitionID:  partitionID,
		partitionN:   partitionN,
		closing:      make(chan struct{}),
		writeNotify:  make(chan struct{}),
		fsyncEnabled: fsyncEnabled,
	}
}

// Open opens the translate file.
func (s *TranslateStore) Open() (err error) {

	// add the path to the problem database if we panic handling it.
	defer func() {
		r := recover()
		if r != nil {
			panic(fmt.Sprintf("pilosa/boltdb/TranslateStore.Open(s.Path='%v') panic with '%v'", s.Path, r))
		}
	}()

	if err := os.MkdirAll(filepath.Dir(s.Path), 0750); err != nil {
		return errors.Wrapf(err, "mkdir %s", filepath.Dir(s.Path))
	} else if s.db, err = bolt.Open(s.Path, 0600, &bolt.Options{Timeout: 1 * time.Second, NoSync: !s.fsyncEnabled}); err != nil {
		return errors.Wrapf(err, "open file: %s", err)
	}

	// Initialize buckets.
	if err := s.db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(bucketKeys); err != nil {
			return err
		} else if _, err := tx.CreateBucketIfNotExists(bucketIDs); err != nil {
			return err
		} else if _, err := tx.CreateBucketIfNotExists(bucketFree); err != nil {
			return err
		}
		return nil
	}); err != nil {
		s.db.Close()
		return err
	}

	return nil
}

// Close closes the underlying database.
func (s *TranslateStore) Close() (err error) {
	s.once.Do(func() { close(s.closing) })

	if s.db != nil {
		if err := s.db.Close(); err != nil {
			return err
		}
	}
	return nil
}

// PartitionID returns the partition id the store was initialized with.
func (s *TranslateStore) PartitionID() int {
	return s.partitionID
}

// ReadOnly returns true if the store is in read-only mode.
func (s *TranslateStore) ReadOnly() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.readOnly
}

// SetReadOnly toggles whether store is in read-only mode.
func (s *TranslateStore) SetReadOnly(v bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.readOnly = v
}

// Size returns the number of bytes in the data file.
func (s *TranslateStore) Size() int64 {
	if s.db == nil {
		return 0
	}
	tx, err := s.db.Begin(false)
	if err != nil {
		return 0
	}
	defer func() { _ = tx.Rollback() }()
	return tx.Size()
}

// FindKeys looks up the ID for each key.
// Keys are not created if they do not exist.
// Missing keys are not considered errors, so the length of the result may be less than that of the input.
func (s *TranslateStore) FindKeys(keys ...string) (map[string]uint64, error) {
	result := make(map[string]uint64, len(keys))
	err := s.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(bucketKeys)
		if bkt == nil {
			return errors.Errorf(errFmtTranslateBucketNotFound, bucketKeys)
		}
		for _, key := range keys {
			id, _ := findIDByKey(bkt, key)
			if id == 0 {
				// The key does not exist.
				continue
			}

			result[key] = id
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return result, nil
}

// translateTransactionSize governs the number of writes to a single
// boltDB bucket we will make in a single db.Update(), before starting
// a new Update. We do this because Put() is quadratic, but Commit is
// expensive enough that we want to do a fair number of updates before
// paying for it.
const translateTransactionSize = 16384

// CreateKeys maps all keys to IDs, creating the IDs if they do not exist.
// If the translator is read-only, this will return an error.
func (s *TranslateStore) CreateKeys(keys ...string) (map[string]uint64, error) {
	if s.ReadOnly() {
		return nil, pilosa.ErrTranslateStoreReadOnly
	}

	written := false
	result := make(map[string]uint64, len(keys))
	idScratch := make([]byte, translateTransactionSize*8)
	for len(keys) > 0 {
		// boltdb performs badly if you write really large numbers of
		// keys all at once...
		err := s.db.Update(func(tx *bolt.Tx) error {
			keyBucket := tx.Bucket(bucketKeys)
			if keyBucket == nil {
				return errors.Errorf(errFmtTranslateBucketNotFound, bucketKeys)
			}
			idBucket := tx.Bucket(bucketIDs)
			if idBucket == nil {
				return errors.Errorf(errFmtTranslateBucketNotFound, bucketIDs)
			}
			freeBucket := tx.Bucket(bucketFree)
			if freeBucket == nil {
				return errors.Errorf(errFmtTranslateBucketNotFound, bucketFree)
			}
			puts := 0

			// we create a freeIDGetter to reduce marshalling
			getter := newFreeIDGetter(freeBucket)
			defer getter.Close()

			for idx, key := range keys {
				id, boltKey := findIDByKey(keyBucket, key)
				if id != 0 {
					result[key] = id
					continue
				}
				// see if we can re-use any IDs first
				if id = getter.GetFreeID(); id == 0 {
					id = pilosa.GenerateNextPartitionedID(s.index, maxID(tx), s.partitionID, s.partitionN)
				}
				idBytes := idScratch[puts*8 : puts*8+8]
				binary.BigEndian.PutUint64(idBytes, id)
				puts++
				if err := keyBucket.Put(boltKey, idBytes); err != nil {
					return err
				} else if err := idBucket.Put(idBytes, boltKey); err != nil {
					return err
				}
				result[key] = id
				written = true
				if puts == translateTransactionSize {
					keys = keys[idx+1:]
					return nil
				}
			}
			keys = keys[len(keys):]
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	if written {
		s.notifyWrite()
	}

	return result, nil
}

// Match finds the IDs of all keys matching a filter.
func (s *TranslateStore) Match(filter func([]byte) bool) ([]uint64, error) {
	var matches []uint64
	err := s.db.View(func(tx *bolt.Tx) error {
		// This uses the id bucket instead of the key bucket so that matches are produced in sorted order.
		idBucket := tx.Bucket(bucketIDs)
		if idBucket == nil {
			return errors.Errorf(errFmtTranslateBucketNotFound, bucketIDs)
		}

		return idBucket.ForEach(func(id, key []byte) error {
			if bytes.Equal(key, emptyKey) {
				key = nil
			}

			if filter(key) {
				matches = append(matches, btou64(id))
			}

			return nil
		})
	})
	if err != nil {
		return nil, err
	}

	return matches, nil
}

// TranslateID converts an integer ID to a string key.
// Returns a blank string if ID does not exist.
func (s *TranslateStore) TranslateID(id uint64) (string, error) {
	tx, err := s.db.Begin(false)
	if err != nil {
		return "", err
	}
	defer func() { _ = tx.Rollback() }()
	return findKeyByID(tx.Bucket(bucketIDs), id), nil
}

// TranslateIDs converts a list of integer IDs to a list of string keys.
func (s *TranslateStore) TranslateIDs(ids []uint64) ([]string, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	tx, err := s.db.Begin(false)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	bucket := tx.Bucket(bucketIDs)

	keys := make([]string, len(ids))
	for i, id := range ids {
		keys[i] = findKeyByID(bucket, id)
	}
	return keys, nil
}

// ForceSet writes the id/key pair to the store even if read only. Used by replication.
func (s *TranslateStore) ForceSet(id uint64, key string) error {
	if err := s.db.Update(func(tx *bolt.Tx) (err error) {
		if err := tx.Bucket(bucketKeys).Put([]byte(key), u64tob(id)); err != nil {
			return err
		} else if err := tx.Bucket(bucketIDs).Put(u64tob(id), []byte(key)); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	s.notifyWrite()
	return nil
}

// EntryReader returns a reader that streams the underlying data file.
func (s *TranslateStore) EntryReader(ctx context.Context, offset uint64) (pilosa.TranslateEntryReader, error) {
	ctx, cancel := context.WithCancel(ctx)
	return &TranslateEntryReader{ctx: ctx, cancel: cancel, store: s, offset: offset}, nil
}

// WriteNotify returns a channel that is closed when a new entry is written.
func (s *TranslateStore) WriteNotify() <-chan struct{} {
	s.mu.RLock()
	ch := s.writeNotify
	s.mu.RUnlock()
	return ch
}

// notifyWrite sends a write notification under write lock.
func (s *TranslateStore) notifyWrite() {
	s.mu.Lock()
	defer s.mu.Unlock()
	close(s.writeNotify)
	s.writeNotify = make(chan struct{})
}

// MaxID returns the highest id in the store.
func (s *TranslateStore) MaxID() (max uint64, err error) {
	if err := s.db.View(func(tx *bolt.Tx) error {
		max = maxID(tx)
		return nil
	}); err != nil {
		return 0, err
	}
	return max, nil
}

// WriteTo writes the contents of the store to the writer.
func (s *TranslateStore) WriteTo(w io.Writer) (int64, error) {
	tx, err := s.db.Begin(false)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()
	return tx.WriteTo(w)
}

// ReadFrom reads the content and overwrites the existing store.
func (s *TranslateStore) ReadFrom(r io.Reader) (n int64, err error) {
	// Close store.
	if err := s.Close(); err != nil {
		return 0, errors.Wrap(err, "closing store")
	}

	// Create a temporary file to snapshot to.
	snapshotPath := s.Path + snapshotExt
	file, err := os.Create(snapshotPath)
	if err != nil {
		return n, errors.Wrap(err, "creating snapshot file")
	}

	// Write payload to snapshot.
	if n, err = io.Copy(file, r); err != nil {
		file.Close()
		return n, errors.Wrap(err, "snapshot write to")
	}

	// we close the file here so we don't still have it open when trying
	// to open it in a moment.
	file.Close()

	// Move snapshot to data file location.
	if err := os.Rename(snapshotPath, s.Path); err != nil {
		return n, errors.Wrap(err, "renaming snapshot")
	}

	// Re-open the store.
	if err := s.Open(); err != nil {
		return n, errors.Wrap(err, "re-opening store")
	}

	return n, nil
}

// MaxID returns the highest id in the store.
func maxID(tx *bolt.Tx) uint64 {
	if key, _ := tx.Bucket(bucketIDs).Cursor().Last(); key != nil {
		return btou64(key)
	}
	return 0
}

type TranslateEntryReader struct {
	ctx    context.Context
	store  *TranslateStore
	offset uint64
	cancel func()
}

// Close closes the reader.
func (r *TranslateEntryReader) Close() error {
	r.cancel()
	return nil
}

// ReadEntry reads the next entry from the underlying translate store.
func (r *TranslateEntryReader) ReadEntry(entry *pilosa.TranslateEntry) error {
	// Ensure reader has not been closed before read.
	select {
	case <-r.ctx.Done():
		return r.ctx.Err()
	case <-r.store.closing:
		return ErrTranslateStoreClosed
	default:
	}

	for {
		// Obtain notification channel before read to ensure concurrency issues.
		writeNotify := r.store.WriteNotify()

		// Find next ID/key pair in transaction.
		var found bool
		if err := r.store.db.View(func(tx *bolt.Tx) error {
			// Find ID/key lookup at offset or later.
			cur := tx.Bucket(bucketIDs).Cursor()
			key, value := cur.Seek(u64tob(r.offset))
			if key == nil {
				return nil
			}

			// Copy ID & key to entry and mark as found.
			found = true
			entry.Index = r.store.index
			entry.Field = r.store.field
			entry.ID = btou64(key)
			entry.Key = string(value)

			// Update offset position.
			r.offset = entry.ID + 1

			return nil
		}); err != nil {
			return err
		} else if found {
			return nil
		}

		// If no entry found, wait for new write or reader close.
		select {
		case <-r.ctx.Done():
			return r.ctx.Err()
		case <-r.store.closing:
			return ErrTranslateStoreClosed
		case <-writeNotify:
		}
	}
}

type boltWrapper struct {
	tx *bolt.Tx
	db *bolt.DB
}

func (w *boltWrapper) Commit() error {
	if w.tx != nil {
		return w.tx.Commit()
	}
	return nil
}

func (w *boltWrapper) Rollback() {
	if w.tx != nil {
		w.tx.Rollback()
	}
}
func (s *TranslateStore) FreeIDs() (*roaring.Bitmap, error) {
	result := roaring.NewBitmap()
	err := s.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(bucketFree)
		if bkt == nil {
			return errors.Errorf(errFmtTranslateBucketNotFound, bucketKeys)
		}
		b := bkt.Get(freeKey)
		err := result.UnmarshalBinary(b)
		if err != nil {
			return err
		}
		return nil
	})
	return result, err
}
func (s *TranslateStore) MergeFree(tx *bolt.Tx, newIDs *roaring.Bitmap) error {
	bkt := tx.Bucket(bucketFree)
	b := bkt.Get(freeKey)
	buf := new(bytes.Buffer)
	if b != nil { //if existing combine with newIDs
		before := roaring.NewBitmap()
		err := before.UnmarshalBinary(b)
		if err != nil {
			return err
		}
		final := newIDs.Union(before)
		_, err = final.WriteTo(buf)
		if err != nil {
			return err
		}
	} else {
		newIDs.WriteTo(buf)
	}
	return bkt.Put(freeKey, buf.Bytes())
}

// Delete removes the lookeup pairs in order to make avialble for reuse but doesn't commit the
// transaction for that is tied to the associated rbf transaction being successful
func (s *TranslateStore) Delete(records *roaring.Bitmap) (pilosa.Commitor, error) {
	tx, err := s.db.Begin(true)
	if err != nil {
		return nil, err
	}
	keyBucket := tx.Bucket(bucketKeys)
	idBucket := tx.Bucket(bucketIDs)
	ids := records.Slice()
	for i := range ids {
		id := u64tob(ids[i])
		boltKey := idBucket.Get(id)
		err = keyBucket.Delete(boltKey)
		if err != nil {
			tx.Rollback()
			return &boltWrapper{}, err
		}
		err = idBucket.Delete(id)
		if err != nil {
			tx.Rollback()
			return &boltWrapper{}, err
		}

	}
	return &boltWrapper{tx: tx}, s.MergeFree(tx, records)
}

// emptyKey is a sentinel byte slice which stands for "" as a key.
var emptyKey = []byte{
	0x00, 0x00, 0x00,
	0x4d, 0x54, 0x4d, 0x54, // MTMT
	0x00,
	0xc2, 0xa0, // NO-BREAK SPACE
	0x00,
}

func findIDByKey(bkt *bolt.Bucket, key string) (uint64, []byte) {
	var boltKey []byte
	if key == "" {
		boltKey = emptyKey
	} else {
		boltKey = []byte(key)
	}

	if value := bkt.Get(boltKey); value != nil {
		return btou64(value), boltKey
	}
	return 0, boltKey
}

// freeIDGetter reduces the amount of marshaling required to get multiple ids
type freeIDGetter struct {
	freeBucket *bolt.Bucket
	b          *roaring.Bitmap
	changed    bool
}

// newFreeIDGetter initializes a new freeIDGetter. If at any point there is a
// failure, it returns an error.
//
// NOTE: For changes to be persisted to the bucket, you must call
// (*freeIDGetter).Close()
func newFreeIDGetter(freeBucket *bolt.Bucket) *freeIDGetter {
	g := &freeIDGetter{
		freeBucket: freeBucket,
	}
	// we ignore this value because it's okay if we dont have a bitmap just yet
	_ = g.getBitmap()
	return g
}

func (g *freeIDGetter) getBitmap() bool {
	if g.b == nil {
		// get the bitmap from freeBucket
		value := g.freeBucket.Get(freeKey)
		if value == nil {
			return false
		}
		// turn the value into a bitmap
		b := roaring.NewBitmap()
		if err := b.UnmarshalBinary(value); err != nil {
			return false
		}
		g.b = b
	}
	return true
}

// GetFreeID tries to get a free ID from the free id bucket. If at any point it
// fails to do so, it returns a 0. Otherwise, it returns the first free ID in the
// bucket
func (g *freeIDGetter) GetFreeID() (id uint64) {
	if !g.getBitmap() {
		return 0
	}
	// get the first free id
	id, ok := g.b.Min()
	if !ok {
		return 0
	}
	// remove that id from the free id bitmap
	if changed, err := g.b.RemoveN(id); changed == 0 || err != nil {
		return 0
	} else {
		g.changed = true
	}
	return id
}

// Close persists any changes to the bitmap back to the bucket and then nils the
// references for safety.
func (g *freeIDGetter) Close() error {
	if g.changed {
		// convert bitmap to binary
		buf, err := g.b.MarshalBinary()
		if err != nil {
			return errors.Wrap(err, "closing free ID Getter")
		}
		// put updated bitmap back into the freeBucket
		if err := g.freeBucket.Put(freeKey, buf); err != nil {
			return errors.Wrap(err, "closing free ID Getter")
		}
	}
	g.b = nil
	g.freeBucket = nil
	return nil
}

func findKeyByID(bkt *bolt.Bucket, id uint64) string {
	boltKey := bkt.Get(u64tob(id))
	if bytes.Equal(boltKey, emptyKey) {
		return ""
	}
	return string(boltKey)
}

// u64tob encodes v to big endian encoding.
func u64tob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

// btou64 decodes b from big endian encoding.
func btou64(b []byte) uint64 { return binary.BigEndian.Uint64(b) }
