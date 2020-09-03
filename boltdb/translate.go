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

package boltdb

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/boltdb/bolt"
	"github.com/pilosa/pilosa/v2"
	"github.com/pkg/errors"
	"github.com/zeebo/blake3"
)

var (
	// ErrTranslateStoreClosed is returned when reading from an TranslateEntryReader
	// and the underlying store is closed.
	ErrTranslateStoreClosed = errors.New("boltdb: translate store closing")

	// ErrTranslateKeyNotFound is returned when translating key
	// and the underlying store returns an empty set
	ErrTranslateKeyNotFound = errors.New("boltdb: translating key returned empty set")

	bucketKeys = []byte("keys")
	bucketIDs  = []byte("ids")
)

const (
	// snapshotExt is the file extension used for an in-process snapshot.
	snapshotExt = ".snapshotting"

	errFmtTranslateBucketNotFound = "boltdb: translate bucket '%s' not found"
)

// OpenTranslateStore opens and initializes a boltdb translation store.
func OpenTranslateStore(path, index, field string, partitionID, partitionN int) (pilosa.TranslateStore, error) {
	s := NewTranslateStore(index, field, partitionID, partitionN)
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

	readOnly    bool
	writeNotify chan struct{}

	// File path to database file.
	Path string
}

// NewTranslateStore returns a new instance of TranslateStore.
func NewTranslateStore(index, field string, partitionID, partitionN int) *TranslateStore {
	return &TranslateStore{
		index:       index,
		field:       field,
		partitionID: partitionID,
		partitionN:  partitionN,
		closing:     make(chan struct{}),
		writeNotify: make(chan struct{}),
	}
}

// Open opens the translate file.
func (s *TranslateStore) Open() (err error) {
	if err := os.MkdirAll(filepath.Dir(s.Path), 0777); err != nil {
		return errors.Wrapf(err, "mkdir %s", filepath.Dir(s.Path))
	} else if s.db, err = bolt.Open(s.Path, 0666, &bolt.Options{Timeout: 1 * time.Second}); err != nil {
		return errors.Wrapf(err, "open file: %s", err)
	}

	// Initialize buckets.
	if err := s.db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(bucketKeys); err != nil {
			return err
		} else if _, err := tx.CreateBucketIfNotExists(bucketIDs); err != nil {
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

// TranslateKey converts a string key to an integer ID.
// If key does not have an associated id then one is created.
func (s *TranslateStore) TranslateKey(key string, writable bool) (uint64, error) {
	ids, err := s.translateKeys([]string{key}, writable)
	if err != nil {
		return 0, err
	}
	if len(ids) == 0 {
		// this should not happen
		return 0, ErrTranslateKeyNotFound
	}
	return ids[0], nil
}

// TranslateKeys converts a slice of string keys to a slice of integer IDs.
// If a key does not have an associated id then one is created.
func (s *TranslateStore) TranslateKeys(keys []string, writable bool) ([]uint64, error) {
	return s.translateKeys(keys, writable)
}

func (s *TranslateStore) translateKeys(keys []string, writable bool) ([]uint64, error) {
	found := 0
	ids := make([]uint64, len(keys))
	err := s.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(bucketKeys)
		if bkt == nil {
			return errors.Errorf(errFmtTranslateBucketNotFound, bucketKeys)
		}
		for i, key := range keys {
			if id, _ := findIDByKey(bkt, key); id != 0 {
				ids[i] = id
				found++
			}
		}
		return nil

	})
	if err != nil {
		return nil, err
	}
	if found == len(keys) {
		return ids, nil
	}
	if s.ReadOnly() {
		return ids, pilosa.ErrTranslateStoreReadOnly
	}
	if !writable {
		return nil, pilosa.ErrTranslatingKeyNotFound
	}
	// Find or create ids under write lock if any keys were not found.
	var written bool
	if err := s.db.Update(func(tx *bolt.Tx) (err error) {
		bkt := tx.Bucket(bucketKeys)
		for i, key := range keys {
			if ids[i] != 0 {
				continue
			}
			var boltKey []byte
			if ids[i], boltKey = findIDByKey(bkt, key); ids[i] != 0 {
				continue
			}
			ids[i] = pilosa.GenerateNextPartitionedID(s.index, maxID(tx), s.partitionID, s.partitionN)
			if err := bkt.Put(boltKey, u64tob(ids[i])); err != nil {
				return err
			} else if err := tx.Bucket(bucketIDs).Put(u64tob(ids[i]), boltKey); err != nil {
				return err
			}
			written = true
		}
		return nil
	}); err != nil {
		return nil, err
	}
	if written {
		s.notifyWrite()
	}
	return ids, nil
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

func findKeyByID(bkt *bolt.Bucket, id uint64) string {
	boltKey := bkt.Get(u64tob(id))
	if bytes.Equal(boltKey, emptyKey) {
		return ""
	}
	return string(boltKey)
}

func (s *TranslateStore) ComputeTranslatorSummary() (sum *pilosa.TranslatorSummary, err error) {
	sum = &pilosa.TranslatorSummary{}
	hasher := blake3.New()

	err = s.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(bucketKeys)
		if bkt == nil {
			panic("bucketKeys not found")
		}

		cur := bkt.Cursor()
		for k, v := cur.First(); k != nil; k, v = cur.Next() {
			input := append(k, v...)
			_, _ = hasher.Write(input)
			sum.KeyCount++
		}

		bkt = tx.Bucket(bucketIDs)
		if bkt == nil {
			panic("bucketIDs not found")
		}

		cur = bkt.Cursor()
		for k, v := cur.First(); k != nil; k, v = cur.Next() {
			input := append(k, v...)
			_, _ = hasher.Write(input)
			sum.IDCount++
		}

		return nil
	})
	if err != nil {
		return nil, err
	}
	var buf [16]byte
	_, _ = hasher.Digest().Read(buf[0:])
	sum.Checksum = string(buf[:])
	return sum, nil
}
