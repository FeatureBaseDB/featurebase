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
	"context"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/boltdb/bolt"
	"github.com/pilosa/pilosa/v2"
	"github.com/pkg/errors"
)

var (
	// ErrTranslateStoreClosed is returned when reading from an TranslateEntryReader
	// and the underlying store is closed.
	ErrTranslateStoreClosed = errors.New("boltdb: translate store closing")
)

// OpenTranslateStore opens and initializes a boltdb translation store.
func OpenTranslateStore(path, index, field string, partitionID int) (pilosa.TranslateStore, error) {
	s := NewTranslateStore(index, field, partitionID)
	s.Path = path
	if err := s.Open(); err != nil {
		return nil, err
	}
	return s, nil
}

// Ensure type implements interface.
var _ pilosa.TranslateStore = &TranslateStore{}

// TranslateStore is an on-disk storage engine for translating string-to-uint64 values.
type TranslateStore struct {
	mu sync.RWMutex
	db *bolt.DB

	index       string
	field       string
	partitionID int

	once    sync.Once
	closing chan struct{}

	readOnly    bool
	writeNotify chan struct{}

	// File path to database file.
	Path string
}

// NewTranslateStore returns a new instance of TranslateStore.
func NewTranslateStore(index, field string, partitionID int) *TranslateStore {
	return &TranslateStore{
		index:       index,
		field:       field,
		partitionID: partitionID,
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
		if _, err := tx.CreateBucketIfNotExists([]byte("keys")); err != nil {
			return err
		} else if _, err := tx.CreateBucketIfNotExists([]byte("ids")); err != nil {
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

// TranslateKeys converts a string key to an integer ID.
// If key does not have an associated id then one is created.
func (s *TranslateStore) TranslateKey(key string) (id uint64, _ error) {
	// Find id by key under read lock.
	if err := s.db.View(func(tx *bolt.Tx) error {
		id = findIDByKey(tx.Bucket([]byte("keys")), key)
		return nil
	}); err != nil {
		return 0, err
	} else if id != 0 {
		return id, nil
	}

	if s.ReadOnly() {
		return 0, pilosa.ErrTranslateStoreReadOnly
	}

	// Find or create id under write lock.
	var written bool
	if err := s.db.Update(func(tx *bolt.Tx) (err error) {
		bkt := tx.Bucket([]byte("keys"))
		if id = findIDByKey(bkt, key); id != 0 {
			return nil
		} else if id, err = bkt.NextSequence(); err != nil {
			return err
		} else if err := bkt.Put([]byte(key), u64tob(id)); err != nil {
			return err
		} else if err := tx.Bucket([]byte("ids")).Put(u64tob(id), []byte(key)); err != nil {
			return err
		}
		written = true
		return nil
	}); err != nil {
		return 0, err
	}

	if written {
		s.notifyWrite()
	}

	return id, nil
}

// TranslateKeys converts a string key to an integer ID.
// If key does not have an associated id then one is created.
func (s *TranslateStore) TranslateKeys(keys []string) (ids []uint64, _ error) {
	if len(keys) == 0 {
		return nil, nil
	}

	// Allocate slice for ID mapping.
	ids = make([]uint64, len(keys))

	// Find ids by key under read lock.
	var found int
	if err := s.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket([]byte("keys"))
		for i, key := range keys {
			if id := findIDByKey(bkt, key); id != 0 {
				ids[i] = id
				found++
			}
		}
		return nil
	}); err != nil {
		return nil, err
	} else if found == len(keys) {
		return ids, nil
	}

	if s.ReadOnly() {
		return ids, pilosa.ErrTranslateStoreReadOnly
	}

	// Find or create ids under write lock if any keys were not found.
	var written bool
	if err := s.db.Update(func(tx *bolt.Tx) (err error) {
		bkt := tx.Bucket([]byte("keys"))
		for i, key := range keys {
			if ids[i] != 0 {
				continue
			}

			if ids[i] = findIDByKey(bkt, key); ids[i] != 0 {
				continue
			} else if ids[i], err = bkt.NextSequence(); err != nil {
				return err
			} else if err := bkt.Put([]byte(key), u64tob(ids[i])); err != nil {
				return err
			} else if err := tx.Bucket([]byte("ids")).Put(u64tob(ids[i]), []byte(key)); err != nil {
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
	return findKeyByID(tx.Bucket([]byte("ids")), id), nil
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

	keys := make([]string, len(ids))
	for i, id := range ids {
		keys[i] = findKeyByID(tx.Bucket([]byte("ids")), id)
	}
	return keys, nil
}

// ForceSet writes the id/key pair to the store even if read only. Used by replication.
func (s *TranslateStore) ForceSet(id uint64, key string) error {
	if err := s.db.Update(func(tx *bolt.Tx) (err error) {
		if err := tx.Bucket([]byte("keys")).Put([]byte(key), u64tob(id)); err != nil {
			return err
		} else if err := tx.Bucket([]byte("ids")).Put(u64tob(id), []byte(key)); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	s.notifyWrite()
	return nil
}

// Reader returns a reader that streams the underlying data file.
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
		if key, _ := tx.Bucket([]byte("ids")).Cursor().Last(); key != nil {
			max = btou64(key)
		}
		return nil
	}); err != nil {
		return 0, err
	}
	return max, nil
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
			cur := tx.Bucket([]byte("ids")).Cursor()
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

func findIDByKey(bkt *bolt.Bucket, key string) uint64 {
	if value := bkt.Get([]byte(key)); value != nil {
		return btou64(value)
	}
	return 0
}

func findKeyByID(bkt *bolt.Bucket, id uint64) string {
	return string(bkt.Get(u64tob(id)))
}
