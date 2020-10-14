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
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/boltdb/bolt"
	"github.com/pilosa/pilosa/v2"
	"github.com/pkg/errors"
	"github.com/zeebo/blake3"

	"runtime/pprof"
)

var _ = ioutil.TempFile
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

func (s *TranslateStore) GetStorePath() string {
	return s.Path
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

	// add the path to the problem database if we panic handling it.
	defer func() {
		r := recover()
		if r != nil {
			panic(fmt.Sprintf("pilosa/boltdb/TranslateStore.Open(s.Path='%v') panic with '%v'", s.Path, r))
		}
	}()

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
// If key does not have an associated id then one is created, unless writable is false,
// then the function will return the error pilosa.ErrTranslatingKeyNotFound.
func (s *TranslateStore) TranslateKey(key string, writable bool) (uint64, error) {
	ids, err := s.translateKeys([]string{key}, writable)
	if err != nil {
		return 0, err
	}
	if len(ids) == 0 {
		return 0, ErrTranslateKeyNotFound
	}
	return ids[0], nil
}

// TranslateKeys converts a slice of string keys to a slice of integer IDs.
// If a key does not have an associated id then one is created, unless writable is false,
// then the function will return the error pilosa.ErrTranslatingKeyNotFound.
func (s *TranslateStore) TranslateKeys(keys []string, writable bool) ([]uint64, error) {
	return s.translateKeys(keys, writable)
}

func (s *TranslateStore) translateKeys(keys []string, writable bool) ([]uint64, error) {
	ids := make([]uint64, 0, len(keys))

	if s.ReadOnly() || !writable {
		found := 0
		if err := s.db.View(func(tx *bolt.Tx) error {
			bkt := tx.Bucket(bucketKeys)
			if bkt == nil {
				return errors.Errorf(errFmtTranslateBucketNotFound, bucketKeys)
			}
			for _, key := range keys {
				if id, _ := findIDByKey(bkt, key); id != 0 {
					ids = append(ids, id)
					found++
				}
			}
			return nil
		}); err != nil {
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
		return nil, nil
	}

	// Find or create ids under write lock if any keys were not found.
	var written bool
	if err := s.db.Update(func(tx *bolt.Tx) (err error) {
		bkt := tx.Bucket(bucketKeys)
		for _, key := range keys {
			id, boltKey := findIDByKey(bkt, key)
			if id != 0 {
				ids = append(ids, id)
				continue
			}
			id = pilosa.GenerateNextPartitionedID(s.index, maxID(tx), s.partitionID, s.partitionN)
			if err := bkt.Put(boltKey, u64tob(id)); err != nil {
				return err
			} else if err := tx.Bucket(bucketIDs).Put(u64tob(id), boltKey); err != nil {
				return err
			}
			ids = append(ids, id)
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

func (s *TranslateStore) ComputeTranslatorSummaryRows() (sum *pilosa.TranslatorSummary, err error) {
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

func (s *TranslateStore) ComputeTranslatorSummaryCols(partitionID int, topo *pilosa.Topology) (sum *pilosa.TranslatorSummary, err error) {
	sum = &pilosa.TranslatorSummary{}
	hasher := blake3.New()

	if partitionID != s.partitionID {
		panic(fmt.Sprintf("inconsistent partitionID arg %v with TranslateStore.paritionID %v", partitionID, s.partitionID))
	}
	firstPrimary := topo.PrimaryNodeIndex(partitionID)

	err = s.db.View(func(tx *bolt.Tx) error {

		bkt := tx.Bucket(bucketKeys) // key -> id
		if bkt == nil {
			panic("bucketKeys not found")
		}

		cur := bkt.Cursor()
		for k, v := cur.First(); k != nil; k, v = cur.Next() {
			input := append(k, v...)
			//vv("55555 ComputeTranslatorSummaryCols(partitionID=%v, path='%v'), k='%v', v=%x", partitionID, s.Path, string(k), v)
			_, _ = hasher.Write(input)
			sum.KeyCount++
		}

		bkt = tx.Bucket(bucketIDs) // id -> key
		if bkt == nil {
			panic("bucketIDs not found")
		}

		cur = bkt.Cursor()
		for k, v := cur.First(); k != nil; k, v = cur.Next() {

			// should the primary be the same for each key in this partition?
			id := btou64(k)
			shard := id / pilosa.ShardWidth

			ks := string(v)
			primary := topo.GetPrimaryForColKeyTranslation(s.index, ks)
			if firstPrimary < 0 {
				firstPrimary = primary
			} else {
				if primary != firstPrimary {
					panic(fmt.Sprintf("s.index='%v' primary (%v) != firstPrimary (%v); key='%v', id=%v, shard=%v; partitionID=%v; topo='%v'", s.index, primary, firstPrimary, ks, id, shard, partitionID, topo.String()))
				}
			}

			// Verify the invariant that the primaries agree. Just a sanity check.
			primaryForShard := topo.GetPrimaryForShardReplication(s.index, shard)
			if primaryForShard != firstPrimary {
				panic(fmt.Sprintf("primaryForShard (%v) != firstPrimary (%v); key='%v', id=%v, shard=%v; partitionID=%v", primaryForShard, firstPrimary, ks, id, shard, partitionID))
			}

			input := append(k, v...)
			//vv("55555 ComputeTranslatorSummaryCols(partitionID=%v, path='%v'), idBucket id=%x  key='%v'", partitionID, s.Path, id, ks)
			_, _ = hasher.Write(input)
			sum.IDCount++
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	sum.PrimaryNodeIndex = firstPrimary

	var buf [16]byte
	_, _ = hasher.Digest().Read(buf[0:])
	sum.Checksum = string(buf[:])
	return sum, nil
}

func (s *TranslateStore) KeyWalker(walk func(key string, col uint64)) error {
	return s.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(bucketKeys)
		if bkt == nil {
			panic("bucketKeys not found")
		}
		cur := bkt.Cursor()
		for k, v := cur.First(); k != nil; k, v = cur.Next() {
			walk(string(k), btou64(v))
		}
		return nil
	})
}
func (s *TranslateStore) IDWalker(walk func(key string, col uint64)) error {
	return s.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(bucketIDs)
		if bkt == nil {
			panic("bucketIDs not found")
		}
		cur := bkt.Cursor()
		for k, v := cur.First(); k != nil; k, v = cur.Next() {
			walk(string(v), btou64(k))
		}
		return nil
	})
}

// call s.notifyWrite() when done
func (s *TranslateStore) SetFwdRevMaps(tx *bolt.Tx, fwd map[string]uint64, rev map[uint64]string) (err error) {

	localTx := false
	if tx == nil {
		localTx = true
		tx, err = s.db.Begin(true)
		if err != nil {
			return err
		}
		defer func() {
			_ = tx.Rollback()
		}()
	}

	// reinitialize buckets
	err = tx.DeleteBucket(bucketKeys)
	if err != nil {
		return err
	}
	err = tx.DeleteBucket(bucketIDs)
	if err != nil {
		return err
	}
	if _, err := tx.CreateBucketIfNotExists(bucketKeys); err != nil {
		return err
	} else if _, err := tx.CreateBucketIfNotExists(bucketIDs); err != nil {
		return err
	}

	key2id := tx.Bucket(bucketKeys)
	for k, v := range fwd {
		err := key2id.Put([]byte(k), u64tob(v))
		if err != nil {
			return err
		}
	}
	id2key := tx.Bucket(bucketIDs)
	for k, v := range rev {
		err := id2key.Put(u64tob(k), []byte(v))
		if err != nil {
			return err
		}
	}
	if localTx {
		return tx.Commit()
	}
	return nil
}

func (s *TranslateStore) GetFwdRevMaps(tx *bolt.Tx) (fwd map[string]uint64, rev map[uint64]string, err error) {
	fwd = make(map[string]uint64)
	rev = make(map[uint64]string)

	key2id := tx.Bucket(bucketKeys)

	err = key2id.ForEach(func(k, v []byte) error {
		fwd[string(k)] = btou64(v)
		return nil
	})
	if err != nil {
		return
	}

	id2key := tx.Bucket(bucketIDs)
	err = id2key.ForEach(func(k, v []byte) error {
		rev[btou64(k)] = string(v)
		return nil
	})
	return
}

//var vv = pilosa.VV

// helpers for repair

// muint64 holds multiple unit64
type muint64 struct {
	slc []uint64
}

func (m *muint64) String() (s string) {
	for _, e := range m.slc {
		s += fmt.Sprintf("%x, ", e)
	}
	return
}

// mstring holds multiple strings
type mstring struct {
	slc []string
}

func (m *mstring) String() (s string) {
	for _, e := range m.slc {
		s += e + ","
	}
	return
}

func addToProblemKeys(problemKeys map[string]*muint64, k string, v uint64, noValue bool) {
	mu, already := problemKeys[k]
	if !already {
		mu = &muint64{}
		problemKeys[k] = mu
	}
	if !noValue {
		mu.slc = append(mu.slc, v)
	}
}
func addToProblemIDs(problemIDs map[uint64]*mstring, k uint64, v string, noValue bool) {
	mu, already := problemIDs[k]
	if !already {
		mu = &mstring{}
		problemIDs[k] = mu
	}
	if !noValue {
		mu.slc = append(mu.slc, v)
	}
}

// only actually apply the fixes if applyKeyRepairs is true.
// if anything changed, return changed == true.
func (s *TranslateStore) RepairKeys(topo *pilosa.Topology, verbose, applyKeyRepairs bool) (changed bool, err error) {
	// strategy: get the full set of keys; the domain keys from
	// the forward key->id mapping, and the range keys from the reverse id->key mapping.
	// Then march through them and make sure they are mapped correctly.
	// At the moment we do try to reuse dangling IDs instead of making
	// new ones. This might not always be possible, but we hope for
	// now that it suffices b/c it minimizes the amount of fragment
	// re-write we may have to do.

	/*
		// ============ profiling ===============
		fd, err := ioutil.TempFile(".", "cpu.prof")
		if err != nil {
			panic(err)
		}
		_ = pprof.StartCPUProfile(fd)
		defer func() {
			pprof.StopCPUProfile()
			fd.Close()
		}()
		// ============ end profiling ===============
	*/

	tx, err := s.db.Begin(true)
	if err != nil {
		return false, err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	fwd, rev, err := s.GetFwdRevMaps(tx)
	if err != nil {
		return false, err
	}

	// place to store the correct stuff.
	//
	// fwd2, rev2: new, repaired versions.
	// INVAR: they only contain (correct) invertible mappings.
	fwd2 := make(map[string]uint64)
	rev2 := make(map[uint64]string)

	// and a place to store the problems.
	problemKeys := make(map[string]*muint64)
	problemIDs := make(map[uint64]*mstring)

fwdscan:
	for k, v := range fwd {
		_, already := fwd2[k]
		if already {
			// k has already been repaired. don't worry about further.
			continue fwdscan
		} else {
			// if its already invertible, then just keep it, no need to repair it

			// INVAR: k is not in fwd2 (at least not yet).
			rkey, ok := rev[v]
			if !ok {
				// k -> v -> X
				addToProblemIDs(problemIDs, v, k, false)
				addToProblemKeys(problemKeys, k, v, false)
				continue fwdscan
			}
			if rkey == k {
				// yay. a good, invertible, mapping. no repair needed.
				if k == "" {
					panic("bad empty key")
				}
				fwd2[k] = v
				rev2[v] = k
				continue fwdscan
			}

			// some kind of problem.
			// what kind?
			// Define problemKey as: 2nd key mapping to id already in fwd2.
			// Define problemID  as: 2nd ID  mapping to key already in fwd2.

			// k -> v -> rkey, and rkey != k.
			v2, ok := fwd[rkey]
			if ok {
				// k -> v -> rkey -> v2, where v2 ?= v
				if v2 == v {
					//  k -> v -> rkey -> v
					// just have a problemKey in k.

					if rkey == "" {
						panic("bad empty rkey")
					}
					fwd2[rkey] = v
					rev2[v] = rkey
					addToProblemKeys(problemKeys, k, v, true)
					continue fwdscan
				}
				// this is i = 1 test case. :)

				//  k -> v -> rkey -> v2, where v != v2, and k != rkey.
				addToProblemKeys(problemKeys, k, v, false)
				addToProblemIDs(problemIDs, v, rkey, false)
				continue fwdscan
			} else {
				// k -> v -> rkey -> X(nil), and rkey != k.
				addToProblemKeys(problemKeys, k, v, false)
				addToProblemKeys(problemKeys, rkey, 0, true)
				addToProblemIDs(problemIDs, v, rkey, false)
			}
		}
	}
revscan:
	for id, key := range rev {
		k1, already := rev2[id]
		_ = k1
		if already {
			// fine, already there.
			continue
		}

		// if its already invertible, keep it.
		rid, ok := fwd[key]
		if !ok {
			// id -> key -> X
			addToProblemKeys(problemKeys, key, 0, true)
			addToProblemIDs(problemIDs, id, key, false)
			continue revscan
		}
		if rid == id {
			// id -> key -> id. good. but should have been added to fwd2/rev2 above.
			panic("should have been added to fwd2/rev2 above!")

		} else {
			// id -> key -> rid, where id != rid
			// so rid -> ?
			keyr, ok := rev[rid]
			if !ok {
				// id -> key -> rid -> X, where id != rid
				addToProblemKeys(problemKeys, key, rid, false)
				addToProblemKeys(problemKeys, key, id, false)
				addToProblemIDs(problemIDs, rid, key, false)
				continue revscan
			}
			if keyr == key {
				// id -> key -> rid -> key. So rid is correct and id is dangling.
				//
				// Heuristic: ASSUME here, that the 2 consistent links key->rid->key are correct,
				// and that the single id -> key is in the wrong. This DOESN'T HAVE
				// TO BE THE CASE.

				if key == "" {
					panic("bad empty key")
				}
				rev2[rid] = key
				fwd2[key] = rid
				addToProblemIDs(problemIDs, id, "", true)
			} else {
				// this is test case i = 0. Must handle it.

				// id -> key -> rid -> keyr, id != rid, keyr != key.
				id2, ok := fwd[keyr]
				if ok && id2 == rid {
					// id -> key -> rid -> keyr -> rid, id != rid, keyr != key.
					// so rid -> keyr -> rid is good.

					if keyr == "" {
						panic("bad empty keyr")
					}
					fwd2[keyr] = rid
					rev2[rid] = keyr
					// and id -> key -> rid is bad, b/c id != rid.
					addToProblemKeys(problemKeys, key, rid, false)
					addToProblemIDs(problemIDs, id, key, false)
					continue revscan
				}
				// one of these 3 cases holds. all have the same treatment.
				// 1) id2 == id: id -> key -> rid -> keyr -> id2; id != rid, rid != id2, keyr != key.
				// 2) id2 != id: id -> key -> rid -> keyr -> id2; id != rid, rid != id2, id2 != id, keyr != key.
				// 3) !ok: id -> key -> rid -> keyr -> X, id != rid, keyr != key.
				addToProblemIDs(problemIDs, id, key, false)
				addToProblemKeys(problemKeys, key, rid, false)
				addToProblemIDs(problemIDs, rid, keyr, false)
			}
		}

	}
	//vv("problemKeys = '%v'", problemKeys)
	//vv("problemIDs  = '%v'", problemIDs)

	newIDs := make(map[uint64]bool)

	// assign new IDs to any problemKeys; but first
	// try to reuse already allocated IDs that are just dangling.
loopProblemKeys:
	for key, ids := range problemKeys {
		// first try a minor repair, maybe it was just mssing from rev
		// and we can avoid allocate another id.

		// sanity check
		v2, already := fwd2[key]
		if already {
			panic(fmt.Sprintf("should not get here since fwd2 is only correct invertibles: key='%v', v2='%x'", key, v2))
		}
		// INVAR: we have no correct mapping for key in fwd2.

		// treat the danglers as "suggestions" for the correction.
		for k, id := range ids.slc {
			_ = k
			_, already = rev2[id]
			if !already {
				// is this correct?
				// id is not in rev2, and key is not in fwd2.
				// therefore, we can add them both and maintain consistency.

				//vv("add %v to fwd2", key)
				if key == "" {
					panic("bad empty key")
				}
				fwd2[key] = id
				rev2[id] = key
				continue loopProblemKeys
			}
		}
		// INVAR: key -> ? don't know. We didn't find a usable suggestion for the id.

		// yes, we get here. We have key. We are looking for a suitable id for it.

		// can we get a usable id from the problemIDs?
		found := false
	suggestions:
		for idp, mkeyp := range problemIDs {
			for _, candk := range mkeyp.slc {
				//vv("checking problemIDs, ipd=%x, candk='%v'; candk==key is %v", idp, candk, candk == key)
				if candk == key {
					// we have a suggestion from problemIDs that idp might work, doing key -> idp.
					// Validate that this is possible.
					k2, already := rev2[idp]
					_ = k2
					if already {
						//vv("idp is already in rev2: idp=%v, k2=%v", idp, k2)
						continue suggestions
					}
					// idp works. put it in the correct set.
					if key == "" {
						panic("bad empty key")
					}
					rev2[idp] = key
					fwd2[key] = idp
					found = true
					break suggestions
				}
			}
		}
		if !found {
			id2 := pilosa.GenerateNextPartitionedID(s.index, maxID(tx), s.partitionID, s.partitionN)
			//vv("could not minor repair, allocating new id2 = %v instead", id2)
			newIDs[id2] = true

			if key == "" {
				panic("bad empty key")
			}
			fwd2[key] = id2
			rev2[id2] = key
		}
	} // end problemKeys

	//for id, keys := range problemIDs {
	//}

	if verbose {
		reportIfGainedOrLostIDs(s, fwd, fwd2, rev, rev2, newIDs)
		reportIfGainedOrLostKeys(s, fwd, fwd2, rev, rev2)
	}

	adds, changes, changeIDs, err := makeStringKeyChanges(verbose, applyKeyRepairs, tx, s, topo, fwd, fwd2, rev, rev2, newIDs)
	if err != nil {
		return false, err
	}
	_, _, _ = adds, changes, changeIDs

	//vv("changedIDs = '%#v'", changeIDs)
	if len(adds) > 0 || len(changes) > 0 || len(changeIDs) > 0 || len(newIDs) > 0 {
		changed = true
	}

	//vv("newIDs = '%#v'", newIDs)
	//vv("fwd2 = '%#v'", fwd2)
	//vv("rev2 = '%#v'", rev2)

	err = tx.Commit()
	if err == nil {
		s.notifyWrite()
	}
	return changed, err
}

func reportIfGainedOrLostIDs(s *TranslateStore, fwd, fwd2 map[string]uint64, rev, rev2 map[uint64]string, newIDs map[uint64]bool) {
	// get all IDs ever mentioned
	before := make(map[uint64]bool)
	after := make(map[uint64]bool)
	for _, id := range fwd {
		before[id] = true
	}
	for _, id := range fwd2 {
		if !newIDs[id] {
			after[id] = true
		}
	}
	for id := range rev {
		before[id] = true
	}
	for id := range rev2 {
		if !newIDs[id] {
			after[id] = true
		}
	}
	nb := len(before)
	na := len(after)
	if nb != na {
		fmt.Printf("# needs-repair: Num ID before %v != Num ID after %v, for boltdb = '%v'. before counts(fwd/rev) = %v/%v. after repair counts(fwd2/rev2) = %v/%v\n", nb, na, s.Path, len(fwd), len(rev), len(fwd2), len(rev2))
	}
	if len(newIDs) > 0 {
		fmt.Printf("# needs-repair: adding newIDs '%#v', for boltdb = '%v'. before counts(fwd/rev) = %v/%v. after repair counts(fwd2/rev2) = %v/%v\n", newIDs, s.Path, len(fwd), len(rev), len(fwd2), len(rev2))
	}
}
func reportIfGainedOrLostKeys(s *TranslateStore, fwd, fwd2 map[string]uint64, rev, rev2 map[uint64]string) {
	// get all IDs ever mentioned
	nb := len(fwd)
	na := len(fwd2)
	if nb != na {
		diffAB := mapDiffStrings(fwd, fwd2)
		diffBA := mapDiffStrings(fwd2, fwd)

		fmt.Printf("# needs-repair: Num Keys before != Num Keys after, for boltdb = '%v'. before counts(fwd/rev) = %v/%v. after repair counts(fwd2/rev2) = %v/%v.  fwd - fwd2 = '%#v';  fwd2-fwd = '%#v'\n", s.Path, len(fwd), len(rev), len(fwd2), len(rev2), diffAB, diffBA)
	}
}

// return A - B
func mapDiffStrings(mapA, mapB map[string]uint64) (r []string) {
	for a := range mapA {
		_, ok := mapB[a]
		if !ok {
			r = append(r, a)
		}
	}
	sort.Strings(r)
	return
}

type BeforeAfterKeyChange struct {
	BeforeID uint64
	AfterID  uint64
}

type BeforeAfterIDChange struct {
	IsDelete     bool
	IsAdd        bool
	BeforeString string
	AfterString  string
}

// do the minimal state update.
// fwd2 is the "after" map, all string keys repaired.
func makeStringKeyChanges(
	verbose bool,
	applyKeyRepairs bool,
	tx *bolt.Tx,
	s *TranslateStore,
	topo *pilosa.Topology,
	fwd, fwd2 map[string]uint64,
	rev, rev2 map[uint64]string,
	newIDs map[uint64]bool,
) (
	adds map[string]uint64,
	changeKeys map[string]*BeforeAfterKeyChange,
	changeIDs map[uint64]*BeforeAfterIDChange,
	err error,
) {
	//vv("makeStringKeyChanges called")

	//vv("fwd2 = '%#v'", fwd2)
	//vv("rev2 = '%#v'", rev2)
	//vv("fwd = '%#v'", fwd)
	//vv("rev = '%#v'", rev)

	var action string
	if applyKeyRepairs {
		action = "applying "
	}

	// addition of string key
	adds = make(map[string]uint64)

	// change of the mapping of key -> id.
	changeKeys = make(map[string]*BeforeAfterKeyChange)

	// changes to bucketIDs
	changeIDs = make(map[uint64]*BeforeAfterIDChange)

	localTx := false
	if applyKeyRepairs && tx == nil {
		localTx = true
		tx, err = s.db.Begin(true)
		if err != nil {
			return
		}
		defer func() {
			//vv("tx.Rollback happening")
			_ = tx.Rollback()
		}()
	}

	key2id := tx.Bucket(bucketKeys)
	id2key := tx.Bucket(bucketIDs)

	// make a copy of rev2 that we can delete from, to see if
	// any additions left in rev2 need to be added after all of
	// rev is analyzed.
	rev2cp := make(map[uint64]string)
	for id, k := range rev2 {
		rev2cp[id] = k
	}

	// first we clean up any stale IDs from id2key. Then the fwd2 pass
	// that follows will write to both key2id and id2key.
	for id, key := range rev {
		//vv("makeStringKeyChanges on rev2: id=%x -> key='%v'", id, key)
		key2, ok := rev2[id]
		if !ok {
			changeIDs[id] = &BeforeAfterIDChange{IsDelete: true}
			if verbose {
				fmt.Printf("# %vkey-translation-delete-id: (id %x -> %v). Remaining for that key: ('%v' -> %x)\n", action, id, key, key, fwd2[key])
			}
			if applyKeyRepairs {
				u := u64tob(id)
				err = id2key.Delete(u)
				if err != nil {
					return
				}
			}
			continue
		}
		delete(rev2cp, id)

		if key2 != key {
			u := u64tob(id)
			k := []byte(key2)
			changeIDs[id] = &BeforeAfterIDChange{
				BeforeString: key,
				AfterString:  key2,
			}
			if verbose {
				fmt.Printf("# %vkey-translation-update-id: (id %x -> %v). fwd2 for that key: ('%v' -> %x)\n", action, id, key2, key2, fwd2[key2])
			}
			if applyKeyRepairs {
				err = id2key.Put(u, k)
				if err != nil {
					return
				}
			}
		}
	}
	// anything leftover in rev2cp is stuff that is new, only
	// in rev2 and not in rev. It needs to be added.
	for id, key2 := range rev2cp {
		u := u64tob(id)
		k := []byte(key2)
		changeIDs[id] = &BeforeAfterIDChange{
			IsAdd: true,
			//BeforeString: left empty
			AfterString: key2,
		}
		if verbose {
			fmt.Printf("# %vkey-translation-add-id: (id %x -> %v). Fwd for that key: ('%v' -> %x)\n", action, id, key2, key2, fwd2[key2])
		}
		if applyKeyRepairs {
			err = id2key.Put(u, k)
			if err != nil {
				return
			}
		}
	}

	// We assume here that fwd2 is a super-set of fwd. No string keys
	// should be deleted in the repair. Confirm that.
	for key, id := range fwd {
		_, ok := fwd2[key]
		if !ok {
			panic(fmt.Sprintf("fwd2 is missing a string key from fwd. key='%v' -> id='%x'", key, id))
		}
	}

	for key2, id2 := range fwd2 {
		//vv("makeStringKeyChanges on fwd2, key2='%v', id2=%x", key2, id2)
		isPrimary := false
		if topo != nil {
			primary := topo.GetPrimaryForColKeyTranslation(s.index, key2)
			isPrimary = s.partitionID == primary
		}
		_ = isPrimary
		id, ok := fwd[key2]
		if !ok {
			adds[key2] = id2

			u2 := u64tob(id2)
			k2 := []byte(key2)
			if verbose {
				fmt.Printf("# %vkey-translation-new-key: ('%v' -> %x) added: isPrimary: %v\n", action, key2, id2, isPrimary)
			}
			if applyKeyRepairs {
				err = key2id.Put(k2, u2)
				if err != nil {
					return
				}
				err = id2key.Put(u2, k2)
				if err != nil {
					return
				}
			}
			continue
		}
		if id != id2 {
			changeKeys[key2] = &BeforeAfterKeyChange{
				BeforeID: id,
				AfterID:  id2,
			}
			if verbose {
				fmt.Printf("# %vkey-translation-change-id: ('%v' -> %x) changes to ('%v' ->  %x); isPrimary: %v\n", action, key2, id, key2, id2, isPrimary)
			}
			if applyKeyRepairs {
				u2 := u64tob(id2)
				k2 := []byte(key2)

				err = key2id.Put(k2, u2)
				if err != nil {
					return
				}
				err = id2key.Put(u2, k2)
				if err != nil {
					return
				}
			}
		}
	}

	if localTx {
		err = tx.Commit()
	}
	return
}

func (s *TranslateStore) DumpBolt(label string) {

	fmt.Printf("dumping bolt %v : path='%v'\n", label, s.Path)

	_ = s.KeyWalker(func(key string, col uint64) {
		fmt.Printf("keyWalker: key '%v' -> col '%x'\n", key, col)
	})
	_ = s.IDWalker(func(key string, col uint64) {
		fmt.Printf("idWalker: id '%x' -> key '%v'\n", col, key)
	})

	fmt.Printf("DONE with dumping bolt %v;   path='%v'\n", label, s.Path)

}
