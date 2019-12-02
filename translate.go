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

package pilosa

import (
	"context"
	"io"
	"sync"

	"github.com/pkg/errors"
)

// Translate store errors.
var (
	ErrTranslateStoreClosed       = errors.New("translate store closed")
	ErrTranslateStoreReaderClosed = errors.New("translate store reader closed")
	ErrReplicationNotSupported    = errors.New("replication not supported")
	ErrTranslateStoreReadOnly     = errors.New("translate store could not find or create key, translate store read only")
	ErrTranslateStoreNotFound     = errors.New("translate store not found")
	ErrCannotOpenV1TranslateFile  = errors.New("cannot open v1 translate .keys file")
)

// TranslateStore is the storage for translation string-to-uint64 values.
type TranslateStore interface {
	io.Closer

	// Returns the maximum ID set on the store.
	MaxID() (uint64, error)

	// Retrieves the partition ID associated with the store.
	// Only applies to index stores.
	PartitionID() int

	// Sets & retrieves whether the store is read-only.
	ReadOnly() bool
	SetReadOnly(v bool)

	// Converts a string key to its autoincrementing integer ID value.
	//
	// Translated id must be associated with a shard in the store's partition
	// unless partition is set to -1.
	TranslateKey(key string) (uint64, error)
	TranslateKeys(key []string) ([]uint64, error)

	// Converts an integer ID to its associated string key.
	TranslateID(id uint64) (string, error)
	TranslateIDs(id []uint64) ([]string, error)

	// Forces the write of a key/id pair, even if read only. Used by replication.
	ForceSet(id uint64, key string) error

	// Returns a reader from the given ID offset.
	EntryReader(ctx context.Context, offset uint64) (TranslateEntryReader, error)
}

// OpenTranslateStoreFunc represents a function for instantiating and opening a TranslateStore.
type OpenTranslateStoreFunc func(path, index, field string, partitionID int) (TranslateStore, error)

// TranslateEntryReader represents a stream of translation entries.
type TranslateEntryReader interface {
	io.Closer
	ReadEntry(entry *TranslateEntry) error
}

// OpenTranslateReaderFunc represents a function for instantiating and opening a TranslateStore.
type OpenTranslateReaderFunc func(ctx context.Context, nodeURL string, offsets TranslateOffsetMap) (TranslateEntryReader, error)

// TranslateEntry represents a key/ID pair from a TranslateStore.
type TranslateEntry struct {
	Index string `json:"index,omitempty"`
	Field string `json:"field,omitempty"`
	ID    uint64 `json:"id,omitempty"`
	Key   string `json:"key,omitempty"`
}

// MultiTranslateEntryReader reads from multiple TranslateEntryReader instances
// and merges them into a single reader.
type MultiTranslateEntryReader struct {
	ctx    context.Context
	cancel func()

	wg sync.WaitGroup

	ch      chan readEntryResponse
	readers []TranslateEntryReader
}

// NewMultiTranslateEntryReader returns a new instance of MultiTranslateEntryReader.
func NewMultiTranslateEntryReader(ctx context.Context, readers []TranslateEntryReader) *MultiTranslateEntryReader {
	r := &MultiTranslateEntryReader{
		readers: readers,
		ch:      make(chan readEntryResponse),
	}
	r.ctx, r.cancel = context.WithCancel(ctx)

	r.wg.Add(len(r.readers))
	for i := range r.readers {
		go func(tr TranslateEntryReader) { defer r.wg.Done(); r.monitor(tr) }(r.readers[i])
	}

	return r
}

// Close stops the reader & child readers and waits for all goroutines to stop.
func (r *MultiTranslateEntryReader) Close() error {
	r.cancel()
	for i := range r.readers {
		r.readers[i].Close()
	}
	r.wg.Wait()
	return nil
}

// ReadEntry reads the next available entry into entry. Returns an error if
// any of the child readers error. Returns io.EOF if reader is closed.
func (r *MultiTranslateEntryReader) ReadEntry(entry *TranslateEntry) error {
	if len(r.readers) == 0 {
		return io.EOF
	}

	select {
	case <-r.ctx.Done():
		return io.EOF
	case resp := <-r.ch:
		if resp.err != nil {
			return resp.err
		}
		*entry = resp.entry
		return nil
	}
}

// monitor runs in a separate goroutine and sends entry reads to the channel.
func (r *MultiTranslateEntryReader) monitor(tr TranslateEntryReader) {
	for {
		var entry TranslateEntry
		err := tr.ReadEntry(&entry)

		select {
		case <-r.ctx.Done():
			return
		case r.ch <- readEntryResponse{entry: entry, err: err}:
		}
	}
}

type readEntryResponse struct {
	entry TranslateEntry
	err   error
}

// TranslateOffsetMap maintains a set of offsets for both indexes & fields.
type TranslateOffsetMap map[string]*IndexTranslateOffsetMap

// IndexOffset returns the offset for the given index.
func (m TranslateOffsetMap) IndexPartitionOffset(name string, partitionID int) uint64 {
	if m[name] == nil {
		return 0
	}
	return m[name].Partitions[partitionID]
}

// SetIndexOffset sets the offset for the given index.
func (m TranslateOffsetMap) SetIndexPartitionOffset(name string, partitionID int, offset uint64) {
	if m[name] == nil {
		m[name] = NewIndexTranslateOffsetMap()
	}
	m[name].Partitions[partitionID] = offset
}

// FieldOffset returns the offset for the given field.
func (m TranslateOffsetMap) FieldOffset(index, name string) uint64 {
	if m[index] == nil {
		return 0
	}
	return m[index].Fields[name]
}

// SetFieldOffset sets the offset for the given field.
func (m TranslateOffsetMap) SetFieldOffset(index, name string, offset uint64) {
	if m[index] == nil {
		m[index] = NewIndexTranslateOffsetMap()
	}
	m[index].Fields[name] = offset
}

type IndexTranslateOffsetMap struct {
	Partitions map[int]uint64    `json:"partitions"`
	Fields     map[string]uint64 `json:"fields"`
}

func NewIndexTranslateOffsetMap() *IndexTranslateOffsetMap {
	return &IndexTranslateOffsetMap{
		Partitions: make(map[int]uint64),
		Fields:     make(map[string]uint64),
	}
}

// Ensure type implements interface.
var _ TranslateStore = &InMemTranslateStore{}

// InMemTranslateStore is an in-memory storage engine for mapping keys to int values.
type InMemTranslateStore struct {
	mu          sync.RWMutex
	partitionID int
	index       string
	field       string
	readOnly    bool
	keys        []string
	lookup      map[string]uint64

	writeNotify chan struct{}
}

// NewInMemTranslateStore returns a new instance of InMemTranslateStore.
func NewInMemTranslateStore(index, field string, partitionID int) *InMemTranslateStore {
	return &InMemTranslateStore{
		partitionID: partitionID,
		index:       index,
		field:       field,
		lookup:      make(map[string]uint64),
		writeNotify: make(chan struct{}),
	}
}

var _ OpenTranslateStoreFunc = OpenInMemTranslateStore

// OpenInMemTranslateStore returns a new instance of InMemTranslateStore.
// Implements OpenTranslateStoreFunc.
func OpenInMemTranslateStore(rawurl, index, field string, partitionID int) (TranslateStore, error) {
	return NewInMemTranslateStore(index, field, partitionID), nil
}

func (s *InMemTranslateStore) Close() error {
	return nil
}

// PartitionID returns the partition id the store was initialized with.
func (s *InMemTranslateStore) PartitionID() int {
	return s.partitionID
}

// ReadOnly returns true if the store is in read-only mode.
func (s *InMemTranslateStore) ReadOnly() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.readOnly
}

// SetReadOnly toggles the read-only mode of the store.
func (s *InMemTranslateStore) SetReadOnly(v bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.readOnly = v
}

// TranslateKeys converts a string key to an integer ID.
// If key does not have an associated id then one is created.
func (s *InMemTranslateStore) TranslateKey(key string) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.readOnly {
		return 0, nil
	}
	return s.translateKey(key), nil
}

// TranslateKeys converts a string key to an integer ID.
// If key does not have an associated id then one is created.
func (s *InMemTranslateStore) TranslateKeys(keys []string) ([]uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.readOnly {
		return nil, nil
	}

	ids := make([]uint64, len(keys))
	for i := range keys {
		ids[i] = s.translateKey(keys[i])
	}
	return ids, nil
}

func (s *InMemTranslateStore) translateKey(key string) uint64 {
	// Return id if it has been added.
	if id, ok := s.lookup[key]; ok {
		return id
	}

	// Generate a new id and update db.
	id := uint64(len(s.keys) + 1)
	s.set(id, key)
	return id
}

// TranslateID converts an integer ID to a string key.
// Returns a blank string if ID does not exist.
func (s *InMemTranslateStore) TranslateID(id uint64) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.translateID(id), nil
}

// TranslateIDs converts a list of integer IDs to a list of string keys.
func (s *InMemTranslateStore) TranslateIDs(ids []uint64) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	keys := make([]string, len(ids))
	for i := range ids {
		keys[i] = s.translateID(ids[i])
	}
	return keys, nil
}

func (s *InMemTranslateStore) translateID(id uint64) string {
	if id == 0 || id > uint64(len(s.keys)) {
		return ""
	}
	return s.keys[id-1]
}

// ForceSet writes the id/key pair to the db. Used by replication.
func (s *InMemTranslateStore) ForceSet(id uint64, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.set(id, key)
	return nil
}

// set assigns the id/key pair to the store.
func (s *InMemTranslateStore) set(id uint64, key string) {
	s.keys = append(s.keys, key)
	s.lookup[key] = id
	s.notifyWrite()
}

// WriteNotify returns a channel that is closed when a new entry is written.
func (s *InMemTranslateStore) WriteNotify() <-chan struct{} {
	s.mu.RLock()
	ch := s.writeNotify
	s.mu.RUnlock()
	return ch
}

// notifyWrite sends a write notification under write lock.
func (s *InMemTranslateStore) notifyWrite() {
	close(s.writeNotify)
	s.writeNotify = make(chan struct{})
}

// EntryReader returns an error. Replication is not supported.
func (s *InMemTranslateStore) EntryReader(ctx context.Context, offset uint64) (TranslateEntryReader, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return newInMemTranslateEntryReader(ctx, s, offset), nil
}

// MaxID returns the highest identifier in the store.
func (s *InMemTranslateStore) MaxID() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return uint64(len(s.keys)), nil
}

// inMemEntryReader represents a stream of translation entries for an inmem translation store.
type inMemTranslateEntryReader struct {
	ctx    context.Context
	cancel func()

	store  *InMemTranslateStore
	offset uint64
}

func newInMemTranslateEntryReader(ctx context.Context, store *InMemTranslateStore, offset uint64) *inMemTranslateEntryReader {
	r := &inMemTranslateEntryReader{
		store:  store,
		offset: offset,
	}
	r.ctx, r.cancel = context.WithCancel(ctx)
	return r
}

// Close stops the reader.
func (r *inMemTranslateEntryReader) Close() error {
	r.cancel()
	return nil
}

// ReadEntry reads the next available entry.
func (r *inMemTranslateEntryReader) ReadEntry(entry *TranslateEntry) error {
	for {
		// Wait until our offset is less than the max id.
		notify := r.store.WriteNotify()
		if maxID, err := r.store.MaxID(); err != nil {
			return err
		} else if r.offset > maxID {
			select {
			case <-r.ctx.Done():
				return io.EOF
			case <-notify:
				continue // restart loop
			}
		}

		// Translate key for offset.
		key, err := r.store.TranslateID(r.offset)
		if err != nil {
			return err
		}

		// Copy id/key pair to entry argument and increment offset for next read.
		entry.Index, entry.Field = r.store.index, r.store.field
		entry.ID, entry.Key = r.offset, key
		r.offset++
		return nil
	}
}
