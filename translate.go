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
	"encoding/json"
	"io"
	"io/ioutil"
	"sync"

	"github.com/pkg/errors"
)

const (
	// translateStoreDir is the subdirctory into which the partitioned
	// translate store data is stored.
	translateStoreDir = "_keys"
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
// For BoltDB implementation an empty string will be converted into the sentinel byte slice:
// var emptyKey = []byte{
// 	0x00, 0x00, 0x00,
// 	0x4d, 0x54, 0x4d, 0x54, // MTMT
// 	0x00,
// 	0xc2, 0xa0, // NO-BREAK SPACE
// 	0x00,
// }
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

	// WriteTo ensures that the TranslateStore implements io.WriterTo.
	// It should write the contents of the store to the writer.
	WriteTo(io.Writer) (int64, error)

	// ReadFrom ensures that the TranslateStore implements io.ReaderFrom.
	// It should read from the reader and replace the data store with
	// the read payload.
	ReadFrom(io.Reader) (int64, error)
}

// OpenTranslateStoreFunc represents a function for instantiating and opening a TranslateStore.
type OpenTranslateStoreFunc func(path, index, field string, partitionID, partitionN int) (TranslateStore, error)

// GenerateNextPartitionedID returns the next ID within the same partition.
func GenerateNextPartitionedID(index string, prev uint64, partitionID, partitionN int) uint64 {
	// If the translation store is not partitioned, just return
	// the next ID.
	if partitionID == -1 {
		return prev + 1
	}
	// Try to use the next ID if it is in the same partition.
	// Otherwise find ID in next shard that has a matching partition.
	for id := prev + 1; ; id += ShardWidth {
		if shardPartition(index, id/ShardWidth, partitionN) == partitionID {
			return id
		}
	}
}

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
	index       string
	field       string
	partitionID int
	partitionN  int
	readOnly    bool
	keysByID    map[uint64]string
	idsByKey    map[string]uint64
	maxID       uint64

	writeNotify chan struct{}
}

// NewInMemTranslateStore returns a new instance of InMemTranslateStore.
func NewInMemTranslateStore(index, field string, partitionID, partitionN int) *InMemTranslateStore {
	return &InMemTranslateStore{
		index:       index,
		field:       field,
		partitionID: partitionID,
		partitionN:  partitionN,
		keysByID:    make(map[uint64]string),
		idsByKey:    make(map[string]uint64),
		writeNotify: make(chan struct{}),
	}
}

var _ OpenTranslateStoreFunc = OpenInMemTranslateStore

// OpenInMemTranslateStore returns a new instance of InMemTranslateStore.
// Implements OpenTranslateStoreFunc.
func OpenInMemTranslateStore(rawurl, index, field string, partitionID, partitionN int) (TranslateStore, error) {
	return NewInMemTranslateStore(index, field, partitionID, partitionN), nil
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
	return s.translateKey(key)
}

// TranslateKeys converts a string key to an integer ID.
// If key does not have an associated id then one is created.
func (s *InMemTranslateStore) TranslateKeys(keys []string) (_ []uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ids := make([]uint64, len(keys))
	for i := range keys {
		if ids[i], err = s.translateKey(keys[i]); err != nil {
			return ids, err
		}
	}
	return ids, nil
}

func (s *InMemTranslateStore) translateKey(key string) (_ uint64, err error) {
	// Return id if it has been added.
	if id, ok := s.idsByKey[key]; ok {
		return id, nil
	} else if s.readOnly {
		return 0, nil
	}

	// Generate a new id and update db.
	var id uint64
	if s.field == "" {
		id = GenerateNextPartitionedID(s.index, s.maxID, s.partitionID, s.partitionN)
	} else {
		id = s.maxID + 1
	}
	s.set(id, key)
	return id, nil
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
	return s.keysByID[id]
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
	s.keysByID[id] = key
	s.idsByKey[key] = id
	if id > s.maxID {
		s.maxID = id
	}
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

// WriteTo implements io.WriterTo. It's not efficient or careful, but we
// don't expect to use InMemTranslateStore much, it's mostly there to
// avoid disk load during testing.
func (s *InMemTranslateStore) WriteTo(w io.Writer) (int64, error) {
	bytes, err := json.Marshal(s.keysByID)
	if err != nil {
		return 0, err
	}
	n, err := w.Write(bytes)
	return int64(n), err
}

// ReadFrom implements io.ReaderFrom. It's not efficient or careful, but we
// don't expect to use InMemTranslateStore much, it's mostly there to
// avoid disk load during testing.
func (s *InMemTranslateStore) ReadFrom(r io.Reader) (count int64, err error) {
	var bytes []byte
	bytes, err = ioutil.ReadAll(r)
	count = int64(len(bytes))
	if err != nil {
		return count, err
	}
	var keysByID map[uint64]string
	err = json.Unmarshal(bytes, &keysByID)
	if err != nil {
		return count, err
	}
	s.maxID = 0
	s.keysByID = keysByID
	s.idsByKey = make(map[string]uint64, len(s.keysByID))
	for k, v := range s.keysByID {
		s.idsByKey[v] = k
		if k > s.maxID {
			s.maxID = k
		}
	}
	return count, nil
}

// MaxID returns the highest identifier in the store.
func (s *InMemTranslateStore) MaxID() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.maxID, nil
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
