// Copyright 2021 Molecula Corp. All rights reserved.
package pilosa

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"sort"
	"sync"

	"github.com/molecula/featurebase/v3/disco"
	"github.com/molecula/featurebase/v3/ingest"
	"github.com/molecula/featurebase/v3/roaring"
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
	ErrTranslatingKeyNotFound     = errors.New("translating key not found")
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
type TranslateStore interface { // TODO: refactor this interface; readonly should be part of the type and replication should be an impl detail
	io.Closer

	// Returns the maximum ID set on the store.
	MaxID() (uint64, error)

	// Retrieves the partition ID associated with the store.
	// Only applies to index stores.
	PartitionID() int

	// Sets & retrieves whether the store is read-only.
	ReadOnly() bool
	SetReadOnly(v bool)

	// FindKeys looks up the ID for each key.
	// Keys are not created if they do not exist.
	// Missing keys are not considered errors, so the length of the result may be less than that of the input.
	FindKeys(keys ...string) (map[string]uint64, error)

	// CreateKeys maps all keys to IDs, creating the IDs if they do not exist.
	// If the translator is read-only, this will return an error.
	CreateKeys(keys ...string) (map[string]uint64, error)

	// Match finds IDs of strings matching the filter.
	Match(filter func([]byte) bool) ([]uint64, error)

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

	Delete(records *roaring.Bitmap) (Commitor, error)
}

// This implements ingest's key translator interface, which differs
// slightly because we want to be able to do fast lookups on arbitrary
// IDs which are not necessarily contiguous small values, so the []string
// from TranslateIDs isn't a good fit.
type ingestKeyTranslator struct {
	store TranslateStore
}

var _ ingest.KeyTranslator = &ingestKeyTranslator{}

func (i ingestKeyTranslator) TranslateKeys(keys ...string) (map[string]uint64, error) {
	return i.store.CreateKeys(keys...)
}

func (i ingestKeyTranslator) TranslateIDs(ids ...uint64) (map[uint64]string, error) {
	keys, err := i.store.TranslateIDs(ids)
	if err != nil {
		return nil, err
	}
	if len(keys) != len(ids) {
		return nil, fmt.Errorf("translating %d id(s), got %d key(s)", len(ids), len(keys))
	}
	out := make(map[uint64]string, len(keys))
	for i, id := range ids {
		out[id] = keys[i]
	}
	return out, nil
}

func newIngestKeyTranslatorFromStore(s TranslateStore) *ingestKeyTranslator {
	return &ingestKeyTranslator{store: s}
}

// TranslatorSummary is returned, for example from the boltdb string key translators,
// by calling ComputeTranslatorSummary(). Non-boltdb mocks, etc no-op that method.
type TranslatorSummary struct {
	Index string

	// ParitionID is filled for column keys
	PartitionID int

	NodeID    string
	StorePath string
	IsPrimary bool
	IsReplica bool

	// PrimaryNodeIndex indexes into the cluster []node array to find the primary
	PrimaryNodeIndex int

	// Field is filled for row keys
	Field string

	// Checksum has a blake3 crypto hash of all the keys->ID and all the ID->key mappings
	Checksum string

	// KeyCount has the number of Key->ID mappings
	KeyCount int

	// IDCount has the number of ID->Key mappings
	IDCount int

	// false for RowIDs, true for string-Key column IDs.
	IsColKey bool
}

func (s *TranslatorSummary) String() string {
	return fmt.Sprintf(`
TranslatorSummary{
	Index      : %v
	PartitionID: %v
	NodeID     : %v
	StorePath  : %v
	IsPrimary  : %v
	IsReplica  : %v
	PrimaryNodeIndex: %v
	Field   : %v
	Checksum: %v
	KeyCount: %v
	IDCount : %v
	IsColKey: %v
}
`,
		s.Index,
		s.PartitionID,
		s.NodeID,
		s.StorePath,
		s.IsPrimary,
		s.IsReplica,
		s.PrimaryNodeIndex,
		s.Field,
		s.Checksum,
		s.KeyCount,
		s.IDCount,
		s.IsColKey,
	)
}

// OpenTranslateStoreFunc represents a function for instantiating and opening a TranslateStore.
type OpenTranslateStoreFunc func(path, index, field string, partitionID, partitionN int, fsyncEnabled bool) (TranslateStore, error)

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
		if disco.ShardToShardPartition(index, id/ShardWidth, partitionN) == partitionID {
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
		r.readers[i].Close() // nolint: errcheck
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

// Empty reports whether there are any actual entries in the map. This
// is distinct from len(m) == 0 in that an entry in this map which is
// itself empty doesn't count as non-empty.
func (m TranslateOffsetMap) Empty() bool {
	for _, sub := range m {
		if !sub.Empty() {
			return false
		}
	}
	return true
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

// Empty reports whether this map has neither partitions nor fields.
func (i *IndexTranslateOffsetMap) Empty() bool {
	return len(i.Partitions) == 0 && len(i.Fields) == 0
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
func OpenInMemTranslateStore(rawurl, index, field string, partitionID, partitionN int, fsyncEnabled bool) (TranslateStore, error) {
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
func (s *InMemTranslateStore) Delete(records *roaring.Bitmap) (Commitor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range records.Slice() {
		key := s.keysByID[id]
		delete(s.keysByID, id)
		delete(s.idsByKey, key)
	}
	return &NopCommitor{}, nil
}

// FindKeys looks up the ID for each key.
// Keys are not created if they do not exist.
// Missing keys are not considered errors, so the length of the result may be less than that of the input.
func (s *InMemTranslateStore) FindKeys(keys ...string) (map[string]uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make(map[string]uint64, len(keys))
	for _, key := range keys {
		id, ok := s.idsByKey[key]
		if !ok {
			// The key does not exist.
			continue
		}

		result[key] = id
	}

	return result, nil
}

// CreateKeys maps all keys to IDs, creating the IDs if they do not exist.
// If the translator is read-only, this will return an error.
func (s *InMemTranslateStore) CreateKeys(keys ...string) (map[string]uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.readOnly {
		return nil, ErrTranslateStoreReadOnly
	}

	result := make(map[string]uint64, len(keys))
	for _, key := range keys {
		id, ok := s.idsByKey[key]
		if !ok {
			// The key does not exist.
			// Generate a new id and update db.
			if s.field == "" {
				id = GenerateNextPartitionedID(s.index, s.maxID, s.partitionID, s.partitionN)
			} else {
				id = s.maxID + 1
			}
			s.set(id, key)
		}

		result[key] = id
	}

	return result, nil
}

func (s *InMemTranslateStore) Match(filter func([]byte) bool) ([]uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var matches []uint64
	for key, id := range s.idsByKey {
		if filter([]byte(key)) {
			matches = append(matches, id)
		}
	}
	sort.Slice(matches, func(i, j int) bool {
		return matches[i] < matches[j]
	})

	return matches, nil
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
	s.mu.Lock()
	defer s.mu.Unlock()
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
