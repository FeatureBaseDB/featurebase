// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package pilosa

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/featurebasedb/featurebase/v3/disco"
	"github.com/featurebasedb/featurebase/v3/roaring"
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
//
//	var emptyKey = []byte{
//		0x00, 0x00, 0x00,
//		0x4d, 0x54, 0x4d, 0x54, // MTMT
//		0x00,
//		0xc2, 0xa0, // NO-BREAK SPACE
//		0x00,
//	}
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

	Begin(write bool) (TranslatorTx, error)

	// ReadFrom ensures that the TranslateStore implements io.ReaderFrom.
	// It should read from the reader and replace the data store with
	// the read payload.
	ReadFrom(io.Reader) (int64, error)

	Delete(records *roaring.Bitmap) (Commitor, error)
}

// TranslatorTx reproduces a subset of the methods on the BoltDB Tx
// object. Others may be needed in the future and we should just add
// them here. The idea is not to scatter direct references to bolt
// stuff throughout the whole codebase.
type TranslatorTx interface {
	WriteTo(io.Writer) (int64, error)
	Rollback() error
	// e.g. Commit() error
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

var _ OpenTranslateStoreFunc = OpenInMemTranslateStore

// OpenInMemTranslateStore returns a new instance of a BoltDB based
// TranslateStore which removes all its files when it's closed, and
// tries to operate off a RAM disk if one is configured and set in the
// environment.  Implements OpenTranslateStoreFunc.
func OpenInMemTranslateStore(rawurl, index, field string, partitionID, partitionN int, fsyncEnabled bool) (TranslateStore, error) {
	bt := NewBoltTranslateStore(index, field, partitionID, partitionN, false)
	iname := index
	if len(iname) > 10 {
		iname = iname[:10]
	}
	fname := field
	if len(fname) > 10 {
		fname = fname[:10]
	}

	tf, err := os.CreateTemp(os.Getenv("RAMDISK"), fmt.Sprintf("bolt-i%s-f%s-%d-%d-", iname, fname, partitionID, partitionN))
	if err != nil {
		return nil, errors.Wrap(err, "making temp file for boltdb key translation")
	}
	bt.Path = tf.Name()
	err = bt.Open()
	if err != nil {
		return nil, errors.Wrap(err, "opening in mem boltdb")
	}
	return &BoltInMemTranslateStore{bt}, err
}

type BoltInMemTranslateStore struct {
	*BoltTranslateStore
}

func (b *BoltInMemTranslateStore) Close() error {
	defer os.RemoveAll(b.BoltTranslateStore.Path)
	err := b.BoltTranslateStore.Close()
	if err != nil {
		return errors.Wrap(err, "closing in mem bolt translate store")
	}
	return nil
}
