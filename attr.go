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
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/boltdb/bolt"
	"github.com/gogo/protobuf/proto"
	"github.com/pilosa/pilosa/internal"
)

// AttrBlockSize is the size of attribute blocks for anti-entropy.
const AttrBlockSize = 100

// Attribute data type enum.
const (
	AttrTypeString = 1
	AttrTypeInt    = 2
	AttrTypeBool   = 3
	AttrTypeFloat  = 4
)

// AttrCache represents a cache for attributes.
type AttrCache struct {
	mu    sync.RWMutex
	attrs map[uint64]map[string]interface{}
}

// Get returns the cached attributes for a given id.
func (c *AttrCache) Get(id uint64) map[string]interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()
	attrs := c.attrs[id]
	if attrs == nil {
		return nil
	}

	// Make a copy for safety
	ret := make(map[string]interface{})
	for k, v := range attrs {
		ret[k] = v
	}
	return ret
}

// Set updates the cached attributes for a given id.
func (c *AttrCache) Set(id uint64, attrs map[string]interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.attrs[id] = attrs
}

// AttrStore represents a storage layer for attributes.
type AttrStore struct {
	mu        sync.RWMutex
	path      string
	db        *bolt.DB
	attrCache *AttrCache
}

// NewAttrCache returns a new instance of AttrCache.
func NewAttrCache() *AttrCache {
	return &AttrCache{
		attrs: make(map[uint64]map[string]interface{}),
	}
}

// NewAttrStore returns a new instance of AttrStore.
func NewAttrStore(path string) *AttrStore {
	return &AttrStore{
		path:      path,
		attrCache: NewAttrCache(),
	}
}

// Path returns path to the store's data file.
func (s *AttrStore) Path() string { return s.path }

// Open opens and initializes the store.
func (s *AttrStore) Open() error {
	// Open storage.
	db, err := bolt.Open(s.path, 0666, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return err
	}
	s.db = db

	// Initialize database.
	if err := s.db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists([]byte("attrs")); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}

	return nil
}

// Close closes the store.
func (s *AttrStore) Close() error {
	if s.db != nil {
		s.db.Close()
	}
	return nil
}

// Attrs returns a set of attributes by ID.
func (s *AttrStore) Attrs(id uint64) (m map[string]interface{}, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check cache for map.
	if m = s.attrCache.Get(id); m != nil {
		return m, nil
	}

	// Find attributes from storage.
	if err = s.db.View(func(tx *bolt.Tx) error {
		m, err = txAttrs(tx, id)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}

	// Add to cache.
	s.attrCache.Set(id, m)

	return
}

// SetAttrs sets attribute values for a given ID.
func (s *AttrStore) SetAttrs(id uint64, m map[string]interface{}) error {
	// Ignore empty maps.
	if len(m) == 0 {
		return nil
	}

	// Check if the attributes already exist under a read-only lock.
	if attr, err := s.Attrs(id); err != nil {
		return err
	} else if attr != nil && mapContains(attr, m) {
		return nil
	}

	// Obtain write lock.
	s.mu.Lock()
	defer s.mu.Unlock()

	var attr map[string]interface{}
	if err := s.db.Update(func(tx *bolt.Tx) error {
		tmp, err := txUpdateAttrs(tx, id, m)
		if err != nil {
			return err
		}
		attr = tmp

		return nil
	}); err != nil {
		return err
	}

	// Swap attributes map in cache.
	s.attrCache.Set(id, attr)

	return nil
}

// SetBulkAttrs sets attribute values for a set of ids.
func (s *AttrStore) SetBulkAttrs(m map[uint64]map[string]interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	attrs := make(map[uint64]map[string]interface{})
	if err := s.db.Update(func(tx *bolt.Tx) error {
		// Collect and sort keys.
		ids := make([]uint64, 0, len(m))
		for id := range m {
			ids = append(ids, id)
		}
		sort.Sort(uint64Slice(ids))

		// Update attributes for each id.
		for _, id := range ids {
			attr, err := txUpdateAttrs(tx, id, m[id])
			if err != nil {
				return err
			}
			attrs[id] = attr
		}

		return nil
	}); err != nil {
		return err
	}

	// Swap attributes map in cache.
	for id, attr := range attrs {
		s.attrCache.Set(id, attr)
	}

	return nil
}

// Blocks returns a list of all blocks in the store.
func (s *AttrStore) Blocks() ([]AttrBlock, error) {
	tx, err := s.db.Begin(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// Wrap cursor to segment by block.
	cur := newBlockCursor(tx.Bucket([]byte("attrs")).Cursor(), AttrBlockSize)

	// Iterate over each block.
	var blocks []AttrBlock
	for cur.nextBlock() {
		block := AttrBlock{ID: cur.blockID()}

		// Compute checksum of every key/value in block.
		h := sha1.New()
		for k, v := cur.next(); k != nil; k, v = cur.next() {
			h.Write(k)
			h.Write(v)
		}
		block.Checksum = h.Sum(nil)

		// Append block.
		blocks = append(blocks, block)
	}

	return blocks, nil
}

// BlockData returns all data for a single block.
func (s *AttrStore) BlockData(i uint64) (map[uint64]map[string]interface{}, error) {
	m := make(map[uint64]map[string]interface{})

	// Start read-only transaction.
	tx, err := s.db.Begin(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// Move to the start of the block.
	min := u64tob(uint64(i) * AttrBlockSize)
	max := u64tob(uint64(i+1) * AttrBlockSize)
	cur := tx.Bucket([]byte("attrs")).Cursor()
	for k, v := cur.Seek(min); k != nil; k, v = cur.Next() {
		// Exit if we're past the end of the block.
		if bytes.Compare(k, max) != -1 {
			break
		}

		// Decode attribute map and associate with id.
		var pb internal.AttrMap
		if err := proto.Unmarshal(v, &pb); err != nil {
			return nil, err
		}
		m[btou64(k)] = decodeAttrs(pb.GetAttrs())
	}

	return m, nil
}

// txAttrs returns a map of attributes for an id.
func txAttrs(tx *bolt.Tx, id uint64) (map[string]interface{}, error) {
	v := tx.Bucket([]byte("attrs")).Get(u64tob(id))
	if v == nil {
		return emptyMap, nil
	}

	var pb internal.AttrMap
	if err := proto.Unmarshal(v, &pb); err != nil {
		return nil, err
	}
	return decodeAttrs(pb.GetAttrs()), nil
}

// txUpdateAttrs updates the attributes for an id.
// Returns the new combined set of attributes for the id.
func txUpdateAttrs(tx *bolt.Tx, id uint64, m map[string]interface{}) (map[string]interface{}, error) {
	attr, err := txAttrs(tx, id)
	if err != nil {
		return nil, err
	}

	// Create a new map if it is empty so we don't update emptyMap.
	if len(attr) == 0 {
		attr = make(map[string]interface{}, len(m))
	}

	// Merge attributes with original values.
	// Nil values should delete keys.
	for k, v := range m {
		if v == nil {
			delete(attr, k)
			continue
		}

		switch v := v.(type) {
		case int:
			attr[k] = int64(v)
		case uint:
			attr[k] = int64(v)
		case uint64:
			attr[k] = int64(v)
		case string, int64, bool, float64:
			attr[k] = v
		default:
			return nil, fmt.Errorf("invalid attr type: %T", v)
		}
	}

	// Marshal and save new values.
	buf, err := proto.Marshal(&internal.AttrMap{Attrs: encodeAttrs(attr)})
	if err != nil {
		return nil, err
	}
	if err := tx.Bucket([]byte("attrs")).Put(u64tob(id), buf); err != nil {
		return nil, err
	}
	return attr, nil
}

func encodeAttrs(m map[string]interface{}) []*internal.Attr {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	a := make([]*internal.Attr, len(keys))
	for i := range keys {
		a[i] = encodeAttr(keys[i], m[keys[i]])
	}
	return a
}

func decodeAttrs(pb []*internal.Attr) map[string]interface{} {
	m := make(map[string]interface{}, len(pb))
	for i := range pb {
		key, value := decodeAttr(pb[i])
		m[key] = value
	}
	return m
}

// encodeAttr converts a key/value pair into an Attr internal representation.
func encodeAttr(key string, value interface{}) *internal.Attr {
	pb := &internal.Attr{Key: key}
	switch value := value.(type) {
	case string:
		pb.Type = AttrTypeString
		pb.StringValue = value
	case float64:
		pb.Type = AttrTypeFloat
		pb.FloatValue = value
	case uint64:
		pb.Type = AttrTypeInt
		pb.IntValue = int64(value)
	case int64:
		pb.Type = AttrTypeInt
		pb.IntValue = value
	case bool:
		pb.Type = AttrTypeBool
		pb.BoolValue = value
	}
	return pb
}

// decodeAttr converts from an Attr internal representation to a key/value pair.
func decodeAttr(attr *internal.Attr) (key string, value interface{}) {
	switch attr.Type {
	case AttrTypeString:
		return attr.Key, attr.StringValue
	case AttrTypeInt:
		return attr.Key, attr.IntValue
	case AttrTypeBool:
		return attr.Key, attr.BoolValue
	case AttrTypeFloat:
		return attr.Key, attr.FloatValue
	default:
		return attr.Key, nil
	}
}

// cloneAttrs returns a shallow clone of m.
func cloneAttrs(m map[string]interface{}) map[string]interface{} {
	other := make(map[string]interface{}, len(m))
	for k, v := range m {
		other[k] = v
	}
	return other
}

// u64tob encodes v to big endian encoding.
func u64tob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

// btou64 decodes b from big endian encoding.
func btou64(b []byte) uint64 { return binary.BigEndian.Uint64(b) }

// emptyMap is a reusable map that contains no keys.
var emptyMap = make(map[string]interface{})

// AttrBlock represents a checksummed block of the attribute store.
type AttrBlock struct {
	ID       uint64 `json:"id"`
	Checksum []byte `json:"checksum"`
}

// AttrBlocks represents a list of blocks.
type AttrBlocks []AttrBlock

// Diff returns a list of block ids that are different or are new in other.
// Block lists must be in sorted order.
func (a AttrBlocks) Diff(other []AttrBlock) []uint64 {
	var ids []uint64
	for {
		// Read next block from each list.
		var blk0, blk1 *AttrBlock
		if len(a) > 0 {
			blk0 = &a[0]
		}
		if len(other) > 0 {
			blk1 = &other[0]
		}

		// Exit if "a" contains no more blocks.
		if blk0 == nil {
			return ids
		}

		// Add block ID if it's different or if it's only in "a".
		if blk1 == nil || blk0.ID < blk1.ID {
			ids = append(ids, blk0.ID)
			a = a[1:]
		} else if blk1.ID < blk0.ID {
			other = other[1:]
		} else {
			if !bytes.Equal(blk0.Checksum, blk1.Checksum) {
				ids = append(ids, blk0.ID)
			}
			a, other = a[1:], other[1:]
		}
	}
}

// blockCursor represents a cursor for iterating over blocks of a bolt bucket.
type blockCursor struct {
	cur  *bolt.Cursor
	base uint64
	n    uint64

	buf struct {
		key    []byte
		value  []byte
		filled bool
	}
}

// newBlockCursor returns a new block cursor that wraps cur using n sized blocks.
func newBlockCursor(c *bolt.Cursor, n int) blockCursor {
	cur := blockCursor{
		cur: c,
		n:   uint64(n),
	}
	cur.buf.key, cur.buf.value = c.First()
	cur.buf.filled = true
	return cur
}

// blockID returns the current block ID. Only valid after call to nextBlock().
func (cur *blockCursor) blockID() uint64 { return cur.base }

// nextBlock moves the cursor to the next block.
// Returns true if another block exists, otherwise returns false.
func (cur *blockCursor) nextBlock() bool {
	if cur.buf.key == nil {
		return false
	}

	cur.base = binary.BigEndian.Uint64(cur.buf.key) / cur.n
	return true
}

// next returns the next key/value within the block.
// Returns nils at the end of the block.
func (cur *blockCursor) next() (key, value []byte) {
	// Use buffered value, if set.
	if cur.buf.filled {
		key, value = cur.buf.key, cur.buf.value
		cur.buf.filled = false
		return key, value
	}

	// Read next key.
	key, value = cur.cur.Next()

	// Fill buffer for EOF.
	if key == nil {
		cur.buf.key, cur.buf.value, cur.buf.filled = key, value, false
		return nil, nil
	}

	// Parse key and buffer if outside of block.
	id := binary.BigEndian.Uint64(key)
	if id/cur.n > cur.base {
		cur.buf.key, cur.buf.value, cur.buf.filled = key, value, true
		return nil, nil
	}

	return key, value
}

// mapContains returns true if all keys & values of subset are in m.
func mapContains(m, subset map[string]interface{}) bool {
	for k, v := range subset {
		value, ok := m[k]
		if !ok || value != v {
			return false
		}
	}
	return true
}
