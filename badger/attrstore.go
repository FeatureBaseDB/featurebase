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

package badger

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"

	"github.com/cespare/xxhash"
	"github.com/dgraph-io/badger"
	"github.com/pilosa/pilosa"
	"github.com/pkg/errors"
)

// attrBlockSize is the size of attribute blocks for anti-entropy.
const attrBlockSize = 100

type attrStore struct {
	path string
	db   *badger.DB
	mu   sync.RWMutex
}

//NewAttrStore returns a new dgraph-io/badger AttrStore
func NewAttrStore(path string) pilosa.AttrStore {
	return newCache(&attrStore{
		path: path,
		mu:   sync.RWMutex{},
	})
}

// Path returns the path to the store's data file.
func (s *attrStore) Path() string {
	return s.path
}

func (s *attrStore) Open() error {
	//using default options:
	opts := badger.DefaultOptions
	//using a smaller value log file size works
	//for GOARCH=386
	opts.ValueLogFileSize = 1 << 29
	//also possible to
	//opts.ValueLogLoadingMode = options.FileIO
	opts.Dir = s.path
	opts.ValueDir = s.path
	db, err := badger.Open(opts)
	if err != nil {
		return errors.Wrap(err, "opening badgerdb storage")
	}
	s.db = db
	return nil
}

func (s *attrStore) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

// Attrs returns a set of attributes by ID.
func (s *attrStore) Attrs(id uint64) (m map[string]interface{}, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if err = s.db.View(func(txn *badger.Txn) error {
		m, err = txnAttrs(txn, id)
		return err
	}); err != nil {
		return nil, errors.Wrap(err, "finding attributes")
	}
	return m, nil
}

func (s *attrStore) SetAttrs(id uint64, m map[string]interface{}) (attr map[string]interface{}, err error) {
	// Ignore empty maps.
	if len(m) == 0 {
		return nil, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.db.Update(func(txn *badger.Txn) error {
		attr, err = txnUpdateAttrs(txn, id, m)
		return err
	}); err != nil {
		return nil, errors.Wrap(err, "updating store")
	}
	return attr, nil

}

func (s *attrStore) SetBulkAttrs(m map[uint64]map[string]interface{}) (map[uint64]map[string]interface{}, error) {

	s.mu.Lock()
	defer s.mu.Unlock()

	attrs := make(map[uint64]map[string]interface{})
	if err := s.db.Update(func(txn *badger.Txn) error {
		// Collect and sort keys.
		ids := make([]uint64, 0, len(m))
		for id := range m {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

		// Update attributes for each id.
		for _, id := range ids {
			attr, err := txnUpdateAttrs(txn, id, m[id])
			if err != nil {
				return err
			}
			attrs[id] = attr
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return attrs, nil
}

// Blocks returns a list of all attribute blocks in the store.
func (s *attrStore) Blocks() ([]pilosa.AttrBlock, error) {
	txn := s.db.NewTransaction(false)

	defer txn.Discard()
	var blocks []pilosa.AttrBlock
	// Create iterator
	opts := badger.DefaultIteratorOptions
	it := txn.NewIterator(opts)
	defer it.Close()
	curBlk := uint64(0)
	var block *pilosa.AttrBlock
	h := xxhash.New()
	for it.Rewind(); true; it.Next() {
		if !it.Valid() {
			//if end add the last block and exit
			if block != nil {
				block.Checksum = h.Sum(nil)
				blocks = append(blocks, *block)
			}
			break
		}
		item := it.Item()
		key := item.Key()
		id := binary.BigEndian.Uint64(key) / attrBlockSize
		//end of block
		if id != curBlk && block != nil {
			block.Checksum = h.Sum(nil)
			blocks = append(blocks, *block)
			block = nil
		}
		//create new block
		if block == nil {
			block = &pilosa.AttrBlock{
				ID: id,
			}
			curBlk = id
			h = xxhash.New()
		}
		val, err := item.Value()
		if err != nil {
			return nil, err
		}
		h.Write(key)
		h.Write(val)
	}
	return blocks, nil
}

// BlockData returns all data for a single block.
func (s *attrStore) BlockData(i uint64) (map[uint64]map[string]interface{}, error) {
	// Start read-only transaction.
	txn := s.db.NewTransaction(false)

	defer txn.Discard()
	m := make(map[uint64]map[string]interface{})
	// Create iterator
	opts := badger.DefaultIteratorOptions
	it := txn.NewIterator(opts)
	defer it.Close()

	// Move to the start of the block.
	min := u64tob(i * attrBlockSize)
	max := u64tob((i + 1) * attrBlockSize)

	for it.Seek(min); it.Valid(); it.Next() {
		item := it.Item()
		key := item.Key()
		if bytes.Compare(key, max) != -1 {
			break
		}
		val, err := item.Value()
		if err != nil {
			return nil, err
		}
		// Decode attribute map and associate with id.
		attrs, err := pilosa.DecodeAttrs(val)
		if err != nil {
			return nil, errors.Wrap(err, "decoding attrs")
		}
		m[btou64(key)] = attrs
	}

	return m, nil

}

// u64tob encodes v to big endian encoding.
func u64tob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

// btou64 decodes b from big endian encoding.
func btou64(b []byte) uint64 { return binary.BigEndian.Uint64(b) }

// txAttrs returns a map of attributes map for an id.
func txnAttrs(txn *badger.Txn, id uint64) (map[string]interface{}, error) {
	item, err := txn.Get(u64tob(id))
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return make(map[string]interface{}), nil
		}
		return nil, errors.Wrap(err, "txn.Get()")
	}

	val, err := item.Value()
	if err != nil {
		return nil, err
	}
	return pilosa.DecodeAttrs(val)

}

// txnUpdateAttrs updates the attributes for an id, and
// returns the new combined set of attributes for the id.
func txnUpdateAttrs(txn *badger.Txn, id uint64, m map[string]interface{}) (map[string]interface{}, error) {
	attr, err := txnAttrs(txn, id)
	if err != nil {
		return nil, err
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
	buf, err := pilosa.EncodeAttrs(attr)
	if err != nil {
		return nil, errors.Wrap(err, "encoding attrs")
	}
	if err := txn.Set(u64tob(id), buf); err != nil {
		return nil, errors.Wrap(err, "saving attrs")
	}
	return attr, nil
}
