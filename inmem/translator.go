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

package inmem

import (
	"context"
	"io"
	"sync"

	"github.com/pilosa/pilosa/v2"
)

// Ensure type implements interface.
var _ pilosa.TranslateStore = &translateStore{}

// translateStore is an in-memory storage engine for translating string-to-uint64 values.
type translateStore struct {
	mu sync.RWMutex

	cols map[string]*translateIndex
	rows map[frameKey]*translateIndex
}

// NewTranslateStore returns a new instance of TranslateStore.
func NewTranslateStore() *translateStore {
	return &translateStore{
		cols: make(map[string]*translateIndex),
		rows: make(map[frameKey]*translateIndex),
	}
}

// Reader returns an error because it is not supported by the inmem store.
func (s *translateStore) Reader(ctx context.Context, offset int64) (io.ReadCloser, error) {
	return nil, pilosa.ErrReplicationNotSupported
}

// TranslateColumnsToUint64 converts value to a uint64 id.
// If value does not have an associated id then one is created.
func (s *translateStore) TranslateColumnsToUint64(index string, values []string) ([]uint64, error) {
	ret := make([]uint64, len(values))

	// Read value under read lock.
	s.mu.RLock()
	if idx := s.cols[index]; idx != nil {
		var writeRequired bool
		for i := range values {
			v, ok := idx.lookup[values[i]]
			if !ok {
				writeRequired = true
			}
			ret[i] = v
		}
		if !writeRequired {
			s.mu.RUnlock()
			return ret, nil
		}
	}
	s.mu.RUnlock()

	// If any values not found then recheck and then add under a write lock.
	s.mu.Lock()
	defer s.mu.Unlock()

	// Recheck if value was created between the read lock and write lock.
	idx := s.cols[index]
	if idx != nil {
		var writeRequired bool
		for i := range values {
			if ret[i] != 0 {
				continue
			}
			v, ok := idx.lookup[values[i]]
			if !ok {
				writeRequired = true
				continue
			}
			ret[i] = v
		}
		if !writeRequired {
			return ret, nil
		}
	}

	// Create index map if it doesn't exists.
	if idx == nil {
		idx = newTranslateIndex()
		s.cols[index] = idx
	}

	// Add new identifiers.
	for i := range values {
		if ret[i] != 0 {
			continue
		}

		idx.seq++
		v := idx.seq
		ret[i] = v
		idx.lookup[values[i]] = v
		idx.reverse[v] = values[i]
	}

	return ret, nil
}

// TranslateColumnToString converts a uint64 id to its associated string value.
// If the id is not associated with a string value then a blank string is returned.
func (s *translateStore) TranslateColumnToString(index string, value uint64) (string, error) {
	s.mu.RLock()
	if idx := s.cols[index]; idx != nil {
		if ret, ok := idx.reverse[value]; ok {
			s.mu.RUnlock()
			return ret, nil
		}
	}
	s.mu.RUnlock()
	return "", nil
}

func (s *translateStore) TranslateRowsToUint64(index, frame string, values []string) ([]uint64, error) {
	key := frameKey{index, frame}

	ret := make([]uint64, len(values))

	// Read value under read lock.
	s.mu.RLock()
	if idx := s.rows[key]; idx != nil {
		var writeRequired bool
		for i := range values {
			v, ok := idx.lookup[values[i]]
			if !ok {
				writeRequired = true
			}
			ret[i] = v
		}
		if !writeRequired {
			s.mu.RUnlock()
			return ret, nil
		}
	}
	s.mu.RUnlock()

	// If any values not found then recheck and then add under a write lock.
	s.mu.Lock()
	defer s.mu.Unlock()

	// Recheck if value was created between the read lock and write lock.
	idx := s.rows[key]
	if idx != nil {
		var writeRequired bool
		for i := range values {
			if ret[i] != 0 {
				continue
			}
			v, ok := idx.lookup[values[i]]
			if !ok {
				writeRequired = true
				continue
			}
			ret[i] = v
		}
		if !writeRequired {
			return ret, nil
		}
	}

	// Create map if it doesn't exists.
	if idx == nil {
		idx = newTranslateIndex()
		s.rows[key] = idx
	}

	// Add new identifiers.
	for i := range values {
		if ret[i] != 0 {
			continue
		}

		idx.seq++
		v := idx.seq
		ret[i] = v
		idx.lookup[values[i]] = v
		idx.reverse[v] = values[i]
	}

	return ret, nil
}

func (s *translateStore) TranslateRowToString(index, frame string, value uint64) (string, error) {
	s.mu.RLock()
	if idx := s.rows[frameKey{index, frame}]; idx != nil {
		if ret, ok := idx.reverse[value]; ok {
			s.mu.RUnlock()
			return ret, nil
		}
	}
	s.mu.RUnlock()
	return "", nil
}

type frameKey struct {
	index string
	frame string
}

type translateIndex struct {
	seq     uint64
	lookup  map[string]uint64
	reverse map[uint64]string
}

func newTranslateIndex() *translateIndex {
	return &translateIndex{
		lookup:  make(map[string]uint64),
		reverse: make(map[uint64]string),
	}
}
