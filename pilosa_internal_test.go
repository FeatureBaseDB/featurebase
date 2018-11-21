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
	"testing"
)

func TestValidateName(t *testing.T) {
	names := []string{
		"a", "ab", "ab1", "b-c", "d_e",
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}
	for _, name := range names {
		if validateName(name) != nil {
			t.Fatalf("Should be valid index name: %s", name)
		}
	}
}

func TestValidateNameInvalid(t *testing.T) {
	names := []string{
		"", "'", "^", "/", "\\", "A", "*", "a:b", "valid?no", "yüce", "1", "_", "-",
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1",
		"exists",
	}
	for _, name := range names {
		if validateName(name) == nil {
			t.Fatalf("Should be invalid index name: %s", name)
		}
	}
}

// memAttrStore represents an in-memory implementation of the AttrStore interface.
type memAttrStore struct {
	store map[uint64]map[string]interface{}
}

func (s *memAttrStore) Path() string                                          { return "" }
func (s *memAttrStore) Open() error                                           { return nil }
func (s *memAttrStore) Close() error                                          { return nil }
func (s *memAttrStore) Attrs(id uint64) (m map[string]interface{}, err error) { return s.store[id], nil }
func (s *memAttrStore) SetAttrs(id uint64, m map[string]interface{}) error {
	s.store[id] = m
	return nil
}
func (s *memAttrStore) SetBulkAttrs(m map[uint64]map[string]interface{}) error {
	for id, v := range m {
		s.store[id] = v
	}
	return nil
}
func (s *memAttrStore) Blocks() ([]AttrBlock, error)                                  { return nil, nil }
func (s *memAttrStore) BlockData(i uint64) (map[uint64]map[string]interface{}, error) { return nil, nil }
