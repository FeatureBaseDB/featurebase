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

package mock

import (
	"context"
	"io"

	"github.com/molecula/featurebase/v2"
)

type TranslateStore struct {
	CloseFunc         func() error
	MaxIDFunc         func() (uint64, error)
	PartitionIDFunc   func() int
	ReadOnlyFunc      func() bool
	SetReadOnlyFunc   func(v bool)
	TranslateKeyFunc  func(key string, writable bool) (uint64, error)
	TranslateKeysFunc func(keys []string, writable bool) ([]uint64, error)
	TranslateIDFunc   func(id uint64) (string, error)
	TranslateIDsFunc  func(ids []uint64) ([]string, error)
	FindKeysFunc      func(keys ...string) (map[string]uint64, error)
	CreateKeysFunc    func(keys ...string) (map[string]uint64, error)
	ForceSetFunc      func(id uint64, key string) error
	EntryReaderFunc   func(ctx context.Context, offset uint64) (pilosa.TranslateEntryReader, error)
}

func (s *TranslateStore) Close() error {
	return s.CloseFunc()
}

func (s *TranslateStore) MaxID() (uint64, error) {
	return s.MaxIDFunc()
}

func (s *TranslateStore) PartitionID() int {
	return s.PartitionIDFunc()
}

func (s *TranslateStore) ReadOnly() bool {
	return s.ReadOnlyFunc()
}

func (s *TranslateStore) SetReadOnly(v bool) {
	s.SetReadOnlyFunc(v)
}

func (s *TranslateStore) TranslateKey(key string, writable bool) (uint64, error) {
	return s.TranslateKeyFunc(key, writable)
}

func (s *TranslateStore) TranslateKeys(keys []string, writable bool) ([]uint64, error) {
	return s.TranslateKeysFunc(keys, writable)
}

func (s *TranslateStore) TranslateID(id uint64) (string, error) {
	return s.TranslateIDFunc(id)
}

func (s *TranslateStore) TranslateIDs(ids []uint64) ([]string, error) {
	return s.TranslateIDsFunc(ids)
}

func (s *TranslateStore) FindKeys(keys ...string) (map[string]uint64, error) {
	return s.FindKeysFunc(keys...)
}

func (s *TranslateStore) CreateKeys(keys ...string) (map[string]uint64, error) {
	return s.CreateKeysFunc(keys...)
}

func (s *TranslateStore) ForceSet(id uint64, key string) error {
	return s.ForceSetFunc(id, key)
}

func (s *TranslateStore) EntryReader(ctx context.Context, offset uint64) (pilosa.TranslateEntryReader, error) {
	return s.EntryReaderFunc(ctx, offset)
}

func (s *TranslateStore) WriteTo(w io.Writer) (int64, error) {
	return 0, nil
}

func (s *TranslateStore) ReadFrom(r io.Reader) (int64, error) {
	return 0, nil
}

var _ pilosa.TranslateEntryReader = (*TranslateEntryReader)(nil)

type TranslateEntryReader struct {
	CloseFunc     func() error
	ReadEntryFunc func(entry *pilosa.TranslateEntry) error
}

func (r *TranslateEntryReader) Close() error {
	return r.CloseFunc()
}

func (r *TranslateEntryReader) ReadEntry(entry *pilosa.TranslateEntry) error {
	return r.ReadEntryFunc(entry)
}
