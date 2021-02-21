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

	"github.com/pilosa/pilosa/v2"
)

var _ pilosa.TranslateStore = (*TranslateStore)(nil)

type TranslateStore struct {
	CloseFunc         func() error
	MaxIDFunc         func() (uint64, error)
	ReadOnlyFunc      func() bool
	SetReadOnlyFunc   func(v bool)
	TranslateKeyFunc  func(key string) (uint64, error)
	TranslateKeysFunc func(keys []string) ([]uint64, error)
	TranslateIDFunc   func(id uint64) (string, error)
	TranslateIDsFunc  func(ids []uint64) ([]string, error)
	ForceSetFunc      func(id uint64, key string) error
	EntryReaderFunc   func(ctx context.Context, offset uint64) (pilosa.TranslateEntryReader, error)
}

func (s *TranslateStore) Close() error {
	return s.CloseFunc()
}

func (s *TranslateStore) MaxID() (uint64, error) {
	return s.MaxIDFunc()
}

func (s *TranslateStore) ReadOnly() bool {
	return s.ReadOnlyFunc()
}

func (s *TranslateStore) SetReadOnly(v bool) {
	s.SetReadOnlyFunc(v)
}

func (s *TranslateStore) TranslateKey(key string) (uint64, error) {
	return s.TranslateKeyFunc(key)
}

func (s *TranslateStore) TranslateKeys(keys []string) ([]uint64, error) {
	return s.TranslateKeysFunc(keys)
}

func (s *TranslateStore) TranslateID(id uint64) (string, error) {
	return s.TranslateIDFunc(id)
}

func (s *TranslateStore) TranslateIDs(ids []uint64) ([]string, error) {
	return s.TranslateIDsFunc(ids)
}

func (s *TranslateStore) ForceSet(id uint64, key string) error {
	return s.ForceSetFunc(id, key)
}

func (s *TranslateStore) EntryReader(ctx context.Context, offset uint64) (pilosa.TranslateEntryReader, error) {
	return s.EntryReaderFunc(ctx, offset)
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
