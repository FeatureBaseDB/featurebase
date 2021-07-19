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

package test

import (
	"context"
	"testing"

	"github.com/molecula/featurebase/v2"
	"github.com/molecula/featurebase/v2/testhook"
)

// Index represents a test wrapper for pilosa.Index.
type Index struct {
	*pilosa.Index
}

// newIndex returns a new instance of Index.
func newIndex(tb testing.TB) *Index {
	path, err := testhook.TempDir(tb, "pilosa-index-")
	if err != nil {
		panic(err)
	}
	h := pilosa.NewHolder(path, pilosa.DefaultHolderConfig())
	testhook.Cleanup(tb, func() {
		h.Close()
	})
	index, err := h.CreateIndex("i", pilosa.IndexOptions{})
	if err != nil {
		panic(err)
	}
	return &Index{Index: index}
}

// MustOpenIndex returns a new, opened index at a temporary path. Panic on error.
func MustOpenIndex(tb testing.TB) *Index {
	index := newIndex(tb)
	return index
}

// Close closes the index and removes the underlying data.
func (i *Index) Close() error {
	return i.Index.Close()
}

// Reopen closes the index and reopens it.
func (i *Index) Reopen() error {
	if err := i.Index.Close(); err != nil {
		return err
	}
	schema, err := i.Schemator.Schema(context.Background())
	if err != nil {
		return err
	}
	return i.OpenWithSchema(schema[i.Name()])
}

// CreateField creates a field with the given options.
func (i *Index) CreateField(name string, opts ...pilosa.FieldOption) (*Field, error) {
	f, err := i.Index.CreateField(name, opts...)
	if err != nil {
		return nil, err
	}
	return &Field{Field: f}, nil
}

// CreateFieldIfNotExists creates a field with the given options if it doesn't exist.
func (i *Index) CreateFieldIfNotExists(name string, opts ...pilosa.FieldOption) (*Field, error) {
	f, err := i.Index.CreateFieldIfNotExists(name, opts...)
	if err != nil {
		return nil, err
	}
	return &Field{Field: f}, nil
}
