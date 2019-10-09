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
	"io/ioutil"
	"os"

	"github.com/pilosa/pilosa/v2"
)

// Index represents a test wrapper for pilosa.Index.
type Index struct {
	*pilosa.Index
}

// newIndex returns a new instance of Index.
func newIndex() *Index {
	path, err := ioutil.TempDir("", "pilosa-index-")
	if err != nil {
		panic(err)
	}
	index, err := pilosa.NewIndex(path, "i")
	if err != nil {
		panic(err)
	}
	return &Index{Index: index}
}

// MustOpenIndex returns a new, opened index at a temporary path. Panic on error.
func MustOpenIndex() *Index {
	index := newIndex()
	if err := index.Open(); err != nil {
		panic(err)
	}
	return index
}

// Close closes the index and removes the underlying data.
func (i *Index) Close() error {
	defer os.RemoveAll(i.Path())
	return i.Index.Close()
}

// Reopen closes the index and reopens it.
func (i *Index) Reopen() error {
	var err error
	if err := i.Index.Close(); err != nil {
		return err
	}

	path, name := i.Path(), i.Name()
	i.Index, err = pilosa.NewIndex(path, name)
	if err != nil {
		return err
	}

	if err := i.Open(); err != nil {
		return err
	}
	return nil
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
