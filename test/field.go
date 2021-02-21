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
	"testing"

	"github.com/pilosa/pilosa/v2"
)

// Field represents a test wrapper for pilosa.Field.
type Field struct {
	*pilosa.Field
}

// newField returns a new instance of Field d/0.
func newField(opts pilosa.FieldOption) *Field {
	path, err := ioutil.TempDir("", "pilosa-field-")
	if err != nil {
		panic(err)
	}
	field, err := pilosa.NewField(path, "i", "f", opts)
	if err != nil {
		panic(err)
	}
	return &Field{Field: field}
}

// mustOpenField returns a new, opened field at a temporary path. Panic on error.
func mustOpenField(opts pilosa.FieldOption) *Field {
	f := newField(opts)
	if err := f.Open(); err != nil {
		panic(err)
	}
	return f
}

// close closes the field and removes the underlying data.
func (f *Field) close() error { // nolint: unparam
	defer os.RemoveAll(f.Path())
	return f.Field.Close()
}

// reopen closes the index and reopens it.
func (f *Field) reopen() error {
	var err error
	if err := f.Field.Close(); err != nil {
		return err
	}

	path, index, name := f.Path(), f.Index(), f.Name()
	f.Field, err = pilosa.NewField(path, index, name, pilosa.OptFieldTypeDefault())
	if err != nil {
		return err
	}

	if err := f.Open(); err != nil {
		return err
	}
	return nil
}

// Ensure field can set its cache
func TestField_SetCacheSize(t *testing.T) {
	f := mustOpenField(pilosa.OptFieldTypeDefault())
	defer f.close()
	cacheSize := uint32(100)

	// Set & retrieve field cache size.
	if err := f.SetCacheSize(cacheSize); err != nil {
		t.Fatal(err)
	} else if q := f.CacheSize(); q != cacheSize {
		t.Fatalf("unexpected field cache size: %d", q)
	}

	// Reload field and verify that it is persisted.
	if err := f.reopen(); err != nil {
		t.Fatal(err)
	} else if q := f.CacheSize(); q != cacheSize {
		t.Fatalf("unexpected field cache size (reopen): %d", q)
	}
}
