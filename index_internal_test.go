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

	"github.com/pilosa/pilosa/v2/testhook"
)

// mustOpenIndex returns a new, opened index at a temporary path. Panic on error.
func mustOpenIndex(tb testing.TB, opt IndexOptions) *Index {
	path, err := testhook.TempDirInDir(tb, *TempDir, "pilosa-index-")
	if err != nil {
		panic(err)
	}
	h := NewHolder(path, nil)
	index, err := h.CreateIndex("i", opt)
	testhook.Cleanup(tb, func() {
		h.Close()
	})

	if err != nil {
		panic(err)
	}

	index.keys = opt.Keys
	index.trackExistence = opt.TrackExistence

	return index
}

// reopen closes the index and reopens it.
func (i *Index) reopen() error {
	if err := i.Close(); err != nil {
		return err
	}
	if err := i.Open(); err != nil {
		return err
	}
	return nil
}

// Ensure that deleting the existence field is handled properly.
func TestIndex_Existence_Delete(t *testing.T) {
	// Create Index (with existence tracking).
	index := mustOpenIndex(t, IndexOptions{TrackExistence: true})
	defer index.Close()

	// Ensure existence field has been created.
	ef := index.Field(existenceFieldName)
	if ef == nil {
		t.Fatalf("expected field to have been created: %s", existenceFieldName)
	} else if !index.trackExistence {
		t.Fatalf("expected index.trackExistence to be true")
	} else if index.existenceFld == nil {
		t.Fatalf("expected index.existenceField to be non-nil")
	}

	// Delete existence field.
	if err := index.DeleteField(existenceFieldName); err != nil {
		t.Fatal(err)
	}

	// Re-open index.
	if err := index.reopen(); err != nil {
		t.Fatal(err)
	}

	// Ensure existence field no longer exists.
	ef = index.Field(existenceFieldName)
	if ef != nil {
		t.Fatalf("expected field to have been deleted: %s", existenceFieldName)
	} else if index.trackExistence {
		t.Fatalf("expected index.trackExistence to be false")
	} else if index.existenceFld != nil {
		t.Fatalf("expected index.existenceField to be nil")
	}
}
