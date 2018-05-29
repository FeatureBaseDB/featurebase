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

package pilosa_test

import (
	"io/ioutil"
	"reflect"
	"testing"

	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/test"
)

// Ensure index can open and retrieve a frame.
func TestIndex_CreateFrameIfNotExists(t *testing.T) {
	index := test.MustOpenIndex()
	defer index.Close()

	// Create frame.
	f, err := index.CreateFrameIfNotExists("f", pilosa.FrameOptions{})
	if err != nil {
		t.Fatal(err)
	} else if f == nil {
		t.Fatal("expected frame")
	}

	// Retrieve existing frame.
	other, err := index.CreateFrameIfNotExists("f", pilosa.FrameOptions{})
	if err != nil {
		t.Fatal(err)
	} else if f.Frame != other.Frame {
		t.Fatal("frame mismatch")
	}

	if f.Frame != index.Frame("f") {
		t.Fatal("frame mismatch")
	}
}

func TestIndex_CreateFrame(t *testing.T) {
	// Ensure time quantum can be set appropriately on a new frame.
	t.Run("TimeQuantum", func(t *testing.T) {
		t.Run("Explicit", func(t *testing.T) {
			index := test.MustOpenIndex()
			defer index.Close()

			// Create frame with explicit quantum.
			f, err := index.CreateFrame("f", pilosa.FrameOptions{TimeQuantum: pilosa.TimeQuantum("YMDH")})
			if err != nil {
				t.Fatal(err)
			} else if q := f.TimeQuantum(); q != pilosa.TimeQuantum("YMDH") {
				t.Fatalf("unexpected frame time quantum: %s", q)
			}
		})
	})

	// Ensure frame can include range columns.
	t.Run("BSIFields", func(t *testing.T) {
		t.Run("OK", func(t *testing.T) {
			index := test.MustOpenIndex()
			defer index.Close()

			// Create frame with schema and verify it exists.
			if f, err := index.CreateFrame("f", pilosa.FrameOptions{
				Fields: []*pilosa.Field{
					{Name: "field0", Type: pilosa.FieldTypeInt, Min: 10, Max: 20},
					{Name: "field1", Type: pilosa.FieldTypeInt, Min: 11, Max: 21},
				},
			}); err != nil {
				t.Fatal(err)
			} else if !reflect.DeepEqual(f.Fields(), []*pilosa.Field{
				{Name: "field0", Type: pilosa.FieldTypeInt, Min: 10, Max: 20},
				{Name: "field1", Type: pilosa.FieldTypeInt, Min: 11, Max: 21},
			}) {
				t.Fatalf("unexpected fields: %#v", f.Fields())
			}

			// Reopen the index & verify the fields are loaded.
			if err := index.Reopen(); err != nil {
				t.Fatal(err)
			} else if f := index.Frame("f"); !reflect.DeepEqual(f.Fields(), []*pilosa.Field{
				{Name: "field0", Type: pilosa.FieldTypeInt, Min: 10, Max: 20},
				{Name: "field1", Type: pilosa.FieldTypeInt, Min: 11, Max: 21},
			}) {
				t.Fatalf("unexpected fields after reopen: %#v", f.Fields())
			}
		})

		t.Run("ErrRangeCacheAllowed", func(t *testing.T) {
			index := test.MustOpenIndex()
			defer index.Close()

			if _, err := index.CreateFrame("f", pilosa.FrameOptions{
				CacheType: pilosa.CacheTypeRanked,
			}); err != nil {
				t.Fatal(err)
			}
		})

		t.Run("BSIFieldsWithCacheTypeNone", func(t *testing.T) {
			index := test.MustOpenIndex()
			defer index.Close()
			if _, err := index.CreateFrame("f", pilosa.FrameOptions{
				CacheType: pilosa.CacheTypeNone,
				CacheSize: uint32(5),
			}); err != nil {
				t.Fatal(err)
			}
		})

		t.Run("ErrFrameFieldsAllowed", func(t *testing.T) {
			index := test.MustOpenIndex()
			defer index.Close()

			if _, err := index.CreateFrame("f", pilosa.FrameOptions{
				Fields: []*pilosa.Field{
					{Name: "field0", Type: pilosa.FieldTypeInt},
				},
			}); err != nil {
				t.Fatal(err)
			}
		})

		t.Run("ErrFieldNameRequired", func(t *testing.T) {
			index := test.MustOpenIndex()
			defer index.Close()

			if _, err := index.CreateFrame("f", pilosa.FrameOptions{
				Fields: []*pilosa.Field{
					{Name: "", Type: pilosa.FieldTypeInt},
				},
			}); err != pilosa.ErrFieldNameRequired {
				t.Fatal(err)
			}
		})

		t.Run("ErrInvalidFieldType", func(t *testing.T) {
			index := test.MustOpenIndex()
			defer index.Close()

			if _, err := index.CreateFrame("f", pilosa.FrameOptions{
				Fields: []*pilosa.Field{
					{Name: "field0", Type: "bad_type"},
				},
			}); err != pilosa.ErrInvalidFieldType {
				t.Fatal(err)
			}
		})

		t.Run("ErrInvalidFieldRange", func(t *testing.T) {
			index := test.MustOpenIndex()
			defer index.Close()

			if _, err := index.CreateFrame("f", pilosa.FrameOptions{
				Fields: []*pilosa.Field{
					{Name: "field0", Type: pilosa.FieldTypeInt, Min: 100, Max: 50},
				},
			}); err != pilosa.ErrInvalidFieldRange {
				t.Fatal(err)
			}
		})
	})
}

// Ensure index can delete a frame.
func TestIndex_DeleteFrame(t *testing.T) {
	index := test.MustOpenIndex()
	defer index.Close()

	// Create frame.
	if _, err := index.CreateFrameIfNotExists("f", pilosa.FrameOptions{}); err != nil {
		t.Fatal(err)
	}

	// Delete frame & verify it's gone.
	if err := index.DeleteFrame("f"); err != nil {
		t.Fatal(err)
	} else if index.Frame("f") != nil {
		t.Fatal("expected nil frame")
	}

	// Delete again to make sure it doesn't error.
	if err := index.DeleteFrame("f"); err != nil {
		t.Fatal(err)
	}
}

// Ensure index can delete a frame.
func TestIndex_InvalidName(t *testing.T) {
	path, err := ioutil.TempDir("", "pilosa-index-")
	if err != nil {
		panic(err)
	}
	index, err := pilosa.NewIndex(path, "ABC")
	if err == nil {
		t.Fatalf("should have gotten an error on index name with caps")
	}
	if index != nil {
		t.Fatalf("unexpected index name %v", index)
	}
}
