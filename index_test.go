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

			// Set index time quantum.
			if err := index.SetTimeQuantum(pilosa.TimeQuantum("YM")); err != nil {
				t.Fatal(err)
			}

			// Create frame with explicit quantum.
			f, err := index.CreateFrame("f", pilosa.FrameOptions{TimeQuantum: pilosa.TimeQuantum("YMDH")})
			if err != nil {
				t.Fatal(err)
			} else if q := f.TimeQuantum(); q != pilosa.TimeQuantum("YMDH") {
				t.Fatalf("unexpected frame time quantum: %s", q)
			}
		})

		t.Run("Inherited", func(t *testing.T) {
			index := test.MustOpenIndex()
			defer index.Close()

			// Set index time quantum.
			if err := index.SetTimeQuantum(pilosa.TimeQuantum("YM")); err != nil {
				t.Fatal(err)
			}

			// Create frame.
			f, err := index.CreateFrame("f", pilosa.FrameOptions{})
			if err != nil {
				t.Fatal(err)
			} else if q := f.TimeQuantum(); q != pilosa.TimeQuantum("YM") {
				t.Fatalf("unexpected frame time quantum: %s", q)
			}
		})
	})

	// Ensure frame can include range columns.
	t.Run("RangeEnabled", func(t *testing.T) {
		t.Run("OK", func(t *testing.T) {
			index := test.MustOpenIndex()
			defer index.Close()

			// Create frame with schema and verify it exists.
			if f, err := index.CreateFrame("f", pilosa.FrameOptions{
				RangeEnabled: true,
				Fields: []*pilosa.Field{
					{Name: "field0", Type: pilosa.FieldTypeInt, Min: 10, Max: 20},
					{Name: "field1", Type: pilosa.FieldTypeInt, Min: 11, Max: 21},
				},
			}); err != nil {
				t.Fatal(err)
			} else if !reflect.DeepEqual(f.Schema(), &pilosa.FrameSchema{
				Fields: []*pilosa.Field{
					{Name: "field0", Type: pilosa.FieldTypeInt, Min: 10, Max: 20},
					{Name: "field1", Type: pilosa.FieldTypeInt, Min: 11, Max: 21},
				},
			}) {
				t.Fatalf("unexpected schema: %#v", f.Schema())
			}

			// Reopen the index & verify the fields are loaded.
			if err := index.Reopen(); err != nil {
				t.Fatal(err)
			} else if f := index.Frame("f"); !reflect.DeepEqual(f.Schema(), &pilosa.FrameSchema{
				Fields: []*pilosa.Field{
					{Name: "field0", Type: pilosa.FieldTypeInt, Min: 10, Max: 20},
					{Name: "field1", Type: pilosa.FieldTypeInt, Min: 11, Max: 21},
				},
			}) {
				t.Fatalf("unexpected schema after reopen: %#v", f.Schema())
			}
		})

		t.Run("ErrInverseRangeNotAllowed", func(t *testing.T) {
			index := test.MustOpenIndex()
			defer index.Close()

			if _, err := index.CreateFrame("f", pilosa.FrameOptions{
				InverseEnabled: true,
				RangeEnabled:   true,
			}); err != pilosa.ErrInverseRangeNotAllowed {
				t.Fatal(err)
			}
		})

		t.Run("ErrRangeCacheNotAllowed", func(t *testing.T) {
			index := test.MustOpenIndex()
			defer index.Close()

			if _, err := index.CreateFrame("f", pilosa.FrameOptions{
				RangeEnabled: true,
				CacheType:    pilosa.CacheTypeRanked,
			}); err != pilosa.ErrRangeCacheNotAllowed {
				t.Fatal(err)
			}
		})

		t.Run("RangeEnabledWithCacheTypeNone", func(t *testing.T) {
			index := test.MustOpenIndex()
			defer index.Close()
			if _, err := index.CreateFrame("f", pilosa.FrameOptions{
				RangeEnabled: true,
				CacheType:    pilosa.CacheTypeNone,
				CacheSize: uint32(5),
			}); err != nil {
				t.Fatal(err)
			}
		})

		t.Run("ErrFrameFieldsNotAllowed", func(t *testing.T) {
			index := test.MustOpenIndex()
			defer index.Close()

			if _, err := index.CreateFrame("f", pilosa.FrameOptions{
				Fields: []*pilosa.Field{
					{Name: "field0", Type: pilosa.FieldTypeInt},
				},
			}); err != pilosa.ErrFrameFieldsNotAllowed {
				t.Fatal(err)
			}
		})

		t.Run("ErrFieldNameRequired", func(t *testing.T) {
			index := test.MustOpenIndex()
			defer index.Close()

			if _, err := index.CreateFrame("f", pilosa.FrameOptions{
				RangeEnabled: true,
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
				RangeEnabled: true,
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
				RangeEnabled: true,
				Fields: []*pilosa.Field{
					{Name: "field0", Type: pilosa.FieldTypeInt, Min: 100, Max: 50},
				},
			}); err != pilosa.ErrInvalidFieldRange {
				t.Fatal(err)
			}
		})
	})

	// Ensure frame cannot be created with a matching row label.
	t.Run("ErrColumnRowLabelEqual", func(t *testing.T) {
		t.Run("Explicit", func(t *testing.T) {
			index := test.MustOpenIndex()
			defer index.Close()

			_, err := index.CreateFrame("f", pilosa.FrameOptions{RowLabel: pilosa.DefaultColumnLabel})
			if err != pilosa.ErrColumnRowLabelEqual {
				t.Fatalf("unexpected error: %s", err)
			}
		})

		t.Run("Default", func(t *testing.T) {
			index := test.MustOpenIndex()
			defer index.Close()
			if err := index.SetColumnLabel(pilosa.DefaultRowLabel); err != nil {
				t.Fatal(err)
			}

			_, err := index.CreateFrame("f", pilosa.FrameOptions{})
			if err != pilosa.ErrColumnRowLabelEqual {
				t.Fatalf("unexpected error: %s", err)
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

// Ensure index can set the default time quantum.
func TestIndex_SetTimeQuantum(t *testing.T) {
	index := test.MustOpenIndex()
	defer index.Close()

	// Set & retrieve time quantum.
	if err := index.SetTimeQuantum(pilosa.TimeQuantum("YMDH")); err != nil {
		t.Fatal(err)
	} else if q := index.TimeQuantum(); q != pilosa.TimeQuantum("YMDH") {
		t.Fatalf("unexpected quantum: %s", q)
	}

	// Reload index and verify that it is persisted.
	if err := index.Reopen(); err != nil {
		t.Fatal(err)
	} else if q := index.TimeQuantum(); q != pilosa.TimeQuantum("YMDH") {
		t.Fatalf("unexpected quantum (reopen): %s", q)
	}
}
