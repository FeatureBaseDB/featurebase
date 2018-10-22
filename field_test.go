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
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/roaring"
	"github.com/pilosa/pilosa/test"
)

// Ensure a field can set & read a bsiGroup value.
func TestField_SetValue(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		idx := test.MustOpenIndex()
		defer idx.Close()

		f, err := idx.CreateField("f", pilosa.OptFieldTypeInt(0, 30))
		if err != nil {
			t.Fatal(err)
		}

		// Set value on field.
		if changed, err := f.SetValue(100, 21); err != nil {
			t.Fatal(err)
		} else if !changed {
			t.Fatal("expected change")
		}

		// Read value.
		if value, exists, err := f.Value(100); err != nil {
			t.Fatal(err)
		} else if value != 21 {
			t.Fatalf("unexpected value: %d", value)
		} else if !exists {
			t.Fatal("expected value to exist")
		}

		// Setting value should return no change.
		if changed, err := f.SetValue(100, 21); err != nil {
			t.Fatal(err)
		} else if changed {
			t.Fatal("expected no change")
		}
	})

	t.Run("Overwrite", func(t *testing.T) {
		idx := test.MustOpenIndex()
		defer idx.Close()

		f, err := idx.CreateField("f", pilosa.OptFieldTypeInt(0, 30))
		if err != nil {
			t.Fatal(err)
		}

		// Set value.
		if changed, err := f.SetValue(100, 21); err != nil {
			t.Fatal(err)
		} else if !changed {
			t.Fatal("expected change")
		}

		// Set different value.
		if changed, err := f.SetValue(100, 23); err != nil {
			t.Fatal(err)
		} else if !changed {
			t.Fatal("expected change")
		}

		// Read value.
		if value, exists, err := f.Value(100); err != nil {
			t.Fatal(err)
		} else if value != 23 {
			t.Fatalf("unexpected value: %d", value)
		} else if !exists {
			t.Fatal("expected value to exist")
		}
	})

	t.Run("ErrBSIGroupNotFound", func(t *testing.T) {
		idx := test.MustOpenIndex()
		defer idx.Close()

		f, err := idx.CreateField("f", pilosa.OptFieldTypeDefault())
		if err != nil {
			t.Fatal(err)
		}

		// Set value.
		if _, err := f.SetValue(100, 21); err != pilosa.ErrBSIGroupNotFound {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrBSIGroupValueTooLow", func(t *testing.T) {
		idx := test.MustOpenIndex()
		defer idx.Close()

		f, err := idx.CreateField("f", pilosa.OptFieldTypeInt(20, 30))
		if err != nil {
			t.Fatal(err)
		}

		// Set value.
		if _, err := f.SetValue(100, 15); err != pilosa.ErrBSIGroupValueTooLow {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrBSIGroupValueTooHigh", func(t *testing.T) {
		idx := test.MustOpenIndex()
		defer idx.Close()

		f, err := idx.CreateField("f", pilosa.OptFieldTypeInt(20, 30))
		if err != nil {
			t.Fatal(err)
		}

		// Set value.
		if _, err := f.SetValue(100, 31); err != pilosa.ErrBSIGroupValueTooHigh {
			t.Fatalf("unexpected error: %s", err)
		}
	})
}

func TestField_NameRestriction(t *testing.T) {
	path, err := ioutil.TempDir("", "pilosa-field-")
	if err != nil {
		panic(err)
	}
	field, err := pilosa.NewField(path, "i", ".meta", pilosa.OptFieldTypeDefault())
	if field != nil {
		t.Fatalf("unexpected field name %s", err)
	}
}

// Ensure that field name validation is consistent.
func TestField_NameValidation(t *testing.T) {
	validFieldNames := []string{
		"foo",
		"hyphen-ated",
		"under_score",
		"abc123",
		"trailing_",
	}
	invalidFieldNames := []string{
		"",
		"123abc",
		"x.y",
		"_foo",
		"-bar",
		"abc def",
		"camelCase",
		"UPPERCASE",
		"a12345678901234567890123456789012345678901234567890123456789012345",
	}

	path, err := ioutil.TempDir("", "pilosa-field-")
	if err != nil {
		panic(err)
	}
	for _, name := range validFieldNames {
		_, err := pilosa.NewField(path, "i", name, pilosa.OptFieldTypeDefault())
		if err != nil {
			t.Fatalf("unexpected field name: %s %s", name, err)
		}
	}
	for _, name := range invalidFieldNames {
		_, err := pilosa.NewField(path, "i", name, pilosa.OptFieldTypeDefault())
		if err == nil {
			t.Fatalf("expected error on field name: %s", name)
		}
	}
}

// Ensure can update and delete available shards.
func TestField_AvailableShards(t *testing.T) {
	idx := test.MustOpenIndex()
	defer idx.Close()

	f, err := idx.CreateField("f", pilosa.OptFieldTypeDefault())
	if err != nil {
		t.Fatal(err)
	}

	// Set values on shards 0 & 2, and verify.
	if _, err := f.SetBit(0, 100, nil); err != nil {
		t.Fatal(err)
	} else if _, err := f.SetBit(0, ShardWidth*2, nil); err != nil {
		t.Fatal(err)
	} else if diff := cmp.Diff(f.AvailableShards().Slice(), []uint64{0, 2}); diff != "" {
		t.Fatal(diff)
	}

	// Set remote shards and verify.
	f.AddRemoteAvailableShards(roaring.NewBitmap(1, 2, 4))
	if diff := cmp.Diff(f.AvailableShards().Slice(), []uint64{0, 1, 2, 4}); diff != "" {
		t.Fatal(diff)
	}

	// Delete shards; only local shards should remain.
	f.RemoveAvailableShard(0)
	f.RemoveAvailableShard(1)
	f.RemoveAvailableShard(2)
	f.RemoveAvailableShard(3)
	f.RemoveAvailableShard(4)
	if diff := cmp.Diff(f.AvailableShards().Slice(), []uint64{0, 2}); diff != "" {
		t.Fatal(diff)
	}
}
