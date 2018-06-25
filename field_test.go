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

	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/test"
)

// Ensure field can open and retrieve a view.
func TestField_CreateViewIfNotExists(t *testing.T) {
	f := test.MustOpenField(pilosa.FieldOptions{})
	defer f.Close()

	// Create view.
	view, err := f.CreateViewIfNotExists("v")
	if err != nil {
		t.Fatal(err)
	} else if view == nil {
		t.Fatal("expected view")
	}

	// Retrieve existing view.
	view2, err := f.CreateViewIfNotExists("v")
	if err != nil {
		t.Fatal(err)
	} else if view != view2 {
		t.Fatal("view mismatch")
	}

	if view != f.View("v") {
		t.Fatal("view mismatch")
	}
}

// Ensure field can set its time quantum.
func TestField_SetTimeQuantum(t *testing.T) {
	f := test.MustOpenField(pilosa.FieldOptions{Type: pilosa.FieldTypeTime})
	defer f.Close()

	// Set & retrieve time quantum.
	if err := f.SetTimeQuantum(pilosa.TimeQuantum("YMDH")); err != nil {
		t.Fatal(err)
	} else if q := f.TimeQuantum(); q != pilosa.TimeQuantum("YMDH") {
		t.Fatalf("unexpected quantum: %s", q)
	}

	// Reload field and verify that it is persisted.
	if err := f.Reopen(); err != nil {
		t.Fatal(err)
	} else if q := f.TimeQuantum(); q != pilosa.TimeQuantum("YMDH") {
		t.Fatalf("unexpected quantum (reopen): %s", q)
	}
}

// Ensure a field can set & read a bsiGroup value.
func TestField_SetValue(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		idx := test.MustOpenIndex()
		defer idx.Close()

		f, err := idx.CreateField("f", pilosa.FieldOptions{
			Type: pilosa.FieldTypeInt,
			Min:  0,
			Max:  30,
		})
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

		f, err := idx.CreateField("f", pilosa.FieldOptions{
			Type: pilosa.FieldTypeInt,
			Min:  0,
			Max:  30,
		})
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

		f, err := idx.CreateField("f", pilosa.FieldOptions{
			Type: pilosa.FieldTypeSet,
		})
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

		f, err := idx.CreateField("f", pilosa.FieldOptions{
			Type: pilosa.FieldTypeInt,
			Min:  20,
			Max:  30,
		})
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

		f, err := idx.CreateField("f", pilosa.FieldOptions{
			Type: pilosa.FieldTypeInt,
			Min:  20,
			Max:  30,
		})
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
	field, err := pilosa.NewField(path, "i", ".meta", pilosa.FieldOptions{})
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
		_, err := pilosa.NewField(path, "i", name, pilosa.FieldOptions{})
		if err != nil {
			t.Fatalf("unexpected field name: %s %s", name, err)
		}
	}
	for _, name := range invalidFieldNames {
		_, err := pilosa.NewField(path, "i", name, pilosa.FieldOptions{})
		if err == nil {
			t.Fatalf("expected error on field name: %s", name)
		}
	}
}

// Ensure field can open and retrieve a view.
func TestField_DeleteView(t *testing.T) {
	f := test.MustOpenField(pilosa.FieldOptions{})
	defer f.Close()

	viewName := pilosa.ViewStandard + "_v"

	// Create view.
	view, err := f.CreateViewIfNotExists(viewName)
	if err != nil {
		t.Fatal(err)
	} else if view == nil {
		t.Fatal("expected view")
	}

	err = f.DeleteView(viewName)
	if err != nil {
		t.Fatal(err)
	}

	if f.View(viewName) != nil {
		t.Fatal("view still exists in field")
	}

	// Recreate view with same name, verify that the old view was not reused.
	view2, err := f.CreateViewIfNotExists(viewName)
	if err != nil {
		t.Fatal(err)
	} else if view == view2 {
		t.Fatal("failed to create new view")
	}
}
