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
	"github.com/pilosa/pilosa/pql"
	"github.com/pilosa/pilosa/test"
)

// Ensure frame can open and retrieve a view.
func TestFrame_CreateViewIfNotExists(t *testing.T) {
	f := test.MustOpenFrame()
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

// Ensure frame can set its time quantum.
func TestFrame_SetTimeQuantum(t *testing.T) {
	f := test.MustOpenFrame()
	defer f.Close()

	// Set & retrieve time quantum.
	if err := f.SetTimeQuantum(pilosa.TimeQuantum("YMDH")); err != nil {
		t.Fatal(err)
	} else if q := f.TimeQuantum(); q != pilosa.TimeQuantum("YMDH") {
		t.Fatalf("unexpected quantum: %s", q)
	}

	// Reload frame and verify that it is persisted.
	if err := f.Reopen(); err != nil {
		t.Fatal(err)
	} else if q := f.TimeQuantum(); q != pilosa.TimeQuantum("YMDH") {
		t.Fatalf("unexpected quantum (reopen): %s", q)
	}
}

// Ensure a frame can set & read a field value.
func TestFrame_SetFieldValue(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		idx := test.MustOpenIndex()
		defer idx.Close()

		f, err := idx.CreateFrame("f", pilosa.FrameOptions{
			RangeEnabled: true,
			Fields: []*pilosa.Field{
				{Name: "field0", Type: pilosa.FieldTypeInt, Min: 0, Max: 30},
				{Name: "field1", Type: pilosa.FieldTypeInt, Min: 20, Max: 25},
			},
		})
		if err != nil {
			t.Fatal(err)
		}

		// Set value on first field.
		if changed, err := f.SetFieldValue(100, "field0", 21); err != nil {
			t.Fatal(err)
		} else if !changed {
			t.Fatal("expected change")
		}

		// Set value on same column but different field.
		if changed, err := f.SetFieldValue(100, "field1", 25); err != nil {
			t.Fatal(err)
		} else if !changed {
			t.Fatal("expected change")
		}

		// Read value.
		if value, exists, err := f.FieldValue(100, "field0"); err != nil {
			t.Fatal(err)
		} else if value != 21 {
			t.Fatalf("unexpected value: %d", value)
		} else if !exists {
			t.Fatal("expected value to exist")
		}

		// Setting value should return no change.
		if changed, err := f.SetFieldValue(100, "field0", 21); err != nil {
			t.Fatal(err)
		} else if changed {
			t.Fatal("expected no change")
		}
	})

	t.Run("Overwrite", func(t *testing.T) {
		idx := test.MustOpenIndex()
		defer idx.Close()

		f, err := idx.CreateFrame("f", pilosa.FrameOptions{
			RangeEnabled: true,
			Fields: []*pilosa.Field{
				{Name: "field0", Type: pilosa.FieldTypeInt, Min: 0, Max: 30},
			},
		})
		if err != nil {
			t.Fatal(err)
		}

		// Set value.
		if changed, err := f.SetFieldValue(100, "field0", 21); err != nil {
			t.Fatal(err)
		} else if !changed {
			t.Fatal("expected change")
		}

		// Set different value.
		if changed, err := f.SetFieldValue(100, "field0", 23); err != nil {
			t.Fatal(err)
		} else if !changed {
			t.Fatal("expected change")
		}

		// Read value.
		if value, exists, err := f.FieldValue(100, "field0"); err != nil {
			t.Fatal(err)
		} else if value != 23 {
			t.Fatalf("unexpected value: %d", value)
		} else if !exists {
			t.Fatal("expected value to exist")
		}
	})

	t.Run("ErrFieldNotFound", func(t *testing.T) {
		idx := test.MustOpenIndex()
		defer idx.Close()

		f, err := idx.CreateFrame("f", pilosa.FrameOptions{
			RangeEnabled: true,
			Fields: []*pilosa.Field{
				{Name: "field0", Type: pilosa.FieldTypeInt, Min: 0, Max: 30},
			},
		})
		if err != nil {
			t.Fatal(err)
		}

		// Set value.
		if _, err := f.SetFieldValue(100, "no_such_field", 21); err != pilosa.ErrFieldNotFound {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrFieldValueTooLow", func(t *testing.T) {
		idx := test.MustOpenIndex()
		defer idx.Close()

		f, err := idx.CreateFrame("f", pilosa.FrameOptions{
			RangeEnabled: true,
			Fields: []*pilosa.Field{
				{Name: "field0", Type: pilosa.FieldTypeInt, Min: 20, Max: 30},
			},
		})
		if err != nil {
			t.Fatal(err)
		}

		// Set value.
		if _, err := f.SetFieldValue(100, "field0", 15); err != pilosa.ErrFieldValueTooLow {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrFieldValueTooHigh", func(t *testing.T) {
		idx := test.MustOpenIndex()
		defer idx.Close()

		f, err := idx.CreateFrame("f", pilosa.FrameOptions{
			RangeEnabled: true,
			Fields: []*pilosa.Field{
				{Name: "field0", Type: pilosa.FieldTypeInt, Min: 20, Max: 30},
			},
		})
		if err != nil {
			t.Fatal(err)
		}

		// Set value.
		if _, err := f.SetFieldValue(100, "field0", 31); err != pilosa.ErrFieldValueTooHigh {
			t.Fatalf("unexpected error: %s", err)
		}
	})
}

func TestFrame_NameRestriction(t *testing.T) {
	path, err := ioutil.TempDir("", "pilosa-frame-")
	if err != nil {
		panic(err)
	}
	frame, err := pilosa.NewFrame(path, "i", ".meta")
	if frame != nil {
		t.Fatalf("unexpected frame name %s", err)
	}
}

// Ensure that frame name validation is consistent.
func TestFrame_NameValidation(t *testing.T) {
	validFrameNames := []string{
		"foo",
		"hyphen-ated",
		"under_score",
		"abc123",
		"trailing_",
	}
	invalidFrameNames := []string{
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

	path, err := ioutil.TempDir("", "pilosa-frame-")
	if err != nil {
		panic(err)
	}
	for _, name := range validFrameNames {
		_, err := pilosa.NewFrame(path, "i", name)
		if err != nil {
			t.Fatalf("unexpected frame name: %s %s", name, err)
		}
	}
	for _, name := range invalidFrameNames {
		_, err := pilosa.NewFrame(path, "i", name)
		if err == nil {
			t.Fatalf("expected error on frame name: %s", name)
		}
	}
}

// Ensure that frame RowLable validation is consistent.
func TestFrame_RowLabelValidation(t *testing.T) {
	validRowLabels := []string{
		"",
		"foo",
		"hyphen-ated",
		"under_score",
		"abc123",
		"trailing_",
		"camelCase",
		"UPPERCASE",
	}
	invalidRowLabels := []string{
		"123abc",
		"x.y",
		"_foo",
		"-bar",
		"abc def",
		"a12345678901234567890123456789012345678901234567890123456789012345",
	}

	path, err := ioutil.TempDir("", "pilosa-frame-")
	if err != nil {
		panic(err)
	}
	f, err := pilosa.NewFrame(path, "i", "f")
	if err != nil {
		t.Fatalf("unexpected frame error: %s", err)
	}

	for _, label := range validRowLabels {
		if err := f.SetRowLabel(label); err != nil {
			t.Fatalf("unexpected row label: %s %s", label, err)
		}
	}
	for _, label := range invalidRowLabels {
		if err := f.SetRowLabel(label); err == nil {
			t.Fatalf("expected error on row label: %s", label)
		}
	}

}

// Ensure frame can open and retrieve a view.
func TestFrame_DeleteView(t *testing.T) {
	f := test.MustOpenFrame()
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
		t.Fatal("view still exists in frame")
	}

	// Recreate view with same name, verify that the old view was not reused.
	view2, err := f.CreateViewIfNotExists(viewName)
	if err != nil {
		t.Fatal(err)
	} else if view == view2 {
		t.Fatal("failed to create new view")
	}
}

// Ensure a field can adjust to its baseValue.
func TestField_BaseValue(t *testing.T) {
	f0 := &pilosa.Field{
		Name: "f0",
		Type: pilosa.FieldTypeInt,
		Min:  -100,
		Max:  900,
	}
	f1 := &pilosa.Field{
		Name: "f1",
		Type: pilosa.FieldTypeInt,
		Min:  0,
		Max:  1000,
	}

	f2 := &pilosa.Field{
		Name: "f2",
		Type: pilosa.FieldTypeInt,
		Min:  100,
		Max:  1100,
	}

	t.Run("Normal Condition", func(t *testing.T) {

		for _, tt := range []struct {
			f             *pilosa.Field
			op            pql.Token
			val           int64
			expBaseValue  uint64
			expOutOfRange bool
		}{
			// LT
			{f0, pql.LT, 5, 105, false},
			{f0, pql.LT, -8, 92, false},
			{f0, pql.LT, -108, 0, true},
			{f0, pql.LT, 1005, 1000, false},
			{f0, pql.LT, 0, 100, false},

			{f1, pql.LT, 5, 5, false},
			{f1, pql.LT, -8, 0, true},
			{f1, pql.LT, 1005, 1000, false},
			{f1, pql.LT, 0, 0, false},

			{f2, pql.LT, 5, 0, true},
			{f2, pql.LT, -8, 0, true},
			{f2, pql.LT, 105, 5, false},
			{f2, pql.LT, 1105, 1000, false},

			// GT
			{f0, pql.GT, -105, 0, false},
			{f0, pql.GT, 5, 105, false},
			{f0, pql.GT, 905, 0, true},
			{f0, pql.GT, 0, 100, false},

			{f1, pql.GT, 5, 5, false},
			{f1, pql.GT, -8, 0, false},
			{f1, pql.GT, 1005, 0, true},
			{f1, pql.GT, 0, 0, false},

			{f2, pql.GT, 5, 0, false},
			{f2, pql.GT, -8, 0, false},
			{f2, pql.GT, 105, 5, false},
			{f2, pql.GT, 1105, 0, true},

			// EQ
			{f0, pql.EQ, -105, 0, true},
			{f0, pql.EQ, 5, 105, false},
			{f0, pql.EQ, 905, 0, true},
			{f0, pql.EQ, 0, 100, false},

			{f1, pql.EQ, 5, 5, false},
			{f1, pql.EQ, -8, 0, true},
			{f1, pql.EQ, 1005, 0, true},
			{f1, pql.EQ, 0, 0, false},

			{f2, pql.EQ, 5, 0, true},
			{f2, pql.EQ, -8, 0, true},
			{f2, pql.EQ, 105, 5, false},
			{f2, pql.EQ, 1105, 0, true},
		} {
			bv, oor := tt.f.BaseValue(tt.op, tt.val)
			if oor != tt.expOutOfRange {
				t.Fatalf("baseValue calculation on %s op %s, expected outOfRange %v, got %v", tt.f.Name, tt.op, tt.expOutOfRange, oor)
			} else if !reflect.DeepEqual(bv, tt.expBaseValue) {
				t.Fatalf("baseValue calculation on %s, expected value %v, got %v", tt.f.Name, tt.expBaseValue, bv)
			}
		}
	})

	t.Run("Betwween Condition", func(t *testing.T) {
		for _, tt := range []struct {
			f               *pilosa.Field
			predMin         int64
			predMax         int64
			expBaseValueMin uint64
			expBaseValueMax uint64
			expOutOfRange   bool
		}{

			{f0, -205, -105, 0, 0, true},
			{f0, -105, 80, 0, 180, false},
			{f0, 5, 20, 105, 120, false},
			{f0, 20, 1005, 120, 1000, false},
			{f0, 1005, 2000, 0, 0, true},

			{f1, -105, -5, 0, 0, true},
			{f1, -5, 20, 0, 20, false},
			{f1, 5, 20, 5, 20, false},
			{f1, 20, 1005, 20, 1000, false},
			{f1, 1005, 2000, 0, 0, true},

			{f2, 5, 95, 0, 0, true},
			{f2, 95, 120, 0, 20, false},
			{f2, 105, 120, 5, 20, false},
			{f2, 120, 1105, 20, 1000, false},
			{f2, 1105, 2000, 0, 0, true},
		} {
			min, max, oor := tt.f.BaseValueBetween(tt.predMin, tt.predMax)
			if oor != tt.expOutOfRange {
				t.Fatalf("baseValueBetween calculation on %s, expected outOfRange %v, got %v", tt.f.Name, tt.expOutOfRange, oor)
			} else if !reflect.DeepEqual(min, tt.expBaseValueMin) || !reflect.DeepEqual(max, tt.expBaseValueMax) {
				t.Fatalf("baseValueBetween calculation on %s, expected min/max %v/%v, got %v/%v", tt.f.Name, tt.expBaseValueMin, tt.expBaseValueMax, min, max)
			}
		}
	})
}
