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
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/pilosa/pilosa/v2/pql"
	"github.com/pilosa/pilosa/v2/roaring"
)

// Ensure a bsiGroup can adjust to its baseValue.
func TestBSIGroup_BaseValue(t *testing.T) {
	b0 := &bsiGroup{
		Name:     "b0",
		Type:     bsiGroupTypeInt,
		Base:     -100,
		BitDepth: 10,
		Min:      -1000,
		Max:      1000,
	}
	b1 := &bsiGroup{
		Name:     "b1",
		Type:     bsiGroupTypeInt,
		Base:     0,
		BitDepth: 8,
		Min:      -255,
		Max:      255,
	}
	b2 := &bsiGroup{
		Name:     "b2",
		Type:     bsiGroupTypeInt,
		Base:     100,
		BitDepth: 11,
		Min:      math.MinInt64,
		Max:      math.MaxInt64,
	}

	t.Run("Normal Condition", func(t *testing.T) {
		for i, tt := range []struct {
			f             *bsiGroup
			op            pql.Token
			val           int64
			expBaseValue  int64
			expOutOfRange bool
		}{
			// LT
			{b0, pql.LT, 5, 105, false},
			{b0, pql.LT, -8, 92, false},
			{b0, pql.LT, -108, -8, false},
			{b0, pql.LT, 1005, 1023, false},
			{b0, pql.LT, 0, 100, false},

			{b1, pql.LT, 5, 5, false},
			{b1, pql.LT, -8, -8, false},
			{b1, pql.LT, 1005, 255, false},
			{b1, pql.LT, 0, 0, false},

			{b2, pql.LT, 5, -95, false},
			{b2, pql.LT, -8, -108, false},
			{b2, pql.LT, 105, 5, false},
			{b2, pql.LT, 1105, 1005, false},

			// GT
			{b0, pql.GT, -5, 95, false},
			{b0, pql.GT, 5, 105, false},
			{b0, pql.GT, 905, 1005, false},
			{b0, pql.GT, 0, 100, false},

			{b1, pql.GT, 5, 5, false},
			{b1, pql.GT, -8, -8, false},
			{b1, pql.GT, 1005, 0, true},
			{b1, pql.GT, 0, 0, false},

			{b2, pql.GT, 5, -95, false},
			{b2, pql.GT, -8, -108, false},
			{b2, pql.GT, 105, 5, false},
			{b2, pql.GT, 1105, 1005, false},

			// EQ
			{b0, pql.EQ, -105, -5, false},
			{b0, pql.EQ, 5, 105, false},
			{b0, pql.EQ, 905, 1005, false},
			{b0, pql.EQ, 0, 100, false},

			{b1, pql.EQ, 5, 5, false},
			{b1, pql.EQ, -8, -8, false},
			{b1, pql.EQ, 1005, 0, true},
			{b1, pql.EQ, 0, 0, false},

			{b2, pql.EQ, 5, -95, false},
			{b2, pql.EQ, -8, -108, false},
			{b2, pql.EQ, 105, 5, false},
			{b2, pql.EQ, 1105, 1005, false},
		} {
			t.Run(fmt.Sprint(i), func(t *testing.T) {
				bv, oor := tt.f.baseValue(tt.op, tt.val)
				if oor != tt.expOutOfRange || !reflect.DeepEqual(bv, tt.expBaseValue) {
					t.Errorf("%s) baseValue(%s, %v)=(%v, %v), expected (%v, %v)", tt.f.Name, tt.op, tt.val, bv, oor, tt.expBaseValue, tt.expOutOfRange)
				}
			})
		}
	})

	t.Run("Between Condition", func(t *testing.T) {
		for i, tt := range []struct {
			f               *bsiGroup
			predMin         int64
			predMax         int64
			expBaseValueMin int64
			expBaseValueMax int64
			expOutOfRange   bool
		}{

			{b0, -205, -105, -105, -5, false},
			{b0, -105, 80, -5, 180, false},
			{b0, 5, 20, 105, 120, false},
			{b0, 20, 1005, 120, 1023, false},
			{b0, 1005, 2000, 0, 0, true},

			{b1, -105, -5, -105, -5, false},
			{b1, -5, 20, -5, 20, false},
			{b1, 5, 20, 5, 20, false},
			{b1, 20, 1005, 20, 255, false},
			{b1, 1005, 2000, 0, 0, true},

			{b2, 5, 95, -95, -5, false},
			{b2, 95, 120, -5, 20, false},
			{b2, 105, 120, 5, 20, false},
			{b2, 120, 1105, 20, 1005, false},
			{b2, 1105, 2000, 1005, 1900, false},
		} {
			min, max, oor := tt.f.baseValueBetween(tt.predMin, tt.predMax)
			if !reflect.DeepEqual(min, tt.expBaseValueMin) || !reflect.DeepEqual(max, tt.expBaseValueMax) || oor != tt.expOutOfRange {
				t.Errorf("%d. %s) baseValueBetween(%v, %v)=(%v, %v, %v), expected (%v, %v, %v)", i, tt.f.Name, tt.predMin, tt.predMax, min, max, oor, tt.expBaseValueMin, tt.expBaseValueMax, tt.expOutOfRange)
			}
		}
	})
}

// Ensure field can open and retrieve a view.
func TestField_DeleteView(t *testing.T) {
	f := MustOpenField(OptFieldTypeDefault())
	defer f.Close()

	viewName := viewStandard + "_v"

	// Create view.
	view, err := f.createViewIfNotExists(viewName)
	if err != nil {
		t.Fatal(err)
	} else if view == nil {
		t.Fatal("expected view")
	}

	err = f.deleteView(viewName)
	if err != nil {
		t.Fatal(err)
	}

	if f.view(viewName) != nil {
		t.Fatal("view still exists in field")
	}

	// Recreate view with same name, verify that the old view was not reused.
	view2, err := f.createViewIfNotExists(viewName)
	if err != nil {
		t.Fatal(err)
	} else if view == view2 {
		t.Fatal("failed to create new view")
	}
}

// TestField represents a test wrapper for Field.
type TestField struct {
	*Field
}

// NewTestField returns a new instance of TestField d/0.
func NewTestField(opts FieldOption) *TestField {
	path, err := ioutil.TempDir(*TempDir, "pilosa-field-")
	if err != nil {
		panic(err)
	}
	field, err := NewField(path, "i", "f", opts)
	if err != nil {
		panic(err)
	}
	return &TestField{Field: field}
}

// MustOpenField returns a new, opened field at a temporary path. Panic on error.
func MustOpenField(opts FieldOption) *TestField {
	f := NewTestField(opts)
	if err := f.Open(); err != nil {
		panic(err)
	}
	return f
}

// Close closes the field and removes the underlying data.
func (f *TestField) Close() error {
	defer os.RemoveAll(f.Path())
	return f.Field.Close()
}

// Reopen closes the index and reopens it.
func (f *TestField) Reopen() error {
	var err error
	if err := f.Field.Close(); err != nil {
		return err
	}

	path, index, name := f.Path(), f.Index(), f.Name()
	f.Field, err = NewField(path, index, name, OptFieldTypeDefault())
	if err != nil {
		return err
	}

	if err := f.Open(); err != nil {
		return err
	}
	return nil
}

func (f *TestField) MustSetBit(row, col uint64, ts ...time.Time) {
	if len(ts) == 0 {
		_, err := f.Field.SetBit(row, col, nil)
		if err != nil {
			panic(err)
		}
	}
	for _, t := range ts {
		_, err := f.Field.SetBit(row, col, &t)
		if err != nil {
			panic(err)
		}
	}
}

// Ensure field can open and retrieve a view.
func TestField_CreateViewIfNotExists(t *testing.T) {
	f := MustOpenField(OptFieldTypeDefault())
	defer f.Close()

	// Create view.
	view, err := f.createViewIfNotExists("v")
	if err != nil {
		t.Fatal(err)
	} else if view == nil {
		t.Fatal("expected view")
	}

	// Retrieve existing view.
	view2, err := f.createViewIfNotExists("v")
	if err != nil {
		t.Fatal(err)
	} else if view != view2 {
		t.Fatal("view mismatch")
	}

	if view != f.view("v") {
		t.Fatal("view mismatch")
	}
}

func TestField_SetTimeQuantum(t *testing.T) {
	f := MustOpenField(OptFieldTypeTime(TimeQuantum("")))
	defer f.Close()

	// Set & retrieve time quantum.
	if err := f.setTimeQuantum(TimeQuantum("YMDH")); err != nil {
		t.Fatal(err)
	} else if q := f.TimeQuantum(); q != TimeQuantum("YMDH") {
		t.Fatalf("unexpected quantum: %s", q)
	}

	// Reload field and verify that it is persisted.
	if err := f.Reopen(); err != nil {
		t.Fatal(err)
	} else if q := f.TimeQuantum(); q != TimeQuantum("YMDH") {
		t.Fatalf("unexpected quantum (reopen): %s", q)
	}
}

func TestField_RowTime(t *testing.T) {
	f := MustOpenField(OptFieldTypeTime(TimeQuantum("")))
	defer f.Close()

	if err := f.setTimeQuantum(TimeQuantum("YMDH")); err != nil {
		t.Fatal(err)
	}

	f.MustSetBit(1, 1, time.Date(2010, time.January, 5, 12, 0, 0, 0, time.UTC))
	f.MustSetBit(1, 2, time.Date(2011, time.January, 5, 12, 0, 0, 0, time.UTC))
	f.MustSetBit(1, 3, time.Date(2010, time.February, 5, 12, 0, 0, 0, time.UTC))
	f.MustSetBit(1, 4, time.Date(2010, time.January, 6, 12, 0, 0, 0, time.UTC))
	f.MustSetBit(1, 5, time.Date(2010, time.January, 5, 13, 0, 0, 0, time.UTC))

	if r, err := f.RowTime(1, time.Date(2010, time.November, 5, 12, 0, 0, 0, time.UTC), "Y"); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(r.Columns(), []uint64{1, 3, 4, 5}) {
		t.Fatalf("wrong columns: %#v", r.Columns())
	}

	if r, err := f.RowTime(1, time.Date(2010, time.February, 7, 13, 0, 0, 0, time.UTC), "YM"); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(r.Columns(), []uint64{3}) {
		t.Fatalf("wrong columns: %#v", r.Columns())
	}

	if r, err := f.RowTime(1, time.Date(2010, time.February, 7, 13, 0, 0, 0, time.UTC), "M"); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(r.Columns(), []uint64{3}) {
		t.Fatalf("wrong columns: %#v", r.Columns())
	}

	if r, err := f.RowTime(1, time.Date(2010, time.January, 5, 12, 0, 0, 0, time.UTC), "MD"); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(r.Columns(), []uint64{1, 5}) {
		t.Fatalf("wrong columns: %#v", r.Columns())
	}

	if r, err := f.RowTime(1, time.Date(2010, time.January, 5, 13, 0, 0, 0, time.UTC), "MDH"); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(r.Columns(), []uint64{5}) {
		t.Fatalf("wrong columns: %#v", r.Columns())
	}

}

func TestField_PersistAvailableShards(t *testing.T) {
	f := MustOpenField(OptFieldTypeDefault())

	// bm represents remote available shards.
	bm := roaring.NewBitmap(1, 2, 3)

	if err := f.AddRemoteAvailableShards(bm); err != nil {
		t.Fatal(err)
	}

	// Reload field and verify that shard data is persisted.
	if err := f.Reopen(); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(f.remoteAvailableShards.Slice(), bm.Slice()) {
		t.Fatalf("unexpected available shards (reopen). expected: %v, but got: %v", bm.Slice(), f.remoteAvailableShards.Slice())
	}

}

// Ensure that persisting available shards having a smaller footprint (for example,
// when going from a bitmap to a smaller, RLE representation) succeeds.
func TestField_PersistAvailableShardsFootprint(t *testing.T) {
	f := MustOpenField(OptFieldTypeDefault())

	// bm represents remote available shards.
	bm := roaring.NewBitmap()
	for i := uint64(0); i < 1204; i += 2 {
		_, err := bm.Add(i)
		if err != nil {
			t.Fatalf("adding bits: %v", err)
		}
	}

	if err := f.AddRemoteAvailableShards(bm); err != nil {
		t.Fatal(err)
	}

	// Reload field and verify that shard data is persisted.
	if err := f.Reopen(); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(f.remoteAvailableShards.Slice(), bm.Slice()) {
		t.Fatalf("unexpected available shards (reopen). expected: %v, but got: %v", bm.Slice(), f.remoteAvailableShards.Slice())
	}

	bm1 := roaring.NewBitmap()
	for i := uint64(1); i < 1204; i += 2 {
		_, err := bm1.Add(i)
		if err != nil {
			t.Fatalf("adding bits: %v", err)
		}
	}

	if err := f.AddRemoteAvailableShards(bm1); err != nil {
		t.Fatal(err)
	}

	// Reload field and verify that shard data is persisted.
	result := bm.Union(bm1)
	if err := f.Reopen(); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(f.remoteAvailableShards.Slice(), result.Slice()) {
		t.Fatalf("unexpected available shards (reopen). expected: %v, but got: %v", bm.Slice(), f.remoteAvailableShards.Slice())
	}

}

// Ensure that FieldOptions.Base defaults to the correct value.
func TestBSIGroup_BaseDefaultValue(t *testing.T) {
	for i, tt := range []struct {
		min     int64
		max     int64
		expBase int64
	}{
		{100, 200, 100},
		{-100, 100, 0},
		{-200, -100, -100},
	} {
		fn := OptFieldTypeInt(tt.min, tt.max)

		// Apply functional option.
		fo := FieldOptions{}
		err := fn(&fo)
		if err != nil {
			t.Fatalf("test %d, applying functional option: %s", i, err.Error())
		}

		if fo.Base != tt.expBase {
			t.Fatalf("test %d, unexpected FieldOptions.Base value. expected: %d, but got: %d", i, tt.expBase, fo.Base)
		}
	}
}
