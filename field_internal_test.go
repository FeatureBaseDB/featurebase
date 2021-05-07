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
	"context"
	"fmt"
	"math"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/pilosa/pilosa/v2/pql"
	"github.com/pilosa/pilosa/v2/roaring"
	"github.com/pilosa/pilosa/v2/testhook"
	. "github.com/pilosa/pilosa/v2/vprint" // nolint:staticcheck
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
			{b0, pql.LT, 1005, 1024, false},
			{b0, pql.LT, 0, 100, false},

			{b1, pql.LT, 5, 5, false},
			{b1, pql.LT, -8, -8, false},
			{b1, pql.LT, 1005, 256, false},
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
			{b1, pql.GT, -300, -256, false},

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
			{b1, 0, -1, 0, 0, true},

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
	f := OpenField(t, OptFieldTypeDefault())
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
	parent *Index
	tb     testing.TB
}

// NewTestField returns a new instance of TestField d/0.
func NewTestField(t *testing.T, opts FieldOption) *TestField {
	path, err := testhook.TempDirInDir(t, *TempDir, "pilosa-field-")
	if err != nil {
		t.Fatal(err)
	}

	cfg := DefaultHolderConfig()
	cfg.StorageConfig.Backend = CurrentBackendOrDefault()
	h := NewHolder(path, cfg)
	PanicOn(h.Open())

	idx, err := h.CreateIndex("i", IndexOptions{})
	if err != nil {
		panic(err)
	}
	field, err := idx.CreateField("f", opts)
	if err != nil {
		t.Fatal(err)
	}
	tf := &TestField{Field: field, parent: idx, tb: t}
	testhook.Cleanup(t, func() {
		h.Close()
	})
	return tf
}

// OpenField returns a new, opened field at a temporary path.
func OpenField(t *testing.T, opts FieldOption) *TestField {
	f := NewTestField(t, opts)
	return f
}

// Close closes the field and removes the underlying data.
func (f *TestField) Close() error {
	if f.idx != nil {
		PanicOn(f.idx.holder.txf.CloseIndex(f.idx))
	}
	defer os.RemoveAll(f.Path())
	return f.Field.Close()
}

// Reopen closes the index and reopens it.
func (f *TestField) Reopen() error {
	name := f.Field.Name()
	if err := f.parent.Close(); err != nil {
		f.parent = nil
		return err
	}
	schema, err := f.parent.Schemator.Schema(context.Background())
	if err != nil {
		return err
	}
	if err := f.parent.OpenWithSchema(schema[f.parent.name]); err != nil {
		f.parent = nil
		return err
	}
	f.Field = f.parent.Field(name)
	return nil
}

func (f *TestField) MustSetBit(tx Tx, row, col uint64, ts ...time.Time) {
	if len(ts) == 0 {
		_, err := f.Field.SetBit(tx, row, col, nil)
		if err != nil {
			panic(err)
		}
	}
	for _, t := range ts {
		_, err := f.Field.SetBit(tx, row, col, &t)
		if err != nil {
			panic(err)
		}
	}
}

// Ensure field can open and retrieve a view.
func TestField_CreateViewIfNotExists(t *testing.T) {
	f := OpenField(t, OptFieldTypeDefault())
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
	f := OpenField(t, OptFieldTypeTime(TimeQuantum("YMDH")))
	defer f.Close()

	// Retrieve time quantum.
	if q := f.TimeQuantum(); q != TimeQuantum("YMDH") {
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
	f := OpenField(t, OptFieldTypeTime(TimeQuantum("YMDH")))
	defer f.Close()

	// Obtain transaction.
	tx := f.idx.holder.txf.NewTx(Txo{Write: writable, Index: f.idx, Field: f.Field, Shard: 0})
	defer tx.Rollback()

	f.MustSetBit(tx, 1, 1, time.Date(2010, time.January, 5, 12, 0, 0, 0, time.UTC))
	f.MustSetBit(tx, 1, 2, time.Date(2011, time.January, 5, 12, 0, 0, 0, time.UTC))
	f.MustSetBit(tx, 1, 3, time.Date(2010, time.February, 5, 12, 0, 0, 0, time.UTC))
	f.MustSetBit(tx, 1, 4, time.Date(2010, time.January, 6, 12, 0, 0, 0, time.UTC))
	f.MustSetBit(tx, 1, 5, time.Date(2010, time.January, 5, 13, 0, 0, 0, time.UTC))

	PanicOn(tx.Commit())

	// obtain 2nd transaction to read it back.
	tx = f.idx.holder.txf.NewTx(Txo{Write: !writable, Index: f.idx, Field: f.Field, Shard: 0})
	defer tx.Rollback()

	if r, err := f.RowTime(tx, 1, time.Date(2010, time.November, 5, 12, 0, 0, 0, time.UTC), "Y"); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(r.Columns(), []uint64{1, 3, 4, 5}) {
		t.Fatalf("wrong columns: %#v", r.Columns())
	}

	if r, err := f.RowTime(tx, 1, time.Date(2010, time.February, 7, 13, 0, 0, 0, time.UTC), "YM"); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(r.Columns(), []uint64{3}) {
		t.Fatalf("wrong columns: %#v", r.Columns())
	}

	if r, err := f.RowTime(tx, 1, time.Date(2010, time.February, 7, 13, 0, 0, 0, time.UTC), "M"); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(r.Columns(), []uint64{3}) {
		t.Fatalf("wrong columns: %#v", r.Columns())
	}

	if r, err := f.RowTime(tx, 1, time.Date(2010, time.January, 5, 12, 0, 0, 0, time.UTC), "MD"); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(r.Columns(), []uint64{1, 5}) {
		t.Fatalf("wrong columns: %#v", r.Columns())
	}

	if r, err := f.RowTime(tx, 1, time.Date(2010, time.January, 5, 13, 0, 0, 0, time.UTC), "MDH"); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(r.Columns(), []uint64{5}) {
		t.Fatalf("wrong columns: %#v", r.Columns())
	}

}

func TestField_PersistAvailableShards(t *testing.T) {
	availableShardFileFlushDuration.Set(200 * time.Millisecond) //shorten the default time to force a file write
	f := OpenField(t, OptFieldTypeDefault())
	defer f.Close()

	// bm represents remote available shards.
	bm := roaring.NewBitmap(1, 2, 3)

	if err := f.AddRemoteAvailableShards(bm); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * availableShardFileFlushDuration.Get())

	// Reload field and verify that shard data is persisted.
	if err := f.Reopen(); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(f.remoteAvailableShards.Slice(), bm.Slice()) {
		t.Fatalf("unexpected available shards (reopen). expected: %v, but got: %v", bm.Slice(), f.remoteAvailableShards.Slice())
	}

}

func TestField_TruncatedAvailableShards(t *testing.T) {
	availableShardFileFlushDuration.Set(200 * time.Millisecond) //shorten the default time to force a file write
	f := OpenField(t, OptFieldTypeDefault())
	defer f.Close()

	// bm represents remote available shards.
	bm := roaring.NewBitmap(1, 2, 3)

	if err := f.AddRemoteAvailableShards(bm); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * availableShardFileFlushDuration.Get())
	f.remoteAvailableShards = roaring.NewBitmap()

	if !reflect.DeepEqual(f.remoteAvailableShards.Slice(), []uint64(nil)) {
		t.Fatalf("unexpected available shards (reopen). expected: %#v, but got: %#v", []uint64{}, f.remoteAvailableShards.Slice())
	}
}

// Ensure that persisting available shards having a smaller footprint (for example,
// when going from a bitmap to a smaller, RLE representation) succeeds.
func TestField_PersistAvailableShardsFootprint(t *testing.T) {
	availableShardFileFlushDuration.Set(200 * time.Millisecond) //shorten the default time to force a file write
	f := OpenField(t, OptFieldTypeDefault())
	defer f.Close()

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
	time.Sleep(2 * availableShardFileFlushDuration.Get())

	if err := f.loadAvailableShards(); err != nil {
		t.Fatal(err)
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

	if err := f.loadAvailableShards(); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(f.remoteAvailableShards.Slice(), result.Slice()) {
		t.Fatalf("unexpected available shards (reload). expected: %v, but got: %v", bm.Slice(), f.remoteAvailableShards.Slice())
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

func TestField_ApplyOptions(t *testing.T) {
	for i, tt := range []struct {
		opts    FieldOptions
		expOpts FieldOptions
	}{
		{
			FieldOptions{
				Type:      FieldTypeSet,
				CacheType: CacheTypeNone,
				CacheSize: 0,
			},
			FieldOptions{
				Type:      FieldTypeSet,
				CacheType: CacheTypeNone,
				CacheSize: 0,
			},
		},
	} {

		fld := &Field{}
		fld.options = applyDefaultOptions(&FieldOptions{})

		if err := fld.applyOptions(tt.opts); err != nil {
			t.Fatal(err)
		}

		if fld.options.CacheType != tt.expOpts.CacheType {
			t.Fatalf("test %d, unexpected FieldOptions.CacheType value. expected: %s, but got: %s", i, tt.expOpts.CacheType, fld.options.CacheType)
		} else if fld.options.CacheSize != tt.expOpts.CacheSize {
			t.Fatalf("test %d, unexpected FieldOptions.CacheSize value. expected: %d, but got: %d", i, tt.expOpts.CacheSize, fld.options.CacheSize)
		}
	}
}

// Ensure that importValue handles requiredDepth correctly.
// This test sets the same column value to 1, then 8, then 1.
// A previous bug was incorrectly determining bitDepth based
// on the values in the import, and not taking existing values
// into consideration. This would cause an import of 1/8/1
// to result in a value of 9 instead of 1.
func TestBSIGroup_importValue(t *testing.T) {
	f := OpenField(t, OptFieldTypeInt(-100, 200))
	defer f.Close()

	qcx := f.idx.holder.txf.NewQcx()
	defer qcx.Abort()

	options := &ImportOptions{}
	for i, tt := range []struct {
		columnIDs []uint64
		values    []int64
		checkVal  int64
		expCols   []uint64
	}{
		{
			[]uint64{100},
			[]int64{1},
			1,
			[]uint64{100},
		},
		{
			[]uint64{100},
			[]int64{8},
			8,
			[]uint64{100},
		},
		{
			[]uint64{100},
			[]int64{1},
			1,
			[]uint64{100},
		},
	} {
		if err := f.importValue(qcx, tt.columnIDs, tt.values, options); err != nil {
			t.Fatalf("test %d, importing values: %s", i, err.Error())
		}
		PanicOn(qcx.Finish())
		qcx.Reset()
		if row, err := f.Range(qcx, f.name, pql.EQ, tt.checkVal); err != nil {
			t.Fatalf("test %d, getting range: %s", i, err.Error())
		} else if !reflect.DeepEqual(row.Columns(), tt.expCols) {
			t.Fatalf("test %d, expected columns: %v, but got: %v", i, tt.expCols, row.Columns())
		}
		PanicOn(qcx.Finish())
		qcx.Reset()
	} // loop
}

func TestIntField_MinMaxForShard(t *testing.T) {
	f := OpenField(t, OptFieldTypeInt(-100, 200))
	defer f.Close()

	qcx := f.idx.holder.txf.NewQcx()
	defer qcx.Abort()

	options := &ImportOptions{}
	for i, test := range []struct {
		name      string
		columnIDs []uint64
		values    []int64
		expMax    ValCount
		expMin    ValCount
	}{
		{
			name:      "zero",
			columnIDs: []uint64{},
			values:    []int64{},
		},
		{
			name:      "single",
			columnIDs: []uint64{1},
			values:    []int64{10},
			expMax:    ValCount{Val: 10, Count: 1},
			expMin:    ValCount{Val: 10, Count: 1},
		},
		{
			name:      "twovals",
			columnIDs: []uint64{1, 2},
			values:    []int64{10, 20},
			expMax:    ValCount{Val: 20, Count: 1},
			expMin:    ValCount{Val: 10, Count: 1},
		},
		{
			name:      "multiplecounts",
			columnIDs: []uint64{1, 2, 3, 4, 5},
			values:    []int64{10, 20, 10, 10, 20},
			expMax:    ValCount{Val: 20, Count: 2},
			expMin:    ValCount{Val: 10, Count: 3},
		},
		{
			name:      "middlevals",
			columnIDs: []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			values:    []int64{10, 20, 10, 10, 20, 11, 12, 11, 13, 11},
			expMax:    ValCount{Val: 20, Count: 2},
			expMin:    ValCount{Val: 10, Count: 3},
		},
		{
			name:      "middlevals",
			columnIDs: []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 100000000, 100000001},
			values:    []int64{10, 20, 10, 10, 20, 11, 12, 11, 13, 11, 44, 1},
			expMax:    ValCount{Val: 20, Count: 2},
			expMin:    ValCount{Val: 10, Count: 3},
		},
	} {
		t.Run(test.name+strconv.Itoa(i), func(t *testing.T) {
			if err := f.importValue(qcx, test.columnIDs, test.values, options); err != nil {
				t.Fatalf("test %d, importing values: %s", i, err.Error())
			}
			PanicOn(qcx.Finish())
			qcx.Reset()

			shard := uint64(0)
			tx := f.idx.holder.txf.NewTx(Txo{Write: !writable, Index: f.idx, Field: f.Field, Shard: shard})
			// Rollback below manually, because we are in a loop.

			maxvc, err := f.MaxForShard(tx, shard, nil)
			if err != nil {
				t.Fatalf("getting max for shard: %v", err)
			}
			if maxvc != test.expMax {
				t.Fatalf("max expected:\n%+v\ngot:\n%+v", test.expMax, maxvc)
			}

			minvc, err := f.MinForShard(tx, shard, nil)
			if err != nil {
				t.Fatalf("getting min for shard: %v", err)
			}
			if minvc != test.expMin {
				t.Fatalf("min expected:\n%+v\ngot:\n%+v", test.expMin, minvc)
			}
			tx.Rollback()
		})
	}
}

// Ensure we get errors when they are expected.
func TestDecimalField_MinMaxBoundaries(t *testing.T) {
	th := newTestHolder(t)

	for i, test := range []struct {
		scale  int64
		min    pql.Decimal
		max    pql.Decimal
		expErr bool
	}{
		{
			scale:  3,
			min:    pql.NewDecimal(math.MinInt64, 0),
			max:    pql.NewDecimal(math.MaxInt64, 0),
			expErr: true,
		},
		{
			scale:  3,
			min:    pql.NewDecimal(math.MinInt64, 3),
			max:    pql.NewDecimal(math.MaxInt64, 3),
			expErr: false,
		},
		{
			scale:  3,
			min:    pql.NewDecimal(44, 0),
			max:    pql.NewDecimal(88, 0),
			expErr: false,
		},
		{
			scale:  3,
			min:    pql.NewDecimal(-44, 0),
			max:    pql.NewDecimal(88, 0),
			expErr: false,
		},
		{
			scale:  19,
			min:    pql.NewDecimal(1, 0),
			max:    pql.NewDecimal(2, 0),
			expErr: true,
		},
		{
			scale:  19,
			min:    pql.NewDecimal(math.MinInt64, 18),
			max:    pql.NewDecimal(math.MaxInt64, 18),
			expErr: true,
		},
		{
			scale:  0,
			min:    pql.NewDecimal(1, 20),
			max:    pql.NewDecimal(2, 20),
			expErr: true,
		},
		{
			scale:  0,
			min:    pql.NewDecimal(1, -1),
			max:    pql.NewDecimal(2, -1),
			expErr: false,
		},
		{
			scale:  0,
			min:    pql.NewDecimal(1, -19),
			max:    pql.NewDecimal(2, -19),
			expErr: true,
		},
	} {
		t.Run("minmax"+strconv.Itoa(i), func(t *testing.T) {
			_, err := NewField(th, "no-path", "i", "f", OptFieldTypeDecimal(test.scale, test.min, test.max))
			if err != nil && test.expErr {
				if !strings.Contains(err.Error(), "is not supported") {
					t.Fatal(err)
				}
			} else if err != nil && !test.expErr {
				t.Fatalf("did not expect error, but got: %s", err)
			} else if err == nil && test.expErr {
				t.Fatal("expected error, but got none")
			}
		})
	}
}

func TestDecimalField_MinMaxForShard(t *testing.T) {
	f := OpenField(t, OptFieldTypeDecimal(3))
	defer f.Close()

	qcx := f.idx.holder.txf.NewQcx()
	defer qcx.Abort()

	options := &ImportOptions{}
	for i, test := range []struct {
		name      string
		columnIDs []uint64
		values    []float64
		expMax    ValCount
		expMin    ValCount
	}{
		{
			name:      "zero",
			columnIDs: []uint64{},
			values:    []float64{},
		},
		{
			name:      "single",
			columnIDs: []uint64{1},
			values:    []float64{10.1},
			expMax:    ValCount{DecimalVal: &pql.Decimal{Value: 10100, Scale: 3}, Count: 1},
			expMin:    ValCount{DecimalVal: &pql.Decimal{Value: 10100, Scale: 3}, Count: 1},
		},
		{
			name:      "twovals",
			columnIDs: []uint64{1, 2},
			values:    []float64{10.1, 20.2},
			expMax:    ValCount{DecimalVal: &pql.Decimal{Value: 20200, Scale: 3}, Count: 1},
			expMin:    ValCount{DecimalVal: &pql.Decimal{Value: 10100, Scale: 3}, Count: 1},
		},
		{
			name:      "multiplecounts",
			columnIDs: []uint64{1, 2, 3, 4, 5},
			values:    []float64{10.1, 20.2, 10.1, 10.1, 20.2},
			expMax:    ValCount{DecimalVal: &pql.Decimal{Value: 20200, Scale: 3}, Count: 2},
			expMin:    ValCount{DecimalVal: &pql.Decimal{Value: 10100, Scale: 3}, Count: 3},
		},
		{
			name:      "middlevals",
			columnIDs: []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			values:    []float64{10.1, 20.2, 10.1, 10.1, 20.2, 11, 12, 11, 13, 11},
			expMax:    ValCount{DecimalVal: &pql.Decimal{Value: 20200, Scale: 3}, Count: 2},
			expMin:    ValCount{DecimalVal: &pql.Decimal{Value: 10100, Scale: 3}, Count: 3},
		},
		{
			name:      "another shard",
			columnIDs: []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 100000000, 100000001},
			values:    []float64{10.1, 20.2, 10.1, 10.1, 20.2, 11, 12, 11, 13, 11, 44.39, 0.23},
			expMax:    ValCount{DecimalVal: &pql.Decimal{Value: 20200, Scale: 3}, Count: 2},
			expMin:    ValCount{DecimalVal: &pql.Decimal{Value: 10100, Scale: 3}, Count: 3},
		},
	} {
		t.Run(test.name+strconv.Itoa(i), func(t *testing.T) {
			if err := f.importFloatValue(qcx, test.columnIDs, test.values, options); err != nil {
				t.Fatalf("test %d, importing values: %s", i, err.Error())
			}

			shard := uint64(0)
			tx := f.idx.holder.txf.NewTx(Txo{Write: !writable, Index: f.idx, Field: f.Field, Shard: shard})
			defer tx.Rollback()

			maxvc, err := f.MaxForShard(tx, shard, nil)
			if err != nil {
				t.Fatalf("getting max for shard: %v", err)
			}
			if !reflect.DeepEqual(maxvc, test.expMax) {
				t.Fatalf("max expected:\n%+v\ngot:\n%+v", test.expMax, maxvc)
			}

			minvc, err := f.MinForShard(tx, shard, nil)
			if err != nil {
				t.Fatalf("getting min for shard: %v", err)
			}
			if !reflect.DeepEqual(minvc, test.expMin) {
				t.Fatalf("min expected:\n%+v\ngot:\n%+v", test.expMin, minvc)
			}
		})
	}
}

func TestBSIGroup_TxReopenDB(t *testing.T) {
	f := OpenField(t, OptFieldTypeInt(-100, 200))
	defer f.Close()

	qcx := f.idx.holder.txf.NewQcx()
	defer qcx.Abort()

	options := &ImportOptions{}
	for i, tt := range []struct {
		columnIDs []uint64
		values    []int64
		checkVal  int64
		expCols   []uint64
	}{
		{
			[]uint64{100},
			[]int64{1},
			1,
			[]uint64{100},
		},
		{
			[]uint64{100},
			[]int64{8},
			8,
			[]uint64{100},
		},
		{
			[]uint64{100},
			[]int64{1},
			1,
			[]uint64{100},
		},
	} {
		if err := f.importValue(qcx, tt.columnIDs, tt.values, options); err != nil {
			t.Fatalf("test %d, importing values: %s", i, err.Error())
		}
		PanicOn(qcx.Finish())
		qcx.Reset()

		if row, err := f.Range(qcx, f.name, pql.EQ, tt.checkVal); err != nil {
			t.Fatalf("test %d, getting range: %s", i, err.Error())
		} else if !reflect.DeepEqual(row.Columns(), tt.expCols) {
			t.Fatalf("test %d, expected columns: %v, but got: %v", i, tt.expCols, row.Columns())
		}
		PanicOn(qcx.Finish())
		qcx.Reset()
	} // loop

	// the test: can we re-open a BSI fragment under Tx store
	_ = f.Reopen()
}

// Ensure that an integer field has the same BitDepth after reopening.
func TestField_SaveMeta(t *testing.T) {
	f := OpenField(t, OptFieldTypeInt(-10, 1000))
	defer f.Close()

	colID := uint64(1)
	val := int64(88)
	expBitDepth := uint64(7)

	// Obtain transaction.
	tx := f.idx.holder.txf.NewTx(Txo{Write: writable, Index: f.idx, Field: f.Field, Shard: 0})
	defer tx.Rollback()

	if changed, err := f.SetValue(tx, colID, val); err != nil {
		t.Fatal(err)
	} else if !changed {
		t.Fatal("expected SetValue to return changed = true")
	} else if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	if f.options.BitDepth != expBitDepth {
		t.Fatalf("expected BitDepth after set to be: %d, got: %d", expBitDepth, f.options.BitDepth)
	}

	tx2 := f.idx.holder.txf.NewTx(Txo{Index: f.idx, Field: f.Field, Shard: 0})
	defer tx2.Rollback()
	if rslt, ok, err := f.Value(tx2, colID); err != nil {
		t.Fatal(err)
	} else if !ok {
		t.Fatal("expected Value() to return exists = true")
	} else if rslt != val {
		t.Fatalf("expected value to be: %d, got: %d", val, rslt)
	}

	// Reload field and verify that it is persisted.
	if err := f.Reopen(); err != nil {
		t.Fatal(err)
	}

	if f.options.BitDepth != expBitDepth {
		t.Fatalf("expected BitDepth after reopen to be: %d, got: %d", expBitDepth, f.options.BitDepth)
	}

	tx3 := f.idx.holder.txf.NewTx(Txo{Index: f.idx, Field: f.Field, Shard: 0})
	defer tx3.Rollback()
	if rslt, ok, err := f.Value(tx3, colID); err != nil {
		t.Fatal(err)
	} else if !ok {
		t.Fatal("expected Value() after reopen to return exists = true")
	} else if rslt != val {
		t.Fatalf("expected value after reopen to be: %d, got: %d", val, rslt)
	}
}
