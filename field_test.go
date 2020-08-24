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
	"math"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/pilosa/pilosa/v2"
	"github.com/pilosa/pilosa/v2/roaring"
	"github.com/pilosa/pilosa/v2/test"
	"github.com/pilosa/pilosa/v2/testhook"
)

var panicOn = pilosa.PanicOn

// Ensure a field can set & read a bsiGroup value.
func TestField_SetValue(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		idx := test.MustOpenIndex(t)
		defer idx.Close()

		f, err := idx.CreateField("f", pilosa.OptFieldTypeInt(math.MinInt64, math.MaxInt64))
		if err != nil {
			t.Fatal(err)
		}

		idxPilosa := f.Field.GetIndex()
		tx := idxPilosa.NewTx(pilosa.Txo{Write: writable, Index: idxPilosa, Field: f.Field})
		defer tx.Rollback()

		// Set value on field.
		if changed, err := f.SetValue(tx, 100, 21); err != nil {
			t.Fatal(err)
		} else if !changed {
			t.Fatal("expected change")
		}

		// Read value.
		if value, exists, err := f.Value(tx, 100); err != nil {
			t.Fatal(err)
		} else if value != 21 {
			t.Fatalf("unexpected value: %d", value)
		} else if !exists {
			t.Fatal("expected value to exist")
		}

		// Setting value should return no change.
		if changed, err := f.SetValue(tx, 100, 21); err != nil {
			t.Fatal(err)
		} else if changed {
			t.Fatal("expected no change")
		}
	})

	t.Run("Overwrite", func(t *testing.T) {
		idx := test.MustOpenIndex(t)
		defer idx.Close()

		f, err := idx.CreateField("f", pilosa.OptFieldTypeInt(math.MinInt64, math.MaxInt64))
		if err != nil {
			t.Fatal(err)
		}
		idxP := f.Field.GetIndex()
		tx := idxP.Txf.NewTx(pilosa.Txo{Write: writable, Index: idxP, Field: f.Field})
		defer tx.Rollback()

		// Set value.
		if changed, err := f.SetValue(tx, 100, 21); err != nil {
			t.Fatal(err)
		} else if !changed {
			t.Fatal("expected change")
		}

		// Set different value.
		if changed, err := f.SetValue(tx, 100, 23); err != nil {
			t.Fatal(err)
		} else if !changed {
			t.Fatal("expected change")
		}

		// Read value.
		if value, exists, err := f.Value(tx, 100); err != nil {
			t.Fatal(err)
		} else if value != 23 {
			t.Fatalf("unexpected value: %d", value)
		} else if !exists {
			t.Fatal("expected value to exist")
		}
	})

	t.Run("ErrBSIGroupNotFound", func(t *testing.T) {
		idx := test.MustOpenIndex(t)
		defer idx.Close()

		f, err := idx.CreateField("f", pilosa.OptFieldTypeDefault())
		if err != nil {
			t.Fatal(err)
		}
		idxP := f.Field.GetIndex()
		tx := idxP.Txf.NewTx(pilosa.Txo{Write: writable, Index: idxP, Field: f.Field})
		defer tx.Rollback()

		// Set value.
		if _, err := f.SetValue(tx, 100, 21); err != pilosa.ErrBSIGroupNotFound {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrBSIGroupValueTooLow", func(t *testing.T) {
		idx := test.MustOpenIndex(t)
		defer idx.Close()

		f, err := idx.CreateField("f", pilosa.OptFieldTypeInt(20, 30))
		if err != nil {
			t.Fatal(err)
		}
		idxP := f.Field.GetIndex()
		tx := idxP.Txf.NewTx(pilosa.Txo{Write: writable, Index: idxP, Field: f.Field})
		defer tx.Rollback()

		// Set value.
		if _, err := f.SetValue(tx, 100, 15); err != pilosa.ErrBSIGroupValueTooLow {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrBSIGroupValueTooHigh", func(t *testing.T) {
		idx := test.MustOpenIndex(t)
		defer idx.Close()

		f, err := idx.CreateField("f", pilosa.OptFieldTypeInt(20, 30))
		if err != nil {
			t.Fatal(err)
		}
		idxP := f.Field.GetIndex()
		tx := idxP.Txf.NewTx(pilosa.Txo{Write: writable, Index: idxP, Field: f.Field})
		defer tx.Rollback()

		// Set value.
		if _, err := f.SetValue(tx, 100, 31); err != pilosa.ErrBSIGroupValueTooHigh {
			t.Fatalf("unexpected error: %s", err)
		}
	})
}

func TestField_NameRestriction(t *testing.T) {
	path, err := testhook.TempDir(t, "pilosa-field-")
	if err != nil {
		panic(err)
	}
	field, err := pilosa.NewField(pilosa.NewHolder(pilosa.DefaultPartitionN), path, "i", ".meta", pilosa.OptFieldTypeDefault())
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
		"charact2301234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890",
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
		"charact23112345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901",
	}

	path, err := testhook.TempDir(t, "pilosa-field-")
	if err != nil {
		panic(err)
	}
	for _, name := range validFieldNames {
		_, err := pilosa.NewField(pilosa.NewHolder(pilosa.DefaultPartitionN), path, "i", name, pilosa.OptFieldTypeDefault())
		if err != nil {
			t.Fatalf("unexpected field name: %s %s", name, err)
		}
	}
	for _, name := range invalidFieldNames {
		_, err := pilosa.NewField(pilosa.NewHolder(pilosa.DefaultPartitionN), path, "i", name, pilosa.OptFieldTypeDefault())
		if err == nil {
			t.Fatalf("expected error on field name: %s", name)
		}
	}
}

// Ensure can update and delete available shards.
func TestField_AvailableShards(t *testing.T) {
	idx := test.MustOpenIndex(t)
	defer idx.Close()

	f, err := idx.CreateField("f", pilosa.OptFieldTypeDefault())
	if err != nil {
		t.Fatal(err)
	}
	idxP := f.Field.GetIndex()
	tx := idxP.Txf.NewTx(pilosa.Txo{Write: writable, Index: idxP, Field: f.Field})
	defer tx.Rollback()

	// Set values on shards 0 & 2, and verify.
	if _, err := f.SetBit(tx, 0, 100, nil); err != nil {
		t.Fatal(err)
	} else if _, err := f.SetBit(tx, 0, ShardWidth*2, nil); err != nil {
		t.Fatal(err)
	} else if diff := cmp.Diff(f.AvailableShards().Slice(), []uint64{0, 2}); diff != "" {
		t.Fatal(diff)
	}
	panicOn(tx.Commit())

	// Set remote shards and verify.
	if err := f.AddRemoteAvailableShards(roaring.NewBitmap(1, 2, 4)); err != nil {
		t.Fatalf("adding remote shards: %v", err)
	}
	if diff := cmp.Diff(f.AvailableShards().Slice(), []uint64{0, 1, 2, 4}); diff != "" {
		t.Fatal(diff)
	}

	// Delete shards; only local shards should remain.
	for i := uint64(0); i < 5; i++ {
		err := f.RemoveAvailableShard(i)
		if err != nil {
			t.Fatalf("removing shard %d: %v", i, err)
		}
	}
	if diff := cmp.Diff(f.AvailableShards().Slice(), []uint64{0, 2}); diff != "" {
		t.Fatal(diff)
	}
}

func TestField_ClearValue(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		idx := test.MustOpenIndex(t)
		defer idx.Close()

		f, err := idx.CreateField("f", pilosa.OptFieldTypeInt(math.MinInt64, math.MaxInt64))
		if err != nil {
			t.Fatal(err)
		}
		idxP := f.Field.GetIndex()
		tx := idxP.Txf.NewTx(pilosa.Txo{Write: writable, Index: idxP, Field: f.Field})
		defer tx.Rollback()

		// Set value on field.
		if changed, err := f.SetValue(tx, 100, 21); err != nil {
			t.Fatal(err)
		} else if !changed {
			t.Fatal("expected change")
		}
		panicOn(tx.Commit())

		tx = idxP.Txf.NewTx(pilosa.Txo{Write: !writable, Index: idxP, Field: f.Field})

		// Read value.
		if value, exists, err := f.Value(tx, 100); err != nil {
			t.Fatal(err)
		} else if value != 21 {
			t.Fatalf("unexpected value: %d", value)
		} else if !exists {
			t.Fatal("expected value to exist")
		}
		tx.Rollback()
		tx = idxP.Txf.NewTx(pilosa.Txo{Write: writable, Index: idxP, Field: f.Field})

		if changed, err := f.ClearValue(tx, 100); err != nil {
			t.Fatal(err)
		} else if !changed {
			t.Fatal(err)
		}
		panicOn(tx.Commit())
		tx = idxP.Txf.NewTx(pilosa.Txo{Write: !writable, Index: idxP, Field: f.Field})
		defer tx.Rollback()

		// Read value.
		if _, exists, err := f.Value(tx, 100); err != nil {
			t.Fatal(err)
		} else if exists {
			t.Fatal("expected value to not exist")
		}
	})
}
