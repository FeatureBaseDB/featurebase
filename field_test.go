// Copyright 2021 Molecula Corp. All rights reserved.
package pilosa_test

import (
	"bytes"
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	pilosa "github.com/molecula/featurebase/v3"
	"github.com/molecula/featurebase/v3/pql"
	"github.com/molecula/featurebase/v3/roaring"
	"github.com/molecula/featurebase/v3/test"
	"github.com/pkg/errors"
)

// Ensure a field can set & read a bsiGroup value.
func TestField_SetValue(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		h, idx := test.MustOpenIndex(t)

		f, err := idx.CreateField("f", pilosa.OptFieldTypeInt(math.MinInt64, math.MaxInt64))
		if err != nil {
			t.Fatal(err)
		}

		qcx := h.Txf().NewWritableQcx()
		defer qcx.Abort()
		// You're going to note the lack of any commits here. That's
		// because, when you have a writable Qcx, *every individual
		// sub-transaction commits immediately*. In theory, we ought
		// to be doing provisional writes and the entire set of writes
		// ought to be able to be reverted. Actually no. We're just committing
		// everything as we go anyway.

		// Set value on field.
		if changed, err := f.SetValue(qcx, 100, 21); err != nil {
			t.Fatal(err)
		} else if !changed {
			t.Fatal("expected change")
		}

		// Read value.
		if value, exists, err := f.Value(qcx, 100); err != nil {
			t.Fatal(err)
		} else if value != 21 {
			t.Fatalf("unexpected value: %d", value)
		} else if !exists {
			t.Fatal("expected value to exist")
		}

		// Setting value should return no change.
		if changed, err := f.SetValue(qcx, 100, 21); err != nil {
			t.Fatal(err)
		} else if changed {
			t.Fatal("expected no change")
		}
	})

	t.Run("Overwrite", func(t *testing.T) {
		h, idx := test.MustOpenIndex(t)

		f, err := idx.CreateField("f", pilosa.OptFieldTypeInt(math.MinInt64, math.MaxInt64))
		if err != nil {
			t.Fatal(err)
		}

		qcx := h.Txf().NewWritableQcx()
		defer qcx.Abort()

		// Set value.
		if changed, err := f.SetValue(qcx, 100, 21); err != nil {
			t.Fatal(err)
		} else if !changed {
			t.Fatal("expected change")
		}

		// Set different value.
		if changed, err := f.SetValue(qcx, 100, 23); err != nil {
			t.Fatal(err)
		} else if !changed {
			t.Fatal("expected change")
		}

		// Read value.
		if value, exists, err := f.Value(qcx, 100); err != nil {
			t.Fatal(err)
		} else if value != 23 {
			t.Fatalf("unexpected value: %d", value)
		} else if !exists {
			t.Fatal("expected value to exist")
		}
	})

	t.Run("ErrBSIGroupNotFound", func(t *testing.T) {
		h, idx := test.MustOpenIndex(t)

		f, err := idx.CreateField("f")
		if err != nil {
			t.Fatal(err)
		}

		qcx := h.Txf().NewWritableQcx()
		defer qcx.Abort()

		// Set value.
		if _, err := f.SetValue(qcx, 100, 21); err != pilosa.ErrBSIGroupNotFound {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrBSIGroupValueTooLow", func(t *testing.T) {
		h, idx := test.MustOpenIndex(t)

		f, err := idx.CreateField("f", pilosa.OptFieldTypeInt(20, 30))
		if err != nil {
			t.Fatal(err)
		}
		qcx := h.Txf().NewWritableQcx()
		defer qcx.Abort()
		// Set value.
		if _, err := f.SetValue(qcx, 100, 15); !errors.Is(err, pilosa.ErrBSIGroupValueTooLow) {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrBSIGroupValueTooHigh", func(t *testing.T) {
		h, idx := test.MustOpenIndex(t)

		f, err := idx.CreateField("f", pilosa.OptFieldTypeInt(20, 30))
		if err != nil {
			t.Fatal(err)
		}

		qcx := h.Txf().NewWritableQcx()
		defer qcx.Abort()

		// Set value.
		if _, err := f.SetValue(qcx, 100, 31); !errors.Is(err, pilosa.ErrBSIGroupValueTooHigh) {
			t.Fatalf("unexpected error: %s", err)
		}
	})
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
		".meta",
		"charact23112345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901",
	}

	_, idx := test.MustOpenIndex(t)
	for _, name := range validFieldNames {
		_, err := idx.CreateField(name)
		if err != nil {
			t.Fatalf("unexpected field name: %s %s", name, err)
		}
	}
	for _, name := range invalidFieldNames {
		_, err := idx.CreateField(name)
		if err == nil {
			t.Fatalf("expected error on field name: %s", name)
		}
	}
}

const includeRemote = false // for calls to Index.AvailableShards(localOnly bool)

// Ensure can update and delete available shards.
func TestField_AvailableShards(t *testing.T) {
	h, idx := test.MustOpenIndex(t)

	f, err := idx.CreateField("fld-shards")
	if err != nil {
		t.Fatal(err)
	}

	qcx := h.Txf().NewWritableQcx()
	defer qcx.Abort()

	// Set values on shards 0 & 2, and verify.
	if _, err := f.SetBit(qcx, 0, 100, nil); err != nil {
		t.Fatal(err)
	} else if _, err := f.SetBit(qcx, 0, ShardWidth*2, nil); err != nil {
		t.Fatal(err)
	} else if diff := cmp.Diff(f.AvailableShards(includeRemote).Slice(), []uint64{0, 2}); diff != "" {
		t.Fatal(diff)
	}

	// Set remote shards and verify.
	if err := f.AddRemoteAvailableShards(roaring.NewBitmap(1, 2, 4)); err != nil {
		t.Fatalf("adding remote shards: %v", err)
	}
	if diff := cmp.Diff(f.AvailableShards(includeRemote).Slice(), []uint64{0, 1, 2, 4}); diff != "" {
		t.Fatal(diff)
	}

	// Delete shards; only local shards should remain.
	for i := uint64(0); i < 5; i++ {
		err := f.RemoveAvailableShard(i)
		if err != nil {
			t.Fatalf("removing shard %d: %v", i, err)
		}
	}
	if diff := cmp.Diff(f.AvailableShards(includeRemote).Slice(), []uint64{0, 2}); diff != "" {
		t.Fatal(diff)
	}
}

func TestField_ClearValue(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		h, idx := test.MustOpenIndex(t)

		f, err := idx.CreateField("f", pilosa.OptFieldTypeInt(math.MinInt64, math.MaxInt64))
		if err != nil {
			t.Fatal(err)
		}
		qcx := h.Txf().NewWritableQcx()
		defer qcx.Abort()

		// Set value on field.
		if changed, err := f.SetValue(qcx, 100, 21); err != nil {
			t.Fatal(err)
		} else if !changed {
			t.Fatal("expected change")
		}

		// Read value.
		if value, exists, err := f.Value(qcx, 100); err != nil {
			t.Fatal(err)
		} else if value != 21 {
			t.Fatalf("unexpected value: %d", value)
		} else if !exists {
			t.Fatal("expected value to exist")
		}

		if changed, err := f.ClearValue(qcx, 100); err != nil {
			t.Fatal(err)
		} else if !changed {
			t.Fatal(err)
		}

		// Read value.
		if _, exists, err := f.Value(qcx, 100); err != nil {
			t.Fatal(err)
		} else if exists {
			t.Fatal("expected value to not exist")
		}
	})
}

func TestFieldInfoMarshal(t *testing.T) {
	f := &pilosa.FieldInfo{Name: "timestamp", CreatedAt: 1649270079233541000,
		Options: pilosa.FieldOptions{
			Base:           0,
			BitDepth:       0x0,
			Min:            pql.NewDecimal(-4294967296, 0),
			Max:            pql.NewDecimal(4294967296, 0),
			Scale:          0,
			Keys:           false,
			NoStandardView: false,
			CacheType:      "",
			Type:           "timestamp",
			TimeUnit:       "s",
			TimeQuantum:    "",
			ForeignIndex:   "",
			TTL:            0,
		},
		Cardinality: (*uint64)(nil),
	}
	a, err := json.Marshal(f)
	if err != nil {
		t.Fatalf("unexpected error marshalling index info,  %v", err)
	}
	expected := []byte(`{"name":"timestamp","createdAt":1649270079233541000,"options":{"type":"timestamp","epoch":"1970-01-01T00:00:00Z","bitDepth":0,"min":-4294967296,"max":4294967296,"timeUnit":"s"}}`)
	if bytes.Compare(a, expected) != 0 {
		t.Fatalf("expected %s, got %s", expected, a)
	}
}

func TestCheckUnixNanoOverflow(t *testing.T) {
	minNano = pilosa.MinTimestampNano.UnixNano()
	maxNano = pilosa.MaxTimestampNano.UnixNano()
	tests := []struct {
		name    string
		epoch   time.Time
		wantErr bool
	}{
		{
			name:    "too small",
			epoch:   time.Unix(-1, minNano),
			wantErr: true,
		},
		{
			name:    "just right-1",
			epoch:   time.Unix(0, minNano),
			wantErr: false,
		},
		{
			name:    "just right-2",
			epoch:   time.Unix(0, maxNano),
			wantErr: false,
		},
		{
			name:    "too large",
			epoch:   time.Unix(1, maxNano),
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := pilosa.CheckEpochOutOfRange(tt.epoch, pilosa.MinTimestampNano, pilosa.MaxTimestampNano); (err != nil) != tt.wantErr {
				t.Errorf("checkUnixNanoOverflow() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
