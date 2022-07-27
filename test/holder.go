// Copyright 2021 Molecula Corp. All rights reserved.
package test

import (
	"math"
	"testing"
	"time"

	pilosa "github.com/molecula/featurebase/v3"
	"github.com/molecula/featurebase/v3/pql"
	"github.com/molecula/featurebase/v3/testhook"
)

// Holder is a test wrapper for pilosa.Holder.
type Holder struct {
	*pilosa.Holder
	tb testing.TB
}

// NewHolder returns a new instance of Holder with a temporary path.
func NewHolder(tb testing.TB) *Holder {
	path, err := testhook.TempDir(tb, "pilosa-holder-")
	if err != nil {
		panic(err)
	}

	cfg := pilosa.DefaultHolderConfig()
	cfg.StorageConfig.FsyncEnabled = false
	cfg.RBFConfig.FsyncEnabled = false
	h := &Holder{Holder: pilosa.NewHolder(path, cfg), tb: tb}

	return h
}

// MustOpenHolder creates and opens a holder at a temporary path. Panic on error.
func MustOpenHolder(tb testing.TB) *Holder {
	h := NewHolder(tb)
	if err := h.Open(); err != nil {
		tb.Fatalf("opening holder: %v", err)
	}
	return h
}

// Close closes the holder. The data should be removed by the
func (h *Holder) Close() error {
	return h.Holder.Close()
}

// Reopen instantiates and opens a new holder.
// Note that the holder must be Closed first.
func (h *Holder) Reopen() error {
	return h.Holder.Open()
}

// MustCreateIndexIfNotExists returns a given index. Panic on error.
func (h *Holder) MustCreateIndexIfNotExists(index string, opt pilosa.IndexOptions) *Index {
	idx, err := h.Holder.CreateIndexIfNotExists(index, opt)
	if err != nil {
		h.tb.Fatalf("creating index: %v", err)
	}
	return &Index{Index: idx}
}

// Row returns a Row for a given field.
func (h *Holder) Row(index, field string, rowID uint64) *pilosa.Row {
	idx := h.MustCreateIndexIfNotExists(index, pilosa.IndexOptions{})
	f, err := idx.CreateFieldIfNotExists(field, pilosa.OptFieldTypeDefault())
	if err != nil {
		panic(err)
	}
	qcx := idx.Txf().NewQcx()
	defer qcx.Abort()

	row, err := f.Row(qcx, rowID)
	if err != nil {
		h.tb.Fatalf("retrieving row: %v", err)
	}
	// clone it so that mmapped storage doesn't disappear from under it
	// once the qcx goes away.
	return row
}

// ReadRow returns a Row for a given field. If the field does not exist,
// it fails the holder's test rather than creating the field.
func (h *Holder) ReadRow(index, field string, rowID uint64) *pilosa.Row {
	idx := h.Holder.Index(index)
	if idx == nil {
		h.tb.Fatalf("read row from index %q: index not found", index)
	}
	f := idx.Field(field)
	if f == nil {
		h.tb.Fatalf("read row from field %q/%q: field not found", index, field)
	}
	qcx := idx.Txf().NewQcx()
	defer qcx.Abort()

	row, err := f.Row(qcx, rowID)
	if err != nil {
		h.tb.Fatalf("retrieving row: %v", err)
	}

	// clone it so that mmapped storage doesn't disappear from under it
	// once the qcx goes away.
	return row.Clone()
}

func (h *Holder) RowTime(index, field string, rowID uint64, t time.Time, quantum string) *pilosa.Row {
	idx := h.MustCreateIndexIfNotExists(index, pilosa.IndexOptions{})
	f, err := idx.CreateFieldIfNotExists(field, pilosa.OptFieldTypeDefault())
	if err != nil {
		panic(err)
	}
	qcx := idx.Txf().NewQcx()
	defer qcx.Abort()

	row, err := f.RowTime(qcx, rowID, t, quantum)
	if err != nil {
		panic(err)
	}

	// clone it so that mmapped storage doesn't disappear from under it
	// once the qcx goes away.
	return row.Clone()
}

// SetBit sets a bit on the given field.
func (h *Holder) SetBit(index, field string, rowID, columnID uint64) {
	h.SetBitTime(index, field, rowID, columnID, nil)
}

// SetBitTime sets a bit with timestamp on the given field.
func (h *Holder) SetBitTime(index, field string, rowID, columnID uint64, t *time.Time) {
	idx := h.MustCreateIndexIfNotExists(index, pilosa.IndexOptions{})
	f, err := idx.CreateFieldIfNotExists(field, pilosa.OptFieldTypeDefault())
	if err != nil {
		panic(err)
	}

	qcx := idx.Txf().NewWritableQcx()
	defer qcx.Abort()

	_, err = f.SetBit(qcx, rowID, columnID, t)
	if err != nil {
		h.tb.Fatalf("setting bit: %v", err)
	}
	err = qcx.Finish()
	if err != nil {
		h.tb.Fatalf("finishing qcx: %v", err)
	}
}

// ClearBit clears a bit on the given field.
func (h *Holder) ClearBit(index, field string, rowID, columnID uint64) {
	idx := h.MustCreateIndexIfNotExists(index, pilosa.IndexOptions{})
	f, err := idx.CreateFieldIfNotExists(field, pilosa.OptFieldTypeDefault())
	if err != nil {
		panic(err)
	}

	qcx := idx.Txf().NewWritableQcx()
	defer qcx.Abort()

	_, err = f.ClearBit(qcx, rowID, columnID)
	if err != nil {
		h.tb.Fatalf("clearing bit: %v", err)
	}
	err = qcx.Finish()
	if err != nil {
		h.tb.Fatalf("finishing qcx: %v", err)
	}
}

// MustSetBits sets columns on a row. Panic on error.
// This function does not accept a timestamp or quantum.
func (h *Holder) MustSetBits(index, field string, rowID uint64, columnIDs ...uint64) {
	for _, columnID := range columnIDs {
		h.SetBit(index, field, rowID, columnID)
	}
}

// SetValue sets an integer value on the given field.
func (h *Holder) SetValue(index, field string, columnID uint64, value int64) *Index {
	idx := h.MustCreateIndexIfNotExists(index, pilosa.IndexOptions{})
	f, err := idx.CreateFieldIfNotExists(field, pilosa.OptFieldTypeInt(math.MinInt64, math.MaxInt64))
	if err != nil {
		panic(err)
	}

	qcx := idx.Txf().NewWritableQcx()
	defer qcx.Abort()
	_, err = f.SetValue(qcx, columnID, value)
	if err != nil {
		h.tb.Fatalf("setting value: %v", err)
	}

	err = qcx.Finish()
	if err != nil {
		h.tb.Fatalf("finishing qcx: %v", err)
	}
	return idx
}

// Value returns the integer value for a given column.
func (h *Holder) Value(index, field string, columnID uint64) (int64, bool) {
	idx := h.MustCreateIndexIfNotExists(index, pilosa.IndexOptions{})
	f, err := idx.CreateFieldIfNotExists(field, pilosa.OptFieldTypeInt(math.MinInt64, math.MaxInt64))
	if err != nil {
		panic(err)
	}

	qcx := idx.Txf().NewQcx()
	defer qcx.Abort()

	val, exists, err := f.Value(qcx, columnID)
	if err != nil {
		panic(err)
	}
	return val, exists
}

// Range returns a Row (of column IDs) for a field based
// on the given range.
func (h *Holder) Range(index, field string, op pql.Token, predicate int64) *pilosa.Row {
	idx := h.MustCreateIndexIfNotExists(index, pilosa.IndexOptions{})
	f, err := idx.CreateFieldIfNotExists(field, pilosa.OptFieldTypeInt(math.MinInt64, math.MaxInt64))
	if err != nil {
		panic(err)
	}

	qcx := h.Txf().NewQcx()
	defer qcx.Abort()

	row, err := f.Range(qcx, field, op, predicate)
	if err != nil {
		panic(err)
	}

	// clone it so that mmapped storage doesn't disappear from under it
	// once the qcx goes away.
	return row.Clone()
}
