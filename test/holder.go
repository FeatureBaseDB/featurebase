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

package test

import (
	"math"
	"testing"
	"time"

	"github.com/molecula/featurebase/v2"
	"github.com/molecula/featurebase/v2/pql"
	"github.com/molecula/featurebase/v2/testhook"
	. "github.com/molecula/featurebase/v2/vprint" // nolint:staticcheck
	"github.com/pkg/errors"
)

// Holder is a test wrapper for pilosa.Holder.
type Holder struct {
	*pilosa.Holder
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
	h := &Holder{Holder: pilosa.NewHolder(path, cfg)}

	return h
}

// MustOpenHolder creates and opens a holder at a temporary path. Panic on error.
func MustOpenHolder(tb testing.TB) *Holder {
	h := NewHolder(tb)
	if err := h.Open(); err != nil {
		panic(err)
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
		panic(err)
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
	var tx pilosa.Tx

	row, err := f.Row(tx, rowID)
	if err != nil {
		panic(err)
	}
	// clone it so that mmapped storage doesn't disappear from under it
	// once the tx goes away.
	return row.Clone()
}

// ReadRow returns a Row for a given field. If the field does not exist,
// it panics rather than creating the field.
func (h *Holder) ReadRow(index, field string, rowID uint64) *pilosa.Row {
	idx := h.Holder.Index(index)
	if idx == nil {
		panic(errors.Wrap(pilosa.ErrIndexNotFound, index))
	}
	f := idx.Field(field)
	if f == nil {
		panic(errors.Wrap(pilosa.ErrFieldNotFound, field))
	}
	var tx pilosa.Tx

	row, err := f.Row(tx, rowID)
	if err != nil {
		panic(err)
	}

	// clone it so that mmapped storage doesn't disappear from under it
	// once the tx goes away.
	return row.Clone()
}

func (h *Holder) RowTime(index, field string, rowID uint64, t time.Time, quantum string) *pilosa.Row {
	idx := h.MustCreateIndexIfNotExists(index, pilosa.IndexOptions{})
	f, err := idx.CreateFieldIfNotExists(field, pilosa.OptFieldTypeDefault())
	if err != nil {
		panic(err)
	}
	var tx pilosa.Tx

	row, err := f.RowTime(tx, rowID, t, quantum)
	if err != nil {
		panic(err)
	}

	// clone it so that mmapped storage doesn't disappear from under it
	// once the tx goes away.
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

	shard := columnID / pilosa.ShardWidth
	tx := idx.Index.Txf().NewTx(pilosa.Txo{Write: true, Index: idx.Index, Shard: shard})
	defer tx.Rollback()

	_, err = f.SetBit(tx, rowID, columnID, t)
	if err != nil {
		panic(err)
	}
	PanicOn(tx.Commit())
}

// ClearBit clears a bit on the given field.
func (h *Holder) ClearBit(index, field string, rowID, columnID uint64) {
	idx := h.MustCreateIndexIfNotExists(index, pilosa.IndexOptions{})
	f, err := idx.CreateFieldIfNotExists(field, pilosa.OptFieldTypeDefault())
	if err != nil {
		panic(err)
	}

	shard := columnID / pilosa.ShardWidth
	tx := idx.Index.Txf().NewTx(pilosa.Txo{Write: true, Index: idx.Index, Shard: shard})
	defer tx.Rollback()

	_, err = f.ClearBit(tx, rowID, columnID)
	if err != nil {
		panic(err)
	}
	PanicOn(tx.Commit())
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
	shard := columnID / pilosa.ShardWidth
	tx := idx.Index.Txf().NewTx(pilosa.Txo{Write: true, Index: idx.Index, Shard: shard})
	defer tx.Rollback()

	_, err = f.SetValue(tx, columnID, value)
	if err != nil {
		panic(err)
	}
	if err := tx.Commit(); err != nil {
		panic(err)
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
	shard := columnID / pilosa.ShardWidth
	tx := idx.Index.Txf().NewTx(pilosa.Txo{Write: false, Index: idx.Index, Shard: shard})
	defer tx.Rollback()

	val, exists, err := f.Value(tx, columnID)
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
	// once the tx goes away.
	return row.Clone()
}
