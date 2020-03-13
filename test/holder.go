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
	"io/ioutil"
	"math"
	"os"
	"time"

	"github.com/pilosa/pilosa/v2"
	"github.com/pilosa/pilosa/v2/boltdb"
	"github.com/pilosa/pilosa/v2/pql"
)

// Holder is a test wrapper for pilosa.Holder.
type Holder struct {
	*pilosa.Holder
}

// NewHolder returns a new instance of Holder with a temporary path.
func NewHolder() *Holder {
	path, err := ioutil.TempDir("", "pilosa-")
	if err != nil {
		panic(err)
	}

	h := &Holder{Holder: pilosa.NewHolder(pilosa.DefaultPartitionN)}
	h.Path = path
	h.Holder.NewAttrStore = boltdb.NewAttrStore

	return h
}

// MustOpenHolder creates and opens a holder at a temporary path. Panic on error.
func MustOpenHolder() *Holder {
	h := NewHolder()
	if err := h.Open(); err != nil {
		panic(err)
	}
	return h
}

// Close closes the holder and removes all underlying data.
func (h *Holder) Close() error {
	defer os.RemoveAll(h.Path)
	return h.Holder.Close()
}

// Reopen instantiates and opens a new holder.
// Note that the holder must be Closed first.
func (h *Holder) Reopen() error {
	path, logger := h.Path, h.Holder.Logger
	h.Holder = pilosa.NewHolder(pilosa.DefaultPartitionN)
	h.Holder.Path = path
	h.Holder.Logger = logger
	h.Holder.NewAttrStore = boltdb.NewAttrStore
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
	row, err := f.Row(rowID)
	if err != nil {
		panic(err)
	}
	return row
}

// ReadRow returns a Row for a given field. If the field does not exist,
// it panics rather than creating the field.
func (h *Holder) ReadRow(index, field string, rowID uint64) *pilosa.Row {
	f := h.Holder.Field(index, field)
	if f == nil {
		panic(pilosa.ErrFieldNotFound)
	}
	row, err := f.Row(rowID)
	if err != nil {
		panic(err)
	}
	return row
}

func (h *Holder) RowAttrStore(index, field string) pilosa.AttrStore {
	idx := h.MustCreateIndexIfNotExists(index, pilosa.IndexOptions{})
	f, err := idx.CreateFieldIfNotExists(field, pilosa.OptFieldTypeDefault())
	if err != nil {
		panic(err)
	}
	return f.RowAttrStore()
}

func (h *Holder) RowTime(index, field string, rowID uint64, t time.Time, quantum string) *pilosa.Row {
	idx := h.MustCreateIndexIfNotExists(index, pilosa.IndexOptions{})
	f, err := idx.CreateFieldIfNotExists(field, pilosa.OptFieldTypeDefault())
	if err != nil {
		panic(err)
	}
	row, err := f.RowTime(rowID, t, quantum)
	if err != nil {
		panic(err)
	}
	return row
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
	_, err = f.SetBit(rowID, columnID, t)
	if err != nil {
		panic(err)
	}
}

// ClearBit clears a bit on the given field.
func (h *Holder) ClearBit(index, field string, rowID, columnID uint64) {
	idx := h.MustCreateIndexIfNotExists(index, pilosa.IndexOptions{})
	f, err := idx.CreateFieldIfNotExists(field, pilosa.OptFieldTypeDefault())
	if err != nil {
		panic(err)
	}
	_, err = f.ClearBit(rowID, columnID)
	if err != nil {
		panic(err)
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
func (h *Holder) SetValue(index, field string, columnID uint64, value int64) {
	idx := h.MustCreateIndexIfNotExists(index, pilosa.IndexOptions{})
	f, err := idx.CreateFieldIfNotExists(field, pilosa.OptFieldTypeInt(math.MinInt64, math.MaxInt64))
	if err != nil {
		panic(err)
	}
	_, err = f.SetValue(columnID, value)
	if err != nil {
		panic(err)
	}
}

// Value returns the integer value for a given column.
func (h *Holder) Value(index, field string, columnID uint64) (int64, bool) {
	idx := h.MustCreateIndexIfNotExists(index, pilosa.IndexOptions{})
	f, err := idx.CreateFieldIfNotExists(field, pilosa.OptFieldTypeInt(math.MinInt64, math.MaxInt64))
	if err != nil {
		panic(err)
	}
	val, exists, err := f.Value(columnID)
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
	row, err := f.Range(field, op, predicate)
	if err != nil {
		panic(err)
	}
	return row
}
