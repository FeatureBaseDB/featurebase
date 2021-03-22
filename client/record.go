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

// package ctl contains all pilosa subcommands other than 'server'. These are
// generally administration, testing, and debugging tools.

package client

// Record is a Column or a FieldValue.
type Record interface {
	Shard(shardWidth uint64) uint64
	Less(other Record) bool
}

// RecordIterator is an iterator for a record.
type RecordIterator interface {
	NextRecord() (Record, error)
}

// Column defines a single Pilosa column.
type Column struct {
	RowID     uint64
	ColumnID  uint64
	RowKey    string
	ColumnKey string
	Timestamp int64
}

// Shard returns the shard for this column.
func (b Column) Shard(shardWidth uint64) uint64 {
	return b.ColumnID / shardWidth
}

// Less returns true if this column sorts before the given Record.
func (b Column) Less(other Record) bool {
	if ob, ok := other.(Column); ok {
		if b.RowID == ob.RowID {
			return b.ColumnID < ob.ColumnID
		}
		return b.RowID < ob.RowID
	}
	return false
}

// FieldValue represents the value for a column within a
// range-encoded field.
type FieldValue struct {
	ColumnID  uint64
	ColumnKey string
	Value     int64
}

// Shard returns the shard for this field value.
func (v FieldValue) Shard(shardWidth uint64) uint64 {
	return v.ColumnID / shardWidth
}

// Less returns true if this field value sorts before the given Record.
func (v FieldValue) Less(other Record) bool {
	if ov, ok := other.(FieldValue); ok {
		return v.ColumnID < ov.ColumnID
	}
	return false
}
