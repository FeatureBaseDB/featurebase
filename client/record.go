// Copyright 2017 Pilosa Corp.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions
// are met:
//
// 1. Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//
// 2. Redistributions in binary form must reproduce the above copyright
// notice, this list of conditions and the following disclaimer in the
// documentation and/or other materials provided with the distribution.
//
// 3. Neither the name of the copyright holder nor the names of its
// contributors may be used to endorse or promote products derived
// from this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND
// CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
// INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
// MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
// DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR
// CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
// BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
// WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
// NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
// DAMAGE.

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
