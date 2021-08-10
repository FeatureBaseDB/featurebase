// Copyright 2021 Molecula Corp.
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

package ingest

import (
	"fmt"
	"sort"

	"github.com/molecula/featurebase/v2/shardwidth"
)

type OpType uint8

const (
	OpNone = OpType(iota)
	OpSet
	OpRemove
	OpClear
	OpWrite
	OpDelete
)

var opNames = []string{
	"none",
	"set",
	"remove",
	"clear",
	"write",
	"delete",
}

func (o OpType) String() string {
	if int(o) < len(opNames) {
		return opNames[o]
	}
	return fmt.Sprintf("invalid-optype-%d", o)
}

func ParseOpType(s string) (OpType, error) {
	for i, v := range opNames[1:] {
		if s == v {
			return OpType(i + 1), nil
		}
	}
	return 0, fmt.Errorf("unknown operation type %q", s)
}

// Operation represents a single set of changes to make to
// the stored data, which means some combination of clearing
// columns, clearing individual bits, or setting bits or values.
// The same data structure can be used whether this represents the
// whole database operation or a single shard's values.
//
// Operations can specify individual per-field operations, which
// have maps of record IDs to values. They can also have a set of
// record IDs and fields to clear. A Clear operation will have only
// record IDs and fields, a Set or Remove will have only FieldOps,
// and a Write will have both -- populating the record IDs and fields
// from the fieldops.
type Operation struct {
	OpType         OpType
	ClearRecordIDs []uint64
	ClearFields    []string
	FieldOps       map[string]*FieldOperation
}

// FieldOperation is the specific set of changes to make to a given
// field.
//
// For a Clear operation, values can be an empty array. For Set or Remove
// operations on sets, RecordIDs can contain duplicates. Times should
// be empty except for time-quantum fields.
type FieldOperation struct {
	RecordIDs []uint64
	Values    []uint64
	// For int/timestamp/decimal, this is the value
	// For time-quantum, this is the timestamp
	// No field has both signed values and timestamps.
	// This is not a place of honor.
	Signed []int64
}

var _ sort.Interface = &FieldOperation{}

// We implement sort.Interface here so we don't have to write sort code.
// This should probably be replaced by smarter sorting later if it's a
// performance issue.
func (f *FieldOperation) Len() int {
	return len(f.RecordIDs)
}

func (f *FieldOperation) Less(i, j int) bool {
	return f.RecordIDs[i] < f.RecordIDs[j]
}

func (f *FieldOperation) Swap(i, j int) {
	f.RecordIDs[i], f.RecordIDs[j] = f.RecordIDs[j], f.RecordIDs[i]
	if f.Values != nil {
		f.Values[i], f.Values[j] = f.Values[j], f.Values[i]
	}
	if f.Signed != nil {
		f.Signed[i], f.Signed[j] = f.Signed[j], f.Signed[i]
	}
}

// Sort sorts the values by record ID. It is not a stable sort.
func (f *FieldOperation) Sort() {
	sort.Sort(f)
}

// Similarly, do this for Operation, which is used only in the Clear case.
var _ sort.Interface = &Operation{}

// We implement sort.Interface here so we don't have to write sort code.
// This should probably be replaced by smarter sorting later if it's a
// performance issue.
func (o *Operation) Len() int {
	return len(o.ClearRecordIDs)
}

func (o *Operation) Less(i, j int) bool {
	return o.ClearRecordIDs[i] < o.ClearRecordIDs[j]
}

func (o *Operation) Swap(i, j int) {
	o.ClearRecordIDs[i], o.ClearRecordIDs[j] = o.ClearRecordIDs[j], o.ClearRecordIDs[i]
}

// Sort sorts the values by record ID. It is not a stable sort.
func (o *Operation) Sort() {
	sort.Sort(o)
}

type ShardedFieldOperation map[uint64]*FieldOperation

// Shard() divides the FieldOperation's values up into corresponding chunks
// based on the shards of record IDs. Record IDs should be sorted before this
// happens.
func (f *FieldOperation) Shard() ShardedFieldOperation {
	if len(f.RecordIDs) == 0 {
		return nil
	}
	shards, ends := shardwidth.FindShards(f.RecordIDs)
	prev := 0
	op := make(ShardedFieldOperation, len(shards))
	for i, shard := range shards {
		endIndex := ends[i]
		subOp := &FieldOperation{RecordIDs: f.RecordIDs[prev:endIndex]}
		if len(f.Values) > 0 {
			subOp.Values = f.Values[prev:endIndex]
		}
		if len(f.Signed) > 0 {
			subOp.Signed = f.Signed[prev:endIndex]
		}
		op[shard] = subOp
		prev = endIndex
	}
	return op
}

// AddPair adds a record ID/value pair where the value is unsigned, as
// when used with set/mutex/time quantum fields.
func (f *FieldOperation) AddPair(rec uint64, value uint64) {
	f.RecordIDs = append(f.RecordIDs, rec)
	f.Values = append(f.Values, value)
}

// AddSignedPair adds a record ID/value pair where the value is signed,
// as when used with int/decimal/timestamp fields.
func (f *FieldOperation) AddSignedPair(rec uint64, value int64) {
	f.RecordIDs = append(f.RecordIDs, rec)
	f.Signed = append(f.Signed, value)
}

// AddStampedPair adds a record/value pair plus a time, which is just
// a Unix time in seconds. (Note, no scaling here; timestamp fields are
// scaled int fields, this is for time quantums.)
func (f *FieldOperation) AddStampedPair(rec uint64, value uint64, stamp int64) {
	f.RecordIDs = append(f.RecordIDs, rec)
	f.Values = append(f.Values, value)
	f.Signed = append(f.Signed, stamp)
}

func translateUnsignedSlice(target []uint64, mapping []uint64) (err error) {
	oops := 0
	for i, v := range target {
		if v >= uint64(len(mapping)) {
			oops++
		} else {
			target[i] = mapping[v]
		}
	}
	if oops > 0 {
		return fmt.Errorf("encountered %d out-of-range keys when applying translation mapping", oops)
	}
	return nil
}

// TranslateUnsigned translates keys according to the provided mapping. This
// is used for sets, mutexes, and time quantums.
func (op *FieldOperation) TranslateKeys(mapping []uint64) error {
	return translateUnsignedSlice(op.RecordIDs, mapping)
}

// TranslateUnsigned translates values according to the provided mapping. This
// is used for sets, mutexes, and time quantums.
func (op *FieldOperation) TranslateUnsigned(mapping []uint64) error {
	return translateUnsignedSlice(op.Values, mapping)
}

// TranslateSigned translates signed values according to the provided mapping.
// If we're using this, it's because we're in an integer-type field, which
// admits using keys for fields, so all key values are actually non-negative,
// but the field's type still requires values be expressed as signed ints.
func (op *FieldOperation) TranslateSigned(mapping []uint64) error {
	oops := 0
	for i, v := range op.Signed {
		if v >= int64(len(mapping)) {
			oops++
		} else {
			op.Signed[i] = int64(mapping[v])
		}
	}
	if oops > 0 {
		return fmt.Errorf("encountered %d out-of-range signed values when applying translation mapping", oops)
	}
	return nil
}

// ShardOperations is a set of Operations associated with a specific shard.
type ShardOperations struct {
	Shard uint64
	Ops   []*Operation
}

// Request is a complete ingest request, which may be any combination
// of operations, which may apply to multiple shards.
type Request struct {
	Ops []*Operation
}

// ShardedRequest is an ingest request, split up into individual per-shard
// operations.
type ShardedRequest struct {
	Ops map[uint64][]*Operation
}

// Shard converts a request into the same request, only sharded.
func (r *Request) Shard() (*ShardedRequest, error) {
	if len(r.Ops) == 0 {
		return &ShardedRequest{Ops: nil}, nil
	}
	req := make(map[uint64][]*Operation)
	shards := make(map[uint64]*Operation)
	// we're getting per-field things, which we want to divide per-shard,
	// and return to per-shard sets of per-field things, so we're inverting
	// the structure.
	for _, op := range r.Ops {
		// for clear and write ops, we also need to split up the
		// ClearRecords values, which may be distinct from the set of
		// records for any given field. For Write ops, we'll then end
		// up adding in field values for some fields.
		if op.OpType == OpClear || op.OpType == OpWrite || op.OpType == OpDelete {
			sharded := ShardIDs(op.ClearRecordIDs)
			for shard, data := range sharded {
				shards[shard] = &Operation{OpType: op.OpType, ClearRecordIDs: data, ClearFields: op.ClearFields, FieldOps: map[string]*FieldOperation{}}
			}
		}
		for field, fieldOp := range op.FieldOps {
			sharded := fieldOp.Shard()
			for shard, data := range sharded {
				shardOp, ok := shards[shard]
				if !ok {
					if op.OpType == OpWrite {
						return nil, fmt.Errorf("write operation has field operation data (%d items) for shard %d, but no clear data", len(data.RecordIDs), shard)
					}
					shardOp = &Operation{OpType: op.OpType}
					shards[shard] = shardOp
					shardOp.FieldOps = map[string]*FieldOperation{field: data}
				} else {
					shardOp.FieldOps[field] = data
				}
			}
		}
		for shard, shardOp := range shards {
			req[shard] = append(req[shard], shardOp)
		}
		for k := range shards {
			delete(shards, k)
		}

	}
	return &ShardedRequest{Ops: req}, nil
}

func (r *Request) Dump(logf func(string, ...interface{})) {
	logf("req: %#v", r)
	for _, op := range r.Ops {
		logf("op: %#v", op)
		if len(op.ClearRecordIDs) > 0 {
			logf("  clearRecordIDs: %d", op.ClearRecordIDs)
		}
		if len(op.ClearFields) > 0 {
			logf("  clearFields: %s", op.ClearFields)
		}
		for field, fieldOp := range op.FieldOps {
			logf("  field %q: %#v", field, fieldOp)
		}
	}
}
