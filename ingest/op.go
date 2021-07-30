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
	"math/bits"

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

type FieldType string

const (
	FieldTypeSet         = "set"
	FieldTypeInt         = "int"
	FieldTypeTimeQuantum = "time"
	FieldTypeTimeStamp   = "timestamp"
	FieldTypeDecimal     = "decimal"
	FieldTypeMutex       = "mutex"
	FieldTypeBool        = "bool"
)

var fieldTypeSorts = map[FieldType]func(*FieldOperation){
	FieldTypeSet:         (*FieldOperation).SortByValues,
	FieldTypeInt:         (*FieldOperation).SortByRecords,
	FieldTypeTimeQuantum: (*FieldOperation).SortByValues,
	FieldTypeDecimal:     (*FieldOperation).SortByRecords,
	FieldTypeMutex:       (*FieldOperation).SortByValues,
	FieldTypeTimeStamp:   (*FieldOperation).SortByRecords,
	FieldTypeBool:        (*FieldOperation).SortByValues,
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

// Sort sorts the values by record ID. It is not a stable sort.
func (o *Operation) Sort() {
	// I am aware that this is a crime, but it avoids rewriting
	// the code and justifies FieldOperation handling the "only record
	// IDs" case.
	f := FieldOperation{RecordIDs: o.ClearRecordIDs}
	f.SortByRecords()
}

type ShardedFieldOperation map[uint64]*FieldOperation

// Shard() divides the FieldOperation's values up into corresponding chunks
// based on the shards of record IDs. Does not further sort IDs within those
// chunks.
func (f *FieldOperation) Shard() ShardedFieldOperation {
	if len(f.RecordIDs) == 0 {
		return nil
	}
	return f.SortToShards()
}

// ShardInto puts the shards it finds into a target map.
func (f *FieldOperation) ShardInto(target ShardedFieldOperation) {
	shards, ends := shardwidth.FindShards(f.RecordIDs)
	prev := 0
	for i, shard := range shards {
		endIndex := ends[i]
		subOp := &FieldOperation{RecordIDs: f.RecordIDs[prev:endIndex]}
		if len(f.Values) > 0 {
			subOp.Values = f.Values[prev:endIndex]
		}
		if len(f.Signed) > 0 {
			subOp.Signed = f.Signed[prev:endIndex]
		}
		target[shard] = subOp
		prev = endIndex
	}
}

func ShardIDs(ids []uint64) (out map[uint64][]uint64) {
	shards, ends := shardwidth.FindShards(ids)
	prev := 0
	out = make(map[uint64][]uint64, len(shards))
	for i, shard := range shards {
		endIndex := ends[i]
		out[shard] = ids[prev:endIndex]
		prev = endIndex
	}
	return out
}

// SortToShards() uses a pseudo-radix-sort to divide inputs into
// shards; the individual shards are not sorted.
func (f *FieldOperation) SortToShards() ShardedFieldOperation {
	if len(f.RecordIDs) == 0 {
		return nil
	}
	diffMask := uint64(0)
	prev := f.RecordIDs[0]
	for _, r := range f.RecordIDs[1:] {
		diffMask |= r ^ prev
		prev = r
	}
	bitsRemaining := bits.Len64(diffMask)
	if bitsRemaining <= shardwidth.Exponent {
		return map[uint64]*FieldOperation{f.RecordIDs[0] >> shardwidth.Exponent: f}
	}
	output := make(ShardedFieldOperation)
	sortToShardsInto(f, bitsRemaining-8, output)
	return output
}

// sortToShardsInto puts the shards it finds into the given map, so that
// as we split off buckets, they can be inserted into the same map.
func sortToShardsInto(f *FieldOperation, shift int, into ShardedFieldOperation) {
	if shift < shardwidth.Exponent {
		shift = shardwidth.Exponent
	}
	nextShift := shift - 8
	if nextShift < shardwidth.Exponent {
		nextShift = shardwidth.Exponent
	}
	// count things that belong in each of the 256 buckets
	var buckets [256]int
	var starts [256]int

	// compute the buckets ourselves
	for _, r := range f.RecordIDs {
		b := (r >> shift) & 0xFF
		buckets[b]++
	}
	total := 0
	// compute starting points of each bucket, converting the
	// bucket counts into ends
	for i := range buckets {
		starts[i] = total
		total += buckets[i]
		buckets[i] = total
	}
	// starts[n] is the index of the first thing that should
	// go in that bucket, buckets[n] is the index of the first
	// thing that shouldn't
	var bucketOp FieldOperation
	for bucket, start := range starts {
		end := buckets[bucket]
		if end <= start {
			continue
		}
		for j := start; j < end; j++ {
			want := int((f.RecordIDs[j] >> shift) & 0xFF)
			for want != bucket {
				// move this to the beginning of the
				// bucket it wants to be in, swapping
				// the thing there here
				dst := starts[want]
				f.RecordIDs[j], f.RecordIDs[dst] = f.RecordIDs[dst], f.RecordIDs[j]
				if f.Values != nil {
					f.Values[j], f.Values[dst] = f.Values[dst], f.Values[j]
				}
				if f.Signed != nil {
					f.Signed[j], f.Signed[dst] = f.Signed[dst], f.Signed[j]
				}
				starts[want]++
				want = int((f.RecordIDs[j] >> shift) & 0xFF)
			}
		}
		// If shift == shardwidth.Exponent, then this is a completed
		// shard and can go into the sharded output. otherwise, we
		// can subdivide it.
		bucketOp.RecordIDs = f.RecordIDs[start:end]
		if f.Values != nil {
			bucketOp.Values = f.Values[start:end]
		}
		if f.Signed != nil {
			bucketOp.Signed = f.Signed[start:end]
		}
		if shift == shardwidth.Exponent {
			x := bucketOp
			into[f.RecordIDs[start]>>shardwidth.Exponent] = &x
		} else {
			sortToShardsInto(&bucketOp, nextShift, into)
		}
	}
}

const shardMask = ((uint64(1) << shardwidth.Exponent) - 1)

// SortByValues sorts the operation by values first, then by record
// ID within each value. This is the best ordering for set/mutex fields,
// where we'll want to generate positions in that order. For these
// purposes, a time quantum or bool counts as a kind of a set.
func (f *FieldOperation) SortByValues() {
	keys := make([]uint64, len(f.RecordIDs))
	for i, v := range f.RecordIDs {
		keys[i] = (f.Values[i] << shardwidth.Exponent) | (v & shardMask)
	}
	f.SortByKeys(keys)
}

// SortByRecords sorts the operation by record ID, and not by value at
// all. This makes the most sense for int fields and the like.
func (f *FieldOperation) SortByRecords() {
	f.SortByKeys(f.RecordIDs)
}

// SortByKeys reorganizes the record IDs and values of f according to the
// corresponding members of keys.
func (f *FieldOperation) SortByKeys(keys []uint64) {
	if len(f.RecordIDs) < 2 {
		return
	}
	diffMask := uint64(0)
	prev := keys[0]
	for _, r := range keys[1:] {
		diffMask |= r ^ prev
		prev = r
	}
	bitsRemaining := bits.Len64(diffMask)
	sortPartialByKeys(f, keys, bitsRemaining-8)
}

// simpleSort sorts a FieldOperation by external keys, or record IDs. It's a
// horribly naive bubble sort because N is small and a more complex algorithm
// doesn't help as much as you'd hope. This beats using stdlib sort by about
// a factor of two for those small N, for larger N we're using the radix sort
// that calls this.
func simpleSort(f *FieldOperation, keys []uint64) {
	if keys != nil {
		// sorting by record IDs
		if f.Values != nil && f.Signed != nil {
			for i := 1; i < len(keys); i++ {
				for j := i; j > 0 && keys[j-1] > keys[j]; j-- {
					keys[j-1], keys[j] = keys[j], keys[j-1]
					f.RecordIDs[j-1], f.RecordIDs[j] = f.RecordIDs[j], f.RecordIDs[j-1]
					f.Values[j-1], f.Values[j] = f.Values[j], f.Values[j-1]
					f.Signed[j-1], f.Signed[j] = f.Signed[j], f.Signed[j-1]

				}
			}
		} else if f.Values != nil {
			for i := 1; i < len(keys); i++ {
				for j := i; j > 0 && keys[j-1] > keys[j]; j-- {
					keys[j-1], keys[j] = keys[j], keys[j-1]
					f.RecordIDs[j-1], f.RecordIDs[j] = f.RecordIDs[j], f.RecordIDs[j-1]

					f.Values[j-1], f.Values[j] = f.Values[j], f.Values[j-1]
				}
			}
		} else if f.Signed != nil {
			for i := 1; i < len(keys); i++ {
				for j := i; j > 0 && keys[j-1] > keys[j]; j-- {
					keys[j-1], keys[j] = keys[j], keys[j-1]
					f.RecordIDs[j-1], f.RecordIDs[j] = f.RecordIDs[j], f.RecordIDs[j-1]
					f.Signed[j-1], f.Signed[j] = f.Signed[j], f.Signed[j-1]
				}
			}
		} else {
			for i := 1; i < len(keys); i++ {
				for j := i; j > 0 && keys[j-1] > keys[j]; j-- {
					keys[j-1], keys[j] = keys[j], keys[j-1]
					f.RecordIDs[j-1], f.RecordIDs[j] = f.RecordIDs[j], f.RecordIDs[j-1]
				}
			}
		}
	} else {
		if f.Values != nil && f.Signed != nil {
			for i := 1; i < len(f.RecordIDs); i++ {
				for j := i; j > 0 && f.RecordIDs[j-1] > f.RecordIDs[j]; j-- {
					f.RecordIDs[j-1], f.RecordIDs[j] = f.RecordIDs[j], f.RecordIDs[j-1]
					f.Values[j-1], f.Values[j] = f.Values[j], f.Values[j-1]
					f.Signed[j-1], f.Signed[j] = f.Signed[j], f.Signed[j-1]
				}
			}
		} else if f.Values != nil {
			for i := 1; i < len(f.RecordIDs); i++ {
				for j := i; j > 0 && f.RecordIDs[j-1] > f.RecordIDs[j]; j-- {
					f.RecordIDs[j-1], f.RecordIDs[j] = f.RecordIDs[j], f.RecordIDs[j-1]
					f.Values[j-1], f.Values[j] = f.Values[j], f.Values[j-1]
				}
			}
		} else if f.Signed != nil {
			for i := 1; i < len(f.RecordIDs); i++ {
				for j := i; j > 0 && f.RecordIDs[j-1] > f.RecordIDs[j]; j-- {
					f.RecordIDs[j-1], f.RecordIDs[j] = f.RecordIDs[j], f.RecordIDs[j-1]
					f.Signed[j-1], f.Signed[j] = f.Signed[j], f.Signed[j-1]
				}
			}
		} else {
			// why do we only have record IDs? I don't know
			for i := 1; i < len(f.RecordIDs); i++ {
				for j := i; j > 0 && f.RecordIDs[j-1] > f.RecordIDs[j]; j-- {
					f.RecordIDs[j-1], f.RecordIDs[j] = f.RecordIDs[j], f.RecordIDs[j-1]
				}
			}
		}
	}
}

func sortPartialByKeys(f *FieldOperation, keys []uint64, shift int) {
	if shift < 0 {
		shift = 0
	}
	externalKeys := &f.RecordIDs[0] != &keys[0]
	nextShift := shift - 8
	if nextShift < 0 {
		nextShift = 0
	}
	// count things that belong in each of the 256 buckets
	var buckets [256]int
	var starts [256]int
	// compute the buckets ourselves
	for _, r := range keys {
		b := (r >> shift) & 0xFF
		buckets[b]++
	}
	total := 0
	// compute starting points of each bucket, converting the
	// bucket counts into ends
	for i := range buckets {
		starts[i] = total
		total += buckets[i]
		buckets[i] = total
	}
	// starts[n] is the index of the first thing that should
	// go in that bucket, buckets[n] is the index of the first
	// thing that shouldn't
	// var newbuckets [256]int
	var bucketOp FieldOperation
	for bucket, start := range starts {
		end := buckets[bucket]
		if end <= start {
			continue
		}
		for j := start; j < end; j++ {
			want := int((keys[j] >> shift) & 0xFF)
			for want != bucket {
				// move this to the beginning of the
				// bucket it wants to be in, swapping
				// the thing there here
				dst := starts[want]
				keys[j], keys[dst] = keys[dst], keys[j]
				// we do this to allow you to just pass in the records as keys
				if externalKeys {
					f.RecordIDs[j], f.RecordIDs[dst] = f.RecordIDs[dst], f.RecordIDs[j]
				}
				if f.Values != nil {
					f.Values[j], f.Values[dst] = f.Values[dst], f.Values[j]
				}
				if f.Signed != nil {
					f.Signed[j], f.Signed[dst] = f.Signed[dst], f.Signed[j]
				}
				starts[want]++
				want = int((keys[j] >> shift) & 0xFF)
			}
		}
		// If shift == shardwidth.Exponent, then this is a completed
		// shard and can go into the sharded output. otherwise, we
		// can subdivide it.
		if shift > 0 {
			bucketOp.RecordIDs = f.RecordIDs[start:end]
			if f.Values != nil {
				bucketOp.Values = f.Values[start:end]
			}
			if f.Signed != nil {
				bucketOp.Signed = f.Signed[start:end]
			}
			// if there's not very many, sort naively instead
			if end-start > 32 {
				sortPartialByKeys(&bucketOp, keys[start:end], nextShift)
			} else {
				// naive stdlib sort
				if externalKeys {
					simpleSort(&bucketOp, keys[start:end])
				} else {
					simpleSort(&bucketOp, nil)
				}
			}
		}
	}
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
	FieldTypes map[string]FieldType
	Ops        []*Operation
}

// ShardedRequest is an ingest request, split up into individual per-shard
// operations.
type ShardedRequest struct {
	FieldTypes map[string]FieldType
	Ops        map[uint64][]*Operation
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
			sorter := fieldTypeSorts[r.FieldTypes[field]]
			if sorter == nil {
				sorter = (*FieldOperation).SortByRecords
			}
			for shard, data := range sharded {
				sorter(data)
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
