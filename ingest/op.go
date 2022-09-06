// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package ingest

import (
	"errors"
	"fmt"
	"math/bits"
	"sort"

	"github.com/featurebasedb/featurebase/v3/shardwidth"
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
//
// When we parse an operation, we assign each op a sequential ID
// within the overall request. We keep these IDs associated with ops
// when splitting them up across shards, so we can reverse this even
// if some shards don't get some ops.
type Operation struct {
	OpType         OpType
	Seq            int // sequence position within a chain of ops
	ClearRecordIDs []uint64
	ClearFields    []string
	FieldOps       map[string]*FieldOperation
}

// Compare reports whether two Operations seem to be the same. While
// a FieldOperation can be "empty" and compare-equal-to a nil FieldOperation,
// no Operation is considered empty even if it has no FieldOps.
func (got *Operation) Compare(expected *Operation) error {
	if got == nil && expected == nil {
		return nil
	}
	if got == nil {
		return fmt.Errorf("expected %q op, got nil", expected.OpType)
	}
	if expected == nil {
		return fmt.Errorf("expected no op, got %q", got.OpType)
	}
	if got.OpType != expected.OpType {
		return fmt.Errorf("operation type mismatch: expected %q, got %q", expected.OpType, got.OpType)
	}
	if len(got.ClearRecordIDs) != len(expected.ClearRecordIDs) {
		return fmt.Errorf("clear record counts differ: expected %d, got %d", len(expected.ClearRecordIDs), len(got.ClearRecordIDs))
	}
	for i, v1 := range got.ClearRecordIDs {
		v2 := expected.ClearRecordIDs[i]
		if v1 != v2 {
			return fmt.Errorf("clear record id %d differs: expected %d, got %d", i, v2, v1)
		}
	}
	// don't assume consistent ordering for the fields, because they're
	// coming out in arbitrary hash order
	seenFields := make(map[string]struct{}, len(expected.ClearFields))
	for _, v1 := range expected.ClearFields {
		seenFields[v1] = struct{}{}
	}
	for _, v2 := range got.ClearFields {
		if _, ok := seenFields[v2]; !ok {
			return fmt.Errorf("field %q cleared unexpectedly", v2)
		}
		delete(seenFields, v2)
	}
	for v1 := range seenFields {
		return fmt.Errorf("field %q should be cleared but wasn't", v1)
	}
	// We check compare even if the op we find on one side is nil, so
	// a round-trip test will consider a missing op and an empty op to
	// be interchangeable.
	for k, fo1 := range got.FieldOps {
		fo2 := expected.FieldOps[k]
		if err := fo1.Compare(fo2); err != nil {
			return fmt.Errorf("field %q mismatch: %w", k, err)
		}
	}
	for k, fo2 := range expected.FieldOps {
		fo1 := got.FieldOps[k]
		if err := fo1.Compare(fo2); err != nil {
			return fmt.Errorf("field %q mismatch: %w", k, err)
		}
	}
	if expected.Seq != got.Seq {
		return fmt.Errorf("sequence mismatch: expected %d, got %d", expected.Seq, got.Seq)
	}
	return nil
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

// Sort sorts the clear record IDs and field list.
func (o *Operation) Sort() {
	// I am aware that this is a crime, but it avoids rewriting
	// the code and justifies FieldOperation handling the "only record
	// IDs" case.
	f := FieldOperation{RecordIDs: o.ClearRecordIDs}
	f.SortByRecords()
	sort.Strings(o.ClearFields)
}

// clone makes a duplicate of the operation without shared storage
func (o *Operation) clone() *Operation {
	o2 := &Operation{
		OpType:         o.OpType,
		Seq:            o.Seq,
		ClearRecordIDs: append([]uint64{}, o.ClearRecordIDs...),
		ClearFields:    append([]string{}, o.ClearFields...),
		FieldOps:       make(map[string]*FieldOperation, len(o.FieldOps)),
	}
	for k, v := range o.FieldOps {
		o2.FieldOps[k] = v.clone()
	}
	return o2
}

// merge merges the fieldops of the provided operation into this operation.
func (o *Operation) merge(o2 *Operation) {
	o.ClearRecordIDs = append(o.ClearRecordIDs, o2.ClearRecordIDs...)
	o.ClearFields = append(o.ClearFields, o2.ClearFields...)
	for f, op := range o2.FieldOps {
		dst := o.FieldOps[f]
		if dst == nil {
			o.FieldOps[f] = op
			continue
		}
		// otherwise append any contents. dst is a pointer-to, so this
		// updates the thing the map points to, we don't have to write back
		// into the map.
		dst.RecordIDs = append(dst.RecordIDs, op.RecordIDs...)
		dst.Values = append(dst.Values, op.Values...)
		dst.Signed = append(dst.Signed, op.Signed...)
	}
}

type ShardedFieldOperation map[uint64]*FieldOperation

// ByShard() divides the FieldOperation's values up into corresponding chunks
// based on the shards of record IDs. Does not further sort IDs within those
// chunks.
func (f *FieldOperation) ByShard() ShardedFieldOperation {
	if len(f.RecordIDs) == 0 {
		return nil
	}
	return f.SortToShards()
}

// clone makes a duplicate of the operation without shared storage
func (f *FieldOperation) clone() *FieldOperation {
	f2 := &FieldOperation{
		RecordIDs: append([]uint64{}, f.RecordIDs...),
		Values:    append(([]uint64)(nil), f.Values...),
		Signed:    append(([]int64)(nil), f.Signed...),
	}
	return f2
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
//
// External keys exist only when we are sorting by value, which is to say,
// when we're using row-oriented formats (set, mutex, time quantum).
// For int/decimal/timestamp fields, we're sorting by record only.
// So, if keys is the same as f.RecordIDs, we're looking at an int field
// or equivalent, so Signed exists and Values doesn't exist.
// Otherwise, we might be looking at a time quantum field (both exist)
// or set/mutex (only Values exist).
func simpleSort(f *FieldOperation, keys []uint64) {
	// keys might actually just point to record IDs, in which case, we don't
	// want to shuffle the corresponding RecordIDs too, because that would just
	// reverse our swaps. If they're different, we actually need to swap them
	// both.
	if &keys[0] != &f.RecordIDs[0] {
		// sorting by record IDs
		if f.Values != nil && f.Signed != nil { // time quantum field
			for i := 1; i < len(keys); i++ {
				for j := i; j > 0 && keys[j-1] > keys[j]; j-- {
					keys[j-1], keys[j] = keys[j], keys[j-1]
					f.RecordIDs[j-1], f.RecordIDs[j] = f.RecordIDs[j], f.RecordIDs[j-1]
					f.Values[j-1], f.Values[j] = f.Values[j], f.Values[j-1]
					f.Signed[j-1], f.Signed[j] = f.Signed[j], f.Signed[j-1]

				}
			}
		} else if f.Values != nil { // set/mutex/bool
			for i := 1; i < len(keys); i++ {
				for j := i; j > 0 && keys[j-1] > keys[j]; j-- {
					keys[j-1], keys[j] = keys[j], keys[j-1]
					f.RecordIDs[j-1], f.RecordIDs[j] = f.RecordIDs[j], f.RecordIDs[j-1]

					f.Values[j-1], f.Values[j] = f.Values[j], f.Values[j-1]
				}
			}
		} else if f.Signed != nil { // can't-happen, we think
			for i := 1; i < len(keys); i++ {
				for j := i; j > 0 && keys[j-1] > keys[j]; j-- {
					keys[j-1], keys[j] = keys[j], keys[j-1]
					f.RecordIDs[j-1], f.RecordIDs[j] = f.RecordIDs[j], f.RecordIDs[j-1]
					f.Signed[j-1], f.Signed[j] = f.Signed[j], f.Signed[j-1]
				}
			}
		} else { // can't-happen, we think
			for i := 1; i < len(keys); i++ {
				for j := i; j > 0 && keys[j-1] > keys[j]; j-- {
					keys[j-1], keys[j] = keys[j], keys[j-1]
					f.RecordIDs[j-1], f.RecordIDs[j] = f.RecordIDs[j], f.RecordIDs[j-1]
				}
			}
		}
	} else {
		if f.Values == nil && f.Signed != nil {
			for i := 1; i < len(f.RecordIDs); i++ {
				for j := i; j > 0 && f.RecordIDs[j-1] > f.RecordIDs[j]; j-- {
					f.RecordIDs[j-1], f.RecordIDs[j] = f.RecordIDs[j], f.RecordIDs[j-1]
					f.Signed[j-1], f.Signed[j] = f.Signed[j], f.Signed[j-1]
				}
			}
		} else if f.Values != nil && f.Signed != nil { // can't happen, we think
			for i := 1; i < len(f.RecordIDs); i++ {
				for j := i; j > 0 && f.RecordIDs[j-1] > f.RecordIDs[j]; j-- {
					f.RecordIDs[j-1], f.RecordIDs[j] = f.RecordIDs[j], f.RecordIDs[j-1]
					f.Values[j-1], f.Values[j] = f.Values[j], f.Values[j-1]
					f.Signed[j-1], f.Signed[j] = f.Signed[j], f.Signed[j-1]
				}
			}
		} else if f.Values != nil { // only happens during testing
			for i := 1; i < len(f.RecordIDs); i++ {
				for j := i; j > 0 && f.RecordIDs[j-1] > f.RecordIDs[j]; j-- {
					f.RecordIDs[j-1], f.RecordIDs[j] = f.RecordIDs[j], f.RecordIDs[j-1]
					f.Values[j-1], f.Values[j] = f.Values[j], f.Values[j-1]
				}
			}
		} else { // should definitely not happen
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
				simpleSort(&bucketOp, keys[start:end])
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

// Compare returns a diagnostic if the field operations do not seem
// equivalent.
func (got *FieldOperation) Compare(expected *FieldOperation) error {
	if got == nil {
		if expected == nil {
			return nil
		}
		// We don't worry about non-empty Values or Signed here, because in theory
		// RecordIDs are the Source of Truth as to what's in the op.
		if len(expected.RecordIDs) == 0 {
			return nil
		}
		return fmt.Errorf("expected field operation with %d records, got nil", len(expected.RecordIDs))
	}
	if expected == nil {
		if len(got.RecordIDs) == 0 {
			return nil
		}
		return fmt.Errorf("expected empty field operation, got %d records", len(got.RecordIDs))
	}
	if len(got.RecordIDs) != len(expected.RecordIDs) {
		return fmt.Errorf("record counts differ: expected %d, got %d", len(expected.RecordIDs), len(got.RecordIDs))
	}
	for i, v1 := range got.RecordIDs {
		v2 := expected.RecordIDs[i]
		if v1 != v2 {
			return fmt.Errorf("record id %d differs: expected %d, got %d", i, v2, v1)
		}
	}
	if len(got.Values) != len(expected.Values) {
		return fmt.Errorf("value counts differ: expected %d, got %d", len(expected.Values), len(got.Values))
	}
	for i, v1 := range got.Values {
		v2 := expected.Values[i]
		if v1 != v2 {
			return fmt.Errorf("value %d differs: expected %d, got %d", i, v2, v1)
		}
	}
	if len(got.Signed) != len(expected.Signed) {
		return fmt.Errorf("signed value counts differ: expected %d, got %d", len(expected.Signed), len(got.Signed))
	}
	for i, v1 := range got.Signed {
		v2 := expected.Signed[i]
		if v1 != v2 {
			return fmt.Errorf("signed value %d differs: expected %d, got %d", i, v2, v1)
		}
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

// ByShard converts a request into the same request, only sharded.
func (r *Request) ByShard(fields map[string]FieldType) (*ShardedRequest, error) {
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
				shards[shard] = &Operation{OpType: op.OpType, Seq: op.Seq, ClearRecordIDs: data, ClearFields: op.ClearFields, FieldOps: map[string]*FieldOperation{}}
			}
		}
		for field, fieldOp := range op.FieldOps {
			sharded := fieldOp.ByShard()
			sorter := fieldTypeSorts[fields[field]]
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
					shardOp = &Operation{OpType: op.OpType, Seq: op.Seq}
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

// merge combines the components of a sharded request back into a single
// unsharded request, processing shards in numerical order.
func (s *ShardedRequest) merge() *Request {
	req := &Request{}
	if s == nil || len(s.Ops) == 0 {
		return req
	}
	shards := make([]uint64, 0, len(s.Ops))
	for shard := range s.Ops {
		shards = append(shards, shard)
	}
	sort.Slice(shards, func(i, j int) bool { return shards[i] < shards[j] })
	for _, shard := range shards {
		ops := s.Ops[shard]
		for _, op := range ops {
			var _ *Operation
			if op.Seq >= len(req.Ops) {
				// Pad out with nil *Operations to the required length
				req.Ops = append(req.Ops, make([]*Operation, op.Seq+1-len(req.Ops))...)
			}
			if req.Ops[op.Seq] == nil {
				req.Ops[op.Seq] = op.clone()
				continue
			}
			req.Ops[op.Seq].merge(op)
		}
	}
	return req
}

func (r *Request) Dump(logf func(string, ...interface{})) {
	logf("req: %#v", r)
	for _, op := range r.Ops {
		logf("op: %#v", op)
		if len(op.ClearRecordIDs) > 0 {
			if len(op.ClearRecordIDs) > 8 {
				logf("  clearRecordIDs: %d...+%d", op.ClearRecordIDs[:8], len(op.ClearRecordIDs)-8)
			} else {
				logf("  clearRecordIDs: %d", op.ClearRecordIDs)
			}
		}
		if len(op.ClearFields) > 0 {
			if len(op.ClearFields) > 8 {
				logf("  clearFields: %s...+%d", op.ClearFields[:8], len(op.ClearFields)-8)
			} else {
				logf("  clearFields: %s", op.ClearFields)
			}
		}
		for field, fieldOp := range op.FieldOps {
			if fieldOp != nil {
				logf("  field %q: op (%d/%d/%d)", field, len(fieldOp.RecordIDs), len(fieldOp.Values), len(fieldOp.Signed))
				if len(fieldOp.RecordIDs) > 0 {
					if len(fieldOp.RecordIDs) > 8 {
						logf("    records %d...+%d", fieldOp.RecordIDs[:8], len(fieldOp.RecordIDs)-8)
					} else {
						logf("    records %d", fieldOp.RecordIDs)
					}
				}
			} else {
				logf("  field %q: nil op", field)
			}
		}
	}
}

func (r *Request) Compare(other *Request) error {
	if other == nil {
		if r != nil && len(r.Ops) != 0 {
			return errors.New("non-empty sharded request can't equal empty/nil sharded request")
		}
		// empty and nil are allowed
		return nil
	}
	if r == nil {
		if other != nil && len(other.Ops) != 0 {
			return errors.New("non-empty sharded request can't equal empty/nil sharded request")
		}
		// empty and nil are allowed
		return nil
	}
	ops := r.Ops
	ops2 := other.Ops
	if len(ops2) != len(ops) {
		return fmt.Errorf("expected %d ops, got %d", len(ops), len(ops2))
	}
	for i, op := range ops {
		if err := op.Compare(ops2[i]); err != nil {
			return fmt.Errorf("op %d: %v", i, err)
		}
	}
	return nil
}

// Compare checks whether two ShardedRequest objects represent the same
// data. Empty shards shouldn't have entries in the map in the first place,
// so we don't accept a nil or 0-length slice of ops as equal to the
// shard key not existing, but we do accept nil or empty requests as
// equal to each other.
func (s *ShardedRequest) Compare(other *ShardedRequest) error {
	if other == nil {
		if s != nil && len(s.Ops) != 0 {
			return errors.New("non-empty sharded request can't equal empty/nil sharded request")
		}
		// empty and nil are allowed
		return nil
	}
	if s == nil {
		if other != nil && len(other.Ops) != 0 {
			return errors.New("non-empty sharded request can't equal empty/nil sharded request")
		}
		// empty and nil are allowed
		return nil
	}
	for shard, ops := range s.Ops {
		ops2, ok := other.Ops[shard]
		if !ok {
			return fmt.Errorf("shard %d missing in other", shard)
		}
		if len(ops2) != len(ops) {
			return fmt.Errorf("shard %d: expected %d ops, got %d", shard, len(ops), len(ops2))
		}
		for i, op := range ops {
			if err := op.Compare(ops2[i]); err != nil {
				return fmt.Errorf("shard %d, op %d: %v", shard, i, err)
			}
		}
	}
	if len(other.Ops) != len(s.Ops) {
		for shard := range other.Ops {
			if _, ok := s.Ops[shard]; !ok {
				return fmt.Errorf("shard %d missing in self", shard)
			}
		}
	}
	return nil
}
