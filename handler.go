// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package pilosa

import (
	"encoding/json"
	"math/bits"
	"time"

	"github.com/featurebasedb/featurebase/v3/ingest"
	"github.com/featurebasedb/featurebase/v3/tracing"
	"github.com/pkg/errors"
)

// QueryRequest represent a request to process a query.
type QueryRequest struct {
	// Index to execute query against.
	Index string

	// The query string to parse and execute.
	Query string

	// The SQL source query, if applicable.
	SQLQuery string

	// The shards to include in the query execution.
	// If empty, all shards are included.
	Shards []uint64

	// If true, indicates that query is part of a larger distributed query.
	// If false, this request is on the originating node.
	Remote bool

	// Query has already been translated. This is only used if Remote
	// is false, Remote=true implies this.
	PreTranslated bool

	// Should we profile this query?
	Profile bool

	// Additional data associated with the query, in cases where there's
	// row-style inputs for precomputed values.
	EmbeddedData []*Row

	// Limit on memory used by request (Extract() only)
	MaxMemory int64
}

// QueryResponse represent a response from a processed query.
type QueryResponse struct {
	// Result for each top-level query call.
	// The result type differs depending on the query; types
	// include: Row, RowIdentifiers, GroupCounts, SignedRow,
	// ValCount, Pair, Pairs, bool, uint64.
	Results []interface{}

	// Error during parsing or execution.
	Err error

	// Profiling data, if any
	Profile *tracing.Profile
}

// MarshalJSON marshals QueryResponse into a JSON-encoded byte slice
func (resp *QueryResponse) MarshalJSON() ([]byte, error) {
	if resp.Err != nil {
		return json.Marshal(struct {
			Err string `json:"error"`
		}{Err: resp.Err.Error()})
	}

	return json.Marshal(struct {
		Results []interface{}    `json:"results"`
		Profile *tracing.Profile `json:"profile,omitempty"`
	}{
		Results: resp.Results,
		Profile: resp.Profile,
	})
}

// HandlerI is the interface for the data handler, a wrapper around
// Pilosa's data store.
type HandlerI interface {
	Serve() error
	Close() error
}

type nopHandler struct{}

func (n nopHandler) Serve() error {
	return nil
}

func (n nopHandler) Close() error {
	return nil
}

// NopHandler is a no-op implementation of the Handler interface.
var NopHandler HandlerI = nopHandler{}

// ImportValueRequest describes the import request structure
// for a value (BSI) import.
// Note: no RowIDs here. have to convert BSI Values into RowIDs internally.
type ImportValueRequest struct {
	Index          string
	IndexCreatedAt int64
	Field          string
	FieldCreatedAt int64
	// if Shard is MaxUint64 (an impossible shard value), this
	// indicates that the column IDs may come from multiple shards.
	Shard           uint64
	ColumnIDs       []uint64 // e.g. weather stationID
	ColumnKeys      []string
	Values          []int64 // e.g. temperature, humidity, barometric pressure
	FloatValues     []float64
	TimestampValues []time.Time
	StringValues    []string
	Clear           bool
	scratch         []int // scratch space to allow us to get a stable sort in reasonable time
}

func (ivr *ImportValueRequest) Clone() *ImportValueRequest {
	newIVR := &ImportValueRequest{}
	if ivr == nil {
		return newIVR
	}
	*newIVR = *ivr
	// don't copy the internal scratch buffer
	newIVR.scratch = nil
	if len(ivr.ColumnIDs) > 0 {
		newIVR.ColumnIDs = make([]uint64, len(ivr.ColumnIDs))
		copy(newIVR.ColumnIDs, ivr.ColumnIDs)
	}
	if len(ivr.ColumnKeys) > 0 {
		newIVR.ColumnKeys = make([]string, len(ivr.ColumnKeys))
		copy(newIVR.ColumnKeys, ivr.ColumnKeys)
	}
	if len(ivr.Values) > 0 {
		newIVR.Values = make([]int64, len(ivr.Values))
		copy(newIVR.Values, ivr.Values)
	}
	if len(ivr.FloatValues) > 0 {
		newIVR.FloatValues = make([]float64, len(ivr.FloatValues))
		copy(newIVR.FloatValues, ivr.FloatValues)
	}
	if len(ivr.TimestampValues) > 0 {
		newIVR.TimestampValues = make([]time.Time, len(ivr.TimestampValues))
		copy(newIVR.TimestampValues, ivr.TimestampValues)
	}
	if len(ivr.StringValues) > 0 {
		newIVR.StringValues = make([]string, len(ivr.StringValues))
		copy(newIVR.StringValues, ivr.StringValues)
	}
	return newIVR
}

// AtomicRecord applies all its Ivr and Ivr atomically, in a Tx.
// The top level Shard has to agree with Ivr[i].Shard and the Iv[i].Shard
// for all i included (in Ivr and Ir). The same goes for the top level Index: all records
// have to be writes to the same Index. These requirements are checked.
type AtomicRecord struct {
	Index string
	Shard uint64

	Ivr []*ImportValueRequest // BSI values
	Ir  []*ImportRequest      // other field types, e.g. single bit
}

func (ar *AtomicRecord) Clone() *AtomicRecord {
	newAR := &AtomicRecord{Index: ar.Index, Shard: ar.Shard}
	newAR.Ivr = make([]*ImportValueRequest, len(ar.Ivr))
	for i, vr := range ar.Ivr {
		newAR.Ivr[i] = vr.Clone()
	}
	newAR.Ir = make([]*ImportRequest, len(ar.Ir))
	for i, vr := range ar.Ir {
		newAR.Ir[i] = vr.Clone()
	}
	return newAR
}

func (ivr *ImportValueRequest) Len() int { return len(ivr.ColumnIDs) }
func (ivr *ImportValueRequest) Less(i, j int) bool {
	if ivr.ColumnIDs[i] < ivr.ColumnIDs[j] {
		return true
	}
	if ivr.ColumnIDs[i] > ivr.ColumnIDs[j] {
		return false
	}
	if len(ivr.scratch) > 0 {
		return ivr.scratch[i] < ivr.scratch[j]
	}
	return false
}

func (ivr *ImportValueRequest) Swap(i, j int) {
	ivr.ColumnIDs[i], ivr.ColumnIDs[j] = ivr.ColumnIDs[j], ivr.ColumnIDs[i]
	if len(ivr.Values) > 0 {
		ivr.Values[i], ivr.Values[j] = ivr.Values[j], ivr.Values[i]
	} else if len(ivr.FloatValues) > 0 {
		ivr.FloatValues[i], ivr.FloatValues[j] = ivr.FloatValues[j], ivr.FloatValues[i]
	} else if len(ivr.TimestampValues) > 0 {
		ivr.TimestampValues[i], ivr.TimestampValues[j] = ivr.TimestampValues[j], ivr.TimestampValues[i]
	} else if len(ivr.StringValues) > 0 {
		ivr.StringValues[i], ivr.StringValues[j] = ivr.StringValues[j], ivr.StringValues[i]
	}
	if len(ivr.scratch) > 0 {
		ivr.scratch[i], ivr.scratch[j] = ivr.scratch[j], ivr.scratch[i]
	}
}

// Validate ensures that the payload of the request is valid.
func (ivr *ImportValueRequest) Validate() error {
	return ivr.ValidateWithTimestamp(ivr.IndexCreatedAt, ivr.FieldCreatedAt)
}

// ValidateWithTimestamp ensures that the payload of the request is valid.
func (ivr *ImportValueRequest) ValidateWithTimestamp(indexCreatedAt, fieldCreatedAt int64) error {
	if ivr.Index == "" || ivr.Field == "" {
		return errors.Errorf("index and field required, but got '%s' and '%s'", ivr.Index, ivr.Field)
	}
	if len(ivr.ColumnIDs) != 0 && len(ivr.ColumnKeys) != 0 {
		return errors.Errorf("must pass either column ids or keys, but not both")
	}
	var valueSetCount int
	if len(ivr.Values) != 0 {
		valueSetCount++
	}
	if len(ivr.FloatValues) != 0 {
		valueSetCount++
	}
	if len(ivr.TimestampValues) != 0 {
		valueSetCount++
	}
	if len(ivr.StringValues) != 0 {
		valueSetCount++
	}
	if valueSetCount > 1 {
		return errors.Errorf("must pass ints, floats, or strings but not multiple")
	}

	if (ivr.IndexCreatedAt != 0 && ivr.IndexCreatedAt != indexCreatedAt) ||
		(ivr.FieldCreatedAt != 0 && ivr.FieldCreatedAt != fieldCreatedAt) {
		return ErrPreconditionFailed
	}
	return nil
}

// ImportRequest describes the import request structure
// for an import.  BSIs use the ImportValueRequest instead.
type ImportRequest struct {
	Index          string
	IndexCreatedAt int64
	Field          string
	FieldCreatedAt int64
	Shard          uint64
	RowIDs         []uint64
	ColumnIDs      []uint64
	RowKeys        []string
	ColumnKeys     []string
	Timestamps     []int64
	Clear          bool
}

// Clone allows copying an import request. Normally you wouldn't, but
// some import functions are destructive on their inputs, and if you
// want to *re-use* an import request, you might need this. If you're
// using this outside tx_test, something is probably wrong.
func (ir *ImportRequest) Clone() *ImportRequest {
	newIR := &ImportRequest{}
	if ir == nil {
		return newIR
	}
	*newIR = *ir
	if ir.RowIDs != nil {
		newIR.RowIDs = make([]uint64, len(ir.RowIDs))
		copy(newIR.RowIDs, ir.RowIDs)
	}
	if ir.ColumnIDs != nil {
		newIR.ColumnIDs = make([]uint64, len(ir.ColumnIDs))
		copy(newIR.ColumnIDs, ir.ColumnIDs)
	}
	if ir.RowKeys != nil {
		newIR.RowKeys = make([]string, len(ir.RowKeys))
		copy(newIR.RowKeys, ir.RowKeys)
	}
	if ir.ColumnKeys != nil {
		newIR.ColumnKeys = make([]string, len(ir.ColumnKeys))
		copy(newIR.ColumnKeys, ir.ColumnKeys)
	}
	if ir.Timestamps != nil {
		newIR.Timestamps = make([]int64, len(ir.Timestamps))
		copy(newIR.Timestamps, ir.Timestamps)
	}
	return newIR
}

// SortToShards takes an import request which has been translated, but may
// not be sorted, and turns it into a map from shard IDs to individual import
// requests. We don't sort the entries within each shard because the correct
// sorting depends on the field type and we don't want to deal with that
// here.
func (ir *ImportRequest) SortToShards() (result map[uint64]*ImportRequest) {
	if len(ir.ColumnIDs) == 0 {
		return nil
	}
	diffMask := uint64(0)
	prev := ir.ColumnIDs[0]
	for _, r := range ir.ColumnIDs[1:] {
		diffMask |= r ^ prev
		prev = r
	}
	bitsRemaining := bits.Len64(diffMask)
	if bitsRemaining <= shardwidth.Exponent {
		shard := ir.ColumnIDs[0] >> shardwidth.Exponent
		ir.Shard = shard
		ir.ColumnKeys = nil
		ir.RowKeys = nil
		return map[uint64]*ImportRequest{shard: ir}
	}
	output := make(map[uint64]*ImportRequest)
	sortToShardsInto(ir, bitsRemaining-8, output)
	return output
}

// sortToShardsInto puts the shards it finds into the given map, so that
// as we split off buckets, they can be inserted into the same map.
func sortToShardsInto(ir *ImportRequest, shift int, into map[uint64]*ImportRequest) {
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
	for _, r := range ir.ColumnIDs {
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
	var bucketOp ImportRequest = *ir
	bucketOp.ColumnKeys = nil
	bucketOp.RowKeys = nil
	origStarts := make([]int, len(starts))
	copy(origStarts, starts[:])
	for bucket, start := range origStarts {
		end := buckets[bucket]
		if end <= start {
			continue
		}
		for j := start; j < end; j++ {
			want := int((ir.ColumnIDs[j] >> shift) & 0xFF)
			for want != bucket {
				// move this to the beginning of the
				// bucket it wants to be in, swapping
				// the thing there here
				dst := starts[want]
				ir.ColumnIDs[j], ir.ColumnIDs[dst] = ir.ColumnIDs[dst], ir.ColumnIDs[j]
				if ir.RowIDs != nil {
					ir.RowIDs[j], ir.RowIDs[dst] = ir.RowIDs[dst], ir.RowIDs[j]
				}
				if ir.Timestamps != nil {
					ir.Timestamps[j], ir.Timestamps[dst] = ir.Timestamps[dst], ir.Timestamps[j]
				}
				starts[want]++
				want = int((ir.ColumnIDs[j] >> shift) & 0xFF)
			}
		}
		// If shift == shardwidth.Exponent, then this is a completed
		// shard and can go into the sharded output. otherwise, we
		// can subdivide it.
		bucketOp.ColumnIDs = ir.ColumnIDs[start:end]
		if ir.RowIDs != nil {
			bucketOp.RowIDs = ir.RowIDs[start:end]
		}
		if ir.Timestamps != nil {
			bucketOp.Timestamps = ir.Timestamps[start:end]
		}
		if shift == shardwidth.Exponent {
			x := bucketOp
			shard := ir.ColumnIDs[start] >> shardwidth.Exponent
			x.Shard = shard
			into[shard] = &x
		} else {
			sortToShardsInto(&bucketOp, nextShift, into)
		}
	}
}

// ValidateWithTimestamp ensures that the payload of the request is valid.
func (ir *ImportRequest) ValidateWithTimestamp(indexCreatedAt, fieldCreatedAt int64) error {
	if (ir.IndexCreatedAt != 0 && ir.IndexCreatedAt != indexCreatedAt) ||
		(ir.FieldCreatedAt != 0 && ir.FieldCreatedAt != fieldCreatedAt) {
		return ErrPreconditionFailed
	}

	return nil
}

const (
	RequestActionSet       = "set"
	RequestActionClear     = "clear"
	RequestActionOverwrite = "overwrite"
)

// ImportRoaringRequest describes the import request structure
// for an import containing roaring-encoded data.
type ImportRoaringRequest struct {
	IndexCreatedAt  int64
	FieldCreatedAt  int64
	Clear           bool
	Action          string // [set, clear, overwrite]
	Block           int
	Views           map[string][]byte
	UpdateExistence bool
}

// ImportRoaringShardRequest is the request for the shard
// transactional endpoint.
type ImportRoaringShardRequest struct {
	// Has this request already been forwarded to all replicas? If
	// Remote=false, then the handling server is responsible for
	// ensuring this request is sent to all repliacs before returning
	// a successful response to the client.
	Remote bool
	Views  []RoaringUpdate
}

// RoaringUpdate represents the bits to clear and then set in a particular view.
type RoaringUpdate struct {
	Field string
	View  string

	// Clear is a roaring encoded bitmatrix of bits to clear. For
	// mutex or int-like fields, only the first row is looked at and
	// the bits in that row are cleared from every row.
	Clear []byte

	// Set is the roaring encoded bitmatrix of bits to set. If this is
	// a mutex or int-like field, we'll assume the first shard width
	// of containers is the exists row and we will first clear all
	// bits in those columns and then set
	Set []byte

	// ClearRecords, when true, denotes that Clear should be
	// interpreted as a single row which will be subtracted from every
	// row in this view.
	ClearRecords bool
}

// ValidateWithTimestamp ensures that the payload of the request is valid.
func (irr *ImportRoaringRequest) ValidateWithTimestamp(indexCreatedAt, fieldCreatedAt int64) error {
	if (irr.IndexCreatedAt != 0 && irr.IndexCreatedAt != indexCreatedAt) ||
		(irr.FieldCreatedAt != 0 && irr.FieldCreatedAt != fieldCreatedAt) {
		return ErrPreconditionFailed
	}
	return nil
}

// ImportResponse is the structured response of an import.
type ImportResponse struct {
	Err string
}

// BlockDataRequest describes the structure of a request
// for fragment block data.
type BlockDataRequest struct {
	Index string
	Field string
	View  string
	Shard uint64
	Block uint64
}

// BlockDataResponse is the structured response of a block
// data request.
type BlockDataResponse struct {
	RowIDs    []uint64
	ColumnIDs []uint64
}

// TranslateKeysRequest describes the structure of a request
// for a batch of key translations.
type TranslateKeysRequest struct {
	Index string
	Field string
	Keys  []string

	// NotWritable is an awkward name, but it's just to keep backward compatibility with client and idk.
	NotWritable bool
}

// TranslateKeysResponse is the structured response of a key
// translation request.
type TranslateKeysResponse struct {
	IDs []uint64
}

// TranslateIDsRequest describes the structure of a request
// for a batch of id translations.
type TranslateIDsRequest struct {
	Index string
	Field string
	IDs   []uint64
}

// TranslateIDsResponse is the structured response of a id
// translation request.
type TranslateIDsResponse struct {
	Keys []string
}
