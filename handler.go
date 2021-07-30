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

package pilosa

import (
	"encoding/json"
	"time"

	"github.com/molecula/featurebase/v2/tracing"
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

// Handler is the interface for the data handler, a wrapper around
// Pilosa's data store.
type Handler interface {
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
var NopHandler Handler = nopHandler{}

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

// AtomicRecord applies all its Ivr and Ivr atomically, in a Tx.
// The top level Shard has to agree with Ivr[i].Shard and the Iv[i].Shard
// for all i included (in Ivr and Ir). The same goes for the top level Index: all records
// have to be writes to the same Index. These requirements are checked.
//
type AtomicRecord struct {
	Index string
	Shard uint64

	Ivr []*ImportValueRequest // BSI values
	Ir  []*ImportRequest      // other field types, e.g. single bit
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

// InspectRequestParams represents the parts of an InspectRequest that
// aren't generic holder filtering attributes.
type InspectRequestParams struct {
	Containers bool // include container details
	Checksum   bool // perform checksums
}

// InspectRequest represents a request for a possibly-partial
// holder inspection, using a provided holder filter and inspect-specific
// parameters.
type InspectRequest struct {
	HolderFilterParams
	InspectRequestParams
}

// InspectResponse contains the structured results for an InspectRequest.
// It may some day be expanded to include metadata about views or indexes.
type InspectResponse struct {
	Fragments []struct {
		Index string
		Field string
		View  string
		Shard int64
		Path  string
		Info  *FragmentInfo
	}
}
