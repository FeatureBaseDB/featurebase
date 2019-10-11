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

	"github.com/pilosa/pilosa/v2/tracing"
)

// QueryRequest represent a request to process a query.
type QueryRequest struct {
	// Index to execute query against.
	Index string

	// The query string to parse and execute.
	Query string

	// The shards to include in the query execution.
	// If empty, all shards are included.
	Shards []uint64

	// Return column attributes, if true.
	ColumnAttrs bool

	// Do not return row attributes, if true.
	ExcludeRowAttrs bool

	// Do not return columns, if true.
	ExcludeColumns bool

	// If true, indicates that query is part of a larger distributed query.
	// If false, this request is on the originating node.
	Remote bool

	// Should we profile this query?
	Profile bool
}

// QueryResponse represent a response from a processed query.
type QueryResponse struct {
	// Result for each top-level query call.
	// Can be a Bitmap, Pairs, or uint64.
	Results []interface{}

	// Set of column attribute objects matching IDs returned in Result.
	ColumnAttrSets []*ColumnAttrSet

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
		Results        []interface{}    `json:"results"`
		ColumnAttrSets []*ColumnAttrSet `json:"columnAttrs,omitempty"`
		Profile        *tracing.Profile `json:"profile,omitempty"`
	}{
		Results:        resp.Results,
		ColumnAttrSets: resp.ColumnAttrSets,
		Profile:        resp.Profile,
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
type ImportValueRequest struct {
	Index      string
	Field      string
	Shard      uint64
	ColumnIDs  []uint64
	ColumnKeys []string
	Values     []int64
}

// ImportRequest describes the import request structure
// for an import.
type ImportRequest struct {
	Index      string
	Field      string
	Shard      uint64
	RowIDs     []uint64
	ColumnIDs  []uint64
	RowKeys    []string
	ColumnKeys []string
	Timestamps []int64
}

// ImportRoaringRequest describes the import request structure
// for an import containing roaring-encoded data.
type ImportRoaringRequest struct {
	Clear bool
	Views map[string][]byte
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
}

// TranslateKeysResponse is the structured response of a key
// translation request.
type TranslateKeysResponse struct {
	IDs []uint64
}
