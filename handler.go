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
	}{
		Results:        resp.Results,
		ColumnAttrSets: resp.ColumnAttrSets,
	})
}

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

var NopHandler Handler = nopHandler{}

type ImportValueRequest struct {
	Index      string
	Field      string
	Shard      uint64
	ColumnIDs  []uint64
	ColumnKeys []string
	Values     []int64
}

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

type ImportRoaringRequest struct {
	Clear bool
	Views map[string][]byte
}

type ImportResponse struct {
	Err string
}

type BlockDataRequest struct {
	Index string
	Field string
	View  string
	Shard uint64
	Block uint64
}

type BlockDataResponse struct {
	RowIDs    []uint64
	ColumnIDs []uint64
}

type TranslateKeysRequest struct {
	Index string
	Field string
	Keys  []string
}

type TranslateKeysResponse struct {
	IDs []uint64
}
