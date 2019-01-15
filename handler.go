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
	var output struct {
		Results        []interface{}    `json:"results,omitempty"`
		ColumnAttrSets []*ColumnAttrSet `json:"columnAttrs,omitempty"`
		Err            string           `json:"error,omitempty"`
	}
	output.Results = resp.Results
	output.ColumnAttrSets = resp.ColumnAttrSets

	if resp.Err != nil {
		output.Err = resp.Err.Error()
	}
	return json.Marshal(output)
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
