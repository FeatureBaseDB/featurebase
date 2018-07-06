package pilosa

import (
	"context"
	"io"
)

// Bit represents the intersection of a row and a column. It can be specifed by
// integer ids or string keys.
type Bit struct {
	RowID     uint64
	ColumnID  uint64
	RowKey    string
	ColumnKey string
	Timestamp int64
}

// FieldValue represents the value for a column within a
// range-encoded field.
type FieldValue struct {
	ColumnID uint64
	Value    int64
}

// InternalClient should be implemented by any struct that enables any transport between nodes
// TODO: Refactor
// Note from Travis: Typically an interface containing more than two or three methods is an indication that
// something hasn't been architected correctly.
// While I understand that putting the entire Client behind an interface might require this many methods,
// I don't want to let it go unquestioned.
type InternalClient interface {
	MaxShardByIndex(ctx context.Context) (map[string]uint64, error)
	Schema(ctx context.Context) ([]*IndexInfo, error)
	CreateIndex(ctx context.Context, index string, opt IndexOptions) error
	FragmentNodes(ctx context.Context, index string, shard uint64) ([]*Node, error)
	Query(ctx context.Context, index string, queryRequest *QueryRequest) (*QueryResponse, error)
	QueryNode(ctx context.Context, uri *URI, index string, queryRequest *QueryRequest) (*QueryResponse, error)
	Import(ctx context.Context, index, field string, shard uint64, bits []Bit) error
	ImportK(ctx context.Context, index, field string, bits []Bit) error
	EnsureIndex(ctx context.Context, name string, options IndexOptions) error
	EnsureField(ctx context.Context, indexName string, fieldName string) error
	ImportValue(ctx context.Context, index, field string, shard uint64, vals []FieldValue) error
	ExportCSV(ctx context.Context, index, field string, shard uint64, w io.Writer) error
	CreateField(ctx context.Context, index, field string) error
	FragmentBlocks(ctx context.Context, uri *URI, index, field string, shard uint64) ([]FragmentBlock, error)
	BlockData(ctx context.Context, uri *URI, index, field string, shard uint64, block int) ([]uint64, []uint64, error)
	ColumnAttrDiff(ctx context.Context, uri *URI, index string, blks []AttrBlock) (map[uint64]map[string]interface{}, error)
	RowAttrDiff(ctx context.Context, uri *URI, index, field string, blks []AttrBlock) (map[uint64]map[string]interface{}, error)
	SendMessage(ctx context.Context, uri *URI, msg []byte) error
	RetrieveShardFromURI(ctx context.Context, index, field string, shard uint64, uri URI) (io.ReadCloser, error)
}

//===============

type InternalQueryClient interface {
	QueryNode(ctx context.Context, uri *URI, index string, queryRequest *QueryRequest) (*QueryResponse, error)
}

type NopInternalQueryClient struct{}

func (n *NopInternalQueryClient) QueryNode(ctx context.Context, uri *URI, index string, queryRequest *QueryRequest) (*QueryResponse, error) {
	return nil, nil
}

func newNopInternalQueryClient() *NopInternalQueryClient {
	return &NopInternalQueryClient{}
}

var _ InternalQueryClient = newNopInternalQueryClient()

//===============

type NopInternalClient struct{}

func newNopInternalClient() NopInternalClient {
	return NopInternalClient{}
}

var _ InternalClient = newNopInternalClient()

func (n NopInternalClient) MaxShardByIndex(context.Context) (map[string]uint64, error) {
	return nil, nil
}
func (n NopInternalClient) Schema(ctx context.Context) ([]*IndexInfo, error) { return nil, nil }
func (n NopInternalClient) CreateIndex(ctx context.Context, index string, opt IndexOptions) error {
	return nil
}
func (n NopInternalClient) FragmentNodes(ctx context.Context, index string, shard uint64) ([]*Node, error) {
	return nil, nil
}
func (n NopInternalClient) Query(ctx context.Context, index string, queryRequest *QueryRequest) (*QueryResponse, error) {
	return nil, nil
}
func (n NopInternalClient) QueryNode(ctx context.Context, uri *URI, index string, queryRequest *QueryRequest) (*QueryResponse, error) {
	return nil, nil
}
func (n NopInternalClient) Import(ctx context.Context, index, field string, shard uint64, bits []Bit) error {
	return nil
}
func (n NopInternalClient) ImportK(ctx context.Context, index, field string, bits []Bit) error {
	return nil
}
func (n NopInternalClient) EnsureIndex(ctx context.Context, name string, options IndexOptions) error {
	return nil
}
func (n NopInternalClient) EnsureField(ctx context.Context, indexName string, fieldName string) error {
	return nil
}
func (n NopInternalClient) ImportValue(ctx context.Context, index, field string, shard uint64, vals []FieldValue) error {
	return nil
}
func (n NopInternalClient) ExportCSV(ctx context.Context, index, field string, shard uint64, w io.Writer) error {
	return nil
}
func (n NopInternalClient) CreateField(ctx context.Context, index, field string) error { return nil }
func (n NopInternalClient) FragmentBlocks(ctx context.Context, uri *URI, index, field string, shard uint64) ([]FragmentBlock, error) {
	return nil, nil
}
func (n NopInternalClient) BlockData(ctx context.Context, uri *URI, index, field string, shard uint64, block int) ([]uint64, []uint64, error) {
	return nil, nil, nil
}
func (n NopInternalClient) ColumnAttrDiff(ctx context.Context, uri *URI, index string, blks []AttrBlock) (map[uint64]map[string]interface{}, error) {
	return nil, nil
}
func (n NopInternalClient) RowAttrDiff(ctx context.Context, uri *URI, index, field string, blks []AttrBlock) (map[uint64]map[string]interface{}, error) {
	return nil, nil
}
func (n NopInternalClient) SendMessage(ctx context.Context, uri *URI, msg []byte) error {
	return nil
}
func (n NopInternalClient) RetrieveShardFromURI(ctx context.Context, index, field string, shard uint64, uri URI) (io.ReadCloser, error) {
	return nil, nil
}
