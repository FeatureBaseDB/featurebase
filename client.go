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
	"context"
	"io"
)

// Bit represents the intersection of a row and a column. It can be specified by
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
	ColumnID  uint64
	ColumnKey string
	Value     int64
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
	PostSchema(ctx context.Context, uri *URI, s *Schema, remote bool) error
	CreateIndex(ctx context.Context, index string, opt IndexOptions) error
	FragmentNodes(ctx context.Context, index string, shard uint64) ([]*Node, error)
	ShardDistribution(ctx context.Context, index string, maxShard uint64) ([]Node, [][]uint64, error)
	Nodes(ctx context.Context) ([]*Node, error)
	Query(ctx context.Context, index string, queryRequest *QueryRequest) (*QueryResponse, error)
	QueryNode(ctx context.Context, uri *URI, index string, queryRequest *QueryRequest) (*QueryResponse, error)
	Import(ctx context.Context, index, field string, shard uint64, bits []Bit, opts ...ImportOption) error
	ImportK(ctx context.Context, index, field string, bits []Bit, opts ...ImportOption) error
	EnsureIndex(ctx context.Context, name string, options IndexOptions) error
	EnsureField(ctx context.Context, indexName string, fieldName string) error
	EnsureFieldWithOptions(ctx context.Context, index, field string, opt FieldOptions) error
	ImportValue(ctx context.Context, index, field string, shard uint64, vals []FieldValue, opts ...ImportOption) error
	ImportValueK(ctx context.Context, index, field string, vals []FieldValue, opts ...ImportOption) error
	ExportCSV(ctx context.Context, index, field string, shard uint64, w io.Writer) error
	CreateField(ctx context.Context, index, field string) error
	CreateFieldWithOptions(ctx context.Context, index, field string, opt FieldOptions) error
	FragmentBlocks(ctx context.Context, uri *URI, index, field, view string, shard uint64) ([]FragmentBlock, error)
	BlockData(ctx context.Context, uri *URI, index, field, view string, shard uint64, block int) ([]uint64, []uint64, error)
	ColumnAttrDiff(ctx context.Context, uri *URI, index string, blks []AttrBlock) (map[uint64]map[string]interface{}, error)
	RowAttrDiff(ctx context.Context, uri *URI, index, field string, blks []AttrBlock) (map[uint64]map[string]interface{}, error)
	SendMessage(ctx context.Context, uri *URI, msg []byte) error
	RetrieveShardFromURI(ctx context.Context, index, field, view string, shard uint64, uri URI) (io.ReadCloser, error)
	ImportRoaring(ctx context.Context, uri *URI, index, field string, shard uint64, remote bool, req *ImportRoaringRequest) error
}

//===============

// InternalQueryClient is the internal interface for querying a node.
type InternalQueryClient interface {
	QueryNode(ctx context.Context, uri *URI, index string, queryRequest *QueryRequest) (*QueryResponse, error)
}

type nopInternalQueryClient struct{}

func (n *nopInternalQueryClient) QueryNode(ctx context.Context, uri *URI, index string, queryRequest *QueryRequest) (*QueryResponse, error) {
	return nil, nil
}

func newNopInternalQueryClient() *nopInternalQueryClient {
	return &nopInternalQueryClient{}
}

var _ InternalQueryClient = newNopInternalQueryClient()

//===============

type nopInternalClient struct{}

func newNopInternalClient() nopInternalClient {
	return nopInternalClient{}
}

var _ InternalClient = newNopInternalClient()

func (n nopInternalClient) MaxShardByIndex(context.Context) (map[string]uint64, error) {
	return nil, nil
}
func (n nopInternalClient) Schema(ctx context.Context) ([]*IndexInfo, error) { return nil, nil }
func (n nopInternalClient) PostSchema(ctx context.Context, uri *URI, s *Schema, remote bool) error {
	return nil
}

func (n nopInternalClient) CreateIndex(ctx context.Context, index string, opt IndexOptions) error {
	return nil
}
func (n nopInternalClient) FragmentNodes(ctx context.Context, index string, shard uint64) ([]*Node, error) {
	return nil, nil
}
func (n nopInternalClient) ShardDistribution(ctx context.Context, index string, maxShard uint64) ([]Node, [][]uint64, error) {
	return nil, nil, nil
}
func (n nopInternalClient) Nodes(ctx context.Context) ([]*Node, error) {
	return nil, nil
}
func (n nopInternalClient) Query(ctx context.Context, index string, queryRequest *QueryRequest) (*QueryResponse, error) {
	return nil, nil
}
func (n nopInternalClient) QueryNode(ctx context.Context, uri *URI, index string, queryRequest *QueryRequest) (*QueryResponse, error) {
	return nil, nil
}
func (n nopInternalClient) Import(ctx context.Context, index, field string, shard uint64, bits []Bit, opts ...ImportOption) error {
	return nil
}
func (n nopInternalClient) ImportK(ctx context.Context, index, field string, bits []Bit, opts ...ImportOption) error {
	return nil
}
func (n nopInternalClient) ImportRoaring(ctx context.Context, uri *URI, index, field string, shard uint64, remote bool, req *ImportRoaringRequest) error {
	return nil
}
func (n nopInternalClient) EnsureIndex(ctx context.Context, name string, options IndexOptions) error {
	return nil
}
func (n nopInternalClient) EnsureField(ctx context.Context, indexName string, fieldName string) error {
	return nil
}
func (n nopInternalClient) EnsureFieldWithOptions(ctx context.Context, index, field string, opt FieldOptions) error {
	return nil
}
func (n nopInternalClient) ImportValue(ctx context.Context, index, field string, shard uint64, vals []FieldValue, opts ...ImportOption) error {
	return nil
}
func (n nopInternalClient) ImportValueK(ctx context.Context, index, field string, vals []FieldValue, opts ...ImportOption) error {
	return nil
}
func (n nopInternalClient) ExportCSV(ctx context.Context, index, field string, shard uint64, w io.Writer) error {
	return nil
}
func (n nopInternalClient) CreateField(ctx context.Context, index, field string) error { return nil }
func (n nopInternalClient) CreateFieldWithOptions(ctx context.Context, index, field string, opt FieldOptions) error {
	return nil
}
func (n nopInternalClient) FragmentBlocks(ctx context.Context, uri *URI, index, field, view string, shard uint64) ([]FragmentBlock, error) {
	return nil, nil
}
func (n nopInternalClient) BlockData(ctx context.Context, uri *URI, index, field, view string, shard uint64, block int) ([]uint64, []uint64, error) {
	return nil, nil, nil
}
func (n nopInternalClient) ColumnAttrDiff(ctx context.Context, uri *URI, index string, blks []AttrBlock) (map[uint64]map[string]interface{}, error) {
	return nil, nil
}
func (n nopInternalClient) RowAttrDiff(ctx context.Context, uri *URI, index, field string, blks []AttrBlock) (map[uint64]map[string]interface{}, error) {
	return nil, nil
}
func (n nopInternalClient) SendMessage(ctx context.Context, uri *URI, msg []byte) error {
	return nil
}
func (n nopInternalClient) RetrieveShardFromURI(ctx context.Context, index, field, view string, shard uint64, uri URI) (io.ReadCloser, error) {
	return nil, nil
}
