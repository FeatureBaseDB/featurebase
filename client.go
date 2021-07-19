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
	"time"

	pnet "github.com/molecula/featurebase/v2/net"
	"github.com/molecula/featurebase/v2/topology"
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
// Another note from Travis: I think we eventually want to unify `InternalClient` with
// the `github.com/molecula/featurebase/v2/client` client.
// Doing that may obviate the need to refactor this.
type InternalClient interface {
	InternalQueryClient

	AvailableShards(ctx context.Context, indexName string) ([]uint64, error)
	MaxShardByIndex(ctx context.Context) (map[string]uint64, error)
	Schema(ctx context.Context) ([]*IndexInfo, error)
	PostSchema(ctx context.Context, uri *pnet.URI, s *Schema, remote bool) error
	CreateIndex(ctx context.Context, index string, opt IndexOptions) error
	FragmentNodes(ctx context.Context, index string, shard uint64) ([]*topology.Node, error)
	PartitionNodes(ctx context.Context, partitionID int) ([]*topology.Node, error)
	Nodes(ctx context.Context) ([]*topology.Node, error)
	Query(ctx context.Context, index string, queryRequest *QueryRequest) (*QueryResponse, error)
	Import(ctx context.Context, index, field string, shard uint64, bits []Bit, opts ...ImportOption) error
	ImportK(ctx context.Context, index, field string, bits []Bit, opts ...ImportOption) error
	EnsureIndex(ctx context.Context, name string, options IndexOptions) error
	EnsureField(ctx context.Context, indexName string, fieldName string) error
	EnsureFieldWithOptions(ctx context.Context, index, field string, opt FieldOptions) error
	ImportValue(ctx context.Context, index, field string, shard uint64, vals []FieldValue, opts ...ImportOption) error
	ImportValueK(ctx context.Context, index, field string, vals []FieldValue, opts ...ImportOption) error
	ImportValue2(ctx context.Context, req *ImportValueRequest, options *ImportOptions) error
	ExportCSV(ctx context.Context, index, field string, shard uint64, w io.Writer) error
	CreateField(ctx context.Context, index, field string) error
	CreateFieldWithOptions(ctx context.Context, index, field string, opt FieldOptions) error
	FragmentBlocks(ctx context.Context, uri *pnet.URI, index, field, view string, shard uint64) ([]FragmentBlock, error)
	BlockData(ctx context.Context, uri *pnet.URI, index, field, view string, shard uint64, block int) ([]uint64, []uint64, error)
	SendMessage(ctx context.Context, uri *pnet.URI, msg []byte) error
	RetrieveShardFromURI(ctx context.Context, index, field, view string, shard uint64, uri pnet.URI) (io.ReadCloser, error)
	RetrieveTranslatePartitionFromURI(ctx context.Context, index string, partition int, uri pnet.URI) (io.ReadCloser, error)
	ImportRoaring(ctx context.Context, uri *pnet.URI, index, field string, shard uint64, remote bool, req *ImportRoaringRequest) error
	ShardReader(ctx context.Context, index string, shard uint64) (io.ReadCloser, error)

	IDAllocDataReader(ctx context.Context) (io.ReadCloser, error)
	IndexTranslateDataReader(ctx context.Context, index string, partitionID int) (io.ReadCloser, error)
	FieldTranslateDataReader(ctx context.Context, index, field string) (io.ReadCloser, error)

	StartTransaction(ctx context.Context, id string, timeout time.Duration, exclusive bool) (*Transaction, error)
	FinishTransaction(ctx context.Context, id string) (*Transaction, error)
	Transactions(ctx context.Context) (map[string]*Transaction, error)
	GetTransaction(ctx context.Context, id string) (*Transaction, error)

	GetNodeUsage(ctx context.Context, uri *pnet.URI) (map[string]NodeUsage, error)
	GetPastQueries(ctx context.Context, uri *pnet.URI) ([]PastQueryStatus, error)

	ImportFieldKeys(ctx context.Context, uri *pnet.URI, index, field string, remote bool, rddbdata io.Reader) error
	ImportIndexKeys(ctx context.Context, uri *pnet.URI, index string, partitionID int, remote bool, rddbdata io.Reader) error
}

//===============

// InternalQueryClient is the internal interface for querying a node.
type InternalQueryClient interface {
	SchemaNode(ctx context.Context, uri *pnet.URI, views bool) ([]*IndexInfo, error)

	QueryNode(ctx context.Context, uri *pnet.URI, index string, queryRequest *QueryRequest) (*QueryResponse, error)

	// Trasnlate keys on the particular node. The parameter writable informs TranslateStore if we can generate a new ID if any of keys does not exist.
	TranslateKeysNode(ctx context.Context, uri *pnet.URI, index, field string, keys []string, writable bool) ([]uint64, error)
	TranslateIDsNode(ctx context.Context, uri *pnet.URI, index, field string, id []uint64) ([]string, error)

	FindIndexKeysNode(ctx context.Context, uri *pnet.URI, index string, keys ...string) (map[string]uint64, error)
	FindFieldKeysNode(ctx context.Context, uri *pnet.URI, index string, field string, keys ...string) (map[string]uint64, error)

	CreateIndexKeysNode(ctx context.Context, uri *pnet.URI, index string, keys ...string) (map[string]uint64, error)
	CreateFieldKeysNode(ctx context.Context, uri *pnet.URI, index string, field string, keys ...string) (map[string]uint64, error)

	MatchFieldKeysNode(ctx context.Context, uri *pnet.URI, index string, field string, like string) ([]uint64, error)
}

type nopInternalQueryClient struct{}

func (nopInternalQueryClient) SchemaNode(ctx context.Context, uri *pnet.URI, views bool) ([]*IndexInfo, error) {
	return nil, nil
}

func (n nopInternalQueryClient) QueryNode(ctx context.Context, uri *pnet.URI, index string, queryRequest *QueryRequest) (*QueryResponse, error) {
	return nil, nil
}

func (n nopInternalQueryClient) TranslateKeysNode(ctx context.Context, uri *pnet.URI, index, field string, keys []string, writable bool) ([]uint64, error) {
	return nil, nil
}

func (n nopInternalQueryClient) TranslateIDsNode(ctx context.Context, uri *pnet.URI, index, field string, ids []uint64) ([]string, error) {
	return nil, nil
}

func (n nopInternalQueryClient) FindIndexKeysNode(ctx context.Context, uri *pnet.URI, index string, keys ...string) (map[string]uint64, error) {
	return nil, nil
}

func (n nopInternalQueryClient) FindFieldKeysNode(ctx context.Context, uri *pnet.URI, index string, field string, keys ...string) (map[string]uint64, error) {
	return nil, nil
}

func (n nopInternalQueryClient) CreateIndexKeysNode(ctx context.Context, uri *pnet.URI, index string, keys ...string) (map[string]uint64, error) {
	return nil, nil
}

func (n nopInternalQueryClient) CreateFieldKeysNode(ctx context.Context, uri *pnet.URI, index string, field string, keys ...string) (map[string]uint64, error) {
	return nil, nil
}

func (n nopInternalQueryClient) MatchFieldKeysNode(ctx context.Context, uri *pnet.URI, index string, field string, like string) ([]uint64, error) {
	return nil, nil
}

func newNopInternalQueryClient() nopInternalQueryClient {
	return nopInternalQueryClient{}
}

var _ InternalQueryClient = newNopInternalQueryClient()

//===============

type nopInternalClient struct{ nopInternalQueryClient }

func newNopInternalClient() nopInternalClient {
	return nopInternalClient{}
}

var _ InternalClient = newNopInternalClient()

func (n nopInternalClient) AvailableShards(ctx context.Context, indexName string) ([]uint64, error) {
	return nil, nil
}

func (n nopInternalClient) MaxShardByIndex(context.Context) (map[string]uint64, error) {
	return nil, nil
}
func (n nopInternalClient) Schema(ctx context.Context) ([]*IndexInfo, error) { return nil, nil }
func (n nopInternalClient) PostSchema(ctx context.Context, uri *pnet.URI, s *Schema, remote bool) error {
	return nil
}

func (n nopInternalClient) CreateIndex(ctx context.Context, index string, opt IndexOptions) error {
	return nil
}
func (n nopInternalClient) FragmentNodes(ctx context.Context, index string, shard uint64) ([]*topology.Node, error) {
	return nil, nil
}
func (n nopInternalClient) PartitionNodes(ctx context.Context, partitionID int) ([]*topology.Node, error) {
	return nil, nil
}
func (n nopInternalClient) Nodes(ctx context.Context) ([]*topology.Node, error) {
	return nil, nil
}
func (n nopInternalClient) Query(ctx context.Context, index string, queryRequest *QueryRequest) (*QueryResponse, error) {
	return nil, nil
}
func (n nopInternalClient) Import(ctx context.Context, index, field string, shard uint64, bits []Bit, opts ...ImportOption) error {
	return nil
}
func (n nopInternalClient) ImportK(ctx context.Context, index, field string, bits []Bit, opts ...ImportOption) error {
	return nil
}
func (n nopInternalClient) ImportValue2(ctx context.Context, req *ImportValueRequest, options *ImportOptions) error {
	return nil
}

func (n nopInternalClient) ImportRoaring(ctx context.Context, uri *pnet.URI, index, field string, shard uint64, remote bool, req *ImportRoaringRequest) error {
	return nil
}

func (n nopInternalClient) ShardReader(ctx context.Context, index string, shard uint64) (io.ReadCloser, error) {
	return nil, nil
}

func (n nopInternalClient) IDAllocDataReader(ctx context.Context) (io.ReadCloser, error) {
	return nil, nil
}

func (n nopInternalClient) IndexTranslateDataReader(ctx context.Context, index string, partitionID int) (io.ReadCloser, error) {
	return nil, nil
}

func (n nopInternalClient) FieldTranslateDataReader(ctx context.Context, index, field string) (io.ReadCloser, error) {
	return nil, nil
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
func (n nopInternalClient) FragmentBlocks(ctx context.Context, uri *pnet.URI, index, field, view string, shard uint64) ([]FragmentBlock, error) {
	return nil, nil
}
func (n nopInternalClient) BlockData(ctx context.Context, uri *pnet.URI, index, field, view string, shard uint64, block int) ([]uint64, []uint64, error) {
	return nil, nil, nil
}
func (n nopInternalClient) SendMessage(ctx context.Context, uri *pnet.URI, msg []byte) error {
	return nil
}
func (n nopInternalClient) RetrieveShardFromURI(ctx context.Context, index, field, view string, shard uint64, uri pnet.URI) (io.ReadCloser, error) {
	return nil, nil
}
func (n nopInternalClient) RetrieveTranslatePartitionFromURI(ctx context.Context, index string, partition int, uri pnet.URI) (io.ReadCloser, error) {
	return nil, nil
}

func (n nopInternalClient) StartTransaction(ctx context.Context, id string, timeout time.Duration, exclusive bool) (*Transaction, error) {
	return nil, nil
}
func (n nopInternalClient) FinishTransaction(ctx context.Context, id string) (*Transaction, error) {
	return nil, nil
}
func (n nopInternalClient) Transactions(ctx context.Context) (map[string]*Transaction, error) {
	return nil, nil
}
func (n nopInternalClient) GetTransaction(ctx context.Context, id string) (*Transaction, error) {
	return nil, nil
}

func (n nopInternalClient) GetNodeUsage(ctx context.Context, uri *pnet.URI) (map[string]NodeUsage, error) {
	return nil, nil
}

func (n nopInternalClient) GetPastQueries(ctx context.Context, uri *pnet.URI) ([]PastQueryStatus, error) {
	return nil, nil
}
func (c nopInternalClient) ImportFieldKeys(ctx context.Context, uri *pnet.URI, index, field string, remote bool, rddbdata io.Reader) error {
	return nil
}

func (c nopInternalClient) ImportIndexKeys(ctx context.Context, uri *pnet.URI, index string, partitionID int, remote bool, rddbdata io.Reader) error {
	return nil
}
