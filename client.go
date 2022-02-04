// Copyright 2021 Molecula Corp. All rights reserved.
package pilosa

import (
	"context"

	pnet "github.com/molecula/featurebase/v3/net"
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
