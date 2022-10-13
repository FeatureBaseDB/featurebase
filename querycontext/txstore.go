// Copyright 2022 Molecula Corp (DBA FeatureBase). All rights reserved.
package querycontext

import (
	"context"
	"fmt"
)

// TxStore represents a transactional database backend, mapping
// {index,field,view,shard} tuples to combinations of specific on-disk
// databases and keys to use with them to find related data.
//
// Each TxStore implements the KeySplitter interface, possibly by
// having a KeySplitter embedded in it. The KeySplitter used with the
// TxStore determines which database to use (the database key) and
// which part of that database to use (the fragment key) to access
// a given fragment.
type TxStore interface {
	KeySplitter

	// NewQueryContext yields a new query context which is read-only.
	NewQueryContext(context.Context) (QueryContext, error)

	// NewWriteQueryContext yields a new query context which can
	// write to the things in the given QueryScope
	NewWriteQueryContext(context.Context, QueryScope) (QueryContext, error)

	// Close attempts to shut down. It can fail if there are still
	// open transactions.
	Close() error
}

// verify that rbfTxStore implements this interface
var _ TxStore = &rbfTxStore{}

// dbKey is an identifier which can distinguish backend databases.
type dbKey string

// Path yields a relative path corresponding to a dbKey. For example
// this could be something like `indexes/i/shards/0`. The path is
// a relative path, presumably to be combined with some root path for
// a txStore
func (d dbKey) Path() string {
	// for now, let's just use the path as the dbkey
	return string(d)
}

// fragKey is an identifier which can be used to tell a backend database
// which data to operate on.
type fragKey string

// KeySplitter knows how to convert fragment identifiers
// (index/field/view/shard) into database backends. This is handled
// by the unexported `keys` method. The exported method, Scope,
// produces a QueryScope which follows corresponding rules.
//
// Imagine that you have two QueryScopes A and B from the same TxStore,
// and two fragment identifiers such that A.Allowed(i1, f1, v1, s1)
// and B.Allowed(i2, f2, v2, s2) are both true. If the keys method
// produces the same database key for these two fragment identifiers,
// then the two query scopes are said to overlap. It doesn't matter
// whether the whole fragment identifiers are identical.
type KeySplitter interface {
	// keys yields two strings that between them denote the
	// index/field/view/shard provided. When two sets of parameters
	// yield the same dbKey result, that means that they share a
	// backing database, and thus that they must share a backing
	// database transaction. The fragKey result is used by operations
	// within the database backend.
	keys(IndexName, FieldName, ViewName, ShardID) (dbKey, fragKey)

	// Scope() yields a new scope which is aware of this KeySplitter
	// and will give correct results for Overlap calls.
	Scope() QueryScope
}

var _ KeySplitter = &indexShardKeySplitter{}
var _ KeySplitter = &fieldShardKeySplitter{}
var _ KeySplitter = &flexibleKeySplitter{}

// indexShardKeySplitter splits the database by index,shard pairs
// (the default for RBF).
type indexShardKeySplitter struct{}

func (*indexShardKeySplitter) keys(index IndexName, field FieldName, view ViewName, shard ShardID) (dbKey, fragKey) {
	d := dbKey(fmt.Sprintf("%s/%012x", index, shard))
	t := fragKey(fmt.Sprintf("%s:%s", field, view))
	return d, t
}

func (*indexShardKeySplitter) Scope() QueryScope {
	return &indexShardQueryScope{}
}

// fieldShardKeySplitter splits the database by field,shard pairs.
type fieldShardKeySplitter struct{}

func (*fieldShardKeySplitter) keys(index IndexName, field FieldName, view ViewName, shard ShardID) (dbKey, fragKey) {
	d := dbKey(fmt.Sprintf("%s/%s/%012x", index, field, shard))
	t := fragKey(view)
	return d, t
}

func (*fieldShardKeySplitter) Scope() QueryScope {
	// a flexibleQueryScope without a flexibleKeySplitter always
	// splits keys by field.
	return &flexibleQueryScope{}
}

// flexibleKeySplitter splits the database into index,shard pairs,
// except for a subset of indexes which get split by field. this
// exists primarily to explore the API space. Do not alter the splitIndexes
// map after initial creation, it will produce inconsistent results.
type flexibleKeySplitter struct {
	splitIndexes map[IndexName]struct{}
}

func NewFlexibleKeySplitter(indexes ...IndexName) *flexibleKeySplitter {
	splitIndexes := make(map[IndexName]struct{}, len(indexes))
	for _, index := range indexes {
		splitIndexes[index] = struct{}{}
	}
	return &flexibleKeySplitter{splitIndexes: splitIndexes}
}

func (f *flexibleKeySplitter) keys(index IndexName, field FieldName, view ViewName, shard ShardID) (dbKey, fragKey) {
	if _, ok := f.splitIndexes[index]; ok {
		return (&fieldShardKeySplitter{}).keys(index, field, view, shard)
	}
	return (&indexShardKeySplitter{}).keys(index, field, view, shard)
}

func (f *flexibleKeySplitter) Scope() QueryScope {
	return &flexibleQueryScope{splitter: f}
}
