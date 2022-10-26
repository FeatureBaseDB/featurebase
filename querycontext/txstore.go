// Copyright 2022 Molecula Corp (DBA FeatureBase). All rights reserved.
package querycontext

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
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

	// dbPath yields a filesystem-friendly string that corresponds to dbKey.
	// possibly it is identical to dbKey, but you might want a terse dbKey
	// like "i/0" and a longer path like "indexes/i/shards/0".
	dbPath(dbKey) (string, error)

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
	d := dbKey(fmt.Sprintf("%s/%08x", index, shard))
	t := fragKey(fmt.Sprintf("%s:%s", field, view))
	return d, t
}

func (*indexShardKeySplitter) Scope() QueryScope {
	return &indexShardQueryScope{}
}

func (*indexShardKeySplitter) dbPath(dbk dbKey) (string, error) {
	slash := strings.IndexByte(string(dbk), '/')
	if slash == -1 {
		return "", fmt.Errorf("malformed dbKey %q", dbk)
	}
	paths := [4]string{"indexes", "", "shards", ""}
	paths[1] = string(dbk)[:slash]
	paths[3] = string(dbk)[slash+1:]
	return filepath.Join(paths[:]...), nil
}

// fieldShardKeySplitter splits the database by field,shard pairs.
type fieldShardKeySplitter struct{}

func (*fieldShardKeySplitter) keys(index IndexName, field FieldName, view ViewName, shard ShardID) (dbKey, fragKey) {
	d := dbKey(fmt.Sprintf("%s/%s/%08x", index, field, shard))
	t := fragKey(view)
	return d, t
}

func (*fieldShardKeySplitter) dbPath(dbk dbKey) (string, error) {
	slash := strings.IndexByte(string(dbk), '/')
	if slash == -1 {
		return "", fmt.Errorf("malformed dbKey %q", dbk)
	}
	paths := [6]string{"indexes", "", "fields", "", "shards", ""}
	paths[1] = string(dbk)[:slash]
	paths[3] = string(dbk)[slash+1:]
	slash = strings.IndexByte(paths[3], '/')
	if slash == -1 {
		return "", fmt.Errorf("malformed dbKey %q", dbk)
	}
	// note order: have to grab the tail of paths[3] before cutting it off
	paths[5] = paths[3][slash+1:]
	paths[3] = paths[3][:slash]
	return filepath.Join(paths[:]...), nil
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

// NewFlexibleKeySplitter creates a KeySplitter which uses index/shard
// splits by default, but splits things into fields if they're in the
// indexes provided. This design is experimental, and should be considered
// pre-deprecated for production use for now.
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

func (f *flexibleKeySplitter) dbPath(dbk dbKey) (string, error) {
	slash := strings.IndexByte(string(dbk), '/')
	if slash == -1 {
		return "", fmt.Errorf("malformed dbKey %q", dbk)
	}
	if _, ok := f.splitIndexes[IndexName(dbk)[:slash]]; ok {
		return (&fieldShardKeySplitter{}).dbPath(dbk)
	}
	return (&indexShardKeySplitter{}).dbPath(dbk)
}

func (f *flexibleKeySplitter) Scope() QueryScope {
	return &flexibleQueryScope{splitter: f}
}
