// Copyright 2022 Molecula Corp (DBA FeatureBase). All rights reserved.
package querycontext

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/molecula/featurebase/v3/keys"
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

	// DeleteIndex deletes the specified index. This may imply closing and
	// deleting files. The close requires this to block until outstanding
	// reads against this index complete. Reads that refer to an index that's
	// being deleted may get partial results.
	DeleteIndex(keys.Index) error

	// DeleteField deletes the specified field. As with DeleteIndex, behavior
	// when there are outstdanding reads may result in some reads seeing partial
	// data.
	DeleteField(keys.Index, keys.Field) error

	// DeleteFragments deletes the specified views from the specified shards.
	// The implementation is encouraged to bundle write operations, for instance,
	// deleting all the views from each shard at once.
	DeleteFragments(keys.Index, keys.Field, []keys.View, []keys.Shard) error

	// ListFieldViews requests a list of fields/view pairs represented in a
	// given index/shard. It is only sensical when using indexShardKeySplitter.
	ListFieldViews(keys.Index, keys.Shard) (map[keys.Field][]keys.View, error)

	// Backend gives a textual identifier for the backend, such as "rbf".
	// Currently we only support one, but we have an API for exposing the
	// information and some day we may want another again.
	Backend() string

	// DumpDot dumps a representation of the TxStore's current state in
	// graphviz dot format to the provided writer.
	DumpDot(w io.Writer) error

	// Contents provides a hierarchical map of the TxStore's contents,
	// according to the logical hierarchy of the database rather than
	// whatever internal structure the TxStore uses.
	Contents() (keys.DBContents, error)
}

// TxBackupRestore represents a TxStore that supports converting hunks of its
// database to and from raw byte streams. Not every TxStore is a TxBackupRestore.
type TxBackupRestore interface {
	// Backup provides an io.ReadCloser which contains the data in some format
	// compatible with Restore. For RBF, that would be "just an RBF file."
	// The backup reflects the state of the provided query context, and Backup
	// either closes that QueryContext on error, or returns a readcloser which
	// releases it on Close.
	Backup(QueryContext, keys.Index, keys.Shard) (io.ReadCloser, error)
	// Restore takes a reader containing data compatible with the backend,
	// such as an RBF file, and replaces all the existing data for this index
	// and shard. It does not release or close the QueryContext.
	Restore(QueryContext, keys.Index, keys.Shard, io.Reader) error
}

// verify that rbfTxStore implements this interface
var _ TxStore = &rbfTxStore{}
var _ TxBackupRestore = &rbfTxStore{}

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
	keys(keys.Index, keys.Field, keys.View, keys.Shard) (dbKey, fragKey)

	// dbKey provides just the dbKey half. exercise caution; it's
	// up to you to be sure you're varying this the right ways.
	dbKey(keys.Index, keys.Field, keys.View, keys.Shard) dbKey
	// fragKey provides just the fragKey half. exercise caution; it's
	// up to you to be sure you're varying this the right ways.
	fragKey(keys.Index, keys.Field, keys.View, keys.Shard) fragKey

	// This is used essentially once, in the path where we have an existing
	// RBF file and want to know what it contains. We only support the case
	// where it's field/view right now.
	parseFragKey(fragKey) (keys.Index, keys.Field, keys.View, keys.Shard, error)

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

// Compatibility note: The existing database format uses this as its proposed name for the directory for a database:
// path := dbs.HolderPath + sep + dbs.Index + sep + backendsDir + sep + ty.DirectoryName() + sep + fmt.Sprintf("shard.%04v", dbs.Shard)
// and the field/view keys used in the database are:
// ~field;view<
// we're using these for compatibility.

func (i *indexShardKeySplitter) keys(index keys.Index, field keys.Field, view keys.View, shard keys.Shard) (dbKey, fragKey) {
	return i.dbKey(index, field, view, shard), i.fragKey(index, field, view, shard)
}

func (*indexShardKeySplitter) dbKey(index keys.Index, field keys.Field, view keys.View, shard keys.Shard) dbKey {
	return dbKey(fmt.Sprintf("%s/%08x", index, shard))
}

func (*indexShardKeySplitter) fragKey(index keys.Index, field keys.Field, view keys.View, shard keys.Shard) fragKey {
	return fragKey(fmt.Sprintf("~%s;%s<", field, view))
}

func (*indexShardKeySplitter) parseFragKey(fk fragKey) (keys.Index, keys.Field, keys.View, keys.Shard, error) {
	l := len(fk)
	if l < 3 || fk[0] != '~' || fk[l-1] != '<' {
		return "", "", "", 0, fmt.Errorf("malformed frag key %q", fk)
	}
	semi := strings.IndexByte(string(fk), ';')
	if semi == -1 {
		return "", "", "", 0, fmt.Errorf("malformed frag key %q", fk)
	}
	return "", keys.Field(fk[1:semi]), keys.View(fk[semi+1 : l-1]), 0, nil
}

func (*indexShardKeySplitter) Scope() QueryScope {
	return &indexShardQueryScope{}
}

func (*indexShardKeySplitter) dbPath(dbk dbKey) (string, error) {
	slash := strings.IndexByte(string(dbk), '/')
	if slash == -1 {
		return "", fmt.Errorf("malformed dbKey %q", dbk)
	}
	index := string(dbk)[:slash]
	shardHex := string(dbk)[slash+1:]
	shardNum, err := strconv.ParseInt(shardHex, 16, 64)
	if err != nil {
		return "", fmt.Errorf("invalid hex shard number in dbKey %q", dbk)
	}
	// the %04d here is a compatibility thing with old short_txkey and should
	// probably be fixed.
	paths := [5]string{index, "backends", "rbf", "shard." + fmt.Sprintf("%04d", shardNum)}
	return filepath.Join(paths[:]...), nil
}

// fieldShardKeySplitter splits the database by field,shard pairs.
type fieldShardKeySplitter struct{}

func (f *fieldShardKeySplitter) keys(index keys.Index, field keys.Field, view keys.View, shard keys.Shard) (dbKey, fragKey) {
	return f.dbKey(index, field, view, shard), f.fragKey(index, field, view, shard)
}

func (*fieldShardKeySplitter) dbKey(index keys.Index, field keys.Field, view keys.View, shard keys.Shard) dbKey {
	return dbKey(fmt.Sprintf("%s/%s/%08x", index, field, shard))
}

func (*fieldShardKeySplitter) fragKey(index keys.Index, field keys.Field, view keys.View, shard keys.Shard) fragKey {
	return fragKey(view)
}

func (*fieldShardKeySplitter) parseFragKey(fk fragKey) (keys.Index, keys.Field, keys.View, keys.Shard, error) {
	return "", "", "", 0, errUnimplemented
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
	splitIndexes map[keys.Index]struct{}
}

// NewFlexibleKeySplitter creates a KeySplitter which uses index/shard
// splits by default, but splits things into fields if they're in the
// indexes provided. This design is experimental, and should be considered
// pre-deprecated for production use for now.
func NewFlexibleKeySplitter(indexes ...keys.Index) *flexibleKeySplitter {
	splitIndexes := make(map[keys.Index]struct{}, len(indexes))
	for _, index := range indexes {
		splitIndexes[index] = struct{}{}
	}
	return &flexibleKeySplitter{splitIndexes: splitIndexes}
}

func (f *flexibleKeySplitter) keys(index keys.Index, field keys.Field, view keys.View, shard keys.Shard) (dbKey, fragKey) {
	if _, ok := f.splitIndexes[index]; ok {
		return (&fieldShardKeySplitter{}).keys(index, field, view, shard)
	}
	return (&indexShardKeySplitter{}).keys(index, field, view, shard)
}

func (f *flexibleKeySplitter) dbKey(index keys.Index, field keys.Field, view keys.View, shard keys.Shard) dbKey {
	if _, ok := f.splitIndexes[index]; ok {
		return (&fieldShardKeySplitter{}).dbKey(index, field, view, shard)
	}
	return (&indexShardKeySplitter{}).dbKey(index, field, view, shard)
}

func (f *flexibleKeySplitter) fragKey(index keys.Index, field keys.Field, view keys.View, shard keys.Shard) fragKey {
	if _, ok := f.splitIndexes[index]; ok {
		return (&fieldShardKeySplitter{}).fragKey(index, field, view, shard)
	}
	return (&indexShardKeySplitter{}).fragKey(index, field, view, shard)
}

// unimplemented because we don't want to figure out what the fragkey implies, and
// in any event we only ever get called with the index style ones
func (*flexibleKeySplitter) parseFragKey(fk fragKey) (keys.Index, keys.Field, keys.View, keys.Shard, error) {
	return "", "", "", 0, errUnimplemented
}

func (f *flexibleKeySplitter) dbPath(dbk dbKey) (string, error) {
	slash := strings.IndexByte(string(dbk), '/')
	if slash == -1 {
		return "", fmt.Errorf("malformed dbKey %q", dbk)
	}
	if _, ok := f.splitIndexes[keys.Index(dbk)[:slash]]; ok {
		return (&fieldShardKeySplitter{}).dbPath(dbk)
	}
	return (&indexShardKeySplitter{}).dbPath(dbk)
}

func (f *flexibleKeySplitter) Scope() QueryScope {
	return &flexibleQueryScope{splitter: f}
}

type unimplementedError struct{}

func (unimplementedError) Error() string {
	return "unimplemented"
}

var errUnimplemented unimplementedError

type noValidStoreError struct{}

func (noValidStoreError) Error() string {
	return "no valid TxStore selected"
}

var errNoValidStore noValidStoreError

var _ TxStore = &nopTxStore{}

// nopTxStore is a TxStore that always fails and won't let you do anything.
type nopTxStore struct {
	indexShardKeySplitter
}

var NopTxStore = &nopTxStore{}

// NewQueryContext yields a new query context which is read-only.
func (*nopTxStore) NewQueryContext(context.Context) (QueryContext, error) {
	return nil, errNoValidStore
}

// NewWriteQueryContext yields a new query context which can
// write to the things in the given QueryScope
func (*nopTxStore) NewWriteQueryContext(context.Context, QueryScope) (QueryContext, error) {
	return nil, errNoValidStore
}

func (*nopTxStore) DumpDot(w io.Writer) error {
	return errNoValidStore
}

func (*nopTxStore) Backend() string {
	return "none"
}

// Close attempts to shut down. It can fail if there are still
// open transactions.
func (*nopTxStore) Close() error {
	return nil
}

// DeleteIndex deletes the specified index. This may imply closing and
// deleting files. The close requires this to block until outstanding
// reads against this index complete. Reads that refer to an index that's
// being deleted may get partial results.
func (*nopTxStore) DeleteIndex(keys.Index) error {
	return errNoValidStore
}

// DeleteField deletes the specified field. As with DeleteIndex, behavior
// when there are outstdanding reads may result in some reads seeing partial
// data.
func (*nopTxStore) DeleteField(keys.Index, keys.Field) error {
	return errNoValidStore
}

// DeleteFragments deletes the specified views from the specified shards.
// The implementation is encouraged to bundle write operations, for instance,
// deleting all the views from each shard at once.
func (*nopTxStore) DeleteFragments(keys.Index, keys.Field, []keys.View, []keys.Shard) error {
	return errNoValidStore
}

func (*nopTxStore) ListFieldViews(keys.Index, keys.Shard) (map[keys.Field][]keys.View, error) {
	return nil, errNoValidStore
}

func (*nopTxStore) Contents() (keys.DBContents, error) {
	return nil, errNoValidStore
}
