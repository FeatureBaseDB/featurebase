// Copyright 2022 Molecula Corp (DBA FeatureBase). All rights reserved.
package querycontext

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"

	"github.com/featurebasedb/featurebase/v3/rbf"
	rbfcfg "github.com/featurebasedb/featurebase/v3/rbf/cfg"
	"github.com/featurebasedb/featurebase/v3/roaring"
)

// rbfDBQueryContexts represents an actual backend DB, and a map of the
// rbfQueryContexts associated with that DB. this lets us check for
// outstanding transactions before closing a database.
type rbfDBQueryContexts struct {
	key    dbKey
	dbPath string // dbPath relative to root
	db     *rbf.DB
	mu     sync.Mutex
	// mutex used to pretend we're using the database even though we're
	// not wired up to it yet
	lockCheck sync.Mutex
	// currently live TxWrappers, pointing to their QueryContexts
	queryContexts map[*rbfTxWrappers]*rbfQueryContext
}

// writeTx obtains a write Tx, and associates it with the given query context. only
// one thing should ever have a write context at once, and because the QueryContext is
// supposed to be protecting us here, it's actually just plain an error for us
// to not get the lock inside here.
func (r *rbfDBQueryContexts) writeTx(rq *rbfQueryContext) (*rbfTxWrappers, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ok := r.lockCheck.TryLock()
	if !ok {
		return nil, errors.New("locking error: tried for write Tx when it was already held")
	}
	tx, err := r.db.Begin(true)
	if err != nil {
		return nil, err
	}
	q := &rbfTxWrappers{db: r, tx: tx, key: r.key, queries: make(map[fragKey]QueryRead), writeTx: true}
	r.queryContexts[q] = rq
	return q, nil
}

// readTx obtains a read Tx, and associates it with the given query context.
func (r *rbfDBQueryContexts) readTx(rq *rbfQueryContext) (*rbfTxWrappers, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	tx, err := r.db.Begin(false)
	if err != nil {
		return nil, err
	}
	q := &rbfTxWrappers{db: r, tx: tx, key: r.key, queries: make(map[fragKey]QueryRead)}
	r.queryContexts[q] = rq
	return q, nil
}

// release marks a given rbfQueryContext as no longer using the db
func (r *rbfDBQueryContexts) release(rt *rbfTxWrappers) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if rt.tx != nil {
		rt.tx.Rollback()
	}
	if rt.writeTx {
		r.lockCheck.Unlock()
	}
	delete(r.queryContexts, rt)
}

// commit commits the transaction associated with the query context, and
// returns any error from the commit.
func (r *rbfDBQueryContexts) commit(rt *rbfTxWrappers) (err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if rt.tx != nil {
		err = rt.tx.Commit()
	}
	r.lockCheck.Unlock()
	delete(r.queryContexts, rt)
	return err
}

// rbfTxStore is the implementation of TxStore which is backed by RBF
// files.
//
// an rbfTxStore maintains a map from dbKeys to rbfDBQueryContexts, which
// is to say, for each dbKey, it has an associated DB and a pool of
// rbfQueryContexts that are using that DB.
//
// it also maintains a list of live writers, which is a map from
// rbfQueryContexts to their QueryScope, used to determine whether a
// proposed new write will overlap with any existing writes, in which
// case it will be paused until they're done.
type rbfTxStore struct {
	KeySplitter
	mu         sync.Mutex
	writeQueue *sync.Cond
	rootPath   string
	// currently live databases, and the query contexts (if any) using them
	dbs         map[dbKey]*rbfDBQueryContexts
	writeScopes map[*rbfQueryContext]QueryScope
	queries     map[*rbfQueryContext]struct{}
	cfg         *rbfcfg.Config
	closed      bool
}

// NewRBFTxStore creates a new RBF-backed TxStore in the given directory. If
// cfg is nil, it will use a `NewDefaultConfig`. All databases will be opened
// using the same config. If splitter is nil, it uses an index/shard splitter.
//
// With the index/shard key splitter, database directory paths look like
// `path/indexes/i/shards/00000000`, with each shard directory containing
// data/wal files.
func NewRBFTxStore(path string, cfg *rbfcfg.Config, splitter KeySplitter) (*rbfTxStore, error) {
	if cfg == nil {
		cfg = rbfcfg.NewDefaultConfig()
	}
	if splitter == nil {
		splitter = &indexShardKeySplitter{}
	}
	r := &rbfTxStore{
		KeySplitter: splitter,
		rootPath:    path,
		dbs:         make(map[dbKey]*rbfDBQueryContexts),
		writeScopes: make(map[*rbfQueryContext]QueryScope),
		queries:     make(map[*rbfQueryContext]struct{}),
		cfg:         cfg,
	}
	r.writeQueue = sync.NewCond(&r.mu)
	return r, nil
}

// getDB gets or creates the db for the given key. call only when you hold
// the rbfTxStore's lock.
func (r *rbfTxStore) getDB(dbk dbKey) (*rbfDBQueryContexts, error) {
	db, ok := r.dbs[dbk]
	if ok {
		return db, nil
	}
	dbPath, err := r.dbPath(dbk)
	if err != nil {
		return nil, err
	}
	db = &rbfDBQueryContexts{key: dbk, dbPath: dbPath, queryContexts: make(map[*rbfTxWrappers]*rbfQueryContext)}
	path := filepath.Join(r.rootPath, db.dbPath)
	// note: we don't need to create the directory here, because rbf.Open
	// creates the directory for us. I was slightly surprised by this and
	// I'm not sure I like it. Note also that the database path is actually
	// a directory containing files named "data" and "wal".
	db.db = rbf.NewDB(path, r.cfg)
	err = db.db.Open()
	if err != nil {
		return nil, err
	}
	r.dbs[dbk] = db
	return db, nil
}

// newReadTx creates a read Tx in the backend, and returns an rbfTxWrappers
// with that plus a map from fragKey to QueryRead objects
func (r *rbfTxStore) newReadTx(rq *rbfQueryContext, dbk dbKey) (*rbfTxWrappers, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	db, err := r.getDB(dbk)
	if err != nil {
		return nil, err
	}
	return db.readTx(rq)
}

// newWriteTx creates a write Tx in the backend, and returns an rbfTxWrappers
// with that plus a map from fragKey to QueryRead objects
func (r *rbfTxStore) newWriteTx(rq *rbfQueryContext, dbk dbKey) (*rbfTxWrappers, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	db, err := r.getDB(dbk)
	if err != nil {
		return nil, err
	}
	return db.writeTx(rq)
}

// writeBlocked checks whether the new write could be blocked by an existing write
// which is already in the list of writers
func (r *rbfTxStore) writeBlocked(scope QueryScope) bool {
	for _, writer := range r.writeScopes {
		if writer.Overlap(scope) {
			return true
		}
	}
	return false
}

// release indicates that the given rbfQueryContext is no longer using this txStore.
// in particular, if it's a write query context, this indicates that it is no
// longer holding any locks, and it may be possible for other writes to continue.
func (r *rbfTxStore) release(rq *rbfQueryContext) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.writeScopes[rq]; ok {
		delete(r.writeScopes, rq)
		// we broadcast, rather than signaling, because it's totally possible that
		// there's still other active writers, and that any *specific* pending write
		// could be blocked by one of those. it's possible that nothing will actually
		// be able to continue yet. but in the typical case, there aren't a huge
		// number waiting, we hope? if writeBlocked checks are showing up a lot, we
		// can revisit this.
		r.writeQueue.Broadcast()
	}
	delete(r.queries, rq)
}

// NewQueryContext creates a read-only query context. It doesn't need to worry
// about writes so it just assumes it can request read access to any database it
// needs.
func (r *rbfTxStore) NewQueryContext(ctx context.Context) (QueryContext, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, errors.New("can't create query context on closed TxStore")
	}
	// we assign random names so that we can debug which query context is which more easily
	rq := &rbfQueryContext{
		txStore: r,
		ctx:     ctx,
		queries: make(map[dbKey]*rbfTxWrappers),
	}
	rq.name = fmt.Sprintf("rqcx-%p", rq)
	runtime.SetFinalizer(rq, finalizeQueryContext)
	r.queries[rq] = struct{}{}
	return rq, nil
}

// NewWriteQueryContext creates a write-capable query context, with writes scoped by
// the provided QueryScope. The query context will be able to create write requests
// for any {index, field, view, shard} that's Allowed() by writes, and can also create
// read requests for others.
func (r *rbfTxStore) NewWriteQueryContext(ctx context.Context, scope QueryScope) (QueryContext, error) {
	// we assign random names so that we can debug which query context is which more easily
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, errors.New("can't create query context on closed TxStore")
	}
	for r.writeBlocked(scope) {
		r.writeQueue.Wait()
	}
	rq := &rbfQueryContext{
		txStore: r,
		ctx:     ctx,
		scope:   scope,
		queries: make(map[dbKey]*rbfTxWrappers),
	}
	rq.name = fmt.Sprintf("wqcx-%p", rq)
	r.writeScopes[rq] = scope
	r.queries[rq] = struct{}{}
	runtime.SetFinalizer(rq, finalizeQueryContext)
	return rq, nil
}

func (r *rbfTxStore) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return errors.New("double-close of TxStore")
	}
	var firstErr error
	// A QueryContext can be live without having any transactions open yet.
	if len(r.queries) > 0 {
		return fmt.Errorf("can't close TxStore while query contexts are outstanding")
	}
	// even if we fail to close databases, *we're* closed and will
	// no longer allow new QueryContexts
	r.closed = true
	for key, db := range r.dbs {
		if len(db.queryContexts) > 0 {
			firstErr = fmt.Errorf("db %q: %d transaction(s) still open", db.dbPath, len(db.queryContexts))
			continue
		}
		if db.db != nil {
			if err := db.db.Close(); err != nil {
				firstErr = err
			}
		}
		// we remove this from our list of databases anyway, as long as it wasn't
		// still in use. we can't retry the close itself.
		delete(r.dbs, key)
	}
	return firstErr
}

// rbfTxWrappers is the per-dbKey part of a QueryContext, representing the set of
// fragment-specific query reads (or writes) associated with a given rbf.Tx. The objects
// are stored here in a map of QueryRead, but they may actually be QueryWrite. (We
// have to grab the write transaction initially, because we can only grab a transaction
// once for each dbKey, and any Allowed fragment could later request a write.)
//
// Underlying Tx are internally locked with an RWMutex on the RBF side, so we don't
// do locking on our side. If multiple QueryRead/QueryWrite are simultaneously
// operating, that's fine, they'll still be serialized at that point. In theory,
// though, that's probably a logic error. Possibly we should check for it.
type rbfTxWrappers struct {
	key     dbKey
	db      *rbfDBQueryContexts
	tx      *rbf.Tx
	queries map[fragKey]QueryRead
	writeTx bool
}

func (txw *rbfTxWrappers) readKey(fk fragKey) (QueryRead, error) {
	qr, ok := txw.queries[fk]
	if !ok {
		read := rbfQueryRead{tx: txw, fk: fk}
		if txw.writeTx {
			write := &rbfQueryWrite{rbfQueryRead: read}
			qr = write
		} else {
			qr = &read
		}
		txw.queries[fk] = qr
	}
	if qw, ok := qr.(*rbfQueryWrite); ok {
		return &qw.rbfQueryRead, nil
	}
	return qr, nil
}

var errWriteRequestOnNonWrite = errors.New("write request tried to use read-only transaction")

func (txw *rbfTxWrappers) writeKey(fk fragKey) (QueryWrite, error) {
	if qr, ok := txw.queries[fk]; ok {
		if qw, ok := qr.(*rbfQueryWrite); ok {
			return qw, nil
		}
		return nil, errWriteRequestOnNonWrite
	}
	write := &rbfQueryWrite{rbfQueryRead: rbfQueryRead{tx: txw, fk: fk}}
	txw.queries[fk] = write
	return write, nil
}

// rbfQueryContext represents a query context backed by an rbfTxStore. It tracks
// its current access to backend resources with a map[dbKey]*rbfTxWrappers.
// For each dbKey it uses, it may have a Tx, which is always a single shared Tx
// used by any and all wrappers that would use that database.
type rbfQueryContext struct {
	mu      sync.Mutex
	name    string
	txStore *rbfTxStore
	ctx     context.Context
	scope   QueryScope
	err     error
	queries map[dbKey]*rbfTxWrappers
	done    bool
}

// verify that rbfQueryContext implements the interface
var _ QueryContext = &rbfQueryContext{}

func (rq *rbfQueryContext) String() string {
	return fmt.Sprintf("rbf-Qcx<%s>%s", rq.name, rq.scope)
}

func (rq *rbfQueryContext) NewRead(index IndexName, field FieldName, view ViewName, shard ShardID) (QueryRead, error) {
	rq.mu.Lock()
	defer rq.mu.Unlock()
	dbk, fk := rq.txStore.keys(index, field, view, shard)
	queries, ok := rq.queries[dbk]
	if !ok {
		var err error
		// new database connection
		// If we can write to this, we request a write query even though this is a read operation,
		// because we will be reusing that query for future operations anyway, and *they* might
		// be writes. If we aren't allowed to write to this, it's still okay to read from it.
		if rq.scope != nil && rq.scope.Allowed(index, field, view, shard) {
			queries, err = rq.txStore.newWriteTx(rq, dbk)
		} else {
			queries, err = rq.txStore.newReadTx(rq, dbk)
		}
		if err != nil {
			return nil, err
		}
		rq.queries[dbk] = queries
	}
	return queries.readKey(fk)
}

func (rq *rbfQueryContext) NewWrite(index IndexName, field FieldName, view ViewName, shard ShardID) (QueryWrite, error) {
	if rq.scope == nil {
		return nil, errors.New("read-only query context can't write")
	}
	dbk, fk := rq.txStore.keys(index, field, view, shard)
	if !rq.scope.Allowed(index, field, view, shard) {
		return nil, fmt.Errorf("query context does not cover writes to db key %s", dbk)
	}
	rq.mu.Lock()
	defer rq.mu.Unlock()
	queries, ok := rq.queries[dbk]
	if !ok {
		var err error
		// new database connection
		queries, err = rq.txStore.newWriteTx(rq, dbk)
		if err != nil {
			return nil, err
		}
		rq.queries[dbk] = queries
	}
	return queries.writeKey(fk)
}

func (rq *rbfQueryContext) Error(args ...interface{}) {
	rq.mu.Lock()
	defer rq.mu.Unlock()
	rq.err = errors.New(fmt.Sprint(args...))
}

func (rq *rbfQueryContext) Errorf(msg string, args ...interface{}) {
	rq.mu.Lock()
	defer rq.mu.Unlock()
	rq.err = fmt.Errorf(msg, args...)
}

// unprotectedRelease is the shared implementation called by both
// Release and Commit to mark us no longer using resources. Call
// with lock held.
func (rq *rbfQueryContext) unprotectedRelease() {
	if rq.done {
		return
	}
	for _, query := range rq.queries {
		query.db.release(query)
	}
	rq.txStore.release(rq)
	rq.done = true
}

func (rq *rbfQueryContext) Release() {
	rq.mu.Lock()
	defer rq.mu.Unlock()
	rq.unprotectedRelease()
}

// Commit tries to write, unless it's already encountered
// an error. Either way, it tries to close every open
// transaction.
func (rq *rbfQueryContext) Commit() error {
	rq.mu.Lock()
	defer rq.mu.Unlock()
	if rq.done {
		return errors.New("query context already released, can't commit")
	}
	if rq.err != nil {
		rq.unprotectedRelease()
		return rq.err
	}
	// fail out early if our context got canceled
	if err := rq.ctx.Err(); err != nil {
		rq.unprotectedRelease()
		return err
	}
	var firstErr error
	for _, query := range rq.queries {
		if query.writeTx && firstErr == nil {
			err := query.db.commit(query)
			if err != nil {
				firstErr = err
			}
		} else {
			query.db.release(query)
		}
	}
	rq.txStore.release(rq)
	rq.done = true
	return firstErr
}

func finalizeQueryContext(rq *rbfQueryContext) {
	if !rq.done {
		fmt.Fprintf(os.Stderr, "rbfQueryContext %s wasn't closed. (writes %v)\n", rq.name, rq.scope)
	}
}

// rbfQueryRead is a fragment-specific read-only wrapper around
// an rbf.Tx.
type rbfQueryRead struct {
	fk fragKey
	tx *rbfTxWrappers
}

func (qr *rbfQueryRead) ContainerIterator(ckey uint64) (citer roaring.ContainerIterator, found bool, err error) {
	return qr.tx.tx.ContainerIterator(string(qr.fk), ckey)
}

func (qr *rbfQueryRead) ApplyFilter(ckey uint64, filter roaring.BitmapFilter) (err error) {
	return qr.tx.tx.ApplyFilter(string(qr.fk), ckey, filter)
}

func (qr *rbfQueryRead) Container(ckey uint64) (*roaring.Container, error) {
	return qr.tx.tx.Container(string(qr.fk), ckey)
}

func (qr *rbfQueryRead) Contains(v uint64) (exists bool, err error) {
	return qr.tx.tx.Contains(string(qr.fk), v)
}

func (qr *rbfQueryRead) Count() (uint64, error) {
	return qr.tx.tx.Count(string(qr.fk))
}

func (qr *rbfQueryRead) Max() (uint64, error) {
	return qr.tx.tx.Max(string(qr.fk))
}

func (qr *rbfQueryRead) Min() (uint64, bool, error) {
	return qr.tx.tx.Min(string(qr.fk))
}

func (qr *rbfQueryRead) CountRange(start, end uint64) (uint64, error) {
	return qr.tx.tx.CountRange(string(qr.fk), start, end)
}

func (qr *rbfQueryRead) RoaringBitmap() (*roaring.Bitmap, error) {
	return qr.tx.tx.RoaringBitmap(string(qr.fk))
}

func (qr *rbfQueryRead) OffsetRange(offset, start, end uint64) (*roaring.Bitmap, error) {
	return qr.tx.tx.OffsetRange(string(qr.fk), offset, start, end)
}

var _ QueryRead = &rbfQueryRead{}

// rbfQueryWrite is a fragment-specific write-capable wrapper
// around an rbf.Tx. It embeds an rbfQueryRead so that, when you
// want to return a QueryRead, you can just return its
// inner object and there's no chance of accidentally upgrading.
type rbfQueryWrite struct {
	rbfQueryRead
}

func (qw *rbfQueryWrite) PutContainer(ckey uint64, c *roaring.Container) error {
	return qw.tx.tx.PutContainer(string(qw.fk), ckey, c)
}

func (qw *rbfQueryWrite) RemoveContainer(ckey uint64) error {
	return qw.tx.tx.RemoveContainer(string(qw.fk), ckey)
}

func (qw *rbfQueryWrite) Add(a ...uint64) (changeCount int, err error) {
	return qw.tx.tx.Add(string(qw.fk), a...)
}

func (qw *rbfQueryWrite) Remove(a ...uint64) (changeCount int, err error) {
	return qw.tx.tx.Remove(string(qw.fk), a...)
}

func (qw *rbfQueryWrite) ApplyRewriter(ckey uint64, filter roaring.BitmapRewriter) (err error) {
	return qw.tx.tx.ApplyRewriter(string(qw.fk), ckey, filter)
}

func (qw *rbfQueryWrite) ImportRoaringBits(rit roaring.RoaringIterator, clear bool, rowSize uint64) (changed int, rowSet map[uint64]int, err error) {
	// TODO: when we finish replacing Qcx/Tx with this, drop the unused "log"
	// flag. It was only ever used by the roaring backend.
	return qw.tx.tx.ImportRoaringBits(string(qw.fk), rit, clear, false, rowSize)
}

var _ QueryWrite = &rbfQueryWrite{}
