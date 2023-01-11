// Copyright 2022 Molecula Corp (DBA FeatureBase). All rights reserved.
package querycontext

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"

	"github.com/molecula/featurebase/v3/keys"
	"github.com/molecula/featurebase/v3/logger"
	"github.com/molecula/featurebase/v3/rbf"
	rbfcfg "github.com/molecula/featurebase/v3/rbf/cfg"
	"github.com/molecula/featurebase/v3/roaring"
	"github.com/molecula/featurebase/v3/task"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
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

func (r *rbfDBQueryContexts) close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.queryContexts) > 0 {
		return fmt.Errorf("db %q: %d transaction(s) still open", r.dbPath, len(r.queryContexts))
	}
	if r.db != nil {
		return r.db.Close()
	}
	return nil
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

// contents reports what fragment keys are in this database.
// This should really return []fragKey, but we're not there yet.
func (r *rbfDBQueryContexts) contents() ([]string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	// scan database for fields and views...
	tx, err := r.db.Begin(false)
	if err != nil {
		return nil, fmt.Errorf("scanning database at %q: %v", r.dbPath, err)
	}
	defer tx.Rollback()
	fvs := tx.FieldViews()
	tx.Rollback()
	return fvs, nil
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
	workerPool  *task.Pool
	logger      logger.Logger
}

func (*rbfTxStore) Backend() string {
	return "rbf"
}

// NewRBFTxStore creates a new RBF-backed TxStore in the given directory. If
// cfg is nil, it will use a `NewDefaultConfig`. All databases will be opened
// using the same config. If splitter is nil, it uses an index/shard splitter.
//
// If present, the logger is used for diagnostic messages, otherwise they're
// written to os.Stderr. If a task.Pool is provided, blocking operations
// may report themselves to it as blocked or unblocked. (For instance, a
// request for a WriteQueryContext will mark itself as blocked while waiting
// on previous contexts.)
//
// With the index/shard key splitter, database directory paths look like
// `path/indexes/i/shards/00000000`, with each shard directory containing
// data/wal files.
func NewRBFTxStore(path string, cfg *rbfcfg.Config, logger logger.Logger, workers *task.Pool, splitter KeySplitter) (*rbfTxStore, error) {
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
		workerPool:  workers,
		logger:      logger,
	}
	r.writeQueue = sync.NewCond(&r.mu)
	return r, nil
}

func (r *rbfTxStore) DumpDot(w io.Writer) error {
	var dg dotGraph
	dg.enqueue(r)
	dg.build(5) // 5 is the right depth to see the whole tree, roughly
	return dg.Write(w)
}

// createDBQueryContext creates a new rbfDBQueryContexts associating
// the dbKey with the provided path, and opening the underlying file.
func (r *rbfTxStore) createDBQueryContext(dbk dbKey, dbPath string) (*rbfDBQueryContexts, error) {
	db := &rbfDBQueryContexts{key: dbk, dbPath: dbPath, queryContexts: make(map[*rbfTxWrappers]*rbfQueryContext)}
	path := filepath.Join(r.rootPath, db.dbPath)
	// note: we don't need to create the directory here, because rbf.Open
	// creates the directory for us. I was slightly surprised by this and
	// I'm not sure I like it. Note also that the database path is actually
	// a directory containing files named "data" and "wal".
	db.db = rbf.NewDB(path, r.cfg)
	err := db.db.Open()
	if err != nil {
		return nil, err
	}
	r.dbs[dbk] = db
	return db, nil
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
	return r.createDBQueryContext(dbk, dbPath)
}

// getExistingDB gets an existing db for the given key. call only when you hold
// the rbfTxStore's lock. this will open a database that already exists, even
// if there isn't an existing rbfDBQueryContexts for it, but will not create
// a new file. If the file doesn't exist, this function returns a nil
// *rbfDBQueryContexts, and a nil error. You have to check the returned db
// before using it. The returned DB is stashed in r.dbs as usual.
func (r *rbfTxStore) getExistingDB(dbk dbKey) (*rbfDBQueryContexts, error) {
	db, ok := r.dbs[dbk]
	if ok && db != nil {
		return db, nil
	}
	dbPath, err := r.dbPath(dbk)
	if err != nil {
		return nil, err
	}
	_, err = os.Stat(filepath.Join(r.rootPath, dbPath))
	if err != nil {
		return nil, err
	}
	return r.createDBQueryContext(dbk, dbPath)
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
	if r.workerPool != nil {
		// if we have been given a worker pool, we mark this worker as
		// blocked until we make it through creating the context, because
		// creating a write query context can block for quite a while.
		r.workerPool.Block()
		defer r.workerPool.Unblock()
	}
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

func (r *rbfTxStore) DeleteIndex(index keys.Index) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	dirPath := filepath.Join(r.rootPath, string(index), "backends", "rbf")
	dirInfo, err := os.Stat(dirPath)
	if err != nil {
		// The directory doesn't exist, so presumably we don't have anything
		// to do here.
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if !dirInfo.IsDir() {
		return fmt.Errorf("%q exists, but is not a directory", dirPath)
	}
	prefix := string(index) + "/"
	for k, db := range r.dbs {
		if !strings.HasPrefix(string(k), prefix) {
			continue
		}
		// Close every matching db.
		// close will fail if there are any open querycontexts, so we can't
		// close anything that's in use. If we close a database, we
		// immediately remove it from our table of databases (which we can
		// do because we're holding the lock on the TxStore).
		// It's harmless to close a database no one is using, it can just
		// be reopened later.
		if err := db.close(); err != nil {
			return err
		}
		delete(r.dbs, k)
	}
	// If we got here, every such database is closed. Let's remove
	// the directory tree for the rbf backend, in its entirety.
	return os.RemoveAll(dirPath)
}

func (r *rbfTxStore) DeleteField(index keys.Index, field keys.Field) error {
	// We don't have to worry about existing reads, because existing
	// reads will be against their own snapshot of the RBF file and
	// won't be affected by changes we make.
	//
	// But something could already have a write lock on one of these files,
	// and we can't safely grab the TxStore lock and then wait for a lock on
	// the file. The simplest solution: Create a WriteQueryContext for each
	// shard as we get to it.
	indexPath := filepath.Join(r.rootPath, string(index))
	shards, err := filepath.Glob(filepath.Join(indexPath, "backends", "rbf", "shard.*"))
	if err != nil {
		return errors.Wrap(err, "finding shards")
	}
	// the field prefix will be the same in all of these
	fieldPrefix := fmt.Sprintf("~%s;", field)
	for _, shardPath := range shards {
		shardName := filepath.Base(shardPath)
		shardNum, err := strconv.ParseInt(shardName[6:], 10, 64)
		if err != nil {
			return fmt.Errorf("malformed shard path %q: %v", shardName, err)
		}
		shard := keys.Shard(shardNum)
		// Do the fancy bits in their own function so we can use defer.
		err = func() error {
			// create a query context, giving us for free all the fancy lock
			// interactions we already figured out once, get a QueryWrite for it
			// because that's the easiest way to get a working rbf.Tx with the
			// right scope, then peek under the hood briefly.
			qcx, err := r.NewWriteQueryContext(context.TODO(), r.Scope().AddIndexShards(index, shard))
			if err != nil {
				return fmt.Errorf("getting write context for %q/%d: %w", index, shard, err)
			}
			defer qcx.Release()
			// This is theoretically more narrow than what we're doing, but we are the
			// implementation internals and are allowed to cheat like this.
			qw, err := qcx.Write(index, field, "", shard)
			if err != nil {
				return fmt.Errorf("getting write access for %q/%d: %w", index, shard, err)
			}
			rqw, ok := qw.(*rbfQueryWrite)
			if !ok {
				return errors.New("internal error: wrong tx type for query write")
			}
			err = rqw.tx.tx.DeleteBitmapsWithPrefix(fieldPrefix)
			if err != nil {
				return errors.Wrap(err, "deleting bitmap")
			}
			return qcx.Commit()
		}()
		if err != nil {
			return fmt.Errorf("deleting field %q from shard %d of index %q: %w", field, shard, index, err)
		}
	}
	return nil
}

type rbfSnapshotReadCloser struct {
	qcx QueryContext
	tx  *rbf.Tx
	io.Reader
}

func (r *rbfSnapshotReadCloser) Close() error {
	r.tx.Rollback()
	r.qcx.Release()
	return nil
}

func (r *rbfTxStore) Backup(qcx QueryContext, index keys.Index, shard keys.Shard) (_ io.ReadCloser, failed error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	// We're responsible for ensuring the querycontext is released, either
	// when we're done or when our returned ReadCloser is closed.
	defer func() {
		if failed != nil {
			qcx.Release()
		}
	}()
	dbk, _ := r.keys(index, "", "", shard)
	db, err := r.getExistingDB(dbk)
	if err != nil {
		return nil, err
	}
	if db == nil {
		return nil, fmt.Errorf("no data for %q/%d", index, shard)
	}
	tx, err := db.db.Begin(false)
	if err != nil {
		return nil, err
	}
	reader, err := tx.SnapshotReader()
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	return &rbfSnapshotReadCloser{qcx: qcx, tx: tx, Reader: reader}, nil
}

// Restore takes a reader containing data compatible with the backend,
// such as an RBF file, and replaces all the existing data for this index
// and shard.
func (r *rbfTxStore) Restore(qcx QueryContext, index keys.Index, shard keys.Shard, src io.Reader) (err error) {
	defer func() {
		// clarify the error once so we don't have to do this everywhere
		if err != nil {
			err = fmt.Errorf("can't restore %q/%d: %w", index, shard, err)
		}
	}()
	r.mu.Lock()
	defer r.mu.Unlock()
	dbk, _ := r.keys(index, "", "", shard)
	if db := r.dbs[dbk]; db != nil {
		// uh-oh... we already have this?
		// let's try to close it.
		err := db.close()
		if err != nil {
			return err
		}
		delete(r.dbs, dbk)
	}
	dbPath, err := r.dbPath(dbk)
	if err != nil {
		return err
	}
	dirPath := filepath.Join(r.rootPath, dbPath)
	parent := filepath.Dir(dirPath)
	if err := os.MkdirAll(parent, 0o750); err != nil {
		return err
	}
	if err := os.Mkdir(dirPath, 0o750); err != nil {
		if os.IsExist(err) {
			// remove existing directory and try again
			err = os.RemoveAll(dirPath)
			if err != nil {
				return err
			}
			err = os.Mkdir(dirPath, 0o750)
		}
		// err could now be nil because it was previously an Exist
		// error, and we replaced it with the result of Mkdir
		if err != nil {
			return err
		}
	}

	// the directory now exists, so.
	dataPath := filepath.Join(dirPath, "data")
	tmpPath := dataPath + ".tmp"
	file, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	defer func() {
		if file != nil {
			// can't really do anything about an error here. it's okay to
			// double-close files, it just produces an error.
			_ = file.Close()
			// remove the temp file, if it still exists, which it might in a failed
			// path.
			_ = os.Remove(tmpPath)
		}
	}()
	_, err = io.Copy(file, src)
	if err != nil {
		return errors.Wrap(err, "copying to file")
	}
	if err = file.Sync(); err != nil {
		return err
	}
	if err = file.Close(); err != nil {
		return err
	}
	if err = os.Rename(tmpPath, dataPath); err != nil {
		return err
	}
	// reopen the file, now that it exists, and report an error if that doesn't work
	_, err = r.getExistingDB(dbk)
	return err
}

func (r *rbfTxStore) ListFieldViews(index keys.Index, shard keys.Shard) (map[keys.Field][]keys.View, error) {
	fvs, err := r.dbContents(index, shard)
	if err != nil {
		return nil, fmt.Errorf("scanning database for existing contents: %v", err)
	}
	// Once we've done a couple of shards, fields and views are likely
	// to already exist.
	result := make(map[keys.Field][]keys.View)
	for _, fv := range fvs {
		_, field, view, _, err := r.parseFragKey(fragKey(fv))
		if err != nil {
			return nil, fmt.Errorf("invalid fragment key %q for %q/%d: %w", fv, index, shard, err)
		}
		result[field] = append(result[field], view)
	}
	return result, nil
}

// DeleteFragment tries to delete the given fragment. It's not an error for
// it to already not exist. This is untested and unverified as of this
// writing; I wrote it just to try to verify the API.
func (r *rbfTxStore) DeleteFragments(index keys.Index, field keys.Field, views []keys.View, shards []keys.Shard) error {
	if _, ok := r.KeySplitter.(*indexShardKeySplitter); !ok {
		return fmt.Errorf("fragment batch delete only supported for indexShardKeySplitter, have %T", r.KeySplitter)
	}
	// nothing to do
	if len(views) == 0 || len(shards) == 0 {
		return nil
	}
	dbKeys := make([]dbKey, len(shards))
	fragKeys := make([]fragKey, len(views))

	for i, view := range views {
		fragKeys[i] = r.fragKey(index, field, view, shards[0])
	}
	for i, shard := range shards {
		dbKeys[i] = r.dbKey(index, field, views[0], shard)
	}
	// Now, for every shard, we want to delete all those views. WARNING:
	// WE DO NOT RESPECT THE QUERYCONTEXT WRITE GATES. We are just going
	// straight into the RBF database and requesting a Tx. This should be
	// safe because we do not block on *anything external to the individual
	// databases, and each of these operations will separately release its
	// lock the moment it's done.
	ctx := context.Background()
	// we use a WithContext errgroup because this provides the convenient
	// trait that if one of the deletes fails, the others can bail prematurely.

	eg, ctx := errgroup.WithContext(ctx)
	// Completely arbitrary value: allow up to 8 running at once.
	sema := make(chan struct{}, 8)
	var err error
	for _, dbKey := range dbKeys {
		if ctx.Err() != nil {
			break
		}
		var db *rbfDBQueryContexts
		db, err = r.getExistingDB(dbKey)
		if err != nil {
			// if we encounter an error, we stop. note that err has
			// scope outside this loop, so this error is available to
			// the surrounding code.
			break
		}
		// getExistingDB is allowed to return a nil DB with no error,
		// indicating that the DB didn't exist.
		if db == nil {
			continue
		}
		// use channel as a cheap semaphore to cap our simultaneous
		// deletes
		sema <- struct{}{}
		eg.Go(func() error {
			defer func() {
				<-sema
			}()
			tx, err := db.db.Begin(true)
			if err != nil {
				return err
			}
			defer tx.Rollback()
			for _, fragKey := range fragKeys {
				err := tx.DeleteBitmapsWithPrefix(string(fragKey))
				if err != nil {
					return err
				}
			}
			return tx.Commit()
		})
	}
	err2 := eg.Wait()
	if err != nil {
		return err
	}
	return err2
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
	// no longer allow new QueryContexts. because of this, we try to close
	// the whole database.
	r.closed = true
	for key, db := range r.dbs {
		if err := db.close(); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		// we remove this from our list of databases anyway, as long as it wasn't
		// still in use. we can't retry the close itself.
		delete(r.dbs, key)
	}
	return firstErr
}

func (r *rbfTxStore) dbContents(index keys.Index, shard keys.Shard) ([]string, error) {
	dbk, _ := r.keys(keys.Index(index), "", "", shard)
	db, err := r.getExistingDB(dbk)
	if err != nil {
		return nil, fmt.Errorf("opening database for %q/%d: %v", index, shard, err)
	}
	if db == nil {
		return nil, fmt.Errorf("opening existing database for %q/%d: no error, but also no database", index, shard)
	}
	return db.contents()
}

func (r *rbfTxStore) Contents() (keys.DBContents, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	indexes, err := filepath.Glob(filepath.Join(r.rootPath, "*"))
	if err != nil {
		return nil, err
	}
	contents := make(keys.DBContents, len(indexes))
	// For each index...
	for _, indexPath := range indexes {
		index := filepath.Base(indexPath)
		shards, err := filepath.Glob(filepath.Join(indexPath, "backends", "rbf", "shard.*"))
		if err != nil {
			return nil, err
		}
		// Create an IndexContents. We can do this unconditionally because we know
		// we haven't seen this index before.
		indexContents := make(keys.IndexContents)
		contents[keys.Index(index)] = indexContents
		for _, shardPath := range shards {
			shardName := filepath.Base(shardPath)
			shardNum, err := strconv.ParseInt(shardName[6:], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("malformed shard path %q: %v", shardName, err)
			}
			shard := keys.Shard(shardNum)
			fvs, err := r.dbContents(keys.Index(index), shard)
			if err != nil {
				return nil, fmt.Errorf("scanning database for existing contents: %v", err)
			}
			// Once we've done a couple of shards, fields and views are likely
			// to already exist.
			for _, fv := range fvs {
				_, field, view, _, err := r.parseFragKey(fragKey(fv))
				if err != nil {
					return nil, fmt.Errorf("invalid fragment key %q in %q: %w", fv, shardPath, err)
				}
				fieldContents := indexContents[field]
				if fieldContents == nil {
					fieldContents = make(keys.FieldContents)
					indexContents[field] = fieldContents
				}
				viewContents := fieldContents[view]
				if viewContents == nil {
					viewContents = make(keys.ViewContents)
					fieldContents[view] = viewContents
				}
				viewContents[shard] = struct{}{}
			}
		}
	}
	return contents, nil
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
	mu      sync.Mutex // ops are not concurrency-safe
}

func (txw *rbfTxWrappers) readKey(fk fragKey) (QueryRead, error) {
	txw.mu.Lock()
	defer txw.mu.Unlock()
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
	txw.mu.Lock()
	defer txw.mu.Unlock()
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

// To allow for flushes, which can replace our tx, we provide wrappers for all
// the QueryRead/QueryWrite functions which use a shared lock. Note that rbf is
// also, now-redundantly, doing locking on the operations that go through it,
// but also we're controlling access to txw.tx.

func (txw *rbfTxWrappers) ContainerIterator(fk fragKey, ckey uint64) (citer roaring.ContainerIterator, found bool, err error) {
	txw.mu.Lock()
	defer txw.mu.Unlock()
	return txw.tx.ContainerIterator(string(fk), ckey)
}

func (txw *rbfTxWrappers) ApplyFilter(fk fragKey, ckey uint64, filter roaring.BitmapFilter) (err error) {
	txw.mu.Lock()
	defer txw.mu.Unlock()
	return txw.tx.ApplyFilter(string(fk), ckey, filter)
}

func (txw *rbfTxWrappers) Container(fk fragKey, ckey uint64) (*roaring.Container, error) {
	txw.mu.Lock()
	defer txw.mu.Unlock()
	return txw.tx.Container(string(fk), ckey)
}

func (txw *rbfTxWrappers) Contains(fk fragKey, v uint64) (exists bool, err error) {
	txw.mu.Lock()
	defer txw.mu.Unlock()
	return txw.tx.Contains(string(fk), v)
}

func (txw *rbfTxWrappers) Count(fk fragKey) (uint64, error) {
	txw.mu.Lock()
	defer txw.mu.Unlock()
	return txw.tx.Count(string(fk))
}

func (txw *rbfTxWrappers) Max(fk fragKey) (uint64, error) {
	txw.mu.Lock()
	defer txw.mu.Unlock()
	return txw.tx.Max(string(fk))
}

func (txw *rbfTxWrappers) Min(fk fragKey) (uint64, bool, error) {
	txw.mu.Lock()
	defer txw.mu.Unlock()
	return txw.tx.Min(string(fk))
}

func (txw *rbfTxWrappers) CountRange(fk fragKey, start, end uint64) (uint64, error) {
	txw.mu.Lock()
	defer txw.mu.Unlock()
	return txw.tx.CountRange(string(fk), start, end)
}

func (txw *rbfTxWrappers) RoaringBitmap(fk fragKey) (*roaring.Bitmap, error) {
	txw.mu.Lock()
	defer txw.mu.Unlock()
	return txw.tx.RoaringBitmap(string(fk))
}

func (txw *rbfTxWrappers) OffsetRange(fk fragKey, offset, start, end uint64) (*roaring.Bitmap, error) {
	txw.mu.Lock()
	defer txw.mu.Unlock()
	return txw.tx.OffsetRange(string(fk), offset, start, end)
}

func (txw *rbfTxWrappers) PutContainer(fk fragKey, ckey uint64, c *roaring.Container) error {
	txw.mu.Lock()
	defer txw.mu.Unlock()
	return txw.tx.PutContainer(string(fk), ckey, c)
}

func (txw *rbfTxWrappers) RemoveContainer(fk fragKey, ckey uint64) error {
	txw.mu.Lock()
	defer txw.mu.Unlock()
	return txw.tx.RemoveContainer(string(fk), ckey)
}

func (txw *rbfTxWrappers) Add(fk fragKey, a ...uint64) (changeCount int, err error) {
	txw.mu.Lock()
	defer txw.mu.Unlock()
	return txw.tx.Add(string(fk), a...)
}

func (txw *rbfTxWrappers) Remove(fk fragKey, a ...uint64) (changeCount int, err error) {
	txw.mu.Lock()
	defer txw.mu.Unlock()
	return txw.tx.Remove(string(fk), a...)
}

func (txw *rbfTxWrappers) ApplyRewriter(fk fragKey, ckey uint64, filter roaring.BitmapRewriter) (err error) {
	txw.mu.Lock()
	defer txw.mu.Unlock()
	return txw.tx.ApplyRewriter(string(fk), ckey, filter)
}

func (txw *rbfTxWrappers) ImportRoaringBits(fk fragKey, rit roaring.RoaringIterator, clear bool, rowSize uint64) (changed int, rowSet map[uint64]int, err error) {
	// TODO: when we finish replacing Qcx/Tx with this, drop the unused "log"
	// flag. It was only ever used by the roaring backend.
	txw.mu.Lock()
	defer txw.mu.Unlock()
	return txw.tx.ImportRoaringBits(string(fk), rit, clear, false, rowSize)
}

func (txw *rbfTxWrappers) Flush() error {
	if !txw.writeTx {
		return errors.New("attempt to flush non-write transaction")
	}
	txw.mu.Lock()
	defer txw.mu.Unlock()
	err := txw.tx.Commit()
	if err != nil {
		return err
	}
	tx, err := txw.db.db.Begin(true)
	if err != nil {
		return errors.Wrap(err, "trying to obtain fresh transaction")
	}
	txw.tx = tx
	return nil
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

// Flush only works correctly when we're using an index/shard split.
func (rq *rbfQueryContext) Flush(index keys.Index, shard keys.Shard) error {
	rq.mu.Lock()
	defer rq.mu.Unlock()
	dbk, _ := rq.txStore.keys(index, "", "", shard)
	txw, ok := rq.queries[dbk]
	if ok {
		return txw.Flush()
	}
	// it's okay if we didn't have one.
	return nil
}

var errAlreadyErrored = errors.New("query context previously encountered an error")

func (rq *rbfQueryContext) Read(index keys.Index, field keys.Field, view keys.View, shard keys.Shard) (QueryRead, error) {
	rq.mu.Lock()
	defer rq.mu.Unlock()
	if rq.err != nil {
		return nil, errAlreadyErrored
	}
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

func (rq *rbfQueryContext) Write(index keys.Index, field keys.Field, view keys.View, shard keys.Shard) (QueryWrite, error) {
	if rq.scope == nil {
		return nil, errors.New("read-only query context can't write")
	}
	dbk, fk := rq.txStore.keys(index, field, view, shard)
	if !rq.scope.Allowed(index, field, view, shard) {
		return nil, fmt.Errorf("query context does not cover writes to db key %s", dbk)
	}
	rq.mu.Lock()
	defer rq.mu.Unlock()
	if rq.err != nil {
		return nil, errAlreadyErrored
	}
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

func (qw *rbfQueryWrite) Flush() (err error) {
	return qw.tx.Flush()
}

var _ QueryWrite = &rbfQueryWrite{}
