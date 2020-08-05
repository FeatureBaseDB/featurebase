// Copyright 2020 Pilosa Corp.
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

// +build skip_building_lmdb_for_now

package pilosa

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/glycerine/idem"
	"github.com/glycerine/lmdb-go/lmdb"
	"github.com/pilosa/pilosa/v2/roaring"
	"github.com/pkg/errors"
)

// lmdbWorker represents a goroutine that has been welded to a
// C thread by runtime.LockOSThread(); so it can do work for lmdb.
// We create a single worker to write and a number of workers to read.
// Each LMDBWrapper maintains a worker pool for access to its database.
type lmdbWorker struct {
	write bool
	env   *lmdb.Env
	dbi   lmdb.DBI

	// coordinate shutdown
	halt *idem.Halter

	// run these jobs
	jobCh chan *lmdbJob
}

// lmdbJob communicates jobs to lmdbWorkers.
type lmdbJob struct {
	write bool
	fn    func(job *lmdbJob)
	err   error
	done  chan struct{}
}

func newLMDBJob(write bool, f func(job *lmdbJob)) *lmdbJob {
	return &lmdbJob{
		write: write,
		fn:    f,
		done:  make(chan struct{}),
	}
}

func (w *LMDBWrapper) newLMDBWorker(write bool, env *lmdb.Env, dbi lmdb.DBI) (wrk *lmdbWorker) {
	w.newLMDBWorkerMu.Lock()
	defer w.newLMDBWorkerMu.Unlock()
	if write {
		if w.lmdbWriterCount > 0 {
			panic("can only have one writing lmdb worker")
		}
		w.lmdbWriterCount++
	}

	wrk = &lmdbWorker{
		write: write,
		env:   env,
		dbi:   dbi,
		halt:  idem.NewHalter(),
		jobCh: make(chan *lmdbJob, 100),
	}
	return
}

// StartWriter makes it apparent in the stack trace
// which goroutine is writing.
func (w *lmdbWorker) StartWriter() {
	if !w.write {
		panic("worker is not marked as writer")
	}
	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		defer w.halt.Done.Close()

		//vv("lmdbWorker.Start(), on gid = '%v'", curGID())

		for {
			select {
			case <-w.halt.ReqStop.Chan:
				return
			case job := <-w.jobCh:
				job.fn(job)
				close(job.done)
			}
		}
	}()
}

// StartReader makes it apparent in the stack trace
// which goroutine(s) are reading.
func (w *lmdbWorker) StartReader() {
	if w.write {
		panic("worker is not marked as reader")
	}
	go func() {
		runtime.LockOSThread()
		defer runtime.UnlockOSThread()
		defer w.halt.Done.Close()

		//vv("lmdbWorker.Start(), on gid = '%v'", curGID())

		for {
			select {
			case <-w.halt.ReqStop.Chan:
				return
			case job := <-w.jobCh:
				job.fn(job)
				close(job.done)
			}
		}
	}()
}

func (w *lmdbWorker) Stop() {
	w.halt.ReqStop.Close()
	<-w.halt.Done.Chan
}

// lmdbRegistrar facilitates shutdown
// of all the lmdb databases started under
// tests. Its needed because most tests don't cleanup
// the *Index(es) they create. But we still
// want to shutdown lmdbDB goroutines
// after tests run.
//
// It also allows opening the same path twice to
// result in sharing the same open database handle, and
// thus the same transactional guarantees.
//
type lmdbRegistrar struct {
	mu sync.Mutex
	mp map[*LMDBWrapper]bool

	path2db map[string]*LMDBWrapper
}

var globalLMDBReg *lmdbRegistrar = newLMDBTestRegistrar()

func newLMDBTestRegistrar() *lmdbRegistrar {

	return &lmdbRegistrar{
		mp:      make(map[*LMDBWrapper]bool),
		path2db: make(map[string]*LMDBWrapper),
	}
}

// register each lmdb created under tests, so we
// can clean them up. This is called by openLMDBWrapper() while
// holding the r.mu.Lock, since it needs to atomically
// check the registry and make a new instance only
// if one does not exist for its path, and otherwise
// return the existing instance.
func (r *lmdbRegistrar) unprotectedRegister(w *LMDBWrapper) {
	r.mp[w] = true
	r.path2db[w.path] = w
}

// unregister removes w from r
func (r *lmdbRegistrar) unregister(w *LMDBWrapper) {
	r.mu.Lock()
	delete(r.mp, w)
	delete(r.path2db, w.path)
	r.mu.Unlock()
}

func DumpAllLMDB() {
	globalLMDBReg.mu.Lock()
	defer globalLMDBReg.mu.Unlock()
	for w := range globalLMDBReg.mp {
		_ = w
		AlwaysPrintf("this lmdb path='%v' has: \n%v\n", w.path, w.StringifiedLMDBKeys(nil))
	}
}

// newLMDBWrapper creates a new empty database, blowing away
// any prior path + "-lmdb" directory.
func (r *lmdbRegistrar) newLMDBWrapper(path string) (*LMDBWrapper, error) {
	bpath := lmdbPath(path)
	err := os.RemoveAll(bpath)
	if err != nil {
		return nil, err
	}
	return r.openLMDBWrapper(bpath)
}

// lmdbPath is a helper for determining the full directory
// in which the lmdb database will be stored.
func lmdbPath(path string) string {
	if !strings.HasSuffix(path, "-lmdb") {
		return path + "-lmdb"
	}
	return path
}

// openLMDBDB opens the database in the bpath directoy
// without deleting any prior content. Any LMDBDB
// database directory will have the "-lmdb" suffix.
//
// openLMDBDB will check the registry and make a new instance only
// if one does not exist for its bpath. Otherwise it returns
// the existing instance. This insures only one lmdbDB
// per bpath in this pilosa node.
func (r *lmdbRegistrar) openLMDBWrapper(path0 string) (*LMDBWrapper, error) {
	path := lmdbPath(path0)

	r.mu.Lock()
	defer r.mu.Unlock()
	w, ok := r.path2db[path]
	if ok {
		// creates the effect of having only one lmdb open per pilosa node.
		return w, nil
	}
	// otherwise, make a new lmdb and store it in globalLMDBReg

	runtime.LockOSThread()
	//vv("NewEnv for lmdb, gid = '%v'", curGID())

	env, err := lmdb.NewEnv()
	panicOn(err)

	err = env.SetMaxDBs(1)
	panicOn(err)
	err = env.SetMapSize(1 << 38) // 256 GB
	panicOn(err)

	const MaxReaders = 254 // default is 126
	err = env.SetMaxReaders(MaxReaders)
	panicOn(err)

	panicOn(os.MkdirAll(path, 0755))

	flags := uint(lmdb.NoReadahead) // | uint(lmdb.NoLock) <<< yikes no

	// unsafe, but get upper bound on performance. TODO: remove these.
	//	WriteMap    = C.MDB_WRITEMAP   // Use a writable memory map.
	//	NoMetaSync  = C.MDB_NOMETASYNC // Don't fsync metapage after commit.
	//	NoSync      = C.MDB_NOSYNC     // Don't fsync after commit.
	flags = flags | lmdb.WriteMap | lmdb.NoMetaSync | lmdb.NoSync

	err = env.Open(path, flags, 0644)
	if err != nil {
		AlwaysPrintf("error env.Open(path='%v'): '%v'; on gid = '%v'", path, err, curGID())
	}
	panicOn(err)

	// In any real application it is important to check for readers that were
	// never closed by their owning process, and for which the owning process
	// has exited.  See the documentation on transactions for more information.
	staleReaders, err := env.ReaderCheck()
	panicOn(err)
	if staleReaders > 0 {
		log.Printf("cleared %d reader slots from dead processes", staleReaders)
	}

	// Open a database handle that will be used for the entire lifetime of this
	// application.  Because the database may not have existed before, and the
	// database may need to be created, we need to get the database handle in
	// an update transacation.
	var dbi lmdb.DBI
	name := filepath.Base(path)
	err = env.Update(func(txn *lmdb.Txn) (err error) {
		dbi, err = txn.CreateDBI(name)
		return err
	})
	panicOn(err)

	//vv("made new dbi=%v on gid = '%v'", dbi, gid)

	w = &LMDBWrapper{
		name:   name,
		env:    env,
		reg:    r,
		path:   path,
		dbi:    dbi,
		halt:   idem.NewHalter(),
		jobQ:   make(chan *lmdbJob),
		hasher: NewBlake3Hasher(),
	}
	r.unprotectedRegister(w)

	w.startStack = stack()

	writer := w.newLMDBWorker(true, env, dbi)
	writer.StartWriter()
	w.writer = writer

	reader := w.newLMDBWorker(false, env, dbi)
	reader.StartReader()
	w.readers = []*lmdbWorker{reader}

	w.startFunnel()
	return w, nil
}

func (w *LMDBWrapper) startFunnel() {
	go func() {
		defer w.halt.Done.Close()

		// use rw to enforce the lmdb.NoLock semanitcs
		// of all readers finished before writer allowed to start.
		var rw sync.RWMutex
		for {
			select {
			case <-w.halt.ReqStop.Chan:
				return
			case job := <-w.jobQ:
				if job.write {
					rw.Lock()
					select {
					case w.writer.jobCh <- job:
					case <-w.halt.ReqStop.Chan:
						rw.Unlock()
						return
					}
					select {
					case <-job.done:
						rw.Unlock()
					case <-w.halt.ReqStop.Chan:
						rw.Unlock()
						return
					}
				} else {
					// TODO: keep a readyReader queue and send jobs to more than one ready readers.
					// For now we just have one reader.
					rw.RLock()
					select {
					case w.readers[0].jobCh <- job:
					case <-w.halt.ReqStop.Chan:
						rw.RUnlock()
						return
					}
					select {
					case <-job.done:
						rw.RUnlock()
					case <-w.halt.ReqStop.Chan:
						rw.RUnlock()
						return
					}
				}
			}
		}
	}()
}

var ErrShutdown = fmt.Errorf("shutting down")

func (w *LMDBWrapper) submit(job *lmdbJob) error {
	select {
	case <-w.halt.ReqStop.Chan:
		return ErrShutdown
	case w.jobQ <- job:
		select {
		case <-w.halt.ReqStop.Chan:
			return ErrShutdown
		case <-job.done:
		}
	}
	return job.err
}

// DeleteIndex deletes all the containers associated with
// the named index from the lmdb database.
func (w *LMDBWrapper) DeleteIndex(indexName string) error {

	// We use the apostrophie rune `'` to locate the end of the
	// index name in the key prefix, so we cannot allow indexNames
	// themselves to contain apostrophies.
	if strings.Contains(indexName, "'") {
		return fmt.Errorf("error: bad indexName `%v` in LMDBWrapper.DeleteIndex() call: indexName cannot contain apostrophes/single quotes.", indexName)
	}
	prefix := badgerIndexOnlyPrefix(indexName)
	return w.DeletePrefix(prefix)
}

// statically confirm that LMDBTx satisfies the Tx interface.
var _ Tx = (*LMDBTx)(nil)

// LMDBWrapper provides the NewLMDBTx() method.
// Execute lmdbJob's via LMDBWrapper.submit(); these must
// be done by the lmdb goroutine worker pool.
type LMDBWrapper struct {
	halt *idem.Halter
	jobQ chan *lmdbJob

	newLMDBWorkerMu sync.Mutex
	lmdbWriterCount int

	env *lmdb.Env

	muDb sync.Mutex

	path string
	name string
	dbi  lmdb.DBI

	// track our registrar for Close / goro leak reporting purposes.
	reg *lmdbRegistrar

	// openTx and openIt are LMDBWrapper scoped tables of all open
	// transactions and iterators. These are primarily for debugging purposes.
	// openTx and openIt should only be read/written after locking the muOpenTxIt mutex.

	// the bool value is the writable attribute of the key *LMDBTx
	openTx map[*LMDBTx]bool

	// the bool value is whether the iterator is reversed
	openIt map[*LMDBIterator]bool

	// protect openTx and openIt
	muOpenTxIt sync.Mutex

	// make LMDBWrapper.Close() idempotent, avoiding panic on double Close()
	closed bool

	// GcEveryDur controls how often the background goroutine
	// runs garbage collection on the on-disk values-log.
	// It defaults to running a GC every 1 minute if left as 0.
	GcEveryDur time.Duration

	hasher *Blake3Hasher

	// doAllocZero sets the corresponding flag on all new LMDBTx.
	// When doAllocZero is true, we zero out any data from lmdb
	// after transcation commit and rollback. This simulates
	// what would happen if we were to use the mmap-ed data
	// from lmdb directly. Currently we copy by default for
	// safety because otherwise TestAPI_ImportColumnAttrs sees
	// corrupted data.
	doAllocZero bool

	// stack() from our creation point, to track tests
	// that haven't closed us.
	startStack string

	DeleteEmptyContainer bool

	writer  *lmdbWorker
	readers []*lmdbWorker

	nextTxSn int64
}

// unprotectedListOpenTxAsString is a debugging helper.
// It is not thread safe, but is only used for debugging. Called internally while
// holding locks.
func (w *LMDBWrapper) unprotectedListOpenTxAsString() (r string) {

	r = "openTx list = ["
	for txn, write := range w.openTx {
		r += fmt.Sprintf("txn p=%p(write:%v), ", txn, write)
	}
	return r + "]"
}

var _ = (*LMDBWrapper)(nil).unprotectedListOpenTxAsString // linter happy

// UnprotectedListOpenItAsString is exported because it is
// used for debugging in some of the pilosa_test tests.
// It is not thread safe, but only used for debugging. Called internally
// while holding locks and externally while not.
func (w *LMDBWrapper) UnprotectedListOpenItAsString() (r string) {
	r = "openIt list = ["
	for it, reverse := range w.openIt {
		r += fmt.Sprintf("it p=%p(reverse:%v), ", it, reverse)
	}
	return r + "]"
}

// NewLMDBTx produces LMDB based ACID transactions. If
// the transaction will modify data, then the write flag must be true.
// Read-only queries should set write to false, to allow more concurrency.
// Methods on a LMDBTx are thread-safe, and can be called from
// different goroutines.
//
// initialIndexName is optional. It is set by the TxFactory from the Txo
// options provided at the Tx creation point. It allows us to recognize
// and isolate cross-index queries more quickly. It can always be empty ""
// but when set is highly useful for debugging. It has no impact
// on transaction behavior.
//
func (w *LMDBWrapper) NewLMDBTx(write bool, initialIndexName string) (tx *LMDBTx) {
	//w.muDb.Lock() // deadlocked here
	//defer w.muDb.Unlock()

	rwflag := uint(0) // writable txn denotated by lack of the lmdb.Readonly flag.
	if !write {
		rwflag = lmdb.Readonly
	}

	sn := atomic.AddInt64(&w.nextTxSn, 1)
	//vv("about to create txn sn=%v, write='%v'; on '%v'/%v, stack=\n'%v'", sn, write, initialIndexName, w.path, stack())
	lmdbTxn, err := w.env.BeginTxn(nil, rwflag)
	panicOn(err)
	//vv("back from creating txn sn=%v, write='%v'; on '%v'/%v", sn, write, initialIndexName, w.path)

	tx = &LMDBTx{
		sn:    sn,
		write: write,
		tx:    lmdbTxn,
		dbi:   w.dbi,
		Db:    w,
		//initloc:              stack(),
		doAllocZero:          w.doAllocZero,
		initialIndexName:     initialIndexName,
		DeleteEmptyContainer: w.DeleteEmptyContainer,
	}
	return
}

// Close shuts down the LMDB database.
func (w *LMDBWrapper) Close() (err error) {
	w.muDb.Lock()
	defer w.muDb.Unlock()
	if !w.closed {
		w.reg.unregister(w)
		w.halt.ReqStop.Close()
		w.closed = true
		w.writer.Stop()
		for _, reader := range w.readers {
			reader.Stop()
		}
	}
	w.env.CloseDBI(w.dbi)
	return nil
}

// LMDBTx wraps a lmdb.Txn and provides the Tx interface
// method implementations.
// The methods on LMDBTx are thread-safe, and can be called
// from different goroutines.
type LMDBTx struct {

	// mu serializes lmdb operations on this single txn instance.
	//
	// reference: https://godoc.org/github.com/dgraph-io/lmdb
	// "Running [two separate -jea] transactions concurrently is OK. However, a
	// transaction itself isn't thread safe, and should only
	// be run serially. It doesn't matter if a transaction is
	// created by one goroutine and passed down to other, as
	// long as the Txn APIs are called serially."
	mu sync.Mutex
	sn int64 // serial number

	write bool
	dbi   lmdb.DBI
	Db    *LMDBWrapper
	tx    *lmdb.Txn

	opcount int

	//initloc string // stack trace of where we were initially created.

	doAllocZero bool

	// for tracking txn boundary issues, track all the memory
	// that we deploy for roaring containers, and zero it on
	// transaction commit/rollback.
	acMu          sync.Mutex // protect ourAllocs and ourContainers
	ourAllocs     [][]byte
	ourContainers []*roaring.Container

	initialIndexName string

	DeleteEmptyContainer bool

	unlocked bool // runtime.UnlockOSThread has been done.
}

func (tx *LMDBTx) Type() string {
	return LmdbTxn
}

func (tx *LMDBTx) UseRowCache() bool {
	return true
}

// overWriteOurAllocs provides detection of memory
// access outside the transactional context, similar to the
// old school electric fence techniques but without setting
// memory mappings to read-only... instead we just zero
// out the memory allocated to roaring containers by a
// transaction after the commit or rollback. This,
// hopefully, will cause some downstream confusion and
// test failures, which we can use to locate who has been
// holding on to memory they should have copied prior
// to transaction commit.
func (tx *LMDBTx) overWriteOurAllocs() {

	tx.acMu.Lock()
	defer tx.acMu.Unlock()
	for _, s := range tx.ourAllocs {

		// The Go compiler recognizes the following pattern and inserts
		// an efficient memclr instruction.
		// See https://github.com/golang/go/issues/5373
		// and https://codereview.appspot.com/137880043
		for i := range s {
			s[i] = 0
			// or
			// Seebs suggested we might see even more crashes :)
			// but since it will be slow (no memclr), we'll leave the default 0 for now.
			//s[i] = -2
		}
	}
	// keep this around if we need to activate out-of-mmap memory access again.
	//for _, v := range tx.ourContainers {
	//v.Invalid = true
	//v.Tx = tx
	//}
}

// Pointer gives us a memory address for the underlying transaction for debugging.
// It is public because we use it in roaring to report invalid container memory access
// outside of a transaction.
func (tx *LMDBTx) Pointer() string {
	return fmt.Sprintf("%p", tx)
}

// Rollback rolls back the transaction.
func (tx *LMDBTx) Rollback() {
	tx.mu.Lock() // hung here? on defer on panic?
	defer tx.mu.Unlock()

	//pp("LMDBTx.Rollback p=%p, its: '%v' initloc: '%v',\n rollbackloc:'%v'", tx, tx.Db.UnprotectedListOpenItAsString(), tx.initloc, stack())
	tx.tx.Abort() // must hold tx.mu mutex lock

	tx.Db.muOpenTxIt.Lock()
	delete(tx.Db.openTx, tx)
	tx.Db.muOpenTxIt.Unlock()

	if tx.doAllocZero {
		// and clear our allocs, to find code using them outside of a txn.
		tx.overWriteOurAllocs()
	}
	if !tx.unlocked {
		//runtime.UnlockOSThread()
		tx.unlocked = true
	}
	//vv("done rolling back LMDBTx sn=%v", tx.sn)
}

// Commit commits the transaction to permanent storage.
// Commits can handle up to 100k updates to fragments
// at once, but not more. This is a LMDBDB imposed limit.
func (tx *LMDBTx) Commit() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	tx.Db.muOpenTxIt.Lock()
	delete(tx.Db.openTx, tx)
	tx.Db.muOpenTxIt.Unlock()

	//pp("LMDBTx.Commit (write:%v) p=%p, stackID=%x openit: '%v' initloc: '%v', commitloc:\n%v", tx.write, tx, stackID, tx.Db.UnprotectedListOpenItAsString(), tx.initloc, stack())

	err := tx.tx.Commit() // must hold tx.mu mutex lock
	panicOn(err)

	if tx.doAllocZero {
		tx.overWriteOurAllocs()
	}
	if !tx.unlocked {
		//runtime.UnlockOSThread()
		tx.unlocked = true
	}
	//vv("done committing LMDBTx sn=%v", tx.sn)
	return err
}

// Readonly returns true iff the LMDBTx is read-only.
func (tx *LMDBTx) Readonly() bool {
	return !tx.write
}

// RoaringBitmap returns the roaring.Bitmap for all bits in the fragment.
func (tx *LMDBTx) RoaringBitmap(index, field, view string, shard uint64) (*roaring.Bitmap, error) {

	return tx.OffsetRange(index, field, view, shard, 0, 0, LeftShifted16MaxContainerKey)
}

// Container returns the requested roaring.Container, selected by fragment and ckey
func (tx *LMDBTx) Container(index, field, view string, shard uint64, ckey uint64) (c *roaring.Container, err error) {

	// values returned from Get() are only valid while the transaction
	// is open. If you need to use a value outside of the transaction then
	// you must use copy() to copy it to another byte slice.
	// BUT here we are already inside the Txn.

	bkey := badgerKey(index, field, view, shard, ckey)
	tx.mu.Lock()
	//var item *lmdb.Item
	//item, err = tx.tx.Get(bkey)

	v, err := tx.tx.Get(tx.dbi, bkey)
	tx.mu.Unlock()

	if lmdb.IsNotFound(err) {
		// Seems crazy, but we, for now at least,
		// match what RoaringTx does by returning nil, nil.
		return nil, nil
	} else {
		if err != nil {
			vv("unexpected error on Container for bkey = '%v'; err='%v' ignoring for now TODO fix me", string(bkey), err)
			//panicOn(err) // mdb_get: invalid argument
			return nil, nil
		}
	}
	n := len(v)
	if n > 0 {
		c = tx.toContainer(v[n-1], v[0:(n-1)])
	}
	return
}

// PutContainer stores rc under the specified fragment and container ckey.
func (tx *LMDBTx) PutContainer(index, field, view string, shard uint64, ckey uint64, rc *roaring.Container) error {

	bkey := badgerKey(index, field, view, shard, ckey)
	var by []byte

	ct := roaring.ContainerType(rc)

	switch ct {
	case containerArray:
		by = fromArray16(roaring.AsArray(rc))
	case containerBitmap:
		by = fromArray64(roaring.AsBitmap(rc))
	case containerRun:
		by = fromInterval16(roaring.AsRuns(rc))
	case containerNil:
		panic("wat? nil container is unexpected, no?!?")
	default:
		panic(fmt.Sprintf("unknown container type: %v", ct))
	}
	tx.mu.Lock()
	err := tx.tx.Put(tx.dbi, bkey, append(by, ct), 0) // TODO: this might make a copy; can meta byte be stored elsewhere?
	tx.mu.Unlock()
	//panicOn(err) // mdb_put: invalid argument

	// TODO(jea): need to handle?
	//	lmdb.TxnFull
	//	lmdb.CursorFull
	//	lmdb.PageFull
	return err
}

// RemoveContainer deletes the container specified by the shard and container key ckey
func (tx *LMDBTx) RemoveContainer(index, field, view string, shard uint64, ckey uint64) error {
	bkey := badgerKey(index, field, view, shard, ckey)
	tx.mu.Lock()
	err := tx.tx.Del(tx.dbi, bkey, nil)
	tx.mu.Unlock()
	if lmdb.IsNotFound(err) {
		return nil
	}
	return err
}

// Add sets all the a bits hot in the specified fragment.
func (tx *LMDBTx) Add(index, field, view string, shard uint64, batched bool, a ...uint64) (changeCount int, err error) {

	// pure hack to match RoaringTx
	defer func() {
		if !batched {
			if changeCount > 0 {
				changeCount = 1
			}
		}
	}()

	// TODO: optimization: group 'a' elements into their containers,
	// and then do all the Adds on that
	// container at once, so we don't retrieve a container per bit.
	// (maybe, for example, using ImportRoaringBits with clear=false).

	for _, v := range a {
		hi, lo := highbits(v), lowbits(v)

		var rct *roaring.Container
		rct, err = tx.Container(index, field, view, shard, hi)
		panicOn(err)
		if err != nil {
			return 0, err
		}
		chng := false
		// TODO optimization: set all the bits in the current container at once. group by container first.
		rc1, chng := rct.Add(lo)
		panicOn(err)
		if chng {
			changeCount++
		}
		if err != nil {
			return changeCount, err
		}
		err = tx.PutContainer(index, field, view, shard, hi, rc1)
		//panicOn(err)
	}
	return
}

// Remove clears all the specified a bits in the chosen fragment.
func (tx *LMDBTx) Remove(index, field, view string, shard uint64, a ...uint64) (changeCount int, err error) {

	// TODO: optimization: group 'a' elements into their containers,
	// and then do all the Removes on that
	// container at once, so we don't retrieve a container per bit.
	// (maybe, for example, using ImportRoaringBits with clear=true).
	for _, v := range a {
		hi, lo := highbits(v), lowbits(v)

		var rct *roaring.Container
		rct, err = tx.Container(index, field, view, shard, hi)
		panicOn(err)
		if err != nil {
			return 0, err
		}
		chng := false
		rc1, chng := rct.Remove(lo)
		panicOn(err)
		if chng {
			changeCount++
		}
		if err != nil {
			return changeCount, err
		}
		if rc1.N() == 0 {
			err = tx.RemoveContainer(index, field, view, shard, hi)
			if err != nil {
				//vv("err = '%v'", err)
				return
			}
		} else {
			err = tx.PutContainer(index, field, view, shard, hi, rc1)
			panicOn(err)
		}
	}
	return
}

// Contains returns exists true iff the bit chosen by key is
// hot (set to 1) in specified fragment.
func (tx *LMDBTx) Contains(index, field, view string, shard uint64, key uint64) (exists bool, err error) {

	lo, hi := lowbits(key), highbits(key)
	bkey := badgerKey(index, field, view, shard, hi)
	tx.mu.Lock()
	var v []byte
	v, err = tx.tx.Get(tx.dbi, bkey)
	tx.mu.Unlock()
	if lmdb.IsNotFound(err) {
		//vv("Contains did not find bkey '%v'", string(bkey))
		return false, nil
	}
	if err != nil {
		return false, err
	}
	n := len(v)
	if n > 0 {
		c := tx.toContainer(v[n-1], v[0:(n-1)])
		exists = c.Contains(lo)
	}
	return exists, err
}

func (tx *LMDBTx) SliceOfShards(index, field, view, optionalViewPath string) (sliceOfShards []uint64, err error) {

	prefix := badgerAllShardPrefix(index, field, view)

	bi := NewLMDBIterator(tx, prefix)
	defer bi.Close()
	//	bi.Seek(prefix)
	//	if !bi.cur.Valid() {
	//	return
	//}

	lastShard := uint64(0)
	firstDone := false
	for bi.Next() {
		shard := shardFromBadgerKey(bi.lastKey)
		if firstDone {
			if shard != lastShard {
				sliceOfShards = append(sliceOfShards, shard)
			}
			lastShard = shard
		} else {
			// first time
			lastShard = shard
			firstDone = true
			sliceOfShards = append(sliceOfShards, shard)
		}

	}
	return
}

// key is the container key for the first roaring Container
// roaring docs: Iterator returns a ContainterIterator which *after* a call to Next(), a call to Value() will
// return the first container at or after key. found will be true if a
// container is found at key.
//
// LMDBTx notes: We auto-stop at the end of this shard, not going beyond.
func (tx *LMDBTx) ContainerIterator(index, field, view string, shard uint64, firstRoaringContainerKey uint64) (citer roaring.ContainerIterator, found bool, err error) {

	// needle example: "idx:'i';fld:'f';vw:'v';shd:'00000000000000000000';key@00000000000000000000"
	needle := badgerKey(index, field, view, shard, firstRoaringContainerKey)

	// prefix example: "idx:'i';fld:'f';vw:'v';shard:'00000000000000000000';key@"
	prefix := badgerPrefix(index, field, view, shard)

	bi := NewLMDBIterator(tx, prefix)
	ok := bi.Seek(needle)
	if !ok {
		////vv("ContainerIterator not ok on seek to needle '%v'; bi='%#v'", string(needle), bi)
		return bi, false, nil
	}
	//vv("ContainerIterator IS ok on seek to needle '%v'", string(needle))

	//	if !bi.ValidForPrefix(prefix) {
	//	return bi, false, nil
	//}

	// have to compare b/c lmdb might give us valid iterator
	// that is past our needle if needle isn't present.
	return bi, bytes.Equal(bi.lastKey, needle), nil
}

// LMDBIterator is the iterator returned from a LMDBTx.ContainerIterator() call.
// It implements the roaring.ContainerIterator interface.
type LMDBIterator struct {
	tx  *LMDBTx
	cur *lmdb.Cursor
	dbi lmdb.DBI

	prefix []byte
	seekto []byte

	// seen counts how many Next() calls we have seen.
	// It is used to match roaring.ContainerIterator semantics.
	// Also useful for testing.
	seen int

	lastKey      []byte
	lastVal      []byte // *roaring.Container
	lastOK       bool
	lastConsumed bool
}

// NewLMDBIterator creates an iterator on tx that will
// only return badgerKeys that start with prefix.
func NewLMDBIterator(tx *LMDBTx, prefix []byte) (bi *LMDBIterator) {
	cur, err := tx.tx.OpenCursor(tx.dbi)
	panicOn(err)

	bi = &LMDBIterator{
		dbi:    tx.dbi,
		tx:     tx,
		cur:    cur,
		prefix: prefix,
	}
	return
}

// Close tells the database and transaction that the user is done
// with the iterator.
// From the lmdb docs: It is important to call this when you're done with iteration.
// else you will get an error on tx.Discard()/Commit().
func (bi *LMDBIterator) Close() {
	bi.cur.Close()
}

// Valid returns false if there are no more values in the iterator's range.
func (bi *LMDBIterator) Valid() bool {
	return bi.lastOK
}

// Seek allows the iterator to start at needle instead of the global begining.
func (bi *LMDBIterator) Seek(needle []byte) (ok bool) {
	//vv("Seek needle '%v'", string(needle))

	bi.seen++ // if ommited, red TestLMDB_ContainerIterator_empty_iteration_loop() in lmdb_test.go.

	getflag := uint(lmdb.SetRange)
	k, v, err := bi.cur.Get(needle, nil, getflag)

	if lmdb.IsNotFound(err) {
		bi.lastKey = nil
		bi.lastVal = nil
		bi.lastOK = false
		bi.lastConsumed = false
		return false
	}
	if len(bi.prefix) > 0 {
		ok = bytes.HasPrefix(k, bi.prefix)
		if !ok {
			bi.lastKey = nil
			bi.lastVal = nil
			bi.lastOK = false
			bi.lastConsumed = false
			return false
		}
	}
	if len(k) == 0 {
		bi.lastKey = nil
		bi.lastVal = nil
		bi.lastOK = false
		bi.lastConsumed = false
		return false
	}

	bi.lastKey = k
	bi.lastVal = v
	if len(v) == 0 {
		// actually under !tx.DeleteEmptyContainer, we can have empty containers.

		//vv("Seek got len v == 0, k='%v', needle='%v'; here is Dump:", string(k), string(needle))
		//bi.tx.Dump()
		//vv("done with dump.")

		// lmdb.go:1008 2020-08-09T16:39:21.228243-05:00 done with dump.
		// panic: len v should not be zero here; for found needle='idx:'valck';fld:'f';vw:'bsig_f';shd:'00000000000000000041';ckey@00000000000000000016' / k='idx:'valck';fld:'f';vw:'bsig_f';shd:'00000000000000000041';ckey@00000000000000000032'
		// we see in the Dump shard 41, ckey 32; but not shard 41 ckey 16

		// repull with the key k we *did* get back
		var k2, v2 []byte
		k2, v2, err = bi.cur.Get(k, nil, getflag)
		if err != nil {
			panic(fmt.Sprintf("repull should not error, since we got key k='%v' already!", string(k)))
		}
		if string(k2) != string(k) {
			panic(fmt.Sprintf("repull should gives same k2 back, since we got key k='%v' already! k2='%v'", string(k), string(k2)))
		}
		if len(v2) > 0 {
			vv("good, v2 had data on repull.")
			bi.lastVal = v2
		} else {
			//panic(fmt.Sprintf("len v should not be zero here; for found needle='%v' / k='%v'", string(needle), string(k)))

			// just bail
			bi.lastKey = nil
			bi.lastVal = nil
			bi.lastOK = false
			bi.lastConsumed = false
			return false
		}
	}
	bi.lastOK = true
	bi.lastConsumed = false

	return true
}

func (bi *LMDBIterator) ValidForPrefix(prefix []byte) bool {
	if !bi.lastOK {
		return false
	}
	if len(bi.prefix) == 0 {
		return true
	}
	return bytes.HasPrefix(bi.lastKey, bi.prefix)
}

func (bi *LMDBIterator) String() (r string) {
	return fmt.Sprintf("LMDBIterator{prefix: '%v', seekto: '%v', seen:%v, lastKey:'%v', lastOK:%v, lastConsumed:%v}", string(bi.prefix), string(bi.seekto), bi.seen, string(bi.lastKey), bi.lastOK, bi.lastConsumed)
}

// Next advances the iterator.
func (bi *LMDBIterator) Next() (ok bool) {
	//vv("top of LMDBIterator.Next(); bi = '%v'", bi)
	//	defer func() {
	//	vv("LMDBIterator.Next() returning ok='%v'; bi = '%v'", ok, bi)
	//	}()
	if bi.lastOK && !bi.lastConsumed {
		//vv("have seek value that has not been consumed, consume it now.")
		bi.seen++
		bi.lastConsumed = true
		if len(bi.lastVal) == 0 {
			panic("bi.lastVal should not have len 0 if lastOK true")
		}
		return true
	}

	getflag := uint(lmdb.Next)
	prefix := bi.prefix

	if bi.seen == 0 {
		if len(bi.prefix) > 0 {
			getflag = lmdb.SetRange
		}
	} else {
		prefix = nil
	}

	bi.seen++
skipEmpty:
	k, v, err := bi.cur.Get(prefix, nil, getflag)
	////vv("Next Get() returned err='%v', k='%v'; stack=\n%v\n", err, string(k), stack())
	//vv("Next Get(getflag='%v' (reference lmdb.Next='%v' and lmdb.SetRange='%v'); prefix='%v') returned err='%v', k='%v'", getflag, uint(lmdb.Next), uint(lmdb.SetRange), string(prefix), err, string(k))
	if lmdb.IsNotFound(err) {
		bi.lastKey = nil
		bi.lastVal = nil
		bi.lastOK = false
		bi.lastConsumed = false
		return false
	}
	if len(bi.prefix) > 0 {
		ok = bytes.HasPrefix(k, bi.prefix)
		if !ok {
			bi.lastKey = nil
			bi.lastVal = nil
			bi.lastOK = false
			bi.lastConsumed = false
			return false
		}
	}
	bi.lastKey = k
	bi.lastVal = v
	if len(v) == 0 {
		// actually under !tx.DeleteEmptyContainer, we can have empty containers!
		goto skipEmpty

		//vv("v should not have len 0, in Next. k='%v'; here is Dump:", string(k))
		//bi.tx.Dump()
		//vv("done with Dump")
		//panic("v should not have len 0")
	}
	bi.lastOK = true
	bi.lastConsumed = true

	return true
}

// Value retrieves what is pointed at currently by the iterator.
func (bi *LMDBIterator) Value() (containerKey uint64, c *roaring.Container) {
	if !bi.lastOK {
		panic("bi.cur not valid")
	}
	containerKey = badgerKeyExtractContainerKey(bi.lastKey)

	v := bi.lastVal
	n := len(v)
	if n > 0 {
		c = bi.tx.toContainer(v[n-1], v[0:(n-1)])
	} else {
		panic("v should not be empty!")
	}
	return
}

// lmdbFinder implements roaring.IteratorFinder.
// It is used by LMDBTx.ForEach()
type lmdbFinder struct {
	tx        *LMDBTx
	index     string
	field     string
	view      string
	shard     uint64
	needClose []Closer
}

// FindIterator lets lmdbFinder implement the roaring.FindIterator interface.
func (bf *lmdbFinder) FindIterator(seek uint64) (roaring.ContainerIterator, bool) {
	a, found, err := bf.tx.ContainerIterator(bf.index, bf.field, bf.view, bf.shard, seek)
	panicOn(err)
	bf.needClose = append(bf.needClose, a)
	return a, found
}

// Close closes all bf.needClose listed Closers.
func (bf *lmdbFinder) Close() {
	for _, i := range bf.needClose {
		i.Close()
	}
}

// NewTxIterator returns a *roaring.Iterator that MUST have Close() called on it BEFORE
// the transaction Commits or Rollsback.
func (tx *LMDBTx) NewTxIterator(index, field, view string, shard uint64) *roaring.Iterator {
	bf := &lmdbFinder{tx: tx, index: index, field: field, view: view, shard: shard, needClose: make([]Closer, 0)}
	itr := roaring.NewIterator(bf)
	return itr
}

// ForEach applies fn to each bitmap in the fragment.
func (tx *LMDBTx) ForEach(index, field, view string, shard uint64, fn func(i uint64) error) error {
	itr := tx.NewTxIterator(index, field, view, shard)
	defer itr.Close()

	// Seek can create many container iterators, thus bf.Close() needClose list.
	itr.Seek(0)
	// v is the bit we are operating on.
	for v, eof := itr.Next(); !eof; v, eof = itr.Next() {
		if err := fn(v); err != nil {
			return err
		}
	}
	return nil
}

// ForEachRange applies fn on the selected range of bits on the chosen fragment.
func (tx *LMDBTx) ForEachRange(index, field, view string, shard uint64, start, end uint64, fn func(uint64) error) error {

	itr := tx.NewTxIterator(index, field, view, shard)
	defer itr.Close()

	itr.Seek(start)

	// v is the bit we are operating on.
	for v, eof := itr.Next(); !eof && v < end; v, eof = itr.Next() {
		if err := fn(v); err != nil {
			return err
		}
	}
	return nil
}

// Count operates on the full bitmap level, so it sums over all the containers
// in the bitmap.
func (tx *LMDBTx) Count(index, field, view string, shard uint64) (uint64, error) {

	a, found, err := tx.ContainerIterator(index, field, view, shard, 0)
	panicOn(err)
	defer a.Close()
	if !found {
		//vv("not found")
		return 0, nil
	}
	result := int32(0)
	//vv("a = '%v'", a.(*LMDBIterator).String())
	for a.Next() {
		ckey, cont := a.Value()
		//vv("on a.Next() loop... a.Value() got ckey '%v'", string(ckey))
		_ = ckey
		result += cont.N()
	}
	//vv("a.Next() returned false")
	return uint64(result), nil
}

// Max is the maximum bit-value in your bitmap.
// Returns zero if the bitmap is empty. Odd, but this is what roaring.Max does.
func (tx *LMDBTx) Max(index, field, view string, shard uint64) (uint64, error) {

	prefix := badgerPrefix(index, field, view, shard)
	seekto := badgerPrefix(index, field, view, shard+1)

	cur, err := tx.tx.OpenCursor(tx.dbi)
	panicOn(err)
	defer cur.Close()

	k, v, err := cur.Get(seekto, nil, lmdb.SetRange)
	_, _ = k, v
	if lmdb.IsNotFound(err) {
		// we have nothing >= seekto, but we might have stuff before it, and we'll wrap backwards.
		k, v, err = cur.Get(nil, nil, lmdb.Prev)
		if lmdb.IsNotFound(err) {
			// empty database
			return 0, nil
		}
	} else {
		// we found something >= seekto, so backup by 1.
		k, v, err = cur.Get(nil, nil, lmdb.Prev)
		if lmdb.IsNotFound(err) {
			// nothing before seekto
			return 0, nil
		}
	}

	// have something, are we in [prefix, seekto) ?
	cmp := bytes.Compare(k, prefix)
	if cmp >= 0 {
		// good, got max in k, v
	} else {
		return 0, nil // nothing in [prefix, seekto).
	}

	hb := badgerKeyExtractContainerKey(k)
	n := len(v)
	if n == 0 {
		return 0, nil
	}
	rc := tx.toContainer(v[n-1], v[0:(n-1)])

	lb := rc.Max()
	return hb<<16 | uint64(lb), nil
}

// Min returns the smallest bit set in the fragment. If no bit is hot,
// the second return argument is false.
func (tx *LMDBTx) Min(index, field, view string, shard uint64) (uint64, bool, error) {

	// Seek can create many container iterators, thus the bf.Close() needClose list.
	bf := &lmdbFinder{tx: tx, index: index, field: field, view: view, shard: shard, needClose: make([]Closer, 0)}
	defer bf.Close()
	itr := roaring.NewIterator(bf)

	itr.Seek(0)

	// v is the bit we are operating on.
	v, eof := itr.Next()
	if eof {
		return 0, false, nil
	}
	return v, true, nil
}

// UnionInPlace unions all the others Bitmaps into a new Bitmap, and then writes it to the
// specified fragment.
func (tx *LMDBTx) UnionInPlace(index, field, view string, shard uint64, others ...*roaring.Bitmap) error {

	rbm, err := tx.RoaringBitmap(index, field, view, shard)
	panicOn(err)

	rbm.UnionInPlace(others...)
	// iterate over the containers that changed within rbm, and write them back to disk.

	it, found := rbm.Containers.Iterator(0)
	_ = found // don't care about the value of found, because first containerKey might be > 0

	for it.Next() {
		containerKey, rc := it.Value()

		// TODO: only write the changed ones back, as optimization?
		//       Compare to ImportRoaringBits.
		err := tx.PutContainer(index, field, view, shard, containerKey, rc)
		panicOn(err)
	}
	return nil
}

// CountRange returns the count of hot bits in the start, end range on the fragment.
// roaring.countRange counts the number of bits set between [start, end).
func (tx *LMDBTx) CountRange(index, field, view string, shard uint64, start, end uint64) (n uint64, err error) {

	if start >= end {
		return 0, nil
	}

	skey := highbits(start)
	ekey := highbits(end)

	citer, found, err := tx.ContainerIterator(index, field, view, shard, skey)
	_ = found
	panicOn(err)

	defer citer.Close()

	// If range is entirely in one container then just count that range.
	if skey == ekey {
		citer.Next()
		_, c := citer.Value()
		return uint64(c.CountRange(int32(lowbits(start)), int32(lowbits(end)))), nil
	}

	for citer.Next() {
		k, c := citer.Value()
		if k < skey {
			citer.Close()
			panic(fmt.Sprintf("should be impossible for k(%v) to be less than skey(%v). tx p=%p", k, skey, tx))
		}

		// k > ekey handles the case when start > end and where start and end
		// are in different containers. Same container case is already handled above.
		if k > ekey {
			break
		}
		if k == skey {
			n += uint64(c.CountRange(int32(lowbits(start)), roaring.MaxContainerVal+1))
			continue
		}
		if k < ekey {
			n += uint64(c.N())
			continue
		}
		if k == ekey {
			n += uint64(c.CountRange(0, int32(lowbits(end))))
			break
		}
	}

	return n, nil
}

// OffsetRange creates a new roaring.Bitmap to return in other. For all the
// hot bits in [start, endx) of the chosen fragment, it stores
// them into other but with offset added to their bit position.
// The primary client is doing this, using ShardWidth, already; see
// fragment.rowFromStorage() in fragment.go. For example:
//
//    data, err := tx.OffsetRange(f.index, f.field, f.view, f.shard,
//                     f.shard*ShardWidth, rowID*ShardWidth, (rowID+1)*ShardWidth)
//                     ^ offset            ^ start           ^ endx
//
// The start and endx arguments are container keys that have been shifted left by 16 bits;
// their highbits() will be taken to determine the actual container keys. This
// is done to conform to the roaring.OffsetRange() argument convention.
//
func (tx *LMDBTx) OffsetRange(index, field, view string, shard, offset, start, endx uint64) (other *roaring.Bitmap, err error) {

	// roaring does these three checks in its OffsetRange
	if lowbits(offset) != 0 {
		panic("offset must not contain low bits")
	}
	if lowbits(start) != 0 {
		panic("range start must not contain low bits")
	}
	if lowbits(endx) != 0 {
		panic("range end must not contain low bits")
	}

	other = roaring.NewSliceBitmap()
	off := highbits(offset)
	hi0, hi1 := highbits(start), highbits(endx)

	needle := badgerKey(index, field, view, shard, hi0)
	prefix := badgerPrefix(index, field, view, shard)

	n2, pre2 := badgerKeyAndPrefix(index, field, view, shard, hi0)
	if string(n2) != string(needle) {
		panic(fmt.Sprintf("problem! n2(%v) != needle(%v), badgerKeyAndPrefix not consitent with badgerKey()", string(n2), string(needle)))
	}
	if string(pre2) != string(prefix) {
		panic(fmt.Sprintf("problem! pre2(%v) != prefix(%v), badgerKeyAndPrefix not consitent with badgerKey()", string(pre2), string(prefix)))
	}

	it := NewLMDBIterator(tx, prefix) // see OffsetRange() panic 'Only one iterator can be active at one time, for a RW txn
	defer it.Close()
	it.Seek(needle)
	for ; it.ValidForPrefix(prefix); it.Next() {
		//vv("through the look, it.lastKey '%v' must have been valid for prefix '%v'", string(it.lastKey), string(prefix))
		bkey := it.lastKey
		k := badgerKeyExtractContainerKey(bkey)

		// >= hi1 is correct b/c endx cannot have any lowbits set.
		if uint64(k) >= hi1 {
			break
		}
		destCkey := off + (k - hi0)

		v := it.lastVal
		n := len(v)
		if n == 0 {
			//vv("why is it.lastVal == v == nil for it.lastKey '%v' must have been valid for prefix '%v'", string(it.lastKey), string(prefix))
			continue
		}
		c := tx.toContainer(v[n-1], v[0:(n-1)])
		other.Containers.Put(destCkey, c.Freeze())
	}
	return other, nil
}

// IncrementOpN increments the tx opcount by changedN
func (tx *LMDBTx) IncrementOpN(index, field, view string, shard uint64, changedN int) {
	tx.opcount += changedN
}

// ImportRoaringBits handles deletes by setting clear=true.
// rowSet[rowID] returns the number of bit changed on that rowID.
func (tx *LMDBTx) ImportRoaringBits(index, field, view string, shard uint64, itr roaring.RoaringIterator, clear bool, log bool, rowSize uint64, data []byte) (changed int, rowSet map[uint64]int, err error) {
	n := itr.Len()
	if n == 0 {
		return
	}
	rowSet = make(map[uint64]int)

	var currRow uint64

	var oldC *roaring.Container
	for itrKey, synthC := itr.NextContainer(); synthC != nil; itrKey, synthC = itr.NextContainer() {
		if rowSize != 0 {
			currRow = itrKey / rowSize
		}
		nsynth := int(synthC.N())
		if nsynth == 0 {
			continue
		}
		// INVAR: nsynth > 0

		oldC, err = tx.Container(index, field, view, shard, itrKey)
		panicOn(err)
		if err != nil {
			return
		}

		if oldC == nil || oldC.N() == 0 {
			// no container at the itrKey in lmdb (or all zero container).
			if clear {
				// changed of 0 and empty rowSet is perfect, no need to change the defaults.
				continue
			} else {

				changed += nsynth
				rowSet[currRow] += nsynth

				err = tx.PutContainer(index, field, view, shard, itrKey, synthC)
				if err != nil {
					return
				}
				continue
			}
		}

		if clear {
			existN := oldC.N() // number of bits set in the old container
			newC := oldC.Difference(synthC)

			// update rowSet and changes
			if newC.N() == existN {
				// INVAR: do changed need adjusting? nope. same bit count,
				// so no change could have happened.
				continue
			} else {
				changes := int(existN - newC.N())
				changed += changes
				rowSet[currRow] -= changes

				if tx.DeleteEmptyContainer && newC.N() == 0 {
					err = tx.RemoveContainer(index, field, view, shard, itrKey)
					if err != nil {
						return
					}
					continue
				}
				err = tx.PutContainer(index, field, view, shard, itrKey, newC)
				if err != nil {
					return
				}
				continue
			}
		} else {
			// setting bits

			existN := oldC.N()
			if existN == roaring.MaxContainerVal+1 {
				// completely full container already, set will do nothing. so changed of 0 default is perfect.
				continue
			}
			if existN == 0 {
				// can nsynth be zero? No, because of the continue/invariant above where nsynth > 0
				changed += nsynth
				rowSet[currRow] += nsynth
				err = tx.PutContainer(index, field, view, shard, itrKey, synthC)
				if err != nil {
					return
				}
				continue
			}

			newC := roaring.Union(oldC, synthC) // UnionInPlace was giving us crashes on overly large containers.

			if roaring.ContainerType(newC) == containerBitmap {
				newC.Repair() // update the bit-count so .n is valid. b/c UnionInPlace doesn't update it.
			}
			if newC.N() != existN {
				changes := int(newC.N() - existN)
				changed += changes
				rowSet[currRow] += changes

				err = tx.PutContainer(index, field, view, shard, itrKey, newC)
				if err != nil {
					panicOn(err)
					return
				}
				continue
			}
		}
	}
	return
}

func (tx *LMDBTx) toContainer(typ byte, v []byte) (r *roaring.Container) {

	if len(v) == 0 {
		return nil
	}

	var w []byte
	if tx.doAllocZero {
		// Do electric fence-inspired bad-memory read detection.
		//
		// The v []byte lives in LMDBDB's memory-mapped vlog-file,
		// and LMDB will recycle it after tx ends with rollback or commit.
		//
		// Problem is, at least some operations were not respecting transaction boundaries.
		// This technique helped us find them. The rowCache was an example.
		//
		// See the global const DetectMemAccessPastTx
		// at the top of txfactory.go to activate/deactivate this.
		//
		// Seebs suggested this nice variation: we could use individual mmaps for these
		// copies, which would be unusable in production, but workable for testing, and then unmap them,
		// which would get us probable segfaults on future accesses to them.
		//
		// The go runtime also has an -efence flag which may be similarly useful if really pressed.
		//
		w = make([]byte, len(v))
		copy(w, v)

		// register w so we can catch out-of-tx memory access
		tx.acMu.Lock()
		defer tx.acMu.Unlock()
		tx.ourAllocs = append(tx.ourAllocs, w)
	} else {
		w = v
	}

	switch typ {
	case containerArray:
		c := roaring.NewContainerArray(toArray16(w))
		if tx.doAllocZero {
			// tx.acMu was acquired above, and Unlock deferred.
			tx.ourContainers = append(tx.ourContainers, c)
		}
		return c
	case containerBitmap:
		c := roaring.NewContainerBitmap(-1, toArray64(w))
		if tx.doAllocZero {
			// tx.acMu was acquired above, and Unlock deferred.
			tx.ourContainers = append(tx.ourContainers, c)
		}
		return c
	case containerRun:
		c := roaring.NewContainerRun(toInterval16(w))
		if tx.doAllocZero {
			// tx.acMu was acquired above, and Unlock deferred.
			tx.ourContainers = append(tx.ourContainers, c)
		}
		return c
	default:
		panic(fmt.Sprintf("unknown container: %v", typ))
	}
}

// StringifiedLMDBKeys returns a string with all the container
// keys available in lmdb.
func (w *LMDBWrapper) StringifiedLMDBKeys(optionalUseThisTx Tx) (r string) {
	if optionalUseThisTx == nil {
		tx := w.NewLMDBTx(!writable, "<StringifiedLMDBKeys>")
		defer tx.Rollback()
		r = stringifiedLMDBKeysTx(tx)
		return
	}

	btx, ok := optionalUseThisTx.(*LMDBTx)
	if !ok {
		return fmt.Sprintf("<not-a-LMDBTx-in-StringifiedLMDBKeys-was-%T>", optionalUseThisTx)
	}
	r = stringifiedLMDBKeysTx(btx)
	return
}

// countBitsSet returns the number of bits set (or "hot") in
// the roaring container value found by the badgerKey()
// formatted bkey.
func (tx *LMDBTx) countBitsSet(bkey []byte) (n int) {

	v, err := tx.tx.Get(tx.dbi, bkey)
	if lmdb.IsNotFound(err) {
		// some queries bkey may not be present! don't panic.
		//panic(fmt.Sprintf("lmdb did not have value for bkey = '%v'", string(bkey)))
		return 0
	}
	panicOn(err)

	n = len(v)
	if n > 0 {
		rc := tx.toContainer(v[n-1], v[0:(n-1)])
		n = int(rc.N())
	}
	return
}

func (tx *LMDBTx) Dump() {
	fmt.Printf("%v\n", stringifiedLMDBKeysTx(tx))
}

// stringifiedLMDBKeysTx reports all the lmdb keys and a
// corresponding blake3 hash viewable by txn within the entire
// lmdb database.
// It also reports how many bits are hot in the roaring container
// (how many bits are set, or 1 rather than 0).
//
// By convention, we must return the empty string if there
// are no keys present. The tests use this to confirm
// an empty database.
func stringifiedLMDBKeysTx(tx *LMDBTx) (r string) {

	r = "allkeys:[\n"
	it := NewLMDBIterator(tx, nil)
	defer it.Close()
	any := false
	for it.Next() {
		any = true

		bkey := it.lastKey
		key := string(bkey)
		ckey := badgerKeyExtractContainerKey(bkey)
		hash := ""
		srbm := ""
		v := it.lastVal
		n := len(v)
		if n == 0 {
			panic("should not have empty v here")
		}
		hash = blake3sum16(v[0:(n - 1)])
		ct := tx.toContainer(v[n-1], v[0:(n-1)])
		cts := roaring.NewSliceContainers()
		cts.Put(ckey, ct)
		rbm := &roaring.Bitmap{Containers: cts}
		srbm = bitmapAsString(rbm)

		r += fmt.Sprintf("%v -> %v (%v hot)\n", key, hash, tx.countBitsSet(bkey))
		r += "          ......." + srbm + "\n"
	}
	r += "]\n   all-in-blake3:" + blake3sum16([]byte(r))

	if !any {
		return ""
	}
	return "lmdb-" + r
}

func (w *LMDBWrapper) DeleteField(index, field, fieldPath string) error {

	// under blue-green roaring_lmdb, the directory will not be found, b/c roaring will have
	// already done the os.RemoveAll().	BUT, RemoveAll returns nil error in this case. Docs:
	// "If the path does not exist, RemoveAll returns nil (no error)"
	err := os.RemoveAll(fieldPath)
	if err != nil {
		return errors.Wrap(err, "removing directory")
	}
	prefix := badgerFieldPrefix(index, field)
	return w.DeletePrefix(prefix)
}

func (w *LMDBWrapper) DeleteFragment(index, field, view string, shard uint64, frag interface{}) error {
	prefix := badgerPrefix(index, field, view, shard)
	return w.DeletePrefix(prefix)
}

func (w *LMDBWrapper) DeletePrefix(prefix []byte) error {

	tx := w.NewLMDBTx(writable, w.name)

	// NewLMDBTx will grab these, so don't lock until after it.
	w.muDb.Lock()
	defer w.muDb.Unlock()

	bi := NewLMDBIterator(tx, prefix)

	for bi.Next() {
		err := bi.cur.Del(0)
		panicOn(err)
	}
	bi.Close()

	err := tx.Commit()
	panicOn(err)

	return nil
}

func (tx *LMDBTx) RoaringBitmapReader(index, field, view string, shard uint64, fragmentPathForRoaring string) (r io.ReadCloser, sz int64, err error) {

	rbm, err := tx.RoaringBitmap(index, field, view, shard)
	if err != nil {
		return nil, -1, errors.Wrap(err, "RoaringBitmapReader RoaringBitmap")
	}
	var buf bytes.Buffer
	sz, err = rbm.WriteTo(&buf)
	if err != nil {
		return nil, -1, errors.Wrap(err, "RoaringBitmapReader rbm.WriteTo(buf)")
	}
	return ioutil.NopCloser(&buf), sz, err
}
