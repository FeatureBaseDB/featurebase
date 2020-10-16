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

// +build amd64

package pilosa

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/glycerine/lmdb-go/lmdb"
	"github.com/pilosa/pilosa/v2/hash"
	"github.com/pilosa/pilosa/v2/rbf"
	"github.com/pilosa/pilosa/v2/roaring"
	"github.com/pilosa/pilosa/v2/txkey"
	"github.com/pkg/errors"
)

const TxInitialMmapSize = 4 << 30 // 4GB

// Linux builds note:
//
// This setting of -DMDB_USE_SYSV_SEM=1 is required in the cgo build
// under Linux to avoid a random deadlock occurance deep inside
// the LMDB C code.
//
// Futher documentation here: https://github.com/bmatsuo/lmdb-go/issues/94
//
// The effect of the -D define above is to that the LMDB C code then
// uses SysV semaphores and avoids POSIX thread-local semaphores. SysV semaphores
// are stored system-wide. POSIX semaphores are kept on thread-local storage,
// which is apparently problematic.

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

func (r *lmdbRegistrar) Size() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	nmp := len(r.mp)
	npa := len(r.path2db)
	if nmp != npa {
		panic(fmt.Sprintf("nmp=%v, vs npa=%v", nmp, npa))
	}
	return nmp
}

var globalLMDBReg *lmdbRegistrar = newLMDBTestRegistrar()

var globalNextTxSnLMDB int64

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
	short := true
	globalLMDBReg.mu.Lock()
	defer globalLMDBReg.mu.Unlock()
	for w := range globalLMDBReg.mp {
		AlwaysPrintf("this lmdb path='%v' has: \n%v\n", w.path, w.StringifiedLMDBKeys(nil, short))
	}
}

// lmdbPath is a helper for determining the full directory
// in which the lmdb database will be stored.
func lmdbPath(path string) string {
	if !strings.HasSuffix(path, "-lmdb@") {
		return path + "-lmdb@"
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
func (r *lmdbRegistrar) OpenDBWrapper(path0 string, doAllocZero bool) (DBWrapper, error) {
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
	defer runtime.UnlockOSThread()

	const MaxReaders = 126 // default is 126
	env, err := lmdb.NewEnvMaxReaders(MaxReaders)
	panicOn(err)

	err = env.SetMaxDBs(1)
	panicOn(err)
	//err = env.SetMapSize(256 << 30) // 256GB
	err = env.SetMapSize(TxInitialMmapSize)
	panicOn(err)

	panicOn(os.MkdirAll(filepath.Dir(path), 0755))

	//flags := uint(lmdb.NoReadahead | lmdb.NoSubdir)
	//flags := uint(lmdb.NoSubdir) // no difference without the No.Readahead on ./query.
	// NoReadahead should be better for random workloads or those bigger than memory,
	// as it avoids loading extra pages which then evict pages you are using.
	//flags := uint(lmdb.NoReadahead)
	flags := uint(0)

	// unsafe, but get upper bound on performance.
	//	WriteMap    = C.MDB_WRITEMAP   // Use a writable memory map.
	//	NoMetaSync  = C.MDB_NOMETASYNC // Don't fsync metapage after commit.
	//	NoSync      = C.MDB_NOSYNC     // Don't fsync after commit.
	// flags = flags | lmdb.WriteMap | lmdb.NoMetaSync | lmdb.NoSync // about the same speed
	// flags = flags | lmdb.NoMetaSync | lmdb.NoSync // slows things down

	// TODO(jea): we got an odd segfault in the roaring/ pkg when we went to read-only mmap
	// with the row-cache off; probably means we were trying to write to mmap-ed
	// memory. Investigate at some point. reference: 2a77b25d ja/write_map_prevents_segfault
	//
	// Update: added COW to roaring/roaring.go:3282  func (c *Container) arrayRemove(),
	// seems to fix the issue.
	//
	//flags = flags | lmdb.WriteMap // seems faster than without: or maybe not. not sure.
	// kRemove  N=  710401   avg/op:      7.714µs   sd:      27.83µs  total: 5.480656859s
	//    kAdd  N=  722835   avg/op:      9.096µs   sd:    105.787µs  total: 6.575497725s

	// ACI not ACID at the moment; no durability
	flags = flags |
		lmdb.NoMemInit | // Disable LMDB memory initialization

		// Note that lmdb.WriteMap requests a big, writable, memory map.
		// On my darwin/OSX laptop with 16GB ram, for instance, we
		// can have difficulty obtaining this, resulting in
		//   panic: mdb_env_open: no space left on device
		lmdb.WriteMap | // Use a writable memory map.

		// default ACI (not Durable) transactions; 300% faster write speed results.
		lmdb.NoMetaSync | // Don't fsync metapage after commit.
		lmdb.NoSync | // Don't fsync after commit.
		lmdb.MapAsync // Flush asynchronously when using the WriteMap flag.

	if !DirExists(path) {
		panicOn(os.MkdirAll(path, 0755))
	}
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

	w = &LMDBWrapper{
		name:        name,
		env:         env,
		reg:         r,
		path:        path,
		dbi:         dbi,
		doAllocZero: doAllocZero,
		openTx:      make(map[*LMDBTx]bool),
	}
	r.unprotectedRegister(w)

	return w, nil
}

func (w *LMDBWrapper) Path() string {
	return w.path
}

func (w *LMDBWrapper) HasData() (has bool, err error) {

	tx, err := w.NewTx(!writable, "", Txo{})
	if err != nil {
		return false, errors.Wrap(err, "HasData NewTx")
	}
	defer tx.Rollback()

	bi := NewLMDBIterator(tx.(*LMDBTx), nil)
	defer bi.Close()

	for bi.Next() {
		return true, nil
	}
	return false, nil
}

func (w *LMDBWrapper) CleanupTx(tx Tx) {
	// inlined into Rollback and Commit, so this is a no-op, just here to satisfy the interface.
}

func (tx *LMDBTx) IsDone() (done bool) {
	return atomic.LoadInt64(&tx.unlocked) == 1
}

func (w *LMDBWrapper) OpenListString() (r string) {

	list := w.listopen()
	if len(list) == 0 {
		return "<no open LMDBTx>"
	}
	for i, ltx := range list {
		if ltx.o.Write {
			r += fmt.Sprintf("[%v]write: _sn_ %v %v, \n", i, ltx.sn, ltx.o)
		} else {
			r += fmt.Sprintf("[%v]read : _sn_ %v %v, \n", i, ltx.sn, ltx.o)
		}
	}
	return
}

func (w *LMDBWrapper) listopen() (slc []*LMDBTx) {
	w.muDb.Lock()
	for v := range w.openTx {
		slc = append(slc, v)
	}
	w.muDb.Unlock()
	return
}

func (w *LMDBWrapper) OpenSnList() (slc []int64) {
	w.muDb.Lock()
	for v := range w.openTx {
		slc = append(slc, v.sn)
	}
	w.muDb.Unlock()
	return
}

var ErrShutdown = fmt.Errorf("shutting down")

// DeleteIndex deletes all the containers associated with
// the named index from the lmdb database.
func (w *LMDBWrapper) DeleteIndex(indexName string) error {

	// We use the apostrophie rune `'` to locate the end of the
	// index name in the key prefix, so we cannot allow indexNames
	// themselves to contain apostrophies.
	if strings.Contains(indexName, "/") {
		return fmt.Errorf("error: bad indexName `%v` in LMDBWrapper.DeleteIndex() call: indexName cannot contain '/'.", indexName)
	}
	prefix := txkey.IndexOnlyPrefix(indexName)
	return w.DeletePrefix(prefix)
}

// statically confirm that LMDBTx satisfies the Tx interface.
var _ Tx = (*LMDBTx)(nil)

// LMDBWrapper provides the NewTx() method.
type LMDBWrapper struct {
	env *lmdb.Env

	muDb sync.Mutex

	path string
	name string
	dbi  lmdb.DBI

	// track our registrar for Close / goro leak reporting purposes.
	reg *lmdbRegistrar

	// make LMDBWrapper.Close() idempotent, avoiding panic on double Close()
	closed bool

	// doAllocZero sets the corresponding flag on all new LMDBTx.
	// When doAllocZero is true, we zero out any data from lmdb
	// after transcation commit and rollback. This simulates
	// what would happen if we were to use the mmap-ed data
	// from lmdb directly. Currently we copy by default for
	// safety because otherwise TestAPI_ImportColumnAttrs sees
	// corrupted data.
	doAllocZero bool

	DeleteEmptyContainer bool

	openTx map[*LMDBTx]bool
}

func (w *LMDBWrapper) SetHolder(h *Holder) {
	// don't need it at the moment
	//w.h = h
}

// NewTxWRITE lets us see in the callstack dumps where the WRITE tx are.
// Can't have more than one active write per database, so the
// 2nd one will block until the first finishes.
func (w *LMDBWrapper) NewTxWRITE() (*lmdb.Txn, error) {
	lmdbTxn, err := w.env.BeginTxn(nil, 0)
	if err != nil {
		if w.env == nil || w.IsClosed() {
			return nil, fmt.Errorf("cannot call NewTxWRITE() on closed LMDB database: '%v'", err)
		}
		return nil, err
	}
	return lmdbTxn, nil
}

// NewTxREAD lets us see in the callstack dumps where the READ tx are.
func (w *LMDBWrapper) NewTxREAD() (*lmdb.Txn, error) {
	lmdbTxn, err := w.env.BeginTxn(nil, lmdb.Readonly)
	if err != nil {
		if w.env == nil || w.IsClosed() {
			return nil, fmt.Errorf("cannot call NewTxREAD() on closed LMDB database: '%v'", err)
		}
		return nil, err
	}
	return lmdbTxn, nil
}

// NewTx produces LMDB based ACID or ACI transactions. If
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
func (w *LMDBWrapper) NewTx(write bool, initialIndexName string, o Txo) (tx Tx, err error) {

	sn := atomic.AddInt64(&globalNextTxSnLMDB, 1)

	//vv("lmdb new tx _sn_ %v; openTx='%v', stack \n%v", sn, w.OpenListString(), stack())
	//vv("lmdb new (write=%v, shard=%v) tx _sn_ %v; openTx='%v'", write, o.Shard, sn, w.OpenListString())

	runtime.LockOSThread()

	var lmdbTxn *lmdb.Txn
	if write {
		// see the WRITE tx on the callstack.
		lmdbTxn, err = w.NewTxWRITE()
		if err != nil {
			return nil, err
		}
	} else {
		// see the READ tx on the callstack.
		lmdbTxn, err = w.NewTxREAD()
		if err != nil {
			return nil, err
		}
	}

	lmdbTxn.RawRead = true

	ltx := &LMDBTx{
		sn:                   sn,
		write:                write,
		tx:                   lmdbTxn,
		dbi:                  w.dbi,
		Db:                   w,
		frag:                 o.Fragment,
		doAllocZero:          w.doAllocZero,
		initialIndexName:     initialIndexName,
		DeleteEmptyContainer: w.DeleteEmptyContainer,
		o:                    o,
		gid:                  curGID(),
	}
	tx = ltx

	if isDebugRun {
		w.muDb.Lock()
		w.openTx[ltx] = true
		w.muDb.Unlock()
	}
	return
}

// Close shuts down the LMDB database.
func (w *LMDBWrapper) Close() (err error) {
	w.muDb.Lock()
	defer w.muDb.Unlock()
	if !w.closed {
		if isDebugRun {
			// complain if there are still Tx in flight, b/c otherwise we will see
			// the somewhat mysterious 'panic: should not be in ReadSlot.free() with slot still owned by gid=107043; refCount=1'
			if len(w.openTx) > 0 {
				AlwaysPrintf("error: cannot close LMDBWrapper with Tx still in flight.")
				return
			}
		}
		w.reg.unregister(w)
		w.closed = true
		w.env.CloseDBI(w.dbi)
		w.env.Close()
		w.env = nil
	}
	return nil
}

func (w *LMDBWrapper) IsClosed() (closed bool) {
	w.muDb.Lock()
	closed = w.closed
	w.muDb.Unlock()
	return
}

// LMDBTx wraps a lmdb.Txn and provides the Tx interface
// method implementations.
// The methods on LMDBTx are thread-safe, and can be called
// from different goroutines.
type LMDBTx struct {

	// mu serializes lmdb operations on this single txn instance.
	mu sync.Mutex
	sn int64 // serial number

	write bool
	dbi   lmdb.DBI
	Db    *LMDBWrapper
	tx    *lmdb.Txn
	frag  *fragment

	opcount int

	//initloc string // stack trace of where we were initially created.

	doAllocZero bool

	initialIndexName string

	DeleteEmptyContainer bool

	unlocked int64 // runtime.UnlockOSThread has been done if > 0

	o Txo

	// NewTx, write operations, Commit and/or Rollback must all take place on
	// the same gid and it must the runtime.LockOSThreaded first. Verify
	// that we are using the right goroutine in a debug build using the
	// gid, stored here, used for NewTx().
	gid uint64
}

// sanity check that database is open.
func (tx *LMDBTx) sanity() {
	if tx.Db.IsClosed() {
		panic("cannot operate on closed LMDB")
	}
}

// debugOnlyGidcheck is expected to be sort of slow. So once we are sure
// of correctness, turn it off.
// func (tx *LMDBTx) debugOnlyGidcheck() {
//
// 	// only applies to write txn. Reads should be able to share the txn.
// 	if tx.o.Write {
// 		cur := curGID()
// 		if cur != tx.gid {
// 			panic(fmt.Sprintf("must use write LMDBTx from same gid that created it! creator gid=%v, but user gid now =%v", tx.gid, cur))
// 		}
// 	}
//
// }

func (tx *LMDBTx) Group() *TxGroup {
	return tx.o.Group
}

func (tx *LMDBTx) Type() string {
	return LmdbTxn
}

func (tx *LMDBTx) UseRowCache() bool {
	return rbf.EnableRowCache
}

// Pointer gives us a memory address for the underlying transaction for debugging.
// It is public because we use it in roaring to report invalid container memory access
// outside of a transaction.
func (tx *LMDBTx) Pointer() string {
	return fmt.Sprintf("%p", tx)
}

const isDebugRun = false

// Rollback rolls back the transaction.
func (tx *LMDBTx) Rollback() {
	notDone := atomic.CompareAndSwapInt64(&tx.unlocked, 0, 1)
	if !notDone {
		return
	}
	//vv("lmdb rollback tx _sn_ %v; stack \n%v", tx.sn) // , stack())
	if isDebugRun {
		tx.sanity()
		tx.Db.muDb.Lock()
		delete(tx.Db.openTx, tx)
		tx.Db.muDb.Unlock()
	}

	tx.mu.Lock()
	defer tx.mu.Unlock()

	//tx.debugOnlyGidcheck()
	tx.tx.Abort() // must hold tx.mu mutex lock

	// use CAS above instead of testing a bool unlocked.
	runtime.UnlockOSThread()
	tx.o.dbs.Cleanup(tx)
}

// Commit commits the transaction to permanent storage.
// Commits can handle up to 100k updates to fragments
// at once, but not more. This is a LMDBDB imposed limit.
func (tx *LMDBTx) Commit() error {
	notDone := atomic.CompareAndSwapInt64(&tx.unlocked, 0, 1)
	if !notDone {
		return nil
	}
	//vv("lmdb commit tx _sn_ %v; stack \n%v", tx.sn, stack())
	if isDebugRun {
		tx.sanity()
		tx.Db.muDb.Lock()
		delete(tx.Db.openTx, tx)
		tx.Db.muDb.Unlock()
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()

	//tx.debugOnlyGidcheck()
	err := tx.tx.Commit() // must hold tx.mu mutex lock
	panicOn(err)

	// replace the if !tx.unlocked with the CAS on tx.unlocked above.
	runtime.UnlockOSThread()
	tx.o.dbs.Cleanup(tx)
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

	bkey := txkey.Key(index, field, view, shard, ckey)
	tx.mu.Lock()

	//tx.debugOnlyGidcheck()

	v, err := tx.tx.Get(tx.dbi, bkey)
	tx.mu.Unlock()

	if lmdb.IsNotFound(err) {
		// Seems crazy, but we, for now at least,
		// match what RoaringTx does by returning nil, nil.
		return nil, nil
	} else {
		if err != nil {
			panicOn(err)
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

	bkey := txkey.Key(index, field, view, shard, ckey)
	var by []byte

	ct := roaring.ContainerType(rc)

	switch ct {
	case roaring.ContainerArray:
		by = fromArray16(roaring.AsArray(rc))
	case roaring.ContainerBitmap:
		by = fromArray64(roaring.AsBitmap(rc))
	case roaring.ContainerRun:
		by = fromInterval16(roaring.AsRuns(rc))
	case roaring.ContainerNil:
		panic("wat? nil container is unexpected, no?!?")
	default:
		panic(fmt.Sprintf("unknown container type: %v", ct))
	}
	tx.mu.Lock()
	//tx.debugOnlyGidcheck()

	err := tx.tx.Put(tx.dbi, bkey, append(by, ct), 0) // TODO: this might make a copy; can meta byte be stored elsewhere?
	tx.mu.Unlock()

	// TODO(jea): need to handle?
	//	lmdb.TxnFull
	//	lmdb.CursorFull
	//	lmdb.PageFull
	return err
}

// RemoveContainer deletes the container specified by the shard and container key ckey
func (tx *LMDBTx) RemoveContainer(index, field, view string, shard uint64, ckey uint64) error {
	bkey := txkey.Key(index, field, view, shard, ckey)
	tx.mu.Lock()
	//tx.debugOnlyGidcheck()

	err := tx.tx.Del(tx.dbi, bkey, nil)
	tx.mu.Unlock()
	if lmdb.IsNotFound(err) {
		return nil
	}
	return err
}

// Add sets all the a bits hot in the specified fragment.
func (tx *LMDBTx) Add(index, field, view string, shard uint64, batched bool, a ...uint64) (changeCount int, err error) {
	return tx.addOrRemove(index, field, view, shard, batched, false, a...)
}

// Remove clears all the specified a bits in the chosen fragment.
func (tx *LMDBTx) Remove(index, field, view string, shard uint64, a ...uint64) (changeCount int, err error) {
	const batched = false
	const remove = true
	return tx.addOrRemove(index, field, view, shard, batched, remove, a...)
}

func (tx *LMDBTx) addOrRemove(index, field, view string, shard uint64, batched, remove bool, a ...uint64) (changeCount int, err error) {
	// pure hack to match RoaringTx
	defer func() {
		if !remove && !batched {
			if changeCount > 0 {
				changeCount = 1
			}
		}
	}()

	if len(a) == 0 {
		return 0, nil
	}

	// have to sort, b/c input is not always sorted.
	sort.Slice(a, func(i, j int) bool { return a[i] < a[j] })

	var lastHi uint64 = math.MaxUint64 // highbits is always less than this starter.
	var rc *roaring.Container
	var hi uint64
	var lo uint16

	for i, v := range a {

		hi, lo = highbits(v), lowbits(v)
		if hi != lastHi {
			// either first time through, or changed to a different container.
			// do we need put the last updated container now?
			if i > 0 {
				// not first time through, write what we got.
				if remove && (rc == nil || rc.N() == 0) {
					err = tx.RemoveContainer(index, field, view, shard, lastHi)
					panicOn(err)
				} else {
					err = tx.PutContainer(index, field, view, shard, lastHi, rc)
					panicOn(err)
				}
			}
			// get the next container
			rc, err = tx.Container(index, field, view, shard, hi)
			panicOn(err)
		} // else same container, keep adding bits to rct.
		chng := false
		// rc can be nil before, and nil after, in both Remove/Add below.
		// The roaring container add() and remove() methods handle this.
		if remove {
			rc, chng = rc.Remove(lo)
		} else {
			rc, chng = rc.Add(lo)
		}
		if chng {
			changeCount++
		}
		lastHi = hi
	}
	// write the last updates.
	if remove {
		if rc == nil || rc.N() == 0 {
			err = tx.RemoveContainer(index, field, view, shard, hi)
			panicOn(err)
		} else {
			err = tx.PutContainer(index, field, view, shard, hi, rc)
			panicOn(err)
		}
	} else {
		if rc == nil || rc.N() == 0 {
			panic("there should be no way to have an empty bitmap AFTER an Add() operation")
		}
		err = tx.PutContainer(index, field, view, shard, hi, rc)
		panicOn(err)
	}
	return
}

// Contains returns exists true iff the bit chosen by key is
// hot (set to 1) in specified fragment.
func (tx *LMDBTx) Contains(index, field, view string, shard uint64, key uint64) (exists bool, err error) {

	lo, hi := lowbits(key), highbits(key)
	bkey := txkey.Key(index, field, view, shard, hi)
	tx.mu.Lock()
	var v []byte

	//tx.debugOnlyGidcheck()

	v, err = tx.tx.Get(tx.dbi, bkey)
	tx.mu.Unlock()
	if lmdb.IsNotFound(err) {
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

	prefix := txkey.AllShardPrefix(index, field, view)

	bi := NewLMDBIterator(tx, prefix)
	defer bi.Close()

	lastShard := uint64(0)
	firstDone := false
	for bi.Next() {
		shard := txkey.ShardFromKey(bi.lastKey)
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
	needle := txkey.Key(index, field, view, shard, firstRoaringContainerKey)

	// prefix example: "idx:'i';fld:'f';vw:'v';shard:'00000000000000000000';key@"
	prefix := txkey.Prefix(index, field, view, shard)

	bi := NewLMDBIterator(tx, prefix)
	ok := bi.Seek(needle)
	if !ok {
		return bi, false, nil
	}

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
// only return rbf that start with prefix.
func NewLMDBIterator(tx *LMDBTx, prefix []byte) (bi *LMDBIterator) {

	tx.mu.Lock()
	//tx.debugOnlyGidcheck()

	cur, err := tx.tx.OpenCursor(tx.dbi)
	tx.mu.Unlock()

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
func (bi *LMDBIterator) Close() {
	bi.tx.mu.Lock()
	//bi.tx.debugOnlyGidcheck()

	bi.cur.Close()
	bi.tx.mu.Unlock()
}

// Valid returns false if there are no more values in the iterator's range.
func (bi *LMDBIterator) Valid() bool {
	return bi.lastOK
}

// Seek allows the iterator to start at needle instead of the global begining.
func (bi *LMDBIterator) Seek(needle []byte) (ok bool) {
	bi.tx.mu.Lock()
	defer bi.tx.mu.Unlock()

	bi.seen++ // if ommited, red TestLMDB_ContainerIterator_empty_iteration_loop() in lmdb_test.go.

	//bi.tx.debugOnlyGidcheck()

	var k, v []byte
	var err error
	getflag := uint(lmdb.SetRange)
	if len(needle) == 0 {
		k, v, err = bi.cur.Get(oneByteSliceOfZero, nil, getflag)
	} else {
		k, v, err = bi.cur.Get(needle, nil, getflag)
	}

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

var oneByteSliceOfZero = []byte{0}

// Next advances the iterator.
func (bi *LMDBIterator) Next() (ok bool) {
	if bi.lastOK && !bi.lastConsumed {
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
	var k, v []byte
	var err error

	bi.tx.mu.Lock()
	if getflag == lmdb.SetRange && len(prefix) == 0 {
		// don't do nil as key on setrange, will panic
		// b/c keys in LMDB must be at least one byte long.
		// http://www.lmdb.tech/doc/group__mdb.html#structMDB__val
		// "Key sizes must be between 1 and mdb_env_get_maxkeysize() inclusive."
		// But if getflag == lmdb.Next, key can be nil.
		k, v, err = bi.cur.Get(oneByteSliceOfZero, nil, getflag)
	} else {
		k, v, err = bi.cur.Get(prefix, nil, getflag)
	}
	bi.tx.mu.Unlock()

	//bi.tx.debugOnlyGidcheck()

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
	containerKey = txkey.KeyExtractContainerKey(bi.lastKey)

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
		return 0, nil
	}
	result := int32(0)
	for a.Next() {
		ckey, cont := a.Value()
		_ = ckey
		result += cont.N()
	}
	return uint64(result), nil
}

// Max is the maximum bit-value in your bitmap.
// Returns zero if the bitmap is empty. Odd, but this is what roaring.Max does.
func (tx *LMDBTx) Max(index, field, view string, shard uint64) (uint64, error) {

	prefix := txkey.Prefix(index, field, view, shard)
	seekto := txkey.Prefix(index, field, view, shard+1)

	//tx.debugOnlyGidcheck()

	cur, err := tx.tx.OpenCursor(tx.dbi)
	panicOn(err)
	defer cur.Close()

	var k, v []byte
	if len(seekto) == 0 {
		_, _, err = cur.Get(oneByteSliceOfZero, nil, lmdb.SetRange)
	} else {
		_, _, err = cur.Get(seekto, nil, lmdb.SetRange)
	}
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

	hb := txkey.KeyExtractContainerKey(k)
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
//
//                     f.shard*ShardWidth, rowID*ShardWidth, (rowID+1)*ShardWidth)
//                     ^ offset            ^ start           ^ endx
//
// The start and endx arguments are container keys that have been shifted left by 16 bits;
// their highbits() will be taken to determine the actual container keys. This
// is done to conform to the roaring.OffsetRange() argument convention.
//
func (tx *LMDBTx) OffsetRange(index, field, view string, shard, offset, start, endx uint64) (other *roaring.Bitmap, err error) {
	//vv("top of LMDBTx OffsetRange(index='%v', field='%v', view='%v', shard='%v', offset: %v start: %v, end: %v)", index, field, view, int(shard), int(offset), int(start), int(endx))
	//defer func() {
	//vv("returning from LMDBTx OffsetRange(index='%v', field='%v', view='%v', shard='%v', offset: %v start: %v, end: %v) other returning is: '%#v' stack=\n%v", index, field, view, int(shard), int(offset), int(start), int(endx), asInts(other.Slice()), stack())
	//}()

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

	needle := txkey.Key(index, field, view, shard, hi0)
	prefix := txkey.Prefix(index, field, view, shard)

	it := NewLMDBIterator(tx, prefix)
	defer it.Close()
	it.Seek(needle)
	for ; it.ValidForPrefix(prefix); it.Next() {
		bkey := it.lastKey
		k := txkey.KeyExtractContainerKey(bkey)

		// >= hi1 is correct b/c endx cannot have any lowbits set.
		if uint64(k) >= hi1 {
			break
		}
		destCkey := off + (k - hi0)

		v := it.lastVal
		n := len(v)
		if n == 0 {
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

			if roaring.ContainerType(newC) == roaring.ContainerBitmap {
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

	//tx.debugOnlyGidcheck()

	if len(v) == 0 {
		return nil
	}

	var w []byte
	useRowCache := tx.UseRowCache()
	if tx.doAllocZero || useRowCache {
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
	} else {
		w = v
	}
	return ToContainer(typ, w)
}

func ToContainer(typ byte, w []byte) (c *roaring.Container) {
	switch typ {
	case roaring.ContainerArray:
		c = roaring.NewContainerArray(toArray16(w))
	case roaring.ContainerBitmap:
		c = roaring.NewContainerBitmap(-1, toArray64(w))
	case roaring.ContainerRun:
		c = roaring.NewContainerRun(toInterval16(w))
	default:
		panic(fmt.Sprintf("unknown container: %v", typ))
	}
	c.SetMapped(true)
	return c
}

// StringifiedLMDBKeys returns a string with all the container
// keys available in lmdb.
func (w *LMDBWrapper) StringifiedLMDBKeys(optionalUseThisTx Tx, short bool) (r string) {
	if optionalUseThisTx == nil {
		tx, _ := w.NewTx(!writable, "<StringifiedLMDBKeys>", Txo{})
		defer tx.Rollback()
		r = stringifiedLMDBKeysTx(tx.(*LMDBTx), short)
		return
	}

	btx, ok := optionalUseThisTx.(*LMDBTx)
	if !ok {
		return fmt.Sprintf("<not-a-LMDBTx-in-StringifiedLMDBKeys-was-%T>", optionalUseThisTx)
	}
	r = stringifiedLMDBKeysTx(btx, short)
	return
}

// countBitsSet returns the number of bits set (or "hot") in
// the roaring container value found by the txkey.Key()
// formatted bkey.
func (tx *LMDBTx) countBitsSet(bkey []byte) (n int) {

	//tx.debugOnlyGidcheck()

	v, err := tx.tx.Get(tx.dbi, bkey)
	if lmdb.IsNotFound(err) {
		// some queries bkey may not be present! don't panic.
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

func (tx *LMDBTx) Dump(short bool, shard uint64) {
	fmt.Printf("%v\n", stringifiedLMDBKeysTx(tx, short))
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
func stringifiedLMDBKeysTx(tx *LMDBTx, short bool) (r string) {

	r = "allkeys:[\n"
	it := NewLMDBIterator(tx, nil)
	defer it.Close()
	any := false
	for it.Next() {
		any = true

		bkey := it.lastKey
		key := txkey.ToString(bkey)
		ckey := txkey.KeyExtractContainerKey(bkey)
		h := ""
		srbm := ""
		v := it.lastVal
		n := len(v)
		if n == 0 {
			panic("should not have empty v here")
		}
		h = hash.Blake3sum16(v[0:(n - 1)])
		ct := tx.toContainer(v[n-1], v[0:(n-1)])
		cts := roaring.NewSliceContainers()
		cts.Put(ckey, ct)
		rbm := &roaring.Bitmap{Containers: cts}
		srbm = BitmapAsString(rbm)

		r += fmt.Sprintf("%v -> %v (%v hot)\n", key, h, tx.countBitsSet(bkey))
		if !short {
			r += "          ......." + srbm + "\n"
		}
	}
	r += "]\n   all-in-blake3:" + hash.Blake3sum16([]byte(r))

	if !any {
		return "<empty lmdb database>"
	}
	return "lmdb-" + r
}

func (w *LMDBWrapper) DeleteDBPath(dbs *DBShard) (err error) {
	path := dbs.pathForType(lmdbTxn)
	err = os.RemoveAll(path)
	if err != nil {
		return errors.Wrap(err, "DeleteDBPath")
	}
	// if we go back to flat instead of inside its own directory,
	// there will be a second -lock file needing deletion too.
	lockfile := path + "-lock"
	if FileExists(lockfile) {
		err = os.RemoveAll(lockfile)
	}
	return
}

func (w *LMDBWrapper) DeleteField(index, field, fieldPath string) (err error) {

	// TODO(jea) cleanup: I think this fieldPath delete just goes away now.
	//  remove this commented stuff once we are sure.
	//
	// under blue-green roaring_lmdb, the directory will not be found, b/c roaring will have
	// already done the os.RemoveAll().	BUT, RemoveAll returns nil error in this case. Docs:
	// "If the path does not exist, RemoveAll returns nil (no error)"

	err = os.RemoveAll(fieldPath)
	if err != nil {
		return errors.Wrap(err, "removing directory")
	}
	prefix := txkey.FieldPrefix(index, field)
	return w.DeletePrefix(prefix)
}

func (w *LMDBWrapper) DeleteFragment(index, field, view string, shard uint64, frag interface{}) error {
	prefix := txkey.Prefix(index, field, view, shard)
	return w.DeletePrefix(prefix)
}

func (w *LMDBWrapper) DeletePrefix(prefix []byte) error {

	tx, _ := w.NewTx(writable, w.name, Txo{})

	// NewTx will grab these, so don't lock until after it.
	w.muDb.Lock()

	bi := NewLMDBIterator(tx.(*LMDBTx), prefix)

	for bi.Next() {
		err := bi.cur.Del(0)
		if err != nil {
			w.muDb.Unlock()
			panic(err)
		}
	}
	bi.Close()

	// Commit will grab the w.muDb lock, so we must release it first.
	w.muDb.Unlock()

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

func (tx *LMDBTx) Options() Txo {
	return tx.o
}

// Sn retreives the serial number of the Tx.
func (tx *LMDBTx) Sn() int64 {
	return tx.sn
}