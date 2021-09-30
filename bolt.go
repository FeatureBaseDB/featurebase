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

package pilosa

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/molecula/featurebase/v2/hash"
	"github.com/molecula/featurebase/v2/roaring"
	"github.com/molecula/featurebase/v2/storage"
	. "github.com/molecula/featurebase/v2/vprint"

	// On Bolt only, we still use the long txkey, because
	// this allows Max() to work readily.
	//
	"github.com/molecula/featurebase/v2/short_txkey"
	txkey "github.com/molecula/featurebase/v2/txkey"
	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"
)

const isDebugRun = false

// boltRegistrar facilitates shutdown
// of all the bolt databases started under
// tests. Its needed because most tests don't cleanup
// the *Index(es) they create. But we still
// want to shutdown boltDB goroutines
// after tests run.
//
// It also allows opening the same path twice to
// result in sharing the same open database handle, and
// thus the same transactional guarantees.
//
type boltRegistrar struct {
	mu sync.Mutex
	mp map[*BoltWrapper]bool

	path2db map[string]*BoltWrapper
}

func (r *boltRegistrar) Size() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	nmp := len(r.mp)
	npa := len(r.path2db)
	if nmp != npa {
		panic(fmt.Sprintf("nmp=%v, vs npa=%v", nmp, npa))
	}
	return nmp
}

var globalBoltReg *boltRegistrar = newBoltTestRegistrar()

var globalNextTxSnBolt int64

func newBoltTestRegistrar() *boltRegistrar {

	return &boltRegistrar{
		mp:      make(map[*BoltWrapper]bool),
		path2db: make(map[string]*BoltWrapper),
	}
}

// register each bolt created under tests, so we
// can clean them up. This is called by openBoltWrapper() while
// holding the r.mu.Lock, since it needs to atomically
// check the registry and make a new instance only
// if one does not exist for its path, and otherwise
// return the existing instance.
func (r *boltRegistrar) unprotectedRegister(w *BoltWrapper) {
	r.mp[w] = true
	r.path2db[w.path] = w
}

// unregister removes w from r
func (r *boltRegistrar) unregister(w *BoltWrapper) {
	r.mu.Lock()
	delete(r.mp, w)
	delete(r.path2db, w.path)
	r.mu.Unlock()
}

func DumpAllBolt() {
	short := true
	globalBoltReg.mu.Lock()
	defer globalBoltReg.mu.Unlock()
	for w := range globalBoltReg.mp {
		AlwaysPrintf("this bolt path='%v' has: \n%v\n", w.path, w.StringifiedBoltKeys(nil, short))
	}
}

// openBoltDB opens the database in the bpath directoy
// without deleting any prior content. Any BoltDB
// database directory will have the "-bolt" suffix.
//
// openBoltDB will check the registry and make a new instance only
// if one does not exist for its bpath. Otherwise it returns
// the existing instance. This insures only one boltDB
// per bpath in this pilosa node.
func (r *boltRegistrar) OpenDBWrapper(path string, doAllocZero bool, cfg *storage.Config) (DBWrapper, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	w, ok := r.path2db[path]
	if ok {
		// creates the effect of having only one bolt open per pilosa node.
		return w, nil
	}
	// otherwise, make a new bolt and store it in globalBoltReg

	dir := filepath.Dir(path)
	if !DirExists(path) {
		PanicOn(os.MkdirAll(dir, 0755))
	}
	fsyncEnabled := true
	if cfg != nil {
		fsyncEnabled = cfg.FsyncEnabled
	}

	db, err := bolt.Open(path, 0666, &bolt.Options{Timeout: 5 * time.Second, InitialMmapSize: TxInitialMmapSize, NoSync: !fsyncEnabled})
	if err != nil {
		return nil, errors.Wrapf(err, fmt.Sprintf("open bolt path '%v'", path))
	}

	// docs on fsync from https://godoc.org/github.com/etcd-io/bbolt
	//
	// Setting the NoSync flag will cause the database to skip fsync()
	// calls after each commit. This can be useful when bulk loading data
	// into a database and you can restart the bulk load in the event of
	// a system failure or database corruption. Do not set this flag for
	// normal use.
	//
	// If the package global IgnoreNoSync constant is true, this value is
	// ignored.  See the comment on that constant for more details.
	//
	// THIS IS UNSAFE. PLEASE USE WITH CAUTION.
	//	NoSync bool

	// When true, skips syncing freelist to disk. This improves the database
	// write performance under normal operation, but requires a full database
	// re-sync during recovery.
	// NoFreelistSync bool

	if cfg != nil && !cfg.FsyncEnabled {
		db.NoSync = true
		db.NoFreelistSync = true
	} else {
		// default to using fsync on bolt.
		db.NoSync = false
		db.NoFreelistSync = false
	}

	err = db.Update(func(tx *bolt.Tx) (err error) {
		_, err = tx.CreateBucketIfNotExists(bucketCT)
		return
	})
	if err != nil {
		return nil, errors.Wrapf(err, fmt.Sprintf("create bolt bucket '%v' in path '%v'", string(bucketCT), path))
	}

	name := filepath.Base(path)
	w = &BoltWrapper{
		name:        name,
		db:          db,
		reg:         r,
		path:        path,
		doAllocZero: doAllocZero,
		openTx:      make(map[*BoltTx]bool),

		DeleteEmptyContainer: true,
		fsyncEnabled:         cfg.FsyncEnabled,
	}
	r.unprotectedRegister(w)

	return w, nil
}

func (w *BoltWrapper) Path() string {
	return w.path
}

func (w *BoltWrapper) HasData() (has bool, err error) {

	tx, err := w.NewTx(!writable, "", Txo{})
	if err != nil {
		return false, errors.Wrap(err, "HasData NewTx")
	}
	defer tx.Rollback()

	bi := NewBoltIterator(tx.(*BoltTx), nil)
	defer bi.Close()

	for bi.Next() {
		return true, nil
	}
	return false, nil
}

func (w *BoltWrapper) CleanupTx(tx Tx) {
	// inlined into Rollback and Commit, so this is a no-op, just here to satisfy the interface.
}
func (w *BoltWrapper) CloseDB() error {
	w.muDb.Lock()
	defer w.muDb.Unlock()
	w.closed = true
	return w.db.Close()
}
func (w *BoltWrapper) OpenDB() error {
	w.muDb.Lock()
	defer w.muDb.Unlock()
	db, err := bolt.Open(w.path, 0666, &bolt.Options{Timeout: 5 * time.Second, InitialMmapSize: TxInitialMmapSize, NoSync: !w.fsyncEnabled})
	if err != nil {
		return err
	}
	w.db = db
	w.closed = false
	return nil
}

func (tx *BoltTx) IsDone() (done bool) {
	return atomic.LoadInt64(&tx.unlocked) == 1
}

func (w *BoltWrapper) OpenListString() (r string) {

	list := w.listopen()
	if len(list) == 0 {
		return "<no open BoltTx>"
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

func (w *BoltWrapper) listopen() (slc []*BoltTx) {
	w.muDb.Lock()
	for v := range w.openTx {
		slc = append(slc, v)
	}
	w.muDb.Unlock()
	return
}

func (w *BoltWrapper) OpenSnList() (slc []int64) {
	w.muDb.Lock()
	for v := range w.openTx {
		slc = append(slc, v.sn)
	}
	w.muDb.Unlock()
	return
}

// DeleteIndex deletes all the containers associated with
// the named index from the bolt database.
func (w *BoltWrapper) DeleteIndex(indexName string) error {

	// We use the apostrophie rune `'` to locate the end of the
	// index name in the key prefix, so we cannot allow indexNames
	// themselves to contain apostrophies.
	if strings.Contains(indexName, "/") {
		return fmt.Errorf("error: bad indexName `%v` in BoltWrapper.DeleteIndex() call: indexName cannot contain '/'", indexName)
	}
	prefix := txkey.IndexOnlyPrefix(indexName)
	return w.DeletePrefix(prefix)
}

// statically confirm that BoltTx satisfies the Tx interface.
var _ Tx = (*BoltTx)(nil)

// BoltWrapper provides the NewTx() method.
type BoltWrapper struct {
	db *bolt.DB

	muDb sync.Mutex

	path string
	name string

	// track our registrar for Close / goro leak reporting purposes.
	reg *boltRegistrar

	// make BoltWrapper.Close() idempotent, avoiding panic on double Close()
	closed bool

	// doAllocZero sets the corresponding flag on all new BoltTx.
	// When doAllocZero is true, we zero out any data from bolt
	// after transcation commit and rollback. This simulates
	// what would happen if we were to use the mmap-ed data
	// from bolt directly. Currently we copy by default for
	// safety because otherwise TestAPI_ImportColumnAttrs sees
	// corrupted data.
	doAllocZero bool

	DeleteEmptyContainer bool
	fsyncEnabled         bool // for tracking whether our initial config wanted fsync on

	openTx map[*BoltTx]bool
}

func (w *BoltWrapper) SetHolder(h *Holder) {
	// don't need it at the moment
	//w.h = h
}

// NewTxWRITE lets us see in the callstack dumps where the WRITE tx are.
// Can't have more than one active write per database, so the
// 2nd one will block until the first finishes.
func (w *BoltWrapper) NewTxWRITE() (*bolt.Tx, error) {
	boltTxn, err := w.db.Begin(true)
	if err != nil {
		if w.db == nil || w.IsClosed() {
			return nil, fmt.Errorf("cannot call NewTxWRITE() on closed Bolt database: '%v'", err)
		}
		return nil, err
	}
	return boltTxn, nil
}

// NewTxREAD lets us see in the callstack dumps where the READ tx are.
func (w *BoltWrapper) NewTxREAD() (*bolt.Tx, error) {
	boltTxn, err := w.db.Begin(false)
	if err != nil {
		if w.db == nil || w.IsClosed() {
			return nil, fmt.Errorf("cannot call NewTxREAD() on closed Bolt database: '%v'", err)
		}
		return nil, err
	}
	return boltTxn, nil
}

// NewTx produces Bolt based transactions. If
// the transaction will modify data, then the write flag must be true.
// Read-only queries should set write to false, to allow more concurrency.
// Methods on a BoltTx are thread-safe, and can be called from
// different goroutines.
//
// initialIndexName is optional. It is set by the TxFactory from the Txo
// options provided at the Tx creation point. It allows us to recognize
// and isolate cross-index queries more quickly. It can always be empty ""
// but when set is highly useful for debugging. It has no impact
// on transaction behavior.
//
func (w *BoltWrapper) NewTx(write bool, initialIndexName string, o Txo) (tx Tx, err error) {

	sn := atomic.AddInt64(&globalNextTxSnBolt, 1)

	////vv("bolt new tx _sn_ %v; openTx='%v', stack \n%v", sn, w.OpenListString(), stack())
	////vv("bolt new (write=%v, shard=%v) tx _sn_ %v; openTx='%v'", write, o.Shard, sn, w.OpenListString())

	var boltTxn *bolt.Tx
	if write {
		// see the WRITE tx on the callstack.
		boltTxn, err = w.NewTxWRITE()
		if err != nil {
			return nil, err
		}
	} else {
		// see the READ tx on the callstack.
		boltTxn, err = w.NewTxREAD()
		if err != nil {
			return nil, err
		}
	}

	ltx := &BoltTx{
		sn:                   sn,
		write:                write,
		tx:                   boltTxn,
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

// Close shuts down the Bolt database.
func (w *BoltWrapper) Close() (err error) {
	w.muDb.Lock()
	defer w.muDb.Unlock()
	if !w.closed {
		if isDebugRun {
			// complain if there are still Tx in flight, b/c otherwise we will see
			// the somewhat mysterious 'panic: should not be in ReadSlot.free() with slot still owned by gid=107043; refCount=1'
			if len(w.openTx) > 0 {
				AlwaysPrintf("error: cannot close BoltWrapper with Tx still in flight.")
				return
			}
		}
		w.reg.unregister(w)
		w.closed = true
		return w.db.Close()
	}
	return nil
}

func (w *BoltWrapper) IsClosed() (closed bool) {
	w.muDb.Lock()
	closed = w.closed
	w.muDb.Unlock()
	return
}

// BoltTx wraps a bolt.Tx and provides the Tx interface
// method implementations.
// The methods on BoltTx are thread-safe, and can be called
// from different goroutines.
type BoltTx struct {

	// mu serializes bolt operations on this single txn instance.
	mu sync.Mutex
	sn int64 // serial number

	write bool
	Db    *BoltWrapper
	tx    *bolt.Tx
	frag  *fragment

	opcount int

	//initloc string // stack trace of where we were initially created.

	doAllocZero bool

	initialIndexName string

	DeleteEmptyContainer bool

	unlocked int64

	o Txo

	// NewTx, write operations, Commit and/or Rollback must all take place on
	// the same gid and it must the runtime.LockOSThreaded first. Verify
	// that we are using the right goroutine in a debug build using the
	// gid, stored here, used for NewTx().
	gid uint64
}

// sanity check that database is open.
func (tx *BoltTx) sanity() {
	if tx.Db.IsClosed() {
		panic("cannot operate on closed Bolt")
	}
}

func (tx *BoltTx) Group() *TxGroup {
	return tx.o.Group
}

func (tx *BoltTx) Type() string {
	return BoltTxn
}

func (tx *BoltTx) UseRowCache() bool {
	return storage.EnableRowCache()
}

// Pointer gives us a memory address for the underlying transaction for debugging.
// It is public because we use it in roaring to report invalid container memory access
// outside of a transaction.
func (tx *BoltTx) Pointer() string {
	return fmt.Sprintf("%p", tx)
}

// Rollback rolls back the transaction.
func (tx *BoltTx) Rollback() {
	notDone := atomic.CompareAndSwapInt64(&tx.unlocked, 0, 1)
	if !notDone {
		return
	}
	////vv("bolt rollback tx _sn_ %v; stack \n%v", tx.sn) // , stack())
	if isDebugRun {
		tx.sanity()
		tx.Db.muDb.Lock()
		delete(tx.Db.openTx, tx)
		tx.Db.muDb.Unlock()
	}

	tx.mu.Lock()
	defer tx.mu.Unlock()

	//tx.debugOnlyGidcheck()
	_ = tx.tx.Rollback() // must hold tx.mu mutex lock

	tx.o.dbs.Cleanup(tx)
}

// Commit commits the transaction to permanent storage.
// Commits can handle up to 100k updates to fragments
// at once, but not more. This is a BoltDB imposed limit.
func (tx *BoltTx) Commit() error {
	notDone := atomic.CompareAndSwapInt64(&tx.unlocked, 0, 1)
	if !notDone {
		////vv("Commit already done")
		return nil
	}
	////vv("bolt commit tx _sn_ %v; path = '%v'; stack \n%v", tx.sn, tx.Db.path, stack())
	//DumpAllBolt()

	if isDebugRun {
		tx.sanity()
		tx.Db.muDb.Lock()
		delete(tx.Db.openTx, tx)
		tx.Db.muDb.Unlock()
	}
	tx.mu.Lock()
	defer tx.mu.Unlock()

	err := tx.tx.Commit()
	PanicOn(err)

	tx.o.dbs.Cleanup(tx)
	return err
}

// Readonly returns true iff the BoltTx is read-only.
func (tx *BoltTx) Readonly() bool {
	return !tx.write
}

// RoaringBitmap returns the roaring.Bitmap for all bits in the fragment.
func (tx *BoltTx) RoaringBitmap(index, field, view string, shard uint64) (*roaring.Bitmap, error) {

	return tx.OffsetRange(index, field, view, shard, 0, 0, LeftShifted16MaxContainerKey)
}

// Container returns the requested roaring.Container, selected by fragment and ckey
func (tx *BoltTx) Container(index, field, view string, shard uint64, ckey uint64) (c *roaring.Container, err error) {

	// values returned from Get() are only valid while the transaction
	// is open. If you need to use a value outside of the transaction then
	// you must use copy() to copy it to another byte slice.
	// BUT here we are already inside the Txn.

	bkey := txkey.Key(index, field, view, shard, ckey)
	tx.mu.Lock()

	//tx.debugOnlyGidcheck()

	bkt := tx.tx.Bucket(bucketCT)
	v := bkt.Get(bkey)
	tx.mu.Unlock()

	if v == nil {
		// not found
		return nil, nil
	}
	n := len(v)
	if n > 0 {
		c = tx.toContainer(v[n-1], v[0:(n-1)])
	}
	return
}

var bucketCT = []byte("ct")

// PutContainer stores rc under the specified fragment and container ckey.
func (tx *BoltTx) PutContainer(index, field, view string, shard uint64, ckey uint64, rc *roaring.Container) error {

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

	bkt := tx.tx.Bucket(bucketCT)
	err := bkt.Put(bkey, append(by, ct))
	////vv("err on put bkey = '%v' was %v", string(bkey), err)
	tx.mu.Unlock()

	return err
}

// RemoveContainer deletes the container specified by the shard and container key ckey
func (tx *BoltTx) RemoveContainer(index, field, view string, shard uint64, ckey uint64) error {
	bkey := txkey.Key(index, field, view, shard, ckey)
	tx.mu.Lock()
	//tx.debugOnlyGidcheck()

	bkt := tx.tx.Bucket(bucketCT)
	err := bkt.Delete(bkey)

	tx.mu.Unlock()
	return err
}

// Add sets all the a bits hot in the specified fragment.
func (tx *BoltTx) Add(index, field, view string, shard uint64, a ...uint64) (changeCount int, err error) {
	return tx.addOrRemove(index, field, view, shard, false, a...)
}

// Remove clears all the specified a bits in the chosen fragment.
func (tx *BoltTx) Remove(index, field, view string, shard uint64, a ...uint64) (changeCount int, err error) {
	return tx.addOrRemove(index, field, view, shard, true, a...)
}

func (tx *BoltTx) addOrRemove(index, field, view string, shard uint64, remove bool, a ...uint64) (changeCount int, err error) {
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
					PanicOn(err)
				} else {
					err = tx.PutContainer(index, field, view, shard, lastHi, rc)
					PanicOn(err)
				}
			}
			// get the next container
			rc, err = tx.Container(index, field, view, shard, hi)
			PanicOn(err)
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
			PanicOn(err)
		} else {
			err = tx.PutContainer(index, field, view, shard, hi, rc)
			PanicOn(err)
		}
	} else {
		if rc == nil || rc.N() == 0 {
			panic("there should be no way to have an empty bitmap AFTER an Add() operation")
		}
		err = tx.PutContainer(index, field, view, shard, hi, rc)
		PanicOn(err)
	}
	return
}

// Contains returns exists true iff the bit chosen by key is
// hot (set to 1) in specified fragment.
func (tx *BoltTx) Contains(index, field, view string, shard uint64, key uint64) (exists bool, err error) {

	lo, hi := lowbits(key), highbits(key)
	bkey := txkey.Key(index, field, view, shard, hi)
	tx.mu.Lock()

	bkt := tx.tx.Bucket(bucketCT)
	v := bkt.Get(bkey)

	tx.mu.Unlock()
	if v == nil {
		return false, nil
	}
	n := len(v)
	if n > 0 {
		c := tx.toContainer(v[n-1], v[0:(n-1)])
		exists = c.Contains(lo)
	}
	return exists, err
}

// key is the container key for the first roaring Container
// roaring docs: Iterator returns a ContainterIterator which *after* a call to Next(), a call to Value() will
// return the first container at or after key. found will be true if a
// container is found at key.
//
// BoltTx notes: We auto-stop at the end of this shard, not going beyond.
func (tx *BoltTx) ContainerIterator(index, field, view string, shard uint64, firstRoaringContainerKey uint64) (citer roaring.ContainerIterator, found bool, err error) {

	// needle example: "idx:'i';fld:'f';vw:'v';shd:'00000000000000000000';key@00000000000000000000"
	needle := txkey.Key(index, field, view, shard, firstRoaringContainerKey)

	// prefix example: "idx:'i';fld:'f';vw:'v';shard:'00000000000000000000';key@"
	prefix := txkey.Prefix(index, field, view, shard)

	bi := NewBoltIterator(tx, prefix)
	ok := bi.Seek(needle)
	if !ok {
		return bi, false, nil
	}

	// have to compare b/c bolt might give us valid iterator
	// that is past our needle if needle isn't present.
	return bi, bytes.Equal(bi.lastKey, needle), nil
}

func (tx *BoltTx) GetFieldSizeBytes(index, field string) (uint64, error) {
	return 0, nil
}

// BoltIterator is the iterator returned from a BoltTx.ContainerIterator() call.
// It implements the roaring.ContainerIterator interface.
type BoltIterator struct {
	tx  *BoltTx
	cur *bolt.Cursor

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

// NewBoltIterator creates an iterator on tx that will
// only return boltKeys that start with prefix.
func NewBoltIterator(tx *BoltTx, prefix []byte) (bi *BoltIterator) {

	tx.mu.Lock()

	bkt := tx.tx.Bucket(bucketCT)
	cur := bkt.Cursor()
	tx.mu.Unlock()

	bi = &BoltIterator{
		tx:     tx,
		cur:    cur,
		prefix: prefix,
	}

	return
}

// Close tells the database and transaction that the user is done
// with the iterator.
func (bi *BoltIterator) Close() {
	// no-op
}

// Valid returns false if there are no more values in the iterator's range.
func (bi *BoltIterator) Valid() bool {
	return bi.lastOK
}

// Seek allows the iterator to start at needle instead of the global begining.
func (bi *BoltIterator) Seek(needle []byte) (ok bool) {
	bi.tx.mu.Lock()
	defer bi.tx.mu.Unlock()

	bi.seen++ // if ommited, red TestBolt_ContainerIterator_empty_iteration_loop()

	k, v := bi.cur.Seek(needle)
	////vv("seek to needle '%v' gives key '%v'", string(needle), txkey.ToString(k))

	if len(k) == 0 {
		// not found, no keys after needle.
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
	bi.lastOK = true
	bi.lastConsumed = false

	return true
}

func (bi *BoltIterator) ValidForPrefix(prefix []byte) bool {
	if !bi.lastOK {
		return false
	}
	if len(bi.prefix) == 0 {
		return true
	}
	return bytes.HasPrefix(bi.lastKey, bi.prefix)
}

func (bi *BoltIterator) String() (r string) {
	return fmt.Sprintf("BoltIterator{prefix: '%v', seekto: '%v', seen:%v, lastKey:'%v', lastOK:%v, lastConsumed:%v}", string(bi.prefix), string(bi.seekto), bi.seen, string(bi.lastKey), bi.lastOK, bi.lastConsumed)
}

// Next advances the iterator.
func (bi *BoltIterator) Next() (ok bool) {
	//vv("Next; bi.prefix='%v'", string(bi.prefix))
	if bi.lastOK && !bi.lastConsumed {
		//vv("lastOk and not consumed, returning wo doing anything")
		bi.seen++
		bi.lastConsumed = true
		if len(bi.lastVal) == 0 {
			panic("bi.lastVal should not have len 0 if lastOK true")
		}
		return true
	}

	var k, v []byte
	if bi.seen == 0 {
		if len(bi.prefix) == 0 {
			bi.tx.mu.Lock()
			k, v = bi.cur.First()
			bi.tx.mu.Unlock()
		} else {
			found := bi.Seek(bi.prefix)
			// increments bi.seen for us.
			if !found {
				bi.lastKey = nil
				bi.lastVal = nil
				bi.lastOK = false
				bi.lastConsumed = false
				return false
			}
			// ready to go
			return true
		}
	}

	bi.seen++
skipEmpty:
	if bi.seen > 1 {
		bi.tx.mu.Lock()
		k, v = bi.cur.Next()
		bi.tx.mu.Unlock()
	}

	if len(k) == 0 {
		// no more
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
func (bi *BoltIterator) Value() (containerKey uint64, c *roaring.Container) {
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

// boltFinder implements roaring.IteratorFinder.
// It is used by BoltTx.ForEach()
type boltFinder struct {
	tx        *BoltTx
	index     string
	field     string
	view      string
	shard     uint64
	needClose []Closer
}

// FindIterator lets boltFinder implement the roaring.FindIterator interface.
func (bf *boltFinder) FindIterator(seek uint64) (roaring.ContainerIterator, bool) {
	a, found, err := bf.tx.ContainerIterator(bf.index, bf.field, bf.view, bf.shard, seek)
	PanicOn(err)
	bf.needClose = append(bf.needClose, a)
	return a, found
}

// Close closes all bf.needClose listed Closers.
func (bf *boltFinder) Close() {
	for _, i := range bf.needClose {
		i.Close()
	}
}

// NewTxIterator returns a *roaring.Iterator that MUST have Close() called on it BEFORE
// the transaction Commits or Rollsback.
func (tx *BoltTx) NewTxIterator(index, field, view string, shard uint64) *roaring.Iterator {

	bf := &boltFinder{tx: tx, index: index, field: field, view: view, shard: shard, needClose: make([]Closer, 0)}
	itr := roaring.NewIterator(bf)
	return itr
}

// ForEach applies fn to each bitmap in the fragment.
func (tx *BoltTx) ForEach(index, field, view string, shard uint64, fn func(i uint64) error) error {

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
func (tx *BoltTx) ForEachRange(index, field, view string, shard uint64, start, end uint64, fn func(uint64) error) error {

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
func (tx *BoltTx) Count(index, field, view string, shard uint64) (uint64, error) {

	a, found, err := tx.ContainerIterator(index, field, view, shard, 0)
	PanicOn(err)
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
func (tx *BoltTx) Max(index, field, view string, shard uint64) (uint64, error) {

	prefix := txkey.Prefix(index, field, view, shard)
	seekto := txkey.Prefix(index, field, view, shard+1)

	bkt := tx.tx.Bucket(bucketCT)
	cur := bkt.Cursor()

	var k, v []byte
	k, _ = cur.Seek(seekto)
	if k == nil {
		// we have nothing >= seekto, but we might have stuff before it, and we'll wrap backwards.
		k, v = cur.Prev()
		if k == nil {
			// empty database
			return 0, nil
		}
	} else {
		// we found something >= seekto, so backup by 1.
		k, v = cur.Prev()
		if k == nil {
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

	n := len(v)
	if n == 0 {
		return 0, nil
	}

	hb := txkey.KeyExtractContainerKey(k)
	rc := tx.toContainer(v[n-1], v[0:(n-1)])

	lb := rc.Max()
	return hb<<16 | uint64(lb), nil
}

// Min returns the smallest bit set in the fragment. If no bit is hot,
// the second return argument is false.
func (tx *BoltTx) Min(index, field, view string, shard uint64) (uint64, bool, error) {

	// Seek can create many container iterators, thus the bf.Close() needClose list.
	bf := &boltFinder{tx: tx, index: index, field: field, view: view, shard: shard, needClose: make([]Closer, 0)}
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
func (tx *BoltTx) UnionInPlace(index, field, view string, shard uint64, others ...*roaring.Bitmap) error {

	rbm, err := tx.RoaringBitmap(index, field, view, shard)
	PanicOn(err)

	rbm.UnionInPlace(others...)
	// iterate over the containers that changed within rbm, and write them back to disk.

	it, found := rbm.Containers.Iterator(0)
	_ = found // don't care about the value of found, because first containerKey might be > 0

	for it.Next() {
		containerKey, rc := it.Value()

		// TODO: only write the changed ones back, as optimization?
		//       Compare to ImportRoaringBits.
		err := tx.PutContainer(index, field, view, shard, containerKey, rc)
		PanicOn(err)
	}
	return nil
}

// CountRange returns the count of hot bits in the start, end range on the fragment.
// roaring.countRange counts the number of bits set between [start, end).
func (tx *BoltTx) CountRange(index, field, view string, shard uint64, start, end uint64) (n uint64, err error) {

	if start >= end {
		return 0, nil
	}

	skey := highbits(start)
	ekey := highbits(end)

	citer, found, err := tx.ContainerIterator(index, field, view, shard, skey)
	_ = found
	PanicOn(err)

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
func (tx *BoltTx) OffsetRange(index, field, view string, shard, offset, start, endx uint64) (other *roaring.Bitmap, err error) {
	////vv("top of BoltTx OffsetRange(index='%v', field='%v', view='%v', shard='%v', offset: %v start: %v, end: %v)", index, field, view, int(shard), int(offset), int(start), int(endx))
	//defer func() {
	////vv("returning from BoltTx OffsetRange(index='%v', field='%v', view='%v', shard='%v', offset: %v start: %v, end: %v) other returning is: '%#v' stack=\n%v", index, field, view, int(shard), int(offset), int(start), int(endx), asInts(other.Slice()), stack())
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

	it := NewBoltIterator(tx, prefix)
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
func (tx *BoltTx) IncrementOpN(index, field, view string, shard uint64, changedN int) {
	tx.opcount += changedN
}

// ImportRoaringBits handles deletes by setting clear=true.
// rowSet[rowID] returns the number of bit changed on that rowID.
func (tx *BoltTx) ImportRoaringBits(index, field, view string, shard uint64, itr roaring.RoaringIterator, clear bool, log bool, rowSize uint64, data []byte) (changed int, rowSet map[uint64]int, err error) {

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
		PanicOn(err)
		if err != nil {
			return
		}

		if oldC == nil || oldC.N() == 0 {
			// no container at the itrKey in bolt (or all zero container).
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
					PanicOn(err)
					return
				}
				continue
			}
		}
	}
	return
}

func (tx *BoltTx) toContainer(typ byte, v []byte) (r *roaring.Container) {

	//tx.debugOnlyGidcheck()

	if len(v) == 0 {
		return nil
	}

	var w []byte
	useRowCache := tx.UseRowCache()
	if tx.doAllocZero || useRowCache {
		// Do electric fence-inspired bad-memory read detection.
		//
		// The v []byte lives in BoltDB's memory-mapped vlog-file,
		// and Bolt will recycle it after tx ends with rollback or commit.
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

// StringifiedBoltKeys returns a string with all the container
// keys available in bolt.
func (w *BoltWrapper) StringifiedBoltKeys(optionalUseThisTx Tx, short bool) (r string) {
	if optionalUseThisTx == nil {
		tx, _ := w.NewTx(!writable, "<StringifiedBoltKeys>", Txo{})
		defer tx.Rollback()
		r = stringifiedBoltKeysTx(tx.(*BoltTx), short)
		return
	}

	btx, ok := optionalUseThisTx.(*BoltTx)
	if !ok {
		return fmt.Sprintf("<not-a-BoltTx-in-StringifiedBoltKeys-was-%T>", optionalUseThisTx)
	}
	r = stringifiedBoltKeysTx(btx, short)
	return
}

// countBitsSet returns the number of bits set (or "hot") in
// the roaring container value found by the txkey.Key()
// formatted bkey.
func (tx *BoltTx) countBitsSet(bkey []byte) (n int) {

	//tx.debugOnlyGidcheck()

	bkt := tx.tx.Bucket(bucketCT)
	v := bkt.Get(bkey)

	if v == nil {
		// some queries bkey may not be present! don't panic.
		return 0
	}

	n = len(v)
	if n > 0 {
		rc := tx.toContainer(v[n-1], v[0:(n-1)])
		n = int(rc.N())
	}
	return
}

func (tx *BoltTx) Dump(short bool, shard uint64) {
	fmt.Printf("%v\n", stringifiedBoltKeysTx(tx, short))
}

func (tx *BoltTx) GetSortedFieldViewList(idx *Index, shard uint64) (fvs []short_txkey.FieldView, err error) {
	bkt := tx.tx.Bucket(bucketCT)
	err = bkt.ForEach(func(bkey, v []byte) error {
		fv := txkey.FieldViewFromFullKey(bkey)
		var shortFV short_txkey.FieldView
		shortFV.Field = fv.Field
		shortFV.View = fv.View
		fvs = append(fvs, shortFV)
		return nil
	})
	return
}

// stringifiedBoltKeysTx reports all the bolt keys and a
// corresponding blake3 hash viewable by txn within the entire
// bolt database.
// It also reports how many bits are hot in the roaring container
// (how many bits are set, or 1 rather than 0).
//
// By convention, we must return the empty string if there
// are no keys present. The tests use this to confirm
// an empty database.
func stringifiedBoltKeysTx(tx *BoltTx, short bool) (r string) {

	r = "allkeys:[\n"
	it := NewBoltIterator(tx, nil)
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
		return "<empty bolt database>"
	}
	return "bolt-" + r
}

func (w *BoltWrapper) DeleteDBPath(dbs *DBShard) (err error) {
	path := dbs.pathForType(boltTxn)
	err = os.RemoveAll(path)
	if err != nil {
		return errors.Wrap(err, "DeleteDBPath")
	}
	return
}

func (w *BoltWrapper) DeleteField(index, field, fieldPath string) (err error) {

	// TODO(jea) cleanup: I think this fieldPath delete just goes away now.
	//  remove this commented stuff once we are sure.
	//
	// under blue-green roaring_bolt, the directory will not be found, b/c roaring will have
	// already done the os.RemoveAll().	BUT, RemoveAll returns nil error in this case. Docs:
	// "If the path does not exist, RemoveAll returns nil (no error)"

	err = os.RemoveAll(fieldPath)
	if err != nil {
		return errors.Wrap(err, "removing directory")
	}
	prefix := txkey.FieldPrefix(index, field)
	return w.DeletePrefix(prefix)
}

func (w *BoltWrapper) DeleteFragment(index, field, view string, shard uint64, frag interface{}) error {
	prefix := txkey.Prefix(index, field, view, shard)
	return w.DeletePrefix(prefix)
}

func (w *BoltWrapper) DeletePrefix(prefix []byte) error {

	tx, _ := w.NewTx(writable, w.name, Txo{})

	// NewTx will grab these, so don't lock until after it.
	w.muDb.Lock()

	bi := NewBoltIterator(tx.(*BoltTx), prefix)

	for bi.Next() {
		//vv("deleting next in cur")
		err := bi.cur.Delete()
		if err != nil {
			w.muDb.Unlock()
			panic(err)
		}
	}
	bi.Close()

	// Commit will grab the w.muDb lock, so we must release it first.
	w.muDb.Unlock()

	err := tx.Commit()
	PanicOn(err)

	return nil
}

func (tx *BoltTx) RoaringBitmapReader(index, field, view string, shard uint64, fragmentPathForRoaring string) (r io.ReadCloser, sz int64, err error) {

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

func (tx *BoltTx) Options() Txo {
	return tx.o
}

// Sn retreives the serial number of the Tx.
func (tx *BoltTx) Sn() int64 {
	return tx.sn
}

func (c *BoltTx) ApplyFilter(index, field, view string, shard uint64, ckey uint64, filter roaring.BitmapFilter) (err error) {
	return GenericApplyFilter(c, index, field, view, shard, ckey, filter)
}
