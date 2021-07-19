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
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"text/tabwriter"

	"github.com/molecula/featurebase/v2/hash"
	"github.com/molecula/featurebase/v2/roaring"
	txkey "github.com/molecula/featurebase/v2/short_txkey"
	"github.com/molecula/featurebase/v2/storage"
	. "github.com/molecula/featurebase/v2/vprint" // nolint:staticcheck
	"github.com/pkg/errors"
	"github.com/zeebo/blake3"
)

// public strings that pilosa/server/config.go can reference
const (
	RoaringTxn string = "roaring"
	RBFTxn     string = "rbf"
	BoltTxn    string = "bolt"
)

// DetectMemAccessPastTx true helps us catch places in api and executor
// where mmapped memory is being accessed after the point in time
// which the transaction has committed or rolled back. Since
// memory segments will be recycled by the underlying databases,
// this can lead to corruption. When DetectMemAccessPastTx is true,
// code in bolt.go will copy the transactionally viewed memory before
// returning it for bitmap reading, and then zero it or overwrite it
// with -2 when the Tx completes.
//
// Should be false for production.
//
const DetectMemAccessPastTx = false

var sep = string(os.PathSeparator)

// Qcx is a (Pilosa) Query Context.
//
// It flexibly expresses the desired grouping of Tx for mass
// rollback at a query's end. It provides one-time commit for
// an atomic import write Tx that involves multiple fragments.
//
// The most common use of Qcx is to call GetTx() to obtain a Tx locally,
// once the index/shard pair is known:
//
//   someFunc(qcx Qcx, idx *Index, shard uint64) (err0 error) {
//		tx, finisher := qcx.GetTx(Txo{Write: true, Index:idx, Shard:shard, ...})
//		defer finisher(&err0)
//      ...
//   }
//
// Qcx reuses read-only Tx on the same index/shard pair. See
// the Qcx.GetTx() for further discussion. The caveat is of
// course that your "new" read Tx actually has an "old" view
// of the database.
//
// At the moment, most
// writes to individual shards are commited eagerly and locally
// when the `defer finisher(&err0)` is run.
// This is done by returning a finisher that actually Commits,
// thus freeing the one write slot for re-use. A single
// writer is also required by RBF, so this design accomodates
// both.
//
// In contrast, the default read Tx generated (or re-used) will
// return a no-op finisher and the group of reads as a whole
// will be rolled back (mmap memory released) en-mass when
// Qcx.Abort() is called at the top-most level.
//
// Local use of a (Tx, finisher) pair obtained from Qcx.GetTx()
// doesn't need to care about these details. Local use should
// always invoke finisher(&err0) or finisher(nil) to complete
// the Tx within the local function scope.
//
// In summary write Tx are typically "local"
// and are never saved into the TxGroup. The parallelism
// supplied by TxGroup typically applies only to read Tx.
//
// The one exception is this rule is for the one write Tx
// used during the api.ImportAtomicRecord routine. There
// we make a special write Tx and use it for all matching writes.
// This is then committed at the final, top-level, Qcx.Finish() call.
//
// See also the Qcx.GetTx() example and the TxGroup description below.
//
type Qcx struct {
	Grp *TxGroup
	Txf *TxFactory

	// if we go back to using Qcx values, this must become a pointer,
	// or otherwise be dealt with because copies of Mutex are a no-no.
	mu sync.Mutex

	// RequiredForAtomicWriteTx is used by api.ImportAtomicRecord
	// to ensure that all writes happen on this one Tx.
	RequiredForAtomicWriteTx *Tx

	// efficient access to the options for RequiredForAtomicWriteTx
	RequiredTxo *Txo

	isRoaring bool

	// top-level context is for a write, so re-use a
	// writable tx for all reads and writes on each given
	// shard
	write bool

	// don't allow automatic reuse now. Must manually call Reset, or NewQcx().
	done bool
}

// Finish commits/rollsback all stored Tx. It no longer resets the
// Qcx for further operations automatically. User must call Reset()
// or NewQxc() again.
func (q *Qcx) Finish() (err error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.RequiredForAtomicWriteTx != nil {
		if q.RequiredTxo.Write {
			err = (*q.RequiredForAtomicWriteTx).Commit() // PanicOn here on 2nd. is this a double commit?
		} else {
			(*q.RequiredForAtomicWriteTx).Rollback()
		}
	}
	err2 := q.Grp.FinishGroup()
	q.done = true

	if err != nil {
		return err
	}
	return err2
}

// Abort rolls back all Tx generated and stored within the Qcx.
// The Qcx is then reset and can be used again immediately.
func (q *Qcx) Abort() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.RequiredForAtomicWriteTx != nil {
		(*q.RequiredForAtomicWriteTx).Rollback()
	}
	q.Grp.AbortGroup()

	q.done = true
}

// Reset forgets everything are starts fresh with an empty
// group, ready for use again as if NewQcx() had been called.
func (q *Qcx) Reset() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.done {
		PanicOn("must call Qcx.Abort() or Qcx.Finish() before calling Reset().")
	}
	q.unprotected_reset()
}

func (q *Qcx) unprotected_reset() {
	q.RequiredForAtomicWriteTx = nil
	q.RequiredTxo = nil
	q.Grp = q.Txf.NewTxGroup()
	q.done = false
}

// NewQcxWithGroup allocates a freshly allocated and empty Grp.
// The top-level executor will set qcx.write = true manually
// if the overall query is a write.
func (f *TxFactory) NewQcx() (qcx *Qcx) {
	qcx = &Qcx{
		Grp: f.NewTxGroup(),
		Txf: f,
	}
	if f.typeOfTx == "roaring" {
		qcx.isRoaring = true
	}
	return
}

var NoopFinisher = func(perr *error) {}

var ErrQcxDone = fmt.Errorf("Qcx already Aborted or Finished, so must call reset before re-use")

// GetTx is used like this:
//
// someFunc(ctx context.Context, shard uint64) (_ interface{}, err0 error) {
//
//		tx, finisher := qcx.GetTx(Txo{Write: !writable, Index: idx, Shard: shard})
//		defer finisher(&err0)
//
//		return e.executeIncludesColumnCallShard(ctx, tx, index, c, shard, col)
//	}
//
// Note we are tracking the returned err0 error value of someFunc(). An option instead is to say
//
//     defer finisher(nil)
//
// This means always Commit writes, ignoring if there were errors. This style
// is expected to be rare compared to the typical
//
//     defer finisher(&err0)
//
// invocation, where err0 is your return from the enclosing function error.
// If the Tx is local and not a part of a group, then the finisher
// consults that error to decides whether to Commit() or Rollback().
//
// If instead the Tx becomes part of a group, then the local finisher() is
// always a no-op, in deference to the Qcx.Finish()
// or Qcx.Abort() calls.
//
// Take care the finisher(&err) is capturing the address of the
// enclosing function's err and that it has not been shadowed
// locally by another _, err := f() call. For this reason, it can
// be clearer (and much safer) to rename the enclosing functions 'err' to 'err0',
// to make it clear we are referring to the first and final error.
//
func (qcx *Qcx) GetTx(o Txo) (tx Tx, finisher func(perr *error), err error) {
	qcx.mu.Lock()
	defer qcx.mu.Unlock()

	if qcx.done {
		return nil, nil, ErrQcxDone
	}

	// roaring uses finer grain, a file per fragment rather than
	// db per shard. So we can't re-use the readTx. Moreover,
	// roaring Tx are No-ops anyway, so just give it a new Tx
	// everytime.
	if qcx.isRoaring {
		return qcx.Txf.NewTx(o), NoopFinisher, nil
	}

	// qcx.write reflects the top executor determination
	// if a write will be done at the end, so we upgrade
	// the "local" read Tx to be writes, so that they
	// don't deadlock against themselves under blue-green.
	o.Write = o.Write || qcx.write

	// In general, we make ALL write transactions local, and never reuse them
	// below. Previously this was to help lmdb.
	//
	// *However* there is one exception: when we have set RequiredForAtomicWriteTx
	// for the importing of an AtomicRequest, then we must use that
	// our single RequiredForAtomicWriteTx for all writes until it
	// is cleared. This one is kept separately from the read TxGroup.
	//
	if o.Write && qcx.RequiredForAtomicWriteTx != nil {
		// verify that shard and index match!
		ro := qcx.RequiredTxo
		if o.Shard != ro.Shard {
			PanicOn(fmt.Sprintf("shard mismatch: o.Shard = %v while qcx.RequiredTxo.Shard = %v", o.Shard, ro.Shard))
		}
		if o.Index == nil {
			PanicOn("o.Index annot be nil")
		}
		if ro.Index == nil {
			PanicOn("ro.Index annot be nil")
		}
		if o.Index.name != ro.Index.name {
			PanicOn(fmt.Sprintf("index mismatch: o.Index = %v while qcx.RequiredTxo.Index = %v", o.Index.name, ro.Index.name))
		}
		return *qcx.RequiredForAtomicWriteTx, NoopFinisher, nil
	}

	if !o.Write && qcx.Grp != nil {
		// read, with a group in place.
		finisher = func(perr *error) {} // finisher is a returned value

		already := false
		tx, already = qcx.Grp.AlreadyHaveTx(o)
		if already {
			return
		}
		o.Group = qcx.Grp
		tx = qcx.Txf.NewTx(o)
		qcx.Grp.AddTx(tx)
		return
	}

	// non atomic writes or not grouped reads
	tx = qcx.Txf.NewTx(o)
	if o.Write {
		finisherDone := false
		finisher = func(perr *error) {
			if finisherDone {
				return
			}
			finisherDone = true // only Commit once.
			// so defer finisher(nil) means always Commit writes, ignoring
			// the enclosing functions return status.
			if perr == nil || *perr == nil {
				PanicOn(tx.Commit())
			} else {
				tx.Rollback()
			}
		}
	} else {
		// read-only txn
		finisher = func(perr *error) {
			tx.Rollback()
		}
	}
	return
}

// StartAtomicWriteTx allocates a Tx and stores it
// in qcx.RequiredForAtomicWriteTx. All subsequent writes
// to this shard/index will re-use it.
func (qcx *Qcx) StartAtomicWriteTx(o Txo) {
	if !o.Write {
		PanicOn("must have o.Write true")
	}
	qcx.mu.Lock()
	defer qcx.mu.Unlock()

	if qcx.RequiredForAtomicWriteTx == nil {
		// new Tx needed
		tx := qcx.Txf.NewTx(o)
		qcx.RequiredForAtomicWriteTx = &tx
		o := tx.Options()
		qcx.RequiredTxo = &o
		return
	}

	// re-using existing

	// verify that shard and index match!
	ro := qcx.RequiredTxo
	if o.Shard != ro.Shard {
		PanicOn(fmt.Sprintf("shard mismatch: o.Shard = %v while qcx.RequiredTxo.Shard = %v", o.Shard, ro.Shard))
	}
	if o.Index == nil {
		PanicOn("o.Index annot be nil")
	}
	if ro.Index == nil {
		PanicOn("ro.Index annot be nil")
	}
	if o.Index.name != ro.Index.name {
		PanicOn(fmt.Sprintf("index mismatch: o.Index = %v while qcx.RequiredTxo.Index = %v", o.Index.name, ro.Index.name))
	}
}

func (qcx *Qcx) SetRequiredForAtomicWriteTx(tx Tx) {
	if tx == nil || NilInside(tx) {
		PanicOn("cannot set nil tx in SetRequiredForAtomicWriteTx")
	}
	qcx.mu.Lock()
	qcx.RequiredForAtomicWriteTx = &tx
	o := tx.Options()
	qcx.RequiredTxo = &o
	qcx.mu.Unlock()
}

func (qcx *Qcx) ClearRequiredForAtomicWriteTx() {
	qcx.mu.Lock()
	qcx.RequiredForAtomicWriteTx = nil
	qcx.RequiredTxo = nil
	qcx.mu.Unlock()
}

func (qcx *Qcx) ListOpenTx() string {
	return qcx.Grp.String()
}

// TxFactory abstracts the creation of Tx interface-level
// transactions so that RBF, BoltDB, or Roaring-fragment-files, or several
// of these at once in parallel, is used as the storage and transction layer.
type TxFactory struct {
	typeOfTx string

	mu sync.Mutex

	types []txtype // blue-green split individually here

	dbsClosed bool // idemopotent CloseDB()

	dbPerShard *DBPerShard

	holder *Holder

	blueGreenReg *blueGreenRegistry

	// allow holder to activate blue-green checking only
	// once we have synced both sides at start up time.
	blueGreenOff bool

	isBlueGreen bool
}

func (f *TxFactory) Types() []txtype {
	return f.types
}

// integer types for fast switch{}
type txtype int

const (
	noneTxn    txtype = 0
	roaringTxn txtype = 1 // these don't really have any transactions
	rbfTxn     txtype = 2
	boltTxn    txtype = 4
)

// DirectoryName just returns a string version of the transaction type. We
// really need to consolidate the storage backend and tx stuff because it's
// currently rather confusing. This method should be addressed (i.e.
// replaced/removed) during that refactor.
func (ty txtype) DirectoryName() string {
	switch ty {
	case roaringTxn:
		return "roaring"
	case rbfTxn:
		return "rbf"
	case boltTxn:
		return "boltdb"
	}
	PanicOn(fmt.Sprintf("unkown txtype %v", int(ty)))
	return ""
}

func (txf *TxFactory) NeedsSnapshot() (b bool) {
	for _, ty := range txf.types {
		switch ty {
		case roaringTxn:
			b = true
			return
		}
	}
	return
}

func MustBackendToTxtype(backend string) (types []txtype) {
	var srcs []string
	if strings.Contains(backend, "_") {
		srcs = strings.Split(backend, "_")
		if len(srcs) != 2 {
			PanicOn("only two blue-green comparisons permitted")
		}
	} else {
		srcs = append(srcs, backend)
	}

	for i, s := range srcs {
		switch s {
		case RoaringTxn: // "roaring"
			types = append(types, roaringTxn)
		case RBFTxn: // "rbf"
			types = append(types, rbfTxn)
		case BoltTxn: // "bolt"
			types = append(types, boltTxn)
		default:
			PanicOn(fmt.Sprintf("unknown backend '%v'", s))
		}
		if i == 1 {
			if types[1] == types[0] {
				PanicOn(fmt.Sprintf("cannot blue-green the same backend on both arms: '%v'", s))
			}
		}
	}
	return
}

// NewTxFactory always opens an existing database. If you
// want to a fresh database, os.RemoveAll on dir/name ahead of time.
// We always store files in a subdir of holderDir.
func NewTxFactory(backend string, holderDir string, holder *Holder) (f *TxFactory, err error) {
	types := MustBackendToTxtype(backend)

	f = &TxFactory{
		types:    types,
		typeOfTx: backend,
		holder:   holder,
	}
	if len(types) == 2 {
		f.blueGreenReg = newBlueGreenReg(types)
		f.isBlueGreen = true
		// blue-green can never use the rowCache.
		storage.SetRowCacheOn(false)
	}
	f.dbPerShard = f.NewDBPerShard(types, holderDir, holder)

	if f.hasRBF() {
		holder.Logger.Infof("rbf config = %#v", holder.cfg.RBFConfig)
	}

	return f, err
}

// Open should be called only once the index metadata is loaded
// from Holder.Open(), so we find all of our indexes.
func (f *TxFactory) Open() error {
	return f.dbPerShard.LoadExistingDBs()
}

// UseRowCache can be more "global" than Tx at the moment, because
// we are sharing the same bool flag in rbf at the moment. If
// this changes then fragment.openStorage() will need a new way
// to determine if it should use the rowCache. Currently it
// doesn't have a tx Tx parameter, so we use the Txf instead.
func (f *TxFactory) UseRowCache() bool {
	return storage.EnableRowCache()
}

// Txo holds the transaction options
type Txo struct {
	Write    bool
	Field    *Field
	Index    *Index
	Fragment *fragment
	Shard    uint64

	dbs *DBShard
	per *DBPerShard

	Group *TxGroup

	blueGreenOff bool
}

func (o Txo) String() string {
	return fmt.Sprintf("Txo{Write:%v, Index:%v Shard:%v Group:%p}", o.Write, o.Index.name, o.Shard, o.Group)
}

func (f *TxFactory) TxType() string {
	return f.typeOfTx
}

func (f *TxFactory) TxTypes() []txtype {
	return f.types
}

func (f *TxFactory) DeleteIndex(name string) (err error) {
	return f.dbPerShard.DeleteIndex(name)
}

func (f *TxFactory) DeleteFieldFromStore(index, field, fieldPath string) (err error) {
	return f.dbPerShard.DeleteFieldFromStore(index, field, fieldPath)
}

func (f *TxFactory) DeleteFragmentFromStore(
	index, field, view string, shard uint64, frag *fragment,
) (err error) {
	return f.dbPerShard.DeleteFragment(index, field, view, shard, frag)
}

func (f *TxFactory) DumpAll() {
	f.dbPerShard.DumpAll()
}

// IndexUsageDetails computes the sum of filesizes used by the node, broken down
// by index, field, fragments and keys.
func (f *TxFactory) IndexUsageDetails(isClosing func() bool) (map[string]IndexUsage, uint64, error) {
	indexUsage := make(map[string]IndexUsage)
	holderPath, err := expandDirName(f.holder.path)
	if err != nil {
		return indexUsage, 0, errors.Wrap(err, "expanding data directory")
	}
	indexesPath, err := expandDirName(f.holder.IndexesPath())
	if err != nil {
		return indexUsage, 0, errors.Wrap(err, "expanding indexes directory")
	}

	idxs := f.holder.Indexes()

	qcx := f.NewQcx()
	defer qcx.Abort()
	for _, idx := range idxs {
		index := idx.name
		indexPath := path.Join(indexesPath, index)

		// field usage
		fieldUsages := make(map[string]FieldUsage)
		fragmentsTotal := uint64(0)
		fieldKeysTotal := uint64(0)
		fieldMetaBytesTotal := uint64(0)
		fieldsTotal := uint64(0)
		flds := idx.Fields()
		for _, fld := range flds {
			field := fld.Name()
			if field == "_keys" {
				continue
			}
			fUsage, err := f.fieldUsage(indexPath, fld)
			if err != nil {
				return indexUsage, 0, errors.Wrapf(err, "getting disk usage for index (%s)", index)
			}

			// non-roaring field usage
			fragmentUsage := uint64(0)

			for _, shard := range fld.AvailableShards(true).Slice() {
				if isClosing() {
					return nil, 0, nil
				}
				if err := func() error {
					tx, finisher, err := qcx.GetTx(Txo{Write: !writable, Index: idx, Shard: shard})
					if err != nil {
						return errors.Wrap(err, "qcx.GetTx")
					}
					defer finisher(nil)

					fieldBytes, err := tx.GetFieldSizeBytes(index, field)
					if err != nil {
						return errors.Wrapf(err, "getting disk usage for non-roaring fragments (%s)", field)
					}
					fragmentUsage += fieldBytes
					return nil
				}(); err != nil {
					return indexUsage, 0, err
				}
			}

			// add non-roaring to roaring
			fUsage.Fragments += fragmentUsage
			fUsage.Total += fragmentUsage

			// add to running total
			fieldMetaBytesTotal += fUsage.Metadata
			fieldKeysTotal += fUsage.Keys
			fragmentsTotal += fUsage.Fragments
			fieldsTotal += fUsage.Total

			fieldUsages[field] = fUsage
		}

		// index metadata
		indexMetaBytes, err := directoryUsage(indexPath, false)
		if err != nil {
			return indexUsage, 0, errors.Wrapf(err, "getting disk usage for index metadata (%s)", index)
		}

		// index keys usage
		indexKeysBytes := uint64(0)
		if idx.keys {
			keysPath := path.Join(indexPath, translateStoreDir)
			indexKeysBytes, _ = directoryUsage(keysPath, true) // if directory doesn't exist, size = 0
		}

		indexUsage[index] = IndexUsage{
			Total:          indexMetaBytes + indexKeysBytes + fieldsTotal,
			Metadata:       indexMetaBytes + fieldMetaBytesTotal,
			IndexKeys:      indexKeysBytes,
			FieldKeysTotal: fieldKeysTotal,
			Fragments:      fragmentsTotal,
			Fields:         fieldUsages,
		}
	}

	// node metadata, e.g. id allocator
	nodeMetaBytes, err := directoryUsage(holderPath, false)
	if err != nil {
		return indexUsage, 0, errors.Wrapf(err, "getting disk usage for node metadata")
	}

	return indexUsage, nodeMetaBytes, nil
}

// fieldUsage computes the sum of filesizes used by a field in
// the filesystem tree (roaring storage), broken down by keys and fragments.
func (f *TxFactory) fieldUsage(indexPath string, fld *Field) (FieldUsage, error) {
	fieldUsage := FieldUsage{}

	field := fld.name

	// row keys
	keysBytes := int64(0)
	var err error
	keysBytes, err = fileSize(fld.TranslateStorePath())
	if err != nil {
		// if file doesn't exist, size = 0
		keysBytes = 0
	}

	// field metadata
	fieldPath := path.Join(indexPath, FieldsDir, field)
	metaBytes, err := directoryUsage(fieldPath, false) // this includes keys
	if err != nil {
		return fieldUsage, errors.Wrapf(err, "getting disk usage for field meta (%s)", field)
	}

	// fragment data
	viewsPath := path.Join(fieldPath, "views")
	fragmentBytes := uint64(0)
	if dirExists(viewsPath) {
		fragmentBytes, err = directoryUsage(viewsPath, true)
		if err != nil {
			return fieldUsage, errors.Wrapf(err, "getting disk usage for field fragments (%s)", field)
		}
	}

	fieldUsage = FieldUsage{
		Total:     metaBytes + fragmentBytes, // metaBytes includes keys
		Metadata:  metaBytes - uint64(keysBytes),
		Fragments: fragmentBytes,
		Keys:      uint64(keysBytes),
	}

	return fieldUsage, nil
}

// NOTE: Go 1.16 introduced a new Readdir() method that is supposed to be more performant.
// Not yet upgraded b/c new method is not compatible with older versions of Go.
func directoryUsage(fname string, recursive bool) (uint64, error) {
	if !dirExists(fname) {
		return 0, errors.Errorf("directory does not exist (%s)", fname)
	}

	var size uint64

	dir, err := os.Open(fname)
	if err != nil {
		return 0, errors.Wrap(err, "opening data subdirectory")
	}
	defer dir.Close()

	files, err := dir.Readdir(-1)
	if err != nil {
		return 0, errors.Wrap(err, "reading data subdirectory")
	}

	for _, file := range files {
		if recursive && file.IsDir() {
			sz, err := directoryUsage(path.Join(fname, file.Name()), true)
			if err != nil {
				return 0, err
			}
			size += sz
		} else {
			size += uint64(file.Size()) // NOTE this cast is safe for regular files, not necessarily others
		}
	}

	return size, nil
}

// CloseIndex is a no-op. This seems to be in place for debugging purposes.
func (f *TxFactory) CloseIndex(idx *Index) error {
	//idx.Dump("CloseIndex")
	return nil
}

func (f *TxFactory) Close() (err error) {
	if f.dbsClosed {
		return nil
	}
	f.dbsClosed = true
	return f.dbPerShard.Close()
}

var globalUseStatTx = false

func init() {
	v := os.Getenv("PILOSA_CALLSTAT")
	if v != "" {
		globalUseStatTx = true
	}
}

// TxGroup holds a set of read and a set of write transactions
// that will en-mass have Rollback() (for the read set) and
// Commit() (for the write set) called on
// them when TxGroup.Finish() is invoked.
// Alternatively, TxGroup.Abort() will call Rollback()
// on all Tx group memebers.
type TxGroup struct {
	mu       sync.Mutex
	fac      *TxFactory
	reads    []Tx
	writes   []Tx
	finished bool

	all map[grpkey]Tx
}

type grpkey struct {
	write bool
	index string
	shard uint64
}

func mustHaveIndexShard(o *Txo) {
	if o.Index == nil || o.Index.name == "" {
		PanicOn("index must be set on Txo")
	}
}

func (g *TxGroup) AlreadyHaveTx(o Txo) (tx Tx, already bool) {
	mustHaveIndexShard(&o)
	g.mu.Lock()
	defer g.mu.Unlock()
	key := grpkey{write: o.Write, index: o.Index.name, shard: o.Shard}
	tx, already = g.all[key]
	return
}

func (g *TxGroup) String() (r string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.reads) == 0 && len(g.writes) == 0 {
		return "<empty-TxGroup>"
	}

	i := 0
	r += "\n"
	for _, tx := range g.reads {
		r += fmt.Sprintf("[%v]read: _sn_ %v %v, \n", i, tx.Sn(), tx.Options())
		i++
	}
	for _, tx := range g.writes {
		r += fmt.Sprintf("[%v]write: _sn_ %v %v, \n", i, tx.Sn(), tx.Options())
		i++
	}
	return
}

// NewTxGroup
func (f *TxFactory) NewTxGroup() (g *TxGroup) {
	g = &TxGroup{
		fac: f,
		all: make(map[grpkey]Tx),
	}
	return
}

// AddTx adds tx to the group.
func (g *TxGroup) AddTx(tx Tx) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.finished {
		PanicOn("in TxGroup.Finish(): TxGroup already finished")
	}
	if NilInside(tx) {
		PanicOn("Cannot add nil Tx to TxGroup")
	}

	if tx.Readonly() {
		g.reads = append(g.reads, tx)
	} else {
		g.writes = append(g.writes, tx)
	}
	o := tx.Options()
	mustHaveIndexShard(&o)

	key := grpkey{write: o.Write, index: o.Index.name, shard: o.Shard}
	prior, ok := g.all[key]
	if ok {
		PanicOn(fmt.Sprintf("already have Tx in group for this, we should have re-used it! prior is '%v'; tx='%v'", prior, tx))
	}
	g.all[key] = tx
}

// Finish commits the write tx and calls Rollback() on
// the read tx contained in the group. Either Abort() or Finish() must
// be called on the TxGroup exactly once.
func (g *TxGroup) FinishGroup() (err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.finished {
		PanicOn("in TxGroup.Finish(): TxGroup already finished")
	}
	g.finished = true
	for i, tx := range g.writes {
		_ = i
		err0 := tx.Commit()
		if err0 != nil {
			if err == nil {
				err = err0 // keep the first error, but Commit them all.
			}
		}
	}
	for _, r := range g.reads {
		r.Rollback()
	}
	return
}

// Abort calls Rollback() on all the group Tx, and marks
// the group as finished. Either Abort() or Finish() must
// be called on the TxGroup.
func (g *TxGroup) AbortGroup() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.finished {
		// defer Abort() probably gets here often by default, just ignore.
		return
	}
	g.finished = true

	for _, r := range g.reads {
		r.Rollback()
	}
	for _, tx := range g.writes {
		tx.Rollback()
	}
}

func (f *TxFactory) NewTx(o Txo) (txn Tx) {
	defer func() {
		if globalUseStatTx {
			txn = newStatTx(txn)
		}
	}()

	if f.isBlueGreen {
		f.mu.Lock()
		o.blueGreenOff = f.blueGreenOff
		f.mu.Unlock()
	}

	indexName := ""
	if o.Index != nil {
		indexName = o.Index.name
	}

	if o.Fragment != nil {
		if o.Fragment.index() != indexName {
			PanicOn(fmt.Sprintf("inconsistent NewTx request: o.Fragment.index='%v' but indexName='%v'", o.Fragment.index(), indexName))
		}
		if o.Fragment.shard != o.Shard {
			PanicOn(fmt.Sprintf("inconsistent NewTx request: o.Fragment.shard='%v' but o.Shard='%v'", o.Fragment.shard, o.Shard))
		}
	}

	// look up in the collection of open databases, and get our
	// per-shard database. Opens a new one if needed.
	dbs, err := f.dbPerShard.GetDBShard(indexName, o.Shard, o.Index)
	PanicOn(err)

	if dbs.Shard != o.Shard {
		PanicOn(fmt.Sprintf("asked for o.Shard=%v but got dbs.Shard=%v", int(o.Shard), int(dbs.Shard)))
	}
	//vv("got dbs='%p' for o.Index='%v'; shard='%v'; dbs.types='%#v'; dbs.W='%#v'", dbs, o.Index.name, o.Shard, dbs.types, dbs.W)

	o.dbs = dbs          // our specific database per shard.
	o.per = f.dbPerShard // for top level debug Dumps

	tx, err := dbs.NewTx(o.Write, indexName, o)
	if err != nil {
		PanicOn(errors.Wrap(err, "dbs.NewTx transaction errored"))
	}
	return tx
}

// has to match the const strings at the top of the file.
func (ty txtype) String() string {
	switch ty {
	case noneTxn:
		return "noneTxn"
	case roaringTxn:
		return "roaring"
	case rbfTxn:
		return "rbf"
	case boltTxn:
		return "bolt"
	}
	PanicOn(fmt.Sprintf("unhandled ty '%v' in txtype.String()", int(ty)))
	return ""
}

// fragmentSpecFromRoaringPath takes a path releative to the
// index directory, not including the name of the index itself.
// The path should not start with the path separator sep ('/' or '\\') rune.
func fragmentSpecFromRoaringPath(path string) (field, view string, shard uint64, err error) {
	if len(path) == 0 {
		err = fmt.Errorf("fragmentSpecFromRoaringPath error: path '%v' too short", path)
		return
	}
	if path[:1] == sep {
		err = fmt.Errorf("fragmentSpecFromRoaringPath error: path '%v' cannot start with separator '%v'; must be relative to the index base directory", path, sep)
		return
	}

	// sample path:
	//        field         view               shard
	// fields/myfield/views/standard/fragments/0
	s := strings.Split(path, "/")
	n := len(s)
	if n != 6 {
		err = fmt.Errorf("len(s)=%v, but expected 5. path='%v'", n, path)
		return
	}
	field = s[1]
	view = s[3]
	shard, err = strconv.ParseUint(s[5], 10, 64)
	if err != nil {
		err = fmt.Errorf("fragmentSpecFromRoaringPath(path='%v') could not parse shard '%v' as uint: '%v'", path, s[5], err)
	}
	return
}

// hashOnly means only show the value hash, not the content bits.
// showOps means display the ops log.
func (idx *Index) StringifiedRoaringKeys(hashOnly, showOps bool, o Txo) (r string) {
	paths, err := listFilesUnderDir(idx.path, false, "", true)
	PanicOn(err)
	index := idx.name

	r = "allkeys:[\n"
	n := 0
	for _, relpath := range paths {
		field, view, shard, err := fragmentSpecFromRoaringPath(relpath)
		if err != nil {
			continue // ignore .meta paths
		}
		if shard != o.Shard {
			continue // only print the shard the Txo is on.
		}
		abspath := idx.path + sep + relpath

		s, _, err := stringifiedRawRoaringFragment(abspath, index, field, view, shard, showOps, hashOnly, os.Stdout)
		PanicOn(err)
		//r += fmt.Sprintf("path:'%v' fragment contains:\n") + s
		//if s == "" {
		//s = "<empty bitmap>"
		//}
		r += s
		n++
	}
	if n == 0 {
		return "<empty roaring data>"
	}
	// note that we can have a bitmap present, but it can be empty
	r += "]\n   all-in-blake3:" + hash.Blake3sum16([]byte(r)) + "\n"

	return "roaring-" + r
}

func RoaringFragmentChecksum(path string, index, field, view string, shard uint64) (r string, hotbits int) {
	defer func() {
		r := recover()
		if r != nil {
			PanicOn(fmt.Sprintf("caught PanicOn on path='%v', index='%v', field='%v', view='%v', shard='%v': %v",
				path, index, field, view, shard, r))
		}
	}()
	hasher := blake3.New()
	showOps := false
	hashOnly := true
	hash, hotbits, err := stringifiedRawRoaringFragment(path, index, field, view, shard, showOps, hashOnly, hasher)
	PanicOn(err)
	fmt.Fprintf(hasher, "%v/%v/%v/%v/%v", index, field, view, shard, hash)
	var buf [16]byte
	_, _ = hasher.Digest().Read(buf[0:])
	return fmt.Sprintf("%x", buf), hotbits

}

func stringifiedRawRoaringFragment(path string, index, field, view string, shard uint64, showOps, hashOnly bool, w io.Writer) (r string, hotbits int, err error) {

	var info roaring.BitmapInfo
	_ = info
	var f *os.File
	f, err = os.Open(path)
	PanicOn(err)
	if err != nil {
		return
	}

	var fi os.FileInfo
	fi, err = f.Stat()
	PanicOn(err)
	if err != nil {
		return
	}
	// Memory map the file.
	data, err := syscall.Mmap(int(f.Fd()), 0, int(fi.Size()), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		err = errors.Wrap(err, "mmapping")
		return
	}
	defer func() {
		err := syscall.Munmap(data)
		if err != nil {
			PanicOn(fmt.Errorf("loadRawRoaringContainer: munmap failed: %v", err))
		}
		PanicOn(f.Close())
	}()

	// Attach the mmap file to the bitmap.
	var rbm *roaring.Bitmap
	rbm, _, err = roaring.InspectBinary(data, true, &info)
	if err != nil {
		err = errors.Wrap(err, "inspecting")
		return
	}

	//cmd.DisplayInfo(info)
	// inlined
	if showOps {
		pC := pointerContext{
			from: info.From,
			to:   info.To,
		}
		if info.ContainerCount > 0 {
			printContainers(w, info, pC)
		}
		if info.Ops > 0 {
			printOps(w, info)
		}
	}

	citer, found := rbm.Containers.Iterator(0)
	_ = found // probably gonna use just the Ops log instead, so don't PanicOn if !found.

	for citer.Next() {
		ckey, ct := citer.Value()
		by := containerToBytes(ct)
		hash := hash.Blake3sum16(by)

		cts := roaring.NewSliceContainers()
		cts.Put(ckey, ct)
		rbm := &roaring.Bitmap{Containers: cts}

		var srbm string
		if !hashOnly {
			srbm = BitmapAsString(rbm)
		}

		bkey := txkey.ToString(txkey.Key(index, field, view, shard, ckey))

		n := ct.N()
		hotbits += int(n)
		r += fmt.Sprintf("%v -> %v (%v hot)\n", bkey, hash, n)
		if !hashOnly {
			r += "          ......." + srbm + "\n"
		}
	}

	return
}

// listFilesUnderDir returns the paths of files found under directory root.
// If includeRoot is true, it returns the full path, otherwise paths are relative to root.
// If requriedSuffix is supplied, the returned file paths will end in that,
// and any other files found during the walk of the directory tree will be ignored.
// If ignoreEmpty is true, files of size 0 will be excluded.
func listFilesUnderDir(root string, includeRoot bool, requiredSuffix string, ignoreEmpty bool) (files []string, err error) {
	if !dirExists(root) {
		return nil, fmt.Errorf("listFilesUnderDir error: root directory '%v' not found", root)
	}
	n := len(root) + 1
	if includeRoot {
		n = 0
	}
	err = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if len(path) < n {
			// ignore
		} else {
			if info == nil {
				PanicOn(fmt.Sprintf("info was nil for path = '%v'", path))
			}
			if info.IsDir() {
				// skip directories.
			} else {
				if ignoreEmpty && info.Size() == 0 {
					return nil
				}
				if requiredSuffix == "" || strings.HasSuffix(path, requiredSuffix) {
					files = append(files, path[n:])
				}
			}
		}
		return nil
	})
	return
}

func dirExists(name string) bool {
	fi, err := os.Stat(name)
	if err != nil {
		return false
	}
	if fi.IsDir() {
		return true
	}
	return false
}

func fileSize(name string) (int64, error) {
	fi, err := os.Stat(name)
	if err != nil {
		return -1, err
	}
	return fi.Size(), nil
}

var _ = fileSize // happy linter

func containerToBytes(ct *roaring.Container) []byte {
	ty := roaring.ContainerType(ct)
	switch ty {
	case roaring.ContainerNil:
		PanicOn("nil roaring.Container")
	case roaring.ContainerArray:
		return fromArray16(roaring.AsArray(ct))
	case roaring.ContainerBitmap:
		return fromArray64(roaring.AsBitmap(ct))
	case roaring.ContainerRun:
		return fromInterval16(roaring.AsRuns(ct))
	}
	PanicOn(fmt.Sprintf("unknown roaring.Container type '%v'", int(ty)))
	return nil
}

type pointerContext struct {
	from, to uintptr
}

func printOps(w io.Writer, info roaring.BitmapInfo) {
	fmt.Fprintln(w, "  Ops:")
	tw := tabwriter.NewWriter(w, 0, 8, 0, '\t', 0)
	fmt.Fprintf(tw, "  \t%s\t%s\t%s\t\n", "TYPE", "OpN", "SIZE")
	printed := 0
	for _, op := range info.OpDetails {
		fmt.Fprintf(tw, "\t%s\t%d\t%d\t\n", op.Type, op.OpN, op.Size)
		printed++
	}
	tw.Flush()
}

func (p *pointerContext) pretty(c roaring.ContainerInfo) string {
	var pointer string
	if c.Mapped {
		if c.Pointer >= p.from && c.Pointer < p.to {
			pointer = fmt.Sprintf("@+0x%x", c.Pointer-p.from)
		} else {
			pointer = fmt.Sprintf("!0x%x!", c.Pointer)
		}
	} else {
		pointer = fmt.Sprintf("0x%x", c.Pointer)
	}
	return fmt.Sprintf("%s \t%d \t%d \t%s ", c.Type, c.N, c.Alloc, pointer)
}

// stolen from ctl/inspect.go
func printContainers(w io.Writer, info roaring.BitmapInfo, pC pointerContext) {
	fmt.Fprintln(w, "  Containers:")
	tw := tabwriter.NewWriter(w, 0, 8, 0, '\t', 0)
	fmt.Fprintf(tw, "  \t\tRoaring\t\t\t\tOps\t\t\t\tFlags\t\n")
	fmt.Fprintf(tw, "\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t\n", "KEY", "TYPE", "N", "ALLOC", "OFFSET", "TYPE", "N", "ALLOC", "OFFSET", "FLAGS")
	c1s := info.Containers
	c2s := info.OpContainers
	l1 := len(c1s)
	l2 := len(c2s)
	i1 := 0
	i2 := 0
	var c1, c2 roaring.ContainerInfo
	c1.Key = ^uint64(0)
	c2.Key = ^uint64(0)
	c1e := false
	c2e := false
	if i1 < l1 {
		c1 = c1s[i1]
		i1++
		c1e = true
	}
	if i2 < l2 {
		c2 = c2s[i2]
		i2++
		c2e = true
	}
	printed := 0
	for c1e || c2e {
		c1used := false
		c2used := false
		var key uint64
		c1fmt := "-\t\t\t"
		c2fmt := "-\t\t\t"
		// If c2 exists, we'll always prefer its flags,
		// if it doesn't, this gets overwritten.
		flags := c2.Flags
		if !c2e || (c1e && c1.Key < c2.Key) {
			c1fmt = pC.pretty(c1)
			key = c1.Key
			c1used = true
			flags = c1.Flags
		} else if !c1e || (c2e && c2.Key < c1.Key) {
			c2fmt = pC.pretty(c2)
			key = c2.Key
			c2used = true
		} else {
			// c1e and c2e both set, and neither key is < the other.
			c1fmt = pC.pretty(c1)
			c2fmt = pC.pretty(c2)
			key = c1.Key
			c1used = true
			c2used = true
		}
		if c1used {
			if i1 < l1 {
				c1 = c1s[i1]
				i1++
			} else {
				c1e = false
			}
		}
		if c2used {
			if i2 < l2 {
				c2 = c2s[i2]
				i2++
			} else {
				c2e = false
			}
		}
		fmt.Fprintf(tw, "\t%d\t%s\t%s\t%s\t\n", key, c1fmt, c2fmt, flags)
		printed++
	}
	tw.Flush()
}

var _ = anyGlobalDBWrappersStillOpen // happy linter

func anyGlobalDBWrappersStillOpen() bool {
	if globalRoaringReg.Size() != 0 {
		return true
	}
	if globalRbfDBReg.Size() != 0 {
		return true
	}
	if globalBoltReg.Size() != 0 {
		return true
	}
	return false
}

func (f *TxFactory) blueGreenOnIfRunningBlueGreen() {
	if len(f.types) == 2 {
		f.blueGreenOff = false
	}
}

func (f *TxFactory) blueGreenOffIfRunningBlueGreen() {
	if len(f.types) == 2 {
		f.blueGreenOff = true
	}
}

func (f *TxFactory) hasRoaring() bool {
	return f.types[0] == roaringTxn || (len(f.types) > 1 && f.types[1] == roaringTxn)
}

func (f *TxFactory) hasRBF() bool {
	return f.types[0] == rbfTxn || (len(f.types) > 1 && f.types[1] == rbfTxn)
}

var _ = (&TxFactory{}).hasRoaring // happy linter

func (f *TxFactory) blueHasData() (hasData bool, err error) {
	if len(f.types) != 2 {
		return false, nil
	}
	return f.dbPerShard.HasData(0)
}

func (f *TxFactory) greenHasData() (hasData bool, err error) {
	n := len(f.types)
	switch n {
	case 1:
		return f.dbPerShard.HasData(0)
	case 2:
		return f.dbPerShard.HasData(1)
	}
	err = fmt.Errorf("unsupported len(f.types): %v. Must be 1 or 2.", n)
	PanicOn(err)
	return
}

// green2blue is called at the very end of Holder.Open(), so
// we know that the holder is ready to go, knowing its holder.Indexes(), fields,
// view, shards, and other metadata if any.
//
// Called by test Test_TxFactory_UpdateBlueFromGreen_OnStartup() in
// txfactory_internal_test.go as well.
//
// This is a noop if we aren't running under a blue_green PILOSA_STORAGE_BACKEND.
func (f *TxFactory) green2blue(holder *Holder) (err0 error) {

	// Holder.Open will always call us, even without blue_green. Which is fine.
	// We are just a no-op in that case.
	if len(f.types) != 2 {
		return nil
	}

	holder.Logger.Infof("green2blue analysis begins.")

	blueDest := f.types[0]
	greenSrc := f.types[1]

	if blueDest == roaringTxn {
		return fmt.Errorf("error: cannot migrate to 'roaring': not implemented.")
	}

	idxs := holder.Indexes()

	verifyInsteadOfCopy := false

	blueHasData, err := f.blueHasData()
	if err != nil {
		return errors.Wrap(err, "TxFactory.green2blue f.blueHasData()")
	}

	greenHasData, err := f.greenHasData()
	if err != nil {
		return errors.Wrap(err, "TxFactory.green2blue f.greenHasData()")
	}
	if !blueHasData && !greenHasData {
		holder.Logger.Infof("no data in blue or green. No migration or verification to do.")
		return nil
	}
	// INVAR: blue has data.
	if !greenHasData {
		holder.Logger.Errorf("cannot migrate from green '%v' because it has no data in it.", greenSrc)
		return fmt.Errorf("error: cannot migrate from green '%v' because it has no data in it.", greenSrc)
	}

	nGoro := runtime.NumCPU()
	if nGoro < 5 {
		// try to get some overlapped IO
		nGoro = 5
	}
	pj := newParallelJobs(nGoro)

	action := "verify"
	if blueHasData {
		verifyInsteadOfCopy = true
		defer holder.Logger.Infof("bitmap-backend verification done    : %v compared to %v", blueDest, greenSrc)
	} else {
		action = "migrate"
		holder.Logger.Infof("bitmap-backend migration starting: populating %v from %v with %v threads", blueDest, greenSrc, nGoro)
		defer holder.Logger.Infof("bitmap-backend migration done    : populated  %v from %v", blueDest, greenSrc)
	}
	firstPjobStarted := false

indexloop:
	for k, idx := range idxs {

		// scan directories
		blueShards, err := f.dbPerShard.TypedDBPerShardGetShardsForIndex(blueDest, idx, "", false)
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("GetDBShard(index='%v') error fetching blueShards", idx.name))
		}

		// scan directories
		greenShards, err := f.dbPerShard.TypedDBPerShardGetShardsForIndex(greenSrc, idx, "", true)
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("GetDBShard(index='%v') error fetching greenShards", idx.name))
		}

		if verifyInsteadOfCopy {
			diff := f.shardSetDiff(blueShards, greenShards)
			if diff != "" {
				return fmt.Errorf("verifyInsteadOfCopy true, blue[%v]=%#v and green[%v]=%#v have different shards for index '%v': '%v'; stack=\n%v", blueDest, blueShards, greenSrc, greenShards, idx.name, diff, Stack())
			}

			// can also check against meta data
			shards := idx.AvailableShards(localOnly).Slice()
			meta := make(map[uint64]bool)
			for _, shard := range shards {
				meta[shard] = true
			}
			diff2 := f.shardSetDiff(greenShards, meta)
			if diff2 != "" {
				return fmt.Errorf("green[%v] = '%#v' and meta data '%#v' have different shards for index '%v': %v", greenSrc, greenShards, shards, idx.name, diff2)
			}
		}

		shardNum := 0
		for shard := range greenShards {
			shardNum++
			shnum := shardNum
			idx := idx
			shard := shard
			k := k
			fun := func(worker int) error {

				dbs, err := f.dbPerShard.GetDBShard(idx.name, shard, idx)
				if err != nil {
					return errors.Wrap(err, fmt.Sprintf("GetDBShard(index='%v', shard='%v')", idx.name, int(shard)))
				}

				holder.Logger.Infof("%v progress on index '%v' (%v of %v): on shard '%v' (%v of %v) [worker %v]",
					action, idx.name, k+1, len(idxs), shard, shnum, len(greenShards), worker)

				if verifyInsteadOfCopy {
					// verify all containers
					err = dbs.verifyBlueEqualsGreen()
					if err != nil {
						return errors.Wrap(err,
							fmt.Sprintf("dbs.verifyBlueEqualsGreen(blue='%v', "+
								"green='%v') for index='%v', shard='%v'",
								blueDest, greenSrc, idx.name, int(shard)))
					}
				} else {
					// the main copy work
					err = dbs.populateBlueFromGreen()
					if err != nil {
						return errors.Wrap(err,
							fmt.Sprintf("dbs.copyGreenToBlue(blue='%v', "+
								"green='%v') for index='%v', shard='%v'",
								blueDest, greenSrc, idx.name, int(shard)))
					}
				}
				return nil
			} // end of fun definition

			if !pj.run(fun) {
				break indexloop
			}
			if !firstPjobStarted {
				firstPjobStarted = true
				defer func() {
					err1 := pj.waitForFinish()
					if err0 == nil {
						err0 = err1
					}
				}()
			}
		}
	}
	return nil
}

func (f *TxFactory) shardSetDiff(blueShards, greenShards map[uint64]bool) (diff string) {
	nb := len(blueShards)
	ng := len(greenShards)
	if nb != ng {
		diff = fmt.Sprintf("blueShard[%v] count = %v; greenShard[%v] count = %v; ", f.types[0], nb, f.types[1], ng)
	}
	bmg := mapDiff(blueShards, greenShards) // get blue - green
	gmb := mapDiff(greenShards, blueShards) // get green - blue

	if len(bmg) == 0 && len(gmb) == 0 {
		return ""
	}
	diff += fmt.Sprintf("shard diff: blueMinusGreen shards: '%#v'; greenMinusBlue shards: '%#v'", bmg, gmb)
	return
}

func (f *TxFactory) GetDBShardPath(index string, shard uint64, idx *Index, ty txtype, write bool) (shardPath string, err error) {
	dbs, err := f.dbPerShard.GetDBShard(index, shard, idx)
	if err != nil {
		return "", errors.Wrap(err, fmt.Sprintf("GetDBShardPath(index='%v', shard='%v', ty='%v')", index, shard, ty.String()))
	}
	shardPath = dbs.pathForType(ty)
	return
}

func (txf *TxFactory) GetFieldView2ShardsMapForIndex(idx *Index) (vs *FieldView2Shards, err error) {
	return txf.dbPerShard.GetFieldView2ShardsMapForIndex(idx)
}
