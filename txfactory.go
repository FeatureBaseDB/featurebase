// Copyright 2021 Molecula Corp. All rights reserved.
package pilosa

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/molecula/featurebase/v2/testhook"
	"github.com/molecula/featurebase/v2/vprint"
	"github.com/pkg/errors"
)

// public strings that pilosa/server/config.go can reference
const (
	RoaringTxn string = "roaring"
	RBFTxn     string = "rbf"
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
	// drop the old group so we aren't holding references to all those Tx
	q.Grp = q.Txf.NewTxGroup()
	if !q.done {
		_ = testhook.Closed(q.Txf.holder.Auditor, q, nil)
	}
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
	// drop the old group so we aren't holding references to all those Tx
	q.Grp = q.Txf.NewTxGroup()
	if !q.done {
		_ = testhook.Closed(q.Txf.holder.Auditor, q, nil)
	}
	q.done = true
}

// Reset forgets everything are starts fresh with an empty
// group, ready for use again as if NewQcx() had been called.
func (q *Qcx) Reset() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.done {
		vprint.PanicOn("must call Qcx.Abort() or Qcx.Finish() before calling Reset().")
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
	_ = testhook.Opened(f.holder.Auditor, qcx, nil)
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
	// if a write will be happen at some point, in which case, to avoid
	// locking problems with multi-shard things, we (probably incorrectly)
	// treat every Tx as its own individual separate Tx.
	//
	// But we still want to open non-write transactions individually, we
	// just can't recycle them (because write operations will come in and
	// we want them to work and commit right away so we're not holding a write
	// lock for long).
	writeLogic := o.Write || qcx.write

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
			vprint.PanicOn(fmt.Sprintf("shard mismatch: o.Shard = %v while qcx.RequiredTxo.Shard = %v", o.Shard, ro.Shard))
		}
		if o.Index == nil {
			vprint.PanicOn("o.Index annot be nil")
		}
		if ro.Index == nil {
			vprint.PanicOn("ro.Index annot be nil")
		}
		if o.Index.name != ro.Index.name {
			vprint.PanicOn(fmt.Sprintf("index mismatch: o.Index = %v while qcx.RequiredTxo.Index = %v", o.Index.name, ro.Index.name))
		}
		return *qcx.RequiredForAtomicWriteTx, NoopFinisher, nil
	}

	if !writeLogic && qcx.Grp != nil {
		// read, with a group in place.
		finisher = func(perr *error) {} // finisher is a returned value

		already := false
		tx, already = qcx.Grp.AlreadyHaveTx(o)
		if already {
			return
		}
		tx = qcx.Txf.NewTx(o)
		qcx.Grp.AddTx(tx, o)
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
				vprint.PanicOn(tx.Commit())
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
		vprint.PanicOn("must have o.Write true")
	}
	qcx.mu.Lock()
	defer qcx.mu.Unlock()

	if qcx.RequiredForAtomicWriteTx == nil {
		// new Tx needed
		tx := qcx.Txf.NewTx(o)
		qcx.RequiredForAtomicWriteTx = &tx
		qcx.RequiredTxo = &o
		return
	}

	// re-using existing

	// verify that shard and index match!
	ro := qcx.RequiredTxo
	if o.Shard != ro.Shard {
		vprint.PanicOn(fmt.Sprintf("shard mismatch: o.Shard = %v while qcx.RequiredTxo.Shard = %v", o.Shard, ro.Shard))
	}
	if o.Index == nil {
		vprint.PanicOn("o.Index annot be nil")
	}
	if ro.Index == nil {
		vprint.PanicOn("ro.Index annot be nil")
	}
	if o.Index.name != ro.Index.name {
		vprint.PanicOn(fmt.Sprintf("index mismatch: o.Index = %v while qcx.RequiredTxo.Index = %v", o.Index.name, ro.Index.name))
	}
}

func (qcx *Qcx) ListOpenTx() string {
	return qcx.Grp.String()
}

// TxFactory abstracts the creation of Tx interface-level
// transactions so that RBF, or Roaring-fragment-files, or several
// of these at once in parallel, is used as the storage and transction layer.
type TxFactory struct {
	typeOfTx string

	typ txtype

	dbsClosed bool // idemopotent CloseDB()

	dbPerShard *DBPerShard

	holder *Holder
}

// integer types for fast switch{}
type txtype int

const (
	noneTxn    txtype = 0
	roaringTxn txtype = 1 // these don't really have any transactions
	rbfTxn     txtype = 2
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
	}
	vprint.PanicOn(fmt.Sprintf("unkown txtype %v", int(ty)))
	return ""
}

func (txf *TxFactory) NeedsSnapshot() (b bool) {
	return txf.typ == roaringTxn
}

func MustBackendToTxtype(backend string) (typ txtype) {
	if strings.Contains(backend, "_") {
		panic("blue-green comparisons removed")
	}

	switch backend {
	case RoaringTxn: // "roaring"
		return roaringTxn
	case RBFTxn: // "rbf"
		return rbfTxn
	}
	panic(fmt.Sprintf("unknown backend '%v'", backend))
}

// NewTxFactory always opens an existing database. If you
// want to a fresh database, os.RemoveAll on dir/name ahead of time.
// We always store files in a subdir of holderDir.
func NewTxFactory(backend string, holderDir string, holder *Holder) (f *TxFactory, err error) {
	typ := MustBackendToTxtype(backend)

	f = &TxFactory{
		typ:      typ,
		typeOfTx: backend,
		holder:   holder,
	}
	f.dbPerShard = f.NewDBPerShard(typ, holderDir, holder)

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

// Txo holds the transaction options
type Txo struct {
	Write    bool
	Field    *Field
	Index    *Index
	Fragment *fragment
	Shard    uint64

	dbs *DBShard
}

func (f *TxFactory) TxType() string {
	return f.typeOfTx
}

func (f *TxFactory) TxTyp() txtype {
	return f.typ
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

// TxGroup holds a set of read transactions
// that will en-mass have Rollback() (for the read set) called on
// them when TxGroup.Finish() is invoked.
// Alternatively, TxGroup.Abort() will call Rollback()
// on all Tx group memebers.
//
// It used to have writes but we never actually used that because
// of the Qcx needing to make every commit get its own transaction.
type TxGroup struct {
	mu       sync.Mutex
	fac      *TxFactory
	reads    []Tx
	finished bool

	all map[grpkey]Tx
}

type grpkey struct {
	index string
	shard uint64
}

func mustHaveIndexShard(o *Txo) {
	if o.Index == nil || o.Index.name == "" {
		vprint.PanicOn("index must be set on Txo")
	}
}

func (g *TxGroup) AlreadyHaveTx(o Txo) (tx Tx, already bool) {
	mustHaveIndexShard(&o)
	g.mu.Lock()
	defer g.mu.Unlock()
	key := grpkey{index: o.Index.name, shard: o.Shard}
	tx, already = g.all[key]
	return
}

func (g *TxGroup) String() (r string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.reads) == 0 {
		return "<empty-TxGroup>"
	}
	r += "\n"
	for i, tx := range g.reads {
		r += fmt.Sprintf("[%v]read: %#v,\n", i, tx)
	}
	return r
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
func (g *TxGroup) AddTx(tx Tx, o Txo) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.finished {
		vprint.PanicOn("in TxGroup.Finish(): TxGroup already finished")
	}
	if NilInside(tx) {
		vprint.PanicOn("Cannot add nil Tx to TxGroup")
	}

	g.reads = append(g.reads, tx)

	key := grpkey{index: o.Index.name, shard: o.Shard}
	prior, ok := g.all[key]
	if ok {
		vprint.PanicOn(fmt.Sprintf("already have Tx in group for this, we should have re-used it! prior is '%v'; tx='%v'", prior, tx))
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
		vprint.PanicOn("in TxGroup.Finish(): TxGroup already finished")
	}
	g.finished = true
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
}

func (f *TxFactory) NewTx(o Txo) (txn Tx) {
	defer func() {
		if globalUseStatTx {
			txn = newStatTx(txn)
		}
	}()

	indexName := ""
	if o.Index != nil {
		indexName = o.Index.name
	}

	if o.Fragment != nil {
		if o.Fragment.index() != indexName {
			vprint.PanicOn(fmt.Sprintf("inconsistent NewTx request: o.Fragment.index='%v' but indexName='%v'", o.Fragment.index(), indexName))
		}
		if o.Fragment.shard != o.Shard {
			vprint.PanicOn(fmt.Sprintf("inconsistent NewTx request: o.Fragment.shard='%v' but o.Shard='%v'", o.Fragment.shard, o.Shard))
		}
	}

	// look up in the collection of open databases, and get our
	// per-shard database. Opens a new one if needed.
	dbs, err := f.dbPerShard.GetDBShard(indexName, o.Shard, o.Index)
	vprint.PanicOn(err)

	if dbs.Shard != o.Shard {
		vprint.PanicOn(fmt.Sprintf("asked for o.Shard=%v but got dbs.Shard=%v", int(o.Shard), int(dbs.Shard)))
	}
	//vv("got dbs='%p' for o.Index='%v'; shard='%v'; dbs.typ='%#v'; dbs.W='%#v'", dbs, o.Index.name, o.Shard, dbs.typ, dbs.W)
	o.dbs = dbs

	tx, err := dbs.NewTx(o.Write, indexName, o)
	if err != nil {
		vprint.PanicOn(errors.Wrap(err, "dbs.NewTx transaction errored"))
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
	}
	vprint.PanicOn(fmt.Sprintf("unhandled ty '%v' in txtype.String()", int(ty)))
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
				vprint.PanicOn(fmt.Sprintf("info was nil for path = '%v'", path))
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

var _ = anyGlobalDBWrappersStillOpen // happy linter

func anyGlobalDBWrappersStillOpen() bool {
	if globalRoaringReg.Size() != 0 {
		return true
	}
	if globalRbfDBReg.Size() != 0 {
		return true
	}
	return false
}

func (f *TxFactory) hasRoaring() bool {
	return f.typ == roaringTxn
}

func (f *TxFactory) hasRBF() bool {
	return f.typ == rbfTxn
}

var _ = (&TxFactory{}).hasRoaring // happy linter

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
