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
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/pkg/errors"
)

var _ = sort.Sort

// types to support a database file per shard

type DBHolder struct {
	Index map[string]*DBIndex
}

func NewDBHolder() *DBHolder {
	return &DBHolder{
		Index: make(map[string]*DBIndex),
	}
}

type DBIndex struct {
	Shard map[uint64]*DBShard
}

type DBWrapper interface {
	NewTx(write bool, initialIndexName string, o Txo) (tx Tx, err error)
	DeleteDBPath(dbs *DBShard) error
	Close() error
	DeleteFragment(index, field, view string, shard uint64, frag interface{}) error
	DeleteField(index, field, fieldPath string) error
	OpenListString() string
	OpenSnList() (sns []int64)
	Path() string
	HasData() (has bool, err error)
	SetHolder(h *Holder)
}

type DBRegistry interface {
	OpenDBWrapper(path string, doAllocZero bool) (DBWrapper, error)
}

type DBShard struct {
	HolderPath string

	Index string
	Shard uint64
	Open  bool

	// With RWMutex, the
	// writer who calls Lock() automatically gets priority over
	// any reader who arrives later, even if the lock is held
	// by a reader to start with.
	mut sync.RWMutex

	types      []txtype
	hasRoaring bool // if either of the types is roaringTxn

	W             []DBWrapper
	ParentDBIndex *DBIndex

	idx *Index
	per *DBPerShard

	useOpenList int
	closed      bool
}

func (dbs *DBShard) DeleteFragment(index, field, view string, shard uint64, frag interface{}) (err error) {
	for _, w := range dbs.W {
		err = w.DeleteFragment(index, field, view, shard, frag)
		if err != nil {
			return err
		}
	}
	return
}

func (dbs *DBShard) DeleteFieldFromStore(index, field, fieldPath string) (err error) {
	for _, w := range dbs.W {
		err = w.DeleteField(index, field, fieldPath)
		if err != nil {
			return err
		}
	}
	return
}

func (dbs *DBShard) Close() (err error) {
	for _, w := range dbs.W {
		err = w.Close()
		if err != nil {
			return err
		}
	}
	dbs.closed = true
	return
}

func (dbs *DBShard) HolderString() string {
	return dbs.HolderPath
}

// Cleanup must be called at every commit/rollback of a Tx, in
// order to release the read-write mutex that guarantees a single
// writer at a time. Each tx must take care to call cleanup()
// exactly once. examples:
//   tx.o.dbs.Cleanup(tx)
//   tx.Options().dbs.Cleanup(tx)
//
func (dbs *DBShard) Cleanup(tx Tx) {
	if dbs == nil {
		return // some tests are using Tx only, no dbs available.
	}
	if useRWLock {
		if !dbs.hasRoaring {
			if tx.Readonly() {
				dbs.mut.RUnlock()
			} else {
				dbs.mut.Unlock()
			}
		}
	}
}

// experimental feature, off for now.
const useRWLock = false

func (dbs *DBShard) NewTx(write bool, initialIndexName string, o Txo) (tx Tx, err error) {
	if useRWLock {
		// enforce only one writer at a time. The dbs.mut is held until
		// the Tx finishes.
		if !dbs.hasRoaring {
			if write {
				dbs.mut.Lock()
			} else {
				dbs.mut.RLock()
			}
		}
	}
	if o.dbs != dbs {
		panic(fmt.Sprintf("TxFactory.NewTx() should have set o.dbs(%p) to equal dbs(%p)", o.dbs, dbs))
	}
	if o.Shard != dbs.Shard {
		panic(fmt.Sprintf("shard disagreement! o.Shard='%v' but dbs.Shard='%v'", int(o.Shard), int(dbs.Shard)))
	}
	var txns []Tx

	for _, w := range dbs.W {
		tx, err = w.NewTx(write, initialIndexName, o)
		if err != nil {
			return nil, err
		}
		txns = append(txns, tx)
	}
	if len(txns) == 1 {
		return
	}
	// blue green
	return dbs.per.txf.newBlueGreenTx(txns[0], txns[1], o.Index, o), nil
}

func (dbs *DBShard) DeleteDBPath() (err error) {
	for _, w := range dbs.W {
		err = w.DeleteDBPath(dbs)
		if err != nil {
			return err
		}
	}
	return
}

type flatkey struct {
	index string
	shard uint64
}

type DBPerShard struct {
	Mu sync.Mutex

	HolderDir string

	dbh *DBHolder

	// just flat, not buried within the Node heirarchy.
	// Easily see how many we have.
	Flatmap map[flatkey]*DBShard

	types      []txtype
	hasRoaring bool

	txf    *TxFactory
	holder *Holder

	// which of our types is not-roaring, since
	// roaring doesn't keep a list of open Tx sn.
	// or default to the 2nd.
	useOpenList int
}

// HasData returns true if the database has at least one key.
// For roaring it returns true if we a fragment stored.
// The `which` argument is the index into the per.W slice. 0 for blue, 1 for green.
// If you pass 1, be sure you have a blue-green configuration.
func (per *DBPerShard) HasData(which int) (hasData bool, err error) {
	// has to aggregate across all available DBShard for each index and shard.

	for _, v := range per.Flatmap {
		hasData, err = v.W[which].HasData()
		if err != nil {
			return
		}
		if hasData {
			return
		}
	}
	return
}

func (per *DBPerShard) ListOpenString() (r string) {
	for _, v := range per.Flatmap {
		r += v.HolderPath + " -> " + v.W[per.useOpenList].OpenListString() + "\n"
	}
	return
}

func (per *DBPerShard) LoadExistingDBs() (err error) {
	idxs := per.holder.Indexes()

	for _, idx := range idxs {

		sos, err := per.txf.GetShardsForIndex(idx, "", true)
		if err != nil {
			return err
		}
		for _, shard := range sos {
			_, err := per.GetDBShard(idx.name, shard, idx)
			if err != nil {
				return errors.Wrap(err, "DBPerShard.LoadExistingDBs GetDBShard()")
			}
		}
	}
	return
}

func (txf *TxFactory) NewDBPerShard(types []txtype, holderDir string, holder *Holder) (d *DBPerShard) {

	useOpenList := 0
	hasRoaring := false
	if types[0] == roaringTxn {
		hasRoaring = true
	}
	if len(types) == 2 {
		// blue-green, avoid the empty roaring Tx open list.
		// Prefer B's open list if neither is roaring.
		if types[0] == roaringTxn || types[1] != roaringTxn {
			useOpenList = 1
		}
		if types[1] == roaringTxn {
			hasRoaring = true
		}
	}

	d = &DBPerShard{
		types:       types,
		HolderDir:   holderDir,
		holder:      holder,
		dbh:         NewDBHolder(),
		Flatmap:     make(map[flatkey]*DBShard),
		txf:         txf,
		useOpenList: useOpenList,
		hasRoaring:  hasRoaring,
	}
	return
}

func (per *DBPerShard) DeleteIndex(index string) (err error) {

	per.Mu.Lock()
	defer per.Mu.Unlock()

	dbi, ok := per.dbh.Index[index]
	if !ok {
		// since we lazily make indexes upon use by a Tx now, we won't
		// have an index for server/ TestQuerySQLUnary/test-20 to delete.
		// Don't freak out. Just return nil.
		return nil
	}
	for _, dbs := range dbi.Shard {
		err = dbs.Close()
		if err != nil {
			return errors.Wrap(err, "DBPerShard.DeleteIndex dbs.Close()")
		}
		for _, ty := range per.types {
			path := dbs.pathForType(ty)
			err = os.RemoveAll(path)
			if err != nil {
				return errors.Wrap(err, fmt.Sprintf("DBPerShard.DeleteIndex os.RemoveAll('%v')", path))
			}
		}
	}
	// allow the index to be created again anew.
	delete(per.dbh.Index, index)

	return
}

func (per *DBPerShard) DeleteFieldFromStore(index, field, fieldPath string) (err error) {
	per.Mu.Lock()
	defer func() {
		if fieldPath != "" {
			panicOn(os.RemoveAll(fieldPath))
		}
		per.Mu.Unlock()
	}()

	dbi, ok := per.dbh.Index[index]
	if !ok {
		// TestIndex_Existence_Delete in index_internal_test.go
		// will call us without having ever created a Tx or DB,
		// so we can't complain here.
		return nil
	}
	for _, dbs := range dbi.Shard {
		for _, w := range dbs.W {
			err := w.DeleteField(index, field, fieldPath)
			panicOn(err)
		}
	}
	return
}

func (per *DBPerShard) DeleteFragment(index, field, view string, shard uint64, frag *fragment) error {

	idx := per.txf.holder.Index(index)
	dbs, err := per.GetDBShard(index, shard, idx)
	if err != nil {
		return err
	}
	return dbs.DeleteFragment(index, field, view, shard, frag)
}

func (dbs *DBShard) DumpAll() {
	short := false
	fmt.Printf("\n============= begin DumpAll dbs=%p index='%v', shard=%v ========\n", dbs, dbs.Index, int(dbs.Shard))
	for i, ty := range dbs.types {
		_ = i
		tx, err := dbs.W[i].NewTx(!writable, "", Txo{Index: dbs.idx})
		panicOn(err)
		defer tx.Rollback()
		fmt.Printf("\n============= dumping dbs.W[%v] %v ========\n", i, ty)
		tx.Dump(short, dbs.Shard)

		switch ty {
		case roaringTxn:
		case rbfTxn:
		case lmdbTxn:
		case boltTxn:
		default:
			panic(fmt.Sprintf("unknown txtyp: '%v'", ty))
		}
	}
	fmt.Printf("\n============= end of DumpAll index='%v', shard=%v ========\n", dbs.Index, int(dbs.Shard))
}

func (per *DBPerShard) DumpAll() {
	per.Mu.Lock()
	defer per.Mu.Unlock()

	found1 := false
	for _, dbi := range per.dbh.Index {
		for _, dbs := range dbi.Shard {
			if dbs.Open {
				found1 = true
				dbs.DumpAll()
			}
		}
	}
	if !found1 {
		AlwaysPrintf("DBPerShard.DumpAll() sees no databases. dir='%v'", per.HolderDir)
	}
}

// if you know the shard, you can use this
// pathForType and prefixForType must be kept in sync!
func (dbs *DBShard) pathForType(ty txtype) string {
	// top level paths will end in "@@"

	// what here for roaring? well, roaringRegistrar.OpenDBWrapper()
	// is a no-op anyhow. so doesn't need to be correct atm.

	path := dbs.HolderPath + sep + dbs.Index + ".index.txstores@@@" + sep + "store" + ty.FileSuffix() + "@" + sep + fmt.Sprintf("shard.%04v%v", dbs.Shard, ty.FileSuffix())
	if ty == boltTxn {
		// special case:
		// bolt doesn't use a directory like the others, just a direct path.
		path += sep + "bolt.db"
	}
	return path
}

// if you don't know the shard, you have to use this.
// prefixForType and pathForType must be kept in sync!
func (per *DBPerShard) prefixForType(idx *Index, ty txtype) string {
	// top level paths will end in "@@"
	return per.HolderDir + sep + idx.name + ".index.txstores@@@" + sep + "store" + ty.FileSuffix() + "@" + sep
}

var ErrNoData = fmt.Errorf("no data")

func (per *DBPerShard) GetDBShard(index string, shard uint64, idx *Index) (dbs *DBShard, err error) {

	per.Mu.Lock()
	defer per.Mu.Unlock()

	dbi, ok := per.dbh.Index[index]
	if !ok {
		dbi = &DBIndex{
			Shard: make(map[uint64]*DBShard),
		}
		per.dbh.Index[index] = dbi
	}
	dbs, ok = dbi.Shard[shard]
	if dbs != nil && dbs.closed {
		if len(per.types) == 1 && per.types[0] == roaringTxn {
			// roaring txn are nil/fake anyway. Don't freak out.
		} else {
			panic(fmt.Sprintf("cannot retain closed dbs across holder ReOpen dbs='%p'; per.types[0]='%v'; len(per.types)=%v", dbs, per.types[0], len(per.types)))
		}
	}
	if !ok {
		dbs = &DBShard{
			types:         per.types,
			ParentDBIndex: dbi,
			Index:         index,
			Shard:         shard,
			HolderPath:    per.HolderDir,
			idx:           idx,
			per:           per,
			useOpenList:   per.useOpenList,
			hasRoaring:    per.hasRoaring,
		}
		dbi.Shard[shard] = dbs
	}
	if !dbs.Open {
		var registry DBRegistry
		for _, ty := range dbs.types {
			switch ty {
			case roaringTxn:
				registry = globalRoaringReg
			case rbfTxn:
				registry = globalRbfDBReg
			case lmdbTxn:
				registry = globalLMDBReg
			case boltTxn:
				registry = globalBoltReg
			default:
				panic(fmt.Sprintf("unknown txtyp: '%v'", ty))
			}
			path := dbs.pathForType(ty)
			w, err := registry.OpenDBWrapper(path, DetectMemAccessPastTx)
			panicOn(err)
			h := idx.Holder()
			w.SetHolder(h)
			dbs.Open = true
			if w != nil && len(dbs.W) == 0 {
				per.Flatmap[flatkey{index: index, shard: shard}] = dbs
			}
			dbs.W = append(dbs.W, w)
		}
	}
	return
}

func (per *DBPerShard) Del(dbs *DBShard) (err error) {
	per.Mu.Lock()
	defer per.Mu.Unlock()

	err = dbs.Close()
	if err != nil {
		return
	}
	panicOn(dbs.DeleteDBPath())
	delete(per.Flatmap, flatkey{index: dbs.Index, shard: dbs.Shard})

	// delete from the heirarchy
	delete(dbs.ParentDBIndex.Shard, dbs.Shard)
	return nil
}

func (per *DBPerShard) Close() (err error) {
	per.Mu.Lock()
	defer per.Mu.Unlock()

	for _, dbi := range per.dbh.Index {
		for _, dbs := range dbi.Shard {
			err = dbs.Close()
			panicOn(err)
		}
	}
	return
}

// DBPerShardGetShardsForIndex returns the shards for idx.
// If requireData, we open the database and see that it has a key, rather
// than assume that the database file presence is enough.
func (f *TxFactory) GetShardsForIndex(idx *Index, roaringViewPath string, requireData bool) (sliceOfShards []uint64, err error) {

	var shards [][]uint64
	for _, ty := range f.types {
		var slc []uint64
		slc, err = f.dbPerShard.TypedDBPerShardGetShardsForIndex(ty, idx, roaringViewPath, requireData)
		if err != nil {
			return
		}
		shards = append(shards, slc)
	}
	n := len(f.types)
	if n != 1 && n != 2 {
		panic(fmt.Sprintf("internal error. only green or blue/green supported. we see types len %v", n))
	}

	// Note: we don't actually know when the blue call and when the green call comes
	// through here. So if we are deleting a shard, we will see a difference earlier
	// in one than the other. TestAPI_ClearFlagForImportAndImportValues for example.
	// Hence we cannot do a blue-green check here for matching shards.

	// If we are populating blue from green, it does matter that we return green.
	return shards[n-1], nil
}

// if roaringViewPath is "" then for ty == roaringTxn we go to disk to discover
// all the view paths under idx for type ty.
// requireData means open the database file and verify that at least one key is set.
func (per *DBPerShard) TypedDBPerShardGetShardsForIndex(ty txtype, idx *Index, roaringViewPath string, requireData bool) (sliceOfShards []uint64, err error) {

	if ty == roaringTxn {
		rx := &RoaringTx{
			Index: idx,
		}
		if roaringViewPath == "" {
			fields := idx.Fields()
			for _, field := range fields {
				for _, view := range field.views() {
					sos, err := rx.SliceOfShards("", "", "", view.path)
					if err != nil {
						return nil,
							errors.Wrap(err, fmt.Sprintf(
								"TypedDBPerShardGetLocalShardsForIndex roaringTxn view.path='%v'", view.path))
					}
					sliceOfShards = append(sliceOfShards, sos...)
				}
			}
			return dedupShardSlice(sliceOfShards), nil
		}
		return rx.SliceOfShards("", "", "", roaringViewPath)
	}
	requiredSuffix := ty.FileSuffix()
	path := per.prefixForType(idx, ty)

	ignoreEmpty := false
	includeRoot := true
	dbf, err := listDirUnderDir(path, includeRoot, requiredSuffix, ignoreEmpty)
	panicOn(err)

	for _, nm := range dbf {

		base := filepath.Base(nm)

		splt := strings.Split(base, requiredSuffix)
		if len(splt) != 2 {
			panic(fmt.Sprintf("should have 2 parts: nm='%v', base(nm)='%v'; requiredSuffix='%v'", nm, base, requiredSuffix))
		}
		prefix := splt[0]
		const shardPrefix = "shard."
		const lenOfShardPrefix = len(shardPrefix)
		if !strings.HasPrefix(prefix, shardPrefix) {
			continue
		}

		// Parse filename into integer.
		shard, err := strconv.ParseUint(prefix[lenOfShardPrefix:], 10, 64)
		if err != nil {
			panicOn(err)
			continue
		}

		// exclude those without data?
		hasData := false

		if requireData {
			hasData, err = per.TypedIndexShardHasData(ty, idx, shard)
			if err != nil {
				return nil, err
			}
			if hasData {
				sliceOfShards = append(sliceOfShards, shard)
			}
		} else {
			// file presence is enough
			sliceOfShards = append(sliceOfShards, shard)
		}
	}
	return
}

func (per *DBPerShard) TypedIndexShardHasData(ty txtype, idx *Index, shard uint64) (hasData bool, err error) {
	whichty := 0
	if len(per.types) == 2 {
		if ty == per.types[1] {
			whichty = 1
		}
	}
	if ty != per.types[whichty] {
		return
	}

	// make the dbs if it doesn't get exist
	dbs, err := per.GetDBShard(idx.name, shard, idx)
	if err != nil {
		return false, errors.Wrap(err, fmt.Sprintf("DBPerShard.TypedIndexShardHasData() "+
			"per.GetDBShard(index='%v', shard='%v', ty='%v')", idx.name, shard, ty.String()))
	}

	return dbs.W[whichty].HasData()
}

func listDirUnderDir(root string, includeRoot bool, requiredSuffix string, ignoreEmpty bool) (files []string, err error) {

	if !dirExists(root) {
		return
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
				// re-opening an RBF database hit this, racing with a directory rename.
				// Don't freak out.
				return nil
			}
			if !info.IsDir() {
				// ignore files
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

// populateBlueFromGreen prepares for a blue_green run at startup time.
//
// It is called at the end of Holder.Open(). This allows the application
// of blue-green checking to pilosa instances that
// were previously run only with a single (solo) backend.
//
// PRE: This operation requires, at its start, either:
//
// (1) an empty blue database -- this allows transitioning from
//     a solo database to blue_green checking where the solo
//     becomes the green; or
//
// (2) that the blue data, if present, be logically
//     identical to the green data -- this allows one to restart
//     a pilosa that was already running in blue_green mode
//     and remain in blue_green mode.
//
// In either case, the goal to to finish populateBlueFromGreen()
// and have the exact same logical set of data in both backends.
//
// Why must the data be identical after Holder.Open() finishes?
// Otherwise subsequent blue-green checks have no hope of
// being accurate.
//
// The blue is the destination -- this is always types[0].
// The green source is always types[1]. The mnemonic is blue_geen.
// The blue is first, so it is in types[0]. The green
// is second, in types[1]. For example, with PILOSA_TXSRC=lmdb_roaring
// we have lmdb as blue, and roaring as green. The contents of
// lmdb must be empty or exactly match roaring. If lmdb
// starts empty, it will be populated from roaring by
// populateBlueFromGreen().
//
func (dbs *DBShard) populateBlueFromGreen() (err error) {

	n := len(dbs.W)
	if n != 2 {
		panic(fmt.Sprintf("populateBlueFromGreen did not find 2 open DBs: have %v", n))
	}

	dest := dbs.W[0] // blue
	src := dbs.W[1]  // green

	// copy all the key/container pairs.
	// Since a shard is fairly small, we think one Tx will suffice.

	readtx, err := src.NewTx(!writable, dbs.Index, Txo{Write: !writable, Index: dbs.idx, Shard: dbs.Shard})
	panicOn(err)
	defer readtx.Rollback()

	writetx, err := dest.NewTx(writable, dbs.Index, Txo{Write: writable, Index: dbs.idx, Shard: dbs.Shard})
	panicOn(err)
	defer writetx.Rollback()

	for _, fld := range dbs.idx.Fields() {
		field := fld.Name()
		for _, vw := range fld.views() {
			view := vw.name
			citer, _, err := readtx.ContainerIterator(dbs.Index, field, view, dbs.Shard, 0)
			if err != nil {
				// might be an empty fragment. If so, let's not freak out.
				if strings.Contains(err.Error(), "fragment not found") {
					continue
				} else {
					return errors.Wrap(err, "DBShard.populateBlueFromGreen readtx.ContainerIterator")
				}
			}

			for citer.Next() {
				ckey, rc := citer.Value()
				err := writetx.PutContainer(dbs.Index, field, view, dbs.Shard, ckey, rc)
				if err != nil {
					citer.Close()
					return errors.Wrap(err, "DBShard.populateBlueFromGreen writetx.PutContainer")
				}
			}
			citer.Close()
		}
	}
	err = writetx.Commit()
	if err != nil {
		return errors.Wrap(err, "writetx.Commit()")
	}
	return nil
}

// verifyBlueEqualsGreen checks that blue and green are identical.
func (dbs *DBShard) verifyBlueEqualsGreen() (err error) {

	n := len(dbs.W)
	if n != 2 {
		panic(fmt.Sprintf("verifyBlueEqualsGreen did not find 2 open DBs: have %v", n))
	}

	blue := dbs.W[0]
	green := dbs.W[1]

	greentx, err := green.NewTx(!writable, dbs.Index, Txo{Write: !writable, Index: dbs.idx, Shard: dbs.Shard})
	panicOn(err)
	defer greentx.Rollback()

	bluetx, err := blue.NewTx(!writable, dbs.Index, Txo{Write: !writable, Index: dbs.idx, Shard: dbs.Shard})
	panicOn(err)
	defer bluetx.Rollback()

	for _, fld := range dbs.idx.Fields() {
		field := fld.Name()
		for _, vw := range fld.views() {

			view := vw.name
			gCiter, _, err := greentx.ContainerIterator(dbs.Index, field, view, dbs.Shard, 0)
			if err != nil {
				if strings.Contains(err.Error(), "fragment not found") {
					continue
				} else {
					return errors.Wrap(err, "DBShard.verifyBlueEqualsGreen greentx.ContainerIterator")
				}
			}

			bCiter, _, err := bluetx.ContainerIterator(dbs.Index, field, view, dbs.Shard, 0)
			if err != nil {
				gCiter.Close()
				if bCiter != nil {
					bCiter.Close()
				}
				return errors.Wrap(err, "DBShard.verifyBlueEqualsGreen bluetx.ContainerIterator")
			}

			for gCiter.Next() {
				greenCkey, greenc := gCiter.Value()

				if !bCiter.Next() {
					bCiter.Close()
					gCiter.Close()
					return errors.Wrap(err, fmt.Sprintf("DBShard.verifyBlueEqualsGreen "+
						"sees missing blue container at index: '%v' field: '%v' view: '%v' "+
						"shard: '%v' the greenCkey: '%v'",
						dbs.Index, field, view, dbs.Shard, greenCkey))
				}
				blueCkey, bluec := bCiter.Value()

				if blueCkey != greenCkey {
					bCiter.Close()
					gCiter.Close()
					return fmt.Errorf("DBShard.verifyBlueEqualsGreen sees sequence-of-ckey "+
						"difference: blueCkey %v not equal to greenCkey %v at index: '%v' field: '%v' view: '%v' "+
						"shard: '%v'",
						blueCkey, greenCkey, dbs.Index, field, view, dbs.Shard)
				}
				nGreen := greenc.N()
				nBlue := bluec.N()
				if nBlue != nGreen {
					bCiter.Close()
					gCiter.Close()
					return errors.Wrap(err, fmt.Sprintf("DBShard.verifyBlueEqualsGreen "+
						"sees variation in blue at index: '%v' field: '%v' view: '%v' "+
						"shard: '%v' ckey: '%v' nHotGreen= %v nHotBlue= %v",
						dbs.Index, field, view, dbs.Shard, greenCkey, nGreen, nBlue))
				}
				err = bluec.BitwiseCompare(greenc)
				if err != nil {
					bCiter.Close()
					gCiter.Close()
					return errors.Wrap(err, fmt.Sprintf("DBShard.verifyBlueEqualsGreen "+
						"sees variation in blue at index: '%v' field: '%v' view: '%v' "+
						"shard: '%v' ckey: '%v' nHotGreen= %v nHotBlue= %v ; BitwiseCompare response: '%v'",
						dbs.Index, field, view, dbs.Shard, greenCkey, nGreen, nBlue, err))
				}
			}
			if bCiter.Next() {
				blueCkey, _ := bCiter.Value()
				bCiter.Close()
				gCiter.Close()
				return errors.Wrap(err, fmt.Sprintf("DBShard.verifyBlueEqualsGreen "+
					"sees extra blue container (not present in green) at index: '%v' field: '%v' view: '%v' "+
					"shard: '%v' the ckey: '%v'",
					dbs.Index, field, view, dbs.Shard, blueCkey))
			}
			bCiter.Close()
			gCiter.Close()
		}
	}

	return nil
}

func dedupShardSlice(sos []uint64) (r []uint64) {
	m := make(map[uint64]struct{})
	for _, s := range sos {
		m[s] = struct{}{}
	}
	for k := range m {
		r = append(r, k)
	}
	return
}
