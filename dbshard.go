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

	rbfcfg "github.com/molecula/featurebase/v2/rbf/cfg"
	txkey "github.com/molecula/featurebase/v2/short_txkey"
	"github.com/molecula/featurebase/v2/storage"
	"github.com/pkg/errors"

	. "github.com/molecula/featurebase/v2/vprint" // nolint:staticcheck
)

var _ = sort.Sort

const (
	// backendsDir is the default backends directory used to store the
	// data for each backend.
	backendsDir = "backends"
)

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
	//needed for restore
	CloseDB() error
	OpenDB() error
}

type DBRegistry interface {
	OpenDBWrapper(path string, doAllocZero bool, cfg *storage.Config) (DBWrapper, error)
}

type DBShard struct {
	HolderPath string

	Index string
	Shard uint64
	Open  bool

	// With RWMutex, the blue-green Tx can start and commit
	// atomically.
	mut sync.RWMutex

	types      []txtype
	stypes     []string
	hasRoaring bool // if either of the types is roaringTxn

	W             []DBWrapper
	ParentDBIndex *DBIndex

	idx *Index
	per *DBPerShard

	useOpenList int
	closed      bool

	isBlueGreen bool
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
	//vv("gid %v top of DBShard %v Cleanup for tx.Sn = %v; dbs=%p; is 2nd: %v; type='%v'; dbs.stypes='%#v'", curGID(), dbs.Shard, tx.Sn(), dbs, tx.Type() == dbs.stypes[1], tx.Type(), dbs.stypes)
	if !dbs.hasRoaring {
		if dbs.isBlueGreen {
			// only release on the 2nd Tx's cleanup
			if tx.Type() == dbs.stypes[1] {
				if tx.Readonly() {
					dbs.mut.RUnlock()
					//vv("gid %v released read-lock on shard %v", curGID(), dbs.Shard)
				} else {
					dbs.mut.Unlock()
					//vv("gid %v released write-lock on shard %v", curGID(), dbs.Shard)
				}
			}
		}
	}
}

func (dbs *DBShard) NewTx(write bool, initialIndexName string, o Txo) (tx Tx, err error) {

	if dbs.isBlueGreen {
		// enforce only one writer at a time. The dbs.mut is held until
		// the Tx finishes. This makes the two Tx in the blue-green Tx atomic.
		if !dbs.hasRoaring {
			if write {
				//vv("shard %v about to write lock by gid %v; stack =\n%v", dbs.Shard, curGID(), stack())
				dbs.mut.Lock()
				//vv("shard %v was write locked by gid %v; stack =\n%v", dbs.Shard, curGID(), stack())
			} else {
				//vv("shard %v about to be read locked by gid %v; stack=\n%v", dbs.Shard, curGID(), stack())
				dbs.mut.RLock()
				//vv("shard %v was read locked by gid %v; stack=\n%v", dbs.Shard, curGID(), stack())
			}
		}
	}
	if o.dbs != dbs {
		PanicOn(fmt.Sprintf("TxFactory.NewTx() should have set o.dbs(%p) to equal dbs(%p)", o.dbs, dbs))
	}
	if o.Shard != dbs.Shard {
		PanicOn(fmt.Sprintf("shard disagreement! o.Shard='%v' but dbs.Shard='%v'", int(o.Shard), int(dbs.Shard)))
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
	tx, err = dbs.per.txf.newBlueGreenTx(txns[0], txns[1], o.Index, o), nil
	//vv("dbshard returning blue-green tx sn %v", tx.Sn())
	return
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

	// cache the shards per index to avoid excessive
	// directory scans of the index directory. Keep per
	// txtype to allow blue-green migrate open to be fast too.
	// Keep it up-to-date as we add shards to avoid doing
	// a filesystem rescan on new shard creation.
	//
	// txtype -> index -> *shardSet
	index2shards map[txtype]map[string]*shardSet

	isBlueGreen bool

	StorageConfig *storage.Config
	RBFConfig     *rbfcfg.Config
}

func newIndex2Shards() (r map[txtype]map[string]*shardSet) {
	r = make(map[txtype]map[string]*shardSet)
	return
}

type shardSet struct {
	shardsMap map[uint64]bool
	shardsVer int64 // increment with each change.

	// give out readonly to repeated consumers if
	// readonlyVer == shardsVer
	readonly    map[uint64]bool
	readonlyVer int64
}

func (a *shardSet) unionInPlace(b *shardSet) {
	shards := b.CloneMaybe()
	for shard := range shards {
		a.add(shard)
	}
}

func (a *shardSet) equals(b *shardSet) bool {
	if len(a.shardsMap) != len(b.shardsMap) {
		return false
	}
	for shardInA := range a.shardsMap {
		_, ok := b.shardsMap[shardInA]
		if !ok {
			return false
		}
	}
	return true

}

func (a *shardSet) shards() []uint64 {
	s := make([]uint64, 0, len(a.shardsMap))
	for si := range a.shardsMap {
		s = append(s, si)
	}
	return s
}

func (ss *shardSet) String() (r string) {
	r = "["
	for k := range ss.shardsMap {
		r += fmt.Sprintf("%v, ", k)
	}
	r += "]"
	return
}

func (ss *shardSet) add(shard uint64) {
	_, already := ss.shardsMap[shard]
	if !already {
		ss.shardsMap[shard] = true
		ss.shardsVer++
	}
}

// CloneMaybe maintains a re-usable readonly version
// ss.shards that can be returned to multiple goroutine
// reads as it will never change. A copy is only made
// once for each change in the shard set.
func (ss *shardSet) CloneMaybe() map[uint64]bool {

	if ss.readonlyVer == ss.shardsVer {
		return ss.readonly
	}

	// readonlyVer is out of date.
	// readonly needs update. We cannot
	// modify the readonly map in place;
	// must make a fully new copy here.
	ss.readonly = make(map[uint64]bool)

	for k, v := range ss.shardsMap {
		ss.readonly[k] = v
	}
	ss.readonlyVer = ss.shardsVer
	return ss.readonly
}

func newShardSet() *shardSet {
	return &shardSet{
		shardsMap: make(map[uint64]bool),
	}
}
func newShardSetFromMap(m map[uint64]bool) *shardSet {
	return &shardSet{
		shardsMap: m,
		shardsVer: 1,
	}
}

// HasData returns true if the database has at least one key.
// For roaring it returns true if we a fragment stored.
// The `which` argument is the index into the per.W slice. 0 for blue, 1 for green.
// If you pass 1, be sure you have a blue-green configuration.
func (per *DBPerShard) HasData(which int) (hasData bool, err error) {
	// has to aggregate across all available DBShard for each index and shard.

	if per.types[which] == roaringTxn {
		return per.RoaringHasData() // this needs to be made accurate
	}

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

func (per *DBPerShard) RoaringHasData() (bool, error) {
	idxs := per.holder.Indexes()
	const requireData = true
	for _, idx := range idxs {
		shards, err := per.TypedDBPerShardGetShardsForIndex(roaringTxn, idx, "", requireData)
		if err != nil {
			return false, err
		}
		if len(shards) > 0 {
			return true, nil
		}
	}
	return false, nil
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

		shardset, err := per.txf.GetShardsForIndex(idx, "", true)
		if err != nil {
			return err
		}
		for shard := range shardset {
			_, err := per.GetDBShard(idx.name, shard, idx)
			if err != nil {
				return errors.Wrap(err, "DBPerShard.LoadExistingDBs GetDBShard()")
			}
		}
	}
	return
}

func (txf *TxFactory) NewDBPerShard(types []txtype, holderDir string, holder *Holder) (d *DBPerShard) {
	if holder.cfg == nil || holder.cfg.RBFConfig == nil || holder.cfg.StorageConfig == nil {
		PanicOn("must have holder.cfg.RBFConfig and holder.cfg.StorageConfig set here")
	}

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
		types:         types,
		HolderDir:     holderDir,
		holder:        holder,
		dbh:           NewDBHolder(),
		Flatmap:       make(map[flatkey]*DBShard),
		txf:           txf,
		useOpenList:   useOpenList,
		hasRoaring:    hasRoaring,
		isBlueGreen:   len(types) > 1,
		index2shards:  newIndex2Shards(),
		StorageConfig: holder.cfg.StorageConfig,
		RBFConfig:     holder.cfg.RBFConfig,
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
			delete(per.index2shards[ty], index)
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
			_ = os.RemoveAll(fieldPath)
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
			if e := w.DeleteField(index, field, fieldPath); e != nil && err == nil {
				err = errors.Wrap(e, "DeleteFieldFromStore()")
			}
		}
	}
	return err
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
		PanicOn(err)
		defer tx.Rollback()
		fmt.Printf("\n============= dumping dbs.W[%v] %v ========\n", i, ty)
		tx.Dump(short, dbs.Shard)

		switch ty {
		case roaringTxn:
		case rbfTxn:
		case boltTxn:
		default:
			PanicOn(fmt.Sprintf("unknown txtyp: '%v'", ty))
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

	path := dbs.HolderPath + sep + dbs.Index + sep + backendsDir + sep + ty.DirectoryName() + sep + fmt.Sprintf("shard.%04v", dbs.Shard)
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
	return per.HolderDir + sep + idx.name + sep + backendsDir + sep + ty.DirectoryName() + sep
}

var ErrNoData = fmt.Errorf("no data")

// keep our cache of shards up-to-date in memory; after the initial
// directory scan, this is all we should we need. Prevents us from
// doing additional, expensive, directory scans.
//
// Caller must hold per.Mu.Lock() already.
func (per *DBPerShard) updateIndex2ShardCacheWithNewShard(dbs *DBShard) {

	for _, ty := range dbs.types {
		mapIndex2shardSet, ok := per.index2shards[ty]
		if !ok {
			mapIndex2shardSet = make(map[string]*shardSet)
			per.index2shards[ty] = mapIndex2shardSet
		}
		// INVAR: mapIndex2shardSet is good, but may be an empty map

		shardset, ok := mapIndex2shardSet[dbs.Index]
		if !ok {
			shardset = newShardSet()
			mapIndex2shardSet[dbs.Index] = shardset
		}
		// INVAR: shardset is present, not nil; a map that can be added to.
		shardset.add(dbs.Shard)
	}
}

func (per *DBPerShard) GetDBShard(index string, shard uint64, idx *Index) (dbs *DBShard, err error) {
	per.Mu.Lock()
	defer per.Mu.Unlock()
	return per.unprotectedGetDBShard(index, shard, idx)
}

func (per *DBPerShard) unprotectedGetDBShard(index string, shard uint64, idx *Index) (dbs *DBShard, err error) {

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
			PanicOn(fmt.Sprintf("cannot retain closed dbs across holder ReOpen dbs='%p'; per.types[0]='%v'; len(per.types)=%v", dbs, per.types[0], len(per.types)))
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
			isBlueGreen:   len(per.types) > 1,
		}
		dbs.stypes = make([]string, len(per.types))
		for i, ty := range per.types {
			dbs.stypes[i] = ty.String()
		}

		dbi.Shard[shard] = dbs
		per.updateIndex2ShardCacheWithNewShard(dbs)
	}
	if !dbs.Open {
		var registry DBRegistry
		for _, ty := range dbs.types {
			switch ty {
			case roaringTxn:
				registry = globalRoaringReg
			case rbfTxn:
				registry = globalRbfDBReg
				registry.(*rbfDBRegistrar).SetRBFConfig(per.RBFConfig)
			case boltTxn:
				registry = globalBoltReg
			default:
				PanicOn(fmt.Sprintf("unknown txtyp: '%v'", ty))
			}
			path := dbs.pathForType(ty)
			w, err := registry.OpenDBWrapper(path, DetectMemAccessPastTx, per.StorageConfig)
			PanicOn(err)
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
	PanicOn(dbs.DeleteDBPath())
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
			PanicOn(err)
		}
	}
	return
}

// DBPerShardGetShardsForIndex returns the shards for idx.
// If requireData, we open the database and see that it has a key, rather
// than assume that the database file presence is enough.
func (f *TxFactory) GetShardsForIndex(idx *Index, roaringViewPath string, requireData bool) (map[uint64]bool, error) {

	n := len(f.types)
	if n != 1 && n != 2 {
		PanicOn(fmt.Sprintf("internal error. only green or blue/green supported. we see types len %v", n))
	}

	var shards []map[uint64]bool
	for _, ty := range f.types {
		ss, err := f.dbPerShard.TypedDBPerShardGetShardsForIndex(ty, idx, roaringViewPath, requireData)
		if err != nil {
			return nil, err
		}
		shards = append(shards, ss)
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
// The returned sliceOfShards should not be modified. We will cache it for subsequent
// queries.
//
// when a new DBShard is made, we will update the list of shards then. Thus
// the per.index2shard should always be up to date AFTER the first call here.
//
// Note: we cannot here call GetView2ShardsMapForIndex() because that only ever
// returns the green data and we are used during migration for both blue
// and green.
//
func (per *DBPerShard) TypedDBPerShardGetShardsForIndex(ty txtype, idx *Index, roaringViewPath string, requireData bool) (shardMap map[uint64]bool, err error) {

	// use the cache, always
	per.Mu.Lock()
	defer per.Mu.Unlock()

	if ty == roaringTxn && roaringViewPath != "" {
		shardMap, err := roaringMapOfShards(roaringViewPath)
		if err != nil {
			return nil, err
		}
		return shardMap, nil
	}

	i2ss, ok := per.index2shards[ty]
	if !ok {
		// index -> shardSet
		i2ss = make(map[string]*shardSet)
		per.index2shards[ty] = i2ss
	}
	// INVAR: i2ss is good, but may be an empty map

	ss, ok := i2ss[idx.name]
	if ok {
		return ss.CloneMaybe(), nil
	}
	// INVAR: cache miss, and index2shards[ty] exists.

	// gotta read shards from disk directory layout.
	setOfShards := newShardSet()
	per.index2shards[ty][idx.name] = setOfShards

	// Upon return, cache the setOfShards value and reuse it next time

	if ty == roaringTxn {
		// INVAR: roaringViewPath == "", because the other case is
		// handled above.
		fields := idx.Fields()
		for _, field := range fields {
			for _, view := range field.views() {
				shardMap, err := roaringMapOfShards(view.path)
				if err != nil {
					return nil,
						errors.Wrap(err, fmt.Sprintf(
							"TypedDBPerShardGetLocalShardsForIndex roaringTxn view.path='%v'", view.path))
				}
				for shard := range shardMap {
					setOfShards.add(shard)
				}
			}
		}
		return setOfShards.CloneMaybe(), nil
	}
	// INVAR: not-roaring.

	path := per.prefixForType(idx, ty)

	ignoreEmpty := false
	includeRoot := true
	dbf, err := listDirUnderDir(path, includeRoot, ignoreEmpty)
	PanicOn(err)

	for _, nm := range dbf {
		base := filepath.Base(nm)

		// We're only interested in "shard.*" files, so skip everything else.
		const shardPrefix = "shard."
		const lenOfShardPrefix = len(shardPrefix)
		if !strings.HasPrefix(base, shardPrefix) {
			continue
		}

		// Parse filename into integer.
		shard, err := strconv.ParseUint(base[lenOfShardPrefix:], 10, 64)
		if err != nil {
			PanicOn(err)
			continue
		}

		// exclude those without data?
		hasData := false

		if requireData {
			hasData, err = per.unprotectedTypedIndexShardHasData(ty, idx, shard)
			if err != nil {
				return nil, err
			}
			if hasData {
				setOfShards.add(shard)
			}
		} else {
			// file presence is enough
			setOfShards.add(shard)
		}
	}
	return setOfShards.CloneMaybe(), nil
}

func (per *DBPerShard) unprotectedTypedIndexShardHasData(ty txtype, idx *Index, shard uint64) (hasData bool, err error) {
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
	dbs, err := per.unprotectedGetDBShard(idx.name, shard, idx)
	if err != nil {
		return false, errors.Wrap(err, fmt.Sprintf("DBPerShard.TypedIndexShardHasData() "+
			"per.GetDBShard(index='%v', shard='%v', ty='%v')", idx.name, shard, ty.String()))
	}

	return dbs.W[whichty].HasData()
}

func listDirUnderDir(root string, includeRoot bool, ignoreEmpty bool) (files []string, err error) {
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
				files = append(files, path[n:])
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
// is second, in types[1]. For example, with PILOSA_STORAGE_BACKEND=bolt_roaring
// we have bolt as blue, and roaring as green. The contents of
// bolt must be empty or exactly match roaring. If bolt
// starts empty, it will be populated from roaring by
// populateBlueFromGreen().
//
func (dbs *DBShard) populateBlueFromGreen() (err error) {

	n := len(dbs.W)
	if n != 2 {
		PanicOn(fmt.Sprintf("populateBlueFromGreen did not find 2 open DBs: have %v", n))
	}

	dest := dbs.W[0] // blue
	src := dbs.W[1]  // green

	// copy all the key/container pairs.
	// Since a shard is fairly small, we think one Tx will suffice.

	readtx, err := src.NewTx(!writable, dbs.Index, Txo{Write: !writable, Index: dbs.idx, Shard: dbs.Shard})
	PanicOn(err)
	defer readtx.Rollback()

	writetx, err := dest.NewTx(writable, dbs.Index, Txo{Write: writable, Index: dbs.idx, Shard: dbs.Shard})
	PanicOn(err)
	defer writetx.Rollback()

	ctWriteCount := 0

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
					writetx.Rollback()
					return errors.Wrap(err, "DBShard.populateBlueFromGreen readtx.ContainerIterator")
				}
			}

			for citer.Next() {
				ckey, rc := citer.Value()
				err := writetx.PutContainer(dbs.Index, field, view, dbs.Shard, ckey, rc)
				if err != nil {
					citer.Close()
					writetx.Rollback()
					return errors.Wrap(err, "DBShard.populateBlueFromGreen writetx.PutContainer")
				}

				ctWriteCount++
				if ctWriteCount%1000 == 1 {

					// regularly commiting smaller batches and the first batch as soon as
					// possible massively speeds up writing to bolt.
					//
					// reference: https://github.com/boltdb/bolt/issues/94
					//
					//  benbjohnson commented on Mar 25, 2014
					//  "Bulk loading more than 1000 items at a time is very slow. This is because nodes
					//   are not splitting before commit which causes large memmove() operations during insertion."
					// runtime.memmove is taking all of the time in our pprof profile, when copying rbf to bolt, so we suspect it is this.
					//
					err = writetx.Commit()
					if err != nil {
						citer.Close()
						writetx.Rollback()
						return errors.Wrap(err, "DBShard.populateBlueFromGreen writetx.Commit")
					}
					writetx, err = dest.NewTx(writable, dbs.Index, Txo{Write: writable, Index: dbs.idx, Shard: dbs.Shard})
					if err != nil {
						citer.Close()
						writetx.Rollback()
						return errors.Wrap(err, "DBShard.populateBlueFromGreen writetx.NewTx inside citer.Next() loop")
					}
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
		PanicOn(fmt.Sprintf("verifyBlueEqualsGreen did not find 2 open DBs: have %v", n))
	}

	blue := dbs.W[0]
	green := dbs.W[1]

	greentx, err := green.NewTx(!writable, dbs.Index, Txo{Write: !writable, Index: dbs.idx, Shard: dbs.Shard})
	PanicOn(err)
	defer greentx.Rollback()

	bluetx, err := blue.NewTx(!writable, dbs.Index, Txo{Write: !writable, Index: dbs.idx, Shard: dbs.Shard})
	PanicOn(err)
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

type FieldView2Shards struct {
	// field -> view -> *shardSet
	m map[string]map[string]*shardSet
}

func (vs *FieldView2Shards) getViewsForField(field string) map[string]*shardSet {
	return vs.m[field]
}

func (vs *FieldView2Shards) has(field, view string, shard uint64) bool {
	vw, ok := vs.m[field]
	if !ok {
		return false
	}
	ss, ok := vw[view]
	if !ok {
		return false
	}
	shardMap := ss.CloneMaybe()
	return shardMap[shard]
}

func (vs *FieldView2Shards) addViewShardSet(fv txkey.FieldView, ss *shardSet) {

	f, ok := vs.m[fv.Field]
	if !ok {
		f = make(map[string]*shardSet)
		vs.m[fv.Field] = f
	}
	// INVAR: f is ready to take ss.

	// existing stuff to merge with?
	prior, ok := f[fv.View]
	if !ok {
		f[fv.View] = ss
		return
	}
	// merge ss and prior. No need to put the union back into f[fv.View]
	// because prior is a pointer.
	prior.unionInPlace(ss)
}

func (a *FieldView2Shards) equals(b *FieldView2Shards) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	if len(a.m) != len(b.m) {
		return false
	}
	for field, viewmapA := range a.m {
		viewmapB, ok := b.m[field]
		if !ok {
			return false
		}
		if len(viewmapB) != len(viewmapA) {
			return false
		}
		for k, va := range viewmapA {
			vb, ok := viewmapB[k]
			if !ok {
				return false
			}
			if !va.equals(vb) {
				return false
			}
		}
	}
	return true
}

func NewFieldView2Shards() *FieldView2Shards {
	return &FieldView2Shards{
		m: make(map[string]map[string]*shardSet), // expected response from GetView2ShardMapForIndex
	}
}

func (vs *FieldView2Shards) addShard(fv txkey.FieldView, shard uint64) {
	viewmap, ok := vs.m[fv.Field]
	if !ok {
		viewmap = make(map[string]*shardSet)
		vs.m[fv.Field] = viewmap
	}
	ss, ok := viewmap[fv.View]
	if !ok {
		ss = newShardSet()
		viewmap[fv.View] = ss
	}
	ss.add(shard)
}

func (vs *FieldView2Shards) String() (r string) {
	r = "\n"
	for field, viewmap := range vs.m {
		for view, shards := range viewmap {
			r += fmt.Sprintf("field '%v' view:'%v' shards:%v\n", field, view, shards)
		}
	}
	r += "\n"
	return
}

func (vs *FieldView2Shards) removeField(name string) {
	delete(vs.m, name)
}

// Note: cannot call this during migration, because
// it only ever returns the green shards if we are in blue-green.
func (per *DBPerShard) GetFieldView2ShardsMapForIndex(idx *Index) (vs *FieldView2Shards, err error) {

	// for blue-green, it does matter that we return green, so we can migrate from it.
	ty := per.types[0]
	if per.isBlueGreen {
		ty = per.types[1]
	}

	switch ty {
	case roaringTxn:
		return roaringGetFieldView2Shards(idx)
	default:
		vs = NewFieldView2Shards()

		shardMap, err := per.TypedDBPerShardGetShardsForIndex(ty, idx, "", true)
		if err != nil {
			return nil, err
		}

		for shard := range shardMap {
			dbs, err := per.GetDBShard(idx.name, shard, idx)
			if err != nil {
				return nil, errors.Wrap(err, "DBPerShard.GetFieldView2ShardsMapForIndex GetDBShard()")
			}
			fieldviews, err := dbs.AllFieldViews()
			if err != nil {
				return nil, errors.Wrap(err, "DBPerShard.GetFieldView2ShardsMapForIndex dbs.AllFieldViews()")
			}
			for _, fv := range fieldviews {
				vs.addShard(fv, shard)
			}
		}
	}

	return
}

func (dbs *DBShard) AllFieldViews() (fvs []txkey.FieldView, err error) {

	tx, err := dbs.NewTx(!writable, dbs.idx.name, Txo{Write: !writable, Shard: dbs.Shard, Index: dbs.idx, dbs: dbs})
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("dbshard.NewTx for index '%v', shard %v", dbs.idx.name, dbs.Shard))
	}
	defer tx.Rollback()
	return tx.GetSortedFieldViewList(dbs.idx, dbs.Shard)
}
