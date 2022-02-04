// Copyright 2021 Molecula Corp. All rights reserved.
package pilosa

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	rbfcfg "github.com/molecula/featurebase/v3/rbf/cfg"
	txkey "github.com/molecula/featurebase/v3/short_txkey"
	"github.com/molecula/featurebase/v3/storage"
	"github.com/pkg/errors"

	"github.com/molecula/featurebase/v3/vprint"
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
	Close() error
	DeleteFragment(index, field, view string, shard uint64, frag interface{}) error
	DeleteField(index, field, fieldPath string) error
	OpenListString() string
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

	typ  txtype
	styp string

	W             DBWrapper
	ParentDBIndex *DBIndex

	idx *Index
	per *DBPerShard

	closed bool
}

func (dbs *DBShard) DeleteFragment(index, field, view string, shard uint64, frag interface{}) (err error) {
	if index != dbs.Index {
		return fmt.Errorf("DeleteFragment called on DBShard for %q with index %q", dbs.Index, index)
	}
	if shard != dbs.Shard {
		return fmt.Errorf("DeleteFragment called on DBShard for %d with shard %d", dbs.Shard, shard)
	}
	return dbs.W.DeleteFragment(index, field, view, shard, frag)
}

func (dbs *DBShard) DeleteFieldFromStore(index, field, fieldPath string) (err error) {
	if index != dbs.Index {
		return fmt.Errorf("DeleteFieldFromStore called on DBShard for %q with index %q", dbs.Index, index)
	}
	return dbs.W.DeleteField(index, field, fieldPath)
}

func (dbs *DBShard) Close() (err error) {
	dbs.closed = true
	return dbs.W.Close()
}

func (dbs *DBShard) NewTx(write bool, initialIndexName string, o Txo) (tx Tx, err error) {
	if initialIndexName != dbs.Index {
		return nil, fmt.Errorf("NewTx called on DBShard for %q with index %q", dbs.Index, initialIndexName)
	}
	if o.dbs != dbs {
		return nil, fmt.Errorf("dbs mismatch: TxFactory.NewTx() should have set o.dbs(%p) to equal dbs(%p)", o.dbs, dbs)
	}
	if o.Shard != dbs.Shard {
		return nil, fmt.Errorf("shard disagreement: o.Shard='%v' but dbs.Shard='%v'", int(o.Shard), int(dbs.Shard))
	}
	return dbs.W.NewTx(write, initialIndexName, o)
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

	typ txtype

	txf    *TxFactory
	holder *Holder

	// cache the shards per index to avoid excessive
	// directory scans of the index directory.
	// Keep it up-to-date as we add shards to avoid doing
	// a filesystem rescan on new shard creation.
	//
	// index -> *shardSet
	index2shards map[string]*shardSet

	StorageConfig *storage.Config
	RBFConfig     *rbfcfg.Config
}

func newIndex2Shards() (r map[string]*shardSet) {
	r = make(map[string]*shardSet)
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

func (txf *TxFactory) NewDBPerShard(typ txtype, holderDir string, holder *Holder) (d *DBPerShard) {
	if holder.cfg == nil || holder.cfg.RBFConfig == nil || holder.cfg.StorageConfig == nil {
		vprint.PanicOn("must have holder.cfg.RBFConfig and holder.cfg.StorageConfig set here")
	}

	d = &DBPerShard{
		typ:           typ,
		HolderDir:     holderDir,
		holder:        holder,
		dbh:           NewDBHolder(),
		Flatmap:       make(map[flatkey]*DBShard),
		txf:           txf,
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
		path := dbs.pathForType(per.typ)
		err = os.RemoveAll(path)
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("DBPerShard.DeleteIndex os.RemoveAll('%v')", path))
		}
		delete(per.index2shards, index)
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
		if e := dbs.W.DeleteField(index, field, fieldPath); e != nil && err == nil {
			err = errors.Wrap(e, "DeleteFieldFromStore()")
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

// if you know the shard, you can use this
// pathForType and prefixForType must be kept in sync!
func (dbs *DBShard) pathForType(ty txtype) string {
	// top level paths will end in "@@"

	// what here for roaring? well, roaringRegistrar.OpenDBWrapper()
	// is a no-op anyhow. so doesn't need to be correct atm.

	path := dbs.HolderPath + sep + dbs.Index + sep + backendsDir + sep + ty.DirectoryName() + sep + fmt.Sprintf("shard.%04v", dbs.Shard)
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
	shardset, ok := per.index2shards[dbs.Index]
	if !ok {
		shardset = newShardSet()
		per.index2shards[dbs.Index] = shardset
	}
	// INVAR: shardset is present, not nil; a map that can be added to.
	shardset.add(dbs.Shard)
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
		vprint.PanicOn(fmt.Sprintf("cannot retain closed dbs across holder ReOpen dbs='%p'; per.typ='%v'", dbs, per.typ))
	}
	if !ok {
		dbs = &DBShard{
			typ:           per.typ,
			ParentDBIndex: dbi,
			Index:         index,
			Shard:         shard,
			HolderPath:    per.HolderDir,
			idx:           idx,
			per:           per,
		}
		dbs.styp = per.typ.String()
		dbi.Shard[shard] = dbs
		per.updateIndex2ShardCacheWithNewShard(dbs)
	}
	if !dbs.Open {
		var registry DBRegistry
		switch dbs.typ {
		case rbfTxn:
			registry = globalRbfDBReg
			registry.(*rbfDBRegistrar).SetRBFConfig(per.RBFConfig)
		default:
			vprint.PanicOn(fmt.Sprintf("unknown txtyp: '%v'", dbs.typ))
		}
		path := dbs.pathForType(dbs.typ)
		w, err := registry.OpenDBWrapper(path, DetectMemAccessPastTx, per.StorageConfig)
		vprint.PanicOn(err)
		h := idx.Holder()
		w.SetHolder(h)
		dbs.Open = true
		per.Flatmap[flatkey{index: index, shard: shard}] = dbs
		dbs.W = w
	}
	return dbs, nil
}

func (per *DBPerShard) Close() (err error) {
	per.Mu.Lock()
	defer per.Mu.Unlock()

	for _, dbi := range per.dbh.Index {
		for _, dbs := range dbi.Shard {
			err = dbs.Close()
			vprint.PanicOn(err)
		}
	}
	return
}

// DBPerShardGetShardsForIndex returns the shards for idx.
// If requireData, we open the database and see that it has a key, rather
// than assume that the database file presence is enough.
func (f *TxFactory) GetShardsForIndex(idx *Index, roaringViewPath string, requireData bool) (map[uint64]bool, error) {
	return f.dbPerShard.TypedDBPerShardGetShardsForIndex(f.typ, idx, roaringViewPath, requireData)
}

// requireData means open the database file and verify that at least one key is set.
// The returned sliceOfShards should not be modified. We will cache it for subsequent
// queries.
//
// when a new DBShard is made, we will update the list of shards then. Thus
// the per.index2shard should always be up to date AFTER the first call here.
//
func (per *DBPerShard) TypedDBPerShardGetShardsForIndex(ty txtype, idx *Index, roaringViewPath string, requireData bool) (shardMap map[uint64]bool, err error) {

	// use the cache, always
	per.Mu.Lock()
	defer per.Mu.Unlock()

	i2ss := per.index2shards

	ss, ok := i2ss[idx.name]
	if ok {
		return ss.CloneMaybe(), nil
	}
	// INVAR: cache miss, and index2shards[ty] exists.

	// gotta read shards from disk directory layout.
	setOfShards := newShardSet()
	per.index2shards[idx.name] = setOfShards

	// Upon return, cache the setOfShards value and reuse it next time

	path := per.prefixForType(idx, ty)

	ignoreEmpty := false
	includeRoot := true
	dbf, err := listDirUnderDir(path, includeRoot, ignoreEmpty)
	vprint.PanicOn(err)

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
			vprint.PanicOn(err)
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
	if ty != per.typ {
		return
	}

	// make the dbs if it doesn't get exist
	dbs, err := per.unprotectedGetDBShard(idx.name, shard, idx)
	if err != nil {
		return false, errors.Wrap(err, fmt.Sprintf("DBPerShard.TypedIndexShardHasData() "+
			"per.GetDBShard(index='%v', shard='%v', ty='%v')", idx.name, shard, ty.String()))
	}

	return dbs.W.HasData()
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

type FieldView2Shards struct {
	// field -> view -> *shardSet
	m map[string]map[string]*shardSet
}

func (vs *FieldView2Shards) getViewsForField(field string) map[string]*shardSet {
	return vs.m[field]
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

func (per *DBPerShard) GetFieldView2ShardsMapForIndex(idx *Index) (vs *FieldView2Shards, err error) {
	ty := per.typ

	switch ty {
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
