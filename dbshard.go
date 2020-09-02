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
}

type DBRegistry interface {
	OpenDBWrapper(path string, doAllocZero bool) (DBWrapper, error)
}

type DBShard struct {
	Path  string
	Index string
	Shard uint64
	Open  bool

	// With RWMutex, the
	// writer who calls Lock() automatically gets priority over
	// any reader who arrives later, even if the lock is held
	// by a reader to start with.
	mut sync.RWMutex

	types         []txtype
	W             []DBWrapper
	ParentDBIndex *DBIndex

	idx *Index
	per *DBPerShard

	useOpenList int
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
	return
}

func (dbs *DBShard) String() string {
	return dbs.Path
}

func (dbs *DBShard) NewTx(write bool, initialIndexName string, o Txo) (tx Tx, err error) {
	dbs.mut.Lock()
	defer dbs.mut.Unlock()

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

type DBPerShard struct {
	Mu sync.Mutex

	Dir string // holder dir

	dbh *DBHolder

	// just flat, not buried within the Node heirarchy.
	// Easily see how many we have.
	Flatmap map[*DBShard]struct{}

	types []txtype

	txf *TxFactory

	// which of our types is not-roaring, since
	// roaring doesn't keep a list of open Tx sn.
	// or default to the 2nd.
	useOpenList int
}

func (per *DBPerShard) ListOpenString() (r string) {
	for v := range per.Flatmap {
		r += v.Path + " -> " + v.W[per.useOpenList].OpenListString() + "\n"
	}
	return
}

func (txf *TxFactory) NewDBPerShard(types []txtype, holderDir string) (d *DBPerShard) {

	useOpenList := 0
	if len(types) == 2 {
		// blue-green, avoid the empty roaring Tx open list.
		// Prefer B's open list if neither is roaring.
		if types[0] == roaringTxn || types[1] != roaringTxn {
			useOpenList = 1
		}
	}

	d = &DBPerShard{
		types:       types,
		Dir:         holderDir,
		dbh:         NewDBHolder(),
		Flatmap:     make(map[*DBShard]struct{}),
		txf:         txf,
		useOpenList: useOpenList,
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
		err := dbs.Close()
		panicOn(err)
		panicOn(os.RemoveAll(dbs.Path))
	}
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

	dbs, err := per.GetDBShard(index, shard, nil)
	panicOn(err)
	return dbs.DeleteFragment(index, field, view, shard, frag)
}

func (dbs *DBShard) DumpAll() {

	for i, ty := range dbs.types {
		_ = i
		tx, err := dbs.W[i].NewTx(!writable, "", Txo{Index: dbs.idx})
		panicOn(err)
		defer tx.Rollback()
		tx.Dump()

		switch ty {
		case roaringTxn:
		case rbfTxn:
		case lmdbTxn:
		default:
			panic(fmt.Sprintf("unknown txtyp: '%v'", ty))
		}
	}
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
		AlwaysPrintf("DBPerShard.DumpAll() sees no databases. dir='%v'", per.Dir)
	}
}

func (per *DBPerShard) Path(index string, shard uint64) string {
	return per.Dir + sep + index + sep + fmt.Sprintf("%04v", shard)
}

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
	if !ok {

		dbs = &DBShard{
			types:         per.types,
			ParentDBIndex: dbi,
			Index:         index,
			Shard:         shard,
			Path:          per.Path(index, shard),
			idx:           idx,
			per:           per,
			useOpenList:   per.useOpenList,
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
			default:
				panic(fmt.Sprintf("unknown txtyp: '%v'", ty))
			}
			w, err := registry.OpenDBWrapper(dbs.Path, DetectMemAccessPastTx)
			panicOn(err)
			dbs.Open = true
			if w != nil && len(dbs.W) == 0 {
				per.Flatmap[dbs] = struct{}{}
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
	delete(per.Flatmap, dbs)

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

// requiredSuffix should be "-badgerdb" for badger, etc.
func DBPerShardGetShardsForIndex(idx *Index, roaringViewPath string) (sliceOfShards []uint64, err error) {

	// follow the blueGreen convention of returning the answer for 'B' or
	// the last wrapper type.
	types := idx.holder.txf.Types()
	ty := types[len(types)-1]

	if ty == roaringTxn {
		rx := &RoaringTx{
			Index: idx,
		}
		return rx.SliceOfShards("", "", "", roaringViewPath)
	}
	requiredSuffix := ty.FileSuffix()
	path := idx.Path()

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
		// Parse filename into integer.
		shard, err := strconv.ParseUint(prefix, 10, 64)
		if err != nil {
			continue
		}
		sliceOfShards = append(sliceOfShards, shard)
	}
	return
}

func listDirUnderDir(root string, includeRoot bool, requiredSuffix string, ignoreEmpty bool) (files []string, err error) {
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
