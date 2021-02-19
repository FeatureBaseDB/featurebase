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
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pilosa/pilosa/v2/rbf"
	"github.com/pilosa/pilosa/v2/shardwidth"
	txkey "github.com/pilosa/pilosa/v2/short_txkey"
	//txkey "github.com/pilosa/pilosa/v2/txkey"
)

// Shard per db evaluation
func TestShardPerDB_SetBit(t *testing.T) {
	f, idx, tx := mustOpenFragment(t, "i", "f", viewStandard, 0, "")
	_ = idx
	defer f.Clean(t)

	// Set bits on the fragment.
	if _, err := f.setBit(tx, 120, 1); err != nil {
		t.Fatal(err)
	} else if _, err := f.setBit(tx, 120, 6); err != nil {
		t.Fatal(err)
	} else if _, err := f.setBit(tx, 121, 0); err != nil {
		t.Fatal(err)
	}
	// should have two containers set in the fragment.

	// Verify counts on rows.
	if n := f.mustRow(tx, 120).Count(); n != 2 {
		t.Fatalf("unexpected count: %d", n)
	} else if n := f.mustRow(tx, 121).Count(); n != 1 {
		t.Fatalf("unexpected count: %d", n)
	}

	// commit the change, and verify it is still there
	panicOn(tx.Commit())

	// Close and reopen the fragment & verify the data.
	err := f.Reopen() // roaring data not being flushed? red on roaring
	if err != nil {
		t.Fatal(err)
	}
	tx = idx.holder.txf.NewTx(Txo{Write: writable, Index: idx, Fragment: f, Shard: f.shard})
	defer tx.Rollback()

	if n := f.mustRow(tx, 120).Count(); n != 2 {
		t.Fatalf("unexpected count (reopen): %d", n)
	} else if n := f.mustRow(tx, 121).Count(); n != 1 {
		t.Fatalf("unexpected count (reopen): %d", n)
	}
}

// test that we find all *local* shards
func Test_DBPerShard_GetShardsForIndex_LocalOnly(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "Test_DBPerShard_GetShardsForIndex_LocalOnly")
	panicOn(err)
	defer os.RemoveAll(tmpdir)

	v2s := NewFieldView2Shards()
	stdShardSet := newShardSet()
	for _, shard := range []uint64{93, 223, 221, 215, 219, 217} {
		stdShardSet.add(shard)
	}
	for _, field := range []string{"f", "_exists"} {
		v2s.addViewShardSet(txkey.FieldView{Field: field, View: "standard"}, stdShardSet)
	}

	for _, src := range []string{"roaring", "bolt", "rbf"} {
		cfg := mustHolderConfig()
		cfg.StorageConfig.Backend = src
		holder := NewHolder(tmpdir, cfg)

		index := "rick"
		idx := makeSampleRoaringDir(tmpdir, index, src, 1, holder, v2s)
		if idx == nil {
			idx, err = NewIndex(holder, filepath.Join(tmpdir, index), index)
			panicOn(err)
		}
		estd := "rick/_exists/views/standard"
		std := "rick/f/views/standard"

		shards, err := holder.txf.GetShardsForIndex(idx, tmpdir+sep+std, false)
		panicOn(err)

		for _, shard := range []uint64{93, 223, 221, 215, 219, 217} {
			if !shards[shard] {
				panic(fmt.Sprintf("missing shard=%v from shards='%#v'", shard, shards))
			}
		}
		if src == "roaring" {
			// check estd too
			shards, err = holder.txf.GetShardsForIndex(idx, tmpdir+sep+estd, false)
			panicOn(err)
			for _, shard := range []uint64{93, 223, 221, 215, 219, 217} {
				if !shards[shard] {
					panic(fmt.Sprintf("missing shard=%v from shards='%#v'", shard, shards))
				}
			}

			// check GetSortedFieldViewList() and roaringGetFieldView2Shards()
			vs, err := roaringGetFieldView2Shards(idx)
			panicOn(err)

			for _, shard := range []uint64{93, 223, 221, 215, 219, 217} {
				tx := idx.holder.txf.NewTx(Txo{Write: !writable, Index: idx, Shard: shard})
				fvs, err := tx.GetSortedFieldViewList(idx, shard)
				panicOn(err)
				// expect these same two field/views for all 6 shards
				expect0 := txkey.FieldView{Field: "_exists", View: "standard"}
				expect1 := txkey.FieldView{Field: "f", View: "standard"}
				if len(fvs) != 2 {
					panic(fmt.Sprintf("fvs should be len 2, got '%#v'", fvs))
				}
				if fvs[0] != expect0 {
					panic(fmt.Sprintf("expected fvs[0]='%#v', but got '%#v'", expect0, fvs[0]))
				}
				if fvs[1] != expect1 {
					panic(fmt.Sprintf("expected fvs[1]='%#v', but got '%#v'", expect1, fvs[1]))
				}

				for _, fv := range fvs {
					if !vs.has(fv.Field, fv.View, shard) {
						panic(fmt.Sprintf("vs did not contain fv='%#v' for shard %v", fv, shard))
					}
				}
				tx.Rollback()
			}
		} else {
			// non-roaring: rbf, bolt

			for _, shard := range []uint64{93, 223, 221, 215, 219, 217} {
				tx := idx.holder.txf.NewTx(Txo{Write: !writable, Index: idx, Shard: shard})
				fvs, err := tx.GetSortedFieldViewList(idx, shard)
				panicOn(err)
				// expect these same two field/views for all 6 shards
				expect0 := txkey.FieldView{Field: "_exists", View: "standard"}
				expect1 := txkey.FieldView{Field: "f", View: "standard"}
				if len(fvs) != 2 {
					panic(fmt.Sprintf("fvs should be len 2, got '%#v'", fvs))
				}
				if fvs[0] != expect0 {
					panic(fmt.Sprintf("expected fvs[0]='%#v', but got '%#v'", expect0, fvs[0]))
				}
				if fvs[1] != expect1 {
					panic(fmt.Sprintf("expected fvs[1]='%#v', but got '%#v'", expect1, fvs[1]))
				}
				tx.Rollback()
			}
		}
		holder.Close()
	}
}

// data for Test_DBPerShard_GetShardsForIndex
//
var sampleRoaringDirList = map[string]string{"roaring": `
rick/f/views/standard/fragments/215.cache
rick/f/views/standard/fragments/221.cache
rick/f/views/standard/fragments/223.cache
rick/f/views/standard/fragments/93.cache
rick/f/views/standard/fragments/217.cache
rick/f/views/standard/fragments/219.cache
rick/f/views/standard/fragments/217
rick/f/views/standard/fragments/219
rick/f/views/standard/fragments/215
rick/f/views/standard/fragments/221
rick/f/views/standard/fragments/223
rick/f/views/standard/fragments/93
rick/_exists/views/standard/fragments/221
rick/_exists/views/standard/fragments/215
rick/_exists/views/standard/fragments/217
rick/_exists/views/standard/fragments/93
rick/_exists/views/standard/fragments/219
rick/_exists/views/standard/fragments/223
`,
	"bolt": `
rick.index.txstores@@@/store-boltdb@@/shard.0093-boltdb@/bolt.db
rick.index.txstores@@@/store-boltdb@@/shard.0215-boltdb@/bolt.db
rick.index.txstores@@@/store-boltdb@@/shard.0217-boltdb@/bolt.db
rick.index.txstores@@@/store-boltdb@@/shard.0219-boltdb@/bolt.db
rick.index.txstores@@@/store-boltdb@@/shard.0221-boltdb@/bolt.db
rick.index.txstores@@@/store-boltdb@@/shard.0223-boltdb@/bolt.db
`,
	"rbf": `
rick.index.txstores@@@/store-rbfdb@@/shard.0093-rbfdb@
rick.index.txstores@@@/store-rbfdb@@/shard.0215-rbfdb@
rick.index.txstores@@@/store-rbfdb@@/shard.0217-rbfdb@
rick.index.txstores@@@/store-rbfdb@@/shard.0219-rbfdb@
rick.index.txstores@@@/store-rbfdb@@/shard.0221-rbfdb@
rick.index.txstores@@@/store-rbfdb@@/shard.0223-rbfdb@
`,
}

func makeSampleRoaringDir(root, index, backend string, minBytes int, h *Holder, view2shards *FieldView2Shards) (idx *Index) {
	shards := []uint64{0, 93, 215, 217, 219, 221, 223}
	fns := strings.Split(sampleRoaringDirList[backend], "\n")
	firstDone := false

	for i, fn := range fns {
		if fn == "" {
			continue
		}
		var shard uint64
		if backend != "roaring" {
			// only have shards for the non-roaring
			shard = shards[i]
		}
		switch backend {
		case "bolt", "rbf":
			idx = helperCreateDBShard(h, index, shard)

			// first time only, we'll actually make all the shards at this point because
			// view2shards has them all anyway.
			if !firstDone {
				firstDone = true
				makeTxTestDBWithViewsShards(h, idx, view2shards)
			}
			continue
		}

		path := root + sep + filepath.Dir(fn)
		panicOn(os.MkdirAll(path, 0755))
		fd, err := os.Create(root + sep + fn)
		panicOn(err)
		if minBytes > 0 {
			_, err := fd.Write(make([]byte, minBytes))
			panicOn(err)
		}
		fd.Close()
	}
	return
}

func helperCreateDBShard(h *Holder, index string, shard uint64) *Index {
	idx, err := h.CreateIndexIfNotExists(index, IndexOptions{})
	panicOn(err)
	dbs, err := h.txf.dbPerShard.GetDBShard(index, shard, idx)
	panicOn(err)
	_ = dbs
	return idx
}

// keep the ocd linter happy
var _ = makeBolttestDB
var _ = makeRBFtestDB

func makeBolttestDB(path string, h *Holder, shard uint64) {
	i := uint64(1)
	w, _ := mustOpenEmptyBoltWrapper(path)
	BoltMustSetBitvalue(w, "index", "field", "view", shard, i)
	w.Close()
}

func makeRBFtestDB(path string, h *Holder, shard uint64) {
	i := uint64(1)

	db := rbf.NewDB(path, nil)
	err := db.Open()
	panicOn(err)
	defer db.Close()

	tx, err := db.Begin(true)
	panicOn(err)

	err = tx.CreateBitmap("x")
	panicOn(err)

	_, err = tx.Add("x", i)
	panicOn(err)

	err = tx.Commit()
	panicOn(err)
}

func makeTxTestDBWithViewsShards(holder *Holder, idx *Index, exp *FieldView2Shards) {

	// TODO(jea): need date time quantum views!!
	batched := false
	for field, viewmap := range exp.m {
		for view, shset := range viewmap {

			ss := shset.CloneMaybe()
			for shard := range ss {

				// simply write 1 bit to each shard to force its creation.
				bits := []uint64{(shard << shardwidth.Exponent) + 1}
				tx := idx.holder.txf.NewTx(Txo{Write: writable, Index: idx, Shard: shard})
				changeCount, err := tx.Add(idx.name, field, view, shard, batched, bits...)
				panicOn(err)
				if changeCount != len(bits) {
					panic(fmt.Sprintf("writing field '%v', view '%v' shard '%v', expected changeCount to equal len bits = %v but was %v", field, view, shard, len(bits), changeCount))
				}

				panicOn(tx.Commit())
			}
		}
	}

}

// test that rbf can give us a map[view]*shardSet
func Test_DBPerShard_GetFieldView2Shards_map_from_RBF(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "Test_DBPerShard_GetFieldView2Shards_map_from_RBF")
	panicOn(err)
	defer os.RemoveAll(tmpdir)

	cfg := mustHolderConfig()
	cfg.StorageConfig.Backend = "rbf"
	holder := NewHolder(tmpdir, cfg)
	defer holder.Close()

	index := "rick"
	field := "f"

	cim := &CreateIndexMessage{
		Index:     index,
		CreatedAt: 0,
		Meta:      IndexOptions{},
	}

	idx, err := holder.createIndex(cim, false)
	panicOn(err)

	exp := NewFieldView2Shards()

	stdShardSet := newShardSet()
	stdShardSet.add(12)
	stdShardSet.add(15)
	exp.addViewShardSet(txkey.FieldView{Field: field, View: "standard"}, stdShardSet)

	hrShardSet := newShardSet()
	hrShardSet.add(7)
	exp.addViewShardSet(txkey.FieldView{Field: field, View: "standard_2019092416"}, hrShardSet)

	makeTxTestDBWithViewsShards(holder, idx, exp)

	// setup is done
	view2shard, err := holder.txf.GetFieldView2ShardsMapForIndex(idx)
	panicOn(err)

	// compare against setup
	if !view2shard.equals(exp) {
		panic(fmt.Sprintf("expected '%v' but got view2shard '%v'", exp, view2shard))
	}
}
