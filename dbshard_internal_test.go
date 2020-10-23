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

	orig := os.Getenv("PILOSA_TXSRC")
	defer os.Setenv("PILOSA_TXSRC", orig) // must restore or will mess up other tests!

	for _, src := range []string{"lmdb", "roaring", "bolt", "rbf"} {

		os.Setenv("PILOSA_TXSRC", src)

		// must make Holder AFTER setting src.
		holder := NewHolder(tmpdir, nil)

		makeSampleRoaringDir(tmpdir, src, 1, holder)

		idx, err := NewIndex(holder, tmpdir, "rick")
		panicOn(err)
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
	"lmdb": `
rick.index.txstores@@@/store-lmdb@@/shard.0093-lmdb@
rick.index.txstores@@@/store-lmdb@@/shard.0215-lmdb@
rick.index.txstores@@@/store-lmdb@@/shard.0217-lmdb@
rick.index.txstores@@@/store-lmdb@@/shard.0219-lmdb@
rick.index.txstores@@@/store-lmdb@@/shard.0221-lmdb@
rick.index.txstores@@@/store-lmdb@@/shard.0223-lmdb@
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

func makeSampleRoaringDir(root, txsrc string, minBytes int, h *Holder) {

	index := "rick"
	shards := []uint64{0, 93, 215, 217, 219, 221, 223}
	fns := strings.Split(sampleRoaringDirList[txsrc], "\n")
	for i, fn := range fns {
		if fn == "" {
			continue
		}
		var shard uint64
		if txsrc != "roaring" {
			// only have shards for the non-roaring
			shard = shards[i]
		}
		switch txsrc {
		case "lmdb":
			makeLMDBtestDB(root+sep+fn, h, shard)
			// also have to make the DBShard in our in-memory tree,
			// or else the search won't find it because
			// DBPerShard won't know anything about it.
			helperCreateDBShard(h, index, shard)
			continue
		case "bolt":
			makeBolttestDB(root+sep+fn, h, shard)
			helperCreateDBShard(h, index, shard)
			continue
		case "rbf":
			makeRBFtestDB(root+sep+fn, h, shard)
			helperCreateDBShard(h, index, shard)
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
}

func helperCreateDBShard(h *Holder, index string, shard uint64) {
	idx, err := h.CreateIndexIfNotExists(index, IndexOptions{})
	panicOn(err)
	dbs, err := h.txf.dbPerShard.GetDBShard(index, shard, idx)
	panicOn(err)
	_ = dbs
}

func makeLMDBtestDB(path string, h *Holder, shard uint64) {
	i := uint64(1)
	w, _ := mustOpenEmptyLMDBWrapper(path)
	LMDBMustSetBitvalue(w, "index", "field", "view", shard, i)
	w.Close()

}

func makeBolttestDB(path string, h *Holder, shard uint64) {
	i := uint64(1)
	w, _ := mustOpenEmptyBoltWrapper(path)
	BoltMustSetBitvalue(w, "index", "field", "view", shard, i)
	w.Close()
}

func makeRBFtestDB(path string, h *Holder, shard uint64) {
	i := uint64(1)

	db := rbf.NewDB(path)
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
