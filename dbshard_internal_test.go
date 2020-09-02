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
	tx = idx.Txf.NewTx(Txo{Write: writable, Index: idx, Fragment: f, Shard: f.shard})
	defer tx.Rollback()

	if n := f.mustRow(tx, 120).Count(); n != 2 {
		t.Fatalf("unexpected count (reopen): %d", n)
	} else if n := f.mustRow(tx, 121).Count(); n != 1 {
		t.Fatalf("unexpected count (reopen): %d", n)
	}
}

// test that we find all shards
func Test_DBPerShard_GetShardsForIndex(t *testing.T) {
	tmpdir, err := ioutil.TempDir("", "TestDBPerShardGetShardsForIndex")
	panicOn(err)

	orig := os.Getenv("PILOSA_TXSRC")
	defer os.Setenv("PILOSA_TXSRC", orig) // must restore or will mess up other tests!

	for _, src := range []string{"lmdb", "roaring", "rbf"} {
		makeSampleRoaringDir(tmpdir, src)
		os.Setenv("PILOSA_TXSRC", src)

		// must make Holder AFTER setting src.
		holder := NewHolder(tmpdir, nil)
		idx, err := NewIndex(holder, tmpdir, "rick")
		panicOn(err)
		estd := "rick/_exists/views/standard"
		std := "rick/f/views/standard"

		sos, err := DBPerShardGetShardsForIndex(idx, tmpdir+sep+std)
		panicOn(err)
		for _, shard := range []uint64{93, 223, 221, 215, 219, 217} {
			if !inSlice(sos, shard) {
				panic(fmt.Sprintf("missing shard=%v from sos='%#v'", shard, sos))
			}
		}
		if src == "roaring" {
			// check estd too
			sos, err = DBPerShardGetShardsForIndex(idx, tmpdir+sep+estd)
			panicOn(err)
			for _, shard := range []uint64{93, 223, 221, 215, 219, 217} {
				if !inSlice(sos, shard) {
					panic(fmt.Sprintf("missing shard=%v from sos='%#v'", shard, sos))
				}
			}
		}
	}
}

func inSlice(sos []uint64, shard uint64) bool {
	for i := range sos {
		if shard == sos[i] {
			return true
		}
	}
	return false
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
rick/0219-lmdb/data.mdb
rick/0219-lmdb/lock.mdb
rick/0093-lmdb/data.mdb
rick/0093-lmdb/lock.mdb
rick/0223-lmdb/data.mdb
rick/0223-lmdb/lock.mdb
rick/0215-lmdb/data.mdb
rick/0215-lmdb/lock.mdb
rick/0217-lmdb/data.mdb
rick/0217-lmdb/lock.mdb
rick/0221-lmdb/data.mdb
rick/0221-lmdb/lock.mdb
`,
	"rbf": `
rick/0223-rbfdb/wal/0000000000000001.wal
rick/0223-rbfdb/data
rick/0093-rbfdb/wal/0000000000000001.wal
rick/0093-rbfdb/data
rick/0217-rbfdb/wal/0000000000000001.wal
rick/0217-rbfdb/data
rick/0215-rbfdb/wal/0000000000000001.wal
rick/0215-rbfdb/data
rick/0221-rbfdb/wal/0000000000000001.wal
rick/0221-rbfdb/data
rick/0219-rbfdb/wal/0000000000000001.wal
rick/0219-rbfdb/data
`,
}

func makeSampleRoaringDir(root, txsrc string) {
	fns := strings.Split(sampleRoaringDirList[txsrc], "\n")
	for _, fn := range fns {
		if fn == "" {
			continue
		}
		path := root + sep + filepath.Dir(fn)
		panicOn(os.MkdirAll(path, 0755))
		fd, err := os.Create(root + sep + fn)
		panicOn(err)
		fd.Close()
	}
}
