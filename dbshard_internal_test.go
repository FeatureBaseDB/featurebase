// Copyright 2021 Molecula Corp. All rights reserved.
package pilosa

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/molecula/featurebase/v3/rbf"
	"github.com/molecula/featurebase/v3/shardwidth"
	txkey "github.com/molecula/featurebase/v3/short_txkey"
	"github.com/molecula/featurebase/v3/testhook"
	. "github.com/molecula/featurebase/v3/vprint" // nolint:staticcheck
)

// Shard per db evaluation
func TestShardPerDB_SetBit(t *testing.T) {
	f, idx, tx := mustOpenFragment(t)
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
	PanicOn(tx.Commit())

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
	tmpdir, err := testhook.TempDir(t, "Test_DBPerShard_GetShardsForIndex_LocalOnly")
	PanicOn(err)
	defer os.RemoveAll(tmpdir)

	v2s := NewFieldView2Shards()
	stdShardSet := newShardSet()
	for _, shard := range []uint64{93, 223, 221, 215, 219, 217} {
		stdShardSet.add(shard)
	}
	for _, field := range []string{"f", "_exists"} {
		v2s.addViewShardSet(txkey.FieldView{Field: field, View: "standard"}, stdShardSet)
	}

	for _, src := range []string{"rbf"} {
		holder := newTestHolder(t)

		index := "rick"
		idx := makeSampleRoaringDir(t, tmpdir, index, src, 1, holder, v2s)
		if idx == nil {
			idx, err = NewIndex(holder, filepath.Join(tmpdir, index), index)
			PanicOn(err)
		}
		std := "rick/fields/f/views/standard"

		shards, err := holder.txf.GetShardsForIndex(idx, tmpdir+sep+std, false)
		PanicOn(err)

		for _, shard := range []uint64{93, 223, 221, 215, 219, 217} {
			if _, ok := shards[shard]; !ok {
				t.Fatalf("missing shard=%v from shards='%#v'", shard, shards)
			}
		}
		for _, shard := range []uint64{93, 223, 221, 215, 219, 217} {
			tx := idx.holder.txf.NewTx(Txo{Write: !writable, Index: idx, Shard: shard})
			fvs, err := tx.GetSortedFieldViewList(idx, shard)
			PanicOn(err)
			// expect these same two field/views for all 6 shards
			expect0 := txkey.FieldView{Field: "_exists", View: "standard"}
			expect1 := txkey.FieldView{Field: "f", View: "standard"}
			if len(fvs) != 2 {
				t.Fatalf("fvs should be len 2, got '%#v' (%s)", fvs, src)
			}
			if fvs[0] != expect0 {
				t.Fatalf("expected fvs[0]='%#v', but got '%#v'", expect0, fvs[0])
			}
			if fvs[1] != expect1 {
				t.Fatalf("expected fvs[1]='%#v', but got '%#v'", expect1, fvs[1])
			}
			tx.Rollback()
		}
	}
}

// data for Test_DBPerShard_GetShardsForIndex
var sampleRoaringDirList = map[string]string{"roaring": `
rick/fields/f/views/standard/fragments/215.cache
rick/fields/f/views/standard/fragments/221.cache
rick/fields/f/views/standard/fragments/223.cache
rick/fields/f/views/standard/fragments/93.cache
rick/fields/f/views/standard/fragments/217.cache
rick/fields/f/views/standard/fragments/219.cache
rick/fields/f/views/standard/fragments/217
rick/fields/f/views/standard/fragments/219
rick/fields/f/views/standard/fragments/215
rick/fields/f/views/standard/fragments/221
rick/fields/f/views/standard/fragments/223
rick/fields/f/views/standard/fragments/93
rick/fields/_exists/views/standard/fragments/221
rick/fields/_exists/views/standard/fragments/215
rick/fields/_exists/views/standard/fragments/217
rick/fields/_exists/views/standard/fragments/93
rick/fields/_exists/views/standard/fragments/219
rick/fields/_exists/views/standard/fragments/223
`,
	"rbf": `
rick/backends/backend-rbf/shard.0093-rbf
rick/backends/backend-rbf/shard.0215-rbf
rick/backends/backend-rbf/shard.0217-rbf
rick/backends/backend-rbf/shard.0219-rbf
rick/backends/backend-rbf/shard.0221-rbf
rick/backends/backend-rbf/shard.0223-rbf
`,
}

func makeSampleRoaringDir(t *testing.T, root, index, backend string, minBytes int, h *Holder, view2shards *FieldView2Shards) (idx *Index) {
	shards := []uint64{0, 93, 215, 217, 219, 221, 223}
	fns := strings.Split(sampleRoaringDirList[backend], "\n")
	firstDone := false

	for i, fn := range fns {
		// This check is here because in sampleRoaringDirList, the first entry
		// of each map value is a line feed, so the strings.Split() above
		// results in a blank entry for the first item. This means that the
		// slice of shards above has an initial entry "0" which is not used.
		if fn == "" {
			continue
		}
		var shard uint64
		switch backend {
		case "rbf":
			shard = shards[i]

			idx = helperCreateDBShard(h, index, shard)

			// first time only, we'll actually make all the shards at this point because
			// view2shards has them all anyway.
			if !firstDone {
				firstDone = true
				makeTxTestDBWithViewsShards(t, h, idx, view2shards)
			}
			continue
		case "roaring":
		default:
			t.Fatalf("invalid backend: %s", backend)
		}

		path := root + sep + filepath.Dir(fn)
		PanicOn(os.MkdirAll(path, 0755))
		fd, err := os.Create(root + sep + fn)
		PanicOn(err)
		if minBytes > 0 {
			_, err := fd.Write(make([]byte, minBytes))
			PanicOn(err)
		}
		fd.Close()
	}
	return
}

func helperCreateDBShard(h *Holder, index string, shard uint64) *Index {
	idx, err := h.CreateIndexIfNotExists(index, "", IndexOptions{})
	PanicOn(err)
	// TODO: It's not clear that this is actually doing anything.
	dbs, err := h.txf.dbPerShard.GetDBShard(index, shard, idx)
	PanicOn(err)
	_ = dbs
	return idx
}

// keep the ocd linter happy
var _ = makeRBFtestDB

func makeRBFtestDB(path string, h *Holder, shard uint64) {
	i := uint64(1)

	db := rbf.NewDB(path, nil)
	err := db.Open()
	PanicOn(err)
	defer db.Close()

	tx, err := db.Begin(true)
	PanicOn(err)

	err = tx.CreateBitmap("x")
	PanicOn(err)

	_, err = tx.Add("x", i)
	PanicOn(err)

	err = tx.Commit()
	PanicOn(err)
}

func makeTxTestDBWithViewsShards(tb testing.TB, holder *Holder, idx *Index, exp *FieldView2Shards) {

	// TODO(jea): need date time quantum views!!
	for field, viewmap := range exp.m {
		for view, shset := range viewmap {

			ss := shset.CloneMaybe()
			for shard := range ss {

				// simply write 1 bit to each shard to force its creation.
				bits := []uint64{(shard << shardwidth.Exponent) + 1}
				tx := idx.holder.txf.NewTx(Txo{Write: writable, Index: idx, Shard: shard})
				changeCount, err := tx.Add(idx.name, field, view, shard, bits...)
				PanicOn(err)
				if changeCount != len(bits) {
					tb.Fatalf("writing field '%v', view '%v' shard '%v', expected changeCount to equal len bits = %v but was %v", field, view, shard, len(bits), changeCount)
				}

				PanicOn(tx.Commit())
			}
		}
	}

}

// test that rbf can give us a map[view]*shardSet
func Test_DBPerShard_GetFieldView2Shards_map_from_RBF(t *testing.T) {
	holder := newTestHolder(t)

	index := "rick"
	field := "f"

	idx, err := holder.CreateIndex(index, "", IndexOptions{})
	if err != nil {
		t.Fatalf("creating index: %v", err)
	}

	exp := NewFieldView2Shards()

	stdShardSet := newShardSet()
	stdShardSet.add(12)
	stdShardSet.add(15)
	exp.addViewShardSet(txkey.FieldView{Field: field, View: "standard"}, stdShardSet)

	hrShardSet := newShardSet()
	hrShardSet.add(7)
	exp.addViewShardSet(txkey.FieldView{Field: field, View: "standard_2019092416"}, hrShardSet)

	makeTxTestDBWithViewsShards(t, holder, idx, exp)

	// setup is done
	view2shard, err := holder.txf.GetFieldView2ShardsMapForIndex(idx)
	PanicOn(err)

	// compare against setup
	if !view2shard.equals(exp) {
		t.Fatalf("expected '%v' but got view2shard '%v'", exp, view2shard)
	}
}
