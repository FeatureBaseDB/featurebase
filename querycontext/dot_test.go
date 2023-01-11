// Copyright 2022 Molecula Corp (DBA FeatureBase). All rights reserved.
package querycontext

import (
	"context"
	"os"
	"testing"

	"github.com/molecula/featurebase/v3/keys"
)

func TestDot(t *testing.T) {
	// test this with two splitters, one of which is the default indexShardKeySplitter. the second one will overwrite
	// the output from the first one.
	for _, splitter := range []KeySplitter{&flexibleKeySplitter{splitIndexes: map[keys.Index]struct{}{"i": {}}}, nil} {
		txs := testTxStore(t, "foo", splitter)
		file, err := os.Create("test.dot")
		if err != nil {
			t.Fatalf("creating file: %v", err)
		}
		defer file.Close()
		q, _ := txs.NewWriteQueryContext(context.Background(), txs.Scope().AddIndex("i").AddIndexShards("k", 0))
		defer q.Release()
		q.Write("i", "f", "v", 0)
		q.Write("i", "g", "v", 0)
		q.Write("i", "f", "v", 1)
		// read, but it's in a writable thing, so still creates rbfQueryWrite
		q.Read("i", "f", "v", 2)
		q.Read("j", "f", "v", 0)
		q2, _ := txs.NewQueryContext(context.Background())
		defer q2.Release()
		q2.Read("i", "f", "v", 0)
		q2.Read("i", "g", "v", 0)
		var dg dotGraph
		// we know what we actually have here...
		inner := txs.inner
		dg.enqueue(inner.(*rbfTxStore))
		dg.build(5)
		err = dg.Write(file)
		if err != nil {
			t.Fatalf("writing dot: %v", err)
		}
	}
}
