// Copyright 2021 Molecula Corp. All rights reserved.
package pilosa

import (
	"fmt"
	"math/rand"
	"runtime"
	"testing"

	"github.com/molecula/featurebase/v2/logger"
	"github.com/molecula/featurebase/v2/syswrap"
)

type cv struct {
	cols []uint64
	vals []int64
}

func forceSnapshotsCheckMapping(t *testing.T) {
	depth := uint64(6)
	f, idx, tx := mustOpenBSIFragment(t, "i", "f", viewStandard, 0)
	tx.Rollback()
	f.Logger = logger.NewLogfLogger(t)
	defer f.Clean(t)

	tx = idx.holder.txf.NewTx(Txo{Write: writable, Index: idx, Fragment: f, Shard: f.shard})
	defer tx.Rollback()

	for i := 0; i < f.MaxOpN; i++ {
		_, _ = f.setBit(tx, 0, uint64(32*i))
	}
	// force snapshot so we get a mmapped row...
	err := f.Snapshot()
	if err != nil {
		t.Fatalf("initial snapshot error: %v", err)
	}

	values := make([]cv, 1024)
	for i := range values {
		cols := make([]uint64, 128)
		vals := make([]int64, 128)
		for j := range cols {
			// pick values in the first 16 cols of each of the 16
			// shards in a default shardwidth, so each set will
			// probably change some values from the previous one.
			cols[j] = uint64(((rand.Int63n(16) & int64(i>>2)) << 16) + rand.Int63n(16))
			vals[j] = int64(rand.Int63n(1 << depth))
		}
		values[i] = cv{cols, vals}
	}

	// modify the original bitmap, until it causes a snapshot, which
	// then invalidates the other map...
	for i := 0; i < 32; i++ {
		cv := values[i%len(values)]
		// periodically force gc, so if we have a small pool of maps
		// we'll go in and out of mapping mode
		if i%5 == 0 {
			runtime.GC()
		}
		err := f.importValue(tx, cv.cols, cv.vals, depth, (i%3 == 1))
		if err != nil {
			t.Fatalf("importValue[%d]: %v", i, err)
		}
		err = f.Snapshot()
		if err != nil {
			t.Fatalf("snapshot[%d]: %v", i, err)
		}
	}
}

// This test should basically never fail, but it might if you were running
// out of available mmaps. Which you can fake up by adding '&& false' to the test
// in newGeneration in generation.go. So this is probably useless but it's
// a failure mode we've been bitten by once...
func TestMmapBehavior(t *testing.T) {
	// rbf and lmdb not happy with this test.
	roaringOnlyTest(t)

	var changed bool
	var original uint64
	defer func() {
		syswrap.SetMaxMapCount(original)
	}()

	for _, mmapMaxVal := range []uint64{0, 3} {
		prev := syswrap.SetMaxMapCount(mmapMaxVal)
		if !changed {
			original = prev
			changed = true
		}
		t.Run(fmt.Sprintf("maps%d", mmapMaxVal), func(t *testing.T) {
			forceSnapshotsCheckMapping(t)
		})
	}
}
