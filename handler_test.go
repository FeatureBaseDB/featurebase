// Copyright 2022 Molecula Corp. All rights reserved.
package pilosa_test

import (
	"math/rand"
	"testing"

	pilosa "github.com/featurebasedb/featurebase/v3"
)

func TestSortToShards(t *testing.T) {
	var ir pilosa.ImportRequest
	expected := make(map[uint64][]uint64)
	const n = 50
	rng := rand.New(rand.NewSource(3))
	for i := 0; i < n; i++ {
		x := uint64(rng.Intn(8 * pilosa.ShardWidth))
		shard := x / pilosa.ShardWidth
		expected[shard] = append(expected[shard], x)
		ir.ColumnIDs = append(ir.ColumnIDs, x)
	}
	out := ir.SortToShards()
	for shard, values := range out {
		if len(values.ColumnIDs) != len(expected[shard]) {
			t.Fatalf("shard %d: expected values %d, got values %d", shard, expected[shard], values.ColumnIDs)
		}
	}
}
