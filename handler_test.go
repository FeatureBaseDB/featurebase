// Copyright 2022 Molecula Corp. All rights reserved.
package pilosa_test

import (
	"fmt"
	"math/rand"
	"strings"
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

func TestQueryResponseSameAs(t *testing.T) {
	var qr1, qr2 pilosa.QueryResponse
	qr1.Results = []any{"testing different results"}
	qr2.Results = []any{"with SCIENCE!"}
	qr1.Err = nil
	qr2.Err = nil
	if err := qr1.SameAs(&qr2); err.Error() != "responses contained different results" {
		t.Fatalf("expected responses to contain different results")
	}
	qr1.Results = []any{"this time, for sure!"}
	qr2.Results = []any{"this time, for sure!"}
	if err := qr1.SameAs(&qr2); err != nil {
		t.Fatalf("expected responses to be the same")
	}
	err1 := fmt.Errorf("out of cheese")
	err2 := fmt.Errorf("out of dryd frorg pills")
	qr1.Err = err1
	if err := qr1.SameAs(&qr2); !strings.Contains(err.Error(), "missing error") {
		t.Fatalf("should have had a missing error")
	}
	qr1.Err = nil
	qr2.Err = err2
	if err := qr1.SameAs(&qr2); !strings.Contains(err.Error(), "unexpected error") {
		t.Fatalf("should have had an unexpected error")
	}
	qr1.Err = err1
	if err := qr1.SameAs(&qr2); !strings.Contains(err.Error(), "wrong error") {
		t.Fatalf("should have had a wrong error")
	}
	qr2.Err = err1
	if err := qr1.SameAs(&qr2); err != nil {
		t.Fatalf("expected responses top be the same")
	}
}
