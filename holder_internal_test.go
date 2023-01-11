// Copyright 2021 Molecula Corp. All rights reserved.
package pilosa

import (
	"context"
	"testing"

	qc "github.com/molecula/featurebase/v3/querycontext"
	"github.com/stretchr/testify/require"
)

// MustIndexQueryContext gets a query context which can write to
// the specified index (and possibly shards), or fails the test.
// The qcx will be automatically cleaned up when the test completes.
func (h *Holder) MustIndexQueryContext(tb testing.TB, index string, shards ...uint64) qc.QueryContext {
	tb.Helper()
	// disregard a leading ^0, because that's idiomatic for "all shards"
	if len(shards) > 0 && shards[0] == ^uint64(0) {
		shards = shards[1:]
	}
	qcx, err := h.NewIndexQueryContext(context.Background(), index, shards...)
	if err != nil {
		tb.Fatalf("creating query context: %v", err)
	}
	tb.Cleanup(qcx.Release)
	return qcx
}

// MustQueryContext gets a read-only query context or fails the test.
func (h *Holder) MustQueryContext(tb testing.TB) qc.QueryContext {
	tb.Helper()
	qcx, err := h.NewQueryContext(context.Background())
	if err != nil {
		tb.Fatalf("creating query context: %v", err)
	}
	tb.Cleanup(qcx.Release)
	return qcx
}

func setupTest(t *testing.T, h *Holder, rowCol []rowCols, indexName string) (*Index, *Field) {
	idx, err := h.CreateIndexIfNotExists(indexName, "", IndexOptions{TrackExistence: true})
	if err != nil {
		t.Fatalf("failed to create index %v: %v", indexName, err)
	}
	f, err := idx.CreateFieldIfNotExists("f", "")
	if err != nil {
		t.Fatalf("failed to create field in index %v: %v", indexName, err)
	}
	existencefield := idx.existenceFld

	qcx := h.MustIndexQueryContext(t, indexName)

	for _, r := range rowCol {
		_, err = f.SetBit(qcx, r.row, r.col, nil)
		if err != nil {
			t.Fatalf("failed to set bit in index %v: %v", indexName, err)
		}

		_, err = existencefield.SetBit(qcx, r.row, r.col, nil)
		if err != nil {
			t.Fatalf("failed to set bit in index %v: %v", indexName, err)
		}
	}

	require.Nil(t, qcx.Commit())

	shardsFound := idx.AvailableShards(includeRemote).Slice()
	if len(shardsFound) != 3 {
		t.Fatalf("expected 3 shards for index %v, got %v", indexName, len(shardsFound))
	}
	return idx, f
}

type rowCols struct {
	row uint64
	col uint64
}

func TestHolder_ProcessDeleteInflight(t *testing.T) {
	h := newTestHolder(t)

	rowCol := []rowCols{
		{1, 1},
		{1, 2},
		{10, ShardWidth + 1},
		{1, ShardWidth * 2},
	}

	idx1, f1 := setupTest(t, h, rowCol, "idxdelete1")
	idx2, f2 := setupTest(t, h, rowCol, "idxdelete2")

	err := h.processDeleteInflight()
	if err != nil {
		t.Fatalf("failed to delete: %v", err)
	}

	tests := []struct {
		idx *Index
		f   *Field
	}{
		{idx1, f1},
		{idx2, f2},
	}

	for _, test := range tests {
		func() {
			idx, f := test.idx, test.f
			qcx := h.MustQueryContext(t)
			for _, r := range rowCol {
				row, err := f.Row(qcx, r.row)
				if err != nil {
					t.Fatalf("failed to get row: %v", err)
				}
				existenceRow, err := idx.existenceFld.Row(qcx, r.row)
				if err != nil {
					t.Fatalf("failed to get row: %v", err)
				}
				if len(row.Columns()) != 0 || len(existenceRow.Columns()) != 0 {
					t.Fatalf("expected columns for fields to be empty after delete")
				}
			}
		}()

	}
}
