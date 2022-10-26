// Copyright 2021 Molecula Corp. All rights reserved.
package pilosa

import (
	"testing"
)

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

	qcx := h.Txf().NewWritableQcx()
	defer qcx.Abort()

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

	if err = qcx.Finish(); err != nil {
		t.Fatalf("failed to commit tx for index %v: %v", indexName, err)
	}

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
			qcx := h.Txf().NewQcx()
			defer qcx.Abort()
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
