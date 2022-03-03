// Copyright 2021 Molecula Corp. All rights reserved.
package pilosa

import (
	"testing"

	"github.com/molecula/featurebase/v3/disco"
	"github.com/molecula/featurebase/v3/testhook"
)

// mustHolderConfig sets up a default holder config for tests.
func mustHolderConfig() *HolderConfig {
	cfg := DefaultHolderConfig()
	cfg.StorageConfig.FsyncEnabled = false
	cfg.RBFConfig.FsyncEnabled = false
	cfg.Schemator = disco.NewInMemSchemator()
	cfg.Sharder = disco.InMemSharder
	return cfg
}

func TestHolder_ProcessDeleteInflight(t *testing.T) {
	path, _ := testhook.TempDir(t, "delete-inflight")
	h := NewHolder(path, mustHolderConfig())
	defer h.Close()

	err := h.Open()
	if err != nil {
		t.Fatalf("failed to open holder: %v", err)
	}

	idx, err := h.CreateIndexIfNotExists("i", IndexOptions{TrackExistence: true})
	if err != nil {
		t.Fatalf("failed to create index: %v", err)
	}
	f, err := idx.CreateFieldIfNotExists("f", OptFieldTypeDefault())
	if err != nil {
		t.Fatalf("failed to create field: %v", err)
	}

	existencefield := idx.existenceFld
	shard := uint64(0)
	tx := idx.Txf().NewTx(Txo{Write: true, Index: idx, Shard: shard})
	defer tx.Rollback()

	rowCol := []struct {
		row uint64
		col uint64
	}{
		{1, 1},
		{1, 2},
		{30, 33},
		{22, 2},
	}
	for _, r := range rowCol {
		_, err = f.SetBit(tx, r.row, r.col, nil)
		if err != nil {
			t.Fatalf("failed to set bit: %v", err)
		}

		_, err = existencefield.SetBit(tx, r.row, r.col, nil)
		if err != nil {
			t.Fatalf("failed to set bit: %v", err)
		}
	}

	if err = tx.Commit(); err != nil {
		t.Fatalf("failed to commit tx: %v", err)
	}

	err = h.processDeleteInflight()
	if err != nil {
		t.Fatalf("failed to delete: %v", err)
	}

	tx = idx.Txf().NewTx(Txo{Write: false, Index: idx, Shard: shard})
	defer tx.Rollback()
	for _, r := range rowCol {
		row, err := f.Row(tx, r.row)
		if err != nil {
			t.Fatalf("failed to get row: %v", err)
		}
		existenceRow, err := existencefield.Row(tx, r.row)
		if err != nil {
			t.Fatalf("failed to get row: %v", err)
		}
		if len(row.Columns()) != 0 || len(existenceRow.Columns()) != 0 {
			t.Fatalf("expected columns for fields to be empty after delete")
		}
	}
}
