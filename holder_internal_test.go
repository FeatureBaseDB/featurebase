// Copyright 2021 Molecula Corp. All rights reserved.
package pilosa

import (
	"testing"

	"github.com/molecula/featurebase/v3/disco"
)

func testSetBit(t *testing.T, h *Holder, index, field string, rowID, columnID uint64) {

	idx, err := h.CreateIndexIfNotExists(index, IndexOptions{})
	if err != nil {
		t.Fatalf("creating index: %v", err)
	}

	f, err := idx.CreateFieldIfNotExists(field, OptFieldTypeDefault())
	if err != nil {
		t.Fatalf("setting bit: %v", err)
	}
	_, err = f.SetBit(nil, rowID, columnID, nil)
	if err != nil {
		t.Fatalf("setting bit: %v", err)
	}
}

// mustHolderConfig sets up a default holder config for tests.
func mustHolderConfig() *HolderConfig {
	cfg := DefaultHolderConfig()
	cfg.StorageConfig.FsyncEnabled = false
	cfg.RBFConfig.FsyncEnabled = false
	cfg.Schemator = disco.InMemSchemator
	cfg.Sharder = disco.InMemSharder
	return cfg
}
