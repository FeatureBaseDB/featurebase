// Copyright 2021 Molecula Corp. All rights reserved.
package pilosa

import (
	"github.com/molecula/featurebase/v3/disco"
)

// mustHolderConfig sets up a default holder config for tests.
func mustHolderConfig() *HolderConfig {
	cfg := DefaultHolderConfig()
	cfg.StorageConfig.FsyncEnabled = false
	cfg.RBFConfig.FsyncEnabled = false
	cfg.Schemator = disco.InMemSchemator
	cfg.Sharder = disco.InMemSharder
	return cfg
}
