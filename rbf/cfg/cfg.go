// Copyright 2017 Pilosa Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cfg

import (
	"github.com/spf13/pflag"
)

const (
	DefaultMinWALCheckpointSize = 1 * (1 << 20) // 1MB
	DefaultMaxWALCheckpointSize = DefaultMaxWALSize / 2
)

// Config defines externally configurable rbf options.
// The separate package avoids circular import.
type Config struct {

	// The maximum allowed database size. Required by mmap.
	MaxSize int64

	// The maximum allowed WAL size. Required by mmap.
	MaxWALSize int64

	// The minimum WAL size before the WAL is copied to the DB.
	MinWALCheckpointSize int64

	// The maximum WAL size before transactions are halted to allow a checkpoint.
	MaxWALCheckpointSize int64

	// Set before calling db.Open()
	FsyncEnabled bool

	// for mmap correctness testing.
	DoAllocZero bool

	// CursorCacheSize is the number of copies of Cursor{} to keep in our
	// readyCursorCh arena to avoid GC pressure.
	CursorCacheSize int64
}

func NewDefaultConfig() *Config {
	return &Config{
		MaxSize:              DefaultMaxSize,
		MaxWALSize:           DefaultMaxWALSize,
		MinWALCheckpointSize: DefaultMinWALCheckpointSize,
		MaxWALCheckpointSize: DefaultMaxWALCheckpointSize,
		FsyncEnabled:         true,

		// CI passed with 20. 50 was too big for CI, even on X-large instances.
		// For now we default to 0, which means use sync.Pool.
		CursorCacheSize: 0,
	}
}

func (cfg *Config) DefineFlags(flags *pflag.FlagSet) {
	default0 := NewDefaultConfig()
	flags.Int64Var(&cfg.MaxSize, "rbf-max-db-size", default0.MaxSize, "RBF maximum size in bytes of a database file (distinct from a WAL file)")
	flags.Int64Var(&cfg.MaxWALSize, "rbf-max-wal-size", default0.MaxWALSize, "RBF maximum size in bytes of a WAL file (distinct from a DB file)")
	flags.Int64Var(&cfg.MinWALCheckpointSize, "rbf-min-wal-checkpoint-size", default0.MinWALCheckpointSize, "RBF minimum size in bytes of a WAL file before attempting checkpoint")
	flags.Int64Var(&cfg.MaxWALCheckpointSize, "rbf-max-wal-checkpoint-size", default0.MaxWALCheckpointSize, "RBF maximum size in bytes of a WAL file before forcing checkpoint")

	// renamed from --rbf-fsync to just --fsync because now it applies to all Tx backends.
	flags.BoolVar(&cfg.FsyncEnabled, "fsync", default0.FsyncEnabled, "enable fsync fully safe flush-to-disk")
	flags.Int64Var(&cfg.CursorCacheSize, "rbf-cursor-cache", default0.CursorCacheSize, "how big a Cursor arena to maintain. 0 means use sync.Pool with dynamic sizing. Note that <= 20 is needed to pass CI. Controls the memory footprint of rbf.")

}
