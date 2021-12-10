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
	MaxSize int64 `toml:"max-db-size"`

	// The maximum allowed WAL size. Required by mmap.
	MaxWALSize int64 `toml:"max-wal-size"`

	// The minimum WAL size before the WAL is copied to the DB.
	MinWALCheckpointSize int64 `toml:"min-wal-checkpoint-size"`

	// The maximum WAL size before transactions are halted to allow a checkpoint.
	MaxWALCheckpointSize int64 `toml:"max-wal-checkpoint-size"`

	// Set before calling db.Open()
	FsyncEnabled bool `toml:"fsync"`

	// for mmap correctness testing.
	DoAllocZero bool `toml:"do-alloc-zero"`

	// CursorCacheSize is the number of copies of Cursor{} to keep in our
	// readyCursorCh arena to avoid GC pressure.
	CursorCacheSize int64 `toml:"cursor-cache-size"`
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
	flags.Int64Var(&cfg.MaxSize, "rbf.max-db-size", default0.MaxSize, "RBF maximum size in bytes of a database file (distinct from a WAL file)")
	flags.Int64Var(&cfg.MaxWALSize, "rbf.max-wal-size", default0.MaxWALSize, "RBF maximum size in bytes of a WAL file (distinct from a DB file)")
	flags.Int64Var(&cfg.MinWALCheckpointSize, "rbf.min-wal-checkpoint-size", default0.MinWALCheckpointSize, "RBF minimum size in bytes of a WAL file before attempting checkpoint")
	flags.Int64Var(&cfg.MaxWALCheckpointSize, "rbf.max-wal-checkpoint-size", default0.MaxWALCheckpointSize, "RBF maximum size in bytes of a WAL file before forcing checkpoint")

	// renamed from --rbf-fsync to just --fsync because now it applies to all Tx backends.
	flags.BoolVar(&cfg.FsyncEnabled, "fsync", default0.FsyncEnabled, "enable fsync fully safe flush-to-disk")
	flags.Int64Var(&cfg.CursorCacheSize, "rbf.cursor-cache-size", default0.CursorCacheSize, "how big a Cursor arena to maintain. 0 means use sync.Pool with dynamic sizing. Note that <= 20 is needed to pass CI. Controls the memory footprint of rbf.")
}
