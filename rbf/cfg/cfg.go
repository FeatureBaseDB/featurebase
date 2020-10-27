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
	"time"

	"github.com/spf13/pflag"
)

// Config defines externally configurable rbf options.
// The separate package avoids circular import.
type Config struct {

	// The maximum allowed database size. Required by mmap.
	MaxSize int64

	// Set before calling db.Open()
	FsyncEnabled bool

	// for mmap correctness testing.
	DoAllocZero bool

	// CheckpointEveryDur if zero means checkpoint after every write.
	// Otherwise, wait and checkpoint at the next write that happens
	// after CheckpointEveryDur since the previous.
	CheckpointEveryDur time.Duration

	// Maximum size of a single WAL segment.
	// May exceed by one page if last page is a bitmap header + bitmap.
	MaxWALSegmentFileSize int
}

func NewDefaultConfig() *Config {
	return &Config{
		MaxSize:               DefaultMaxSize,
		FsyncEnabled:          true,
		CheckpointEveryDur:    10 * time.Second,
		MaxWALSegmentFileSize: 1 << 16,
	}
}

func (cfg *Config) DefineFlags(flags *pflag.FlagSet) {
	default0 := NewDefaultConfig()
	flags.IntVar(&cfg.MaxWALSegmentFileSize, "rbf-max-wal", default0.MaxWALSegmentFileSize, "RBF write-Ahead-Log file size in bytes")
	flags.DurationVar(&cfg.CheckpointEveryDur, "rbf-checkpoint-dur", default0.CheckpointEveryDur, "RBF checkpoint on the next write that occurs this long or more after the previous write. 0 means checkpoint after every write.")
	flags.Int64Var(&cfg.MaxSize, "rbf-max-db-size", default0.MaxSize, "RBF maximum size in bytes of a database file (distinct from a WAL file)")
	flags.BoolVar(&cfg.FsyncEnabled, "rbf-fsync", default0.FsyncEnabled, "RBF: enable fsync fully safe flush-to-disk at each checkpoint")
}
