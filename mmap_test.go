// Copyright 2020 Pilosa Corp.
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

package pilosa

import (
	"math/rand"
	"runtime"
	"testing"

	"github.com/pilosa/pilosa/v2/logger"
)

type cv struct {
	cols []uint64
	vals []int64
}

// This test should basically never fail, but it might if you were running
// out of available mmaps. Which you can fake up by adding '&& false' to the test
// in newGeneration in generation.go. So this is probably useless but it's
// a failure mode we've been bitten by once...
func TestMmapBehavior(t *testing.T) {
	depth := uint(6)
	f := mustOpenBSIFragment("i", "f", viewStandard, 0)
	f.Logger = logger.NewLogfLogger(t)
	defer f.Clean(t)

	for i := 0; i < f.MaxOpN; i++ {
		_, _ = f.setBit(0, uint64(i*32))
	}
	// force snapshot so we get a mmapped row...
	err := f.Snapshot()
	if err != nil {
		t.Fatalf("initial snapshot error: %v", err)
	}

	values := make([]cv, 1024)
	for i := range values {
		cols := make([]uint64, 128)
		vals := make([]int64, 128)
		for j := range cols {
			// pick values in the first 16 cols of each of the 16
			// shards in a default shardwidth, so each set will
			// probably change some values from the previous one.
			cols[j] = uint64(((rand.Int63n(16) & int64(i>>2)) << 16) + rand.Int63n(16))
			vals[j] = int64(rand.Int63n(1 << depth))
		}
		values[i] = cv{cols, vals}
	}

	// modify the original bitmap, until it causes a snapshot, which
	// then invalidates the other map...
	for i := 0; i < 32; i++ {
		cv := values[i%len(values)]
		runtime.GC()
		err := f.importValue(cv.cols, cv.vals, depth, (i%3 == 1))
		if err != nil {
			t.Fatalf("importValue[%d]: %v", i, err)
		}
		err = f.Snapshot()
		if err != nil {
			t.Fatalf("snapshot[%d]: %v", i, err)
		}
	}
}
