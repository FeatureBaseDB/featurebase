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
	"sync/atomic"
	"testing"
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
	var done int64
	f := mustOpenBSIFragment("i", "f", viewStandard, 0)
	defer f.Clean(t)

	ch := make(chan struct{})

	for i := 0; i < f.MaxOpN; i++ {
		_, _ = f.setBit(0, uint64(i*32))
	}
	// force snapshot so we get a mmapped row...
	_ = f.Snapshot()
	row := f.row(0)
	segment := row.Segments()[0]
	bitmap := segment.data

	// request information from the frozen bitmap we got back
	go func() {
		for atomic.LoadInt64(&done) == 0 {
			for i := 0; i < f.MaxOpN; i++ {
				_ = bitmap.Contains(uint64(i * 32))
			}
		}
		close(ch)
	}()

	values := make([]cv, 1024)
	for i := range values {
		cols := make([]uint64, 512)
		vals := make([]int64, 512)
		for j := range cols {
			cols[j] = uint64(rand.Int63n(ShardWidth))
			vals[j] = int64(rand.Int63n(1 << depth))
		}
		values[i] = cv{cols, vals}
	}

	// modify the original bitmap, until it causes a snapshot, which
	// then invalidates the other map...
	for j := 0; j < 5; j++ {
		for i := 0; i < f.MaxOpN/int(depth+1); i++ {
			cv := values[i%len(values)]
			err := f.importValue(cv.cols, cv.vals, depth, (i%3 == 1))
			if err != nil {
				t.Fatalf("importValue[%d][%d]: %v", j, i, err)
			}
		}
	}
	atomic.StoreInt64(&done, 1)
	<-ch
}
