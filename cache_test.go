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

package pilosa_test

import (
	"reflect"
	"testing"

	"github.com/pilosa/pilosa/v2"
)

// Ensure cache stays constrained to its configured size.
func TestCache_Rank_Size(t *testing.T) {
	cacheSize := uint32(3)
	cache := pilosa.NewRankCache(cacheSize)
	for i := 1; i < int(2*cacheSize); i++ {
		cache.Add(uint64(i), 3)
	}
	cache.Recalculate()
	if cache.Len() != int(cacheSize) {
		t.Fatalf("unexpected cache Size: %d!=%d expected\n", cache.Len(), cacheSize)
	}
}

// Ensure cache entries set below threshold are handled appropriately.
func TestCache_Rank_Threshold(t *testing.T) {
	cacheSize := uint32(5)
	cache := pilosa.NewRankCache(cacheSize)
	for i := 1; i < int(2*cacheSize); i++ {
		cache.Add(uint64(i), 3)
	}

	// Set the cache value for rows 4 and 5 to a number below the threshold
	// value (which is 3), and ensure that they gets zeroed out.
	cache.Add(4, 1)
	cache.BulkAdd(5, 1)
	cache.Recalculate()

	if cache.Get(4) != 0 {
		t.Fatalf("unexpected cache value after Add: %d!=%d expected\n", cache.Get(4), 0)
	}
	if cache.Get(5) != 0 {
		t.Fatalf("unexpected cache value after BulkAdd: %d!=%d expected\n", cache.Get(5), 0)
	}
}

// Test that consecutive writes show up in Top.
// On later writes, the cache skips recalculation to save CPU time.
// This used to mean that the later writes would not show up in Top.
// Now, the cache is flagged as dirty and recalculated during the call to Top.
func TestCache_Rank_Dirty(t *testing.T) {
	cacheSize := uint32(5)
	cache := pilosa.NewRankCache(cacheSize)

	type pair struct{ ID, Count uint64 }
	expect := []pair{
		{5, 2},
		{4, 1},
	}

	for _, v := range expect {
		cache.Add(v.ID, v.Count)
	}

	var got []pair
	for _, p := range cache.Top() {
		got = append(got, pair(p))
	}

	if !reflect.DeepEqual(expect, got) {
		t.Fatalf("wrote %v but got %v", expect, got)
	}
}
