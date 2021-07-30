// Copyright 2021 Molecula Corp.
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

package ingest_test

import (
	"math/rand"
	"reflect"
	"testing"

	"github.com/molecula/featurebase/v2/ingest"
	"github.com/molecula/featurebase/v2/shardwidth"
)

type opShardingTestCase struct {
	name   string
	input  ingest.Request
	output *ingest.ShardedRequest
}

var opShardingTestCases = []opShardingTestCase{
	{
		name: "sample",
		input: ingest.Request{
			Ops: []*ingest.Operation{
				{
					OpType: ingest.OpSet,
					FieldOps: map[string]*ingest.FieldOperation{
						"shard0": {
							RecordIDs: []uint64{0, 1},
						},
						"shard0-1": {
							RecordIDs: []uint64{
								0,
								1 << shardwidth.Exponent,
							},
						},
						"shard1": {
							RecordIDs: []uint64{
								1 << shardwidth.Exponent,
								1<<shardwidth.Exponent + 1,
							},
						},
					},
				},
				{
					OpType: ingest.OpRemove,
					FieldOps: map[string]*ingest.FieldOperation{
						"shard0-2": {
							RecordIDs: []uint64{1, 2<<shardwidth.Exponent + 1},
						},
					},
				},
			},
		},
		output: &ingest.ShardedRequest{
			Ops: map[uint64][]*ingest.Operation{
				0: {
					{
						OpType: ingest.OpSet,
						FieldOps: map[string]*ingest.FieldOperation{
							"shard0": {
								RecordIDs: []uint64{0, 1},
							},
							"shard0-1": {
								RecordIDs: []uint64{
									0,
								},
							},
						},
					},
					{
						OpType: ingest.OpRemove,
						FieldOps: map[string]*ingest.FieldOperation{
							"shard0-2": {
								RecordIDs: []uint64{1},
							},
						},
					},
				},
				1: {
					{
						OpType: ingest.OpSet,
						FieldOps: map[string]*ingest.FieldOperation{
							"shard0-1": {
								RecordIDs: []uint64{
									1 << shardwidth.Exponent,
								},
							},
							"shard1": {
								RecordIDs: []uint64{
									1 << shardwidth.Exponent,
									1<<shardwidth.Exponent + 1,
								},
							},
						},
					},
				},
				2: {
					{
						OpType: ingest.OpRemove,
						FieldOps: map[string]*ingest.FieldOperation{
							"shard0-2": {
								RecordIDs: []uint64{2<<shardwidth.Exponent + 1},
							},
						},
					},
				},
			},
		},
	},
}

func TestOpSharding(t *testing.T) {
	for _, c := range opShardingTestCases {
		sharded, err := c.input.Shard()
		if err != nil {
			t.Errorf("sharding: unexpected error %v", err)
		}
		if !reflect.DeepEqual(sharded, c.output) {
			t.Fatalf("%s: expected %#v, got %#v", c.name, c.output, sharded)
		}
	}
}

func TestFancySharding(t *testing.T) {
	const shardLimit = 700
	const recordCount = 5000
	grr := rand.New(rand.NewSource(0))
	for i := 0; i < 100; i++ {
		f := &ingest.FieldOperation{RecordIDs: make([]uint64, recordCount), Values: make([]uint64, recordCount)}
		shards := make([]int, shardLimit)
		for j := range f.RecordIDs {
			v := uint64(grr.Int63n(shardLimit << shardwidth.Exponent))
			f.RecordIDs[j] = v
			f.Values[j] = uint64(grr.Int63n(8))
			shards[v>>shardwidth.Exponent]++
		}

		sharded := f.SortToShards()
		for shard, data := range sharded {
			if len(data.RecordIDs) != shards[shard] {
				t.Errorf("shard %d: expected %d items, got %d", shard, shards[shard], len(data.RecordIDs))
			}
			for _, v := range data.RecordIDs {
				if (v >> shardwidth.Exponent) != shard {
					t.Errorf("shard %d: got %x, which should be in %d", shard, v, v>>shardwidth.Exponent)
				}
			}
			// expect sorted-ness
			data.SortByRecords()
			prev := data.RecordIDs[0]
			for i, next := range data.RecordIDs[1:] {
				if next < prev {
					t.Errorf("index %d: prev %d, next %d", i+1, prev, next)
				}
				prev = next
			}
			data.SortByValues()
			prevV, prevRec := data.Values[0], data.RecordIDs[0]
			for i, nextRec := range data.RecordIDs[1:] {
				nextV := data.Values[i+1]
				if nextV < prevV {
					t.Errorf("index %d: prev value %d, next value %d", i+1, prevV, nextV)
				}
				if nextV == prevV {
					if nextRec < prevRec {
						t.Errorf("index %d, value %d: prev rec %d, next rec %d", i+1, nextV, prevRec, nextRec)
					}
				}
				prevV = nextV
				prevRec = nextRec
			}
		}
	}
}
