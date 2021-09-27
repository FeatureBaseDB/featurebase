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

package ingest

import (
	"math/rand"
	"testing"

	"github.com/molecula/featurebase/v2/shardwidth"
)

type opShardingTestCase struct {
	name   string
	input  *Request
	output *ShardedRequest
}

var opShardingTestCases = []opShardingTestCase{
	{
		name: "sample",
		input: &Request{
			Ops: []*Operation{
				{
					OpType: OpSet,
					FieldOps: map[string]*FieldOperation{
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
					OpType: OpRemove,
					Seq:    1,
					FieldOps: map[string]*FieldOperation{
						"shard0-2": {
							RecordIDs: []uint64{1, 2<<shardwidth.Exponent + 1},
						},
					},
				},
			},
		},
		output: &ShardedRequest{
			Ops: map[uint64][]*Operation{
				0: {
					{
						OpType: OpSet,
						FieldOps: map[string]*FieldOperation{
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
						OpType: OpRemove,
						Seq:    1,
						FieldOps: map[string]*FieldOperation{
							"shard0-2": {
								RecordIDs: []uint64{1},
							},
						},
					},
				},
				1: {
					{
						OpType: OpSet,
						FieldOps: map[string]*FieldOperation{
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
						OpType: OpRemove,
						Seq:    1,
						FieldOps: map[string]*FieldOperation{
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

func TestOpShardingSmall(t *testing.T) {
	codec, _ := NewJSONCodec(nil)
	_ = codec.AddIntField("shard0", nil)
	_ = codec.AddIntField("shard1", nil)
	_ = codec.AddIntField("shard0-1", nil)
	_ = codec.AddIntField("shard0-2", nil)

	fieldTypes := codec.FieldTypes()
	for _, c := range opShardingTestCases {
		sharded, err := c.input.ByShard(fieldTypes)
		if err != nil {
			t.Errorf("sharding: unexpected error %v", err)
		}
		if err := sharded.Compare(c.output); err != nil {
			t.Fatalf("%s: shard: %v", c.name, err)
		}
		merged := sharded.merge()
		if err := c.input.Compare(merged); err != nil {
			t.Fatalf("%s: merge: %v", c.name, err)
		}
	}
}

func TestOpShardingLarge(t *testing.T) {
	codec, err := NewJSONCodec(nil)
	if err != nil {
		t.Fatalf("creating codec: %v", err)
	}
	_ = codec.AddSetField("set", nil)
	_ = codec.AddTimeQuantumField("tq", nil)
	_ = codec.AddIntField("int", nil)

	// and now we populate encodeTests[1] with a larger pool of data
	// if this isn't large enough, the scattering across shards means
	// we coincidentally end up with the time quantum field not getting
	// tested on simpleSort.
	const dataSize = 120000
	shardCount := uint64(300) // shards we want to target
	passes := uint64(0)
	recordIDs := make([]uint64, dataSize)
	values := make([]uint64, dataSize)
	timeStamps := make([]int64, dataSize)
	signedValues := make([]int64, dataSize)
	for i := uint64(0); i < dataSize; i++ {
		if (i % shardCount) == 0 {
			passes++
		}
		recordIDs[i] = ((i % shardCount) << shardwidth.Exponent) + passes
		values[i] = (i % 4)
		timeStamps[i] = int64(1234567890e9 + (i * 100e9))
		signedValues[i] = (int64(i) % 16) // no negative values because they won't work with keys
	}
	op := &Operation{
		OpType:   OpSet,
		FieldOps: map[string]*FieldOperation{},
	}
	req := &Request{
		Ops: []*Operation{op},
	}
	op.FieldOps["tq"] = &FieldOperation{
		RecordIDs: append([]uint64{}, recordIDs...),
		Values:    append([]uint64{}, values...),
		Signed:    timeStamps,
	}
	op.FieldOps["int"] = &FieldOperation{
		RecordIDs: recordIDs,
		Signed:    signedValues,
	}
	// for sets, we want to shuffle things into fewer shards, and ensure
	// non-duplication of values within each record, but also have lots
	// of duplication of record IDs in the low shards
	recordIDs = make([]uint64, dataSize)
	values = make([]uint64, dataSize)
	valuesPerRecord := uint64(5)
	recordsPerShard := dataSize / valuesPerRecord / 30
	if recordsPerShard < 1 {
		recordsPerShard = 1
	}
	shard := uint64(0)
	nextID := uint64(0)
	nextValue := uint64(0)
	for i := uint64(0); i < dataSize; i++ {
		recordIDs[i] = nextID
		values[i] = nextValue + (i % valuesPerRecord)
		nextValue++
		if nextValue == valuesPerRecord {
			nextValue = 0
			nextID++
			if nextID%(1<<shardwidth.Exponent) == recordsPerShard {
				shard++
				nextID = (shard << shardwidth.Exponent)
				if valuesPerRecord > 1 {
					valuesPerRecord--
				}
			}
		}
	}
	op.FieldOps["set"] = &FieldOperation{
		RecordIDs: recordIDs,
		Values:    values,
	}
	fieldTypes := codec.FieldTypes()
	sharded, err := req.ByShard(fieldTypes)
	if err != nil {
		t.Errorf("sharding: unexpected error %v", err)
	}
	merged := sharded.merge()
	if err := req.Compare(merged); err != nil {
		t.Fatalf("merge comparison: %v", err)
	}
}

func TestFancySharding(t *testing.T) {
	const shardLimit = 700
	const recordCount = 5000
	grr := rand.New(rand.NewSource(0))
	for i := 0; i < 100; i++ {
		f := &FieldOperation{RecordIDs: make([]uint64, recordCount), Values: make([]uint64, recordCount)}
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
