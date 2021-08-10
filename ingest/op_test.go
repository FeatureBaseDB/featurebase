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
