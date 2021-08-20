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
	"fmt"
	"testing"
	"time"

	"github.com/molecula/featurebase/v2/shardwidth"
)

func unusableSampleTranslator(keys ...string) (map[string]uint64, error) {
	out := make(map[string]uint64, len(keys))
	for _, key := range keys {
		out[key] = uint64(len(out)) * 13
	}
	return out, nil
}

func TestSimpleCodec(t *testing.T) {
	c, _ := NewJSONCodec(nil)
	_ = c.AddSetField("set", nil)
	_ = c.AddSetField("setkeys", unusableSampleTranslator)
	_ = c.AddMutexField("mutex", nil)
	_ = c.AddMutexField("mutexkeys", unusableSampleTranslator)
	_ = c.AddTimeQuantumField("tq", nil)
	_ = c.AddIntField("int", nil)
	_ = c.AddIntField("intkeys", unusableSampleTranslator)
	epoch, err := time.Parse("2006-01-02", "2020-01-01")
	if err != nil {
		t.Fatalf("can't parse sample epoch time: %v", err)
	}
	_ = c.AddTimestampField("ts", time.Millisecond, epoch.Unix()*1000)
	_ = c.AddDecimalField("dec", 2)
	_ = c.AddBoolField("bool")
	var nextShard = uint64(1<<shardwidth.Exponent) + 5
	sampleJson := []byte(fmt.Sprintf(`
[
  {
    "action": "set",
    "records": {
      "2": {
        "set": [ 2 ],
        "tq": {
          "time": "2006-01-02T15:04:05.999999999Z",
          "values": [ 6 ]
        },
	"dec": 1.02,
        "int": 3,
	"mutex": 4,
	"mutexkeys": "key-a",
        "intkeys": "key-a"
      },
      "%d": {
        "set": [ 3 ],
	"bool": true
      },
      "1": {
        "set": [ 2 ],
	"bool": false,
        "setkeys": [
          "key-a",
          "key-b"
        ],
        "tq": { "values": [ 3, 4 ] }
      }
    }
  },
  {
    "action": "clear",
    "record_ids": [ 5, 6, 7 ],
    "fields": [ "tq" ]
  },
  {
    "action": "write",
    "records": {
      "3": {
        "set": 2,
	"mutex": 4,
	"mutexkeys": "key-a",
        "tq": {
          "time": "2006-01-02T15:04:05.999999999Z",
          "values": 6
        },
	"ts": "2020-01-01T00:01:00.000000000Z",
        "int": 3,
        "intkeys": "key-a"
      },
      "4": {
        "set": [ 3 ],
	"ts": 1577836860000
      },
      "5": {
        "set": [ 2 ],
        "setkeys": [ "key-a", "key-b" ],
        "tq": { "values": [ 3, 4 ] }
      }
    }
  },
  {
    "action": "delete",
    "record_ids": [ 9 ]
  },
  {
    "action": "set",
    "records": {
      "2": {
	 "mutex": 5,
	 "mutexkeys": "key-b"
      }
    }
  }
]
`, nextShard))
	var expected = map[uint64][]*Operation{
		0: {
			{
				OpType: OpSet,
				FieldOps: map[string]*FieldOperation{
					"mutex": {
						RecordIDs: []uint64{2},
						Values:    []uint64{4},
					},
					"set": {
						RecordIDs: []uint64{1, 2},
						Values:    []uint64{2, 2},
					},
					"tq": {
						RecordIDs: []uint64{1, 1, 2},
						Values:    []uint64{3, 4, 6},
						Signed:    []int64{0, 0, 1136214245999999999},
					},
					"mutexkeys": {
						RecordIDs: []uint64{2},
						Values:    []uint64{0},
					},
					"int": {
						RecordIDs: []uint64{2},
						Signed:    []int64{3},
					},
					"intkeys": {
						RecordIDs: []uint64{2},
						Signed:    []int64{0},
					},
					"setkeys": {
						RecordIDs: []uint64{1, 1},
						Values:    []uint64{0, 13},
					},
					"dec": {
						RecordIDs: []uint64{2},
						Signed:    []int64{102},
					},
					"bool": {
						RecordIDs: []uint64{1},
						Values:    []uint64{0},
					},
				},
			},
			{
				OpType:         OpClear,
				ClearRecordIDs: []uint64{5, 6, 7},
				ClearFields:    []string{"tq"},
			},
			{
				OpType:         OpWrite,
				ClearRecordIDs: []uint64{3, 4, 5},
				ClearFields:    []string{"int", "intkeys", "mutex", "mutexkeys", "set", "setkeys", "tq", "ts"},
				FieldOps: map[string]*FieldOperation{
					"mutex": {
						RecordIDs: []uint64{3},
						Values:    []uint64{4},
					},
					"set": {
						RecordIDs: []uint64{3, 5, 4},
						Values:    []uint64{2, 2, 3},
					},
					"tq": {
						RecordIDs: []uint64{5, 5, 3},
						Values:    []uint64{3, 4, 6},
						Signed:    []int64{0, 0, 1136214245999999999},
					},
					"mutexkeys": {
						RecordIDs: []uint64{3},
						Values:    []uint64{0},
					},
					"int": {
						RecordIDs: []uint64{3},
						Signed:    []int64{3},
					},
					"intkeys": {
						RecordIDs: []uint64{3},
						Signed:    []int64{0},
					},
					"setkeys": {
						RecordIDs: []uint64{5, 5},
						Values:    []uint64{0, 13},
					},
					"ts": {
						RecordIDs: []uint64{3, 4},
						Signed:    []int64{60000, 1577836860000},
					},
				},
			},
			{
				OpType:         OpDelete,
				ClearRecordIDs: []uint64{9},
			},
			{
				OpType: OpSet,
				FieldOps: map[string]*FieldOperation{
					"mutex": {
						RecordIDs: []uint64{2},
						Values:    []uint64{5},
					},
					"mutexkeys": {
						RecordIDs: []uint64{2},
						Values:    []uint64{13},
					},
				},
			},
		},
		1: {
			{
				OpType: OpSet,
				FieldOps: map[string]*FieldOperation{
					"set": {
						RecordIDs: []uint64{nextShard},
						Values:    []uint64{3},
					},
					"bool": {
						RecordIDs: []uint64{nextShard},
						Values:    []uint64{1},
					},
				},
			},
		},
	}
	req, err := c.ParseBytes(sampleJson)
	if err != nil {
		t.Fatalf("parsing sample buffer: %v", err)
	}
	// req.Dump(t.Logf)
	sharded, err := req.ByShard()
	if err != nil {
		t.Errorf("sharding err: %v", err)
	}
	for shard, ops := range sharded.Ops {
		for i, op := range ops {
			op.Sort()
			for field, fop := range op.FieldOps {
				sorter := fieldTypeSorts[req.FieldTypes[field]]
				if sorter == nil {
					sorter = (*FieldOperation).SortByRecords
				}
				sorter(fop)
			}
			var expectedOp *Operation
			if i < len(expected[shard]) {
				expectedOp = expected[shard][i]
			}
			if err = op.Compare(expectedOp); err != nil {
				t.Errorf("shard %d, op %d: %v", shard, i, err)
			}
		}
	}
}
