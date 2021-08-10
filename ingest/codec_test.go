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
	"testing"
	"time"
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
	_ = c.AddSetField("setKeys", unusableSampleTranslator)
	_ = c.AddMutexField("mutex", nil)
	_ = c.AddMutexField("mutexKeys", unusableSampleTranslator)
	_ = c.AddTimeQuantumField("tq", nil)
	_ = c.AddIntField("int", nil)
	_ = c.AddIntField("intKeys", unusableSampleTranslator)
	epoch, err := time.Parse("2006-01-02", "2020-01-01")
	if err != nil {
		t.Fatalf("can't parse sample epoch time: %v", err)
	}
	_ = c.AddTimestampField("ts", time.Millisecond, epoch.Unix()*1000)
	_ = c.AddDecimalField("dec", 2)
	_ = c.AddBoolField("bool")
	sampleJson := []byte(`
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
	"mutexKeys": "key-a",
        "intKeys": "key-a"
      },
      "1234567": {
        "set": [ 3 ],
	"bool": true
      },
      "1": {
        "set": [ 2 ],
	"bool": false,
        "setKeys": [
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
        "set": [ 2 ],
	"mutex": 4,
	"mutexKeys": "key-a",
        "tq": {
          "time": "2006-01-02T15:04:05.999999999Z",
          "values": [ 6 ]
        },
	"ts": "2020-01-01T00:01:00.000000000Z",
        "int": 3,
        "intKeys": "key-a"
      },
      "4": {
        "set": [ 3 ],
	"ts": 1577836860000
      },
      "5": {
        "set": [ 2 ],
        "setKeys": [ "key-a", "key-b" ],
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
	 "mutexKeys": "key-b"
      }
    }
  }
]
`)
	req, err := c.ParseBytes(sampleJson)
	if err != nil {
		t.Fatalf("parsing sample buffer: %v", err)
	}
	t.Logf("req: %#v", req)
	for _, op := range req.Ops {
		t.Logf("op: %#v", op)
		if len(op.ClearRecordIDs) > 0 {
			t.Logf("  clearRecordIDs: %d", op.ClearRecordIDs)
		}
		if len(op.ClearFields) > 0 {
			t.Logf("  clearFields: %s", op.ClearFields)
		}
		for field, fieldOp := range op.FieldOps {
			t.Logf("  field %q: %#v", field, fieldOp)
		}
	}
	sharded, err := req.Shard()
	if err != nil {
		t.Errorf("sharding err: %v", err)
	}
	for shard, ops := range sharded.Ops {
		t.Logf("shard %d:", shard)
		for _, op := range ops {
			t.Logf("  op: %q", op.OpType.String())
			if len(op.ClearRecordIDs) > 0 {
				t.Logf("    clearRecordIDs: %d", op.ClearRecordIDs)
			}
			if len(op.ClearFields) > 0 {
				t.Logf("    clearFields: %s", op.ClearFields)
			}
			for field, fieldOp := range op.FieldOps {
				t.Logf("    field %q: %#v", field, fieldOp)
			}
		}
	}
}
