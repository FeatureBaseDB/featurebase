// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package ingest

import (
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/molecula/featurebase/v3/shardwidth"
)

func TestStableTranslator(t *testing.T) {
	tr := newStableTranslator()
	m1, err := tr.TranslateKeys("a", "b")
	if err != nil {
		t.Fatalf("translation error on initial keys: %v", err)
	}
	m2, err := tr.TranslateIDs(m1["a"], m1["b"], 6)
	if err != nil {
		t.Fatalf("translation error on reverse lookup: %v", err)
	}
	m3, err := tr.TranslateKeys("a", "k-6")
	if err != nil {
		t.Fatalf("translation error on new keys: %v", err)
	}
	for k, v := range m3 {
		if m2[v] != k {
			t.Fatalf("expected round trip to equate %q and %d", k, v)
		}
	}
}

func TestMakeCodec(t *testing.T) {
	codec, _ := NewJSONCodec(nil)
	err := codec.AddSetField("set", nil)
	if err != nil {
		t.Fatalf("unexpected error creating field: %v", err)
	}
	err = codec.AddSetField("set", nil)
	if err == nil {
		t.Fatalf("expected error creating duplicate field, didn't get it")
	}
}

func TestEncode(t *testing.T) {
	codec, _ := NewJSONCodec(nil)
	_ = codec.AddSetField("set", nil)
	_ = codec.AddSetField("setkeys", newStableTranslator())
	_ = codec.AddMutexField("mutex", nil)
	_ = codec.AddMutexField("mutexkeys", newStableTranslator())
	_ = codec.AddTimeQuantumField("tq", nil)
	_ = codec.AddTimeQuantumField("tqkeys", newStableTranslator())
	_ = codec.AddIntField("int", nil)
	_ = codec.AddIntField("intkeys", newStableTranslator())
	epoch, err := time.Parse("2006-01-02", "2020-01-01")
	if err != nil {
		t.Fatalf("can't parse sample epoch time: %v", err)
	}
	_ = codec.AddTimestampField("ts", "ms", epoch.Unix()*1000)
	_ = codec.AddDecimalField("dec", 2)
	_ = codec.AddBoolField("bool")

	codecs := []*JSONCodec{codec}

	// redo all of that, only on a keyed translator
	codec, _ = NewJSONCodec(newStableTranslator())
	_ = codec.AddSetField("set", nil)
	_ = codec.AddSetField("setkeys", newStableTranslator())
	_ = codec.AddMutexField("mutex", nil)
	_ = codec.AddMutexField("mutexkeys", newStableTranslator())
	_ = codec.AddTimeQuantumField("tq", nil)
	_ = codec.AddTimeQuantumField("tqkeys", newStableTranslator())
	_ = codec.AddIntField("int", nil)
	_ = codec.AddIntField("intkeys", newStableTranslator())
	_ = codec.AddTimestampField("ts", "ms", epoch.Unix()*1000)
	_ = codec.AddDecimalField("dec", 2)
	_ = codec.AddBoolField("bool")

	codecs = append(codecs, codec)

	encodeTests := []*Request{
		{
			Ops: []*Operation{
				{
					OpType:         OpWrite,
					ClearRecordIDs: []uint64{0, 1, 2, 3, 5},
					ClearFields:    []string{"bool", "dec", "int", "intkeys", "mutex", "mutexkeys", "set", "setkeys", "tq", "tqkeys", "ts"},
					FieldOps: map[string]*FieldOperation{
						"int": {
							RecordIDs: []uint64{0, 1},
							Signed:    []int64{1, -3},
						},
						"intkeys": {
							RecordIDs: []uint64{0, 1},
							Signed:    []int64{1, 1},
						},
						"set": {
							RecordIDs: []uint64{0, 1, 2, 2},
							Values:    []uint64{1, 1, 0, 1},
						},
						"setkeys": {
							RecordIDs: []uint64{0, 1, 2, 2},
							Values:    []uint64{1, 1, 0, 1},
						},
						"mutex": {
							RecordIDs: []uint64{0, 1},
							Values:    []uint64{1, 2},
						},
						"mutexkeys": {
							RecordIDs: []uint64{0, 1},
							Values:    []uint64{1, 2},
						},
						"tq": {
							RecordIDs: []uint64{5, 5},
							Values:    []uint64{8, 9},
							Signed:    []int64{1234567890e9, 1234567890e9},
						},
						"tqkeys": {
							RecordIDs: []uint64{3, 3},
							Values:    []uint64{2, 4},
							Signed:    []int64{1234567890e9, 1234567890e9},
						},
						"ts": {
							RecordIDs: []uint64{0},
							Signed:    []int64{1},
						},
						"bool": {
							RecordIDs: []uint64{0, 1},
							Values:    []uint64{0, 1},
						},
						"dec": {
							RecordIDs: []uint64{0, 1, 2},
							Signed:    []int64{123, -123, 0},
						},
					},
				},
				{
					OpType:         OpClear,
					Seq:            1,
					ClearRecordIDs: []uint64{6},
					ClearFields:    []string{"tq"},
				},
			},
		},
		{
			// this one needs to get filled in programmatically; see below
			Ops: []*Operation{
				{
					OpType:   OpSet,
					FieldOps: map[string]*FieldOperation{},
				},
			},
		},
		{
			Ops: []*Operation{
				{
					OpType: OpRemove,
					FieldOps: map[string]*FieldOperation{
						"set": {},
					},
				},
			},
		},
	}
	// and now we populate encodeTests[1] with a larger pool of data
	const dataSize = 5000
	shardCount := uint64(600) // shards we want to target
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
	// ensure record IDs are sorted, because other stuff might rely on this
	sort.Slice(recordIDs, func(i, j int) bool { return recordIDs[i] < recordIDs[j] })
	op := encodeTests[1].Ops[0]
	op.FieldOps["tq"] = &FieldOperation{
		RecordIDs: append([]uint64{}, recordIDs...),
		Values:    append([]uint64{}, values...),
		Signed:    append([]int64{}, timeStamps...),
	}
	op.FieldOps["tqkeys"] = &FieldOperation{
		RecordIDs: append([]uint64{}, recordIDs...),
		Values:    values,
		Signed:    timeStamps,
	}
	op.FieldOps["int"] = &FieldOperation{
		RecordIDs: append([]uint64{}, recordIDs...),
		Signed:    append([]int64{}, signedValues...),
	}
	op.FieldOps["intkeys"] = &FieldOperation{
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
	sort.Slice(recordIDs, func(i, j int) bool { return recordIDs[i] < recordIDs[j] })
	op.FieldOps["set"] = &FieldOperation{
		RecordIDs: append([]uint64{}, recordIDs...),
		Values:    append([]uint64{}, values...),
	}
	op.FieldOps["setkeys"] = &FieldOperation{
		RecordIDs: recordIDs,
		Values:    values,
	}
	var buf []byte
	for i, tc := range encodeTests {
		for _, c := range codecs {
			data, err := c.AppendBytes(tc, buf[:0])
			if err != nil {
				t.Fatalf("encode test %d: error encoding: %v", i, err)
			}
			// t.Logf("data:\n%s", data)
			req, err := c.ParseBytes(data)
			if err != nil {
				t.Logf("encode test %d: data:\n%s", i, data)
				t.Fatalf("encode test %d: error parsing: %v", i, err)
			}
			err = req.Compare(tc)
			if err != nil {
				t.Logf("encode test %d: data:\n%s", i, data)
				t.Fatalf("encode test %d: round-trip mismatch: %v", i, err)
			}
			data, err = c.AppendBytes(req, buf[:0])
			if err != nil {
				t.Fatalf("encode test %d: error encoding: %v", i, err)
			}
			// t.Logf("data:\n%s", data)
			req2, err := c.ParseBytes(data)
			if err != nil {
				t.Logf("encode test %d: data:\n%s", i, data)
				t.Fatalf("encode test %d: error parsing: %v", i, err)
			}
			err = req2.Compare(tc)
			if err != nil {
				t.Logf("encode test %d: data:\n%s", i, data)
				t.Fatalf("encode test %d: round-trip mismatch: %v", i, err)
			}
		}
	}
}

func TestCodecErrors(t *testing.T) {
	codec, _ := NewJSONCodec(nil)
	_ = codec.AddSetField("set", nil)
	_ = codec.AddSetField("setkeys", newStableTranslator())
	_ = codec.AddMutexField("mutex", nil)
	_ = codec.AddMutexField("mutexkeys", newStableTranslator())
	_ = codec.AddTimeQuantumField("tq", nil)
	_ = codec.AddTimeQuantumField("tqkeys", newStableTranslator())
	_ = codec.AddIntField("int", nil)
	_ = codec.AddIntField("intkeys", newStableTranslator())
	epoch, err := time.Parse("2006-01-02", "2020-01-01")
	if err != nil {
		t.Fatalf("can't parse sample epoch time: %v", err)
	}
	_ = codec.AddTimestampField("ts", "ms", epoch.Unix()*1000)
	_ = codec.AddDecimalField("dec", 2)
	_ = codec.AddBoolField("bool")

	testCases := []struct {
		name  string
		json  []byte
		error string
	}{
		{
			name:  "no action",
			json:  []byte(`[{"records":{"0":{"set":[0]}}}]`),
			error: "action not specified",
		},
		{
			name:  "unknown action",
			json:  []byte(`[{"action":"yeet","records":{"0":{"set":[0]}}}]`),
			error: "unknown action",
		},
		{
			name:  "unknown field",
			json:  []byte(`[{"action":"set","records":{"0":{"settee":[0]}}}]`),
			error: "field not found",
		},
		{
			name:  "unknown operation field",
			json:  []byte(`[{"action":"set","yeet":false,"records":{"0":{"set":[0]}}}]`),
			error: "unknown operation field",
		},
		{
			name:  "expected operation",
			json:  []byte(`[true]`),
			error: "expected operation",
		},
		{
			name:  "expecting key",
			json:  []byte(`[{"action":"set","records":{"0":{"setkeys":0}}}]`),
			error: "expecting key",
		},
		{
			name:  "invalid int for bool",
			json:  []byte(`[{"action":"set","records":{"0":{"bool":2}}}]`),
			error: "boolean should be",
		},
		{
			name:  "invalid number for bool",
			json:  []byte(`[{"action":"set","records":{"0":{"bool":1.3}}}]`),
			error: "looks like Number",
		},
		{
			name:  "invalid string for bool",
			json:  []byte(`[{"action":"set","records":{"0":{"bool":"truly"}}}]`),
			error: "expecting boolean",
		},
		{
			name:  "nonsense bool",
			json:  []byte(`[{"action":"set","records":{"0":{"bool":[]}}}]`),
			error: "boolean should be",
		},
		{
			name:  "expecting numeric value",
			json:  []byte(`[{"action":"set","records":{"0":{"set":0.1}}}]`),
			error: "invalid syntax",
		},
		{
			name:  "expecting value",
			json:  []byte(`[{"action":"set","records":{"0":{"setkeys":true}}}]`),
			error: "expecting value",
		},
		{
			name:  "expecting array-key",
			json:  []byte(`[{"action":"set","records":{"0":{"setkeys":[0]}}}]`),
			error: "expecting key",
		},
		{
			name:  "expecting array-value",
			json:  []byte(`[{"action":"set","records":{"0":{"setkeys":[true]}}}]`),
			error: "expecting value",
		},
		{
			name:  "expecting numeric array-value",
			json:  []byte(`[{"action":"set","records":{"0":{"set":[0.1]}}}]`),
			error: "invalid syntax",
		},
		{
			name:  "expecting numeric value",
			json:  []byte(`[{"action":"set","records":{"0":{"int":0.1}}}]`),
			error: "invalid syntax",
		},
		{
			name:  "expecting int key",
			json:  []byte(`[{"action":"set","records":{"0":{"intkeys":0}}}]`),
			error: "expecting string key",
		},
		{
			name:  "expecting int value",
			json:  []byte(`[{"action":"set","records":{"0":{"int":[0]}}}]`),
			error: "expecting integer value",
		},
		{
			name:  "expecting string array-value",
			json:  []byte(`[{"action":"set","records":{"0":{"intkeys":["a"]}}}]`),
			error: "expecting string key",
		},
		{
			name:  "expecting numeric mutex value",
			json:  []byte(`[{"action":"set","records":{"0":{"mutex":0.1}}}]`),
			error: "invalid syntax",
		},
		{
			name:  "expecting mutex key",
			json:  []byte(`[{"action":"set","records":{"0":{"mutexkeys":0}}}]`),
			error: "expecting string key",
		},
		{
			name:  "expecting mutex value",
			json:  []byte(`[{"action":"set","records":{"0":{"mutex":[0]}}}]`),
			error: "expecting integer value",
		},
		{
			name:  "expecting mutex string value",
			json:  []byte(`[{"action":"set","records":{"0":{"mutexkeys":["a"]}}}]`),
			error: "expecting string key",
		},
		{
			name:  "time quantum invalid time",
			json:  []byte(`[{"action":"set","records":{"0":{"tq":{"time":[],"values":[3]}}}}]`),
			error: "expecting time",
		},
		{
			name:  "time stamp invalid integer",
			json:  []byte(`[{"action":"set","records":{"0":{"ts":1.3}}}]`),
			error: "parsing numeric time",
		},
		{
			name:  "time stamp invalid string",
			json:  []byte(`[{"action":"set","records":{"0":{"ts":"RFC3339"}}}]`),
			error: "parsing time",
		},
		{
			name:  "time stamp invalid type",
			json:  []byte(`[{"action":"set","records":{"0":{"ts":[]}}}]`),
			error: "expecting time",
		},
		{
			name:  "invalid decimal",
			json:  []byte(`[{"action":"set","records":{"0":{"dec":[]}}}]`),
			error: "expecting floating",
		},
		{
			name:  "duplicate record",
			json:  []byte(`[{"action":"set","records":{"0":{"int":1},"0":{"set":0}}}]`),
			error: "duplicated in input",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req, err := codec.ParseBytes(tc.json)
			if err == nil {
				req.Dump(t.Logf)
				t.Fatalf("expected error like %q, got request instead", tc.error)
			} else {
				msg := err.Error()
				if !strings.Contains(msg, tc.error) {
					t.Fatalf("expected error like %q, got %q", tc.error, msg)
				}
			}
		})
	}
}

func TestSimpleCodec(t *testing.T) {
	codec, _ := NewJSONCodec(nil)
	_ = codec.AddSetField("set", nil)
	_ = codec.AddSetField("setkeys", newStableTranslator())
	_ = codec.AddMutexField("mutex", nil)
	_ = codec.AddMutexField("mutexkeys", newStableTranslator())
	_ = codec.AddTimeQuantumField("tq", nil)
	_ = codec.AddIntField("int", nil)
	_ = codec.AddIntField("intkeys", newStableTranslator())
	epoch, err := time.Parse("2006-01-02", "2020-01-01")
	if err != nil {
		t.Fatalf("can't parse sample epoch time: %v", err)
	}
	_ = codec.AddTimestampField("ts", "ms", epoch.Unix()*1000)
	_ = codec.AddDecimalField("dec", 2)
	_ = codec.AddBoolField("bool")
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
        "int": "3",
	"mutex": 4,
	"bool": 1,
	"mutexkeys": "key-a",
        "intkeys": "key-a"
      },
      "%d": {
        "set": [ 3 ],
	"bool": true
      },
      "1": {
        "set": [ 2 ],
	"bool": 0,
        "setkeys": [
          "key-a",
          "key-b"
        ],
        "tq": { "values": [ 3, 4 ] }
	},
	"3":{
		"bool": "true",
		"ts": 27
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
          "time": 1234567890,
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
	var expected = &ShardedRequest{
		Ops: map[uint64][]*Operation{
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
							Values:    []uint64{0, 1},
						},
						"dec": {
							RecordIDs: []uint64{2},
							Signed:    []int64{102},
						},
						"bool": {
							RecordIDs: []uint64{1, 2, 3},
							Values:    []uint64{0, 1, 1},
						},
						"ts": {
							RecordIDs: []uint64{3},
							Signed:    []int64{27},
						},
					},
				},
				{
					OpType:         OpClear,
					Seq:            1,
					ClearRecordIDs: []uint64{5, 6, 7},
					ClearFields:    []string{"tq"},
				},
				{
					OpType:         OpWrite,
					Seq:            2,
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
							Signed:    []int64{0, 0, 1234567890e9},
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
							Values:    []uint64{0, 1},
						},
						"ts": {
							RecordIDs: []uint64{3, 4},
							Signed:    []int64{60000, 1577836860000},
						},
					},
				},
				{
					OpType:         OpDelete,
					Seq:            3,
					ClearRecordIDs: []uint64{9},
				},
				{
					OpType: OpSet,
					Seq:    4,
					FieldOps: map[string]*FieldOperation{
						"mutex": {
							RecordIDs: []uint64{2},
							Values:    []uint64{5},
						},
						"mutexkeys": {
							RecordIDs: []uint64{2},
							Values:    []uint64{1},
						},
					},
				},
			},
			1: {
				{
					OpType: OpSet,
					Seq:    0,
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
		},
	}
	req, err := codec.ParseBytes(sampleJson)
	if err != nil {
		t.Fatalf("parsing sample buffer: %v", err)
	}
	// req.Dump(t.Logf)
	fieldTypes := codec.FieldTypes()
	sharded, err := req.ByShard(fieldTypes)
	if err != nil {
		t.Errorf("sharding err: %v", err)
	}
	for shard, ops := range sharded.Ops {
		for i, op := range ops {
			op.Sort()
			for field, fop := range op.FieldOps {
				sorter := fieldTypeSorts[fieldTypes[field]]
				if sorter == nil {
					sorter = (*FieldOperation).SortByRecords
				}
				sorter(fop)
			}
			var expectedOp *Operation
			if i < len(expected.Ops[shard]) {
				expectedOp = expected.Ops[shard][i]
			}
			if err = op.Compare(expectedOp); err != nil {
				t.Errorf("shard %d, op %d: %v", shard, i, err)
			}
		}
	}
}

func expectEqualFieldOp(t *testing.T, fo1, fo2 *FieldOperation) {
	if err := fo1.Compare(fo2); err != nil {
		t.Fatalf("unexpected fieldOp mismatch: %v", err)
	}
	if err := fo2.Compare(fo1); err != nil {
		t.Fatalf("unexpected fieldOp mismatch (inverted): %v", err)
	}
}

func expectUnequalFieldOp(t *testing.T, msg string, fo1, fo2 *FieldOperation) {
	if err := fo1.Compare(fo2); err == nil {
		t.Fatalf("unexpected fieldOp equality %s", msg)
	}
	if err := fo2.Compare(fo1); err == nil {
		t.Fatalf("unexpected fieldOp equality %s", msg)
	}
}

func expectEqualOp(t *testing.T, fo1, fo2 *Operation) {
	if err := fo1.Compare(fo2); err != nil {
		t.Fatalf("unexpected Op mismatch: %v", err)
	}
	if err := fo2.Compare(fo1); err != nil {
		t.Fatalf("unexpected Op mismatch (inverted): %v", err)
	}
}

func expectUnequalOp(t *testing.T, msg string, fo1, fo2 *Operation) {
	if err := fo1.Compare(fo2); err == nil {
		t.Fatalf("unexpected Op equality %s", msg)
	}
	if err := fo2.Compare(fo1); err == nil {
		t.Fatalf("unexpected Op equality %s", msg)
	}
}

func expectEqualReq(t *testing.T, fo1, fo2 *Request) {
	if err := fo1.Compare(fo2); err != nil {
		t.Fatalf("unexpected Request mismatch: %v", err)
	}
	if err := fo2.Compare(fo1); err != nil {
		t.Fatalf("unexpected Request mismatch (inverted): %v", err)
	}
}

func expectUnequalReq(t *testing.T, msg string, fo1, fo2 *Request) {
	if err := fo1.Compare(fo2); err == nil {
		t.Fatalf("unexpected Request equality %s", msg)
	}
	if err := fo2.Compare(fo1); err == nil {
		t.Fatalf("unexpected Request equality %s", msg)
	}
}

func expectEqualShardedReq(t *testing.T, fo1, fo2 *ShardedRequest) {
	if err := fo1.Compare(fo2); err != nil {
		t.Fatalf("unexpected Request mismatch: %v", err)
	}
	if err := fo2.Compare(fo1); err != nil {
		t.Fatalf("unexpected Request mismatch (inverted): %v", err)
	}
}

func expectUnequalShardedReq(t *testing.T, msg string, fo1, fo2 *ShardedRequest) {
	if err := fo1.Compare(fo2); err == nil {
		t.Fatalf("unexpected Request equality %s", msg)
	}
	if err := fo2.Compare(fo1); err == nil {
		t.Fatalf("unexpected Request equality %s", msg)
	}
}

func testCompareFieldOpMutate(t *testing.T, fo1 *FieldOperation) {
	fo2 := fo1.clone()
	expectEqualFieldOp(t, fo1, fo2)

	fo1.RecordIDs = fo1.RecordIDs[:1]
	expectUnequalFieldOp(t, "short record IDs", fo1, fo2)
	fo1.RecordIDs = fo1.RecordIDs[:2]

	fo1.RecordIDs[1] = 2
	expectUnequalFieldOp(t, "mismatched record IDs", fo1, fo2)
	fo1.RecordIDs[1] = fo2.RecordIDs[1]

	if len(fo1.Values) != 0 {
		fo1.Values = fo1.Values[:1]
		expectUnequalFieldOp(t, "short values", fo1, fo2)
		fo1.Values = fo1.Values[:2]

		fo1.Values[1]++
		expectUnequalFieldOp(t, "mismatched values", fo1, fo2)
		fo1.Values[1] = fo2.Values[1]
	}

	if len(fo1.Signed) != 0 {
		fo1.Signed = fo1.Signed[:1]
		expectUnequalFieldOp(t, "short signed values", fo1, fo2)
		fo1.Signed = fo1.Signed[:2]

		fo1.Signed[1]++
		expectUnequalFieldOp(t, "mismatched signed values", fo1, fo2)
		fo1.Signed[1] = fo2.Signed[1]
	}
}

func TestCompareFieldOperation(t *testing.T) {
	fo := &FieldOperation{
		RecordIDs: []uint64{0, 1},
		Values:    []uint64{0, 1},
	}
	testCompareFieldOpMutate(t, fo)
	fo.Signed = []int64{-3, 5}
	testCompareFieldOpMutate(t, fo)
	fo.Values = nil
	testCompareFieldOpMutate(t, fo)

	// after this, fo has only RecordIDs
	fo.Signed = nil

	var fNil *FieldOperation
	expectUnequalFieldOp(t, "nil and non-empty", fo, fNil)
	fo.RecordIDs = fo.RecordIDs[:0]

	expectEqualFieldOp(t, fo, fNil)

	if err := fNil.Compare(fNil); err != nil {
		t.Fatalf("expected nil and nil to be equal: %v", err)
	}
}

func TestCompareOperation(t *testing.T) {
	op1 := &Operation{
		OpType:         OpWrite,
		ClearRecordIDs: []uint64{0, 1},
		ClearFields:    []string{"a", "b"},
		Seq:            1,
		FieldOps: map[string]*FieldOperation{
			"c": {
				RecordIDs: []uint64{0},
				Values:    []uint64{0},
			},
		},
	}
	op2 := op1.clone()

	expectEqualOp(t, op1, op2)

	op1.Seq++
	expectUnequalOp(t, "seq mismatch", op1, op2)
	op1.Seq = op2.Seq

	op1.OpType++
	expectUnequalOp(t, "opType mismatch", op1, op2)
	op1.OpType = op2.OpType

	op1.ClearRecordIDs = op1.ClearRecordIDs[:1]
	expectUnequalOp(t, "short clearRecordIDs", op1, op2)
	op1.ClearRecordIDs = op1.ClearRecordIDs[:2]

	op1.ClearRecordIDs[1]++
	expectUnequalOp(t, "mismatched clearRecordIDs", op1, op2)
	op1.ClearRecordIDs[1] = op2.ClearRecordIDs[1]

	op1.ClearFields = op1.ClearFields[:1]
	expectUnequalOp(t, "short clearFields", op1, op2)
	op1.ClearFields = op1.ClearFields[:2]

	op1.ClearFields[1] = "z"
	expectUnequalOp(t, "mismatched clearFields", op1, op2)
	op1.ClearFields[1] = op2.ClearFields[1]

	op1.FieldOps["d"] = op1.FieldOps["c"]
	expectUnequalOp(t, "extra fieldOp", op1, op2)
	op2.FieldOps["d"] = op2.FieldOps["c"]
	expectEqualOp(t, op1, op2)
	op1.FieldOps["c"].RecordIDs[0]++
	expectUnequalOp(t, "mismatched fieldOp", op1, op2)

	expectUnequalOp(t, "nil Op", nil, op1)
	expectEqualOp(t, nil, nil)
}

func TestCompareRequest(t *testing.T) {
	r1 := &Request{
		Ops: []*Operation{
			{OpType: OpSet},
			{OpType: OpSet, Seq: 1},
		},
	}
	r2 := &Request{
		Ops: []*Operation{
			{OpType: OpSet},
			{OpType: OpSet, Seq: 1},
		},
	}
	expectEqualReq(t, r1, r2)
	r1.Ops = r1.Ops[:1]
	expectUnequalReq(t, "short ops", r1, r2)
	r1.Ops = r1.Ops[:2]

	r1.Ops[1].Seq = 2
	expectUnequalReq(t, "ops mismatch", r1, r2)
	r1.Ops = r1.Ops[:2]

	expectUnequalReq(t, "non-empty and nil", r1, nil)

	r1.Ops = r1.Ops[:0]
	expectEqualReq(t, r1, nil)
}

func TestCompareShardedRequest(t *testing.T) {
	r1 := &ShardedRequest{
		Ops: map[uint64][]*Operation{
			1: {
				{OpType: OpSet},
				{OpType: OpSet, Seq: 1},
			},
			2: {
				{OpType: OpSet},
				{OpType: OpSet, Seq: 1},
			},
		},
	}
	r2 := &ShardedRequest{
		Ops: map[uint64][]*Operation{
			1: {
				{OpType: OpSet},
				{OpType: OpSet, Seq: 1},
			},
			2: {
				{OpType: OpSet},
				{OpType: OpSet, Seq: 1},
			},
		},
	}
	expectEqualShardedReq(t, r1, r2)
	stash := r1.Ops[2]
	delete(r1.Ops, 2)
	expectUnequalShardedReq(t, "short opmap", r1, r2)
	expectUnequalShardedReq(t, "nil vs nonempty", r1, nil)
	delete(r1.Ops, 1)
	expectEqualShardedReq(t, r1, nil)
	r1.Ops[2] = stash[:1]
	expectUnequalShardedReq(t, "short ops", r1, r2)
	r1.Ops[2] = stash
	r1.Ops[2][1].Seq = 2
	expectUnequalShardedReq(t, "mismatched ops", r1, r2)

}
