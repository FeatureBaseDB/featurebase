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

package proto

import (
	"errors"
	"reflect"
	"testing"

	"github.com/molecula/featurebase/v2"
	"github.com/molecula/featurebase/v2/ingest"
)

func testOneRoundTrip(t *testing.T, s pilosa.Serializer, obj pilosa.Message, expectedMarshalErr error, expectedUnmarshalErr error, expectedMismatchErr error) {
	repr, err := s.Marshal(obj)
	if err != nil {
		if expectedMarshalErr == nil {
			t.Fatalf("unexpected marshalling error %q", err.Error())
		}
		if err.Error() != expectedMarshalErr.Error() {
			t.Fatalf("expecting marshalling error %q, got %q", expectedMarshalErr.Error(), err.Error())
		}
	} else {
		if expectedMarshalErr != nil {
			t.Fatalf("expected marshalling error %q, got no error", expectedMarshalErr.Error())
		}
	}

	obj2 := reflect.New(reflect.TypeOf(obj).Elem()).Interface()
	err = s.Unmarshal(repr, obj2)
	if err != nil {
		if expectedUnmarshalErr == nil {
			t.Fatalf("unexpected unmarshalling error %q", err.Error())
		}
		if err.Error() != expectedUnmarshalErr.Error() {
			t.Fatalf("expecting unmarshalling error %q, got %q", expectedUnmarshalErr.Error(), err.Error())
		}
	} else {
		if expectedUnmarshalErr != nil {
			t.Fatalf("expected unmarshalling error %q, got no error", expectedUnmarshalErr.Error())
		}
	}
	switch real := obj.(type) {
	case *ingest.ShardedRequest:
		real2 := obj2.(*ingest.ShardedRequest)
		err := real.Compare(real2)
		if err != nil {
			if expectedMismatchErr == nil {
				t.Fatalf("unexpected compare error %q", err.Error())
			}
			if err.Error() != expectedMismatchErr.Error() {
				t.Fatalf("expecting compare error %q, got %q", expectedMismatchErr.Error(), err.Error())
			}
		} else {
			if expectedMismatchErr != nil {
				t.Fatalf("expected compare error %q, got no error", expectedMismatchErr.Error())
			}
		}
	default:
		if !reflect.DeepEqual(obj, obj2) {
			t.Fatalf("serialization round trip failed for %T:\nexpected %#v\ngot %#v", obj, obj, obj2)
		}
	}
}

type shardedIngestRequestTest struct {
	req *ingest.ShardedRequest
	err error
}

var shardedIngestRequestTestcases = []shardedIngestRequestTest{
	{
		req: &ingest.ShardedRequest{
			Ops: map[uint64][]*ingest.Operation{
				1: {
					{
						OpType:         ingest.OpWrite,
						ClearFields:    []string{"clearField", "clearField2"},
						ClearRecordIDs: []uint64{1, 7, 9},
						FieldOps: map[string]*ingest.FieldOperation{
							"writeAll": {
								RecordIDs: []uint64{3, 6, 8},
								Values:    []uint64{0, 17, 34},
								Signed:    []int64{-9, 23, 17},
							},
							"writeValues": {
								RecordIDs: []uint64{3, 6, 8},
								Values:    []uint64{0, 17, 34},
							},
							"writeSigned": {
								RecordIDs: []uint64{3, 6, 8},
								Signed:    []int64{-9, 23, 17},
							},
						},
					},
					{
						OpType: ingest.OpSet,
						FieldOps: map[string]*ingest.FieldOperation{
							"foo": nil,
						},
					},
				},
				2: {},
				3: nil,
			},
		},
	},
	{
		req: &ingest.ShardedRequest{
			Ops: map[uint64][]*ingest.Operation{
				1: {
					{
						OpType: ingest.OpWrite,
						FieldOps: map[string]*ingest.FieldOperation{
							"writeAll": {},
						},
					},
					{
						OpType: ingest.OpSet,
						FieldOps: map[string]*ingest.FieldOperation{
							"foo": nil,
						},
					},
					nil,
				},
			},
		},
		err: errors.New("shard 1: expected 3 ops, got 2"),
	},
}

func TestIngestRoundTrip(t *testing.T) {
	for _, tc := range shardedIngestRequestTestcases {
		t.Logf("next case")
		testOneRoundTrip(t, DefaultSerializer, tc.req, nil, nil, tc.err)
	}
}