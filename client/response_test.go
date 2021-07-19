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

// package ctl contains all pilosa subcommands other than 'server'. These are
// generally administration, testing, and debugging tools.

package client

import (
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"testing"

	"github.com/molecula/featurebase/v2/pb"
)

func TestNewRowResultFromInternal(t *testing.T) {
	targetColumns := []uint64{5, 10}
	row := &pb.Row{
		Columns: []uint64{5, 10},
	}
	result, err := newRowResultFromInternal(row)
	if err != nil {
		t.Fatalf("Failed with error: %s", err)
	}
	if !reflect.DeepEqual(targetColumns, result.Columns) {
		t.Fatal()
	}
}

func TestNewQueryResponseFromInternal(t *testing.T) {
	targetColumns := []uint64{5, 10}
	targetCountItems := []CountResultItem{
		{ID: 10, Count: 100},
	}
	row := &pb.Row{
		Columns: []uint64{5, 10},
	}
	pairs := []*pb.Pair{
		{ID: 10, Count: 100},
	}
	response := &pb.QueryResponse{
		Results: []*pb.QueryResult{
			{Type: QueryResultTypeRow, Row: row},
			{Type: QueryResultTypePairs, Pairs: pairs},
		},
		Err: "",
	}
	qr, err := newQueryResponseFromInternal(response)
	if err != nil {
		t.Fatalf("Failed with error: %s", err)
	}
	if qr.ErrorMessage != "" {
		t.Fatalf("ErrorMessage should be empty")
	}
	if !qr.Success {
		t.Fatalf("IsSuccess should be true")
	}

	results := qr.Results()
	if len(results) != 2 {
		t.Fatalf("Number of results should be 2")
	}
	if results[0] != qr.Result() {
		t.Fatalf("Result() should return the first result")
	}
	if !reflect.DeepEqual(targetColumns, results[0].Row().Columns) {
		t.Fatalf("The row result should contain the columns")
	}
	if !reflect.DeepEqual(targetCountItems, results[1].CountItems()) {
		t.Fatalf("The response should include count items")
	}
}

func TestNewQueryResponseWithErrorFromInternal(t *testing.T) {
	response := &pb.QueryResponse{
		Err: "some error",
	}
	qr, err := newQueryResponseFromInternal(response)
	if err != nil {
		t.Fatalf("Failed with error: %s", err)
	}
	if qr.ErrorMessage != "some error" {
		t.Fatalf("The response should include the error message")
	}
	if qr.Success {
		t.Fatalf("IsSuccess should be false")
	}
	if qr.Result() != nil {
		t.Fatalf("If there are no results, Result should return nil")
	}
}

func TestCountResultItemToString(t *testing.T) {
	tests := []struct {
		item     *CountResultItem
		expected string
	}{
		{item: &CountResultItem{ID: 100, Count: 50}, expected: "100:50"},
		{item: &CountResultItem{Key: "blah", Count: 50}, expected: "blah:50"},
		{item: &CountResultItem{Key: "blah", ID: 22, Count: 50}, expected: "blah:50"},
		{item: &CountResultItem{Key: "blah", ID: 22}, expected: "blah:0"},
		{item: &CountResultItem{}, expected: "0:0"},
	}

	for i, tst := range tests {
		t.Run(fmt.Sprintf("%d: ", i), func(t *testing.T) {
			if tst.expected != tst.item.String() {
				t.Fatalf("%s != %s", tst.expected, tst.item.String())
			}
		})
	}
}

func TestMarshalResults(t *testing.T) {
	row := &pb.Row{
		Columns: []uint64{5, 10},
	}
	pairs := []*pb.Pair{
		{ID: 10, Count: 100},
	}
	pbufResults := []*pb.QueryResult{
		{Type: QueryResultTypeRow, Row: row},
		{Type: QueryResultTypePairs, Pairs: pairs},
	}
	resultJSONStrings := make([]string, len(pbufResults))
	for i, pr := range pbufResults {
		r, err := newQueryResultFromInternal(pr)
		if err != nil {
			t.Fatal(err)
		}
		b, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		resultJSONStrings[i] = string(b)
	}
	targetJSON := []string{
		`{"columns":[5,10],"keys":[]}`,
		`[{"id":10,"count":100}]`,
	}
	for i := range targetJSON {
		if sortedString(targetJSON[i]) != sortedString(resultJSONStrings[i]) {
			t.Fatalf("%v != %v ", targetJSON[i], resultJSONStrings[i])
		}
	}

}

func TestUnknownQueryResultType(t *testing.T) {
	result := &pb.QueryResult{
		Type: 999,
	}
	_, err := newQueryResultFromInternal(result)
	if err != ErrUnknownType {
		t.Fatalf("Should have failed with ErrUnknownType")
	}
}

func TestTopNResult(t *testing.T) {
	result := TopNResult{
		CountResultItem{ID: 100, Count: 10},
	}
	expectResult(t, result, QueryResultTypePairsField, RowResult{}, []CountResultItem{{100, "", 10}}, 0, 0, false, nil, RowIdentifiersResult{})
}

func TestRowResult(t *testing.T) {
	result := RowResult{
		Columns: []uint64{1, 2, 3},
	}
	targetBmp := RowResult{
		Columns: []uint64{1, 2, 3},
	}
	expectResult(t, result, QueryResultTypeRow, targetBmp, nil, 0, 0, false, nil, RowIdentifiersResult{})
}

func TestRowResultNilColumns(t *testing.T) {
	result := RowResult{
		Columns: nil,
	}
	_, err := result.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
}

func TestSumCountResult(t *testing.T) {
	result := ValCountResult{
		Val: 100,
		Cnt: 50,
	}
	expectResult(t, result, QueryResultTypeValCount, RowResult{}, nil, 100, 50, false, nil, RowIdentifiersResult{})
}

func TestIntResult(t *testing.T) {
	result := IntResult(11)
	expectResult(t, result, QueryResultTypeUint64, RowResult{}, nil, 0, 11, false, nil, RowIdentifiersResult{})
}

func TestBoolResult(t *testing.T) {
	result := BoolResult(true)
	expectResult(t, result, QueryResultTypeBool, RowResult{}, nil, 0, 0, true, nil, RowIdentifiersResult{})
}

func TestNilResult(t *testing.T) {
	result := NilResult{}
	expectResult(t, result, QueryResultTypeNil, RowResult{}, nil, 0, 0, false, nil, RowIdentifiersResult{})
}

func TestGroupCountResult(t *testing.T) {
	result := GroupCountResult{
		{Groups: []FieldRow{{FieldName: "f1", RowID: 1}}, Count: 2},
		{Groups: []FieldRow{{FieldName: "f1", RowID: 2}}, Count: 1},
	}
	expectResult(t, result, QueryResultTypeGroupCounts, RowResult{}, nil, 0, 0, false, []GroupCount{
		{Groups: []FieldRow{{FieldName: "f1", RowID: 1}}, Count: 2},
		{Groups: []FieldRow{{FieldName: "f1", RowID: 2}}, Count: 1},
	}, RowIdentifiersResult{})
}

func TestGroupCountWithValueResult(t *testing.T) {
	var a, b int64 = -1, 1

	result := GroupCountResult{
		{Groups: []FieldRow{{FieldName: "f1", Value: &a}}, Count: 1},
		{Groups: []FieldRow{{FieldName: "f1", Value: &b}}, Count: 1},
	}

	var aa, bb int64 = -1, 1
	expectResult(t, result, QueryResultTypeGroupCounts, RowResult{}, nil, 0, 0, false, []GroupCount{
		{Groups: []FieldRow{{FieldName: "f1", Value: &aa}}, Count: 1},
		{Groups: []FieldRow{{FieldName: "f1", Value: &bb}}, Count: 1},
	}, RowIdentifiersResult{})
}

func TestRowIdentifiersResult(t *testing.T) {
	result := RowIdentifiersResult{
		IDs: []uint64{1, 2, 3, 4},
	}
	expectResult(t, result, QueryResultTypeRowIdentifiers, RowResult{}, nil, 0, 0, false, nil, RowIdentifiersResult{
		IDs: []uint64{1, 2, 3, 4},
	})
}

func expectResult(t *testing.T, r QueryResult, resultType uint32, bmp RowResult,
	countItems []CountResultItem, sum int64, count int64, changed bool,
	groupCounts []GroupCount, rowIdentifiers RowIdentifiersResult) {
	if resultType != r.Type() {
		log.Fatalf("Result type: %d != %d", resultType, r.Type())
	}
	if !reflect.DeepEqual(bmp, r.Row()) {
		log.Fatalf("Row: %v != %v", bmp, r.Row())
	}
	if !reflect.DeepEqual(countItems, r.CountItems()) {
		log.Fatalf("Count items: %v != %v", countItems, r.CountItems())
	}
	if count != r.Count() {
		log.Fatalf("Count: %d != %d", count, r.Count())
	}
	if sum != r.Value() {
		log.Fatalf("Sum: %d != %d", sum, r.Value())
	}
	if changed != r.Changed() {
		log.Fatalf("Changed: %v != %v", changed, r.Changed())
	}
	if !reflect.DeepEqual(groupCounts, r.GroupCounts()) {
		log.Fatalf("Group counts: %v != %v", groupCounts, r.GroupCounts())
	}
	if !reflect.DeepEqual(rowIdentifiers, r.RowIdentifiers()) {
		log.Fatalf("Row identifiers: %v != %v", rowIdentifiers, r.RowIdentifiers())
	}
}
