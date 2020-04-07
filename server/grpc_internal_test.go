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

package server

import (
	"testing"

	"github.com/pilosa/pilosa/v2"
	"github.com/pilosa/pilosa/v2/logger"
)

func TestGRPC(t *testing.T) {
	t.Run("makeRows", func(t *testing.T) {
		type expHeader struct {
			name     string
			dataType string
		}

		type expColumn interface{}

		va, vb := int64(-11), int64(-12)
		tests := []struct {
			result     interface{}
			expHeaders []expHeader
			expColumns [][]expColumn
		}{
			{
				pilosa.ValCount{Val: 1, Count: 1},
				[]expHeader{{"value", "int64"}, {"count", "int64"}},
				[][]expColumn{{int64(1), int64(1)}},
			},
			{
				pilosa.ValCount{FloatVal: 1.24, Count: 1},
				[]expHeader{{"value", "float64"}, {"count", "int64"}},
				[][]expColumn{{float64(1.24), int64(1)}},
			},
			// Row (uint64)
			{
				pilosa.NewRow(10, 11, 12),
				[]expHeader{
					{"_id", "uint64"},
				},
				[][]expColumn{
					{uint64(10)},
					{uint64(11)},
					{uint64(12)},
				},
			},
			// Row (string)
			{
				&pilosa.Row{Keys: []string{"ten", "eleven", "twelve"}},
				[]expHeader{
					{"_id", "string"},
				},
				[][]expColumn{
					{"ten"},
					{"eleven"},
					{"twelve"},
				},
			},
			// Pair (uint64)
			{
				pilosa.Pair{ID: 10, Count: 123},
				[]expHeader{
					{"_id", "uint64"},
					{"count", "uint64"},
				},
				[][]expColumn{
					{uint64(10), uint64(123)},
				},
			},
			// Pair (string)
			{
				pilosa.Pair{Key: "ten", Count: 123},
				[]expHeader{
					{"_id", "string"},
					{"count", "uint64"},
				},
				[][]expColumn{
					{string("ten"), uint64(123)},
				},
			},
			// []Pair (uint64)
			{
				[]pilosa.Pair{
					{ID: 10, Count: 123},
					{ID: 11, Count: 456},
				},
				[]expHeader{
					{"_id", "uint64"},
					{"count", "uint64"},
				},
				[][]expColumn{
					{uint64(10), uint64(123)},
					{uint64(11), uint64(456)},
				},
			},
			// []Pair (string)
			{
				[]pilosa.Pair{
					{Key: "ten", Count: 123},
					{Key: "eleven", Count: 456},
				},
				[]expHeader{
					{"_id", "string"},
					{"count", "uint64"},
				},
				[][]expColumn{
					{"ten", uint64(123)},
					{"eleven", uint64(456)},
				},
			},
			// []GroupCount (uint64)
			{
				[]pilosa.GroupCount{
					pilosa.GroupCount{
						Group: []pilosa.FieldRow{
							{Field: "a", RowID: 10},
							{Field: "b", RowID: 11},
						},
						Count: 123,
					},
					pilosa.GroupCount{
						Group: []pilosa.FieldRow{
							{Field: "a", RowID: 10},
							{Field: "b", RowID: 12},
						},
						Count: 456,
					},
					pilosa.GroupCount{
						Group: []pilosa.FieldRow{
							{Field: "va", Value: &va},
							{Field: "vb", Value: &vb},
						},
						Count: 789,
					},
				},
				[]expHeader{
					{"a", "uint64"},
					{"b", "uint64"},
					{"count", "uint64"},
					{"sum", "int64"},
				},
				[][]expColumn{
					{uint64(10), uint64(11), uint64(123), int64(0)},
					{uint64(10), uint64(12), uint64(456), int64(0)},
					{int64(va), int64(vb), uint64(789), int64(0)},
				},
			},
			// []GroupCount (string)
			{
				[]pilosa.GroupCount{
					pilosa.GroupCount{
						Group: []pilosa.FieldRow{
							{Field: "a", RowKey: "ten"},
							{Field: "b", RowKey: "eleven"},
						},
						Count: 123,
					},
					{
						Group: []pilosa.FieldRow{
							{Field: "a", RowKey: "ten"},
							{Field: "b", RowKey: "twelve"},
						},
						Count: 456,
					},
				},
				[]expHeader{
					{"a", "string"},
					{"b", "string"},
					{"count", "uint64"},
					{"sum", "int64"},
				},
				[][]expColumn{
					{"ten", "eleven", uint64(123), int64(0)},
					{"ten", "twelve", uint64(456), int64(0)},
				},
			},
			// RowIdentifiers (uint64)
			{
				pilosa.RowIdentifiers{
					Rows: []uint64{10, 11, 12},
				},
				[]expHeader{
					{"", "uint64"}, // This is blank because we don't expose RowIdentifiers.field, so we have no way to set it for tests.
				},
				[][]expColumn{
					{uint64(10)},
					{uint64(11)},
					{uint64(12)},
				},
			},
			// RowIdentifiers (string)
			{
				pilosa.RowIdentifiers{
					Keys: []string{"ten", "eleven", "twelve"},
				},
				[]expHeader{
					{"", "string"}, // This is blank because we don't expose RowIdentifiers.field, so we have no way to set it for tests.
				},
				[][]expColumn{
					{"ten"},
					{"eleven"},
					{"twelve"},
				},
			},
			// uint64
			{
				uint64(123),
				[]expHeader{
					{"count", "uint64"},
				},
				[][]expColumn{
					{uint64(123)},
				},
			},
			// bool
			{
				true,
				[]expHeader{
					{"result", "bool"},
				},
				[][]expColumn{
					{true},
				},
			},
		}

		logger := logger.NopLogger
		for ti, test := range tests {
			results := make([]interface{}, 0)
			results = append(results, test.result)

			qr := pilosa.QueryResponse{}
			qr.Results = results

			ch := makeRows(qr, logger)

			cnt := 0
			for row := range ch {
				// Ensure headers match (on the first row).
				if cnt == 0 {
					for i, header := range row.GetHeaders() {
						if header.Name != test.expHeaders[i].name {
							t.Fatalf("test %d expected header name: %s, but got: %s", ti, test.expHeaders[i].name, header.Name)
						}
						if header.Datatype != test.expHeaders[i].dataType {
							t.Fatalf("test %d expected header data type: %s, but got: %s", ti, test.expHeaders[i].dataType, header.Datatype)
						}
					}
				}

				// Ensure column data matches.
				for i, column := range row.GetColumns() {
					switch v := test.expColumns[cnt][i].(type) {
					case string:
						val := column.GetStringVal()
						if val != v {
							t.Fatalf("test %d expected column val: %v, but got: %v", ti, v, val)
						}
					case uint64:
						val := column.GetUint64Val()
						if val != v {
							t.Fatalf("test %d expected column val: %v, but got: %v", ti, v, val)
						}
					case bool:
						val := column.GetBoolVal()
						if val != v {
							t.Fatalf("test %d expected column val: %v, but got: %v", ti, v, val)
						}
					case int64:
						val := column.GetInt64Val()
						if val != v {
							t.Fatalf("test %d expected column val: %v but got: %v", ti, v, val)
						}
					case float64:
						val := column.GetFloat64Val()
						if val != v {
							t.Fatalf("test %d expected column val: %v but got: %v", ti, v, val)
						}
					default:
						t.Fatalf("test %d has unhandled data type: %T", ti, v)
					}
				}

				cnt++
			}
		}
	})
}
