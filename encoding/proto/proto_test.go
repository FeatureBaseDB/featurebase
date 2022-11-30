// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package proto

import (
	"reflect"
	"testing"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"github.com/gomem/gomem/pkg/dataframe"
	pilosa "github.com/molecula/featurebase/v3"
	"github.com/molecula/featurebase/v3/pb"
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
	if !reflect.DeepEqual(obj, obj2) {
		t.Fatalf("serialization round trip failed for %T:\nexpected %#v\ngot %#v", obj, obj, obj2)
	}
}

func TestEncodeDecodeDistinctTimestamp(t *testing.T) {
	s := Serializer{}
	pbTime := pb.DistinctTimestamp{
		Values: []string{"this", "is", "fake", "timestamp", "values"},
		Name:   "pbtime",
	}
	piloTime := pilosa.DistinctTimestamp{
		Values: []string{"this", "is", "fake", "timestamp", "values"},
		Name:   "pbtime",
	}
	decoded := s.decodeDistinctTimestamp(&pbTime)
	if !reflect.DeepEqual(decoded, piloTime) {
		t.Errorf("failed to decode DistinctTimestamp. expected %v got %v", piloTime, decoded)
	}
	encoded := s.encodeDistinctTimestamp(piloTime)
	if !reflect.DeepEqual(encoded, &pbTime) {
		t.Errorf("failed to encode DistinctTimestamp. expected %v got %v", &pbTime, encoded)
	}
}

func TestDecodeQueryResult(t *testing.T) {
	t.Run("DistinctTimestamp", func(t *testing.T) {
		pbTime := pb.DistinctTimestamp{
			Values: []string{"this", "is", "fake", "timestamp", "values"},
			Name:   "pbtime",
		}
		piloTime := pilosa.DistinctTimestamp{
			Values: []string{"this", "is", "fake", "timestamp", "values"},
			Name:   "pbtime",
		}
		q := &pb.QueryResult{Type: queryResultTypeDistinctTimestamp, DistinctTimestamp: &pbTime}
		s := Serializer{}
		decoded := s.decodeQueryResult(q)
		if !reflect.DeepEqual(decoded, piloTime) {
			t.Errorf("failed to decode DistinctTimestamp. expected %v got %v", piloTime, decoded)
		}
	})
}

func TestDataFrameQueryResult(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())

	t.Run("Dataframe", func(t *testing.T) {
		df, err := CreateDataframe(pool)
		if err != nil {
			t.Fatalf("need a good starting point %v", err)
		}
		s := Serializer{}
		pdf := s.encodeDataFrame(df)
		q := &pb.QueryResult{Type: queryResultTypeDataFrame, DataFrame: pdf}
		decoded := s.decodeQueryResult(q).(*dataframe.DataFrame)

		if !df.Equals(decoded) {
			t.Errorf("failed to decode Dataframe. expected\n %#v\n got\n %#v", df, decoded)
		}
	})
}

func TestArrowResult(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.NewGoAllocator())

	t.Run("Arrow", func(t *testing.T) {
		table := CreateArrowTable(pool)
		defer table.Release()
		s := Serializer{}
		art := s.encodeArrowTable(table)
		q := &pb.QueryResult{Type: queryResultTypeArrowTable, ArrowTable: art}
		decoded := s.decodeQueryResult(q).(arrow.Table)

		if table.NumRows() != decoded.NumRows() {
			t.Errorf("failed to decode Dataframe. expected\n %#v\n got\n %#v", table.NumRows(), decoded.NumRows())
		}
	})
}

func CreateDataframe(pool memory.Allocator) (*dataframe.DataFrame, error) {
	table := CreateArrowTable(pool)
	return dataframe.NewDataFrameFromTable(pool, table)
}

func CreateArrowTable(pool memory.Allocator) arrow.Table {
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "INT", Type: arrow.PrimitiveTypes.Int64},
			{Name: "FLOAT", Type: arrow.PrimitiveTypes.Float64},
		},
		nil,
	)

	b := array.NewRecordBuilder(pool, schema)
	defer b.Release()
	// create 2 chunks
	b.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3, 4, 5, 6}, nil)
	b.Field(0).(*array.Int64Builder).AppendValues([]int64{7, 8, 9, 10}, []bool{true, true, false, true})
	b.Field(1).(*array.Float64Builder).AppendValues([]float64{1, 2, 3, 4, 5, 6}, nil)
	b.Field(1).(*array.Float64Builder).AppendValues([]float64{7, 8, 9, 10}, []bool{true, true, false, true})

	rec1 := b.NewRecord()

	b.Field(0).(*array.Int64Builder).AppendValues([]int64{11, 12, 13, 14, 15, 16, 17, 18, 19, 20}, nil)
	b.Field(1).(*array.Float64Builder).AppendValues([]float64{11, 12, 13, 14, 15, 16, 17, 18, 19, 20}, nil)

	rec2 := b.NewRecord()

	b.Field(0).(*array.Int64Builder).AppendValues([]int64{31, 32, 33, 34, 35, 36, 37, 38, 39, 40}, nil)
	b.Field(1).(*array.Float64Builder).AppendValues([]float64{31, 32, 33, 34, 35, 36, 37, 38, 39, 40}, nil)

	rec3 := b.NewRecord()
	records := []arrow.Record{rec1, rec2, rec3}
	return array.NewTableFromRecords(schema, records)
}

func areEqualKV(a, b []pilosa.RowKV) bool {
	return reflect.DeepEqual(a, b)
}

func TestExtractedIDMatrixSorted(t *testing.T) {
	areEqualEIM := func(a, b *pilosa.ExtractedIDMatrix) bool {
		if !reflect.DeepEqual(a.Fields, b.Fields) {
			return false
		}
		if len(a.Columns) != len(b.Columns) {
			return false
		}
		for i := range a.Columns {
			if a.Columns[i].ColumnID != b.Columns[i].ColumnID {
				return false
			}
			for x := range a.Columns[i].Rows {
				if len(a.Columns[i].Rows[x]) != len(b.Columns[i].Rows[x]) {
					return false
				}
				for y := range a.Columns[i].Rows[x] {
					if a.Columns[i].Rows[x][y] != b.Columns[i].Rows[x][y] {
						return false
					}
				}
			}
		}
		return true
	}
	compare := func(a, b pilosa.ExtractedIDMatrixSorted) bool {
		return areEqualKV(a.RowKVs, b.RowKVs) && areEqualEIM(a.ExtractedIDMatrix, b.ExtractedIDMatrix)
	}
	t.Run("ExtractedIDMatrixSorted", func(t *testing.T) {
		// not correct ExtractedIDMatrix contents, but will test the typing appropriately
		em := &pilosa.ExtractedIDMatrix{
			Fields: []string{"A", "B", "C"},
			Columns: []pilosa.ExtractedIDColumn{
				{ColumnID: 1, Rows: [][]uint64{{1}}},
				{ColumnID: 2, Rows: [][]uint64{{2}}},
				{ColumnID: 3, Rows: [][]uint64{{3}}},
			},
		}
		rows := []pilosa.RowKV{
			{RowID: 1, Value: int64(10)},
			{RowID: 2, Value: int64(20)},
			{RowID: 3, Value: int64(30)},
		}
		table := pilosa.ExtractedIDMatrixSorted{
			ExtractedIDMatrix: em,
			RowKVs:            rows,
		}
		s := Serializer{}
		before := s.endcodeExtractedIDMatrixSorted(table)
		q := &pb.QueryResult{Type: queryResultTypeExtractedIDMatrixSorted, ExtractedIDMatrixSorted: before}
		decoded := s.decodeQueryResult(q).(pilosa.ExtractedIDMatrixSorted)

		if !compare(table, decoded) {
			t.Errorf("failed to decode ExtractedIDMatrixSorted. expected\n %#v\n got\n %#v", table, decoded)
		}
	})
}

func TestSerializer_RowKVs(t *testing.T) {
	tests := []struct {
		name string
		args []pilosa.RowKV
	}{
		{
			name: "IDs",
			args: []pilosa.RowKV{
				{RowID: 1, Value: []uint64{10}},
			},
		},
		{
			name: "Keys",
			args: []pilosa.RowKV{
				{RowID: 1, Value: []string{"aaaa"}},
			},
		},
		{
			name: "BSIValue",
			args: []pilosa.RowKV{
				{RowID: 1, Value: int64(52)},
			},
		},
		{
			name: "MutexID",
			args: []pilosa.RowKV{
				{RowID: 1, Value: uint64(42)},
			},
		},
		{
			name: "MutexKey",
			args: []pilosa.RowKV{
				{RowID: 1, Value: "aaaa"},
			},
		},
		{
			name: "Bool",
			args: []pilosa.RowKV{
				{RowID: 1, Value: true},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := Serializer{}
			encoded := s.encodeRowKVs(tt.args)
			after := s.decodeRowKVs(encoded)
			if !areEqualKV(tt.args, after) {
				t.Fatalf("expected: %v got: %v", tt.args, after)
			}
		})
	}
}

func TestSerializer_ExtractedTable(t *testing.T) {
	compare := func(a, b pilosa.ExtractedTable) bool {
		return reflect.DeepEqual(a, b)
		// return true
	}
	tests := []struct {
		name string
		args pilosa.ExtractedTable
	}{
		{
			name: "uint64",
			args: pilosa.ExtractedTable{
				Fields: []pilosa.ExtractedTableField{
					{Name: "a", Type: "uint64"},
				},
				Columns: []pilosa.ExtractedTableColumn{
					{
						Column: pilosa.KeyOrID{ID: 0},
						Rows:   []interface{}{uint64(1)},
					},
				},
			},
		},
		{
			name: "int64",
			args: pilosa.ExtractedTable{
				Fields: []pilosa.ExtractedTableField{
					{Name: "a", Type: "int64"},
				},
				Columns: []pilosa.ExtractedTableColumn{
					{
						Column: pilosa.KeyOrID{ID: 0},
						Rows:   []interface{}{int64(1)},
					},
				},
			},
		},
		{
			name: "string",
			args: pilosa.ExtractedTable{
				Fields: []pilosa.ExtractedTableField{
					{Name: "a", Type: "string"},
				},
				Columns: []pilosa.ExtractedTableColumn{
					{
						Column: pilosa.KeyOrID{ID: 0},
						Rows:   []interface{}{"a"},
					},
				},
			},
		},
		{
			name: "[]string",
			args: pilosa.ExtractedTable{
				Fields: []pilosa.ExtractedTableField{
					{Name: "a", Type: "[]string"},
				},
				Columns: []pilosa.ExtractedTableColumn{
					{
						Column: pilosa.KeyOrID{ID: 0},
						Rows:   []interface{}{[]string{"a"}},
					},
				},
			},
		},
		{
			name: "bool",
			args: pilosa.ExtractedTable{
				Fields: []pilosa.ExtractedTableField{
					{Name: "a", Type: "bool"},
				},
				Columns: []pilosa.ExtractedTableColumn{
					{
						Column: pilosa.KeyOrID{ID: 0},
						Rows:   []interface{}{true},
					},
				},
			},
		},
		{
			name: "[]uint64",
			args: pilosa.ExtractedTable{
				Fields: []pilosa.ExtractedTableField{
					{Name: "a", Type: "[]uint64"},
				},
				Columns: []pilosa.ExtractedTableColumn{
					{
						Column: pilosa.KeyOrID{ID: 0},
						Rows:   []interface{}{[]uint64{2}},
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := Serializer{}
			encoded := s.encodeExtractedTable(tt.args)
			after := s.decodeExtractedTable(encoded)
			if !compare(tt.args, after) {
				t.Fatalf("expected: %v got: %v", tt.args, after)
			}
		})
	}
}
