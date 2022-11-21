// Copyright 2021 Molecula Corp. All rights reserved.
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
