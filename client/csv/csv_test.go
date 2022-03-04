// Copyright 2021 Molecula Corp. All rights reserved.
package csv_test

import (
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/molecula/featurebase/v3"
	"github.com/molecula/featurebase/v3/client"
	"github.com/molecula/featurebase/v3/client/csv"
)

func TestCSVColumnIterator(t *testing.T) {
	reader := strings.NewReader(`1,10,683793200
		5,20,683793300
		3,41,683793385`)
	iterator := csv.NewColumnIterator(csv.RowIDColumnID, reader)
	columns := []client.Record{}
	for {
		column, err := iterator.NextRecord()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		columns = append(columns, column)
	}
	if len(columns) != 3 {
		t.Fatalf("There should be 3 columns")
	}
	target := []client.Column{
		{RowID: 1, ColumnID: 10, Timestamp: 683793200},
		{RowID: 5, ColumnID: 20, Timestamp: 683793300},
		{RowID: 3, ColumnID: 41, Timestamp: 683793385},
	}
	for i := range target {
		if !reflect.DeepEqual(target[i], columns[i]) {
			t.Fatalf("%v != %v", target[i], columns[i])
		}
	}
}

func TestCSVColumnIteratorWithTimestampFormatRowIDColumnID(t *testing.T) {
	format := "2006-01-02T03:04"
	reader := strings.NewReader(`1,10,1991-09-02T09:33
		5,20,1991-09-02T09:35
		3,41,1991-09-02T09:36`)
	iterator := csv.NewColumnIteratorWithTimestampFormat(csv.RowIDColumnID, reader, format)
	records := []client.Record{}
	for {
		record, err := iterator.NextRecord()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		records = append(records, record)
	}
	target := []client.Column{
		{RowID: 1, ColumnID: 10, Timestamp: 683803980000000000},
		{RowID: 5, ColumnID: 20, Timestamp: 683804100000000000},
		{RowID: 3, ColumnID: 41, Timestamp: 683804160000000000},
	}
	if len(records) != len(target) {
		t.Fatalf("There should be %d columns", len(target))
	}
	for i := range target {
		if !reflect.DeepEqual(target[i], records[i]) {
			t.Fatalf("%v != %v", target[i], records[i])
		}
	}
}

func TestCSVColumnIteratorWithTimestampFormatRowKeyColumnKey(t *testing.T) {
	format := "2006-01-02T03:04"
	reader := strings.NewReader(`one,ten,1991-09-02T09:33
		five,twenty,1991-09-02T09:35
		three,forty-one,1991-09-02T09:36`)
	iterator := csv.NewColumnIteratorWithTimestampFormat(csv.RowKeyColumnKey, reader, format)
	records := []client.Record{}
	for {
		record, err := iterator.NextRecord()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		records = append(records, record)
	}
	target := []client.Column{
		{RowKey: "one", ColumnKey: "ten", Timestamp: 683803980000000000},
		{RowKey: "five", ColumnKey: "twenty", Timestamp: 683804100000000000},
		{RowKey: "three", ColumnKey: "forty-one", Timestamp: 683804160000000000},
	}
	if len(records) != len(target) {
		t.Fatalf("There should be %d columns", len(target))
	}
	for i := range target {
		if !reflect.DeepEqual(target[i], records[i]) {
			t.Fatalf("%v != %v", target[i], records[i])
		}
	}
}

func TestCSVColumnIteratorWithTimestampFormatFail(t *testing.T) {
	format := "2014-07-16"
	reader := strings.NewReader(`1,10,X`)
	iterator := csv.NewColumnIteratorWithTimestampFormat(csv.RowIDColumnID, reader, format)
	_, err := iterator.NextRecord()
	if err == nil {
		t.Fatalf("Should have failed")
	}
}

func TestCSVValueIteratorWithColumnID(t *testing.T) {
	reader := strings.NewReader(`1,10
		5,-20
		3,41
	`)
	iterator := csv.NewValueIterator(csv.ColumnID, reader)
	values := []client.Record{}
	for {
		value, err := iterator.NextRecord()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		values = append(values, value)
	}
	target := []pilosa.FieldValue{
		{ColumnID: 1, Value: 10},
		{ColumnID: 5, Value: -20},
		{ColumnID: 3, Value: 41},
	}
	if len(values) != len(target) {
		t.Fatalf("There should be %d values, got %d", len(target), len(values))
	}
	for i := range target {
		v := values[i].(client.FieldValue)
		if !reflect.DeepEqual(pilosa.FieldValue(v), target[i]) {
			t.Fatalf("'%+v' != '%+v'", target[i], values[i])
		}
	}
}

func TestCSVValueIteratorWithColumnKey(t *testing.T) {
	reader := strings.NewReader(`one,10
		five,-20
		three,41
	`)
	iterator := csv.NewValueIterator(csv.ColumnKey, reader)
	values := []client.Record{}
	for {
		value, err := iterator.NextRecord()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		values = append(values, value)
	}
	target := []pilosa.FieldValue{
		{ColumnKey: "one", Value: 10},
		{ColumnKey: "five", Value: -20},
		{ColumnKey: "three", Value: 41},
	}
	if len(values) != len(target) {
		t.Fatalf("There should be %d values, got %d", len(target), len(values))
	}
	for i := range target {
		v := values[i].(client.FieldValue)
		if !reflect.DeepEqual(pilosa.FieldValue(v), target[i]) {
			t.Fatalf("%v != %v", target[i], values[i])
		}
	}
}

func TestCSValueIteratorWithInvalidFormat(t *testing.T) {
	reader := strings.NewReader("1,2")
	iterator := csv.NewValueIterator(csv.RowIDColumnID, reader)
	_, err := iterator.NextRecord()
	if err == nil {
		t.Fatalf("should have failed")
	}
}

func TestCSVColumnIteratorInvalidInput(t *testing.T) {
	invalidInputs := []string{
		// less than 2 columns
		"155",
		// invalid row ID
		"a5,155",
		// invalid column ID
		"155,a5",
		// invalid timestamp
		"155,255,a5",
	}
	for _, text := range invalidInputs {
		iterator := csv.NewColumnIterator(csv.RowIDColumnID, strings.NewReader(text))
		_, err := iterator.NextRecord()
		if err == nil {
			t.Fatalf("CSVColumnIterator input: %s should fail", text)
		}
	}
}

func TestCSVValueIteratorInvalidInput(t *testing.T) {
	invalidInputs := []string{
		// less than 2 columns
		"155",
		// invalid column ID
		"a5,155",
		// invalid value
		"155,a5",
	}
	for _, text := range invalidInputs {
		iterator := csv.NewValueIterator(csv.ColumnID, strings.NewReader(text))
		_, err := iterator.NextRecord()
		if err == nil {
			t.Fatalf("CSVValueIterator input: %s should fail", text)
		}
	}
}

func TestCSVColumnIteratorError(t *testing.T) {
	iterator := csv.NewColumnIterator(csv.RowIDColumnID, &BrokenReader{})
	_, err := iterator.NextRecord()
	if err == nil {
		t.Fatal("CSVColumnIterator should fail with error")
	}
}

func TestCSVValueIteratorError(t *testing.T) {
	iterator := csv.NewValueIterator(csv.ColumnID, &BrokenReader{})
	_, err := iterator.NextRecord()
	if err == nil {
		t.Fatal("CSVValueIterator should fail with error")
	}
}

type BrokenReader struct{}

func (r BrokenReader) Read(p []byte) (n int, err error) {
	return 0, errors.New("broken reader")
}
