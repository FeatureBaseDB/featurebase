// Copyright 2021 Molecula Corp. All rights reserved.
package pilosa

import (
	"context"
	"encoding/hex"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/memory"
)

func TempFileName(prefix string) string {
	randBytes := make([]byte, 16)
	rand.Read(randBytes)
	return filepath.Join(os.TempDir(), prefix+hex.EncodeToString(randBytes))
}

func Test_TableParquet(t *testing.T) {
	// create a arrow table
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "num", Type: arrow.PrimitiveTypes.Float64},
		},
		nil, // no metadata
	)
	mem := memory.NewGoAllocator()
	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()
	b.Field(0).(*array.Float64Builder).AppendValues([]float64{1.0, 1.5, 2.0}, nil)
	table := array.NewTableFromRecords(schema, []arrow.Record{b.NewRecord()})
	defer table.Release()
	fileName := TempFileName("pq-")
	// save it as  a parquet file
	err := writeTableParquet(table, fileName)
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(fileName)

	// read it back in and compare the result
	got, err := readTableParquetCtx(context.Background(), fileName, mem)
	if err != nil {
		t.Fatalf("readTableParquetCtx() error = %v", err)
	}
	if got.NumCols() != table.NumCols() {
		t.Errorf("got:%v expected:%v", got.NumCols(), table.NumCols())
	}
}
