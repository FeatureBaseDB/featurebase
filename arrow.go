// Copyright 2021 Molecula Corp. All rights reserved.
package pilosa

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/ipc"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"github.com/apache/arrow/go/v10/parquet"
	"github.com/apache/arrow/go/v10/parquet/file"
	"github.com/apache/arrow/go/v10/parquet/pqarrow"
	"github.com/featurebasedb/featurebase/v3/pql"
	"github.com/featurebasedb/featurebase/v3/tracing"
	"github.com/gomem/gomem/pkg/dataframe"
	"github.com/pkg/errors"
)

/*
The function Arrow provides filtered access to the raw values stored in the dataframe.
If Arrow is just provided a bitmap filter, such as ConstRow or any Bitmap Operation,
all the values associated with each column are returned.  This set can be limited with
the addition of the header parameter
Example:
Arrow(ConstRow(columns=[2,4,6]),header=["fval"])
*/

// executeApply executes a Arrow() call.
func (e *executor) executeArrow(ctx context.Context, qcx *Qcx, index string, c *pql.Call, shards []uint64, opt *ExecOptions) (arrow.Table, error) {
	if !e.dataframeEnabled {
		return nil, errors.New("Dataframe support not enabled")
	}
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeArrow")
	defer span.Finish()
	if len(c.Children) > 1 {
		return nil, errors.New("Apply() only accepts a single bitmap input filter")
	}
	var columnFilter []string
	if cols, ok := c.Args["header"].([]interface{}); ok {
		columnFilter = make([]string, 0, len(cols))
		for _, v := range cols {
			columnFilter = append(columnFilter, v.(string))
		}
	}
	mapcounter := 0
	reducecounter := 0
	pool := memory.NewGoAllocator() // TODO(twg) 2022/09/01 singledton?
	// Execute calls in bulk on each remote node and merge.
	mu := &sync.Mutex{}
	mapFn := func(ctx context.Context, shard uint64, mopt *mapOptions) (_ interface{}, err error) {
		mu.Lock()
		mapcounter++
		mu.Unlock()
		return e.executeArrowShard(ctx, qcx, index, c, shard, pool, columnFilter)
	}
	tables := make([]*BasicTable, 0)

	reduceFn := func(ctx context.Context, prev, v interface{}) interface{} {
		mu.Lock()
		reducecounter++
		mu.Unlock()
		if v == nil {
			return prev
		}
		switch t := v.(type) {
		case *BasicTable:

			if t.resolver != nil {
				mu.Lock()
				tables = append(tables, t)
				mu.Unlock()
			}
		case arrow.Table:
			if t.NumRows() > 0 {
				bt := BasicTableFromArrow(t, pool)
				mu.Lock()
				tables = append(tables, bt)
				mu.Unlock()
			}
		}
		return nil
	}

	_, err := e.mapReduce(ctx, index, shards, c, opt, mapFn, reduceFn)
	if err != nil {
		return nil, err
	}
	if len(tables) == 0 {
		return &BasicTable{name: "empty"}, nil
	}
	tbl := Concat(tables[0].Schema(), tables, pool)
	r := dataframe.NewChunkResolver(tbl.Column(0))
	return &BasicTable{resolver: &r, table: tbl}, nil
}

type BasicTable struct {
	resolver dataframe.Resolver
	table    arrow.Table
	filtered bool
	name     string
}

func (st *BasicTable) Name() string {
	return st.name
}

func (st *BasicTable) Schema() *arrow.Schema {
	if st.table != nil {
		return st.table.Schema()
	}
	return &arrow.Schema{}
}

func (st *BasicTable) IsFiltered() bool {
	return st.filtered
}

func (st *BasicTable) NumRows() int64 {
	if st.resolver == nil {
		return 0
	}
	return int64(st.resolver.NumRows())
}

func (st *BasicTable) NumCols() int64 {
	if st.table != nil {
		return st.table.NumCols()
	}
	return 0
}

func (st *BasicTable) Column(i int) *arrow.Column {
	if st.table != nil {
		return st.table.Column(i)
	}
	return nil
}

func (st *BasicTable) Retain() {
	if st.table != nil {
		st.table.Retain()
	}
}

func (st *BasicTable) Release() {
	if st.table != nil {
		st.table.Retain()
	}
}

func (st *BasicTable) Get(column, row int) interface{} {
	field := st.Schema().Field(column)
	c, i := st.resolver.Resolve(row)
	nullable := field.Nullable

	chunk := st.Column(column).Data().Chunk(c)
	// TODO(twg) 2023/01/26 potential NULL support?
	if nullable && chunk.IsNull(i) {
		return nil
	}
	switch field.Type.(type) {
	case *arrow.BooleanType:
		return chunk.(*array.Boolean).Value(i)
	case *arrow.Int8Type:
		v := chunk.(*array.Int8).Int8Values()
		return int64(v[i])
	case *arrow.Int16Type:
		v := chunk.(*array.Int16).Int16Values()
		return int64(v[i])
	case *arrow.Int32Type:
		v := chunk.(*array.Int32).Int32Values()
		return int64(v[i])
	case *arrow.Int64Type:
		v := chunk.(*array.Int64).Int64Values()
		return int64(v[i])
	case *arrow.Uint8Type:
		v := chunk.(*array.Uint8).Uint8Values()
		return uint64(v[i])
	case *arrow.Uint16Type:
		v := chunk.(*array.Uint16).Uint16Values()
		return uint64(v[i])
	case *arrow.Uint32Type:
		v := chunk.(*array.Uint32).Uint32Values()
		return uint64(v[i])
	case *arrow.Uint64Type:
		v := chunk.(*array.Uint64).Uint64Values()
		return v[i]
	case *arrow.Float32Type:
		v := chunk.(*array.Float32).Float32Values()
		return float64(v[i])
	case *arrow.Float64Type:
		v := chunk.(*array.Float64).Float64Values()
		return v[i]
	case *arrow.StringType:
		return chunk.(*array.String).Value(i)
	}
	return 0
}

func builderFrom(mem memory.Allocator, dt arrow.DataType, size int64) array.Builder {
	var bldr array.Builder
	switch dt := dt.(type) {
	case *arrow.BooleanType:
		bldr = array.NewBooleanBuilder(mem)
	case *arrow.Int8Type:
		bldr = array.NewInt8Builder(mem)
	case *arrow.Int16Type:
		bldr = array.NewInt16Builder(mem)
	case *arrow.Int32Type:
		bldr = array.NewInt32Builder(mem)
	case *arrow.Int64Type:
		bldr = array.NewInt64Builder(mem)
	case *arrow.Uint8Type:
		bldr = array.NewUint8Builder(mem)
	case *arrow.Uint16Type:
		bldr = array.NewUint16Builder(mem)
	case *arrow.Uint32Type:
		bldr = array.NewUint32Builder(mem)
	case *arrow.Uint64Type:
		bldr = array.NewUint64Builder(mem)
	case *arrow.Float32Type:
		bldr = array.NewFloat32Builder(mem)
	case *arrow.Float64Type:
		bldr = array.NewFloat64Builder(mem)
	case *arrow.StringType:
		bldr = array.NewStringBuilder(mem)
	default:
		panic(fmt.Errorf("builderFrom: invalid Arrow type %v", dt))
	}
	bldr.Reserve(int(size))
	return bldr
}

func appendData(bldr array.Builder, v interface{}) {
	switch bldr := bldr.(type) {
	case *array.BooleanBuilder:
		bldr.Append(v.(bool))
	case *array.Int8Builder:
		bldr.Append(v.(int8))
	case *array.Int16Builder:
		bldr.Append(v.(int16))
	case *array.Int32Builder:
		bldr.Append(v.(int32))
	case *array.Int64Builder:
		bldr.Append(v.(int64))
	case *array.Uint8Builder:
		bldr.Append(v.(uint8))
	case *array.Uint16Builder:
		bldr.Append(v.(uint16))
	case *array.Uint32Builder:
		bldr.Append(v.(uint32))
	case *array.Uint64Builder:
		bldr.Append(v.(uint64))
	case *array.Float32Builder:
		bldr.Append(v.(float32))
	case *array.Float64Builder:
		bldr.Append(v.(float64))
	case *array.StringBuilder:
		bldr.Append(v.(string))
	default:
		panic(fmt.Errorf("appendData: invalid Arrow builder type %T", bldr))
	}
}

func Concat(schema *arrow.Schema, tables []*BasicTable, mem memory.Allocator) arrow.Table {
	if len(tables) == 1 {
		if !tables[0].IsFiltered() {
			return tables[0]
		}
	}
	cols := make([]arrow.Column, len(schema.Fields()))

	defer func(cols []arrow.Column) {
		for i := range cols {
			cols[i].Release()
		}
	}(cols)
	sz := 0
	for i := range tables {
		sz += int(tables[i].NumRows())
	}
	for i := range cols {
		field := schema.Field(i)
		arrs := make([]arrow.Array, 0)
		builder := builderFrom(mem, field.Type, int64(sz))
		for t := range tables {
			table := tables[t]
			if table.IsFiltered() {
				for row := 0; row < int(table.NumRows()); row++ {
					v := table.Get(i, row)
					appendData(builder, v)
				}
				arrs = append(arrs, builder.NewArray())

			} else {
				parts := table.Column(i).Data()
				arrs = append(arrs, parts.Chunks()...)
			}
		}
		chunk := arrow.NewChunked(field.Type, arrs)
		cols[i] = *arrow.NewColumn(field, chunk)
		chunk.Release()
	}
	return array.NewTable(schema, cols, -1)
}

func (st *BasicTable) MarshalJSON() ([]byte, error) {
	results := make(map[string]interface{})
	n := 0
	if st.table != nil {
		n = int(st.table.NumCols())
	}
	for b := 0; b < n; b++ {
		col := st.table.Column(b)
		result := make([]interface{}, st.resolver.NumRows())
		for n := st.resolver.NumRows() - 1; n >= 0; n-- {
			v := st.Get(b, n)
			result[n] = v

		}
		results[col.Name()] = result
	}
	return json.Marshal(results)
}

func BasicTableFromArrow(table arrow.Table, mem memory.Allocator) *BasicTable {
	col := table.Column(0)
	r := dataframe.NewChunkResolver(col)
	return &BasicTable{resolver: &r, table: table}
}

func filterColumns(filters []string, table arrow.Table) arrow.Table {
	filters = append(filters, "_ID")
	schema := table.Schema()
	// TODO(twg) 2022/11/09 add glob support
	allFields := schema.Fields()
	in := func(key string) bool {
		for _, v := range filters {
			if v == key {
				return true
			}
		}
		return false
	}
	cols := make([]arrow.Column, 0)
	fields := make([]arrow.Field, 0)
	for i := range allFields {
		field := allFields[i]
		if in(field.Name) {
			cols = append(cols, *table.Column(i))
			fields = append(fields, field)
		}
	}

	filterdSchema := arrow.NewSchema(fields, nil) // TODO(twg) 2022/11/09 handle meta:w
	return array.NewTable(filterdSchema, cols, table.NumRows())
}

func (e *executor) executeArrowShard(ctx context.Context, qcx *Qcx, index string, c *pql.Call, shard uint64, pool memory.Allocator, columnFilter []string) (*BasicTable, error) {
	name := fmt.Sprintf("a. %v", shard)
	span, _ := tracing.StartSpanFromContext(ctx, "Executor.executeArrowShard")
	defer span.Finish()

	var filter *Row
	if len(c.Children) == 1 {
		row, err := e.executeBitmapCallShard(ctx, qcx, index, c.Children[0], shard)
		if err != nil {
			return nil, err
		}
		filter = row
		if !filter.Any() {
			// no need to actuall run the query for its not operating against any values
			return &BasicTable{name: name}, nil
		}
	}
	//
	ids := filter.ShardColumns() // needs to be shard columns
	// Fetch index.
	idx := e.Holder.Index(index)
	if idx == nil {
		return nil, newNotFoundError(ErrIndexNotFound, index)
	}

	fname := idx.GetDataFramePath(shard)

	if !e.dataFrameExists(fname) {
		return &BasicTable{name: name}, nil
	}

	table, err := e.getDataTable(ctx, fname, pool)
	if err != nil {
		return nil, errors.Wrap(err, "arrow readTableParquet")
	}
	defer table.Release()
	if len(columnFilter) > 0 {
		table = filterColumns(columnFilter, table)
	}
	df, err := dataframe.NewDataFrameFromTable(pool, table)
	if err != nil {
		return nil, errors.Wrap(err, "arrow NewDataFromTable")
	}
	p := dataframe.NewChunkResolver(df.ColumnAt(0))
	var resolver dataframe.Resolver
	resolver = &p
	if filter != nil {
		if len(ids) == 0 {
			return &BasicTable{name: name}, nil
		}
		resolver, err = filterDataframe(resolver, pool, ids)
		if err != nil {
			return nil, errors.Wrap(err, "filtering dataframe")
		}
	}
	table.Retain()
	return &BasicTable{resolver: resolver, table: table, filtered: filter != nil, name: name}, nil
}

func (e *executor) dataFrameExists(fname string) bool {
	if e.typeIsParquet() {
		if _, err := os.Stat(fname + ".parquet"); os.IsNotExist(err) {
			return false
		}
		return true
	}
	if _, err := os.Stat(fname + ".arrow"); os.IsNotExist(err) {
		return false
	}
	return true
}

func (e *executor) getDataTable(ctx context.Context, fname string, mem memory.Allocator) (arrow.Table, error) {
	table, ok := e.frameCache[fname]
	if ok {
		return table, nil
	}
	if e.typeIsParquet() {
		table, err := readTableParquetCtx(ctx, fname, mem)
		e.frameCache[fname] = table
		return table, err
	}
	table, err := readTableArrow(fname, mem)
	if err != nil {
		return nil, err
	}
	e.frameCache[fname] = table
	return table, err
}

func (e *executor) typeIsParquet() bool {
	return e.datafameUseParquet
}

func (e *executor) IsDataframeFile(name string) bool {
	if e.typeIsParquet() {
		return strings.HasSuffix(name, ".parquet")
	}
	return strings.HasSuffix(name, ".arrow")
}

func (e *executor) SaveTable(name string, table arrow.Table, mem memory.Allocator) error {
	if e.typeIsParquet() {
		return writeTableParquet(table, name)
	}
	return writeTableArrow(table, name, mem)
}

func (e *executor) TableExtension() string {
	if e.typeIsParquet() {
		return ".parquet"
	}
	return ".arrow"
}

func readTableArrow(filename string, mem memory.Allocator) (arrow.Table, error) {
	r, err := os.Open(filename + ".arrow")
	if err != nil {
		return nil, err
	}
	rr, err := ipc.NewFileReader(r, ipc.WithAllocator(mem))
	if err != nil {
		return nil, err
	}
	defer rr.Close()
	records := make([]arrow.Record, rr.NumRecords(), rr.NumRecords())
	i := 0
	for {
		rec, err := rr.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		records[i] = rec
		i++
	}
	records = records[:i]
	table := array.NewTableFromRecords(rr.Schema(), records)
	return table, nil
}

func readTableParquetCtx(ctx context.Context, filename string, mem memory.Allocator) (arrow.Table, error) {
	r, err := os.Open(filename + ".parquet")
	if err != nil {
		return nil, err
	}
	defer r.Close()

	pf, err := file.NewParquetReader(r)
	if err != nil {
		return nil, err
	}

	reader, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{}, mem)
	if err != nil {
		return nil, err
	}
	return reader.ReadTable(ctx)
}

func writeTableParquet(table arrow.Table, filename string) error {
	f, err := os.Create(filename + ".parquet")
	if err != nil {
		return err
	}
	defer f.Close()
	props := parquet.NewWriterProperties(parquet.WithDictionaryDefault(false))
	arrProps := pqarrow.DefaultWriterProps()
	chunkSize := 10 * 1024 * 1024
	err = pqarrow.WriteTable(table, f, int64(chunkSize), props, arrProps)
	if err != nil {
		return err
	}
	f.Sync()
	return nil
}

func writeTableArrow(table arrow.Table, filename string, mem memory.Allocator) error {
	f, err := os.Create(filename + ".arrow")
	if err != nil {
		return err
	}
	defer f.Close()
	writer, err := ipc.NewFileWriter(f, ipc.WithAllocator(mem), ipc.WithSchema(table.Schema()))
	if err != nil {
		panic(err)
	}
	chunkSize := int64(0)
	tr := array.NewTableReader(table, chunkSize)
	defer tr.Release()
	n := 0
	for tr.Next() {
		arec := tr.Record()
		err = writer.Write(arec)
		if err != nil {
			panic(err)
		}
		n++
	}
	err = writer.Close()
	if err != nil {
		panic(err)
	}
	f.Sync()
	return nil
}
