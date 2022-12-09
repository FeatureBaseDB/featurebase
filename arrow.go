// Copyright 2021 Molecula Corp. All rights reserved.
package pilosa

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"github.com/gomem/gomem/pkg/dataframe"
	"github.com/featurebasedb/featurebase/v3/pql"
	"github.com/featurebasedb/featurebase/v3/tracing"
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
	tables := make([]*basicTable, 0)

	reduceFn := func(ctx context.Context, prev, v interface{}) interface{} {
		mu.Lock()
		reducecounter++
		mu.Unlock()
		if v == nil {
			return prev
		}
		switch t := v.(type) {
		case *basicTable:

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
		return &basicTable{name: "empty"}, nil
	}
	tbl := Concat(tables[0].Schema(), tables, pool)
	r := dataframe.NewChunkResolver(tbl.Column(0))
	return &basicTable{resolver: &r, table: tbl}, nil
}

type basicTable struct {
	resolver dataframe.Resolver
	table    arrow.Table
	filtered bool
	name     string
}

func (st *basicTable) Name() string {
	return st.name
}

func (st *basicTable) Schema() *arrow.Schema {
	if st.table != nil {
		return st.table.Schema()
	}
	return &arrow.Schema{}
}

func (st *basicTable) IsFiltered() bool {
	return st.filtered
}

func (st *basicTable) NumRows() int64 {
	if st.resolver == nil {
		return 0
	}
	return int64(st.resolver.NumRows())
}

func (st *basicTable) NumCols() int64 {
	if st.table != nil {
		return st.table.NumCols()
	}
	return 0
}

func (st *basicTable) Column(i int) *arrow.Column {
	if st.table != nil {
		return st.table.Column(i)
	}
	return nil
}

func (st *basicTable) Retain() {
	if st.table != nil {
		st.table.Retain()
	}
}

func (st *basicTable) Release() {
	if st.table != nil {
		st.table.Retain()
	}
}

func (st *basicTable) Get(column, row int) interface{} {
	field := st.Schema().Field(column)
	c, i := st.resolver.Resolve(row)

	chunk := st.Column(column).Data().Chunk(c)
	switch field.Type.(type) {
	// case *arrow.BooleanType:
	//	v := chunk.(*array.Boolean).BooleanValues()
	//	return v[i]
	case *arrow.Int8Type:
		v := chunk.(*array.Int8).Int8Values()
		return v[i]
	case *arrow.Int16Type:
		v := chunk.(*array.Int16).Int16Values()
		return v[i]
	case *arrow.Int32Type:
		v := chunk.(*array.Int32).Int32Values()
		return v[i]
	case *arrow.Int64Type:
		v := chunk.(*array.Int64).Int64Values()
		return v[i]
	case *arrow.Uint8Type:
		v := chunk.(*array.Uint8).Uint8Values()
		return v[i]
	case *arrow.Uint16Type:
		v := chunk.(*array.Uint16).Uint16Values()
		return v[i]
	case *arrow.Uint32Type:
		v := chunk.(*array.Uint32).Uint32Values()
		return v[i]
	case *arrow.Uint64Type:
		v := chunk.(*array.Uint64).Uint64Values()
		return v[i]
	case *arrow.Float32Type:
		v := chunk.(*array.Float32).Float32Values()
		return v[i]
	case *arrow.Float64Type:
		v := chunk.(*array.Float64).Float64Values()
		return v[i]
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
	default:
		panic(fmt.Errorf("npy2root: invalid Arrow type %v", dt))
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
	default:
		panic(fmt.Errorf("npy2root: invalid Arrow builder type %T", bldr))
	}
}

func Concat(schema *arrow.Schema, tables []*basicTable, mem memory.Allocator) arrow.Table {
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

func (st *basicTable) MarshalJSON() ([]byte, error) {
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

func BasicTableFromArrow(table arrow.Table, mem memory.Allocator) *basicTable {
	col := table.Column(0)
	r := dataframe.NewChunkResolver(col)
	return &basicTable{resolver: &r, table: table}
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

func (e *executor) executeArrowShard(ctx context.Context, qcx *Qcx, index string, c *pql.Call, shard uint64, pool memory.Allocator, columnFilter []string) (*basicTable, error) {
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
			return &basicTable{name: name}, nil
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
	if _, err := os.Stat(fname + ".parquet"); os.IsNotExist(err) {
		return &basicTable{name: name}, nil
	}

	table, err := readTableParquetCtx(context.TODO(), fname, pool)
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
			return &basicTable{name: name}, nil
		}
		resolver, err = filterDataframe(resolver, pool, ids)
		if err != nil {
			return nil, errors.Wrap(err, "filtering dataframe")
		}
	}
	table.Retain()
	return &basicTable{resolver: resolver, table: table, filtered: filter != nil, name: name}, nil
}
