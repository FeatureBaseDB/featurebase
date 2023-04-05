// Copyright 2021 Molecula Corp. All rights reserved.
package pilosa

import (
	"bytes"
	"context"
	"encoding/json"
	"math"

	"github.com/featurebasedb/featurebase/v3/pql"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
	"github.com/featurebasedb/featurebase/v3/tracing"
	"github.com/featurebasedb/featurebase/v3/tstore"
	"github.com/featurebasedb/featurebase/v3/wireprotocol"
	"github.com/pkg/errors"
)

/*
The function Tstore provides filtered access to the point lookup values in tsore.
If Tstore is just provided a bitmap filter, such as ConstRow or any Bitmap Operation,
all the values associated with each column are returned.  This set can be limited with
the addition of the header parameter
Example:
Tstore(ConstRow(columns=[2,4,6]),header=["fval"])
*/

func (e *executor) executeTstore(ctx context.Context, qcx *Qcx, index string, c *pql.Call, shards []uint64, opt *ExecOptions) (*TupleResults, error) {
	span, ctx := tracing.StartSpanFromContext(ctx, "Executor.executeTstore")
	defer span.Finish()
	if len(c.Children) > 1 {
		return nil, errors.New("Tstore() only accepts a single bitmap input filter")
	}
	var columnFilter []string
	if cols, ok := c.Args["header"].([]interface{}); ok {
		columnFilter = make([]string, 0, len(cols))
		for _, v := range cols {
			columnFilter = append(columnFilter, v.(string))
		}
	}
	// Execute calls in bulk on each remote node and merge.
	mapFn := func(ctx context.Context, shard uint64, mopt *mapOptions) (_ interface{}, err error) {
		return e.executeTstoreShard(ctx, qcx, index, c, shard, columnFilter)
	}
	results := &TupleResults{}

	reduceFn := func(ctx context.Context, prev, v interface{}) interface{} {
		if v == nil {
			return prev
		}
		r := v.(*TupleResults)
		if results.TupleSchema == nil {
			// just use the first one i get, they all should be the same
			results.TupleSchema = r.TupleSchema
		}
		results.Append(r)
		return nil
	}

	_, err := e.mapReduce(ctx, index, shards, c, opt, mapFn, reduceFn)
	if err != nil {
		return nil, err
	}
	return results, nil
}

func (e *executor) executeTstoreShard(ctx context.Context, qcx *Qcx, index string, c *pql.Call, shard uint64, columnFilter []string) (*TupleResults, error) {
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
			return &TupleResults{}, nil
		}
	}
	//
	ids := filter.Columns() // needs to be shard columns
	// Fetch index.
	idx := e.Holder.Index(index)
	if idx == nil {
		return nil, newNotFoundError(ErrIndexNotFound, index)
	}
	b, err := idx.GetTStore(shard)
	if err != nil {
		return nil, err
	}
	first := tstore.Int(0)
	last := tstore.Int(math.MaxInt32)
	i := 0
	in := func(a tstore.Sortable) bool {
		if i >= len(ids) {
			return false
		}
		for tstore.Int(ids[i]).Less(a) {
			i++
			if len(ids) == i {
				return false
			}
		}
		return true
	}
	if len(ids) == 0 {
		in = func(a tstore.Sortable) bool {
			return true
		}
	} else {
		first = tstore.Int(ids[0])
		last = tstore.Int(ids[len(ids)-1] + 1)
	}

	itr, err := b.NewRangeIterator(tstore.Int(first), tstore.Int(last))
	defer itr.Dispose()

	result := &TupleResults{}
	unset := true
	for itr.Next() {
		item, key := itr.Item()
		if unset {
			if len(columnFilter) > 0 {
				newSchema := make(types.Schema, 0)
				for _, name := range columnFilter {
					for i := range item.TupleSchema {
						if item.TupleSchema[i].ColumnName == name{
							newSchema=append(newSchema, item.TupleSchema[i]))
						}
					}
				}
				if len(newSchema) == 0{
					return &TupleResults{}, nil
				}

			}
			result.TupleSchema = item.TupleSchema
			unset = false
		}
		if in(key) {
			result.Add(item.Tuple)
		}
	}
	return result, nil
}

type TupleResults struct {
	TupleSchema types.Schema
	rows        []types.Row
}

func (tr *TupleResults) Add(item types.Row) {
	tr.rows = append(tr.rows, item)
}

func (tr *TupleResults) Append(t *TupleResults) {
	tr.rows = append(tr.rows, t.rows...)
}

func (tr *TupleResults) MarshalJSON() ([]byte, error) {
	results := make(map[string]interface{})
	columns := make([][]string, 0)
	for i := range tr.TupleSchema {
		column := tr.TupleSchema[i]
		columns = append(columns, []string{column.ColumnName, column.Type.BaseTypeName()})
	}
	rows := make([][]interface{}, 0)
	for i := range tr.rows {
		item := tr.rows[i]
		row := make([]interface{}, len(columns))
		for c := range item {
			row[c] = item[c]
		}
		rows = append(rows, row)
	}
	results["schema"] = columns
	results["rows"] = rows
	return json.Marshal(results)
}

func (tr *TupleResults) ToBytes() ([]byte, error) {
	buf := new(bytes.Buffer)

	// get the bytes for the schema
	b, err := wireprotocol.WriteSchema(tr.TupleSchema)
	if err != nil {
		return nil, errors.Wrap(err, "serializing tuple schema")
	}
	_, err = buf.Write(b)
	if err != nil {
		return nil, errors.Wrap(err, "serializing tuple schema")
	}

	// build a map of columnNames to column indexes in the schema
	/*
		colMap := make(map[string]int)
		for i, sc := range tr.TupleSchema {
			colMap[sc.ColumnName] = i
		}
	*/

	// iterate the tupleData - outside loop is rows
	for _, trow := range tr.rows {
		rb, err := wireprotocol.WriteRow(trow, tr.TupleSchema)
		if err != nil {
			return nil, errors.Wrap(err, "serializing tuple row")
		}
		_, err = buf.Write(rb)
		if err != nil {
			return nil, errors.Wrap(err, "serializing tuple row")
		}
	}

	// write done to the buffer
	b = wireprotocol.WriteDone()
	_, err = buf.Write(b)
	if err != nil {
		return nil, errors.Wrap(err, "serializing tuples")
	}

	return buf.Bytes(), nil
}

func NewTupleResultFromBytes(data []byte) (*TupleResults, error) {
	rdr := bytes.NewReader(data)
	_, err := wireprotocol.ExpectToken(rdr, wireprotocol.TOKEN_SCHEMA_INFO)
	if err != nil {
		return nil, err
	}
	// get the row schema from the import data
	schema, err := wireprotocol.ReadSchema(rdr)
	if err != nil {
		return nil, err
	}

	// read rows until we get to the end
	tk, err := wireprotocol.ReadToken(rdr)
	if err != nil {
		return nil, err
	}
	rows := make([]types.Row, 0)
	for tk == wireprotocol.TOKEN_ROW {
		row, err := wireprotocol.ReadRow(rdr, schema)
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)

		tk, err = wireprotocol.ReadToken(rdr)
		if err != nil {
			return nil, err
		}
	}
	if tk != wireprotocol.TOKEN_DONE {
		return nil, errors.Errorf("unexpected token '%d'", tk)
	}
	return &TupleResults{TupleSchema: schema, rows: rows}, nil
}
