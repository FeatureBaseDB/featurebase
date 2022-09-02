// Copyright 2021 Molecula Corp. All rights reserved.
package sql3_test

import (
	"context"
	"fmt"
	"log"
	"strings"
	"testing"
	"time"

	pilosa "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/sql3/parser"
	planner_types "github.com/featurebasedb/featurebase/v3/sql3/planner/types"
	sql_test "github.com/featurebasedb/featurebase/v3/sql3/test"
	"github.com/featurebasedb/featurebase/v3/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQL_Execute(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()

	ctx := context.Background()
	api := c.GetNode(0).API
	svr := c.GetNode(0).Server

	for i, test := range tableTests {
		tableTestName := fmt.Sprintf("table-%d", i)
		if test.name != "" {
			tableTestName = test.name
		}
		t.Run(tableTestName, func(t *testing.T) {

			var err error
			// Create a table with all field types.
			if test.table.columns != nil {
				_, _, err := sql_test.MustQueryRows(t, svr, test.table.createTable())
				assert.NoError(t, err)
			}

			if len(test.table.rows) > 0 {

				// Populate fields with data.
				qcx := api.Txf().NewQcx()

				// idIdx is the index position of the _id column. If a source provides the
				// _id somewhere other than column 0, then we need to add logic here to find
				// its index.
				idIdx := 0
				for i, col := range test.table.columns {
					if col.name == "_id" {
						continue
					}

					colIDs := make([]uint64, 0)
					colKeys := make([]string, 0)

					addColID := func(v interface{}) {
						switch id := v.(type) {
						case uint64:
							colIDs = append(colIDs, id)
						case int64:
							colIDs = append(colIDs, uint64(id))
						case string:
							colKeys = append(colKeys, id)
						default:
							t.Fatalf("unexpected type for colid '%T'", v)
						}
					}

					switch col.typ.(type) {
					case *parser.DataTypeInt:
						vals := make([]int64, 0)
						for _, row := range test.table.rows {
							if row[i] == nil {
								continue
							}
							addColID(row[idIdx])
							vals = append(vals, row[i].(int64))
						}
						if len(vals) == 0 {
							continue
						}
						req := &pilosa.ImportValueRequest{
							Index:      test.table.name,
							Field:      col.name,
							Shard:      0,
							ColumnIDs:  colIDs,
							ColumnKeys: colKeys,
							Values:     vals,
						}

						err = api.ImportValue(ctx, qcx, req)
						assert.NoError(t, err)

					case *parser.DataTypeBool:
						vals := make([]uint64, 0)
						for _, row := range test.table.rows {
							if row[i] == nil {
								continue
							}
							addColID(row[idIdx])
							if row[i].(bool) {
								vals = append(vals, 1)
							} else {
								vals = append(vals, 0)
							}
						}
						if len(vals) == 0 {
							continue
						}
						req := &pilosa.ImportRequest{
							Index:      test.table.name,
							Field:      col.name,
							Shard:      0,
							ColumnIDs:  colIDs,
							ColumnKeys: colKeys,
							RowIDs:     vals,
						}

						err = api.Import(ctx, qcx, req)
						assert.NoError(t, err)

					case *parser.DataTypeDecimal:
						vals := make([]float64, 0)
						for _, row := range test.table.rows {
							if row[i] == nil {
								continue
							}
							addColID(row[idIdx])
							vals = append(vals, row[i].(float64))
						}
						if len(vals) == 0 {
							continue
						}
						req := &pilosa.ImportValueRequest{
							Index:       test.table.name,
							Field:       col.name,
							Shard:       0,
							ColumnIDs:   colIDs,
							ColumnKeys:  colKeys,
							FloatValues: vals,
						}

						err = api.ImportValue(ctx, qcx, req)
						assert.NoError(t, err)

					case *parser.DataTypeIDSet:
						rowIDs := make([]uint64, 0)
						for _, row := range test.table.rows {
							if row[i] == nil {
								continue
							}
							rowSet := row[i].([]int64)
							for k := range rowSet {
								addColID(row[idIdx])
								rowIDs = append(rowIDs, uint64(rowSet[k]))
							}
						}
						if len(rowIDs) == 0 {
							continue
						}
						req := &pilosa.ImportRequest{
							Index:      test.table.name,
							Field:      col.name,
							Shard:      0,
							ColumnIDs:  colIDs,
							ColumnKeys: colKeys,
							RowIDs:     rowIDs,
						}
						err = api.Import(ctx, qcx, req)
						assert.NoError(t, err)

					case *parser.DataTypeID:
						rowIDs := make([]uint64, 0)
						for _, row := range test.table.rows {
							if row[i] == nil {
								continue
							}
							addColID(row[idIdx])
							rowIDs = append(rowIDs, uint64(row[i].(int64)))
						}

						if len(rowIDs) == 0 {
							continue
						}
						req := &pilosa.ImportRequest{
							Index:      test.table.name,
							Field:      col.name,
							Shard:      0,
							ColumnIDs:  colIDs,
							ColumnKeys: colKeys,
							RowIDs:     rowIDs,
						}
						err = api.Import(ctx, qcx, req)
						assert.NoError(t, err)

					case *parser.DataTypeString:
						rowKeys := make([]string, 0)
						for _, row := range test.table.rows {
							if row[i] == nil {
								continue
							}
							addColID(row[idIdx])
							rowKeys = append(rowKeys, row[i].(string))
						}

						if len(rowKeys) == 0 {
							continue
						}
						req := &pilosa.ImportRequest{
							Index:      test.table.name,
							Field:      col.name,
							Shard:      0,
							ColumnIDs:  colIDs,
							ColumnKeys: colKeys,
							RowKeys:    rowKeys,
						}
						err = api.Import(ctx, qcx, req)
						assert.NoError(t, err)

					case *parser.DataTypeStringSet:
						rowKeys := make([]string, 0)
						for _, row := range test.table.rows {
							if row[i] == nil {
								continue
							}
							rowSet := row[i].([]string)
							for k := range rowSet {
								addColID(row[idIdx])
								rowKeys = append(rowKeys, rowSet[k])
							}
						}

						if len(rowKeys) == 0 {
							continue
						}
						req := &pilosa.ImportRequest{
							Index:      test.table.name,
							Field:      col.name,
							Shard:      0,
							ColumnIDs:  colIDs,
							ColumnKeys: colKeys,
							RowKeys:    rowKeys,
						}
						err = api.Import(ctx, qcx, req)
						assert.NoError(t, err)

					case *parser.DataTypeTimestamp:
						vals := make([]time.Time, 0)
						for _, row := range test.table.rows {
							if row[i] == nil {
								continue
							}
							addColID(row[idIdx])
							vals = append(vals, row[i].(time.Time))
						}
						if len(vals) == 0 {
							continue
						}
						req := &pilosa.ImportValueRequest{
							Index:           test.table.name,
							Field:           col.name,
							Shard:           0,
							ColumnIDs:       colIDs,
							ColumnKeys:      colKeys,
							TimestampValues: vals,
						}

						err = api.ImportValue(ctx, qcx, req)
						assert.NoError(t, err)

					default:
						t.Fatalf("column type not supported: %s", col.typ)
					}
				}
			}

			for i, sqltest := range test.sqlTests {
				sqlTestName := fmt.Sprintf("test-%d", i)
				if sqltest.name != "" {
					sqlTestName = sqltest.name
				}
				t.Run(sqlTestName, func(t *testing.T) {
					for _, sql := range sqltest.sqls {
						t.Run(fmt.Sprintf("sql-%s", sql), func(t *testing.T) {
							log.Printf("SQL: %s", sql)
							rows, headers, err := sql_test.MustQueryRows(t, svr, sql)

							// Check expected error instead of results.
							if sqltest.expErr != "" {
								if assert.Error(t, err) {
									assert.Contains(t, err.Error(), sqltest.expErr)
								}
								return
							}

							require.NoError(t, err)

							// Check headers.
							assert.ElementsMatch(t, sqltest.expHdrs, headers)

							// make a map of column name to header index
							m := make(map[string]int)
							for i := range headers {
								m[headers[i].Name] = i
							}

							// Put the expRows in the same column order as the headers returned
							// by the query.
							exp := make([][]interface{}, len(sqltest.expRows))
							for i := range sqltest.expRows {
								exp[i] = make([]interface{}, len(headers))
								for j := range sqltest.expHdrs {
									targetIdx := m[sqltest.expHdrs[j].Name]
									if !assert.GreaterOrEqual(t, len(sqltest.expRows[i]), len(headers)) {
										t.Fatalf("expected row set has fewer columns than returned headers")
									}
									exp[i][targetIdx] = sqltest.expRows[i][j]
								}
							}

							switch sqltest.compare {
							case compareExactOrdered:
								assert.EqualValues(t, len(sqltest.expRows), len(rows))
								assert.EqualValues(t, exp, rows)
							case compareExactUnordered:
								assert.EqualValues(t, len(sqltest.expRows), len(rows))
								assert.ElementsMatch(t, exp, rows)
							case compareIncludedIn:
								assert.EqualValues(t, sqltest.expRowCount, len(rows))
								for _, row := range rows {
									assert.Contains(t, exp, row)
								}
							}
						})
					}
				})
			}
		})
	}
}

//////////////////////////////////////////////////////////////////////

type fldType parser.ExprDataType

// fldType constants are providing a map of a defined test type to the
// parser.ExprDataType
var (
	fldTypeID        fldType = parser.NewDataTypeID()
	fldTypeBool      fldType = parser.NewDataTypeBool()
	fldTypeIDSet     fldType = parser.NewDataTypeIDSet()
	fldTypeInt       fldType = parser.NewDataTypeInt()
	fldTypeDecimal2  fldType = parser.NewDataTypeDecimal(2)
	fldTypeString    fldType = parser.NewDataTypeString()
	fldTypeStringSet fldType = parser.NewDataTypeStringSet()
	fldTypeTimestamp fldType = parser.NewDataTypeTimestamp()
)

type compareMethod string

const (
	compareExactOrdered   compareMethod = "exactOrdered"
	compareExactUnordered compareMethod = "exactUnordered"
	compareIncludedIn     compareMethod = "includedIn"
)

type tableTest struct {
	name     string
	table    source
	sqlTests []sqlTest
}

type sqlTest struct {
	name        string
	sqls        []string
	expHdrs     []*planner_types.PlannerColumn
	expRows     [][]interface{}
	expErr      string
	compare     compareMethod
	expRowCount int
}

// The following "source" types are helpers for creating a test table.
type sourceColumn struct {
	name    string
	typ     fldType
	options string
}

func tbl(name string, columns []sourceColumn, rows []sourceRow) source {
	return source{
		name:    name,
		columns: columns,
		rows:    rows,
	}
}

func srcHdrs(hdrs ...sourceColumn) []sourceColumn {
	return hdrs
}

func srcHdr(name string, typ fldType, opts ...string) sourceColumn {
	return sourceColumn{
		name:    name,
		typ:     typ,
		options: strings.Join(opts, " "),
	}
}

func srcRows(rows ...sourceRow) []sourceRow {
	return rows
}
func srcRow(cells ...interface{}) sourceRow {
	return cells
}

type sourceRow []interface{}

type source struct {
	name    string
	columns []sourceColumn
	rows    []sourceRow
}

func (s source) createTable() string {
	ct := "CREATE TABLE " + s.name + " ("

	cols := []string{}
	for _, col := range s.columns {
		f := col.name + " " + col.typ.TypeName()
		if col.options != "" {
			f += " " + col.options
		}
		cols = append(cols, f)
	}
	ct += strings.Join(cols, ",")

	ct += `)`

	log.Printf("CREATE: %s", ct)
	return ct
}

// hdrs is just a helper function to make the test definition look cleaner.
func hdrs(hdrs ...*planner_types.PlannerColumn) []*planner_types.PlannerColumn {
	return hdrs
}

// hdr is just a helper function to make the test definition look cleaner.
func hdr(name string, typ fldType) *planner_types.PlannerColumn {
	return &planner_types.PlannerColumn{
		Name: name,
		Type: typ,
	}
}

// row helpers for expected results
func rows(rows ...[]interface{}) [][]interface{} {
	return rows
}

func row(cells ...interface{}) []interface{} {
	return cells
}

func sqls(sqls ...string) []string {
	return sqls
}
