// Copyright 2021 Molecula Corp. All rights reserved.
package sql3_test

import (
	"fmt"
	"log"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/molecula/featurebase/v3/sql3/parser"
	planner_types "github.com/molecula/featurebase/v3/sql3/planner/types"
	sql_test "github.com/molecula/featurebase/v3/sql3/test"
	"github.com/molecula/featurebase/v3/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQL_Execute(t *testing.T) {
	c := test.MustRunCluster(t, 1)
	defer c.Close()

	svr := c.GetNode(0).Server

	for i, test := range tableTests {
		tableTestName := fmt.Sprintf("table-%d", i)
		if test.name != "" {
			tableTestName = test.name
		} else if test.table.name != "" {
			tableTestName = test.table.name
		}
		t.Run(tableTestName, func(t *testing.T) {

			// Create a table with all field types.
			if test.table.columns != nil {
				_, _, err := sql_test.MustQueryRows(t, svr, test.table.createTable())
				assert.NoError(t, err)
			}

			if len(test.table.rows) > 0 {
				// Populate fields with data.
				_, _, err := sql_test.MustQueryRows(t, svr, test.table.insertInto(t))
				assert.NoError(t, err)
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
								m[headers[i].ColumnName] = i
							}

							// Put the expRows in the same column order as the headers returned
							// by the query.
							exp := make([][]interface{}, len(sqltest.expRows))
							for i := range sqltest.expRows {
								exp[i] = make([]interface{}, len(headers))
								for j := range sqltest.expHdrs {
									targetIdx := m[sqltest.expHdrs[j].ColumnName]
									assert.GreaterOrEqual(t, len(sqltest.expRows[i]), len(headers),
										"expected row set has fewer columns than returned headers")
									exp[i][targetIdx] = sqltest.expRows[i][j]
								}
							}

							if sqltest.sortStringKeys {
								sortStringKeys(rows)
							}

							switch sqltest.compare {
							case compareExactOrdered:
								assert.Equal(t, len(sqltest.expRows), len(rows))
								assert.EqualValues(t, exp, rows)
							case compareExactUnordered:
								assert.Equal(t, len(sqltest.expRows), len(rows))
								assert.ElementsMatch(t, exp, rows)
							case compareIncludedIn:
								assert.Equal(t, sqltest.expRowCount, len(rows))
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

// sortStringKeys goes through an entire set of rows, and for any []string it
// finds, it orders the elements. This is obviously only useful in tests, and
// only in cases where we expect the elements to match, but we don't care what
// order they're in. It's basically the equivalent of assert.ElementsMatch(),
// but the way we use that on rows doesn't recurse down into the field values
// within each row.
func sortStringKeys(in [][]interface{}) {
	for i := range in {
		for j := range in[i] {
			switch v := in[i][j].(type) {
			case []string:
				sort.Strings(v)
			}
		}
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
	name           string
	sqls           []string
	expHdrs        []*planner_types.PlannerColumn
	expRows        [][]interface{}
	expErr         string
	compare        compareMethod
	sortStringKeys bool
	expRowCount    int
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

type sourceRows []sourceRow

// insertTuples returns the list of tuples (as a single string) to use as the
// VALUES value in an INSERT INTO statement.
func (sr sourceRows) insertTuples(t *testing.T) string {
	var afterFirstRow bool
	var sb strings.Builder
	for _, row := range sr {
		if afterFirstRow {
			sb.WriteString(",")
		}

		var afterFirstCell bool
		sb.WriteString("(")

		for _, cell := range row {
			if afterFirstCell {
				sb.WriteString(",")
			}
			switch v := cell.(type) {
			case string:
				sb.WriteString("'" + v + "'")
			case int64:
				sb.WriteString(fmt.Sprintf("%d", v))
			case float64:
				sb.WriteString(fmt.Sprintf("%.2f", v))
			case []int64:
				strs := make([]string, len(v))
				for i := range v {
					strs[i] = fmt.Sprintf("%d", v[i])
				}
				sb.WriteString("[" + strings.Join(strs, ",") + "]")
			case []string:
				if len(v) == 0 {
					sb.WriteString("[]")
				} else {
					sb.WriteString("['" + strings.Join(v, "','") + "']")
				}
			case bool:
				sb.WriteString(fmt.Sprintf("%v", v))
			case nil:
				sb.WriteString("null")
			case time.Time:
				sb.WriteString("'" + v.Format(time.RFC3339) + "'")

			default:
				t.Fatalf("unsupported cell type: %T", cell)
			}
			afterFirstCell = true
		}

		sb.WriteString(")")
		afterFirstRow = true
	}
	return sb.String()
}

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

func (s source) insertInto(t *testing.T) string {
	ii := "INSERT INTO " + s.name + " VALUES "
	ii += sourceRows(s.rows).insertTuples(t)
	return ii
}

// hdrs is just a helper function to make the test definition look cleaner.
func hdrs(hdrs ...*planner_types.PlannerColumn) []*planner_types.PlannerColumn {
	return hdrs
}

// hdr is just a helper function to make the test definition look cleaner.
func hdr(name string, typ fldType) *planner_types.PlannerColumn {
	return &planner_types.PlannerColumn{
		ColumnName: name,
		Type:       typ,
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
