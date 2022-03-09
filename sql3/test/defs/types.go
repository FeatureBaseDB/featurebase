// Copyright 2021 Molecula Corp. All rights reserved.
package defs

import (
	"fmt"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/featurebasedb/featurebase/v3/sql3/parser"
	planner_types "github.com/featurebasedb/featurebase/v3/sql3/planner/types"
)

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
	CompareExactOrdered   compareMethod = "exactOrdered"
	CompareExactUnordered compareMethod = "exactUnordered"
	CompareIncludedIn     compareMethod = "includedIn"
)

type TableTest struct {
	name     string
	Table    source
	SQLTests []SQLTest
	PQLTests []PQLTest
}

// Name returns a string name which can be used to distingish test runs. It
// takes an integer value which will be used as part of a generic table name in
// the case where a name value was not provided in the definition.
func (tt TableTest) Name(i int) string {
	name := fmt.Sprintf("table-%d", i)
	if tt.name != "" {
		name = tt.name
	} else if tt.Table.name != "" {
		name = tt.Table.name
	}
	return name
}

// HasTable returns true is TableTest has a table defined which should be
// created.
func (tt TableTest) HasTable() bool {
	return tt.Table.columns != nil
}

// HasData returns true is TableTest has a data defined which should be
// inserted.
func (tt TableTest) HasData() bool {
	return len(tt.Table.rows) > 0
}

func (tt TableTest) CreateTable() string {
	if !tt.HasTable() {
		return ""
	}
	return tt.Table.createTable()
}

func (tt TableTest) InsertInto(t *testing.T) string {
	if !tt.HasTable() {
		return ""
	}
	return tt.Table.insertInto(t)
}

type SQLTest struct {
	name           string
	SQLs           []string
	ExpHdrs        []*planner_types.PlannerColumn
	ExpRows        [][]interface{}
	ExpErr         string
	Compare        compareMethod
	SortStringKeys bool
	ExpRowCount    int
}

// Name returns a string name which can be used to distingish test runs. It
// takes an integer value which will be used as part of a generic table name in
// the case where a name value was not provided in the definition.
func (s SQLTest) Name(i int) string {
	name := fmt.Sprintf("test-%d", i)
	if s.name != "" {
		name = s.name
	}
	return name
}

type PQLTest struct {
	name    string
	PQLs    []string
	Table   string
	ExpHdrs []*planner_types.PlannerColumn
	ExpRows [][]interface{}
	ExpErr  string
}

// Name returns a string name which can be used to distingish test runs. It
// takes an integer value which will be used as part of a generic table name in
// the case where a name value was not provided in the definition.
func (s PQLTest) Name(i int) string {
	name := fmt.Sprintf("test-%d", i)
	if s.name != "" {
		name = s.name
	}
	return name
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
