// Copyright 2021 Molecula Corp. All rights reserved.
package defs

import (
	"fmt"
	"log"
	"strings"
	"testing"
	"time"

	featurebase "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/dax"
)

// fldType constants are providing a map of a defined test type to the
// featurebase.WireQueryField.
var (
	fldTypeID featurebase.WireQueryField = featurebase.WireQueryField{
		Type:     dax.BaseTypeID,
		BaseType: dax.BaseTypeID,
	}
	fldTypeBool featurebase.WireQueryField = featurebase.WireQueryField{
		Type:     dax.BaseTypeBool,
		BaseType: dax.BaseTypeBool,
	}
	fldTypeIDSet featurebase.WireQueryField = featurebase.WireQueryField{
		Type:     dax.BaseTypeIDSet,
		BaseType: dax.BaseTypeIDSet,
	}
	fldTypeInt featurebase.WireQueryField = featurebase.WireQueryField{
		Type:     dax.BaseTypeInt,
		BaseType: dax.BaseTypeInt,
	}
	fldTypeDecimal2 featurebase.WireQueryField = featurebase.WireQueryField{
		Type:     dax.BaseTypeDecimal + "(2)",
		BaseType: dax.BaseTypeDecimal,
		TypeInfo: map[string]interface{}{"scale": int64(2)},
	}
	fldTypeString featurebase.WireQueryField = featurebase.WireQueryField{
		Type:     dax.BaseTypeString,
		BaseType: dax.BaseTypeString,
	}
	fldTypeStringSet featurebase.WireQueryField = featurebase.WireQueryField{
		Type:     dax.BaseTypeStringSet,
		BaseType: dax.BaseTypeStringSet,
	}
	fldTypeTimestamp featurebase.WireQueryField = featurebase.WireQueryField{
		Type:     dax.BaseTypeTimestamp,
		BaseType: dax.BaseTypeTimestamp,
	}
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

func (tt TableTest) InsertInto(t *testing.T, rowSets ...int) string {
	if !tt.HasTable() {
		return ""
	}
	if len(rowSets) == 0 {
		rowSets = []int{0}
	}
	return tt.Table.insertInto(t, rowSets)
}

type PlanCheckFunc func([]byte) error

type SQLTest struct {
	name           string
	SQLs           []string
	ExpHdrs        []*featurebase.WireQueryField
	ExpRows        [][]interface{}
	ExpRowsPlus1   [][][]interface{}
	ExpErr         string
	Compare        compareMethod
	SortStringKeys bool
	ExpRowCount    int
	PlanCheck      PlanCheckFunc
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
	name         string
	PQLs         []string
	Table        string
	ExpHdrs      []*featurebase.WireQueryField
	ExpRows      [][]interface{}
	ExpRowsPlus1 [][][]interface{}
	ExpErr       string
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
	typ     featurebase.WireQueryField
	options string
}

func tbl(name string, columns []sourceColumn, rows ...[]sourceRow) source {
	return source{
		name:    name,
		columns: columns,
		rows:    rows,
	}
}

func srcHdrs(hdrs ...sourceColumn) []sourceColumn {
	return hdrs
}

func srcHdr(name string, typ featurebase.WireQueryField, opts ...string) sourceColumn {
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
	rows    [][]sourceRow
}

func (s source) createTable() string {
	ct := "CREATE TABLE " + s.name + " ("

	cols := []string{}
	for _, col := range s.columns {
		f := col.name + " " + col.typ.Type
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

func (s source) insertInto(t *testing.T, rowSets []int) string {
	ii := "INSERT INTO " + s.name + " VALUES "
	for _, rowSet := range rowSets {
		ii += sourceRows(s.rows[rowSet]).insertTuples(t)
	}
	log.Printf("INSERT: %s", ii)
	return ii
}

// hdrs is just a helper function to make the test definition look cleaner.
func hdrs(hdrs ...*featurebase.WireQueryField) []*featurebase.WireQueryField {
	return hdrs
}

// hdr is just a helper function to make the test definition look cleaner. It
// applies the `name` value to the provided WireQueryType.
func hdr(name string, typ featurebase.WireQueryField) *featurebase.WireQueryField {
	return &featurebase.WireQueryField{
		Name:     dax.FieldName(name),
		Type:     typ.Type,
		BaseType: typ.BaseType,
		TypeInfo: typ.TypeInfo,
	}
}

// row helpers for expected results
func rows(rows ...[]interface{}) [][]interface{} {
	return rows
}

func rowSets(rowSets ...[][]interface{}) [][][]interface{} {
	return rowSets
}

func row(cells ...interface{}) []interface{} {
	return cells
}

func sqls(sqls ...string) []string {
	return sqls
}
