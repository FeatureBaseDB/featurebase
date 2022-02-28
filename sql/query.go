// Copyright 2021 Molecula Corp. All rights reserved.
package sql

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
)

const timeFormat = "2006-01-02T15:04"

// LT creates a less than query.
func LT(fieldName string, value interface{}) string {
	return fmt.Sprintf("Row(%s<%s)", fieldName, formatValue(value))
}

// LTE creates a less than or equal query.
func LTE(fieldName string, value interface{}) string {
	return fmt.Sprintf("Row(%s<=%s)", fieldName, formatValue(value))
}

// GT creates a greater than query.
func GT(fieldName string, value interface{}) string {
	return fmt.Sprintf("Row(%s>%s)", fieldName, formatValue(value))
}

// GTE creates a greater than or equal query.
func GTE(fieldName string, value interface{}) string {
	return fmt.Sprintf("Row(%s>=%s)", fieldName, formatValue(value))
}

// Equals creates an equals query.
func Equals(fieldName string, value interface{}) string {
	return fmt.Sprintf("Row(%s=%s)", fieldName, formatValue(value))
}

// NotEquals creates a not equals query.
func NotEquals(fieldName string, value interface{}) string {
	return fmt.Sprintf("Row(%s!=%s)", fieldName, formatValue(value))
}

// NotNull creates a not equal to null query.
func NotNull(fieldName string) string {
	return fmt.Sprintf("Row(%s!=null)", fieldName)
}

// Row query
func Row(fieldName string, rowIDOrKey interface{}) (string, error) {
	rowStr, err := formatIDKeyBool(rowIDOrKey)
	if err != nil {
		return "", err
	}
	text := fmt.Sprintf("Row(%s=%s)", fieldName, rowStr)
	return text, nil
}

// RowRange is a Row query with from,to times
func RowRange(fieldName string, rowIDOrKey interface{}, start time.Time, end time.Time) (string, error) {
	rowStr, err := formatIDKeyBool(rowIDOrKey)
	if err != nil {
		return "", err
	}
	text := fmt.Sprintf("Row(%s=%s,from='%s',to='%s')", fieldName, rowStr, start.Format(timeFormat), end.Format(timeFormat))
	return text, nil
}

// Union query - see rowOperation
func Union(rows ...string) string {
	return rowOperation("Union", rows...)
}

// Intersect query - see rowOperation
func Intersect(rows ...string) string {
	return rowOperation("Intersect", rows...)
}

// Not query
func Not(rows ...string) string {
	return rowOperation("Not", rows...)
}

// Like creates a Rows query filtered by a pattern.
// An underscore ('_') can be used as a placeholder for a single UTF-8 codepoint or a percent sign ('%') can be used as a placeholder for 0 or more codepoints.
// All other codepoints in the pattern are matched exactly.
func Like(fieldName string, pattern string) string {
	pattern = strings.ReplaceAll(pattern, `\`, `\\`)
	pattern = strings.ReplaceAll(pattern, `'`, `\'`)
	return fmt.Sprintf("UnionRows(Rows(field='%s',like='%s'))", fieldName, pattern)
}

// Between creates a between query.
func Between(fieldName string, a interface{}, b interface{}) string {
	return fmt.Sprintf("Row(%s >< [%s,%s])", fieldName, formatValue(a), formatValue(b))
}

// Distinct creates a Distinct query.
func Distinct(indexName, fieldName string) string {
	return fmt.Sprintf("Distinct(Row(%s!=null),index='%s',field='%s')", fieldName, indexName, fieldName)
}

// RowDistinct creates a Distinct query with the given row filter.
func RowDistinct(indexName, fieldName string, row string) string {
	return fmt.Sprintf("Distinct(%s,index='%s',field='%s')", row, indexName, fieldName)
}

// Rows creates a Rows query with defaults
func Rows(fieldName string) string {
	return fmt.Sprintf("Rows(field='%s')", fieldName)
}

// RowsLimit creates a Rows query with the given limit
func RowsLimit(fieldName string, limit int64) (string, error) {
	if limit < 0 {
		return "", errors.New("rows limit must be non-negative")
	}
	text := fmt.Sprintf("Rows(field='%s',limit=%d)", fieldName, limit)
	return text, nil
}

// All creates an All query.
// Returns the set columns with existence true.
func All() string {
	return "All()"
}

// Count creates a Count query.
// Returns the number of set columns in the ROW_CALL passed in.
func Count(rowCall string) string {
	return fmt.Sprintf("Count(%s)", rowCall)
}

// Sum creates a sum query.
func Sum(fieldName string, row string) string {
	return valQuery(fieldName, "Sum", row)
}

// Min creates a min query.
func Min(fieldName string, row string) string {
	return valQuery(fieldName, "Min", row)
}

// Max creates a max query.
func Max(fieldName string, row string) string {
	return valQuery(fieldName, "Max", row)
}

// TopN creates a TopN query with the given item count.
// Returns the id and count of the top n rows (by count of columns) in the field.
func TopN(fieldName string, n uint64) string {
	return fmt.Sprintf("TopN(%s,n=%d)", fieldName, n)
}

// RowTopN creates a TopN query with the given item count and row.
// This variant supports customizing the row query.
func RowTopN(fieldName string, n uint64, row string) string {
	return fmt.Sprintf("TopN(%s,%s,n=%d)", fieldName, row, n)
}

// GroupByBase creates a GroupBy query with the given functional options.
func GroupByBase(rows []string, limit int64, filter, aggregate, having string) (string, error) {
	if len(rows) == 0 {
		return "", errors.New("there should be at least one rows query")
	}
	if limit < 0 {
		return "", errors.New("limit must be non-negative")
	}

	// rows
	text := fmt.Sprintf("GroupBy(%s", strings.Join(rows, ","))

	// limit
	if limit > 0 {
		text += fmt.Sprintf(",limit=%d", limit)
	}

	// filter
	if filter != "" {
		text += fmt.Sprintf(",filter=%s", filter)
	}

	// aggregate
	if aggregate != "" {
		text += fmt.Sprintf(",aggregate=%s", aggregate)
	}

	// having
	if having != "" {
		text += fmt.Sprintf(",having=%s", having)
	}

	text += ")"
	return text, nil
}

// Limit creates a limit query.
func Limit(row string, limit uint, offset uint) string {
	return fmt.Sprintf("Limit(%s, limit=%d, offset=%d)", row, limit, offset)
}

// Offset creates a limit query but only with an offset.
func Offset(row string, offset uint) string {
	return fmt.Sprintf("Limit(%s, offset=%d)", row, offset)
}

// ConstRow creates a query value that uses a list of columns in place of a Row query.
func ConstRow(ids ...interface{}) string {
	if ids == nil {
		ids = []interface{}{}
	}
	data, _ := json.Marshal(ids)
	return fmt.Sprintf("ConstRow(columns=%s)", data)
}

// Extract creates an Extract query.
// It accepts a bitmap query to select columns and a list of fields to select rows.
func Extract(rowCall string, fields ...string) string {
	var rowsCall string
	for _, r := range fields {
		rowsCall += "," + fmt.Sprintf("Rows(%s)", r)
	}

	return fmt.Sprintf("Extract(%s%s)", rowCall, rowsCall)
}

func valQuery(fieldName string, op string, row string) string {
	if row != "" {
		row += ","
	}
	return fmt.Sprintf("%s(%sfield='%s')", op, row, fieldName)
}

func rowOperation(name string, rows ...string) string {
	return fmt.Sprintf("%s(%s)", name, strings.Join(rows, ","))
}

func formatIDKeyBool(idKeyBool interface{}) (string, error) {
	if b, ok := idKeyBool.(bool); ok {
		return strconv.FormatBool(b), nil
	}
	if flt, ok := idKeyBool.(float64); ok {
		return fmt.Sprintf("%f", flt), nil
	}
	return formatIDKey(idKeyBool)
}

func formatIDKey(idKey interface{}) (string, error) {
	switch v := idKey.(type) {
	case uint:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint32:
		return strconv.FormatUint(uint64(v), 10), nil
	case uint64:
		return strconv.FormatUint(v, 10), nil
	case int:
		return strconv.FormatInt(int64(v), 10), nil
	case int32:
		return strconv.FormatInt(int64(v), 10), nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case string:
		v = strings.ReplaceAll(v, `\`, `\\`)
		return fmt.Sprintf(`'%s'`, strings.ReplaceAll(v, `'`, `\'`)), nil
	default:
		return "", errors.Errorf("id/key is not a string or integer type: %#v", idKey)
	}
}

func formatValue(value interface{}) string {
	switch value.(type) {
	case string:
		return fmt.Sprintf("%q", value)
	case float64, float32:
		// In order to test expected values, we set the precision
		// to 8. TODO: It's likely we'll need to address this
		// at some point.
		return fmt.Sprintf("%.8f", value)
	default:
		return fmt.Sprintf("%d", value)
	}
}
