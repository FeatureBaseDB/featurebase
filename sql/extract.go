// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package sql

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	pilosa "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/pql"
	"github.com/pkg/errors"
	"vitess.io/vitess/go/vt/sqlparser"
)

// parseColumn is a column parsed from a sql query. Its qualifier
// value should map to either the name or alias of a parseTable.
type parseColumn struct {
	name      string
	qualifier string
}

// parseTable is a table parsed from a sql query. It includes
// its name and alias, along with a boolean indicating whether
// the table is the primary side of a join statement (i.e. it
// refers to the Pilosa column _id).
type parseTable struct {
	name    string
	alias   string
	primary bool          // indicates the side of the join representing the column _id
	column  *parseColumn  // contains the related column from the ON clause
	index   *pilosa.Index // the pilosa index related to this table
}

// parseTables is a slice of parseTable parsed from a single sql query.
type parseTables []*parseTable

// byName returns the parseTable from the slice which matches on name.
// If there is no match it returns nil.
func (j parseTables) byName(n string) *parseTable {
	for i := range j {
		if j[i].name == n {
			return j[i]
		}
	}
	return nil
}

// byName returns the parseTable from the slice which matches on alias.
// If there is no match it returns nil.
func (j parseTables) byAlias(a string) *parseTable {
	for i := range j {
		if j[i].alias == a {
			return j[i]
		}
	}
	return nil
}

// primary returns the primary parseTable from the slice.
// In order for this to be useful, it is assumed that
// joinTables contains exactly two joinTable pointers
// (a primary and a secondary).
func (j parseTables) primary() *parseTable {
	for i := range j {
		if j[i].primary {
			return j[i]
		}
	}
	return nil
}

// secondary returns the secondary parseTable from the slice.
func (j parseTables) secondary() *parseTable {
	for i := range j {
		if !j[i].primary {
			return j[i]
		}
	}
	return nil
}

// tableWhere represents a parseTable from a sql query along
// with the portion of the where clause that relates to
// that table. For example, if a sql query had:
//
//	from tbl1, tbl2
//	where tbl1.field1=1 and tbl2.field2=2
//
// then each table would have a separate tableWhere object
// with the where made up of only the field with matching qualifier.
type tableWhere struct {
	table *parseTable
	where string
}

// tableWheres is a slice of tableWhere.
type tableWheres []*tableWhere

// extractParseTable returns a parseTable for the sqlparser.TableExpr.
func extractParseTable(tableExpr sqlparser.TableExpr) (*parseTable, error) {
	switch tbl := tableExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		tableName := tbl.Expr.(sqlparser.TableName).ToViewName().Name.String()
		alias := tbl.As.String()
		if alias == "" {
			alias = tableName
		}
		return &parseTable{
			name:  tableName,
			alias: alias,
		}, nil
	}

	return nil, errors.New("unsupported table expression")
}

func extractSelectFields(index *pilosa.Index, stmt *sqlparser.Select) ([]Column, selectFeatures, error) {
	columns := []Column{}
	features := selectFeatures{}
	for _, item := range stmt.SelectExprs {
		switch expr := item.(type) {
		case *sqlparser.AliasedExpr:
			var column Column
			var alias = expr.As.String()
			switch colExpr := expr.Expr.(type) {
			case *sqlparser.ColName:
				fieldName := colExpr.Name.String()

				if fieldName == ColID {
					if index.Options().Keys {
						column = NewKeyIndexColumn(index, alias)
					} else {
						column = NewIDIndexColumn(index, alias)
					}
				} else {
					field := index.Field(fieldName)
					if field == nil {
						return nil, features, errors.Wrapf(pilosa.ErrFieldNotFound, "field %s", fieldName)
					}
					column = NewFieldColumn(field, alias)
				}
			case *sqlparser.FuncExpr:
				funcName := FuncName(strings.ToLower(colExpr.Name.String()))
				var field *pilosa.Field

				if len(colExpr.Exprs) != 1 {
					return nil, features, errors.New("function should have a single argument")
				}

				switch expr := colExpr.Exprs[0].(type) {
				case *sqlparser.AliasedExpr:
					if colExpr, ok := expr.Expr.(*sqlparser.ColName); ok {
						fieldName := colExpr.Name.String()
						field = index.Field(fieldName)
						if field == nil {
							return nil, features, errors.Wrapf(pilosa.ErrFieldNotFound, "field %s", fieldName)
						}
					} else {
						return nil, features, errors.New("table name is required")
					}
				case *sqlparser.StarExpr:
					// We don't currently track this; it either has a field or doesn't.
				default:
					return nil, features, errors.New("table name is required")
				}

				switch funcName {
				case FuncCount, FuncMin, FuncMax, FuncSum, FuncAvg:
					column = NewFuncColumn(funcName, field, alias)
				default:
					return nil, features, fmt.Errorf("unknown function: %s", funcName)
				}
				features.funcs = append(features.funcs, selectFunc{
					funcName: funcName,
					field:    field,
				})
			default:
				return nil, features, errors.New("table name is required")
			}
			columns = append(columns, column)
		case *sqlparser.StarExpr:
			columns = append(columns, NewStarColumn())
		default:
			return nil, features, errors.New("only column names or * are supported in select")
		}
	}
	return columns, features, nil
}

func extractIndexName(stmt *sqlparser.Select) (string, error) {
	if len(stmt.From) != 1 {
		return "", errors.New("selecting from multiple tables is not supported")
	}

	fromExpr := stmt.From[0]
	switch from := fromExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		indexName := from.Expr.(sqlparser.TableName).ToViewName().Name.String()
		return indexName, nil
	}

	return "", errors.New("unsupported from clause")
}

// extractParseColumn returns a parseColumn for the sqlparser.ColName.
func extractParseColumn(col *sqlparser.ColName) (*parseColumn, error) {
	colName := col.Name.String()
	qualifier := col.Qualifier.ToViewName().Name.String()

	return &parseColumn{
		name:      colName,
		qualifier: qualifier,
	}, nil
}

func extractWhere(index *pilosa.Index, expr sqlparser.Expr) (string, error) {
	switch e := expr.(type) {
	case *sqlparser.ComparisonExpr:
		parseCol, op, val, err := extractComparison(e)
		if err != nil {
			return "", err
		}

		if parseCol.name == "_id" {
			switch op {
			case "=":
				return ConstRow(val), nil
			case "in":
				switch valExpr := val.(type) {
				case []interface{}:
					return ConstRow(valExpr...), nil
				}
			}
		}

		field := index.Field(parseCol.name)
		if field == nil {
			return "", errors.Wrap(pilosa.ErrFieldNotFound, parseCol.name)
		}

		switch field.Type() {
		case pilosa.FieldTypeInt, pilosa.FieldTypeDecimal, pilosa.FieldTypeTimestamp:
			switch op {
			case "=":
				return Equals(field.Name(), val), nil
			case "<":
				return LT(field.Name(), val), nil
			case "<=":
				return LTE(field.Name(), val), nil
			case ">":
				return GT(field.Name(), val), nil
			case ">=":
				return GTE(field.Name(), val), nil
			case "<>", "!=":
				return NotEquals(field.Name(), val), nil
			}
		default:
			switch op {
			case "=":
				return Row(field.Name(), val)
			case "in":
				var qs []string
				switch valExpr := val.(type) {
				case []interface{}:
					for _, v := range valExpr {
						q, err := Row(field.Name(), v)
						if err != nil {
							return "", err
						}
						qs = append(qs, q)
					}
					return Union(qs...), nil
				default:
					return "", fmt.Errorf("in operator expects `[]interface{}` but got: %T", valExpr)
				}
			case "like":
				sval, ok := val.(string)
				if !ok {
					return "", fmt.Errorf("like operator expects `string` but got: %T", val)
				}
				return Like(field.Name(), sval), nil
			}
		}
	case *sqlparser.AndExpr:
		pql, err := extractWhereDateRange(index, e.Left, e.Right)
		if err == nil {
			return pql, err
		}
		left, err := extractWhere(index, e.Left)
		if err != nil {
			return "", err
		}
		right, err := extractWhere(index, e.Right)
		if err != nil {
			return "", err
		}
		return Intersect(left, right), nil
	case *sqlparser.OrExpr:
		left, err := extractWhere(index, e.Left)
		if err != nil {
			return "", err
		}
		right, err := extractWhere(index, e.Right)
		if err != nil {
			return "", err
		}
		return Union(left, right), nil
	case *sqlparser.NotExpr:
		expr, err := extractWhere(index, e.Expr)
		if err != nil {
			return "", err
		}
		return Not(expr), nil
	case *sqlparser.ParenExpr:
		expr, err := extractWhere(index, e.Expr)
		if err != nil {
			return "", err
		}
		return expr, nil
	case *sqlparser.RangeCond:
		if e.Operator != "between" {
			return "", errors.New("only between is supported")
		}
		left, ok := e.Left.(*sqlparser.ColName)
		if !ok {
			return "", errors.New("left operand must be a column name")
		}
		columnName := left.Name.String()
		fieldName, isSpecial := ExtractFieldName(columnName)
		if isSpecial {
			return "", errors.New("special fields are not allowed here")
		}
		field := index.Field(fieldName)

		switch field.Type() {
		case pilosa.FieldTypeInt:
			fromNum, err := extractInt(e.From)
			if err != nil {
				return "", err
			}
			toNum, err := extractInt(e.To)
			if err != nil {
				return "", err
			}
			return Between(field.Name(), fromNum, toNum), nil
		case pilosa.FieldTypeDecimal:
			fromNum, err := extractFloat(e.From)
			if err != nil {
				return "", err
			}
			toNum, err := extractFloat(e.To)
			if err != nil {
				return "", err
			}
			return Between(field.Name(), fromNum, toNum), nil
		case pilosa.FieldTypeTimestamp:
			fromTime, err := extractTimestamp(e.From)
			if err != nil {
				return "", err
			}
			toTime, err := extractTimestamp(e.To)
			if err != nil {
				return "", err
			}
			return Between(field.Name(), fromTime, toTime), nil
		default:
			return "", errors.New("only int and float64 fields are supported")
		}
	case *sqlparser.IsExpr:
		left, ok := e.Expr.(*sqlparser.ColName)
		if !ok {
			return "", errors.New("left operand must be a column name")
		}
		field := index.Field(left.Name.String())
		if field.Type() == pilosa.FieldTypeInt || field.Type() == pilosa.FieldTypeTimestamp {
			if e.Operator == "is not null" {
				return NotNull(field.Name()), nil
			}
			return "", fmt.Errorf("only `is not null` is supported for %s fields", field.Type())
		}
		return "", fmt.Errorf("`is` expression is supported only for %s fields", field.Type())
	}
	return "", errors.New("cannot extract where")
}

func extractWhereDateRange(index *pilosa.Index, leftExpr sqlparser.Expr, rightExpr sqlparser.Expr) (string, error) {
	var fieldName string
	var fromExpr sqlparser.Expr
	var toExpr sqlparser.Expr
	var isSpecial bool
	var idOrKey interface{}

	// Where part is expected to be in one of the following forms:
	// where FIELD=VALUE and FIELD between DATETIME_FORMAT and DATETIME_FORMAT
	// Or:
	// where FIELD between DATETIME_FORMAT and DATETIME_FORMAT and FIELD=VALUE

	extract := func(compExpr *sqlparser.ComparisonExpr, rangeCond *sqlparser.RangeCond) error {
		parseCol, op, val, err := extractComparison(compExpr)
		if err != nil {
			return err
		}
		idOrKey = val
		if op != "=" {
			// only = operator can exist here.
			return errors.New("only = operator can exist here")
		}
		fieldName, isSpecial = ExtractFieldName(parseCol.name)
		if isSpecial {
			// column name cannot be special here.
			return errors.New("column name cannot be special here")
		}
		if rangeCond.Operator != "between" {
			// only between is accepted at this point.
			return errors.New("only between is accepted at this point")
		}
		condFieldName, _ := ExtractFieldName(rangeCond.Left.(*sqlparser.ColName).Name.String())
		if condFieldName != fieldName {
			// the field names in both sides should be the same, otherwise reject.
			return errors.New("the field names in both sides should be the same, otherwise reject")
		}
		fromExpr = rangeCond.From
		toExpr = rangeCond.To
		return nil
	}

	if left, ok := leftExpr.(*sqlparser.ComparisonExpr); ok {
		// if FIELD=VALUE part is on the left, and range part is on the right
		if right, ok := rightExpr.(*sqlparser.RangeCond); ok {
			if err := extract(left, right); err != nil {
				return "", err
			}
		} else {
			return "", errors.New("right is not a range cond")
		}
	} else if right, ok := rightExpr.(*sqlparser.ComparisonExpr); ok {
		// if FIELD=VALUE part is on the right, and left part is on the left
		if left, ok := leftExpr.(*sqlparser.RangeCond); ok {
			if err := extract(right, left); err != nil {
				return "", err
			}
		} else {
			return "", errors.New("left is not a range cond")
		}
	}

	fromStr, err := extractStr(fromExpr)
	if err != nil {
		return "", err
	}
	toStr, err := extractStr(toExpr)
	if err != nil {
		return "", err
	}
	// try to convert `from` and `to` to time
	fromTime, ok := ConvertToTime(fromStr)
	if !ok {
		return "", errors.New("from operand must be in the correct time format")
	}
	toTime, ok := ConvertToTime(toStr)
	if !ok {
		return "", errors.New("to operand must be in the correct time format")
	}

	field := index.Field(fieldName)
	return RowRange(field.Name(), idOrKey, fromTime, toTime)
}

func extractVal(e sqlparser.Expr) (interface{}, error) {
	val, ok := e.(*sqlparser.SQLVal)
	if !ok {
		return nil, errors.New("expression must be a value")
	}
	switch val.Type {
	case sqlparser.StrVal:
		return string(val.Val), nil
	case sqlparser.IntVal:
		value, err := strconv.Atoi(string(val.Val))
		if err != nil {
			return nil, err
		}
		return value, nil
	case sqlparser.FloatVal:
		value, err := strconv.ParseFloat(string(val.Val), 64)
		if err != nil {
			return nil, err
		}
		return value, nil
	default:
		return nil, fmt.Errorf("unknown type: %d", val.Type)
	}
}

func extractInt(e sqlparser.Expr) (int, error) {
	val, err := extractVal(e)
	if err != nil {
		return 0, err
	}
	num, ok := val.(int)
	if !ok {
		return 0, errors.New("value must be an integer")
	}
	return num, nil
}

func extractFloat(e sqlparser.Expr) (float64, error) {
	val, err := extractVal(e)
	if err != nil {
		return 0, err
	}

	var num float64
	switch v := val.(type) {
	case int:
		num = float64(v)
	case float64:
		num = v
	default:
		return 0, errors.New("value must be convertable to a float64")
	}
	return num, nil
}

func extractTimestamp(e sqlparser.Expr) (time.Time, error) {
	val, err := extractVal(e)
	if err != nil {
		return time.Time{}, err
	}
	s, ok := val.(string)
	if !ok {
		return time.Time{}, errors.New("value must be an ISO 8601-formated timestamp string")
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, errors.New("value must be an ISO 8601-formated timestamp string")
	}
	return t, nil
}

func extractStr(e sqlparser.Expr) (string, error) {
	val, err := extractVal(e)
	if err != nil {
		return "", err
	}
	s, ok := val.(string)
	if !ok {
		return "", errors.New("value must be a string")
	}
	return s, nil
}

func extractTuple(tuple sqlparser.ValTuple) ([]interface{}, error) {
	result := []interface{}{}
	for _, item := range tuple {
		if val, ok := item.(*sqlparser.SQLVal); ok {
			v, err := extractVal(val)
			if err != nil {
				return nil, err
			}
			result = append(result, v)
		} else {
			return nil, errors.New("tuple should contain integers or strings")
		}
	}
	return result, nil
}

func extractComparison(expr *sqlparser.ComparisonExpr) (col *parseColumn, op string, value interface{}, err error) {
	op = expr.Operator
	if colExpr, ok := expr.Left.(*sqlparser.ColName); ok {
		col, err = extractParseColumn(colExpr)
		if err != nil {
			return
		}
		if op == "in" {
			switch valExpr := expr.Right.(type) {
			case sqlparser.ValTuple:
				value, err = extractTuple(valExpr)

			default:
				err = fmt.Errorf("in operator excepts only a tuple or a query, received `%s`",
					reflect.TypeOf(valExpr).String())
				return
			}
		} else {
			value, err = extractVal(expr.Right)
		}
		if err != nil {
			return
		}
	} else {
		if colExpr, ok := expr.Right.(*sqlparser.ColName); ok {
			col, err = extractParseColumn(colExpr)
			if err != nil {
				return
			}
			if op == "in" {
				value, err = extractTuple(expr.Left.(sqlparser.ValTuple))
			} else {
				value, err = extractVal(expr.Left)
			}
			if err != nil {
				return
			}
		} else {
			err = errors.New("either left or right operand should be a column name")
		}
	}
	return
}

func extractLimitOffset(stmt *sqlparser.Select) (limit uint, offset uint, hasLimit bool, hasOffset bool, err error) {
	if stmt.Limit == nil {
		return 0, 0, false, false, nil
	}
	if offsetExpr, ok := stmt.Limit.Offset.(*sqlparser.SQLVal); ok {
		val, err := extractVal(offsetExpr)
		if err != nil {
			return 0, 0, false, false, err
		}
		if offsetVal, ok := val.(int); ok {
			offset = uint(offsetVal)
		} else {
			return 0, 0, false, false, errors.New("offset must be an integer")
		}
		hasOffset = true
	}
	if limitExpr, ok := stmt.Limit.Rowcount.(*sqlparser.SQLVal); ok {
		val, err := extractVal(limitExpr)
		if err != nil {
			return 0, 0, false, false, err
		}
		if limitVal, ok := val.(int); ok {
			limit = uint(limitVal)
		} else {
			return 0, 0, false, false, errors.New("limit must be an integer")
		}
		hasLimit = true
	}
	return limit, offset, hasLimit, hasOffset, nil
}

// extractOrderBy returns the order by fields and directions (asc/desc)
// as separate string slices.
func extractOrderBy(stmt *sqlparser.Select) ([]string, []string, error) {
	var flds []string
	var dirs []string

	for _, item := range stmt.OrderBy {
		switch colExpr := item.Expr.(type) {
		case *sqlparser.ColName:
			colName := colExpr.Name.String()
			flds = append(flds, colName)
			dirs = append(dirs, item.Direction)
		}
	}

	return flds, dirs, nil
}

func extractTableNames(stmt *sqlparser.Select) ([]string, error) {
	if len(stmt.From) == 0 {
		return []string{}, nil
	}

	switch from := stmt.From[0].(type) {
	case *sqlparser.AliasedTableExpr:
		tableName := from.Expr.(sqlparser.TableName).ToViewName().Name.String()
		return []string{tableName}, nil
	case *sqlparser.JoinTableExpr:
		ret := []string{}
		switch left := from.LeftExpr.(type) {
		case *sqlparser.AliasedTableExpr:
			leftTableName := left.Expr.(sqlparser.TableName).ToViewName().Name.String()
			ret = append(ret, leftTableName)
		}
		switch right := from.RightExpr.(type) {
		case *sqlparser.AliasedTableExpr:
			rightTableName := right.Expr.(sqlparser.TableName).ToViewName().Name.String()
			ret = append(ret, rightTableName)
		}
		return ret, nil
	}

	return []string{}, nil
}

func extractGroupByFieldNames(stmt sqlparser.GroupBy) ([]string, error) {
	fields := make([]string, len(stmt))
	for i, item := range stmt {
		col, ok := item.(*sqlparser.ColName)
		if !ok {
			return nil, errors.New("group by accepts columns")
		}
		fields[i] = col.Name.String()
	}
	return fields, nil
}

func extractHavingClause(stmt *sqlparser.Where) (*HavingClause, error) {
	if stmt == nil {
		return nil, nil
	}
	if stmt.Type != "having" {
		return nil, fmt.Errorf("invalid having type: %s", stmt.Type)
	}

	hc := &HavingClause{}

	switch having := stmt.Expr.(type) {
	case *sqlparser.RangeCond:
		if having.Operator != "between" {
			return nil, errors.New("only between is supported")
		}
		hc.Subj = having.Left.(*sqlparser.ColName).Name.String()
		hc.Cond.Op = pql.BETWEEN
		fromPred, err := extractInt(having.From)
		if err != nil {
			return nil, err
		}
		toPred, err := extractInt(having.To)
		if err != nil {
			return nil, err
		}
		vals := make([]interface{}, 2)
		switch hc.Subj {
		case "count":
			vals[0] = uint64(fromPred)
			vals[1] = uint64(toPred)
		case "sum":
			vals[0] = int64(fromPred)
			vals[1] = int64(toPred)
		}
		hc.Cond.Value = vals
		return hc, nil
	case *sqlparser.AndExpr:
		left := having.Left.(*sqlparser.ComparisonExpr)
		right := having.Right.(*sqlparser.ComparisonExpr)
		leftName := left.Left.(*sqlparser.ColName).Name.String()
		rightName := right.Left.(*sqlparser.ColName).Name.String()
		if leftName != rightName {
			return nil, fmt.Errorf("having comparitors do not match: %s, %s", leftName, rightName)
		}
		hc.Subj = leftName

		leftOp := extractComparisonOp(left)
		rightOp := extractComparisonOp(right)

		leftPred, err := extractInt(left.Right)
		if err != nil {
			return nil, err
		}
		rightPred, err := extractInt(right.Right)
		if err != nil {
			return nil, err
		}

		intVals := make([]int, 2)
		if leftOp == pql.GT && rightOp == pql.LT {
			intVals[0] = leftPred
			intVals[1] = rightPred
			hc.Cond.Op = pql.BTWN_LT_LT
		} else if leftOp == pql.GT && rightOp == pql.LTE {
			intVals[0] = leftPred
			intVals[1] = rightPred
			hc.Cond.Op = pql.BTWN_LT_LTE
		} else if leftOp == pql.GTE && rightOp == pql.LT {
			intVals[0] = leftPred
			intVals[1] = rightPred
			hc.Cond.Op = pql.BTWN_LTE_LT
		} else if leftOp == pql.GTE && rightOp == pql.LTE {
			intVals[0] = leftPred
			intVals[1] = rightPred
			hc.Cond.Op = pql.BETWEEN
		} else if leftOp == pql.LT && rightOp == pql.GT {
			intVals[0] = rightPred
			intVals[1] = leftPred
			hc.Cond.Op = pql.BTWN_LT_LT
		} else if leftOp == pql.LT && rightOp == pql.GTE {
			intVals[0] = rightPred
			intVals[1] = leftPred
			hc.Cond.Op = pql.BTWN_LTE_LT
		} else if leftOp == pql.LTE && rightOp == pql.GT {
			intVals[0] = rightPred
			intVals[1] = leftPred
			hc.Cond.Op = pql.BTWN_LT_LTE
		} else if leftOp == pql.LTE && rightOp == pql.GTE {
			intVals[0] = rightPred
			intVals[1] = leftPred
			hc.Cond.Op = pql.BETWEEN
		}

		vals := make([]interface{}, 2)
		switch hc.Subj {
		case "count":
			vals[0] = uint64(intVals[0])
			vals[1] = uint64(intVals[1])
		case "sum":
			vals[0] = int64(intVals[0])
			vals[1] = int64(intVals[1])
		}
		hc.Cond.Value = vals

		return hc, nil
	case *sqlparser.ComparisonExpr:
		hc.Subj = having.Left.(*sqlparser.ColName).Name.String()
		hc.Cond.Op = extractComparisonOp(having)
		switch hc.Subj {
		case "count":
			pred, err := extractInt(having.Right)
			if err != nil {
				return nil, err
			}
			hc.Cond.Value = uint64(pred)
		case "sum":
			pred, err := extractInt(having.Right)
			if err != nil {
				return nil, err
			}
			hc.Cond.Value = int64(pred)
		}
		return hc, nil
	}

	return nil, errors.New("unsupported having clause")
}

func extractComparisonOp(expr *sqlparser.ComparisonExpr) pql.Token {
	switch expr.Operator {
	case "==":
		return pql.EQ
	case "!=":
		return pql.NEQ
	case "<":
		return pql.LT
	case "<=":
		return pql.LTE
	case ">":
		return pql.GT
	case ">=":
		return pql.GTE
	}
	return pql.ILLEGAL
}

// extractJoinTables returns a slice of parseTable containing two
// items, the primary and secondary join tables. This function does
// not extract join tables of the form:
//
//	from tbl1, tbl2
//
// The from clause must be of the form:
//
//	from tbl1 INNER JOIN tbl2 ON ...
func extractJoinTables(stmt *sqlparser.Select) (parseTables, error) {
	if len(stmt.From) != 1 {
		return nil, errors.New("selecting from multiple tables is not supported")
	}

	tbls := make([]*parseTable, 2)

	from, ok := stmt.From[0].(*sqlparser.JoinTableExpr)
	if !ok {
		return nil, errors.New("unsupported join clause")
	}

	leftTable, err := extractParseTable(from.LeftExpr)
	if err != nil {
		return nil, errors.Wrap(err, "extracting left join table")
	}
	rightTable, err := extractParseTable(from.RightExpr)
	if err != nil {
		return nil, errors.Wrap(err, "extracting right join table")
	}

	// It is not important which table goes in which tbls position;
	// the primary/secondary table will be determined later.
	tbls[0] = leftTable
	tbls[1] = rightTable

	// Get the ON condition and determine which table is primary.
	switch onCond := from.Condition.On.(type) {
	case *sqlparser.ComparisonExpr:
		if onCond.Operator != "=" {
			return nil, errors.Errorf("unsupported on condition comparison type: %s", onCond.Operator)
		}
		// get left ColName
		left, ok := onCond.Left.(*sqlparser.ColName)
		if !ok {
			return nil, errors.New("left join operand must be a column name")
		}
		leftJoinCol, err := extractParseColumn(left)
		if err != nil {
			return nil, errors.Wrap(err, "extracting left join column")
		}
		// get right ColName
		right, ok := onCond.Right.(*sqlparser.ColName)
		if !ok {
			return nil, errors.New("right join operand must be a column name")
		}
		rightJoinCol, err := extractParseColumn(right)
		if err != nil {
			return nil, errors.Wrap(err, "extracting right join column")
		}

		// The primary column is set as the column referencing the "_id" field.
		var primaryColumn *parseColumn
		if leftJoinCol.name == ColID && rightJoinCol.name != ColID {
			primaryColumn = leftJoinCol
		} else if leftJoinCol.name != ColID && rightJoinCol.name == ColID {
			primaryColumn = rightJoinCol
		} else {
			return nil, errors.Errorf("exactly one join column must be %s, have: %s, %s", ColID, leftJoinCol.name, rightJoinCol.name)
		}

		// populate the joinTable column and primary fields
		for _, jc := range []*parseColumn{leftJoinCol, rightJoinCol} {
			var found bool
			for i := range tbls {
				if tbls[i].alias == jc.qualifier {
					tbls[i].column = jc
					if jc == primaryColumn {
						tbls[i].primary = true
					}
					found = true
				}
			}
			if !found {
				return nil, errors.Errorf("no tables match qualifier: %s", jc.qualifier)
			}
		}

	default:
		return nil, errors.Errorf("unsupported on condition type: %T", onCond)
	}

	return tbls, nil
}

// extractWheres returns the slice of tableWhere for the sql query.
func extractWheres(indexes []*pilosa.Index, tbls parseTables, expr sqlparser.Expr) (tableWheres, error) {
	wheres := make([]*tableWhere, 0)

	// Set the index associated with each parseTable.
	// TODO: may be able to move this to parseTables creation?
	for _, idx := range indexes {
		tbl := tbls.byName(idx.Name())
		if tbl == nil {
			return nil, errors.Errorf("index not in parseTables: %s", idx.Name())
		}
		tbl.index = idx
	}

	// make a map of tbls alias to slice index.
	m := make(map[string]int)
	for i, tbl := range tbls {
		m[tbl.alias] = i
	}

	switch e := expr.(type) {
	case *sqlparser.ComparisonExpr:
		pCol, op, val, err := extractComparison(e)
		if err != nil {
			return nil, err
		}

		pTable := tbls.byAlias(pCol.qualifier)
		if pTable == nil {
			return nil, errors.Errorf("no index for qaulifier: %s", pCol.qualifier)
		} else if pTable.index == nil {
			return nil, errors.Errorf("parse table has no index: %s", pTable.name)
		}

		field := pTable.index.Field(pCol.name)

		tw := &tableWhere{
			table: pTable,
		}

		if field.Type() == pilosa.FieldTypeInt || field.Type() == pilosa.FieldTypeTimestamp {
			if field.Type() == pilosa.FieldTypeInt {
				if _, ok := val.(int); !ok {
					return nil, errors.New("right operand must be a number")
				}
			} else { // timestamp
				if _, ok := val.(time.Time); !ok {
					return nil, errors.New("right operand must be a timestamp")
				}
			}

			switch op {
			case "=":
				tw.where = Equals(field.Name(), val)
			case "<":
				tw.where = LT(field.Name(), val)
			case "<=":
				tw.where = LTE(field.Name(), val)
			case ">":
				tw.where = GT(field.Name(), val)
			case ">=":
				tw.where = GTE(field.Name(), val)
			case "<>":
				fallthrough
			case "!=":
				tw.where = NotEquals(field.Name(), val)
			}
			return append(wheres, tw), nil
		}
		if op == "=" {
			if tw.where, err = Row(field.Name(), val); err != nil {
				return nil, err
			}
			return append(wheres, tw), nil
		}

		if op == "in" {
			var qs []string
			switch valExpr := val.(type) {
			case []interface{}:
				for _, v := range valExpr {
					r, e := Row(field.Name(), v)
					if e != nil {
						return nil, errors.Wrap(err, "extracting where statements")
					}
					qs = append(qs, r)
				}
				tw.where = Union(qs...)
				return append(wheres, tw), nil

			default:
				return nil, fmt.Errorf("in operator expects `[]interface{}` but got: %T", valExpr)
			}
		}

	case *sqlparser.AndExpr:
		left, err := extractWheres(indexes, tbls, e.Left)
		if err != nil {
			return nil, err
		}
		right, err := extractWheres(indexes, tbls, e.Right)
		if err != nil {
			return nil, err
		}

		// The following logic is used to build the where portion of the query
		// related to each table. The goal is to return one or two tableWhere
		// objects (either 0 or 1 for each table in the join).
		if len(left) == 1 && len(right) == 1 && left[0].table == right[0].table {
			// if left(1) and right(1) are from the same alias,
			// then intersect them into left and return left(1)
			left[0].where = Intersect(left[0].where, right[0].where)
			return left, nil
		} else if len(left) == 1 && len(right) == 1 && left[0].table != right[0].table {
			// if left(1) and right(1) are NOT from the same alias,
			// then return final(2)
			return []*tableWhere{left[0], right[0]}, nil
		} else if len(left) == 1 && len(right) == 2 {
			// if left(1) and right(2)
			// then intersect the 1's and return final(2)
			if left[0].table == right[0].table {
				left[0].where = Intersect(left[0].where, right[0].where)
				return []*tableWhere{left[0], right[1]}, nil
			} else if left[0].table == right[1].table {
				left[0].where = Intersect(left[0].where, right[1].where)
				return []*tableWhere{left[0], right[0]}, nil
			}
			return nil, errors.Errorf("no matching table on right: %s", left[0].table.name)
		} else if len(left) == 2 && len(right) == 1 {
			// if left(2) and right(1),
			// then intersect the 1's and return final(2)
			if right[0].table == left[0].table {
				right[0].where = Intersect(right[0].where, left[0].where)
				return []*tableWhere{left[1], right[0]}, nil
			} else if right[0].table == left[1].table {
				right[0].where = Intersect(right[0].where, left[1].where)
				return []*tableWhere{left[0], right[0]}, nil
			}
			return nil, errors.Errorf("no matching table on left: %s", right[0].table.name)
		} else if len(left) == 2 && len(right) == 2 {
			// if left(2) and right(2)
			// then intsect both and return final(2)
			if left[0].table == right[0].table && left[1].table == right[1].table {
				left[0].where = Intersect(left[0].where, right[0].where)
				left[1].where = Intersect(left[1].where, right[1].where)
				return left, nil
			} else if left[0].table == right[1].table && left[1].table == right[0].table {
				left[0].where = Intersect(left[0].where, right[1].where)
				left[1].where = Intersect(left[1].where, right[0].where)
				return left, nil
			}
			return nil, errors.Errorf("non-matching tables: %s/%s, %s/%s", left[0].table.name, left[1].table.name, right[0].table.name, right[1].table.name)
		}
		return nil, errors.Errorf("invalid table count; expected 1 or 2, but got: %d, %d", len(left), len(right))
	case *sqlparser.OrExpr:
		left, err := extractWheres(indexes, tbls, e.Left)
		if err != nil {
			return nil, err
		}
		right, err := extractWheres(indexes, tbls, e.Right)
		if err != nil {
			return nil, err
		}

		if len(left) == 1 && len(right) == 1 && left[0].table == right[0].table {
			// if left(1) and right(1) are from the same alias,
			// then union them and return final(1)
			left[0].where = Union(left[0].where, right[0].where)
			return left, nil
		}
		return nil, errors.Errorf("invalid table count; expected 1/1, but got: %d/%d", len(left), len(right))
	case *sqlparser.NotExpr:
		expr, err := extractWheres(indexes, tbls, e.Expr)
		if err != nil {
			return nil, err
		}
		if len(expr) == 1 {
			expr[0].where = Not(expr[0].where)
			return expr, nil
		}
		return nil, errors.Errorf("not support a single expression. got: %d", len(expr))
	case *sqlparser.ParenExpr:
		expr, err := extractWheres(indexes, tbls, e.Expr)
		if err != nil {
			return nil, err
		}
		if len(expr) == 1 {
			return expr, nil
		}
		return nil, errors.Errorf("not support a single expression. got: %d", len(expr))
	case *sqlparser.RangeCond:
		if e.Operator != "between" {
			return nil, errors.New("only between is supported")
		}
		left, ok := e.Left.(*sqlparser.ColName)
		if !ok {
			return nil, errors.New("left operand must be a column name")
		}

		pCol, err := extractParseColumn(left)
		if err != nil {
			return nil, errors.Wrap(err, "extracting parse column")
		}

		pTable := tbls.byAlias(pCol.qualifier)
		if pTable == nil {
			return nil, errors.Errorf("no index for qaulifier: %s", pCol.qualifier)
		} else if pTable.index == nil {
			return nil, errors.Errorf("parse table has no index: %s", pTable.name)
		}

		fieldName, isSpecial := ExtractFieldName(pCol.name)
		if isSpecial {
			return nil, errors.New("special fields are not allowed here")
		}

		tw := &tableWhere{
			table: pTable,
		}

		field := pTable.index.Field(fieldName)
		if field.Type() == pilosa.FieldTypeInt {
			fromNum, err := extractInt(e.From)
			if err != nil {
				return nil, err
			}
			toNum, err := extractInt(e.To)
			if err != nil {
				return nil, err
			}
			tw.where = Between(field.Name(), fromNum, toNum)
			return append(wheres, tw), nil
		}
		if field.Type() == pilosa.FieldTypeTimestamp {
			fromTime, err := extractTimestamp(e.From)
			if err != nil {
				return nil, err
			}
			toTime, err := extractTimestamp(e.To)
			if err != nil {
				return nil, err
			}
			tw.where = Between(field.Name(), fromTime, toTime)
			return append(wheres, tw), nil
		}
		return nil, errors.New("only int or timestamp fields are supported")
	case *sqlparser.IsExpr:
		left, ok := e.Expr.(*sqlparser.ColName)
		if !ok {
			return nil, errors.New("left operand must be a column name")
		}

		pCol, err := extractParseColumn(left)
		if err != nil {
			return nil, errors.Wrap(err, "extracting parse column")
		}

		pTable := tbls.byAlias(pCol.qualifier)
		if pTable == nil {
			return nil, errors.Errorf("no index for qaulifier: %s", pCol.qualifier)
		} else if pTable.index == nil {
			return nil, errors.Errorf("parse table has no index: %s", pTable.name)
		}

		tw := &tableWhere{
			table: pTable,
		}

		field := pTable.index.Field(pCol.name)
		if field.Type() == pilosa.FieldTypeInt || field.Type() == pilosa.FieldTypeTimestamp {
			if e.Operator == "is not null" {
				tw.where = NotNull(field.Name())
				return append(wheres, tw), nil
			}
			return nil, fmt.Errorf("only `is not null` is supported for %s fields", field.Type())
		}
		return nil, fmt.Errorf("`is` expression is supported only for %s fields", field.Type())
	}
	return nil, errors.New("cannot extract where")
}
