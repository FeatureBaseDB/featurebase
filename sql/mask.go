// Copyright 2021 Molecula Corp. All rights reserved.
package sql

import (
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"
)

type selectPart int

const (
	SelectPartDistinct selectPart = 1 << iota
	SelectPartID
	SelectPartStar
	SelectPartField
	SelectPartFields
	SelectPartCountStar
	SelectPartCountField
	SelectPartCountDistinctField
	SelectPartMinField
	SelectPartMaxField
	SelectPartSumField
	SelectPartAvgField
)

type fromPart int

const (
	FromPartTable fromPart = 1 << iota
	FromPartTables
	FromPartJoin
)

type wherePart int

const (
	WherePartIDCondition wherePart = 1 << iota
	WherePartFieldCondition
	WherePartMultiFieldCondition
)

type groupByPart int

const (
	GroupByPartField groupByPart = 1 << iota
	GroupByPartFields
)

type havingPart int

const (
	HavingPartCondition havingPart = 1 << iota
)

type orderByPart int

const (
	OrderByPartField orderByPart = 1 << iota
	OrderByPartFields
)

type limitPart int

const (
	LimitPartLimit limitPart = 1 << iota
	LimitPartOffset
)

type QueryMask struct {
	SelectMask  selectPart
	FromMask    fromPart
	WhereMask   wherePart
	GroupByMask groupByPart
	HavingMask  havingPart
	OrderByMask orderByPart
	LimitMask   limitPart
}

func NewQueryMask(sp selectPart, fp fromPart, wp wherePart, gp groupByPart, hp havingPart) QueryMask {
	qm := QueryMask{}
	qm.orSelect(sp)
	qm.orFrom(fp)
	qm.orWhere(wp)
	qm.orGroupBy(gp)
	qm.orHaving(hp)
	return qm
}

// ApplyFilter returns true if m passes the filter f.
// Note: only certain query parts are included; namely,
// the orderBy and limit masks are not applied to the
// filter.
func (m *QueryMask) ApplyFilter(f QueryMask) bool {
	if m.SelectMask&f.SelectMask != m.SelectMask {
		return false
	}
	if m.FromMask&f.FromMask != m.FromMask {
		return false
	}
	if m.WhereMask&f.WhereMask != m.WhereMask {
		return false
	}
	if m.GroupByMask&f.GroupByMask != m.GroupByMask {
		return false
	}
	if m.HavingMask&f.HavingMask != m.HavingMask {
		return false
	}
	return true
}

// orSelect applies the bitwise-OR operation to the selectMask.
func (m *QueryMask) orSelect(o selectPart) {
	m.SelectMask |= o
}

// orFrom applies the bitwise-OR operation to the fromMask.
func (m *QueryMask) orFrom(o fromPart) {
	m.FromMask |= o
}

// orWhere applies the bitwise-OR operation to the whereMask.
func (m *QueryMask) orWhere(parts ...wherePart) {
	for _, o := range parts {
		m.WhereMask |= o
	}
}

// orGroupBy applies the bitwise-OR operation to the groupByMask.
func (m *QueryMask) orGroupBy(o groupByPart) {
	m.GroupByMask |= o
}

// orHaving applies the bitwise-OR operation to the havingMask.
func (m *QueryMask) orHaving(o havingPart) {
	m.HavingMask |= o
}

// orOrderBy applies the bitwise-OR operation to the orderByMask.
func (m *QueryMask) orOrderBy(o orderByPart) {
	m.OrderByMask |= o
}

// orLimit applies the bitwise-OR operation to the limitMask.
func (m *QueryMask) orLimit(o limitPart) {
	m.LimitMask |= o
}

// HasSelect returns true if the mask contains a supported select clause.
func (m *QueryMask) HasSelect() bool {
	return m.SelectMask > 0
}

// HasSelectPart returns true if the mask contains the provided select part.
func (m *QueryMask) HasSelectPart(p selectPart) bool {
	return (m.SelectMask & p) > 0
}

// HasFrom returns true if the mask contains a supported from clause.
func (m *QueryMask) HasFrom() bool {
	return m.FromMask > 0
}

// HasWhere returns true if the mask contains a supported where clause.
func (m *QueryMask) HasWhere() bool {
	return m.WhereMask > 0
}

// HasGroupBy returns true if the mask contains a supported group by clause.
func (m *QueryMask) HasGroupBy() bool {
	return m.GroupByMask > 0
}

// HasHaving returns true if the mask contains a supported having clause.
func (m *QueryMask) HasHaving() bool {
	return m.HavingMask > 0
}

// HasOrderBy returns true if the mask contains a supported order by clause.
func (m *QueryMask) HasOrderBy() bool {
	return m.OrderByMask > 0
}

// HasLimit returns true if the mask contains a supported limit clause.
func (m *QueryMask) HasLimit() bool {
	return m.LimitMask > 0
}

//////////////////////////////////////////////////////////////////////////

func MustGenerateMask(sql string) QueryMask {
	parsed, err := sqlparser.Parse(sql)
	if err != nil {
		return QueryMask{}
	}
	return GenerateMask(parsed)
}

func GenerateMask(parsed sqlparser.Statement) QueryMask {
	qm := QueryMask{}

	switch stmt := parsed.(type) {
	case *sqlparser.Select:
		if strings.ToLower(strings.TrimSpace(stmt.Distinct)) == "distinct" {
			qm.orSelect(SelectPartDistinct)
		}
		// select parts
		var fldCount int
		for _, item := range stmt.SelectExprs {
			switch expr := item.(type) {
			case *sqlparser.AliasedExpr:
				switch colExpr := expr.Expr.(type) {
				case *sqlparser.ColName:
					name := colExpr.Name.String()
					if name == ColID {
						qm.orSelect(SelectPartID)
					} else {
						fldCount++
					}
				case *sqlparser.FuncExpr:
					funcName := FuncName(strings.ToLower(colExpr.Name.String()))
					switch len(colExpr.Exprs) {
					case 1:
						var isStar bool
						var isField bool
						var isDistinctField bool
						switch exp := colExpr.Exprs[0].(type) {
						case *sqlparser.AliasedExpr:
							switch exp.Expr.(type) {
							case *sqlparser.ColName:
								isField = true
								isDistinctField = colExpr.Distinct
							}
						case *sqlparser.StarExpr:
							isStar = true
						}

						switch funcName {
						case FuncCount:
							if isStar {
								qm.orSelect(SelectPartCountStar)
							} else if isDistinctField {
								qm.orSelect(SelectPartCountDistinctField)
							} else if isField {
								qm.orSelect(SelectPartCountField)
							}
						case FuncMin:
							if isField {
								qm.orSelect(SelectPartMinField)
							}
						case FuncMax:
							if isField {
								qm.orSelect(SelectPartMaxField)
							}
						case FuncSum:
							if isField {
								qm.orSelect(SelectPartSumField)
							}
						case FuncAvg:
							if isField {
								qm.orSelect(SelectPartAvgField)
							}
						}
					}
				}
			case *sqlparser.StarExpr:
				qm.orSelect(SelectPartStar)
			}
		}
		if fldCount == 1 {
			qm.orSelect(SelectPartField)
		} else if fldCount > 1 {
			qm.orSelect(SelectPartFields)
		}

		// from parts
		switch len(stmt.From) {
		case 1:
			switch stmt.From[0].(type) {
			case *sqlparser.AliasedTableExpr:
				qm.orFrom(FromPartTable)
			case *sqlparser.JoinTableExpr:
				qm.orFrom(FromPartJoin)
			}
		case 2:
			switch stmt.From[0].(type) {
			case *sqlparser.AliasedTableExpr:
				switch stmt.From[1].(type) {
				case *sqlparser.AliasedTableExpr:
					qm.orFrom(FromPartTables)
				}
			}
		}

		// where parts
		where := stmt.Where
		if where != nil {
			switch where.Type {
			case "where":
				qm.orWhere(generateWhereMask(where.Expr)...)
			}
		}

		// group by parts
		var groupByFieldCount int
		for _, item := range stmt.GroupBy {
			switch item.(type) {
			case *sqlparser.ColName:
				groupByFieldCount++
			}
		}
		if groupByFieldCount == 1 {
			qm.orGroupBy(GroupByPartField)
		} else if groupByFieldCount > 1 {
			qm.orGroupBy(GroupByPartFields)
		}

		// having parts
		if stmt.Having != nil {
			qm.orHaving(HavingPartCondition)
		}

		// order by parts
		var orderByFieldCount int
		for _, item := range stmt.OrderBy {
			switch item.Expr.(type) {
			case *sqlparser.ColName:
				orderByFieldCount++
			}
		}
		if orderByFieldCount == 1 {
			qm.orOrderBy(OrderByPartField)
		} else if orderByFieldCount > 1 {
			qm.orOrderBy(OrderByPartFields)
		}

		// limit parts
		if stmt.Limit != nil {
			switch stmt.Limit.Rowcount.(type) {
			case *sqlparser.SQLVal:
				qm.orLimit(LimitPartLimit)
			}
			switch stmt.Limit.Offset.(type) {
			case *sqlparser.SQLVal:
				qm.orLimit(LimitPartOffset)
			}
		}
	}
	return qm
}

// TODO: add more recursion within the comparison operators (left/right parts)
func generateWhereMask(e sqlparser.Expr) []wherePart {
	var wp []wherePart

	switch expr := e.(type) {
	case *sqlparser.ComparisonExpr:
		var leftName string
		if colExpr, ok := expr.Left.(*sqlparser.ColName); ok {
			leftName = colExpr.Name.String()
		}
		switch leftName {
		case ColID:
			wp = append(wp, WherePartIDCondition)
		case "":
			//
		default:
			wp = append(wp, WherePartFieldCondition)
		}
	case *sqlparser.RangeCond:
		wp = append(wp, WherePartFieldCondition)
	case *sqlparser.AndExpr, *sqlparser.OrExpr:
		// TODO: we need to recursively ensure that the left/right
		// sides of these and/or expressions are field-op-val, and
		// that none of the fields are "_id"
		// TODO: could we use extractComparison or something like it?
		wp = append(wp, WherePartMultiFieldCondition)
	case *sqlparser.NotExpr:
		wp = append(wp, generateWhereMask(expr.Expr)...)
	case *sqlparser.IsExpr:
		wp = append(wp, WherePartFieldCondition)
	case *sqlparser.ParenExpr:
		wp = append(wp, generateWhereMask(expr.Expr)...)
	}

	return wp
}
