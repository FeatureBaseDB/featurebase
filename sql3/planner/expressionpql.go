// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"strconv"
	"strings"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/pql"
	"github.com/featurebasedb/featurebase/v3/sql3"
	"github.com/featurebasedb/featurebase/v3/sql3/parser"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
)

// generatePQLCallFromExpr returns a *pql.Call tree for a given plan expression
func (p *ExecutionPlanner) generatePQLCallFromExpr(ctx context.Context, expr types.PlanExpression) (_ *pql.Call, err error) {
	if expr == nil {
		return nil, nil
	}

	switch expr := expr.(type) {
	case *binOpPlanExpression:
		return p.generatePQLCallFromBinaryExpr(ctx, expr)

	case *callPlanExpression:
		switch strings.ToUpper(expr.name) {
		case "SETCONTAINS":
			col := expr.args[0].(*qualifiedRefPlanExpression)

			pqlValue, err := planExprToValue(expr.args[1])
			if err != nil {
				return nil, err
			}
			return &pql.Call{
				Name: "Row",
				Args: map[string]interface{}{
					col.columnName: pqlValue,
				},
			}, nil

		case "SETCONTAINSALL":
			col := expr.args[0].(*qualifiedRefPlanExpression)

			set, ok := expr.args[1].(*exprSetLiteralPlanExpression)
			if !ok {
				return nil, sql3.NewErrInternalf("unexpected argument type '%T'", expr.args[1])
			}

			call := &pql.Call{
				Name:     "Intersect",
				Children: []*pql.Call{},
			}

			for _, m := range set.members {
				pqlValue, err := planExprToValue(m)
				if err != nil {
					return nil, err
				}
				rc := &pql.Call{
					Name: "Row",
					Args: map[string]interface{}{
						col.columnName: pqlValue,
					},
				}
				call.Children = append(call.Children, rc)
			}
			return call, nil

		case "SETCONTAINSANY":
			col := expr.args[0].(*qualifiedRefPlanExpression)

			set, ok := expr.args[1].(*exprSetLiteralPlanExpression)
			if !ok {
				return nil, sql3.NewErrInternalf("unexpected argument type '%T'", expr.args[1])
			}

			call := &pql.Call{
				Name:     "Union",
				Children: []*pql.Call{},
			}

			for _, m := range set.members {
				pqlValue, err := planExprToValue(m)
				if err != nil {
					return nil, err
				}
				rc := &pql.Call{
					Name: "Row",
					Args: map[string]interface{}{
						col.columnName: pqlValue,
					},
				}
				call.Children = append(call.Children, rc)
			}
			return call, nil

		case "RANGEQ":
			col := expr.args[0].(*qualifiedRefPlanExpression)

			var fromValue interface{}
			switch fromExpr := expr.args[1].(type) {
			case *stringLiteralPlanExpression:
				// parse timestamp from string and use the int value
				ts := fromExpr.ConvertToTimestamp()
				if ts == nil {
					return nil, sql3.NewErrInvalidTypeCoercion(0, 0, fromExpr.value, parser.NewDataTypeTimestamp().TypeDescription())
				}
				fromValue = ts.Unix()

			case *intLiteralPlanExpression:
				// use the int value
				fromValue = fromExpr.value

			case *nullLiteralPlanExpression:
				fromValue = nil

			default:
				return nil, sql3.NewErrInternalf("unexpected argument type '%T'", expr.args[1])
			}

			var toValue interface{}
			switch toExpr := expr.args[2].(type) {
			case *stringLiteralPlanExpression:
				// parse timestamp from string and use the int value
				ts := toExpr.ConvertToTimestamp()
				if ts == nil {
					return nil, sql3.NewErrInvalidTypeCoercion(0, 0, toExpr.value, parser.NewDataTypeTimestamp().TypeDescription())
				}
				toValue = ts.Unix()

			case *intLiteralPlanExpression:
				// use the int value
				toValue = toExpr.value

			case *nullLiteralPlanExpression:
				toValue = nil

			default:
				return nil, sql3.NewErrInternalf("unexpected argument type '%T'", expr.args[1])
			}

			if fromValue == nil && toValue == nil {
				return nil, sql3.NewErrQRangeFromAndToTimeCannotBeBothNull(0, 0)
			}

			var call *pql.Call
			if fromValue == nil && toValue != nil {
				call = &pql.Call{
					Name: "Rows",
					Args: map[string]interface{}{
						"field": strings.ToLower(col.columnName),
						"to":    toValue,
					},
				}
			} else if fromValue != nil && toValue == nil {
				call = &pql.Call{
					Name: "Rows",
					Args: map[string]interface{}{
						"field": strings.ToLower(col.columnName),
						"from":  fromValue,
					},
				}
			} else {
				call = &pql.Call{
					Name: "Rows",
					Args: map[string]interface{}{
						"field": strings.ToLower(col.columnName),
						"from":  fromValue,
						"to":    toValue,
					},
				}
			}

			return call, nil

		default:
			return nil, sql3.NewErrInternalf("unsupported scalar function '%s'", expr.name)
		}

	case *inOpPlanExpression:
		// lhs will be qualified ref
		lhs, ok := expr.lhs.(*qualifiedRefPlanExpression)
		if !ok {
			return nil, sql3.NewErrInternalf("unexpected lhs %T", expr.lhs)
		}

		// rhs is expression list - need to convert to a big OR

		list, ok := expr.rhs.(*exprListPlanExpression)
		if !ok {
			return nil, sql3.NewErrInternalf("unexpected argument type '%T'", expr.rhs)
		}

		// if it is the _id column, we can use ConstRow with a list
		if strings.EqualFold(lhs.columnName, string(dax.PrimaryKeyFieldName)) {
			values := make([]interface{}, len(list.exprs))
			for i, m := range list.exprs {
				pqlValue, err := planExprToValue(m)
				if err != nil {
					return nil, err
				}
				values[i] = pqlValue
			}
			call := &pql.Call{
				Name: "ConstRow",
				Args: map[string]interface{}{
					"columns": values,
				},
				Type: pql.PrecallGlobal,
			}
			return call, nil
		}
		// otherwise, OR them all
		call := &pql.Call{
			Name:     "Union",
			Children: []*pql.Call{},
		}

		for _, m := range list.exprs {
			pqlValue, err := planExprToValue(m)
			if err != nil {
				return nil, err
			}
			rc := &pql.Call{
				Name: "Row",
				Args: map[string]interface{}{
					lhs.columnName: pqlValue,
				},
			}
			call.Children = append(call.Children, rc)
		}
		return call, nil
	case *betweenOpPlanExpression:
		lhs, ok := expr.lhs.(*qualifiedRefPlanExpression)
		if !ok {
			return nil, sql3.NewErrInternalf("expected expression type: planner.qualifiedRefPlanExpression got:%T", expr.lhs)
		}
		rexp, ok := expr.rhs.(*rangePlanExpression)
		if !ok {
			return nil, sql3.NewErrInternalf("expected expression type: planner.rangePlanExpression got:%T", expr.rhs)
		}
		lower, err := planExprToValue(rexp.lhs)
		if err != nil {
			return nil, err
		}
		upper, err := planExprToValue(rexp.rhs)
		if err != nil {
			return nil, err
		}
		call := &pql.Call{
			Name: "Row",
			Args: map[string]interface{}{
				lhs.columnName: &pql.Condition{
					Op:    pql.BETWEEN,
					Value: []interface{}{lower, upper},
				},
			},
		}
		return call, nil
	default:
		return nil, sql3.NewErrInternalf("unexpected expression type: %T", expr)
	}
}

func (p *ExecutionPlanner) generatePQLCallFromBinaryExpr(ctx context.Context, expr *binOpPlanExpression) (_ *pql.Call, err error) {
	switch op := expr.op; op {
	case parser.AND, parser.OR:
		name := "Intersect"
		if op == parser.OR {
			name = "Union"
		}

		x, err := p.generatePQLCallFromExpr(ctx, expr.lhs)
		if err != nil {
			return nil, err
		}
		y, err := p.generatePQLCallFromExpr(ctx, expr.rhs)
		if err != nil {
			return nil, err
		}

		return &pql.Call{
			Name:     name,
			Children: []*pql.Call{x, y},
		}, nil

	case parser.EQ:
		lhs, ok := expr.lhs.(*qualifiedRefPlanExpression)
		if !ok {
			return nil, sql3.NewErrInternalf("unexpected lhs %T", expr.lhs)
		}

		pqlValue, err := planExprToValue(expr.rhs)
		if err != nil {
			return nil, err
		}

		switch typ := expr.lhs.Type().(type) {
		case *parser.DataTypeInt:
			return &pql.Call{
				Name: "Row",
				Args: map[string]interface{}{
					lhs.columnName: &pql.Condition{
						Op:    pql.EQ,
						Value: pqlValue,
					},
				},
			}, nil

		case *parser.DataTypeID:
			if strings.EqualFold(lhs.columnName, string(dax.PrimaryKeyFieldName)) {
				return &pql.Call{
					Name: "ConstRow",
					Args: map[string]interface{}{
						"columns": []interface{}{pqlValue},
					},
					Type: pql.PrecallGlobal,
				}, nil
			}
			return &pql.Call{
				Name: "Row",
				Args: map[string]interface{}{
					lhs.columnName: pqlValue,
				},
			}, nil

		case *parser.DataTypeString:
			if strings.EqualFold(lhs.columnName, string(dax.PrimaryKeyFieldName)) {
				return &pql.Call{
					Name: "ConstRow",
					Args: map[string]interface{}{
						"columns": []interface{}{pqlValue},
					},
					Type: pql.PrecallGlobal,
				}, nil
			}
			return &pql.Call{
				Name: "Row",
				Args: map[string]interface{}{
					lhs.columnName: pqlValue,
				},
			}, nil

		case *parser.DataTypeTimestamp:
			return &pql.Call{
				Name: "Row",
				Args: map[string]interface{}{
					lhs.columnName: &pql.Condition{
						Op:    pql.EQ,
						Value: pqlValue,
					},
				},
			}, nil

		case *parser.DataTypeBool:
			return &pql.Call{
				Name: "Row",
				Args: map[string]interface{}{
					lhs.columnName: pqlValue,
				},
			}, nil

		case *parser.DataTypeDecimal:
			val, ok := pqlValue.(float64)
			if !ok {
				return nil, sql3.NewErrInternalf("unexpected type '%T", pqlValue)
			}
			d := pql.FromFloat64(val)
			return &pql.Call{
				Name: "Row",
				Args: map[string]interface{}{
					lhs.columnName: &pql.Condition{
						Op:    pql.EQ,
						Value: d,
					},
				},
			}, nil

		default:
			return nil, sql3.NewErrInternalf("unsupported type for binary expression: %v (%T)", typ, typ)
		}

	case parser.NE:
		lhs, ok := expr.lhs.(*qualifiedRefPlanExpression)
		if !ok {
			return nil, sql3.NewErrInternalf("unexpected lhs %T", expr.lhs)
		}

		pqlValue, err := planExprToValue(expr.rhs)
		if err != nil {
			return nil, err
		}

		switch typ := expr.lhs.Type().(type) {
		case *parser.DataTypeInt:
			return &pql.Call{
				Name: "Row",
				Args: map[string]interface{}{
					lhs.columnName: &pql.Condition{
						Op:    pql.NEQ,
						Value: pqlValue,
					},
				},
			}, nil

		case *parser.DataTypeID:
			return nil, sql3.NewErrUnsupported(0, 0, true, "not equal operator on id typed columns")

		case *parser.DataTypeString:
			return nil, sql3.NewErrUnsupported(0, 0, true, "not equal operator on string typed columns")

		case *parser.DataTypeTimestamp:
			return &pql.Call{
				Name: "Row",
				Args: map[string]interface{}{
					lhs.columnName: &pql.Condition{
						Op:    pql.NEQ,
						Value: pqlValue,
					},
				},
			}, nil

		case *parser.DataTypeBool:
			return nil, sql3.NewErrUnsupported(0, 0, true, "not equal operator on bool typed columns")

		case *parser.DataTypeDecimal:
			val, ok := pqlValue.(float64)
			if !ok {
				return nil, sql3.NewErrInternalf("unexpected type '%T", pqlValue)
			}
			d := pql.FromFloat64(val)
			return &pql.Call{
				Name: "Row",
				Args: map[string]interface{}{
					lhs.columnName: &pql.Condition{
						Op:    pql.NEQ,
						Value: d,
					},
				},
			}, nil

		default:
			return nil, sql3.NewErrInternalf("unsupported type for binary expression: %v (%T)", typ, typ)
		}

	case parser.LT, parser.LE, parser.GT, parser.GE:
		lhs, ok := expr.lhs.(*qualifiedRefPlanExpression)
		if !ok {
			return nil, sql3.NewErrInternalf("unexpected lhs %T", expr.lhs)
		}

		pqlValue, err := planExprToValue(expr.rhs)
		if err != nil {
			return nil, err
		}

		switch typ := expr.lhs.Type().(type) {
		case *parser.DataTypeInt:
			pqlOp, err := sqlToPQLOp(op)
			if err != nil {
				return nil, err
			}
			return &pql.Call{
				Name: "Row",
				Args: map[string]interface{}{
					lhs.columnName: &pql.Condition{
						Op:    pqlOp,
						Value: pqlValue,
					},
				},
			}, nil

		case *parser.DataTypeID:
			return nil, sql3.NewErrUnsupported(0, 0, false, "range queries on id typed columns")

		case *parser.DataTypeString:
			return nil, sql3.NewErrUnsupported(0, 0, false, "range queries on string typed columns")

		case *parser.DataTypeTimestamp:
			pqlOp, err := sqlToPQLOp(op)
			if err != nil {
				return nil, err
			}
			return &pql.Call{
				Name: "Row",
				Args: map[string]interface{}{
					lhs.columnName: &pql.Condition{
						Op:    pqlOp,
						Value: pqlValue,
					},
				},
			}, nil

		case *parser.DataTypeDecimal:

			pqlOp, err := sqlToPQLOp(op)
			if err != nil {
				return nil, err
			}
			val, ok := pqlValue.(float64)
			if !ok {
				return nil, sql3.NewErrInternalf("unexpected type '%T", pqlValue)
			}
			d := pql.FromFloat64(val)
			return &pql.Call{
				Name: "Row",
				Args: map[string]interface{}{
					lhs.columnName: &pql.Condition{
						Op:    pqlOp,
						Value: d,
					},
				},
			}, nil

		default:
			return nil, sql3.NewErrInternalf("unsupported type for binary expression: %v (%T)", typ, typ)
		}

	case parser.BITAND, parser.BITOR, parser.BITNOT, parser.LSHIFT, parser.RSHIFT:
		return nil, sql3.NewErrInternal("bitwise operators are not supported here")

	case parser.PLUS, parser.MINUS, parser.STAR, parser.SLASH, parser.REM: // +
		return nil, sql3.NewErrInternal("aritmetic operators are not supported here")

	case parser.CONCAT:
		return nil, sql3.NewErrInternal("concatenation operator is not supported here")

	case parser.IN, parser.NOTIN:
		return nil, sql3.NewErrInternal("IN operator is not supported")

	case parser.IS, parser.ISNOT:
		lhs, ok := expr.lhs.(*qualifiedRefPlanExpression)
		if !ok {
			return nil, sql3.NewErrInternalf("unexpected lhs %T", expr.lhs)
		}

		pqlOp := pql.EQ
		if op == parser.ISNOT {
			pqlOp = pql.NEQ
		}
		switch typ := expr.lhs.Type().(type) {
		case *parser.DataTypeID:
			if strings.EqualFold(lhs.columnName, string(dax.PrimaryKeyFieldName)) {
				return nil, sql3.NewErrInvalidColumnInFilterExpression(0, 0, string(dax.PrimaryKeyFieldName), "is/is not null")
			}
			return nil, sql3.NewErrInvalidTypeInFilterExpression(0, 0, typ.TypeDescription(), "is/is not null")

		case *parser.DataTypeInt, *parser.DataTypeDecimal, *parser.DataTypeTimestamp:
			return &pql.Call{
				Name: "Row",
				Args: map[string]interface{}{
					lhs.columnName: &pql.Condition{
						Op:    pqlOp,
						Value: nil,
					},
				},
			}, nil

		default:
			return nil, sql3.NewErrInvalidTypeInFilterExpression(0, 0, typ.TypeDescription(), "is/is not null")
		}

	case parser.BETWEEN, parser.NOTBETWEEN:
		return nil, sql3.NewErrInternal("BETWEEN operator is not supported")

	default:
		return nil, sql3.NewErrInternalf("unexpected binary expression operator: %s", expr.op)
	}
}

// sqlToPQLOp converts a parser operation token to PQL.
func sqlToPQLOp(op parser.Token) (pql.Token, error) {
	switch op {
	case parser.EQ:
		return pql.EQ, nil
	case parser.NE:
		return pql.NEQ, nil
	case parser.LT:
		return pql.LT, nil
	case parser.LE:
		return pql.LTE, nil
	case parser.GT:
		return pql.GT, nil
	case parser.GE:
		return pql.GTE, nil
	default:
		return pql.ILLEGAL, sql3.NewErrInternalf("cannot convert SQL op %q to PQL", op)
	}
}

// planExprToValue converts a literal parser expression node to a value.
func planExprToValue(expr types.PlanExpression) (interface{}, error) {
	switch expr := expr.(type) {
	case *intLiteralPlanExpression:
		return expr.value, nil
	case *stringLiteralPlanExpression:
		return expr.value, nil
	case *dateLiteralPlanExpression:
		return expr.value, nil
	case *boolLiteralPlanExpression:
		return expr.value, nil
	case *floatLiteralPlanExpression:
		f, err := strconv.ParseFloat(expr.value, 64)
		if err != nil {
			return nil, err
		}
		return f, nil
	default:
		return nil, sql3.NewErrInternalf("cannot convert SQL expression %T to a literal value", expr)
	}
}
