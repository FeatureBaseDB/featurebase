// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"strings"

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

		default:
			return nil, sql3.NewErrInternalf("unsupported scalar function '%s'", expr.name)
		}

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

	case parser.EQ, parser.NE, parser.LT, parser.LE, parser.GT, parser.GE:
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
			if strings.EqualFold(lhs.columnName, "_id") {
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
			if strings.EqualFold(lhs.columnName, "_id") {
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
					lhs.columnName: pqlValue,
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
			if strings.EqualFold(lhs.columnName, "_id") {
				return nil, sql3.NewErrInvalidColumnInFilterExpression(0, 0, "_id", "is/is not null")
			}
			return nil, sql3.NewErrInvalidTypeInFilterExpression(0, 0, typ.TypeName(), "is/is not null")

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
			return nil, sql3.NewErrInvalidTypeInFilterExpression(0, 0, typ.TypeName(), "is/is not null")
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
	default:
		return nil, sql3.NewErrInternalf("cannot convert SQL expression %T to a literal value", expr)
	}
}
