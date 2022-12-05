// Copyright 2021 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"fmt"

	pilosa "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/pql"
	"github.com/featurebasedb/featurebase/v3/sql3"
	"github.com/featurebasedb/featurebase/v3/sql3/parser"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
)

// PlanOpPQLAggregate plan operator handles a single pql aggregate
type PlanOpPQLAggregate struct {
	planner   *ExecutionPlanner
	tableName string
	filter    types.PlanExpression
	aggregate types.Aggregable

	warnings []string
}

func NewPlanOpPQLAggregate(p *ExecutionPlanner, tableName string, aggregate types.Aggregable, filter types.PlanExpression) *PlanOpPQLAggregate {
	return &PlanOpPQLAggregate{
		planner:   p,
		tableName: tableName,
		filter:    filter,
		aggregate: aggregate,
		warnings:  make([]string, 0),
	}
}

func (p *PlanOpPQLAggregate) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_op"] = fmt.Sprintf("%T", p)
	ps := make([]string, 0)
	for _, e := range p.Schema() {
		ps = append(ps, fmt.Sprintf("'%s', '%s', '%s'", e.ColumnName, e.RelationName, e.Type.TypeDescription()))
	}
	result["_schema"] = ps
	result["tableName"] = p.tableName
	if p.filter != nil {
		result["filter"] = p.filter.Plan()
	}
	result["aggregate"] = p.aggregate.AggExpression().Plan()
	return result

}

func (p *PlanOpPQLAggregate) String() string {
	return ""
}

func (p *PlanOpPQLAggregate) AddWarning(warning string) {
	p.warnings = append(p.warnings, warning)
}

func (p *PlanOpPQLAggregate) Warnings() []string {
	return p.warnings
}

func (p *PlanOpPQLAggregate) Schema() types.Schema {
	result := make(types.Schema, 1)
	s := &types.PlannerColumn{
		ColumnName:   "",
		RelationName: "",
		Type:         p.aggregate.AggExpression().Type(),
	}
	result[0] = s
	return result
}

func (p *PlanOpPQLAggregate) Children() []types.PlanOperator {
	return []types.PlanOperator{}
}

func (p *PlanOpPQLAggregate) Iterator(ctx context.Context, row types.Row) (types.RowIterator, error) {
	return &pqlAggregateRowIter{
		planner:   p.planner,
		tableName: p.tableName,
		filter:    p.filter,
		aggregate: p.aggregate,
	}, nil
}

func (p *PlanOpPQLAggregate) WithChildren(children ...types.PlanOperator) (types.PlanOperator, error) {
	return NewPlanOpPQLAggregate(p.planner, p.tableName, p.aggregate, p.filter), nil
}

type pqlAggregateRowIter struct {
	planner   *ExecutionPlanner
	tableName string
	filter    types.PlanExpression
	aggregate types.Aggregable

	resultValue interface{}
}

var _ types.RowIterator = (*pqlAggregateRowIter)(nil)

func (i *pqlAggregateRowIter) Next(ctx context.Context) (types.Row, error) {
	if i.resultValue == nil {
		var call *pql.Call
		var cond *pql.Call
		var err error

		err = i.planner.checkAccess(ctx, i.tableName, accessTypeReadData)
		if err != nil {
			return nil, err
		}

		cond, err = i.planner.generatePQLCallFromExpr(ctx, i.filter)
		if err != nil {
			return nil, err
		}

		expr, ok := i.aggregate.AggExpression().(*qualifiedRefPlanExpression)
		if !ok {
			return nil, sql3.NewErrInternalf("unexpected aggregate expression type '%T'", i.aggregate.AggExpression())
		}

		switch i.aggregate.AggType() {
		case types.AGGREGATE_COUNT_DISTINCT:
			//make a distinct call
			distinctCond := &pql.Call{
				Name: "Distinct",
				Args: map[string]interface{}{"field": expr.columnName},
				Type: pql.PrecallGlobal,
			}
			//add the cond to the distinct
			if cond != nil {
				distinctCond.Children = []*pql.Call{cond}
			}
			cond = distinctCond

			call = &pql.Call{Name: "Count", Children: []*pql.Call{cond}}

		case types.AGGREGATE_COUNT:
			if cond == nil {
				// COUNT() should ignore null values
				// if the data type of the expression supports an existence bitmap for
				// the underlying FeatureBase data type use it to eliminate nulls from the aggregate
				switch expr.dataType.(type) {
				case *parser.DataTypeInt, *parser.DataTypeTimestamp, *parser.DataTypeDecimal:
					cond = &pql.Call{
						Name: "Row",
						Args: map[string]interface{}{
							expr.columnName: &pql.Condition{Op: pql.NEQ, Value: nil},
						},
					}
				default:
					cond = &pql.Call{Name: "All"}
				}
			}
			call = &pql.Call{Name: "Count", Children: []*pql.Call{cond}}

		case types.AGGREGATE_AVG:
			if cond == nil {
				// COUNT() should ignore null values
				// if the data type of the expression supports an existence bitmap for
				// the underlying FeatureBase data type use it to eliminate nulls from the aggregate
				switch expr.dataType.(type) {
				case *parser.DataTypeInt, *parser.DataTypeTimestamp, *parser.DataTypeDecimal:
					cond = &pql.Call{
						Name: "Row",
						Args: map[string]interface{}{
							expr.columnName: &pql.Condition{Op: pql.NEQ, Value: nil},
						},
					}
				default:
					cond = &pql.Call{Name: "All"}
				}
			}

			call = &pql.Call{
				Name:     "Sum",
				Args:     map[string]interface{}{"field": expr.columnName},
				Children: []*pql.Call{cond},
			}

		case types.AGGREGATE_SUM:
			if cond == nil {
				cond = &pql.Call{Name: "All"}
			}
			call = &pql.Call{
				Name:     "Sum",
				Args:     map[string]interface{}{"field": expr.columnName},
				Children: []*pql.Call{cond},
			}

		case types.AGGREGATE_MAX:
			if cond == nil {
				cond = &pql.Call{Name: "All"}
			}

			call = &pql.Call{
				Name:     "Max",
				Args:     map[string]interface{}{"field": expr.columnName},
				Children: []*pql.Call{cond},
			}

		case types.AGGREGATE_MIN:
			if cond == nil {
				cond = &pql.Call{Name: "All"}
			}

			call = &pql.Call{
				Name:     "Min",
				Args:     map[string]interface{}{"field": expr.columnName},
				Children: []*pql.Call{cond},
			}

		case types.AGGREGATE_PERCENTILE:

			additionalExprs := i.aggregate.AggAdditionalExpr()
			if len(additionalExprs) != 1 {
				return nil, sql3.NewErrInternalf("unexpected AggAdditionalExpr() length (%d)", len(additionalExprs))
			}
			nthExpr := additionalExprs[0]

			nthValue, err := nthExpr.Evaluate(nil)
			if err != nil {
				return nil, err
			}
			coercedNthValue, err := coerceValue(nthExpr.Type(), parser.NewDataTypeDecimal(4), nthValue, parser.Pos{Line: 0, Column: 0})
			if err != nil {
				return nil, err
			}
			nth, ok := coercedNthValue.(pql.Decimal)
			if !ok {
				return nil, sql3.NewErrInternalf("unexpected aggregate nth arg type '%T'", coercedNthValue)
			}

			if cond == nil {
				cond = &pql.Call{Name: "All"}
			}

			call = &pql.Call{
				Name: "Percentile",
				Args: map[string]interface{}{
					"field": expr.columnName,
					"nth":   nth,
				},
				Children: []*pql.Call{cond},
			}

		default:
			return nil, sql3.NewErrInternalf("unhandled aggregate type '%d'", i.aggregate.AggType())
		}

		queryResponse, err := i.planner.executor.Execute(ctx, i.tableName, &pql.Query{Calls: []*pql.Call{call}}, nil, nil)
		if err != nil {
			return nil, err
		}

		switch actualResult := queryResponse.Results[0].(type) {
		case uint64:
			i.resultValue = int64(actualResult)

		case pilosa.ValCount:
			if actualResult.DecimalVal == nil {
				if i.aggregate.AggType() == types.AGGREGATE_AVG {
					average := float64(actualResult.Val) / float64(actualResult.Count)
					i.resultValue = pql.NewDecimal(int64(average*10000), 4)
				} else {
					i.resultValue = int64(actualResult.Val)
				}
			} else {
				if i.aggregate.AggType() == types.AGGREGATE_AVG {
					average := actualResult.DecimalVal.Float64() / float64(actualResult.Count)
					i.resultValue = pql.NewDecimal(int64(average*10000), 4)
				} else {
					i.resultValue = *actualResult.DecimalVal
				}
			}
		default:
			return nil, sql3.NewErrInternalf("unexpected result type '%T'", queryResponse.Results[0])
		}

		row := make([]interface{}, 1)
		row[0] = i.resultValue
		return row, nil

	}
	return nil, types.ErrNoMoreRows
}
