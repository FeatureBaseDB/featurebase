// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"fmt"

	"github.com/molecula/featurebase/v3/sql3"
	"github.com/molecula/featurebase/v3/sql3/planner/types"
)

// PlanOpFanout is a query fanout operator that will execute an operator across all cluster nodes
type PlanOpFanout struct {
	planner  *ExecutionPlanner
	ChildOp  types.PlanOperator
	warnings []string
}

func NewPlanOpFanout(planner *ExecutionPlanner, child types.PlanOperator) *PlanOpFanout {
	return &PlanOpFanout{
		planner:  planner,
		ChildOp:  child,
		warnings: make([]string, 0),
	}
}

func (p *PlanOpFanout) Schema() types.Schema {
	return p.ChildOp.Schema()
}

func (p *PlanOpFanout) Iterator(ctx context.Context, row types.Row) (types.RowIterator, error) {
	return newFanOutIterator(p.planner, p.ChildOp), nil
}

func (p *PlanOpFanout) WithChildren(children ...types.PlanOperator) (types.PlanOperator, error) {
	if len(children) != 1 {
		return nil, sql3.NewErrInternalf("unexpected number of children '%d'", len(children))
	}
	return NewPlanOpFanout(p.planner, children[0]), nil
}

func (p *PlanOpFanout) Children() []types.PlanOperator {
	return []types.PlanOperator{
		p.ChildOp,
	}
}

func (p *PlanOpFanout) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_op"] = fmt.Sprintf("%T", p)
	ps := make([]string, 0)
	for _, e := range p.Schema() {
		ps = append(ps, fmt.Sprintf("'%s', '%s', '%s'", e.ColumnName, e.RelationName, e.Type.TypeDescription()))
	}
	result["_schema"] = ps
	result["child"] = p.ChildOp.Plan()
	return result
}

func (p *PlanOpFanout) String() string {
	return ""
}

func (p *PlanOpFanout) AddWarning(warning string) {
	p.warnings = append(p.warnings, warning)
}

func (p *PlanOpFanout) Warnings() []string {
	return p.warnings
}

func (p *PlanOpFanout) Expressions() []types.PlanExpression {
	return []types.PlanExpression{}
}

func (p *PlanOpFanout) WithUpdatedExpressions(exprs ...types.PlanExpression) (types.PlanOperator, error) {
	if len(exprs) != 1 {
		return nil, sql3.NewErrInternalf("unexpected number of exprs '%d'", len(exprs))
	}
	return NewPlanOpFilter(p.planner, exprs[0], p.ChildOp), nil
}

type fanOutIterator struct {
	planner *ExecutionPlanner
	childOp types.PlanOperator
	rows    types.Rows
}

func newFanOutIterator(planner *ExecutionPlanner, childOp types.PlanOperator) *fanOutIterator {
	return &fanOutIterator{
		planner: planner,
		childOp: childOp,
	}
}

func (i *fanOutIterator) Next(ctx context.Context) (types.Row, error) {
	if i.rows == nil {
		rows, err := i.planner.mapReducePlanOp(ctx, i.childOp, func(ctx context.Context, prev, v types.Rows) (types.Rows, error) {
			return append(prev, v...), nil
		})
		if err != nil {
			return nil, err
		}
		i.rows = rows
	}
	if len(i.rows) > 0 {
		row := i.rows[0]
		// Move to next result element.
		i.rows = i.rows[1:]
		return row, nil
	}
	return nil, types.ErrNoMoreRows
}
