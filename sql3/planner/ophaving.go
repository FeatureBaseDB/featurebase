// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"fmt"

	"github.com/molecula/featurebase/v3/sql3"
	"github.com/molecula/featurebase/v3/sql3/planner/types"
)

// PlanOpHaving is a filter operator for the HAVING clause
type PlanOpHaving struct {
	planner   *ExecutionPlanner
	ChildOp   types.PlanOperator
	Predicate types.PlanExpression

	warnings []string
}

func NewPlanOpHaving(planner *ExecutionPlanner, predicate types.PlanExpression, child types.PlanOperator) *PlanOpHaving {
	return &PlanOpHaving{
		planner:   planner,
		Predicate: predicate,
		ChildOp:   child,
		warnings:  make([]string, 0),
	}
}

func (p *PlanOpHaving) Schema() types.Schema {
	return p.ChildOp.Schema()
}

func (p *PlanOpHaving) Iterator(ctx context.Context, row types.Row) (types.RowIterator, error) {
	i, err := p.ChildOp.Iterator(ctx, row)
	if err != nil {
		return nil, err
	}
	return newFilterIterator(ctx, p.Predicate, i), nil
}

func (p *PlanOpHaving) WithChildren(children ...types.PlanOperator) (types.PlanOperator, error) {
	if len(children) != 1 {
		return nil, sql3.NewErrInternalf("unexpected number of children '%d'", len(children))
	}
	return NewPlanOpHaving(p.planner, p.Predicate, children[0]), nil
}

func (p *PlanOpHaving) Children() []types.PlanOperator {
	return []types.PlanOperator{
		p.ChildOp,
	}
}

func (p *PlanOpHaving) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_op"] = fmt.Sprintf("%T", p)
	ps := make([]string, 0)
	for _, e := range p.Schema() {
		ps = append(ps, fmt.Sprintf("'%s', '%s', '%s'", e.ColumnName, e.RelationName, e.Type.TypeDescription()))
	}
	result["_schema"] = ps
	result["predicate"] = p.Predicate.Plan()
	result["child"] = p.ChildOp.Plan()
	return result
}

func (p *PlanOpHaving) String() string {
	return ""
}

func (p *PlanOpHaving) AddWarning(warning string) {
	p.warnings = append(p.warnings, warning)
}

func (p *PlanOpHaving) Warnings() []string {
	return p.warnings
}

func (p *PlanOpHaving) Expressions() []types.PlanExpression {
	if p.Predicate != nil {
		return []types.PlanExpression{
			p.Predicate,
		}
	}
	return []types.PlanExpression{}
}

func (p *PlanOpHaving) WithUpdatedExpressions(exprs ...types.PlanExpression) (types.PlanOperator, error) {
	if len(exprs) != 1 {
		return nil, sql3.NewErrInternalf("unexpected number of exprs '%d'", len(exprs))
	}
	return NewPlanOpHaving(p.planner, exprs[0], p.ChildOp), nil
}
