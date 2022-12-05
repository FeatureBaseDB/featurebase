// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"fmt"

	"github.com/molecula/featurebase/v3/sql3"
	"github.com/molecula/featurebase/v3/sql3/planner/types"
)

// PlanOpTop implements the TOP operator
type PlanOpTop struct {
	ChildOp  types.PlanOperator
	expr     types.PlanExpression
	warnings []string
}

func NewPlanOpTop(expr types.PlanExpression, child types.PlanOperator) *PlanOpTop {
	return &PlanOpTop{
		ChildOp:  child,
		expr:     expr,
		warnings: make([]string, 0),
	}
}

func (p *PlanOpTop) Schema() types.Schema {
	return p.ChildOp.Schema()
}

func (p *PlanOpTop) Iterator(ctx context.Context, row types.Row) (types.RowIterator, error) {
	iter, err := p.ChildOp.Iterator(ctx, row)
	if err != nil {
		return nil, err
	}
	return newTopIter(p.expr, iter), nil
}

func (p *PlanOpTop) Children() []types.PlanOperator {
	return []types.PlanOperator{
		p.ChildOp,
	}
}

func (p *PlanOpTop) WithChildren(children ...types.PlanOperator) (types.PlanOperator, error) {
	if len(children) != 1 {
		return nil, sql3.NewErrInternalf("unexpected number of children '%d'", len(children))
	}
	return NewPlanOpTop(p.expr, children[0]), nil
}

func (p *PlanOpTop) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_op"] = fmt.Sprintf("%T", p)
	sc := make([]string, 0)
	for _, e := range p.Schema() {
		sc = append(sc, fmt.Sprintf("'%s', '%s', '%s'", e.ColumnName, e.RelationName, e.Type.TypeDescription()))
	}
	result["_schema"] = sc

	result["expr"] = p.expr
	result["child"] = p.ChildOp.Plan()
	return result
}

func (p *PlanOpTop) String() string {
	return ""
}

func (p *PlanOpTop) AddWarning(warning string) {
	p.warnings = append(p.warnings, warning)
}

func (p *PlanOpTop) Warnings() []string {
	var w []string
	w = append(w, p.warnings...)
	w = append(w, p.ChildOp.Warnings()...)
	return w
}

type topIter struct {
	child types.RowIterator
	expr  types.PlanExpression

	rowCount int64
	topValue int64

	hasStarted *struct{}
}

func newTopIter(expr types.PlanExpression, child types.RowIterator) *topIter {
	return &topIter{
		child: child,
		expr:  expr,
	}
}

func (i *topIter) Next(ctx context.Context) (types.Row, error) {
	if i.hasStarted == nil {
		topEval, err := i.expr.Evaluate(nil)
		if err != nil {
			return nil, err
		}
		var ok bool
		i.topValue, ok = topEval.(int64)
		if !ok {
			return nil, sql3.NewErrInternalf("unexpected top expression result type %T", topEval)
		}
		i.hasStarted = &struct{}{}

	}

	if i.rowCount >= i.topValue {
		return nil, types.ErrNoMoreRows
	}

	row, err := i.child.Next(ctx)
	if err != nil {
		return nil, err
	}
	i.rowCount++
	return row, nil
}
