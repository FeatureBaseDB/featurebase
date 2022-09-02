// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"fmt"

	"github.com/featurebasedb/featurebase/v3/sql3"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
)

// PlanOpGroupBy handles the GROUP BY clause
// this is the default GROUP BY operator and may be replaced by the optimizer
// with one or more of the PQL related group by or aggregate operators
type PlanOpGroupBy struct {
	ChildOp      types.PlanOperator
	Aggregates   []types.PlanExpression
	GroupByExprs []types.PlanExpression
	warnings     []string
}

func NewPlanOpGroupBy(aggregates []types.PlanExpression, groupByExprs []types.PlanExpression, child types.PlanOperator) *PlanOpGroupBy {
	return &PlanOpGroupBy{
		ChildOp:      child,
		Aggregates:   aggregates,
		GroupByExprs: groupByExprs,
	}
}

// Schema for GroupBy is the group by expressions followed by the aggregate expressions
func (p *PlanOpGroupBy) Schema() types.Schema {
	result := make(types.Schema, len(p.GroupByExprs)+len(p.Aggregates))
	for idx, expr := range p.GroupByExprs {
		ref, ok := expr.(*qualifiedRefPlanExpression)
		if !ok {
			continue
		}
		s := &types.PlannerColumn{
			Name:  ref.columnName,
			Table: ref.tableName,
			Type:  expr.Type(),
		}
		result[idx] = s
	}
	offset := len(p.GroupByExprs)
	for idx, agg := range p.Aggregates {
		s := &types.PlannerColumn{
			Name:  "",
			Table: "",
			Type:  agg.Type(),
		}
		result[idx+offset] = s
	}

	return result
}

func (p *PlanOpGroupBy) Iterator(ctx context.Context, row types.Row) (types.RowIterator, error) {
	// TODO(pok) implement group by with group by expressions
	i, err := p.ChildOp.Iterator(ctx, row)
	if err != nil {
		return nil, err
	}
	aggs := []types.PlanExpression{}
	aggs = append(aggs, p.GroupByExprs...)
	aggs = append(aggs, p.Aggregates...)
	return newGroupByIter(ctx, aggs, i), nil
}

func (p *PlanOpGroupBy) Children() []types.PlanOperator {
	return []types.PlanOperator{
		p.ChildOp,
	}
}

func (p *PlanOpGroupBy) WithChildren(children ...types.PlanOperator) (types.PlanOperator, error) {
	if len(children) != 1 {
		return nil, sql3.NewErrInternalf("unexpected number of children '%d'", len(children))
	}
	return NewPlanOpGroupBy(p.Aggregates, p.GroupByExprs, children[0]), nil
}

func (p *PlanOpGroupBy) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_op"] = fmt.Sprintf("%T", p)
	sc := make([]string, 0)
	for _, e := range p.Schema() {
		sc = append(sc, fmt.Sprintf("'%s', '%s', '%s'", e.Name, e.Table, e.Type.TypeName()))
	}
	result["_schema"] = sc
	result["child"] = p.ChildOp.Plan()
	ps := make([]interface{}, 0)
	for _, e := range p.Aggregates {
		ps = append(ps, e.Plan())
	}
	result["aggregates"] = ps
	ps = make([]interface{}, 0)
	for _, e := range p.GroupByExprs {
		ps = append(ps, e.Plan())
	}
	result["groupByExprs"] = ps
	return result
}

func (p *PlanOpGroupBy) String() string {
	return ""
}

func (p *PlanOpGroupBy) AddWarning(warning string) {
	p.warnings = append(p.warnings, warning)
}

func (p *PlanOpGroupBy) Warnings() []string {
	var w []string
	w = append(w, p.warnings...)
	w = append(w, p.ChildOp.Warnings()...)
	return w
}

type groupByIter struct {
	aggregates []types.PlanExpression
	child      types.RowIterator
	ctx        context.Context
	buf        []types.AggregationBuffer
	done       bool
}

func newGroupByIter(ctx context.Context, aggregates []types.PlanExpression, child types.RowIterator) *groupByIter {
	return &groupByIter{
		aggregates: aggregates,
		child:      child,
		ctx:        ctx,
		buf:        make([]types.AggregationBuffer, len(aggregates)),
	}
}

func (i *groupByIter) Next(ctx context.Context) (types.Row, error) {
	if i.done {
		return nil, types.ErrNoMoreRows
	}

	i.done = true

	var err error
	for j, a := range i.aggregates {
		i.buf[j], err = newAggregationBuffer(a)
		if err != nil {
			return nil, err
		}
	}

	for {
		row, err := i.child.Next(ctx)
		if err != nil {
			if err == types.ErrNoMoreRows {
				break
			}
			return nil, err
		}

		if err := updateBuffers(ctx, i.buf, row); err != nil {
			return nil, err
		}
	}

	return evalBuffers(ctx, i.buf)
}

func newAggregationBuffer(expr types.PlanExpression) (types.AggregationBuffer, error) {
	switch n := expr.(type) {
	case types.Aggregable:
		return n.NewBuffer()
	default:
		return NewAggLastBuffer(expr), nil
	}
}

func updateBuffers(ctx context.Context, buffers []types.AggregationBuffer, row types.Row) error {
	for _, b := range buffers {
		if err := b.Update(ctx, row); err != nil {
			return err
		}
	}
	return nil
}

func evalBuffers(ctx context.Context, buffers []types.AggregationBuffer) (types.Row, error) {
	var row = make(types.Row, len(buffers))
	var err error
	for i, b := range buffers {
		row[i], err = b.Eval(ctx)
		if err != nil {
			return nil, err
		}
	}
	return row, nil
}
