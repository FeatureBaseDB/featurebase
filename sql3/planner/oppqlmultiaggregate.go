// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"fmt"

	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
)

// PlanOpPQLMultiAggregate plan operator handles executing multiple 'sibling' pql aggregate queries
type PlanOpPQLMultiAggregate struct {
	planner   *ExecutionPlanner
	operators []*PlanOpPQLAggregate
	warnings  []string
}

func NewPlanOpPQLMultiAggregate(p *ExecutionPlanner, operators []*PlanOpPQLAggregate) *PlanOpPQLMultiAggregate {
	return &PlanOpPQLMultiAggregate{
		planner:   p,
		operators: operators,
		warnings:  make([]string, 0),
	}
}

func (p *PlanOpPQLMultiAggregate) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_op"] = fmt.Sprintf("%T", p)
	result["_schema"] = p.Schema().Plan()
	ps := make([]interface{}, 0)
	for _, e := range p.operators {
		ps = append(ps, e.Plan())
	}
	result["operators"] = ps
	return result
}

func (p *PlanOpPQLMultiAggregate) String() string {
	return ""
}

func (p *PlanOpPQLMultiAggregate) AddWarning(warning string) {
	p.warnings = append(p.warnings, warning)
}

func (p *PlanOpPQLMultiAggregate) Warnings() []string {
	return p.warnings
}

func (p *PlanOpPQLMultiAggregate) Schema() types.Schema {
	result := make(types.Schema, len(p.operators))
	for idx, aggOp := range p.operators {
		s := &types.PlannerColumn{
			ColumnName:   aggOp.aggregate.String(),
			RelationName: "",
			Type:         aggOp.aggregate.Type(),
		}
		result[idx] = s
	}
	return result
}

func (p *PlanOpPQLMultiAggregate) Children() []types.PlanOperator {
	return []types.PlanOperator{}
}

func (p *PlanOpPQLMultiAggregate) Iterator(ctx context.Context, row types.Row) (types.RowIterator, error) {
	iterators := make([]types.RowIterator, 0)

	for _, op := range p.operators {
		iter, err := op.Iterator(ctx, row)
		if err != nil {
			return nil, err
		}
		iterators = append(iterators, iter)
	}

	return &pqlMultiAggregateRowIter{
		planner:   p.planner,
		iterators: iterators,
	}, nil
}

func (p *PlanOpPQLMultiAggregate) WithChildren(children ...types.PlanOperator) (types.PlanOperator, error) {
	return nil, nil
}

type pqlMultiAggregateRowIter struct {
	planner   *ExecutionPlanner
	iterators []types.RowIterator
	doneLatch bool
}

var _ types.RowIterator = (*pqlMultiAggregateRowIter)(nil)

func (i *pqlMultiAggregateRowIter) Next(ctx context.Context) (types.Row, error) {
	if !i.doneLatch {
		var row = make(types.Row, len(i.iterators))
		for idx, iter := range i.iterators {
			irow, err := iter.Next(ctx)
			if err != nil {
				return nil, err
			}
			row[idx] = irow[0]
		}
		i.doneLatch = true
		return row, nil
	}
	return nil, types.ErrNoMoreRows
}
