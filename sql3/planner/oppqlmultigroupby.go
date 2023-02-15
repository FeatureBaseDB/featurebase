// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"fmt"

	"github.com/featurebasedb/featurebase/v3/sql3"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
)

// PlanOpPQLMultiGroupBy plan operator handles executing multiple 'sibling' pql group by queries
// it will materialize the result sets from each of its operators and then merge them.
// Its iterator will return a row consisting of all the group by columns in the order specified
// followed by all aggregates in order
type PlanOpPQLMultiGroupBy struct {
	planner      *ExecutionPlanner
	operators    []*PlanOpPQLGroupBy
	groupByExprs []types.PlanExpression
	warnings     []string
}

func NewPlanOpPQLMultiGroupBy(p *ExecutionPlanner, operators []*PlanOpPQLGroupBy, groupByExprs []types.PlanExpression) *PlanOpPQLMultiGroupBy {
	return &PlanOpPQLMultiGroupBy{
		planner:      p,
		operators:    operators,
		groupByExprs: groupByExprs,
		warnings:     make([]string, 0),
	}
}

func (p *PlanOpPQLMultiGroupBy) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_op"] = fmt.Sprintf("%T", p)
	result["_schema"] = p.Schema().Plan()
	ps := make([]interface{}, 0)
	for _, e := range p.operators {
		ps = append(ps, e.Plan())
	}
	result["operators"] = ps
	ps = make([]interface{}, 0)
	for _, e := range p.groupByExprs {
		ps = append(ps, e.Plan())
	}
	result["groupByColumns"] = ps
	return result
}

func (p *PlanOpPQLMultiGroupBy) String() string {
	return ""
}

func (p *PlanOpPQLMultiGroupBy) AddWarning(warning string) {
	p.warnings = append(p.warnings, warning)
}

func (p *PlanOpPQLMultiGroupBy) Warnings() []string {
	return p.warnings
}

func (p *PlanOpPQLMultiGroupBy) Schema() types.Schema {
	result := make(types.Schema, len(p.groupByExprs)+len(p.operators))
	for idx, expr := range p.groupByExprs {
		ref, ok := expr.(*qualifiedRefPlanExpression)
		if !ok {
			continue
		}
		s := &types.PlannerColumn{
			ColumnName:   ref.columnName,
			RelationName: ref.tableName,
			Type:         expr.Type(),
		}
		result[idx] = s
	}
	offset := len(p.groupByExprs)
	for idx, aggOp := range p.operators {
		s := &types.PlannerColumn{
			ColumnName:   aggOp.aggregate.String(),
			RelationName: "",
			Type:         aggOp.aggregate.FirstChildExpr().Type(),
		}
		result[idx+offset] = s
	}

	return result
}

func (p *PlanOpPQLMultiGroupBy) Children() []types.PlanOperator {
	return []types.PlanOperator{}
}

func (p *PlanOpPQLMultiGroupBy) Iterator(ctx context.Context, row types.Row) (types.RowIterator, error) {
	iterators := make([]types.RowIterator, 0)

	for _, op := range p.operators {
		iter, err := op.Iterator(ctx, row)
		if err != nil {
			return nil, err
		}
		iterators = append(iterators, iter)
	}

	return &pqlMultiGroupByRowIter{
		planner:        p.planner,
		groupByColumns: p.groupByExprs,
		iterators:      iterators,
	}, nil
}

func (p *PlanOpPQLMultiGroupBy) WithChildren(children ...types.PlanOperator) (types.PlanOperator, error) {
	return nil, nil
}

func (p *PlanOpPQLMultiGroupBy) Expressions() []types.PlanExpression {
	return p.groupByExprs
}

func (p *PlanOpPQLMultiGroupBy) WithUpdatedExpressions(exprs ...types.PlanExpression) (types.PlanOperator, error) {
	if len(exprs) != len(p.groupByExprs) {
		return nil, sql3.NewErrInternalf("unexpected number of exprs '%d'", len(exprs))
	}
	return NewPlanOpPQLMultiGroupBy(p.planner, p.operators, exprs), nil
}

// pqlMultiGroupByRowIter is an iterator for the PlanOpPQLMultiGroupBy operator
// it provides rows consisting of the group by columns in the order they
// were specified and lastly the aggregates in the order they were specified
type pqlMultiGroupByRowIter struct {
	planner        *ExecutionPlanner
	groupByColumns []types.PlanExpression
	iterators      []types.RowIterator
	groupCache     map[string]types.Row

	groupKeys []string
}

var _ types.RowIterator = (*pqlMultiGroupByRowIter)(nil)

func (i *pqlMultiGroupByRowIter) Next(ctx context.Context) (types.Row, error) {
	if i.groupCache == nil {
		//consume all the rows from the child iterators
		i.groupCache = make(map[string]types.Row)
		if err := i.computeMultiGroupBy(ctx); err != nil {
			return nil, err
		}
	}

	if len(i.groupKeys) > 0 {
		key := i.groupKeys[0]

		row, ok := i.groupCache[key]
		if !ok {
			return nil, sql3.NewErrInternalf("unexpected absence of key")
		}
		// Move to next result element.
		i.groupKeys = i.groupKeys[1:]
		return row, nil
	}
	return nil, types.ErrNoMoreRows
}

func (i *pqlMultiGroupByRowIter) computeMultiGroupBy(ctx context.Context) error {
	//for each operator, consume all rows
	for iteratorIdx, iter := range i.iterators {

		//get the first row
		irow, err := iter.Next(ctx)
		if err != nil {
			if err == types.ErrNoMoreRows {
				continue
			}
			return err
		}

		for {
			//build a key for the group by columns for this row
			key, _, err := groupingKey(ctx, i.groupByColumns, irow)
			if err != nil {
				return err
			}

			aggIndex := iteratorIdx + len(i.groupByColumns)

			// get the group from the cache
			cachedRow, ok := i.groupCache[key]
			if ok {
				// if the group exists then update the row
				// NB: the aggregate for this iterator is at the end of irow
				cachedRow[aggIndex] = irow[len(irow)-1]
			} else {
				// if the group does not exist, add a row; set length to be number of group by columns + number of aggregates
				cachedRow := make([]interface{}, len(i.groupByColumns)+len(i.iterators))
				// copy the group by values into the new row
				for gidx := range i.groupByColumns {
					cachedRow[gidx] = irow[gidx]
				}
				// write the aggregate value in
				cachedRow[aggIndex] = irow[len(irow)-1]

				// write the row to the cache
				i.groupCache[key] = cachedRow
				if err != nil {
					return err
				}

				//record the new key
				i.groupKeys = append(i.groupKeys, key)
			}

			irow, err = iter.Next(ctx)
			if err != nil {
				if err == types.ErrNoMoreRows {
					break
				}
				return err
			}
		}
	}

	return nil
}
