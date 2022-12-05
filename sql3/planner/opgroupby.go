// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"fmt"
	"hash/maphash"

	"github.com/featurebasedb/featurebase/v3/errors"
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
		warnings:     make([]string, 0),
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
			ColumnName:   ref.columnName,
			RelationName: ref.tableName,
			Type:         expr.Type(),
		}
		result[idx] = s
	}
	offset := len(p.GroupByExprs)
	for idx, agg := range p.Aggregates {
		s := &types.PlannerColumn{
			ColumnName:   agg.String(),
			RelationName: "",
			Type:         agg.Type(),
		}
		result[idx+offset] = s
	}

	return result
}

func (p *PlanOpGroupBy) Iterator(ctx context.Context, row types.Row) (types.RowIterator, error) {
	i, err := p.ChildOp.Iterator(ctx, row)
	if err != nil {
		return nil, err
	}
	if len(p.GroupByExprs) == 0 {
		return newGroupByIter(ctx, p.Aggregates, i), nil
	} else {
		return newGroupByGroupingIter(ctx, p.Aggregates, p.GroupByExprs, i), nil
	}
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

func (p *PlanOpGroupBy) Expressions() []types.PlanExpression {
	result := []types.PlanExpression{}
	result = append(result, p.Aggregates...)
	return result
}

func (p *PlanOpGroupBy) WithUpdatedExpressions(exprs ...types.PlanExpression) (types.PlanOperator, error) {
	if len(exprs) != 1 {
		return nil, sql3.NewErrInternalf("unexpected number of exprs '%d'", len(exprs))
	}
	return NewPlanOpGroupBy(exprs, p.GroupByExprs, p.ChildOp), nil
}

func (p *PlanOpGroupBy) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_op"] = fmt.Sprintf("%T", p)
	sc := make([]string, 0)
	for _, e := range p.Schema() {
		sc = append(sc, fmt.Sprintf("'%s', '%s', '%s'", e.ColumnName, e.RelationName, e.Type.TypeDescription()))
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
	aggregates         []types.PlanExpression
	child              types.RowIterator
	ctx                context.Context
	aggregationBuffers *keysAndAggregations
	done               bool
}

func newGroupByIter(ctx context.Context, aggregates []types.PlanExpression, child types.RowIterator) *groupByIter {
	return &groupByIter{
		aggregates: aggregates,
		child:      child,
		ctx:        ctx,
		aggregationBuffers: &keysAndAggregations{
			buffers: make([]types.AggregationBuffer, len(aggregates)),
		},
	}
}

func (i *groupByIter) Next(ctx context.Context) (types.Row, error) {
	if i.done {
		return nil, types.ErrNoMoreRows
	}

	i.done = true

	var err error
	for j, a := range i.aggregates {
		i.aggregationBuffers.buffers[j], err = newAggregationBuffer(a)
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

		if err := updateBuffers(ctx, i.aggregationBuffers, row); err != nil {
			return nil, err
		}
	}

	return evalBuffers(ctx, i.aggregationBuffers)
}

type keysAndAggregations struct {
	groupByKeys []interface{}
	buffers     []types.AggregationBuffer
}

type groupByGroupingIter struct {
	aggregates   []types.PlanExpression
	groupByExprs []types.PlanExpression
	aggregations ObjectCache
	keys         []uint64
	child        types.RowIterator
}

func newGroupByGroupingIter(ctx context.Context, aggregates, groupByExprs []types.PlanExpression, child types.RowIterator) *groupByGroupingIter {
	return &groupByGroupingIter{
		aggregates:   aggregates,
		groupByExprs: groupByExprs,
		child:        child,
	}
}

func (i *groupByGroupingIter) Next(ctx context.Context) (types.Row, error) {
	if i.aggregations == nil {
		i.aggregations = NewMapObjectCache()
		if err := i.compute(ctx); err != nil {
			return nil, err
		}
	}

	if len(i.keys) > 0 {
		buffers, err := i.get(i.keys[0])
		if err != nil {
			return nil, err
		}

		i.keys = i.keys[1:]

		aggRow, err := evalBuffers(ctx, buffers)
		if err != nil {
			return nil, err
		}

		var row = make(types.Row, len(i.groupByExprs)+len(aggRow))
		copy(row, buffers.groupByKeys)
		copy(row[len(buffers.groupByKeys):], aggRow)
		return row, nil
	}
	return nil, types.ErrNoMoreRows
}

func (i *groupByGroupingIter) compute(ctx context.Context) error {
	for {
		row, err := i.child.Next(ctx)
		if err != nil {
			if err == types.ErrNoMoreRows {
				break
			}
			return err
		}

		key, keyValues, err := groupingKeyHash(ctx, i.groupByExprs, row)
		if err != nil {
			return err
		}

		b, err := i.get(key)
		if errors.Is(err, sql3.ErrCacheKeyNotFound) {
			b = &keysAndAggregations{}
			b.buffers = make([]types.AggregationBuffer, len(i.aggregates))
			for j, a := range i.aggregates {
				b.buffers[j], err = newAggregationBuffer(a)
				if err != nil {
					return err
				}
			}
			b.groupByKeys = keyValues
			if err := i.aggregations.PutObject(key, b); err != nil {
				return err
			}
			i.keys = append(i.keys, key)
		} else if err != nil {
			return err
		}

		err = updateBuffers(ctx, b, row)
		if err != nil {
			return err
		}
	}
	return nil
}

func (i *groupByGroupingIter) get(key uint64) (*keysAndAggregations, error) {
	v, err := i.aggregations.GetObject(key)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, nil
	}
	return v.(*keysAndAggregations), err
}

func newAggregationBuffer(expr types.PlanExpression) (types.AggregationBuffer, error) {
	switch n := expr.(type) {
	case types.Aggregable:
		return n.NewBuffer()
	default:
		return NewAggLastBuffer(expr), nil
	}
}

func updateBuffers(ctx context.Context, buffers *keysAndAggregations, row types.Row) error {
	for _, b := range buffers.buffers {
		if err := b.Update(ctx, row); err != nil {
			return err
		}
	}
	return nil
}

func evalBuffers(ctx context.Context, aggregationBuffers *keysAndAggregations) (types.Row, error) {
	var row = make(types.Row, len(aggregationBuffers.buffers))
	var err error
	for i, b := range aggregationBuffers.buffers {
		row[i], err = b.Eval(ctx)
		if err != nil {
			return nil, err
		}
	}
	return row, nil
}

func groupingKeyHash(ctx context.Context, groupByExprs []types.PlanExpression, row types.Row) (uint64, types.Row, error) {
	rowKeys := make([]interface{}, len(groupByExprs))
	var hash maphash.Hash
	hash.SetSeed(prototypeHash.Seed())
	for i, expr := range groupByExprs {
		v, err := expr.Evaluate(row)
		if err != nil {
			return 0, nil, err
		}
		_, err = hash.Write(([]byte)(fmt.Sprintf("%#v,", v)))
		if err != nil {
			return 0, nil, err
		}
		rowKeys[i] = v
	}
	result := hash.Sum64()
	return result, rowKeys, nil
}
