// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"fmt"
	"reflect"

	"github.com/molecula/featurebase/v3/sql3"
	"github.com/molecula/featurebase/v3/sql3/parser"
	"github.com/molecula/featurebase/v3/sql3/planner/types"
)

// aggregator for the COUNT function
type aggregateCount struct {
	count int64
	expr  types.PlanExpression
}

func NewAggCountBuffer(child types.PlanExpression) *aggregateCount {
	return &aggregateCount{0, child}
}

func (c *aggregateCount) Update(ctx context.Context, row types.Row) error {
	var inc bool
	v, err := c.expr.Evaluate(row)
	if v != nil {
		inc = true
	}

	if err != nil {
		return err
	}

	if inc {
		c.count += 1
	}
	return nil
}

func (c *aggregateCount) Eval(ctx context.Context) (interface{}, error) {
	return c.count, nil
}

// aggregator for the COUNT DISTINCT function
type aggregateCountDistinct struct {
	valueSeen map[string]struct{}
	expr      types.PlanExpression
}

func NewAggCountDistinctBuffer(child types.PlanExpression) *aggregateCountDistinct {
	return &aggregateCountDistinct{make(map[string]struct{}), child}
}

func (c *aggregateCountDistinct) Update(ctx context.Context, row types.Row) error {
	var value interface{}
	v, err := c.expr.Evaluate(row)
	if v == nil {
		return nil
	}

	if err != nil {
		return err
	}

	value = v

	hash := fmt.Sprintf("%v", value)
	c.valueSeen[hash] = struct{}{}

	return nil
}

func (c *aggregateCountDistinct) Eval(ctx context.Context) (interface{}, error) {
	return int64(len(c.valueSeen)), nil
}

// countPlanExpression handles COUNT()
type countPlanExpression struct {
	arg            types.PlanExpression
	returnDataType parser.ExprDataType
}

var _ types.Aggregable = (*countPlanExpression)(nil)

func newCountPlanExpression(arg types.PlanExpression, returnDataType parser.ExprDataType) *countPlanExpression {
	return &countPlanExpression{
		arg:            arg,
		returnDataType: returnDataType,
	}
}

func (n *countPlanExpression) Evaluate(currentRow []interface{}) (interface{}, error) {
	arg, ok := n.arg.(*qualifiedRefPlanExpression)
	if !ok {
		return nil, sql3.NewErrInternalf("unexpected aggregate function arg type '%T'", n.arg)
	}
	return currentRow[arg.columnIndex], nil
}

func (n *countPlanExpression) NewBuffer() (types.AggregationBuffer, error) {
	return NewAggCountBuffer(n), nil
}

func (n *countPlanExpression) AggType() types.AggregateFunctionType {
	return types.AGGREGATE_COUNT
}

func (n *countPlanExpression) AggExpression() types.PlanExpression {
	return n.arg
}

func (n *countPlanExpression) AggAdditionalExpr() []types.PlanExpression {
	return []types.PlanExpression{}
}

func (n *countPlanExpression) Type() parser.ExprDataType {
	return n.returnDataType
}

func (n *countPlanExpression) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_expr"] = fmt.Sprintf("%T", n)
	result["dataType"] = n.Type().TypeName()
	result["arg"] = n.arg.Plan()
	return result
}

func (n *countPlanExpression) Children() []types.PlanExpression {
	return []types.PlanExpression{}
}

func (n *countPlanExpression) WithChildren(children ...types.PlanExpression) (types.PlanExpression, error) {
	return n, nil
}

// countDistinctPlanExpression handles COUNT(DISTINCT)
type countDistinctPlanExpression struct {
	arg            types.PlanExpression
	returnDataType parser.ExprDataType
}

var _ types.Aggregable = (*countDistinctPlanExpression)(nil)

func newCountDistinctPlanExpression(arg types.PlanExpression, returnDataType parser.ExprDataType) *countDistinctPlanExpression {
	return &countDistinctPlanExpression{
		arg:            arg,
		returnDataType: returnDataType,
	}
}

func (n *countDistinctPlanExpression) Evaluate(currentRow []interface{}) (interface{}, error) {
	arg, ok := n.arg.(*qualifiedRefPlanExpression)
	if !ok {
		return nil, sql3.NewErrInternalf("unexpected aggregate function arg type '%T'", n.arg)
	}
	return currentRow[arg.columnIndex], nil
}

func (n *countDistinctPlanExpression) NewBuffer() (types.AggregationBuffer, error) {
	return NewAggCountDistinctBuffer(n), nil
}

func (n *countDistinctPlanExpression) AggType() types.AggregateFunctionType {
	return types.AGGREGATE_COUNT_DISTINCT
}

func (n *countDistinctPlanExpression) AggExpression() types.PlanExpression {
	return n.arg
}

func (n *countDistinctPlanExpression) AggAdditionalExpr() []types.PlanExpression {
	return []types.PlanExpression{}
}

func (n *countDistinctPlanExpression) Type() parser.ExprDataType {
	return n.returnDataType
}

func (n *countDistinctPlanExpression) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_expr"] = fmt.Sprintf("%T", n)
	result["dataType"] = n.Type().TypeName()
	result["arg"] = n.arg.Plan()
	return result
}

func (n *countDistinctPlanExpression) Children() []types.PlanExpression {
	return nil
}

func (n *countDistinctPlanExpression) WithChildren(children ...types.PlanExpression) (types.PlanExpression, error) {
	return n, nil
}

// aggregator for the SUM function
type aggregateSum struct {
	isnil bool
	sum   float64
	expr  types.PlanExpression
}

func NewAggSumBuffer(child types.PlanExpression) *aggregateSum {
	return &aggregateSum{true, float64(0), child}
}

func (m *aggregateSum) Update(ctx context.Context, row types.Row) error {
	v, err := m.expr.Evaluate(row)
	if err != nil {
		return err
	}

	if v == nil {
		return nil
	}

	var val interface{} = 0

	if m.isnil {
		m.sum = 0
		m.isnil = false
	}

	m.sum += val.(float64)

	//return nil
	return sql3.NewErrInternalf("implement me")
}

func (m *aggregateSum) Eval(ctx context.Context) (interface{}, error) {
	if m.isnil {
		return nil, nil
	}
	return m.sum, nil
}

// sumPlanExpression handles SUM()
type sumPlanExpression struct {
	arg            types.PlanExpression
	returnDataType parser.ExprDataType
}

var _ types.Aggregable = (*sumPlanExpression)(nil)

func newSumPlanExpression(arg types.PlanExpression, returnDataType parser.ExprDataType) *sumPlanExpression {
	return &sumPlanExpression{
		arg:            arg,
		returnDataType: returnDataType,
	}
}

func (n *sumPlanExpression) Evaluate(currentRow []interface{}) (interface{}, error) {
	arg, ok := n.arg.(*qualifiedRefPlanExpression)
	if !ok {
		return nil, sql3.NewErrInternalf("unexpected aggregate function arg type '%T'", n.arg)
	}
	return currentRow[arg.columnIndex], nil
}

func (n *sumPlanExpression) NewBuffer() (types.AggregationBuffer, error) {
	return NewAggSumBuffer(n), nil
}

func (n *sumPlanExpression) AggType() types.AggregateFunctionType {
	return types.AGGREGATE_SUM
}

func (n *sumPlanExpression) AggExpression() types.PlanExpression {
	return n.arg
}

func (n *sumPlanExpression) AggAdditionalExpr() []types.PlanExpression {
	return []types.PlanExpression{}
}

func (n *sumPlanExpression) Type() parser.ExprDataType {
	return n.returnDataType
}

func (n *sumPlanExpression) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_expr"] = fmt.Sprintf("%T", n)
	result["dataType"] = n.Type().TypeName()
	result["arg"] = n.arg.Plan()
	return result
}

func (n *sumPlanExpression) Children() []types.PlanExpression {
	return nil
}

func (n *sumPlanExpression) WithChildren(children ...types.PlanExpression) (types.PlanExpression, error) {
	return n, nil
}

// aggregator for AVG
type aggregateAvg struct {
	sum  float64
	rows int64
	expr types.PlanExpression
}

func NewAggAvgBuffer(child types.PlanExpression) *aggregateAvg {
	const (
		sum  = float64(0)
		rows = int64(0)
	)
	return &aggregateAvg{sum, rows, child}
}

func (a *aggregateAvg) Update(ctx context.Context, row types.Row) error {
	v, err := a.expr.Evaluate(row)
	if err != nil {
		return err
	}

	if v == nil {
		return nil
	}
	a.sum += v.(float64)
	a.rows += 1

	//return nil
	return sql3.NewErrInternalf("implement me")
}

func (a *aggregateAvg) Eval(ctx context.Context) (interface{}, error) {
	// This case is triggered when no rows exist.
	if a.sum == 0 && a.rows == 0 {
		return nil, nil
	}

	if a.rows == 0 {
		return float64(0), nil
	}

	return a.sum / float64(a.rows), nil
}

// avgPlanExpression handles AVG()
type avgPlanExpression struct {
	arg            types.PlanExpression
	returnDataType parser.ExprDataType
}

var _ types.Aggregable = (*avgPlanExpression)(nil)

func newAvgPlanExpression(arg types.PlanExpression, returnDataType parser.ExprDataType) *avgPlanExpression {
	return &avgPlanExpression{
		arg:            arg,
		returnDataType: returnDataType,
	}
}

func (n *avgPlanExpression) Evaluate(currentRow []interface{}) (interface{}, error) {
	arg, ok := n.arg.(*qualifiedRefPlanExpression)
	if !ok {
		return nil, sql3.NewErrInternalf("unexpected aggregate function arg type '%T'", n.arg)
	}
	return currentRow[arg.columnIndex], nil
}

func (n *avgPlanExpression) NewBuffer() (types.AggregationBuffer, error) {
	return NewAggAvgBuffer(n), nil
}

func (n *avgPlanExpression) AggType() types.AggregateFunctionType {
	return types.AGGREGATE_AVG
}

func (n *avgPlanExpression) AggExpression() types.PlanExpression {
	return n.arg
}

func (n *avgPlanExpression) AggAdditionalExpr() []types.PlanExpression {
	return []types.PlanExpression{}
}

func (n *avgPlanExpression) Type() parser.ExprDataType {
	return n.returnDataType
}

func (n *avgPlanExpression) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_expr"] = fmt.Sprintf("%T", n)
	result["dataType"] = n.Type().TypeName()
	result["arg"] = n.arg.Plan()
	return result
}

func (n *avgPlanExpression) Children() []types.PlanExpression {
	return nil
}

func (n *avgPlanExpression) WithChildren(children ...types.PlanExpression) (types.PlanExpression, error) {
	return n, nil
}

// aggregator for MIN
type aggreagateMin struct {
	val  interface{}
	expr types.PlanExpression
}

func NewAggMinBuffer(child types.PlanExpression) *aggreagateMin {
	return &aggreagateMin{nil, child}
}

func (m *aggreagateMin) Update(ctx context.Context, row types.Row) error {
	v, err := m.expr.Evaluate(row)
	if err != nil {
		return err
	}

	if reflect.TypeOf(v) == nil {
		return nil
	}

	if m.val == nil {
		m.val = v
		return nil
	}

	//return nil
	return sql3.NewErrInternalf("implement me")

}

func (m *aggreagateMin) Eval(ctx context.Context) (interface{}, error) {
	return m.val, nil
}

// minPlanExpression handles MIN()
type minPlanExpression struct {
	arg            types.PlanExpression
	returnDataType parser.ExprDataType
}

var _ types.Aggregable = (*minPlanExpression)(nil)

func newMinPlanExpression(arg types.PlanExpression, returnDataType parser.ExprDataType) *minPlanExpression {
	return &minPlanExpression{
		arg:            arg,
		returnDataType: returnDataType,
	}
}

func (n *minPlanExpression) Evaluate(currentRow []interface{}) (interface{}, error) {
	arg, ok := n.arg.(*qualifiedRefPlanExpression)
	if !ok {
		return nil, sql3.NewErrInternalf("unexpected aggregate function arg type '%T'", n.arg)
	}
	return currentRow[arg.columnIndex], nil
}

func (n *minPlanExpression) NewBuffer() (types.AggregationBuffer, error) {
	return NewAggMinBuffer(n), nil
}

func (n *minPlanExpression) AggType() types.AggregateFunctionType {
	return types.AGGREGATE_MIN
}

func (n *minPlanExpression) AggExpression() types.PlanExpression {
	return n.arg
}

func (n *minPlanExpression) AggAdditionalExpr() []types.PlanExpression {
	return []types.PlanExpression{}
}

func (n *minPlanExpression) Type() parser.ExprDataType {
	return n.returnDataType
}

func (n *minPlanExpression) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_expr"] = fmt.Sprintf("%T", n)
	result["dataType"] = n.Type().TypeName()
	result["arg"] = n.arg.Plan()
	return result
}

func (n *minPlanExpression) Children() []types.PlanExpression {
	return nil
}

func (n *minPlanExpression) WithChildren(children ...types.PlanExpression) (types.PlanExpression, error) {
	return n, nil
}

// aggregator for MAX
type aggregateMax struct {
	val  interface{}
	expr types.PlanExpression
}

func NewAggMaxBuffer(child types.PlanExpression) *aggregateMax {
	return &aggregateMax{nil, child}
}

func (m *aggregateMax) Update(ctx context.Context, row types.Row) error {
	v, err := m.expr.Evaluate(row)
	if err != nil {
		return err
	}

	if reflect.TypeOf(v) == nil {
		return nil
	}

	if m.val == nil {
		m.val = v
		return nil
	}

	//return nil
	return sql3.NewErrInternalf("implement me")

}

func (m *aggregateMax) Eval(ctx context.Context) (interface{}, error) {
	return m.val, nil
}

// maxPlanExpression handles MAX()
type maxPlanExpression struct {
	arg            types.PlanExpression
	returnDataType parser.ExprDataType
}

var _ types.Aggregable = (*maxPlanExpression)(nil)

func newMaxPlanExpression(arg types.PlanExpression, returnDataType parser.ExprDataType) *maxPlanExpression {
	return &maxPlanExpression{
		arg:            arg,
		returnDataType: returnDataType,
	}
}

func (n *maxPlanExpression) Evaluate(currentRow []interface{}) (interface{}, error) {
	arg, ok := n.arg.(*qualifiedRefPlanExpression)
	if !ok {
		return nil, sql3.NewErrInternalf("unexpected aggregate function arg type '%T'", n.arg)
	}
	return currentRow[arg.columnIndex], nil
}

func (n *maxPlanExpression) NewBuffer() (types.AggregationBuffer, error) {
	return NewAggMaxBuffer(n), nil
}

func (n *maxPlanExpression) AggType() types.AggregateFunctionType {
	return types.AGGREGATE_MAX
}

func (n *maxPlanExpression) AggExpression() types.PlanExpression {
	return n.arg
}

func (n *maxPlanExpression) AggAdditionalExpr() []types.PlanExpression {
	return []types.PlanExpression{}
}

func (n *maxPlanExpression) Type() parser.ExprDataType {
	return n.returnDataType
}

func (n *maxPlanExpression) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_expr"] = fmt.Sprintf("%T", n)
	result["dataType"] = n.Type().TypeName()
	result["arg"] = n.arg.Plan()
	return result
}

func (n *maxPlanExpression) Children() []types.PlanExpression {
	return nil
}

func (n *maxPlanExpression) WithChildren(children ...types.PlanExpression) (types.PlanExpression, error) {
	return n, nil
}

// percentilePlanExpression handles PERCENTILE()
type percentilePlanExpression struct {
	arg            types.PlanExpression
	nthArg         types.PlanExpression
	returnDataType parser.ExprDataType
}

var _ types.Aggregable = (*percentilePlanExpression)(nil)

func newPercentilePlanExpression(arg types.PlanExpression, nthArg types.PlanExpression, returnDataType parser.ExprDataType) *percentilePlanExpression {
	return &percentilePlanExpression{
		arg:            arg,
		nthArg:         nthArg,
		returnDataType: returnDataType,
	}
}

func (n *percentilePlanExpression) Evaluate(currentRow []interface{}) (interface{}, error) {
	arg, ok := n.arg.(*qualifiedRefPlanExpression)
	if !ok {
		return nil, sql3.NewErrInternalf("unexpected aggregate function arg type '%T'", n.arg)
	}
	return currentRow[arg.columnIndex], nil
}

func (n *percentilePlanExpression) NewBuffer() (types.AggregationBuffer, error) {
	return NewAggCountBuffer(n), nil
}

func (n *percentilePlanExpression) AggType() types.AggregateFunctionType {
	return types.AGGREGATE_PERCENTILE
}

func (n *percentilePlanExpression) AggExpression() types.PlanExpression {
	return n.arg
}

func (n *percentilePlanExpression) AggAdditionalExpr() []types.PlanExpression {
	return []types.PlanExpression{
		n.nthArg,
	}
}

func (n *percentilePlanExpression) Type() parser.ExprDataType {
	return n.returnDataType
}

func (n *percentilePlanExpression) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_expr"] = fmt.Sprintf("%T", n)
	result["dataType"] = n.Type().TypeName()
	result["arg"] = n.arg.Plan()
	return result
}

func (n *percentilePlanExpression) Children() []types.PlanExpression {
	return nil
}

func (n *percentilePlanExpression) WithChildren(children ...types.PlanExpression) (types.PlanExpression, error) {
	return n, nil
}

//aggregator for last
type aggregateLast struct {
	val  interface{}
	expr types.PlanExpression
}

func NewAggLastBuffer(child types.PlanExpression) *aggregateLast {
	return &aggregateLast{nil, child}
}

func (l *aggregateLast) Update(ctx context.Context, row types.Row) error {
	v, err := l.expr.Evaluate(row)
	if err != nil {
		return err
	}

	if v == nil {
		return nil
	}

	l.val = v
	return nil
}

func (l *aggregateLast) Eval(ctx context.Context) (interface{}, error) {
	return l.val, nil
}

// aggregator for first
type aggregateFirst struct {
	val  interface{}
	expr types.PlanExpression
}

func NewFirstBuffer(child types.PlanExpression) *aggregateFirst {
	return &aggregateFirst{nil, child}
}

func (f *aggregateFirst) Update(ctx context.Context, row types.Row) error {
	if f.val != nil {
		return nil
	}

	v, err := f.expr.Evaluate(row)
	if err != nil {
		return err
	}

	if v == nil {
		return nil
	}

	f.val = v

	return nil
}

func (f *aggregateFirst) Eval(ctx context.Context) (interface{}, error) {
	return f.val, nil
}
