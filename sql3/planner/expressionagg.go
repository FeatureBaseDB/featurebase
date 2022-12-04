// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"fmt"

	"github.com/molecula/featurebase/v3/pql"
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

func (n *countPlanExpression) String() string {
	return fmt.Sprintf("count(%s)", n.arg.String())
}

func (n *countPlanExpression) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_expr"] = fmt.Sprintf("%T", n)
	result["dataType"] = n.Type().TypeName()
	result["arg"] = n.arg.Plan()
	return result
}

func (n *countPlanExpression) Children() []types.PlanExpression {
	return []types.PlanExpression{
		n.arg,
	}
}

func (n *countPlanExpression) WithChildren(children ...types.PlanExpression) (types.PlanExpression, error) {
	if len(children) != 1 {
		return nil, sql3.NewErrInternalf("unexpected number of children '%d'", len(children))
	}
	return newCountPlanExpression(children[0], n.returnDataType), nil
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

func (n *countDistinctPlanExpression) String() string {
	return fmt.Sprintf("count(distinct %s)", n.arg.String())
}

func (n *countDistinctPlanExpression) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_expr"] = fmt.Sprintf("%T", n)
	result["dataType"] = n.Type().TypeName()
	result["arg"] = n.arg.Plan()
	return result
}

func (n *countDistinctPlanExpression) Children() []types.PlanExpression {
	return []types.PlanExpression{
		n.arg,
	}
}

func (n *countDistinctPlanExpression) WithChildren(children ...types.PlanExpression) (types.PlanExpression, error) {
	if len(children) != 1 {
		return nil, sql3.NewErrInternalf("unexpected number of children '%d'", len(children))
	}
	return newCountDistinctPlanExpression(children[0], n.returnDataType), nil
}

// aggregator for the SUM function
type aggregateSum struct {
	sum  interface{}
	expr types.PlanExpression
}

func NewAggSumBuffer(child types.PlanExpression) *aggregateSum {
	return &aggregateSum{
		expr: child,
	}
}

func (m *aggregateSum) Update(ctx context.Context, row types.Row) error {
	v, err := m.expr.Evaluate(row)
	if err != nil {
		return err
	}

	//if null, skip
	if v == nil {
		return nil
	}

	sumExpr, ok := m.expr.(*sumPlanExpression)
	if !ok {
		return sql3.NewErrInternalf("unexpected aggregate expression type '%T'", m.expr)
	}

	switch dataType := sumExpr.arg.Type().(type) {
	case *parser.DataTypeDecimal:
		val, ok := v.(pql.Decimal)
		if !ok {
			return sql3.NewErrInternalf("unexpected type conversion '%T'", v)
		}
		var dsum pql.Decimal
		if m.sum != nil {
			dsum, ok = m.sum.(pql.Decimal)
			if !ok {
				return sql3.NewErrInternalf("unexpected type conversion '%T'", m.sum)
			}
		} else {
			dsum = pql.NewDecimal(0, dataType.Scale)
		}
		dsum = pql.AddDecimal(dsum, val)
		m.sum = dsum
	default:
		return sql3.NewErrInternalf("unhandled aggregate expression datatype '%T'", dataType)
	}
	return nil
}

func (m *aggregateSum) Eval(ctx context.Context) (interface{}, error) {
	switch m.expr.Type().(type) {
	case *parser.DataTypeDecimal:
		dsum, ok := m.sum.(pql.Decimal)
		if !ok {
			return nil, sql3.NewErrInternalf("unexpected type conversion '%T'", m.sum)
		}
		return dsum, nil
	default:
		return nil, sql3.NewErrInternalf("unhandled aggregate expression datatype '%T'", m.expr.Type())
	}

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

func (n *sumPlanExpression) String() string {
	return fmt.Sprintf("sum(%s)", n.arg.String())
}

func (n *sumPlanExpression) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_expr"] = fmt.Sprintf("%T", n)
	result["dataType"] = n.Type().TypeName()
	result["arg"] = n.arg.Plan()
	return result
}

func (n *sumPlanExpression) Children() []types.PlanExpression {
	return []types.PlanExpression{
		n.arg,
	}
}

func (n *sumPlanExpression) WithChildren(children ...types.PlanExpression) (types.PlanExpression, error) {
	if len(children) != 1 {
		return nil, sql3.NewErrInternalf("unexpected number of children '%d'", len(children))
	}
	return newSumPlanExpression(children[0], n.returnDataType), nil
}

// aggregator for AVG()
type aggregateAvg struct {
	sum  interface{}
	rows int64
	expr types.PlanExpression
}

func NewAggAvgBuffer(child types.PlanExpression) *aggregateAvg {
	return &aggregateAvg{
		expr: child,
	}
}

func (a *aggregateAvg) Update(ctx context.Context, row types.Row) error {
	v, err := a.expr.Evaluate(row)
	if err != nil {
		return err
	}

	if v == nil {
		return nil
	}

	aggExpr, ok := a.expr.(*avgPlanExpression)
	if !ok {
		return sql3.NewErrInternalf("unexpected aggregate expression type '%T'", a.expr)
	}

	// we're going to do the sum in the return type
	switch returnType := aggExpr.returnDataType.(type) {
	case *parser.DataTypeDecimal:

		// get the current agg value
		var ok bool
		var aggVal pql.Decimal
		if a.sum == nil {
			aggVal = pql.NewDecimal(0, returnType.Scale)
		} else {
			aggVal, ok = a.sum.(pql.Decimal)
			if !ok {
				return sql3.NewErrInternalf("unexpected type conversion '%T'", v)
			}
		}

		switch dataType := aggExpr.arg.Type().(type) {
		case *parser.DataTypeDecimal:
			thisVal, ok := v.(pql.Decimal)
			if !ok {
				return sql3.NewErrInternalf("unexpected type conversion '%T'", v)
			}

			a.sum = pql.AddDecimal(thisVal, aggVal)

		case *parser.DataTypeInt:
			thisIVal, ok := v.(int64)
			if !ok {
				return sql3.NewErrInternalf("unexpected type conversion '%T'", v)
			}

			thisVal := pql.FromInt64(thisIVal, returnType.Scale)
			a.sum = pql.AddDecimal(thisVal, aggVal)

		default:
			return sql3.NewErrInternalf("unhandled aggregate expression datatype '%T'", dataType)
		}
	default:
		return sql3.NewErrInternalf("unhandled aggregate expression datatype '%T'", returnType)
	}
	a.rows += 1

	return nil
}

func (a *aggregateAvg) Eval(ctx context.Context) (interface{}, error) {
	// bail if we have no aggregate at all
	if a.sum == nil && a.rows == 0 {
		return nil, nil
	}

	aggExpr, ok := a.expr.(*avgPlanExpression)
	if !ok {
		return nil, sql3.NewErrInternalf("unexpected aggregate expression type '%T'", a.expr)
	}

	switch returnType := aggExpr.returnDataType.(type) {
	case *parser.DataTypeDecimal:

		// if no rows, average is 0
		if a.rows == 0 {
			return pql.NewDecimal(0, returnType.Scale), nil
		}
		count := pql.FromInt64(a.rows, returnType.Scale)

		sum, ok := a.sum.(pql.Decimal)
		if !ok {
			return nil, sql3.NewErrInternalf("unexpected type conversion '%T'", a.sum)
		}
		return pql.DivideDecimal(sum, count), nil

	default:
		return nil, sql3.NewErrInternalf("unhandled aggregate expression datatype '%T'", returnType)
	}

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

func (n *avgPlanExpression) String() string {
	return fmt.Sprintf("avg(%s)", n.arg.String())
}

func (n *avgPlanExpression) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_expr"] = fmt.Sprintf("%T", n)
	result["dataType"] = n.Type().TypeName()
	result["arg"] = n.arg.Plan()
	return result
}

func (n *avgPlanExpression) Children() []types.PlanExpression {
	return []types.PlanExpression{
		n.arg,
	}
}

func (n *avgPlanExpression) WithChildren(children ...types.PlanExpression) (types.PlanExpression, error) {
	if len(children) != 1 {
		return nil, sql3.NewErrInternalf("unexpected number of children '%d'", len(children))
	}
	return newAvgPlanExpression(children[0], n.returnDataType), nil
}

// aggregator for MIN()
type aggregateMin struct {
	val  interface{}
	expr types.PlanExpression
}

func NewAggMinBuffer(child types.PlanExpression) *aggregateMin {
	return &aggregateMin{nil, child}
}

func (m *aggregateMin) Update(ctx context.Context, row types.Row) error {
	v, err := m.expr.Evaluate(row)
	if err != nil {
		return err
	}

	// skip if nil
	if v == nil {
		return nil
	}

	// if we have no min, then set the value
	if m.val == nil {
		m.val = v
		return nil
	}

	aggExpr, ok := m.expr.(*minPlanExpression)
	if !ok {
		return sql3.NewErrInternalf("unexpected aggregate expression type '%T'", m.expr)
	}

	switch dataType := aggExpr.arg.Type().(type) {
	case *parser.DataTypeDecimal:
		thisVal, ok := v.(pql.Decimal)
		if !ok {
			return sql3.NewErrInternalf("unexpected type conversion '%T'", v)
		}

		aggVal, ok := m.val.(pql.Decimal)
		if !ok {
			return sql3.NewErrInternalf("unexpected type conversion '%T'", v)
		}

		if thisVal.LessThan(aggVal) {
			m.val = thisVal
		}

	case *parser.DataTypeInt:
		thisVal, ok := v.(int64)
		if !ok {
			return sql3.NewErrInternalf("unexpected type conversion '%T'", v)
		}

		aggVal, ok := m.val.(int64)
		if !ok {
			return sql3.NewErrInternalf("unexpected type conversion '%T'", v)
		}

		if thisVal < aggVal {
			m.val = thisVal
		}

	default:
		return sql3.NewErrInternalf("unhandled aggregate expression datatype '%T'", dataType)
	}
	return nil
}

func (m *aggregateMin) Eval(ctx context.Context) (interface{}, error) {
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

func (n *minPlanExpression) String() string {
	return fmt.Sprintf("min(%s)", n.arg.String())
}

func (n *minPlanExpression) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_expr"] = fmt.Sprintf("%T", n)
	result["dataType"] = n.Type().TypeName()
	result["arg"] = n.arg.Plan()
	return result
}

func (n *minPlanExpression) Children() []types.PlanExpression {
	return []types.PlanExpression{
		n.arg,
	}
}

func (n *minPlanExpression) WithChildren(children ...types.PlanExpression) (types.PlanExpression, error) {
	if len(children) != 1 {
		return nil, sql3.NewErrInternalf("unexpected number of children '%d'", len(children))
	}
	return newMinPlanExpression(children[0], n.returnDataType), nil
}

// aggregator for MAX()
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

	// skip if nil
	if v == nil {
		return nil
	}

	// if we have no min, then set the value
	if m.val == nil {
		m.val = v
		return nil
	}

	aggExpr, ok := m.expr.(*maxPlanExpression)
	if !ok {
		return sql3.NewErrInternalf("unexpected aggregate expression type '%T'", m.expr)
	}

	switch dataType := aggExpr.arg.Type().(type) {
	case *parser.DataTypeDecimal:
		thisVal, ok := v.(pql.Decimal)
		if !ok {
			return sql3.NewErrInternalf("unexpected type conversion '%T'", v)
		}

		aggVal, ok := m.val.(pql.Decimal)
		if !ok {
			return sql3.NewErrInternalf("unexpected type conversion '%T'", v)
		}

		if thisVal.GreaterThan(aggVal) {
			m.val = thisVal
		}

	case *parser.DataTypeInt:
		thisVal, ok := v.(int64)
		if !ok {
			return sql3.NewErrInternalf("unexpected type conversion '%T'", v)
		}

		aggVal, ok := m.val.(int64)
		if !ok {
			return sql3.NewErrInternalf("unexpected type conversion '%T'", v)
		}

		if thisVal > aggVal {
			m.val = thisVal
		}

	default:
		return sql3.NewErrInternalf("unhandled aggregate expression datatype '%T'", dataType)
	}

	return nil
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

func (n *maxPlanExpression) String() string {
	return fmt.Sprintf("max(%s)", n.arg.String())
}

func (n *maxPlanExpression) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_expr"] = fmt.Sprintf("%T", n)
	result["dataType"] = n.Type().TypeName()
	result["arg"] = n.arg.Plan()
	return result
}

func (n *maxPlanExpression) Children() []types.PlanExpression {
	return []types.PlanExpression{
		n.arg,
	}
}

func (n *maxPlanExpression) WithChildren(children ...types.PlanExpression) (types.PlanExpression, error) {
	if len(children) != 1 {
		return nil, sql3.NewErrInternalf("unexpected number of children '%d'", len(children))
	}
	return newMaxPlanExpression(children[0], n.returnDataType), nil
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

func (n *percentilePlanExpression) String() string {
	return fmt.Sprintf("percentile(%s)", n.arg.String())
}

func (n *percentilePlanExpression) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_expr"] = fmt.Sprintf("%T", n)
	result["dataType"] = n.Type().TypeName()
	result["arg"] = n.arg.Plan()
	result["ntharg"] = n.nthArg.Plan()
	return result
}

func (n *percentilePlanExpression) Children() []types.PlanExpression {
	return []types.PlanExpression{
		n.arg,
		n.nthArg,
	}
}

func (n *percentilePlanExpression) WithChildren(children ...types.PlanExpression) (types.PlanExpression, error) {
	if len(children) != 2 {
		return nil, sql3.NewErrInternalf("unexpected number of children '%d'", len(children))
	}
	return newPercentilePlanExpression(children[0], children[1], n.returnDataType), nil
}

// aggregator for LAST()
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

// aggregator for FIRST()
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
