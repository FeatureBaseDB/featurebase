package types

import (
	"context"
	"fmt"

	"github.com/featurebasedb/featurebase/v3/sql3/parser"
)

// TODO(pok) we can get rid of this - we have expression types for all of these now...
type AggregateFunctionType int

// The list of AggregateFunction.
const (
	// Special tokens
	AGGREGATE_ILLEGAL AggregateFunctionType = iota
	AGGREGATE_COUNT
	AGGREGATE_COUNT_DISTINCT
	AGGREGATE_SUM
	AGGREGATE_AVG
	AGGREGATE_PERCENTILE
	AGGREGATE_MIN
	AGGREGATE_MAX
)

// PlanExpression is an expression node for an execution plan
type PlanExpression interface {
	fmt.Stringer

	// evaluates expression based on current row
	Evaluate(currentRow []interface{}) (interface{}, error)

	// returns the type of the expression
	Type() parser.ExprDataType

	// returns the child expressions for this expression
	Children() []PlanExpression

	// creates a new expression node with the children replaced
	WithChildren(children ...PlanExpression) (PlanExpression, error)

	// returns a map containing a rich description of this expression; intended to be
	// marshalled into json
	Plan() map[string]interface{}
}

// Aggregattion buffer is an interface to something that maintains an aggregate during query
// execution
type AggregationBuffer interface {
	Eval(ctx context.Context) (interface{}, error)
	Update(ctx context.Context, row Row) error
}

// interface to an expression that is a an aggregate
type Aggregable interface {
	fmt.Stringer

	NewBuffer() (AggregationBuffer, error)
	AggType() AggregateFunctionType
	AggExpression() PlanExpression
	AggAdditionalExpr() []PlanExpression
}

// interface to something that can be identified by a name
type IdentifiableByName interface {
	Name() string
}
