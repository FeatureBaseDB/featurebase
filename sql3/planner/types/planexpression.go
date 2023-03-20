package types

import (
	"context"
	"fmt"

	"github.com/featurebasedb/featurebase/v3/sql3/parser"
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

// AggregationBuffer is an interface to something that maintains an aggregate
// during query execution.
type AggregationBuffer interface {
	Eval(ctx context.Context) (interface{}, error)
	Update(ctx context.Context, row Row) error
}

// Aggregable is an interface for an expression that is a an aggregate.
type Aggregable interface {
	fmt.Stringer

	// creates a new aggregation buffer for this aggregate
	NewBuffer() (AggregationBuffer, error)

	// convenience to get the first argument of the aggregate
	FirstChildExpr() PlanExpression

	// returns all the child expressions for this aggregate
	Children() []PlanExpression

	// returns the type of the aggregate
	Type() parser.ExprDataType
}

// IdentifiableByName is an interface for something that can be identified by a
// name.
type IdentifiableByName interface {
	Name() string
}
