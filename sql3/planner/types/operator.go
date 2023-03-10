package types

import (
	"context"
	"errors"
	"fmt"

	"github.com/featurebasedb/featurebase/v3/sql3/parser"
)

// PlanOperator is a node in an execution plan.
type PlanOperator interface {
	fmt.Stringer

	// Children of this operator.
	Children() []PlanOperator

	// Schema of this operator.
	Schema() Schema

	// Iterator produces an iterator from this node. The current row being
	// evaluated is provided, as well as the context of the query.
	Iterator(ctx context.Context, row Row) (RowIterator, error)

	// WithChildren creates a new node with the children passed
	WithChildren(children ...PlanOperator) (PlanOperator, error)

	// Plan returns a map containing a rich description of this operator;
	// intended to be marshalled into json.
	Plan() map[string]interface{}

	// AddWarning adds a warning to the plan.
	AddWarning(warning string)

	// Warnings returns a list of warnings as strings.
	Warnings() []string
}

// ContainsExpressions exposes expressions in plan operators
type ContainsExpressions interface {
	// returns the list of expressions contained by the plan operator
	Expressions() []PlanExpression

	// WithUpdatedExpressions returns a the operator with expressions updated
	WithUpdatedExpressions(exprs ...PlanExpression) (PlanOperator, error)
}

// PlannerColumn is the definition of a column returned as a set from each operator
type PlannerColumn struct {
	ColumnName   string
	RelationName string
	AliasName    string
	Type         parser.ExprDataType
}

// Relation is an interface to something that can be treated as a relation
type Relation interface {
	Name() string
}

// FilteredRelation is an interface to something that can be treated as a relation that can be filtered
type FilteredRelation interface {
	Relation
	IsFilterable() bool
	UpdateFilters(filterCondition PlanExpression) (PlanOperator, error)
	UpdateTimeQuantumFilters(filters ...PlanExpression) (PlanOperator, error)
}

// Schema is the definition a set of columns from each operator
type Schema []*PlannerColumn

func (r Schema) Plan() []map[string]interface{} {
	result := make([]map[string]interface{}, len(r))
	for i, s := range r {
		m := make(map[string]interface{})
		m["name"] = s.ColumnName
		m["alias"] = s.AliasName
		m["relation"] = s.RelationName
		m["type"] = s.Type.TypeDescription()
		result[i] = m
	}
	return result
}

// Row is a tuple (of values)
type Row []interface{}

// Rows is a table of rows
type Rows []Row

// Append appends all the values in r2 to this row and returns the result
func (r Row) Append(r2 Row) Row {
	row := make(Row, len(r)+len(r2))
	copy(row, r)
	for i := range r2 {
		row[i+len(r)] = r2[i]
	}
	return row
}

// ErrNoMoreRows is a 'special' error returned to signify no more rows
var ErrNoMoreRows = errors.New("ErrNoMoreRows")

// RowIterator is an iterator that produces rows (or an error)
type RowIterator interface {
	Next(ctx context.Context) (Row, error)
}

type RowIterable interface {
	Iterator(ctx context.Context, row Row) (RowIterator, error)
}
