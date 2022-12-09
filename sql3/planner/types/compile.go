package types

import (
	"context"

	"github.com/featurebasedb/featurebase/v3/sql3/parser"
)

type CompilePlanner interface {
	CompilePlan(context.Context, parser.Statement) (PlanOperator, error)
}

// Ensure type implements interface.
var _ CompilePlanner = (*nopCompilePlanner)(nil)

// nopCompilePlanner is a no-op implementation of the CompilePlanner interface.
type nopCompilePlanner struct{}

func NewNopCompilePlanner() *nopCompilePlanner {
	return &nopCompilePlanner{}
}

func (p *nopCompilePlanner) CompilePlan(ctx context.Context, stmt parser.Statement) (PlanOperator, error) {
	return nil, nil
}
