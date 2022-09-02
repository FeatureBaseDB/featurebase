// Package sql3 contains the latest version of FeatureBase SQL support.
package sql3

import (
	"context"

	"github.com/featurebasedb/featurebase/v3/sql3/parser"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
)

type CompilePlanner interface {
	CompilePlan(context.Context, parser.Statement) (types.PlanOperator, error)
}

// Ensure type implements interface.
var _ CompilePlanner = (*NopCompilePlanner)(nil)

// NopCompilePlanner is a no-op implementation of the CompilePlanner interface.
type NopCompilePlanner struct{}

func NewNopCompilePlanner() *NopCompilePlanner {
	return &NopCompilePlanner{}
}

func (p *NopCompilePlanner) CompilePlan(ctx context.Context, stmt parser.Statement) (types.PlanOperator, error) {
	return nil, nil
}
