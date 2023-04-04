// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import (
	"context"

	"github.com/featurebasedb/featurebase/v3/sql3"
	"github.com/featurebasedb/featurebase/v3/sql3/parser"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
)

// compilePredictStatement compiles a parser.PredictStatement AST into a PlanOperator
func (p *ExecutionPlanner) compilePredictStatement(ctx context.Context, stmt *parser.PredictStatement) (types.PlanOperator, error) {

	// go get the model
	modelName := parser.IdentName(stmt.ModelName)

	// does the model exist
	obj, err := p.getModelByName(modelName)
	if err != nil {
		return nil, err
	}
	if obj == nil {
		return nil, sql3.NewErrInternalf("model '%s' not found", modelName)
	}

	selOp, err := p.compileSelectStatement(stmt.InputQuery, true)
	if err != nil {
		return nil, err
	}

	predict := NewPlanOpPredict(p, obj, selOp)
	predict.AddWarning("🦖 here there be dragons! PREDICT statement is experimental.")

	query := NewPlanOpQuery(p, predict, p.sql)

	return query, nil
}

func (p *ExecutionPlanner) analyzePredictStatement(ctx context.Context, stmt *parser.PredictStatement) error {

	// analyze the select
	_, err := p.analyzeSelectStatement(ctx, stmt.InputQuery)
	if err != nil {
		return err
	}

	return nil
}
