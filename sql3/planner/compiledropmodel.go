// Copyright 2023 Molecula Corp. All rights reserved.

package planner

import (
	"github.com/featurebasedb/featurebase/v3/sql3"
	"github.com/featurebasedb/featurebase/v3/sql3/parser"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
)

// compileDropModelStatement compiles a DROP MODEL statement into a PlanOperator.
func (p *ExecutionPlanner) compileDropModelStatement(stmt *parser.DropModelStatement) (_ types.PlanOperator, err error) {
	modelName := parser.IdentName(stmt.Name)
	v, err := p.getModelByName(modelName)
	if err != nil {
		return nil, err
	}
	if v == nil && !stmt.IfExists.IsValid() {
		return nil, sql3.NewErrModelNotFound(0, 0, modelName)
	}

	dropModel := NewPlanOpDropModel(p, stmt.IfExists.IsValid(), modelName)
	dropModel.AddWarning("🦖 here there be dragons! DROP MODEL statement is experimental.")

	return NewPlanOpQuery(p, dropModel, p.sql), nil
}
