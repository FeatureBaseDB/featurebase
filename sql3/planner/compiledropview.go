// Copyright 2023 Molecula Corp. All rights reserved.

package planner

import (
	"github.com/molecula/featurebase/v3/sql3"
	"github.com/molecula/featurebase/v3/sql3/parser"
	"github.com/molecula/featurebase/v3/sql3/planner/types"
)

// compileDropViewStatement compiles a DROP VIEW statement into a PlanOperator.
func (p *ExecutionPlanner) compileDropViewStatement(stmt *parser.DropViewStatement) (_ types.PlanOperator, err error) {
	viewName := parser.IdentName(stmt.Name)
	v, err := p.getViewByName(viewName)
	if err != nil {
		return nil, err
	}
	if v == nil && !stmt.IfExists.IsValid() {
		return nil, sql3.NewErrViewNotFound(0, 0, viewName)
	}

	return NewPlanOpQuery(p, NewPlanOpDropView(p, stmt.IfExists.IsValid(), viewName), p.sql), nil
}
