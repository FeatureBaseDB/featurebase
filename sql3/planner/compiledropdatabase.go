// Copyright 2021 Molecula Corp. All rights reserved.

package planner

import (
	"context"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/sql3"
	"github.com/featurebasedb/featurebase/v3/sql3/parser"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
)

// compileDropDatabaseStatement compiles a DROP DATABASE statement into a
// PlanOperator.
func (p *ExecutionPlanner) compileDropDatabaseStatement(ctx context.Context, stmt *parser.DropDatabaseStatement) (_ types.PlanOperator, err error) {
	databaseName := parser.IdentName(stmt.Name)
	dbname := dax.DatabaseName(databaseName)
	db, err := p.schemaAPI.DatabaseByName(ctx, dbname)
	if err != nil {
		if isDatabaseNotFoundError(err) {
			return nil, sql3.NewErrDatabaseNotFound(stmt.Name.NamePos.Line, stmt.Name.NamePos.Column, databaseName)
		}
		return nil, err
	}
	return NewPlanOpQuery(p, NewPlanOpDropDatabase(p, db), p.sql), nil
}
