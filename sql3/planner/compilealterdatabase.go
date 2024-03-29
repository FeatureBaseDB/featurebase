// Copyright 2021 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"strings"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/sql3"
	"github.com/featurebasedb/featurebase/v3/sql3/parser"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
)

// compileAlterDatabaseStatement compiles an ALTER DATABASE statement into a
// PlanOperator.
func (p *ExecutionPlanner) compileAlterDatabaseStatement(ctx context.Context, stmt *parser.AlterDatabaseStatement) (_ types.PlanOperator, err error) {
	databaseName := strings.ToLower(parser.IdentName(stmt.Name))

	// does the database exist
	dbname := dax.DatabaseName(databaseName)
	db, err := p.schemaAPI.DatabaseByName(ctx, dbname)
	if err != nil {
		if isDatabaseNotFoundError(err) {
			return nil, sql3.NewErrDatabaseNotFound(stmt.Name.NamePos.Line, stmt.Name.NamePos.Column, databaseName)
		}
		return nil, err
	}

	if stmt.With.IsValid() {
		return NewPlanOpQuery(p, NewPlanOpAlterDatabase(p, db, alterOpSet, stmt.Option), p.sql), nil
	} else {
		return nil, sql3.NewErrInternal("unhandled alter operation")
	}
}

// analyzeAlterDatabaseStatement analyze an ALTER DATABASE statement and returns an
// error if anything is invalid.
func (p *ExecutionPlanner) analyzeAlterDatabaseStatement(stmt *parser.AlterDatabaseStatement) error {
	return nil
}
