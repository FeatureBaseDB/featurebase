// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import (
	"context"

	pilosa "github.com/molecula/featurebase/v3"
	"github.com/molecula/featurebase/v3/sql3"
	"github.com/molecula/featurebase/v3/sql3/parser"
	"github.com/molecula/featurebase/v3/sql3/planner/types"
	"github.com/pkg/errors"
)

// compileBulkInsertStatement compiles a BULK INSERT statement into a
// PlanOperator.
func (p *ExecutionPlanner) compileBulkInsertStatement(stmt *parser.BulkInsertStatement) (_ types.PlanOperator, err error) {
	tableName := parser.IdentName(stmt.Table)

	/*table*/
	_, err = p.schemaAPI.IndexInfo(context.Background(), tableName)
	if err != nil {
		if errors.Is(err, pilosa.ErrIndexNotFound) {
			return nil, sql3.NewErrTableNotFound(stmt.Table.NamePos.Line, stmt.Table.NamePos.Column, tableName)
		}
		return nil, err
	}

	return NewPlanOpBulkInsert(p, tableName), nil
}

// analyzeBulkInsertStatement analyzes a BULK INSERT statement and returns an
// error if anything is invalid.
func (p *ExecutionPlanner) analyzeBulkInsertStatement(stmt *parser.BulkInsertStatement) error {
	//check referred to table exists
	tableName := parser.IdentName(stmt.Table)
	/*table*/ _, err := p.schemaAPI.IndexInfo(context.Background(), tableName)
	if err != nil {
		if errors.Is(err, pilosa.ErrIndexNotFound) {
			return sql3.NewErrTableNotFound(stmt.Table.NamePos.Line, stmt.Table.NamePos.Column, tableName)
		}
		return err
	}

	return nil
}
