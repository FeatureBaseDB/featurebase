// Copyright 2021 Molecula Corp. All rights reserved.

package planner

import (
	"context"

	pilosa "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/sql3"
	"github.com/featurebasedb/featurebase/v3/sql3/parser"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
	"github.com/pkg/errors"
)

// compileDropTableStatement compiles a DROP TABLE statement into a
// PlanOperator.
func (p *ExecutionPlanner) compileDropTableStatement(stmt *parser.DropTableStatement) (_ types.PlanOperator, err error) {
	tableName := parser.IdentName(stmt.Name)
	index, err := p.schemaAPI.IndexInfo(context.Background(), tableName)
	if err != nil {
		if errors.Is(err, pilosa.ErrIndexNotFound) {
			return nil, sql3.NewErrTableNotFound(stmt.Name.NamePos.Line, stmt.Name.NamePos.Column, tableName)
		}
		return nil, err
	}
	return NewPlanOpQuery(p, NewPlanOpDropTable(p, index), p.sql), nil
}
