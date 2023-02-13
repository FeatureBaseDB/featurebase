// Copyright 2021 Molecula Corp. All rights reserved.

package planner

import (
	"context"

	pilosa "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/sql3"
	"github.com/featurebasedb/featurebase/v3/sql3/parser"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
)

// compileDropTableStatement compiles a DROP TABLE statement into a
// PlanOperator.
func (p *ExecutionPlanner) compileDropTableStatement(ctx context.Context, stmt *parser.DropTableStatement) (_ types.PlanOperator, err error) {
	tableName := parser.IdentName(stmt.Name)
	tname := dax.TableName(tableName)
	tbl, err := p.schemaAPI.TableByName(ctx, tname)
	if err != nil {
		if isTableNotFoundError(err) {
			return nil, sql3.NewErrTableNotFound(stmt.Name.NamePos.Line, stmt.Name.NamePos.Column, tableName)
		}
		return nil, err
	}
	return NewPlanOpQuery(p, NewPlanOpDropTable(p, pilosa.TableToIndexInfo(tbl)), p.sql), nil
}
