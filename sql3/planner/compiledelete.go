// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import (
	"context"

	"github.com/featurebasedb/featurebase/v3/sql3/parser"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
)

// compileDeleteStatement compiles a parser.DeleteStatment AST into a PlanOperator
func (p *ExecutionPlanner) compileDeleteStatement(stmt *parser.DeleteStatement) (types.PlanOperator, error) {
	query := NewPlanOpQuery(p, NewPlanOpNullTable(), p.sql)

	tableName := parser.IdentName(stmt.TableName.Name)

	// source expression
	source, err := p.compileSource(query, stmt.Source)
	if err != nil {
		return nil, err
	}

	// handle the where clause
	where, err := p.compileExpr(stmt.WhereExpr)
	if err != nil {
		return nil, err
	}

	_, sourceIsScan := source.(*PlanOpPQLTableScan)

	// no where clause and source is a scan so it's a truncate
	if where == nil && sourceIsScan {
		delOp := NewPlanOpPQLTruncateTable(p, string(tableName))

		children := []types.PlanOperator{
			delOp,
		}
		return query.WithChildren(children...)
	}

	var delOp types.PlanOperator

	// if we did have a where, insert the filter op
	if where != nil {
		delOp = NewPlanOpPQLConstRowDelete(p, string(tableName), NewPlanOpFilter(p, where, source))
	} else {
		delOp = NewPlanOpPQLConstRowDelete(p, string(tableName), source)
	}

	children := []types.PlanOperator{
		delOp,
	}
	return query.WithChildren(children...)
}

func (p *ExecutionPlanner) analyzeDeleteStatement(ctx context.Context, stmt *parser.DeleteStatement) error {

	_, err := p.analyzeSource(ctx, stmt.Source, stmt)
	if err != nil {
		return err
	}

	// if we have a where clause, check that
	if stmt.WhereExpr != nil {
		expr, err := p.analyzeExpression(ctx, stmt.WhereExpr, stmt)
		if err != nil {
			return err
		}
		stmt.WhereExpr = expr
	}

	return nil
}
