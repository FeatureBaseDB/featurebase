// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import (
	"context"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/sql3"
	"github.com/featurebasedb/featurebase/v3/sql3/parser"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
)

// compileCopyStatement compiles a parser.CopyStatement AST into a PlanOperator
func (p *ExecutionPlanner) compileCopyStatement(stmt *parser.CopyStatement) (types.PlanOperator, error) {
	query := NewPlanOpQuery(p, NewPlanOpNullTable(), p.sql)
	query.AddWarning("🦖 here there be dragons! COPY statement is experimental.")

	// handle projections
	projections := make([]types.PlanExpression, 0)
	for _, c := range stmt.Source.PossibleOutputColumns() {
		expr := &parser.QualifiedRef{
			Table:       &parser.Ident{Name: c.TableName},
			Column:      &parser.Ident{Name: c.ColumnName},
			ColumnIndex: c.ColumnIndex,
			RefDataType: c.Datatype,
		}
		planExpr, err := p.compileExpr(expr)
		if err != nil {
			return nil, err
		}
		projections = append(projections, planExpr)
	}

	// handle the where clause
	where, err := p.compileExpr(stmt.WhereExpr)
	if err != nil {
		return nil, err
	}

	// compile source
	source, err := p.compileSource(query, stmt.Source)
	if err != nil {
		return nil, err
	}

	// if we did have a where, insert the filter op
	if where != nil {
		source = NewPlanOpFilter(p, where, source)
	}

	var compiledOp types.PlanOperator
	url := ""
	apiKey := ""

	if stmt.Url != nil {
		lit, ok := stmt.Url.(*parser.StringLit)
		if !ok {
			return nil, sql3.NewErrStringLiteral(stmt.Url.Pos().Line, stmt.Url.Pos().Column)
		}
		url = lit.Value
	}

	if stmt.ApiKey != nil {
		lit, ok := stmt.ApiKey.(*parser.StringLit)
		if !ok {
			return nil, sql3.NewErrStringLiteral(stmt.ApiKey.Pos().Line, stmt.ApiKey.Pos().Column)
		}
		apiKey = lit.Value
	}

	// get the source table
	tname := dax.TableName(stmt.Source.String())
	tbl, err := p.schemaAPI.TableByName(context.Background(), tname)
	if err != nil {
		if isTableNotFoundError(err) {
			return nil, sql3.NewErrTableNotFound(0, 0, stmt.Source.String())
		}
		return nil, err
	}
	// get the ddl of source table and subst target table name
	ddl := generateTableDDL(tbl, stmt.TargetName.Name)
	compiledOp = NewPlanOpCopy(p, stmt.TargetName.Name, url, apiKey, ddl, NewPlanOpProjection(projections, source))
	children := []types.PlanOperator{
		compiledOp,
	}
	return query.WithChildren(children...)
}

func (p *ExecutionPlanner) analyzeCopyStatement(ctx context.Context, stmt *parser.CopyStatement) error {

	// analyze source
	var err error
	source, err := p.analyzeSource(ctx, stmt.Source, stmt)
	if err != nil {
		return err
	}
	stmt.Source = source

	// analyze where
	expr, err := p.analyzeExpression(ctx, stmt.WhereExpr, stmt)
	if err != nil {
		return err
	}
	stmt.WhereExpr = expr

	expr, err = p.analyzeExpression(ctx, stmt.Url, stmt)
	if err != nil {
		return err
	}
	stmt.Url = expr

	expr, err = p.analyzeExpression(ctx, stmt.ApiKey, stmt)
	if err != nil {
		return err
	}
	stmt.ApiKey = expr

	return nil
}
