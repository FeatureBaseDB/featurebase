// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import (
	"context"

	"github.com/featurebasedb/featurebase/v3/sql3/parser"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
)

// compileCreateViewStatement compiles a parser.CreateViewStatement AST into a PlanOperator
func (p *ExecutionPlanner) compileCreateViewStatement(stmt *parser.CreateViewStatement) (types.PlanOperator, error) {
	viewName := parser.IdentName(stmt.Name)
	view := &viewSystemObject{
		name: viewName,
	}

	// compile select
	_, err := p.compileSelectStatement(stmt.Select, true)
	if err != nil {
		return nil, err
	}
	view.statement = stmt.Select.String()

	query := NewPlanOpQuery(p, NewPlanOpCreateView(p, stmt.IfNotExists.IsValid(), view), p.sql)
	return query, nil
}

// compileAlterViewStatement compiles a parser.AlterViewStatement AST into a PlanOperator
func (p *ExecutionPlanner) compileAlterViewStatement(stmt *parser.AlterViewStatement) (types.PlanOperator, error) {
	viewName := parser.IdentName(stmt.Name)
	view := &viewSystemObject{
		name: viewName,
	}

	// compile select
	_, err := p.compileSelectStatement(stmt.Select, true)
	if err != nil {
		return nil, err
	}
	view.statement = stmt.Select.String()

	query := NewPlanOpQuery(p, NewPlanOpAlterView(p, view), p.sql)
	return query, nil
}

func (p *ExecutionPlanner) analyzeCreateViewStatement(ctx context.Context, stmt *parser.CreateViewStatement) error {
	//analyze the select
	_, err := p.analyzeSelectStatement(ctx, stmt.Select)
	if err != nil {
		return err
	}

	return nil
}

func (p *ExecutionPlanner) analyzeAlterViewStatement(ctx context.Context, stmt *parser.AlterViewStatement) error {
	//analyze the select
	_, err := p.analyzeSelectStatement(ctx, stmt.Select)
	if err != nil {
		return err
	}

	return nil
}
