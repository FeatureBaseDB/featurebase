// Copyright 2021 Molecula Corp. All rights reserved.

package planner

import (
	"github.com/featurebasedb/featurebase/v3/sql3"
	"github.com/featurebasedb/featurebase/v3/sql3/parser"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
)

// compileCreateDatabaseStatement compiles a CREATE DATABASE statement into a
// PlanOperator.
func (p *ExecutionPlanner) compileCreateDatabaseStatement(stmt *parser.CreateDatabaseStatement) (_ types.PlanOperator, err error) {
	databaseName := parser.IdentName(stmt.Name)
	failIfExists := !stmt.IfNotExists.IsValid()

	// apply table options
	description := ""
	for _, option := range stmt.Options {
		switch o := option.(type) {
		case *parser.CommentOption:
			e := o.Expr.(*parser.StringLit)
			description = e.Value
		}
	}

	cop := NewPlanOpCreateDatabase(p, databaseName, failIfExists, description)
	return NewPlanOpQuery(p, cop, p.sql), nil
}

// analyzeCreateDatabaseStatement analyzes a CREATE DATABASE statement and
// returns an error if anything is invalid.
func (p *ExecutionPlanner) analyzeCreateDatabaseStatement(stmt *parser.CreateDatabaseStatement) error {
	//check database options
	for _, option := range stmt.Options {

		switch o := option.(type) {
		case *parser.CommentOption:
			_, ok := o.Expr.(*parser.StringLit)
			if !ok {
				return sql3.NewErrStringLiteral(o.Expr.Pos().Line, o.Expr.Pos().Column)
			}

		default:
			return sql3.NewErrInternalf("unhandled database option type '%T'", option)
		}
	}

	return nil
}
