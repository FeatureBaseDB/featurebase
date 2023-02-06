// Copyright 2021 Molecula Corp. All rights reserved.

package planner

import (
	"strconv"

	"github.com/featurebasedb/featurebase/v3/sql3"
	"github.com/featurebasedb/featurebase/v3/sql3/parser"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
)

// compileCreateDatabaseStatement compiles a CREATE DATABASE statement into a
// PlanOperator.
func (p *ExecutionPlanner) compileCreateDatabaseStatement(stmt *parser.CreateDatabaseStatement) (_ types.PlanOperator, err error) {
	databaseName := parser.IdentName(stmt.Name)
	failIfExists := !stmt.IfNotExists.IsValid()

	units := 0
	description := ""

	// apply database options
	if stmt.With.IsValid() {
		for _, option := range stmt.Options {
			switch o := option.(type) {
			case *parser.UnitsOption:
				e := o.Expr.(*parser.IntegerLit)
				i, err := strconv.ParseInt(e.Value, 10, 64)
				if err != nil {
					return nil, err
				}
				units = int(i)
			case *parser.CommentOption:
				e := o.Expr.(*parser.StringLit)
				description = e.Value
			}
		}
	}

	cop := NewPlanOpCreateDatabase(p, databaseName, failIfExists, units, description)
	return NewPlanOpQuery(p, cop, p.sql), nil
}

// analyzeCreateDatabaseStatement analyzes a CREATE DATABASE statement and
// returns an error if anything is invalid.
func (p *ExecutionPlanner) analyzeCreateDatabaseStatement(stmt *parser.CreateDatabaseStatement) error {
	if stmt.With.IsValid() {
		// no checks if WITH is not provided
		return nil
	}

	//check database options
	for _, option := range stmt.Options {

		switch o := option.(type) {
		case *parser.UnitsOption:
			//check the type of the expression
			literal, ok := o.Expr.(*parser.IntegerLit)
			if !ok {
				return sql3.NewErrIntegerLiteral(o.Expr.Pos().Line, o.Expr.Pos().Column)
			}
			// units needs to be >=0 and we'll cap conservatively at 10000
			i, err := strconv.ParseInt(literal.Value, 10, 64)
			if err != nil {
				return err
			}
			if i < 0 || i > 10000 {
				return sql3.NewErrInvalidUnitsValue(o.Expr.Pos().Line, o.Expr.Pos().Column, i)
			}

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
