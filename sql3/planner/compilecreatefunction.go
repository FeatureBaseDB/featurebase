// Copyright 2023 Molecula Corp. All rights reserved.

package planner

import (
	"strings"

	"github.com/featurebasedb/featurebase/v3/sql3"
	"github.com/featurebasedb/featurebase/v3/sql3/parser"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
)

// compileCreateFunctionStatement compiles a parser.CreateFunctionStatement AST into a PlanOperator
func (p *ExecutionPlanner) compileCreateFunctionStatement(stmt *parser.CreateFunctionStatement) (types.PlanOperator, error) {
	functionName := parser.IdentName(stmt.Name)
	function := &functionSystemObject{
		name: functionName,
	}

	lang := "sql"
	if len(stmt.Options) > 0 {
		for _, o := range stmt.Options {
			switch strings.ToLower(o.Name.String()) {
			case "language":
				lit, ok := o.OptionExpr.(*parser.StringLit)
				if !ok {
					return nil, sql3.NewErrStringLiteral(o.OptionExpr.Pos().Line, o.OptionExpr.Pos().Column)
				}
				l := strings.ToLower(lit.Value)
				switch l {
				case "python":
					lang = l
				default:
					return nil, sql3.NewErrInternalf("unsupported language '%s'", l)
				}
			}
		}
	}
	function.language = lang

	// TODO(pok) - hobble user defined functions for now

	switch lang {
	case "sql":
		return nil, sql3.NewErrInternalf("unsupported language '%s'", lang)
	case "python":
		// return nil, sql3.NewErrInternalf("unsupported language '%s'", lang)

		// function body is in the return statement
		if len(stmt.Body) != 1 {
			return nil, sql3.NewErrInternalf("unexpected body len '%d'", len(stmt.Body))
		}

		rs, ok := stmt.Body[0].(*parser.ReturnStatement)
		if !ok {
			return nil, sql3.NewErrInternalf("unexpected statement type '%T'", stmt.Body[0])
		}

		bexpr, ok := rs.ReturnExpr.(*parser.StringLit)
		if !ok {
			return nil, sql3.NewErrInternalf("unexpected expression type '%T'", rs.ReturnExpr)
		}

		function.body = bexpr.Value
	default:
		return nil, sql3.NewErrInternalf("unsupported language '%s'", lang)
	}

	fn := NewPlanOpCreateFunction(p, stmt.IfNotExists.IsValid(), function)
	fn.AddWarning("🦖 here there be dragons! CREATE FUNCTION statement is experimental.")

	query := NewPlanOpQuery(p, fn, p.sql)
	return query, nil
}

func (p *ExecutionPlanner) analyzeCreateFunctionStatement(stmt *parser.CreateFunctionStatement) error {

	return nil
}
