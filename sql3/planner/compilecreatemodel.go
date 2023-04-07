// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"strings"

	"github.com/featurebasedb/featurebase/v3/sql3"
	"github.com/featurebasedb/featurebase/v3/sql3/parser"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
)

// TODO (pok) what does 'if not exists' do?

// compileCreateModelStatement compiles a parser.CreateModelStatement AST into a PlanOperator
func (p *ExecutionPlanner) compileCreateModelStatement(stmt *parser.CreateModelStatement) (types.PlanOperator, error) {
	modelName := parser.IdentName(stmt.Name)

	// does the model exist
	obj, err := p.getModelByName(modelName)
	if err != nil {
		return nil, err
	}
	if obj != nil {
		return nil, sql3.NewErrInternalf("model '%s' already exists", modelName)
	}

	// if we got to here model does not exist
	model := &modelSystemObject{
		name: modelName,
	}

	for _, o := range stmt.Options {
		optName := parser.IdentName(o.Name)

		switch strings.ToLower(optName) {
		case "modeltype":
			lit, ok := o.OptionExpr.(*parser.StringLit)
			if !ok {
				return nil, sql3.NewErrInternalf("unexpected type '%T'", o.OptionExpr)
			}
			model.modelType = lit.Value

		case "labels":
			lit, ok := o.OptionExpr.(*parser.ArrayLiteralExpr)
			if !ok {
				return nil, sql3.NewErrInternalf("unexpected type '%T'", o.OptionExpr)
			}
			model.labels = make([]string, len(lit.Members))
			for i, m := range lit.Members {
				mlit, ok := m.(*parser.StringLit)
				if !ok {
					return nil, sql3.NewErrInternalf("unexpected type '%T'", m)
				}
				model.labels[i] = mlit.Value
			}

		default:
			return nil, sql3.NewErrInternalf("unexpected model option '%s'", optName)
		}
	}

	selOp, err := p.compileSelectStatement(stmt.ModelQuery, true)
	if err != nil {
		return nil, err
	}

	// build a list of input columns for the model from the select query
	schema := selOp.Schema()
	model.inputColumns = make([]string, 0)
	for _, p := range schema {
		// if we have no column name, we have an error
		if len(p.ColumnName) == 0 {
			return nil, sql3.NewErrInternalf("query output columns used as inputs to models must be named")
		}
		// exclude any that are in the labels
		isLabel := false
		for _, l := range model.labels {
			if strings.EqualFold(p.ColumnName, l) {
				isLabel = true
				break
			}
		}
		if !isLabel {
			model.inputColumns = append(model.inputColumns, p.ColumnName)
		}
	}
	createModel := NewPlanOpCreateModel(p, model, selOp)
	createModel.AddWarning("🦖 here there be dragons! CREATE MODEL statement is experimental.")

	query := NewPlanOpQuery(p, createModel, p.sql)
	return query, nil
}

func (p *ExecutionPlanner) analyzeCreateModelStatement(ctx context.Context, stmt *parser.CreateModelStatement) error {
	// iterate the options
	for _, opt := range stmt.Options {
		optName := parser.IdentName(opt.Name)
		if !isValidModelOption(optName) {
			return sql3.NewErrInternalf("invalid model option '%s'", optName)
		}
		e, err := p.analyzeModelOptionExpr(ctx, optName, opt.OptionExpr, stmt)
		if err != nil {
			return err
		}
		opt.OptionExpr = e
	}

	// analyze the select
	_, err := p.analyzeSelectStatement(ctx, stmt.ModelQuery)
	if err != nil {
		return err
	}
	return nil
}

func isValidModelOption(name string) bool {
	switch strings.ToLower(name) {
	case "modeltype":
		return true

	case "labels":
		return true

	default:
		return false
	}
}

func (p *ExecutionPlanner) analyzeModelOptionExpr(ctx context.Context, optName string, expr parser.Expr, scope parser.Statement) (parser.Expr, error) {
	if expr == nil {
		return nil, nil
	}

	e, err := p.analyzeExpression(ctx, expr, scope)
	if err != nil {
		return nil, err
	}

	switch strings.ToLower(optName) {
	case "modeltype":

		// model type needs to be a string literal
		if !(e.IsLiteral() && typeIsString(e.DataType())) {
			return nil, sql3.NewErrStringLiteral(e.Pos().Line, e.Pos().Column)
		}
		ty, ok := e.(*parser.StringLit)
		if !ok {
			return nil, sql3.NewErrInternalf("unexpected type '%T'", e)
		}

		// these are the model types supported
		switch strings.ToLower(ty.Value) {
		case "linear_regresssion":
			break
		default:
			return nil, sql3.NewErrInternalf("unexpected model tyoe '%s'", ty.Value)
		}
		return e, nil

	case "labels":
		// labels needs to be a string array literal
		// TODO (pok) revist 'set' literals (should be array literal; type checking could be robustified etc.)
		if !e.IsLiteral() {
			return nil, sql3.NewErrInternalf("string array literal expected")
		}
		ok, baseType := typeIsSet(e.DataType())
		if !ok {
			return nil, sql3.NewErrInternalf("array expression expected")
		}
		if !typeIsString(baseType) {
			return nil, sql3.NewErrInternalf("string array expected")
		}
		return e, nil

	default:
		return nil, sql3.NewErrInternalf("unexpected option name '%s'", optName)
	}
}
