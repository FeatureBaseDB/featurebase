// Copyright 2021 Molecula Corp. All rights reserved.

package planner

import (
	"context"

	pilosa "github.com/molecula/featurebase/v3"
	"github.com/molecula/featurebase/v3/logger"
	"github.com/molecula/featurebase/v3/sql3"
	"github.com/molecula/featurebase/v3/sql3/parser"
	"github.com/molecula/featurebase/v3/sql3/planner/types"
)

// ExecutionPlanner compiles SQL text into a query plan
type ExecutionPlanner struct {
	executor       pilosa.Executor
	schemaAPI      pilosa.SchemaAPI
	systemAPI      pilosa.SystemAPI
	systemLayerAPI pilosa.SystemLayerAPI
	importer       pilosa.Importer
	logger         logger.Logger
	sql            string
}

func NewExecutionPlanner(executor pilosa.Executor, schemaAPI pilosa.SchemaAPI, systemAPI pilosa.SystemAPI, systemLayerAPI pilosa.SystemLayerAPI, importer pilosa.Importer, logger logger.Logger, sql string) *ExecutionPlanner {
	return &ExecutionPlanner{
		executor:       executor,
		schemaAPI:      newSystemTableDefintionsWrapper(schemaAPI),
		systemAPI:      systemAPI,
		systemLayerAPI: systemLayerAPI,
		importer:       importer,
		logger:         logger,
		sql:            sql,
	}
}

// CompilePlan takes an AST (parser.Statement) and compiles into a query plan returning the root
// PlanOperator
// The act of compiling includes an analysis step that does semantic analysis of the AST, this includes
// type checking, and sometimes AST rewriting. The compile phase uses the type-checked and rewritten AST
// to produce a query plan.
func (p *ExecutionPlanner) CompilePlan(ctx context.Context, stmt parser.Statement) (types.PlanOperator, error) {
	// call analyze first
	err := p.analyzePlan(stmt)
	if err != nil {
		return nil, err
	}

	var rootOperator types.PlanOperator
	switch stmt := stmt.(type) {
	case *parser.SelectStatement:
		rootOperator, err = p.compileSelectStatement(stmt, false)
	case *parser.ShowTablesStatement:
		rootOperator, err = p.compileShowTablesStatement(stmt)
	case *parser.ShowColumnsStatement:
		rootOperator, err = p.compileShowColumnsStatement(stmt)
	case *parser.ShowCreateTableStatement:
		rootOperator, err = p.compileShowCreateTableStatement(stmt)
	case *parser.CreateTableStatement:
		rootOperator, err = p.compileCreateTableStatement(stmt)
	case *parser.AlterTableStatement:
		rootOperator, err = p.compileAlterTableStatement(stmt)
	case *parser.DropTableStatement:
		rootOperator, err = p.compileDropTableStatement(stmt)
	case *parser.InsertStatement:
		rootOperator, err = p.compileInsertStatement(stmt)
	case *parser.BulkInsertStatement:
		rootOperator, err = p.compileBulkInsertStatement(stmt)
	case *parser.DeleteStatement:
		rootOperator, err = p.compileDeleteStatement(stmt)
	default:
		return nil, sql3.NewErrInternalf("cannot plan statement: %T", stmt)
	}
	// Optimize the plan.
	if err == nil {
		rootOperator, err = p.optimizePlan(ctx, rootOperator)
	}
	return rootOperator, err
}

func (p *ExecutionPlanner) analyzePlan(stmt parser.Statement) error {
	switch stmt := stmt.(type) {
	case *parser.SelectStatement:
		_, err := p.analyzeSelectStatement(stmt)
		return err
	case *parser.ShowTablesStatement:
		return nil
	case *parser.ShowColumnsStatement:
		return nil
	case *parser.ShowCreateTableStatement:
		return nil
	case *parser.CreateTableStatement:
		return p.analyzeCreateTableStatement(stmt)
	case *parser.AlterTableStatement:
		return p.analyzeAlterTableStatement(stmt)
	case *parser.DropTableStatement:
		return nil
	case *parser.InsertStatement:
		return p.analyzeInsertStatement(stmt)
	case *parser.BulkInsertStatement:
		return p.analyzeBulkInsertStatement(stmt)
	case *parser.DeleteStatement:
		return p.analyzeDeleteStatement(stmt)
	default:
		return sql3.NewErrInternalf("cannot analyze statement: %T", stmt)
	}
}

type accessType byte

const (
	accessTypeReadData accessType = iota
	accessTypeWriteData
	accessTypeCreateObject
	accessTypeAlterObject
	accessTypeDropObject
)

func (p *ExecutionPlanner) checkAccess(ctx context.Context, objectName string, _ accessType) error {
	return nil
}
