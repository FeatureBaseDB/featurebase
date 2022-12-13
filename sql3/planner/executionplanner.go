// Copyright 2021 Molecula Corp. All rights reserved.

package planner

import (
	"context"

	pilosa "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/logger"
	"github.com/featurebasedb/featurebase/v3/sql3"
	"github.com/featurebasedb/featurebase/v3/sql3/parser"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
)

// PlannerScope holds scope for the planner
// there is a stack of these in the ExecutionPlanner and some corresponding push/pop functions
// this allows us to do scoped operations without passing stuff down into
// every function
type PlannerScope struct {
	scope types.PlanOperator
}

// ExecutionPlanner compiles SQL text into a query plan
type ExecutionPlanner struct {
	executor       pilosa.Executor
	schemaAPI      pilosa.SchemaAPI
	systemAPI      pilosa.SystemAPI
	systemLayerAPI pilosa.SystemLayerAPI
	importer       pilosa.Importer
	logger         logger.Logger
	sql            string
	scopeStack     *scopeStack
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
		scopeStack:     newScopeStack(),
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
		return p.analyzeSelectStatement(stmt)
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

// addReference is a convenience function that allows the planner to keep track
// of references so we can use them during optimization.
func (p *ExecutionPlanner) addReference(ref *qualifiedRefPlanExpression) error {
	table := p.scopeStack.read()
	if table == nil {
		return sql3.NewErrInternalf("unexpected symbol table state")
	}

	switch s := table.scope.(type) {
	case *PlanOpQuery:
		s.referenceList = append(s.referenceList, ref)
	}
	return nil
}

// scopeStack is a stack of PlannerScope with the usual push/pop methods.
type scopeStack struct {
	st []*PlannerScope
}

// newScopeStack returns a scope stack initialized with zero elements on the
// stack.
func newScopeStack() *scopeStack {
	return &scopeStack{
		st: make([]*PlannerScope, 0),
	}
}

// push adds the provided PlanOperator (as the scope of a PlannerScope) to the
// scope stack.
func (ss *scopeStack) push(scope types.PlanOperator) {
	ss.st = append(ss.st, &PlannerScope{
		scope: scope,
	})
}

// pop removes (and returns) the last scope pushed to the stack.
func (ss *scopeStack) pop() *PlannerScope {
	if len(ss.st) == 0 {
		return nil
	}
	ret := ss.st[len(ss.st)-1]
	ss.st = ss.st[:len(ss.st)-1]
	return ret
}

// read returns the last scope pushed to the stack, but unlike pop, it does not
// remove it.
func (ss *scopeStack) read() *PlannerScope {
	if len(ss.st) == 0 {
		return nil
	}
	return ss.st[len(ss.st)-1]
}
