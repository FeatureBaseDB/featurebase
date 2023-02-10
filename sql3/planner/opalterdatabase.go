// Copyright 2021 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"fmt"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/sql3"
	"github.com/featurebasedb/featurebase/v3/sql3/parser"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
)

// PlanOpAlterDatabase plan operator to alter a database.
type PlanOpAlterDatabase struct {
	planner   *ExecutionPlanner
	database  *dax.Database
	operation alterOperation
	option    parser.DatabaseOption
	warnings  []string
}

func NewPlanOpAlterDatabase(p *ExecutionPlanner, db *dax.Database, operation alterOperation, option parser.DatabaseOption) *PlanOpAlterDatabase {
	return &PlanOpAlterDatabase{
		planner:   p,
		database:  db,
		operation: operation,
		option:    option,
		warnings:  make([]string, 0),
	}
}

func (p *PlanOpAlterDatabase) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_op"] = fmt.Sprintf("%T", p)
	result["databaseName"] = p.database.Name
	result["operation"] = p.operation
	result["option"] = p.option
	return result
}

func (p *PlanOpAlterDatabase) AddWarning(warning string) {
	p.warnings = append(p.warnings, warning)
}

func (p *PlanOpAlterDatabase) Warnings() []string {
	return p.warnings
}

func (p *PlanOpAlterDatabase) String() string {
	return ""
}

func (p *PlanOpAlterDatabase) Schema() types.Schema {
	return types.Schema{}
}

func (p *PlanOpAlterDatabase) Children() []types.PlanOperator {
	return []types.PlanOperator{}
}

func (p *PlanOpAlterDatabase) Iterator(ctx context.Context, row types.Row) (types.RowIterator, error) {
	return &alterDatabaseRowIter{
		planner:   p.planner,
		database:  p.database,
		operation: p.operation,
		option:    p.option,
	}, nil
}

func (p *PlanOpAlterDatabase) WithChildren(children ...types.PlanOperator) (types.PlanOperator, error) {
	return nil, nil
}

type alterDatabaseRowIter struct {
	planner   *ExecutionPlanner
	database  *dax.Database
	operation alterOperation
	option    parser.DatabaseOption
}

var _ types.RowIterator = (*alterDatabaseRowIter)(nil)

func (i *alterDatabaseRowIter) Next(ctx context.Context) (types.Row, error) {
	switch i.operation {
	case alterOpSet:
		var optName string
		var optValue string

		switch v := i.option.(type) {
		case *parser.UnitsOption:
			e := v.Expr.(*parser.IntegerLit)
			optName = "workers-min"
			optValue = e.Value
		default:
			return nil, sql3.NewErrInvalidDatabaseOption(0, 0, i.option.String())
		}

		if err := i.planner.schemaAPI.SetDatabaseOption(ctx, i.database.ID, optName, optValue); err != nil {
			return nil, err
		}
	}
	return nil, types.ErrNoMoreRows
}
