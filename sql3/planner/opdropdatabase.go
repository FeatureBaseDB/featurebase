// Copyright 2021 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"fmt"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
)

// PlanOpDropDatabase plan operator to drop a database.
type PlanOpDropDatabase struct {
	planner  *ExecutionPlanner
	db       *dax.Database
	warnings []string
}

func NewPlanOpDropDatabase(p *ExecutionPlanner, db *dax.Database) *PlanOpDropDatabase {
	return &PlanOpDropDatabase{
		planner:  p,
		db:       db,
		warnings: make([]string, 0),
	}
}

func (p *PlanOpDropDatabase) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_op"] = fmt.Sprintf("%T", p)
	result["databaseName"] = p.db.Name
	return result
}

func (p *PlanOpDropDatabase) String() string {
	return ""
}

func (p *PlanOpDropDatabase) AddWarning(warning string) {
	p.warnings = append(p.warnings, warning)
}

func (p *PlanOpDropDatabase) Warnings() []string {
	return p.warnings
}

func (p *PlanOpDropDatabase) Schema() types.Schema {
	return types.Schema{}
}

func (p *PlanOpDropDatabase) Children() []types.PlanOperator {
	return []types.PlanOperator{}
}

func (p *PlanOpDropDatabase) Iterator(ctx context.Context, row types.Row) (types.RowIterator, error) {
	return &dropDatabaseRowIter{
		planner: p.planner,
		db:      p.db,
	}, nil
}

func (p *PlanOpDropDatabase) WithChildren(children ...types.PlanOperator) (types.PlanOperator, error) {
	return nil, nil
}

type dropDatabaseRowIter struct {
	planner *ExecutionPlanner
	db      *dax.Database
}

var _ types.RowIterator = (*dropDatabaseRowIter)(nil)

func (i *dropDatabaseRowIter) Next(ctx context.Context) (types.Row, error) {
	err := i.planner.checkAccess(ctx, string(i.db.Name), accessTypeDropObject)
	if err != nil {
		return nil, err
	}

	err = i.planner.schemaAPI.DropDatabase(ctx, i.db.ID)
	if err != nil {
		return nil, err
	}
	return nil, types.ErrNoMoreRows
}
