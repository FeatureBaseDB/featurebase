// Copyright 2021 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"fmt"

	pilosa "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/sql3"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
	"github.com/pkg/errors"
)

// PlanOpCreateDatabase is a plan operator that creates a database.
type PlanOpCreateDatabase struct {
	planner      *ExecutionPlanner
	databaseName string
	failIfExists bool
	description  string
	warnings     []string
}

// NewPlanOpCreateDatabase returns a new PlanOpCreateDatabase planoperator
func NewPlanOpCreateDatabase(p *ExecutionPlanner, databaseName string, failIfExists bool, description string) *PlanOpCreateDatabase {
	return &PlanOpCreateDatabase{
		planner:      p,
		databaseName: databaseName,
		failIfExists: failIfExists,
		description:  description,
		warnings:     make([]string, 0),
	}
}

func (p *PlanOpCreateDatabase) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_op"] = fmt.Sprintf("%T", p)
	result["name"] = p.databaseName
	result["failIfExists"] = p.failIfExists
	return result
}

func (p *PlanOpCreateDatabase) String() string {
	return ""
}

func (p *PlanOpCreateDatabase) AddWarning(warning string) {
	p.warnings = append(p.warnings, warning)
}

func (p *PlanOpCreateDatabase) Warnings() []string {
	return p.warnings
}

func (p *PlanOpCreateDatabase) Schema() types.Schema {
	return types.Schema{}
}

func (p *PlanOpCreateDatabase) Children() []types.PlanOperator {
	return []types.PlanOperator{}
}

func (p *PlanOpCreateDatabase) Iterator(ctx context.Context, row types.Row) (types.RowIterator, error) {
	return &createDatabaseRowIter{
		planner:      p.planner,
		databaseName: p.databaseName,
		failIfExists: p.failIfExists,
		description:  p.description,
	}, nil
}

func (p *PlanOpCreateDatabase) WithChildren(children ...types.PlanOperator) (types.PlanOperator, error) {
	return nil, nil
}

type createDatabaseRowIter struct {
	planner      *ExecutionPlanner
	databaseName string
	failIfExists bool
	description  string
}

var _ types.RowIterator = (*createDatabaseRowIter)(nil)

func (i *createDatabaseRowIter) Next(ctx context.Context) (types.Row, error) {
	//create the database
	db := &dax.Database{
		Name: dax.DatabaseName(i.databaseName),

		Description: i.description,
	}

	if err := i.planner.schemaAPI.CreateDatabase(ctx, db); err != nil {
		if _, ok := errors.Cause(err).(pilosa.ConflictError); ok {
			if i.failIfExists {
				return nil, sql3.NewErrDatabaseExists(0, 0, i.databaseName)
			}
		} else {
			return nil, err
		}
	}
	return nil, types.ErrNoMoreRows
}
