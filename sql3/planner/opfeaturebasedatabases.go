// Copyright 2021 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"fmt"
	"time"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/sql3/parser"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
)

// PlanOpFeatureBaseDatabases wraps a []*Database that is returned from
// schemaAPI.Schema().
type PlanOpFeatureBaseDatabases struct {
	planner  *ExecutionPlanner
	dbs      []*dax.Database
	warnings []string
}

func NewPlanOpFeatureBaseDatabases(planner *ExecutionPlanner, dbs []*dax.Database) *PlanOpFeatureBaseDatabases {
	return &PlanOpFeatureBaseDatabases{
		planner:  planner,
		dbs:      dbs,
		warnings: make([]string, 0),
	}
}

func (p *PlanOpFeatureBaseDatabases) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_op"] = fmt.Sprintf("%T", p)
	result["_schema"] = p.Schema().Plan()
	return result
}

func (p *PlanOpFeatureBaseDatabases) String() string {
	return ""
}

func (p *PlanOpFeatureBaseDatabases) AddWarning(warning string) {
	p.warnings = append(p.warnings, warning)
}

func (p *PlanOpFeatureBaseDatabases) Warnings() []string {
	return p.warnings
}

func (p *PlanOpFeatureBaseDatabases) Schema() types.Schema {
	return types.Schema{
		&types.PlannerColumn{
			RelationName: "fb_databases",
			ColumnName:   string(dax.PrimaryKeyFieldName),
			Type:         parser.NewDataTypeString(),
		},
		&types.PlannerColumn{
			RelationName: "fb_databases",
			ColumnName:   "name",
			Type:         parser.NewDataTypeString(),
		},
		&types.PlannerColumn{
			RelationName: "fb_databases",
			ColumnName:   "owner",
			Type:         parser.NewDataTypeString(),
		},
		&types.PlannerColumn{
			RelationName: "fb_databases",
			ColumnName:   "updated_by",
			Type:         parser.NewDataTypeString(),
		},
		&types.PlannerColumn{
			RelationName: "fb_databases",
			ColumnName:   "created_at",
			Type:         parser.NewDataTypeTimestamp(),
		},
		&types.PlannerColumn{
			RelationName: "fb_databases",
			ColumnName:   "updated_at",
			Type:         parser.NewDataTypeTimestamp(),
		},
		&types.PlannerColumn{
			RelationName: "fb_databases",
			ColumnName:   "description",
			Type:         parser.NewDataTypeString(),
		},
	}
}

func (p *PlanOpFeatureBaseDatabases) Children() []types.PlanOperator {
	return []types.PlanOperator{}
}

func (p *PlanOpFeatureBaseDatabases) Iterator(ctx context.Context, row types.Row) (types.RowIterator, error) {
	return &showDatabasesRowIter{
		planner: p.planner,
		dbs:     p.dbs,
	}, nil
}

func (p *PlanOpFeatureBaseDatabases) WithChildren(children ...types.PlanOperator) (types.PlanOperator, error) {
	return NewPlanOpFeatureBaseDatabases(p.planner, p.dbs), nil
}

type showDatabasesRowIter struct {
	planner  *ExecutionPlanner
	dbs      []*dax.Database
	rowIndex int
}

var _ types.RowIterator = (*showDatabasesRowIter)(nil)

func (i *showDatabasesRowIter) Next(ctx context.Context) (types.Row, error) {
	if i.rowIndex < len(i.dbs) {

		dbID := i.dbs[i.rowIndex].ID
		dbName := i.dbs[i.rowIndex].Name

		createdAt := time.Unix(i.dbs[i.rowIndex].CreatedAt, 0)
		updatedAt := time.Unix(i.dbs[i.rowIndex].UpdatedAt, 0)
		row := []interface{}{
			dbID,
			dbName,
			i.dbs[i.rowIndex].Owner,
			i.dbs[i.rowIndex].UpdatedBy,
			createdAt.Format(time.RFC3339),
			updatedAt.Format(time.RFC3339),
			i.dbs[i.rowIndex].Options.WorkersMin,
			i.dbs[i.rowIndex].Description,
		}
		i.rowIndex += 1
		return row, nil
	}
	return nil, types.ErrNoMoreRows
}
