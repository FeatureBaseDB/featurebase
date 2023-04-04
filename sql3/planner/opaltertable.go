// Copyright 2021 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"fmt"

	pilosa "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/sql3"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
)

// PlanOpAlterTable plan operator to alter a table.
type PlanOpAlterTable struct {
	planner       *ExecutionPlanner
	tableName     string
	operation     alterOperation
	oldColumnName string
	newColumnName string
	columnDef     *createTableField
	warnings      []string
}

func NewPlanOpAlterTable(p *ExecutionPlanner, tableName string, operation alterOperation, oldColumnName string, newColumnName string, columnDef *createTableField) *PlanOpAlterTable {
	return &PlanOpAlterTable{
		planner:       p,
		tableName:     tableName,
		operation:     operation,
		oldColumnName: oldColumnName,
		newColumnName: newColumnName,
		columnDef:     columnDef,
		warnings:      make([]string, 0),
	}
}

func (p *PlanOpAlterTable) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_op"] = fmt.Sprintf("%T", p)
	result["tableName"] = p.tableName
	return result
}

func (p *PlanOpAlterTable) AddWarning(warning string) {
	p.warnings = append(p.warnings, warning)
}

func (p *PlanOpAlterTable) Warnings() []string {
	return p.warnings
}

func (p *PlanOpAlterTable) String() string {
	return ""
}

func (p *PlanOpAlterTable) Schema() types.Schema {
	return types.Schema{}
}

func (p *PlanOpAlterTable) Children() []types.PlanOperator {
	return []types.PlanOperator{}
}

func (p *PlanOpAlterTable) Iterator(ctx context.Context, row types.Row) (types.RowIterator, error) {
	return &alterTableRowIter{
		planner:       p.planner,
		operation:     p.operation,
		tableName:     p.tableName,
		columnDef:     p.columnDef,
		oldColumnName: p.oldColumnName,
	}, nil
}

func (p *PlanOpAlterTable) WithChildren(children ...types.PlanOperator) (types.PlanOperator, error) {
	return nil, nil
}

type alterTableRowIter struct {
	planner       *ExecutionPlanner
	operation     alterOperation
	tableName     string
	columnDef     *createTableField
	oldColumnName string
}

var _ types.RowIterator = (*alterTableRowIter)(nil)

func (i *alterTableRowIter) Next(ctx context.Context) (types.Row, error) {
	switch i.operation {
	case alterOpAdd:
		tname := dax.TableName(i.tableName)
		fname := dax.FieldName(i.columnDef.name)
		fos := i.columnDef.fos

		fld, err := pilosa.FieldFromFieldOptions(fname, fos...)
		if err != nil {
			return nil, err
		}
		// all newly created fields unconditionally have TrackExistence turned on.
		fld.Options.TrackExistence = true

		if err := i.planner.schemaAPI.CreateField(ctx, tname, fld); err != nil {
			return nil, err
		}

	case alterOpDrop:
		err := i.planner.schemaAPI.DeleteField(ctx, dax.TableName(i.tableName), dax.FieldName(i.oldColumnName))
		if err != nil {
			return nil, err
		}

	case alterOpRename:
		return nil, sql3.NewErrInternal("column rename is unimplemented")

	}
	return nil, types.ErrNoMoreRows
}
