// Copyright 2021 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"fmt"

	pilosa "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
	"github.com/pkg/errors"
)

// PlanOpCreateTable plan operator that creates a table.
type PlanOpCreateTable struct {
	planner       *ExecutionPlanner
	tableName     string
	failIfExists  bool
	isKeyed       bool
	keyPartitions int
	columns       []*createTableField
	warnings      []string
}

func NewPlanOpCreateTable(p *ExecutionPlanner, tableName string, failIfExists bool, isKeyed bool, keyPartitions int, columns []*createTableField) *PlanOpCreateTable {
	return &PlanOpCreateTable{
		planner:       p,
		tableName:     tableName,
		failIfExists:  failIfExists,
		isKeyed:       isKeyed,
		keyPartitions: keyPartitions,
		columns:       columns,
		warnings:      make([]string, 0),
	}
}

func (p *PlanOpCreateTable) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_op"] = fmt.Sprintf("%T", p)
	ps := make([]string, 0)
	for _, e := range p.Schema() {
		ps = append(ps, fmt.Sprintf("'%s', '%s', '%s'", e.ColumnName, e.RelationName, e.Type.TypeDescription()))
	}
	result["_schema"] = ps
	result["name"] = p.tableName
	result["failIfExists"] = p.failIfExists
	return result
}

func (p *PlanOpCreateTable) String() string {
	return ""
}

func (p *PlanOpCreateTable) AddWarning(warning string) {
	p.warnings = append(p.warnings, warning)
}

func (p *PlanOpCreateTable) Warnings() []string {
	return p.warnings
}

func (p *PlanOpCreateTable) Schema() types.Schema {
	return types.Schema{}
}

func (p *PlanOpCreateTable) Children() []types.PlanOperator {
	return []types.PlanOperator{}
}

func (p *PlanOpCreateTable) Iterator(ctx context.Context, row types.Row) (types.RowIterator, error) {
	return &createTableRowIter{
		planner:       p.planner,
		tableName:     p.tableName,
		failIfExists:  p.failIfExists,
		isKeyed:       p.isKeyed,
		keyPartitions: p.keyPartitions,
		columns:       p.columns,
	}, nil
}

func (p *PlanOpCreateTable) WithChildren(children ...types.PlanOperator) (types.PlanOperator, error) {
	return nil, nil
}

type createTableRowIter struct {
	planner       *ExecutionPlanner
	tableName     string
	failIfExists  bool
	isKeyed       bool
	keyPartitions int
	columns       []*createTableField
}

var _ types.RowIterator = (*createTableRowIter)(nil)

func (i *createTableRowIter) Next(ctx context.Context) (types.Row, error) {
	//create the table
	options := pilosa.IndexOptions{
		Keys:           i.isKeyed,
		TrackExistence: true,
		PartitionN:     i.keyPartitions,
	}

	fields := make([]pilosa.CreateFieldObj, len(i.columns))
	for i, f := range i.columns {
		fields[i] = pilosa.CreateFieldObj{
			Name:    f.name,
			Options: f.fos,
		}
	}

	// TODO (pok) add ability to add description here
	if err := i.planner.schemaAPI.CreateIndexAndFields(ctx, i.tableName, options, fields); err != nil {
		if _, ok := errors.Cause(err).(pilosa.ConflictError); ok {
			if i.failIfExists {
				return nil, err
			}
		} else {
			return nil, err
		}
	}
	return nil, types.ErrNoMoreRows
}
