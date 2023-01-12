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

// PlanOpCreateTable plan operator that creates a table.
type PlanOpCreateTable struct {
	planner       *ExecutionPlanner
	tableName     string
	failIfExists  bool
	isKeyed       bool
	keyPartitions int
	description   string
	columns       []*createTableField
	warnings      []string
}

// NewPlanOpCreateTable returns a new PlanOpCreateTable planoperator
func NewPlanOpCreateTable(p *ExecutionPlanner, tableName string, failIfExists bool, isKeyed bool, keyPartitions int, description string, columns []*createTableField) *PlanOpCreateTable {
	return &PlanOpCreateTable{
		planner:       p,
		tableName:     tableName,
		failIfExists:  failIfExists,
		isKeyed:       isKeyed,
		keyPartitions: keyPartitions,
		columns:       columns,
		description:   description,
		warnings:      make([]string, 0),
	}
}

func (p *PlanOpCreateTable) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_op"] = fmt.Sprintf("%T", p)
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
		description:   p.description,
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
	description   string
	columns       []*createTableField
}

var _ types.RowIterator = (*createTableRowIter)(nil)

func (i *createTableRowIter) Next(ctx context.Context) (types.Row, error) {
	//create the table

	fields := make([]*dax.Field, 0, len(i.columns)+1)

	// add the _id column
	var idType dax.BaseType = dax.BaseTypeID
	if i.isKeyed {
		idType = dax.BaseTypeString
	}
	fields = append(fields, &dax.Field{
		Name: "_id",
		Type: idType,
	})

	for _, f := range i.columns {
		fld, err := pilosa.FieldFromFieldOptions(dax.FieldName(f.name), f.fos...)
		if err != nil {
			return nil, errors.Wrapf(err, "creating field from field options: %s", f.name)
		}
		fields = append(fields, fld)
	}

	tbl := &dax.Table{
		Name:   dax.TableName(i.tableName),
		Fields: fields,
		// TODO(tlt): once we can support different partitionN's per table,
		// replace dax.DefaultPartitionN with i.keyPartitions.
		PartitionN: dax.DefaultPartitionN,

		Description: i.description,
	}

	if err := i.planner.schemaAPI.CreateTable(ctx, tbl); err != nil {
		if _, ok := errors.Cause(err).(pilosa.ConflictError); ok {
			if i.failIfExists {
				return nil, sql3.NewErrTableExists(0, 0, i.tableName)
			}
		} else {
			return nil, err
		}
	}
	return nil, types.ErrNoMoreRows
}
