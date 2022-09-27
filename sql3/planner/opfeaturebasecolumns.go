// Copyright 2021 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"fmt"
	"time"

	pilosa "github.com/molecula/featurebase/v3"
	"github.com/molecula/featurebase/v3/sql3/parser"
	"github.com/molecula/featurebase/v3/sql3/planner/types"
)

// PlanOpFeatureBaseColumns wraps an Index that is returned from schemaAPI.Schema().
type PlanOpFeatureBaseColumns struct {
	index    *pilosa.IndexInfo
	warnings []string
}

func NewPlanOpFeatureBaseColumns(index *pilosa.IndexInfo) *PlanOpFeatureBaseColumns {
	node := &PlanOpFeatureBaseColumns{
		index:    index,
		warnings: make([]string, 0),
	}
	return node
}

func (p *PlanOpFeatureBaseColumns) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_op"] = fmt.Sprintf("%T", p)
	ps := make([]string, 0)
	for _, e := range p.Schema() {
		ps = append(ps, fmt.Sprintf("'%s', '%s', '%s'", e.ColumnName, e.RelationName, e.Type.TypeName()))
	}
	result["_schema"] = ps
	return result
}

func (p *PlanOpFeatureBaseColumns) String() string {
	return ""
}

func (p *PlanOpFeatureBaseColumns) AddWarning(warning string) {
	p.warnings = append(p.warnings, warning)
}

func (p *PlanOpFeatureBaseColumns) Warnings() []string {
	return p.warnings
}

func (p *PlanOpFeatureBaseColumns) Schema() types.Schema {
	return types.Schema{
		&types.PlannerColumn{
			RelationName: "fb$table_columns",
			ColumnName:   "name",
			Type:         parser.NewDataTypeString(),
		},
		&types.PlannerColumn{
			RelationName: "fb$table_columns",
			ColumnName:   "type",
			Type:         parser.NewDataTypeString(),
		},
		&types.PlannerColumn{
			RelationName: "fb$table_columns",
			ColumnName:   "internal_type",
			Type:         parser.NewDataTypeString(),
		},
		&types.PlannerColumn{
			RelationName: "fb$table_columns",
			ColumnName:   "created_at",
			Type:         parser.NewDataTypeTimestamp(),
		},
		&types.PlannerColumn{
			RelationName: "fb$table_columns",
			ColumnName:   "keys",
			Type:         parser.NewDataTypeBool(),
		},
		&types.PlannerColumn{
			RelationName: "fb$table_columns",
			ColumnName:   "cache_type",
			Type:         parser.NewDataTypeString(),
		},
		&types.PlannerColumn{
			RelationName: "fb$table_columns",
			ColumnName:   "cache_size",
			Type:         parser.NewDataTypeInt(),
		},
		&types.PlannerColumn{
			RelationName: "fb$table_columns",
			ColumnName:   "scale",
			Type:         parser.NewDataTypeInt(),
		},
		&types.PlannerColumn{
			RelationName: "fb$table_columns",
			ColumnName:   "min",
			Type:         parser.NewDataTypeInt(),
		},
		&types.PlannerColumn{
			RelationName: "fb$table_columns",
			ColumnName:   "max",
			Type:         parser.NewDataTypeInt(),
		},
		&types.PlannerColumn{
			RelationName: "fb$table_columns",
			ColumnName:   "timeunit",
			Type:         parser.NewDataTypeString(),
		},
		&types.PlannerColumn{
			RelationName: "fb$table_columns",
			ColumnName:   "epoch",
			Type:         parser.NewDataTypeInt(),
		},
		&types.PlannerColumn{
			RelationName: "fb$table_columns",
			ColumnName:   "timequantum",
			Type:         parser.NewDataTypeString(),
		},
		&types.PlannerColumn{
			RelationName: "fb$table_columns",
			ColumnName:   "ttl",
			Type:         parser.NewDataTypeInt(),
		},
	}
}

func (p *PlanOpFeatureBaseColumns) Children() []types.PlanOperator {
	return []types.PlanOperator{}
}

func (p *PlanOpFeatureBaseColumns) Iterator(ctx context.Context, row types.Row) (types.RowIterator, error) {
	return &showColumnsRowIter{
		index: p.index,
	}, nil
}

func (p *PlanOpFeatureBaseColumns) WithChildren(children ...types.PlanOperator) (types.PlanOperator, error) {
	return NewPlanOpFeatureBaseColumns(p.index), nil
}

type showColumnsRowIter struct {
	index    *pilosa.IndexInfo
	rowIndex int
}

var _ types.RowIterator = (*showColumnsRowIter)(nil)

func (i *showColumnsRowIter) Next(ctx context.Context) (types.Row, error) {
	if i.rowIndex < len(i.index.Fields) {
		fields := i.index.Fields

		tm := time.Unix(0, fields[i.rowIndex].CreatedAt)

		row := []interface{}{
			fields[i.rowIndex].Name,
			fieldSQLDataType(fields[i.rowIndex]).TypeName(),
			fields[i.rowIndex].Options.Type,
			tm.Format(time.RFC3339),
			fields[i.rowIndex].Options.Keys,
			fields[i.rowIndex].Options.CacheType,
			fields[i.rowIndex].Options.CacheSize,
			fields[i.rowIndex].Options.Scale,
			fields[i.rowIndex].Options.Min.ToInt64(0),
			fields[i.rowIndex].Options.Max.ToInt64(0),
			fields[i.rowIndex].Options.TimeUnit,
			0, //TODO(pok) get Epoch from somewhere?
			fields[i.rowIndex].Options.TimeQuantum.String(),
			fields[i.rowIndex].Options.TTL.String(),
		}

		i.rowIndex += 1
		return row, nil
	}
	return nil, types.ErrNoMoreRows
}
