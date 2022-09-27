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

// PlanOpFeatureBaseTables wraps a []*IndexInfo that is returned from
// schemaAPI.Schema().
type PlanOpFeatureBaseTables struct {
	indexInfo []*pilosa.IndexInfo
	warnings  []string
}

func NewPlanOpFeatureBaseTables(indexInfo []*pilosa.IndexInfo) *PlanOpFeatureBaseTables {
	return &PlanOpFeatureBaseTables{
		indexInfo: indexInfo,
		warnings:  make([]string, 0),
	}
}

func (p *PlanOpFeatureBaseTables) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_op"] = fmt.Sprintf("%T", p)
	ps := make([]string, 0)
	for _, e := range p.Schema() {
		ps = append(ps, fmt.Sprintf("'%s', '%s', '%s'", e.ColumnName, e.RelationName, e.Type.TypeName()))
	}
	result["_schema"] = ps
	return result
}

func (p *PlanOpFeatureBaseTables) String() string {
	return ""
}

func (p *PlanOpFeatureBaseTables) AddWarning(warning string) {
	p.warnings = append(p.warnings, warning)
}

func (p *PlanOpFeatureBaseTables) Warnings() []string {
	return p.warnings
}

func (p *PlanOpFeatureBaseTables) Schema() types.Schema {
	return types.Schema{
		&types.PlannerColumn{
			RelationName: "fb$tables",
			ColumnName:   "name",
			Type:         parser.NewDataTypeString(),
		},
		&types.PlannerColumn{
			RelationName: "fb$tables",
			ColumnName:   "created_at",
			Type:         parser.NewDataTypeTimestamp(),
		},
		&types.PlannerColumn{
			RelationName: "fb$tables",
			ColumnName:   "track_existence",
			Type:         parser.NewDataTypeBool(),
		},
		&types.PlannerColumn{
			RelationName: "fb$tables",
			ColumnName:   "keys",
			Type:         parser.NewDataTypeBool(),
		},
		&types.PlannerColumn{
			RelationName: "fb$tables",
			ColumnName:   "shard_width",
			Type:         parser.NewDataTypeInt(),
		},
	}
}

func (p *PlanOpFeatureBaseTables) Children() []types.PlanOperator {
	return []types.PlanOperator{}
}

func (p *PlanOpFeatureBaseTables) Iterator(ctx context.Context, row types.Row) (types.RowIterator, error) {
	return &showTablesRowIter{
		indexInfo: p.indexInfo,
	}, nil
}

func (p *PlanOpFeatureBaseTables) WithChildren(children ...types.PlanOperator) (types.PlanOperator, error) {
	return NewPlanOpFeatureBaseTables(p.indexInfo), nil
}

type showTablesRowIter struct {
	indexInfo []*pilosa.IndexInfo
	rowIndex  int
}

var _ types.RowIterator = (*showTablesRowIter)(nil)

func (i *showTablesRowIter) Next(ctx context.Context) (types.Row, error) {
	if i.rowIndex < len(i.indexInfo) {
		tm := time.Unix(0, i.indexInfo[i.rowIndex].CreatedAt)
		row := []interface{}{
			i.indexInfo[i.rowIndex].Name,
			tm.Format(time.RFC3339),
			i.indexInfo[i.rowIndex].Options.TrackExistence,
			i.indexInfo[i.rowIndex].Options.Keys,
			i.indexInfo[i.rowIndex].ShardWidth,
		}
		i.rowIndex += 1
		return row, nil
	}
	return nil, types.ErrNoMoreRows
}
