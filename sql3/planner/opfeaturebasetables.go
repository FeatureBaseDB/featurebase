// Copyright 2021 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"fmt"
	"strings"
	"time"

	pilosa "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/sql3/parser"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
)

// PlanOpFeatureBaseTables wraps a []*IndexInfo that is returned from
// schemaAPI.Schema().
type PlanOpFeatureBaseTables struct {
	planner    *ExecutionPlanner
	indexInfo  []*pilosa.IndexInfo
	withSystem bool
	warnings   []string
}

func NewPlanOpFeatureBaseTables(planner *ExecutionPlanner, indexInfo []*pilosa.IndexInfo, withSystem bool) *PlanOpFeatureBaseTables {
	return &PlanOpFeatureBaseTables{
		planner:    planner,
		indexInfo:  indexInfo,
		withSystem: withSystem,
		warnings:   make([]string, 0),
	}
}

func (p *PlanOpFeatureBaseTables) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_op"] = fmt.Sprintf("%T", p)
	result["_schema"] = p.Schema().Plan()
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
			RelationName: "fb_tables",
			ColumnName:   string(dax.PrimaryKeyFieldName),
			Type:         parser.NewDataTypeString(),
		},
		&types.PlannerColumn{
			RelationName: "fb_tables",
			ColumnName:   "name",
			Type:         parser.NewDataTypeString(),
		},
		&types.PlannerColumn{
			RelationName: "fb_tables",
			ColumnName:   "owner",
			Type:         parser.NewDataTypeString(),
		},
		&types.PlannerColumn{
			RelationName: "fb_tables",
			ColumnName:   "updated_by",
			Type:         parser.NewDataTypeString(),
		},
		&types.PlannerColumn{
			RelationName: "fb_tables",
			ColumnName:   "created_at",
			Type:         parser.NewDataTypeTimestamp(),
		},
		&types.PlannerColumn{
			RelationName: "fb_tables",
			ColumnName:   "updated_at",
			Type:         parser.NewDataTypeTimestamp(),
		},
		&types.PlannerColumn{
			RelationName: "fb_tables",
			ColumnName:   "keys",
			Type:         parser.NewDataTypeBool(),
		},
		&types.PlannerColumn{
			RelationName: "fb_tables",
			ColumnName:   "space_used",
			Type:         parser.NewDataTypeInt(),
		},
		&types.PlannerColumn{
			RelationName: "fb_tables",
			ColumnName:   "description",
			Type:         parser.NewDataTypeString(),
		},
	}
}

func (p *PlanOpFeatureBaseTables) Children() []types.PlanOperator {
	return []types.PlanOperator{}
}

func (p *PlanOpFeatureBaseTables) Iterator(ctx context.Context, row types.Row) (types.RowIterator, error) {
	return &showTablesRowIter{
		planner:    p.planner,
		indexInfo:  p.indexInfo,
		withSystem: p.withSystem,
	}, nil
}

func (p *PlanOpFeatureBaseTables) WithChildren(children ...types.PlanOperator) (types.PlanOperator, error) {
	return NewPlanOpFeatureBaseTables(p.planner, p.indexInfo, p.withSystem), nil
}

type showTablesRowIter struct {
	planner    *ExecutionPlanner
	indexInfo  []*pilosa.IndexInfo
	withSystem bool

	result types.Rows
}

var _ types.RowIterator = (*showTablesRowIter)(nil)

func (i *showTablesRowIter) Next(ctx context.Context) (types.Row, error) {
	if i.result == nil {
		i.result = make(types.Rows, 0)

		for _, idx := range i.indexInfo {

			indexName := idx.Name

			// if we don't want system tables filter them out (currently by name prefix)
			// TODO(pok) - we need an is_system attribute so we can filter on that instead
			if !i.withSystem && strings.HasPrefix(indexName, "fb_") {
				continue
			}

			var err error
			var spaceUsed pilosa.DiskUsage
			switch strings.ToLower(indexName) {
			case fbDatabaseInfo, fbDatabaseNodes, fbPerformanceCounters, fbExecRequests, fbTableDDL:
				spaceUsed = pilosa.DiskUsage{
					Usage: 0,
				}
			default:
				u := i.planner.systemAPI.DataDir()

				// TODO(tlt): GetDiskUsage needs to be behind an interface because
				// this doesn't work in serverless. For now I'm just going to skip
				// it based on the emtpy DataDir, but let's do this the right way.
				if u != "" {
					u = fmt.Sprintf("%s/indexes/%s", u, indexName)

					spaceUsed, err = pilosa.GetDiskUsage(u)
					if err != nil {
						return nil, err
					}
				}
			}

			createdAt := time.Unix(0, idx.CreatedAt)
			updatedAt := time.Unix(0, idx.UpdatedAt)
			row := []interface{}{
				indexName,
				indexName,
				idx.Owner,
				idx.LastUpdateUser,
				createdAt.Format(time.RFC3339),
				updatedAt.Format(time.RFC3339),
				idx.Options.Keys,
				spaceUsed.Usage,
				idx.Options.Description,
			}
			i.result = append(i.result, row)
		}
	}

	if len(i.result) > 0 {
		row := i.result[0]

		// Move to next result element.
		i.result = i.result[1:]
		return row, nil
	}
	return nil, types.ErrNoMoreRows
}
