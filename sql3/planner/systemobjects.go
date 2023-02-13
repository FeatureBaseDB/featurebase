// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"time"

	pilosa "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/sql3"
	"github.com/featurebasedb/featurebase/v3/sql3/parser"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
)

type viewSystemObject struct {
	name      string
	statement string
}

func (p *ExecutionPlanner) ensureViewsSystemTableExists(ctx context.Context) error {
	_, err := p.schemaAPI.TableByName(ctx, "fb_views")
	if err != nil {
		if !isTableNotFoundError(err) {
			return err
		}

		//  create table fb_views (
		// 		_id string
		//		name string
		//		statement string
		//		owner string
		//		updated_by string
		//		created_at timestamp
		//		updated_at timestamp
		//  );

		// if it doesn't, create it by making the appropriate iterator
		iter := &createTableRowIter{
			planner:       p,
			tableName:     "fb_views",
			failIfExists:  false,
			isKeyed:       true,
			keyPartitions: 0,
			columns: []*createTableField{
				{
					planner:  p,
					name:     "name",
					typeName: dax.BaseTypeString,
					fos: []pilosa.FieldOption{
						pilosa.OptFieldTypeMutex(pilosa.DefaultCacheType, pilosa.DefaultCacheSize),
						pilosa.OptFieldKeys(),
					},
				},
				{
					planner:  p,
					name:     "statement",
					typeName: dax.BaseTypeString,
					fos: []pilosa.FieldOption{
						pilosa.OptFieldTypeMutex(pilosa.DefaultCacheType, pilosa.DefaultCacheSize),
						pilosa.OptFieldKeys(),
					},
				},
				{
					planner:  p,
					name:     "owner",
					typeName: dax.BaseTypeString,
					fos: []pilosa.FieldOption{
						pilosa.OptFieldTypeMutex(pilosa.DefaultCacheType, pilosa.DefaultCacheSize),
						pilosa.OptFieldKeys(),
					},
				},
				{
					planner:  p,
					name:     "updated_by",
					typeName: dax.BaseTypeString,
					fos: []pilosa.FieldOption{
						pilosa.OptFieldTypeMutex(pilosa.DefaultCacheType, pilosa.DefaultCacheSize),
						pilosa.OptFieldKeys(),
					},
				},
				{
					planner:  p,
					name:     "created_at",
					typeName: dax.BaseTypeTimestamp,
					fos: []pilosa.FieldOption{
						pilosa.OptFieldTypeTimestamp(pilosa.DefaultEpoch, pilosa.TimeUnitSeconds),
					},
				},
				{
					planner:  p,
					name:     "updated_at",
					typeName: dax.BaseTypeTimestamp,
					fos: []pilosa.FieldOption{
						pilosa.OptFieldTypeTimestamp(pilosa.DefaultEpoch, pilosa.TimeUnitSeconds),
					},
				},
			},
			description: "system table for views",
		}
		// call next on our iterator to create the table
		_, err := iter.Next(ctx)
		if err != nil && err != types.ErrNoMoreRows {
			return err
		}
	}
	return nil
}

func (p *ExecutionPlanner) getViewByName(ctx context.Context, name string) (*viewSystemObject, error) {
	err := p.ensureViewsSystemTableExists(ctx)
	if err != nil {
		return nil, err
	}

	tbl, err := p.schemaAPI.TableByName(ctx, "fb_views")
	if err != nil {
		return nil, sql3.NewErrTableNotFound(0, 0, "fb_views")
	}

	cols := make([]string, len(tbl.Fields))
	for i, c := range tbl.Fields {
		cols[i] = string(c.Name)
	}

	iter := &tableScanRowIter{
		planner:   p,
		tableName: "fb_views",
		columns:   cols,
		predicate: newBinOpPlanExpression(
			newQualifiedRefPlanExpression("fb_views", "_id", 0, parser.NewDataTypeString()),
			parser.EQ,
			newStringLiteralPlanExpression(name),
			parser.NewDataTypeBool(),
		),
		topExpr: nil,
	}

	row, err := iter.Next(ctx)
	if err != nil {
		if err == types.ErrNoMoreRows {
			// view does not exist
			return nil, nil
		}
		return nil, err
	}

	return &viewSystemObject{
		name:      row[1].(string),
		statement: row[2].(string),
	}, nil
}

func (p *ExecutionPlanner) insertView(ctx context.Context, view *viewSystemObject) error {
	err := p.ensureViewsSystemTableExists(ctx)
	if err != nil {
		return err
	}

	createTime := time.Now().UTC()

	iter := &insertRowIter{
		planner:   p,
		tableName: "fb_views",
		targetColumns: []*qualifiedRefPlanExpression{
			newQualifiedRefPlanExpression("fb_views", "_id", 0, parser.NewDataTypeString()),
			newQualifiedRefPlanExpression("fb_views", "name", 0, parser.NewDataTypeString()),
			newQualifiedRefPlanExpression("fb_views", "statement", 0, parser.NewDataTypeString()),
			newQualifiedRefPlanExpression("fb_views", "owner", 0, parser.NewDataTypeString()),
			newQualifiedRefPlanExpression("fb_views", "updated_by", 0, parser.NewDataTypeString()),
			newQualifiedRefPlanExpression("fb_views", "created_at", 0, parser.NewDataTypeTimestamp()),
			newQualifiedRefPlanExpression("fb_views", "updated_at", 0, parser.NewDataTypeTimestamp()),
		},
		insertValues: [][]types.PlanExpression{
			{
				newStringLiteralPlanExpression(view.name),
				newStringLiteralPlanExpression(view.name),
				newStringLiteralPlanExpression(view.statement),
				newStringLiteralPlanExpression(""),
				newStringLiteralPlanExpression(""),
				newDateLiteralPlanExpression(createTime),
				newDateLiteralPlanExpression(createTime),
			},
		},
	}
	_, err = iter.Next(ctx)
	if err != nil && err != types.ErrNoMoreRows {
		return err
	}
	return nil
}

func (p *ExecutionPlanner) updateView(ctx context.Context, view *viewSystemObject) error {
	err := p.ensureViewsSystemTableExists(ctx)
	if err != nil {
		return err
	}

	updateTime := time.Now().UTC()

	iter := &insertRowIter{
		planner:   p,
		tableName: "fb_views",
		targetColumns: []*qualifiedRefPlanExpression{
			newQualifiedRefPlanExpression("fb_views", "_id", 0, parser.NewDataTypeString()),
			newQualifiedRefPlanExpression("fb_views", "statement", 0, parser.NewDataTypeString()),
			newQualifiedRefPlanExpression("fb_views", "updated_by", 0, parser.NewDataTypeString()),
			newQualifiedRefPlanExpression("fb_views", "updated_at", 0, parser.NewDataTypeTimestamp()),
		},
		insertValues: [][]types.PlanExpression{
			{
				newStringLiteralPlanExpression(view.name),
				newStringLiteralPlanExpression(view.statement),
				newStringLiteralPlanExpression(""),
				newDateLiteralPlanExpression(updateTime),
			},
		},
	}
	_, err = iter.Next(ctx)
	if err != nil && err != types.ErrNoMoreRows {
		return err
	}
	return nil
}

func (p *ExecutionPlanner) deleteView(ctx context.Context, viewName string) error {
	err := p.ensureViewsSystemTableExists(ctx)
	if err != nil {
		return err
	}

	iter := &filteredDeleteRowIter{
		planner:   p,
		tableName: "fb_views",
		filter: newBinOpPlanExpression(
			newQualifiedRefPlanExpression("fb_views", "_id", 0, parser.NewDataTypeString()),
			parser.EQ,
			newStringLiteralPlanExpression(viewName),
			parser.NewDataTypeBool(),
		),
	}
	_, err = iter.Next(ctx)
	if err != nil && err != types.ErrNoMoreRows {
		return err
	}
	return nil
}
