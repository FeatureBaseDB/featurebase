// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"encoding/json"
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

type functionSystemObject struct {
	name     string
	language string
	body     string
}

type modelSystemObject struct {
	name         string
	status       string
	modelType    string
	labels       []string
	inputColumns []string
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
			newQualifiedRefPlanExpression("fb_views", string(dax.PrimaryKeyFieldName), 0, parser.NewDataTypeString()),
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
			newQualifiedRefPlanExpression("fb_views", string(dax.PrimaryKeyFieldName), 0, parser.NewDataTypeString()),
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
				newTimestampLiteralPlanExpression(createTime),
				newTimestampLiteralPlanExpression(createTime),
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
			newQualifiedRefPlanExpression("fb_views", string(dax.PrimaryKeyFieldName), 0, parser.NewDataTypeString()),
			newQualifiedRefPlanExpression("fb_views", "statement", 0, parser.NewDataTypeString()),
			newQualifiedRefPlanExpression("fb_views", "updated_by", 0, parser.NewDataTypeString()),
			newQualifiedRefPlanExpression("fb_views", "updated_at", 0, parser.NewDataTypeTimestamp()),
		},
		insertValues: [][]types.PlanExpression{
			{
				newStringLiteralPlanExpression(view.name),
				newStringLiteralPlanExpression(view.statement),
				newStringLiteralPlanExpression(""),
				newTimestampLiteralPlanExpression(updateTime),
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
			newQualifiedRefPlanExpression("fb_views", string(dax.PrimaryKeyFieldName), 0, parser.NewDataTypeString()),
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

func (p *ExecutionPlanner) ensureFunctionsSystemTableExists() error {
	_, err := p.schemaAPI.TableByName(context.Background(), "fb_functions")
	if err != nil {
		if !isTableNotFoundError(err) {
			return err
		}

		//  create table fb_functions (
		// 		_id string
		//		name string
		//		language string
		//		body string
		//		owner string
		//		updated_by string
		//		created_at timestamp
		//		updated_at timestamp
		//  );

		// if it doesn't, create it by making the appropriate iterator
		iter := &createTableRowIter{
			planner:       p,
			tableName:     "fb_functions",
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
					name:     "language",
					typeName: dax.BaseTypeString,
					fos: []pilosa.FieldOption{
						pilosa.OptFieldTypeMutex(pilosa.DefaultCacheType, pilosa.DefaultCacheSize),
						pilosa.OptFieldKeys(),
					},
				},
				{
					planner:  p,
					name:     "body",
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
			description: "system table for functions",
		}
		// call next on our iterator to create the table
		_, err := iter.Next(context.Background())
		if err != nil && err != types.ErrNoMoreRows {
			return err
		}
	}
	return nil
}

func (p *ExecutionPlanner) getFunctionByName(name string) (*functionSystemObject, error) {
	err := p.ensureFunctionsSystemTableExists()
	if err != nil {
		return nil, err
	}

	tbl, err := p.schemaAPI.TableByName(context.Background(), "fb_functions")
	if err != nil {
		return nil, sql3.NewErrTableNotFound(0, 0, "fb_functions")
	}

	cols := make([]string, len(tbl.Fields))
	for i, c := range tbl.Fields {
		cols[i] = string(c.Name)
	}

	iter := &tableScanRowIter{
		planner:   p,
		tableName: "fb_functions",
		columns:   cols,
		predicate: newBinOpPlanExpression(
			newQualifiedRefPlanExpression("fb_functions", "_id", 0, parser.NewDataTypeString()),
			parser.EQ,
			newStringLiteralPlanExpression(name),
			parser.NewDataTypeBool(),
		),
		topExpr: nil,
	}

	row, err := iter.Next(context.Background())
	if err != nil {
		if err == types.ErrNoMoreRows {
			// view does not exist
			return nil, nil
		}
		return nil, err
	}

	return &functionSystemObject{
		name:     row[1].(string),
		language: row[2].(string),
		body:     row[3].(string),
	}, nil
}

func (p *ExecutionPlanner) insertFunction(function *functionSystemObject) error {
	err := p.ensureFunctionsSystemTableExists()
	if err != nil {
		return err
	}

	createTime := time.Now().UTC()

	iter := &insertRowIter{
		planner:   p,
		tableName: "fb_functions",
		targetColumns: []*qualifiedRefPlanExpression{
			newQualifiedRefPlanExpression("fb_functions", "_id", 0, parser.NewDataTypeString()),
			newQualifiedRefPlanExpression("fb_functions", "name", 0, parser.NewDataTypeString()),
			newQualifiedRefPlanExpression("fb_functions", "language", 0, parser.NewDataTypeString()),
			newQualifiedRefPlanExpression("fb_functions", "body", 0, parser.NewDataTypeString()),
			newQualifiedRefPlanExpression("fb_functions", "owner", 0, parser.NewDataTypeString()),
			newQualifiedRefPlanExpression("fb_functions", "updated_by", 0, parser.NewDataTypeString()),
			newQualifiedRefPlanExpression("fb_functions", "created_at", 0, parser.NewDataTypeTimestamp()),
			newQualifiedRefPlanExpression("fb_functions", "updated_at", 0, parser.NewDataTypeTimestamp()),
		},
		insertValues: [][]types.PlanExpression{
			{
				newStringLiteralPlanExpression(function.name),
				newStringLiteralPlanExpression(function.name),
				newStringLiteralPlanExpression(function.language),
				newStringLiteralPlanExpression(function.body),
				newStringLiteralPlanExpression(""),
				newStringLiteralPlanExpression(""),
				newTimestampLiteralPlanExpression(createTime),
				newTimestampLiteralPlanExpression(createTime),
			},
		},
	}
	_, err = iter.Next(context.Background())
	if err != nil && err != types.ErrNoMoreRows {
		return err
	}
	return nil
}

func (p *ExecutionPlanner) updateFunction(function *functionSystemObject) error {
	err := p.ensureFunctionsSystemTableExists()
	if err != nil {
		return err
	}

	updateTime := time.Now().UTC()

	iter := &insertRowIter{
		planner:   p,
		tableName: "fb_functions",
		targetColumns: []*qualifiedRefPlanExpression{
			newQualifiedRefPlanExpression("fb_functions", "_id", 0, parser.NewDataTypeString()),
			newQualifiedRefPlanExpression("fb_functions", "language", 0, parser.NewDataTypeString()),
			newQualifiedRefPlanExpression("fb_functions", "body", 0, parser.NewDataTypeString()),
			newQualifiedRefPlanExpression("fb_functions", "updated_by", 0, parser.NewDataTypeString()),
			newQualifiedRefPlanExpression("fb_functions", "updated_at", 0, parser.NewDataTypeTimestamp()),
		},
		insertValues: [][]types.PlanExpression{
			{
				newStringLiteralPlanExpression(function.name),
				newStringLiteralPlanExpression(function.language),
				newStringLiteralPlanExpression(function.body),
				newStringLiteralPlanExpression(""),
				newTimestampLiteralPlanExpression(updateTime),
			},
		},
	}
	_, err = iter.Next(context.Background())
	if err != nil && err != types.ErrNoMoreRows {
		return err
	}
	return nil
}

func (p *ExecutionPlanner) deleteFunction(functionName string) error {
	err := p.ensureFunctionsSystemTableExists()
	if err != nil {
		return err
	}

	iter := &filteredDeleteRowIter{
		planner:   p,
		tableName: "fb_functions",
		filter: newBinOpPlanExpression(
			newQualifiedRefPlanExpression("fb_functions", "_id", 0, parser.NewDataTypeString()),
			parser.EQ,
			newStringLiteralPlanExpression(functionName),
			parser.NewDataTypeBool(),
		),
	}
	_, err = iter.Next(context.Background())
	if err != nil && err != types.ErrNoMoreRows {
		return err
	}
	return nil
}

func (p *ExecutionPlanner) ensureModelsSystemTableExists() error {
	_, err := p.schemaAPI.TableByName(context.Background(), "fb_models")
	if err != nil {
		if !isTableNotFoundError(err) {
			return err
		}
		//  create table fb_models (
		// 		_id string
		//		name string
		//		status string
		//		model_type string
		//		labels string --this is an array of string, we'll store it as a json object until we have string[] type in sql
		//		input_columns string  --this is an array for string, we'll store it as a json object until we have string[] type in sql
		//		owner string
		//		updated_by string
		//		created_at timestamp
		//		updated_at timestamp
		//  );

		// if it doesn't, create it by making the appropriate iterator
		iter := &createTableRowIter{
			planner:       p,
			tableName:     "fb_models",
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
					name:     "status",
					typeName: dax.BaseTypeString,
					fos: []pilosa.FieldOption{
						pilosa.OptFieldTypeMutex(pilosa.DefaultCacheType, pilosa.DefaultCacheSize),
						pilosa.OptFieldKeys(),
					},
				},
				{
					planner:  p,
					name:     "model_type",
					typeName: dax.BaseTypeString,
					fos: []pilosa.FieldOption{
						pilosa.OptFieldTypeMutex(pilosa.DefaultCacheType, pilosa.DefaultCacheSize),
						pilosa.OptFieldKeys(),
					},
				},
				{
					planner:  p,
					name:     "labels",
					typeName: dax.BaseTypeString,
					fos: []pilosa.FieldOption{
						pilosa.OptFieldTypeMutex(pilosa.DefaultCacheType, pilosa.DefaultCacheSize),
						pilosa.OptFieldKeys(),
					},
				},
				{
					planner:  p,
					name:     "input_columns",
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
			description: "system table for models",
		}
		// call next on our iterator to create the table
		_, err := iter.Next(context.Background())
		if err != nil && err != types.ErrNoMoreRows {
			return err
		}
	}
	return nil
}

func (p *ExecutionPlanner) ensureModelDataSystemTableExists() error {
	_, err := p.schemaAPI.TableByName(context.Background(), "fb_model_data")
	if err != nil {
		if !isTableNotFoundError(err) {
			return err
		}
		//  create table fb_model_data (
		// 		_id string
		//		model_id string
		//		data string
		//  );

		// if it doesn't, create it by making the appropriate iterator
		iter := &createTableRowIter{
			planner:       p,
			tableName:     "fb_model_data",
			failIfExists:  false,
			isKeyed:       true,
			keyPartitions: 0,
			columns: []*createTableField{
				{
					planner:  p,
					name:     "model_id",
					typeName: dax.BaseTypeString,
					fos: []pilosa.FieldOption{
						pilosa.OptFieldTypeMutex(pilosa.DefaultCacheType, pilosa.DefaultCacheSize),
						pilosa.OptFieldKeys(),
					},
				},
				{
					planner:  p,
					name:     "data",
					typeName: dax.BaseTypeString,
					fos: []pilosa.FieldOption{
						pilosa.OptFieldTypeMutex(pilosa.DefaultCacheType, pilosa.DefaultCacheSize),
						pilosa.OptFieldKeys(),
					},
				},
			},
			description: "system table for model data",
		}
		// call next on our iterator to create the table
		_, err := iter.Next(context.Background())
		if err != nil && err != types.ErrNoMoreRows {
			return err
		}
	}
	return nil
}

func (p *ExecutionPlanner) getModelByName(name string) (*modelSystemObject, error) {
	err := p.ensureModelsSystemTableExists()
	if err != nil {
		return nil, err
	}

	tbl, err := p.schemaAPI.TableByName(context.Background(), "fb_models")
	if err != nil {
		return nil, sql3.NewErrTableNotFound(0, 0, "fb_models")
	}

	cols := make([]string, len(tbl.Fields))
	for i, c := range tbl.Fields {
		cols[i] = string(c.Name)
	}

	iter := &tableScanRowIter{
		planner:   p,
		tableName: "fb_models",
		columns:   cols,
		predicate: newBinOpPlanExpression(
			newQualifiedRefPlanExpression("fb_models", "_id", 0, parser.NewDataTypeString()),
			parser.EQ,
			newStringLiteralPlanExpression(name),
			parser.NewDataTypeBool(),
		),
		topExpr: nil,
	}

	row, err := iter.Next(context.Background())
	if err != nil {
		if err == types.ErrNoMoreRows {
			// model does not exist
			return nil, nil
		}
		return nil, err
	}

	labels := make([]string, 0)
	err = json.Unmarshal([]byte(row[4].(string)), &labels)
	if err != nil {
		return nil, err
	}

	inputColumns := make([]string, 0)
	err = json.Unmarshal([]byte(row[5].(string)), &inputColumns)
	if err != nil {
		return nil, err
	}

	return &modelSystemObject{
		name:         row[1].(string),
		status:       row[2].(string),
		modelType:    row[3].(string),
		labels:       labels,
		inputColumns: inputColumns,
	}, nil
}

func (p *ExecutionPlanner) insertModel(model *modelSystemObject) error {
	err := p.ensureModelsSystemTableExists()
	if err != nil {
		return err
	}

	createTime := time.Now().UTC()

	labelJson, err := json.Marshal(model.labels)
	if err != nil {
		return err
	}

	inputColumnsJson, err := json.Marshal(model.inputColumns)
	if err != nil {
		return err
	}

	iter := &insertRowIter{
		planner:   p,
		tableName: "fb_models",
		targetColumns: []*qualifiedRefPlanExpression{
			newQualifiedRefPlanExpression("fb_models", "_id", 0, parser.NewDataTypeString()),
			newQualifiedRefPlanExpression("fb_models", "name", 0, parser.NewDataTypeString()),
			newQualifiedRefPlanExpression("fb_models", "status", 0, parser.NewDataTypeString()),
			newQualifiedRefPlanExpression("fb_models", "model_type", 0, parser.NewDataTypeString()),
			newQualifiedRefPlanExpression("fb_models", "labels", 0, parser.NewDataTypeString()),
			newQualifiedRefPlanExpression("fb_models", "input_columns", 0, parser.NewDataTypeString()),
			newQualifiedRefPlanExpression("fb_models", "owner", 0, parser.NewDataTypeString()),
			newQualifiedRefPlanExpression("fb_models", "updated_by", 0, parser.NewDataTypeString()),
			newQualifiedRefPlanExpression("fb_models", "created_at", 0, parser.NewDataTypeTimestamp()),
			newQualifiedRefPlanExpression("fb_models", "updated_at", 0, parser.NewDataTypeTimestamp()),
		},
		insertValues: [][]types.PlanExpression{
			{
				newStringLiteralPlanExpression(model.name),
				newStringLiteralPlanExpression(model.name),
				newStringLiteralPlanExpression(model.status),
				newStringLiteralPlanExpression(model.modelType),
				newStringLiteralPlanExpression(string(labelJson)),
				newStringLiteralPlanExpression(string(inputColumnsJson)),
				newStringLiteralPlanExpression(""),
				newStringLiteralPlanExpression(""),
				newTimestampLiteralPlanExpression(createTime),
				newTimestampLiteralPlanExpression(createTime),
			},
		},
	}
	_, err = iter.Next(context.Background())
	if err != nil && err != types.ErrNoMoreRows {
		return err
	}
	return nil
}

func (p *ExecutionPlanner) updateModel(model *modelSystemObject) error {
	err := p.ensureModelsSystemTableExists()
	if err != nil {
		return err
	}

	updateTime := time.Now().UTC()

	labelJson, err := json.Marshal(model.labels)
	if err != nil {
		return err
	}

	inputColumnsJson, err := json.Marshal(model.inputColumns)
	if err != nil {
		return err
	}

	iter := &insertRowIter{
		planner:   p,
		tableName: "fb_models",
		targetColumns: []*qualifiedRefPlanExpression{
			newQualifiedRefPlanExpression("fb_models", "_id", 0, parser.NewDataTypeString()),
			newQualifiedRefPlanExpression("fb_models", "status", 0, parser.NewDataTypeString()),
			newQualifiedRefPlanExpression("fb_models", "model_type", 0, parser.NewDataTypeString()),
			newQualifiedRefPlanExpression("fb_models", "labels", 0, parser.NewDataTypeString()),
			newQualifiedRefPlanExpression("fb_models", "input_columns", 0, parser.NewDataTypeString()),
			newQualifiedRefPlanExpression("fb_models", "updated_by", 0, parser.NewDataTypeString()),
			newQualifiedRefPlanExpression("fb_models", "updated_at", 0, parser.NewDataTypeTimestamp()),
		},
		insertValues: [][]types.PlanExpression{
			{
				newStringLiteralPlanExpression(model.name),
				newStringLiteralPlanExpression(model.status),
				newStringLiteralPlanExpression(model.modelType),
				newStringLiteralPlanExpression(string(labelJson)),
				newStringLiteralPlanExpression(string(inputColumnsJson)),
				newStringLiteralPlanExpression(""),
				newTimestampLiteralPlanExpression(updateTime),
			},
		},
	}
	_, err = iter.Next(context.Background())
	if err != nil && err != types.ErrNoMoreRows {
		return err
	}
	return nil
}

func (p *ExecutionPlanner) deleteModel(modelName string) error {
	err := p.ensureModelsSystemTableExists()
	if err != nil {
		return err
	}

	err = p.ensureModelDataSystemTableExists()
	if err != nil {
		return err
	}

	iter := &filteredDeleteRowIter{
		planner:   p,
		tableName: "fb_models",
		filter: newBinOpPlanExpression(
			newQualifiedRefPlanExpression("fb_models", "_id", 0, parser.NewDataTypeString()),
			parser.EQ,
			newStringLiteralPlanExpression(modelName),
			parser.NewDataTypeBool(),
		),
	}
	_, err = iter.Next(context.Background())
	if err != nil && err != types.ErrNoMoreRows {
		return err
	}

	iter = &filteredDeleteRowIter{
		planner:   p,
		tableName: "fb_model_data",
		filter: newBinOpPlanExpression(
			newQualifiedRefPlanExpression("fb_model_data", "model_id", 0, parser.NewDataTypeString()),
			parser.EQ,
			newStringLiteralPlanExpression(modelName),
			parser.NewDataTypeBool(),
		),
	}
	_, err = iter.Next(context.Background())
	if err != nil && err != types.ErrNoMoreRows {
		return err
	}

	return nil
}
