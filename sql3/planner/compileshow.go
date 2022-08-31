// Copyright 2021 Molecula Corp. All rights reserved.

package planner

import (
	"context"

	pilosa "github.com/molecula/featurebase/v3"
	"github.com/molecula/featurebase/v3/sql3"
	"github.com/molecula/featurebase/v3/sql3/parser"
	"github.com/molecula/featurebase/v3/sql3/planner/types"
	"github.com/pkg/errors"
)

func (p *ExecutionPlanner) compileShowTablesStatement(stmt parser.Statement) (types.PlanOperator, error) {
	indexInfo, err := p.schemaAPI.Schema(context.Background(), false)
	if err != nil {
		return nil, errors.Wrap(err, "getting schema")
	}

	columns := []types.PlanExpression{&qualifiedRefPlanExpression{
		tableName:   "fb$tables",
		columnName:  "name",
		columnIndex: 0,
		dataType:    parser.NewDataTypeString(),
	}, &qualifiedRefPlanExpression{
		tableName:   "fb$tables",
		columnName:  "created_at",
		columnIndex: 1,
		dataType:    parser.NewDataTypeTimestamp(),
	}, &qualifiedRefPlanExpression{
		tableName:   "fb$tables",
		columnName:  "track_existence",
		columnIndex: 2,
		dataType:    parser.NewDataTypeBool(),
	}, &qualifiedRefPlanExpression{
		tableName:   "fb$tables",
		columnName:  "keys",
		columnIndex: 3,
		dataType:    parser.NewDataTypeBool(),
	}, &qualifiedRefPlanExpression{
		tableName:   "fb$tables",
		columnName:  "shard_width",
		columnIndex: 4,
		dataType:    parser.NewDataTypeInt(),
	}}

	return NewPlanOpQuery(NewPlanOpProjection(columns, NewPlanOpFeatureBaseTables(indexInfo)), p.sql), nil
}

func (p *ExecutionPlanner) compileShowColumnsStatement(stmt *parser.ShowColumnsStatement) (_ types.PlanOperator, err error) {
	tableName := parser.IdentName(stmt.TableName)
	index, err := p.schemaAPI.IndexInfo(context.Background(), tableName)
	if err != nil {
		if errors.Is(err, pilosa.ErrIndexNotFound) {
			return nil, sql3.NewErrTableNotFound(stmt.TableName.NamePos.Line, stmt.TableName.NamePos.Column, tableName)
		}
		return nil, err
	}

	columns := []types.PlanExpression{&qualifiedRefPlanExpression{
		tableName:   "fb$table_columns",
		columnName:  "name",
		columnIndex: 0,
		dataType:    parser.NewDataTypeString(),
	}, &qualifiedRefPlanExpression{ // the SQL3 data type description
		tableName:   "fb$table_columns",
		columnName:  "type",
		columnIndex: 1,
		dataType:    parser.NewDataTypeString(),
	}, &qualifiedRefPlanExpression{ // the FeatureBase 'native' data type description
		tableName:   "fb$table_columns",
		columnName:  "internal_type",
		columnIndex: 2,
		dataType:    parser.NewDataTypeString(),
	}, &qualifiedRefPlanExpression{
		tableName:   "fb$table_columns",
		columnName:  "created_at",
		columnIndex: 3,
		dataType:    parser.NewDataTypeTimestamp(),
	}, &qualifiedRefPlanExpression{
		tableName:   "fb$table_columns",
		columnName:  "keys",
		columnIndex: 4,
		dataType:    parser.NewDataTypeBool(),
	}, &qualifiedRefPlanExpression{
		tableName:   "fb$table_columns",
		columnName:  "cache_type",
		columnIndex: 5,
		dataType:    parser.NewDataTypeString(),
	}, &qualifiedRefPlanExpression{
		tableName:   "fb$table_columns",
		columnName:  "cache_size",
		columnIndex: 6,
		dataType:    parser.NewDataTypeInt(),
	}, &qualifiedRefPlanExpression{
		tableName:   "fb$table_columns",
		columnName:  "scale",
		columnIndex: 7,
		dataType:    parser.NewDataTypeInt(),
	}, &qualifiedRefPlanExpression{
		tableName:   "fb$table_columns",
		columnName:  "min",
		columnIndex: 8,
		dataType:    parser.NewDataTypeInt(),
	}, &qualifiedRefPlanExpression{
		tableName:   "fb$table_columns",
		columnName:  "max",
		columnIndex: 9,
		dataType:    parser.NewDataTypeInt(),
	}, &qualifiedRefPlanExpression{
		tableName:   "fb$table_columns",
		columnName:  "timeunit",
		columnIndex: 10,
		dataType:    parser.NewDataTypeString(),
	}, &qualifiedRefPlanExpression{
		tableName:   "fb$table_columns",
		columnName:  "epoch",
		columnIndex: 11,
		dataType:    parser.NewDataTypeInt(),
	}, &qualifiedRefPlanExpression{
		tableName:   "fb$table_columns",
		columnName:  "timequantum",
		columnIndex: 12,
		dataType:    parser.NewDataTypeString(),
	}, &qualifiedRefPlanExpression{
		tableName:   "fb$table_columns",
		columnName:  "ttl",
		columnIndex: 13,
		dataType:    parser.NewDataTypeString(),
	}}

	return NewPlanOpQuery(NewPlanOpProjection(columns, NewPlanOpFeatureBaseColumns(index)), p.sql), nil
}
