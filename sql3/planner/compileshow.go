// Copyright 2021 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"strings"

	pilosa "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/sql3"
	"github.com/featurebasedb/featurebase/v3/sql3/parser"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
	"github.com/pkg/errors"
)

func (p *ExecutionPlanner) compileShowDatabasesStatement(ctx context.Context, stmt parser.Statement) (types.PlanOperator, error) {
	dbs, err := p.schemaAPI.Databases(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "getting databases")
	}

	columns := []types.PlanExpression{
		&qualifiedRefPlanExpression{
			tableName:   "fb_databases",
			columnName:  "_id",
			columnIndex: 0,
			dataType:    parser.NewDataTypeString(),
		},
		&qualifiedRefPlanExpression{
			tableName:   "fb_databases",
			columnName:  "name",
			columnIndex: 1,
			dataType:    parser.NewDataTypeString(),
		},
		&qualifiedRefPlanExpression{
			tableName:   "fb_databases",
			columnName:  "owner",
			columnIndex: 2,
			dataType:    parser.NewDataTypeString(),
		},
		&qualifiedRefPlanExpression{
			tableName:   "fb_databases",
			columnName:  "updated_by",
			columnIndex: 3,
			dataType:    parser.NewDataTypeString(),
		},
		&qualifiedRefPlanExpression{
			tableName:   "fb_databases",
			columnName:  "created_at",
			columnIndex: 4,
			dataType:    parser.NewDataTypeTimestamp(),
		},
		&qualifiedRefPlanExpression{
			tableName:   "fb_databases",
			columnName:  "updated_at",
			columnIndex: 5,
			dataType:    parser.NewDataTypeTimestamp(),
		},
		&qualifiedRefPlanExpression{
			tableName:   "fb_databases",
			columnName:  "units",
			columnIndex: 6,
			dataType:    parser.NewDataTypeInt(),
		},
		&qualifiedRefPlanExpression{
			tableName:   "fb_databases",
			columnName:  "description",
			columnIndex: 7,
			dataType:    parser.NewDataTypeString(),
		}}

	return NewPlanOpQuery(p, NewPlanOpProjection(columns, NewPlanOpFeatureBaseDatabases(p, dbs)), p.sql), nil
}

func (p *ExecutionPlanner) compileShowTablesStatement(ctx context.Context, stmt parser.Statement) (types.PlanOperator, error) {
	tbls, err := p.schemaAPI.Tables(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "getting tables")
	}

	columns := []types.PlanExpression{
		&qualifiedRefPlanExpression{
			tableName:   "fb_tables",
			columnName:  "_id",
			columnIndex: 0,
			dataType:    parser.NewDataTypeString(),
		},
		&qualifiedRefPlanExpression{
			tableName:   "fb_tables",
			columnName:  "name",
			columnIndex: 1,
			dataType:    parser.NewDataTypeString(),
		},
		&qualifiedRefPlanExpression{
			tableName:   "fb_tables",
			columnName:  "owner",
			columnIndex: 2,
			dataType:    parser.NewDataTypeString(),
		},
		&qualifiedRefPlanExpression{
			tableName:   "fb_tables",
			columnName:  "updated_by",
			columnIndex: 3,
			dataType:    parser.NewDataTypeString(),
		},
		&qualifiedRefPlanExpression{
			tableName:   "fb_tables",
			columnName:  "created_at",
			columnIndex: 4,
			dataType:    parser.NewDataTypeTimestamp(),
		},
		&qualifiedRefPlanExpression{
			tableName:   "fb_tables",
			columnName:  "updated_at",
			columnIndex: 5,
			dataType:    parser.NewDataTypeTimestamp(),
		},
		&qualifiedRefPlanExpression{
			tableName:   "fb_tables",
			columnName:  "keys",
			columnIndex: 6,
			dataType:    parser.NewDataTypeBool(),
		},
		&qualifiedRefPlanExpression{
			tableName:   "fb_tables",
			columnName:  "space_used",
			columnIndex: 7,
			dataType:    parser.NewDataTypeInt(),
		},
		&qualifiedRefPlanExpression{
			tableName:   "fb_tables",
			columnName:  "description",
			columnIndex: 8,
			dataType:    parser.NewDataTypeString(),
		}}

	return NewPlanOpQuery(p, NewPlanOpProjection(columns, NewPlanOpFeatureBaseTables(p, pilosa.TablesToIndexInfos(tbls))), p.sql), nil
}

func (p *ExecutionPlanner) compileShowColumnsStatement(ctx context.Context, stmt *parser.ShowColumnsStatement) (_ types.PlanOperator, err error) {
	tableName := parser.IdentName(stmt.TableName)
	tname := dax.TableName(tableName)
	tbl, err := p.schemaAPI.TableByName(ctx, tname)
	if err != nil {
		if isTableNotFoundError(err) {
			return nil, sql3.NewErrTableNotFound(stmt.TableName.NamePos.Line, stmt.TableName.NamePos.Column, tableName)
		}
		return nil, err
	}

	columns := []types.PlanExpression{&qualifiedRefPlanExpression{
		tableName:   "fb_table_columns",
		columnName:  "_id",
		columnIndex: 0,
		dataType:    parser.NewDataTypeString(),
	}, &qualifiedRefPlanExpression{
		tableName:   "fb_table_columns",
		columnName:  "name",
		columnIndex: 1,
		dataType:    parser.NewDataTypeString(),
	}, &qualifiedRefPlanExpression{ // the SQL3 data type description
		tableName:   "fb_table_columns",
		columnName:  "type",
		columnIndex: 2,
		dataType:    parser.NewDataTypeString(),
	}, &qualifiedRefPlanExpression{ // the FeatureBase 'native' data type description
		tableName:   "fb_table_columns",
		columnName:  "internal_type",
		columnIndex: 3,
		dataType:    parser.NewDataTypeString(),
	}, &qualifiedRefPlanExpression{
		tableName:   "fb_table_columns",
		columnName:  "created_at",
		columnIndex: 4,
		dataType:    parser.NewDataTypeTimestamp(),
	}, &qualifiedRefPlanExpression{
		tableName:   "fb_table_columns",
		columnName:  "keys",
		columnIndex: 5,
		dataType:    parser.NewDataTypeBool(),
	}, &qualifiedRefPlanExpression{
		tableName:   "fb_table_columns",
		columnName:  "cache_type",
		columnIndex: 6,
		dataType:    parser.NewDataTypeString(),
	}, &qualifiedRefPlanExpression{
		tableName:   "fb_table_columns",
		columnName:  "cache_size",
		columnIndex: 7,
		dataType:    parser.NewDataTypeInt(),
	}, &qualifiedRefPlanExpression{
		tableName:   "fb_table_columns",
		columnName:  "scale",
		columnIndex: 8,
		dataType:    parser.NewDataTypeInt(),
	}, &qualifiedRefPlanExpression{
		tableName:   "fb_table_columns",
		columnName:  "min",
		columnIndex: 9,
		dataType:    parser.NewDataTypeInt(),
	}, &qualifiedRefPlanExpression{
		tableName:   "fb_table_columns",
		columnName:  "max",
		columnIndex: 10,
		dataType:    parser.NewDataTypeInt(),
	}, &qualifiedRefPlanExpression{
		tableName:   "fb_table_columns",
		columnName:  "timeunit",
		columnIndex: 11,
		dataType:    parser.NewDataTypeString(),
	}, &qualifiedRefPlanExpression{
		tableName:   "fb_table_columns",
		columnName:  "epoch",
		columnIndex: 12,
		dataType:    parser.NewDataTypeInt(),
	}, &qualifiedRefPlanExpression{
		tableName:   "fb_table_columns",
		columnName:  "timequantum",
		columnIndex: 13,
		dataType:    parser.NewDataTypeString(),
	}, &qualifiedRefPlanExpression{
		tableName:   "fb_table_columns",
		columnName:  "ttl",
		columnIndex: 14,
		dataType:    parser.NewDataTypeString(),
	}}

	return NewPlanOpQuery(p, NewPlanOpProjection(columns, NewPlanOpFeatureBaseColumns(tbl)), p.sql), nil
}

func (p *ExecutionPlanner) compileShowCreateTableStatement(ctx context.Context, stmt *parser.ShowCreateTableStatement) (_ types.PlanOperator, err error) {
	tableName := parser.IdentName(stmt.TableName)
	tname := dax.TableName(tableName)
	if _, err := p.schemaAPI.TableByName(ctx, tname); err != nil {
		if isTableNotFoundError(err) {
			return nil, sql3.NewErrTableNotFound(stmt.TableName.NamePos.Line, stmt.TableName.NamePos.Column, tableName)
		}
		return nil, err
	}

	// get the system table
	systemTable, ok := systemTables[fbTableDDL]
	if !ok {
		return nil, sql3.NewErrInternalf("unable to find system table fb_table_ddl")
	}

	// make an op for the system table
	systemTableScan := NewPlanOpSystemTable(p, systemTable)

	// get the columns from the schmema
	columns := systemTable.schema

	// get the ref name column and build projections
	projections := make([]types.PlanExpression, 0)
	var nameRef *qualifiedRefPlanExpression
	for idx, col := range columns {
		if strings.EqualFold(col.ColumnName, "name") {
			nameRef = newQualifiedRefPlanExpression(col.RelationName, col.ColumnName, idx, col.Type)
		}
		if strings.EqualFold(col.ColumnName, "ddl") {
			projections = append(projections, newQualifiedRefPlanExpression(col.RelationName, col.ColumnName, idx, col.Type))
		}
	}
	if nameRef == nil || len(projections) == 0 {
		return nil, sql3.NewErrInternalf("unable to find system table columns")
	}

	// make a filter espression
	filterExpr := newBinOpPlanExpression(nameRef, parser.EQ, newStringLiteralPlanExpression(tableName), parser.NewDataTypeBool())

	// make a filter op
	filter := NewPlanOpFilter(p, filterExpr, systemTableScan)

	return NewPlanOpQuery(p, NewPlanOpProjection(projections, filter), p.sql), nil
}
