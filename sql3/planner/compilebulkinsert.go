// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	pilosa "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/sql3"
	"github.com/featurebasedb/featurebase/v3/sql3/parser"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
	goerrors "github.com/pkg/errors"
)

// compileBulkInsertStatement compiles a BULK INSERT statement into a
// PlanOperator.
func (p *ExecutionPlanner) compileBulkInsertStatement(stmt *parser.BulkInsertStatement) (_ types.PlanOperator, err error) {
	tableName := parser.IdentName(stmt.Table)

	tname := dax.TableName(tableName)
	tbl, err := p.schemaAPI.TableByName(context.Background(), tname)
	if err != nil {
		if isTableNotFoundError(err) {
			return nil, sql3.NewErrTableNotFound(stmt.Table.NamePos.Line, stmt.Table.NamePos.Column, tableName)
		}
		return nil, err
	}

	err = p.checkAccess(context.Background(), tableName, accessTypeWriteData)
	if err != nil {
		return nil, err
	}

	// create an options
	options := &bulkInsertOptions{}

	// data source
	sliteral, sok := stmt.DataSource.(*parser.StringLit)
	if !sok {
		return nil, sql3.NewErrStringLiteral(stmt.DataSource.Pos().Line, stmt.DataSource.Pos().Column)
	}
	options.sourceData = sliteral.Value

	// format specifier
	sliteral, sok = stmt.Format.(*parser.StringLit)
	if !sok {
		return nil, sql3.NewErrStringLiteral(stmt.Format.Pos().Line, stmt.Format.Pos().Column)
	}
	options.format = sliteral.Value

	// input specifier
	sliteral, sok = stmt.Input.(*parser.StringLit)
	if !sok {
		return nil, sql3.NewErrStringLiteral(stmt.Input.Pos().Line, stmt.Input.Pos().Column)
	}
	options.input = sliteral.Value

	switch strings.ToUpper(options.input) {
	case "FILE":
		// file should exist
		if _, err := os.Stat(options.sourceData); goerrors.Is(err, os.ErrNotExist) {
			return nil, sql3.NewErrReadingDatasource(stmt.DataSource.Pos().Line, stmt.DataSource.Pos().Column, options.sourceData, fmt.Sprintf("file '%s' does not exist", options.sourceData))
		}
	case "URL", "STREAM":
		// nothing to do here
		break
	default:
		return nil, sql3.NewErrInvalidInputSpecifier(stmt.Input.Pos().Line, stmt.Input.Pos().Column, options.input)
	}

	// HEADER_ROW
	bliteral, sok := stmt.HeaderRow.(*parser.BoolLit)
	if !sok {
		return nil, sql3.NewErrBoolLiteral(stmt.HeaderRow.Pos().Line, stmt.HeaderRow.Pos().Column)
	}
	options.hasHeaderRow = bliteral.Value

	// ALLOW_MISSING_VALUES
	bliteral, sok = stmt.AllowMissingValues.(*parser.BoolLit)
	if !sok {
		return nil, sql3.NewErrBoolLiteral(stmt.AllowMissingValues.Pos().Line, stmt.AllowMissingValues.Pos().Column)
	}
	options.allowMissingValues = bliteral.Value

	// batchsize
	literal, ok := stmt.BatchSize.(*parser.IntegerLit)
	if !ok {
		return nil, sql3.NewErrIntegerLiteral(stmt.BatchSize.Pos().Line, stmt.BatchSize.Pos().Column)
	}
	i, err := strconv.ParseInt(literal.Value, 10, 64)
	if err != nil {
		return nil, err
	}
	options.batchSize = int(i)

	// rows limit
	literal, ok = stmt.RowsLimit.(*parser.IntegerLit)
	if !ok {
		return nil, sql3.NewErrIntegerLiteral(stmt.RowsLimit.Pos().Line, stmt.RowsLimit.Pos().Column)
	}
	i, err = strconv.ParseInt(literal.Value, 10, 64)
	if err != nil {
		return nil, err
	}
	options.rowsLimit = int(i)

	// build the target columns
	options.targetColumns = make([]*qualifiedRefPlanExpression, 0)
	for _, m := range stmt.Columns {
		for idx, fld := range tbl.Fields {
			if strings.EqualFold(string(fld.Name), m.Name) {
				options.targetColumns = append(options.targetColumns, newQualifiedRefPlanExpression(tableName, m.Name, idx, fieldSQLDataType(pilosa.FieldToFieldInfo(fld))))
				break
			}
		}
	}

	// build the map expressions
	options.mapExpressions = make([]*bulkInsertMapColumn, 0)
	for _, m := range stmt.MapList {
		expr, err := p.compileExpr(m.MapExpr)
		if err != nil {
			return nil, err
		}

		mapType, err := dataTypeFromParserType(m.Type)
		if err != nil {
			return nil, err
		}

		options.mapExpressions = append(options.mapExpressions, &bulkInsertMapColumn{
			name:    m.Name.String(),
			expr:    expr,
			colType: mapType,
		})
	}

	// build the transforms
	options.transformExpressions = make([]types.PlanExpression, 0)
	if stmt.TransformList != nil {
		for _, t := range stmt.TransformList {
			expr, err := p.compileExpr(t)
			if err != nil {
				return nil, err
			}
			options.transformExpressions = append(options.transformExpressions, expr)
		}
	}

	return NewPlanOpQuery(p, NewPlanOpBulkInsert(p, tableName, options), p.sql), nil
}

// analyzeBulkInsertStatement analyzes a BULK INSERT statement and returns an
// error if anything is invalid.
func (p *ExecutionPlanner) analyzeBulkInsertStatement(stmt *parser.BulkInsertStatement) error {
	// check referred to table exists
	tableName := parser.IdentName(stmt.Table)
	tname := dax.TableName(tableName)
	tbl, err := p.schemaAPI.TableByName(context.Background(), tname)
	if err != nil {
		if isTableNotFoundError(err) {
			return sql3.NewErrTableNotFound(stmt.Table.NamePos.Line, stmt.Table.NamePos.Column, tableName)
		}
		return err
	}

	// check source

	// source should be literal and a string
	if !(stmt.DataSource.IsLiteral() && typeIsString(stmt.DataSource.DataType())) {
		return sql3.NewErrStringLiteral(stmt.DataSource.Pos().Line, stmt.DataSource.Pos().Column)
	}

	// check options

	// check we have format specifier
	if stmt.Format == nil {
		return sql3.NewErrFormatSpecifierExpected(stmt.With.Line, stmt.With.Column)
	}

	// format should be literal and a string
	if !(stmt.Format.IsLiteral() && typeIsString(stmt.Format.DataType())) {
		return sql3.NewErrStringLiteral(stmt.Format.Pos().Line, stmt.Format.Pos().Column)
	}

	format, ok := stmt.Format.(*parser.StringLit)
	if !ok {
		return sql3.NewErrStringLiteral(stmt.Format.Pos().Line, stmt.Format.Pos().Column)
	}

	// check map and other correctness per format
	switch strings.ToUpper(format.Value) {
	case "CSV":
		// for csv the map expressions need to be integer values
		// that represent the offsets in the source file
		for _, im := range stmt.MapList {
			if !(im.MapExpr.IsLiteral() && typeIsInteger(im.MapExpr.DataType())) {
				return sql3.NewErrIntegerLiteral(im.MapExpr.Pos().Line, im.MapExpr.Pos().Column)
			}
		}
	case "PARQUET":
		// for paquet the map expressions need to be string values
		// that represent the offsets in the source file
		for _, im := range stmt.MapList {
			if !(im.MapExpr.IsLiteral() && typeIsString(im.MapExpr.DataType())) {
				return sql3.NewErrStringLiteral(im.MapExpr.Pos().Line, im.MapExpr.Pos().Column)
			}
		}
	case "NDJSON":
		// for ndjson the map expressions need to be string values
		// that represent json path expressions
		for _, im := range stmt.MapList {
			if !(im.MapExpr.IsLiteral() && typeIsString(im.MapExpr.DataType())) {
				return sql3.NewErrStringLiteral(im.MapExpr.Pos().Line, im.MapExpr.Pos().Column)
			}
		}

	default:
		return sql3.NewErrInvalidFormatSpecifier(stmt.Format.Pos().Line, stmt.Format.Pos().Column, format.Value)
	}

	// check we have input specifier
	if stmt.Input == nil {
		return sql3.NewErrInputSpecifierExpected(stmt.With.Line, stmt.With.Column)
	}

	// input should be literal and a string
	if !(stmt.Input.IsLiteral() && typeIsString(stmt.Input.DataType())) {
		return sql3.NewErrStringLiteral(stmt.Input.Pos().Line, stmt.Input.Pos().Column)
	}

	// input specifier either FILE or URL
	input, ok := stmt.Input.(*parser.StringLit)
	if !ok {
		return sql3.NewErrStringLiteral(stmt.Input.Pos().Line, stmt.Input.Pos().Column)
	}
	if !(strings.EqualFold(input.Value, "FILE") || strings.EqualFold(input.Value, "URL") || strings.EqualFold(input.Value, "STREAM")) {
		return sql3.NewErrInvalidInputSpecifier(stmt.Input.Pos().Line, stmt.Input.Pos().Column, input.Value)
	}

	// batch size should default to 1000
	if stmt.BatchSize == nil {
		stmt.BatchSize = &parser.IntegerLit{
			Value: "1000",
		}
	}
	// batch size should be literal and an int
	if !(stmt.BatchSize.IsLiteral() && typeIsInteger(stmt.BatchSize.DataType())) {
		return sql3.NewErrIntegerLiteral(stmt.BatchSize.Pos().Line, stmt.BatchSize.Pos().Column)
	}
	// check batch size > 0
	literal, ok := stmt.BatchSize.(*parser.IntegerLit)
	if !ok {
		return sql3.NewErrIntegerLiteral(stmt.BatchSize.Pos().Line, stmt.BatchSize.Pos().Column)
	}
	i, err := strconv.ParseInt(literal.Value, 10, 64)
	if err != nil {
		return err
	}
	if i == 0 {
		return sql3.NewErrInvalidBatchSize(stmt.BatchSize.Pos().Line, stmt.BatchSize.Pos().Column, int(i))
	}

	// rowslimit should default to 0
	if stmt.RowsLimit == nil {
		stmt.RowsLimit = &parser.IntegerLit{
			Value: "0",
		}
	}
	// rowslimit should be literal and an int
	if !(stmt.RowsLimit.IsLiteral() && typeIsInteger(stmt.RowsLimit.DataType())) {
		return sql3.NewErrIntegerLiteral(stmt.RowsLimit.Pos().Line, stmt.RowsLimit.Pos().Column)
	}

	// header row is true if specified, false if not
	stmt.HeaderRow = &parser.BoolLit{
		Value: stmt.HeaderRow != nil,
	}

	// allow missing values is true if specified, false if not
	stmt.AllowMissingValues = &parser.BoolLit{
		Value: stmt.AllowMissingValues != nil,
	}

	// analyze map expressions
	for i, m := range stmt.MapList {
		typeName := parser.IdentName(m.Type.Name)
		if !parser.IsValidTypeName(typeName) {
			return sql3.NewErrUnknownType(m.Type.Name.NamePos.Line, m.Type.Name.NamePos.Column, typeName)
		}
		ex, err := p.analyzeExpression(m.MapExpr, stmt)
		if err != nil {
			return err
		}
		stmt.MapList[i].MapExpr = ex
	}

	// check columns
	if stmt.Columns == nil {
		// we didn't get any columns so the column list is implictly
		// the column list of the table referenced

		stmt.Columns = []*parser.Ident{}
		for _, fld := range tbl.Fields {
			stmt.Columns = append(stmt.Columns, &parser.Ident{
				NamePos: parser.Pos{Line: 0, Column: 0},
				Name:    string(fld.Name),
			})
		}
	}

	// check map count is the same as target column count if there are no transforms
	if stmt.TransformList == nil {
		if len(stmt.Columns) != len(stmt.MapList) {
			return sql3.NewErrInsertExprTargetCountMismatch(stmt.MapRparen.Line, stmt.MapRparen.Column)
		}
	}

	// analyze transform expressions
	if stmt.TransformList != nil {

		// check transform count is the same as target column count if there are transforms
		if len(stmt.Columns) != len(stmt.TransformList) {
			return sql3.NewErrInsertExprTargetCountMismatch(stmt.TransformRparen.Line, stmt.TransformRparen.Column)
		}

		for i, t := range stmt.TransformList {
			ex, err := p.analyzeExpression(t, stmt)
			if err != nil {
				return err
			}

			stmt.TransformList[i] = ex
		}
	}

	// check columns being inserted to are actual columns and that one of them is the _id column
	// also do type checking, and check there are no dupes
	columnNameMap := make(map[string]struct{})
	foundID := false
	for idx, cm := range stmt.Columns {
		found := false
		for _, fld := range tbl.Fields {
			if strings.EqualFold(cm.Name, string(fld.Name)) {
				found = true
				colDataType := fieldSQLDataType(pilosa.FieldToFieldInfo(fld))

				// if we have transforms check that type and target colum ref are assignment compatible
				// else check that the map expressions type and target column ref are assignment compatible
				if stmt.TransformList != nil {
					t := stmt.TransformList[idx]
					if !typesAreAssignmentCompatible(colDataType, t.DataType()) {
						return sql3.NewErrTypeAssignmentIncompatible(t.Pos().Line, t.Pos().Column, t.DataType().TypeDescription(), colDataType.TypeDescription())
					}
				} else {
					// this assumes that map and col list have already been checked for length
					me := stmt.MapList[idx]
					t, err := dataTypeFromParserType(me.Type)
					if err != nil {
						return err
					}
					if !typesAreAssignmentCompatible(colDataType, t) {
						return sql3.NewErrTypeAssignmentIncompatible(me.MapExpr.Pos().Line, me.MapExpr.Pos().Column, t.TypeDescription(), colDataType.TypeDescription())
					}
				}
				break
			}
		}
		if !found {
			return sql3.NewErrColumnNotFound(cm.NamePos.Line, cm.NamePos.Line, cm.Name)
		}

		// Ensure the column name hasn't already appeared in the list of
		// columns.
		colName := strings.ToLower(cm.Name)
		if _, found := columnNameMap[colName]; found {
			return sql3.NewErrDuplicateColumn(cm.NamePos.Line, cm.NamePos.Column, colName)
		}
		columnNameMap[colName] = struct{}{}

		if strings.EqualFold(cm.Name, "_id") {
			foundID = true
		}
	}
	if !foundID {
		return sql3.NewErrInsertMustHaveIDColumn(stmt.ColumnsRparen.Line, stmt.ColumnsRparen.Column)
	}

	// check we have columns other than just _id
	if len(stmt.Columns) < 2 {
		return sql3.NewErrInsertMustAtLeastOneNonIDColumn(stmt.ColumnsLparen.Line, stmt.ColumnsLparen.Column)
	}

	return nil
}
