// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"strings"

	pilosa "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/sql3"
	"github.com/featurebasedb/featurebase/v3/sql3/parser"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
)

// compileInsertStatement compiles an INSERT statement into a PlanOperator.
func (p *ExecutionPlanner) compileInsertStatement(ctx context.Context, stmt *parser.InsertStatement) (_ types.PlanOperator, err error) {
	tableName := strings.ToLower(parser.IdentName(stmt.Table))

	targetColumns := []*qualifiedRefPlanExpression{}
	insertValues := [][]types.PlanExpression{}

	tname := dax.TableName(tableName)
	tbl, err := p.schemaAPI.TableByName(ctx, tname)
	if err != nil {
		if isTableNotFoundError(err) {
			return nil, sql3.NewErrTableNotFound(stmt.Table.NamePos.Line, stmt.Table.NamePos.Column, tableName)
		}
		return nil, err
	}

	if len(stmt.Columns) > 0 {
		for _, columnIdent := range stmt.Columns {
			colName := strings.ToLower(parser.IdentName(columnIdent))

			if strings.EqualFold(colName, string(dax.PrimaryKeyFieldName)) {
				targetColumns = append(targetColumns, newQualifiedRefPlanExpression(tableName, colName, 0, parser.NewDataTypeID()))
				continue
			}

			for idx, field := range tbl.Fields {
				if strings.EqualFold(colName, string(field.Name)) {
					targetColumns = append(targetColumns, newQualifiedRefPlanExpression(tableName, colName, idx, fieldSQLDataType(pilosa.FieldToFieldInfo(field))))
					break
				}
			}
		}
	} else {
		for idx, field := range tbl.Fields {
			if strings.EqualFold("_exists", string(field.Name)) {
				continue
			}
			targetColumns = append(targetColumns, newQualifiedRefPlanExpression(tableName, string(field.Name), idx, fieldSQLDataType(pilosa.FieldToFieldInfo(field))))
		}
	}

	//add expressions from values list
	for _, tuple := range stmt.TupleList {
		tupleValues := []types.PlanExpression{}
		for _, expr := range tuple.Exprs {
			e, err := p.compileExpr(expr)
			if err != nil {
				return nil, err
			}
			tupleValues = append(tupleValues, e)
		}
		insertValues = append(insertValues, tupleValues)
	}

	return NewPlanOpQuery(p, NewPlanOpInsert(p, tableName, targetColumns, insertValues), p.sql), nil
}

// analyzeInsertStatement analyzes an INSERT statement and returns and error if
// anything is invalid.
func (p *ExecutionPlanner) analyzeInsertStatement(ctx context.Context, stmt *parser.InsertStatement) error {
	// Check that referred table exists.
	tableName := strings.ToLower(parser.IdentName(stmt.Table))
	tname := dax.TableName(tableName)
	tbl, err := p.schemaAPI.TableByName(ctx, tname)
	if err != nil {
		if isTableNotFoundError(err) {
			return sql3.NewErrTableNotFound(stmt.Table.NamePos.Line, stmt.Table.NamePos.Column, tableName)
		}
		return err
	}

	typeNames := make([]parser.ExprDataType, 0)
	// If the insert statement does not provide the list of columns in which to
	// insert the values, then the assumption is that the values apply to ALL
	// fields in the table.
	if len(stmt.Columns) == 0 {
		// Generate the list of types from the FeatureBase index.
		for _, field := range tbl.Fields {
			if strings.EqualFold("_exists", string(field.Name)) {
				continue
			}
			typeNames = append(typeNames, fieldSQLDataType(pilosa.FieldToFieldInfo(field)))
		}
		// Make sure (implicit) insert list and expression list have the same
		// number of items.
		for _, tuple := range stmt.TupleList {
			if len(typeNames) != len(tuple.Exprs) {
				return sql3.NewErrInsertExprTargetCountMismatch(tuple.Lparen.Line, tuple.Lparen.Column)
			}
		}
	} else {
		// Check column list refers to actual columns, and that there are no
		// dupes.
		columnNameMap := make(map[string]struct{})
		for _, columnIdent := range stmt.Columns {
			colName := strings.ToLower(parser.IdentName(columnIdent))
			var typeName parser.ExprDataType

			if strings.EqualFold(colName, string(dax.PrimaryKeyFieldName)) {
				columnNameMap[string(dax.PrimaryKeyFieldName)] = struct{}{}

				// Determine, from the existing table, whether the _id is of
				// type ID or STRING.
				var idType parser.ExprDataType
				if tbl.StringKeys() {
					idType = parser.NewDataTypeString()
				} else {
					idType = parser.NewDataTypeID()
				}
				typeNames = append(typeNames, idType)

				continue
			}

			// Find the column in the existing table.
			columnFound := false
			for _, field := range tbl.Fields {
				if strings.EqualFold(colName, string(field.Name)) {
					typeName = fieldSQLDataType(pilosa.FieldToFieldInfo(field))
					columnFound = true
					break
				}
			}
			if !columnFound {
				return sql3.NewErrColumnNotFound(columnIdent.NamePos.Line, columnIdent.NamePos.Column, colName)
			}

			// Ensure the column name hasn't already appeared in the list of
			// columns.
			if _, found := columnNameMap[colName]; found {
				return sql3.NewErrDuplicateColumn(columnIdent.NamePos.Line, columnIdent.NamePos.Column, colName)
			}

			typeNames = append(typeNames, typeName)
			columnNameMap[colName] = struct{}{}
		}

		// Ensure we have an _id column.
		if _, ok := columnNameMap[string(dax.PrimaryKeyFieldName)]; !ok {
			return sql3.NewErrInsertMustHaveIDColumn(stmt.ColumnsLparen.Line, stmt.ColumnsLparen.Column)
		}

		// Ensure we have at least one more than just the _id column.
		if len(stmt.Columns) < 2 {
			return sql3.NewErrInsertMustAtLeastOneNonIDColumn(stmt.ColumnsLparen.Line, stmt.ColumnsLparen.Column)
		}

		// Make sure insert list and expression list have the same number of items.
		for _, tuple := range stmt.TupleList {
			if len(stmt.Columns) != len(tuple.Exprs) {
				return sql3.NewErrInsertExprTargetCountMismatch(tuple.Lparen.Line, tuple.Lparen.Column)
			}
		}
	}

	// Check each of the expressions.
	for _, tuple := range stmt.TupleList {
		for i, expr := range tuple.Exprs {
			e, err := p.analyzeExpression(ctx, expr, stmt)
			if err != nil {
				return err
			}

			// Type check against same ordinal position in column type list.
			if !typesAreAssignmentCompatible(typeNames[i], e.DataType()) {
				return sql3.NewErrTypeAssignmentIncompatible(expr.Pos().Line, expr.Pos().Column, e.DataType().TypeDescription(), typeNames[i].TypeDescription())
			}

			tuple.Exprs[i] = e
		}
	}

	return nil
}
