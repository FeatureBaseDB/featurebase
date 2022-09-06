// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"strings"

	pilosa "github.com/molecula/featurebase/v3"
	"github.com/molecula/featurebase/v3/sql3"
	"github.com/molecula/featurebase/v3/sql3/parser"
	"github.com/molecula/featurebase/v3/sql3/planner/types"
	"github.com/pkg/errors"
)

// compileInsertStatement compiles an INSERT statement into a PlanOperator.
func (p *ExecutionPlanner) compileInsertStatement(stmt *parser.InsertStatement) (_ types.PlanOperator, err error) {
	tableName := parser.IdentName(stmt.Table)

	targetColumns := []*qualifiedRefPlanExpression{}
	insertValues := []types.PlanExpression{}

	table, err := p.schemaAPI.IndexInfo(context.Background(), tableName)
	if err != nil {
		if errors.Is(err, pilosa.ErrIndexNotFound) {
			return nil, sql3.NewErrTableNotFound(stmt.Table.NamePos.Line, stmt.Table.NamePos.Column, tableName)
		}
		return nil, err
	}

	if len(stmt.Columns) > 0 {
		for _, columnIdent := range stmt.Columns {
			colName := parser.IdentName(columnIdent)

			if strings.EqualFold(colName, "_id") {
				targetColumns = append(targetColumns, newQualifiedRefPlanExpression(tableName, colName, 0, parser.NewDataTypeID()))
				continue
			}

			for idx, field := range table.Fields {
				if strings.EqualFold(colName, field.Name) {
					targetColumns = append(targetColumns, newQualifiedRefPlanExpression(tableName, colName, idx, fieldSQLDataType(field)))
					break
				}
			}
		}
	} else {
		for idx, field := range table.Fields {
			if strings.EqualFold("_exists", field.Name) {
				continue
			}
			targetColumns = append(targetColumns, newQualifiedRefPlanExpression(tableName, field.Name, idx, fieldSQLDataType(field)))
		}
	}

	//add expressions from values list
	for _, expr := range stmt.ValueList.Exprs {
		e, err := p.compileExpr(expr)
		if err != nil {
			return nil, err
		}
		insertValues = append(insertValues, e)
	}

	return NewPlanOpInsert(p, tableName, targetColumns, insertValues), nil
}

// analyzeInsertStatement analyzes an INSERT statement and returns and error if
// anything is invalid.
func (p *ExecutionPlanner) analyzeInsertStatement(stmt *parser.InsertStatement) error {
	// Check that referred table exists.
	tableName := parser.IdentName(stmt.Table)
	table, err := p.schemaAPI.IndexInfo(context.Background(), tableName)
	if err != nil {
		if errors.Is(err, pilosa.ErrIndexNotFound) {
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
		for _, field := range table.Fields {
			if strings.EqualFold("_exists", field.Name) {
				continue
			}
			typeNames = append(typeNames, fieldSQLDataType(field))
		}
		// Make sure (implicit) insert list and expression list have the same
		// number of items.
		if len(typeNames) != len(stmt.ValueList.Exprs) {
			return sql3.NewErrInsertExprTargetCountMismatch(stmt.ValueList.Lparen.Line, stmt.ValueList.Lparen.Column)
		}
	} else {
		// Check column list refers to actual columns, and that there are no
		// dupes.
		columnNameMap := make(map[string]struct{})
		for _, columnIdent := range stmt.Columns {
			colName := parser.IdentName(columnIdent)
			var typeName parser.ExprDataType

			if strings.EqualFold(colName, "_id") {
				columnNameMap["_id"] = struct{}{}

				// Determine, from the existing table, whether the _id is of
				// type ID or STRING.
				var idType parser.ExprDataType
				if table.Options.Keys {
					idType = parser.NewDataTypeString()
				} else {
					idType = parser.NewDataTypeID()
				}
				typeNames = append(typeNames, idType)

				continue
			}

			// Find the column in the existing table.
			columnFound := false
			for _, field := range table.Fields {
				if strings.EqualFold(colName, field.Name) {
					typeName = fieldSQLDataType(field)
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
		if _, ok := columnNameMap["_id"]; !ok {
			return sql3.NewErrInsertMustHaveIDColumn(stmt.ColumnsLparen.Line, stmt.ColumnsLparen.Column)
		}

		// Ensure we have at least one more than just the _id column.
		if len(stmt.Columns) < 2 {
			return sql3.NewErrInsertMustAtLeastOneNonIDColumn(stmt.ColumnsLparen.Line, stmt.ColumnsLparen.Column)
		}

		// Make sure insert list and expression list have the same number of items.
		if len(stmt.Columns) != len(stmt.ValueList.Exprs) {
			return sql3.NewErrInsertExprTargetCountMismatch(stmt.ValueList.Lparen.Line, stmt.ValueList.Lparen.Column)
		}
	}

	// Check each of the expressions.
	for i, expr := range stmt.ValueList.Exprs {
		e, err := p.analyzeExpression(expr, stmt)
		if err != nil {
			return err
		}

		// Type check against same ordinal position in column type list.
		if !typesAreAssignmentCompatible(typeNames[i], e.DataType()) {
			return sql3.NewErrTypeAssignmentIncompatible(expr.Pos().Line, expr.Pos().Column, e.DataType().TypeName(), typeNames[i].TypeName())
		}

		stmt.ValueList.Exprs[i] = e
	}

	return nil
}
