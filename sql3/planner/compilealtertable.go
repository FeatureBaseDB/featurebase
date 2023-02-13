// Copyright 2021 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"strings"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/sql3"
	"github.com/featurebasedb/featurebase/v3/sql3/parser"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
)

type alterOperation int64

const (
	alterOpAdd alterOperation = iota
	alterOpDrop
	alterOpRename
	alterOpSet
)

// compileAlterTableStatement compiles an ALTER TABLE statement into a
// PlanOperator.
func (p *ExecutionPlanner) compileAlterTableStatement(ctx context.Context, stmt *parser.AlterTableStatement) (_ types.PlanOperator, err error) {
	tableName := parser.IdentName(stmt.Name)

	// does the table exist
	tname := dax.TableName(tableName)
	tbl, err := p.schemaAPI.TableByName(ctx, tname)
	if err != nil {
		if isTableNotFoundError(err) {
			return nil, sql3.NewErrTableNotFound(stmt.Name.NamePos.Line, stmt.Name.NamePos.Column, tableName)
		}
		return nil, err
	}

	if stmt.Drop.IsValid() {
		columnName := parser.IdentName(stmt.DropColumnName)

		// does this column exist
		found := false
		for _, f := range tbl.Fields {
			if strings.EqualFold(string(f.Name), columnName) {
				found = true
				break
			}
		}
		if !found {
			return nil, sql3.NewErrColumnNotFound(stmt.DropColumnName.NamePos.Line, stmt.DropColumnName.NamePos.Column, columnName)
		}

		return NewPlanOpQuery(p, NewPlanOpAlterTable(p, tableName, alterOpDrop, columnName, "", nil), p.sql), nil
	} else if stmt.Add.IsValid() {
		col := stmt.ColumnDef
		columnName := parser.IdentName(col.Name)

		// does this column exist
		for _, f := range tbl.Fields {
			if strings.EqualFold(string(f.Name), columnName) {
				return nil, sql3.NewErrDuplicateColumn(col.Name.NamePos.Line, col.Name.NamePos.Column, columnName)
			}
		}

		column, err := p.compileColumn(ctx, col)
		if err != nil {
			return nil, err
		}
		return NewPlanOpQuery(p, NewPlanOpAlterTable(p, tableName, alterOpAdd, "", columnName, column), p.sql), nil

	} else if stmt.Rename.IsValid() {
		oldColumnName := parser.IdentName(stmt.OldColumnName)
		newColumnName := parser.IdentName(stmt.NewColumnName)
		return NewPlanOpQuery(p, NewPlanOpAlterTable(p, tableName, alterOpRename, oldColumnName, newColumnName, nil), p.sql), nil
	} else {
		return nil, sql3.NewErrInternal("unhandled alter operation")
	}
}

// analyzeAlterTableStatement analyze an ALTER TABLE statement and returns an
// error if anything is invalid.
func (p *ExecutionPlanner) analyzeAlterTableStatement(stmt *parser.AlterTableStatement) error {

	if stmt.Drop.IsValid() {
		//no checks for now
	} else if stmt.Add.IsValid() {
		col := stmt.ColumnDef
		columnName := parser.IdentName(col.Name)
		typeName := parser.IdentName(col.Type.Name)
		if !parser.IsValidTypeName(typeName) {
			return sql3.NewErrUnknownType(col.Type.Name.NamePos.Line, col.Type.Name.NamePos.Column, typeName)
		}

		if strings.ToLower(columnName) == "_id" {
			//not allowed to add an _id column after the fact
			return sql3.NewErrTableIDColumnAlter(col.Name.NamePos.Line, col.Name.NamePos.Column)
		}

		err := p.analyzeColumn(typeName, col)
		if err != nil {
			return err
		}
	} else if stmt.Rename.IsValid() {
		//check the new and old are not the same
		oldColumnName := parser.IdentName(stmt.OldColumnName)
		newColumnName := parser.IdentName(stmt.NewColumnName)
		if strings.EqualFold(oldColumnName, newColumnName) {
			return sql3.NewErrDuplicateColumn(stmt.NewColumnName.NamePos.Line, stmt.NewColumnName.NamePos.Column, newColumnName)
		}
	}
	return nil
}
