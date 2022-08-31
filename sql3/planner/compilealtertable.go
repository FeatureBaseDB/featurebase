// Copyright 2021 Molecula Corp. All rights reserved.

package planner

import (
	"strings"

	"github.com/molecula/featurebase/v3/sql3"
	"github.com/molecula/featurebase/v3/sql3/parser"
	"github.com/molecula/featurebase/v3/sql3/planner/types"
)

type alterOperation int64

const (
	alterOpAdd alterOperation = iota
	alterOpDrop
	alterOpRename
)

// compileAlterTableStatement compiles an ALTER TABLE statement into a
// PlanOperator.
func (p *ExecutionPlanner) compileAlterTableStatement(stmt *parser.AlterTableStatement) (_ types.PlanOperator, err error) {
	tableName := parser.IdentName(stmt.Name)
	if stmt.Drop.IsValid() {
		columnName := parser.IdentName(stmt.DropColumnName)
		return NewPlanOpQuery(NewPlanOpAlterTable(p, tableName, alterOpDrop, columnName, "", nil), p.sql), nil
	} else if stmt.Add.IsValid() {
		col := stmt.ColumnDef
		columnName := parser.IdentName(col.Name)
		column, err := p.compileColumn(col)
		if err != nil {
			return nil, err
		}
		return NewPlanOpQuery(NewPlanOpAlterTable(p, tableName, alterOpAdd, "", columnName, column), p.sql), nil

	} else if stmt.Rename.IsValid() {
		oldColumnName := parser.IdentName(stmt.OldColumnName)
		newColumnName := parser.IdentName(stmt.NewColumnName)
		return NewPlanOpQuery(NewPlanOpAlterTable(p, tableName, alterOpRename, oldColumnName, newColumnName, nil), p.sql), nil
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
