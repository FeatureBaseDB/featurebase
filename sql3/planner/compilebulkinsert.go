// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"os"
	"strconv"
	"strings"

	pilosa "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/sql3"
	"github.com/featurebasedb/featurebase/v3/sql3/parser"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
	"github.com/pkg/errors"
)

// compileBulkInsertStatement compiles a BULK INSERT statement into a
// PlanOperator.
func (p *ExecutionPlanner) compileBulkInsertStatement(stmt *parser.BulkInsertStatement) (_ types.PlanOperator, err error) {
	tableName := parser.IdentName(stmt.Table)

	table, err := p.schemaAPI.IndexInfo(context.Background(), tableName)
	if err != nil {
		if errors.Is(err, pilosa.ErrIndexNotFound) {
			return nil, sql3.NewErrTableNotFound(stmt.Table.NamePos.Line, stmt.Table.NamePos.Column, tableName)
		}
		return nil, err
	}

	err = p.checkAccess(context.Background(), tableName, accessTypeWriteData)
	if err != nil {
		return nil, err
	}

	options := &bulkInsertOptions{
		format: "CSV", //only format supported right now
	}

	sliteral, sok := stmt.DataFile.(*parser.StringLit)
	if !sok {
		return nil, sql3.NewErrInternalf("unexpected file name type '%T'", stmt.DataFile)
	}
	options.fileName = sliteral.Value

	//file should exist
	if _, err := os.Stat(options.fileName); errors.Is(err, os.ErrNotExist) {
		// TODO (pok) need proper error
		return nil, sql3.NewErrInternalf("file '%s' does not exist", stmt.DataFile)
	}

	literal, ok := stmt.BatchSize.(*parser.IntegerLit)
	if !ok {
		return nil, sql3.NewErrInternalf("unexpected batch size type '%T'", stmt.BatchSize)
	}
	i, err := strconv.ParseInt(literal.Value, 10, 64)
	if err != nil {
		return nil, err
	}
	options.batchSize = int(i)

	literal, ok = stmt.RowsLimit.(*parser.IntegerLit)
	if !ok {
		return nil, sql3.NewErrInternalf("unexpected rowslimit type '%T'", stmt.RowsLimit)
	}
	i, err = strconv.ParseInt(literal.Value, 10, 64)
	if err != nil {
		return nil, err
	}
	options.rowsLimit = int(i)

	options.idColumnMap = make([]interface{}, 0)
	if stmt.MapId.ColumnExprs != nil {
		for _, m := range stmt.MapId.ColumnExprs.Exprs {
			literal, ok = m.(*parser.IntegerLit)
			if !ok {
				return nil, sql3.NewErrInternalf("unexpected id map expr type '%T'", m)
			}
			i, err = strconv.ParseInt(literal.Value, 10, 64)
			if err != nil {
				return nil, err
			}
			options.idColumnMap = append(options.idColumnMap, i)
		}
	}

	if stmt.ColumnMap != nil {
		options.columnMap = make([]*bulkInsertMappedColumn, 0)
		for _, m := range stmt.ColumnMap {
			literal, ok = m.SourceColumnOffset.(*parser.IntegerLit)
			if !ok {
				return nil, sql3.NewErrInternalf("unexpected column map expr type '%T'", m)
			}
			i, err = strconv.ParseInt(literal.Value, 10, 64)
			if err != nil {
				return nil, err
			}

			for _, fld := range table.Fields {
				if strings.EqualFold(fld.Name, m.TargetColumn.Name) {
					cm := &bulkInsertMappedColumn{
						columnSource:   i,
						columnName:     m.TargetColumn.Name,
						columnDataType: fieldSQLDataType(fld),
					}
					options.columnMap = append(options.columnMap, cm)
					break
				}
			}
		}
	} else {
		options.columnMap = make([]*bulkInsertMappedColumn, 0)
		//handle the case of a default mapping based on the table
		i := 0
		for _, fld := range table.Fields {
			if strings.EqualFold(fld.Name, "_id") {
				continue
			}
			cm := &bulkInsertMappedColumn{
				columnSource:   i,
				columnName:     fld.Name,
				columnDataType: fieldSQLDataType(fld),
			}
			options.columnMap = append(options.columnMap, cm)
			i += 1
		}
	}

	return NewPlanOpBulkInsert(p, tableName, table.Options.Keys, options), nil
}

// analyzeBulkInsertStatement analyzes a BULK INSERT statement and returns an
// error if anything is invalid.
func (p *ExecutionPlanner) analyzeBulkInsertStatement(stmt *parser.BulkInsertStatement) error {
	//check referred to table exists
	tableName := parser.IdentName(stmt.Table)
	table, err := p.schemaAPI.IndexInfo(context.Background(), tableName)
	if err != nil {
		if errors.Is(err, pilosa.ErrIndexNotFound) {
			return sql3.NewErrTableNotFound(stmt.Table.NamePos.Line, stmt.Table.NamePos.Column, tableName)
		}
		return err
	}

	// check filename

	// file should be literal and a string
	if !(stmt.DataFile.IsLiteral() && typeIsString(stmt.DataFile.DataType())) {
		return sql3.NewErrStringLiteral(stmt.DataFile.Pos().Line, stmt.DataFile.Pos().Column)
	}

	// check options

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

	// format should default to CSV
	if stmt.Format == nil {
		stmt.Format = &parser.StringLit{
			Value: "CSV",
		}
	}

	// format should be literal and a string
	if !(stmt.Format.IsLiteral() && typeIsString(stmt.Format.DataType())) {
		return sql3.NewErrStringLiteral(stmt.Format.Pos().Line, stmt.Format.Pos().Column)
	}

	//CSV is the only format supported right now
	format, ok := stmt.Format.(*parser.StringLit)
	if !ok {
		return sql3.NewErrInternalf("unexpected format type '%T'", stmt.Format)
	}
	if !strings.EqualFold(format.Value, "CSV") {
		//TODO (pok) - proper error needed here
		return sql3.NewErrInternalf("unexpected format '%s'", format.Value)
	}

	// if we have an id map, check expressions are literals and ints
	if stmt.MapId.ColumnExprs != nil {
		for _, im := range stmt.MapId.ColumnExprs.Exprs {
			if !(im.IsLiteral() && typeIsInteger(im.DataType())) {
				return sql3.NewErrIntegerLiteral(im.Pos().Line, im.Pos().Column)
			}
		}
	}

	//if we have a column map, check offset expressions and target column names
	if stmt.ColumnMap != nil {
		for _, cm := range stmt.ColumnMap {
			if !(cm.SourceColumnOffset.IsLiteral() && typeIsInteger(cm.SourceColumnOffset.DataType())) {
				return sql3.NewErrIntegerLiteral(cm.SourceColumnOffset.Pos().Line, cm.SourceColumnOffset.Pos().Column)
			}
			found := false
			for _, fld := range table.Fields {
				if strings.EqualFold(cm.TargetColumn.Name, fld.Name) {
					found = true
					break
				}
			}
			if !found {
				return sql3.NewErrColumnNotFound(cm.TargetColumn.NamePos.Line, cm.TargetColumn.NamePos.Line, cm.TargetColumn.Name)
			}
		}
	}
	return nil
}
