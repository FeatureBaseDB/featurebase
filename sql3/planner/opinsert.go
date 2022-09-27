// Copyright 2021 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"fmt"
	"strings"
	"time"

	pilosa "github.com/molecula/featurebase/v3"
	"github.com/molecula/featurebase/v3/pql"
	"github.com/molecula/featurebase/v3/sql3"
	"github.com/molecula/featurebase/v3/sql3/parser"
	"github.com/molecula/featurebase/v3/sql3/planner/types"
)

// PlanOpInsert plan operator to handle INSERT.
type PlanOpInsert struct {
	planner       *ExecutionPlanner
	tableName     string
	targetColumns []*qualifiedRefPlanExpression
	insertValues  []types.PlanExpression
	warnings      []string
}

func NewPlanOpInsert(p *ExecutionPlanner, tableName string, targetColumns []*qualifiedRefPlanExpression, insertValues []types.PlanExpression) *PlanOpInsert {
	return &PlanOpInsert{
		planner:       p,
		tableName:     tableName,
		targetColumns: targetColumns,
		insertValues:  insertValues,
		warnings:      make([]string, 0),
	}
}

func (p *PlanOpInsert) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_op"] = fmt.Sprintf("%T", p)
	sc := make([]string, 0)
	for _, e := range p.Schema() {
		sc = append(sc, fmt.Sprintf("'%s', '%s', '%s'", e.ColumnName, e.RelationName, e.Type.TypeName()))
	}
	result["_schema"] = sc
	result["tableName"] = p.tableName
	ps := make([]interface{}, 0)
	for _, e := range p.targetColumns {
		ps = append(ps, e.Plan())
	}
	result["targetColumns"] = ps
	ps = make([]interface{}, 0)
	for _, e := range p.insertValues {
		ps = append(ps, e.Plan())
	}
	result["insertValues"] = ps
	return result
}

func (p *PlanOpInsert) String() string {
	return ""
}

func (p *PlanOpInsert) AddWarning(warning string) {
	p.warnings = append(p.warnings, warning)
}

func (p *PlanOpInsert) Warnings() []string {
	return p.warnings
}

func (p *PlanOpInsert) Schema() types.Schema {
	return types.Schema{}
}

func (p *PlanOpInsert) Children() []types.PlanOperator {
	return []types.PlanOperator{}
}

func (p *PlanOpInsert) Iterator(ctx context.Context, row types.Row) (types.RowIterator, error) {
	return &insertRowIter{
		planner:       p.planner,
		tableName:     p.tableName,
		targetColumns: p.targetColumns,
		insertValues:  p.insertValues,
	}, nil
}

func (p *PlanOpInsert) WithChildren(children ...types.PlanOperator) (types.PlanOperator, error) {
	return NewPlanOpInsert(p.planner, p.tableName, p.targetColumns, p.insertValues), nil
}

type insertRowIter struct {
	planner       *ExecutionPlanner
	tableName     string
	targetColumns []*qualifiedRefPlanExpression
	insertValues  []types.PlanExpression
}

var _ types.RowIterator = (*insertRowIter)(nil)

func (i *insertRowIter) Next(ctx context.Context) (types.Row, error) {
	qcx := i.planner.computeAPI.Txf().NewQcx()

	colIDs := make([]uint64, 0)
	colKeys := make([]string, 0)

	addColID := func(v interface{}) error {
		switch id := v.(type) {
		case int64:
			colIDs = append(colIDs, uint64(id))
		case uint64:
			colIDs = append(colIDs, id)
		case string:
			colKeys = append(colKeys, id)
		default:
			return sql3.NewErrInternalf("unhandled _id data type '%T'", id)
		}
		return nil
	}

	//find the _id column and evaluate
	var err error
	var columnID interface{}
	for idx, iv := range i.insertValues {
		targetColumn := i.targetColumns[idx]
		if strings.EqualFold(targetColumn.columnName, "_id") {
			columnID, err = iv.Evaluate(nil)
			if err != nil {
				return nil, err
			}
			break
		}
	}

	//eval all the expressions and do the insert
	for idx, iv := range i.insertValues {
		colIDs = make([]uint64, 0)
		colKeys = make([]string, 0)

		targetColumn := i.targetColumns[idx]

		if strings.EqualFold(targetColumn.columnName, "_id") {
			continue
		}

		eval, err := iv.Evaluate(nil)
		if err != nil {
			return nil, err
		}

		//nothing to do if a value is null
		if eval == nil {
			continue
		}

		sourceType := iv.Type()
		switch targetType := i.targetColumns[idx].dataType.(type) {
		case *parser.DataTypeInt:

			err = addColID(columnID)
			if err != nil {
				return nil, err
			}

			vals := make([]int64, 1)
			vals[0] = eval.(int64)

			req := &pilosa.ImportValueRequest{
				Index:      i.tableName,
				Field:      targetColumn.columnName,
				Shard:      0, //TODO: handle non-0 shards
				ColumnIDs:  colIDs,
				ColumnKeys: colKeys,
				Values:     vals,
			}

			err = i.planner.computeAPI.ImportValue(ctx, qcx, req)
			if err != nil {
				return nil, err
			}

		case *parser.DataTypeBool:
			err = addColID(columnID)
			if err != nil {
				return nil, err
			}

			val := eval.(bool)
			vals := make([]uint64, 1)
			if val {
				vals[0] = 1
			} else {
				vals[0] = 0
			}

			req := &pilosa.ImportRequest{
				Index:      i.tableName,
				Field:      targetColumn.columnName,
				Shard:      0, //TODO: handle non-0 shards
				ColumnIDs:  colIDs,
				ColumnKeys: colKeys,
				RowIDs:     vals,
			}

			err = i.planner.computeAPI.Import(ctx, qcx, req)
			if err != nil {
				return nil, err
			}

		case *parser.DataTypeDecimal:
			err = addColID(columnID)
			if err != nil {
				return nil, err
			}

			vals := make([]float64, 1)
			vals[0] = eval.(pql.Decimal).Float64()

			req := &pilosa.ImportValueRequest{
				Index:       i.tableName,
				Field:       targetColumn.columnName,
				Shard:       0, //TODO: handle non-0 shards
				ColumnIDs:   colIDs,
				ColumnKeys:  colKeys,
				FloatValues: vals,
			}

			err = i.planner.computeAPI.ImportValue(ctx, qcx, req)
			if err != nil {
				return nil, err
			}

		case *parser.DataTypeID:
			err = addColID(columnID)
			if err != nil {
				return nil, err
			}

			coercedVal, err := coerceValue(sourceType, targetType, eval, parser.Pos{Line: 0, Column: 0})
			if err != nil {
				return nil, err
			}

			vals := make([]uint64, 1)
			vals[0] = uint64(coercedVal.(int64))

			req := &pilosa.ImportRequest{
				Index:      i.tableName,
				Field:      targetColumn.columnName,
				Shard:      0, //TODO: handle non-0 shards
				ColumnIDs:  colIDs,
				ColumnKeys: colKeys,
				RowIDs:     vals,
			}

			err = i.planner.computeAPI.Import(ctx, qcx, req)
			if err != nil {
				return nil, err
			}

		case *parser.DataTypeIDSet:
			rowIDs := make([]uint64, 0)
			rowSet := eval.([]int64)
			for k := range rowSet {
				err = addColID(columnID)
				if err != nil {
					return nil, err
				}
				rowIDs = append(rowIDs, uint64(rowSet[k]))
			}

			req := &pilosa.ImportRequest{
				Index:      i.tableName,
				Field:      targetColumn.columnName,
				Shard:      0, //TODO: handle non-0 shards
				ColumnIDs:  colIDs,
				ColumnKeys: colKeys,
				RowIDs:     rowIDs,
			}
			err = i.planner.computeAPI.Import(ctx, qcx, req)
			if err != nil {
				return nil, err
			}

		case *parser.DataTypeIDSetQuantum:
			rowIDs := make([]uint64, 0)
			timestamps := make([]int64, 0)

			coercedVal, err := coerceValue(sourceType, targetType, eval, parser.Pos{Line: 0, Column: 0})
			if err != nil {
				return nil, err
			}

			record := coercedVal.([]interface{})
			rowSet := record[1].([]int64)
			for k := range rowSet {
				err = addColID(columnID)
				if err != nil {
					return nil, err
				}
				rowIDs = append(rowIDs, uint64(rowSet[k]))
			}

			if record[0] == nil {
				timestamps = nil
			} else {
				timestamp, ok := record[0].(time.Time)
				if !ok {
					return nil, sql3.NewErrInternalf("unexpected type '%T'", record[0])
				}
				for range rowSet {
					timestamps = append(timestamps, timestamp.Unix())
				}
			}

			req := &pilosa.ImportRequest{
				Index:      i.tableName,
				Field:      targetColumn.columnName,
				Shard:      0, //TODO: handle non-0 shards
				ColumnIDs:  colIDs,
				ColumnKeys: colKeys,
				RowIDs:     rowIDs,
				Timestamps: timestamps,
			}
			err = i.planner.computeAPI.Import(ctx, qcx, req)
			if err != nil {
				return nil, err
			}

		case *parser.DataTypeString:
			err = addColID(columnID)
			if err != nil {
				return nil, err
			}

			rowKeys := make([]string, 1)
			rowKeys[0] = eval.(string)

			req := &pilosa.ImportRequest{
				Index:      i.tableName,
				Field:      targetColumn.columnName,
				Shard:      0, //TODO: handle non-0 shards
				ColumnIDs:  colIDs,
				ColumnKeys: colKeys,
				RowKeys:    rowKeys,
			}
			err = i.planner.computeAPI.Import(ctx, qcx, req)
			if err != nil {
				return nil, err
			}

		case *parser.DataTypeStringSet:
			rowKeys := make([]string, 0)
			rowSet := eval.([]string)
			for k := range rowSet {
				err = addColID(columnID)
				if err != nil {
					return nil, err
				}
				rowKeys = append(rowKeys, rowSet[k])
			}

			req := &pilosa.ImportRequest{
				Index:      i.tableName,
				Field:      targetColumn.columnName,
				Shard:      0, //TODO: handle non-0 shards
				ColumnIDs:  colIDs,
				ColumnKeys: colKeys,
				RowKeys:    rowKeys,
			}
			err = i.planner.computeAPI.Import(ctx, qcx, req)
			if err != nil {
				return nil, err
			}

		case *parser.DataTypeStringSetQuantum:
			rowKeys := make([]string, 0)
			timestamps := make([]int64, 0)

			coercedVal, err := coerceValue(sourceType, targetType, eval, parser.Pos{Line: 0, Column: 0})
			if err != nil {
				return nil, err
			}

			record := coercedVal.([]interface{})
			rowSet := record[1].([]string)
			for k := range rowSet {
				err = addColID(columnID)
				if err != nil {
					return nil, err
				}
				rowKeys = append(rowKeys, rowSet[k])
			}

			if record[0] == nil {
				timestamps = nil
			} else {
				timestamp, ok := record[0].(time.Time)
				if !ok {
					return nil, sql3.NewErrInternalf("unexpected type '%T'", record[0])
				}
				for range rowSet {
					timestamps = append(timestamps, timestamp.Unix())
				}
			}

			req := &pilosa.ImportRequest{
				Index:      i.tableName,
				Field:      targetColumn.columnName,
				Shard:      0, //TODO: handle non-0 shards
				ColumnIDs:  colIDs,
				ColumnKeys: colKeys,
				RowKeys:    rowKeys,
				Timestamps: timestamps,
			}
			err = i.planner.computeAPI.Import(ctx, qcx, req)
			if err != nil {
				return nil, err
			}

		case *parser.DataTypeTimestamp:
			err = addColID(columnID)
			if err != nil {
				return nil, err
			}

			coercedVal, err := coerceValue(sourceType, targetType, eval, parser.Pos{Line: 0, Column: 0})
			if err != nil {
				return nil, err
			}

			vals := make([]time.Time, 1)
			vals[0] = coercedVal.(time.Time)

			req := &pilosa.ImportValueRequest{
				Index:           i.tableName,
				Field:           targetColumn.columnName,
				Shard:           0, //TODO: handle non-0 shards
				ColumnIDs:       colIDs,
				ColumnKeys:      colKeys,
				TimestampValues: vals,
			}

			err = i.planner.computeAPI.ImportValue(ctx, qcx, req)
			if err != nil {
				return nil, err
			}

		default:
			return nil, sql3.NewErrInternalf("unhandled data type '%T'", targetType)
		}
	}

	return nil, types.ErrNoMoreRows
}
