// Copyright 2021 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"time"

	pilosa "github.com/molecula/featurebase/v3"
	"github.com/molecula/featurebase/v3/sql3"
	"github.com/molecula/featurebase/v3/sql3/parser"
	"github.com/molecula/featurebase/v3/sql3/planner/types"
)

// bulkInsertMappedColumn specifies a mapping from the source
// data to a target column name
type bulkInsertMappedColumn struct {
	// source expression
	// using an interface for so we have flexibility in data types as format changes
	columnSource interface{}
	// name of the target column
	columnName string
	// data type of the target column
	columnDataType parser.ExprDataType
}

// bulkInsertOptions contains options for bulk insert
type bulkInsertOptions struct {
	// name of the file we're going to read
	fileName string
	// number of rows in a batch
	batchSize int
	// stop after this many rows
	rowsLimit int
	// format specifier (CSV is the only one right now)
	format string
	// the column map in the source data to use as the _id value
	// if empty or nill auto increment
	// using an interface so we have flexibility in data types as format changes
	idColumnMap []interface{}
	// column mappings
	columnMap []*bulkInsertMappedColumn
}

// PlanOpBulkInsert plan operator to handle INSERT.
type PlanOpBulkInsert struct {
	planner   *ExecutionPlanner
	tableName string
	isKeyed   bool
	options   *bulkInsertOptions
	warnings  []string
}

func NewPlanOpBulkInsert(p *ExecutionPlanner, tableName string, isKeyed bool, options *bulkInsertOptions) *PlanOpBulkInsert {
	return &PlanOpBulkInsert{
		planner:   p,
		tableName: tableName,
		isKeyed:   isKeyed,
		options:   options,
		warnings:  make([]string, 0),
	}
}

func (p *PlanOpBulkInsert) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_op"] = fmt.Sprintf("%T", p)
	sc := make([]string, 0)
	for _, e := range p.Schema() {
		sc = append(sc, fmt.Sprintf("'%s', '%s', '%s'", e.ColumnName, e.RelationName, e.Type.TypeName()))
	}
	result["_schema"] = sc
	result["tableName"] = p.tableName

	options := make(map[string]interface{})
	options["batchsize"] = p.options.batchSize
	options["rowslimit"] = p.options.rowsLimit
	options["format"] = p.options.format
	if len(p.options.idColumnMap) > 0 {
		options["idColumnMap"] = p.options.idColumnMap
	} else {
		options["idColumnMap"] = "autoincrement"
	}
	colMap := make([]interface{}, 0)
	for _, m := range p.options.columnMap {
		cm := make(map[string]interface{})
		cm["columnSource"] = m.columnSource
		cm["columnName"] = m.columnName
		colMap = append(colMap, cm)
	}
	options["columnMap"] = colMap

	result["options"] = options
	return result
}

func (p *PlanOpBulkInsert) String() string {
	return ""
}

func (p *PlanOpBulkInsert) AddWarning(warning string) {
	p.warnings = append(p.warnings, warning)
}

func (p *PlanOpBulkInsert) Warnings() []string {
	return p.warnings
}

func (p *PlanOpBulkInsert) Schema() types.Schema {
	return types.Schema{}
}

func (p *PlanOpBulkInsert) Children() []types.PlanOperator {
	return []types.PlanOperator{}
}

func (p *PlanOpBulkInsert) Iterator(ctx context.Context, row types.Row) (types.RowIterator, error) {
	return &bulkInsertCSVRowIter{
		planner:   p.planner,
		tableName: p.tableName,
		isKeyed:   p.isKeyed,
		options:   p.options,
	}, nil
}

func (p *PlanOpBulkInsert) WithChildren(children ...types.PlanOperator) (types.PlanOperator, error) {
	return NewPlanOpBulkInsert(p.planner, p.tableName, p.isKeyed, p.options), nil
}

type bulkInsertCSVRowIter struct {
	planner   *ExecutionPlanner
	tableName string
	isKeyed   bool
	options   *bulkInsertOptions

	latch        *struct{}
	currentBatch []interface{}
	lastKeyValue uint64
}

var _ types.RowIterator = (*bulkInsertCSVRowIter)(nil)

func (i *bulkInsertCSVRowIter) Next(ctx context.Context) (types.Row, error) {
	if i.latch == nil {
		i.latch = &struct{}{}
		i.lastKeyValue = 0

		f, err := os.Open(i.options.fileName)
		if err != nil {
			return nil, err
		}

		defer f.Close()

		linesRead := 0
		csvReader := csv.NewReader(f)
		for {
			rec, err := csvReader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
			// do something with read line
			err = i.processCSVLine(ctx, rec)
			if err != nil {
				return nil, err
			}
			linesRead += 1
			// bail if we have a rows limit and we've hit it
			if i.options.rowsLimit > 0 && linesRead >= i.options.rowsLimit {
				break
			}
		}
	}
	return nil, types.ErrNoMoreRows
}

func (i *bulkInsertCSVRowIter) processCSVLine(ctx context.Context, line []string) error {
	if i.currentBatch == nil {
		i.currentBatch = make([]interface{}, 0)
	}
	i.currentBatch = append(i.currentBatch, line)
	if len(i.currentBatch) >= i.options.batchSize {
		log.Printf("BULK INSERT: processing batch (%d)", len(i.currentBatch))
		err := i.processBatch(ctx)
		log.Printf("BULK INSERT: batch processed")
		if err != nil {
			return err
		}
	}
	return nil
}

func (i *bulkInsertCSVRowIter) processBatch(ctx context.Context) error {

	batchLen := len(i.currentBatch)

	colIDs := make([]uint64, batchLen)
	colKeys := make([]string, batchLen)

	insertData := make([]interface{}, len(i.options.columnMap))

	addColID := func(index int, v interface{}) error {
		switch id := v.(type) {
		case int64:
			colIDs[index] = uint64(id)
		case uint64:
			colIDs[index] = id
		case string:
			colKeys[index] = id
		default:
			return sql3.NewErrInternalf("unhandled _id data type '%T'", id)
		}
		return nil
	}

	// make objects for each column depending on data type
	for cidx, mc := range i.options.columnMap {
		switch targetType := mc.columnDataType.(type) {
		case *parser.DataTypeID:
			vals := make([]uint64, batchLen)
			insertData[cidx] = vals

		case *parser.DataTypeInt:
			vals := make([]int64, batchLen)
			insertData[cidx] = vals

		case *parser.DataTypeString:
			vals := make([]string, batchLen)
			insertData[cidx] = vals

		case *parser.DataTypeTimestamp:
			vals := make([]time.Time, batchLen)
			insertData[cidx] = vals

		default:
			return sql3.NewErrInternalf("unhandled target type '%T'", targetType)
		}
	}

	// for each row in the batch add value to each mapped column
	log.Printf("BULK INSERT: building batch...")
	for rowIdx, row := range i.currentBatch {
		csvRow, ok := row.([]string)
		if !ok {
			return sql3.NewErrInternalf("unexpected row type '%T'", row)
		}

		//handle each column
		for colIdx, mc := range i.options.columnMap {

			columnPosition, ok := mc.columnSource.(int64)
			if !ok {
				return sql3.NewErrInternalf("unexpected columnPosition type '%T'", mc.columnSource)
			}
			switch targetType := mc.columnDataType.(type) {
			case *parser.DataTypeID:
				valueStr := csvRow[columnPosition]
				insertValue, err := strconv.ParseUint(valueStr, 10, 64)
				if err != nil {
					return err
				}
				columnData, ok := insertData[colIdx].([]uint64)
				if !ok {
					return sql3.NewErrInternalf("unexpected columnData type '%T'", insertData[colIdx])
				}
				columnData[rowIdx] = insertValue

			case *parser.DataTypeInt:
				valueStr := csvRow[columnPosition]
				insertValue, err := strconv.ParseInt(valueStr, 10, 64)
				if err != nil {
					return err
				}
				columnData, ok := insertData[colIdx].([]int64)
				if !ok {
					return sql3.NewErrInternalf("unexpected columnData type '%T'", insertData[colIdx])
				}
				columnData[rowIdx] = insertValue

			case *parser.DataTypeString:
				insertValue := csvRow[columnPosition]
				columnData, ok := insertData[colIdx].([]string)
				if !ok {
					return sql3.NewErrInternalf("unexpected columnData type '%T'", insertData[colIdx])
				}
				columnData[rowIdx] = insertValue

			case *parser.DataTypeTimestamp:
				valueStr := csvRow[columnPosition]

				var insertValue time.Time
				if tm, err := time.ParseInLocation(time.RFC3339Nano, valueStr, time.UTC); err == nil {
					insertValue = tm
				} else if tm, err := time.ParseInLocation(time.RFC3339, valueStr, time.UTC); err == nil {
					insertValue = tm
				} else if tm, err := time.ParseInLocation("2006-01-02 15:04:05", valueStr, time.UTC); err == nil {
					insertValue = tm
				} else if tm, err := time.ParseInLocation("2006-01-02", valueStr, time.UTC); err == nil {
					insertValue = tm
				} else {
					return err
				}
				columnData, ok := insertData[colIdx].([]time.Time)
				if !ok {
					return sql3.NewErrInternalf("unexpected columnData type '%T'", insertData[colIdx])
				}
				columnData[rowIdx] = insertValue

			default:
				return sql3.NewErrInternalf("unhandled target type '%T'", targetType)
			}

		}

		// add _id
		if len(i.options.idColumnMap) > 0 {
			return sql3.NewErrInternalf("not yet implemented")
		} else {
			// if the table is keyed, use the string representation of an integer key value
			if i.isKeyed {
				//auto increment
				err := addColID(rowIdx, fmt.Sprintf("%d", i.lastKeyValue))
				if err != nil {
					return err
				}
			} else {
				//auto increment
				err := addColID(rowIdx, i.lastKeyValue)
				if err != nil {
					return err
				}
			}
			i.lastKeyValue += 1
		}
	}
	log.Printf("BULK INSERT: building batch complete")

	// now loop again and actually do the insert

	log.Printf("BULK INSERT: inserting columns...")
	qcx := i.planner.computeAPI.Txf().NewQcx()

	//nil out colids if the table is keyed
	for colIdx, mc := range i.options.columnMap {
		log.Printf("BULK INSERT: inserting column '%s'...", mc.columnName)
		if i.isKeyed {
			colIDs = nil
		}
		switch targetType := mc.columnDataType.(type) {
		case *parser.DataTypeID:
			vals, ok := insertData[colIdx].([]uint64)
			if !ok {
				return sql3.NewErrInternalf("unexpected insert data type '%T'", insertData[colIdx])
			}

			req := &pilosa.ImportRequest{
				Index:      i.tableName,
				Field:      mc.columnName,
				Shard:      0, //TODO: handle non-0 shards
				ColumnIDs:  colIDs,
				ColumnKeys: colKeys,
				RowIDs:     vals,
			}

			err := i.planner.computeAPI.Import(ctx, qcx, req)
			if err != nil {
				return err
			}

		case *parser.DataTypeInt:
			vals, ok := insertData[colIdx].([]int64)
			if !ok {
				return sql3.NewErrInternalf("unexpected insert data type '%T'", insertData[colIdx])
			}

			req := &pilosa.ImportValueRequest{
				Index:      i.tableName,
				Field:      mc.columnName,
				Shard:      0, //TODO: handle non-0 shards
				ColumnIDs:  colIDs,
				ColumnKeys: colKeys,
				Values:     vals,
			}

			err := i.planner.computeAPI.ImportValue(ctx, qcx, req)
			if err != nil {
				return err
			}

		case *parser.DataTypeString:
			vals, ok := insertData[colIdx].([]string)
			if !ok {
				return sql3.NewErrInternalf("unexpected insert data type '%T'", insertData[colIdx])
			}

			req := &pilosa.ImportRequest{
				Index:      i.tableName,
				Field:      mc.columnName,
				Shard:      0, //TODO: handle non-0 shards
				ColumnIDs:  colIDs,
				ColumnKeys: colKeys,
				RowKeys:    vals,
			}
			err := i.planner.computeAPI.Import(ctx, qcx, req)
			if err != nil {
				return err
			}

		case *parser.DataTypeTimestamp:
			vals, ok := insertData[colIdx].([]time.Time)
			if !ok {
				return sql3.NewErrInternalf("unexpected insert data type '%T'", insertData[colIdx])
			}

			// TODO (pok) - getting and error for timestamp columns
			// 'Error: local import after remote imports: number of columns (1) and number of values (0) do not match'
			req := &pilosa.ImportValueRequest{
				Index:           i.tableName,
				Field:           mc.columnName,
				Shard:           0, //TODO: handle non-0 shards
				ColumnIDs:       colIDs,
				ColumnKeys:      colKeys,
				TimestampValues: vals,
			}

			err := i.planner.computeAPI.ImportValue(ctx, qcx, req)
			if err != nil {
				return err
			}

		default:
			return sql3.NewErrInternalf("unhandled target type '%T'", targetType)
		}
		log.Printf("BULK INSERT: inserting column '%s' complete.", mc.columnName)
	}
	log.Printf("BULK INSERT: inserting columns complete.")

	/*


		//eval all the expressions and do the insert
		for idx, iv := range i.insertValues {

			sourceType := iv.Type()
			switch targetType := i.targetColumns[idx].dataType.(type) {

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
					for _ = range rowSet {
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
					for _ = range rowSet {
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

			default:
				return nil, sql3.NewErrInternalf("unhandled data type '%T'", targetType)
			}
		}*/

	// done with current batch
	i.currentBatch = nil
	return nil
}
