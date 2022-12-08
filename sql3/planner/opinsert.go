// Copyright 2021 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"fmt"
	"strings"
	"time"

	pilosa "github.com/molecula/featurebase/v3"
	fbbatch "github.com/molecula/featurebase/v3/batch"
	"github.com/molecula/featurebase/v3/dax"
	"github.com/molecula/featurebase/v3/pql"
	"github.com/molecula/featurebase/v3/sql3"
	"github.com/molecula/featurebase/v3/sql3/planner/types"
	"github.com/pkg/errors"
)

// PlanOpInsert plan operator to handle INSERT.
type PlanOpInsert struct {
	planner       *ExecutionPlanner
	tableName     string
	targetColumns []*qualifiedRefPlanExpression
	insertValues  [][]types.PlanExpression
	warnings      []string
}

func NewPlanOpInsert(p *ExecutionPlanner, tableName string, targetColumns []*qualifiedRefPlanExpression, insertValues [][]types.PlanExpression) *PlanOpInsert {
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
		sc = append(sc, fmt.Sprintf("'%s', '%s', '%s'", e.ColumnName, e.RelationName, e.Type.TypeDescription()))
	}
	result["_schema"] = sc
	result["tableName"] = p.tableName
	ps := make([]interface{}, 0)
	for _, e := range p.targetColumns {
		ps = append(ps, e.Plan())
	}
	result["targetColumns"] = ps
	pps := make([]interface{}, 0)
	for _, tuple := range p.insertValues {
		ps := make([]interface{}, 0)
		for _, e := range tuple {
			ps = append(ps, e.Plan())
		}
		pps = append(pps, ps)
	}
	result["insertValues"] = pps
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
	insertValues  [][]types.PlanExpression
}

var _ types.RowIterator = (*insertRowIter)(nil)

func (i *insertRowIter) Next(ctx context.Context) (types.Row, error) {
	// posID is the position of the "_id" column in both the targetColumns and
	// values lists.
	var posID int

	// posVals maps the position in the tuple to the position in the row.Values.
	// It essentially takes the _id column into account and skips it.
	//
	// So for example, if we have sql:
	//   INSERT INTO (a, _id, b, c) VALUES ('aa', 1, 'bb', 'cc');
	//
	// Then we want row.ID = 1 and row.Values to contain {'aa', 'bb', 'cc'}.
	// This means posVals would contain the map []int{0, 1*, 1, 2}, which maps
	// VALUES positions (0,2,3) to row.Values (0, 1, 2). Note, the _id position
	// (shown as 1* in the example above) isn't used because we handle it
	// separately.
	posVals := make([]int, len(i.targetColumns))

	var foundPosID bool
	for j := range i.targetColumns {
		if foundPosID {
			posVals[j] = j - 1
			continue
		}
		if strings.EqualFold(i.targetColumns[j].columnName, "_id") {
			posID = j
			foundPosID = true
		}
		posVals[j] = j
	}

	// batchSize is currently set to the size of the entire
	// VALUES list. In the future we may want to break this up into smaller
	// batches.
	batchSize := len(i.insertValues)

	// idxInfoBase is the full IndexInfo stored in the schema. The instance of
	// IndexInfo used in the import (and created below) will be based on the
	// information from idxInfoBase, but the fields may be a limited subset, and
	// may be in a different order.
	tname := dax.TableName(i.tableName)
	tbl, err := i.planner.schemaAPI.TableByName(ctx, tname)
	if err != nil {
		return nil, sql3.NewErrTableNotFound(0, 0, i.tableName)
	}
	idxInfoBase := pilosa.TableToIndexInfo(tbl)

	// idxInfo is a subset of idxInfoBase, containing only those fields included
	// in the INSERT INTO statement (i.e. only i.targetcolumns), and in the
	// order specified.
	idxInfo := &pilosa.IndexInfo{
		Name:       idxInfoBase.Name,
		CreatedAt:  idxInfoBase.CreatedAt,
		Options:    idxInfoBase.Options,
		Fields:     make([]*pilosa.FieldInfo, len(i.targetColumns)-1),
		ShardWidth: idxInfoBase.ShardWidth,
	}

	// Set up Fields based on i.targetColumns.
	var counter int
	for ii, targetColumn := range i.targetColumns {
		// Skip the "_id" column.
		if ii == posID {
			continue
		}
		idxInfo.Fields[counter] = idxInfoBase.Field(targetColumn.columnName)
		counter++
	}

	batch, err := fbbatch.NewBatch(i.planner.importer, batchSize, idxInfo, idxInfo.Fields,
		fbbatch.OptUseShardTransactionalEndpoint(true),
	)
	if err != nil {
		return nil, errors.Wrap(err, "setting up batch")
	}

	// row is the single instance of batch.Row allocated. It is re-used
	// throughout the for loop to minimize memory allocation.
	var row fbbatch.Row

	// Initialize row.Values to the size of the target columns, but exclude the
	// record ID ("_id") since that's stored in row.ID.
	row.Values = make([]interface{}, len(i.targetColumns)-1)

	for rowNumber, tuple := range i.insertValues {
		// Evaluate and set the record ID.
		if eval, err := tuple[posID].Evaluate(nil); err != nil {
			return nil, errors.Wrapf(err, "evaluating record id: %v", tuple[posID])
		} else {
			// These value types correspond to the types supported in batch.Add().
			switch recid := eval.(type) {
			case string, uint64, []byte:
				row.ID = recid
			case int64:
				if recid < 0 {
					return nil, sql3.NewErrInternalf("_id value cannot be negative: %d", recid)
				}
				row.ID = uint64(recid)
			default:
				// If we get to here, it's likey that the id type is unsupported
				// and will cause an error in batch.Add(). So there's no need to
				// return an error here in this default.
				row.ID = eval
			}
		}

		// Loop over the values in the tuple and populate the Row values.
		for idx, iv := range tuple {
			// Skip the record ID because that was already handled above
			// (prior to this loop).
			if idx == posID {
				continue
			}

			eval, err := iv.Evaluate(nil)
			if err != nil {
				return nil, errors.Wrapf(err, "evaluating tuple value: %v", iv)
			}

			columnName := idxInfo.Fields[posVals[idx]].Name

			// batch.Add does not typically look at field type to determine how
			// to handle a particular value in a row. Instead, it uses value
			// type. As an example, if the value type is int64, then batch.Add
			// assumes that it should be handled as if going into an `int`
			// field. Therefore, we need to look at the field type here and make
			// sure that the value types being sent through batch.Add align with
			// the field type assumptions that batch.Add is making.
			switch opts := idxInfo.Fields[posVals[idx]].Options; opts.Type {

			// By the time we get here, we assume that the planner has already
			// determined the value type such that the Evaluate() method called
			// above results in the correct value types. There is one exception:
			// sql3 treats all integer values as int64. This means that an ID
			// field, which expects a uint64 value, would get treated as an int
			// field. In order to avoid that, we cast int64 values to uint64
			// when the field type is set or mutex.
			case pilosa.FieldTypeSet, pilosa.FieldTypeMutex:
				switch v := eval.(type) {
				case int64:
					if v < 0 {
						return nil, sql3.NewErrInternalf("converting negative value to uint64: %d", v)
					}
					row.Values[posVals[idx]] = uint64(v)
				case []int64:
					uint64s := make([]uint64, len(v))
					for i := range v {
						if v[i] < 0 {
							return nil, sql3.NewErrInternalf("converting negative slice value to uint64: %d", v[i])
						}
						uint64s[i] = uint64(v[i])
					}
					row.Values[posVals[idx]] = uint64s
				default:
					row.Values[posVals[idx]] = eval
				}

			case pilosa.FieldTypeInt:
				if eval != nil {
					v, ok := eval.(int64)
					if !ok {
						return nil, sql3.NewErrInternalf("unexpected type %v", eval)
					}
					// check the min and max constraints here
					if v < opts.Min.ToInt64(0) || v > opts.Max.ToInt64(0) {
						return nil, sql3.NewErrInsertValueOutOfRange(0, 0, columnName, rowNumber+1, v)
					}
				}
				row.Values[posVals[idx]] = eval

			case pilosa.FieldTypeDecimal:
				if eval != nil {
					v, ok := eval.(pql.Decimal)
					if !ok {
						return nil, sql3.NewErrInternalf("unexpected type %v", eval)
					}
					// check the min and max constraints here
					if v.LessThan(opts.Min) || v.GreaterThan(opts.Max) {
						return nil, sql3.NewErrInsertValueOutOfRange(0, 0, columnName, rowNumber+1, v)
					}
				}
				row.Values[posVals[idx]] = eval

			case pilosa.FieldTypeTimestamp:
				switch v := eval.(type) {

				// time.Time is used for date literals generated in the parser.
				// For example, if using `current_time`, the type received here
				// will be a time.Time.
				case time.Time:
					// Convert Base, which is the epoch for Timestamp fields, to
					// a time.Time value.
					unit := fbbatch.TimeUnit(opts.TimeUnit)
					epoch, err := fbbatch.Int64ToTimestamp(unit, time.Time{}, opts.Base)
					if err != nil {
						return nil, errors.Wrapf(err, "converting base to epoch: %d", opts.Base)
					}

					i64, err := fbbatch.TimestampToInt64(unit, epoch, v)
					if err != nil {
						return nil, errors.Wrapf(err, "converting timestamp to int64: %s", v)
					}
					row.Values[posVals[idx]] = i64

				// string is the normal case for dates; used when the date is
				// provided as a string in the INSERT INTO statement.
				case string:
					ts, err := timestampFromString(v)
					if err != nil {
						return nil, errors.Wrapf(err, "parsing timestamp: %s", v)
					}

					// Convert Base, which is the epoch for Timestamp fields, to
					// a time.Time value.
					unit := fbbatch.TimeUnit(opts.TimeUnit)
					epoch, err := fbbatch.Int64ToTimestamp(unit, time.Time{}, opts.Base)
					if err != nil {
						return nil, errors.Wrapf(err, "converting base to epoch: %d", opts.Base)
					}

					i64, err := fbbatch.TimestampToInt64(unit, epoch, ts)
					if err != nil {
						return nil, errors.Wrapf(err, "converting timestamp to int64: %s", v)
					}
					row.Values[posVals[idx]] = i64

				// nil is to support `null` values.
				case nil:
					row.Values[posVals[idx]] = eval

				default:
					return nil, sql3.NewErrInternalf("unsupported timestamp type: %T", eval)
				}

			default:
				row.Values[posVals[idx]] = eval
			}
		}

		if err := batch.Add(row); err != nil {
			// Breaking here on ErrBatchNowFull is only valid because we are
			// explicity setting the batch size to the number of tuples in the
			// INSERT INTO statement. Which means we're only handling a single
			// batch. If this evolves to handle multiple batches, this will need
			// to instead call batch.Import() and continue looping over tuples.
			// We may also need to handle ErrBatchNowStale.
			if err == fbbatch.ErrBatchNowFull {
				break
			}
			return nil, errors.Wrap(err, "adding record")
		}
	}

	if err := batch.Import(); err != nil {
		return nil, errors.Wrap(err, "importing batch")
	}

	return nil, types.ErrNoMoreRows
}
