// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	pilosa "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/pql"
	"github.com/featurebasedb/featurebase/v3/sql3"
	"github.com/featurebasedb/featurebase/v3/sql3/parser"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
)

// PlanOpCopy is a copy operator
type PlanOpCopy struct {
	planner     *ExecutionPlanner
	targetTable string
	url         string
	apiKey      string
	ddl         string
	ChildOp     types.PlanOperator

	warnings []string
}

func NewPlanOpCopy(planner *ExecutionPlanner, targetName string, url string, apiKey string, ddl string, child types.PlanOperator) *PlanOpCopy {
	return &PlanOpCopy{
		planner:     planner,
		targetTable: targetName,
		url:         url,
		apiKey:      apiKey,
		ddl:         ddl,
		ChildOp:     child,
		warnings:    make([]string, 0),
	}
}

func (p *PlanOpCopy) Schema() types.Schema {
	return types.Schema{}
}

func (p *PlanOpCopy) Iterator(ctx context.Context, row types.Row) (types.RowIterator, error) {
	child, err := p.ChildOp.Iterator(ctx, row)
	if err != nil {
		return nil, err
	}
	if p.url != "" {
		return newRemoteCopyIterator(p.planner, p.targetTable, p.url, p.apiKey, p.ddl, p.ChildOp.Schema(), child), nil
	}
	return newCopyIterator(p.planner, p.targetTable, p.ddl, p.ChildOp.Schema(), child), nil
}

func (p *PlanOpCopy) WithChildren(children ...types.PlanOperator) (types.PlanOperator, error) {
	if len(children) != 1 {
		return nil, sql3.NewErrInternalf("unexpected number of children '%d'", len(children))
	}
	return NewPlanOpCopy(p.planner, p.targetTable, p.url, p.apiKey, p.ddl, children[0]), nil
}

func (p *PlanOpCopy) Children() []types.PlanOperator {
	return []types.PlanOperator{
		p.ChildOp,
	}
}

func (p *PlanOpCopy) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_op"] = fmt.Sprintf("%T", p)
	result["_schema"] = p.Schema().Plan()
	result["child"] = p.ChildOp.Plan()
	result["child"] = p.ChildOp.Plan()
	return result
}

func (p *PlanOpCopy) String() string {
	return ""
}

func (p *PlanOpCopy) AddWarning(warning string) {
	p.warnings = append(p.warnings, warning)
}

func (p *PlanOpCopy) Warnings() []string {
	return p.warnings
}

func (p *PlanOpCopy) Expressions() []types.PlanExpression {
	return []types.PlanExpression{}
}

func (p *PlanOpCopy) WithUpdatedExpressions(exprs ...types.PlanExpression) (types.PlanOperator, error) {
	if len(exprs) > 0 {
		return nil, sql3.NewErrInternalf("unexpected number of exprs '%d'", len(exprs))
	}
	return p, nil
}

type copyIterator struct {
	planner         *ExecutionPlanner
	targetTableName string
	copySchema      types.Schema
	ddl             string
	child           types.RowIterator
	hasStarted      *struct{}
}

func newCopyIterator(planner *ExecutionPlanner, targetTableName string, ddl string, copySchema types.Schema, childIter types.RowIterator) *copyIterator {
	return &copyIterator{
		planner:         planner,
		targetTableName: targetTableName,
		ddl:             ddl,
		copySchema:      copySchema,
		child:           childIter,
	}
}

func (i *copyIterator) Next(ctx context.Context) (types.Row, error) {
	if i.hasStarted == nil {
		// parse and execute the ddl to create the table
		ast, err := parser.NewParser(strings.NewReader(i.ddl)).ParseStatement()
		if err != nil {
			return nil, err
		}
		ct, ok := ast.(*parser.CreateTableStatement)
		if !ok {
			return nil, sql3.NewErrInternalf("unexpected ast type")
		}
		// analyze
		err = i.planner.analyzeCreateTableStatement(ct)
		if err != nil {
			return nil, err
		}
		ctOp, err := i.planner.compileCreateTableStatement(ctx, ct)
		if err != nil {
			return nil, err
		}
		ctIter, err := ctOp.Iterator(context.Background(), nil)
		if err != nil {
			return nil, err
		}
		_, err = ctIter.Next(ctx)
		if err != nil && err != types.ErrNoMoreRows {
			return nil, err
		}

		targetColumns := make([]*qualifiedRefPlanExpression, 0)

		for _, s := range i.copySchema {
			targetColumns = append(targetColumns, newQualifiedRefPlanExpression(i.targetTableName, s.ColumnName, 0, s.Type))
		}

		// build an insert iterator for the target table
		insertIter := &insertRowIter{
			planner:       i.planner,
			tableName:     i.targetTableName,
			targetColumns: targetColumns,
		}

		batchCount := 0
		insertBatch := make([][]types.PlanExpression, 0)

		for {
			// get a source row
			row, err := i.child.Next(ctx)
			if err != nil {
				if err == types.ErrNoMoreRows {
					break
				}
				return nil, err
			}

			// add it to target batch

			irow := make([]types.PlanExpression, len(row))
			for i, s := range i.copySchema {
				switch ty := s.Type.(type) {
				case *parser.DataTypeID, *parser.DataTypeInt:
					val, ok := row[i].(int64)
					if !ok {
						return nil, sql3.NewErrInternalf("unexpected type '%T'", row[i])
					}
					irow[i] = newIntLiteralPlanExpression(val)

				case *parser.DataTypeDecimal:
					val, ok := row[i].(pql.Decimal)
					if !ok {
						return nil, sql3.NewErrInternalf("unexpected type '%T'", row[i])
					}
					irow[i] = newFloatLiteralPlanExpression(val.String())

				case *parser.DataTypeString:
					val, ok := row[i].(string)
					if !ok {
						return nil, sql3.NewErrInternalf("unexpected type '%T'", row[i])
					}
					irow[i] = newStringLiteralPlanExpression(val)

				case *parser.DataTypeBool:
					val, ok := row[i].(bool)
					if !ok {
						return nil, sql3.NewErrInternalf("unexpected type '%T'", row[i])
					}
					irow[i] = newBoolLiteralPlanExpression(val)

				case *parser.DataTypeTimestamp:
					val, ok := row[i].(time.Time)
					if !ok {
						return nil, sql3.NewErrInternalf("unexpected type '%T'", row[i])
					}
					irow[i] = newTimestampLiteralPlanExpression(val)

				case *parser.DataTypeStringSet:
					val, ok := row[i].([]string)
					if !ok {
						return nil, sql3.NewErrInternalf("unexpected type '%T'", row[i])
					}

					members := make([]types.PlanExpression, 0)
					for _, m := range val {
						members = append(members, newStringLiteralPlanExpression(m))
					}
					irow[i] = newExprArrayLiteralPlanExpression(members, parser.NewDataTypeArray(parser.NewDataTypeString()))

				case *parser.DataTypeIDSet:
					val, ok := row[i].([]int64)
					if !ok {
						return nil, sql3.NewErrInternalf("unexpected type '%T'", row[i])
					}

					members := make([]types.PlanExpression, 0)
					for _, m := range val {
						members = append(members, newIntLiteralPlanExpression(m))
					}
					irow[i] = newExprArrayLiteralPlanExpression(members, parser.NewDataTypeArray(parser.NewDataTypeID()))

				default:
					return nil, sql3.NewErrInternalf("unhandled type '%T'", ty)
				}
			}
			insertBatch = append(insertBatch, irow)

			// inc batch count
			batchCount += 1
			if batchCount > 1000 {
				// do the insert
				insertIter.insertValues = insertBatch
				_, err = insertIter.Next(context.Background())
				if err != nil && err != types.ErrNoMoreRows {
					return nil, err
				}
				// reset
				batchCount = 0
				insertBatch = make([][]types.PlanExpression, 0)
			}
		}
		if len(insertBatch) > 0 {
			// do the insert
			insertIter.insertValues = insertBatch
			_, err = insertIter.Next(context.Background())
			if err != nil && err != types.ErrNoMoreRows {
				return nil, err
			}
		}

		i.hasStarted = &struct{}{}
	}
	return nil, types.ErrNoMoreRows
}

type remoteCopyIterator struct {
	planner         *ExecutionPlanner
	targetTableName string
	copySchema      types.Schema
	url             string
	apiKey          string
	ddl             string
	child           types.RowIterator
	hasStarted      *struct{}
}

func newRemoteCopyIterator(planner *ExecutionPlanner, targetTableName string, url string, apiKey string, ddl string, copySchema types.Schema, childIter types.RowIterator) *remoteCopyIterator {
	return &remoteCopyIterator{
		planner:         planner,
		targetTableName: targetTableName,
		url:             url,
		apiKey:          apiKey,
		ddl:             ddl,
		copySchema:      copySchema,
		child:           childIter,
	}
}

func (i *remoteCopyIterator) remoteExec(ctx context.Context, sql string) (*pilosa.WireQueryResponse, error) {
	// Create HTTP request.
	req, err := http.NewRequest("POST", i.url, strings.NewReader(sql))
	if err != nil {
		return nil, sql3.NewErrInternalf("error executing remotely: %s", err.Error())
	}

	req.Header.Set("Content-Length", strconv.Itoa(len(sql)))
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "pilosa/"+i.planner.systemAPI.Version())
	if len(i.apiKey) > 0 {
		req.Header.Set("X-API-Key", i.apiKey)
	}

	// Execute request against the host.
	resp, err := http.DefaultClient.Do(req.WithContext(ctx))
	if err != nil {
		return nil, sql3.NewErrInternalf("error executing remotely: %s", err.Error())
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, sql3.NewErrInternalf("error executing remotely: %s", err.Error())
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if resp.StatusCode == 401 {
			return nil, sql3.NewErrRemoteUnauthorized(0, 0, i.url)
		}
		// we have an error
		return nil, sql3.NewErrInternalf("error  executing remotely: %d, %s", resp.StatusCode, string(body))
	}

	sqlResponse := &pilosa.WireQueryResponse{}
	err = sqlResponse.UnmarshalJSONTyped([]byte(body), true)
	if err != nil {
		return nil, sql3.NewErrInternalf("error executing remotely: %s", err.Error())
	}

	if len(sqlResponse.Error) > 0 {
		return nil, sql3.NewErrInternalf("error executing remotely: %s", sqlResponse.Error)
	}

	return sqlResponse, nil
}

func (i *remoteCopyIterator) Next(ctx context.Context) (types.Row, error) {
	if i.hasStarted == nil {
		// execute the ddl to create the table
		_, err := i.remoteExec(ctx, i.ddl)
		if err != nil {
			return nil, err
		}

		// build bulk insert statement
		var buf bytes.Buffer
		buf.WriteString("bulk insert into ")
		fmt.Fprintf(&buf, "%s", i.targetTableName)
		buf.WriteString(" (")

		for i, s := range i.copySchema {
			if i > 0 {
				buf.WriteString(", ")
			}
			fmt.Fprintf(&buf, "%s", s.ColumnName)
		}
		buf.WriteString(") map (")
		for i, s := range i.copySchema {
			if i > 0 {
				buf.WriteString(", ")
			}
			fmt.Fprintf(&buf, "'$._%d' %s", i, s.Type.TypeDescription())
		}
		buf.WriteString(") from x'")
		header := buf.String()

		batchCount := 0
		var batchBuf bytes.Buffer

		for {
			// get a source row
			row, err := i.child.Next(ctx)
			if err != nil {
				if err == types.ErrNoMoreRows {
					break
				}
				return nil, err
			}

			// add it to target batch
			var rowBuf bytes.Buffer
			rowBuf.WriteString("{")
			for i, s := range i.copySchema {
				if i > 0 {
					rowBuf.WriteString(",")
				}
				fmt.Fprintf(&rowBuf, `"_%d":`, i)

				if row[i] == nil {
					rowBuf.WriteString("null")
					continue
				}

				switch ty := s.Type.(type) {
				case *parser.DataTypeID, *parser.DataTypeInt:
					val, ok := row[i].(int64)
					if !ok {
						return nil, sql3.NewErrInternalf("unexpected type '%T'", row[i])
					}
					fmt.Fprintf(&rowBuf, "%d", val)

				case *parser.DataTypeString:
					val, ok := row[i].(string)
					if !ok {
						return nil, sql3.NewErrInternalf("unexpected type '%T'", row[i])
					}
					// escape single quotes
					val = strings.ReplaceAll(val, `'`, `''`)
					// and double quotes
					val = strings.ReplaceAll(val, `"`, `\"`)
					// and line feeds
					if strings.Contains(val, "\n") {
						val = strings.ReplaceAll(val, "\n", "\\n")
					}
					fmt.Fprintf(&rowBuf, `"%s"`, val)

				case *parser.DataTypeBool:
					val, ok := row[i].(bool)
					if !ok {
						return nil, sql3.NewErrInternalf("unexpected type '%T'", row[i])
					}
					fmt.Fprintf(&rowBuf, "%v", val)

				case *parser.DataTypeTimestamp:
					val, ok := row[i].(time.Time)
					if !ok {
						return nil, sql3.NewErrInternalf("unexpected type '%T'", row[i])
					}
					fmt.Fprintf(&rowBuf, `"%s"`, val.Format(time.RFC3339Nano))

				case *parser.DataTypeStringSet:
					val, ok := row[i].([]string)
					if !ok {
						return nil, sql3.NewErrInternalf("unexpected type '%T'", row[i])
					}
					rowBuf.WriteString("[")
					for j, s := range val {
						if j > 0 {
							rowBuf.WriteString(",")
						}
						fmt.Fprintf(&rowBuf, `"%s"`, s)
					}
					rowBuf.WriteString("]")

				case *parser.DataTypeIDSet:
					val, ok := row[i].([]int64)
					if !ok {
						return nil, sql3.NewErrInternalf("unexpected type '%T'", row[i])
					}
					rowBuf.WriteString("[")
					for j, s := range val {
						if j > 0 {
							rowBuf.WriteString(",")
						}
						fmt.Fprintf(&rowBuf, `%d`, s)
					}
					rowBuf.WriteString("]")

				default:
					return nil, sql3.NewErrInternalf("unhandled type '%T'", ty)
				}
			}
			rowBuf.WriteString("}\n")
			batchBuf.Write(rowBuf.Bytes())

			// inc batch count
			batchCount += 1
			if batchCount > 10000 {
				// do the insert

				var reqBuf bytes.Buffer
				reqBuf.WriteString(header)
				reqBuf.Write(batchBuf.Bytes())
				reqBuf.WriteString("' with batchsize 10000 input 'STREAM' format 'NDJSON'")

				_, err := i.remoteExec(ctx, reqBuf.String())
				if err != nil {
					return nil, err
				}

				// reset
				batchCount = 0
				batchBuf.Reset()
			}
		}
		if batchCount > 0 {
			// do the insert

			var reqBuf bytes.Buffer
			reqBuf.WriteString(header)
			reqBuf.Write(batchBuf.Bytes())
			reqBuf.WriteString("' with batchsize 10000 input 'STREAM' format 'NDJSON'")

			_, err := i.remoteExec(ctx, reqBuf.String())
			if err != nil {
				return nil, err
			}
		}

		i.hasStarted = &struct{}{}
	}
	return nil, types.ErrNoMoreRows
}
