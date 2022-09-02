// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"fmt"

	pilosa "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/pql"
	"github.com/featurebasedb/featurebase/v3/sql3"
	"github.com/featurebasedb/featurebase/v3/sql3/parser"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
	"github.com/pkg/errors"
)

// PlanOpPQLTableScan plan operator handles a PQL table scan
type PlanOpPQLTableScan struct {
	planner   *ExecutionPlanner
	tableName string
	columns   []types.PlanExpression
	filter    types.PlanExpression
	topExpr   types.PlanExpression
	warnings  []string
}

func NewPlanOpPQLTableScan(p *ExecutionPlanner, tableName string, columns []types.PlanExpression, filter types.PlanExpression) *PlanOpPQLTableScan {
	return &PlanOpPQLTableScan{
		planner:   p,
		tableName: tableName,
		columns:   columns,
		filter:    filter,
	}
}

func (p *PlanOpPQLTableScan) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_op"] = fmt.Sprintf("%T", p)
	sc := make([]string, 0)
	for _, e := range p.Schema() {
		sc = append(sc, fmt.Sprintf("'%s', '%s', '%s'", e.Name, e.Table, e.Type.TypeName()))
	}
	result["_schema"] = sc

	result["tableName"] = p.tableName

	if p.topExpr != nil {
		result["topExpr"] = p.topExpr.Plan()
	}
	if p.filter != nil {
		result["filter"] = p.filter.Plan()
	}

	ps := make([]interface{}, 0)
	for _, c := range p.columns {
		ps = append(ps, c.Plan())
	}
	result["columns"] = ps
	return result
}

func (p *PlanOpPQLTableScan) String() string {
	return ""
}

func (p *PlanOpPQLTableScan) AddWarning(warning string) {
	p.warnings = append(p.warnings, warning)
}

func (p *PlanOpPQLTableScan) Warnings() []string {
	return p.warnings
}

func (p *PlanOpPQLTableScan) Schema() types.Schema {
	result := make(types.Schema, 0)
	for _, col := range p.columns {
		si, ok := col.(types.SchemaIdentifiable)
		if ok {
			result = append(result, &types.PlannerColumn{
				Name:  si.Name(),
				Table: p.tableName,
				Type:  col.Type(),
			})
		}
	}
	return result
}

func (p *PlanOpPQLTableScan) Children() []types.PlanOperator {
	return []types.PlanOperator{}
}

func (p *PlanOpPQLTableScan) Iterator(ctx context.Context, row types.Row) (types.RowIterator, error) {
	return &tableScanRowIter{
		planner:   p.planner,
		tableName: p.tableName,
		columns:   p.columns,
		predicate: p.filter,
		topExpr:   p.topExpr,
	}, nil
}

func (p *PlanOpPQLTableScan) WithChildren(children ...types.PlanOperator) (types.PlanOperator, error) {
	return nil, nil
}

// TODO(pok) remove the name mapping here and do it by ordinal position

type tableScanRowIter struct {
	planner   *ExecutionPlanner
	tableName string
	columns   []types.PlanExpression
	predicate types.PlanExpression
	topExpr   types.PlanExpression

	result          []pilosa.ExtractedTableColumn
	rowWidth        int
	sourceColumnMap map[string]int
	targetColumnMap map[string]int
}

var _ types.RowIterator = (*tableScanRowIter)(nil)

func (i *tableScanRowIter) Next(ctx context.Context) (types.Row, error) {
	if i.result == nil {
		err := i.planner.checkAccess(ctx, i.tableName, accessTypeReadData)
		if err != nil {
			return nil, err
		}

		//go get the schema def and map names to indexes in the resultant row
		table, err := i.planner.schemaAPI.IndexInfo(context.Background(), i.tableName)
		if err != nil {
			if errors.Is(err, pilosa.ErrIndexNotFound) {
				return nil, sql3.NewErrInternalf("table not found '%s'", i.tableName)
			}
			return nil, err
		}
		i.rowWidth = len(table.Fields)

		i.targetColumnMap = make(map[string]int)
		for idx, fld := range table.Fields {
			i.targetColumnMap[fld.Name] = idx
		}

		var cond *pql.Call

		cond, err = i.planner.generatePQLCallFromExpr(ctx, i.predicate)
		if err != nil {
			return nil, err
		}
		if cond == nil {
			cond = &pql.Call{Name: "All"}
		}

		if i.topExpr != nil {
			_, ok := i.topExpr.(*intLiteralPlanExpression)
			if !ok {
				return nil, sql3.NewErrInternalf("unexpected top expression type: %T", i.topExpr)
			}
			pqlValue, err := planExprToValue(i.topExpr)
			if err != nil {
				return nil, err
			}
			cond = &pql.Call{
				Name:     "Limit",
				Children: []*pql.Call{cond},
				Args:     map[string]interface{}{"limit": pqlValue},
				Type:     pql.PrecallGlobal,
			}
		}

		call := &pql.Call{Name: "Extract", Children: []*pql.Call{cond}}
		for _, c := range i.columns {
			col, ok := c.(types.SchemaIdentifiable)
			if !ok {
				return nil, sql3.NewErrInternalf("unexpected column type '%T'", c)
			}

			// Skip the _id field.
			if col.Name() == "_id" {
				continue
			}
			call.Children = append(call.Children,
				&pql.Call{
					Name: "Rows",
					Args: map[string]interface{}{"field": col.Name()},
				},
			)
		}

		queryResponse, err := i.planner.executor.Execute(ctx, i.tableName, &pql.Query{Calls: []*pql.Call{call}}, nil, nil)
		if err != nil {
			return nil, err
		}
		tbl, ok := queryResponse.Results[0].(pilosa.ExtractedTable)
		if !ok {
			return nil, sql3.NewErrInternalf("unexpected Extract() result type: %T", queryResponse.Results[0])
		}
		i.result = tbl.Columns
		i.sourceColumnMap = make(map[string]int)
		for idx, fld := range tbl.Fields {
			i.sourceColumnMap[fld.Name] = idx
		}
	}

	if len(i.result) > 0 {
		row := make([]interface{}, i.rowWidth)

		for _, c := range i.columns {
			result := i.result[0]

			col, ok := c.(types.SchemaIdentifiable)
			if !ok {
				return nil, sql3.NewErrInternalf("unexpected column type '%T'", c)
			}

			targetColIdx, ok := i.targetColumnMap[col.Name()]
			if !ok {
				return nil, sql3.NewErrInternalf("target index not found for column named %s", col.Name())
			}

			if col.Name() == "_id" {
				if result.Column.Keyed {
					row[targetColIdx] = result.Column.Key
				} else {
					row[targetColIdx] = int64(result.Column.ID)
				}
			} else {

				sourceColIdx, ok := i.sourceColumnMap[col.Name()]
				if !ok {
					return nil, sql3.NewErrInternalf("source index not found for column named %s", col.Name())
				}
				switch c.Type().(type) {
				case *parser.DataTypeIDSet:
					//empty sets are null
					val, ok := result.Rows[sourceColIdx].([]uint64)
					if !ok {
						return nil, sql3.NewErrInternalf("unexpected type for column value '%T'", result.Rows[sourceColIdx])
					}
					if len(val) == 0 {
						row[targetColIdx] = nil
					} else {
						row[targetColIdx] = val
					}

				case *parser.DataTypeStringSet:
					//empty sets are null
					val, ok := result.Rows[sourceColIdx].([]string)
					if !ok {
						return nil, sql3.NewErrInternalf("unexpected type for column value '%T'", result.Rows[sourceColIdx])
					}
					if len(val) == 0 {
						row[targetColIdx] = nil
					} else {
						row[targetColIdx] = val
					}

				default:
					row[targetColIdx] = result.Rows[sourceColIdx]
				}
			}
		}

		// Move to next result element.
		i.result = i.result[1:]
		return row, nil
	}
	return nil, types.ErrNoMoreRows
}
