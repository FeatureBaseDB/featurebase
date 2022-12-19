// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"fmt"

	"github.com/molecula/featurebase/v3/dax"
	"github.com/molecula/featurebase/v3/pql"
	"github.com/molecula/featurebase/v3/sql3"
	"github.com/molecula/featurebase/v3/sql3/planner/types"
)

// TODO (pok) we should look at a drop and recreate, or an actual truncate PQL op

// PlanOpPQLTruncateTable plan operator to delete rows from a table based on a single key.
type PlanOpPQLTruncateTable struct {
	planner   *ExecutionPlanner
	tableName string
	warnings  []string
}

func NewPlanOpPQLTruncateTable(p *ExecutionPlanner, tableName string) *PlanOpPQLTruncateTable {
	return &PlanOpPQLTruncateTable{
		planner:   p,
		tableName: tableName,
		warnings:  make([]string, 0),
	}
}

func (p *PlanOpPQLTruncateTable) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_op"] = fmt.Sprintf("%T", p)
	ps := make([]string, 0)
	for _, e := range p.Schema() {
		ps = append(ps, fmt.Sprintf("'%s', '%s', '%s'", e.ColumnName, e.RelationName, e.Type.TypeDescription()))
	}
	result["_schema"] = ps
	result["tableName"] = p.tableName
	return result
}

func (p *PlanOpPQLTruncateTable) String() string {
	return ""
}

func (p *PlanOpPQLTruncateTable) AddWarning(warning string) {
	p.warnings = append(p.warnings, warning)
}

func (p *PlanOpPQLTruncateTable) Warnings() []string {
	return p.warnings
}

func (p *PlanOpPQLTruncateTable) Schema() types.Schema {
	return types.Schema{}
}

func (p *PlanOpPQLTruncateTable) Children() []types.PlanOperator {
	return []types.PlanOperator{}
}

func (p *PlanOpPQLTruncateTable) Iterator(ctx context.Context, row types.Row) (types.RowIterator, error) {
	return &truncateTableRowIter{
		planner:   p.planner,
		tableName: p.tableName,
	}, nil
}

func (p *PlanOpPQLTruncateTable) WithChildren(children ...types.PlanOperator) (types.PlanOperator, error) {
	return p, nil
}

type truncateTableRowIter struct {
	planner   *ExecutionPlanner
	tableName string
}

var _ types.RowIterator = (*truncateTableRowIter)(nil)

func (i *truncateTableRowIter) Next(ctx context.Context) (types.Row, error) {
	err := i.planner.checkAccess(ctx, i.tableName, accessTypeWriteData)
	if err != nil {
		return nil, err
	}

	cond := &pql.Call{
		Name: "All",
	}
	call := &pql.Call{Name: "Delete", Children: []*pql.Call{cond}}

	tbl, err := i.planner.schemaAPI.TableByName(ctx, dax.TableName(i.tableName))
	if err != nil {
		return nil, sql3.NewErrTableNotFound(0, 0, i.tableName)
	}

	_, err = i.planner.executor.Execute(ctx, tbl, &pql.Query{Calls: []*pql.Call{call}}, nil, nil)
	if err != nil {
		return nil, err
	}

	return nil, types.ErrNoMoreRows
}
