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

// PlanOpPQLFilteredDelete plan operator to delete rows from a table based on a filter expression.
type PlanOpPQLFilteredDelete struct {
	planner   *ExecutionPlanner
	tableName string
	filter    types.PlanExpression
	warnings  []string
}

func NewPlanOpPQLFilteredDelete(p *ExecutionPlanner, tableName string, filter types.PlanExpression) *PlanOpPQLFilteredDelete {
	return &PlanOpPQLFilteredDelete{
		planner:   p,
		tableName: tableName,
		filter:    filter,
		warnings:  make([]string, 0),
	}
}

func (p *PlanOpPQLFilteredDelete) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_op"] = fmt.Sprintf("%T", p)
	result["_schema"] = p.Schema().Plan()
	result["filter"] = p.filter.Plan()
	result["tableName"] = p.tableName
	return result
}

func (p *PlanOpPQLFilteredDelete) String() string {
	return ""
}

func (p *PlanOpPQLFilteredDelete) AddWarning(warning string) {
	p.warnings = append(p.warnings, warning)
}

func (p *PlanOpPQLFilteredDelete) Warnings() []string {
	return p.warnings
}

func (p *PlanOpPQLFilteredDelete) Schema() types.Schema {
	return types.Schema{}
}

func (p *PlanOpPQLFilteredDelete) Children() []types.PlanOperator {
	return []types.PlanOperator{}
}

func (p *PlanOpPQLFilteredDelete) Iterator(ctx context.Context, row types.Row) (types.RowIterator, error) {
	return &filteredDeleteRowIter{
		planner:   p.planner,
		tableName: p.tableName,
		filter:    p.filter,
	}, nil
}

func (p *PlanOpPQLFilteredDelete) WithChildren(children ...types.PlanOperator) (types.PlanOperator, error) {
	if len(children) != 0 {
		return nil, sql3.NewErrInternalf("unexpected number of children '%d'", len(children))
	}
	return NewPlanOpPQLFilteredDelete(p.planner, p.tableName, p.filter), nil
}

type filteredDeleteRowIter struct {
	planner   *ExecutionPlanner
	tableName string
	filter    types.PlanExpression
}

var _ types.RowIterator = (*filteredDeleteRowIter)(nil)

func (i *filteredDeleteRowIter) Next(ctx context.Context) (types.Row, error) {
	var err error

	err = i.planner.checkAccess(ctx, i.tableName, accessTypeWriteData)
	if err != nil {
		return nil, err
	}

	cond, err := i.planner.generatePQLCallFromExpr(ctx, i.filter)
	if err != nil {
		return nil, err
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
