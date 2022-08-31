// Copyright 2021 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"fmt"

	"github.com/molecula/featurebase/v3/sql3/planner/types"
)

// PlanOpBulkInsert plan operator to handle INSERT.
type PlanOpBulkInsert struct {
	planner   *ExecutionPlanner
	tableName string
	warnings  []string
}

func NewPlanOpBulkInsert(p *ExecutionPlanner, tableName string) *PlanOpBulkInsert {
	return &PlanOpBulkInsert{
		planner:   p,
		tableName: tableName,
	}
}

func (p *PlanOpBulkInsert) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_op"] = fmt.Sprintf("%T", p)
	sc := make([]string, 0)
	for _, e := range p.Schema() {
		sc = append(sc, fmt.Sprintf("'%s', '%s', '%s'", e.Name, e.Table, e.Type.TypeName()))
	}
	result["_schema"] = sc
	result["tableName"] = p.tableName
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
	return &bulkInsertRowIter{
		planner:   p.planner,
		tableName: p.tableName,
	}, nil
}

func (p *PlanOpBulkInsert) WithChildren(children ...types.PlanOperator) (types.PlanOperator, error) {
	return NewPlanOpBulkInsert(p.planner, p.tableName), nil
}

type bulkInsertRowIter struct {
	planner   *ExecutionPlanner
	tableName string
}

var _ types.RowIterator = (*bulkInsertRowIter)(nil)

func (i *bulkInsertRowIter) Next(ctx context.Context) (types.Row, error) {

	return nil, types.ErrNoMoreRows
}
