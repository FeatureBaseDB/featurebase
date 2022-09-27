// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"fmt"

	"github.com/molecula/featurebase/v3/sql3/planner/types"
)

// PlanOpNullTable is an operator for a null table
// basically when you do select 1, you're using the null table
type PlanOpNullTable struct {
	warnings []string
}

func NewPlanOpNullTable() *PlanOpNullTable {
	return &PlanOpNullTable{
		warnings: make([]string, 0),
	}
}

func (p *PlanOpNullTable) Schema() types.Schema {
	return types.Schema{}
}

func (p *PlanOpNullTable) Iterator(ctx context.Context, row types.Row) (types.RowIterator, error) {
	return &nullTableIterator{}, nil
}

func (p *PlanOpNullTable) WithChildren(children ...types.PlanOperator) (types.PlanOperator, error) {
	return NewPlanOpNullTable(), nil
}

func (p *PlanOpNullTable) Children() []types.PlanOperator {
	return []types.PlanOperator{}
}

func (p *PlanOpNullTable) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_op"] = fmt.Sprintf("%T", p)
	ps := make([]string, 0)
	for _, e := range p.Schema() {
		ps = append(ps, fmt.Sprintf("'%s', '%s', '%s'", e.ColumnName, e.RelationName, e.Type.TypeName()))
	}
	result["_schema"] = ps
	return result
}

func (p *PlanOpNullTable) String() string {
	return ""
}

func (p *PlanOpNullTable) AddWarning(warning string) {
	p.warnings = append(p.warnings, warning)
}

func (p *PlanOpNullTable) Warnings() []string {
	return p.warnings
}

type nullTableIterator struct {
	rowConsumed bool
}

func (i *nullTableIterator) Next(ctx context.Context) (types.Row, error) {
	if !i.rowConsumed {
		i.rowConsumed = true
		return make([]interface{}, 0), nil
	}
	return nil, types.ErrNoMoreRows
}
