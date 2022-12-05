// Copyright 2021 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"fmt"

	pilosa "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
)

// PlanOpDropTable plan operator to drop a table.
type PlanOpDropTable struct {
	planner  *ExecutionPlanner
	index    *pilosa.IndexInfo
	warnings []string
}

func NewPlanOpDropTable(p *ExecutionPlanner, index *pilosa.IndexInfo) *PlanOpDropTable {
	return &PlanOpDropTable{
		planner:  p,
		index:    index,
		warnings: make([]string, 0),
	}
}

func (p *PlanOpDropTable) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_op"] = fmt.Sprintf("%T", p)
	ps := make([]string, 0)
	for _, e := range p.Schema() {
		ps = append(ps, fmt.Sprintf("'%s', '%s', '%s'", e.ColumnName, e.RelationName, e.Type.TypeDescription()))
	}
	result["_schema"] = ps
	result["tableName"] = p.index.Name
	return result
}

func (p *PlanOpDropTable) String() string {
	return ""
}

func (p *PlanOpDropTable) AddWarning(warning string) {
	p.warnings = append(p.warnings, warning)
}

func (p *PlanOpDropTable) Warnings() []string {
	return p.warnings
}

func (p *PlanOpDropTable) Schema() types.Schema {
	return types.Schema{}
}

func (p *PlanOpDropTable) Children() []types.PlanOperator {
	return []types.PlanOperator{}
}

func (p *PlanOpDropTable) Iterator(ctx context.Context, row types.Row) (types.RowIterator, error) {
	return &dropTableRowIter{
		planner: p.planner,
		index:   p.index,
	}, nil
}

func (p *PlanOpDropTable) WithChildren(children ...types.PlanOperator) (types.PlanOperator, error) {
	return nil, nil
}

type dropTableRowIter struct {
	planner *ExecutionPlanner
	index   *pilosa.IndexInfo
}

var _ types.RowIterator = (*dropTableRowIter)(nil)

func (i *dropTableRowIter) Next(ctx context.Context) (types.Row, error) {
	err := i.planner.checkAccess(ctx, i.index.Name, accessTypeDropObject)
	if err != nil {
		return nil, err
	}

	err = i.planner.schemaAPI.DeleteIndex(ctx, i.index.Name)
	if err != nil {
		return nil, err
	}
	return nil, types.ErrNoMoreRows
}
