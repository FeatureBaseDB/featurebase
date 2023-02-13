// Copyright 2023 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"fmt"

	"github.com/featurebasedb/featurebase/v3/sql3"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
)

// PlanOpDropView plan operator to drop a view.
type PlanOpDropView struct {
	planner  *ExecutionPlanner
	viewName string
	ifExists bool
	warnings []string
}

func NewPlanOpDropView(p *ExecutionPlanner, ifExists bool, viewName string) *PlanOpDropView {
	return &PlanOpDropView{
		planner:  p,
		viewName: viewName,
		ifExists: ifExists,
		warnings: make([]string, 0),
	}
}

func (p *PlanOpDropView) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_op"] = fmt.Sprintf("%T", p)
	result["viewName"] = p.viewName
	result["isExists"] = p.ifExists
	return result
}

func (p *PlanOpDropView) String() string {
	return ""
}

func (p *PlanOpDropView) AddWarning(warning string) {
	p.warnings = append(p.warnings, warning)
}

func (p *PlanOpDropView) Warnings() []string {
	return p.warnings
}

func (p *PlanOpDropView) Schema() types.Schema {
	return types.Schema{}
}

func (p *PlanOpDropView) Children() []types.PlanOperator {
	return []types.PlanOperator{}
}

func (p *PlanOpDropView) Iterator(ctx context.Context, row types.Row) (types.RowIterator, error) {
	return &dropViewRowIter{
		planner:  p.planner,
		ifExists: p.ifExists,
		viewName: p.viewName,
	}, nil
}

func (p *PlanOpDropView) WithChildren(children ...types.PlanOperator) (types.PlanOperator, error) {
	return nil, nil
}

type dropViewRowIter struct {
	planner  *ExecutionPlanner
	ifExists bool
	viewName string
}

var _ types.RowIterator = (*dropViewRowIter)(nil)

func (i *dropViewRowIter) Next(ctx context.Context) (types.Row, error) {
	err := i.planner.checkAccess(ctx, i.viewName, accessTypeDropObject)
	if err != nil {
		return nil, err
	}

	// check in the views table to see if it exists
	v, err := i.planner.getViewByName(ctx, i.viewName)
	if err != nil {
		return nil, err
	}
	if v == nil {
		if i.ifExists {
			return nil, types.ErrNoMoreRows
		}
		return nil, sql3.NewErrViewNotFound(0, 0, i.viewName)
	}

	err = i.planner.deleteView(ctx, i.viewName)
	if err != nil {
		return nil, err
	}

	return nil, types.ErrNoMoreRows
}
