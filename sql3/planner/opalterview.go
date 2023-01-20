// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"fmt"

	"github.com/featurebasedb/featurebase/v3/sql3"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
)

// PlanOpAlterView implements the ALTER VIEW operator
type PlanOpAlterView struct {
	planner  *ExecutionPlanner
	view     *viewSystemObject
	warnings []string
}

func NewPlanOpAlterView(planner *ExecutionPlanner, view *viewSystemObject) *PlanOpAlterView {
	return &PlanOpAlterView{
		planner:  planner,
		view:     view,
		warnings: make([]string, 0),
	}
}

func (p *PlanOpAlterView) Schema() types.Schema {
	return types.Schema{}
}

func (p *PlanOpAlterView) Iterator(ctx context.Context, row types.Row) (types.RowIterator, error) {
	return newAlterViewIter(p.planner, p.view), nil
}

func (p *PlanOpAlterView) Children() []types.PlanOperator {
	return []types.PlanOperator{}
}

func (p *PlanOpAlterView) WithChildren(children ...types.PlanOperator) (types.PlanOperator, error) {
	if len(children) != 0 {
		return nil, sql3.NewErrInternalf("unexpected number of children '%d'", len(children))
	}
	return NewPlanOpAlterView(p.planner, p.view), nil
}

func (p *PlanOpAlterView) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_op"] = fmt.Sprintf("%T", p)
	result["_schema"] = p.Schema().Plan()
	result["model"] = p.view.name
	return result
}

func (p *PlanOpAlterView) String() string {
	return ""
}

func (p *PlanOpAlterView) AddWarning(warning string) {
	p.warnings = append(p.warnings, warning)
}

func (p *PlanOpAlterView) Warnings() []string {
	var w []string
	w = append(w, p.warnings...)
	return w
}

type alterViewIter struct {
	planner *ExecutionPlanner
	view    *viewSystemObject
}

func newAlterViewIter(planner *ExecutionPlanner, view *viewSystemObject) *alterViewIter {
	return &alterViewIter{
		planner: planner,
		view:    view,
	}
}

func (i *alterViewIter) Next(ctx context.Context) (types.Row, error) {
	// now check in the views table to see if it exists
	v, err := i.planner.getViewByName(i.view.name)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, sql3.NewErrViewNotFound(0, 0, i.view.name)
	}

	// now store the view into fb_views
	err = i.planner.updateView(i.view)
	if err != nil {
		return nil, err
	}
	return nil, types.ErrNoMoreRows
}
