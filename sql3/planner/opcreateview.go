// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"fmt"

	"github.com/molecula/featurebase/v3/dax"
	"github.com/molecula/featurebase/v3/sql3"
	"github.com/molecula/featurebase/v3/sql3/planner/types"
)

// PlanOpCreateView implements the CREATE VIEW operator
type PlanOpCreateView struct {
	planner     *ExecutionPlanner
	view        *viewSystemObject
	ifNotExists bool
	warnings    []string
}

func NewPlanOpCreateView(planner *ExecutionPlanner, ifNotExists bool, view *viewSystemObject) *PlanOpCreateView {
	return &PlanOpCreateView{
		planner:     planner,
		view:        view,
		ifNotExists: ifNotExists,
		warnings:    make([]string, 0),
	}
}

func (p *PlanOpCreateView) Schema() types.Schema {
	return types.Schema{}
}

func (p *PlanOpCreateView) Iterator(ctx context.Context, row types.Row) (types.RowIterator, error) {
	return newCreateViewIter(p.planner, p.ifNotExists, p.view), nil
}

func (p *PlanOpCreateView) Children() []types.PlanOperator {
	return []types.PlanOperator{}
}

func (p *PlanOpCreateView) WithChildren(children ...types.PlanOperator) (types.PlanOperator, error) {
	if len(children) != 0 {
		return nil, sql3.NewErrInternalf("unexpected number of children '%d'", len(children))
	}
	return NewPlanOpCreateView(p.planner, p.ifNotExists, p.view), nil
}

func (p *PlanOpCreateView) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_op"] = fmt.Sprintf("%T", p)
	result["_schema"] = p.Schema().Plan()
	result["model"] = p.view.name
	return result
}

func (p *PlanOpCreateView) String() string {
	return ""
}

func (p *PlanOpCreateView) AddWarning(warning string) {
	p.warnings = append(p.warnings, warning)
}

func (p *PlanOpCreateView) Warnings() []string {
	var w []string
	w = append(w, p.warnings...)
	return w
}

type createViewIter struct {
	planner     *ExecutionPlanner
	view        *viewSystemObject
	ifNotExists bool
}

func newCreateViewIter(planner *ExecutionPlanner, ifNotExists bool, view *viewSystemObject) *createViewIter {
	return &createViewIter{
		planner:     planner,
		view:        view,
		ifNotExists: ifNotExists,
	}
}

func (i *createViewIter) Next(ctx context.Context) (types.Row, error) {
	// make sure we have no existing table named the same as our view
	viewName := dax.TableName(i.view.name)
	tbl, err := i.planner.schemaAPI.TableByName(context.Background(), viewName)
	if err != nil {
		if !isTableNotFoundError(err) {
			return nil, err
		}
	}
	if tbl != nil {
		if i.ifNotExists {
			return nil, types.ErrNoMoreRows
		}
		return nil, sql3.NewErrViewExists(0, 0, i.view.name)
	}

	// now check in the views table to see if it is exists
	v, err := i.planner.getViewByName(i.view.name)
	if err != nil {
		return nil, err
	}
	if v != nil {
		if i.ifNotExists {
			return nil, types.ErrNoMoreRows
		}
		return nil, sql3.NewErrViewExists(0, 0, i.view.name)
	}

	// now store the view into fb_views
	err = i.planner.insertView(i.view)
	if err != nil {
		return nil, err
	}
	return nil, types.ErrNoMoreRows
}
