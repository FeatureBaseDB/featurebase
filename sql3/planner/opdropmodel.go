// Copyright 2023 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"fmt"

	"github.com/featurebasedb/featurebase/v3/sql3"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
)

// PlanOpDropModel plan operator to drop a view.
type PlanOpDropModel struct {
	planner   *ExecutionPlanner
	modelName string
	ifExists  bool
	warnings  []string
}

func NewPlanOpDropModel(p *ExecutionPlanner, ifExists bool, modelName string) *PlanOpDropModel {
	return &PlanOpDropModel{
		planner:   p,
		modelName: modelName,
		ifExists:  ifExists,
		warnings:  make([]string, 0),
	}
}

func (p *PlanOpDropModel) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_op"] = fmt.Sprintf("%T", p)
	result["modelName"] = p.modelName
	result["isExists"] = p.ifExists
	return result
}

func (p *PlanOpDropModel) String() string {
	return ""
}

func (p *PlanOpDropModel) AddWarning(warning string) {
	p.warnings = append(p.warnings, warning)
}

func (p *PlanOpDropModel) Warnings() []string {
	return p.warnings
}

func (p *PlanOpDropModel) Schema() types.Schema {
	return types.Schema{}
}

func (p *PlanOpDropModel) Children() []types.PlanOperator {
	return []types.PlanOperator{}
}

func (p *PlanOpDropModel) Iterator(ctx context.Context, row types.Row) (types.RowIterator, error) {
	return &dropModelRowIter{
		planner:   p.planner,
		ifExists:  p.ifExists,
		modelName: p.modelName,
	}, nil
}

func (p *PlanOpDropModel) WithChildren(children ...types.PlanOperator) (types.PlanOperator, error) {
	return nil, nil
}

type dropModelRowIter struct {
	planner   *ExecutionPlanner
	ifExists  bool
	modelName string
}

var _ types.RowIterator = (*dropModelRowIter)(nil)

func (i *dropModelRowIter) Next(ctx context.Context) (types.Row, error) {
	err := i.planner.checkAccess(ctx, i.modelName, accessTypeDropObject)
	if err != nil {
		return nil, err
	}

	// check in the models table to see if it exists
	v, err := i.planner.getModelByName(i.modelName)
	if err != nil {
		return nil, err
	}
	if v == nil {
		if i.ifExists {
			return nil, types.ErrNoMoreRows
		}
		return nil, sql3.NewErrModelNotFound(0, 0, i.modelName)
	}

	err = i.planner.deleteModel(i.modelName)
	if err != nil {
		return nil, err
	}

	return nil, types.ErrNoMoreRows
}
