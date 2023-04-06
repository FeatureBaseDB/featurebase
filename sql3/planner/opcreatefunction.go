// Copyright 2023 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"fmt"

	"github.com/featurebasedb/featurebase/v3/sql3"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
)

// PlanOpCreateFunction implements the CREATE FUNCTION operator
type PlanOpCreateFunction struct {
	planner     *ExecutionPlanner
	function    *functionSystemObject
	ifNotExists bool
	warnings    []string
}

func NewPlanOpCreateFunction(planner *ExecutionPlanner, ifNotExists bool, function *functionSystemObject) *PlanOpCreateFunction {
	return &PlanOpCreateFunction{
		planner:     planner,
		function:    function,
		ifNotExists: ifNotExists,
		warnings:    make([]string, 0),
	}
}

func (p *PlanOpCreateFunction) Schema() types.Schema {
	return types.Schema{}
}

func (p *PlanOpCreateFunction) Iterator(ctx context.Context, row types.Row) (types.RowIterator, error) {
	return newCreateFunctionIter(p.planner, p.ifNotExists, p.function), nil
}

func (p *PlanOpCreateFunction) Children() []types.PlanOperator {
	return []types.PlanOperator{}
}

func (p *PlanOpCreateFunction) WithChildren(children ...types.PlanOperator) (types.PlanOperator, error) {
	if len(children) != 0 {
		return nil, sql3.NewErrInternalf("unexpected number of children '%d'", len(children))
	}
	return NewPlanOpCreateFunction(p.planner, p.ifNotExists, p.function), nil
}

func (p *PlanOpCreateFunction) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_op"] = fmt.Sprintf("%T", p)
	result["_schema"] = p.Schema().Plan()
	result["model"] = p.function.name
	return result
}

func (p *PlanOpCreateFunction) String() string {
	return ""
}

func (p *PlanOpCreateFunction) AddWarning(warning string) {
	p.warnings = append(p.warnings, warning)
}

func (p *PlanOpCreateFunction) Warnings() []string {
	var w []string
	w = append(w, p.warnings...)
	return w
}

type createFunctionIter struct {
	planner     *ExecutionPlanner
	function    *functionSystemObject
	ifNotExists bool
}

func newCreateFunctionIter(planner *ExecutionPlanner, ifNotExists bool, function *functionSystemObject) *createFunctionIter {
	return &createFunctionIter{
		planner:     planner,
		function:    function,
		ifNotExists: ifNotExists,
	}
}

func (i *createFunctionIter) Next(ctx context.Context) (types.Row, error) {
	// now check in the functions table to see if it is exists
	v, err := i.planner.getFunctionByName(i.function.name)
	if err != nil {
		return nil, err
	}
	if v != nil {
		if i.ifNotExists {
			return nil, types.ErrNoMoreRows
		}
		return nil, sql3.NewErrViewExists(0, 0, i.function.name)
	}

	// now store the view into fb_functions
	err = i.planner.insertFunction(i.function)
	if err != nil {
		return nil, err
	}
	return nil, types.ErrNoMoreRows
}
