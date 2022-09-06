// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"fmt"

	"github.com/molecula/featurebase/v3/sql3"
	"github.com/molecula/featurebase/v3/sql3/parser"
	"github.com/molecula/featurebase/v3/sql3/planner/types"
)

// PlanOpTableValuedFunction is an operator for a subquery
type PlanOpTableValuedFunction struct {
	planner  *ExecutionPlanner
	callExpr types.PlanExpression
	warnings []string
}

func NewPlanOpTableValuedFunction(p *ExecutionPlanner, callExpr types.PlanExpression) *PlanOpTableValuedFunction {
	return &PlanOpTableValuedFunction{
		planner:  p,
		callExpr: callExpr,
		warnings: make([]string, 0),
	}
}

func (p *PlanOpTableValuedFunction) Schema() types.Schema {
	result := make(types.Schema, 0)
	tvfResultType, ok := p.callExpr.Type().(*parser.DataTypeSubtable)
	if !ok {
		return result
	}
	for _, member := range tvfResultType.Columns {
		result = append(result, &types.PlannerColumn{
			Name:  member.Name,
			Table: "",
			Type:  member.DataType,
		})
	}
	return result
}

func (p *PlanOpTableValuedFunction) Iterator(ctx context.Context, row types.Row) (types.RowIterator, error) {
	return nil, sql3.NewErrInternalf("table valued functions are not yet implemented")
}

func (p *PlanOpTableValuedFunction) Children() []types.PlanOperator {
	return []types.PlanOperator{}
}

func (p *PlanOpTableValuedFunction) WithChildren(children ...types.PlanOperator) (types.PlanOperator, error) {
	return nil, nil
}

func (p *PlanOpTableValuedFunction) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_op"] = fmt.Sprintf("%T", p)
	sc := make([]string, 0)
	for _, e := range p.Schema() {
		sc = append(sc, fmt.Sprintf("'%s', '%s', '%s'", e.Name, e.Table, e.Type.TypeName()))
	}
	result["_schema"] = sc

	return result
}

func (p *PlanOpTableValuedFunction) String() string {
	return ""
}

func (p *PlanOpTableValuedFunction) AddWarning(warning string) {
	p.warnings = append(p.warnings, warning)
}

func (p *PlanOpTableValuedFunction) Warnings() []string {
	var w []string
	w = append(w, p.warnings...)
	return w

}
