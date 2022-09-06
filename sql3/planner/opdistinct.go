// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import (
	"fmt"

	"github.com/molecula/featurebase/v3/sql3/planner/types"
)

// PlanOpDistinct plan operator handles DISTINCT
type PlanOpDistinct struct {
	planner  *ExecutionPlanner
	source   types.PlanOperator
	warnings []string
}

func NewPlanOpDistinct(p *ExecutionPlanner, source types.PlanOperator) *PlanOpDistinct {
	return &PlanOpDistinct{
		planner:  p,
		source:   source,
		warnings: make([]string, 0),
	}
}

func (n *PlanOpDistinct) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_op"] = fmt.Sprintf("%T", n)
	return result
}

func (n *PlanOpDistinct) AddWarning(warning string) {
	n.warnings = append(n.warnings, warning)
}

func (n *PlanOpDistinct) Warnings() []string {
	var w []string
	w = append(w, n.warnings...)
	w = append(w, n.source.Warnings()...)
	return w
}
