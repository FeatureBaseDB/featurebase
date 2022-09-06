// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"fmt"

	"github.com/molecula/featurebase/v3/sql3"
	"github.com/molecula/featurebase/v3/sql3/planner/types"
)

// PlanOpRelAlias implements an alias for a relation
type PlanOpRelAlias struct {
	ChildOp  types.PlanOperator
	alias    string
	warnings []string
}

func NewPlanOpRelAlias(alias string, child types.PlanOperator) *PlanOpRelAlias {
	return &PlanOpRelAlias{
		ChildOp:  child,
		alias:    alias,
		warnings: make([]string, 0),
	}
}

func (p *PlanOpRelAlias) Schema() types.Schema {
	return p.ChildOp.Schema()
}

func (p *PlanOpRelAlias) Iterator(ctx context.Context, row types.Row) (types.RowIterator, error) {
	return p.ChildOp.Iterator(ctx, row)
}

func (p *PlanOpRelAlias) Children() []types.PlanOperator {
	return []types.PlanOperator{
		p.ChildOp,
	}
}

func (p *PlanOpRelAlias) WithChildren(children ...types.PlanOperator) (types.PlanOperator, error) {
	if len(children) != 1 {
		return nil, sql3.NewErrInternalf("unexpected number of children '%d'", len(children))
	}
	return NewPlanOpRelAlias(p.alias, children[0]), nil
}

func (p *PlanOpRelAlias) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_op"] = fmt.Sprintf("%T", p)
	sc := make([]string, 0)
	for _, e := range p.Schema() {
		sc = append(sc, fmt.Sprintf("'%s', '%s', '%s'", e.Name, e.Table, e.Type.TypeName()))
	}
	result["_schema"] = sc

	result["alias"] = p.alias
	result["child"] = p.ChildOp.Plan()
	return result
}

func (p *PlanOpRelAlias) String() string {
	return ""
}

func (p *PlanOpRelAlias) AddWarning(warning string) {
	p.warnings = append(p.warnings, warning)
}

func (p *PlanOpRelAlias) Warnings() []string {
	var w []string
	w = append(w, p.warnings...)
	w = append(w, p.ChildOp.Warnings()...)
	return w
}

func (p *PlanOpRelAlias) Name() string {
	return p.alias
}
