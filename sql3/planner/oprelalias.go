// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"fmt"

	"github.com/featurebasedb/featurebase/v3/sql3"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
)

// PlanOpRelAlias implements an alias for a relation
type PlanOpRelAlias struct {
	ChildOp   types.PlanOperator
	aliasName string
	warnings  []string
}

func NewPlanOpRelAlias(alias string, child types.PlanOperator) *PlanOpRelAlias {
	return &PlanOpRelAlias{
		ChildOp:   child,
		aliasName: alias,
		warnings:  make([]string, 0),
	}
}

func (p *PlanOpRelAlias) Schema() types.Schema {
	schema := p.ChildOp.Schema()
	for _, s := range schema {
		s.AliasName = p.aliasName
	}
	return schema
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
	return NewPlanOpRelAlias(p.aliasName, children[0]), nil
}

func (p *PlanOpRelAlias) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_op"] = fmt.Sprintf("%T", p)
	result["_schema"] = p.Schema().Plan()
	result["aliasName"] = p.aliasName
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

// func (p *PlanOpRelAlias) Name() string {
// 	return p.aliasName
// }

// func (p *PlanOpRelAlias) IsFilterable() bool {
// 	ch, ok := p.ChildOp.(types.FilteredRelation)
// 	if !ok {
// 		return false
// 	}
// 	return ch.IsFilterable()
// }

// func (p *PlanOpRelAlias) UpdateFilters(filterCondition types.PlanExpression) (types.PlanOperator, error) {
// 	ch, ok := p.ChildOp.(types.FilteredRelation)
// 	if !ok {
// 		return nil, sql3.NewErrInternalf("childop is not filterable")
// 	}
// 	newChild, err := ch.UpdateFilters(filterCondition)
// 	if err != nil {
// 		return nil, err
// 	}
// 	p.ChildOp = newChild
// 	return p, nil
// }

// func (p *PlanOpRelAlias) UpdateTimeQuantumFilters(filters ...types.PlanExpression) (types.PlanOperator, error) {
// 	ch, ok := p.ChildOp.(types.FilteredRelation)
// 	if !ok {
// 		return nil, sql3.NewErrInternalf("childop is not filterable")
// 	}
// 	newChild, err := ch.UpdateTimeQuantumFilters(filters...)
// 	if err != nil {
// 		return nil, err
// 	}
// 	p.ChildOp = newChild
// 	return p, nil
// }
