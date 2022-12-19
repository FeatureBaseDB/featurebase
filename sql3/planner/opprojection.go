// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"fmt"

	"github.com/featurebasedb/featurebase/v3/sql3"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
)

// PlanOpProjection handles row projection and expression evaluation
type PlanOpProjection struct {
	ChildOp     types.PlanOperator
	Projections []types.PlanExpression
	warnings    []string
}

func NewPlanOpProjection(expressions []types.PlanExpression, child types.PlanOperator) *PlanOpProjection {
	return &PlanOpProjection{
		ChildOp:     child,
		Projections: expressions,
		warnings:    make([]string, 0),
	}
}

func (p *PlanOpProjection) Schema() types.Schema {
	var s = make(types.Schema, len(p.Projections))
	for i, e := range p.Projections {
		s[i] = ExpressionToColumn(e)
	}
	return s
}

func (p *PlanOpProjection) Iterator(ctx context.Context, row types.Row) (types.RowIterator, error) {
	i, err := p.ChildOp.Iterator(ctx, row)
	if err != nil {
		return nil, err
	}
	return &iter{
		p:         p,
		childIter: i,
		row:       row,
	}, nil
}

func (p *PlanOpProjection) Children() []types.PlanOperator {
	return []types.PlanOperator{
		p.ChildOp,
	}
}

func (p *PlanOpProjection) WithChildren(children ...types.PlanOperator) (types.PlanOperator, error) {
	if len(children) != 1 {
		return nil, sql3.NewErrInternalf("unexpected number of children '%d'", len(children))
	}
	return NewPlanOpProjection(p.Projections, children[0]), nil
}

func (p *PlanOpProjection) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["__op"] = fmt.Sprintf("%T", p)
	sc := make([]string, 0)
	for _, e := range p.Schema() {
		sc = append(sc, fmt.Sprintf("'%s', '%s', '%s'", e.ColumnName, e.RelationName, e.Type.TypeDescription()))
	}
	result["__schema"] = sc

	result["child"] = p.ChildOp.Plan()

	ps := make([]interface{}, 0)
	for _, e := range p.Projections {
		ps = append(ps, e.Plan())
	}
	result["_projections"] = ps

	return result
}

func (p *PlanOpProjection) String() string {
	return ""
}

func (p *PlanOpProjection) AddWarning(warning string) {
	p.warnings = append(p.warnings, warning)
}

func (p *PlanOpProjection) Warnings() []string {
	var w []string
	w = append(w, p.warnings...)
	if p.ChildOp != nil {
		w = append(w, p.ChildOp.Warnings()...)
	}
	return w
}

func (p *PlanOpProjection) Expressions() []types.PlanExpression {
	return p.Projections
}

func (p *PlanOpProjection) WithUpdatedExpressions(exprs ...types.PlanExpression) (types.PlanOperator, error) {
	if len(exprs) != len(p.Projections) {
		return nil, sql3.NewErrInternalf("unexpected number of exprs '%d'", len(exprs))
	}
	return NewPlanOpProjection(exprs, p.ChildOp), nil
}

func ExpressionToColumn(e types.PlanExpression) *types.PlannerColumn {
	name := ""
	relationName := ""

	switch thisExpr := e.(type) {
	case *qualifiedRefPlanExpression:
		name = thisExpr.columnName
		relationName = thisExpr.tableName

	case *aliasPlanExpression:
		name = thisExpr.aliasName
	}

	return &types.PlannerColumn{
		ColumnName:   name,
		RelationName: relationName,
		Type:         e.Type(),
	}
}

type iter struct {
	p         *PlanOpProjection
	childIter types.RowIterator
	row       types.Row
}

func (i *iter) Next(ctx context.Context) (types.Row, error) {
	childRow, err := i.childIter.Next(ctx)
	if err != nil {
		return nil, err
	}

	return ProjectRow(ctx, i.p.Projections, childRow)
}

// ProjectRow evaluates a set of projections.
func ProjectRow(ctx context.Context, projections []types.PlanExpression, row types.Row) (types.Row, error) {
	var fields types.Row
	for _, expr := range projections {
		f, fErr := expr.Evaluate(row)
		if fErr != nil {
			return nil, fErr
		}
		fields = append(fields, f)
	}
	return fields, nil
}
