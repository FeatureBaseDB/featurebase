// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"fmt"

	"github.com/molecula/featurebase/v3/sql3"
	"github.com/molecula/featurebase/v3/sql3/planner/types"
)

// PlanOpNestedLoops plan operator handles a join
// For each row in the top input, scan the bottom input and output matching rows
type PlanOpNestedLoops struct {
	top      types.PlanOperator
	bottom   types.PlanOperator
	cond     types.PlanExpression
	warnings []string
}

func NewPlanOpNestedLoops(top, bottom types.PlanOperator, condition types.PlanExpression) *PlanOpNestedLoops {
	return &PlanOpNestedLoops{
		top:      top,
		bottom:   bottom,
		cond:     condition,
		warnings: make([]string, 0),
	}
}

func (p *PlanOpNestedLoops) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_op"] = fmt.Sprintf("%T", p)
	ps := make([]string, 0)
	for _, e := range p.Schema() {
		ps = append(ps, fmt.Sprintf("'%s', '%s', '%s'", e.ColumnName, e.RelationName, e.Type.TypeDescription()))
	}
	result["_schema"] = ps
	result["top"] = p.top.Plan()
	result["bottom"] = p.bottom.Plan()
	result["condition"] = p.cond.Plan()
	return result
}

func (p *PlanOpNestedLoops) String() string {
	return ""
}

func (p *PlanOpNestedLoops) AddWarning(warning string) {
	p.warnings = append(p.warnings, warning)
}

func (p *PlanOpNestedLoops) Warnings() []string {
	return p.warnings
}

func (p *PlanOpNestedLoops) Schema() types.Schema {
	result := types.Schema{}
	result = append(result, p.top.Schema()...)
	result = append(result, p.bottom.Schema()...)
	return result
}

func (p *PlanOpNestedLoops) Children() []types.PlanOperator {
	return []types.PlanOperator{
		p.top,
		p.bottom,
	}
}

func (p *PlanOpNestedLoops) Iterator(ctx context.Context, row types.Row) (types.RowIterator, error) {
	topIter, err := p.top.Iterator(ctx, row)
	if err != nil {
		return nil, err
	}

	rowWidth := len(row) + len(p.top.Schema()) + len(p.bottom.Schema())
	return newNestedLoopsIter(ctx, joinTypeInner, topIter, p.bottom, row, p.cond, rowWidth, row), nil
}

func (p *PlanOpNestedLoops) WithChildren(children ...types.PlanOperator) (types.PlanOperator, error) {
	if len(children) != 2 {
		return nil, sql3.NewErrInternalf("unexpected number of children '%d'", len(children))
	}
	return NewPlanOpNestedLoops(children[0], children[1], p.cond), nil
}

func (p *PlanOpNestedLoops) Expressions() []types.PlanExpression {
	if p.cond != nil {
		return []types.PlanExpression{
			p.cond,
		}
	}
	return []types.PlanExpression{}
}

func (p *PlanOpNestedLoops) WithUpdatedExpressions(exprs ...types.PlanExpression) (types.PlanOperator, error) {
	if len(exprs) != 1 {
		return nil, sql3.NewErrInternalf("unexpected number of exprs '%d'", len(exprs))
	}
	p.cond = exprs[0]
	return p, nil
}

type joinType byte

const (
	joinTypeInner joinType = iota
	joinTypeLeft
	joinTypeRight
)

type nestedLoopsIter struct {
	typ joinType

	top    types.RowIterator
	bottom types.RowIterator
	ctx    context.Context
	cond   types.PlanExpression

	bottomProvider types.RowIterable

	topRow     types.Row
	foundMatch bool
	rowSize    int

	originalRow types.Row
}

func newNestedLoopsIter(ctx context.Context, jt joinType, top types.RowIterator, bottom types.RowIterable, scopeRow types.Row, joinCondition types.PlanExpression, rowWidth int, originalRow types.Row) *nestedLoopsIter {
	return &nestedLoopsIter{
		typ:            jt,
		top:            top,
		bottomProvider: bottom,
		cond:           joinCondition,
		rowSize:        rowWidth,
		originalRow:    originalRow,
		ctx:            ctx,
	}
}

func (i *nestedLoopsIter) loadTop(ctx context.Context) error {
	if i.topRow != nil {
		return nil
	}

	r, err := i.top.Next(ctx)
	if err != nil {
		return err
	}
	i.topRow = i.originalRow.Append(r)
	i.foundMatch = false

	//DEBUG log.Printf("top row %v", i.topRow)

	return nil
}

func (i *nestedLoopsIter) loadBottom(ctx context.Context) (row types.Row, err error) {
	if i.bottom == nil {
		// DEBUG log.Printf("bottom row initializing iterator...")
		var iter types.RowIterator
		iter, err = i.bottomProvider.Iterator(ctx, i.topRow)
		if err != nil {
			return nil, err
		}

		i.bottom = iter
	}
	rightRow, err := i.bottom.Next(ctx)
	if err != nil {
		if err == types.ErrNoMoreRows {
			// DEBUG log.Printf("bottom end of rows")
			i.bottom = nil
			i.topRow = nil
			return nil, types.ErrNoMoreRows
		}
		return nil, err
	}

	//DEBUG log.Printf("bottom row %v", rightRow)
	return rightRow, nil
}

func (i *nestedLoopsIter) buildRow(primary, secondary types.Row) types.Row {
	row := make(types.Row, i.rowSize)

	primary = primary[len(i.originalRow):]

	var first, second types.Row
	var secondOffset int
	switch i.typ {
	case joinTypeRight:
		first = secondary
		second = primary
		secondOffset = len(row) - len(second)
	default:
		first = primary
		second = secondary
		secondOffset = len(first)
	}

	copy(row, first)
	copy(row[secondOffset:], second)
	return row
}

func conditionIsTrue(ctx context.Context, row types.Row, cond types.PlanExpression) (bool, error) {
	if cond == nil {
		return true, nil
	}
	v, err := cond.Evaluate(row)
	if err != nil {
		return false, err
	}
	return v == true, nil
}

func (i *nestedLoopsIter) Next(ctx context.Context) (types.Row, error) {
	for {
		if err := i.loadTop(ctx); err != nil {
			return nil, err
		}

		primary := i.topRow
		secondary, err := i.loadBottom(ctx)
		if err != nil {
			if err == types.ErrNoMoreRows {
				if !i.foundMatch && (i.typ == joinTypeLeft || i.typ == joinTypeRight) {
					row := i.buildRow(primary, nil)
					return row, nil
				}
				continue
			}
			return nil, err
		}

		row := i.buildRow(primary, secondary)
		matches, err := conditionIsTrue(ctx, row, i.cond)
		if err != nil {
			return nil, err
		}

		if !matches {
			continue
		}

		i.foundMatch = true

		// DEBUG log.Printf("join result %v", row)

		return row, nil
	}
}
