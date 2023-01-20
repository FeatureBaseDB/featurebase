// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"fmt"

	"github.com/featurebasedb/featurebase/v3/sql3"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
)

// PlanOpNestedLoops plan operator handles a join
// For each row in the top input, scan the bottom input and output matching rows
type PlanOpNestedLoops struct {
	top      types.PlanOperator
	bottom   types.PlanOperator
	cond     types.PlanExpression
	jType    joinType
	warnings []string
}

func NewPlanOpNestedLoops(top, bottom types.PlanOperator, jType joinType, condition types.PlanExpression) *PlanOpNestedLoops {
	return &PlanOpNestedLoops{
		top:      top,
		bottom:   bottom,
		cond:     condition,
		jType:    jType,
		warnings: make([]string, 0),
	}
}

func (p *PlanOpNestedLoops) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_op"] = fmt.Sprintf("%T", p)
	result["_schema"] = p.Schema().Plan()
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
	return newNestedLoopsIter(ctx, p.jType, topIter, p.bottom, row, p.cond, rowWidth, row), nil
}

func (p *PlanOpNestedLoops) WithChildren(children ...types.PlanOperator) (types.PlanOperator, error) {
	if len(children) != 2 {
		return nil, sql3.NewErrInternalf("unexpected number of children '%d'", len(children))
	}
	return NewPlanOpNestedLoops(children[0], children[1], p.jType, p.cond), nil
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
	joinTypeInner joinType = iota // records that have matching values in both tables
	joinTypeLeft                  // all records from the left table, and the matched records from the right table
	joinTypeRight                 // all records from the right table, and the matched records from the left table
	joinTypeFull                  // all records when there is a match in either left or right table
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

func (i *nestedLoopsIter) buildRow(primary, secondary types.Row) (types.Row, error) {
	row := make(types.Row, i.rowSize)

	primary = primary[len(i.originalRow):]

	var first, second types.Row
	var secondOffset int
	switch i.typ {
	case joinTypeLeft:
		first = primary
		second = secondary
		secondOffset = len(first)

	case joinTypeInner:
		first = primary
		second = secondary
		secondOffset = len(first)

	default:
		return nil, sql3.NewErrInternalf("unsupported join type %d", i.typ)
	}

	copy(row, first)
	copy(row[secondOffset:], second)
	return row, nil
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
				// no more rows from secondary
				switch i.typ {
				case joinTypeInner:
					continue

				case joinTypeLeft:
					if !i.foundMatch {
						row, err := i.buildRow(primary, nil)
						if err != nil {
							return nil, err
						}
						return row, nil
					}
					continue

				case joinTypeRight:
					return nil, sql3.NewErrInternalf("unhandled join type %v", i.typ)

				case joinTypeFull:
					return nil, sql3.NewErrInternalf("unhandled join type %v", i.typ)

				default:
					return nil, sql3.NewErrInternalf("unhandled join type %v", i.typ)
				}
			}
			return nil, err
		}

		row, err := i.buildRow(primary, secondary)
		if err != nil {
			return nil, err
		}
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
