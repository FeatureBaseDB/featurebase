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
	warnings []string
}

func NewPlanOpNestedLoops(top, bottom types.PlanOperator) *PlanOpNestedLoops {
	return &PlanOpNestedLoops{
		top:    top,
		bottom: bottom,
	}
}

func (p *PlanOpNestedLoops) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_op"] = fmt.Sprintf("%T", p)
	ps := make([]string, 0)
	for _, e := range p.Schema() {
		ps = append(ps, fmt.Sprintf("'%s', '%s', '%s'", e.Name, e.Table, e.Type.TypeName()))
	}
	result["_schema"] = ps
	result["top"] = p.top.Plan()
	result["bottom"] = p.bottom.Plan()
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
	return newNestedLoopsIter(ctx, joinTypeInner, topIter, p.bottom, row, nil, rowWidth, row), nil
}

func (p *PlanOpNestedLoops) WithChildren(children ...types.PlanOperator) (types.PlanOperator, error) {
	if len(children) != 2 {
		return nil, sql3.NewErrInternalf("unexpected number of children '%d'", len(children))
	}
	return NewPlanOpNestedLoops(children[0], children[1]), nil
}

type joinType byte

const (
	joinTypeInner joinType = iota
	joinTypeLeft
	joinTypeRight
)

// joinMode defines the mode in which a join will be performed.
type joinMode byte

const (
	// unknownMode is the default mode. It will start iterating without really
	// knowing in which mode it will end up computing the join. If it
	// iterates the right side fully one time and so far it fits in memory,
	// then it will switch to memory mode. Otherwise, if at some point during
	// this first iteration it finds that it does not fit in memory, will
	// switch to multipass mode.
	unknownMode joinMode = iota
	// memoryMode computes all the join directly in memory iterating each
	// side of the join exactly once.
	//memoryMode
	// multipassMode computes the join by iterating the left side once,
	// and the right side one time for each row in the left side.
	multipassMode
)

type nestedLoopsIter struct {
	typ joinType

	top    types.RowIterator
	bottom types.RowIterator
	ctx    context.Context
	cond   types.PlanExpression

	secondaryProvider types.RowIterable

	primaryRow types.Row
	foundMatch bool
	rowSize    int

	originalRow types.Row
	scopeLen    int

	mode          joinMode
	secondaryRows RowCache
}

func newNestedLoopsIter(ctx context.Context, jt joinType, top types.RowIterator, bottom types.RowIterable, scopeRow types.Row, joinCondition types.PlanExpression, rowWidth int, originalRow types.Row) *nestedLoopsIter {
	return &nestedLoopsIter{
		typ:               jt,
		top:               top,
		secondaryProvider: bottom,
		cond:              joinCondition,
		rowSize:           rowWidth,
		originalRow:       originalRow,
		secondaryRows:     newInMemoryRowCache(),
		ctx:               ctx,
	}
}

func (i *nestedLoopsIter) loadPrimary(ctx context.Context) error {
	// If primary has already been loaded, it's safe to no-op.
	if i.primaryRow != nil {
		return nil
	}

	r, err := i.top.Next(ctx)
	if err != nil {
		return err
	}
	i.primaryRow = i.originalRow.Append(r)
	i.foundMatch = false

	return nil
}

/*func (i *nestedLoopsIter) loadSecondaryInMemory(ctx context.Context) error {
	iter, err := i.secondaryProvider.Iterator(ctx, i.primaryRow)
	if err != nil {
		return err
	}

	for {
		row, err := iter.Next(ctx)
		if err == types.ErrNoMoreRows {
			break
		}
		if err != nil {
			//iter.Close(ctx)
			return err
		}

		if err := i.secondaryRows.Add(row); err != nil {
			//iter.Close(ctx)
			return err
		}
	}

	//err = iter.Close(ctx)
	//if err != nil {
	//	return err
	//}

	if len(i.secondaryRows.Get()) == 0 {
		return types.ErrNoMoreRows
	}

	return nil
}*/

func (i *nestedLoopsIter) loadSecondary(ctx context.Context) (row types.Row, err error) {
	/*if i.mode == memoryMode {
		if len(i.secondaryRows.Get()) == 0 {
			if err = i.loadSecondaryInMemory(ctx); err != nil {
				if err == types.ErrNoMoreRows {
					i.primaryRow = nil
					i.pos = 0
				}
				return nil, err
			}
		}

		if i.pos >= len(i.secondaryRows.Get()) {
			i.primaryRow = nil
			i.pos = 0
			return nil, types.ErrNoMoreRows
		}

		row := i.secondaryRows.Get()[i.pos]
		i.pos++
		return row, nil
	}*/

	if i.bottom == nil {
		var iter types.RowIterator
		iter, err = i.secondaryProvider.Iterator(ctx, i.primaryRow)
		if err != nil {
			return nil, err
		}

		i.bottom = iter
	}

	rightRow, err := i.bottom.Next(ctx)
	if err != nil {
		if err == types.ErrNoMoreRows {
			//err = i.bottom.Close(ctx)
			i.bottom = nil
			//if err != nil {
			//	return nil, err
			//}
			i.primaryRow = nil

			// If we got to this point and the mode is still unknown it means
			// the right side fits in memory, so the mode changes to memory
			// join.
			//if i.mode == unknownMode {
			//	i.mode = memoryMode
			//}

			return nil, types.ErrNoMoreRows
		}
		return nil, err
	}

	if i.mode == unknownMode {
		var switchToMultipass bool
		//if !ctx.Memory.HasAvailable() {
		//switchToMultipass = true
		//} else {
		err := i.secondaryRows.Add(rightRow)
		if err != nil { //&& !sql.ErrNoMemoryAvailable.Is(err) {
			return nil, err
		}
		//}

		if switchToMultipass {
			//i.Dispose()
			i.secondaryRows = nil
			i.mode = multipassMode
		}
	}

	return rightRow, nil
}

func (i *nestedLoopsIter) buildRow(primary, secondary types.Row) types.Row {
	toCut := len(i.originalRow) - i.scopeLen
	row := make(types.Row, i.rowSize-toCut)

	scope := primary[:i.scopeLen]
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
		secondOffset = i.scopeLen + len(first)
	}

	copy(row, scope)
	copy(row[i.scopeLen:], first)
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
		if err := i.loadPrimary(ctx); err != nil {
			return nil, err
		}

		primary := i.primaryRow
		secondary, err := i.loadSecondary(ctx)
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
		return row, nil
	}
}
