// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/featurebasedb/featurebase/v3/sql3"
	"github.com/featurebasedb/featurebase/v3/sql3/parser"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
)

// orderByOrder is the direction of the order by (ascending or descending).
type orderByOrder int

const (
	orderByAsc  orderByOrder = 1
	orderByDesc orderByOrder = 2
)

// nullOrdering specifies how to handle null values during order by.
type nullOrdering byte

const (
	nullOrderingFirst nullOrdering = iota
	nullOrderingLast  nullOrdering = 2
)

// OrderByExpression is the expression on which an order by can be computed
type OrderByExpression struct {
	Expr         types.PlanExpression
	Order        orderByOrder
	NullOrdering nullOrdering
}

// PlanOpOrderBy plan operator handles ORDER BY
type PlanOpOrderBy struct {
	ChildOp       types.PlanOperator
	orderByFields []*OrderByExpression

	warnings []string
}

func NewPlanOpOrderBy(orderByFields []*OrderByExpression, child types.PlanOperator) *PlanOpOrderBy {
	return &PlanOpOrderBy{
		ChildOp:       child,
		orderByFields: orderByFields,
		warnings:      make([]string, 0),
	}
}

func (n *PlanOpOrderBy) Schema() types.Schema {
	return n.ChildOp.Schema()
}

func (n *PlanOpOrderBy) Iterator(ctx context.Context, row types.Row) (types.RowIterator, error) {
	iter, err := n.ChildOp.Iterator(ctx, row)
	if err != nil {
		return nil, err
	}
	return newOrderByIter(ctx, n, iter), nil
}

func (n *PlanOpOrderBy) Children() []types.PlanOperator {
	return []types.PlanOperator{
		n.ChildOp,
	}
}

func (n *PlanOpOrderBy) WithChildren(children ...types.PlanOperator) (types.PlanOperator, error) {
	if len(children) != 1 {
		return nil, sql3.NewErrInternalf("unexpected number of children '%d'", len(children))
	}
	return NewPlanOpOrderBy(n.orderByFields, children[0]), nil
}

func (n *PlanOpOrderBy) String() string {
	return ""
}

func (n *PlanOpOrderBy) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_op"] = fmt.Sprintf("%T", n)
	sc := make([]string, 0)
	for _, e := range n.Schema() {
		sc = append(sc, fmt.Sprintf("'%s', '%s', '%s'", e.ColumnName, e.RelationName, e.Type.TypeName()))
	}
	result["_schema"] = sc

	result["child"] = n.ChildOp.Plan()
	ps := make([]interface{}, 0)
	for _, e := range n.orderByFields {
		ps = append(ps, &map[string]interface{}{
			"expr":         e.Expr.Plan(),
			"order":        e.Order,
			"nullOrdering": e.NullOrdering,
		})
	}
	result["orderByFields"] = ps
	return result
}

func (n *PlanOpOrderBy) AddWarning(warning string) {
	n.warnings = append(n.warnings, warning)
}

func (n *PlanOpOrderBy) Warnings() []string {
	var w []string
	w = append(w, n.warnings...)
	w = append(w, n.ChildOp.Warnings()...)
	return w
}

type orderByIter struct {
	s          *PlanOpOrderBy
	childIter  types.RowIterator
	sortedRows []types.Row
}

var _ types.RowIterator = (*orderByIter)(nil)

func newOrderByIter(ctx context.Context, s *PlanOpOrderBy, child types.RowIterator) *orderByIter {
	return &orderByIter{
		s:         s,
		childIter: child,
	}
}

func (i *orderByIter) Next(ctx context.Context) (types.Row, error) {
	if i.sortedRows == nil {
		err := i.computeOrderByRows(ctx)
		if err != nil {
			return nil, err
		}
	}

	if len(i.sortedRows) > 0 {
		row := i.sortedRows[0]
		// Move to next result element.
		i.sortedRows = i.sortedRows[1:]
		return row, nil
	}
	return nil, types.ErrNoMoreRows
}

func (i *orderByIter) computeOrderByRows(ctx context.Context) error {
	cache := newInMemoryRowCache()

	for {
		row, err := i.childIter.Next(ctx)

		if err == types.ErrNoMoreRows {
			break
		}
		if err != nil {
			return err
		}

		if err := cache.Add(row); err != nil {
			return err
		}
	}

	rows := cache.AllRows()
	sorter := &OrderBySorter{
		SortFields: i.s.orderByFields,
		Rows:       rows,
		LastError:  nil,
		Ctx:        ctx,
	}
	sort.Stable(sorter)
	if sorter.LastError != nil {
		return sorter.LastError
	}
	i.sortedRows = rows
	return nil
}

type OrderBySorter struct {
	SortFields []*OrderByExpression
	Rows       []types.Row
	LastError  error
	Ctx        context.Context
}

func (s *OrderBySorter) Len() int {
	return len(s.Rows)
}

func (s *OrderBySorter) Swap(i, j int) {
	s.Rows[i], s.Rows[j] = s.Rows[j], s.Rows[i]
}

func (s *OrderBySorter) Less(i, j int) bool {
	if s.LastError != nil {
		return false
	}

	//TODO(pok) handle multi column sort

	a := s.Rows[i]
	b := s.Rows[j]
	for _, sf := range s.SortFields {
		av, err := sf.Expr.Evaluate(a)
		if err != nil {
			s.LastError = sql3.NewErrInternalf("unable to sort '%s'", err.Error())
			return false
		}

		bv, err := sf.Expr.Evaluate(b)
		if err != nil {
			s.LastError = sql3.NewErrInternalf("unable to sort '%s'", err.Error())
			return false
		}

		if sf.Order == orderByDesc {
			av, bv = bv, av
		}

		if av == nil && bv == nil {
			continue
		} else if av == nil {
			return sf.NullOrdering == nullOrderingFirst
		} else if bv == nil {
			return sf.NullOrdering != nullOrderingFirst
		}

		switch sf.Expr.Type().(type) {
		case *parser.DataTypeInt, *parser.DataTypeID:
			avInt, aok := av.(int64)
			bvInt, bok := bv.(int64)
			if !(aok && bok) {
				s.LastError = sql3.NewErrInternalf("unexpected type conversion result")
				return false
			}
			if avInt > bvInt {
				return false
			}
			return true

		case *parser.DataTypeBool:
			avBool, aok := av.(bool)
			bvBool, bok := bv.(bool)
			if !(aok && bok) {
				s.LastError = sql3.NewErrInternalf("unexpected type conversion result")
				return false
			}
			if avBool == bvBool {
				return false
			}
			return true

		case *parser.DataTypeTimestamp:
			avTime, aok := av.(time.Time)
			bvTime, bok := bv.(time.Time)
			if !(aok && bok) {
				s.LastError = sql3.NewErrInternalf("unexpected type conversion result")
				return false
			}
			if avTime.After(bvTime) {
				return false
			}
			return true

		default:
			s.LastError = sql3.NewErrInternalf("unhandled data type '%T'", sf.Expr.Type())
			return false
		}
	}

	return false
}
