// Copyright 2021 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/molecula/featurebase/v3/sql3"
	"github.com/molecula/featurebase/v3/sql3/planner/types"
)

//TODO(pok) push order by down as far as possible
//TODO(pok) handle the case of the order by expressions not being in a projection list
//TODO(pok) you can't group by _id in PQL, so we need to not use a PQL group by operator here
//TODO(pok) move constant folding to the here

var optimizerFunctions = []OptimizerFunc{
	// if we have a group by that has one TableScanOperator,
	// no Top or TopN or Distincts, try to use a PQL(multi)
	// groupby operator instead
	tryToReplaceGroupByWithPQLGroupBy,

	// if we have a group by with no group by exprs that has
	// one TableScanOperator, no Top or TopN or Distincts, try
	// to use a PQL aggregate operators instead
	tryToReplaceGroupByWithPQLAggregate,

	// update the columnIdx for all the references in the projections
	// based on the child operator for a projection
	fixGroupByProjections,

	// update the columnIdx for all the references in the projections
	// based on the child operator for a projection
	fixJoinProjections,

	// if the query has one TableScanOperator then push the top
	// expression down into that operator
	pushdownPQLTop,
}

type OptimizerScope struct {
}

type OptimizerFunc func(context.Context, *ExecutionPlanner, types.PlanOperator, *OptimizerScope) (types.PlanOperator, bool, error)

// optimizePlan takes a plan from the compiler and executes a series of transforms on it to optimize it
func (p *ExecutionPlanner) optimizePlan(ctx context.Context, plan types.PlanOperator) (types.PlanOperator, error) {
	var err error
	var result = plan
	for _, ofunc := range optimizerFunctions {
		result, err = p.optimizeNode(ctx, result, ofunc)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (p *ExecutionPlanner) optimizeNode(ctx context.Context, node types.PlanOperator, ofunc OptimizerFunc) (types.PlanOperator, error) {
	op, same, err := ofunc(ctx, p, node, nil)
	if err != nil {
		return nil, err
	}
	if !same {
		return op, nil
	}
	return node, nil
}

func tryToReplaceGroupByWithPQLAggregate(ctx context.Context, a *ExecutionPlanner, n types.PlanOperator, scope *OptimizerScope) (types.PlanOperator, bool, error) {
	//bail if there are any joins
	scans, err := hasOnlyTableScans(ctx, a, n, scope)
	if err != nil {
		return nil, false, err
	}
	if !scans {
		return n, true, nil
	}
	//bail if there is a top
	top, err := hasTop(ctx, a, n, scope)
	if err != nil {
		return nil, false, err
	}
	if top {
		return n, true, nil
	}

	//go find the table scan operators
	tables := getTableScanOperators(ctx, a, n, scope)

	//only do this if we have one TableScanOperator
	if len(tables) == 1 {
		return TransformPlanOp(n, func(node types.PlanOperator) (types.PlanOperator, bool, error) {
			switch n := node.(type) {
			case *PlanOpGroupBy:
				//only do this if there are no group by expressions
				if len(n.GroupByExprs) == 0 {

					//table scan
					table := tables[0]
					ops := make([]*PlanOpPQLAggregate, 0)

					for _, agg := range n.Aggregates {
						aggregable, ok := agg.(types.Aggregable)
						if !ok {
							return n, false, sql3.NewErrInternalf("unexpected aggregate function arg type '%T'", agg)
						}

						ops = append(ops, NewPlanOpPQLAggregate(a, table.tableName, aggregable, table.filter))
					}
					newOp := NewPlanOpPQLMultiAggregate(a, ops)
					lenOps := len(ops)
					if lenOps > 1 {
						newOp.AddWarning(fmt.Sprintf("Multiple (%d) aggregates referenced in select list will result in multiple aggregate queries being executed.", lenOps))
					}
					return newOp, false, nil
				}
				return n, true, nil
			default:
				return n, true, nil
			}
		})
	}
	return n, true, nil
}

func tryToReplaceGroupByWithPQLGroupBy(ctx context.Context, a *ExecutionPlanner, n types.PlanOperator, scope *OptimizerScope) (types.PlanOperator, bool, error) {
	//bail if there are any joins
	scans, err := hasOnlyTableScans(ctx, a, n, scope)
	if err != nil {
		return nil, false, err
	}
	if !scans {
		return n, true, nil
	}
	//bail if there is a top
	top, err := hasTop(ctx, a, n, scope)
	if err != nil {
		return nil, false, err
	}
	if top {
		return n, true, nil
	}

	//go find the table scan operators
	tables := getTableScanOperators(ctx, a, n, scope)

	//only do this if we have one TableScanOperator
	if len(tables) == 1 {
		return TransformPlanOp(n, func(node types.PlanOperator) (types.PlanOperator, bool, error) {
			switch n := node.(type) {
			case *PlanOpGroupBy:
				//table scan
				table := tables[0]
				//only do this if we have group by expressions
				if len(n.GroupByExprs) > 0 {
					//use a multi group by if more than 1 aggregate
					if len(n.Aggregates) > 1 {
						ops := make([]*PlanOpPQLGroupBy, 0)
						for _, agg := range n.Aggregates {

							aggregable, ok := agg.(types.Aggregable)
							if !ok {
								return n, false, sql3.NewErrInternalf("unexpected aggregate function arg type '%T'", agg)
							}
							ops = append(ops, NewPlanOpPQLGroupBy(a, table.tableName, n.GroupByExprs, table.filter, aggregable))
						}
						newOp := NewPlanOpPQLMultiGroupBy(a, ops, n.GroupByExprs)
						newOp.AddWarning(fmt.Sprintf("Multiple (%d) aggregates referenced in select list will result in multiple group by aggregate queries being executed.", len(ops)))
						return newOp, false, nil
					}
					//only one aggregate
					aggregable, ok := n.Aggregates[0].(types.Aggregable)
					if !ok {
						return n, false, sql3.NewErrInternalf("unexpected aggregate function arg type '%T'", n.Aggregates[0])
					}
					newOp := NewPlanOpPQLGroupBy(a, table.tableName, n.GroupByExprs, table.filter, aggregable)
					return newOp, false, nil
				}
				return n, true, nil
			default:
				return n, true, nil
			}
		})
	}
	return n, true, nil
}

func pushdownPQLTop(ctx context.Context, a *ExecutionPlanner, n types.PlanOperator, scope *OptimizerScope) (types.PlanOperator, bool, error) {
	//bail if there are any joins
	hasOnlyScans, err := hasOnlyTableScans(ctx, a, n, scope)
	if err != nil {
		return nil, false, err
	}
	if !hasOnlyScans {
		return n, true, nil
	}

	//go find the table scan operators
	tables := getTableScanOperators(ctx, a, n, scope)

	//only do this if we have one TableScanOperator
	if len(tables) == 1 {
		return TransformPlanOp(n, func(node types.PlanOperator) (types.PlanOperator, bool, error) {
			switch n := node.(type) {
			case *PlanOpTop:
				table := tables[0]
				//set the topExpr for the PlanOpTableScan
				table.topExpr = n.expr
				//return the child of the top node to eliminate it
				return n.ChildOp, false, nil
			default:
				return n, true, nil
			}
		})
	}
	return n, true, nil
}

func areAggregablesEqual(lhs types.Aggregable, rhs types.Aggregable) bool {
	if reflect.TypeOf(lhs) == reflect.TypeOf(rhs) {
		lhsRef, lhsok := lhs.AggExpression().(*qualifiedRefPlanExpression)
		rhsRef, rhsok := rhs.AggExpression().(*qualifiedRefPlanExpression)
		if lhsok && rhsok {
			return strings.EqualFold(lhsRef.columnName, rhsRef.columnName)
		}
	}
	return false
}

func fixGroupByProjections(ctx context.Context, a *ExecutionPlanner, n types.PlanOperator, scope *OptimizerScope) (types.PlanOperator, bool, error) {
	return TransformPlanOp(n, func(node types.PlanOperator) (types.PlanOperator, bool, error) {
		switch n := node.(type) {
		case *PlanOpProjection:
			switch childOp := n.ChildOp.(type) {
			case *PlanOpGroupBy:
				//PlanOpGroupBy's iterator returns group by exprs, then aggregates in the order they appear

				for idx, pj := range n.Projections {
					expr, _, err := TransformExpr(pj, func(e types.PlanExpression) (types.PlanExpression, bool, error) {
						switch e.(type) {
						case types.Aggregable:
							// if we have a Aggregable, the AggExpression() will be a qualified ref
							// given we are in the context of a PlanOpProjection with a PlanOpGroupBy
							// we can use the ordinal position of the projection as the column index
							ae := newQualifiedRefPlanExpression("", "", idx, e.Type())
							return ae, false, nil
						default:
							return e, true, nil
						}
					})
					if err != nil {
						return n, true, err
					}
					n.Projections[idx] = expr
				}
				return n, false, nil

			case *PlanOpPQLGroupBy:
				// PlanOpGroupBy's iterator returns group by exprs, then the single aggregate in the order they appear

				// make a map of the names of the group by columns
				groupByColumnsNameMap := make(map[string]int)
				for gidx, gbe := range childOp.groupByExprs {
					gbeRef, ok := gbe.(*qualifiedRefPlanExpression)
					if !ok {
						return nil, false, sql3.NewErrInternalf("unexpected group by expression type '%T'", gbe)
					}
					gbeRef.columnIndex = gidx
					groupByColumnsNameMap[gbeRef.columnName] = gidx
				}
				//set the index for the aggregate to be the length of the group by list
				aggregateIndex := len(childOp.groupByExprs)

				//loop projections:
				//1. looking for the Aggregable and replace it with a qualifiedRefPlanExpression pointing to
				//	 the offset in the child iterator
				//2. looking for the qualified refs replace it with a qualifiedRefPlanExpression pointing to
				//	 the offset in the child iterator
				for idx, pj := range n.Projections {
					expr, _, err := TransformExpr(pj, func(e types.PlanExpression) (types.PlanExpression, bool, error) {
						switch thisExpr := e.(type) {
						case types.Aggregable:
							ae := newQualifiedRefPlanExpression(fmt.Sprintf("$PlanOpPQLGroupBy:%d", aggregateIndex), "", aggregateIndex, e.Type())
							return ae, false, nil
						case *qualifiedRefPlanExpression:
							colIdx, ok := groupByColumnsNameMap[thisExpr.columnName]
							if ok {
								ae := newQualifiedRefPlanExpression(fmt.Sprintf("$PlanOpPQLGroupBy.%s:%d", thisExpr.columnName, colIdx), thisExpr.columnName, colIdx, e.Type())
								return ae, false, nil
							}
							return e, true, nil
						default:
							return e, true, nil
						}
					})
					if err != nil {
						return n, true, err
					}
					n.Projections[idx] = expr
				}
				return n, false, nil

			case *PlanOpPQLMultiAggregate:
				//PlanOpGroupBy's iterator returns aggregates in the order they appear

				// make a list of the aggregables
				aggregableList := make([]types.Aggregable, 0)
				for _, op := range childOp.operators {
					aggregableList = append(aggregableList, op.aggregate)
				}

				//loop projections:
				//1. looking for the Aggregable and replace it with a qualifiedRefPlanExpression pointing to
				//	 the offset in the child iterator
				for idx, pj := range n.Projections {
					expr, _, err := TransformExpr(pj, func(e types.PlanExpression) (types.PlanExpression, bool, error) {
						switch thisExpr := e.(type) {
						case types.Aggregable:
							for idx, a := range aggregableList {
								if areAggregablesEqual(thisExpr, a) {
									ae := newQualifiedRefPlanExpression(fmt.Sprintf("$PlanOpPQLMultiAggregate:%d", idx), "", idx, e.Type())
									return ae, false, nil
								}
							}
							return e, true, nil
						default:
							return e, true, nil
						}
					})
					if err != nil {
						return n, true, err
					}
					n.Projections[idx] = expr
				}
				return n, false, nil

			case *PlanOpPQLMultiGroupBy:
				//PlanOpPQLMultiGroupBy's iterator returns group by exprs, then the aggregates in the order they appear

				// make a map of the names of the group by columns and update the indexes
				groupByColumnsNameMap := make(map[string]int)
				for gidx, gbe := range childOp.groupByExprs {
					gbeRef, ok := gbe.(*qualifiedRefPlanExpression)
					if !ok {
						return nil, false, sql3.NewErrInternalf("unexpected group by expression type '%T'", gbe)
					}
					gbeRef.columnIndex = gidx
					groupByColumnsNameMap[gbeRef.columnName] = gidx
				}
				//set the index for the start of the aggregates to be the length of the group by list
				aggregateStartIndex := len(childOp.groupByExprs)

				// make a list of the aggregables
				aggregableList := make([]types.Aggregable, 0)
				for _, op := range childOp.operators {
					aggregableList = append(aggregableList, op.aggregate)
				}

				//loop projections:
				//1. looking for the Aggregable and replace it with a qualifiedRefPlanExpression pointing to
				//	 the offset in the child iterator
				//2. looking for the qualified refs replace it with a qualifiedRefPlanExpression pointing to
				//	 the offset in the child iterator
				for idx, pj := range n.Projections {
					expr, _, err := TransformExpr(pj, func(e types.PlanExpression) (types.PlanExpression, bool, error) {
						switch thisExpr := e.(type) {
						case types.Aggregable:
							for idx, a := range aggregableList {
								if areAggregablesEqual(thisExpr, a) {
									ae := newQualifiedRefPlanExpression(fmt.Sprintf("$PlanOpPQLMultiGroupBy:%d", idx+aggregateStartIndex), "", idx+aggregateStartIndex, e.Type())
									return ae, false, nil
								}
							}
							return e, true, nil
						case *qualifiedRefPlanExpression:
							colIdx, ok := groupByColumnsNameMap[thisExpr.columnName]
							if ok {
								ae := newQualifiedRefPlanExpression(fmt.Sprintf("$PlanOpPQLMultiGroupBy.%s:%d", thisExpr.columnName, colIdx), thisExpr.columnName, colIdx, e.Type())
								return ae, false, nil
							}
							return e, true, nil

						default:
							return e, true, nil
						}
					})
					if err != nil {
						return n, true, err
					}
					n.Projections[idx] = expr
				}
				return n, false, nil
			}
			return n, true, nil
		default:
			return n, true, nil
		}
	})
}

func fixJoinProjections(ctx context.Context, a *ExecutionPlanner, n types.PlanOperator, scope *OptimizerScope) (types.PlanOperator, bool, error) {
	return TransformPlanOp(n, func(node types.PlanOperator) (types.PlanOperator, bool, error) {
		switch n := node.(type) {
		case *PlanOpProjection:
			switch childOp := n.ChildOp.(type) {
			case *PlanOpNestedLoops:
				//PlanOpNestedLoops iterator returns columns from top iterator and then columns from bottom iterator

				//make a map of names from the schema
				schemaNameMap := make(map[string]int)
				schema := childOp.Schema()
				for idx, s := range schema {
					key := fmt.Sprintf("%s.%s", s.Table, s.Name)
					schemaNameMap[key] = idx
				}

				for idx, pj := range n.Projections {
					expr, _, err := TransformExpr(pj, func(e types.PlanExpression) (types.PlanExpression, bool, error) {
						switch thisExpr := e.(type) {
						case *qualifiedRefPlanExpression:
							key := fmt.Sprintf("%s.%s", thisExpr.tableName, thisExpr.columnName)
							colIdx, ok := schemaNameMap[key]
							if ok {
								ae := newQualifiedRefPlanExpression(fmt.Sprintf("$PlanOpNestedLoops.%s.%s:%d", thisExpr.tableName, thisExpr.columnName, colIdx), thisExpr.columnName, colIdx, e.Type())
								return ae, false, nil
							}
							return e, true, nil

						default:
							return e, true, nil
						}
					})
					if err != nil {
						return n, true, err
					}
					n.Projections[idx] = expr
				}
				return n, false, nil
			}
			return n, true, nil
		default:
			return n, true, nil
		}
	})
}

// hasTop inspects a plan op tree and returns true (or error) if there are Top
// operators.
func hasTop(ctx context.Context, a *ExecutionPlanner, n types.PlanOperator, scope *OptimizerScope) (bool, error) {
	result := false
	InspectPlan(n, func(node types.PlanOperator) bool {
		switch node.(type) {
		case *PlanOpTop:
			result = true
			return false
		}
		return true
	})
	return result, nil
}

// hasTopN inspects a plan op tree and returns true (or error) if there are TopN
// operators.
func hasTopN(ctx context.Context, a *ExecutionPlanner, n types.PlanOperator, scope *OptimizerScope) (bool, error) {
	//TODO(pok) implement this
	return false, nil
}

// inspects a plan op tree and returns false (or error) if there are read operators other
// than table scans
func hasOnlyTableScans(ctx context.Context, a *ExecutionPlanner, n types.PlanOperator, scope *OptimizerScope) (bool, error) {
	//assume true
	result := true
	InspectPlan(n, func(node types.PlanOperator) bool {
		// if we find a nested loops, nope to only table scans
		switch node.(type) {
		case *PlanOpNestedLoops:
			result = false
			return false
		}
		return true
	})
	return result, nil
}

// inspects a plan op tree and returns a list (or error) of all the PlanOpTableScan operators
func getTableScanOperators(ctx context.Context, a *ExecutionPlanner, n types.PlanOperator, scope *OptimizerScope) []*PlanOpPQLTableScan {
	var tables []*PlanOpPQLTableScan
	//go find the table scan operators
	InspectPlan(n, func(node types.PlanOperator) bool {
		switch nd := node.(type) {
		case *PlanOpPQLTableScan:
			tables = append(tables, nd)
			return false
		}
		return true
	})
	return tables
}
