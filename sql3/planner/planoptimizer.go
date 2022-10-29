// Copyright 2021 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/featurebasedb/featurebase/v3/errors"
	"github.com/featurebasedb/featurebase/v3/sql3"
	"github.com/featurebasedb/featurebase/v3/sql3/parser"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
)

//TODO(pok) push order by down as far as possible
//TODO(pok) handle the case of the order by expressions not being in a projection list
//TODO(pok) you can't group by _id in PQL, so we need to not use a PQL group by operator here
//TODO(pok) move constant folding to in here

// a function prototype for all optimizer rules
type OptimizerFunc func(context.Context, *ExecutionPlanner, types.PlanOperator, *OptimizerScope) (types.PlanOperator, bool, error)

// a list of optimzer rules; order can be important important
var optimizerFunctions = []OptimizerFunc{
	// push down filter predicates as far as possible,
	pushdownFilters,

	// if we have a group by that has one TableScanOperator,
	// no Top or TopN or Distincts, try to use a PQL(multi)
	// groupby operator instead
	tryToReplaceGroupByWithPQLGroupBy,

	// if we have a group by with no group by exprs that has
	// one TableScanOperator, no Top or TopN or Distincts, try
	// to use a PQL aggregate operators instead
	tryToReplaceGroupByWithPQLAggregate,

	// if we have a subtable call on a timequantum type
	// take the join out and use the appropriate PQL operator instead
	tryToRewriteSubtableJoins,

	// update the columnIdx for all the references in the projections
	// based on the child operator for a projection
	fixGroupByProjections,

	// update the columnIdx for all the references in joins
	fixJoinFieldRefs,

	// update the columnIdx for all the references in group bys
	fixGroupByFieldRefs,

	// if the query has one TableScanOperator then push the top
	// expression down into that operator
	pushdownPQLTop,
}

// this will be used in future for symbol resolution when CTEs and subquery support matures
// and we need to introduce the concept of scope to symbol resolution
type OptimizerScope struct {
}

// optimizePlan takes a plan from the compiler and executes a series of transforms on it to optimize it
func (p *ExecutionPlanner) optimizePlan(ctx context.Context, plan types.PlanOperator) (types.PlanOperator, error) {

	//log.Println("================================================================================")
	//log.Println("plan pre-optimzation")
	//jplan := plan.Plan()
	//a, _ := json.MarshalIndent(jplan, "", "    ")
	//log.Println(string(a))
	//log.Println("--------------------------------------------------------------------------------")

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

// a set of filters for a operator graph
type filterSet struct {
	filterConditions  []types.PlanExpression
	filtersByRelation map[string][]types.PlanExpression
	handledFilters    []types.PlanExpression
	relationAliases   RelationAliasesMap
}

func newFilterSet(filter types.PlanExpression, filtersByTable map[string][]types.PlanExpression, tableAliases RelationAliasesMap) *filterSet {
	return &filterSet{
		filterConditions:  splitOnAnd(filter),
		filtersByRelation: filtersByTable,
		relationAliases:   tableAliases,
	}
}

func (fs *filterSet) availableFiltersForTable(table string) []types.PlanExpression {
	filters, ok := fs.filtersByRelation[table]
	if !ok {
		return nil
	}
	return remainingExpressions(filters, fs.handledFilters)
}

func (fs *filterSet) handledCount() int {
	return len(fs.handledFilters)
}

func (fs *filterSet) markFiltersHandled(exprs ...types.PlanExpression) {
	fs.handledFilters = append(fs.handledFilters, exprs...)
}

func (fs *filterSet) unhandledPredicates(ctx context.Context) []types.PlanExpression {
	var available []types.PlanExpression
	for _, e := range fs.filterConditions {
		available = append(available, remainingExpressions([]types.PlanExpression{e}, fs.handledFilters)...)
	}
	return available
}

func remainingExpressions(allExprs, lessExprs []types.PlanExpression) []types.PlanExpression {
	var remainder []types.PlanExpression
	for _, e := range allExprs {
		var found bool
		for _, s := range lessExprs {
			if reflect.DeepEqual(e, s) {
				found = true
				break
			}
		}

		if !found {
			remainder = append(remainder, e)
		}
	}
	return remainder
}

// RelationAliasesMap is a map of aliases to Relations
type RelationAliasesMap map[string]types.IdentifiableByName

func (ta RelationAliasesMap) addAlias(alias types.IdentifiableByName, target types.IdentifiableByName) error {
	lowerName := strings.ToLower(alias.Name())
	if _, ok := ta[lowerName]; ok {
		return sql3.NewErrInternalf("unexpected duplicate alias name")
	}
	ta[lowerName] = target
	return nil
}

func getRelationAliases(n types.PlanOperator, scope *OptimizerScope) (RelationAliasesMap, error) {
	var aliases RelationAliasesMap
	var aliasFn func(node types.PlanOperator) bool
	var inspectErr error
	aliasFn = func(node types.PlanOperator) bool {
		if node == nil {
			return false
		}

		if at, ok := node.(*PlanOpRelAlias); ok {
			switch t := at.ChildOp.(type) {
			case *PlanOpPQLTableScan:
				inspectErr = aliases.addAlias(at, t)
			case *PlanOpSubquery:
				inspectErr = aliases.addAlias(at, t)
			default:
				panic(fmt.Sprintf("unexpected child node '%T'", at.ChildOp))
			}
			return false
		}

		switch node := node.(type) {
		case *PlanOpPQLTableScan:
			inspectErr = aliases.addAlias(node, node)
			return false
		}

		return true
	}

	aliases = make(RelationAliasesMap)
	InspectPlan(n, aliasFn)
	if inspectErr != nil {
		return nil, inspectErr
	}
	return aliases, inspectErr
}

// governs how far down filter push down can go
func filterPushdownChildSelector(c ParentContext) bool {
	switch c.Parent.(type) {
	case *PlanOpRelAlias:
		//definitely don't go any further than alias
		return false
	}
	return true
}

// governs how far down filter push down above tables can go
func filterPushdownAboveTablesChildSelector(c ParentContext) bool {
	if !filterPushdownChildSelector(c) {
		return false
	}
	switch c.Parent.(type) {
	case *PlanOpFilter:
		switch c.Operator.(type) {
		case *PlanOpRelAlias, *PlanOpPQLTableScan:
			return false
		}
	}

	return true
}

// returns an expression given a list of expressions, if the list is > 2 expressions, all the individual
// expressions are ANDed together
func joinExprsWithAnd(exprs ...types.PlanExpression) types.PlanExpression {
	switch len(exprs) {
	case 0:
		return nil
	case 1:
		return exprs[0]
	default:
		result := newBinOpPlanExpression(exprs[0], parser.AND, exprs[1], parser.NewDataTypeBool())
		for _, e := range exprs[2:] {
			result = newBinOpPlanExpression(result, parser.AND, e, parser.NewDataTypeBool())
		}
		return result
	}
}

func removePushedDownConditions(ctx context.Context, a *ExecutionPlanner, node *PlanOpFilter, filters *filterSet) (types.PlanOperator, bool, error) {
	if filters.handledCount() == 0 {
		return node, true, nil
	}

	unhandled := filters.unhandledPredicates(ctx)
	if len(unhandled) == 0 {
		return node.ChildOp, false, nil
	}

	joinedExpr := joinExprsWithAnd(unhandled...)
	return NewPlanOpFilter(a, joinedExpr, node.ChildOp), false, nil
}

func getRelation(node types.PlanOperator) types.IdentifiableByName {
	var relation types.IdentifiableByName
	InspectPlan(node, func(node types.PlanOperator) bool {
		switch n := node.(type) {
		case *PlanOpPQLTableScan:
			relation = n
			return false
		}
		return true
	})
	return relation
}

func pushdownFiltersToFilterableRelations(ctx context.Context, a *ExecutionPlanner, tableNode types.PlanOperator, scope *OptimizerScope, filters *filterSet, tableAliases RelationAliasesMap) (types.PlanOperator, bool, error) {
	// only do this if it is an alias or a pql table scan
	switch tableNode.(type) {
	case *PlanOpRelAlias, *PlanOpPQLTableScan:
		// continue
	default:
		return nil, true, sql3.NewErrInternalf("unexpected op type '%T'", tableNode)
	}

	table := getRelation(tableNode)
	if table == nil {
		return tableNode, true, nil
	}

	ft, ok := table.(types.FilteredRelation)
	if !ok {
		return tableNode, true, nil
	}

	// do we have any filters for this table? if not, bail...
	tableFilters := filters.availableFiltersForTable(table.Name())
	if len(tableFilters) == 0 {
		return tableNode, true, nil
	}
	filters.markFiltersHandled(tableFilters...)

	tableFilters, _, err := fixFieldRefIndexesOnExpressions(ctx, scope, a, tableNode.Schema(), tableFilters...)
	if err != nil {
		return nil, true, err
	}

	newOp, err := ft.UpdateFilters(joinExprsWithAnd(tableFilters...))
	if err != nil {
		return nil, true, err
	}
	return newOp, false, nil
}

func pushdownFiltersToAboveRelation(ctx context.Context, a *ExecutionPlanner, tableNode types.PlanOperator, scope *OptimizerScope, filters *filterSet) (types.PlanOperator, bool, error) {
	table := getRelation(tableNode)
	if table == nil {
		return tableNode, true, nil
	}

	// reposition any remaining filters for a table to directly above the table itself
	var pushedDownFilterExpression types.PlanExpression
	if tableFilters := filters.availableFiltersForTable(table.Name()); len(tableFilters) > 0 {
		filters.markFiltersHandled(tableFilters...)

		handled, _, err := fixFieldRefIndexesOnExpressions(ctx, scope, a, tableNode.Schema(), tableFilters...)
		if err != nil {
			return nil, true, err
		}

		pushedDownFilterExpression = joinExprsWithAnd(handled...)
	}

	switch tableNode.(type) {
	case *PlanOpRelAlias, *PlanOpPQLTableScan:
		node := tableNode
		if pushedDownFilterExpression != nil {
			return NewPlanOpFilter(a, pushedDownFilterExpression, node), false, nil
		}
		return node, false, nil
	default:
		return nil, true, sql3.NewErrInternalf("unexpected op type '%T'", tableNode)
	}
}

func pushdownFilters(ctx context.Context, a *ExecutionPlanner, n types.PlanOperator, scope *OptimizerScope) (types.PlanOperator, bool, error) {

	tableAliases, err := getRelationAliases(n, scope)
	if err != nil {
		return nil, true, err
	}

	pushdownFiltersForFilterableRelations := func(n *PlanOpFilter, filters *filterSet) (types.PlanOperator, bool, error) {
		return TransformPlanOpWithParent(n, filterPushdownChildSelector, func(c ParentContext) (types.PlanOperator, bool, error) {
			switch node := c.Operator.(type) {
			case *PlanOpFilter:
				n, samePred, err := removePushedDownConditions(ctx, a, node, filters)
				if err != nil {
					return nil, true, err
				}
				n, sameFix, err := fixFieldRefIndexesForOperator(ctx, a, n, scope)
				if err != nil {
					return nil, true, err
				}
				return n, samePred && sameFix, nil

			case *PlanOpRelAlias, *PlanOpPQLTableScan:
				n, samePred, err := pushdownFiltersToFilterableRelations(ctx, a, node, scope, filters, tableAliases)
				if err != nil {
					return nil, true, err
				}
				n, sameFix, err := fixFieldRefIndexesForOperator(ctx, a, n, scope)
				if err != nil {
					return nil, true, err
				}
				return n, samePred && sameFix, nil
			default:
				return fixFieldRefIndexesForOperator(ctx, a, node, scope)
			}
		})
	}

	pushdownFiltersCloseToRelations := func(n types.PlanOperator, filters *filterSet) (types.PlanOperator, bool, error) {
		return TransformPlanOpWithParent(n, filterPushdownAboveTablesChildSelector, func(c ParentContext) (types.PlanOperator, bool, error) {
			switch node := c.Operator.(type) {
			case *PlanOpFilter:
				n, same, err := removePushedDownConditions(ctx, a, node, filters)
				if err != nil {
					return nil, true, err
				}
				if same {
					return n, true, nil
				}
				n, _, err = fixFieldRefIndexesForOperator(ctx, a, n, scope)
				if err != nil {
					return nil, true, err
				}
				return n, false, nil
			case *PlanOpRelAlias, *PlanOpPQLTableScan:
				table, same, err := pushdownFiltersToAboveRelation(ctx, a, node, scope, filters)
				if err != nil {
					return nil, true, err
				}
				if same {
					return node, true, nil
				}
				node, _, err = fixFieldRefIndexesForOperator(ctx, a, table, scope)
				if err != nil {
					return nil, true, err
				}
				return node, false, nil
			default:
				return fixFieldRefIndexesForOperator(ctx, a, node, scope)
			}
		})
	}

	return TransformPlanOp(n, func(node types.PlanOperator) (types.PlanOperator, bool, error) {
		switch n := node.(type) {
		case *PlanOpFilter:
			filtersByTable := getFiltersByRelation(n)
			filters := newFilterSet(n.Predicate, filtersByTable, tableAliases)

			// first push down filters to any op that implements FilteredRelation
			node, sameA, err := pushdownFiltersForFilterableRelations(n, filters)
			if err != nil {
				return nil, true, err
			}

			// second push down filters as close as possible to the relations they apply to
			node, sameB, err := pushdownFiltersCloseToRelations(node, filters)
			if err != nil {
				return nil, true, err
			}
			return node, sameA && sameB, nil

		default:
			return n, true, nil
		}
	})
}

// getFiltersByRelation returns a map of relations name to filter expressions for the op provided
func getFiltersByRelation(n types.PlanOperator) map[string][]types.PlanExpression {
	filters := make(map[string][]types.PlanExpression)

	InspectPlan(n, func(node types.PlanOperator) bool {
		switch nd := node.(type) {
		case *PlanOpFilter:
			fs := exprToRelationFilters(nd.Predicate)

			for k, exprs := range fs {
				filters[k] = append(filters[k], exprs...)
			}

		}
		return true
	})

	return filters
}

// exprToRelationFilters returns a map of relation name to filter expressions for the expression
// passed after the expression is split on AND.
func exprToRelationFilters(expr types.PlanExpression) map[string][]types.PlanExpression {
	filters := make(map[string][]types.PlanExpression)
	for _, expr := range splitOnAnd(expr) {
		var seenTables = make(map[string]bool)
		var lastTable string
		hasSubquery := false

		InspectExpression(expr, func(e types.PlanExpression) bool {
			f, ok := e.(*qualifiedRefPlanExpression)
			if ok {
				if !seenTables[f.tableName] {
					seenTables[f.tableName] = true
					lastTable = f.tableName
				}
			} else if _, isSubquery := e.(*subqueryPlanExpression); isSubquery {
				hasSubquery = true
				return false
			}

			return true
		})

		if len(seenTables) == 1 && !hasSubquery {
			filters[lastTable] = append(filters[lastTable], expr)
		}
	}

	return filters
}

// splitOnAnd breaks binops that are AND expressions into a list recursively
func splitOnAnd(expr types.PlanExpression) []types.PlanExpression {
	binOp, ok := expr.(*binOpPlanExpression)
	if !ok || binOp.op != parser.AND {
		return []types.PlanExpression{
			expr,
		}
	}

	return append(
		splitOnAnd(binOp.lhs),
		splitOnAnd(binOp.rhs)...,
	)
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

// the semantic for accessing a timequantum field is to use the subtable() table valued function in a join
// rewrite queries that use this pattern to use the appropriate PQL call
func tryToRewriteSubtableJoins(ctx context.Context, a *ExecutionPlanner, n types.PlanOperator, scope *OptimizerScope) (types.PlanOperator, bool, error) {
	//bail if there are no joins
	joins := getNestedLoopOperators(ctx, a, n, scope)
	if len(joins) == 0 {
		return n, true, nil
	}

	//get the projections, we're going to need them later
	projections := getPlanOpProjectionOperators(ctx, a, n, scope)

	return TransformPlanOp(n, func(node types.PlanOperator) (types.PlanOperator, bool, error) {
		switch nl := node.(type) {
		case *PlanOpNestedLoops:
			var tvf *PlanOpTableValuedFunction
			// bail if the join does not have a tvf as one of the operators
			tvftop, topok := nl.top.(*PlanOpTableValuedFunction)
			tvfbottom, bottomok := nl.bottom.(*PlanOpTableValuedFunction)

			//bail if both sides of the join are a tvf
			if topok && bottomok {
				return nl, true, nil
			}
			if topok {
				tvf = tvftop
			}
			if bottomok {
				tvf = tvfbottom
			}
			//if tvf == nil, then neither side is a tvf
			if tvf == nil {
				return nl, true, nil
			}

			//check it is the subtable() tvf
			tvfCall, ok := tvf.callExpr.(*callPlanExpression)
			if !ok {
				return nl, true, nil
			}
			if !strings.EqualFold(tvfCall.name, "subtable") {
				return nl, true, nil
			}

			// if there is no join condition, it's an extract; replace the 'value' reference
			// with a reference with the first argument and remove the join
			if nl.cond == nil {
				// get the first argument column from the tvf

				// for each of the projection operators, for each of the projections
				// transform each of the referenced values with a the first arg

				a.logger.Debugf("%T", projections)
			}

			// there is a join condition, make sure it is one that is permissible (range queries only?)
			a.logger.Debugf("%T", tvf)

			return nl, true, nil
		default:
			return nl, true, nil
		}
	})
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

func fixJoinFieldRefs(ctx context.Context, a *ExecutionPlanner, n types.PlanOperator, scope *OptimizerScope) (types.PlanOperator, bool, error) {
	return TransformPlanOp(n, func(node types.PlanOperator) (types.PlanOperator, bool, error) {
		switch n := node.(type) {
		case *PlanOpNestedLoops:
			_, _, err := fixFieldRefIndexesForOperator(ctx, a, n, scope)
			if err != nil {
				return nil, true, err
			}
			return n, true, nil
		default:
			return n, true, nil
		}
	})
}

func fixGroupByFieldRefs(ctx context.Context, a *ExecutionPlanner, n types.PlanOperator, scope *OptimizerScope) (types.PlanOperator, bool, error) {
	return TransformPlanOp(n, func(node types.PlanOperator) (types.PlanOperator, bool, error) {
		switch n := node.(type) {
		case *PlanOpGroupBy:
			_, _, err := fixFieldRefIndexesForOperator(ctx, a, n, scope)
			if err != nil {
				return nil, true, err
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

// inspects a plan op tree and returns a list (or error) of all the PlanOpProjection operators
func getPlanOpProjectionOperators(ctx context.Context, a *ExecutionPlanner, n types.PlanOperator, scope *OptimizerScope) []*PlanOpProjection {
	var projs []*PlanOpProjection
	InspectPlan(n, func(node types.PlanOperator) bool {
		switch nd := node.(type) {
		case *PlanOpProjection:
			projs = append(projs, nd)
			return false
		}
		return true
	})
	return projs
}

// inspects a plan op tree and returns a list (or error) of all the PlanOpNestedLoops operators
func getNestedLoopOperators(ctx context.Context, a *ExecutionPlanner, n types.PlanOperator, scope *OptimizerScope) []*PlanOpNestedLoops {
	var joins []*PlanOpNestedLoops
	InspectPlan(n, func(node types.PlanOperator) bool {
		switch nd := node.(type) {
		case *PlanOpNestedLoops:
			joins = append(joins, nd)
			return false
		}
		return true
	})
	return joins
}

func fixFieldRefIndexes(ctx context.Context, scope *OptimizerScope, a *ExecutionPlanner, schema types.Schema, exp types.PlanExpression) (types.PlanExpression, bool, error) {
	return TransformExpr(exp, func(e types.PlanExpression) (types.PlanExpression, bool, error) {
		switch e := e.(type) {
		case *qualifiedRefPlanExpression:
			for i, col := range schema {
				newIndex := i
				if e.Name() == col.ColumnName && e.tableName == col.RelationName {
					if newIndex != e.columnIndex {
						// update the column index
						return newQualifiedRefPlanExpression(e.tableName, e.columnName, newIndex, e.dataType), false, nil
					}
					return e, true, nil
				}
			}
			return nil, true, sql3.NewErrColumnNotFound(0, 0, e.Name())
		}
		return e, true, nil
	})
}

func fixFieldRefIndexesOnExpressions(ctx context.Context, scope *OptimizerScope, a *ExecutionPlanner, schema types.Schema, expressions ...types.PlanExpression) ([]types.PlanExpression, bool, error) {
	var result []types.PlanExpression
	var res types.PlanExpression
	var same bool
	var err error
	for i := range expressions {
		e := expressions[i]
		res, same, err = fixFieldRefIndexes(ctx, scope, a, schema, e)
		if err != nil {
			return nil, true, err
		}
		if !same {
			if result == nil {
				result = make([]types.PlanExpression, len(expressions))
				copy(result, expressions)
			}
			result[i] = res
		}
	}
	if len(result) > 0 {
		return result, false, nil
	}
	return expressions, true, nil
}

func fixFieldRefIndexesForOperator(ctx context.Context, a *ExecutionPlanner, node types.PlanOperator, scope *OptimizerScope) (types.PlanOperator, bool, error) {
	if _, ok := node.(types.ContainsExpressions); !ok {
		return node, true, nil
	}

	var schemas []types.Schema
	for _, child := range node.Children() {
		schemas = append(schemas, child.Schema())
	}

	if len(schemas) < 1 {
		return node, true, nil
	}

	n, sameC, err := TransformPlanOpExprsWithPlanOp(node, func(_ types.PlanOperator, e types.PlanExpression) (types.PlanExpression, bool, error) {
		for _, schema := range schemas {
			fixed, same, err := fixFieldRefIndexes(ctx, scope, a, schema, e)
			if err == nil {
				return fixed, same, nil
			}

			if errors.Is(err, sql3.ErrColumnNotFound) {
				continue
			}
			return nil, true, err
		}

		return e, true, nil
	})

	if err != nil {
		return nil, true, err
	}

	sameJ := true
	var cond types.PlanExpression
	switch j := n.(type) {
	case *PlanOpNestedLoops:
		cond, sameJ, err = fixFieldRefIndexes(ctx, scope, a, j.Schema(), j.cond)
		if err != nil {
			return nil, true, err
		}
		if !sameJ {
			n, err = j.WithUpdatedExpressions(cond)
			if err != nil {
				return nil, true, err
			}
		}
	}

	return n, sameC && sameJ, nil
}
