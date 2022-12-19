// Copyright 2021 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"fmt"
	"reflect"
	"strings"

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
	// fix expression references for having
	removeUnusedExtractColumnReferences,

	// fix expression references for having
	fixHavingReferences,

	// push down filter predicates as far as possible,
	pushdownFilters,

	// try to use a PlanOpPQLFilteredDelete instead of PlanOpPQLConstRowDelete
	tryToReplaceConstRowDeleteWithFilteredDelete,

	// if we have a group by that has one TableScanOperator,
	// try to use a PQL(multi)groupby operator instead
	tryToReplaceGroupByWithPQLGroupBy,

	// if we have a group by with no group by exprs that has
	// one TableScanOperator,  try to use a PQL aggregate operators instead
	tryToReplaceGroupByWithPQLAggregate,

	// if we have a subtable call on a timequantum type
	// take the join out and use the appropriate PQL operator instead
	tryToRewriteSubtableJoins,

	// update the columnIdx for all the qualified references in various operators
	fixFieldRefs,

	// update the columnIdx for all the references in the projections
	// based on the child operator for a projection
	fixProjectionReferences,

	// if the query has one TableScanOperator then push the top
	// expression down into that operator
	pushdownPQLTop,
}

// this will be used in future for symbol resolution when CTEs and subquery support matures
// and we need to introduce the concept of scope to symbol resolution
type OptimizerScope struct {
}

func dumpPlan(prefix []string, root types.PlanOperator, suffix string) {
	// DEBUG !!
	// for _, s := range prefix {
	// 	log.Println(s)
	// }
	// jplan := root.Plan()
	// a, _ := json.MarshalIndent(jplan, "", "    ")
	// log.Println(string(a))
	// log.Println()
	// DEBUG !!
}

// optimizePlan takes a plan from the compiler and executes a series of transforms on it to optimize it
func (p *ExecutionPlanner) optimizePlan(ctx context.Context, plan types.PlanOperator) (types.PlanOperator, error) {

	dumpPlan(
		[]string{"================================================================================", "plan pre-optimzation"},
		plan,
		"--------------------------------------------------------------------------------",
	)

	var err error
	var result = plan
	for _, ofunc := range optimizerFunctions {
		result, err = p.optimizeNode(ctx, result, ofunc)
		if err != nil {
			return nil, err
		}
	}

	dumpPlan(
		[]string{"================================================================================", "plan post-optimzation"},
		plan,
		"--------------------------------------------------------------------------------",
	)

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

// build a map of alias names to relations
func getRelationAliases(n types.PlanOperator, scope *OptimizerScope) (RelationAliasesMap, error) {
	var inspectErr error

	aliases := make(RelationAliasesMap)
	InspectPlan(n, func(node types.PlanOperator) bool {
		if node == nil {
			return false
		}

		switch node := node.(type) {
		case *PlanOpRelAlias:
			switch t := node.ChildOp.(type) {
			case *PlanOpPQLTableScan:
				inspectErr = aliases.addAlias(node, t)
			case *PlanOpSubquery:
				inspectErr = aliases.addAlias(node, t)
			default:
				inspectErr = sql3.NewErrInternalf("unexpected alias child type '%T", node.ChildOp)
			}
			return false

		case *PlanOpPQLTableScan:
			inspectErr = aliases.addAlias(node, node)
			return false

		}
		return true
	})

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

// when we compile and create a PlanOpPQLTableScan we just add all the columns to the underlying extract. This is a bad idea, since
// extracts are expensive, more so when we are askign for columns we don't actually need. This function removes those uneeded references.
func removeUnusedExtractColumnReferences(ctx context.Context, a *ExecutionPlanner, n types.PlanOperator, scope *OptimizerScope) (types.PlanOperator, bool, error) {
	// get all the qualifiedRefs across the plan
	// using a map to eliminate dupes and we don't
	// care about the order when iterating
	refs := make(map[string]*qualifiedRefPlanExpression)
	InspectOperatorExpressions(n, func(pe types.PlanExpression) bool {
		switch qref := pe.(type) {
		case *qualifiedRefPlanExpression:
			refs[qref.String()] = qref
			return false
		}
		return true
	})

	return TransformPlanOpWithParent(n, func(c ParentContext) bool { return true }, func(c ParentContext) (types.PlanOperator, bool, error) {
		switch thisNode := c.Operator.(type) {
		case *PlanOpPQLTableScan:

			newExtractList := make([]string, 0)

			// loop thru the extract list and make a new extract list
			// with just the columns we need
			alias, ok := c.Parent.(*PlanOpRelAlias)
			if ok {
				// handle the case where the parent is an alias
				for _, ex := range thisNode.columns {
					for _, ref := range refs {
						if (strings.EqualFold(ref.tableName, thisNode.tableName) || strings.EqualFold(ref.tableName, alias.alias)) && strings.EqualFold(ex, ref.columnName) {
							newExtractList = append(newExtractList, ex)
							break
						}
					}
				}
			} else {
				for _, ex := range thisNode.columns {
					for _, ref := range refs {
						if strings.EqualFold(ref.tableName, thisNode.tableName) && strings.EqualFold(ex, ref.columnName) {
							newExtractList = append(newExtractList, ex)
							break
						}
					}
				}
			}

			// newExtractList should now contain just the cols that are referenced
			return NewPlanOpPQLTableScan(a, thisNode.tableName, newExtractList), false, nil

		default:
			return thisNode, true, nil
		}
	})
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

func pushdownFiltersToFilterableRelations(ctx context.Context, a *ExecutionPlanner, tableNode types.PlanOperator, scope *OptimizerScope, filters *filterSet, tableAliases RelationAliasesMap) (types.PlanOperator, bool, error) {
	var table types.IdentifiableByName

	// only do this if it is a pql table scan
	switch rel := tableNode.(type) {
	case *PlanOpPQLTableScan:
		table = rel
	default:
		return tableNode, true, nil
	}

	// is the thing filterable?
	ft, ok := table.(types.FilteredRelation)
	if !ok {
		return tableNode, true, nil
	}

	// do we have any filters for this table? if not, bail...
	availableFilters := filters.availableFiltersForTable(table.Name())
	if len(availableFilters) == 0 {
		return tableNode, true, nil
	}

	tableFilters := make([]types.PlanExpression, 0)
	// can the filters be pushed down?
	for _, tf := range availableFilters {
		// try and generate a pql call graph, if we can't we can't push the filter down
		_, err := a.generatePQLCallFromExpr(ctx, tf)
		if err == nil {
			tableFilters = append(tableFilters, tf)
		}
	}
	// did we end up with any filters?
	if len(tableFilters) == 0 {
		return tableNode, true, nil
	}

	filters.markFiltersHandled(tableFilters...)

	// fix the field refs
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
	var table types.IdentifiableByName

	// only do this if it is a pql table scan
	switch rel := tableNode.(type) {
	case *PlanOpPQLTableScan:
		table = rel
	default:
		return tableNode, true, nil
	}

	// reposition any remaining filters for a table to directly above the table itself
	var pushedDownFilterExpression types.PlanExpression
	if tableFilters := filters.availableFiltersForTable(table.Name()); len(tableFilters) > 0 {
		filters.markFiltersHandled(tableFilters...)

		// fix the field refs
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
		return node, true, nil
	default:
		return nil, true, sql3.NewErrInternalf("unexpected op type '%T'", tableNode)
	}
}

func pushdownFilters(ctx context.Context, a *ExecutionPlanner, n types.PlanOperator, scope *OptimizerScope) (types.PlanOperator, bool, error) {

	tableAliases, err := getRelationAliases(n, scope)
	if err != nil {
		return nil, true, err
	}

	// push filter terms down into anything that supports being filtered directly
	pushdownFiltersForFilterableRelations := func(n *PlanOpFilter, filters *filterSet) (types.PlanOperator, bool, error) {
		return TransformPlanOpWithParent(n, filterPushdownChildSelector, func(c ParentContext) (types.PlanOperator, bool, error) {
			switch node := c.Operator.(type) {

			// for the filter in question remove any terms that have been pushed down
			case *PlanOpFilter:
				n, samePred, err := removePushedDownConditions(ctx, a, node, filters)
				if err != nil {
					return nil, true, err
				}
				return n, samePred, nil

			// PlanOpPQLTableScan supports being filtered, PlanOpRelAlias is included here as a "transparent" op
			case *PlanOpRelAlias, *PlanOpPQLTableScan:
				n, samePred, err := pushdownFiltersToFilterableRelations(ctx, a, node, scope, filters, tableAliases)
				if err != nil {
					return nil, true, err
				}
				return n, samePred, nil
			default:
				return node, true, nil
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
				return n, false, nil

			case *PlanOpRelAlias, *PlanOpPQLTableScan:
				_, same, err := pushdownFiltersToAboveRelation(ctx, a, node, scope, filters)
				if err != nil {
					return nil, true, err
				}
				if same {
					return node, true, nil
				}
				return node, false, nil
			default:
				return node, true, nil
			}
		})
	}

	// look for filter ops and push the conditions within them down to things that can be filtered
	return TransformPlanOp(n, func(node types.PlanOperator) (types.PlanOperator, bool, error) {
		switch thisNode := node.(type) {
		case *PlanOpFilter:

			// get the filter conditions from this filter in a map by table
			filtersByTable := getFiltersByRelation(n)

			// make a struct to hold the expression for this filter, the broken up filter conditions
			// and a map of alias name to relations
			filters := newFilterSet(thisNode.Predicate, filtersByTable, tableAliases)

			// first push down filters to any op that supports a filter
			newNode, sameA, err := pushdownFiltersForFilterableRelations(thisNode, filters)
			if err != nil {
				return nil, true, err
			}

			// second push down filters as close as possible to the relations they apply to
			var sameB bool
			newNode, sameB, err = pushdownFiltersCloseToRelations(newNode, filters)
			if err != nil {
				return nil, true, err
			}
			return newNode, sameA && sameB, nil

		default:
			return node, true, nil
		}
	})
}

// getFiltersByRelation returns a map of relations name to filter expressions for the op provided
func getFiltersByRelation(n types.PlanOperator) map[string][]types.PlanExpression {
	filters := make(map[string][]types.PlanExpression)

	InspectPlan(n, func(node types.PlanOperator) bool {
		switch thisNode := node.(type) {
		case *PlanOpFilter:
			fs := exprToRelationFilters(thisNode.Predicate)

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
			switch thisExpr := e.(type) {
			case *qualifiedRefPlanExpression:
				if !seenTables[thisExpr.tableName] {
					seenTables[thisExpr.tableName] = true
					lastTable = thisExpr.tableName
				}
			case *subqueryPlanExpression:
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
	joins, err := hasJoins(ctx, a, n, scope)
	if err != nil {
		return nil, false, err
	}
	if joins {
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

func tryToReplaceConstRowDeleteWithFilteredDelete(ctx context.Context, a *ExecutionPlanner, n types.PlanOperator, scope *OptimizerScope) (types.PlanOperator, bool, error) {
	return TransformPlanOp(n, func(node types.PlanOperator) (types.PlanOperator, bool, error) {
		switch node := node.(type) {
		case *PlanOpPQLConstRowDelete:
			switch child := node.ChildOp.(type) {
			case *PlanOpPQLTableScan:
				if child.filter != nil {
					_, err := a.generatePQLCallFromExpr(ctx, child.filter)
					if err == nil {
						return NewPlanOpPQLFilteredDelete(a, node.tableName, child.filter), false, nil
					}
				}
				return node, true, nil
			default:
				return node, true, nil
			}
		default:
			return node, true, nil
		}
	})
}

func tryToReplaceGroupByWithPQLGroupBy(ctx context.Context, a *ExecutionPlanner, n types.PlanOperator, scope *OptimizerScope) (types.PlanOperator, bool, error) {
	//bail if there are any joins
	joins, err := hasJoins(ctx, a, n, scope)
	if err != nil {
		return nil, false, err
	}
	if joins {
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
	// bail if there are any joins
	joins, err := hasJoins(ctx, a, n, scope)
	if err != nil {
		return nil, false, err
	}
	if joins {
		return n, true, nil
	}

	// get a list of tables that have projections as parents
	var tables []*PlanOpPQLTableScan
	_, _, err = TransformPlanOpWithParent(n, func(c ParentContext) bool { return true }, func(c ParentContext) (types.PlanOperator, bool, error) {
		parent := c.Parent
		node := c.Operator

		switch thisNode := node.(type) {
		case *PlanOpPQLTableScan:
			switch parent.(type) {
			case *PlanOpProjection:
				tables = append(tables, thisNode)
			}

		}
		return node, true, nil

	})
	if err != nil {
		return nil, false, err
	}

	// only do this if we have one TableScanOperator
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

// fixes references for a projection op depending on child
func fixProjectionReferences(ctx context.Context, a *ExecutionPlanner, n types.PlanOperator, scope *OptimizerScope) (types.PlanOperator, bool, error) {
	return TransformPlanOp(n, func(node types.PlanOperator) (types.PlanOperator, bool, error) {
		switch thisNode := node.(type) {
		case *PlanOpProjection:
			switch childOp := thisNode.ChildOp.(type) {

			case *PlanOpGroupBy, *PlanOpHaving, *PlanOpPQLGroupBy, *PlanOpPQLMultiAggregate, *PlanOpPQLMultiGroupBy:
				childSchema := childOp.Schema()

				for idx, pj := range thisNode.Projections {
					expr, _, err := TransformExpr(pj, func(e types.PlanExpression) (types.PlanExpression, bool, error) {
						switch thisAggregate := e.(type) {
						case types.Aggregable:
							// if we have a Aggregable, the AggExpression() will be a qualified ref
							// given we are in the context of a PlanOpProjection with a PlanOpGroupBy/Having
							// we can use the ordinal position of the projection as the column index
							for idx, sc := range childSchema {
								if strings.EqualFold(thisAggregate.String(), sc.ColumnName) {
									ae := newQualifiedRefPlanExpression("", "", idx, e.Type())
									return ae, false, nil
								}
							}
							return nil, true, sql3.NewErrColumnNotFound(0, 0, thisAggregate.String())

						case *qualifiedRefPlanExpression:
							for idx, sc := range childSchema {
								if matchesSchema(thisAggregate, sc) {
									if idx != thisAggregate.columnIndex {
										// update the column index
										return newQualifiedRefPlanExpression(thisAggregate.tableName, thisAggregate.columnName, idx, thisAggregate.dataType), false, nil
									}
									return thisAggregate, true, nil
								}
							}
							return nil, true, sql3.NewErrColumnNotFound(0, 0, thisAggregate.String())

						default:
							return e, true, nil
						}
					}, func(parentExpr, childExpr types.PlanExpression) bool {
						// if the parent is an aggregable, and the child is a qualified ref
						// we will skip, because the qualified ref should have already been handled in
						// fixFieldRefs
						switch parentExpr.(type) {
						case types.Aggregable:
							switch childExpr.(type) {
							case *qualifiedRefPlanExpression:
								return false
							}
						}
						return true
					})
					if err != nil {
						return thisNode, true, err
					}
					thisNode.Projections[idx] = expr
				}
				return thisNode, false, nil

			// everything else that can be a child of projection
			case *PlanOpRelAlias, *PlanOpFilter, *PlanOpPQLTableScan, *PlanOpNestedLoops:
				exprs, same, err := fixFieldRefIndexesOnExpressions(ctx, scope, a, childOp.Schema(), thisNode.Projections...)
				if err != nil {
					return thisNode, true, err
				}
				thisNode.Projections = exprs
				return thisNode, same, err

			default:
				return thisNode, true, nil
			}

		default:
			return thisNode, true, nil
		}
	})
}

func fixFieldRefs(ctx context.Context, a *ExecutionPlanner, n types.PlanOperator, scope *OptimizerScope) (types.PlanOperator, bool, error) {
	return TransformPlanOp(n, func(node types.PlanOperator) (types.PlanOperator, bool, error) {
		switch thisNode := node.(type) {
		case *PlanOpFilter:
			// fix references for the expressions referenced in the filter predicate expression
			schema := thisNode.Schema()
			expressions := thisNode.Expressions()
			fixed, same, err := fixFieldRefIndexesOnExpressions(ctx, scope, a, schema, expressions...)
			if err != nil {
				return nil, true, err
			}
			newNode, err := thisNode.WithUpdatedExpressions(fixed...)
			if err != nil {
				return nil, true, err
			}
			return newNode, same, nil

		case *PlanOpNestedLoops:
			// fix references for the expressions referenced in the join condition expression
			schema := thisNode.Schema()
			expressions := thisNode.Expressions()
			fixed, same, err := fixFieldRefIndexesOnExpressions(ctx, scope, a, schema, expressions...)
			if err != nil {
				return nil, true, err
			}
			newNode, err := thisNode.WithUpdatedExpressions(fixed...)
			if err != nil {
				return nil, true, err
			}
			return newNode, same, nil

		case *PlanOpGroupBy:
			// fix references for the expressions referenced in the aggregate functions or the group by clause
			schema := thisNode.ChildOp.Schema()
			aggregateExpressions := thisNode.Aggregates
			fixedAggregateExpressions, aggregateSame, err := fixFieldRefIndexesOnExpressions(ctx, scope, a, schema, aggregateExpressions...)
			if err != nil {
				return nil, true, err
			}

			groupByExpressions := thisNode.GroupByExprs
			fixedGroupByExpressions, groupBySame, err := fixFieldRefIndexesOnExpressions(ctx, scope, a, schema, groupByExpressions...)
			if err != nil {
				return nil, true, err
			}

			newNode := NewPlanOpGroupBy(fixedAggregateExpressions, fixedGroupByExpressions, thisNode.ChildOp)
			newNode.warnings = append(newNode.warnings, thisNode.warnings...)
			return newNode, aggregateSame && groupBySame, nil

		case *PlanOpPQLMultiGroupBy:

			schema := thisNode.operators[0].Schema()
			for idx, op := range thisNode.operators {
				if idx > 0 {
					opSchema := op.Schema()
					last := opSchema[len(opSchema)-1]
					schema = append(schema, last)
				}
			}
			expressions := thisNode.Expressions()
			fixed, same, err := fixFieldRefIndexesOnExpressions(ctx, scope, a, schema, expressions...)
			if err != nil {
				return nil, true, err
			}
			newNode, err := thisNode.WithUpdatedExpressions(fixed...)
			if err != nil {
				return nil, true, err
			}
			return newNode, same, nil

		default:
			return node, true, nil
		}
	})
}

func fixHavingReferences(ctx context.Context, a *ExecutionPlanner, n types.PlanOperator, scope *OptimizerScope) (types.PlanOperator, bool, error) {
	return TransformPlanOp(n, func(node types.PlanOperator) (types.PlanOperator, bool, error) {
		switch thisNode := node.(type) {
		case *PlanOpHaving:
			// fix references for the expressions referenced in the having predicate expression
			schema := thisNode.Schema()
			expressions := thisNode.Expressions()
			fixed, same, err := fixFieldRefIndexesOnExpressionsForHaving(ctx, scope, a, schema, expressions...)
			if err != nil {
				return nil, true, err
			}
			newNode, err := thisNode.WithUpdatedExpressions(fixed...)
			if err != nil {
				return nil, true, err
			}
			return newNode, same, nil

		default:
			return node, true, nil
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

// inspects a plan op tree and returns false (or error) if there are read join operators
func hasJoins(ctx context.Context, a *ExecutionPlanner, n types.PlanOperator, scope *OptimizerScope) (bool, error) {
	// assume false
	result := false
	InspectPlan(n, func(node types.PlanOperator) bool {
		switch node.(type) {
		case *PlanOpNestedLoops:
			result = true
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

// for a list of expressions and an operator schema, fix the references for any qualifiedRef expressions
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

func matchesSchema(qualifiedRef *qualifiedRefPlanExpression, col *types.PlannerColumn) bool {
	if strings.EqualFold(qualifiedRef.Name(), col.ColumnName) {
		if len(qualifiedRef.tableName) == 0 { // do we have a qualifier?
			return true
		}
		if qualifiedRef.tableName == col.RelationName || qualifiedRef.tableName == col.AliasName {
			return true
		}
	}
	return false
}

func fixFieldRefIndexes(ctx context.Context, scope *OptimizerScope, a *ExecutionPlanner, schema types.Schema, exp types.PlanExpression) (types.PlanExpression, bool, error) {
	return TransformExpr(exp, func(e types.PlanExpression) (types.PlanExpression, bool, error) {
		switch typedExpr := e.(type) {
		case *qualifiedRefPlanExpression:
			for i, col := range schema {
				newIndex := i
				if matchesSchema(typedExpr, col) {
					if newIndex != typedExpr.columnIndex {
						// update the column index
						return newQualifiedRefPlanExpression(typedExpr.tableName, typedExpr.columnName, newIndex, typedExpr.dataType), false, nil
					}
					return e, true, nil
				}
			}
			return nil, true, sql3.NewErrColumnNotFound(0, 0, typedExpr.Name())
		}
		return e, true, nil
	}, func(parentExpr, childExpr types.PlanExpression) bool {
		return true
	})
}

// for a list of expressions and an operator schema, fix the references for any qualifiedRef expressions
func fixFieldRefIndexesOnExpressionsForHaving(ctx context.Context, scope *OptimizerScope, a *ExecutionPlanner, schema types.Schema, expressions ...types.PlanExpression) ([]types.PlanExpression, bool, error) {
	var result []types.PlanExpression
	var res types.PlanExpression
	var same bool
	var err error
	for i := range expressions {
		e := expressions[i]
		res, same, err = fixFieldRefIndexesForHaving(ctx, scope, a, schema, e)
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

func fixFieldRefIndexesForHaving(ctx context.Context, scope *OptimizerScope, a *ExecutionPlanner, schema types.Schema, exp types.PlanExpression) (types.PlanExpression, bool, error) {
	return TransformExpr(exp, func(e types.PlanExpression) (types.PlanExpression, bool, error) {
		switch typedExpr := e.(type) {
		case *sumPlanExpression, *countPlanExpression, *countDistinctPlanExpression,
			*avgPlanExpression, *minPlanExpression, *maxPlanExpression,
			*percentilePlanExpression:
			for i, col := range schema {
				if strings.EqualFold(typedExpr.String(), col.ColumnName) {
					e := newQualifiedRefPlanExpression("", "", i, typedExpr.Type())
					return e, false, nil
				}
			}
			return nil, true, sql3.NewErrColumnNotFound(0, 0, typedExpr.String())

		case *qualifiedRefPlanExpression:
			for i, col := range schema {
				newIndex := i
				if matchesSchema(typedExpr, col) {
					if newIndex != typedExpr.columnIndex {
						// update the column index
						return newQualifiedRefPlanExpression(typedExpr.tableName, typedExpr.columnName, newIndex, typedExpr.dataType), false, nil
					}
					return e, true, nil
				}
			}
			return nil, true, sql3.NewErrColumnNotFound(0, 0, typedExpr.Name())
		}
		return e, true, nil
	}, func(parentExpr, childExpr types.PlanExpression) bool {
		switch parentExpr.(type) {
		case *sumPlanExpression, *countPlanExpression, *countDistinctPlanExpression,
			*avgPlanExpression, *minPlanExpression, *maxPlanExpression,
			*percentilePlanExpression:
			return false
		default:
			return true
		}
	})
}
