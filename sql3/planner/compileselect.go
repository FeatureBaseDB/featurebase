// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"strings"

	pilosa "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/sql3"
	"github.com/featurebasedb/featurebase/v3/sql3/parser"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
	"github.com/pkg/errors"
)

// compileSelectStatment compiles a parser.SelectStatment AST into a PlanOperator
func (p *ExecutionPlanner) compileSelectStatement(stmt *parser.SelectStatement, isSubquery bool) (types.PlanOperator, error) {
	query := NewPlanOpQuery(p, NewPlanOpNullTable(), p.sql)

	aggregates := make([]types.PlanExpression, 0)

	// handle projections
	projections := make([]types.PlanExpression, 0)
	for _, c := range stmt.Columns {
		planExpr, err := p.compileExpr(c.Expr)
		if err != nil {
			return nil, errors.Wrap(err, "planning select column expression")
		}
		if c.Alias != nil {
			planExpr = newAliasPlanExpression(c.Alias.Name, planExpr)
		}
		projections = append(projections, planExpr)
		aggregates = p.gatherExprAggregates(planExpr, aggregates)
	}

	// group by clause.
	groupByExprs := make([]types.PlanExpression, 0)
	for _, expr := range stmt.GroupByExprs {
		switch expr := expr.(type) {
		case *parser.QualifiedRef:
			groupByExprs = append(groupByExprs, newQualifiedRefPlanExpression(expr.Table.Name, expr.Column.Name, expr.ColumnIndex, expr.DataType()))
		default:
			return nil, sql3.NewErrInternalf("unsupported expression type in GROUP BY clause: %T", expr)
		}
	}
	var err error

	// handle the where clause
	where, err := p.compileExpr(stmt.WhereExpr)
	if err != nil {
		return nil, err
	}

	// source expression
	source, err := p.compileSource(query, stmt.Source)
	if err != nil {
		return nil, err
	}

	// if we did have a where, insert the filter op
	if where != nil {
		aggregates = p.gatherExprAggregates(where, aggregates)
		source = NewPlanOpFilter(p, where, source)
	}

	// handle the having clause
	having, err := p.compileExpr(stmt.HavingExpr)
	if err != nil {
		return nil, err
	}

	if having != nil {
		// gather aggregates
		aggregates = p.gatherExprAggregates(having, aggregates)

		// make sure that any references are columns in the group by list, or in an aggregate

		// make a list of group by expresssions
		aggregateAndGroupByExprs := make([]types.PlanExpression, 0)
		aggregateAndGroupByExprs = append(aggregateAndGroupByExprs, groupByExprs...)
		// add to that the refs used by all the aggregates..
		for _, agg := range aggregates {
			InspectExpression(agg, func(expr types.PlanExpression) bool {
				switch ex := expr.(type) {
				case *sumPlanExpression, *countPlanExpression, *countDistinctPlanExpression,
					*avgPlanExpression, *minPlanExpression, *maxPlanExpression,
					*percentilePlanExpression:
					ch := ex.Children()
					// first arg is always the ref
					aggregateAndGroupByExprs = append(aggregateAndGroupByExprs, ch[0])
					return false
				}
				return true
			})
		}

		// inspect the having expression, build a list of references that are not
		// part of an aggregate
		havingReferences := make([]*qualifiedRefPlanExpression, 0)
		InspectExpression(having, func(expr types.PlanExpression) bool {
			switch ex := expr.(type) {
			case *sumPlanExpression, *countPlanExpression, *countDistinctPlanExpression,
				*avgPlanExpression, *minPlanExpression, *maxPlanExpression,
				*percentilePlanExpression:
				return false
			case *qualifiedRefPlanExpression:
				havingReferences = append(havingReferences, ex)
				return false
			}
			return true
		})

		// check the list of references against the aggregate and group by expressions
		for _, nae := range havingReferences {
			found := false
			for _, pe := range aggregateAndGroupByExprs {
				gbe, ok := pe.(*qualifiedRefPlanExpression)
				if !ok {
					continue
				}
				if strings.EqualFold(nae.columnName, gbe.columnName) &&
					strings.EqualFold(nae.tableName, gbe.tableName) {
					found = true
					break
				}
			}
			if !found {
				return nil, sql3.NewErrInvalidUngroupedColumnReferenceInHaving(0, 0, nae.columnName)
			}
		}
	}

	// do we have straight projection or a group by?
	var compiledOp types.PlanOperator
	if len(aggregates) > 0 {
		//check that any projections that are not aggregates are in the group by list
		nonAggregateReferences := make([]*qualifiedRefPlanExpression, 0)
		for _, expr := range projections {
			InspectExpression(expr, func(expr types.PlanExpression) bool {
				switch ex := expr.(type) {
				case *sumPlanExpression, *countPlanExpression, *countDistinctPlanExpression,
					*avgPlanExpression, *minPlanExpression, *maxPlanExpression,
					*percentilePlanExpression:
					//return false for these, because thats as far down we want to inspect
					return false
				case *qualifiedRefPlanExpression:
					nonAggregateReferences = append(nonAggregateReferences, ex)
					return false
				}
				return true
			})
		}

		for _, nae := range nonAggregateReferences {
			found := false
			for _, pe := range groupByExprs {
				gbe, ok := pe.(*qualifiedRefPlanExpression)
				if !ok {
					continue
				}
				if strings.EqualFold(nae.columnName, gbe.columnName) &&
					strings.EqualFold(nae.tableName, gbe.tableName) {
					found = true
					break
				}
			}
			if !found {
				return nil, sql3.NewErrInvalidUngroupedColumnReference(0, 0, nae.columnName)
			}
		}
		var groupByOp types.PlanOperator
		groupByOp = NewPlanOpGroupBy(aggregates, groupByExprs, source)
		if having != nil {
			groupByOp = NewPlanOpHaving(p, having, groupByOp)
		}
		compiledOp = NewPlanOpProjection(projections, groupByOp)
	} else {
		compiledOp = NewPlanOpProjection(projections, source)
	}

	// handle order by
	if len(stmt.OrderingTerms) > 0 {
		orderByFields := make([]*OrderByExpression, 0)
		for _, ot := range stmt.OrderingTerms {
			index, err := p.compileOrderingTermExpr(ot.X)
			if err != nil {
				return nil, err
			}
			// get the data type from the projection
			projDataType := projections[index].Type()

			// don't let a sort happen on something unsortable right now
			switch projDataType.(type) {
			case *parser.DataTypeStringSet, *parser.DataTypeIDSet:
				return nil, sql3.NewErrExpectedSortableExpression(0, 0, projDataType.TypeDescription())
			}

			f := &OrderByExpression{
				Index:    index,
				ExprType: projDataType,
			}
			f.Order = orderByAsc
			if ot.Desc.IsValid() {
				f.Order = orderByDesc
			}
			orderByFields = append(orderByFields, f)
		}
		compiledOp = NewPlanOpOrderBy(orderByFields, compiledOp)
	}

	// insert the top operator if it exists
	if stmt.Top.IsValid() {
		topExpr, err := p.compileExpr(stmt.TopExpr)
		if err != nil {
			return nil, err
		}
		compiledOp = NewPlanOpTop(topExpr, compiledOp)
	}

	// handle distinct
	if stmt.Distinct.IsValid() {
		compiledOp = NewPlanOpDistinct(p, compiledOp)
	}

	// if it is a subquery, don't wrap in a PlanOpQuery
	if isSubquery {
		return compiledOp, nil
	}
	children := []types.PlanOperator{
		compiledOp,
	}
	return query.WithChildren(children...)
}

func (p *ExecutionPlanner) gatherExprAggregates(expr types.PlanExpression, aggregates []types.PlanExpression) []types.PlanExpression {
	result := aggregates
	InspectExpression(expr, func(expr types.PlanExpression) bool {
		switch ex := expr.(type) {
		case *sumPlanExpression, *countPlanExpression, *countDistinctPlanExpression,
			*avgPlanExpression, *minPlanExpression, *maxPlanExpression,
			*percentilePlanExpression:
			found := false
			for _, ag := range result {
				//compare based on string representation
				if strings.EqualFold(ag.String(), ex.String()) {
					found = true
					break
				}
			}
			if !found {
				result = append(result, ex)
			}
			// return false because thats as far down we want to inspect
			return false
		}
		return true
	})
	return result
}

func (p *ExecutionPlanner) compileSource(scope *PlanOpQuery, source parser.Source) (types.PlanOperator, error) {
	if source == nil {
		return NewPlanOpNullTable(), nil
	}

	switch sourceExpr := source.(type) {
	case *parser.JoinClause:
		scope.AddWarning("🦖 here there be dragons! JOINS are experimental.")

		// what sort of join is it?
		jType := joinTypeInner
		if sourceExpr.Operator.Left.IsValid() {
			jType = joinTypeLeft
		} else if sourceExpr.Operator.Right.IsValid() {
			return nil, sql3.NewErrUnsupported(sourceExpr.Operator.Right.Line, sourceExpr.Operator.Right.Column, false, "RIGHT join types")
		} else if sourceExpr.Operator.Full.IsValid() {
			return nil, sql3.NewErrUnsupported(sourceExpr.Operator.Full.Line, sourceExpr.Operator.Full.Column, false, "FULL join types")
		}

		// handle the join condition
		var joinCondition types.PlanExpression
		if sourceExpr.Constraint == nil {
			scope.AddWarning("⚠️  cartesian products are never a good idea - are you missing a join constraint?")
			joinCondition = nil
		} else {
			switch join := sourceExpr.Constraint.(type) {
			case *parser.OnConstraint:
				expr, err := p.compileExpr(join.X)
				if err != nil {
					return nil, err
				}
				joinCondition = expr
			default:
				return nil, sql3.NewErrInternalf("unexecpted constraint type '%T'", join)
			}
		}

		// compile top and bottom child ops
		topOp, err := p.compileSource(scope, sourceExpr.X)
		if err != nil {
			return nil, err
		}
		bottomOp, err := p.compileSource(scope, sourceExpr.Y)
		if err != nil {
			return nil, err
		}
		return NewPlanOpNestedLoops(topOp, bottomOp, jType, joinCondition), nil

	case *parser.QualifiedTableName:

		tableName := parser.IdentName(sourceExpr.Name)

		// doing this check here because we don't have a 'system' flag that exists in the FB schema
		st, ok := systemTables[strings.ToLower(tableName)]
		if ok {
			var op types.PlanOperator
			op = NewPlanOpSystemTable(p, st)
			if st.requiresFanout {
				op = NewPlanOpFanout(p, op)
			}
			if sourceExpr.Alias != nil {
				aliasName := parser.IdentName(sourceExpr.Alias)
				return NewPlanOpRelAlias(aliasName, op), nil
			}
			return op, nil

		}
		// get all the columns for this table - we will eliminate unused ones
		// later on in the optimizer
		extractColumns := make([]string, 0)

		for _, oc := range sourceExpr.OutputColumns {
			extractColumns = append(extractColumns, oc.ColumnName)
		}

		if sourceExpr.Alias != nil {
			aliasName := parser.IdentName(sourceExpr.Alias)

			return NewPlanOpRelAlias(aliasName, NewPlanOpPQLTableScan(p, tableName, extractColumns)), nil
		}
		return NewPlanOpPQLTableScan(p, tableName, extractColumns), nil

	case *parser.TableValuedFunction:
		callExpr, err := p.compileCallExpr(sourceExpr.Call)
		if err != nil {
			return nil, err
		}

		if sourceExpr.Alias != nil {
			aliasName := parser.IdentName(sourceExpr.Alias)
			return NewPlanOpRelAlias(aliasName, NewPlanOpTableValuedFunction(p, callExpr)), nil
		}

		return NewPlanOpTableValuedFunction(p, callExpr), nil

	case *parser.ParenSource:
		if sourceExpr.Alias != nil {
			aliasName := parser.IdentName(sourceExpr.Alias)
			op, err := p.compileSource(scope, sourceExpr.X)
			if err != nil {
				return nil, err
			}
			return NewPlanOpRelAlias(aliasName, op), nil
		}

		return p.compileSource(scope, sourceExpr.X)

	case *parser.SelectStatement:
		subQuery, err := p.compileSelectStatement(sourceExpr, true)
		if err != nil {
			return nil, err
		}
		return NewPlanOpSubquery(subQuery), nil

	default:
		return nil, sql3.NewErrInternalf("unexpected source type: %T", source)
	}
}

func (p *ExecutionPlanner) analyzeSource(source parser.Source, scope parser.Statement) (parser.Source, error) {
	if source == nil {
		return nil, nil
	}
	switch source := source.(type) {
	case *parser.JoinClause:
		x, err := p.analyzeSource(source.X, scope)
		if err != nil {
			return nil, err
		}
		y, err := p.analyzeSource(source.Y, scope)
		if err != nil {
			return nil, err
		}
		if source.Constraint != nil {
			switch join := source.Constraint.(type) {
			case *parser.OnConstraint:
				ex, err := p.analyzeExpression(join.X, scope)
				if err != nil {
					return nil, err
				}
				join.X = ex
			default:
				return nil, sql3.NewErrInternalf("unexpected constraint type '%T'", join)
			}
		}
		source.X = x
		source.Y = y
		return source, nil

	case *parser.ParenSource:
		x, err := p.analyzeSource(source.X, scope)
		if err != nil {
			return nil, err
		}
		source.X = x
		return source, nil

	case *parser.QualifiedTableName:

		objectName := parser.IdentName(source.Name)

		// check views first
		view, err := p.getViewByName(objectName)
		if err != nil {
			return nil, err
		}

		// if view is not null, it exists
		if view != nil {
			// parse the select statement
			ast, err := parser.NewParser(strings.NewReader(view.statement)).ParseStatement()
			if err != nil {
				return nil, err
			}
			sel, ok := ast.(*parser.SelectStatement)
			if !ok {
				return nil, sql3.NewErrInternalf("unexpected ast type")
			}
			// analyze the select statement
			expr, err := p.analyzeSelectStatement(sel)
			if err != nil {
				return nil, err
			}
			selExpr, ok := expr.(*parser.SelectStatement)
			if !ok {
				return nil, sql3.NewErrInternalf("unexpected analyzed type")
			}

			// rewrite as a paren source with the select
			paren := &parser.ParenSource{
				X:     selExpr,
				Alias: source.Alias,
			}
			return paren, nil
		}

		// check table exists
		tname := dax.TableName(objectName)
		tbl, err := p.schemaAPI.TableByName(context.Background(), tname)
		if err != nil {
			if isTableNotFoundError(err) {
				return nil, sql3.NewErrTableOrViewNotFound(source.Name.NamePos.Line, source.Name.NamePos.Column, objectName)
			}
			return nil, err
		}

		// populate the output columns from the source
		for i, fld := range tbl.Fields {
			soc := &parser.SourceOutputColumn{
				TableName:   objectName,
				ColumnName:  string(fld.Name),
				ColumnIndex: i,
				Datatype:    fieldSQLDataType(pilosa.FieldToFieldInfo(fld)),
			}
			source.OutputColumns = append(source.OutputColumns, soc)
		}

		return source, nil

	case *parser.TableValuedFunction:
		// check it actually is a table valued function - we only support one right now; subtable()
		switch strings.ToUpper(source.Name.Name) {
		case "SUBTABLE":
			_, err := p.analyzeCallExpression(source.Call, scope)
			if err != nil {
				return nil, err
			}

			tvfResultType, ok := source.Call.ResultDataType.(*parser.DataTypeSubtable)
			if !ok {
				return nil, sql3.NewErrInternalf("unexepected tvf return type")
			}

			// populate the output columns from the source
			for idx, member := range tvfResultType.Columns {
				soc := &parser.SourceOutputColumn{
					TableName:   "", // TODO (pok) use the tq column actually referenced as the table name
					ColumnName:  member.Name,
					ColumnIndex: idx,
					Datatype:    member.DataType,
				}
				source.OutputColumns = append(source.OutputColumns, soc)
			}

		default:
			return nil, sql3.NewErrInternalf("table valued function expected")
		}

		return source, nil

	case *parser.SelectStatement:
		expr, err := p.analyzeSelectStatement(source)
		if err != nil {
			return nil, err
		}
		selExpr, ok := expr.(*parser.SelectStatement)
		if !ok {
			return nil, sql3.NewErrInternalf("unexpected analyzed type")
		}
		return selExpr, nil

	default:
		return nil, sql3.NewErrInternalf("unexpected source type: %T", source)
	}
}

func (p *ExecutionPlanner) analyzeSelectStatement(stmt *parser.SelectStatement) (parser.Expr, error) {
	// analyze source first - needed for name resolution
	source, err := p.analyzeSource(stmt.Source, stmt)
	if err != nil {
		return nil, err
	}
	stmt.Source = source

	if err := p.analyzeSelectStatementWildcards(stmt); err != nil {
		return nil, err
	}

	for _, col := range stmt.Columns {
		expr, err := p.analyzeExpression(col.Expr, stmt)
		if err != nil {
			return nil, err
		}
		if expr != nil {
			col.Expr = expr
		}
	}

	expr, err := p.analyzeExpression(stmt.TopExpr, stmt)
	if err != nil {
		return nil, err
	}
	if expr != nil {
		if !(expr.IsLiteral() && typeIsInteger(expr.DataType())) {
			return nil, sql3.NewErrIntegerLiteral(stmt.TopExpr.Pos().Line, stmt.TopExpr.Pos().Column)
		}
		stmt.TopExpr = expr
	}

	expr, err = p.analyzeExpression(stmt.HavingExpr, stmt)
	if err != nil {
		return nil, err
	}
	stmt.HavingExpr = expr

	expr, err = p.analyzeExpression(stmt.WhereExpr, stmt)
	if err != nil {
		return nil, err
	}
	stmt.WhereExpr = expr

	for i, g := range stmt.GroupByExprs {
		expr, err = p.analyzeExpression(g, stmt)
		if err != nil {
			return nil, err
		}
		if expr != nil {
			stmt.GroupByExprs[i] = expr
		}
	}

	expr, err = p.analyzeExpression(stmt.HavingExpr, stmt)
	if err != nil {
		return nil, err
	}
	if expr != nil {
		stmt.HavingExpr = expr
	}

	for _, term := range stmt.OrderingTerms {
		expr, err := p.analyzeOrderingTermExpression(term.X, stmt)
		if err != nil {
			return nil, err
		}
		term.X = expr
	}

	return stmt, nil
}

func (p *ExecutionPlanner) analyzeSelectStatementWildcards(stmt *parser.SelectStatement) error {
	if !stmt.HasWildcard() {
		return nil
	}

	// replace wildcards with column references
	newColumns := make([]*parser.ResultColumn, 0, len(stmt.Columns))
	for _, col := range stmt.Columns {

		//handle the case of unqualified *
		if col.Star.IsValid() {

			cols, err := p.columnsFromSource(stmt.Source)
			if err != nil {
				return err
			}
			newColumns = append(newColumns, cols...)

		} else {
			//handle the case of a qualified ref with a *
			if ref, ok := col.Expr.(*parser.QualifiedRef); ok && ref.Star.IsValid() {
				refName := parser.IdentName(ref.Table)
				src := stmt.Source.SourceFromAlias(refName)
				if src == nil {
					return sql3.NewErrTableNotFound(ref.Table.NamePos.Line, ref.Table.NamePos.Column, refName)
				}

				cols, err := p.columnsFromSource(src)
				if err != nil {
					return err
				}
				newColumns = append(newColumns, cols...)

			} else {
				//add the column as is...
				newColumns = append(newColumns, col)
			}
		}
	}
	stmt.Columns = newColumns

	return nil
}

func (p *ExecutionPlanner) columnsFromSource(source parser.Source) ([]*parser.ResultColumn, error) {
	result := []*parser.ResultColumn{}

	switch src := source.(type) {
	case *parser.JoinClause:
		return nil, sql3.NewErrInternal("joins are not currently supported")
	case *parser.ParenSource:
		for _, oc := range src.PossibleOutputColumns() {
			result = append(result, &parser.ResultColumn{
				Expr: &parser.QualifiedRef{
					Table:       &parser.Ident{Name: oc.TableName},
					Column:      &parser.Ident{Name: oc.ColumnName},
					ColumnIndex: oc.ColumnIndex,
				},
			})
		}
		return result, nil

	case *parser.QualifiedTableName:
		for _, oc := range src.PossibleOutputColumns() {
			result = append(result, &parser.ResultColumn{
				Expr: &parser.QualifiedRef{
					Table:       &parser.Ident{Name: oc.TableName},
					Column:      &parser.Ident{Name: oc.ColumnName},
					ColumnIndex: oc.ColumnIndex,
				},
			})
		}
		return result, nil

	case *parser.SelectStatement:
		for _, oc := range src.PossibleOutputColumns() {
			result = append(result, &parser.ResultColumn{
				Expr: &parser.QualifiedRef{
					Table:       &parser.Ident{Name: oc.TableName},
					Column:      &parser.Ident{Name: oc.ColumnName},
					ColumnIndex: oc.ColumnIndex,
				},
			})
		}
		return result, nil

	default:
		return nil, sql3.NewErrInternalf("unexpected source type: %T", source)
	}
}
