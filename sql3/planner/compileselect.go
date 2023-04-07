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
)

// compileSelectStatment compiles a parser.SelectStatment AST into a PlanOperator
func (p *ExecutionPlanner) compileSelectStatement(stmt *parser.SelectStatement, isSubquery bool) (types.PlanOperator, error) {
	query := NewPlanOpQuery(p, NewPlanOpNullTable(), p.sql)

	aggregates := make([]types.PlanExpression, 0)

	// compile select list and generate a list of projections
	projections := make([]types.PlanExpression, 0)
	for _, c := range stmt.Columns {
		planExpr, err := p.compileExpr(c.Expr)
		if err != nil {
			return nil, err
		}
		if c.Alias != nil {
			planExpr = newAliasPlanExpression(c.Alias.Name, planExpr)
		}
		projections = append(projections, planExpr)
		aggregates = p.gatherExprAggregates(planExpr, aggregates)
	}

	// compile group by clause and generate a list of group by expressions
	groupByExprs := make([]types.PlanExpression, 0)
	for _, expr := range stmt.GroupByExprs {
		switch expr := expr.(type) {
		case *parser.QualifiedRef:
			groupByExprs = append(groupByExprs, newQualifiedRefPlanExpression(expr.Table.Name, expr.Column.Name, expr.ColumnIndex, expr.DataType()))
		default:
			return nil, sql3.NewErrInternalf("unsupported expression type in GROUP BY clause: %T", expr)
		}
	}

	// compile the where clause
	where, err := p.compileExpr(stmt.WhereExpr)
	if err != nil {
		return nil, err
	}

	// compile source expression
	source, err := p.compileSource(query, stmt.Source)
	if err != nil {
		return nil, err
	}

	// if we did have a where, insert the filter op after source
	if where != nil {
		aggregates = p.gatherExprAggregates(where, aggregates)
		source = NewPlanOpFilter(p, where, source)
	}

	// compile the having clause
	having, err := p.compileExpr(stmt.HavingExpr)
	if err != nil {
		return nil, err
	}

	// if we have a having, check references
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
				case types.Aggregable:
					ch := ex.Children()
					// first arg is always the ref, except for count(*)
					if len(ch) > 0 {
						aggregateAndGroupByExprs = append(aggregateAndGroupByExprs, ch[0])
					}
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
			case types.Aggregable:
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

	// compile order by and generate a list of ordering expressions
	orderByExprs := make([]*OrderByExpression, 0)
	nonReferenceOrderByExpressions := make([]types.PlanExpression, 0)
	if len(stmt.OrderingTerms) > 0 {
		for _, ot := range stmt.OrderingTerms {
			// compile the ordering term
			expr, err := p.compileOrderingTermExpr(ot.X, projections, stmt.Source)
			if err != nil {
				return nil, err
			}

			f := &OrderByExpression{
				Expr: expr,
			}
			f.Order = orderByAsc
			if ot.Desc.IsValid() {
				f.Order = orderByDesc
			}
			orderByExprs = append(orderByExprs, f)
		}

		// if the expression is just references, we
		// can put the sort directly after the source
		for _, oe := range orderByExprs {
			_, ok := oe.Expr.(*qualifiedRefPlanExpression)
			if !ok {
				nonReferenceOrderByExpressions = append(nonReferenceOrderByExpressions, oe.Expr)
			}
		}

		// all the order by expressions are references, so we can put the order by before the
		// projection
		if len(nonReferenceOrderByExpressions) == 0 {
			source = NewPlanOpOrderBy(orderByExprs, source)
		}
	}

	var compiledOp types.PlanOperator

	// do we have straight projection or a group by?
	if len(aggregates) > 0 {
		// we have a group by

		//check that any projections that are not aggregates are in the group by list
		nonAggregateReferences := make([]*qualifiedRefPlanExpression, 0)
		for _, expr := range projections {
			InspectExpression(expr, func(expr types.PlanExpression) bool {
				switch ex := expr.(type) {
				case types.Aggregable:
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
		// no group by, just a straight projection
		compiledOp = NewPlanOpProjection(projections, source)
	}

	// handle the case where we have order by expressions and they are not references
	// in this case we need to put the order by after the projection
	if len(orderByExprs) > 0 && len(nonReferenceOrderByExpressions) > 0 {

		// if the order by expressions contain a reference not in the projection list,
		// we have to create a new projection, add references to current projection,
		// and place the new order by in between

		// get a list of all the refs for the order by exprs
		orderByRefs := make(map[string]*qualifiedRefPlanExpression)
		for _, oe := range orderByExprs {
			ex, ok := oe.Expr.(*qualifiedRefPlanExpression)
			if ok {
				orderByRefs[ex.String()] = ex
			}
		}

		// get a list of all the projection refs
		projRefs := make(map[string]*qualifiedRefPlanExpression)
		for _, p := range projections {
			InspectExpression(p, func(expr types.PlanExpression) bool {
				switch ex := expr.(type) {
				case *qualifiedRefPlanExpression:
					projRefs[ex.String()] = ex
					return false
				}
				return true
			})
		}

		// iterate the order by terms, make a list of the ones not projected
		unprojectedRefs := make([]*qualifiedRefPlanExpression, 0)
		for kobr, obr := range orderByRefs {
			_, found := projRefs[kobr]
			if !found {
				unprojectedRefs = append(unprojectedRefs, obr)
			}
		}

		// sigh - ok. If we have unprojected refs, we need to insert a projection
		if len(unprojectedRefs) > 0 {
			// create the final projection list - this will go before the order by
			newProjections := make([]types.PlanExpression, len(projections))
			for i, p := range projections {
				switch pe := p.(type) {
				case *aliasPlanExpression:
					newProjections[i] = newQualifiedRefPlanExpression("", pe.aliasName, i, pe.Type())
				case *qualifiedRefPlanExpression:
					newProjections[i] = newQualifiedRefPlanExpression(pe.tableName, pe.columnName, i, pe.Type())
				default:
					newProjections[i] = newQualifiedRefPlanExpression("", p.String(), i, p.Type())
				}
			}

			// add the unprojected refs to the existing projection op
			projectionOp, ok := compiledOp.(*PlanOpProjection)
			if !ok {
				return nil, sql3.NewErrInternalf("unexpected compiledOp type '%T'", compiledOp)
			}
			for _, uref := range unprojectedRefs {
				projectionOp.Projections = append(projectionOp.Projections, uref)
			}

			// add the order by on top of this
			// rewrite all the order by expressions that are not qualified refs to be qualified
			// refs referring to the expression
			for i, oe := range orderByExprs {
				_, ok := oe.Expr.(*qualifiedRefPlanExpression)
				if !ok {
					orderByExprs[i].Expr = newQualifiedRefPlanExpression("", oe.Expr.String(), 0, oe.Expr.Type())
				}
			}
			compiledOp = NewPlanOpOrderBy(orderByExprs, compiledOp)

			// add the final projection on top of this
			compiledOp = NewPlanOpProjection(newProjections, compiledOp)

		} else {
			// rewrite all the order by expressions that are not qualified refs to be qualified
			// refs referring to the expression
			for i, oe := range orderByExprs {
				_, ok := oe.Expr.(*qualifiedRefPlanExpression)
				if !ok {
					orderByExprs[i].Expr = newQualifiedRefPlanExpression("", oe.Expr.String(), 0, oe.Expr.Type())
				}
			}
			compiledOp = NewPlanOpOrderBy(orderByExprs, compiledOp)
		}
	}

	// insert the top operator if it exists, or limit - analyzer should have caught the case of both existing
	if stmt.Top.IsValid() {
		topExpr, err := p.compileExpr(stmt.TopExpr)
		if err != nil {
			return nil, err
		}
		compiledOp = NewPlanOpTop(topExpr, compiledOp)
	}
	// handle limit
	if stmt.Limit.IsValid() {
		limitExpr, err := p.compileExpr(stmt.LimitExpr)
		if err != nil {
			return nil, err
		}
		compiledOp = NewPlanOpTop(limitExpr, compiledOp)
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
		case types.Aggregable:
			found := false
			for _, ag := range result {
				//compare based on string representation
				if strings.EqualFold(ag.String(), ex.String()) {
					found = true
					break
				}
			}
			if !found {
				result = append(result, ex.(types.PlanExpression))
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

		tableName := strings.ToLower(parser.IdentName(sourceExpr.Name))

		// doing this check here because we don't have a 'system' flag that exists in the FB schema
		st, ok := systemTables.table(tableName)
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

		// get any query hints
		queryHints := make([]*TableQueryHint, 0)
		for _, o := range sourceExpr.QueryOptions {
			h := &TableQueryHint{
				name: parser.IdentName(o.OptionName),
			}
			for _, op := range o.OptionParams {
				h.params = append(h.params, parser.IdentName(op))
			}
			queryHints = append(queryHints, h)
		}

		// get all the columns for this table - we will eliminate unused ones
		// later on in the optimizer
		extractColumns := make([]string, 0)

		for _, oc := range sourceExpr.OutputColumns {
			extractColumns = append(extractColumns, oc.ColumnName)
		}

		if sourceExpr.Alias != nil {
			aliasName := parser.IdentName(sourceExpr.Alias)

			return NewPlanOpRelAlias(aliasName, NewPlanOpPQLTableScan(p, tableName, extractColumns, queryHints)), nil
		}
		return NewPlanOpPQLTableScan(p, tableName, extractColumns, queryHints), nil

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

func (p *ExecutionPlanner) analyzeSource(ctx context.Context, source parser.Source, scope parser.Statement) (parser.Source, error) {
	if source == nil {
		return nil, nil
	}
	switch source := source.(type) {
	case *parser.JoinClause:
		x, err := p.analyzeSource(ctx, source.X, scope)
		if err != nil {
			return nil, err
		}
		y, err := p.analyzeSource(ctx, source.Y, scope)
		if err != nil {
			return nil, err
		}
		if source.Constraint != nil {
			switch join := source.Constraint.(type) {
			case *parser.OnConstraint:
				ex, err := p.analyzeExpression(ctx, join.X, scope)
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
		x, err := p.analyzeSource(ctx, source.X, scope)
		if err != nil {
			return nil, err
		}
		source.X = x
		return source, nil

	case *parser.QualifiedTableName:

		objectName := strings.ToLower(parser.IdentName(source.Name))

		// check views first
		view, err := p.getViewByName(ctx, objectName)
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
			expr, err := p.analyzeSelectStatement(ctx, sel)
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

		// if we got to here, not a view, so do table stuff

		// check table exists
		tname := dax.TableName(objectName)
		tbl, err := p.schemaAPI.TableByName(ctx, tname)
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

		// check query hints
		for _, o := range source.QueryOptions {
			opt := parser.IdentName(o.OptionName)
			switch strings.ToLower(opt) {
			case "flatten":
				// should have 1 param and should be a column name
				if len(o.OptionParams) != 1 {
					// error
					return nil, sql3.NewErrInvalidQueryHintParameterCount(o.LParen.Column, o.LParen.Line, opt, "column name", 1, len(o.OptionParams))
				}
				for _, op := range o.OptionParams {
					param := parser.IdentName(op)
					found := false
					for _, oc := range source.OutputColumns {
						if strings.EqualFold(param, oc.ColumnName) {
							found = true
							break
						}
					}
					if !found {
						return nil, sql3.NewErrColumnNotFound(op.NamePos.Line, op.NamePos.Column, param)
					}
				}

			default:
				return nil, sql3.NewErrUnknownQueryHint(o.OptionName.NamePos.Line, o.OptionName.NamePos.Column, opt)
			}
		}

		return source, nil

	case *parser.TableValuedFunction:
		return nil, sql3.NewErrUnsupported(0, 0, true, "table valued function")

	case *parser.SelectStatement:
		expr, err := p.analyzeSelectStatement(ctx, source)
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

func (p *ExecutionPlanner) analyzeSelectStatement(ctx context.Context, stmt *parser.SelectStatement) (parser.Expr, error) {
	// analyze source first - needed for name resolution
	source, err := p.analyzeSource(ctx, stmt.Source, stmt)
	if err != nil {
		return nil, err
	}
	stmt.Source = source

	if err := p.analyzeSelectStatementWildcards(stmt); err != nil {
		return nil, err
	}

	for _, col := range stmt.Columns {
		expr, err := p.analyzeExpression(ctx, col.Expr, stmt)
		if err != nil {
			return nil, err
		}
		if expr != nil {
			col.Expr = expr
		}
	}

	if stmt.TopExpr != nil && stmt.LimitExpr != nil {
		return nil, sql3.NewErrErrTopLimitCannotCoexist(stmt.TopExpr.Pos().Line, stmt.TopExpr.Pos().Column)
	}

	expr, err := p.analyzeExpression(ctx, stmt.TopExpr, stmt)
	if err != nil {
		return nil, err
	}
	if expr != nil {
		if !(expr.IsLiteral() && typeIsInteger(expr.DataType())) {
			return nil, sql3.NewErrIntegerLiteral(stmt.TopExpr.Pos().Line, stmt.TopExpr.Pos().Column)
		}
		stmt.TopExpr = expr
	}

	expr, err = p.analyzeExpression(ctx, stmt.LimitExpr, stmt)
	if err != nil {
		return nil, err
	}
	if expr != nil {
		if !(expr.IsLiteral() && typeIsInteger(expr.DataType())) {
			return nil, sql3.NewErrIntegerLiteral(stmt.LimitExpr.Pos().Line, stmt.LimitExpr.Pos().Column)
		}
		stmt.LimitExpr = expr
	}

	expr, err = p.analyzeExpression(ctx, stmt.HavingExpr, stmt)
	if err != nil {
		return nil, err
	}
	stmt.HavingExpr = expr

	expr, err = p.analyzeExpression(ctx, stmt.WhereExpr, stmt)
	if err != nil {
		return nil, err
	}
	stmt.WhereExpr = expr

	for i, g := range stmt.GroupByExprs {
		expr, err = p.analyzeExpression(ctx, g, stmt)
		if err != nil {
			return nil, err
		}
		if expr != nil {
			stmt.GroupByExprs[i] = expr
		}
	}

	expr, err = p.analyzeExpression(ctx, stmt.HavingExpr, stmt)
	if err != nil {
		return nil, err
	}
	if expr != nil {
		stmt.HavingExpr = expr
	}

	for _, term := range stmt.OrderingTerms {
		err := p.analyzeOrderingTermExpression(term.X, stmt)
		if err != nil {
			return nil, err
		}
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
				refName := strings.ToLower(parser.IdentName(ref.Table))
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

// TODO(pok) - looks increasingly likely that this can be factored out since all join types
// do the same thing
func (p *ExecutionPlanner) columnsFromSource(source parser.Source) ([]*parser.ResultColumn, error) {
	result := []*parser.ResultColumn{}

	switch src := source.(type) {
	case *parser.JoinClause:
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
