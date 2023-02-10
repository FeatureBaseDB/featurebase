// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"strconv"
	"strings"

	"github.com/featurebasedb/featurebase/v3/sql3"
	"github.com/featurebasedb/featurebase/v3/sql3/parser"
)

// analyze a parser.Expr. returns the analyzed parser.Expr
func (p *ExecutionPlanner) analyzeExpression(ctx context.Context, expr parser.Expr, scope parser.Statement) (parser.Expr, error) {
	if expr == nil {
		return nil, nil
	}

	switch e := expr.(type) {
	case *parser.BinaryExpr:
		return p.analyzeBinaryExpression(ctx, e, scope)

	case *parser.BoolLit:
		return e, nil

	case *parser.Call:
		return p.analyzeCallExpression(ctx, e, scope)

	case *parser.CastExpr:
		analyzedExpr, err := p.analyzeExpression(ctx, e.X, scope)
		if err != nil {
			return nil, err
		}

		targetType, err := dataTypeFromParserType(e.Type)
		if err != nil {
			return nil, err
		}
		if !typesCanBeCast(analyzedExpr.DataType(), targetType) {
			return nil, sql3.NewErrInvalidCast(analyzedExpr.Pos().Line, analyzedExpr.Pos().Column, analyzedExpr.DataType().TypeDescription(), targetType.TypeDescription())
		}
		e.X = analyzedExpr
		e.ResultDataType = targetType
		return e, nil

	case *parser.ExprList:
		for i, ex := range e.Exprs {
			listExpr, err := p.analyzeExpression(ctx, ex, scope)
			if err != nil {
				return nil, err
			}
			e.Exprs[i] = listExpr
		}
		return e, nil

	case *parser.Ident:
		switch sc := scope.(type) {
		case *parser.SelectStatement:
			if sc.Source == nil {
				return nil, sql3.NewErrColumnNotFound(e.NamePos.Line, e.NamePos.Column, e.Name)
			}

			// go find the first ident in the source that matches
			oc, err := sc.Source.OutputColumnNamed(e.Name)
			if err != nil {
				return nil, err
			} else if oc == nil {
				return nil, sql3.NewErrColumnNotFound(e.NamePos.Line, e.NamePos.Column, e.Name)
			}

			// now turn *parser.Ident into *parser.QualifiedRef
			ident := &parser.QualifiedRef{
				Table: &parser.Ident{
					Name:    oc.TableName,
					NamePos: e.NamePos,
				},
				Column: &parser.Ident{
					Name:    oc.ColumnName,
					NamePos: e.NamePos,
				},
				ColumnIndex: oc.ColumnIndex,
			}
			return p.analyzeExpression(ctx, ident, scope)

		case *parser.InsertStatement:
			return nil, sql3.NewErrColumnNotFound(e.NamePos.Line, e.NamePos.Column, e.Name)

		case *parser.DeleteStatement:

			// go find the first ident in the source that matches
			oc, err := sc.Source.OutputColumnNamed(e.Name)
			if err != nil {
				return nil, err
			} else if oc == nil {
				return nil, sql3.NewErrColumnNotFound(e.NamePos.Line, e.NamePos.Column, e.Name)
			}

			ident := &parser.QualifiedRef{
				Table: &parser.Ident{
					Name:    oc.TableName,
					NamePos: e.NamePos,
				},
				Column: &parser.Ident{
					Name:    oc.ColumnName,
					NamePos: e.NamePos,
				},
				ColumnIndex: oc.ColumnIndex,
			}
			return p.analyzeExpression(ctx, ident, scope)

		default:
			return nil, sql3.NewErrInternalf("unhandled scope type '%T'", sc)
		}

	case *parser.Variable:
		switch sc := scope.(type) {
		case *parser.BulkInsertStatement:
			// get the name of the variable without the @
			varname := e.VarName()

			for idx, mi := range sc.MapList {
				if strings.EqualFold(varname, mi.Name.Name) {
					e.VariableIndex = idx

					dataType, err := dataTypeFromParserType(mi.Type)
					if err != nil {
						return nil, sql3.NewErrUnknownType(e.NamePos.Line, e.NamePos.Column, mi.Type.String())
					}
					e.VarDataType = dataType
					return e, nil
				}
			}
			return nil, sql3.NewErrUnknownIdentifier(e.NamePos.Line, e.NamePos.Column, varname)
		default:
			return nil, sql3.NewErrInternalf("unhandled scope type '%T'", sc)
		}

	case *parser.NullLit:
		return e, nil

	case *parser.IntegerLit:
		return e, nil

	case *parser.FloatLit:
		return e, nil

	case *parser.StringLit:
		return e, nil

	case *parser.DateLit:
		return e, nil

	case *parser.ParenExpr:
		pexpr, err := p.analyzeExpression(ctx, e.X, scope)
		if err != nil {
			return nil, err
		}
		e.X = pexpr
		return e, nil

	case *parser.SetLiteralExpr:
		for i, ex := range e.Members {
			listExpr, err := p.analyzeExpression(ctx, ex, scope)
			if err != nil {
				return nil, err
			}
			e.Members[i] = listExpr
		}

		if len(e.Members) == 0 {
			return nil, sql3.NewErrLiteralEmptySetNotAllowed(e.Lbracket.Line, e.Lbracket.Column)
		}

		setDataType := e.Members[0].DataType()
		switch setDataType.(type) {
		case *parser.DataTypeID, *parser.DataTypeInt:
			//make sure everything else is an int
			for _, mbr := range e.Members {
				if !typeIsInteger(mbr.DataType()) {
					return nil, sql3.NewErrIntExpressionExpected(mbr.Pos().Line, mbr.Pos().Column)
				}
			}
			e.ResultDataType = parser.NewDataTypeIDSet()

		case *parser.DataTypeString:
			//make sure everything else is a string
			for _, mbr := range e.Members {
				if !typeIsString(mbr.DataType()) {
					return nil, sql3.NewErrStringExpressionExpected(mbr.Pos().Line, mbr.Pos().Column)
				}
			}
			e.ResultDataType = parser.NewDataTypeStringSet()

		default:
			return nil, sql3.NewErrSetLiteralMustContainIntOrString(e.Members[0].Pos().Line, e.Members[0].Pos().Column)
		}

		return e, nil

	case *parser.TupleLiteralExpr:
		memberTypes := make([]parser.ExprDataType, 0)
		for i, ex := range e.Members {
			memberExpr, err := p.analyzeExpression(ctx, ex, scope)
			if err != nil {
				return nil, err
			}
			e.Members[i] = memberExpr
			memberTypes = append(memberTypes, memberExpr.DataType())
		}

		if len(e.Members) == 0 {
			return nil, sql3.NewErrLiteralEmptyTupleNotAllowed(e.Lbrace.Line, e.Lbrace.Column)
		}
		e.ResultDataType = parser.NewDataTypeTuple(memberTypes)

		return e, nil

	case *parser.QualifiedRef:
		switch sc := scope.(type) {
		case *parser.SelectStatement:

			if e.Table.Name == "" {
				// there is no table or alias name in the qualifier so go look for the first matching column from any of the sources
				oc, err := sc.Source.OutputColumnNamed(e.Column.Name)
				if err != nil {
					return nil, err
				}
				if oc != nil {
					e.RefDataType = oc.Datatype
					e.ColumnIndex = oc.ColumnIndex
					return e, nil

				}
				return nil, sql3.NewErrColumnNotFound(e.Column.NamePos.Line, e.Column.NamePos.Column, e.Column.Name)

			} else {
				oc, err := sc.Source.OutputColumnQualifierNamed(e.Table.Name, e.Column.Name)
				if err != nil {
					return nil, err
				}
				if oc != nil {
					e.RefDataType = oc.Datatype
					e.ColumnIndex = oc.ColumnIndex
					return e, nil

				}
				return nil, sql3.NewErrColumnNotFound(e.Column.NamePos.Line, e.Column.NamePos.Column, e.Column.Name)
			}

		case *parser.DeleteStatement:
			oc, err := sc.Source.OutputColumnNamed(e.Column.Name)
			if err != nil {
				return nil, err
			}
			if oc != nil {
				e.RefDataType = oc.Datatype
				e.ColumnIndex = oc.ColumnIndex
				return e, nil

			}
			return nil, sql3.NewErrColumnNotFound(e.Column.NamePos.Line, e.Column.NamePos.Column, e.Column.Name)

		default:
			return nil, sql3.NewErrInternalf("unhandled scope type '%T'", sc)
		}

	case *parser.Range:
		return p.analyzeRangeExpression(ctx, e, scope)

	case *parser.CaseExpr:
		operand, err := p.analyzeExpression(ctx, e.Operand, scope)
		if err != nil {
			return nil, err
		}
		e.Operand = operand

		for i, ex := range e.Blocks {
			block, err := p.analyzeCaseBlockExpression(ctx, ex, e, scope)
			if err != nil {
				return nil, err
			}
			e.Blocks[i] = block
		}

		elseExpr, err := p.analyzeExpression(ctx, e.ElseExpr, scope)
		if err != nil {
			return nil, err
		}
		e.ElseExpr = elseExpr

		//type checking...
		if e.Operand != nil {
			//we are "case expr when" form, so need to make sure that 'expr' and all block conditions are equatable
			for _, blk := range e.Blocks {
				if !typesAreComparable(e.Operand.DataType(), blk.Condition.DataType()) {
					return nil, sql3.NewErrTypesAreNotEquatable(blk.Condition.Pos().Line, blk.Condition.Pos().Column, e.Operand.DataType().TypeDescription(), blk.Condition.DataType().TypeDescription())
				}
			}
		} else {
			//we are "case when" form, so need to make sure that all block conditions are bool
			for _, blk := range e.Blocks {
				if !typeIsBool(blk.Condition.DataType()) {
					return nil, sql3.NewErrBooleanExpressionExpected(blk.Condition.Pos().Line, blk.Condition.Pos().Column)
				}
			}
		}

		if len(e.Blocks) == 0 {
			return nil, sql3.NewErrInternalf("unexpected case blocks length")
		}

		//set the result type for the case to the type of the first block
		caseType := e.Blocks[0].DataType()

		//now check all the other blocks to make sure that each body is assignment compatible with that type
		for _, blk := range e.Blocks {
			if !typesAreAssignmentCompatible(caseType, blk.Body.DataType()) {
				return nil, sql3.NewErrTypeAssignmentIncompatible(blk.Body.Pos().Line, blk.Body.Pos().Column, caseType.TypeDescription(), blk.Body.DataType().TypeDescription())
			}
		}

		//if there is an else check that too
		if e.ElseExpr != nil {
			if !typesAreAssignmentCompatible(caseType, e.ElseExpr.DataType()) {
				return nil, sql3.NewErrTypeAssignmentIncompatible(e.ElseExpr.Pos().Line, e.ElseExpr.Pos().Column, caseType.TypeDescription(), e.ElseExpr.DataType().TypeDescription())
			}
		}

		e.ResultDataType = caseType

		return e, nil

	case *parser.UnaryExpr:
		return p.analyzeUnaryExpression(ctx, e, scope)

	case *parser.SelectStatement:
		selExpr, err := p.analyzeSelectStatement(ctx, e)
		if err != nil {
			return nil, err
		}
		// if we return more than one column
		if len(e.Columns) > 1 {
			return nil, sql3.NewErrInternalf("subquery must return only one column")
		}
		return selExpr, nil

	default:
		return nil, sql3.NewErrInternalf("unexpected SQL expression type: %T", expr)
	}
}

func (p *ExecutionPlanner) analyzeUnaryExpression(ctx context.Context, expr *parser.UnaryExpr, scope parser.Statement) (parser.Expr, error) {

	x, err := p.analyzeExpression(ctx, expr.X, scope)
	if err != nil {
		return nil, err
	}
	expr.X = x

	switch op := expr.Op; op {

	//bitwise operators
	case parser.BITNOT:
		if !typeIsCompatibleWithBitwiseOperator(x.DataType()) {
			return nil, sql3.NewErrTypeIncompatibleWithBitwiseOperator(x.Pos().Line, x.Pos().Column, op.String(), x.DataType().TypeDescription())
		}
		expr.ResultDataType = x.DataType()
		return expr, nil

	//arithmetic operators
	case parser.PLUS, parser.MINUS:
		if !typeIsCompatibleWithArithmeticOperator(x.DataType(), op) {
			return nil, sql3.NewErrTypeIncompatibleWithArithmeticOperator(x.Pos().Line, x.Pos().Column, op.String(), x.DataType().TypeDescription())
		}
		if typeIsInteger(x.DataType()) {
			expr.ResultDataType = parser.NewDataTypeInt()
		} else if typeIsDecimal(x.DataType()) {
			fd, ok := x.DataType().(*parser.DataTypeDecimal)
			if !ok {
				return nil, sql3.NewErrInternalf("unexpected data type")
			}
			expr.ResultDataType = fd
		} else {
			return nil, sql3.NewErrInternalf("unexpected unary expression type: %T", x.DataType())
		}
		return expr, nil

	default:
		return nil, sql3.NewErrInternalf("unexpected unary expression operator: %s", op)
	}
}

func (p *ExecutionPlanner) analyzeBinaryExpression(ctx context.Context, expr *parser.BinaryExpr, scope parser.Statement) (parser.Expr, error) {

	//analyze both sides first
	x, err := p.analyzeExpression(ctx, expr.X, scope)
	if err != nil {
		return nil, err
	}
	expr.X = x
	y, err := p.analyzeExpression(ctx, expr.Y, scope)
	if err != nil {
		return nil, err
	}
	expr.Y = y

	// check nil for either of these expressions after they were ananlyzed, they may have been eliminated
	// in which case we return the remaining one or nil if both have been eliminated
	if x == nil && y == nil {
		return nil, nil
	}
	if x == nil {
		return y, nil
	}
	if y == nil {
		return x, nil
	}

	//handle operator
	switch op := expr.Op; op {

	//logical operators
	case parser.AND, parser.OR:
		if !typeIsCompatibleWithLogicalOperator(x.DataType()) {
			return nil, sql3.NewErrTypeIncompatibleWithLogicalOperator(x.Pos().Line, x.Pos().Column, op.String(), x.DataType().TypeDescription())
		}
		if !typeIsCompatibleWithLogicalOperator(y.DataType()) {
			return nil, sql3.NewErrTypeIncompatibleWithLogicalOperator(y.Pos().Line, y.Pos().Column, op.String(), y.DataType().TypeDescription())
		}
		//logical operator so type of expr is bool
		expr.ResultDataType = parser.NewDataTypeBool()
		return expr, nil

	//equality operators
	case parser.EQ, parser.NE:
		if !typeIsCompatibleWithEqualityOperator(x.DataType()) {
			return nil, sql3.NewErrTypeIncompatibleWithEqualityOperator(x.Pos().Line, x.Pos().Column, op.String(), x.DataType().TypeDescription())
		}
		if !typeIsCompatibleWithEqualityOperator(y.DataType()) {
			return nil, sql3.NewErrTypeIncompatibleWithEqualityOperator(y.Pos().Line, y.Pos().Column, op.String(), y.DataType().TypeDescription())
		}
		if typeIsTimestamp(x.DataType()) && y.IsLiteral() && typeIsString(y.DataType()) {
			// we have a string literal on the rhs being compared to a date so
			// try to convert to a date literal
			rhs, ok := y.(*parser.StringLit)
			if !ok {
				return nil, sql3.NewErrInternalf("unexpected expression type '%T'", y)
			}
			newRhs := rhs.ConvertToTimestamp()
			if newRhs != nil {
				expr.Y = newRhs
				y = newRhs
			}
		}
		if !typesAreComparable(x.DataType(), y.DataType()) {
			return nil, sql3.NewErrTypesAreNotEquatable(x.Pos().Line, x.Pos().Column, x.DataType().TypeDescription(), y.DataType().TypeDescription())
		}
		//equality operator so type of expr is bool
		expr.ResultDataType = parser.NewDataTypeBool()
		return expr, nil

	//comparison operators
	case parser.LT, parser.LE, parser.GT, parser.GE:
		if !typeIsCompatibleWithComparisonOperator(x.DataType()) {
			return nil, sql3.NewErrTypeIncompatibleWithComparisonOperator(x.Pos().Line, x.Pos().Column, op.String(), x.DataType().TypeDescription())
		}
		if typeIsTimestamp(x.DataType()) && y.IsLiteral() && typeIsString(y.DataType()) {
			// we have a string literal on the rhs being compared to a date so
			// try to convert to a date literal
			rhs, ok := y.(*parser.StringLit)
			if !ok {
				return nil, sql3.NewErrInternalf("unexpected expression type '%T'", y)
			}
			newRhs := rhs.ConvertToTimestamp()
			if newRhs != nil {
				expr.Y = newRhs
				y = newRhs
			}
		}
		if !typeIsCompatibleWithComparisonOperator(y.DataType()) {
			return nil, sql3.NewErrTypeIncompatibleWithComparisonOperator(y.Pos().Line, y.Pos().Column, op.String(), y.DataType().TypeDescription())
		}
		if !typesAreComparable(x.DataType(), y.DataType()) {
			return nil, sql3.NewErrTypesAreNotEquatable(x.Pos().Line, x.Pos().Column, x.DataType().TypeDescription(), y.DataType().TypeDescription())
		}
		//comparison operator so type of expr is bool
		expr.ResultDataType = parser.NewDataTypeBool()
		return expr, nil

	//arithmetic operators
	case parser.PLUS, parser.MINUS, parser.STAR, parser.SLASH, parser.REM:
		if !typeIsCompatibleWithArithmeticOperator(x.DataType(), op) {
			return nil, sql3.NewErrTypeIncompatibleWithArithmeticOperator(x.Pos().Line, x.Pos().Column, op.String(), x.DataType().TypeDescription())
		}
		if !typeIsCompatibleWithArithmeticOperator(y.DataType(), op) {
			return nil, sql3.NewErrTypeIncompatibleWithArithmeticOperator(y.Pos().Line, y.Pos().Column, op.String(), y.DataType().TypeDescription())
		}

		coercedType, err := typesCoercedForArithmeticOperator(x.DataType(), y.DataType(), x.Pos())
		if err != nil {
			return nil, err
		}
		expr.ResultDataType = coercedType
		return expr, nil

		/*
			opx, okx := x.(*NumLiteralPlanExpresssion)
			opy, oky := y.(*NumLiteralPlanExpresssion)
			if okx && oky {
				//both literals so we can fold
				numx, err := strconv.Atoi(opx.value)
				if err != nil {
					return nil, err
				}
				numy, err := strconv.Atoi(opy.value)
				if err != nil {
					return nil, err
				}

				switch op {
				case parser.PLUS:
					value := numx + numy
					return NewNumLiteralPlanExpresssion(p, strconv.Itoa(value)), nil

				case parser.MINUS:
					value := numx - numy
					return NewNumLiteralPlanExpresssion(p, strconv.Itoa(value)), nil

				case parser.STAR:
					value := numx * numy
					return NewNumLiteralPlanExpresssion(p, strconv.Itoa(value)), nil

				case parser.SLASH:
					value := numx / numy
					return NewNumLiteralPlanExpresssion(p, strconv.Itoa(value)), nil

				case parser.REM:
					value := numx % numy
					return NewNumLiteralPlanExpresssion(p, strconv.Itoa(value)), nil

				default:
					//run home to momma
					return NewBinOpPlanExpression(p, x, expr.Op, y), nil
				}
			} else {
				return NewBinOpPlanExpression(p, x, expr.Op, y), nil
			}*/

	//bitwise operators
	case parser.BITAND, parser.BITOR, parser.LSHIFT, parser.RSHIFT:
		if !typeIsCompatibleWithBitwiseOperator(x.DataType()) {
			return nil, sql3.NewErrTypeIncompatibleWithBitwiseOperator(x.Pos().Line, x.Pos().Column, op.String(), x.DataType().TypeDescription())
		}
		if !typeIsCompatibleWithBitwiseOperator(y.DataType()) {
			return nil, sql3.NewErrTypeIncompatibleWithBitwiseOperator(y.Pos().Line, y.Pos().Column, op.String(), y.DataType().TypeDescription())
		}
		coercedType, err := typesCoercedForBitwiseOperator(x.DataType(), y.DataType(), x.Pos())
		if err != nil {
			return nil, err
		}
		expr.ResultDataType = coercedType
		return expr, nil

		/*
			opx, okx := x.(*NumLiteralPlanExpression)
			opy, oky := y.(*NumLiteralPlanExpression)
			if okx && oky {
				//both literals so we can fold
				numx, err := strconv.Atoi(opx.value)
				if err != nil {
					return nil, err
				}
				numy, err := strconv.Atoi(opy.value)
				if err != nil {
					return nil, err
				}

				switch op {
				case parser.PLUS:
					value := numx + numy
					return NewNumLiteralPlanExpression(p, strconv.Itoa(value)), nil

				case parser.MINUS:
					value := numx - numy
					return NewNumLiteralPlanExpression(p, strconv.Itoa(value)), nil

				case parser.STAR:
					value := numx * numy
					return NewNumLiteralPlanExpression(p, strconv.Itoa(value)), nil

				case parser.SLASH:
					value := numx / numy
					return NewNumLiteralPlanExpression(p, strconv.Itoa(value)), nil

				case parser.REM:
					value := numx % numy
					return NewNumLiteralPlanExpression(p, strconv.Itoa(value)), nil

				default:
					//run home to momma
					return newBinOpPlanExpression(x, expr.Op, y), nil
				}
			} else {
				return newBinOpPlanExpression(x, expr.Op, y), nil
			}*/

	//null test
	case parser.IS, parser.ISNOT:
		_, ok := expr.Y.(*parser.NullLit)
		if !ok {
			return nil, sql3.NewErrInternalf("NULL expected")
		}
		//no type check against null...logical operator so type of expr is bool
		expr.ResultDataType = parser.NewDataTypeBool()
		return expr, nil

	case parser.IN, parser.NOTIN:
		lst, ok := y.(*parser.ExprList)
		if !ok {
			return nil, sql3.NewErrExpressionListExpected(y.Pos().Line, y.Pos().Column)
		}

		for idx, ex := range lst.Exprs {

			//check to see if our expression is a select statement
			//if it is it needs special handling
			sel, ok := ex.(*parser.SelectStatement)
			if ok {
				//we have a select in the expression list so make sure it is the only thing in the expression list
				if len(lst.Exprs) > 1 {
					return nil, sql3.NewErrInternalf("expresion list should only contain one select statement")
				}
				//make sure select only returns one column
				if len(sel.Columns) > 1 {
					return nil, sql3.NewErrInternalf("select used as part of IN expression should only return one column")
				}
				if !typesAreComparable(x.DataType(), sel.Columns[0].Expr.DataType()) {
					return nil, sql3.NewErrTypesAreNotEquatable(x.Pos().Line, x.Pos().Column, x.DataType().TypeDescription(), ex.DataType().TypeDescription())
				}
				//need to turn this into an inner join
				operator := &parser.JoinOperator{
					Inner: expr.OpPos,
				}

				constraint := &parser.OnConstraint{
					X: &parser.BinaryExpr{
						X:              expr.X,
						Op:             parser.EQ,
						Y:              sel.Columns[0].Expr,
						ResultDataType: parser.NewDataTypeBool(),
					},
				}

				switch scopeStmt := scope.(type) {
				case *parser.SelectStatement:

					if lhs, ok := scopeStmt.Source.(*parser.JoinClause); ok {
						scopeStmt.Source = &parser.JoinClause{
							X:        lhs.X,
							Operator: lhs.Operator,
							Y: &parser.JoinClause{
								X:          lhs.Y,
								Operator:   operator,
								Y:          sel,
								Constraint: constraint,
							},
							Constraint: lhs.Constraint,
						}
					} else {
						scopeStmt.Source = &parser.JoinClause{
							X:          scopeStmt.Source,
							Operator:   operator,
							Y:          sel,
							Constraint: constraint,
						}
					}

				case *parser.DeleteStatement:
					if lhs, ok := scopeStmt.Source.(*parser.JoinClause); ok {
						scopeStmt.Source = &parser.JoinClause{
							X:        lhs.X,
							Operator: lhs.Operator,
							Y: &parser.JoinClause{
								X:          lhs.Y,
								Operator:   operator,
								Y:          sel,
								Constraint: constraint,
							},
							Constraint: lhs.Constraint,
						}
					} else {
						scopeStmt.Source = &parser.JoinClause{
							X:          scopeStmt.Source,
							Operator:   operator,
							Y:          sel,
							Constraint: constraint,
						}
					}

				default:
					return nil, sql3.NewErrInternalf("unexpected scope type '%T'", scope)
				}
				// we are eliminating this expression, since we moved it into the source, so
				// return nil
				return nil, nil
			}

			//not a sql statement

			//handle the case of of tthe LHS of the expression being a timestamp, the RHS being a string literal
			//if so, try to coerce to a timestamp
			if typeIsTimestamp(x.DataType()) && typeIsString(ex.DataType()) && ex.IsLiteral() {
				litExpr, ok := ex.(*parser.StringLit)
				if ok {
					tsLit := litExpr.ConvertToTimestamp()
					if tsLit != nil {
						ex = tsLit
						lst.Exprs[idx] = ex
					}
				}
			}

			//make sure LHS and RHS types are comparable
			if !typesAreComparable(x.DataType(), ex.DataType()) {
				return nil, sql3.NewErrTypesAreNotEquatable(x.Pos().Line, x.Pos().Column, x.DataType().TypeDescription(), ex.DataType().TypeDescription())
			}
		}

		expr.ResultDataType = parser.NewDataTypeBool()
		return expr, nil

	case parser.BETWEEN, parser.NOTBETWEEN:
		if !typeIsRange(y.DataType()) {
			return nil, sql3.NewErrTypeIncompatibleWithBetweenOperator(x.Pos().Line, x.Pos().Column, op.String(), x.DataType().TypeDescription())
		}

		ok, err := typesAreRangeComparable(x.DataType(), y.DataType())
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, sql3.NewErrTypeIncompatibleWithBetweenOperator(x.Pos().Line, x.Pos().Column, op.String(), x.DataType().TypeDescription())
		}
		expr.ResultDataType = parser.NewDataTypeBool()
		return expr, nil

	case parser.CONCAT:
		if !typeIsCompatibleWithConcatOperator(x.DataType()) {
			return nil, sql3.NewErrTypeIncompatibleWithConcatOperator(x.Pos().Line, x.Pos().Column, op.String(), x.DataType().TypeDescription())
		}
		if !typeIsCompatibleWithConcatOperator(y.DataType()) {
			return nil, sql3.NewErrTypeIncompatibleWithConcatOperator(y.Pos().Line, y.Pos().Column, op.String(), y.DataType().TypeDescription())
		}
		expr.ResultDataType = parser.NewDataTypeString()
		return expr, nil

	case parser.LIKE, parser.NOTLIKE:
		if !typeIsCompatibleWithLikeOperator(x.DataType()) {
			return nil, sql3.NewErrTypeIncompatibleWithLikeOperator(x.Pos().Line, x.Pos().Column, op.String(), x.DataType().TypeDescription())
		}
		if !typeIsCompatibleWithLikeOperator(y.DataType()) {
			return nil, sql3.NewErrTypeIncompatibleWithLikeOperator(y.Pos().Line, y.Pos().Column, op.String(), y.DataType().TypeDescription())
		}
		//comparison operator so type of expr is bool
		expr.ResultDataType = parser.NewDataTypeBool()
		return expr, nil

	default:
		return nil, sql3.NewErrInternalf("unexpected binary expression operator: %s", op)
	}
}

func (p *ExecutionPlanner) analyzeRangeExpression(ctx context.Context, expr *parser.Range, scope parser.Statement) (parser.Expr, error) {
	//analyze subscripts
	x, err := p.analyzeExpression(ctx, expr.X, scope)
	if err != nil {
		return nil, err
	}
	expr.X = x
	y, err := p.analyzeExpression(ctx, expr.Y, scope)
	if err != nil {
		return nil, err
	}
	expr.Y = y

	//check to see if we have string literals that are actually dates
	xLiteral, ok := x.(*parser.StringLit)
	if ok {
		tsLiteral := xLiteral.ConvertToTimestamp()
		if tsLiteral != nil {
			expr.X = tsLiteral
		}
	}

	yLiteral, ok := y.(*parser.StringLit)
	if ok {
		tsLiteral := yLiteral.ConvertToTimestamp()
		if tsLiteral != nil {
			expr.Y = tsLiteral
		}
	}

	if !typeCanBeUsedInRange(expr.X.DataType()) {
		return nil, sql3.NewErrTypeCannotBeUsedAsRangeSubscript(expr.X.Pos().Line, expr.X.Pos().Column, expr.X.DataType().TypeDescription())
	}
	if !typeCanBeUsedInRange(expr.Y.DataType()) {
		return nil, sql3.NewErrTypeCannotBeUsedAsRangeSubscript(expr.Y.Pos().Line, expr.Y.Pos().Column, expr.Y.DataType().TypeDescription())
	}
	canbeUsed, coercedType := typesOfRangeBoundsAreTheSame(expr.X.DataType(), expr.Y.DataType())
	if !canbeUsed {
		return nil, sql3.NewErrIncompatibleTypesForRangeSubscripts(expr.Pos().Line, expr.Pos().Column, expr.X.DataType().TypeDescription(), expr.Y.DataType().TypeDescription())
	}
	expr.ResultDataType = parser.NewDataTypeRange(coercedType)

	return expr, nil
}

func (p *ExecutionPlanner) analyzeCaseBlockExpression(ctx context.Context, expr *parser.CaseBlock, caseScope *parser.CaseExpr, scope parser.Statement) (*parser.CaseBlock, error) {
	x, err := p.analyzeExpression(ctx, expr.Body, scope)
	if err != nil {
		return nil, err
	}
	expr.Body = x
	y, err := p.analyzeExpression(ctx, expr.Condition, scope)
	if err != nil {
		return nil, err
	}
	expr.Condition = y

	return expr, nil
}

func (p *ExecutionPlanner) analyzeOrderingTermExpression(expr parser.Expr, scope parser.Statement) (parser.Expr, error) {
	if expr == nil {
		return nil, nil
	}

	// ordering terms need to be either a column name, an alias name or an integer literal representing
	// position of column in the select list

	switch thisExpr := expr.(type) {
	case *parser.Ident:
		switch sc := scope.(type) {
		case *parser.SelectStatement:

			// go find the first ident in the projection list that matches
			columnIndex := 0
			found := false
			for idx, proj := range sc.Columns {
				// if the expression is a qualified ref, check the name
				colExpr, ok := proj.Expr.(*parser.QualifiedRef)
				if ok && strings.EqualFold(thisExpr.Name, colExpr.Column.Name) {
					columnIndex = idx
					found = true
					break
				}
				// try the alias is there is one
				if proj.Alias != nil && strings.EqualFold(thisExpr.Name, proj.Alias.Name) {
					columnIndex = idx
					found = true
					break
				}
			}

			if !found {
				return nil, sql3.NewErrColumnNotFound(thisExpr.NamePos.Line, thisExpr.NamePos.Column, thisExpr.Name)
			}

			// turn *parser.Ident into *parser.QualifiedRef
			ident := &parser.QualifiedRef{
				Table: &parser.Ident{
					Name:    "",
					NamePos: parser.Pos{Line: 0, Column: 0},
				},
				Column: &parser.Ident{
					Name:    thisExpr.Name,
					NamePos: thisExpr.NamePos,
				},
				ColumnIndex: columnIndex,
				// since this is a ordring term, we don't care about the type
				RefDataType: parser.NewDataTypeVoid(),
			}
			return ident, nil

		default:
			return nil, sql3.NewErrInternalf("unhandled scope type '%T'", sc)
		}

	case *parser.IntegerLit:
		switch sc := scope.(type) {
		case *parser.SelectStatement:
			// check to see if the offset is in the range
			value, err := strconv.ParseInt(thisExpr.Value, 10, 64)
			if err != nil {
				return nil, sql3.NewErrInternalf("unexpected integer literal value")
			}
			if value < 1 || value > int64(len(sc.Columns)) {
				return nil, sql3.NewErrExpectedSortExpressionReference(0, 0)
			}
		default:
			return nil, sql3.NewErrInternalf("unhandled scope type '%T'", sc)
		}

	default:
		return nil, sql3.NewErrExpectedSortExpressionReference(expr.Pos().Line, expr.Pos().Column)
	}
	return expr, nil
}
