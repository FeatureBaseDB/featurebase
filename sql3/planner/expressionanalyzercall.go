// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import (
	"strings"

	"github.com/molecula/featurebase/v3/sql3"
	"github.com/molecula/featurebase/v3/sql3/parser"
)

// analyze a *parser.Call and return the parser.Expr
func (p *ExecutionPlanner) analyzeCallExpression(call *parser.Call, scope parser.Statement) (parser.Expr, error) {
	//analyze all the args
	for i, a := range call.Args {
		arg, err := p.analyzeExpression(a, scope)
		if err != nil {
			return nil, err
		}
		call.Args[i] = arg
	}
	switch strings.ToUpper(call.Name.Name) {
	case "COUNT":
		//check to see if we have a star, if we do turn it into a qualified ref to _id
		if call.Star.IsValid() && len(call.Args) == 0 {
			newArg := &parser.Ident{
				NamePos: call.Star,
				Name:    "_id",
			}
			arg, err := p.analyzeExpression(newArg, scope)
			if err != nil {
				return nil, err
			}
			call.Args = append(call.Args, arg)
		}
		// one argument only
		if len(call.Args) != 1 {
			return nil, sql3.NewErrCallParameterCountMismatch(call.Rparen.Line, call.Rparen.Column, call.Name.Name, 1, len(call.Args))
		}
		//make sure it's a qualified ref
		_, ok := call.Args[0].(*parser.QualifiedRef)
		if !ok {
			return nil, sql3.NewErrExpectedColumnReference(call.Args[0].Pos().Line, call.Args[0].Pos().Column)
		}
		//COUNT always returns int
		call.ResultDataType = parser.NewDataTypeInt()

	case "SUM":
		// can't do a sum on *
		if call.Star.IsValid() && len(call.Args) == 0 {
			return nil, sql3.NewErrExpectedColumnReference(call.Star.Line, call.Star.Column)
		}
		// one argument only
		if len(call.Args) != 1 {
			return nil, sql3.NewErrCallParameterCountMismatch(call.Rparen.Line, call.Rparen.Column, call.Name.Name, 1, len(call.Args))
		}

		//make sure it's a qualified ref
		ref, ok := call.Args[0].(*parser.QualifiedRef)
		if !ok {
			return nil, sql3.NewErrExpectedColumnReference(call.Args[0].Pos().Line, call.Args[0].Pos().Column)
		}

		//can't do a sum on _id
		if strings.EqualFold(ref.Column.Name, "_id") {
			return nil, sql3.NewErrIdColumnNotValidForAggregateFunction(call.Args[0].Pos().Line, call.Args[0].Pos().Column, call.Name.Name)
		}

		//make sure the ref is sum-able
		if !(typeIsInteger(ref.DataType()) || typeIsDecimal(ref.DataType())) {
			return nil, sql3.NewErrIntOrDecimalExpressionExpected(ref.Table.NamePos.Line, ref.Table.NamePos.Column)
		}

		call.ResultDataType = ref.DataType()

	case "AVG":
		// can't do an avg on a *
		if call.Star.IsValid() && len(call.Args) == 0 {
			return nil, sql3.NewErrExpectedColumnReference(call.Star.Line, call.Star.Column)
		}

		// one argument only
		if len(call.Args) != 1 {
			return nil, sql3.NewErrCallParameterCountMismatch(call.Rparen.Line, call.Rparen.Column, call.Name.Name, 1, len(call.Args))
		}

		ref, ok := call.Args[0].(*parser.QualifiedRef)
		if !ok {
			return nil, sql3.NewErrExpectedColumnReference(call.Args[0].Pos().Line, call.Args[0].Pos().Column)
		}

		//can't do a avg on _id
		if strings.EqualFold(ref.Column.Name, "_id") {
			return nil, sql3.NewErrIdColumnNotValidForAggregateFunction(call.Args[0].Pos().Line, call.Args[0].Pos().Column, call.Name.Name)
		}

		//make sure the ref is avg-able
		if !(typeIsInteger(ref.DataType()) || typeIsDecimal(ref.DataType())) {
			return nil, sql3.NewErrIntOrDecimalExpressionExpected(ref.Table.NamePos.Line, ref.Table.NamePos.Column)
		}

		call.ResultDataType = parser.NewDataTypeDecimal(4)

	case "PERCENTILE":
		// can't do an percentile on a *
		if call.Star.IsValid() && len(call.Args) == 0 {
			return nil, sql3.NewErrExpectedColumnReference(call.Star.Line, call.Star.Column)
		}

		if len(call.Args) != 2 {
			return nil, sql3.NewErrCallParameterCountMismatch(call.Rparen.Line, call.Rparen.Column, call.Name.Name, 1, len(call.Args))
		}

		//first arg should be a qualified ref
		ref, ok := call.Args[0].(*parser.QualifiedRef)
		if !ok {
			return nil, sql3.NewErrExpectedColumnReference(call.Args[0].Pos().Line, call.Args[0].Pos().Column)
		}

		//can't do a percentile on _id
		if strings.EqualFold(ref.Column.Name, "_id") {
			return nil, sql3.NewErrIdColumnNotValidForAggregateFunction(call.Args[0].Pos().Line, call.Args[0].Pos().Column, call.Name.Name)
		}

		//make sure the ref is percentilable-able
		if !(typeIsInteger(ref.DataType()) || typeIsDecimal(ref.DataType()) || typeIsTimestamp(ref.DataType())) {
			return nil, sql3.NewErrIntOrDecimalOrTimestampExpressionExpected(ref.Table.NamePos.Line, ref.Table.NamePos.Column)
		}

		//second column is the nth value
		targetType := parser.NewDataTypeDecimal(4)
		if !typesAreAssignmentCompatible(targetType, call.Args[1].DataType()) {
			return nil, sql3.NewErrParameterTypeMistmatch(call.Args[1].Pos().Line, call.Args[1].Pos().Column, targetType.TypeDescription(), call.Args[1].DataType().TypeDescription())
		}

		//make sure it's literal
		if !call.Args[1].IsLiteral() {
			return nil, sql3.NewErrLiteralExpected(call.Args[1].Pos().Line, call.Args[1].Pos().Column)
		}

		//return the data type of the referenced column
		call.ResultDataType = ref.DataType()

	case "MIN", "MAX":
		// can't do an min/max on a *
		if call.Star.IsValid() && len(call.Args) == 0 {
			return nil, sql3.NewErrExpectedColumnReference(call.Star.Line, call.Star.Column)
		}

		// one argument
		if len(call.Args) != 1 {
			return nil, sql3.NewErrCallParameterCountMismatch(call.Rparen.Line, call.Rparen.Column, call.Name.Name, 1, len(call.Args))
		}

		// first arg should be a qualified ref
		ref, ok := call.Args[0].(*parser.QualifiedRef)
		if !ok {
			return nil, sql3.NewErrExpectedColumnReference(call.Args[0].Pos().Line, call.Args[0].Pos().Column)
		}

		// can't do a min/max on _id
		if strings.EqualFold(ref.Column.Name, "_id") {
			return nil, sql3.NewErrIdColumnNotValidForAggregateFunction(call.Args[0].Pos().Line, call.Args[0].Pos().Column, call.Name.Name)
		}

		// make sure the ref is min/max-able
		if !(typeIsInteger(ref.DataType()) || typeIsDecimal(ref.DataType()) || typeIsTimestamp(ref.DataType())) {
			return nil, sql3.NewErrIntOrDecimalOrTimestampExpressionExpected(ref.Table.NamePos.Line, ref.Table.NamePos.Column)
		}

		// return the data type of the referenced column
		call.ResultDataType = ref.DataType()

	case "SETCONTAINS":
		// two arguments
		if len(call.Args) != 2 {
			return nil, sql3.NewErrCallParameterCountMismatch(call.Rparen.Line, call.Rparen.Column, call.Name.Name, 2, len(call.Args))
		}

		ok, baseType := typeIsSet(call.Args[0].DataType())
		if !ok {
			return nil, sql3.NewErrSetExpressionExpected(call.Args[0].Pos().Line, call.Args[0].Pos().Column)
		}

		if !typesAreComparable(baseType, call.Args[1].DataType()) {
			return nil, sql3.NewErrTypesAreNotEquatable(call.Args[1].Pos().Line, call.Args[1].Pos().Column, call.Args[0].DataType().TypeDescription(), call.Args[1].DataType().TypeDescription())
		}
		call.ResultDataType = parser.NewDataTypeBool()

	case "SETCONTAINSALL":
		//two arguments
		if len(call.Args) != 2 {
			return nil, sql3.NewErrCallParameterCountMismatch(call.Rparen.Line, call.Rparen.Column, call.Name.Name, 2, len(call.Args))
		}

		// first arg should be set
		ok, baseType1 := typeIsSet(call.Args[0].DataType())
		if !ok {
			return nil, sql3.NewErrSetExpressionExpected(call.Args[0].Pos().Line, call.Args[0].Pos().Column)
		}

		// second arg should be set
		ok, baseType2 := typeIsSet(call.Args[1].DataType())
		if !ok {
			return nil, sql3.NewErrSetExpressionExpected(call.Args[0].Pos().Line, call.Args[0].Pos().Column)
		}

		//types from both set should be comparable
		if !typesAreComparable(baseType1, baseType2) {
			return nil, sql3.NewErrTypesAreNotEquatable(call.Args[1].Pos().Line, call.Args[1].Pos().Column, baseType1.TypeDescription(), baseType2.TypeDescription())
		}

		call.ResultDataType = parser.NewDataTypeBool()

	case "SETCONTAINSANY":
		if len(call.Args) != 2 {
			return nil, sql3.NewErrCallParameterCountMismatch(call.Rparen.Line, call.Rparen.Column, call.Name.Name, 2, len(call.Args))
		}

		// first arg should be set
		ok, baseType1 := typeIsSet(call.Args[0].DataType())
		if !ok {
			return nil, sql3.NewErrSetExpressionExpected(call.Args[0].Pos().Line, call.Args[0].Pos().Column)
		}

		// second arg should be set
		ok, baseType2 := typeIsSet(call.Args[1].DataType())
		if !ok {
			return nil, sql3.NewErrSetExpressionExpected(call.Args[0].Pos().Line, call.Args[0].Pos().Column)
		}

		// types from both sets should be comparable
		if !typesAreComparable(baseType1, baseType2) {
			return nil, sql3.NewErrTypesAreNotEquatable(call.Args[1].Pos().Line, call.Args[1].Pos().Column, baseType1.TypeDescription(), baseType2.TypeDescription())
		}

		call.ResultDataType = parser.NewDataTypeBool()

	case "DATEPART":
		return p.analyzeFunctionDatePart(call, scope)

	case "SUBTABLE":
		return p.analyzeFunctionSubtable(call, scope)
	case "REVERSE":
		return p.analyseFunctionReverse(call, scope)
	case "CHAR":
		return p.analyseFunctionChar(call, scope)
	case "UPPER":
		return p.analyzeFunctionUpper(call, scope)
	case "STRINGSPLIT":
		return p.analyseFunctionStringSplit(call, scope)
	case "SUBSTRING":
		return p.analyseFunctionSubstring(call, scope)
	case "LOWER":
		return p.analyzeFunctionLower(call, scope)
	case "REPLACEALL":
		return p.analyseFunctionReplaceAll(call, scope)
	case "TRIM":
		return p.analyseFunctionTrim(call, scope)
	case "RTRIM":
		return p.analyseFunctionTrim(call, scope)
	case "LTRIM":
		return p.analyseFunctionTrim(call, scope)
	case "SUFFIX":
		return p.analyseFunctionPrefixSuffix(call, scope)
	case "PREFIX":
		return p.analyseFunctionPrefixSuffix(call, scope)
	case "SPACE":
		return p.analyseFunctionSpace(call, scope)
	default:
		return nil, sql3.NewErrCallUnknownFunction(call.Name.NamePos.Line, call.Name.NamePos.Column, call.Name.Name)
	}
	return call, nil
}
