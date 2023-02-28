package planner

import (
	"github.com/featurebasedb/featurebase/v3/sql3"
	"github.com/featurebasedb/featurebase/v3/sql3/parser"
)

func (p *ExecutionPlanner) analyseFunctionQRangeGt(call *parser.Call, scope parser.Statement) (parser.Expr, error) {
	if len(call.Args) != 2 {
		return nil, sql3.NewErrCallParameterCountMismatch(call.Rparen.Line, call.Rparen.Column, call.Name.Name, 1, len(call.Args))
	}

	// first arg should be time quantum
	ok, _ := typeIsTimeQuantum(call.Args[0].DataType())
	if !ok {
		return nil, sql3.NewErrTimeQuantumExpressionExpected(call.Args[0].Pos().Line, call.Args[0].Pos().Column)
	}

	// second is timestamp
	targetType := parser.NewDataTypeTimestamp()
	if !typesAreAssignmentCompatible(targetType, call.Args[1].DataType()) {
		return nil, sql3.NewErrTypeAssignmentIncompatible(call.Args[0].Pos().Line, call.Args[0].Pos().Column, call.Args[0].DataType().TypeDescription(), targetType.TypeDescription())
	}

	call.ResultDataType = parser.NewDataTypeBool()

	return call, nil
}

func (n *callPlanExpression) EvaluateQRangeGt(currentRow []interface{}) (interface{}, error) {
	targetSetEval, err := n.args[0].Evaluate(currentRow)
	if err != nil {
		return nil, err
	}

	testSetEval, err := n.args[1].Evaluate(currentRow)
	if err != nil {
		return nil, err
	}

	if targetSetEval != nil {
		switch typ := n.args[0].Type().(type) {
		case *parser.DataTypeStringSet:
			targetSet, ok := targetSetEval.([]string)
			if !ok {
				return nil, sql3.NewErrInternalf("unable to convert value")
			}

			testSet, ok := testSetEval.([]string)
			if !ok {
				return nil, sql3.NewErrInternalf("unable to convert value")
			}

			return stringSetContainsAll(targetSet, testSet), nil

		case *parser.DataTypeIDSet:
			targetSet, ok := targetSetEval.([]int64)
			if !ok {
				return nil, sql3.NewErrInternalf("unable to convert value")
			}

			testSet, ok := testSetEval.([]int64)
			if !ok {
				return nil, sql3.NewErrInternalf("unable to convert value")
			}

			return intSetContainsAll(targetSet, testSet), nil

		default:
			return nil, sql3.NewErrInternalf("unexpected data type '%T'", typ)
		}

	}

	return nil, sql3.NewErrInternalf("unable to to find column '%s' in currentColumns", n.name)
}
