package planner

import (
	"github.com/featurebasedb/featurebase/v3/sql3"
	"github.com/featurebasedb/featurebase/v3/sql3/parser"
)

func (p *ExecutionPlanner) analyzeFunctionRangeQ(call *parser.Call, scope parser.Statement) (parser.Expr, error) {
	if len(call.Args) != 3 {
		return nil, sql3.NewErrCallParameterCountMismatch(call.Rparen.Line, call.Rparen.Column, call.Name.Name, 3, len(call.Args))
	}

	// first arg should be time quantum type
	ok, _ := typeIsTimeQuantum(call.Args[0].DataType())
	if !ok {
		return nil, sql3.NewErrTimeQuantumExpressionExpected(call.Args[0].Pos().Line, call.Args[0].Pos().Column)
	}

	// second is 'from' timestamp, can be null
	targetType := parser.NewDataTypeTimestamp()
	if !typesAreAssignmentCompatible(targetType, call.Args[1].DataType()) {
		return nil, sql3.NewErrTypeAssignmentIncompatible(call.Args[0].Pos().Line, call.Args[0].Pos().Column, call.Args[0].DataType().TypeDescription(), targetType.TypeDescription())
	}

	_, fromLiteralNull := call.Args[1].(*parser.NullLit)

	// second is 'to' timestamp, can be null
	if !typesAreAssignmentCompatible(targetType, call.Args[2].DataType()) {
		return nil, sql3.NewErrTypeAssignmentIncompatible(call.Args[0].Pos().Line, call.Args[0].Pos().Column, call.Args[0].DataType().TypeDescription(), targetType.TypeDescription())
	}
	_, toLiteralNull := call.Args[2].(*parser.NullLit)

	// if we have two literal nulls we have a problem
	if fromLiteralNull && toLiteralNull {
		return nil, sql3.NewErrQRangeFromAndToTimeCannotBeBothNull(call.Rparen.Line, call.Rparen.Column)
	}

	call.ResultDataType = parser.NewDataTypeBool()

	return call, nil
}

func (n *callPlanExpression) EvaluateRangeQ(currentRow []interface{}) (interface{}, error) {
	// rangeq() should only ever be used as a push down filter for now - if we get to here, we should error
	return nil, sql3.NewErrQRangeInvalidUse(0, 0)
}
