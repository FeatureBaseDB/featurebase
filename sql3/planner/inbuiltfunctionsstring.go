package planner

import (
	"strconv"
	"strings"

	"github.com/molecula/featurebase/v3/sql3"
	"github.com/molecula/featurebase/v3/sql3/parser"
	"github.com/molecula/featurebase/v3/sql3/planner/types"
)

func (p *ExecutionPlanner) analyseFunctionReverse(call *parser.Call, scope parser.Statement) (parser.Expr, error) {
	//one argument
	if len(call.Args) != 1 {
		return nil, sql3.NewErrCallParameterCountMismatch(call.Rparen.Line, call.Rparen.Column, call.Name.Name, 1, len(call.Args))
	}

	if !typeIsString(call.Args[0].DataType()) {
		return nil, sql3.NewErrStringExpressionExpected(call.Args[0].Pos().Line, call.Args[0].Pos().Column)
	}

	call.ResultDataType = parser.NewDataTypeString()

	return call, nil
}

func (p *ExecutionPlanner) analyseFunctionSubstring(call *parser.Call, scope parser.Statement) (parser.Expr, error) {
	if len(call.Args) <= 1 || len(call.Args) > 3 {
		return nil, sql3.NewErrCallParameterCountMismatch(call.Rparen.Line, call.Rparen.Column, call.Name.Name, 2, len(call.Args))
	}

	if !typeIsString(call.Args[0].DataType()) {
		return nil, sql3.NewErrStringExpressionExpected(call.Args[0].Pos().Line, call.Args[0].Pos().Column)
	}

	// the third parameter is optional
	for i := 1; i < len(call.Args); i++ {
		if !typeIsInteger(call.Args[i].DataType()) {
			return nil, sql3.NewErrIntExpressionExpected(call.Args[i].Pos().Line, call.Args[i].Pos().Column)
		}
	}

	call.ResultDataType = parser.NewDataTypeString()

	return call, nil
}

func (p *ExecutionPlanner) analyseFunctionReplaceAll(call *parser.Call, scope parser.Statement) (parser.Expr, error) {
	if len(call.Args) != 3 {
		return nil, sql3.NewErrCallParameterCountMismatch(call.Rparen.Line, call.Rparen.Column, call.Name.Name, 3, len(call.Args))
	}
	// input string
	if !typeIsString(call.Args[0].DataType()) {
		return nil, sql3.NewErrStringExpressionExpected(call.Args[0].Pos().Line, call.Args[0].Pos().Column)
	}
	// string to find and replace
	if !typeIsString(call.Args[1].DataType()) {
		return nil, sql3.NewErrStringExpressionExpected(call.Args[1].Pos().Line, call.Args[1].Pos().Column)
	}
	// string to replace with
	if !typeIsString(call.Args[2].DataType()) {
		return nil, sql3.NewErrStringExpressionExpected(call.Args[2].Pos().Line, call.Args[2].Pos().Column)
	}

	call.ResultDataType = parser.NewDataTypeString()

	return call, nil
}

// reverses the string
func (n *callPlanExpression) EvaluateReverse(currentRow []interface{}) (interface{}, error) {
	stringArgOne, err := evaluateStringArg(n.args[0], currentRow)
	if err != nil {
		return nil, err
	}

	// reverse the string
	runes := []rune(stringArgOne)
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}
	return string(runes), nil
}

func (p *ExecutionPlanner) analyzeFunctionUpper(call *parser.Call, scope parser.Statement) (parser.Expr, error) {
	//one argument for Upper Function
	if len(call.Args) != 1 {
		return nil, sql3.NewErrCallParameterCountMismatch(call.Rparen.Line, call.Rparen.Column, call.Name.Name, 1, len(call.Args))
	}

	if !typeIsString(call.Args[0].DataType()) {
		return nil, sql3.NewErrStringExpressionExpected(call.Args[0].Pos().Line, call.Args[0].Pos().Column)
	}

	call.ResultDataType = parser.NewDataTypeString()

	return call, nil
}

// Convert string to Upper case
func (n *callPlanExpression) EvaluateUpper(currentRow []interface{}) (interface{}, error) {
	stringArgOne, err := evaluateStringArg(n.args[0], currentRow)
	if err != nil {
		return nil, err
	}

	// convert to Upper
	return strings.ToUpper(stringArgOne), nil
}

// Takes string, startIndex and length and returns the substring.
func (n *callPlanExpression) EvaluateSubstring(currentRow []interface{}) (interface{}, error) {
	stringArgOne, err := evaluateStringArg(n.args[0], currentRow)
	if err != nil {
		return nil, err
	}

	// this takes a sliding window approach to evaluate substring.
	startIndex, err := strconv.Atoi(n.args[1].String())
	if err != nil {
		return nil, sql3.NewErrInternalf("unexpected type converion %T", n.args[1])
	}
	if startIndex >= len(stringArgOne) {
		return "", nil
	}

	endIndex := len(stringArgOne)
	if len(n.args) > 2 {
		ln, err := strconv.Atoi(n.args[2].String())
		if err != nil {
			return nil, sql3.NewErrInternalf("unexpected type converion %T", n.args[1])
		}
		endIndex = startIndex + ln
	}
	if endIndex < 0 {
		return "", nil
	}

	if startIndex < 0 {
		startIndex = 0
	}

	if endIndex > len(stringArgOne) {
		return stringArgOne[startIndex:], nil
	}

	return stringArgOne[startIndex:endIndex], nil
}

// takes string, findstring, replacestring.
// replaces all occurances of findstring with replacestring
func (n *callPlanExpression) EvaluateReplaceAll(currentRow []interface{}) (interface{}, error) {
	stringArgOne, err := evaluateStringArg(n.args[0], currentRow)
	if err != nil {
		return nil, err
	}
	stringArgTwo, err := evaluateStringArg(n.args[1], currentRow)
	if err != nil {
		return nil, err
	}
	stringArgThree, err := evaluateStringArg(n.args[2], currentRow)
	if err != nil {
		return nil, err
	}
	return strings.ReplaceAll(stringArgOne, stringArgTwo, stringArgThree), nil
}

func evaluateStringArg(n types.PlanExpression, currentRow []interface{}) (string, error) {
	argOneEval, err := n.Evaluate(currentRow)
	if err != nil {
		return "", err
	}

	stringArgOne, ok := argOneEval.(string)
	if !ok {
		return "", sql3.NewErrInternalf("unexpected type converion %T", argOneEval)
	}

	return stringArgOne, nil
}
