package planner

import (
	"strings"

	"github.com/featurebasedb/featurebase/v3/sql3"
	"github.com/featurebasedb/featurebase/v3/sql3/parser"
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

func (p *ExecutionPlanner) analyseFunctionChar(call *parser.Call, scope parser.Statement) (parser.Expr, error) {
	//one argument
	if len(call.Args) != 1 {
		return nil, sql3.NewErrCallParameterCountMismatch(call.Rparen.Line, call.Rparen.Column, call.Name.Name, 1, len(call.Args))
	}

	if !typeIsInteger(call.Args[0].DataType()) {
		return nil, sql3.NewErrIntExpressionExpected(call.Args[0].Pos().Line, call.Args[0].Pos().Column)
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

func (p *ExecutionPlanner) analyzeFunctionLower(call *parser.Call, scope parser.Statement) (parser.Expr, error) {
	if len(call.Args) != 1 {
		return nil, sql3.NewErrCallParameterCountMismatch(call.Rparen.Line, call.Rparen.Column, call.Name.Name, 1, len(call.Args))
	}

	if !typeIsString(call.Args[0].DataType()) {
		return nil, sql3.NewErrStringExpressionExpected(call.Args[0].Pos().Line, call.Args[0].Pos().Column)
	}
	call.ResultDataType = parser.NewDataTypeString()

	return call, nil
}

func (p *ExecutionPlanner) analyseFunctionStringSplit(call *parser.Call, scope parser.Statement) (parser.Expr, error) {
	if len(call.Args) <= 1 || len(call.Args) > 3 {
		return nil, sql3.NewErrCallParameterCountMismatch(call.Rparen.Line, call.Rparen.Column, call.Name.Name, 2, len(call.Args))
	}

	if !typeIsString(call.Args[0].DataType()) {
		return nil, sql3.NewErrStringExpressionExpected(call.Args[0].Pos().Line, call.Args[0].Pos().Column)
	}

	// string seperator
	if !typeIsString(call.Args[1].DataType()) {
		return nil, sql3.NewErrStringExpressionExpected(call.Args[1].Pos().Line, call.Args[1].Pos().Column)
	}

	// third argument is the position. optional, defaults to 0
	if len(call.Args) == 3 && !typeIsInteger(call.Args[2].DataType()) {
		return nil, sql3.NewErrIntExpressionExpected(call.Args[2].Pos().Line, call.Args[2].Pos().Column)
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

func (n *callPlanExpression) EvaluateChar(currentRow []interface{}) (interface{}, error) {
	// Get the integer argument from the function call
	intArg, err := evaluateIntArg(n.args[0], currentRow)
	if err != nil {
		return "", err
	}

	// Return the character that corresponds to the integer value
	return string(rune(intArg)), nil
}

// Takes string, startIndex and length and returns the substring.
func (n *callPlanExpression) EvaluateSubstring(currentRow []interface{}) (interface{}, error) {
	stringArgOne, err := evaluateStringArg(n.args[0], currentRow)
	if err != nil {
		return nil, err
	}

	// this takes a sliding window approach to evaluate substring.
	startIndex, err := evaluateIntArg(n.args[1], currentRow)
	if err != nil {
		return nil, err
	}

	if startIndex < 0 {
		return nil, sql3.NewErrValueOutOfRange(0, 0, startIndex)
	}

	if startIndex >= len(stringArgOne) {
		return nil, sql3.NewErrValueOutOfRange(0, 0, startIndex)
	}

	endIndex := len(stringArgOne)
	if len(n.args) > 2 {
		ln, err := evaluateIntArg(n.args[2], currentRow)
		if err != nil {
			return nil, err
		}
		endIndex = startIndex + ln
	}

	if endIndex < startIndex {
		return nil, sql3.NewErrValueOutOfRange(0, 0, endIndex)
	}

	if endIndex > len(stringArgOne) {
		return nil, sql3.NewErrValueOutOfRange(0, 0, endIndex)
	}

	return stringArgOne[startIndex:endIndex], nil
}

func (n *callPlanExpression) EvaluateLower(currentRow []interface{}) (interface{}, error) {
	stringArgOne, err := evaluateStringArg(n.args[0], currentRow)
	if err != nil {
		return nil, err
	}

	return strings.ToLower(stringArgOne), nil
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

// takes a string, seperator and the position `n`, splits the string and returns n'th substring
func (n *callPlanExpression) EvaluateStringSplit(currentRow []interface{}) (interface{}, error) {
	inputString, err := evaluateStringArg(n.args[0], currentRow)
	if err != nil {
		return nil, err
	}
	seperator, err := evaluateStringArg(n.args[1], currentRow)
	if err != nil {
		return nil, err
	}
	if len(n.args) == 2 {
		return strings.Split(inputString, seperator)[0], nil
	}
	pos, err := evaluateIntArg(n.args[2], currentRow)
	if err != nil {
		return nil, err
	}

	res := strings.Split(inputString, seperator)
	if pos <= 0 {
		return res[0], nil
	} else if len(res) > pos {
		return res[pos], nil
	}
	return "", nil
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

func evaluateIntArg(n types.PlanExpression, currentRow []interface{}) (int, error) {
	argOneEval, err := n.Evaluate(currentRow)
	if err != nil {
		return 0, err
	}

	intArgOne, ok := argOneEval.(int64)
	if !ok {
		return 0, sql3.NewErrInternalf("unexpected type converion %T", argOneEval)
	}

	return int(intArgOne), nil
}

// Analyze function for Trim/RTrim/LTrim
func (p *ExecutionPlanner) analyseFunctionTrim(call *parser.Call, scope parser.Statement) (parser.Expr, error) {
	//one argument for Trim Functions
	if len(call.Args) != 1 {
		return nil, sql3.NewErrCallParameterCountMismatch(call.Rparen.Line, call.Rparen.Column, call.Name.Name, 1, len(call.Args))
	}

	if !typeIsString(call.Args[0].DataType()) {
		return nil, sql3.NewErrStringExpressionExpected(call.Args[0].Pos().Line, call.Args[0].Pos().Column)
	}

	call.ResultDataType = parser.NewDataTypeString()

	return call, nil
}

// Execute Trim function to remove whitespaces from string
func (n *callPlanExpression) EvaluateTrim(currentRow []interface{}) (interface{}, error) {
	stringArgOne, err := evaluateStringArg(n.args[0], currentRow)
	if err != nil {
		return nil, err
	}

	// Trim the whitespace from string
	return strings.TrimSpace(stringArgOne), nil
}

// Execute RTrim function to remove trailing whitespaces from string
func (n *callPlanExpression) EvaluateRTrim(currentRow []interface{}) (interface{}, error) {
	stringArgOne, err := evaluateStringArg(n.args[0], currentRow)
	if err != nil {
		return nil, err
	}

	// Trim the trailing whitespace from string
	return strings.TrimRight(stringArgOne, " "), nil
}

// Execute LTrim function to remove leading whitespaces from string
func (n *callPlanExpression) EvaluateLTrim(currentRow []interface{}) (interface{}, error) {
	stringArgOne, err := evaluateStringArg(n.args[0], currentRow)
	if err != nil {
		return nil, err
	}

	// Trim the leading whitespace from string
	return strings.TrimLeft(stringArgOne, " "), nil
}

func (p *ExecutionPlanner) analyseFunctionPrefixSuffix(call *parser.Call, scope parser.Statement) (parser.Expr, error) {
	if len(call.Args) != 2 {
		return nil, sql3.NewErrCallParameterCountMismatch(call.Rparen.Line, call.Rparen.Column, call.Name.Name, 2, len(call.Args))
	}

	if !typeIsString(call.Args[0].DataType()) {
		return nil, sql3.NewErrStringExpressionExpected(call.Args[0].Pos().Line, call.Args[0].Pos().Column)
	}

	if !typeIsInteger(call.Args[1].DataType()) {
		return nil, sql3.NewErrIntExpressionExpected(call.Args[1].Pos().Line, call.Args[1].Pos().Column)
	}

	call.ResultDataType = parser.NewDataTypeString()

	return call, nil
}

func (n *callPlanExpression) EvaluatePrefix(currentRow []interface{}) (interface{}, error) {
	stringArgOne, err := evaluateStringArg(n.args[0], currentRow)
	if err != nil {
		return nil, err
	}

	intArgTwo, err := evaluateIntArg(n.args[1], currentRow)
	if err != nil {
		return nil, err
	}

	// if the length is less than zero, out of range
	if intArgTwo < 0 {
		return nil, sql3.NewErrValueOutOfRange(0, 0, intArgTwo)
	}

	if intArgTwo > len(stringArgOne) {
		return nil, sql3.NewErrValueOutOfRange(0, 0, intArgTwo)
	}

	return stringArgOne[:intArgTwo], nil
}

func (n *callPlanExpression) EvaluateSuffix(currentRow []interface{}) (interface{}, error) {
	stringArgOne, err := evaluateStringArg(n.args[0], currentRow)
	if err != nil {
		return nil, err
	}

	intArgTwo, err := evaluateIntArg(n.args[1], currentRow)
	if err != nil {
		return nil, err
	}

	// if the length is less than zero, out of range
	if intArgTwo < 0 {
		return nil, sql3.NewErrValueOutOfRange(0, 0, intArgTwo)
	}

	if intArgTwo > len(stringArgOne) {
		return nil, sql3.NewErrValueOutOfRange(0, 0, intArgTwo)
	}

	return stringArgOne[len(stringArgOne)-intArgTwo:], nil
}
