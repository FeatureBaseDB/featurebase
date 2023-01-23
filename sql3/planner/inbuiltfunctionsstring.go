package planner

import (
	"fmt"
	"strings"

	"github.com/featurebasedb/featurebase/v3/pql"
	"github.com/featurebasedb/featurebase/v3/sql3"
	"github.com/featurebasedb/featurebase/v3/sql3/parser"
)

func (p *ExecutionPlanner) analyseFunctionReverse(call *parser.Call, scope parser.Statement) (parser.Expr, error) {
	//one argument
	if len(call.Args) != 1 {
		return nil, sql3.NewErrCallParameterCountMismatch(call.Rparen.Line, call.Rparen.Column, call.Name.Name, 1, len(call.Args))
	}

	if !typeIsString(call.Args[0].DataType()) && !typeIsVoid(call.Args[0].DataType()) {
		return nil, sql3.NewErrStringExpressionExpected(call.Args[0].Pos().Line, call.Args[0].Pos().Column)
	}

	call.ResultDataType = parser.NewDataTypeString()

	return call, nil
}

func (p *ExecutionPlanner) analyzeFunctionLower(call *parser.Call, scope parser.Statement) (parser.Expr, error) {
	if len(call.Args) != 1 {
		return nil, sql3.NewErrCallParameterCountMismatch(call.Rparen.Line, call.Rparen.Column, call.Name.Name, 1, len(call.Args))
	}

	if !typeIsString(call.Args[0].DataType()) && !typeIsVoid(call.Args[0].DataType()) {
		return nil, sql3.NewErrStringExpressionExpected(call.Args[0].Pos().Line, call.Args[0].Pos().Column)
	}
	call.ResultDataType = parser.NewDataTypeString()

	return call, nil
}

func (p *ExecutionPlanner) analyzeFunctionUpper(call *parser.Call, scope parser.Statement) (parser.Expr, error) {
	//one argument for Upper Function
	if len(call.Args) != 1 {
		return nil, sql3.NewErrCallParameterCountMismatch(call.Rparen.Line, call.Rparen.Column, call.Name.Name, 1, len(call.Args))
	}

	if !typeIsString(call.Args[0].DataType()) && !typeIsVoid(call.Args[0].DataType()) {
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

	if !typeIsInteger(call.Args[0].DataType()) && !typeIsVoid(call.Args[0].DataType()) {
		return nil, sql3.NewErrIntExpressionExpected(call.Args[0].Pos().Line, call.Args[0].Pos().Column)
	}

	call.ResultDataType = parser.NewDataTypeString()

	return call, nil
}

func (p *ExecutionPlanner) analyseFunctionAscii(call *parser.Call, scope parser.Statement) (parser.Expr, error) {
	//one argument
	if len(call.Args) != 1 {
		return nil, sql3.NewErrCallParameterCountMismatch(call.Rparen.Line, call.Rparen.Column, call.Name.Name, 1, len(call.Args))
	}

	if !typeIsString(call.Args[0].DataType()) && !typeIsVoid(call.Args[0].DataType()) {
		return nil, sql3.NewErrStringExpressionExpected(call.Args[0].Pos().Line, call.Args[0].Pos().Column)
	}

	call.ResultDataType = parser.NewDataTypeInt()

	return call, nil
}

func (p *ExecutionPlanner) analyseFunctionSubstring(call *parser.Call, scope parser.Statement) (parser.Expr, error) {
	if len(call.Args) <= 1 || len(call.Args) > 3 {
		return nil, sql3.NewErrCallParameterCountMismatch(call.Rparen.Line, call.Rparen.Column, call.Name.Name, 2, len(call.Args))
	}

	if !typeIsString(call.Args[0].DataType()) && !typeIsVoid(call.Args[0].DataType()) {
		return nil, sql3.NewErrStringExpressionExpected(call.Args[0].Pos().Line, call.Args[0].Pos().Column)
	}

	// the third parameter is optional
	for i := 1; i < len(call.Args); i++ {
		if !typeIsInteger(call.Args[i].DataType()) && !typeIsVoid(call.Args[i].DataType()) {
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
	if !typeIsString(call.Args[0].DataType()) && !typeIsVoid(call.Args[0].DataType()) {
		return nil, sql3.NewErrStringExpressionExpected(call.Args[0].Pos().Line, call.Args[0].Pos().Column)
	}
	// string to find and replace
	if !typeIsString(call.Args[1].DataType()) && !typeIsVoid(call.Args[1].DataType()) {
		return nil, sql3.NewErrStringExpressionExpected(call.Args[1].Pos().Line, call.Args[1].Pos().Column)
	}
	// string to replace with
	if !typeIsString(call.Args[2].DataType()) && !typeIsVoid(call.Args[2].DataType()) {
		return nil, sql3.NewErrStringExpressionExpected(call.Args[2].Pos().Line, call.Args[2].Pos().Column)
	}

	call.ResultDataType = parser.NewDataTypeString()

	return call, nil
}

func (p *ExecutionPlanner) analyseFunctionStringSplit(call *parser.Call, scope parser.Statement) (parser.Expr, error) {
	if len(call.Args) <= 1 || len(call.Args) > 3 {
		return nil, sql3.NewErrCallParameterCountMismatch(call.Rparen.Line, call.Rparen.Column, call.Name.Name, 2, len(call.Args))
	}

	if !typeIsString(call.Args[0].DataType()) && !typeIsVoid(call.Args[0].DataType()) {
		return nil, sql3.NewErrStringExpressionExpected(call.Args[0].Pos().Line, call.Args[0].Pos().Column)
	}

	// string seperator
	if !typeIsString(call.Args[1].DataType()) && !typeIsVoid(call.Args[1].DataType()) {
		return nil, sql3.NewErrStringExpressionExpected(call.Args[1].Pos().Line, call.Args[1].Pos().Column)
	}

	// third argument is the position. optional, defaults to 0
	if len(call.Args) == 3 && !typeIsInteger(call.Args[2].DataType()) && !typeIsVoid(call.Args[2].DataType()) {
		return nil, sql3.NewErrIntExpressionExpected(call.Args[2].Pos().Line, call.Args[2].Pos().Column)
	}

	call.ResultDataType = parser.NewDataTypeString()

	return call, nil
}

// Analyze function for Trim/RTrim/LTrim
func (p *ExecutionPlanner) analyseFunctionTrim(call *parser.Call, scope parser.Statement) (parser.Expr, error) {
	//one argument for Trim Functions
	if len(call.Args) != 1 {
		return nil, sql3.NewErrCallParameterCountMismatch(call.Rparen.Line, call.Rparen.Column, call.Name.Name, 1, len(call.Args))
	}

	if !typeIsString(call.Args[0].DataType()) && !typeIsVoid(call.Args[0].DataType()) {
		return nil, sql3.NewErrStringExpressionExpected(call.Args[0].Pos().Line, call.Args[0].Pos().Column)
	}

	call.ResultDataType = parser.NewDataTypeString()

	return call, nil
}

func (p *ExecutionPlanner) analyseFunctionPrefixSuffixReplicate(call *parser.Call, scope parser.Statement) (parser.Expr, error) {
	if len(call.Args) != 2 {
		return nil, sql3.NewErrCallParameterCountMismatch(call.Rparen.Line, call.Rparen.Column, call.Name.Name, 2, len(call.Args))
	}

	if !typeIsString(call.Args[0].DataType()) && !typeIsVoid(call.Args[0].DataType()) {
		return nil, sql3.NewErrStringExpressionExpected(call.Args[0].Pos().Line, call.Args[0].Pos().Column)
	}

	if !typeIsInteger(call.Args[1].DataType()) && !typeIsVoid(call.Args[1].DataType()) {
		return nil, sql3.NewErrIntExpressionExpected(call.Args[1].Pos().Line, call.Args[1].Pos().Column)
	}

	call.ResultDataType = parser.NewDataTypeString()

	return call, nil
}

func (p *ExecutionPlanner) analyseFunctionSpace(call *parser.Call, scope parser.Statement) (parser.Expr, error) {
	//one argument
	if len(call.Args) != 1 {
		return nil, sql3.NewErrCallParameterCountMismatch(call.Rparen.Line, call.Rparen.Column, call.Name.Name, 1, len(call.Args))
	}

	if !typeIsInteger(call.Args[0].DataType()) && !typeIsVoid(call.Args[0].DataType()) {
		return nil, sql3.NewErrIntExpressionExpected(call.Args[0].Pos().Line, call.Args[0].Pos().Column)
	}

	call.ResultDataType = parser.NewDataTypeString()
	return call, nil
}

func (p *ExecutionPlanner) analyseFunctionLen(call *parser.Call, scope parser.Statement) (parser.Expr, error) {
	if len(call.Args) != 1 {
		return nil, sql3.NewErrCallParameterCountMismatch(call.Rparen.Line, call.Rparen.Column, call.Name.Name, 1, len(call.Args))
	}

	if !typeIsString(call.Args[0].DataType()) && !typeIsVoid(call.Args[0].DataType()) {
		return nil, sql3.NewErrStringExpressionExpected(call.Args[0].Pos().Line, call.Args[0].Pos().Column)
	}
	call.ResultDataType = parser.NewDataTypeInt()
	return call, nil
}

// format(format_string, args...)
func (p *ExecutionPlanner) analyseFunctionFormat(call *parser.Call, scope parser.Statement) (parser.Expr, error) {
	// should have at least one argument to check call.args[0] string
	if len(call.Args) < 1 {
		return nil, sql3.NewErrCallParameterCountMismatch(call.Rparen.Line, call.Rparen.Column, call.Name.Name, 1, len(call.Args))
	}
	if !typeIsString(call.Args[0].DataType()) && !typeIsVoid(call.Args[0].DataType()) {
		return nil, sql3.NewErrStringExpressionExpected(call.Args[0].Pos().Line, call.Args[0].Pos().Column)
	}

	for i := 1; i < len(call.Args); i++ {
		if typeIsVoid(call.Args[i].DataType()) {
			return nil, sql3.NewErrLiteralNullNotAllowed(call.Args[i].Pos().Line, call.Args[i].Pos().Column)
		}
	}
	call.ResultDataType = parser.NewDataTypeString()
	return call, nil
}

// charindex(substring, str, pos)
func (p *ExecutionPlanner) analyseFunctionCharIndex(call *parser.Call, scope parser.Statement) (parser.Expr, error) {
	if len(call.Args) <= 1 || len(call.Args) > 3 {
		return nil, sql3.NewErrCallParameterCountMismatch(call.Rparen.Line, call.Rparen.Column, call.Name.Name, 3, len(call.Args))
	}
	// first paramater is the substring for which index needs to be identified
	if !typeIsString(call.Args[0].DataType()) && !typeIsVoid(call.Args[0].DataType()) {
		return nil, sql3.NewErrStringExpressionExpected(call.Args[0].Pos().Line, call.Args[0].Pos().Column)
	}

	// second paramater is the string where we search the position of the substring ( first param)
	if !typeIsString(call.Args[1].DataType()) && !typeIsVoid(call.Args[1].DataType()) {
		return nil, sql3.NewErrStringExpressionExpected(call.Args[1].Pos().Line, call.Args[1].Pos().Column)
	}

	// the third parameter is the position. optional, defaults to 0
	if len(call.Args) == 3 && !typeIsInteger(call.Args[2].DataType()) && !typeIsVoid(call.Args[2].DataType()) {
		return nil, sql3.NewErrIntExpressionExpected(call.Args[2].Pos().Line, call.Args[2].Pos().Column)
	}

	call.ResultDataType = parser.NewDataTypeInt()

	return call, nil
}

// EvaluateReverse reverses the string
func (n *callPlanExpression) EvaluateReverse(currentRow []interface{}) (interface{}, error) {
	argEval, err := n.args[0].Evaluate(currentRow)
	if err != nil {
		return nil, err
	}
	if argEval == nil {
		return nil, nil
	}
	stringArg, ok := argEval.(string)
	if !ok {
		return nil, sql3.NewErrUnexpectedTypeConversion(0, 0, argEval)
	}

	// reverse the string
	runes := []rune(stringArg)
	for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
		runes[i], runes[j] = runes[j], runes[i]
	}
	return string(runes), nil
}

func (n *callPlanExpression) EvaluateLower(currentRow []interface{}) (interface{}, error) {
	argEval, err := n.args[0].Evaluate(currentRow)
	if err != nil {
		return nil, err
	}
	if argEval == nil {
		return nil, nil
	}
	stringArg, ok := argEval.(string)
	if !ok {
		return nil, sql3.NewErrUnexpectedTypeConversion(0, 0, argEval)
	}

	return strings.ToLower(stringArg), nil
}

// EvaluateUpper converts string to Upper case
func (n *callPlanExpression) EvaluateUpper(currentRow []interface{}) (interface{}, error) {
	argEval, err := n.args[0].Evaluate(currentRow)
	if err != nil {
		return nil, err
	}
	if argEval == nil {
		return nil, nil
	}
	stringArg, ok := argEval.(string)
	if !ok {
		return nil, sql3.NewErrUnexpectedTypeConversion(0, 0, argEval)
	}

	// convert to Upper
	return strings.ToUpper(stringArg), nil
}

func (n *callPlanExpression) EvaluateChar(currentRow []interface{}) (interface{}, error) {
	// Get the integer argument from the function call
	argEval, err := n.args[0].Evaluate(currentRow)
	if err != nil {
		return 0, err
	}
	if argEval == nil {
		return nil, nil
	}
	intArg, ok := argEval.(int64)
	if !ok {
		return 0, sql3.NewErrUnexpectedTypeConversion(0, 0, argEval)
	}
	// ascii range is [0-255]
	if intArg < 0 || intArg > 255 {
		return nil, sql3.NewErrValueOutOfRange(0, 0, intArg)
	}

	// Return the character that corresponds to the integer value
	return string(rune(intArg)), nil
}

// EvaluateAscii this takes a string and returns the ascii value.
// sthe string should be of the length 1.
func (n *callPlanExpression) EvaluateAscii(currentRow []interface{}) (interface{}, error) {
	// Get the string argument from the function call
	argEval, err := n.args[0].Evaluate(currentRow)
	if err != nil {
		return nil, err
	}
	if argEval == nil {
		return nil, nil
	}
	stringArg, ok := argEval.(string)
	if !ok {
		return nil, sql3.NewErrUnexpectedTypeConversion(0, 0, argEval)
	}

	if len(stringArg) == 0 {
		return "", nil
	}

	if len(stringArg) != 1 {
		return nil, sql3.NewErrStringLengthMismatch(0, 0, 1, stringArg)
	}

	res := []rune(stringArg)
	return int64(res[0]), nil
}

// EvaluateSubstring takes string, startIndex and length and returns the substring.
func (n *callPlanExpression) EvaluateSubstring(currentRow []interface{}) (interface{}, error) {
	argEval, err := n.args[0].Evaluate(currentRow)
	if err != nil {
		return nil, err
	}
	if argEval == nil {
		return nil, nil
	}
	stringArgOne, ok := argEval.(string)
	if !ok {
		return nil, sql3.NewErrUnexpectedTypeConversion(0, 0, argEval)
	}

	// this takes a sliding window approach to evaluate substring.
	argEval, err = n.args[1].Evaluate(currentRow)
	if err != nil {
		return 0, err
	}
	if argEval == nil {
		return nil, nil
	}

	startIndex, ok := argEval.(int64)
	if !ok {
		return 0, sql3.NewErrUnexpectedTypeConversion(0, 0, argEval)
	}

	if startIndex < 0 || startIndex >= int64(len(stringArgOne)) {
		return nil, sql3.NewErrValueOutOfRange(0, 0, startIndex)
	}

	endIndex := int64(len(stringArgOne))
	if len(n.args) > 2 {
		argEval, err = n.args[2].Evaluate(currentRow)
		if err != nil {
			return 0, err
		}
		if argEval == nil {
			return nil, nil
		}
		ln, ok := argEval.(int64)
		if !ok {
			return 0, sql3.NewErrUnexpectedTypeConversion(0, 0, argEval)
		}
		endIndex = startIndex + ln
	}

	if endIndex < startIndex || endIndex > int64(len(stringArgOne)) {
		return nil, sql3.NewErrValueOutOfRange(0, 0, endIndex)
	}

	return stringArgOne[startIndex:endIndex], nil
}

// EvaluateReplaceAll takes string, findstring, replacestring.
// replaces all occurances of findstring with replacestring
func (n *callPlanExpression) EvaluateReplaceAll(currentRow []interface{}) (interface{}, error) {
	argEval, err := n.args[0].Evaluate(currentRow)
	if err != nil {
		return nil, err
	}
	if argEval == nil {
		return nil, nil
	}
	stringArgOne, ok := argEval.(string)
	if !ok {
		return nil, sql3.NewErrUnexpectedTypeConversion(0, 0, argEval)
	}
	argEval, err = n.args[1].Evaluate(currentRow)
	if err != nil {
		return nil, err
	}
	if argEval == nil {
		return nil, nil
	}
	stringArgTwo, ok := argEval.(string)
	if !ok {
		return nil, sql3.NewErrUnexpectedTypeConversion(0, 0, argEval)
	}
	argEval, err = n.args[2].Evaluate(currentRow)
	if err != nil {
		return nil, err
	}
	if argEval == nil {
		return nil, nil
	}
	stringArgThree, ok := argEval.(string)
	if !ok {
		return nil, sql3.NewErrUnexpectedTypeConversion(0, 0, argEval)
	}
	return strings.ReplaceAll(stringArgOne, stringArgTwo, stringArgThree), nil
}

// EvaluateStringSplit takes a string, seperator and the position `n`, splits the string and returns n'th substring
func (n *callPlanExpression) EvaluateStringSplit(currentRow []interface{}) (interface{}, error) {
	argEval, err := n.args[0].Evaluate(currentRow)
	if err != nil {
		return nil, err
	}
	if argEval == nil {
		return nil, nil
	}
	inputString, ok := argEval.(string)
	if !ok {
		return nil, sql3.NewErrUnexpectedTypeConversion(0, 0, argEval)
	}

	argEval, err = n.args[1].Evaluate(currentRow)
	if err != nil {
		return nil, err
	}
	if argEval == nil {
		return nil, nil
	}
	seperator, ok := argEval.(string)
	if !ok {
		return nil, sql3.NewErrUnexpectedTypeConversion(0, 0, argEval)
	}

	if len(n.args) == 2 {
		return strings.Split(inputString, seperator)[0], nil
	}
	argEval, err = n.args[2].Evaluate(currentRow)
	if err != nil {
		return nil, err
	}
	if argEval == nil {
		return nil, nil
	}
	pos, ok := argEval.(int64)
	if !ok {
		return nil, sql3.NewErrUnexpectedTypeConversion(0, 0, argEval)
	}

	res := strings.Split(inputString, seperator)
	if pos <= 0 {
		return res[0], nil
	} else if int64(len(res)) > pos {
		return res[pos], nil
	}
	return "", nil
}

// EvaluateTrim function to remove whitespaces from string
func (n *callPlanExpression) EvaluateTrim(currentRow []interface{}) (interface{}, error) {
	argEval, err := n.args[0].Evaluate(currentRow)
	if err != nil {
		return nil, err
	}
	if argEval == nil {
		return nil, nil
	}
	stringArg, ok := argEval.(string)
	if !ok {
		return nil, sql3.NewErrUnexpectedTypeConversion(0, 0, argEval)
	}

	// Trim the whitespace from string
	return strings.TrimSpace(stringArg), nil
}

// EvaluateRTrim function removes trailing whitespaces from string
func (n *callPlanExpression) EvaluateRTrim(currentRow []interface{}) (interface{}, error) {
	argEval, err := n.args[0].Evaluate(currentRow)
	if err != nil {
		return nil, err
	}
	if argEval == nil {
		return nil, nil
	}
	stringArg, ok := argEval.(string)
	if !ok {
		return nil, sql3.NewErrUnexpectedTypeConversion(0, 0, argEval)
	}

	// Trim the trailing whitespace from string
	return strings.TrimRight(stringArg, " "), nil
}

// EvaluateLTrim function removes leading whitespaces from string
func (n *callPlanExpression) EvaluateLTrim(currentRow []interface{}) (interface{}, error) {
	argEval, err := n.args[0].Evaluate(currentRow)
	if err != nil {
		return nil, err
	}
	if argEval == nil {
		return nil, nil
	}
	stringArg, ok := argEval.(string)
	if !ok {
		return nil, sql3.NewErrUnexpectedTypeConversion(0, 0, argEval)
	}

	// Trim the leading whitespace from string
	return strings.TrimLeft(stringArg, " "), nil
}

func (n *callPlanExpression) EvaluatePrefix(currentRow []interface{}) (interface{}, error) {
	argEval, err := n.args[0].Evaluate(currentRow)
	if err != nil {
		return nil, err
	}
	if argEval == nil {
		return nil, nil
	}
	stringArgOne, ok := argEval.(string)
	if !ok {
		return nil, sql3.NewErrUnexpectedTypeConversion(0, 0, argEval)
	}

	argEval, err = n.args[1].Evaluate(currentRow)
	if err != nil {
		return nil, err
	}
	if argEval == nil {
		return nil, nil
	}
	intArgTwo, ok := argEval.(int64)
	if !ok {
		return nil, sql3.NewErrUnexpectedTypeConversion(0, 0, argEval)
	}

	if intArgTwo < 0 || intArgTwo > int64(len(stringArgOne)) {
		return nil, sql3.NewErrValueOutOfRange(0, 0, intArgTwo)
	}

	return stringArgOne[:intArgTwo], nil
}

func (n *callPlanExpression) EvaluateSuffix(currentRow []interface{}) (interface{}, error) {
	argEval, err := n.args[0].Evaluate(currentRow)
	if err != nil {
		return nil, err
	}
	if argEval == nil {
		return nil, nil
	}
	stringArgOne, ok := argEval.(string)
	if !ok {
		return nil, sql3.NewErrUnexpectedTypeConversion(0, 0, argEval)
	}

	argEval, err = n.args[1].Evaluate(currentRow)
	if err != nil {
		return nil, err
	}
	if argEval == nil {
		return nil, nil
	}
	intArgTwo, ok := argEval.(int64)
	if !ok {
		return nil, sql3.NewErrUnexpectedTypeConversion(0, 0, argEval)
	}

	if intArgTwo < 0 || intArgTwo > int64(len(stringArgOne)) {
		return nil, sql3.NewErrValueOutOfRange(0, 0, intArgTwo)
	}

	return stringArgOne[int64(len(stringArgOne))-intArgTwo:], nil
}

func (n *callPlanExpression) EvaluateSpace(currentRow []interface{}) (interface{}, error) {
	// Get the integer argument from the function call
	argEval, err := n.args[0].Evaluate(currentRow)
	if err != nil {
		return nil, err
	}
	if argEval == nil {
		return nil, nil
	}
	intArg, ok := argEval.(int64)
	if !ok {
		return nil, sql3.NewErrUnexpectedTypeConversion(0, 0, argEval)
	}

	// Return a string containing a number of spaces equal to the integer value
	spaces := ""
	for i := int64(0); i < intArg; i++ {
		spaces += " "
	}
	return spaces, nil
}

func (n *callPlanExpression) EvaluateLen(currentRow []interface{}) (interface{}, error) {
	argEval, err := n.args[0].Evaluate(currentRow)
	if err != nil {
		return nil, err
	}
	if argEval == nil {
		return nil, nil
	}
	stringArg, ok := argEval.(string)
	if !ok {
		return nil, sql3.NewErrUnexpectedTypeConversion(0, 0, argEval)
	}

	return int64(len([]rune(stringArg))), nil
}
func (n *callPlanExpression) EvaluateReplicate(currentRow []interface{}) (interface{}, error) {
	argEval, err := n.args[0].Evaluate(currentRow)
	if err != nil {
		return nil, err
	}
	if argEval == nil {
		return nil, nil
	}
	stringArg, ok := argEval.(string)
	if !ok {
		return nil, sql3.NewErrUnexpectedTypeConversion(0, 0, argEval)
	}

	argEval, err = n.args[1].Evaluate(currentRow)
	if err != nil {
		return nil, err
	}
	if argEval == nil {
		return nil, nil
	}
	intArg, ok := argEval.(int64)
	if !ok {
		return nil, sql3.NewErrUnexpectedTypeConversion(0, 0, argEval)
	}

	if intArg < 0 {
		return nil, sql3.NewErrValueOutOfRange(0, 0, intArg)
	}

	return strings.Repeat(stringArg, int(intArg)), nil

}

// EvaluateFormat function formats according to a format specifier and returns resulting string.
func (n *callPlanExpression) EvaluateFormat(currentRow []interface{}) (interface{}, error) {
	// first arg must be a string
	argEval, err := n.args[0].Evaluate(currentRow)
	if err != nil {
		return nil, err
	}
	if argEval == nil {
		return nil, nil
	}
	formatString, ok := argEval.(string)
	if !ok {
		return nil, sql3.NewErrUnexpectedTypeConversion(0, 0, argEval)
	}

	var args []interface{}

	// loop, since args can be of any length.
	for _, arg := range n.args[1:] {
		argEval, err = arg.Evaluate(currentRow)
		if err != nil {
			return nil, err
		}
		if argEval == nil {
			// this should never happen.
			return nil, sql3.NewErrLiteralNullNotAllowed(0, 0)
		}
		args = append(args, argEval)
	}
	return fmt.Sprintf(formatString, args...), nil
}

// EvaluateCharIndex function gets the position of the substring in the given string
func (n *callPlanExpression) EvaluateCharIndex(currentRow []interface{}) (interface{}, error) {

	argEval, err := n.args[0].Evaluate(currentRow)
	if err != nil {
		return nil, err
	}
	if argEval == nil {
		return nil, nil
	}
	subString, ok := argEval.(string)
	if !ok {
		return nil, sql3.NewErrUnexpectedTypeConversion(0, 0, argEval)
	}

	argEval, err = n.args[1].Evaluate(currentRow)
	if err != nil {
		return nil, err
	}
	if argEval == nil {
		return nil, nil
	}
	inputString, ok := argEval.(string)
	if !ok {
		return nil, sql3.NewErrUnexpectedTypeConversion(0, 0, argEval)
	}

	if len(n.args) == 2 {
		res := strings.Index(inputString, subString)
		if res > -1 {
			newpos := int64(res)
			return int64(newpos), nil
		}
		return int64(res), nil
	}

	argEval, err = n.args[2].Evaluate(currentRow)
	if err != nil {
		return nil, err
	}
	if argEval == nil {
		return nil, nil
	}
	pos, ok := argEval.(int64)
	if !ok {
		return nil, sql3.NewErrUnexpectedTypeConversion(0, 0, argEval)
	}

	if pos < 0 {
		return nil, sql3.NewErrValueOutOfRange(0, 0, pos)
	}
	if pos >= int64(len(inputString)) {
		return nil, sql3.NewErrValueOutOfRange(0, 0, pos)
	}
	res := strings.Index(inputString[pos:], subString)
	if res > -1 {
		newpos := int64(res) + pos
		return int64(newpos), nil
	}
	return int64(res), nil
}

func (p *ExecutionPlanner) analyseFunctionStr(call *parser.Call, scope parser.Statement) (parser.Expr, error) {
	if len(call.Args) < 1 || len(call.Args) > 3 {
		return nil, sql3.NewErrCallParameterCountMismatch(call.Rparen.Line, call.Rparen.Column, call.Name.Name, 1, len(call.Args))
	}
	targetType := parser.NewDataTypeDecimal(4)
	if !typesAreAssignmentCompatible(targetType, call.Args[0].DataType()) {
		return nil, sql3.NewErrTypeAssignmentIncompatible(call.Args[0].Pos().Line, call.Args[0].Pos().Column, call.Args[0].DataType().TypeDescription(), targetType.TypeDescription())
	}

	// the second and third parameters are optional
	for i := 1; i < len(call.Args); i++ {
		if !typeIsInteger(call.Args[i].DataType()) {
			return nil, sql3.NewErrIntExpressionExpected(call.Args[i].Pos().Line, call.Args[i].Pos().Column)
		}
	}

	call.ResultDataType = parser.NewDataTypeString()

	return call, nil
}

func (n *callPlanExpression) EvaluateStr(currentRow []interface{}) (interface{}, error) {
	argOneEval, err := n.args[0].Evaluate(currentRow)
	if err != nil {
		return "", err
	}
	if argOneEval == nil {
		return nil, nil
	}

	coercedValue, err := coerceValue(n.args[0].Type(), parser.NewDataTypeDecimal(4), argOneEval, parser.Pos{})

	decimalArgOne, ok := coercedValue.(pql.Decimal)
	if !ok {
		return nil, sql3.NewErrInternalf("unexpected type conversion '%T'", coercedValue)
	}

	intArgTwo := int64(10)
	if len(n.args) > 1 {
		argEval, err := n.args[1].Evaluate(currentRow)
		if err != nil {
			return nil, err
		}
		intArgTwo, ok = argEval.(int64)
		if !ok {
			return nil, sql3.NewErrInternalf("unexpected type converion %T", argEval)
		}
	}

	intArgThree := int64(0)
	if len(n.args) > 2 {
		argEval, err := n.args[2].Evaluate(currentRow)
		if err != nil {
			return nil, err
		}
		intArgThree, ok = argEval.(int64)
		if !ok {
			return nil, sql3.NewErrInternalf("unexpected type converion %T", argEval)
		}
	}

	floatValue := decimalArgOne.Float64()
	strFormat := fmt.Sprintf("%%%d.%df", intArgTwo, intArgThree)
	decimalString := fmt.Sprintf(strFormat, floatValue)

	// check if value can be displayed within allocated length
	if int64(len(decimalString)) > intArgTwo {
		decimalString = ""
		for i := int64(0); i < intArgTwo; i++ {
			decimalString += "*"
		}
	}

	return decimalString, nil
}
