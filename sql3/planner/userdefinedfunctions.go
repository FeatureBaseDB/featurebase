package planner

import (
	"github.com/featurebasedb/featurebase/v3/sql3"
	"github.com/featurebasedb/featurebase/v3/sql3/parser"
)

func (p *ExecutionPlanner) analyzeUserDefinedFunction(call *parser.Call, scope parser.Statement, function *functionSystemObject) (parser.Expr, error) {
	// TODO(pok) removing user defined functions for now
	// // hard code to 1 string parameter
	// if len(call.Args) != 1 {
	// 	return nil, sql3.NewErrCallParameterCountMismatch(call.Rparen.Line, call.Rparen.Column, call.Name.Name, 2, len(call.Args))
	// }

	// // arg 1
	// argType := parser.NewDataTypeString()
	// if !typesAreAssignmentCompatible(argType, call.Args[0].DataType()) {
	// 	return nil, sql3.NewErrParameterTypeMistmatch(call.Args[0].Pos().Line, call.Args[0].Pos().Column, call.Args[0].DataType().TypeDescription(), argType.TypeDescription())
	// }

	// //return string
	// call.ResultDataType = parser.NewDataTypeString()

	// return call, nil
	return nil, sql3.NewErrUnsupported(0, 0, false, "user defined functions")
}

func (n *callPlanExpression) evaluateUserDefinedFunction(currentRow []interface{}) (interface{}, error) {
	// TODO(pok) removing what effectively is a remote code exploit
	// we will come back to this to add sql udfs and external code later

	// argEval, err := n.args[0].Evaluate(currentRow)
	// if err != nil {
	// 	return nil, err
	// }
	// // nil if anything is nil
	// if argEval == nil {
	// 	return nil, nil
	// }

	// //get the value
	// coercedArg, err := coerceValue(n.args[0].Type(), parser.NewDataTypeString(), argEval, parser.Pos{Line: 0, Column: 0})
	// if err != nil {
	// 	return nil, err
	// }

	// arg, argOk := coercedArg.(string)
	// if !argOk {
	// 	return nil, sql3.NewErrInternalf("unable to convert value")
	// }

	// // save the body to a temp file
	// file, err := os.CreateTemp("", "py-body")
	// if err != nil {
	// 	return nil, err
	// }
	// defer os.Remove(file.Name())

	// file.Write([]byte(n.udfReference.body))

	// cmd := exec.Command("python3", file.Name(), arg)
	// stdout, err := cmd.Output()

	// if err != nil {
	// 	return nil, err
	// }

	// retVal := string(stdout)
	// return retVal, nil
	return nil, sql3.NewErrUnsupported(0, 0, false, "user defined functions")
}
