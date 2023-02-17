package planner

import (
	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/sql3"
	"github.com/featurebasedb/featurebase/v3/sql3/parser"
)

// TODO (pok) this needs to go somewhere
/*func (p *ExecutionPlanner) analyzeFunctionRecord(call *parser.Call, scope parser.Statement) (parser.Expr, error) {
	if len(call.Args) != 2 {
		return nil, sql3.NewErrCallParameterCountMismatch(call.Rparen.Line, call.Rparen.Column, call.Name.Name, 2, len(call.Args))
	}
	// timestamp
	timestampType := parser.NewDataTypeTimestamp()
	if !typesAreAssignmentCompatible(timestampType, call.Args[0].DataType()) {
		return nil, sql3.NewErrParameterTypeMistmatch(call.Args[0].Pos().Line, call.Args[0].Pos().Column, call.Args[0].DataType().TypeName(), timestampType.TypeName())
	}

	// set
	ok, _ := typeIsSet(call.Args[1].DataType())
	if !ok {
		return nil, sql3.NewErrSetExpressionExpected(call.Args[1].Pos().Line, call.Args[1].Pos().Column)
	}

	//return record
	call.ResultDataType = parser.NewDataTypeSubtable([]*parser.SubtableColumn{
		{
			Name:     "",
			DataType: timestampType,
		},
		{
			Name:     "",
			DataType: call.Args[1].DataType(),
		},
	})

	return call, nil
}
*/

func (p *ExecutionPlanner) analyzeFunctionSubtable(call *parser.Call, scope parser.Statement) (parser.Expr, error) {
	if len(call.Args) != 1 {
		return nil, sql3.NewErrCallParameterCountMismatch(call.Rparen.Line, call.Rparen.Column, call.Name.Name, 2, len(call.Args))
	}
	// set
	ok, _ := typeIsTimeQuantum(call.Args[0].DataType())
	if !ok {
		// TODO (pok) send back the right error
		return nil, sql3.NewErrSetExpressionExpected(call.Args[0].Pos().Line, call.Args[0].Pos().Column)
	}
	call.ResultDataType = parser.NewDataTypeSubtable([]*parser.SubtableColumn{
		{
			Name:     string(dax.PrimaryKeyFieldName),
			DataType: parser.NewDataTypeID(),
		},
		{
			Name:     "timestamp",
			DataType: parser.NewDataTypeTimestamp(),
		},
		{
			Name:     "value",
			DataType: call.Args[0].DataType(),
		},
	})
	return call, nil
}
