package planner

import (
	"strings"
	"time"

	featurebase "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/sql3"
	"github.com/featurebasedb/featurebase/v3/sql3/parser"
)

const intervalYear = "YY"
const intervalYearDay = "YD"
const intervalMonth = "M"
const intervalDay = "D"
const intervalWeeKDay = "W"
const intervalWeek = "WK"
const intervalHour = "HH"
const intervalMinute = "MI"
const intervalSecond = "S"
const intervalMillisecond = "MS"
const intervalNanosecond = "NS"

func (p *ExecutionPlanner) analyzeFunctionDatePart(call *parser.Call, scope parser.Statement) (parser.Expr, error) {

	if len(call.Args) != 2 {
		return nil, sql3.NewErrCallParameterCountMismatch(call.Rparen.Line, call.Rparen.Column, call.Name.Name, 2, len(call.Args))
	}
	// interval
	intervalType := parser.NewDataTypeString()
	if !typesAreAssignmentCompatible(intervalType, call.Args[0].DataType()) {
		return nil, sql3.NewErrParameterTypeMistmatch(call.Args[0].Pos().Line, call.Args[0].Pos().Column, call.Args[0].DataType().TypeDescription(), intervalType.TypeDescription())
	}

	// date
	dateType := parser.NewDataTypeTimestamp()
	if !typesAreAssignmentCompatible(dateType, call.Args[1].DataType()) {
		return nil, sql3.NewErrParameterTypeMistmatch(call.Args[1].Pos().Line, call.Args[1].Pos().Column, call.Args[1].DataType().TypeDescription(), dateType.TypeDescription())
	}

	//return int
	call.ResultDataType = parser.NewDataTypeInt()

	return call, nil
}

func (p *ExecutionPlanner) analyzeFunctionToTimestamp(call *parser.Call, scope parser.Statement) (parser.Expr, error) {
	//param1 is the number to be converted to timestamp. This param is required.
	//param2 is the time unit of the numeric value in param 1. This param is optional.
	//ToTimestamp can be invoked with just param1.
	if len(call.Args) != 1 && len(call.Args) != 2 {
		return nil, sql3.NewErrCallParameterCountMismatch(call.Rparen.Line, call.Rparen.Column, call.Name.Name, 2, len(call.Args))
	}

	//param1 is a integer of type int64
	param1Type := parser.NewDataTypeInt()
	if !typesAreAssignmentCompatible(param1Type, call.Args[0].DataType()) {
		return nil, sql3.NewErrParameterTypeMistmatch(call.Args[0].Pos().Line, call.Args[0].Pos().Column, call.Args[0].DataType().TypeDescription(), param1Type.TypeDescription())
	}

	//param2 is a string and it should be one of 's', 'ms', 'us', 'ns'.
	//param2 is optional, will be defaulted to 's' if not supplied.
	if len(call.Args) == 2 {
		param2Type := parser.NewDataTypeString()
		if !typesAreAssignmentCompatible(param2Type, call.Args[1].DataType()) {
			return nil, sql3.NewErrParameterTypeMistmatch(call.Args[1].Pos().Line, call.Args[1].Pos().Column, call.Args[1].DataType().TypeDescription(), param2Type.TypeDescription())
		}
	}
	//ToTimestamp returns a timestamp calculated from param1 using time unit passed in param 2
	call.ResultDataType = parser.NewDataTypeTimestamp()
	return call, nil
}

func (n *callPlanExpression) EvaluateDatepart(currentRow []interface{}) (interface{}, error) {
	intervalEval, err := n.args[0].Evaluate(currentRow)
	if err != nil {
		return nil, err
	}

	dateEval, err := n.args[1].Evaluate(currentRow)
	if err != nil {
		return nil, err
	}

	// nil if anything is nil
	if intervalEval == nil || dateEval == nil {
		return nil, nil
	}

	//get the date value
	coercedDate, err := coerceValue(n.args[1].Type(), parser.NewDataTypeTimestamp(), dateEval, parser.Pos{Line: 0, Column: 0})
	if err != nil {
		return nil, err
	}

	date, dateOk := coercedDate.(time.Time)
	if !dateOk {
		return nil, sql3.NewErrInternalf("unable to convert value")
	}

	//get the interval value
	coercedInterval, err := coerceValue(n.args[0].Type(), parser.NewDataTypeString(), intervalEval, parser.Pos{Line: 0, Column: 0})
	if err != nil {
		return nil, err
	}

	interval, intervalOk := coercedInterval.(string)
	if !intervalOk {
		return nil, sql3.NewErrInternalf("unable to convert value")
	}

	switch strings.ToUpper(interval) {
	case intervalYear:
		return int64(date.Year()), nil

	case intervalYearDay:
		return int64(date.YearDay()), nil

	case intervalMonth:
		return int64(date.Month()), nil

	case intervalDay:
		return int64(date.Day()), nil

	case intervalWeeKDay:
		return int64(date.Weekday()), nil

	case intervalWeek:
		_, isoWeek := date.ISOWeek()
		return int64(isoWeek), nil

	case intervalHour:
		return int64(date.Hour()), nil

	case intervalMinute:
		return int64(date.Minute()), nil

	case intervalSecond:
		return int64(date.Second()), nil

	case intervalMillisecond:
		return int64(date.Nanosecond() * 1000 * 1000), nil

	case intervalNanosecond:
		return int64(date.Nanosecond()), nil

	default:
		return nil, sql3.NewErrCallParameterValueInvalid(0, 0, interval, "interval")
	}

}

func (n *callPlanExpression) EvaluateToTimestamp(currentRow []interface{}) (interface{}, error) {
	//retrieve param1, the number to be converted to timestamp
	param1, err := n.args[0].Evaluate(currentRow)
	if err != nil {
		return nil, err
	} else if param1 == nil {
		//if the param1 is null silently return null timestamp value
		return nil, nil
	}
	coercedParam1, err := coerceValue(n.args[0].Type(), parser.NewDataTypeInt(), param1, parser.Pos{Line: 0, Column: 0})
	if err != nil {
		//raise error if param 1 is not an integer. Should we return nil instead of raising error here? see note at return.
		return nil, err
	}
	num, ok := coercedParam1.(int64)
	if !ok {
		//raise error if param 1 is not an integer. Should we return nil instead of raising error here? see note at return.
		return nil, sql3.NewErrInternalf("unable to convert value")
	}

	//retrieve param2, time unit for param1, if not supplied default to seconds 's'.
	var unit string = featurebase.TimeUnitSeconds
	if len(n.args) == 2 {
		param2, err := n.args[1].Evaluate(currentRow)
		if err != nil {
			//raise error if unable to retieve the argument for param2
			return nil, err
		}
		coercedParam2, err := coerceValue(n.args[1].Type(), parser.NewDataTypeString(), param2, parser.Pos{Line: 0, Column: 0})
		if err != nil {
			//raise error is param2 is not a string
			return nil, err
		}
		unit, ok = coercedParam2.(string)
		if !ok {
			//raise error is param2 is not a string
			return nil, sql3.NewErrInternalf("unable to convert value")
		}
		if !featurebase.IsValidTimeUnit(unit) {
			//raise error is param2 is not a valid time unit
			return nil, sql3.NewErrCallParameterValueInvalid(0, 0, unit, "timeunit")
		}
	}
	//should we throw error or return nil if the conversion fails? what is the desired behaviour when ToTimestamp errors for one bad record in a batch of thousands?
	return featurebase.ValToTimestamp(unit, num)
}
