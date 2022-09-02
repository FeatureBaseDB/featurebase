package planner

import (
	"strings"
	"time"

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
		return nil, sql3.NewErrParameterTypeMistmatch(call.Args[0].Pos().Line, call.Args[0].Pos().Column, call.Args[0].DataType().TypeName(), intervalType.TypeName())
	}

	// date
	dateType := parser.NewDataTypeTimestamp()
	if !typesAreAssignmentCompatible(dateType, call.Args[1].DataType()) {
		return nil, sql3.NewErrParameterTypeMistmatch(call.Args[1].Pos().Line, call.Args[1].Pos().Column, call.Args[1].DataType().TypeName(), dateType.TypeName())
	}

	//return int
	call.ResultDataType = parser.NewDataTypeInt()

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
