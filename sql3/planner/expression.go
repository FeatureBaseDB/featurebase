// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/featurebasedb/featurebase/v3/pql"
	"github.com/featurebasedb/featurebase/v3/sql3"
	"github.com/featurebasedb/featurebase/v3/sql3/parser"
	"github.com/featurebasedb/featurebase/v3/sql3/planner/types"
)

// coerceValue coerces a value from a source type to a target type. If the types do not allow a conversion
// an error is produced
func coerceValue(sourceType parser.ExprDataType, targetType parser.ExprDataType, value interface{}, atPos parser.Pos) (interface{}, error) {
	switch sourceType.(type) {

	case *parser.DataTypeInt:
		switch t := targetType.(type) {
		case *parser.DataTypeInt:
			return value, nil

		case *parser.DataTypeID:
			val, ok := value.(int64)
			if !ok {
				return nil, sql3.NewErrInternalf("unexpected value type '%T'", value)
			}
			return val, nil

		case *parser.DataTypeDecimal:
			val, ok := value.(int64)
			if !ok {
				return nil, sql3.NewErrInternalf("unexpected value type '%T'", value)
			}
			return pql.NewDecimal(val*int64(math.Pow(10, float64(t.Scale))), t.Scale), nil
		}

	case *parser.DataTypeID:
		switch t := targetType.(type) {
		case *parser.DataTypeID:
			return value, nil

		case *parser.DataTypeInt:
			return value, nil

		case *parser.DataTypeDecimal:
			val, ok := value.(int64)
			if !ok {
				return nil, sql3.NewErrInternalf("unexpected value type '%T'", value)
			}
			return pql.NewDecimal(int64(val)*int64(math.Pow(10, float64(t.Scale))), t.Scale), nil
		}

	case *parser.DataTypeDecimal:
		switch targetType.(type) {
		case *parser.DataTypeDecimal:
			return value, nil
		}

	case *parser.DataTypeString:
		switch targetType.(type) {
		case *parser.DataTypeString:
			return value, nil
		case *parser.DataTypeTimestamp:
			//try to coerce to a date
			val, ok := value.(string)
			if !ok {
				return nil, sql3.NewErrInternalf("unexpected value type '%T'", value)
			}
			if tm, err := time.ParseInLocation(time.RFC3339Nano, val, time.UTC); err == nil {
				return tm, nil
			} else if tm, err := time.ParseInLocation(time.RFC3339, val, time.UTC); err == nil {
				return tm, nil
			} else if tm, err := time.ParseInLocation("2006-01-02", val, time.UTC); err == nil {
				return tm, nil
			} else {
				return nil, sql3.NewErrInvalidTypeCoercion(0, 0, val, targetType.TypeDescription())
			}
		}

	case *parser.DataTypeTimestamp:
		switch targetType.(type) {
		case *parser.DataTypeTimestamp:
			return value, nil
		}

	case *parser.DataTypeIDSet:
		switch targetType.(type) {
		case *parser.DataTypeIDSet:
			return value, nil
		case *parser.DataTypeIDSetQuantum:
			return []interface{}{
				nil, //no timestamp
				value,
			}, nil
		}

	case *parser.DataTypeStringSet:
		switch targetType.(type) {
		case *parser.DataTypeStringSet:
			return value, nil
		case *parser.DataTypeStringSetQuantum:
			return []interface{}{
				nil, //no timestamp
				value,
			}, nil
		}

	case *parser.DataTypeTuple:
		switch targetType.(type) {
		case *parser.DataTypeIDSetQuantum:
			return value, nil

		case *parser.DataTypeStringSetQuantum:
			return value, nil
		}

	default:
		return nil, sql3.NewErrInternalf("unhandled source type '%T'", sourceType)
	}
	return nil, sql3.NewErrTypeMismatch(atPos.Line, atPos.Column, targetType.TypeDescription(), sourceType.TypeDescription())
}

// unaryOpPlanExpression is a unary op
type unaryOpPlanExpression struct {
	op  parser.Token
	rhs types.PlanExpression

	resultDataType parser.ExprDataType
}

func newUnaryOpPlanExpression(op parser.Token, rhs types.PlanExpression, dataType parser.ExprDataType) *unaryOpPlanExpression {
	return &unaryOpPlanExpression{
		op:             op,
		rhs:            rhs,
		resultDataType: dataType,
	}
}

func (n *unaryOpPlanExpression) Evaluate(currentRow []interface{}) (interface{}, error) {
	evalRhs, err := n.rhs.Evaluate(currentRow)
	if err != nil {
		return nil, err
	}
	switch n.op {
	case parser.BITNOT:
		return n.bitNotWithTypeCheck(evalRhs)
	case parser.PLUS:
		return n.plusWithTypeCheck(evalRhs)
	case parser.MINUS:
		return n.minusWithTypeCheck(evalRhs)
	default:
		return nil, sql3.NewErrInternalf("unhandled operator %d", n.op)
	}
}

func (n *unaryOpPlanExpression) Type() parser.ExprDataType {
	return n.resultDataType
}

func (n *unaryOpPlanExpression) String() string {
	return fmt.Sprintf("%s%s", n.op.String(), n.rhs.String())
}

func (n *unaryOpPlanExpression) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_expr"] = fmt.Sprintf("%T", n)
	result["dataType"] = n.Type().TypeDescription()
	result["op"] = n.op
	result["rhs"] = n.rhs.Plan()
	return result
}

func (n *unaryOpPlanExpression) Children() []types.PlanExpression {
	return []types.PlanExpression{
		n.rhs,
	}
}

func (n *unaryOpPlanExpression) WithChildren(children ...types.PlanExpression) (types.PlanExpression, error) {
	if len(children) != 1 {
		return nil, sql3.NewErrInternalf("unexpected number of children '%d'", len(children))
	}
	return newUnaryOpPlanExpression(n.op, children[0], n.resultDataType), nil
}

func (n *unaryOpPlanExpression) bitNotWithTypeCheck(rhs interface{}) (interface{}, error) {
	switch n.resultDataType.(type) {
	case *parser.DataTypeID:
		nr, nrok := rhs.(int64)
		if nrok {
			return ^nr, nil
		}
		return nil, sql3.NewErrInternalf("unexpected incompatible types '%T", rhs)

	case *parser.DataTypeInt:
		nr, nrok := rhs.(int64)
		if nrok {
			return ^nr, nil
		}
		return nil, sql3.NewErrInternalf("unexpected incompatible types '%T", rhs)

	default:
		return nil, sql3.NewErrInternalf("unexpected type '%T", n.resultDataType)
	}
}

func (n *unaryOpPlanExpression) plusWithTypeCheck(rhs interface{}) (interface{}, error) {
	switch n.resultDataType.(type) {
	case *parser.DataTypeID:
		nr, nrok := rhs.(int64)
		if nrok {
			return +nr, nil
		}
		return nil, sql3.NewErrInternalf("unexpected incompatible types '%T", rhs)

	case *parser.DataTypeInt:
		coercedRhs, err := coerceValue(n.rhs.Type(), n.resultDataType, rhs, parser.Pos{Line: 0, Column: 0})
		if err != nil {
			return nil, err
		}

		nr, nrok := coercedRhs.(int64)
		if nrok {
			return +nr, nil
		}
		return nil, sql3.NewErrInternalf("unexpected incompatible types '%T", rhs)

	case *parser.DataTypeDecimal:
		nr, nrok := rhs.(pql.Decimal)
		if nrok {
			val := nr.Value()
			if !val.IsInt64() {
				return nil, sql3.NewErrInternalf("decimal value overflow: %v", rhs)
			}
			return pql.NewDecimal(+val.Int64(), nr.Scale), nil
		}
		return nil, sql3.NewErrInternalf("unexpected incompatible types '%T", rhs)

	default:
		return nil, sql3.NewErrInternalf("unexpected type '%T", n.resultDataType)
	}
}

func (n *unaryOpPlanExpression) minusWithTypeCheck(rhs interface{}) (interface{}, error) {
	switch n.resultDataType.(type) {
	case *parser.DataTypeID:
		nr, nrok := rhs.(int64)
		if nrok {
			return -nr, nil
		}
		return nil, sql3.NewErrInternalf("unexpected incompatible types '%T", rhs)

	case *parser.DataTypeInt:
		coercedRhs, err := coerceValue(n.rhs.Type(), n.resultDataType, rhs, parser.Pos{Line: 0, Column: 0})
		if err != nil {
			return nil, err
		}

		nr, nrok := coercedRhs.(int64)
		if nrok {
			return -nr, nil
		}
		return nil, sql3.NewErrInternalf("unexpected incompatible types '%T", rhs)

	case *parser.DataTypeDecimal:
		nr, nrok := rhs.(pql.Decimal)
		if nrok {
			val := nr.Value()
			if !val.IsInt64() {
				return nil, sql3.NewErrInternalf("decimal value overflow: %v", rhs)
			}
			return pql.NewDecimal(-val.Int64(), nr.Scale), nil

		}
		return nil, sql3.NewErrInternalf("unexpected incompatible types '%T", rhs)

	default:
		return nil, sql3.NewErrInternalf("unexpected type '%T", n.resultDataType)
	}
}

// binOpPlanExpression is a binary op
type binOpPlanExpression struct {
	lhs types.PlanExpression
	op  parser.Token
	rhs types.PlanExpression

	resultDataType parser.ExprDataType
}

func newBinOpPlanExpression(lhs types.PlanExpression, op parser.Token, rhs types.PlanExpression, dataType parser.ExprDataType) *binOpPlanExpression {
	return &binOpPlanExpression{
		lhs:            lhs,
		op:             op,
		rhs:            rhs,
		resultDataType: dataType,
	}
}

func (n *binOpPlanExpression) Evaluate(currentRow []interface{}) (interface{}, error) {
	evalLhs, err := n.lhs.Evaluate(currentRow)
	if err != nil {
		return nil, err
	}
	evalRhs, err := n.rhs.Evaluate(currentRow)
	if err != nil {
		return nil, err
	}

	if n.op == parser.IS || n.op == parser.ISNOT {
		isNull := evalLhs == nil
		if n.op == parser.ISNOT {
			isNull = !isNull
		}
		return isNull, nil
	}

	coercedDataType, err := typeCoerceType(n.lhs.Type(), n.rhs.Type(), parser.Pos{Line: 0, Column: 0})
	if err != nil {
		return nil, err
	}

	switch coercedDataType.(type) {
	case *parser.DataTypeBool:
		//if either side is nil, return nil
		if evalLhs == nil || evalRhs == nil {
			return nil, nil
		}
		nl, nlok := evalLhs.(bool)
		nr, nrok := evalRhs.(bool)
		if nlok && nrok {
			switch n.op {
			case parser.NE:
				return nl != nr, nil
			case parser.EQ:
				return nl == nr, nil

			default:
				return nil, sql3.NewErrInternalf("unhandled operator %d", n.op)
			}
		}
		return nil, sql3.NewErrInternalf("unexpected type conversion error '%t', '%t'", nlok, nrok)

	case *parser.DataTypeInt:
		//if either side is nil, return nil
		if evalLhs == nil || evalRhs == nil {
			return nil, nil
		}

		coercedLhs, err := coerceValue(n.lhs.Type(), coercedDataType, evalLhs, parser.Pos{Line: 0, Column: 0})
		if err != nil {
			return nil, err
		}

		coercedRhs, err := coerceValue(n.rhs.Type(), coercedDataType, evalRhs, parser.Pos{Line: 0, Column: 0})
		if err != nil {
			return nil, err
		}

		nl, nlok := coercedLhs.(int64)
		nr, nrok := coercedRhs.(int64)
		if nlok && nrok {
			switch n.op {
			case parser.NE:
				return nl != nr, nil
			case parser.EQ:
				return nl == nr, nil
			case parser.LE:
				return nl <= nr, nil
			case parser.GE:
				return nl >= nr, nil
			case parser.GT:
				return nl > nr, nil
			case parser.LT:
				return nl < nr, nil

			case parser.BITAND:
				return nl & nr, nil
			case parser.BITOR:
				return nl | nr, nil

			case parser.LSHIFT:
				return nl << nr, nil
			case parser.RSHIFT:
				return nl >> nr, nil

			case parser.PLUS:
				return nl + nr, nil
			case parser.MINUS:
				return nl - nr, nil
			case parser.STAR:
				return nl * nr, nil
			case parser.SLASH:
				return nl / nr, nil

			case parser.REM:
				return nl % nr, nil

			default:
				return nil, sql3.NewErrInternalf("unhandled operator %d", n.op)
			}
		}
		return nil, sql3.NewErrInternalf("unexpected type conversion error '%t', '%t'", nlok, nrok)

	case *parser.DataTypeID:
		//if either side is nil, return nil
		if evalLhs == nil || evalRhs == nil {
			return nil, nil
		}

		coercedLhs, err := coerceValue(n.lhs.Type(), coercedDataType, evalLhs, parser.Pos{Line: 0, Column: 0})
		if err != nil {
			return nil, err
		}

		coercedRhs, err := coerceValue(n.rhs.Type(), coercedDataType, evalRhs, parser.Pos{Line: 0, Column: 0})
		if err != nil {
			return nil, err
		}

		nl, nlok := coercedLhs.(int64)
		nr, nrok := coercedRhs.(int64)
		if nlok && nrok {
			switch n.op {
			case parser.NE:
				return nl != nr, nil
			case parser.EQ:
				return nl == nr, nil
			case parser.LE:
				return nl <= nr, nil
			case parser.GE:
				return nl >= nr, nil
			case parser.GT:
				return nl > nr, nil
			case parser.LT:
				return nl < nr, nil

			case parser.BITAND:
				return nl & nr, nil
			case parser.BITOR:
				return nl | nr, nil

			case parser.LSHIFT:
				return nl << nr, nil
			case parser.RSHIFT:
				return nl >> nr, nil

			case parser.PLUS:
				return nl + nr, nil
			case parser.MINUS:
				return nl - nr, nil
			case parser.STAR:
				return nl * nr, nil
			case parser.SLASH:
				return nl / nr, nil

			case parser.REM:
				return nl % nr, nil

			default:
				return nil, sql3.NewErrInternalf("unhandled operator %d", n.op)
			}
		}
		return nil, sql3.NewErrInternalf("unexpected type conversion error '%t', '%t'", nlok, nrok)

	case *parser.DataTypeDecimal:
		//if either side is nil, return nil
		if evalLhs == nil || evalRhs == nil {
			return nil, nil
		}

		coercedLhs, err := coerceValue(n.lhs.Type(), coercedDataType, evalLhs, parser.Pos{Line: 0, Column: 0})
		if err != nil {
			return nil, err
		}

		coercedRhs, err := coerceValue(n.rhs.Type(), coercedDataType, evalRhs, parser.Pos{Line: 0, Column: 0})
		if err != nil {
			return nil, err
		}

		nld, nlok := coercedLhs.(pql.Decimal)
		nrd, nrok := coercedRhs.(pql.Decimal)

		if nlok && nrok {
			switch n.op {
			case parser.NE:
				return !nld.EqualTo(nrd), nil
			case parser.EQ:
				return nld.EqualTo(nrd), nil
			case parser.LE:
				return nld.LessThanOrEqualTo(nrd), nil
			case parser.GE:
				return nld.GreaterThanOrEqualTo(nrd), nil
			case parser.GT:
				return nld.GreaterThan(nrd), nil
			case parser.LT:
				return nld.LessThan(nrd), nil

			case parser.PLUS:
				return pql.AddDecimal(nld, nrd), nil
			case parser.MINUS:
				return pql.SubtractDecimal(nld, nrd), nil
			case parser.STAR:
				return pql.MultiplyDecimal(nld, nrd), nil
			case parser.SLASH:
				return pql.DivideDecimal(nld, nrd), nil

			default:
				return nil, sql3.NewErrInternalf("unhandled operator %d", n.op)
			}
		}
		return nil, sql3.NewErrInternalf("unexpected type conversion error '%t', '%t'", nlok, nrok)

	case *parser.DataTypeTimestamp:
		//if either side is nil, return nil
		if evalLhs == nil || evalRhs == nil {
			return nil, nil
		}

		coercedLhs, err := coerceValue(n.lhs.Type(), coercedDataType, evalLhs, parser.Pos{Line: 0, Column: 0})
		if err != nil {
			return nil, err
		}

		coercedRhs, err := coerceValue(n.rhs.Type(), coercedDataType, evalRhs, parser.Pos{Line: 0, Column: 0})
		if err != nil {
			return nil, err
		}

		nl, nlok := coercedLhs.(time.Time)
		nr, nrok := coercedRhs.(time.Time)

		if nlok && nrok {
			switch n.op {
			case parser.NE:
				return nl != nr, nil
			case parser.EQ:
				return nl == nr, nil
			case parser.LE:
				return nl == nr || nl.Before(nr), nil
			case parser.GE:
				return nl == nr || nl.After(nr), nil
			case parser.GT:
				return nl.After(nr), nil
			case parser.LT:
				return nl.Before(nr), nil

			default:
				return nil, sql3.NewErrInternalf("unhandled operator %d", n.op)
			}
		}
		return nil, sql3.NewErrInternalf("unexpected type conversion error '%t', '%t'", nlok, nrok)

	case *parser.DataTypeIDSet:
		//if either side is nil, return nil
		if evalLhs == nil || evalRhs == nil {
			return nil, nil
		}

		nl, nlok := evalLhs.([]int64)
		nr, nrok := evalRhs.([]int64)

		if nlok && nrok {
			switch n.op {
			case parser.NE:
				return !intSetContainsAll(nl, nr), nil
			case parser.EQ:
				return intSetContainsAll(nl, nr), nil

			default:
				return nil, sql3.NewErrInternalf("unhandled operator %d", n.op)
			}
		}
		return nil, sql3.NewErrInternalf("unexpected type conversion error '%t', '%t'", nlok, nrok)

	case *parser.DataTypeString:
		//if either side is nil, return nil
		if evalLhs == nil || evalRhs == nil {
			return nil, nil
		}

		nl, nlok := evalLhs.(string)
		nr, nrok := evalRhs.(string)
		if nlok && nrok {
			switch n.op {

			case parser.NE:
				return nl != nr, nil

			case parser.EQ:
				return nl == nr, nil

			case parser.CONCAT:
				return nl + nr, nil

			case parser.LIKE:
				regexPattern := wildCardToRegexp(nr)

				matched, err := regexp.MatchString(regexPattern, nl)
				if err != nil {
					return nil, err
				}
				return matched, nil

			case parser.NOTLIKE:
				regexPattern := wildCardToRegexp(nr)
				matched, err := regexp.MatchString(regexPattern, nl)
				if err != nil {
					return nil, err
				}
				return !matched, nil

			default:
				return nil, sql3.NewErrInternalf("unhandled operator %d", n.op)
			}
		}
		return nil, sql3.NewErrInternalf("unexpected type conversion error '%t', '%t'", nlok, nrok)

	case *parser.DataTypeStringSet:
		//if either side is nil, return nil
		if evalLhs == nil || evalRhs == nil {
			return nil, nil
		}

		nl, nlok := evalLhs.([]string)
		nr, nrok := evalRhs.([]string)

		if nlok && nrok {
			switch n.op {
			case parser.NE:
				return !stringSetContainsAll(nl, nr), nil
			case parser.EQ:
				return stringSetContainsAll(nl, nr), nil

			default:
				return nil, sql3.NewErrInternalf("unhandled operator %d", n.op)
			}
		}
		return nil, sql3.NewErrInternalf("unexpected type conversion error '%t', '%t'", nlok, nrok)

	default:
		return nil, sql3.NewErrInternalf("unhandled type '%s'", coercedDataType.TypeDescription())
	}
}

func (n *binOpPlanExpression) Type() parser.ExprDataType {
	return n.resultDataType
}

func (n *binOpPlanExpression) String() string {
	return fmt.Sprintf("%s%s%s", n.lhs.String(), n.op.String(), n.rhs.String())
}

func (n *binOpPlanExpression) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_expr"] = fmt.Sprintf("%T", n)
	result["dataType"] = n.Type().TypeDescription()
	result["op"] = n.op
	result["lhs"] = n.lhs.Plan()
	result["rhs"] = n.rhs.Plan()
	return result
}

func (n *binOpPlanExpression) Children() []types.PlanExpression {
	return []types.PlanExpression{
		n.lhs,
		n.rhs,
	}
}

func (n *binOpPlanExpression) WithChildren(children ...types.PlanExpression) (types.PlanExpression, error) {
	if len(children) != 2 {
		return nil, sql3.NewErrInternalf("unexpected number of children '%d'", len(children))
	}
	return newBinOpPlanExpression(children[0], n.op, children[1], n.resultDataType), nil
}

// rangePlanExpression is a range expression
type rangePlanExpression struct {
	lhs types.PlanExpression
	rhs types.PlanExpression

	resultDataType parser.ExprDataType
}

func newRangeOpPlanExpression(lhs types.PlanExpression, rhs types.PlanExpression, dataType parser.ExprDataType) *rangePlanExpression {
	return &rangePlanExpression{
		lhs:            lhs,
		rhs:            rhs,
		resultDataType: dataType,
	}
}

func (n *rangePlanExpression) Evaluate(currentRow []interface{}) (interface{}, error) {
	evalLhs, err := n.lhs.Evaluate(currentRow)
	if err != nil {
		return nil, err
	}
	evalRhs, err := n.rhs.Evaluate(currentRow)
	if err != nil {
		return nil, err
	}

	if evalLhs == nil || evalRhs == nil {
		return nil, nil
	}

	/*nl*/
	_, nlok := evalLhs.(int64)
	/*nr*/ _, nrok := evalRhs.(int64)
	if nlok && nrok {
		return true, nil
	}
	return nil, sql3.NewErrInternalf("unexpected type conversion error '%t', '%t'", nlok, nrok)
}

func (n *rangePlanExpression) Type() parser.ExprDataType {
	return n.resultDataType
}

func (n *rangePlanExpression) String() string {
	return fmt.Sprintf("between %s and %s", n.lhs.String(), n.rhs.String())
}

func (n *rangePlanExpression) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_expr"] = fmt.Sprintf("%T", n)
	result["dataType"] = n.Type().TypeDescription()
	result["lhs"] = n.lhs.Plan()
	result["rhs"] = n.rhs.Plan()
	return result
}

func (n *rangePlanExpression) Children() []types.PlanExpression {
	return []types.PlanExpression{
		n.lhs,
		n.rhs,
	}
}

func (n *rangePlanExpression) WithChildren(children ...types.PlanExpression) (types.PlanExpression, error) {
	if len(children) != 1 {
		return nil, sql3.NewErrInternalf("unexpected number of children '%d'", len(children))
	}
	return newRangeOpPlanExpression(children[0], children[1], n.resultDataType), nil
}

// casePlanExpression is a case expr
type casePlanExpression struct {
	baseExpr types.PlanExpression
	blocks   []types.PlanExpression
	elseExpr types.PlanExpression

	resultDataType parser.ExprDataType
}

func newCasePlanExpression(baseExpr types.PlanExpression, blocks []types.PlanExpression, elseExpr types.PlanExpression, dataType parser.ExprDataType) *casePlanExpression {
	return &casePlanExpression{
		baseExpr:       baseExpr,
		blocks:         blocks,
		elseExpr:       elseExpr,
		resultDataType: dataType,
	}
}

func (n *casePlanExpression) Evaluate(currentRow []interface{}) (interface{}, error) {
	if n.baseExpr != nil {
		evalBase, err := n.baseExpr.Evaluate(currentRow)
		if err != nil {
			return nil, err
		}
		if evalBase == nil {
			return nil, nil
		}
		for _, block := range n.blocks {
			caseBlock, ok := block.(*caseBlockPlanExpression)
			if !ok {
				return nil, sql3.NewErrInternalf("unexpected block type '%T'", block)
			}

			evalBlock, err := caseBlock.condition.Evaluate(currentRow)
			if err != nil {
				return nil, err
			}
			switch n.baseExpr.Type().(type) {
			case *parser.DataTypeInt:
				nl, nlok := evalBase.(int64)
				nr, nrok := evalBlock.(int64)
				if nlok && nrok {
					if nl == nr {
						evalBlockBody, err := caseBlock.body.Evaluate(currentRow)
						if err != nil {
							return nil, err
						}
						if evalBlockBody == nil {
							return nil, nil
						}
						switch caseBlock.body.Type().(type) {
						case *parser.DataTypeInt:
							b, bok := evalBlockBody.(int64)
							if bok {
								return b, nil
							}
							return nil, sql3.NewErrInternalf("unexpected type conversion error '%t'", bok)
						default:
							return nil, sql3.NewErrInternalf("unhandled type '%s'", n.baseExpr.Type())
						}
					}
				} else {
					return nil, sql3.NewErrInternalf("unexpected type conversion error '%t', '%t'", nlok, nrok)
				}
			default:
				return nil, sql3.NewErrInternalf("unhandled type '%s'", n.baseExpr.Type())
			}
		}
		//if we get to here, we're falling back to else
		if n.elseExpr != nil {
			evalElse, err := n.elseExpr.Evaluate(currentRow)
			if err != nil {
				return nil, err
			}
			if evalElse == nil {
				return nil, nil
			}
			switch n.elseExpr.Type().(type) {
			case *parser.DataTypeInt:
				el, elok := evalElse.(int64)
				if elok {
					return el, nil
				}
				return nil, sql3.NewErrInternalf("unexpected type conversion error '%t'", elok)
			default:
				return nil, sql3.NewErrInternalf("unhandled type '%s'", n.elseExpr.Type())

			}
		}
		return nil, nil
	} else {
		for _, block := range n.blocks {
			caseBlock, ok := block.(*caseBlockPlanExpression)
			if !ok {
				return nil, sql3.NewErrInternalf("unexpected block type '%T'", block)
			}

			evalBlock, err := caseBlock.condition.Evaluate(currentRow)
			if err != nil {
				return nil, err
			}
			bl, blok := evalBlock.(bool)
			if !blok {
				return nil, sql3.NewErrInternalf("unexpected type conversion error '%t'", blok)
			}
			if bl {
				evalBlockBody, err := caseBlock.body.Evaluate(currentRow)
				if err != nil {
					return nil, err
				}
				if evalBlockBody == nil {
					return nil, nil
				}
				switch caseBlock.body.Type().(type) {
				case *parser.DataTypeInt:
					b, bok := evalBlockBody.(int64)
					if bok {
						return b, nil
					}
					return nil, sql3.NewErrInternalf("unexpected type conversion error '%t'", bok)

				case *parser.DataTypeBool:
					b, bok := evalBlockBody.(bool)
					if bok {
						return b, nil
					}
					return nil, sql3.NewErrInternalf("unexpected type conversion error '%t'", bok)

				case *parser.DataTypeString:
					s, sok := evalBlockBody.(string)
					if sok {
						return s, nil
					}
					return nil, sql3.NewErrInternalf("unexpected type conversion error '%t'", sok)
				default:
					return nil, sql3.NewErrInternalf("unhandled type '%T'", caseBlock.body.Type())
				}
			}
		}
		//if we get to here, we're falling back to else
		if n.elseExpr != nil {
			evalElse, err := n.elseExpr.Evaluate(currentRow)
			if err != nil {
				return nil, err
			}
			if evalElse == nil {
				return nil, nil
			}
			switch n.elseExpr.Type().(type) {
			case *parser.DataTypeInt:
				el, elok := evalElse.(int64)
				if elok {
					return el, nil
				}
				return nil, sql3.NewErrInternalf("unexpected type conversion error '%t'", elok)
			case *parser.DataTypeString:
				s, sok := evalElse.(string)
				if sok {
					return s, nil
				}
				return nil, sql3.NewErrInternalf("unexpected type conversion error '%t'", sok)
			default:
				return nil, sql3.NewErrInternalf("unhandled type '%T'", n.elseExpr.Type())

			}
		}
		return nil, nil
	}
}

func (n *casePlanExpression) Type() parser.ExprDataType {
	return n.resultDataType
}

func (n *casePlanExpression) String() string {
	var result string
	if n.baseExpr != nil {
		result = fmt.Sprintf("case %s", n.baseExpr)
	} else {
		result = "case"
	}
	for _, blk := range n.blocks {
		result += fmt.Sprintf(" %s", blk.String())
	}
	if n.elseExpr != nil {
		result += fmt.Sprintf(" else %s end", n.elseExpr)
	} else {
		result += " end"
	}
	return result
}

func (n *casePlanExpression) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_expr"] = fmt.Sprintf("%T", n)
	result["dataType"] = n.Type().TypeDescription()
	if n.baseExpr != nil {
		result["baseExpr"] = n.baseExpr.Plan()
	}
	if n.elseExpr != nil {
		result["elseExpr"] = n.elseExpr.Plan()
	}
	ps := make([]interface{}, 0)
	for _, e := range n.blocks {
		ps = append(ps, e.Plan())
	}
	result["blocks"] = ps
	return result
}

func (n *casePlanExpression) Children() []types.PlanExpression {
	result := make([]types.PlanExpression, 0)
	if n.baseExpr != nil {
		result = append(result, n.baseExpr)
	}
	result = append(result, n.blocks...)
	if n.elseExpr != nil {
		result = append(result, n.elseExpr)
	}
	return result
}

func (n *casePlanExpression) WithChildren(children ...types.PlanExpression) (types.PlanExpression, error) {
	currentLen := 0
	if n.baseExpr != nil {
		currentLen += 1
	}
	currentLen += len(n.blocks)
	if n.elseExpr != nil {
		currentLen += 1
	}
	if len(children) != currentLen {
		return nil, sql3.NewErrInternalf("unexpected number of children '%d'", len(children))
	}

	offset := 0
	var newBaseExpr types.PlanExpression
	if n.baseExpr != nil {
		newBaseExpr = children[offset]
		offset += 1
	}
	newBlocks := make([]types.PlanExpression, len(n.blocks))
	copy(newBlocks[0:], children[offset:offset+len(n.blocks)])
	offset += len(n.blocks)

	var newElseExpr types.PlanExpression
	if n.elseExpr != nil {
		newElseExpr = children[offset]
	}
	return newCasePlanExpression(newBaseExpr, newBlocks, newElseExpr, n.resultDataType), nil
}

// caseBlockPlanExpression is for case blocks
type caseBlockPlanExpression struct {
	condition types.PlanExpression
	body      types.PlanExpression
}

func newCaseBlockPlanExpression(condition types.PlanExpression, body types.PlanExpression) *caseBlockPlanExpression {
	return &caseBlockPlanExpression{
		condition: condition,
		body:      body,
	}
}

func (n *caseBlockPlanExpression) Evaluate(currentRow []interface{}) (interface{}, error) {
	return nil, nil
}

func (n *caseBlockPlanExpression) Type() parser.ExprDataType {
	return parser.NewDataTypeBool()
}

func (n *caseBlockPlanExpression) String() string {
	return fmt.Sprintf("when %s then %s end", n.condition.String(), n.body.String())
}

func (n *caseBlockPlanExpression) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_expr"] = fmt.Sprintf("%T", n)
	result["dataType"] = n.Type().TypeDescription()
	result["condition"] = n.condition.Plan()
	result["body"] = n.body.Plan()
	return result
}

func (n *caseBlockPlanExpression) Children() []types.PlanExpression {
	return []types.PlanExpression{
		n.condition,
		n.body,
	}
}

func (n *caseBlockPlanExpression) WithChildren(children ...types.PlanExpression) (types.PlanExpression, error) {
	if len(children) != 2 {
		return nil, sql3.NewErrInternalf("unexpected number of children '%d'", len(children))
	}
	return newCaseBlockPlanExpression(children[0], children[1]), nil
}

// subqueryPlanExpression is a select statement (when used in an expression)
type subqueryPlanExpression struct {
	op types.PlanOperator
}

func newSubqueryPlanExpression(op types.PlanOperator) *subqueryPlanExpression {
	return &subqueryPlanExpression{
		op: op,
	}
}

func (n *subqueryPlanExpression) Evaluate(currentRow []interface{}) (interface{}, error) {
	//get an iterator
	iter, err := n.op.Iterator(context.Background(), currentRow)
	if err != nil {
		return nil, err
	}

	//get the first row
	row, err := iter.Next(context.Background())
	if err != nil {
		if err == types.ErrNoMoreRows {
			//no rows, so return null
			//TODO(pok) - check that this is the right behavior
			return nil, nil
		}
		return nil, err
	}
	result := row[0]

	//make sure we don't have a next row - this is an error
	_, err = iter.Next(context.Background())
	if err != nil && err == types.ErrNoMoreRows {
		return result, nil
	}
	return nil, sql3.NewErrSingleRowExpected(0, 0)
}

func (n *subqueryPlanExpression) Type() parser.ExprDataType {
	return parser.NewDataTypeBool()
}

func (n *subqueryPlanExpression) String() string {
	return n.op.String()
}

func (n *subqueryPlanExpression) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_expr"] = fmt.Sprintf("%T", n)
	result["dataType"] = n.Type().TypeDescription()
	result["subquery"] = n.op.Plan()
	return result
}

func (n *subqueryPlanExpression) Children() []types.PlanExpression {
	return []types.PlanExpression{}
}

func (n *subqueryPlanExpression) WithChildren(children ...types.PlanExpression) (types.PlanExpression, error) {
	return n, nil
}

// betweenOpPlanExpression is a 'between/not between' op
type betweenOpPlanExpression struct {
	lhs types.PlanExpression
	op  parser.Token
	rhs types.PlanExpression
}

func newBetweenOpPlanExpression(lhs types.PlanExpression, op parser.Token, rhs types.PlanExpression) *betweenOpPlanExpression {
	return &betweenOpPlanExpression{
		lhs: lhs,
		op:  op,
		rhs: rhs,
	}
}

func (n *betweenOpPlanExpression) Evaluate(currentRow []interface{}) (interface{}, error) {
	evalLhs, err := n.lhs.Evaluate(currentRow)
	if err != nil {
		return nil, err
	}

	exprRange, ok := n.rhs.(*rangePlanExpression)
	if !ok {
		return nil, sql3.NewErrInternal("range expression expected")
	}

	rangeLower, err := exprRange.lhs.Evaluate(currentRow)
	if err != nil {
		return nil, err
	}
	rangeUpper, err := exprRange.rhs.Evaluate(currentRow)
	if err != nil {
		return nil, err
	}

	if evalLhs == nil || rangeLower == nil || rangeUpper == nil {
		return nil, nil
	}

	switch rType := n.rhs.Type().(type) {
	case *parser.DataTypeRange:
		switch rType.SubscriptType.(type) {
		case *parser.DataTypeInt:

			nl, nlok := evalLhs.(int64)
			rl, rlok := rangeLower.(int64)
			ru, ruok := rangeUpper.(int64)

			if !(nlok && rlok && ruok) {
				return nil, sql3.NewErrInternalf("unexpected type conversion error '%t', '%t', '%t'", nlok, rlok, ruok)
			}
			result := nl >= rl && nl <= ru
			if n.op == parser.NOTBETWEEN {
				result = !result
			}
			return result, nil

		case *parser.DataTypeTimestamp:

			nl, nlok := evalLhs.(time.Time)
			rl, rlok := rangeLower.(time.Time)
			ru, ruok := rangeUpper.(time.Time)

			if !(nlok && rlok && ruok) {
				return nil, sql3.NewErrInternalf("unexpected type conversion error '%t', '%t', '%t'", nlok, rlok, ruok)
			}
			result := (nl == rl || nl.After(rl)) && (nl == ru || nl.Before(ru))
			if n.op == parser.NOTBETWEEN {
				result = !result
			}
			return result, nil

		default:
			return nil, sql3.NewErrInternalf("unexpected range type '%T'", rType.SubscriptType)
		}

	default:
		return nil, sql3.NewErrInternalf("unexpected range type '%T'", n.rhs.Type())
	}
}

func (n *betweenOpPlanExpression) Type() parser.ExprDataType {
	return parser.NewDataTypeBool()
}

func (n *betweenOpPlanExpression) String() string {
	if n.op == parser.BETWEEN {
		return fmt.Sprintf("between %s and %s", n.lhs.String(), n.rhs.String())
	}
	return fmt.Sprintf("not between %s and %s", n.lhs.String(), n.rhs.String())
}

func (n *betweenOpPlanExpression) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_expr"] = fmt.Sprintf("%T", n)
	result["dataType"] = n.Type().TypeDescription()
	result["lhs"] = n.lhs.Plan()
	result["rhs"] = n.rhs.Plan()
	return result
}

func (n *betweenOpPlanExpression) Children() []types.PlanExpression {
	return []types.PlanExpression{
		n.lhs,
		n.rhs,
	}
}

func (n *betweenOpPlanExpression) WithChildren(children ...types.PlanExpression) (types.PlanExpression, error) {
	if len(children) != 2 {
		return nil, sql3.NewErrInternalf("unexpected number of children '%d'", len(children))
	}
	return newBetweenOpPlanExpression(children[0], n.op, children[1]), nil
}

// inOpPlanExpression is an 'in/not in' op
type inOpPlanExpression struct {
	lhs types.PlanExpression
	op  parser.Token
	rhs types.PlanExpression
}

func newInOpPlanExpression(lhs types.PlanExpression, op parser.Token, rhs types.PlanExpression) *inOpPlanExpression {
	return &inOpPlanExpression{
		lhs: lhs,
		op:  op,
		rhs: rhs,
	}
}

func (n *inOpPlanExpression) Evaluate(currentRow []interface{}) (interface{}, error) {
	evalLhs, err := n.lhs.Evaluate(currentRow)
	if err != nil {
		return nil, err
	}

	//if lhs is nil, bail
	if evalLhs == nil {
		return nil, nil
	}

	exprList, ok := n.rhs.(*exprListPlanExpression)
	if !ok {
		return nil, sql3.NewErrInternal("expression list expected")
	}

	listMembers := []interface{}{}

	//evaluate all the list members
	for _, lm := range exprList.exprs {
		lv, err := lm.Evaluate(currentRow)
		if err != nil {
			return nil, err
		}
		//if any of the list members eval to nil, bail
		if lv == nil {
			return nil, nil
		}
		listMembers = append(listMembers, lv)
	}

	result := false

	switch n.lhs.Type().(type) {

	case *parser.DataTypeInt, *parser.DataTypeID:
		nl, nlok := evalLhs.(int64)
		if !nlok {
			return nil, sql3.NewErrInternalf("unable to convert lhs expression to type '%s'", n.lhs.Type().TypeDescription())
		}

		for _, lm := range listMembers {
			l, lok := lm.(int64)
			if !lok {
				return nil, sql3.NewErrInternalf("unable to convert list expression to type '%s'", n.lhs.Type().TypeDescription())
			}
			if nl == l {
				result = true
				break
			}
		}

	case *parser.DataTypeBool:
		nl, nlok := evalLhs.(bool)
		if !nlok {
			return nil, sql3.NewErrInternalf("unable to convert lhs expression to type '%s'", n.lhs.Type().TypeDescription())
		}

		for _, lm := range listMembers {
			l, lok := lm.(bool)
			if !lok {
				return nil, sql3.NewErrInternalf("unable to convert list expression to type '%s'", n.lhs.Type().TypeDescription())
			}
			if nl == l {
				result = true
				break
			}
		}

	case *parser.DataTypeDecimal:
		nl, nlok := evalLhs.(pql.Decimal)
		if !nlok {
			return nil, sql3.NewErrInternalf("unable to convert lhs expression to type '%s'", n.lhs.Type().TypeDescription())
		}

		for _, lm := range listMembers {
			l, lok := lm.(pql.Decimal)
			if !lok {
				return nil, sql3.NewErrInternalf("unable to convert list expression to type '%s'", n.lhs.Type().TypeDescription())
			}
			if nl.EqualTo(l) {
				result = true
				break
			}
		}

	case *parser.DataTypeIDSet:
		nl, nlok := evalLhs.([]int64)
		if !nlok {
			return nil, sql3.NewErrInternalf("unable to convert lhs expression to type '%s'", n.lhs.Type().TypeDescription())
		}

		for _, lm := range listMembers {
			l, lok := lm.([]int64)
			if !lok {
				return nil, sql3.NewErrInternalf("unable to convert list expression to type '%s'", n.lhs.Type().TypeDescription())
			}
			if intSetContainsAll(nl, l) {
				result = true
				break
			}
		}

	case *parser.DataTypeString:
		nl, nlok := evalLhs.(string)
		if !nlok {
			return nil, sql3.NewErrInternalf("unable to convert lhs expression to type '%s'", n.lhs.Type().TypeDescription())
		}

		for _, lm := range listMembers {
			l, lok := lm.(string)
			if !lok {
				return nil, sql3.NewErrInternalf("unable to convert list expression to type '%s'", n.lhs.Type().TypeDescription())
			}
			if nl == l {
				result = true
				break
			}
		}

	case *parser.DataTypeStringSet:
		nl, nlok := evalLhs.([]string)
		if !nlok {
			return nil, sql3.NewErrInternalf("unable to convert lhs expression to type '%s'", n.lhs.Type().TypeDescription())
		}

		for _, lm := range listMembers {
			l, lok := lm.([]string)
			if !lok {
				return nil, sql3.NewErrInternalf("unable to convert list expression to type '%s'", n.lhs.Type().TypeDescription())
			}
			if stringSetContainsAll(nl, l) {
				result = true
				break
			}
		}

	case *parser.DataTypeTimestamp:
		nl, nlok := evalLhs.(time.Time)
		if !nlok {
			return nil, sql3.NewErrInternalf("unable to convert lhs expression to type '%s'", n.lhs.Type().TypeDescription())
		}

		for _, lm := range listMembers {
			l, lok := lm.(time.Time)
			if !lok {
				return nil, sql3.NewErrInternalf("unable to convert list expression to type '%s'", n.lhs.Type().TypeDescription())
			}
			if nl == l {
				result = true
				break
			}
		}

	default:
		return nil, sql3.NewErrInternalf("unhandled type '%T'", n.lhs.Type())
	}

	if n.op == parser.NOTIN {
		return !result, nil
	} else {
		return result, nil
	}
}

func (n *inOpPlanExpression) Type() parser.ExprDataType {
	return parser.NewDataTypeBool()
}

func (n *inOpPlanExpression) String() string {
	s := n.lhs.String()
	if n.op == parser.NOTIN {
		s += " not "
	}
	s += " in ("
	s += n.rhs.String()
	s += ")"
	return s
}

func (n *inOpPlanExpression) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_expr"] = fmt.Sprintf("%T", n)
	result["dataType"] = n.Type().TypeDescription()
	result["lhs"] = n.lhs.Plan()
	result["rhs"] = n.rhs.Plan()
	return result

}

func (n *inOpPlanExpression) Children() []types.PlanExpression {
	return []types.PlanExpression{
		n.lhs,
		n.rhs,
	}
}

func (n *inOpPlanExpression) WithChildren(children ...types.PlanExpression) (types.PlanExpression, error) {
	if len(children) != 2 {
		return nil, sql3.NewErrInternalf("unexpected number of children '%d'", len(children))
	}
	return newInOpPlanExpression(children[0], n.op, children[1]), nil
}

// callPlanExpression is a function call
type callPlanExpression struct {
	name     string
	args     []types.PlanExpression
	dataType parser.ExprDataType
}

func newCallPlanExpression(name string, args []types.PlanExpression, dataType parser.ExprDataType) *callPlanExpression {
	return &callPlanExpression{
		name:     name,
		args:     args,
		dataType: dataType,
	}
}

func (n *callPlanExpression) Evaluate(currentRow []interface{}) (interface{}, error) {
	switch strings.ToUpper(n.name) {
	case "SETCONTAINS":
		return n.EvaluateSetContains(currentRow)
	case "SETCONTAINSANY":
		return n.EvaluateSetContainsAny(currentRow)
	case "SETCONTAINSALL":
		return n.EvaluateSetContainsAll(currentRow)
	case "DATEPART":
		return n.EvaluateDatepart(currentRow)
	default:
		return nil, sql3.NewErrInternalf("unhandled function name '%s'", n.name)
	}
}

func (n *callPlanExpression) Type() parser.ExprDataType {
	return n.dataType
}

func (n *callPlanExpression) String() string {
	args := ""
	for idx, arg := range n.args {
		if idx > 0 {
			args += ", "
		}
		args += arg.String()
	}
	return fmt.Sprintf("%s(%s)", n.name, args)
}

func (n *callPlanExpression) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_expr"] = fmt.Sprintf("%T", n)
	result["name"] = n.name
	result["dataType"] = n.Type().TypeDescription()
	ps := make([]interface{}, 0)
	for _, e := range n.args {
		ps = append(ps, e.Plan())
	}
	result["args"] = ps
	return result
}

func (n *callPlanExpression) Children() []types.PlanExpression {
	return n.args
}

func (n *callPlanExpression) WithChildren(children ...types.PlanExpression) (types.PlanExpression, error) {
	if len(children) != len(n.args) {
		return nil, sql3.NewErrInternalf("unexpected number of children '%d'", len(children))
	}
	return newCallPlanExpression(n.name, children, n.dataType), nil
}

// aliasPlanExpression is a alias ref
type aliasPlanExpression struct {
	types.IdentifiableByName
	aliasName string
	expr      types.PlanExpression
}

func newAliasPlanExpression(aliasName string, expr types.PlanExpression) *aliasPlanExpression {
	return &aliasPlanExpression{
		aliasName: aliasName,
		expr:      expr,
	}
}

func (n *aliasPlanExpression) Name() string {
	return n.aliasName
}

// evaluates expression based on current row and column
func (n *aliasPlanExpression) Evaluate(currentRow []interface{}) (interface{}, error) {
	return n.expr.Evaluate(currentRow)
}

// returns the type of the expression
func (n *aliasPlanExpression) Type() parser.ExprDataType {
	return n.expr.Type()
}

func (n *aliasPlanExpression) String() string {
	return fmt.Sprintf("%s as %s", n.expr.String(), n.aliasName)
}

func (n *aliasPlanExpression) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_expr"] = fmt.Sprintf("%T", n)
	result["dataType"] = n.Type().TypeDescription()
	result["aliasName"] = n.aliasName
	result["expr"] = n.expr.Plan()
	return result
}

func (n *aliasPlanExpression) Children() []types.PlanExpression {
	return []types.PlanExpression{
		n.expr,
	}
}

func (n *aliasPlanExpression) WithChildren(children ...types.PlanExpression) (types.PlanExpression, error) {
	if len(children) != 1 {
		return nil, sql3.NewErrInternalf("unexpected number of children '%d'", len(children))
	}
	return newAliasPlanExpression(n.aliasName, children[0]), nil
}

// qualifiedRefPlanExpression is a qualified ref
type qualifiedRefPlanExpression struct {
	types.IdentifiableByName
	tableName   string
	columnName  string
	columnIndex int
	dataType    parser.ExprDataType
}

func newQualifiedRefPlanExpression(tableName string, columnName string, columnIndex int, dataType parser.ExprDataType) *qualifiedRefPlanExpression {
	return &qualifiedRefPlanExpression{
		tableName:   tableName,
		columnName:  columnName,
		columnIndex: columnIndex,
		dataType:    dataType,
	}
}

func (n *qualifiedRefPlanExpression) Evaluate(currentRow []interface{}) (interface{}, error) {
	if n.columnIndex < 0 || n.columnIndex >= len(currentRow) {
		return nil, sql3.NewErrInternalf("unable to to find column '%d' in currentColumns", n.columnIndex)
	}

	if currentRow[n.columnIndex] == nil {
		return currentRow[n.columnIndex], nil
	}

	switch n.dataType.(type) {
	case *parser.DataTypeIDSet:
		row, ok := currentRow[n.columnIndex].([]uint64)
		if !ok {
			return nil, sql3.NewErrInternalf("unexpected type for current row '%T'", currentRow[n.columnIndex])
		}
		result := make([]int64, len(row))
		for i, v := range row {
			result[i] = int64(v)
		}
		return result, nil

	case *parser.DataTypeID:
		//this could be an int64 or a uint64 internally
		iv, iok := currentRow[n.columnIndex].(int64)
		if iok {
			return iv, nil
		}
		v, ok := currentRow[n.columnIndex].(uint64)
		if !ok {
			return nil, sql3.NewErrInternalf("unexpected type for current row '%T'", currentRow[n.columnIndex])
		}
		return int64(v), nil

	default:
		return currentRow[n.columnIndex], nil
	}
}

func (n *qualifiedRefPlanExpression) Name() string {
	return n.columnName
}

func (n *qualifiedRefPlanExpression) Type() parser.ExprDataType {
	return n.dataType
}

func (n *qualifiedRefPlanExpression) String() string {
	if len(n.tableName) > 0 {
		return fmt.Sprintf("%s.%s", n.tableName, n.columnName)
	}
	return n.columnName
}

func (n *qualifiedRefPlanExpression) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_expr"] = fmt.Sprintf("%T", n)
	result["tableName"] = n.tableName
	result["columnName"] = n.columnName
	result["columnIndex"] = n.columnIndex
	result["dataType"] = n.dataType.TypeDescription()
	return result
}

func (n *qualifiedRefPlanExpression) Children() []types.PlanExpression {
	return []types.PlanExpression{}
}

func (n *qualifiedRefPlanExpression) WithChildren(children ...types.PlanExpression) (types.PlanExpression, error) {
	return n, nil
}

// variableRefPlanExpression is a variable ref
type variableRefPlanExpression struct {
	types.IdentifiableByName
	name          string
	variableIndex int
	dataType      parser.ExprDataType
}

func newVariableRefPlanExpression(name string, variableIndex int, dataType parser.ExprDataType) *variableRefPlanExpression {
	return &variableRefPlanExpression{
		name:          name,
		variableIndex: variableIndex,
		dataType:      dataType,
	}
}

func (n *variableRefPlanExpression) Evaluate(currentRow []interface{}) (interface{}, error) {
	if n.variableIndex < 0 || n.variableIndex >= len(currentRow) {
		return nil, sql3.NewErrInternalf("unable to to find variable '%d'", n.variableIndex)
	}

	if currentRow[n.variableIndex] == nil {
		return currentRow[n.variableIndex], nil
	}

	switch n.dataType.(type) {

	default:
		return currentRow[n.variableIndex], nil
	}
}

func (n *variableRefPlanExpression) Name() string {
	return n.name
}

func (n *variableRefPlanExpression) Type() parser.ExprDataType {
	return n.dataType
}

func (n *variableRefPlanExpression) String() string {
	return fmt.Sprintf("@%s", n.name)
}

func (n *variableRefPlanExpression) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_expr"] = fmt.Sprintf("%T", n)
	result["name"] = n.name
	result["dataType"] = n.dataType.TypeDescription()
	return result
}

func (n *variableRefPlanExpression) Children() []types.PlanExpression {
	return []types.PlanExpression{}
}

func (n *variableRefPlanExpression) WithChildren(children ...types.PlanExpression) (types.PlanExpression, error) {
	return n, nil
}

// nullLiteralPlanExpression is a null literal
type nullLiteralPlanExpression struct{}

func newNullLiteralPlanExpression() *nullLiteralPlanExpression {
	return &nullLiteralPlanExpression{}
}

func (n *nullLiteralPlanExpression) Evaluate(currentRow []interface{}) (interface{}, error) {
	return nil, nil
}

func (n *nullLiteralPlanExpression) Type() parser.ExprDataType {
	return parser.NewDataTypeVoid()
}

func (n *nullLiteralPlanExpression) String() string {
	return "null"
}

func (n *nullLiteralPlanExpression) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_expr"] = fmt.Sprintf("%T", n)
	result["dataType"] = n.Type().TypeDescription()
	return result
}

func (n *nullLiteralPlanExpression) Children() []types.PlanExpression {
	return []types.PlanExpression{}
}

func (n *nullLiteralPlanExpression) WithChildren(children ...types.PlanExpression) (types.PlanExpression, error) {
	return n, nil
}

// intLiteralPlanExpression is an integer literal
type intLiteralPlanExpression struct {
	value int64
}

func newIntLiteralPlanExpression(value int64) *intLiteralPlanExpression {
	return &intLiteralPlanExpression{
		value: value,
	}
}

func (n *intLiteralPlanExpression) Evaluate(currentRow []interface{}) (interface{}, error) {
	return n.value, nil
}

func (n *intLiteralPlanExpression) Type() parser.ExprDataType {
	return parser.NewDataTypeInt()
}

func (n *intLiteralPlanExpression) String() string {
	return fmt.Sprintf("%d", n.value)
}

func (n *intLiteralPlanExpression) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_expr"] = fmt.Sprintf("%T", n)
	result["dataType"] = n.Type().TypeDescription()
	result["value"] = n.value
	return result
}

func (n *intLiteralPlanExpression) Children() []types.PlanExpression {
	return []types.PlanExpression{}
}

func (n *intLiteralPlanExpression) WithChildren(children ...types.PlanExpression) (types.PlanExpression, error) {
	return n, nil
}

// floatLiteralPlanExpression is a float literal
type floatLiteralPlanExpression struct {
	value string
}

func newFloatLiteralPlanExpression(value string) *floatLiteralPlanExpression {
	return &floatLiteralPlanExpression{
		value: value,
	}
}

func (n *floatLiteralPlanExpression) Evaluate(currentRow []interface{}) (interface{}, error) {
	return pql.ParseDecimal(n.value)
}

func (n *floatLiteralPlanExpression) Type() parser.ExprDataType {
	scale := parser.NumDecimalPlaces(n.value)
	return parser.NewDataTypeDecimal(int64(scale))
}

func (n *floatLiteralPlanExpression) String() string {
	return n.value
}

func (n *floatLiteralPlanExpression) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_expr"] = fmt.Sprintf("%T", n)
	result["dataType"] = n.Type().TypeDescription()
	result["value"] = n.value
	return result
}

func (n *floatLiteralPlanExpression) Children() []types.PlanExpression {
	return []types.PlanExpression{}
}

func (n *floatLiteralPlanExpression) WithChildren(children ...types.PlanExpression) (types.PlanExpression, error) {
	return n, nil
}

// boolLiteralPlanExpression is a bool literal
type boolLiteralPlanExpression struct {
	value bool
}

func newBoolLiteralPlanExpression(value bool) *boolLiteralPlanExpression {
	return &boolLiteralPlanExpression{
		value: value,
	}
}

func (n *boolLiteralPlanExpression) Evaluate(currentRow []interface{}) (interface{}, error) {
	return n.value, nil
}

func (n *boolLiteralPlanExpression) Type() parser.ExprDataType {
	return parser.NewDataTypeBool()
}

func (n *boolLiteralPlanExpression) String() string {
	return fmt.Sprintf("%v", n.value)
}

func (n *boolLiteralPlanExpression) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_expr"] = fmt.Sprintf("%T", n)
	result["dataType"] = n.Type().TypeDescription()
	result["value"] = n.value
	return result
}

func (n *boolLiteralPlanExpression) Children() []types.PlanExpression {
	return []types.PlanExpression{}
}

func (n *boolLiteralPlanExpression) WithChildren(children ...types.PlanExpression) (types.PlanExpression, error) {
	return n, nil
}

// dateLiteralPlanExpression is a date literal
type dateLiteralPlanExpression struct {
	value time.Time
}

func newDateLiteralPlanExpression(value time.Time) *dateLiteralPlanExpression {
	return &dateLiteralPlanExpression{
		value: value,
	}
}

func (n *dateLiteralPlanExpression) Evaluate(currentRow []interface{}) (interface{}, error) {
	return n.value, nil
}

func (n *dateLiteralPlanExpression) Type() parser.ExprDataType {
	return parser.NewDataTypeTimestamp()
}

func (n *dateLiteralPlanExpression) String() string {
	return n.value.Format(time.RFC3339Nano)
}

func (n *dateLiteralPlanExpression) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_expr"] = fmt.Sprintf("%T", n)
	result["dataType"] = n.Type().TypeDescription()
	result["value"] = n.value
	return result
}

func (n *dateLiteralPlanExpression) Children() []types.PlanExpression {
	return []types.PlanExpression{}
}

func (n *dateLiteralPlanExpression) WithChildren(children ...types.PlanExpression) (types.PlanExpression, error) {
	return n, nil
}

// stringLiteralPlanExpression is a string literal
type stringLiteralPlanExpression struct {
	value string
}

func newStringLiteralPlanExpression(value string) *stringLiteralPlanExpression {
	return &stringLiteralPlanExpression{
		value: value,
	}
}

func (n *stringLiteralPlanExpression) Evaluate(currentRow []interface{}) (interface{}, error) {
	return n.value, nil
}

func (n *stringLiteralPlanExpression) Type() parser.ExprDataType {
	return parser.NewDataTypeString()
}

func (n *stringLiteralPlanExpression) String() string {
	return fmt.Sprintf("'%s'", n.value)
}

func (n *stringLiteralPlanExpression) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_expr"] = fmt.Sprintf("%T", n)
	result["dataType"] = n.Type().TypeDescription()
	result["value"] = n.value
	return result
}

func (n *stringLiteralPlanExpression) Children() []types.PlanExpression {
	return []types.PlanExpression{}
}

func (n *stringLiteralPlanExpression) WithChildren(children ...types.PlanExpression) (types.PlanExpression, error) {
	return n, nil
}

// castPlanExpressionis a cast op
type castPlanExpression struct {
	lhs        types.PlanExpression
	targetType parser.ExprDataType
}

func newCastPlanExpression(lhs types.PlanExpression, targetType parser.ExprDataType) *castPlanExpression {
	return &castPlanExpression{
		lhs:        lhs,
		targetType: targetType,
	}
}

func (n *castPlanExpression) Evaluate(currentRow []interface{}) (interface{}, error) {
	evalLhs, err := n.lhs.Evaluate(currentRow)
	if err != nil {
		return nil, err
	}
	switch sourceType := n.lhs.Type().(type) {
	case *parser.DataTypeInt:
		nl, nlok := evalLhs.(int64)
		if !nlok {
			return nil, sql3.NewErrInternalf("unable to cast expression of type '%T' to type '%T'", n.lhs.Type(), n.targetType)
		}
		switch tt := n.targetType.(type) {
		case *parser.DataTypeInt, *parser.DataTypeID:
			return nl, nil
		case *parser.DataTypeBool:
			return nl > 0, nil
		case *parser.DataTypeDecimal:
			return pql.NewDecimal(nl*int64(math.Pow(10, float64(tt.Scale))), tt.Scale), nil
		case *parser.DataTypeString:
			return fmt.Sprintf("%d", nl), nil
		case *parser.DataTypeTimestamp:
			tm := time.Unix(nl, 0).UTC()
			return tm, nil
		}

	case *parser.DataTypeID:
		nl, nlok := evalLhs.(int64)
		if !nlok {
			return nil, sql3.NewErrInternalf("unable to cast expression of type '%T' to type '%T'", n.lhs.Type(), n.targetType)
		}
		switch tt := n.targetType.(type) {
		case *parser.DataTypeInt, *parser.DataTypeID:
			return nl, nil
		case *parser.DataTypeBool:
			return nl > 0, nil
		case *parser.DataTypeDecimal:
			return pql.NewDecimal(nl*int64(math.Pow(10, float64(tt.Scale))), tt.Scale), nil
		case *parser.DataTypeString:
			return fmt.Sprintf("%d", nl), nil
		case *parser.DataTypeTimestamp:
			tm := time.Unix(nl, 0).UTC()
			return tm, nil
		}

	case *parser.DataTypeBool:
		nl, nlok := evalLhs.(bool)
		if !nlok {
			return nil, sql3.NewErrInternalf("unable to cast expression of type '%T' to type '%T'", n.lhs.Type(), n.targetType)
		}
		switch n.targetType.(type) {
		case *parser.DataTypeInt, *parser.DataTypeID:
			if nl {
				return int64(1), nil
			}
			return int64(0), nil
		case *parser.DataTypeBool:
			return nl, nil
		case *parser.DataTypeString:
			return fmt.Sprintf("%v", nl), nil
		}

	case *parser.DataTypeDecimal:
		nl, nlok := evalLhs.(pql.Decimal)
		if !nlok {
			return nil, sql3.NewErrInternalf("unable to cast expression of type '%T' to type '%T'", n.lhs.Type(), n.targetType)
		}
		switch n.targetType.(type) {
		case *parser.DataTypeDecimal:
			return nl, nil
		case *parser.DataTypeString:
			return fmt.Sprintf("%v", nl), nil
		}

	case *parser.DataTypeIDSet:
		nl, nlok := evalLhs.([]int64)
		if !nlok {
			return nil, sql3.NewErrInternalf("unable to cast expression of type '%T' to type '%T'", n.lhs.Type(), n.targetType)
		}
		switch n.targetType.(type) {
		case *parser.DataTypeIDSet:
			return nl, nil
		case *parser.DataTypeString:
			//TODO(pok) come up with a better string representation of idset
			return fmt.Sprintf("%v", nl), nil
		}

	case *parser.DataTypeString:
		nl, nlok := evalLhs.(string)
		if !nlok {
			return nil, sql3.NewErrInternalf("unable to cast expression of type '%T' to type '%T'", n.lhs.Type(), n.targetType)
		}
		switch tt := n.targetType.(type) {
		case *parser.DataTypeInt, *parser.DataTypeID:
			i, err := strconv.Atoi(nl)
			if err != nil {
				//TODO(pok) need to push location into here
				return nil, sql3.NewErrInvalidCast(0, 0, nl, n.targetType.TypeDescription())
			}
			return int64(i), nil

		case *parser.DataTypeBool:
			i, err := strconv.ParseBool(nl)
			if err != nil {
				//TODO(pok) need to push location into here
				return nil, sql3.NewErrInvalidCast(0, 0, nl, n.targetType.TypeDescription())
			}
			return i, nil

		case *parser.DataTypeDecimal:
			castValue, err := pql.ParseDecimal(nl)
			if err != nil {
				//TODO(pok) need to push location into here
				return nil, sql3.NewErrInvalidCast(0, 0, nl, n.targetType.TypeDescription())
			}
			if tt.Scale < castValue.Scale {
				return nil, sql3.NewErrInvalidCast(0, 0, nl, n.targetType.TypeDescription())
			}

			return castValue, nil

		case *parser.DataTypeString:
			return nl, nil

		case *parser.DataTypeTimestamp:
			if tm, err := time.ParseInLocation(time.RFC3339Nano, nl, time.UTC); err == nil {
				return tm, nil
			} else if tm, err := time.ParseInLocation(time.RFC3339, nl, time.UTC); err == nil {
				return tm, nil
			} else if tm, err := time.ParseInLocation("2006-01-02", nl, time.UTC); err == nil {
				return tm, nil
			} else {
				return nil, sql3.NewErrInvalidCast(0, 0, nl, n.targetType.TypeDescription())
			}
		}

	case *parser.DataTypeStringSet:
		nl, nlok := evalLhs.([]string)
		if !nlok {
			return nil, sql3.NewErrInternalf("unable to cast expression of type '%T' to type '%T'", n.lhs.Type(), n.targetType)
		}
		switch n.targetType.(type) {
		case *parser.DataTypeStringSet:
			return nl, nil
		case *parser.DataTypeString:
			sort.Strings(nl)

			var ret strings.Builder

			// open bracket
			ret.WriteString("[")

			// elements
			var afterFirst bool
			for i := range nl {
				if afterFirst {
					ret.WriteString(",")
				}
				ret.WriteString(`"` + strings.ReplaceAll(nl[i], `"`, `\"`) + `"`)
				afterFirst = true
			}

			// close braket
			ret.WriteString("]")

			return ret.String(), nil
		}

	case *parser.DataTypeTimestamp:
		nl, nlok := evalLhs.(time.Time)
		if !nlok {
			return nil, sql3.NewErrInternalf("unable to cast expression of type '%T' to type '%T'", n.lhs.Type(), n.targetType)
		}
		switch n.targetType.(type) {
		case *parser.DataTypeTimestamp:
			return nl, nil
		case *parser.DataTypeInt:
			return nl.Unix(), nil
		case *parser.DataTypeString:
			return nl.Format(time.RFC3339), nil
		}

	default:
		return nil, sql3.NewErrInternalf("unhandled cast type '%T'", sourceType)
	}
	return nil, sql3.NewErrInternalf("unable to cast expression of type '%T' to type '%T'", n.lhs.Type(), n.targetType)
}

func (n *castPlanExpression) Type() parser.ExprDataType {
	return n.targetType
}

func (n *castPlanExpression) String() string {
	return fmt.Sprintf("cast(%s as %s)", n.lhs.String(), n.targetType.TypeDescription())
}

func (n *castPlanExpression) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_expr"] = fmt.Sprintf("%T", n)
	result["dataType"] = n.Type().TypeDescription()
	result["lhs"] = n.lhs.Plan()
	return result
}

func (n *castPlanExpression) Children() []types.PlanExpression {
	return []types.PlanExpression{
		n.lhs,
	}
}

func (n *castPlanExpression) WithChildren(children ...types.PlanExpression) (types.PlanExpression, error) {
	if len(children) != 1 {
		return nil, sql3.NewErrInternalf("unexpected number of children '%d'", len(children))
	}
	return newCastPlanExpression(children[0], n.targetType), nil
}

// exprListPlanExpression is an expression list
type exprListPlanExpression struct {
	exprs []types.PlanExpression
}

func newExprListExpression(exprs []types.PlanExpression) *exprListPlanExpression {
	return &exprListPlanExpression{
		exprs: exprs,
	}
}

func (n *exprListPlanExpression) Evaluate(currentRow []interface{}) (interface{}, error) {
	return nil, nil
}

func (n *exprListPlanExpression) Type() parser.ExprDataType {
	return parser.NewDataTypeVoid()
}

func (n *exprListPlanExpression) String() string {
	var s string
	for idx, expr := range n.exprs {
		if idx > 0 {
			s += ", "
		}
		s += expr.String()
	}
	return fmt.Sprintf("(%s)", s)
}

func (n *exprListPlanExpression) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_expr"] = fmt.Sprintf("%T", n)
	ps := make([]interface{}, 0)
	for _, e := range n.exprs {
		ps = append(ps, e.Plan())
	}
	result["exprs"] = ps
	return result
}

func (n *exprListPlanExpression) Children() []types.PlanExpression {
	return n.exprs
}

func (n *exprListPlanExpression) WithChildren(children ...types.PlanExpression) (types.PlanExpression, error) {
	if len(children) != len(n.exprs) {
		return nil, sql3.NewErrInternalf("unexpected number of children '%d'", len(children))
	}
	return newExprListExpression(children), nil
}

// exprSetLiteralPlanExpression is a set literal
type exprSetLiteralPlanExpression struct {
	members  []types.PlanExpression
	dataType parser.ExprDataType
}

func newExprSetLiteralPlanExpression(members []types.PlanExpression, dataType parser.ExprDataType) *exprSetLiteralPlanExpression {
	return &exprSetLiteralPlanExpression{
		members:  members,
		dataType: dataType,
	}
}

func (n *exprSetLiteralPlanExpression) Evaluate(currentRow []interface{}) (interface{}, error) {
	switch typ := n.dataType.(type) {
	case *parser.DataTypeIDSet:
		result := []int64{}
		for _, e := range n.members {
			er, err := e.Evaluate(currentRow)
			if err != nil {
				return nil, err
			}
			coercedEr, err := coerceValue(e.Type(), &parser.DataTypeID{}, er, parser.Pos{Line: 0, Column: 0})
			if err != nil {
				return nil, err
			}
			eri, ok := coercedEr.(int64)
			if !ok {
				return nil, sql3.NewErrInternalf("unable to convert element result")
			}
			result = append(result, eri)
		}
		return result, nil

	case *parser.DataTypeStringSet:
		result := []string{}
		for _, e := range n.members {
			er, err := e.Evaluate(currentRow)
			if err != nil {
				return nil, err
			}
			ers, ok := er.(string)
			if !ok {
				return nil, sql3.NewErrInternalf("unable to convert element result")
			}
			result = append(result, ers)
		}
		return result, nil
	default:
		return nil, sql3.NewErrInternalf("unexpected set literal type '%T'", typ)
	}
}

func (n *exprSetLiteralPlanExpression) Type() parser.ExprDataType {
	return n.dataType
}

func (n *exprSetLiteralPlanExpression) String() string {
	var members string
	for idx, m := range n.members {
		if idx > 0 {
			members += ", "
		}
		members += m.String()
	}
	return fmt.Sprintf("[%s]", members)
}

func (n *exprSetLiteralPlanExpression) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_expr"] = fmt.Sprintf("%T", n)
	ps := make([]interface{}, 0)
	for _, e := range n.members {
		ps = append(ps, e.Plan())
	}
	result["members"] = ps
	return result
}

func (n *exprSetLiteralPlanExpression) Children() []types.PlanExpression {
	return n.members
}

func (n *exprSetLiteralPlanExpression) WithChildren(children ...types.PlanExpression) (types.PlanExpression, error) {
	if len(children) != len(n.members) {
		return nil, sql3.NewErrInternalf("unexpected number of children '%d'", len(children))
	}
	return newExprSetLiteralPlanExpression(children, n.dataType), nil
}

// exprTupleLiteralPlanExpression is a tuple literal
type exprTupleLiteralPlanExpression struct {
	members  []types.PlanExpression
	dataType parser.ExprDataType
}

func newExprTupleLiteralPlanExpression(members []types.PlanExpression, dataType parser.ExprDataType) *exprTupleLiteralPlanExpression {
	return &exprTupleLiteralPlanExpression{
		members:  members,
		dataType: dataType,
	}
}

func (n *exprTupleLiteralPlanExpression) Evaluate(currentRow []interface{}) (interface{}, error) {
	timestampEval, err := n.members[0].Evaluate(currentRow)
	if err != nil {
		return nil, err
	}

	// if it is a string, do a coercion
	if val, ok := timestampEval.(string); ok {
		if tm, err := timestampFromString(val); err != nil {
			return nil, sql3.NewErrInvalidTypeCoercion(0, 0, val, n.members[0].Type().TypeDescription())
		} else {
			timestampEval = tm
		}
	}

	setEval, err := n.members[1].Evaluate(currentRow)
	if err != nil {
		return nil, err
	}

	// nil if anything is nil
	if timestampEval == nil || setEval == nil {
		return nil, nil
	}

	return []interface{}{
		timestampEval,
		setEval,
	}, nil
}

func (n *exprTupleLiteralPlanExpression) Type() parser.ExprDataType {
	return n.dataType
}

func (n *exprTupleLiteralPlanExpression) String() string {
	members := ""
	for idx, m := range n.members {
		if idx > 0 {
			members += ", "
		}
		members += m.String()
	}
	return fmt.Sprintf("{%s}", members)
}

func (n *exprTupleLiteralPlanExpression) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_expr"] = fmt.Sprintf("%T", n)
	ps := make([]interface{}, 0)
	for _, e := range n.members {
		ps = append(ps, e.Plan())
	}
	result["members"] = ps
	return result
}

func (n *exprTupleLiteralPlanExpression) Children() []types.PlanExpression {
	return n.members
}

func (n *exprTupleLiteralPlanExpression) WithChildren(children ...types.PlanExpression) (types.PlanExpression, error) {
	if len(children) != len(n.members) {
		return nil, sql3.NewErrInternalf("unexpected number of children '%d'", len(children))
	}
	return newExprTupleLiteralPlanExpression(children, n.dataType), nil
}

// compileExpr returns a types.PlanExpression tree for a given parser.Expr
func (p *ExecutionPlanner) compileExpr(expr parser.Expr) (_ types.PlanExpression, err error) {
	if expr == nil {
		return nil, nil
	}

	switch expr := expr.(type) {
	case *parser.BinaryExpr:
		return p.compileBinaryExpr(expr)

	case *parser.BoolLit:
		return newBoolLiteralPlanExpression(expr.Value), nil

	case *parser.Call:
		return p.compileCallExpr(expr)

	case *parser.CastExpr:
		castExpr, err := p.compileExpr(expr.X)
		if err != nil {
			return nil, err
		}
		dataType, err := dataTypeFromParserType(expr.Type)
		if err != nil {
			return nil, err
		}
		return newCastPlanExpression(castExpr, dataType), nil

	case *parser.Exists:
		return nil, sql3.NewErrInternal("exists expressions are not supported")

	case *parser.ExprList:
		exprList := []types.PlanExpression{}
		for _, e := range expr.Exprs {
			listExpr, err := p.compileExpr(e)
			if err != nil {
				return nil, err
			}
			exprList = append(exprList, listExpr)
		}
		return newExprListExpression(exprList), nil

	case *parser.SetLiteralExpr:
		exprList := []types.PlanExpression{}
		for _, e := range expr.Members {
			listExpr, err := p.compileExpr(e)
			if err != nil {
				return nil, err
			}
			exprList = append(exprList, listExpr)
		}
		return newExprSetLiteralPlanExpression(exprList, expr.DataType()), nil

	case *parser.TupleLiteralExpr:
		exprList := []types.PlanExpression{}
		for _, e := range expr.Members {
			listExpr, err := p.compileExpr(e)
			if err != nil {
				return nil, err
			}
			exprList = append(exprList, listExpr)
		}
		return newExprTupleLiteralPlanExpression(exprList, expr.DataType()), nil

	case *parser.Ident:
		return nil, sql3.NewErrInternal("identifiers are not supported")

	case *parser.NullLit:
		return newNullLiteralPlanExpression(), nil

	case *parser.IntegerLit:

		val, err := strconv.ParseInt(expr.Value, 10, 64)
		if err != nil {
			return nil, err
		}
		return newIntLiteralPlanExpression(val), nil

	case *parser.FloatLit:
		return newFloatLiteralPlanExpression(expr.Value), nil

	case *parser.DateLit:
		return newDateLiteralPlanExpression(expr.Value), nil

	case *parser.ParenExpr:
		return p.compileExpr(expr.X)

	case *parser.Variable:
		ref := newVariableRefPlanExpression(expr.Name, expr.VariableIndex, expr.DataType())
		return ref, nil

	case *parser.QualifiedRef:
		ref := newQualifiedRefPlanExpression(parser.IdentName(expr.Table), parser.IdentName(expr.Column), expr.ColumnIndex, expr.DataType())
		p.addReference(ref)
		return ref, nil

	case *parser.Range:
		lhs, err := p.compileExpr(expr.X)
		if err != nil {
			return nil, err
		}
		rhs, err := p.compileExpr(expr.Y)
		if err != nil {
			return nil, err
		}
		return newRangeOpPlanExpression(lhs, rhs, expr.ResultDataType), nil

	case *parser.StringLit:
		return newStringLiteralPlanExpression(expr.Value), nil

	case *parser.UnaryExpr:
		return p.compileUnaryExpr(expr)

	case *parser.CaseExpr:
		operand, err := p.compileExpr(expr.Operand)
		if err != nil {
			return nil, err
		}
		blocks := []types.PlanExpression{}
		for _, b := range expr.Blocks {
			block, err := p.compileExpr(b)
			if err != nil {
				return nil, err
			}
			blocks = append(blocks, block)
		}

		elseExpr, err := p.compileExpr(expr.ElseExpr)
		if err != nil {
			return nil, err
		}
		return newCasePlanExpression(operand, blocks, elseExpr, expr.DataType()), nil

	case *parser.CaseBlock:

		condition, err := p.compileExpr(expr.Condition)
		if err != nil {
			return nil, err
		}
		body, err := p.compileExpr(expr.Body)
		if err != nil {
			return nil, err
		}
		return newCaseBlockPlanExpression(condition, body), nil

	case *parser.SelectStatement:
		selOp, err := p.compileSelectStatement(expr, true)
		if err != nil {
			return nil, err
		}
		return newSubqueryPlanExpression(selOp), nil

	default:
		return nil, sql3.NewErrInternalf("unexpected SQL expression type: %T", expr)
	}
}

func (p *ExecutionPlanner) compileUnaryExpr(expr *parser.UnaryExpr) (_ types.PlanExpression, err error) {
	switch op := expr.Op; op {

	//bitwise operators
	case parser.BITNOT:
		x, err := p.compileExpr(expr.X)
		if err != nil {
			return nil, err
		}
		return newUnaryOpPlanExpression(expr.Op, x, expr.ResultDataType), nil

	//arithmetic operators
	case parser.PLUS, parser.MINUS:
		x, err := p.compileExpr(expr.X)
		if err != nil {
			return nil, err
		}
		return newUnaryOpPlanExpression(expr.Op, x, expr.ResultDataType), nil
	default:
		return nil, sql3.NewErrInternalf("unexpected unary expression operator: %s", expr.Op)
	}
}

func (p *ExecutionPlanner) compileBinaryExpr(expr *parser.BinaryExpr) (_ types.PlanExpression, err error) {
	x, err := p.compileExpr(expr.X)
	if err != nil {
		return nil, err
	}
	y, err := p.compileExpr(expr.Y)
	if err != nil {
		return nil, err
	}

	switch op := expr.Op; op {

	//logical operators
	case parser.AND, parser.OR:
		return newBinOpPlanExpression(x, expr.Op, y, expr.ResultDataType), nil

	//equality operators
	case parser.EQ, parser.NE:
		return newBinOpPlanExpression(x, expr.Op, y, expr.ResultDataType), nil

	//comparison operators
	case parser.LT, parser.LE, parser.GT, parser.GE:
		return newBinOpPlanExpression(x, expr.Op, y, expr.ResultDataType), nil

	//arithmetic operators
	case parser.PLUS, parser.MINUS, parser.STAR, parser.SLASH, parser.REM:

		//TODO(pok) move constant folding to optimizer
		opx, okx := x.(*intLiteralPlanExpression)
		opy, oky := y.(*intLiteralPlanExpression)
		if okx && oky {
			//both literals so we can fold
			numx := opx.value
			numy := opy.value

			switch op {
			case parser.PLUS:
				value := numx + numy
				return newIntLiteralPlanExpression(value), nil

			case parser.MINUS:
				value := numx - numy
				return newIntLiteralPlanExpression(value), nil

			case parser.STAR:
				value := numx * numy
				return newIntLiteralPlanExpression(value), nil

			case parser.SLASH:
				value := numx / numy
				return newIntLiteralPlanExpression(value), nil

			case parser.REM:
				value := numx % numy
				return newIntLiteralPlanExpression(value), nil

			default:
				//run home to momma
				return newBinOpPlanExpression(x, expr.Op, y, expr.ResultDataType), nil
			}
		} else {
			return newBinOpPlanExpression(x, expr.Op, y, expr.ResultDataType), nil
		}

	//bitwise operators
	case parser.BITAND, parser.BITOR, parser.LSHIFT, parser.RSHIFT:
		return newBinOpPlanExpression(x, expr.Op, y, expr.ResultDataType), nil

	//null test
	case parser.IS, parser.ISNOT:
		return newBinOpPlanExpression(x, expr.Op, y, expr.ResultDataType), nil

	case parser.IN, parser.NOTIN:
		return newInOpPlanExpression(x, expr.Op, y), nil

	case parser.BETWEEN, parser.NOTBETWEEN:
		return newBetweenOpPlanExpression(x, expr.Op, y), nil

	case parser.CONCAT:
		return newBinOpPlanExpression(x, expr.Op, y, expr.ResultDataType), nil

	case parser.LIKE, parser.NOTLIKE:
		return newBinOpPlanExpression(x, expr.Op, y, expr.ResultDataType), nil

	default:
		return nil, sql3.NewErrInternalf("unexpected binary expression operator: %s", expr.Op)
	}
}

func (p *ExecutionPlanner) compileCallExpr(expr *parser.Call) (_ types.PlanExpression, err error) {
	args := []types.PlanExpression{}
	for _, a := range expr.Args {
		arg, err := p.compileExpr(a)
		if err != nil {
			return nil, err
		}
		args = append(args, arg)
	}

	callName := strings.ToUpper(parser.IdentName(expr.Name))
	switch callName {
	case "COUNT":
		var agg types.PlanExpression
		if expr.Distinct.IsValid() {
			agg = newCountDistinctPlanExpression(args[0], expr.ResultDataType)
		} else {
			agg = newCountPlanExpression(args[0], expr.ResultDataType)
		}
		p.addAggregate(agg)
		return agg, nil

	case "SUM":
		agg := newSumPlanExpression(args[0], expr.ResultDataType)
		p.addAggregate(agg)
		return agg, nil

	case "AVG":
		agg := newAvgPlanExpression(args[0], expr.ResultDataType)
		p.addAggregate(agg)
		return agg, nil

	case "PERCENTILE":
		agg := newPercentilePlanExpression(args[0], args[1], expr.ResultDataType)
		p.addAggregate(agg)
		return agg, nil

	case "MIN":
		agg := newMinPlanExpression(args[0], expr.ResultDataType)
		p.addAggregate(agg)
		return agg, nil

	case "MAX":
		agg := newMaxPlanExpression(args[0], expr.ResultDataType)
		p.addAggregate(agg)
		return agg, nil

	default:
		return newCallPlanExpression(parser.IdentName(expr.Name), args, expr.ResultDataType), nil
	}
}

func (p *ExecutionPlanner) compileOrderingTermExpr(expr parser.Expr) (index int, err error) {
	if expr == nil {
		return 0, nil
	}

	switch thisExpr := expr.(type) {
	case *parser.QualifiedRef:
		return thisExpr.ColumnIndex, nil

	case *parser.IntegerLit:
		val, err := strconv.ParseInt(thisExpr.Value, 10, 64)
		if err != nil {
			return 0, err
		}
		// subtract one because ordering terms are 1 based, not 0 based
		return int(val - 1), nil

	default:
		return 0, sql3.NewErrInternalf("unexpected ordering expression type: %T", expr)

	}
}

// wildCardToRegexp converts a wildcard pattern to a regular expression pattern.
// used by the LIKE/NOT LIKE operator
func wildCardToRegexp(pattern string) string {
	var result strings.Builder
	result.WriteString("(?i)")

	rpattern := strings.Replace(pattern, "%", ".*", -1)
	rpattern = strings.Replace(rpattern, "_", ".+", -1)
	result.WriteString(rpattern)

	return result.String()
}

// timeFromString attempts to parse the string to a time.Time using a series of
// time formats.
func timestampFromString(s string) (time.Time, error) {
	if tm, err := time.ParseInLocation(time.RFC3339Nano, s, time.UTC); err == nil {
		return tm, nil
	} else if tm, err := time.ParseInLocation(time.RFC3339, s, time.UTC); err == nil {
		return tm, nil
	} else if tm, err := time.ParseInLocation("2006-01-02", s, time.UTC); err == nil {
		return tm, nil
	}

	return time.Time{}, sql3.NewErrInvalidTypeCoercion(0, 0, s, "time.Time")
}
