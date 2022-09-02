package planner

import (
	"strings"

	"github.com/featurebasedb/featurebase/v3/sql3"
	"github.com/featurebasedb/featurebase/v3/sql3/parser"
)

func (n *callPlanExpression) EvaluateSetContains(currentRow []interface{}) (interface{}, error) {
	targetSetEval, err := n.args[0].Evaluate(currentRow)
	if err != nil {
		return nil, err
	}

	testValueEval, err := n.args[1].Evaluate(currentRow)
	if err != nil {
		return nil, err
	}

	//if either term is null, then null
	if testValueEval == nil || targetSetEval == nil {
		return nil, nil
	}

	if targetSetEval != nil {
		switch typ := n.args[0].Type().(type) {
		case *parser.DataTypeStringSet:
			targetSet, ok := targetSetEval.([]string)
			if !ok {
				return nil, sql3.NewErrInternalf("unable to convert value")
			}

			testValue, ok := testValueEval.(string)
			if !ok {
				return nil, sql3.NewErrInternalf("unable to convert value")
			}

			return stringSetContains(targetSet, testValue), nil

		case *parser.DataTypeIDSet:
			targetSet, ok := targetSetEval.([]int64)
			if !ok {
				return nil, sql3.NewErrInternalf("unable to convert value")
			}

			testValue, ok := testValueEval.(int64)
			if !ok {
				return nil, sql3.NewErrInternalf("unable to convert value")
			}

			return intSetContains(targetSet, int64(testValue)), nil

		default:
			return nil, sql3.NewErrInternalf("unexpected data type '%T'", typ)
		}
	}
	return nil, sql3.NewErrInternalf("unable to to find column '%s' in currentColumns", n.name)
}

func (n *callPlanExpression) EvaluateSetContainsAny(currentRow []interface{}) (interface{}, error) {
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

			return stringSetContainsAny(targetSet, testSet), nil

		case *parser.DataTypeIDSet:
			targetSet, ok := targetSetEval.([]int64)
			if !ok {
				return nil, sql3.NewErrInternalf("unable to convert value")
			}

			testSet, ok := testSetEval.([]int64)
			if !ok {
				return nil, sql3.NewErrInternalf("unable to convert value")
			}

			return intSetContainsAny(targetSet, testSet), nil

		default:
			return nil, sql3.NewErrInternalf("unexpected data type '%T'", typ)
		}

	}
	return nil, sql3.NewErrInternalf("unable to to find column '%s' in currentColumns", n.name)
}

func (n *callPlanExpression) EvaluateSetContainsAll(currentRow []interface{}) (interface{}, error) {
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

func stringSetContains(set []string, val string) bool {
	for _, v := range set {
		if strings.EqualFold(v, val) {
			return true
		}
	}
	return false
}

func stringSetContainsAny(targetSet []string, testSet []string) bool {
	for _, test := range testSet {
		if stringSetContains(targetSet, test) {
			return true
		}
	}
	return false
}

func stringSetContainsAll(targetSet []string, testSet []string) bool {
	for _, test := range testSet {
		if !stringSetContains(targetSet, test) {
			return false
		}
	}
	return true
}

func intSetContains(set []int64, val int64) bool {
	for _, v := range set {
		if v == val {
			return true
		}
	}
	return false
}

func intSetContainsAny(targetSet []int64, testSet []int64) bool {
	for _, test := range testSet {
		if intSetContains(targetSet, test) {
			return true
		}
	}
	return false
}

func intSetContainsAll(targetSet []int64, testSet []int64) bool {
	for _, test := range testSet {
		if !intSetContains(targetSet, test) {
			return false
		}
	}
	return true
}
