// Copyright 2021 Molecula Corp. All rights reserved.
package defs

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/PaesslerAG/gval"
	"github.com/PaesslerAG/jsonpath"
	"github.com/featurebasedb/featurebase/v3/errors"
)

// TableTests is the list of tests which get run by TestSQL_Execute in
// sql_test.go. They're defined here just to keep the test definitions separate
// from the test execution logic.
var TableTests []TableTest = []TableTest{
	minmaxnegatives,
	unkeyed,
	keyed,

	selectTests,
	selectKeyedTests,
	selectHavingTests,
	filterPredicates,
	filterPredicatesIdKey,
	filterPredicatesId,
	filterPredicatesInt,
	filterPredicatesBool,
	filterPredicatesTimestamp,
	filterPredicatesDecimal,
	filterPredicatesString,
	orderByTests,
	distinctTests,

	subqueryTests,
	viewTests,

	topTests,

	deleteTests,

	setLiteralTests,
	setFunctionTests,
	setParameterTests,
	datePartTests,
	toTimestampTests,
	datetimeAddTests,
	stringScalarFunctionsTests,

	insertTest,
	insertTimestampTest,
	keyedInsertTest,
	timestampLiterals,
	unaryOpExprWithInt,
	unaryOpExprWithID,
	unaryOpExprWithBool,
	unaryOpExprWithDecimal,
	unaryOpExprWithTimestamp,
	unaryOpExprWithIDSet,
	unaryOpExprWithString,
	unaryOpExprWithStringSet,

	binOpExprWithIntInt,
	binOpExprWithIntBool,
	binOpExprWithIntID,
	binOpExprWithIntDecimal,
	binOpExprWithIntTimestamp,
	binOpExprWithIntIDSet,
	binOpExprWithIntString,
	binOpExprWithIntStringSet,

	binOpExprWithBoolInt,
	binOpExprWithBoolBool,
	binOpExprWithBoolID,
	binOpExprWithBoolDecimal,
	binOpExprWithBoolTimestamp,
	binOpExprWithBoolIDSet,
	binOpExprWithBoolString,
	binOpExprWithBoolStringSet,

	binOpExprWithIDInt,
	binOpExprWithIDBool,
	binOpExprWithIDID,
	binOpExprWithIDDecimal,
	binOpExprWithIDTimestamp,
	binOpExprWithIDIDSet,
	binOpExprWithIDString,
	binOpExprWithIDStringSet,

	binOpExprWithDecInt,
	binOpExprWithDecBool,
	binOpExprWithDecID,
	binOpExprWithDecDecimal,
	binOpExprWithDecTimestamp,
	binOpExprWithDecIDSet,
	binOpExprWithDecString,
	binOpExprWithDecStringSet,

	binOpExprWithTSInt,
	binOpExprWithTSBool,
	binOpExprWithTSID,
	binOpExprWithTSDecimal,
	binOpExprWithTSTimestamp,
	binOpExprWithTSIDSet,
	binOpExprWithTSString,
	binOpExprWithTSStringSet,

	binOpExprWithIDSetInt,
	binOpExprWithIDSetBool,
	binOpExprWithIDSetID,
	binOpExprWithIDSetDecimal,
	binOpExprWithIDSetTimestamp,
	binOpExprWithIDSetIDSet,
	binOpExprWithIDSetString,
	binOpExprWithIDSetStringSet,

	binOpExprWithStringInt,
	binOpExprWithStringBool,
	binOpExprWithStringID,
	binOpExprWithStringDecimal,
	binOpExprWithStringTimestamp,
	binOpExprWithStringIDSet,
	binOpExprWithStringString,
	binOpExprWithStringStringSet,

	binOpExprWithStringSetInt,
	binOpExprWithStringSetBool,
	binOpExprWithStringSetID,
	binOpExprWithStringSetDecimal,
	binOpExprWithStringSetTimestamp,
	binOpExprWithStringSetIDSet,
	binOpExprWithStringSetString,
	binOpExprWithStringSetStringSet,

	//cast tests
	castIntLiteral,
	castInt,

	castBool,
	castDecimal,
	castID,
	castIDSet,
	castString,
	castStringSet,
	castTimestamp,

	//like tests
	likeTests,
	notLikeTests,

	//null tests
	nullTests,
	notNullTests,

	//null filter tests
	nullFilterTests,

	//between tests
	betweenTests,
	notBetweenTests,

	//in tests
	inTests,
	notInTests,

	//aggregate tests
	countTests,
	countDistinctTests,
	sumTests,
	avgTests,
	percentileTests,
	minmaxTests,

	//groupby tests
	groupByTests,

	//create table tests
	createTable,
	alterTable,

	//joins
	joinTestsUsers,
	joinTestsOrders,
	joinTests,

	// bulk insert
	bulkInsertTable,
	bulkInsert,

	//bool (batch logic)
	boolTests,

	// time quantums
	timeQuantumTest,
	timeQuantumQueryTest,

	// forward-ported SQL1 tests
	sql1TestsGrouper,
	sql1TestsJoiner,
	sql1TestsDelete,
	sql1TestsQueries,
}

func knownTimestamp() time.Time {
	tm, err := time.ParseInLocation(time.RFC3339, "2012-11-01T22:08:41+00:00", time.UTC)
	if err != nil {
		panic(err.Error())
	}
	return tm
}

func knownSubSecondTimestamp() time.Time {
	tm := knownTimestamp()
	duration, err := time.ParseDuration("100200300ns")
	if err != nil {
		panic(err.Error())
	}
	tm = tm.Add(duration)
	return tm
}

func timestampFromString(s string) time.Time {
	tm, err := time.ParseInLocation(time.RFC3339, s, time.UTC)
	if err != nil {
		panic(err.Error())
	}
	return tm
}

// operatorPresentAtPath() tests if an named operator exists in a plan
//
//	operatorPresentAtPath() takes a FeatureBase query plan as a []byte
//	(so we do not have to convert to and from string), a path (as a
//	jsonpath expression) and an operator. The function returns nil if
//	the result of the jsonpath expression evaluation contains the operator.
func operatorPresentAtPath(jplan []byte, path string, operator string) error {
	// fmt.Printf("%s\n", string(jplan))
	v := interface{}(nil)
	err := json.Unmarshal(jplan, &v)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("expected '%s' to be present", operator))
	}

	builder := gval.Full(jsonpath.PlaceholderExtension())
	expr, err := builder.NewEvaluable(path)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("expected '%s' to be present", operator))
	}
	eval, err := expr(context.Background(), v)
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("expected '%s' to be present", operator))
	}
	s, ok := eval.(string)
	if ok && strings.EqualFold(s, operator) {
		return nil
	}
	return fmt.Errorf("expected '%s' to be present", operator)
}
