// Copyright 2021 Molecula Corp. All rights reserved.
package defs

import (
	"time"
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
	stringScalarFunctionsTests,

	insertTest,
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

	//bool (batch logic)
	boolTests,

	// time quantums
	timeQuantumInsertTest,
}

func knownTimestamp() time.Time {
	tm, err := time.ParseInLocation(time.RFC3339, "2012-11-01T22:08:41+00:00", time.UTC)
	if err != nil {
		panic(err.Error())
	}
	return tm
}
