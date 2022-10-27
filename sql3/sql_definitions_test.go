// Copyright 2021 Molecula Corp. All rights reserved.
package sql3_test

import (
	"time"

	"github.com/molecula/featurebase/v3/pql"
)

// tableTests is the list of tests which get run by TestSQL_Execute in
// sql_test.go. They're defined here just to keep the test definitions separate
// from the test execution logic.
var tableTests []tableTest = []tableTest{
	{
		name: "minmaxnegatives",
		table: tbl(
			"minmaxnegatives",
			srcHdrs(
				srcHdr("_id", fldTypeID),
				srcHdr("positive_int", fldTypeInt, "min 10", "max 100"),
				srcHdr("negative_int", fldTypeInt, "min -100", "max -10"),
			),
			srcRows(
				srcRow(int64(1), int64(11), int64(-11)),
				srcRow(int64(2), int64(22), int64(-22)),
				srcRow(int64(3), int64(33), int64(-33)),
			),
		),
		sqlTests: []sqlTest{},
	},
	{
		name: "unkeyed",
		table: tbl(
			"unkeyed",
			srcHdrs(
				srcHdr("_id", fldTypeID),
				srcHdr("an_int", fldTypeInt, "min 0", "max 100"),
				srcHdr("an_id_set", fldTypeIDSet),
				srcHdr("an_id", fldTypeID),
				srcHdr("a_string", fldTypeString),
				srcHdr("a_string_set", fldTypeStringSet),
				srcHdr("a_decimal", fldTypeDecimal2),
			),
			srcRows(
				srcRow(int64(1), int64(11), []int64{11, 12, 13}, int64(101), "str1", []string{"a1", "b1", "c1"}, float64(123.45)),
				srcRow(int64(2), int64(22), []int64{21, 22, 23}, int64(201), "str2", []string{"a2", "b2", "c2"}, float64(234.56)),
				srcRow(int64(3), int64(33), []int64{31, 32, 33}, int64(301), "str3", []string{"a3", "b3", "c3"}, float64(345.67)),
				srcRow(int64(4), int64(44), []int64{41, 42, 43}, int64(401), "str4", []string{"a4", "b4", "c4"}, float64(456.78)),
			),
		),
		sqlTests: []sqlTest{
			{
				// Select all.
				name: "select-all",
				sqls: sqls(
					"select * from unkeyed",
					"select _id, an_int, an_id_set, an_id, a_string, a_string_set, a_decimal from unkeyed",
				),
				expHdrs: hdrs(
					hdr("_id", fldTypeID),
					hdr("an_int", fldTypeInt),
					hdr("an_id_set", fldTypeIDSet),
					hdr("an_id", fldTypeID),
					hdr("a_string", fldTypeString),
					hdr("a_string_set", fldTypeStringSet),
					hdr("a_decimal", fldTypeDecimal2),
				),
				expRows: rows(
					row(int64(1), int64(11), []int64{11, 12, 13}, int64(101), "str1", []string{"a1", "b1", "c1"}, pql.NewDecimal(12345, 2)),
					row(int64(2), int64(22), []int64{21, 22, 23}, int64(201), "str2", []string{"a2", "b2", "c2"}, pql.NewDecimal(23456, 2)),
					row(int64(3), int64(33), []int64{31, 32, 33}, int64(301), "str3", []string{"a3", "b3", "c3"}, pql.NewDecimal(34567, 2)),
					row(int64(4), int64(44), []int64{41, 42, 43}, int64(401), "str4", []string{"a4", "b4", "c4"}, pql.NewDecimal(45678, 2)),
				),
				compare: compareExactUnordered,
			},
			{
				// Select all with top.
				name: "select-all-with-top",
				sqls: sqls(
					"select top(2) * from unkeyed",
				),
				expHdrs: hdrs(
					hdr("_id", fldTypeID),
					hdr("an_int", fldTypeInt),
					hdr("an_id_set", fldTypeIDSet),
					hdr("an_id", fldTypeID),
					hdr("a_string", fldTypeString),
					hdr("a_string_set", fldTypeStringSet),
					hdr("a_decimal", fldTypeDecimal2),
				),
				expRows: rows(
					row(int64(1), int64(11), []int64{11, 12, 13}, int64(101), "str1", []string{"a1", "b1", "c1"}, pql.NewDecimal(12345, 2)),
					row(int64(2), int64(22), []int64{21, 22, 23}, int64(201), "str2", []string{"a2", "b2", "c2"}, pql.NewDecimal(23456, 2)),
				),
				compare: compareExactUnordered,
			},
			{
				// Select all with where on each field.
				name: "select-all-with-where",
				sqls: sqls(
					"select * from unkeyed where an_int = 22",
					"select * from unkeyed where a_string = 'str2'",
					"select * from unkeyed where an_id = 201",
				),
				expHdrs: hdrs(
					hdr("_id", fldTypeID),
					hdr("an_int", fldTypeInt),
					hdr("an_id_set", fldTypeIDSet),
					hdr("an_id", fldTypeID),
					hdr("a_string", fldTypeString),
					hdr("a_string_set", fldTypeStringSet),
					hdr("a_decimal", fldTypeDecimal2),
				),
				expRows: rows(
					row(int64(2), int64(22), []int64{21, 22, 23}, int64(201), "str2", []string{"a2", "b2", "c2"}, pql.NewDecimal(23456, 2)),
				),
				compare: compareExactOrdered,
			},
		},
	},
	{
		table: tbl(
			"keyed",
			srcHdrs(
				srcHdr("_id", fldTypeString),
				srcHdr("an_int", fldTypeInt, "min 0", "max 100"),
				srcHdr("an_id_set", fldTypeIDSet),
				srcHdr("an_id", fldTypeID),
				srcHdr("a_string", fldTypeString),
				srcHdr("a_string_set", fldTypeStringSet),
			),
			srcRows(
				srcRow("one", int64(11), []int64{11, 12, 13}, int64(101), "str1", []string{"a1", "b1", "c1"}),
				srcRow("two", int64(22), []int64{21, 22, 23}, int64(201), "str2", []string{"a2", "b2", "c2"}),
				srcRow("three", int64(33), []int64{31, 32, 33}, int64(301), "str3", []string{"a3", "b3", "c3"}),
				srcRow("four", int64(44), []int64{41, 42, 43}, int64(401), "str4", []string{"a4", "b4", "c4"}),
			),
		),
		sqlTests: []sqlTest{
			{
				// Select all.
				name: "select-all",
				sqls: sqls(
					"select * from keyed",
					"select _id, an_int, an_id_set, an_id, a_string, a_string_set from keyed",
				),
				expHdrs: hdrs(
					hdr("_id", fldTypeString),
					hdr("an_int", fldTypeInt),
					hdr("an_id_set", fldTypeIDSet),
					hdr("an_id", fldTypeID),
					hdr("a_string", fldTypeString),
					hdr("a_string_set", fldTypeStringSet),
				),
				expRows: rows(
					row("one", int64(11), []int64{11, 12, 13}, int64(101), "str1", []string{"a1", "b1", "c1"}),
					row("two", int64(22), []int64{21, 22, 23}, int64(201), "str2", []string{"a2", "b2", "c2"}),
					row("three", int64(33), []int64{31, 32, 33}, int64(301), "str3", []string{"a3", "b3", "c3"}),
					row("four", int64(44), []int64{41, 42, 43}, int64(401), "str4", []string{"a4", "b4", "c4"}),
				),
				compare: compareExactUnordered,
			},
			{
				// Select all with top.
				name: "select-all-with-top",
				sqls: sqls(
					"select top(2) * from keyed",
				),
				expHdrs: hdrs(
					hdr("_id", fldTypeString),
					hdr("an_int", fldTypeInt),
					hdr("an_id_set", fldTypeIDSet),
					hdr("an_id", fldTypeID),
					hdr("a_string", fldTypeString),
					hdr("a_string_set", fldTypeStringSet),
				),
				expRows: rows(
					row("one", int64(11), []int64{11, 12, 13}, int64(101), "str1", []string{"a1", "b1", "c1"}),
					row("two", int64(22), []int64{21, 22, 23}, int64(201), "str2", []string{"a2", "b2", "c2"}),
					row("three", int64(33), []int64{31, 32, 33}, int64(301), "str3", []string{"a3", "b3", "c3"}),
					row("four", int64(44), []int64{41, 42, 43}, int64(401), "str4", []string{"a4", "b4", "c4"}),
				),
				compare:     compareIncludedIn,
				expRowCount: 2,
			},
			{
				// Select all with where on int field.
				name: "select-all-with-where",
				sqls: sqls(
					"select * from keyed where an_int = 22",
					"select * from keyed where a_string = 'str2'",
					"select * from keyed where an_id = 201",
				),
				expHdrs: hdrs(
					hdr("_id", fldTypeString),
					hdr("an_int", fldTypeInt),
					hdr("an_id_set", fldTypeIDSet),
					hdr("an_id", fldTypeID),
					hdr("a_string", fldTypeString),
					hdr("a_string_set", fldTypeStringSet),
				),
				expRows: rows(
					row("two", int64(22), []int64{21, 22, 23}, int64(201), "str2", []string{"a2", "b2", "c2"}),
				),
				compare: compareExactUnordered,
			},
		},
	},

	setLiteralTests,
	setFunctionTests,
	setParameterTests,
	datePartTests,

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

	//joins
	joinTestsUsers,
	joinTestsOrders,
	joinTests,

	//time quantums
	// Skip for now - timeQuantumInsertTest,
}

var insertTest = tableTest{
	table: tbl(
		"testinsert",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("a", fldTypeInt, "min 0", "max 1000"),
			srcHdr("b", fldTypeInt, "min 0", "max 1000"),
			srcHdr("s", fldTypeString),
			srcHdr("bl", fldTypeBool),
			srcHdr("d", fldTypeDecimal2),
			srcHdr("event", fldTypeStringSet),
			srcHdr("ievent", fldTypeIDSet),
		),
		nil,
	),
	sqlTests: []sqlTest{
		{
			// Insert
			sqls: sqls(
				"insert into testinsert (_id, a, b, s, bl, d, event, ievent) values (4, 40, 400, 'foo', false, 10.12, ['A', 'B', 'C'], [1, 2, 3])",
			),
			expHdrs: hdrs(),
			expRows: rows(),
			compare: compareExactUnordered,
		},
		{
			// Insert with nulls
			sqls: sqls(
				"insert into testinsert (_id, a, b, s, bl, d, event, ievent) values (5, null, null, null, null, null, null, null)",
				"insert into testinsert (_id, a, b, s, bl, d, event, ievent) values (6, 1, null, null, null, null, null, null)",
			),
			expHdrs: hdrs(),
			expRows: rows(),
			compare: compareExactUnordered,
		},
		{
			// InsertBadTable
			sqls: sqls(
				"insert into ifoo (a, b) values (1, 2)",
			),
			expErr: "table 'ifoo' not found",
		},
		{
			// InsertBadColumn
			sqls: sqls(
				"insert into testinsert (c, b) values (1, 2)",
			),
			expErr: "column 'c' not found",
		},
		{
			// InsertDupeColumn
			sqls: sqls(
				"insert into testinsert (a, a, b) values (1, 2)",
			),
			expErr: "duplicate column 'a'",
		},
		{
			// InsertMismatchColumnValues
			sqls: sqls(
				"insert into testinsert (_id, a, b) values (1)",
			),
			expErr: "mismatch in the count of expressions and target columns",
		},
		{
			// InsertHandleMissingColumns
			sqls: sqls(
				"insert into testinsert values (4, 40, 400)",
			),
			expErr: "mismatch in the count of expressions and target columns",
		},
		{
			// InsertHandleMissingId
			sqls: sqls(
				"insert into testinsert (a, b) values (1, 2)",
			),
			expErr: "insert column list must have '_id' column specified",
		},
		{
			// InsertHandleMissingIdPlusOneOther
			sqls: sqls(
				"insert into testinsert (_id) values (1)",
			),
			expErr: "insert column list must have at least one non '_id' column specified",
		},
		{
			// InsertSetsTypeError
			sqls: sqls(
				"insert into testinsert (_id, a, event) values (4, 40, [101, 150])",
			),
			expErr: "an expression of type 'IDSET' cannot be assigned to type 'STRINGSET'",
		},
		{
			// InsertSetsTypeError2
			sqls: sqls(
				"insert into testinsert (_id, a, ievent) values (4, 40, ['POST', 'GET'])",
			),
			expErr: "an expression of type 'STRINGSET' cannot be assigned to type 'IDSET'",
		},
	},
}

var keyedInsertTest = tableTest{
	name: "keyedinsert",
	table: tbl(
		"testkeyedinsert",
		srcHdrs(
			srcHdr("_id", fldTypeString),
			srcHdr("a", fldTypeInt, "min 0", "max 1000"),
			srcHdr("b", fldTypeInt, "min 0", "max 1000"),
			srcHdr("s", fldTypeString),
			srcHdr("bl", fldTypeBool),
			srcHdr("d", fldTypeDecimal2),
			srcHdr("event", fldTypeStringSet),
			srcHdr("ievent", fldTypeIDSet),
		),
		nil,
	),
	sqlTests: []sqlTest{
		{
			// Insert
			sqls: sqls(
				"insert into testkeyedinsert (_id, a, b, s, bl, d, event, ievent) values ('four', 40, 400, 'foo', false, 10.12, ['A', 'B', 'C'], [1, 2, 3])",
			),
			expHdrs: hdrs(),
			expRows: rows(),
			compare: compareExactUnordered,
		},
	},
}

func knownTimestamp() time.Time {
	tm, err := time.ParseInLocation(time.RFC3339, "2012-11-01T22:08:41+00:00", time.UTC)
	if err != nil {
		panic(err.Error())
	}
	return tm
}

var timestampLiterals = tableTest{
	table: tbl(
		"testtimestampliterals",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("a", fldTypeInt, "min 0", "max 1000"),
			srcHdr("b", fldTypeInt, "min 0", "max 1000"),
			srcHdr("d", fldTypeDecimal2),
			srcHdr("ts", fldTypeTimestamp),
			srcHdr("event", fldTypeStringSet),
			srcHdr("ievent", fldTypeIDSet),
		),
		srcRows(),
	),
	sqlTests: []sqlTest{
		{
			// InsertWithCurrentTimestamp
			sqls: sqls(
				"insert into testtimestampliterals (_id, a, b, d, ts, event, ievent) values (4, 40, 400, 10.12, current_timestamp, ['A', 'B', 'C'], [1, 2, 3])",
			),
			expHdrs: hdrs(),
			expRows: rows(),
			compare: compareExactUnordered,
		},
		{
			// InsertWithCurrentDate
			sqls: sqls(
				"insert into testtimestampliterals (_id, a, b, d, ts, event, ievent) values (4, 40, 400, 10.12, current_date, ['A', 'B', 'C'], [1, 2, 3])",
			),
			expHdrs: hdrs(),
			expRows: rows(),
			compare: compareExactUnordered,
		},
	},
}
