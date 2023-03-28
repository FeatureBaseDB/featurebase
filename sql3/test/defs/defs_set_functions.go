package defs

// set literal tests
var setLiteralTests = TableTest{
	name: "selectwithsetliterals",
	Table: tbl(
		"selectwithsetliterals",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("a", fldTypeInt, "min 0", "max 1000"),
			srcHdr("b", fldTypeInt, "min 0", "max 1000"),
			srcHdr("event", fldTypeStringSet),
			srcHdr("ievent", fldTypeIDSet),
		),
		srcRows(
			srcRow(int64(1), int64(10), int64(100), []string{"POST"}, nil),
			srcRow(int64(2), int64(20), int64(200), []string{"GET"}, nil),
			srcRow(int64(3), int64(30), int64(300), []string{"GET", "POST"}, []int64{101}),
		),
	),
	SQLTests: []SQLTest{
		{
			// SetContainsSelectList
			name: "set-contains-select-list",
			SQLs: sqls(
				"select _id, setcontains(event, 'POST') from selectwithsetliterals",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeBool),
			),
			ExpRows: rows(
				row(int64(1), true),
				row(int64(2), false),
				row(int64(3), true),
			),
			Compare: CompareExactUnordered,
		},
		{
			// SetContainsAllSelectList
			name: "set-contains-all-select-list",
			SQLs: sqls(
				"select _id, setcontainsall(event, ['POST']) from selectwithsetliterals",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeBool),
			),
			ExpRows: rows(
				row(int64(1), true),
				row(int64(2), false),
				row(int64(3), true),
			),
			Compare: CompareExactUnordered,
		},
		{
			// SetContainsAnySelectList
			name: "set-contains-any-select-list",
			SQLs: sqls(
				"select _id, setcontainsany(event, ['POST', 'DELETE']) from selectwithsetliterals",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeBool),
			),
			ExpRows: rows(
				row(int64(1), true),
				row(int64(2), false),
				row(int64(3), true),
			),
			Compare: CompareExactUnordered,
		},
		{
			// SetContainsSelectListInt
			name: "set-contains-select-list-int",
			SQLs: sqls(
				"select _id, setcontains(ievent, 101) from selectwithsetliterals",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeBool),
			),
			ExpRows: rows(
				row(int64(1), nil),
				row(int64(2), nil),
				row(int64(3), true),
			),
			Compare: CompareExactUnordered,
		},
		{
			// SetContainsSelectAllListInt
			name: "set-contains-select-all-list-int",
			SQLs: sqls(
				"select _id, setcontainsall(ievent, [101]) from selectwithsetliterals",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeBool),
			),
			ExpRows: rows(
				row(int64(1), false),
				row(int64(2), false),
				row(int64(3), true),
			),
			Compare: CompareExactUnordered,
		},
		{
			// SetContainsSelectAnyListInt
			name: "set-contains-select-any-list-int",
			SQLs: sqls(
				"select _id, setcontainsany(ievent, [100, 101, 102]) from selectwithsetliterals",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeBool),
			),
			ExpRows: rows(
				row(int64(1), nil),
				row(int64(2), nil),
				row(int64(3), true),
			),
			Compare: CompareExactUnordered,
		},
		{
			// SetContainsWithLiteral
			// SetContainsWithLiteralInt
			// SetContainsWithLiteralAny
			// SetContainsWithLiteralAnyInt
			// SetContainsWithLiteralAll
			// SetContainsWithLiteralAllInt
			name: "set-contains-with-literal",
			SQLs: sqls(
				"select _id, setcontains(['POST'], 'POST') from selectwithsetliterals",
				"select _id, setcontains([101], 101) from selectwithsetliterals",
				"select _id, setcontainsany(['POST'], ['POST']) from selectwithsetliterals",
				"select _id, setcontainsany([101], [101]) from selectwithsetliterals",
				"select _id, setcontainsall(['POST'], ['POST']) from selectwithsetliterals",
				"select _id, setcontainsall([101], [101]) from selectwithsetliterals",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeBool),
			),
			ExpRows: rows(
				row(int64(1), true),
				row(int64(2), true),
				row(int64(3), true),
			),
			Compare: CompareExactUnordered,
		},
	},
}

// set function tests
var setFunctionTests = TableTest{
	name: "selectwithset",
	Table: tbl(
		"selectwithset",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("a", fldTypeInt, "min 0", "max 1000"),
			srcHdr("b", fldTypeInt, "min 0", "max 1000"),
			srcHdr("event", fldTypeStringSet),
			srcHdr("ievent", fldTypeIDSet),
		),
		srcRows(
			srcRow(int64(1), int64(10), int64(100), []string{"POST"}, []int64{101}),
			srcRow(int64(2), int64(20), int64(200), []string{"GET"}, []int64(nil)),
			srcRow(int64(3), int64(30), int64(300), []string{"GET", "POST"}, []int64(nil)),
		),
	),
	SQLTests: []SQLTest{
		{
			name: "set-contains",
			SQLs: sqls(
				"select * from selectwithset where setcontains(event, 'POST')",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("a", fldTypeInt),
				hdr("b", fldTypeInt),
				hdr("event", fldTypeStringSet),
				hdr("ievent", fldTypeIDSet),
			),
			ExpRows: rows(
				row(int64(1), int64(10), int64(100), []string{"POST"}, []int64{101}),
				row(int64(3), int64(30), int64(300), []string{"GET", "POST"}, nil),
			),
			Compare:        CompareExactUnordered,
			SortStringKeys: true,
		},
		{
			// SetContains

			SQLs: sqls(
				"select * from selectwithset where setcontains(event, 'POST')",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("a", fldTypeInt),
				hdr("b", fldTypeInt),
				hdr("event", fldTypeStringSet),
				hdr("ievent", fldTypeIDSet),
			),
			ExpRows: rows(
				row(int64(1), int64(10), int64(100), []string{"POST"}, []int64{101}),
				row(int64(3), int64(30), int64(300), []string{"GET", "POST"}, nil),
			),
			Compare:        CompareExactUnordered,
			SortStringKeys: true,
		},
		{
			// SetContainsInt
			name: "set-contains-int",
			SQLs: sqls(
				"select * from selectwithset where setcontains(ievent, 101)",
				"select * from selectwithset where setcontainsany(ievent, [101])",
				"select * from selectwithset where setcontainsall(ievent, [101])",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("a", fldTypeInt),
				hdr("b", fldTypeInt),
				hdr("event", fldTypeStringSet),
				hdr("ievent", fldTypeIDSet),
			),
			ExpRows: rows(
				row(int64(1), int64(10), int64(100), []string{"POST"}, []int64{101}),
			),
			Compare: CompareExactUnordered,
		},
		{
			// SetContainsInt
			name: "set-contains-int-using-value",
			SQLs: sqls(
				"select _id, setcontainsany(ievent, [101]) from selectwithset",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeBool),
			),
			ExpRows: rows(
				row(int64(1), true),
				row(int64(2), nil),
				row(int64(3), nil),
			),
			Compare: CompareExactUnordered,
		},
		{
			// SetContainsOrSetContains
			// SetContainsAny
			name: "set-contains-or-set-contains",
			SQLs: sqls(
				"select * from selectwithset where setcontains(event, 'POST') or setcontains(event, 'GET')",
				"select * from selectwithset where setcontainsany(event, ['POST', 'GET'])",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("a", fldTypeInt),
				hdr("b", fldTypeInt),
				hdr("event", fldTypeStringSet),
				hdr("ievent", fldTypeIDSet),
			),
			ExpRows: rows(
				row(int64(1), int64(10), int64(100), []string{"POST"}, []int64{101}),
				row(int64(2), int64(20), int64(200), []string{"GET"}, nil),
				row(int64(3), int64(30), int64(300), []string{"GET", "POST"}, nil),
			),
			Compare:        CompareExactUnordered,
			SortStringKeys: true,
		},
		{
			// SetContainsAndSetContains
			// SetContainsAll
			name: "set-contains-and-set-contains",
			SQLs: sqls(
				"select * from selectwithset where setcontains(event, 'POST') and setcontains(event, 'GET')",
				"select * from selectwithset where setcontainsall(event, ['POST', 'GET'])",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("a", fldTypeInt),
				hdr("b", fldTypeInt),
				hdr("event", fldTypeStringSet),
				hdr("ievent", fldTypeIDSet),
			),
			ExpRows: rows(
				row(int64(3), int64(30), int64(300), []string{"GET", "POST"}, nil),
			),
			Compare:        CompareExactUnordered,
			SortStringKeys: true,
		},
		{
			// SetContainsWrongType
			name: "set-contains-wrong-type",
			SQLs: sqls(
				"select * from selectwithset where setcontains(event, 1)",
			),
			ExpErr: "types 'stringset' and 'int' are not equatable",
		},
		{
			// SetContainsWrongTypeInt
			name: "set-contains-wrong-type-int",
			SQLs: sqls(
				"select * from selectwithset where setcontains(ievent, 'foo')",
			),
			ExpErr: "types 'idset' and 'string' are not equatable",
		},
		{
			// SetContainsWrongTypeSet
			name: "set-contains-wrong-type-set",
			SQLs: sqls(
				"select * from selectwithset where setcontains(event, ['foo'])",
			),
			ExpErr: "types 'stringset' and 'stringset' are not equatable",
		},
		{
			// SetContainsWrongTypeSet
			name: "set-contains-null-in-values",
			SQLs: sqls(
				"select * from selectwithset where setcontains(event, [null])",
			),
			ExpErr: "set literal must contain ints or strings",
		},
		{
			// SetContainsWrongTypeSet
			name: "set-contains-null-value",
			SQLs: sqls(
				"select * from selectwithset where setcontains(event, null)",
			),
			ExpErr: "types 'stringset' and 'void' are not equatable",
		},
		{
			// SetContainsWrongTypeSet
			name: "set-contains-null-set",
			SQLs: sqls(
				"select * from selectwithset where setcontains(null, [1])",
			),
			ExpErr: "set expression expected",
		},
	},
}

// set parameter tests
var setParameterTests = TableTest{

	Table: tbl(
		"selectwithsetparams",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("a", fldTypeInt, "min 0", "max 1000"),
			srcHdr("b", fldTypeInt, "min 0", "max 1000"),
			srcHdr("event", fldTypeStringSet),
			srcHdr("ievent", fldTypeIDSet),
		),
		srcRows(
			srcRow(int64(1), int64(10), int64(100), []string{"POST"}, []int64{101}),
			srcRow(int64(2), int64(20), int64(200), []string{"GET"}, nil),
			srcRow(int64(3), int64(30), int64(300), []string{"GET", "POST"}, nil),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select setcontains(['POST', 'GET'])",
			),
			ExpErr: "count of formal parameters (2) does not match count of actual parameters (1)",
		},
		{
			SQLs: sqls(
				"select setcontains(1, 2)",
			),
			ExpErr: "set expression expected",
		},
		{
			SQLs: sqls(
				"select setcontains(['POST', 'GET'], 1)",
			),
			ExpErr: "types 'stringset' and 'int' are not equatable",
		},
		{
			SQLs: sqls(
				"select setcontains([1, 2], '1')",
			),
			ExpErr: "types 'idset' and 'string' are not equatable",
		},

		{
			SQLs: sqls(
				"select setcontainsall(['POST', 'GET'])",
				"select setcontainsany(['POST', 'GET'])",
			),
			ExpErr: "count of formal parameters (2) does not match count of actual parameters (1)",
		},
		{
			SQLs: sqls(
				"select setcontainsall(1, 2)",
				"select setcontainsany(1, 2)",
			),
			ExpErr: "set expression expected",
		},
		{
			SQLs: sqls(
				"select setcontainsall(['POST', 'GET'], [1, 2])",
				"select setcontainsany(['POST', 'GET'], [1, 2])",
			),
			ExpErr: "types 'string' and 'id' are not equatable",
		},
		{
			SQLs: sqls(
				"select setcontainsall([1, 2], ['1', '2'])",
				"select setcontainsany([1, 2], ['1', '2'])",
			),
			ExpErr: "types 'id' and 'string' are not equatable",
		},
	},
}

// test IDSetQ and StringSetQ functions
var setTimeQuantumTests = TableTest{
	name: "selectwithsetqliterals",
	Table: tbl(
		"selectwithsetqliterals",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("a", fldTypeInt, "min 0", "max 1000"),
			srcHdr("b", fldTypeInt, "min 0", "max 1000"),
			srcHdr("ssq1", fldTypeStringSetQ, "timequantum 'YMD'"),
			srcHdr("isq1", fldTypeIDSetQ, "timequantum 'YMD'"),
		),
	),
	SQLTests: []SQLTest{
		// can't find example syntax or docs for how to put time quantum fields in with srcRow()
		// so i'm inserting them with SQL statements.
		{
			SQLs: sqls(
				"insert into selectwithsetqliterals(_id, a, b, ssq1, isq1) values (1, 10, 100, {'2022-01-03T00:00:00Z', ['foo']}, {'2022-01-01T00:00:00Z', [99, 101]})",
				"insert into selectwithsetqliterals(_id, a, b, ssq1, isq1) values (2, 20, 200, {'2022-01-03T00:00:00Z', ['bar']}, {'2022-01-01T00:00:00Z', [100]})",
				"insert into selectwithsetqliterals(_id, a, b, ssq1, isq1) values (3, 30, 300, {'2022-01-03T00:00:00Z', ['foo', 'bar']}, {'2022-01-01T00:00:00Z', [101]})",
			),
			ExpHdrs: hdrs(),
			ExpRows: rows(),
			Compare: CompareExactUnordered,
		},
		{
			// SetContainsSelectList
			name: "set-contains-select-list",
			SQLs: sqls(
				"select _id, setcontains(ssq1, 'bar') from selectwithsetqliterals",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeBool),
			),
			ExpRows: rows(
				row(int64(1), false),
				row(int64(2), true),
				row(int64(3), true),
			),
			Compare: CompareExactUnordered,
		},
		{
			// SetContainsSelectListInt
			name: "set-contains-select-list-int",
			SQLs: sqls(
				"select _id, setcontains(isq1, 101) from selectwithsetqliterals",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeBool),
			),
			ExpRows: rows(
				row(int64(1), true),
				row(int64(2), false),
				row(int64(3), true),
			),
			Compare: CompareExactUnordered,
		},
		{
			// SetContainsWithLiteral
			// SetContainsWithLiteralInt
			// SetContainsWithLiteralAny
			// SetContainsWithLiteralAnyInt
			// SetContainsWithLiteralAll
			// SetContainsWithLiteralAllInt
			name: "set-contains-with-literal",
			SQLs: sqls(
				"select _id, setcontains(['foo'], 'foo') from selectwithsetqliterals",
				"select _id, setcontains([101], 101) from selectwithsetqliterals",
				"select _id, setcontainsany(['foo'], ['foo']) from selectwithsetqliterals",
				"select _id, setcontainsany([101], [101]) from selectwithsetqliterals",
				"select _id, setcontainsall(['foo'], ['foo']) from selectwithsetqliterals",
				"select _id, setcontainsall([101], [101]) from selectwithsetqliterals",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeBool),
			),
			ExpRows: rows(
				row(int64(1), true),
				row(int64(2), true),
				row(int64(3), true),
			),
			Compare: CompareExactUnordered,
		},
	},
}
