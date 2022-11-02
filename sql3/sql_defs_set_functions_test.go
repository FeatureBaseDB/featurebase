package sql3_test

// set literal tests
var setLiteralTests = tableTest{
	name: "selectwithsetliterals",
	table: tbl(
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
	sqlTests: []sqlTest{
		{
			// SetContainsSelectList
			name: "set-contains-select-list",
			sqls: sqls(
				"select _id, setcontains(event, 'POST') from selectwithsetliterals",
			),
			expHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeBool),
			),
			expRows: rows(
				row(int64(1), true),
				row(int64(2), false),
				row(int64(3), true),
			),
			compare: compareExactUnordered,
		},
		{
			// SetContainsSelectListInt
			name: "set-contains-select-list-int",
			sqls: sqls(
				"select _id, setcontains(ievent, 101) from selectwithsetliterals",
			),
			expHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeBool),
			),
			expRows: rows(
				row(int64(1), nil),
				row(int64(2), nil),
				row(int64(3), true),
			),
			compare: compareExactUnordered,
		},
		{
			// SetContainsWithLiteral
			// SetContainsWithLiteralInt
			// SetContainsWithLiteralAny
			// SetContainsWithLiteralAnyInt
			// SetContainsWithLiteralAll
			// SetContainsWithLiteralAllInt
			name: "set-contains-with-literal",
			sqls: sqls(
				"select _id, setcontains(['POST'], 'POST') from selectwithsetliterals",
				"select _id, setcontains([101], 101) from selectwithsetliterals",
				"select _id, setcontainsany(['POST'], ['POST']) from selectwithsetliterals",
				"select _id, setcontainsany([101], [101]) from selectwithsetliterals",
				"select _id, setcontainsall(['POST'], ['POST']) from selectwithsetliterals",
				"select _id, setcontainsall([101], [101]) from selectwithsetliterals",
			),
			expHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeBool),
			),
			expRows: rows(
				row(int64(1), true),
				row(int64(2), true),
				row(int64(3), true),
			),
			compare: compareExactUnordered,
		},
	},
}

// set function tests
var setFunctionTests = tableTest{
	name: "selectwithset",
	table: tbl(
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
			srcRow(int64(2), int64(20), int64(200), []string{"GET"}, nil),
			srcRow(int64(3), int64(30), int64(300), []string{"GET", "POST"}, nil),
		),
	),
	sqlTests: []sqlTest{
		{
			name: "set-contains",
			sqls: sqls(
				"select * from selectwithset where setcontains(event, 'POST')",
			),
			expHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("a", fldTypeInt),
				hdr("b", fldTypeInt),
				hdr("event", fldTypeStringSet),
				hdr("ievent", fldTypeIDSet),
			),
			expRows: rows(
				row(int64(1), int64(10), int64(100), []string{"POST"}, []int64{101}),
				row(int64(3), int64(30), int64(300), []string{"GET", "POST"}, nil),
			),
			compare:        compareExactUnordered,
			sortStringKeys: true,
		},
		{
			// SetContains

			sqls: sqls(
				"select * from selectwithset where setcontains(event, 'POST')",
			),
			expHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("a", fldTypeInt),
				hdr("b", fldTypeInt),
				hdr("event", fldTypeStringSet),
				hdr("ievent", fldTypeIDSet),
			),
			expRows: rows(
				row(int64(1), int64(10), int64(100), []string{"POST"}, []int64{101}),
				row(int64(3), int64(30), int64(300), []string{"GET", "POST"}, nil),
			),
			compare:        compareExactUnordered,
			sortStringKeys: true,
		},
		{
			// SetContainsInt
			name: "set-contains-int",
			sqls: sqls(
				"select * from selectwithset where setcontains(ievent, 101)",
			),
			expHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("a", fldTypeInt),
				hdr("b", fldTypeInt),
				hdr("event", fldTypeStringSet),
				hdr("ievent", fldTypeIDSet),
			),
			expRows: rows(
				row(int64(1), int64(10), int64(100), []string{"POST"}, []int64{101}),
			),
			compare: compareExactUnordered,
		},
		{
			// SetContainsOrSetContains
			// SetContainsAny
			name: "set-contains-or-set-contains",
			sqls: sqls(
				"select * from selectwithset where setcontains(event, 'POST') or setcontains(event, 'GET')",
				"select * from selectwithset where setcontainsany(event, ['POST', 'GET'])",
			),
			expHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("a", fldTypeInt),
				hdr("b", fldTypeInt),
				hdr("event", fldTypeStringSet),
				hdr("ievent", fldTypeIDSet),
			),
			expRows: rows(
				row(int64(1), int64(10), int64(100), []string{"POST"}, []int64{101}),
				row(int64(2), int64(20), int64(200), []string{"GET"}, nil),
				row(int64(3), int64(30), int64(300), []string{"GET", "POST"}, nil),
			),
			compare:        compareExactUnordered,
			sortStringKeys: true,
		},
		{
			// SetContainsAndSetContains
			// SetContainsAll
			name: "set-contains-and-set-contains",
			sqls: sqls(
				"select * from selectwithset where setcontains(event, 'POST') and setcontains(event, 'GET')",
				"select * from selectwithset where setcontainsall(event, ['POST', 'GET'])",
			),
			expHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("a", fldTypeInt),
				hdr("b", fldTypeInt),
				hdr("event", fldTypeStringSet),
				hdr("ievent", fldTypeIDSet),
			),
			expRows: rows(
				row(int64(3), int64(30), int64(300), []string{"GET", "POST"}, nil),
			),
			compare:        compareExactUnordered,
			sortStringKeys: true,
		},
		{
			// SetContainsWrongType
			name: "set-contains-wrong-type",
			sqls: sqls(
				"select * from selectwithset where setcontains(event, 1)",
			),
			expErr: "types 'STRINGSET' and 'INT' are not equatable",
		},
		{
			// SetContainsWrongTypeInt
			name: "set-contains-wrong-type-int",
			sqls: sqls(
				"select * from selectwithset where setcontains(ievent, 'foo')",
			),
			expErr: "types 'IDSET' and 'STRING' are not equatable",
		},
		{
			// SetContainsWrongTypeSet
			name: "set-contains-wrong-type-set",
			sqls: sqls(
				"select * from selectwithset where setcontains(event, ['foo'])",
			),
			expErr: "types 'STRINGSET' and 'STRINGSET' are not equatable",
		},
	},
}

// set parameter tests
var setParameterTests = tableTest{

	table: tbl(
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
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select setcontains(['POST', 'GET'])",
			),
			expErr: "count of formal parameters (2) does not match count of actual parameters (1)",
		},
		{
			sqls: sqls(
				"select setcontains(1, 2)",
			),
			expErr: "set expression expected",
		},
		{
			sqls: sqls(
				"select setcontains(['POST', 'GET'], 1)",
			),
			expErr: "types 'STRINGSET' and 'INT' are not equatable",
		},
		{
			sqls: sqls(
				"select setcontains([1, 2], '1')",
			),
			expErr: "types 'IDSET' and 'STRING' are not equatable",
		},

		{
			sqls: sqls(
				"select setcontainsall(['POST', 'GET'])",
				"select setcontainsany(['POST', 'GET'])",
			),
			expErr: "count of formal parameters (2) does not match count of actual parameters (1)",
		},
		{
			sqls: sqls(
				"select setcontainsall(1, 2)",
				"select setcontainsany(1, 2)",
			),
			expErr: "set expression expected",
		},
		{
			sqls: sqls(
				"select setcontainsall(['POST', 'GET'], [1, 2])",
				"select setcontainsany(['POST', 'GET'], [1, 2])",
			),
			expErr: "types 'STRING' and 'ID' are not equatable",
		},
		{
			sqls: sqls(
				"select setcontainsall([1, 2], ['1', '2'])",
				"select setcontainsany([1, 2], ['1', '2'])",
			),
			expErr: "types 'ID' and 'STRING' are not equatable",
		},
	},
}
