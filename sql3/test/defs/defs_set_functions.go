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
			srcRow(int64(2), int64(20), int64(200), []string{"GET"}, nil),
			srcRow(int64(3), int64(30), int64(300), []string{"GET", "POST"}, nil),
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
