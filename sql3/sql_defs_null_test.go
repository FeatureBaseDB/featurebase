package sql3_test

// NULL tests
var nullTests = tableTest{
	table: tbl(
		"null_all_types",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("i", fldTypeInt, "min 0", "max 1000"),
			srcHdr("i1", fldTypeInt, "min 0", "max 1000"),
			srcHdr("b1", fldTypeBool),
			srcHdr("d1", fldTypeDecimal2),
			srcHdr("id1", fldTypeID),
			srcHdr("ids1", fldTypeIDSet),
			srcHdr("s1", fldTypeString),
			srcHdr("ss1", fldTypeStringSet),
			srcHdr("t1", fldTypeTimestamp),
		),
		srcRows(
			srcRow(int64(1), int64(1), nil, nil, nil, nil, nil, nil, nil, nil),
		),
	),
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select _id is null from null_all_types",
			),
			expHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			expRows: rows(
				row(bool(false)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select i is null from null_all_types",
			),
			expHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			expRows: rows(
				row(bool(false)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select i1 is null from null_all_types",
			),
			expHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			expRows: rows(
				row(bool(true)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select b1 is null from null_all_types",
			),
			expHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			expRows: rows(
				row(bool(true)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select d1 is null from null_all_types",
			),
			expHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			expRows: rows(
				row(bool(true)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select id1 is null from null_all_types",
			),
			expHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			expRows: rows(
				row(bool(true)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select ids1 is null from null_all_types",
			),
			expHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			expRows: rows(
				row(bool(true)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select s1 is null from null_all_types",
			),
			expHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			expRows: rows(
				row(bool(true)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select ss1 is null from null_all_types",
			),
			expHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			expRows: rows(
				row(bool(true)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select t1 is null from null_all_types",
			),
			expHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			expRows: rows(
				row(bool(true)),
			),
			compare: compareExactUnordered,
		},
	},
}

// NOT NULL tests
var notNullTests = tableTest{
	table: tbl(
		"not_null_all_types",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("i", fldTypeInt, "min 0", "max 1000"),
			srcHdr("i1", fldTypeInt, "min 0", "max 1000"),
			srcHdr("b1", fldTypeBool),
			srcHdr("d1", fldTypeDecimal2),
			srcHdr("id1", fldTypeID),
			srcHdr("ids1", fldTypeIDSet),
			srcHdr("s1", fldTypeString),
			srcHdr("ss1", fldTypeStringSet),
			srcHdr("t1", fldTypeTimestamp),
		),
		srcRows(
			srcRow(int64(1), int64(1), nil, nil, nil, nil, nil, nil, nil, nil),
		),
	),
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select _id is not null from not_null_all_types",
			),
			expHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			expRows: rows(
				row(bool(true)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select i1 is not null from not_null_all_types",
			),
			expHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			expRows: rows(
				row(bool(false)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select b1 is not null from not_null_all_types",
			),
			expHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			expRows: rows(
				row(bool(false)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select d1 is not null from not_null_all_types",
			),
			expHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			expRows: rows(
				row(bool(false)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select id1 is not null from not_null_all_types",
			),
			expHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			expRows: rows(
				row(bool(false)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select ids1 is not null from not_null_all_types",
			),
			expHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			expRows: rows(
				row(bool(false)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select s1 is not null from not_null_all_types",
			),
			expHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			expRows: rows(
				row(bool(false)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select ss1 is not null from not_null_all_types",
			),
			expHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			expRows: rows(
				row(bool(false)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select t1 is not null from not_null_all_types",
			),
			expHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			expRows: rows(
				row(bool(false)),
			),
			compare: compareExactUnordered,
		},
	},
}

// NULL filter condition tests
var nullFilterTests = tableTest{
	table: tbl(
		"null_filter_all_types",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("i", fldTypeInt, "min 0", "max 1000"),
			srcHdr("i1", fldTypeInt, "min 0", "max 1000"),
			srcHdr("b1", fldTypeBool),
			srcHdr("d1", fldTypeDecimal2),
			srcHdr("id1", fldTypeID),
			srcHdr("ids1", fldTypeIDSet),
			srcHdr("s1", fldTypeString),
			srcHdr("ss1", fldTypeStringSet),
			srcHdr("t1", fldTypeTimestamp),
		),
		srcRows(
			srcRow(int64(1), int64(1), nil, nil, nil, nil, nil, nil, nil, nil),
			srcRow(int64(2), int64(1), int64(10), bool(true), float64(10), int64(20), []int64{101}, string("foo"), []string{"GET", "POST"}, knownTimestamp()),
		),
	),
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select _id from null_filter_all_types where _id is null",
			),
			expErr: "'_id' column cannot be used in a is/is not null filter expression",
		},
		{
			sqls: sqls(
				"select _id from null_filter_all_types where _id is not null",
			),
			expErr: "'_id' column cannot be used in a is/is not null filter expression",
		},
		{
			sqls: sqls(
				"select _id from null_filter_all_types where i1 is null",
			),
			expHdrs: hdrs(
				hdr("_id", fldTypeID),
			),
			expRows: rows(
				row(int64(1)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select _id from null_filter_all_types where i1 is not null",
			),
			expHdrs: hdrs(
				hdr("_id", fldTypeID),
			),
			expRows: rows(
				row(int64(2)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select _id from null_filter_all_types where b1 is null",
			),
			expErr: "unsupported type 'BOOL' for is/is not null filter expression",
		},
		{
			sqls: sqls(
				"select _id from null_filter_all_types where b1 is not null",
			),
			expErr: "unsupported type 'BOOL' for is/is not null filter expression",
		},
		{
			sqls: sqls(
				"select _id from null_filter_all_types where d1 is null",
			),
			expHdrs: hdrs(
				hdr("_id", fldTypeID),
			),
			expRows: rows(
				row(int64(1)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select _id from null_filter_all_types where d1 is not null",
			),
			expHdrs: hdrs(
				hdr("_id", fldTypeID),
			),
			expRows: rows(
				row(int64(2)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select _id from null_filter_all_types where id1 is null",
			),
			expErr: "unsupported type 'ID' for is/is not null filter expression",
		},
		{
			sqls: sqls(
				"select _id from null_filter_all_types where id1 is not null",
			),
			expErr: "unsupported type 'ID' for is/is not null filter expression",
		},
		{
			sqls: sqls(
				"select _id from null_filter_all_types where ids1 is null",
			),
			expErr: "unsupported type 'IDSET' for is/is not null filter expression",
		},
		{
			sqls: sqls(
				"select _id from null_filter_all_types where ids1 is not null",
			),
			expErr: "unsupported type 'IDSET' for is/is not null filter expression",
		},
		{
			sqls: sqls(
				"select _id from null_filter_all_types where s1 is null",
			),
			expErr: "unsupported type 'STRING' for is/is not null filter expression",
		},
		{
			sqls: sqls(
				"select _id from null_filter_all_types where s1 is not null",
			),
			expErr: "unsupported type 'STRING' for is/is not null filter expression",
		},
		{
			sqls: sqls(
				"select _id from null_filter_all_types where ss1 is null",
			),
			expErr: "unsupported type 'STRINGSET' for is/is not null filter expression",
		},
		{
			sqls: sqls(
				"select _id from null_filter_all_types where ss1 is not null",
			),
			expErr: "unsupported type 'STRINGSET' for is/is not null filter expression",
		},
		{
			sqls: sqls(
				"select _id from null_filter_all_types where t1 is null",
			),
			expHdrs: hdrs(
				hdr("_id", fldTypeID),
			),
			expRows: rows(
				row(int64(1)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select _id from null_filter_all_types where t1 is not null",
			),
			expHdrs: hdrs(
				hdr("_id", fldTypeID),
			),
			expRows: rows(
				row(int64(2)),
			),
			compare: compareExactUnordered,
		},
	},
}
