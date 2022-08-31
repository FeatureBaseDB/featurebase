package sql3_test

//NULL tests
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

//NOT NULL tests
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
