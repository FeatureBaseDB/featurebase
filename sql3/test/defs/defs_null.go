package defs

// NULL tests
var nullTests = TableTest{
	Table: tbl(
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
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select _id is null from null_all_types",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			ExpRows: rows(
				row(bool(false)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select i is null from null_all_types",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			ExpRows: rows(
				row(bool(false)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select i1 is null from null_all_types",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			ExpRows: rows(
				row(bool(true)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select b1 is null from null_all_types",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			ExpRows: rows(
				row(bool(true)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select d1 is null from null_all_types",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			ExpRows: rows(
				row(bool(true)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select id1 is null from null_all_types",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			ExpRows: rows(
				row(bool(true)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select ids1 is null from null_all_types",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			ExpRows: rows(
				row(bool(true)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select s1 is null from null_all_types",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			ExpRows: rows(
				row(bool(true)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select ss1 is null from null_all_types",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			ExpRows: rows(
				row(bool(true)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select t1 is null from null_all_types",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			ExpRows: rows(
				row(bool(true)),
			),
			Compare: CompareExactUnordered,
		},
	},
}

// NOT NULL tests
var notNullTests = TableTest{
	Table: tbl(
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
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select _id is not null from not_null_all_types",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			ExpRows: rows(
				row(bool(true)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select i1 is not null from not_null_all_types",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			ExpRows: rows(
				row(bool(false)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select b1 is not null from not_null_all_types",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			ExpRows: rows(
				row(bool(false)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select d1 is not null from not_null_all_types",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			ExpRows: rows(
				row(bool(false)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select id1 is not null from not_null_all_types",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			ExpRows: rows(
				row(bool(false)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select ids1 is not null from not_null_all_types",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			ExpRows: rows(
				row(bool(false)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select s1 is not null from not_null_all_types",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			ExpRows: rows(
				row(bool(false)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select ss1 is not null from not_null_all_types",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			ExpRows: rows(
				row(bool(false)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select t1 is not null from not_null_all_types",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			ExpRows: rows(
				row(bool(false)),
			),
			Compare: CompareExactUnordered,
		},
	},
}

// NULL filter condition tests
var nullFilterTests = TableTest{
	Table: tbl(
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
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select _id from null_filter_all_types where _id is null",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
			),
			ExpRows: rows(),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id from null_filter_all_types where _id is not null",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
			),
			ExpRows: rows(
				row(int64(1)),
				row(int64(2)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id from null_filter_all_types where i1 is null",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
			),
			ExpRows: rows(
				row(int64(1)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id from null_filter_all_types where i1 is not null",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
			),
			ExpRows: rows(
				row(int64(2)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id from null_filter_all_types where b1 is null",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
			),
			ExpRows: rows(
				row(int64(1)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id from null_filter_all_types where b1 is not null",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
			),
			ExpRows: rows(
				row(int64(2)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id from null_filter_all_types where d1 is null",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
			),
			ExpRows: rows(
				row(int64(1)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id from null_filter_all_types where d1 is not null",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
			),
			ExpRows: rows(
				row(int64(2)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id from null_filter_all_types where id1 is null",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
			),
			ExpRows: rows(
				row(int64(1)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id from null_filter_all_types where id1 is not null",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
			),
			ExpRows: rows(
				row(int64(2)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id from null_filter_all_types where ids1 is null",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
			),
			ExpRows: rows(
				row(int64(1)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id from null_filter_all_types where ids1 is not null",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
			),
			ExpRows: rows(
				row(int64(2)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id from null_filter_all_types where s1 is null",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
			),
			ExpRows: rows(
				row(int64(1)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id from null_filter_all_types where s1 is not null",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
			),
			ExpRows: rows(
				row(int64(2)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id from null_filter_all_types where ss1 is null",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
			),
			ExpRows: rows(
				row(int64(1)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id from null_filter_all_types where ss1 is not null",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
			),
			ExpRows: rows(
				row(int64(2)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id from null_filter_all_types where t1 is null",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
			),
			ExpRows: rows(
				row(int64(1)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id from null_filter_all_types where t1 is not null",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
			),
			ExpRows: rows(
				row(int64(2)),
			),
			Compare: CompareExactUnordered,
		},
	},
}
