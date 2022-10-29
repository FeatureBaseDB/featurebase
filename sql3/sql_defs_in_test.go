package sql3_test

// IN tests
var inTests = tableTest{
	table: tbl(
		"in_all_types",
		srcHdrs(
			srcHdr("_id", fldTypeID),
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
			srcRow(int64(1), int64(1000), bool(true), float64(12.34), int64(20), []int64{101, 102}, string("foo"), []string{"101", "102"}, knownTimestamp()),
		),
	),
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select _id in (1, 10) from in_all_types",
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
				"select i1 in (1, 1000) from in_all_types",
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
				"select b1 in (true, false) from in_all_types",
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
				"select d1 in (1.23, 4.56) from in_all_types",
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
				"select id1 in (3, 7) from in_all_types",
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
				"select ids1 in ([101, 102], [456, 789]) from in_all_types",
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
				"select s1 in ('foo', 'bar') from in_all_types",
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
				"select ss1 in (['a', 'b'], ['101', '102']) from in_all_types",
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
				"select t1 in ('2010-11-01T22:08:41+00:00', '2013-11-01T22:08:41+00:00', '2012-11-01T22:08:41+00:00') from in_all_types",
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

// NOT IN tests
var notInTests = tableTest{
	table: tbl(
		"not_in_all_types",
		srcHdrs(
			srcHdr("_id", fldTypeID),
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
			srcRow(int64(1), int64(1000), bool(true), float64(12.34), int64(20), []int64{101, 102}, string("foo"), []string{"101", "102"}, knownTimestamp()),
		),
	),
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select _id not in (1, 10) from not_in_all_types",
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
				"select i1 not in (1, 1000) from not_in_all_types",
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
				"select b1 not in (true, false) from not_in_all_types",
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
				"select d1 not in (1.23, 4.56) from not_in_all_types",
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
				"select id1 not in (3, 7) from not_in_all_types",
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
				"select ids1 not in ([101, 102], [456, 789]) from not_in_all_types",
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
				"select s1 not in ('foo', 'bar') from not_in_all_types",
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
				"select ss1 not in (['a', 'b'], ['101', '102']) from not_in_all_types",
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
				"select t1 not in ('2010-11-01T22:08:41+00:00', '2013-11-01T22:08:41+00:00', '2012-11-01T22:08:41+00:00') from not_in_all_types",
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
