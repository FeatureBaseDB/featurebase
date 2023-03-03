package defs

var filterPredicates = TableTest{
	Table: tbl(
		"filter_predicates",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("i1", fldTypeInt),
			srcHdr("b1", fldTypeBool),
			srcHdr("id1", fldTypeID),
			srcHdr("ids1", fldTypeIDSet),
			srcHdr("d1", fldTypeDecimal2),
			srcHdr("s1", fldTypeString),
			srcHdr("ss1", fldTypeStringSet),
			srcHdr("ts1", fldTypeTimestamp),
		),
		srcRows(
			srcRow(int64(1), int64(10), bool(false), int64(1), []int64{10, 20, 30}, float64(10.00), string("10"), []string{"10", "20", "30"}, string("2001-11-01T22:08:41+00:00")),
			srcRow(int64(2), int64(20), bool(true), int64(2), []int64{11, 21, 31}, float64(20.00), string("20"), []string{"11", "21", "31"}, string("2002-11-01T22:08:41+00:00")),
			srcRow(int64(3), int64(30), bool(false), int64(3), []int64{12, 22, 32}, float64(30.00), string("30"), []string{"12", "22", "32"}, string("2003-11-01T22:08:41+00:00")),
			srcRow(int64(4), int64(40), bool(false), int64(4), []int64{10, 20, 30}, float64(40.00), string("40"), []string{"10", "20", "30"}, string("2004-11-01T22:08:41+00:00")),
			srcRow(int64(5), int64(50), bool(true), int64(5), []int64{11, 21, 31}, float64(50.00), string("50"), []string{"11", "21", "31"}, string("2005-11-01T22:08:41+00:00")),
			srcRow(int64(6), int64(60), bool(false), int64(6), []int64{12, 22, 32}, float64(60.00), string("60"), []string{"12", "22", "32"}, string("2006-11-01T22:08:41+00:00")),
		),
	),
}

var filterPredicatesIdKey = TableTest{
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select _id from filter_predicates where _id != 1",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
			),
			ExpRows: rows(
				row(int64(2)),
				row(int64(3)),
				row(int64(4)),
				row(int64(5)),
				row(int64(6)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id from filter_predicates where _id = 1",
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
				"select _id from filter_predicates where _id > 5",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
			),
			ExpRows: rows(
				row(int64(6)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id from filter_predicates where _id >= 5",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
			),
			ExpRows: rows(
				row(int64(5)),
				row(int64(6)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id from filter_predicates where _id < 2",
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
				"select _id from filter_predicates where _id <= 2",
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
	},
}

var filterPredicatesId = TableTest{
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select _id from filter_predicates where id1 != 1",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
			),
			ExpRows: rows(
				row(int64(2)),
				row(int64(3)),
				row(int64(4)),
				row(int64(5)),
				row(int64(6)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id from filter_predicates where id1 = 1",
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
				"select _id from filter_predicates where id1 > 5",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
			),
			ExpRows: rows(
				row(int64(6)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id from filter_predicates where id1 >= 5",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
			),
			ExpRows: rows(
				row(int64(5)),
				row(int64(6)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id from filter_predicates where id1 < 2",
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
				"select _id from filter_predicates where id1 <= 2",
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
	},
}

var filterPredicatesInt = TableTest{
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select _id from filter_predicates where i1 != 10",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
			),
			ExpRows: rows(
				row(int64(2)),
				row(int64(3)),
				row(int64(4)),
				row(int64(5)),
				row(int64(6)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id from filter_predicates where i1 = 10",
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
				"select _id from filter_predicates where i1 > 50",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
			),
			ExpRows: rows(
				row(int64(6)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id from filter_predicates where i1 >= 50",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
			),
			ExpRows: rows(
				row(int64(5)),
				row(int64(6)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id from filter_predicates where i1 < 20",
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
				"select _id from filter_predicates where i1 <= 20",
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
	},
}

var filterPredicatesBool = TableTest{
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select _id from filter_predicates where b1 != true",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
			),
			ExpRows: rows(
				row(int64(1)),
				row(int64(3)),
				row(int64(4)),
				row(int64(6)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id from filter_predicates where b1 = true",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
			),
			ExpRows: rows(
				row(int64(2)),
				row(int64(5)),
			),
			Compare: CompareExactUnordered,
		},
	},
}

var filterPredicatesTimestamp = TableTest{
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select _id from filter_predicates where ts1 != '2001-11-01T22:08:41+00:00'",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
			),
			ExpRows: rows(
				row(int64(2)),
				row(int64(3)),
				row(int64(4)),
				row(int64(5)),
				row(int64(6)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id from filter_predicates where ts1 = '2001-11-01T22:08:41+00:00'",
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
				"select _id from filter_predicates where ts1 > '2005-11-01T22:08:41+00:00'",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
			),
			ExpRows: rows(
				row(int64(6)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id from filter_predicates where ts1 >= '2005-11-01T22:08:41+00:00'",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
			),
			ExpRows: rows(
				row(int64(5)),
				row(int64(6)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id from filter_predicates where ts1 < '2002-11-01T22:08:41+00:00'",
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
				"select _id from filter_predicates where ts1 < '2002-11-01T22:08:41Z'",
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
				"select _id from filter_predicates where ts1 <= '2002-11-01T22:08:41+00:00'",
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
	},
}

var filterPredicatesDecimal = TableTest{
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select _id from filter_predicates where d1 != 10.00",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
			),
			ExpRows: rows(
				row(int64(2)),
				row(int64(3)),
				row(int64(4)),
				row(int64(5)),
				row(int64(6)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id from filter_predicates where d1 = 10.00",
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
				"select _id from filter_predicates where d1 > 50.00",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
			),
			ExpRows: rows(
				row(int64(6)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id from filter_predicates where d1 >= 50.00",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
			),
			ExpRows: rows(
				row(int64(5)),
				row(int64(6)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id from filter_predicates where d1 < 20.00",
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
				"select _id from filter_predicates where d1 <= 20.00",
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
	},
}

var filterPredicatesString = TableTest{
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select _id from filter_predicates where s1 != '10'",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
			),
			ExpRows: rows(
				row(int64(2)),
				row(int64(3)),
				row(int64(4)),
				row(int64(5)),
				row(int64(6)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id from filter_predicates where s1 = '10'",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
			),
			ExpRows: rows(
				row(int64(1)),
			),
			Compare: CompareExactUnordered,
		},
	},
}
