package sql3_test

import "time"

var unaryOpExprWithInt = tableTest{
	table: tbl(
		"unoptesti",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("i", fldTypeInt, "min 0", "max 1000"),
		),
		srcRows(
			srcRow(int64(1), int64(10)),
		),
	),
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select -i from unoptesti;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			expRows: rows(
				row(int64(-10)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select !i from unoptesti;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			expRows: rows(
				row(int64(-11)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select +i from unoptesti;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			expRows: rows(
				row(int64(10)),
			),
			compare: compareExactUnordered,
		},
	},
}

var unaryOpExprWithBool = tableTest{
	table: tbl(
		"unoptest_b",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("i", fldTypeBool),
		),
		srcRows(
			srcRow(int64(1), bool(false)),
		),
	),
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select -i from unoptest_b;",
			),
			expErr: "operator '-' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select !i from unoptest_b;",
			),
			expErr: "operator '!' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select +i from unoptest_b;",
			),
			expErr: "operator '+' incompatible with type 'BOOL'",
		},
	},
}

var unaryOpExprWithID = tableTest{
	table: tbl(
		"unoptestid",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("a", fldTypeInt, "min 0", "max 1000"),
		),
		srcRows(
			srcRow(int64(1), int64(10)),
		),
	),
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select -_id from unoptestid;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			expRows: rows(
				row(int64(-1)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select !_id from unoptestid;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeID),
			),
			expRows: rows(
				row(int64(-2)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select +_id from unoptestid;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			expRows: rows(
				row(int64(1)),
			),
			compare: compareExactUnordered,
		},
	},
}

var unaryOpExprWithDecimal = tableTest{
	table: tbl(
		"unoptestd",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("d", fldTypeDecimal2),
		),
		srcRows(
			srcRow(int64(1), float64(12.34)),
		),
	),
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select -d from unoptestd;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeDecimal2),
			),
			expRows: rows(
				row(float64(-12.34)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select !d from unoptestd;",
			),
			expErr: "operator '!' incompatible with type 'DECIMAL(2)'",
		},
		{
			sqls: sqls(
				"select +d from unoptestd;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeDecimal2),
			),
			expRows: rows(
				row(float64(12.34)),
			),
			compare: compareExactUnordered,
		},
	},
}

var unaryOpExprWithTimestamp = tableTest{
	table: tbl(
		"unoptestts",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("ts", fldTypeTimestamp),
		),
		srcRows(
			srcRow(int64(1), time.Time(knownTimestamp())),
		),
	),
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select -ts from unoptestts;",
			),
			expErr: "operator '-' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select !ts from unoptestts;",
			),
			expErr: "operator '!' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select +ts from unoptestts;",
			),
			expErr: "operator '+' incompatible with type 'TIMESTAMP'",
		},
	},
}

var unaryOpExprWithIDSet = tableTest{
	table: tbl(
		"unoptestids",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("ids", fldTypeIDSet),
		),
		srcRows(
			srcRow(int64(1), []int64{11, 12, 13}),
		),
	),
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select -ids from unoptestids;",
			),
			expErr: "operator '-' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select !ids from unoptestids;",
			),
			expErr: "operator '!' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select +ids from unoptestids;",
			),
			expErr: "operator '+' incompatible with type 'IDSET'",
		},
	},
}

var unaryOpExprWithString = tableTest{
	table: tbl(
		"unoptest_s",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("s", fldTypeString),
		),
		srcRows(
			srcRow(int64(1), string("foo")),
		),
	),
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select -s from unoptest_s;",
			),
			expErr: "operator '-' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select !s from unoptest_s;",
			),
			expErr: "operator '!' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select +s from unoptest_s;",
			),
			expErr: "operator '+' incompatible with type 'STRING'",
		},
	},
}

var unaryOpExprWithStringSet = tableTest{
	table: tbl(
		"unoptestss",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("s", fldTypeStringSet),
		),
		srcRows(
			srcRow(int64(1), []string{"11", "12", "13"}),
		),
	),
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select -s from unoptestss;",
			),
			expErr: "operator '-' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select !s from unoptestss;",
			),
			expErr: "operator '!' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select +s from unoptestss;",
			),
			expErr: "operator '+' incompatible with type 'STRINGSET'",
		},
	},
}
