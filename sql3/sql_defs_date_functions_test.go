package sql3_test

// datepart tests
var datePartTests = tableTest{

	table: tbl(
		"dateparttests",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("a", fldTypeInt, "min 0", "max 1000"),
			srcHdr("b", fldTypeInt, "min 0", "max 1000"),
			srcHdr("ts", fldTypeTimestamp),
		),
		srcRows(
			srcRow(int64(1), int64(10), int64(100), knownTimestamp()),
		),
	),
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select datepart()",
			),
			expErr: "count of formal parameters (2) does not match count of actual parameters (0)",
		},
		{
			sqls: sqls(
				"select datepart(1, 2)",
			),
			expErr: "an expression of type 'INT' cannot be passed to a parameter of type 'STRING'",
		},
		{
			sqls: sqls(
				"select datepart('1', 2)",
			),
			expErr: "an expression of type 'INT' cannot be passed to a parameter of type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select datepart('1', current_timestamp)",
			),
			expErr: "invalid value '1' for parameter 'interval'",
		},
		{
			sqls: sqls(
				"select _id, datepart('yy', ts) from dateparttests",
			),
			expHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeInt),
			),
			expRows: rows(
				row(int64(1), int64(2012)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select _id, datepart('yd', ts) from dateparttests",
			),
			expHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeInt),
			),
			expRows: rows(
				row(int64(1), int64(306)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select _id, datepart('m', ts) from dateparttests",
			),
			expHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeInt),
			),
			expRows: rows(
				row(int64(1), int64(11)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select _id, datepart('d', ts) from dateparttests",
			),
			expHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeInt),
			),
			expRows: rows(
				row(int64(1), int64(1)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select _id, datepart('w', ts) from dateparttests",
			),
			expHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeInt),
			),
			expRows: rows(
				row(int64(1), int64(4)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select _id, datepart('wk', ts) from dateparttests",
			),
			expHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeInt),
			),
			expRows: rows(
				row(int64(1), int64(44)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select _id, datepart('hh', ts) from dateparttests",
			),
			expHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeInt),
			),
			expRows: rows(
				row(int64(1), int64(22)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select _id, datepart('mi', ts) from dateparttests",
			),
			expHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeInt),
			),
			expRows: rows(
				row(int64(1), int64(8)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select _id, datepart('s', ts) from dateparttests",
			),
			expHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeInt),
			),
			expRows: rows(
				row(int64(1), int64(41)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select _id, datepart('ms', ts) from dateparttests",
			),
			expHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeInt),
			),
			expRows: rows(
				row(int64(1), int64(0)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select _id, datepart('ns', ts) from dateparttests",
			),
			expHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeInt),
			),
			expRows: rows(
				row(int64(1), int64(0)),
			),
			compare: compareExactUnordered,
		},
	},
}
