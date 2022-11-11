package defs

// datepart tests
var datePartTests = TableTest{

	Table: tbl(
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
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select datepart()",
			),
			ExpErr: "count of formal parameters (2) does not match count of actual parameters (0)",
		},
		{
			SQLs: sqls(
				"select datepart(1, 2)",
			),
			ExpErr: "an expression of type 'INT' cannot be passed to a parameter of type 'STRING'",
		},
		{
			SQLs: sqls(
				"select datepart('1', 2)",
			),
			ExpErr: "an expression of type 'INT' cannot be passed to a parameter of type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select datepart('1', current_timestamp)",
			),
			ExpErr: "invalid value '1' for parameter 'interval'",
		},
		{
			SQLs: sqls(
				"select _id, datepart('yy', ts) from dateparttests",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(1), int64(2012)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id, datepart('yd', ts) from dateparttests",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(1), int64(306)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id, datepart('m', ts) from dateparttests",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(1), int64(11)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id, datepart('d', ts) from dateparttests",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(1), int64(1)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id, datepart('w', ts) from dateparttests",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(1), int64(4)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id, datepart('wk', ts) from dateparttests",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(1), int64(44)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id, datepart('hh', ts) from dateparttests",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(1), int64(22)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id, datepart('mi', ts) from dateparttests",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(1), int64(8)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id, datepart('s', ts) from dateparttests",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(1), int64(41)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id, datepart('ms', ts) from dateparttests",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(1), int64(0)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id, datepart('ns', ts) from dateparttests",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(1), int64(0)),
			),
			Compare: CompareExactUnordered,
		},
	},
}
