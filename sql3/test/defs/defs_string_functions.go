package defs

// string function tests
var stringScalarFunctionsTests = TableTest{

	Table: tbl(
		"stringscalarfunctions",
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
				"select reverse('this')",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(string("siht")),
			),
			Compare: CompareExactUnordered,
		},
	},
}
