// Copyright 2021 Molecula Corp. All rights reserved.
package defs

var timestampLiterals = TableTest{
	Table: tbl(
		"testtimestampliterals",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("a", fldTypeInt, "min 0", "max 1000"),
			srcHdr("b", fldTypeInt, "min 0", "max 1000"),
			srcHdr("d", fldTypeDecimal2),
			srcHdr("ts", fldTypeTimestamp),
			srcHdr("event", fldTypeStringSet),
			srcHdr("ievent", fldTypeIDSet),
		),
	),
	SQLTests: []SQLTest{
		{
			// InsertWithCurrentTimestamp
			SQLs: sqls(
				"insert into testtimestampliterals (_id, a, b, d, ts, event, ievent) values (4, 40, 400, 10.12, current_timestamp, ['A', 'B', 'C'], [1, 2, 3])",
			),
			ExpHdrs: hdrs(),
			ExpRows: rows(),
			Compare: CompareExactUnordered,
		},
		{
			// InsertWithCurrentDate
			SQLs: sqls(
				"insert into testtimestampliterals (_id, a, b, d, ts, event, ievent) values (4, 40, 400, 10.12, current_date, ['A', 'B', 'C'], [1, 2, 3])",
			),
			ExpHdrs: hdrs(),
			ExpRows: rows(),
			Compare: CompareExactUnordered,
		},
	},
}
