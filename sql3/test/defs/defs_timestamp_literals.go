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
				"insert into testtimestampliterals (_id, a, b, d, ts, event, ievent) values (1, 40, 400, 10.12, current_timestamp, ['A', 'B', 'C'], [1, 2, 3])",
			),
			ExpHdrs: hdrs(),
			ExpRows: rows(),
			Compare: CompareExactUnordered,
		},
		{
			// InsertWithCurrentDate
			SQLs: sqls(
				"insert into testtimestampliterals (_id, a, b, d, ts, event, ievent) values (2, 40, 400, 10.12, current_date, ['A', 'B', 'C'], [1, 2, 3])",
			),
			ExpHdrs: hdrs(),
			ExpRows: rows(),
			Compare: CompareExactUnordered,
		},
		{
			// Insert literal 0 into a timestamp, it should be stored as 1970-01-01 00:00:00 +0000 UTC (unix epoch base value)
			SQLs: sqls(
				"insert into testtimestampliterals (_id, a, b, d, ts, event, ievent) values (3, 40, 400, 10.12, 0, ['A', 'B', 'C'], [1, 2, 3])",
			),
			ExpHdrs: hdrs(),
			ExpRows: rows(),
			Compare: CompareExactUnordered,
		},
		{
			// Insert literal -86400 into a timestamp, it should be stored as 1969-12-31 00:00:00 +0000 UTC (unix epoch base value)
			SQLs: sqls(
				"insert into testtimestampliterals (_id, a, b, d, ts, event, ievent) values (4, 40, 400, 10.12, -86400, ['A', 'B', 'C'], [1, 2, 3])",
			),
			ExpHdrs: hdrs(),
			ExpRows: rows(),
			Compare: CompareExactUnordered,
		},
		{
			//compare test is done only for integer test cases (_id in (3,4), because only for these cases we have a determinate year(1970, 1969) to look for.
			SQLs: sqls(
				"select _id, datepart('yy', ts) as \"yy\" from testtimestampliterals where _id in (3,4)",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("yy", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(3), int64(1970)),
				row(int64(4), int64(1969)),
			),
			Compare: CompareExactUnordered,
		},
	},
}
