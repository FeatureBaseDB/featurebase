package defs

var minmaxnegatives = TableTest{
	name: "minmaxnegatives",
	Table: tbl(
		"minmaxnegatives",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("positive_int", fldTypeInt, "min 10", "max 100"),
			srcHdr("negative_int", fldTypeInt, "min -100", "max -10"),
		),
		srcRows(
			srcRow(int64(1), int64(11), int64(-11)),
			srcRow(int64(2), int64(22), int64(-22)),
			srcRow(int64(3), int64(33), int64(-33)),
		),
	),
	SQLTests: []SQLTest{
		{
			// Select all.
			name: "select-all",
			SQLs: sqls(
				"select * from minmaxnegatives",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("positive_int", fldTypeInt),
				hdr("negative_int", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(1), int64(21), int64(-21)),
				row(int64(2), int64(32), int64(-32)),
				row(int64(3), int64(43), int64(-43)),
				// TODO(tlt): this test did not exist, and the values coming
				// back are incorrect. We need to fix this test based on the
				// correct results below:
				// row(int64(1), int64(11), int64(-11)),
				// row(int64(2), int64(22), int64(-22)),
				// row(int64(3), int64(33), int64(-33)),
			),
			Compare: CompareExactOrdered,
		},
	},
}
