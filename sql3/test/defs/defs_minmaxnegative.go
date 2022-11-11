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
	SQLTests: []SQLTest{},
}
