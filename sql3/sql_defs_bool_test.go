package sql3_test

// BOOL tests
var boolTests = tableTest{
	name: "single-bool-field",
	table: tbl(
		"singleboolfield",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("a_bool", fldTypeBool),
		),
		srcRows(),
	),
	sqlTests: []sqlTest{
		{
			// Insert, step 1.
			name: "insert1",
			sqls: sqls(
				`insert into singleboolfield (_id, a_bool) values
						(1, true),
						(2, true),
						(3, false),
						(4, false),
						(5, null),
						(6, null)`,
			),
			expHdrs: hdrs(),
			expRows: rows(),
			compare: compareExactOrdered,
		},
		{
			// Select all, step 1.
			name: "select-all1",
			sqls: sqls(
				"select * from singleboolfield",
			),
			expHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("a_bool", fldTypeBool),
			),
			expRows: rows(
				row(int64(1), true),
				row(int64(2), true),
				row(int64(3), false),
				row(int64(4), false),
				row(int64(5), nil),
				row(int64(6), nil),
			),
			compare: compareExactOrdered,
		},
		{
			// Insert, step 2. Change bool values to all other combinations.
			name: "insert2",
			sqls: sqls(
				`insert into singleboolfield (_id, a_bool) values
						(1, false),
						(2, null),
						(3, true),
						(4, null),
						(5, false),
						(6, true)`,
			),
			expHdrs: hdrs(),
			expRows: rows(),
			compare: compareExactOrdered,
		},
		{
			// Select all, step 2.
			name: "select-all2",
			sqls: sqls(
				"select * from singleboolfield",
			),
			expHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("a_bool", fldTypeBool),
			),
			expRows: rows(
				row(int64(1), false),
				row(int64(2), nil),
				row(int64(3), true),
				row(int64(4), nil),
				row(int64(5), false),
				row(int64(6), true),
			),
			compare: compareExactOrdered,
		},
	},
}
