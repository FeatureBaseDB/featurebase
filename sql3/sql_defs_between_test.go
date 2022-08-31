package sql3_test

//BETWEEN tests
var betweenTests = tableTest{
	table: tbl(
		"between_all_types",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("i1", fldTypeInt, "min 0", "max 1000"),
			srcHdr("b1", fldTypeBool),
			srcHdr("d1", fldTypeDecimal2),
			srcHdr("id1", fldTypeID),
			srcHdr("ids1", fldTypeIDSet),
			srcHdr("s1", fldTypeString),
			srcHdr("ss1", fldTypeStringSet),
			srcHdr("t1", fldTypeTimestamp),
		),
		srcRows(
			srcRow(int64(1), int64(1000), bool(true), float64(12.34), int64(20), []int64{101, 102}, string("foo"), []string{"101", "102"}, knownTimestamp()),
		),
	),
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select _id between 1 and 10 from between_all_types",
			),
			expHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			expRows: rows(
				row(bool(true)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select i1 between 1 and 10 from between_all_types",
			),
			expHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			expRows: rows(
				row(bool(false)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select b1 between true and false from between_all_types",
			),
			expErr: "type 'BOOL' cannot be used a range subscript",
		},
		{
			sqls: sqls(
				"select d1 between 1.23 and 4.56 from between_all_types",
			),
			expErr: "type 'DECIMAL(2)' cannot be used a range subscript",
		},
		{
			sqls: sqls(
				"select id1 between 3 and 7 from between_all_types",
			),
			expHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			expRows: rows(
				row(bool(false)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select ids1 between [100, 102] and [456, 789] from between_all_types",
			),
			expErr: "type 'IDSET' cannot be used a range subscript",
		},
		{
			sqls: sqls(
				"select s1 between 'foo' and 'bar' from between_all_types",
			),
			expErr: "type 'STRING' cannot be used a range subscript",
		},
		{
			sqls: sqls(
				"select ss1 between ['a', 'b'] and ['c', 'd'] from between_all_types",
			),
			expErr: "type 'STRINGSET' cannot be used a range subscript",
		},
		{
			sqls: sqls(
				"select t1 between '2010-11-01T22:08:41+00:00' and '2013-11-01T22:08:41+00:00' from between_all_types",
			),
			expHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			expRows: rows(
				row(bool(true)),
			),
			compare: compareExactUnordered,
		},
	},
}

//NOT BETWEEN tests
var notBetweenTests = tableTest{
	table: tbl(
		"not_between_all_types",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("i1", fldTypeInt, "min 0", "max 1000"),
			srcHdr("b1", fldTypeBool),
			srcHdr("d1", fldTypeDecimal2),
			srcHdr("id1", fldTypeID),
			srcHdr("ids1", fldTypeIDSet),
			srcHdr("s1", fldTypeString),
			srcHdr("ss1", fldTypeStringSet),
			srcHdr("t1", fldTypeTimestamp),
		),
		srcRows(
			srcRow(int64(1), int64(1000), bool(true), float64(12.34), int64(20), []int64{101, 102}, string("foo"), []string{"101", "102"}, knownTimestamp()),
		),
	),
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select _id not between 1 and 10 from not_between_all_types",
			),
			expHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			expRows: rows(
				row(bool(false)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select i1 not between 1 and 10 from not_between_all_types",
			),
			expHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			expRows: rows(
				row(bool(true)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select b1 not between true and false from not_between_all_types",
			),
			expErr: "type 'BOOL' cannot be used a range subscript",
		},
		{
			sqls: sqls(
				"select d1 not between 1.23 and 4.56 from not_between_all_types",
			),
			expErr: "type 'DECIMAL(2)' cannot be used a range subscript",
		},
		{
			sqls: sqls(
				"select id1 between 3 and 7 from not_between_all_types",
			),
			expHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			expRows: rows(
				row(bool(false)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select ids1 not between [100, 102] and [456, 789] from not_between_all_types",
			),
			expErr: "type 'IDSET' cannot be used a range subscript",
		},
		{
			sqls: sqls(
				"select s1 not between 'foo' and 'bar' from not_between_all_types",
			),
			expErr: "type 'STRING' cannot be used a range subscript",
		},
		{
			sqls: sqls(
				"select ss1 not between ['a', 'b'] and ['c', 'd'] from not_between_all_types",
			),
			expErr: "type 'STRINGSET' cannot be used a range subscript",
		},
		{
			sqls: sqls(
				"select t1 not between '2010-11-01T22:08:41+00:00' and '2013-11-01T22:08:41+00:00' from not_between_all_types",
			),
			expHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			expRows: rows(
				row(bool(false)),
			),
			compare: compareExactUnordered,
		},
	},
}
