package defs

// IN tests
var inTests = TableTest{
	Table: tbl(
		"in_all_types",
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
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select _id in (1, 10) from in_all_types",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			ExpRows: rows(
				row(bool(true)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select i1 in (1, 1000) from in_all_types",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			ExpRows: rows(
				row(bool(true)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select b1 in (true, false) from in_all_types",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			ExpRows: rows(
				row(bool(true)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select d1 in (1.23, 4.56) from in_all_types",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			ExpRows: rows(
				row(bool(false)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select id1 in (3, 7) from in_all_types",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			ExpRows: rows(
				row(bool(false)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select ids1 in ([101, 102], [456, 789]) from in_all_types",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			ExpRows: rows(
				row(bool(true)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select s1 in ('foo', 'bar') from in_all_types",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			ExpRows: rows(
				row(bool(true)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select ss1 in (['a', 'b'], ['101', '102']) from in_all_types",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			ExpRows: rows(
				row(bool(true)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select t1 in ('2010-11-01T22:08:41+00:00', '2013-11-01T22:08:41+00:00', '2012-11-01T22:08:41+00:00') from in_all_types",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			ExpRows: rows(
				row(bool(true)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select t1._id in (select t2._id from in_all_types as t2) from in_all_types as t1",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			ExpRows: rows(
				row(bool(true)),
			),
			Compare: CompareExactUnordered,
		},
		// This test fails - re-enable it when fixing the bug
		/*
			{
				SQLs: sqls(
					"select t1._id in (select t2.id1 from in_all_types as t2) from in_all_types as t1",
				),
				ExpHdrs: hdrs(
					hdr("", fldTypeBool),
				),
				ExpRows: rows(
					row(bool(false)),
				),
				Compare: CompareExactUnordered,
			},
		*/
		// This test also fails.
		// Once re-enabled and passing, it provides coverage for expressionanalyzer.go
		// (*ExecutionPlanner).analyzeBinaryExpression in the case where the left hand side
		// of the IN contains a JOIN.
		/*
			{
				SQLs: sqls(
					"select a1._id from in_all_types a1 inner join in_all_types a2 on a1._id=a2._id where a1._id in (select _id from in_all_types);",
				),
				ExpHdrs: hdrs(
					hdr("", fldTypeBool),
				),
				ExpRows: rows(
					row(bool(true)),
				),
				Compare: CompareExactUnordered,
			},
		*/
	},
}

// NOT IN tests
var notInTests = TableTest{
	Table: tbl(
		"not_in_all_types",
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
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select _id not in (1, 10) from not_in_all_types",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			ExpRows: rows(
				row(bool(false)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select i1 not in (1, 1000) from not_in_all_types",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			ExpRows: rows(
				row(bool(false)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select b1 not in (true, false) from not_in_all_types",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			ExpRows: rows(
				row(bool(false)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select d1 not in (1.23, 4.56) from not_in_all_types",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			ExpRows: rows(
				row(bool(true)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select id1 not in (3, 7) from not_in_all_types",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			ExpRows: rows(
				row(bool(true)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select ids1 not in ([101, 102], [456, 789]) from not_in_all_types",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			ExpRows: rows(
				row(bool(false)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select s1 not in ('foo', 'bar') from not_in_all_types",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			ExpRows: rows(
				row(bool(false)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select ss1 not in (['a', 'b'], ['101', '102']) from not_in_all_types",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			ExpRows: rows(
				row(bool(false)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select t1 not in ('2010-11-01T22:08:41+00:00', '2013-11-01T22:08:41+00:00', '2012-11-01T22:08:41+00:00') from not_in_all_types",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			ExpRows: rows(
				row(bool(false)),
			),
			Compare: CompareExactUnordered,
		},
		// this test currently causes a panic
		/*
			{
				SQLs: sqls(
					"select t1._id in (select t2.s1 from in_all_types as t2) from in_all_types as t1",
				),
				ExpHdrs: hdrs(
					hdr("", fldTypeBool),
				),
				ExpErr:  "types 'id' and 'string' are not equatable",
				Compare: CompareExactUnordered,
			},
		*/
		{
			SQLs: sqls(
				"select t1._id not in (select t2._id from in_all_types as t2) from in_all_types as t1",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			ExpRows: rows(
				row(bool(false)),
			),
			Compare: CompareExactUnordered,
		},
		// This test fails - re-enable it when the bug is fixed
		/*
			{
				SQLs: sqls(
					"select t1._id not in (select t2.id1 from in_all_types as t2) from in_all_types as t1",
				),
				ExpHdrs: hdrs(
					hdr("", fldTypeBool),
				),
				ExpRows: rows(
					row(bool(true)),
				),
				Compare: CompareExactUnordered,
			},
		*/
		// This test also fails.
		// Once re-enabled and passing, it provides coverage for expressionanalyzer.go
		// (*ExecutionPlanner).analyzeBinaryExpression in the case where the left hand side
		// of the IN contains a JOIN.
		/*
			{
				SQLs: sqls(
					"select a1._id from in_all_types a1 inner join in_all_types a2 on a1._id=a2._id where a1._id not in (select _id from in_all_types);",
				),
				ExpHdrs: hdrs(
					hdr("", fldTypeBool),
				),
				ExpRows: rows(
					row(bool(true)),
				),
				Compare: CompareExactUnordered,
			},
		*/
	},
}
