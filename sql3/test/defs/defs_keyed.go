package defs

var Keyed TableTest = keyed

var keyed = TableTest{
	Table: tbl(
		"keyed",
		srcHdrs(
			srcHdr("_id", fldTypeString),
			srcHdr("an_int", fldTypeInt, "min 0", "max 100"),
			srcHdr("an_id_set", fldTypeIDSet),
			srcHdr("an_id", fldTypeID),
			srcHdr("a_string", fldTypeString),
			srcHdr("a_string_set", fldTypeStringSet),
		),
		srcRows(
			srcRow("one", int64(11), []int64{11, 12, 13}, int64(101), "str1", []string{"a1", "b1", "c1"}),
			srcRow("two", int64(22), []int64{11, 12, 23}, int64(201), "str2", []string{"a2", "b2", "c2"}),
			srcRow("three", int64(33), []int64{11, 32, 33}, int64(301), "str3", []string{"a3", "b3", "c3"}),
			srcRow("four", int64(44), []int64{41, 42, 43}, int64(401), "str4", []string{"a4", "b4", "c4"}),
		),
		srcRows(
			srcRow("five", int64(55), []int64{51, 52, 53}, int64(501), "str5", []string{"a5", "b5", "c5"}),
			srcRow("six", int64(66), []int64{61, 62, 63}, int64(601), "str6", []string{"a6", "b6", "c6"}),
		),
	),
	SQLTests: []SQLTest{
		{
			// Select all.
			name: "select-all",
			SQLs: sqls(
				"select * from keyed",
				"select _id, an_int, an_id_set, an_id, a_string, a_string_set from keyed",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeString),
				hdr("an_int", fldTypeInt),
				hdr("an_id_set", fldTypeIDSet),
				hdr("an_id", fldTypeID),
				hdr("a_string", fldTypeString),
				hdr("a_string_set", fldTypeStringSet),
			),
			ExpRows: rows(
				row("one", int64(11), []int64{11, 12, 13}, int64(101), "str1", []string{"a1", "b1", "c1"}),
				row("two", int64(22), []int64{11, 12, 23}, int64(201), "str2", []string{"a2", "b2", "c2"}),
				row("three", int64(33), []int64{11, 32, 33}, int64(301), "str3", []string{"a3", "b3", "c3"}),
				row("four", int64(44), []int64{41, 42, 43}, int64(401), "str4", []string{"a4", "b4", "c4"}),
			),
			ExpRowsPlus1: rowSets(
				rows(
					row("one", int64(11), []int64{11, 12, 13}, int64(101), "str1", []string{"a1", "b1", "c1"}),
					row("two", int64(22), []int64{11, 12, 23}, int64(201), "str2", []string{"a2", "b2", "c2"}),
					row("three", int64(33), []int64{11, 32, 33}, int64(301), "str3", []string{"a3", "b3", "c3"}),
					row("four", int64(44), []int64{41, 42, 43}, int64(401), "str4", []string{"a4", "b4", "c4"}),
					row("five", int64(55), []int64{51, 52, 53}, int64(501), "str5", []string{"a5", "b5", "c5"}),
					row("six", int64(66), []int64{61, 62, 63}, int64(601), "str6", []string{"a6", "b6", "c6"}),
				),
			),
			Compare:        CompareExactUnordered,
			SortStringKeys: true,
		},
		{
			// Select all with top.
			name: "select-all-with-top",
			SQLs: sqls(
				"select top(2) * from keyed",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeString),
				hdr("an_int", fldTypeInt),
				hdr("an_id_set", fldTypeIDSet),
				hdr("an_id", fldTypeID),
				hdr("a_string", fldTypeString),
				hdr("a_string_set", fldTypeStringSet),
			),
			ExpRows: rows(
				row("one", int64(11), []int64{11, 12, 13}, int64(101), "str1", []string{"a1", "b1", "c1"}),
				row("two", int64(22), []int64{11, 12, 23}, int64(201), "str2", []string{"a2", "b2", "c2"}),
				row("three", int64(33), []int64{11, 32, 33}, int64(301), "str3", []string{"a3", "b3", "c3"}),
				row("four", int64(44), []int64{41, 42, 43}, int64(401), "str4", []string{"a4", "b4", "c4"}),
			),
			Compare:        CompareIncludedIn,
			SortStringKeys: true,
			ExpRowCount:    2,
		},
		{
			// Select all with where on int field.
			name: "select-all-with-where",
			SQLs: sqls(
				"select * from keyed where an_int = 22",
				"select * from keyed where a_string = 'str2'",
				"select * from keyed where an_id = 201",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeString),
				hdr("an_int", fldTypeInt),
				hdr("an_id_set", fldTypeIDSet),
				hdr("an_id", fldTypeID),
				hdr("a_string", fldTypeString),
				hdr("a_string_set", fldTypeStringSet),
			),
			ExpRows: rows(
				row("two", int64(22), []int64{11, 12, 23}, int64(201), "str2", []string{"a2", "b2", "c2"}),
			),
			Compare:        CompareExactUnordered,
			SortStringKeys: true,
		},
	},
	PQLTests: []PQLTest{
		{
			name:  "minrow",
			Table: "keyed",
			PQLs:  []string{"MinRow(field=an_id_set)"},
			ExpHdrs: hdrs(
				hdr("an_id_set", fldTypeID),
				hdr("count", fldTypeID),
			),
			ExpRows: rows(
				row(int64(11), int64(1)),
			),
		},
		{
			name:  "maxrow",
			Table: "keyed",
			PQLs:  []string{"MaxRow(field=an_id_set)"},
			ExpHdrs: hdrs(
				hdr("an_id_set", fldTypeID),
				hdr("count", fldTypeID),
			),
			ExpRows: rows(
				row(int64(43), int64(1)),
			),
		},
		{
			name:  "topk",
			Table: "keyed",
			PQLs:  []string{"TopK(an_id_set, k=2)"},
			ExpHdrs: hdrs(
				hdr("an_id_set", fldTypeID),
				hdr("count", fldTypeID),
			),
			ExpRows: rows(
				row(int64(11), int64(3)),
				row(int64(12), int64(2)),
			),
		},
		// TODO(tlt): figure out why this sometimes fails on multi-node setups
		// {
		// 	name:  "topn",
		// 	Table: "keyed",
		// 	PQLs:  []string{"TopN(an_id_set, n=2)"},
		// 	ExpHdrs: hdrs(
		// 		hdr("an_id_set", fldTypeID),
		// 		hdr("count", fldTypeID),
		// 	),
		// 	ExpRows: rows(
		// 		row(int64(11), int64(3)),
		// 		row(int64(12), int64(2)),
		// 	),
		// },
		{
			name:  "rows",
			Table: "keyed",
			PQLs:  []string{"Rows(field=an_id_set)"},
			ExpHdrs: hdrs(
				hdr("an_id_set", fldTypeID),
			),
			ExpRows: rows(
				row(int64(11)),
				row(int64(12)),
				row(int64(13)),
				row(int64(23)),
				row(int64(32)),
				row(int64(33)),
				row(int64(41)),
				row(int64(42)),
				row(int64(43)),
			),
		},
		{
			name:  "includescolumn",
			Table: "keyed",
			PQLs:  []string{"IncludesColumn(Row(an_id_set=12), column='two')"},
			ExpHdrs: hdrs(
				hdr("result", fldTypeBool),
			),
			ExpRows: rows(
				row(true),
			),
		},
		{
			name:  "constrow",
			Table: "keyed",
			PQLs:  []string{"Extract(ConstRow(columns=['two']), Rows(an_id))"},
			ExpHdrs: hdrs(
				hdr("_id", fldTypeString),
				hdr("an_id", fldTypeID),
			),
			ExpRows: rows(
				row("two", int64(201)),
			),
		},
		{
			name:  "fieldvalue",
			Table: "keyed",
			PQLs:  []string{"FieldValue(field=an_int, column='three')"},
			ExpHdrs: hdrs(
				hdr("value", fldTypeInt),
				hdr("count", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(33), int64(1)),
			),
		},
		{
			name:  "unionrows",
			Table: "keyed",
			PQLs:  []string{"Count(UnionRows(Rows(field=an_id_set)))"},
			ExpHdrs: hdrs(
				hdr("count", fldTypeID),
			),
			ExpRows: rows(
				row(int64(4)),
			),
		},
	},
}
