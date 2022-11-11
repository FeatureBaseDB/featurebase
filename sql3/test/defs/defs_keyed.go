package defs

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
			srcRow("two", int64(22), []int64{21, 22, 23}, int64(201), "str2", []string{"a2", "b2", "c2"}),
			srcRow("three", int64(33), []int64{31, 32, 33}, int64(301), "str3", []string{"a3", "b3", "c3"}),
			srcRow("four", int64(44), []int64{41, 42, 43}, int64(401), "str4", []string{"a4", "b4", "c4"}),
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
				row("two", int64(22), []int64{21, 22, 23}, int64(201), "str2", []string{"a2", "b2", "c2"}),
				row("three", int64(33), []int64{31, 32, 33}, int64(301), "str3", []string{"a3", "b3", "c3"}),
				row("four", int64(44), []int64{41, 42, 43}, int64(401), "str4", []string{"a4", "b4", "c4"}),
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
				row("two", int64(22), []int64{21, 22, 23}, int64(201), "str2", []string{"a2", "b2", "c2"}),
				row("three", int64(33), []int64{31, 32, 33}, int64(301), "str3", []string{"a3", "b3", "c3"}),
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
				row("two", int64(22), []int64{21, 22, 23}, int64(201), "str2", []string{"a2", "b2", "c2"}),
			),
			Compare:        CompareExactUnordered,
			SortStringKeys: true,
		},
	},
}
