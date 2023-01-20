package defs

var viewTests = TableTest{
	name: "viewtests",
	Table: tbl(
		"viewtable",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("a_string", fldTypeString),
			srcHdr("a_int", fldTypeInt),
		),
		srcRows(
			srcRow(int64(1), "str1", int64(10)),
			srcRow(int64(2), "str1", int64(20)),
			srcRow(int64(3), "str2", int64(30)),
			srcRow(int64(4), "str2", int64(40)),
			srcRow(int64(5), "str3", int64(50)),
		),
	),
	SQLTests: []SQLTest{
		{
			name: "create-view",
			SQLs: sqls(
				"create view viewonviewtable as select _id, a_string, a_int from viewtable;",
			),
			ExpHdrs:        hdrs(),
			ExpRows:        rows(),
			Compare:        CompareExactUnordered,
			SortStringKeys: true,
		},
		{
			name: "create-view-should-fail",
			SQLs: sqls(
				"create view viewonviewtable as select _id, a_string, a_int from viewtable;",
			),
			ExpErr: "view 'viewonviewtable' already exists",
		},
		{
			name: "create-view-should-not-fail",
			SQLs: sqls(
				"create view if not exists viewonviewtable as select _id, a_string, a_int from viewtable;",
			),
			ExpHdrs:        hdrs(),
			ExpRows:        rows(),
			Compare:        CompareExactUnordered,
			SortStringKeys: true,
		},
		{
			name: "select-view",
			SQLs: sqls(
				"select * from viewonviewtable;",
				"select _id, a_string, a_int from viewonviewtable;",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("a_string", fldTypeString),
				hdr("a_int", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(1), "str1", int64(10)),
				row(int64(2), "str1", int64(20)),
				row(int64(3), "str2", int64(30)),
				row(int64(4), "str2", int64(40)),
				row(int64(5), "str3", int64(50)),
			),
			Compare:        CompareExactUnordered,
			SortStringKeys: true,
		},
		{
			name: "alter-view",
			SQLs: sqls(
				"alter view viewonviewtable as select _id, a_string, a_int from viewtable where a_int > 20;",
			),
			ExpHdrs:        hdrs(),
			ExpRows:        rows(),
			Compare:        CompareExactUnordered,
			SortStringKeys: true,
		},
		{
			name: "select-alter-view",
			SQLs: sqls(
				"select * from viewonviewtable;",
				"select _id, a_string, a_int from viewonviewtable;",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("a_string", fldTypeString),
				hdr("a_int", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(3), "str2", int64(30)),
				row(int64(4), "str2", int64(40)),
				row(int64(5), "str3", int64(50)),
			),
			Compare:        CompareExactUnordered,
			SortStringKeys: true,
		},
		{
			name: "drop-view",
			SQLs: sqls(
				"drop view viewonviewtable;",
			),
			ExpHdrs:        hdrs(),
			ExpRows:        rows(),
			Compare:        CompareExactUnordered,
			SortStringKeys: true,
		},
		{
			name: "drop-view-if-exists-after-drop",
			SQLs: sqls(
				"drop view if exists viewonviewtable;",
			),
			ExpHdrs:        hdrs(),
			ExpRows:        rows(),
			Compare:        CompareExactUnordered,
			SortStringKeys: true,
		},
		{
			name: "select-view-after-drop",
			SQLs: sqls(
				"select * from viewonviewtable;",
			),
			ExpErr: "table or view 'viewonviewtable' not found",
		},
	},
}
