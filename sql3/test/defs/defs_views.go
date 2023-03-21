package defs

import "time"

var viewTests = TableTest{
	name: "viewtests",
	Table: tbl(
		"viewtable",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("a_string", fldTypeString),
			srcHdr("a_int", fldTypeInt),
			srcHdr("a_date", fldTypeTimestamp),
		),
		srcRows(
			srcRow(int64(1), "str1", int64(10), time.Unix(0, 0).UTC()),
			srcRow(int64(2), "str1", int64(20), time.Unix(0, 0).UTC()),
			srcRow(int64(3), "str2", int64(30), time.Unix(0, 0).UTC()),
			srcRow(int64(4), "str2", int64(40), time.Unix(0, 0).UTC()),
			srcRow(int64(5), "str3", int64(50), time.Unix(0, 0).UTC()),
		),
	),
	SQLTests: []SQLTest{
		// test for error where an table exist with the requested new view name.
		{
			name: "create-view-should-fail",
			SQLs: sqls(
				"create view viewtable as select _id, a_string, a_int from viewtable;",
			),
			ExpErr: "table or view 'viewtable' already exists",
		},
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
		{
			name: "create-view-with-built-in-literals",
			SQLs: sqls(
				"create view if not exists viewwithliteral as select _id, a_string, a_int, a_date from viewtable where a_date<CURRENT_TIMESTAMP or a_date<CURRENT_DATE or a_date<'2023-03-15T00:00:00Z';",
			),
			ExpHdrs:        hdrs(),
			ExpRows:        rows(),
			Compare:        CompareExactUnordered,
			SortStringKeys: true,
		},
		{
			name: "select-view-with-built-in-literals",
			SQLs: sqls(
				"select * from viewwithliteral;",
				"select _id, a_string, a_int, a_date from viewwithliteral;",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("a_string", fldTypeString),
				hdr("a_int", fldTypeInt),
				hdr("a_date", fldTypeTimestamp),
			),
			ExpRows: rows(
				row(int64(1), "str1", int64(10), time.Unix(0, 0).UTC()),
				row(int64(2), "str1", int64(20), time.Unix(0, 0).UTC()),
				row(int64(3), "str2", int64(30), time.Unix(0, 0).UTC()),
				row(int64(4), "str2", int64(40), time.Unix(0, 0).UTC()),
				row(int64(5), "str3", int64(50), time.Unix(0, 0).UTC()),
			),
			Compare:        CompareExactUnordered,
			SortStringKeys: true,
		},
	},
}
