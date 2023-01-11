package defs

var subqueryTests = TableTest{
	name: "subquerytable",
	Table: tbl(
		"subquerytable",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("a_string", fldTypeString),
		),
		srcRows(
			srcRow(int64(1), "str1"),
			srcRow(int64(2), "str1"),
			srcRow(int64(3), "str2"),
			srcRow(int64(4), "str2"),
			srcRow(int64(5), "str3"),
		),
	),
	SQLTests: []SQLTest{
		{
			name: "select-count",
			SQLs: sqls(
				"select sum(mycount) as thecount from (select count(a_string) as mycount, a_string from subquerytable group BY a_string);",
			),
			ExpHdrs: hdrs(
				hdr("thecount", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(5)),
			),
			Compare:        CompareExactUnordered,
			SortStringKeys: true,
		},
		{
			name: "select-count-distinct",
			SQLs: sqls(
				"select sum(mycount) as thecount from (select count(distinct a_string) as mycount, a_string from subquerytable group BY a_string);",
			),
			ExpHdrs: hdrs(
				hdr("thecount", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(3)),
			),
			Compare:        CompareExactUnordered,
			SortStringKeys: true,
		},
	},
}
