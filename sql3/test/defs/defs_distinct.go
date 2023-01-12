package defs

import "github.com/featurebasedb/featurebase/v3/pql"

// distinct tests
var distinctTests = TableTest{
	Table: tbl(
		"distinct_test",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("i1", fldTypeInt),
			srcHdr("b1", fldTypeBool),
			srcHdr("id1", fldTypeID),
			srcHdr("ids1", fldTypeIDSet),
			srcHdr("d1", fldTypeDecimal2),
			srcHdr("s1", fldTypeString),
			srcHdr("ss1", fldTypeStringSet),
			srcHdr("ts1", fldTypeTimestamp),
		),
		srcRows(
			srcRow(int64(1), int64(10), bool(false), int64(1), []int64{10, 20, 30}, float64(10.00), string("10"), []string{"10", "20", "30"}, knownTimestamp()),
			srcRow(int64(2), int64(20), bool(true), int64(2), []int64{11, 21, 31}, float64(20.00), string("20"), []string{"11", "21", "31"}, knownTimestamp()),
			srcRow(int64(3), int64(30), bool(false), int64(3), []int64{12, 22, 32}, float64(30.00), string("30"), []string{"12", "22", "32"}, knownTimestamp()),
			srcRow(int64(4), int64(10), bool(false), int64(1), []int64{10, 20, 30}, float64(10.00), string("10"), []string{"10", "20", "30"}, knownTimestamp()),
			srcRow(int64(5), int64(20), bool(true), int64(2), []int64{11, 21, 31}, float64(20.00), string("20"), []string{"11", "21", "31"}, knownTimestamp()),
			srcRow(int64(6), int64(30), bool(false), int64(3), []int64{12, 22, 32}, float64(30.00), string("30"), []string{"12", "22", "32"}, knownTimestamp()),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select distinct i1, b1, id1 from distinct_test",
			),
			ExpHdrs: hdrs(
				hdr("i1", fldTypeInt),
				hdr("b1", fldTypeBool),
				hdr("id1", fldTypeID),
			),
			ExpRows: rows(
				row(int64(10), bool(false), int64(1)),
				row(int64(20), bool(true), int64(2)),
				row(int64(30), bool(false), int64(3)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select distinct i1 from distinct_test",
			),
			ExpHdrs: hdrs(
				hdr("i1", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(10)),
				row(int64(20)),
				row(int64(30)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select distinct b1 from distinct_test",
			),
			ExpHdrs: hdrs(
				hdr("b1", fldTypeBool),
			),
			ExpRows: rows(
				row(bool(false)),
				row(bool(true)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select distinct id1 from distinct_test",
			),
			ExpHdrs: hdrs(
				hdr("id1", fldTypeID),
			),
			ExpRows: rows(
				row(int64(1)),
				row(int64(2)),
				row(int64(3)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select distinct ids1 from distinct_test",
			),
			ExpHdrs: hdrs(
				hdr("ids1", fldTypeIDSet),
			),
			ExpRows: rows(
				row([]int64{10, 20, 30}),
				row([]int64{11, 21, 31}),
				row([]int64{12, 22, 32}),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select distinct d1 from distinct_test",
			),
			ExpHdrs: hdrs(
				hdr("d1", fldTypeDecimal2),
			),
			ExpRows: rows(
				row(pql.NewDecimal(1000, 2)),
				row(pql.NewDecimal(2000, 2)),
				row(pql.NewDecimal(3000, 2)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select distinct s1 from distinct_test",
			),
			ExpHdrs: hdrs(
				hdr("s1", fldTypeString),
			),
			ExpRows: rows(
				row(string("10")),
				row(string("20")),
				row(string("30")),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select distinct ss1 from distinct_test",
			),
			ExpHdrs: hdrs(
				hdr("ss1", fldTypeStringSet),
			),
			ExpRows: rows(
				row([]string{"10", "20", "30"}),
				row([]string{"11", "21", "31"}),
				row([]string{"12", "22", "32"}),
			),
			Compare:        CompareExactUnordered,
			SortStringKeys: true,
		},
		{
			SQLs: sqls(
				"select distinct ts1 from distinct_test",
			),
			ExpHdrs: hdrs(
				hdr("ts1", fldTypeTimestamp),
			),
			ExpRows: rows(
				row(knownTimestamp()),
			),
			Compare: CompareExactUnordered,
		},
	},
}
