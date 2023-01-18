package defs

import "github.com/featurebasedb/featurebase/v3/pql"

var Unkeyed TableTest = unkeyed

var unkeyed = TableTest{
	name: "unkeyed",
	Table: tbl(
		"unkeyed",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("an_int", fldTypeInt, "min 0", "max 100"),
			srcHdr("an_id_set", fldTypeIDSet),
			srcHdr("an_id", fldTypeID),
			srcHdr("a_string", fldTypeString),
			srcHdr("a_string_set", fldTypeStringSet),
			srcHdr("a_decimal", fldTypeDecimal2),
		),
		srcRows(
			srcRow(int64(1), int64(11), []int64{11, 12, 13}, int64(101), "str1", []string{"a1", "b1", "c1"}, float64(123.45)),
			srcRow(int64(2), int64(22), []int64{21, 22, 23}, int64(201), "str2", []string{"a2", "b2", "c2"}, float64(234.56)),
			srcRow(int64(3), int64(33), []int64{31, 32, 33}, int64(301), "str3", []string{"a3", "b3", "c3"}, float64(345.67)),
			srcRow(int64(4), int64(44), []int64{41, 42, 43}, int64(401), "str4", []string{"a4", "b4", "c4"}, float64(456.78)),
		),
	),
	SQLTests: []SQLTest{
		{
			// Select all.
			name: "select-all",
			SQLs: sqls(
				"select * from unkeyed",
				"select _id, an_int, an_id_set, an_id, a_string, a_string_set, a_decimal from unkeyed",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("an_int", fldTypeInt),
				hdr("an_id_set", fldTypeIDSet),
				hdr("an_id", fldTypeID),
				hdr("a_string", fldTypeString),
				hdr("a_string_set", fldTypeStringSet),
				hdr("a_decimal", fldTypeDecimal2),
			),
			ExpRows: rows(
				row(int64(1), int64(11), []int64{11, 12, 13}, int64(101), "str1", []string{"a1", "b1", "c1"}, pql.NewDecimal(12345, 2)),
				row(int64(2), int64(22), []int64{21, 22, 23}, int64(201), "str2", []string{"a2", "b2", "c2"}, pql.NewDecimal(23456, 2)),
				row(int64(3), int64(33), []int64{31, 32, 33}, int64(301), "str3", []string{"a3", "b3", "c3"}, pql.NewDecimal(34567, 2)),
				row(int64(4), int64(44), []int64{41, 42, 43}, int64(401), "str4", []string{"a4", "b4", "c4"}, pql.NewDecimal(45678, 2)),
			),
			Compare:        CompareExactUnordered,
			SortStringKeys: true,
		},
		{
			// Select all with top.
			name: "select-all-with-top",
			SQLs: sqls(
				"select top(2) * from unkeyed",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("an_int", fldTypeInt),
				hdr("an_id_set", fldTypeIDSet),
				hdr("an_id", fldTypeID),
				hdr("a_string", fldTypeString),
				hdr("a_string_set", fldTypeStringSet),
				hdr("a_decimal", fldTypeDecimal2),
			),
			ExpRows: rows(
				row(int64(1), int64(11), []int64{11, 12, 13}, int64(101), "str1", []string{"a1", "b1", "c1"}, pql.NewDecimal(12345, 2)),
				row(int64(2), int64(22), []int64{21, 22, 23}, int64(201), "str2", []string{"a2", "b2", "c2"}, pql.NewDecimal(23456, 2)),
			),
			Compare:        CompareExactUnordered,
			SortStringKeys: true,
		},
		{
			// Select all with where on each field.
			name: "select-all-with-where",
			SQLs: sqls(
				"select * from unkeyed where an_int = 22",
				"select * from unkeyed where a_string = 'str2'",
				"select * from unkeyed where an_id = 201",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("an_int", fldTypeInt),
				hdr("an_id_set", fldTypeIDSet),
				hdr("an_id", fldTypeID),
				hdr("a_string", fldTypeString),
				hdr("a_string_set", fldTypeStringSet),
				hdr("a_decimal", fldTypeDecimal2),
			),
			ExpRows: rows(
				row(int64(2), int64(22), []int64{21, 22, 23}, int64(201), "str2", []string{"a2", "b2", "c2"}, pql.NewDecimal(23456, 2)),
			),
			Compare:        CompareExactOrdered,
			SortStringKeys: true,
		},
	},
	PQLTests: []PQLTest{
		{
			name:  "options",
			Table: "unkeyed",
			PQLs:  []string{"Options(Count(Row(an_id_set=1)), shards=[0])"},
			ExpHdrs: hdrs(
				hdr("count", fldTypeID),
			),
			ExpRows: rows(
				row(int64(0)),
			),
		},
	},
}
