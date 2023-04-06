// Copyright 2023 Molecula Corp. All rights reserved.

package defs

// copy
var copyTable = TableTest{
	name: "copyTable",
	Table: tbl(
		"copytest",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("id_col", fldTypeID),
			srcHdr("string_col", fldTypeString),
			srcHdr("int_col", fldTypeInt),
			srcHdr("decimal_col", fldTypeDecimal2),
			srcHdr("bool_col", fldTypeBool),
			srcHdr("time_col", fldTypeTimestamp),
			srcHdr("stringset_col", fldTypeStringSet),
			srcHdr("idset_col", fldTypeIDSet),
		),
		srcRows(
			srcRow(int64(1), int64(10), string("foo"), int64(10), float64(10), bool(false), knownTimestamp(), []string{"foo", "bar"}, []int64{1, 2}),
			srcRow(int64(2), int64(11), string("foo1"), int64(11), float64(11), bool(true), knownTimestamp(), []string{"foo1", "bar1"}, []int64{11, 21}),
			srcRow(int64(3), int64(12), string("foo2"), int64(12), float64(12), bool(false), knownTimestamp(), []string{"foo2", "bar2"}, []int64{12, 22}),
			srcRow(int64(4), int64(13), string("foo3"), int64(13), float64(13), bool(true), knownTimestamp(), []string{"foo3", "bar3"}, []int64{13, 23}),
		),
	),
	SQLTests: nil,
}

var copyTests = TableTest{
	name: "copyTests",
	SQLTests: []SQLTest{
		{
			name: "copy-no-table-to-table",
			SQLs: sqls(
				`copy foo to bar;`,
			),
			ExpErr: "table or view 'foo' not found",
		},
		{
			name: "copy-table-to-table-same-name",
			SQLs: sqls(
				`copy copytest to copytest;`,
			),
			ExpErr: "already exists",
		},
		{
			name: "copy-table-to-table",
			SQLs: sqls(
				`copy copytest to copytesttwo;`,
			),
			ExpHdrs: hdrs(),
			ExpRows: rows(),
			Compare: CompareExactOrdered,
		},
	},
}
