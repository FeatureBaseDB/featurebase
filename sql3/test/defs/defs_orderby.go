package defs

var orderByTests = TableTest{
	name: "orderByTests",
	Table: tbl(
		"order_by_test",
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
			srcRow(int64(1), int64(44), []int64{11, 12, 13}, int64(101), "str1", []string{"a1", "b1", "c1"}, float64(123.45)),
			srcRow(int64(2), int64(33), []int64{21, 22, 23}, int64(201), "str2", []string{"a2", "b2", "c2"}, float64(234.56)),
			srcRow(int64(3), int64(21), []int64{31, 32, 33}, int64(301), "str3", []string{"a3", "b3", "c3"}, float64(345.67)),
			srcRow(int64(4), int64(10), []int64{41, 42, 43}, int64(401), "str4", []string{"a4", "b4", "c4"}, float64(456.78)),
		),
	),
	SQLTests: []SQLTest{
		{
			name: "order-by-stringset",
			SQLs: sqls(
				"select * from order_by_test order by a_string_set asc",
			),
			ExpErr: "unable to sort a column of type 'stringset'",
		},
		{
			name: "order-by-idset",
			SQLs: sqls(
				"select * from order_by_test order by an_id_set asc",
			),
			ExpErr: "unable to sort a column of type 'idset'",
		},
		{
			SQLs: sqls(
				"select an_int from order_by_test order by an_id asc",
			),
			ExpHdrs: hdrs(
				hdr("an_int", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(44)),
				row(int64(33)),
				row(int64(21)),
				row(int64(10)),
			),
			Compare: CompareExactOrdered,
		},
		{
			SQLs: sqls(
				"select an_int, an_id from order_by_test order by a_decimal asc",
			),
			ExpHdrs: hdrs(
				hdr("an_int", fldTypeInt),
				hdr("an_id", fldTypeID),
			),
			ExpRows: rows(
				row(int64(44), int64(101)),
				row(int64(33), int64(201)),
				row(int64(21), int64(301)),
				row(int64(10), int64(401)),
			),
			Compare: CompareExactOrdered,
		},
		{
			SQLs: sqls(
				"select an_int + 1 as foo, an_id from order_by_test order by foo asc, a_decimal asc",
			),
			ExpHdrs: hdrs(
				hdr("foo", fldTypeInt),
				hdr("an_id", fldTypeID),
			),
			ExpRows: rows(
				row(int64(11), int64(401)),
				row(int64(22), int64(301)),
				row(int64(34), int64(201)),
				row(int64(45), int64(101)),
			),
			Compare: CompareExactOrdered,
		},
		{
			SQLs: sqls(
				"select an_int from order_by_test order by an_int asc",
			),
			ExpHdrs: hdrs(
				hdr("an_int", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(10)),
				row(int64(21)),
				row(int64(33)),
				row(int64(44)),
			),
			Compare: CompareExactOrdered,
		},
		{
			SQLs: sqls(
				"select an_int as foo from order_by_test order by foo asc",
			),
			ExpHdrs: hdrs(
				hdr("foo", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(10)),
				row(int64(21)),
				row(int64(33)),
				row(int64(44)),
			),
			Compare: CompareExactOrdered,
		},
		{
			SQLs: sqls(
				"select an_int as foo from order_by_test order by 1 asc",
			),
			ExpHdrs: hdrs(
				hdr("foo", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(10)),
				row(int64(21)),
				row(int64(33)),
				row(int64(44)),
			),
			Compare: CompareExactOrdered,
		},
		{
			SQLs: sqls(
				"select an_int + 1 from order_by_test order by 1 asc",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(11)),
				row(int64(22)),
				row(int64(34)),
				row(int64(45)),
			),
			Compare: CompareExactOrdered,
		},
		{
			SQLs: sqls(
				"select an_int + 1 as bar from order_by_test order by bar desc",
			),
			ExpHdrs: hdrs(
				hdr("bar", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(45)),
				row(int64(34)),
				row(int64(22)),
				row(int64(11)),
			),
			Compare: CompareExactOrdered,
		},
	},
}
