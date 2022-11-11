package defs

var keyedInsertTest = TableTest{
	name: "keyedinsert",
	Table: tbl(
		"testkeyedinsert",
		srcHdrs(
			srcHdr("_id", fldTypeString),
			srcHdr("a", fldTypeInt, "min 0", "max 1000"),
			srcHdr("b", fldTypeInt, "min 0", "max 1000"),
			srcHdr("s", fldTypeString),
			srcHdr("bl", fldTypeBool),
			srcHdr("d", fldTypeDecimal2),
			srcHdr("event", fldTypeStringSet),
			srcHdr("ievent", fldTypeIDSet),
		),
		nil,
	),
	SQLTests: []SQLTest{
		{
			// Insert
			SQLs: sqls(
				"insert into testkeyedinsert (_id, a, b, s, bl, d, event, ievent) values ('four', 40, 400, 'foo', false, 10.12, ['A', 'B', 'C'], [1, 2, 3])",
			),
			ExpHdrs: hdrs(),
			ExpRows: rows(),
			Compare: CompareExactUnordered,
		},
	},
}
