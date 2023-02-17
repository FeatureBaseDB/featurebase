package defs

// time quantum insert tests
var timeQuantumInsertTest = TableTest{
	Table: tbl(
		"time_quantum_insert",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("i1", fldTypeInt, "min 0", "max 1000"),
			srcHdr("ss1", fldTypeStringSet, "timequantum 'YMD'"),
			srcHdr("ids1", fldTypeIDSet, "timequantum 'YMD'"),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"insert into time_quantum_insert (_id, i1, ss1, ids1) values (1, 1, ['1'], [1])",
			),
			ExpHdrs: hdrs(),
			ExpRows: rows(),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"insert into time_quantum_insert (_id, i1, ss1, ids1) values (1, 1, {['1']}, {[1]})",
			),
			ExpErr: "an expression of type 'tuple(stringset)' cannot be assigned to type 'stringset'",
		},
		{
			SQLs: sqls(
				"insert into time_quantum_insert (_id, i1, ss1, ids1) values (1, 1, {1676649734, ['1']}, {1676649734, [1]})",
			),
			ExpHdrs: hdrs(),
			ExpRows: rows(),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"insert into time_quantum_insert (_id, i1, ss1, ids1) values (1, 1, {'2022-01-01T00:00:00Z', ['1']}, {'2022-01-01T00:00:00Z', [1]})",
			),
			ExpHdrs: hdrs(),
			ExpRows: rows(),
			Compare: CompareExactUnordered,
		},
	},
}

// time quantum query tests
var timeQuantumQueryTest = TableTest{
	Table: tbl(
		"timeQuantumQueryTest",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("i1", fldTypeInt, "min 0", "max 1000"),
			srcHdr("b1", fldTypeBool),
			srcHdr("d1", fldTypeDecimal2),
			srcHdr("id1", fldTypeID),
			srcHdr("ids1", fldTypeIDSet),
			srcHdr("s1", fldTypeString),
			srcHdr("ss1", fldTypeStringSet),
			srcHdr("t1", fldTypeTimestamp),
		),
		srcRows(
			srcRow(int64(1), int64(1000), bool(true), float64(12.34), int64(20), []int64{101, 102}, string("foo"), []string{"101", "102"}, knownTimestamp()),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select _id not like '%f_' from not_like_all_types",
			),
			ExpErr: "operator 'NOTLIKE' incompatible with type 'id'",
		},
	},
}
