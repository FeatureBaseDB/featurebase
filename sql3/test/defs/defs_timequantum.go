package defs

// time quantum tests
var timeQuantumTest = TableTest{
	Table: tbl(
		"time_quantum_insert",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("i1", fldTypeInt, "min 0", "max 1000"),
			srcHdr("ss1", fldTypeStringSetQ, "timequantum 'YMD'"),
			srcHdr("ids1", fldTypeIDSetQ, "timequantum 'YMD'"),
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
			ExpErr: "an expression of type 'tuple(array(string))' cannot be assigned to type 'stringsetq'",
		},
		{
			SQLs: sqls(
				"insert into time_quantum_insert (_id, i1, ss1, ids1) values (1, 1, ['1'], {[1]})",
			),
			ExpErr: "an expression of type 'tuple(array(int))' cannot be assigned to type 'idsetq'",
		},
		{
			SQLs: sqls(
				"insert into time_quantum_insert (_id, i1, ss1, ids1) values (1, 1, {'notatimestamp', ['1']}, [1])",
			),
			ExpErr: "unable to convert 'notatimestamp' to type 'timestamp'",
		},
		{
			SQLs: sqls(
				"insert into time_quantum_insert (_id, i1, ss1, ids1) values (1, 1, ['1'], {'notatimestamp', [1]})",
			),
			ExpErr: "unable to convert 'notatimestamp' to type 'timestamp'",
		},
		{
			SQLs: sqls(
				"insert into time_quantum_insert (_id, i1, ss1, ids1) values (1, 1, {'2022-01-01T00:00:00Z', [1]}, {[1]})",
			),
			ExpErr: "an expression of type 'tuple(string, array(int))' cannot be assigned to type 'stringsetq'",
		},
		{
			SQLs: sqls(
				"insert into time_quantum_insert (_id, i1, ss1, ids1) values (1, 1, ['1'], {'2022-01-01T00:00:00Z', ['1']})",
			),
			ExpErr: "an expression of type 'tuple(string, array(string))' cannot be assigned to type 'idsetq'",
		},
		{
			SQLs: sqls(
				"insert into time_quantum_insert (_id, i1, ss1, ids1) values (1, 1, '1', {[1]})",
			),
			ExpErr: "an expression of type 'string' cannot be assigned to type 'stringsetq'",
		},
		{
			SQLs: sqls(
				"insert into time_quantum_insert (_id, i1, ss1, ids1) values (1, 1, ['1'], 1)",
			),
			ExpErr: "an expression of type 'int' cannot be assigned to type 'idsetq'",
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
				"insert into time_quantum_insert(_id, i1, ss1, ids1) values (1, 3, ['test1'], [1])",
			),
			ExpHdrs: hdrs(),
			ExpRows: rows(),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"insert into time_quantum_insert(_id, i1, ss1, ids1) values (1, 3, {1676649734, ['test2']}, {1676649734, [2]})",
			),
			ExpHdrs: hdrs(),
			ExpRows: rows(),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"insert into time_quantum_insert(_id, i1, ss1, ids1) values (1, 3, {'2022-01-01T00:00:00Z', ['test3']}, {'2022-01-01T00:00:00Z', [3]})",
			),
			ExpHdrs: hdrs(),
			ExpRows: rows(),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"insert into time_quantum_insert(_id, i1, ss1, ids1) values (1, 3, {'2022-01-02T00:00:00Z', ['test4']}, {'2022-01-01T00:00:00Z', [4]})",
			),
			ExpHdrs: hdrs(),
			ExpRows: rows(),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"insert into time_quantum_insert(_id, i1, ss1, ids1) values (1, 3, {'2022-01-03T00:00:00Z', ['test5']}, {'2022-01-01T00:00:00Z', [5]})",
			),
			ExpHdrs: hdrs(),
			ExpRows: rows(),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select a._id, a.ss1 from time_quantum_insert a where rangeq(a.ss1, '2022-01-02T00:00:00Z')",
			),
			ExpErr: "'rangeq': count of formal parameters (3) does not match count of actual parameters (2)",
		},
		{
			SQLs: sqls(
				"select a._id, a.ss1 from time_quantum_insert a where rangeq(a.ss1, null, null)",
			),
			ExpErr: "calling ranqeq() 'from' and 'to' parameters cannot both be null",
		},
		{
			SQLs: sqls(
				"select a._id, a.ss1 from time_quantum_insert a where rangeq(a.i1, null, null)",
			),
			ExpErr: "time quantum expression expected",
		},
		{
			SQLs: sqls(
				"select a._id, a.ss1, rangeq(a.ss1, '2022-01-02T00:00:00Z', null) from time_quantum_insert a",
			),
			ExpErr: "calling ranqeq() usage invalid",
		},
		{
			name: "stringset-rangeq",
			SQLs: sqls(
				"select a._id, a.ss1 from time_quantum_insert a where rangeq(a.ss1, '2022-01-02T00:00:00Z', null)",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("ss1", fldTypeStringSetQ),
			),
			ExpRows: rows(
				row(int64(1), []string{"1", "test1", "test2"}),
			),
			Compare: CompareExactUnordered,
		},
		{
			name: "idset-rangeq",
			SQLs: sqls(
				"select a._id, a.ids1 from time_quantum_insert a where rangeq(a.ids1, '2022-01-02T00:00:00Z', null)",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("ids1", fldTypeIDSetQ),
			),
			ExpRows: rows(
				row(int64(1), []int64{1, 2}),
			),
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
				"select _id not like '%f_' from timeQuantumQueryTest",
			),
			ExpErr: "operator 'NOTLIKE' incompatible with type 'id'",
		},
	},
}
