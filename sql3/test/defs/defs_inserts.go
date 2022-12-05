package defs

var insertTest = TableTest{
	Table: tbl(
		"testinsert",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("a", fldTypeInt, "min 0", "max 1000"),
			srcHdr("b", fldTypeInt, "min 0", "max 1000"),
			srcHdr("s", fldTypeString),
			srcHdr("bl", fldTypeBool),
			srcHdr("d", fldTypeDecimal2),
			srcHdr("event", fldTypeStringSet),
			srcHdr("ievent", fldTypeIDSet),
		),
	),
	SQLTests: []SQLTest{
		{
			// Insert
			SQLs: sqls(
				"insert into testinsert (_id, a, b, s, bl, d, event, ievent) values (4, 40, 400, 'foo', false, 10.12, ['A', 'B', 'C'], [1, 2, 3])",
			),
			ExpHdrs: hdrs(),
			ExpRows: rows(),
			Compare: CompareExactUnordered,
		},
		{
			// Replace
			SQLs: sqls(
				"replace into testinsert (_id, a, b, s, bl, d, event, ievent) values (4, 40, 400, 'foo', false, 10.12, ['A', 'B', 'C'], [1, 2, 3])",
			),
			ExpHdrs: hdrs(),
			ExpRows: rows(),
			Compare: CompareExactUnordered,
		},
		{
			// Insert multiple tuples
			SQLs: sqls(
				"insert into testinsert (_id, a, b, s, bl, d, event, ievent) values (4, 40, 400, 'foo', false, 10.12, ['A', 'B', 'C'], [1, 2, 3]), (5, 50, 500, 'var', true, 20.24, ['X', 'Y', 'Z'], [4, 5, 6])",
			),
			ExpHdrs: hdrs(),
			ExpRows: rows(),
			Compare: CompareExactUnordered,
		},
		{
			// Insert with nulls
			SQLs: sqls(
				"insert into testinsert (_id, a, b, s, bl, d, event, ievent) values (5, null, null, null, null, null, null, null)",
				"insert into testinsert (_id, a, b, s, bl, d, event, ievent) values (6, 1, null, null, null, null, null, null)",
			),
			ExpHdrs: hdrs(),
			ExpRows: rows(),
			Compare: CompareExactUnordered,
		},
		{
			// Insert with exprs
			SQLs: sqls(
				"insert into testinsert (_id, a, b, s, bl, d, event, ievent) values (4, 40*10, 400+1, 'foo' || 'bar', 1 > 2, 10.12 + 3.1, ['A', 'B', 'C'], [1, 2, 3])",
			),
			ExpHdrs: hdrs(),
			ExpRows: rows(),
			Compare: CompareExactUnordered,
		},
		{
			// InsertBadTable
			SQLs: sqls(
				"insert into ifoo (a, b) values (1, 2)",
			),
			ExpErr: "table 'ifoo' not found",
		},
		{
			// InsertBadColumn
			SQLs: sqls(
				"insert into testinsert (c, b) values (1, 2)",
			),
			ExpErr: "column 'c' not found",
		},
		{
			// InsertDupeColumn
			SQLs: sqls(
				"insert into testinsert (a, a, b) values (1, 2)",
			),
			ExpErr: "duplicate column 'a'",
		},
		{
			// InsertMismatchColumnValues
			SQLs: sqls(
				"insert into testinsert (_id, a, b) values (1)",
			),
			ExpErr: "mismatch in the count of expressions and target columns",
		},
		{
			// InsertHandleMissingColumns
			SQLs: sqls(
				"insert into testinsert values (4, 40, 400)",
			),
			ExpErr: "mismatch in the count of expressions and target columns",
		},
		{
			// InsertHandleMissingId
			SQLs: sqls(
				"insert into testinsert (a, b) values (1, 2)",
			),
			ExpErr: "insert column list must have '_id' column specified",
		},
		{
			// InsertHandleMissingIdPlusOneOther
			SQLs: sqls(
				"insert into testinsert (_id) values (1)",
			),
			ExpErr: "insert column list must have at least one non '_id' column specified",
		},
		{
			// InsertSetsTypeError
			SQLs: sqls(
				"insert into testinsert (_id, a, event) values (4, 40, [101, 150])",
			),
			ExpErr: "an expression of type 'IDSET' cannot be assigned to type 'STRINGSET'",
		},
		{
			// InsertSetsTypeError2
			SQLs: sqls(
				"insert into testinsert (_id, a, ievent) values (4, 40, ['POST', 'GET'])",
			),
			ExpErr: "an expression of type 'STRINGSET' cannot be assigned to type 'IDSET'",
		},
	},
}
