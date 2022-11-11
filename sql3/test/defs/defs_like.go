package defs

// LIKE tests
var likeTests = TableTest{
	Table: tbl(
		"like_all_types",
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
				"select _id like '%f_' from like_all_types",
			),
			ExpErr: "operator 'LIKE' incompatible with type 'ID'",
		},
		{
			SQLs: sqls(
				"select i1 like '%f_' from like_all_types",
			),
			ExpErr: "operator 'LIKE' incompatible with type 'INT'",
		},
		{
			SQLs: sqls(
				"select b1 like '%f_' from like_all_types",
			),
			ExpErr: "operator 'LIKE' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select d1 like '%f_' from like_all_types",
			),
			ExpErr: "operator 'LIKE' incompatible with type 'DECIMAL(2)'",
		},
		{
			SQLs: sqls(
				"select id1 like '%f_' from like_all_types",
			),
			ExpErr: "operator 'LIKE' incompatible with type 'ID'",
		},
		{
			SQLs: sqls(
				"select ids1 like '%f_' from like_all_types",
			),
			ExpErr: "operator 'LIKE' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select s1 like '%f_' from like_all_types",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			ExpRows: rows(
				row(bool(true)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select ss1 like '%f_' from like_all_types",
			),
			ExpErr: "operator 'LIKE' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select t1 like '%f_' from like_all_types",
			),
			ExpErr: "operator 'LIKE' incompatible with type 'TIMESTAMP'",
		},
	},
}

// NOT LIKE tests
var notLikeTests = TableTest{
	Table: tbl(
		"not_like_all_types",
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
			ExpErr: "operator 'NOTLIKE' incompatible with type 'ID'",
		},
		{
			SQLs: sqls(
				"select i1 not like '%f_' from not_like_all_types",
			),
			ExpErr: "operator 'NOTLIKE' incompatible with type 'INT'",
		},
		{
			SQLs: sqls(
				"select b1 not like '%f_' from not_like_all_types",
			),
			ExpErr: "operator 'NOTLIKE' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select d1 not like '%f_' from not_like_all_types",
			),
			ExpErr: "operator 'NOTLIKE' incompatible with type 'DECIMAL(2)'",
		},
		{
			SQLs: sqls(
				"select id1 not like '%f_' from not_like_all_types",
			),
			ExpErr: "operator 'NOTLIKE' incompatible with type 'ID'",
		},
		{
			SQLs: sqls(
				"select ids1 not like '%f_' from not_like_all_types",
			),
			ExpErr: "operator 'NOTLIKE' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select s1 not like '%f_' from not_like_all_types",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			ExpRows: rows(
				row(bool(false)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select ss1 not like '%f_' from not_like_all_types",
			),
			ExpErr: "operator 'NOTLIKE' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select t1 not like '%f_' from not_like_all_types",
			),
			ExpErr: "operator 'NOTLIKE' incompatible with type 'TIMESTAMP'",
		},
	},
}
