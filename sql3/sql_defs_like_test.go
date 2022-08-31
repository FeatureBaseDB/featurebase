package sql3_test

//LIKE tests
var likeTests = tableTest{
	table: tbl(
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
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select _id like '%f_' from like_all_types",
			),
			expErr: "operator 'LIKE' incompatible with type 'ID'",
		},
		{
			sqls: sqls(
				"select i1 like '%f_' from like_all_types",
			),
			expErr: "operator 'LIKE' incompatible with type 'INT'",
		},
		{
			sqls: sqls(
				"select b1 like '%f_' from like_all_types",
			),
			expErr: "operator 'LIKE' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select d1 like '%f_' from like_all_types",
			),
			expErr: "operator 'LIKE' incompatible with type 'DECIMAL(2)'",
		},
		{
			sqls: sqls(
				"select id1 like '%f_' from like_all_types",
			),
			expErr: "operator 'LIKE' incompatible with type 'ID'",
		},
		{
			sqls: sqls(
				"select ids1 like '%f_' from like_all_types",
			),
			expErr: "operator 'LIKE' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select s1 like '%f_' from like_all_types",
			),
			expHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			expRows: rows(
				row(bool(true)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select ss1 like '%f_' from like_all_types",
			),
			expErr: "operator 'LIKE' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select t1 like '%f_' from like_all_types",
			),
			expErr: "operator 'LIKE' incompatible with type 'TIMESTAMP'",
		},
	},
}

//NOT LIKE tests
var notLikeTests = tableTest{
	table: tbl(
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
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select _id not like '%f_' from not_like_all_types",
			),
			expErr: "operator 'NOTLIKE' incompatible with type 'ID'",
		},
		{
			sqls: sqls(
				"select i1 not like '%f_' from not_like_all_types",
			),
			expErr: "operator 'NOTLIKE' incompatible with type 'INT'",
		},
		{
			sqls: sqls(
				"select b1 not like '%f_' from not_like_all_types",
			),
			expErr: "operator 'NOTLIKE' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select d1 not like '%f_' from not_like_all_types",
			),
			expErr: "operator 'NOTLIKE' incompatible with type 'DECIMAL(2)'",
		},
		{
			sqls: sqls(
				"select id1 not like '%f_' from not_like_all_types",
			),
			expErr: "operator 'NOTLIKE' incompatible with type 'ID'",
		},
		{
			sqls: sqls(
				"select ids1 not like '%f_' from not_like_all_types",
			),
			expErr: "operator 'NOTLIKE' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select s1 not like '%f_' from not_like_all_types",
			),
			expHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			expRows: rows(
				row(bool(false)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select ss1 not like '%f_' from not_like_all_types",
			),
			expErr: "operator 'NOTLIKE' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select t1 not like '%f_' from not_like_all_types",
			),
			expErr: "operator 'NOTLIKE' incompatible with type 'TIMESTAMP'",
		},
	},
}
