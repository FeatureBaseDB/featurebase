package defs

// join tests
var bulkInsertTable = TableTest{
	name: "bulkInsertTable",
	Table: tbl(
		"bulktest",
		srcHdrs(
			srcHdr("_id", fldTypeString),
			srcHdr("id_col", fldTypeID),
			srcHdr("string_col", fldTypeString),
			srcHdr("int_col", fldTypeInt),
			srcHdr("decimal_col", fldTypeDecimal2),
			srcHdr("bool_col", fldTypeBool),
			srcHdr("time_col", fldTypeTimestamp),
			srcHdr("stringset_col", fldTypeStringSet),
			srcHdr("idset_col", fldTypeIDSet),
		),
	),
	SQLTests: nil,
}

var bulkInsert = TableTest{
	name: "bulkInsert",
	SQLTests: []SQLTest{
		{
			name: "timestamp-csv-text",
			SQLs: sqls(
				`BULK INSERT INTO 
					bulktest (_id, id_col, string_col, int_col,decimal_col, bool_col, time_col, stringset_col, idset_col)
					map (0 ID, 1 STRING, 2 INT, 3 DECIMAL(2), 4 BOOL, 5 TIMESTAMP, 6 STRINGSET, 7 IDSET)
					transform(@1, @0, @1, @2, @3, @4, @5, @6, @7)
					FROM x'1,TEST,-123,1.12,0,2013-07-15T01:18:46Z,stringset1, 1
					2,TEST2,321,31.2,1,2014-07-15T01:18:46Z,stringset1, 1
					1,TEST,-123,1.12,0,2013-07-15T01:18:46Z,stringset2, 2'
					with
						BATCHSIZE 10000
						format 'CSV'
						input 'STREAM';`,
			),
			ExpHdrs: hdrs(),
			ExpRows: rows(),
			Compare: CompareExactOrdered,
		},
		{
			name: "leftjoin",
			SQLs: sqls(
				"select time_col from bulktest where _id = 'TEST2';",
			),
			ExpHdrs: hdrs(
				hdr("time_col", fldTypeTimestamp),
			),
			ExpRows: rows(
				row(timestampFromString("2014-07-15T01:18:46Z")),
			),
			Compare: CompareExactOrdered,
		},
	},
}
