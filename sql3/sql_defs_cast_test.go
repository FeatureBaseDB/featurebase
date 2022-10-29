package sql3_test

import (
	"time"

	"github.com/featurebasedb/featurebase/v3/pql"
)

func expectedCastTime() time.Time {
	return time.Unix(1000, 0).UTC()
}

var castIntLiteral = tableTest{
	table: tbl(
		"",
		nil,
		nil,
	),
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select cast(1 as int)",
			),
			expHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			expRows: rows(
				row(int64(1)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select cast(1 as bool)",
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
				"select cast(0 as bool)",
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
				"select cast(1 as decimal(2))",
			),
			expHdrs: hdrs(
				hdr("", fldTypeDecimal2),
			),
			expRows: rows(
				row(pql.NewDecimal(100, 2)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select cast(1 as id)",
			),
			expHdrs: hdrs(
				hdr("", fldTypeID),
			),
			expRows: rows(
				row(int64(1)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select cast(1 as idset)",
			),
			expErr: "'INT' cannot be cast to 'IDSET'",
		},
		{
			sqls: sqls(
				"select cast(1 as string)",
			),
			expHdrs: hdrs(
				hdr("", fldTypeString),
			),
			expRows: rows(
				row(string("1")),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select cast(1 as stringset)",
			),
			expErr: "'INT' cannot be cast to 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select cast(1000 as timestamp)",
			),
			expHdrs: hdrs(
				hdr("", fldTypeTimestamp),
			),
			expRows: rows(
				row(time.Time(expectedCastTime())),
			),
			compare: compareExactUnordered,
		},
	},
}

var castInt = tableTest{
	table: tbl(
		"cast_int",
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
				"select _id, cast(i1 as int) from cast_int",
			),
			expHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeInt),
			),
			expRows: rows(
				row(int64(1), int64(1000)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select _id, cast(i1 as bool) from cast_int",
			),
			expHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeBool),
			),
			expRows: rows(
				row(int64(1), bool(true)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select _id, cast(i1 as decimal(2)) from cast_int",
			),
			expHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeDecimal2),
			),
			expRows: rows(
				row(int64(1), pql.NewDecimal(100000, 2)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select _id, cast(i1 as id) from cast_int",
			),
			expHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeID),
			),
			expRows: rows(
				row(int64(1), int64(1000)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select _id, cast(i1 as idset) from cast_int",
			),
			expErr: "'INT' cannot be cast to 'IDSET'",
		},
		{
			sqls: sqls(
				"select _id, cast(i1 as string) from cast_int",
			),
			expHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeString),
			),
			expRows: rows(
				row(int64(1), string("1000")),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select _id, cast(i1 as stringset) from cast_int",
			),
			expErr: "'INT' cannot be cast to 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select _id, cast(i1 as timestamp) from cast_int",
			),
			expHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeTimestamp),
			),
			expRows: rows(
				row(int64(1), time.Time(expectedCastTime())),
			),
			compare: compareExactUnordered,
		},
	},
}

var castBool = tableTest{
	table: tbl(
		"cast_bool",
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
				"select _id, cast(b1 as int) from cast_bool",
			),
			expHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeInt),
			),
			expRows: rows(
				row(int64(1), int64(1)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select _id, cast(b1 as bool) from cast_bool",
			),
			expHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeBool),
			),
			expRows: rows(
				row(int64(1), bool(true)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select _id, cast(b1 as decimal(2)) from cast_bool",
			),
			expErr: "'BOOL' cannot be cast to 'DECIMAL(2)'",
		},
		{
			sqls: sqls(
				"select _id, cast(b1 as id) from cast_bool",
			),
			expErr: "'BOOL' cannot be cast to 'ID'",
		},
		{
			sqls: sqls(
				"select _id, cast(b1 as idset) from cast_bool",
			),
			expErr: "'BOOL' cannot be cast to 'IDSET'",
		},
		{
			sqls: sqls(
				"select _id, cast(b1 as string) from cast_bool",
			),
			expHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeString),
			),
			expRows: rows(
				row(int64(1), string("true")),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select _id, cast(b1 as stringset) from cast_bool",
			),
			expErr: "'BOOL' cannot be cast to 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select _id, cast(b1 as timestamp) from cast_bool",
			),
			expErr: "'BOOL' cannot be cast to 'TIMESTAMP'",
		},
	},
}

var castDecimal = tableTest{
	table: tbl(
		"cast_dec",
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
				"select _id, cast(d1 as int) from cast_dec",
			),
			expErr: "'DECIMAL(2)' cannot be cast to 'INT'",
		},
		{
			sqls: sqls(
				"select _id, cast(d1 as bool) from cast_dec",
			),
			expErr: "'DECIMAL(2)' cannot be cast to 'BOOL'",
		},
		{
			sqls: sqls(
				"select _id, cast(d1 as decimal(2)) from cast_dec",
			),
			expHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeDecimal2),
			),
			expRows: rows(
				row(int64(1), pql.NewDecimal(1234, 2)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select _id, cast(d1 as id) from cast_dec",
			),
			expErr: "'DECIMAL(2)' cannot be cast to 'ID'",
		},
		{
			sqls: sqls(
				"select _id, cast(d1 as idset) from cast_dec",
			),
			expErr: "'DECIMAL(2)' cannot be cast to 'IDSET'",
		},
		{
			sqls: sqls(
				"select _id, cast(d1 as string) from cast_dec",
			),
			expHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeString),
			),
			expRows: rows(
				row(int64(1), string("12.34")),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select _id, cast(d1 as stringset) from cast_dec",
			),
			expErr: "'DECIMAL(2)' cannot be cast to 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select _id, cast(d1 as timestamp) from cast_dec",
			),
			expErr: "'DECIMAL(2)' cannot be cast to 'TIMESTAMP'",
		},
	},
}

var castID = tableTest{
	table: tbl(
		"cast_id",
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
				"select _id, cast(id1 as int) from cast_id",
			),
			expHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeInt),
			),
			expRows: rows(
				row(int64(1), int64(20)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select _id, cast(id1 as bool) from cast_id",
			),
			expHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeBool),
			),
			expRows: rows(
				row(int64(1), bool(true)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select _id, cast(id1 as decimal(2)) from cast_id",
			),
			expHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeDecimal2),
			),
			expRows: rows(
				row(int64(1), pql.NewDecimal(2000, 2)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select _id, cast(id1 as id) from cast_id",
			),
			expHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeID),
			),
			expRows: rows(
				row(int64(1), int64(20)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select _id, cast(id1 as idset) from cast_id",
			),
			expErr: "'ID' cannot be cast to 'IDSET'",
		},
		{
			sqls: sqls(
				"select _id, cast(id1 as string) from cast_id",
			),
			expErr: "'ID' cannot be cast to 'STRING'",
		},
		{
			sqls: sqls(
				"select _id, cast(id1 as stringset) from cast_id",
			),
			expErr: "'ID' cannot be cast to 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select _id, cast(id1 as timestamp) from cast_id",
			),
			expErr: "'ID' cannot be cast to 'TIMESTAMP'",
		},
	},
}

var castIDSet = tableTest{
	table: tbl(
		"cast_ids",
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
				"select _id, cast(ids1 as int) from cast_ids",
			),
			expErr: "'IDSET' cannot be cast to 'INT'",
		},
		{
			sqls: sqls(
				"select _id, cast(ids1 as bool) from cast_ids",
			),
			expErr: "'IDSET' cannot be cast to 'BOOL'",
		},
		{
			sqls: sqls(
				"select _id, cast(ids1 as decimal(2)) from cast_ids",
			),
			expErr: "'IDSET' cannot be cast to 'DECIMAL(2)'",
		},
		{
			sqls: sqls(
				"select _id, cast(ids1 as id) from cast_ids",
			),
			expErr: "'IDSET' cannot be cast to 'ID'",
		},
		{
			sqls: sqls(
				"select _id, cast(ids1 as idset) from cast_ids",
			),
			expHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeIDSet),
			),
			expRows: rows(
				row(int64(1), []int64{101, 102}),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select _id, cast(ids1 as string) from cast_ids",
			),
			expHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeString),
			),
			expRows: rows(
				row(int64(1), string("[101 102]")),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select _id, cast(ids1 as stringset) from cast_ids",
			),
			expErr: "'IDSET' cannot be cast to 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select _id, cast(ids1 as timestamp) from cast_ids",
			),
			expErr: "'IDSET' cannot be cast to 'TIMESTAMP'",
		},
	},
}

var castString = tableTest{
	table: tbl(
		"cast_string",
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
				"select _id, cast(s1 as int) from cast_string",
			),
			expErr: "'foo' cannot be cast to 'INT'",
		},
		{
			sqls: sqls(
				"select _id, cast('11' as int) from cast_string",
			),
			expHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeInt),
			),
			expRows: rows(
				row(int64(1), int64(11)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select _id, cast(s1 as bool) from cast_string",
			),
			expErr: "'foo' cannot be cast to 'BOOL'",
		},
		{
			sqls: sqls(
				"select _id, cast('true' as bool) from cast_string",
			),
			expHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeBool),
			),
			expRows: rows(
				row(int64(1), bool(true)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select _id, cast(s1 as decimal(2)) from cast_string",
			),
			expErr: "'foo' cannot be cast to 'DECIMAL(2)'",
		},
		{
			sqls: sqls(
				"select _id, cast('12.34' as decimal(2)) from cast_string",
			),
			expHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeDecimal2),
			),
			expRows: rows(
				row(int64(1), pql.NewDecimal(1234, 2)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select _id, cast(s1 as id) from cast_string",
			),
			expErr: "'foo' cannot be cast to 'ID'",
		},
		{
			sqls: sqls(
				"select _id, cast('11' as id) from cast_string",
			),
			expHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeID),
			),
			expRows: rows(
				row(int64(1), int64(11)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select _id, cast(s1 as idset) from cast_string",
			),
			expErr: "'STRING' cannot be cast to 'IDSET'",
		},
		{
			sqls: sqls(
				"select _id, cast(s1 as string) from cast_string",
			),
			expHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeString),
			),
			expRows: rows(
				row(int64(1), string("foo")),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select _id, cast(s1 as stringset) from cast_string",
			),
			expErr: "'STRING' cannot be cast to 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select _id, cast(s1 as timestamp) from cast_string",
			),
			expErr: "'foo' cannot be cast to 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select _id, cast('1970-01-01T00:16:40Z' as timestamp) from cast_string",
			),
			expHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeTimestamp),
			),
			expRows: rows(
				row(int64(1), time.Time(expectedCastTime())),
			),
			compare: compareExactUnordered,
		},
	},
}

var castStringSet = tableTest{
	table: tbl(
		"cast_ss",
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
				"select _id, cast(ss1 as int) from cast_ss",
			),
			expErr: "'STRINGSET' cannot be cast to 'INT'",
		},
		{
			sqls: sqls(
				"select _id, cast(ss1 as bool) from cast_ss",
			),
			expErr: "'STRINGSET' cannot be cast to 'BOOL'",
		},
		{
			sqls: sqls(
				"select _id, cast(ss1 as decimal(2)) from cast_ss",
			),
			expErr: "'STRINGSET' cannot be cast to 'DECIMAL(2)'",
		},
		{
			sqls: sqls(
				"select _id, cast(ss1 as id) from cast_ss",
			),
			expErr: "'STRINGSET' cannot be cast to 'ID'",
		},
		{
			sqls: sqls(
				"select _id, cast(ss1 as idset) from cast_ss",
			),
			expErr: "'STRINGSET' cannot be cast to 'IDSET'",
		},
		{
			sqls: sqls(
				"select _id, cast(ss1 as string) from cast_ss",
			),
			expHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeString),
			),
			expRows: rows(
				row(int64(1), string(`["101","102"]`)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select _id, cast(ss1 as stringset) from cast_ss",
			),
			expHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeStringSet),
			),
			expRows: rows(
				row(int64(1), []string{"101", "102"}),
			),
			compare:        compareExactUnordered,
			sortStringKeys: true,
		},
		{
			sqls: sqls(
				"select _id, cast(ss1 as timestamp) from cast_ss",
			),
			expErr: "'STRINGSET' cannot be cast to 'TIMESTAMP'",
		},
	},
}

var castTimestamp = tableTest{
	table: tbl(
		"cast_ts",
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
				"select _id, cast(t1 as int) from cast_ts",
			),
			expHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeInt),
			),
			expRows: rows(
				row(int64(1), int64(1351807721)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select _id, cast(t1 as bool) from cast_ts",
			),
			expErr: "'TIMESTAMP' cannot be cast to 'BOOL'",
		},
		{
			sqls: sqls(
				"select _id, cast(t1 as decimal(2)) from cast_ts",
			),
			expErr: "'TIMESTAMP' cannot be cast to 'DECIMAL(2)'",
		},
		{
			sqls: sqls(
				"select _id, cast(t1 as id) from cast_ts",
			),
			expErr: "'TIMESTAMP' cannot be cast to 'ID'",
		},
		{
			sqls: sqls(
				"select _id, cast(t1 as idset) from cast_ts",
			),
			expErr: "'TIMESTAMP' cannot be cast to 'IDSET'",
		},
		{
			sqls: sqls(
				"select _id, cast(t1 as string) from cast_ts",
			),
			expHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeString),
			),
			expRows: rows(
				row(int64(1), string("2012-11-01T22:08:41Z")),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select _id, cast(t1 as stringset) from cast_ts",
			),
			expErr: "'TIMESTAMP' cannot be cast to 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select _id, cast(t1 as timestamp) from cast_ts",
			),
			expHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeTimestamp),
			),
			expRows: rows(
				row(int64(1), knownTimestamp()),
			),
			compare: compareExactUnordered,
		},
	},
}
