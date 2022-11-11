package defs

import (
	"time"

	"github.com/featurebasedb/featurebase/v3/pql"
)

func expectedCastTime() time.Time {
	return time.Unix(1000, 0).UTC()
}

var castIntLiteral = TableTest{
	Table: tbl(
		"",
		nil,
		nil,
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select cast(1 as int)",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(1)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select cast(1 as bool)",
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
				"select cast(0 as bool)",
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
				"select cast(1 as decimal(2))",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeDecimal2),
			),
			ExpRows: rows(
				row(pql.NewDecimal(100, 2)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select cast(1 as id)",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeID),
			),
			ExpRows: rows(
				row(int64(1)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select cast(1 as idset)",
			),
			ExpErr: "'INT' cannot be cast to 'IDSET'",
		},
		{
			SQLs: sqls(
				"select cast(1 as string)",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(string("1")),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select cast(1 as stringset)",
			),
			ExpErr: "'INT' cannot be cast to 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select cast(1000 as timestamp)",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeTimestamp),
			),
			ExpRows: rows(
				row(time.Time(expectedCastTime())),
			),
			Compare: CompareExactUnordered,
		},
	},
}

var castInt = TableTest{
	Table: tbl(
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
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select _id, cast(i1 as int) from cast_int",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(1), int64(1000)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id, cast(i1 as bool) from cast_int",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeBool),
			),
			ExpRows: rows(
				row(int64(1), bool(true)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id, cast(i1 as decimal(2)) from cast_int",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeDecimal2),
			),
			ExpRows: rows(
				row(int64(1), pql.NewDecimal(100000, 2)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id, cast(i1 as id) from cast_int",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeID),
			),
			ExpRows: rows(
				row(int64(1), int64(1000)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id, cast(i1 as idset) from cast_int",
			),
			ExpErr: "'INT' cannot be cast to 'IDSET'",
		},
		{
			SQLs: sqls(
				"select _id, cast(i1 as string) from cast_int",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(int64(1), string("1000")),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id, cast(i1 as stringset) from cast_int",
			),
			ExpErr: "'INT' cannot be cast to 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select _id, cast(i1 as timestamp) from cast_int",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeTimestamp),
			),
			ExpRows: rows(
				row(int64(1), time.Time(expectedCastTime())),
			),
			Compare: CompareExactUnordered,
		},
	},
}

var castBool = TableTest{
	Table: tbl(
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
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select _id, cast(b1 as int) from cast_bool",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(1), int64(1)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id, cast(b1 as bool) from cast_bool",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeBool),
			),
			ExpRows: rows(
				row(int64(1), bool(true)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id, cast(b1 as decimal(2)) from cast_bool",
			),
			ExpErr: "'BOOL' cannot be cast to 'DECIMAL(2)'",
		},
		{
			SQLs: sqls(
				"select _id, cast(b1 as id) from cast_bool",
			),
			ExpErr: "'BOOL' cannot be cast to 'ID'",
		},
		{
			SQLs: sqls(
				"select _id, cast(b1 as idset) from cast_bool",
			),
			ExpErr: "'BOOL' cannot be cast to 'IDSET'",
		},
		{
			SQLs: sqls(
				"select _id, cast(b1 as string) from cast_bool",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(int64(1), string("true")),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id, cast(b1 as stringset) from cast_bool",
			),
			ExpErr: "'BOOL' cannot be cast to 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select _id, cast(b1 as timestamp) from cast_bool",
			),
			ExpErr: "'BOOL' cannot be cast to 'TIMESTAMP'",
		},
	},
}

var castDecimal = TableTest{
	Table: tbl(
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
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select _id, cast(d1 as int) from cast_dec",
			),
			ExpErr: "'DECIMAL(2)' cannot be cast to 'INT'",
		},
		{
			SQLs: sqls(
				"select _id, cast(d1 as bool) from cast_dec",
			),
			ExpErr: "'DECIMAL(2)' cannot be cast to 'BOOL'",
		},
		{
			SQLs: sqls(
				"select _id, cast(d1 as decimal(2)) from cast_dec",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeDecimal2),
			),
			ExpRows: rows(
				row(int64(1), pql.NewDecimal(1234, 2)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id, cast(d1 as id) from cast_dec",
			),
			ExpErr: "'DECIMAL(2)' cannot be cast to 'ID'",
		},
		{
			SQLs: sqls(
				"select _id, cast(d1 as idset) from cast_dec",
			),
			ExpErr: "'DECIMAL(2)' cannot be cast to 'IDSET'",
		},
		{
			SQLs: sqls(
				"select _id, cast(d1 as string) from cast_dec",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(int64(1), string("12.34")),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id, cast(d1 as stringset) from cast_dec",
			),
			ExpErr: "'DECIMAL(2)' cannot be cast to 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select _id, cast(d1 as timestamp) from cast_dec",
			),
			ExpErr: "'DECIMAL(2)' cannot be cast to 'TIMESTAMP'",
		},
	},
}

var castID = TableTest{
	Table: tbl(
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
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select _id, cast(id1 as int) from cast_id",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(1), int64(20)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id, cast(id1 as bool) from cast_id",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeBool),
			),
			ExpRows: rows(
				row(int64(1), bool(true)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id, cast(id1 as decimal(2)) from cast_id",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeDecimal2),
			),
			ExpRows: rows(
				row(int64(1), pql.NewDecimal(2000, 2)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id, cast(id1 as id) from cast_id",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeID),
			),
			ExpRows: rows(
				row(int64(1), int64(20)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id, cast(id1 as idset) from cast_id",
			),
			ExpErr: "'ID' cannot be cast to 'IDSET'",
		},
		{
			SQLs: sqls(
				"select _id, cast(id1 as string) from cast_id",
			),
			ExpErr: "'ID' cannot be cast to 'STRING'",
		},
		{
			SQLs: sqls(
				"select _id, cast(id1 as stringset) from cast_id",
			),
			ExpErr: "'ID' cannot be cast to 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select _id, cast(id1 as timestamp) from cast_id",
			),
			ExpErr: "'ID' cannot be cast to 'TIMESTAMP'",
		},
	},
}

var castIDSet = TableTest{
	Table: tbl(
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
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select _id, cast(ids1 as int) from cast_ids",
			),
			ExpErr: "'IDSET' cannot be cast to 'INT'",
		},
		{
			SQLs: sqls(
				"select _id, cast(ids1 as bool) from cast_ids",
			),
			ExpErr: "'IDSET' cannot be cast to 'BOOL'",
		},
		{
			SQLs: sqls(
				"select _id, cast(ids1 as decimal(2)) from cast_ids",
			),
			ExpErr: "'IDSET' cannot be cast to 'DECIMAL(2)'",
		},
		{
			SQLs: sqls(
				"select _id, cast(ids1 as id) from cast_ids",
			),
			ExpErr: "'IDSET' cannot be cast to 'ID'",
		},
		{
			SQLs: sqls(
				"select _id, cast(ids1 as idset) from cast_ids",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeIDSet),
			),
			ExpRows: rows(
				row(int64(1), []int64{101, 102}),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id, cast(ids1 as string) from cast_ids",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(int64(1), string("[101 102]")),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id, cast(ids1 as stringset) from cast_ids",
			),
			ExpErr: "'IDSET' cannot be cast to 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select _id, cast(ids1 as timestamp) from cast_ids",
			),
			ExpErr: "'IDSET' cannot be cast to 'TIMESTAMP'",
		},
	},
}

var castString = TableTest{
	Table: tbl(
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
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select _id, cast(s1 as int) from cast_string",
			),
			ExpErr: "'foo' cannot be cast to 'INT'",
		},
		{
			SQLs: sqls(
				"select _id, cast('11' as int) from cast_string",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(1), int64(11)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id, cast(s1 as bool) from cast_string",
			),
			ExpErr: "'foo' cannot be cast to 'BOOL'",
		},
		{
			SQLs: sqls(
				"select _id, cast('true' as bool) from cast_string",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeBool),
			),
			ExpRows: rows(
				row(int64(1), bool(true)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id, cast(s1 as decimal(2)) from cast_string",
			),
			ExpErr: "'foo' cannot be cast to 'DECIMAL(2)'",
		},
		{
			SQLs: sqls(
				"select _id, cast('12.34' as decimal(2)) from cast_string",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeDecimal2),
			),
			ExpRows: rows(
				row(int64(1), pql.NewDecimal(1234, 2)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id, cast(s1 as id) from cast_string",
			),
			ExpErr: "'foo' cannot be cast to 'ID'",
		},
		{
			SQLs: sqls(
				"select _id, cast('11' as id) from cast_string",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeID),
			),
			ExpRows: rows(
				row(int64(1), int64(11)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id, cast(s1 as idset) from cast_string",
			),
			ExpErr: "'STRING' cannot be cast to 'IDSET'",
		},
		{
			SQLs: sqls(
				"select _id, cast(s1 as string) from cast_string",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(int64(1), string("foo")),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id, cast(s1 as stringset) from cast_string",
			),
			ExpErr: "'STRING' cannot be cast to 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select _id, cast(s1 as timestamp) from cast_string",
			),
			ExpErr: "'foo' cannot be cast to 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select _id, cast('1970-01-01T00:16:40Z' as timestamp) from cast_string",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeTimestamp),
			),
			ExpRows: rows(
				row(int64(1), time.Time(expectedCastTime())),
			),
			Compare: CompareExactUnordered,
		},
	},
}

var castStringSet = TableTest{
	Table: tbl(
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
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select _id, cast(ss1 as int) from cast_ss",
			),
			ExpErr: "'STRINGSET' cannot be cast to 'INT'",
		},
		{
			SQLs: sqls(
				"select _id, cast(ss1 as bool) from cast_ss",
			),
			ExpErr: "'STRINGSET' cannot be cast to 'BOOL'",
		},
		{
			SQLs: sqls(
				"select _id, cast(ss1 as decimal(2)) from cast_ss",
			),
			ExpErr: "'STRINGSET' cannot be cast to 'DECIMAL(2)'",
		},
		{
			SQLs: sqls(
				"select _id, cast(ss1 as id) from cast_ss",
			),
			ExpErr: "'STRINGSET' cannot be cast to 'ID'",
		},
		{
			SQLs: sqls(
				"select _id, cast(ss1 as idset) from cast_ss",
			),
			ExpErr: "'STRINGSET' cannot be cast to 'IDSET'",
		},
		{
			SQLs: sqls(
				"select _id, cast(ss1 as string) from cast_ss",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(int64(1), string(`["101","102"]`)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id, cast(ss1 as stringset) from cast_ss",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeStringSet),
			),
			ExpRows: rows(
				row(int64(1), []string{"101", "102"}),
			),
			Compare:        CompareExactUnordered,
			SortStringKeys: true,
		},
		{
			SQLs: sqls(
				"select _id, cast(ss1 as timestamp) from cast_ss",
			),
			ExpErr: "'STRINGSET' cannot be cast to 'TIMESTAMP'",
		},
	},
}

var castTimestamp = TableTest{
	Table: tbl(
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
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select _id, cast(t1 as int) from cast_ts",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(1), int64(1351807721)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id, cast(t1 as bool) from cast_ts",
			),
			ExpErr: "'TIMESTAMP' cannot be cast to 'BOOL'",
		},
		{
			SQLs: sqls(
				"select _id, cast(t1 as decimal(2)) from cast_ts",
			),
			ExpErr: "'TIMESTAMP' cannot be cast to 'DECIMAL(2)'",
		},
		{
			SQLs: sqls(
				"select _id, cast(t1 as id) from cast_ts",
			),
			ExpErr: "'TIMESTAMP' cannot be cast to 'ID'",
		},
		{
			SQLs: sqls(
				"select _id, cast(t1 as idset) from cast_ts",
			),
			ExpErr: "'TIMESTAMP' cannot be cast to 'IDSET'",
		},
		{
			SQLs: sqls(
				"select _id, cast(t1 as string) from cast_ts",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(int64(1), string("2012-11-01T22:08:41Z")),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id, cast(t1 as stringset) from cast_ts",
			),
			ExpErr: "'TIMESTAMP' cannot be cast to 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select _id, cast(t1 as timestamp) from cast_ts",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeTimestamp),
			),
			ExpRows: rows(
				row(int64(1), knownTimestamp()),
			),
			Compare: CompareExactUnordered,
		},
	},
}
