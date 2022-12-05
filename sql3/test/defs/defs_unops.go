package defs

import (
	"time"

	"github.com/molecula/featurebase/v3/pql"
)

var unaryOpExprWithInt = TableTest{
	Table: tbl(
		"unoptesti",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("i", fldTypeInt, "min 0", "max 1000"),
		),
		srcRows(
			srcRow(int64(1), int64(10)),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select -i from unoptesti;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(-10)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select !i from unoptesti;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(-11)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select +i from unoptesti;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(10)),
			),
			Compare: CompareExactUnordered,
		},
	},
}

var unaryOpExprWithBool = TableTest{
	Table: tbl(
		"unoptest_b",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("i", fldTypeBool),
		),
		srcRows(
			srcRow(int64(1), bool(false)),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select -i from unoptest_b;",
			),
			ExpErr: "operator '-' incompatible with type 'bool'",
		},
		{
			SQLs: sqls(
				"select !i from unoptest_b;",
			),
			ExpErr: "operator '!' incompatible with type 'bool'",
		},
		{
			SQLs: sqls(
				"select +i from unoptest_b;",
			),
			ExpErr: "operator '+' incompatible with type 'bool'",
		},
	},
}

var unaryOpExprWithID = TableTest{
	Table: tbl(
		"unoptestid",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("a", fldTypeInt, "min 0", "max 1000"),
		),
		srcRows(
			srcRow(int64(1), int64(10)),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select -_id from unoptestid;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(-1)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select !_id from unoptestid;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeID),
			),
			ExpRows: rows(
				row(int64(-2)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select +_id from unoptestid;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(1)),
			),
			Compare: CompareExactUnordered,
		},
	},
}

var unaryOpExprWithDecimal = TableTest{
	Table: tbl(
		"unoptestd",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("d", fldTypeDecimal2),
		),
		srcRows(
			srcRow(int64(1), float64(12.34)),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select -d from unoptestd;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeDecimal2),
			),
			ExpRows: rows(
				row(pql.NewDecimal(-1234, 2)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select !d from unoptestd;",
			),
			ExpErr: "operator '!' incompatible with type 'decimal(2)'",
		},
		{
			SQLs: sqls(
				"select +d from unoptestd;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeDecimal2),
			),
			ExpRows: rows(
				row(pql.NewDecimal(1234, 2)),
			),
			Compare: CompareExactUnordered,
		},
	},
}

var unaryOpExprWithTimestamp = TableTest{
	Table: tbl(
		"unoptestts",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("ts", fldTypeTimestamp),
		),
		srcRows(
			srcRow(int64(1), time.Time(knownTimestamp())),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select -ts from unoptestts;",
			),
			ExpErr: "operator '-' incompatible with type 'timestamp'",
		},
		{
			SQLs: sqls(
				"select !ts from unoptestts;",
			),
			ExpErr: "operator '!' incompatible with type 'timestamp'",
		},
		{
			SQLs: sqls(
				"select +ts from unoptestts;",
			),
			ExpErr: "operator '+' incompatible with type 'timestamp'",
		},
	},
}

var unaryOpExprWithIDSet = TableTest{
	Table: tbl(
		"unoptestids",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("ids", fldTypeIDSet),
		),
		srcRows(
			srcRow(int64(1), []int64{11, 12, 13}),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select -ids from unoptestids;",
			),
			ExpErr: "operator '-' incompatible with type 'idset'",
		},
		{
			SQLs: sqls(
				"select !ids from unoptestids;",
			),
			ExpErr: "operator '!' incompatible with type 'idset'",
		},
		{
			SQLs: sqls(
				"select +ids from unoptestids;",
			),
			ExpErr: "operator '+' incompatible with type 'idset'",
		},
	},
}

var unaryOpExprWithString = TableTest{
	Table: tbl(
		"unoptest_s",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("s", fldTypeString),
		),
		srcRows(
			srcRow(int64(1), string("foo")),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select -s from unoptest_s;",
			),
			ExpErr: "operator '-' incompatible with type 'string'",
		},
		{
			SQLs: sqls(
				"select !s from unoptest_s;",
			),
			ExpErr: "operator '!' incompatible with type 'string'",
		},
		{
			SQLs: sqls(
				"select +s from unoptest_s;",
			),
			ExpErr: "operator '+' incompatible with type 'string'",
		},
	},
}

var unaryOpExprWithStringSet = TableTest{
	Table: tbl(
		"unoptestss",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("s", fldTypeStringSet),
		),
		srcRows(
			srcRow(int64(1), []string{"11", "12", "13"}),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select -s from unoptestss;",
			),
			ExpErr: "operator '-' incompatible with type 'stringset'",
		},
		{
			SQLs: sqls(
				"select !s from unoptestss;",
			),
			ExpErr: "operator '!' incompatible with type 'stringset'",
		},
		{
			SQLs: sqls(
				"select +s from unoptestss;",
			),
			ExpErr: "operator '+' incompatible with type 'stringset'",
		},
	},
}
