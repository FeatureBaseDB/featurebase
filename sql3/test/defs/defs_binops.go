package defs

import (
	"time"

	"github.com/featurebasedb/featurebase/v3/pql"
)

// INT bin op tests
var binOpExprWithIntInt = TableTest{
	Table: tbl(
		"binoptesti_i",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("a", fldTypeInt, "min 0", "max 1000"),
			srcHdr("b", fldTypeInt, "min 0", "max 1000"),
		),
		srcRows(
			srcRow(int64(1), int64(10), int64(20)),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select a != b from binoptesti_i;",
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
				"select a = b from binoptesti_i;",
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
				"select a <= b from binoptesti_i;",
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
				"select a >= b from binoptesti_i;",
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
				"select a < b from binoptesti_i;",
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
				"select a > b from binoptesti_i;",
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
				"select a & b from binoptesti_i;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(0)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select a | b from binoptesti_i;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(30)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select a << b from binoptesti_i;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(10485760)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select a >> b from binoptesti_i;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(0)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select a + b from binoptesti_i;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(30)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select a - b from binoptesti_i;",
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
				"select a * b from binoptesti_i;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(200)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select a / b from binoptesti_i;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(0)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select a % b from binoptesti_i;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(10)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select a || b from binoptesti_i;",
			),
			ExpErr: "operator '||' incompatible with type 'INT'",
		},
	},
}

var binOpExprWithIntBool = TableTest{
	Table: tbl(
		"binoptesti_b",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("a", fldTypeInt, "min 0", "max 1000"),
			srcHdr("b", fldTypeBool),
		),
		srcRows(
			srcRow(int64(1), int64(10), bool(true)),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select a != b from binoptesti_b;",
			),
			ExpErr: "types 'INT' and 'BOOL' are not equatable",
		},
		{
			SQLs: sqls(
				"select a = b from binoptesti_b;",
			),
			ExpErr: "types 'INT' and 'BOOL' are not equatable",
		},
		{
			SQLs: sqls(
				"select a <= b from binoptesti_b;",
			),
			ExpErr: "operator '<=' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a >= b from binoptesti_b;",
			),
			ExpErr: "operator '>=' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a < b from binoptesti_b;",
			),
			ExpErr: "operator '<' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a > b from binoptesti_b;",
			),
			ExpErr: "operator '>' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a & b from binoptesti_b;",
			),
			ExpErr: "operator '&' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a | b from binoptesti_b;",
			),
			ExpErr: "operator '|' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a << b from binoptesti_b;",
			),
			ExpErr: "operator '<<' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a >> b from binoptesti_b;",
			),
			ExpErr: "operator '>>' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a + b from binoptesti_b;",
			),
			ExpErr: "operator '+' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a - b from binoptesti_b;",
			),
			ExpErr: "operator '-' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a * b from binoptesti_b;",
			),
			ExpErr: "operator '*' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a / b from binoptesti_b;",
			),
			ExpErr: "operator '/' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a % b from binoptesti_b;",
			),
			ExpErr: "operator '%' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a || b from binoptesti_b;",
			),
			ExpErr: "operator '||' incompatible with type 'INT'",
		},
	},
}

var binOpExprWithIntID = TableTest{
	Table: tbl(
		"binoptesti_id",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("b", fldTypeInt, "min 0", "max 1000"),
		),
		srcRows(
			srcRow(int64(10), int64(20)),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select b != _id from binoptesti_id;",
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
				"select b = _id from binoptesti_id;",
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
				"select b <= _id from binoptesti_id;",
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
				"select b >= _id from binoptesti_id;",
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
				"select b < _id from binoptesti_id;",
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
				"select b > _id from binoptesti_id;",
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
				"select b & _id from binoptesti_id;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(0)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select b | _id from binoptesti_id;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(30)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select b << _id from binoptesti_id;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(20480)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select b >> _id from binoptesti_id;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(0)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select b + _id from binoptesti_id;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(30)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select b - _id from binoptesti_id;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(10)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select b * _id from binoptesti_id;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(200)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select b / _id from binoptesti_id;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(2)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select b % _id from binoptesti_id;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(0)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select b || _id from binoptesti_id;",
			),
			ExpErr: "operator '||' incompatible with type 'INT'",
		},
	},
}

var binOpExprWithIntDecimal = TableTest{
	Table: tbl(
		"binoptesti_d",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("a", fldTypeInt, "min 0", "max 1000"),
			srcHdr("d", fldTypeDecimal2),
		),
		srcRows(
			srcRow(int64(1), int64(20), float64(12.34)),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select a != d from binoptesti_d;",
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
				"select a = d from binoptesti_d;",
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
				"select a <= d from binoptesti_d;",
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
				"select a >= d from binoptesti_d;",
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
				"select a < d from binoptesti_d;",
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
				"select a > d from binoptesti_d;",
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
				"select a & d from binoptesti_d;",
			),
			ExpErr: "operator '&' incompatible with type 'DECIMAL(2)'",
		},
		{
			SQLs: sqls(
				"select a | d from binoptesti_d;",
			),
			ExpErr: "operator '|' incompatible with type 'DECIMAL(2)'",
		},
		{
			SQLs: sqls(
				"select a << d from binoptesti_d;",
			),
			ExpErr: "operator '<<' incompatible with type 'DECIMAL(2)'",
		},
		{
			SQLs: sqls(
				"select a >> d from binoptesti_d;",
			),
			ExpErr: "operator '>>' incompatible with type 'DECIMAL(2)'",
		},
		{
			SQLs: sqls(
				"select a + d from binoptesti_d;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeDecimal2),
			),
			ExpRows: rows(
				row(pql.NewDecimal(3234, 2)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select a - d from binoptesti_d;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeDecimal2),
			),
			ExpRows: rows(
				row(pql.NewDecimal(766, 2)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select a * d from binoptesti_d;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeDecimal2),
			),
			ExpRows: rows(
				row(pql.NewDecimal(24680, 2)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select a / d from binoptesti_d;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeDecimal2),
			),
			ExpRows: rows(
				row(pql.NewDecimal(162, 2)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select a % d from binoptesti_d;",
			),
			ExpErr: "operator '%' incompatible with type 'DECIMAL(2)'",
		},
		{
			SQLs: sqls(
				"select a || d from binoptesti_d;",
			),
			ExpErr: "operator '||' incompatible with type 'INT'",
		},
	},
}

var binOpExprWithIntTimestamp = TableTest{
	Table: tbl(
		"binoptesti_ts",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("a", fldTypeInt, "min 0", "max 1000"),
			srcHdr("ts", fldTypeTimestamp),
		),
		srcRows(
			srcRow(int64(1), int64(20), time.Time(knownTimestamp())),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select a != ts from binoptesti_ts;",
			),
			ExpErr: "types 'INT' and 'TIMESTAMP' are not equatable",
		},
		{
			SQLs: sqls(
				"select a = ts from binoptesti_ts;",
			),
			ExpErr: "types 'INT' and 'TIMESTAMP' are not equatable",
		},
		{
			SQLs: sqls(
				"select a <= ts from binoptesti_ts;",
			),
			ExpErr: "types 'INT' and 'TIMESTAMP' are not equatable",
		},
		{
			SQLs: sqls(
				"select a >= ts from binoptesti_ts;",
			),
			ExpErr: "types 'INT' and 'TIMESTAMP' are not equatable",
		},
		{
			SQLs: sqls(
				"select a < ts from binoptesti_ts;",
			),
			ExpErr: "types 'INT' and 'TIMESTAMP' are not equatable",
		},
		{
			SQLs: sqls(
				"select a > ts from binoptesti_ts;",
			),
			ExpErr: "types 'INT' and 'TIMESTAMP' are not equatable",
		},
		{
			SQLs: sqls(
				"select a & ts from binoptesti_ts;",
			),
			ExpErr: "operator '&' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a | ts from binoptesti_ts;",
			),
			ExpErr: "operator '|' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a << ts from binoptesti_ts;",
			),
			ExpErr: "operator '<<' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a >> ts from binoptesti_ts;",
			),
			ExpErr: "operator '>>' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a + ts from binoptesti_ts;",
			),
			ExpErr: "operator '+' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a - ts from binoptesti_ts;",
			),
			ExpErr: "operator '-' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a * ts from binoptesti_ts;",
			),
			ExpErr: "operator '*' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a / ts from binoptesti_ts;",
			),
			ExpErr: "operator '/' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a % ts from binoptesti_ts;",
			),
			ExpErr: "operator '%' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a || ts from binoptesti_ts;",
			),
			ExpErr: "operator '||' incompatible with type 'INT'",
		},
	},
}

var binOpExprWithIntIDSet = TableTest{
	Table: tbl(
		"binoptesti_ids",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("a", fldTypeInt, "min 0", "max 1000"),
			srcHdr("b", fldTypeIDSet),
		),
		srcRows(
			srcRow(int64(1), int64(20), []int64{101, 102}),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select a != b from binoptesti_ids;",
			),
			ExpErr: "types 'INT' and 'IDSET' are not equatable",
		},
		{
			SQLs: sqls(
				"select a = b from binoptesti_ids;",
			),
			ExpErr: "types 'INT' and 'IDSET' are not equatable",
		},
		{
			SQLs: sqls(
				"select a <= b from binoptesti_ids;",
			),
			ExpErr: " operator '<=' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a >= b from binoptesti_ids;",
			),
			ExpErr: " operator '>=' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a < b from binoptesti_ids;",
			),
			ExpErr: " operator '<' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a > b from binoptesti_ids;",
			),
			ExpErr: " operator '>' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a & b from binoptesti_ids;",
			),
			ExpErr: " operator '&' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a | b from binoptesti_ids;",
			),
			ExpErr: " operator '|' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a << b from binoptesti_ids;",
			),
			ExpErr: " operator '<<' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a >> b from binoptesti_ids;",
			),
			ExpErr: " operator '>>' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a + b from binoptesti_ids;",
			),
			ExpErr: " operator '+' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a - b from binoptesti_ids;",
			),
			ExpErr: " operator '-' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a * b from binoptesti_ids;",
			),
			ExpErr: " operator '*' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a / b from binoptesti_ids;",
			),
			ExpErr: " operator '/' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a % b from binoptesti_ids;",
			),
			ExpErr: " operator '%' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a || b from binoptesti_ids;",
			),
			ExpErr: "operator '||' incompatible with type 'INT'",
		},
	},
}

var binOpExprWithIntString = TableTest{
	Table: tbl(
		"binoptesti_s",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("a", fldTypeInt, "min 0", "max 1000"),
			srcHdr("b", fldTypeString),
		),
		srcRows(
			srcRow(int64(1), int64(20), string("101")),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select a != b from binoptesti_s;",
			),
			ExpErr: "types 'INT' and 'STRING' are not equatable",
		},
		{
			SQLs: sqls(
				"select a = b from binoptesti_s;",
			),
			ExpErr: "types 'INT' and 'STRING' are not equatable",
		},
		{
			SQLs: sqls(
				"select a <= b from binoptesti_s;",
			),
			ExpErr: " operator '<=' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a >= b from binoptesti_s;",
			),
			ExpErr: " operator '>=' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a < b from binoptesti_s;",
			),
			ExpErr: " operator '<' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a > b from binoptesti_s;",
			),
			ExpErr: " operator '>' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a & b from binoptesti_s;",
			),
			ExpErr: " operator '&' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a | b from binoptesti_s;",
			),
			ExpErr: " operator '|' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a << b from binoptesti_s;",
			),
			ExpErr: " operator '<<' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a >> b from binoptesti_s;",
			),
			ExpErr: " operator '>>' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a + b from binoptesti_s;",
			),
			ExpErr: " operator '+' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a - b from binoptesti_s;",
			),
			ExpErr: " operator '-' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a * b from binoptesti_s;",
			),
			ExpErr: " operator '*' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a / b from binoptesti_s;",
			),
			ExpErr: " operator '/' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a % b from binoptesti_s;",
			),
			ExpErr: " operator '%' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a || b from binoptesti_s;",
			),
			ExpErr: "operator '||' incompatible with type 'INT'",
		},
	},
}

var binOpExprWithIntStringSet = TableTest{
	Table: tbl(
		"binoptesti_ss",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("a", fldTypeInt, "min 0", "max 1000"),
			srcHdr("b", fldTypeStringSet),
		),
		srcRows(
			srcRow(int64(1), int64(20), []string{"101", "102"}),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select a != b from binoptesti_ss;",
			),
			ExpErr: "types 'INT' and 'STRINGSET' are not equatable",
		},
		{
			SQLs: sqls(
				"select a = b from binoptesti_ss;",
			),
			ExpErr: "types 'INT' and 'STRINGSET' are not equatable",
		},
		{
			SQLs: sqls(
				"select a <= b from binoptesti_ss;",
			),
			ExpErr: " operator '<=' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a >= b from binoptesti_ss;",
			),
			ExpErr: " operator '>=' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a < b from binoptesti_ss;",
			),
			ExpErr: " operator '<' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a > b from binoptesti_ss;",
			),
			ExpErr: " operator '>' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a & b from binoptesti_ss;",
			),
			ExpErr: " operator '&' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a | b from binoptesti_ss;",
			),
			ExpErr: " operator '|' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a << b from binoptesti_ss;",
			),
			ExpErr: " operator '<<' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a >> b from binoptesti_ss;",
			),
			ExpErr: " operator '>>' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a + b from binoptesti_ss;",
			),
			ExpErr: " operator '+' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a - b from binoptesti_ss;",
			),
			ExpErr: " operator '-' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a * b from binoptesti_ss;",
			),
			ExpErr: " operator '*' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a / b from binoptesti_ss;",
			),
			ExpErr: " operator '/' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a % b from binoptesti_ss;",
			),
			ExpErr: " operator '%' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a || b from binoptesti_ss;",
			),
			ExpErr: "operator '||' incompatible with type 'INT'",
		},
	},
}

// BOOL bin op tests
var binOpExprWithBoolInt = TableTest{
	Table: tbl(
		"binoptestb_i",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("a", fldTypeBool),
			srcHdr("b", fldTypeInt, "min 0", "max 1000"),
		),
		srcRows(
			srcRow(int64(1), bool(true), int64(20)),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select a != b from binoptestb_i;",
			),
			ExpErr: "types 'BOOL' and 'INT' are not equatable",
		},
		{
			SQLs: sqls(
				"select a = b from binoptestb_i;",
			),
			ExpErr: "types 'BOOL' and 'INT' are not equatable",
		},
		{
			SQLs: sqls(
				"select a <= b from binoptestb_i;",
			),
			ExpErr: "operator '<=' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a >= b from binoptestb_i;",
			),
			ExpErr: "operator '>=' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a < b from binoptestb_i;",
			),
			ExpErr: "operator '<' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a > b from binoptestb_i;",
			),
			ExpErr: "operator '>' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a & b from binoptestb_i;",
			),
			ExpErr: "operator '&' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a | b from binoptestb_i;",
			),
			ExpErr: "operator '|' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a << b from binoptestb_i;",
			),
			ExpErr: "operator '<<' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a >> b from binoptestb_i;",
			),
			ExpErr: "operator '>>' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a + b from binoptestb_i;",
			),
			ExpErr: "operator '+' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a - b from binoptestb_i;",
			),
			ExpErr: "operator '-' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a * b from binoptestb_i;",
			),
			ExpErr: "operator '*' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a / b from binoptestb_i;",
			),
			ExpErr: "operator '/' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a % b from binoptestb_i;",
			),
			ExpErr: "operator '%' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a || b from binoptestb_i;",
			),
			ExpErr: "operator '||' incompatible with type 'BOOL'",
		},
	},
}

var binOpExprWithBoolBool = TableTest{
	Table: tbl(
		"binoptestb_b",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("a", fldTypeBool),
			srcHdr("b", fldTypeBool),
		),
		srcRows(
			srcRow(int64(1), bool(true), bool(true)),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select a != b from binoptestb_b;",
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
				"select a = b from binoptestb_b;",
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
				"select a <= b from binoptestb_b;",
			),
			ExpErr: "operator '<=' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a >= b from binoptestb_b;",
			),
			ExpErr: "operator '>=' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a < b from binoptestb_b;",
			),
			ExpErr: "operator '<' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a > b from binoptestb_b;",
			),
			ExpErr: "operator '>' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a & b from binoptestb_b;",
			),
			ExpErr: "operator '&' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a | b from binoptestb_b;",
			),
			ExpErr: "operator '|' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a << b from binoptestb_b;",
			),
			ExpErr: "operator '<<' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a >> b from binoptestb_b;",
			),
			ExpErr: "operator '>>' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a + b from binoptestb_b;",
			),
			ExpErr: "operator '+' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a - b from binoptestb_b;",
			),
			ExpErr: "operator '-' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a * b from binoptestb_b;",
			),
			ExpErr: "operator '*' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a / b from binoptestb_b;",
			),
			ExpErr: "operator '/' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a % b from binoptestb_b;",
			),
			ExpErr: "operator '%' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a || b from binoptestb_b;",
			),
			ExpErr: "operator '||' incompatible with type 'BOOL'",
		},
	},
}

var binOpExprWithBoolID = TableTest{
	Table: tbl(
		"binoptestb_id",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("b", fldTypeBool),
		),
		srcRows(
			srcRow(int64(10), bool(true)),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select b != _id from binoptestb_id;",
			),
			ExpErr: "types 'BOOL' and 'ID' are not equatable",
		},
		{
			SQLs: sqls(
				"select b = _id from binoptestb_id;",
			),
			ExpErr: "types 'BOOL' and 'ID' are not equatable",
		},
		{
			SQLs: sqls(
				"select b <= _id from binoptestb_id;",
			),
			ExpErr: "operator '<=' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select b >= _id from binoptestb_id;",
			),
			ExpErr: "operator '>=' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select b < _id from binoptestb_id;",
			),
			ExpErr: "operator '<' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select b > _id from binoptestb_id;",
			),
			ExpErr: "operator '>' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select b & _id from binoptestb_id;",
			),
			ExpErr: "operator '&' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select b | _id from binoptestb_id;",
			),
			ExpErr: "operator '|' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select b << _id from binoptestb_id;",
			),
			ExpErr: "operator '<<' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select b >> _id from binoptestb_id;",
			),
			ExpErr: "operator '>>' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select b + _id from binoptestb_id;",
			),
			ExpErr: "operator '+' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select b - _id from binoptestb_id;",
			),
			ExpErr: "operator '-' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select b * _id from binoptestb_id;",
			),
			ExpErr: "operator '*' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select b / _id from binoptestb_id;",
			),
			ExpErr: "operator '/' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select b % _id from binoptestb_id;",
			),
			ExpErr: "operator '%' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select b || _id from binoptestb_id;",
			),
			ExpErr: "operator '||' incompatible with type 'BOOL'",
		},
	},
}

var binOpExprWithBoolDecimal = TableTest{
	Table: tbl(
		"binoptestb_d",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("a", fldTypeBool),
			srcHdr("d", fldTypeDecimal2),
		),
		srcRows(
			srcRow(int64(1), bool(true), float64(12.34)),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select a != d from binoptestb_d;",
			),
			ExpErr: "types 'BOOL' and 'DECIMAL(2)' are not equatable",
		},
		{
			SQLs: sqls(
				"select a = d from binoptestb_d;",
			),
			ExpErr: "types 'BOOL' and 'DECIMAL(2)' are not equatable",
		},
		{
			SQLs: sqls(
				"select a <= d from binoptestb_d;",
			),
			ExpErr: "operator '<=' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a >= d from binoptestb_d;",
			),
			ExpErr: "operator '>=' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a < d from binoptestb_d;",
			),
			ExpErr: "operator '<' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a > d from binoptestb_d;",
			),
			ExpErr: "operator '>' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a & d from binoptestb_d;",
			),
			ExpErr: "operator '&' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a | d from binoptestb_d;",
			),
			ExpErr: "operator '|' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a << d from binoptestb_d;",
			),
			ExpErr: "operator '<<' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a >> d from binoptestb_d;",
			),
			ExpErr: "operator '>>' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a + d from binoptestb_d;",
			),
			ExpErr: "operator '+' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a - d from binoptestb_d;",
			),
			ExpErr: "operator '-' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a * d from binoptestb_d;",
			),
			ExpErr: "operator '*' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a / d from binoptestb_d;",
			),
			ExpErr: "operator '/' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a % d from binoptestb_d;",
			),
			ExpErr: "operator '%' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a || d from binoptestb_d;",
			),
			ExpErr: "operator '||' incompatible with type 'BOOL'",
		},
	},
}

var binOpExprWithBoolTimestamp = TableTest{
	Table: tbl(
		"binoptestb_ts",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("a", fldTypeBool),
			srcHdr("ts", fldTypeTimestamp),
		),
		srcRows(
			srcRow(int64(1), bool(true), time.Time(knownTimestamp())),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select a != ts from binoptestb_ts;",
			),
			ExpErr: "types 'BOOL' and 'TIMESTAMP' are not equatable",
		},
		{
			SQLs: sqls(
				"select a = ts from binoptestb_ts;",
			),
			ExpErr: "types 'BOOL' and 'TIMESTAMP' are not equatable",
		},
		{
			SQLs: sqls(
				"select a <= ts from binoptestb_ts;",
			),
			ExpErr: "operator '<=' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a >= ts from binoptestb_ts;",
			),
			ExpErr: "operator '>=' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a < ts from binoptestb_ts;",
			),
			ExpErr: "operator '<' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a > ts from binoptestb_ts;",
			),
			ExpErr: "operator '>' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a & ts from binoptestb_ts;",
			),
			ExpErr: "operator '&' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a | ts from binoptestb_ts;",
			),
			ExpErr: "operator '|' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a << ts from binoptestb_ts;",
			),
			ExpErr: "operator '<<' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a >> ts from binoptestb_ts;",
			),
			ExpErr: "operator '>>' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a + ts from binoptestb_ts;",
			),
			ExpErr: "operator '+' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a - ts from binoptestb_ts;",
			),
			ExpErr: "operator '-' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a * ts from binoptestb_ts;",
			),
			ExpErr: "operator '*' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a / ts from binoptestb_ts;",
			),
			ExpErr: "operator '/' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a % ts from binoptestb_ts;",
			),
			ExpErr: "operator '%' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a || ts from binoptestb_ts;",
			),
			ExpErr: "operator '||' incompatible with type 'BOOL'",
		},
	},
}

var binOpExprWithBoolIDSet = TableTest{
	Table: tbl(
		"binoptestb_ids",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("a", fldTypeBool),
			srcHdr("b", fldTypeIDSet),
		),
		srcRows(
			srcRow(int64(1), bool(true), []int64{101, 102}),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select a != b from binoptestb_ids;",
			),
			ExpErr: "types 'BOOL' and 'IDSET' are not equatable",
		},
		{
			SQLs: sqls(
				"select a = b from binoptestb_ids;",
			),
			ExpErr: "types 'BOOL' and 'IDSET' are not equatable",
		},
		{
			SQLs: sqls(
				"select a <= b from binoptestb_ids;",
			),
			ExpErr: " operator '<=' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a >= b from binoptestb_ids;",
			),
			ExpErr: " operator '>=' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a < b from binoptestb_ids;",
			),
			ExpErr: " operator '<' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a > b from binoptestb_ids;",
			),
			ExpErr: " operator '>' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a & b from binoptestb_ids;",
			),
			ExpErr: " operator '&' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a | b from binoptestb_ids;",
			),
			ExpErr: " operator '|' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a << b from binoptestb_ids;",
			),
			ExpErr: " operator '<<' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a >> b from binoptestb_ids;",
			),
			ExpErr: " operator '>>' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a + b from binoptestb_ids;",
			),
			ExpErr: " operator '+' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a - b from binoptestb_ids;",
			),
			ExpErr: " operator '-' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a * b from binoptestb_ids;",
			),
			ExpErr: " operator '*' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a / b from binoptestb_ids;",
			),
			ExpErr: " operator '/' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a % b from binoptestb_ids;",
			),
			ExpErr: " operator '%' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a || b from binoptestb_ids;",
			),
			ExpErr: "operator '||' incompatible with type 'BOOL'",
		},
	},
}

var binOpExprWithBoolString = TableTest{
	Table: tbl(
		"binoptestb_s",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("a", fldTypeBool),
			srcHdr("b", fldTypeString),
		),
		srcRows(
			srcRow(int64(1), bool(true), string("101")),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select a != b from binoptestb_s;",
			),
			ExpErr: "types 'BOOL' and 'STRING' are not equatable",
		},
		{
			SQLs: sqls(
				"select a = b from binoptestb_s;",
			),
			ExpErr: "types 'BOOL' and 'STRING' are not equatable",
		},
		{
			SQLs: sqls(
				"select a <= b from binoptestb_s;",
			),
			ExpErr: " operator '<=' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a >= b from binoptestb_s;",
			),
			ExpErr: " operator '>=' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a < b from binoptestb_s;",
			),
			ExpErr: " operator '<' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a > b from binoptestb_s;",
			),
			ExpErr: " operator '>' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a & b from binoptestb_s;",
			),
			ExpErr: " operator '&' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a | b from binoptestb_s;",
			),
			ExpErr: " operator '|' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a << b from binoptestb_s;",
			),
			ExpErr: " operator '<<' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a >> b from binoptestb_s;",
			),
			ExpErr: " operator '>>' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a + b from binoptestb_s;",
			),
			ExpErr: " operator '+' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a - b from binoptestb_s;",
			),
			ExpErr: " operator '-' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a * b from binoptestb_s;",
			),
			ExpErr: " operator '*' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a / b from binoptestb_s;",
			),
			ExpErr: " operator '/' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a % b from binoptestb_s;",
			),
			ExpErr: " operator '%' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a || b from binoptestb_s;",
			),
			ExpErr: "operator '||' incompatible with type 'BOOL'",
		},
	},
}

var binOpExprWithBoolStringSet = TableTest{
	Table: tbl(
		"binoptestb_ss",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("a", fldTypeBool),
			srcHdr("b", fldTypeStringSet),
		),
		srcRows(
			srcRow(int64(1), bool(true), []string{"101", "102"}),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select a != b from binoptestb_ss;",
			),
			ExpErr: "types 'BOOL' and 'STRINGSET' are not equatable",
		},
		{
			SQLs: sqls(
				"select a = b from binoptestb_ss;",
			),
			ExpErr: "types 'BOOL' and 'STRINGSET' are not equatable",
		},
		{
			SQLs: sqls(
				"select a <= b from binoptestb_ss;",
			),
			ExpErr: " operator '<=' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a >= b from binoptestb_ss;",
			),
			ExpErr: " operator '>=' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a < b from binoptestb_ss;",
			),
			ExpErr: " operator '<' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a > b from binoptestb_ss;",
			),
			ExpErr: " operator '>' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a & b from binoptestb_ss;",
			),
			ExpErr: " operator '&' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a | b from binoptestb_ss;",
			),
			ExpErr: " operator '|' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a << b from binoptestb_ss;",
			),
			ExpErr: " operator '<<' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a >> b from binoptestb_ss;",
			),
			ExpErr: " operator '>>' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a + b from binoptestb_ss;",
			),
			ExpErr: " operator '+' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a - b from binoptestb_ss;",
			),
			ExpErr: " operator '-' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a * b from binoptestb_ss;",
			),
			ExpErr: " operator '*' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a / b from binoptestb_ss;",
			),
			ExpErr: " operator '/' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a % b from binoptestb_ss;",
			),
			ExpErr: " operator '%' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select a || b from binoptestb_ss;",
			),
			ExpErr: "operator '||' incompatible with type 'BOOL'",
		},
	},
}

// ID bin op tests
var binOpExprWithIDInt = TableTest{
	Table: tbl(
		"binoptestid_i",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("b", fldTypeInt, "min 0", "max 1000"),
		),
		srcRows(
			srcRow(int64(10), int64(20)),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select _id != b from binoptestid_i;",
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
				"select _id = b from binoptestid_i;",
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
				"select _id <= b from binoptestid_i;",
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
				"select _id >= b from binoptestid_i;",
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
				"select _id < b from binoptestid_i;",
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
				"select _id > b from binoptestid_i;",
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
				"select _id & b from binoptestid_i;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(0)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id | b from binoptestid_i;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(30)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id << b from binoptestid_i;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(10485760)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id >> b from binoptestid_i;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(0)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id + b from binoptestid_i;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(30)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id - b from binoptestid_i;",
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
				"select _id * b from binoptestid_i;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(200)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id / b from binoptestid_i;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(0)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id % b from binoptestid_i;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(10)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id || b from binoptestid_i;",
			),
			ExpErr: "operator '||' incompatible with type 'ID'",
		},
	},
}

var binOpExprWithIDBool = TableTest{
	Table: tbl(
		"binoptestid_b",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("b", fldTypeBool),
		),
		srcRows(
			srcRow(int64(10), bool(true)),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select _id != b from binoptestid_b;",
			),
			ExpErr: "types 'ID' and 'BOOL' are not equatable",
		},
		{
			SQLs: sqls(
				"select _id = b from binoptestid_b;",
			),
			ExpErr: "types 'ID' and 'BOOL' are not equatable",
		},
		{
			SQLs: sqls(
				"select _id <= b from binoptestid_b;",
			),
			ExpErr: "operator '<=' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select _id >= b from binoptestid_b;",
			),
			ExpErr: "operator '>=' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select _id < b from binoptestid_b;",
			),
			ExpErr: "operator '<' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select _id > b from binoptestid_b;",
			),
			ExpErr: "operator '>' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select _id & b from binoptestid_b;",
			),
			ExpErr: "operator '&' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select _id | b from binoptestid_b;",
			),
			ExpErr: "operator '|' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select _id << b from binoptestid_b;",
			),
			ExpErr: "operator '<<' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select _id >> b from binoptestid_b;",
			),
			ExpErr: "operator '>>' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select _id + b from binoptestid_b;",
			),
			ExpErr: "operator '+' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select _id - b from binoptestid_b;",
			),
			ExpErr: "operator '-' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select _id * b from binoptestid_b;",
			),
			ExpErr: "operator '*' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select _id / b from binoptestid_b;",
			),
			ExpErr: "operator '/' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select _id % b from binoptestid_b;",
			),
			ExpErr: "operator '%' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select _id || b from binoptestid_b;",
			),
			ExpErr: "operator '||' incompatible with type 'ID'",
		},
	},
}

var binOpExprWithIDID = TableTest{
	Table: tbl(
		"binoptestid_id",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("b", fldTypeID),
		),
		srcRows(
			srcRow(int64(10), int64(20)),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select _id != b from binoptestid_id;",
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
				"select _id = b from binoptestid_id;",
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
				"select _id <= b from binoptestid_id;",
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
				"select _id >= b from binoptestid_id;",
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
				"select _id < b from binoptestid_id;",
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
				"select _id > b from binoptestid_id;",
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
				"select _id & b from binoptestid_id;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeID),
			),
			ExpRows: rows(
				row(int64(0)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id | b from binoptestid_id;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeID),
			),
			ExpRows: rows(
				row(int64(30)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id << b from binoptestid_id;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeID),
			),
			ExpRows: rows(
				row(int64(10485760)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id >> b from binoptestid_id;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeID),
			),
			ExpRows: rows(
				row(int64(0)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id + b from binoptestid_id;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeID),
			),
			ExpRows: rows(
				row(int64(30)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id - b from binoptestid_id;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeID),
			),
			ExpRows: rows(
				row(int64(-10)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id * b from binoptestid_id;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeID),
			),
			ExpRows: rows(
				row(int64(200)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id / b from binoptestid_id;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeID),
			),
			ExpRows: rows(
				row(int64(0)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id % b from binoptestid_id;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeID),
			),
			ExpRows: rows(
				row(int64(10)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id || b from binoptestid_id;",
			),
			ExpErr: "operator '||' incompatible with type 'ID'",
		},
	},
}

var binOpExprWithIDDecimal = TableTest{
	Table: tbl(
		"binoptestid_d",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("a", fldTypeID),
			srcHdr("d", fldTypeDecimal2),
		),
		srcRows(
			srcRow(int64(1), int64(20), float64(12.34)),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select a != d from binoptesti_d;",
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
				"select a = d from binoptesti_d;",
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
				"select a <= d from binoptesti_d;",
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
				"select a >= d from binoptesti_d;",
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
				"select a < d from binoptesti_d;",
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
				"select a > d from binoptesti_d;",
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
				"select a & d from binoptesti_d;",
			),
			ExpErr: "operator '&' incompatible with type 'DECIMAL(2)'",
		},
		{
			SQLs: sqls(
				"select a | d from binoptesti_d;",
			),
			ExpErr: "operator '|' incompatible with type 'DECIMAL(2)'",
		},
		{
			SQLs: sqls(
				"select a << d from binoptesti_d;",
			),
			ExpErr: "operator '<<' incompatible with type 'DECIMAL(2)'",
		},
		{
			SQLs: sqls(
				"select a >> d from binoptesti_d;",
			),
			ExpErr: "operator '>>' incompatible with type 'DECIMAL(2)'",
		},
		{
			SQLs: sqls(
				"select a + d from binoptesti_d;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeDecimal2),
			),
			ExpRows: rows(
				row(pql.NewDecimal(3234, 2)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select a - d from binoptesti_d;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeDecimal2),
			),
			ExpRows: rows(
				row(pql.NewDecimal(766, 2)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select a * d from binoptesti_d;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeDecimal2),
			),
			ExpRows: rows(
				row(pql.NewDecimal(24680, 2)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select a / d from binoptesti_d;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeDecimal2),
			),
			ExpRows: rows(
				row(pql.NewDecimal(162, 2)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select a % d from binoptesti_d;",
			),
			ExpErr: "operator '%' incompatible with type 'DECIMAL(2)'",
		},
		{
			SQLs: sqls(
				"select a || d from binoptesti_d;",
			),
			ExpErr: "operator '||' incompatible with type 'INT'",
		},
	},
}

var binOpExprWithIDTimestamp = TableTest{
	Table: tbl(
		"binoptestid_ts",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("a", fldTypeID),
			srcHdr("ts", fldTypeTimestamp),
		),
		srcRows(
			srcRow(int64(1), int64(20), time.Time(knownTimestamp())),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select a != ts from binoptesti_ts;",
			),
			ExpErr: "types 'INT' and 'TIMESTAMP' are not equatable",
		},
		{
			SQLs: sqls(
				"select a = ts from binoptesti_ts;",
			),
			ExpErr: "types 'INT' and 'TIMESTAMP' are not equatable",
		},
		{
			SQLs: sqls(
				"select a <= ts from binoptesti_ts;",
			),
			ExpErr: "types 'INT' and 'TIMESTAMP' are not equatable",
		},
		{
			SQLs: sqls(
				"select a >= ts from binoptesti_ts;",
			),
			ExpErr: "types 'INT' and 'TIMESTAMP' are not equatable",
		},
		{
			SQLs: sqls(
				"select a < ts from binoptesti_ts;",
			),
			ExpErr: "types 'INT' and 'TIMESTAMP' are not equatable",
		},
		{
			SQLs: sqls(
				"select a > ts from binoptesti_ts;",
			),
			ExpErr: "types 'INT' and 'TIMESTAMP' are not equatable",
		},
		{
			SQLs: sqls(
				"select a & ts from binoptesti_ts;",
			),
			ExpErr: "operator '&' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a | ts from binoptesti_ts;",
			),
			ExpErr: "operator '|' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a << ts from binoptesti_ts;",
			),
			ExpErr: "operator '<<' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a >> ts from binoptesti_ts;",
			),
			ExpErr: "operator '>>' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a + ts from binoptesti_ts;",
			),
			ExpErr: "operator '+' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a - ts from binoptesti_ts;",
			),
			ExpErr: "operator '-' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a * ts from binoptesti_ts;",
			),
			ExpErr: "operator '*' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a / ts from binoptesti_ts;",
			),
			ExpErr: "operator '/' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a % ts from binoptesti_ts;",
			),
			ExpErr: "operator '%' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a || ts from binoptesti_ts;",
			),
			ExpErr: "operator '||' incompatible with type 'INT'",
		},
	},
}

var binOpExprWithIDIDSet = TableTest{
	Table: tbl(
		"binoptestid_ids",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("a", fldTypeID),
			srcHdr("b", fldTypeIDSet),
		),
		srcRows(
			srcRow(int64(1), int64(20), []int64{101, 102}),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select a != b from binoptesti_ids;",
			),
			ExpErr: "types 'INT' and 'IDSET' are not equatable",
		},
		{
			SQLs: sqls(
				"select a = b from binoptesti_ids;",
			),
			ExpErr: "types 'INT' and 'IDSET' are not equatable",
		},
		{
			SQLs: sqls(
				"select a <= b from binoptesti_ids;",
			),
			ExpErr: " operator '<=' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a >= b from binoptesti_ids;",
			),
			ExpErr: " operator '>=' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a < b from binoptesti_ids;",
			),
			ExpErr: " operator '<' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a > b from binoptesti_ids;",
			),
			ExpErr: " operator '>' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a & b from binoptesti_ids;",
			),
			ExpErr: " operator '&' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a | b from binoptesti_ids;",
			),
			ExpErr: " operator '|' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a << b from binoptesti_ids;",
			),
			ExpErr: " operator '<<' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a >> b from binoptesti_ids;",
			),
			ExpErr: " operator '>>' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a + b from binoptesti_ids;",
			),
			ExpErr: " operator '+' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a - b from binoptesti_ids;",
			),
			ExpErr: " operator '-' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a * b from binoptesti_ids;",
			),
			ExpErr: " operator '*' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a / b from binoptesti_ids;",
			),
			ExpErr: " operator '/' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a % b from binoptesti_ids;",
			),
			ExpErr: " operator '%' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a || b from binoptesti_ids;",
			),
			ExpErr: "operator '||' incompatible with type 'INT'",
		},
	},
}

var binOpExprWithIDString = TableTest{
	Table: tbl(
		"binoptestid_s",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("a", fldTypeID),
			srcHdr("b", fldTypeString),
		),
		srcRows(
			srcRow(int64(1), int64(20), string("101")),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select a != b from binoptesti_s;",
			),
			ExpErr: "types 'INT' and 'STRING' are not equatable",
		},
		{
			SQLs: sqls(
				"select a = b from binoptesti_s;",
			),
			ExpErr: "types 'INT' and 'STRING' are not equatable",
		},
		{
			SQLs: sqls(
				"select a <= b from binoptesti_s;",
			),
			ExpErr: " operator '<=' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a >= b from binoptesti_s;",
			),
			ExpErr: " operator '>=' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a < b from binoptesti_s;",
			),
			ExpErr: " operator '<' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a > b from binoptesti_s;",
			),
			ExpErr: " operator '>' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a & b from binoptesti_s;",
			),
			ExpErr: " operator '&' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a | b from binoptesti_s;",
			),
			ExpErr: " operator '|' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a << b from binoptesti_s;",
			),
			ExpErr: " operator '<<' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a >> b from binoptesti_s;",
			),
			ExpErr: " operator '>>' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a + b from binoptesti_s;",
			),
			ExpErr: " operator '+' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a - b from binoptesti_s;",
			),
			ExpErr: " operator '-' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a * b from binoptesti_s;",
			),
			ExpErr: " operator '*' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a / b from binoptesti_s;",
			),
			ExpErr: " operator '/' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a % b from binoptesti_s;",
			),
			ExpErr: " operator '%' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a || b from binoptesti_s;",
			),
			ExpErr: "operator '||' incompatible with type 'INT'",
		},
	},
}

var binOpExprWithIDStringSet = TableTest{
	Table: tbl(
		"binoptestid_ss",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("a", fldTypeID),
			srcHdr("b", fldTypeStringSet),
		),
		srcRows(
			srcRow(int64(1), int64(20), []string{"101", "102"}),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select a != b from binoptesti_ss;",
			),
			ExpErr: "types 'INT' and 'STRINGSET' are not equatable",
		},
		{
			SQLs: sqls(
				"select a = b from binoptesti_ss;",
			),
			ExpErr: "types 'INT' and 'STRINGSET' are not equatable",
		},
		{
			SQLs: sqls(
				"select a <= b from binoptesti_ss;",
			),
			ExpErr: " operator '<=' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a >= b from binoptesti_ss;",
			),
			ExpErr: " operator '>=' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a < b from binoptesti_ss;",
			),
			ExpErr: " operator '<' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a > b from binoptesti_ss;",
			),
			ExpErr: " operator '>' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a & b from binoptesti_ss;",
			),
			ExpErr: " operator '&' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a | b from binoptesti_ss;",
			),
			ExpErr: " operator '|' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a << b from binoptesti_ss;",
			),
			ExpErr: " operator '<<' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a >> b from binoptesti_ss;",
			),
			ExpErr: " operator '>>' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a + b from binoptesti_ss;",
			),
			ExpErr: " operator '+' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a - b from binoptesti_ss;",
			),
			ExpErr: " operator '-' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a * b from binoptesti_ss;",
			),
			ExpErr: " operator '*' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a / b from binoptesti_ss;",
			),
			ExpErr: " operator '/' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a % b from binoptesti_ss;",
			),
			ExpErr: " operator '%' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a || b from binoptesti_ss;",
			),
			ExpErr: "operator '||' incompatible with type 'INT'",
		},
	},
}

// DECIMAL bin op tests
var binOpExprWithDecInt = TableTest{
	Table: tbl(
		"binoptestdec_i",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("b", fldTypeInt, "min 0", "max 1000"),
			srcHdr("d", fldTypeDecimal2),
		),
		srcRows(
			srcRow(int64(10), int64(20), float64(12.34)),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select d != b from binoptestdec_i;",
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
				"select d = b from binoptestdec_i;",
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
				"select d <= b from binoptestdec_i;",
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
				"select d >= b from binoptestdec_i;",
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
				"select d < b from binoptestdec_i;",
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
				"select d > b from binoptestdec_i;",
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
				"select d & b from binoptestdec_i;",
			),
			ExpErr: "operator '&' incompatible with type 'DECIMAL(2)'",
		},
		{
			SQLs: sqls(
				"select d | b from binoptestdec_i;",
			),
			ExpErr: "operator '|' incompatible with type 'DECIMAL(2)'",
		},
		{
			SQLs: sqls(
				"select d << b from binoptestdec_i;",
			),
			ExpErr: "operator '<<' incompatible with type 'DECIMAL(2)'",
		},
		{
			SQLs: sqls(
				"select d >> b from binoptestdec_i;",
			),
			ExpErr: "operator '>>' incompatible with type 'DECIMAL(2)'",
		},
		{
			SQLs: sqls(
				"select d + b from binoptestdec_i;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeDecimal2),
			),
			ExpRows: rows(
				row(pql.NewDecimal(3234, 2)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select d - b from binoptestdec_i;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeDecimal2),
			),
			ExpRows: rows(
				row(pql.NewDecimal(-766, 2)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select d * b from binoptestdec_i;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeDecimal2),
			),
			ExpRows: rows(
				row(pql.NewDecimal(24680, 2)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select d / b from binoptestdec_i;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeDecimal2),
			),
			ExpRows: rows(
				row(pql.NewDecimal(61, 2)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select d % b from binoptestdec_i;",
			),
			ExpErr: "operator '%' incompatible with type 'DECIMAL(2)'",
		},
		{
			SQLs: sqls(
				"select d || b from binoptestdec_i;",
			),
			ExpErr: "operator '||' incompatible with type 'DECIMAL(2)'",
		},
	},
}

var binOpExprWithDecBool = TableTest{
	Table: tbl(
		"binoptestdec_b",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("b", fldTypeBool),
			srcHdr("d", fldTypeDecimal2),
		),
		srcRows(
			srcRow(int64(10), bool(true), float64(12.34)),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select d != b from binoptestdec_b;",
			),
			ExpErr: "types 'DECIMAL(2)' and 'BOOL' are not equatable",
		},
		{
			SQLs: sqls(
				"select d = b from binoptestdec_b;",
			),
			ExpErr: "types 'DECIMAL(2)' and 'BOOL' are not equatable",
		},
		{
			SQLs: sqls(
				"select d <= b from binoptestdec_b;",
			),
			ExpErr: "operator '<=' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select d >= b from binoptestdec_b;",
			),
			ExpErr: "operator '>=' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select d < b from binoptestdec_b;",
			),
			ExpErr: "operator '<' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select d > b from binoptestdec_b;",
			),
			ExpErr: "operator '>' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select d & b from binoptestdec_b;",
			),
			ExpErr: "operator '&' incompatible with type 'DECIMAL(2)'",
		},
		{
			SQLs: sqls(
				"select d | b from binoptestdec_b;",
			),
			ExpErr: "operator '|' incompatible with type 'DECIMAL(2)'",
		},
		{
			SQLs: sqls(
				"select d << b from binoptestdec_b;",
			),
			ExpErr: "operator '<<' incompatible with type 'DECIMAL(2)'",
		},
		{
			SQLs: sqls(
				"select d >> b from binoptestdec_b;",
			),
			ExpErr: "operator '>>' incompatible with type 'DECIMAL(2)'",
		},
		{
			SQLs: sqls(
				"select d + b from binoptestdec_b;",
			),
			ExpErr: "operator '+' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select d - b from binoptestdec_b;",
			),
			ExpErr: "operator '-' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select d * b from binoptestdec_b;",
			),
			ExpErr: "operator '*' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select d / b from binoptestdec_b;",
			),
			ExpErr: "operator '/' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select d % b from binoptestdec_b;",
			),
			ExpErr: "operator '%' incompatible with type 'DECIMAL(2)'",
		},
		{
			SQLs: sqls(
				"select d || b from binoptestdec_b;",
			),
			ExpErr: "operator '||' incompatible with type 'DECIMAL(2)'",
		},
	},
}

var binOpExprWithDecID = TableTest{
	Table: tbl(
		"binoptestdec_id",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("b", fldTypeID),
			srcHdr("d", fldTypeDecimal2),
		),
		srcRows(
			srcRow(int64(10), int64(20), float64(12.34)),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select d != b from binoptestdec_id;",
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
				"select d = b from binoptestdec_id;",
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
				"select d <= b from binoptestdec_id;",
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
				"select d >= b from binoptestdec_id;",
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
				"select d < b from binoptestdec_id;",
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
				"select d > b from binoptestdec_id;",
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
				"select d & b from binoptestdec_id;",
			),
			ExpErr: "operator '&' incompatible with type 'DECIMAL(2)'",
		},
		{
			SQLs: sqls(
				"select d | b from binoptestdec_id;",
			),
			ExpErr: "operator '|' incompatible with type 'DECIMAL(2)'",
		},
		{
			SQLs: sqls(
				"select d << b from binoptestdec_id;",
			),
			ExpErr: "operator '<<' incompatible with type 'DECIMAL(2)'",
		},
		{
			SQLs: sqls(
				"select d >> b from binoptestdec_id;",
			),
			ExpErr: "operator '>>' incompatible with type 'DECIMAL(2)'",
		},
		{
			SQLs: sqls(
				"select d + b from binoptestdec_id;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeDecimal2),
			),
			ExpRows: rows(
				row(pql.NewDecimal(3234, 2)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select d - b from binoptestdec_id;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeDecimal2),
			),
			ExpRows: rows(
				row(pql.NewDecimal(-766, 2)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select d * b from binoptestdec_id;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeDecimal2),
			),
			ExpRows: rows(
				row(pql.NewDecimal(24680, 2)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select d / b from binoptestdec_id;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeDecimal2),
			),
			ExpRows: rows(
				row(pql.NewDecimal(61, 2)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select d % b from binoptestdec_id;",
			),
			ExpErr: "operator '%' incompatible with type 'DECIMAL(2)'",
		},
		{
			SQLs: sqls(
				"select d || b from binoptestdec_id;",
			),
			ExpErr: "operator '||' incompatible with type 'DECIMAL(2)'",
		},
	},
}

var binOpExprWithDecDecimal = TableTest{
	Table: tbl(
		"binoptestdec_d",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("a", fldTypeDecimal2),
			srcHdr("d", fldTypeDecimal2),
		),
		srcRows(
			srcRow(int64(1), float64(20.00), float64(12.34)),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select a != d from binoptestdec_d;",
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
				"select a = d from binoptestdec_d;",
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
				"select a <= d from binoptestdec_d;",
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
				"select a >= d from binoptestdec_d;",
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
				"select a < d from binoptestdec_d;",
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
				"select a > d from binoptestdec_d;",
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
				"select a & d from binoptestdec_d;",
			),
			ExpErr: "operator '&' incompatible with type 'DECIMAL(2)'",
		},
		{
			SQLs: sqls(
				"select a | d from binoptestdec_d;",
			),
			ExpErr: "operator '|' incompatible with type 'DECIMAL(2)'",
		},
		{
			SQLs: sqls(
				"select a << d from binoptestdec_d;",
			),
			ExpErr: "operator '<<' incompatible with type 'DECIMAL(2)'",
		},
		{
			SQLs: sqls(
				"select a >> d from binoptestdec_d;",
			),
			ExpErr: "operator '>>' incompatible with type 'DECIMAL(2)'",
		},
		{
			SQLs: sqls(
				"select a + d from binoptestdec_d;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeDecimal2),
			),
			ExpRows: rows(
				row(pql.NewDecimal(3234, 2)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select a - d from binoptestdec_d;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeDecimal2),
			),
			ExpRows: rows(
				row(pql.NewDecimal(766, 2)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select a * d from binoptestdec_d;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeDecimal2),
			),
			ExpRows: rows(
				row(pql.NewDecimal(24680, 2)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select a / d from binoptestdec_d;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeDecimal2),
			),
			ExpRows: rows(
				row(pql.NewDecimal(162, 2)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select a % d from binoptestdec_d;",
			),
			ExpErr: "operator '%' incompatible with type 'DECIMAL(2)'",
		},
		{
			SQLs: sqls(
				"select a || d from binoptestdec_d;",
			),
			ExpErr: "operator '||' incompatible with type 'DECIMAL(2)'",
		},
	},
}

var binOpExprWithDecTimestamp = TableTest{
	Table: tbl(
		"binoptestdec_ts",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("a", fldTypeDecimal2),
			srcHdr("ts", fldTypeTimestamp),
		),
		srcRows(
			srcRow(int64(1), float64(20.00), time.Time(knownTimestamp())),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select a != ts from binoptestdec_ts;",
			),
			ExpErr: "types 'DECIMAL(2)' and 'TIMESTAMP' are not equatable",
		},
		{
			SQLs: sqls(
				"select a = ts from binoptestdec_ts;",
			),
			ExpErr: "types 'DECIMAL(2)' and 'TIMESTAMP' are not equatable",
		},
		{
			SQLs: sqls(
				"select a <= ts from binoptestdec_ts;",
			),
			ExpErr: "types 'DECIMAL(2)' and 'TIMESTAMP' are not equatable",
		},
		{
			SQLs: sqls(
				"select a >= ts from binoptestdec_ts;",
			),
			ExpErr: "types 'DECIMAL(2)' and 'TIMESTAMP' are not equatable",
		},
		{
			SQLs: sqls(
				"select a < ts from binoptestdec_ts;",
			),
			ExpErr: "types 'DECIMAL(2)' and 'TIMESTAMP' are not equatable",
		},
		{
			SQLs: sqls(
				"select a > ts from binoptestdec_ts;",
			),
			ExpErr: "types 'DECIMAL(2)' and 'TIMESTAMP' are not equatable",
		},
		{
			SQLs: sqls(
				"select a & ts from binoptestdec_ts;",
			),
			ExpErr: "operator '&' incompatible with type 'DECIMAL(2)'",
		},
		{
			SQLs: sqls(
				"select a | ts from binoptestdec_ts;",
			),
			ExpErr: "operator '|' incompatible with type 'DECIMAL(2)'",
		},
		{
			SQLs: sqls(
				"select a << ts from binoptestdec_ts;",
			),
			ExpErr: "operator '<<' incompatible with type 'DECIMAL(2)'",
		},
		{
			SQLs: sqls(
				"select a >> ts from binoptestdec_ts;",
			),
			ExpErr: "operator '>>' incompatible with type 'DECIMAL(2)'",
		},
		{
			SQLs: sqls(
				"select a + ts from binoptestdec_ts;",
			),
			ExpErr: "operator '+' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a - ts from binoptestdec_ts;",
			),
			ExpErr: "operator '-' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a * ts from binoptestdec_ts;",
			),
			ExpErr: "operator '*' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a / ts from binoptestdec_ts;",
			),
			ExpErr: "operator '/' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a % ts from binoptestdec_ts;",
			),
			ExpErr: "operator '%' incompatible with type 'DECIMAL(2)'",
		},
		{
			SQLs: sqls(
				"select a || ts from binoptestdec_ts;",
			),
			ExpErr: "operator '||' incompatible with type 'DECIMAL(2)'",
		},
	},
}

var binOpExprWithDecIDSet = TableTest{
	Table: tbl(
		"binoptestdec_ids",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("a", fldTypeDecimal2),
			srcHdr("b", fldTypeIDSet),
		),
		srcRows(
			srcRow(int64(1), float64(20.00), []int64{101, 102}),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select a != b from binoptestdec_ids;",
			),
			ExpErr: "types 'DECIMAL(2)' and 'IDSET' are not equatable",
		},
		{
			SQLs: sqls(
				"select a = b from binoptestdec_ids;",
			),
			ExpErr: "types 'DECIMAL(2)' and 'IDSET' are not equatable",
		},
		{
			SQLs: sqls(
				"select a <= b from binoptestdec_ids;",
			),
			ExpErr: " operator '<=' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a >= b from binoptestdec_ids;",
			),
			ExpErr: " operator '>=' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a < b from binoptestdec_ids;",
			),
			ExpErr: " operator '<' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a > b from binoptestdec_ids;",
			),
			ExpErr: " operator '>' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a & b from binoptestdec_ids;",
			),
			ExpErr: " operator '&' incompatible with type 'DECIMAL(2)'",
		},
		{
			SQLs: sqls(
				"select a | b from binoptestdec_ids;",
			),
			ExpErr: " operator '|' incompatible with type 'DECIMAL(2)'",
		},
		{
			SQLs: sqls(
				"select a << b from binoptestdec_ids;",
			),
			ExpErr: " operator '<<' incompatible with type 'DECIMAL(2)'",
		},
		{
			SQLs: sqls(
				"select a >> b from binoptestdec_ids;",
			),
			ExpErr: " operator '>>' incompatible with type 'DECIMAL(2)'",
		},
		{
			SQLs: sqls(
				"select a + b from binoptestdec_ids;",
			),
			ExpErr: " operator '+' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a - b from binoptestdec_ids;",
			),
			ExpErr: " operator '-' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a * b from binoptestdec_ids;",
			),
			ExpErr: " operator '*' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a / b from binoptestdec_ids;",
			),
			ExpErr: " operator '/' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a % b from binoptestdec_ids;",
			),
			ExpErr: " operator '%' incompatible with type 'DECIMAL(2)'",
		},
		{
			SQLs: sqls(
				"select a || b from binoptestdec_ids;",
			),
			ExpErr: "operator '||' incompatible with type 'DECIMAL(2)'",
		},
	},
}

var binOpExprWithDecString = TableTest{
	Table: tbl(
		"binoptestdec_s",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("a", fldTypeDecimal2),
			srcHdr("b", fldTypeString),
		),
		srcRows(
			srcRow(int64(1), float64(20.00), string("101")),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select a != b from binoptestdec_s;",
			),
			ExpErr: "types 'DECIMAL(2)' and 'STRING' are not equatable",
		},
		{
			SQLs: sqls(
				"select a = b from binoptestdec_s;",
			),
			ExpErr: "types 'DECIMAL(2)' and 'STRING' are not equatable",
		},
		{
			SQLs: sqls(
				"select a <= b from binoptestdec_s;",
			),
			ExpErr: " operator '<=' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a >= b from binoptestdec_s;",
			),
			ExpErr: " operator '>=' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a < b from binoptestdec_s;",
			),
			ExpErr: " operator '<' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a > b from binoptestdec_s;",
			),
			ExpErr: " operator '>' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a & b from binoptestdec_s;",
			),
			ExpErr: " operator '&' incompatible with type 'DECIMAL(2)'",
		},
		{
			SQLs: sqls(
				"select a | b from binoptestdec_s;",
			),
			ExpErr: " operator '|' incompatible with type 'DECIMAL(2)'",
		},
		{
			SQLs: sqls(
				"select a << b from binoptestdec_s;",
			),
			ExpErr: " operator '<<' incompatible with type 'DECIMAL(2)'",
		},
		{
			SQLs: sqls(
				"select a >> b from binoptestdec_s;",
			),
			ExpErr: " operator '>>' incompatible with type 'DECIMAL(2)'",
		},
		{
			SQLs: sqls(
				"select a + b from binoptestdec_s;",
			),
			ExpErr: " operator '+' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a - b from binoptestdec_s;",
			),
			ExpErr: " operator '-' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a * b from binoptestdec_s;",
			),
			ExpErr: " operator '*' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a / b from binoptestdec_s;",
			),
			ExpErr: " operator '/' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a % b from binoptestdec_s;",
			),
			ExpErr: " operator '%' incompatible with type 'DECIMAL(2)'",
		},
		{
			SQLs: sqls(
				"select a || b from binoptestdec_s;",
			),
			ExpErr: "operator '||' incompatible with type 'DECIMAL(2)'",
		},
	},
}

var binOpExprWithDecStringSet = TableTest{
	Table: tbl(
		"binoptestdec_ss",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("a", fldTypeDecimal2),
			srcHdr("b", fldTypeStringSet),
		),
		srcRows(
			srcRow(int64(1), float64(20.00), []string{"101", "102"}),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select a != b from binoptestdec_ss;",
			),
			ExpErr: "types 'DECIMAL(2)' and 'STRINGSET' are not equatable",
		},
		{
			SQLs: sqls(
				"select a = b from binoptestdec_ss;",
			),
			ExpErr: "types 'DECIMAL(2)' and 'STRINGSET' are not equatable",
		},
		{
			SQLs: sqls(
				"select a <= b from binoptestdec_ss;",
			),
			ExpErr: " operator '<=' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a >= b from binoptestdec_ss;",
			),
			ExpErr: " operator '>=' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a < b from binoptestdec_ss;",
			),
			ExpErr: " operator '<' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a > b from binoptestdec_ss;",
			),
			ExpErr: " operator '>' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a & b from binoptestdec_ss;",
			),
			ExpErr: " operator '&' incompatible with type 'DECIMAL(2)'",
		},
		{
			SQLs: sqls(
				"select a | b from binoptestdec_ss;",
			),
			ExpErr: " operator '|' incompatible with type 'DECIMAL(2)'",
		},
		{
			SQLs: sqls(
				"select a << b from binoptestdec_ss;",
			),
			ExpErr: " operator '<<' incompatible with type 'DECIMAL(2)'",
		},
		{
			SQLs: sqls(
				"select a >> b from binoptestdec_ss;",
			),
			ExpErr: " operator '>>' incompatible with type 'DECIMAL(2)'",
		},
		{
			SQLs: sqls(
				"select a + b from binoptestdec_ss;",
			),
			ExpErr: " operator '+' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a - b from binoptestdec_ss;",
			),
			ExpErr: " operator '-' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a * b from binoptestdec_ss;",
			),
			ExpErr: " operator '*' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a / b from binoptestdec_ss;",
			),
			ExpErr: " operator '/' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a % b from binoptestdec_ss;",
			),
			ExpErr: " operator '%' incompatible with type 'DECIMAL(2)'",
		},
		{
			SQLs: sqls(
				"select a || b from binoptestdec_ss;",
			),
			ExpErr: "operator '||' incompatible with type 'DECIMAL(2)'",
		},
	},
}

// TIMESTAMP bin op tests
var binOpExprWithTSInt = TableTest{
	Table: tbl(
		"binoptestts_i",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("b", fldTypeInt, "min 0", "max 1000"),
			srcHdr("d", fldTypeTimestamp),
		),
		srcRows(
			srcRow(int64(10), int64(20), time.Time(knownTimestamp())),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select d != b from binoptestts_i;",
			),
			ExpErr: "types 'TIMESTAMP' and 'INT' are not equatable",
		},
		{
			SQLs: sqls(
				"select d = b from binoptestts_i;",
			),
			ExpErr: "types 'TIMESTAMP' and 'INT' are not equatable",
		},
		{
			SQLs: sqls(
				"select d <= b from binoptestts_i;",
			),
			ExpErr: "types 'TIMESTAMP' and 'INT' are not equatable",
		},
		{
			SQLs: sqls(
				"select d >= b from binoptestts_i;",
			),
			ExpErr: "types 'TIMESTAMP' and 'INT' are not equatable",
		},
		{
			SQLs: sqls(
				"select d < b from binoptestts_i;",
			),
			ExpErr: "types 'TIMESTAMP' and 'INT' are not equatable",
		},
		{
			SQLs: sqls(
				"select d > b from binoptestts_i;",
			),
			ExpErr: "types 'TIMESTAMP' and 'INT' are not equatable",
		},
		{
			SQLs: sqls(
				"select d & b from binoptestts_i;",
			),
			ExpErr: "operator '&' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select d | b from binoptestts_i;",
			),
			ExpErr: "operator '|' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select d << b from binoptestts_i;",
			),
			ExpErr: "operator '<<' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select d >> b from binoptestts_i;",
			),
			ExpErr: "operator '>>' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select d + b from binoptestts_i;",
			),
			ExpErr: "operator '+' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select d - b from binoptestts_i;",
			),
			ExpErr: "operator '-' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select d * b from binoptestts_i;",
			),
			ExpErr: "operator '*' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select d / b from binoptestts_i;",
			),
			ExpErr: "operator '/' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select d % b from binoptestts_i;",
			),
			ExpErr: "operator '%' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select d || b from binoptestts_i;",
			),
			ExpErr: "operator '||' incompatible with type 'TIMESTAMP'",
		},
	},
}

var binOpExprWithTSBool = TableTest{
	Table: tbl(
		"binoptestts_b",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("b", fldTypeBool),
			srcHdr("d", fldTypeTimestamp),
		),
		srcRows(
			srcRow(int64(10), bool(true), time.Time(knownTimestamp())),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select d != b from binoptestts_b;",
			),
			ExpErr: "types 'TIMESTAMP' and 'BOOL' are not equatable",
		},
		{
			SQLs: sqls(
				"select d = b from binoptestts_b;",
			),
			ExpErr: "types 'TIMESTAMP' and 'BOOL' are not equatable",
		},
		{
			SQLs: sqls(
				"select d <= b from binoptestts_b;",
			),
			ExpErr: "operator '<=' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select d >= b from binoptestts_b;",
			),
			ExpErr: "operator '>=' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select d < b from binoptestts_b;",
			),
			ExpErr: "operator '<' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select d > b from binoptestts_b;",
			),
			ExpErr: "operator '>' incompatible with type 'BOOL'",
		},
		{
			SQLs: sqls(
				"select d & b from binoptestts_b;",
			),
			ExpErr: "operator '&' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select d | b from binoptestts_b;",
			),
			ExpErr: "operator '|' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select d << b from binoptestts_b;",
			),
			ExpErr: "operator '<<' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select d >> b from binoptestts_b;",
			),
			ExpErr: "operator '>>' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select d + b from binoptestts_b;",
			),
			ExpErr: "operator '+' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select d - b from binoptestts_b;",
			),
			ExpErr: "operator '-' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select d * b from binoptestts_b;",
			),
			ExpErr: "operator '*' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select d / b from binoptestts_b;",
			),
			ExpErr: "operator '/' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select d % b from binoptestts_b;",
			),
			ExpErr: "operator '%' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select d || b from binoptestts_b;",
			),
			ExpErr: "operator '||' incompatible with type 'TIMESTAMP'",
		},
	},
}

var binOpExprWithTSID = TableTest{
	Table: tbl(
		"binoptestts_id",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("b", fldTypeID),
			srcHdr("d", fldTypeTimestamp),
		),
		srcRows(
			srcRow(int64(10), int64(20), time.Time(knownTimestamp())),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select d != b from binoptestts_id;",
			),
			ExpErr: "types 'TIMESTAMP' and 'ID' are not equatable",
		},
		{
			SQLs: sqls(
				"select d = b from binoptestts_id;",
			),
			ExpErr: "types 'TIMESTAMP' and 'ID' are not equatable",
		},
		{
			SQLs: sqls(
				"select d <= b from binoptestts_id;",
			),
			ExpErr: "types 'TIMESTAMP' and 'ID' are not equatable",
		},
		{
			SQLs: sqls(
				"select d >= b from binoptestts_id;",
			),
			ExpErr: "types 'TIMESTAMP' and 'ID' are not equatable",
		},
		{
			SQLs: sqls(
				"select d < b from binoptestts_id;",
			),
			ExpErr: "types 'TIMESTAMP' and 'ID' are not equatable",
		},
		{
			SQLs: sqls(
				"select d > b from binoptestts_id;",
			),
			ExpErr: "types 'TIMESTAMP' and 'ID' are not equatable",
		},
		{
			SQLs: sqls(
				"select d & b from binoptestts_id;",
			),
			ExpErr: "operator '&' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select d | b from binoptestts_id;",
			),
			ExpErr: "operator '|' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select d << b from binoptestts_id;",
			),
			ExpErr: "operator '<<' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select d >> b from binoptestts_id;",
			),
			ExpErr: "operator '>>' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select d + b from binoptestts_id;",
			),
			ExpErr: "operator '+' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select d - b from binoptestts_id;",
			),
			ExpErr: "operator '-' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select d * b from binoptestts_id;",
			),
			ExpErr: "operator '*' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select d / b from binoptestts_id;",
			),
			ExpErr: "operator '/' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select d % b from binoptestts_id;",
			),
			ExpErr: "operator '%' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select d || b from binoptestts_id;",
			),
			ExpErr: "operator '||' incompatible with type 'TIMESTAMP'",
		},
	},
}

var binOpExprWithTSDecimal = TableTest{
	Table: tbl(
		"binoptestts_d",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("a", fldTypeTimestamp),
			srcHdr("d", fldTypeDecimal2),
		),
		srcRows(
			srcRow(int64(1), time.Time(knownTimestamp()), float64(12.34)),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select a != d from binoptestts_d;",
			),
			ExpErr: "types 'TIMESTAMP' and 'DECIMAL(2)' are not equatable",
		},
		{
			SQLs: sqls(
				"select a = d from binoptestts_d;",
			),
			ExpErr: "types 'TIMESTAMP' and 'DECIMAL(2)' are not equatable",
		},
		{
			SQLs: sqls(
				"select a <= d from binoptestts_d;",
			),
			ExpErr: "types 'TIMESTAMP' and 'DECIMAL(2)' are not equatable",
		},
		{
			SQLs: sqls(
				"select a >= d from binoptestts_d;",
			),
			ExpErr: "types 'TIMESTAMP' and 'DECIMAL(2)' are not equatable",
		},
		{
			SQLs: sqls(
				"select a < d from binoptestts_d;",
			),
			ExpErr: "types 'TIMESTAMP' and 'DECIMAL(2)' are not equatable",
		},
		{
			SQLs: sqls(
				"select a > d from binoptestts_d;",
			),
			ExpErr: "types 'TIMESTAMP' and 'DECIMAL(2)' are not equatable",
		},
		{
			SQLs: sqls(
				"select a & d from binoptestts_d;",
			),
			ExpErr: "operator '&' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a | d from binoptestts_d;",
			),
			ExpErr: "operator '|' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a << d from binoptestts_d;",
			),
			ExpErr: "operator '<<' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a >> d from binoptestts_d;",
			),
			ExpErr: "operator '>>' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a + d from binoptestts_d;",
			),
			ExpErr: "operator '+' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a - d from binoptestts_d;",
			),
			ExpErr: "operator '-' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a * d from binoptestts_d;",
			),
			ExpErr: "operator '*' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a / d from binoptestts_d;",
			),
			ExpErr: "operator '/' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a % d from binoptestts_d;",
			),
			ExpErr: "operator '%' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a || d from binoptestts_d;",
			),
			ExpErr: "operator '||' incompatible with type 'TIMESTAMP'",
		},
	},
}

var binOpExprWithTSTimestamp = TableTest{
	Table: tbl(
		"binoptestts_ts",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("a", fldTypeTimestamp),
			srcHdr("ts", fldTypeTimestamp),
		),
		srcRows(
			srcRow(int64(1), time.Time(knownTimestamp()), time.Time(knownTimestamp())),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select a != ts from binoptestts_ts;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			ExpRows: rows(
				row(bool(false)),
			),
			Compare: CompareExactUnordered},
		{
			SQLs: sqls(
				"select a = ts from binoptestts_ts;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			ExpRows: rows(
				row(bool(true)),
			),
			Compare: CompareExactUnordered},
		{
			SQLs: sqls(
				"select a <= ts from binoptestts_ts;",
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
				"select a >= ts from binoptestts_ts;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			ExpRows: rows(
				row(bool(true)),
			),
			Compare: CompareExactUnordered},
		{
			SQLs: sqls(
				"select a < ts from binoptestts_ts;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			ExpRows: rows(
				row(bool(false)),
			),
			Compare: CompareExactUnordered},
		{
			SQLs: sqls(
				"select a > ts from binoptestts_ts;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			ExpRows: rows(
				row(bool(false)),
			),
			Compare: CompareExactUnordered},
		{
			SQLs: sqls(
				"select a & ts from binoptestts_ts;",
			),
			ExpErr: "operator '&' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a | ts from binoptestts_ts;",
			),
			ExpErr: "operator '|' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a << ts from binoptestts_ts;",
			),
			ExpErr: "operator '<<' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a >> ts from binoptestts_ts;",
			),
			ExpErr: "operator '>>' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a + ts from binoptestts_ts;",
			),
			ExpErr: "operator '+' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a - ts from binoptestts_ts;",
			),
			ExpErr: "operator '-' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a * ts from binoptestts_ts;",
			),
			ExpErr: "operator '*' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a / ts from binoptestts_ts;",
			),
			ExpErr: "operator '/' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a % ts from binoptestts_ts;",
			),
			ExpErr: "operator '%' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a || ts from binoptestts_ts;",
			),
			ExpErr: "operator '||' incompatible with type 'TIMESTAMP'",
		},
	},
}

var binOpExprWithTSIDSet = TableTest{
	Table: tbl(
		"binoptestts_ids",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("a", fldTypeTimestamp),
			srcHdr("b", fldTypeIDSet),
		),
		srcRows(
			srcRow(int64(1), time.Time(knownTimestamp()), []int64{101, 102}),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select a != b from binoptestts_ids;",
			),
			ExpErr: "types 'TIMESTAMP' and 'IDSET' are not equatable",
		},
		{
			SQLs: sqls(
				"select a = b from binoptestts_ids;",
			),
			ExpErr: "types 'TIMESTAMP' and 'IDSET' are not equatable",
		},
		{
			SQLs: sqls(
				"select a <= b from binoptestts_ids;",
			),
			ExpErr: " operator '<=' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a >= b from binoptestts_ids;",
			),
			ExpErr: " operator '>=' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a < b from binoptestts_ids;",
			),
			ExpErr: " operator '<' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a > b from binoptestts_ids;",
			),
			ExpErr: " operator '>' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a & b from binoptestts_ids;",
			),
			ExpErr: " operator '&' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a | b from binoptestts_ids;",
			),
			ExpErr: " operator '|' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a << b from binoptestts_ids;",
			),
			ExpErr: " operator '<<' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a >> b from binoptestts_ids;",
			),
			ExpErr: " operator '>>' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a + b from binoptestts_ids;",
			),
			ExpErr: " operator '+' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a - b from binoptestts_ids;",
			),
			ExpErr: " operator '-' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a * b from binoptestts_ids;",
			),
			ExpErr: " operator '*' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a / b from binoptestts_ids;",
			),
			ExpErr: " operator '/' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a % b from binoptestts_ids;",
			),
			ExpErr: " operator '%' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a || b from binoptestts_ids;",
			),
			ExpErr: "operator '||' incompatible with type 'TIMESTAMP'",
		},
	},
}

var binOpExprWithTSString = TableTest{
	Table: tbl(
		"binoptestts_s",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("a", fldTypeTimestamp),
			srcHdr("b", fldTypeString),
		),
		srcRows(
			srcRow(int64(1), time.Time(knownTimestamp()), string("101")),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select a != b from binoptestts_s;",
			),
			ExpErr: "types 'TIMESTAMP' and 'STRING' are not equatable",
		},
		{
			SQLs: sqls(
				"select a = b from binoptestts_s;",
			),
			ExpErr: "types 'TIMESTAMP' and 'STRING' are not equatable",
		},
		{
			SQLs: sqls(
				"select a <= b from binoptestts_s;",
			),
			ExpErr: " operator '<=' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a >= b from binoptestts_s;",
			),
			ExpErr: " operator '>=' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a < b from binoptestts_s;",
			),
			ExpErr: " operator '<' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a > b from binoptestts_s;",
			),
			ExpErr: " operator '>' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a & b from binoptestts_s;",
			),
			ExpErr: " operator '&' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a | b from binoptestts_s;",
			),
			ExpErr: " operator '|' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a << b from binoptestts_s;",
			),
			ExpErr: " operator '<<' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a >> b from binoptestts_s;",
			),
			ExpErr: " operator '>>' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a + b from binoptestts_s;",
			),
			ExpErr: " operator '+' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a - b from binoptestts_s;",
			),
			ExpErr: " operator '-' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a * b from binoptestts_s;",
			),
			ExpErr: " operator '*' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a / b from binoptestts_s;",
			),
			ExpErr: " operator '/' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a % b from binoptestts_s;",
			),
			ExpErr: " operator '%' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a || b from binoptestts_s;",
			),
			ExpErr: "operator '||' incompatible with type 'TIMESTAMP'",
		},
	},
}

var binOpExprWithTSStringSet = TableTest{
	Table: tbl(
		"binoptestts_ss",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("a", fldTypeTimestamp),
			srcHdr("b", fldTypeStringSet),
		),
		srcRows(
			srcRow(int64(1), time.Time(knownTimestamp()), []string{"101", "102"}),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select a != b from binoptestts_ss;",
			),
			ExpErr: "types 'TIMESTAMP' and 'STRINGSET' are not equatable",
		},
		{
			SQLs: sqls(
				"select a = b from binoptestts_ss;",
			),
			ExpErr: "types 'TIMESTAMP' and 'STRINGSET' are not equatable",
		},
		{
			SQLs: sqls(
				"select a <= b from binoptestts_ss;",
			),
			ExpErr: " operator '<=' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a >= b from binoptestts_ss;",
			),
			ExpErr: " operator '>=' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a < b from binoptestts_ss;",
			),
			ExpErr: " operator '<' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a > b from binoptestts_ss;",
			),
			ExpErr: " operator '>' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a & b from binoptestts_ss;",
			),
			ExpErr: " operator '&' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a | b from binoptestts_ss;",
			),
			ExpErr: " operator '|' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a << b from binoptestts_ss;",
			),
			ExpErr: " operator '<<' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a >> b from binoptestts_ss;",
			),
			ExpErr: " operator '>>' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a + b from binoptestts_ss;",
			),
			ExpErr: " operator '+' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a - b from binoptestts_ss;",
			),
			ExpErr: " operator '-' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a * b from binoptestts_ss;",
			),
			ExpErr: " operator '*' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a / b from binoptestts_ss;",
			),
			ExpErr: " operator '/' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a % b from binoptestts_ss;",
			),
			ExpErr: " operator '%' incompatible with type 'TIMESTAMP'",
		},
		{
			SQLs: sqls(
				"select a || b from binoptestts_ss;",
			),
			ExpErr: "operator '||' incompatible with type 'TIMESTAMP'",
		},
	},
}

// IDSET bin op tests
var binOpExprWithIDSetInt = TableTest{
	Table: tbl(
		"binoptestids_i",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("b", fldTypeInt),
			srcHdr("d", fldTypeIDSet),
		),
		srcRows(
			srcRow(int64(10), int64(20), []int64{20, 21}),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select d != b from binoptestids_i;",
			),
			ExpErr: "types 'IDSET' and 'INT' are not equatable",
		},
		{
			SQLs: sqls(
				"select d = b from binoptestids_i;",
			),
			ExpErr: "types 'IDSET' and 'INT' are not equatable",
		},
		{
			SQLs: sqls(
				"select d <= b from binoptestids_i;",
			),
			ExpErr: "operator '<=' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select d >= b from binoptestids_i;",
			),
			ExpErr: "operator '>=' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select d < b from binoptestids_i;",
			),
			ExpErr: "operator '<' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select d > b from binoptestids_i;",
			),
			ExpErr: "operator '>' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select d & b from binoptestids_i;",
			),
			ExpErr: "operator '&' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select d | b from binoptestids_i;",
			),
			ExpErr: "operator '|' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select d << b from binoptestids_i;",
			),
			ExpErr: "operator '<<' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select d >> b from binoptestids_i;",
			),
			ExpErr: "operator '>>' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select d + b from binoptestids_i;",
			),
			ExpErr: "operator '+' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select d - b from binoptestids_i;",
			),
			ExpErr: "operator '-' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select d * b from binoptestids_i;",
			),
			ExpErr: "operator '*' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select d / b from binoptestids_i;",
			),
			ExpErr: "operator '/' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select d % b from binoptestids_i;",
			),
			ExpErr: "operator '%' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select d || b from binoptestids_i;",
			),
			ExpErr: "operator '||' incompatible with type 'IDSET'",
		},
	},
}

var binOpExprWithIDSetBool = TableTest{
	Table: tbl(
		"binoptestids_b",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("b", fldTypeBool),
			srcHdr("d", fldTypeIDSet),
		),
		srcRows(
			srcRow(int64(10), bool(true), []int64{20, 21}),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select d != b from binoptestids_b;",
			),
			ExpErr: "types 'IDSET' and 'BOOL' are not equatable",
		},
		{
			SQLs: sqls(
				"select d = b from binoptestids_b;",
			),
			ExpErr: "types 'IDSET' and 'BOOL' are not equatable",
		},
		{
			SQLs: sqls(
				"select d <= b from binoptestids_b;",
			),
			ExpErr: "operator '<=' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select d >= b from binoptestids_b;",
			),
			ExpErr: "operator '>=' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select d < b from binoptestids_b;",
			),
			ExpErr: "operator '<' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select d > b from binoptestids_b;",
			),
			ExpErr: "operator '>' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select d & b from binoptestids_b;",
			),
			ExpErr: "operator '&' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select d | b from binoptestids_b;",
			),
			ExpErr: "operator '|' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select d << b from binoptestids_b;",
			),
			ExpErr: "operator '<<' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select d >> b from binoptestids_b;",
			),
			ExpErr: "operator '>>' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select d + b from binoptestids_b;",
			),
			ExpErr: "operator '+' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select d - b from binoptestids_b;",
			),
			ExpErr: "operator '-' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select d * b from binoptestids_b;",
			),
			ExpErr: "operator '*' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select d / b from binoptestids_b;",
			),
			ExpErr: "operator '/' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select d % b from binoptestids_b;",
			),
			ExpErr: "operator '%' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select d || b from binoptestids_b;",
			),
			ExpErr: "operator '||' incompatible with type 'IDSET'",
		},
	},
}

var binOpExprWithIDSetID = TableTest{
	Table: tbl(
		"binoptestids_id",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("b", fldTypeID),
			srcHdr("d", fldTypeIDSet),
		),
		srcRows(
			srcRow(int64(10), int64(20), []int64{20, 21}),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select d != b from binoptestids_id;",
			),
			ExpErr: "types 'IDSET' and 'ID' are not equatable",
		},
		{
			SQLs: sqls(
				"select d = b from binoptestids_id;",
			),
			ExpErr: "types 'IDSET' and 'ID' are not equatable",
		},
		{
			SQLs: sqls(
				"select d <= b from binoptestids_id;",
			),
			ExpErr: "operator '<=' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select d >= b from binoptestids_id;",
			),
			ExpErr: "operator '>=' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select d < b from binoptestids_id;",
			),
			ExpErr: "operator '<' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select d > b from binoptestids_id;",
			),
			ExpErr: "operator '>' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select d & b from binoptestids_id;",
			),
			ExpErr: "operator '&' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select d | b from binoptestids_id;",
			),
			ExpErr: "operator '|' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select d << b from binoptestids_id;",
			),
			ExpErr: "operator '<<' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select d >> b from binoptestids_id;",
			),
			ExpErr: "operator '>>' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select d + b from binoptestids_id;",
			),
			ExpErr: "operator '+' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select d - b from binoptestids_id;",
			),
			ExpErr: "operator '-' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select d * b from binoptestids_id;",
			),
			ExpErr: "operator '*' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select d / b from binoptestids_id;",
			),
			ExpErr: "operator '/' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select d % b from binoptestids_id;",
			),
			ExpErr: "operator '%' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select d || b from binoptestids_id;",
			),
			ExpErr: "operator '||' incompatible with type 'IDSET'",
		},
	},
}

var binOpExprWithIDSetDecimal = TableTest{
	Table: tbl(
		"binoptestids_d",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("a", fldTypeIDSet),
			srcHdr("d", fldTypeDecimal2),
		),
		srcRows(
			srcRow(int64(1), []int64{20, 21}, float64(12.34)),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select a != d from binoptestids_d;",
			),
			ExpErr: "types 'IDSET' and 'DECIMAL(2)' are not equatable",
		},
		{
			SQLs: sqls(
				"select a = d from binoptestids_d;",
			),
			ExpErr: "types 'IDSET' and 'DECIMAL(2)' are not equatable",
		},
		{
			SQLs: sqls(
				"select a <= d from binoptestids_d;",
			),
			ExpErr: "operator '<=' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a >= d from binoptestids_d;",
			),
			ExpErr: "operator '>=' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a < d from binoptestids_d;",
			),
			ExpErr: "operator '<' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a > d from binoptestids_d;",
			),
			ExpErr: "operator '>' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a & d from binoptestids_d;",
			),
			ExpErr: "operator '&' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a | d from binoptestids_d;",
			),
			ExpErr: "operator '|' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a << d from binoptestids_d;",
			),
			ExpErr: "operator '<<' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a >> d from binoptestids_d;",
			),
			ExpErr: "operator '>>' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a + d from binoptestids_d;",
			),
			ExpErr: "operator '+' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a - d from binoptestids_d;",
			),
			ExpErr: "operator '-' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a * d from binoptestids_d;",
			),
			ExpErr: "operator '*' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a / d from binoptestids_d;",
			),
			ExpErr: "operator '/' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a % d from binoptestids_d;",
			),
			ExpErr: "operator '%' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a || d from binoptestids_d;",
			),
			ExpErr: "operator '||' incompatible with type 'IDSET'",
		},
	},
}

var binOpExprWithIDSetTimestamp = TableTest{
	Table: tbl(
		"binoptestids_ts",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("a", fldTypeIDSet),
			srcHdr("ts", fldTypeTimestamp),
		),
		srcRows(
			srcRow(int64(1), []int64{20, 21}, time.Time(knownTimestamp())),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select a != ts from binoptestids_ts;",
			),
			ExpErr: "types 'IDSET' and 'TIMESTAMP' are not equatable",
		},
		{
			SQLs: sqls(
				"select a = ts from binoptestids_ts;",
			),
			ExpErr: "types 'IDSET' and 'TIMESTAMP' are not equatable",
		},
		{
			SQLs: sqls(
				"select a <= ts from binoptestids_ts;",
			),
			ExpErr: " operator '<=' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a >= ts from binoptestids_ts;",
			),
			ExpErr: " operator '>=' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a < ts from binoptestids_ts;",
			),
			ExpErr: " operator '<' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a > ts from binoptestids_ts;",
			),
			ExpErr: " operator '>' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a & ts from binoptestids_ts;",
			),
			ExpErr: "operator '&' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a | ts from binoptestids_ts;",
			),
			ExpErr: "operator '|' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a << ts from binoptestids_ts;",
			),
			ExpErr: "operator '<<' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a >> ts from binoptestids_ts;",
			),
			ExpErr: "operator '>>' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a + ts from binoptestids_ts;",
			),
			ExpErr: "operator '+' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a - ts from binoptestids_ts;",
			),
			ExpErr: "operator '-' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a * ts from binoptestids_ts;",
			),
			ExpErr: "operator '*' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a / ts from binoptestids_ts;",
			),
			ExpErr: "operator '/' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a % ts from binoptestids_ts;",
			),
			ExpErr: "operator '%' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a || ts from binoptestids_ts;",
			),
			ExpErr: "operator '||' incompatible with type 'IDSET'",
		},
	},
}

var binOpExprWithIDSetIDSet = TableTest{
	Table: tbl(
		"binoptestids_ids",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("a", fldTypeIDSet),
			srcHdr("b", fldTypeIDSet),
		),
		srcRows(
			srcRow(int64(1), []int64{101, 103}, []int64{101, 102}),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select a != b from binoptestids_ids;",
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
				"select a = b from binoptestids_ids;",
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
				"select a <= b from binoptestids_ids;",
			),
			ExpErr: " operator '<=' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a >= b from binoptestids_ids;",
			),
			ExpErr: " operator '>=' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a < b from binoptestids_ids;",
			),
			ExpErr: " operator '<' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a > b from binoptestids_ids;",
			),
			ExpErr: " operator '>' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a & b from binoptestids_ids;",
			),
			ExpErr: " operator '&' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a | b from binoptestids_ids;",
			),
			ExpErr: " operator '|' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a << b from binoptestids_ids;",
			),
			ExpErr: " operator '<<' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a >> b from binoptestids_ids;",
			),
			ExpErr: " operator '>>' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a + b from binoptestids_ids;",
			),
			ExpErr: " operator '+' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a - b from binoptestids_ids;",
			),
			ExpErr: " operator '-' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a * b from binoptestids_ids;",
			),
			ExpErr: " operator '*' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a / b from binoptestids_ids;",
			),
			ExpErr: " operator '/' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a % b from binoptestids_ids;",
			),
			ExpErr: " operator '%' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a || b from binoptestids_ids;",
			),
			ExpErr: "operator '||' incompatible with type 'IDSET'",
		},
	},
}

var binOpExprWithIDSetString = TableTest{
	Table: tbl(
		"binoptestids_s",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("a", fldTypeIDSet),
			srcHdr("b", fldTypeString),
		),
		srcRows(
			srcRow(int64(1), []int64{101, 102}, string("101")),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select a != b from binoptestids_s;",
			),
			ExpErr: "types 'IDSET' and 'STRING' are not equatable",
		},
		{
			SQLs: sqls(
				"select a = b from binoptestids_s;",
			),
			ExpErr: "types 'IDSET' and 'STRING' are not equatable",
		},
		{
			SQLs: sqls(
				"select a <= b from binoptestids_s;",
			),
			ExpErr: " operator '<=' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a >= b from binoptestids_s;",
			),
			ExpErr: " operator '>=' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a < b from binoptestids_s;",
			),
			ExpErr: " operator '<' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a > b from binoptestids_s;",
			),
			ExpErr: " operator '>' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a & b from binoptestids_s;",
			),
			ExpErr: " operator '&' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a | b from binoptestids_s;",
			),
			ExpErr: " operator '|' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a << b from binoptestids_s;",
			),
			ExpErr: " operator '<<' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a >> b from binoptestids_s;",
			),
			ExpErr: " operator '>>' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a + b from binoptestids_s;",
			),
			ExpErr: " operator '+' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a - b from binoptestids_s;",
			),
			ExpErr: " operator '-' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a * b from binoptestids_s;",
			),
			ExpErr: " operator '*' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a / b from binoptestids_s;",
			),
			ExpErr: " operator '/' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a % b from binoptestids_s;",
			),
			ExpErr: " operator '%' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a || b from binoptestids_s;",
			),
			ExpErr: "operator '||' incompatible with type 'IDSET'",
		},
	},
}

var binOpExprWithIDSetStringSet = TableTest{
	Table: tbl(
		"binoptestids_ss",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("a", fldTypeIDSet),
			srcHdr("b", fldTypeStringSet),
		),
		srcRows(
			srcRow(int64(1), []int64{102, 103}, []string{"101", "102"}),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select a != b from binoptestids_ss;",
			),
			ExpErr: "types 'IDSET' and 'STRINGSET' are not equatable",
		},
		{
			SQLs: sqls(
				"select a = b from binoptestids_ss;",
			),
			ExpErr: "types 'IDSET' and 'STRINGSET' are not equatable",
		},
		{
			SQLs: sqls(
				"select a <= b from binoptestids_ss;",
			),
			ExpErr: " operator '<=' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a >= b from binoptestids_ss;",
			),
			ExpErr: " operator '>=' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a < b from binoptestids_ss;",
			),
			ExpErr: " operator '<' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a > b from binoptestids_ss;",
			),
			ExpErr: " operator '>' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a & b from binoptestids_ss;",
			),
			ExpErr: " operator '&' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a | b from binoptestids_ss;",
			),
			ExpErr: " operator '|' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a << b from binoptestids_ss;",
			),
			ExpErr: " operator '<<' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a >> b from binoptestids_ss;",
			),
			ExpErr: " operator '>>' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a + b from binoptestids_ss;",
			),
			ExpErr: " operator '+' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a - b from binoptestids_ss;",
			),
			ExpErr: " operator '-' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a * b from binoptestids_ss;",
			),
			ExpErr: " operator '*' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a / b from binoptestids_ss;",
			),
			ExpErr: " operator '/' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a % b from binoptestids_ss;",
			),
			ExpErr: " operator '%' incompatible with type 'IDSET'",
		},
		{
			SQLs: sqls(
				"select a || b from binoptestids_ss;",
			),
			ExpErr: "operator '||' incompatible with type 'IDSET'",
		},
	},
}

// STRING bin op tests
var binOpExprWithStringInt = TableTest{
	Table: tbl(
		"binoptests_i",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("b", fldTypeInt),
			srcHdr("d", fldTypeString),
		),
		srcRows(
			srcRow(int64(10), int64(20), string("foo")),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select d != b from binoptests_i;",
			),
			ExpErr: "types 'STRING' and 'INT' are not equatable",
		},
		{
			SQLs: sqls(
				"select d = b from binoptests_i;",
			),
			ExpErr: "types 'STRING' and 'INT' are not equatable",
		},
		{
			SQLs: sqls(
				"select d <= b from binoptests_i;",
			),
			ExpErr: "operator '<=' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select d >= b from binoptests_i;",
			),
			ExpErr: "operator '>=' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select d < b from binoptests_i;",
			),
			ExpErr: "operator '<' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select d > b from binoptests_i;",
			),
			ExpErr: "operator '>' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select d & b from binoptests_i;",
			),
			ExpErr: "operator '&' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select d | b from binoptests_i;",
			),
			ExpErr: "operator '|' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select d << b from binoptests_i;",
			),
			ExpErr: "operator '<<' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select d >> b from binoptests_i;",
			),
			ExpErr: "operator '>>' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select d + b from binoptests_i;",
			),
			ExpErr: "operator '+' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select d - b from binoptests_i;",
			),
			ExpErr: "operator '-' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select d * b from binoptests_i;",
			),
			ExpErr: "operator '*' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select d / b from binoptests_i;",
			),
			ExpErr: "operator '/' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select d % b from binoptests_i;",
			),
			ExpErr: "operator '%' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select d || b from binoptests_i;",
			),
			ExpErr: "operator '||' incompatible with type 'INT'",
		},
	},
}

var binOpExprWithStringBool = TableTest{
	Table: tbl(
		"binoptests_b",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("b", fldTypeBool),
			srcHdr("d", fldTypeString),
		),
		srcRows(
			srcRow(int64(10), bool(true), string("foo")),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select d != b from binoptests_b;",
			),
			ExpErr: "types 'STRING' and 'BOOL' are not equatable",
		},
		{
			SQLs: sqls(
				"select d = b from binoptests_b;",
			),
			ExpErr: "types 'STRING' and 'BOOL' are not equatable",
		},
		{
			SQLs: sqls(
				"select d <= b from binoptests_b;",
			),
			ExpErr: "operator '<=' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select d >= b from binoptests_b;",
			),
			ExpErr: "operator '>=' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select d < b from binoptests_b;",
			),
			ExpErr: "operator '<' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select d > b from binoptests_b;",
			),
			ExpErr: "operator '>' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select d & b from binoptests_b;",
			),
			ExpErr: "operator '&' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select d | b from binoptests_b;",
			),
			ExpErr: "operator '|' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select d << b from binoptests_b;",
			),
			ExpErr: "operator '<<' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select d >> b from binoptests_b;",
			),
			ExpErr: "operator '>>' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select d + b from binoptests_b;",
			),
			ExpErr: "operator '+' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select d - b from binoptests_b;",
			),
			ExpErr: "operator '-' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select d * b from binoptests_b;",
			),
			ExpErr: "operator '*' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select d / b from binoptests_b;",
			),
			ExpErr: "operator '/' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select d % b from binoptests_b;",
			),
			ExpErr: "operator '%' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select d || b from binoptests_b;",
			),
			ExpErr: "operator '||' incompatible with type 'BOOL'",
		},
	},
}

var binOpExprWithStringID = TableTest{
	Table: tbl(
		"binoptests_id",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("b", fldTypeID),
			srcHdr("d", fldTypeString),
		),
		srcRows(
			srcRow(int64(10), int64(20), string("foo")),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select d != b from binoptests_id;",
			),
			ExpErr: "types 'STRING' and 'ID' are not equatable",
		},
		{
			SQLs: sqls(
				"select d = b from binoptests_id;",
			),
			ExpErr: "types 'STRING' and 'ID' are not equatable",
		},
		{
			SQLs: sqls(
				"select d <= b from binoptests_id;",
			),
			ExpErr: "operator '<=' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select d >= b from binoptests_id;",
			),
			ExpErr: "operator '>=' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select d < b from binoptests_id;",
			),
			ExpErr: "operator '<' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select d > b from binoptests_id;",
			),
			ExpErr: "operator '>' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select d & b from binoptests_id;",
			),
			ExpErr: "operator '&' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select d | b from binoptests_id;",
			),
			ExpErr: "operator '|' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select d << b from binoptests_id;",
			),
			ExpErr: "operator '<<' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select d >> b from binoptests_id;",
			),
			ExpErr: "operator '>>' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select d + b from binoptests_id;",
			),
			ExpErr: "operator '+' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select d - b from binoptests_id;",
			),
			ExpErr: "operator '-' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select d * b from binoptests_id;",
			),
			ExpErr: "operator '*' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select d / b from binoptests_id;",
			),
			ExpErr: "operator '/' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select d % b from binoptests_id;",
			),
			ExpErr: "operator '%' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select d || b from binoptests_id;",
			),
			ExpErr: "operator '||' incompatible with type 'ID'",
		},
	},
}

var binOpExprWithStringDecimal = TableTest{
	Table: tbl(
		"binoptests_d",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("a", fldTypeString),
			srcHdr("d", fldTypeDecimal2),
		),
		srcRows(
			srcRow(int64(1), string("foo"), float64(12.34)),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select a != d from binoptests_d;",
			),
			ExpErr: "types 'STRING' and 'DECIMAL(2)' are not equatable",
		},
		{
			SQLs: sqls(
				"select a = d from binoptests_d;",
			),
			ExpErr: "types 'STRING' and 'DECIMAL(2)' are not equatable",
		},
		{
			SQLs: sqls(
				"select a <= d from binoptests_d;",
			),
			ExpErr: "operator '<=' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a >= d from binoptests_d;",
			),
			ExpErr: "operator '>=' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a < d from binoptests_d;",
			),
			ExpErr: "operator '<' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a > d from binoptests_d;",
			),
			ExpErr: "operator '>' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a & d from binoptests_d;",
			),
			ExpErr: "operator '&' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a | d from binoptests_d;",
			),
			ExpErr: "operator '|' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a << d from binoptests_d;",
			),
			ExpErr: "operator '<<' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a >> d from binoptests_d;",
			),
			ExpErr: "operator '>>' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a + d from binoptests_d;",
			),
			ExpErr: "operator '+' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a - d from binoptests_d;",
			),
			ExpErr: "operator '-' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a * d from binoptests_d;",
			),
			ExpErr: "operator '*' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a / d from binoptests_d;",
			),
			ExpErr: "operator '/' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a % d from binoptests_d;",
			),
			ExpErr: "operator '%' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a || d from binoptests_d;",
			),
			ExpErr: "operator '||' incompatible with type 'DECIMAL(2)'",
		},
	},
}

var binOpExprWithStringTimestamp = TableTest{
	Table: tbl(
		"binoptests_ts",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("a", fldTypeString),
			srcHdr("ts", fldTypeTimestamp),
		),
		srcRows(
			srcRow(int64(1), string("foo"), time.Time(knownTimestamp())),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select a != ts from binoptests_ts;",
			),
			ExpErr: "types 'STRING' and 'TIMESTAMP' are not equatable",
		},
		{
			SQLs: sqls(
				"select a = ts from binoptests_ts;",
			),
			ExpErr: "types 'STRING' and 'TIMESTAMP' are not equatable",
		},
		{
			SQLs: sqls(
				"select a <= ts from binoptests_ts;",
			),
			ExpErr: " operator '<=' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a >= ts from binoptests_ts;",
			),
			ExpErr: " operator '>=' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a < ts from binoptests_ts;",
			),
			ExpErr: " operator '<' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a > ts from binoptests_ts;",
			),
			ExpErr: " operator '>' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a & ts from binoptests_ts;",
			),
			ExpErr: "operator '&' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a | ts from binoptests_ts;",
			),
			ExpErr: "operator '|' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a << ts from binoptests_ts;",
			),
			ExpErr: "operator '<<' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a >> ts from binoptests_ts;",
			),
			ExpErr: "operator '>>' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a + ts from binoptests_ts;",
			),
			ExpErr: "operator '+' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a - ts from binoptests_ts;",
			),
			ExpErr: "operator '-' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a * ts from binoptests_ts;",
			),
			ExpErr: "operator '*' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a / ts from binoptests_ts;",
			),
			ExpErr: "operator '/' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a % ts from binoptests_ts;",
			),
			ExpErr: "operator '%' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a || ts from binoptests_ts;",
			),
			ExpErr: "operator '||' incompatible with type 'TIMESTAMP'",
		},
	},
}

var binOpExprWithStringIDSet = TableTest{
	Table: tbl(
		"binoptests_ids",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("a", fldTypeString),
			srcHdr("b", fldTypeIDSet),
		),
		srcRows(
			srcRow(int64(1), string("foo"), []int64{101, 102}),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select a != b from binoptests_ids;",
			),
			ExpErr: "types 'STRING' and 'IDSET' are not equatable",
		},
		{
			SQLs: sqls(
				"select a = b from binoptests_ids;",
			),
			ExpErr: "types 'STRING' and 'IDSET' are not equatable",
		},
		{
			SQLs: sqls(
				"select a <= b from binoptests_ids;",
			),
			ExpErr: " operator '<=' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a >= b from binoptests_ids;",
			),
			ExpErr: " operator '>=' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a < b from binoptests_ids;",
			),
			ExpErr: " operator '<' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a > b from binoptests_ids;",
			),
			ExpErr: " operator '>' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a & b from binoptests_ids;",
			),
			ExpErr: " operator '&' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a | b from binoptests_ids;",
			),
			ExpErr: " operator '|' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a << b from binoptests_ids;",
			),
			ExpErr: " operator '<<' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a >> b from binoptests_ids;",
			),
			ExpErr: " operator '>>' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a + b from binoptests_ids;",
			),
			ExpErr: " operator '+' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a - b from binoptests_ids;",
			),
			ExpErr: " operator '-' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a * b from binoptests_ids;",
			),
			ExpErr: " operator '*' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a / b from binoptests_ids;",
			),
			ExpErr: " operator '/' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a % b from binoptests_ids;",
			),
			ExpErr: " operator '%' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a || b from binoptests_ids;",
			),
			ExpErr: "operator '||' incompatible with type 'IDSET'",
		},
	},
}

var binOpExprWithStringString = TableTest{
	Table: tbl(
		"binoptests_s",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("a", fldTypeString),
			srcHdr("b", fldTypeString),
		),
		srcRows(
			srcRow(int64(1), string("foo"), string("101")),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select a != b from binoptests_s;",
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
				"select a = b from binoptests_s;",
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
				"select a <= b from binoptests_s;",
			),
			ExpErr: " operator '<=' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a >= b from binoptests_s;",
			),
			ExpErr: " operator '>=' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a < b from binoptests_s;",
			),
			ExpErr: " operator '<' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a > b from binoptests_s;",
			),
			ExpErr: " operator '>' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a & b from binoptests_s;",
			),
			ExpErr: " operator '&' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a | b from binoptests_s;",
			),
			ExpErr: " operator '|' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a << b from binoptests_s;",
			),
			ExpErr: " operator '<<' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a >> b from binoptests_s;",
			),
			ExpErr: " operator '>>' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a + b from binoptests_s;",
			),
			ExpErr: " operator '+' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a - b from binoptests_s;",
			),
			ExpErr: " operator '-' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a * b from binoptests_s;",
			),
			ExpErr: " operator '*' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a / b from binoptests_s;",
			),
			ExpErr: " operator '/' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a % b from binoptests_s;",
			),
			ExpErr: " operator '%' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a || b from binoptests_s;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(string("foo101")),
			),
			Compare: CompareExactUnordered,
		},
	},
}

var binOpExprWithStringStringSet = TableTest{
	Table: tbl(
		"binoptests_ss",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("a", fldTypeString),
			srcHdr("b", fldTypeStringSet),
		),
		srcRows(
			srcRow(int64(1), string("foo"), []string{"101", "102"}),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select a != b from binoptests_ss;",
			),
			ExpErr: "types 'STRING' and 'STRINGSET' are not equatable",
		},
		{
			SQLs: sqls(
				"select a = b from binoptests_ss;",
			),
			ExpErr: "types 'STRING' and 'STRINGSET' are not equatable",
		},
		{
			SQLs: sqls(
				"select a <= b from binoptests_ss;",
			),
			ExpErr: " operator '<=' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a >= b from binoptests_ss;",
			),
			ExpErr: " operator '>=' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a < b from binoptests_ss;",
			),
			ExpErr: " operator '<' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a > b from binoptests_ss;",
			),
			ExpErr: " operator '>' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a & b from binoptests_ss;",
			),
			ExpErr: " operator '&' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a | b from binoptests_ss;",
			),
			ExpErr: " operator '|' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a << b from binoptests_ss;",
			),
			ExpErr: " operator '<<' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a >> b from binoptests_ss;",
			),
			ExpErr: " operator '>>' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a + b from binoptests_ss;",
			),
			ExpErr: " operator '+' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a - b from binoptests_ss;",
			),
			ExpErr: " operator '-' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a * b from binoptests_ss;",
			),
			ExpErr: " operator '*' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a / b from binoptests_ss;",
			),
			ExpErr: " operator '/' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a % b from binoptests_ss;",
			),
			ExpErr: " operator '%' incompatible with type 'STRING'",
		},
		{
			SQLs: sqls(
				"select a || b from binoptests_ss;",
			),
			ExpErr: "operator '||' incompatible with type 'STRINGSET'",
		},
	},
}

// STRINGSET bin op tests
var binOpExprWithStringSetInt = TableTest{
	Table: tbl(
		"binoptestss_i",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("b", fldTypeInt),
			srcHdr("d", fldTypeStringSet),
		),
		srcRows(
			srcRow(int64(10), int64(20), []string{"20", "21"}),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select d != b from binoptestss_i;",
			),
			ExpErr: "types 'STRINGSET' and 'INT' are not equatable",
		},
		{
			SQLs: sqls(
				"select d = b from binoptestss_i;",
			),
			ExpErr: "types 'STRINGSET' and 'INT' are not equatable",
		},
		{
			SQLs: sqls(
				"select d <= b from binoptestss_i;",
			),
			ExpErr: "operator '<=' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select d >= b from binoptestss_i;",
			),
			ExpErr: "operator '>=' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select d < b from binoptestss_i;",
			),
			ExpErr: "operator '<' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select d > b from binoptestss_i;",
			),
			ExpErr: "operator '>' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select d & b from binoptestss_i;",
			),
			ExpErr: "operator '&' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select d | b from binoptestss_i;",
			),
			ExpErr: "operator '|' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select d << b from binoptestss_i;",
			),
			ExpErr: "operator '<<' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select d >> b from binoptestss_i;",
			),
			ExpErr: "operator '>>' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select d + b from binoptestss_i;",
			),
			ExpErr: "operator '+' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select d - b from binoptestss_i;",
			),
			ExpErr: "operator '-' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select d * b from binoptestss_i;",
			),
			ExpErr: "operator '*' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select d / b from binoptestss_i;",
			),
			ExpErr: "operator '/' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select d % b from binoptestss_i;",
			),
			ExpErr: "operator '%' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select d || b from binoptestss_i;",
			),
			ExpErr: "operator '||' incompatible with type 'STRINGSET'",
		},
	},
}

var binOpExprWithStringSetBool = TableTest{
	Table: tbl(
		"binoptestss_b",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("b", fldTypeBool),
			srcHdr("d", fldTypeStringSet),
		),
		srcRows(
			srcRow(int64(10), bool(true), []string{"20", "21"}),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select d != b from binoptestss_b;",
			),
			ExpErr: "types 'STRINGSET' and 'BOOL' are not equatable",
		},
		{
			SQLs: sqls(
				"select d = b from binoptestss_b;",
			),
			ExpErr: "types 'STRINGSET' and 'BOOL' are not equatable",
		},
		{
			SQLs: sqls(
				"select d <= b from binoptestss_b;",
			),
			ExpErr: "operator '<=' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select d >= b from binoptestss_b;",
			),
			ExpErr: "operator '>=' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select d < b from binoptestss_b;",
			),
			ExpErr: "operator '<' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select d > b from binoptestss_b;",
			),
			ExpErr: "operator '>' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select d & b from binoptestss_b;",
			),
			ExpErr: "operator '&' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select d | b from binoptestss_b;",
			),
			ExpErr: "operator '|' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select d << b from binoptestss_b;",
			),
			ExpErr: "operator '<<' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select d >> b from binoptestss_b;",
			),
			ExpErr: "operator '>>' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select d + b from binoptestss_b;",
			),
			ExpErr: "operator '+' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select d - b from binoptestss_b;",
			),
			ExpErr: "operator '-' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select d * b from binoptestss_b;",
			),
			ExpErr: "operator '*' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select d / b from binoptestss_b;",
			),
			ExpErr: "operator '/' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select d % b from binoptestss_b;",
			),
			ExpErr: "operator '%' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select d || b from binoptestss_b;",
			),
			ExpErr: "operator '||' incompatible with type 'STRINGSET'",
		},
	},
}

var binOpExprWithStringSetID = TableTest{
	Table: tbl(
		"binoptestss_id",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("b", fldTypeID),
			srcHdr("d", fldTypeStringSet),
		),
		srcRows(
			srcRow(int64(10), int64(20), []string{"20", "21"}),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select d != b from binoptestss_id;",
			),
			ExpErr: "types 'STRINGSET' and 'ID' are not equatable",
		},
		{
			SQLs: sqls(
				"select d = b from binoptestss_id;",
			),
			ExpErr: "types 'STRINGSET' and 'ID' are not equatable",
		},
		{
			SQLs: sqls(
				"select d <= b from binoptestss_id;",
			),
			ExpErr: "operator '<=' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select d >= b from binoptestss_id;",
			),
			ExpErr: "operator '>=' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select d < b from binoptestss_id;",
			),
			ExpErr: "operator '<' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select d > b from binoptestss_id;",
			),
			ExpErr: "operator '>' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select d & b from binoptestss_id;",
			),
			ExpErr: "operator '&' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select d | b from binoptestss_id;",
			),
			ExpErr: "operator '|' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select d << b from binoptestss_id;",
			),
			ExpErr: "operator '<<' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select d >> b from binoptestss_id;",
			),
			ExpErr: "operator '>>' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select d + b from binoptestss_id;",
			),
			ExpErr: "operator '+' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select d - b from binoptestss_id;",
			),
			ExpErr: "operator '-' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select d * b from binoptestss_id;",
			),
			ExpErr: "operator '*' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select d / b from binoptestss_id;",
			),
			ExpErr: "operator '/' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select d % b from binoptestss_id;",
			),
			ExpErr: "operator '%' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select d || b from binoptestss_id;",
			),
			ExpErr: "operator '||' incompatible with type 'STRINGSET'",
		},
	},
}

var binOpExprWithStringSetDecimal = TableTest{
	Table: tbl(
		"binoptestss_d",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("a", fldTypeStringSet),
			srcHdr("d", fldTypeDecimal2),
		),
		srcRows(
			srcRow(int64(1), []string{"20", "21"}, float64(12.34)),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select a != d from binoptestss_d;",
			),
			ExpErr: "types 'STRINGSET' and 'DECIMAL(2)' are not equatable",
		},
		{
			SQLs: sqls(
				"select a = d from binoptestss_d;",
			),
			ExpErr: "types 'STRINGSET' and 'DECIMAL(2)' are not equatable",
		},
		{
			SQLs: sqls(
				"select a <= d from binoptestss_d;",
			),
			ExpErr: "operator '<=' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a >= d from binoptestss_d;",
			),
			ExpErr: "operator '>=' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a < d from binoptestss_d;",
			),
			ExpErr: "operator '<' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a > d from binoptestss_d;",
			),
			ExpErr: "operator '>' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a & d from binoptestss_d;",
			),
			ExpErr: "operator '&' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a | d from binoptestss_d;",
			),
			ExpErr: "operator '|' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a << d from binoptestss_d;",
			),
			ExpErr: "operator '<<' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a >> d from binoptestss_d;",
			),
			ExpErr: "operator '>>' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a + d from binoptestss_d;",
			),
			ExpErr: "operator '+' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a - d from binoptestss_d;",
			),
			ExpErr: "operator '-' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a * d from binoptestss_d;",
			),
			ExpErr: "operator '*' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a / d from binoptestss_d;",
			),
			ExpErr: "operator '/' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a % d from binoptestss_d;",
			),
			ExpErr: "operator '%' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a || d from binoptestss_d;",
			),
			ExpErr: "operator '||' incompatible with type 'STRINGSET'",
		},
	},
}

var binOpExprWithStringSetTimestamp = TableTest{
	Table: tbl(
		"binoptestss_ts",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("a", fldTypeStringSet),
			srcHdr("ts", fldTypeTimestamp),
		),
		srcRows(
			srcRow(int64(1), []string{"20", "21"}, time.Time(knownTimestamp())),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select a != ts from binoptestss_ts;",
			),
			ExpErr: "types 'STRINGSET' and 'TIMESTAMP' are not equatable",
		},
		{
			SQLs: sqls(
				"select a = ts from binoptestss_ts;",
			),
			ExpErr: "types 'STRINGSET' and 'TIMESTAMP' are not equatable",
		},
		{
			SQLs: sqls(
				"select a <= ts from binoptestss_ts;",
			),
			ExpErr: " operator '<=' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a >= ts from binoptestss_ts;",
			),
			ExpErr: " operator '>=' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a < ts from binoptestss_ts;",
			),
			ExpErr: " operator '<' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a > ts from binoptestss_ts;",
			),
			ExpErr: " operator '>' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a & ts from binoptestss_ts;",
			),
			ExpErr: "operator '&' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a | ts from binoptestss_ts;",
			),
			ExpErr: "operator '|' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a << ts from binoptestss_ts;",
			),
			ExpErr: "operator '<<' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a >> ts from binoptestss_ts;",
			),
			ExpErr: "operator '>>' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a + ts from binoptestss_ts;",
			),
			ExpErr: "operator '+' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a - ts from binoptestss_ts;",
			),
			ExpErr: "operator '-' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a * ts from binoptestss_ts;",
			),
			ExpErr: "operator '*' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a / ts from binoptestss_ts;",
			),
			ExpErr: "operator '/' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a % ts from binoptestss_ts;",
			),
			ExpErr: "operator '%' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a || ts from binoptestss_ts;",
			),
			ExpErr: "operator '||' incompatible with type 'STRINGSET'",
		},
	},
}

var binOpExprWithStringSetIDSet = TableTest{
	Table: tbl(
		"binoptestss_ids",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("a", fldTypeStringSet),
			srcHdr("b", fldTypeIDSet),
		),
		srcRows(
			srcRow(int64(1), []string{"101", "103"}, []int64{101, 102}),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select a != b from binoptestss_ids;",
			),
			ExpErr: "types 'STRINGSET' and 'IDSET' are not equatable",
		},
		{
			SQLs: sqls(
				"select a = b from binoptestss_ids;",
			),
			ExpErr: "types 'STRINGSET' and 'IDSET' are not equatable",
		},
		{
			SQLs: sqls(
				"select a <= b from binoptestss_ids;",
			),
			ExpErr: " operator '<=' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a >= b from binoptestss_ids;",
			),
			ExpErr: " operator '>=' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a < b from binoptestss_ids;",
			),
			ExpErr: " operator '<' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a > b from binoptestss_ids;",
			),
			ExpErr: " operator '>' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a & b from binoptestss_ids;",
			),
			ExpErr: " operator '&' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a | b from binoptestss_ids;",
			),
			ExpErr: " operator '|' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a << b from binoptestss_ids;",
			),
			ExpErr: " operator '<<' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a >> b from binoptestss_ids;",
			),
			ExpErr: " operator '>>' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a + b from binoptestss_ids;",
			),
			ExpErr: " operator '+' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a - b from binoptestss_ids;",
			),
			ExpErr: " operator '-' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a * b from binoptestss_ids;",
			),
			ExpErr: " operator '*' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a / b from binoptestss_ids;",
			),
			ExpErr: " operator '/' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a % b from binoptestss_ids;",
			),
			ExpErr: " operator '%' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a || b from binoptestss_ids;",
			),
			ExpErr: "operator '||' incompatible with type 'STRINGSET'",
		},
	},
}

var binOpExprWithStringSetString = TableTest{
	Table: tbl(
		"binoptestss_s",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("a", fldTypeStringSet),
			srcHdr("b", fldTypeString),
		),
		srcRows(
			srcRow(int64(1), []string{"101", "102"}, string("101")),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select a != b from binoptestss_s;",
			),
			ExpErr: "types 'STRINGSET' and 'STRING' are not equatable",
		},
		{
			SQLs: sqls(
				"select a = b from binoptestss_s;",
			),
			ExpErr: "types 'STRINGSET' and 'STRING' are not equatable",
		},
		{
			SQLs: sqls(
				"select a <= b from binoptestss_s;",
			),
			ExpErr: " operator '<=' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a >= b from binoptestss_s;",
			),
			ExpErr: " operator '>=' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a < b from binoptestss_s;",
			),
			ExpErr: " operator '<' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a > b from binoptestss_s;",
			),
			ExpErr: " operator '>' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a & b from binoptestss_s;",
			),
			ExpErr: " operator '&' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a | b from binoptestss_s;",
			),
			ExpErr: " operator '|' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a << b from binoptestss_s;",
			),
			ExpErr: " operator '<<' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a >> b from binoptestss_s;",
			),
			ExpErr: " operator '>>' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a + b from binoptestss_s;",
			),
			ExpErr: " operator '+' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a - b from binoptestss_s;",
			),
			ExpErr: " operator '-' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a * b from binoptestss_s;",
			),
			ExpErr: " operator '*' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a / b from binoptestss_s;",
			),
			ExpErr: " operator '/' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a % b from binoptestss_s;",
			),
			ExpErr: " operator '%' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a || b from binoptestss_s;",
			),
			ExpErr: "operator '||' incompatible with type 'STRINGSET'",
		},
	},
}

var binOpExprWithStringSetStringSet = TableTest{
	Table: tbl(
		"binoptestss_ss",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("a", fldTypeStringSet),
			srcHdr("b", fldTypeStringSet),
		),
		srcRows(
			srcRow(int64(1), []string{"102", "103"}, []string{"101", "102"}),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select a != b from binoptestss_ss;",
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
				"select a = b from binoptestss_ss;",
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
				"select a <= b from binoptestss_ss;",
			),
			ExpErr: " operator '<=' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a >= b from binoptestss_ss;",
			),
			ExpErr: " operator '>=' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a < b from binoptestss_ss;",
			),
			ExpErr: " operator '<' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a > b from binoptestss_ss;",
			),
			ExpErr: " operator '>' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a & b from binoptestss_ss;",
			),
			ExpErr: " operator '&' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a | b from binoptestss_ss;",
			),
			ExpErr: " operator '|' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a << b from binoptestss_ss;",
			),
			ExpErr: " operator '<<' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a >> b from binoptestss_ss;",
			),
			ExpErr: " operator '>>' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a + b from binoptestss_ss;",
			),
			ExpErr: " operator '+' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a - b from binoptestss_ss;",
			),
			ExpErr: " operator '-' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a * b from binoptestss_ss;",
			),
			ExpErr: " operator '*' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a / b from binoptestss_ss;",
			),
			ExpErr: " operator '/' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a % b from binoptestss_ss;",
			),
			ExpErr: " operator '%' incompatible with type 'STRINGSET'",
		},
		{
			SQLs: sqls(
				"select a || b from binoptestss_ss;",
			),
			ExpErr: "operator '||' incompatible with type 'STRINGSET'",
		},
	},
}
