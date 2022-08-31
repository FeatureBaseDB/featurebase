package sql3_test

import "time"

//INT bin op tests
var binOpExprWithIntInt = tableTest{
	table: tbl(
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
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select a != b from binoptesti_i;",
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
				"select a = b from binoptesti_i;",
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
				"select a <= b from binoptesti_i;",
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
				"select a >= b from binoptesti_i;",
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
				"select a < b from binoptesti_i;",
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
				"select a > b from binoptesti_i;",
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
				"select a & b from binoptesti_i;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			expRows: rows(
				row(int64(0)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select a | b from binoptesti_i;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			expRows: rows(
				row(int64(30)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select a << b from binoptesti_i;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			expRows: rows(
				row(int64(10485760)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select a >> b from binoptesti_i;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			expRows: rows(
				row(int64(0)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select a + b from binoptesti_i;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			expRows: rows(
				row(int64(30)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select a - b from binoptesti_i;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			expRows: rows(
				row(int64(-10)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select a * b from binoptesti_i;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			expRows: rows(
				row(int64(200)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select a / b from binoptesti_i;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			expRows: rows(
				row(int64(0)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select a % b from binoptesti_i;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			expRows: rows(
				row(int64(10)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select a || b from binoptesti_i;",
			),
			expErr: "operator '||' incompatible with type 'INT'",
		},
	},
}

var binOpExprWithIntBool = tableTest{
	table: tbl(
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
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select a != b from binoptesti_b;",
			),
			expErr: "types 'INT' and 'BOOL' are not equatable",
		},
		{
			sqls: sqls(
				"select a = b from binoptesti_b;",
			),
			expErr: "types 'INT' and 'BOOL' are not equatable",
		},
		{
			sqls: sqls(
				"select a <= b from binoptesti_b;",
			),
			expErr: "operator '<=' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a >= b from binoptesti_b;",
			),
			expErr: "operator '>=' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a < b from binoptesti_b;",
			),
			expErr: "operator '<' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a > b from binoptesti_b;",
			),
			expErr: "operator '>' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a & b from binoptesti_b;",
			),
			expErr: "operator '&' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a | b from binoptesti_b;",
			),
			expErr: "operator '|' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a << b from binoptesti_b;",
			),
			expErr: "operator '<<' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a >> b from binoptesti_b;",
			),
			expErr: "operator '>>' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a + b from binoptesti_b;",
			),
			expErr: "operator '+' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a - b from binoptesti_b;",
			),
			expErr: "operator '-' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a * b from binoptesti_b;",
			),
			expErr: "operator '*' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a / b from binoptesti_b;",
			),
			expErr: "operator '/' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a % b from binoptesti_b;",
			),
			expErr: "operator '%' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a || b from binoptesti_b;",
			),
			expErr: "operator '||' incompatible with type 'INT'",
		},
	},
}

var binOpExprWithIntID = tableTest{
	table: tbl(
		"binoptesti_id",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("b", fldTypeInt, "min 0", "max 1000"),
		),
		srcRows(
			srcRow(int64(10), int64(20)),
		),
	),
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select b != _id from binoptesti_id;",
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
				"select b = _id from binoptesti_id;",
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
				"select b <= _id from binoptesti_id;",
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
				"select b >= _id from binoptesti_id;",
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
				"select b < _id from binoptesti_id;",
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
				"select b > _id from binoptesti_id;",
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
				"select b & _id from binoptesti_id;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			expRows: rows(
				row(int64(0)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select b | _id from binoptesti_id;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			expRows: rows(
				row(int64(30)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select b << _id from binoptesti_id;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			expRows: rows(
				row(int64(20480)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select b >> _id from binoptesti_id;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			expRows: rows(
				row(int64(0)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select b + _id from binoptesti_id;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			expRows: rows(
				row(int64(30)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select b - _id from binoptesti_id;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			expRows: rows(
				row(int64(10)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select b * _id from binoptesti_id;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			expRows: rows(
				row(int64(200)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select b / _id from binoptesti_id;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			expRows: rows(
				row(int64(2)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select b % _id from binoptesti_id;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			expRows: rows(
				row(int64(0)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select b || _id from binoptesti_id;",
			),
			expErr: "operator '||' incompatible with type 'INT'",
		},
	},
}

var binOpExprWithIntDecimal = tableTest{
	table: tbl(
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
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select a != d from binoptesti_d;",
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
				"select a = d from binoptesti_d;",
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
				"select a <= d from binoptesti_d;",
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
				"select a >= d from binoptesti_d;",
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
				"select a < d from binoptesti_d;",
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
				"select a > d from binoptesti_d;",
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
				"select a & d from binoptesti_d;",
			),
			expErr: "operator '&' incompatible with type 'DECIMAL(2)'",
		},
		{
			sqls: sqls(
				"select a | d from binoptesti_d;",
			),
			expErr: "operator '|' incompatible with type 'DECIMAL(2)'",
		},
		{
			sqls: sqls(
				"select a << d from binoptesti_d;",
			),
			expErr: "operator '<<' incompatible with type 'DECIMAL(2)'",
		},
		{
			sqls: sqls(
				"select a >> d from binoptesti_d;",
			),
			expErr: "operator '>>' incompatible with type 'DECIMAL(2)'",
		},
		{
			sqls: sqls(
				"select a + d from binoptesti_d;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeDecimal2),
			),
			expRows: rows(
				row(float64(32.34)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select a - d from binoptesti_d;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeDecimal2),
			),
			expRows: rows(
				row(float64(7.66)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select a * d from binoptesti_d;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeDecimal2),
			),
			expRows: rows(
				row(float64(246.8)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select a / d from binoptesti_d;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeDecimal2),
			),
			expRows: rows(
				//TODO(pok) this float64 thing is for the birds
				row(float64(1.6207455429497568)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select a % d from binoptesti_d;",
			),
			expErr: "operator '%' incompatible with type 'DECIMAL(2)'",
		},
		{
			sqls: sqls(
				"select a || d from binoptesti_d;",
			),
			expErr: "operator '||' incompatible with type 'INT'",
		},
	},
}

var binOpExprWithIntTimestamp = tableTest{
	table: tbl(
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
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select a != ts from binoptesti_ts;",
			),
			expErr: "types 'INT' and 'TIMESTAMP' are not equatable",
		},
		{
			sqls: sqls(
				"select a = ts from binoptesti_ts;",
			),
			expErr: "types 'INT' and 'TIMESTAMP' are not equatable",
		},
		{
			sqls: sqls(
				"select a <= ts from binoptesti_ts;",
			),
			expErr: "types 'INT' and 'TIMESTAMP' are not equatable",
		},
		{
			sqls: sqls(
				"select a >= ts from binoptesti_ts;",
			),
			expErr: "types 'INT' and 'TIMESTAMP' are not equatable",
		},
		{
			sqls: sqls(
				"select a < ts from binoptesti_ts;",
			),
			expErr: "types 'INT' and 'TIMESTAMP' are not equatable",
		},
		{
			sqls: sqls(
				"select a > ts from binoptesti_ts;",
			),
			expErr: "types 'INT' and 'TIMESTAMP' are not equatable",
		},
		{
			sqls: sqls(
				"select a & ts from binoptesti_ts;",
			),
			expErr: "operator '&' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a | ts from binoptesti_ts;",
			),
			expErr: "operator '|' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a << ts from binoptesti_ts;",
			),
			expErr: "operator '<<' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a >> ts from binoptesti_ts;",
			),
			expErr: "operator '>>' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a + ts from binoptesti_ts;",
			),
			expErr: "operator '+' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a - ts from binoptesti_ts;",
			),
			expErr: "operator '-' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a * ts from binoptesti_ts;",
			),
			expErr: "operator '*' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a / ts from binoptesti_ts;",
			),
			expErr: "operator '/' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a % ts from binoptesti_ts;",
			),
			expErr: "operator '%' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a || ts from binoptesti_ts;",
			),
			expErr: "operator '||' incompatible with type 'INT'",
		},
	},
}

var binOpExprWithIntIDSet = tableTest{
	table: tbl(
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
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select a != b from binoptesti_ids;",
			),
			expErr: "types 'INT' and 'IDSET' are not equatable",
		},
		{
			sqls: sqls(
				"select a = b from binoptesti_ids;",
			),
			expErr: "types 'INT' and 'IDSET' are not equatable",
		},
		{
			sqls: sqls(
				"select a <= b from binoptesti_ids;",
			),
			expErr: " operator '<=' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a >= b from binoptesti_ids;",
			),
			expErr: " operator '>=' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a < b from binoptesti_ids;",
			),
			expErr: " operator '<' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a > b from binoptesti_ids;",
			),
			expErr: " operator '>' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a & b from binoptesti_ids;",
			),
			expErr: " operator '&' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a | b from binoptesti_ids;",
			),
			expErr: " operator '|' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a << b from binoptesti_ids;",
			),
			expErr: " operator '<<' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a >> b from binoptesti_ids;",
			),
			expErr: " operator '>>' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a + b from binoptesti_ids;",
			),
			expErr: " operator '+' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a - b from binoptesti_ids;",
			),
			expErr: " operator '-' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a * b from binoptesti_ids;",
			),
			expErr: " operator '*' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a / b from binoptesti_ids;",
			),
			expErr: " operator '/' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a % b from binoptesti_ids;",
			),
			expErr: " operator '%' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a || b from binoptesti_ids;",
			),
			expErr: "operator '||' incompatible with type 'INT'",
		},
	},
}

var binOpExprWithIntString = tableTest{
	table: tbl(
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
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select a != b from binoptesti_s;",
			),
			expErr: "types 'INT' and 'STRING' are not equatable",
		},
		{
			sqls: sqls(
				"select a = b from binoptesti_s;",
			),
			expErr: "types 'INT' and 'STRING' are not equatable",
		},
		{
			sqls: sqls(
				"select a <= b from binoptesti_s;",
			),
			expErr: " operator '<=' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a >= b from binoptesti_s;",
			),
			expErr: " operator '>=' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a < b from binoptesti_s;",
			),
			expErr: " operator '<' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a > b from binoptesti_s;",
			),
			expErr: " operator '>' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a & b from binoptesti_s;",
			),
			expErr: " operator '&' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a | b from binoptesti_s;",
			),
			expErr: " operator '|' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a << b from binoptesti_s;",
			),
			expErr: " operator '<<' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a >> b from binoptesti_s;",
			),
			expErr: " operator '>>' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a + b from binoptesti_s;",
			),
			expErr: " operator '+' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a - b from binoptesti_s;",
			),
			expErr: " operator '-' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a * b from binoptesti_s;",
			),
			expErr: " operator '*' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a / b from binoptesti_s;",
			),
			expErr: " operator '/' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a % b from binoptesti_s;",
			),
			expErr: " operator '%' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a || b from binoptesti_s;",
			),
			expErr: "operator '||' incompatible with type 'INT'",
		},
	},
}

var binOpExprWithIntStringSet = tableTest{
	table: tbl(
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
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select a != b from binoptesti_ss;",
			),
			expErr: "types 'INT' and 'STRINGSET' are not equatable",
		},
		{
			sqls: sqls(
				"select a = b from binoptesti_ss;",
			),
			expErr: "types 'INT' and 'STRINGSET' are not equatable",
		},
		{
			sqls: sqls(
				"select a <= b from binoptesti_ss;",
			),
			expErr: " operator '<=' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a >= b from binoptesti_ss;",
			),
			expErr: " operator '>=' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a < b from binoptesti_ss;",
			),
			expErr: " operator '<' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a > b from binoptesti_ss;",
			),
			expErr: " operator '>' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a & b from binoptesti_ss;",
			),
			expErr: " operator '&' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a | b from binoptesti_ss;",
			),
			expErr: " operator '|' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a << b from binoptesti_ss;",
			),
			expErr: " operator '<<' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a >> b from binoptesti_ss;",
			),
			expErr: " operator '>>' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a + b from binoptesti_ss;",
			),
			expErr: " operator '+' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a - b from binoptesti_ss;",
			),
			expErr: " operator '-' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a * b from binoptesti_ss;",
			),
			expErr: " operator '*' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a / b from binoptesti_ss;",
			),
			expErr: " operator '/' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a % b from binoptesti_ss;",
			),
			expErr: " operator '%' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a || b from binoptesti_ss;",
			),
			expErr: "operator '||' incompatible with type 'INT'",
		},
	},
}

//BOOL bin op tests
var binOpExprWithBoolInt = tableTest{
	table: tbl(
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
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select a != b from binoptestb_i;",
			),
			expErr: "types 'BOOL' and 'INT' are not equatable",
		},
		{
			sqls: sqls(
				"select a = b from binoptestb_i;",
			),
			expErr: "types 'BOOL' and 'INT' are not equatable",
		},
		{
			sqls: sqls(
				"select a <= b from binoptestb_i;",
			),
			expErr: "operator '<=' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a >= b from binoptestb_i;",
			),
			expErr: "operator '>=' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a < b from binoptestb_i;",
			),
			expErr: "operator '<' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a > b from binoptestb_i;",
			),
			expErr: "operator '>' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a & b from binoptestb_i;",
			),
			expErr: "operator '&' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a | b from binoptestb_i;",
			),
			expErr: "operator '|' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a << b from binoptestb_i;",
			),
			expErr: "operator '<<' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a >> b from binoptestb_i;",
			),
			expErr: "operator '>>' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a + b from binoptestb_i;",
			),
			expErr: "operator '+' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a - b from binoptestb_i;",
			),
			expErr: "operator '-' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a * b from binoptestb_i;",
			),
			expErr: "operator '*' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a / b from binoptestb_i;",
			),
			expErr: "operator '/' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a % b from binoptestb_i;",
			),
			expErr: "operator '%' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a || b from binoptestb_i;",
			),
			expErr: "operator '||' incompatible with type 'BOOL'",
		},
	},
}

var binOpExprWithBoolBool = tableTest{
	table: tbl(
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
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select a != b from binoptestb_b;",
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
				"select a = b from binoptestb_b;",
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
				"select a <= b from binoptestb_b;",
			),
			expErr: "operator '<=' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a >= b from binoptestb_b;",
			),
			expErr: "operator '>=' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a < b from binoptestb_b;",
			),
			expErr: "operator '<' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a > b from binoptestb_b;",
			),
			expErr: "operator '>' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a & b from binoptestb_b;",
			),
			expErr: "operator '&' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a | b from binoptestb_b;",
			),
			expErr: "operator '|' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a << b from binoptestb_b;",
			),
			expErr: "operator '<<' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a >> b from binoptestb_b;",
			),
			expErr: "operator '>>' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a + b from binoptestb_b;",
			),
			expErr: "operator '+' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a - b from binoptestb_b;",
			),
			expErr: "operator '-' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a * b from binoptestb_b;",
			),
			expErr: "operator '*' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a / b from binoptestb_b;",
			),
			expErr: "operator '/' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a % b from binoptestb_b;",
			),
			expErr: "operator '%' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a || b from binoptestb_b;",
			),
			expErr: "operator '||' incompatible with type 'BOOL'",
		},
	},
}

var binOpExprWithBoolID = tableTest{
	table: tbl(
		"binoptestb_id",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("b", fldTypeBool),
		),
		srcRows(
			srcRow(int64(10), bool(true)),
		),
	),
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select b != _id from binoptestb_id;",
			),
			expErr: "types 'BOOL' and 'ID' are not equatable",
		},
		{
			sqls: sqls(
				"select b = _id from binoptestb_id;",
			),
			expErr: "types 'BOOL' and 'ID' are not equatable",
		},
		{
			sqls: sqls(
				"select b <= _id from binoptestb_id;",
			),
			expErr: "operator '<=' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select b >= _id from binoptestb_id;",
			),
			expErr: "operator '>=' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select b < _id from binoptestb_id;",
			),
			expErr: "operator '<' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select b > _id from binoptestb_id;",
			),
			expErr: "operator '>' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select b & _id from binoptestb_id;",
			),
			expErr: "operator '&' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select b | _id from binoptestb_id;",
			),
			expErr: "operator '|' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select b << _id from binoptestb_id;",
			),
			expErr: "operator '<<' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select b >> _id from binoptestb_id;",
			),
			expErr: "operator '>>' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select b + _id from binoptestb_id;",
			),
			expErr: "operator '+' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select b - _id from binoptestb_id;",
			),
			expErr: "operator '-' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select b * _id from binoptestb_id;",
			),
			expErr: "operator '*' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select b / _id from binoptestb_id;",
			),
			expErr: "operator '/' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select b % _id from binoptestb_id;",
			),
			expErr: "operator '%' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select b || _id from binoptestb_id;",
			),
			expErr: "operator '||' incompatible with type 'BOOL'",
		},
	},
}

var binOpExprWithBoolDecimal = tableTest{
	table: tbl(
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
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select a != d from binoptestb_d;",
			),
			expErr: "types 'BOOL' and 'DECIMAL(2)' are not equatable",
		},
		{
			sqls: sqls(
				"select a = d from binoptestb_d;",
			),
			expErr: "types 'BOOL' and 'DECIMAL(2)' are not equatable",
		},
		{
			sqls: sqls(
				"select a <= d from binoptestb_d;",
			),
			expErr: "operator '<=' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a >= d from binoptestb_d;",
			),
			expErr: "operator '>=' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a < d from binoptestb_d;",
			),
			expErr: "operator '<' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a > d from binoptestb_d;",
			),
			expErr: "operator '>' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a & d from binoptestb_d;",
			),
			expErr: "operator '&' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a | d from binoptestb_d;",
			),
			expErr: "operator '|' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a << d from binoptestb_d;",
			),
			expErr: "operator '<<' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a >> d from binoptestb_d;",
			),
			expErr: "operator '>>' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a + d from binoptestb_d;",
			),
			expErr: "operator '+' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a - d from binoptestb_d;",
			),
			expErr: "operator '-' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a * d from binoptestb_d;",
			),
			expErr: "operator '*' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a / d from binoptestb_d;",
			),
			expErr: "operator '/' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a % d from binoptestb_d;",
			),
			expErr: "operator '%' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a || d from binoptestb_d;",
			),
			expErr: "operator '||' incompatible with type 'BOOL'",
		},
	},
}

var binOpExprWithBoolTimestamp = tableTest{
	table: tbl(
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
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select a != ts from binoptestb_ts;",
			),
			expErr: "types 'BOOL' and 'TIMESTAMP' are not equatable",
		},
		{
			sqls: sqls(
				"select a = ts from binoptestb_ts;",
			),
			expErr: "types 'BOOL' and 'TIMESTAMP' are not equatable",
		},
		{
			sqls: sqls(
				"select a <= ts from binoptestb_ts;",
			),
			expErr: "operator '<=' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a >= ts from binoptestb_ts;",
			),
			expErr: "operator '>=' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a < ts from binoptestb_ts;",
			),
			expErr: "operator '<' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a > ts from binoptestb_ts;",
			),
			expErr: "operator '>' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a & ts from binoptestb_ts;",
			),
			expErr: "operator '&' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a | ts from binoptestb_ts;",
			),
			expErr: "operator '|' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a << ts from binoptestb_ts;",
			),
			expErr: "operator '<<' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a >> ts from binoptestb_ts;",
			),
			expErr: "operator '>>' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a + ts from binoptestb_ts;",
			),
			expErr: "operator '+' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a - ts from binoptestb_ts;",
			),
			expErr: "operator '-' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a * ts from binoptestb_ts;",
			),
			expErr: "operator '*' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a / ts from binoptestb_ts;",
			),
			expErr: "operator '/' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a % ts from binoptestb_ts;",
			),
			expErr: "operator '%' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a || ts from binoptestb_ts;",
			),
			expErr: "operator '||' incompatible with type 'BOOL'",
		},
	},
}

var binOpExprWithBoolIDSet = tableTest{
	table: tbl(
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
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select a != b from binoptestb_ids;",
			),
			expErr: "types 'BOOL' and 'IDSET' are not equatable",
		},
		{
			sqls: sqls(
				"select a = b from binoptestb_ids;",
			),
			expErr: "types 'BOOL' and 'IDSET' are not equatable",
		},
		{
			sqls: sqls(
				"select a <= b from binoptestb_ids;",
			),
			expErr: " operator '<=' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a >= b from binoptestb_ids;",
			),
			expErr: " operator '>=' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a < b from binoptestb_ids;",
			),
			expErr: " operator '<' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a > b from binoptestb_ids;",
			),
			expErr: " operator '>' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a & b from binoptestb_ids;",
			),
			expErr: " operator '&' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a | b from binoptestb_ids;",
			),
			expErr: " operator '|' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a << b from binoptestb_ids;",
			),
			expErr: " operator '<<' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a >> b from binoptestb_ids;",
			),
			expErr: " operator '>>' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a + b from binoptestb_ids;",
			),
			expErr: " operator '+' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a - b from binoptestb_ids;",
			),
			expErr: " operator '-' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a * b from binoptestb_ids;",
			),
			expErr: " operator '*' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a / b from binoptestb_ids;",
			),
			expErr: " operator '/' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a % b from binoptestb_ids;",
			),
			expErr: " operator '%' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a || b from binoptestb_ids;",
			),
			expErr: "operator '||' incompatible with type 'BOOL'",
		},
	},
}

var binOpExprWithBoolString = tableTest{
	table: tbl(
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
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select a != b from binoptestb_s;",
			),
			expErr: "types 'BOOL' and 'STRING' are not equatable",
		},
		{
			sqls: sqls(
				"select a = b from binoptestb_s;",
			),
			expErr: "types 'BOOL' and 'STRING' are not equatable",
		},
		{
			sqls: sqls(
				"select a <= b from binoptestb_s;",
			),
			expErr: " operator '<=' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a >= b from binoptestb_s;",
			),
			expErr: " operator '>=' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a < b from binoptestb_s;",
			),
			expErr: " operator '<' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a > b from binoptestb_s;",
			),
			expErr: " operator '>' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a & b from binoptestb_s;",
			),
			expErr: " operator '&' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a | b from binoptestb_s;",
			),
			expErr: " operator '|' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a << b from binoptestb_s;",
			),
			expErr: " operator '<<' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a >> b from binoptestb_s;",
			),
			expErr: " operator '>>' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a + b from binoptestb_s;",
			),
			expErr: " operator '+' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a - b from binoptestb_s;",
			),
			expErr: " operator '-' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a * b from binoptestb_s;",
			),
			expErr: " operator '*' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a / b from binoptestb_s;",
			),
			expErr: " operator '/' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a % b from binoptestb_s;",
			),
			expErr: " operator '%' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a || b from binoptestb_s;",
			),
			expErr: "operator '||' incompatible with type 'BOOL'",
		},
	},
}

var binOpExprWithBoolStringSet = tableTest{
	table: tbl(
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
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select a != b from binoptestb_ss;",
			),
			expErr: "types 'BOOL' and 'STRINGSET' are not equatable",
		},
		{
			sqls: sqls(
				"select a = b from binoptestb_ss;",
			),
			expErr: "types 'BOOL' and 'STRINGSET' are not equatable",
		},
		{
			sqls: sqls(
				"select a <= b from binoptestb_ss;",
			),
			expErr: " operator '<=' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a >= b from binoptestb_ss;",
			),
			expErr: " operator '>=' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a < b from binoptestb_ss;",
			),
			expErr: " operator '<' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a > b from binoptestb_ss;",
			),
			expErr: " operator '>' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a & b from binoptestb_ss;",
			),
			expErr: " operator '&' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a | b from binoptestb_ss;",
			),
			expErr: " operator '|' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a << b from binoptestb_ss;",
			),
			expErr: " operator '<<' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a >> b from binoptestb_ss;",
			),
			expErr: " operator '>>' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a + b from binoptestb_ss;",
			),
			expErr: " operator '+' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a - b from binoptestb_ss;",
			),
			expErr: " operator '-' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a * b from binoptestb_ss;",
			),
			expErr: " operator '*' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a / b from binoptestb_ss;",
			),
			expErr: " operator '/' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a % b from binoptestb_ss;",
			),
			expErr: " operator '%' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select a || b from binoptestb_ss;",
			),
			expErr: "operator '||' incompatible with type 'BOOL'",
		},
	},
}

//ID bin op tests
var binOpExprWithIDInt = tableTest{
	table: tbl(
		"binoptestid_i",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("b", fldTypeInt, "min 0", "max 1000"),
		),
		srcRows(
			srcRow(int64(10), int64(20)),
		),
	),
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select _id != b from binoptestid_i;",
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
				"select _id = b from binoptestid_i;",
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
				"select _id <= b from binoptestid_i;",
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
				"select _id >= b from binoptestid_i;",
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
				"select _id < b from binoptestid_i;",
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
				"select _id > b from binoptestid_i;",
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
				"select _id & b from binoptestid_i;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			expRows: rows(
				row(int64(0)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select _id | b from binoptestid_i;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			expRows: rows(
				row(int64(30)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select _id << b from binoptestid_i;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			expRows: rows(
				row(int64(10485760)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select _id >> b from binoptestid_i;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			expRows: rows(
				row(int64(0)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select _id + b from binoptestid_i;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			expRows: rows(
				row(int64(30)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select _id - b from binoptestid_i;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			expRows: rows(
				row(int64(-10)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select _id * b from binoptestid_i;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			expRows: rows(
				row(int64(200)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select _id / b from binoptestid_i;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			expRows: rows(
				row(int64(0)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select _id % b from binoptestid_i;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			expRows: rows(
				row(int64(10)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select _id || b from binoptestid_i;",
			),
			expErr: "operator '||' incompatible with type 'ID'",
		},
	},
}

var binOpExprWithIDBool = tableTest{
	table: tbl(
		"binoptestid_b",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("b", fldTypeBool),
		),
		srcRows(
			srcRow(int64(10), bool(true)),
		),
	),
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select _id != b from binoptestid_b;",
			),
			expErr: "types 'ID' and 'BOOL' are not equatable",
		},
		{
			sqls: sqls(
				"select _id = b from binoptestid_b;",
			),
			expErr: "types 'ID' and 'BOOL' are not equatable",
		},
		{
			sqls: sqls(
				"select _id <= b from binoptestid_b;",
			),
			expErr: "operator '<=' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select _id >= b from binoptestid_b;",
			),
			expErr: "operator '>=' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select _id < b from binoptestid_b;",
			),
			expErr: "operator '<' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select _id > b from binoptestid_b;",
			),
			expErr: "operator '>' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select _id & b from binoptestid_b;",
			),
			expErr: "operator '&' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select _id | b from binoptestid_b;",
			),
			expErr: "operator '|' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select _id << b from binoptestid_b;",
			),
			expErr: "operator '<<' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select _id >> b from binoptestid_b;",
			),
			expErr: "operator '>>' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select _id + b from binoptestid_b;",
			),
			expErr: "operator '+' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select _id - b from binoptestid_b;",
			),
			expErr: "operator '-' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select _id * b from binoptestid_b;",
			),
			expErr: "operator '*' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select _id / b from binoptestid_b;",
			),
			expErr: "operator '/' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select _id % b from binoptestid_b;",
			),
			expErr: "operator '%' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select _id || b from binoptestid_b;",
			),
			expErr: "operator '||' incompatible with type 'ID'",
		},
	},
}

var binOpExprWithIDID = tableTest{
	table: tbl(
		"binoptestid_id",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("b", fldTypeID),
		),
		srcRows(
			srcRow(int64(10), int64(20)),
		),
	),
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select _id != b from binoptestid_id;",
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
				"select _id = b from binoptestid_id;",
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
				"select _id <= b from binoptestid_id;",
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
				"select _id >= b from binoptestid_id;",
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
				"select _id < b from binoptestid_id;",
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
				"select _id > b from binoptestid_id;",
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
				"select _id & b from binoptestid_id;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeID),
			),
			expRows: rows(
				row(int64(0)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select _id | b from binoptestid_id;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeID),
			),
			expRows: rows(
				row(int64(30)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select _id << b from binoptestid_id;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeID),
			),
			expRows: rows(
				row(int64(10485760)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select _id >> b from binoptestid_id;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeID),
			),
			expRows: rows(
				row(int64(0)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select _id + b from binoptestid_id;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeID),
			),
			expRows: rows(
				row(int64(30)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select _id - b from binoptestid_id;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeID),
			),
			expRows: rows(
				row(int64(-10)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select _id * b from binoptestid_id;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeID),
			),
			expRows: rows(
				row(int64(200)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select _id / b from binoptestid_id;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeID),
			),
			expRows: rows(
				row(int64(0)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select _id % b from binoptestid_id;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeID),
			),
			expRows: rows(
				row(int64(10)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select _id || b from binoptestid_id;",
			),
			expErr: "operator '||' incompatible with type 'ID'",
		},
	},
}

var binOpExprWithIDDecimal = tableTest{
	table: tbl(
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
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select a != d from binoptesti_d;",
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
				"select a = d from binoptesti_d;",
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
				"select a <= d from binoptesti_d;",
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
				"select a >= d from binoptesti_d;",
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
				"select a < d from binoptesti_d;",
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
				"select a > d from binoptesti_d;",
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
				"select a & d from binoptesti_d;",
			),
			expErr: "operator '&' incompatible with type 'DECIMAL(2)'",
		},
		{
			sqls: sqls(
				"select a | d from binoptesti_d;",
			),
			expErr: "operator '|' incompatible with type 'DECIMAL(2)'",
		},
		{
			sqls: sqls(
				"select a << d from binoptesti_d;",
			),
			expErr: "operator '<<' incompatible with type 'DECIMAL(2)'",
		},
		{
			sqls: sqls(
				"select a >> d from binoptesti_d;",
			),
			expErr: "operator '>>' incompatible with type 'DECIMAL(2)'",
		},
		{
			sqls: sqls(
				"select a + d from binoptesti_d;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeDecimal2),
			),
			expRows: rows(
				row(float64(32.34)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select a - d from binoptesti_d;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeDecimal2),
			),
			expRows: rows(
				row(float64(7.66)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select a * d from binoptesti_d;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeDecimal2),
			),
			expRows: rows(
				row(float64(246.8)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select a / d from binoptesti_d;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeDecimal2),
			),
			expRows: rows(
				//TODO(pok) this float64 thing is for the birds
				row(float64(1.6207455429497568)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select a % d from binoptesti_d;",
			),
			expErr: "operator '%' incompatible with type 'DECIMAL(2)'",
		},
		{
			sqls: sqls(
				"select a || d from binoptesti_d;",
			),
			expErr: "operator '||' incompatible with type 'INT'",
		},
	},
}

var binOpExprWithIDTimestamp = tableTest{
	table: tbl(
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
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select a != ts from binoptesti_ts;",
			),
			expErr: "types 'INT' and 'TIMESTAMP' are not equatable",
		},
		{
			sqls: sqls(
				"select a = ts from binoptesti_ts;",
			),
			expErr: "types 'INT' and 'TIMESTAMP' are not equatable",
		},
		{
			sqls: sqls(
				"select a <= ts from binoptesti_ts;",
			),
			expErr: "types 'INT' and 'TIMESTAMP' are not equatable",
		},
		{
			sqls: sqls(
				"select a >= ts from binoptesti_ts;",
			),
			expErr: "types 'INT' and 'TIMESTAMP' are not equatable",
		},
		{
			sqls: sqls(
				"select a < ts from binoptesti_ts;",
			),
			expErr: "types 'INT' and 'TIMESTAMP' are not equatable",
		},
		{
			sqls: sqls(
				"select a > ts from binoptesti_ts;",
			),
			expErr: "types 'INT' and 'TIMESTAMP' are not equatable",
		},
		{
			sqls: sqls(
				"select a & ts from binoptesti_ts;",
			),
			expErr: "operator '&' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a | ts from binoptesti_ts;",
			),
			expErr: "operator '|' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a << ts from binoptesti_ts;",
			),
			expErr: "operator '<<' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a >> ts from binoptesti_ts;",
			),
			expErr: "operator '>>' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a + ts from binoptesti_ts;",
			),
			expErr: "operator '+' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a - ts from binoptesti_ts;",
			),
			expErr: "operator '-' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a * ts from binoptesti_ts;",
			),
			expErr: "operator '*' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a / ts from binoptesti_ts;",
			),
			expErr: "operator '/' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a % ts from binoptesti_ts;",
			),
			expErr: "operator '%' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a || ts from binoptesti_ts;",
			),
			expErr: "operator '||' incompatible with type 'INT'",
		},
	},
}

var binOpExprWithIDIDSet = tableTest{
	table: tbl(
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
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select a != b from binoptesti_ids;",
			),
			expErr: "types 'INT' and 'IDSET' are not equatable",
		},
		{
			sqls: sqls(
				"select a = b from binoptesti_ids;",
			),
			expErr: "types 'INT' and 'IDSET' are not equatable",
		},
		{
			sqls: sqls(
				"select a <= b from binoptesti_ids;",
			),
			expErr: " operator '<=' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a >= b from binoptesti_ids;",
			),
			expErr: " operator '>=' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a < b from binoptesti_ids;",
			),
			expErr: " operator '<' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a > b from binoptesti_ids;",
			),
			expErr: " operator '>' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a & b from binoptesti_ids;",
			),
			expErr: " operator '&' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a | b from binoptesti_ids;",
			),
			expErr: " operator '|' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a << b from binoptesti_ids;",
			),
			expErr: " operator '<<' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a >> b from binoptesti_ids;",
			),
			expErr: " operator '>>' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a + b from binoptesti_ids;",
			),
			expErr: " operator '+' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a - b from binoptesti_ids;",
			),
			expErr: " operator '-' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a * b from binoptesti_ids;",
			),
			expErr: " operator '*' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a / b from binoptesti_ids;",
			),
			expErr: " operator '/' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a % b from binoptesti_ids;",
			),
			expErr: " operator '%' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a || b from binoptesti_ids;",
			),
			expErr: "operator '||' incompatible with type 'INT'",
		},
	},
}

var binOpExprWithIDString = tableTest{
	table: tbl(
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
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select a != b from binoptesti_s;",
			),
			expErr: "types 'INT' and 'STRING' are not equatable",
		},
		{
			sqls: sqls(
				"select a = b from binoptesti_s;",
			),
			expErr: "types 'INT' and 'STRING' are not equatable",
		},
		{
			sqls: sqls(
				"select a <= b from binoptesti_s;",
			),
			expErr: " operator '<=' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a >= b from binoptesti_s;",
			),
			expErr: " operator '>=' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a < b from binoptesti_s;",
			),
			expErr: " operator '<' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a > b from binoptesti_s;",
			),
			expErr: " operator '>' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a & b from binoptesti_s;",
			),
			expErr: " operator '&' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a | b from binoptesti_s;",
			),
			expErr: " operator '|' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a << b from binoptesti_s;",
			),
			expErr: " operator '<<' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a >> b from binoptesti_s;",
			),
			expErr: " operator '>>' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a + b from binoptesti_s;",
			),
			expErr: " operator '+' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a - b from binoptesti_s;",
			),
			expErr: " operator '-' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a * b from binoptesti_s;",
			),
			expErr: " operator '*' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a / b from binoptesti_s;",
			),
			expErr: " operator '/' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a % b from binoptesti_s;",
			),
			expErr: " operator '%' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a || b from binoptesti_s;",
			),
			expErr: "operator '||' incompatible with type 'INT'",
		},
	},
}

var binOpExprWithIDStringSet = tableTest{
	table: tbl(
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
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select a != b from binoptesti_ss;",
			),
			expErr: "types 'INT' and 'STRINGSET' are not equatable",
		},
		{
			sqls: sqls(
				"select a = b from binoptesti_ss;",
			),
			expErr: "types 'INT' and 'STRINGSET' are not equatable",
		},
		{
			sqls: sqls(
				"select a <= b from binoptesti_ss;",
			),
			expErr: " operator '<=' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a >= b from binoptesti_ss;",
			),
			expErr: " operator '>=' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a < b from binoptesti_ss;",
			),
			expErr: " operator '<' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a > b from binoptesti_ss;",
			),
			expErr: " operator '>' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a & b from binoptesti_ss;",
			),
			expErr: " operator '&' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a | b from binoptesti_ss;",
			),
			expErr: " operator '|' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a << b from binoptesti_ss;",
			),
			expErr: " operator '<<' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a >> b from binoptesti_ss;",
			),
			expErr: " operator '>>' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a + b from binoptesti_ss;",
			),
			expErr: " operator '+' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a - b from binoptesti_ss;",
			),
			expErr: " operator '-' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a * b from binoptesti_ss;",
			),
			expErr: " operator '*' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a / b from binoptesti_ss;",
			),
			expErr: " operator '/' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a % b from binoptesti_ss;",
			),
			expErr: " operator '%' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a || b from binoptesti_ss;",
			),
			expErr: "operator '||' incompatible with type 'INT'",
		},
	},
}

//DECIMAL bin op tests
var binOpExprWithDecInt = tableTest{
	table: tbl(
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
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select d != b from binoptestdec_i;",
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
				"select d = b from binoptestdec_i;",
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
				"select d <= b from binoptestdec_i;",
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
				"select d >= b from binoptestdec_i;",
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
				"select d < b from binoptestdec_i;",
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
				"select d > b from binoptestdec_i;",
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
				"select d & b from binoptestdec_i;",
			),
			expErr: "operator '&' incompatible with type 'DECIMAL(2)'",
		},
		{
			sqls: sqls(
				"select d | b from binoptestdec_i;",
			),
			expErr: "operator '|' incompatible with type 'DECIMAL(2)'",
		},
		{
			sqls: sqls(
				"select d << b from binoptestdec_i;",
			),
			expErr: "operator '<<' incompatible with type 'DECIMAL(2)'",
		},
		{
			sqls: sqls(
				"select d >> b from binoptestdec_i;",
			),
			expErr: "operator '>>' incompatible with type 'DECIMAL(2)'",
		},
		{
			sqls: sqls(
				"select d + b from binoptestdec_i;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeDecimal2),
			),
			expRows: rows(
				row(float64(32.34)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select d - b from binoptestdec_i;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeDecimal2),
			),
			expRows: rows(
				row(float64(-7.66)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select d * b from binoptestdec_i;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeDecimal2),
			),
			expRows: rows(
				row(float64(246.8)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select d / b from binoptestdec_i;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeDecimal2),
			),
			expRows: rows(
				row(float64(0.617)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select d % b from binoptestdec_i;",
			),
			expErr: "operator '%' incompatible with type 'DECIMAL(2)'",
		},
		{
			sqls: sqls(
				"select d || b from binoptestdec_i;",
			),
			expErr: "operator '||' incompatible with type 'DECIMAL(2)'",
		},
	},
}

var binOpExprWithDecBool = tableTest{
	table: tbl(
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
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select d != b from binoptestdec_b;",
			),
			expErr: "types 'DECIMAL(2)' and 'BOOL' are not equatable",
		},
		{
			sqls: sqls(
				"select d = b from binoptestdec_b;",
			),
			expErr: "types 'DECIMAL(2)' and 'BOOL' are not equatable",
		},
		{
			sqls: sqls(
				"select d <= b from binoptestdec_b;",
			),
			expErr: "operator '<=' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select d >= b from binoptestdec_b;",
			),
			expErr: "operator '>=' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select d < b from binoptestdec_b;",
			),
			expErr: "operator '<' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select d > b from binoptestdec_b;",
			),
			expErr: "operator '>' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select d & b from binoptestdec_b;",
			),
			expErr: "operator '&' incompatible with type 'DECIMAL(2)'",
		},
		{
			sqls: sqls(
				"select d | b from binoptestdec_b;",
			),
			expErr: "operator '|' incompatible with type 'DECIMAL(2)'",
		},
		{
			sqls: sqls(
				"select d << b from binoptestdec_b;",
			),
			expErr: "operator '<<' incompatible with type 'DECIMAL(2)'",
		},
		{
			sqls: sqls(
				"select d >> b from binoptestdec_b;",
			),
			expErr: "operator '>>' incompatible with type 'DECIMAL(2)'",
		},
		{
			sqls: sqls(
				"select d + b from binoptestdec_b;",
			),
			expErr: "operator '+' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select d - b from binoptestdec_b;",
			),
			expErr: "operator '-' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select d * b from binoptestdec_b;",
			),
			expErr: "operator '*' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select d / b from binoptestdec_b;",
			),
			expErr: "operator '/' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select d % b from binoptestdec_b;",
			),
			expErr: "operator '%' incompatible with type 'DECIMAL(2)'",
		},
		{
			sqls: sqls(
				"select d || b from binoptestdec_b;",
			),
			expErr: "operator '||' incompatible with type 'DECIMAL(2)'",
		},
	},
}

var binOpExprWithDecID = tableTest{
	table: tbl(
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
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select d != b from binoptestdec_id;",
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
				"select d = b from binoptestdec_id;",
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
				"select d <= b from binoptestdec_id;",
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
				"select d >= b from binoptestdec_id;",
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
				"select d < b from binoptestdec_id;",
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
				"select d > b from binoptestdec_id;",
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
				"select d & b from binoptestdec_id;",
			),
			expErr: "operator '&' incompatible with type 'DECIMAL(2)'",
		},
		{
			sqls: sqls(
				"select d | b from binoptestdec_id;",
			),
			expErr: "operator '|' incompatible with type 'DECIMAL(2)'",
		},
		{
			sqls: sqls(
				"select d << b from binoptestdec_id;",
			),
			expErr: "operator '<<' incompatible with type 'DECIMAL(2)'",
		},
		{
			sqls: sqls(
				"select d >> b from binoptestdec_id;",
			),
			expErr: "operator '>>' incompatible with type 'DECIMAL(2)'",
		},
		{
			sqls: sqls(
				"select d + b from binoptestdec_id;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeDecimal2),
			),
			expRows: rows(
				row(float64(32.34)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select d - b from binoptestdec_id;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeDecimal2),
			),
			expRows: rows(
				row(float64(-7.66)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select d * b from binoptestdec_id;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeDecimal2),
			),
			expRows: rows(
				row(float64(246.8)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select d / b from binoptestdec_id;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeDecimal2),
			),
			expRows: rows(
				row(float64(0.617)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select d % b from binoptestdec_id;",
			),
			expErr: "operator '%' incompatible with type 'DECIMAL(2)'",
		},
		{
			sqls: sqls(
				"select d || b from binoptestdec_id;",
			),
			expErr: "operator '||' incompatible with type 'DECIMAL(2)'",
		},
	},
}

var binOpExprWithDecDecimal = tableTest{
	table: tbl(
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
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select a != d from binoptestdec_d;",
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
				"select a = d from binoptestdec_d;",
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
				"select a <= d from binoptestdec_d;",
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
				"select a >= d from binoptestdec_d;",
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
				"select a < d from binoptestdec_d;",
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
				"select a > d from binoptestdec_d;",
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
				"select a & d from binoptestdec_d;",
			),
			expErr: "operator '&' incompatible with type 'DECIMAL(2)'",
		},
		{
			sqls: sqls(
				"select a | d from binoptestdec_d;",
			),
			expErr: "operator '|' incompatible with type 'DECIMAL(2)'",
		},
		{
			sqls: sqls(
				"select a << d from binoptestdec_d;",
			),
			expErr: "operator '<<' incompatible with type 'DECIMAL(2)'",
		},
		{
			sqls: sqls(
				"select a >> d from binoptestdec_d;",
			),
			expErr: "operator '>>' incompatible with type 'DECIMAL(2)'",
		},
		{
			sqls: sqls(
				"select a + d from binoptestdec_d;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeDecimal2),
			),
			expRows: rows(
				row(float64(32.34)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select a - d from binoptestdec_d;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeDecimal2),
			),
			expRows: rows(
				row(float64(7.66)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select a * d from binoptestdec_d;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeDecimal2),
			),
			expRows: rows(
				row(float64(246.8)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select a / d from binoptestdec_d;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeDecimal2),
			),
			expRows: rows(
				//TODO(pok) this float64 thing is for the birds
				row(float64(1.6207455429497568)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select a % d from binoptestdec_d;",
			),
			expErr: "operator '%' incompatible with type 'DECIMAL(2)'",
		},
		{
			sqls: sqls(
				"select a || d from binoptestdec_d;",
			),
			expErr: "operator '||' incompatible with type 'DECIMAL(2)'",
		},
	},
}

var binOpExprWithDecTimestamp = tableTest{
	table: tbl(
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
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select a != ts from binoptestdec_ts;",
			),
			expErr: "types 'DECIMAL(2)' and 'TIMESTAMP' are not equatable",
		},
		{
			sqls: sqls(
				"select a = ts from binoptestdec_ts;",
			),
			expErr: "types 'DECIMAL(2)' and 'TIMESTAMP' are not equatable",
		},
		{
			sqls: sqls(
				"select a <= ts from binoptestdec_ts;",
			),
			expErr: "types 'DECIMAL(2)' and 'TIMESTAMP' are not equatable",
		},
		{
			sqls: sqls(
				"select a >= ts from binoptestdec_ts;",
			),
			expErr: "types 'DECIMAL(2)' and 'TIMESTAMP' are not equatable",
		},
		{
			sqls: sqls(
				"select a < ts from binoptestdec_ts;",
			),
			expErr: "types 'DECIMAL(2)' and 'TIMESTAMP' are not equatable",
		},
		{
			sqls: sqls(
				"select a > ts from binoptestdec_ts;",
			),
			expErr: "types 'DECIMAL(2)' and 'TIMESTAMP' are not equatable",
		},
		{
			sqls: sqls(
				"select a & ts from binoptestdec_ts;",
			),
			expErr: "operator '&' incompatible with type 'DECIMAL(2)'",
		},
		{
			sqls: sqls(
				"select a | ts from binoptestdec_ts;",
			),
			expErr: "operator '|' incompatible with type 'DECIMAL(2)'",
		},
		{
			sqls: sqls(
				"select a << ts from binoptestdec_ts;",
			),
			expErr: "operator '<<' incompatible with type 'DECIMAL(2)'",
		},
		{
			sqls: sqls(
				"select a >> ts from binoptestdec_ts;",
			),
			expErr: "operator '>>' incompatible with type 'DECIMAL(2)'",
		},
		{
			sqls: sqls(
				"select a + ts from binoptestdec_ts;",
			),
			expErr: "operator '+' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a - ts from binoptestdec_ts;",
			),
			expErr: "operator '-' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a * ts from binoptestdec_ts;",
			),
			expErr: "operator '*' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a / ts from binoptestdec_ts;",
			),
			expErr: "operator '/' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a % ts from binoptestdec_ts;",
			),
			expErr: "operator '%' incompatible with type 'DECIMAL(2)'",
		},
		{
			sqls: sqls(
				"select a || ts from binoptestdec_ts;",
			),
			expErr: "operator '||' incompatible with type 'DECIMAL(2)'",
		},
	},
}

var binOpExprWithDecIDSet = tableTest{
	table: tbl(
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
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select a != b from binoptestdec_ids;",
			),
			expErr: "types 'DECIMAL(2)' and 'IDSET' are not equatable",
		},
		{
			sqls: sqls(
				"select a = b from binoptestdec_ids;",
			),
			expErr: "types 'DECIMAL(2)' and 'IDSET' are not equatable",
		},
		{
			sqls: sqls(
				"select a <= b from binoptestdec_ids;",
			),
			expErr: " operator '<=' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a >= b from binoptestdec_ids;",
			),
			expErr: " operator '>=' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a < b from binoptestdec_ids;",
			),
			expErr: " operator '<' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a > b from binoptestdec_ids;",
			),
			expErr: " operator '>' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a & b from binoptestdec_ids;",
			),
			expErr: " operator '&' incompatible with type 'DECIMAL(2)'",
		},
		{
			sqls: sqls(
				"select a | b from binoptestdec_ids;",
			),
			expErr: " operator '|' incompatible with type 'DECIMAL(2)'",
		},
		{
			sqls: sqls(
				"select a << b from binoptestdec_ids;",
			),
			expErr: " operator '<<' incompatible with type 'DECIMAL(2)'",
		},
		{
			sqls: sqls(
				"select a >> b from binoptestdec_ids;",
			),
			expErr: " operator '>>' incompatible with type 'DECIMAL(2)'",
		},
		{
			sqls: sqls(
				"select a + b from binoptestdec_ids;",
			),
			expErr: " operator '+' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a - b from binoptestdec_ids;",
			),
			expErr: " operator '-' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a * b from binoptestdec_ids;",
			),
			expErr: " operator '*' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a / b from binoptestdec_ids;",
			),
			expErr: " operator '/' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a % b from binoptestdec_ids;",
			),
			expErr: " operator '%' incompatible with type 'DECIMAL(2)'",
		},
		{
			sqls: sqls(
				"select a || b from binoptestdec_ids;",
			),
			expErr: "operator '||' incompatible with type 'DECIMAL(2)'",
		},
	},
}

var binOpExprWithDecString = tableTest{
	table: tbl(
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
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select a != b from binoptestdec_s;",
			),
			expErr: "types 'DECIMAL(2)' and 'STRING' are not equatable",
		},
		{
			sqls: sqls(
				"select a = b from binoptestdec_s;",
			),
			expErr: "types 'DECIMAL(2)' and 'STRING' are not equatable",
		},
		{
			sqls: sqls(
				"select a <= b from binoptestdec_s;",
			),
			expErr: " operator '<=' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a >= b from binoptestdec_s;",
			),
			expErr: " operator '>=' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a < b from binoptestdec_s;",
			),
			expErr: " operator '<' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a > b from binoptestdec_s;",
			),
			expErr: " operator '>' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a & b from binoptestdec_s;",
			),
			expErr: " operator '&' incompatible with type 'DECIMAL(2)'",
		},
		{
			sqls: sqls(
				"select a | b from binoptestdec_s;",
			),
			expErr: " operator '|' incompatible with type 'DECIMAL(2)'",
		},
		{
			sqls: sqls(
				"select a << b from binoptestdec_s;",
			),
			expErr: " operator '<<' incompatible with type 'DECIMAL(2)'",
		},
		{
			sqls: sqls(
				"select a >> b from binoptestdec_s;",
			),
			expErr: " operator '>>' incompatible with type 'DECIMAL(2)'",
		},
		{
			sqls: sqls(
				"select a + b from binoptestdec_s;",
			),
			expErr: " operator '+' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a - b from binoptestdec_s;",
			),
			expErr: " operator '-' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a * b from binoptestdec_s;",
			),
			expErr: " operator '*' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a / b from binoptestdec_s;",
			),
			expErr: " operator '/' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a % b from binoptestdec_s;",
			),
			expErr: " operator '%' incompatible with type 'DECIMAL(2)'",
		},
		{
			sqls: sqls(
				"select a || b from binoptestdec_s;",
			),
			expErr: "operator '||' incompatible with type 'DECIMAL(2)'",
		},
	},
}

var binOpExprWithDecStringSet = tableTest{
	table: tbl(
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
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select a != b from binoptestdec_ss;",
			),
			expErr: "types 'DECIMAL(2)' and 'STRINGSET' are not equatable",
		},
		{
			sqls: sqls(
				"select a = b from binoptestdec_ss;",
			),
			expErr: "types 'DECIMAL(2)' and 'STRINGSET' are not equatable",
		},
		{
			sqls: sqls(
				"select a <= b from binoptestdec_ss;",
			),
			expErr: " operator '<=' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a >= b from binoptestdec_ss;",
			),
			expErr: " operator '>=' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a < b from binoptestdec_ss;",
			),
			expErr: " operator '<' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a > b from binoptestdec_ss;",
			),
			expErr: " operator '>' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a & b from binoptestdec_ss;",
			),
			expErr: " operator '&' incompatible with type 'DECIMAL(2)'",
		},
		{
			sqls: sqls(
				"select a | b from binoptestdec_ss;",
			),
			expErr: " operator '|' incompatible with type 'DECIMAL(2)'",
		},
		{
			sqls: sqls(
				"select a << b from binoptestdec_ss;",
			),
			expErr: " operator '<<' incompatible with type 'DECIMAL(2)'",
		},
		{
			sqls: sqls(
				"select a >> b from binoptestdec_ss;",
			),
			expErr: " operator '>>' incompatible with type 'DECIMAL(2)'",
		},
		{
			sqls: sqls(
				"select a + b from binoptestdec_ss;",
			),
			expErr: " operator '+' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a - b from binoptestdec_ss;",
			),
			expErr: " operator '-' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a * b from binoptestdec_ss;",
			),
			expErr: " operator '*' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a / b from binoptestdec_ss;",
			),
			expErr: " operator '/' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a % b from binoptestdec_ss;",
			),
			expErr: " operator '%' incompatible with type 'DECIMAL(2)'",
		},
		{
			sqls: sqls(
				"select a || b from binoptestdec_ss;",
			),
			expErr: "operator '||' incompatible with type 'DECIMAL(2)'",
		},
	},
}

//TIMESTAMP bin op tests
var binOpExprWithTSInt = tableTest{
	table: tbl(
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
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select d != b from binoptestts_i;",
			),
			expErr: "types 'TIMESTAMP' and 'INT' are not equatable",
		},
		{
			sqls: sqls(
				"select d = b from binoptestts_i;",
			),
			expErr: "types 'TIMESTAMP' and 'INT' are not equatable",
		},
		{
			sqls: sqls(
				"select d <= b from binoptestts_i;",
			),
			expErr: "types 'TIMESTAMP' and 'INT' are not equatable",
		},
		{
			sqls: sqls(
				"select d >= b from binoptestts_i;",
			),
			expErr: "types 'TIMESTAMP' and 'INT' are not equatable",
		},
		{
			sqls: sqls(
				"select d < b from binoptestts_i;",
			),
			expErr: "types 'TIMESTAMP' and 'INT' are not equatable",
		},
		{
			sqls: sqls(
				"select d > b from binoptestts_i;",
			),
			expErr: "types 'TIMESTAMP' and 'INT' are not equatable",
		},
		{
			sqls: sqls(
				"select d & b from binoptestts_i;",
			),
			expErr: "operator '&' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select d | b from binoptestts_i;",
			),
			expErr: "operator '|' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select d << b from binoptestts_i;",
			),
			expErr: "operator '<<' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select d >> b from binoptestts_i;",
			),
			expErr: "operator '>>' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select d + b from binoptestts_i;",
			),
			expErr: "operator '+' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select d - b from binoptestts_i;",
			),
			expErr: "operator '-' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select d * b from binoptestts_i;",
			),
			expErr: "operator '*' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select d / b from binoptestts_i;",
			),
			expErr: "operator '/' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select d % b from binoptestts_i;",
			),
			expErr: "operator '%' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select d || b from binoptestts_i;",
			),
			expErr: "operator '||' incompatible with type 'TIMESTAMP'",
		},
	},
}

var binOpExprWithTSBool = tableTest{
	table: tbl(
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
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select d != b from binoptestts_b;",
			),
			expErr: "types 'TIMESTAMP' and 'BOOL' are not equatable",
		},
		{
			sqls: sqls(
				"select d = b from binoptestts_b;",
			),
			expErr: "types 'TIMESTAMP' and 'BOOL' are not equatable",
		},
		{
			sqls: sqls(
				"select d <= b from binoptestts_b;",
			),
			expErr: "operator '<=' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select d >= b from binoptestts_b;",
			),
			expErr: "operator '>=' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select d < b from binoptestts_b;",
			),
			expErr: "operator '<' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select d > b from binoptestts_b;",
			),
			expErr: "operator '>' incompatible with type 'BOOL'",
		},
		{
			sqls: sqls(
				"select d & b from binoptestts_b;",
			),
			expErr: "operator '&' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select d | b from binoptestts_b;",
			),
			expErr: "operator '|' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select d << b from binoptestts_b;",
			),
			expErr: "operator '<<' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select d >> b from binoptestts_b;",
			),
			expErr: "operator '>>' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select d + b from binoptestts_b;",
			),
			expErr: "operator '+' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select d - b from binoptestts_b;",
			),
			expErr: "operator '-' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select d * b from binoptestts_b;",
			),
			expErr: "operator '*' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select d / b from binoptestts_b;",
			),
			expErr: "operator '/' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select d % b from binoptestts_b;",
			),
			expErr: "operator '%' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select d || b from binoptestts_b;",
			),
			expErr: "operator '||' incompatible with type 'TIMESTAMP'",
		},
	},
}

var binOpExprWithTSID = tableTest{
	table: tbl(
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
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select d != b from binoptestts_id;",
			),
			expErr: "types 'TIMESTAMP' and 'ID' are not equatable",
		},
		{
			sqls: sqls(
				"select d = b from binoptestts_id;",
			),
			expErr: "types 'TIMESTAMP' and 'ID' are not equatable",
		},
		{
			sqls: sqls(
				"select d <= b from binoptestts_id;",
			),
			expErr: "types 'TIMESTAMP' and 'ID' are not equatable",
		},
		{
			sqls: sqls(
				"select d >= b from binoptestts_id;",
			),
			expErr: "types 'TIMESTAMP' and 'ID' are not equatable",
		},
		{
			sqls: sqls(
				"select d < b from binoptestts_id;",
			),
			expErr: "types 'TIMESTAMP' and 'ID' are not equatable",
		},
		{
			sqls: sqls(
				"select d > b from binoptestts_id;",
			),
			expErr: "types 'TIMESTAMP' and 'ID' are not equatable",
		},
		{
			sqls: sqls(
				"select d & b from binoptestts_id;",
			),
			expErr: "operator '&' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select d | b from binoptestts_id;",
			),
			expErr: "operator '|' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select d << b from binoptestts_id;",
			),
			expErr: "operator '<<' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select d >> b from binoptestts_id;",
			),
			expErr: "operator '>>' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select d + b from binoptestts_id;",
			),
			expErr: "operator '+' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select d - b from binoptestts_id;",
			),
			expErr: "operator '-' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select d * b from binoptestts_id;",
			),
			expErr: "operator '*' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select d / b from binoptestts_id;",
			),
			expErr: "operator '/' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select d % b from binoptestts_id;",
			),
			expErr: "operator '%' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select d || b from binoptestts_id;",
			),
			expErr: "operator '||' incompatible with type 'TIMESTAMP'",
		},
	},
}

var binOpExprWithTSDecimal = tableTest{
	table: tbl(
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
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select a != d from binoptestts_d;",
			),
			expErr: "types 'TIMESTAMP' and 'DECIMAL(2)' are not equatable",
		},
		{
			sqls: sqls(
				"select a = d from binoptestts_d;",
			),
			expErr: "types 'TIMESTAMP' and 'DECIMAL(2)' are not equatable",
		},
		{
			sqls: sqls(
				"select a <= d from binoptestts_d;",
			),
			expErr: "types 'TIMESTAMP' and 'DECIMAL(2)' are not equatable",
		},
		{
			sqls: sqls(
				"select a >= d from binoptestts_d;",
			),
			expErr: "types 'TIMESTAMP' and 'DECIMAL(2)' are not equatable",
		},
		{
			sqls: sqls(
				"select a < d from binoptestts_d;",
			),
			expErr: "types 'TIMESTAMP' and 'DECIMAL(2)' are not equatable",
		},
		{
			sqls: sqls(
				"select a > d from binoptestts_d;",
			),
			expErr: "types 'TIMESTAMP' and 'DECIMAL(2)' are not equatable",
		},
		{
			sqls: sqls(
				"select a & d from binoptestts_d;",
			),
			expErr: "operator '&' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a | d from binoptestts_d;",
			),
			expErr: "operator '|' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a << d from binoptestts_d;",
			),
			expErr: "operator '<<' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a >> d from binoptestts_d;",
			),
			expErr: "operator '>>' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a + d from binoptestts_d;",
			),
			expErr: "operator '+' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a - d from binoptestts_d;",
			),
			expErr: "operator '-' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a * d from binoptestts_d;",
			),
			expErr: "operator '*' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a / d from binoptestts_d;",
			),
			expErr: "operator '/' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a % d from binoptestts_d;",
			),
			expErr: "operator '%' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a || d from binoptestts_d;",
			),
			expErr: "operator '||' incompatible with type 'TIMESTAMP'",
		},
	},
}

var binOpExprWithTSTimestamp = tableTest{
	table: tbl(
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
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select a != ts from binoptestts_ts;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			expRows: rows(
				row(bool(false)),
			),
			compare: compareExactUnordered},
		{
			sqls: sqls(
				"select a = ts from binoptestts_ts;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			expRows: rows(
				row(bool(true)),
			),
			compare: compareExactUnordered},
		{
			sqls: sqls(
				"select a <= ts from binoptestts_ts;",
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
				"select a >= ts from binoptestts_ts;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			expRows: rows(
				row(bool(true)),
			),
			compare: compareExactUnordered},
		{
			sqls: sqls(
				"select a < ts from binoptestts_ts;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			expRows: rows(
				row(bool(false)),
			),
			compare: compareExactUnordered},
		{
			sqls: sqls(
				"select a > ts from binoptestts_ts;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeBool),
			),
			expRows: rows(
				row(bool(false)),
			),
			compare: compareExactUnordered},
		{
			sqls: sqls(
				"select a & ts from binoptestts_ts;",
			),
			expErr: "operator '&' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a | ts from binoptestts_ts;",
			),
			expErr: "operator '|' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a << ts from binoptestts_ts;",
			),
			expErr: "operator '<<' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a >> ts from binoptestts_ts;",
			),
			expErr: "operator '>>' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a + ts from binoptestts_ts;",
			),
			expErr: "operator '+' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a - ts from binoptestts_ts;",
			),
			expErr: "operator '-' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a * ts from binoptestts_ts;",
			),
			expErr: "operator '*' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a / ts from binoptestts_ts;",
			),
			expErr: "operator '/' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a % ts from binoptestts_ts;",
			),
			expErr: "operator '%' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a || ts from binoptestts_ts;",
			),
			expErr: "operator '||' incompatible with type 'TIMESTAMP'",
		},
	},
}

var binOpExprWithTSIDSet = tableTest{
	table: tbl(
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
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select a != b from binoptestts_ids;",
			),
			expErr: "types 'TIMESTAMP' and 'IDSET' are not equatable",
		},
		{
			sqls: sqls(
				"select a = b from binoptestts_ids;",
			),
			expErr: "types 'TIMESTAMP' and 'IDSET' are not equatable",
		},
		{
			sqls: sqls(
				"select a <= b from binoptestts_ids;",
			),
			expErr: " operator '<=' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a >= b from binoptestts_ids;",
			),
			expErr: " operator '>=' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a < b from binoptestts_ids;",
			),
			expErr: " operator '<' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a > b from binoptestts_ids;",
			),
			expErr: " operator '>' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a & b from binoptestts_ids;",
			),
			expErr: " operator '&' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a | b from binoptestts_ids;",
			),
			expErr: " operator '|' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a << b from binoptestts_ids;",
			),
			expErr: " operator '<<' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a >> b from binoptestts_ids;",
			),
			expErr: " operator '>>' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a + b from binoptestts_ids;",
			),
			expErr: " operator '+' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a - b from binoptestts_ids;",
			),
			expErr: " operator '-' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a * b from binoptestts_ids;",
			),
			expErr: " operator '*' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a / b from binoptestts_ids;",
			),
			expErr: " operator '/' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a % b from binoptestts_ids;",
			),
			expErr: " operator '%' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a || b from binoptestts_ids;",
			),
			expErr: "operator '||' incompatible with type 'TIMESTAMP'",
		},
	},
}

var binOpExprWithTSString = tableTest{
	table: tbl(
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
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select a != b from binoptestts_s;",
			),
			expErr: "types 'TIMESTAMP' and 'STRING' are not equatable",
		},
		{
			sqls: sqls(
				"select a = b from binoptestts_s;",
			),
			expErr: "types 'TIMESTAMP' and 'STRING' are not equatable",
		},
		{
			sqls: sqls(
				"select a <= b from binoptestts_s;",
			),
			expErr: " operator '<=' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a >= b from binoptestts_s;",
			),
			expErr: " operator '>=' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a < b from binoptestts_s;",
			),
			expErr: " operator '<' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a > b from binoptestts_s;",
			),
			expErr: " operator '>' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a & b from binoptestts_s;",
			),
			expErr: " operator '&' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a | b from binoptestts_s;",
			),
			expErr: " operator '|' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a << b from binoptestts_s;",
			),
			expErr: " operator '<<' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a >> b from binoptestts_s;",
			),
			expErr: " operator '>>' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a + b from binoptestts_s;",
			),
			expErr: " operator '+' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a - b from binoptestts_s;",
			),
			expErr: " operator '-' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a * b from binoptestts_s;",
			),
			expErr: " operator '*' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a / b from binoptestts_s;",
			),
			expErr: " operator '/' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a % b from binoptestts_s;",
			),
			expErr: " operator '%' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a || b from binoptestts_s;",
			),
			expErr: "operator '||' incompatible with type 'TIMESTAMP'",
		},
	},
}

var binOpExprWithTSStringSet = tableTest{
	table: tbl(
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
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select a != b from binoptestts_ss;",
			),
			expErr: "types 'TIMESTAMP' and 'STRINGSET' are not equatable",
		},
		{
			sqls: sqls(
				"select a = b from binoptestts_ss;",
			),
			expErr: "types 'TIMESTAMP' and 'STRINGSET' are not equatable",
		},
		{
			sqls: sqls(
				"select a <= b from binoptestts_ss;",
			),
			expErr: " operator '<=' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a >= b from binoptestts_ss;",
			),
			expErr: " operator '>=' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a < b from binoptestts_ss;",
			),
			expErr: " operator '<' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a > b from binoptestts_ss;",
			),
			expErr: " operator '>' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a & b from binoptestts_ss;",
			),
			expErr: " operator '&' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a | b from binoptestts_ss;",
			),
			expErr: " operator '|' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a << b from binoptestts_ss;",
			),
			expErr: " operator '<<' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a >> b from binoptestts_ss;",
			),
			expErr: " operator '>>' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a + b from binoptestts_ss;",
			),
			expErr: " operator '+' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a - b from binoptestts_ss;",
			),
			expErr: " operator '-' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a * b from binoptestts_ss;",
			),
			expErr: " operator '*' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a / b from binoptestts_ss;",
			),
			expErr: " operator '/' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a % b from binoptestts_ss;",
			),
			expErr: " operator '%' incompatible with type 'TIMESTAMP'",
		},
		{
			sqls: sqls(
				"select a || b from binoptestts_ss;",
			),
			expErr: "operator '||' incompatible with type 'TIMESTAMP'",
		},
	},
}

//IDSET bin op tests
var binOpExprWithIDSetInt = tableTest{
	table: tbl(
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
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select d != b from binoptestids_i;",
			),
			expErr: "types 'IDSET' and 'INT' are not equatable",
		},
		{
			sqls: sqls(
				"select d = b from binoptestids_i;",
			),
			expErr: "types 'IDSET' and 'INT' are not equatable",
		},
		{
			sqls: sqls(
				"select d <= b from binoptestids_i;",
			),
			expErr: "operator '<=' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select d >= b from binoptestids_i;",
			),
			expErr: "operator '>=' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select d < b from binoptestids_i;",
			),
			expErr: "operator '<' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select d > b from binoptestids_i;",
			),
			expErr: "operator '>' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select d & b from binoptestids_i;",
			),
			expErr: "operator '&' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select d | b from binoptestids_i;",
			),
			expErr: "operator '|' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select d << b from binoptestids_i;",
			),
			expErr: "operator '<<' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select d >> b from binoptestids_i;",
			),
			expErr: "operator '>>' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select d + b from binoptestids_i;",
			),
			expErr: "operator '+' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select d - b from binoptestids_i;",
			),
			expErr: "operator '-' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select d * b from binoptestids_i;",
			),
			expErr: "operator '*' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select d / b from binoptestids_i;",
			),
			expErr: "operator '/' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select d % b from binoptestids_i;",
			),
			expErr: "operator '%' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select d || b from binoptestids_i;",
			),
			expErr: "operator '||' incompatible with type 'IDSET'",
		},
	},
}

var binOpExprWithIDSetBool = tableTest{
	table: tbl(
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
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select d != b from binoptestids_b;",
			),
			expErr: "types 'IDSET' and 'BOOL' are not equatable",
		},
		{
			sqls: sqls(
				"select d = b from binoptestids_b;",
			),
			expErr: "types 'IDSET' and 'BOOL' are not equatable",
		},
		{
			sqls: sqls(
				"select d <= b from binoptestids_b;",
			),
			expErr: "operator '<=' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select d >= b from binoptestids_b;",
			),
			expErr: "operator '>=' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select d < b from binoptestids_b;",
			),
			expErr: "operator '<' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select d > b from binoptestids_b;",
			),
			expErr: "operator '>' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select d & b from binoptestids_b;",
			),
			expErr: "operator '&' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select d | b from binoptestids_b;",
			),
			expErr: "operator '|' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select d << b from binoptestids_b;",
			),
			expErr: "operator '<<' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select d >> b from binoptestids_b;",
			),
			expErr: "operator '>>' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select d + b from binoptestids_b;",
			),
			expErr: "operator '+' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select d - b from binoptestids_b;",
			),
			expErr: "operator '-' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select d * b from binoptestids_b;",
			),
			expErr: "operator '*' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select d / b from binoptestids_b;",
			),
			expErr: "operator '/' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select d % b from binoptestids_b;",
			),
			expErr: "operator '%' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select d || b from binoptestids_b;",
			),
			expErr: "operator '||' incompatible with type 'IDSET'",
		},
	},
}

var binOpExprWithIDSetID = tableTest{
	table: tbl(
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
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select d != b from binoptestids_id;",
			),
			expErr: "types 'IDSET' and 'ID' are not equatable",
		},
		{
			sqls: sqls(
				"select d = b from binoptestids_id;",
			),
			expErr: "types 'IDSET' and 'ID' are not equatable",
		},
		{
			sqls: sqls(
				"select d <= b from binoptestids_id;",
			),
			expErr: "operator '<=' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select d >= b from binoptestids_id;",
			),
			expErr: "operator '>=' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select d < b from binoptestids_id;",
			),
			expErr: "operator '<' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select d > b from binoptestids_id;",
			),
			expErr: "operator '>' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select d & b from binoptestids_id;",
			),
			expErr: "operator '&' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select d | b from binoptestids_id;",
			),
			expErr: "operator '|' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select d << b from binoptestids_id;",
			),
			expErr: "operator '<<' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select d >> b from binoptestids_id;",
			),
			expErr: "operator '>>' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select d + b from binoptestids_id;",
			),
			expErr: "operator '+' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select d - b from binoptestids_id;",
			),
			expErr: "operator '-' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select d * b from binoptestids_id;",
			),
			expErr: "operator '*' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select d / b from binoptestids_id;",
			),
			expErr: "operator '/' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select d % b from binoptestids_id;",
			),
			expErr: "operator '%' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select d || b from binoptestids_id;",
			),
			expErr: "operator '||' incompatible with type 'IDSET'",
		},
	},
}

var binOpExprWithIDSetDecimal = tableTest{
	table: tbl(
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
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select a != d from binoptestids_d;",
			),
			expErr: "types 'IDSET' and 'DECIMAL(2)' are not equatable",
		},
		{
			sqls: sqls(
				"select a = d from binoptestids_d;",
			),
			expErr: "types 'IDSET' and 'DECIMAL(2)' are not equatable",
		},
		{
			sqls: sqls(
				"select a <= d from binoptestids_d;",
			),
			expErr: "operator '<=' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a >= d from binoptestids_d;",
			),
			expErr: "operator '>=' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a < d from binoptestids_d;",
			),
			expErr: "operator '<' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a > d from binoptestids_d;",
			),
			expErr: "operator '>' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a & d from binoptestids_d;",
			),
			expErr: "operator '&' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a | d from binoptestids_d;",
			),
			expErr: "operator '|' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a << d from binoptestids_d;",
			),
			expErr: "operator '<<' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a >> d from binoptestids_d;",
			),
			expErr: "operator '>>' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a + d from binoptestids_d;",
			),
			expErr: "operator '+' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a - d from binoptestids_d;",
			),
			expErr: "operator '-' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a * d from binoptestids_d;",
			),
			expErr: "operator '*' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a / d from binoptestids_d;",
			),
			expErr: "operator '/' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a % d from binoptestids_d;",
			),
			expErr: "operator '%' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a || d from binoptestids_d;",
			),
			expErr: "operator '||' incompatible with type 'IDSET'",
		},
	},
}

var binOpExprWithIDSetTimestamp = tableTest{
	table: tbl(
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
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select a != ts from binoptestids_ts;",
			),
			expErr: "types 'IDSET' and 'TIMESTAMP' are not equatable",
		},
		{
			sqls: sqls(
				"select a = ts from binoptestids_ts;",
			),
			expErr: "types 'IDSET' and 'TIMESTAMP' are not equatable",
		},
		{
			sqls: sqls(
				"select a <= ts from binoptestids_ts;",
			),
			expErr: " operator '<=' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a >= ts from binoptestids_ts;",
			),
			expErr: " operator '>=' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a < ts from binoptestids_ts;",
			),
			expErr: " operator '<' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a > ts from binoptestids_ts;",
			),
			expErr: " operator '>' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a & ts from binoptestids_ts;",
			),
			expErr: "operator '&' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a | ts from binoptestids_ts;",
			),
			expErr: "operator '|' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a << ts from binoptestids_ts;",
			),
			expErr: "operator '<<' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a >> ts from binoptestids_ts;",
			),
			expErr: "operator '>>' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a + ts from binoptestids_ts;",
			),
			expErr: "operator '+' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a - ts from binoptestids_ts;",
			),
			expErr: "operator '-' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a * ts from binoptestids_ts;",
			),
			expErr: "operator '*' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a / ts from binoptestids_ts;",
			),
			expErr: "operator '/' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a % ts from binoptestids_ts;",
			),
			expErr: "operator '%' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a || ts from binoptestids_ts;",
			),
			expErr: "operator '||' incompatible with type 'IDSET'",
		},
	},
}

var binOpExprWithIDSetIDSet = tableTest{
	table: tbl(
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
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select a != b from binoptestids_ids;",
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
				"select a = b from binoptestids_ids;",
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
				"select a <= b from binoptestids_ids;",
			),
			expErr: " operator '<=' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a >= b from binoptestids_ids;",
			),
			expErr: " operator '>=' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a < b from binoptestids_ids;",
			),
			expErr: " operator '<' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a > b from binoptestids_ids;",
			),
			expErr: " operator '>' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a & b from binoptestids_ids;",
			),
			expErr: " operator '&' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a | b from binoptestids_ids;",
			),
			expErr: " operator '|' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a << b from binoptestids_ids;",
			),
			expErr: " operator '<<' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a >> b from binoptestids_ids;",
			),
			expErr: " operator '>>' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a + b from binoptestids_ids;",
			),
			expErr: " operator '+' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a - b from binoptestids_ids;",
			),
			expErr: " operator '-' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a * b from binoptestids_ids;",
			),
			expErr: " operator '*' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a / b from binoptestids_ids;",
			),
			expErr: " operator '/' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a % b from binoptestids_ids;",
			),
			expErr: " operator '%' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a || b from binoptestids_ids;",
			),
			expErr: "operator '||' incompatible with type 'IDSET'",
		},
	},
}

var binOpExprWithIDSetString = tableTest{
	table: tbl(
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
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select a != b from binoptestids_s;",
			),
			expErr: "types 'IDSET' and 'STRING' are not equatable",
		},
		{
			sqls: sqls(
				"select a = b from binoptestids_s;",
			),
			expErr: "types 'IDSET' and 'STRING' are not equatable",
		},
		{
			sqls: sqls(
				"select a <= b from binoptestids_s;",
			),
			expErr: " operator '<=' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a >= b from binoptestids_s;",
			),
			expErr: " operator '>=' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a < b from binoptestids_s;",
			),
			expErr: " operator '<' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a > b from binoptestids_s;",
			),
			expErr: " operator '>' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a & b from binoptestids_s;",
			),
			expErr: " operator '&' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a | b from binoptestids_s;",
			),
			expErr: " operator '|' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a << b from binoptestids_s;",
			),
			expErr: " operator '<<' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a >> b from binoptestids_s;",
			),
			expErr: " operator '>>' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a + b from binoptestids_s;",
			),
			expErr: " operator '+' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a - b from binoptestids_s;",
			),
			expErr: " operator '-' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a * b from binoptestids_s;",
			),
			expErr: " operator '*' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a / b from binoptestids_s;",
			),
			expErr: " operator '/' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a % b from binoptestids_s;",
			),
			expErr: " operator '%' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a || b from binoptestids_s;",
			),
			expErr: "operator '||' incompatible with type 'IDSET'",
		},
	},
}

var binOpExprWithIDSetStringSet = tableTest{
	table: tbl(
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
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select a != b from binoptestids_ss;",
			),
			expErr: "types 'IDSET' and 'STRINGSET' are not equatable",
		},
		{
			sqls: sqls(
				"select a = b from binoptestids_ss;",
			),
			expErr: "types 'IDSET' and 'STRINGSET' are not equatable",
		},
		{
			sqls: sqls(
				"select a <= b from binoptestids_ss;",
			),
			expErr: " operator '<=' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a >= b from binoptestids_ss;",
			),
			expErr: " operator '>=' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a < b from binoptestids_ss;",
			),
			expErr: " operator '<' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a > b from binoptestids_ss;",
			),
			expErr: " operator '>' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a & b from binoptestids_ss;",
			),
			expErr: " operator '&' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a | b from binoptestids_ss;",
			),
			expErr: " operator '|' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a << b from binoptestids_ss;",
			),
			expErr: " operator '<<' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a >> b from binoptestids_ss;",
			),
			expErr: " operator '>>' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a + b from binoptestids_ss;",
			),
			expErr: " operator '+' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a - b from binoptestids_ss;",
			),
			expErr: " operator '-' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a * b from binoptestids_ss;",
			),
			expErr: " operator '*' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a / b from binoptestids_ss;",
			),
			expErr: " operator '/' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a % b from binoptestids_ss;",
			),
			expErr: " operator '%' incompatible with type 'IDSET'",
		},
		{
			sqls: sqls(
				"select a || b from binoptestids_ss;",
			),
			expErr: "operator '||' incompatible with type 'IDSET'",
		},
	},
}

//STRING bin op tests
var binOpExprWithStringInt = tableTest{
	table: tbl(
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
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select d != b from binoptests_i;",
			),
			expErr: "types 'STRING' and 'INT' are not equatable",
		},
		{
			sqls: sqls(
				"select d = b from binoptests_i;",
			),
			expErr: "types 'STRING' and 'INT' are not equatable",
		},
		{
			sqls: sqls(
				"select d <= b from binoptests_i;",
			),
			expErr: "operator '<=' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select d >= b from binoptests_i;",
			),
			expErr: "operator '>=' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select d < b from binoptests_i;",
			),
			expErr: "operator '<' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select d > b from binoptests_i;",
			),
			expErr: "operator '>' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select d & b from binoptests_i;",
			),
			expErr: "operator '&' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select d | b from binoptests_i;",
			),
			expErr: "operator '|' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select d << b from binoptests_i;",
			),
			expErr: "operator '<<' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select d >> b from binoptests_i;",
			),
			expErr: "operator '>>' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select d + b from binoptests_i;",
			),
			expErr: "operator '+' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select d - b from binoptests_i;",
			),
			expErr: "operator '-' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select d * b from binoptests_i;",
			),
			expErr: "operator '*' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select d / b from binoptests_i;",
			),
			expErr: "operator '/' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select d % b from binoptests_i;",
			),
			expErr: "operator '%' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select d || b from binoptests_i;",
			),
			expErr: "operator '||' incompatible with type 'INT'",
		},
	},
}

var binOpExprWithStringBool = tableTest{
	table: tbl(
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
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select d != b from binoptests_b;",
			),
			expErr: "types 'STRING' and 'BOOL' are not equatable",
		},
		{
			sqls: sqls(
				"select d = b from binoptests_b;",
			),
			expErr: "types 'STRING' and 'BOOL' are not equatable",
		},
		{
			sqls: sqls(
				"select d <= b from binoptests_b;",
			),
			expErr: "operator '<=' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select d >= b from binoptests_b;",
			),
			expErr: "operator '>=' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select d < b from binoptests_b;",
			),
			expErr: "operator '<' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select d > b from binoptests_b;",
			),
			expErr: "operator '>' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select d & b from binoptests_b;",
			),
			expErr: "operator '&' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select d | b from binoptests_b;",
			),
			expErr: "operator '|' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select d << b from binoptests_b;",
			),
			expErr: "operator '<<' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select d >> b from binoptests_b;",
			),
			expErr: "operator '>>' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select d + b from binoptests_b;",
			),
			expErr: "operator '+' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select d - b from binoptests_b;",
			),
			expErr: "operator '-' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select d * b from binoptests_b;",
			),
			expErr: "operator '*' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select d / b from binoptests_b;",
			),
			expErr: "operator '/' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select d % b from binoptests_b;",
			),
			expErr: "operator '%' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select d || b from binoptests_b;",
			),
			expErr: "operator '||' incompatible with type 'BOOL'",
		},
	},
}

var binOpExprWithStringID = tableTest{
	table: tbl(
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
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select d != b from binoptests_id;",
			),
			expErr: "types 'STRING' and 'ID' are not equatable",
		},
		{
			sqls: sqls(
				"select d = b from binoptests_id;",
			),
			expErr: "types 'STRING' and 'ID' are not equatable",
		},
		{
			sqls: sqls(
				"select d <= b from binoptests_id;",
			),
			expErr: "operator '<=' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select d >= b from binoptests_id;",
			),
			expErr: "operator '>=' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select d < b from binoptests_id;",
			),
			expErr: "operator '<' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select d > b from binoptests_id;",
			),
			expErr: "operator '>' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select d & b from binoptests_id;",
			),
			expErr: "operator '&' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select d | b from binoptests_id;",
			),
			expErr: "operator '|' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select d << b from binoptests_id;",
			),
			expErr: "operator '<<' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select d >> b from binoptests_id;",
			),
			expErr: "operator '>>' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select d + b from binoptests_id;",
			),
			expErr: "operator '+' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select d - b from binoptests_id;",
			),
			expErr: "operator '-' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select d * b from binoptests_id;",
			),
			expErr: "operator '*' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select d / b from binoptests_id;",
			),
			expErr: "operator '/' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select d % b from binoptests_id;",
			),
			expErr: "operator '%' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select d || b from binoptests_id;",
			),
			expErr: "operator '||' incompatible with type 'ID'",
		},
	},
}

var binOpExprWithStringDecimal = tableTest{
	table: tbl(
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
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select a != d from binoptests_d;",
			),
			expErr: "types 'STRING' and 'DECIMAL(2)' are not equatable",
		},
		{
			sqls: sqls(
				"select a = d from binoptests_d;",
			),
			expErr: "types 'STRING' and 'DECIMAL(2)' are not equatable",
		},
		{
			sqls: sqls(
				"select a <= d from binoptests_d;",
			),
			expErr: "operator '<=' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a >= d from binoptests_d;",
			),
			expErr: "operator '>=' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a < d from binoptests_d;",
			),
			expErr: "operator '<' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a > d from binoptests_d;",
			),
			expErr: "operator '>' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a & d from binoptests_d;",
			),
			expErr: "operator '&' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a | d from binoptests_d;",
			),
			expErr: "operator '|' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a << d from binoptests_d;",
			),
			expErr: "operator '<<' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a >> d from binoptests_d;",
			),
			expErr: "operator '>>' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a + d from binoptests_d;",
			),
			expErr: "operator '+' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a - d from binoptests_d;",
			),
			expErr: "operator '-' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a * d from binoptests_d;",
			),
			expErr: "operator '*' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a / d from binoptests_d;",
			),
			expErr: "operator '/' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a % d from binoptests_d;",
			),
			expErr: "operator '%' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a || d from binoptests_d;",
			),
			expErr: "operator '||' incompatible with type 'DECIMAL(2)'",
		},
	},
}

var binOpExprWithStringTimestamp = tableTest{
	table: tbl(
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
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select a != ts from binoptests_ts;",
			),
			expErr: "types 'STRING' and 'TIMESTAMP' are not equatable",
		},
		{
			sqls: sqls(
				"select a = ts from binoptests_ts;",
			),
			expErr: "types 'STRING' and 'TIMESTAMP' are not equatable",
		},
		{
			sqls: sqls(
				"select a <= ts from binoptests_ts;",
			),
			expErr: " operator '<=' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a >= ts from binoptests_ts;",
			),
			expErr: " operator '>=' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a < ts from binoptests_ts;",
			),
			expErr: " operator '<' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a > ts from binoptests_ts;",
			),
			expErr: " operator '>' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a & ts from binoptests_ts;",
			),
			expErr: "operator '&' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a | ts from binoptests_ts;",
			),
			expErr: "operator '|' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a << ts from binoptests_ts;",
			),
			expErr: "operator '<<' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a >> ts from binoptests_ts;",
			),
			expErr: "operator '>>' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a + ts from binoptests_ts;",
			),
			expErr: "operator '+' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a - ts from binoptests_ts;",
			),
			expErr: "operator '-' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a * ts from binoptests_ts;",
			),
			expErr: "operator '*' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a / ts from binoptests_ts;",
			),
			expErr: "operator '/' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a % ts from binoptests_ts;",
			),
			expErr: "operator '%' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a || ts from binoptests_ts;",
			),
			expErr: "operator '||' incompatible with type 'TIMESTAMP'",
		},
	},
}

var binOpExprWithStringIDSet = tableTest{
	table: tbl(
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
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select a != b from binoptests_ids;",
			),
			expErr: "types 'STRING' and 'IDSET' are not equatable",
		},
		{
			sqls: sqls(
				"select a = b from binoptests_ids;",
			),
			expErr: "types 'STRING' and 'IDSET' are not equatable",
		},
		{
			sqls: sqls(
				"select a <= b from binoptests_ids;",
			),
			expErr: " operator '<=' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a >= b from binoptests_ids;",
			),
			expErr: " operator '>=' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a < b from binoptests_ids;",
			),
			expErr: " operator '<' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a > b from binoptests_ids;",
			),
			expErr: " operator '>' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a & b from binoptests_ids;",
			),
			expErr: " operator '&' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a | b from binoptests_ids;",
			),
			expErr: " operator '|' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a << b from binoptests_ids;",
			),
			expErr: " operator '<<' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a >> b from binoptests_ids;",
			),
			expErr: " operator '>>' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a + b from binoptests_ids;",
			),
			expErr: " operator '+' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a - b from binoptests_ids;",
			),
			expErr: " operator '-' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a * b from binoptests_ids;",
			),
			expErr: " operator '*' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a / b from binoptests_ids;",
			),
			expErr: " operator '/' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a % b from binoptests_ids;",
			),
			expErr: " operator '%' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a || b from binoptests_ids;",
			),
			expErr: "operator '||' incompatible with type 'IDSET'",
		},
	},
}

var binOpExprWithStringString = tableTest{
	table: tbl(
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
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select a != b from binoptests_s;",
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
				"select a = b from binoptests_s;",
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
				"select a <= b from binoptests_s;",
			),
			expErr: " operator '<=' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a >= b from binoptests_s;",
			),
			expErr: " operator '>=' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a < b from binoptests_s;",
			),
			expErr: " operator '<' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a > b from binoptests_s;",
			),
			expErr: " operator '>' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a & b from binoptests_s;",
			),
			expErr: " operator '&' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a | b from binoptests_s;",
			),
			expErr: " operator '|' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a << b from binoptests_s;",
			),
			expErr: " operator '<<' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a >> b from binoptests_s;",
			),
			expErr: " operator '>>' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a + b from binoptests_s;",
			),
			expErr: " operator '+' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a - b from binoptests_s;",
			),
			expErr: " operator '-' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a * b from binoptests_s;",
			),
			expErr: " operator '*' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a / b from binoptests_s;",
			),
			expErr: " operator '/' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a % b from binoptests_s;",
			),
			expErr: " operator '%' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a || b from binoptests_s;",
			),
			expHdrs: hdrs(
				hdr("", fldTypeString),
			),
			expRows: rows(
				row(string("foo101")),
			),
			compare: compareExactUnordered,
		},
	},
}

var binOpExprWithStringStringSet = tableTest{
	table: tbl(
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
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select a != b from binoptests_ss;",
			),
			expErr: "types 'STRING' and 'STRINGSET' are not equatable",
		},
		{
			sqls: sqls(
				"select a = b from binoptests_ss;",
			),
			expErr: "types 'STRING' and 'STRINGSET' are not equatable",
		},
		{
			sqls: sqls(
				"select a <= b from binoptests_ss;",
			),
			expErr: " operator '<=' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a >= b from binoptests_ss;",
			),
			expErr: " operator '>=' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a < b from binoptests_ss;",
			),
			expErr: " operator '<' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a > b from binoptests_ss;",
			),
			expErr: " operator '>' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a & b from binoptests_ss;",
			),
			expErr: " operator '&' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a | b from binoptests_ss;",
			),
			expErr: " operator '|' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a << b from binoptests_ss;",
			),
			expErr: " operator '<<' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a >> b from binoptests_ss;",
			),
			expErr: " operator '>>' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a + b from binoptests_ss;",
			),
			expErr: " operator '+' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a - b from binoptests_ss;",
			),
			expErr: " operator '-' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a * b from binoptests_ss;",
			),
			expErr: " operator '*' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a / b from binoptests_ss;",
			),
			expErr: " operator '/' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a % b from binoptests_ss;",
			),
			expErr: " operator '%' incompatible with type 'STRING'",
		},
		{
			sqls: sqls(
				"select a || b from binoptests_ss;",
			),
			expErr: "operator '||' incompatible with type 'STRINGSET'",
		},
	},
}

//STRINGSET bin op tests
var binOpExprWithStringSetInt = tableTest{
	table: tbl(
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
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select d != b from binoptestss_i;",
			),
			expErr: "types 'STRINGSET' and 'INT' are not equatable",
		},
		{
			sqls: sqls(
				"select d = b from binoptestss_i;",
			),
			expErr: "types 'STRINGSET' and 'INT' are not equatable",
		},
		{
			sqls: sqls(
				"select d <= b from binoptestss_i;",
			),
			expErr: "operator '<=' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select d >= b from binoptestss_i;",
			),
			expErr: "operator '>=' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select d < b from binoptestss_i;",
			),
			expErr: "operator '<' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select d > b from binoptestss_i;",
			),
			expErr: "operator '>' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select d & b from binoptestss_i;",
			),
			expErr: "operator '&' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select d | b from binoptestss_i;",
			),
			expErr: "operator '|' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select d << b from binoptestss_i;",
			),
			expErr: "operator '<<' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select d >> b from binoptestss_i;",
			),
			expErr: "operator '>>' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select d + b from binoptestss_i;",
			),
			expErr: "operator '+' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select d - b from binoptestss_i;",
			),
			expErr: "operator '-' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select d * b from binoptestss_i;",
			),
			expErr: "operator '*' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select d / b from binoptestss_i;",
			),
			expErr: "operator '/' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select d % b from binoptestss_i;",
			),
			expErr: "operator '%' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select d || b from binoptestss_i;",
			),
			expErr: "operator '||' incompatible with type 'STRINGSET'",
		},
	},
}

var binOpExprWithStringSetBool = tableTest{
	table: tbl(
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
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select d != b from binoptestss_b;",
			),
			expErr: "types 'STRINGSET' and 'BOOL' are not equatable",
		},
		{
			sqls: sqls(
				"select d = b from binoptestss_b;",
			),
			expErr: "types 'STRINGSET' and 'BOOL' are not equatable",
		},
		{
			sqls: sqls(
				"select d <= b from binoptestss_b;",
			),
			expErr: "operator '<=' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select d >= b from binoptestss_b;",
			),
			expErr: "operator '>=' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select d < b from binoptestss_b;",
			),
			expErr: "operator '<' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select d > b from binoptestss_b;",
			),
			expErr: "operator '>' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select d & b from binoptestss_b;",
			),
			expErr: "operator '&' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select d | b from binoptestss_b;",
			),
			expErr: "operator '|' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select d << b from binoptestss_b;",
			),
			expErr: "operator '<<' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select d >> b from binoptestss_b;",
			),
			expErr: "operator '>>' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select d + b from binoptestss_b;",
			),
			expErr: "operator '+' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select d - b from binoptestss_b;",
			),
			expErr: "operator '-' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select d * b from binoptestss_b;",
			),
			expErr: "operator '*' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select d / b from binoptestss_b;",
			),
			expErr: "operator '/' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select d % b from binoptestss_b;",
			),
			expErr: "operator '%' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select d || b from binoptestss_b;",
			),
			expErr: "operator '||' incompatible with type 'STRINGSET'",
		},
	},
}

var binOpExprWithStringSetID = tableTest{
	table: tbl(
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
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select d != b from binoptestss_id;",
			),
			expErr: "types 'STRINGSET' and 'ID' are not equatable",
		},
		{
			sqls: sqls(
				"select d = b from binoptestss_id;",
			),
			expErr: "types 'STRINGSET' and 'ID' are not equatable",
		},
		{
			sqls: sqls(
				"select d <= b from binoptestss_id;",
			),
			expErr: "operator '<=' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select d >= b from binoptestss_id;",
			),
			expErr: "operator '>=' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select d < b from binoptestss_id;",
			),
			expErr: "operator '<' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select d > b from binoptestss_id;",
			),
			expErr: "operator '>' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select d & b from binoptestss_id;",
			),
			expErr: "operator '&' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select d | b from binoptestss_id;",
			),
			expErr: "operator '|' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select d << b from binoptestss_id;",
			),
			expErr: "operator '<<' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select d >> b from binoptestss_id;",
			),
			expErr: "operator '>>' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select d + b from binoptestss_id;",
			),
			expErr: "operator '+' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select d - b from binoptestss_id;",
			),
			expErr: "operator '-' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select d * b from binoptestss_id;",
			),
			expErr: "operator '*' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select d / b from binoptestss_id;",
			),
			expErr: "operator '/' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select d % b from binoptestss_id;",
			),
			expErr: "operator '%' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select d || b from binoptestss_id;",
			),
			expErr: "operator '||' incompatible with type 'STRINGSET'",
		},
	},
}

var binOpExprWithStringSetDecimal = tableTest{
	table: tbl(
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
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select a != d from binoptestss_d;",
			),
			expErr: "types 'STRINGSET' and 'DECIMAL(2)' are not equatable",
		},
		{
			sqls: sqls(
				"select a = d from binoptestss_d;",
			),
			expErr: "types 'STRINGSET' and 'DECIMAL(2)' are not equatable",
		},
		{
			sqls: sqls(
				"select a <= d from binoptestss_d;",
			),
			expErr: "operator '<=' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a >= d from binoptestss_d;",
			),
			expErr: "operator '>=' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a < d from binoptestss_d;",
			),
			expErr: "operator '<' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a > d from binoptestss_d;",
			),
			expErr: "operator '>' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a & d from binoptestss_d;",
			),
			expErr: "operator '&' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a | d from binoptestss_d;",
			),
			expErr: "operator '|' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a << d from binoptestss_d;",
			),
			expErr: "operator '<<' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a >> d from binoptestss_d;",
			),
			expErr: "operator '>>' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a + d from binoptestss_d;",
			),
			expErr: "operator '+' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a - d from binoptestss_d;",
			),
			expErr: "operator '-' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a * d from binoptestss_d;",
			),
			expErr: "operator '*' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a / d from binoptestss_d;",
			),
			expErr: "operator '/' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a % d from binoptestss_d;",
			),
			expErr: "operator '%' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a || d from binoptestss_d;",
			),
			expErr: "operator '||' incompatible with type 'STRINGSET'",
		},
	},
}

var binOpExprWithStringSetTimestamp = tableTest{
	table: tbl(
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
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select a != ts from binoptestss_ts;",
			),
			expErr: "types 'STRINGSET' and 'TIMESTAMP' are not equatable",
		},
		{
			sqls: sqls(
				"select a = ts from binoptestss_ts;",
			),
			expErr: "types 'STRINGSET' and 'TIMESTAMP' are not equatable",
		},
		{
			sqls: sqls(
				"select a <= ts from binoptestss_ts;",
			),
			expErr: " operator '<=' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a >= ts from binoptestss_ts;",
			),
			expErr: " operator '>=' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a < ts from binoptestss_ts;",
			),
			expErr: " operator '<' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a > ts from binoptestss_ts;",
			),
			expErr: " operator '>' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a & ts from binoptestss_ts;",
			),
			expErr: "operator '&' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a | ts from binoptestss_ts;",
			),
			expErr: "operator '|' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a << ts from binoptestss_ts;",
			),
			expErr: "operator '<<' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a >> ts from binoptestss_ts;",
			),
			expErr: "operator '>>' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a + ts from binoptestss_ts;",
			),
			expErr: "operator '+' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a - ts from binoptestss_ts;",
			),
			expErr: "operator '-' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a * ts from binoptestss_ts;",
			),
			expErr: "operator '*' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a / ts from binoptestss_ts;",
			),
			expErr: "operator '/' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a % ts from binoptestss_ts;",
			),
			expErr: "operator '%' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a || ts from binoptestss_ts;",
			),
			expErr: "operator '||' incompatible with type 'STRINGSET'",
		},
	},
}

var binOpExprWithStringSetIDSet = tableTest{
	table: tbl(
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
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select a != b from binoptestss_ids;",
			),
			expErr: "types 'STRINGSET' and 'IDSET' are not equatable",
		},
		{
			sqls: sqls(
				"select a = b from binoptestss_ids;",
			),
			expErr: "types 'STRINGSET' and 'IDSET' are not equatable",
		},
		{
			sqls: sqls(
				"select a <= b from binoptestss_ids;",
			),
			expErr: " operator '<=' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a >= b from binoptestss_ids;",
			),
			expErr: " operator '>=' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a < b from binoptestss_ids;",
			),
			expErr: " operator '<' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a > b from binoptestss_ids;",
			),
			expErr: " operator '>' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a & b from binoptestss_ids;",
			),
			expErr: " operator '&' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a | b from binoptestss_ids;",
			),
			expErr: " operator '|' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a << b from binoptestss_ids;",
			),
			expErr: " operator '<<' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a >> b from binoptestss_ids;",
			),
			expErr: " operator '>>' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a + b from binoptestss_ids;",
			),
			expErr: " operator '+' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a - b from binoptestss_ids;",
			),
			expErr: " operator '-' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a * b from binoptestss_ids;",
			),
			expErr: " operator '*' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a / b from binoptestss_ids;",
			),
			expErr: " operator '/' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a % b from binoptestss_ids;",
			),
			expErr: " operator '%' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a || b from binoptestss_ids;",
			),
			expErr: "operator '||' incompatible with type 'STRINGSET'",
		},
	},
}

var binOpExprWithStringSetString = tableTest{
	table: tbl(
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
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select a != b from binoptestss_s;",
			),
			expErr: "types 'STRINGSET' and 'STRING' are not equatable",
		},
		{
			sqls: sqls(
				"select a = b from binoptestss_s;",
			),
			expErr: "types 'STRINGSET' and 'STRING' are not equatable",
		},
		{
			sqls: sqls(
				"select a <= b from binoptestss_s;",
			),
			expErr: " operator '<=' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a >= b from binoptestss_s;",
			),
			expErr: " operator '>=' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a < b from binoptestss_s;",
			),
			expErr: " operator '<' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a > b from binoptestss_s;",
			),
			expErr: " operator '>' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a & b from binoptestss_s;",
			),
			expErr: " operator '&' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a | b from binoptestss_s;",
			),
			expErr: " operator '|' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a << b from binoptestss_s;",
			),
			expErr: " operator '<<' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a >> b from binoptestss_s;",
			),
			expErr: " operator '>>' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a + b from binoptestss_s;",
			),
			expErr: " operator '+' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a - b from binoptestss_s;",
			),
			expErr: " operator '-' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a * b from binoptestss_s;",
			),
			expErr: " operator '*' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a / b from binoptestss_s;",
			),
			expErr: " operator '/' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a % b from binoptestss_s;",
			),
			expErr: " operator '%' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a || b from binoptestss_s;",
			),
			expErr: "operator '||' incompatible with type 'STRINGSET'",
		},
	},
}

var binOpExprWithStringSetStringSet = tableTest{
	table: tbl(
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
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"select a != b from binoptestss_ss;",
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
				"select a = b from binoptestss_ss;",
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
				"select a <= b from binoptestss_ss;",
			),
			expErr: " operator '<=' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a >= b from binoptestss_ss;",
			),
			expErr: " operator '>=' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a < b from binoptestss_ss;",
			),
			expErr: " operator '<' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a > b from binoptestss_ss;",
			),
			expErr: " operator '>' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a & b from binoptestss_ss;",
			),
			expErr: " operator '&' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a | b from binoptestss_ss;",
			),
			expErr: " operator '|' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a << b from binoptestss_ss;",
			),
			expErr: " operator '<<' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a >> b from binoptestss_ss;",
			),
			expErr: " operator '>>' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a + b from binoptestss_ss;",
			),
			expErr: " operator '+' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a - b from binoptestss_ss;",
			),
			expErr: " operator '-' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a * b from binoptestss_ss;",
			),
			expErr: " operator '*' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a / b from binoptestss_ss;",
			),
			expErr: " operator '/' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a % b from binoptestss_ss;",
			),
			expErr: " operator '%' incompatible with type 'STRINGSET'",
		},
		{
			sqls: sqls(
				"select a || b from binoptestss_ss;",
			),
			expErr: "operator '||' incompatible with type 'STRINGSET'",
		},
	},
}
