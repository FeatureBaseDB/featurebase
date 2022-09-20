package sql3_test

import (
	"github.com/featurebasedb/featurebase/v3/pql"
	"github.com/featurebasedb/featurebase/v3/sql3/parser"
)

// aggregate function tests
var countTests = tableTest{
	table: tbl(
		"count_test",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("i1", fldTypeInt, "min 0", "max 1000"),
			srcHdr("d1", fldTypeDecimal2),
			srcHdr("i2", fldTypeInt, "min 0", "max 1000"),
		),
		srcRows(
			srcRow(int64(1), int64(10), float64(10), int64(100)),
			srcRow(int64(2), int64(10), float64(10), int64(200)),
			srcRow(int64(3), int64(11), float64(11), nil),
			srcRow(int64(4), int64(12), float64(12), nil),
			srcRow(int64(5), int64(12), float64(12), nil),
			srcRow(int64(6), int64(13), float64(13), nil),
		),
	),
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"SELECT COUNT(i1, d1) AS count_rows FROM count_test",
			),
			expErr: "count of formal parameters (1) does not match count of actual parameters (2)",
		},
		{
			sqls: sqls(
				"SELECT COUNT(1) AS count_rows FROM count_test",
			),
			expErr: "column reference expected",
		},
		{
			sqls: sqls(
				"SELECT COUNT(*) AS count_rows FROM count_test",
				"SELECT COUNT(_id) AS count_rows FROM count_test",
			),
			expHdrs: hdrs(
				hdr("count_rows", fldTypeInt),
			),
			expRows: rows(
				row(int64(6)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"SELECT COUNT(i1) as a, COUNT(i2) as b FROM count_test",
			),
			expHdrs: hdrs(
				hdr("a", fldTypeInt),
				hdr("b", fldTypeInt),
			),
			expRows: rows(
				row(int64(6), int64(2)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"SELECT COUNT(*) + 10 - 11 * 2 AS count_rows FROM count_test",
			),
			expHdrs: hdrs(
				hdr("count_rows", fldTypeInt),
			),
			expRows: rows(
				row(int64(-6)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"SELECT COUNT(*) AS count_rows FROM count_test WHERE i1 = 10",
			),
			expHdrs: hdrs(
				hdr("count_rows", fldTypeInt),
			),
			expRows: rows(
				row(int64(2)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"SELECT COUNT(*) AS count_rows FROM count_test WHERE i1 != 10",
			),
			expHdrs: hdrs(
				hdr("count_rows", fldTypeInt),
			),
			expRows: rows(
				row(int64(4)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"SELECT COUNT(*) AS count_rows FROM count_test WHERE i1 < 12",
			),
			expHdrs: hdrs(
				hdr("count_rows", fldTypeInt),
			),
			expRows: rows(
				row(int64(3)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"SELECT COUNT(*) AS count_rows FROM count_test WHERE i1 > 12",
			),
			expHdrs: hdrs(
				hdr("count_rows", fldTypeInt),
			),
			expRows: rows(
				row(int64(1)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"SELECT COUNT(*) AS count_rows FROM count_test WHERE i1 = 10 AND i2 = 100",
			),
			expHdrs: hdrs(
				hdr("count_rows", fldTypeInt),
			),
			expRows: rows(
				row(int64(1)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"SELECT COUNT(*) AS count_rows FROM count_test WHERE i1 = 10 OR i1 = 200 OR i1 = 12",
			),
			expHdrs: hdrs(
				hdr("count_rows", fldTypeInt),
			),
			expRows: rows(
				row(int64(4)),
			),
			compare: compareExactUnordered,
		},
	},
}

var countDistinctTests = tableTest{
	table: tbl(
		"count_d_test",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("i1", fldTypeInt, "min 0", "max 1000"),
			srcHdr("i2", fldTypeInt, "min 0", "max 1000"),
		),
		srcRows(
			srcRow(int64(1), int64(10), int64(100)),
			srcRow(int64(2), int64(10), int64(200)),
			srcRow(int64(3), int64(11), nil),
			srcRow(int64(4), int64(12), nil),
			srcRow(int64(5), int64(12), nil),
			srcRow(int64(6), int64(13), nil),
		),
	),
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"SELECT COUNT(distinct i1) AS count_rows FROM count_d_test",
			),
			expHdrs: hdrs(
				hdr("count_rows", fldTypeInt),
			),
			expRows: rows(
				row(int64(4)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"SELECT COUNT(distinct i1) AS count_rows FROM count_d_test where i1 > 11",
			),
			expHdrs: hdrs(
				hdr("count_rows", fldTypeInt),
			),
			expRows: rows(
				row(int64(2)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"SELECT COUNT(distinct i1) AS count_rows, sum(i1) as sum_rows FROM count_d_test where i1 > 11",
			),
			expHdrs: hdrs(
				hdr("count_rows", fldTypeInt),
				hdr("sum_rows", fldTypeInt),
			),
			expRows: rows(
				row(int64(2), int64(37)),
			),
			compare: compareExactUnordered,
		},
	},
}

var sumTests = tableTest{
	table: tbl(
		"sum_test",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("i1", fldTypeInt, "min 0", "max 1000"),
			srcHdr("d1", fldTypeDecimal2),
			srcHdr("i2", fldTypeInt, "min 0", "max 1000"),
			srcHdr("s1", fldTypeString),
		),
		srcRows(
			srcRow(int64(1), int64(10), float64(10), int64(100), string("foo")),
			srcRow(int64(2), int64(10), float64(10), int64(200), string("foo")),
			srcRow(int64(3), int64(11), float64(11), nil, string("foo")),
			srcRow(int64(4), int64(12), float64(12), nil, string("foo")),
			srcRow(int64(5), int64(12), float64(12), nil, string("foo")),
			srcRow(int64(6), int64(13), float64(13), nil, string("foo")),
		),
	),
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"SELECT sum(*) AS sum_rows FROM sum_test",
			),
			expErr: "column reference expected",
		},
		{
			sqls: sqls(
				"SELECT sum(_id) AS sum_rows FROM sum_test",
			),
			expErr: "_id column cannot be used in aggregate function 'sum'",
		},
		{
			sqls: sqls(
				"SELECT sum(1) AS sum_rows FROM sum_test",
			),
			expErr: "column reference expected",
		},
		{
			sqls: sqls(
				"SELECT sum(i1, d1) AS sum_rows FROM sum_test",
			),
			expErr: "count of formal parameters (1) does not match count of actual parameters (2)",
		},
		{
			sqls: sqls(
				"SELECT sum(i1) AS sum_rows FROM sum_test",
			),
			expHdrs: hdrs(
				hdr("sum_rows", fldTypeInt),
			),
			expRows: rows(
				row(int64(68)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"SELECT sum(d1) AS sum_rows FROM sum_test",
			),
			expHdrs: hdrs(
				hdr("sum_rows", fldTypeDecimal2),
			),
			expRows: rows(
				row(pql.NewDecimal(6800, 2)),
			),
			compare: compareExactUnordered,
		},
	},
}

var avgTests = tableTest{
	table: tbl(
		"avg_test",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("i1", fldTypeInt, "min 0", "max 1000"),
			srcHdr("d1", fldTypeDecimal2),
			srcHdr("s1", fldTypeString),
		),
		srcRows(
			srcRow(int64(1), int64(10), float64(10), string("foo")),
			srcRow(int64(2), int64(10), float64(10), string("foo")),
			srcRow(int64(3), int64(11), float64(11), string("foo")),
			srcRow(int64(4), int64(12), float64(12), string("foo")),
			srcRow(int64(5), int64(12), float64(12), string("foo")),
			srcRow(int64(6), int64(13), float64(13), string("foo")),
		),
	),
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"SELECT avg(*) AS avg_rows FROM avg_test",
			),
			expErr: "column reference expected",
		},
		{
			sqls: sqls(
				"SELECT avg(_id) AS avg_rows FROM avg_test",
			),
			expErr: "_id column cannot be used in aggregate function 'avg'",
		},
		{
			sqls: sqls(
				"SELECT avg(i1, d1) AS avg_rows FROM avg_test",
			),
			expErr: "count of formal parameters (1) does not match count of actual parameters (2)",
		},
		{
			sqls: sqls(
				"SELECT avg(s1) AS avg_rows FROM avg_test",
			),
			expErr: "integer or decimal expression expected",
		},
		{
			sqls: sqls(
				"SELECT avg(i1) AS avg_rows FROM avg_test",
				"SELECT avg(d1) AS avg_rows FROM avg_test",
			),
			expHdrs: hdrs(
				hdr("avg_rows", parser.NewDataTypeDecimal(4)),
			),
			expRows: rows(
				row(pql.NewDecimal(113333, 4)),
			),
			compare: compareExactUnordered,
		},
	},
}

var percentileTests = tableTest{
	table: tbl(
		"percentile_test",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("i1", fldTypeInt, "min 0", "max 1000"),
			srcHdr("d1", fldTypeDecimal2),
			srcHdr("s1", fldTypeString),
		),
		srcRows(
			srcRow(int64(1), int64(10), float64(10), string("foo")),
			srcRow(int64(2), int64(10), float64(10), string("foo")),
			srcRow(int64(3), int64(11), float64(11), string("foo")),
			srcRow(int64(4), int64(12), float64(12), string("foo")),
			srcRow(int64(5), int64(12), float64(12), string("foo")),
			srcRow(int64(6), int64(13), float64(13), string("foo")),
		),
	),
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"SELECT percentile(*) AS avg_rows FROM percentile_test",
			),
			expErr: "column reference expected",
		},
		{
			sqls: sqls(
				"SELECT percentile(10, i1) AS avg_rows FROM percentile_test",
			),
			expErr: "column reference expected",
		},
		{
			sqls: sqls(
				"SELECT percentile(_id, 50) AS avg_rows FROM percentile_test",
			),
			expErr: "_id column cannot be used in aggregate function 'percentile'",
		},
		{
			sqls: sqls(
				"SELECT percentile(i1, d1) AS avg_rows FROM percentile_test",
			),
			expErr: "literal expression expected",
		},
		{
			sqls: sqls(
				"SELECT percentile(s1, 50) AS avg_rows FROM percentile_test",
			),
			expErr: "integer, decimal or timestamp expression expected",
		},
		{
			sqls: sqls(
				"SELECT percentile(i1, 50) AS p_rows FROM percentile_test",
			),
			expHdrs: hdrs(
				hdr("p_rows", fldTypeInt),
			),
			expRows: rows(
				row(int64(12)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"SELECT percentile(d1, 50) AS p_rows FROM percentile_test",
			),
			expHdrs: hdrs(
				hdr("p_rows", fldTypeDecimal2),
			),
			expRows: rows(
				row(pql.NewDecimal(1000, 2)),
			),
			compare: compareExactUnordered,
		},
	},
}

var minmaxTests = tableTest{
	table: tbl(
		"minmax_test",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("i1", fldTypeInt, "min 0", "max 1000"),
			srcHdr("d1", fldTypeDecimal2),
			srcHdr("s1", fldTypeString),
		),
		srcRows(
			srcRow(int64(1), int64(10), float64(10), string("foo")),
			srcRow(int64(2), int64(10), float64(10), string("foo")),
			srcRow(int64(3), int64(11), float64(11), string("foo")),
			srcRow(int64(4), int64(12), float64(12), string("foo")),
			srcRow(int64(5), int64(12), float64(12), string("foo")),
			srcRow(int64(6), int64(13), float64(13), string("foo")),
		),
	),
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"SELECT min(*) AS p_rows FROM minmax_test",
				"SELECT max(*) AS p_rows FROM minmax_test",
			),
			expErr: "column reference expected",
		},
		{
			sqls: sqls(
				"SELECT min(i1, d1) AS p_rows FROM minmax_test",
				"SELECT max(i1, d1) AS p_rows FROM minmax_test",
			),
			expErr: "count of formal parameters (1) does not match count of actual parameters (2)",
		},
		{
			sqls: sqls(
				"SELECT min(1) AS p_rows FROM minmax_test",
				"SELECT max(1) AS p_rows FROM minmax_test",
			),
			expErr: "column reference expected",
		},
		{
			sqls: sqls(
				"SELECT min(_id) AS p_rows FROM minmax_test",
				"SELECT max(_id) AS p_rows FROM minmax_test",
			),
			expErr: "_id column cannot be used in aggregate function",
		},
		{
			sqls: sqls(
				"SELECT min(s1) AS p_rows FROM minmax_test",
				"SELECT max(s1) AS p_rows FROM minmax_test",
			),
			expErr: "integer, decimal or timestamp expression expected",
		},
		{
			sqls: sqls(
				"SELECT min(i1) AS p_rows FROM minmax_test",
			),
			expHdrs: hdrs(
				hdr("p_rows", fldTypeInt),
			),
			expRows: rows(
				row(int64(10)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"SELECT max(i1) AS p_rows FROM minmax_test",
			),
			expHdrs: hdrs(
				hdr("p_rows", fldTypeInt),
			),
			expRows: rows(
				row(int64(13)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"SELECT min(d1) AS p_rows FROM minmax_test",
			),
			expHdrs: hdrs(
				hdr("p_rows", fldTypeDecimal2),
			),
			expRows: rows(
				row(pql.NewDecimal(1000, 2)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"SELECT max(d1) AS p_rows FROM minmax_test",
			),
			expHdrs: hdrs(
				hdr("p_rows", fldTypeDecimal2),
			),
			expRows: rows(
				row(pql.NewDecimal(1300, 2)),
			),
			compare: compareExactUnordered,
		},
	},
}
