package sql3_test

import (
	"github.com/featurebasedb/featurebase/v3/pql"
	"github.com/featurebasedb/featurebase/v3/sql3/parser"
)

// groupby tests
var groupByTests = tableTest{
	table: tbl(
		"groupby_test",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("i1", fldTypeInt, "min 0", "max 1000"),
			srcHdr("d1", fldTypeDecimal2),
			srcHdr("s1", fldTypeString),
			srcHdr("i2", fldTypeInt, "min 0", "max 1000"),
		),
		srcRows(
			srcRow(int64(1), int64(10), float64(10), string("10"), int64(100)),
			srcRow(int64(2), int64(10), float64(10), string("10"), int64(200)),
			srcRow(int64(3), int64(11), float64(11), string("11"), nil),
			srcRow(int64(4), int64(12), float64(12), string("12"), nil),
			srcRow(int64(5), int64(12), float64(12), string("12"), nil),
			srcRow(int64(6), int64(13), float64(13), string("13"), nil),
		),
	),
	sqlTests: []sqlTest{
		{
			sqls: sqls(
				"SELECT COUNT(*), i1 FROM groupby_test group by i1",
				"SELECT COUNT(_id), i1 FROM groupby_test group by i1",
			),
			expHdrs: hdrs(
				hdr("", fldTypeInt),
				hdr("i1", fldTypeInt),
			),
			expRows: rows(
				row(int64(2), int64(10)),
				row(int64(1), int64(11)),
				row(int64(2), int64(12)),
				row(int64(1), int64(13)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"SELECT COUNT(distinct i2) AS count_rows, i1 FROM groupby_test group by i1",
			),
			expHdrs: hdrs(
				hdr("count_rows", fldTypeInt),
				hdr("i1", fldTypeInt),
			),
			expRows: rows(
				row(int64(2), int64(10)),
				row(int64(0), int64(11)),
				row(int64(0), int64(12)),
				row(int64(0), int64(13)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"SELECT sum(i2) AS sum_rows, i1 FROM groupby_test group by i1",
			),
			expHdrs: hdrs(
				hdr("sum_rows", fldTypeInt),
				hdr("i1", fldTypeInt),
			),
			expRows: rows(
				row(int64(300), int64(10)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"SELECT COUNT(*) FROM groupby_test group by i1",
			),
			expHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			expRows: rows(
				row(int64(2)),
				row(int64(1)),
				row(int64(2)),
				row(int64(1)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select count(distinct i2) AS count_rows, sum(i2) as sum_rows, i1 from groupby_test group by i1",
			),
			expHdrs: hdrs(
				hdr("count_rows", fldTypeInt),
				hdr("sum_rows", fldTypeInt),
				hdr("i1", fldTypeInt),
			),
			expRows: rows(
				row(int64(2), int64(300), int64(10)),
				row(int64(0), nil, int64(11)),
				row(int64(0), nil, int64(12)),
				row(int64(0), nil, int64(13)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select avg(i1) as avg_rows, i1 from groupby_test group by i1",
			),
			expHdrs: hdrs(
				hdr("avg_rows", parser.NewDataTypeDecimal(4)),
				hdr("i1", fldTypeInt),
			),
			expRows: rows(
				row(pql.NewDecimal(100000, 4), int64(10)),
				row(pql.NewDecimal(110000, 4), int64(11)),
				row(pql.NewDecimal(120000, 4), int64(12)),
				row(pql.NewDecimal(130000, 4), int64(13)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select avg(d1) as avg_rows, i1 from groupby_test group by i1",
			),
			expHdrs: hdrs(
				hdr("avg_rows", parser.NewDataTypeDecimal(4)),
				hdr("i1", fldTypeInt),
			),
			expRows: rows(
				row(pql.NewDecimal(100000, 4), int64(10)),
				row(pql.NewDecimal(110000, 4), int64(11)),
				row(pql.NewDecimal(120000, 4), int64(12)),
				row(pql.NewDecimal(130000, 4), int64(13)),
			),
			compare: compareExactUnordered,
		},
		{
			sqls: sqls(
				"select percentile(i1, 0) as p_rows, i1 from groupby_test group by i1",
			),
			expErr: "aggregate 'PERCENTILE()' not allowed in GROUP BY",
		},
		{
			sqls: sqls(
				"select min(i1) as p_rows, i1 from groupby_test group by i1",
			),
			expErr: "aggregate 'MIN()' not allowed in GROUP BY",
		},
		{
			sqls: sqls(
				"select max(i1) as p_rows, i1 from groupby_test group by i1",
			),
			expErr: "aggregate 'MAX()' not allowed in GROUP BY",
		},
	},
}
