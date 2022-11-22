package defs

import (
	featurebase "github.com/molecula/featurebase/v3"
	"github.com/molecula/featurebase/v3/dax"
	"github.com/molecula/featurebase/v3/pql"
)

// groupby tests
var groupByTests = TableTest{
	Table: tbl(
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
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"SELECT COUNT(*), i1 FROM groupby_test group by i1",
				"SELECT COUNT(_id), i1 FROM groupby_test group by i1",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
				hdr("i1", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(2), int64(10)),
				row(int64(1), int64(11)),
				row(int64(2), int64(12)),
				row(int64(1), int64(13)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"SELECT COUNT(distinct i2) AS count_rows, i1 FROM groupby_test group by i1",
			),
			ExpHdrs: hdrs(
				hdr("count_rows", fldTypeInt),
				hdr("i1", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(2), int64(10)),
				row(int64(0), int64(11)),
				row(int64(0), int64(12)),
				row(int64(0), int64(13)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"SELECT sum(i2) AS sum_rows, i1 FROM groupby_test group by i1",
			),
			ExpHdrs: hdrs(
				hdr("sum_rows", fldTypeInt),
				hdr("i1", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(300), int64(10)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"SELECT COUNT(*) FROM groupby_test group by i1",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(2)),
				row(int64(1)),
				row(int64(2)),
				row(int64(1)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select count(distinct i2) AS count_rows, sum(i2) as sum_rows, i1 from groupby_test group by i1",
			),
			ExpHdrs: hdrs(
				hdr("count_rows", fldTypeInt),
				hdr("sum_rows", fldTypeInt),
				hdr("i1", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(2), int64(300), int64(10)),
				row(int64(0), nil, int64(11)),
				row(int64(0), nil, int64(12)),
				row(int64(0), nil, int64(13)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select avg(i1) as avg_rows, i1 from groupby_test group by i1",
			),
			ExpHdrs: hdrs(
				hdr("avg_rows", featurebase.WireQueryField{
					Type:     dax.BaseTypeDecimal + "(4)",
					BaseType: dax.BaseTypeDecimal,
					TypeInfo: map[string]interface{}{"scale": int64(4)},
				}),
				hdr("i1", fldTypeInt),
			),
			ExpRows: rows(
				row(pql.NewDecimal(100000, 4), int64(10)),
				row(pql.NewDecimal(110000, 4), int64(11)),
				row(pql.NewDecimal(120000, 4), int64(12)),
				row(pql.NewDecimal(130000, 4), int64(13)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select avg(d1) as avg_rows, i1 from groupby_test group by i1",
			),
			ExpHdrs: hdrs(
				hdr("avg_rows", featurebase.WireQueryField{
					Type:     dax.BaseTypeDecimal + "(4)",
					BaseType: dax.BaseTypeDecimal,
					TypeInfo: map[string]interface{}{"scale": int64(4)},
				}),
				hdr("i1", fldTypeInt),
			),
			ExpRows: rows(
				row(pql.NewDecimal(100000, 4), int64(10)),
				row(pql.NewDecimal(110000, 4), int64(11)),
				row(pql.NewDecimal(120000, 4), int64(12)),
				row(pql.NewDecimal(130000, 4), int64(13)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select percentile(i1, 0) as p_rows, i1 from groupby_test group by i1",
			),
			ExpErr: "aggregate 'PERCENTILE()' not allowed in GROUP BY",
		},
		{
			SQLs: sqls(
				"select min(i1) as p_rows, i1 from groupby_test group by i1",
			),
			ExpErr: "aggregate 'MIN()' not allowed in GROUP BY",
		},
		{
			SQLs: sqls(
				"select max(i1) as p_rows, i1 from groupby_test group by i1",
			),
			ExpErr: "aggregate 'MAX()' not allowed in GROUP BY",
		},
	},
}
