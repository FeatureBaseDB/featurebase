package defs

import (
	featurebase "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/pql"
)

// groupby tests
var groupByTests = TableTest{
	name: "groupby_test",
	Table: tbl(
		"groupby_test",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("i1", fldTypeInt, "min 0", "max 1000"),
			srcHdr("d1", fldTypeDecimal2),
			srcHdr("s1", fldTypeString),
			srcHdr("i2", fldTypeInt, "min 0", "max 1000"),
			srcHdr("is1", fldTypeIDSet),
			srcHdr("id1", fldTypeID),
		),
		srcRows(
			srcRow(int64(1), int64(10), float64(10), string("10"), int64(100), []int64{1, 2}, int64(1)),
			srcRow(int64(2), int64(10), float64(10), string("10"), int64(200), []int64{1, 2}, int64(2)),
			srcRow(int64(3), int64(11), float64(11), string("11"), nil, []int64{1, 3}, int64(3)),
			srcRow(int64(4), int64(12), float64(12), string("12"), nil, []int64{2, 3}, int64(4)),
			srcRow(int64(5), int64(12), float64(12), string("12"), nil, []int64{1, 3}, int64(1)),
			srcRow(int64(6), int64(13), float64(13), string("13"), nil, []int64{1, 2, 3}, int64(6)),
			srcRow(int64(7), nil, nil, nil, nil, nil, nil),
		),
	),
	SQLTests: []SQLTest{
		{
			name: "groupby-int-orderby-incorrect-reference",
			SQLs: sqls(
				"SELECT COUNT(*), i1 FROM groupby_test group by i1 order by count(*) asc",
				"SELECT COUNT(_id), i1 FROM groupby_test group by i1 order by count(*) asc",
			),
			ExpErr: "column reference, alias reference or column position expected",
		},
		{
			name: "groupby-int-orderby-2",
			SQLs: sqls(
				"SELECT COUNT(*), i1 FROM groupby_test group by i1 order by 2 asc",
				"SELECT COUNT(_id), i1 FROM groupby_test group by i1 order by 2 asc",
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
			Compare: CompareExactOrdered,
		},
		{
			name: "groupby-aliased-int-orderby-aliased-int",
			SQLs: sqls(
				"SELECT COUNT(*), i1 as c FROM groupby_test group by i1 order by c asc",
				"SELECT COUNT(_id), i1 as c FROM groupby_test group by i1 order by c asc",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
				hdr("c", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(2), int64(10)),
				row(int64(1), int64(11)),
				row(int64(2), int64(12)),
				row(int64(1), int64(13)),
			),
			Compare: CompareExactOrdered,
		},
		{
			name: "groupby-aliased-int-orderby-int",
			SQLs: sqls(
				"SELECT COUNT(*), i1 as c FROM groupby_test group by i1 order by i1 asc",
				"SELECT COUNT(_id), i1 as c FROM groupby_test group by i1 order by i1 asc",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
				hdr("c", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(2), int64(10)),
				row(int64(1), int64(11)),
				row(int64(2), int64(12)),
				row(int64(1), int64(13)),
			),
			Compare: CompareExactOrdered,
		},
		{
			name: "groupby-int",
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
			name: "groupby-distinct-int2",
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
			name: "groupby-int-sum-i2",
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
			name: "groupby-int-count",
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
			name: "groupby-int-distinct-sum",
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
			name: "groupby-int-average",
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
			name: "groupby-int-average-decimal",
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
			name: "groupby-percentile",
			SQLs: sqls(
				"select percentile(i1, 0) as p_rows, i1 from groupby_test group by i1",
			),
			ExpErr: "aggregate 'PERCENTILE()' not allowed in GROUP BY",
		},
		{
			name: "groupby-min",
			SQLs: sqls(
				"select min(i1) as p_rows, i1 from groupby_test group by i1",
			),
			ExpErr: "aggregate 'MIN()' not allowed in GROUP BY",
		},
		{
			name: "groupby-max",
			SQLs: sqls(
				"select max(i1) as p_rows, i1 from groupby_test group by i1",
			),
			ExpErr: "aggregate 'MAX()' not allowed in GROUP BY",
		},
		{
			name: "groupby-intset",
			SQLs: sqls(
				"SELECT COUNT(*), is1 FROM groupby_test group by is1",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
				hdr("is1", fldTypeIDSet),
			),
			ExpRows: rows(
				row(int64(5), []int64{1}),
				row(int64(4), []int64{2}),
				row(int64(4), []int64{3}),
			),
			Compare: CompareExactOrdered,
		},
		{
			name: "groupby-id",
			SQLs: sqls(
				"SELECT COUNT(*), id1 FROM groupby_test group by id1",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
				hdr("id1", fldTypeID),
			),
			ExpRows: rows(
				row(int64(2), int64(1)),
				row(int64(1), int64(2)),
				row(int64(1), int64(3)),
				row(int64(1), int64(4)),
				row(int64(1), int64(6)),
			),
			Compare: CompareExactOrdered,
		},
		{
			name: "groupby-string",
			SQLs: sqls(
				"SELECT COUNT(*), s1 FROM groupby_test group by s1",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
				hdr("s1", fldTypeString),
			),
			ExpRows: rows(
				row(int64(2), string("10")),
				row(int64(1), string("11")),
				row(int64(2), string("12")),
				row(int64(1), string("13")),
			),
			Compare: CompareExactOrdered,
		},
		{
			name: "groupby-string-orderby-str-desc",
			SQLs: sqls(
				"SELECT COUNT(*), s1 FROM groupby_test group by s1 order by s1 desc",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
				hdr("s1", fldTypeString),
			),
			ExpRows: rows(
				row(int64(1), string("13")),
				row(int64(2), string("12")),
				row(int64(1), string("11")),
				row(int64(2), string("10")),
			),
			Compare: CompareExactOrdered,
		},
	},
}
