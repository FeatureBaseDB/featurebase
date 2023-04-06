package defs

import (
	featurebase "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/pql"
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
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"SELECT COUNT(*), i1 FROM groupby_test group by i1 order by count(*) asc",
				"SELECT COUNT(_id), i1 FROM groupby_test group by i1 order by count(*) asc",
			),
			ExpErr: "column reference, alias reference or column position expected",
		},
		{
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
		{
			SQLs: sqls(
				"SELECT COUNT(*), is1 FROM groupby_test group by is1",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
				hdr("is1", fldTypeIDSet),
			),
			ExpRows: rows(
				row(int64(2), []int64{1, 2}),
				row(int64(2), []int64{1, 3}),
				row(int64(1), []int64{2, 3}),
				row(int64(1), []int64{1, 2, 3}),
			),
			Compare: CompareExactOrdered,
		},
		{
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
	},
}

// groupby/distinct with sets tests
var groupBySetDistinctTests = TableTest{
	Table: tbl(
		"groupby_set_test",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("ids1", fldTypeIDSet),
			srcHdr("ss1", fldTypeStringSet),
		),
		srcRows(
			srcRow(int64(1), []int64{1, 2}, []string{"a", "b"}),
			srcRow(int64(2), []int64{3, 4}, []string{"d", "e"}),
			srcRow(int64(3), []int64{1, 4}, []string{"a", "d"}),
			srcRow(int64(4), []int64{3, 2}, []string{"c", "b"}),
			srcRow(int64(5), []int64{3, 2}, []string{"c", "b"}),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select distinct ids1 from groupby_set_test with (flatter(foo))",
			),
			ExpErr: "unknown query hint 'flatter'",
		},
		{
			SQLs: sqls(
				"select distinct ids1 from groupby_set_test with (flatten(foo))",
			),
			ExpErr: "column 'foo' not found",
		},
		{
			SQLs: sqls(
				"select distinct ids1 from groupby_set_test with (flatten(foo, bar))",
			),
			ExpErr: "query hint 'flatten' expected 1 parameter(s) (column name), got 2 parameters",
		},
		{
			SQLs: sqls(
				"select distinct ids1 from groupby_set_test",
			),
			ExpHdrs: hdrs(
				hdr("ids1", fldTypeIDSet),
			),
			ExpRows: rows(
				row([]int64{1, 2}),
				row([]int64{3, 4}),
				row([]int64{1, 4}),
				row([]int64{2, 3}),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select distinct ids1 from groupby_set_test with (flatten(ids1))",
			),
			ExpHdrs: hdrs(
				hdr("ids1", fldTypeIDSet),
			),
			ExpRows: rows(
				row([]int64{1}),
				row([]int64{2}),
				row([]int64{3}),
				row([]int64{4}),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select distinct ids1, ss1 from groupby_set_test",
			),
			ExpHdrs: hdrs(
				hdr("ids1", fldTypeIDSet),
				hdr("ss1", fldTypeStringSet),
			),
			ExpRows: rows(
				row([]int64{1, 2}, []string{"a", "b"}),
				row([]int64{3, 4}, []string{"d", "e"}),
				row([]int64{1, 4}, []string{"a", "d"}),
				row([]int64{2, 3}, []string{"b", "c"}),
			),
			Compare:        CompareExactUnordered,
			SortStringKeys: true,
		},
		{
			SQLs: sqls(
				"select distinct ids1, ss1 from groupby_set_test with (flatten(ids1))",
			),
			ExpHdrs: hdrs(
				hdr("ids1", fldTypeIDSet),
				hdr("ss1", fldTypeStringSet),
			),
			ExpRows: rows(
				row([]int64{1, 2}, []string{"a", "b"}),
				row([]int64{3, 4}, []string{"d", "e"}),
				row([]int64{1, 4}, []string{"a", "d"}),
				row([]int64{2, 3}, []string{"b", "c"}),
			),
			Compare:        CompareExactUnordered,
			SortStringKeys: true,
		},
		{
			SQLs: sqls(
				"select count(*), ids1 from groupby_set_test group by ids1",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
				hdr("ids1", fldTypeIDSet),
			),
			ExpRows: rows(
				row(int64(1), []int64{1, 2}),
				row(int64(1), []int64{3, 4}),
				row(int64(1), []int64{1, 4}),
				row(int64(2), []int64{2, 3}),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select count(*), ids1 from groupby_set_test with (flatten(ids1)) group by ids1",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
				hdr("ids1", fldTypeIDSet),
			),
			ExpRows: rows(
				row(int64(2), []int64{1}),
				row(int64(3), []int64{2}),
				row(int64(3), []int64{3}),
				row(int64(2), []int64{4}),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select distinct ss1 from groupby_set_test",
			),
			ExpHdrs: hdrs(
				hdr("ss1", fldTypeStringSet),
			),
			ExpRows: rows(
				row([]string{"a", "b"}),
				row([]string{"d", "e"}),
				row([]string{"a", "d"}),
				row([]string{"b", "c"}),
			),
			Compare:        CompareExactUnordered,
			SortStringKeys: true,
		},
		{
			SQLs: sqls(
				"select distinct ss1 from groupby_set_test with (flatten(ss1))",
			),
			ExpHdrs: hdrs(
				hdr("ss1", fldTypeStringSet),
			),
			ExpRows: rows(
				row([]string{"a"}),
				row([]string{"b"}),
				row([]string{"c"}),
				row([]string{"d"}),
				row([]string{"e"}),
			),
			Compare:        CompareExactUnordered,
			SortStringKeys: true,
		},
		{
			SQLs: sqls(
				"select count(*), ss1 from groupby_set_test group by ss1",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
				hdr("ss1", fldTypeStringSet),
			),
			ExpRows: rows(
				row(int64(1), []string{"a", "b"}),
				row(int64(1), []string{"d", "e"}),
				row(int64(1), []string{"a", "d"}),
				row(int64(2), []string{"b", "c"}),
			),
			Compare:        CompareExactUnordered,
			SortStringKeys: true,
		},
		{
			SQLs: sqls(
				"select count(*), ss1 from groupby_set_test with (flatten(ss1)) group by ss1",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
				hdr("ss1", fldTypeStringSet),
			),
			ExpRows: rows(
				row(int64(2), []string{"a"}),
				row(int64(3), []string{"b"}),
				row(int64(2), []string{"c"}),
				row(int64(2), []string{"d"}),
				row(int64(1), []string{"e"}),
			),
			Compare:        CompareExactUnordered,
			SortStringKeys: true,
		},
	},
}
