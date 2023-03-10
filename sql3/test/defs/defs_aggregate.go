package defs

import (
	featurebase "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/pql"
)

// aggregate function tests
var countTests = TableTest{
	Table: tbl(
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
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"SELECT COUNT(i1, d1) AS count_rows FROM count_test",
			),
			ExpErr: "count of formal parameters (1) does not match count of actual parameters (2)",
		},
		{
			SQLs: sqls(
				"SELECT COUNT(1) AS count_rows FROM count_test",
			),
			ExpErr: "column reference expected",
		},
		{
			SQLs: sqls(
				"SELECT COUNT(*) AS count_rows FROM count_test",
				"SELECT COUNT(_id) AS count_rows FROM count_test",
			),
			ExpHdrs: hdrs(
				hdr("count_rows", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(6)),
			),
			Compare: CompareExactUnordered,
			PlanCheck: func(jplan []byte) error {
				return operatorPresentAtPath(jplan, "$.child.child.operators[0]._op", "*planner.PlanOpPQLAggregate")
			},
		},
		{
			SQLs: sqls(
				"SELECT COUNT(i1) as a, COUNT(i2) as b FROM count_test",
			),
			ExpHdrs: hdrs(
				hdr("a", fldTypeInt),
				hdr("b", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(6), int64(2)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"SELECT COUNT(*) + 10 - 11 * 2 AS count_rows FROM count_test",
			),
			ExpHdrs: hdrs(
				hdr("count_rows", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(-6)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"SELECT COUNT(*) AS count_rows FROM count_test WHERE i1 = 10",
			),
			ExpHdrs: hdrs(
				hdr("count_rows", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(2)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"SELECT COUNT(*) AS count_rows FROM count_test WHERE i1 != 10",
			),
			ExpHdrs: hdrs(
				hdr("count_rows", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(4)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"SELECT COUNT(*) AS count_rows FROM count_test WHERE i1 < 12",
			),
			ExpHdrs: hdrs(
				hdr("count_rows", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(3)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"SELECT COUNT(*) AS count_rows FROM count_test WHERE i1 > 12",
			),
			ExpHdrs: hdrs(
				hdr("count_rows", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(1)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"SELECT COUNT(*) AS count_rows FROM count_test WHERE i1 = 10 AND i2 = 100",
			),
			ExpHdrs: hdrs(
				hdr("count_rows", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(1)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"SELECT COUNT(*) AS count_rows FROM count_test WHERE i1 = 10 OR i1 = 200 OR i1 = 12",
			),
			ExpHdrs: hdrs(
				hdr("count_rows", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(4)),
			),
			Compare: CompareExactUnordered,
		},
	},
}

var countDistinctTests = TableTest{
	Table: tbl(
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
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"SELECT COUNT(distinct i1) AS count_rows FROM count_d_test",
			),
			ExpHdrs: hdrs(
				hdr("count_rows", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(4)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"SELECT COUNT(distinct i1) AS count_rows FROM count_d_test where i1 > 11",
			),
			ExpHdrs: hdrs(
				hdr("count_rows", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(2)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"SELECT COUNT(distinct i1) AS count_rows, sum(i1) as sum_rows FROM count_d_test where i1 > 11",
			),
			ExpHdrs: hdrs(
				hdr("count_rows", fldTypeInt),
				hdr("sum_rows", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(2), int64(37)),
			),
			Compare: CompareExactUnordered,
		},
	},
}

var sumTests = TableTest{
	Table: tbl(
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
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"SELECT sum(*) AS sum_rows FROM sum_test",
			),
			ExpErr: "column reference expected",
		},
		{
			SQLs: sqls(
				"SELECT sum(_id) AS sum_rows FROM sum_test",
			),
			ExpErr: "_id column cannot be used in aggregate function 'sum'",
		},
		{
			SQLs: sqls(
				"SELECT sum(1) AS sum_rows FROM sum_test",
			),
			ExpHdrs: hdrs(
				hdr("sum_rows", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(6)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"SELECT sum(i1, d1) AS sum_rows FROM sum_test",
			),
			ExpErr: "count of formal parameters (1) does not match count of actual parameters (2)",
		},
		{
			SQLs: sqls(
				"SELECT sum(i1) AS sum_rows FROM sum_test",
			),
			ExpHdrs: hdrs(
				hdr("sum_rows", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(68)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"SELECT sum(d1) AS sum_rows FROM sum_test",
			),
			ExpHdrs: hdrs(
				hdr("sum_rows", fldTypeDecimal2),
			),
			ExpRows: rows(
				row(pql.NewDecimal(6800, 2)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"SELECT sum(d1 + 5) AS sum_rows FROM sum_test",
			),
			ExpHdrs: hdrs(
				hdr("sum_rows", fldTypeDecimal2),
			),
			ExpRows: rows(
				row(pql.NewDecimal(9800, 2)),
			),
			Compare: CompareExactUnordered,
		},
	},
}

var avgTests = TableTest{
	Table: tbl(
		"avg_test",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("i1", fldTypeInt, "min 0", "max 1000"),
			srcHdr("d1", fldTypeDecimal2),
			srcHdr("s1", fldTypeString),
			srcHdr("id1", fldTypeID),
		),
		srcRows(
			srcRow(int64(1), int64(10), float64(10), string("foo"), int64(10)),
			srcRow(int64(2), int64(10), float64(10), string("foo"), int64(11)),
			srcRow(int64(3), int64(11), float64(11), string("foo"), int64(12)),
			srcRow(int64(4), int64(12), float64(12), string("foo"), int64(13)),
			srcRow(int64(5), int64(12), float64(12), string("foo"), int64(14)),
			srcRow(int64(6), int64(13), float64(13), string("foo"), int64(15)),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"SELECT avg(*) AS avg_rows FROM avg_test",
			),
			ExpErr: "column reference expected",
		},
		{
			SQLs: sqls(
				"SELECT avg(_id) AS avg_rows FROM avg_test",
			),
			ExpErr: "_id column cannot be used in aggregate function 'avg'",
		},
		{
			SQLs: sqls(
				"SELECT avg(i1, d1) AS avg_rows FROM avg_test",
			),
			ExpErr: "count of formal parameters (1) does not match count of actual parameters (2)",
		},
		{
			SQLs: sqls(
				"SELECT avg(s1) AS avg_rows FROM avg_test",
			),
			ExpErr: "integer or decimal expression expected",
		},
		{
			SQLs: sqls(
				"SELECT avg(id1) AS avg_rows FROM avg_test",
			),
			ExpHdrs: hdrs(
				hdr("avg_rows", featurebase.WireQueryField{
					Type:     dax.BaseTypeDecimal + "(4)",
					BaseType: dax.BaseTypeDecimal,
					TypeInfo: map[string]interface{}{"scale": int64(4)},
				}),
			),
			ExpRows: rows(
				row(pql.NewDecimal(125000, 4)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"SELECT avg(i1) AS avg_rows FROM avg_test",
				"SELECT avg(d1) AS avg_rows FROM avg_test",
			),
			ExpHdrs: hdrs(
				hdr("avg_rows", featurebase.WireQueryField{
					Type:     dax.BaseTypeDecimal + "(4)",
					BaseType: dax.BaseTypeDecimal,
					TypeInfo: map[string]interface{}{"scale": int64(4)},
				}),
			),
			ExpRows: rows(
				row(pql.NewDecimal(113333, 4)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"SELECT avg(len(s1)) AS avg_rows FROM avg_test",
			),
			ExpHdrs: hdrs(
				hdr("avg_rows", featurebase.WireQueryField{
					Type:     dax.BaseTypeDecimal + "(4)",
					BaseType: dax.BaseTypeDecimal,
					TypeInfo: map[string]interface{}{"scale": int64(4)},
				}),
			),
			ExpRows: rows(
				row(pql.NewDecimal(30000, 4)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"SELECT avg(i1) AS avg_rows FROM avg_test WHERE i1 > 100",
			),
			ExpHdrs: hdrs(
				hdr("avg_rows", featurebase.WireQueryField{
					Type:     dax.BaseTypeDecimal + "(4)",
					BaseType: dax.BaseTypeDecimal,
					TypeInfo: map[string]interface{}{"scale": int64(4)},
				}),
			),
			ExpRows: rows(
				row(pql.NewDecimal(0, 4)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"SELECT avg(d1) AS avg_rows FROM avg_test WHERE d1 > 100.0",
			),
			ExpHdrs: hdrs(
				hdr("avg_rows", featurebase.WireQueryField{
					Type:     dax.BaseTypeDecimal + "(4)",
					BaseType: dax.BaseTypeDecimal,
					TypeInfo: map[string]interface{}{"scale": int64(4)},
				}),
			),
			ExpRows: rows(
				row(pql.NewDecimal(0, 4)),
			),
			Compare: CompareExactUnordered,
		},
	},
}

var percentileTests = TableTest{
	Table: tbl(
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
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"SELECT percentile(*) AS avg_rows FROM percentile_test",
			),
			ExpErr: "column reference expected",
		},
		{
			SQLs: sqls(
				"SELECT percentile(10, i1) AS avg_rows FROM percentile_test",
			),
			ExpErr: "column reference expected",
		},
		{
			SQLs: sqls(
				"SELECT percentile(_id, 50) AS avg_rows FROM percentile_test",
			),
			ExpErr: "_id column cannot be used in aggregate function 'percentile'",
		},
		{
			SQLs: sqls(
				"SELECT percentile(i1, d1) AS avg_rows FROM percentile_test",
			),
			ExpErr: "literal expression expected",
		},
		{
			SQLs: sqls(
				"SELECT percentile(s1, 50) AS avg_rows FROM percentile_test",
			),
			ExpErr: "integer, decimal or timestamp expression expected",
		},
		{
			SQLs: sqls(
				"SELECT percentile(i1, 50) AS p_rows FROM percentile_test",
			),
			ExpHdrs: hdrs(
				hdr("p_rows", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(12)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"SELECT percentile(d1, 50) AS p_rows FROM percentile_test",
			),
			ExpHdrs: hdrs(
				hdr("p_rows", fldTypeDecimal2),
			),
			ExpRows: rows(
				row(pql.NewDecimal(1000, 2)),
			),
			Compare: CompareExactUnordered,
		},
	},
}

var minmaxTests = TableTest{
	Table: tbl(
		"minmax_test",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("i1", fldTypeInt, "min 0", "max 1000"),
			srcHdr("d1", fldTypeDecimal2),
			srcHdr("s1", fldTypeString),
			srcHdr("ts1", fldTypeTimestamp),
		),
		srcRows(
			srcRow(int64(1), int64(10), float64(10), string("afoo"), timestampFromString("2013-07-15T01:18:46Z")),
			srcRow(int64(2), int64(10), float64(10), string("bfoo"), timestampFromString("2014-07-15T01:18:46Z")),
			srcRow(int64(3), int64(11), float64(11), string("cfoo"), timestampFromString("2015-07-15T01:18:46Z")),
			srcRow(int64(4), int64(12), float64(12), string("dfoo"), timestampFromString("2016-07-15T01:18:46Z")),
			srcRow(int64(5), int64(12), float64(12), string("efoo"), timestampFromString("2017-07-15T01:18:46Z")),
			srcRow(int64(6), int64(13), float64(13), string("ffoo"), timestampFromString("2018-07-15T01:18:46Z")),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"SELECT min(*) AS p_rows FROM minmax_test",
				"SELECT max(*) AS p_rows FROM minmax_test",
			),
			ExpErr: "column reference expected",
		},
		{
			SQLs: sqls(
				"SELECT min(i1, d1) AS p_rows FROM minmax_test",
				"SELECT max(i1, d1) AS p_rows FROM minmax_test",
			),
			ExpErr: "count of formal parameters (1) does not match count of actual parameters (2)",
		},
		{
			SQLs: sqls(
				"SELECT min(1) AS p_rows FROM minmax_test",
				"SELECT max(1) AS p_rows FROM minmax_test",
			),
			ExpHdrs: hdrs(
				hdr("p_rows", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(1)),
			),
			Compare: CompareExactUnordered},
		{
			SQLs: sqls(
				"SELECT min(_id) AS p_rows FROM minmax_test",
				"SELECT max(_id) AS p_rows FROM minmax_test",
			),
			ExpErr: "_id column cannot be used in aggregate function",
		},
		{
			SQLs: sqls(
				"SELECT min(s1) AS p_rows FROM minmax_test",
			),
			ExpHdrs: hdrs(
				hdr("p_rows", fldTypeString),
			),
			ExpRows: rows(
				row(string("afoo")),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"SELECT max(s1) AS p_rows FROM minmax_test",
			),
			ExpHdrs: hdrs(
				hdr("p_rows", fldTypeString),
			),
			ExpRows: rows(
				row(string("ffoo")),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"SELECT min(len(s1)) AS p_rows FROM minmax_test",
			),
			ExpHdrs: hdrs(
				hdr("p_rows", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(4)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"SELECT max(len(s1)) AS p_rows FROM minmax_test",
			),
			ExpHdrs: hdrs(
				hdr("p_rows", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(4)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"SELECT min(i1) AS p_rows FROM minmax_test",
			),
			ExpHdrs: hdrs(
				hdr("p_rows", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(10)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"SELECT max(i1) AS p_rows FROM minmax_test",
			),
			ExpHdrs: hdrs(
				hdr("p_rows", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(13)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"SELECT min(d1) AS p_rows FROM minmax_test",
			),
			ExpHdrs: hdrs(
				hdr("p_rows", fldTypeDecimal2),
			),
			ExpRows: rows(
				row(pql.NewDecimal(1000, 2)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"SELECT max(d1) AS p_rows FROM minmax_test",
			),
			ExpHdrs: hdrs(
				hdr("p_rows", fldTypeDecimal2),
			),
			ExpRows: rows(
				row(pql.NewDecimal(1300, 2)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select min(ts1) as min_val, max(ts1) as max_val from minmax_test",
			),
			ExpHdrs: hdrs(
				hdr("min_val", fldTypeTimestamp),
				hdr("max_val", fldTypeTimestamp),
			),
			ExpRows: rows(
				row(timestampFromString("2013-07-15T01:18:46Z"), timestampFromString("2018-07-15T01:18:46Z")),
			),
			Compare: CompareExactUnordered,
		},
	},
}
