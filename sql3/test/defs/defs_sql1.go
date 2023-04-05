package defs

import (
	"fmt"
	"time"
)

var sql1TestsGrouper = TableTest{
	name: "sql1testsgrouper",
	Table: tbl(
		"grouper",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("color", fldTypeString),
			srcHdr("score", fldTypeInt, "min -1000", "max 1000"),
			srcHdr("age", fldTypeInt, "min 0", "max 100"),
			srcHdr("height", fldTypeInt, "min 0", "max 1000"),
			srcHdr("timestamp", fldTypeTimestamp),
		),
		srcRows(
			srcRow(int64(1), string("blue"), int64(-10), int64(27), int64(20), string("2011-04-02T12:32:00Z")),
			srcRow(int64(2), string("blue"), int64(-8), int64(16), int64(30), string("2011-01-02T12:32:00Z")),
			srcRow(int64(3), string("red"), int64(6), int64(19), int64(40), string("2012-01-02T12:32:00Z")),
			srcRow(int64(4), string("green"), int64(0), int64(27), int64(50), string("2013-09-02T12:32:00Z")),
			srcRow(int64(5), string("blue"), int64(-2), int64(16), int64(60), string("2014-01-02T12:32:00Z")),
			srcRow(int64(6), string("blue"), int64(100), int64(34), int64(70), string("2010-05-02T12:32:00Z")),
			srcRow(int64(7), string("blue"), int64(0), int64(27), int64(80), string("2016-08-02T12:32:00Z")),
			srcRow(int64(8), nil, int64(-13), int64(16), int64(90), string("2020-01-02T12:32:00Z")),
			srcRow(int64(9), string("red"), int64(80), int64(16), int64(100), string("2000-03-02T12:32:00Z")),
			srcRow(int64(10), string("red"), int64(-2), int64(31), int64(110), string("2018-01-02T12:32:00Z")),
		),
	),
	SQLTests: nil,
}

var sql1TestsJoiner = TableTest{
	name: "sql1testsjoiner",
	Table: tbl(
		"joiner",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("grouperid", fldTypeInt, "min 0", "max 1000"),
			srcHdr("jointype", fldTypeInt, "min -1000", "max 1000"),
		),
		srcRows(
			srcRow(int64(1), int64(1), int64(1)),
			srcRow(int64(2), int64(2), int64(1)),
			srcRow(int64(3), int64(5), int64(1)),
			srcRow(int64(4), int64(6), int64(1)),
			srcRow(int64(5), int64(7), int64(1)),
			srcRow(int64(6), int64(3), int64(2)),
			srcRow(int64(7), int64(8), int64(2)),
			srcRow(int64(8), int64(9), int64(2)),
			srcRow(int64(9), int64(1), int64(3)),
			srcRow(int64(10), int64(2), int64(3)),
		),
	),
	SQLTests: nil,
}

var sql1TestsDelete = TableTest{
	name: "sql1testsdelete",
	Table: tbl(
		"delete_me",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("unused", fldTypeInt),
		),
		srcRows(srcRow(int64(1), int64(1))),
	),
	SQLTests: nil,
}

// grouperTimeX extracts the time associated with record ID x.
// note that the record IDs are 1..10, not 0..9, so we subtract one.
func grouperTimeX(x int) time.Time {
	t, err := time.ParseInLocation(time.RFC3339, sql1TestsGrouper.Table.rows[0][x-1][5].(string), time.UTC)
	if err != nil {
		panic(fmt.Sprintf("failed to parse time for id %d", x))
	}
	return t
}

var sql1TestsQueries = TableTest{
	name: "sql1testsqueries",
	SQLTests: []SQLTest{
		{
			// Extract(Limit(All(), limit=100, offset=0),Rows(age))
			SQLs: sqls("select age from grouper;"),
			ExpHdrs: hdrs(
				hdr("age", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(27)),
				row(int64(16)),
				row(int64(19)),
				row(int64(27)),
				row(int64(16)),
				row(int64(34)),
				row(int64(27)),
				row(int64(16)),
				row(int64(16)),
				row(int64(31)),
			),
			Compare: CompareExactOrdered,
		},
		{
			// Extract(Limit(ConstRow(columns=[2]), limit=100, offset=0),Rows(age),Rows(color),Rows(height),Rows(score))
			SQLs: sqls("select * from grouper where _id=2;"),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("age", fldTypeInt),
				hdr("color", fldTypeString),
				hdr("height", fldTypeInt),
				hdr("score", fldTypeInt),
				hdr("timestamp", fldTypeTimestamp),
			),
			ExpRows: rows(
				row(int64(2), int64(16), string("blue"), int64(30), int64(-8), grouperTimeX(2)),
			),
			Compare: CompareExactOrdered,
		},
		{
			// Extract(Limit(ConstRow(columns=[2]), limit=100, offset=0),Rows(age),Rows(color),Rows(height),Rows(score))
			SQLs: sqls("select * from grouper;"),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("age", fldTypeInt),
				hdr("color", fldTypeString),
				hdr("height", fldTypeInt),
				hdr("score", fldTypeInt),
				hdr("timestamp", fldTypeTimestamp),
			),
			ExpRows: rows(
				row(int64(1), int64(27), string("blue"), int64(20), int64(-10), grouperTimeX(0+1)),
				row(int64(2), int64(16), string("blue"), int64(30), int64(-8), grouperTimeX(1+1)),
				row(int64(3), int64(19), string("red"), int64(40), int64(6), grouperTimeX(2+1)),
				row(int64(4), int64(27), string("green"), int64(50), int64(0), grouperTimeX(3+1)),
				row(int64(5), int64(16), string("blue"), int64(60), int64(-2), grouperTimeX(4+1)),
				row(int64(6), int64(34), string("blue"), int64(70), int64(100), grouperTimeX(5+1)),
				row(int64(7), int64(27), string("blue"), int64(80), int64(0), grouperTimeX(6+1)),
				row(int64(8), int64(16), nil, int64(90), int64(-13), grouperTimeX(7+1)),
				row(int64(9), int64(16), string("red"), int64(100), int64(80), grouperTimeX(8+1)),
				row(int64(10), int64(31), string("red"), int64(110), int64(-2), grouperTimeX(9+1)),
			),
			Compare: CompareExactOrdered,
		},
		// join
		{
			// Count(Intersect(All(),Distinct(Row(grouperid!=null),index='joiner',field='grouperid')))
			SQLs: sqls("select count(*) from grouper g INNER JOIN joiner j ON g._id = j.grouperid;"),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(10)),
			),
			Compare: CompareExactOrdered,
		},
		{
			// Intersect(All(),Distinct(Row(grouperid!=null),index='joiner',field='grouperid'))
			SQLs:    sqls("select distinct _id from grouper g INNER JOIN joiner j ON g._id = j.grouperid;"),
			ExpHdrs: hdrs(hdr("_id", fldTypeID)),
			ExpRows: rows(
				row(int64(1)),
				row(int64(2)),
				row(int64(3)),
				row(int64(5)),
				row(int64(6)),
				row(int64(7)),
				row(int64(8)),
				row(int64(9)),
			),
			Compare: CompareExactUnordered,
		},
		{
			// Intersect(Row(color='red'),Distinct(Row(grouperid!=null),index='joiner',field='grouperid'))
			SQLs:    sqls("select _id from grouper g INNER JOIN joiner j ON g._id = j.grouperid where g.color = 'red';"),
			ExpHdrs: hdrs(hdr("_id", fldTypeID)),
			ExpRows: rows(
				row(int64(3)),
				row(int64(9)),
			),
			Compare: CompareExactUnordered,
		},
		{
			// Intersect(Row(color='red'),Distinct(Row(jointype=2),index='joiner',field='grouperid'))
			SQLs:    sqls("select _id from grouper g INNER JOIN joiner j ON g._id = j.grouperid where g.color = 'red' and j.jointype = 2;"),
			ExpHdrs: hdrs(hdr("_id", fldTypeID)),
			ExpRows: rows(
				row(int64(3)),
				row(int64(9)),
			),
			Compare: CompareExactUnordered,
		},
		// order by
		{
			// Distinct(Row(score!=null),index='grouper',field='score')
			SQLs:    sqls("select distinct score from grouper order by score asc;"),
			ExpHdrs: hdrs(hdr("score", fldTypeInt)),
			ExpRows: rows(
				row(int64(-13)),
				row(int64(-10)),
				row(int64(-8)),
				row(int64(-2)),
				row(int64(0)),
				row(int64(6)),
				row(int64(80)),
				row(int64(100)),
			),
			Compare: CompareExactOrdered,
		},
		{
			// Distinct(Row(score!=null),index='grouper',field='score')
			SQLs:    sqls("select distinct score from grouper order by score desc;"),
			ExpHdrs: hdrs(hdr("score", fldTypeInt)),
			ExpRows: rows(
				row(int64(100)),
				row(int64(80)),
				row(int64(6)),
				row(int64(0)),
				row(int64(-2)),
				row(int64(-8)),
				row(int64(-10)),
				row(int64(-13)),
			),
			Compare: CompareExactOrdered,
		},
		// {
		// 	// Distinct(Row(score!=null),index='grouper',field='score')
		// 	SQLs:    sqls("select distinct score from grouper order by score asc limit 5;"),
		// 	ExpHdrs: hdrs(hdr("score", fldTypeInt)),
		// 	ExpRows: rows(
		// 		row(int64(-13)),
		// 		row(int64(-10)),
		// 		row(int64(-8)),
		// 		row(int64(-2)),
		// 		row(int64(0)),
		// 	),
		// 	Compare: CompareExactOrdered,
		// },
		// {
		// 	// Distinct(Row(score!=null),index='grouper',field='score')
		// 	SQLs:    sqls("select distinct score from grouper order by score desc limit 5;"),
		// 	ExpHdrs: hdrs(hdr("score", fldTypeInt)),
		// 	ExpRows: rows(
		// 		row(int64(100)),
		// 		row(int64(80)),
		// 		row(int64(6)),
		// 		row(int64(0)),
		// 		row(int64(-2)),
		// 	),
		// 	Compare: CompareExactOrdered,
		// },
		// distinct
		{
			// Distinct(Row(score!=null),index='grouper',field='score')
			SQLs:    sqls("select distinct score from grouper;"),
			ExpHdrs: hdrs(hdr("score", fldTypeInt)),
			ExpRows: rows(
				row(int64(-13)),
				row(int64(-10)),
				row(int64(-8)),
				row(int64(-2)),
				row(int64(0)),
				row(int64(6)),
				row(int64(80)),
				row(int64(100)),
			),
			Compare: CompareExactUnordered,
		},
		{
			// Distinct(Row(height!=null),index='grouper',field='height')
			SQLs:    sqls("select distinct height from grouper;"),
			ExpHdrs: hdrs(hdr("height", fldTypeInt)),
			ExpRows: rows(
				row(int64(20)),
				row(int64(30)),
				row(int64(40)),
				row(int64(50)),
				row(int64(60)),
				row(int64(70)),
				row(int64(80)),
				row(int64(90)),
				row(int64(100)),
				row(int64(110)),
			),
			Compare: CompareExactUnordered,
		},
		// groupby
		{
			// GroupBy(Rows(field='age'),limit=100)
			SQLs: sqls("select age as yrs, count(*) as cnt from grouper group by age;"),
			ExpHdrs: hdrs(
				hdr("yrs", fldTypeInt),
				hdr("cnt", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(16), int64(4)),
				row(int64(19), int64(1)),
				row(int64(27), int64(3)),
				row(int64(31), int64(1)),
				row(int64(34), int64(1)),
			),
			Compare: CompareExactUnordered,
		},
		// {
		// 	// GroupBy(Rows(field='age'),Rows(field='color'),limit=100)
		// 	SQLs: sqls("select age, color, count(*) from grouper group by age, color;"),
		// 	ExpHdrs: hdrs(
		// 		hdr("age", fldTypeInt),
		// 		hdr("color", fldTypeString),
		// 		hdr("", fldTypeInt),
		// 	),
		// 	ExpRows: rows(
		// 		row(int64(16), "blue", int64(2)),
		// 		row(int64(16), "red", int64(1)),
		// 		row(int64(19), "red", int64(1)),
		// 		row(int64(27), "blue", int64(2)),
		// 		row(int64(27), "green", int64(1)),
		// 		row(int64(31), "red", int64(1)),
		// 		row(int64(34), "blue", int64(1)),
		// 	),
		// 	Compare: CompareExactUnordered,
		// },
		// {
		// 	// GroupBy(Rows(field='age'),Rows(field='color'),limit=100,filter=Row(age=27),aggregate=Sum(field='height'))
		// 	SQLs: sqls("select age, color, sum(height) from grouper where age = 27 group by age, color;"),
		// 	ExpHdrs: hdrs(
		// 		hdr("age", fldTypeInt),
		// 		hdr("color", fldTypeString),
		// 		hdr("", fldTypeInt),
		// 	),
		// 	ExpRows: rows(
		// 		row(int64(27), "blue", int64(100)),
		// 		row(int64(27), "green", int64(50)),
		// 	),
		// 	Compare: CompareExactUnordered,
		// },
		// {
		// 	// GroupBy(Rows(field='age'),limit=100,having=Condition(count>1))
		// 	SQLs: sqls("select age, count(*) as cnt from grouper group by age having count(*) > 1;"),
		// 	ExpHdrs: hdrs(
		// 		hdr("age", fldTypeInt),
		// 		hdr("", fldTypeInt),
		// 	),
		// 	ExpRows: rows(
		// 		row(int64(16), int64(4)),
		// 		row(int64(27), int64(3)),
		// 	),
		// 	Compare: CompareExactUnordered,
		// },
		// {
		// 	// GroupBy(Rows(field='age'),limit=100,having=Condition(1<=count<=3))
		// 	SQLs: sqls("select age, count(*) from grouper group by age having count(*) between 1 and 3;"),
		// 	ExpHdrs: hdrs(
		// 		hdr("age", fldTypeInt),
		// 		hdr("", fldTypeInt),
		// 	),
		// 	ExpRows: rows(
		// 		row(int64(19), int64(1)),
		// 		row(int64(27), int64(3)),
		// 		row(int64(31), int64(1)),
		// 		row(int64(34), int64(1)),
		// 	),
		// 	Compare: CompareExactUnordered,
		// },
		// {
		// 	// GroupBy(Rows(field='age'),limit=3)
		// 	SQLs: sqls("select age, count(*) as cnt from grouper group by age order by cnt desc, age desc limit 3;"),
		// 	ExpHdrs: hdrs(
		// 		hdr("age", fldTypeInt),
		// 		hdr("cnt", fldTypeInt),
		// 	),
		// 	ExpRows: rows(
		// 		row(int64(16), int64(4)),
		// 		row(int64(27), int64(3)),
		// 		row(int64(19), int64(1)),
		// 	),
		// 	Compare: CompareExactOrdered,
		// },
		{
			// GroupBy(Rows(field='age'),Rows(field='height'),filter=Intersect(Row(timestamp>"2017-09-02T12:32:00Z"),Row(height>40)))
			SQLs: sqls("select age, height from grouper where timestamp > '2017-09-02T12:32:00Z' and height > 40 group by age, height;"),
			ExpHdrs: hdrs(
				hdr("age", fldTypeInt),
				hdr("height", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(16), int64(90)),
				row(int64(31), int64(110)),
			),
			Compare: CompareExactUnordered,
		},
		{
			// Extract(Union(Row(timestamp>"2017-09-02T12:32:00Z"),Row(height>90)),Rows(age), Rows(height))
			SQLs: sqls("select age, height from grouper where timestamp > '2017-09-02T12:32:00Z' or height > 90;"),
			ExpHdrs: hdrs(
				hdr("age", fldTypeInt),
				hdr("height", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(16), int64(90)),
				row(int64(16), int64(100)),
				row(int64(31), int64(110)),
			),
			Compare: CompareExactUnordered,
		},
		{
			//Extract(Intersect(Row(timestamp>"2017-09-02T12:32:00Z"),Row(timestamp<"2019-09-02T12:32:00Z")),Rows(age), Rows(height))
			SQLs: sqls("select age, height from grouper where timestamp > '2017-09-02T12:32:00Z' and timestamp < '2019-09-02T12:32:00Z';"),
			ExpHdrs: hdrs(
				hdr("age", fldTypeInt),
				hdr("height", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(31), int64(110)),
			),
			Compare: CompareExactUnordered,
		},
		{
			//Extract(Intersect(Row(timestamp>"2017-09-02T12:32:00Z"),Row(timestamp<"2019-09-02T12:32:00Z")),Rows(age), Rows(height))
			//Testing the parenthesis around where clause
			SQLs: sqls("select age, height from grouper where (timestamp > '2017-09-02T12:32:00Z' and timestamp < '2019-09-02T12:32:00Z');"),
			ExpHdrs: hdrs(
				hdr("age", fldTypeInt),
				hdr("height", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(31), int64(110)),
			),
			Compare: CompareExactUnordered,
		},
		{
			//Testing empty parenthesis around where clause
			SQLs:   sqls("select age, height from grouper where ();"),
			ExpErr: "expected expression, found",
		},
		{
			//Distinct(Row(timestamp>"2019-09-02T12:32:00Z"), index='grouper',field='age')
			SQLs: sqls("select distinct age from grouper where timestamp > '2019-09-02T12:32:00Z';"),
			ExpHdrs: hdrs(
				hdr("age", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(16)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls("show tables;"),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeString),
				hdr("name", fldTypeString),
				hdr("owner", fldTypeString),
				hdr("updated_by", fldTypeString),
				hdr("created_at", fldTypeTimestamp),
				hdr("updated_at", fldTypeTimestamp),
				hdr("keys", fldTypeBool),
				hdr("space_used", fldTypeInt),
				hdr("description", fldTypeString),
			),
			ExpRows: rows(
				row(nil, "grouper"),
				row(nil, "joiner"),
				row(nil, "delete_me"),
			),
			Compare: ComparePartial,
		},
		{
			SQLs: sqls("show columns from grouper;"),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeString),
				hdr("name", fldTypeString),
				hdr("type", fldTypeString),
				hdr("created_at", fldTypeTimestamp),
				hdr("keys", fldTypeBool),
				hdr("cache_type", fldTypeString),
				hdr("cache_size", fldTypeInt),
				hdr("scale", fldTypeInt),
				hdr("length", fldTypeInt),
				hdr("min", fldTypeInt),
				hdr("max", fldTypeInt),
				hdr("timeunit", fldTypeString),
				hdr("epoch", fldTypeInt),
				hdr("timequantum", fldTypeString),
				hdr("ttl", fldTypeString),
			),
			ExpRows: rows(
				row(nil, string("age"), string("int")),
				row(nil, string("color"), string("string")),
				row(nil, string("height"), string("int")),
				row(nil, string("score"), string("int")),
				row(nil, string("timestamp"), string("timestamp")),
			),
			Compare: ComparePartial,
		},
		// {
		// 	SQLs:    sqls("drop table delete_me;"),
		// 	ExpHdrs: hdrs(),
		// 	ExpRows: rows(),
		// 	Compare: CompareExactOrdered,
		// },
		// The following cases test different paths within the `case *sqlparser.AndExpr`
		// of extract.go by providing different WHERE conditions.
		{
			// len(left) == 2 && len(right) == 1
			// right[0].table == left[0].table
			SQLs:    sqls("select _id from grouper g INNER JOIN joiner j ON g._id = j.grouperid where g.color = 'red' and j.jointype = 2 and g.age = 16;"),
			ExpHdrs: hdrs(hdr("_id", fldTypeID)),
			ExpRows: rows(
				row(int64(9)),
			),
			Compare: CompareExactUnordered,
		},
		{
			// len(left) == 2 && len(right) == 1
			// right[0].table == left[1].table {
			SQLs:    sqls("select _id from grouper g INNER JOIN joiner j ON g._id = j.grouperid where j.jointype = 2 and g.color = 'red' and g.age = 16;"),
			ExpHdrs: hdrs(hdr("_id", fldTypeID)),
			ExpRows: rows(
				row(int64(9)),
			),
			Compare: CompareExactUnordered,
		},
		{
			// len(left) == 1 && len(right) == 1 && left[0].table != right[0].table
			SQLs:    sqls("select _id from grouper g INNER JOIN joiner j ON g._id = j.grouperid where g.color = 'red' and g.age = 16 and j.jointype = 2;"),
			ExpHdrs: hdrs(hdr("_id", fldTypeID)),
			ExpRows: rows(
				row(int64(9)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs:   sqls("select * from index_not_found;"),
			ExpErr: "table or view 'index_not_found' not found",
		},
		{
			SQLs:   sqls("select field_not_found from grouper;"),
			ExpErr: "column 'field_not_found' not found",
		},
		{
			SQLs:   sqls("select * from grouper, index_not_found;"),
			ExpErr: "table or view 'index_not_found' not found",
		},
		{
			SQLs:   sqls("select _id, age, field_not_found from grouper;"),
			ExpErr: "column 'field_not_found' not found",
		},
		{
			SQLs:   sqls("select age, color, count(*) from grouper group by field_not_found, age, color;"),
			ExpErr: "column 'field_not_found' not found",
		},
		{
			SQLs:   sqls("select count(*) from grouper inner join joiner on grouper._id = joiner.field_not_found;"),
			ExpErr: "column 'field_not_found' not found",
		},
	},
}
