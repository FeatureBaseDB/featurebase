package main

import (
	"strings"
	"testing"

	"github.com/pilosa/pilosa/v2/pql"
	"github.com/shurcooL/go-goon"
)

/*
sqlite db this test validated this against
create table bits (field ,row , column , timestamp);
           create table λbsi (field string,column bigint , val bigint);
          CREATE VIEW columns as
            select distinct column from λbits
            UNION
					  select distinct column from λbsi;

	         INSERT INTO λbits VALUES( 'color','red',1);
          INSERT INTO λbits VALUES( 'color','red',2);
          INSERT INTO λbits VALUES( 'color','red',3);
          INSERT INTO λbits VALUES( 'color','red',4);
          INSERT INTO λbits VALUES( 'color','red',5);
          INSERT INTO λbits VALUES( 'color','green',2);
          INSERT INTO λbits VALUES( 'color','green',4);
          INSERT INTO λbits VALUES( 'color','green',6);
          INSERT INTO λbits VALUES( 'color','green',8);
         INSERT INTO λbits VALUES( 'color','yellow',1);
          INSERT INTO λbits VALUES( 'color','yellow',3);
          INSERT INTO λbits VALUES( 'color','yellow',5);
          INSERT INTO λbits VALUES( 'color','yellow',7);
          INSERT INTO λbits VALUES( 'color','orange',5);
          INSERT INTO λbits VALUES( 'color','orange',6);
          INSERT INTO λbits VALUES( 'color','orange',7);
          INSERT INTO λbits VALUES( 'color','orange',8);

          INSERT INTO λbits VALUES( 'rating',1,8);
          INSERT INTO λbits VALUES( 'rating',1,7);
          INSERT INTO λbits VALUES( 'rating',1,10);
          INSERT INTO λbits VALUES( 'rating',2,6);
          INSERT INTO λbits VALUES( 'rating',2,5);
          INSERT INTO λbits VALUES( 'rating',3,4);
          INSERT INTO λbits VALUES( 'rating',4,3);
          INSERT INTO λbits VALUES( 'rating',4,2);

          INSERT INTO λbsi VALUES( 'luminosity',1, 1);
          INSERT INTO λbsi VALUES( 'luminosity',2, 2);
          INSERT INTO λbsi VALUES( 'luminosity',3, 4);
          INSERT INTO λbsi VALUES( 'luminosity',4, 5);
          INSERT INTO λbsi VALUES( 'luminosity',5, 4);
          INSERT INTO λbsi VALUES( 'luminosity',6, 3);
          INSERT INTO λbsi VALUES( 'luminosity',7, 2);
          INSERT INTO λbsi VALUES( 'luminosity',8, 1);
*/
func GetTests() []struct {
	Pql string
	Sql string
} {
	return []struct {
		Pql string
		Sql string
	}{
		{
			Pql: "Row(rating=0)",
			Sql: `select distinct column from λbits where field="rating" AND row="0"`,
		},
		{
			Pql: "Row(color=red)",
			Sql: `select distinct column from λbits where field="color" AND row="red"`,
		},
		{
			Pql: "Row(luminosity>2)",
			Sql: `select distinct column from λbsi where field="luminosity" and val>2`,
		},
		{
			Pql: "Row(luminosity<2)",
			Sql: `select distinct column from λbsi where field="luminosity" and val<2`,
		},
		{
			Pql: "Row(luminosity==4)",
			Sql: `select distinct column from λbsi where field="luminosity" and val=4`,
		},
		{
			Pql: "Row(luminosity<=4)",
			Sql: `select distinct column from λbsi where field="luminosity" and val<=4`,
		},
		{
			Pql: "Row(luminosity>=4)",
			Sql: `select distinct column from λbsi where field="luminosity" and val>=4`,
		},
		{
			Pql: "Row(luminosity!=4)",
			Sql: `select distinct column from λbsi where field="luminosity" and val!=4`,
		},
		{
			Pql: "Row(2<=luminosity<=4)",
			Sql: `select distinct column from λbsi where field="luminosity" and val>=2 and val<=4`,
		},
		{
			Pql: "Row(2<luminosity<=4)",
			Sql: `select distinct column from λbsi where field="luminosity" and val>2 and val<=4`,
		},
		{
			Pql: "Row(2<luminosity<4)",
			Sql: `select distinct column from λbsi where field="luminosity" and val>2 and val<4`,
		},
		{
			Pql: `Row(event=1, from='2021-02-01T00:00', to='2021-02-05T03:00')`,
			Sql: `select distinct column from λbits where field="event" AND row="1" AND timestamp >= "2021-02-01 00:00" AND timestamp < "2021-02-05 03:00"`,
		},
		{
			Pql: "Count(row(color=red))",
			Sql: `select count(*) from( select distinct column from λbits where field="color" AND row="red" )`,
		},
		{
			Pql: `Intersect(Row(color=red),Row(color=green))`,
			Sql: `select column from(select distinct column from λbits where field="color" AND row="red" intersect select distinct column from λbits where field="color" AND row="green")`,
		},
		{
			Pql: "Count(Union(Row(color=red),Row(color=green)))",
			Sql: `select count(*) from( select column from(select distinct column from λbits where field="color" AND row="red" union select distinct column from λbits where field="color" AND row="green") )`,
		},
		{
			Pql: "Union(Row(color=red),Row(color=green))",
			Sql: `select column from(select distinct column from λbits where field="color" AND row="red" union select distinct column from λbits where field="color" AND row="green")`,
		},
		{
			Pql: "Difference(Row(color=red),Row(color=green))",
			Sql: `select column from(select distinct column from λbits where field="color" AND row="red" except select distinct column from λbits where field="color" AND row="green")`,
		},
		{
			Pql: `All()`,
			Sql: `select column from λcolumns`,
		},
		{
			Pql: "Not(Row(color=red))",
			Sql: `select column from λcolumns except select distinct column from λbits where field="color" AND row="red"`,
		},
		{
			Pql: "Xor(Row(color=red),Row(color=green))",
			Sql: `select column from(select distinct column from λbits where field="color" AND row="red" union select distinct column from λbits where field="color" AND row="green") except select column from(select distinct column from λbits where field="color" AND row="red" intersect select distinct column from λbits where field="color" AND row="green")`,
		},
		{
			Pql: "Distinct(field=luminosity)",
			Sql: `select distinct val from λbsi where field="luminosity"`,
		},
		{
			Pql: "Distinct(Row(color=red),field=luminosity)",
			Sql: `select distinct val from λbsi where field="luminosity" AND column in (select distinct column from λbits where field="color" AND row="red")`,
		},
		{
			Pql: "Distinct(Intersect(Row(color=red),Row(color=green)),field=luminosity)",
			Sql: `select distinct val from λbsi where field="luminosity" AND column in (select column from(select distinct column from λbits where field="color" AND row="red" intersect select distinct column from λbits where field="color" AND row="green"))`,
		},
		{
			Pql: "Rows(color)",
			Sql: `select distinct field, row from λbits where field="color" order by row`,
		},
		{
			Pql: "Rows(color, limit=10)",
			Sql: `select distinct field, row from λbits where field="color" order by row limit 10`,
		},
		{
			Pql: "Rows(color, column=1)",
			Sql: `select distinct field, row from λbits where field="color" and column='1' order by row`,
		},
		//		{ // TODO (twg) Not supported by this test, can't reliably get key ordering
		//		Pql: "Rows(color, previous=green )",
		//	    Sql: `select distinct field, row from λbits where field="color" and row>'green' order by row`,
		//		},
		{
			Pql: "GroupBy(Rows(color))",
			Sql: `select  f1.field, f1.row,count(*) as cnt from λbits f1 where f1.field='color' group by f1.field,f1.row order by cnt desc`,
		},
		{
			Pql: "GroupBy(Rows(color),Rows(rating))",
			Sql: `select  f1.field, f1.row,f2.field, f2.row,count(*) as cnt from λbits f1 inner join λbits f2 on f1.column = f2.column where f1.field='color'and f2.field='rating' group by f1.field,f1.row,f2.field,f2.row order by cnt desc`,
		},
		{
			Pql: "TopN(color)",
			Sql: `select row,count(*) as cnt from λbits where field="color" group by row order by cnt desc`,
		},
		{
			Pql: "TopN(color,n=2)",
			Sql: `select row,count(*) as cnt from λbits where field="color" group by row order by cnt desc limit 2`,
		},
		{
			Pql: "TopN(color,Row(color=red),n=2)",
			Sql: `select row,count(*) as cnt from λbits where field="color" and column in (select distinct column from λbits where field="color" AND row="red") group by row order by cnt desc limit 2`,
		},
		{
			Pql: "Topk(color)",
			Sql: `select row,count(*) as cnt from λbits where field="color" group by row order by cnt desc`,
		},
		{
			Pql: "TopK(color, k=2, filter=Row(rating=1))",
			Sql: `select row,count(*) as cnt from λbits where field="color" and column in (select distinct column from λbits where field="rating" AND row="1") group by row order by cnt desc limit 2`,
		},
		{
			Pql: `Min(field="luminosity")`,
			Sql: `select val,count(*) from λbsi where val=(select min(val) from λbsi where field="luminosity")`,
		},
		{
			Pql: `Max(field="luminosity")`,
			Sql: `select val,count(*) from λbsi where val=(select max(val) from λbsi where field="luminosity")`,
		},
		{
			Pql: `Min(Row(color=orange),field="luminosity")`,
			Sql: `select val,count(*) from λbsi where val=(select min(val) from λbsi where field="luminosity" and column in (select distinct column from λbits where field="color" AND row="orange")) and column in (select distinct column from λbits where field="color" AND row="orange")`,
		},
		{
			Pql: `Min(Intersect(Row(color=red),Row(color=orange)),field="luminosity")`,
			Sql: `select val,count(*) from λbsi where val=(select min(val) from λbsi where field="luminosity" and column in (select column from(select distinct column from λbits where field="color" AND row="red" intersect select distinct column from λbits where field="color" AND row="orange"))) and column in (select column from(select distinct column from λbits where field="color" AND row="red" intersect select distinct column from λbits where field="color" AND row="orange"))`,
		},
		{
			Pql: `Sum(field="luminosity")`,
			Sql: `select sum(val),count(*) from λbsi where field="luminosity"`,
		},
		{
			Pql: `ConstRow(columns=[1,3,5])`,
			Sql: `select column1 as column from (values (1),(3),(5))`,
		},
		{
			Pql: `UnionRows(Rows(color))`,
			Sql: `select distinct b.column from (select field,row from(select distinct field, row from λbits where field="color" order by row)) sq inner join λbits b on sq.field = b.field and sq.row = b.row`,
		},
		{
			Pql: `UnionRows(Rows(color),Rows(rating))`,
			Sql: `select distinct b.column from (select field,row from(select distinct field, row from λbits where field="color" order by row)union all select field,row from (select distinct field, row from λbits where field="rating" order by row)) sq inner join λbits b on sq.field = b.field and sq.row = b.row`,
		},
		{
			Pql: `IncludesColumn(Row(color=red), column=1)`,
			Sql: `select count(*)>0 from (select column from(select distinct column from λbits where field="color" AND row="red") where column='1')`,
		},
		{
			Pql: `Extract(Row(color=red),Rows(color),Rows(rating))`,
			Sql: `select sq.field, sq.row, b.column from (select field,row from(select distinct field, row from λbits where field="color" order by row)union all select field,row from (select distinct field, row from λbits where field="rating" order by row)) sq inner join λbits b on sq.field = b.field and sq.row = b.row where b.column in (select distinct column from λbits where field="color" AND row="red")`,
		},
		{
			Pql: `Extract(ConstRow(columns=[1,2,3]))`,
			Sql: `select '' as field, '' as row,column from (select column1 as column from (values (1),(2),(3)))`,
		},
	}
}
func TestParse(t *testing.T) {

	for _, tst := range GetTests() {

		q, err := pql.NewParser(strings.NewReader(tst.Pql)).Parse()
		panicOn(err)
		sql, _, _, err := ToSql(q.Calls, "")
		panicOn(err)
		if sql != tst.Sql {
			vv("\npql:%v\ngenerated:\n%v\nexpected:\n%v\n", tst.Pql, sql, tst.Sql)
			goon.Dump(q)
		}

	}
}
