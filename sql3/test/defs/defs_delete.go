// Copyright 2021 Molecula Corp. All rights reserved.
package defs

import (
	"time"

	"github.com/featurebasedb/featurebase/v3/sql3/parser"
)

func earlyMay2022() time.Time {
	tm, err := parser.ConvertStringToTimestamp("2022-05-05T13:00:00+00:00")
	if err != nil {
		panic(err.Error())
	}
	return tm
}

func lateMay2022() time.Time {
	tm, err := parser.ConvertStringToTimestamp("2022-05-28T13:00:00+00:00")
	if err != nil {
		panic(err.Error())
	}
	return tm
}

// DELETE tests
var deleteTests = TableTest{
	name: "delete_tests",
	Table: tbl(
		"del_all_types",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("i1", fldTypeInt, "min 0", "max 1000"),
			srcHdr("b1", fldTypeBool),
			srcHdr("d1", fldTypeDecimal2),
			srcHdr("id1", fldTypeID),
			srcHdr("ids1", fldTypeIDSet),
			srcHdr("s1", fldTypeString),
			srcHdr("ss1", fldTypeStringSet),
			srcHdr("t1", fldTypeTimestamp),
		),
		srcRows(
			srcRow(int64(1), int64(1000), bool(true), float64(12.34), int64(20), []int64{101, 102}, string("foo"), []string{"101", "102"}, earlyMay2022()),
			srcRow(int64(2), int64(1000), bool(true), float64(12.34), int64(20), []int64{101, 102}, string("foo"), []string{"101", "102"}, earlyMay2022()),
			srcRow(int64(3), int64(1000), bool(true), float64(12.34), int64(20), []int64{101, 102}, string("foo"), []string{"101", "102"}, earlyMay2022()),
			srcRow(int64(4), int64(1000), bool(true), float64(12.34), int64(20), []int64{101, 102}, string("foo"), []string{"101", "102"}, earlyMay2022()),
			srcRow(int64(5), int64(1000), bool(true), float64(12.34), int64(20), []int64{101, 102}, string("foo"), []string{"101", "102"}, lateMay2022()),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"delete from del_all_types where _id = 1;",
			),
			ExpHdrs: hdrs(),
			ExpRows: rows(),
			Compare: CompareExactUnordered,
		},
		{
			// ordering is important here - this test validates the previous delete happened
			SQLs: sqls(
				"select _id from del_all_types where _id = 1;",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
			),
			ExpRows: rows(),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"delete from del_all_types where _id in (2, 3);",
			),
			ExpHdrs: hdrs(),
			ExpRows: rows(),
			Compare: CompareExactUnordered,
		},
		{
			// ordering is important here - this test validates the previous delete happened
			SQLs: sqls(
				"select _id from del_all_types where _id = 2 or _id = 3;",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
			),
			ExpRows: rows(),
			Compare: CompareExactUnordered,
		},

		// delete with in
		{
			SQLs: sqls(
				"create table sub_query (_id id, i1 int min 0 max 1000);",
			),
			ExpHdrs: hdrs(),
			ExpRows: rows(),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"insert into sub_query values (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6);",
			),
			ExpHdrs: hdrs(),
			ExpRows: rows(),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"delete from del_all_types where _id in (select _id from sub_query where i1 > 3) and i1 > 10;",
			),
			ExpHdrs: hdrs(),
			ExpRows: rows(),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id from del_all_types where _id > 4;",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
			),
			ExpRows: rows(),
			Compare: CompareExactUnordered,
		},
		// inner joins in delete are apparently not supported yet
		// when they are, this will fill in test coverage for expressionanalyzer.go
		/*
			{
				SQLs: sqls(
					"delete from del_all_types a1 inner join del_all_types a2 on a1._id=a2._id where a1._id in (select _id from del_all_types);",
				),
				ExpHdrs: hdrs(
					hdr("_id", fldTypeID),
				),
				ExpRows: rows(),
				Compare: CompareExactUnordered,
			},
			{
				SQLs: sqls(
					"select _id from del_all_types;",
				),
				ExpHdrs: hdrs(
					hdr("_id", fldTypeID),
				),
				ExpRows: rows(),
				Compare: CompareExactUnordered,
			},
		*/

		// dates
		{
			SQLs: sqls(
				`insert into del_all_types 
				 values
					(1,1000,true,12.34,20,[101,102],'foo',['101','102'],'2010-01-01T00:00:00Z'),
					(2,1000,true,12.34,20,[101,102],'foo',['101','102'],'2012-11-01T22:08:41Z'),
					(3,1000,true,12.34,20,[101,102],'foo',['101','102'],'2012-11-01T22:08:41Z'),
					(4,1000,true,12.34,20,[101,102],'foo',['101','102'],'2020-01-01T00:00:00Z');`,
			),
			ExpHdrs: hdrs(),
			ExpRows: rows(),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"delete from del_all_types where t1 > '2010-01-01T00:00:00Z';",
			),
			ExpHdrs: hdrs(),
			ExpRows: rows(),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id from del_all_types where _id > 1;",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
			),
			ExpRows: rows(),
			Compare: CompareExactUnordered,
		},

		// ints
		{
			SQLs: sqls(
				`insert into del_all_types 
				 values
					(1,100,true,12.34,20,[101,102],'foo',['101','102'],'2010-01-01T00:00:00Z'),
					(2,200,true,12.34,20,[101,102],'foo',['101','102'],'2012-11-01T22:08:41Z'),
					(3,300,true,12.34,20,[101,102],'foo',['101','102'],'2012-11-01T22:08:41Z'),
					(4,400,true,12.34,20,[101,102],'foo',['101','102'],'2020-01-01T00:00:00Z');`,
			),
			ExpHdrs: hdrs(),
			ExpRows: rows(),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"delete from del_all_types where i1 > 200;",
			),
			ExpHdrs: hdrs(),
			ExpRows: rows(),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id from del_all_types where i1 > 200;",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
			),
			ExpRows: rows(),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"delete from del_all_types where i1 < 300;",
			),
			ExpHdrs: hdrs(),
			ExpRows: rows(),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id from del_all_types;",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
			),
			ExpRows: rows(),
			Compare: CompareExactUnordered,
		},

		// bool
		{
			SQLs: sqls(
				`insert into del_all_types 
				 values
					(1,100,true,12.34,20,[101,102],'foo',['101','102'],'2010-01-01T00:00:00Z'),
					(2,200,true,12.34,20,[101,102],'foo',['101','102'],'2012-11-01T22:08:41Z'),
					(3,300,true,12.34,20,[101,102],'foo',['101','102'],'2012-11-01T22:08:41Z'),
					(4,400,true,12.34,20,[101,102],'foo',['101','102'],'2020-01-01T00:00:00Z');`,
			),
			ExpHdrs: hdrs(),
			ExpRows: rows(),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"delete from del_all_types where b1 = true;",
			),
			ExpHdrs: hdrs(),
			ExpRows: rows(),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id from del_all_types;",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
			),
			ExpRows: rows(),
			Compare: CompareExactUnordered,
		},

		// id sets
		{
			SQLs: sqls(
				`insert into del_all_types 
				 values
					(1,100,true,12.34,20,[101,102],'foo',['101','102'],'2010-01-01T00:00:00Z'),
					(2,200,true,12.34,20,[101,102],'foo',['101','102'],'2012-11-01T22:08:41Z'),
					(3,300,true,12.34,20,[101,102],'foo',['101','102'],'2012-11-01T22:08:41Z'),
					(4,400,true,12.34,20,[101,102,103],'foo',['101','102'],'2020-01-01T00:00:00Z');`,
			),
			ExpHdrs: hdrs(),
			ExpRows: rows(),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"delete from del_all_types where setcontains(ids1, 103);",
			),
			ExpHdrs: hdrs(),
			ExpRows: rows(),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id from del_all_types where _id = 4;",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
			),
			ExpRows: rows(),
			Compare: CompareExactUnordered,
		},

		// compound expressions
		{
			SQLs: sqls(
				`insert into del_all_types 
				values
					(1,100,true,12.34,20,[101,102],'foo',['101','102'],'2010-01-01T00:00:00Z'),
					(2,200,true,12.35,20,[101,102],'foo',['101','102'],'2012-11-01T22:08:41Z'),
					(3,300,true,12.36,20,[101,102],'foo',['101','102'],'2012-11-01T22:08:41Z'),
					(4,400,true,12.37,20,[101,102,103],'foo',['101','102'],'2020-01-01T00:00:00Z');`,
			),
			ExpHdrs: hdrs(),
			ExpRows: rows(),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"delete from del_all_types where d1 = 12.36 and i1 = 300;",
			),
			ExpHdrs: hdrs(),
			ExpRows: rows(),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id from del_all_types where _id = 3;",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
			),
			ExpRows: rows(),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"delete from del_all_types where d1 = 12.34 or i1 = 200;",
			),
			ExpHdrs: hdrs(),
			ExpRows: rows(),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id from del_all_types where _id = 1 or _id = 2;",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
			),
			ExpRows: rows(),
			Compare: CompareExactUnordered,
		},

		// scalar function filters
		{
			SQLs: sqls(
				`insert into del_all_types 
				 values
					(1,100,true,12.34,20,[101,102],'foo',['101','102'],'2010-01-01T00:00:00Z'),
					(2,200,true,12.35,20,[101,102],'bar',['101','102'],'2012-11-01T22:08:41Z'),
					(3,300,true,12.36,20,[101,102],'zoo',['101','102'],'2012-11-01T22:08:41Z'),
					(4,400,true,12.37,20,[101,102,103],'raz',['101','102','103'],'2020-01-01T00:00:00Z');`,
			),
			ExpHdrs: hdrs(),
			ExpRows: rows(),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"delete from del_all_types where substring(s1, 0, 1) = 'f';",
			),
			ExpHdrs: hdrs(),
			ExpRows: rows(),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id from del_all_types where _id = 1;",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
			),
			ExpRows: rows(),
			Compare: CompareExactUnordered,
		},

		// delete everything
		{
			SQLs: sqls(
				`insert into del_all_types 
				 values
					(1,100,true,12.34,20,[101,102],'foo',['101','102'],'2010-01-01T00:00:00Z'),
					(2,200,true,12.34,20,[101,102],'foo',['101','102'],'2012-11-01T22:08:41Z'),
					(3,300,true,12.34,20,[101,102],'foo',['101','102'],'2012-11-01T22:08:41Z'),
					(4,400,true,12.34,20,[101,102],'foo',['101','102'],'2020-01-01T00:00:00Z');`,
			),
			ExpHdrs: hdrs(),
			ExpRows: rows(),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"delete from del_all_types;",
			),
			ExpHdrs: hdrs(),
			ExpRows: rows(),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id from del_all_types;",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
			),
			ExpRows: rows(),
			Compare: CompareExactUnordered,
		},
	},
}
