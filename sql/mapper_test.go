// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package sql

import (
	"fmt"
	"testing"

	"vitess.io/vitess/go/vt/sqlparser"
)

func TestParse(t *testing.T) {
	tests := []struct {
		sql     string
		expMask QueryMask
	}{
		{
			sql: "select _id from tbl",
			expMask: QueryMask{
				SelectMask: SelectPartID,
				FromMask:   FromPartTable,
			},
		},
		{
			sql: "select * from tbl",
			expMask: QueryMask{
				SelectMask: SelectPartStar,
				FromMask:   FromPartTable,
			},
		},
		{
			sql: "select fld from tbl",
			expMask: QueryMask{
				SelectMask: SelectPartField,
				FromMask:   FromPartTable,
			},
		},
		{
			sql: "select fld1, fld2 from tbl",
			expMask: QueryMask{
				SelectMask: SelectPartFields,
				FromMask:   FromPartTable,
			},
		},
		{
			sql: "select count(*) from tbl",
			expMask: QueryMask{
				SelectMask: SelectPartCountStar,
				FromMask:   FromPartTable,
			},
		},
		{
			sql: "select min(fld) from tbl",
			expMask: QueryMask{
				SelectMask: SelectPartMinField,
				FromMask:   FromPartTable,
			},
		},
		{
			sql: "select max(fld) from tbl",
			expMask: QueryMask{
				SelectMask: SelectPartMaxField,
				FromMask:   FromPartTable,
			},
		},
		{
			sql: "select sum(fld) from tbl",
			expMask: QueryMask{
				SelectMask: SelectPartSumField,
				FromMask:   FromPartTable,
			},
		},
		{
			sql: "select avg(fld) from tbl",
			expMask: QueryMask{
				SelectMask: SelectPartAvgField,
				FromMask:   FromPartTable,
			},
		},
		{
			sql: "select _id, count(*) from tbl",
			expMask: QueryMask{
				SelectMask: SelectPartID | SelectPartCountStar,
				FromMask:   FromPartTable,
			},
		},
		{
			sql: "select _id from tbl1, tbl2",
			expMask: QueryMask{
				SelectMask: SelectPartID,
				FromMask:   FromPartTables,
			},
		},
		{
			sql: "select _id from tbl1 INNER JOIN tbl2",
			expMask: QueryMask{
				SelectMask: SelectPartID,
				FromMask:   FromPartJoin,
			},
		},
		{
			sql: "select * from tbl where _id = 1",
			expMask: QueryMask{
				SelectMask: SelectPartStar,
				FromMask:   FromPartTable,
				WhereMask:  WherePartIDCondition,
			},
		},
		{
			sql: "select * from tbl where fld = 1",
			expMask: QueryMask{
				SelectMask: SelectPartStar,
				FromMask:   FromPartTable,
				WhereMask:  WherePartFieldCondition,
			},
		},
		{
			sql: "select * from tbl group by fld",
			expMask: QueryMask{
				SelectMask:  SelectPartStar,
				FromMask:    FromPartTable,
				GroupByMask: GroupByPartField,
			},
		},
		{
			sql: "select * from tbl group by fld1, fld2",
			expMask: QueryMask{
				SelectMask:  SelectPartStar,
				FromMask:    FromPartTable,
				GroupByMask: GroupByPartFields,
			},
		},
		{
			sql: "select fld, sum(fld) from tbl group by fld",
			expMask: QueryMask{
				SelectMask:  SelectPartField | SelectPartSumField,
				FromMask:    FromPartTable,
				GroupByMask: GroupByPartField,
			},
		},
		{
			sql: "select fld, sum(fld) from tbl group by fld having sum > 10",
			expMask: QueryMask{
				SelectMask:  SelectPartField | SelectPartSumField,
				FromMask:    FromPartTable,
				GroupByMask: GroupByPartField,
				HavingMask:  HavingPartCondition,
			},
		},
		{
			sql: "select fld from tbl order by fld",
			expMask: QueryMask{
				SelectMask:  SelectPartField,
				FromMask:    FromPartTable,
				OrderByMask: OrderByPartField,
			},
		},
		{
			sql: "select fld from tbl order by fld1, fld2",
			expMask: QueryMask{
				SelectMask:  SelectPartField,
				FromMask:    FromPartTable,
				OrderByMask: OrderByPartFields,
			},
		},
		{
			sql: "select fld from tbl limit 10",
			expMask: QueryMask{
				SelectMask: SelectPartField,
				FromMask:   FromPartTable,
				LimitMask:  LimitPartLimit,
			},
		},
		{
			sql: "select fld from tbl limit 10, 5",
			expMask: QueryMask{
				SelectMask: SelectPartField,
				FromMask:   FromPartTable,
				LimitMask:  LimitPartLimit | LimitPartOffset,
			},
		},
		{
			sql: "select distinct fld from tbl",
			expMask: QueryMask{
				SelectMask: SelectPartDistinct | SelectPartField,
				FromMask:   FromPartTable,
			},
		},
		{
			sql: "select count(*) from tbl where fld = 1",
			expMask: QueryMask{
				SelectMask: SelectPartCountStar,
				FromMask:   FromPartTable,
				WhereMask:  WherePartFieldCondition,
			},
		},
		{
			sql: "select count(*) from tbl where fld1 = 1 and fld2 = 2",
			expMask: QueryMask{
				SelectMask: SelectPartCountStar,
				FromMask:   FromPartTable,
				WhereMask:  WherePartMultiFieldCondition,
			},
		},
		{
			sql: "select count(*) from tbl where fld1 = 1 or fld2 = 2",
			expMask: QueryMask{
				SelectMask: SelectPartCountStar,
				FromMask:   FromPartTable,
				WhereMask:  WherePartMultiFieldCondition,
			},
		},
		{
			sql: "select _id from tbl where not fld = 1 limit 10",
			expMask: QueryMask{
				SelectMask: SelectPartID,
				FromMask:   FromPartTable,
				WhereMask:  WherePartFieldCondition,
				LimitMask:  LimitPartLimit,
			},
		},
		{
			sql: "select _id from tbl where fld between 1 and 3",
			expMask: QueryMask{
				SelectMask: SelectPartID,
				FromMask:   FromPartTable,
				WhereMask:  WherePartFieldCondition,
			},
		},
		{
			sql: "select _id from tbl where fld1 between 1 and 3 and fld2 = 2",
			expMask: QueryMask{
				SelectMask: SelectPartID,
				FromMask:   FromPartTable,
				WhereMask:  WherePartMultiFieldCondition,
			},
		},
		{
			sql: "select count(*) from tbl where fld is not null",
			expMask: QueryMask{
				SelectMask: SelectPartCountStar,
				FromMask:   FromPartTable,
				WhereMask:  WherePartFieldCondition,
			},
		},
		{
			sql: "select fld, count(*) from tbl group by fld",
			expMask: QueryMask{
				SelectMask:  SelectPartField | SelectPartCountStar,
				FromMask:    FromPartTable,
				GroupByMask: GroupByPartField,
			},
		},
		{
			sql: "select fld1, fld2, count(*) from grouper group by fld1, fld2",
			expMask: QueryMask{
				SelectMask:  SelectPartFields | SelectPartCountStar,
				FromMask:    FromPartTable,
				GroupByMask: GroupByPartFields,
			},
		},
		{
			sql: "select fld1, fld2, count(*) from tbl where fld1 = 1 group by fld1, fld2 limit 1",
			expMask: QueryMask{
				SelectMask:  SelectPartFields | SelectPartCountStar,
				FromMask:    FromPartTable,
				WhereMask:   WherePartFieldCondition,
				GroupByMask: GroupByPartFields,
				LimitMask:   LimitPartLimit,
			},
		},
		{
			sql: "select count(distinct fld) from tbl",
			expMask: QueryMask{
				SelectMask: SelectPartCountDistinctField,
				FromMask:   FromPartTable,
			},
		},
		{
			sql: "select count(*) from tbl1 INNER JOIN tbl2 ON tbl1._id = tbl2.bsi",
			expMask: QueryMask{
				SelectMask: SelectPartCountStar,
				FromMask:   FromPartJoin,
			},
		},
		{
			sql: "select count(*) from tbl1 INNER JOIN tbl2 ON tbl1._id = tbl2.bsi where tbl1.fld1 = 1 and tbl2.fld2 = 2",
			expMask: QueryMask{
				SelectMask: SelectPartCountStar,
				FromMask:   FromPartJoin,
				WhereMask:  WherePartMultiFieldCondition,
			},
		},
	}

	mapper := NewMapper()
	for i, test := range tests {
		t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
			_, mask, err := mapper.Parse(test.sql)
			if err != nil {
				t.Fatal(err)
			}

			if mask != test.expMask {
				t.Fatalf("expected mask: %v, but got: %v", test.expMask, mask)
			}
		})
	}
}

// This thing passes even if join is not implemented.
// TODO: make this actually a test
func TestSelectJoin(t *testing.T) {
	tests := []struct {
		sql string
	}{
		{
			// Count(Intersect(All(),Distinct(Row(grouperid!=null),index='joiner',field='grouperid')))
			sql: "select count(*) from grouper g INNER JOIN joiner j ON g._id = j.grouperid",
		},
		{
			// Intersect(All(),Distinct(Row(grouperid!=null),index='joiner',field='grouperid'))
			sql: "select _id from grouper g INNER JOIN joiner j ON g._id = j.grouperid",
		},
		{
			// Intersect(Row(color='red'),Distinct(Row(grouperid!=null),index='joiner',field='grouperid'))
			sql: "select _id from grouper g INNER JOIN joiner j ON g._id = j.grouperid where g.color = 'red'",
		},
		{
			// Intersect(All(),Distinct(Row(grouperid!=null),index='joiner',field='grouperid'))
			sql: "select _id from grouper g INNER JOIN joiner j ON g._id = j.grouperid where g.color = 'red' and j.jointype = 2",
		},
	}

	mapper := NewMapper()
	for i, test := range tests {
		t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
			if m, err := mapper.MapSQL(test.sql); err != nil {
				t.Fatal(err)
			} else {
				if stmt, ok := m.Statement.(*sqlparser.Select); !ok {
					t.Fatalf("%s: expected Statement: sqlparser.Select, got %T", test.sql, m.Statement)
				} else {
					t.Logf("%+v\n", stmt)
				}
			}
		})
	}
}

func TestOrderBy(t *testing.T) {
	tests := []struct {
		sql string
	}{
		{
			sql: "select distinct score from grouper order by score asc",
		},
		{
			sql: "select distinct score from grouper order by score desc",
		},
		{
			sql: "select distinct score from grouper order by score asc limit 5",
		},
		{
			sql: "select distinct score from grouper order by score desc limit 5",
		},
	}
	mapper := NewMapper()
	for i, test := range tests {
		t.Run(fmt.Sprintf("test-%d", i), func(t *testing.T) {
			if m, err := mapper.MapSQL(test.sql); err != nil {
				t.Fatal(err)
			} else {
				if stmt, ok := m.Statement.(*sqlparser.Select); !ok {
					t.Fatalf("%s: expected Statement: sqlparser.Select, got %T", test.sql, m.Statement)
				} else {
					t.Logf("%+v\n", stmt)
				}
			}
		})
	}
}
