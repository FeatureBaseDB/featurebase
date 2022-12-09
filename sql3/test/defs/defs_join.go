package defs

import "github.com/featurebasedb/featurebase/v3/pql"

// join tests
var joinTestsUsers = TableTest{
	name: "jointestusers",
	Table: tbl(
		"users",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("name", fldTypeString),
			srcHdr("age", fldTypeInt),
		),
		srcRows(
			srcRow(int64(0), string("a"), int64(21)),
			srcRow(int64(1), string("b"), int64(18)),
			srcRow(int64(2), string("c"), int64(28)),
			srcRow(int64(3), string("d"), int64(34)),
		),
	),
	SQLTests: nil,
}

var joinTestsOrders = TableTest{
	name: "jointestorders",
	Table: tbl(
		"orders",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("userid", fldTypeID),
			srcHdr("price", fldTypeDecimal2),
		),
		srcRows(
			srcRow(int64(0), int64(1), float64(9.99)),
			srcRow(int64(1), int64(0), float64(3.99)),
			srcRow(int64(2), int64(2), float64(14.99)),
			srcRow(int64(3), int64(3), float64(5.99)),
			srcRow(int64(4), int64(1), float64(12.99)),
			srcRow(int64(5), int64(2), float64(1.99)),
		),
	),
	SQLTests: nil,
}

var joinTests = TableTest{
	name: "innerjointest",
	SQLTests: []SQLTest{
		{
			name: "innerjoin-aggregate-groupby",
			SQLs: sqls(
				"select u._id, sum(orders.price) from orders o inner join users u on o.userid = u._id group by u._id;",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeDecimal2),
			),
			ExpRows: rows(
				row(int64(1), pql.NewDecimal(2298, 2)),
				row(int64(0), pql.NewDecimal(399, 2)),
				row(int64(2), pql.NewDecimal(1698, 2)),
				row(int64(3), pql.NewDecimal(599, 2)),
			),
			Compare: CompareExactOrdered,
		},
		{
			name: "innerjoin-aggregate-groupby-sum-filter",
			SQLs: sqls(
				"select sum(price) from orders o inner join users u on o.userid = u._id where u.age > 20;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeDecimal2),
			),
			ExpRows: rows(
				row(pql.NewDecimal(2696, 2)),
			),
			Compare: CompareExactOrdered,
		},
		{
			name: "innerjoin-aggregate-groupby-count-distinct-filter",
			SQLs: sqls(
				"SELECT COUNT(DISTINCT u.name) FROM orders o JOIN users u ON o.userid = u._id WHERE o.price > 10;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(2)),
			),
			Compare: CompareExactOrdered,
		},
		{
			name: "innerjoin-aggregate-groupby-count-filter",
			SQLs: sqls(
				"SELECT COUNT(u.name) FROM orders o JOIN users u ON o.userid = u._id WHERE o.price > 10;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(2)),
			),
			Compare: CompareExactOrdered,
		},
	},
}
