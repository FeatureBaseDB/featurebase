package sql3_test

// join tests
var joinTestsUsers = tableTest{
	name: "jointestusers",
	table: tbl(
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
	sqlTests: nil,
}

var joinTestsOrders = tableTest{
	name: "jointestorders",
	table: tbl(
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
	sqlTests: nil,
}

var joinTests = tableTest{
	name: "innerjointest",
	sqlTests: []sqlTest{
		{
			name: "innerjoin-aggregate-groupby",
			sqls: sqls(
				"select u._id, sum(orders.price) from orders o inner join users u on o.userid = u._id group by u._id;",
			),
			expHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeDecimal2),
			),
			expRows: rows(
				row(int64(1), float64(22.98)),
				row(int64(0), float64(3.99)),
				row(int64(2), float64(16.98)),
				row(int64(3), float64(5.99)),
			),
			compare: compareExactOrdered,
		},
	},
}
