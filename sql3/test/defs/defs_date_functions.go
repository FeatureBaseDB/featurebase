package defs

import "time"

// datepart tests
var datePartTests = TableTest{

	Table: tbl(
		"dateparttests",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("a", fldTypeInt, "min 0", "max 1000"),
			srcHdr("b", fldTypeInt, "min 0", "max 1000"),
			srcHdr("ts", fldTypeTimestamp, "timeunit 'ns'"),
		),
		srcRows(
			srcRow(int64(1), int64(10), int64(100), knownSubSecondTimestamp()),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select datepart()",
			),
			ExpErr: "count of formal parameters (2) does not match count of actual parameters (0)",
		},
		{
			SQLs: sqls(
				"select datepart(1, 2)",
			),
			ExpErr: "an expression of type 'int' cannot be passed to a parameter of type 'string'",
		},
		{
			SQLs: sqls(
				"select datepart('1', current_timestamp)",
			),
			ExpErr: "invalid value '1' for parameter 'interval'",
		},
		{
			SQLs: sqls(
				"select _id, datepart('yy', ts) from dateparttests",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(1), int64(2012)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id, datepart('yd', ts) from dateparttests",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(1), int64(306)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id, datepart('m', ts) from dateparttests",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(1), int64(11)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id, datepart('d', ts) from dateparttests",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(1), int64(1)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id, datepart('w', ts) from dateparttests",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(1), int64(4)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id, datepart('wk', ts) from dateparttests",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(1), int64(44)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id, datepart('hh', ts) from dateparttests",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(1), int64(22)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id, datepart('mi', ts) from dateparttests",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(1), int64(8)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id, datepart('s', ts) from dateparttests",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(1), int64(41)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id, datepart('ms', ts) from dateparttests",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(1), int64(100)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id, datepart('us', ts) from dateparttests",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(1), int64(200)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id, datepart('ns', ts) from dateparttests",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(1), int64(300)),
			),
			Compare: CompareExactUnordered,
		},
		{
			//test datepart(timestamp, part) for implicit conversion of integer value passed as argument to timestamp param
			SQLs: sqls(
				"select datepart('yy', 0) as \"yy\", datepart('m', 0) as \"m\", datepart('d', 0) as \"d\"",
			),
			ExpHdrs: hdrs(
				hdr("yy", fldTypeInt),
				hdr("m", fldTypeInt),
				hdr("d", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(1970), int64(1), int64(1)),
			),
			Compare: CompareExactUnordered,
		},
	},
}

// toTimestamp tests
var toTimestampTests = TableTest{

	Table: tbl(
		"",
		nil,
		nil,
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select totimestamp()",
			),
			ExpErr: "count of formal parameters (2) does not match count of actual parameters (0)",
		},
		{
			SQLs: sqls(
				"select totimestamp('a')",
			),
			ExpErr: "an expression of type 'string' cannot be passed to a parameter of type 'int'",
		},
		{
			SQLs: sqls(
				"select totimestamp(1, 2)",
			),
			ExpErr: "an expression of type 'int' cannot be passed to a parameter of type 'string'",
		},
		{
			SQLs: sqls(
				"select totimestamp(1, 'x')",
			),
			ExpErr: "invalid value 'x' for parameter 'timeunit'",
		},
		{
			//test ToTimestamp(num, timeunit) for all possible time unit values
			SQLs: sqls(
				"select totimestamp(1000) as \"default\", totimestamp(1000, 's') as \"s\", totimestamp(1000000, 'ms') as \"ms\", totimestamp(1000000000, 'us') as \"us\", totimestamp(1000000000, 'µs') as \"µs\", totimestamp(1000000000000, 'ns') as \"ns\"",
			),
			ExpHdrs: hdrs(
				hdr("default", fldTypeTimestamp),
				hdr("s", fldTypeTimestamp),
				hdr("ms", fldTypeTimestamp),
				hdr("us", fldTypeTimestamp),
				hdr("µs", fldTypeTimestamp),
				hdr("ns", fldTypeTimestamp),
			),
			ExpRows: rows(
				row(time.Unix(1000, 0).UTC(),
					time.Unix(1000, 0).UTC(),
					time.UnixMilli(1000000).UTC(),
					time.UnixMicro(1000000000).UTC(),
					time.UnixMicro(1000000000).UTC(),
					time.Unix(0, 1000000000000).UTC()),
			),
			Compare: CompareExactUnordered,
		},
	},
}

// datetimeAdd tests
var datetimeAddTests = TableTest{

	Table: tbl(
		"datetimeadd",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("ts", fldTypeTimestamp, "timeunit 'ns'"),
		),
		srcRows(
			srcRow(int64(1), knownSubSecondTimestamp()),
		),
	),
	SQLTests: []SQLTest{
		{
			SQLs: sqls(
				"select datetimeadd()",
			),
			ExpErr: "count of formal parameters (3) does not match count of actual parameters (0)",
		},
		{
			SQLs: sqls(
				"select datetimeadd(1,1,current_timestamp)",
			),
			ExpErr: "an expression of type 'int' cannot be passed to a parameter of type 'string'",
		},
		{
			SQLs: sqls(
				"select datetimeadd('yy', '2',current_timestamp)",
			),
			ExpErr: "an expression of type 'string' cannot be passed to a parameter of type 'int'",
		},
		{
			SQLs: sqls(
				"select datetimeadd('yy', 2, true)",
			),
			ExpErr: "an expression of type 'bool' cannot be passed to a parameter of type 'timestamp'",
		},
		{
			SQLs: sqls(
				"select datetimeadd('x',1,current_timestamp)",
			),
			ExpErr: "invalid value 'x' for parameter 'timeunit'",
		},
		{
			SQLs: sqls(
				"select datetimeadd('ms',7000,'YYYY-MM-DDTHH:MM:SS')",
			),
			ExpErr: "unable to convert 'YYYY-MM-DDTHH:MM:SS' to type 'timestamp'",
		},
		//Test datetimeadd() for all possible time units
		{
			SQLs: sqls(
				"select _id, datepart('YY',datetimeadd('YY', 1, ts)) from datetimeadd",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(1), int64(2013)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id, datepart('M',datetimeadd('M', 1, ts)) from datetimeadd",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(1), int64(12)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id, datepart('D',datetimeadd('D', 1, ts)) from datetimeadd",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(1), int64(2)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id, datepart('HH',datetimeadd('HH', 1, ts)) from datetimeadd",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(1), int64(23)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id, datepart('MI',datetimeadd('MI', 1, ts)) from datetimeadd",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(1), int64(9)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id, datepart('S',datetimeadd('S', 1, ts)) from datetimeadd",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(1), int64(42)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id, datepart('MS',datetimeadd('MS', 1, ts)) from datetimeadd",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(1), int64(101)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id, datepart('US',datetimeadd('US', 1, ts)) from datetimeadd",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(1), int64(201)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id, datepart('NS',datetimeadd('NS', 1, ts)) from datetimeadd",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(1), int64(301)),
			),
			Compare: CompareExactUnordered,
		},
		//test datetimeadd() for subtraction
		{
			SQLs: sqls(
				"select _id, datepart('YY',datetimeadd('YY', -1, ts)) from datetimeadd",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(1), int64(2011)),
			),
			Compare: CompareExactUnordered,
		},
		//test datetimeadd() for transition
		{
			SQLs: sqls(
				"select _id, datepart('NS',datetimeadd('NS', 700, ts)) as a, datepart('US',datetimeadd('NS', 700, ts)) as b from datetimeadd",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("a", fldTypeInt),
				hdr("b", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(1), int64(0), int64(201)),
			),
			Compare: CompareExactUnordered,
		},
		//test datetimeadd() for literals
		{
			SQLs: sqls(
				"select _id, datepart('YY',datetimeadd('YY', 1, 0)) from datetimeadd",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(1), int64(1971)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id, datepart('YY',datetimeadd('YY', 1, '2023-03-03T00:00:00Z')) from datetimeadd",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(1), int64(2024)),
			),
			Compare: CompareExactUnordered,
		},
	},
}
