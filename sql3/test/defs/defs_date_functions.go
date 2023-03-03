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
			srcHdr("ts", fldTypeTimestamp),
		),
		srcRows(
			srcRow(int64(1), int64(10), int64(100), knownTimestamp()),
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
				row(int64(1), int64(0)),
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
				row(int64(1), int64(0)),
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

var dateTimeNameTests = TableTest{

	Table: tbl(
		"datetimenametests",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("ts", fldTypeTimestamp),
		),
		srcRows(
			srcRow(int64(1), knownTimestamp()),
		),
	),
	SQLTests: []SQLTest{
		// dateTimeName tests
		// make sure returning a string as the year still works
		// if this works then the other parts converted to strings of digits should also work
		{
			SQLs: sqls(
				"select _id, datetimename('yy', ts) from dateparttests",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(int64(1), "2012"),
			),
			Compare: CompareExactUnordered,
		},
		//check to make sure it gets a month name correctly
		{
			SQLs: sqls(
				"select _id, datetimename('m', ts) from dateparttests",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(int64(1), "November"),
			),
			Compare: CompareExactUnordered,
		},
		// check to make sure it gets a day of the week correctly
		{
			SQLs: sqls(
				"select _id, datetimename('w', ts) from dateparttests",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(int64(1), "Thursday"),
			),
			Compare: CompareExactUnordered,
		},
	},
}
