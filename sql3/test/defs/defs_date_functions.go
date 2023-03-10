package defs

import (
	"fmt"
	"time"
)

// datetimepart tests
var dateTimePartTests = TableTest{

	Table: tbl(
		"datetimeparttests",
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
			name: "DateTimePartIncorrectParamsCount",
			SQLs: sqls(
				"select datetimepart()",
			),
			ExpErr: "count of formal parameters (2) does not match count of actual parameters (0)",
		},
		{
			name: "DateTimePartIntError",
			SQLs: sqls(
				"select datetimepart(1, 2)",
			),
			ExpErr: "an expression of type 'int' cannot be passed to a parameter of type 'string'",
		},
		{
			name: "DateTimePartInvalidParam",
			SQLs: sqls(
				"select datetimepart('1', current_timestamp)",
			),
			ExpErr: "invalid value '1' for parameter 'interval'",
		},
		{
			name: "ToTimestampWrongParamsCount",
			SQLs: sqls(
				"select totimestamp()",
			),
			ExpErr: "count of formal parameters (2) does not match count of actual parameters (0)",
		},
		{
			name: "ToTimestampStringError",
			SQLs: sqls(
				"select totimestamp('a')",
			),
			ExpErr: "an expression of type 'string' cannot be passed to a parameter of type 'int'",
		},
		{
			name: "ToTimestampIntError",
			SQLs: sqls(
				"select totimestamp(1, 2)",
			),
			ExpErr: "an expression of type 'int' cannot be passed to a parameter of type 'string'",
		},
		{
			name: "ToTimestampInvalid",
			SQLs: sqls(
				"select totimestamp(1, 'x')",
			),
			ExpErr: "invalid value 'x' for parameter 'timeunit'",
		},
		{
			name: "DATETIMEPARTYY",
			SQLs: sqls(
				"select _id, datetimepart('yy', ts) from datetimeparttests",
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
			name: "DATETIMEPARTYD",
			SQLs: sqls(
				"select _id, datetimepart('yd', ts) from datetimeparttests",
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
			name: "DATETIMEPARTM",
			SQLs: sqls(
				"select _id, datetimepart('m', ts) from datetimeparttests",
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
			name: "DATETIMEPARTD",
			SQLs: sqls(
				"select _id, datetimepart('d', ts) from datetimeparttests",
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
			name: "DATETIMEPARTW",
			SQLs: sqls(
				"select _id, datetimepart('w', ts) from datetimeparttests",
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
			name: "DATETIMEPARTWK",
			SQLs: sqls(
				"select _id, datetimepart('wk', ts) from datetimeparttests",
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
			name: "DATETIMEPARTHH",
			SQLs: sqls(
				"select _id, datetimepart('hh', ts) from datetimeparttests",
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
			name: "DateTimePartMI",
			SQLs: sqls(
				"select _id, datetimepart('mi', ts) from datetimeparttests",
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
			name: "DateTimePartS",
			SQLs: sqls(
				"select _id, datetimepart('s', ts) from datetimeparttests",
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
			name: "DateTimePartMS",
			SQLs: sqls(
				"select _id, datetimepart('ms', ts) from datetimeparttests",
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
			name: "DateTimePartUS",
			SQLs: sqls(
				"select _id, datetimepart('us', ts) from datetimeparttests",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(1), int64(100200)),
			),
			Compare: CompareExactUnordered,
		},
		{
			name: "DateTimePartNS",
			SQLs: sqls(
				"select _id, datetimepart('ns', ts) from datetimeparttests",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(1), int64(100200300)),
			),
			Compare: CompareExactUnordered,
		},
		{
			//test datetimepart(timestamp, part) for implicit conversion of integer value passed as argument to timestamp param
			name: "DateTimePartImplicitIntConversion",
			SQLs: sqls(
				"select datetimepart('yy', 0) as \"yy\", datetimepart('m', 0) as \"m\", datetimepart('d', 0) as \"d\"",
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
			name: "ToTimestampAllPossibleValues",
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
		{
			name: "DateTimeFromPartsParamsCountMismatch",
			SQLs: sqls(
				"select datetimefromparts(12,32,43,34,34,34)",
			),
			ExpErr: "count of formal parameters (7) does not match count of actual parameters (6)",
		},
		{
			name: "DateTimeFromPartsParamsTypeMismatch",
			SQLs: sqls(
				"select datetimefromparts(12,32,43,34,34,34,'foo')",
			),
			ExpErr: "an expression of type 'string' cannot be passed to a parameter of type 'int'",
		},
		{
			name: "DateTimeFromPartsInvalidDatetimePart",
			SQLs: sqls(
				"select datetimefromparts(10000,1,1,1,1,1,1)",
			),
			ExpErr: "[0:0] not a valid datetimepart 10000",
		},
		{
			name: "DateTimeFromPartsInvalidDatetimePart2",
			SQLs: sqls(
				"select datetimefromparts(2023,2,29,1,1,1,1)",
			),
			ExpErr: "[0:0] not a valid datetimepart 29",
		},
		{
			name: "DateTimeFromPartsKnownTimestamp",
			SQLs: sqls(
				fmt.Sprintf("select datetimefromparts(%d,%d,%d,%d,%d,%d,%d) as datetime", knownTimestamp().Year(),
					knownTimestamp().Month(), knownTimestamp().Day(), knownTimestamp().Hour(), knownTimestamp().Minute(),
					knownTimestamp().Second(), knownTimestamp().Nanosecond()/(1000*1000)),
			),
			ExpHdrs: hdrs(
				hdr("datetime", fldTypeTimestamp),
			),
			ExpRows: rows(
				row(knownTimestamp()),
			),
			Compare: CompareExactUnordered,
		},
		{
			name: "DateTimeFromPartsAllZeros",
			SQLs: sqls(
				fmt.Sprintf("select datetimefromparts(%d,%d,%d,%d,%d,%d,%d) as datetime", 0, 1, 1, 0, 0, 0, 0),
			),
			ExpHdrs: hdrs(
				hdr("datetime", fldTypeTimestamp),
			),
			ExpRows: rows(
				row(time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC)),
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
				"select _id, datetimename('yy', ts) from datetimenametests",
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
				"select _id, datetimename('m', ts) from datetimenametests",
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
				"select _id, datetimename('w', ts) from datetimenametests",
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
				"select _id, datetimepart('YY',datetimeadd('YY', 1, ts)) from datetimeadd",
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
				"select _id, datetimepart('M',datetimeadd('M', 1, ts)) from datetimeadd",
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
				"select _id, datetimepart('D',datetimeadd('D', 1, ts)) from datetimeadd",
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
				"select _id, datetimepart('HH',datetimeadd('HH', 1, ts)) from datetimeadd",
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
				"select _id, datetimepart('MI',datetimeadd('MI', 1, ts)) from datetimeadd",
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
				"select _id, datetimepart('S',datetimeadd('S', 1, ts)) from datetimeadd",
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
				"select _id, datetimepart('MS',datetimeadd('MS', 1, ts)) from datetimeadd",
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
				"select _id, datetimepart('US',datetimeadd('US', 1, ts)) from datetimeadd",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(1), int64(100201)),
			),
			Compare: CompareExactUnordered,
		},
		{
			SQLs: sqls(
				"select _id, datetimepart('NS',datetimeadd('NS', 1, ts)) from datetimeadd",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(1), int64(100200301)),
			),
			Compare: CompareExactUnordered,
		},
		//test datetimeadd() for subtraction
		{
			SQLs: sqls(
				"select _id, datetimepart('YY',datetimeadd('YY', -1, ts)) from datetimeadd",
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
				"select _id, datetimepart('NS',datetimeadd('NS', 700, ts)) as a, datetimepart('US',datetimeadd('NS', 700, ts)) as b from datetimeadd",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("a", fldTypeInt),
				hdr("b", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(1), int64(100201000), int64(100201)),
			),
			Compare: CompareExactUnordered,
		},
		//test datetimeadd() for literals
		{
			SQLs: sqls(
				"select _id, datetimepart('YY',datetimeadd('YY', 1, 0)) from datetimeadd",
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
				"select _id, datetimepart('YY',datetimeadd('YY', 1, '2023-03-03T00:00:00Z')) from datetimeadd",
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

var dateTruncTests = TableTest{

	Table: tbl(
		"datetrunctests",
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
			name: "DateTruncIncorrectParamsCount",
			SQLs: sqls(
				"select date_trunc()",
			),
			ExpErr: "count of formal parameters (2) does not match count of actual parameters (0)",
		},
		{
			name: "DateTruncTypeError",
			SQLs: sqls(
				"select date_trunc(1, 2)",
			),
			ExpErr: "an expression of type 'int' cannot be passed to a parameter of type 'string'",
		},
		{
			name: "DateTruncInvalidParam",
			SQLs: sqls(
				"select date_trunc('1', current_timestamp)",
			),
			ExpErr: "invalid value '1' for parameter 'interval'",
		},
		{
			name: "DateTruncOnYear",
			SQLs: sqls(
				"select _id, date_trunc('yy', ts) from datetrunctests",
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
		{
			name: "DateTruncOnMonth",
			SQLs: sqls(
				"select _id, date_trunc('m', ts) from datetrunctests",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(int64(1), "2012-11"),
			),
			Compare: CompareExactUnordered,
		},
		{
			name: "DateTruncOnDay",
			SQLs: sqls(
				"select _id, date_trunc('d', ts) from datetrunctests",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(int64(1), "2012-11-01"),
			),
			Compare: CompareExactUnordered,
		},
		{
			name: "DateTruncOnHour",
			SQLs: sqls(
				"select _id, date_trunc('hh', ts) from datetrunctests",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(int64(1), "2012-11-01T22"),
			),
			Compare: CompareExactUnordered,
		},
		{
			name: "DateTruncOnMinute",
			SQLs: sqls(
				"select _id, date_trunc('mi', ts) from datetrunctests",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(int64(1), "2012-11-01T22:08"),
			),
			Compare: CompareExactUnordered,
		},
		{
			name: "DateTruncOnSecond",
			SQLs: sqls(
				"select _id, date_trunc('s', ts) from datetrunctests",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(int64(1), "2012-11-01T22:08:41"),
			),
			Compare: CompareExactUnordered,
		},
		{
			name: "DateTruncOnMilliS",
			SQLs: sqls(
				"select _id, date_trunc('ms', ts) from datetrunctests",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(int64(1), "2012-11-01T22:08:41.100"),
			),
			Compare: CompareExactUnordered,
		},
		{
			name: "DateTruncOnMicroS",
			SQLs: sqls(
				"select _id, date_trunc('us', ts) from datetrunctests",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(int64(1), "2012-11-01T22:08:41.100200"),
			),
			Compare: CompareExactUnordered,
		},
		{
			name: "DateTruncOnNanoS",
			SQLs: sqls(
				"select _id, date_trunc('ns', ts) from datetrunctests",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(int64(1), "2012-11-01T22:08:41.100200300"),
			),
			Compare: CompareExactUnordered,
		},
		{
			name: "VerifyTimeStamp",
			SQLs: sqls(
				"select _id, datetimename('ns', ts) from datetrunctests",
			),
			ExpHdrs: hdrs(
				hdr("_id", fldTypeID),
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(int64(1), "100200300"),
			),
			Compare: CompareExactUnordered,
		},
	},
}

var datetimedifftests = TableTest{
	name: "DatetimeDiff",
	Table: tbl("dttable",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("startTime", fldTypeTimestamp, "timeunit 'ns'"),
			srcHdr("endTime", fldTypeTimestamp, "timeunit 'ns'"),
		),
		srcRows(
			srcRow(int64(1), knownSubSecondTimestamp(), knownSubSecondTimestamp2()),
		)),
	SQLTests: []SQLTest{
		{
			name: "DatetimeDiffWrongParamCount",
			SQLs: sqls(
				"select datetimediff(startTime, endTime) from dttable;",
			),
			ExpErr: "count of formal parameters (3) does not match count of actual parameters (2)",
		},
		{
			name: "DatetimeDiffInvalidType",
			SQLs: sqls(
				"select datetimediff('yy','nope', endTime) from dttable;",
			),
			ExpErr: "[0:0] unable to convert 'nope' to type 'timestamp'",
		},
		{
			name: "DatetimeDiffNull",
			SQLs: sqls(
				"select datetimediff(null, startTime, endTime) from dttable;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(nil),
			),
			Compare: CompareExactUnordered,
		},
		{
			name: "DatetimeDiffYY",
			SQLs: sqls(
				"select datetimediff('yy', startTime, endTime) from dttable;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(10)),
			),
			Compare: CompareExactUnordered,
		},
		{
			name: "DatetimeDiffM",
			SQLs: sqls(
				"select datetimediff('m', startTime, endTime) from dttable;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(121)),
			),
			Compare: CompareExactUnordered,
		},
		{
			name: "DatetimeDiffD",
			SQLs: sqls(
				"select datetimediff('d', startTime, endTime) from dttable;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(3689)),
			),
			Compare: CompareExactUnordered,
		},
		{
			name: "DatetimeDiffHH",
			SQLs: sqls(
				"select datetimediff('hh', startTime, endTime) from dttable;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(88555)),
			),
			Compare: CompareExactUnordered,
		},
		{
			name: "DatetimeDiffMI",
			SQLs: sqls(
				"select datetimediff('mi', startTime, endTime) from dttable;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(5313356)),
			),
			Compare: CompareExactUnordered,
		},
		{
			name: "DatetimeDiffS",
			SQLs: sqls(
				"select datetimediff('s', startTime, endTime) from dttable;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(318801373)),
			),
			Compare: CompareExactUnordered,
		},
		{
			name: "DatetimeDiffMS",
			SQLs: sqls(
				"select datetimediff('ms', startTime, endTime) from dttable;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(318801373200)),
			),
			Compare: CompareExactUnordered,
		},
		{
			name: "DatetimeDiffUS",
			SQLs: sqls(
				"select datetimediff('us', startTime, endTime) from dttable;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(318801373200300)),
			),
			Compare: CompareExactUnordered,
		},
		{
			name: "DatetimeDiffNS",
			SQLs: sqls(
				"select datetimediff('ns', startTime, endTime) from dttable;",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(318801373200300500)),
			),
			Compare: CompareExactUnordered,
		},
	},
}
