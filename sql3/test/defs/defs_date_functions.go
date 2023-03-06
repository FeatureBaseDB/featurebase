package defs

import (
	"fmt"
	"time"
)

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
			name: "DatePartIncorrectParamsCount",
			SQLs: sqls(
				"select datepart()",
			),
			ExpErr: "count of formal parameters (2) does not match count of actual parameters (0)",
		},
		{
			name: "DatePartIntError",
			SQLs: sqls(
				"select datepart(1, 2)",
			),
			ExpErr: "an expression of type 'int' cannot be passed to a parameter of type 'string'",
		},
		{
			name: "DatePartInvalidParam",
			SQLs: sqls(
				"select datepart('1', current_timestamp)",
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
			name: "DATEPARTYY",
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
			name: "DATEPARTYD",
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
			name: "DATEPARTM",
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
			name: "DATEPARTD",
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
			name: "DATEPARTW",
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
			name: "DATEPARTWK",
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
			name: "DATEPARTHH",
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
			name: "DatePartMI",
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
			name: "DatePartS",
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
			name: "DatePartMS",
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
			name: "DatePartNS",
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
			name: "DatePartImplicitIntConversion",
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
				fmt.Sprintf("select datetimefromparts(%d,%d,%d,%d,%d,%d,%d) as datetime", 0, 0, 0, 0, 0, 0, 0),
			),
			ExpHdrs: hdrs(
				hdr("datetime", fldTypeTimestamp),
			),
			ExpRows: rows(
				row(time.Date(0, 0, 0, 0, 0, 0, 0, time.UTC)),
			),
		},
	},
}
