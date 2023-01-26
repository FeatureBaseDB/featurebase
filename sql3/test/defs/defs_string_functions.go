package defs

// string function tests
var stringScalarFunctionsTests = TableTest{

	Table: tbl(
		"stringscalarfunctions",
		srcHdrs(
			srcHdr("_id", fldTypeID),
			srcHdr("a", fldTypeInt, "min 0", "max 1000"),
			srcHdr("b", fldTypeInt, "min 0", "max 1000"),
			srcHdr("ts", fldTypeTimestamp),
			srcHdr("a_string", fldTypeString),
		),
		srcRows(
			srcRow(int64(1), int64(10), int64(100), knownTimestamp(), "hello"),
		),
	),
	SQLTests: []SQLTest{
		{
			name: "ReverseNull",
			SQLs: sqls(
				"select reverse(null)",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(nil),
			),
			Compare: CompareExactUnordered,
		},
		{
			name: "ReverseEmpty",
			SQLs: sqls(
				"select reverse('')",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(string("")),
			),
			Compare: CompareExactUnordered,
		},
		{
			name: "ReverseString",
			SQLs: sqls(
				"select reverse('this')",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(string("siht")),
			),
			Compare: CompareExactUnordered,
		},
		{
			name: "ReverseReverseString",
			SQLs: sqls(
				"select reverse(reverse('this'))",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(string("this")),
			),
			Compare: CompareExactUnordered,
		},
		{
			name: "SubstringNull",
			SQLs: sqls(
				"select substring(null, 1, 3)",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(nil),
			),
			Compare: CompareExactUnordered,
		},
		{
			name: "SubstringNullInt",
			SQLs: sqls(
				"select substring('some_string', null, 3)",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(nil),
			),
			Compare: CompareExactUnordered,
		},
		{
			name: "SubstringPositiveIndex",
			SQLs: sqls(
				"select substring('testing', 1, 3)",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(string("est")),
			),
			Compare: CompareExactUnordered,
		},
		{
			name: "SubstringNegativeIndex",
			SQLs: sqls(
				"select substring('testing', -10, 14)",
			),
			ExpErr: "[0:0] value '-10' out of range",
		},
		{
			name: "SubstringNoLength",
			SQLs: sqls(
				"select substring('testing', -5)",
			),
			ExpErr: "[0:0] value '-5' out of range",
		},
		{
			name: "ReverseSubstring",
			SQLs: sqls(
				"select reverse(substring('testing', 0))",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(string("gnitset")),
			),
			Compare: CompareExactUnordered,
		},
		{
			name: "SubstringReverse",
			SQLs: sqls(
				"select substring(reverse('testing'), 3)",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(string("tset")),
			),
			Compare: CompareExactUnordered,
		},
		{
			name: "StringSplitNull",
			SQLs: sqls(
				"select stringsplit(null, ',')",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(nil),
			),
			Compare: CompareExactUnordered,
		},
		{
			name: "StringSplitNoPos",
			SQLs: sqls(
				"select stringsplit('string,split', ',')",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(string("string")),
			),
			Compare: CompareExactUnordered,
		},
		{
			name: "StringSplitPos",
			SQLs: sqls(
				"select stringsplit('string,split,now', stringsplit(',mid,', 'mid', 1), 2)",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(string("now")),
			),
			Compare: CompareExactUnordered,
		},
		{
			name: "CharNull",
			SQLs: sqls(
				"select char(null)",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(nil),
			),
			Compare: CompareExactUnordered,
		},
		{
			name: "CharInt",
			SQLs: sqls(
				"select char(82)",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(string("R")),
			),
			Compare: CompareExactUnordered,
		},
		{
			name: "CharString",
			SQLs: sqls(
				"select char('R')",
			),
			ExpErr: "integer expression expected",
		},
		{
			name: "ASCIINull",
			SQLs: sqls(
				"select ascii(null)",
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
			name: "ASCIILengthMisMatch",
			SQLs: sqls(
				"select ascii('longer')",
			),
			ExpErr: "[0:0] value 'longer' should be of the length 1",
		},
		{
			name: "ASCIIString",
			SQLs: sqls(
				"select ascii('R')",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(82)),
			),
			Compare: CompareExactUnordered,
		},
		{
			name: "ASCIIInt",
			SQLs: sqls(
				"select ascii(32)",
			),
			ExpErr: "string expression expected",
		},
		{
			name: "UpperNull",
			SQLs: sqls(
				"select upper(null)",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(nil),
			),
			Compare: CompareExactOrdered,
		},
		{
			name: "ConvertingStringtoUpper",
			SQLs: sqls(
				"select upper('this')",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(string("THIS")),
			),
			Compare: CompareExactOrdered,
		},
		{
			name: "IncorrectArgumentsforUpper",
			SQLs: sqls(
				"select upper('a','b')",
			),
			ExpErr: "'upper': count of formal parameters (1) does not match count of actual parameters (2)",
		},
		{
			name: "IncorrectInputforUpper",
			SQLs: sqls(
				"select upper(1)",
			),
			ExpErr: "string expression expected",
		},
		{
			name: "LowerNull",
			SQLs: sqls(
				"select lower(null)",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(nil),
			),
			Compare: CompareExactOrdered,
		},
		{
			name: "StringLower",
			SQLs: sqls(
				"select lower('AaBbCcDdEeFfGg-_0123')",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(string("aabbccddeeffgg-_0123")),
			),
			Compare: CompareExactOrdered,
		},
		{
			name: "IncorrectArgumentsforLower",
			SQLs: sqls(
				"select lower('LOWER','lower')",
			),
			ExpErr: "'lower': count of formal parameters (1) does not match count of actual parameters (2)",
		},
		{
			name: "IncorrectInputforLower",
			SQLs: sqls(
				"select lower(1234)",
			),
			ExpErr: "string expression expected",
		},
		{
			name: "ReplaceAllNullString",
			SQLs: sqls(
				"select replaceall(null,'data','feature')",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(nil),
			),
			Compare: CompareExactUnordered,
		},
		{
			name: "ReplaceAllNullArg",
			SQLs: sqls(
				"select replaceall('hello database',null,'feature')",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(nil),
			),
			Compare: CompareExactUnordered,
		},
		{
			name: "ReplaceAllString",
			SQLs: sqls(
				"select replaceall('hello database','data','feature')",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(string("hello featurebase")),
			),
			Compare: CompareExactUnordered,
		},
		{
			name: "ReplaceAllStringMultiple",
			SQLs: sqls(
				"select replaceall('Buffalo Buffalo buffalo buffalo Buffalo', 'Buffalo', 'Buffalo buffalo');",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(string("Buffalo buffalo Buffalo buffalo buffalo buffalo Buffalo buffalo")),
			),
			Compare: CompareExactUnordered,
		},
		{
			name: "ReplaceAllReverseSubstringUpper",
			SQLs: sqls(
				"select replaceall(reverse('gnitset'),substring('testing',4),upper('ed'));",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(string("testED")),
			),
			Compare: CompareExactUnordered,
		},
		{
			name: "IncorrectArgumentsReplaceAll",
			SQLs: sqls(
				"select replaceall('ab','b')",
			),
			ExpErr: "'replaceall': count of formal parameters (3) does not match count of actual parameters (2)",
		},
		{
			name: "IncorrectInputforReplaceAll",
			SQLs: sqls(
				"select replaceall('test','e',1)",
			),
			ExpErr: "string expression expected",
		},
		{
			name: "TrimNull",
			SQLs: sqls(
				"select trim(null)",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(nil),
			),
			Compare: CompareExactOrdered,
		},
		{
			name: "RemovingWhitespacefromStringusingTrim",
			SQLs: sqls(
				"select trim('  this  ')",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(string("this")),
			),
			Compare: CompareExactOrdered,
		},
		{
			name: "IncorrectArgumentsforTrim",
			SQLs: sqls(
				"select trim('  a ','b')",
			),
			ExpErr: "'trim': count of formal parameters (1) does not match count of actual parameters (2)",
		},
		{
			name: "IncorrectInputforTrim",
			SQLs: sqls(
				"select trim(1)",
			),
			ExpErr: "string expression expected",
		},
		//Prefix()
		{
			name: "PrefixNull",
			SQLs: sqls(
				"SELECT PREFIX(NULL, 34)",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(nil),
			),
			Compare: CompareExactOrdered,
		},
		{
			name: "IncorrectArgumentsforPrefix",
			SQLs: sqls(
				"SELECT PREFIX('string')",
			),
			ExpErr: "'PREFIX': count of formal parameters (2) does not match count of actual parameters (1)",
		},
		{
			name: "IncorrectInputforPrefix",
			SQLs: sqls(
				"SELECT PREFIX(1,'string')",
			),
			ExpErr: "string expression expected",
		},
		{
			name: "LengthLargerThanStringforPrefix",
			SQLs: sqls(
				"SELECT PREFIX('string', 7)",
			),
			ExpErr: "[0:0] value '7' out of range",
		},
		{
			name: "NegativeLengthforPrefix",
			SQLs: sqls(
				"SELECT PREFIX('string', -1)",
			),
			ExpErr: "[0:0] value '-1' out of range",
		},
		{
			name: "ZeroLengthforPrefix",
			SQLs: sqls(
				"SELECT PREFIX('string', 0)",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(string("")),
			),
			Compare: CompareExactOrdered,
		},
		{
			name: "GetFirstThreeforPrefix",
			SQLs: sqls(
				"SELECT PREFIX('string', 3)",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(string("str")),
			),
			Compare: CompareExactOrdered,
		},
		{
			name: "FullStringforPrefix",
			SQLs: sqls(
				"SELECT PREFIX('string', 6)",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(string("string")),
			),
			Compare: CompareExactOrdered,
		},
		//Suffix()
		{
			name: "SuffixNull",
			SQLs: sqls(
				"SELECT SUFFIX(NULL, 23)",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(nil),
			),
			Compare: CompareExactOrdered,
		},
		{
			name: "IncorrectArgumentsforSuffix",
			SQLs: sqls(
				"SELECT SUFFIX('string')",
			),
			ExpErr: "'SUFFIX': count of formal parameters (2) does not match count of actual parameters (1)",
		},
		{
			name: "IncorrectInputforSuffix",
			SQLs: sqls(
				"SELECT SUFFIX(1,'string')",
			),
			ExpErr: "string expression expected",
		},
		{
			name: "LengthLargerThanStringforSuffix",
			SQLs: sqls(
				"SELECT SUFFIX('string', 7)",
			),
			ExpErr: "[0:0] value '7' out of range",
		},
		{
			name: "NegativeLengthforSuffix",
			SQLs: sqls(
				"SELECT SUFFIX('string', -1)",
			),
			ExpErr: "[0:0] value '-1' out of range",
		},
		{
			name: "ZeroLengthforSuffix",
			SQLs: sqls(
				"SELECT SUFFIX('string', 0)",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(string("")),
			),
			Compare: CompareExactOrdered,
		},
		{
			name: "GetFirstThreeforSuffix",
			SQLs: sqls(
				"SELECT SUFFIX('string', 3)",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(string("ing")),
			),
			Compare: CompareExactOrdered,
		},
		{
			name: "FullStringforSuffix",
			SQLs: sqls(
				"SELECT SUFFIX('string', 6)",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(string("string")),
			),
			Compare: CompareExactOrdered,
		},
		{
			name: "RTrimNull",
			SQLs: sqls(
				"select rtrim(null)",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(nil),
			),
			Compare: CompareExactOrdered,
		},
		{
			name: "RemovingTrailingspacefromStringusingRTrim",
			SQLs: sqls(
				"select rtrim('  this  ')",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(string("  this")),
			),
			Compare: CompareExactOrdered,
		},
		{
			name: "IncorrectArgumentsforRTrim",
			SQLs: sqls(
				"select rtrim(' a ',' b ')",
			),
			ExpErr: "'rtrim': count of formal parameters (1) does not match count of actual parameters (2)",
		},
		{
			name: "IncorrectInputforRTrim",
			SQLs: sqls(
				"select rtrim(1)",
			),
			ExpErr: "string expression expected",
		},
		{
			name: "LTrimNull",
			SQLs: sqls(
				"select ltrim(null)",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(nil),
			),
			Compare: CompareExactOrdered,
		},
		{
			name: "RemovingLeadingspacefromStringusingLTrim",
			SQLs: sqls(
				"select ltrim('  this  ')",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(string("this  ")),
			),
			Compare: CompareExactOrdered,
		},
		{
			name: "IncorrectArgumentsforLTrim",
			SQLs: sqls(
				"select ltrim(' a ',' b ')",
			),
			ExpErr: "'ltrim': count of formal parameters (1) does not match count of actual parameters (2)",
		},
		{
			name: "IncorrectInputforLTrim",
			SQLs: sqls(
				"select ltrim(1)",
			),
			ExpErr: "string expression expected",
		},
		{
			name: "SpaceZero",
			SQLs: sqls(
				"select space(0)",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(string("")),
			),
			Compare: CompareExactOrdered,
		},
		{
			name: "SpaceFive",
			SQLs: sqls(
				"select space(5)",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(string("     ")),
			),
			Compare: CompareExactOrdered,
		},
		{
			name: "LenNull",
			SQLs: sqls(
				"select len(null)",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(nil),
			),
			Compare: CompareExactOrdered,
		},
		{
			name: "LenString",
			SQLs: sqls(
				"select len(' length  ')",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(9)),
			),
			Compare: CompareExactOrdered,
		},
		{
			name: "LenChar",
			SQLs: sqls(
				"select len(char(114))",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(1)),
			),
			Compare: CompareExactOrdered,
		},
		{
			name: "ReplicateString",
			SQLs: sqls(
				"select replicate('this',2)",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(string("thisthis")),
			),
			Compare: CompareExactOrdered,
		},
		{
			name: "ReplicateNull",
			SQLs: sqls(
				"select replicate(null,null)",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(nil),
			),
			Compare: CompareExactOrdered,
		},
		{
			name: "ReplicateincorrectArguments",
			SQLs: sqls(
				"select replicate('this',)",
			),
			ExpErr: "[1:25] 'replicate': count of formal parameters (2) does not match count of actual parameters (1)",
		},
		{
			name: "ReplicateincorrectTypeofArguments",
			SQLs: sqls(
				"select replicate(1,2)",
			),
			ExpErr: "[1:18] string expression expected",
		},
		{
			name: "ReplicateincorrectTypeofArguments",
			SQLs: sqls(
				"select replicate('this','this')",
			),
			ExpErr: "[1:25] integer expression expected",
		},
		{
			name: "ReplicateOutofRange",
			SQLs: sqls(
				"select replicate('this',-1)",
			),
			ExpErr: "[0:0] value '-1' out of range",
		},
		{
			name: "FormatString",
			SQLs: sqls(
				"select format('this or %s', 'that')",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(string("this or that")),
			),
			Compare: CompareExactOrdered,
		},
		{
			name: "FormatBoolean",
			SQLs: sqls(
				"select format('is this %t?', true)",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(string("is this true?")),
			),
			Compare: CompareExactOrdered,
		},
		{
			name: "FormatInteger",
			SQLs: sqls(
				"select format('%d > %d', 11 , 9)",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(string("11 > 9")),
			),
			Compare: CompareExactOrdered,
		},
		{
			name: "FormatNullString",
			SQLs: sqls(
				"select format(null,'this')",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(nil),
			),
			Compare: CompareExactOrdered,
		},
		{
			name: "FormatLengthZero",
			SQLs: sqls(
				"select format()",
			),
			ExpErr:  "[1:15] 'format': count of formal parameters (1) does not match count of actual parameters (0)",
			Compare: CompareExactOrdered,
		},
		{
			name: "FormatLengthOne",
			SQLs: sqls(
				"select format('noArg')",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(string("noArg")),
			),
			Compare: CompareExactOrdered,
		},
	},
}
