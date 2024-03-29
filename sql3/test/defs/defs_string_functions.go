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
		// Reverse
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
			name: "ReverseNoParam",
			SQLs: sqls(
				"select reverse()",
			),
			ExpErr: "count of formal parameters (1) does not match count of actual parameters (0)",
		},
		{
			name: "ReverseInt",
			SQLs: sqls(
				"select reverse(22)",
			),
			ExpErr: "[1:16] string expression expected",
		},
		{
			name: "ReverseFromTable",
			SQLs: sqls(
				"select reverse(a_string) from stringscalarfunctions",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(string("olleh")),
			),
			Compare: CompareExactUnordered,
		},
		// Substring
		{
			name: "SubstringNoParam",
			SQLs: sqls(
				"select Substring()",
			),
			ExpErr: "count of formal parameters (2) does not match count of actual parameters (0)",
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
			name: "SubstringFromTable",
			SQLs: sqls(
				"select substring(a_string, 1, 1) from stringscalarfunctions",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(string("e")),
			),
			Compare: CompareExactUnordered,
		},
		// StringSplit
		{
			name: "StringSplitNoParam",
			SQLs: sqls(
				"select StringSplit()",
			),
			ExpErr: "count of formal parameters (2) does not match count of actual parameters (0)",
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
			name: "StringSplitNullDelim",
			SQLs: sqls(
				"select stringsplit('hello', null)",
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
			name: "StringSplitNullPos",
			SQLs: sqls(
				"select stringsplit('test,hello', ',', null)",
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
			name: "StringSplitFromTable",
			SQLs: sqls(
				"select stringsplit(a_string, 'l') from stringscalarfunctions",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(string("he")),
			),
			Compare: CompareExactUnordered,
		},
		// Char
		{
			name: "CharNoParam",
			SQLs: sqls(
				"select char()",
			),
			ExpErr: "count of formal parameters (1) does not match count of actual parameters (0)",
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
			name: "CharIntMaxValue",
			SQLs: sqls(
				"select char(255)",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(string("ÿ")),
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
			name: "CharNegative",
			SQLs: sqls(
				"select char(-1)",
			),
			ExpErr: "value '-1' out of range",
		},
		{
			name: "CharLargeValue",
			SQLs: sqls(
				"select char(256)",
			),
			ExpErr: "value '256' out of range",
		},
		{
			name: "AsciiCharFromTable",
			SQLs: sqls(
				"select ascii(char(a)) from stringscalarfunctions",
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
			name: "AsciiCharLargeValue",
			SQLs: sqls(
				"select ascii(char(255))",
			),
			ExpErr: "value 'ÿ' should be of the length 1",
		},
		// Ascii
		{
			name: "AsciiNoParam",
			SQLs: sqls(
				"select ascii()",
			),
			ExpErr: "count of formal parameters (1) does not match count of actual parameters (0)",
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
		// Upper
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
		// Lower
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
		// Replaceall
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
			name: "ReplaceAllNullArgAgain",
			SQLs: sqls(
				"select replaceall('hello database', 'data', null)",
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
		// Trim
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
		// Prefix
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
			name: "PrefixNullArg",
			SQLs: sqls(
				"SELECT PREFIX('string', null)",
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
		// Suffix
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
			name: "SuffixNullArg",
			SQLs: sqls(
				"SELECT SUFFIX('string', null)",
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
		// rtrim
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
		// ltrim
		{
			name: "ltrimNull",
			SQLs: sqls(
				"SELECT ltrim(NULL)",
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
		// space
		{
			name: "SpaceNull",
			SQLs: sqls(
				"SELECT space(NULL)",
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
			name: "SpaceNoParam",
			SQLs: sqls(
				"select space()",
			),
			ExpErr: "count of formal parameters (1) does not match count of actual parameters (0)",
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
		// len
		{
			name: "LenNoParam",
			SQLs: sqls(
				"select len()",
			),
			ExpErr: "count of formal parameters (1) does not match count of actual parameters (0)",
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
		// replicate
		{
			name: "ReplicateString",
			SQLs: sqls(
				"select replicate('this', 2)",
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
				"select replicate(null, 3)",
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
			name: "ReplicateNullArg",
			SQLs: sqls(
				"select replicate('string',null)",
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
		// format
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
			name: "FormatNullArgument",
			SQLs: sqls(
				"select format('format = %d', null)",
			),
			ExpErr:  "[1:30] null literal not allowed",
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
		// charindex
		{
			name: "CharIndexIncorrectNumberofArguments",
			SQLs: sqls(
				"select charindex('is','this is great',3,4)",
			),
			ExpErr: "'charindex': count of formal parameters (3) does not match count of actual parameters (4)",
		},
		{
			name: "CharIndexIncorrectTypeofArgumentsforPosition",
			SQLs: sqls(
				"select charindex('is','this is great','you')",
			),
			ExpErr: "integer expression expected",
		},
		{
			name: "CharIndexIncorrectTypeofArgumentsforInputString",
			SQLs: sqls(
				"select charindex('is',23,3)",
			),
			ExpErr: "string expression expected",
		},
		{
			name: "CharIndexIncorrectTypeofArgumentsforSubString",
			SQLs: sqls(
				"select charindex(1,'this is great',3)",
			),
			ExpErr: "string expression expected",
		},
		{
			name: "CharIndexPositionOutofRangewithNegativePosition",
			SQLs: sqls(
				"select charindex('is','this is great',-1)",
			),
			ExpErr: "value '-1' out of range",
		},
		{
			name: "CharIndexPositionOutofRange",
			SQLs: sqls(
				"select charindex('is','this is great',15)",
			),
			ExpErr: "value '15' out of range",
		},
		{
			name: "CharIndexofSubstring",
			SQLs: sqls(
				"Select charindex('is','this is great')",
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
			name: "CharIndexofSubstringwithPosition",
			SQLs: sqls(
				"Select charindex('is','this is great',3)",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(5)),
			),
			Compare: CompareExactOrdered,
		},
		{
			name: "CharIndexSubstringNotfound",
			SQLs: sqls(
				"Select charindex('abc','this is great',3)",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(int64(-1)),
			),
			Compare: CompareExactOrdered,
		},
		{
			name: "CharIndexNullString",
			SQLs: sqls(
				"Select charindex(null, 'this is great', 3)",
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
			name: "CharIndexNullArg2",
			SQLs: sqls(
				"Select charindex('abc', Null, 3)",
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
			name: "CharIndexNullArg3",
			SQLs: sqls(
				"Select charindex('abc', 'this is great', Null)",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeInt),
			),
			ExpRows: rows(
				row(nil),
			),
			Compare: CompareExactOrdered,
		},
		// str
		{
			name: "StrIntValue",
			SQLs: sqls(
				"select str(12345)",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(string("     12345")),
			),
			Compare: CompareExactOrdered,
		},
		{
			name: "StrIntValueAtEdge",
			SQLs: sqls(
				"select str(12345, 5)",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(string("12345")),
			),
			Compare: CompareExactOrdered,
		},
		{
			name: "StrIntValueWithPrecision",
			SQLs: sqls(
				"select str(12345, 5, 5)",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(string("*****")),
			),
			Compare: CompareExactOrdered,
		},
		{
			name: "StrDecimalValue",
			SQLs: sqls(
				"select str(12345.678)",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(string("     12346")),
			),
			Compare: CompareExactOrdered,
		},
		{
			name: "StrDecimalValueAtEdge",
			SQLs: sqls(
				"select str(12345.19, 5)",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(string("12345")),
			),
			Compare: CompareExactOrdered,
		},
		{
			name: "StrDecimalValueWithPrecision",
			SQLs: sqls(
				"select str(12345.789, 8, 2)",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(string("12345.79")),
			),
			Compare: CompareExactOrdered,
		},
		{
			name: "StrNegativeDecimalValueWithPrecision",
			SQLs: sqls(
				"select str(-2345.789, 8, 2)",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(string("-2345.79")),
			),
			Compare: CompareExactOrdered,
		},
		{
			name: "TooFewArgumentsforStr",
			SQLs: sqls(
				"select str()",
			),
			ExpErr: "'str': count of formal parameters (1) does not match count of actual parameters (0)",
		},
		{
			name: "TooManyArgumentsforStr",
			SQLs: sqls(
				"select str(1, 1, 1, 1)",
			),
			ExpErr: "'str': count of formal parameters (1) does not match count of actual parameters (4)",
		},
		{
			name: "StrPrecisionLargerThanValue",
			SQLs: sqls(
				"select str(1234.99, 10, 200)",
			),
			ExpHdrs: hdrs(
				hdr("", fldTypeString),
			),
			ExpRows: rows(
				row(string("**********")),
			),
			Compare: CompareExactOrdered,
		},
		{
			name: "StrNull",
			SQLs: sqls(
				"select str(null)",
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
			name: "StrNullArg",
			SQLs: sqls(
				"select str(1, null)",
			),
			ExpErr: "null literal not allowed",
		},
		{
			name: "StrNullArg2",
			SQLs: sqls(
				"select str(1, 0, null)",
			),
			ExpErr: "null literal not allowed",
		},
	},
}
