// Copyright 2021 Molecula Corp. All rights reserved.
package parser_test

import (
	"strings"
	"testing"

	"github.com/featurebasedb/featurebase/v3/sql3/parser"
)

func TestScanner_Scan(t *testing.T) {
	t.Run("IDENT", func(t *testing.T) {
		t.Run("Unquoted", func(t *testing.T) {
			AssertScan(t, `foo_BAR123`, parser.IDENT, `foo_BAR123`)
		})
		t.Run("Quoted", func(t *testing.T) {
			AssertScan(t, `"crazy ~!#*&# column name"" foo"`, parser.QIDENT, `crazy ~!#*&# column name" foo`)
		})
		t.Run("NoEndQuote", func(t *testing.T) {
			AssertScan(t, `"unfinished`, parser.ILLEGAL, `"unfinished`)
		})
		t.Run("x", func(t *testing.T) {
			AssertScan(t, `x`, parser.IDENT, `x`)
		})
		t.Run("StartingX", func(t *testing.T) {
			AssertScan(t, `xyz`, parser.IDENT, `xyz`)
		})
		t.Run("WithComment", func(t *testing.T) {
			AssertScan(t, "-- this is a comment\n\n-- more comments\nfoo", parser.IDENT, `foo`)
		})
	})

	t.Run("KEYWORD", func(t *testing.T) {
		AssertScan(t, `BEGIN`, parser.BEGIN, `BEGIN`)
	})

	t.Run("STRING", func(t *testing.T) {
		t.Run("OK", func(t *testing.T) {
			AssertScan(t, `'this is ''a'' string'`, parser.STRING, `this is 'a' string`)
		})
		t.Run("NoEndQuote", func(t *testing.T) {
			AssertScan(t, `'unfinished`, parser.ILLEGAL, `'unfinished`)
		})
		t.Run("NoEndQuoteNL", func(t *testing.T) {
			AssertScan(t, "'unfinished\n", parser.UNTERMSTRING, `'unfinished`)
		})
	})
	t.Run("BLOB", func(t *testing.T) {
		t.Run("LowerX", func(t *testing.T) {
			AssertScan(t, `x'0123456789abcdef'`, parser.BLOB, `0123456789abcdef`)
		})
		t.Run("UpperX", func(t *testing.T) {
			AssertScan(t, `X'0123456789ABCDEF'`, parser.BLOB, `0123456789ABCDEF`)
		})
		t.Run("NoEndQuote", func(t *testing.T) {
			AssertScan(t, `x'0123`, parser.ILLEGAL, `x'0123`)
		})
		t.Run("QuotedQuote", func(t *testing.T) {
			AssertScan(t, `x'01''23'`, parser.BLOB, `01'23`)
		})
	})

	t.Run("INTEGER", func(t *testing.T) {
		AssertScan(t, `123`, parser.INTEGER, `123`)
	})

	t.Run("FLOAT", func(t *testing.T) {
		AssertScan(t, `123.456`, parser.FLOAT, `123.456`)
		AssertScan(t, `.1`, parser.FLOAT, `.1`)
		AssertScan(t, `123e456`, parser.FLOAT, `123e456`)
		AssertScan(t, `123E456`, parser.FLOAT, `123E456`)
		AssertScan(t, `123.456E78`, parser.FLOAT, `123.456E78`)
		AssertScan(t, `123.E45`, parser.FLOAT, `123.E45`)
		AssertScan(t, `123E+4`, parser.FLOAT, `123E+4`)
		AssertScan(t, `123E-4`, parser.FLOAT, `123E-4`)
		AssertScan(t, `123E`, parser.ILLEGAL, `123E`)
		AssertScan(t, `123E+`, parser.ILLEGAL, `123E+`)
		AssertScan(t, `123E-`, parser.ILLEGAL, `123E-`)
	})

	t.Run("EOF", func(t *testing.T) {
		AssertScan(t, " \n\t\r", parser.EOF, ``)
	})

	t.Run("SEMI", func(t *testing.T) {
		AssertScan(t, ";", parser.SEMI, ";")
	})
	t.Run("LP", func(t *testing.T) {
		AssertScan(t, "(", parser.LP, "(")
	})
	t.Run("RP", func(t *testing.T) {
		AssertScan(t, ")", parser.RP, ")")
	})
	t.Run("COMMA", func(t *testing.T) {
		AssertScan(t, ",", parser.COMMA, ",")
	})
	t.Run("NE", func(t *testing.T) {
		AssertScan(t, "!=", parser.NE, "!=")
	})
	t.Run("BITNOT", func(t *testing.T) {
		AssertScan(t, "!", parser.BITNOT, "!")
	})
	t.Run("EQ", func(t *testing.T) {
		AssertScan(t, "=", parser.EQ, "=")
	})
	t.Run("LE", func(t *testing.T) {
		AssertScan(t, "<=", parser.LE, "<=")
	})
	t.Run("LSHIFT", func(t *testing.T) {
		AssertScan(t, "<<", parser.LSHIFT, "<<")
	})
	t.Run("LT", func(t *testing.T) {
		AssertScan(t, "<", parser.LT, "<")
	})
	t.Run("GE", func(t *testing.T) {
		AssertScan(t, ">=", parser.GE, ">=")
	})
	t.Run("RSHIFT", func(t *testing.T) {
		AssertScan(t, ">>", parser.RSHIFT, ">>")
	})
	t.Run("GT", func(t *testing.T) {
		AssertScan(t, ">", parser.GT, ">")
	})
	t.Run("BITAND", func(t *testing.T) {
		AssertScan(t, "&", parser.BITAND, "&")
	})
	t.Run("CONCAT", func(t *testing.T) {
		AssertScan(t, "||", parser.CONCAT, "||")
	})
	t.Run("BITOR", func(t *testing.T) {
		AssertScan(t, "|", parser.BITOR, "|")
	})
	t.Run("PLUS", func(t *testing.T) {
		AssertScan(t, "+", parser.PLUS, "+")
	})
	t.Run("MINUS", func(t *testing.T) {
		AssertScan(t, "-", parser.MINUS, "-")
	})
	t.Run("STAR", func(t *testing.T) {
		AssertScan(t, "*", parser.STAR, "*")
	})
	t.Run("SLASH", func(t *testing.T) {
		AssertScan(t, "/", parser.SLASH, "/")
	})
	t.Run("REM", func(t *testing.T) {
		AssertScan(t, "%", parser.REM, "%")
	})
	t.Run("DOT", func(t *testing.T) {
		AssertScan(t, ".", parser.DOT, ".")
	})
	t.Run("ILLEGAL", func(t *testing.T) {
		AssertScan(t, "^", parser.ILLEGAL, "^")
	})
}

// AssertScan asserts the value of the first scan to s.
func AssertScan(tb testing.TB, s string, expectedTok parser.Token, expectedLit string) {
	tb.Helper()
	_, tok, lit := parser.NewScanner(strings.NewReader(s)).Scan()
	if tok != expectedTok || lit != expectedLit {
		tb.Fatalf("Scan(%q)=<%s,%s>, want <%s,%s>", s, tok, lit, expectedTok, expectedLit)
	}
}
