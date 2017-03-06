package pql

import "strings"

// Token is a lexical token of the PQL language.
type Token int

const (
	// Special tokens
	ILLEGAL Token = iota
	EOF
	WS

	literal_beg
	IDENT     // main
	STRING    // "foo"
	BADSTRING // bad escape or unclosed string
	NUMBER    // 12345
	FLOAT     // 100.2
	literal_end

	keyword_beg
	ALL
	keyword_end

	EQ     // =
	COMMA  // ,
	LPAREN // (
	RPAREN // )
	LBRACK // (
	RBRACK // )
)

var tokens = [...]string{
	ILLEGAL: "ILLEGAL",
	EOF:     "EOF",
	WS:      "WS",

	IDENT:  "IDENT",
	NUMBER: "NUMBER",

	ALL: "ALL",

	EQ:     "=",
	COMMA:  ",",
	LPAREN: "(",
	RPAREN: ")",
	LBRACK: "(",
	RBRACK: ")",
}

var keywords map[string]Token

func init() {
	keywords = make(map[string]Token)
	for tok := keyword_beg + 1; tok < keyword_end; tok++ {
		keywords[strings.ToLower(tokens[tok])] = tok
	}
}

// String returns the string representation of the token.
func (tok Token) String() string {
	if tok >= 0 && tok < Token(len(tokens)) {
		return tokens[tok]
	}
	return ""
}

// Lookup returns the token associated with a given string.
func Lookup(ident string) Token {
	if tok, ok := keywords[strings.ToLower(ident)]; ok {
		return tok
	}
	return IDENT
}

// Pos specifies the line and character position of a token.
// The Char and Line are both zero-based indexes.
type Pos struct {
	Line int
	Char int
}
