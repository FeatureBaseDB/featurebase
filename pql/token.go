// Copyright 2017 Pilosa Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
	INTEGER   // 12345
	FLOAT     // 100.2
	literal_end

	keyword_beg
	ALL
	keyword_end

	ASSIGN  // =
	EQ      // ==
	NEQ     // !=
	LT      // <
	LTE     // <=
	GT      // >
	GTE     // >=
	BETWEEN // ><
	COMMA   // ,
	LPAREN  // (
	RPAREN  // )
	LBRACK  // (
	RBRACK  // )
)

var tokens = [...]string{
	ILLEGAL: "ILLEGAL",
	EOF:     "EOF",
	WS:      "WS",

	IDENT:   "IDENT",
	INTEGER: "INTEGER",
	FLOAT:   "FLOAT",

	ALL: "ALL",

	ASSIGN:  "=",
	EQ:      "==",
	NEQ:     "!=",
	LT:      "<",
	LTE:     "<=",
	GT:      ">",
	GTE:     ">=",
	BETWEEN: "><",
	COMMA:   ",",
	LPAREN:  "(",
	RPAREN:  ")",
	LBRACK:  "(",
	RBRACK:  ")",
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
