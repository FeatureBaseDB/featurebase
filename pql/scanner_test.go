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

package pql_test

import (
	"strings"
	"testing"

	"github.com/pilosa/pilosa/pql"
)

func TestScanner_Scan(t *testing.T) {
	var tests = []struct {
		s   string
		tok pql.Token
		lit string
		pos pql.Pos
	}{
		// Special tokens (EOF, ILLEGAL, WS)
		{s: ``, tok: pql.EOF},
		{s: `#`, tok: pql.ILLEGAL, lit: `#`},
		{s: ` `, tok: pql.WS, lit: " "},
		{s: "\t", tok: pql.WS, lit: "\t"},
		{s: "\n", tok: pql.WS, lit: "\n"},

		{s: `=`, tok: pql.EQ, lit: `=`},
		{s: `,`, tok: pql.COMMA, lit: `,`},
		{s: `(`, tok: pql.LPAREN, lit: `(`},
		{s: `)`, tok: pql.RPAREN, lit: `)`},
		{s: `[`, tok: pql.LBRACK, lit: `[`},
		{s: `]`, tok: pql.RBRACK, lit: `]`},

		{s: `foo`, tok: pql.IDENT, lit: `foo`},
		{s: `100`, tok: pql.INTEGER, lit: `100`},
		{s: `100.3`, tok: pql.FLOAT, lit: `100.3`},

		{s: `all`, tok: pql.ALL, lit: `all`},
		{s: `ALL`, tok: pql.ALL, lit: `ALL`}, // case insensitive
	}

	for i, tt := range tests {
		s := pql.NewScanner(strings.NewReader(tt.s))
		tok, pos, lit := s.Scan()
		if tt.tok != tok {
			t.Errorf("%d. %q token mismatch: exp=%q got=%q <%q>", i, tt.s, tt.tok, tok, lit)
		} else if tt.pos.Line != pos.Line || tt.pos.Char != pos.Char {
			t.Errorf("%d. %q pos mismatch: exp=%#v got=%#v", i, tt.s, tt.pos, pos)
		} else if tt.lit != lit {
			t.Errorf("%d. %q literal mismatch: exp=%q got=%q", i, tt.s, tt.lit, lit)
		}
	}
}
