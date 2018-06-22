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

// Token is a lexical token of the PQL language.
type Token int

const (
	// Special tokens
	ILLEGAL Token = iota

	ASSIGN  // =
	EQ      // ==
	NEQ     // !=
	LT      // <
	LTE     // <=
	GT      // >
	GTE     // >=
	BETWEEN // ><
)

var tokens = [...]string{
	ILLEGAL: "ILLEGAL",

	ASSIGN:  "=",
	EQ:      "==",
	NEQ:     "!=",
	LT:      "<",
	LTE:     "<=",
	GT:      ">",
	GTE:     ">=",
	BETWEEN: "><",
}

// String returns the string representation of the token.
func (tok Token) String() string {
	if tok >= 0 && tok < Token(len(tokens)) {
		return tokens[tok]
	}
	return ""
}
