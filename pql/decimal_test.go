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
	"testing"

	"github.com/pilosa/pilosa/v2/pql"
)

// Ensure call can be converted into a string.
func TestDecimal(t *testing.T) {
	t.Run("Parse", func(t *testing.T) {
		tests := []struct {
			s      string
			exp    pql.Decimal
			expErr error // TODO: add tests for errors
		}{
			{"123.4567", pql.Decimal{false, 1234567, 4}, nil},
			{"  123.4567", pql.Decimal{false, 1234567, 4}, nil},
			{"  123.4567  ", pql.Decimal{false, 1234567, 4}, nil},
			{"123.456700", pql.Decimal{false, 1234567, 4}, nil},
			{"00123.4567", pql.Decimal{false, 1234567, 4}, nil},
			{"+123.4567", pql.Decimal{false, 1234567, 4}, nil},
			{"-123.4567", pql.Decimal{true, 1234567, 4}, nil},
			{"-00123.4567", pql.Decimal{true, 1234567, 4}, nil},
			{"-12.25", pql.Decimal{true, 1225, 2}, nil},

			{"123", pql.Decimal{false, 123, 0}, nil},
			{"-12300", pql.Decimal{true, 123, -2}, nil},
			{"+012300", pql.Decimal{false, 123, -2}, nil},
			{"12300", pql.Decimal{false, 123, -2}, nil},
			{"12300.", pql.Decimal{false, 123, -2}, nil},
			{"12300.0", pql.Decimal{false, 123, -2}, nil},
			{"123.0", pql.Decimal{false, 123, 0}, nil},

			{"0.123", pql.Decimal{false, 123, 3}, nil},
			{"0.001230", pql.Decimal{false, 123, 5}, nil},
			{" 0.001230 ", pql.Decimal{false, 123, 5}, nil},
			{"-0.001230 ", pql.Decimal{true, 123, 5}, nil},
		}
		for i, test := range tests {
			dec, err := pql.ParseDecimal(test.s)
			if err != nil {
				t.Fatalf("parsing string `%s`: %s", test.s, err)
			} else if dec != test.exp {
				t.Fatalf("test %d expected: %v, but got: %v", i, test.exp, dec)
			}
		}
	})

	t.Run("ToInt64", func(t *testing.T) {
		tests := []struct {
			dec   pql.Decimal
			scale int64
			exp   int64
		}{
			{pql.Decimal{false, 0, 0}, 0, 0},  // 0 : 0
			{pql.Decimal{false, 0, 0}, 1, 0},  // 0 : 0.0
			{pql.Decimal{false, 0, 0}, -1, 0}, // 0 : 0

			{pql.Decimal{false, 1234567, 4}, 5, 12345670}, // 123.4567 : 123.45670
			{pql.Decimal{false, 1234567, 4}, 4, 1234567},  // 123.4567 : 123.4567
			{pql.Decimal{false, 1234567, 4}, 3, 123456},   // 123.4567 : 123.456

			{pql.Decimal{true, 1234567, 4}, 5, -12345670}, // -123.4567 : -123.45670
			{pql.Decimal{true, 1234567, 4}, 4, -1234567},  // -123.4567 : -123.4567
			{pql.Decimal{true, 1234567, 4}, 3, -123456},   // -123.4567 : -123.456

			{pql.Decimal{false, 123, -2}, 5, 1230000000}, // 12300 : 12300.00000
			{pql.Decimal{false, 123, -2}, -1, 1230},      // 12300 : 1230
			{pql.Decimal{false, 123, 1}, -1, 1},          // 12.3 : 1
			{pql.Decimal{false, 123, 1}, -2, 0},          // 12.3 : 0
		}
		for i, test := range tests {
			v := test.dec.ToInt64(test.scale)
			if v != test.exp {
				t.Fatalf("test %d expected: %d, but got: %d", i, test.exp, v)
			}
		}
	})

	t.Run("String", func(t *testing.T) {
		tests := []struct {
			s   string
			exp string
		}{
			{"123.4567", "123.4567"},
			{"  123.4567", "123.4567"},
			{"  123.4567  ", "123.4567"},
			{"123.456700", "123.4567"},
			{"00123.4567", "123.4567"},
			{"+123.4567", "123.4567"},
			{"-123.4567", "-123.4567"},
			{"-00123.4567", "-123.4567"},
			{"-12.25", "-12.25"},

			{"123", "123"},
			{"-12300", "-12300"},
			{"+012300", "12300"},
			{"12300", "12300"},

			{"12300.", "12300"},
			{"12300.0", "12300"},
			{"123.0", "123"},

			{"0.123", "0.123"},
			{"0.001230", "0.00123"},
			{" 0.001230 ", "0.00123"},
			{"-0.001230 ", "-0.00123"},
		}
		for i, test := range tests {
			dec, err := pql.ParseDecimal(test.s)
			if err != nil {
				t.Fatalf("parsing string `%s`: %s", test.s, err)
			}
			if str := dec.String(); str != test.exp {
				t.Fatalf("test %d expected: %s, but got: %s", i, test.exp, str)
			}
		}
	})
}
