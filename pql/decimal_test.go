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

	"github.com/pilosa/pilosa/v2/pql"
)

// Ensure call can be converted into a string.
func TestDecimal(t *testing.T) {
	t.Run("Parse", func(t *testing.T) {
		tests := []struct {
			s      string
			exp    pql.Decimal
			expErr string
		}{
			{"0", pql.Decimal{0, 0}, ""},
			{"-0", pql.Decimal{0, 0}, ""},
			{"0.0", pql.Decimal{0, 0}, ""},
			{"-0.00", pql.Decimal{0, 0}, ""},
			{"123.4567", pql.Decimal{1234567, 4}, ""},
			{"  123.4567", pql.Decimal{1234567, 4}, ""},
			{"  123.4567  ", pql.Decimal{1234567, 4}, ""},
			{"123.456700", pql.Decimal{1234567, 4}, ""},
			{"00123.4567", pql.Decimal{1234567, 4}, ""},
			{"+123.4567", pql.Decimal{1234567, 4}, ""},
			{"-123.4567", pql.Decimal{-1234567, 4}, ""},
			{"-00123.4567", pql.Decimal{-1234567, 4}, ""},
			{"-12.25", pql.Decimal{-1225, 2}, ""},

			{"123", pql.Decimal{123, 0}, ""},
			{"-12300", pql.Decimal{-123, -2}, ""},
			{"+012300", pql.Decimal{123, -2}, ""},
			{"12300", pql.Decimal{123, -2}, ""},
			{"12300.", pql.Decimal{123, -2}, ""},
			{"12300.0", pql.Decimal{123, -2}, ""},
			{"123.0", pql.Decimal{123, 0}, ""},

			{".123", pql.Decimal{123, 3}, ""},
			{"0.123", pql.Decimal{123, 3}, ""},
			{"0.001230", pql.Decimal{123, 5}, ""},
			{" 0.001230 ", pql.Decimal{123, 5}, ""},
			{"-0.001230 ", pql.Decimal{-123, 5}, ""},

			// int64 edges.
			{".000009223372036854775807", pql.Decimal{9223372036854775807, 24}, ""},
			{"-.000009223372036854775807", pql.Decimal{-9223372036854775807, 24}, ""},
			{"92233720368547.75807", pql.Decimal{9223372036854775807, 5}, ""},
			{"-92233720368547.75807", pql.Decimal{-9223372036854775807, 5}, ""},
			{"9223372036854775807000", pql.Decimal{9223372036854775807, -3}, ""},
			{"-9223372036854775807000", pql.Decimal{-9223372036854775807, -3}, ""},

			// Error cases.
			{"", pql.Decimal{}, "decimal string is empty"},
			{"-", pql.Decimal{}, "decimal string is empty"},
			{"*0.123", pql.Decimal{}, "invalid syntax"},
			{"abc", pql.Decimal{}, "invalid syntax"},
			{"0.12.3", pql.Decimal{}, "invalid decimal string"},
			{"--12300", pql.Decimal{}, "invalid negative value"},
			{"922337203685477580.8", pql.Decimal{}, "value out of range"},
			{"-922337203685477580.8", pql.Decimal{}, "value out of range"},
			{"9223372036854775808000", pql.Decimal{}, "value out of range"},
			{"-9223372036854775808000", pql.Decimal{}, "value out of range"},
		}
		for i, test := range tests {
			dec, err := pql.ParseDecimal(test.s)
			if test.expErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.expErr) {
					t.Fatalf("test %d expected error to contain: %s, but got: %v", i, test.expErr, err)
				}
			} else if err != nil {
				t.Fatalf("test %d parsing string `%s`: %s", i, test.s, err)
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
			{pql.Decimal{0, 0}, 0, 0},  // 0 : 0
			{pql.Decimal{0, 0}, 1, 0},  // 0 : 0.0
			{pql.Decimal{0, 0}, -1, 0}, // 0 : 0

			{pql.Decimal{1234567, 4}, 5, 12345670}, // 123.4567 : 123.45670
			{pql.Decimal{1234567, 4}, 4, 1234567},  // 123.4567 : 123.4567
			{pql.Decimal{1234567, 4}, 3, 123456},   // 123.4567 : 123.456

			{pql.Decimal{-1234567, 4}, 5, -12345670}, // -123.4567 : -123.45670
			{pql.Decimal{-1234567, 4}, 4, -1234567},  // -123.4567 : -123.4567
			{pql.Decimal{-1234567, 4}, 3, -123456},   // -123.4567 : -123.456

			{pql.Decimal{123, -2}, 5, 1230000000}, // 12300 : 12300.00000
			{pql.Decimal{123, -2}, -1, 1230},      // 12300 : 1230
			{pql.Decimal{123, 1}, -1, 1},          // 12.3 : 1
			{pql.Decimal{123, 1}, -2, 0},          // 12.3 : 0
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
