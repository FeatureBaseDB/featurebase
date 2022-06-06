// Copyright 2021 Molecula Corp. All rights reserved.
package pql_test

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/molecula/featurebase/v3/pql"
)

// Ensure call can be converted into a string.
func TestDecimal(t *testing.T) {
	t.Run("Parse", func(t *testing.T) {
		tests := []struct {
			s      string
			exp    pql.Decimal
			expErr string
		}{
			{"0", pql.NewDecimal(0, 0), ""},
			{"-0", pql.NewDecimal(0, 0), ""},
			{"0.0", pql.NewDecimal(0, 0), ""},
			{"0.", pql.NewDecimal(0, 0), ""},
			{"-0.00", pql.NewDecimal(0, 0), ""},
			{"123.4567", pql.NewDecimal(1234567, 4), ""},
			{"123.456700", pql.NewDecimal(1234567, 4), ""},
			{"00123.4567", pql.NewDecimal(1234567, 4), ""},
			{"+123.4567", pql.NewDecimal(1234567, 4), ""},
			{"-123.4567", pql.NewDecimal(-1234567, 4), ""},
			{"-00123.4567", pql.NewDecimal(-1234567, 4), ""},
			{"-12.25", pql.NewDecimal(-1225, 2), ""},

			{"123", pql.NewDecimal(123, 0), ""},
			{"-12300", pql.NewDecimal(-123, -2), ""},
			{"+012300", pql.NewDecimal(123, -2), ""},
			{"12300", pql.NewDecimal(123, -2), ""},
			{"12300.", pql.NewDecimal(123, -2), ""},
			{"12300.0", pql.NewDecimal(123, -2), ""},
			{"123.0", pql.NewDecimal(123, 0), ""},

			{".123", pql.NewDecimal(123, 3), ""},
			{"0.123", pql.NewDecimal(123, 3), ""},
			{"0.001230", pql.NewDecimal(123, 5), ""},
			{"-0.001230", pql.NewDecimal(-123, 5), ""},

			// int64 edges.
			{".000009223372036854775807", pql.NewDecimal(9223372036854775807, 24), ""},
			{"-.000009223372036854775808", pql.NewDecimal(-9223372036854775808, 24), ""},
			{"92233720368547.75807", pql.NewDecimal(9223372036854775807, 5), ""},
			{"-92233720368547.75807", pql.NewDecimal(-9223372036854775807, 5), ""},
			{"9223372036854775807000", pql.NewDecimal(9223372036854775807, -3), ""},
			{"-9223372036854775807000", pql.NewDecimal(-9223372036854775807, -3), ""},

			// precision adjustment
			{"2.666666666666666667", pql.NewDecimal(2666666666666666667, 18), ""},
			{"2.6666666666666666667", pql.NewDecimal(2666666666666666666, 18), ""},
			{"2.6666666666666666666667", pql.NewDecimal(2666666666666666666, 18), ""},
			{"-9.223372036854775808", pql.NewDecimal(-9223372036854775808, 18), ""},
			{"-9.223372036854775809", pql.NewDecimal(-922337203685477580, 17), ""},
			{"9.223372036854775807", pql.NewDecimal(9223372036854775807, 18), ""},
			{"9.223372036854775808", pql.NewDecimal(922337203685477580, 17), ""},

			// Error cases.
			{"", pql.Decimal{}, "decimal string is empty"},
			{"-", pql.Decimal{}, "decimal string is empty"},
			{"*0.123", pql.Decimal{}, "invalid syntax"},
			{"abc", pql.Decimal{}, "invalid syntax"},
			{"0.12.3", pql.Decimal{}, "invalid decimal string"},
			{"--12300", pql.Decimal{}, "invalid syntax"},
			{"  123.4567  ", pql.Decimal{}, "invalid syntax"},
			{"  123.4567", pql.Decimal{}, "invalid syntax"},
			{"123.4567 ", pql.Decimal{}, "invalid syntax"},
			{"0.a", pql.Decimal{}, "invalid syntax"},

			// These are no longer error cases since we introduced precision adjustment.
			//{"922337203685477580.9", pql.Decimal{}, "value out of range"},
			//{"-922337203685477580.9", pql.Decimal{}, "value out of range"},
			{"9223372036854775808000", pql.Decimal{}, "value out of range"},
			{"-9223372036854775809000", pql.Decimal{}, "value out of range"},
		}
		for i, test := range tests {
			dec, err := pql.ParseDecimal(test.s)
			if test.expErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.expErr) {
					t.Fatalf("test %d parsing string `%s`: expected error to contain: %s, but got: %v", i, test.s, test.expErr, err)
				}
			} else if err != nil {
				t.Fatalf("test %d parsing string `%s`: %s", i, test.s, err)
			} else if !dec.EqualTo(test.exp) {
				t.Fatalf("test %d parsing string `%s`: expected: %v, but got: %v", i, test.s, test.exp, dec)
			}
		}
	})

	t.Run("ToInt64", func(t *testing.T) {
		tests := []struct {
			dec   pql.Decimal
			scale int64
			exp   int64
		}{
			{pql.NewDecimal(0, 0), 0, 0},  // 0 : 0
			{pql.NewDecimal(0, 0), 1, 0},  // 0 : 0.0
			{pql.NewDecimal(0, 0), -1, 0}, // 0 : 0

			{pql.NewDecimal(1234567, 4), 5, 12345670}, // 123.4567 : 123.45670
			{pql.NewDecimal(1234567, 4), 4, 1234567},  // 123.4567 : 123.4567
			{pql.NewDecimal(1234567, 4), 3, 123456},   // 123.4567 : 123.456

			{pql.NewDecimal(-1234567, 4), 5, -12345670}, // -123.4567 : -123.45670
			{pql.NewDecimal(-1234567, 4), 4, -1234567},  // -123.4567 : -123.4567
			{pql.NewDecimal(-1234567, 4), 3, -123456},   // -123.4567 : -123.456

			{pql.NewDecimal(123, -2), 5, 1230000000}, // 12300 : 12300.00000
			{pql.NewDecimal(123, -2), -1, 1230},      // 12300 : 1230
			{pql.NewDecimal(123, 1), -1, 1},          // 12.3 : 1
			{pql.NewDecimal(123, 1), -2, 0},          // 12.3 : 0
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
			{"+0.001230", "0.00123"},
			{"-0.001230", "-0.00123"},
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

	t.Run("Comparisons", func(t *testing.T) {
		tests := []struct {
			d1     pql.Decimal
			d2     pql.Decimal
			expLT  bool
			expLTE bool
			expGT  bool
			expGTE bool
			expEQ  bool
		}{
			{pql.NewDecimal(0, 0), pql.NewDecimal(0, 0), false, true, false, true, true},
			{pql.NewDecimal(0, 0), pql.NewDecimal(10, 0), true, true, false, false, false},
			{pql.NewDecimal(10, 0), pql.NewDecimal(0, 0), false, false, true, true, false},
			{pql.NewDecimal(123456, 3), pql.NewDecimal(123456, 3), false, true, false, true, true},
			{pql.NewDecimal(123456, 3), pql.NewDecimal(123456, 4), false, false, true, true, false},
			{pql.NewDecimal(123456, 4), pql.NewDecimal(123456, 3), true, true, false, false, false},
			{pql.NewDecimal(1233456, 4), pql.NewDecimal(123456, 3), true, true, false, false, false},

			{pql.NewDecimal(0, 0), pql.NewDecimal(-10, 0), false, false, true, true, false},
			{pql.NewDecimal(-10, 0), pql.NewDecimal(0, 0), true, true, false, false, false},
			{pql.NewDecimal(-123456, 3), pql.NewDecimal(-123456, 3), false, true, false, true, true},
			{pql.NewDecimal(-123456, 3), pql.NewDecimal(-123456, 4), true, true, false, false, false},
			{pql.NewDecimal(-123456, 4), pql.NewDecimal(-123456, 3), false, false, true, true, false},
			{pql.NewDecimal(-1233456, 4), pql.NewDecimal(-123456, 3), false, false, true, true, false},

			{pql.NewDecimal(10, 0), pql.NewDecimal(-10, 0), false, false, true, true, false},
			{pql.NewDecimal(-10, 0), pql.NewDecimal(10, 0), true, true, false, false, false},
			{pql.NewDecimal(-123456, 3), pql.NewDecimal(123456, 3), true, true, false, false, false},
			{pql.NewDecimal(123456, 3), pql.NewDecimal(-123456, 3), false, false, true, true, false},
			{pql.NewDecimal(-123456, 3), pql.NewDecimal(123456, 4), true, true, false, false, false},
			{pql.NewDecimal(123456, 3), pql.NewDecimal(-123456, 4), false, false, true, true, false},
			{pql.NewDecimal(-123456, 4), pql.NewDecimal(123456, 3), true, true, false, false, false},
			{pql.NewDecimal(123456, 4), pql.NewDecimal(-123456, 3), false, false, true, true, false},
			{pql.NewDecimal(-1233456, 4), pql.NewDecimal(123456, 3), true, true, false, false, false},
			{pql.NewDecimal(1233456, 4), pql.NewDecimal(-123456, 3), false, false, true, true, false},

			{pql.NewDecimal(9223372036854775807, 0), pql.NewDecimal(9223372036854775807, 0), false, true, false, true, true},
			{pql.NewDecimal(9223372036854775807, 2), pql.NewDecimal(9223372036854775807, 0), true, true, false, false, false},
			{pql.NewDecimal(9223372036854775807, 19), pql.NewDecimal(9223372036854775807, 0), true, true, false, false, false},

			{pql.NewDecimal(-9223372036854775808, 0), pql.NewDecimal(-9223372036854775808, 0), false, true, false, true, true},
			{pql.NewDecimal(-9223372036854775808, 0), pql.NewDecimal(-9223372036854775807, 0), true, true, false, false, false},
			{pql.NewDecimal(-9223372036854775808, 2), pql.NewDecimal(-9223372036854775808, 0), false, false, true, true, false},
			{pql.NewDecimal(-9223372036854775808, 19), pql.NewDecimal(-9223372036854775807, 0), false, false, true, true, false},
		}
		for i, test := range tests {
			if got := test.d1.LessThan(test.d2); got != test.expLT {
				t.Fatalf("test LT %d expected %s < %s to be %v, but got: %v", i, test.d1, test.d2, test.expLT, got)
			}
			if got := test.d1.LessThanOrEqualTo(test.d2); got != test.expLTE {
				t.Fatalf("test LTE %d expected %s <= %s to be %v, but got: %v", i, test.d1, test.d2, test.expLTE, got)
			}
			if got := test.d1.GreaterThan(test.d2); got != test.expGT {
				t.Fatalf("test GT %d expected %s > %s to be %v, but got: %v", i, test.d1, test.d2, test.expGT, got)
			}
			if got := test.d1.GreaterThanOrEqualTo(test.d2); got != test.expGTE {
				t.Fatalf("test GTE %d expected %s >= %s to be %v, but got: %v", i, test.d1, test.d2, test.expGTE, got)
			}
			if got := test.d1.EqualTo(test.d2); got != test.expEQ {
				t.Fatalf("test EQ %d expected %s == %s to be %v, but got: %v", i, test.d1, test.d2, test.expEQ, got)
			}
		}
	})

	t.Run("JSON", func(t *testing.T) {
		t.Run("Unmarshal", func(t *testing.T) {
			tests := []struct {
				json string
				exp  pql.Decimal
			}{
				{"1234.56", pql.NewDecimal(123456, 2)},
			}
			for i, test := range tests {
				b := []byte(test.json)
				dec := &pql.Decimal{}
				if err := json.Unmarshal(b, &dec); err != nil {
					panic(err)
				}
				if !reflect.DeepEqual(*dec, test.exp) {
					t.Fatalf("test %d expected: %T, but got: %T", i, test.exp, dec)
				}
			}
		})
	})
}
