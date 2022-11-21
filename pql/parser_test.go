// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package pql_test

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/featurebasedb/featurebase/v3/pql"
	_ "github.com/featurebasedb/featurebase/v3/test"
)

// Ensure the parser can parse PQL.
func TestParser_Parse(t *testing.T) {
	// Parse with no children or arguments.
	t.Run("Empty", func(t *testing.T) {
		q, err := pql.ParseString(`Bitmap()`)
		if err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(q.Calls[0],
			&pql.Call{
				Name: "Bitmap",
			},
		) {
			t.Fatalf("unexpected call: %s", q.Calls[0])
		}
	})

	// Parse with only children.
	t.Run("ChildrenOnly", func(t *testing.T) {
		q, err := pql.ParseString(`Union(  Bitmap()  , Count()  )`)
		if err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(q.Calls[0],
			&pql.Call{
				Name: "Union",
				Children: []*pql.Call{
					{Name: "Bitmap"},
					{Name: "Count"},
				},
			},
		) {
			t.Fatalf("unexpected call: %s", q.Calls[0])
		}
	})

	// Parse a single child with a single argument.
	t.Run("ChildWithArgument", func(t *testing.T) {
		q, err := pql.ParseString(`Count( Bitmap( id=100))`)
		if err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(q.Calls[0],
			&pql.Call{
				Name: "Count",
				Children: []*pql.Call{
					{Name: "Bitmap", Args: map[string]interface{}{"id": int64(100)}},
				},
			},
		) {
			t.Fatalf("unexpected call: %s", q.Calls[0])
		}
	})

	// Parse with only arguments.
	t.Run("ArgumentsOnly", func(t *testing.T) {
		q, err := pql.ParseString(`Row( key= value, foo='bar', age = 12 , bool0=true, bool1=false, x=null, escape="\" \\escape\n\\\\"  )`)
		if err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(q.Calls[0],
			&pql.Call{
				Name: "Row",
				Args: map[string]interface{}{
					"key":    "value",
					"foo":    "bar",
					"age":    int64(12),
					"bool0":  true,
					"bool1":  false,
					"x":      nil,
					"escape": "\" \\escape\n\\\\",
				},
			},
		) {
			t.Fatalf("unexpected call: %#v", q.Calls[0])
		}
	})

	// Parse with decimal arguments.
	t.Run("WithDecimalArgs", func(t *testing.T) {
		q, err := pql.ParseString(`Row( key=12.25, foo= 13.167, bar=2., baz=0.9)`)
		if err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(q.Calls[0],
			&pql.Call{
				Name: "Row",
				Args: map[string]interface{}{
					"key": pql.NewDecimal(1225, 2),
					"foo": pql.NewDecimal(13167, 3),
					"bar": pql.NewDecimal(2, 0),
					"baz": pql.NewDecimal(9, 1),
				},
			},
		) {
			t.Fatalf("unexpected call: %#v", q.Calls[0])
		}
	})

	// Parse with float arguments.
	t.Run("WithNegativeArgs", func(t *testing.T) {
		q, err := pql.ParseString(`Row( key=-12.25, foo= -13)`)
		if err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(q.Calls[0],
			&pql.Call{
				Name: "Row",
				Args: map[string]interface{}{
					"key": pql.NewDecimal(-1225, 2),
					"foo": int64(-13),
				},
			},
		) {
			t.Fatalf("unexpected call: %#v", q.Calls[0])
		}
	})

	// Parse with both child calls and arguments.
	t.Run("ChildrenAndArguments", func(t *testing.T) {
		q, err := pql.ParseString(`TopN(f, Bitmap(id=100, field=other), n=3)`)
		if err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(q.Calls[0],
			&pql.Call{
				Name: "TopN",
				Children: []*pql.Call{{
					Name: "Bitmap",
					Args: map[string]interface{}{"id": int64(100), "field": "other"},
				}},
				Args: map[string]interface{}{"n": int64(3), "_field": "f"},
			},
		) {
			t.Fatalf("unexpected call: %#v", q.Calls[0])
		}
	})

	// Parse a list argument.
	t.Run("ListArgument", func(t *testing.T) {
		q, err := pql.ParseString(`TopN(f, ids=[0,10,30])`)
		if err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(q.Calls[0],
			&pql.Call{
				Name: "TopN",
				Args: map[string]interface{}{
					"_field": "f",
					"ids":    []interface{}{int64(0), int64(10), int64(30)},
				},
			},
		) {
			t.Fatalf("unexpected call: %#v", q.Calls[0])
		}
	})

	// Parse with condition arguments.
	t.Run("WithCondition", func(t *testing.T) {
		q, err := pql.ParseString(`Row(key=foo, x == 12.25, y >= 100, z >< [4,8], m != null, n == null)`)
		if err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(q.Calls[0],
			&pql.Call{
				Name: "Row",
				Args: map[string]interface{}{
					"key": "foo",
					"x":   &pql.Condition{Op: pql.EQ, Value: pql.NewDecimal(1225, 2)},
					"y":   &pql.Condition{Op: pql.GTE, Value: int64(100)},
					"z":   &pql.Condition{Op: pql.BETWEEN, Value: []interface{}{int64(4), int64(8)}},
					"m":   &pql.Condition{Op: pql.NEQ, Value: nil},
					"n":   &pql.Condition{Op: pql.EQ, Value: nil},
				},
			},
		) {
			t.Fatalf("unexpected call: %#v", q.Calls[0])
		}
	})

	t.Run("MixedCase", func(t *testing.T) {
		q, err := pql.ParseString(`roW(x=3)`)
		if err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(q.Calls[0],
			&pql.Call{
				Name: "Row",
				Args: map[string]interface{}{
					"x": int64(3),
				},
			},
		) {
			t.Fatalf("unexpected call: %#v", q.Calls[0])
		}
	})

	t.Run("Timestamp", func(t *testing.T) {
		twos := "2022-02-22T22:22:22Z"
		date, err := time.Parse(time.RFC3339, twos)
		if err != nil {
			t.Fatal(err)
		}
		q, err := pql.ParseString(`Row(x>'2022-02-22T22:22:22Z')`)
		if err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(q.Calls[0],
			&pql.Call{
				Name: "Row",
				Args: map[string]interface{}{
					"x": &pql.Condition{Op: pql.GT, Value: date},
				},
			},
		) {
			t.Fatalf("unexpected call: %#v", q.Calls[0])
		}
		_, err = pql.ParseString(`Row(x>'2024-04-24T24:24:24Z')`)
		if err == nil {
			t.Fatal("no error parsing invalid date")
		} else if !strings.Contains(err.Error(), "not a valid timestamp") {
			t.Fatalf("expected error for invalid timestamp, got: %s", err.Error())
		}
		// str := `Row('2024-04-24T23:24:24Z' < x < '2024-04-24T24:24:24Z')`
		str := `Row('2024-04-24T23:24:24Z' < x < '2024-04-24T24:24:24Z')`
		_, err = pql.ParseString(str)
		if err == nil {
			t.Fatal("no error parsing invalid date")
		} else if !strings.Contains(err.Error(), "not a valid timestamp") {
			t.Fatalf("expected error for invalid timestamp, got: %s on string '%v'", err.Error(), str)
		}
	})

	t.Run("VariousSpaces", func(t *testing.T) {
		q, err := pql.ParseString(`TopN( x )`)
		if err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(q.Calls[0],
			&pql.Call{
				Name: "TopN",
				Args: map[string]interface{}{"_field": "x"},
			},
		) {
			t.Fatalf("unexpected call: %#v", q.Calls[0])
		}
	})
	t.Run("Apply", func(t *testing.T) {
		twos := "2022-02-22T22:22:22Z"
		date, _ := time.Parse(time.RFC3339, twos)
		defaultFilter := `Row(x>'2022-02-22T22:22:22Z')`
		// ivy := `(+/++A+B*B`
		// ivy := `(+/ sqrt(A*A + B*B)/rho x 1-2 ? || )`
		// ivy := `text*here`
		defRes := []*pql.Call{
			{
				Name: "Row",
				Args: map[string]interface{}{
					"x": &pql.Condition{Op: pql.GT, Value: date},
				},
			},
		}
		programs := []struct {
			prog        string
			description string
			filter      string
			children    []*pql.Call
		}{
			{"(+/ sqrt(x*x + y*y))/rho x )", "average distance from x to y", defaultFilter, defRes},
			{"rho sample", "size of input values", defaultFilter, defRes},
			{"(1 drop sample)", " first element removed", defaultFilter, defRes},
			{"(-1 drop sample)", " last element removed", defaultFilter, defRes},
			{"(1 drop sample) - (-1 drop sample)", "subtract those", defaultFilter, defRes},
			{"_ > 0", "which ones of the last difference are greater than zero", defaultFilter, defRes},
			{"+/ _", "sum list from previous calc", defaultFilter, defRes},
			{"(1 drop sample) > (-1 drop sample)", "compare array", defaultFilter, defRes},
			{"1 2 3 +.* 2 3 4", "inner product with + then * and equals 20", defaultFilter, defRes},
			{"(1 drop sample) +.> (-1 drop sample)", "inner product with + then >  ", defaultFilter, defRes},
			{"op inc x = (1 drop sample) +.> (-1 drop sample)", "function of previous  ", defaultFilter, defRes},
			{"op sum3 x = (-2 drop x) +.> (-1 drop 1 drop x) + 2 drop x", "function of previous  ", defaultFilter, defRes},
			{"op grid3 x = 3 ((rho x)-2) rho (-2 drop x), (-1 drop 1 drop x), 2 drop x", "make 3x2 grid from array", defaultFilter, defRes},
			{"x[ up x ]", "sort the array", defaultFilter, defRes},
			{"x[ up x ]", "no filter", "", nil},
		}
		for i := range programs {
			ivy := programs[i].prog
			filter := programs[i].filter
			children := programs[i].children
			qstring := fmt.Sprintf(`Apply(%s,"%s")`, filter, ivy)
			if filter == "" {
				qstring = fmt.Sprintf(`Apply("%s")`, ivy)
			}
			q, err := pql.ParseString(qstring)
			if err != nil {
				t.Fatal(err)
			} else if !reflect.DeepEqual(q.Calls[0],
				&pql.Call{
					Name:     "Apply",
					Children: children,
					Args:     map[string]interface{}{"_ivy": ivy},
				},
			) {
				t.Fatalf("unexpected call: %#v", q.Calls[0])
			}
		}
	})
}

func TestUnquote(t *testing.T) {
	tests := []struct {
		name   string
		value  string
		exp    string
		expErr string
	}{
		{
			name:  "simple double",
			value: `"hello"`,
			exp:   "hello",
		},
		{
			name:  "simple single",
			value: `'hello'`,
			exp:   "hello",
		},
		{
			name:  "double with esc",
			value: `"he\"llo"`,
			exp:   "he\"llo",
		},
		{
			name:  "single with esc",
			value: `'he\'llo'`,
			exp:   "he'llo",
		},
		{
			name:  "single with backslash and esc",
			value: `'he\\\'llo'`,
			exp:   `he\'llo`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := pql.Unquote(test.value)
			if testErr(t, test.expErr, err) {
				return
			}
			if got != test.exp {
				t.Errorf("exp: '%s'\ngot: '%s'", test.exp, got)
			}
		})
	}
}

func testErr(t *testing.T, exp string, actual error) (done bool) {
	t.Helper()
	if exp == "" && actual == nil {
		return false
	}
	if exp == "" && actual != nil {
		t.Fatalf("unexpected error: %v", actual)
	}
	if exp != "" && actual == nil {
		t.Fatalf("expected error like '%s'", exp)
	}
	if !strings.Contains(actual.Error(), exp) {
		t.Fatalf("unmatched errs exp/got\n%s\n%v", exp, actual)
	}
	return true
}
