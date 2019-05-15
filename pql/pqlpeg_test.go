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

import (
	"fmt"
	"reflect"
	"strconv"
	"testing"
)

func TestPEG(t *testing.T) {
	p := PQL{Buffer: `
SetBit(Union(Zitmap(row==4), Intersect(Qitmap(blah>4), Ritmap(field="http://zoo9.com=\\'hello' and \"hello\"")), Hitmap(row=ag-bee)), a="4z", b=5) Count(Union(Witmap(row=5.73, frame=.10), Row(zztop><[2, 9]))) TopN(blah, fields=["hello", "goodbye", "zero"])`[1:]}
	p.Init()
	err := p.Parse()
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}
	p.Execute()

	p = PQL{Buffer: `SetRowAttrs(attr="http://zoo9.com=\\'hello' "and \"hello\"")`}
	p.Init()
	err = p.Parse()
	if err == nil {
		t.Fatalf("should have been an error because of the interior unescaped double quote")
	}

	q, err := ParseString("TopN(blah, Bitmap(id==other), field=f, n=0)")
	if err != nil {
		t.Fatalf("should have parsed: %v", err)
	}
	if q.String() != `TopN(Bitmap(id == "other"), _field="blah", field="f", n=0)` {
		t.Fatalf("Failed, got: %s", q)
	}

	_, err = ParseString("C(a=falsen0)")
	if err != nil {
		t.Fatalf("falsen0 should have been parsed as a string")
	}

	q, err = ParseString("Bitmap(row=4, did==other)")
	if err != nil {
		t.Fatalf("should have parsed: %v", err)
	}

	if q.String() != `Bitmap(did == "other", row=4)` {
		t.Fatalf("got %s", q)
	}

}

func TestOldPQL(t *testing.T) {
	_, err := ParseString(`SetBit(f=11, col=1)`)
	if err != nil {
		t.Fatalf("should have parsed: %v", err)
	}
}

func TestPEGWorking(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		ncalls int
	}{
		{
			name:   "Empty",
			input:  "",
			ncalls: 0},
		{
			name:   "Set",
			input:  "Set(2, f=10)",
			ncalls: 1},
		{
			name:   "SetWithColKeySingleQuote",
			input:  `Set('foo', f=10)`,
			ncalls: 1},
		{
			name:   "SetWithColKeyDoubleQuote",
			input:  `Set("foo", f=10)`,
			ncalls: 1},
		{
			name:   "SetTime",
			input:  "Set(2, f=1, 1999-12-31T00:00)",
			ncalls: 1},
		{
			name:   "DoubleSet",
			input:  "Set(1, a=4)Set(2, a=4)",
			ncalls: 2},
		{
			name:   "DoubleSetSpc",
			input:  "Set(1, a=4) Set(2, a=4)",
			ncalls: 2},
		{
			name:   "DoubleSetNewline",
			input:  "Set(1, a=4) \n Set(2, a=4)",
			ncalls: 2},
		{
			name:   "SetWithArbCall",
			input:  "Set(1, a=4)Blerg(z=ha)",
			ncalls: 2},
		{
			name:   "SetArbSet",
			input:  "Set(1, a=4)Blerg(z=ha)Set(2, z=99)",
			ncalls: 3},
		{
			name:   "ArbSetArb",
			input:  "Arb(q=1, a=4)Set(1, z=9)Arb(z=99)",
			ncalls: 3},
		{
			name:   "SetStringArg",
			input:  "Set(1, a=zoom)",
			ncalls: 1},
		{
			name:   "SetManyArgs",
			input:  "Set(1, a=4, b=5)",
			ncalls: 1},
		{
			name:   "SetManyMixedArgs",
			input:  "Set(1, a=4, bsd=haha)",
			ncalls: 1},
		{
			name:   "SetTimestamp",
			input:  "Set(1, a=4, 2017-04-03T19:34)",
			ncalls: 1},
		{
			name:   "Union()",
			input:  "Union()",
			ncalls: 1},
		{
			name:   "UnionOneRow",
			input:  "Union(Row(a=1))",
			ncalls: 1},
		{
			name:   "UnionTwoRows",
			input:  "Union(Row(a=1), Row(z=44))",
			ncalls: 1},
		{
			name:   "UnionNested",
			input:  "Union(Intersect(Row(), Union(Row(), Row())), Row())",
			ncalls: 1},
		{
			name:   "TopN no args",
			input:  "TopN(boondoggle)",
			ncalls: 1},
		{
			name:   "TopN with args",
			input:  "TopN(boon, doggle=9)",
			ncalls: 1},
		{
			name:   "double quoted args",
			input:  `B(a="zm''e")`,
			ncalls: 1},
		{
			name:   "single quoted args",
			input:  `B(a='zm""e')`,
			ncalls: 1},
		{
			name:   "SetRowAttrs",
			input:  "SetRowAttrs(blah, 9, a=47)",
			ncalls: 1},
		{
			name:   "SetRowAttrs2args",
			input:  "SetRowAttrs(blah, 9, a=47, b=bval)",
			ncalls: 1},
		{
			name:   "SetRowAttrsWithRowKeySingleQuote",
			input:  "SetRowAttrs(blah, 'rowKey', a=47)",
			ncalls: 1},
		{
			name:   "SetRowAttrsWithRowKeyDoubleQuote",
			input:  `SetRowAttrs(blah, "rowKey", a=47)`,
			ncalls: 1},
		{
			name:   "SetColumnAttrs",
			input:  "SetColumnAttrs(9, a=47)",
			ncalls: 1},
		{
			name:   "SetColumnAttrs2args",
			input:  "SetColumnAttrs(9, a=47, b=bval)",
			ncalls: 1},
		{
			name:   "SetColumnAttrsWithColKeySingleQuote",
			input:  "SetColumnAttrs('colKey', a=47)",
			ncalls: 1},
		{
			name:   "SetColumnAttrsWithColKeyDoubleQuote",
			input:  `SetColumnAttrs("colKey", a=47)`,
			ncalls: 1},
		{
			name:   "Clear",
			input:  "Clear(1, a=53)",
			ncalls: 1},
		{
			name:   "Clear2args",
			input:  "Clear(1, a=53, b=33)",
			ncalls: 1},
		{
			name:   "TopN",
			input:  "TopN(myfield, n=44)",
			ncalls: 1},
		{
			name:   "TopNBitmap",
			input:  "TopN(myfield, Row(a=47), n=10)",
			ncalls: 1},
		{
			name:   "RangeLT",
			input:  "Row(a < 4)",
			ncalls: 1},
		{
			name:   "RangeGT",
			input:  "Row(a > 4)",
			ncalls: 1},
		{
			name:   "RangeLTE",
			input:  "Row(a <= 4)",
			ncalls: 1},
		{
			name:   "RangeGTE",
			input:  "Row(a >= 4)",
			ncalls: 1},
		{
			name:   "RangeEQ",
			input:  "Row(a == 4)",
			ncalls: 1},
		{
			name:   "RangeNEQ",
			input:  "Row(a != null)",
			ncalls: 1},
		{
			name:   "RangeLTLT",
			input:  "Row(4 < a < 9)",
			ncalls: 1},
		{
			name:   "RangeLTLTE",
			input:  "Row(4 < a <= 9)",
			ncalls: 1},
		{
			name:   "RangeLTELT",
			input:  "Row(4 <= a < 9)",
			ncalls: 1},
		{
			name:   "RangeLTELTE",
			input:  "Row(4 <= a <= 9)",
			ncalls: 1},
		{
			name:   "RangeTime",
			input:  "Row(a=4, from=2010-07-04T00:00, to=2010-08-04T00:00)",
			ncalls: 1},
		{
			name:   "RangeTimeQuotes",
			input:  `Row(a=4, from='2010-07-04T00:00', to="2010-08-04T00:00")`,
			ncalls: 1},
		{
			name:   "RangeTimeFromQuotes",
			input:  `Row(a=4, from='2010-07-04T00:00')`,
			ncalls: 1},
		{
			name:   "RangeTimeToQuotes",
			input:  `Row(a=4, to="2010-08-04T00:00")`,
			ncalls: 1},
		{
			name:   "Dashed Frame",
			input:  "Set(1, my-frame=9)",
			ncalls: 1},
		{
			name: "newlines",
			input: `Set(
1,
my-frame
=9)`,
			ncalls: 1},
		{
			name:   "OldRange",
			input:  "Range(blah=1, 2019-04-07T00:00, 2019-08-07T00:00)",
			ncalls: 1},
	}

	for i, test := range tests {
		t.Run(test.name+strconv.Itoa(i), func(t *testing.T) {
			q, err := ParseString(test.input)
			if err != nil {
				t.Fatalf("parsing query '%s': %v", test.input, err)
			}
			if len(q.Calls) != test.ncalls {
				t.Fatalf("wrong number of calls for '%s': %#v", test.input, q.Calls)
			}
		})
	}
}

func TestPEGErrors(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "SetNoParens",
			input: "Set"},
		{
			name:  "SetBadTimestamp",
			input: "Set(1, a=4, 2017-94-03T19:34)"},
		{
			name:  "SetTimestampNoArg",
			input: "Set(1, 2017-04-03T19:34)"},
		{
			name:  "SetStartingComma",
			input: "Set(, 1, a=4)"},
		{
			name:  "StartinCommaArb",
			input: "Zeeb(, a=4)"},
		{
			name:  "SetRowAttrs0args",
			input: "SetRowAttrs(blah, 9)"},
		{
			name:  "Clear0args",
			input: "Clear(9)"},
		{
			name:  "RangeTimeGT",
			input: "Row(a>4, 2010-07-04T00:00, 2010-08-04T00:00)"},
		{
			name:  "RangeTimeOneStamp",
			input: "Row(a=4, 2010-07-04T00:00)"},
		{
			name:  "ArgOutOfBounds",
			input: "Row(a=9223372036854775808)"},
		{
			name:  "ArgOutOfBoundsNeg",
			input: "Row(a=-9223372036854775809)"},
	}

	for i, test := range tests {
		t.Run(test.name+strconv.Itoa(i), func(t *testing.T) {
			q, err := ParseString(test.input)
			if err == nil {
				t.Fatalf("parsing query '%s' - expected error, got: %s", test.input, q)
			}
		})
	}
}

func TestPQLDeepEquality(t *testing.T) {
	tests := []struct {
		name string
		call string
		exp  *Call
	}{
		{
			name: "Set",
			call: "Set(1, a=7, 2010-07-08T14:44)",
			exp: &Call{
				Name: "Set",
				Args: map[string]interface{}{
					"a":          int64(7),
					"_col":       int64(1),
					"_timestamp": "2010-07-08T14:44",
				},
			}},
		{
			name: "SetRowAttrs",
			call: "SetRowAttrs(myfield, 9, z=4)",
			exp: &Call{
				Name: "SetRowAttrs",
				Args: map[string]interface{}{
					"z":      int64(4),
					"_field": "myfield",
					"_row":   int64(9),
				},
			}},
		{
			name: "SetRowAttrsWithRowKeySingleQuote",
			call: "SetRowAttrs(myfield, 'rowKey', z=4)",
			exp: &Call{
				Name: "SetRowAttrs",
				Args: map[string]interface{}{
					"z":      int64(4),
					"_field": "myfield",
					"_row":   "rowKey",
				},
			}},
		{
			name: "SetRowAttrsWithRowKeyDoubleQuote",
			call: `SetRowAttrs(myfield, "rowKey", z=4)`,
			exp: &Call{
				Name: "SetRowAttrs",
				Args: map[string]interface{}{
					"z":      int64(4),
					"_field": "myfield",
					"_row":   "rowKey",
				},
			}},
		{
			name: "SetColumnAttrs",
			call: "SetColumnAttrs(9, z=4)",
			exp: &Call{
				Name: "SetColumnAttrs",
				Args: map[string]interface{}{
					"z":    int64(4),
					"_col": int64(9),
				},
			}},
		{
			name: "SetColumnAttrsWithColKeySingleQuote",
			call: "SetColumnAttrs('colKey', z=4)",
			exp: &Call{
				Name: "SetColumnAttrs",
				Args: map[string]interface{}{
					"z":    int64(4),
					"_col": "colKey",
				},
			}},
		{
			name: "SetColumnAttrsWithColKeyDoubleQuote",
			call: `SetColumnAttrs("colKey", z=4)`,
			exp: &Call{
				Name: "SetColumnAttrs",
				Args: map[string]interface{}{
					"z":    int64(4),
					"_col": "colKey",
				},
			}},
		{
			name: "Clear",
			call: "Clear(1, a=7)",
			exp: &Call{
				Name: "Clear",
				Args: map[string]interface{}{
					"a":    int64(7),
					"_col": int64(1),
				},
			}},
		{
			name: "TopN",
			call: "TopN(myfield, Row(), a=7)",
			exp: &Call{
				Name: "TopN",
				Args: map[string]interface{}{
					"a":      int64(7),
					"_field": "myfield",
				},
				Children: []*Call{
					{Name: "Row"},
				},
			}},
		{
			name: "RangeEQ",
			call: "Row(a==7)",
			exp: &Call{
				Name: "Row",
				Args: map[string]interface{}{
					"a": &Condition{
						Op:    EQ,
						Value: int64(7),
					},
				},
			}},
		{
			name: "RangeLT",
			call: "Row(a<7)",
			exp: &Call{
				Name: "Row",
				Args: map[string]interface{}{
					"a": &Condition{
						Op:    LT,
						Value: int64(7),
					},
				},
			}},
		{
			name: "RangeLTE",
			call: "Row(a<=7)",
			exp: &Call{
				Name: "Row",
				Args: map[string]interface{}{
					"a": &Condition{
						Op:    LTE,
						Value: int64(7),
					},
				},
			}},
		{
			name: "RangeGTE",
			call: "Row(a>=7)",
			exp: &Call{
				Name: "Row",
				Args: map[string]interface{}{
					"a": &Condition{
						Op:    GTE,
						Value: int64(7),
					},
				},
			}},
		{
			name: "RangeGT",
			call: "Row(a>7)",
			exp: &Call{
				Name: "Row",
				Args: map[string]interface{}{
					"a": &Condition{
						Op:    GT,
						Value: int64(7),
					},
				},
			}},
		{
			name: "RangeNEQ",
			call: "Row(a!=null)",
			exp: &Call{
				Name: "Row",
				Args: map[string]interface{}{
					"a": &Condition{
						Op:    NEQ,
						Value: nil,
					},
				},
			}},
		{
			name: "RangeLTELT",
			call: "Row(4 <= a < 9)",
			exp: &Call{
				Name: "Row",
				Args: map[string]interface{}{
					"a": &Condition{
						Op:    BETWEEN,
						Value: []interface{}{int64(4), int64(8)},
					},
				},
			}},
		{
			name: "RangeLTLT",
			call: "Row(4 < a < 9)",
			exp: &Call{
				Name: "Row",
				Args: map[string]interface{}{
					"a": &Condition{
						Op:    BETWEEN,
						Value: []interface{}{int64(5), int64(8)},
					},
				},
			}},
		{
			name: "RangeLTELTE",
			call: "Row(4 <= a <= 9)",
			exp: &Call{
				Name: "Row",
				Args: map[string]interface{}{
					"a": &Condition{
						Op:    BETWEEN,
						Value: []interface{}{int64(4), int64(9)},
					},
				},
			}},
		{
			name: "RangeLTLTE",
			call: "Row(4 < a <= 9)",
			exp: &Call{
				Name: "Row",
				Args: map[string]interface{}{
					"a": &Condition{
						Op:    BETWEEN,
						Value: []interface{}{int64(5), int64(9)},
					},
				},
			}},
		{
			name: "Sum",
			call: "Sum(field=f)",
			exp: &Call{
				Name: "Sum",
				Args: map[string]interface{}{
					"field": "f",
				},
			}},
		{
			name: "Weird dash",
			call: "Sum(field-=f)",
			exp: &Call{
				Name: "Sum",
				Args: map[string]interface{}{
					"field-": "f",
				},
			}},
		{
			name: "SumChild",
			call: "Sum(Row(), field=f)",
			exp: &Call{
				Name: "Sum",
				Args: map[string]interface{}{
					"field": "f",
				},
				Children: []*Call{
					{Name: "Row"},
				},
			}},
		{
			name: "MinChild",
			call: "Min(Row(), field=f)",
			exp: &Call{
				Name: "Min",
				Args: map[string]interface{}{
					"field": "f",
				},
				Children: []*Call{
					{Name: "Row"},
				},
			}},
		{
			name: "MaxChild",
			call: "Max(Row(), field=f)",
			exp: &Call{
				Name: "Max",
				Args: map[string]interface{}{
					"field": "f",
				},
				Children: []*Call{
					{Name: "Row"},
				},
			}},
		{
			name: "OptionsWrapper",
			call: "Options(Row(f1=123), excludeRowAttrs=true)",
			exp: &Call{
				Name: "Options",
				Args: map[string]interface{}{
					"excludeRowAttrs": true,
				},
				Children: []*Call{
					{
						Name: "Row",
						Args: map[string]interface{}{
							"f1": int64(123),
						},
					},
				},
			}},
		{
			name: "GroupBy",
			call: "GroupBy(Rows(), filter=Row(a=1))",
			exp: &Call{
				Name: "GroupBy",
				Args: map[string]interface{}{
					"filter": &Call{
						Name: "Row",
						Args: map[string]interface{}{
							"a": int64(1),
						},
					},
				},
				Children: []*Call{
					{Name: "Rows"},
				},
			}},
		{
			name: "GroupByFilterRangeLTLT",
			call: "GroupBy(Rows(), filter=Row(4 < a < 9))",
			exp: &Call{
				Name: "GroupBy",
				Args: map[string]interface{}{
					"filter": &Call{
						Name: "Row",
						Args: map[string]interface{}{
							"a": &Condition{
								Op:    BETWEEN,
								Value: []interface{}{int64(5), int64(8)},
							},
						},
					},
				},
				Children: []*Call{
					{Name: "Rows"},
				},
			}},
	}

	for i, test := range tests {
		t.Run(test.name+strconv.Itoa(i), func(t *testing.T) {
			q, err := ParseString(test.call)
			if err != nil {
				t.Fatalf("parsing query '%s': %v", test.call, err)
			}

			if !reflect.DeepEqual(test.exp, q.Calls[0]) {
				t.Fatalf("unexpected call:\n%s\ninstead of:\n%s\n'%#v'\ninstead of:\n'%#v'", q.Calls[0], test.exp, q.Calls[0], test.exp)
			}
		})
	}
}

func TestDuplicateArgError(t *testing.T) {
	tests := []struct {
		name string
		call string
	}{
		// case 1
		{
			name: "StringConditional",
			call: "Row(a==foo, a==bar)",
		},
		// case 2
		{
			name: "StringValue",
			call: "Row(a=foo, a=bar)",
		},
		// case 3
		{
			name: "IntConditional",
			call: "Row(a>5, a>6)",
		},
		// case 4
		{
			name: "IntValue",
			call: "Row(a=7, a=8)",
		},
		// case 5
		{
			name: "List",
			call: "Row(a=[7], a=[7,8])",
		},
	}
	for i, test := range tests {
		t.Run(test.name+strconv.Itoa(i), func(t *testing.T) {
			_, err := ParseString(test.call)
			expErr := fmt.Sprintf("%s: a", duplicateArgErrorMessage)
			if err == nil {
				t.Fatalf("expected error for duplicate argument: %s", test.call)
			} else if err.Error() != expErr {
				t.Fatalf("expected error: %s, but got: %v", expErr, err.Error())
			}
		})
	}
}
