package pql

import (
	"strconv"
	"testing"
)

func TestPEG(t *testing.T) {
	p := PQL{Buffer: `
SetBit(Union(Zitmap(row==4), Intersect(Qitmap(blah>4), Ritmap(field="http://zoo9.com=\\'hello' and \"hello\"")), Hitmap(row=ag-bee)), a="4z", b=5) Count(Union(Witmap(row=5.73, frame=.10), Range(zztop><[2, 9]))) TopN(blah, fields=["hello", "goodbye", "zero"])`[1:]}
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

	q, err = ParseString("C(a=falsen0)")
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
			input:  "Set(1, a=4)",
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
			name:  "SetEmpty",
			input: "Set()"},
		{
			name:  "SetNoCol",
			input: "Set(a=4)"},
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
			name:  "SetRowAttrsNoField",
			input: "SetRowAttrs(a=4)"},
		{
			name:  "SetColAttrsNoField",
			input: "SetColAttrs(a=4)"},
		{
			name:  "ClearNoCol",
			input: "Clear(a=4)"},
		{
			name:  "SetStartingComma",
			input: "Set(, 1, a=4)"},
		{
			name:  "StartinCommaArb",
			input: "Zeeb(, a=4)"},
		{
			name:  "TopN No Field",
			input: "TopN(a=77)"},
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
