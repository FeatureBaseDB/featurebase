// Copyright 2021 Molecula Corp. All rights reserved.
package pql_test

import (
	"strings"
	"testing"

	"github.com/molecula/featurebase/v3/pql"
)

// Ensure call can be converted into a string.
func TestCall_String(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		c := &pql.Call{Name: "Bitmap"}
		if s := c.String(); s != `Bitmap()` {
			t.Fatalf("unexpected string: %s", s)
		}
	})
	t.Run("With Args", func(t *testing.T) {
		c := &pql.Call{
			Name: "Range",
			Args: map[string]interface{}{
				"other":  "f",
				"field0": &pql.Condition{Op: pql.GTE, Value: 10},
			},
		}
		if s := c.String(); s != `Range(field0>=10, other="f")` {
			t.Fatalf("unexpected string: %s", s)
		}
	})
}

// Ensure condition string with subject is correct.
func TestCondition_StringWithSubj(t *testing.T) {
	subj := "subj"
	for _, tt := range []struct {
		op  pql.Token
		val interface{}
		exp string
	}{
		{pql.BETWEEN, []interface{}{int64(4), int64(8)}, "4<=subj<=8"},
		{pql.BETWEEN, []interface{}{uint64(5), uint64(9)}, "5<=subj<=9"},
		{pql.BETWEEN, []interface{}{pql.NewDecimal(-401, 2), pql.NewDecimal(802, 1)}, "-4.01<=subj<=80.2"},
		{pql.EQ, nil, "subj==null"},
		{pql.NEQ, nil, "subj!=null"},
	} {
		c := &pql.Condition{
			Op:    tt.op,
			Value: tt.val,
		}
		if sws := c.StringWithSubj(subj); sws != tt.exp {
			t.Fatalf("invalid between string. expected: %s, got %s", tt.exp, sws)
		}
	}
}

func TestQuery_ExpandVars(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		output  string
		vars    map[string]interface{}
		wantErr bool
	}{
		{
			name:   "ExpandRowEQInterior",
			input:  `count(row(animal=$var1))`,
			output: `Count(Union(Row(animal="cat"), Row(animal="dog"), Row(animal="pig")))`,
			vars:   map[string]interface{}{"var1": []interface{}{"cat", "dog", "pig"}},
		},
		{
			name:   "ExpandRowEQInterior-NoValues",
			input:  `count(row(animal=$var1))`,
			output: `Count(All())`,
			vars:   map[string]interface{}{"var1": []interface{}{}},
		},
		{
			name:   "ExpandRowEQExterior",
			input:  `row(animal=$var1)`,
			output: `Union(Row(animal="cat"))`,
			vars:   map[string]interface{}{"var1": []interface{}{"cat"}},
		},
		{
			name:   "ExpandRowEQExterior-NoValues",
			input:  `row(animal=$var1)`,
			output: `All()`,
			vars:   map[string]interface{}{"var1": []interface{}{}},
		},
		{
			name:   "ExpandRowGT",
			input:  `count(row(num>$var1))`,
			output: `Count(Union(Row(num>5), Row(num>10)))`,
			vars:   map[string]interface{}{"var1": []interface{}{5, 10}},
		},
		{
			name:   "ExpandRowGT-NoValues",
			input:  `count(row(num>$var1))`,
			output: `Count(All())`,
			vars:   map[string]interface{}{"var1": []interface{}{}},
		},
		{
			name:   "ExpandRowNOT",
			input:  `count(row(num!=$var1))`,
			output: `Count(Union(Row(num!=5), Row(num!=10)))`,
			vars:   map[string]interface{}{"var1": []interface{}{5, 10}},
		},
		{
			name:   "ExpandRowLTString",
			input:  `count(row(num<$var1))`,
			output: `Count(Union(Row(num<"cat"), Row(num<"dog")))`,
			vars:   map[string]interface{}{"var1": []interface{}{"cat", "dog"}},
		},
		{
			name:   "ExpandRowsInterior",
			input:  `GroupBy(rows($var1), limit=5)`,
			output: `GroupBy(Rows(_field="cat"), Rows(_field="dog"), limit=5)`,
			vars:   map[string]interface{}{"var1": []interface{}{"cat", "dog"}},
		},
		{
			name:    "ExpandRowsInterior-NoValues",
			input:   `GroupBy(rows($var1), limit=5)`,
			output:  `GroupBy(), limit=5)`,
			vars:    map[string]interface{}{"var1": []interface{}{}},
			wantErr: true,
		},
		{
			name:   "ExpandRowsExterior",
			input:  `rows($var1)`,
			output: `Rows(_field="cat")` + "\n" + `Rows(_field="dog")`,
			vars:   map[string]interface{}{"var1": []interface{}{"cat", "dog"}},
		},
		{
			name:    "ExpandRowsExterior-NoValues",
			input:   `rows($var1)`,
			output:  ``,
			vars:    map[string]interface{}{"var1": []interface{}{}},
			wantErr: true,
		},
		{
			name:   "ExpandRowAndRows",
			input:  `GroupBy(Rows($animal), limit=7, filter=Row(size=$size))`,
			output: `GroupBy(Rows(_field="cat"), Rows(_field="dog"), filter=Union(Row(size="lg"), Row(size="md")), limit=7)`,
			vars:   map[string]interface{}{"animal": []interface{}{"cat", "dog"}, "size": []interface{}{"lg", "md"}},
		},
		{
			name:    "ExpandRowAndRows-NoValues1",
			input:   `GroupBy(Rows($animal), limit=7, filter=Row(size=$size))`,
			output:  `GroupBy(filter=All(), limit=7)`,
			vars:    map[string]interface{}{"animal": []interface{}{}, "size": []interface{}{}},
			wantErr: true,
		},
		{
			name:   "ExpandRowAndRows-NoValues2",
			input:  `GroupBy(Rows($animal), limit=7, filter=Row(size=$size))`,
			output: `GroupBy(Rows(_field="cat"), Rows(_field="dog"), filter=All(), limit=7)`,
			vars:   map[string]interface{}{"animal": []interface{}{"cat", "dog"}, "size": []interface{}{}},
		},
		{
			name:    "ExpandRowAndRows-NoValues3",
			input:   `GroupBy(Rows($animal), limit=7, filter=Row(size=$size))`,
			output:  `GroupBy(filter=Union(Row(size="lg"), Row(size="md")), limit=7)`,
			vars:    map[string]interface{}{"animal": []interface{}{}, "size": []interface{}{"lg", "md"}},
			wantErr: true,
		},
		{
			name:    "ExpandBad",
			input:   `$animal`,
			vars:    map[string]interface{}{"animal": []interface{}{"cat", "dog"}, "columns": []interface{}{5, 10}},
			wantErr: true,
		},
		{
			name:    "ExpandBad2",
			input:   `GroupBy($animal)`,
			output:  `Intersect(ConstRow(columns=[5, 10]), Union(Row(animal="cat"), Row(animal="dog")))`,
			wantErr: true,
		},
		{
			name:   "ExpandAsCSV",
			input:  `Intersect(ConstRow(columns=$var2), Row(animal=$var1))`,
			output: `Intersect(ConstRow(columns=[5,10]), Union(Row(animal="cat"), Row(animal="dog")))`,
			vars:   map[string]interface{}{"var1": []interface{}{"cat", "dog"}, "var2": []interface{}{5, 10}},
		},
		{
			name:   "ExpandAsCSV-NoValues",
			input:  `Intersect(ConstRow(columns=$var2), Row(animal=$var1))`,
			output: `Intersect(ConstRow(columns=[]), All())`,
			vars:   map[string]interface{}{"var1": []interface{}{}, "var2": []interface{}{}},
		},
		{
			name:   "ExpandPercentile",
			input:  `Percentile(field="bytes", nth=99.0, filter=Row(level=$animal))`,
			output: `Percentile(field="bytes", filter=Union(Row(level="cat"), Row(level="dog")), nth=99)`,
			vars:   map[string]interface{}{"animal": []interface{}{"cat", "dog"}, "columns": []interface{}{5, 10}},
		},
		{
			name:   "ExpandPercentile-NoValues",
			input:  `Percentile(field="bytes", nth=99.0, filter=Row(level=$animal))`,
			output: `Percentile(field="bytes", filter=All(), nth=99)`,
			vars:   map[string]interface{}{"animal": []interface{}{}, "columns": []interface{}{}},
		},
		{
			name:   "Union-NoValues-1",
			input:  `Count(Union(row(x=$var1), row(y=$var2)), limit=5)`,
			output: `Count(Union(All(), Union(Row(y="cat"), Row(y="dog"))), limit=5)`,
			vars:   map[string]interface{}{"var1": []interface{}{}, "var2": []interface{}{"cat", "dog"}},
		},
		{
			name:    "Rows-NoValues-1",
			input:   `groupby(rows($var1))`,
			output:  `GroupBy()`,
			vars:    map[string]interface{}{"var1": []interface{}{}},
			wantErr: true,
		},
		{
			name:   "Extract-NoValues-1",
			input:  `Extract(Limit(Row(animals=$var1), limit=1000), Rows($var2))`,
			output: `Extract(Limit(All(), limit=1000))`,
			vars:   map[string]interface{}{"var1": []interface{}{}, "var2": []interface{}{}},
		},
		{
			name:   "Extract-NoValues-2",
			input:  `Extract(Limit(Row(animals=$var1), limit=1000), Rows($var2))`,
			output: `Extract(Limit(Union(Row(animals="a"), Row(animals="b")), limit=1000))`,
			vars:   map[string]interface{}{"var1": []interface{}{"a", "b"}, "var2": []interface{}{}},
		},
		{
			name:   "Extract-NoValues-3",
			input:  `Extract(Limit(Row(animals=$var1), limit=1000), Rows($var2))`,
			output: `Extract(Limit(All(), limit=1000), Rows(_field="a"), Rows(_field="b"))`,
			vars:   map[string]interface{}{"var1": []interface{}{}, "var2": []interface{}{"a", "b"}},
		},
		{
			name:   "Extract-SomeValues-1",
			input:  `Extract(Union(Row(x=$var1), Row(y=$var2)), Rows(a), Rows(b))`,
			output: `Extract(Union(Union(Row(x="cat"), Row(x="dog")), All()), Rows(_field="a"), Rows(_field="b"))`,
			vars:   map[string]interface{}{"var1": []interface{}{"cat", "dog"}, "var2": []interface{}{}},
		},
		{
			name:   "GroupBy-SomeValues-1",
			input:  `GroupBy(Rows(a), Rows(b), filter=Union(Row(x=$var1), Row(y=$var2)), limit=10)`,
			output: `GroupBy(Rows(_field="a"), Rows(_field="b"), filter=Union(All(), Union(Row(y="cat"), Row(y="dog"))), limit=10)`,
			vars:   map[string]interface{}{"var1": []interface{}{}, "var2": []interface{}{"cat", "dog"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := pql.NewParser(strings.NewReader(tt.input)).Parse()
			if err != nil {
				if !tt.wantErr {
					t.Errorf("Parse error = %v", err)
				}
				return
			}

			got, err := q.ExpandVars(tt.vars)
			if err != nil {
				if !tt.wantErr {
					t.Errorf("Query.ExpandVars() error = %v", err)
				}
				return
			}
			if tt.output != got.String() && !tt.wantErr {
				t.Errorf("got %v, want %v", got, tt.output)
			}
			if tt.wantErr {
				t.Error("test succeeded, but expected error")
			}
		})
	}
}
