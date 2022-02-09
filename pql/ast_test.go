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
		{pql.BETWEEN, []interface{}{pql.Decimal{Value: -401, Scale: 2}, pql.Decimal{Value: 802, Scale: 1}}, "-4.01<=subj<=80.2"},
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
			name:   "ExpandRowEQExterior",
			input:  `row(animal=$var1)`,
			output: `Union(Row(animal="cat"))`,
			vars:   map[string]interface{}{"var1": []interface{}{"cat"}},
		},
		{
			name:   "ExpandRowGT",
			input:  `count(row(num>$var1))`,
			output: `Count(Union(Row(num>5), Row(num>10)))`,
			vars:   map[string]interface{}{"var1": []interface{}{5, 10}},
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
			name:   "ExpandRowsExterior",
			input:  `rows($var1)`,
			output: `Rows(_field="cat")` + "\n" + `Rows(_field="dog")`,
			vars:   map[string]interface{}{"var1": []interface{}{"cat", "dog"}},
		},
		{
			name:   "ExpandRowAndRows",
			input:  `GroupBy(Rows($animal), limit=7, filter=Row(size=$size))`,
			output: `GroupBy(Rows(_field="cat"), Rows(_field="dog"), filter=Union(Row(size="lg"), Row(size="md")), limit=7)`,
			vars:   map[string]interface{}{"animal": []interface{}{"cat", "dog"}, "size": []interface{}{"lg", "md"}},
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
			name:   "ExpandPercentile",
			input:  `Percentile(field="bytes", nth=99.0, filter=Row(level=$animal))`,
			output: `Percentile(field="bytes", filter=Union(Row(level="cat"), Row(level="dog")), nth=99)`,
			vars:   map[string]interface{}{"animal": []interface{}{"cat", "dog"}, "columns": []interface{}{5, 10}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := pql.NewParser(strings.NewReader(tt.input)).Parse()
			if err != nil {
				if !tt.wantErr {
					t.Errorf("Parse error = %v, wantErr %v", err, tt.wantErr)
				}
				return
			}

			got, err := q.ExpandVars(tt.vars)
			if err != nil {
				t.Errorf("Query.ExpandVars() error = %v", err)
				return
			}
			if tt.output != got.String() {
				t.Errorf("got %v, want %v", got, tt.output)
			}
		})
	}
}
