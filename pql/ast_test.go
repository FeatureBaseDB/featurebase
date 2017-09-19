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
	"reflect"
	"testing"

	"github.com/pilosa/pilosa/pql"
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
				"frame":  "f",
				"field0": &pql.Condition{Op: pql.GTE, Value: 10},
			},
		}
		if s := c.String(); s != `Range(field0 >= 10, frame="f")` {
			t.Fatalf("unexpected string: %s", s)
		}
	})
}

// Ensure condition can handle values for BETWEEN operator.
func TestCondition_Value(t *testing.T) {
	t.Run("Between Values", func(t *testing.T) {
		for _, tt := range []struct {
			val []interface{}
			exp []int64
		}{
			{[]interface{}{int64(4), int64(8)}, []int64{4, 8}},
			{[]interface{}{uint64(4), uint64(8)}, []int64{4, 8}},
			{[]interface{}{uint64(1), uint64(2), uint64(3)}, []int64{1, 2, 3}},
		} {
			c := &pql.Condition{
				Op:    pql.BETWEEN,
				Value: tt.val,
			}
			v, err := c.IntSliceValue()
			if err != nil {
				t.Fatal(err)
			} else if !reflect.DeepEqual(v, tt.exp) {
				t.Fatalf("invalid between values. expected: %v, got %v", tt.exp, v)
			}
		}
	})
}

// Ensure call can be converted into a string.
func TestCall_SupportsInverse(t *testing.T) {
	t.Run("Bitmap", func(t *testing.T) {
		q, err := pql.ParseString(`Bitmap()`)
		if err != nil {
			t.Fatal(err)
		} else if q.Calls[0].SupportsInverse() != true {
			t.Fatalf("call should support inverse: %s", q.Calls[0])
		}
	})
	t.Run("Count Bitmap", func(t *testing.T) {
		q, err := pql.ParseString(`Count(Bitmap())`)
		if err != nil {
			t.Fatal(err)
		} else if q.Calls[0].SupportsInverse() == true {
			t.Fatalf("call should not support inverse: %s", q.Calls[0])
		}
	})
	t.Run("Union Bitmaps", func(t *testing.T) {
		q, err := pql.ParseString(`Union(Bitmap(), Bitmap())`)
		if err != nil {
			t.Fatal(err)
		} else if q.Calls[0].SupportsInverse() == true {
			t.Fatalf("call should not support inverse: %s", q.Calls[0])
		}
	})

}

// Ensure call is correctly determined to be against an inverse view.
func TestCall_IsInverse(t *testing.T) {
	t.Run("Bitmap Row", func(t *testing.T) {
		q, err := pql.ParseString(`Bitmap(frame="f", row=1)`)
		if err != nil {
			t.Fatal(err)
		} else if q.Calls[0].IsInverse("row", "col") != false {
			t.Fatalf("incorrect call inverse: %s", q.Calls[0])
		}
	})
	t.Run("Bitmap Column", func(t *testing.T) {
		q, err := pql.ParseString(`Bitmap(frame="f", col=1)`)
		if err != nil {
			t.Fatal(err)
		} else if q.Calls[0].IsInverse("row", "col") != true {
			t.Fatalf("incorrect call inverse: %s", q.Calls[0])
		}
	})
	t.Run("Bitmap Column No Label", func(t *testing.T) {
		q, err := pql.ParseString(`Bitmap(frame="f", col=1)`)
		if err != nil {
			t.Fatal(err)
		} else if q.Calls[0].IsInverse("rowX", "colX") != false {
			t.Fatalf("incorrect call inverse: %s", q.Calls[0])
		}
	})
	t.Run("Count", func(t *testing.T) {
		q, err := pql.ParseString(`Count(Bitmap(frame="f", col=1))`)
		if err != nil {
			t.Fatal(err)
		} else if q.Calls[0].IsInverse("row", "col") != false {
			t.Fatalf("incorrect call inverse: %s", q.Calls[0])
		}
	})

}
