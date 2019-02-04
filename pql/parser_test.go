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
	_ "github.com/pilosa/pilosa/test"
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
		q, err := pql.ParseString(`MyCall( key= value, foo='bar', age = 12 , bool0=true, bool1=false, x=null, escape="\" \\escape\n\\\\"  )`)
		if err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(q.Calls[0],
			&pql.Call{
				Name: "MyCall",
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

	// Parse with float arguments.
	t.Run("WithFloatArgs", func(t *testing.T) {
		q, err := pql.ParseString(`MyCall( key=12.25, foo= 13.167, bar=2., baz=0.9)`)
		if err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(q.Calls[0],
			&pql.Call{
				Name: "MyCall",
				Args: map[string]interface{}{
					"key": 12.25,
					"foo": 13.167,
					"bar": 2.,
					"baz": 0.9,
				},
			},
		) {
			t.Fatalf("unexpected call: %#v", q.Calls[0])
		}
	})

	// Parse with float arguments.
	t.Run("WithNegativeArgs", func(t *testing.T) {
		q, err := pql.ParseString(`MyCall( key=-12.25, foo= -13)`)
		if err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(q.Calls[0],
			&pql.Call{
				Name: "MyCall",
				Args: map[string]interface{}{
					"key": -12.25,
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
		q, err := pql.ParseString(`MyCall(key=foo, x == 12.25, y >= 100, z >< [4,8], m != null)`)
		if err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(q.Calls[0],
			&pql.Call{
				Name: "MyCall",
				Args: map[string]interface{}{
					"key": "foo",
					"x":   &pql.Condition{Op: pql.EQ, Value: 12.25},
					"y":   &pql.Condition{Op: pql.GTE, Value: int64(100)},
					"z":   &pql.Condition{Op: pql.BETWEEN, Value: []interface{}{int64(4), int64(8)}},
					"m":   &pql.Condition{Op: pql.NEQ, Value: nil},
				},
			},
		) {
			t.Fatalf("unexpected call: %#v", q.Calls[0])
		}
	})

}
