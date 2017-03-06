package pql_test

import (
	"reflect"
	"testing"

	"github.com/pilosa/pilosa/pql"
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
					&pql.Call{Name: "Bitmap"},
					&pql.Call{Name: "Count"},
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
					{Name: "Bitmap", Args: map[string]interface{}{"id": uint64(100)}},
				},
			},
		) {
			t.Fatalf("unexpected call: %s", q.Calls[0])
		}
	})

	// Parse with only arguments.
	t.Run("ArgumentsOnly", func(t *testing.T) {
		q, err := pql.ParseString(`MyCall( key= value, foo="bar", age = 12 , bool0=true, bool1=false, x=null  )`)
		if err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(q.Calls[0],
			&pql.Call{
				Name: "MyCall",
				Args: map[string]interface{}{
					"key":   "value",
					"foo":   "bar",
					"age":   uint64(12),
					"bool0": true,
					"bool1": false,
					"x":     nil,
				},
			},
		) {
			t.Fatalf("unexpected call: %#v", q.Calls[0])
		}
	})

	// Parse with float arguments.
	t.Run("WithFloatArgs", func(t *testing.T) {
		q, err := pql.ParseString(`MyCall( key=12.25, foo= 13.167)`)
		if err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(q.Calls[0],
			&pql.Call{
				Name: "MyCall",
				Args: map[string]interface{}{
					"key": 12.25,
					"foo": 13.167,
				},
			},
		) {
			t.Fatalf("unexpected call: %#v", q.Calls[0])
		}
	})

	// Parse with both child calls and arguments.
	t.Run("ChildrenAndArguments", func(t *testing.T) {
		q, err := pql.ParseString(`TopN(Bitmap(id=100, frame=other), frame=f, n=3)`)
		if err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(q.Calls[0],
			&pql.Call{
				Name: "TopN",
				Children: []*pql.Call{{
					Name: "Bitmap",
					Args: map[string]interface{}{"id": uint64(100), "frame": "other"},
				}},
				Args: map[string]interface{}{"n": uint64(3), "frame": "f"},
			},
		) {
			t.Fatalf("unexpected call: %#v", q.Calls[0])
		}
	})

	// Parse a list argument.
	t.Run("ListArgument", func(t *testing.T) {
		q, err := pql.ParseString(`TopN(frame="f", ids=[0,10,30])`)
		if err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(q.Calls[0],
			&pql.Call{
				Name: "TopN",
				Args: map[string]interface{}{
					"frame": "f",
					"ids":   []interface{}{uint64(0), uint64(10), uint64(30)},
				},
			},
		) {
			t.Fatalf("unexpected call: %#v", q.Calls[0])
		}
	})
}
