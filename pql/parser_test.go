package pql_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/umbel/pilosa/pql"
)

// Ensure the parser can parse a "Bitmap()" function with keyed args.
func TestParser_Parse_Bitmap_Key(t *testing.T) {
	q, err := pql.ParseString(`Bitmap(id=1, frame="b.n")`)
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(q, &pql.Query{
		Root: &pql.Bitmap{
			ID:    1,
			Frame: "b.n",
		},
	}) {
		t.Fatalf("unexpected query: %s", spew.Sdump(q))
	}
}

// Ensure the parser can parse a "Bitmap()" function with array args.
func TestParser_Parse_Bitmap_Array(t *testing.T) {
	q, err := pql.ParseString(`Bitmap(1, "b.n")`)
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(q, &pql.Query{
		Root: &pql.Bitmap{
			ID:    1,
			Frame: "b.n",
		},
	}) {
		t.Fatalf("unexpected query: %s", spew.Sdump(q))
	}
}

// Ensure the parser can parse a "ClearBit()" function with keyed args.
func TestParser_Parse_ClearBit_Key(t *testing.T) {
	q, err := pql.ParseString(`ClearBit(id=1, frame="b.n", profileID = 3)`)
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(q, &pql.Query{
		Root: &pql.ClearBit{
			ID:        1,
			Frame:     "b.n",
			ProfileID: 3,
		},
	}) {
		t.Fatalf("unexpected query: %s", spew.Sdump(q))
	}
}

// Ensure the parser can parse a "ClearBit()" function with array args.
func TestParser_Parse_ClearBit_Array(t *testing.T) {
	q, err := pql.ParseString(`ClearBit(1, "b.n", 3)`)
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(q, &pql.Query{
		Root: &pql.ClearBit{
			ID:        1,
			Frame:     "b.n",
			ProfileID: 3,
		},
	}) {
		t.Fatalf("unexpected query: %s", spew.Sdump(q))
	}
}

// Ensure the parser can parse a "count()" function.
func TestParser_Parse_Count(t *testing.T) {
	q, err := pql.ParseString(`Count(Bitmap(1))`)
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(q, &pql.Query{
		Root: &pql.Count{
			Input: &pql.Bitmap{
				ID: 1,
			},
		},
	}) {
		t.Fatalf("unexpected query: %s", spew.Sdump(q))
	}
}

// Ensure the parser can parse a "difference()" function.
func TestParser_Parse_Difference(t *testing.T) {
	q, err := pql.ParseString(`Difference(Bitmap(1), Bitmap(2))`)
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(q, &pql.Query{
		Root: &pql.Difference{
			Inputs: pql.BitmapCalls{
				&pql.Bitmap{ID: 1},
				&pql.Bitmap{ID: 2},
			},
		},
	}) {
		t.Fatalf("unexpected query: %s", spew.Sdump(q))
	}
}

// Ensure the parser can parse a "intersect()" function.
func TestParser_Parse_Intersect(t *testing.T) {
	q, err := pql.ParseString(`Intersect(Bitmap(1), Bitmap(2))`)
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(q, &pql.Query{
		Root: &pql.Intersect{
			Inputs: pql.BitmapCalls{
				&pql.Bitmap{ID: 1},
				&pql.Bitmap{ID: 2},
			},
		},
	}) {
		t.Fatalf("unexpected query: %s", spew.Sdump(q))
	}
}

// Ensure the parser can parse a "Profile()" function with keyed args.
func TestParser_Parse_Profile_Key(t *testing.T) {
	q, err := pql.ParseString(`Profile(id=1)`)
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(q, &pql.Query{
		Root: &pql.Profile{ID: 1},
	}) {
		t.Fatalf("unexpected query: %s", spew.Sdump(q))
	}
}

// Ensure the parser can parse a "Profile()" function with array args.
func TestParser_Parse_Profile_Array(t *testing.T) {
	q, err := pql.ParseString(`Profile(1)`)
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(q, &pql.Query{
		Root: &pql.Profile{ID: 1},
	}) {
		t.Fatalf("unexpected query: %s", spew.Sdump(q))
	}
}

// Ensure the parser can parse a "range()" function with keyed args.
func TestParser_Parse_Range_Key(t *testing.T) {
	q, err := pql.ParseString(`Range(start="2000-01-02T03:04", id=20, frame="b.n", end="2001-01-02T03:04")`)
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(q, &pql.Query{
		Root: &pql.Range{
			ID:        20,
			Frame:     "b.n",
			StartTime: time.Date(2000, 1, 2, 3, 4, 0, 0, time.UTC),
			EndTime:   time.Date(2001, 1, 2, 3, 4, 0, 0, time.UTC),
		},
	}) {
		t.Fatalf("unexpected query: %s", spew.Sdump(q))
	}
}

// Ensure the parser can parse a "range()" function with array args.
func TestParser_Parse_Range_Array(t *testing.T) {
	q, err := pql.ParseString(`Range(20, "b.n", "2000-01-02T03:04", "2001-01-02T03:04")`)
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(q, &pql.Query{
		Root: &pql.Range{
			ID:        20,
			Frame:     "b.n",
			StartTime: time.Date(2000, 1, 2, 3, 4, 0, 0, time.UTC),
			EndTime:   time.Date(2001, 1, 2, 3, 4, 0, 0, time.UTC),
		},
	}) {
		t.Fatalf("unexpected query: %s", spew.Sdump(q))
	}
}

// Ensure the parser can parse a "SetBit()" function with keyed args.
func TestParser_Parse_SetBit_Key(t *testing.T) {
	q, err := pql.ParseString(`SetBit(id=1, frame="b.n", profileID = 3)`)
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(q, &pql.Query{
		Root: &pql.SetBit{
			ID:        1,
			Frame:     "b.n",
			ProfileID: 3,
		},
	}) {
		t.Fatalf("unexpected query: %s", spew.Sdump(q))
	}
}

// Ensure the parser can parse a "SetBit()" function with array args.
func TestParser_Parse_SetBit_Array(t *testing.T) {
	q, err := pql.ParseString(`SetBit(1, "b.n", 3)`)
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(q, &pql.Query{
		Root: &pql.SetBit{
			ID:        1,
			Frame:     "b.n",
			ProfileID: 3,
		},
	}) {
		t.Fatalf("unexpected query: %s", spew.Sdump(q))
	}
}

// Ensure the parser can parse a "SetBitmapAttrs()" function with keyed args.
func TestParser_Parse_SetBitmapAttrs_Key(t *testing.T) {
	q, err := pql.ParseString(`SetBitmapAttrs(id=1, frame="b.n", foo="bar", bar=123, baz=true, bat=false, x=null)`)
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(q, &pql.Query{
		Root: &pql.SetBitmapAttrs{
			ID:    1,
			Frame: "b.n",
			Attrs: map[string]interface{}{
				"foo": "bar",
				"bar": uint64(123),
				"baz": true,
				"bat": false,
				"x":   nil,
			},
		},
	}) {
		t.Fatalf("unexpected query: %s", spew.Sdump(q))
	}
}

// Ensure the parser can parse a "SetBitmapAttrs()" function with array args.
func TestParser_Parse_SetBitmapAttrs_Array(t *testing.T) {
	q, err := pql.ParseString(`SetBitmapAttrs(1, "b.n", foo=bar, bar=123)`)
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(q, &pql.Query{
		Root: &pql.SetBitmapAttrs{
			ID:    1,
			Frame: "b.n",
			Attrs: map[string]interface{}{
				"foo": "bar",
				"bar": uint64(123),
			},
		},
	}) {
		t.Fatalf("unexpected query: %s", spew.Sdump(q))
	}
}

// Ensure the parser can parse a "TopN()" function with keyed args.
func TestParser_Parse_TopN_Key(t *testing.T) {
	q, err := pql.ParseString(`TopN(Bitmap(100), frame="b.n", n=2, ids=[1,2,3], field="XXX", [5,10,15])`)
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(q, &pql.Query{
		Root: &pql.TopN{
			Src:       &pql.Bitmap{ID: 100},
			Frame:     "b.n",
			N:         2,
			BitmapIDs: []uint64{1, 2, 3},
			Field:     "XXX",
			Filters:   []interface{}{uint64(5), uint64(10), uint64(15)},
		},
	}) {
		t.Fatalf("unexpected query: %s", spew.Sdump(q))
	}

	if s := q.String(); s != `TopN(Bitmap(id=100), frame=b.n, n=2, ids=[1,2,3], field="XXX", [5,10,15])` {
		t.Fatalf("unexpected string encoding: %s", s)
	}
}

// Ensure the parser can parse a "TopN()" function with array args.
func TestParser_Parse_TopN_Array(t *testing.T) {
	q, err := pql.ParseString(`TopN(Bitmap(100), "b.n", 2, "XXX", ["foo",true,false])`)
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(q, &pql.Query{
		Root: &pql.TopN{
			Src:     &pql.Bitmap{ID: 100},
			Frame:   "b.n",
			N:       2,
			Field:   "XXX",
			Filters: []interface{}{"foo", true, false},
		},
	}) {
		t.Fatalf("unexpected query: %s", spew.Sdump(q))
	}

	if s := q.String(); s != `TopN(Bitmap(id=100), frame=b.n, n=2, field="XXX", ["foo",true,false])` {
		t.Fatalf("unexpected string encoding: %s", s)
	}
}

// Ensure the parser can parse a "union()" function.
func TestParser_Parse_Union(t *testing.T) {
	q, err := pql.ParseString(`Union(Bitmap(1), Bitmap(2))`)
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(q, &pql.Query{
		Root: &pql.Union{
			Inputs: pql.BitmapCalls{
				&pql.Bitmap{ID: 1},
				&pql.Bitmap{ID: 2},
			},
		},
	}) {
		t.Fatalf("unexpected query: %s", spew.Sdump(q))
	}
}
