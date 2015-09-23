package pql_test

import (
	"reflect"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/umbel/pilosa/pql"
)

// Ensure the parser can parse a "clear()" function with keyed args.
func TestParser_Parse_Clear_Key(t *testing.T) {
	q, err := pql.ParseString(`clear(id=1, frame="b.n", filter=2, profile_id = 3)`)
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(q, &pql.Query{
		Root: &pql.Clear{
			ID:        1,
			Frame:     "b.n",
			Filter:    2,
			ProfileID: 3,
		},
	}) {
		t.Fatalf("unexpected query: %s", spew.Sdump(q))
	}
}

// Ensure the parser can parse a "clear()" function with array args.
func TestParser_Parse_Clear_Array(t *testing.T) {
	q, err := pql.ParseString(`clear(1, "b.n", 2, 3)`)
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(q, &pql.Query{
		Root: &pql.Clear{
			ID:        1,
			Frame:     "b.n",
			Filter:    2,
			ProfileID: 3,
		},
	}) {
		t.Fatalf("unexpected query: %s", spew.Sdump(q))
	}
}

// Ensure the parser can parse a "count()" function.
func TestParser_Parse_Count(t *testing.T) {
	q, err := pql.ParseString(`count(get(1))`)
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(q, &pql.Query{
		Root: &pql.Count{
			Input: &pql.Get{
				ID: 1,
			},
		},
	}) {
		t.Fatalf("unexpected query: %s", spew.Sdump(q))
	}
}

// Ensure the parser can parse a "difference()" function.
func TestParser_Parse_Difference(t *testing.T) {
	q, err := pql.ParseString(`difference(get(1), get(2))`)
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(q, &pql.Query{
		Root: &pql.Difference{
			Inputs: pql.BitmapCalls{
				&pql.Get{ID: 1},
				&pql.Get{ID: 2},
			},
		},
	}) {
		t.Fatalf("unexpected query: %s", spew.Sdump(q))
	}
}

// Ensure the parser can parse a "get()" function with keyed args.
func TestParser_Parse_Get_Key(t *testing.T) {
	q, err := pql.ParseString(`get(id=1, frame="b.n")`)
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(q, &pql.Query{
		Root: &pql.Get{
			ID:    1,
			Frame: "b.n",
		},
	}) {
		t.Fatalf("unexpected query: %s", spew.Sdump(q))
	}
}

// Ensure the parser can parse a "get()" function with array args.
func TestParser_Parse_Get_Array(t *testing.T) {
	q, err := pql.ParseString(`get(1, "b.n")`)
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(q, &pql.Query{
		Root: &pql.Get{
			ID:    1,
			Frame: "b.n",
		},
	}) {
		t.Fatalf("unexpected query: %s", spew.Sdump(q))
	}
}

// Ensure the parser can parse a "intersect()" function.
func TestParser_Parse_Intersect(t *testing.T) {
	q, err := pql.ParseString(`intersect(get(1), get(2))`)
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(q, &pql.Query{
		Root: &pql.Intersect{
			Inputs: pql.BitmapCalls{
				&pql.Get{ID: 1},
				&pql.Get{ID: 2},
			},
		},
	}) {
		t.Fatalf("unexpected query: %s", spew.Sdump(q))
	}
}

// Ensure the parser can parse a "range()" function with keyed args.
func TestParser_Parse_Range_Key(t *testing.T) {
	q, err := pql.ParseString(`range(start="2000-01-02T03:04", id=20, frame="b.n", end="2001-01-02T03:04")`)
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
	q, err := pql.ParseString(`range(20, "b.n", "2000-01-02T03:04", "2001-01-02T03:04")`)
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

// Ensure the parser can parse a "set()" function with keyed args.
func TestParser_Parse_Set_Key(t *testing.T) {
	q, err := pql.ParseString(`set(id=1, frame="b.n", filter=2, profile_id = 3)`)
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(q, &pql.Query{
		Root: &pql.Set{
			ID:        1,
			Frame:     "b.n",
			Filter:    2,
			ProfileID: 3,
		},
	}) {
		t.Fatalf("unexpected query: %s", spew.Sdump(q))
	}
}

// Ensure the parser can parse a "set()" function with array args.
func TestParser_Parse_Set_Array(t *testing.T) {
	q, err := pql.ParseString(`set(1, "b.n", 2, 3)`)
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(q, &pql.Query{
		Root: &pql.Set{
			ID:        1,
			Frame:     "b.n",
			Filter:    2,
			ProfileID: 3,
		},
	}) {
		t.Fatalf("unexpected query: %s", spew.Sdump(q))
	}
}

// Ensure the parser can parse a "top-n()" function with keyed args.
func TestParser_Parse_TopN_Key(t *testing.T) {
	q, err := pql.ParseString(`top-n(frame="b.n", n=2)`)
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(q, &pql.Query{
		Root: &pql.TopN{
			Frame: "b.n",
			N:     2,
		},
	}) {
		t.Fatalf("unexpected query: %s", spew.Sdump(q))
	}
}

// Ensure the parser can parse a "top-n()" function with array args.
func TestParser_Parse_TopN_Array(t *testing.T) {
	q, err := pql.ParseString(`top-n("b.n", 2)`)
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(q, &pql.Query{
		Root: &pql.TopN{
			Frame: "b.n",
			N:     2,
		},
	}) {
		t.Fatalf("unexpected query: %s", spew.Sdump(q))
	}
}

// Ensure the parser can parse a "union()" function.
func TestParser_Parse_Union(t *testing.T) {
	q, err := pql.ParseString(`union(get(1), get(2))`)
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(q, &pql.Query{
		Root: &pql.Union{
			Inputs: pql.BitmapCalls{
				&pql.Get{ID: 1},
				&pql.Get{ID: 2},
			},
		},
	}) {
		t.Fatalf("unexpected query: %s", spew.Sdump(q))
	}
}
