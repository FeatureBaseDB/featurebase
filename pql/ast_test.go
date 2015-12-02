package pql_test

import (
	"testing"
	"time"

	"github.com/umbel/pilosa/pql"
)

// Ensure the Clear call can be converted into a string.
func TestClear_String(t *testing.T) {
	s := (&pql.Clear{ID: 1, Frame: "x.n", Filter: 2, ProfileID: 3}).String()
	if s != `clear(id=1, frame=x.n, filter=2, profile_id=3)` {
		t.Fatalf("unexpected string: %s", s)
	}
}

// Ensure the Count call can be converted into a string.
func TestCount_String(t *testing.T) {
	s := (&pql.Count{Input: &pql.Get{ID: 1, Frame: "x.n"}}).String()
	if s != `count(get(id=1, frame=x.n))` {
		t.Fatalf("unexpected string: %s", s)
	}
}

// Ensure the Difference call can be converted into a string.
func TestDifference_String(t *testing.T) {
	s := (&pql.Difference{Inputs: pql.BitmapCalls{
		&pql.Get{ID: 1, Frame: "x.n"},
		&pql.Get{ID: 2},
	},
	}).String()
	if s != `difference(get(id=1, frame=x.n), get(id=2))` {
		t.Fatalf("unexpected string: %s", s)
	}
}

// Ensure the Get call can be converted into a string.
func TestGet_String(t *testing.T) {
	s := (&pql.Get{ID: 1, Frame: "x.n"}).String()
	if s != `get(id=1, frame=x.n)` {
		t.Fatalf("unexpected string: %s", s)
	}
}

// Ensure the Intersect call can be converted into a string.
func TestIntersect_String(t *testing.T) {
	s := (&pql.Intersect{Inputs: pql.BitmapCalls{
		&pql.Get{ID: 1, Frame: "x.n"},
		&pql.Get{ID: 2},
	},
	}).String()
	if s != `intersect(get(id=1, frame=x.n), get(id=2))` {
		t.Fatalf("unexpected string: %s", s)
	}
}

// Ensure the Range call can be converted into a string.
func TestRange_String(t *testing.T) {
	s := (&pql.Range{
		ID:        1,
		Frame:     "x.n",
		StartTime: time.Unix(0, 0).UTC(),
		EndTime:   time.Date(2000, 1, 2, 3, 4, 0, 0, time.UTC),
	}).String()
	if s != `range(id=1, frame=x.n, start=1970-01-01T00:00, end=2000-01-02T03:04)` {
		t.Fatalf("unexpected string: %s", s)
	}
}

// Ensure the Set call can be converted into a string.
func TestSet_String(t *testing.T) {
	s := (&pql.Set{ID: 1, Frame: "x.n", Filter: 2, ProfileID: 3}).String()
	if s != `set(id=1, frame=x.n, filter=2, profile_id=3)` {
		t.Fatalf("unexpected string: %s", s)
	}
}

// Ensure the Union call can be converted into a string.
func TestUnion_String(t *testing.T) {
	s := (&pql.Union{Inputs: pql.BitmapCalls{
		&pql.Get{ID: 1, Frame: "x.n"},
		&pql.Get{ID: 2},
	},
	}).String()
	if s != `union(get(id=1, frame=x.n), get(id=2))` {
		t.Fatalf("unexpected string: %s", s)
	}
}
