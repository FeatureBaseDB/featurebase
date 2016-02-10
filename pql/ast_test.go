package pql_test

import (
	"testing"
	"time"

	"github.com/umbel/pilosa/pql"
)

// Ensure the Bitmap call can be converted into a string.
func TestBitmap_String(t *testing.T) {
	s := (&pql.Bitmap{ID: 1, Frame: "x.n"}).String()
	if s != `Bitmap(id=1, frame=x.n)` {
		t.Fatalf("unexpected string: %s", s)
	}
}

// Ensure the ClearBit call can be converted into a string.
func TestClearBit_String(t *testing.T) {
	s := (&pql.ClearBit{ID: 1, Frame: "x.n", ProfileID: 3}).String()
	if s != `ClearBit(id=1, frame=x.n, profileID=3)` {
		t.Fatalf("unexpected string: %s", s)
	}
}

// Ensure the Count call can be converted into a string.
func TestCount_String(t *testing.T) {
	s := (&pql.Count{Input: &pql.Bitmap{ID: 1, Frame: "x.n"}}).String()
	if s != `Count(Bitmap(id=1, frame=x.n))` {
		t.Fatalf("unexpected string: %s", s)
	}
}

// Ensure the Difference call can be converted into a string.
func TestDifference_String(t *testing.T) {
	s := (&pql.Difference{Inputs: pql.BitmapCalls{
		&pql.Bitmap{ID: 1, Frame: "x.n"},
		&pql.Bitmap{ID: 2},
	},
	}).String()
	if s != `Difference(Bitmap(id=1, frame=x.n), Bitmap(id=2))` {
		t.Fatalf("unexpected string: %s", s)
	}
}

// Ensure the Intersect call can be converted into a string.
func TestIntersect_String(t *testing.T) {
	s := (&pql.Intersect{Inputs: pql.BitmapCalls{
		&pql.Bitmap{ID: 1, Frame: "x.n"},
		&pql.Bitmap{ID: 2},
	},
	}).String()
	if s != `Intersect(Bitmap(id=1, frame=x.n), Bitmap(id=2))` {
		t.Fatalf("unexpected string: %s", s)
	}
}

// Ensure the Profile call can be converted into a string.
func TestProfile_String(t *testing.T) {
	if s := (&pql.Profile{ID: 1}).String(); s != `Profile(id=1)` {
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
	if s != `Range(id=1, frame=x.n, start=1970-01-01T00:00, end=2000-01-02T03:04)` {
		t.Fatalf("unexpected string: %s", s)
	}
}

// Ensure the SetBit call can be converted into a string.
func TestSetBit_String(t *testing.T) {
	s := (&pql.SetBit{ID: 1, Frame: "x.n", ProfileID: 3}).String()
	if s != `SetBit(id=1, frame=x.n, profileID=3)` {
		t.Fatalf("unexpected string: %s", s)
	}
}

// Ensure the SetBitmapAttrs call can be converted into a string.
func TestSetBitmapAttrs_String(t *testing.T) {
	s := (&pql.SetBitmapAttrs{ID: 1, Frame: "x.n", Attrs: map[string]interface{}{"foo": "bar", "baz": 123, "bat": true, "x": nil}}).String()
	if s != `SetBitmapAttrs(id=1, frame=x.n, bat=true, baz=123, foo="bar", x=null)` {
		t.Fatalf("unexpected string: %s", s)
	}
}

// Ensure the Union call can be converted into a string.
func TestUnion_String(t *testing.T) {
	s := (&pql.Union{Inputs: pql.BitmapCalls{
		&pql.Bitmap{ID: 1, Frame: "x.n"},
		&pql.Bitmap{ID: 2},
	},
	}).String()
	if s != `Union(Bitmap(id=1, frame=x.n), Bitmap(id=2))` {
		t.Fatalf("unexpected string: %s", s)
	}
}
