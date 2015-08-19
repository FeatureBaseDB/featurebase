package query_test

import (
	"reflect"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/umbel/pilosa/query"
)

// Ensure the parser can parse a get() query.
func TestParser_Parse_Get(t *testing.T) {
	if q, err := query.Parse(MustLex("get(10)")); err != nil {
		t.Fatal(err)
	} else if q.Operation != "get" {
		t.Fatalf("unexpected operation: %q", q.Operation)
	} else if !reflect.DeepEqual(q.Args, map[string]interface{}{
		"id":    uint64(10),
		"frame": "general",
	}) {
		t.Fatalf("unexpected args:\n\n%s", spew.Sprint(q.Args))
	}
}

// Ensure the parser can parse a get() query with a keyword.
func TestParser_Parse_Get_Keyword(t *testing.T) {
	if q, err := query.Parse(MustLex("get(id=10)")); err != nil {
		t.Fatal(err)
	} else if q.Operation != "get" {
		t.Fatalf("unexpected operation: %q", q.Operation)
	} else if !reflect.DeepEqual(q.Args, map[string]interface{}{"id": uint64(10), "frame": "general"}) {
		t.Fatalf("unexpected args:\n\n%s", spew.Sprint(q.Args))
	}
}

// Ensure the parser can parse a get() query with a keyword.
func TestParser_Parse_Get_MultiKeyword(t *testing.T) {
	if q, err := query.Parse(MustLex("get(id=10, frame=brands)")); err != nil {
		t.Fatal(err)
	} else if q.Operation != "get" {
		t.Fatalf("unexpected operation: %q", q.Operation)
	} else if !reflect.DeepEqual(q.Args, map[string]interface{}{"id": uint64(10), "frame": "brands"}) {
		t.Fatalf("unexpected args:\n\n%s", spew.Sprint(q.Args))
	}
}

// Ensure the parser can parse a clear() query.
func TestParser_Parse_Clear(t *testing.T) {
	if q, err := query.Parse(MustLex("clear(10, general, 0, 20)")); err != nil {
		t.Fatal(err)
	} else if q.Operation != "clear" {
		t.Fatalf("unexpected operation: %q", q.Operation)
	} else if !reflect.DeepEqual(q.Args, map[string]interface{}{
		"id":         uint64(10),
		"frame":      "general",
		"filter":     uint64(0),
		"profile_id": uint64(20),
	}) {
		t.Fatalf("unexpected args:\n\n%s", spew.Sprint(q.Args))
	}
}

// Ensure the parser can parse a set() query.
func TestParser_Parse_Set(t *testing.T) {
	if q, err := query.Parse(MustLex("set(10, general, 0, 20)")); err != nil {
		t.Fatal(err)
	} else if q.Operation != "set" {
		t.Fatalf("unexpected operation: %q", q.Operation)
	} else if !reflect.DeepEqual(q.Args, map[string]interface{}{
		"id":         uint64(10),
		"frame":      "general",
		"filter":     uint64(0),
		"profile_id": uint64(20),
	}) {
		t.Fatalf("unexpected args:\n\n%s", spew.Sprint(q.Args))
	}
}

// Ensure the parser can parse a nested query.
func TestParser_Parse_Nested(t *testing.T) {
	q, err := query.Parse(MustLex("union(get(10,general), get(11,brand), get(12))"))
	if err != nil {
		t.Fatal(err)
	} else if q.Operation != "union" {
		t.Fatalf("unexpected operation: %q", q.Operation)
	} else if len(q.Subqueries) != 3 {
		t.Fatalf("unexpected subquery count: %d", len(q.Subqueries))
	}

	if sq := q.Subqueries[0]; sq.Operation != "get" {
		t.Fatalf("unexpected subquery(0) operation: %q", sq.Operation)
	} else if !reflect.DeepEqual(sq.Args, map[string]interface{}{"id": uint64(10), "frame": "general"}) {
		t.Fatalf("unexpected subquery(0) args:\n\n%s", spew.Sprint(q.Args))
	}

	if sq := q.Subqueries[1]; sq.Operation != "get" {
		t.Fatalf("unexpected subquery(1) operation: %q", sq.Operation)
	} else if !reflect.DeepEqual(sq.Args, map[string]interface{}{"id": uint64(11), "frame": "brand"}) {
		t.Fatalf("unexpected subquery(1) args:\n\n%s", spew.Sprint(sq.Args))
	}

	if sq := q.Subqueries[2]; sq.Operation != "get" {
		t.Fatalf("unexpected subquery(2) operation: %q", sq.Operation)
	} else if !reflect.DeepEqual(sq.Args, map[string]interface{}{"id": uint64(12), "frame": "general"}) {
		t.Fatalf("unexpected subquery(2) args:\n\n%s", spew.Sprint(sq.Args))
	}
}

// Ensure the parser can parse a query with lists.
func TestParser_Parse_Lists(t *testing.T) {
	q, err := query.Parse(MustLex("top-n(get(10, general), [1,2,3], 50)"))
	if err != nil {
		t.Fatal(err)
	} else if q.Operation != "top-n" {
		t.Fatalf("unexpected operation: %q", q.Operation)
	} else if !reflect.DeepEqual(q.Args, map[string]interface{}{"ids": []uint64{1, 2, 3}, "n": 50}) {
		t.Fatalf("unexpected args:\n\n%s", spew.Sprint(q.Args))
	}

	if sq := q.Subqueries[0]; sq.Operation != "get" {
		t.Fatalf("unexpected subquery(0) operation: %q", sq.Operation)
	} else if !reflect.DeepEqual(sq.Args, map[string]interface{}{"id": uint64(10), "frame": "general"}) {
		t.Fatalf("unexpected subquery(0) args:\n\n%s", spew.Sprint(q.Args))
	}
}

// Ensure the parser can parse "all()".
func TestParser_Parse_All(t *testing.T) {
	q, err := query.Parse(MustLex("top-n(all(), general, 30)"))
	if err != nil {
		t.Fatal(err)
	} else if q.Operation != "top-n" {
		t.Fatalf("unexpected operation: %q", q.Operation)
	}

	if sq := q.Subqueries[0]; sq.Operation != "all" {
		t.Fatalf("unexpected subquery(0) operation: %q", sq.Operation)
	}
}

// Ensure the parser can parse bracketed lists.
func TestParser_Parse_Lists_Bracketed(t *testing.T) {
	q, err := query.Parse(MustLex("plugin(get(99), [get(10), get(11)])"))
	if err != nil {
		t.Fatalf("expected error")
	}
	spew.Dump(q)
}

// Ensure the parser can parse a recall query.
func TestParser_Parse_Recall(t *testing.T) {
	q, err := query.Parse(MustLex("recall(12345,1,12345,2,12345,3)"))
	if err != nil {
		t.Fatal(err)
	}
	spew.Dump(q)
}
