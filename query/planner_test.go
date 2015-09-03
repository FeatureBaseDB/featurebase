package query_test

import (
	"reflect"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/umbel/pilosa/db"
	"github.com/umbel/pilosa/query"
	"github.com/umbel/pilosa/util"
)

// Ensure the query planner can plan a "get" query.
func TestQueryPlanner_Plan_Get(t *testing.T) {
	q, err := query.QueryForPQL("get(10,default)")
	if err != nil {
		t.Fatal(err)
	}

	d, frag1 := NewDefaultDB()
	planner := query.QueryPlanner{Database: d, Query: q}

	id := util.RandomUUID()
	plan, err := planner.Plan(q, &id, frag1.GetLocation())
	if err != nil {
		t.Fatal(err)
	}

	if n := len(*plan); n != 3 {
		t.Fatalf("unexpected plan length: %d", n)
	}

	if step := (*plan)[0].(query.GetQueryStep); step.Operation != "get" {
		t.Fatalf("unexpected step(0) operation: %s", step.Operation)
	} else if step.Slice != 0 {
		t.Fatalf("unexpected step(0) slice: %d", step.Slice)
	} else if !reflect.DeepEqual(step.Bitmap, &db.Bitmap{Id: 10, FrameType: "default", Filter: 0}) {
		t.Fatalf("unexpected step(0) bitmap: %s", spew.Sprint(step.Bitmap))
	}

	if step := (*plan)[1].(query.GetQueryStep); step.Operation != "get" {
		t.Fatalf("unexpected step(1) operation: %s", step.Operation)
	} else if step.Slice != 1 {
		t.Fatalf("unexpected step(1) slice: %d", step.Slice)
	} else if !reflect.DeepEqual(step.Bitmap, &db.Bitmap{Id: 10, FrameType: "default", Filter: 0}) {
		t.Fatalf("unexpected step(1) bitmap: %s", spew.Sprint(step.Bitmap))
	}

	if step := (*plan)[2].(query.CatQueryStep); step.Operation != "cat" {
		t.Fatalf("unexpected step(2) operation: %s", step.Operation)
	} else if !reflect.DeepEqual(step.Inputs, []*pilosa.GUID{
		(*plan)[0].(query.GetQueryStep).Id,
		(*plan)[1].(query.GetQueryStep).Id,
	}) {
		t.Fatalf("unexpected step(2) inputs: %s", spew.Sprint(step.Inputs))
	}
}

// Ensure the query planner can plan a "set" query.
func TestQueryPlanner_Plan_Set(t *testing.T) {
	q, err := query.QueryForPQL("set(10, default, 0, 100)")
	if err != nil {
		t.Fatal(err)
	}

	d, frag1 := NewDefaultDB()
	planner := query.QueryPlanner{Database: d, Query: q}

	id := util.RandomUUID()
	plan, err := planner.Plan(q, &id, frag1.GetLocation())
	if err != nil {
		t.Fatal(err)
	}

	if n := len(*plan); n != 1 {
		t.Fatalf("unexpected plan length: %d", n)
	}

	if step := (*plan)[0].(query.SetQueryStep); step.Operation != "set" {
		t.Fatalf("unexpected step(0) operation: %s", step.Operation)
	} else if step.ProfileId != 100 {
		t.Fatalf("unexpected step(0) profile id: %d", step.ProfileId)
	} else if !reflect.DeepEqual(step.Bitmap, &db.Bitmap{Id: 10, FrameType: "default", Filter: 0}) {
		t.Fatalf("unexpected step(0) bitmap: %s", spew.Sprint(step.Bitmap))
	}
}

// Ensure the query planner can plan a "top-n" query.
func TestQueryPlanner_Plan_TopN(t *testing.T) {
	q, err := query.QueryForPQL("top-n(get(10, default), default, 50,[1,2,3])")
	if err != nil {
		t.Fatal(err)
	}

	d, frag1 := NewDefaultDB()
	planner := query.QueryPlanner{Database: d, Query: q}

	id := util.RandomUUID()
	plan, err := planner.Plan(q, &id, frag1.GetLocation())
	if err != nil {
		t.Fatal(err)
	}

	if n := len(*plan); n != 5 {
		t.Fatalf("unexpected plan length: %d", n)
	}

	if step := (*plan)[0].(query.GetQueryStep); step.Operation != "get" {
		t.Fatalf("unexpected step(0) operation: %s", step.Operation)
	} else if !reflect.DeepEqual(step.Bitmap, &db.Bitmap{Id: 10, FrameType: "default", Filter: 0}) {
		t.Fatalf("unexpected step(0) bitmap: %s", spew.Sprint(step.Bitmap))
	}

	if step := (*plan)[1].(*query.TopNQueryStep); step.Operation != "top-n" {
		t.Fatalf("unexpected step(1) operation: %s", step.Operation)
	} else if step.Input != (*plan)[0].(query.GetQueryStep).Id {
		t.Fatalf("unexpected step(1) input: %d", step.Input)
	} else if step.N != 50 {
		t.Fatalf("unexpected step(1) n: %d", step.N)
	} else if !reflect.DeepEqual(step.Filters, []uint64{1, 2, 3}) {
		t.Fatalf("unexpected step(1) filters: %s", spew.Sprint(step.Filters))
	}

	if step := (*plan)[2].(query.GetQueryStep); step.Operation != "get" {
		t.Fatalf("unexpected step(2) operation: %s", step.Operation)
	} else if !reflect.DeepEqual(step.Bitmap, &db.Bitmap{Id: 10, FrameType: "default", Filter: 0}) {
		t.Fatalf("unexpected step(2) bitmap: %s", spew.Sprint(step.Bitmap))
	}

	if step := (*plan)[3].(*query.TopNQueryStep); step.Operation != "top-n" {
		t.Fatalf("unexpected step(3) operation: %s", step.Operation)
	} else if step.Input != (*plan)[2].(query.GetQueryStep).Id {
		t.Fatalf("unexpected step(3) input: %d", step.Input)
	} else if step.N != 50 {
		t.Fatalf("unexpected step(3) n: %d", step.N)
	}
}

// Ensure the query planner can plan a "top-n" all() query.
func TestQueryPlanner_Plan_TopN_All(t *testing.T) {
	q, err := query.QueryForPQL("top-n(all(), default, 50, [1,2,3])")
	if err != nil {
		t.Fatal(err)
	}

	d, frag1 := NewDefaultDB()
	planner := query.QueryPlanner{Database: d, Query: q}

	id := util.RandomUUID()
	plan, err := planner.Plan(q, &id, frag1.GetLocation())
	if err != nil {
		t.Fatal(err)
	}

	if n := len(*plan); n != 3 {
		t.Fatalf("unexpected plan length: %d", n)
	}

	if step := (*plan)[0].(*query.TopNQueryStep); step.Operation != "top-n" {
		t.Fatalf("unexpected step(0) operation: %s", step.Operation)
	} else if step.Input != nil {
		t.Fatalf("unexpected step(0) input: %v", step.Input)
	} else if step.N != 50 {
		t.Fatalf("unexpected step(0) n: %d", step.N)
	} else if !reflect.DeepEqual(step.Filters, []uint64{1, 2, 3}) {
		t.Fatalf("unexpected step(0) filters: %s", spew.Sprint(step.Filters))
	}
}

// Ensure the query planner can plan a "union" query.
func TestQueryPlanner_Plan_Union(t *testing.T) {
	id1 := util.RandomUUID()
	q1 := query.Query{Id: &id1, Operation: "get", Args: map[string]interface{}{"id": uint64(10), "frame": "default"}}

	id2 := util.RandomUUID()
	q2 := query.Query{Id: &id2, Operation: "get", Args: map[string]interface{}{"id": uint64(20), "frame": "default"}}

	id3 := util.RandomUUID()
	q := query.Query{Id: &id3, Operation: "union", Subqueries: []query.Query{q1, q2}}

	d, frag1 := NewDefaultDB()
	planner := query.QueryPlanner{Database: d, Query: &q}

	id := util.RandomUUID()
	plan, err := planner.Plan(&q, &id, frag1.GetLocation())
	if err != nil {
		t.Fatal(err)
	}

	if n := len(*plan); n != 7 {
		t.Fatalf("unexpected plan size: %d", n)
	}

	// First step should be a "get" step.
	if step := (*plan)[0].(query.GetQueryStep); step.Operation != "get" {
		t.Fatalf("unexpected step(0) operation: %s", step.Operation)
	} else if step.Slice != 0 {
		t.Fatalf("unexpected step(0) slice: %d", step.Slice)
	} else if !reflect.DeepEqual(step.Bitmap, &db.Bitmap{Id: 10, FrameType: "default", Filter: 0}) {
		t.Fatalf("unexpected step(0) bitmap: %s", spew.Sprint(step.Bitmap))
	}

	// Second step should also be a "get" step.
	if step := (*plan)[1].(query.GetQueryStep); step.Operation != "get" {
		t.Fatalf("unexpected step(1) operation: %s", step.Operation)
	} else if step.Slice != 0 {
		t.Fatalf("unexpected step(1) slice: %d", step.Slice)
	} else if !reflect.DeepEqual(step.Bitmap, &db.Bitmap{Id: 20, FrameType: "default", Filter: 0}) {
		t.Fatalf("unexpected step(1) bitmap: %s", spew.Sprint(step.Bitmap))
	}

	// Third step should union the first two steps.
	if step := (*plan)[2].(query.UnionQueryStep); step.Operation != "union" {
		t.Fatalf("unexpected step(2) operation: %s", step.Operation)
	} else if !reflect.DeepEqual(step.Inputs, []*pilosa.GUID{
		(*plan)[0].(query.GetQueryStep).Id,
		(*plan)[1].(query.GetQueryStep).Id,
	}) {
		t.Fatalf("unexpected step(2) inputs: %s", spew.Sprint(step.Inputs))
	}

	// Fourth step should be a "get" step.
	if step := (*plan)[3].(query.GetQueryStep); step.Operation != "get" {
		t.Fatalf("unexpected step(3) operation: %s", step.Operation)
	} else if step.Slice != 1 {
		t.Fatalf("unexpected step(3) slice: %d", step.Slice)
	} else if !reflect.DeepEqual(step.Bitmap, &db.Bitmap{Id: 10, FrameType: "default", Filter: 0}) {
		t.Fatalf("unexpected step(3) bitmap: %s", spew.Sprint(step.Bitmap))
	}

	// Fifth step should also be a "get" step.
	if step := (*plan)[4].(query.GetQueryStep); step.Operation != "get" {
		t.Fatalf("unexpected step(4) operation: %s", step.Operation)
	} else if step.Slice != 1 {
		t.Fatalf("unexpected step(4) slice: %d", step.Slice)
	} else if !reflect.DeepEqual(step.Bitmap, &db.Bitmap{Id: 20, FrameType: "default", Filter: 0}) {
		t.Fatalf("unexpected step(4) bitmap: %s", spew.Sprint(step.Bitmap))
	}

	// Sixth step should union the previous two steps.
	if step := (*plan)[5].(query.UnionQueryStep); step.Operation != "union" {
		t.Fatalf("unexpected step(5) operation: %s", step.Operation)
	} else if !reflect.DeepEqual(step.Inputs, []*pilosa.GUID{
		(*plan)[3].(query.GetQueryStep).Id,
		(*plan)[4].(query.GetQueryStep).Id,
	}) {
		t.Fatalf("unexpected step(5) inputs: %s", spew.Sprint(step.Inputs))
	}

	// Final step should concatenate the two union steps.
	if step := (*plan)[6].(query.CatQueryStep); step.Operation != "cat" {
		t.Fatalf("unexpected step(6) operation: %s", step.Operation)
	} else if !reflect.DeepEqual(step.Inputs, []*pilosa.GUID{
		(*plan)[2].(query.UnionQueryStep).Id,
		(*plan)[5].(query.UnionQueryStep).Id,
	}) {
		t.Fatalf("unexpected step(6) inputs: %s", spew.Sprint(step.Inputs))
	}
}

// NewDefaultDB returns a simple, initialized database and fragment.
func NewDefaultDB() (*db.Database, *db.Fragment) {
	// Create an empty database
	c := db.NewCluster()
	d := c.GetOrCreateDatabase("main")
	f := d.GetOrCreateFrame("default")

	s1 := d.GetOrCreateSlice(0)
	frag1 := d.GetOrCreateFragment(f, s1, util.Id())
	pid := util.RandomUUID()
	p1 := db.NewProcess(&pid)
	p1.SetHost("----192.1.1.0----")
	frag1.SetProcess(p1)

	s2 := d.GetOrCreateSlice(1)
	frag2 := d.GetOrCreateFragment(f, s2, util.Id())
	pid = util.RandomUUID()
	p2 := db.NewProcess(&pid)
	p2.SetHost("----192.1.1.1----")
	frag2.SetProcess(p2)

	return d, frag1
}
