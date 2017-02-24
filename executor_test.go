package pilosa_test

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/pql"
)

// Ensure a bitmap query can be executed.
func TestExecutor_Execute_Bitmap(t *testing.T) {
	idx := MustOpenIndex()
	defer idx.Close()
	idx.MustCreateFragmentIfNotExists("d", "f", 0).MustSetBits(10, 3)
	idx.MustCreateFragmentIfNotExists("d", "f", 1).MustSetBits(10, SliceWidth+1)

	if err := idx.Frame("d", "f").BitmapAttrStore().SetAttrs(10, map[string]interface{}{"foo": "bar", "baz": uint64(123)}); err != nil {
		t.Fatal(err)
	}

	e := NewExecutor(idx.Index, NewCluster(1))
	if res, err := e.Execute(context.Background(), "d", MustParse(`Bitmap(id=10, frame=f)`), nil, nil); err != nil {
		t.Fatal(err)
	} else if bits := res[0].(*pilosa.Bitmap).Bits(); !reflect.DeepEqual(bits, []uint64{3, SliceWidth + 1}) {
		t.Fatalf("unexpected bits: %+v", bits)
	} else if attrs := res[0].(*pilosa.Bitmap).Attrs; !reflect.DeepEqual(attrs, map[string]interface{}{"foo": "bar", "baz": uint64(123)}) {
		t.Fatalf("unexpected attrs: %s", spew.Sdump(attrs))
	}
}

// Ensure a difference query can be executed.
func TestExecutor_Execute_Difference(t *testing.T) {
	idx := MustOpenIndex()
	defer idx.Close()
	idx.MustCreateFragmentIfNotExists("d", "general", 0).MustSetBits(10, 1)
	idx.MustCreateFragmentIfNotExists("d", "general", 0).MustSetBits(10, 2)
	idx.MustCreateFragmentIfNotExists("d", "general", 0).MustSetBits(10, 3)
	idx.MustCreateFragmentIfNotExists("d", "general", 0).MustSetBits(11, 2)
	idx.MustCreateFragmentIfNotExists("d", "general", 0).MustSetBits(11, 4)

	e := NewExecutor(idx.Index, NewCluster(1))
	if res, err := e.Execute(context.Background(), "d", MustParse(`Difference(Bitmap(id=10), Bitmap(id=11))`), nil, nil); err != nil {
		t.Fatal(err)
	} else if bits := res[0].(*pilosa.Bitmap).Bits(); !reflect.DeepEqual(bits, []uint64{1, 3}) {
		t.Fatalf("unexpected bits: %+v", bits)
	}
}

// Ensure an intersect query can be executed.
func TestExecutor_Execute_Intersect(t *testing.T) {
	idx := MustOpenIndex()
	defer idx.Close()
	idx.MustCreateFragmentIfNotExists("d", "general", 0).MustSetBits(10, 1)
	idx.MustCreateFragmentIfNotExists("d", "general", 1).MustSetBits(10, SliceWidth+1)
	idx.MustCreateFragmentIfNotExists("d", "general", 1).MustSetBits(10, SliceWidth+2)

	idx.MustCreateFragmentIfNotExists("d", "general", 0).MustSetBits(11, 1)
	idx.MustCreateFragmentIfNotExists("d", "general", 0).MustSetBits(11, 2)
	idx.MustCreateFragmentIfNotExists("d", "general", 1).MustSetBits(11, SliceWidth+2)

	e := NewExecutor(idx.Index, NewCluster(1))
	if res, err := e.Execute(context.Background(), "d", MustParse(`Intersect(Bitmap(id=10), Bitmap(id=11))`), nil, nil); err != nil {
		t.Fatal(err)
	} else if bits := res[0].(*pilosa.Bitmap).Bits(); !reflect.DeepEqual(bits, []uint64{1, SliceWidth + 2}) {
		t.Fatalf("unexpected bits: %+v", bits)
	}
}

// Ensure a profile query can be executed.
func TestExecutor_Execute_Profile(t *testing.T) {
	idx := MustOpenIndex()
	defer idx.Close()
	idx.MustCreateDBIfNotExists("d").MustSetBit("f", 3, 10, nil)
	idx.MustCreateDBIfNotExists("d").MustSetBit("f", SliceWidth+1, 10, nil)

	if err := idx.DB("d").ProfileAttrStore().SetAttrs(10, map[string]interface{}{"foo": "bar"}); err != nil {
		t.Fatal(err)
	}

	e := NewExecutor(idx.Index, NewCluster(1))
	if res, err := e.Execute(context.Background(), "d", MustParse(`Profile(id=10, frame=f)`), nil, nil); err != nil {
		t.Fatal(err)
	} else if bits := res[0].(*pilosa.Bitmap).Bits(); !reflect.DeepEqual(bits, []uint64{3, SliceWidth + 1}) {
		t.Fatalf("unexpected bits: %+v", bits)
	} else if attrs := res[0].(*pilosa.Bitmap).Attrs; !reflect.DeepEqual(attrs, map[string]interface{}{"foo": "bar"}) {
		t.Fatalf("unexpected attrs: %s", spew.Sdump(attrs))
	}
}

// Ensure a union query can be executed.
func TestExecutor_Execute_Union(t *testing.T) {
	idx := MustOpenIndex()
	defer idx.Close()
	idx.MustCreateFragmentIfNotExists("d", "general", 0).MustSetBits(10, 0)
	idx.MustCreateFragmentIfNotExists("d", "general", 1).MustSetBits(10, SliceWidth+1)
	idx.MustCreateFragmentIfNotExists("d", "general", 1).MustSetBits(10, SliceWidth+2)

	idx.MustCreateFragmentIfNotExists("d", "general", 0).MustSetBits(11, 2)
	idx.MustCreateFragmentIfNotExists("d", "general", 1).MustSetBits(11, SliceWidth+2)

	e := NewExecutor(idx.Index, NewCluster(1))
	if res, err := e.Execute(context.Background(), "d", MustParse(`Union(Bitmap(id=10), Bitmap(id=11))`), nil, nil); err != nil {
		t.Fatal(err)
	} else if bits := res[0].(*pilosa.Bitmap).Bits(); !reflect.DeepEqual(bits, []uint64{0, 2, SliceWidth + 1, SliceWidth + 2}) {
		t.Fatalf("unexpected bits: %+v", bits)
	}
}

// Ensure a count query can be executed.
func TestExecutor_Execute_Count(t *testing.T) {
	idx := MustOpenIndex()
	defer idx.Close()
	idx.MustCreateFragmentIfNotExists("d", "f", 0).MustSetBits(10, 3)
	idx.MustCreateFragmentIfNotExists("d", "f", 1).MustSetBits(10, SliceWidth+1)
	idx.MustCreateFragmentIfNotExists("d", "f", 1).MustSetBits(10, SliceWidth+2)

	e := NewExecutor(idx.Index, NewCluster(1))
	if res, err := e.Execute(context.Background(), "d", MustParse(`Count(Bitmap(id=10, frame=f))`), nil, nil); err != nil {
		t.Fatal(err)
	} else if res[0] != uint64(3) {
		t.Fatalf("unexpected n: %d", res[0])
	}
}

// Ensure a set query can be executed.
func TestExecutor_Execute_SetBit(t *testing.T) {
	idx := MustOpenIndex()
	defer idx.Close()

	e := NewExecutor(idx.Index, NewCluster(1))
	f := idx.MustCreateFragmentIfNotExists("d", "f", 0)
	if n := f.Bitmap(11).Count(); n != 0 {
		t.Fatalf("unexpected bitmap count: %d", n)
	}

	if res, err := e.Execute(context.Background(), "d", MustParse(`SetBit(id=11, frame=f, profileID=1)`), nil, nil); err != nil {
		t.Fatal(err)
	} else {
		if !res[0].(bool) {
			t.Fatalf("expected bit changed")
		}
	}

	if n := f.Bitmap(11).Count(); n != 1 {
		t.Fatalf("unexpected bitmap count: %d", n)
	}
	if res, err := e.Execute(context.Background(), "d", MustParse(`SetBit(id=11, frame=f, profileID=1)`), nil, nil); err != nil {
		t.Fatal(err)
	} else {
		if res[0].(bool) {
			t.Fatalf("expected bit unchanged")
		}
	}
}

// Ensure a SetBitmapAttrs() query can be executed.
func TestExecutor_Execute_SetBitmapAttrs(t *testing.T) {
	idx := MustOpenIndex()
	defer idx.Close()

	// Set two fields on f/10.
	// Also set fields on other bitmaps and frames to test isolation.
	e := NewExecutor(idx.Index, NewCluster(1))
	if _, err := e.Execute(context.Background(), "d", MustParse(`SetBitmapAttrs(id=10, frame=f, foo="bar")`), nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Execute(context.Background(), "d", MustParse(`SetBitmapAttrs(id=200, frame=f, YYY=1)`), nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Execute(context.Background(), "d", MustParse(`SetBitmapAttrs(id=10, frame=XXX, YYY=1)`), nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Execute(context.Background(), "d", MustParse(`SetBitmapAttrs(id=10, frame=f, baz=123, bat=true)`), nil, nil); err != nil {
		t.Fatal(err)
	}

	f := idx.Frame("d", "f")
	if m, err := f.BitmapAttrStore().Attrs(10); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(m, map[string]interface{}{"foo": "bar", "baz": uint64(123), "bat": true}) {
		t.Fatalf("unexpected bitmap attr: %#v", m)
	}
}

// Ensure a TopN() query can be executed.
func TestExecutor_Execute_TopN(t *testing.T) {
	idx := MustOpenIndex()
	defer idx.Close()

	// Set bits for bitmaps 0, 10, & 20 across two slices.
	idx.MustCreateFragmentIfNotExists("d", "f", 0).SetBit(0, 0)
	idx.MustCreateFragmentIfNotExists("d", "f", 0).SetBit(0, 1)
	idx.MustCreateFragmentIfNotExists("d", "f", 1).SetBit(0, SliceWidth)
	idx.MustCreateFragmentIfNotExists("d", "f", 1).SetBit(0, SliceWidth+2)
	idx.MustCreateFragmentIfNotExists("d", "f", 5).SetBit(0, (5*SliceWidth)+100)
	idx.MustCreateFragmentIfNotExists("d", "f", 0).SetBit(10, 0)
	idx.MustCreateFragmentIfNotExists("d", "f", 1).SetBit(10, SliceWidth)
	idx.MustCreateFragmentIfNotExists("d", "f", 0).SetBit(20, SliceWidth)
	idx.MustCreateFragmentIfNotExists("d", "other", 0).SetBit(0, 0)

	// Execute query.
	e := NewExecutor(idx.Index, NewCluster(1))
	if result, err := e.Execute(context.Background(), "d", MustParse(`TopN(frame=f, n=2)`), nil, nil); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(result[0], []pilosa.Pair{
		{Key: 0, Count: 5},
		{Key: 10, Count: 2},
	}) {
		t.Fatalf("unexpected result: %s", spew.Sdump(result))
	}
}
func TestExecutor_Execute_TopN_fill(t *testing.T) {
	idx := MustOpenIndex()
	defer idx.Close()

	// Set bits for bitmaps 0, 10, & 20 across two slices.
	idx.MustCreateFragmentIfNotExists("d", "f", 0).SetBit(0, 0)
	idx.MustCreateFragmentIfNotExists("d", "f", 0).SetBit(0, 1)
	idx.MustCreateFragmentIfNotExists("d", "f", 0).SetBit(0, 2)
	idx.MustCreateFragmentIfNotExists("d", "f", 1).SetBit(0, SliceWidth)
	idx.MustCreateFragmentIfNotExists("d", "f", 1).SetBit(1, SliceWidth+2)
	idx.MustCreateFragmentIfNotExists("d", "f", 1).SetBit(1, SliceWidth)

	// Execute query.
	e := NewExecutor(idx.Index, NewCluster(1))
	if result, err := e.Execute(context.Background(), "d", MustParse(`TopN(frame=f, n=1)`), nil, nil); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(result, []interface{}{[]pilosa.Pair{
		{Key: 0, Count: 4},
	}}) {
		t.Fatalf("unexpected result: %s", spew.Sdump(result))
	}
}

// Ensure a TopN() query with a source bitmap can be executed.
func TestExecutor_Execute_TopN_Src(t *testing.T) {
	idx := MustOpenIndex()
	defer idx.Close()

	// Set bits for bitmaps 0, 10, & 20 across two slices.
	idx.MustCreateFragmentIfNotExists("d", "f", 0).SetBit(0, 0)
	idx.MustCreateFragmentIfNotExists("d", "f", 0).SetBit(0, 1)
	idx.MustCreateFragmentIfNotExists("d", "f", 1).SetBit(0, SliceWidth)
	idx.MustCreateFragmentIfNotExists("d", "f", 1).SetBit(10, SliceWidth)
	idx.MustCreateFragmentIfNotExists("d", "f", 1).SetBit(10, SliceWidth+1)
	idx.MustCreateFragmentIfNotExists("d", "f", 1).SetBit(20, SliceWidth)
	idx.MustCreateFragmentIfNotExists("d", "f", 1).SetBit(20, SliceWidth+1)
	idx.MustCreateFragmentIfNotExists("d", "f", 1).SetBit(20, SliceWidth+2)

	// Create an intersecting bitmap.
	idx.MustCreateFragmentIfNotExists("d", "other", 1).SetBit(100, SliceWidth)
	idx.MustCreateFragmentIfNotExists("d", "other", 1).SetBit(100, SliceWidth+1)
	idx.MustCreateFragmentIfNotExists("d", "other", 1).SetBit(100, SliceWidth+2)

	// Execute query.
	e := NewExecutor(idx.Index, NewCluster(1))
	if result, err := e.Execute(context.Background(), "d", MustParse(`TopN(Bitmap(id=100, frame=other), frame=f, n=3)`), nil, nil); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(result, []interface{}{[]pilosa.Pair{
		{Key: 20, Count: 3},
		{Key: 10, Count: 2},
		{Key: 0, Count: 1},
	}}) {
		t.Fatalf("unexpected result: %s", spew.Sdump(result))
	}
}

// Ensure a range query can be executed.
func TestExecutor_Execute_Range(t *testing.T) {
	idx := MustOpenIndex()
	defer idx.Close()

	db := idx.MustCreateDBIfNotExists("d")
	db.SetTimeQuantum(pilosa.TimeQuantum("YMDH"))
	db.MustSetBit("f", 1, 2, MustParseTimePtr("1999-12-31 00:00"))
	db.MustSetBit("f", 1, 3, MustParseTimePtr("2000-01-01 00:00"))
	db.MustSetBit("f", 1, 4, MustParseTimePtr("2000-01-02 00:00"))
	db.MustSetBit("f", 1, 5, MustParseTimePtr("2000-02-01 00:00"))
	db.MustSetBit("f", 1, 6, MustParseTimePtr("2001-01-01 00:00"))
	db.MustSetBit("f", 1, 7, MustParseTimePtr("2002-01-01 02:00"))

	db.MustSetBit("f", 1, 2, MustParseTimePtr("1999-12-30 00:00"))  // too early
	db.MustSetBit("f", 1, 2, MustParseTimePtr("2002-02-01 00:00"))  // too late
	db.MustSetBit("f", 10, 2, MustParseTimePtr("2001-01-01 00:00")) // different bitmap

	e := NewExecutor(idx.Index, NewCluster(1))
	if res, err := e.Execute(context.Background(), "d", MustParse(`Range(id=1, frame=f, start="1999-12-31T00:00", end="2002-01-01T03:00")`), nil, nil); err != nil {
		t.Fatal(err)
	} else if bits := res[0].(*pilosa.Bitmap).Bits(); !reflect.DeepEqual(bits, []uint64{2, 3, 4, 5, 6, 7}) {
		t.Fatalf("unexpected bits: %+v", bits)
	}
}

// Ensure a remote query can return a bitmap.
func TestExecutor_Execute_Remote_Bitmap(t *testing.T) {
	c := NewCluster(2)

	// Create secondary server and update second cluster node.
	s := NewServer()
	defer s.Close()
	c.Nodes[1].Host = s.Host()

	// Mock secondary server's executor to verify arguments and return a bitmap.
	s.Handler.Executor.ExecuteFn = func(ctx context.Context, db string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		if db != `d` {
			t.Fatalf("unexpected db: %s", db)
		} else if query.String() != `Bitmap(frame="f", id=10)` {
			t.Fatalf("unexpected query: %s", query.String())
		} else if !reflect.DeepEqual(slices, []uint64{0}) { //TODO: this is incorrect because the calling node doesn't know about slice 2
			t.Fatalf("unexpected slices: %+v", slices)
		}

		// Set bits in slice 0 & 2.
		bm := pilosa.NewBitmap(
			(0*SliceWidth)+1,
			(0*SliceWidth)+2,
			(2*SliceWidth)+4,
		)
		return []interface{}{bm}, nil
	}

	// Create local executor data.
	// The local node owns slice 1.
	idx := MustOpenIndex()
	defer idx.Close()
	idx.MustCreateFragmentIfNotExists("d", "f", 1).MustSetBits(10, (1*SliceWidth)+1)

	e := NewExecutor(idx.Index, c)
	if res, err := e.Execute(context.Background(), "d", MustParse(`Bitmap(id=10, frame=f)`), nil, nil); err != nil {
		t.Fatal(err)
	} else if bits := res[0].(*pilosa.Bitmap).Bits(); !reflect.DeepEqual(bits, []uint64{1, 2, (1 * SliceWidth) + 1, 2*SliceWidth + 4}) {
		t.Fatalf("unexpected bits: %+v", bits)
	}
}

// Ensure a remote query can return a count.
func TestExecutor_Execute_Remote_Count(t *testing.T) {
	c := NewCluster(2)

	// Create secondary server and update second cluster node.
	s := NewServer()
	defer s.Close()
	c.Nodes[1].Host = s.Host()

	// Mock secondary server's executor to return a count.
	s.Handler.Executor.ExecuteFn = func(ctx context.Context, db string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		return []interface{}{uint64(10)}, nil
	}

	// Create local executor data. The local node owns slice 1.
	idx := MustOpenIndex()
	defer idx.Close()
	idx.MustCreateFragmentIfNotExists("d", "f", 1).MustSetBits(10, (1*SliceWidth)+1)
	idx.MustCreateFragmentIfNotExists("d", "f", 1).MustSetBits(10, (1*SliceWidth)+2)

	e := NewExecutor(idx.Index, c)
	if res, err := e.Execute(context.Background(), "d", MustParse(`Count(Bitmap(id=10, frame=f))`), nil, nil); err != nil {
		t.Fatal(err)
	} else if res[0] != uint64(12) {
		t.Fatalf("unexpected n: %d", res[0])
	}
}

// Ensure a remote query can set bits on multiple nodes.
func TestExecutor_Execute_Remote_SetBit(t *testing.T) {
	c := NewCluster(2)
	c.ReplicaN = 2

	// Create secondary server and update second cluster node.
	s := NewServer()
	defer s.Close()
	c.Nodes[1].Host = s.Host()

	// Mock secondary server's executor to verify arguments.
	var remoteCalled bool
	s.Handler.Executor.ExecuteFn = func(ctx context.Context, db string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		if db != `d` {
			t.Fatalf("unexpected db: %s", db)
		} else if query.String() != `SetBit(frame="f", id=10, profileID=2)` {
			t.Fatalf("unexpected query: %s", query.String())
		}
		remoteCalled = true
		return []interface{}{nil}, nil
	}

	// Create local executor data.
	idx := MustOpenIndex()
	defer idx.Close()

	e := NewExecutor(idx.Index, c)
	if _, err := e.Execute(context.Background(), "d", MustParse(`SetBit(id=10, frame=f, profileID=2)`), nil, nil); err != nil {
		t.Fatal(err)
	}

	// Verify that one bit is set on both node's index.
	if n := idx.MustCreateFragmentIfNotExists("d", "f", 0).Bitmap(10).Count(); n != 1 {
		t.Fatalf("unexpected local count: %d", n)
	}
	if !remoteCalled {
		t.Fatalf("expected remote execution")
	}
}

// Ensure a remote query can set bits on multiple nodes.
func TestExecutor_Execute_Remote_SetBit_With_Timestamp(t *testing.T) {
	c := NewCluster(2)
	c.ReplicaN = 2

	// Create secondary server and update second cluster node.
	s := NewServer()
	defer s.Close()
	c.Nodes[1].Host = s.Host()

	// Mock secondary server's executor to verify arguments.
	var remoteCalled bool
	s.Handler.Executor.ExecuteFn = func(ctx context.Context, db string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		if db != `d` {
			t.Fatalf("unexpected db: %s", db)
		} else if query.String() != `SetBit(frame="f", id=10, profileID=2, timestamp="2016-12-11T10:09:07")` {
			t.Fatalf("unexpected query: %s", query.String())
		}
		remoteCalled = true
		return []interface{}{nil}, nil
	}

	// Create local executor data.
	idx := MustOpenIndex()
	defer idx.Close()
	idx.CreateDBIfNotExists("d")
	oldQuantum := idx.DB("d").TimeQuantum()
	defer func() {
		// restore db quantum
		idx.DB("d").SetTimeQuantum(oldQuantum)
	}()
	// need to set the quantum otherwise SetBit fails silently
	idx.DB("d").SetTimeQuantum("Y")

	e := NewExecutor(idx.Index, c)
	if _, err := e.Execute(context.Background(), "d", MustParse(`SetBit(id=10, frame=f, profileID=2, timestamp="2016-12-11T10:09:07")`), nil, nil); err != nil {
		t.Fatal(err)
	}

	// Verify that one bit is set on both node's index.
	if n := idx.MustCreateFragmentIfNotExists("d", "f_2016", 0).Bitmap(10).Count(); n != 1 {
		t.Fatalf("unexpected local count: %d", n)
	}
	if !remoteCalled {
		t.Fatalf("expected remote execution")
	}
}

// Ensure a remote query can return a top-n query.
func TestExecutor_Execute_Remote_TopN(t *testing.T) {
	c := NewCluster(2)

	// Create secondary server and update second cluster node.
	s := NewServer()
	defer s.Close()
	c.Nodes[1].Host = s.Host()

	// Mock secondary server's executor to verify arguments and return a bitmap.
	var remoteExecN int
	s.Handler.Executor.ExecuteFn = func(ctx context.Context, db string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		if db != `d` {
			t.Fatalf("unexpected db: %s", db)
		} else if !reflect.DeepEqual(slices, []uint64{0, 2}) {
			t.Fatalf("unexpected slices: %+v", slices)
		}

		// Query should be executed twice. Once to get the top bitmaps for the
		// slices and a second time to get the counts for a set of bitmaps.
		switch remoteExecN {
		case 0:
			if query.String() != `TopN(frame="f", n=3)` {
				t.Fatalf("unexpected query(0): %s", query.String())
			}
		case 1:
			if query.String() != `TopN(frame="f", ids=[0,10,30], n=0)` {
				t.Fatalf("unexpected query(1): %s", query.String())
			}
		default:
			t.Fatalf("too many remote exec calls")
		}
		remoteExecN++

		// Return pair counts.
		return []interface{}{[]pilosa.Pair{
			{Key: 0, Count: 5},
			{Key: 10, Count: 2},
			{Key: 30, Count: 2},
		}}, nil
	}

	// Create local executor data on slice 1 & 3.
	idx := MustOpenIndex()
	defer idx.Close()
	idx.MustCreateFragmentIfNotExists("d", "f", 1).MustSetBits(30, (1*SliceWidth)+1)
	idx.MustCreateFragmentIfNotExists("d", "f", 3).MustSetBits(30, (3*SliceWidth)+2)

	e := NewExecutor(idx.Index, c)
	if res, err := e.Execute(context.Background(), "d", MustParse(`TopN(frame=f, n=3)`), nil, nil); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(res, []interface{}{[]pilosa.Pair{
		{Key: 0, Count: 5},
		{Key: 30, Count: 4},
		{Key: 10, Count: 2},
	}}) {
		t.Fatalf("unexpected results: %s", spew.Sdump(res))
	}
}

// Executor represents a test wrapper for pilosa.Executor.
type Executor struct {
	*pilosa.Executor
}

// NewExecutor returns a new instance of Executor.
// The executor always matches the hostname of the first cluster node.
func NewExecutor(index *pilosa.Index, cluster *pilosa.Cluster) *Executor {
	e := &Executor{Executor: pilosa.NewExecutor()}
	e.Index = index
	e.Cluster = cluster
	e.Host = cluster.Nodes[0].Host
	return e
}

// MustParse parses s into a PQL query. Panic on error.
func MustParse(s string) *pql.Query {
	q, err := pql.NewParser(strings.NewReader(s)).Parse()
	if err != nil {
		panic(err)
	}
	return q
}
