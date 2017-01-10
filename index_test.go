package pilosa_test

import (
	"bytes"
	"context"
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/internal"
	"github.com/pilosa/pilosa/pql"
)

// Ensure index can delete a database and its underlying files.
func TestIndex_DeleteDB(t *testing.T) {
	idx := MustOpenIndex()
	defer idx.Close()

	// Write bits to separate databases.
	f0 := idx.MustCreateFragmentIfNotExists("d0", "f", 0)
	if _, err := f0.SetBit(100, 200); err != nil {
		t.Fatal(err)
	}
	f1 := idx.MustCreateFragmentIfNotExists("d1", "f", 0)
	if _, err := f1.SetBit(100, 200); err != nil {
		t.Fatal(err)
	}

	// Ensure d0 exists.
	if _, err := os.Stat(idx.DBPath("d0")); err != nil {
		t.Fatal(err)
	}

	// Delete d0.
	if err := idx.DeleteDB("d0"); err != nil {
		t.Fatal(err)
	}

	// Ensure d0 files are removed & d1 still exists.
	if _, err := os.Stat(idx.DBPath("d0")); !os.IsNotExist(err) {
		t.Fatal("expected d0 file deletion")
	} else if _, err := os.Stat(idx.DBPath("d1")); err != nil {
		t.Fatal("expected d1 files to still exist", err)
	}
}

// Ensure index can sync with a remote index.
func TestIndexSyncer_SyncIndex(t *testing.T) {
	cluster := NewCluster(2)

	// Create a local index.
	idx0 := MustOpenIndex()
	defer idx0.Close()

	// Create a remote index wrapped by an HTTP
	idx1 := MustOpenIndex()
	defer idx1.Close()
	s := NewServer()
	defer s.Close()
	s.Handler.Index = idx1.Index
	s.Handler.Executor.ExecuteFn = func(ctx context.Context, db string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		e := pilosa.NewExecutor()
		e.Index = idx1.Index
		e.Host = cluster.Nodes[1].Host
		e.Cluster = cluster
		return e.Execute(ctx, db, query, slices, opt)
	}

	// Mock 2-node, fully replicated cluster.
	cluster.ReplicaN = 2
	cluster.Nodes[0].Host = "localhost:0"
	cluster.Nodes[1].Host = MustParseURLHost(s.URL)

	// Set data on the local index.
	f := idx0.MustCreateFragmentIfNotExists("d", "f", 0)
	if _, err := f.SetBit(0, 10); err != nil {
		t.Fatal(err)
	} else if _, err := f.SetBit(2, 20); err != nil {
		t.Fatal(err)
	} else if _, err := f.SetBit(120, 10); err != nil {
		t.Fatal(err)
	} else if _, err := f.SetBit(200, 4); err != nil {
		t.Fatal(err)
	}

	f = idx0.MustCreateFragmentIfNotExists("d", "f0", 1)
	if _, err := f.SetBit(9, SliceWidth+5); err != nil {
		t.Fatal(err)
	}

	idx0.MustCreateFragmentIfNotExists("y", "z", 0)

	// Set data on the remote index.
	f = idx1.MustCreateFragmentIfNotExists("d", "f", 0)
	if _, err := f.SetBit(0, 4000); err != nil {
		t.Fatal(err)
	} else if _, err := f.SetBit(3, 10); err != nil {
		t.Fatal(err)
	} else if _, err := f.SetBit(120, 10); err != nil {
		t.Fatal(err)
	}

	f = idx1.MustCreateFragmentIfNotExists("y", "z", 3)
	if _, err := f.SetBit(10, (3*SliceWidth)+4); err != nil {
		t.Fatal(err)
	} else if _, err := f.SetBit(10, (3*SliceWidth)+5); err != nil {
		t.Fatal(err)
	} else if _, err := f.SetBit(10, (3*SliceWidth)+7); err != nil {
		t.Fatal(err)
	}

	// Set highest slice.
	idx0.DB("d").SetRemoteMaxSlice(1)
	idx0.DB("y").SetRemoteMaxSlice(3)

	// Set up syncer.
	syncer := pilosa.IndexSyncer{
		Index:   idx0.Index,
		Host:    cluster.Nodes[0].Host,
		Cluster: cluster,
	}

	if err := syncer.SyncIndex(); err != nil {
		t.Fatal(err)
	}

	// Verify data is the same on both nodes.
	for i, idx := range []*Index{idx0, idx1} {
		f := idx.Fragment("d", "f", 0)
		if a := f.Bitmap(0).Bits(); !reflect.DeepEqual(a, []uint64{10, 4000}) {
			t.Fatalf("unexpected bits(%d/0): %+v", i, a)
		} else if a := f.Bitmap(2).Bits(); !reflect.DeepEqual(a, []uint64{20}) {
			t.Fatalf("unexpected bits(%d/2): %+v", i, a)
		} else if a := f.Bitmap(3).Bits(); !reflect.DeepEqual(a, []uint64{10}) {
			t.Fatalf("unexpected bits(%d/3): %+v", i, a)
		} else if a := f.Bitmap(120).Bits(); !reflect.DeepEqual(a, []uint64{10}) {
			t.Fatalf("unexpected bits(%d/120): %+v", i, a)
		} else if a := f.Bitmap(200).Bits(); !reflect.DeepEqual(a, []uint64{4}) {
			t.Fatalf("unexpected bits(%d/200): %+v", i, a)
		}

		f = idx.Fragment("d", "f0", 1)
		a := f.Bitmap(9).Bits()
		if !reflect.DeepEqual(a, []uint64{SliceWidth + 5}) {
			t.Fatalf("unexpected bits(%d/d/f0): %+v", i, a)
		}
		if a := f.Bitmap(9).Bits(); !reflect.DeepEqual(a, []uint64{SliceWidth + 5}) {
			t.Fatalf("unexpected bits(%d/d/f0): %+v", i, a)
		}
		f = idx.Fragment("y", "z", 3)
		if a := f.Bitmap(10).Bits(); !reflect.DeepEqual(a, []uint64{(3 * SliceWidth) + 4, (3 * SliceWidth) + 5, (3 * SliceWidth) + 7}) {
			t.Fatalf("unexpected bits(%d/y/z): %+v", i, a)
		}
	}
}

// Ensure index can handle Messenger messages.
func TestIndex_HandleMessage(t *testing.T) {
	// Create a local index.
	idx0 := MustOpenIndex()
	defer idx0.Close()

	msg0 := &internal.CreateSliceMessage{
		DB:    "d",
		Slice: 8,
	}
	idx0.HandleMessage(msg0)
	if ms := idx0.MaxSlices(); !reflect.DeepEqual(ms, map[string]uint64{"d": 8}) {
		t.Fatalf("unexpected max slice: %s", ms)
	}

	msg1 := &internal.DeleteDBMessage{
		DB: "d",
	}
	idx0.HandleMessage(msg1)
	if ms := idx0.MaxSlices(); !reflect.DeepEqual(ms, map[string]uint64{}) {
		t.Fatalf("unexpected delete db: %s", ms)
	}
}

// Index is a test wrapper for pilosa.Index.
type Index struct {
	*pilosa.Index
	LogOutput bytes.Buffer
}

// NewIndex returns a new instance of Index with a temporary path.
func NewIndex() *Index {
	path, err := ioutil.TempDir("", "pilosa-")
	if err != nil {
		panic(err)
	}

	i := &Index{Index: pilosa.NewIndex()}
	i.Path = path
	i.Index.LogOutput = &i.LogOutput

	return i
}

// MustOpenIndex creates and opens an index at a temporary path. Panic on error.
func MustOpenIndex() *Index {
	i := NewIndex()
	if err := i.Open(); err != nil {
		panic(err)
	}
	return i
}

// Close closes the index and removes all underlying data.
func (i *Index) Close() error {
	defer os.RemoveAll(i.Path)
	return i.Index.Close()
}

// MustCreateDBIfNotExists returns a given db. Panic on error.
func (i *Index) MustCreateDBIfNotExists(db string) *DB {
	d, err := i.Index.CreateDBIfNotExists(db)
	if err != nil {
		panic(err)
	}
	return &DB{DB: d}
}

// MustCreateFrameIfNotExists returns a given frame. Panic on error.
func (i *Index) MustCreateFrameIfNotExists(db, frame string) *Frame {
	f, err := i.Index.CreateFrameIfNotExists(db, frame)
	if err != nil {
		panic(err)
	}
	return &Frame{Frame: f}
}

// MustCreateFragmentIfNotExists returns a given fragment. Panic on error.
func (i *Index) MustCreateFragmentIfNotExists(db, frame string, slice uint64) *Fragment {
	f, err := i.Index.CreateFragmentIfNotExists(db, frame, slice)
	if err != nil {
		panic(err)
	}
	return &Fragment{Fragment: f}
}
