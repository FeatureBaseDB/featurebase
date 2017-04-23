package pilosa_test

import (
	"bytes"
	"context"
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/pql"
)

// Ensure holder can delete a database and its underlying files.
func TestHolder_DeleteDB(t *testing.T) {
	hldr := MustOpenHolder()
	defer hldr.Close()

	// Write bits to separate databases.
	f0 := hldr.MustCreateFragmentIfNotExists("d0", "f", pilosa.ViewStandard, 0)
	if _, err := f0.SetBit(100, 200); err != nil {
		t.Fatal(err)
	}
	f1 := hldr.MustCreateFragmentIfNotExists("d1", "f", pilosa.ViewStandard, 0)
	if _, err := f1.SetBit(100, 200); err != nil {
		t.Fatal(err)
	}

	// Ensure d0 exists.
	if _, err := os.Stat(hldr.DBPath("d0")); err != nil {
		t.Fatal(err)
	}

	// Delete d0.
	if err := hldr.DeleteDB("d0"); err != nil {
		t.Fatal(err)
	}

	// Ensure d0 files are removed & d1 still exists.
	if _, err := os.Stat(hldr.DBPath("d0")); !os.IsNotExist(err) {
		t.Fatal("expected d0 file deletion")
	} else if _, err := os.Stat(hldr.DBPath("d1")); err != nil {
		t.Fatal("expected d1 files to still exist", err)
	}
}

// Ensure holder can sync with a remote holder.
func TestHolderSyncer_SyncHolder(t *testing.T) {
	cluster := NewCluster(2)

	// Create a local holder.
	hldr0 := MustOpenHolder()
	defer hldr0.Close()

	// Create a remote holder wrapped by an HTTP
	hldr1 := MustOpenHolder()
	defer hldr1.Close()
	s := NewServer()
	defer s.Close()
	s.Handler.Holder = hldr1.Holder
	s.Handler.Executor.ExecuteFn = func(ctx context.Context, db string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		e := pilosa.NewExecutor()
		e.Holder = hldr1.Holder
		e.Host = cluster.Nodes[1].Host
		e.Cluster = cluster
		return e.Execute(ctx, db, query, slices, opt)
	}

	// Mock 2-node, fully replicated cluster.
	cluster.ReplicaN = 2
	cluster.Nodes[0].Host = "localhost:0"
	cluster.Nodes[1].Host = MustParseURLHost(s.URL)

	// Create frames on nodes.
	for _, hldr := range []*Holder{hldr0, hldr1} {
		hldr.MustCreateFrameIfNotExists("d", "f")
		hldr.MustCreateFrameIfNotExists("d", "f0")
		hldr.MustCreateFrameIfNotExists("y", "z")
	}

	// Set data on the local holder.
	f := hldr0.MustCreateFragmentIfNotExists("d", "f", pilosa.ViewStandard, 0)
	if _, err := f.SetBit(0, 10); err != nil {
		t.Fatal(err)
	} else if _, err := f.SetBit(2, 20); err != nil {
		t.Fatal(err)
	} else if _, err := f.SetBit(120, 10); err != nil {
		t.Fatal(err)
	} else if _, err := f.SetBit(200, 4); err != nil {
		t.Fatal(err)
	}

	f = hldr0.MustCreateFragmentIfNotExists("d", "f0", pilosa.ViewStandard, 1)
	if _, err := f.SetBit(9, SliceWidth+5); err != nil {
		t.Fatal(err)
	}

	hldr0.MustCreateFragmentIfNotExists("y", "z", pilosa.ViewStandard, 0)

	// Set data on the remote holder.
	f = hldr1.MustCreateFragmentIfNotExists("d", "f", pilosa.ViewStandard, 0)
	if _, err := f.SetBit(0, 4000); err != nil {
		t.Fatal(err)
	} else if _, err := f.SetBit(3, 10); err != nil {
		t.Fatal(err)
	} else if _, err := f.SetBit(120, 10); err != nil {
		t.Fatal(err)
	}

	f = hldr1.MustCreateFragmentIfNotExists("y", "z", pilosa.ViewStandard, 3)
	if _, err := f.SetBit(10, (3*SliceWidth)+4); err != nil {
		t.Fatal(err)
	} else if _, err := f.SetBit(10, (3*SliceWidth)+5); err != nil {
		t.Fatal(err)
	} else if _, err := f.SetBit(10, (3*SliceWidth)+7); err != nil {
		t.Fatal(err)
	}

	// Set highest slice.
	hldr0.DB("d").SetRemoteMaxSlice(1)
	hldr0.DB("y").SetRemoteMaxSlice(3)

	// Set up syncer.
	syncer := pilosa.HolderSyncer{
		Holder:  hldr0.Holder,
		Host:    cluster.Nodes[0].Host,
		Cluster: cluster,
	}

	if err := syncer.SyncHolder(); err != nil {
		t.Fatal(err)
	}

	// Verify data is the same on both nodes.
	for i, hldr := range []*Holder{hldr0, hldr1} {
		f := hldr.Fragment("d", "f", pilosa.ViewStandard, 0)
		if a := f.Row(0).Bits(); !reflect.DeepEqual(a, []uint64{10, 4000}) {
			t.Fatalf("unexpected bits(%d/0): %+v", i, a)
		} else if a := f.Row(2).Bits(); !reflect.DeepEqual(a, []uint64{20}) {
			t.Fatalf("unexpected bits(%d/2): %+v", i, a)
		} else if a := f.Row(3).Bits(); !reflect.DeepEqual(a, []uint64{10}) {
			t.Fatalf("unexpected bits(%d/3): %+v", i, a)
		} else if a := f.Row(120).Bits(); !reflect.DeepEqual(a, []uint64{10}) {
			t.Fatalf("unexpected bits(%d/120): %+v", i, a)
		} else if a := f.Row(200).Bits(); !reflect.DeepEqual(a, []uint64{4}) {
			t.Fatalf("unexpected bits(%d/200): %+v", i, a)
		}

		f = hldr.Fragment("d", "f0", pilosa.ViewStandard, 1)
		a := f.Row(9).Bits()
		if !reflect.DeepEqual(a, []uint64{SliceWidth + 5}) {
			t.Fatalf("unexpected bits(%d/d/f0): %+v", i, a)
		}
		if a := f.Row(9).Bits(); !reflect.DeepEqual(a, []uint64{SliceWidth + 5}) {
			t.Fatalf("unexpected bits(%d/d/f0): %+v", i, a)
		}
		f = hldr.Fragment("y", "z", pilosa.ViewStandard, 3)
		if a := f.Row(10).Bits(); !reflect.DeepEqual(a, []uint64{(3 * SliceWidth) + 4, (3 * SliceWidth) + 5, (3 * SliceWidth) + 7}) {
			t.Fatalf("unexpected bits(%d/y/z): %+v", i, a)
		}
	}
}

// Holder is a test wrapper for pilosa.Holder.
type Holder struct {
	*pilosa.Holder
	LogOutput bytes.Buffer
}

// NewHolder returns a new instance of Holder with a temporary path.
func NewHolder() *Holder {
	path, err := ioutil.TempDir("", "pilosa-")
	if err != nil {
		panic(err)
	}

	h := &Holder{Holder: pilosa.NewHolder()}
	h.Path = path
	h.Holder.LogOutput = &h.LogOutput

	return h
}

// MustOpenHolder creates and opens a holder at a temporary path. Panic on error.
func MustOpenHolder() *Holder {
	h := NewHolder()
	if err := h.Open(); err != nil {
		panic(err)
	}
	return h
}

// Close closes the holder and removes all underlying data.
func (h *Holder) Close() error {
	defer os.RemoveAll(h.Path)
	return h.Holder.Close()
}

// MustCreateDBIfNotExists returns a given db. Panic on error.
func (h *Holder) MustCreateDBIfNotExists(db string, opt pilosa.DBOptions) *DB {
	d, err := h.Holder.CreateDBIfNotExists(db, opt)
	if err != nil {
		panic(err)
	}
	return &DB{DB: d}
}

// MustCreateFrameIfNotExists returns a given frame. Panic on error.
func (h *Holder) MustCreateFrameIfNotExists(db, frame string) *Frame {
	f, err := h.MustCreateDBIfNotExists(db, pilosa.DBOptions{}).CreateFrameIfNotExists(frame, pilosa.FrameOptions{})
	if err != nil {
		panic(err)
	}
	return f
}

// MustCreateFragmentIfNotExists returns a given fragment. Panic on error.
func (h *Holder) MustCreateFragmentIfNotExists(db, frame, view string, slice uint64) *Fragment {
	d := h.MustCreateDBIfNotExists(db, pilosa.DBOptions{})
	f, err := d.CreateFrameIfNotExists(frame, pilosa.FrameOptions{})
	if err != nil {
		panic(err)
	}
	v, err := f.CreateViewIfNotExists(view)
	if err != nil {
		panic(err)
	}
	frag, err := v.CreateFragmentIfNotExists(slice)
	if err != nil {
		panic(err)
	}
	return &Fragment{Fragment: frag}
}
