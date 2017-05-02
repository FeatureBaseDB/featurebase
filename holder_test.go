// Copyright 2017 Pilosa Corp.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

// Ensure holder can delete an index and its underlying files.
func TestHolder_DeleteIndex(t *testing.T) {
	hldr := MustOpenHolder()
	defer hldr.Close()

	// Write bits to separate indexes.
	f0 := hldr.MustCreateFragmentIfNotExists("i0", "f", pilosa.ViewStandard, 0)
	if _, err := f0.SetBit(100, 200); err != nil {
		t.Fatal(err)
	}
	f1 := hldr.MustCreateFragmentIfNotExists("i1", "f", pilosa.ViewStandard, 0)
	if _, err := f1.SetBit(100, 200); err != nil {
		t.Fatal(err)
	}

	// Ensure i0 exists.
	if _, err := os.Stat(hldr.IndexPath("i0")); err != nil {
		t.Fatal(err)
	}

	// Delete i0.
	if err := hldr.DeleteIndex("i0"); err != nil {
		t.Fatal(err)
	}

	// Ensure i0 files are removed & i1 still exists.
	if _, err := os.Stat(hldr.IndexPath("i0")); !os.IsNotExist(err) {
		t.Fatal("expected i0 file deletion")
	} else if _, err := os.Stat(hldr.IndexPath("i1")); err != nil {
		t.Fatal("expected i1 files to still exist", err)
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
	s.Handler.Executor.ExecuteFn = func(ctx context.Context, index string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		e := pilosa.NewExecutor()
		e.Holder = hldr1.Holder
		e.Host = cluster.Nodes[1].Host
		e.Cluster = cluster
		return e.Execute(ctx, index, query, slices, opt)
	}

	// Mock 2-node, fully replicated cluster.
	cluster.ReplicaN = 2
	cluster.Nodes[0].Host = "localhost:0"
	cluster.Nodes[1].Host = MustParseURLHost(s.URL)

	// Create frames on nodes.
	for _, hldr := range []*Holder{hldr0, hldr1} {
		hldr.MustCreateFrameIfNotExists("i", "f")
		hldr.MustCreateFrameIfNotExists("i", "f0")
		hldr.MustCreateFrameIfNotExists("y", "z")
	}

	// Set data on the local holder.
	f := hldr0.MustCreateFragmentIfNotExists("i", "f", pilosa.ViewStandard, 0)
	if _, err := f.SetBit(0, 10); err != nil {
		t.Fatal(err)
	} else if _, err := f.SetBit(2, 20); err != nil {
		t.Fatal(err)
	} else if _, err := f.SetBit(120, 10); err != nil {
		t.Fatal(err)
	} else if _, err := f.SetBit(200, 4); err != nil {
		t.Fatal(err)
	}

	f = hldr0.MustCreateFragmentIfNotExists("i", "f0", pilosa.ViewStandard, 1)
	if _, err := f.SetBit(9, SliceWidth+5); err != nil {
		t.Fatal(err)
	}

	hldr0.MustCreateFragmentIfNotExists("y", "z", pilosa.ViewStandard, 0)

	// Set data on the remote holder.
	f = hldr1.MustCreateFragmentIfNotExists("i", "f", pilosa.ViewStandard, 0)
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
	hldr0.Index("i").SetRemoteMaxSlice(1)
	hldr0.Index("y").SetRemoteMaxSlice(3)

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
		f := hldr.Fragment("i", "f", pilosa.ViewStandard, 0)
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

		f = hldr.Fragment("i", "f0", pilosa.ViewStandard, 1)
		a := f.Row(9).Bits()
		if !reflect.DeepEqual(a, []uint64{SliceWidth + 5}) {
			t.Fatalf("unexpected bits(%d/i/f0): %+v", i, a)
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

// MustCreateIndexIfNotExists returns a given index. Panic on error.
func (h *Holder) MustCreateIndexIfNotExists(index string, opt pilosa.IndexOptions) *Index {
	idx, err := h.Holder.CreateIndexIfNotExists(index, opt)
	if err != nil {
		panic(err)
	}
	return &Index{Index: idx}
}

// MustCreateFrameIfNotExists returns a given frame. Panic on error.
func (h *Holder) MustCreateFrameIfNotExists(index, frame string) *Frame {
	f, err := h.MustCreateIndexIfNotExists(index, pilosa.IndexOptions{}).CreateFrameIfNotExists(frame, pilosa.FrameOptions{})
	if err != nil {
		panic(err)
	}
	return f
}

// MustCreateFragmentIfNotExists returns a given fragment. Panic on error.
func (h *Holder) MustCreateFragmentIfNotExists(index, frame, view string, slice uint64) *Fragment {
	idx := h.MustCreateIndexIfNotExists(index, pilosa.IndexOptions{})
	f, err := idx.CreateFrameIfNotExists(frame, pilosa.FrameOptions{})
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

// MustCreateRankedFragmentIfNotExists returns a given fragment with a ranked cache. Panic on error.
func (h *Holder) MustCreateRankedFragmentIfNotExists(index, frame, view string, slice uint64) *Fragment {
	idx := h.MustCreateIndexIfNotExists(index, pilosa.IndexOptions{})
	f, err := idx.CreateFrameIfNotExists(frame, pilosa.FrameOptions{CacheType: pilosa.CacheTypeRanked})
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
