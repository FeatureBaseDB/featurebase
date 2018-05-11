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
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/pql"
	"github.com/pilosa/pilosa/server"
	"github.com/pilosa/pilosa/test"
)

func TestHolder_Open(t *testing.T) {
	t.Run("ErrIndexName", func(t *testing.T) {
		h := test.MustOpenHolder()

		bufLogger := test.NewBufferLogger()
		h.Holder.Logger = bufLogger

		defer h.Close()

		if err := os.Mkdir(h.IndexPath("!"), 0777); err != nil {
			t.Fatal(err)
		} else if err := h.Holder.Close(); err != nil {
			t.Fatal(err)
		}
		if err := h.Reopen(); err != nil {
			t.Fatal(err)
		}

		if bufbytes, err := bufLogger.ReadAll(); err != nil {
			t.Fatal(err)
		} else if !bytes.Contains(bufbytes, []byte("ERROR opening index: !")) {
			t.Fatalf("expected log error:\n%s", bufbytes)
		}
	})

	t.Run("ErrIndexPermission", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("Skipping permissions test since user is root.")
		}
		h := test.MustOpenHolder()
		defer h.Close()

		if _, err := h.CreateIndex("test", pilosa.IndexOptions{}); err != nil {
			t.Fatal(err)
		} else if err := h.Holder.Close(); err != nil {
			t.Fatal(err)
		} else if err := os.Chmod(h.IndexPath("test"), 0000); err != nil {
			t.Fatal(err)
		}
		defer os.Chmod(h.IndexPath("test"), 0777)

		if err := h.Reopen(); err == nil || !strings.Contains(err.Error(), "permission denied") {
			t.Fatalf("unexpected error: %s", err)
		}
	})
	t.Run("ErrIndexAttrStoreCorrupt", func(t *testing.T) {
		h := test.MustOpenHolder()
		defer h.Close()

		if _, err := h.CreateIndex("test", pilosa.IndexOptions{}); err != nil {
			t.Fatal(err)
		} else if err := h.Holder.Close(); err != nil {
			t.Fatal(err)
		} else if err := os.Truncate(filepath.Join(h.IndexPath("test"), ".data"), 2); err != nil {
			t.Fatal(err)
		}

		if err := h.Reopen(); err == nil || !strings.Contains(err.Error(), "open index: name=test, err=opening attrstore: opening storage: invalid database") {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrFramePermission", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("Skipping permissions test since user is root.")
		}
		h := test.MustOpenHolder()
		defer h.Close()

		if idx, err := h.CreateIndex("foo", pilosa.IndexOptions{}); err != nil {
			t.Fatal(err)
		} else if _, err := idx.CreateFrame("bar", pilosa.FrameOptions{}); err != nil {
			t.Fatal(err)
		} else if err := h.Holder.Close(); err != nil {
			t.Fatal(err)
		} else if err := os.Chmod(filepath.Join(h.Path, "foo", "bar"), 0000); err != nil {
			t.Fatal(err)
		}
		defer os.Chmod(filepath.Join(h.Path, "foo", "bar"), 0777)

		if err := h.Reopen(); err == nil || !strings.Contains(err.Error(), "permission denied") {
			t.Fatalf("unexpected error: %s", err)
		}
	})
	t.Run("ErrFrameMetaCorrupt", func(t *testing.T) {
		h := test.MustOpenHolder()
		defer h.Close()

		if idx, err := h.CreateIndex("foo", pilosa.IndexOptions{}); err != nil {
			t.Fatal(err)
		} else if _, err := idx.CreateFrame("bar", pilosa.FrameOptions{}); err != nil {
			t.Fatal(err)
		} else if err := h.Holder.Close(); err != nil {
			t.Fatal(err)
		} else if err := os.Truncate(filepath.Join(h.Path, "foo", "bar", ".meta"), 2); err != nil {
			t.Fatal(err)
		}

		if err := h.Reopen(); err == nil || !strings.Contains(err.Error(), "open index: name=foo, err=opening frames: open frame: name=bar, err=loading meta: unmarshaling: unexpected EOF") {
			t.Fatalf("unexpected error: %s", err)
		}
	})
	t.Run("ErrFrameAttrStoreCorrupt", func(t *testing.T) {
		h := test.MustOpenHolder()
		defer h.Close()

		if idx, err := h.CreateIndex("foo", pilosa.IndexOptions{}); err != nil {
			t.Fatal(err)
		} else if _, err := idx.CreateFrame("bar", pilosa.FrameOptions{}); err != nil {
			t.Fatal(err)
		} else if err := h.Holder.Close(); err != nil {
			t.Fatal(err)
		} else if err := os.Truncate(filepath.Join(h.Path, "foo", "bar", ".data"), 2); err != nil {
			t.Fatal(err)
		}

		if err := h.Reopen(); err == nil || !strings.Contains(err.Error(), "open index: name=foo, err=opening frames: open frame: name=bar, err=opening attrstore: opening storage: invalid database") {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrViewPermission", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("Skipping permissions test since user is root.")
		}
		h := test.MustOpenHolder()
		defer h.Close()

		if idx, err := h.CreateIndex("foo", pilosa.IndexOptions{}); err != nil {
			t.Fatal(err)
		} else if frame, err := idx.CreateFrame("bar", pilosa.FrameOptions{}); err != nil {
			t.Fatal(err)
		} else if _, err := frame.CreateViewIfNotExists(pilosa.ViewStandard); err != nil {
			t.Fatal(err)
		} else if err := h.Holder.Close(); err != nil {
			t.Fatal(err)
		} else if err := os.Chmod(filepath.Join(h.Path, "foo", "bar", "views", "standard"), 0000); err != nil {
			t.Fatal(err)
		}
		defer os.Chmod(filepath.Join(h.Path, "foo", "bar", "views", "standard"), 0777)

		if err := h.Reopen(); err == nil || !strings.Contains(err.Error(), "permission denied") {
			t.Fatalf("unexpected error: %s", err)
		}
	})
	t.Run("ErrViewFragmentsMkdir", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("Skipping permissions test since user is root.")
		}
		h := test.MustOpenHolder()
		defer h.Close()

		if idx, err := h.CreateIndex("foo", pilosa.IndexOptions{}); err != nil {
			t.Fatal(err)
		} else if frame, err := idx.CreateFrame("bar", pilosa.FrameOptions{}); err != nil {
			t.Fatal(err)
		} else if _, err := frame.CreateViewIfNotExists(pilosa.ViewStandard); err != nil {
			t.Fatal(err)
		} else if err := h.Holder.Close(); err != nil {
			t.Fatal(err)
		} else if err := os.Chmod(filepath.Join(h.Path, "foo", "bar", "views", "standard", "fragments"), 0000); err != nil {
			t.Fatal(err)
		}
		defer os.Chmod(filepath.Join(h.Path, "foo", "bar", "views", "standard", "fragments"), 0777)

		if err := h.Reopen(); err == nil || !strings.Contains(err.Error(), "permission denied") {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrFragmentStoragePermission", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("Skipping permissions test since user is root.")
		}
		h := test.MustOpenHolder()
		defer h.Close()

		if idx, err := h.CreateIndex("foo", pilosa.IndexOptions{}); err != nil {
			t.Fatal(err)
		} else if frame, err := idx.CreateFrame("bar", pilosa.FrameOptions{}); err != nil {
			t.Fatal(err)
		} else if view, err := frame.CreateViewIfNotExists(pilosa.ViewStandard); err != nil {
			t.Fatal(err)
		} else if _, err := view.SetBit(0, 0); err != nil {
			t.Fatal(err)
		} else if err := h.Holder.Close(); err != nil {
			t.Fatal(err)
		} else if err := os.Chmod(filepath.Join(h.Path, "foo", "bar", "views", "standard", "fragments", "0"), 0000); err != nil {
			t.Fatal(err)
		}
		defer os.Chmod(filepath.Join(h.Path, "foo", "bar", "views", "standard", "fragments", "0"), 0666)

		if err := h.Reopen(); err == nil || !strings.Contains(err.Error(), "permission denied") {
			t.Fatalf("unexpected error: %s", err)
		}
	})
	t.Run("ErrFragmentStorageCorrupt", func(t *testing.T) {
		h := test.MustOpenHolder()
		defer h.Close()

		if idx, err := h.CreateIndex("foo", pilosa.IndexOptions{}); err != nil {
			t.Fatal(err)
		} else if frame, err := idx.CreateFrame("bar", pilosa.FrameOptions{}); err != nil {
			t.Fatal(err)
		} else if view, err := frame.CreateViewIfNotExists(pilosa.ViewStandard); err != nil {
			t.Fatal(err)
		} else if _, err := view.SetBit(0, 0); err != nil {
			t.Fatal(err)
		} else if err := h.Holder.Close(); err != nil {
			t.Fatal(err)
		} else if err := os.Truncate(filepath.Join(h.Path, "foo", "bar", "views", "standard", "fragments", "0"), 2); err != nil {
			t.Fatal(err)
		}

		if err := h.Reopen(); err == nil || !strings.Contains(err.Error(), "open fragment: slice=0, err=opening storage: unmarshal storage") {
			t.Fatalf("unexpected error: %s", err)
		}
	})

	t.Run("ErrFragmentCachePermission", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("Skipping permissions test since user is root.")
		}
		h := test.MustOpenHolder()
		defer h.Close()

		if idx, err := h.CreateIndex("foo", pilosa.IndexOptions{}); err != nil {
			t.Fatal(err)
		} else if frame, err := idx.CreateFrame("bar", pilosa.FrameOptions{}); err != nil {
			t.Fatal(err)
		} else if view, err := frame.CreateViewIfNotExists(pilosa.ViewStandard); err != nil {
			t.Fatal(err)
		} else if _, err := view.SetBit(0, 0); err != nil {
			t.Fatal(err)
		} else if err := view.Fragment(0).FlushCache(); err != nil {
			t.Fatal(err)
		} else if err := h.Holder.Close(); err != nil {
			t.Fatal(err)
		} else if err := os.Chmod(filepath.Join(h.Path, "foo", "bar", "views", "standard", "fragments", "0.cache"), 0000); err != nil {
			t.Fatal(err)
		}
		defer os.Chmod(filepath.Join(h.Path, "foo", "bar", "views", "standard", "fragments", "0.cache"), 0666)

		if err := h.Reopen(); err == nil || !strings.Contains(err.Error(), "permission denied") {
			t.Fatalf("unexpected error: %s", err)
		}
	})
}

func TestHolder_HasData(t *testing.T) {
	t.Run("IndexDirectory", func(t *testing.T) {
		h := test.MustOpenHolder()
		defer h.Close()

		if ok, err := h.HasData(); ok || err != nil {
			t.Fatal("expected HasData to return false, no err, but", ok, err)
		}

		if _, err := h.CreateIndex("test", pilosa.IndexOptions{}); err != nil {
			t.Fatal(err)
		}

		if ok, err := h.HasData(); !ok || err != nil {
			t.Fatal("expected HasData to return true, but ", ok, err)
		}
	})

	t.Run("Peek", func(t *testing.T) {
		h := test.NewHolder()

		if ok, err := h.HasData(); ok || err != nil {
			t.Fatal("expected HasData to return false, no err, but", ok, err)
		}

		// Create an index directory to indicate data exists.
		if err := os.Mkdir(h.IndexPath("test"), 0777); err != nil {
			t.Fatal(err)
		}

		if ok, err := h.HasData(); !ok || err != nil {
			t.Fatal("expected HasData to return true, no err, but", ok, err)
		}
	})

	t.Run("Peek at missing directory", func(t *testing.T) {
		h := test.NewHolder()

		// Ensure that hasData is false when dir doesn't exist.
		h.Path = "bad-path"

		if ok, err := h.HasData(); ok || err != nil {
			t.Fatal("expected HasData to return false, no err, but", ok, err)
		}
	})
}

// Ensure holder can delete an index and its underlying files.
func TestHolder_DeleteIndex(t *testing.T) {
	hldr := test.MustOpenHolder()
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
	cluster := test.NewCluster(2)
	client := server.GetHTTPClient(nil)
	// Create a local holder.
	hldr0 := test.MustOpenHolder()
	defer hldr0.Close()

	// Create a remote holder wrapped by an HTTP
	hldr1 := test.MustOpenHolder()
	defer hldr1.Close()
	s := test.NewServer()
	defer s.Close()
	s.Handler.API.Holder = hldr1.Holder
	s.Handler.Executor.ExecuteFn = func(ctx context.Context, index string, query *pql.Query, slices []uint64, opt *pilosa.ExecOptions) ([]interface{}, error) {
		e := pilosa.NewExecutor(client)
		e.Holder = hldr1.Holder
		e.Node = cluster.Nodes[1]
		e.Cluster = cluster
		return e.Execute(ctx, index, query, slices, opt)
	}

	// Mock 2-node, fully replicated cluster.
	cluster.ReplicaN = 2

	uri, err := pilosa.NewURIFromAddress(s.URL)
	if err != nil {
		t.Fatal(err)
	}

	cluster.Nodes[0].URI = test.NewURIFromHostPort("localhost", 0)
	cluster.Nodes[1].URI = *uri

	// Create frames on nodes.
	for _, hldr := range []*test.Holder{hldr0, hldr1} {
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
		Holder:       hldr0.Holder,
		Node:         cluster.Nodes[0],
		Cluster:      cluster,
		RemoteClient: server.GetHTTPClient(nil),
		Stats:        pilosa.NopStatsClient,
	}

	if err := syncer.SyncHolder(); err != nil {
		t.Fatal(err)
	}

	// Verify data is the same on both nodes.
	for i, hldr := range []*test.Holder{hldr0, hldr1} {
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

// Ensure holder can clean up orphaned fragments.
func TestHolderCleaner_CleanHolder(t *testing.T) {
	cluster := test.NewCluster(2)

	// Create a local holder.
	hldr0 := test.MustOpenHolder()
	defer hldr0.Close()

	// Mock 2-node, fully replicated cluster.
	cluster.ReplicaN = 2

	cluster.Nodes[0].URI = test.NewURIFromHostPort("localhost", 0)

	// Create frames on nodes.
	for _, hldr := range []*test.Holder{hldr0} {
		hldr.MustCreateFrameIfNotExists("i", "f")
		hldr.MustCreateFrameIfNotExists("i", "f0")
		hldr.MustCreateFrameIfNotExists("y", "z")
	}

	// Set data on the local holder.
	f := hldr0.MustCreateFragmentIfNotExists("i", "f", pilosa.ViewStandard, 0)
	if _, err := f.SetBit(0, 10); err != nil {
		t.Fatal(err)
	} else if _, err := f.SetBit(0, 4000); err != nil {
		t.Fatal(err)
	} else if _, err := f.SetBit(2, 20); err != nil {
		t.Fatal(err)
	} else if _, err := f.SetBit(3, 10); err != nil {
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

	f = hldr0.MustCreateFragmentIfNotExists("y", "z", pilosa.ViewStandard, 2)
	if _, err := f.SetBit(10, (2*SliceWidth)+4); err != nil {
		t.Fatal(err)
	} else if _, err := f.SetBit(10, (2*SliceWidth)+5); err != nil {
		t.Fatal(err)
	} else if _, err := f.SetBit(10, (2*SliceWidth)+7); err != nil {
		t.Fatal(err)
	}

	// Set highest slice.
	hldr0.Index("i").SetRemoteMaxSlice(1)
	hldr0.Index("y").SetRemoteMaxSlice(2)

	// Keep replication the same and ensure we get the expected results.
	cluster.ReplicaN = 2

	// Set up cleaner for replication 2.
	cleaner2 := pilosa.HolderCleaner{
		Node:    cluster.Nodes[0],
		Holder:  hldr0.Holder,
		Cluster: cluster,
	}

	if err := cleaner2.CleanHolder(); err != nil {
		t.Fatal(err)
	}

	// Verify data is the same on both nodes.
	for i, hldr := range []*test.Holder{hldr0} {
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
		f = hldr.Fragment("y", "z", pilosa.ViewStandard, 2)
		if a := f.Row(10).Bits(); !reflect.DeepEqual(a, []uint64{(2 * SliceWidth) + 4, (2 * SliceWidth) + 5, (2 * SliceWidth) + 7}) {
			t.Fatalf("unexpected bits(%d/y/z): %+v", i, a)
		}
	}

	// Change replication factor to ensure we have fragments to remove.
	cluster.ReplicaN = 1

	// Set up cleaner for replication 1.
	cleaner1 := pilosa.HolderCleaner{
		Node:    cluster.Nodes[0],
		Holder:  hldr0.Holder,
		Cluster: cluster,
	}

	if err := cleaner1.CleanHolder(); err != nil {
		t.Fatal(err)
	}

	// Verify data is the same on both nodes.
	for i, hldr := range []*test.Holder{hldr0} {
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
		if f != nil {
			t.Fatalf("expected fragment to be deleted: (%d/i/f0): %+v", i, f)
		}

		f = hldr.Fragment("y", "z", pilosa.ViewStandard, 2)
		if a := f.Row(10).Bits(); !reflect.DeepEqual(a, []uint64{(2 * SliceWidth) + 4, (2 * SliceWidth) + 5, (2 * SliceWidth) + 7}) {
			t.Fatalf("unexpected bits(%d/y/z): %+v", i, a)
		}
	}
}
