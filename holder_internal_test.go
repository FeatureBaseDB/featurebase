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

package pilosa

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/pilosa/pilosa/roaring"
)

type tHolder struct {
	*Holder
}

// Close closes the holder and removes all underlying data.
func (h *tHolder) Close() error {
	defer os.RemoveAll(h.Path)
	return h.Holder.Close()
}

// Reopen instantiates and opens a new holder.
// Note that the holder must be Closed first.
func (h *tHolder) Reopen() error {
	path, logger := h.Path, h.Holder.Logger
	h.Holder = NewHolder()
	h.Holder.Path = path
	h.Holder.Logger = logger
	return h.Holder.Open()
}

func newHolder() *tHolder {
	path, err := ioutil.TempDir("", "pilosa-")
	if err != nil {
		panic(err)
	}

	h := &tHolder{Holder: NewHolder()}
	h.Path = path
	return h
}

// MustCreateFieldIfNotExists returns a given field. Panic on error.
func (h *tHolder) MustCreateFieldIfNotExists(index, field string) *Field {
	f, err := h.MustCreateIndexIfNotExists(index, IndexOptions{}).CreateFieldIfNotExists(field, OptFieldTypeDefault())
	if err != nil {
		panic(err)
	}
	return f
}

// MustCreateIndexIfNotExists returns a given index. Panic on error.
func (h *tHolder) MustCreateIndexIfNotExists(index string, opt IndexOptions) *Index {
	idx, err := h.Holder.CreateIndexIfNotExists(index, opt)
	if err != nil {
		panic(err)
	}
	return idx
}

// SetBit clears a bit on the given field.
func (h *tHolder) SetBit(index, field string, rowID, columnID uint64) {
	f := h.MustCreateFieldIfNotExists(index, field)
	_, err := f.SetBit(rowID, columnID, nil)
	if err != nil {
		panic(err)
	}
}

// Row returns a Row for a given field.
func (h *tHolder) Row(index, field string, rowID uint64) *Row {
	f := h.MustCreateFieldIfNotExists(index, field)
	row, err := f.Row(rowID)
	if err != nil {
		panic(err)
	}
	return row
}

func TestHolder_Optn(t *testing.T) {
	t.Run("ErrViewPermission", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("Skipping permissions test since user is root.")
		}
		h := newHolder()
		defer h.Close()

		if idx, err := h.CreateIndex("foo", IndexOptions{}); err != nil {
			t.Fatal(err)
		} else if field, err := idx.CreateField("bar", OptFieldTypeDefault()); err != nil {
			t.Fatal(err)
		} else if _, err := field.createViewIfNotExists(viewStandard); err != nil {
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
		h := newHolder()
		defer h.Close()

		if idx, err := h.CreateIndex("foo", IndexOptions{}); err != nil {
			t.Fatal(err)
		} else if field, err := idx.CreateField("bar", OptFieldTypeDefault()); err != nil {
			t.Fatal(err)
		} else if _, err := field.createViewIfNotExists(viewStandard); err != nil {
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

	t.Run("ErrFragmentCachePermission", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("Skipping permissions test since user is root.")
		}
		h := newHolder()
		defer h.Close()

		if idx, err := h.CreateIndex("foo", IndexOptions{}); err != nil {
			t.Fatal(err)
		} else if field, err := idx.CreateField("bar", OptFieldTypeDefault()); err != nil {
			t.Fatal(err)
		} else if view, err := field.createViewIfNotExists(viewStandard); err != nil {
			t.Fatal(err)
		} else if _, err := field.SetBit(0, 0, nil); err != nil {
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

// Ensure holder can clean up orphaned fragments.
func TestHolderCleaner_CleanHolder(t *testing.T) {
	cluster := NewTestCluster(2)

	// Create a local holder.
	hldr0 := newHolder()
	defer hldr0.Close()

	// Mock 2-node, fully replicated cluster.
	cluster.ReplicaN = 2

	cluster.nodes[0].URI = NewTestURIFromHostPort("localhost", 0)

	// Create fields on nodes.
	for _, hldr := range []*tHolder{hldr0} {
		hldr.MustCreateFieldIfNotExists("i", "f")
		hldr.MustCreateFieldIfNotExists("i", "f0")
		hldr.MustCreateFieldIfNotExists("y", "z")
	}

	// Set data on the local holder.
	hldr0.SetBit("i", "f", 0, 10)
	hldr0.SetBit("i", "f", 0, 4000)
	hldr0.SetBit("i", "f", 2, 20)
	hldr0.SetBit("i", "f", 3, 10)
	hldr0.SetBit("i", "f", 120, 10)
	hldr0.SetBit("i", "f", 200, 4)

	hldr0.SetBit("i", "f0", 9, ShardWidth+5)

	hldr0.SetBit("y", "z", 10, (2*ShardWidth)+4)
	hldr0.SetBit("y", "z", 10, (2*ShardWidth)+5)
	hldr0.SetBit("y", "z", 10, (2*ShardWidth)+7)

	// Set highest shard.
	hldr0.Field("i", "f").addRemoteAvailableShards(roaring.NewBitmap(0, 1))
	hldr0.Field("y", "z").addRemoteAvailableShards(roaring.NewBitmap(0, 1, 2))

	// Keep replication the same and ensure we get the expected results.
	cluster.ReplicaN = 2

	// Set up cleaner for replication 2.
	cleaner2 := holderCleaner{
		Node:    cluster.nodes[0],
		Holder:  hldr0.Holder,
		Cluster: cluster,
	}

	if err := cleaner2.CleanHolder(); err != nil {
		t.Fatal(err)
	}

	// Verify data is the same on both nodes.
	for i, hldr := range []*tHolder{hldr0} {
		if a := hldr.Row("i", "f", 0).Columns(); !reflect.DeepEqual(a, []uint64{10, 4000}) {
			t.Fatalf("unexpected columns(%d/0): %+v", i, a)
		} else if a := hldr.Row("i", "f", 2).Columns(); !reflect.DeepEqual(a, []uint64{20}) {
			t.Fatalf("unexpected columns(%d/2): %+v", i, a)
		} else if a := hldr.Row("i", "f", 3).Columns(); !reflect.DeepEqual(a, []uint64{10}) {
			t.Fatalf("unexpected columns(%d/3): %+v", i, a)
		} else if a := hldr.Row("i", "f", 120).Columns(); !reflect.DeepEqual(a, []uint64{10}) {
			t.Fatalf("unexpected columns(%d/120): %+v", i, a)
		} else if a := hldr.Row("i", "f", 200).Columns(); !reflect.DeepEqual(a, []uint64{4}) {
			t.Fatalf("unexpected columns(%d/200): %+v", i, a)
		}

		if a := hldr.Row("i", "f0", 9).Columns(); !reflect.DeepEqual(a, []uint64{ShardWidth + 5}) {
			t.Fatalf("unexpected columns(%d/d/f0): %+v", i, a)
		}

		if a := hldr.Row("y", "z", 10).Columns(); !reflect.DeepEqual(a, []uint64{(2 * ShardWidth) + 4, (2 * ShardWidth) + 5, (2 * ShardWidth) + 7}) {
			t.Fatalf("unexpected columns(%d/y/z): %+v", i, a)
		}
	}

	// Change replication factor to ensure we have fragments to remove.
	cluster.ReplicaN = 1

	// Set up cleaner for replication 1.
	cleaner1 := holderCleaner{
		Node:    cluster.nodes[0],
		Holder:  hldr0.Holder,
		Cluster: cluster,
	}

	if err := cleaner1.CleanHolder(); err != nil {
		t.Fatal(err)
	}

	// Verify data is the same on both nodes.
	for i, hldr := range []*tHolder{hldr0} {
		if a := hldr.Row("i", "f", 0).Columns(); !reflect.DeepEqual(a, []uint64{10, 4000}) {
			t.Fatalf("unexpected columns(%d/0): %+v", i, a)
		} else if a := hldr.Row("i", "f", 2).Columns(); !reflect.DeepEqual(a, []uint64{20}) {
			t.Fatalf("unexpected columns(%d/2): %+v", i, a)
		} else if a := hldr.Row("i", "f", 3).Columns(); !reflect.DeepEqual(a, []uint64{10}) {
			t.Fatalf("unexpected columns(%d/3): %+v", i, a)
		} else if a := hldr.Row("i", "f", 120).Columns(); !reflect.DeepEqual(a, []uint64{10}) {
			t.Fatalf("unexpected columns(%d/120): %+v", i, a)
		} else if a := hldr.Row("i", "f", 200).Columns(); !reflect.DeepEqual(a, []uint64{4}) {
			t.Fatalf("unexpected columns(%d/200): %+v", i, a)
		}

		f := hldr.fragment("i", "f0", viewStandard, 1)
		if f != nil {
			t.Fatalf("expected fragment to be deleted: (%d/i/f0): %+v", i, f)
		}

		if a := hldr.Row("y", "z", 10).Columns(); !reflect.DeepEqual(a, []uint64{(2 * ShardWidth) + 4, (2 * ShardWidth) + 5, (2 * ShardWidth) + 7}) {
			t.Fatalf("unexpected columns(%d/y/z): %+v", i, a)
		}
	}
}

// Ensure holder can reopen.
func TestHolderCleaner_Reopen(t *testing.T) {
	h := NewHolder()
	h.Path = "path"
	h.Open()
	h.Close()
	h.Open()
	h.Close()
}
