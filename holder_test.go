// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package pilosa_test

import (
	"context"
	"math"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	pilosa "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/pql"
	"github.com/featurebasedb/featurebase/v3/test"
	"github.com/pkg/errors"
)

func TestHolder_Open(t *testing.T) {
	t.Run("ErrIndexPermission", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("Skipping permissions test since user is root.")
		}
		// Manual open because MustOpenHolder closes automatically and fails the test on
		// double-close.
		h := test.NewHolder(t)
		err := h.Open()
		if err != nil {
			t.Fatalf("opening holder: %v", err)
		}
		// no automatic close here, because we manually close this, and then
		// *fail* to reopen it.

		if _, err := h.CreateIndex("test", pilosa.IndexOptions{}); err != nil {
			t.Fatal(err)
		} else if err := h.Close(); err != nil {
			t.Fatal(err)
		} else if err := os.Chmod(h.IndexPath("test"), 0000); err != nil {
			t.Fatal(err)
		}
		defer func() {
			_ = os.Chmod(h.IndexPath("test"), 0755)
		}()

		if err := h.Reopen(); err == nil || !strings.Contains(err.Error(), "permission denied") {
			t.Fatalf("unexpected error: %v", err)
		}
	})
	t.Run("ForeignIndex", func(t *testing.T) {
		t.Run("ErrForeignIndexNotFound", func(t *testing.T) {
			h := test.MustOpenHolder(t)

			if idx, err := h.CreateIndex("foo", pilosa.IndexOptions{}); err != nil {
				t.Fatal(err)
			} else {
				_, err := idx.CreateField("bar", pilosa.OptFieldTypeInt(0, 100), pilosa.OptFieldForeignIndex("nonexistent"))
				if err == nil {
					t.Fatalf("expected error: %s", pilosa.ErrForeignIndexNotFound)
				} else if errors.Cause(err) != pilosa.ErrForeignIndexNotFound {
					t.Fatalf("expected error: %s, but got: %s", pilosa.ErrForeignIndexNotFound, err)
				}
			}
		})

		// Foreign index zzz is opened after foo/bar.
		t.Run("ForeignIndexNotOpenYet", func(t *testing.T) {
			h := test.MustOpenHolder(t)

			if _, err := h.CreateIndex("zzz", pilosa.IndexOptions{}); err != nil {
				t.Fatal(err)
			} else if idx, err := h.CreateIndex("foo", pilosa.IndexOptions{}); err != nil {
				t.Fatal(err)
			} else if _, err := idx.CreateField("bar", pilosa.OptFieldTypeInt(0, 100), pilosa.OptFieldForeignIndex("zzz")); err != nil {
				t.Fatal(err)
			} else if err := h.Holder.Close(); err != nil {
				t.Fatal(err)
			}

			if err := h.Reopen(); err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
		})

		// Foreign index aaa is opened before foo/bar.
		t.Run("ForeignIndexIsOpen", func(t *testing.T) {
			h := test.MustOpenHolder(t)

			if _, err := h.CreateIndex("aaa", pilosa.IndexOptions{}); err != nil {
				t.Fatal(err)
			} else if idx, err := h.CreateIndex("foo", pilosa.IndexOptions{}); err != nil {
				t.Fatal(err)
			} else if _, err := idx.CreateField("bar", pilosa.OptFieldTypeInt(0, 100), pilosa.OptFieldForeignIndex("aaa")); err != nil {
				t.Fatal(err)
			} else if err := h.Holder.Close(); err != nil {
				t.Fatal(err)
			}

			if err := h.Reopen(); err != nil {
				t.Fatalf("unexpected error: %s", err)
			}
		})

		// Try to re-create existing index
		t.Run("CreateIndexIfNotExists", func(t *testing.T) {
			h := test.MustOpenHolder(t)

			idx1, err := h.CreateIndexIfNotExists("aaa", pilosa.IndexOptions{})
			if err != nil {
				t.Fatal(err)
			}

			if _, err = h.CreateIndex("aaa", pilosa.IndexOptions{}); err == nil {
				t.Fatalf("expected: ConflictError, got: nil")
			} else if _, ok := err.(pilosa.ConflictError); !ok {
				t.Fatalf("expected: ConflictError, got: %s", err)
			}

			idx2, err := h.CreateIndexIfNotExists("aaa", pilosa.IndexOptions{})
			if err != nil {
				t.Fatal(err)
			}

			if idx1 != idx2 {
				t.Fatalf("expected the same indexes, got: %s and %s", idx1.Name(), idx2.Name())
			}
		})
	})
}

func TestHolder_HasData(t *testing.T) {
	t.Run("IndexDirectory", func(t *testing.T) {
		h := test.MustOpenHolder(t)

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
		h := test.MustOpenHolder(t)

		if ok, err := h.HasData(); ok || err != nil {
			t.Fatal("expected HasData to return false, no err, but", ok, err)
		}

		// Create an index directory to indicate data exists.
		if err := os.Mkdir(h.IndexPath("test"), 0750); err != nil {
			t.Fatal(err)
		}

		if ok, err := h.HasData(); !ok || err != nil {
			t.Fatal("expected HasData to return true, no err, but", ok, err)
		}
	})

	t.Run("Peek at missing directory", func(t *testing.T) {
		// Ensure that hasData is false when dir doesn't exist.

		// Note that we are intentionally not using test.NewHolder,
		// because we want to create a Holder object with an invalid path,
		// rather than creating a valid holder with a temporary path.
		h := pilosa.NewHolder("bad-path", pilosa.TestHolderConfig())

		if ok, err := h.HasData(); ok || err != nil {
			t.Fatal("expected HasData to return false, no err, but", ok, err)
		}
	})
}

// Ensure holder can delete an index and its underlying files.
func TestHolder_DeleteIndex(t *testing.T) {

	hldr := test.MustOpenHolder(t)

	// Write bits to separate indexes.
	hldr.SetBit("i0", "f", 100, 200)
	hldr.SetBit("i1", "f", 100, 200)

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
	c := test.MustUnsharedCluster(t, 2)
	c.GetIdleNode(0).Config.Cluster.ReplicaN = 2
	c.GetIdleNode(0).Config.AntiEntropy.Interval = 0
	c.GetIdleNode(1).Config.Cluster.ReplicaN = 2
	c.GetIdleNode(1).Config.AntiEntropy.Interval = 0
	err := c.Start()

	if err != nil {
		t.Fatalf("starting cluster: %v", err)
	}
	defer c.Close()

	_, err = c.GetNode(0).API.CreateIndex(context.Background(), c.Idx(), pilosa.IndexOptions{})
	if err != nil {
		t.Fatalf("creating index i: %v", err)
	}
	_, err = c.GetNode(0).API.CreateIndex(context.Background(), c.Idx("y"), pilosa.IndexOptions{})
	if err != nil {
		t.Fatalf("creating index y: %v", err)
	}
	_, err = c.GetNode(0).API.CreateField(context.Background(), c.Idx(), "f", pilosa.OptFieldTypeSet(pilosa.DefaultCacheType, pilosa.DefaultCacheSize))
	if err != nil {
		t.Fatalf("creating field f: %v", err)
	}
	_, err = c.GetNode(0).API.CreateField(context.Background(), c.Idx(), "f0", pilosa.OptFieldTypeSet(pilosa.DefaultCacheType, pilosa.DefaultCacheSize))
	if err != nil {
		t.Fatalf("creating field f0: %v", err)
	}
	_, err = c.GetNode(0).API.CreateField(context.Background(), c.Idx("y"), "z", pilosa.OptFieldTypeMutex(pilosa.DefaultCacheType, pilosa.DefaultCacheSize))
	if err != nil {
		t.Fatalf("creating field z in y: %v", err)
	}
	_, err = c.GetNode(0).API.CreateField(context.Background(), c.Idx("y"), "b", pilosa.OptFieldTypeBool())
	if err != nil {
		t.Fatalf("creating field b in y: %v", err)
	}

	hldr0 := &test.Holder{Holder: c.GetNode(0).Server.Holder()}
	hldr1 := &test.Holder{Holder: c.GetNode(1).Server.Holder()}

	// Set data on the local holder.
	hldr0.SetBit(c.Idx(), "f", 0, 10)
	hldr0.SetBit(c.Idx(), "f", 2, 20)
	hldr0.SetBit(c.Idx(), "f", 120, 10)
	hldr0.SetBit(c.Idx(), "f", 200, 4)

	hldr0.SetBit(c.Idx(), "f0", 9, ShardWidth+5)

	// Set a bit to create the fragment.
	hldr0.SetBit(c.Idx("y"), "z", 0, 0)
	hldr0.SetBit(c.Idx("y"), "b", 0, 0) // rowID = 0 means false

	// Set data on the remote holder.
	hldr1.SetBit(c.Idx(), "f", 0, 4000)
	hldr1.SetBit(c.Idx(), "f", 3, 10)
	hldr1.SetBit(c.Idx(), "f", 120, 10)

	hldr1.SetBit(c.Idx("y"), "z", 10, (3*ShardWidth)+4)
	hldr1.SetBit(c.Idx("y"), "z", 10, (3*ShardWidth)+5)
	hldr1.SetBit(c.Idx("y"), "z", 10, (3*ShardWidth)+7)

	hldr1.SetBit(c.Idx("y"), "b", 1, (3*ShardWidth)+4) // true
	hldr1.SetBit(c.Idx("y"), "b", 0, (3*ShardWidth)+5) // false
	hldr1.SetBit(c.Idx("y"), "b", 1, (3*ShardWidth)+7) // true

	err = c.GetNode(0).Server.SyncData()
	if err != nil {
		t.Fatalf("syncing node 0: %v", err)
	}
	err = c.GetNode(1).Server.SyncData()
	if err != nil {
		t.Fatalf("syncing node 1: %v", err)
	}

	// Verify data is the same on both nodes.
	for i, hldr := range []*test.Holder{hldr0, hldr1} {
		if a := hldr.Row(c.Idx(), "f", 0).Columns(); !reflect.DeepEqual(a, []uint64{10, 4000}) {
			t.Errorf("unexpected columns(%d/0): %+v", i, a)
		}
		if a := hldr.Row(c.Idx(), "f", 2).Columns(); !reflect.DeepEqual(a, []uint64{20}) {
			t.Errorf("unexpected columns(%d/2): %+v", i, a)
		}
		if a := hldr.Row(c.Idx(), "f", 3).Columns(); !reflect.DeepEqual(a, []uint64{10}) {
			t.Errorf("unexpected columns(%d/3): %+v", i, a)
		}
		if a := hldr.Row(c.Idx(), "f", 120).Columns(); !reflect.DeepEqual(a, []uint64{10}) {
			t.Errorf("unexpected columns(%d/120): %+v", i, a)
		}
		if a := hldr.Row(c.Idx(), "f", 200).Columns(); !reflect.DeepEqual(a, []uint64{4}) {
			t.Errorf("unexpected columns(%d/200): %+v", i, a)
		}

		if a := hldr.Row(c.Idx(), "f0", 9).Columns(); !reflect.DeepEqual(a, []uint64{ShardWidth + 5}) {
			t.Errorf("unexpected columns(%d/d/f0): %+v", i, a)
		}

		if a := hldr.Row(c.Idx("y"), "z", 10).Columns(); !reflect.DeepEqual(a, []uint64{(3 * ShardWidth) + 4, (3 * ShardWidth) + 5, (3 * ShardWidth) + 7}) {
			t.Errorf("unexpected columns(%d/y/z): %+v", i, a)
		}

		if a := hldr.Row(c.Idx("y"), "b", 0).Columns(); !reflect.DeepEqual(a, []uint64{0, (3 * ShardWidth) + 5}) {
			t.Errorf("unexpected false columns(%d/y/b): %+v", i, a)
		}
		if a := hldr.Row(c.Idx("y"), "b", 1).Columns(); !reflect.DeepEqual(a, []uint64{(3 * ShardWidth) + 4, (3 * ShardWidth) + 7}) {
			t.Errorf("unexpected true columns(%d/y/b): %+v", i, a)
		}
	}
}

// Ensure holder can sync with a remote holder and respects
// the row boundaries of the block.
func TestHolderSyncer_BlockIteratorLimits(t *testing.T) {
	c := test.MustUnsharedCluster(t, 3)
	c.GetIdleNode(0).Config.Cluster.ReplicaN = 3
	c.GetIdleNode(0).Config.AntiEntropy.Interval = 0
	c.GetIdleNode(1).Config.Cluster.ReplicaN = 3
	c.GetIdleNode(1).Config.AntiEntropy.Interval = 0
	c.GetIdleNode(2).Config.Cluster.ReplicaN = 3
	c.GetIdleNode(2).Config.AntiEntropy.Interval = 0
	err := c.Start()
	if err != nil {
		t.Fatalf("starting cluster: %v", err)
	}
	defer c.Close()

	_, err = c.GetNode(0).API.CreateIndex(context.Background(), c.Idx(), pilosa.IndexOptions{})
	if err != nil {
		t.Fatalf("creating index i: %v", err)
	}
	_, err = c.GetNode(0).API.CreateField(context.Background(), c.Idx(), "f", pilosa.OptFieldTypeSet(pilosa.DefaultCacheType, pilosa.DefaultCacheSize))
	if err != nil {
		t.Fatalf("creating field f: %v", err)
	}

	blockEdge := uint64(pilosa.HashBlockSize)

	hldr0 := &test.Holder{Holder: c.GetNode(0).Server.Holder()}
	hldr1 := &test.Holder{Holder: c.GetNode(1).Server.Holder()}
	hldr2 := &test.Holder{Holder: c.GetNode(2).Server.Holder()}

	// Set data on the local holder.
	hldr0.SetBit(c.Idx(), "f", blockEdge-1, 10)
	hldr0.SetBit(c.Idx(), "f", blockEdge, 20)

	// Set the same data on one of the replicas
	// so that we have a quorum.
	hldr1.SetBit(c.Idx(), "f", blockEdge-1, 10)
	hldr1.SetBit(c.Idx(), "f", blockEdge, 20)

	// Leave the third replica empty to force a block merge.
	//
	err = c.GetNode(0).Server.SyncData()
	if err != nil {
		t.Fatalf("syncing node 0: %v", err)
	}

	// Verify data is the same on all nodes.
	for i, hldr := range []*test.Holder{hldr0, hldr1, hldr2} {
		if a := hldr.Row(c.Idx(), "f", blockEdge-1).Columns(); !reflect.DeepEqual(a, []uint64{10}) {
			t.Errorf("unexpected columns(%d/block 0): %+v", i, a)
		}
		if a := hldr.Row(c.Idx(), "f", blockEdge).Columns(); !reflect.DeepEqual(a, []uint64{20}) {
			t.Errorf("unexpected columns(%d/block 1): %+v", i, a)
		}
	}
}

// Ensure holder correctly handles clears during block sync.
func TestHolderSyncer_Clears(t *testing.T) {
	c := test.MustUnsharedCluster(t, 3)
	c.GetIdleNode(0).Config.Cluster.ReplicaN = 3
	c.GetIdleNode(0).Config.AntiEntropy.Interval = 0
	c.GetIdleNode(1).Config.Cluster.ReplicaN = 3
	c.GetIdleNode(1).Config.AntiEntropy.Interval = 0
	c.GetIdleNode(2).Config.Cluster.ReplicaN = 3
	c.GetIdleNode(2).Config.AntiEntropy.Interval = 0
	err := c.Start()
	if err != nil {
		t.Fatalf("starting cluster: %v", err)
	}
	defer c.Close()

	_, err = c.GetNode(0).API.CreateIndex(context.Background(), c.Idx(), pilosa.IndexOptions{})
	if err != nil {
		t.Fatalf("creating index i: %v", err)
	}
	_, err = c.GetNode(0).API.CreateField(context.Background(), c.Idx(), "f", pilosa.OptFieldTypeSet(pilosa.DefaultCacheType, pilosa.DefaultCacheSize))
	if err != nil {
		t.Fatalf("creating field f: %v", err)
	}

	hldr0 := &test.Holder{Holder: c.GetNode(0).Server.Holder()}
	hldr1 := &test.Holder{Holder: c.GetNode(1).Server.Holder()}
	hldr2 := &test.Holder{Holder: c.GetNode(2).Server.Holder()}

	// Set data on the local holder that should be cleared
	// because it's the only instance of this value.
	hldr0.SetBit(c.Idx(), "f", 0, 30)

	// Set similar data on the replicas, but
	// different from what's on local. This should end
	// up being set on all replicas
	hldr1.SetBit(c.Idx(), "f", 0, 20)
	hldr2.SetBit(c.Idx(), "f", 0, 20)

	err = c.GetNode(0).Server.SyncData()
	if err != nil {
		t.Fatalf("syncing node 0: %v", err)
	}

	// Verify data is the same on all nodes.
	for i, hldr := range []*test.Holder{hldr0, hldr1, hldr2} {
		if a := hldr.Row(c.Idx(), "f", 0).Columns(); !reflect.DeepEqual(a, []uint64{20}) {
			t.Errorf("unexpected columns(%d): %+v", i, a)
		}
	}
}

// Ensure holder can sync time quantum views with a remote holder.
func TestHolderSyncer_TimeQuantum(t *testing.T) {
	c := test.MustUnsharedCluster(t, 2)
	c.GetIdleNode(0).Config.Cluster.ReplicaN = 2
	c.GetIdleNode(0).Config.AntiEntropy.Interval = 0
	c.GetIdleNode(1).Config.Cluster.ReplicaN = 2
	c.GetIdleNode(1).Config.AntiEntropy.Interval = 0
	err := c.Start()
	if err != nil {
		t.Fatalf("starting cluster: %v", err)
	}
	defer c.Close()

	quantum := "D"

	_, err = c.GetNode(0).API.CreateIndex(context.Background(), c.Idx(), pilosa.IndexOptions{})
	if err != nil {
		t.Fatalf("creating index i: %v", err)
	}
	_, err = c.GetNode(0).API.CreateField(context.Background(), c.Idx(), "f", pilosa.OptFieldTypeTime(pilosa.TimeQuantum(quantum), "0"))
	if err != nil {
		t.Fatalf("creating field f: %v", err)
	}

	hldr0 := &test.Holder{Holder: c.GetNode(0).Server.Holder()}
	hldr1 := &test.Holder{Holder: c.GetNode(1).Server.Holder()}

	// Set data on the local holder for node0.
	t1 := time.Date(2018, 8, 1, 12, 30, 0, 0, time.UTC)
	t2 := time.Date(2018, 8, 2, 12, 30, 0, 0, time.UTC)
	hldr0.SetBitTime(c.Idx(), "f", 0, 1, &t1)
	hldr0.SetBitTime(c.Idx(), "f", 0, 2, &t2)

	// Set data on node1.
	hldr1.SetBitTime(c.Idx(), "f", 0, 22, &t2)

	err = c.GetNode(0).Server.SyncData()
	if err != nil {
		t.Fatalf("syncing node 0: %v", err)
	}

	// Verify data is the same on both nodes.
	for i, hldr := range []*test.Holder{hldr0, hldr1} {
		if a := hldr.RowTime(c.Idx(), "f", 0, t1, quantum).Columns(); !reflect.DeepEqual(a, []uint64{1}) {
			t.Errorf("unexpected columns(%d/0): %+v", i, a)
		}
		if a := hldr.RowTime(c.Idx(), "f", 0, t2, quantum).Columns(); !reflect.DeepEqual(a, []uint64{2, 22}) {
			t.Errorf("unexpected columns(%d/0): %+v", i, a)
		}
	}
}

// Ensure holder can sync integer views with a remote holder.
func TestHolderSyncer_IntField(t *testing.T) {
	t.Run("BasicSync", func(t *testing.T) {
		c := test.MustUnsharedCluster(t, 2)
		c.GetIdleNode(0).Config.Cluster.ReplicaN = 2
		c.GetIdleNode(0).Config.AntiEntropy.Interval = 0
		c.GetIdleNode(1).Config.Cluster.ReplicaN = 2
		c.GetIdleNode(1).Config.AntiEntropy.Interval = 0
		err := c.Start()
		if err != nil {
			t.Fatalf("starting cluster: %v", err)
		}
		defer c.Close()

		var idx0 *pilosa.Index
		idx0, err = c.GetNode(0).API.CreateIndex(context.Background(), c.Idx(), pilosa.IndexOptions{})
		_ = idx0
		if err != nil {
			t.Fatalf("creating index i: %v", err)
		}
		_, err = c.GetNode(0).API.CreateField(context.Background(), c.Idx(), "f", pilosa.OptFieldTypeInt(0, 100))
		if err != nil {
			t.Fatalf("creating field f: %v", err)
		}

		hldr0 := &test.Holder{Holder: c.GetNode(0).Server.Holder()}
		hldr1 := &test.Holder{Holder: c.GetNode(1).Server.Holder()}

		// Set data on the local holder for node0. columnID=1, value=1
		hldr0.SetValue(c.Idx(), "f", 1, 1)

		// in c0 expect the 1 bit

		// Set data on node1. columnID=2, value=2
		idx1 := hldr1.SetValue(c.Idx(), "f", 2, 2)
		_ = idx1

		err = c.GetNode(0).Server.SyncData()
		if err != nil {
			t.Fatalf("syncing node 0: %v", err)
		}

		// expect 3 rows, the 1 bit + 2 rows for the 2 value as BSI. But, we only see that c0 overwrote c1.

		// Problem is: data at c1 was replaced by c0, instead of being merged with existing c1.
		// Problem is: data at c0 did not receive and merge the c1 data.

		// Verify data is the same on both nodes.
		for i, hldr := range []*test.Holder{hldr0, hldr1} {
			if a, exists := hldr.Value(c.Idx(), "f", 1); !exists || a != 1 {
				// expects exists==true, a==1
				t.Errorf("unexpected value(node%d/0): a:%d, exists: %v", i, a, exists)
			}
			if a, exists := hldr.Value(c.Idx(), "f", 2); exists {
				t.Errorf("unexpected value(node%d/1): a:%d, exists: %v", i, a, exists)
			}
		}
	})

	t.Run("MultiShard", func(t *testing.T) {
		t.Skip() // skipping due to changed partitioning strategy
		c := test.MustUnsharedCluster(t, 2)
		c.GetIdleNode(0).Config.Cluster.ReplicaN = 2
		c.GetIdleNode(0).Config.AntiEntropy.Interval = 0
		c.GetIdleNode(1).Config.Cluster.ReplicaN = 2
		c.GetIdleNode(1).Config.AntiEntropy.Interval = 0
		err := c.Start()
		if err != nil {
			t.Fatalf("starting cluster: %v", err)
		}
		defer c.Close()

		var idx0 *pilosa.Index
		_ = idx0
		idx0, err = c.GetNode(0).API.CreateIndex(context.Background(), c.Idx(), pilosa.IndexOptions{})
		_ = idx0
		if err != nil {
			t.Fatalf("creating index i: %v", err)
		}
		_, err = c.GetNode(0).API.CreateField(context.Background(), c.Idx(), "f", pilosa.OptFieldTypeInt(math.MinInt64, math.MaxInt64))
		if err != nil {
			t.Fatalf("creating field f: %v", err)
		}

		hldr0 := &test.Holder{Holder: c.GetNode(0).Server.Holder()}
		hldr1 := &test.Holder{Holder: c.GetNode(1).Server.Holder()}

		// Set data on the local holder for node0.
		hldr0.SetValue(c.Idx(), "f", 1*pilosa.ShardWidth, 11)
		hldr0.SetValue(c.Idx(), "f", 3*pilosa.ShardWidth, 32)
		hldr0.SetValue(c.Idx(), "f", 4*pilosa.ShardWidth, math.MinInt32)
		hldr0.SetValue(c.Idx(), "f", 7*pilosa.ShardWidth, math.MinInt32)

		// Set data on node1.
		hldr1.SetValue(c.Idx(), "f", 0*pilosa.ShardWidth, 2)
		hldr1.SetValue(c.Idx(), "f", 2*pilosa.ShardWidth, 22)
		hldr1.SetValue(c.Idx(), "f", 4*pilosa.ShardWidth, math.MaxInt32)
		hldr1.SetValue(c.Idx(), "f", 7*pilosa.ShardWidth, math.MaxInt32)

		// Primary for shards (for index c.Idx()):
		// node0: [0,3,7]
		// node1: [1,2,4]

		err = c.GetNode(0).Server.SyncData()
		if err != nil {
			t.Fatalf("syncing node 0: %v", err)
		}
		err = c.GetNode(1).Server.SyncData()
		if err != nil {
			t.Fatalf("syncing node 1: %v", err)
		}

		// dump the rbf keys for both c0 and c1

		// Verify data is the same on both nodes.
		for i, hldr := range []*test.Holder{hldr0, hldr1} {
			if a := hldr.Range(c.Idx(), "f", pql.GT, 0); !reflect.DeepEqual(a.Columns(), []uint64{2 * pilosa.ShardWidth, 3 * pilosa.ShardWidth, 4 * pilosa.ShardWidth}) {
				t.Errorf("unexpected columns(node%d/0): %d", i, a.Columns())
			}
			if a := hldr.Range(c.Idx(), "f", pql.LT, 0); !reflect.DeepEqual(a.Columns(), []uint64{7 * pilosa.ShardWidth}) {
				t.Errorf("unexpected columns(node%d/0): %d", i, a.Columns())
			}
		}
	})
}
