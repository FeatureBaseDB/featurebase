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
	"errors"
	"fmt"
	"io"
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/pilosa/pilosa/v2"
	"github.com/pilosa/pilosa/v2/boltdb"
	"github.com/pilosa/pilosa/v2/http"
	"github.com/pilosa/pilosa/v2/mock"
	"github.com/pilosa/pilosa/v2/server"
	"github.com/pilosa/pilosa/v2/test"
)

func TestInMemTranslateStore_TranslateKey(t *testing.T) {
	s := pilosa.NewInMemTranslateStore("IDX", "FLD", 0, pilosa.DefaultPartitionN)

	// Ensure initial key translates to ID 1.
	if id, err := s.TranslateKey("foo"); err != nil {
		t.Fatal(err)
	} else if got, want := id, uint64(1); got != want {
		t.Fatalf("TranslateKey()=%d, want %d", got, want)
	}

	// Ensure next key autoincrements.
	if id, err := s.TranslateKey("bar"); err != nil {
		t.Fatal(err)
	} else if got, want := id, uint64(2); got != want {
		t.Fatalf("TranslateKey()=%d, want %d", got, want)
	}

	// Ensure retranslating existing key returns original ID.
	if id, err := s.TranslateKey("foo"); err != nil {
		t.Fatal(err)
	} else if got, want := id, uint64(1); got != want {
		t.Fatalf("TranslateKey()=%d, want %d", got, want)
	}
}

func TestInMemTranslateStore_TranslateID(t *testing.T) {
	s := pilosa.NewInMemTranslateStore("IDX", "FLD", 0, pilosa.DefaultPartitionN)

	// Setup initial keys.
	if _, err := s.TranslateKey("foo"); err != nil {
		t.Fatal(err)
	} else if _, err := s.TranslateKey("bar"); err != nil {
		t.Fatal(err)
	}

	// Ensure IDs can be translated back to keys.
	if key, err := s.TranslateID(1); err != nil {
		t.Fatal(err)
	} else if got, want := key, "foo"; got != want {
		t.Fatalf("TranslateID()=%s, want %s", got, want)
	}

	if key, err := s.TranslateID(2); err != nil {
		t.Fatal(err)
	} else if got, want := key, "bar"; got != want {
		t.Fatalf("TranslateID()=%s, want %s", got, want)
	}
}

func TestMultiTranslateEntryReader(t *testing.T) {
	t.Run("None", func(t *testing.T) {
		r := pilosa.NewMultiTranslateEntryReader(context.Background(), nil)
		defer r.Close()

		var entry pilosa.TranslateEntry
		if err := r.ReadEntry(&entry); err != io.EOF {
			t.Fatal(err)
		}
	})

	t.Run("Single", func(t *testing.T) {
		var r0 mock.TranslateEntryReader
		r0.CloseFunc = func() error { return nil }
		r0.ReadEntryFunc = func(entry *pilosa.TranslateEntry) error {
			*entry = pilosa.TranslateEntry{Index: "i", Field: "f", ID: 1, Key: "foo"}
			return nil
		}
		r := pilosa.NewMultiTranslateEntryReader(context.Background(), []pilosa.TranslateEntryReader{&r0})
		defer r.Close()

		var entry pilosa.TranslateEntry
		if err := r.ReadEntry(&entry); err != nil {
			t.Fatal(err)
		} else if diff := cmp.Diff(entry, pilosa.TranslateEntry{Index: "i", Field: "f", ID: 1, Key: "foo"}); diff != "" {
			t.Fatal(diff)
		}
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Multi", func(t *testing.T) {
		ready0, ready1 := make(chan struct{}), make(chan struct{})

		var r0 mock.TranslateEntryReader
		r0.CloseFunc = func() error { return nil }
		r0.ReadEntryFunc = func(entry *pilosa.TranslateEntry) error {
			if _, ok := <-ready0; !ok {
				return io.EOF
			}
			*entry = pilosa.TranslateEntry{Index: "i0", Field: "f0", ID: 1, Key: "foo"}
			return nil
		}

		var r1 mock.TranslateEntryReader
		r1.CloseFunc = func() error { return nil }
		r1.ReadEntryFunc = func(entry *pilosa.TranslateEntry) error {
			if _, ok := <-ready1; !ok {
				return io.EOF
			}
			*entry = pilosa.TranslateEntry{Index: "i1", Field: "f1", ID: 2, Key: "bar"}
			return nil
		}

		r := pilosa.NewMultiTranslateEntryReader(context.Background(), []pilosa.TranslateEntryReader{&r1, &r0})
		defer r.Close()

		// Ensure r0 is read first
		ready0 <- struct{}{}
		var entry pilosa.TranslateEntry
		if err := r.ReadEntry(&entry); err != nil {
			t.Fatal(err)
		} else if diff := cmp.Diff(entry, pilosa.TranslateEntry{Index: "i0", Field: "f0", ID: 1, Key: "foo"}); diff != "" {
			t.Fatal(diff)
		}

		// Unblock r1.
		ready1 <- struct{}{}

		// Read from r1 next.
		if err := r.ReadEntry(&entry); err != nil {
			t.Fatal(err)
		} else if diff := cmp.Diff(entry, pilosa.TranslateEntry{Index: "i1", Field: "f1", ID: 2, Key: "bar"}); diff != "" {
			t.Fatal(diff)
		}

		// Close both readers.
		close(ready0)
		close(ready1)
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Error", func(t *testing.T) {
		var r0 mock.TranslateEntryReader
		r0.CloseFunc = func() error { return nil }
		r0.ReadEntryFunc = func(entry *pilosa.TranslateEntry) error {
			return errors.New("marker")
		}
		r := pilosa.NewMultiTranslateEntryReader(context.Background(), []pilosa.TranslateEntryReader{&r0})
		defer r.Close()

		var entry pilosa.TranslateEntry
		if err := r.ReadEntry(&entry); err == nil || err.Error() != `marker` {
			t.Fatalf("unexpected error: %s", err)
		}
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
	})
}

// Test key translation with multiple nodes.
func TestTranslation_Reset(t *testing.T) {
	// We need to ensure that the translate key partitions for each
	// node are getting set as read-only based on the full cluster,
	// not just the state of the cluster at the time of the individual
	// node restart.
	t.Run("RollingRestart", func(t *testing.T) {
		// Start a 4-node cluster.
		// Note that the prefix on the nodeID is intentional; it puts the
		// nodes in a specific order which exercises the condition for
		// which we are testing. In a normal use case, these would be
		// randomly generated uuids, so this is mimicking that.
		c := test.MustRunCluster(t, 4,
			[]server.CommandOption{
				server.OptCommandServerOptions(
					pilosa.OptServerIsCoordinator(true),
					pilosa.OptServerNodeID("2node0"),
					pilosa.OptServerOpenTranslateStore(boltdb.OpenTranslateStore),
					pilosa.OptServerOpenTranslateReader(http.GetOpenTranslateReaderFunc(nil)),
				)},
			[]server.CommandOption{
				server.OptCommandServerOptions(
					pilosa.OptServerIsCoordinator(false),
					pilosa.OptServerNodeID("4node1"),
					pilosa.OptServerOpenTranslateStore(boltdb.OpenTranslateStore),
					pilosa.OptServerOpenTranslateReader(http.GetOpenTranslateReaderFunc(nil)),
				)},
			[]server.CommandOption{
				server.OptCommandServerOptions(
					pilosa.OptServerIsCoordinator(false),
					pilosa.OptServerNodeID("3node2"),
					pilosa.OptServerOpenTranslateStore(boltdb.OpenTranslateStore),
					pilosa.OptServerOpenTranslateReader(http.GetOpenTranslateReaderFunc(nil)),
				)},
			[]server.CommandOption{
				server.OptCommandServerOptions(
					pilosa.OptServerIsCoordinator(false),
					pilosa.OptServerNodeID("1node3"),
					pilosa.OptServerOpenTranslateStore(boltdb.OpenTranslateStore),
					pilosa.OptServerOpenTranslateReader(http.GetOpenTranslateReaderFunc(nil)),
				)},
		)
		defer c.Close()

		node0 := c.GetNode(0)
		node1 := c.GetNode(1)
		node2 := c.GetNode(2)
		node3 := c.GetNode(3)

		ctx := context.Background()
		idx := "i"

		// Create an index with keys.
		if _, err := node0.API.CreateIndex(ctx, idx,
			pilosa.IndexOptions{
				Keys: true,
			}); err != nil {
			t.Fatal(err)
		}

		// Stop the cluster.
		if err := node0.Command.Close(); err != nil {
			t.Fatal(err)
		}
		if err := node1.Command.Close(); err != nil {
			t.Fatal(err)
		}
		if err := node2.Command.Close(); err != nil {
			t.Fatal(err)
		}
		if err := node3.Command.Close(); err != nil {
			t.Fatal(err)
		}

		// Restart the nodes serially.
		if err := node0.SoftOpen(); err != nil {
			t.Fatal(err)
		}
		gossipSeeds := []string{node0.GossipAddress()}

		node1.Config.Gossip.Seeds = gossipSeeds
		if err := node1.SoftOpen(); err != nil {
			t.Fatal(err)
		}
		node2.Config.Gossip.Seeds = gossipSeeds
		if err := node2.SoftOpen(); err != nil {
			t.Fatal(err)
		}
		node3.Config.Gossip.Seeds = gossipSeeds
		if err := node3.SoftOpen(); err != nil {
			t.Fatal(err)
		}

		// Send a key translation request that triggers
		// a read-only translate store error if the
		// translate store sync is not reset correctly.

		// Generate request body for translate row keys request
		reqBody, err := node0.API.Serializer.Marshal(&pilosa.TranslateKeysRequest{
			Index: idx,
			Keys:  []string{"a1"},
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := node0.API.TranslateKeys(ctx, bytes.NewReader(reqBody)); err != nil {
			t.Fatal(err)
		}
	})
}

// Test index key translation replication under node failure.
func TestTranslation_Replication(t *testing.T) {
	t.Run("Replication", func(t *testing.T) {
		c := test.MustRunCluster(t, 3,
			[]server.CommandOption{
				server.OptCommandServerOptions(
					pilosa.OptServerIsCoordinator(true),
					pilosa.OptServerOpenTranslateStore(boltdb.OpenTranslateStore),
					pilosa.OptServerOpenTranslateReader(http.GetOpenTranslateReaderFunc(nil)),
					pilosa.OptServerReplicaN(2),
				)},
			[]server.CommandOption{
				server.OptCommandServerOptions(
					pilosa.OptServerIsCoordinator(false),
					pilosa.OptServerOpenTranslateStore(boltdb.OpenTranslateStore),
					pilosa.OptServerOpenTranslateReader(http.GetOpenTranslateReaderFunc(nil)),
					pilosa.OptServerReplicaN(2),
				)},
			[]server.CommandOption{
				server.OptCommandServerOptions(
					pilosa.OptServerIsCoordinator(false),
					pilosa.OptServerOpenTranslateStore(boltdb.OpenTranslateStore),
					pilosa.OptServerOpenTranslateReader(http.GetOpenTranslateReaderFunc(nil)),
					pilosa.OptServerReplicaN(2),
				)},
		)
		defer c.Close()

		node0 := c.GetNode(0)
		node1 := c.GetNode(1)

		ctx := context.Background()
		idx := "i"
		field := "f"

		// Create an index with keys.
		if _, err := node0.API.CreateIndex(ctx, idx,
			pilosa.IndexOptions{
				Keys: true,
			}); err != nil {
			t.Fatal(err)
		}

		if _, err := node0.API.CreateField(ctx, idx, field); err != nil {
			t.Fatal(err)
		}

		// Write data on first node.
		// these keys are a minimal example to reproduce the problem for the case of a 3-node cluster with replication factor 2
		if _, err := node0.Queryf(t, idx, "", `
	    Set("x1", f=1)
	    Set("x2", f=1)
    `); err != nil {
			t.Fatal(err)
		}

		exp := `{"results":[{"attrs":{},"columns":[],"keys":["x1","x2"]}]}`

		if !test.CheckClusterState(node0, pilosa.ClusterStateNormal, 1000) {
			t.Fatalf("unexpected node0 cluster state: %s", node0.API.State())
		} else if !test.CheckClusterState(node1, pilosa.ClusterStateNormal, 1000) {
			t.Fatalf("unexpected node1 cluster state: %s", node1.API.State())
		}

		// Verify the data exists
		node0.QueryExpect(t, idx, "", `Row(f=1)`, exp)

		// Kill one node.
		if err := c.CloseAndRemove(1); err != nil {
			t.Fatal(err)
		}

		// Verify the data exists with one node down
		node0.QueryExpect(t, idx, "", `Row(f=1)`, exp)
	})
}

// Test key translation with multiple nodes.
func TestTranslation_Coordinator(t *testing.T) {
	// Ensure that field key translations requests sent to
	// non-coordinator nodes are forwarded to the coordinator.
	t.Run("ForwardFieldKey", func(t *testing.T) {
		t.Skip("Short term skip to avoid go 1.13 test Should remove ASAP")
		// Start a 2-node cluster.
		c := test.MustRunCluster(t, 2,
			[]server.CommandOption{
				server.OptCommandServerOptions(
					pilosa.OptServerIsCoordinator(true),
					pilosa.OptServerNodeID("node0"),
					pilosa.OptServerOpenTranslateStore(boltdb.OpenTranslateStore),
					pilosa.OptServerOpenTranslateReader(http.GetOpenTranslateReaderFunc(nil)),
				)},
			[]server.CommandOption{
				server.OptCommandServerOptions(
					pilosa.OptServerIsCoordinator(false),
					pilosa.OptServerNodeID("node1"),
					pilosa.OptServerOpenTranslateStore(boltdb.OpenTranslateStore),
					pilosa.OptServerOpenTranslateReader(http.GetOpenTranslateReaderFunc(nil)),
				)},
		)
		defer c.Close()

		node0 := c.GetNode(0)
		node1 := c.GetNode(1)

		ctx := context.Background()
		idx := "i"
		fld := "f"

		// Create an index without keys.
		if _, err := node0.API.CreateIndex(ctx, idx,
			pilosa.IndexOptions{
				Keys: false,
			}); err != nil {
			t.Fatal(err)
		}

		// Create a field with keys.
		if _, err := node1.API.CreateField(ctx, idx, fld,
			pilosa.OptFieldKeys(),
		); err != nil {
			t.Fatal(err)
		}

		keys := []string{"one", "two", "three"}
		for i := range keys {
			pql := fmt.Sprintf(`Set(%d, %s="%s")`, i+1, fld, keys[i])

			// Send a translation request to node1 (non-coordinator).
			_, err := node1.API.Query(ctx,
				&pilosa.QueryRequest{Index: idx, Query: pql},
			)
			if err != nil {
				t.Fatal(err)
			}
		}

		for i := len(keys) - 1; i >= 0; i-- {
			// Read the row and ensure the key was set.
			qry := fmt.Sprintf(`Row(%s="%s")`, fld, keys[i])
			resp, err := node0.API.Query(ctx,
				&pilosa.QueryRequest{Index: idx, Query: qry},
			)
			if err != nil {
				t.Fatal(err)
			}
			row := resp.Results[0].(*pilosa.Row)
			val := uint64(i + 1)
			if cols := row.Columns(); !reflect.DeepEqual(cols, []uint64{val}) {
				t.Fatalf("unexpected columns: %+v", cols)
			}
		}
	})
}
