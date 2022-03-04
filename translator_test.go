// Copyright 2021 Molecula Corp. All rights reserved.
package pilosa_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	pilosa "github.com/molecula/featurebase/v3"
	"github.com/molecula/featurebase/v3/boltdb"
	"github.com/molecula/featurebase/v3/mock"
	"github.com/molecula/featurebase/v3/server"
	"github.com/molecula/featurebase/v3/test"
	"github.com/molecula/featurebase/v3/topology"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

func TestInMemTranslateStore_TranslateID(t *testing.T) {
	s := pilosa.NewInMemTranslateStore("IDX", "FLD", 0, topology.DefaultPartitionN)

	// Setup initial keys.
	if _, err := s.CreateKeys("foo"); err != nil {
		t.Fatal(err)
	} else if _, err := s.CreateKeys("bar"); err != nil {
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

func TestTranslation_KeyNotFound(t *testing.T) {
	c := test.MustRunCluster(t, 4,
		[]server.CommandOption{
			server.OptCommandServerOptions(
				pilosa.OptServerNodeID("node0"),
				pilosa.OptServerOpenTranslateStore(boltdb.OpenTranslateStore),
				pilosa.OptServerOpenTranslateReader(pilosa.GetOpenTranslateReaderFunc(nil)),
			)},
		[]server.CommandOption{
			server.OptCommandServerOptions(
				pilosa.OptServerNodeID("node1"),
				pilosa.OptServerOpenTranslateStore(boltdb.OpenTranslateStore),
				pilosa.OptServerOpenTranslateReader(pilosa.GetOpenTranslateReaderFunc(nil)),
			)},
		[]server.CommandOption{
			server.OptCommandServerOptions(
				pilosa.OptServerNodeID("node2"),
				pilosa.OptServerOpenTranslateStore(boltdb.OpenTranslateStore),
				pilosa.OptServerOpenTranslateReader(pilosa.GetOpenTranslateReaderFunc(nil)),
			)},
		[]server.CommandOption{
			server.OptCommandServerOptions(
				pilosa.OptServerNodeID("node3"),
				pilosa.OptServerOpenTranslateStore(boltdb.OpenTranslateStore),
				pilosa.OptServerOpenTranslateReader(pilosa.GetOpenTranslateReaderFunc(nil)),
			)},
	)
	defer c.Close()

	node0 := c.GetNode(0)
	node1 := c.GetNode(1)
	node2 := c.GetNode(2)
	node3 := c.GetNode(3)

	ctx := context.Background()
	idx, fld := "i", "f"
	// Create an index with keys.
	if _, err := node0.API.CreateIndex(ctx, idx, pilosa.IndexOptions{Keys: true}); err != nil {
		t.Fatal(err)
	}
	// Create an index with keys.
	if _, err := node0.API.CreateField(ctx, idx, fld, pilosa.OptFieldKeys()); err != nil {
		t.Fatal(err)
	}

	// write a new key and get id
	req, err := node0.API.Serializer.Marshal(&pilosa.TranslateKeysRequest{
		Index:       idx,
		Field:       fld,
		Keys:        []string{"k1"},
		NotWritable: false,
	})
	if err != nil {
		t.Fatal(err)
	}

	if buf, err := node0.API.TranslateKeys(ctx, bytes.NewReader(req)); err != nil {
		t.Fatal(err)
	} else {
		var resp pilosa.TranslateKeysResponse
		if err = node0.API.Serializer.Unmarshal(buf, &resp); err != nil {
			t.Fatal(err)
		}
		id1 := resp.IDs[0]

		// read non-existing key
		req, err = node3.API.Serializer.Marshal(&pilosa.TranslateKeysRequest{
			Index:       idx,
			Field:       fld,
			Keys:        []string{"k2"},
			NotWritable: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		if buf, err = node3.API.TranslateKeys(ctx, bytes.NewReader(req)); err != nil {
			t.Fatal(err)
		}
		if err = node3.API.Serializer.Unmarshal(buf, &resp); err != nil {
			t.Fatal(err)
		} else if resp.IDs != nil {
			t.Fatalf("TranslateKeys(%+v): expected: nil, got: %d", string(req), resp)
		}

		req, err = node1.API.Serializer.Marshal(&pilosa.TranslateKeysRequest{
			Index:       idx,
			Keys:        []string{"k2"},
			NotWritable: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		if buf, err = node1.API.TranslateKeys(ctx, bytes.NewReader(req)); err != nil {
			t.Fatal(err)
		}
		if err = node1.API.Serializer.Unmarshal(buf, &resp); err != nil {
			t.Fatal(err)
		} else if resp.IDs != nil {
			t.Fatalf("TranslateKeys(%+v): expected: nil, got: %d", req, resp)
		}

		req, err = node2.API.Serializer.Marshal(&pilosa.TranslateKeysRequest{
			Index:       idx,
			Field:       fld,
			Keys:        []string{"k2", "k1"},
			NotWritable: false,
		})
		if err != nil {
			t.Fatal(err)
		}
		if buf, err = node2.API.TranslateKeys(ctx, bytes.NewReader(req)); err != nil {
			t.Fatal(err)
		}
		if err = node2.API.Serializer.Unmarshal(buf, &resp); err != nil {
			t.Fatal(err)
		}
		if resp.IDs[0] != id1+1 || resp.IDs[1] != id1 {
			t.Fatalf("TranslateKeys(%+v): expected: %d,%d, got: %d,%d", req, id1+1, id1, resp.IDs[0], resp.IDs[1])
		}
	}
}

func TestInMemTranslateStore_ReadKey(t *testing.T) {
	s := pilosa.NewInMemTranslateStore("IDX", "FLD", 0, topology.DefaultPartitionN)

	ids, err := s.FindKeys("foo")
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 0 {
		t.Errorf("unexpected IDs: %v", ids)
	}

	// Ensure next key autoincrements.
	ids, err = s.CreateKeys("foo")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := ids["foo"], uint64(1); got != want {
		t.Fatalf("TranslateKey()=%d, want %d", got, want)
	}

	ids, err = s.FindKeys("foo")
	if err != nil {
		t.Fatal(err)
	}
	if got, want := ids["foo"], uint64(1); got != want {
		t.Fatalf("TranslateKey()=%d, want %d", got, want)
	}
}

// Test key translation with multiple nodes.
func TestTranslation_Primary(t *testing.T) {
	// Ensure that field key translations requests sent to
	// non-primary nodes are forwarded to the primary.
	t.Run("ForwardFieldKey", func(t *testing.T) {
		// Start a 2-node cluster.
		c := test.MustRunCluster(t, 3,
			[]server.CommandOption{
				server.OptCommandServerOptions(
					pilosa.OptServerNodeID("node0"),
					pilosa.OptServerOpenTranslateStore(boltdb.OpenTranslateStore),
					pilosa.OptServerOpenTranslateReader(pilosa.GetOpenTranslateReaderFunc(nil)),
				)},
			[]server.CommandOption{
				server.OptCommandServerOptions(
					pilosa.OptServerNodeID("node1"),
					pilosa.OptServerOpenTranslateStore(boltdb.OpenTranslateStore),
					pilosa.OptServerOpenTranslateReader(pilosa.GetOpenTranslateReaderFunc(nil)),
				)},
			[]server.CommandOption{
				server.OptCommandServerOptions(
					pilosa.OptServerNodeID("node2"),
					pilosa.OptServerOpenTranslateStore(boltdb.OpenTranslateStore),
					pilosa.OptServerOpenTranslateReader(pilosa.GetOpenTranslateReaderFunc(nil)),
				)},
		)
		defer c.Close()

		node0 := c.GetPrimary()
		node1 := c.GetNonPrimary()

		ctx := context.Background()
		idx := "i"
		fld := "f"

		// Create an index without keys.
		if _, err := node1.API.CreateIndex(ctx, idx,
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

			// Send a translation request to node1 (non-primary).
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

func TestTranslation_TranslateIDsOnCluster(t *testing.T) {
	c := test.MustRunCluster(t, 4,
		[]server.CommandOption{
			server.OptCommandServerOptions(
				pilosa.OptServerNodeID("node0"),
				pilosa.OptServerOpenTranslateStore(boltdb.OpenTranslateStore),
				pilosa.OptServerOpenTranslateReader(pilosa.GetOpenTranslateReaderFunc(nil)),
			)},
		[]server.CommandOption{
			server.OptCommandServerOptions(
				pilosa.OptServerNodeID("node1"),
				pilosa.OptServerOpenTranslateStore(boltdb.OpenTranslateStore),
				pilosa.OptServerOpenTranslateReader(pilosa.GetOpenTranslateReaderFunc(nil)),
			)},
		[]server.CommandOption{
			server.OptCommandServerOptions(
				pilosa.OptServerNodeID("node2"),
				pilosa.OptServerOpenTranslateStore(boltdb.OpenTranslateStore),
				pilosa.OptServerOpenTranslateReader(pilosa.GetOpenTranslateReaderFunc(nil)),
			)},
		[]server.CommandOption{
			server.OptCommandServerOptions(
				pilosa.OptServerNodeID("node3"),
				pilosa.OptServerOpenTranslateStore(boltdb.OpenTranslateStore),
				pilosa.OptServerOpenTranslateReader(pilosa.GetOpenTranslateReaderFunc(nil)),
			)},
	)
	defer c.Close()

	coord := c.GetPrimary()
	other := c.GetNonPrimary()

	ctx := context.Background()
	idx, fld := "i", "f"
	// Create an index with keys.
	if _, err := coord.API.CreateIndex(ctx, idx, pilosa.IndexOptions{Keys: true}); err != nil {
		t.Fatal(err)
	}

	// Create an index with keys.
	if _, err := coord.API.CreateField(ctx, idx, fld, pilosa.OptFieldKeys()); err != nil {
		t.Fatal(err)
	}

	keys := []string{"k0", "k1", "k2", "k3", "k4", "k5", "k6", "k7", "k8", "k9"}
	// write a new key and get id
	req, err := coord.API.Serializer.Marshal(&pilosa.TranslateKeysRequest{
		Index:       idx,
		Field:       fld,
		Keys:        keys,
		NotWritable: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	if buf, err := coord.API.TranslateKeys(ctx, bytes.NewReader(req)); err != nil {
		t.Fatal(err)
	} else {
		var (
			respKeys pilosa.TranslateKeysResponse
			respIDs  pilosa.TranslateIDsResponse
		)
		if err = other.API.Serializer.Unmarshal(buf, &respKeys); err != nil {
			t.Fatal(err)
		}
		ids := respKeys.IDs

		// translate ids
		req, err = other.API.Serializer.Marshal(&pilosa.TranslateIDsRequest{
			Index: idx,
			Field: fld,
			IDs:   ids,
		})
		if err != nil {
			t.Fatal(err)
		}
		if buf, err = other.API.TranslateIDs(ctx, bytes.NewReader(req)); err != nil {
			t.Fatal(err)
		}
		if err = other.API.Serializer.Unmarshal(buf, &respIDs); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(respIDs.Keys, keys) {
			t.Fatalf("TranslateIDs(%+v): expected: %+v, got: %+v", ids, keys, respIDs.Keys)
		}
	}
}

func TestTranslation_Cluster_CreateFind(t *testing.T) {
	c := test.MustRunCluster(t, 3)
	defer c.Close()

	c.CreateField(t, "i", pilosa.IndexOptions{Keys: true}, "f", pilosa.OptFieldKeys())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Use the alphabet to test keys.
	testKeys := make(map[string]struct{})
	for i := 'a'; i <= 'z'; i++ {
		testKeys[string(i)] = struct{}{}
	}

	t.Run("Index", func(t *testing.T) {
		// Create all index keys, split across nodes.
		{
			parts := make([][]string, len(c.Nodes))
			{
				// Randomly partition the keys.
				i := 0
				for k := range testKeys {
					parts[i%len(c.Nodes)] = append(parts[i%len(c.Nodes)], k)
					i++
				}
			}

			// Create some keys on each node.
			var g errgroup.Group
			defer g.Wait() //nolint:errcheck
			for i, keys := range parts {
				i, keys := i, keys
				g.Go(func() error {
					_, err := c.GetNode(i).API.CreateIndexKeys(ctx, "i", keys...)
					return err
				})
			}
			if err := g.Wait(); err != nil {
				t.Errorf("creating keys: %v", err)
				return
			}
		}

		// Check that all index keys exist, and consistently map to the same IDs.
		{
			// Convert the keys to a list.
			keyList := make([]string, 0, len(testKeys))
			for k := range testKeys {
				keyList = append(keyList, k)
			}

			// Obtain authoritative translations for the keys.
			translations, err := c.GetPrimary().API.FindIndexKeys(ctx, "i", keyList...)
			if err != nil {
				t.Errorf("obtaining authoritative translations: %v", err)
				return
			}
			for _, k := range keyList {
				if _, ok := translations[k]; !ok {
					t.Errorf("key %q is missing", k)
				}
			}

			// Check that all nodes agree on these translations.
			var g errgroup.Group
			defer g.Wait() //nolint:errcheck
			for i, n := range c.Nodes {
				x, api := i, n.API
				g.Go(func() (err error) {
					defer func() { err = errors.Wrapf(err, "translating on node %d", x) }()
					localTranslations, err := api.FindIndexKeys(ctx, "i", keyList...)
					if err != nil {
						return errors.Wrap(err, "finding translations")
					}
					return compareTranslations(translations, localTranslations)
				})
			}
			if err := g.Wait(); err != nil {
				t.Errorf("finding keys: %v", err)
				return
			}

			// Check that re-invoking create returns the original translations.
			for i, n := range c.Nodes {
				x, api := i, n.API
				g.Go(func() (err error) {
					defer func() { err = errors.Wrapf(err, "translating on node %d", x) }()
					localTranslations, err := api.CreateIndexKeys(ctx, "i", keyList...)
					if err != nil {
						return errors.Wrap(err, "finding translations")
					}
					return compareTranslations(translations, localTranslations)
				})
			}
			if err := g.Wait(); err != nil {
				t.Errorf("checking re-create of keys: %v", err)
				return
			}
		}
	})
	t.Run("Field", func(t *testing.T) {
		// Create all field keys, split across nodes.
		{
			parts := make([][]string, len(c.Nodes))
			{
				// Randomly partition the keys.
				i := 0
				for k := range testKeys {
					parts[i%len(c.Nodes)] = append(parts[i%len(c.Nodes)], k)
					i++
				}
			}

			// Create some keys on each node.
			var g errgroup.Group
			defer g.Wait() //nolint:errcheck
			for i, keys := range parts {
				i, keys := i, keys
				g.Go(func() error {
					_, err := c.GetNode(i).API.CreateFieldKeys(ctx, "i", "f", keys...)
					return err
				})
			}
			if err := g.Wait(); err != nil {
				t.Errorf("creating keys: %v", err)
				return
			}
		}

		// Check that all field keys exist, and consistently map to the same IDs.
		{
			// Convert the keys to a list.
			keyList := make([]string, 0, len(testKeys))
			for k := range testKeys {
				keyList = append(keyList, k)
			}

			// Obtain authoritative translations for the keys.
			translations, err := c.GetPrimary().API.FindFieldKeys(ctx, "i", "f", keyList...)
			if err != nil {
				t.Errorf("obtaining authoritative translations: %v", err)
				return
			}
			for _, k := range keyList {
				if _, ok := translations[k]; !ok {
					t.Errorf("key %q is missing", k)
				}
			}

			// Check that all nodes agree on these translations.
			var g errgroup.Group
			defer g.Wait() //nolint:errcheck
			for i, n := range c.Nodes {
				x, api := i, n.API
				g.Go(func() (err error) {
					defer func() { err = errors.Wrapf(err, "translating on node %d", x) }()
					localTranslations, err := api.FindFieldKeys(ctx, "i", "f", keyList...)
					if err != nil {
						return errors.Wrap(err, "finding translations")
					}
					return compareTranslations(translations, localTranslations)
				})
			}
			if err := g.Wait(); err != nil {
				t.Errorf("finding keys: %v", err)
				return
			}

			// Check that re-invoking create returns the original translations.
			for i, n := range c.Nodes {
				x, api := i, n.API
				g.Go(func() (err error) {
					defer func() { err = errors.Wrapf(err, "translating on node %d", x) }()
					localTranslations, err := api.CreateFieldKeys(ctx, "i", "f", keyList...)
					if err != nil {
						return errors.Wrap(err, "finding translations")
					}
					return compareTranslations(translations, localTranslations)
				})
			}
			if err := g.Wait(); err != nil {
				t.Errorf("checking re-create of keys: %v", err)
				return
			}
		}
	})
}

func TestTranslation_Cluster_CreateFindUnkeyed(t *testing.T) {
	c := test.MustRunCluster(t, 3)
	defer c.Close()

	c.CreateField(t, "i", pilosa.IndexOptions{}, "f")

	t.Run("Index", func(t *testing.T) {
		t.Run("Create", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			_, err := c.GetNonPrimary().API.CreateIndexKeys(ctx, "i", "foo")
			if err == nil {
				t.Fatal("unexpected success")
			}
			expect := `cannot create keys on unkeyed index "i"`
			if got := err.Error(); got != expect {
				t.Fatalf("expected error %q but got %q", expect, got)
			}
		})
		t.Run("Find", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			_, err := c.GetNonPrimary().API.FindIndexKeys(ctx, "i", "foo")
			if err == nil {
				t.Fatal("unexpected success")
			}
			expect := `cannot find keys on unkeyed index "i"`
			if got := err.Error(); got != expect {
				t.Fatalf("expected error %q but got %q", expect, got)
			}
		})
	})
	t.Run("Field", func(t *testing.T) {
		t.Run("Create", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			_, err := c.GetNonPrimary().API.CreateFieldKeys(ctx, "i", "f", "foo")
			if err == nil {
				t.Fatal("unexpected success")
			}
			expect := `cannot create keys on unkeyed field "f"`
			if got := err.Error(); got != expect {
				t.Fatalf("expected error %q but got %q", expect, got)
			}
		})
		t.Run("Find", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			_, err := c.GetNonPrimary().API.FindFieldKeys(ctx, "i", "f", "foo")
			if err == nil {
				t.Fatal("unexpected success")
			}
			expect := `cannot find keys on unkeyed field "f"`
			if got := err.Error(); got != expect {
				t.Fatalf("expected error %q but got %q", expect, got)
			}
		})
	})
}

func compareTranslations(expected, got map[string]uint64) error {
	for key, id := range got {
		if realID, ok := expected[key]; !ok {
			return errors.Errorf("unexpected key %q mapped to ID %d", key, id)
		} else if id != realID {
			return errors.Errorf("mismatched translation: expected %q:%d but got %q:%d", key, realID, key, id)
		}
	}
	for key, realID := range expected {
		if id, ok := got[key]; !ok {
			return errors.Errorf("missing translation of key %q", key)
		} else if id != realID {
			return errors.Errorf("mismatched translation: expected %q:%d but got %q:%d", key, realID, key, id)
		}
	}
	return nil
}
