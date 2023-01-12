// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
package pilosa_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"reflect"
	"testing"
	"time"

	pilosa "github.com/featurebasedb/featurebase/v3"
	"github.com/featurebasedb/featurebase/v3/mock"
	"github.com/featurebasedb/featurebase/v3/test"
	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

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
	c := test.MustRunCluster(t, 4)
	defer c.Close()

	node0 := c.GetNode(0)
	node1 := c.GetNode(1)
	node2 := c.GetNode(2)
	node3 := c.GetNode(3)

	ctx := context.Background()
	index, fld := c.Idx(), "f"
	// Create an index with keys.
	if _, err := node0.API.CreateIndex(ctx, index, pilosa.IndexOptions{Keys: true}); err != nil {
		t.Fatal(err)
	}
	// Create an index with keys.
	if _, err := node0.API.CreateField(ctx, index, fld, pilosa.OptFieldKeys()); err != nil {
		t.Fatal(err)
	}

	// write a new key and get id
	req, err := node0.API.Serializer.Marshal(&pilosa.TranslateKeysRequest{
		Index:       index,
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
			Index:       index,
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
			Index:       index,
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
			Index:       index,
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

func TestTranslation_TranslateIDsOnCluster(t *testing.T) {
	c := test.MustRunCluster(t, 4)
	defer c.Close()

	coord := c.GetPrimary()
	other := c.GetNonPrimary()

	ctx := context.Background()
	index, fld := c.Idx(), "f"
	// Create an index with keys.
	if _, err := coord.API.CreateIndex(ctx, index, pilosa.IndexOptions{Keys: true}); err != nil {
		t.Fatal(err)
	}

	// Create an index with keys.
	if _, err := coord.API.CreateField(ctx, index, fld, pilosa.OptFieldKeys()); err != nil {
		t.Fatal(err)
	}

	keys := []string{"k0", "k1", "k2", "k3", "k4", "k5", "k6", "k7", "k8", "k9"}
	// write a new key and get id
	req, err := coord.API.Serializer.Marshal(&pilosa.TranslateKeysRequest{
		Index:       index,
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
			Index: index,
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

	c.CreateField(t, c.Idx(), pilosa.IndexOptions{Keys: true}, "f", pilosa.OptFieldKeys())

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
					_, err := c.GetNode(i).API.CreateIndexKeys(ctx, c.Idx(), keys...)
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
			translations, err := c.GetPrimary().API.FindIndexKeys(ctx, c.Idx(), keyList...)
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
					localTranslations, err := api.FindIndexKeys(ctx, c.Idx(), keyList...)
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
					localTranslations, err := api.CreateIndexKeys(ctx, c.Idx(), keyList...)
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
					_, err := c.GetNode(i).API.CreateFieldKeys(ctx, c.Idx(), "f", keys...)
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
			translations, err := c.GetPrimary().API.FindFieldKeys(ctx, c.Idx(), "f", keyList...)
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
					localTranslations, err := api.FindFieldKeys(ctx, c.Idx(), "f", keyList...)
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
					localTranslations, err := api.CreateFieldKeys(ctx, c.Idx(), "f", keyList...)
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
	i := c.Idx()

	c.CreateField(t, i, pilosa.IndexOptions{}, "f")

	t.Run("Index", func(t *testing.T) {
		t.Run("Create", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			_, err := c.GetNonPrimary().API.CreateIndexKeys(ctx, i, "foo")
			if err == nil {
				t.Fatal("unexpected success")
			}
			expect := fmt.Sprintf(`cannot create keys on unkeyed index "%s"`, i)
			if got := err.Error(); got != expect {
				t.Fatalf("expected error %q but got %q", expect, got)
			}
		})
		t.Run("Find", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			_, err := c.GetNonPrimary().API.FindIndexKeys(ctx, i, "foo")
			if err == nil {
				t.Fatal("unexpected success")
			}
			expect := fmt.Sprintf(`cannot find keys on unkeyed index "%s"`, i)
			if got := err.Error(); got != expect {
				t.Fatalf("expected error %q but got %q", expect, got)
			}
		})
	})
	t.Run("Field", func(t *testing.T) {
		t.Run("Create", func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			_, err := c.GetNonPrimary().API.CreateFieldKeys(ctx, i, "f", "foo")
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

			_, err := c.GetNonPrimary().API.FindFieldKeys(ctx, i, "f", "foo")
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
