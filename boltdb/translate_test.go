// Copyright 2021 Molecula Corp. All rights reserved.
package boltdb_test

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"strconv"
	"testing"
	"time"

	pilosa "github.com/molecula/featurebase/v3"
	"github.com/molecula/featurebase/v3/boltdb"
	"github.com/molecula/featurebase/v3/roaring"
	"github.com/molecula/featurebase/v3/testhook"
	"github.com/molecula/featurebase/v3/topology"
)

//var vv = pilosa.VV

func TestTranslateStore_CreateKeys(t *testing.T) {
	s := MustOpenNewTranslateStore(t)
	defer MustCloseTranslateStore(s)

	ids, err := s.CreateKeys("abc", "abc")
	if err != nil {
		t.Fatal(err)
	} else if _, ok := ids["abc"]; !ok {
		t.Fatalf(`missing "abc"; got %v`, ids)
	} else if len(ids) > 1 {
		t.Fatalf("expected one key, got %d in %v", len(ids), ids)
	}

	// Ensure different keys translate to different IDs.
	ids1, err := s.CreateKeys("foo", "bar")
	if err != nil {
		t.Fatal(err)
	} else if foo, bar := ids1["foo"], ids1["bar"]; foo == bar {
		t.Fatalf(`"foo" and "bar" map back to the same ID %d`, foo)
	}

	// Ensure retranslation returns original IDs.
	if ids, err := s.CreateKeys("bar", "foo"); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(ids, ids1) {
		t.Fatalf("retranslation produced result %v which is different from original translation %v", ids, ids1)
	}

	// Ensure retranslating with existing and non-existing keys returns correctly.
	if ids, err := s.CreateKeys("foo", "baz", "bar"); err != nil {
		t.Fatal(err)
	} else if got, want := ids["foo"], ids1["foo"]; got != want {
		t.Fatalf(`mismatched ID %d for "foo" (previously %d)`, got, want)
	} else if _, ok := ids["baz"]; !ok {
		t.Fatalf(`missing translation for "baz"; got %v`, ids)
	} else if got, want := ids["bar"], ids1["bar"]; got != want {
		t.Fatalf(`mismatched ID %d for "bar" (previously %d)`, got, want)
	}
}

func TestTranslateStore_TranslateID(t *testing.T) {
	s := MustOpenNewTranslateStore(t)
	defer MustCloseTranslateStore(s)

	// Setup initial keys.
	ids, err := s.CreateKeys("foo", "bar", "")
	if err != nil {
		t.Fatal(err)
	}

	// Ensure IDs can be translated back to keys.
	for key, id := range ids {
		k, err := s.TranslateID(id)
		if err != nil {
			t.Fatal(err)
		}
		if k != key {
			t.Fatalf("TranslateID()=%s, want %s", k, key)
		}
	}
}

func TestTranslateStore_TranslateIDs(t *testing.T) {
	s := MustOpenNewTranslateStore(t)
	defer MustCloseTranslateStore(s)

	// Setup initial keys.
	ids, err := s.CreateKeys("foo", "bar")
	if err != nil {
		t.Fatal(err)
	}

	// Ensure IDs can be translated back to keys.
	if keys, err := s.TranslateIDs([]uint64{ids["foo"], ids["bar"], 1}); err != nil {
		t.Fatal(err)
	} else if got, want := keys[0], "foo"; got != want {
		t.Fatalf("TranslateIDs()[0]=%s, want %s", got, want)
	} else if got, want := keys[1], "bar"; got != want {
		t.Fatalf("TranslateIDs()[1]=%s, want %s", got, want)
	} else if got, want := keys[2], ""; got != want {
		t.Fatalf("TranslateIDs()[2]=%s, want %s", got, want)
	}
}

func TestTranslateStore_FindKeys(t *testing.T) {
	cases := []struct {
		name   string
		data   []string
		lookup []string
	}{
		{
			name:   "All",
			data:   []string{"plugh", "xyzzy", "h"},
			lookup: []string{"plugh", "xyzzy", "h"},
		},
		{
			name:   "Extra",
			data:   []string{"plugh", "xyzzy", "h"},
			lookup: []string{"plugh", "xyzzy", "h", "65"},
		},
		{
			name:   "None",
			data:   []string{"a", "b", "c"},
			lookup: []string{"d", "e"},
		},
		{
			name:   "Empty",
			lookup: []string{"h"},
		},
		{
			name: "LookupNothing",
		},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			s := MustOpenNewTranslateStore(t)
			defer MustCloseTranslateStore(s)

			var naiveMap map[string]uint64
			if c.data != nil {
				// Load in key data.
				keys := c.data
				ids, err := s.CreateKeys(keys...)
				if err != nil {
					t.Errorf("failed to import keys: %v", err)
					return
				}
				if len(ids) != len(keys) {
					t.Errorf("mapped %d keys to %d ids", len(keys), len(ids))
					return
				}
				naiveMap = ids
			}

			// Compute expected lookup result.
			result := map[string]uint64{}
			for _, key := range c.lookup {
				id, ok := naiveMap[key]
				if !ok {
					// The key is expected to be missing.
					continue
				}

				result[key] = id
			}

			// Find the keys.
			found, err := s.FindKeys(c.lookup...)
			if err != nil {
				t.Errorf("failed to find keys: %v", err)
			} else if !reflect.DeepEqual(result, found) {
				t.Errorf("expected %v but found %v", result, found)
			}
		})
	}
}

func TestTranslateStore_MaxID(t *testing.T) {
	s := MustOpenNewTranslateStore(t)
	defer MustCloseTranslateStore(s)

	// Generate a bunch of keys.
	var lastk uint64
	for i := 0; i < 1026; i++ {
		key := strconv.Itoa(i)
		ids, err := s.CreateKeys(key)
		if err != nil {
			t.Fatalf("translating %d: %v", i, err)
		}
		lastk = ids[key]
	}

	// Verify the max ID.
	max, err := s.MaxID()
	if err != nil {
		t.Fatalf("checking max ID: %v", err)
	}
	if max != lastk {
		t.Fatalf("last key is %d but max is %d", lastk, max)
	}
}

func TestTranslateStore_EntryReader(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		s := MustOpenNewTranslateStore(t)
		defer MustCloseTranslateStore(s)

		// Create multiple new keys.
		ids1, err := s.CreateKeys("foo", "bar")
		if err != nil {
			t.Fatal(err)
		}

		// Start reader from initial position.
		var entry pilosa.TranslateEntry
		r, err := s.EntryReader(context.Background(), 0)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()

		// Read first entry.
		if err := r.ReadEntry(&entry); err != nil {
			t.Fatal(err)
		} else if got, want := entry.ID, ids1["foo"]; got != want {
			t.Fatalf("ReadEntry() ID=%d, want %d", got, want)
		} else if got, want := entry.Key, "foo"; got != want {
			t.Fatalf("ReadEntry() Key=%s, want %s", got, want)
		}

		// Read next entry.
		if err := r.ReadEntry(&entry); err != nil {
			t.Fatal(err)
		} else if got, want := entry.ID, ids1["bar"]; got != want {
			t.Fatalf("ReadEntry() ID=%d, want %d", got, want)
		} else if got, want := entry.Key, "bar"; got != want {
			t.Fatalf("ReadEntry() Key=%s, want %s", got, want)
		}

		// Insert next key while reader is open.
		ids2, err := s.CreateKeys("baz")
		if err != nil {
			t.Fatal(err)
		}

		// Read newly created entry.
		if err := r.ReadEntry(&entry); err != nil {
			t.Fatal(err)
		} else if got, want := entry.ID, ids2["baz"]; got != want {
			t.Fatalf("ReadEntry() ID=%d, want %d", got, want)
		} else if got, want := entry.Key, "baz"; got != want {
			t.Fatalf("ReadEntry() Key=%s, want %s", got, want)
		}

		// Ensure reader closes cleanly.
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
	})

	// Ensure reader will read as soon as a new write comes in using WriteNotify().
	t.Run("WriteNotify", func(t *testing.T) {
		s := MustOpenNewTranslateStore(t)
		defer MustCloseTranslateStore(s)

		// Start reader from initial position.
		r, err := s.EntryReader(context.Background(), 0)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()

		// cache holds the translated key id so we can check it later
		cache := make(chan uint64)

		// Insert key in separate goroutine.
		// Sleep momentarily to reader hangs.
		translateErr := make(chan error)
		go func() {
			time.Sleep(100 * time.Millisecond)
			ids, err := s.CreateKeys("foo")
			if err != nil {
				translateErr <- err
			}
			cache <- ids["foo"]
		}()

		var entry pilosa.TranslateEntry
		if err := r.ReadEntry(&entry); err != nil {
			t.Fatal(err)
		} else if got, want := entry.ID, <-cache; got != want {
			t.Fatalf("ReadEntry() ID=%d, want %d", got, want)
		} else if got, want := entry.Key, "foo"; got != want {
			t.Fatalf("ReadEntry() Key=%s, want %s", got, want)
		}

		select {
		case err := <-translateErr:
			t.Fatalf("translate error: %s", err)
		default:
		}
	})

	// Ensure exits read on close.
	t.Run("Close", func(t *testing.T) {
		s := MustOpenNewTranslateStore(t)
		defer MustCloseTranslateStore(s)

		// Start reader from initial position.
		r, err := s.EntryReader(context.Background(), 0)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()

		// Insert key in separate goroutine.
		// Sleep momentarily to reader hangs.
		closeErr := make(chan error)
		go func() {
			time.Sleep(100 * time.Millisecond)
			if err := r.Close(); err != nil {
				closeErr <- err
			}
		}()

		var entry pilosa.TranslateEntry
		if err := r.ReadEntry(&entry); err != context.Canceled {
			t.Fatalf("unexpected error: %#v", err)
		}

		select {
		case err := <-closeErr:
			t.Fatalf("close error: %s", err)
		default:
		}
	})

	// Ensure exits read on store close.
	t.Run("StoreClose", func(t *testing.T) {
		s := MustOpenNewTranslateStore(t)
		defer MustCloseTranslateStore(s)

		// Start reader from initial position.
		r, err := s.EntryReader(context.Background(), 0)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()

		// Insert key in separate goroutine.
		// Sleep momentarily to reader hangs.
		closeErr := make(chan error)
		go func() {
			time.Sleep(100 * time.Millisecond)
			if err := s.Close(); err != nil {
				closeErr <- err
			}
		}()

		var entry pilosa.TranslateEntry
		if err := r.ReadEntry(&entry); err != boltdb.ErrTranslateStoreClosed {
			t.Fatalf("unexpected error: %#v", err)
		}

		select {
		case err := <-closeErr:
			t.Fatalf("close error: %s", err)
		default:
		}
	})
}

// MustNewTranslateStore returns a new TranslateStore with a temporary path.
func MustNewTranslateStore(tb testing.TB) *boltdb.TranslateStore {
	f, err := testhook.TempFile(tb, "translate-store")
	if err != nil {
		panic(err)
	} else if err := f.Close(); err != nil {
		panic(err)
	}

	s := boltdb.NewTranslateStore("I", "F", 0, topology.DefaultPartitionN, false)
	s.Path = f.Name()
	return s
}
func TestTranslateStore_Delete(t *testing.T) {
	s := MustOpenNewTranslateStore(t)
	defer MustCloseTranslateStore(s)

	// Setup initial keys.
	ids, err := s.CreateKeys("foo", "bar", "deleteme")
	if err != nil {
		t.Fatal(err)
	}

	records := roaring.NewBitmap(ids["deleteme"])
	c, err := s.Delete(records)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Commit(); err != nil {
		t.Fatal(err)
	}
	r, e := s.FreeIDs()
	if e != nil {
		t.Fatal(err)
	}
	freeids := r.Slice()
	if len(freeids) == 0 {
		t.Fatalf("expected to have free id")
	}
	if freeids[0] != ids["deleteme"] {
		t.Fatalf("expected [%v] and got %v", ids["deleteme"], freeids[0])
	}

	records2 := roaring.NewBitmap(ids["foo"])
	c, err = s.Delete(records2)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Commit(); err != nil {
		t.Fatal(err)
	}
	r, e = s.FreeIDs()
	if e != nil {
		t.Fatal(err)
	}
	freeids = r.Slice()
	if len(freeids) != 2 {
		t.Fatalf("expected to have 2 free ids")
	}
}
func TestTranslateStore_ReadWrite(t *testing.T) {
	t.Run("WriteTo_ReadFrom", func(t *testing.T) {
		s := MustOpenNewTranslateStore(t)
		defer MustCloseTranslateStore(s)

		batch0 := []string{}
		for i := 0; i < 100; i++ {
			batch0 = append(batch0, fmt.Sprintf("key%d", i))
		}
		batch1 := []string{}
		for i := 100; i < 200; i++ {
			batch1 = append(batch1, fmt.Sprintf("key%d", i))
		}

		// Populate the store with the keys in batch0.
		batch0IDs, err := s.CreateKeys(batch0...)
		if err != nil {
			t.Fatal(err)
		}

		// Put the contents of the store into a buffer.
		buf := bytes.NewBuffer(nil)
		expN := int64(32768)

		// After this, the buffer should contain batch0.
		if n, err := s.WriteTo(buf); err != nil {
			t.Fatalf("writing to buffer: %s", err)
		} else if n != expN {
			t.Fatalf("expected buffer size: %d, but got: %d", expN, n)
		}

		// Populate the store with the keys in batch1.
		batch1IDs, err := s.CreateKeys(batch1...)
		if err != nil {
			t.Fatal(err)
		}

		expIDs := map[string]uint64{
			"key50":  batch0IDs["key50"],
			"key150": batch1IDs["key150"],
		}

		// Check the IDs for a key from each batch.
		if ids, err := s.FindKeys("key50", "key150"); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(expIDs, ids) {
			t.Fatalf("first expected ids: %v, but got: %v", expIDs, ids)
		}

		// Reset the contents of the store with the data in the buffer.
		if n, err := s.ReadFrom(buf); err != nil {
			t.Fatalf("reading from buffer: %s", err)
		} else if n != expN {
			t.Fatalf("expected buffer size: %d, but got: %d", expN, n)
		}

		// This time, we expect the second key to be different because
		// we overwrote the store, and then just set that key.
		if ids, err := s.CreateKeys("key50", "key150"); err != nil {
			t.Fatal(err)
		} else if ids["key50"] != expIDs["key50"] {
			t.Fatalf("last expected ids[key50]: %d, but got: %d", expIDs["key50"], ids["key50"])
		} else if ids["key150"] == expIDs["key150"] {
			t.Fatalf("last expected different ids[key150]: %d, but got: %d", expIDs["key150"], ids["key150"])
		}
	})
}

// MustOpenNewTranslateStore returns a new, opened TranslateStore.
func MustOpenNewTranslateStore(tb testing.TB) *boltdb.TranslateStore {
	s := MustNewTranslateStore(tb)
	if err := s.Open(); err != nil {
		panic(err)
	}
	return s
}

// MustCloseTranslateStore closes s and removes the underlying data file.
func MustCloseTranslateStore(s *boltdb.TranslateStore) {
	if err := s.Close(); err != nil {
		panic(err)
	}
}
