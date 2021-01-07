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
package boltdb_test

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/pilosa/pilosa/v2"
	"github.com/pilosa/pilosa/v2/boltdb"
	"github.com/pilosa/pilosa/v2/topology"
)

//var vv = pilosa.VV

func TestTranslateStore_TranslateKey(t *testing.T) {
	s := MustOpenNewTranslateStore()
	defer MustCloseTranslateStore(s)

	// Ensure initial key translates to first ID for shard
	id1, err := s.TranslateKey("foo", true)
	if err != nil {
		t.Fatal(err)
	}

	// Ensure next key autoincrements.
	if id, err := s.TranslateKey("bar", true); err != nil {
		t.Fatal(err)
	} else if got, want := id, id1+1; got != want {
		t.Fatalf("TranslateKey()=%d, want %d", got, want)
	}

	// Ensure retranslating existing key returns original ID.
	if id, err := s.TranslateKey("foo", true); err != nil {
		t.Fatal(err)
	} else if got, want := id, id1; got != want {
		t.Fatalf("TranslateKey()=%d, want %d", got, want)
	}
}

func TestTranslateStore_TranslateKeys(t *testing.T) {
	s := MustOpenNewTranslateStore()
	defer MustCloseTranslateStore(s)

	ids, err := s.TranslateKeys([]string{"abc", "abc"}, true)
	if err != nil {
		t.Fatal(err)
	} else if got, want := ids[1], ids[0]; got != want {
		t.Fatalf("TranslateKeys()[1]=%d, want %d", got, want)
	}

	// Ensure initial keys translate to incrementing IDs.
	ids1, err := s.TranslateKeys([]string{"foo", "bar"}, true)
	if err != nil {
		t.Fatal(err)
	} else if got, want := ids1[1], ids1[0]+1; got != want {
		t.Fatalf("TranslateKeys()[1]=%d, want %d", got, want)
	}

	// Ensure retranslation returns original IDs.
	if ids, err := s.TranslateKeys([]string{"foo", "bar"}, true); err != nil {
		t.Fatal(err)
	} else if got, want := ids[0], ids1[0]; got != want {
		t.Fatalf("TranslateKeys()[0]=%d, want %d", got, want)
	} else if got, want := ids[1], ids1[1]; got != want {
		t.Fatalf("TranslateKeys()[1]=%d, want %d", got, want)
	}

	// Ensure retranslating with existing and non-existing keys returns correctly.
	if ids, err := s.TranslateKeys([]string{"foo", "baz", "bar"}, true); err != nil {
		t.Fatal(err)
	} else if got, want := ids[0], ids1[0]; got != want {
		t.Fatalf("TranslateKeys()[0]=%d, want %d", got, want)
	} else if got, want := ids[1], ids1[0]+2; got != want {
		t.Fatalf("TranslateKeys()[1]=%d, want %d", got, want)
	} else if got, want := ids[2], ids1[1]; got != want {
		t.Fatalf("TranslateKeys()[2]=%d, want %d", got, want)
	}
}

func TestTranslateStore_CreateKeys(t *testing.T) {
	s := MustOpenNewTranslateStore()
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

func TestTranslateStore_ReadKey(t *testing.T) {
	s := MustOpenNewTranslateStore()
	defer MustCloseTranslateStore(s)

	id, err := s.TranslateKey("foo", false)
	if err != pilosa.ErrTranslatingKeyNotFound {
		t.Fatal(err)
	}
	if id != 0 {
		t.Fatalf("TranslateKey()=%d, want %d", id, 0)
	}

	s.SetReadOnly(true)
	id, err = s.TranslateKey("foo", true)
	if err == nil {
		t.Fatalf("got error: %+v, want: 'translate store read only'", err)
	}
	if id != 0 {
		t.Fatalf("TranslateKey()=%d, want %d", id, 0)
	}
	s.SetReadOnly(false)

	// Ensure next key autoincrements.
	if id, err = s.TranslateKey("foo", true); err != nil {
		t.Fatal(err)
	}
	id1, err := s.TranslateKey("foo", false)
	if err != nil {
		t.Fatal(err)
	}
	if id1 != id {
		t.Fatalf("TranslateKey()=%d, want %d", id1, id)
	}
}

func TestTranslateStore_ReadKeys(t *testing.T) {
	s := MustOpenNewTranslateStore()
	defer MustCloseTranslateStore(s)

	ids, err := s.TranslateKeys([]string{"foo", "bar", "baz", "baz", "bar", "foo"}, false)
	if err != pilosa.ErrTranslatingKeyNotFound {
		t.Fatal(err)
	}
	for _, id := range ids {
		if id != 0 {
			t.Fatalf("TranslateKeys()=%d, want %d", id, 0)
		}
	}

	// Ensure next key autoincrements.
	if ids, err = s.TranslateKeys([]string{"foo", "bar", "baz", "baz", "bar", "foo"}, true); err != nil {
		t.Fatal(err)
	}
	ids1, err := s.TranslateKeys([]string{"foo", "bar", "baz", "baz", "bar", "foo"}, false)
	if err != nil {
		t.Fatal(err)
	}
	for i := range ids1 {
		if ids1[i] != ids[i] {
			t.Fatalf("TranslateKeys()=%d, want %d", ids1[i], ids[i])
		}
	}
}
func TestTranslateStore_TranslateID(t *testing.T) {
	s := MustOpenNewTranslateStore()
	defer MustCloseTranslateStore(s)

	// Setup initial keys.
	id1, err := s.TranslateKey("foo", true)
	if err != nil {
		t.Fatal(err)
	}
	id2, err := s.TranslateKey("bar", true)
	if err != nil {
		t.Fatal(err)
	}
	id3, err := s.TranslateKey("", true)
	if err != nil {
		t.Fatal(err)
	}

	// Ensure IDs can be translated back to keys.
	if key, err := s.TranslateID(id1); err != nil {
		t.Fatal(err)
	} else if got, want := key, "foo"; got != want {
		t.Fatalf("TranslateID()=%s, want %s", got, want)
	}

	if key, err := s.TranslateID(id2); err != nil {
		t.Fatal(err)
	} else if got, want := key, "bar"; got != want {
		t.Fatalf("TranslateID()=%s, want %s", got, want)
	}

	if key, err := s.TranslateID(id3); err != nil {
		t.Fatal(err)
	} else if got, want := key, ""; got != want {
		t.Fatalf("TranslateID()=%s, want %s", got, want)
	}

}

func TestTranslateStore_TranslateIDs(t *testing.T) {
	s := MustOpenNewTranslateStore()
	defer MustCloseTranslateStore(s)

	// Setup initial keys.
	ids, err := s.TranslateKeys([]string{"foo", "bar"}, true)
	if err != nil {
		t.Fatal(err)
	}

	// Ensure IDs can be translated back to keys.
	if keys, err := s.TranslateIDs([]uint64{ids[0], ids[1], 1}); err != nil {
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
			s := MustOpenNewTranslateStore()
			defer MustCloseTranslateStore(s)

			var naiveMap map[string]uint64
			if c.data != nil {
				// Load in key data.
				keys := c.data
				ids, err := s.TranslateKeys(keys, true)
				if err != nil {
					t.Errorf("failed to import keys: %v", err)
					return
				}
				if len(ids) != len(keys) {
					t.Errorf("mapped %d keys to %d ids", len(keys), len(ids))
					return
				}
				naiveMap = make(map[string]uint64, len(keys))
				for i, key := range keys {
					naiveMap[key] = ids[i]
				}
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
	s := MustOpenNewTranslateStore()
	defer MustCloseTranslateStore(s)

	// Generate a bunch of keys.
	var lastk uint64
	for i := 0; i < 1026; i++ {
		k, err := s.TranslateKey(strconv.Itoa(i), true)
		if err != nil {
			t.Fatalf("translating %d: %v", i, err)
		}
		lastk = k
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
		s := MustOpenNewTranslateStore()
		defer MustCloseTranslateStore(s)

		// Create multiple new keys.
		ids1, err := s.TranslateKeys([]string{"foo", "bar"}, true)
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
		} else if got, want := entry.ID, ids1[0]; got != want {
			t.Fatalf("ReadEntry() ID=%d, want %d", got, want)
		} else if got, want := entry.Key, "foo"; got != want {
			t.Fatalf("ReadEntry() Key=%s, want %s", got, want)
		}

		// Read next entry.
		if err := r.ReadEntry(&entry); err != nil {
			t.Fatal(err)
		} else if got, want := entry.ID, ids1[1]; got != want {
			t.Fatalf("ReadEntry() ID=%d, want %d", got, want)
		} else if got, want := entry.Key, "bar"; got != want {
			t.Fatalf("ReadEntry() Key=%s, want %s", got, want)
		}

		// Insert next key while reader is open.
		id2, err := s.TranslateKey("baz", true)
		if err != nil {
			t.Fatal(err)
		}

		// Read newly created entry.
		if err := r.ReadEntry(&entry); err != nil {
			t.Fatal(err)
		} else if got, want := entry.ID, id2; got != want {
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
		s := MustOpenNewTranslateStore()
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
			id, err := s.TranslateKey("foo", true)
			if err != nil {
				translateErr <- err
			}
			cache <- id
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
		s := MustOpenNewTranslateStore()
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
		s := MustOpenNewTranslateStore()
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
func MustNewTranslateStore() *boltdb.TranslateStore {
	f, err := ioutil.TempFile("", "")
	if err != nil {
		panic(err)
	} else if err := f.Close(); err != nil {
		panic(err)
	}

	s := boltdb.NewTranslateStore("I", "F", 0, topology.DefaultPartitionN)
	s.Path = f.Name()
	return s
}

func TestTranslateStore_ReadWrite(t *testing.T) {
	t.Run("WriteTo_ReadFrom", func(t *testing.T) {
		s := MustOpenNewTranslateStore()
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
		batch0IDs, err := s.TranslateKeys(batch0, true)
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
		batch1IDs, err := s.TranslateKeys(batch1, true)
		if err != nil {
			t.Fatal(err)
		}

		expIDs := []uint64{batch0IDs[50], batch1IDs[50]}

		// Check the IDs for a key from each batch.
		if ids, err := s.TranslateKeys([]string{"key50", "key150"}, false); err != nil {
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
		if ids, err := s.TranslateKeys([]string{"key50", "key150"}, true); err != nil {
			t.Fatal(err)
		} else if ids[0] != expIDs[0] {
			t.Fatalf("last expected ids[0]: %d, but got: %d", expIDs[0], ids[0])
		} else if ids[1] == expIDs[1] {
			t.Fatalf("last expected different ids[1]: %d, but got: %d", expIDs[1], ids[1])
		}
	})
}

// MustOpenNewTranslateStore returns a new, opened TranslateStore.
func MustOpenNewTranslateStore() *boltdb.TranslateStore {
	s := MustNewTranslateStore()
	if err := s.Open(); err != nil {
		panic(err)
	}
	return s
}

// MustCloseTranslateStore closes s and removes the underlying data file.
func MustCloseTranslateStore(s *boltdb.TranslateStore) {
	if err := s.Close(); err != nil {
		panic(err)
	} else if err := os.Remove(s.Path); err != nil {
		panic(err)
	}
}

func TestCryptoHashPerKey(t *testing.T) {
	s := MustOpenNewTranslateStore()
	defer MustCloseTranslateStore(s)

	// hash one translation

	expect := map[int]string{
		1: string([]byte{0x76, 0x48, 0x8b, 0x70, 0xe8, 0x54, 0x35, 0xc6, 0x8e, 0xa6, 0x4, 0x6c, 0xfa, 0xd2, 0x1a, 0x12}),
		2: string([]byte{0x81, 0x46, 0x84, 0x37, 0x26, 0x96, 0x41, 0xf3, 0x54, 0x4e, 0x98, 0xbc, 0x48, 0xab, 0x1b, 0xf0}),
		3: string([]byte{0x7f, 0xe9, 0xf, 0x6d, 0x7b, 0x14, 0x1, 0x44, 0xb2, 0x4e, 0xd0, 0x86, 0x2f, 0x62, 0x8c, 0xa9}),
	}
	for n := 1; n < 4; n++ {
		var batch0 []string
		for i := 0; i < n; i++ {
			batch0 = append(batch0, fmt.Sprintf("key%d", i))
		}

		// Populate the store with the keys in batch0.
		batch0IDs, err := s.TranslateKeys(batch0, true)
		_ = batch0IDs
		if err != nil {
			t.Fatal(err)
		}

		// done with setup
		sum, err := s.ComputeTranslatorSummaryCols(0, pilosa.NewTopology(&pilosa.Jmphasher{}, topology.DefaultPartitionN, 1, nil))
		if err != nil {
			panic(err)
		}
		nkey := sum.KeyCount
		nid := sum.IDCount
		observedChecksum := sum.Checksum
		if nkey != n {
			panic("wrong key count")
		}
		if nkey != nid {
			panic("key count should match id count")
		}

		// shardwidth 22 has different hashes, of course.
		if pilosa.ShardWidth == 20 {
			expectedChecksum := expect[n]
			if observedChecksum != expectedChecksum {
				panic(fmt.Sprintf("got wrong checksum obs '%#v' vs expected '%#v'", observedChecksum, expectedChecksum))
			}
		}
	}

}

func TestTranslateStore_RepairNonInvertibleStringKeyTranslation(t *testing.T) {

	const N = 6
	// before repair
	var fwd [N]map[string]uint64
	var rev [N]map[uint64]string

	// after repair
	var fwd2 [N]map[string]uint64
	var rev2 [N]map[uint64]string

	// case 0: forward is messed up (unlikely but check for it anyway, be sure we can repair)
	// "key0" -> id 0  // correct.
	// "key1" -> id 0  // wrong. after Repair, should see key1 -> 1 (0xec0002)
	//
	// id 0 -> "key0" // correct
	// id 1 -> "key1" // correct
	//
	fwd[0] = map[string]uint64{"key0": 0xec00001, "key1": 0xec00001}
	rev[0] = map[uint64]string{0xec00001: "key0", 0xec00002: "key1"}
	fwd2[0] = map[string]uint64{"key0": 0xec00001, "key1": 0xec00002}
	rev2[0] = map[uint64]string{0xec00001: "key0", 0xec00002: "key1"}

	// case 1: reverse is messed up (we have seen this in the past)
	// "key0" -> id 0  // correct
	// "key1" -> id 1  // correct
	//
	// id 0 -> "key0" // correct.
	// id 1 -> "key0" // wrong. after Repair, should see id 1 -> "key1"
	//
	fwd[1] = map[string]uint64{"key0": 0xec00001, "key1": 0xec00002}
	rev[1] = map[uint64]string{0xec00001: "key0", 0xec00002: "key0"}
	fwd2[1] = map[string]uint64{"key0": 0xec00001, "key1": 0xec00002}
	rev2[1] = map[uint64]string{0xec00001: "key0", 0xec00002: "key1"}

	// case 2: only present in reverse.
	fwd[2] = map[string]uint64{}
	rev[2] = map[uint64]string{0xec00001: "key0"}
	fwd2[2] = map[string]uint64{"key0": 0xec00001}
	rev2[2] = map[uint64]string{0xec00001: "key0"}

	// case 3: same thing. with camoflage.
	fwd[3] = map[string]uint64{"key1": 0xec00002}
	rev[3] = map[uint64]string{0xec00001: "key0", 0xec00002: "key1"}
	fwd2[3] = map[string]uint64{"key0": 0xec00001, "key1": 0xec00002}
	rev2[3] = map[uint64]string{0xec00001: "key0", 0xec00002: "key1"}

	// case 4: only present in forward.

	fwd[4] = map[string]uint64{"key0": 0xec00001}
	rev[4] = map[uint64]string{}
	fwd2[4] = map[string]uint64{"key0": 0xec00001}
	rev2[4] = map[uint64]string{0xec00001: "key0"}

	// case 5: same thing. with camoflage.
	fwd[5] = map[string]uint64{"key0": 0xec00001}
	rev[5] = map[uint64]string{0xec00002: "key1"}
	fwd2[5] = map[string]uint64{"key0": 0xec00001, "key1": 0xec00002}
	rev2[5] = map[uint64]string{0xec00001: "key0", 0xec00002: "key1"}

	// case 6: we had an id, but b/c of the fix, that id is no longer used.
	//         now that id might still be used in the fragment for a column,
	//         and so we will need to remove that id/column from the fragment.
	// encapsulated: "did it affect the state of the fields?"

	for i := 0; i < 5; i++ {
		//println("i = ", i)
		s := MustOpenNewTranslateStore()
		defer MustCloseTranslateStore(s)

		if err := s.SetFwdRevMaps(nil, fwd[i], rev[i]); err != nil {
			t.Fatal(err)
		}

		if err := verifyState("setup", i, s, fwd[i], rev[i]); err != nil {
			t.Fatal(err)
		}

		var topo *pilosa.Topology
		verbose := false
		applyKeyRepairs := true
		changed, err := s.RepairKeys(topo, verbose, applyKeyRepairs)
		if err != nil {
			t.Fatal(err)
		}
		if !changed {
			t.Fatalf("expected changes!")
		}

		if err := verifyState("afterRepair", i, s, fwd2[i], rev2[i]); err != nil {
			t.Fatal(err)
		}
	}
}

func verifyState(label string, i int, s *boltdb.TranslateStore, fwd map[string]uint64, rev map[uint64]string) error {

	// verify the setup
	const writable = true
	for key, expectID := range fwd {
		id, err := s.TranslateKey(key, !writable)
		if err != nil {
			return err
		}
		if id != expectID {
			return fmt.Errorf("fwd %v problem. i=%v, for key '%v', expected %x, observed %x", label, i, key, expectID, id)
		}
	}
	for id, expectKey := range rev {
		key, err := s.TranslateID(id)
		if err != nil {
			return err
		}
		if key != expectKey {
			return fmt.Errorf("rev %v problem. i=%v, for id '%x', expected %v, observed %v", label, i, id, expectKey, key)
		}
	}
	return nil
}
