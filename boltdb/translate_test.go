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
	"testing"
	"time"

	"github.com/pilosa/pilosa/v2"
	"github.com/pilosa/pilosa/v2/boltdb"
)

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

	s := boltdb.NewTranslateStore("I", "F", 0, pilosa.DefaultPartitionN)
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
