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
	"context"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/pilosa/pilosa/v2"
	"github.com/pilosa/pilosa/v2/boltdb"
)

func TestTranslateStore_TranslateKey(t *testing.T) {
	s := MustOpenNewTranslateStore()
	defer MustCloseTranslateStore(s)

	// Ensure initial key translates to first ID for shard
	if id, err := s.TranslateKey("foo"); err != nil {
		t.Fatal(err)
	} else if got, want := id, uint64(247463937); got != want {
		t.Fatalf("TranslateKey()=%d, want %d", got, want)
	}

	// Ensure next key autoincrements.
	if id, err := s.TranslateKey("bar"); err != nil {
		t.Fatal(err)
	} else if got, want := id, uint64(247463938); got != want {
		t.Fatalf("TranslateKey()=%d, want %d", got, want)
	}

	// Ensure retranslating existing key returns original ID.
	if id, err := s.TranslateKey("foo"); err != nil {
		t.Fatal(err)
	} else if got, want := id, uint64(247463937); got != want {
		t.Fatalf("TranslateKey()=%d, want %d", got, want)
	}
}

func TestTranslateStore_TranslateKeys(t *testing.T) {
	s := MustOpenNewTranslateStore()
	defer MustCloseTranslateStore(s)

	// Ensure initial keys translate to incrementing IDs.
	if ids, err := s.TranslateKeys([]string{"foo", "bar"}); err != nil {
		t.Fatal(err)
	} else if got, want := ids[0], uint64(247463937); got != want {
		t.Fatalf("TranslateKeys()[0]=%d, want %d", got, want)
	} else if got, want := ids[1], uint64(247463938); got != want {
		t.Fatalf("TranslateKeys()[1]=%d, want %d", got, want)
	}

	// Ensure retranslation returns original IDs.
	if ids, err := s.TranslateKeys([]string{"foo", "bar"}); err != nil {
		t.Fatal(err)
	} else if got, want := ids[0], uint64(247463937); got != want {
		t.Fatalf("TranslateKeys()[0]=%d, want %d", got, want)
	} else if got, want := ids[1], uint64(247463938); got != want {
		t.Fatalf("TranslateKeys()[1]=%d, want %d", got, want)
	}

	// Ensure retranslating with existing and non-existing keys returns correctly.
	if ids, err := s.TranslateKeys([]string{"foo", "baz", "bar"}); err != nil {
		t.Fatal(err)
	} else if got, want := ids[0], uint64(247463937); got != want {
		t.Fatalf("TranslateKeys()[0]=%d, want %d", got, want)
	} else if got, want := ids[1], uint64(247463939); got != want {
		t.Fatalf("TranslateKeys()[1]=%d, want %d", got, want)
	} else if got, want := ids[2], uint64(247463938); got != want {
		t.Fatalf("TranslateKeys()[2]=%d, want %d", got, want)
	}
}

func TestTranslateStore_TranslateID(t *testing.T) {
	s := MustOpenNewTranslateStore()
	defer MustCloseTranslateStore(s)

	// Setup initial keys.
	id1, err := s.TranslateKey("foo")
	if err != nil {
		t.Fatal(err)
	}
	id2, err := s.TranslateKey("bar")
	if err != nil {
		t.Fatal(err)
	}
	id3, err := s.TranslateKey("")
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
	ids, err := s.TranslateKeys([]string{"foo", "bar"})
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
		if _, err := s.TranslateKeys([]string{"foo", "bar"}); err != nil {
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
		} else if got, want := entry.ID, uint64(247463937); got != want {
			t.Fatalf("ReadEntry() ID=%d, want %d", got, want)
		} else if got, want := entry.Key, "foo"; got != want {
			t.Fatalf("ReadEntry() Key=%s, want %s", got, want)
		}

		// Read next entry.
		if err := r.ReadEntry(&entry); err != nil {
			t.Fatal(err)
		} else if got, want := entry.ID, uint64(247463938); got != want {
			t.Fatalf("ReadEntry() ID=%d, want %d", got, want)
		} else if got, want := entry.Key, "bar"; got != want {
			t.Fatalf("ReadEntry() Key=%s, want %s", got, want)
		}

		// Insert next key while reader is open.
		if _, err := s.TranslateKey("baz"); err != nil {
			t.Fatal(err)
		}

		// Read newly created entry.
		if err := r.ReadEntry(&entry); err != nil {
			t.Fatal(err)
		} else if got, want := entry.ID, uint64(247463939); got != want {
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

		// Insert key in separate goroutine.
		// Sleep momentarily to reader hangs.
		translateErr := make(chan error)
		go func() {
			time.Sleep(100 * time.Millisecond)
			if _, err := s.TranslateKey("foo"); err != nil {
				translateErr <- err
			}
		}()

		var entry pilosa.TranslateEntry
		if err := r.ReadEntry(&entry); err != nil {
			t.Fatal(err)
		} else if got, want := entry.ID, uint64(247463937); got != want {
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
