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
	"context"
	"errors"
	"io"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/pilosa/pilosa/v2"
	"github.com/pilosa/pilosa/v2/mock"
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
