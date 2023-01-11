// Copyright 2021 Molecula Corp. All rights reserved.
package pilosa_test

import (
	"os"
	"strings"
	"testing"

	pilosa "github.com/molecula/featurebase/v3"
	"github.com/molecula/featurebase/v3/test"
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

		if _, err := h.CreateIndex("test", "", pilosa.IndexOptions{}); err != nil {
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

			if idx, err := h.CreateIndex("foo", "", pilosa.IndexOptions{}); err != nil {
				t.Fatal(err)
			} else {
				_, err := idx.CreateField("bar", "", pilosa.OptFieldTypeInt(0, 100), pilosa.OptFieldForeignIndex("nonexistent"))
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

			if _, err := h.CreateIndex("zzz", "", pilosa.IndexOptions{}); err != nil {
				t.Fatal(err)
			} else if idx, err := h.CreateIndex("foo", "", pilosa.IndexOptions{}); err != nil {
				t.Fatal(err)
			} else if _, err := idx.CreateField("bar", "", pilosa.OptFieldTypeInt(0, 100), pilosa.OptFieldForeignIndex("zzz")); err != nil {
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

			if _, err := h.CreateIndex("aaa", "", pilosa.IndexOptions{}); err != nil {
				t.Fatal(err)
			} else if idx, err := h.CreateIndex("foo", "", pilosa.IndexOptions{}); err != nil {
				t.Fatal(err)
			} else if _, err := idx.CreateField("bar", "", pilosa.OptFieldTypeInt(0, 100), pilosa.OptFieldForeignIndex("aaa")); err != nil {
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

			idx1, err := h.CreateIndexIfNotExists("aaa", "", pilosa.IndexOptions{})
			if err != nil {
				t.Fatal(err)
			}

			if _, err = h.CreateIndex("aaa", "", pilosa.IndexOptions{}); err == nil {
				t.Fatalf("expected: ConflictError, got: nil")
			} else if _, ok := err.(pilosa.ConflictError); !ok {
				t.Fatalf("expected: ConflictError, got: %s", err)
			}

			idx2, err := h.CreateIndexIfNotExists("aaa", "", pilosa.IndexOptions{})
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

		if _, err := h.CreateIndex("test", "", pilosa.IndexOptions{}); err != nil {
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
		h, err := pilosa.NewHolder("bad-path", pilosa.TestHolderConfig())
		if err != nil {
			t.Fatalf("surprisingly, got an error from NewHolder with an invalid path which we didn't expect: %v", err)
		}

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
