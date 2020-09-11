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

package rbf_test

import (
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/pilosa/pilosa/v2/rbf"
)

func TestDB_Open(t *testing.T) {
	db := NewDB()
	if err := db.Open(); err != nil {
		t.Fatal(err)
	} else if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDB_Checkpoint(t *testing.T) {
	if testing.Short() {
		t.Skip("-short enabled, skipping")
	}

	db := MustOpenDB(t)
	defer MustCloseDB(t, db)

	// Create bitmap.
	if tx, err := db.Begin(true); err != nil {
		t.Fatal(err)
	} else if err := tx.CreateBitmap("x"); err != nil {
		t.Fatal(err)
	} else if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Create a bunch of transactions to generate WAL segments.
	rand := rand.New(rand.NewSource(0))
	for i := 0; i < 1000; i++ {
		if tx, err := db.Begin(true); err != nil {
			t.Fatal(err)
		} else if _, err := tx.Add("x", rand.Uint64()); err != nil {
			t.Fatal(err)
		} else if _, err := tx.Add("x", rand.Uint64()); err != nil {
			t.Fatal(err)
		} else if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}

	// Ensure there is no more than two WAL segments.
	if n := len(db.WALSegments()); n > 2 {
		t.Fatalf("expected two or fewer WAL segments, got %d", n)
	}
}

func TestDB_Recovery(t *testing.T) {
	// Ensure a bitmap header written without a bitmap is truncated.
	t.Run("TruncPartialWALBitmap", func(t *testing.T) {
		db := MustOpenDB(t)
		defer MustCloseDB(t, db)

		a := make([]uint64, rbf.ArrayMaxSize+100)
		for i := range a {
			a[i] = uint64(i)
		}

		// Create bitmap & generate enough values to create a bitmap container.
		if tx, err := db.Begin(true); err != nil {
			t.Fatal(err)
		} else if err := tx.CreateBitmap("x"); err != nil {
			t.Fatal(err)
		} else if _, err := tx.Add("x", a...); err != nil {
			t.Fatal(err)
		} else if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}

		// Add one additional bit in a second transaction.
		if tx, err := db.Begin(true); err != nil {
			t.Fatal(err)
		} else if _, err := tx.Add("x", uint64(len(a))); err != nil {
			t.Fatal(err)
		} else if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}

		// Close database & truncate WAL to remove commit page & bitmap data page.
		segment := db.ActiveWALSegment()
		if err := db.Close(); err != nil {
			t.Fatal(err)
		} else if err := os.Truncate(segment.Path(), segment.Size()-(2*rbf.PageSize)); err != nil {
			t.Fatal(err)
		}

		// Reopen database.
		newDB := rbf.NewDB(db.Path)
		if err := newDB.Open(); err != nil {
			t.Fatal(err)
		}
		defer MustCloseDB(t, newDB)

		// Verify last insert was not added.
		tx, err := newDB.Begin(true)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()

		if exists, err := tx.Contains("x", uint64(len(a))); exists || err != nil {
			t.Fatalf("Contains()=<%v,%#v>", exists, err)
		} else if exists, err := tx.Contains("x", uint64(len(a)-1)); !exists || err != nil {
			t.Fatalf("Contains()=<%v,%#v>", exists, err)
		}
		tx.Rollback()
	})
}

func TestDB_BeginWithExclusiveLock(t *testing.T) {
	t.Run("EnsureBlock", func(t *testing.T) {
		db := MustOpenDB(t)
		defer MustCloseDB(t, db)

		tx, err := db.BeginWithExclusiveLock()
		if err != nil {
			t.Fatal(err)
		} else if err := tx.CreateBitmap("x"); err != nil {
			t.Fatal(err)
		}

		// Attempt to start another transaction in a second goroutine.
		ch := make(chan struct{})
		go func() {
			tx1, err := db.Begin(false)
			if err != nil {
				panic(err)
			}
			defer tx1.Rollback()
			close(ch) // signal
		}()

		// Ensure other transctions are blocked during an exclusive lock.
		select {
		case <-ch:
			t.Fatal("secondary transaction too soon")
		case <-time.After(100 * time.Millisecond):
		}

		// Release exclusive lock.
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}

		// Ensure other transaction to begin after exclusive lock released.
		select {
		case <-time.After(1 * time.Second):
			t.Fatal("expected secondary transaction")
		case <-ch:
		}
	})

	t.Run("EnsureNoWAL", func(t *testing.T) {
		db := MustOpenDB(t)
		defer MustCloseDB(t, db)

		tx, err := db.BeginWithExclusiveLock()
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()

		if err := tx.CreateBitmap("x"); err != nil {
			t.Fatal(err)
		} else if got, want := db.WALSize(), int64(0); got != want {
			t.Fatalf("WALSize()=%d, want %d", got, want)
		}
	})
}

func TestDB_HasData(t *testing.T) {

	db := MustOpenDB(t)
	defer MustCloseDB(t, db)

	// HasData should start out false.
	const requireOneHotBit = true
	hasAnything, err := db.HasData(!requireOneHotBit)
	if err != nil {
		t.Fatal(err)
	}
	if hasAnything {
		t.Fatalf("HasData reported existing data on an empty database")
	}

	hasAnything, err = db.HasData(requireOneHotBit)
	if err != nil {
		t.Fatal(err)
	}
	if hasAnything {
		t.Fatalf("HasData reported existing data on an empty database")
	}

	// check that HasData sees a committed record.

	// Create bitmap with no hot bits.
	if tx, err := db.Begin(true); err != nil {
		t.Fatal(err)
	} else if err := tx.CreateBitmap("x"); err != nil {
		t.Fatal(err)
	} else if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// HasData(false) should now report seeing the 'x' record, even though
	// the value is an empty bitmap, since !requireOneHotBit.
	hasAnything, err = db.HasData(!requireOneHotBit)
	if err != nil {
		t.Fatal(err)
	}
	if !hasAnything {
		t.Fatalf("HasData(!requireOneHotBit) reported no data on a database that has 'x' written to it")
	}

	// HasData(requireOneHotBit) should report false, since we have no hot bits.
	hasAnything, err = db.HasData(requireOneHotBit)
	if err != nil {
		t.Fatal(err)
	}
	if hasAnything {
		t.Fatalf("HasData(requireOneHotBit) reported data on a database that has 'x' -> empty bitmap")
	}

	// hot up a bit.
	if tx, err := db.Begin(true); err != nil {
		t.Fatal(err)
	} else if _, err := tx.Add("x", rand.Uint64()); err != nil {
		t.Fatal(err)
	} else if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Now it shouldn't matter, we should get HasData true either way
	// HasData(requireOneHotBit) should report false, since we have no hot bits.
	hasAnything, err = db.HasData(requireOneHotBit)
	if err != nil {
		t.Fatal(err)
	}
	if !hasAnything {
		t.Fatalf("HasData should have seen the hot bit")
	}
	hasAnything, err = db.HasData(!requireOneHotBit)
	if err != nil {
		t.Fatal(err)
	}
	if !hasAnything {
		t.Fatalf("HasData should have seen the hot bit")
	}
}
