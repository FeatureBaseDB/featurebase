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
