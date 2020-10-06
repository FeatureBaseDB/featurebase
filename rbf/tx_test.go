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
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/pilosa/pilosa/v2/race"
	"github.com/pilosa/pilosa/v2/rbf"
	"github.com/pilosa/pilosa/v2/txkey"
)

func TestTx_CommitRollback(t *testing.T) {
	t.Run("NoReopen", func(t *testing.T) {
		db := MustOpenDB(t)
		defer MustCloseDB(t, db)

		// Create bitmap in transaction but rollback.
		tx, err := db.Begin(true)
		if err != nil {
			t.Fatal(err)
		} else if err := tx.CreateBitmap("x"); err != nil {
			t.Fatal(err)
		}
		tx.Rollback()

		// Create bitmap in transaction again but commit.
		if tx, err := db.Begin(true); err != nil {
			t.Fatal(err)
		} else if err := tx.CreateBitmap("x"); err != nil {
			t.Fatal(err)
		} else if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}

		// Create bitmap again but it should fail as it already exists.
		if tx, err := db.Begin(true); err != nil {
			t.Fatal(err)
		} else if err := tx.CreateBitmap("x"); err == nil || err != rbf.ErrBitmapExists {
			tx.Rollback()
			t.Fatal(err)
		} else if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("Reopen", func(t *testing.T) {
		db := MustOpenDB(t)
		defer func() { MustCloseDB(t, db) }()

		// Create bitmap in transaction but rollback.
		tx, err := db.Begin(true)
		if err != nil {
			t.Fatal(err)
		} else if err := tx.CreateBitmap("x"); err != nil {
			t.Fatal(err)
		}
		tx.Rollback()
		db = MustReopenDB(t, db)

		// Create bitmap in transaction again but commit.
		if tx, err := db.Begin(true); err != nil {
			t.Fatal(err)
		} else if err := tx.CreateBitmap("x"); err != nil {
			t.Fatal(err)
		} else if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		db = MustReopenDB(t, db)

		// Create bitmap again but it should fail as it already exists.
		if tx, err := db.Begin(true); err != nil {
			t.Fatal(err)
		} else if err := tx.CreateBitmap("x"); err == nil || err != rbf.ErrBitmapExists {
			tx.Rollback()
			t.Fatal(err)
		} else if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("SingleWriter", func(t *testing.T) {
		//t.Skip("NEED TO FIX IN RACE") //TODO (twg)
		db := MustOpenDB(t)
		defer MustCloseDB(t, db)

		// Start write transaction.
		ch0 := make(chan struct{})
		tx0 := MustBegin(t, db, true)
		go func() {
			<-ch0
			tx0.Rollback()
		}()

		// Start separate write transaction in different goroutine.
		ch1 := make(chan struct{})
		go func() {
			tx1 := MustBegin(t, db, true)
			close(ch1)
			_ = tx1.Commit()
		}()

		// Ensure second tx doesn't start.
		select {
		case <-ch1:
			t.Fatal("second tx started while first tx active")
		case <-time.After(10 * time.Millisecond):
		}

		// Finish first transaction.
		close(ch0)
		select {
		case <-ch1:
		case <-time.After(10 * time.Millisecond):
			t.Fatal("second tx should have started after first tx closed")
		}
	})
}

func TestTx_Add(t *testing.T) {
	db := MustOpenDB(t)
	defer MustCloseDB(t, db)
	tx := MustBegin(t, db, true)
	defer tx.Rollback()

	if err := tx.CreateBitmap("x"); err != nil {
		t.Fatal(err)
	}

	if _, err := tx.Add("x", 1); err != nil {
		t.Fatal(err)
	} else if _, err := tx.Add("x", 10); err != nil {
		t.Fatal(err)
	} else if _, err := tx.Add("x", 3); err != nil {
		t.Fatal(err)
	}

	for _, v := range []uint64{1, 3, 10} {
		if ok, err := tx.Contains("x", v); err != nil {
			t.Fatal(err)
		} else if !ok {
			t.Fatalf("Tx.Contains(%d): expected true", v)
		}
	}

	if ok, err := tx.Contains("x", 2); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatal("Tx.Contains(): expected false")
	}
}

func TestTx_DeleteBitmap(t *testing.T) {
	db := MustOpenDB(t)
	defer MustCloseDB(t, db)
	tx := MustBegin(t, db, true)
	defer tx.Rollback()

	// Create bitmap & add value.
	if err := tx.CreateBitmap("x"); err != nil {
		t.Fatal(err)
	} else if _, err := tx.Add("x", 1); err != nil {
		t.Fatal(err)
	}

	// Recreate bitmap & ensure value does not exist.
	if err := tx.DeleteBitmap("x"); err != nil {
		t.Fatal(err)
	} else if err := tx.CreateBitmap("x"); err != nil {
		t.Fatal(err)
	} else if ok, err := tx.Contains("x", 1); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatal("expected no value in recreated bitmap")
	}
}

func TestTx_RenameBitmap(t *testing.T) {
	db := MustOpenDB(t)
	defer MustCloseDB(t, db)
	tx := MustBegin(t, db, true)
	defer tx.Rollback()

	// Create bitmap & add value.
	if err := tx.CreateBitmap("x"); err != nil {
		t.Fatal(err)
	} else if _, err := tx.Add("x", 1); err != nil {
		t.Fatal(err)
	}

	// Rename bitmap & ensure value still exists.
	if err := tx.RenameBitmap("x", "y"); err != nil {
		t.Fatal(err)
	} else if ok, err := tx.Contains("y", 1); err != nil {
		t.Fatal(err)
	} else if !ok {
		t.Fatal("expected value in renamed bitmap")
	}
}

func TestTx_Add_Quick(t *testing.T) {
	if testing.Short() {
		t.Skip("-short enabled, skipping")
	} else if is32Bit() {
		t.Skip("32-bit build, skipping quick check tests")
	} else if race.Enabled {
		t.Skip("race detection enabled, skipping")
	}

	QuickCheck(t, func(t *testing.T, rand *rand.Rand) {
		t.Parallel()

		db := MustOpenDB(t)
		defer MustCloseDB(t, db)
		tx := MustBegin(t, db, true)
		defer tx.Rollback()
		values := GenerateValues(rand, 10000)

		if err := tx.CreateBitmap("x"); err != nil {
			t.Fatal(err)
		}

		// Insert values in random order.
		for _, i := range rand.Perm(len(values)) {
			v := values[i]
			if _, err := tx.Add("x", v); err != nil {
				t.Fatalf("Add(%d) i=%d err=%q", v, i, err)
			}
		}

		// Verify all bits are written.
		for i, v := range values {
			if ok, err := tx.Contains("x", v); !ok || err != nil {
				t.Fatalf("Contains(%d)=(%v,%v) i=%d hi=%d lo=%d", v, ok, err, i, highbits(v), lowbits(v))
			}
		}
	})
}

func TestTx_AddRemove_Quick(t *testing.T) {
	if testing.Short() {
		t.Skip("-short enabled, skipping")
	} else if is32Bit() {
		t.Skip("32-bit build, skipping quick check tests")
	} else if race.Enabled {
		t.Skip("race detection enabled, skipping")
	}

	QuickCheck(t, func(t *testing.T, rand *rand.Rand) {
		t.Parallel()

		db := MustOpenDB(t)
		defer MustCloseDB(t, db)
		tx := MustBegin(t, db, true)
		defer tx.Rollback()
		values := GenerateValues(rand, 10000)

		if err := tx.CreateBitmap("x"); err != nil {
			t.Fatal(err)
		}

		// Insert values in random order.
		for _, i := range rand.Perm(len(values)) {
			if _, err := tx.Add("x", values[i]); err != nil {
				t.Fatalf("Add(%d) i=%d err=%q", values[i], i, err)
			}
		}

		// Remove half the values in random order.
		for _, i := range rand.Perm(len(values)) {
			if _, err := tx.Remove("x", values[i]); err != nil {
				t.Fatalf("Remove(%d) i=%d err=%q", values[i], i, err)
			}
		}

		// Verify all bits are removed.
		for i, v := range values {
			if ok, err := tx.Contains("x", v); ok || err != nil {
				t.Fatalf("Contains(%d)=(%v,%v) i=%d hi=%d lo=%d", v, ok, err, i, highbits(v), lowbits(v))
			}
		}

		// Re-add those values back in.
		for _, i := range rand.Perm(len(values)) {
			if _, err := tx.Add("x", values[i]); err != nil {
				t.Fatalf("Re-Add(%d) i=%d err=%q", values[i], i, err)
			}
		}

		// Verify all bits are written.
		for i, v := range values {
			if ok, err := tx.Contains("x", v); !ok || err != nil {
				t.Fatalf("Contains(%d)=(%v,%v) i=%d hi=%d lo=%d", v, ok, err, i, highbits(v), lowbits(v))
			}
		}
	})
}

func TestTx_Multiple_CreateBitmap(t *testing.T) {
	rand := rand.New(rand.NewSource(0))
	db := MustOpenDB(t)
	defer MustCloseDB(t, db)
	tx := MustBegin(t, db, true)
	defer tx.Rollback()
	values := GenerateValues(rand, 2)

	if err := tx.CreateBitmap("x/1"); err != nil {
		t.Fatal(err)
	}

	// Insert values in random order.
	for _, i := range rand.Perm(len(values)) {
		if _, err := tx.Add("x/1", values[i]); err != nil {
			t.Fatalf("Add(%d) i=%d err=%q", values[i], i, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit 1 err=%q", err)
	}

	tx1 := MustBegin(t, db, true)
	defer tx1.Rollback()

	if err := tx1.CreateBitmap("x/2"); err != nil {
		t.Fatal(err)
	}

	// Insert values in random order.
	for _, i := range rand.Perm(len(values)) {
		if _, err := tx1.Add("x/2", values[i]); err != nil {
			t.Fatalf("Add(%d) i=%d err=%q", values[i], i, err)
		}
	}
	if err := tx1.Commit(); err != nil {
		t.Fatalf("Commit 2 err=%q", err)
	}
}

func TestTx_CursorCrashArray(t *testing.T) {
	db := MustOpenDB(t)
	defer MustCloseDB(t, db)
	tx := MustBegin(t, db, true)
	defer tx.Rollback()
	if err := tx.CreateBitmap("x"); err != nil {
		t.Fatal(err)
	}
	c, err := tx.Cursor("x")
	if err != nil {
		t.Fatal(err)
	}
	//setArray(t, 0, 2379, &c)
	//setArray(t, 1, 2337, &c)
	setArray(t, 32, 1216, &c)
	setArray(t, 33, 1195, &c)
	setArray(t, 48, 1186, &c)
	setArray(t, 49, 1223, &c)
	setArray(t, 50, 1223, &c)

}

func TestTx_CursorCrashBitmap(t *testing.T) {
	if testing.Short() {
		t.Skip("-short enabled, skipping")
	}

	db := MustOpenDB(t)
	defer MustCloseDB(t, db)
	tx := MustBegin(t, db, true)
	defer tx.Rollback()
	if err := tx.CreateBitmap("x"); err != nil {
		t.Fatal(err)
	}
	c, err := tx.Cursor("x")
	if err != nil {
		t.Fatal(err)
	}
	setArray(t, 0, 22510, &c)
	setArray(t, 1, 23584, &c)
}

func setArray(tb testing.TB, key, num int, c *rbf.Cursor) {
	for i := uint64(0); i < uint64(num); i++ {
		v := i | (uint64(key) << 16)
		if _, err := c.Add(v); err != nil {
			tb.Fatal(err)
		}
	}
}

func BenchmarkTx_Add(b *testing.B) {
	for _, n := range []int{10000, 100000, 1000000} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			rand := rand.New(rand.NewSource(0))

			values := make([]uint64, n)
			for i := range values {
				values[i] = uint64(rand.Intn(rbf.ShardWidth))
			}
			b.ResetTimer()
			t := time.Now()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				func() {
					db := MustOpenDB(b)
					defer MustCloseDB(b, db)
					tx := MustBegin(b, db, true)
					defer tx.Rollback()

					for _, v := range values {
						if _, err := tx.Add("x", v); err != nil {
							b.Fatalf("Add(%d) i=%d err=%q", v, i, err)
						}
					}
				}()
			}

			b.ReportMetric(float64(time.Since(t).Nanoseconds())/float64(n*b.N), "ns/op")
		})
	}
}

func BenchmarkTx_Contains(b *testing.B) {
	for _, n := range []int{10000, 100000, 1000000} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			rand := rand.New(rand.NewSource(0))

			values := make([]uint64, n)
			for i := range values {
				values[i] = uint64(rand.Intn(rbf.ShardWidth))
			}

			db := MustOpenDB(b)
			defer MustCloseDB(b, db)
			tx := MustBegin(b, db, true)
			defer tx.Rollback()

			b.ResetTimer()
			t := time.Now()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				for _, v := range values {
					if _, err := tx.Contains("x", v); err != nil {
						b.Fatalf("Contains(%d) i=%d err=%q", v, i, err)
					}
				}
			}

			b.ReportMetric(float64(time.Since(t).Nanoseconds())/float64(n*b.N), "ns/op")
		})
	}
}

func TestTx_Dump(t *testing.T) {
	db := MustOpenDB(t)
	defer MustCloseDB(t, db)
	tx := MustBegin(t, db, true)
	defer tx.Rollback()

	index, field, view, shard := "i", "f", "v", uint64(15)

	nm := rbfName(index, field, view, shard)

	if err := tx.CreateBitmap(nm); err != nil {
		t.Fatal(err)
	} else if _, err := tx.Add(nm, 0x00000001, 0x00000002, 0x00010003, 0x00030004); err != nil {
		t.Fatal(err)
	}

	// test that we don't crash, and get *something* back
	s := tx.DumpString(true, math.MaxUint64)
	if s == "" {
		panic("should have had 3 containers!")
	}
}

func rbfName(index, field, view string, shard uint64) string {
	return string(txkey.Prefix(index, field, view, shard))
}
