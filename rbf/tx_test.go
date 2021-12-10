package rbf_test

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/molecula/featurebase/v2/rbf"
	"github.com/molecula/featurebase/v2/roaring"
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

	t.Run("ReopenReadOnly", func(t *testing.T) {
		db := MustOpenDB(t)
		defer func() { MustCloseDB(t, db) }()

		// Create bitmap in transaction and commit.
		if tx, err := db.Begin(true); err != nil {
			t.Fatal(err)
		} else if err := tx.CreateBitmap("x"); err != nil {
			t.Fatal(err)
		} else if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		db = MustReopenDB(t, db)

		// Create bitmap again but it should fail as it already exists.
		if tx, err := db.Begin(false); err != nil {
			t.Fatal(err)
		} else if _, err := tx.Count("x"); err != nil {
			t.Fatal(err)
		} else if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("SingleWriter", func(t *testing.T) {
		db := MustOpenDB(t)
		defer MustCloseDB(t, db)

		var wg sync.WaitGroup
		defer wg.Wait()

		// Start write transaction.
		ch0 := make(chan struct{})
		tx0 := MustBegin(t, db, true)
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-ch0
			tx0.Rollback()
		}()

		// Start separate write transaction in different goroutine.
		ch1 := make(chan struct{})
		wg.Add(1)
		go func() {
			defer wg.Done()
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

// deallocateTree had a bug which caused a page to be marked neither free
// nor in-use.
func TestTx_DeallocateTree(t *testing.T) {
	db := MustOpenDB(t)
	defer MustCloseDB(t, db)
	tx := MustBegin(t, db, true)
	defer tx.Rollback()
	var err error

	// Create bitmap & add value.
	if err = tx.CreateBitmap("x"); err != nil {
		t.Fatal(err)
	}
	const N = 315
	slots := make([]uint64, N)
	for i := range slots {
		slots[i] = uint64(i) << 20
	}
	if _, err = tx.Add("x", slots...); err != nil {
		t.Fatal(err)
	}
	if err = tx.Check(); err != nil {
		t.Fatalf("check: %v", err)
	}
	if err = tx.DeleteBitmap("x"); err != nil {
		t.Fatal(err)
	}
	if ok, err := tx.Contains("x", 0); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatal("expected no value in recreated bitmap")
	}
	if err = tx.Check(); err != nil {
		t.Fatalf("check: %v", err)
	}
}

func TestTx_RecreateBitmap(t *testing.T) {
	db := MustOpenDB(t)
	defer MustCloseDB(t, db)
	tx := MustBegin(t, db, true)
	defer tx.Rollback()

	// Create bitmap & add value.
	if err := tx.CreateBitmap("x"); err != nil {
		t.Fatal(err)
	}
	const N = 825000
	slots := make([]uint64, N)
	for i := range slots {
		slots[i] = uint64(i) << 20
	}
	if _, err := tx.Add("x", slots...); err != nil {
		t.Fatal(err)
	}
	err := tx.Commit()
	if err != nil {
		t.Fatal(err)
	}
	tx = MustBegin(t, db, true)
	defer tx.Rollback()
	// Delete bitmap, verifying that it's gone.
	if err := tx.DeleteBitmap("x"); err != nil {
		t.Fatal(err)
	} else {
		if ok, err := tx.Contains("x", 0); err != nil {
			t.Fatal(err)
		} else if ok {
			t.Fatal("expected no value in recreated bitmap")
		}
	}
	err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}
	tx = MustBegin(t, db, true)
	defer tx.Rollback()
	if err := tx.CreateBitmap("x"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Add("x", slots...); err != nil {
		t.Fatal(err)
	}
	err = tx.Commit()
	if err != nil {
		t.Fatal(err)
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

func TestTx_DeallocateToFreeList(t *testing.T) {
	db := MustOpenDB(t)
	defer MustCloseDB(t, db)
	tx := MustBegin(t, db, true)
	defer tx.Rollback()
	var err error

	// Create bitmap & add value.
	if err = tx.CreateBitmap("x"); err != nil {
		t.Fatal(err)
	}
	if err = tx.CreateBitmap("y"); err != nil {
		t.Fatal(err)
	}
	const N = 12274831
	slots := make([]uint64, N)
	for i := range slots {
		slots[i] = uint64(i) << 10
	}
	bm := roaring.NewBitmap(slots...)
	if _, err = tx.AddRoaring("x", bm); err != nil {
		t.Fatal(err)
	}
	if err = tx.Check(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 500; i++ {
		if _, err := tx.Add("y", uint64(i)<<16); err != nil {
			t.Fatal(err)
		}
	}
	if err = tx.Check(); err != nil {
		t.Fatal(err)
	}
	if err := tx.DeleteBitmap("y"); err != nil {
		t.Fatal(err)
	}
	if err = tx.Check(); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	tx = MustBegin(t, db, true)
	defer tx.Rollback()
	// Delete bitmap, verifying that it's gone.
	if err := tx.DeleteBitmap("x"); err != nil {
		t.Fatal(err)
	} else {
		if ok, err := tx.Contains("x", 0); err != nil {
			t.Fatal(err)
		} else if ok {
			t.Fatal("expected no value in recreated bitmap")
		}
	}
	if err = tx.Check(); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	tx = MustBegin(t, db, true)
	defer tx.Rollback()
	if err := tx.CreateBitmap("x"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.AddRoaring("x", bm); err != nil {
		t.Fatal(err)
	}
	if err = tx.Check(); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestTx_AddRemove_Quick(t *testing.T) {
	if testing.Short() {
		t.Skip("-short enabled, skipping")
	} else if is32Bit() {
		t.Skip("32-bit build, skipping quick check tests")
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
	setArray(t, 32, 1216, c)
	setArray(t, 33, 1195, c)
	setArray(t, 48, 1186, c)
	setArray(t, 49, 1223, c)
	setArray(t, 50, 1223, c)

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
	setArray(t, 0, 22510, c)
	setArray(t, 1, 23584, c)
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
	for _, n := range []int{1, 10, 1000} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			rand := rand.New(rand.NewSource(0))

			values := make([]uint64, n)
			for i := range values {
				values[i] = uint64(rand.Intn(rbf.ShardWidth))
			}
			b.ResetTimer()

			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				func() {
					db := MustOpenDB(b)
					defer MustCloseDB(b, db)

					for _, v := range values {
						func() {
							tx := MustBegin(b, db, true)
							defer tx.Rollback()
							if _, err := tx.Add("x", v); err != nil {
								b.Fatal(err)
							} else if err := tx.Commit(); err != nil {
								b.Fatal(err)
							}
						}()
					}
				}()
			}
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

func TestTx_CreateBitmap(t *testing.T) {
	t.Run("Bulk", func(t *testing.T) {
		db := MustOpenDB(t)
		defer MustCloseDB(t, db)

		tx, err := db.Begin(true)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()

		if err := tx.CreateBitmap(fmt.Sprintf("%4000x", 0)); err != nil {
			t.Fatal(err)
		} else if err := tx.CreateBitmap(fmt.Sprintf("%4000x", 1)); err != nil {
			t.Fatal(err)
		} else if err := tx.CreateBitmap(fmt.Sprintf("%4000x", 2)); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestTx_DeleteBitmapsWithPrefix(t *testing.T) {
	db := MustOpenDB(t)
	defer MustCloseDB(t, db)
	prefix := "abc"
	bitmapSize := 10000
	//	var err error
	// create a interleaved set up array and bitmap containers

	bits := make([]uint64, bitmapSize)
	x := uint64(1)
	for i := 0; i < len(bits); i++ {
		bits[i] = x
		x = x + 2
	}
	ifError := func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	}
	checkInfos := func() {
		tx := MustBegin(t, db, false)
		defer tx.Rollback()
		infos, err := tx.PageInfos()
		ifError(err)
		for pgno, info := range infos {
			switch info := info.(type) {
			case *rbf.MetaPageInfo:
				fmt.Printf("%-8d ", pgno)
				fmt.Printf("%-10s ", "meta")
				fmt.Printf("pageN=%d,walid=%d,rootrec=%d,freelist=%d\n", info.PageN, info.WALID, info.RootRecordPageNo, info.FreelistPageNo)

			case *rbf.RootRecordPageInfo:
				fmt.Printf("%-8d ", pgno)
				fmt.Printf("%-10s ", "rootrec")
				fmt.Printf("next=%d\n", info.Next)

			case *rbf.LeafPageInfo:
				fmt.Printf("%-8d ", pgno)
				fmt.Printf("%-10s ", "leaf")
				fmt.Printf("flags=x%x,celln=%d\n", info.Flags, info.CellN)

			case *rbf.BranchPageInfo:
				fmt.Printf("%-8d ", pgno)
				fmt.Printf("%-10s ", "branch")
				fmt.Printf("flags=x%x,celln=%d\n", info.Flags, info.CellN)

			case *rbf.BitmapPageInfo:
				fmt.Printf("%-8d ", pgno)
				fmt.Printf("%-10s ", "bitmap")
				fmt.Printf("-\n")

			case *rbf.FreePageInfo:
				fmt.Printf("%-8d ", pgno)
				fmt.Printf("%-10s ", "free")
				fmt.Printf("-\n")

			default:
				t.Fatal(fmt.Sprintf("unexpected page info type %T", info))
			}
		}

	}
	populate := func() {
		tx := MustBegin(t, db, true)
		defer tx.Rollback()
		for i := uint64(0); i < 16; i++ {
			bm := roaring.NewBitmap()
			if i%3 == 0 {
				bm.Put(i, roaring.NewContainerBitmap(6144, bits))
				if _, err := tx.AddRoaring(prefix, bm); err != nil {
					panic(err)
				}
			} else {
				bm.Put(i, roaring.NewContainerArray([]uint16{2, 4, 5, 7}))
				if _, err := tx.AddRoaring(prefix, bm); err != nil {
					panic(err)
				}

			}
		}
		ifError(tx.Commit())
	}

	checkInfos()
	populate()
	checkInfos()
	ifError(db.Check())

	tx := MustBegin(t, db, true)
	tx.DeleteBitmapsWithPrefix(prefix)
	ifError(tx.Commit())
	ifError(db.Check())
	checkInfos()
	populate()
	ifError(db.Check())
	checkInfos()

}
