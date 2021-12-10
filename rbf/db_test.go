package rbf_test

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	_ "net/http/pprof"

	"github.com/molecula/featurebase/v2/rbf"
	rbfcfg "github.com/molecula/featurebase/v2/rbf/cfg"
	"golang.org/x/sync/errgroup"
)

func TestDB_Open(t *testing.T) {
	db := NewDB(t)
	if err := db.Open(); err != nil {
		t.Fatal(err)
	} else if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDB_WAL(t *testing.T) {
	t.Run("ErrTxTooLarge", func(t *testing.T) {
		config := rbfcfg.NewDefaultConfig()
		config.MaxWALSize = 4 * rbf.PageSize

		db := MustOpenDB(t, config)
		defer MustCloseDB(t, db)

		tx := MustBegin(t, db, true)
		defer tx.Rollback()

		if err := tx.CreateBitmap("x"); err != nil {
			t.Fatal(err)
		} else if err := tx.CreateBitmap("y"); err != rbf.ErrTxTooLarge {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("Halt", func(t *testing.T) {
		if testing.Short() {
			t.Skip("-short enabled, skipping")
		}

		config := rbfcfg.NewDefaultConfig()
		config.MaxWALSize = 16 * rbf.PageSize
		config.MaxWALCheckpointSize = 8 * rbf.PageSize
		config.MinWALCheckpointSize = 4 * rbf.PageSize

		db := MustOpenDB(t, config)
		defer MustCloseDB(t, db)

		// Continuously run read overlapping transactions.
		ctx, cancel := context.WithCancel(context.Background())
		g, ctx := errgroup.WithContext(ctx)
		for i := 0; i < 10; i++ {
			i := i
			g.Go(func() error {
				time.Sleep(time.Duration(i) * 10 * time.Millisecond) // stagger

				for {
					if err := ctx.Err(); err != nil {
						return nil
					}

					if err := func() error {
						tx, err := db.Begin(false)
						if err != nil {
							return err
						}
						defer tx.Rollback()
						time.Sleep(20 * time.Millisecond)
						return nil
					}(); err != nil {
						return err
					}
				}
			})
		}

		// Generate updates to the DB/WAL.
		for i := 0; i < 100; i++ {
			func() {
				tx := MustBegin(t, db, true)
				defer tx.Rollback()

				if err := tx.CreateBitmapIfNotExists("x"); err != nil {
					t.Fatal(err)
				} else if _, err := tx.Add("x", uint64(i)); err != nil {
					t.Fatal(err)
				} else if err := tx.Commit(); err != nil {
					t.Fatal(err)
				}
			}()
		}

		// Stop read transactions & wait.
		cancel()
		if err := g.Wait(); err != nil {
			t.Fatal(err)
		}
	})
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
		tx0, err := db.Begin(true)
		if err != nil {
			t.Fatal(err)
		} else if _, err := tx0.Add("x", uint64(len(a))); err != nil {
			t.Fatal(err)
		}

		// Start a read-only transaction so the write tx does not checkpoint the WAL.
		tx1, err := db.Begin(false)
		if err != nil {
			t.Fatal(err)
		}

		// Commit write transaction.
		if err := tx0.Commit(); err != nil {
			t.Fatal(err)
		}
		tx1.Rollback()

		// Close database & truncate WAL to remove commit page & bitmap data page.
		walPath, walSize := db.WALPath(), db.WALSize()
		if err := db.Close(); err != nil {
			t.Fatal(err)
		} else if err := os.Truncate(walPath, walSize-(2*rbf.PageSize)); err != nil {
			t.Fatal(err)
		}

		// Reopen database.
		newDB := rbf.NewDB(db.Path, nil)
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

// Ensures the DB can continuously write while readers are executing.
func TestDB_MultiTx(t *testing.T) {
	if testing.Short() {
		t.Skip("-short enabled, skipping")
	}

	cfg := rbfcfg.NewDefaultConfig()
	db := MustOpenDB(t, cfg)
	defer MustCloseDB(t, db)

	// Run multiple readers in separate goroutines.
	ctx, cancel := context.WithCancel(context.Background())
	g, ctx := errgroup.WithContext(ctx)
	for i := 0; i < 4; i++ {
		g.Go(func() error {
			for {
				if ctx.Err() != nil {
					return nil // cancelled, return no error
				} else if err := func() error {
					tx, err := db.Begin(false)
					if err != nil {
						return err
					}
					defer tx.Rollback()

					time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)

					for i := 0; i < rand.Intn(1000); i++ {
						v := rand.Intn(1 << 20)
						if _, err := tx.Contains("x", uint64(v)); err != nil {
							return err
						}
					}
					return nil
				}(); err != nil {
					return err
				}

				time.Sleep(time.Duration(rand.Intn(int(100 * time.Millisecond))))
			}
		})
	}

	// Continuously set/clear bits while readers are executing.
	for i := 0; i < 1000; i++ {
		func() {
			tx, err := db.Begin(true)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()

			for j := 0; j < rand.Intn(100); j++ {
				v := rand.Intn(1 << 20)
				if _, err := tx.Add("x", uint64(v)); err != nil {
					t.Fatal(err)
				}

			}

			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
		}()
	}

	// Stop readers & wait.
	cancel()
	if err := g.Wait(); err != nil {
		t.Fatal(err)
	}
}

// better diagnosis of deadlocks/hung situations versus just really slow "Quick" tests.
func TestMain(m *testing.M) {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		panic(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	fmt.Printf("rbf/ TestMain: online stack-traces: curl http://localhost:%v/debug/pprof/goroutine?debug=2\n", port)
	go func() {
		err := http.Serve(l, nil)
		if err != nil {
			panic(err)
		}
	}()
	os.Exit(m.Run())
}
