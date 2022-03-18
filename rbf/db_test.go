// Copyright 2021 Molecula Corp. All rights reserved.
package rbf_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	_ "net/http/pprof"

	"github.com/felixge/fgprof"
	"github.com/molecula/featurebase/v3/rbf"
	rbfcfg "github.com/molecula/featurebase/v3/rbf/cfg"
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

	t.Run("ErrTxTooLargeWithBitmap", func(t *testing.T) {
		config := rbfcfg.NewDefaultConfig()
		config.MaxWALSize = 5 * rbf.PageSize

		db := MustOpenDB(t, config)
		defer MustCloseDB(t, db)

		tx := MustBegin(t, db, true)
		defer tx.Rollback()

		if err := tx.CreateBitmap("x"); err != nil {
			t.Fatal(err)
		}

		// Fill array until it has the maximum number of elements.
		for i := uint64(0); i < rbf.ArrayMaxSize; i++ {
			if _, err := tx.Add("x", i); err != nil {
				t.Fatal(err)
			}
		}

		// Issuing one more item to a full array should convert it to a bitmap
		// page and cause the write to return "tx too large". Previous to the
		// FB-828 fix, this would write past the mmap size so it was inaccessible.
		if _, err := tx.Add("x", rbf.ArrayMaxSize); err == nil || !errors.Is(err, rbf.ErrTxTooLarge) {
			t.Fatalf("unexpected error: %#v", err)
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

	// initially this is just a cut and paste of the Halt test, except that
	// we close the DB while the reads are still running.
	t.Run("Close", func(t *testing.T) {
		if testing.Short() {
			t.Skip("-short enabled, skipping")
		}

		config := rbfcfg.NewDefaultConfig()
		config.MaxWALSize = 16 * rbf.PageSize
		config.MaxWALCheckpointSize = 8 * rbf.PageSize
		config.MinWALCheckpointSize = 4 * rbf.PageSize

		db := MustOpenDB(t, config)

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
						// give the db time to close between when we opened and
						// when we run the Container call
						time.Sleep(10 * time.Millisecond)
						_, err = tx.Container("x", 0)
						if err != nil {
							t.Fatalf("requesting container: %v", err)
						}
						defer tx.Rollback()
						return nil
					}(); err != nil {
						// it's okay to ErrClosed, because we plan to close
						// the database out from under us.
						if err != rbf.ErrClosed {
							return err
						} else {
							return nil
						}
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
				time.Sleep(1 * time.Millisecond)
			}()
		}
		// close the db now.
		err := db.Close()
		if err != nil {
			t.Fatalf("closing db: %v", err)
		}
		// delay a bit to let some readers try to read
		time.Sleep(20 * time.Millisecond)

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

					n := rand.Intn(500) + 500
					for i := 0; i < n; i++ {
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
	for i := 0; i < 100; i++ {
		func() {
			tx, err := db.Begin(true)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()

			n := rand.Intn(90) + 10
			for j := 0; j < n; j++ {
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

func TestDB_DebugInfo(t *testing.T) {
	db := MustOpenDB(t)
	defer MustCloseDB(t, db)

	tx := MustBegin(t, db, true)
	defer tx.Rollback()

	info := db.DebugInfo()
	if got, want := info.Path, db.Path; got != want {
		t.Fatalf("Path=%q, want %q", got, want)
	} else if got, want := len(info.Txs), 1; got != want {
		t.Fatalf("len(Txs)=%d, want %d", got, want)
	}
}

// premake pool of random values
const randPool = (1 << 18)

// benchmarkOneCheckpoint
func benchmarkOneCheckpoint(b *testing.B, randInts []int) {
	cfg := rbfcfg.NewDefaultConfig()
	// extremely low to force checkpointing
	cfg.MinWALCheckpointSize = rbf.PageSize * 16
	cfg.MaxWALCheckpointSize = rbf.PageSize * 64
	var _ rbfcfg.Config
	db := MustOpenDB(b, cfg)
	defer MustCloseDB(b, db)

	// Run multiple readers in separate goroutines.
	ctx, cancel := context.WithCancel(context.Background())
	g, ctx := errgroup.WithContext(ctx)
	for i := 0; i < 8; i++ {
		i := i
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

					time.Sleep(time.Duration(rand.Intn(int(3 * time.Millisecond))))

					times := rand.Intn(1000) + 1
					for j := 0; j < times; j++ {
						v := randInts[((i<<10)+j)%(randPool-1)]
						if _, err := tx.Contains("x", uint64(v)); err != nil {
							return err
						}
					}
					return nil
				}(); err != nil {
					return err
				}
				// time.Sleep(time.Duration(rand.Intn(int(3 * time.Millisecond))))
			}
		})
	}

	// Continuously set/clear bits while readers are executing.
	next := 0
	for i := 0; i < 1000; i++ {
		func() {
			tx, err := db.Begin(true)
			if err != nil {
				b.Fatal(err)
			}
			defer tx.Rollback()

			times := rand.Intn(100)
			for j := 0; j < times; j++ {
				v := randInts[next]
				next = (next + 1) % (randPool - 1)
				if j&7 == 0 {
					// some removes but they're less frequent
					if _, err := tx.Remove("x", uint64(v)); err != nil {
						b.Fatal(err)
					}
				} else {
					if _, err := tx.Add("x", uint64(v)); err != nil {
						b.Fatal(err)
					}
				}

			}
			if err := tx.Commit(); err != nil {
				b.Fatal(err)
			}
		}()
	}

	// Stop readers & wait.
	cancel()
	if err := g.Wait(); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkDbCheckpoint(b *testing.B) {
	out, err := os.Create("cp.out")
	if err != nil {
		b.Fatalf("creating log file: %v", err)
	}
	done := fgprof.Start(out, fgprof.FormatPprof)
	b.StopTimer()
	// premake these because otherwise it's >5% of CPU in the reads
	randInts := make([]int, randPool)
	for i := range randInts {
		v1, v2 := rand.Intn(1<<24), rand.Intn(1<<24)
		// minimum gives us a skewed distribution which makes lower values more
		// likely than higher values, so we get a mix of container types
		if v1 < v2 {
			randInts[i] = v1
		} else {
			randInts[i] = v2
		}
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		benchmarkOneCheckpoint(b, randInts)
	}
	b.StopTimer()
	done()
}

// better diagnosis of deadlocks/hung situations versus just really slow "Quick" tests.
func TestMain(m *testing.M) {
	l, err := net.Listen("tcp", "localhost:0")
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
