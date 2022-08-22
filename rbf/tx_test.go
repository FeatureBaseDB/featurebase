// Copyright 2021 Molecula Corp. All rights reserved.
package rbf_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/molecula/featurebase/v3/rbf"
	"github.com/molecula/featurebase/v3/roaring"
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
		case <-time.After(50 * time.Millisecond):
		}

		// Finish first transaction.
		close(ch0)
		select {
		case <-ch1:
		case <-time.After(10 * time.Second):
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

func arraySizedChunk() []uint16 {
	v := make([]uint16, rbf.ArrayMaxSize)
	for i := range v {
		v[i] = uint16(i)
	}
	return v
}

var convenientPrepopulatedArray = arraySizedChunk()

// populateBitmapWithArrays
func populateBitmapWithArrays(tb testing.TB, tx *rbf.Tx, n int, name string) {
	c := roaring.NewContainerArray(convenientPrepopulatedArray)
	for i := 0; i < n; i++ {
		err := tx.PutContainer(name, uint64(i), c)
		if err != nil {
			tb.Fatal(err)
		}
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
	const N = 825
	populateBitmapWithArrays(t, tx, N, "x")
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
	populateBitmapWithArrays(t, tx, N, "x")
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
	// Insert large array values.
	populateBitmapWithArrays(t, tx, 4080, "x")

	if err = tx.Check(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 500; i++ {
		if _, err := tx.Add("y", uint64(i)<<16+32768); err != nil {
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
	populateBitmapWithArrays(t, tx, 4080, "x")

	if err = tx.Check(); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func TestTx_RemoveContainer(t *testing.T) {
	t.Parallel()

	db := MustOpenDB(t)
	defer MustCloseDB(t, db)

	tx := MustBegin(t, db, true)
	defer tx.Rollback()

	if err := tx.CreateBitmap("x"); err != nil {
		t.Fatal(err)
	}

	// Insert large array values.
	populateBitmapWithArrays(t, tx, 500, "x")

	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	tx = MustBegin(t, db, true)
	defer tx.Rollback()

	// Remove all array values.
	for i := 0; i < 500; i++ {
		err := tx.RemoveContainer("x", uint64(i))
		if err != nil {
			t.Fatal(err)
		}
	}
	// This triggered a different panic without the relevant patch.
	err := tx.RemoveContainer("x", 500)
	if err != nil {
		t.Fatal(err)
	}

	if err := tx.Commit(); err != nil {
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

func TestTx_Remove(t *testing.T) {
	// reusable containers for the tests to work with so we don't have to do
	// millions of adds to get millions of bits
	// justEnoughBits to cause a container to mutate to bitmap in RBF
	justEnoughBits := make([]uint16, 4082)
	for i := range justEnoughBits {
		justEnoughBits[i] = uint16(i)
	}
	justEnoughBitmap := make([]uint64, 1024)
	for i := 0; i < 64; i++ {
		justEnoughBitmap[i] = ^uint64(0)
	}
	// and now a sparse-ish one, to test non-contiguous blocks
	sparseBits := make([]uint16, 0, 4082)
	// every 8th bit so it's easy to fill them in in the bitmap form
	for i := 0; i < cap(sparseBits); i++ {
		sparseBits = append(sparseBits, uint16(i*8))
	}
	sparseBitmapData := make([]uint64, 1024)
	for i := range sparseBitmapData {
		sparseBitmapData[i] = 0x0101010101010101
	}
	tinyArray := roaring.NewContainerArray(justEnoughBits[:1])
	smallArray := roaring.NewContainerArray(justEnoughBits[:8])
	bigArray := roaring.NewContainerArray(justEnoughBits[:rbf.ArrayMaxSize])
	smallBitmap := roaring.NewContainerBitmapN(justEnoughBitmap, 4096)
	sparseSmallArray := roaring.NewContainerArray(sparseBits[:16])
	sparseBigArray := roaring.NewContainerArray(sparseBits[:4070])
	sparseBitmap := roaring.NewContainerBitmapN(sparseBitmapData, 65536/8)
	t.Logf("sparseSmallArray: %d", sparseSmallArray.N())

	t.Run("FullContiguous", func(t *testing.T) {
		if testing.Short() {
			t.Skip("-short enabled, skipping")
		}

		for _, containers := range []uint64{1, 2, 31} {
			t.Run(fmt.Sprint(containers), func(t *testing.T) {
				db := MustOpenDB(t)
				defer MustCloseDB(t, db)

				// Add bits
				func() {
					tx := MustBegin(t, db, true)
					defer tx.Rollback()

					if err := tx.CreateBitmap("x"); err != nil {
						t.Fatal(err)
					}
					// for each container, we want to create it as an array, then turn it into a bitmap.
					for i := uint64(0); i < containers; i++ {
						if err := tx.PutContainer("x", i, smallArray); err != nil {
							t.Fatalf("putting small container %d: %v", i, err)
						}
						if err := tx.PutContainer("x", i, bigArray); err != nil {
							t.Fatalf("putting big container %d: %v", i, err)
						}
						if err := tx.PutContainer("x", i, smallBitmap); err != nil {
							t.Fatalf("putting bitmap container %d: %v", i, err)
						}
					}
					if err := tx.Commit(); err != nil {
						t.Fatal(err)
					}
				}()

				// Remove bits
				func() {
					tx := MustBegin(t, db, true)
					defer tx.Rollback()

					for i := uint64(0); i < containers; i++ {
						// remove all the bits
						if err := tx.PutContainer("x", i, bigArray); err != nil {
							t.Fatalf("putting big container %d: %v", i, err)
						}
						if err := tx.PutContainer("x", i, smallArray); err != nil {
							t.Fatalf("putting small container %d: %v", i, err)
						}
						if err := tx.PutContainer("x", i, tinyArray); err != nil {
							t.Fatalf("putting one-item container %d: %v", i, err)
						}
						if _, err := tx.Remove("x", i<<16); err != nil {
							t.Fatalf("removing last bit: %v", err)
						}
					}
					if err := tx.Commit(); err != nil {
						t.Fatal(err)
					}
				}()

				// Verify that all bits have been removed.
				tx := MustBegin(t, db, false)
				defer tx.Rollback()
				if n, err := tx.Count("x"); err != nil {
					t.Fatal(err)
				} else if got, want := n, uint64(0); got != want {
					t.Fatalf("Count=%d, want %d", got, want)
				}
			})
		}
	})

	t.Run("PartialNonContiguous", func(t *testing.T) {
		db := MustOpenDB(t)
		defer MustCloseDB(t, db)

		// Add bits
		const containers = 256
		bitsAdded := 0
		func() {
			tx := MustBegin(t, db, true)
			defer tx.Rollback()

			if err := tx.CreateBitmap("x"); err != nil {
				t.Fatal(err)
			}
			for i := uint64(0); i < containers; i++ {
				if err := tx.PutContainer("x", i, sparseSmallArray); err != nil {
					t.Fatalf("putting small container %d: %v", i, err)
				}
				bitsThisContainer := sparseSmallArray.N()
				if i&1 == 1 {
					if err := tx.PutContainer("x", i, sparseBigArray); err != nil {
						t.Fatalf("putting big container %d: %v", i, err)
					}
					bitsThisContainer = sparseBigArray.N()
				}
				if i&2 == 2 {
					if err := tx.PutContainer("x", i, sparseBitmap); err != nil {
						t.Fatalf("putting bitmap container %d: %v", i, err)
					}
					bitsThisContainer = sparseBitmap.N()
				}
				bitsAdded += int(bitsThisContainer)
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
		}()

		// Remove some bits in small contiguous chunks. First 16 bits from each container.
		var deleteN int
		for i := uint64(0); i < containers; i++ {
			func() {
				tx := MustBegin(t, db, true)
				defer tx.Rollback()
				for j := uint64(0); j < 16; j++ {
					if n, err := tx.Remove("x", (i<<16)+(j*8)); err != nil || n != 1 {
						t.Fatalf("Remove(%d)=(%d,%v)", i, n, err)
					}
					deleteN++
				}
				if err := tx.Commit(); err != nil {
					t.Fatal(err)
				}
			}()
		}

		// Verify that we have the correct count afterward.
		tx := MustBegin(t, db, false)
		defer tx.Rollback()
		if n, err := tx.Count("x"); err != nil {
			t.Fatal(err)
		} else if got, want := n, uint64(bitsAdded-deleteN); got != want {
			t.Fatalf("Count=%d, want %d", got, want)
		}
	})

	t.Run("DeleteEmptyBitmap", func(t *testing.T) {
		db := MustOpenDB(t)
		defer MustCloseDB(t, db)

		// Create bitmap.
		func() {
			tx := MustBegin(t, db, true)
			defer tx.Rollback()
			if err := tx.CreateBitmap("x"); err != nil {
				t.Fatal(err)
			} else if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
		}()

		// Remove bitmap.
		func() {
			tx := MustBegin(t, db, true)
			defer tx.Rollback()
			if err := tx.DeleteBitmap("x"); err != nil {
				t.Fatal(err)
			} else if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
		}()

		// Ensure bitmap no longer exists.
		tx := MustBegin(t, db, false)
		defer tx.Rollback()
		if exists, err := tx.BitmapExists("x"); err != nil {
			t.Fatal(err)
		} else if exists {
			t.Fatal("expected bitmap to be removed")
		}
	})

	t.Run("WithTreeDepth", func(t *testing.T) {
		for depth := 1; depth <= 3; depth++ {
			t.Run(fmt.Sprint(depth), func(t *testing.T) {
				db := MustOpenDB(t)
				defer MustCloseDB(t, db)

				// Create bitmap & insert until we hit a tree depth.
				var containerN int
				func() {
					tx := MustBegin(t, db, true)
					defer tx.Rollback()
					if err := tx.CreateBitmap("x"); err != nil {
						t.Fatal(err)
					}
					for i := uint64(0); ; i++ {
						if err := tx.PutContainer("x", i, bigArray); err != nil {
							t.Fatalf("putting big container %d: %v", i, err)
						}
						containerN++

						if d, err := tx.Depth("x"); err != nil {
							t.Fatal(err)
						} else if d == depth {
							break
						}
					}
					if err := tx.Commit(); err != nil {
						t.Fatal(err)
					}
				}()

				// Remove all bits in reverse order.
				func() {
					tx := MustBegin(t, db, true)
					defer tx.Rollback()
					for i := containerN - 1; i >= 0; i-- {
						if err := tx.PutContainer("x", uint64(i), nil); err != nil {
							t.Fatalf("removing container (%d)=(%v)", uint64(i)<<16, err)
						}
					}
					if err := tx.Commit(); err != nil {
						t.Fatal(err)
					}
				}()

				// Ensure bitmap no longer exists.
				tx := MustBegin(t, db, false)
				defer tx.Rollback()
				for i := uint64(0); i < uint64(containerN); i++ {
					c, err := tx.Container("x", i)
					if err != nil {
						t.Fatalf("checking for container %d: %v", i, err)
					}
					if c != nil {
						t.Fatalf("container %d still exists after removal", i)
					}
				}
			})
		}
	})

	t.Run("RollbackAfterDelete", func(t *testing.T) {
		db := MustOpenDB(t)
		defer MustCloseDB(t, db)

		func() {
			tx := MustBegin(t, db, true)
			defer tx.Rollback()

			if err := tx.CreateBitmap("x"); err != nil {
				t.Fatal(err)
			} else if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
		}()

		// Add bits
		const bitN = 1000
		for i := uint64(0); i < bitN; i++ {
			func() {
				tx := MustBegin(t, db, true)
				defer tx.Rollback()

				if _, err := tx.Add("x", i<<16); err != nil {
					t.Fatalf("Add(%d) err=%q", i<<16, err)
				}

				// Only commit every other bit.
				if i%2 == 1 {
					if err := tx.Commit(); err != nil {
						t.Fatal(err)
					}
				}
			}()
		}

		// Verify that we have the correct count afterward.
		tx := MustBegin(t, db, false)
		defer tx.Rollback()
		if n, err := tx.Count("x"); err != nil {
			t.Fatal(err)
		} else if got, want := n, uint64(bitN/2); got != want {
			t.Fatalf("Count=%d, want %d", got, want)
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

// FB-1229: This test verifies that the database will be truncated as pages at
// the end of the file are pushed to the freelist.
func TestTx_ReclaimAfterDelete(t *testing.T) {
	const containerN = 5000
	const batchSize = 500

	db := MustOpenDB(t)
	defer MustCloseDB(t, db)

	keys := rand.New(rand.NewSource(0)).Perm(containerN)
	inserted := make(map[uint64]struct{})

	for i := 0; i < len(keys); i += batchSize {
		func() {
			tx := MustBegin(t, db, true)
			defer tx.Rollback()

			if err := tx.CreateBitmapIfNotExists("x"); err != nil {
				t.Fatal(err)
			}

			// Insert a bunch of containers.
			c := roaring.NewContainerArray(convenientPrepopulatedArray)
			for j := 0; j < batchSize; j++ {
				key := uint64(keys[i+j])
				if err := tx.PutContainer("x", key, c); err != nil {
					t.Fatal(err)
				}
				inserted[key] = struct{}{}
			}

			// Insert some already inserted containers.
			var deleted int
			for k := range inserted {
				if err := tx.RemoveContainer("x", uint64(k)); err != nil {
					t.Fatal(err)
				}
				delete(inserted, k)

				if deleted++; deleted > 200 {
					break
				}
			}

			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
		}()
	}

	fi, err := os.Stat(db.DataPath())
	if err != nil {
		t.Fatal(err)
	}
	origSize := fi.Size()

	// Delete all containers.
	func() {
		tx := MustBegin(t, db, true)
		defer tx.Rollback()

		for _, key := range keys {
			if err := tx.RemoveContainer("x", uint64(key)); err != nil {
				t.Fatal(err)
			}
		}

		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}()

	// Verify database has shrunk after checkpoint.
	if err := db.Checkpoint(); err != nil {
		t.Fatal(err)
	} else if fi, err := os.Stat(db.DataPath()); err != nil {
		t.Fatal(err)
	} else if fi.Size() >= origSize {
		t.Fatalf("size did not shrink: originally %d bytes, ended with %d bytes", fi.Size(), origSize)
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
	var b bytes.Buffer
	pBuf := func(msg string, args ...interface{}) (int, error) {
		return fmt.Fprintf(&b, msg, args...)
	}
	checkInfos := func(pf func(string, ...interface{}) (int, error)) {
		tx := MustBegin(t, db, false)
		defer tx.Rollback()
		infos, err := tx.PageInfos()
		ifError(err)
		for pgno, info := range infos {
			switch info := info.(type) {
			case *rbf.MetaPageInfo:
				pf("%-8d ", pgno)
				pf("%-10s ", "meta")
				pf("pageN=%d,walid=%d,rootrec=%d,freelist=%d\n", info.PageN, info.WALID, info.RootRecordPageNo, info.FreelistPageNo)

			case *rbf.RootRecordPageInfo:
				pf("%-8d ", pgno)
				pf("%-10s ", "rootrec")
				pf("next=%d\n", info.Next)

			case *rbf.LeafPageInfo:
				pf("%-8d ", pgno)
				pf("%-10s ", "leaf")
				pf("flags=x%x,celln=%d\n", info.Flags, info.CellN)

			case *rbf.BranchPageInfo:
				pf("%-8d ", pgno)
				pf("%-10s ", "branch")
				pf("flags=x%x,celln=%d\n", info.Flags, info.CellN)

			case *rbf.BitmapPageInfo:
				pf("%-8d ", pgno)
				pf("%-10s ", "bitmap")
				pf("-\n")

			case *rbf.FreePageInfo:
				pf("%-8d ", pgno)
				pf("%-10s ", "free")
				pf("-\n")

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

	checkInfos(pBuf)
	populate()
	checkInfos(pBuf)
	ifError(db.Check())

	tx := MustBegin(t, db, true)
	tx.DeleteBitmapsWithPrefix(prefix)
	ifError(tx.Commit())
	ifError(db.Check())
	checkInfos(pBuf)
	populate()
	ifError(db.Check())
	checkInfos(pBuf)

}

func TestTx_Check(t *testing.T) {
	t.Run("EmptyBranchPage", func(t *testing.T) {
		t.Parallel()

		db := MustOpenDB(t)
		defer MustCloseDBNoCheck(t, db)
		tx := MustBegin(t, db, true)
		defer tx.Rollback()

		if err := tx.CreateBitmap("x"); err != nil {
			t.Fatal(err)
		}

		// Insert enough array containers to split page.
		for i := 0; i < 1000; i++ {
			if _, err := tx.Add("x", uint64(i<<16)); err != nil {
				t.Fatalf("Add(%d) err=%q", i<<16, err)
			}
		}

		// Read page types for all pages.
		infos, err := tx.PageInfos()
		if err != nil {
			t.Fatal(err)
		}

		// Commit & checkpoint to flush to the data file.
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		} else if err := db.Checkpoint(); err != nil {
			t.Fatal(err)
		}

		// Corrupt first branch page found by zeroing out the cell count.
		var pgno uint32
		for _, info := range infos {
			if info, ok := info.(*rbf.BranchPageInfo); ok {
				pgno = info.Pgno
				page := mustReadPage(t, db.DataPath(), pgno)
				binary.BigEndian.PutUint16(page[8:10], 0) // zero cell count
				mustWritePage(t, db.DataPath(), pgno, page)
				break
			}
		}

		// Verify that check now returns an error.
		if err := db.Check(); err == nil || !strings.Contains(err.Error(), fmt.Sprintf("branch page %d is empty", pgno)) {
			t.Fatalf("unexpected error: %#v", err)
		}
	})

	t.Run("ErrBadFreelist", func(t *testing.T) {
		t.Parallel()

		db := MustOpenDBAt(t, filepath.Join("testdata", "check", "bad-freelist"))
		defer db.Close()
		tx := MustBegin(t, db, false)
		defer tx.Rollback()

		if err, ok := tx.Check().(rbf.ErrorList); !ok {
			t.Fatal("expected error list")
		} else if s := err.FullError(); !strings.Contains(s, `branch cell index out of range: pgno=2 i=0 n=0`) {
			t.Fatalf("unexpected error:\n%s", s)
		}
	})

	t.Run("ErrBadBitmap", func(t *testing.T) {
		t.Parallel()

		db := MustOpenDBAt(t, filepath.Join("testdata", "check", "bad-bitmap"))
		defer db.Close()
		tx := MustBegin(t, db, false)
		defer tx.Rollback()

		if err, ok := tx.Check().(rbf.ErrorList); !ok {
			t.Fatal("expected error list")
		} else if s := err.FullError(); !strings.Contains(s, `cannot read page: pgno=65537 parent=3 err=rbf: page read out of bounds: pgno=65537 max=3`) {
			t.Fatalf("unexpected error:\n%s", s)
		}
	})
}

func mustReadPage(tb testing.TB, path string, pgno uint32) []byte {
	tb.Helper()
	f, err := os.Open(path)
	if err != nil {
		tb.Fatal(err)
	}
	defer f.Close()

	buf := make([]byte, rbf.PageSize)
	if _, err := f.ReadAt(buf, int64(pgno)*rbf.PageSize); err != nil {
		tb.Fatal(err)
	}
	return buf
}

func mustWritePage(tb testing.TB, path string, pgno uint32, buf []byte) {
	tb.Helper()
	f, err := os.OpenFile(path, os.O_WRONLY, 0600)
	if err != nil {
		tb.Fatal(err)
	}

	if _, err := f.WriteAt(buf, int64(pgno)*rbf.PageSize); err != nil {
		tb.Fatal(err)
	} else if err := f.Close(); err != nil {
		tb.Fatal(err)
	}
}
