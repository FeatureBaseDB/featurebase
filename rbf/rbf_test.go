// Copyright 2021 Molecula Corp. All rights reserved.
package rbf_test

import (
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"testing"

	"github.com/molecula/featurebase/v3/logger"
	"github.com/molecula/featurebase/v3/rbf"
	rbfcfg "github.com/molecula/featurebase/v3/rbf/cfg"
	"github.com/molecula/featurebase/v3/testhook"
)

var quickCheckN *int = flag.Int("quickchecks", 10, "The number of iterations for each quickcheck")

// Ensure root record helper functions work to read & write root records.
func TestReadWriteRootRecord(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		buf := make([]byte, 26)

		// Write records.
		if remaining, err := rbf.WriteRootRecord(buf, &rbf.RootRecord{Pgno: 10, Name: "foo"}); err != nil {
			t.Fatal(err)
		} else if remaining, err = rbf.WriteRootRecord(remaining, &rbf.RootRecord{Pgno: 11, Name: "bar"}); err != nil {
			t.Fatal(err)
		} else if _, err := rbf.WriteRootRecord(remaining, &rbf.RootRecord{Pgno: 12, Name: "baz"}); err != io.ErrShortBuffer {
			t.Fatalf("unexpected error: %#v", err) // buffer too short
		}

		// Read records back.
		if rec, remaining, err := rbf.ReadRootRecord(buf); err != nil {
			t.Fatal(err)
		} else if got, want := *rec, (rbf.RootRecord{Pgno: 10, Name: "foo"}); got != want {
			t.Fatalf("ReadRootRecord=%#v, want %#v", got, want)
		} else if rec, remaining, err = rbf.ReadRootRecord(remaining); err != nil {
			t.Fatal(err)
		} else if got, want := *rec, (rbf.RootRecord{Pgno: 11, Name: "bar"}); got != want {
			t.Fatalf("ReadRootRecord=%#v, want %#v", got, want)
		} else if rec, _, _ := rbf.ReadRootRecord(remaining); rec != nil {
			t.Fatalf("expected nil record, got %#v", rec)
		}
	})
}

// NewDB returns a new instance of DB with a temporary path.
func NewDB(tb testing.TB, cfg ...*rbfcfg.Config) *rbf.DB {
	path, err := testhook.TempDir(tb, "rbfdb")
	if err != nil {
		panic(err)
	}
	return NewDBAt(tb, path, cfg...)
}

// NewDBAt returns a new instance of DB with a given path.
func NewDBAt(tb testing.TB, path string, cfg ...*rbfcfg.Config) *rbf.DB {
	var cfg0 *rbfcfg.Config
	if len(cfg) > 0 {
		cfg0 = cfg[0]
	}
	return rbf.NewDB(path, cfg0)
}

// MustOpenDB returns a db opened on a temporary file. On error, fail test.
func MustOpenDB(tb testing.TB, cfg ...*rbfcfg.Config) *rbf.DB {
	tb.Helper()
	path, err := testhook.TempDir(tb, "rbfdb")
	if err != nil {
		panic(err)
	}
	return MustOpenDBAt(tb, path, cfg...)
}

// MustOpenDBAt returns a db opened on an existing file. On error, fail test.
func MustOpenDBAt(tb testing.TB, path string, cfg ...*rbfcfg.Config) *rbf.DB {
	tb.Helper()
	if len(cfg) == 0 || cfg[0] == nil {
		newconf := rbfcfg.NewDefaultConfig()
		newconf.Logger = logger.NewLogfLogger(tb)
		cfg = []*rbfcfg.Config{newconf}
	} else if cfg[0].Logger == nil {
		cfg[0].Logger = logger.NewLogfLogger(tb)
	}
	db := NewDBAt(tb, path, cfg...)
	if err := db.Open(); err != nil {
		tb.Fatal(err)
	}
	return db
}

// MustCloseDB closes db. On error, fail test.
// This function also also performs an integrity check on the DB.
func MustCloseDB(tb testing.TB, db *rbf.DB) {
	tb.Helper()
	if err := db.Check(); err != nil && err != rbf.ErrClosed {
		tb.Fatal(err)
	}
	MustCloseDBNoCheck(tb, db)
}

// MustCloseDBNoCheck closes db. On error, fail test.
func MustCloseDBNoCheck(tb testing.TB, db *rbf.DB) {
	tb.Helper()
	if n := db.TxN(); n != 0 {
		tb.Fatalf("db still has %d active transactions; must closed before closing db", n)
	} else if err := db.Close(); err != nil && err != rbf.ErrClosed {
		tb.Fatal(err)
	} else if err := os.RemoveAll(db.Path); err != nil {
		tb.Fatal(err)
	}
}

// MustReopenDB closes and reopens a database.
func MustReopenDB(tb testing.TB, db *rbf.DB) *rbf.DB {
	tb.Helper()
	if err := db.Check(); err != nil {
		tb.Fatal(err)
	} else if err := db.Close(); err != nil {
		tb.Fatal(err)
	}

	other := rbf.NewDB(db.Path, nil)
	if err := other.Open(); err != nil {
		tb.Fatal(err)
	}
	return other
}

// MustBegin returns a new transaction or fails.
func MustBegin(tb testing.TB, db *rbf.DB, writable bool) *rbf.Tx {
	tb.Helper()
	tx, err := db.Begin(writable)
	if err != nil {
		tb.Fatal(err)
	}
	return tx
}

var _ = MustAddRandom

// MustAddRandom adds values to a bitmap in a random order.
func MustAddRandom(tb testing.TB, rand *rand.Rand, tx *rbf.Tx, name string, values ...uint64) {
	tb.Helper()
	for _, i := range rand.Perm(len(values)) {
		v := values[i]
		if _, err := tx.Add(name, v); err != nil {
			tb.Fatalf("Add(%d) i=%d err=%q", v, i, err)
		}
	}
}

// GenerateValues returns a sorted list of random values.
func GenerateValues(rand *rand.Rand, n int) []uint64 {
	a := make([]uint64, n)
	for i := range a {
		a[i] = uint64(rand.Intn(rbf.ShardWidth))
	}
	sort.Slice(a, func(i, j int) bool { return a[i] < a[j] })
	return a
}

// QuickCheck executes fn multiple times with a different PRNG.
func QuickCheck(t *testing.T, fn func(t *testing.T, rand *rand.Rand)) {
	for i := 0; i < *quickCheckN; i++ {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			fn(t, rand.New(rand.NewSource(int64(i))))
		})
	}
}

func highbits(v uint64) uint64 { return v >> 16 }
func lowbits(v uint64) uint16  { return uint16(v & 0xFFFF) }

// is32Bit returns true if the architecture is 32-bit.
func is32Bit() bool { return runtime.GOARCH == "386" }
