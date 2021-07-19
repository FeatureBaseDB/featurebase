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
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"testing"

	"github.com/molecula/featurebase/v2/rbf"
	rbfcfg "github.com/molecula/featurebase/v2/rbf/cfg"
	"github.com/molecula/featurebase/v2/testhook"
)

var quickCheckN *int = flag.Int("quickchecks", 10, "The number of iterations for each quickcheck")

// Ensure root record helper functions work to read & write root records.
func TestReadWriteRootRecord(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		buf := make([]byte, 26)

		// Write records.
		if remaining, err := rbf.WriteRootRecord(buf, &rbf.RootRecord{Pgno: 10, Name: "foo"}); err != nil {
			t.Fatal(err)
		} else if remaining, err := rbf.WriteRootRecord(remaining, &rbf.RootRecord{Pgno: 11, Name: "bar"}); err != nil {
			t.Fatal(err)
		} else if _, err := rbf.WriteRootRecord(remaining, &rbf.RootRecord{Pgno: 12, Name: "baz"}); err != io.ErrShortBuffer {
			t.Fatalf("unexpected error: %#v", err) // buffer too short
		}

		// Read records back.
		if rec, remaining, err := rbf.ReadRootRecord(buf); err != nil {
			t.Fatal(err)
		} else if got, want := *rec, (rbf.RootRecord{Pgno: 10, Name: "foo"}); got != want {
			t.Fatalf("ReadRootRecord=%#v, want %#v", got, want)
		} else if rec, remaining, err := rbf.ReadRootRecord(remaining); err != nil {
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

	var cfg0 *rbfcfg.Config
	if len(cfg) > 0 {
		cfg0 = cfg[0]
	}
	db := rbf.NewDB(path, cfg0)
	return db
}

// MustOpenDB returns a db opened on a temporary file. On error, fail test.
func MustOpenDB(tb testing.TB, cfg ...*rbfcfg.Config) *rbf.DB {
	tb.Helper()
	db := NewDB(tb, cfg...)
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
	} else if n := db.TxN(); n != 0 {
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

var _ = ToRows

// ToRows returns a sorted list of rows from a set of values.
func ToRows(values []uint64) []*Row {
	m := make(map[uint64][]uint64)
	for _, v := range values {
		id := v / rbf.ShardWidth
		m[id] = append(m[id], v&rbf.RowValueMask)
	}

	a := make([]*Row, 0, len(m))
	for id, values := range m {
		a = append(a, &Row{ID: id, Values: values})
	}
	sort.Slice(a, func(i, j int) bool { return a[i].ID < a[j].ID })
	return a
}

var _ = Row{}

type Row struct {
	ID     uint64
	Values []uint64
}

func (r *Row) Bitmap() []uint64 {
	a := make([]uint64, rbf.ShardWidth/64)
	for _, v := range r.Values {
		a[v/64] |= 1 << (v % 64)
	}
	return a
}

// Union returns the union of r and other's values.
func (r *Row) Union(other *Row) []uint64 {
	m := make(map[uint64]struct{})
	for _, v := range r.Values {
		m[v] = struct{}{}
	}
	for _, v := range other.Values {
		m[v] = struct{}{}
	}

	a := make([]uint64, 0, len(m))
	for v := range m {
		a = append(a, v)
	}
	sort.Slice(a, func(i, j int) bool { return a[i] < a[j] })
	return a
}

// Intersect returns the intersection of r & other's values.
func (r *Row) Intersect(other *Row) []uint64 {
	m := make(map[uint64]struct{})
	for _, v := range r.Values {
		m[v] = struct{}{}
	}

	a := make([]uint64, 0)
	used := make(map[uint64]struct{})
	for _, v := range other.Values {
		if _, ok := used[v]; ok {
			continue
		}
		if _, ok := m[v]; ok {
			used[v] = struct{}{}
			a = append(a, v)
		}
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
