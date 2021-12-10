package pilosa

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/quick"

	"github.com/davecgh/go-spew/spew"
	"github.com/molecula/featurebase/v2/pql"
	"github.com/molecula/featurebase/v2/roaring"
	"github.com/molecula/featurebase/v2/storage"
	"github.com/molecula/featurebase/v2/testhook"
	. "github.com/molecula/featurebase/v2/vprint" // nolint:staticcheck
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

// Test flags
var (
	// In order to generate the sample fragment file,
	// run an import and copy PILOSA_DATA_DIR/INDEX_NAME/FRAME_NAME/0 to testdata/sample_view
	FragmentPath = flag.String("fragment", "testdata/sample_view/0", "fragment path")

	TempDir = flag.String("temp-dir", "", "Directory in which to place temporary data (e.g. for benchmarking). Useful if you are trying to benchmark different storage configurations.")
)

// Ensure a fragment can set a bit and retrieve it.
func TestFragment_SetBit(t *testing.T) {
	f, idx, tx := mustOpenFragment(t, "i", "f", viewStandard, 0, "")
	defer f.Clean(t)

	// Set bits on the fragment.
	if _, err := f.setBit(tx, 120, 1); err != nil {
		t.Fatal(err)
	} else if _, err := f.setBit(tx, 120, 6); err != nil {
		t.Fatal(err)
	} else if _, err := f.setBit(tx, 121, 0); err != nil {
		t.Fatal(err)
	}
	// should have two containers set in the fragment.

	// Verify counts on rows.
	if n := f.mustRow(tx, 120).Count(); n != 2 {
		t.Fatalf("unexpected count: %d", n)
	} else if n := f.mustRow(tx, 121).Count(); n != 1 {
		t.Fatalf("unexpected count: %d", n)
	}

	// commit the change, and verify it is still there
	PanicOn(tx.Commit())

	// Close and reopen the fragment & verify the data.
	err := f.Reopen()
	if err != nil {
		t.Fatal(err)
	}
	tx = idx.holder.txf.NewTx(Txo{Write: writable, Index: idx, Fragment: f, Shard: f.shard})
	defer tx.Rollback()

	if n := f.mustRow(tx, 120).Count(); n != 2 {
		t.Fatalf("unexpected count (reopen): %d", n)
	} else if n := f.mustRow(tx, 121).Count(); n != 1 {
		t.Fatalf("unexpected count (reopen): %d", n)
	}
}

// Ensure a fragment can clear a set bit.
func TestFragment_ClearBit(t *testing.T) {
	f, idx, tx := mustOpenFragment(t, "i", "f", viewStandard, 0, "")
	_ = idx
	defer f.Clean(t)

	// Set and then clear bits on the fragment.
	if _, err := f.setBit(tx, 1000, 1); err != nil {
		t.Fatal(err)
	} else if _, err := f.setBit(tx, 1000, 2); err != nil {
		t.Fatal(err)
	} else if _, err := f.clearBit(tx, 1000, 1); err != nil {
		t.Fatal(err)
	}

	// Verify count on row.
	if n := f.mustRow(tx, 1000).Count(); n != 1 {
		t.Fatalf("unexpected count: %d", n)
	}
	// The Reopen below implies this test is looking at storage consistency.
	// In that spirit, we will check that the Tx Commit is visible afterwards.
	PanicOn(tx.Commit())

	tx = idx.holder.txf.NewTx(Txo{Write: !writable, Index: idx, Fragment: f, Shard: f.shard})
	defer tx.Rollback()

	// Close and reopen the fragment & verify the data.
	if err := f.Reopen(); err != nil {
		t.Fatal(err)
	} else if n := f.mustRow(tx, 1000).Count(); n != 1 {
		t.Fatalf("unexpected count (reopen): %d", n)
	}
}

/* We suspect this test is no longer valid under the new Tx
   framework in which we always copy mmap-ed rows before
   returning them. So we will comment it out for now.
   If someone knows any reason for this to stick around,
   let us know; we couldn't figure out how to adapt
   to do a meaningful test under Tx. - jaten / tgruben

// What about rowcache timing.
func TestFragment_RowcacheMap(t *testing.T) {
	var done int64
	f, _, tx := mustOpenFragment(t, "i", "f", viewStandard, 0, "")

	// Under -race, this test turns out to take a fairly long time
	// to run with larger OpN, because we write 50,000 bits to
	// the bitmap, and everything is being race-detected, and we don't
	// actually need that many to get the result we care about.
	f.MaxOpN = 2000
	defer f.Clean(t) // failing here with     TestFragment_RowcacheMap: fragment_internal_test.go:2859: fragment /var/folders/2x/hm9gp5ys3k9gmm5f_vzm_6wc0000gn/T/pilosa-fragment-001943331: unmarshalled bitmap different: differing containers for key 0: <run container, N=10131, len 2000x interval> vs <run container, N=12000, len 2000x interval>

	ch := make(chan struct{})

	for i := 0; i < f.MaxOpN; i++ {
		_, _ = f.setBit(tx, 0, uint64(i*32))
	}
	// force snapshot so we get a mmapped row...
	_ = f.Snapshot()
	row := f.mustRow(tx, 0)
	tx.Commit(0)
	segment := row.Segments()[0]
	bitmap := segment.data

	// request information from the frozen bitmap we got back
	go func() {
		for atomic.LoadInt64(&done) == 0 {
			for i := 0; i < f.MaxOpN; i++ {
				_ = bitmap.Contains(uint64(i * 32))
			}
		}
		close(ch)
	}()

	// modify the original bitmap, until it causes a snapshot, which
	// then invalidates the other map...
	for j := 0; j < 5; j++ {
		for i := 0; i < f.MaxOpN; i++ {
			_, _ = f.setBit(tx, 0, uint64(i*32+j+1))
		}
	}
	atomic.StoreInt64(&done, 1)
	<-ch
}
*/

// Ensure a fragment can clear a row.
func TestFragment_ClearRow(t *testing.T) {
	f, idx, tx := mustOpenFragment(t, "i", "f", viewStandard, 0, "")
	_ = idx
	defer f.Clean(t)

	// Set and then clear bits on the fragment.
	if _, err := f.setBit(tx, 1000, 1); err != nil {
		t.Fatal(err)
	} else if _, err := f.setBit(tx, 1000, 65536); err != nil {
		t.Fatal(err)
	} else if _, err := f.unprotectedClearRow(tx, 1000); err != nil {
		t.Fatal(err)
	}

	// Verify count on row.
	if n := f.mustRow(tx, 1000).Count(); n != 0 {
		t.Fatalf("unexpected count: %d", n)
	}
	PanicOn(tx.Commit())
	tx = idx.holder.txf.NewTx(Txo{Write: !writable, Index: idx, Fragment: f, Shard: f.shard})
	defer tx.Rollback()

	// Close and reopen the fragment & verify the data.
	if err := f.Reopen(); err != nil {
		t.Fatal(err)
	} else if n := f.mustRow(tx, 1000).Count(); n != 0 {
		t.Fatalf("unexpected count (reopen): %d", n)
	}
}

// Ensure a fragment can set a row.
func TestFragment_SetRow(t *testing.T) {
	f, idx, tx := mustOpenFragment(t, "i", "f", viewStandard, 7, "")
	_ = idx
	defer f.Clean(t)

	// Obtain transction.

	rowID := uint64(1000)

	// Set bits on the fragment.
	if _, err := f.setBit(tx, rowID, 7*ShardWidth+1); err != nil {
		t.Fatal(err)
	} else if _, err := f.setBit(tx, rowID, 7*ShardWidth+65536); err != nil {
		t.Fatal(err)
	}

	// Verify data on row.
	if cols := f.mustRow(tx, rowID).Columns(); !reflect.DeepEqual(cols, []uint64{7*ShardWidth + 1, 7*ShardWidth + 65536}) {
		t.Fatalf("unexpected columns: %+v", cols)
	}
	// Verify count on row.
	if n := f.mustRow(tx, rowID).Count(); n != 2 {
		t.Fatalf("unexpected count: %d", n)
	}

	// Set row (overwrite existing data).
	row := NewRow(7*ShardWidth+1, 7*ShardWidth+65537, 7*ShardWidth+140000)
	if changed, err := f.unprotectedSetRow(tx, row, rowID); err != nil {
		t.Fatal(err)
	} else if !changed {
		t.Fatalf("expected changed value: %v", changed)
	}

	PanicOn(tx.Commit())
	tx = idx.holder.txf.NewTx(Txo{Write: writable, Index: idx, Fragment: f, Shard: f.shard})
	defer tx.Rollback()

	// Verify data on row.
	if cols := f.mustRow(tx, rowID).Columns(); !reflect.DeepEqual(cols, []uint64{7*ShardWidth + 1, 7*ShardWidth + 65537, 7*ShardWidth + 140000}) {
		t.Fatalf("unexpected columns after set row: %+v", cols)
	}
	// Verify count on row.
	if n := f.mustRow(tx, rowID).Count(); n != 3 {
		t.Fatalf("unexpected count after set row: %d", n)
	}

	PanicOn(tx.Commit())
	tx = idx.holder.txf.NewTx(Txo{Write: !writable, Index: idx, Fragment: f, Shard: f.shard})

	// Close and reopen the fragment & verify the data.
	if err := f.Reopen(); err != nil {
		t.Fatal(err)
	} else if n := f.mustRow(tx, rowID).Count(); n != 3 {
		t.Fatalf("unexpected count (reopen): %d", n)
	}

	tx.Rollback()
	tx = idx.holder.txf.NewTx(Txo{Write: writable, Index: idx, Fragment: f, Shard: f.shard})
	defer tx.Rollback()

	// verify that setting something from a row which lacks a segment for
	// this fragment's shard still clears this fragment correctly.
	notOurs := NewRow(8*ShardWidth + 1024)
	if changed, err := f.unprotectedSetRow(tx, notOurs, rowID); err != nil {
		t.Fatal(err)
	} else if !changed {
		t.Fatalf("setRow didn't report a change")
	}
	if cols := f.mustRow(tx, rowID).Columns(); len(cols) != 0 {
		t.Fatalf("expected setting a row with no entries to clear the cache")
	}
	PanicOn(tx.Commit())
}

// Ensure a fragment can set & read a value.
func TestFragment_SetValue(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		f, idx, tx := mustOpenFragment(t, "i", "f", viewStandard, 0, "")
		_ = idx
		defer f.Clean(t)

		// Set value.
		if changed, err := f.setValue(tx, 100, 16, 3829); err != nil {
			t.Fatal(err)
		} else if !changed {
			t.Fatal("expected change")
		}

		// Read value.
		if value, exists, err := f.value(tx, 100, 16); err != nil {
			t.Fatal(err)
		} else if value != 3829 {
			t.Fatalf("unexpected value: %d", value)
		} else if !exists {
			t.Fatal("expected to exist")
		}

		// Setting value should return no change.
		if changed, err := f.setValue(tx, 100, 16, 3829); err != nil {
			t.Fatal(err)
		} else if changed {
			t.Fatal("expected no change")
		}

		// same after Commit
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		tx = idx.holder.txf.NewTx(Txo{Write: writable, Index: idx, Fragment: f, Shard: f.shard})
		defer tx.Rollback()

		// Read value.
		if value, exists, err := f.value(tx, 100, 16); err != nil {
			t.Fatal(err)
		} else if value != 3829 {
			t.Fatalf("unexpected value: %d", value)
		} else if !exists {
			t.Fatal("expected to exist")
		}

		// Setting value should return no change.
		if changed, err := f.setValue(tx, 100, 16, 3829); err != nil {
			t.Fatal(err)
		} else if changed {
			t.Fatal("expected no change")
		}
	})

	t.Run("Overwrite", func(t *testing.T) {
		f, idx, tx := mustOpenFragment(t, "i", "f", viewStandard, 0, "")
		_ = idx
		defer f.Clean(t)

		// Set value.
		if changed, err := f.setValue(tx, 100, 16, 3829); err != nil {
			t.Fatal(err)
		} else if !changed {
			t.Fatal("expected change")
		}

		// Overwriting value should overwrite all bits.
		if changed, err := f.setValue(tx, 100, 16, 2028); err != nil {
			t.Fatal(err)
		} else if !changed {
			t.Fatal("expected change")
		}

		// Read value.
		if value, exists, err := f.value(tx, 100, 16); err != nil {
			t.Fatal(err)
		} else if value != 2028 {
			t.Fatalf("unexpected value: %d", value)
		} else if !exists {
			t.Fatal("expected to exist")
		}

		// Read value after commit.
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		tx = idx.holder.txf.NewTx(Txo{Write: writable, Index: idx, Fragment: f, Shard: f.shard})
		defer tx.Rollback()

		if value, exists, err := f.value(tx, 100, 16); err != nil {
			t.Fatal(err)
		} else if value != 2028 {
			t.Fatalf("unexpected value: %d", value)
		} else if !exists {
			t.Fatal("expected to exist")
		}

	})

	t.Run("Clear", func(t *testing.T) {
		f, idx, tx := mustOpenFragment(t, "i", "f", viewStandard, 0, "")
		_ = idx
		defer f.Clean(t)

		// Set value.
		if changed, err := f.setValue(tx, 100, 16, 3829); err != nil {
			t.Fatal(err)
		} else if !changed {
			t.Fatal("expected change")
		}

		// Clear value should overwrite all bits, and set not-null to 0.
		if changed, err := f.clearValue(tx, 100, 16, 2028); err != nil {
			t.Fatal(err)
		} else if !changed {
			t.Fatal("expected change")
		}

		// Read value.
		if value, exists, err := f.value(tx, 100, 16); err != nil {
			t.Fatal(err)
		} else if value != 0 {
			t.Fatalf("unexpected value: %d", value)
		} else if exists {
			t.Fatal("expected to not exist")
		}

		// Same after Commit
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
		tx = idx.holder.txf.NewTx(Txo{Write: writable, Index: idx, Fragment: f, Shard: f.shard})
		defer tx.Rollback()

		if value, exists, err := f.value(tx, 100, 16); err != nil {
			t.Fatal(err)
		} else if value != 0 {
			t.Fatalf("unexpected value: %d", value)
		} else if exists {
			t.Fatal("expected to not exist")
		}
	})

	t.Run("NotExists", func(t *testing.T) {
		f, idx, tx := mustOpenFragment(t, "i", "f", viewStandard, 0, "")
		_ = idx
		defer f.Clean(t)
		defer tx.Rollback()

		// Set value.
		if changed, err := f.setValue(tx, 100, 10, 20); err != nil {
			t.Fatal(err)
		} else if !changed {
			t.Fatal("expected change")
		}

		// Non-existent value.
		if value, exists, err := f.value(tx, 101, 11); err != nil {
			t.Fatal(err)
		} else if value != 0 {
			t.Fatalf("unexpected value: %d", value)
		} else if exists {
			t.Fatal("expected to not exist")
		}

	})

	t.Run("QuickCheck", func(t *testing.T) {
		if err := quick.Check(func(bitDepth uint64, bitN uint64, values []uint64) bool {
			// Limit bit depth & maximum values.
			bitDepth = (bitDepth % 62) + 1
			bitN = (bitN % 99) + 1

			for i := range values {
				values[i] = values[i] % (1 << bitDepth)
			}

			f, idx, tx := mustOpenFragment(t, "i", "f", viewStandard, 0, "")
			_ = idx
			defer f.Clean(t)

			// Set values.
			m := make(map[uint64]int64)
			for _, value := range values {
				columnID := value % bitN

				m[columnID] = int64(value)

				if _, err := f.setValue(tx, columnID, bitDepth, int64(value)); err != nil {
					t.Fatal(err)
				}
			}

			// Ensure values are set.
			for columnID, value := range m {
				v, exists, err := f.value(tx, columnID, bitDepth)
				if err != nil {
					t.Fatal(err)
				} else if value != int64(v) {
					t.Fatalf("value mismatch: columnID=%d, bitdepth=%d, value: %d != %d", columnID, bitDepth, value, v)
				} else if !exists {
					t.Fatalf("value should exist: columnID=%d", columnID)
				}
			}

			// Same after Commit
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			tx = idx.holder.txf.NewTx(Txo{Write: writable, Index: idx, Fragment: f, Shard: f.shard})
			defer tx.Rollback()

			// Ensure values are set.
			for columnID, value := range m {
				v, exists, err := f.value(tx, columnID, bitDepth)
				if err != nil {
					t.Fatal(err)
				} else if value != int64(v) {
					t.Fatalf("value mismatch: columnID=%d, bitdepth=%d, value: %d != %d", columnID, bitDepth, value, v)
				} else if !exists {
					t.Fatalf("value should exist: columnID=%d", columnID)
				}
			}

			return true
		}, nil); err != nil {
			t.Fatal(err)
		}
	})
}

// Ensure a fragment can sum values.
func TestFragment_Sum(t *testing.T) {
	const bitDepth = 16

	f, idx, tx := mustOpenFragment(t, "i", "f", viewStandard, 0, "")
	defer f.Clean(t)

	// Set values.
	vals := []struct {
		cid uint64
		val int64
	}{
		{1000, 382},
		{2000, 300},
		{2500, -600},
		{3000, 2818},
		{4000, 300},
	}
	for _, v := range vals {
		if _, err := f.setValue(tx, v.cid, bitDepth, v.val); err != nil {
			t.Fatal(err)
		}
	}

	PanicOn(tx.Commit())
	tx = idx.holder.txf.NewTx(Txo{Write: !writable, Index: idx, Fragment: f, Shard: f.shard})
	defer tx.Rollback()

	t.Run("NoFilter", func(t *testing.T) {
		if sum, n, err := f.sum(tx, nil, bitDepth); err != nil {
			t.Fatal(err)
		} else if n != 5 {
			t.Fatalf("unexpected count: %d", n)
		} else if got, exp := sum, int64(382+300-600+2818+300); got != exp {
			t.Fatalf("unexpected sum: got: %d, exp: %d", sum, exp)
		}
	})

	t.Run("WithFilter", func(t *testing.T) {
		if sum, n, err := f.sum(tx, NewRow(2000, 4000, 5000), bitDepth); err != nil {
			t.Fatal(err)
		} else if n != 2 {
			t.Fatalf("unexpected count: %d", n)
		} else if got, exp := sum, int64(300+300); got != exp {
			t.Fatalf("unexpected sum: got: %d, exp: %d", sum, exp)
		}
	})

	tx.Rollback()
	tx = idx.holder.txf.NewTx(Txo{Write: writable, Index: idx, Fragment: f, Shard: f.shard})
	defer tx.Rollback()

	// verify that clearValue clears values
	if _, err := f.clearValue(tx, 1000, bitDepth, 23); err != nil {
		t.Fatal(err)
	}

	PanicOn(tx.Commit())
	tx = idx.holder.txf.NewTx(Txo{Write: !writable, Index: idx, Fragment: f, Shard: f.shard})
	defer tx.Rollback()

	t.Run("ClearValue", func(t *testing.T) {
		if sum, n, err := f.sum(tx, nil, bitDepth); err != nil {
			t.Fatal(err)
		} else if n != 4 {
			t.Fatalf("unexpected count: %d", n)
		} else if got, exp := sum, int64(3800-382-600); got != exp {
			t.Fatalf("unexpected sum: got: %d, exp: %d", sum, exp)
		}
	})
}

// Ensure a fragment can find the min and max of values.
func TestFragment_MinMax(t *testing.T) {
	const bitDepth = 16

	f, idx, tx := mustOpenFragment(t, "i", "f", viewStandard, 0, "")
	defer f.Clean(t)

	// Set values.
	if _, err := f.setValue(tx, 1000, bitDepth, 382); err != nil {
		t.Fatal(err)
	} else if _, err := f.setValue(tx, 2000, bitDepth, 300); err != nil {
		t.Fatal(err)
	} else if _, err := f.setValue(tx, 3000, bitDepth, 2818); err != nil {
		t.Fatal(err)
	} else if _, err := f.setValue(tx, 4000, bitDepth, 300); err != nil {
		t.Fatal(err)
	} else if _, err := f.setValue(tx, 5000, bitDepth, 2818); err != nil {
		t.Fatal(err)
	} else if _, err := f.setValue(tx, 6000, bitDepth, 2817); err != nil {
		t.Fatal(err)
	} else if _, err := f.setValue(tx, 7000, bitDepth, 0); err != nil {
		t.Fatal(err)
	}

	PanicOn(tx.Commit())

	// the new tx is shared by Min/Max below.
	tx = idx.holder.txf.NewTx(Txo{Write: !writable, Index: idx, Fragment: f, Shard: f.shard})
	defer tx.Rollback()

	t.Run("Min", func(t *testing.T) {
		tests := []struct {
			filter *Row
			exp    int64
			cnt    uint64
		}{
			{filter: nil, exp: 0, cnt: 1},
			{filter: NewRow(2000, 4000, 5000), exp: 300, cnt: 2},
			{filter: NewRow(2000, 4000), exp: 300, cnt: 2},
			{filter: NewRow(1), exp: 0, cnt: 0},
			{filter: NewRow(1000), exp: 382, cnt: 1},
			{filter: NewRow(7000), exp: 0, cnt: 1},
		}
		for i, test := range tests {
			if min, cnt, err := f.min(tx, test.filter, bitDepth); err != nil {
				t.Fatal(err)
			} else if min != test.exp {
				t.Errorf("test %d expected min: %v, but got: %v", i, test.exp, min)
			} else if cnt != test.cnt {
				t.Errorf("test %d expected cnt: %v, but got: %v", i, test.cnt, cnt)
			}
		}
	})

	t.Run("Max", func(t *testing.T) {
		tests := []struct {
			filter *Row
			exp    int64
			cnt    uint64
		}{
			{filter: nil, exp: 2818, cnt: 2},
			{filter: NewRow(2000, 4000, 5000), exp: 2818, cnt: 1},
			{filter: NewRow(2000, 4000), exp: 300, cnt: 2},
			{filter: NewRow(1), exp: 0, cnt: 0},
			{filter: NewRow(1000), exp: 382, cnt: 1},
			{filter: NewRow(7000), exp: 0, cnt: 1},
		}
		for i, test := range tests {
			var columns []uint64
			if test.filter != nil {
				columns = test.filter.Columns()
			}

			if max, cnt, err := f.max(tx, test.filter, bitDepth); err != nil {
				t.Fatal(err)
			} else if max != test.exp || cnt != test.cnt {
				t.Errorf("%d. max(%v, %v)=(%v, %v), expected (%v, %v)", i, columns, bitDepth, max, cnt, test.exp, test.cnt)
			}
		}
	})
}

// Ensure a fragment query for matching values.
func TestFragment_Range(t *testing.T) {
	const bitDepth = 16

	t.Run("EQ", func(t *testing.T) {
		f, idx, tx := mustOpenFragment(t, "i", "f", viewStandard, 0, "")
		_ = idx
		defer f.Clean(t)

		// Set values.
		if _, err := f.setValue(tx, 1000, bitDepth, 382); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(tx, 2000, bitDepth, 300); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(tx, 3000, bitDepth, 2818); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(tx, 4000, bitDepth, 300); err != nil {
			t.Fatal(err)
		}

		// Query for equality.
		if b, err := f.rangeOp(tx, pql.EQ, bitDepth, 300); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(b.Columns(), []uint64{2000, 4000}) {
			t.Fatalf("unexpected columns: %+v", b.Columns())
		}
	})

	t.Run("EQOversizeRegression", func(t *testing.T) {
		f, idx, tx := mustOpenFragment(t, "i", "f", viewStandard, 0, "")
		_ = idx
		defer f.Clean(t)

		// Set values.
		if _, err := f.setValue(tx, 1000, 1, 0); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(tx, 2000, 1, 1); err != nil {
			t.Fatal(err)
		}

		// Query for equality.
		if b, err := f.rangeOp(tx, pql.EQ, 1, 3); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(b.Columns(), []uint64{}) {
			t.Fatalf("unexpected columns: %+v", b.Columns())
		}
		if b, err := f.rangeOp(tx, pql.EQ, 1, 4); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(b.Columns(), []uint64{}) {
			t.Fatalf("unexpected columns: %+v", b.Columns())
		}
	})

	t.Run("NEQ", func(t *testing.T) {
		f, idx, tx := mustOpenFragment(t, "i", "f", viewStandard, 0, "")
		_ = idx
		defer f.Clean(t)

		// Set values.
		if _, err := f.setValue(tx, 1000, bitDepth, 382); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(tx, 2000, bitDepth, 300); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(tx, 3000, bitDepth, 2818); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(tx, 4000, bitDepth, 300); err != nil {
			t.Fatal(err)
		}

		// Query for inequality.
		if b, err := f.rangeOp(tx, pql.NEQ, bitDepth, 300); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(b.Columns(), []uint64{1000, 3000}) {
			t.Fatalf("unexpected columns: %+v", b.Columns())
		}
	})

	t.Run("LT", func(t *testing.T) {
		f, idx, tx := mustOpenFragment(t, "i", "f", viewStandard, 0, "")
		_ = idx
		defer f.Clean(t)

		// Set values.
		if _, err := f.setValue(tx, 1000, bitDepth, 382); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(tx, 2000, bitDepth, 300); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(tx, 3000, bitDepth, 2817); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(tx, 4000, bitDepth, 301); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(tx, 5000, bitDepth, 1); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(tx, 6000, bitDepth, 0); err != nil {
			t.Fatal(err)
		}

		// Query for values less than (ending with set column).
		if b, err := f.rangeOp(tx, pql.LT, bitDepth, 301); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(b.Columns(), []uint64{2000, 5000, 6000}) {
			t.Fatalf("unexpected columns: %+v", b.Columns())
		}

		// Query for values less than (ending with unset column).
		if b, err := f.rangeOp(tx, pql.LT, bitDepth, 300); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(b.Columns(), []uint64{5000, 6000}) {
			t.Fatalf("unexpected columns: %+v", b.Columns())
		}

		// Query for values less than or equal to (ending with set column).
		if b, err := f.rangeOp(tx, pql.LTE, bitDepth, 301); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(b.Columns(), []uint64{2000, 4000, 5000, 6000}) {
			t.Fatalf("unexpected columns: %+v", b.Columns())
		}

		// Query for values less than or equal to (ending with unset column).
		if b, err := f.rangeOp(tx, pql.LTE, bitDepth, 300); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(b.Columns(), []uint64{2000, 5000, 6000}) {
			t.Fatalf("unexpected columns: %+v", b.Columns())
		}
	})

	t.Run("LTRegression", func(t *testing.T) {
		f, idx, tx := mustOpenFragment(t, "i", "f", viewStandard, 0, "")
		_ = idx
		defer f.Clean(t)

		if _, err := f.setValue(tx, 1, 1, 1); err != nil {
			t.Fatal(err)
		}

		if b, err := f.rangeOp(tx, pql.LT, 1, 2); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(b.Columns(), []uint64{1}) {
			t.Fatalf("unepxected coulmns: %+v", b.Columns())
		}
	})

	t.Run("LTMaxRegression", func(t *testing.T) {
		f, idx, tx := mustOpenFragment(t, "i", "f", viewStandard, 0, "")
		_ = idx
		defer f.Clean(t)

		if _, err := f.setValue(tx, 1, 2, 3); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(tx, 2, 2, 0); err != nil {
			t.Fatal(err)
		}

		if b, err := f.rangeLTUnsigned(tx, NewRow(1, 2), 2, 3, false); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(b.Columns(), []uint64{2}) {
			t.Fatalf("unepxected coulmns: %+v", b.Columns())
		}
	})

	t.Run("GT", func(t *testing.T) {
		f, idx, tx := mustOpenFragment(t, "i", "f", viewStandard, 0, "")
		_ = idx
		defer f.Clean(t)

		// Set values.
		if _, err := f.setValue(tx, 1000, bitDepth, 382); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(tx, 2000, bitDepth, 300); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(tx, 3000, bitDepth, 2817); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(tx, 4000, bitDepth, 301); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(tx, 5000, bitDepth, 1); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(tx, 6000, bitDepth, 0); err != nil {
			t.Fatal(err)
		}

		// Query for values greater than (ending with unset bit).
		if b, err := f.rangeOp(tx, pql.GT, bitDepth, 300); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(b.Columns(), []uint64{1000, 3000, 4000}) {
			t.Fatalf("unexpected columns: %+v", b.Columns())
		}

		// Query for values greater than (ending with set bit).
		if b, err := f.rangeOp(tx, pql.GT, bitDepth, 301); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(b.Columns(), []uint64{1000, 3000}) {
			t.Fatalf("unexpected columns: %+v", b.Columns())
		}

		// Query for values greater than or equal to (ending with unset bit).
		if b, err := f.rangeOp(tx, pql.GTE, bitDepth, 300); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(b.Columns(), []uint64{1000, 2000, 3000, 4000}) {
			t.Fatalf("unexpected columns: %+v", b.Columns())
		}

		// Query for values greater than or equal to (ending with set bit).
		if b, err := f.rangeOp(tx, pql.GTE, bitDepth, 301); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(b.Columns(), []uint64{1000, 3000, 4000}) {
			t.Fatalf("unexpected columns: %+v", b.Columns())
		}
	})

	t.Run("GTMinRegression", func(t *testing.T) {
		f, idx, tx := mustOpenFragment(t, "i", "f", viewStandard, 0, "")
		_ = idx
		defer f.Clean(t)

		if _, err := f.setValue(tx, 1, 2, 0); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(tx, 2, 2, 1); err != nil {
			t.Fatal(err)
		}

		if b, err := f.rangeGTUnsigned(tx, NewRow(1, 2), 2, 0, false); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(b.Columns(), []uint64{2}) {
			t.Fatalf("unepxected coulmns: %+v", b.Columns())
		}
	})

	t.Run("GTOversizeRegression", func(t *testing.T) {
		f, idx, tx := mustOpenFragment(t, "i", "f", viewStandard, 0, "")
		_ = idx
		defer f.Clean(t)

		if _, err := f.setValue(tx, 1, 2, 0); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(tx, 2, 2, 1); err != nil {
			t.Fatal(err)
		}

		if b, err := f.rangeGTUnsigned(tx, NewRow(1, 2), 2, 4, false); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(b.Columns(), []uint64{}) {
			t.Fatalf("unepxected coulmns: %+v", b.Columns())
		}
	})

	t.Run("BETWEEN", func(t *testing.T) {
		f, idx, tx := mustOpenFragment(t, "i", "f", viewStandard, 0, "")
		_ = idx
		defer f.Clean(t)

		// Set values.
		if _, err := f.setValue(tx, 1000, bitDepth, 382); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(tx, 2000, bitDepth, 300); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(tx, 3000, bitDepth, 2817); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(tx, 4000, bitDepth, 301); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(tx, 5000, bitDepth, 1); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(tx, 6000, bitDepth, 0); err != nil {
			t.Fatal(err)
		}

		// Query for values greater than (ending with unset column).
		if b, err := f.rangeBetween(tx, bitDepth, 300, 2817); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(b.Columns(), []uint64{1000, 2000, 3000, 4000}) {
			t.Fatalf("unexpected columns: %+v", b.Columns())
		}

		// Query for values greater than (ending with set column).
		if b, err := f.rangeBetween(tx, bitDepth, 301, 2817); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(b.Columns(), []uint64{1000, 3000, 4000}) {
			t.Fatalf("unexpected columns: %+v", b.Columns())
		}

		// Query for values greater than or equal to (ending with unset column).
		if b, err := f.rangeBetween(tx, bitDepth, 301, 2816); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(b.Columns(), []uint64{1000, 4000}) {
			t.Fatalf("unexpected columns: %+v", b.Columns())
		}

		// Query for values greater than or equal to (ending with set column).
		if b, err := f.rangeBetween(tx, bitDepth, 300, 2816); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(b.Columns(), []uint64{1000, 2000, 4000}) {
			t.Fatalf("unexpected columns: %+v", b.Columns())
		}
	})

	t.Run("BetweenCommonBitsRegression", func(t *testing.T) {
		f, idx, tx := mustOpenFragment(t, "i", "f", viewStandard, 0, "")
		_ = idx
		defer f.Clean(t)

		if _, err := f.setValue(tx, 1, 64, 0xf0); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(tx, 2, 64, 0xf1); err != nil {
			t.Fatal(err)
		}

		if b, err := f.rangeBetweenUnsigned(tx, NewRow(1, 2), 64, 0xf0, 0xf1); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(b.Columns(), []uint64{1, 2}) {
			t.Fatalf("unepxected coulmns: %+v", b.Columns())
		}
	})
}

// benchmarkSetValues is a helper function to explore, very roughly, the cost
// of setting values.
func benchmarkSetValues(b *testing.B, tx Tx, bitDepth uint64, f *fragment, cfunc func(uint64) uint64) {
	column := uint64(0)
	for i := 0; i < b.N; i++ {
		// We're not checking the error because this is a benchmark.
		// That does mean the result could be completely wrong...
		_, _ = f.setValue(tx, column, bitDepth, int64(i))
		column = cfunc(column)
	}
}

// Benchmark performance of setValue for BSI ranges.
func BenchmarkFragment_SetValue(b *testing.B) {
	depths := []uint64{4, 8, 16}
	for _, bitDepth := range depths {
		name := fmt.Sprintf("Depth%d", bitDepth)
		f, idx, tx := mustOpenFragment(b, "i", "f", viewBSIGroupPrefix+"foo", 0, "none")
		_ = idx

		b.Run(name+"_Sparse", func(b *testing.B) {
			benchmarkSetValues(b, tx, bitDepth, f, func(u uint64) uint64 { return (u + 70000) & (ShardWidth - 1) })
		})
		f.Clean(b)
		f, idx, tx = mustOpenFragment(b, "i", "f", viewBSIGroupPrefix+"foo", 0, "none")
		_ = idx
		b.Run(name+"_Dense", func(b *testing.B) {
			benchmarkSetValues(b, tx, bitDepth, f, func(u uint64) uint64 { return (u + 1) & (ShardWidth - 1) })
		})
		f.Clean(b)
	}
}

// makeBenchmarkImportValueData produces data that's supposed to be all within
// the same shard; for fragment purposes, implicitly shard 0. This also gets
// used by the field tests, but import requests are supposed to be per-shard,
// so it's important that we generate values only within a given shard.
func makeBenchmarkImportValueData(b *testing.B, bitDepth uint64, cfunc func(uint64) uint64) []ImportValueRequest {
	b.StopTimer()
	column := uint64(0)
	// we don't average an alloc-per-bit, so we use a much larger N to get
	// meaningful data from -benchmem
	n := b.N * 10000
	batches := make([]ImportValueRequest, 0, (n/ShardWidth)+1)
	mask := int64(1<<bitDepth) - 1
	prev := uint64(0)
	var values []int64
	var columns []uint64
	for i := 0; i < n; i++ {
		values = append(values, int64(i)&mask)
		columns = append(columns, column)
		column = cfunc(column)
		if column < prev {
			req := ImportValueRequest{ColumnIDs: columns, Values: values}
			columns = []uint64{}
			values = []int64{}
			batches = append(batches, req)
		}
		prev = column
	}
	if len(columns) > 0 {
		req := ImportValueRequest{ColumnIDs: columns, Values: values}
		batches = append(batches, req)
	}
	b.StartTimer()
	return batches
}

// benchmarkImportValues is a helper function to explore, very roughly, the cost
// of setting values using the special setter used for imports.
func benchmarkImportValues(b *testing.B, tx Tx, bitDepth uint64, f *fragment, cfunc func(uint64) uint64) {
	batches := makeBenchmarkImportValueData(b, bitDepth, cfunc)
	for _, req := range batches {
		err := f.importValue(tx, req.ColumnIDs, req.Values, bitDepth, false)
		if err != nil {
			b.Fatalf("error importing values: %s", err)
		}
	}
}

// Benchmark performance of setValue for BSI ranges.
func BenchmarkFragment_ImportValue(b *testing.B) {
	depths := []uint64{4, 8, 16, 32}
	for _, bitDepth := range depths {
		name := fmt.Sprintf("Depth%d", bitDepth)
		f, idx, tx := mustOpenBSIFragment(b, "i", "f", viewBSIGroupPrefix+"foo", 0)
		_ = idx
		b.Run(name+"_Sparse", func(b *testing.B) {
			benchmarkImportValues(b, tx, bitDepth, f, func(u uint64) uint64 { return (u + 19) & (ShardWidth - 1) })
		})
		f.Clean(b)
		f, idx, tx = mustOpenBSIFragment(b, "i", "f", viewBSIGroupPrefix+"foo", 0)
		_ = idx
		b.Run(name+"_Dense", func(b *testing.B) {
			benchmarkImportValues(b, tx, bitDepth, f, func(u uint64) uint64 { return (u + 1) & (ShardWidth - 1) })
		})
		f.Clean(b)
	}
}

// BenchmarkFragment_RepeatedSmallImports tests the situation where updates are
// constantly coming in across a large column space so that each fragment gets
// only a few updates during each import.
//
// We test a variety of combinations of the number of separate updates(imports),
// the number of bits in the import, the number of rows in the fragment (which
// is a pretty good proxy for fragment size on disk), and the MaxOpN on the
// fragment which controls how many set bits occur before a snapshot is done. If
// the number of bits in a given import is greater than MaxOpN, bulkImport will
// always go through the standard snapshotting import path.
func BenchmarkFragment_RepeatedSmallImports(b *testing.B) {
	for _, numUpdates := range []int{100} {
		for _, bitsPerUpdate := range []int{100, 1000} {
			for _, numRows := range []int{1000, 100000, 1000000} {
				for _, opN := range []int{1, 5000, 50000} {
					b.Run(fmt.Sprintf("Rows%dUpdates%dBits%dOpN%d", numRows, numUpdates, bitsPerUpdate, opN), func(b *testing.B) {
						for a := 0; a < b.N; a++ {
							b.StopTimer()
							// build the update data set all at once - this will get applied
							// to a fragment in numUpdates batches
							updateRows := make([]uint64, numUpdates*bitsPerUpdate)
							updateCols := make([]uint64, numUpdates*bitsPerUpdate)
							for i := 0; i < numUpdates*bitsPerUpdate; i++ {
								updateRows[i] = uint64(rand.Int63n(int64(numRows))) // row id
								updateCols[i] = uint64(rand.Int63n(ShardWidth))     // column id
							}
							f, idx, tx := mustOpenFragment(b, "i", "f", viewStandard, 0, "")
							_ = idx
							f.MaxOpN = opN
							defer f.Clean(b)

							err := f.importRoaringT(tx, getZipfRowsSliceRoaring(uint64(numRows), 1, 0, ShardWidth), false)
							if err != nil {
								b.Fatalf("importing base data for benchmark: %v", err)
							}
							b.StartTimer()
							for i := 0; i < numUpdates; i++ {
								err := f.bulkImportStandard(tx,
									updateRows[bitsPerUpdate*i:bitsPerUpdate*(i+1)],
									updateRows[bitsPerUpdate*i:bitsPerUpdate*(i+1)],
									&ImportOptions{},
								)
								if err != nil {
									b.Fatalf("doing small bulk import: %v", err)
								}
							}
							tx.Rollback() // don't exhaust the Tx space under b.N iterations.
						}
					})
				}
			}
		}
	}
}

func BenchmarkFragment_RepeatedSmallImportsRoaring(b *testing.B) {
	for _, numUpdates := range []int{100} {
		for _, bitsPerUpdate := range []uint64{100, 1000} {
			for _, numRows := range []uint64{1000, 100000, 1000000} {
				for _, opN := range []int{1, 5000, 50000} {
					b.Run(fmt.Sprintf("Rows%dUpdates%dBits%dOpN%d", numRows, numUpdates, bitsPerUpdate, opN), func(b *testing.B) {
						for a := 0; a < b.N; a++ {
							b.StopTimer()
							// build the update data set all at once - this will get applied
							// to a fragment in numUpdates batches
							f, idx, tx := mustOpenFragment(b, "i", "f", viewStandard, 0, "")
							_ = idx
							f.MaxOpN = opN
							defer f.Clean(b)

							err := f.importRoaringT(tx, getZipfRowsSliceRoaring(numRows, 1, 0, ShardWidth), false)
							if err != nil {
								b.Fatalf("importing base data for benchmark: %v", err)
							}
							for i := 0; i < numUpdates; i++ {
								data := getUpdataRoaring(numRows, bitsPerUpdate, int64(i))
								b.StartTimer()
								err := f.importRoaringT(tx, data, false)
								b.StopTimer()
								if err != nil {
									b.Fatalf("doing small roaring import: %v", err)
								}
							}
						}
					})
				}
			}
		}
	}
}

func BenchmarkFragment_RepeatedSmallValueImports(b *testing.B) {
	initialCols := make([]uint64, 0, ShardWidth)
	initialVals := make([]int64, 0, ShardWidth)
	for i := uint64(0); i < ShardWidth; i++ {
		// every 29 columns, skip between 0 and 12 columns
		if i%29 == 0 {
			i += i % 13
		}
		initialCols = append(initialCols, i)
		initialVals = append(initialVals, int64(rand.Int63n(1<<21)))
	}

	for _, numUpdates := range []int{100} {
		for _, valsPerUpdate := range []int{10, 100} {
			updateCols := make([]uint64, numUpdates*valsPerUpdate)
			updateVals := make([]int64, numUpdates*valsPerUpdate)
			for i := 0; i < numUpdates*valsPerUpdate; i++ {
				updateCols[i] = uint64(rand.Int63n(ShardWidth))
				updateVals[i] = int64(rand.Int63n(1 << 21))
			}

			for _, opN := range []int{1, 5000, 50000} {
				b.Run(fmt.Sprintf("Updates%dVals%dOpN%d", numUpdates, valsPerUpdate, opN), func(b *testing.B) {
					for i := 0; i < b.N; i++ {
						b.StopTimer()
						f, _, tx := mustOpenBSIFragment(b, "i", "f", viewBSIGroupPrefix+"foo", 0)
						f.MaxOpN = opN

						err := f.importValue(tx, initialCols, initialVals, 21, false)
						if err != nil {
							b.Fatalf("initial value import: %v", err)
						}
						b.StartTimer()
						for j := 0; j < numUpdates; j++ {
							err := f.importValue(tx,
								updateCols[valsPerUpdate*j:valsPerUpdate*(j+1)],
								updateVals[valsPerUpdate*j:valsPerUpdate*(j+1)],
								21,
								false,
							)
							if err != nil {
								b.Fatalf("importing values: %v", err)
							}
						}
						tx.Rollback() // don't exhaust the Tx over the b.N iterations.
					}
				})
			}

		}
	}
}

// Ensure a fragment can snapshot correctly.
func TestFragment_Snapshot(t *testing.T) {
	f, idx, tx := mustOpenFragment(t, "i", "f", viewStandard, 0, "")
	defer f.Clean(t)

	// Set and then clear bits on the fragment.
	if _, err := f.setBit(tx, 1000, 1); err != nil {
		t.Fatal(err)
	} else if _, err := f.setBit(tx, 1000, 2); err != nil {
		t.Fatal(err)
	} else if _, err := f.clearBit(tx, 1000, 1); err != nil {
		t.Fatal(err)
	}
	PanicOn(tx.Commit())
	tx = idx.holder.txf.NewTx(Txo{Write: !writable, Index: idx, Fragment: f, Shard: f.shard})
	defer tx.Rollback()

	// Snapshot bitmap and verify data.
	if err := f.Snapshot(); err != nil {
		t.Fatal(err)
	} else if n := f.mustRow(tx, 1000).Count(); n != 1 {
		t.Fatalf("unexpected count: %d", n)
	}

	// Close and reopen the fragment & verify the data.
	if err := f.Reopen(); err != nil {
		t.Fatal(err)
	} else if n := f.mustRow(tx, 1000).Count(); n != 1 {
		t.Fatalf("unexpected count (reopen): %d", n)
	}
}

// Ensure a fragment can iterate over all bits in order.
func TestFragment_ForEachBit(t *testing.T) {
	f, idx, tx := mustOpenFragment(t, "i", "f", viewStandard, 0, "")
	_ = idx
	defer f.Clean(t)

	// Set bits on the fragment.
	if _, err := f.setBit(tx, 100, 20); err != nil {
		t.Fatal(err)
	} else if _, err := f.setBit(tx, 2, 38); err != nil {
		t.Fatal(err)
	} else if _, err := f.setBit(tx, 2, 37); err != nil {
		t.Fatal(err)
	}

	// Iterate over bits.
	var result [][2]uint64
	if err := f.forEachBit(tx, func(rowID, columnID uint64) error {
		result = append(result, [2]uint64{rowID, columnID})
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// Verify bits are correct.
	if !reflect.DeepEqual(result, [][2]uint64{{2, 37}, {2, 38}, {100, 20}}) {
		t.Fatalf("unexpected result: %#v", result)
	}
}

// Ensure a fragment can return the top n results.
func TestFragment_Top(t *testing.T) {
	f, idx, tx := mustOpenFragment(t, "i", "f", viewStandard, 0, CacheTypeRanked)
	_ = idx
	defer f.Clean(t)

	// Set bits on the rows 100, 101, & 102.
	f.mustSetBits(tx, 100, 1, 3, 200)
	f.mustSetBits(tx, 101, 1)
	f.mustSetBits(tx, 102, 1, 2)
	f.RecalculateCache()

	// Retrieve top rows.
	if pairs, err := f.top(tx, topOptions{N: 2}); err != nil {
		t.Fatal(err)
	} else if len(pairs) != 2 {
		t.Fatalf("unexpected count: %d", len(pairs))
	} else if pairs[0] != (Pair{ID: 100, Count: 3}) {
		t.Fatalf("unexpected pair(0): %v", pairs[0])
	} else if pairs[1] != (Pair{ID: 102, Count: 2}) {
		t.Fatalf("unexpected pair(1): %v", pairs[1])
	}
}

// Ensure a fragment can return top rows that intersect with an input row.
func TestFragment_TopN_Intersect(t *testing.T) {
	f, idx, tx := mustOpenFragment(t, "i", "f", viewStandard, 0, CacheTypeRanked)
	_ = idx
	defer f.Clean(t)

	// Create an intersecting input row.
	src := NewRow(1, 2, 3)

	// Set bits on various rows.
	f.mustSetBits(tx, 100, 1, 10, 11, 12)    // one intersection
	f.mustSetBits(tx, 101, 1, 2, 3, 4)       // three intersections
	f.mustSetBits(tx, 102, 1, 2, 4, 5, 6)    // two intersections
	f.mustSetBits(tx, 103, 1000, 1001, 1002) // no intersection
	f.RecalculateCache()

	// Retrieve top rows.
	if pairs, err := f.top(tx, topOptions{N: 3, Src: src}); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(pairs, []Pair{
		{ID: 101, Count: 3},
		{ID: 102, Count: 2},
		{ID: 100, Count: 1},
	}) {
		t.Fatalf("unexpected pairs: %s", spew.Sdump(pairs))
	}
}

// Ensure a fragment can return top rows that have many columns set.
func TestFragment_TopN_Intersect_Large(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}

	f, idx, tx := mustOpenFragment(t, "i", "f", viewStandard, 0, CacheTypeRanked)
	_ = idx
	defer f.Clean(t)

	// Create an intersecting input row.
	src := NewRow(
		980, 981, 982, 983, 984, 985, 986, 987, 988, 989,
		990, 991, 992, 993, 994, 995, 996, 997, 998, 999,
	)

	bm := roaring.NewBTreeBitmap()
	// Set bits on rows 0 - 999. Higher rows have higher bit counts.
	for i := uint64(0); i < 1000; i++ {
		for j := uint64(0); j < i; j++ {
			addToBitmap(bm, i, j)
		}
	}
	b := &bytes.Buffer{}
	_, err := bm.WriteTo(b)
	if err != nil {
		t.Fatalf("writing to bytes: %v", err)
	}
	err = f.importRoaringT(tx, b.Bytes(), false)
	if err != nil {
		t.Fatalf("importing data: %v", err)
	}
	f.RecalculateCache()

	// Retrieve top rows.
	if pairs, err := f.top(tx, topOptions{N: 10, Src: src}); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(pairs, []Pair{
		{ID: 999, Count: 19},
		{ID: 998, Count: 18},
		{ID: 997, Count: 17},
		{ID: 996, Count: 16},
		{ID: 995, Count: 15},
		{ID: 994, Count: 14},
		{ID: 993, Count: 13},
		{ID: 992, Count: 12},
		{ID: 991, Count: 11},
		{ID: 990, Count: 10},
	}) {
		t.Fatalf("unexpected pairs: %s", spew.Sdump(pairs))
	}
}

// Ensure a fragment can return top rows when specified by ID.
func TestFragment_TopN_IDs(t *testing.T) {
	f, idx, tx := mustOpenFragment(t, "i", "f", viewStandard, 0, CacheTypeRanked)
	_ = idx
	defer f.Clean(t)

	// Set bits on various rows.
	f.mustSetBits(tx, 100, 1, 2, 3)
	f.mustSetBits(tx, 101, 4, 5, 6, 7)
	f.mustSetBits(tx, 102, 8, 9, 10, 11, 12)

	// Retrieve top rows.
	if pairs, err := f.top(tx, topOptions{RowIDs: []uint64{100, 101, 200}}); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(pairs, []Pair{
		{ID: 101, Count: 4},
		{ID: 100, Count: 3},
	}) {
		t.Fatalf("unexpected pairs: %s", spew.Sdump(pairs))
	}
}

// Ensure a fragment return none if CacheTypeNone is set
func TestFragment_TopN_NopCache(t *testing.T) {
	f, idx, tx := mustOpenFragment(t, "i", "f", viewStandard, 0, CacheTypeNone)
	_ = idx
	defer f.Clean(t)

	// Set bits on various rows.
	f.mustSetBits(tx, 100, 1, 2, 3)
	f.mustSetBits(tx, 101, 4, 5, 6, 7)
	f.mustSetBits(tx, 102, 8, 9, 10, 11, 12)

	// Retrieve top rows.
	if pairs, err := f.top(tx, topOptions{RowIDs: []uint64{100, 101, 200}}); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(pairs, []Pair{}) {
		t.Fatalf("unexpected pairs: %s", spew.Sdump(pairs))
	}
}

// Ensure the fragment cache limit works
func TestFragment_TopN_CacheSize(t *testing.T) {
	shard := uint64(0)
	cacheSize := uint32(3)

	// Create Index.
	index := mustOpenIndex(t, IndexOptions{})
	defer index.Close()

	// Create field.
	field, err := index.CreateFieldIfNotExists("f", OptFieldTypeSet(CacheTypeRanked, cacheSize))
	if err != nil {
		t.Fatal(err)
	}

	// Create view.
	view, err := field.createViewIfNotExists(viewStandard)
	if err != nil {
		t.Fatal(err)
	}

	// Create fragment.
	frag, err := view.CreateFragmentIfNotExists(shard)
	if err != nil {
		t.Fatal(err)
	}
	// Close the storage so we can re-open it without encountering a flock.
	frag.Close()

	f := frag
	if err := f.Open(); err != nil {
		PanicOn(err)
	}
	defer f.Clean(t)

	// Obtain transaction.
	tx := index.holder.txf.NewTx(Txo{Write: writable, Index: index, Fragment: f, Shard: f.shard})
	defer tx.Rollback()

	// Set bits on various rows.
	f.mustSetBits(tx, 100, 1, 2, 3)
	f.mustSetBits(tx, 101, 4, 5, 6, 7)
	f.mustSetBits(tx, 102, 8, 9, 10, 11, 12)
	f.mustSetBits(tx, 103, 8, 9, 10, 11, 12, 13)
	f.mustSetBits(tx, 104, 8, 9, 10, 11, 12, 13, 14)
	f.mustSetBits(tx, 105, 10, 11)

	f.RecalculateCache()

	p := []Pair{
		{ID: 104, Count: 7},
		{ID: 103, Count: 6},
		{ID: 102, Count: 5},
	}

	// Retrieve top rows.
	if pairs, err := f.top(tx, topOptions{N: 5}); err != nil {
		t.Fatal(err)
	} else if len(pairs) > int(cacheSize) {
		t.Fatalf("TopN count cannot exceed cache size: %d", cacheSize)
	} else if pairs[0] != (Pair{ID: 104, Count: 7}) {
		t.Fatalf("unexpected pair(0): %v", pairs)
	} else if !reflect.DeepEqual(pairs, p) {
		t.Fatalf("Invalid TopN result set: %s", spew.Sdump(pairs))
	}
}

// Ensure fragment can return a checksum for its blocks.
func TestFragment_Checksum(t *testing.T) {
	f, idx, tx := mustOpenFragment(t, "i", "f", viewStandard, 0, "")
	_ = idx
	defer f.Clean(t)
	tx.Rollback() // allow f.Checksum to make its read tx.

	// Retrieve checksum and set bits.
	orig, err := f.Checksum()
	if err != nil {
		t.Fatal(err)
	}

	tx = idx.holder.txf.NewTx(Txo{Write: writable, Index: idx, Fragment: f, Shard: f.shard})
	defer tx.Rollback()

	if _, err := f.setBit(tx, 1, 200); err != nil {
		t.Fatal(err)
	} else if _, err := f.setBit(tx, HashBlockSize*2, 200); err != nil {
		t.Fatal(err)
	}
	PanicOn(tx.Commit())

	// Ensure new checksum is different.
	if chksum, err := f.Checksum(); err != nil {
		t.Fatal(err)
	} else if bytes.Equal(chksum, orig) {
		t.Fatalf("expected checksum to change: %x - %x", chksum, orig)
	}
}

// Ensure fragment can return a checksum for a given block.
func TestFragment_Blocks(t *testing.T) {
	f, idx, tx := mustOpenFragment(t, "i", "f", viewStandard, 0, "")
	_ = idx
	defer f.Clean(t)

	// Retrieve initial checksum.
	var prev []FragmentBlock

	// Set first bit.
	if _, err := f.setBit(tx, 0, 0); err != nil {
		t.Fatal(err)
	}
	PanicOn(tx.Commit())
	blocks, err := f.Blocks() // FAIL: TestFragment_Blocks b/c 0 blocks back
	if err != nil {
		t.Fatal(err)
	} else if blocks[0].Checksum == nil {
		t.Fatalf("expected checksum: %x", blocks[0].Checksum)
	}
	prev = blocks

	tx = idx.holder.txf.NewTx(Txo{Write: true, Index: idx, Fragment: f, Shard: f.shard})
	// Set bit on different row.
	if _, err := f.setBit(tx, 20, 0); err != nil {
		t.Fatal(err)
	}
	PanicOn(tx.Commit())
	blocks, err = f.Blocks()
	if err != nil {
		t.Fatal(err)
	} else if bytes.Equal(blocks[0].Checksum, prev[0].Checksum) {
		t.Fatalf("expected checksum to change: %x", blocks[0].Checksum)
	}
	prev = blocks

	// Set bit on different column.
	tx = idx.holder.txf.NewTx(Txo{Write: true, Index: idx, Fragment: f, Shard: f.shard})
	defer tx.Rollback()
	if _, err := f.setBit(tx, 20, 100); err != nil {
		t.Fatal(err)
	}
	PanicOn(tx.Commit())
	blocks, err = f.Blocks()
	if err != nil {
		t.Fatal(err)
	} else if bytes.Equal(blocks[0].Checksum, prev[0].Checksum) {
		t.Fatalf("expected checksum to change: %x", blocks[0].Checksum)
	}
}

// Ensure fragment returns an empty checksum if no data exists for a block.
func TestFragment_Blocks_Empty(t *testing.T) {
	f, idx, tx := mustOpenFragment(t, "i", "f", viewStandard, 0, "")
	_ = idx
	defer f.Clean(t)

	// Set bits on a different block.
	if _, err := f.setBit(tx, 100, 1); err != nil {
		t.Fatal(err)
	}
	PanicOn(tx.Commit()) // f.Blocks() will start a new Tx, so the SetBit needs to be visible before that.

	// Ensure checksum for block 1 is blank.
	if blocks, err := f.Blocks(); err != nil {
		t.Fatal(err)
	} else if len(blocks) != 1 {
		t.Fatalf("unexpected block count: %d", len(blocks))
	} else if blocks[0].ID != 1 {
		t.Fatalf("unexpected block id: %d", blocks[0].ID)
	}
}

// Ensure a fragment's cache can be persisted between restarts.
func TestFragment_LRUCache_Persistence(t *testing.T) {
	f, idx, tx := mustOpenFragment(t, "i", "f", viewStandard, 0, CacheTypeLRU)
	_ = idx
	defer f.Clean(t)

	// Set bits on the fragment.
	for i := uint64(0); i < 1000; i++ {
		if _, err := f.setBit(tx, i, 0); err != nil {
			t.Fatal(err)
		}
	}

	// Verify correct cache type and size.
	if cache, ok := f.cache.(*lruCache); !ok {
		t.Fatalf("unexpected cache: %T", f.cache)
	} else if cache.Len() != 1000 {
		t.Fatalf("unexpected cache len: %d", cache.Len())
	}

	PanicOn(tx.Commit())

	// Reopen the fragment.
	if err := f.Reopen(); err != nil {
		t.Fatal(err)
	}

	// Re-verify correct cache type and size.
	if cache, ok := f.cache.(*lruCache); !ok {
		t.Fatalf("unexpected cache: %T", f.cache)
	} else if cache.Len() != 1000 {
		t.Fatalf("unexpected cache len: %d", cache.Len())
	}
}

// Ensure a fragment's cache can be persisted between restarts.
func TestFragment_RankCache_Persistence(t *testing.T) {
	roaringOnlyTest(t)

	index := mustOpenIndex(t, IndexOptions{})
	defer index.Close()

	// Create field.
	field, err := index.CreateFieldIfNotExists("f", OptFieldTypeSet(CacheTypeRanked, DefaultCacheSize))
	if err != nil {
		t.Fatal(err)
	}

	// Create view.
	view, err := field.createViewIfNotExists(viewStandard)
	if err != nil {
		t.Fatal(err)
	}

	// Create fragment.
	f, err := view.CreateFragmentIfNotExists(0)
	if err != nil {
		t.Fatal(err)
	}

	// Obtain transaction.
	tx := index.holder.txf.NewTx(Txo{Write: writable, Index: index, Fragment: f, Shard: f.shard})
	defer tx.Rollback()

	// Set bits on the fragment.
	for i := uint64(0); i < 1000; i++ {
		if _, err := f.setBit(tx, i, 0); err != nil {
			t.Fatal(err)
		}
	}

	PanicOn(tx.Commit())
	tx = index.holder.txf.NewTx(Txo{Write: !writable, Index: index, Fragment: f, Shard: f.shard})
	defer tx.Rollback()

	// Verify correct cache type and size.
	if cache, ok := f.cache.(*rankCache); !ok {
		t.Fatalf("unexpected cache: %T", f.cache)
	} else if cache.Len() != 1000 {
		t.Fatalf("unexpected cache len: %d", cache.Len())
	}

	// Reopen the index.
	if err := index.reopen(); err != nil {
		t.Fatal(err)
	}

	// Re-fetch fragment.
	f = index.Field("f").view(viewStandard).Fragment(0)

	// Re-verify correct cache type and size.
	if cache, ok := f.cache.(*rankCache); !ok {
		t.Fatalf("unexpected cache: %T", f.cache)
	} else if cache.Len() != 1000 {
		t.Fatalf("unexpected cache len: %d", cache.Len())
	}
}

func roaringOnlyTest(t *testing.T) {
	src := CurrentBackend()
	if src == RoaringTxn || (storage.DefaultBackend == RoaringTxn && src == "") {
		// okay to run, we are under roaring only
	} else {
		t.Skip("skip for everything but roaring")
	}
}

func roaringOnlyBenchmark(b *testing.B) {
	src := CurrentBackend()
	if src == RoaringTxn || (storage.DefaultBackend == RoaringTxn && src == "") {
		// okay to run, we are under roaring only
	} else {
		b.Skip("skip for everything but roaring")
	}
}

// Ensure a fragment can be copied to another fragment.
func TestFragment_WriteTo_ReadFrom(t *testing.T) {
	// roaringOnlyTest(t)

	f0, _, tx := mustOpenFragment(t, "i", "f", viewStandard, 0, "")
	defer f0.Clean(t)

	// Set and then clear bits on the fragment.
	if _, err := f0.setBit(tx, 1000, 1); err != nil {
		t.Fatal(err)
	} else if _, err := f0.setBit(tx, 1000, 2); err != nil {
		t.Fatal(err)
	} else if _, err := f0.clearBit(tx, 1000, 1); err != nil {
		t.Fatal(err)
	}
	err := tx.Commit()
	if err != nil {
		t.Fatalf("committing write: %v", err)
	}

	// Verify cache is populated.
	if n := f0.cache.Len(); n != 1 {
		t.Fatalf("unexpected cache size: %d", n)
	}

	// Write fragment to a buffer.
	var buf bytes.Buffer
	wn, err := f0.WriteTo(&buf)
	if err != nil {
		t.Fatal(err)
	}

	// Read into another fragment.
	f1, idx, tx := mustOpenFragment(t, "i", "f", viewStandard, 0, "")
	tx.Rollback()

	defer f1.Clean(t)

	if rn, err := f1.ReadFrom(&buf); err != nil { // eventually calls fragment.fillFragmentFromArchive
		t.Fatal(err)
	} else if wn != rn {
		t.Fatalf("read/write byte count mismatch: wn=%d, rn=%d", wn, rn)
	}
	// make a read-only Tx after ReadFrom has committed.
	tx = idx.holder.txf.NewTx(Txo{Write: !writable, Index: idx, Fragment: f1, Shard: f1.shard})

	// Verify cache is in other fragment.
	if n := f1.cache.Len(); n != 1 {
		t.Fatalf("unexpected cache size: %d", n)
	}

	// Verify data in other fragment.
	if a := f1.mustRow(tx, 1000).Columns(); !reflect.DeepEqual(a, []uint64{2}) {
		t.Fatalf("unexpected columns: %+v", a)
	}

	// Close and reopen the fragment & verify the data.
	if err := f1.Reopen(); err != nil {
		t.Fatal(err)
	} else if n := f1.cache.Len(); n != 1 {
		t.Fatalf("unexpected cache size (reopen): %d", n)
	} else if a := f1.mustRow(tx, 1000).Columns(); !reflect.DeepEqual(a, []uint64{2}) {
		t.Fatalf("unexpected columns (reopen): %+v", a)
	}
}

func BenchmarkFragment_Blocks(b *testing.B) {
	if *FragmentPath == "" {
		b.Skip("no fragment specified")
	}
	th := newTestHolder(b)

	// Open the fragment specified by the path. Note that newFragment
	// is overriding the usual holder-to-fragment path logic...
	f := newFragment(th, makeTestFragSpec(*FragmentPath, "i", "f", viewStandard), 0, 0)
	if err := f.Open(); err != nil {
		b.Fatal(err)
	}
	defer f.Clean(b)

	// Reset timer and execute benchmark.
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if a, err := f.Blocks(); err != nil {
			b.Fatal(err)
		} else if len(a) == 0 {
			b.Fatal("no blocks in fragment")
		}
	}
}

func BenchmarkFragment_IntersectionCount(b *testing.B) {
	f, idx, tx := mustOpenFragment(b, "i", "f", viewStandard, 0, "")
	defer f.Clean(b)
	f.MaxOpN = math.MaxInt32

	// Generate some intersecting data.
	for i := 0; i < 10000; i += 2 {
		if _, err := f.setBit(tx, 1, uint64(i)); err != nil {
			b.Fatal(err)
		}
	}
	for i := 0; i < 10000; i += 3 {
		if _, err := f.setBit(tx, 2, uint64(i)); err != nil {
			b.Fatal(err)
		}
	}

	PanicOn(tx.Commit())
	tx = idx.holder.txf.NewTx(Txo{Write: !writable, Index: idx, Fragment: f, Shard: f.shard})
	defer tx.Rollback()

	// Snapshot to disk before benchmarking.
	if err := f.Snapshot(); err != nil {
		b.Fatal(err)
	}

	// Start benchmark
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if n := f.mustRow(tx, 1).intersectionCount(f.mustRow(tx, 2)); n == 0 {
			b.Fatalf("unexpected count: %d", n)
		}
	}
}

func TestFragment_Tanimoto(t *testing.T) {
	f, idx, tx := mustOpenFragment(t, "i", "f", viewStandard, 0, CacheTypeRanked)
	_ = idx
	defer f.Clean(t)

	src := NewRow(1, 2, 3)

	// Set bits on the rows 100, 101, & 102.
	f.mustSetBits(tx, 100, 1, 3, 2, 200)
	f.mustSetBits(tx, 101, 1, 3)
	f.mustSetBits(tx, 102, 1, 2, 10, 12)
	f.RecalculateCache()

	if pairs, err := f.top(tx, topOptions{TanimotoThreshold: 50, Src: src}); err != nil {
		t.Fatal(err)
	} else if len(pairs) != 2 {
		t.Fatalf("unexpected count: %d", len(pairs))
	} else if pairs[0] != (Pair{ID: 100, Count: 3}) {
		t.Fatalf("unexpected pair(0): %v", pairs[0])
	} else if pairs[1] != (Pair{ID: 101, Count: 2}) {
		t.Fatalf("unexpected pair(1): %v", pairs[1])
	}
}

func TestFragment_Zero_Tanimoto(t *testing.T) {
	f, idx, tx := mustOpenFragment(t, "i", "f", viewStandard, 0, CacheTypeRanked)
	_ = idx
	defer f.Clean(t)

	src := NewRow(1, 2, 3)

	// Set bits on the rows 100, 101, & 102.
	f.mustSetBits(tx, 100, 1, 3, 2, 200)
	f.mustSetBits(tx, 101, 1, 3)
	f.mustSetBits(tx, 102, 1, 2, 10, 12)
	f.RecalculateCache()

	if pairs, err := f.top(tx, topOptions{TanimotoThreshold: 0, Src: src}); err != nil {
		t.Fatal(err)
	} else if len(pairs) != 3 {
		t.Fatalf("unexpected count: %d", len(pairs))
	} else if pairs[0] != (Pair{ID: 100, Count: 3}) {
		t.Fatalf("unexpected pair(0): %v", pairs[0])
	} else if pairs[1] != (Pair{ID: 101, Count: 2}) {
		t.Fatalf("unexpected pair(1): %v", pairs[1])
	} else if pairs[2] != (Pair{ID: 102, Count: 2}) {
		t.Fatalf("unexpected pair(1): %v", pairs[2])
	}
}

func TestFragment_Snapshot_Run(t *testing.T) {
	roaringOnlyTest(t)

	f, idx, tx := mustOpenFragment(t, "i", "f", viewStandard, 0, "")
	_ = idx
	defer f.Clean(t)

	// Set bits on the fragment.
	for i := uint64(1); i < 3; i++ {
		if _, err := f.setBit(tx, 1000, i); err != nil {
			t.Fatal(err)
		}
	}

	// Snapshot bitmap and verify data.
	if err := f.Snapshot(); err != nil {
		t.Fatal(err)
	} else if n := f.mustRow(tx, 1000).Count(); n != 2 {
		t.Fatalf("unexpected count: %d", n)
	}

	// Close and reopen the fragment & verify the data.
	if err := f.Reopen(); err != nil {
		t.Fatal(err)
	} else if n := f.mustRow(tx, 1000).Count(); n != 2 {
		t.Fatalf("unexpected count (reopen): %d", n)
	}
}

// Ensure a fragment can set mutually exclusive values.
func TestFragment_SetMutex(t *testing.T) {
	f, _, tx := mustOpenMutexFragment(t, "i", "f", viewStandard, 0, "")
	defer f.Clean(t)

	var cols []uint64

	// Set a value on column 100.
	if _, err := f.setBit(tx, 1, 100); err != nil {
		t.Fatal(err)
	}
	// Verify the value was set.
	cols = f.mustRow(tx, 1).Columns()
	if !reflect.DeepEqual(cols, []uint64{100}) {
		t.Fatalf("mutex unexpected columns: %v", cols)
	}

	// Set a different value on column 100.
	if _, err := f.setBit(tx, 2, 100); err != nil {
		t.Fatal(err)
	}
	// Verify that value (row 1) was replaced (by row 2).
	cols = f.mustRow(tx, 1).Columns()
	if !reflect.DeepEqual(cols, []uint64{}) {
		t.Fatalf("mutex unexpected columns: %v", cols)
	}
	cols = f.mustRow(tx, 2).Columns()
	if !reflect.DeepEqual(cols, []uint64{100}) {
		t.Fatalf("mutex unexpected columns: %v", cols)
	}
}

// Ensure a fragment can import into set fields.
func TestFragment_ImportSet(t *testing.T) {
	tests := []struct {
		setRowIDs   []uint64
		setColIDs   []uint64
		setExp      map[uint64][]uint64
		clearRowIDs []uint64
		clearColIDs []uint64
		clearExp    map[uint64][]uint64
	}{
		{
			[]uint64{1, 1, 1, 1},
			[]uint64{0, 1, 2, 3},
			map[uint64][]uint64{
				1: {0, 1, 2, 3},
			},
			[]uint64{},
			[]uint64{},
			map[uint64][]uint64{
				1: {0, 1, 2, 3},
			},
		},
		{
			[]uint64{1, 1, 1, 1, 2, 2, 2, 2},
			[]uint64{0, 1, 2, 3, 0, 1, 2, 3},
			map[uint64][]uint64{
				1: {0, 1, 2, 3},
				2: {0, 1, 2, 3},
			},
			[]uint64{1, 1, 2},
			[]uint64{1, 2, 3},
			map[uint64][]uint64{
				1: {0, 3},
				2: {0, 1, 2},
			},
		},
		{
			[]uint64{1, 1, 1, 1, 2},
			[]uint64{0, 1, 2, 3, 1},
			map[uint64][]uint64{
				1: {0, 1, 2, 3},
				2: {1},
			},
			[]uint64{1, 1, 1, 1},
			[]uint64{0, 1, 2, 3},
			map[uint64][]uint64{
				1: {},
				2: {1},
			},
		},
		{
			[]uint64{1, 1, 1, 1, 2, 2, 1},
			[]uint64{0, 1, 2, 3, 1, 8, 1},
			map[uint64][]uint64{
				1: {0, 1, 2, 3},
				2: {1, 8},
			},
			[]uint64{1, 1},
			[]uint64{0, 0},
			map[uint64][]uint64{
				1: {1, 2, 3},
				2: {1, 8},
			},
		},
		{
			[]uint64{1, 2, 3},
			[]uint64{8, 8, 8},
			map[uint64][]uint64{
				1: {8},
				2: {8},
				3: {8},
			},
			[]uint64{1, 2, 3},
			[]uint64{9, 9, 9},
			map[uint64][]uint64{
				1: {8},
				2: {8},
				3: {8},
			},
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("importset%d", i), func(t *testing.T) {
			f, idx, tx := mustOpenFragment(t, "i", "f", viewStandard, 0, "")
			_ = idx
			defer f.Clean(t)

			// Set import.
			err := f.bulkImport(tx, test.setRowIDs, test.setColIDs, &ImportOptions{})
			if err != nil {
				t.Fatalf("bulk importing ids: %v", err)
			}

			// Check for expected results.
			for k, v := range test.setExp {
				cols := f.mustRow(tx, k).Columns()
				if !reflect.DeepEqual(cols, v) {
					t.Fatalf("expected: %v, but got: %v", v, cols)
				}
			}

			// Clear import.
			err = f.bulkImport(tx, test.clearRowIDs, test.clearColIDs, &ImportOptions{Clear: true})
			if err != nil {
				t.Fatalf("bulk clearing ids: %v", err)
			}

			// Check for expected results.
			for k, v := range test.clearExp {
				cols := f.mustRow(tx, k).Columns()
				if !reflect.DeepEqual(cols, v) {
					t.Fatalf("expected: %v, but got: %v", v, cols)
				}
			}
		})
	}
}

func TestFragment_ImportSet_WithTxCommit(t *testing.T) {
	tests := []struct {
		setRowIDs   []uint64
		setColIDs   []uint64
		setExp      map[uint64][]uint64
		clearRowIDs []uint64
		clearColIDs []uint64
		clearExp    map[uint64][]uint64
	}{
		{
			[]uint64{1, 1, 1, 1},
			[]uint64{0, 1, 2, 3},
			map[uint64][]uint64{
				1: {0, 1, 2, 3},
			},
			[]uint64{},
			[]uint64{},
			map[uint64][]uint64{
				1: {0, 1, 2, 3},
			},
		},
		{
			[]uint64{1, 1, 1, 1, 2, 2, 2, 2},
			[]uint64{0, 1, 2, 3, 0, 1, 2, 3},
			map[uint64][]uint64{
				1: {0, 1, 2, 3},
				2: {0, 1, 2, 3},
			},
			[]uint64{1, 1, 2},
			[]uint64{1, 2, 3},
			map[uint64][]uint64{
				1: {0, 3},
				2: {0, 1, 2},
			},
		},
		{
			[]uint64{1, 1, 1, 1, 2},
			[]uint64{0, 1, 2, 3, 1},
			map[uint64][]uint64{
				1: {0, 1, 2, 3},
				2: {1},
			},
			[]uint64{1, 1, 1, 1},
			[]uint64{0, 1, 2, 3},
			map[uint64][]uint64{
				1: {},
				2: {1},
			},
		},
		{
			[]uint64{1, 1, 1, 1, 2, 2, 1},
			[]uint64{0, 1, 2, 3, 1, 8, 1},
			map[uint64][]uint64{
				1: {0, 1, 2, 3},
				2: {1, 8},
			},
			[]uint64{1, 1},
			[]uint64{0, 0},
			map[uint64][]uint64{
				1: {1, 2, 3},
				2: {1, 8},
			},
		},
		{
			[]uint64{1, 2, 3},
			[]uint64{8, 8, 8},
			map[uint64][]uint64{
				1: {8},
				2: {8},
				3: {8},
			},
			[]uint64{1, 2, 3},
			[]uint64{9, 9, 9},
			map[uint64][]uint64{
				1: {8},
				2: {8},
				3: {8},
			},
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("importset%d", i), func(t *testing.T) {
			f, idx, tx := mustOpenFragment(t, "i", "f", viewStandard, 0, "")
			_ = idx
			defer f.Clean(t)

			// Set import.
			err := f.bulkImport(tx, test.setRowIDs, test.setColIDs, &ImportOptions{})
			if err != nil {
				t.Fatalf("bulk importing ids: %v", err)
			}

			PanicOn(tx.Commit())
			tx = idx.holder.txf.NewTx(Txo{Write: !writable, Index: idx, Fragment: f, Shard: f.shard})
			defer tx.Rollback()

			// Check for expected results.
			for k, v := range test.setExp {
				cols := f.mustRow(tx, k).Columns()
				if !reflect.DeepEqual(cols, v) {
					t.Fatalf("expected: %v, but got: %v", v, cols)
				}
			}

			tx.Rollback()
			tx = idx.holder.txf.NewTx(Txo{Write: writable, Index: idx, Fragment: f, Shard: f.shard})
			defer tx.Rollback()

			// Clear import.
			err = f.bulkImport(tx, test.clearRowIDs, test.clearColIDs, &ImportOptions{Clear: true})
			if err != nil {
				t.Fatalf("bulk clearing ids: %v", err)
			}

			PanicOn(tx.Commit())
			tx = idx.holder.txf.NewTx(Txo{Write: !writable, Index: idx, Fragment: f, Shard: f.shard})
			defer tx.Rollback()

			// Check for expected results.
			for k, v := range test.clearExp {
				cols := f.mustRow(tx, k).Columns()
				if !reflect.DeepEqual(cols, v) {
					t.Fatalf("expected: %v, but got: %v", v, cols)
				}
			}
		})
	}
}

func TestFragment_ConcurrentImport(t *testing.T) {
	t.Run("bulkImportStandard", func(t *testing.T) {
		shard := uint64(0)
		f, idx, tx := mustOpenFragment(t, "i", "f", viewStandard, shard, "")
		_ = idx
		defer f.Clean(t)
		// note: write Tx must be used on the same goroutine that created them.
		// So we close out the "default" Tx created by mustOpenFragment, and
		// have the goroutines below each make their own. One should get the
		// write lock first, and thus they should get serialized.
		tx.Rollback()

		eg := errgroup.Group{}
		eg.Go(func() error {
			tx := idx.holder.txf.NewTx(Txo{Write: writable, Index: idx, Fragment: f, Shard: shard})
			defer func() { PanicOn(tx.Commit()) }()
			return f.bulkImportStandard(tx, []uint64{1, 2}, []uint64{1, 2}, &ImportOptions{})
		})
		eg.Go(func() error {
			tx := idx.holder.txf.NewTx(Txo{Write: writable, Index: idx, Fragment: f, Shard: shard})
			defer func() { PanicOn(tx.Commit()) }()
			return f.bulkImportStandard(tx, []uint64{3, 4}, []uint64{3, 4}, &ImportOptions{})
		})
		err := eg.Wait()
		if err != nil {
			t.Fatalf("importing data to fragment: %v", err)
		}
	})
}

// Ensure a fragment can import mutually exclusive values.
func TestFragment_ImportMutex(t *testing.T) {
	tests := []struct {
		setRowIDs   []uint64
		setColIDs   []uint64
		setExp      map[uint64][]uint64
		clearRowIDs []uint64
		clearColIDs []uint64
		clearExp    map[uint64][]uint64
	}{
		{
			[]uint64{1, 1, 1, 1},
			[]uint64{0, 1, 2, 3},
			map[uint64][]uint64{
				1: {0, 1, 2, 3},
			},
			[]uint64{},
			[]uint64{},
			map[uint64][]uint64{
				1: {0, 1, 2, 3},
			},
		},
		{
			[]uint64{1, 1, 1, 1, 2, 2, 2, 2},
			[]uint64{0, 1, 2, 3, 0, 1, 2, 3},
			map[uint64][]uint64{
				1: {},
				2: {0, 1, 2, 3},
			},
			[]uint64{1, 1, 2},
			[]uint64{1, 2, 3},
			map[uint64][]uint64{
				1: {},
				2: {0, 1, 2},
			},
		},
		{
			[]uint64{1, 1, 1, 1, 2},
			[]uint64{0, 1, 2, 3, 1},
			map[uint64][]uint64{
				1: {0, 2, 3},
				2: {1},
			},
			[]uint64{1, 1, 1, 1},
			[]uint64{0, 1, 2, 3},
			map[uint64][]uint64{
				1: {},
				2: {1},
			},
		},
		{
			[]uint64{1, 1, 1, 1, 2, 2, 1},
			[]uint64{0, 1, 2, 3, 1, 8, 1},
			map[uint64][]uint64{
				1: {0, 1, 2, 3},
				2: {8},
			},
			[]uint64{1, 1},
			[]uint64{0, 0},
			map[uint64][]uint64{
				1: {1, 2, 3},
				2: {8},
			},
		},
		{
			[]uint64{1, 2, 3},
			[]uint64{8, 8, 8},
			map[uint64][]uint64{
				1: {},
				2: {},
				3: {8},
			},
			[]uint64{1, 2, 3},
			[]uint64{9, 9, 9},
			map[uint64][]uint64{
				1: {},
				2: {},
				3: {8},
			},
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("importmutex%d", i), func(t *testing.T) {
			f, _, tx := mustOpenMutexFragment(t, "i", "f", viewStandard, 0, "")
			defer f.Clean(t)

			// Set import.
			err := f.bulkImport(tx, test.setRowIDs, test.setColIDs, &ImportOptions{})
			if err != nil {
				t.Fatalf("bulk importing ids: %v", err)
			}

			// Check for expected results.
			for k, v := range test.setExp {
				cols := f.mustRow(tx, k).Columns()
				if !reflect.DeepEqual(cols, v) {
					t.Fatalf("row: %d, expected: %v, but got: %v", k, v, cols)
				}
			}

			// Clear import.
			err = f.bulkImport(tx, test.clearRowIDs, test.clearColIDs, &ImportOptions{Clear: true})
			if err != nil {
				t.Fatalf("bulk clearing ids: %v", err)
			}

			// Check for expected results.
			for k, v := range test.clearExp {
				cols := f.mustRow(tx, k).Columns()
				if !reflect.DeepEqual(cols, v) {
					t.Fatalf("row: %d expected: %v, but got: %v", k, v, cols)
				}
			}
		})
	}
}

// Ensure a fragment can import mutually exclusive values.
// Now with Commits in the middle.
func TestFragment_ImportMutex_WithTxCommit(t *testing.T) {
	tests := []struct {
		setRowIDs   []uint64
		setColIDs   []uint64
		setExp      map[uint64][]uint64
		clearRowIDs []uint64
		clearColIDs []uint64
		clearExp    map[uint64][]uint64
	}{
		{
			[]uint64{1, 1, 1, 1},
			[]uint64{0, 1, 2, 3},
			map[uint64][]uint64{
				1: {0, 1, 2, 3},
			},
			[]uint64{},
			[]uint64{},
			map[uint64][]uint64{
				1: {0, 1, 2, 3},
			},
		},
		{
			[]uint64{1, 1, 1, 1, 2, 2, 2, 2},
			[]uint64{0, 1, 2, 3, 0, 1, 2, 3},
			map[uint64][]uint64{
				1: {},
				2: {0, 1, 2, 3},
			},
			[]uint64{1, 1, 2},
			[]uint64{1, 2, 3},
			map[uint64][]uint64{
				1: {},
				2: {0, 1, 2},
			},
		},
		{
			[]uint64{1, 1, 1, 1, 2},
			[]uint64{0, 1, 2, 3, 1},
			map[uint64][]uint64{
				1: {0, 2, 3},
				2: {1},
			},
			[]uint64{1, 1, 1, 1},
			[]uint64{0, 1, 2, 3},
			map[uint64][]uint64{
				1: {},
				2: {1},
			},
		},
		{
			[]uint64{1, 1, 1, 1, 2, 2, 1},
			[]uint64{0, 1, 2, 3, 1, 8, 1},
			map[uint64][]uint64{
				1: {0, 1, 2, 3},
				2: {8},
			},
			[]uint64{1, 1},
			[]uint64{0, 0},
			map[uint64][]uint64{
				1: {1, 2, 3},
				2: {8},
			},
		},
		{
			[]uint64{1, 2, 3},
			[]uint64{8, 8, 8},
			map[uint64][]uint64{
				1: {},
				2: {},
				3: {8},
			},
			[]uint64{1, 2, 3},
			[]uint64{9, 9, 9},
			map[uint64][]uint64{
				1: {},
				2: {},
				3: {8},
			},
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("importmutex%d", i), func(t *testing.T) {
			f, idx, tx := mustOpenMutexFragment(t, "i", "f", viewStandard, 0, "")
			defer f.Clean(t)

			// Set import.
			err := f.bulkImport(tx, test.setRowIDs, test.setColIDs, &ImportOptions{})
			if err != nil {
				t.Fatalf("bulk importing ids: %v", err)
			}

			PanicOn(tx.Commit())
			tx = idx.holder.txf.NewTx(Txo{Write: !writable, Index: idx, Fragment: f, Shard: f.shard})
			defer tx.Rollback()

			// Check for expected results.
			for k, v := range test.setExp {
				cols := f.mustRow(tx, k).Columns()
				if !reflect.DeepEqual(cols, v) {
					t.Fatalf("row: %d, expected: %v, but got: %v", k, v, cols)
				}
			}

			tx.Rollback()
			tx = idx.holder.txf.NewTx(Txo{Write: writable, Index: idx, Fragment: f, Shard: f.shard})
			defer tx.Rollback()

			// Clear import.
			err = f.bulkImport(tx, test.clearRowIDs, test.clearColIDs, &ImportOptions{Clear: true})
			if err != nil {
				t.Fatalf("bulk clearing ids: %v", err)
			}

			PanicOn(tx.Commit())
			tx = idx.holder.txf.NewTx(Txo{Write: !writable, Index: idx, Fragment: f, Shard: f.shard})
			defer tx.Rollback()

			// Check for expected results.
			for k, v := range test.clearExp {
				cols := f.mustRow(tx, k).Columns()
				if !reflect.DeepEqual(cols, v) {
					t.Fatalf("row: %d expected: %v, but got: %v", k, v, cols)
				}
			}
		})
	}
}

// Ensure a fragment can import bool values.
func TestFragment_ImportBool(t *testing.T) {
	tests := []struct {
		setRowIDs   []uint64
		setColIDs   []uint64
		setExp      map[uint64][]uint64
		clearRowIDs []uint64
		clearColIDs []uint64
		clearExp    map[uint64][]uint64
	}{
		{
			[]uint64{1, 1, 1, 1},
			[]uint64{0, 1, 2, 3},
			map[uint64][]uint64{
				1: {0, 1, 2, 3},
			},
			[]uint64{},
			[]uint64{},
			map[uint64][]uint64{
				1: {0, 1, 2, 3},
			},
		},
		{
			[]uint64{0, 0, 0, 0, 1, 1, 1, 1},
			[]uint64{0, 1, 2, 3, 0, 1, 2, 3},
			map[uint64][]uint64{
				0: {},
				1: {0, 1, 2, 3},
			},
			[]uint64{1, 1, 2},
			[]uint64{1, 2, 3},
			map[uint64][]uint64{
				0: {},
				1: {0, 3},
				2: {},
			},
		},
		{
			[]uint64{0, 0, 0, 0, 1},
			[]uint64{0, 1, 2, 3, 1},
			map[uint64][]uint64{
				0: {0, 2, 3},
				1: {1},
			},
			[]uint64{1, 1, 1, 1},
			[]uint64{0, 1, 2, 3},
			map[uint64][]uint64{
				0: {0, 2, 3},
				1: {},
			},
		},
		{
			[]uint64{1, 1, 1, 1, 0, 0, 1},
			[]uint64{0, 1, 2, 3, 1, 8, 1},
			map[uint64][]uint64{
				0: {8},
				1: {0, 1, 2, 3},
			},
			[]uint64{1, 1},
			[]uint64{0, 0},
			map[uint64][]uint64{
				0: {8},
				1: {1, 2, 3},
			},
		},
		{
			[]uint64{0, 1, 2},
			[]uint64{8, 8, 8},
			map[uint64][]uint64{
				0: {},
				1: {}, // This isn't {8} because fragment doesn't validate bool values.
				2: {8},
			},
			[]uint64{1, 2, 3},
			[]uint64{9, 9, 9},
			map[uint64][]uint64{
				0: {},
				1: {},
				2: {8},
			},
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("importmutex%d", i), func(t *testing.T) {
			f, _, tx := mustOpenBoolFragment(t, "i", "f", viewStandard, 0, "")
			defer f.Clean(t)

			// Set import.
			err := f.bulkImport(tx, test.setRowIDs, test.setColIDs, &ImportOptions{})
			if err != nil {
				t.Fatalf("bulk importing ids: %v", err)
			}

			// Check for expected results.
			for k, v := range test.setExp {
				cols := f.mustRow(tx, k).Columns()
				if !reflect.DeepEqual(cols, v) {
					t.Fatalf("expected: %v, but got: %v", v, cols)
				}
			}

			// Clear import.
			err = f.bulkImport(tx, test.clearRowIDs, test.clearColIDs, &ImportOptions{Clear: true})
			if err != nil {
				t.Fatalf("bulk importing ids: %v", err)
			}

			// Check for expected results.
			for k, v := range test.clearExp {
				cols := f.mustRow(tx, k).Columns()
				if !reflect.DeepEqual(cols, v) {
					t.Fatalf("expected: %v, but got: %v", v, cols)
				}
			}
		})
	}
}

// Ensure a fragment can import bool values, with Commits in between writes and reads.
func TestFragment_ImportBool_WithTxCommit(t *testing.T) {
	tests := []struct {
		setRowIDs   []uint64
		setColIDs   []uint64
		setExp      map[uint64][]uint64
		clearRowIDs []uint64
		clearColIDs []uint64
		clearExp    map[uint64][]uint64
	}{
		{
			[]uint64{1, 1, 1, 1},
			[]uint64{0, 1, 2, 3},
			map[uint64][]uint64{
				1: {0, 1, 2, 3},
			},
			[]uint64{},
			[]uint64{},
			map[uint64][]uint64{
				1: {0, 1, 2, 3},
			},
		},
		{
			[]uint64{0, 0, 0, 0, 1, 1, 1, 1},
			[]uint64{0, 1, 2, 3, 0, 1, 2, 3},
			map[uint64][]uint64{
				0: {},
				1: {0, 1, 2, 3},
			},
			[]uint64{1, 1, 2},
			[]uint64{1, 2, 3},
			map[uint64][]uint64{
				0: {},
				1: {0, 3},
				2: {},
			},
		},
		{
			[]uint64{0, 0, 0, 0, 1},
			[]uint64{0, 1, 2, 3, 1},
			map[uint64][]uint64{
				0: {0, 2, 3},
				1: {1},
			},
			[]uint64{1, 1, 1, 1},
			[]uint64{0, 1, 2, 3},
			map[uint64][]uint64{
				0: {0, 2, 3},
				1: {},
			},
		},
		{
			[]uint64{1, 1, 1, 1, 0, 0, 1},
			[]uint64{0, 1, 2, 3, 1, 8, 1},
			map[uint64][]uint64{
				0: {8},
				1: {0, 1, 2, 3},
			},
			[]uint64{1, 1},
			[]uint64{0, 0},
			map[uint64][]uint64{
				0: {8},
				1: {1, 2, 3},
			},
		},
		{
			[]uint64{0, 1, 2},
			[]uint64{8, 8, 8},
			map[uint64][]uint64{
				0: {},
				1: {}, // This isn't {8} because fragment doesn't validate bool values.
				2: {8},
			},
			[]uint64{1, 2, 3},
			[]uint64{9, 9, 9},
			map[uint64][]uint64{
				0: {},
				1: {},
				2: {8},
			},
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("importmutex%d", i), func(t *testing.T) {
			f, idx, tx := mustOpenBoolFragment(t, "i", "f", viewStandard, 0, "")
			defer f.Clean(t)

			// Set import.
			err := f.bulkImport(tx, test.setRowIDs, test.setColIDs, &ImportOptions{})
			if err != nil {
				t.Fatalf("bulk importing ids: %v", err)
			}

			PanicOn(tx.Commit())
			tx = idx.holder.txf.NewTx(Txo{Write: !writable, Index: idx, Fragment: f, Shard: f.shard})
			defer tx.Rollback()

			// Check for expected results.
			for k, v := range test.setExp {
				cols := f.mustRow(tx, k).Columns()
				if !reflect.DeepEqual(cols, v) {
					t.Fatalf("expected: %v, but got: %v", v, cols)
				}
			}

			tx.Rollback()
			tx = idx.holder.txf.NewTx(Txo{Write: writable, Index: idx, Fragment: f, Shard: f.shard})
			defer tx.Rollback()

			// Clear import.
			err = f.bulkImport(tx, test.clearRowIDs, test.clearColIDs, &ImportOptions{Clear: true})
			if err != nil {
				t.Fatalf("bulk importing ids: %v", err)
			}

			PanicOn(tx.Commit())
			tx = idx.holder.txf.NewTx(Txo{Write: !writable, Index: idx, Fragment: f, Shard: f.shard})
			defer tx.Rollback()

			// Check for expected results.
			for k, v := range test.clearExp {
				cols := f.mustRow(tx, k).Columns()
				if !reflect.DeepEqual(cols, v) {
					t.Fatalf("expected: %v, but got: %v", v, cols)
				}
			}
		})
	}
}

func makeTestFragSpec(path, index, field, view0 string) fragSpec {

	// /tmp/holder-dir199550572/i/f/views/standard/fragments/0 -> /tmp/holder-dir199550572/i/f/views/standard
	splt := strings.Split(path, sep+"fragments"+sep)
	return fragSpec{
		index: &Index{path: splt[0], name: index},
		field: &Field{name: field},
		view:  &view{path: splt[0], name: view0},
	}
}

func BenchmarkFragment_Snapshot(b *testing.B) {
	if *FragmentPath == "" {
		b.Skip("no fragment specified")
	}

	b.ReportAllocs()
	// Open the fragment specified by the path.
	f := newFragment(newTestHolder(b), makeTestFragSpec(*FragmentPath, "i", "f", viewStandard), 0, 0)
	if err := f.Open(); err != nil {
		b.Fatal(err)
	}
	defer f.Clean(b)
	b.ResetTimer()

	// Reset timer and execute benchmark.
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		err := f.Snapshot()
		if err != nil {
			b.Fatalf("unexpected count (reopen): %s", err)
		}
	}
}

func BenchmarkFragment_FullSnapshot(b *testing.B) {
	f, idx, tx := mustOpenFragment(b, "i", "f", viewStandard, 0, "")
	_ = idx
	tx.Rollback()
	defer f.Clean(b)

	// Generate some intersecting data.
	maxX := ShardWidth / 2
	sz := maxX
	rows := make([]uint64, sz)
	cols := make([]uint64, sz)

	options := &ImportOptions{}
	max := 0
	for row := 0; row < 100; row++ {
		val := 1
		i := 0
		for col := 0; col < ShardWidth/2; col++ {
			rows[i] = uint64(row)
			cols[i] = uint64(val)
			val += 2
			i++
		}

		tx := idx.holder.txf.NewTx(Txo{Write: !writable, Index: idx, Fragment: f, Shard: f.shard})
		defer tx.Rollback()

		if err := f.bulkImport(tx, rows, cols, options); err != nil {
			b.Fatalf("Error Building Sample: %s", err)
		}
		tx.Rollback()
		if row > max {
			max = row
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := f.Snapshot(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFragment_Import(b *testing.B) {
	b.StopTimer()
	maxX := ShardWidth * 5 * 2
	sz := maxX
	rows := make([]uint64, sz)
	cols := make([]uint64, sz)
	i := 0
	for row := 0; row < 100; row++ {
		val := 1
		for col := 0; col < ShardWidth/2; col++ {
			rows[i] = uint64(row)
			cols[i] = uint64(val)
			val += 2
			i++
		}
		if i == maxX {
			break
		}
	}
	rowsUse, colsUse := make([]uint64, len(rows)), make([]uint64, len(cols))
	b.ReportAllocs()
	options := &ImportOptions{}
	for i := 0; i < b.N; i++ {
		// since bulkImport modifies the input slices, we make new copies for each round
		copy(rowsUse, rows)
		copy(colsUse, cols)
		f, idx, tx := mustOpenFragment(b, "i", "f", viewStandard, 0, "")
		_ = idx
		b.StartTimer()
		if err := f.bulkImport(tx, rowsUse, colsUse, options); err != nil {
			b.Errorf("Error Building Sample: %s", err)
		}
		b.StopTimer()
		tx.Rollback()
		f.Clean(b)
	}
}

var (
	rowCases         = []uint64{2, 50, 1000, 10000, 100000}
	colCases         = []uint64{20, 1000, 5000, 50000, 500000}
	concurrencyCases = []int{2, 4, 16}
	cacheCases       = []string{CacheTypeNone, CacheTypeRanked}
)

func BenchmarkImportRoaring(b *testing.B) {
	for _, numRows := range rowCases {
		data := getZipfRowsSliceRoaring(numRows, 1, 0, ShardWidth)
		b.Logf("%dRows: %.2fMB\n", numRows, float64(len(data))/1024/1024)
		for _, cacheType := range cacheCases {
			b.Run(fmt.Sprintf("Rows%dCache_%s", numRows, cacheType), func(b *testing.B) {
				b.StopTimer()
				for i := 0; i < b.N; i++ {
					f, _, tx := mustOpenFragment(b, "i", fmt.Sprintf("r%dc%s", numRows, cacheType), viewStandard, 0, cacheType)
					b.StartTimer()

					err := f.importRoaringT(tx, data, false)
					if err != nil {
						// we don't actually particularly
						// care whether this succeeds,
						// but if it's happening we want
						// it to be done.
						_ = f.holder.SnapshotQueue.Await(f)
						f.Clean(b)
						b.Fatalf("import error: %v", err)
					}
					b.StopTimer()
					f.Clean(b)
				}
			})
		}
	}
}

func BenchmarkImportRoaringConcurrent(b *testing.B) {
	if testing.Short() {
		b.SkipNow()
	}
	for _, numRows := range rowCases {
		data := make([][]byte, 0, len(concurrencyCases))
		data = append(data, getZipfRowsSliceRoaring(numRows, 0, 0, ShardWidth))
		b.Logf("%dRows: %.2fMB\n", numRows, float64(len(data[0]))/1024/1024)
		for _, concurrency := range concurrencyCases {
			// add more data sets
			for j := len(data); j < concurrency; j++ {
				data = append(data, getZipfRowsSliceRoaring(numRows, int64(j), 0, ShardWidth))
			}
			for _, cacheType := range cacheCases {
				b.Run(fmt.Sprintf("Rows%dConcurrency%dCache_%s", numRows, concurrency, cacheType), func(b *testing.B) {
					b.StopTimer()
					frags := make([]*fragment, concurrency)
					txs := make([]Tx, concurrency)
					for i := 0; i < b.N; i++ {
						for j := 0; j < concurrency; j++ {
							frags[j], _, txs[j] = mustOpenFragment(b, "i", "f", viewStandard, uint64(j), cacheType)
						}
						eg := errgroup.Group{}
						b.StartTimer()
						for j := 0; j < concurrency; j++ {
							j := j
							eg.Go(func() error {
								defer txs[j].Rollback()

								err := frags[j].importRoaringT(txs[j], data[j], false)
								// error unimportant if it happened, but we want
								// any snapshots to have finished.
								_ = frags[j].holder.SnapshotQueue.Await(frags[j])
								return err
							})
						}
						err := eg.Wait()
						if err != nil {
							b.Errorf("importing fragment: %v", err)
						}
						b.StopTimer()
						for j := 0; j < concurrency; j++ {
							frags[j].Clean(b)
						}
					}
				})
			}
		}
	}
}
func BenchmarkImportRoaringUpdateConcurrent(b *testing.B) {
	roaringOnlyBenchmark(b)
	if testing.Short() {
		b.SkipNow()
	}
	for _, numRows := range rowCases {
		for _, numCols := range colCases {
			data := getZipfRowsSliceRoaring(numRows, 1, 0, ShardWidth)
			updata := getUpdataRoaring(numRows, numCols, 1)
			for _, concurrency := range concurrencyCases {
				for _, cacheType := range cacheCases {
					b.Run(fmt.Sprintf("Rows%dCols%dConcurrency%dCache_%s", numRows, numCols, concurrency, cacheType), func(b *testing.B) {
						b.StopTimer()
						frags := make([]*fragment, concurrency)
						txs := make([]Tx, concurrency)
						for i := 0; i < b.N; i++ {
							for j := 0; j < concurrency; j++ {
								frags[j], _, txs[j] = mustOpenFragment(b, "i", "f", viewStandard, uint64(j), cacheType)

								// the cost of actually doing the op log for the large initial data set
								// is excessive. force storage into snapshotted state, then use import
								// to generate an op log and/or snapshot.
								// note: skipped for rbf, bolt, lmdb, above.
								_, _, err := frags[j].storage.ImportRoaringBits(data, false, false, 0)
								if err != nil {
									b.Fatalf("importing roaring: %v", err)
								}
								err = frags[j].holder.SnapshotQueue.Immediate(frags[j])
								if err != nil {
									b.Fatalf("snapshot after import: %v", err)
								}
							}
							eg := errgroup.Group{}
							b.StartTimer()
							for j := 0; j < concurrency; j++ {
								j := j
								eg.Go(func() error {
									defer txs[j].Rollback()

									err := frags[j].importRoaringT(txs[j], updata, false)
									err2 := frags[j].holder.SnapshotQueue.Await(frags[j])
									if err == nil {
										err = err2
									}
									return err
								})
							}
							err := eg.Wait()
							if err != nil {
								b.Errorf("importing fragment: %v", err)
							}
							b.StopTimer()
							for j := 0; j < concurrency; j++ {
								frags[j].Clean(b)
							}
						}
					})
				}
			}
		}
	}
}

func BenchmarkImportStandard(b *testing.B) {
	for _, cacheType := range cacheCases {
		for _, numRows := range rowCases {
			rowIDsOrig, columnIDsOrig := getZipfRowsSliceStandard(numRows, 1, 0, ShardWidth)
			rowIDs, columnIDs := make([]uint64, len(rowIDsOrig)), make([]uint64, len(columnIDsOrig))
			b.Run(fmt.Sprintf("Rows%dCache_%s", numRows, cacheType), func(b *testing.B) {
				b.StopTimer()
				for i := 0; i < b.N; i++ {
					copy(rowIDs, rowIDsOrig)
					copy(columnIDs, columnIDsOrig)
					f, idx, tx := mustOpenFragment(b, "i", fmt.Sprintf("r%dc%s", numRows, cacheType), viewStandard, 0, cacheType)
					_ = idx
					b.StartTimer()
					err := f.bulkImport(tx, rowIDs, columnIDs, &ImportOptions{})
					if err != nil {
						b.Errorf("import error: %v", err)
					}
					b.StopTimer()
					tx.Rollback()
					f.Clean(b)
				}
			})
		}
	}
}

func BenchmarkImportRoaringUpdate(b *testing.B) {
	fileSize := make(map[string]int64)
	names := []string{}
	for _, numRows := range rowCases {
		data := getZipfRowsSliceRoaring(numRows, 1, 0, ShardWidth)
		for _, numCols := range colCases {
			updata := getUpdataRoaring(numRows, numCols, 1)
			for _, cacheType := range cacheCases {
				name := fmt.Sprintf("Rows%dCols%dCache_%s", numRows, numCols, cacheType)
				names = append(names, name)
				b.Run(name, func(b *testing.B) {
					b.StopTimer()
					for i := 0; i < b.N; i++ {
						f, idx, tx := mustOpenFragment(b, "i", fmt.Sprintf("r%dc%dcache_%s", numRows, numCols, cacheType), viewStandard, 0, cacheType)
						_ = idx

						// the cost of actually doing the op log for the large initial data set
						// is excessive. force storage into snapshotted state, then use import
						// to generate an op log and/or snapshot.
						itr, err := roaring.NewRoaringIterator(data)
						PanicOn(err)
						_, _, err = tx.ImportRoaringBits(f.index(), f.field(), f.view(), f.shard, itr, false, false, 0)
						if err != nil {
							b.Errorf("import error: %v", err)
						}
						err = f.holder.SnapshotQueue.Immediate(f)
						if err != nil {
							b.Errorf("snapshot after import error: %v", err)
						}
						b.StartTimer()
						err = f.importRoaringT(tx, updata, false)
						if err != nil {
							f.Clean(b)
							b.Errorf("import error: %v", err)
						}
						err = f.holder.SnapshotQueue.Await(f)
						if err != nil {
							b.Errorf("snapshot after import error: %v", err)
						}
						b.StopTimer()
						var stat os.FileInfo
						var statTarget io.Writer
						err = f.gen.Transaction(&statTarget, func() error {
							targetFile, ok := statTarget.(*os.File)
							if ok {
								stat, _ = targetFile.Stat()
							} else {
								b.Errorf("couldn't stat file")
							}
							return nil
						})
						if err != nil {
							b.Errorf("transaction error: %v", err)
						}
						fileSize[name] = stat.Size()
						f.Clean(b)
					}
				})

			}
		}
	}
	for _, name := range names {
		b.Logf("%s: %.2fMB\n", name, float64(fileSize[name])/1024/1024)
	}
}

// BenchmarkUpdatePathological imports more data into an existing fragment than
// already exists, but the larger data comes after the smaller data. If
// SliceContainers are in use, this can cause horrible performance.
func BenchmarkUpdatePathological(b *testing.B) {
	exists := getZipfRowsSliceRoaring(100000, 1, 0, 400000)
	inc := getZipfRowsSliceRoaring(100000, 2, 400000, ShardWidth)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		f, idx, tx := mustOpenFragment(b, "i", "f", viewStandard, 0, DefaultCacheType)
		_ = idx

		err := f.importRoaringT(tx, exists, false)
		if err != nil {
			b.Fatalf("importing roaring: %v", err)
		}
		b.StartTimer()
		err = f.importRoaringT(tx, inc, false)
		if err != nil {
			b.Fatalf("importing second: %v", err)
		}

	}

}

var bigFrag string

func initBigFrag(tb testing.TB) {
	if bigFrag == "" {
		f, _, tx := mustOpenFragment(tb, "i", "f", viewStandard, 0, DefaultCacheType)
		for i := int64(0); i < 10; i++ {
			// 10 million rows, 1 bit per column, random seeded by i
			data := getZipfRowsSliceRoaring(10000000, i, 0, ShardWidth)
			err := f.importRoaringT(tx, data, false)
			if err != nil {
				PanicOn(fmt.Sprintf("setting up fragment data: %v", err))
			}
		}
		err := f.Close()
		if err != nil {
			PanicOn(fmt.Sprintf("closing fragment: %v", err))
		}
		bigFrag = f.path()
		PanicOn(tx.Commit())
	}
}

func BenchmarkImportIntoLargeFragment(b *testing.B) {
	b.StopTimer()
	initBigFrag(b)
	rowsOrig, colsOrig := getUpdataSlices(10000000, 11000, 0)
	rows, cols := make([]uint64, len(rowsOrig)), make([]uint64, len(colsOrig))
	opts := &ImportOptions{}
	for i := 0; i < b.N; i++ {
		origF, err := os.Open(bigFrag)
		if err != nil {
			b.Fatalf("opening frag file: %v", err)
		}
		fi, err := testhook.TempFileInDir(b, *TempDir, "")
		if err != nil {
			b.Fatalf("getting temp file: %v", err)
		}
		_, err = io.Copy(fi, origF)
		if err != nil {
			b.Fatalf("copying fragment file: %v", err)
		}
		origF.Close()
		fi.Close()

		h := NewHolder(fi.Name(), mustHolderConfig())
		PanicOn(h.Open())
		idx, err := h.CreateIndex("i", IndexOptions{})
		PanicOn(err)

		f := newFragment(h, makeTestFragSpec(fi.Name(), "i", "f", viewStandard), 0, 0)
		err = f.Open()
		if err != nil {
			b.Fatalf("opening fragment: %v", err)
		}

		// Obtain transaction.
		tx := idx.holder.txf.NewTx(Txo{Write: writable, Index: idx, Fragment: f, Shard: f.shard})
		defer tx.Rollback()

		copy(rows, rowsOrig)
		copy(cols, colsOrig)
		b.StartTimer()
		err = f.bulkImport(tx, rows, cols, opts)
		b.StopTimer()
		if err != nil {
			b.Fatalf("bulkImport: %v", err)
		}
		PanicOn(tx.Commit())
		f.Clean(b)
		h.Close()
	}
}

func BenchmarkImportRoaringIntoLargeFragment(b *testing.B) {
	b.StopTimer()
	initBigFrag(b)
	updata := getUpdataRoaring(10000000, 11000, 0)
	th := newTestHolder(b)
	for i := 0; i < b.N; i++ {
		origF, err := os.Open(bigFrag)
		if err != nil {
			b.Fatalf("opening frag file: %v", err)
		}
		fi, err := testhook.TempFileInDir(b, *TempDir, "")
		if err != nil {
			b.Fatalf("getting temp file: %v", err)
		}
		_, err = io.Copy(fi, origF)
		if err != nil {
			b.Fatalf("copying fragment file: %v", err)
		}
		origF.Close()
		fi.Close()

		// want to do this, but no path argument.
		//nf, idx, tx := mustOpenFragmentFlags(index, field, view string, shard uint64, cacheType string, flags byte)

		idx := fragTestMustOpenIndex("i", th, IndexOptions{})
		if th.NeedsSnapshot() {
			th.SnapshotQueue = newSnapshotQueue(1, 1, nil)
		}
		// XXX TODO: newFragment is using the wrong path here, we should fix that someday.
		f := newFragment(th, makeTestFragSpec(fi.Name(), "i", "f", viewStandard), 0, 0)
		defer f.Clean(b)

		tx := idx.holder.txf.NewTx(Txo{Write: writable, Index: idx, Fragment: f, Shard: f.shard})
		defer tx.Rollback()

		err = f.Open()
		if err != nil {
			b.Fatalf("opening fragment: %v", err)
		}
		b.StartTimer()
		err = f.importRoaringT(tx, updata, false)
		b.StopTimer()
		if err != nil {
			b.Fatalf("bulkImport: %v", err)
		}
	}
}

func TestGetZipfRowsSliceRoaring(t *testing.T) {
	f, idx, tx := mustOpenFragment(t, "i", "f", viewStandard, 0, DefaultCacheType)
	_ = idx

	data := getZipfRowsSliceRoaring(10, 1, 0, ShardWidth)
	err := f.importRoaringT(tx, data, false)
	if err != nil {
		t.Fatalf("importing roaring: %v", err)
	}
	rows, err := f.rows(context.Background(), tx, 0)
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(rows, []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}) {
		t.Fatalf("unexpected rows: %v", rows)
	}
	for i := uint64(1); i < 10; i++ {
		if f.mustRow(tx, i).Count() >= f.mustRow(tx, i-1).Count() {
			t.Fatalf("suspect distribution from getZipfRowsSliceRoaring")
		}
	}
	f.Clean(t)
}

func prepareSampleRowData(b *testing.B, bits int, rows uint64, width uint64) (*fragment, *Index, Tx) {
	f, idx, tx := mustOpenFragment(b, "i", "f", viewStandard, 0, "none")
	for i := 0; i < bits; i++ {
		data := getUniformRowsSliceRoaring(rows, int64(rows)+int64(i), 0, width)
		err := f.importRoaringT(tx, data, false)
		if err != nil {
			b.Fatalf("creating sample data: %v", err)
		}
	}
	return f, idx, tx
}

type txFrag struct {
	rows, width uint64
	tx          Tx
	idx         *Index
	frag        *fragment
}

func benchmarkRowsOnTestcase(b *testing.B, ctx context.Context, txf txFrag) {
	col := uint64(0)
	for i := 0; i < b.N; i++ {
		_, err := txf.frag.rows(ctx, txf.tx, 0, roaring.NewBitmapColumnFilter(col))
		if err != nil {
			b.Fatalf("retrieving rows for col %d: %v", col, err)
		}
		col = (col + 1) % txf.width
	}
}

// benchmarkRowsMaybeWritable uses a local flag which shadows the package-scope
// const writable. Neat, huh.
func benchmarkRowsMaybeWritable(b *testing.B, writable bool) {
	var depths = []uint64{384, 2048}
	testCases := make([]txFrag, 0, len(depths))
	defer func() {
		for _, testCase := range testCases {
			testCase.frag.Clean(b)
		}
	}()
	bg := context.Background()
	for _, rows := range depths {
		frag, idx, tx := prepareSampleRowData(b, 3, rows, ShardWidth)
		if !writable {
			err := tx.Commit()
			if err != nil {
				b.Fatalf("error committing sample data: %v", err)
			}
			tx = idx.holder.txf.NewTx(Txo{Write: writable, Index: idx, Fragment: frag, Shard: 0})
			defer tx.Rollback()
		}
		testCases = append(testCases, txFrag{
			rows:  rows,
			width: ShardWidth,
			frag:  frag,
			idx:   idx,
			tx:    tx,
		})
	}
	for _, testCase := range testCases {
		b.Run(fmt.Sprintf("%d", testCase.rows), func(b *testing.B) {
			benchmarkRowsOnTestcase(b, bg, testCase)
		})
	}
}
func BenchmarkRows(b *testing.B) {
	b.Run("writable", func(b *testing.B) {
		benchmarkRowsMaybeWritable(b, true)
	})
	b.Run("readonly", func(b *testing.B) {
		benchmarkRowsMaybeWritable(b, false)
	})
}

// getZipfRowsSliceRoaring generates a random fragment with the given number of
// rows, and 1 bit set in each column. The row each bit is set in is chosen via
// the Zipf generator, and so will be skewed toward lower row numbers. If this
// is edited to change the data distribution, getZipfRowsSliceStandard should be
// edited as well. TODO switch to generating row-major for perf boost
func getZipfRowsSliceRoaring(numRows uint64, seed int64, startCol, endCol uint64) []byte {
	b := roaring.NewBTreeBitmap()
	s := rand.NewSource(seed)
	r := rand.New(s)
	z := rand.NewZipf(r, 1.6, 50, numRows-1)
	bufSize := 1 << 14
	posBuf := make([]uint64, 0, bufSize)
	for i := uint64(startCol); i < endCol; i++ {
		row := z.Uint64()
		posBuf = append(posBuf, row*ShardWidth+i)
		if len(posBuf) == bufSize {
			sort.Slice(posBuf, func(i int, j int) bool { return posBuf[i] < posBuf[j] })
			b.DirectAddN(posBuf...)
		}
	}
	b.DirectAddN(posBuf...)
	buf := bytes.NewBuffer(make([]byte, 0, 100000))
	_, err := b.WriteTo(buf)
	if err != nil {
		PanicOn(err)
	}
	return buf.Bytes()
}

// getUniformRowsSliceRoaring produces evenly-distributed values as a roaring
// bitmap slice. This is useful if you want to avoid the higher rows being
// sparse; see also getZipfRowsSliceRoaring.
func getUniformRowsSliceRoaring(numRows uint64, seed int64, startCol, endCol uint64) []byte {
	b := roaring.NewBTreeBitmap()
	s := rand.NewSource(seed)
	r := rand.New(s)
	bufSize := 1 << 14
	posBuf := make([]uint64, 0, bufSize)
	for i := uint64(startCol); i < endCol; i++ {
		row := uint64(r.Int63n(int64(numRows)))
		posBuf = append(posBuf, row*ShardWidth+i)
		if len(posBuf) == bufSize {
			sort.Slice(posBuf, func(i int, j int) bool { return posBuf[i] < posBuf[j] })
			b.DirectAddN(posBuf...)
		}
	}
	b.DirectAddN(posBuf...)
	buf := bytes.NewBuffer(make([]byte, 0, 100000))
	_, err := b.WriteTo(buf)
	if err != nil {
		PanicOn(err)
	}
	return buf.Bytes()
}

func getUpdataSlices(numRows, numCols uint64, seed int64) (rows, cols []uint64) {
	getUpdataInto(func(row, col uint64) bool {
		rows = append(rows, row)
		cols = append(cols, col)
		return true
	}, numRows, numCols, seed)
	return rows, cols
}

// getUpdataRoaring gets a byte slice containing a roaring bitmap which
// represents numCols set bits distributed randomly throughout a shard's column
// space and zipfianly throughout numRows rows.
func getUpdataRoaring(numRows, numCols uint64, seed int64) []byte {
	b := roaring.NewBitmap()

	getUpdataInto(func(row, col uint64) bool {
		return b.DirectAdd(row*ShardWidth + col)
	}, numRows, numCols, seed)

	buf := bytes.NewBuffer(make([]byte, 0, 100000))
	_, err := b.WriteTo(buf)
	if err != nil {
		PanicOn(err)
	}
	return buf.Bytes()
}

func getUpdataInto(f func(row, col uint64) bool, numRows, numCols uint64, seed int64) int {
	s := rand.NewSource(seed)
	r := rand.New(s)
	z := rand.NewZipf(r, 1.6, 50, numRows-1)

	i := uint64(0)
	// ensure we get exactly the number we asked for. it turns out we had
	// a horrible pathological edge case for exactly 10,000 entries in an
	// imported bitmap, and hit it only occasionally...
	for i < numCols {
		col := uint64(r.Int63n(ShardWidth))
		row := z.Uint64()
		if f(row, col) {
			i++
		}
	}
	return int(i)
}

// getZipfRowsSliceStandard is the same as getZipfRowsSliceRoaring, but returns
// row and column ids instead of a byte slice containing roaring bitmap data.
func getZipfRowsSliceStandard(numRows uint64, seed int64, startCol, endCol uint64) (rowIDs, columnIDs []uint64) {
	s := rand.NewSource(seed)
	r := rand.New(s)
	z := rand.NewZipf(r, 1.6, 50, numRows-1)
	rowIDs, columnIDs = make([]uint64, ShardWidth), make([]uint64, ShardWidth)
	for i := uint64(startCol); i < endCol; i++ {
		rowIDs[i] = z.Uint64()
		columnIDs[i] = i
	}
	return rowIDs, columnIDs
}

func BenchmarkFileWrite(b *testing.B) {
	for _, numRows := range rowCases {
		data := getZipfRowsSliceRoaring(numRows, 1, 0, ShardWidth)
		b.Run(fmt.Sprintf("Rows%d", numRows), func(b *testing.B) {
			b.StopTimer()
			for i := 0; i < b.N; i++ {
				// DO NOT CONVERT THIS ONE TO USE TESTHOOK.
				// We're deleting these files as we go because
				// otherwise the benchmark could fill up
				// $TMPDIR before it finishes running.
				f, err := ioutil.TempFile(*TempDir, "")
				if err != nil {
					b.Fatalf("getting temp file: %v", err)
				}
				b.StartTimer()
				_, err = f.Write(data)
				if err != nil {
					os.Remove(f.Name())
					b.Fatal(err)
				}
				err = f.Sync()
				if err != nil {
					os.Remove(f.Name())
					b.Fatal(err)
				}
				err = f.Close()
				if err != nil {
					os.Remove(f.Name())
					b.Fatal(err)
				}
				b.StopTimer()
				os.Remove(f.Name())
			}
		})
	}

}

/////////////////////////////////////////////////////////////////////

// not called under Tx stores b/c f.idx.NeedsSnapshot() in Clean() avoids it.
func (f *fragment) sanityCheck(t testing.TB) {
	newBM := roaring.NewFileBitmap()
	file, err := os.Open(f.path())
	if err != nil {
		t.Fatalf("sanityCheck couldn't open file %s: %v", f.path(), err)
	}
	defer file.Close()
	data, err := ioutil.ReadAll(file)
	if err != nil {
		t.Fatalf("sanityCheck couldn't read fragment %s: %v", f.path(), err)
	}
	err = newBM.UnmarshalBinary(data)
	if err != nil {
		t.Fatalf("sanityCheck couldn't unmarshal fragment %s: %v", f.path(), err)
	}
	// Refactor fragment.storage
	// note: not called for rbf, see above.
	if equal, reason := newBM.BitwiseEqual(f.storage); !equal {
		t.Fatalf("fragment %s: unmarshalled bitmap different: %v", f.path(), reason)
	}
}

// Clean used to delete fragments, but doesn't anymore -- deleting is
// handled by the testhook.TempDir when appropriate.
func (f *fragment) Clean(t testing.TB) {
	f.mu.Lock()
	// we need to ensure that we unlock the mutex before terminating
	// the clean operation, but we need it held during the sanity
	// check or else, in some cases, the background snapshot queue
	// can decide to pick it up.
	func() {
		// should we skip snapshot queue stuff under bolt/rbf?
		defer f.mu.Unlock()

		// rbf doesn't need snapshot, so this stuff is skipped.
		// The snapshot queue stuff doesn't work under rbf.
		if f.idx.NeedsSnapshot() {
			err := f.holder.SnapshotQueue.Await(f)
			if err != nil {
				t.Fatalf("snapshot failed before sanity check: %v", err)
			}
			f.sanityCheck(t)
			if f.storage != nil && f.storage.Source != nil {
				if f.storage.Source.Dead() {
					t.Fatalf("cleaning up fragment %s, source %s, source already dead", f.path(), f.storage.Source.ID())
				}
			}
		}
	}()
	errc := f.Close()
	// prevent double-closes of generation during testing.
	f.gen = nil
	if errc != nil {
		t.Fatalf("error closing fragment: %v", errc)
	}
}

// importRoaringT calls importRoaring with context.Background() for convenience
func (f *fragment) importRoaringT(tx Tx, data []byte, clear bool) error {

	return f.importRoaring(context.Background(), tx, data, clear)
}

// mustOpenFragment returns a new instance of Fragment with a temporary path.
func mustOpenFragment(tb testing.TB, index, field, view string, shard uint64, cacheType string) (*fragment, *Index, Tx) {
	return mustOpenFragmentFlags(tb, index, field, view, shard, cacheType, 0)
}

func mustOpenBSIFragment(tb testing.TB, index, field, view string, shard uint64) (*fragment, *Index, Tx) {
	return mustOpenFragmentFlags(tb, index, field, view, shard, "", 1)
}

func newTestHolder(tb testing.TB) *Holder {
	path, _ := testhook.TempDirInDir(tb, *TempDir, "holder-dir")
	h := NewHolder(path, mustHolderConfig())
	PanicOn(h.Open())
	testhook.Cleanup(tb, func() {
		h.Close()
	})
	//h.SnapshotQueue = newSnapshotQueue(1, 1, nil)
	return h
}

// fragTestMustOpenIndex returns a new, opened index at a temporary path. Panic on error.
func fragTestMustOpenIndex(index string, holder *Holder, opt IndexOptions) *Index {
	cim := &CreateIndexMessage{
		Index:     index,
		CreatedAt: 0,
		Meta:      opt,
	}

	holder.mu.Lock()
	idx, err := holder.createIndex(cim, false)
	holder.mu.Unlock()
	PanicOn(err)

	idx.keys = opt.Keys
	idx.trackExistence = opt.TrackExistence

	if err := idx.Open(); err != nil {
		PanicOn(err)
	}
	return idx
}

// mustOpenFragment returns a new instance of Fragment with a temporary path.
func mustOpenFragmentFlags(tb testing.TB, index, field, view string, shard uint64, cacheType string, flags byte) (*fragment, *Index, Tx) {
	if cacheType == "" {
		cacheType = DefaultCacheType
	}

	th := newTestHolder(tb)
	idx := fragTestMustOpenIndex(index, th, IndexOptions{})
	if th.NeedsSnapshot() {
		th.SnapshotQueue = newSnapshotQueue(1, 1, nil)
	}

	fragDir := fmt.Sprintf("%v/%v/views/%v/fragments/", idx.path, field, view)
	PanicOn(os.MkdirAll(fragDir, 0777))
	fragPath := fragDir + fmt.Sprintf("%v", shard)
	f := newFragment(th, makeTestFragSpec(fragPath, index, field, view), shard, flags)

	tx := idx.holder.txf.NewTx(Txo{Write: writable, Index: idx, Fragment: f, Shard: shard})
	testhook.Cleanup(tb, func() {
		tx.Rollback()
		PanicOn(idx.holder.txf.CloseIndex(idx))
	})

	f.CacheType = cacheType

	if err := f.Open(); err != nil {
		PanicOn(err)
	}
	return f, idx, tx
}

// mustOpenMutexFragment returns a new instance of Fragment for a mutex field.
func mustOpenMutexFragment(tb testing.TB, index, field, view string, shard uint64, cacheType string) (*fragment, *Index, Tx) {
	frag, idx, tx := mustOpenFragment(tb, index, field, view, shard, cacheType)
	frag.mutexVector = newRowsVector(frag)
	return frag, idx, tx
}

// mustOpenBoolFragment returns a new instance of Fragment for a bool field.
func mustOpenBoolFragment(tb testing.TB, index, field, view string, shard uint64, cacheType string) (*fragment, *Index, Tx) {
	frag, idx, tx := mustOpenFragment(tb, index, field, view, shard, cacheType)
	frag.mutexVector = newBoolVector(frag)
	return frag, idx, tx
}

// Reopen closes the fragment and reopens it as a new instance.
func (f *fragment) Reopen() error {
	if err := f.Close(); err != nil {
		return err
	}
	if err := f.Open(); err != nil {
		return err
	}
	return nil
}

// mustSetBits sets columns on a row. Panic on error.
// This function does not accept a timestamp or quantum.
func (f *fragment) mustSetBits(tx Tx, rowID uint64, columnIDs ...uint64) {
	for _, columnID := range columnIDs {
		if _, err := f.setBit(tx, rowID, columnID); err != nil {
			PanicOn(err)
		}
	}
}

func addToBitmap(bm *roaring.Bitmap, rowID uint64, columnIDs ...uint64) {
	// we'll reuse the columnIDs slice and fill it with positions for DirectAddN
	for i, c := range columnIDs {
		columnIDs[i] = rowID*ShardWidth + c%ShardWidth
	}
	bm.DirectAddN(columnIDs...)
}

// Test Various methods of retrieving RowIDs
func TestFragment_RowsIteration(t *testing.T) {
	t.Run("firstContainer", func(t *testing.T) {
		f, idx, tx := mustOpenFragment(t, "i", "f", viewStandard, 0, "")
		_ = idx
		defer f.Clean(t)

		expectedAll := make([]uint64, 0)
		expectedOdd := make([]uint64, 0)
		for i := uint64(100); i < uint64(200); i++ {
			if _, err := f.setBit(tx, i, i%2); err != nil {
				t.Fatal(err)
			}
			expectedAll = append(expectedAll, i)
			if i%2 == 1 {
				expectedOdd = append(expectedOdd, i)
			}
		}

		ids, err := f.rows(context.Background(), tx, 0)
		if err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(expectedAll, ids) {
			t.Fatalf("Do not match %v %v", expectedAll, ids)
		}

		ids, err = f.rows(context.Background(), tx, 0, roaring.NewBitmapColumnFilter(1))
		if err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(expectedOdd, ids) {
			t.Fatalf("Do not match %v %v", expectedOdd, ids)
		}
	})

	t.Run("secondRow", func(t *testing.T) {
		f, idx, tx := mustOpenFragment(t, "i", "f", viewStandard, 0, "")
		_ = idx

		defer f.Clean(t)

		expected := []uint64{1, 2}
		if _, err := f.setBit(tx, 1, 66000); err != nil {
			t.Fatal(err)
		} else if _, err := f.setBit(tx, 2, 66000); err != nil {
			t.Fatal(err)
		} else if _, err := f.setBit(tx, 2, 166000); err != nil {
			t.Fatal(err)
		}
		PanicOn(tx.Commit())

		tx = idx.holder.txf.NewTx(Txo{Write: !writable, Index: idx, Fragment: f, Shard: f.shard})
		defer tx.Rollback()

		ids, err := f.rows(context.Background(), tx, 0)
		if err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(expected, ids) {
			t.Fatalf("Do not match %v %v", expected, ids)
		}

		ids, err = f.rows(context.Background(), tx, 0, roaring.NewBitmapColumnFilter(66000))
		if err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(expected, ids) {
			t.Fatalf("Do not match %v %v", expected, ids)
		}
	})

	t.Run("combinations", func(t *testing.T) {
		f, idx, tx := mustOpenFragment(t, "i", "f", viewStandard, 0, "")
		_ = idx

		defer f.Clean(t)

		expectedRows := make([]uint64, 0)
		for r := uint64(1); r < uint64(10000); r += 250 {
			expectedRows = append(expectedRows, r)
			for c := uint64(1); c < uint64(ShardWidth-1); c += (ShardWidth >> 5) {
				if _, err := f.setBit(tx, r, c); err != nil {
					t.Fatal(err)
				}

				ids, err := f.rows(context.Background(), tx, 0)
				if err != nil {
					t.Fatal(err)
				} else if !reflect.DeepEqual(expectedRows, ids) {
					t.Fatalf("Do not match %v %v", expectedRows, ids)
				}
				ids, err = f.rows(context.Background(), tx, 0, roaring.NewBitmapColumnFilter(c))
				if err != nil {
					t.Fatal(err)
				} else if !reflect.DeepEqual(expectedRows, ids) {
					t.Fatalf("Do not match %v %v", expectedRows, ids)
				}
			}
		}
	})
}

// Test Importing roaring data.
func TestFragment_RoaringImport(t *testing.T) {
	tests := [][][]uint64{
		{
			[]uint64{0},
			[]uint64{1},
		},
		{
			[]uint64{0, 65535, 65536, 65537, 65538, 65539, 130000},
			[]uint64{1000, 67000, 130000},
		},
		{
			[]uint64{0, 65535, 65536, 65537, 65538, 65539, 130000},
			[]uint64{0, 65535, 65536, 65537, 65538, 65539, 130000},
		},
		{
			[]uint64{0, 65535, 65536, 1<<20 + 1, 1<<20*2 + 1},
			[]uint64{1, 1<<20 + 2, 1<<20*2 + 2},
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("importroaring%d", i), func(t *testing.T) {
			f, idx, tx := mustOpenFragment(t, "i", "f", viewStandard, 0, "")
			_ = idx
			defer f.Clean(t)

			for num, input := range test {
				buf := &bytes.Buffer{}
				bm := roaring.NewBitmap(input...)
				_, err := bm.WriteTo(buf)
				if err != nil {
					t.Fatalf("writing to buffer: %v", err)
				}
				err = f.importRoaringT(tx, buf.Bytes(), false)
				if err != nil {
					t.Fatalf("importing roaring: %v", err)
				}

				exp := calcExpected(test[:num+1]...)
				for row, expCols := range exp {
					cols := f.mustRow(tx, uint64(row)).Columns()
					t.Logf("\nrow: %d\n  exp:%v\n  got:%v", row, expCols, cols)
					if !reflect.DeepEqual(cols, expCols) {
						t.Fatalf("input%d, row %d\n  exp:%v\n  got:%v", num, row, expCols, cols)
					}
				}
			}
		})
	}
}

// Test Importing roaring data.
func TestFragment_RoaringImportTopN(t *testing.T) {
	tests := []struct {
		rowIDs  []uint64
		colIDs  []uint64
		rowIDs2 []uint64
		colIDs2 []uint64
		roaring []uint64
	}{
		{
			rowIDs:  []uint64{4, 4, 4, 4},
			colIDs:  []uint64{0, 1, 2, 3},
			rowIDs2: []uint64{5, 5, 5, 5, 5},
			colIDs2: []uint64{0, 1, 2, 3, 4},
			roaring: []uint64{0, 65535, 65536, 1<<20 + 1, 1<<20 + 2, 1<<20*2 + 1},
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("importroaring%d", i), func(t *testing.T) {
			f, idx, tx := mustOpenFragment(t, "i", "f", viewStandard, 0, CacheTypeRanked)
			_ = idx
			defer f.Clean(t)

			options := &ImportOptions{}
			err := f.bulkImport(tx, test.rowIDs, test.colIDs, options)
			if err != nil {
				t.Fatalf("bulk importing ids: %v", err)
			}

			expPairs := calcTop(test.rowIDs, test.colIDs)
			pairs, err := f.top(tx, topOptions{})
			if err != nil {
				t.Fatalf("executing top after bulk import: %v", err)
			}
			if !reflect.DeepEqual(expPairs, pairs) {
				t.Fatalf("post bulk import:\n  exp: %v\n  got: %v\n", expPairs, pairs)
			}

			err = f.bulkImport(tx, test.rowIDs2, test.colIDs2, options)
			if err != nil {
				t.Fatalf("bulk importing ids: %v", err)
			}
			test.rowIDs = append(test.rowIDs, test.rowIDs2...)
			test.colIDs = append(test.colIDs, test.colIDs2...)
			expPairs = calcTop(test.rowIDs, test.colIDs)
			pairs, err = f.top(tx, topOptions{})
			if err != nil {
				t.Fatalf("executing top after bulk import: %v", err)
			}
			if !reflect.DeepEqual(expPairs, pairs) {
				t.Fatalf("post bulk import2:\n  exp: %v\n  got: %v\n", expPairs, pairs)
			}

			buf := &bytes.Buffer{}
			bm := roaring.NewBitmap(test.roaring...)
			_, err = bm.WriteTo(buf)
			if err != nil {
				t.Fatalf("writing to buffer: %v", err)
			}
			err = f.importRoaringT(tx, buf.Bytes(), false)
			if err != nil {
				t.Fatalf("importing roaring: %v", err)
			}
			rows, cols := toRowsCols(test.roaring)
			expPairs = calcTop(append(test.rowIDs, rows...), append(test.colIDs, cols...))
			pairs, err = f.top(tx, topOptions{})
			if err != nil {
				t.Fatalf("executing top after roaring import: %v", err)
			}
			if !reflect.DeepEqual(expPairs, pairs) {
				t.Fatalf("post Roaring:\n  exp: %v\n  got: %v\n", expPairs, pairs)
			}
		})
	}
}

func toRowsCols(roaring []uint64) (rowIDs, colIDs []uint64) {
	rowIDs, colIDs = make([]uint64, len(roaring)), make([]uint64, len(roaring))
	for i, bit := range roaring {
		rowIDs[i] = bit / ShardWidth
		colIDs[i] = bit % ShardWidth
	}
	return rowIDs, colIDs
}

func calcTop(rowIDs, colIDs []uint64) []Pair {
	if len(rowIDs) != len(colIDs) {
		PanicOn("row and col ids must be of equal len")
	}
	// make map of rowID to colID set in order to dedup
	counts := make(map[uint64]map[uint64]struct{})
	for i := 0; i < len(rowIDs); i++ {
		row, col := rowIDs[i], colIDs[i]
		m, ok := counts[row]
		if !ok {
			m = make(map[uint64]struct{})
			counts[row] = m
		}
		m[col] = struct{}{}
	}

	// build slice of pairs from map
	ret := make([]Pair, 0)
	for row, cols := range counts {
		ret = append(ret, Pair{ID: row, Count: uint64(len(cols))})
	}

	// reverse sort by count
	sort.Slice(ret, func(i, j int) bool { return ret[i].Count > ret[j].Count })
	return ret
}

// calcExpected takes a number of slices of uint64 represented data to be added
// to a fragment. It calculates which rows and bits set in each row would be
// expected after importing that data, and returns a [][]uint64 where the index
// into the first slice is row id.
func calcExpected(inputs ...[]uint64) [][]uint64 {
	// create map of row id to set of column values in that row.
	rows := make(map[uint64]map[uint64]struct{})
	var maxrow uint64
	for _, input := range inputs {
		for _, val := range input {
			row := val / ShardWidth
			if row > maxrow {
				maxrow = row
			}
			m, ok := rows[row]
			if !ok {
				m = make(map[uint64]struct{})
				rows[row] = m
			}
			m[val%ShardWidth] = struct{}{}
		}
	}

	// initialize ret slice
	ret := make([][]uint64, maxrow+1)
	for i := range ret {
		ret[i] = make([]uint64, 0)
	}

	// populate ret slices from rows map
	for row, vals := range rows {
		for val := range vals {
			ret[row] = append(ret[row], val)
		}
	}

	// sort ret slices
	for _, slice := range ret {
		sort.Slice(slice, func(i int, j int) bool { return slice[i] < slice[j] })
	}

	return ret
}

func TestFragmentRowIterator(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		f, idx, tx := mustOpenFragment(t, "i", "f", "v", 0, CacheTypeRanked)
		_ = idx

		defer f.Clean(t)

		f.mustSetBits(tx, 0, 0)
		f.mustSetBits(tx, 1, 0)
		f.mustSetBits(tx, 2, 0)
		f.mustSetBits(tx, 3, 0)

		iter, err := f.rowIterator(tx, false)
		if err != nil {
			t.Fatal(err)
		}
		for i := uint64(0); i < 4; i++ {
			row, id, _, wrapped, err := iter.Next()
			if err != nil {
				t.Fatal(err)
			}
			if id != i {
				t.Fatalf("expected row %d but got %d", i, id)
			}
			if wrapped {
				t.Fatalf("shouldn't have wrapped")
			}
			if !reflect.DeepEqual(row.Columns(), []uint64{0}) {
				t.Fatalf("got wrong columns back on iteration %d - should just be 0 but %v", i, row.Columns())
			}
		}
		row, id, _, wrapped, err := iter.Next()
		if err != nil {
			t.Fatal(err)
		}
		if row != nil {
			t.Fatalf("row should be nil after iterator is exhausted, got %v", row.Columns())
		}
		if id != 0 {
			t.Fatalf("id should be 0 after iterator is exhausted, got %d", id)
		}
		if !wrapped {
			t.Fatalf("wrapped should be true after iterator is exhausted")
		}
	})

	t.Run("skipped rows", func(t *testing.T) {
		f, idx, tx := mustOpenFragment(t, "i", "f", "v", 0, CacheTypeRanked)
		_ = idx
		defer f.Clean(t)

		f.mustSetBits(tx, 1, 0)
		f.mustSetBits(tx, 3, 0)
		f.mustSetBits(tx, 5, 0)
		f.mustSetBits(tx, 7, 0)

		iter, err := f.rowIterator(tx, false)
		if err != nil {
			t.Fatal(err)
		}
		for i := uint64(1); i < 8; i += 2 {
			row, id, _, wrapped, err := iter.Next()
			if err != nil {
				t.Fatal(err)
			}
			if id != i {
				t.Fatalf("expected row %d but got %d", i, id)
			}
			if wrapped {
				t.Fatalf("shouldn't have wrapped")
			}
			if !reflect.DeepEqual(row.Columns(), []uint64{0}) {
				t.Fatalf("got wrong columns back on iteration %d - should just be 0 but %v", i, row.Columns())
			}
		}
		row, id, _, wrapped, err := iter.Next()
		if err != nil {
			t.Fatal(err)
		}
		if row != nil {
			t.Fatalf("row should be nil after iterator is exhausted, got %v", row.Columns())
		}
		if id != 0 {
			t.Fatalf("id should be 0 after iterator is exhausted, got %d", id)
		}
		if !wrapped {
			t.Fatalf("wrapped should be true after iterator is exhausted")
		}
	})

	t.Run("basic wrapped", func(t *testing.T) {
		f, idx, tx := mustOpenFragment(t, "i", "f", "v", 0, CacheTypeRanked)
		_ = idx
		defer f.Clean(t)

		f.mustSetBits(tx, 0, 0)
		f.mustSetBits(tx, 1, 0)
		f.mustSetBits(tx, 2, 0)
		f.mustSetBits(tx, 3, 0)

		iter, err := f.rowIterator(tx, true)
		if err != nil {
			t.Fatal(err)
		}
		for i := uint64(0); i < 5; i++ {
			row, id, _, wrapped, err := iter.Next()
			if err != nil {
				t.Fatal(err)
			}
			if id != i%4 {
				t.Fatalf("expected row %d but got %d", i%4, id)
			}
			if wrapped && i < 4 {
				t.Fatalf("shouldn't have wrapped")
			} else if !wrapped && i >= 4 {
				t.Fatalf("should have wrapped")
			}
			if !reflect.DeepEqual(row.Columns(), []uint64{0}) {
				t.Fatalf("got wrong columns back on iteration %d - should just be 0 but %v", i, row.Columns())
			}
		}
	})

	t.Run("skipped rows wrapped", func(t *testing.T) {
		f, _, tx := mustOpenFragment(t, "i", "f", "v", 0, CacheTypeRanked)
		defer f.Clean(t)

		f.mustSetBits(tx, 1, 0)
		f.mustSetBits(tx, 3, 0)
		f.mustSetBits(tx, 5, 0)
		f.mustSetBits(tx, 7, 0)

		iter, err := f.rowIterator(tx, true)
		if err != nil {
			t.Fatal(err)
		}
		for i := uint64(1); i < 10; i += 2 {
			row, id, _, wrapped, err := iter.Next()
			if err != nil {
				t.Fatal(err)
			}
			if id != i%8 {
				t.Errorf("expected row %d but got %d", i%8, id)
			}
			if wrapped && i < 8 {
				t.Errorf("shouldn't have wrapped")
			} else if !wrapped && i >= 8 {
				t.Errorf("should have wrapped")
			}
			if !reflect.DeepEqual(row.Columns(), []uint64{0}) {
				t.Fatalf("got wrong columns back on iteration %d - should just be 0 but %v", i, row.Columns())
			}
		}
	})
}

// same, with commits
func TestFragmentRowIterator_WithTxCommit(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		f, idx, tx := mustOpenFragment(t, "i", "f", "v", 0, CacheTypeRanked)
		_ = idx

		defer f.Clean(t)

		f.mustSetBits(tx, 0, 0)
		f.mustSetBits(tx, 1, 0)
		f.mustSetBits(tx, 2, 0)
		f.mustSetBits(tx, 3, 0)

		PanicOn(tx.Commit())
		tx = idx.holder.txf.NewTx(Txo{Write: !writable, Index: idx, Fragment: f, Shard: f.shard})
		defer tx.Rollback()

		iter, err := f.rowIterator(tx, false)
		if err != nil {
			t.Fatal(err)
		}
		for i := uint64(0); i < 4; i++ {
			row, id, _, wrapped, err := iter.Next()
			if err != nil {
				t.Fatal(err)
			}
			if id != i {
				t.Fatalf("expected row %d but got %d", i, id)
			}
			if wrapped {
				t.Fatalf("shouldn't have wrapped")
			}
			if !reflect.DeepEqual(row.Columns(), []uint64{0}) {
				t.Fatalf("got wrong columns back on iteration %d - should just be 0 but %v", i, row.Columns())
			}
		}
		row, id, _, wrapped, err := iter.Next()
		if err != nil {
			t.Fatal(err)
		}
		if row != nil {
			t.Fatalf("row should be nil after iterator is exhausted, got %v", row.Columns())
		}
		if id != 0 {
			t.Fatalf("id should be 0 after iterator is exhausted, got %d", id)
		}
		if !wrapped {
			t.Fatalf("wrapped should be true after iterator is exhausted")
		}
	})

	t.Run("skipped rows", func(t *testing.T) {
		f, idx, tx := mustOpenFragment(t, "i", "f", "v", 0, CacheTypeRanked)
		_ = idx
		defer f.Clean(t)

		f.mustSetBits(tx, 1, 0)
		f.mustSetBits(tx, 3, 0)
		f.mustSetBits(tx, 5, 0)
		f.mustSetBits(tx, 7, 0)

		PanicOn(tx.Commit())
		tx = idx.holder.txf.NewTx(Txo{Write: !writable, Index: idx, Fragment: f, Shard: f.shard})
		defer tx.Rollback()

		iter, err := f.rowIterator(tx, false)
		if err != nil {
			t.Fatal(err)
		}
		for i := uint64(1); i < 8; i += 2 {
			row, id, _, wrapped, err := iter.Next()
			if err != nil {
				t.Fatal(err)
			}
			if id != i {
				t.Fatalf("expected row %d but got %d", i, id)
			}
			if wrapped {
				t.Fatalf("shouldn't have wrapped")
			}
			if !reflect.DeepEqual(row.Columns(), []uint64{0}) {
				t.Fatalf("got wrong columns back on iteration %d - should just be 0 but %v", i, row.Columns())
			}
		}
		row, id, _, wrapped, err := iter.Next()
		if err != nil {
			t.Fatal(err)
		}
		if row != nil {
			t.Fatalf("row should be nil after iterator is exhausted, got %v", row.Columns())
		}
		if id != 0 {
			t.Fatalf("id should be 0 after iterator is exhausted, got %d", id)
		}
		if !wrapped {
			t.Fatalf("wrapped should be true after iterator is exhausted")
		}
	})

	t.Run("basic wrapped", func(t *testing.T) {
		f, idx, tx := mustOpenFragment(t, "i", "f", "v", 0, CacheTypeRanked)
		_ = idx
		defer f.Clean(t)

		f.mustSetBits(tx, 0, 0)
		f.mustSetBits(tx, 1, 0)
		f.mustSetBits(tx, 2, 0)
		f.mustSetBits(tx, 3, 0)

		PanicOn(tx.Commit())
		tx = idx.holder.txf.NewTx(Txo{Write: !writable, Index: idx, Fragment: f, Shard: f.shard})
		defer tx.Rollback()

		iter, err := f.rowIterator(tx, true)
		if err != nil {
			t.Fatal(err)
		}
		for i := uint64(0); i < 5; i++ {
			row, id, _, wrapped, err := iter.Next()
			if err != nil {
				t.Fatal(err)
			}
			if id != i%4 {
				t.Fatalf("expected row %d but got %d", i%4, id)
			}
			if wrapped && i < 4 {
				t.Fatalf("shouldn't have wrapped")
			} else if !wrapped && i >= 4 {
				t.Fatalf("should have wrapped")
			}
			if !reflect.DeepEqual(row.Columns(), []uint64{0}) {
				t.Fatalf("got wrong columns back on iteration %d - should just be 0 but %v", i, row.Columns())
			}
		}
	})

	t.Run("skipped rows wrapped", func(t *testing.T) {
		f, idx, tx := mustOpenFragment(t, "i", "f", "v", 0, CacheTypeRanked)
		_ = idx
		defer f.Clean(t)

		f.mustSetBits(tx, 1, 0)
		f.mustSetBits(tx, 3, 0)
		f.mustSetBits(tx, 5, 0)
		f.mustSetBits(tx, 7, 0)

		PanicOn(tx.Commit())
		tx = idx.holder.txf.NewTx(Txo{Write: !writable, Index: idx, Fragment: f, Shard: f.shard})
		defer tx.Rollback()

		iter, err := f.rowIterator(tx, true)
		if err != nil {
			t.Fatal(err)
		}
		for i := uint64(1); i < 10; i += 2 {
			row, id, _, wrapped, err := iter.Next()
			if err != nil {
				t.Fatal(err)
			} else if row == nil {
				t.Fatal("expected row")
			}
			if id != i%8 {
				t.Errorf("expected row %d but got %d", i%8, id)
			}
			if wrapped && i < 8 {
				t.Errorf("shouldn't have wrapped")
			} else if !wrapped && i >= 8 {
				t.Errorf("should have wrapped")
			}
			if !reflect.DeepEqual(row.Columns(), []uint64{0}) {
				t.Fatalf("got wrong columns back on iteration %d - should just be 0 but got %v", i, row.Columns())
			}
		}
	})
}

func TestUnionInPlaceMapped(t *testing.T) {
	roaringOnlyTest(t)

	f, _, _ := mustOpenFragment(t, "i", "f", "v", 0, CacheTypeNone)
	// note: clean has to be deferred first, because it has to run with
	// the lock *not* held, because it is sometimes so it has to grab the
	// lock...
	defer f.Clean(t)

	f.mu.Lock()
	defer f.mu.Unlock()
	r0 := rand.New(rand.NewSource(2))
	r1 := rand.New(rand.NewSource(1))
	data0 := randPositions(1000000, r0)
	setBM0 := roaring.NewBitmap()
	setBM0.OpWriter = nil
	_, err := setBM0.Add(data0...)
	if err != nil {
		t.Fatalf("adding bits: %v", err)
	}
	count0 := setBM0.Count()

	data1 := randPositions(1000000, r1)
	setBM1 := roaring.NewBitmap()
	setBM1.OpWriter = nil
	_, err = setBM1.Add(data1...)
	if err != nil {
		t.Fatalf("adding bits: %v", err)
	}
	count1 := setBM1.Count()

	// now we write setBM0 into f.storage.
	_, err = unprotectedWriteToFragment(f, setBM0)
	if err != nil {
		t.Fatalf("trying to flush fragment to disk: %v", err)
	}
	countF := f.storage.Count()

	f.storage.UnionInPlace(setBM1)
	countUnion := f.storage.Count()

	// UnionInPlace produces no ops log, we have to make it snapshot, to
	// ensure that the on-disk representation is correct. Note, UIP is
	// not used for things that are modifying real fragments, usually;
	// it's used only in computation of things that usually don't go to
	// disk, which is why we handle this specially in testing and not
	// generically.
	err = f.holder.SnapshotQueue.Immediate(f)
	if err != nil {
		t.Fatalf("snapshot after union-in-place: %v", err)
	}

	if count0 != countF {
		t.Fatalf("writing bitmap to storage changed count: %d => %d", count0, countF)
	}
	min := count0
	if count1 > min {
		min = count1
	}
	max := count0 + count1
	// We don't know how many bits we should have, because of overlap,
	// but it should be between the size of the largest bitmap and the
	// sum of the bitmaps.
	if countUnion < min || countUnion > max {
		t.Fatalf("union of sets with cardinality %d and %d should be between %d and %d, got %d",
			count0, count1, min, max, countUnion)
	}
}

func randPositions(n int, r *rand.Rand) []uint64 {
	ret := make([]uint64, n)
	for i := 0; i < n; i++ {
		ret[i] = uint64(r.Int63n(ShardWidth))
	}
	return ret
}

func TestFragmentPositionsForValue(t *testing.T) {
	f, _, _ := mustOpenFragment(t, "i", "f", "v", 0, CacheTypeNone)
	defer f.Clean(t)

	tests := []struct {
		columnID uint64
		bitDepth uint64
		value    int64
		clear    bool
		toSet    []uint64
		toClear  []uint64
	}{
		{
			columnID: 0,
			bitDepth: 1,
			value:    0,
			toSet:    []uint64{0},                          // exists bit only
			toClear:  []uint64{ShardWidth, ShardWidth * 2}, // sign bit & 1-position
		},
		{
			columnID: 0,
			bitDepth: 3,
			value:    0,
			toSet:    []uint64{0},                                                              // exists bit only
			toClear:  []uint64{ShardWidth * 1, ShardWidth * 2, ShardWidth * 3, ShardWidth * 4}, // sign bit, 1, 2, 4
		},
		{
			columnID: 1,
			bitDepth: 3,
			value:    0,
			toSet:    []uint64{1},                                                                    // exists bit only
			toClear:  []uint64{ShardWidth + 1, ShardWidth*2 + 1, ShardWidth*3 + 1, ShardWidth*4 + 1}, // sign bit, 1, 2, 4
		},
		{
			columnID: 0,
			bitDepth: 1,
			value:    1,
			toSet:    []uint64{0, ShardWidth * 2}, // exists bit, 1
			toClear:  []uint64{ShardWidth},        // sign bit only
		},
		{
			columnID: 0,
			bitDepth: 4,
			value:    10,
			toSet:    []uint64{0, ShardWidth * 3, ShardWidth * 5},              // exists bit, 2, 8
			toClear:  []uint64{ShardWidth * 1, ShardWidth * 2, ShardWidth * 4}, // sign bit, 1, 4
		},
		{
			columnID: 0,
			bitDepth: 5,
			value:    10,
			toSet:    []uint64{0, ShardWidth * 3, ShardWidth * 5},                              // exists bit, 2, 8
			toClear:  []uint64{ShardWidth * 1, ShardWidth * 2, ShardWidth * 4, ShardWidth * 6}, // sign bit, 1, 4, 16
		},
	}

	for i, test := range tests {
		toSet, toClear := make([]uint64, 0), make([]uint64, 0)
		var err error
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			toSet, toClear, err = f.positionsForValue(test.columnID, test.bitDepth, test.value, test.clear, toSet, toClear)
			if err != nil {
				t.Fatalf("getting positions: %v", err)
			}

			if len(toSet) != len(test.toSet) || len(toClear) != len(test.toClear) {
				t.Fatalf("differing lengths (exp/got): set\n%v\n%v\nclear\n%v\n%v\n", test.toSet, toSet, test.toClear, toClear)
			}
			for i := 0; i < len(toSet); i++ {
				if toSet[i] != test.toSet[i] {
					t.Errorf("toSet don't match at %d - exp:%d and got:%d", i, test.toSet[i], toSet[i])
				}
			}
			for i := 0; i < len(toClear); i++ {
				if toClear[i] != test.toClear[i] {
					t.Errorf("toClear don't match at %d - exp:%d and got:%d", i, test.toClear[i], toClear[i])
				}
			}
		})
	}
}

func TestIntLTRegression(t *testing.T) {
	f, idx, tx := mustOpenFragment(t, "i", "f", "v", 0, CacheTypeNone)
	_ = idx
	defer f.Clean(t)

	_, err := f.setValue(tx, 1, 6, 33)
	if err != nil {
		t.Fatalf("setting value: %v", err)
	}

	row, err := f.rangeOp(tx, pql.LT, 6, 33)
	if err != nil {
		t.Fatalf("doing range of: %v", err)
	}

	if !row.IsEmpty() {
		t.Errorf("expected nothing, but got: %v", row.Columns())
	}
}

func sliceEq(x, y []uint64) bool {
	if len(x) == 0 {
		x = nil
	}
	if len(y) == 0 {
		y = nil
	}
	return reflect.DeepEqual(x, y)
}

func TestFragmentBSIUnsigned(t *testing.T) {
	shard := uint64(0)
	f, idx, tx := mustOpenFragment(t, "i", "f", "v", shard, CacheTypeNone)
	defer f.Clean(t)

	// Number of bits to test.
	const k = 6

	// Load all numbers into an effectively diagonal matrix.
	for i := 0; i < 1<<k; i++ {
		ok, err := f.setValue(tx, uint64(i), k, int64(i))
		if err != nil {
			t.Fatalf("failed to set col %d to %d: %v", uint64(i), int64(i), err)
		} else if !ok {
			t.Fatalf("no change when setting col %d to %d", uint64(i), int64(i))
		}
	}
	PanicOn(tx.Commit()) // t.Run beolow on different goro and so need their own Tx anyway.

	// Generate a list of columns.
	cols := make([]uint64, 1<<k)
	for i := range cols {
		cols[i] = uint64(i)
	}

	// Minimum and maximum checking bounds.
	// This is mostly arbitrary.
	minCheck, maxCheck := -3, 1<<(k+1)

	t.Run("<", func(t *testing.T) {

		tx := idx.holder.txf.NewTx(Txo{Write: !writable, Index: idx, Fragment: f, Shard: shard})
		defer tx.Rollback()

		for i := minCheck; i < maxCheck; i++ {
			row, err := f.rangeLT(tx, k, int64(i), false)
			if err != nil {
				t.Fatalf("failed to query fragment: %v", err)
			}
			var expect []uint64
			switch {
			case i < 0:
			case i < len(cols):
				expect = cols[:i]
			default:
				expect = cols
			}
			got := row.Columns()
			if !sliceEq(expect, got) {
				t.Errorf("expected %v but got %v for x < %d", expect, got, i)
			}
		}
	})
	t.Run("<=", func(t *testing.T) {

		tx := idx.holder.txf.NewTx(Txo{Write: !writable, Index: idx, Fragment: f, Shard: shard})
		defer tx.Rollback()

		for i := minCheck; i < maxCheck; i++ {
			row, err := f.rangeLT(tx, k, int64(i), true)
			if err != nil {
				t.Fatalf("failed to query fragment: %v", err)
			}
			var expect []uint64
			switch {
			case i < 0:
			case i < len(cols)-1:
				expect = cols[:i+1]
			default:
				expect = cols
			}
			got := row.Columns()
			if !sliceEq(expect, got) {
				t.Errorf("expected %v but got %v for x <= %d", expect, got, i)
			}
		}
	})
	t.Run(">", func(t *testing.T) {

		tx := idx.holder.txf.NewTx(Txo{Write: !writable, Index: idx, Fragment: f, Shard: shard})
		defer tx.Rollback()

		for i := minCheck; i < maxCheck; i++ {
			row, err := f.rangeGT(tx, k, int64(i), false)
			if err != nil {
				t.Fatalf("failed to query fragment: %v", err)
			}
			var expect []uint64
			switch {
			case i < 0:
				expect = cols
			case i < len(cols)-1:
				expect = cols[i+1:]
			default:
			}
			got := row.Columns()
			if !sliceEq(expect, got) {
				t.Errorf("expected %v but got %v for x > %d", expect, got, i)
			}
		}
	})
	t.Run(">=", func(t *testing.T) {
		tx := idx.holder.txf.NewTx(Txo{Write: !writable, Index: idx, Fragment: f, Shard: shard})
		defer tx.Rollback()

		for i := minCheck; i < maxCheck; i++ {
			row, err := f.rangeGT(tx, k, int64(i), true)
			if err != nil {
				t.Fatalf("failed to query fragment: %v", err)
			}
			var expect []uint64
			switch {
			case i < 0:
				expect = cols
			case i < len(cols):
				expect = cols[i:]
			default:
			}
			got := row.Columns()
			if !sliceEq(expect, got) {
				t.Errorf("expected %v but got %v for x >= %d", expect, got, i)
			}
		}
	})
	t.Run("Range", func(t *testing.T) {

		tx := idx.holder.txf.NewTx(Txo{Write: !writable, Index: idx, Fragment: f, Shard: shard})
		defer tx.Rollback()

		for i := minCheck; i < maxCheck; i++ {
			for j := i; j < maxCheck; j++ {
				row, err := f.rangeBetween(tx, k, int64(i), int64(j))
				if err != nil {
					t.Fatalf("failed to query fragment: %v", err)
				}
				var lower, upper int
				switch {
				case i < 0:
					lower = 0
				case i > len(cols):
					lower = len(cols)
				default:
					lower = i
				}
				switch {
				case j < 0:
					upper = 0
				case j >= len(cols):
					upper = len(cols)
				default:
					upper = j + 1
				}
				expect := cols[lower:upper]
				got := row.Columns()
				if !sliceEq(expect, got) {
					t.Errorf("expected %v but got %v for %d <= x <= %d", expect, got, i, j)
				}
			}
		}
	})
	t.Run("==", func(t *testing.T) {

		tx := idx.holder.txf.NewTx(Txo{Write: !writable, Index: idx, Fragment: f, Shard: shard})
		defer tx.Rollback()

		for i := minCheck; i < maxCheck; i++ {
			row, err := f.rangeEQ(tx, k, int64(i))
			if err != nil {
				t.Fatalf("failed to query fragment: %v", err)
			}
			var expect []uint64
			if i >= 0 && i < len(cols) {
				expect = cols[i : i+1]
			}
			got := row.Columns()
			if !sliceEq(expect, got) {
				t.Errorf("expected %v but got %v for x == %d", expect, got, i)
			}
		}
	})
}

// same, WithTxCommit version
func TestFragmentBSIUnsigned_WithTxCommit(t *testing.T) {
	f, idx, tx := mustOpenFragment(t, "i", "f", "v", 0, CacheTypeNone)
	_ = idx
	defer f.Clean(t)

	// Number of bits to test.
	const k = 6

	// Load all numbers into an effectively diagonal matrix.
	for i := 0; i < 1<<k; i++ {
		ok, err := f.setValue(tx, uint64(i), k, int64(i))
		if err != nil {
			t.Fatalf("failed to set col %d to %d: %v", uint64(i), int64(i), err)
		} else if !ok {
			t.Fatalf("no change when setting col %d to %d", uint64(i), int64(i))
		}
	}

	PanicOn(tx.Commit())
	tx = idx.holder.txf.NewTx(Txo{Write: !writable, Index: idx, Fragment: f, Shard: f.shard})
	defer tx.Rollback()

	// Generate a list of columns.
	cols := make([]uint64, 1<<k)
	for i := range cols {
		cols[i] = uint64(i)
	}

	// Minimum and maximum checking bounds.
	// This is mostly arbitrary.
	minCheck, maxCheck := -3, 1<<(k+1)

	t.Run("<", func(t *testing.T) {
		for i := minCheck; i < maxCheck; i++ {
			row, err := f.rangeLT(tx, k, int64(i), false)
			if err != nil {
				t.Fatalf("failed to query fragment: %v", err)
			}
			var expect []uint64
			switch {
			case i < 0:
			case i < len(cols):
				expect = cols[:i]
			default:
				expect = cols
			}
			got := row.Columns()
			if !sliceEq(expect, got) {
				t.Errorf("expected %v but got %v for x < %d", expect, got, i)
			}
		}
	})
	t.Run("<=", func(t *testing.T) {
		for i := minCheck; i < maxCheck; i++ {
			row, err := f.rangeLT(tx, k, int64(i), true)
			if err != nil {
				t.Fatalf("failed to query fragment: %v", err)
			}
			var expect []uint64
			switch {
			case i < 0:
			case i < len(cols)-1:
				expect = cols[:i+1]
			default:
				expect = cols
			}
			got := row.Columns()
			if !sliceEq(expect, got) {
				t.Errorf("expected %v but got %v for x <= %d", expect, got, i)
			}
		}
	})
	t.Run(">", func(t *testing.T) {
		for i := minCheck; i < maxCheck; i++ {
			row, err := f.rangeGT(tx, k, int64(i), false)
			if err != nil {
				t.Fatalf("failed to query fragment: %v", err)
			}
			var expect []uint64
			switch {
			case i < 0:
				expect = cols
			case i < len(cols)-1:
				expect = cols[i+1:]
			default:
			}
			got := row.Columns()
			if !sliceEq(expect, got) {
				t.Errorf("expected %v but got %v for x > %d", expect, got, i)
			}
		}
	})
	t.Run(">=", func(t *testing.T) {
		for i := minCheck; i < maxCheck; i++ {
			row, err := f.rangeGT(tx, k, int64(i), true)
			if err != nil {
				t.Fatalf("failed to query fragment: %v", err)
			}
			var expect []uint64
			switch {
			case i < 0:
				expect = cols
			case i < len(cols):
				expect = cols[i:]
			default:
			}
			got := row.Columns()
			if !sliceEq(expect, got) {
				t.Errorf("expected %v but got %v for x >= %d", expect, got, i)
			}
		}
	})
	t.Run("Range", func(t *testing.T) {
		for i := minCheck; i < maxCheck; i++ {
			for j := i; j < maxCheck; j++ {
				row, err := f.rangeBetween(tx, k, int64(i), int64(j))
				if err != nil {
					t.Fatalf("failed to query fragment: %v", err)
				}
				var lower, upper int
				switch {
				case i < 0:
					lower = 0
				case i > len(cols):
					lower = len(cols)
				default:
					lower = i
				}
				switch {
				case j < 0:
					upper = 0
				case j >= len(cols):
					upper = len(cols)
				default:
					upper = j + 1
				}
				expect := cols[lower:upper]
				got := row.Columns()
				if !sliceEq(expect, got) {
					t.Errorf("expected %v but got %v for %d <= x <= %d", expect, got, i, j)
				}
			}
		}
	})
	t.Run("==", func(t *testing.T) {
		for i := minCheck; i < maxCheck; i++ {
			row, err := f.rangeEQ(tx, k, int64(i))
			if err != nil {
				t.Fatalf("failed to query fragment: %v", err)
			}
			var expect []uint64
			if i >= 0 && i < len(cols) {
				expect = cols[i : i+1]
			}
			got := row.Columns()
			if !sliceEq(expect, got) {
				t.Errorf("expected %v but got %v for x == %d", expect, got, i)
			}
		}
	})
}

func TestFragmentBSISigned(t *testing.T) {
	f, idx, tx := mustOpenFragment(t, "i", "f", "v", 0, CacheTypeNone)
	_ = idx
	defer f.Clean(t)

	// Number of bits to test.
	const k = 6

	// Load all numbers into an effectively diagonal matrix.
	minVal, maxVal := 1-(1<<k), (1<<k)-1
	for i := minVal; i <= maxVal; i++ {
		ok, err := f.setValue(tx, uint64(i-minVal), k, int64(i))
		if err != nil {
			t.Fatalf("failed to set col %d to %d: %v", uint64(i-minVal), int64(i), err)
		} else if !ok {
			t.Fatalf("no change when setting col %d to %d", uint64(i-minVal), int64(i))
		}
	}

	PanicOn(tx.Commit())
	tx = idx.holder.txf.NewTx(Txo{Write: !writable, Index: idx, Fragment: f, Shard: f.shard})
	defer tx.Rollback()

	// Generate a list of columns.
	cols := make([]uint64, (maxVal-minVal)+1)
	for i := range cols {
		cols[i] = uint64(i)
	}

	// Minimum and maximum checking bounds.
	// This is mostly arbitrary.
	minCheck, maxCheck := 2*minVal, 2*maxVal

	t.Run("<", func(t *testing.T) {
		for i := minCheck; i < maxCheck; i++ {
			row, err := f.rangeLT(tx, k, int64(i), false)
			if err != nil {
				t.Fatalf("failed to query fragment: %v", err)
			}
			var expect []uint64
			switch {
			case i < minVal:
			case i <= maxVal:
				expect = cols[:i-minVal]
			default:
				expect = cols
			}
			got := row.Columns()
			if !sliceEq(expect, got) {
				t.Errorf("expected %v but got %v for x < %d", expect, got, i)
			}
		}
	})
	t.Run("<=", func(t *testing.T) {
		for i := minCheck; i < maxCheck; i++ {
			row, err := f.rangeLT(tx, k, int64(i), true)
			if err != nil {
				t.Fatalf("failed to query fragment: %v", err)
			}
			var expect []uint64
			switch {
			case i < minVal:
			case i < maxVal:
				expect = cols[:(i-minVal)+1]
			default:
				expect = cols
			}
			got := row.Columns()
			if !sliceEq(expect, got) {
				t.Errorf("expected %v but got %v for x <= %d", expect, got, i)
			}
		}
	})
	t.Run(">", func(t *testing.T) {
		for i := minCheck; i < maxCheck; i++ {
			row, err := f.rangeGT(tx, k, int64(i), false)
			if err != nil {
				t.Fatalf("failed to query fragment: %v", err)
			}
			var expect []uint64
			switch {
			case i < minVal:
				expect = cols
			case i < maxVal:
				expect = cols[(i-minVal)+1:]
			default:
			}
			got := row.Columns()
			if !sliceEq(expect, got) {
				t.Errorf("expected %v but got %v for x > %d", expect, got, i)
			}
		}
	})
	t.Run(">=", func(t *testing.T) {
		for i := minCheck; i < maxCheck; i++ {
			row, err := f.rangeGT(tx, k, int64(i), true)
			if err != nil {
				t.Fatalf("failed to query fragment: %v", err)
			}
			var expect []uint64
			switch {
			case i < minVal:
				expect = cols
			case i <= maxVal:
				expect = cols[i-minVal:]
			default:
			}
			got := row.Columns()
			if !sliceEq(expect, got) {
				t.Errorf("expected %v but got %v for x >= %d", expect, got, i)
			}
		}
	})
	t.Run("Range", func(t *testing.T) {
		for i := minCheck; i < maxCheck; i++ {
			for j := i; j < maxCheck; j++ {
				row, err := f.rangeBetween(tx, k, int64(i), int64(j))
				if err != nil {
					t.Fatalf("failed to query fragment: %v", err)
				}
				var lower, upper int
				switch {
				case i < minVal:
					lower = 0
				case i > maxVal:
					lower = len(cols)
				default:
					lower = i - minVal
				}
				switch {
				case j < minVal:
					upper = 0
				case j > maxVal:
					upper = len(cols)
				default:
					upper = (j - minVal) + 1
				}
				expect := cols[lower:upper]
				got := row.Columns()
				if !sliceEq(expect, got) {
					t.Errorf("expected %v but got %v for %d <= x <= %d", expect, got, i, j)
				}
			}
		}
	})
	t.Run("==", func(t *testing.T) {
		for i := minCheck; i < maxCheck; i++ {
			row, err := f.rangeEQ(tx, k, int64(i))
			if err != nil {
				t.Fatalf("failed to query fragment: %v", err)
			}
			var expect []uint64
			if i >= minVal && i <= maxVal {
				expect = cols[i-minVal : (i-minVal)+1]
			}
			got := row.Columns()
			if !sliceEq(expect, got) {
				t.Errorf("expected %v but got %v for x == %d", expect, got, i)
			}
		}
	})
}

func TestImportClearRestart(t *testing.T) {
	roaringOnlyTest(t)

	tests := []struct {
		rows []uint64
		cols []uint64
	}{
		{
			rows: []uint64{1},
			cols: []uint64{1},
		},
		{
			rows: []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 1},
			cols: []uint64{1, 2, 3, 4, 5, 6, 7, 8, 9, 500000},
		},
		{
			rows: []uint64{0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
			cols: []uint64{0, 65535, 65536, 131071, 131072, 196607, 196608, 262143, 262144, 1000000},
		},
		{
			rows: []uint64{1, 2, 20, 200, 2000, 200000},
			cols: []uint64{1, 1, 1, 1, 1, 1},
		},
	}
	for i, test := range tests {
		for _, maxOpN := range []int{0, 10000} {
			t.Run(fmt.Sprintf("%dMaxOpN%d", i, maxOpN), func(t *testing.T) {
				testrows, testcols := make([]uint64, len(test.rows)), make([]uint64, len(test.rows))
				copy(testrows, test.rows)
				copy(testcols, test.cols)
				exp := make(map[uint64]map[uint64]struct{}) // row num to cols
				if len(testrows) != len(testcols) {
					t.Fatalf("bad test spec-need same number of rows/cols, %d/%d", len(testrows), len(testcols))
				}
				// set up expected data
				expOpN := 0
				for i := range testrows {
					row, col := testrows[i], testcols[i]
					cols, ok := exp[row]
					if !ok {
						exp[row] = make(map[uint64]struct{})
						cols = exp[row]
					}
					if _, ok = cols[col]; !ok {
						expOpN++
						cols[col] = struct{}{}
					}
				}

				f, idx, tx := mustOpenFragment(t, "i", "f", viewStandard, 0, "")
				_ = idx
				f.MaxOpN = maxOpN

				err := f.bulkImport(tx, testrows, testcols, &ImportOptions{})
				if err != nil {
					t.Fatalf("initial small import: %v", err)
				}
				if idx.holder.txf.TxType() == RoaringTxn {
					if expOpN <= maxOpN && f.opN != expOpN {
						t.Errorf("unexpected opN - %d is not %d", f.opN, expOpN)
					}
				}
				check(t, tx, f, exp)

				err = f.Close()
				if err != nil {
					t.Fatalf("closing fragment: %v", err)
				}
				PanicOn(tx.Commit())

				err = f.Open()
				tx = idx.holder.txf.NewTx(Txo{Write: writable, Index: idx, Fragment: f, Shard: f.shard})
				defer tx.Rollback()
				if err != nil {
					t.Fatalf("reopening fragment: %v", err)
				}

				if idx.holder.txf.TxType() == RoaringTxn {
					if expOpN <= maxOpN && f.opN != expOpN {
						t.Errorf("unexpected opN after close/open %d is not %d", f.opN, expOpN)
					}
				}

				check(t, tx, f, exp)

				h := newTestHolder(t)
				idx2, err := h.CreateIndex("i", IndexOptions{})
				_ = idx2
				PanicOn(err)

				// OVERWRITING the f.path with a new fragment
				f2 := newFragment(h, makeTestFragSpec(f.path(), "i", "f", viewStandard), 0, 0)
				f2.MaxOpN = maxOpN
				f2.CacheType = f.CacheType

				PanicOn(tx.Commit()) // match the f.closeStorage which overlaps the f2 creation.

				tx2 := idx.holder.txf.NewTx(Txo{Write: writable, Index: idx, Fragment: f2, Shard: f2.shard})
				defer tx2.Rollback()

				err = f.Close()
				if err != nil {
					t.Fatalf("closing storage: %v", err)
				}

				err = f2.Open()
				if err != nil {
					t.Fatalf("opening new fragment: %v", err)
				}

				if idx.holder.txf.TxType() == RoaringTxn {
					if expOpN <= maxOpN && f2.opN != expOpN {
						t.Errorf("unexpected opN after close/open %d is not %d", f2.opN, expOpN)
					}
				}

				check(t, tx2, f2, exp)

				copy(testrows, test.rows)
				copy(testcols, test.cols)
				err = f2.bulkImport(tx2, testrows, testcols, &ImportOptions{Clear: true})
				if err != nil {
					t.Fatalf("clearing imported data: %v", err)
				}

				// clear exp, but leave rows in so we re-query them in `check`
				for row := range exp {
					exp[row] = nil
				}

				check(t, tx2, f2, exp)

				PanicOn(tx2.Commit())

				h3 := NewHolder(filepath.Dir(f2.path()), mustHolderConfig())
				testhook.Cleanup(t, func() {
					h3.Close()
				})

				idx3, err := h3.CreateIndex("i", IndexOptions{})
				_ = idx3
				PanicOn(err)

				f3 := newFragment(h3, makeTestFragSpec(f2.path(), "i", "f", viewStandard), 0, 0)
				f3.MaxOpN = maxOpN
				f3.CacheType = f.CacheType

				tx3 := idx.holder.txf.NewTx(Txo{Write: writable, Index: idx, Fragment: f3, Shard: f3.shard})
				defer tx3.Rollback()

				err = f2.Close()
				if err != nil {
					t.Fatalf("f2 closing storage: %v", err)
				}

				err = f3.Open()
				if err != nil {
					t.Fatalf("opening f3: %v", err)
				}
				defer f3.Clean(t)

				check(t, tx3, f3, exp)

			})

		}
	}
}

func check(t *testing.T, tx Tx, f *fragment, exp map[uint64]map[uint64]struct{}) {

	for rowID, colsExp := range exp {
		colsAct := f.mustRow(tx, rowID).Columns()
		if len(colsAct) != len(colsExp) {
			t.Errorf("row %d len mismatch got: %d exp:%d", rowID, len(colsAct), len(colsExp))
		}
		for _, colAct := range colsAct {
			if _, ok := colsExp[colAct]; !ok {
				t.Errorf("extra column: %d", colAct)
			}
		}
		for colExp := range colsExp {
			found := false
			for _, colAct := range colsAct {
				if colExp == colAct {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("expected %d, but not found", colExp)
			}
		}
	}

}

func TestImportValueConcurrent(t *testing.T) {
	f, idx, tx := mustOpenBSIFragment(t, "i", "f", viewBSIGroupPrefix+"foo", 0)
	defer f.Clean(t)
	// we will be making a new Tx each time, so we can rollback the default provided one.
	tx.Rollback()

	ty := idx.holder.txf.TxTyp()
	switch ty {
	case roaringTxn:
		t.Skip(fmt.Sprintf("skipping TestImportValueConcurrent under " +
			"roaring because the lack of transactional consistency " +
			"from Roaring-per-file will create false comparison " +
			"failures."))
	}

	eg := &errgroup.Group{}
	for i := 0; i < 4; i++ {
		i := i
		eg.Go(func() error {
			tx := idx.holder.txf.NewTx(Txo{Write: writable, Index: idx, Fragment: f, Shard: f.shard})
			defer tx.Rollback()
			for j := uint64(0); j < 10; j++ {
				err := f.importValue(tx, []uint64{j}, []int64{int64(rand.Int63n(1000))}, 10, i%2 == 0)
				if err != nil {
					return err
				}
			}
			return nil
		})
	}
	err := eg.Wait()
	if err != nil {
		t.Fatalf("concurrently importing values: %v", err)
	}
}

func TestImportMultipleValues(t *testing.T) {
	tests := []struct {
		cols      []uint64
		vals      []int64
		checkCols []uint64
		checkVals []int64
		depth     uint64
	}{
		{
			cols:      []uint64{0, 0},
			vals:      []int64{97, 100},
			depth:     7,
			checkCols: []uint64{0},
			checkVals: []int64{100},
		},
	}

	for i, test := range tests {
		for _, maxOpN := range []int{0, 10000} { // test small/large write
			t.Run(fmt.Sprintf("%dLowOpN", i), func(t *testing.T) {
				f, _, tx := mustOpenBSIFragment(t, "i", "f", viewBSIGroupPrefix+"foo", 0)
				f.MaxOpN = maxOpN
				defer f.Clean(t)

				err := f.importValue(tx, test.cols, test.vals, test.depth, false)
				if err != nil {
					t.Fatalf("importing values: %v", err)
				}

				// probably too slow, would hit disk alot:
				//PanicOn(tx.Commit())
				//tx = idx.holder.txf.NewTx(Txo{Write: !writable, Index: idx, Fragment: f, Shard:f.shard, ShardSet:true})
				//defer tx.Rollback()

				for i := range test.checkCols {
					cc, cv := test.checkCols[i], test.checkVals[i]
					n, exists, err := f.value(tx, cc, test.depth)
					if err != nil {
						t.Fatalf("getting value: %v", err)
					}
					if !exists {
						t.Errorf("column %d should exist", cc)
					}
					if n != cv {
						t.Errorf("wrong value: %d is not %d", n, cv)
					}
				}
			})

		}
	}
}

func TestImportValueRowCache(t *testing.T) {
	type testCase struct {
		cols      []uint64
		vals      []int64
		checkCols []uint64
		depth     uint64
	}
	tests := []struct {
		tc1 testCase
		tc2 testCase
	}{
		{
			tc1: testCase{
				cols:      []uint64{2},
				vals:      []int64{1},
				depth:     1,
				checkCols: []uint64{2},
			},
			tc2: testCase{
				cols:      []uint64{1000},
				vals:      []int64{1},
				depth:     1,
				checkCols: []uint64{2, 1000},
			},
		},
	}

	for i, test := range tests {
		for _, maxOpN := range []int{1, 10000} {
			t.Run(fmt.Sprintf("%dMaxOpN%d", i, maxOpN), func(t *testing.T) {
				f, _, tx := mustOpenBSIFragment(t, "i", "f", viewBSIGroupPrefix+"foo", 0)
				f.MaxOpN = maxOpN
				defer f.Clean(t)

				// First import (tc1)
				if err := f.importValue(tx, test.tc1.cols, test.tc1.vals, test.tc1.depth, false); err != nil {
					t.Fatalf("importing values: %v", err)
				}

				if r, err := f.rangeOp(tx, pql.GT, test.tc1.depth, 0); err != nil {
					t.Error("getting range of values")
				} else if !reflect.DeepEqual(r.Columns(), test.tc1.checkCols) {
					t.Errorf("wrong column values. expected: %v, but got: %v", test.tc1.checkCols, r.Columns())
				}

				// Second import (tc2)
				if err := f.importValue(tx, test.tc2.cols, test.tc2.vals, test.tc2.depth, false); err != nil {
					t.Fatalf("importing values: %v", err)
				}

				if r, err := f.rangeOp(tx, pql.GT, test.tc2.depth, 0); err != nil {
					t.Error("getting range of values")
				} else if !reflect.DeepEqual(r.Columns(), test.tc2.checkCols) {
					t.Errorf("wrong column values. expected: %v, but got: %v", test.tc2.checkCols, r.Columns())
				}
			})
		}
	}
}

// part of copy-on-write patch: test for races
// do we see races/corruption around concurrent read/write.
// especially on writes to the row cache.
func TestFragmentConcurrentReadWrite(t *testing.T) {
	f, idx, tx := mustOpenFragment(t, "i", "f", viewStandard, 0, CacheTypeRanked)
	defer f.Clean(t)
	tx.Rollback()

	eg := &errgroup.Group{}
	eg.Go(func() error {

		ltx := idx.holder.txf.NewTx(Txo{Write: writable, Index: idx, Fragment: f, Shard: f.shard})

		for i := uint64(0); i < 1000; i++ {
			_, err := f.setBit(ltx, i%4, i)
			if err != nil {
				return errors.Wrap(err, "setting bit")
			}
		}
		PanicOn(ltx.Commit())
		return nil
	})

	// need read-only Tx so as not to block on the writer finishing above.
	tx1 := idx.holder.txf.NewTx(Txo{Write: !writable, Index: idx, Fragment: f, Shard: f.shard})
	defer tx1.Rollback()

	acc := uint64(0)
	for i := uint64(0); i < 100; i++ {
		r := f.mustRow(tx1, i%4)
		acc += r.Count()
	}
	if err := eg.Wait(); err != nil {
		t.Errorf("error from setting a bit: %v", err)
	}

	t.Logf("%d", acc)
}

func TestRemapCache(t *testing.T) {
	f, _, tx := mustOpenFragment(t, "i", "f", viewStandard, 0, "")
	defer f.Close()
	index, field, view, shard := f.index(), f.field(), f.view(), f.shard

	// request a PanicOn that doesn't kill the program on fault
	wouldFault := debug.SetPanicOnFault(true)
	defer func() {
		debug.SetPanicOnFault(wouldFault)
		if r := recover(); r != nil {
			if err, ok := r.(error); ok {
				// special case: if we caught a page fault, we diagnose that directly. sadly,
				// we can't see the actual values that were used to generate this, probably.
				if err.Error() == "runtime error: invalid memory address or nil pointer dereference" {
					t.Fatalf("segfault trapped during remap test (expected failure mode)")
				}
			}
			t.Fatalf("unexpected PanicOn: %v", r)
		}
	}()

	// create a container
	_, err := tx.Add(index, field, view, shard, 65537)
	if err != nil {
		t.Fatalf("storage add: %v", err)
	}
	// cause the container to be mapped
	err = f.Snapshot()
	if err != nil {
		t.Fatalf("storage snapshot: %v", err)
	}
	// freeze the row
	_ = f.mustRow(tx, 0)
	// add a bit that isn't in that container, so that container doesn't
	// change
	_, err = tx.Add(index, field, view, shard, 2)
	if err != nil {
		t.Fatalf("storage add: %v", err)
	}
	// make the original container be the most recent, thus cached, container
	_, err = f.bit(tx, 0, 65537)
	if err != nil {
		t.Fatalf("storage bit check: %v", err)
	}
	// force snapshot, remapping the containers
	err = f.Snapshot()
	if err != nil {
		t.Fatalf("storage snapshot: %v", err)
	}
	// get rid of the old mapping
	runtime.GC()
	// try to read that container again
	_, err = f.bit(tx, 0, 65537)
	if err != nil {
		t.Fatalf("storage bit check: %v", err)
	}
}

func TestFragment_Bug_Q2DoubleDelete(t *testing.T) {
	f, idx, tx := mustOpenFragment(t, "i", "f", viewStandard, 0, "")
	_ = idx
	// byShardWidth is a map of the same roaring (fragment) data generated
	// with different shard widths.
	// TODO: a better approach may be to generate this in the test based
	// on shard width.
	byShardWidth := make(map[uint64][]byte)
	// row/col: 1/1
	byShardWidth[1<<20] = []byte{60, 48, 0, 0, 1, 0, 0, 0, 16, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 24, 0, 0, 0, 1, 0}
	byShardWidth[1<<22] = []byte{60, 48, 0, 0, 1, 0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 24, 0, 0, 0, 1, 0}

	var b []byte
	if data, ok := byShardWidth[ShardWidth]; ok {
		b = data
	}

	_ = idx
	defer f.Clean(t)

	err := f.importRoaringT(tx, b, false)
	if err != nil {
		t.Fatalf("importing roaring: %v", err)
	}

	//check the bit
	res := f.mustRow(tx, 1).Columns()
	if len(res) < 1 || f.mustRow(tx, 1).Columns()[0] != 1 {
		t.Fatalf("expecting 1 got: %v", res)
	}
	//clear the bit
	changed, _ := f.clearBit(tx, 1, 1)
	if !changed {
		t.Fatalf("expected change got %v", changed)
	}

	//check missing
	res = f.mustRow(tx, 1).Columns()
	if len(res) != 0 {
		t.Fatalf("expected nothing got %v", res)
	}

	// import again
	err = f.importRoaringT(tx, b, false)
	if err != nil {
		t.Fatalf("importing roaring: %v", err)
	}

	//check
	res = f.mustRow(tx, 1).Columns()
	if len(res) < 1 || f.mustRow(tx, 1).Columns()[0] != 1 {
		t.Fatalf("again expecting 1 got: %v", res)
	}

	changed, _ = f.clearBit(tx, 1, 1)
	if !changed {
		t.Fatalf("again expected change got %v", changed) // again expected change got false
	}

	//check missing
	res = f.mustRow(tx, 1).Columns()
	if len(res) != 0 {
		t.Fatalf("expected nothing got %v", res)
	}
}

var mutexSamplesPrepared sync.Once

func requireMutexSampleData(tb testing.TB) {
	mutexSamplesPrepared.Do(func() { prepareMutexSampleData(tb) })
}

// a few mutex tests want common largeish pools of mutex data
type mutexSampleData struct {
	name           string
	rng            mutexSampleRange
	colIDs, rowIDs [3][]uint64
}

// scratchSpace copies the values over corresponding entries in slices,
// reusing existing storage when possible.
func (m *mutexSampleData) scratchSpace(idx int, cols, rows []uint64) ([]uint64, []uint64) {
	colIDs := m.colIDs[idx]
	rowIDs := m.rowIDs[idx]

	if cap(cols) < len(colIDs) {
		cols = make([]uint64, len(colIDs))
	} else {
		cols = cols[:len(colIDs)]
	}
	copy(cols, colIDs)

	if cap(rows) < len(rowIDs) {
		rows = make([]uint64, len(rowIDs))
	} else {
		rows = rows[:len(rowIDs)]
	}
	copy(rows, rowIDs)
	return cols, rows
}

// mutex data wants to exist for differing numbers of rows, and different
// densities, and reasonably-large N. But we don't want a huge pool of nested
// maps, so...
type mutexSampleRange int16

// density should be able to range from every bit filled to almost no
// bits filled. So, how many bits per container? How about pow(2, N), where N
// can be negative. 16 is the highest possible value, and -16 is the lowest,
// and 0 means about one bit per container on average.
func (m mutexSampleRange) density() int8 {
	return int8(m >> 8)
}

// bottom 8 bits are log2 of number of rows.
func (m mutexSampleRange) rows() uint32 {
	return uint32(1) << (m & 0x1F)
}

// just a convenience thing to hide the implementation
func newMutexSampleRange(density int8, rows uint8) mutexSampleRange {
	if density > 16 {
		density = 16
	}
	if density < -16 {
		density = -16
	}
	return mutexSampleRange((int16(density) << 8) | int16(rows))
}

var sampleMutexData = map[string]*mutexSampleData{}

type mutexDensity struct {
	name    string
	density int8
}

type mutexSize struct {
	name string
	rows uint8
}

var mutexDensities = []mutexDensity{
	{"64K", 16},
	// {"32K", 15}, // 50-50
	// {"16K", 14}, // 1/4
	// {"8K", 13},
	// {"4K", 12}, // a fair number of things
	{"1K", 10},
	// {"1", 0},   // about one per container
	// {"empty", -14}, // almost none
}

var mutexSizes = []mutexSize{
	// {"4r", 2},
	// {"16r", 4},
	// {"256r", 8},
	{"2Kr", 11},
	// {"65Kr", 16},
}

var mutexCaches = []string{
	"ranked",
	"none",
}

const mutexSampleDataSize = ShardWidth * len(mutexSampleData{}.colIDs)

// prepareMutexSampleData creates multiple sets of data for each density and
// number of rows, so that we can test performance when overwriting also.
func prepareMutexSampleData(tb testing.TB) {
	myrand := rand.New(rand.NewSource(9))
	for _, d := range mutexDensities {
		// at density 16, we want everything to be adjacent.
		// at density 0, we want about 65k between items.
		// The average spacing we want is 1<<(16 - density),
		// so random numbers between 0 and twice that would
		// be close, but we never want 0, so, subtract 1 from
		// "twice that", then add 1 to the result.
		//
		// So for density 16, we compute spacing of 1, then
		// draw random numbers in [0,1), and add 1 to them.
		spacing := ((1 << (16 - d.density)) * 2) - 1
		for _, s := range mutexSizes {
			rng := newMutexSampleRange(d.density, s.rows)
			col := uint64(0)

			rows := (int64(1) << s.rows)

			colIDs := make([]uint64, mutexSampleDataSize)
			rowIDs := make([]uint64, mutexSampleDataSize)
			data := &mutexSampleData{name: d.name + "/" + s.name, rng: rng}
			prev := uint64(0)
			generated := 0
			for idx := 0; int(prev) < len(data.colIDs); idx++ {
				if spacing > 1 {
					col += uint64(myrand.Int63n(int64(spacing))) + 1
				} else {
					col++
				}
				// can only import one fragment at a time,
				// though!
				if col/ShardWidth > prev {
					data.colIDs[prev] = colIDs[generated:idx:idx]
					data.rowIDs[prev] = rowIDs[generated:idx:idx]
					generated = idx
					prev = col / ShardWidth
					if int(prev) >= len(data.colIDs) {
						break
					}
				}
				row := uint64(myrand.Int63n(rows))
				colIDs[idx] = col % ShardWidth
				rowIDs[idx] = row
			}
			sampleMutexData[data.name] = data
		}
	}
	d := mutexSampleData{
		name: "extra",
		rowIDs: [3][]uint64{
			{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
			{1},
			{2},
		},
		colIDs: [3][]uint64{
			{0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22, 24, 26, 28, 30, 1, 3, 5, 7, 8, 11, 13, 15, 17, 19, 21, 23},
			{29},
			{33},
		},
		rng: 2,
	}
	for i := 0; i < 16; i++ {
		if (i % 16) != 4 {
			d.colIDs[0][i] += 65536
		}
	}
	sampleMutexData[d.name] = &d
}

var importBatchSizes = []int{40, 80, 240, 2048}

func TestImportMutexSampleData(t *testing.T) {
	requireMutexSampleData(t)
	var scratchCols [3][]uint64
	var scratchRows [3][]uint64
	for _, data := range sampleMutexData {
		for i := range scratchRows {
			scratchCols[i], scratchRows[i] = data.scratchSpace(i, scratchCols[i], scratchRows[i])
		}
		seen := make(map[uint64]struct{})
		t.Run(data.name, func(t *testing.T) {
			batchSize := 16384
			f, _, tx := mustOpenMutexFragment(t, "i", "f", viewStandard, 0, "")
			defer f.Clean(t)
			// Set import.
			var err error

			for i := 0; i < len(scratchCols); i++ {
				cols := scratchCols[i]
				rows := scratchRows[i]
				for _, v := range cols {
					seen[v] = struct{}{}
				}

				for j := 0; j < len(cols); j += batchSize {
					max := j + batchSize
					if len(cols) < max {
						max = len(cols)
					}
					err = f.bulkImport(tx, rows[j:max:max], cols[j:max:max], &ImportOptions{})
					if err != nil {
						t.Fatalf("bulk importing ids [%d/3] [%d:%d]: %v", i+1, j, max, err)
					}
				}
				count := uint64(0)
				for k := uint32(0); k < data.rng.rows(); k++ {
					c := f.mustRow(tx, uint64(k)).Count()
					count += c
				}
				if int(count) != len(seen) {
					t.Fatalf("for %d rows, %d density, import %d/3: expected %d results, got %d",
						data.rng.rows(), data.rng.density(), i+1, len(seen), count)
				}
			}
		})
	}
}

// BenchmarkImportMutexSampleData tries to time importing mutex data.
// The tricky part is defining a meaningful b.N that can apply across
// different batch sizes, densities, and so on. So, basically, we take
// b.N, and multiply by 65536, to get "N containers" of data, meaning
// that the amount of data we want to process is independent of all of
// the other factors. But for sparse data sets, that means rewriting
// the same data a number of times, which isn't ideal.
func BenchmarkImportMutexSampleData(b *testing.B) {
	requireMutexSampleData(b)
	var cols []uint64
	var rows []uint64
	var data *mutexSampleData
	var cache string
	var batchSize int
	var frag *fragment
	var tx Tx
	var idx *Index
	benchmarkOneFragmentImports := func(b *testing.B, idx int) {
		cols, rows = data.scratchSpace(idx, cols, rows)
		toDo := b.N << 16
		start := 0
		for toDo > 0 {
			max := start + batchSize
			if len(cols) < max {
				max = len(cols)
			}
			err := frag.bulkImport(tx, rows[start:max:max], cols[start:max:max], &ImportOptions{})
			if err != nil {
				b.Fatalf("bulk importing ids [%d:%d]: %v", start, max, err)
			}
			toDo -= (max - start)
			start = max
			if start >= len(cols) {
				start = 0
				b.StopTimer()
				// recreate data again because bulkImport overwrote it
				cols, rows = data.scratchSpace(idx, cols, rows)
				b.StartTimer()
			}
		}
	}
	benchmarkFragmentImports := func(b *testing.B) {
		frag, idx, tx = mustOpenMutexFragment(b, "i", "f", viewStandard, 0, cache)
		defer frag.Clean(b)
		for i := range data.colIDs {
			b.Run(fmt.Sprintf("write-%d", i), func(b *testing.B) {
				benchmarkOneFragmentImports(b, i)
			})
			// Then commit that write and do another one as a new Tx.
			err := tx.Commit()
			if err != nil {
				b.Fatalf("error commiting write: %v", err)
			}
			tx = idx.holder.txf.NewTx(Txo{Write: writable, Index: idx, Fragment: frag, Shard: 0})
			defer tx.Rollback()
		}
	}
	for _, data = range sampleMutexData {
		b.Run(data.name, func(b *testing.B) {
			for _, batchSize = range importBatchSizes {
				b.Run(strconv.Itoa(batchSize), func(b *testing.B) {
					for _, cache = range mutexCaches {
						b.Run(cache, benchmarkFragmentImports)
					}
				})
			}
		})
	}
}

func testOneParallelSlice(t *testing.T, p *parallelSlices) {
	// the easy answer
	seen := make(map[uint64]uint64, len(p.cols))
	//t.Logf("cols %d, rows %d", p.cols, p.rows)
	for i, c := range p.cols {
		seen[c] = p.rows[i]
	}
	unsorted := p.prune()
	t.Logf("unsorted %t, cols %d, rows %d", unsorted, p.cols, p.rows)
	if unsorted {
		sort.Stable(p)
	}
	unsorted = p.prune()
	if unsorted {
		t.Fatalf("slice still unsorted after sort")
	}
	t.Logf("pruned/sorted cols %d, rows %d", p.cols, p.rows)
	if len(p.cols) != len(seen) {
		t.Fatalf("expected %d entries, found %d", len(seen), len(p.cols))
	}
	for i, c := range p.cols {
		if seen[c] != p.rows[i] {
			t.Fatalf("expected %d:%d, found :%d", c, seen[c], p.rows[i])
		}
	}
}

func TestParallelSlices(t *testing.T) {
	cols := make([]uint64, 256)
	rows := make([]uint64, 256)
	// ensure at least some overlap by coercing columns into a range
	// smaller than number of entries
	for i := range cols {
		cols[i] = rand.Uint64() & ((uint64(len(cols)) / 2) - 1)
		rows[i] = rand.Uint64() & 0xff
	}
	t.Run("random", func(t *testing.T) {
		testOneParallelSlice(t, &parallelSlices{cols: cols, rows: rows})
	})
	cols = cols[:cap(cols)]
	rows = rows[:cap(rows)]
	// in-order but no overlap
	col := uint64(0)
	for i := range cols {
		cols[i] = col
		col = col + (rand.Uint64() & 3) + 1
		rows[i] = rand.Uint64() & 0xff
	}
	t.Run("ordered", func(t *testing.T) {
		testOneParallelSlice(t, &parallelSlices{cols: cols, rows: rows})
	})
	cols = cols[:cap(cols)]
	rows = rows[:cap(rows)]
	// in-order with
	col = uint64(0)
	for i := range cols {
		cols[i] = col
		col = col + (rand.Uint64() & 3)
		rows[i] = rand.Uint64() & 0xff
	}
	t.Run("orderlapping", func(t *testing.T) {
		testOneParallelSlice(t, &parallelSlices{cols: cols, rows: rows})
	})
}

func testOneParallelSliceFullPrune(t *testing.T, p *parallelSlices) {
	// the easy answer
	seen := make(map[uint64]uint64, len(p.cols))
	//t.Logf("cols %d, rows %d", p.cols, p.rows)
	for i, c := range p.cols {
		seen[c] = p.cols[i]
	}
	//t.Logf("before fullPrune: cols %d, rows %d", p.cols, p.rows)
	p.fullPrune()

	//t.Logf("after fullPrune, pruned/sorted cols %d, rows %d", p.cols, p.rows)
	if len(p.cols) != len(seen) {
		t.Fatalf("expected %d entries, found %d", len(seen), len(p.cols))
	}
	for i := range p.cols {
		if i == 0 {
			continue
		}
		if p.cols[i] <= p.cols[i-1] {
			t.Fatalf("expected p.cols[i=%v]=%v <= p.cols[i-1=%v]=%v", i, p.cols[i], i-1, p.cols[i-1])
		}
	}
}

func TestParallelSlicesFullPrune(t *testing.T) {
	cols := make([]uint64, 0)
	rows := make([]uint64, 0)

	t.Run("no_loss", func(t *testing.T) {
		testOneParallelSliceFullPrune(t, &parallelSlices{cols: cols, rows: rows})
	})

	cols = make([]uint64, 1)
	rows = make([]uint64, 1)
	cols[0] = 3

	t.Run("no_loss", func(t *testing.T) {
		testOneParallelSliceFullPrune(t, &parallelSlices{cols: cols, rows: rows})
	})

	cols = make([]uint64, 2)
	rows = make([]uint64, 2)
	cols[1] = 1 // sorted

	t.Run("no_loss", func(t *testing.T) {
		testOneParallelSliceFullPrune(t, &parallelSlices{cols: cols, rows: rows})
	})

	cols = make([]uint64, 2)
	rows = make([]uint64, 2)
	// one duplicate 0

	t.Run("no_loss", func(t *testing.T) {
		testOneParallelSliceFullPrune(t, &parallelSlices{cols: cols, rows: rows})
	})

	cols = make([]uint64, 2)
	rows = make([]uint64, 2)
	cols[0] = 1 // unsorted

	t.Run("no_loss", func(t *testing.T) {
		testOneParallelSliceFullPrune(t, &parallelSlices{cols: cols, rows: rows})
	})

	cols = make([]uint64, 3)
	rows = make([]uint64, 3)
	cols[0] = 2 // unsorted
	cols[1] = 1 // unsorted

	t.Run("no_loss", func(t *testing.T) {
		testOneParallelSliceFullPrune(t, &parallelSlices{cols: cols, rows: rows})
	})

	cols = make([]uint64, 3)
	rows = make([]uint64, 3)
	// three duplicate 0s

	t.Run("no_loss", func(t *testing.T) {
		testOneParallelSliceFullPrune(t, &parallelSlices{cols: cols, rows: rows})
	})

	cols = make([]uint64, 3)
	rows = make([]uint64, 3)
	// three duplicate 1s
	cols[0] = 1
	cols[1] = 1
	cols[2] = 1

	t.Run("no_loss", func(t *testing.T) {
		testOneParallelSliceFullPrune(t, &parallelSlices{cols: cols, rows: rows})
	})

	cols = make([]uint64, 256)
	rows = make([]uint64, 256)

	// ensure at least some overlap by coercing columns into a range
	// smaller than number of entries
	for i := range cols {
		cols[i] = rand.Uint64() & ((uint64(len(cols)) / 2) - 1)
		rows[i] = rand.Uint64() & 0xff
	}
	t.Run("random", func(t *testing.T) {
		testOneParallelSliceFullPrune(t, &parallelSlices{cols: cols, rows: rows})
	})

	cols = cols[:cap(cols)]
	rows = rows[:cap(rows)]
	// in-order but no overlap
	col := uint64(0)
	for i := range cols {
		cols[i] = col
		col = col + (rand.Uint64() & 3) + 1
		rows[i] = rand.Uint64() & 0xff
	}
	t.Run("ordered", func(t *testing.T) {
		testOneParallelSliceFullPrune(t, &parallelSlices{cols: cols, rows: rows})
	})
	cols = cols[:cap(cols)]
	rows = rows[:cap(rows)]
	// in-order with
	col = uint64(0)
	for i := range cols {
		cols[i] = col
		col = col + (rand.Uint64() & 3)
		rows[i] = rand.Uint64() & 0xff
	}
	t.Run("orderlapping", func(t *testing.T) {
		testOneParallelSliceFullPrune(t, &parallelSlices{cols: cols, rows: rows})
	})
}

func compareSlices(tb testing.TB, name string, s1, s2 []uint64) {
	if len(s1) != len(s2) {
		tb.Fatalf("slice length mismatch %q: expected %d items %d, got %d items %d",
			name, len(s1), s1, len(s2), s2)
	}
	for i, v := range s1 {
		if s2[i] != v {
			tb.Fatalf("row mismatch %q: expected item %d to be %d, got %d",
				name, i, s1[i], s2[i])
		}
	}
}

type sliceDifferenceTestCase struct {
	original, remove, expected []uint64
}

func TestSliceDifference(t *testing.T) {
	testCases := map[string]sliceDifferenceTestCase{
		"noOverlap": {
			original: []uint64{1, 2, 3},
			remove:   []uint64{0, 5},
			expected: []uint64{1, 2, 3},
		},
		"before": {
			original: []uint64{3, 5, 7},
			remove:   []uint64{0, 6},
			expected: []uint64{3, 5, 7},
		},
		"after": {
			original: []uint64{3, 5, 7},
			remove:   []uint64{8, 10},
			expected: []uint64{3, 5, 7},
		},
		"all": {
			original: []uint64{3, 5, 7},
			remove:   []uint64{3, 5, 7},
			expected: []uint64{},
		},
		"first": {
			original: []uint64{3, 5, 7},
			remove:   []uint64{3},
			expected: []uint64{5, 7},
		},
		"last": {
			original: []uint64{3, 5, 7},
			remove:   []uint64{7},
			expected: []uint64{3, 5},
		},
		"middle": {
			original: []uint64{3, 5, 7},
			remove:   []uint64{5},
			expected: []uint64{3, 7},
		},
	}
	var scratch []uint64
	for name, tc := range testCases {
		scratch = append(scratch[:0], tc.original...)
		result := sliceDifference(scratch, tc.remove)
		compareSlices(t, name, tc.expected, result)
	}
}

func TestBitmapGrowth(t *testing.T) {
	roaringOnlyTest(t)
	f, _, tx := mustOpenFragment(t, "i", "f", viewBSIGroupPrefix+"foo", 0, "")
	path := f.path()
	defer f.Clean(t)
	const values = 500
	cols := make([]uint64, values)
	vals := make([]int64, values)
	for i := range cols {
		cols[i] = uint64(rand.Int63n(65536))
		vals[i] = rand.Int63n(24)
	}
	err := f.importValue(tx, cols, vals, 7, false)
	if err != nil {
		t.Fatalf("importing values: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("statting %s: %v", path, err)
	}
	prevSize := info.Size()
	prevOpN := f.opN
	err = f.importValue(tx, cols, vals, 7, false)
	if err != nil {
		t.Fatalf("importing values: %v", err)
	}
	info, err = os.Stat(path)
	if err != nil {
		t.Fatalf("statting %s: %v", path, err)
	}
	deltaSize := info.Size() - prevSize
	deltaOpN := f.opN - prevOpN
	// This is somewhat arbitrary, but the issue tested for was that
	// opN would grow by 0 or 1 with multiple KB of actual ops written.
	// If deltaOpN is at least 20, we'll probably see snapshots happening
	// at least occasionally, and if deltaSize is under 1024, the writes
	// are probably going to be small enough that the regular backlog of
	// snapshotting catches them anyway.
	if deltaSize > 1024 && deltaOpN < 20 {
		t.Fatalf("bitmap grew by %d bytes but OpN only grew by %d",
			deltaSize, deltaOpN)
	}
}
