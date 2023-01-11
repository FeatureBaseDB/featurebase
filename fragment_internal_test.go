// Copyright 2021 Molecula Corp. All rights reserved.
package pilosa

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"testing"
	"testing/quick"

	"github.com/davecgh/go-spew/spew"
	"github.com/molecula/featurebase/v3/pql"
	qc "github.com/molecula/featurebase/v3/querycontext"
	"github.com/molecula/featurebase/v3/roaring"
	"github.com/molecula/featurebase/v3/testhook"
	. "github.com/molecula/featurebase/v3/vprint" // nolint:staticcheck
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

// Test flags
var (
	// In order to generate the sample fragment file,
	// run an import and copy PILOSA_DATA_DIR/INDEX_NAME/FRAME_NAME/0 to testdata/sample_view
	FragmentPath = flag.String("fragment", "testdata/sample_view/0", "fragment path")
)

// mustQueryContext requests a brand new query context against a fragment, which
// if it is a write QueryContext, will be scoped to that specific fragment's
// index and shard. This, and mustRead/mustWrite, are workarounds for the fact
// that the whole point of QueryContext is to get us sustained access with shared
// transactions, and fragment_internal_test is all about doing multiple sequential
// operations that aren't sharing a backend transaction. Rather than muddle the
// usual interface around this special case, we write helpers for the test code.
func mustQueryContext(tb testing.TB, f *fragment, write bool) qc.QueryContext {
	var qcx qc.QueryContext
	var err error
	if write {
		qcx, err = f.holder.NewIndexQueryContext(context.Background(), f.index(), f.shard)
	} else {
		qcx, err = f.holder.NewQueryContext(context.Background())
	}
	if err != nil {
		tb.Fatalf("creating query context (write %t): %v", write, err)
	}
	tb.Cleanup(qcx.Release)
	return qcx
}

// mustRead yields a new QueryContext and QueryRead, or fails trying. This is
// in test code because it's nonsensical outside of tests; in non-test
// circumstances, you'd have a meaningful higher level operation to own the
// QueryContext.
func mustRead(tb testing.TB, f *fragment) (qc.QueryContext, qc.QueryRead) {
	qcx := mustQueryContext(tb, f, false)
	qr, err := f.qcxRead(qcx)
	if err != nil {
		tb.Fatalf("creating query read: %v", err)
	}
	return qcx, qr
}

// mustWrite yields a new QueryContext and QueryWrite, or fails trying. This is
// in test code because it's nonsensical outside of tests; in non-test
// circumstances, you'd have a meaningful higher level operation to own the
// QueryContext.
func mustWrite(tb testing.TB, f *fragment) (qc.QueryContext, qc.QueryWrite) {
	qcx := mustQueryContext(tb, f, true)
	qw, err := f.qcxWrite(qcx)
	if err != nil {
		tb.Fatalf("creating query write: %v", err)
	}
	return qcx, qw
}

// mustRow returns a row by ID. Panic on error. Only used for testing.
func (f *fragment) mustRow(tb testing.TB, qr qc.QueryRead, rowID uint64) *Row {
	row, err := f.row(qr, rowID)
	if err != nil {
		tb.Fatalf("reading row %d: %v", rowID, err)
	}
	return row
}

// Ensure a fragment can set a bit and retrieve it.
func TestFragment_SetBit(t *testing.T) {
	f, qcx, qw := mustOpenFragment(t)
	defer f.Clean(t)

	// Set bits on the fragment.
	if _, err := f.setBit(qw, 120, 1); err != nil {
		t.Fatal(err)
	} else if _, err := f.setBit(qw, 120, 6); err != nil {
		t.Fatal(err)
	} else if _, err := f.setBit(qw, 121, 0); err != nil {
		t.Fatal(err)
	}
	// should have two containers set in the fragment.

	// Verify counts on rows.
	if n := f.mustRow(t, qw, 120).Count(); n != 2 {
		t.Fatalf("unexpected count: %d", n)
	} else if n := f.mustRow(t, qw, 121).Count(); n != 1 {
		t.Fatalf("unexpected count: %d", n)
	}

	require.Nil(t, qcx.Commit())

	// Close and reopen the fragment & verify the data.
	err := f.Reopen(t)
	if err != nil {
		t.Fatal(err)
	}
	_, qr := mustRead(t, f)

	if n := f.mustRow(t, qr, 120).Count(); n != 2 {
		t.Fatalf("unexpected count (reopen): %d", n)
	} else if n := f.mustRow(t, qr, 121).Count(); n != 1 {
		t.Fatalf("unexpected count (reopen): %d", n)
	}
}

// Ensure a fragment can clear a set bit.
func TestFragment_ClearBit(t *testing.T) {
	f, qcx, qw := mustOpenFragment(t)
	defer f.Clean(t)

	// Set and then clear bits on the fragment.
	if _, err := f.setBit(qw, 1000, 1); err != nil {
		t.Fatal(err)
	} else if _, err := f.setBit(qw, 1000, 2); err != nil {
		t.Fatal(err)
	} else if _, err := f.clearBit(qw, 1000, 1); err != nil {
		t.Fatal(err)
	}

	// Verify count on row.
	if n := f.mustRow(t, qw, 1000).Count(); n != 1 {
		t.Fatalf("unexpected count: %d", n)
	}
	// The Reopen below implies this test is looking at storage consistency.
	// In that spirit, we will check that the Tx Commit is visible afterwards.
	require.Nil(t, qcx.Commit())

	_, qr := mustRead(t, f)

	// Close and reopen the fragment & verify the data.
	if err := f.Reopen(t); err != nil {
		t.Fatal(err)
	} else if n := f.mustRow(t, qr, 1000).Count(); n != 1 {
		t.Fatalf("unexpected count (reopen): %d", n)
	}
}

// Ensure a fragment can clear a row.
func TestFragment_ClearRow(t *testing.T) {
	f, qcx, qw := mustOpenFragment(t)
	defer f.Clean(t)

	// Set and then clear bits on the fragment.
	if _, err := f.setBit(qw, 1000, 1); err != nil {
		t.Fatal(err)
	} else if _, err := f.setBit(qw, 1000, 65536); err != nil {
		t.Fatal(err)
	} else if _, err := f.unprotectedClearRow(qw, 1000); err != nil {
		t.Fatal(err)
	}

	// Verify count on row.
	if n := f.mustRow(t, qw, 1000).Count(); n != 0 {
		t.Fatalf("unexpected count: %d", n)
	}
	require.Nil(t, qcx.Commit())
	_, qr := mustRead(t, f)

	// Close and reopen the fragment & verify the data.
	if err := f.Reopen(t); err != nil {
		t.Fatal(err)
	} else if n := f.mustRow(t, qr, 1000).Count(); n != 0 {
		t.Fatalf("unexpected count (reopen): %d", n)
	}
}

// Ensure a fragment can set a row.
func TestFragment_SetRow(t *testing.T) {
	f, qcx, qw := mustOpenFragment(t)
	defer f.Clean(t)

	// Obtain transction.

	rowID := uint64(1000)

	// Set bits on the fragment.
	if _, err := f.setBit(qw, rowID, 1); err != nil {
		t.Fatal(err)
	} else if _, err := f.setBit(qw, rowID, 65536); err != nil {
		t.Fatal(err)
	}

	// Verify data on row.
	if cols := f.mustRow(t, qw, rowID).Columns(); !reflect.DeepEqual(cols, []uint64{1, 65536}) {
		t.Fatalf("unexpected columns: %+v", cols)
	}
	// Verify count on row.
	if n := f.mustRow(t, qw, rowID).Count(); n != 2 {
		t.Fatalf("unexpected count: %d", n)
	}

	// Set row (overwrite existing data).
	row := NewRow(1, 65537, 140000)
	if changed, err := f.unprotectedSetRow(qw, row, rowID); err != nil {
		t.Fatal(err)
	} else if !changed {
		t.Fatalf("expected changed value: %v", changed)
	}

	require.Nil(t, qcx.Commit())
	qcx, qw = mustWrite(t, f)

	// Verify data on row.
	if cols := f.mustRow(t, qw, rowID).Columns(); !reflect.DeepEqual(cols, []uint64{1, 65537, 140000}) {
		t.Fatalf("unexpected columns after set row: %+v", cols)
	}
	// Verify count on row.
	if n := f.mustRow(t, qw, rowID).Count(); n != 3 {
		t.Fatalf("unexpected count after set row: %d", n)
	}

	require.Nil(t, qcx.Commit())

	qcx, qr := mustRead(t, f)

	// Close and reopen the fragment & verify the data.
	if err := f.Reopen(t); err != nil {
		t.Fatal(err)
	} else if n := f.mustRow(t, qr, rowID).Count(); n != 3 {
		t.Fatalf("unexpected count (reopen): %d", n)
	}

	qcx.Release()
	qcx, qw = mustWrite(t, f)

	// verify that setting something from a row which lacks a segment for
	// this fragment's shard still clears this fragment correctly.
	notOurs := NewRow(8*ShardWidth + 1024)
	if changed, err := f.unprotectedSetRow(qw, notOurs, rowID); err != nil {
		t.Fatal(err)
	} else if !changed {
		t.Fatalf("setRow didn't report a change")
	}
	if cols := f.mustRow(t, qw, rowID).Columns(); len(cols) != 0 {
		t.Fatalf("expected setting a row with no entries to clear the cache")
	}
	require.Nil(t, qcx.Commit())
}

// Ensure a fragment can set & read a value.
func TestFragment_SetValue(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		f, qcx, qw := mustOpenFragment(t)
		defer f.Clean(t)

		// Set value.
		if changed, err := f.setValue(qw, 100, 16, 3829); err != nil {
			t.Fatal(err)
		} else if !changed {
			t.Fatal("expected change")
		}

		// Read value.
		if value, exists, err := f.value(qw, 100, 16); err != nil {
			t.Fatal(err)
		} else if value != 3829 {
			t.Fatalf("unexpected value: %d", value)
		} else if !exists {
			t.Fatal("expected to exist")
		}

		// Setting value should return no change.
		if changed, err := f.setValue(qw, 100, 16, 3829); err != nil {
			t.Fatal(err)
		} else if changed {
			t.Fatal("expected no change")
		}

		// same after Commit
		if err := qcx.Commit(); err != nil {
			t.Fatal(err)
		}
		_, qw = mustWrite(t, f)

		// Read value.
		if value, exists, err := f.value(qw, 100, 16); err != nil {
			t.Fatal(err)
		} else if value != 3829 {
			t.Fatalf("unexpected value: %d", value)
		} else if !exists {
			t.Fatal("expected to exist")
		}

		// Setting value should return no change.
		if changed, err := f.setValue(qw, 100, 16, 3829); err != nil {
			t.Fatal(err)
		} else if changed {
			t.Fatal("expected no change")
		}
	})

	t.Run("Overwrite", func(t *testing.T) {
		f, qcx, qw := mustOpenFragment(t)
		defer f.Clean(t)

		// Set value.
		if changed, err := f.setValue(qw, 100, 16, 3829); err != nil {
			t.Fatal(err)
		} else if !changed {
			t.Fatal("expected change")
		}

		// Overwriting value should overwrite all bits.
		if changed, err := f.setValue(qw, 100, 16, 2028); err != nil {
			t.Fatal(err)
		} else if !changed {
			t.Fatal("expected change")
		}

		// Read value.
		if value, exists, err := f.value(qw, 100, 16); err != nil {
			t.Fatal(err)
		} else if value != 2028 {
			t.Fatalf("unexpected value: %d", value)
		} else if !exists {
			t.Fatal("expected to exist")
		}

		// Read value after commit.
		if err := qcx.Commit(); err != nil {
			t.Fatal(err)
		}
		_, qw = mustWrite(t, f)

		if value, exists, err := f.value(qw, 100, 16); err != nil {
			t.Fatal(err)
		} else if value != 2028 {
			t.Fatalf("unexpected value: %d", value)
		} else if !exists {
			t.Fatal("expected to exist")
		}

	})

	t.Run("Clear", func(t *testing.T) {
		f, qcx, qw := mustOpenFragment(t)
		defer f.Clean(t)

		// Set value.
		if changed, err := f.setValue(qw, 100, 16, 3829); err != nil {
			t.Fatal(err)
		} else if !changed {
			t.Fatal("expected change")
		}

		// Clear value should overwrite all bits, and set not-null to 0.
		if changed, err := f.clearValue(qw, 100, 16, 2028); err != nil {
			t.Fatal(err)
		} else if !changed {
			t.Fatal("expected change")
		}

		// Read value.
		if value, exists, err := f.value(qw, 100, 16); err != nil {
			t.Fatal(err)
		} else if value != 0 {
			t.Fatalf("unexpected value: %d", value)
		} else if exists {
			t.Fatal("expected to not exist")
		}

		// Same after Commit
		if err := qcx.Commit(); err != nil {
			t.Fatal(err)
		}
		_, qw = mustWrite(t, f)

		if value, exists, err := f.value(qw, 100, 16); err != nil {
			t.Fatal(err)
		} else if value != 0 {
			t.Fatalf("unexpected value: %d", value)
		} else if exists {
			t.Fatal("expected to not exist")
		}
	})

	t.Run("NotExists", func(t *testing.T) {
		f, _, qw := mustOpenFragment(t)
		defer f.Clean(t)

		// Set value.
		if changed, err := f.setValue(qw, 100, 10, 20); err != nil {
			t.Fatal(err)
		} else if !changed {
			t.Fatal("expected change")
		}

		// Non-existent value.
		if value, exists, err := f.value(qw, 101, 11); err != nil {
			t.Fatal(err)
		} else if value != 0 {
			t.Fatalf("unexpected value: %d", value)
		} else if exists {
			t.Fatal("expected to not exist")
		}

	})

	t.Run("QuickCheck", func(t *testing.T) {
		f, qcx, _ := mustOpenFragment(t)
		defer f.Clean(t)
		qcx.Release()

		if err := quick.Check(func(bitDepth uint64, bitN uint64, values []uint64) bool {
			qcx, qw := mustWrite(t, f)
			defer qcx.Release()
			// Limit bit depth & maximum values.
			bitDepth = (bitDepth % 8) + 1
			bitN = (bitN % 99) + 1

			for i := range values {
				values[i] = values[i] % (1 << bitDepth)
			}

			// Set values.
			m := make(map[uint64]int64)
			for _, value := range values {
				columnID := value % bitN

				m[columnID] = int64(value)

				if _, err := f.setValue(qw, columnID, bitDepth, int64(value)); err != nil {
					t.Fatal(err)
				}
			}

			// Ensure values are set.
			for columnID, value := range m {
				v, exists, err := f.value(qw, columnID, bitDepth)
				if err != nil {
					t.Fatal(err)
				} else if value != int64(v) {
					t.Fatalf("value mismatch: columnID=%d, bitdepth=%d, value: %d != %d", columnID, bitDepth, value, v)
				} else if !exists {
					t.Fatalf("value should exist: columnID=%d", columnID)
				}
			}

			// Same after Commit
			if err := qcx.Commit(); err != nil {
				t.Fatal(err)
			}
			qcx, qr := mustRead(t, f)
			defer qcx.Release()

			// Ensure values are set.
			for columnID, value := range m {
				v, exists, err := f.value(qr, columnID, bitDepth)
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

	f, qcx, qw := mustOpenFragment(t)
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
		if _, err := f.setValue(qw, v.cid, bitDepth, v.val); err != nil {
			t.Fatal(err)
		}
	}

	require.Nil(t, qcx.Commit())
	qcx, qr := mustRead(t, f)

	t.Run("NoFilter", func(t *testing.T) {
		if sum, n, err := f.sum(qr, nil, bitDepth); err != nil {
			t.Fatal(err)
		} else if n != 5 {
			t.Fatalf("unexpected count: %d", n)
		} else if got, exp := sum, int64(382+300-600+2818+300); got != exp {
			t.Fatalf("unexpected sum: got: %d, exp: %d", sum, exp)
		}
	})

	t.Run("WithFilter", func(t *testing.T) {
		if sum, n, err := f.sum(qr, NewRow(2000, 4000, 5000), bitDepth); err != nil {
			t.Fatal(err)
		} else if n != 2 {
			t.Fatalf("unexpected count: %d", n)
		} else if got, exp := sum, int64(300+300); got != exp {
			t.Fatalf("unexpected sum: got: %d, exp: %d", sum, exp)
		}
	})

	qcx.Release()
	qcx, qw = mustWrite(t, f)

	// verify that clearValue clears values
	if _, err := f.clearValue(qw, 1000, bitDepth, 23); err != nil {
		t.Fatal(err)
	}

	require.Nil(t, qcx.Commit())
	_, qr = mustRead(t, f)

	t.Run("ClearValue", func(t *testing.T) {
		if sum, n, err := f.sum(qr, nil, bitDepth); err != nil {
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

	f, qcx, qw := mustOpenFragment(t)
	defer f.Clean(t)

	// Set values.
	if _, err := f.setValue(qw, 1000, bitDepth, 382); err != nil {
		t.Fatal(err)
	} else if _, err := f.setValue(qw, 2000, bitDepth, 300); err != nil {
		t.Fatal(err)
	} else if _, err := f.setValue(qw, 3000, bitDepth, 2818); err != nil {
		t.Fatal(err)
	} else if _, err := f.setValue(qw, 4000, bitDepth, 300); err != nil {
		t.Fatal(err)
	} else if _, err := f.setValue(qw, 5000, bitDepth, 2818); err != nil {
		t.Fatal(err)
	} else if _, err := f.setValue(qw, 6000, bitDepth, 2817); err != nil {
		t.Fatal(err)
	} else if _, err := f.setValue(qw, 7000, bitDepth, 0); err != nil {
		t.Fatal(err)
	}

	require.Nil(t, qcx.Commit())

	// the new tx is shared by Min/Max below.
	_, qr := mustRead(t, f)

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
			if min, cnt, err := f.min(qr, test.filter, bitDepth); err != nil {
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

			if max, cnt, err := f.max(qr, test.filter, bitDepth); err != nil {
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
		f, _, qw := mustOpenFragment(t)
		defer f.Clean(t)

		// Set values.
		if _, err := f.setValue(qw, 1000, bitDepth, 382); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(qw, 2000, bitDepth, 300); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(qw, 3000, bitDepth, 2818); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(qw, 4000, bitDepth, 300); err != nil {
			t.Fatal(err)
		}

		// Query for equality.
		if b, err := f.rangeOp(qw, pql.EQ, bitDepth, 300); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(b.Columns(), []uint64{2000, 4000}) {
			t.Fatalf("unexpected columns: %+v", b.Columns())
		}
	})

	t.Run("EQOversizeRegression", func(t *testing.T) {
		f, _, qw := mustOpenFragment(t)
		defer f.Clean(t)

		// Set values.
		if _, err := f.setValue(qw, 1000, 1, 0); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(qw, 2000, 1, 1); err != nil {
			t.Fatal(err)
		}

		// Query for equality.
		if b, err := f.rangeOp(qw, pql.EQ, 1, 3); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(b.Columns(), []uint64{}) {
			t.Fatalf("unexpected columns: %+v", b.Columns())
		}
		if b, err := f.rangeOp(qw, pql.EQ, 1, 4); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(b.Columns(), []uint64{}) {
			t.Fatalf("unexpected columns: %+v", b.Columns())
		}
	})

	t.Run("NEQ", func(t *testing.T) {
		f, _, qw := mustOpenFragment(t)
		defer f.Clean(t)

		// Set values.
		if _, err := f.setValue(qw, 1000, bitDepth, 382); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(qw, 2000, bitDepth, 300); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(qw, 3000, bitDepth, 2818); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(qw, 4000, bitDepth, 300); err != nil {
			t.Fatal(err)
		}

		// Query for inequality.
		if b, err := f.rangeOp(qw, pql.NEQ, bitDepth, 300); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(b.Columns(), []uint64{1000, 3000}) {
			t.Fatalf("unexpected columns: %+v", b.Columns())
		}
	})

	t.Run("LT", func(t *testing.T) {
		f, _, qw := mustOpenFragment(t)
		defer f.Clean(t)

		// Set values.
		if _, err := f.setValue(qw, 1000, bitDepth, 382); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(qw, 2000, bitDepth, 300); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(qw, 3000, bitDepth, 2817); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(qw, 4000, bitDepth, 301); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(qw, 5000, bitDepth, 1); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(qw, 6000, bitDepth, 0); err != nil {
			t.Fatal(err)
		}

		// Query for values less than (ending with set column).
		if b, err := f.rangeOp(qw, pql.LT, bitDepth, 301); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(b.Columns(), []uint64{2000, 5000, 6000}) {
			t.Fatalf("unexpected columns: %+v", b.Columns())
		}

		// Query for values less than (ending with unset column).
		if b, err := f.rangeOp(qw, pql.LT, bitDepth, 300); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(b.Columns(), []uint64{5000, 6000}) {
			t.Fatalf("unexpected columns: %+v", b.Columns())
		}

		// Query for values less than or equal to (ending with set column).
		if b, err := f.rangeOp(qw, pql.LTE, bitDepth, 301); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(b.Columns(), []uint64{2000, 4000, 5000, 6000}) {
			t.Fatalf("unexpected columns: %+v", b.Columns())
		}

		// Query for values less than or equal to (ending with unset column).
		if b, err := f.rangeOp(qw, pql.LTE, bitDepth, 300); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(b.Columns(), []uint64{2000, 5000, 6000}) {
			t.Fatalf("unexpected columns: %+v", b.Columns())
		}
	})

	t.Run("LTRegression", func(t *testing.T) {
		f, _, qw := mustOpenFragment(t)
		defer f.Clean(t)

		if _, err := f.setValue(qw, 1, 1, 1); err != nil {
			t.Fatal(err)
		}

		if b, err := f.rangeOp(qw, pql.LT, 1, 2); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(b.Columns(), []uint64{1}) {
			t.Fatalf("unepxected coulmns: %+v", b.Columns())
		}
	})

	t.Run("LTMaxRegression", func(t *testing.T) {
		f, _, qw := mustOpenFragment(t)
		defer f.Clean(t)

		if _, err := f.setValue(qw, 1, 2, 3); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(qw, 2, 2, 0); err != nil {
			t.Fatal(err)
		}

		if b, err := f.rangeLTUnsigned(qw, NewRow(1, 2), 2, 3, false); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(b.Columns(), []uint64{2}) {
			t.Fatalf("unepxected coulmns: %+v", b.Columns())
		}
	})

	t.Run("GT", func(t *testing.T) {
		f, _, qw := mustOpenFragment(t)
		defer f.Clean(t)

		// Set values.
		if _, err := f.setValue(qw, 1000, bitDepth, 382); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(qw, 2000, bitDepth, 300); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(qw, 3000, bitDepth, 2817); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(qw, 4000, bitDepth, 301); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(qw, 5000, bitDepth, 1); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(qw, 6000, bitDepth, 0); err != nil {
			t.Fatal(err)
		}

		// Query for values greater than (ending with unset bit).
		if b, err := f.rangeOp(qw, pql.GT, bitDepth, 300); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(b.Columns(), []uint64{1000, 3000, 4000}) {
			t.Fatalf("unexpected columns: %+v", b.Columns())
		}

		// Query for values greater than (ending with set bit).
		if b, err := f.rangeOp(qw, pql.GT, bitDepth, 301); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(b.Columns(), []uint64{1000, 3000}) {
			t.Fatalf("unexpected columns: %+v", b.Columns())
		}

		// Query for values greater than or equal to (ending with unset bit).
		if b, err := f.rangeOp(qw, pql.GTE, bitDepth, 300); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(b.Columns(), []uint64{1000, 2000, 3000, 4000}) {
			t.Fatalf("unexpected columns: %+v", b.Columns())
		}

		// Query for values greater than or equal to (ending with set bit).
		if b, err := f.rangeOp(qw, pql.GTE, bitDepth, 301); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(b.Columns(), []uint64{1000, 3000, 4000}) {
			t.Fatalf("unexpected columns: %+v", b.Columns())
		}
	})

	t.Run("GTMinRegression", func(t *testing.T) {
		f, _, qw := mustOpenFragment(t)
		defer f.Clean(t)

		if _, err := f.setValue(qw, 1, 2, 0); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(qw, 2, 2, 1); err != nil {
			t.Fatal(err)
		}

		if b, err := f.rangeGTUnsigned(qw, NewRow(1, 2), 2, 0, false); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(b.Columns(), []uint64{2}) {
			t.Fatalf("unepxected coulmns: %+v", b.Columns())
		}
	})

	t.Run("GTOversizeRegression", func(t *testing.T) {
		f, _, qw := mustOpenFragment(t)
		defer f.Clean(t)

		if _, err := f.setValue(qw, 1, 2, 0); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(qw, 2, 2, 1); err != nil {
			t.Fatal(err)
		}

		if b, err := f.rangeGTUnsigned(qw, NewRow(1, 2), 2, 4, false); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(b.Columns(), []uint64{}) {
			t.Fatalf("unepxected coulmns: %+v", b.Columns())
		}
	})

	t.Run("BETWEEN", func(t *testing.T) {
		f, _, qw := mustOpenFragment(t)
		defer f.Clean(t)

		// Set values.
		if _, err := f.setValue(qw, 1000, bitDepth, 382); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(qw, 2000, bitDepth, 300); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(qw, 3000, bitDepth, 2817); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(qw, 4000, bitDepth, 301); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(qw, 5000, bitDepth, 1); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(qw, 6000, bitDepth, 0); err != nil {
			t.Fatal(err)
		}

		// Query for values greater than (ending with unset column).
		if b, err := f.rangeBetween(qw, bitDepth, 300, 2817); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(b.Columns(), []uint64{1000, 2000, 3000, 4000}) {
			t.Fatalf("unexpected columns: %+v", b.Columns())
		}

		// Query for values greater than (ending with set column).
		if b, err := f.rangeBetween(qw, bitDepth, 301, 2817); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(b.Columns(), []uint64{1000, 3000, 4000}) {
			t.Fatalf("unexpected columns: %+v", b.Columns())
		}

		// Query for values greater than or equal to (ending with unset column).
		if b, err := f.rangeBetween(qw, bitDepth, 301, 2816); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(b.Columns(), []uint64{1000, 4000}) {
			t.Fatalf("unexpected columns: %+v", b.Columns())
		}

		// Query for values greater than or equal to (ending with set column).
		if b, err := f.rangeBetween(qw, bitDepth, 300, 2816); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(b.Columns(), []uint64{1000, 2000, 4000}) {
			t.Fatalf("unexpected columns: %+v", b.Columns())
		}
	})

	t.Run("BetweenCommonBitsRegression", func(t *testing.T) {
		f, _, qw := mustOpenFragment(t)
		defer f.Clean(t)

		if _, err := f.setValue(qw, 1, 64, 0xf0); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(qw, 2, 64, 0xf1); err != nil {
			t.Fatal(err)
		}

		if b, err := f.rangeBetweenUnsigned(qw, NewRow(1, 2), 64, 0xf0, 0xf1); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(b.Columns(), []uint64{1, 2}) {
			t.Fatalf("unepxected coulmns: %+v", b.Columns())
		}
	})
}

// benchmarkSetValues is a helper function to explore, very roughly, the cost
// of setting values.
func benchmarkSetValues(b *testing.B, qw qc.QueryWrite, bitDepth uint64, f *fragment, cfunc func(uint64) uint64) {
	column := uint64(0)
	for i := 0; i < b.N; i++ {
		// We're not checking the error because this is a benchmark.
		// That does mean the result could be completely wrong...
		_, _ = f.setValue(qw, column, bitDepth, int64(i))
		column = cfunc(column)
	}
}

// Benchmark performance of setValue for BSI ranges.
func BenchmarkFragment_SetValue(b *testing.B) {
	depths := []uint64{4, 8, 16}
	for _, bitDepth := range depths {
		name := fmt.Sprintf("Depth%d", bitDepth)
		f, _, qw := mustOpenFragment(b, OptFieldTypeSet("none", 0))

		b.Run(name+"_Sparse", func(b *testing.B) {
			benchmarkSetValues(b, qw, bitDepth, f, func(u uint64) uint64 { return (u + 70000) & (ShardWidth - 1) })
		})
		f.Clean(b)
		f, _, qw = mustOpenFragment(b, OptFieldTypeSet("none", 0))
		b.Run(name+"_Dense", func(b *testing.B) {
			benchmarkSetValues(b, qw, bitDepth, f, func(u uint64) uint64 { return (u + 1) & (ShardWidth - 1) })
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
func benchmarkImportValues(b *testing.B, qw qc.QueryWrite, bitDepth uint64, f *fragment, cfunc func(uint64) uint64) {
	batches := makeBenchmarkImportValueData(b, bitDepth, cfunc)
	for _, req := range batches {
		err := f.importValue(qw, req.ColumnIDs, req.Values, bitDepth, false)
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
		f, _, qw := mustOpenFragment(b)
		b.Run(name+"_Sparse", func(b *testing.B) {
			benchmarkImportValues(b, qw, bitDepth, f, func(u uint64) uint64 { return (u + 19) & (ShardWidth - 1) })
		})
		f.Clean(b)
		f, _, qw = mustOpenFragment(b)
		b.Run(name+"_Dense", func(b *testing.B) {
			benchmarkImportValues(b, qw, bitDepth, f, func(u uint64) uint64 { return (u + 1) & (ShardWidth - 1) })
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
// is a pretty good proxy for fragment size on disk).
func BenchmarkFragment_RepeatedSmallImports(b *testing.B) {
	for _, numUpdates := range []int{100} {
		for _, bitsPerUpdate := range []int{100, 1000} {
			for _, numRows := range []int{1000, 100000, 1000000} {
				b.Run(fmt.Sprintf("Rows%dUpdates%dBits%d", numRows, numUpdates, bitsPerUpdate), func(b *testing.B) {
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
						func() {
							f, qcx, qw := mustOpenFragment(b)
							defer f.Clean(b)
							defer qcx.Release()

							err := f.importRoaring(context.Background(), qw, getZipfRowsSliceRoaring(uint64(numRows), 1, 0, ShardWidth), false)
							if err != nil {
								b.Fatalf("importing base data for benchmark: %v", err)
							}
							b.StartTimer()
							for i := 0; i < numUpdates; i++ {
								err := f.bulkImportStandard(qw,
									updateRows[bitsPerUpdate*i:bitsPerUpdate*(i+1)],
									updateRows[bitsPerUpdate*i:bitsPerUpdate*(i+1)],
									&ImportOptions{},
								)
								if err != nil {
									b.Fatalf("doing small bulk import: %v", err)
								}
							}
						}()
					}
				})
			}
		}
	}
}

func BenchmarkFragment_RepeatedSmallImportsRoaring(b *testing.B) {
	for _, numUpdates := range []int{100} {
		for _, bitsPerUpdate := range []uint64{100, 1000} {
			for _, numRows := range []uint64{1000, 100000, 1000000} {
				b.Run(fmt.Sprintf("Rows%dUpdates%dBits%d", numRows, numUpdates, bitsPerUpdate), func(b *testing.B) {
					for a := 0; a < b.N; a++ {
						b.StopTimer()
						// build the update data set all at once - this will get applied
						// to a fragment in numUpdates batches
						func() {
							f, qcx, qw := mustOpenFragment(b)
							defer f.Clean(b)
							defer qcx.Release()

							err := f.importRoaring(context.Background(), qw, getZipfRowsSliceRoaring(numRows, 1, 0, ShardWidth), false)
							if err != nil {
								b.Fatalf("importing base data for benchmark: %v", err)
							}
							for i := 0; i < numUpdates; i++ {
								data := getUpdataRoaring(numRows, bitsPerUpdate, int64(i))
								b.StartTimer()
								err := f.importRoaring(context.Background(), qw, data, false)
								b.StopTimer()
								if err != nil {
									b.Fatalf("doing small roaring import: %v", err)
								}
							}
						}()
					}
				})
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

			b.Run(fmt.Sprintf("Updates%dVals%d", numUpdates, valsPerUpdate), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					b.StopTimer()
					func() {
						f, qcx, qw := mustOpenFragment(b)
						defer qcx.Release()

						err := f.importValue(qw, initialCols, initialVals, 21, false)
						if err != nil {
							b.Fatalf("initial value import: %v", err)
						}
						b.StartTimer()
						for j := 0; j < numUpdates; j++ {
							err := f.importValue(qw,
								updateCols[valsPerUpdate*j:valsPerUpdate*(j+1)],
								updateVals[valsPerUpdate*j:valsPerUpdate*(j+1)],
								21,
								false,
							)
							if err != nil {
								b.Fatalf("importing values: %v", err)
							}
						}
					}()

				}
			})
		}
	}
}

// Ensure a fragment can return the top n results.
func TestFragment_Top(t *testing.T) {
	f, _, qw := mustOpenFragment(t, OptFieldTypeSet(CacheTypeRanked, DefaultCacheSize))
	defer f.Clean(t)

	// Set bits on the rows 100, 101, & 102.
	f.mustSetBits(t, qw, 100, 1, 3, 200)
	f.mustSetBits(t, qw, 101, 1)
	f.mustSetBits(t, qw, 102, 1, 2)
	f.RecalculateCache()

	// Retrieve top rows.
	if pairs, err := f.top(qw, topOptions{N: 2}); err != nil {
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
	f, _, qw := mustOpenFragment(t, OptFieldTypeSet(CacheTypeRanked, DefaultCacheSize))
	defer f.Clean(t)

	// Create an intersecting input row.
	src := NewRow(1, 2, 3)

	// Set bits on various rows.
	f.mustSetBits(t, qw, 100, 1, 10, 11, 12)    // one intersection
	f.mustSetBits(t, qw, 101, 1, 2, 3, 4)       // three intersections
	f.mustSetBits(t, qw, 102, 1, 2, 4, 5, 6)    // two intersections
	f.mustSetBits(t, qw, 103, 1000, 1001, 1002) // no intersection
	f.RecalculateCache()

	// Retrieve top rows.
	if pairs, err := f.top(qw, topOptions{N: 3, Src: src}); err != nil {
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

	f, _, qw := mustOpenFragment(t, OptFieldTypeSet(CacheTypeRanked, DefaultCacheSize))
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
	err = f.importRoaring(context.Background(), qw, b.Bytes(), false)
	if err != nil {
		t.Fatalf("importing data: %v", err)
	}
	f.RecalculateCache()

	// Retrieve top rows.
	if pairs, err := f.top(qw, topOptions{N: 10, Src: src}); err != nil {
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
	f, _, qw := mustOpenFragment(t, OptFieldTypeSet(CacheTypeRanked, DefaultCacheSize))
	defer f.Clean(t)

	// Set bits on various rows.
	f.mustSetBits(t, qw, 100, 1, 2, 3)
	f.mustSetBits(t, qw, 101, 4, 5, 6, 7)
	f.mustSetBits(t, qw, 102, 8, 9, 10, 11, 12)

	// Retrieve top rows.
	if pairs, err := f.top(qw, topOptions{RowIDs: []uint64{100, 101, 200}}); err != nil {
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
	f, _, qw := mustOpenFragment(t, OptFieldTypeSet(CacheTypeNone, 0))
	defer f.Clean(t)

	// Set bits on various rows.
	f.mustSetBits(t, qw, 100, 1, 2, 3)
	f.mustSetBits(t, qw, 101, 4, 5, 6, 7)
	f.mustSetBits(t, qw, 102, 8, 9, 10, 11, 12)

	// Retrieve top rows.
	if pairs, err := f.top(qw, topOptions{RowIDs: []uint64{100, 101, 200}}); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(pairs, []Pair{}) {
		t.Fatalf("unexpected pairs: %s", spew.Sdump(pairs))
	}
}

// Ensure the fragment cache limit works
func TestFragment_TopN_CacheSize(t *testing.T) {
	cacheSize := uint32(3)

	f, _, qw := mustOpenFragment(t, OptFieldTypeSet(CacheTypeRanked, cacheSize))
	defer f.Clean(t)

	// Set bits on various rows.
	f.mustSetBits(t, qw, 100, 1, 2, 3)
	f.mustSetBits(t, qw, 101, 4, 5, 6, 7)
	f.mustSetBits(t, qw, 102, 8, 9, 10, 11, 12)
	f.mustSetBits(t, qw, 103, 8, 9, 10, 11, 12, 13)
	f.mustSetBits(t, qw, 104, 8, 9, 10, 11, 12, 13, 14)
	f.mustSetBits(t, qw, 105, 10, 11)

	f.RecalculateCache()

	p := []Pair{
		{ID: 104, Count: 7},
		{ID: 103, Count: 6},
		{ID: 102, Count: 5},
	}

	// Retrieve top rows.
	if pairs, err := f.top(qw, topOptions{N: 5}); err != nil {
		t.Fatal(err)
	} else if len(pairs) > int(cacheSize) {
		t.Fatalf("TopN count cannot exceed cache size: %d", cacheSize)
	} else if pairs[0] != (Pair{ID: 104, Count: 7}) {
		t.Fatalf("unexpected pair(0): %v", pairs)
	} else if !reflect.DeepEqual(pairs, p) {
		t.Fatalf("Invalid TopN result set: %s", spew.Sdump(pairs))
	}
}

// Ensure a fragment's cache can be persisted between restarts.
func TestFragment_LRUCache_Persistence(t *testing.T) {
	f, qcx, qw := mustOpenFragment(t, OptFieldTypeSet(CacheTypeLRU, 0))
	defer f.Clean(t)

	// Set bits on the fragment.
	for i := uint64(0); i < 1000; i++ {
		if _, err := f.setBit(qw, i, 0); err != nil {
			t.Fatal(err)
		}
	}

	// Verify correct cache type and size.
	if cache, ok := f.cache.(*lruCache); !ok {
		t.Fatalf("unexpected cache: %T", f.cache)
	} else if cache.Len() != 1000 {
		t.Fatalf("unexpected cache len: %d", cache.Len())
	}

	require.Nil(t, qcx.Commit())

	// Reopen the fragment.
	if err := f.Reopen(t); err != nil {
		t.Fatal(err)
	}

	// Re-verify correct cache type and size.
	if cache, ok := f.cache.(*lruCache); !ok {
		t.Fatalf("unexpected cache: %T", f.cache)
	} else if cache.Len() != 1000 {
		t.Fatalf("unexpected cache len: %d", cache.Len())
	}
}

func BenchmarkFragment_IntersectionCount(b *testing.B) {
	f, qcx, qw := mustOpenFragment(b)
	defer f.Clean(b)

	// Generate some intersecting data.
	for i := 0; i < 10000; i += 2 {
		if _, err := f.setBit(qw, 1, uint64(i)); err != nil {
			b.Fatal(err)
		}
	}
	for i := 0; i < 10000; i += 3 {
		if _, err := f.setBit(qw, 2, uint64(i)); err != nil {
			b.Fatal(err)
		}
	}

	require.Nil(b, qcx.Commit())
	_, qr := mustRead(b, f)

	// Start benchmark
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if n := f.mustRow(b, qw, 1).intersectionCount(f.mustRow(b, qr, 2)); n == 0 {
			b.Fatalf("unexpected count: %d", n)
		}
	}
}

func TestFragment_Tanimoto(t *testing.T) {
	f, _, qw := mustOpenFragment(t, OptFieldTypeSet(CacheTypeRanked, DefaultCacheSize))
	defer f.Clean(t)

	src := NewRow(1, 2, 3)

	// Set bits on the rows 100, 101, & 102.
	f.mustSetBits(t, qw, 100, 1, 3, 2, 200)
	f.mustSetBits(t, qw, 101, 1, 3)
	f.mustSetBits(t, qw, 102, 1, 2, 10, 12)
	f.RecalculateCache()

	if pairs, err := f.top(qw, topOptions{TanimotoThreshold: 50, Src: src}); err != nil {
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
	f, _, qw := mustOpenFragment(t, OptFieldTypeSet(CacheTypeRanked, DefaultCacheSize))
	defer f.Clean(t)

	src := NewRow(1, 2, 3)

	// Set bits on the rows 100, 101, & 102.
	f.mustSetBits(t, qw, 100, 1, 3, 2, 200)
	f.mustSetBits(t, qw, 101, 1, 3)
	f.mustSetBits(t, qw, 102, 1, 2, 10, 12)
	f.RecalculateCache()

	if pairs, err := f.top(qw, topOptions{TanimotoThreshold: 0, Src: src}); err != nil {
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

// Ensure a fragment can set mutually exclusive values.
func TestFragment_SetMutex(t *testing.T) {
	f, _, qw := mustOpenFragment(t, OptFieldTypeMutex(DefaultCacheType, DefaultCacheSize))
	defer f.Clean(t)

	var cols []uint64

	// Set a value on column 100.
	if _, err := f.setBit(qw, 1, 100); err != nil {
		t.Fatal(err)
	}
	// Verify the value was set.
	cols = f.mustRow(t, qw, 1).Columns()
	if !reflect.DeepEqual(cols, []uint64{100}) {
		t.Fatalf("mutex unexpected columns: %v", cols)
	}

	// Set a different value on column 100.
	if _, err := f.setBit(qw, 2, 100); err != nil {
		t.Fatal(err)
	}
	// Verify that value (row 1) was replaced (by row 2).
	cols = f.mustRow(t, qw, 1).Columns()
	if !reflect.DeepEqual(cols, []uint64{}) {
		t.Fatalf("mutex unexpected columns: %v", cols)
	}
	cols = f.mustRow(t, qw, 2).Columns()
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
			f, _, qw := mustOpenFragment(t)
			defer f.Clean(t)

			// Set import.
			err := f.bulkImport(qw, test.setRowIDs, test.setColIDs, &ImportOptions{})
			if err != nil {
				t.Fatalf("bulk importing ids: %v", err)
			}

			// Check for expected results.
			for k, v := range test.setExp {
				cols := f.mustRow(t, qw, k).Columns()
				if !reflect.DeepEqual(cols, v) {
					t.Fatalf("expected: %v, but got: %v", v, cols)
				}
			}

			// Clear import.
			err = f.bulkImport(qw, test.clearRowIDs, test.clearColIDs, &ImportOptions{Clear: true})
			if err != nil {
				t.Fatalf("bulk clearing ids: %v", err)
			}

			// Check for expected results.
			for k, v := range test.clearExp {
				cols := f.mustRow(t, qw, k).Columns()
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
			f, qcx, qw := mustOpenFragment(t)
			defer f.Clean(t)

			// Set import.
			err := f.bulkImport(qw, test.setRowIDs, test.setColIDs, &ImportOptions{})
			if err != nil {
				t.Fatalf("bulk importing ids: %v", err)
			}

			require.Nil(t, qcx.Commit())
			_, qr := mustRead(t, f)

			// Check for expected results.
			for k, v := range test.setExp {
				cols := f.mustRow(t, qr, k).Columns()
				if !reflect.DeepEqual(cols, v) {
					t.Fatalf("expected: %v, but got: %v", v, cols)
				}
			}

			qcx, qw = mustWrite(t, f)

			// Clear import.
			err = f.bulkImport(qw, test.clearRowIDs, test.clearColIDs, &ImportOptions{Clear: true})
			if err != nil {
				t.Fatalf("bulk clearing ids: %v", err)
			}

			require.Nil(t, qcx.Commit())
			_, qr = mustRead(t, f)

			// Check for expected results.
			for k, v := range test.clearExp {
				cols := f.mustRow(t, qr, k).Columns()
				if !reflect.DeepEqual(cols, v) {
					t.Fatalf("expected: %v, but got: %v", v, cols)
				}
			}
		})
	}
}

func TestFragment_ConcurrentImport(t *testing.T) {
	t.Run("bulkImportStandard", func(t *testing.T) {
		f, qcx, _ := mustOpenFragment(t)
		// we want to make new ones, so there can't be an existing write
		qcx.Release()
		defer f.Clean(t)

		eg := errgroup.Group{}
		eg.Go(func() error {
			qcx, qw := mustWrite(t, f)
			defer func() { require.Nil(t, qcx.Commit()) }()
			return f.bulkImportStandard(qw, []uint64{1, 2}, []uint64{1, 2}, &ImportOptions{})
		})
		eg.Go(func() error {
			qcx, qw := mustWrite(t, f)
			defer func() { require.Nil(t, qcx.Commit()) }()
			return f.bulkImportStandard(qw, []uint64{3, 4}, []uint64{3, 4}, &ImportOptions{})
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
			f, _, qw := mustOpenFragment(t, OptFieldTypeMutex(DefaultCacheType, DefaultCacheSize))
			defer f.Clean(t)

			// Set import.
			err := f.bulkImport(qw, test.setRowIDs, test.setColIDs, &ImportOptions{})
			if err != nil {
				t.Fatalf("bulk importing ids: %v", err)
			}

			// Check for expected results.
			for k, v := range test.setExp {
				cols := f.mustRow(t, qw, k).Columns()
				if !reflect.DeepEqual(cols, v) {
					t.Fatalf("row: %d, expected: %v, but got: %v", k, v, cols)
				}
			}

			// Clear import.
			err = f.bulkImport(qw, test.clearRowIDs, test.clearColIDs, &ImportOptions{Clear: true})
			if err != nil {
				t.Fatalf("bulk clearing ids: %v", err)
			}

			// Check for expected results.
			for k, v := range test.clearExp {
				cols := f.mustRow(t, qw, k).Columns()
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
			f, qcx, qw := mustOpenFragment(t, OptFieldTypeMutex(DefaultCacheType, DefaultCacheSize))
			defer f.Clean(t)

			// Set import.
			err := f.bulkImport(qw, test.setRowIDs, test.setColIDs, &ImportOptions{})
			if err != nil {
				t.Fatalf("bulk importing ids: %v", err)
			}

			require.Nil(t, qcx.Commit())
			qcx, qr := mustRead(t, f)

			// Check for expected results.
			for k, v := range test.setExp {
				cols := f.mustRow(t, qr, k).Columns()
				if !reflect.DeepEqual(cols, v) {
					t.Fatalf("row: %d, expected: %v, but got: %v", k, v, cols)
				}
			}
			qcx.Release()

			qcx, qw = mustWrite(t, f)

			// Clear import.
			err = f.bulkImport(qw, test.clearRowIDs, test.clearColIDs, &ImportOptions{Clear: true})
			if err != nil {
				t.Fatalf("bulk clearing ids: %v", err)
			}

			require.Nil(t, qcx.Commit())

			_, qr = mustRead(t, f)

			// Check for expected results.
			for k, v := range test.clearExp {
				cols := f.mustRow(t, qr, k).Columns()
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
			f, _, qw := mustOpenFragment(t, OptFieldTypeBool())
			defer f.Clean(t)

			// Set import.
			err := f.bulkImport(qw, test.setRowIDs, test.setColIDs, &ImportOptions{})
			if err != nil {
				t.Fatalf("bulk importing ids: %v", err)
			}

			// Check for expected results.
			for k, v := range test.setExp {
				cols := f.mustRow(t, qw, k).Columns()
				if !reflect.DeepEqual(cols, v) {
					t.Fatalf("expected: %v, but got: %v", v, cols)
				}
			}

			// Clear import.
			err = f.bulkImport(qw, test.clearRowIDs, test.clearColIDs, &ImportOptions{Clear: true})
			if err != nil {
				t.Fatalf("bulk importing ids: %v", err)
			}

			// Check for expected results.
			for k, v := range test.clearExp {
				cols := f.mustRow(t, qw, k).Columns()
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
			f, qcx, qw := mustOpenFragment(t, OptFieldTypeBool())
			defer f.Clean(t)

			// Set import.
			err := f.bulkImport(qw, test.setRowIDs, test.setColIDs, &ImportOptions{})
			if err != nil {
				t.Fatalf("bulk importing ids: %v", err)
			}

			require.Nil(t, qcx.Commit())
			qcx, qr := mustRead(t, f)

			// Check for expected results.
			for k, v := range test.setExp {
				cols := f.mustRow(t, qr, k).Columns()
				if !reflect.DeepEqual(cols, v) {
					t.Fatalf("expected: %v, but got: %v", v, cols)
				}
			}

			qcx.Release()
			qcx, qw = mustWrite(t, f)

			// Clear import.
			err = f.bulkImport(qw, test.clearRowIDs, test.clearColIDs, &ImportOptions{Clear: true})
			if err != nil {
				t.Fatalf("bulk importing ids: %v", err)
			}

			require.Nil(t, qcx.Commit())
			_, qr = mustRead(t, f)

			// Check for expected results.
			for k, v := range test.clearExp {
				cols := f.mustRow(t, qr, k).Columns()
				if !reflect.DeepEqual(cols, v) {
					t.Fatalf("expected: %v, but got: %v", v, cols)
				}
			}
		})
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
		func() {
			f, qcx, qw := mustOpenFragment(b)
			defer f.Clean(b)
			defer qcx.Release()
			b.StartTimer()
			if err := f.bulkImport(qw, rowsUse, colsUse, options); err != nil {
				b.Errorf("Error Building Sample: %s", err)
			}
			b.StopTimer()
		}()
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
					f, _, qw := mustOpenFragment(b, OptFieldTypeSet(cacheType, 0))
					b.StartTimer()

					err := f.importRoaring(context.Background(), qw, data, false)
					if err != nil {
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
					qcxs := make([]qc.QueryContext, concurrency)
					qws := make([]qc.QueryWrite, concurrency)
					for i := 0; i < b.N; i++ {
						for j := 0; j < concurrency; j++ {
							frags[j], qcxs[j], qws[j] = mustOpenFragment(b, OptFieldTypeSet(cacheType, 0))
						}
						eg := errgroup.Group{}
						b.StartTimer()
						for j := 0; j < concurrency; j++ {
							j := j
							eg.Go(func() error {
								defer qcxs[j].Release()

								err := frags[j].importRoaring(context.Background(), qws[j], data[j], false)
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
					func() {
						f, qcx, qw := mustOpenFragment(b, OptFieldTypeSet(cacheType, 0))
						defer f.Clean(b)
						defer qcx.Release()
						b.StartTimer()
						err := f.bulkImport(qw, rowIDs, columnIDs, &ImportOptions{})
						if err != nil {
							b.Errorf("import error: %v", err)
						}
						b.StopTimer()
					}()
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
						f, _, qw := mustOpenFragment(b, OptFieldTypeSet(cacheType, 0))

						itr, err := roaring.NewRoaringIterator(data)
						PanicOn(err)
						_, _, err = qw.ImportRoaringBits(itr, false, 0)
						if err != nil {
							b.Errorf("import error: %v", err)
						}
						b.StartTimer()
						err = f.importRoaring(context.Background(), qw, updata, false)
						if err != nil {
							f.Clean(b)
							b.Errorf("import error: %v", err)
						}
						b.StopTimer()
						var stat os.FileInfo
						var statTarget io.Writer
						err = func() error {
							targetFile, ok := statTarget.(*os.File)
							if ok {
								stat, _ = targetFile.Stat()
							} else {
								b.Errorf("couldn't stat file")
							}
							return nil
						}()
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
		func() {
			f, qcx, qw := mustOpenFragment(b, OptFieldTypeSet(DefaultCacheType, 0))
			defer qcx.Release()

			err := f.importRoaring(context.Background(), qw, exists, false)
			if err != nil {
				b.Fatalf("importing roaring: %v", err)
			}
			b.StartTimer()
			err = f.importRoaring(context.Background(), qw, inc, false)
			if err != nil {
				b.Fatalf("importing second: %v", err)
			}
		}()
	}

}

var bigFrag string

func initBigFrag(tb testing.TB) {
	if bigFrag == "" {
		f, qcx, qw := mustOpenFragment(tb, OptFieldTypeSet(DefaultCacheType, 0))
		for i := int64(0); i < 10; i++ {
			// 10 million rows, 1 bit per column, random seeded by i
			data := getZipfRowsSliceRoaring(10000000, i, 0, ShardWidth)
			err := f.importRoaring(context.Background(), qw, data, false)
			if err != nil {
				PanicOn(fmt.Sprintf("setting up fragment data: %v", err))
			}
		}
		err := f.Close()
		if err != nil {
			PanicOn(fmt.Sprintf("closing fragment: %v", err))
		}
		bigFrag = f.path()
		require.Nil(tb, qcx.Commit())
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
		fi, err := testhook.TempFile(b, "")
		if err != nil {
			b.Fatalf("getting temp file: %v", err)
		}
		_, err = io.Copy(fi, origF)
		if err != nil {
			b.Fatalf("copying fragment file: %v", err)
		}
		origF.Close()
		fi.Close()

		func() {
			f, qcx, qw := mustOpenFragment(b)
			defer qcx.Release()
			copy(rows, rowsOrig)
			copy(cols, colsOrig)
			b.StartTimer()
			err = f.bulkImport(qw, rows, cols, opts)
			b.StopTimer()
			if err != nil {
				b.Fatalf("bulkImport: %v", err)
			}
			require.Nil(b, qcx.Commit())
			f.Clean(b)
		}()
	}
}

func BenchmarkImportRoaringIntoLargeFragment(b *testing.B) {
	b.StopTimer()
	initBigFrag(b)
	updata := getUpdataRoaring(10000000, 11000, 0)
	for i := 0; i < b.N; i++ {
		origF, err := os.Open(bigFrag)
		if err != nil {
			b.Fatalf("opening frag file: %v", err)
		}
		fi, err := testhook.TempFile(b, "")
		if err != nil {
			b.Fatalf("getting temp file: %v", err)
		}
		_, err = io.Copy(fi, origF)
		if err != nil {
			b.Fatalf("copying fragment file: %v", err)
		}
		origF.Close()
		fi.Close()
		func() {
			f, qcx, qw := mustOpenFragment(b)
			defer f.Clean(b)
			defer qcx.Release()
			b.StartTimer()
			err = f.importRoaring(context.Background(), qw, updata, false)
			b.StopTimer()
			if err != nil {
				b.Fatalf("bulkImport: %v", err)
			}
		}()
	}
}

func TestGetZipfRowsSliceRoaring(t *testing.T) {
	f, _, qw := mustOpenFragment(t, OptFieldTypeSet(DefaultCacheType, 0))

	data := getZipfRowsSliceRoaring(10, 1, 0, ShardWidth)
	err := f.importRoaring(context.Background(), qw, data, false)
	if err != nil {
		t.Fatalf("importing roaring: %v", err)
	}
	rows, err := f.rows(context.Background(), qw, 0)
	if err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(rows, []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}) {
		t.Fatalf("unexpected rows: %v", rows)
	}
	for i := uint64(1); i < 10; i++ {
		if f.mustRow(t, qw, i).Count() >= f.mustRow(t, qw, i-1).Count() {
			t.Fatalf("suspect distribution from getZipfRowsSliceRoaring")
		}
	}
	f.Clean(t)
}

func prepareSampleRowData(b *testing.B, bits int, rows uint64, width uint64) (*fragment, qc.QueryContext, qc.QueryWrite) {
	f, qcx, qw := mustOpenFragment(b, OptFieldTypeSet("none", 0))
	for i := 0; i < bits; i++ {
		data := getUniformRowsSliceRoaring(rows, int64(rows)+int64(i), 0, width)
		err := f.importRoaring(context.Background(), qw, data, false)
		if err != nil {
			b.Fatalf("creating sample data: %v", err)
		}
	}
	return f, qcx, qw
}

type txFrag struct {
	rows, width uint64
	qr          qc.QueryRead // may actually secretly be a querywrite
	frag        *fragment
}

func benchmarkRowsOnTestcase(b *testing.B, ctx context.Context, txf txFrag) {
	col := uint64(0)
	for i := 0; i < b.N; i++ {
		_, err := txf.frag.rows(ctx, txf.qr, 0, roaring.NewBitmapColumnFilter(col))
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
		var qr qc.QueryRead
		f, qcx, qw := prepareSampleRowData(b, 3, rows, ShardWidth)
		qr = qw
		if !writable {
			err := qcx.Commit()
			if err != nil {
				b.Fatalf("error committing sample data: %v", err)
			}
			_, qr = mustRead(b, f)
		}
		testCases = append(testCases, txFrag{
			rows:  rows,
			width: ShardWidth,
			frag:  f,
			qr:    qr,
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

// Clean used to delete fragments, but doesn't anymore -- deleting is
// handled by the testhook.TempDir when appropriate.
// TODO(jaffee): this can likely go away entirely... it was doing snapshot/source/generation stuff that it no longer needs to.
func (f *fragment) Clean(t testing.TB) {
	errc := f.Close()
	if errc != nil {
		t.Fatalf("error closing fragment: %v", errc)
	}
}

func newTestHolder(tb testing.TB) *Holder {
	path := tb.TempDir()
	h, err := NewHolder(path, TestHolderConfig())
	if err != nil {
		tb.Fatalf("creating test holder: %v", err)
	}
	err = h.Open()
	if err != nil {
		tb.Fatalf("opening test holder: %v", err)
	}
	testhook.Cleanup(tb, func() {
		h.Close()
	})

	return h
}

// newTestField creates a field with the specified field options
func newTestField(tb testing.TB, fieldOpts ...FieldOption) (*Holder, *Index, *Field) {
	if len(fieldOpts) == 0 {
		fieldOpts = []FieldOption{OptFieldTypeDefault()}
	}
	h := newTestHolder(tb)
	idx, err := h.CreateIndex("i", "", IndexOptions{})
	if err != nil {
		tb.Fatalf("creating test index: %v", err)
	}
	fld, err := idx.CreateField("f", "", fieldOpts...)
	if err != nil {
		tb.Fatalf("creating test field: %v", err)
	}
	return h, idx, fld
}

// newTestView creates a view with the specified options.
func newTestView(tb testing.TB, fieldOpts ...FieldOption) (*Holder, *Index, *Field, *view) {
	h, idx, fld := newTestField(tb, fieldOpts...)
	v, _, err := fld.createViewIfNotExistsBase(&CreateViewMessage{Index: "i", Field: "f", View: "v"})
	if err != nil {
		tb.Fatalf("creating test view: %v", err)
	}
	return h, idx, fld, v
}

// newTestFragment makes the default /i/f/v/0 fragment, and returns the
// things. the test holder will be deleted automatically in test cleanup.
func newTestFragment(tb testing.TB, fieldOpts ...FieldOption) (*Holder, *Index, *Field, *view, *fragment) {
	h, idx, fld, v := newTestView(tb, fieldOpts...)
	f := v.newFragment(0)
	return h, idx, fld, v, f
}

// mustOpenFragment returns a new instance of Fragment with a temporary path.
// It returns an initial QueryContext and QueryWrite attached to that fragment,
// because it turns out we nearly always want to write to a fragment we're
// creating for testing.
func mustOpenFragment(tb testing.TB, fieldOpts ...FieldOption) (*fragment, qc.QueryContext, qc.QueryWrite) {
	_, idx, fld, v, f := newTestFragment(tb, fieldOpts...)
	fragDir := filepath.Join(idx.path, fld.name, "views", v.name, "fragments")
	err := os.MkdirAll(fragDir, 0700)
	if err != nil {
		tb.Fatalf("creating fragment directory: %v", err)
	}

	f.CacheType = fld.options.CacheType

	// Note this horrible crime: We create our QueryContext for this fragment
	// *before* we open the fragment, because we need a QueryContext for the
	// open. Even though in fact nothing ever happens -- we just made a new fragment
	// so there can't be an existing cache, we hope.
	qcx, qw := mustWrite(tb, f)
	if err := f.Open(); err != nil {
		tb.Fatalf("opening fragment: %v", err)
	}
	return f, qcx, qw
}

// Reopen closes the fragment and reopens it as a new instance.
//
// This... almost certainly doesn't really mean anything anymore.
// In the days of Roaring, this meant flushing all our data structures and
// re-reading from disk. Now, with RBF, we're not closing the underlying
// database, or anything...
func (f *fragment) Reopen(tb testing.TB) error {
	if err := f.Close(); err != nil {
		return err
	}
	if err := f.Open(); err != nil {
		tb.Fatalf("opening fragment: %v", err)
	}
	return nil
}

// mustSetBits sets columns on a row. Panic on error.
// This function does not accept a timestamp or quantum.
func (f *fragment) mustSetBits(tb testing.TB, qw qc.QueryWrite, rowID uint64, columnIDs ...uint64) {
	for _, columnID := range columnIDs {
		if _, err := f.setBit(qw, rowID, columnID); err != nil {
			require.Nil(tb, err)
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
		f, _, qw := mustOpenFragment(t)
		defer f.Clean(t)

		expectedAll := make([]uint64, 0)
		expectedOdd := make([]uint64, 0)
		for i := uint64(100); i < uint64(200); i++ {
			if _, err := f.setBit(qw, i, i%2); err != nil {
				t.Fatal(err)
			}
			expectedAll = append(expectedAll, i)
			if i%2 == 1 {
				expectedOdd = append(expectedOdd, i)
			}
		}

		ids, err := f.rows(context.Background(), qw, 0)
		if err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(expectedAll, ids) {
			t.Fatalf("Do not match %v %v", expectedAll, ids)
		}

		ids, err = f.rows(context.Background(), qw, 0, roaring.NewBitmapColumnFilter(1))
		if err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(expectedOdd, ids) {
			t.Fatalf("Do not match %v %v", expectedOdd, ids)
		}
	})

	t.Run("secondRow", func(t *testing.T) {
		f, qcx, qw := mustOpenFragment(t)

		defer f.Clean(t)

		expected := []uint64{1, 2}
		if _, err := f.setBit(qw, 1, 66000); err != nil {
			t.Fatal(err)
		} else if _, err := f.setBit(qw, 2, 66000); err != nil {
			t.Fatal(err)
		} else if _, err := f.setBit(qw, 2, 166000); err != nil {
			t.Fatal(err)
		}
		require.Nil(t, qcx.Commit())

		_, qr := mustRead(t, f)

		ids, err := f.rows(context.Background(), qr, 0)
		if err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(expected, ids) {
			t.Fatalf("Do not match %v %v", expected, ids)
		}

		ids, err = f.rows(context.Background(), qr, 0, roaring.NewBitmapColumnFilter(66000))
		if err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(expected, ids) {
			t.Fatalf("Do not match %v %v", expected, ids)
		}
	})

	t.Run("combinations", func(t *testing.T) {
		f, _, qw := mustOpenFragment(t)

		defer f.Clean(t)

		expectedRows := make([]uint64, 0)
		for r := uint64(1); r < uint64(10000); r += 250 {
			expectedRows = append(expectedRows, r)
			for c := uint64(1); c < uint64(ShardWidth-1); c += (ShardWidth >> 5) {
				if _, err := f.setBit(qw, r, c); err != nil {
					t.Fatal(err)
				}

				ids, err := f.rows(context.Background(), qw, 0)
				if err != nil {
					t.Fatal(err)
				} else if !reflect.DeepEqual(expectedRows, ids) {
					t.Fatalf("Do not match %v %v", expectedRows, ids)
				}
				ids, err = f.rows(context.Background(), qw, 0, roaring.NewBitmapColumnFilter(c))
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
			f, _, qw := mustOpenFragment(t)
			defer f.Clean(t)

			for num, input := range test {
				buf := &bytes.Buffer{}
				bm := roaring.NewBitmap(input...)
				_, err := bm.WriteTo(buf)
				if err != nil {
					t.Fatalf("writing to buffer: %v", err)
				}
				err = f.importRoaring(context.Background(), qw, buf.Bytes(), false)
				if err != nil {
					t.Fatalf("importing roaring: %v", err)
				}

				exp := calcExpected(test[:num+1]...)
				for row, expCols := range exp {
					cols := f.mustRow(t, qw, uint64(row)).Columns()
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
			f, _, qw := mustOpenFragment(t, OptFieldTypeSet(CacheTypeRanked, DefaultCacheSize))
			defer f.Clean(t)

			options := &ImportOptions{}
			err := f.bulkImport(qw, test.rowIDs, test.colIDs, options)
			if err != nil {
				t.Fatalf("bulk importing ids: %v", err)
			}

			expPairs := calcTop(test.rowIDs, test.colIDs)
			pairs, err := f.top(qw, topOptions{})
			if err != nil {
				t.Fatalf("executing top after bulk import: %v", err)
			}
			if !reflect.DeepEqual(expPairs, pairs) {
				t.Fatalf("post bulk import:\n  exp: %v\n  got: %v\n", expPairs, pairs)
			}

			err = f.bulkImport(qw, test.rowIDs2, test.colIDs2, options)
			if err != nil {
				t.Fatalf("bulk importing ids: %v", err)
			}
			test.rowIDs = append(test.rowIDs, test.rowIDs2...)
			test.colIDs = append(test.colIDs, test.colIDs2...)
			expPairs = calcTop(test.rowIDs, test.colIDs)
			pairs, err = f.top(qw, topOptions{})
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
			err = f.importRoaring(context.Background(), qw, buf.Bytes(), false)
			if err != nil {
				t.Fatalf("importing roaring: %v", err)
			}
			rows, cols := toRowsCols(test.roaring)
			expPairs = calcTop(append(test.rowIDs, rows...), append(test.colIDs, cols...))
			pairs, err = f.top(qw, topOptions{})
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
		f, _, qw := mustOpenFragment(t, OptFieldTypeSet(CacheTypeRanked, DefaultCacheSize))

		defer f.Clean(t)

		f.mustSetBits(t, qw, 0, 0)
		f.mustSetBits(t, qw, 1, 0)
		f.mustSetBits(t, qw, 2, 0)
		f.mustSetBits(t, qw, 3, 0)

		iter, err := f.rowIterator(qw, false)
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
		f, _, qw := mustOpenFragment(t, OptFieldTypeSet(CacheTypeRanked, DefaultCacheSize))
		defer f.Clean(t)

		f.mustSetBits(t, qw, 1, 0)
		f.mustSetBits(t, qw, 3, 0)
		f.mustSetBits(t, qw, 5, 0)
		f.mustSetBits(t, qw, 7, 0)

		iter, err := f.rowIterator(qw, false)
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
		f, _, qw := mustOpenFragment(t, OptFieldTypeSet(CacheTypeRanked, DefaultCacheSize))
		defer f.Clean(t)

		f.mustSetBits(t, qw, 0, 0)
		f.mustSetBits(t, qw, 1, 0)
		f.mustSetBits(t, qw, 2, 0)
		f.mustSetBits(t, qw, 3, 0)

		iter, err := f.rowIterator(qw, true)
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
		f, _, qw := mustOpenFragment(t, OptFieldTypeSet(CacheTypeRanked, DefaultCacheSize))
		defer f.Clean(t)

		f.mustSetBits(t, qw, 1, 0)
		f.mustSetBits(t, qw, 3, 0)
		f.mustSetBits(t, qw, 5, 0)
		f.mustSetBits(t, qw, 7, 0)

		iter, err := f.rowIterator(qw, true)
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
		f, qcx, qw := mustOpenFragment(t, OptFieldTypeSet(CacheTypeRanked, DefaultCacheSize))

		defer f.Clean(t)

		f.mustSetBits(t, qw, 0, 0)
		f.mustSetBits(t, qw, 1, 0)
		f.mustSetBits(t, qw, 2, 0)
		f.mustSetBits(t, qw, 3, 0)

		require.Nil(t, qcx.Commit())
		_, qr := mustRead(t, f)

		iter, err := f.rowIterator(qr, false)
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
		f, qcx, qw := mustOpenFragment(t, OptFieldTypeSet(CacheTypeRanked, DefaultCacheSize))
		defer f.Clean(t)

		f.mustSetBits(t, qw, 1, 0)
		f.mustSetBits(t, qw, 3, 0)
		f.mustSetBits(t, qw, 5, 0)
		f.mustSetBits(t, qw, 7, 0)

		require.Nil(t, qcx.Commit())
		_, qr := mustRead(t, f)

		iter, err := f.rowIterator(qr, false)
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
		f, qcx, qw := mustOpenFragment(t, OptFieldTypeSet(CacheTypeRanked, DefaultCacheSize))
		defer f.Clean(t)

		f.mustSetBits(t, qw, 0, 0)
		f.mustSetBits(t, qw, 1, 0)
		f.mustSetBits(t, qw, 2, 0)
		f.mustSetBits(t, qw, 3, 0)

		require.Nil(t, qcx.Commit())
		_, qr := mustRead(t, f)

		iter, err := f.rowIterator(qr, true)
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
		f, qcx, qw := mustOpenFragment(t, OptFieldTypeSet(CacheTypeRanked, DefaultCacheSize))
		defer f.Clean(t)

		f.mustSetBits(t, qw, 1, 0)
		f.mustSetBits(t, qw, 3, 0)
		f.mustSetBits(t, qw, 5, 0)
		f.mustSetBits(t, qw, 7, 0)

		require.Nil(t, qcx.Commit())
		_, qr := mustRead(t, f)

		iter, err := f.rowIterator(qr, true)
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

func TestFragmentPositionsForValue(t *testing.T) {
	// We don't need the querycontext or querywrite because positionsForValue
	// doesn't actually talk to the fragment in any way, it's just math.
	f, _, _ := mustOpenFragment(t, OptFieldTypeSet(CacheTypeNone, 0))
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
	f, _, qw := mustOpenFragment(t, OptFieldTypeSet(CacheTypeNone, 0))
	defer f.Clean(t)

	_, err := f.setValue(qw, 1, 6, 33)
	if err != nil {
		t.Fatalf("setting value: %v", err)
	}

	row, err := f.rangeOp(qw, pql.LT, 6, 33)
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
	f, qcx, qw := mustOpenFragment(t, OptFieldTypeSet(CacheTypeNone, 0))
	defer f.Clean(t)

	// Number of bits to test.
	const k = 6

	// Load all numbers into an effectively diagonal matrix.
	for i := 0; i < 1<<k; i++ {
		ok, err := f.setValue(qw, uint64(i), k, int64(i))
		if err != nil {
			t.Fatalf("failed to set col %d to %d: %v", uint64(i), int64(i), err)
		} else if !ok {
			t.Fatalf("no change when setting col %d to %d", uint64(i), int64(i))
		}
	}
	require.Nil(t, qcx.Commit()) // t.Run beolow on different goro and so need their own Tx anyway.

	// Generate a list of columns.
	cols := make([]uint64, 1<<k)
	for i := range cols {
		cols[i] = uint64(i)
	}

	// Minimum and maximum checking bounds.
	// This is mostly arbitrary.
	minCheck, maxCheck := -3, 1<<(k+1)

	t.Run("<", func(t *testing.T) {
		_, qr := mustRead(t, f)

		for i := minCheck; i < maxCheck; i++ {
			row, err := f.rangeLT(qr, k, int64(i), false)
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
		_, qr := mustRead(t, f)

		for i := minCheck; i < maxCheck; i++ {
			row, err := f.rangeLT(qr, k, int64(i), true)
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
		_, qr := mustRead(t, f)

		for i := minCheck; i < maxCheck; i++ {
			row, err := f.rangeGT(qr, k, int64(i), false)
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
		_, qr := mustRead(t, f)

		for i := minCheck; i < maxCheck; i++ {
			row, err := f.rangeGT(qr, k, int64(i), true)
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
		_, qr := mustRead(t, f)

		for i := minCheck; i < maxCheck; i++ {
			for j := i; j < maxCheck; j++ {
				row, err := f.rangeBetween(qr, k, int64(i), int64(j))
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
		_, qr := mustRead(t, f)

		for i := minCheck; i < maxCheck; i++ {
			row, err := f.rangeEQ(qr, k, int64(i))
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
	f, qcx, qw := mustOpenFragment(t, OptFieldTypeSet(CacheTypeNone, 0))
	defer f.Clean(t)

	// Number of bits to test.
	const k = 6

	// Load all numbers into an effectively diagonal matrix.
	for i := 0; i < 1<<k; i++ {
		ok, err := f.setValue(qw, uint64(i), k, int64(i))
		if err != nil {
			t.Fatalf("failed to set col %d to %d: %v", uint64(i), int64(i), err)
		} else if !ok {
			t.Fatalf("no change when setting col %d to %d", uint64(i), int64(i))
		}
	}

	require.Nil(t, qcx.Commit())
	_, qr := mustRead(t, f)

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
			row, err := f.rangeLT(qr, k, int64(i), false)
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
			row, err := f.rangeLT(qr, k, int64(i), true)
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
			row, err := f.rangeGT(qr, k, int64(i), false)
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
			row, err := f.rangeGT(qr, k, int64(i), true)
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
				row, err := f.rangeBetween(qr, k, int64(i), int64(j))
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
			row, err := f.rangeEQ(qr, k, int64(i))
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
	f, qcx, qw := mustOpenFragment(t, OptFieldTypeSet(CacheTypeNone, 0))
	defer f.Clean(t)

	// Number of bits to test.
	const k = 6

	// Load all numbers into an effectively diagonal matrix.
	minVal, maxVal := 1-(1<<k), (1<<k)-1
	for i := minVal; i <= maxVal; i++ {
		ok, err := f.setValue(qw, uint64(i-minVal), k, int64(i))
		if err != nil {
			t.Fatalf("failed to set col %d to %d: %v", uint64(i-minVal), int64(i), err)
		} else if !ok {
			t.Fatalf("no change when setting col %d to %d", uint64(i-minVal), int64(i))
		}
	}

	require.Nil(t, qcx.Commit())
	_, qr := mustRead(t, f)

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
			row, err := f.rangeLT(qr, k, int64(i), false)
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
			row, err := f.rangeLT(qr, k, int64(i), true)
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
			row, err := f.rangeGT(qr, k, int64(i), false)
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
			row, err := f.rangeGT(qr, k, int64(i), true)
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
				row, err := f.rangeBetween(qr, k, int64(i), int64(j))
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
			row, err := f.rangeEQ(qr, k, int64(i))
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

func TestImportValueConcurrent(t *testing.T) {
	f, qcx, _ := mustOpenFragment(t)
	defer f.Clean(t)
	// we will be making a new QueryContext each time, so we can rollback the default provided one.
	qcx.Release()

	eg := &errgroup.Group{}
	for i := 0; i < 4; i++ {
		i := i
		eg.Go(func() error {
			qcx, qw := mustWrite(t, f)
			defer qcx.Release()
			for j := uint64(0); j < 10; j++ {
				err := f.importValue(qw, []uint64{j}, []int64{int64(rand.Int63n(1000))}, 10, i%2 == 0)
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
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			f, _, qw := mustOpenFragment(t)
			defer f.Clean(t)

			err := f.importValue(qw, test.cols, test.vals, test.depth, false)
			if err != nil {
				t.Fatalf("importing values: %v", err)
			}

			for i := range test.checkCols {
				cc, cv := test.checkCols[i], test.checkVals[i]
				n, exists, err := f.value(qw, cc, test.depth)
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
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			f, _, qw := mustOpenFragment(t)
			defer f.Clean(t)

			// First import (tc1)
			if err := f.importValue(qw, test.tc1.cols, test.tc1.vals, test.tc1.depth, false); err != nil {
				t.Fatalf("importing values: %v", err)
			}

			if r, err := f.rangeOp(qw, pql.GT, test.tc1.depth, 0); err != nil {
				t.Error("getting range of values")
			} else if !reflect.DeepEqual(r.Columns(), test.tc1.checkCols) {
				t.Errorf("wrong column values. expected: %v, but got: %v", test.tc1.checkCols, r.Columns())
			}

			// Second import (tc2)
			if err := f.importValue(qw, test.tc2.cols, test.tc2.vals, test.tc2.depth, false); err != nil {
				t.Fatalf("importing values: %v", err)
			}

			if r, err := f.rangeOp(qw, pql.GT, test.tc2.depth, 0); err != nil {
				t.Error("getting range of values")
			} else if !reflect.DeepEqual(r.Columns(), test.tc2.checkCols) {
				t.Errorf("wrong column values. expected: %v, but got: %v", test.tc2.checkCols, r.Columns())
			}
		})
	}
}

// part of copy-on-write patch: test for races
// do we see races/corruption around concurrent read/write.
// especially on writes to the row cache.
func TestFragmentConcurrentReadWrite(t *testing.T) {
	f, qcx, _ := mustOpenFragment(t, OptFieldTypeSet(CacheTypeRanked, DefaultCacheSize))
	defer f.Clean(t)
	qcx.Release()

	eg := &errgroup.Group{}
	eg.Go(func() error {
		qcx, qw := mustWrite(t, f)

		for i := uint64(0); i < 1000; i++ {
			_, err := f.setBit(qw, i%4, i)
			if err != nil {
				return errors.Wrap(err, "setting bit")
			}
		}
		require.Nil(t, qcx.Commit())
		return nil
	})

	_, qr := mustRead(t, f)

	acc := uint64(0)
	for i := uint64(0); i < 100; i++ {
		r := f.mustRow(t, qr, i%4)
		acc += r.Count()
	}
	if err := eg.Wait(); err != nil {
		t.Errorf("error from setting a bit: %v", err)
	}
}

func TestFragment_Bug_Q2DoubleDelete(t *testing.T) {
	f, _, qw := mustOpenFragment(t)
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

	defer f.Clean(t)

	err := f.importRoaring(context.Background(), qw, b, false)
	if err != nil {
		t.Fatalf("importing roaring: %v", err)
	}

	//check the bit
	res := f.mustRow(t, qw, 1).Columns()
	if len(res) < 1 || f.mustRow(t, qw, 1).Columns()[0] != 1 {
		t.Fatalf("expecting 1 got: %v", res)
	}
	//clear the bit
	changed, _ := f.clearBit(qw, 1, 1)
	if !changed {
		t.Fatalf("expected change got %v", changed)
	}

	//check missing
	res = f.mustRow(t, qw, 1).Columns()
	if len(res) != 0 {
		t.Fatalf("expected nothing got %v", res)
	}

	// import again
	err = f.importRoaring(context.Background(), qw, b, false)
	if err != nil {
		t.Fatalf("importing roaring: %v", err)
	}

	//check
	res = f.mustRow(t, qw, 1).Columns()
	if len(res) < 1 || f.mustRow(t, qw, 1).Columns()[0] != 1 {
		t.Fatalf("again expecting 1 got: %v", res)
	}

	changed, _ = f.clearBit(qw, 1, 1)
	if !changed {
		t.Fatalf("again expected change got %v", changed) // again expected change got false
	}

	//check missing
	res = f.mustRow(t, qw, 1).Columns()
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
	// {"64K", 16},
	// {"32K", 15}, // 50-50
	//{"16K", 14}, // 1/4
	{"8K", 13},
	// {"4K", 12}, // a fair number of things
	{"1K", 10},
	// {"1", 0},   // about one per container
	// {"empty", -14}, // almost none
}

var mutexSizes = []mutexSize{
	// {"4r", 2},
	{"16r", 4},
	// {"256r", 8},
	// {"2Kr", 11},
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
			f, _, qw := mustOpenFragment(t, OptFieldTypeMutex(DefaultCacheType, DefaultCacheSize))
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
					err = f.bulkImport(qw, rows[j:max:max], cols[j:max:max], &ImportOptions{})
					if err != nil {
						t.Fatalf("bulk importing ids [%d/3] [%d:%d]: %v", i+1, j, max, err)
					}
				}
				count := uint64(0)
				for k := uint32(0); k < data.rng.rows(); k++ {
					c := f.mustRow(t, qw, uint64(k)).Count()
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
	var f *fragment
	var qcx qc.QueryContext
	var qw qc.QueryWrite
	benchmarkOneFragmentImports := func(b *testing.B, idx int) {
		cols, rows = data.scratchSpace(idx, cols, rows)
		toDo := b.N << 16
		start := 0
		for toDo > 0 {
			max := start + batchSize
			if len(cols) < max {
				max = len(cols)
			}
			err := f.bulkImport(qw, rows[start:max:max], cols[start:max:max], &ImportOptions{})
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
		f, qcx, qw = mustOpenFragment(b, OptFieldTypeMutex(cache, DefaultCacheSize))
		defer f.Clean(b)
		defer qcx.Release()
		for i := range data.colIDs {
			b.Run(fmt.Sprintf("write-%d", i), func(b *testing.B) {
				benchmarkOneFragmentImports(b, i)
			})
			// Then commit that write and do another one as a new Tx.
			err := qcx.Commit()
			if err != nil {
				b.Fatalf("error commiting write: %v", err)
			}
			qcx, qw = mustWrite(b, f)
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

func TestImportRoaringSingleValued(t *testing.T) {
	f, _, qw := mustOpenFragment(t)
	defer f.Clean(t)

	clear := roaring.NewBitmap(0, 1, ShardWidth-1)
	set := roaring.NewBitMatrix(ShardWidth, [][]uint64{
		{},
		{},
		{0, 1, ShardWidth - 1},
	}...)

	err := f.ImportRoaringSingleValued(context.Background(), qw, clear.Roaring(), set.Roaring())
	if err != nil {
		t.Fatalf("importing: %v", err)
	}

	// try setting stuff before
	set = roaring.NewBitMatrix(ShardWidth, [][]uint64{
		{},
		{0, 1, ShardWidth - 1},
	}...)
	if err := f.ImportRoaringSingleValued(context.Background(), qw, clear.Roaring(), set.Roaring()); err != nil {
		t.Fatalf("importing: %v", err)
	}

	// try setting stuff after
	set = roaring.NewBitMatrix(ShardWidth, [][]uint64{
		{},
		{},
		{},
		{0, 1, ShardWidth - 1},
	}...)

	if err := f.ImportRoaringSingleValued(context.Background(), qw, clear.Roaring(), set.Roaring()); err != nil {
		t.Fatalf("importing: %v", err)
	}

	result, err := qw.RoaringBitmap()
	if err != nil {
		t.Fatalf("getting bitmap: %v", err)
	}

	if !reflect.DeepEqual(result.Slice(), []uint64{ShardWidth * 3, ShardWidth*3 + 1, ShardWidth*4 - 1}) {
		t.Fatalf("unexpected result: %v", result.Slice())
	}

}
