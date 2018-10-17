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

package pilosa

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"math"
	"reflect"
	"sort"
	"testing"
	"testing/quick"

	"github.com/davecgh/go-spew/spew"
	"github.com/pilosa/pilosa/pql"
	"github.com/pilosa/pilosa/roaring"
)

// Test flags
var (
	// In order to generate the sample fragment file,
	// run an import and copy PILOSA_DATA_DIR/INDEX_NAME/FRAME_NAME/0 to testdata/sample_view
	FragmentPath = flag.String("fragment", "testdata/sample_view/0", "fragment path")
)

// Ensure a fragment can set a bit and retrieve it.
func TestFragment_SetBit(t *testing.T) {
	f := mustOpenFragment("i", "f", viewStandard, 0, "")
	defer f.Close()

	// Set bits on the fragment.
	if _, err := f.setBit(120, 1); err != nil {
		t.Fatal(err)
	} else if _, err := f.setBit(120, 6); err != nil {
		t.Fatal(err)
	} else if _, err := f.setBit(121, 0); err != nil {
		t.Fatal(err)
	}

	// Verify counts on rows.
	if n := f.row(120).Count(); n != 2 {
		t.Fatalf("unexpected count: %d", n)
	} else if n := f.row(121).Count(); n != 1 {
		t.Fatalf("unexpected count: %d", n)
	}

	// Close and reopen the fragment & verify the data.
	if err := f.reopen(); err != nil {
		t.Fatal(err)
	} else if n := f.row(120).Count(); n != 2 {
		t.Fatalf("unexpected count (reopen): %d", n)
	} else if n := f.row(121).Count(); n != 1 {
		t.Fatalf("unexpected count (reopen): %d", n)
	}
}

// Ensure a fragment can clear a set bit.
func TestFragment_ClearBit(t *testing.T) {
	f := mustOpenFragment("i", "f", viewStandard, 0, "")
	defer f.Close()

	// Set and then clear bits on the fragment.
	if _, err := f.setBit(1000, 1); err != nil {
		t.Fatal(err)
	} else if _, err := f.setBit(1000, 2); err != nil {
		t.Fatal(err)
	} else if _, err := f.clearBit(1000, 1); err != nil {
		t.Fatal(err)
	}

	// Verify count on row.
	if n := f.row(1000).Count(); n != 1 {
		t.Fatalf("unexpected count: %d", n)
	}

	// Close and reopen the fragment & verify the data.
	if err := f.reopen(); err != nil {
		t.Fatal(err)
	} else if n := f.row(1000).Count(); n != 1 {
		t.Fatalf("unexpected count (reopen): %d", n)
	}
}

// Ensure a fragment can clear a row.
func TestFragment_ClearRow(t *testing.T) {
	f := mustOpenFragment("i", "f", viewStandard, 0, "")
	defer f.Close()

	// Set and then clear bits on the fragment.
	if _, err := f.setBit(1000, 1); err != nil {
		t.Fatal(err)
	} else if _, err := f.setBit(1000, 65536); err != nil {
		t.Fatal(err)
	} else if _, err := f.unprotectedClearRow(1000); err != nil {
		t.Fatal(err)
	}

	// Verify count on row.
	if n := f.row(1000).Count(); n != 0 {
		t.Fatalf("unexpected count: %d", n)
	}

	// Close and reopen the fragment & verify the data.
	if err := f.reopen(); err != nil {
		t.Fatal(err)
	} else if n := f.row(1000).Count(); n != 0 {
		t.Fatalf("unexpected count (reopen): %d", n)
	}
}

// Ensure a fragment can set a row.
func TestFragment_SetRow(t *testing.T) {
	f := mustOpenFragment("i", "f", viewStandard, 7, "")
	defer f.Close()

	rowID := uint64(1000)

	// Set bits on the fragment.
	if _, err := f.setBit(rowID, 8000001); err != nil {
		t.Fatal(err)
	} else if _, err := f.setBit(rowID, 8065536); err != nil {
		t.Fatal(err)
	}

	// Verify data on row.
	if cols := f.row(rowID).Columns(); !reflect.DeepEqual(cols, []uint64{8000001, 8065536}) {
		t.Fatalf("unexpected columns: %+v", cols)
	}
	// Verify count on row.
	if n := f.row(rowID).Count(); n != 2 {
		t.Fatalf("unexpected count: %d", n)
	}

	// Set row (overwrite existing data).
	row := NewRow(8000002, 8065537, 8131074)
	if changed, err := f.unprotectedSetRow(row, rowID); err != nil {
		t.Fatal(err)
	} else if !changed {
		t.Fatalf("expected changed value: %v", changed)
	}

	// Verify data on row.
	if cols := f.row(rowID).Columns(); !reflect.DeepEqual(cols, []uint64{8000002, 8065537, 8131074}) {
		t.Fatalf("unexpected columns after set row: %+v", cols)
	}
	// Verify count on row.
	if n := f.row(rowID).Count(); n != 3 {
		t.Fatalf("unexpected count after set row: %d", n)
	}

	// Close and reopen the fragment & verify the data.
	if err := f.reopen(); err != nil {
		t.Fatal(err)
	} else if n := f.row(rowID).Count(); n != 3 {
		t.Fatalf("unexpected count (reopen): %d", n)
	}
}

// Ensure a fragment can set & read a value.
func TestFragment_SetValue(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		f := mustOpenFragment("i", "f", viewStandard, 0, "")
		defer f.Close()

		// Set value.
		if changed, err := f.setValue(100, 16, 3829); err != nil {
			t.Fatal(err)
		} else if !changed {
			t.Fatal("expected change")
		}

		// Read value.
		if value, exists, err := f.value(100, 16); err != nil {
			t.Fatal(err)
		} else if value != 3829 {
			t.Fatalf("unexpected value: %d", value)
		} else if !exists {
			t.Fatal("expected to exist")
		}

		// Setting value should return no change.
		if changed, err := f.setValue(100, 16, 3829); err != nil {
			t.Fatal(err)
		} else if changed {
			t.Fatal("expected no change")
		}
	})

	t.Run("Overwrite", func(t *testing.T) {
		f := mustOpenFragment("i", "f", viewStandard, 0, "")
		defer f.Close()

		// Set value.
		if changed, err := f.setValue(100, 16, 3829); err != nil {
			t.Fatal(err)
		} else if !changed {
			t.Fatal("expected change")
		}

		// Overwriting value should overwrite all bits.
		if changed, err := f.setValue(100, 16, 2028); err != nil {
			t.Fatal(err)
		} else if !changed {
			t.Fatal("expected change")
		}

		// Read value.
		if value, exists, err := f.value(100, 16); err != nil {
			t.Fatal(err)
		} else if value != 2028 {
			t.Fatalf("unexpected value: %d", value)
		} else if !exists {
			t.Fatal("expected to exist")
		}
	})

	t.Run("NotExists", func(t *testing.T) {
		f := mustOpenFragment("i", "f", viewStandard, 0, "")
		defer f.Close()

		// Set value.
		if changed, err := f.setValue(100, 10, 20); err != nil {
			t.Fatal(err)
		} else if !changed {
			t.Fatal("expected change")
		}

		// Non-existent value.
		if value, exists, err := f.value(100, 11); err != nil {
			t.Fatal(err)
		} else if value != 0 {
			t.Fatalf("unexpected value: %d", value)
		} else if exists {
			t.Fatal("expected to not exist")
		}
	})

	t.Run("QuickCheck", func(t *testing.T) {
		if err := quick.Check(func(bitDepth uint, bitN uint64, values []uint64) bool {
			// Limit bit depth & maximum values.
			bitDepth = (bitDepth % 62) + 1
			bitN = (bitN % 99) + 1

			for i := range values {
				values[i] = values[i] % (1 << bitDepth)
			}

			f := mustOpenFragment("i", "f", viewStandard, 0, "")
			defer f.Close()

			// Set values.
			m := make(map[uint64]int64)
			for _, value := range values {
				columnID := value % bitN

				m[columnID] = int64(value)

				if _, err := f.setValue(columnID, bitDepth, value); err != nil {
					t.Fatal(err)
				}
			}

			// Ensure values are set.
			for columnID, value := range m {
				v, exists, err := f.value(columnID, bitDepth)
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

	f := mustOpenFragment("i", "f", viewStandard, 0, "")
	defer f.Close()

	// Set values.
	if _, err := f.setValue(1000, bitDepth, 382); err != nil {
		t.Fatal(err)
	} else if _, err := f.setValue(2000, bitDepth, 300); err != nil {
		t.Fatal(err)
	} else if _, err := f.setValue(3000, bitDepth, 2818); err != nil {
		t.Fatal(err)
	} else if _, err := f.setValue(4000, bitDepth, 300); err != nil {
		t.Fatal(err)
	}

	t.Run("NoFilter", func(t *testing.T) {
		if sum, n, err := f.sum(nil, bitDepth); err != nil {
			t.Fatal(err)
		} else if n != 4 {
			t.Fatalf("unexpected count: %d", n)
		} else if sum != 3800 {
			t.Fatalf("unexpected sum: %d", sum)
		}
	})

	t.Run("WithFilter", func(t *testing.T) {
		if sum, n, err := f.sum(NewRow(2000, 4000, 5000), bitDepth); err != nil {
			t.Fatal(err)
		} else if n != 2 {
			t.Fatalf("unexpected count: %d", n)
		} else if sum != 600 {
			t.Fatalf("unexpected sum: %d", sum)
		}
	})
}

// Ensure a fragment can find the min and max of values.
func TestFragment_MinMax(t *testing.T) {
	const bitDepth = 16

	f := mustOpenFragment("i", "f", viewStandard, 0, "")
	defer f.Close()

	// Set values.
	if _, err := f.setValue(1000, bitDepth, 382); err != nil {
		t.Fatal(err)
	} else if _, err := f.setValue(2000, bitDepth, 300); err != nil {
		t.Fatal(err)
	} else if _, err := f.setValue(3000, bitDepth, 2818); err != nil {
		t.Fatal(err)
	} else if _, err := f.setValue(4000, bitDepth, 300); err != nil {
		t.Fatal(err)
	} else if _, err := f.setValue(5000, bitDepth, 2818); err != nil {
		t.Fatal(err)
	} else if _, err := f.setValue(6000, bitDepth, 2817); err != nil {
		t.Fatal(err)
	} else if _, err := f.setValue(7000, bitDepth, 0); err != nil {
		t.Fatal(err)
	}

	t.Run("Min", func(t *testing.T) {
		tests := []struct {
			filter *Row
			exp    uint64
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
			if min, cnt, err := f.min(test.filter, bitDepth); err != nil {
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
			exp    uint64
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
			if max, cnt, err := f.max(test.filter, bitDepth); err != nil {
				t.Fatal(err)
			} else if max != test.exp {
				t.Errorf("test %d expected max: %v, but got: %v", i, test.exp, max)
			} else if cnt != test.cnt {
				t.Errorf("test %d expected cnt: %v, but got: %v", i, test.cnt, cnt)
			}
		}
	})
}

// Ensure a fragment query for matching values.
func TestFragment_Range(t *testing.T) {
	const bitDepth = 16

	t.Run("EQ", func(t *testing.T) {
		f := mustOpenFragment("i", "f", viewStandard, 0, "")
		defer f.Close()

		// Set values.
		if _, err := f.setValue(1000, bitDepth, 382); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(2000, bitDepth, 300); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(3000, bitDepth, 2818); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(4000, bitDepth, 300); err != nil {
			t.Fatal(err)
		}

		// Query for equality.
		if b, err := f.rangeOp(pql.EQ, bitDepth, 300); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(b.Columns(), []uint64{2000, 4000}) {
			t.Fatalf("unexpected columns: %+v", b.Columns())
		}
	})

	t.Run("NEQ", func(t *testing.T) {
		f := mustOpenFragment("i", "f", viewStandard, 0, "")
		defer f.Close()

		// Set values.
		if _, err := f.setValue(1000, bitDepth, 382); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(2000, bitDepth, 300); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(3000, bitDepth, 2818); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(4000, bitDepth, 300); err != nil {
			t.Fatal(err)
		}

		// Query for inequality.
		if b, err := f.rangeOp(pql.NEQ, bitDepth, 300); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(b.Columns(), []uint64{1000, 3000}) {
			t.Fatalf("unexpected columns: %+v", b.Columns())
		}
	})

	t.Run("LT", func(t *testing.T) {
		f := mustOpenFragment("i", "f", viewStandard, 0, "")
		defer f.Close()

		// Set values.
		if _, err := f.setValue(1000, bitDepth, 382); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(2000, bitDepth, 300); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(3000, bitDepth, 2817); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(4000, bitDepth, 301); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(5000, bitDepth, 1); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(6000, bitDepth, 0); err != nil {
			t.Fatal(err)
		}

		// Query for values less than (ending with set column).
		if b, err := f.rangeOp(pql.LT, bitDepth, 301); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(b.Columns(), []uint64{2000, 5000, 6000}) {
			t.Fatalf("unexpected columns: %+v", b.Columns())
		}

		// Query for values less than (ending with unset column).
		if b, err := f.rangeOp(pql.LT, bitDepth, 300); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(b.Columns(), []uint64{5000, 6000}) {
			t.Fatalf("unexpected columns: %+v", b.Columns())
		}

		// Query for values less than or equal to (ending with set column).
		if b, err := f.rangeOp(pql.LTE, bitDepth, 301); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(b.Columns(), []uint64{2000, 4000, 5000, 6000}) {
			t.Fatalf("unexpected columns: %+v", b.Columns())
		}

		// Query for values less than or equal to (ending with unset column).
		if b, err := f.rangeOp(pql.LTE, bitDepth, 300); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(b.Columns(), []uint64{2000, 5000, 6000}) {
			t.Fatalf("unexpected columns: %+v", b.Columns())
		}
	})

	t.Run("GT", func(t *testing.T) {
		f := mustOpenFragment("i", "f", viewStandard, 0, "")
		defer f.Close()

		// Set values.
		if _, err := f.setValue(1000, bitDepth, 382); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(2000, bitDepth, 300); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(3000, bitDepth, 2817); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(4000, bitDepth, 301); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(5000, bitDepth, 1); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(6000, bitDepth, 0); err != nil {
			t.Fatal(err)
		}

		// Query for values greater than (ending with unset bit).
		if b, err := f.rangeOp(pql.GT, bitDepth, 300); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(b.Columns(), []uint64{1000, 3000, 4000}) {
			t.Fatalf("unexpected columns: %+v", b.Columns())
		}

		// Query for values greater than (ending with set bit).
		if b, err := f.rangeOp(pql.GT, bitDepth, 301); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(b.Columns(), []uint64{1000, 3000}) {
			t.Fatalf("unexpected columns: %+v", b.Columns())
		}

		// Query for values greater than or equal to (ending with unset bit).
		if b, err := f.rangeOp(pql.GTE, bitDepth, 300); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(b.Columns(), []uint64{1000, 2000, 3000, 4000}) {
			t.Fatalf("unexpected columns: %+v", b.Columns())
		}

		// Query for values greater than or equal to (ending with set bit).
		if b, err := f.rangeOp(pql.GTE, bitDepth, 301); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(b.Columns(), []uint64{1000, 3000, 4000}) {
			t.Fatalf("unexpected columns: %+v", b.Columns())
		}
	})

	t.Run("BETWEEN", func(t *testing.T) {
		f := mustOpenFragment("i", "f", viewStandard, 0, "")
		defer f.Close()

		// Set values.
		if _, err := f.setValue(1000, bitDepth, 382); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(2000, bitDepth, 300); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(3000, bitDepth, 2817); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(4000, bitDepth, 301); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(5000, bitDepth, 1); err != nil {
			t.Fatal(err)
		} else if _, err := f.setValue(6000, bitDepth, 0); err != nil {
			t.Fatal(err)
		}

		// Query for values greater than (ending with unset column).
		if b, err := f.rangeBetween(bitDepth, 300, 2817); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(b.Columns(), []uint64{1000, 2000, 3000, 4000}) {
			t.Fatalf("unexpected columns: %+v", b.Columns())
		}

		// Query for values greater than (ending with set column).
		if b, err := f.rangeBetween(bitDepth, 301, 2817); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(b.Columns(), []uint64{1000, 3000, 4000}) {
			t.Fatalf("unexpected columns: %+v", b.Columns())
		}

		// Query for values greater than or equal to (ending with unset column).
		if b, err := f.rangeBetween(bitDepth, 301, 2816); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(b.Columns(), []uint64{1000, 4000}) {
			t.Fatalf("unexpected columns: %+v", b.Columns())
		}

		// Query for values greater than or equal to (ending with set column).
		if b, err := f.rangeBetween(bitDepth, 300, 2816); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(b.Columns(), []uint64{1000, 2000, 4000}) {
			t.Fatalf("unexpected columns: %+v", b.Columns())
		}
	})
}

// Ensure a fragment can snapshot correctly.
func TestFragment_Snapshot(t *testing.T) {
	f := mustOpenFragment("i", "f", viewStandard, 0, "")
	defer f.Close()

	// Set and then clear bits on the fragment.
	if _, err := f.setBit(1000, 1); err != nil {
		t.Fatal(err)
	} else if _, err := f.setBit(1000, 2); err != nil {
		t.Fatal(err)
	} else if _, err := f.clearBit(1000, 1); err != nil {
		t.Fatal(err)
	}

	// Snapshot bitmap and verify data.
	if err := f.Snapshot(); err != nil {
		t.Fatal(err)
	} else if n := f.row(1000).Count(); n != 1 {
		t.Fatalf("unexpected count: %d", n)
	}

	// Close and reopen the fragment & verify the data.
	if err := f.reopen(); err != nil {
		t.Fatal(err)
	} else if n := f.row(1000).Count(); n != 1 {
		t.Fatalf("unexpected count (reopen): %d", n)
	}
}

// Ensure a fragment can iterate over all bits in order.
func TestFragment_ForEachBit(t *testing.T) {
	f := mustOpenFragment("i", "f", viewStandard, 0, "")
	defer f.Close()

	// Set bits on the fragment.
	if _, err := f.setBit(100, 20); err != nil {
		t.Fatal(err)
	} else if _, err := f.setBit(2, 38); err != nil {
		t.Fatal(err)
	} else if _, err := f.setBit(2, 37); err != nil {
		t.Fatal(err)
	}

	// Iterate over bits.
	var result [][2]uint64
	if err := f.forEachBit(func(rowID, columnID uint64) error {
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
	f := mustOpenFragment("i", "f", viewStandard, 0, CacheTypeRanked)
	defer f.Close()
	// Set bits on the rows 100, 101, & 102.
	f.mustSetBits(100, 1, 3, 200)
	f.mustSetBits(101, 1)
	f.mustSetBits(102, 1, 2)
	f.RecalculateCache()

	// Retrieve top rows.
	if pairs, err := f.top(topOptions{N: 2}); err != nil {
		t.Fatal(err)
	} else if len(pairs) != 2 {
		t.Fatalf("unexpected count: %d", len(pairs))
	} else if pairs[0] != (Pair{ID: 100, Count: 3}) {
		t.Fatalf("unexpected pair(0): %v", pairs[0])
	} else if pairs[1] != (Pair{ID: 102, Count: 2}) {
		t.Fatalf("unexpected pair(1): %v", pairs[1])
	}
}

// Ensure a fragment can filter rows when retrieving the top n rows.
func TestFragment_Top_Filter(t *testing.T) {
	f := mustOpenFragment("i", "f", viewStandard, 0, CacheTypeRanked)
	defer f.Close()

	// Set bits on the rows 100, 101, & 102.
	f.mustSetBits(100, 1, 3, 200)
	f.mustSetBits(101, 1)
	f.mustSetBits(102, 1, 2)
	f.RecalculateCache()
	// Assign attributes.
	f.RowAttrStore.SetAttrs(101, map[string]interface{}{"x": int64(10)})
	f.RowAttrStore.SetAttrs(102, map[string]interface{}{"x": int64(20)})

	// Retrieve top rows.
	if pairs, err := f.top(topOptions{
		N:            2,
		FilterName:   "x",
		FilterValues: []interface{}{int64(10), int64(15), int64(20)},
	}); err != nil {
		t.Fatal(err)
	} else if len(pairs) != 2 {
		t.Fatalf("unexpected count: %d", len(pairs))
	} else if pairs[0] != (Pair{ID: 102, Count: 2}) {
		t.Fatalf("unexpected pair(0): %v", pairs[0])
	} else if pairs[1] != (Pair{ID: 101, Count: 1}) {
		t.Fatalf("unexpected pair(1): %v", pairs[1])
	}
}

// Ensure a fragment can return top rows that intersect with an input row.
func TestFragment_TopN_Intersect(t *testing.T) {
	f := mustOpenFragment("i", "f", viewStandard, 0, CacheTypeRanked)
	defer f.Close()

	// Create an intersecting input row.
	src := NewRow(1, 2, 3)

	// Set bits on various rows.
	f.mustSetBits(100, 1, 10, 11, 12)    // one intersection
	f.mustSetBits(101, 1, 2, 3, 4)       // three intersections
	f.mustSetBits(102, 1, 2, 4, 5, 6)    // two intersections
	f.mustSetBits(103, 1000, 1001, 1002) // no intersection
	f.RecalculateCache()

	// Retrieve top rows.
	if pairs, err := f.top(topOptions{N: 3, Src: src}); err != nil {
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

	f := mustOpenFragment("i", "f", viewStandard, 0, CacheTypeRanked)
	defer f.Close()

	// Create an intersecting input row.
	src := NewRow(
		980, 981, 982, 983, 984, 985, 986, 987, 988, 989,
		990, 991, 992, 993, 994, 995, 996, 997, 998, 999,
	)

	// Set bits on rows 0 - 999. Higher rows have higher bit counts.
	for i := uint64(0); i < 1000; i++ {
		for j := uint64(0); j < i; j++ {
			f.mustSetBits(i, j)
		}
	}
	f.RecalculateCache()

	// Retrieve top rows.
	if pairs, err := f.top(topOptions{N: 10, Src: src}); err != nil {
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
	f := mustOpenFragment("i", "f", viewStandard, 0, CacheTypeRanked)
	defer f.Close()

	// Set bits on various rows.
	f.mustSetBits(100, 1, 2, 3)
	f.mustSetBits(101, 4, 5, 6, 7)
	f.mustSetBits(102, 8, 9, 10, 11, 12)

	// Retrieve top rows.
	if pairs, err := f.top(topOptions{RowIDs: []uint64{100, 101, 200}}); err != nil {
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
	f := mustOpenFragment("i", "f", viewStandard, 0, CacheTypeNone)
	defer f.Close()

	// Set bits on various rows.
	f.mustSetBits(100, 1, 2, 3)
	f.mustSetBits(101, 4, 5, 6, 7)
	f.mustSetBits(102, 8, 9, 10, 11, 12)

	// Retrieve top rows.
	if pairs, err := f.top(topOptions{RowIDs: []uint64{100, 101, 200}}); err != nil {
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
	index := mustOpenIndex(IndexOptions{})
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
		panic(err)
	}
	defer f.Close()

	// Set bits on various rows.
	f.mustSetBits(100, 1, 2, 3)
	f.mustSetBits(101, 4, 5, 6, 7)
	f.mustSetBits(102, 8, 9, 10, 11, 12)
	f.mustSetBits(103, 8, 9, 10, 11, 12, 13)
	f.mustSetBits(104, 8, 9, 10, 11, 12, 13, 14)
	f.mustSetBits(105, 10, 11)

	f.RecalculateCache()

	p := []Pair{
		{ID: 104, Count: 7},
		{ID: 103, Count: 6},
		{ID: 102, Count: 5},
	}

	// Retrieve top rows.
	if pairs, err := f.top(topOptions{N: 5}); err != nil {
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
	f := mustOpenFragment("i", "f", viewStandard, 0, "")
	defer f.Close()

	// Retrieve checksum and set bits.
	orig := f.Checksum()
	if _, err := f.setBit(1, 200); err != nil {
		t.Fatal(err)
	} else if _, err := f.setBit(HashBlockSize*2, 200); err != nil {
		t.Fatal(err)
	}

	// Ensure new checksum is different.
	if chksum := f.Checksum(); bytes.Equal(chksum, orig) {
		t.Fatalf("expected checksum to change: %x - %x", chksum, orig)
	}
}

// Ensure fragment can return a checksum for a given block.
func TestFragment_Blocks(t *testing.T) {
	f := mustOpenFragment("i", "f", viewStandard, 0, "")
	defer f.Close()

	// Retrieve initial checksum.
	var prev []FragmentBlock

	// Set first bit.
	if _, err := f.setBit(0, 0); err != nil {
		t.Fatal(err)
	}
	blocks := f.Blocks()
	if blocks[0].Checksum == nil {
		t.Fatalf("expected checksum: %x", blocks[0].Checksum)
	}
	prev = blocks

	// Set bit on different row.
	if _, err := f.setBit(20, 0); err != nil {
		t.Fatal(err)
	}
	blocks = f.Blocks()
	if bytes.Equal(blocks[0].Checksum, prev[0].Checksum) {
		t.Fatalf("expected checksum to change: %x", blocks[0].Checksum)
	}
	prev = blocks

	// Set bit on different column.
	if _, err := f.setBit(20, 100); err != nil {
		t.Fatal(err)
	}
	blocks = f.Blocks()
	if bytes.Equal(blocks[0].Checksum, prev[0].Checksum) {
		t.Fatalf("expected checksum to change: %x", blocks[0].Checksum)
	}
}

// Ensure fragment returns an empty checksum if no data exists for a block.
func TestFragment_Blocks_Empty(t *testing.T) {
	f := mustOpenFragment("i", "f", viewStandard, 0, "")
	defer f.Close()

	// Set bits on a different block.
	if _, err := f.setBit(100, 1); err != nil {
		t.Fatal(err)
	}

	// Ensure checksum for block 1 is blank.
	if blocks := f.Blocks(); len(blocks) != 1 {
		t.Fatalf("unexpected block count: %d", len(blocks))
	} else if blocks[0].ID != 1 {
		t.Fatalf("unexpected block id: %d", blocks[0].ID)
	}
}

// Ensure a fragment's cache can be persisted between restarts.
func TestFragment_LRUCache_Persistence(t *testing.T) {
	f := mustOpenFragment("i", "f", viewStandard, 0, CacheTypeLRU)
	defer f.Close()

	// Set bits on the fragment.
	for i := uint64(0); i < 1000; i++ {
		if _, err := f.setBit(i, 0); err != nil {
			t.Fatal(err)
		}
	}

	// Verify correct cache type and size.
	if cache, ok := f.cache.(*lruCache); !ok {
		t.Fatalf("unexpected cache: %T", f.cache)
	} else if cache.Len() != 1000 {
		t.Fatalf("unexpected cache len: %d", cache.Len())
	}

	// Reopen the fragment.
	if err := f.reopen(); err != nil {
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
	index := mustOpenIndex(IndexOptions{})
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

	// Set bits on the fragment.
	for i := uint64(0); i < 1000; i++ {
		if _, err := f.setBit(i, 0); err != nil {
			t.Fatal(err)
		}
	}

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

// Ensure a fragment can be copied to another fragment.
func TestFragment_WriteTo_ReadFrom(t *testing.T) {
	f0 := mustOpenFragment("i", "f", viewStandard, 0, "")
	defer f0.Close()

	// Set and then clear bits on the fragment.
	if _, err := f0.setBit(1000, 1); err != nil {
		t.Fatal(err)
	} else if _, err := f0.setBit(1000, 2); err != nil {
		t.Fatal(err)
	} else if _, err := f0.clearBit(1000, 1); err != nil {
		t.Fatal(err)
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
	f1 := mustOpenFragment("i", "f", viewStandard, 0, "")
	if rn, err := f1.ReadFrom(&buf); err != nil {
		t.Fatal(err)
	} else if wn != rn {
		t.Fatalf("read/write byte count mismatch: wn=%d, rn=%d", wn, rn)
	}

	// Verify cache is in other fragment.
	if n := f1.cache.Len(); n != 1 {
		t.Fatalf("unexpected cache size: %d", n)
	}

	// Verify data in other fragment.
	if a := f1.row(1000).Columns(); !reflect.DeepEqual(a, []uint64{2}) {
		t.Fatalf("unexpected columns: %+v", a)
	}

	// Close and reopen the fragment & verify the data.
	if err := f1.reopen(); err != nil {
		t.Fatal(err)
	} else if n := f1.cache.Len(); n != 1 {
		t.Fatalf("unexpected cache size (reopen): %d", n)
	} else if a := f1.row(1000).Columns(); !reflect.DeepEqual(a, []uint64{2}) {
		t.Fatalf("unexpected columns (reopen): %+v", a)
	}
}

func BenchmarkFragment_Blocks(b *testing.B) {
	if *FragmentPath == "" {
		b.Skip("no fragment specified")
	}

	// Open the fragment specified by the path.
	f := newFragment(*FragmentPath, "i", "f", viewStandard, 0)
	if err := f.Open(); err != nil {
		b.Fatal(err)
	}
	defer f.Close()

	// Reset timer and execute benchmark.
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if a := f.Blocks(); len(a) == 0 {
			b.Fatal("no blocks in fragment")
		}
	}
}

func BenchmarkFragment_IntersectionCount(b *testing.B) {
	f := mustOpenFragment("i", "f", viewStandard, 0, "")
	defer f.Close()
	f.MaxOpN = math.MaxInt32

	// Generate some intersecting data.
	for i := 0; i < 10000; i += 2 {
		if _, err := f.setBit(1, uint64(i)); err != nil {
			b.Fatal(err)
		}
	}
	for i := 0; i < 10000; i += 3 {
		if _, err := f.setBit(2, uint64(i)); err != nil {
			b.Fatal(err)
		}
	}

	// Snapshot to disk before benchmarking.
	if err := f.Snapshot(); err != nil {
		b.Fatal(err)
	}

	// Start benchmark
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if n := f.row(1).intersectionCount(f.row(2)); n == 0 {
			b.Fatalf("unexpected count: %d", n)
		}
	}
}

func TestFragment_Tanimoto(t *testing.T) {
	f := mustOpenFragment("i", "f", viewStandard, 0, CacheTypeRanked)
	defer f.Close()

	src := NewRow(1, 2, 3)

	// Set bits on the rows 100, 101, & 102.
	f.mustSetBits(100, 1, 3, 2, 200)
	f.mustSetBits(101, 1, 3)
	f.mustSetBits(102, 1, 2, 10, 12)
	f.RecalculateCache()

	if pairs, err := f.top(topOptions{TanimotoThreshold: 50, Src: src}); err != nil {
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
	f := mustOpenFragment("i", "f", viewStandard, 0, CacheTypeRanked)
	defer f.Close()

	src := NewRow(1, 2, 3)

	// Set bits on the rows 100, 101, & 102.
	f.mustSetBits(100, 1, 3, 2, 200)
	f.mustSetBits(101, 1, 3)
	f.mustSetBits(102, 1, 2, 10, 12)
	f.RecalculateCache()

	if pairs, err := f.top(topOptions{TanimotoThreshold: 0, Src: src}); err != nil {
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
	f := mustOpenFragment("i", "f", viewStandard, 0, "")
	defer f.Close()

	// Set bits on the fragment.
	for i := uint64(1); i < 3; i++ {
		if _, err := f.setBit(1000, i); err != nil {
			t.Fatal(err)
		}
	}

	// Snapshot bitmap and verify data.
	if err := f.Snapshot(); err != nil {
		t.Fatal(err)
	} else if n := f.row(1000).Count(); n != 2 {
		t.Fatalf("unexpected count: %d", n)
	}

	// Close and reopen the fragment & verify the data.
	if err := f.reopen(); err != nil {
		t.Fatal(err)
	} else if n := f.row(1000).Count(); n != 2 {
		t.Fatalf("unexpected count (reopen): %d", n)
	}
}

// Ensure a fragment can set mutually exclusive values.
func TestFragment_SetMutex(t *testing.T) {
	f := mustOpenMutexFragment("i", "f", viewStandard, 0, "")
	defer f.Close()

	var cols []uint64

	// Set a value on column 100.
	if _, err := f.setBit(1, 100); err != nil {
		t.Fatal(err)
	}
	// Verify the value was set.
	cols = f.row(1).Columns()
	if !reflect.DeepEqual(cols, []uint64{100}) {
		t.Fatalf("mutex unexpected columns: %v", cols)
	}

	// Set a different value on column 100.
	if _, err := f.setBit(2, 100); err != nil {
		t.Fatal(err)
	}
	// Verify that value (row 1) was replaced (by row 2).
	cols = f.row(1).Columns()
	if !reflect.DeepEqual(cols, []uint64{}) {
		t.Fatalf("mutex unexpected columns: %v", cols)
	}
	cols = f.row(2).Columns()
	if !reflect.DeepEqual(cols, []uint64{100}) {
		t.Fatalf("mutex unexpected columns: %v", cols)
	}
}

// Ensure a fragment can import mutually exclusive values.
func TestFragment_ImportMutex(t *testing.T) {
	tests := []struct {
		rowIDs []uint64
		colIDs []uint64
		exp    map[uint64][]uint64
	}{
		{
			[]uint64{1, 1, 1, 1},
			[]uint64{0, 1, 2, 3},
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
		},
		{
			[]uint64{1, 1, 1, 1, 2},
			[]uint64{0, 1, 2, 3, 1},
			map[uint64][]uint64{
				1: {0, 2, 3},
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
		},
		{
			[]uint64{1, 2, 3},
			[]uint64{8, 8, 8},
			map[uint64][]uint64{
				1: {},
				2: {},
				3: {8},
			},
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("importmutex%d", i), func(t *testing.T) {
			f := mustOpenMutexFragment("i", "f", viewStandard, 0, "")
			defer f.Close()

			err := f.bulkImport(test.rowIDs, test.colIDs)
			if err != nil {
				t.Fatalf("bulk importing ids: %v", err)
			}

			// Check for expected results.
			for k, v := range test.exp {
				cols := f.row(k).Columns()
				if !reflect.DeepEqual(cols, v) {
					t.Fatalf("expected: %v, but got: %v", v, cols)
				}
			}
		})
	}
}

// Ensure a fragment can import bool values.
func TestFragment_ImportBool(t *testing.T) {
	tests := []struct {
		rowIDs []uint64
		colIDs []uint64
		exp    map[uint64][]uint64
	}{
		{
			[]uint64{1, 1, 1, 1},
			[]uint64{0, 1, 2, 3},
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
		},
		{
			[]uint64{0, 0, 0, 0, 1},
			[]uint64{0, 1, 2, 3, 1},
			map[uint64][]uint64{
				0: {0, 2, 3},
				1: {1},
			},
		},
		{
			[]uint64{1, 1, 1, 1, 0, 0, 1},
			[]uint64{0, 1, 2, 3, 1, 8, 1},
			map[uint64][]uint64{
				0: {8},
				1: {0, 1, 2, 3},
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
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("importmutex%d", i), func(t *testing.T) {
			f := mustOpenBoolFragment("i", "f", viewStandard, 0, "")
			defer f.Close()

			err := f.bulkImport(test.rowIDs, test.colIDs)
			if err != nil {
				t.Fatalf("bulk importing ids: %v", err)
			}

			// Check for expected results.
			for k, v := range test.exp {
				cols := f.row(k).Columns()
				if !reflect.DeepEqual(cols, v) {
					t.Fatalf("expected: %v, but got: %v", v, cols)
				}
			}
		})
	}
}

func BenchmarkFragment_Snapshot(b *testing.B) {
	if *FragmentPath == "" {
		b.Skip("no fragment specified")
	}

	b.ReportAllocs()
	// Open the fragment specified by the path.
	f := newFragment(*FragmentPath, "i", "f", viewStandard, 0)
	if err := f.Open(); err != nil {
		b.Fatal(err)
	}
	defer f.Close()
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
	f := mustOpenFragment("i", "f", viewStandard, 0, "")
	defer f.Close()
	// Generate some intersecting data.
	maxX := 1048576 / 2
	sz := maxX
	rows := make([]uint64, sz)
	cols := make([]uint64, sz)

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
		if err := f.bulkImport(rows, cols); err != nil {
			b.Fatalf("Error Building Sample: %s", err)
		}
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
	f := mustOpenFragment("i", "f", viewStandard, 0, "")
	defer f.Close()
	maxX := 1048576 * 5 * 2
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
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := f.bulkImport(rows, cols); err != nil {
			b.Fatalf("Error Building Sample: %s", err)
		}
	}
}

/////////////////////////////////////////////////////////////////////

// mustOpenFragment returns a new instance of Fragment with a temporary path.
func mustOpenFragment(index, field, view string, shard uint64, cacheType string) *fragment {
	file, err := ioutil.TempFile("", "pilosa-fragment-")
	if err != nil {
		panic(err)
	}
	file.Close()

	if cacheType == "" {
		cacheType = DefaultCacheType
	}

	f := newFragment(file.Name(), index, field, view, shard)
	f.CacheType = cacheType
	f.RowAttrStore = &memAttrStore{
		store: make(map[uint64]map[string]interface{}),
	}

	if err := f.Open(); err != nil {
		panic(err)
	}
	return f
}

// mustOpenMutexFragment returns a new instance of Fragment for a mutex field.
func mustOpenMutexFragment(index, field, view string, shard uint64, cacheType string) *fragment {
	frag := mustOpenFragment(index, field, view, shard, cacheType)
	frag.mutexVector = newRowsVector(frag)
	return frag
}

// mustOpenBoolFragment returns a new instance of Fragment for a bool field.
func mustOpenBoolFragment(index, field, view string, shard uint64, cacheType string) *fragment {
	frag := mustOpenFragment(index, field, view, shard, cacheType)
	frag.mutexVector = newBoolVector(frag)
	return frag
}

// Reopen closes the fragment and reopens it as a new instance.
func (f *fragment) reopen() error {
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
func (f *fragment) mustSetBits(rowID uint64, columnIDs ...uint64) {
	for _, columnID := range columnIDs {
		if _, err := f.setBit(rowID, columnID); err != nil {
			panic(err)
		}
	}
}

// Test Various methods of retrieving RowIDs
func TestFragment_RowsIteration(t *testing.T) {
	t.Run("firstContainer", func(t *testing.T) {
		f := mustOpenFragment("i", "f", viewStandard, 0, "")
		defer f.Close()

		expectedAll := make([]uint64, 0)
		expectedOdd := make([]uint64, 0)
		for i := uint64(100); i < uint64(200); i++ {
			if _, err := f.setBit(i, i%2); err != nil {
				t.Fatal(err)
			}
			expectedAll = append(expectedAll, i)
			if i%2 == 1 {
				expectedOdd = append(expectedOdd, i)
			}
		}

		ids := f.rows()
		if !reflect.DeepEqual(expectedAll, ids) {
			t.Fatalf("Do not match %v %v", expectedAll, ids)
		}

		ids = f.rowsForColumn(1)
		if !reflect.DeepEqual(expectedOdd, ids) {
			t.Fatalf("Do not match %v %v", expectedOdd, ids)
		}
	})

	t.Run("secondRow", func(t *testing.T) {
		f := mustOpenFragment("i", "f", viewStandard, 0, "")
		defer f.Close()

		expected := []uint64{1, 2}
		if _, err := f.setBit(1, 66000); err != nil {
			t.Fatal(err)
		} else if _, err := f.setBit(2, 66000); err != nil {
			t.Fatal(err)
		} else if _, err := f.setBit(2, 166000); err != nil {
			t.Fatal(err)
		}

		ids := f.rows()
		if !reflect.DeepEqual(expected, ids) {
			t.Fatalf("Do not match %v %v", expected, ids)
		}

		ids = f.rowsForColumn(66000)
		if !reflect.DeepEqual(expected, ids) {
			t.Fatalf("Do not match %v %v", expected, ids)
		}
	})

	t.Run("combinations", func(t *testing.T) {
		f := mustOpenFragment("i", "f", viewStandard, 0, "")
		defer f.Close()

		expectedRows := make([]uint64, 0)
		for r := uint64(1); r < uint64(10000); r += 100 {
			expectedRows = append(expectedRows, r)
			for c := uint64(1); c < uint64(ShardWidth-1); c += 10000 {
				if _, err := f.setBit(r, c); err != nil {
					t.Fatal(err)
				}

				ids := f.rows()
				if !reflect.DeepEqual(expectedRows, ids) {
					t.Fatalf("Do not match %v %v", expectedRows, ids)
				}
				ids = f.rowsForColumn(c)
				if !reflect.DeepEqual(expectedRows, ids) {
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
			f := mustOpenFragment("i", "f", viewStandard, 0, "")
			defer f.Close()
			for num, input := range test {
				buf := &bytes.Buffer{}
				bm := roaring.NewBitmap(input...)
				_, err := bm.WriteTo(buf)
				if err != nil {
					t.Fatalf("writing to buffer: %v", err)
				}
				f.importRoaring(buf.Bytes())
				exp := calcExpected(test[:num+1]...)
				for row, expCols := range exp {
					cols := f.row(uint64(row)).Columns()
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
			f := mustOpenFragment("i", "f", viewStandard, 0, CacheTypeRanked)
			defer f.Close()

			err := f.bulkImport(test.rowIDs, test.colIDs)
			if err != nil {
				t.Fatalf("bulk importing ids: %v", err)
			}
			expPairs := calcTop(test.rowIDs, test.colIDs)
			pairs, err := f.top(topOptions{})
			if err != nil {
				t.Fatalf("executing top after bulk import: %v", err)
			}
			if !reflect.DeepEqual(expPairs, pairs) {
				t.Fatalf("post bulk import:\n  exp: %v\n  got: %v\n", expPairs, pairs)
			}

			err = f.bulkImport(test.rowIDs2, test.colIDs2)
			if err != nil {
				t.Fatalf("bulk importing ids: %v", err)
			}
			test.rowIDs = append(test.rowIDs, test.rowIDs2...)
			test.colIDs = append(test.colIDs, test.colIDs2...)
			expPairs = calcTop(test.rowIDs, test.colIDs)
			pairs, err = f.top(topOptions{})
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
			f.importRoaring(buf.Bytes())
			rows, cols := toRowsCols(test.roaring)
			expPairs = calcTop(append(test.rowIDs, rows...), append(test.colIDs, cols...))
			pairs, err = f.top(topOptions{})
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
		panic("row and col ids must be of equal len")
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
	var maxrow uint64 = 0
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
