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
	"math/bits"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/pilosa/pilosa/v2/rbf"
	"github.com/pilosa/pilosa/v2/roaring"
)

func TestCursor_FirstNext(t *testing.T) {
	db := MustOpenDB(t)
	defer MustCloseDB(t, db)
	tx := MustBegin(t, db, true)
	defer MustRollback(t, tx)

	if err := tx.CreateBitmap("x"); err != nil {
		t.Fatal(err)
	} else if _, err := tx.Add("x", 0x00000001, 0x00000002, 0x00010003, 0x00030004); err != nil {
		t.Fatal(err)
	}

	c, err := tx.Cursor("x")
	if err != nil {
		t.Fatal(err)
	} else if err := c.First(); err != nil {
		t.Fatal(err)
	}

	if err := c.Next(); err != nil {
		t.Fatal(err)
	} else if got, want := c.Key(), uint64(0); got != want {
		t.Fatalf("Next()=%d, want %d", got, want)
	} else if got, want := c.Values(), []uint16{1, 2}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Values()=%#v, want %#v", got, want)
	}

	if err := c.Next(); err != nil {
		t.Fatal(err)
	} else if got, want := c.Key(), uint64(1); got != want {
		t.Fatalf("Next()=%d, want %d", got, want)
	} else if got, want := c.Values(), []uint16{3}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Values()=%#v, want %#v", got, want)
	}

	if err := c.Next(); err != nil {
		t.Fatal(err)
	} else if got, want := c.Key(), uint64(3); got != want {
		t.Fatalf("Next()=%d, want %d", got, want)
	} else if got, want := c.Values(), []uint16{4}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Values()=%#v, want %#v", got, want)
	}
}

func TestCursor_FirstNext_Quick(t *testing.T) {
	if testing.Short() {
		t.Skip("-short enabled, skipping")
	} else if is32Bit() {
		t.Skip("32-bit build, skipping quick check tests")
	} else if rbf.RaceEnabled {
		t.Skip("race detection enabled, skipping")
	}

	const n = 100000

	QuickCheck(t, func(t *testing.T, rand *rand.Rand) {
		t.Parallel()

		// Generate sorted list of values.
		values := make([]uint64, rand.Intn(n))
		for i := range values {
			values[i] = uint64(rand.Intn(rbf.ShardWidth))
		}
		sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })

		db := MustOpenDB(t)
		defer MustCloseDB(t, db)
		tx := MustBegin(t, db, true)
		defer MustRollback(t, tx)

		// Insert values in random order.
		if err := tx.CreateBitmap("x"); err != nil {
			t.Fatal(err)
		}
		for _, i := range rand.Perm(len(values)) {
			v := values[i]
			if _, err := tx.Add("x", v); err != nil {
				t.Fatalf("Add(%d) i=%d err=%q", v, i, err)
			}
		}

		// Generate unique bucketed values.
		type Item struct {
			key    uint64
			values []uint16
		}
		var items []Item
		m := make(map[uint64]struct{})
		for _, v := range values {
			if _, ok := m[v]; ok {
				continue
			}
			m[v] = struct{}{}

			hi, lo := highbits(v), lowbits(v)
			if len(items) == 0 || items[len(items)-1].key != hi {
				items = append(items, Item{key: hi})
			}

			item := &items[len(items)-1]
			item.values = append(item.values, lo)
		}

		// Verify cursor returns correct value groups.
		c, err := tx.Cursor("x")
		if err != nil {
			t.Fatal(err)
		} else if err := c.First(); err != nil {
			t.Fatal(err)
		}
		for _, item := range items {
			if err := c.Next(); err != nil {
				t.Fatal(err)
			} else if got, want := c.Key(), item.key; got != want {
				t.Fatalf("Key()=%d, want %d", got, want)
			} else if got, want := c.Values(), item.values; !reflect.DeepEqual(got, want) {
				t.Fatalf("len(Values())=%v, want %v", len(got), len(want))
			}
		}
	})
}

func TestCursor_LastPrev(t *testing.T) {
	db := MustOpenDB(t)
	defer MustCloseDB(t, db)
	tx := MustBegin(t, db, true)
	defer MustRollback(t, tx)

	if err := tx.CreateBitmap("x"); err != nil {
		t.Fatal(err)
	} else if _, err := tx.Add("x", 0x00000001, 0x00000002, 0x00010003, 0x00030004); err != nil {
		t.Fatal(err)
	}

	c, err := tx.Cursor("x")
	if err != nil {
		t.Fatal(err)
	} else if err := c.Last(); err != nil {
		t.Fatal(err)
	}

	if err := c.Prev(); err != nil {
		t.Fatal(err)
	} else if got, want := c.Key(), uint64(3); got != want {
		t.Fatalf("Prev()=%d, want %d", got, want)
	} else if got, want := c.Values(), []uint16{4}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Values()=%#v, want %#v", got, want)
	}

	if err := c.Prev(); err != nil {
		t.Fatal(err)
	} else if got, want := c.Key(), uint64(1); got != want {
		t.Fatalf("Prev()=%d, want %d", got, want)
	} else if got, want := c.Values(), []uint16{3}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Values()=%#v, want %#v", got, want)
	}

	if err := c.Prev(); err != nil {
		t.Fatal(err)
	} else if got, want := c.Key(), uint64(0); got != want {
		t.Fatalf("Prev()=%d, want %d", got, want)
	} else if got, want := c.Values(), []uint16{1, 2}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Values()=%#v, want %#v", got, want)
	}
}

func TestCursor_LastPrev_Quick(t *testing.T) {
	if testing.Short() {
		t.Skip("-short enabled, skipping")
	} else if is32Bit() {
		t.Skip("32-bit build, skipping quick check tests")
	} else if rbf.RaceEnabled {
		t.Skip("race detection enabled, skipping")
	}

	const n = 100000

	QuickCheck(t, func(t *testing.T, rand *rand.Rand) {
		t.Parallel()

		// Generate sorted list of values.
		values := make([]uint64, n)
		for i := range values {
			values[i] = uint64(rand.Intn(rbf.ShardWidth))
		}
		sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })

		db := MustOpenDB(t)
		defer MustCloseDB(t, db)
		tx := MustBegin(t, db, true)
		defer MustRollback(t, tx)

		// Insert values in random order.
		if err := tx.CreateBitmap("x"); err != nil {
			t.Fatal(err)
		}
		for _, i := range rand.Perm(len(values)) {
			v := values[i]
			if _, err := tx.Add("x", v); err != nil {
				t.Fatalf("Add(%d) i=%d err=%q", v, i, err)
			}
		}

		// Generate unique bucketed values.
		type Item struct {
			key    uint64
			values []uint16
		}
		var items []Item
		m := make(map[uint64]struct{})
		for _, v := range values {
			if _, ok := m[v]; ok {
				continue
			}
			m[v] = struct{}{}

			hi, lo := highbits(v), lowbits(v)
			if len(items) == 0 || items[len(items)-1].key != hi {
				items = append(items, Item{key: hi})
			}

			item := &items[len(items)-1]
			item.values = append(item.values, lo)
		}

		// Verify cursor returns correct value groups.
		c, err := tx.Cursor("x")
		if err != nil {
			t.Fatal(err)
		} else if err := c.Last(); err != nil {
			t.Fatal(err)
		}
		for i := len(items) - 1; i >= 0; i-- {
			if err := c.Prev(); err != nil {
				t.Fatal(err)
			} else if got, want := c.Key(), items[i].key; got != want {
				t.Fatalf("Key()=%d, want %d", got, want)
			} else if got, want := c.Values(), items[i].values; !reflect.DeepEqual(got, want) {
				t.Fatalf("len(Values())=%v, want %v", len(got), len(want))
			}
		}
	})
}

func TestCursor_Union(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		db := MustOpenDB(t)
		defer MustCloseDB(t, db)
		tx := MustBegin(t, db, true)
		defer MustRollback(t, tx)

		if err := tx.CreateBitmap("x"); err != nil {
			t.Fatal(err)
		}

		row := make([]uint64, rbf.ShardWidth/64)

		if _, err := tx.Add("x", 1, 3); err != nil {
			t.Fatal(err)
		} else if _, err := tx.Add("x", rbf.ShardWidth+1, rbf.ShardWidth+2, rbf.ShardWidth+7); err != nil {
			t.Fatal(err)
		}

		c, err := tx.Cursor("x")
		if err != nil {
			t.Fatal(err)
		}
		if err := c.Union(0, row); err != nil {
			t.Fatal(err)
		} else if row[0] != 0b00001010 {
			t.Fatalf("unexpected row[0]: 0b%b", row[0])
		}

		if err := c.Union(1, row); err != nil {
			t.Fatal(err)
		} else if row[0] != 0b10001110 {
			t.Fatalf("unexpected row[0]: 0b%b", row[0])
		}
	})

	t.Run("Quick", func(t *testing.T) {
		if testing.Short() {
			t.Skip("-short enabled, skipping")
		} else if is32Bit() {
			t.Skip("32-bit build, skipping quick check tests")
		} else if rbf.RaceEnabled {
			t.Skip("race detection enabled, skipping")
		}

		QuickCheck(t, func(t *testing.T, rand *rand.Rand) {
			t.Parallel()

			db := MustOpenDB(t)
			defer MustCloseDB(t, db)
			tx := MustBegin(t, db, true)
			defer MustRollback(t, tx)
			values := GenerateValues(rand, 100000)
			rows := ToRows(values)

			if err := tx.CreateBitmap("x"); err != nil {
				t.Fatal(err)
			}
			MustAddRandom(t, rand, tx, "x", values...)

			// Iterate over rows and randomly choose another row to union.
			for i, row0 := range rows {
				row1 := rows[rand.Intn(len(rows))]

				bitmap := row0.Bitmap()
				c, err := tx.Cursor("x")
				if err != nil {
					t.Fatal(err)
				} else if err := c.Union(row1.ID, bitmap); err != nil {
					return
				}

				if got, want := len(rbf.RowValues(bitmap)), len(row0.Union(row1)); got != want {
					t.Fatalf("%d. len()=%d, want %d", i, got, want)
				}
			}
		})
	})
}

func TestCursor_Intersect(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		db := MustOpenDB(t)
		defer MustCloseDB(t, db)
		tx := MustBegin(t, db, true)
		defer MustRollback(t, tx)

		row := make([]uint64, rbf.ShardWidth/64)

		if err := tx.CreateBitmap("x"); err != nil {
			t.Fatal(err)
		}

		if _, err := tx.Add("x", 1, 3); err != nil {
			t.Fatal(err)
		} else if _, err := tx.Add("x", rbf.ShardWidth+1, rbf.ShardWidth+2, rbf.ShardWidth+7); err != nil {
			t.Fatal(err)
		}

		c, err := tx.Cursor("x")
		if err != nil {
			t.Fatal(err)
		}

		if err := c.Union(0, row); err != nil {
			t.Fatal(err)
		} else if row[0] != 0b00001010 {
			t.Fatalf("unexpected row[0]: %#v", row[0])
		}

		if err := c.Intersect(1, row); err != nil {
			t.Fatal(err)
		} else if row[0] != 0b00000010 {
			t.Fatalf("unexpected row[0]: %#v", row[0])
		}
	})

	t.Run("Quick", func(t *testing.T) {
		if testing.Short() {
			t.Skip("-short enabled, skipping")
		} else if is32Bit() {
			t.Skip("32-bit build, skipping quick check tests")
		} else if rbf.RaceEnabled {
			t.Skip("race detection enabled, skipping")
		}

		QuickCheck(t, func(t *testing.T, rand *rand.Rand) {
			t.Parallel()

			db := MustOpenDB(t)
			defer MustCloseDB(t, db)
			tx := MustBegin(t, db, true)
			defer MustRollback(t, tx)
			values := GenerateValues(rand, rand.Intn(100000))
			rows := ToRows(values)

			if err := tx.CreateBitmap("x"); err != nil {
				t.Fatal(err)
			}
			MustAddRandom(t, rand, tx, "x", values...)

			// Iterate over rows and randomly choose another row to union.
			for i, row0 := range rows {
				row1 := rows[rand.Intn(len(rows))]

				bitmap := row0.Bitmap()
				if c, err := tx.Cursor("x"); err != nil {
					t.Fatal(err)
				} else if err := c.Intersect(row1.ID, bitmap); err != nil {
					t.Fatal(err)
				}

				if got, want := len(rbf.RowValues(bitmap)), len(row0.Intersect(row1)); got != want {
					t.Fatalf("%d. len()=%d, want %d", i, got, want)
				}
			}
		})
	})
}

func makeBitmap(bit []uint16) (n int, ret []uint64) {
	ret = make([]uint64, 1024)
	for _, v := range bit {
		ret[v/64] |= 1 << uint64(v%64)
	}
	n = 0
	for _, v := range ret {
		n += bits.OnesCount64(v)
	}
	return
}

func TestCursor_AddRoaring(t *testing.T) {
	db := MustOpenDB(t)
	defer MustCloseDB(t, db)
	tx := MustBegin(t, db, true)
	defer MustRollback(t, tx)

	if err := tx.CreateBitmap("x"); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name        string
		fieldview   string
		rb          *roaring.Bitmap
		wantChanged bool
		wantErr     bool
	}{{
		name:      "no view",
		fieldview: "a/standard",
		rb: func() *roaring.Bitmap {
			bm := roaring.NewBitmap()
			return bm
		}(),
		wantChanged: false,
		wantErr:     true},
		{
			name:      "initial Array",
			fieldview: "x",
			rb: func() *roaring.Bitmap {
				bm := roaring.NewBitmap()
				bm.Put(0, roaring.NewContainerArray([]uint16{1, 2}))
				return bm
			}(),
			wantChanged: true,
			wantErr:     false}, {
			name:      "initial RLE",
			fieldview: "x",
			rb: func() *roaring.Bitmap {
				bm := roaring.NewBitmap()
				bm.Put(1, roaring.NewContainerRun([]roaring.Interval16{{Start: 10, Last: 20000}}))
				return bm
			}(),
			wantChanged: true,
			wantErr:     false},
		{
			name:      "initial Bitmap",
			fieldview: "x",
			rb: func() *roaring.Bitmap {
				bm := roaring.NewBitmap()
				bm.Put(3, roaring.NewContainerBitmap(makeBitmap([]uint16{4, 8, 12})))
				return bm
			}(),
			wantChanged: true,
			wantErr:     false},
		{
			name:      "merge Array exist",
			fieldview: "x",
			rb: func() *roaring.Bitmap {
				bm := roaring.NewBitmap()
				bm.Put(0, roaring.NewContainerArray([]uint16{1, 2}))
				return bm
			}(),
			wantChanged: false,
			wantErr:     false}, {
			name:      "merge Array present",
			fieldview: "x",
			rb: func() *roaring.Bitmap {
				bm := roaring.NewBitmap()
				bm.Put(0, roaring.NewContainerArray([]uint16{3, 4}))
				return bm
			}(),
			wantChanged: true,
			wantErr:     false},
		{
			name:      "merge Bitmap exist",
			fieldview: "x",
			rb: func() *roaring.Bitmap {
				bm := roaring.NewBitmap()
				bm.Put(3, roaring.NewContainerBitmap(makeBitmap([]uint16{4, 8, 12})))
				return bm
			}(),
			wantChanged: false,
			wantErr:     false},
		{
			name:      "merge Bitmap ",
			fieldview: "x",
			rb: func() *roaring.Bitmap {
				bm := roaring.NewBitmap()
				bm.Put(3, roaring.NewContainerBitmap(makeBitmap([]uint16{75})))
				return bm
			}(),
			wantChanged: true,
			wantErr:     false},
		{
			name:      "merge BitmapArray ",
			fieldview: "x",
			rb: func() *roaring.Bitmap {
				bm := roaring.NewBitmap()
				bm.Put(0, roaring.NewContainerBitmap(makeBitmap([]uint16{75})))
				return bm
			}(),
			wantChanged: true,
			wantErr:     false}, {
			name:      "too Big Array ",
			fieldview: "x",
			rb: func() *roaring.Bitmap {
				bm := roaring.NewBitmap()
				items := make([]uint16, rbf.ArrayMaxSize+2)
				for i := 0; i < len(items); i++ {
					items[i] = uint16(i)
				}
				bm.Put(10, roaring.NewContainerArray(items))
				return bm
			}(),
			wantChanged: true,
			wantErr:     false},
		{
			name:      "too Big RLE ",
			fieldview: "x",
			rb: func() *roaring.Bitmap {
				bm := roaring.NewBitmap()
				items := make([]roaring.Interval16, rbf.RLEMaxSize+2)
				x := uint16(0)
				for i := 0; i < len(items); i++ {
					v := roaring.Interval16{Start: x, Last: x + 1}
					x += 3
					items[i] = v
				}
				bm.Put(10, roaring.NewContainerRun(items))
				return bm
			}(),
			wantChanged: true,
			wantErr:     false}, {
			name:      "empty container ",
			fieldview: "x",
			rb: func() *roaring.Bitmap {
				bm := roaring.NewBitmap()
				bm.Put(11, roaring.NewContainerArray([]uint16{}))
				return bm
			}(),
			wantChanged: false,
			wantErr:     false},
		{
			name:      "merge RLE",
			fieldview: "x",
			rb: func() *roaring.Bitmap {
				bm := roaring.NewBitmap()
				bm.Put(1, roaring.NewContainerRun([]roaring.Interval16{{Start: 1, Last: 12}}))
				return bm
			}(),
			wantChanged: true,
			wantErr:     false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotChanged, err := tx.AddRoaring(tt.fieldview, tt.rb)
			if (err != nil) != tt.wantErr {
				t.Errorf("Cursor.AddRoaring() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotChanged != tt.wantChanged {
				t.Errorf("Cursor.AddRoaring() = %v, want %v", gotChanged, tt.wantChanged)
			}
		})
	}
}

func TestCursor_RLETesting(t *testing.T) {
	db := MustOpenDB(t)
	defer MustCloseDB(t, db)
	tx := MustBegin(t, db, true)
	defer MustRollback(t, tx)
	//setup RLE
	if err := tx.CreateBitmap("x"); err != nil {
		t.Fatal(err)
	}
	rb := func() *roaring.Bitmap {
		bm := roaring.NewBitmap()
		bm.Put(0, roaring.NewContainerRun([]roaring.Interval16{{Start: 10, Last: 11}}))
		return bm
	}()
	_, err := tx.AddRoaring("x", rb)
	if err != nil {
		t.Errorf("Add Roaring Failed %v", err)
	}
	//
	tests := []struct {
		name        string
		args        []uint64
		want        []uint16
		wantChanged bool
		wantErr     bool
	}{{
		name:        "update run at Last",
		args:        []uint64{0x0000000c},
		want:        []uint16{0x0000000a, 0x0000000b, 0x0000000c},
		wantChanged: true,
		wantErr:     false,
	},
		{
			name:        "update run at begining",
			args:        []uint64{0x00000001, 0x00000002},
			want:        []uint16{0x00000001, 0x00000002, 0x0000000a, 0x0000000b, 0x0000000c},
			wantChanged: true,
			wantErr:     false,
		},
		{
			name:        "add run at end",
			args:        []uint64{0x0000000f},
			want:        []uint16{0x00000001, 0x00000002, 0x0000000a, 0x0000000b, 0x0000000c, 0x0000000f},
			wantChanged: true,
			wantErr:     false,
		},
		{
			name:        "no change",
			args:        []uint64{0x0000000b},
			want:        []uint16{0x00000001, 0x00000002, 0x0000000a, 0x0000000b, 0x0000000c, 0x0000000f},
			wantChanged: false,
			wantErr:     false,
		}, {
			name:        "update start",
			args:        []uint64{0x0000000e},
			want:        []uint16{0x00000001, 0x00000002, 0x0000000a, 0x0000000b, 0x0000000c, 0x0000000e, 0x0000000f},
			wantChanged: true,
			wantErr:     false,
		}, {
			name:        "combine",
			args:        []uint64{0x0000000d},
			want:        []uint16{0x00000001, 0x00000002, 0x0000000a, 0x0000000b, 0x0000000c, 0x0000000d, 0x0000000e, 0x0000000f},
			wantChanged: true,
			wantErr:     false,
		}, {
			name:        "add end",
			args:        []uint64{0x0000ffff},
			want:        []uint16{0x00000001, 0x00000002, 0x0000000a, 0x0000000b, 0x0000000c, 0x0000000d, 0x0000000e, 0x0000000f, 0x0000ffff},
			wantChanged: true,
			wantErr:     false,
		},
		{
			name:        "overflow container",
			args:        []uint64{0x00010000},
			want:        []uint16{0x00000001, 0x00000002, 0x0000000a, 0x0000000b, 0x0000000c, 0x0000000d, 0x0000000e, 0x0000000f, 0x0000ffff},
			wantChanged: true,
			wantErr:     false,
		},
	}

	c, err := tx.Cursor("x")
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			changed, err := tx.Add("x", tt.args...)
			if tt.wantErr && err == nil {
				t.Errorf("No Error %v", err)
			} else if tt.wantChanged && !changed {
				t.Errorf("No Change %v", err)
			} else if err != nil {
				t.Fatal(err)
			} else if err := c.First(); err != nil {
				t.Fatal(err)
			}
			if got, want := c.Values(), tt.want; !reflect.DeepEqual(got, want) {
				t.Fatalf("Values()=%#v, want %#v", got, want)
			}
		})
	}

	t.Run("overflow followup", func(t *testing.T) {
		//verify than next container got created and is valid
		if err := c.Next(); err != nil { //skip the buffered?
			t.Fatal(err)
		}
		if err := c.Next(); err != nil {
			t.Fatal(err)
		}

		want := []uint16{0}
		if got, want := c.Values(), want; !reflect.DeepEqual(got, want) {
			t.Fatalf("Values()=%#v, want %#v", got, want)
		} else if got, want := c.Key(), uint64(1); !reflect.DeepEqual(got, want) {
			t.Fatalf("Key()=%#v, want %#v", got, want)
		}
	})
}

func TestCursor_RLEConversion(t *testing.T) {
	db := MustOpenDB(t)
	defer MustCloseDB(t, db)
	tx := MustBegin(t, db, true)
	defer MustRollback(t, tx)
	//setup RLE with full container
	if err := tx.CreateBitmap("x"); err != nil {
		t.Fatal(err)
	}
	want := make([]uint16, 0, rbf.ArrayMaxSize)
	rb := func() *roaring.Bitmap {
		bm := roaring.NewBitmap()
		runs := make([]roaring.Interval16, rbf.RLEMaxSize)
		x := uint16(1)
		for i := range runs {
			runs[i] = roaring.Interval16{Start: x, Last: x + 1}
			want = append(want, x)
			want = append(want, x+1)
			x += 3
		}
		bm.Put(0, roaring.NewContainerRun(runs))
		return bm
	}()

	_, err := tx.AddRoaring("x", rb)
	if err != nil {
		t.Errorf("Add Roaring Failed %v", err)
	}
	c, err := tx.Cursor("x")
	if err != nil {
		t.Fatal(err)
	} else if err := c.First(); err != nil {
		t.Fatal(err)
	}

	if c.CurrentPageType() != rbf.ContainerTypeRLE {
		t.Fatalf("Should Be RLE but is: %v\n", c.CurrentPageType())
	}
	exists, err := c.Contains(0x7)
	if err != nil {
		t.Fatalf("ERR:%v", err)
	}
	if !exists {
		t.Fatalf("Should Contain %v", 0x7)
	}
	exists, err = c.Contains(0x6)
	if err != nil {
		t.Fatalf("ERR:%v", err)
	}
	if exists {
		t.Fatalf("Should Not Contain %v", 0x6)
	}

	//add a few bits to create another run
	_, err = tx.Add("x",
		func() []uint64 {
			r := make([]uint64, 0, 128)
			for x := uint64(65408); x < 65536; x++ {
				r = append(r, x)
				want = append(want, uint16(x))
			}
			return r
		}()...)
	if err != nil {
		t.Fatalf("ERR adding bits: %v\n", err)

	}

	if err := c.First(); err != nil {
		t.Fatal(err)
	}
	if got, want := c.Values(), want; !reflect.DeepEqual(got, want) {
		t.Fatalf("Values()=%#v, want %#v", got, want)
	} else if c.CurrentPageType() != rbf.ContainerTypeBitmap {
		t.Fatalf("Should be bitmap but is %v", c.CurrentPageType())
	}

}

type EasyWalker struct {
	tx   *rbf.Tx
	path strings.Builder
}

func (e *EasyWalker) Visitor(pgno uint32, records []*rbf.RootRecord) {
	for _, record := range records {
		e.VisitRoot(record.Pgno, record.Name)
		rbf.Page(e.tx, record.Pgno, e)
	}
}
func (e *EasyWalker) VisitRoot(pgno uint32, name string) {
	e.path.WriteString("R")
}
func (e *EasyWalker) VisitBranch(pgno uint32) {
	e.path.WriteString("B")

}
func (e *EasyWalker) VisitLeaf(pgno uint32) {
	e.path.WriteString("L")

}
func (e *EasyWalker) VisitBitmap(pgno uint32) {
}
func (e *EasyWalker) String() string {
	return e.path.String()
}

func TestCursor_UpdateBranchCells(t *testing.T) {
	db := MustOpenDB(t)
	defer MustCloseDB(t, db)
	tx := MustBegin(t, db, true)
	defer MustRollback(t, tx)
	if err := tx.CreateBitmap("x"); err != nil {
		t.Fatal(err)
	}
	c, err := tx.Cursor("x")
	if err != nil {
		t.Fatal(err)
	}
	if err != nil {
		t.Fatal(err)
	}
	changed, err := c.Add(1)
	if changed {

		if err := c.First(); err != nil {
			t.Fatal(err)
		}
		if got, want := c.Values(), []uint16{uint16(1)}; !reflect.DeepEqual(got, want) {
			t.Fatal(err)
		}
	} else {
		t.Fatal("Expected Add Change")
	}
	changed, err = c.Remove(1)
	if changed {
		c.First()
		if got, want := c.Values(), []uint16{}; !reflect.DeepEqual(got, want) {
			t.Fatal(err)
		}
	} else {
		t.Fatal("Expected Remove Change")
	}

	rb := func() *roaring.Bitmap {
		bm := roaring.NewBitmap()
		bm.Put(0, roaring.NewContainerRun([]roaring.Interval16{{Start: 1, Last: 2}}))
		return bm
	}()
	_, err = tx.AddRoaring("x", rb)
	if err != nil {
		t.Fatal(err)
	}
	changed, err = c.Remove(2)
	if changed {
		c.First()
		if got, want := c.Values(), []uint16{1}; !reflect.DeepEqual(got, want) {
			t.Fatal(err)
		}
	} else {
		t.Fatal("Expected Remove Change")
	}

	changed, err = c.Remove(1)
	if changed {
		c.First()
		if got, want := c.Values(), []uint16{}; !reflect.DeepEqual(got, want) {
			t.Fatal(err)
		}
	} else {
		t.Fatal("Expected Remove Change")
	}

}

func TestCursor_SplitBranchCells(t *testing.T) {
	db := MustOpenDB(t)
	defer MustCloseDB(t, db)
	tx := MustBegin(t, db, true)
	defer MustRollback(t, tx)
	if err := tx.CreateBitmap("x"); err != nil {
		t.Fatal(err)
	}
	rb := func(key uint64) *roaring.Bitmap {
		bm := roaring.NewBitmap()
		bits := make([]uint64, rbf.BitmapN)
		n := 0
		for i := range bits {
			bits[i] = ^uint64(0)
			n += 64
		}
		bm.Put(key, roaring.NewContainerBitmap(n, bits))
		return bm
	}
	//634 == offset, 24== size of leafcell with bitmap
	// measured should split at 634+(24*314)
	numContainers := 314
	for i := 0; i < numContainers; i++ { //need to calculate how many will force a split
		b := rb(uint64(i))
		tx.AddRoaring("x", b)
	}
	before := &EasyWalker{tx: tx}
	rbf.Page(tx, 0, before)
	if before.String() != "RL" {

		t.Fatalf("Expecting RL (one branch) got %v", before.String())

	}
	// adding one more container should split it
	tx.AddRoaring("x", rb(uint64(numContainers)))
	after := &EasyWalker{tx: tx}
	rbf.Page(tx, 0, after)
	if after.String() != "RBLL" {

		t.Fatalf("Expecting RBLL (a branch split) got %v", after.String())

	}

}

func TestCursor_RemoveCells(t *testing.T) {
	db := MustOpenDB(t)
	defer MustCloseDB(t, db)
	tx := MustBegin(t, db, true)
	defer MustRollback(t, tx)
	if err := tx.CreateBitmap("x"); err != nil {
		t.Fatal(err)
	}
	cur, _ := tx.Cursor("x")
	rb := func(key uint64) *roaring.Bitmap {
		bm := roaring.NewBitmap()
		bits := make([]uint64, rbf.BitmapN)
		n := 0
		for i := range bits {
			bits[i] = ^uint64(0)
			n += 64
		}
		bm.Put(key, roaring.NewContainerBitmap(n, bits))
		return bm
	}
	numContainers := 455 //enough containers to cause a split
	for i := 0; i < numContainers; i++ {
		b := rb(uint64(i))
		tx.AddRoaring("x", b)
	}

	for i := numContainers; i >= 1; i-- {
		cur.RemoveRoaring(rb(uint64(i)))
	}

	cur.RemoveRoaring(rb(uint64(0)))

	//f, err := os.OpenFile("before.dot", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 066)
}

//These aren't test i'm just using to generate graphs to look at structure
func TestCursor_PlayContainer(t *testing.T) {
	db := MustOpenDB(t)
	defer MustCloseDB(t, db)
	tx := MustBegin(t, db, true)
	defer MustRollback(t, tx)
	if err := tx.CreateBitmap("x"); err != nil {
		t.Fatal(err)
	}
	many := func(c *rbf.Cursor, start, count uint64) {
		for i := start; i < start+count; i++ {
			c.Add(i)
		}
	}
	cur, _ := tx.Cursor("x")
	offset := uint64(0)
	many(cur, 0, rbf.ArrayMaxSize+offset)
	many(cur, 65536, rbf.ArrayMaxSize+offset)
	/*
	   many(cur, 2*65536, rbf.ArrayMaxSize+offset)
	   many(cur, 3*65536, rbf.ArrayMaxSize)    //+offset)
	   many(cur, 4*65536, rbf.ArrayMaxSize)    //+offset)
	   many(cur, 5*65536, rbf.ArrayMaxSize)    //+offset)
	   many(cur, 6*65536, 10)                  //+offset)
	   many(cur, 7*65536, 10)                  //+offset)
	   many(cur, 8*65536, rbf.ArrayMaxSize+10) //+offset)
	   many(cur, 9*65536, rbf.ArrayMaxSize+10) //+offset)
	*/

	cur.First()
	cur.Dump("fun.dot")
}

func TestCursor_OneBitmap(t *testing.T) {
	db := MustOpenDB(t)
	defer MustCloseDB(t, db)
	tx := MustBegin(t, db, true)
	defer MustRollback(t, tx)
	if err := tx.CreateBitmap("x"); err != nil {
		t.Fatal(err)
	}
	rb := func(key uint64) *roaring.Bitmap {
		bm := roaring.NewBitmap()
		bits := make([]uint64, rbf.BitmapN)
		n := 0
		for i := range bits {
			bits[i] = ^uint64(0)
			n += 64
		}
		bm.Put(key, roaring.NewContainerBitmap(n, bits))
		return bm
	}
	numContainers := 4
	for i := 0; i < numContainers; i++ { //need to calculate how many will force a split
		b := rb(uint64(i)) // measured at i=454 seems reasonable should occur at Len(branchcells)+header >8192
		tx.AddRoaring("x", b)
	}
	cur, err := tx.Cursor("x")
	if err != nil {
		panic(err)
	}
	cur.First()
	cur.Dump("fun.dot")
}
func TestCursor_GenerateAll(t *testing.T) {
	db := MustOpenDB(t)
	defer MustCloseDB(t, db)
	tx := MustBegin(t, db, true)
	defer MustRollback(t, tx)
	if err := tx.CreateBitmap("x"); err != nil {
		t.Fatal(err)
	}
	ar := func() *roaring.Bitmap {
		bm := roaring.NewBitmap()
		bm.Put(11, roaring.NewContainerArray([]uint16{1, 2, 3}))
		return bm
	}()
	tx.AddRoaring("x", ar)
	rb := func() *roaring.Bitmap {
		bm := roaring.NewBitmap()
		bm.Put(1, roaring.NewContainerRun([]roaring.Interval16{{Start: 1, Last: 12}}))
		return bm
	}()
	tx.AddRoaring("x", rb)
	bb := func() *roaring.Bitmap {
		bm := roaring.NewBitmap()
		bm.Put(0, roaring.NewContainerBitmap(makeBitmap([]uint16{75})))
		return bm
	}()
	tx.AddRoaring("x", bb)
	if err := tx.CreateBitmap("field/view/"); err != nil {
		t.Fatal(err)
	}
	tx.AddRoaring("field/view/", bb)
	cur, err := tx.Cursor("field/view/")
	if err != nil {
		panic(err)
	}
	cur.Dump("fun.dot")
}
