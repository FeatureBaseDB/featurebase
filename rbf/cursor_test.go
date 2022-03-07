// Copyright 2021 Molecula Corp. All rights reserved.
package rbf_test

import (
	"bytes"
	"io"
	"math/bits"
	"math/rand"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/molecula/featurebase/v3/rbf"
	"github.com/molecula/featurebase/v3/roaring"
)

func TestCursor_FirstNext(t *testing.T) {
	db := MustOpenDB(t)
	defer MustCloseDB(t, db)
	tx := MustBegin(t, db, true)
	defer tx.Rollback()

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
	}

	const n = 10000

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
		defer tx.Rollback()

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
				t.Fatalf("len(Values())=%v/%#v, want %v/%#v", len(got), got, len(want), want)
			}
		}
	})
}

func TestCursor_LastPrev(t *testing.T) {
	db := MustOpenDB(t)
	defer MustCloseDB(t, db)
	tx := MustBegin(t, db, true)
	defer tx.Rollback()

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
	}

	const n = 10000

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
		defer tx.Rollback()

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
	defer tx.Rollback()

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
		wantErr:     false},
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
	defer tx.Rollback()
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
			changeCount, err := tx.Add("x", tt.args...)
			if tt.wantErr && err == nil {
				t.Errorf("No Error %v", err)
			} else if tt.wantChanged && changeCount == 0 {
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
		got := c.Values()
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("Values()=%#v, want %#v", got, want)
		}
		if g, w := c.Key(), uint64(1); !reflect.DeepEqual(g, w) {
			t.Fatalf("Key()=%#v, want %#v", g, w)
		}
	})
}

func TestCursor_BitmapBitN(t *testing.T) {
	db := MustOpenDB(t)
	defer MustCloseDB(t, db)
	tx := MustBegin(t, db, true)
	defer tx.Rollback()
	if err := tx.CreateBitmap("x"); err != nil {
		t.Fatal(err)
	}
	bmData := make([]uint64, 1024)
	for i := range bmData {
		bmData[i] = 0x5555555555555555
	}
	ct := roaring.NewContainerBitmap(-1, bmData)
	err := tx.PutContainer("x", 0, ct)
	if err != nil {
		t.Fatalf("error writing container: %v", err)
	}
	// Putting a container to the same slot, which is also a bitmap
	// (rather than a BitmapPtr), while the existing cell is a BitmapPtr,
	// may not update BitN correctly.
	ct, _ = ct.Add(1)
	err = tx.PutContainer("x", 0, ct)
	if err != nil {
		t.Fatalf("rewriting container: %v", err)
	}
	v, err := tx.Container("x", 0)
	if err != nil {
		t.Fatalf("getting container: %v", err)
	}
	c1 := v.N()
	v.Repair()
	c2 := v.N()
	if c1 != c2 {
		t.Fatalf("expected count %d, got %d", c2, c1)
	}
	tx.Commit()
}

func TestCursor_RLEConversion(t *testing.T) {
	db := MustOpenDB(t)
	defer MustCloseDB(t, db)
	tx := MustBegin(t, db, true)
	defer tx.Rollback()
	//setup RLE with full container
	if err := tx.CreateBitmap("x"); err != nil {
		t.Fatal(err)
	}
	exp := 0 // expected count of BitN
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
			exp += 2
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

	c.DebugSlowCheckAllPages()

	//i := 0
	for x := uint64(65408); x < 65536; x++ {
		_, err = tx.Add("x", x)
		if err != nil {
			panic(err)
		}
		//vv("on i=%v, x = %v", i, x)
		c.DebugSlowCheckAllPages()
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

	c.DebugSlowCheckAllPages()
	//vv("past 2nd check")

	got, want := c.Values(), want
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Values()=%#v, want %#v", got, want)
	}

	// Split a run.
	// There's a run at the end from 0xff80 - 0xffff (65408 - 65535).
	// Split it in half.
	removeMe := uint64(65471)
	_, err = c.Remove(removeMe)
	if err != nil {
		t.Fatalf("remove failed")
	}

	got2 := c.Values()
	target := uint16(removeMe)
	for _, v := range got2 {
		if v == target {
			t.Fatalf("removal of %v from rle did not succeed", removeMe)
		}
	}

	c.DebugSlowCheckAllPages()

	// add it back
	_, err = c.Add(removeMe)
	if err != nil {
		t.Fatalf("add failed")
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Values()=%#v, want %#v", got, want)
	}

	c.DebugSlowCheckAllPages()
}

// test shrinking from bitmap down to array when a bit is removed
func TestCursor_BitmapToArrayConversion(t *testing.T) {
	db := MustOpenDB(t)
	defer MustCloseDB(t, db)
	tx := MustBegin(t, db, true)
	defer tx.Rollback()

	// Setup a Bitmap with more than ArrayMaxSize, then
	// delete bits to down to below ArrayMaxSize, and
	// verify the container type got converted to an array
	// and that the BitN and ElemN are always correct.
	//
	if err := tx.CreateBitmap("x"); err != nil {
		t.Fatal(err)
	}

	// start with n = 4084 bits, over ArrayMaxSize
	rb := roaring.NewBitmap()
	bits := make([]uint64, rbf.BitmapN)
	n := 0
	for i := range bits {
		bits[i] = 15 // ^uint64(0)
		n += 4
		if n > rbf.ArrayMaxSize {
			break
		}
	}
	rb.Put(0, roaring.NewContainerBitmap(n, bits))

	// setup to delete random bits down
	values := rb.Slice()
	valmap := make(map[uint64]bool)
	for _, v := range values {
		valmap[v] = true
	}

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

	if c.CurrentPageType() != rbf.ContainerTypeBitmapPtr {
		t.Fatalf("Should Be BitmapPtr but is: %v\n", c.CurrentPageType())
	}

	exists, err := c.Contains(0x3)
	if err != nil {
		t.Fatalf("ERR:%v", err)
	}
	if !exists {
		t.Fatalf("Should Contain %v", 0x3)
	}

	exists, err = c.Contains(0x4)
	if err != nil {
		t.Fatalf("ERR:%v", err)
	}
	if exists {
		t.Fatalf("Should Not Contain %v", 0x4)
	}

	c.DebugSlowCheckAllPages()

	for len(valmap) > 0 {

		// pick a bit to evict
		var removeMe uint64
		for removeMe = range valmap {
			break
		}

		c.DebugSlowCheckAllPages()

		exists, err := c.Contains(removeMe)
		if err != nil {
			t.Fatalf("ERR:%v", err)
		}
		if !exists {
			t.Fatalf("Should Contain %v", removeMe)
		}

		_, err = c.Remove(removeMe)
		if err != nil {
			t.Fatalf("remove failed")
		}
		//vv("removed %v, have %v left", removeMe, len(valmap))
		c.DebugSlowCheckAllPages()

		exists, err = c.Contains(removeMe)
		if err != nil {
			t.Fatalf("ERR:%v", err)
		}
		if exists {
			t.Fatalf("Should NOT Contain %v", removeMe)
		}

		delete(valmap, removeMe)
		n := len(valmap)

		if n > rbf.ArrayMaxSize {
			if c.CurrentPageType() != rbf.ContainerTypeBitmapPtr {
				t.Fatalf("Should Be BitmapPtr but is: %v\n", c.CurrentPageType())
			}
		} else {
			if n > 0 { // no pages if n == 0, so cannot get a CurrentPageType()
				if c.CurrentPageType() == rbf.ContainerTypeBitmapPtr {
					t.Fatalf("Should NOT Be BitmapPtr but is indeed: %v\n", c.CurrentPageType())
				}
			}
		}
	}
	// check it is empty
	va := c.Values()
	if len(va) != 0 {
		t.Fatalf("expected empty container, but see %v values left: '%#v'", len(va), va)
	}

}

type EasyWalker struct {
	tx   *rbf.Tx
	path strings.Builder
}

func (e *EasyWalker) Visitor(pgno uint32, records []*rbf.RootRecord) {
	for _, record := range records {
		e.VisitRoot(record.Pgno, record.Name)
		rbf.WalkPage(e.tx, record.Pgno, e)
	}
}
func (e *EasyWalker) VisitRoot(pgno uint32, name string) {
	e.path.WriteString("R")
}
func (e *EasyWalker) Visit(pgno uint32, node rbf.Nodetype) {
	switch node {
	case rbf.Branch:
		e.path.WriteString("B")
	case rbf.Leaf:
		e.path.WriteString("L")
	case rbf.Bitmap:
		e.path.WriteString("b")
	}

}
func (e *EasyWalker) String() string {
	return e.path.String()
}

func TestDumpDot(t *testing.T) {
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

	_, err = c.Add(1)
	if err != nil {
		t.Fatal(err)
	}
	var b bytes.Buffer
	rbf.Dumpdot(tx, 0, " ", &b)
}

func TestCursor_UpdateBranchCells(t *testing.T) {
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

	changed, err := c.Add(1)
	if err != nil {
		t.Fatal(err)
	} else if !changed {
		t.Fatal("Expected Add Change")
	} else if err := c.First(); err != nil {
		t.Fatal(err)
	} else if got, want := c.Values(), []uint16{uint16(1)}; !reflect.DeepEqual(got, want) {
		t.Fatal(err)
	}

	changed, err = c.Remove(1)
	if err != nil {
		t.Fatal(err)
	} else if !changed {
		t.Fatal("Expected Remove Change")
	} else if err := c.First(); err != nil && err != io.EOF {
		t.Fatal(err)
	} else if got, want := c.Values(), ([]uint16)(nil); !reflect.DeepEqual(got, want) {
		t.Fatal(err)
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
		if err := c.First(); err != nil && err != io.EOF {
			panic(err)
		}
		if got, want := c.Values(), []uint16{1}; !reflect.DeepEqual(got, want) {
			t.Fatal(err)
		}
	} else {
		t.Fatal("Expected Remove Change")
	}

	changed, err = c.Remove(1)
	if changed {
		if err := c.First(); err != nil && err != io.EOF {
			panic(err)
		}
		if got, want := c.Values(), ([]uint16)(nil); !reflect.DeepEqual(got, want) {
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
	defer tx.Rollback()
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
		if _, err := tx.AddRoaring("x", b); err != nil {
			panic(err)
		}
	}
	before := &EasyWalker{tx: tx}
	rbf.WalkPage(tx, 0, before)
	if before.String() != "RL" {

		t.Fatalf("Expecting RL (one branch) got %v", before.String())

	}
	// adding one more container should split it
	if _, err := tx.AddRoaring("x", rb(uint64(numContainers))); err != nil {
		panic(err)
	}

	after := &EasyWalker{tx: tx}
	rbf.WalkPage(tx, 0, after)
	if after.String() != "RBLL" {

		t.Fatalf("Expecting RBLL (a branch split) got %v", after.String())

	}
	//
	c, _ := tx.Cursor("x") //added just for dot code coverage
	c.Dump("test.dump")
	os.Remove("test.dump")
}

func TestCursor_RemoveCells(t *testing.T) {
	db := MustOpenDB(t)
	defer MustCloseDB(t, db)
	tx := MustBegin(t, db, true)
	defer tx.Rollback()
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
		if _, err := tx.AddRoaring("x", b); err != nil {
			panic(err)
		}
	}

	for i := numContainers; i >= 1; i-- {
		if _, err := cur.RemoveRoaring(rb(uint64(i))); err != nil {
			panic(err)
		}
	}

	if _, err := cur.RemoveRoaring(rb(uint64(0))); err != nil {
		panic(err)
	}

	//f, err := os.OpenFile("before.dot", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 066)
}

//These aren't test i'm just using to generate graphs to look at structure
func TestCursor_PlayContainer(t *testing.T) {
	db := MustOpenDB(t)
	defer MustCloseDB(t, db)
	tx := MustBegin(t, db, true)
	defer tx.Rollback()
	if err := tx.CreateBitmap("x"); err != nil {
		t.Fatal(err)
	}
	many := func(c *rbf.Cursor, start, count uint64) {
		for i := start; i < start+count; i++ {
			if _, err := c.Add(i); err != nil {
				panic(err)
			}
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

	if err := cur.First(); err != nil {
		panic(err)
	}
}

func TestCursor_OneBitmap(t *testing.T) {
	db := MustOpenDB(t)
	defer MustCloseDB(t, db)
	tx := MustBegin(t, db, true)
	defer tx.Rollback()
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
		if _, err := tx.AddRoaring("x", b); err != nil {
			panic(err)
		}
	}
	cur, err := tx.Cursor("x")
	if err != nil {
		panic(err)
	}
	if err := cur.First(); err != nil {
		panic(err)
	}
}
func TestCursor_GenerateAll(t *testing.T) {
	db := MustOpenDB(t)
	defer MustCloseDB(t, db)
	tx := MustBegin(t, db, true)
	defer tx.Rollback()
	if err := tx.CreateBitmap("x"); err != nil {
		t.Fatal(err)
	}
	ar := func() *roaring.Bitmap {
		bm := roaring.NewBitmap()
		bm.Put(11, roaring.NewContainerArray([]uint16{1, 2, 3}))
		return bm
	}()
	if _, err := tx.AddRoaring("x", ar); err != nil {
		panic(err)
	}
	rb := func() *roaring.Bitmap {
		bm := roaring.NewBitmap()
		bm.Put(1, roaring.NewContainerRun([]roaring.Interval16{{Start: 1, Last: 12}}))
		return bm
	}()
	if _, err := tx.AddRoaring("x", rb); err != nil {
		panic(err)
	}
	bb := func() *roaring.Bitmap {
		bm := roaring.NewBitmap()
		bm.Put(0, roaring.NewContainerBitmap(makeBitmap([]uint16{75})))
		return bm
	}()
	if _, err := tx.AddRoaring("x", bb); err != nil {
		panic(err)
	}
	if err := tx.CreateBitmap("field/view/"); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.AddRoaring("field/view/", bb); err != nil {
		panic(err)
	}
}

// test ForEachRange handles Bitmaps, because BitmapPtr
// case was missing
func TestForEachRange(t *testing.T) {
	db := MustOpenDB(t)
	defer MustCloseDB(t, db)
	tx := MustBegin(t, db, true)
	defer tx.Rollback()

	if err := tx.CreateBitmap("x"); err != nil {
		t.Fatal(err)
	}

	rb := roaring.NewBitmap()
	bits := make([]uint64, rbf.BitmapN)
	n := 0
	for i := range bits {
		bits[i] = 15 // ^uint64(0)
		n += 4
		if n > rbf.ArrayMaxSize {
			break
		}
	}

	rb.Put(0, roaring.NewContainerBitmap(n, bits))
	crun := roaring.NewContainerRun([]roaring.Interval16{{Start: 0, Last: 1<<16 - 1}})
	rb.Put(1, crun)
	rb.Put(2, roaring.NewContainerArray([]uint16{1, 1024, 1<<16 - 1}))

	// setup to delete random bits down
	values := rb.Slice()
	valmap := make(map[uint64]bool)
	for _, v := range values {
		valmap[v] = true
	}

	_, err := tx.AddRoaring("x", rb)
	if err != nil {
		t.Errorf("Add Roaring Failed %v", err)
	}

	c, err := tx.Cursor("x")
	if err != nil {
		t.Fatal(err)
	}
	c.DebugSlowCheckAllPages()

	if err := c.First(); err != nil {
		t.Fatal(err)
	}

	exists, err := c.Contains(0x3)
	if err != nil {
		t.Fatalf("ERR:%v", err)
	}
	if !exists {
		t.Fatalf("Should Contain %v", 0x3)
	}

	exists, err = c.Contains(0x4)
	if err != nil {
		t.Fatalf("ERR:%v", err)
	}
	if exists {
		t.Fatalf("Should Not Contain %v", 0x4)
	}

	c.DebugSlowCheckAllPages()

	_ = tx.ForEach("x", func(i uint64) error {
		delete(valmap, i)
		return nil
	})

	// check it is empty
	if len(valmap) != 0 {
		t.Fatalf("expected empty container, but see %v values left: '%#v'", len(valmap), valmap)
	}
}

func TestCursor_PutContainer(t *testing.T) {
	t.Run("BitmapToArray", func(t *testing.T) {
		db := MustOpenDB(t)
		defer MustCloseDB(t, db)

		tx := MustBegin(t, db, true)
		defer tx.Rollback()

		if err := tx.CreateBitmap("x"); err != nil {
			t.Fatal(err)
		}

		bmData := make([]uint64, 1024)
		for i := range bmData {
			bmData[i] = 0x5555555555555555
		}
		if err := tx.PutContainer("x", 0, roaring.NewContainerBitmap(-1, bmData)); err != nil {
			t.Fatal(err)
		}
		if err := tx.PutContainer("x", 0, roaring.NewContainerArray([]uint16{0})); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("BitmapToBitmap", func(t *testing.T) {
		db := MustOpenDB(t)
		defer MustCloseDB(t, db)

		tx := MustBegin(t, db, true)
		defer tx.Rollback()

		if err := tx.CreateBitmap("x"); err != nil {
			t.Fatal(err)
		}

		data0 := make([]uint64, 1024)
		for i := range data0 {
			data0[i] = 0x5555555555555555
		}
		if err := tx.PutContainer("x", 0, roaring.NewContainerBitmap(-1, data0)); err != nil {
			t.Fatal(err)
		}

		data1 := make([]uint64, 1024)
		for i := range data0 {
			data1[i] = 0x7777777777777777
		}
		if err := tx.PutContainer("x", 0, roaring.NewContainerBitmap(-1, data1)); err != nil {
			t.Fatal(err)
		}

		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	})
}
