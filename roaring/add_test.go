// Copyright 2020 Pilosa Corp.
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

//go:build !race
// +build !race

package roaring

import (
	"fmt"
	"math/bits"
	"testing"

	"golang.org/x/exp/rand"
)

func randomMask(dst *[1024]uint64, seed uint64) uint32 {
	_ = &dst[0]

	var src rand.PCGSource
	src.Seed(seed)
	var n uint32
	for i := range dst {
		v := src.Uint64()
		n += uint32(bits.OnesCount64(v))
		dst[i] = src.Uint64()
	}
	return n
}

func randomArray(arr *[4096]uint16, mask *[1024]uint64, seed uint64) []uint16 {
	_, _ = &arr[0], &mask[0]

	var src rand.PCGSource
	src.Seed(seed)

	*mask = [1024]uint64{}

	nv := src.Uint64()
	n := (nv % 4096) >> ((nv / 4096) % 10)
	for i := uint64(0); i < n; i++ {
		v := uint16(src.Uint64())
		mask[v/64] |= 1 << (v % 64)
	}

	x := arr[:0]
	for i, v := range mask {
		for v != 0 {
			j := bits.TrailingZeros64(v)
			v &^= 1 << j
			x = append(x, 64*uint16(i)+uint16(j))
		}
	}

	return x
}

func splatArray(arr ...uint16) (dst [1024]uint64) {
	for _, v := range arr {
		dst[v/64] |= 1 << (v % 64)
	}
	return
}

func diffMask(t *testing.T, in string, expect, got *[1024]uint64) bool {
	t.Helper()

	n := 0
	for i := range expect {
		expv, gotv := expect[i], got[i]
		unexpected, missing := gotv&^expv, expv&^gotv
		for unexpected != 0 {
			j := bits.TrailingZeros64(unexpected)
			unexpected &^= 1 << j
			t.Errorf("unexpected value %d in %s", 64*i+j, in)
		}
		for missing != 0 {
			j := bits.TrailingZeros64(missing)
			missing &^= 1 << j
			t.Errorf("missing value %d in %s", 64*i+j, in)
		}
		n += bits.OnesCount64(unexpected | missing)
		if n > 20 {
			t.Errorf("too many errors in %s", in)
			break
		}
	}
	return n > 0
}

// TestAddInternal randomly tests the full and half adder logic in Add.
func TestAddInternal(t *testing.T) {
	t.Parallel()

	testCount := 2000
	if testing.Short() {
		testCount = 20
	}

	t.Run("addArrayArrayToArray", func(t *testing.T) {
		t.Parallel()

		var src rand.PCGSource
		src.Seed(1)
		seedup := make(map[uint64]struct{})

		for i := 0; i < testCount; i++ {
		genSeed:
			seed := src.Uint64()
			if _, dup := seedup[seed]; dup {
				goto genSeed
			}
			seedup[seed] = struct{}{}
			t.Run(fmt.Sprint(seed), func(t *testing.T) {
				t.Parallel()
				tries := 0
				var xmask, ymask [1024]uint64
				var xarr, yarr [4096]uint16
				var gotdst, gotcarry [4096]uint16
			try:
				x := randomArray(&xarr, &xmask, seed)
				y := randomArray(&yarr, &ymask, 3*seed)
				do, co := addArrayArrayToArray(&gotdst, &gotcarry, x, y)
				if do|co == ^uint16(0) {
					if tries > 100 {
						t.Fatal("repeatedly failing")
					}
					seed++
					tries++
					goto try
				}
				var dstmask, dstcarry [1024]uint64
				expdo, expco := addMaskMaskToMask(&dstmask, &dstcarry, &xmask, &ymask)
				if uint32(expdo) != expdo || uint32(expco) != expco {
					t.Errorf("expected %d/%d out but got %d/%d out", expdo, expco, do, co)
				}
				gotdstmask, gotcarrymask := splatArray(gotdst[:do]...), splatArray(gotcarry[:co]...)
				dstfail := diffMask(t, "lower bit", &dstmask, &gotdstmask)
				carryfail := diffMask(t, "carry bit", &dstcarry, &gotcarrymask)
				if dstfail || carryfail {
					t.Log(x, y)
				}
			})
		}
	})

	t.Run("addArrayArrayArrayToArray", func(t *testing.T) {
		t.Parallel()

		var src rand.PCGSource
		src.Seed(2)
		seedup := make(map[uint64]struct{})

		for i := 0; i < testCount; i++ {
		genSeed:
			seed := src.Uint64()
			if _, dup := seedup[seed]; dup {
				goto genSeed
			}
			seedup[seed] = struct{}{}
			t.Run(fmt.Sprint(seed), func(t *testing.T) {
				t.Parallel()

				tries := 0
				var xmask, ymask, zmask [1024]uint64
				var xarr, yarr, zarr [4096]uint16
				var gotdst, gotcarry [4096]uint16
			try:
				x := randomArray(&xarr, &xmask, seed)
				y := randomArray(&yarr, &ymask, 3*seed)
				z := randomArray(&zarr, &zmask, 5*seed)
				do, co := addArrayArrayArrayToArray(&gotdst, &gotcarry, x, y, z)
				if do|co == ^uint16(0) {
					if tries > 100 {
						t.Fatal("repeatedly failing")
					}
					seed++
					tries++
					goto try
				}
				var dstmask, dstcarry [1024]uint64
				expdo, expco := addMaskMaskMaskToMask(&dstmask, &dstcarry, &xmask, &ymask, &zmask)
				if uint32(expdo) != expdo || uint32(expco) != expco {
					t.Errorf("expected %d/%d out but got %d/%d out", expdo, expco, do, co)
				}
				gotdstmask, gotcarrymask := splatArray(gotdst[:do]...), splatArray(gotcarry[:co]...)
				dstfail := diffMask(t, "lower bit", &dstmask, &gotdstmask)
				carryfail := diffMask(t, "carry bit", &dstcarry, &gotcarrymask)
				if dstfail || carryfail {
					t.Log(x, y, z)
				}
			})
		}
	})

	t.Run("addArrayArrayArrayToMask", func(t *testing.T) {
		t.Parallel()

		var src rand.PCGSource
		src.Seed(3)
		seedup := make(map[uint64]struct{})

		for i := 0; i < testCount; i++ {
		genSeed:
			seed := src.Uint64()
			if _, dup := seedup[seed]; dup {
				goto genSeed
			}
			seedup[seed] = struct{}{}
			t.Run(fmt.Sprint(seed), func(t *testing.T) {
				t.Parallel()

				var xmask, ymask, zmask [1024]uint64
				var xarr, yarr, zarr [4096]uint16
				x := randomArray(&xarr, &xmask, seed)
				y := randomArray(&yarr, &ymask, 3*seed)
				z := randomArray(&zarr, &zmask, 5*seed)
				var gotdst, gotcarry [1024]uint64
				do, co := addArrayArrayArrayToMask(&gotdst, &gotcarry, x, y, z)
				var dstmask, dstcarry [1024]uint64
				expdo, expco := addMaskMaskMaskToMask(&dstmask, &dstcarry, &xmask, &ymask, &zmask)
				if uint32(expdo) != expdo || uint32(expco) != expco {
					t.Errorf("expected %d/%d out but got %d/%d out", expdo, expco, do, co)
				}
				dstfail := diffMask(t, "lower bit", &dstmask, &gotdst)
				carryfail := diffMask(t, "carry bit", &dstcarry, &gotcarry)
				if dstfail || carryfail {
					t.Log(x, y, z)
				}
			})
		}
	})

	t.Run("addArrayArrayMaskToMask", func(t *testing.T) {
		t.Parallel()

		var src rand.PCGSource
		src.Seed(4)
		seedup := make(map[uint64]struct{})

		for i := 0; i < testCount; i++ {
		genSeed:
			seed := src.Uint64()
			if _, dup := seedup[seed]; dup {
				goto genSeed
			}
			seedup[seed] = struct{}{}
			t.Run(fmt.Sprint(seed), func(t *testing.T) {
				t.Parallel()

				var xmask, ymask, z [1024]uint64
				var xarr, yarr [4096]uint16
				x := randomArray(&xarr, &xmask, seed)
				y := randomArray(&yarr, &ymask, 3*seed)
				zc := randomMask(&z, 5*seed)
				var gotdst, gotcarry [1024]uint64
				do, co := addArrayArrayMaskToMask(&gotdst, &gotcarry, x, y, &z, zc)
				var dstmask, dstcarry [1024]uint64
				expdo, expco := addMaskMaskMaskToMask(&dstmask, &dstcarry, &xmask, &ymask, &z)
				if uint32(expdo) != expdo || uint32(expco) != expco {
					t.Errorf("expected %d/%d out but got %d/%d out", expdo, expco, do, co)
				}
				dstfail := diffMask(t, "lower bit", &dstmask, &gotdst)
				carryfail := diffMask(t, "carry bit", &dstcarry, &gotcarry)
				if dstfail || carryfail {
					t.Log(x, y, z)
				}
			})
		}
	})

	t.Run("addArrayMaskMaskToMask", func(t *testing.T) {
		t.Parallel()

		var src rand.PCGSource
		src.Seed(5)
		seedup := make(map[uint64]struct{})

		for i := 0; i < testCount; i++ {
		genSeed:
			seed := src.Uint64()
			if _, dup := seedup[seed]; dup {
				goto genSeed
			}
			seedup[seed] = struct{}{}
			t.Run(fmt.Sprint(seed), func(t *testing.T) {
				t.Parallel()

				var xmask, y, z [1024]uint64
				var xarr [4096]uint16
				x := randomArray(&xarr, &xmask, seed)
				randomMask(&y, 3*seed)
				randomMask(&z, 5*seed)
				var gotdst, gotcarry [1024]uint64
				do, co := addArrayMaskMaskToMask(&gotdst, &gotcarry, x, &y, &z)
				var dstmask, dstcarry [1024]uint64
				expdo, expco := addMaskMaskMaskToMask(&dstmask, &dstcarry, &xmask, &y, &z)
				if uint32(expdo) != expdo || uint32(expco) != expco {
					t.Errorf("expected %d/%d out but got %d/%d out", expdo, expco, do, co)
				}
				dstfail := diffMask(t, "lower bit", &dstmask, &gotdst)
				carryfail := diffMask(t, "carry bit", &dstcarry, &gotcarry)
				if dstfail || carryfail {
					t.Log(x, y, z)
				}
			})
		}
	})
}

// TestAdd randomly tests addition logic.
func TestAdd(t *testing.T) {
	t.Parallel()

	var src rand.PCGSource
	src.Seed(8)
	seedup := make(map[uint64]struct{})

	for i := 0; i < 20; i++ {
	genSeed:
		seed := src.Uint64()
		if _, dup := seedup[seed]; dup {
			goto genSeed
		}
		seedup[seed] = struct{}{}
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			t.Parallel()

			var pcg rand.PCGSource
			pcg.Seed(seed)
			rnd := rand.New(&pcg)
			numzipf := rand.NewZipf(rnd, 1.5, 2, 1<<20)
			countzipf := rand.NewZipf(rnd, 1.3, 7, 1<<44)

			var x, y []*Bitmap
			xvals, yvals := map[uint64]uint64{}, map[uint64]uint64{}
			for n := numzipf.Uint64(); n > 0; n-- {
				idx := pcg.Uint64() % (1 << 20)
				xvals[idx] = countzipf.Uint64()
				yvals[idx] = countzipf.Uint64()
			}
			for xn := numzipf.Uint64(); uint64(len(xvals)) < xn; {
				xvals[pcg.Uint64()%(1<<20)] = countzipf.Uint64()
			}
			for yn := numzipf.Uint64(); uint64(len(yvals)) < yn; {
				yvals[pcg.Uint64()%(1<<20)] = countzipf.Uint64()
			}

			expectSums := map[uint64]uint64{}
			for k, v := range xvals {
				if v == 0 {
					delete(xvals, k)
					continue
				}
				expectSums[k] = v
				for v != 0 {
					i := bits.TrailingZeros64(v)
					v &^= 1 << i
					for len(x) <= i {
						x = append(x, NewBitmap())
					}
					x[i].DirectAdd(k)
				}
			}
			for k, v := range yvals {
				if v == 0 {
					delete(xvals, k)
					continue
				}
				expectSums[k] += v
				for v != 0 {
					i := bits.TrailingZeros64(v)
					v &^= 1 << i
					for len(y) <= i {
						y = append(y, NewBitmap())
					}
					y[i].DirectAdd(k)
				}
			}

			sumsBSI := Add(x, y)
			gotSums := make(map[uint64]uint64, len(expectSums))
			for i, b := range sumsBSI {
				mask := uint64(1) << i
				for _, k := range b.Slice() {
					gotSums[k] |= mask
				}
			}
			for k, v := range gotSums {
				if _, ok := expectSums[k]; !ok {
					t.Errorf("unexpected sum of %d for %d", v, k)
				}
			}
			for k, v := range expectSums {
				got, ok := gotSums[k]
				if !ok {
					t.Errorf("missing sum of %d for %d", v, k)
					continue
				}
				if got != v {
					t.Errorf("sum for %d differs: expected %d but got %d", k, v, got)
				}
			}
		})
	}
}
