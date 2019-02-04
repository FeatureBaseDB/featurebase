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

package roaring

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

// String produces a human viewable string of the contents.
func (iv interval16) String() string {
	return fmt.Sprintf("[%d, %d]", iv.start, iv.last)
}

func (c *Container) String() string {
	return fmt.Sprintf("<%s container  n=%d, array[%d], runs[%d], bitmap[%d]> type:%d", c.info().Type, c.n, len(c.array), len(c.runs), len(c.bitmap), c.containerType)
}

func TestRunAppendInterval(t *testing.T) {
	a := Container{containerType: containerRun}
	tests := []struct {
		base []interval16
		app  interval16
		exp  int32
	}{
		{
			base: []interval16{},
			app:  interval16{start: 22, last: 25},
			exp:  4,
		},
		{
			base: []interval16{{start: 20, last: 23}},
			app:  interval16{start: 22, last: 25},
			exp:  2,
		},
		{
			base: []interval16{{start: 20, last: 23}},
			app:  interval16{start: 21, last: 22},
			exp:  0,
		},
		{
			base: []interval16{{start: 20, last: 23}},
			app:  interval16{start: 19, last: 25},
			exp:  2, // runAppendInterval explicitly does not support intervals whose start is < c.runs[-1].start
		},
	}

	for i, test := range tests {
		a.runs = test.base
		if n := a.runAppendInterval(test.app); n != test.exp {
			t.Fatalf("test #%v expected %v, but got %v", i, test.exp, n)
		}
	}

}

func TestInterval16RunLen(t *testing.T) {
	iv := interval16{start: 7, last: 9}
	if iv.runlen() != 3 {
		t.Fatalf("should be 3")
	}
	iv = interval16{start: 7, last: 7}
	if iv.runlen() != 1 {
		t.Fatalf("should be 1")
	}
}

func TestContainerRunAdd(t *testing.T) {
	c := Container{runs: make([]interval16, 0), containerType: containerRun}
	tests := []struct {
		op  uint16
		exp []interval16
	}{
		{1, []interval16{{start: 1, last: 1}}},
		{2, []interval16{{start: 1, last: 2}}},
		{4, []interval16{{start: 1, last: 2}, {start: 4, last: 4}}},
		{3, []interval16{{start: 1, last: 4}}},
		{10, []interval16{{start: 1, last: 4}, {start: 10, last: 10}}},
		{7, []interval16{{start: 1, last: 4}, {start: 7, last: 7}, {start: 10, last: 10}}},
		{6, []interval16{{start: 1, last: 4}, {start: 6, last: 7}, {start: 10, last: 10}}},
		{0, []interval16{{start: 0, last: 4}, {start: 6, last: 7}, {start: 10, last: 10}}},
		{8, []interval16{{start: 0, last: 4}, {start: 6, last: 8}, {start: 10, last: 10}}},
	}
	for _, test := range tests {
		c.mapped = true
		ret := c.add(test.op)
		if !ret {
			t.Fatalf("result of adding new bit should be true: %v", c.runs)
		}
		if !reflect.DeepEqual(c.runs, test.exp) {
			t.Fatalf("Should have %v, but got %v after adding %v", test.exp, c.runs, test.op)
		}
		if c.mapped {
			t.Fatalf("container should not be mapped after adding bit %v", test.op)
		}
	}
}

func TestContainerRunAdd2(t *testing.T) {
	c := Container{runs: make([]interval16, 0), containerType: containerRun}
	ret := c.add(0)
	if !ret {
		t.Fatalf("result of adding new bit should be true: %v", c.runs)
	}
	if !reflect.DeepEqual(c.runs, []interval16{{start: 0, last: 0}}) {
		t.Fatalf("should have 1 run of length 1, but have %v", c.runs)
	}
	ret = c.add(0)
	if ret {
		t.Fatalf("result of adding existing bit should be false: %v", c.runs)
	}
}

func TestRunCountRange(t *testing.T) {
	c := Container{runs: make([]interval16, 0), containerType: containerRun}
	cnt := c.runCountRange(2, 9)
	if cnt != 0 {
		t.Fatalf("should get 0 from empty container, but got: %v", cnt)
	}
	c.add(5)
	c.add(6)
	c.add(7)

	cnt = c.runCountRange(2, 9)
	if cnt != 3 {
		t.Fatalf("should get 3 from interval within range, but got: %v", cnt)
	}

	c.add(8)
	c.add(9)
	c.add(10)
	c.add(11)

	cnt = c.runCountRange(6, 8)
	if cnt != 2 {
		t.Fatalf("should get 2 from range within interval, but got: %v", cnt)
	}

	cnt = c.runCountRange(3, 9)
	if cnt != 4 {
		t.Fatalf("should get 4 from range overlaps front of interval, but got: %v", cnt)
	}

	cnt = c.runCountRange(9, 14)
	if cnt != 3 {
		t.Fatalf("should get 3 from range overlaps back of interval, but got: %v", cnt)
	}

	c.add(17)
	c.add(19)
	c.add(18)

	cnt = c.runCountRange(1, 22)
	if cnt != 10 {
		t.Fatalf("should get 10 from multiple ranges in interval, but got: %v", cnt)
	}

	c.add(13)
	c.add(14)

	cnt = c.runCountRange(6, 18)
	if cnt != 9 {
		t.Fatalf("should get 9 from multiple ranges overlapping both sides, but got: %v", cnt)
	}
	// verify that the disparate ops resulted in three separate runs
	cnt = c.countRuns()
	if cnt != 3 {
		t.Fatalf("should get 3 total runs, but got: %v [%v]", cnt, c.runs)
	}
}

func TestRunContains(t *testing.T) {
	c := Container{runs: make([]interval16, 0), containerType: containerRun}
	if c.runContains(5) {
		t.Fatalf("empty run container should not contain 5")
	}
	c.add(5)
	if !c.runContains(5) {
		t.Fatalf("run container with 5 should contain 5")
	}

	c.add(6)
	c.add(7)

	c.add(9)
	c.add(10)
	c.add(11)

	if !c.runContains(10) {
		t.Fatalf("run container with 10 in second run should contain 10")
	}
}

func TestBitmapCountRange(t *testing.T) {
	c := Container{containerType: containerBitmap}
	tests := []struct {
		start  int32
		end    int32
		bitmap []uint64
		exp    int32
	}{
		{start: 0, end: 1, bitmap: []uint64{1}, exp: 1},
		{start: 2, end: 7, bitmap: []uint64{0xFFFFFFFFFFFFFF18}, exp: 2},
		{start: 67, end: 68, bitmap: []uint64{0, 0x8}, exp: 1},
		{start: 1, end: 68, bitmap: []uint64{0x3, 0x8, 0xF}, exp: 2},
		{start: 1, end: 258, bitmap: []uint64{0xF, 0x8, 0xA, 0x4, 0xFFFFFFFFFFFFFFFF}, exp: 9},
		{start: 66, end: 71, bitmap: []uint64{0xF, 0xFFFFFFFFFFFFFF18}, exp: 2},
		{start: 63, end: 64, bitmap: []uint64{0x8000000000000000}, exp: 1},
	}

	for i, test := range tests {
		c.bitmap = test.bitmap
		if ret := c.bitmapCountRange(test.start, test.end); ret != test.exp {
			t.Fatalf("test #%v count of %v from %v to %v should be %v but got %v", i, test.bitmap, test.start, test.end, test.exp, ret)
		}
	}
}

func TestIntersectionCountArrayBitmap3(t *testing.T) {
	a, b := &Container{}, &Container{}
	a.containerType = containerBitmap
	a.bitmap = getFullBitmap()
	a.n = maxContainerVal + 1

	b.containerType = containerBitmap
	b.bitmap = getFullBitmap()
	b.n = maxContainerVal + 1
	res := intersectBitmapBitmap(a, b)
	if res.n != res.count() || res.n != maxContainerVal+1 {
		t.Fatalf("test #1 intersectCountBitmapBitmap fail orig: %v new: %v exp: %v", res.n, res.count(), maxContainerVal+1)
	}

	a.bitmapToRun()
	res = intersectBitmapRun(b, a)
	if res.n != res.count() || res.n != maxContainerVal+1 {
		t.Fatalf("test #2 intersectCountBitmapRun fail orig: %v new: %v exp: %v", res.n, res.count(), maxContainerVal+1)
	}
	b.bitmapToRun()
	res = intersectRunRun(a, b)
	n := intersectionCountRunRun(a, b)
	if res.n != res.count() || res.n != maxContainerVal+1 || res.n != int32(n) {
		t.Fatalf("test #3 intersectCountRunRun fail orig: %v new: %v exp: %v", res.n, res.count(), maxContainerVal+1)
	}
}

func TestIntersectionCountArrayBitmap2(t *testing.T) {
	a, b := &Container{}, &Container{}
	tests := []struct {
		array  []uint16
		bitmap []uint64
		exp    int32
	}{
		{
			array:  []uint16{0},
			bitmap: []uint64{1},
			exp:    1,
		},
		{
			array:  []uint16{0, 1},
			bitmap: []uint64{3},
			exp:    2,
		},
		{
			array:  []uint16{64, 128, 129, 2000},
			bitmap: []uint64{932421, 2},
			exp:    0,
		},
		{
			array:  []uint16{0, 65, 130, 195},
			bitmap: []uint64{255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255},
			exp:    4,
		},
		{
			array:  []uint16{63, 120, 543, 639, 12000},
			bitmap: []uint64{0x8000000000000000, 0, 0, 0, 0, 0, 0, 0, 0, 0x8000000000000000},
			exp:    2,
		},
	}

	for i, test := range tests {
		a.array = test.array
		a.containerType = containerArray
		b.bitmap = test.bitmap
		b.containerType = containerBitmap
		ret := intersectionCountArrayBitmap(a, b)
		if ret != test.exp {
			t.Fatalf("test #%v intersectCountArrayBitmap fail received: %v exp: %v", i, ret, test.exp)
		}
	}
}

func TestRunRemove(t *testing.T) {
	c := Container{runs: []interval16{{start: 2, last: 10}, {start: 12, last: 13}, {start: 15, last: 16}}, containerType: containerRun}
	tests := []struct {
		op     uint16
		exp    []interval16
		expRet bool
	}{
		{2, []interval16{{start: 3, last: 10}, {start: 12, last: 13}, {start: 15, last: 16}}, true},
		{10, []interval16{{start: 3, last: 9}, {start: 12, last: 13}, {start: 15, last: 16}}, true},
		{12, []interval16{{start: 3, last: 9}, {start: 13, last: 13}, {start: 15, last: 16}}, true},
		{13, []interval16{{start: 3, last: 9}, {start: 15, last: 16}}, true},
		{16, []interval16{{start: 3, last: 9}, {start: 15, last: 15}}, true},
		{6, []interval16{{start: 3, last: 5}, {start: 7, last: 9}, {start: 15, last: 15}}, true},
		{8, []interval16{{start: 3, last: 5}, {start: 7, last: 7}, {start: 9, last: 9}, {start: 15, last: 15}}, true},
		{8, []interval16{{start: 3, last: 5}, {start: 7, last: 7}, {start: 9, last: 9}, {start: 15, last: 15}}, false},
		{1, []interval16{{start: 3, last: 5}, {start: 7, last: 7}, {start: 9, last: 9}, {start: 15, last: 15}}, false},
		{44, []interval16{{start: 3, last: 5}, {start: 7, last: 7}, {start: 9, last: 9}, {start: 15, last: 15}}, false},
	}

	for i, test := range tests {
		c.mapped = true
		ret := c.remove(test.op)
		if ret != test.expRet || !reflect.DeepEqual(c.runs, test.exp) {
			t.Fatalf("test #%v Unexpected result removing %v from runs. Expected %v, got %v. Expected %v, got %v", i, test.op, test.expRet, ret, test.exp, c.runs)
		}
		if ret && c.mapped {
			t.Fatalf("test #%v container was not unmapped although bit %v was removed", i, test.op)
		}
		if !ret && !c.mapped {
			t.Fatalf("test #%v container was unmapped although bit %v was not removed", i, test.op)
		}
	}
}

func TestRunMax(t *testing.T) {
	c := Container{runs: []interval16{{start: 2, last: 10}, {start: 12, last: 13}, {start: 15, last: 16}}, containerType: containerRun}
	max := c.max()
	if max != 16 {
		t.Fatalf("max for %v should be 16", c.runs)
	}

	c = Container{runs: []interval16{}}
	max = c.max()
	if max != 0 {
		t.Fatalf("max for %v should be 0", c.runs)
	}
}

func TestIntersectionCountArrayRun(t *testing.T) {
	a := &Container{containerType: containerArray, array: []uint16{1, 5, 10, 11, 12}}
	b := &Container{containerType: containerRun, runs: []interval16{{start: 2, last: 10}, {start: 12, last: 13}, {start: 15, last: 16}}}

	ret := intersectionCountArrayRun(a, b)
	if ret != 3 {
		t.Fatalf("count of %v with %v should be 3, but got %v", a.array, b.runs, ret)
	}
}

func TestIntersectionCountBitmapRun(t *testing.T) {
	a := &Container{containerType: containerBitmap, bitmap: []uint64{0x8000000000000000}}
	b := &Container{containerType: containerRun, runs: []interval16{{start: 63, last: 64}}}

	ret := intersectionCountBitmapRun(a, b)
	if ret != 1 {
		t.Fatalf("count of %v with %v should be 1, but got %v", a.bitmap, b.runs, ret)
	}

	a = &Container{containerType: containerBitmap, bitmap: []uint64{0xF0000001, 0xFF00000000000000, 0xFF000000000000F0, 0x0F0000}}
	b = &Container{containerType: containerRun, runs: []interval16{{start: 29, last: 31}, {start: 125, last: 134}, {start: 191, last: 197}, {start: 200, last: 300}}}

	ret = intersectionCountBitmapRun(a, b)
	if ret != 14 {
		t.Fatalf("count of %v with %v should be 14, but got %v", a.bitmap, b.runs, ret)
	}
}

func TestIntersectionCountRunRun(t *testing.T) {
	a := &Container{}
	b := &Container{}
	tests := []struct {
		aruns []interval16
		bruns []interval16
		exp   int32
	}{
		{
			aruns: []interval16{},
			bruns: []interval16{{start: 3, last: 8}}, exp: 0},
		{
			aruns: []interval16{{start: 2, last: 10}},
			bruns: []interval16{{start: 3, last: 8}}, exp: 6},
		{
			aruns: []interval16{{start: 2, last: 10}},
			bruns: []interval16{{start: 1, last: 11}}, exp: 9},
		{
			aruns: []interval16{{start: 2, last: 10}},
			bruns: []interval16{{start: 0, last: 2}}, exp: 1},
		{
			aruns: []interval16{{start: 2, last: 10}},
			bruns: []interval16{{start: 1, last: 10}}, exp: 9},
		{
			aruns: []interval16{{start: 2, last: 10}},
			bruns: []interval16{{start: 5, last: 12}}, exp: 6},
		{
			aruns: []interval16{{start: 2, last: 10}},
			bruns: []interval16{{start: 10, last: 99}}, exp: 1},
		{
			aruns: []interval16{{start: 2, last: 10}, {start: 44, last: 99}},
			bruns: []interval16{{start: 12, last: 14}}, exp: 0},
		{
			aruns: []interval16{{start: 2, last: 10}, {start: 12, last: 13}},
			bruns: []interval16{{start: 2, last: 10}, {start: 12, last: 13}}, exp: 11},
		{
			aruns: []interval16{{start: 8, last: 12}, {start: 15, last: 19}},
			bruns: []interval16{{start: 9, last: 9}, {start: 11, last: 17}}, exp: 6},
	}
	for i, test := range tests {
		a.containerType = containerRun
		b.containerType = containerRun
		a.runs = test.aruns
		b.runs = test.bruns
		ret := intersectionCountRunRun(a, b)
		if ret != test.exp {
			t.Fatalf("test #%v failed intersecting %v with %v should be %v, but got %v", i, test.aruns, test.bruns, test.exp, ret)
		}
	}
}

func TestIntersectArrayRun(t *testing.T) {
	a := &Container{}
	b := &Container{}
	tests := []struct {
		array []uint16
		runs  []interval16
		exp   []uint16
	}{
		{
			array: []uint16{1, 4, 5, 7, 10, 11, 12},
			runs:  []interval16{{start: 5, last: 10}},
			exp:   []uint16{5, 7, 10},
		},
		{
			array: []uint16{},
			runs:  []interval16{{start: 5, last: 10}},
			exp:   []uint16(nil),
		},
		{
			array: []uint16{1, 4, 5, 7, 10, 11, 12},
			runs:  []interval16{},
			exp:   []uint16(nil),
		},
		{
			array: []uint16{0, 1, 4, 5, 7, 10, 11, 12},
			runs:  []interval16{{start: 0, last: 5}, {start: 7, last: 7}},
			exp:   []uint16{0, 1, 4, 5, 7},
		},
	}

	for i, test := range tests {
		a.containerType = containerArray
		b.containerType = containerRun
		a.array = test.array
		b.runs = test.runs
		ret := intersectArrayRun(a, b)
		if !reflect.DeepEqual(ret.array, test.exp) {
			t.Fatalf("test #%v expected %v, but got %v", i, test.exp, ret.array)
		}
	}
}

func TestIntersectRunRun(t *testing.T) {
	a := &Container{}
	b := &Container{}
	tests := []struct {
		aruns []interval16
		bruns []interval16
		exp   []interval16
		expN  int32
	}{
		{
			aruns: []interval16{},
			bruns: []interval16{{start: 5, last: 10}},
			exp:   []interval16(nil),
			expN:  0,
		},
		{
			aruns: []interval16{{start: 5, last: 12}},
			bruns: []interval16{{start: 5, last: 10}},
			exp:   []interval16{{start: 5, last: 10}},
			expN:  6,
		},
		{
			aruns: []interval16{{start: 1, last: 3}, {start: 5, last: 5}, {start: 7, last: 8}, {start: 9, last: 12}},
			bruns: []interval16{{start: 5, last: 10}},
			exp:   []interval16{{start: 5, last: 5}, {start: 7, last: 10}},
			expN:  5,
		},
		{
			aruns: []interval16{{start: 20, last: 30}},
			bruns: []interval16{{start: 5, last: 10}, {start: 19, last: 21}},
			exp:   []interval16{{start: 20, last: 21}},
			expN:  2,
		},
		{
			aruns: []interval16{{start: 5, last: 10}},
			bruns: []interval16{{start: 7, last: 12}},
			exp:   []interval16{{start: 7, last: 10}},
			expN:  4,
		},
		{
			aruns: []interval16{{start: 5, last: 12}},
			bruns: []interval16{{start: 7, last: 10}},
			exp:   []interval16{{start: 7, last: 10}},
			expN:  4,
		},
	}
	for i, test := range tests {
		a.containerType = containerRun
		b.containerType = containerRun
		a.runs = test.aruns
		b.runs = test.bruns
		ret := intersectRunRun(a, b)
		if ret.n != test.expN {
			t.Fatalf("test #%v expected n to be %v, but got %v", i, test.expN, ret.n)
		}
		if !reflect.DeepEqual(ret.runs, test.exp) {
			t.Fatalf("test #%v expected %v, but got %v", i, test.exp, ret.runs)
		}
	}

}

func TestIntersectBitmapRunBitmap(t *testing.T) {
	a := &Container{bitmap: make([]uint64, bitmapN)}
	b := &Container{}
	tests := []struct {
		bitmap []uint64
		runs   []interval16
		exp    []uint64
		expN   int32
	}{
		{
			bitmap: []uint64{1},
			runs:   []interval16{{start: 0, last: 0}, {start: 2, last: 5}, {start: 62, last: 71}, {start: 77, last: 4096}},
			exp:    []uint64{1},
			expN:   1,
		},
		{
			bitmap: []uint64{0xFFFFFFFFFFFFFFFF},
			runs:   []interval16{{start: 1, last: 1}},
			exp:    []uint64{2},
			expN:   1,
		},
		{
			bitmap: []uint64{0xFFFFFFFFFFFFFFFF},
			runs:   []interval16{{start: 1, last: 1}, {start: 10, last: 12}, {start: 61, last: 77}},
			exp:    []uint64{0xe000000000001C02},
			expN:   7,
		},
		{
			bitmap: []uint64{0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF},
			runs:   []interval16{{start: 1, last: 1}, {start: 61, last: 77}},
			exp:    []uint64{0xE000000000000002, 0x00000000000003FFF},
			expN:   18,
		},
		{
			bitmap: []uint64{0xFFFFFFFFFFFFFFFF, 1, 1, 1, 0xA, 1, 1, 0, 1},
			runs:   []interval16{{start: 63, last: 10000}},
			exp:    []uint64{0x8000000000000000, 1, 1, 1, 0xA, 1, 1, 0, 1},
			expN:   9,
		},
	}
	for i, test := range tests {
		copy(a.bitmap, test.bitmap)
		b.runs = test.runs
		b.n = 4097 // ;)
		exp := make([]uint64, bitmapN)
		copy(exp, test.exp)
		a.containerType = containerBitmap
		b.containerType = containerRun
		ret := intersectBitmapRun(a, b)
		if ret.isArray() {
			ret.arrayToBitmap()
		}
		if !reflect.DeepEqual(ret.bitmap, exp) {
			t.Fatalf("test #%v expected %v, but got %v", i, exp, ret.bitmap)
		}
		if ret.n != test.expN {
			t.Fatalf("test #%v expected n to be %v, but got %v", i, test.expN, ret.n)
		}
	}

}

func TestIntersectBitmapRunArray(t *testing.T) {
	a := &Container{bitmap: make([]uint64, bitmapN)}
	b := &Container{}
	tests := []struct {
		bitmap []uint64
		runs   []interval16
		exp    []uint16
		expN   int32
	}{
		{
			bitmap: []uint64{1},
			runs:   []interval16{{start: 0, last: 0}, {start: 2, last: 5}, {start: 62, last: 71}, {start: 77, last: 4096}},
			exp:    []uint16{0},
			expN:   1,
		},
		{
			bitmap: []uint64{0xFFFFFFFFFFFFFFFF},
			runs:   []interval16{{start: 1, last: 1}},
			exp:    []uint16{1},
			expN:   1,
		},
		{
			bitmap: []uint64{0xFFFFFFFFFFFFFFFF},
			runs:   []interval16{{start: 1, last: 1}, {start: 10, last: 12}, {start: 61, last: 77}},
			exp:    []uint16{1, 10, 11, 12, 61, 62, 63},
			expN:   7,
		},
		{
			bitmap: []uint64{0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF},
			runs:   []interval16{{start: 1, last: 1}, {start: 61, last: 68}},
			exp:    []uint16{1, 61, 62, 63, 64, 65, 66, 67, 68},
			expN:   9,
		},
		{
			bitmap: []uint64{0xFFFFFFFFFFFFFFFF, 1, 1, 1, 0xA, 1, 1, 0, 1},
			runs:   []interval16{{start: 63, last: 10000}, {start: 65000, last: 65535}},
			exp:    []uint16{63, 64, 128, 192, 257, 259, 320, 384, 512},
			expN:   9,
		},
	}
	for i, test := range tests {
		copy(a.bitmap, test.bitmap)
		b.runs = test.runs
		a.containerType = containerBitmap
		b.containerType = containerRun
		ret := intersectBitmapRun(a, b)
		if !reflect.DeepEqual(ret.array, test.exp) {
			t.Fatalf("test #%v expected %v, but got %v", i, test.exp, ret.array)
		}
		if ret.n != test.expN {
			t.Fatalf("test #%v expected n to be %v, but got %v", i, test.expN, ret.n)
		}
	}

}

func TestUnionMixed(t *testing.T) {

	// array container
	a := &Container{}
	a.array = []uint16{1, 4, 5, 7, 10, 11, 12}
	a.containerType = containerArray
	a.n = 7

	// bitmap container
	b := &Container{bitmap: make([]uint64, bitmapN)}
	b.bitmap[0] = uint64(0x3)
	b.n = 2
	b.containerType = containerBitmap

	// run container
	r := &Container{}
	r.runs = []interval16{{start: 5, last: 10}}
	r.containerType = containerRun
	r.n = 6

	t.Run("various container Unions", func(t *testing.T) {
		tests := []struct {
			name string
			c1   *Container
			c2   *Container
			exp  []uint16
		}{
			{name: "run-array", c1: r, c2: a, exp: []uint16{1, 4, 5, 6, 7, 8, 9, 10, 11, 12}},
			{name: "array-run", c1: a, c2: r, exp: []uint16{1, 4, 5, 6, 7, 8, 9, 10, 11, 12}},
			{name: "run-run", c1: r, c2: r, exp: []uint16{5, 6, 7, 8, 9, 10}},

			{name: "bitmap-run", c1: b, c2: r, exp: []uint16{0, 1, 5, 6, 7, 8, 9, 10}},
			{name: "run-bitmap", c1: r, c2: b, exp: []uint16{0, 1, 5, 6, 7, 8, 9, 10}},
			{name: "array-bitmap", c1: a, c2: b, exp: []uint16{0, 1, 4, 5, 7, 10, 11, 12}},
			{name: "bitmap-array", c1: a, c2: b, exp: []uint16{0, 1, 4, 5, 7, 10, 11, 12}},
		}
		for _, tt := range tests {
			res := union(tt.c1, tt.c2)
			// convert to array for comparison
			if res.isBitmap() {
				res.bitmapToArray()
			} else if res.isRun() {
				res.runToArray()
			}
			if !reflect.DeepEqual(res.array, tt.exp) {
				t.Fatalf("test %s expected %v, but got %v", tt.name, tt.exp, res.array)
			}
		}
	})
}

func TestIntersectMixed(t *testing.T) {
	a := &Container{}
	b := &Container{}
	c := &Container{}

	a.runs = []interval16{{start: 5, last: 10}}
	a.n = 6
	a.containerType = containerRun
	b.array = []uint16{1, 4, 5, 7, 10, 11, 12}
	b.n = 7
	b.containerType = containerArray
	res := intersect(a, b)
	if !reflect.DeepEqual(res.array, []uint16{5, 7, 10}) {
		t.Fatalf("test #1 expected %v, but got %v", []uint16{5, 7, 10}, res.array)
	}
	res = intersect(b, a)
	if !reflect.DeepEqual(res.array, []uint16{5, 7, 10}) {
		t.Fatalf("test #1 expected %v, but got %v", []uint16{5, 7, 10}, res.array)
	}

	res = intersect(a, a)
	if !reflect.DeepEqual(res.runs, []interval16{{start: 5, last: 10}}) {
		t.Fatalf("test #3 expected %v, but got %v", []interval16{{start: 5, last: 10}}, res.runs)
	}
	c.bitmap = []uint64{0x60}
	c.n = 2
	c.containerType = containerBitmap

	res = intersect(c, a)
	if !reflect.DeepEqual(res.array, []uint16{5, 6}) {
		t.Fatalf("test #4 expected %v, but got %v", []uint16{6}, res.array)
	}

	res = intersect(a, c)
	if !reflect.DeepEqual(res.array, []uint16{5, 6}) {
		t.Fatalf("test #5 expected %v, but got %v", []uint16{6}, res.array)
	}

	res = intersect(b, c)
	if !reflect.DeepEqual(res.array, []uint16{5}) {
		t.Fatalf("test #6 expected %v, but got %v", []uint16{5}, res.array)
	}
	res = intersect(c, b)
	if !reflect.DeepEqual(res.array, []uint16{5}) {
		t.Fatalf("test #7 expected %v, but got %v", []uint16{5}, res.array)
	}

}
func TestDifferenceMixed(t *testing.T) {
	a := &Container{}
	b := &Container{}
	c := &Container{}
	d := &Container{}

	a.runs = []interval16{{start: 5, last: 10}}
	a.n = a.runCountRange(0, 100)
	a.containerType = containerRun

	b.array = []uint16{0, 2, 4, 6, 8, 10, 12}
	b.n = int32(len(b.array))
	b.containerType = containerArray

	d.array = []uint16{1, 3, 5, 7, 9, 11, 12}
	d.n = int32(len(d.array))
	d.containerType = containerArray

	res := difference(a, b)

	if !reflect.DeepEqual(res.array, []uint16{5, 7, 9}) {
		t.Fatalf("test #1 expected %v, but got %#v", []uint16{5, 7, 9}, res)
	}

	res = difference(b, a)
	if !reflect.DeepEqual(res.array, []uint16{0, 2, 4, 12}) {
		t.Fatalf("test #2 expected %v, but got %v", []uint16{0, 2, 4, 12}, res.array)
	}

	res = difference(a, a)
	if !reflect.DeepEqual(res.runs, []interval16{}) {
		t.Fatalf("test #3 expected empty but got %v", res.runs)
	}

	c.bitmap = MakeBitmap([]uint64{0x64})
	c.n = c.countRange(0, 100)
	c.containerType = containerBitmap
	res = difference(c, a)
	if !reflect.DeepEqual(res.bitmap, MakeBitmap([]uint64{0x4})) {
		t.Fatalf("test #4 expected %v, but got %v", []uint16{4}, res.bitmap)
	}

	res = difference(a, c)
	if !reflect.DeepEqual(res.runs, []interval16{{start: 7, last: 10}}) {
		t.Fatalf("test #5 expected %v, but got %v", []interval16{{start: 7, last: 10}}, res.runs)
	}

	res = difference(b, c)
	if !reflect.DeepEqual(res.array, []uint16{0, 4, 8, 10, 12}) {
		t.Fatalf("test #6 expected %v, but got %v", []uint16{0, 4, 8, 10, 12}, res.array)
	}

	res = difference(c, b)
	if !reflect.DeepEqual(res.array, []uint16{5}) {
		t.Fatalf("test #7 expected %v, but got %v", []uint16{5}, res.array)
	}

	res = difference(b, b)
	if res.n != 0 {
		t.Fatalf("test #8 expected 0, but got %d", res.n)
	}

	res = difference(c, c)
	if res.n != 0 {
		t.Fatalf("test #9 expected 0, but got %d", res.n)
	}

	res = difference(d, b)
	if !reflect.DeepEqual(res.array, []uint16{1, 3, 5, 7, 9, 11}) {
		t.Fatalf("test #10 expected %v, but got %d", []uint16{1, 3, 5, 7, 9, 11}, res.array)
	}

	res = difference(b, d)
	if !reflect.DeepEqual(res.array, []uint16{0, 2, 4, 6, 8, 10}) {
		t.Fatalf("test #11 expected %v, but got %d", []uint16{0, 2, 4, 6, 8, 10}, res.array)
	}

}

func TestUnionRunRun(t *testing.T) {
	a := &Container{}
	b := &Container{}
	tests := []struct {
		aruns []interval16
		bruns []interval16
		exp   []interval16
	}{
		{
			aruns: []interval16{},
			bruns: []interval16{{start: 5, last: 10}},
			exp:   []interval16{{start: 5, last: 10}},
		},
		{
			aruns: []interval16{{start: 5, last: 12}},
			bruns: []interval16{{start: 5, last: 10}},
			exp:   []interval16{{start: 5, last: 12}},
		},
		{
			aruns: []interval16{{start: 1, last: 3}, {start: 5, last: 5}, {start: 7, last: 8}, {start: 9, last: 12}},
			bruns: []interval16{{start: 5, last: 10}},
			exp:   []interval16{{start: 1, last: 3}, {start: 5, last: 12}},
		},
		{
			aruns: []interval16{{start: 1, last: 3}, {start: 5, last: 5}, {start: 7, last: 8}, {start: 9, last: 12}},
			bruns: []interval16{{start: 2, last: 65535}},
			exp:   []interval16{{start: 1, last: 65535}},
		},
		{
			aruns: []interval16{{start: 2, last: 65535}},
			bruns: []interval16{{start: 1, last: 3}, {start: 5, last: 5}, {start: 7, last: 8}, {start: 9, last: 12}},
			exp:   []interval16{{start: 1, last: 65535}},
		},
		{
			aruns: []interval16{{start: 1, last: 3}, {start: 5, last: 5}, {start: 7, last: 8}, {start: 9, last: 12}},
			bruns: []interval16{{start: 0, last: 65535}},
			exp:   []interval16{{start: 0, last: 65535}},
		},
		{
			aruns: []interval16{{start: 0, last: 65535}},
			bruns: []interval16{{start: 1, last: 3}, {start: 5, last: 5}, {start: 7, last: 8}, {start: 9, last: 12}},
			exp:   []interval16{{start: 0, last: 65535}},
		},
		{
			aruns: []interval16{{start: 1, last: 3}, {start: 5, last: 5}, {start: 7, last: 9}, {start: 12, last: 22}},
			bruns: []interval16{{start: 2, last: 8}, {start: 16, last: 27}, {start: 33, last: 34}},
			exp:   []interval16{{start: 1, last: 9}, {start: 12, last: 27}, {start: 33, last: 34}},
		},
	}
	for i, test := range tests {
		a.runs = test.aruns
		b.runs = test.bruns
		a.containerType = containerRun
		b.containerType = containerRun
		ret := unionRunRun(a, b)
		if !reflect.DeepEqual(ret.runs, test.exp) {
			t.Fatalf("test #%v expected %v, but got %v", i, test.exp, ret.runs)
		}
	}
}

func TestUnionArrayRun(t *testing.T) {
	a := &Container{}
	b := &Container{}
	tests := []struct {
		array []uint16
		runs  []interval16
		exp   []uint16
	}{
		{
			array: []uint16{1, 4, 5, 7, 10, 11, 12},
			runs:  []interval16{{start: 5, last: 10}},
			exp:   []uint16{1, 4, 5, 6, 7, 8, 9, 10, 11, 12},
		},
		{
			array: []uint16{},
			runs:  []interval16{{start: 5, last: 10}},
			exp:   []uint16{5, 6, 7, 8, 9, 10},
		},
		{
			array: []uint16{1, 4, 5, 7, 10, 11, 12},
			runs:  []interval16{},
			exp:   []uint16{1, 4, 5, 7, 10, 11, 12},
		},
		{
			array: []uint16{0, 1, 4, 5, 7, 10, 11, 12},
			runs:  []interval16{{start: 0, last: 5}, {start: 7, last: 7}},
			exp:   []uint16{0, 1, 2, 3, 4, 5, 7, 10, 11, 12},
		},
	}

	for i, test := range tests {
		a.array = test.array
		b.runs = test.runs
		a.containerType = containerArray
		b.containerType = containerRun
		ret := unionArrayRun(a, b)
		if !reflect.DeepEqual(ret.array, test.exp) {
			t.Fatalf("test #%v expected %v, but got %v", i, test.exp, ret.array)
		}
	}
}

func TestBitmapSetRange(t *testing.T) {
	c := &Container{containerType: containerBitmap, bitmap: make([]uint64, bitmapN)}
	tests := []struct {
		bitmap []uint64
		start  uint64
		last   uint64
		exp    []uint64
		expN   int32
	}{
		{
			bitmap: []uint64{0x0000000000FFF900},
			start:  9,
			last:   10,
			exp:    []uint64{0x0000000000FFFF00},
			expN:   16,
		},
		{
			bitmap: []uint64{0xFF0, 0xFF, 0xFF},
			start:  60,
			last:   130,
			exp:    []uint64{0xF000000000000FF0, 0xFFFFFFFFFFFFFFFF, 0xFF},
			expN:   84,
		},
	}

	for i, test := range tests {
		copy(c.bitmap, test.bitmap)
		c.n = c.countRange(0, 65535)
		c.bitmapSetRange(test.start, test.last+1)
		if !reflect.DeepEqual(c.bitmap[:len(test.exp)], test.exp) {
			t.Fatalf("test %#v expected %x, got %x", i, test.exp, c.bitmap[:len(test.bitmap)])
		}
		if test.expN != c.n {
			t.Fatalf("test #%v expected n to be %v, but got %v", i, test.expN, c.n)
		}
	}
}

func TestArrayToBitmap(t *testing.T) {
	a := &Container{containerType: containerArray}
	tests := []struct {
		array []uint16
		exp   []uint64
	}{
		{
			array: []uint16{},
			exp:   []uint64{},
		},
		{
			array: []uint16{0, 1, 2, 3},
			exp:   []uint64{0xF},
		},
	}

	for i, test := range tests {
		exp := make([]uint64, bitmapN)
		copy(exp, test.exp)

		a.array = test.array
		a.n = int32(len(test.array))
		a.arrayToBitmap()
		if !reflect.DeepEqual(a.bitmap, exp) {
			t.Fatalf("test #%v expected %v, but got %v", i, exp, a.bitmap)
		}
	}
}

func TestBitmapToArray(t *testing.T) {
	a := &Container{containerType: containerBitmap}
	tests := []struct {
		bitmap []uint64
		exp    []uint16
	}{
		{
			bitmap: []uint64{},
			exp:    []uint16{},
		},
		{
			bitmap: []uint64{0xF},
			exp:    []uint16{0, 1, 2, 3},
		},
	}
	for i, test := range tests {
		a.bitmap = make([]uint64, bitmapN)
		n := int32(0)
		for i, v := range test.bitmap {
			a.bitmap[i] = v
			n += int32(popcount(v))
		}
		a.n = n

		a.bitmapToArray()
		if !reflect.DeepEqual(a.array, test.exp) {
			t.Fatalf("test #%v expected %v, but got %v", i, test.exp, a.array)
		}
	}
}

func TestRunToBitmap(t *testing.T) {
	a := &Container{containerType: containerRun}
	tests := []struct {
		runs []interval16
		exp  []uint64
	}{
		{
			runs: []interval16{},
			exp:  []uint64{},
		},
		{
			runs: []interval16{{start: 0, last: 0}},
			exp:  []uint64{1},
		},
		{
			runs: []interval16{{start: 0, last: 4}},
			exp:  []uint64{31},
		},
		{
			runs: []interval16{{start: 2, last: 2}, {start: 5, last: 7}, {start: 13, last: 14}, {start: 17, last: 17}},
			exp:  []uint64{155876},
		},
		{
			runs: []interval16{{start: 0, last: 3}, {start: 60, last: 67}},
			exp:  []uint64{0xF00000000000000F, 0x000000000000000F},
		},
	}

	for i, test := range tests {
		exp := make([]uint64, bitmapN)
		n := 0
		for i, v := range test.exp {
			exp[i] = v
			n += int(popcount(v))
		}

		a.runs = test.runs
		a.n = int32(n)
		a.runToBitmap()
		if !reflect.DeepEqual(a.bitmap, exp) {
			t.Fatalf("test #%v expected %v, but got %v", i, exp, a.bitmap)
		}
	}
}

func getFullBitmap() []uint64 {
	x := make([]uint64, 1024)
	for i := range x {
		x[i] = uint64(0xFFFFFFFFFFFFFFFF)
	}
	return x

}

func TestBitmapToRun(t *testing.T) {
	a := &Container{containerType: containerBitmap}
	tests := []struct {
		bitmap []uint64
		exp    []interval16
	}{
		{
			// empty run
			bitmap: []uint64{},
			exp:    []interval16{},
		},
		{
			// single-bit run
			bitmap: []uint64{1},
			exp:    []interval16{{start: 0, last: 0}},
		},
		{
			// single multi-bit run in one word
			bitmap: []uint64{31},
			exp:    []interval16{{start: 0, last: 4}},
		},
		{
			// multiple runs in one word
			bitmap: []uint64{155876},
			exp:    []interval16{{start: 2, last: 2}, {start: 5, last: 7}, {start: 13, last: 14}, {start: 17, last: 17}},
		},
		{
			// span two words, both mixed
			bitmap: []uint64{0xF00000000000000F, 0x000000000000000F},
			exp:    []interval16{{start: 0, last: 3}, {start: 60, last: 67}},
		},
		{
			// span two words, first = maxBitmap
			bitmap: []uint64{0xFFFFFFFFFFFFFFFF, 0xF},
			exp:    []interval16{{start: 0, last: 67}},
		},
		{
			// span two words, second = maxBitmap
			bitmap: []uint64{0xF000000000000000, 0xFFFFFFFFFFFFFFFF},
			exp:    []interval16{{start: 60, last: 127}},
		},
		{
			// span three words
			bitmap: []uint64{0xF000000000000000, 0xFFFFFFFFFFFFFFFF, 0xF},
			exp:    []interval16{{start: 60, last: 131}},
		},
		{
			bitmap: make([]uint64, bitmapN),
			exp:    []interval16{{start: 65408, last: 65535}},
		},
		{
			bitmap: getFullBitmap(),
			exp:    []interval16{{start: 0, last: 65535}},
		},
	}
	tests[8].bitmap[1022] = 0xFFFFFFFFFFFFFFFF
	tests[8].bitmap[1023] = 0xFFFFFFFFFFFFFFFF

	for i, test := range tests {
		a.bitmap = make([]uint64, bitmapN)
		n := 0
		for i, v := range test.bitmap {
			a.bitmap[i] = v
			n += int(popcount(v))
		}
		a.n = int32(n)
		x := a.bitmap
		a.bitmapToRun()
		if !reflect.DeepEqual(a.runs, test.exp) {
			t.Fatalf("test #%v expected %v, but got %v", i, test.exp, a.runs)
		}
		a.runToBitmap()
		if !reflect.DeepEqual(a.bitmap, x) {
			t.Fatalf("test #%v expected %v, but got %v", i, a.bitmap, x)
		}
	}
}

func TestArrayToRun(t *testing.T) {
	a := &Container{containerType: containerArray}
	tests := []struct {
		array []uint16
		exp   []interval16
	}{
		{
			array: []uint16{},
			exp:   []interval16{},
		},
		{
			array: []uint16{0},
			exp:   []interval16{{start: 0, last: 0}},
		},
		{
			array: []uint16{0, 1, 2, 3, 4},
			exp:   []interval16{{start: 0, last: 4}},
		},
		{
			array: []uint16{2, 5, 6, 7, 13, 14, 17},
			exp:   []interval16{{start: 2, last: 2}, {start: 5, last: 7}, {start: 13, last: 14}, {start: 17, last: 17}},
		},
	}

	for i, test := range tests {
		a.array = test.array
		a.n = int32(len(test.array))
		a.arrayToRun()
		if !reflect.DeepEqual(a.runs, test.exp) {
			t.Fatalf("test #%v expected %v, but got %v", i, test.exp, a.runs)
		}
	}
}

func TestRunToArray(t *testing.T) {
	a := &Container{containerType: containerRun}
	tests := []struct {
		runs []interval16
		exp  []uint16
	}{
		{
			runs: []interval16{},
			exp:  []uint16{},
		},
		{
			runs: []interval16{{start: 0, last: 0}},
			exp:  []uint16{0},
		},
		{
			runs: []interval16{{start: 0, last: 4}},
			exp:  []uint16{0, 1, 2, 3, 4},
		},
		{
			runs: []interval16{{start: 2, last: 2}, {start: 5, last: 7}, {start: 13, last: 14}, {start: 17, last: 17}},
			exp:  []uint16{2, 5, 6, 7, 13, 14, 17},
		},
	}

	for i, test := range tests {
		a.runs = test.runs
		a.n = int32(len(test.exp))
		a.runToArray()
		if !reflect.DeepEqual(a.array, test.exp) {
			t.Fatalf("test #%v expected %v, but got %v", i, test.exp, a.array)
		}
	}
}

func TestBitmapZeroRange(t *testing.T) {
	c := &Container{containerType: containerBitmap, bitmap: make([]uint64, bitmapN)}
	tests := []struct {
		bitmap []uint64
		start  uint64
		last   uint64
		exp    []uint64
		expN   int32
	}{
		{
			bitmap: []uint64{0x0000000000FFFF00},
			start:  9,
			last:   10,
			exp:    []uint64{0x0000000000FFF900},
			expN:   14,
		},
		{
			bitmap: []uint64{0xFF0, 0xFF, 0xFF},
			start:  60,
			last:   130,
			exp:    []uint64{0xFF0, 0, 0xF8},
			expN:   13,
		},
	}

	for i, test := range tests {
		copy(c.bitmap, test.bitmap)
		c.n = c.countRange(0, 65535)
		c.bitmapZeroRange(test.start, test.last+1)
		if !reflect.DeepEqual(c.bitmap[:len(test.exp)], test.exp) {
			t.Fatalf("test %#v expected %x, got %x", i, test.exp, c.bitmap[:len(test.bitmap)])
		}
		if test.expN != c.n {
			t.Fatalf("test #%v expected n to be %v, but got %v", i, test.expN, c.n)
		}
		for i := range test.bitmap {
			c.bitmap[i] = 0
		}
	}

}

func TestUnionBitmapRun(t *testing.T) {
	a := &Container{containerType: containerBitmap, bitmap: make([]uint64, bitmapN)}
	b := &Container{containerType: containerRun}
	tests := []struct {
		bitmap []uint64
		runs   []interval16
		exp    []uint64
		expN   int32
	}{
		{
			bitmap: []uint64{2},
			runs:   []interval16{{start: 0, last: 0}, {start: 2, last: 5}, {start: 62, last: 71}, {start: 77, last: 78}},
			exp:    []uint64{0xC00000000000003F, 0x60FF},
			expN:   18,
		},
	}
	for i, test := range tests {
		copy(a.bitmap, test.bitmap)
		a.n = a.bitmapCountRange(0, 65535)
		b.runs = test.runs
		b.n = b.runCountRange(0, 65535)
		ret := unionBitmapRun(a, b)
		if ret.isArray() {
			ret.arrayToBitmap()
		}
		if !reflect.DeepEqual(ret.bitmap[:len(test.exp)], test.exp) {
			t.Fatalf("test #%v expected %x, but got %x", i, test.exp, ret.bitmap[:len(test.exp)])
		}
		if ret.n != test.expN {
			t.Fatalf("test #%v expected n to be %v, but got %v", i, test.expN, ret.n)
		}
		for i := range test.bitmap {
			a.bitmap[i] = 0
		}
	}
}

func TestBitmapCountRuns(t *testing.T) {
	c := &Container{containerType: containerBitmap, bitmap: make([]uint64, bitmapN)}
	tests := []struct {
		bitmap []uint64
		exp    int32
	}{
		{
			bitmap: []uint64{0xFF00FF00},
			exp:    2,
		},
		{
			bitmap: []uint64{0xFF00FF0000000000, 0x1},
			exp:    2,
		},
		{
			bitmap: []uint64{0xFF00FF0000000000, 0x2, 0x100},
			exp:    4,
		},
		{
			bitmap: []uint64{0xFF00FF0000000000, 0x1010101FF0101010, 0x100},
			exp:    10,
		},
	}

	for i, test := range tests {
		copy(c.bitmap, test.bitmap)

		ret := c.bitmapCountRuns()
		if ret != test.exp {
			t.Fatalf("test #%v expected %v but got %v", i, test.exp, ret)
		}

		for j := range test.bitmap {
			c.bitmap[j] = 0
		}
	}

	test := tests[3]
	for j, v := range test.bitmap {
		c.bitmap[1024-len(test.bitmap)+j] = v

	}
	ret := c.bitmapCountRuns()
	if ret != test.exp {
		t.Fatalf("test at end expected %v but got %v", test.exp, ret)
	}
}

func TestArrayCountRuns(t *testing.T) {
	c := &Container{containerType: containerArray}
	tests := []struct {
		array []uint16
		exp   int32
	}{
		{
			array: []uint16{},
			exp:   0,
		},
		{
			array: []uint16{0},
			exp:   1,
		},
		{
			array: []uint16{1},
			exp:   1,
		},
		{
			array: []uint16{1, 2, 3, 5},
			exp:   2,
		},
		{
			array: []uint16{0, 1, 3, 9, 2048, 4096, 4097, 65534, 65535},
			exp:   6,
		},
		{
			array: []uint16{0, 10, 11, 12},
			exp:   2,
		},
	}

	for i, test := range tests {
		c.array = test.array
		ret := c.arrayCountRuns()
		if ret != test.exp {
			t.Fatalf("test #%v expected %v but got %v", i, test.exp, ret)
		}
	}
}

func TestDifferenceArrayRun(t *testing.T) {
	a := &Container{containerType: containerArray}
	b := &Container{containerType: containerRun}
	tests := []struct {
		array []uint16
		runs  []interval16
		exp   []uint16
	}{
		{
			array: []uint16{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12},
			runs:  []interval16{{start: 5, last: 10}},
			exp:   []uint16{0, 1, 2, 3, 4, 11, 12},
		},
	}
	for i, test := range tests {
		a.array = test.array
		a.n = int32(len(a.array))
		b.runs = test.runs
		b.n = b.runCountRange(0, 100)
		ret := differenceArrayRun(a, b)
		if !reflect.DeepEqual(ret.array, test.exp) {
			t.Fatalf("test #%v expected %v, but got %v", i, test.exp, ret.array)
		}
	}
}

func TestDifferenceRunArray(t *testing.T) {
	a := &Container{containerType: containerRun}
	b := &Container{containerType: containerArray}
	tests := []struct {
		runs  []interval16
		array []uint16
		exp   []interval16
	}{
		{
			runs:  []interval16{{start: 0, last: 12}},
			array: []uint16{5, 6, 7, 8, 9, 10},
			exp:   []interval16{{start: 0, last: 4}, {start: 11, last: 12}},
		},
		{
			runs:  []interval16{{start: 0, last: 12}},
			array: []uint16{0, 1, 2, 3},
			exp:   []interval16{{start: 4, last: 12}},
		},
		{
			runs:  []interval16{{start: 0, last: 12}},
			array: []uint16{9, 10, 11, 12, 13},
			exp:   []interval16{{start: 0, last: 8}},
		},
		{
			runs:  []interval16{{start: 1, last: 12}},
			array: []uint16{0, 9, 10, 11, 12, 13},
			exp:   []interval16{{start: 1, last: 8}},
		},
		{
			runs:  []interval16{{start: 1, last: 12}, {start: 14, last: 14}, {start: 18, last: 18}},
			array: []uint16{0, 9, 10, 11, 12, 13, 14, 17},
			exp:   []interval16{{start: 1, last: 8}, {start: 18, last: 18}},
		},
		{
			runs:  []interval16{{start: 1, last: 12}, {start: 14, last: 14}, {start: 18, last: 18}},
			array: []uint16{0, 9, 10, 11, 12, 13, 14, 17, 19},
			exp:   []interval16{{start: 1, last: 8}, {start: 18, last: 18}},
		},
		{
			runs:  []interval16{{start: 1, last: 12}, {start: 14, last: 17}, {start: 19, last: 28}},
			array: []uint16{0, 9, 10, 11, 12, 13, 14, 17, 19, 25, 27},
			exp:   []interval16{{start: 1, last: 8}, {start: 15, last: 16}, {start: 20, last: 24}, {start: 26, last: 26}, {start: 28, last: 28}},
		},
		{
			runs:  []interval16{{start: 0, last: 20}, {start: 65533, last: 65535}},
			array: []uint16{65533, 65534, 65535},
			exp:   []interval16{{start: 0, last: 20}},
		},
		{
			runs:  []interval16{{start: 0, last: 20}, {start: 65530, last: 65535}},
			array: []uint16{37, 65535},
			exp:   []interval16{{start: 0, last: 20}, {start: 65530, last: 65534}},
		},
	}
	for i, test := range tests {
		a.runs = test.runs
		a.n = a.runCountRange(0, 100)
		b.array = test.array
		b.n = int32(len(b.array))
		ret := differenceRunArray(a, b)
		if !reflect.DeepEqual(ret.runs, test.exp) {
			t.Fatalf("test #%v expected %v, but got %v", i, test.exp, ret.runs)
		}
	}
}
func MakeBitmap(start []uint64) []uint64 {
	b := make([]uint64, bitmapN)
	copy(b, start)
	return b
}
func MakeLastBitSet() []uint64 {
	obj := NewFileBitmap(65535)
	c := obj.container(0)
	c.arrayToBitmap()
	return c.bitmap
}

func TestDifferenceRunBitmap(t *testing.T) {
	a := &Container{containerType: containerRun}
	b := &Container{containerType: containerBitmap, bitmap: make([]uint64, bitmapN)}
	tests := []struct {
		runs   []interval16
		bitmap []uint64
		exp    []interval16
	}{
		{
			runs:   []interval16{{start: 0, last: 63}},
			bitmap: MakeBitmap([]uint64{0x0000FFFF000000F0}),
			exp:    []interval16{{start: 0, last: 3}, {start: 8, last: 31}, {start: 48, last: 63}},
		},
		{
			runs:   []interval16{{start: 0, last: 63}},
			bitmap: MakeBitmap([]uint64{0x8000000000000000}),
			exp:    []interval16{{start: 0, last: 62}},
		},
		{
			runs:   []interval16{{start: 0, last: 63}},
			bitmap: MakeBitmap([]uint64{0x0000000000000001}),
			exp:    []interval16{{start: 1, last: 63}},
		},
		{
			runs:   []interval16{{start: 0, last: 63}},
			bitmap: MakeBitmap([]uint64{0x0, 0x0000000000000001}),
			exp:    []interval16{{start: 0, last: 63}},
		},
		{
			runs:   []interval16{{start: 0, last: 65}},
			bitmap: MakeBitmap([]uint64{0x0, 0x0000000000000001}),
			exp:    []interval16{{start: 0, last: 63}, {start: 65, last: 65}},
		},
		{
			runs:   []interval16{{start: 0, last: 65}},
			bitmap: MakeBitmap([]uint64{0x0, 0x8000000000000000}),
			exp:    []interval16{{start: 0, last: 65}},
		},
		{
			runs:   []interval16{{start: 1, last: 65535}},
			bitmap: MakeBitmap([]uint64{0x0000000000000001}),
			exp:    []interval16{{start: 1, last: 65535}},
		},
		{
			runs:   []interval16{{start: 0, last: 65533}, {start: 65535, last: 65535}},
			bitmap: MakeLastBitSet(),
			exp:    []interval16{{start: 0, last: 65533}},
		},
	}
	for i, test := range tests {
		a.runs = test.runs
		a.n = a.runCountRange(0, 65536)
		copy(b.bitmap, test.bitmap)
		b.n = b.bitmapCountRange(0, 65536)
		ret := differenceRunBitmap(a, b)
		if !reflect.DeepEqual(ret.runs, test.exp) {
			t.Fatalf("test #%v expected %v, but got %v", i, test.exp, ret.runs)
		}
	}
}

func TestDifferenceBitmapRun(t *testing.T) {
	a := &Container{containerType: containerBitmap, bitmap: make([]uint64, bitmapN)}
	b := &Container{containerType: containerRun}
	tests := []struct {
		bitmap []uint64
		runs   []interval16
		exp    []uint64
	}{
		{
			bitmap: []uint64{0xFFFFFFFFFFFFFFFF},
			runs:   []interval16{{start: 4, last: 7}, {start: 32, last: 47}},
			exp:    []uint64{0xFFFF0000FFFFFF0F},
		},
		{
			bitmap: []uint64{0xFFFFFFFFFFFFFFBF},
			runs:   []interval16{{start: 0, last: 5}, {start: 7, last: 63}},
			exp:    []uint64{0x0000000000000000},
		},
		{
			bitmap: []uint64{0xFFFFFFFFFFFFFFBF},
			runs:   []interval16{{start: 0, last: 5}},
			exp:    []uint64{0xFFFFFFFFFFFFFF80},
		},
		{
			bitmap: []uint64{0xFFFFFFFFFFFFFFFF},
			runs:   []interval16{{start: 60, last: 63}},
			exp:    []uint64{0x0FFFFFFFFFFFFFFF},
		},
		{
			bitmap: []uint64{0xFFFFFFFFFFFFFFFF},
			runs:   []interval16{{start: 60, last: 65}},
			exp:    []uint64{0x0FFFFFFFFFFFFFFF},
		},
		{
			bitmap: []uint64{0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF},
			runs:   []interval16{{start: 60, last: 65}, {start: 67, last: 72}, {start: 126, last: 130}},
			exp:    []uint64{0x0FFFFFFFFFFFFFFF, 0x3FFFFFFFFFFFFE04, 0xFFFFFFFFFFFFFFF8},
		},
		{
			bitmap: []uint64{0x0000000000000001},
			runs:   []interval16{{start: 0, last: 0}},
			exp:    []uint64{0x0000000000000000},
		},
		{
			bitmap: []uint64{0x8000000000000000},
			runs:   []interval16{{start: 63, last: 63}},
			exp:    []uint64{0x0000000000000000},
		},
		{
			bitmap: []uint64{0xC000000000000000, 0x0000000000000003},
			runs:   []interval16{{start: 63, last: 64}},
			exp:    []uint64{0x4000000000000000, 0x0000000000000002},
		},
		{
			bitmap: []uint64{0x0000000000000000},
			runs:   []interval16{{start: 5, last: 7}},
			exp:    []uint64{0x0000000000000000},
		},
		{
			bitmap: bitmapLastBitSet(),
			runs:   []interval16{{start: 65535, last: 65535}},
			exp:    bitmapEmpty(),
		},
		{
			bitmap: bitmapFull(),
			runs:   []interval16{{start: 0, last: 65535}},
			exp:    bitmapEmpty(),
		},
	}
	for i, test := range tests {
		copy(a.bitmap, test.bitmap)
		a.n = a.bitmapCountRange(0, 65536)
		b.runs = test.runs
		b.n = b.runCountRange(0, 65536)
		ret := differenceBitmapRun(a, b)
		if !reflect.DeepEqual(ret.bitmap[:len(test.exp)], test.exp) {
			t.Fatalf("test #%v expected \n%X, but got \n%X", i, test.exp, ret.bitmap[:len(test.exp)])
		}
	}
}

func TestDifferenceBitmapArray(t *testing.T) {
	b := &Container{containerType: containerBitmap, bitmap: make([]uint64, bitmapN)}
	a := &Container{containerType: containerArray}
	tests := []struct {
		bitmap []uint64
		array  []uint16
		exp    []uint16
	}{
		{
			bitmap: MakeBitmap([]uint64{0xFF0F}),
			array:  []uint16{0, 1, 2, 3, 4, 5, 6, 7, 10},
			exp:    []uint16{8, 9, 11, 12, 13, 14, 15},
		},
		{
			bitmap: []uint64{0x0000},
			array:  []uint16{0, 1, 2, 3, 4, 5, 6, 7, 10},
			exp:    []uint16{},
		},
		{
			bitmap: []uint64{0xFFFF},
			array:  []uint16{0, 1, 2, 3, 4, 5, 6, 7, 10},
			exp:    []uint16{8, 9, 11, 12, 13, 14, 15},
		},
		{
			bitmap: bitmapOddBitsSet(),
			array:  []uint16{0, 1, 2, 3, 4, 5, 6, 7, 10},
			exp:    []uint16{9, 11, 13, 15, 17, 19, 21, 23, 25, 27, 29, 31, 33, 35, 37, 39, 41, 43, 45, 47, 49, 51, 53, 55, 57, 59, 61, 63},
		},
		{
			bitmap: bitmapOddBitsSet(),
			array:  []uint16{63},
			exp:    []uint16{1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21, 23, 25, 27, 29, 31, 33, 35, 37, 39, 41, 43, 45, 47, 49, 51, 53, 55, 57, 59, 61},
		},
		{
			bitmap: MakeBitmap([]uint64{0x0000FFFF000000F0}),
			array:  []uint16{4, 5, 6, 7, 20, 21, 22, 23, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47},
			exp:    []uint16{},
		},
	}
	for i, test := range tests {
		b.bitmap[0] = test.bitmap[0]
		b.n = b.count()
		a.array = test.array
		ret := differenceBitmapArray(b, a)
		if !reflect.DeepEqual(ret.array, test.exp) {
			t.Fatalf("test #%v expected %X, but got %X", i, test.exp, ret.array)
		}
	}
}

func TestDifferenceBitmapBitmap(t *testing.T) {
	a := &Container{bitmap: make([]uint64, bitmapN), containerType: containerBitmap}
	b := &Container{bitmap: make([]uint64, bitmapN), containerType: containerBitmap}
	tests := []struct {
		abitmap []uint64
		bbitmap []uint64
		exp     []uint16
	}{
		{
			abitmap: []uint64{0xFF00FFFFFFFFFFFF},
			bbitmap: []uint64{0xFFFFFFFFFFFFF000},
			exp:     []uint16{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
		},
		{
			abitmap: []uint64{0xF},
			bbitmap: []uint64{0},
			exp:     []uint16{0, 1, 2, 3},
		},
	}
	for i, test := range tests {
		a.bitmap[0] = test.abitmap[0]
		b.bitmap[0] = test.bbitmap[0]

		ret := differenceBitmapBitmap(a, b)
		if !reflect.DeepEqual(ret.array, test.exp) {
			t.Fatalf("test #%v expected \n%X, but got \n%X", i, test.exp, ret.array)
		}
	}
}

func TestDifferenceRunRun(t *testing.T) {
	a := &Container{containerType: containerRun}
	b := &Container{containerType: containerRun}
	tests := []struct {
		aruns []interval16
		bruns []interval16
		exp   []interval16
		expn  int32
	}{
		{
			// this tests all six overlap combinations
			// A                     [   ]                   [   ]                        [ ]              [     ]             [   ]                 [  ]
			// B                    [     ]                [   ]                     [ ]                     [  ]                [   ]                    [  ]
			aruns: []interval16{{start: 3, last: 6}, {start: 13, last: 16}, {start: 24, last: 26}, {start: 33, last: 38}, {start: 43, last: 46}, {start: 53, last: 56}},
			bruns: []interval16{{start: 1, last: 8}, {start: 11, last: 14}, {start: 21, last: 23}, {start: 35, last: 37}, {start: 44, last: 48}, {start: 57, last: 59}},
			exp:   []interval16{{start: 15, last: 16}, {start: 24, last: 26}, {start: 33, last: 34}, {start: 38, last: 38}, {start: 43, last: 43}, {start: 53, last: 56}},
			expn:  13,
		},
	}
	for i, test := range tests {
		a.runs = test.aruns
		a.n = a.runCountRange(0, 100)
		b.runs = test.bruns
		b.n = b.runCountRange(0, 100)
		ret := differenceRunRun(a, b)
		if !reflect.DeepEqual(ret.runs, test.exp) {
			t.Fatalf("test #%v expected %v, but got %v", i, test.exp, ret.runs)
		}
		if ret.n != test.expn {
			t.Fatalf("test #%v expected n=%v, but got n=%v", i, test.expn, ret.n)
		}
	}
}

func TestWriteReadArray(t *testing.T) {
	ca := &Container{array: []uint16{1, 10, 100, 1000}, n: 4, containerType: containerArray}
	ba := NewFileBitmap()
	ba.Containers.Put(0, ca)
	ba2 := NewFileBitmap()
	var buf bytes.Buffer
	_, err := ba.WriteTo(&buf)
	if err != nil {
		t.Fatalf("error writing: %v", err)
	}
	err = ba2.UnmarshalBinary(buf.Bytes())
	if err != nil {
		t.Fatalf("error unmarshaling: %v", err)
	}
	if !reflect.DeepEqual(ba2.Containers.Get(0).array, ca.array) {
		t.Fatalf("array test expected %x, but got %x", ca.array, ba2.Containers.Get(0).array)
	}
}

func TestWriteReadBitmap(t *testing.T) {
	// create bitmap containing > 4096 bits
	cb := &Container{bitmap: make([]uint64, bitmapN), n: 129 * 32, containerType: containerBitmap}
	for i := 0; i < 129; i++ {
		cb.bitmap[i] = 0x5555555555555555
	}
	bb := NewFileBitmap()
	bb.Containers.Put(0, cb)
	bb2 := NewFileBitmap()
	var buf bytes.Buffer
	_, err := bb.WriteTo(&buf)
	if err != nil {
		t.Fatalf("error writing: %v", err)
	}
	err = bb2.UnmarshalBinary(buf.Bytes())
	if err != nil {
		t.Fatalf("error unmarshaling: %v", err)
	}
	if !reflect.DeepEqual(bb2.Containers.Get(0).bitmap, cb.bitmap) {
		t.Fatalf("bitmap test expected %x, but got %x", cb.bitmap, bb2.Containers.Get(0).bitmap)
	}
}

func TestWriteReadFullBitmap(t *testing.T) {
	// create bitmap containing > 4096 bits
	cb := &Container{bitmap: make([]uint64, bitmapN), n: 65536, containerType: containerBitmap}
	for i := 0; i < bitmapN; i++ {
		cb.bitmap[i] = 0xffffffffffffffff
	}
	bb := NewFileBitmap()
	bb.Containers.Put(0, cb)
	bb2 := NewFileBitmap()
	var buf bytes.Buffer
	_, err := bb.WriteTo(&buf)
	if err != nil {
		t.Fatalf("error writing: %v", err)
	}
	err = bb2.UnmarshalBinary(buf.Bytes())
	if err != nil {
		t.Fatalf("error unmarshaling: %v", err)
	}
	if !reflect.DeepEqual(bb2.Containers.Get(0).bitmap, cb.bitmap) {
		t.Fatalf("bitmap test expected %x, but got %x", cb.bitmap, bb2.Containers.Get(0).bitmap)
	}

	if bb2.Containers.Get(0).n != cb.n {
		t.Fatalf("bitmap test expected count %x, but got %x", cb.n, bb2.Containers.Get(0).n)
	}
	if bb2.Containers.Get(0).count() != cb.count() {
		t.Fatalf("bitmap test expected count %x, but got %x", cb.n, bb2.Containers.Get(0).n)
	}
}

func TestWriteReadRun(t *testing.T) {
	cr := &Container{runs: []interval16{{start: 3, last: 13}, {start: 100, last: 109}}, n: 21, containerType: containerRun}
	br := NewFileBitmap()
	br.Containers.Put(0, cr)
	br2 := NewFileBitmap()
	var buf bytes.Buffer
	_, err := br.WriteTo(&buf)
	if err != nil {
		t.Fatalf("error writing: %v", err)
	}
	err = br2.UnmarshalBinary(buf.Bytes())
	if err != nil {
		t.Fatalf("error unmarshaling: %v", err)
	}
	if !reflect.DeepEqual(br2.Containers.Get(0).runs, cr.runs) {
		t.Fatalf("run test expected %x, but got %x", cr.runs, br2.Containers.Get(0).runs)
	}
}

func TestXorArrayRun(t *testing.T) {
	tests := []struct {
		a   *Container
		b   *Container
		exp *Container
	}{
		{
			a:   &Container{array: []uint16{1, 5, 10, 11, 12}, containerType: containerArray},
			b:   &Container{runs: []interval16{{start: 2, last: 10}, {start: 12, last: 13}, {start: 15, last: 16}}, containerType: containerRun},
			exp: &Container{array: []uint16{1, 2, 3, 4, 6, 7, 8, 9, 11, 13, 15, 16}, containerType: containerArray, n: 12},
		}, {
			a:   &Container{array: []uint16{1, 5, 10, 11, 12, 13, 14}, containerType: containerArray},
			b:   &Container{runs: []interval16{{start: 2, last: 10}, {start: 12, last: 13}, {start: 15, last: 16}}, containerType: containerRun},
			exp: &Container{array: []uint16{1, 2, 3, 4, 6, 7, 8, 9, 11, 14, 15, 16}, containerType: containerArray, n: 12},
		}, {
			a:   &Container{array: []uint16{65535}, containerType: containerArray},
			b:   &Container{runs: []interval16{{start: 65534, last: 65535}}, containerType: containerRun},
			exp: &Container{array: []uint16{65534}, containerType: containerArray, n: 1},
		}, {
			a:   &Container{array: []uint16{65535}, containerType: containerArray},
			b:   &Container{runs: []interval16{{start: 65535, last: 65535}}, containerType: containerRun},
			exp: &Container{array: []uint16{}, containerType: containerArray, n: 0},
		},
	}

	for i, test := range tests {
		test.a.n = test.a.count()
		test.b.n = test.b.count()
		ret := xor(test.a, test.b)
		if !reflect.DeepEqual(ret, test.exp) {
			t.Fatalf("test #%v expected %v, but got %v", i, test.exp, ret)
		}
		ret = xor(test.b, test.a)
		if !reflect.DeepEqual(ret, test.exp) {
			t.Fatalf("test #%v.1 expected %v, but got %v", i, test.exp, ret)
		}
	}

}

//special case that didn't fit the xorrunrun table testing below.
func TestXorRunRun1(t *testing.T) {
	a := &Container{containerType: containerRun}
	b := &Container{containerType: containerRun}
	a.runs = []interval16{{start: 4, last: 10}}
	b.runs = []interval16{{start: 5, last: 10}}
	ret := xorRunRun(a, b)
	if !reflect.DeepEqual(ret.array, []uint16{4}) {
		t.Fatalf("test #1 expected %v, but got %v", []uint16{4}, ret.array)
	}
	ret = xorRunRun(b, a)
	if !reflect.DeepEqual(ret.array, []uint16{4}) {
		t.Fatalf("test #1 expected %v, but got %v", []uint16{4}, ret.array)
	}
}

func TestXorRunRun(t *testing.T) {
	a := &Container{containerType: containerRun}
	b := &Container{containerType: containerRun}
	tests := []struct {
		aruns []interval16
		bruns []interval16
		exp   []interval16
	}{
		{
			aruns: []interval16{},
			bruns: []interval16{{start: 5, last: 10}},
			exp:   []interval16{{start: 5, last: 10}},
		},
		{
			aruns: []interval16{{start: 0, last: 4}},
			bruns: []interval16{{start: 6, last: 10}},
			exp:   []interval16{{start: 0, last: 4}, {start: 6, last: 10}},
		},
		{
			aruns: []interval16{{start: 0, last: 6}},
			bruns: []interval16{{start: 4, last: 10}},
			exp:   []interval16{{start: 0, last: 3}, {start: 7, last: 10}},
		},
		{
			aruns: []interval16{{start: 4, last: 10}},
			bruns: []interval16{{start: 0, last: 6}},
			exp:   []interval16{{start: 0, last: 3}, {start: 7, last: 10}},
		},
		{
			aruns: []interval16{{start: 0, last: 10}},
			bruns: []interval16{{start: 0, last: 6}},
			exp:   []interval16{{start: 7, last: 10}},
		},
		{
			aruns: []interval16{{start: 0, last: 6}},
			bruns: []interval16{{start: 0, last: 10}},
			exp:   []interval16{{start: 7, last: 10}},
		},
		{
			aruns: []interval16{{start: 0, last: 6}},
			bruns: []interval16{{start: 0, last: 10}},
			exp:   []interval16{{start: 7, last: 10}},
		},
		{
			aruns: []interval16{{start: 5, last: 12}},
			bruns: []interval16{{start: 5, last: 10}},
			exp:   []interval16{{start: 11, last: 12}},
		},
		{
			aruns: []interval16{{start: 1, last: 3}, {start: 5, last: 5}, {start: 7, last: 12}},
			bruns: []interval16{{start: 5, last: 10}},
			exp:   []interval16{{start: 1, last: 3}, {start: 6, last: 6}, {start: 11, last: 12}},
		},
		{
			aruns: []interval16{{start: 1, last: 3}, {start: 5, last: 5}, {start: 7, last: 12}},
			bruns: []interval16{{start: 2, last: 65535}},
			exp:   []interval16{{start: 1, last: 1}, {start: 4, last: 4}, {start: 6, last: 6}, {start: 13, last: 65535}},
		},
		{
			aruns: []interval16{{start: 2, last: 65535}},
			bruns: []interval16{{start: 1, last: 3}, {start: 5, last: 5}, {start: 7, last: 12}},
			exp:   []interval16{{start: 1, last: 1}, {start: 4, last: 4}, {start: 6, last: 6}, {start: 13, last: 65535}},
		},
		{
			aruns: []interval16{{start: 1, last: 3}, {start: 5, last: 5}, {start: 7, last: 12}},
			bruns: []interval16{{start: 0, last: 65535}},
			exp:   []interval16{{start: 0, last: 0}, {start: 4, last: 4}, {start: 6, last: 6}, {start: 13, last: 65535}},
		},
		{
			aruns: []interval16{{start: 0, last: 65535}},
			bruns: []interval16{{start: 1, last: 3}, {start: 5, last: 5}, {start: 7, last: 12}},
			exp:   []interval16{{start: 0, last: 0}, {start: 4, last: 4}, {start: 6, last: 6}, {start: 13, last: 65535}},
		},
		{
			aruns: []interval16{{start: 1, last: 3}, {start: 5, last: 5}, {start: 7, last: 9}, {start: 12, last: 22}},
			bruns: []interval16{{start: 2, last: 8}, {start: 16, last: 27}, {start: 33, last: 34}},
			exp:   []interval16{{start: 1, last: 1}, {start: 4, last: 4}, {start: 6, last: 6}, {start: 9, last: 9}, {start: 12, last: 15}, {start: 23, last: 27}, {start: 33, last: 34}},
		},
		{
			aruns: []interval16{{start: 65530, last: 65535}},
			bruns: []interval16{{start: 65532, last: 65535}},
			exp:   []interval16{{start: 65530, last: 65531}},
		},
	}
	for i, test := range tests {
		a.runs = test.aruns
		b.runs = test.bruns
		ret := xorRunRun(a, b)
		if !reflect.DeepEqual(ret.runs, test.exp) {
			t.Fatalf("test #%v expected %v, but got %v", i, test.exp, ret.runs)
		}
		ret = xorRunRun(b, a)
		if !reflect.DeepEqual(ret.runs, test.exp) {
			t.Fatalf("test #%v.1 expected %v, but got %v", i, test.exp, ret.runs)
		}
	}
}

func TestBitmapXorRange(t *testing.T) {
	c := &Container{bitmap: make([]uint64, bitmapN), containerType: containerBitmap}
	tests := []struct {
		bitmap []uint64
		start  uint64
		last   uint64
		exp    []uint64
		expN   int32
	}{
		{
			bitmap: []uint64{0x0000000000000000},
			start:  0,
			last:   2,
			exp:    []uint64{0x0000000000000007},
			expN:   3,
		},
		{
			bitmap: []uint64{0xF1},
			start:  4,
			last:   8,
			exp:    []uint64{0x101},
			expN:   2,
		},
		{
			bitmap: []uint64{0xAA},
			start:  0,
			last:   7,
			exp:    []uint64{0x55},
			expN:   4,
		},
		{
			bitmap: []uint64{0x0, 0x0000000000000000, 0x0000000000000000},
			start:  63,
			last:   128,
			exp:    []uint64{0x8000000000000000, 0xFFFFFFFFFFFFFFFF, 0x000000000000001},
			expN:   66,
		},
		{
			bitmap: []uint64{0x0, 0x00000000000000FF, 0x0000000000000000},
			start:  63,
			last:   128,
			exp:    []uint64{0x8000000000000000, 0xFFFFFFFFFFFFFF00, 0x000000000000001},
			expN:   58,
		},
		{
			bitmap: []uint64{0x0, 0x0, 0x0},
			start:  129,
			last:   131,
			exp:    []uint64{0x0000000000000000, 0x0000000000000000, 0x00000000000000E},
			expN:   3,
		},
	}

	for i, test := range tests {
		copy(c.bitmap, test.bitmap)
		c.n = c.countRange(0, 65535)
		c.bitmapXorRange(test.start, test.last+1)
		if !reflect.DeepEqual(c.bitmap[:len(test.exp)], test.exp) {
			t.Fatalf("test %#v expected %x, got %x", i, test.exp, c.bitmap[:len(test.bitmap)])
		}
		if test.expN != c.n {
			t.Fatalf("test #%v expected n to be %v, but got %v", i, test.expN, c.n)
		}
	}
}

func TestXorBitmapRun(t *testing.T) {
	a := &Container{containerType: containerBitmap}
	b := &Container{containerType: containerRun}
	tests := []struct {
		bitmap []uint64
		runs   []interval16
		exp    []uint64
	}{
		{
			bitmap: []uint64{0x0, 0x0, 0x0},
			runs:   []interval16{{start: 129, last: 131}},
			exp:    []uint64{0x0, 0x0, 0x00000000000000E},
		},
	}
	for i, test := range tests {
		a.bitmap = test.bitmap
		b.runs = test.runs
		//xorBitmapRun
		ret := xor(a, b)
		if !reflect.DeepEqual(ret.bitmap, test.exp) {
			t.Fatalf("test #%v expected %v, but got %v", i, test.exp, ret.bitmap)
		}
		ret = xor(b, a)
		if !reflect.DeepEqual(ret.bitmap, test.exp) {
			t.Fatalf("test #%v.1 expected %v, but got %v", i, test.exp, ret.bitmap)
		}
	}

}

func TestIteratorArray(t *testing.T) {
	// use values that span two containers
	b := NewFileBitmap(0, 1, 10, 100, 1000, 10000, 90000, 100000)
	if !b.Containers.Get(0).isArray() {
		t.Fatalf("wrong container type")
	}

	itr := b.Iterator()
	if !(itr.key == 0 && itr.j == -1) {
		t.Fatalf("iterator did not zero correctly: %v\n", itr)
	}

	itr.Seek(1000)
	if !(itr.key == 0 && itr.j == 3) {
		t.Fatalf("iterator did not seek correctly: %#v\n", itr)
	}

	itr.Seek(10000)
	itr.Next()
	val, eof := itr.Next()
	if !(itr.key == 1 && itr.j == 0 && val == 90000 && !eof) {
		t.Fatalf("iterator did not next correctly across containers: %v\n", itr)
	}

	itr.Seek(80000)
	if !(itr.key == 1 && itr.j == -1) {
		t.Fatalf("iterator did not seek missing value correctly: %v\n", itr)
	}

	itr.Seek(100000)
	if !(itr.key == 1 && itr.j == 0) {
		t.Fatalf("iterator did not seek correctly in multiple containers: %v\n", itr)
	}

	val, eof = itr.Next()
	if !(val == 100000 && !eof) {
		t.Fatalf("iterator did not next correctly: %d, %v\n", val, eof)
	}

	val, eof = itr.Next()
	if !(val == 0 && eof) {
		t.Fatalf("iterator did not eof correctly: %d, %v\n", val, eof)
	}
}

func TestIteratorBitmap(t *testing.T) {
	// use values that span two containers
	// this dataset will update to bitmap after enough Adds,
	// but won't update to RLE until Optimize() is called
	b := NewFileBitmap()
	for i := uint64(61000); i < 71000; i++ {
		b.Add(i)
	}
	for i := uint64(75000); i < 75100; i++ {
		b.Add(i)
	}
	if !b.Containers.Get(0).isBitmap() {
		t.Fatalf("wrong container type")
	}

	itr := b.Iterator()
	if !(itr.key == 0 && itr.j == -1) {
		t.Fatalf("iterator did not zero correctly: %v\n", itr)
	}

	itr.Seek(65000)
	if !(itr.key == 0 && itr.j == 64999) {
		t.Fatalf("iterator did not seek correctly: %v\n", itr)
	}

	itr.Seek(65535)
	itr.Next()
	val, eof := itr.Next()
	if !(itr.key == 1 && itr.j == 0 && val == 65536 && !eof) {
		t.Fatalf("iterator did not next correctly across containers: %v\n", itr)
	}

	itr.Seek(74000)
	if !(itr.key == 1 && itr.j == 8463) {
		t.Fatalf("iterator did not seek missing value correctly: %v\n", itr)
	}

	itr.Seek(70999)
	if !(itr.key == 1 && itr.j == 5462) {
		t.Fatalf("iterator did not seek correctly in multiple containers: %v\n", itr)
	}

	val, eof = itr.Next()
	if !(val == 70999 && !eof) {
		t.Fatalf("iterator did not next correctly: %d, %v\n", val, eof)
	}

	itr.Seek(75100)
	val, eof = itr.Next()
	if !(val == 0 && eof) {
		t.Fatalf("iterator did not eof correctly: %d, %v\n", val, eof)
	}
}

func TestIteratorRuns(t *testing.T) {
	b := NewFileBitmap(0, 1, 2, 3, 4, 5, 1000, 1001, 1002, 1003, 1004, 1005, 100000, 100001, 100002, 100003, 100004, 100005)
	b.Optimize()
	if !b.Containers.Get(0).isRun() {
		t.Fatalf("wrong container type")
	}

	itr := b.Iterator()
	if !(itr.key == 0 && itr.j == 0 && itr.k == -1) {
		t.Fatalf("iterator did not zero correctly: %v\n", itr)
	}

	itr.Seek(4)
	if !(itr.key == 0 && itr.j == 0 && itr.k == 3) {
		t.Fatalf("iterator did not seek correctly: %v\n", itr)
	}
	itr.Next()
	itr.Next()
	val, eof := itr.Next()
	if !(val == 1000 && !eof) {
		t.Fatalf("iterator did not next correctly across runs: %v, %v", val, itr)
	}
	itr.Next()
	val, eof = itr.Next()
	if !(val == 1002 && !eof) {
		t.Fatalf("iterator did not next correctly within a run: %v, %v", val, itr)
	}
	itr.Next()
	itr.Next()
	itr.Next()
	val, eof = itr.Next()
	if !(val == 100000 && !eof) {
		t.Fatalf("iterator did not next correctly across containers: %v, %v", val, itr)
	}

	itr.Seek(500)
	if !(itr.key == 0 && itr.j == 1 && itr.k == -1) {
		t.Fatalf("iterator did not seek missing value correctly: %v\n", itr)
	}

	itr.Seek(1004)
	if !(itr.key == 0 && itr.j == 1 && itr.k == 3) {
		t.Fatalf("iterator did not seek correctly in multiple runs: %v\n", itr)
	}

	itr.Seek(1005)
	if !(itr.key == 0 && itr.j == 1 && itr.k == 4) {
		t.Fatalf("iterator did not seek correctly to end of run: %v\n", itr)
	}

	itr.Seek(100005)
	if !(itr.key == 1 && itr.j == 0 && itr.k == 4) {
		t.Fatalf("iterator did not seek correctly in multiple containers: %v\n", itr)
	}

	itr.Next()
	val, eof = itr.Next()
	if !(val == 0 && eof) {
		t.Fatalf("iterator did not eof correctly: %d, %v\n", val, eof)
	}
}

func TestIteratorVarious(t *testing.T) {
	tests := []struct {
		bm  *Bitmap
		exp uint64
	}{
		{
			bm:  NewFileBitmap(3, 4, 5),
			exp: 3,
		},
		{
			bm:  bitmapVariousContainers(),
			exp: 61221,
		},
		{
			bm:  NewFileBitmap(2, 66000, 70000, 70001, 70002, 70003, 70004),
			exp: 7,
		},
	}

	for i, test := range tests {
		test.bm.Optimize()
		t.Run(fmt.Sprintf("#%d:", i), func(t *testing.T) {
			if cnt := test.bm.Count(); cnt != test.exp {
				t.Fatalf("merged count %d is not %d", cnt, test.exp)
			}
			iter := test.bm.Iterator()
			bits := make([]uint64, 0, test.bm.Count())
			for v, eof := iter.Next(); !eof; v, eof = iter.Next() {
				bits = append(bits, v)
			}
			if length := len(bits); uint64(length) != test.exp {
				t.Fatalf("length %d is not %d", length, test.exp)
			}
		})
	}

}

func TestRunBinSearchContains(t *testing.T) {
	tests := []struct {
		runs  []interval16
		index uint16
		exp   struct {
			index int32
			found bool
		}
	}{
		{
			runs:  []interval16{{start: 0, last: 10}},
			index: uint16(3),
			exp: struct {
				index int32
				found bool
			}{index: 0, found: true},
		},
		{
			runs:  []interval16{{start: 0, last: 10}},
			index: uint16(13),
			exp: struct {
				index int32
				found bool
			}{index: 0, found: false},
		},
		{
			runs:  []interval16{{start: 0, last: 10}, {start: 20, last: 30}},
			index: uint16(13),
			exp: struct {
				index int32
				found bool
			}{index: 0, found: false},
		},
		{
			runs:  []interval16{{start: 0, last: 10}, {start: 20, last: 30}},
			index: uint16(36),
			exp: struct {
				index int32
				found bool
			}{index: 1, found: false},
		},
	}
	for i, test := range tests {
		index := test.index
		runs := test.runs
		idx, found := binSearchRuns(index, runs)

		if test.exp.index != idx && test.exp.found != found {
			t.Fatalf("test #%v expected %v , but got %v %v", i, test.exp, idx, found)
		}
	}
}

func TestRunBinSearch(t *testing.T) {
	tests := []struct {
		runs   []interval16
		search uint16
		exp    bool
		expi   int32
	}{
		{
			runs:   []interval16{{2, 10}, {50, 60}, {80, 90}},
			search: 1,
			exp:    false,
			expi:   0,
		},
		{
			runs:   []interval16{{2, 10}, {50, 60}, {80, 90}},
			search: 2,
			exp:    true,
			expi:   0,
		},
		{
			runs:   []interval16{{2, 10}, {50, 60}, {80, 90}},
			search: 5,
			exp:    true,
			expi:   0,
		},
		{
			runs:   []interval16{{2, 10}, {50, 60}, {80, 90}},
			search: 10,
			exp:    true,
			expi:   0,
		},
		{
			runs:   []interval16{{2, 10}, {50, 60}, {80, 90}},
			search: 20,
			exp:    false,
			expi:   1,
		},
		{
			runs:   []interval16{{2, 10}, {50, 60}, {80, 90}},
			search: 55,
			exp:    true,
			expi:   1,
		},
		{
			runs:   []interval16{{2, 10}, {50, 60}, {80, 90}},
			search: 70,
			exp:    false,
			expi:   2,
		},
		{
			runs:   []interval16{{2, 10}, {50, 60}, {80, 90}},
			search: 100,
			exp:    false,
			expi:   3,
		},
	}
	for i, test := range tests {
		idx, contains := binSearchRuns(test.search, test.runs)
		if !(test.exp == contains && test.expi == idx) {
			t.Fatalf("test #%v expected (%v, %v) but got (%v, %v)", i, test.exp, test.expi, contains, idx)
		}
	}
}
func TestBitmap_RemoveEmptyContainers(t *testing.T) {
	bm1 := NewFileBitmap(1<<16, 2<<16, 3<<16)
	bm1.Remove(2 << 16)
	if bm1.countEmptyContainers() != 1 {
		t.Fatalf("Should be 1 empty container ")
	}
	bm1.removeEmptyContainers()

	if bm1.countEmptyContainers() != 0 {
		t.Fatalf("Should be no empty containers ")
	}
}

func TestBitmap_BitmapWriteToWithEmpty(t *testing.T) {
	bm1 := NewFileBitmap(1<<16, 2<<16, 3<<16)
	bm1.Remove(2 << 16)
	var buf bytes.Buffer
	if _, err := bm1.WriteTo(&buf); err != nil {
		t.Fatalf("Failure to write to bitmap buffer. ")
	}
	bm0 := NewFileBitmap()
	bm0.UnmarshalBinary(buf.Bytes())
	if bm0.countEmptyContainers() != 0 {
		t.Fatalf("Should be no empty containers ")
	}
	if bm0.Count() != bm1.Count() {
		t.Fatalf("Counts do not match after a marshal %d %d", bm0.Count(), bm1.Count())
	}
}

func TestSearch64(t *testing.T) {
	tests := []struct {
		a     []uint64
		value uint64
		exp   int
	}{
		{
			a:     []uint64{1, 5, 10, 12},
			value: 5,
			exp:   1,
		},
		{
			a:     []uint64{1, 5, 10, 12},
			value: 1,
			exp:   0,
		},
		{
			a:     []uint64{1, 5, 10, 12},
			value: 0,
			exp:   -1,
		},
		{
			a:     []uint64{1, 5, 10, 12},
			value: 2,
			exp:   -2,
		},
		{
			a:     []uint64{1, 5, 10, 12},
			value: 7,
			exp:   -3,
		},
		{
			a:     []uint64{1, 5, 10, 12},
			value: 11,
			exp:   -4,
		},
		{
			a:     []uint64{1, 5, 10, 12},
			value: 13,
			exp:   -5,
		},
		{
			a:     []uint64{1, 5, 10, 12},
			value: 3843534,
			exp:   -5,
		},
		{
			a:     []uint64{},
			value: 3843534,
			exp:   -1,
		},
		{
			a:     []uint64{},
			value: 0,
			exp:   -1,
		},
		{
			a:     []uint64{0},
			value: 0,
			exp:   0,
		},
		{
			a:     []uint64{0},
			value: 1,
			exp:   -2,
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%d in %v", test.value, test.a), func(t *testing.T) {
			actual := search64(test.a, test.value)
			if actual != test.exp {
				t.Errorf("got: %d, exp: %d", actual, test.exp)
			}
		})
	}
}

func TestIntersectArrayBitmap(t *testing.T) {
	a, b := &Container{containerType: containerArray}, &Container{
		containerType: containerBitmap,
		bitmap:        make([]uint64, bitmapN),
	}
	tests := []struct {
		array  []uint16
		bitmap []uint64
		exp    []uint16
	}{
		{
			array:  []uint16{0},
			bitmap: []uint64{1},
			exp:    []uint16{0},
		},
		{
			array:  []uint16{0, 1},
			bitmap: []uint64{3},
			exp:    []uint16{0, 1},
		},
		{
			array:  []uint16{64, 128, 129, 2000},
			bitmap: []uint64{932421, 2},
			exp:    []uint16{},
		},
		{
			array:  []uint16{0, 65, 130, 195},
			bitmap: []uint64{255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255},
			exp:    []uint16{0, 65, 130, 195},
		},
		{
			array:  []uint16{63, 120, 543, 639, 12000},
			bitmap: []uint64{0x8000000000000000, 0, 0, 0, 0, 0, 0, 0, 0, 0x8000000000000000},
			exp:    []uint16{63, 639},
		},
		{
			array:  []uint16{0, 1, 63, 120, 543, 639, 12000, 65534, 65535},
			bitmap: bitmapOddBitsSet(),
			exp:    []uint16{1, 63, 543, 639, 65535},
		},
		{
			array:  []uint16{0, 1, 63, 120, 543, 639, 12000, 65534, 65535},
			bitmap: bitmapEvenBitsSet(),
			exp:    []uint16{0, 120, 12000, 65534},
		},
	}

	for i, test := range tests {
		a.array = test.array
		a.containerType = containerArray
		copy(b.bitmap, test.bitmap)
		b.containerType = containerBitmap
		ret := intersectArrayBitmap(a, b).array
		if len(ret) == 0 && len(test.exp) == 0 {
			continue
		}
		if !reflect.DeepEqual(ret, test.exp) {
			t.Fatalf("test #%v intersectArrayBitmap received: %v exp: %v", i, ret, test.exp)
		}
	}
}

func TestBitmapClone(t *testing.T) {
	b := NewFileBitmap()
	for i := uint64(61000); i < 71000; i++ {
		b.Add(i)
	}
	c := b.Clone()
	if err := bitmapsEqual(b, c); err != nil {
		t.Fatalf("Clone Objects not equal: %v\n", err)
	}
	d := func() *Bitmap { //anybody know how to declare a nil value?
		return nil
	}()
	e := d.Clone()
	if e != nil {
		t.Fatalf("Clone nil Objects not equal\n")
	}
}

// rleCont returns a slice of numbers all in the range starting from
// container_width*num, and ending at container_width*(num+1)-1. If left is
// true, then the first 100 bits will be set, if mid is true, 100 bits in the
// middle will be set, if right is true, the last 100 bits will be set.
// sets 100 bits per true
func rleCont(num int, left, mid, right bool) []uint64 {
	ret := make([]uint64, 0)
	base := containerWidth * uint64(num)
	if left {
		for i := uint64(0); i < 100; i++ {
			ret = append(ret, base+i)
		}
	}
	if mid {
		for i := containerWidth / 2; i < containerWidth/2+100; i++ {
			ret = append(ret, base+i)
		}
	}
	if right {
		for i := containerWidth - 100; i < containerWidth; i++ {
			ret = append(ret, base+i)
		}
	}
	return ret
}

// sets 2 bits per true.
func arrCont(num int, left, mid, right bool) []uint64 {
	ret := make([]uint64, 0)
	base := containerWidth * uint64(num)
	if left {
		ret = append(ret, base+0, base+2)
	}
	if mid {
		half := containerWidth / 2
		ret = append(ret, base+half, base+half+2)
	}
	if right {
		ret = append(ret, base+containerWidth-3, base+containerWidth-1)
	}
	return ret
}

// sets 6667 bits per true.
func bitCont(num int, left, mid, right bool) []uint64 {
	ret := make([]uint64, 0)
	base := containerWidth * uint64(num)
	if left {
		for i := uint64(0); i < 20001; i += 3 {
			ret = append(ret, base+i)
		}
	}
	if mid {
		for i := uint64(21000); i < 41001; i += 3 {
			ret = append(ret, base+i)
		}
	}
	if right {
		for i := uint64(45537); i <= 65535; i += 3 {
			ret = append(ret, base+i)
		}
	}
	return ret
}

func bitmapVariousContainers() *Bitmap {
	bits := make([]uint64, 0)
	bits = append(bits, rleCont(0, true, true, true)...)
	bits = append(bits, rleCont(1, true, true, true)...)
	bits = append(bits, arrCont(2, true, true, true)...)
	bits = append(bits, arrCont(3, true, true, true)...)
	bits = append(bits, bitCont(4, true, true, true)...)
	bits = append(bits, bitCont(5, true, true, true)...)
	bits = append(bits, rleCont(6, true, true, true)...)
	bits = append(bits, bitCont(7, true, true, true)...)
	bits = append(bits, arrCont(8, true, true, true)...)
	bits = append(bits, rleCont(9, true, true, true)...)
	bm := NewFileBitmap(bits...)
	bm.Optimize()
	return bm
}

///////////////////////////////////////////////////////////////////////////

func getFunctionName(i interface{}) string {
	x := runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
	y := strings.Split(x, ".")
	y = y[len(y)-1:]
	return y[0]
}

func TestContainerCombinations(t *testing.T) {

	cts := setupContainerTests()

	containerTypes := []byte{containerArray, containerBitmap, containerRun}

	// map used for a more descriptive print
	cm := map[byte]string{
		containerArray:  "array",
		containerBitmap: "bitmap",
		containerRun:    "run",
	}

	testOps := []testOp{
		// intersect
		{intersect, "empty", "empty", "empty"},
		{intersect, "empty", "full", "empty"},
		{intersect, "empty", "firstBitSet", "empty"},
		{intersect, "empty", "lastBitSet", "empty"},
		{intersect, "empty", "firstBitUnset", "empty"},
		{intersect, "empty", "lastBitUnset", "empty"},
		{intersect, "empty", "innerBitsSet", "empty"},
		{intersect, "empty", "outerBitsSet", "empty"},
		{intersect, "empty", "oddBitsSet", "empty"},
		{intersect, "empty", "evenBitsSet", "empty"},
		//
		{intersect, "full", "empty", "empty"},
		{intersect, "full", "full", "full"},
		{intersect, "full", "firstBitSet", "firstBitSet"},
		{intersect, "full", "lastBitSet", "lastBitSet"},
		{intersect, "full", "firstBitUnset", "firstBitUnset"},
		{intersect, "full", "lastBitUnset", "lastBitUnset"},
		{intersect, "full", "innerBitsSet", "innerBitsSet"},
		{intersect, "full", "outerBitsSet", "outerBitsSet"},
		{intersect, "full", "oddBitsSet", "oddBitsSet"},
		{intersect, "full", "evenBitsSet", "evenBitsSet"},
		//
		{intersect, "firstBitSet", "empty", "empty"},
		{intersect, "firstBitSet", "full", "firstBitSet"},
		{intersect, "firstBitSet", "firstBitSet", "firstBitSet"},
		{intersect, "firstBitSet", "lastBitSet", "empty"},
		{intersect, "firstBitSet", "firstBitUnset", "empty"},
		{intersect, "firstBitSet", "lastBitUnset", "firstBitSet"},
		{intersect, "firstBitSet", "innerBitsSet", "empty"},
		{intersect, "firstBitSet", "outerBitsSet", "firstBitSet"},
		{intersect, "firstBitSet", "oddBitsSet", "empty"},
		{intersect, "firstBitSet", "evenBitsSet", "firstBitSet"},
		//
		{intersect, "lastBitSet", "empty", "empty"},
		{intersect, "lastBitSet", "full", "lastBitSet"},
		{intersect, "lastBitSet", "firstBitSet", "empty"},
		{intersect, "lastBitSet", "lastBitSet", "lastBitSet"},
		{intersect, "lastBitSet", "firstBitUnset", "lastBitSet"},
		{intersect, "lastBitSet", "lastBitUnset", "empty"},
		{intersect, "lastBitSet", "innerBitsSet", "empty"},
		{intersect, "lastBitSet", "outerBitsSet", "lastBitSet"},
		{intersect, "lastBitSet", "oddBitsSet", "lastBitSet"},
		{intersect, "lastBitSet", "evenBitsSet", "empty"},
		//
		{intersect, "firstBitUnset", "empty", "empty"},
		{intersect, "firstBitUnset", "full", "firstBitUnset"},
		{intersect, "firstBitUnset", "firstBitSet", "empty"},
		{intersect, "firstBitUnset", "lastBitSet", "lastBitSet"},
		{intersect, "firstBitUnset", "firstBitUnset", "firstBitUnset"},
		{intersect, "firstBitUnset", "lastBitUnset", "innerBitsSet"},
		{intersect, "firstBitUnset", "innerBitsSet", "innerBitsSet"},
		{intersect, "firstBitUnset", "outerBitsSet", "lastBitSet"},
		{intersect, "firstBitUnset", "oddBitsSet", "oddBitsSet"},
		//{intersect, "firstBitUnset", "evenBitsSet", ""},
		//
		{intersect, "lastBitUnset", "empty", "empty"},
		{intersect, "lastBitUnset", "full", "lastBitUnset"},
		{intersect, "lastBitUnset", "firstBitSet", "firstBitSet"},
		{intersect, "lastBitUnset", "lastBitSet", "empty"},
		{intersect, "lastBitUnset", "firstBitUnset", "innerBitsSet"},
		{intersect, "lastBitUnset", "lastBitUnset", "lastBitUnset"},
		{intersect, "lastBitUnset", "innerBitsSet", "innerBitsSet"},
		{intersect, "lastBitUnset", "outerBitsSet", "firstBitSet"},
		//{intersect, "lastBitUnset", "oddBitsSet", ""},
		{intersect, "lastBitUnset", "evenBitsSet", "evenBitsSet"},
		//
		{intersect, "innerBitsSet", "empty", "empty"},
		{intersect, "innerBitsSet", "full", "innerBitsSet"},
		{intersect, "innerBitsSet", "firstBitSet", "empty"},
		{intersect, "innerBitsSet", "lastBitSet", "empty"},
		{intersect, "innerBitsSet", "firstBitUnset", "innerBitsSet"},
		{intersect, "innerBitsSet", "lastBitUnset", "innerBitsSet"},
		{intersect, "innerBitsSet", "innerBitsSet", "innerBitsSet"},
		{intersect, "innerBitsSet", "outerBitsSet", "empty"},
		//{intersect, "innerBitsSet", "oddBitsSet", ""},
		//{intersect, "innerBitsSet", "evenBitsSet", ""},
		//
		{intersect, "outerBitsSet", "empty", "empty"},
		{intersect, "outerBitsSet", "full", "outerBitsSet"},
		{intersect, "outerBitsSet", "firstBitSet", "firstBitSet"},
		{intersect, "outerBitsSet", "lastBitSet", "lastBitSet"},
		{intersect, "outerBitsSet", "firstBitUnset", "lastBitSet"},
		{intersect, "outerBitsSet", "lastBitUnset", "firstBitSet"},
		{intersect, "outerBitsSet", "innerBitsSet", "empty"},
		{intersect, "outerBitsSet", "outerBitsSet", "outerBitsSet"},
		{intersect, "outerBitsSet", "oddBitsSet", "lastBitSet"},
		{intersect, "outerBitsSet", "evenBitsSet", "firstBitSet"},
		//
		{intersect, "oddBitsSet", "empty", "empty"},
		{intersect, "oddBitsSet", "full", "oddBitsSet"},
		{intersect, "oddBitsSet", "firstBitSet", "empty"},
		{intersect, "oddBitsSet", "lastBitSet", "lastBitSet"},
		{intersect, "oddBitsSet", "firstBitUnset", "oddBitsSet"},
		//{intersect, "oddBitsSet", "lastBitUnset", ""},
		//{intersect, "oddBitsSet", "innerBitsSet", ""},
		{intersect, "oddBitsSet", "outerBitsSet", "lastBitSet"},
		{intersect, "oddBitsSet", "oddBitsSet", "oddBitsSet"},
		{intersect, "oddBitsSet", "evenBitsSet", "empty"},
		//
		{intersect, "evenBitsSet", "empty", "empty"},
		{intersect, "evenBitsSet", "full", "evenBitsSet"},
		{intersect, "evenBitsSet", "firstBitSet", "firstBitSet"},
		{intersect, "evenBitsSet", "lastBitSet", "empty"},
		//{intersect, "evenBitsSet", "firstBitUnset", ""},
		{intersect, "evenBitsSet", "lastBitUnset", "evenBitsSet"},
		//{intersect, "evenBitsSet", "innerBitsSet", ""},
		{intersect, "evenBitsSet", "outerBitsSet", "firstBitSet"},
		{intersect, "evenBitsSet", "oddBitsSet", "empty"},
		{intersect, "evenBitsSet", "evenBitsSet", "evenBitsSet"},

		// union
		{union, "empty", "empty", "empty"},
		{union, "empty", "full", "full"},
		{union, "empty", "firstBitSet", "firstBitSet"},
		{union, "empty", "lastBitSet", "lastBitSet"},
		{union, "empty", "firstBitUnset", "firstBitUnset"},
		{union, "empty", "lastBitUnset", "lastBitUnset"},
		{union, "empty", "innerBitsSet", "innerBitsSet"},
		{union, "empty", "outerBitsSet", "outerBitsSet"},
		{union, "empty", "oddBitsSet", "oddBitsSet"},
		{union, "empty", "evenBitsSet", "evenBitsSet"},
		//
		{union, "full", "empty", "full"},
		{union, "full", "full", "full"},
		{union, "full", "firstBitSet", "full"},
		{union, "full", "lastBitSet", "full"},
		{union, "full", "firstBitUnset", "full"},
		{union, "full", "lastBitUnset", "full"},
		{union, "full", "innerBitsSet", "full"},
		{union, "full", "outerBitsSet", "full"},
		{union, "full", "oddBitsSet", "full"},
		{union, "full", "evenBitsSet", "full"},
		//
		{union, "firstBitSet", "empty", "firstBitSet"},
		{union, "firstBitSet", "full", "full"},
		{union, "firstBitSet", "firstBitSet", "firstBitSet"},
		{union, "firstBitSet", "lastBitSet", "outerBitsSet"},
		{union, "firstBitSet", "firstBitUnset", "full"},
		{union, "firstBitSet", "lastBitUnset", "lastBitUnset"},
		{union, "firstBitSet", "innerBitsSet", "lastBitUnset"},
		{union, "firstBitSet", "outerBitsSet", "outerBitsSet"},
		//{union, "firstBitSet", "oddBitsSet", ""},
		{union, "firstBitSet", "evenBitsSet", "evenBitsSet"},
		//
		{union, "lastBitSet", "empty", "lastBitSet"},
		{union, "lastBitSet", "full", "full"},
		{union, "lastBitSet", "firstBitSet", "outerBitsSet"},
		{union, "lastBitSet", "lastBitSet", "lastBitSet"},
		{union, "lastBitSet", "firstBitUnset", "firstBitUnset"},
		{union, "lastBitSet", "lastBitUnset", "full"},
		{union, "lastBitSet", "innerBitsSet", "firstBitUnset"},
		{union, "lastBitSet", "outerBitsSet", "outerBitsSet"},
		{union, "lastBitSet", "oddBitsSet", "oddBitsSet"},
		//{union, "lastBitSet", "evenBitsSet", ""},
		//
		{union, "firstBitUnset", "empty", "firstBitUnset"},
		{union, "firstBitUnset", "full", "full"},
		{union, "firstBitUnset", "firstBitSet", "full"},
		{union, "firstBitUnset", "lastBitSet", "firstBitUnset"},
		{union, "firstBitUnset", "firstBitUnset", "firstBitUnset"},
		{union, "firstBitUnset", "lastBitUnset", "full"},
		{union, "firstBitUnset", "innerBitsSet", "firstBitUnset"},
		{union, "firstBitUnset", "outerBitsSet", "full"},
		{union, "firstBitUnset", "oddBitsSet", "firstBitUnset"},
		{union, "firstBitUnset", "evenBitsSet", "full"},
		//
		{union, "lastBitUnset", "empty", "lastBitUnset"},
		{union, "lastBitUnset", "full", "full"},
		{union, "lastBitUnset", "firstBitSet", "lastBitUnset"},
		{union, "lastBitUnset", "lastBitSet", "full"},
		{union, "lastBitUnset", "firstBitUnset", "full"},
		{union, "lastBitUnset", "lastBitUnset", "lastBitUnset"},
		{union, "lastBitUnset", "innerBitsSet", "lastBitUnset"},
		{union, "lastBitUnset", "outerBitsSet", "full"},
		{union, "lastBitUnset", "oddBitsSet", "full"},
		{union, "lastBitUnset", "evenBitsSet", "lastBitUnset"},
		//
		{union, "innerBitsSet", "empty", "innerBitsSet"},
		{union, "innerBitsSet", "full", "full"},
		{union, "innerBitsSet", "firstBitSet", "lastBitUnset"},
		{union, "innerBitsSet", "lastBitSet", "firstBitUnset"},
		{union, "innerBitsSet", "firstBitUnset", "firstBitUnset"},
		{union, "innerBitsSet", "lastBitUnset", "lastBitUnset"},
		{union, "innerBitsSet", "innerBitsSet", "innerBitsSet"},
		{union, "innerBitsSet", "outerBitsSet", "full"},
		{union, "innerBitsSet", "oddBitsSet", "firstBitUnset"},
		{union, "innerBitsSet", "evenBitsSet", "lastBitUnset"},
		//
		{union, "outerBitsSet", "empty", "outerBitsSet"},
		{union, "outerBitsSet", "full", "full"},
		{union, "outerBitsSet", "firstBitSet", "outerBitsSet"},
		{union, "outerBitsSet", "lastBitSet", "outerBitsSet"},
		{union, "outerBitsSet", "firstBitUnset", "full"},
		{union, "outerBitsSet", "lastBitUnset", "full"},
		{union, "outerBitsSet", "innerBitsSet", "full"},
		{union, "outerBitsSet", "outerBitsSet", "outerBitsSet"},
		//{union, "outerBitsSet", "oddBitsSet", ""},
		//{union, "outerBitsSet", "evenBitsSet", ""},
		//
		{union, "oddBitsSet", "empty", "oddBitsSet"},
		{union, "oddBitsSet", "full", "full"},
		//{union, "oddBitsSet", "firstBitSet", ""},
		{union, "oddBitsSet", "lastBitSet", "oddBitsSet"},
		{union, "oddBitsSet", "firstBitUnset", "firstBitUnset"},
		{union, "oddBitsSet", "lastBitUnset", "full"},
		{union, "oddBitsSet", "innerBitsSet", "firstBitUnset"},
		//{union, "oddBitsSet", "outerBitsSet", ""},
		{union, "oddBitsSet", "oddBitsSet", "oddBitsSet"},
		{union, "oddBitsSet", "evenBitsSet", "full"},
		//
		{union, "evenBitsSet", "empty", "evenBitsSet"},
		{union, "evenBitsSet", "full", "full"},
		{union, "evenBitsSet", "firstBitSet", "evenBitsSet"},
		//{union, "evenBitsSet", "lastBitSet", ""},
		{union, "evenBitsSet", "firstBitUnset", "full"},
		{union, "evenBitsSet", "lastBitUnset", "lastBitUnset"},
		{union, "evenBitsSet", "innerBitsSet", "lastBitUnset"},
		//{union, "evenBitsSet", "outerBitsSet", ""},
		{union, "evenBitsSet", "oddBitsSet", "full"},
		{union, "evenBitsSet", "evenBitsSet", "evenBitsSet"},

		// difference
		{difference, "empty", "empty", "empty"},
		{difference, "empty", "full", "empty"},
		{difference, "empty", "firstBitSet", "empty"},
		{difference, "empty", "lastBitSet", "empty"},
		{difference, "empty", "firstBitUnset", "empty"},
		{difference, "empty", "lastBitUnset", "empty"},
		{difference, "empty", "innerBitsSet", "empty"},
		{difference, "empty", "outerBitsSet", "empty"},
		{difference, "empty", "oddBitsSet", "empty"},
		{difference, "empty", "evenBitsSet", "empty"},
		//
		{difference, "full", "empty", "full"},
		{difference, "full", "full", "empty"},
		{difference, "full", "firstBitSet", "firstBitUnset"},
		{difference, "full", "lastBitSet", "lastBitUnset"},
		{difference, "full", "firstBitUnset", "firstBitSet"},
		{difference, "full", "lastBitUnset", "lastBitSet"},
		{difference, "full", "innerBitsSet", "outerBitsSet"},
		{difference, "full", "outerBitsSet", "innerBitsSet"},
		{difference, "full", "oddBitsSet", "evenBitsSet"},
		{difference, "full", "evenBitsSet", "oddBitsSet"},
		//
		{difference, "firstBitSet", "empty", "firstBitSet"},
		{difference, "firstBitSet", "full", "empty"},
		{difference, "firstBitSet", "firstBitSet", "empty"},
		{difference, "firstBitSet", "lastBitSet", "firstBitSet"},
		{difference, "firstBitSet", "firstBitUnset", "firstBitSet"},
		{difference, "firstBitSet", "lastBitUnset", "empty"},
		{difference, "firstBitSet", "innerBitsSet", "firstBitSet"},
		{difference, "firstBitSet", "outerBitsSet", "empty"},
		{difference, "firstBitSet", "oddBitsSet", "firstBitSet"},
		{difference, "firstBitSet", "evenBitsSet", "empty"},
		//
		{difference, "lastBitSet", "empty", "lastBitSet"},
		{difference, "lastBitSet", "full", "empty"},
		{difference, "lastBitSet", "firstBitSet", "lastBitSet"},
		{difference, "lastBitSet", "lastBitSet", "empty"},
		{difference, "lastBitSet", "firstBitUnset", "empty"},
		{difference, "lastBitSet", "lastBitUnset", "lastBitSet"},
		{difference, "lastBitSet", "innerBitsSet", "lastBitSet"},
		{difference, "lastBitSet", "outerBitsSet", "empty"},
		{difference, "lastBitSet", "oddBitsSet", "empty"},
		{difference, "lastBitSet", "evenBitsSet", "lastBitSet"},
		//
		{difference, "firstBitUnset", "empty", "firstBitUnset"},
		{difference, "firstBitUnset", "full", "empty"},
		{difference, "firstBitUnset", "firstBitSet", "firstBitUnset"},
		{difference, "firstBitUnset", "lastBitSet", "innerBitsSet"},
		{difference, "firstBitUnset", "firstBitUnset", "empty"},
		{difference, "firstBitUnset", "lastBitUnset", "lastBitSet"},
		{difference, "firstBitUnset", "innerBitsSet", "lastBitSet"},
		{difference, "firstBitUnset", "outerBitsSet", "innerBitsSet"},
		//{difference, "firstBitUnset", "oddBitsSet", ""},
		{difference, "firstBitUnset", "evenBitsSet", "oddBitsSet"},
		//
		{difference, "lastBitUnset", "empty", "lastBitUnset"},
		{difference, "lastBitUnset", "full", "empty"},
		{difference, "lastBitUnset", "firstBitSet", "innerBitsSet"},
		{difference, "lastBitUnset", "lastBitSet", "lastBitUnset"},
		{difference, "lastBitUnset", "firstBitUnset", "firstBitSet"},
		{difference, "lastBitUnset", "lastBitUnset", "empty"},
		{difference, "lastBitUnset", "innerBitsSet", "firstBitSet"},
		{difference, "lastBitUnset", "outerBitsSet", "innerBitsSet"},
		{difference, "lastBitUnset", "oddBitsSet", "evenBitsSet"},
		//{difference, "lastBitUnset", "evenBitsSet", ""},
		//
		{difference, "innerBitsSet", "empty", "innerBitsSet"},
		{difference, "innerBitsSet", "full", "empty"},
		{difference, "innerBitsSet", "firstBitSet", "innerBitsSet"},
		{difference, "innerBitsSet", "lastBitSet", "innerBitsSet"},
		{difference, "innerBitsSet", "firstBitUnset", "empty"},
		{difference, "innerBitsSet", "lastBitUnset", "empty"},
		{difference, "innerBitsSet", "innerBitsSet", "empty"},
		{difference, "innerBitsSet", "outerBitsSet", "innerBitsSet"},
		//{difference, "innerBitsSet", "oddBitsSet", ""},
		//{difference, "innerBitsSet", "evenBitsSet", ""},
		//
		{difference, "outerBitsSet", "empty", "outerBitsSet"},
		{difference, "outerBitsSet", "full", "empty"},
		{difference, "outerBitsSet", "firstBitSet", "lastBitSet"},
		{difference, "outerBitsSet", "lastBitSet", "firstBitSet"},
		{difference, "outerBitsSet", "firstBitUnset", "firstBitSet"},
		{difference, "outerBitsSet", "lastBitUnset", "lastBitSet"},
		{difference, "outerBitsSet", "innerBitsSet", "outerBitsSet"},
		{difference, "outerBitsSet", "outerBitsSet", "empty"},
		{difference, "outerBitsSet", "oddBitsSet", "firstBitSet"},
		{difference, "outerBitsSet", "evenBitsSet", "lastBitSet"},
		//
		{difference, "oddBitsSet", "empty", "oddBitsSet"},
		{difference, "oddBitsSet", "full", "empty"},
		{difference, "oddBitsSet", "firstBitSet", "oddBitsSet"},
		//{difference, "oddBitsSet", "lastBitSet", ""},
		{difference, "oddBitsSet", "firstBitUnset", "empty"},
		{difference, "oddBitsSet", "lastBitUnset", "lastBitSet"},
		{difference, "oddBitsSet", "innerBitsSet", "lastBitSet"},
		//{difference, "oddBitsSet", "outerBitsSet", ""},
		{difference, "oddBitsSet", "oddBitsSet", "empty"},
		{difference, "oddBitsSet", "evenBitsSet", "oddBitsSet"},
		//
		{difference, "evenBitsSet", "empty", "evenBitsSet"},
		{difference, "evenBitsSet", "full", "empty"},
		//{difference, "evenBitsSet", "firstBitSet", ""},
		{difference, "evenBitsSet", "lastBitSet", "evenBitsSet"},
		{difference, "evenBitsSet", "firstBitUnset", "firstBitSet"},
		{difference, "evenBitsSet", "lastBitUnset", "empty"},
		{difference, "evenBitsSet", "innerBitsSet", "firstBitSet"},
		//{difference, "evenBitsSet", "outerBitsSet", ""},
		{difference, "evenBitsSet", "oddBitsSet", "evenBitsSet"},
		{difference, "evenBitsSet", "evenBitsSet", "empty"},

		// xor
		{xor, "empty", "empty", "empty"},
		{xor, "empty", "full", "full"},
		{xor, "empty", "firstBitSet", "firstBitSet"},
		{xor, "empty", "lastBitSet", "lastBitSet"},
		{xor, "empty", "firstBitUnset", "firstBitUnset"},
		{xor, "empty", "lastBitUnset", "lastBitUnset"},
		{xor, "empty", "innerBitsSet", "innerBitsSet"},
		{xor, "empty", "outerBitsSet", "outerBitsSet"},
		{xor, "empty", "oddBitsSet", "oddBitsSet"},
		{xor, "empty", "evenBitsSet", "evenBitsSet"},
		//
		{xor, "full", "empty", "full"},
		{xor, "full", "full", "empty"},
		{xor, "full", "firstBitSet", "firstBitUnset"},
		{xor, "full", "lastBitSet", "lastBitUnset"},
		{xor, "full", "firstBitUnset", "firstBitSet"},
		{xor, "full", "lastBitUnset", "lastBitSet"},
		{xor, "full", "innerBitsSet", "outerBitsSet"},
		{xor, "full", "outerBitsSet", "innerBitsSet"},
		{xor, "full", "oddBitsSet", "evenBitsSet"},
		{xor, "full", "evenBitsSet", "oddBitsSet"},
		//
		{xor, "firstBitSet", "empty", "firstBitSet"},
		{xor, "firstBitSet", "full", "firstBitUnset"},
		{xor, "firstBitSet", "firstBitSet", "empty"},
		{xor, "firstBitSet", "lastBitSet", "outerBitsSet"},
		{xor, "firstBitSet", "firstBitUnset", "full"},
		{xor, "firstBitSet", "lastBitUnset", "innerBitsSet"},
		{xor, "firstBitSet", "innerBitsSet", "lastBitUnset"},
		{xor, "firstBitSet", "outerBitsSet", "lastBitSet"},
		//{xor, "firstBitSet", "oddBitsSet", ""},
		//{xor, "firstBitSet", "evenBitsSet", ""},
		//
		{xor, "lastBitSet", "empty", "lastBitSet"},
		{xor, "lastBitSet", "full", "lastBitUnset"},
		{xor, "lastBitSet", "firstBitSet", "outerBitsSet"},
		{xor, "lastBitSet", "lastBitSet", "empty"},
		{xor, "lastBitSet", "firstBitUnset", "innerBitsSet"},
		{xor, "lastBitSet", "lastBitUnset", "full"},
		{xor, "lastBitSet", "innerBitsSet", "firstBitUnset"},
		{xor, "lastBitSet", "outerBitsSet", "firstBitSet"},
		//{xor, "lastBitSet", "oddBitsSet", ""},
		//{xor, "lastBitSet", "evenBitsSet", ""},
		//
		{xor, "firstBitUnset", "empty", "firstBitUnset"},
		{xor, "firstBitUnset", "full", "firstBitSet"},
		{xor, "firstBitUnset", "firstBitSet", "full"},
		{xor, "firstBitUnset", "lastBitSet", "innerBitsSet"},
		{xor, "firstBitUnset", "firstBitUnset", "empty"},
		{xor, "firstBitUnset", "lastBitUnset", "outerBitsSet"},
		{xor, "firstBitUnset", "innerBitsSet", "lastBitSet"},
		{xor, "firstBitUnset", "outerBitsSet", "lastBitUnset"},
		//{xor, "firstBitUnset", "oddBitsSet", ""},
		//{xor, "firstBitUnset", "evenBitsSet", ""},
		//
		{xor, "lastBitUnset", "empty", "lastBitUnset"},
		{xor, "lastBitUnset", "full", "lastBitSet"},
		{xor, "lastBitUnset", "firstBitSet", "innerBitsSet"},
		{xor, "lastBitUnset", "lastBitSet", "full"},
		{xor, "lastBitUnset", "firstBitUnset", "outerBitsSet"},
		{xor, "lastBitUnset", "lastBitUnset", "empty"},
		{xor, "lastBitUnset", "innerBitsSet", "firstBitSet"},
		{xor, "lastBitUnset", "outerBitsSet", "firstBitUnset"},
		//{xor, "lastBitUnset", "oddBitsSet", ""},
		//{xor, "lastBitUnset", "evenBitsSet", ""},
		//
		{xor, "innerBitsSet", "empty", "innerBitsSet"},
		{xor, "innerBitsSet", "full", "outerBitsSet"},
		{xor, "innerBitsSet", "firstBitSet", "lastBitUnset"},
		{xor, "innerBitsSet", "lastBitSet", "firstBitUnset"},
		{xor, "innerBitsSet", "firstBitUnset", "lastBitSet"},
		{xor, "innerBitsSet", "lastBitUnset", "firstBitSet"},
		{xor, "innerBitsSet", "innerBitsSet", "empty"},
		{xor, "innerBitsSet", "outerBitsSet", "full"},
		//{xor, "innerBitsSet", "oddBitsSet", ""},
		//{xor, "innerBitsSet", "evenBitsSet", ""},
		//
		{xor, "outerBitsSet", "empty", "outerBitsSet"},
		{xor, "outerBitsSet", "full", "innerBitsSet"},
		{xor, "outerBitsSet", "firstBitSet", "lastBitSet"},
		{xor, "outerBitsSet", "lastBitSet", "firstBitSet"},
		{xor, "outerBitsSet", "firstBitUnset", "lastBitUnset"},
		{xor, "outerBitsSet", "lastBitUnset", "firstBitUnset"},
		{xor, "outerBitsSet", "innerBitsSet", "full"},
		{xor, "outerBitsSet", "outerBitsSet", "empty"},
		//{xor, "outerBitsSet", "oddBitsSet", ""},
		//{xor, "outerBitsSet", "evenBitsSet", ""},
		//
		{xor, "oddBitsSet", "empty", "oddBitsSet"},
		{xor, "oddBitsSet", "full", "evenBitsSet"},
		//{xor, "oddBitsSet", "firstBitSet", ""},
		//{xor, "oddBitsSet", "lastBitSet", ""},
		//{xor, "oddBitsSet", "firstBitUnset", ""},
		//{xor, "oddBitsSet", "lastBitUnset", ""},
		//{xor, "oddBitsSet", "innerBitsSet", ""},
		//{xor, "oddBitsSet", "outerBitsSet", ""},
		{xor, "oddBitsSet", "oddBitsSet", "empty"},
		{xor, "oddBitsSet", "evenBitsSet", "full"},
		//
		{xor, "evenBitsSet", "empty", "evenBitsSet"},
		{xor, "evenBitsSet", "full", "oddBitsSet"},
		//{xor, "evenBitsSet", "firstBitSet", ""},
		//{xor, "evenBitsSet", "lastBitSet", ""},
		//{xor, "evenBitsSet", "firstBitUnset", ""},
		//{xor, "evenBitsSet", "lastBitUnset", ""},
		//{xor, "evenBitsSet", "innerBitsSet", ""},
		//{xor, "evenBitsSet", "outerBitsSet", ""},
		{xor, "evenBitsSet", "oddBitsSet", "full"},
		{xor, "evenBitsSet", "evenBitsSet", "empty"},

		// flip
		{flip, "empty", "", "full"},
		{flip, "full", "", "empty"},
		{flip, "firstBitSet", "", "firstBitUnset"},
		{flip, "lastBitSet", "", "lastBitUnset"},
		{flip, "firstBitUnset", "", "firstBitSet"},
		{flip, "lastBitUnset", "", "lastBitSet"},
		{flip, "innerBitsSet", "", "outerBitsSet"},
		{flip, "outerBitsSet", "", "innerBitsSet"},
		{flip, "oddBitsSet", "", "evenBitsSet"},
		{flip, "evenBitsSet", "", "oddBitsSet"},
	}
	for _, testOp := range testOps {
		for _, x := range containerTypes {
			for _, y := range containerTypes {
				desc := fmt.Sprintf("%s(%s/%s, %s/%s)", getFunctionName(testOp.f), cm[x], testOp.x, cm[y], testOp.y)
				ret := runContainerFunc(testOp.f, cts[x][testOp.x], cts[y][testOp.y])
				exp := testOp.exp

				// Convert to all container types and check result.
				for _, ct := range containerTypes {
					clone := ret.Clone()
					if ct == containerArray {
						if clone.isBitmap() {
							clone.bitmapToArray()
						} else if clone.isRun() {
							clone.runToArray()
						}
						if clone.n != cts[ct][exp].n {
							t.Fatalf("test %s expected array n=%d, but got n=%d", desc, cts[ct][exp].n, clone.n)
						}
						// Because xorRunRun resulting in an empty container returns an array container with a
						// nil slice array, then we need to check len() on array first (look for 0).
						if !(len(clone.array) == 0 && len(cts[ct][exp].array) == 0) && !reflect.DeepEqual(clone.array, cts[ct][exp].array) {
							t.Fatalf("test %s expected array %X, but got %X", desc, cts[ct][exp].array, clone.array)
						}
					} else if ct == containerBitmap {
						if clone.isArray() {
							clone.arrayToBitmap()
						} else if clone.isRun() {
							clone.runToBitmap()
						}
						if clone.n != cts[ct][exp].n {
							t.Fatalf("test %s expected bitmap n=%d, but got n=%d", desc, cts[ct][exp].n, clone.n)
						}
						if !reflect.DeepEqual(clone.bitmap, cts[ct][exp].bitmap) {
							t.Fatalf("test %s expected bitmap %X, but got %X", desc, cts[ct][exp].bitmap, clone.bitmap)
						}
					} else if ct == containerRun {
						if clone.isArray() {
							clone.arrayToRun()
						} else if clone.isBitmap() {
							clone.bitmapToRun()
						}
						if clone.n != cts[ct][exp].n {
							t.Fatalf("test %s expected runs n=%d, but got n=%d", desc, cts[ct][exp].n, clone.n)
						}
						if !reflect.DeepEqual(clone.runs, cts[ct][exp].runs) {
							t.Fatalf("test %s expected runs %X, but got %X", desc, cts[ct][exp].runs, clone.runs)
						}
					}
				}
			}
		}
	}
}

//func getFunc(func(a, b *container) *container, m, n *container) *container {
func runContainerFunc(f interface{}, c ...*Container) *Container {
	switch f := f.(type) {
	case func(*Container) *Container:
		return f(c[0])
	case func(*Container, *Container) *Container:
		return f(c[0], c[1])
	}
	return nil
}

func TestUnmarshalOfficialRoaring(t *testing.T) {
	//generated serialize image from java(clojure) with arrays
	rbContainerWithTwoArrays, _ := hex.DecodeString("3A300000020000000000020001000000180000001E0000000100020003000100")
	bm := NewBitmap()
	er := bm.UnmarshalBinary(rbContainerWithTwoArrays)
	if er != nil {
		t.Fatalf("UnmarshalOfficialRoaring %s", er)
	}
	if bm.Count() != 4 {
		t.Fatalf("unexpected bitmap %v expected bits [1 2 3 65537]", bm.Slice())
	}
	//generated serialize image from java(clojure) with a run and array
	rbContainerWithRLEandArray, _ := hex.DecodeString("3B3001000100000900010000000100010009000100")
	bm = NewBitmap()
	er = bm.UnmarshalBinary(rbContainerWithRLEandArray)
	if er != nil {
		t.Fatalf("UnmarshalOfficialRoaring %s", er)
	}
	if bm.Count() != 11 {
		t.Fatalf("unexpected bitmap %v expected bits [1 2 3 4 5 6 7 8 9 10 65537]", bm.Slice())
	}
	//had to use an external file because emacs was barfing on the long line :()
	_bitmap_array_container, _ := ioutil.ReadFile("testdata/bitmapcontainer.roaringbitmap")
	bm = NewBitmap()
	er = bm.UnmarshalBinary(_bitmap_array_container)
	if er != nil {
		t.Fatalf("UnmarshalOfficialRoaring %s", er)
	}
	if bm.Count() != 10000 {
		t.Fatalf("expecting X got %d", bm.Count())
	}

}

func BenchmarkUnionBitmapBitmapInPlace(b *testing.B) {
	b1 := newTestBitmapContainer()
	b2 := newTestBitmapContainer()
	for n := 0; n < b.N; n++ {
		unionBitmapBitmapInPlace(b1, b2)
	}
}

func BenchmarkBitmapRepair(b *testing.B) {
	b1 := newTestBitmapContainer()
	for n := 0; n < b.N; n++ {
		b1.bitmapRepair()
	}
}

func newTestBitmapContainer() *Container {
	var (
		buf       = make([]uint64, bitmapN)
		ob        = buf[:bitmapN]
		container = &Container{
			bitmap:        ob,
			n:             0,
			containerType: containerBitmap,
		}
	)
	return container
}

/*
// This function exercises an arcane edge case in dead code.
// It doesn't need to be run right now.
func TestEquals(t *testing.T) {
	bma := NewBitmap()
	bmr := NewBitmap()
	for i := uint64(0); i < 30; i++ {
		bma.Add(i)
		bmr.Add(i)
	}
	bmr.Optimize()
	bmi := bma.Intersect(bmr)
	err := bitmapsEqual(bmi, bma)
	if err != nil {
		t.Fatalf("expected intersection to equal array")
	}
	err = bitmapsEqual(bmi, bmr)
	if err != nil {
		t.Fatalf("expected intersection to equal run")
	}
}
*/
func TestShiftArray(t *testing.T) {
	a := &Container{
		containerType: containerArray,
	}
	tests := []struct {
		array []uint16
		exp   []uint16
	}{
		{
			array: []uint16{1},
			exp:   []uint16{2},
		},
		{
			array: []uint16{},
			exp:   []uint16{},
		},
		{
			array: []uint16{1, 2, 3, 4, 5, 11, 12},
			exp:   []uint16{2, 3, 4, 5, 6, 12, 13},
		},
		{
			array: []uint16{65535},
			exp:   []uint16{},
		},
	}

	for i, test := range tests {
		a.array = test.array
		a.n = int32(len(a.array))
		ret1, _ := shift(a)      // test generic shift function
		ret2, _ := shiftArray(a) // test array-specific shift function
		if !reflect.DeepEqual(ret1.array, test.exp) {
			t.Fatalf("test #%v shift() expected %v, but got %v", i, test.exp, ret1.array)
		} else if !reflect.DeepEqual(ret2.array, test.exp) {
			t.Fatalf("test #%v shiftArray() expected %v, but got %v", i, test.exp, ret2.array)
		}
	}
}

func TestShiftBitmap(t *testing.T) {
	a := &Container{
		containerType: containerBitmap,
	}
	tests := []struct {
		bitmap []uint64
		exp    []uint64
	}{
		{
			bitmap: bitmapFirstBitSet(),
			exp:    bitmapSecondBitSet(),
		},
		{
			bitmap: bitmapLastBitSet(),
			exp:    bitmapEmpty(),
		},
		{
			bitmap: bitmapLastBitFirstRowSet(),
			exp:    bitmapFirstBitSecoundRowSet(),
		},
	}

	for i, test := range tests {
		a.bitmap = test.bitmap
		a.n = 1
		ret1, _ := shift(a)       // test generic shift function
		ret2, _ := shiftBitmap(a) // test bitmap-specific shift function
		if !reflect.DeepEqual(ret1.bitmap, test.exp) {
			t.Fatalf("test #%v shift() expected %v, but got %v", i, test.exp, ret1.bitmap)
		} else if !reflect.DeepEqual(ret2.bitmap, test.exp) {
			t.Fatalf("test #%v shiftBitmap() expected %v, but got %v", i, test.exp, ret2.bitmap)
		}
	}
}
func TestShiftRun(t *testing.T) {
	a := &Container{
		containerType: containerRun,
	}

	tests := []struct {
		runs  []interval16
		n     int32
		en    int32
		exp   []interval16
		carry bool
	}{
		{
			runs:  []interval16{{start: 5, last: 10}},
			n:     5,
			en:    5,
			exp:   []interval16{{start: 6, last: 11}},
			carry: false,
		},
		{
			runs:  []interval16{{start: 5, last: 65535}},
			n:     65530,
			en:    65529,
			exp:   []interval16{{start: 6, last: 65535}},
			carry: true,
		},
		{
			runs:  []interval16{{start: 65535, last: 65535}},
			n:     1,
			en:    0,
			exp:   []interval16{},
			carry: true,
		},
	}

	for i, test := range tests {
		a.runs = test.runs
		a.n = test.n
		ret1, c1 := shift(a)    // test generic shift function
		ret2, c2 := shiftRun(a) // test run-specific shift function
		if !reflect.DeepEqual(ret1.runs, test.exp) && c1 == test.carry && ret1.n == test.en {
			t.Fatalf("test #%v shift() expected %v, but got %v %d", i, test.exp, ret1.runs, ret1.n)
		} else if !reflect.DeepEqual(ret2.runs, test.exp) && c2 == test.carry && ret2.n == test.en {
			t.Fatalf("test #%v shiftRun() expected %v, but got %v %d", i, test.exp, ret2.runs, ret2.n)
		}
	}
}
