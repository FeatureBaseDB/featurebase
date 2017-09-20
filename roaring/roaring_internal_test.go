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
	"fmt"
	"reflect"
	"testing"
)

// String produces a human viewable string of the contents.
func (iv interval16) String() string {
	return fmt.Sprintf("[%d, %d]", iv.start, iv.last)
}

func (c *container) String() string {
	return fmt.Sprintf("<%s container  n=%d, array[%d], runs[%d], bitmap[%d]> type:%d", c.info().Type, c.n, len(c.array), len(c.runs), len(c.bitmap), c.container_type)
}

func TestRunAppendInterval(t *testing.T) {
	a := container{container_type: ContainerRun}
	tests := []struct {
		base []interval16
		app  interval16
		exp  int
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
	c := container{runs: make([]interval16, 0), container_type: ContainerRun}
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
	c := container{runs: make([]interval16, 0), container_type: ContainerRun}
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
	c := container{runs: make([]interval16, 0), container_type: ContainerRun}
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
	c.add(18)
	c.add(19)

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
}

func TestRunContains(t *testing.T) {
	c := container{runs: make([]interval16, 0), container_type: ContainerRun}
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
	c := container{container_type: ContainerBitmap}
	tests := []struct {
		start  int
		end    int
		bitmap []uint64
		exp    int
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
	a, b := &container{}, &container{}
	a.container_type = ContainerBitmap
	a.bitmap = getFullBitmap()
	a.n = maxContainerVal + 1

	b.container_type = ContainerBitmap
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
	if res.n != res.count() || res.n != maxContainerVal+1 || res.n != int(n) {
		t.Fatalf("test #3 intersectCountRunRun fail orig: %v new: %v exp: %v", res.n, res.count(), maxContainerVal+1)
	}
}

func TestIntersectionCountArrayBitmap2(t *testing.T) {
	a, b := &container{}, &container{}
	tests := []struct {
		array  []uint16
		bitmap []uint64
		exp    int
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
		a.container_type = ContainerArray
		b.bitmap = test.bitmap
		b.container_type = ContainerBitmap
		ret1 := int(intersectionCountArrayBitmapOld(a, b))
		ret2 := intersectionCountArrayBitmap(a, b)
		if ret1 != ret2 || ret2 != test.exp {
			t.Fatalf("test #%v intersectCountArrayBitmap fail orig: %v new: %v exp: %v", i, ret1, ret2, test.exp)
		}
	}
}

func TestRunRemove(t *testing.T) {
	c := container{runs: []interval16{{start: 2, last: 10}, {start: 12, last: 13}, {start: 15, last: 16}}, container_type: ContainerRun}
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
	c := container{runs: []interval16{{start: 2, last: 10}, {start: 12, last: 13}, {start: 15, last: 16}}, container_type: ContainerRun}
	max := c.max()
	if max != 16 {
		t.Fatalf("max for %v should be 16", c.runs)
	}

	c = container{runs: []interval16{}}
	max = c.max()
	if max != 0 {
		t.Fatalf("max for %v should be 0", c.runs)
	}
}

func TestIntersectionCountArrayRun(t *testing.T) {
	a := &container{array: []uint16{1, 5, 10, 11, 12}}
	b := &container{runs: []interval16{{start: 2, last: 10}, {start: 12, last: 13}, {start: 15, last: 16}}}

	ret := intersectionCountArrayRun(a, b)
	if ret != 3 {
		t.Fatalf("count of %v with %v should be 3, but got %v", a.array, b.runs, ret)
	}
}

func TestIntersectionCountBitmapRun(t *testing.T) {
	a := &container{bitmap: []uint64{0x8000000000000000}}
	b := &container{runs: []interval16{{start: 63, last: 64}}}

	ret := intersectionCountBitmapRun(a, b)
	if ret != 1 {
		t.Fatalf("count of %v with %v should be 1, but got %v", a.bitmap, b.runs, ret)
	}

	a = &container{bitmap: []uint64{0xF0000001, 0xFF00000000000000, 0xFF000000000000F0, 0x0F0000}}
	b = &container{runs: []interval16{{start: 29, last: 31}, {start: 125, last: 134}, {start: 191, last: 197}, {start: 200, last: 300}}}

	ret = intersectionCountBitmapRun(a, b)
	if ret != 14 {
		t.Fatalf("count of %v with %v should be 14, but got %v", a.bitmap, b.runs, ret)
	}
}

func TestIntersectionCountRunRun(t *testing.T) {
	a := &container{}
	b := &container{}
	tests := []struct {
		aruns []interval16
		bruns []interval16
		exp   int
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
		a.runs = test.aruns
		b.runs = test.bruns
		ret := intersectionCountRunRun(a, b)
		if ret != test.exp {
			t.Fatalf("test #%v failed intersecting %v with %v should be %v, but got %v", i, test.aruns, test.bruns, test.exp, ret)
		}
	}
}

func TestIntersectArrayRun(t *testing.T) {
	a := &container{}
	b := &container{}
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
		a.array = test.array
		b.runs = test.runs
		ret := intersectArrayRun(a, b)
		if !reflect.DeepEqual(ret.array, test.exp) {
			t.Fatalf("test #%v expected %v, but got %v", i, test.exp, ret.array)
		}
	}
}

func TestIntersectRunRun(t *testing.T) {
	a := &container{}
	b := &container{}
	tests := []struct {
		aruns []interval16
		bruns []interval16
		exp   []interval16
		expN  int
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
	a := &container{bitmap: make([]uint64, bitmapN)}
	b := &container{}
	tests := []struct {
		bitmap []uint64
		runs   []interval16
		exp    []uint64
		expN   int
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
		for i, v := range test.bitmap {
			a.bitmap[i] = v
		}
		b.runs = test.runs
		b.n = 4097 // ;)
		exp := make([]uint64, bitmapN)
		for i, v := range test.exp {
			exp[i] = v
		}
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
	a := &container{bitmap: make([]uint64, bitmapN)}
	b := &container{}
	tests := []struct {
		bitmap []uint64
		runs   []interval16
		exp    []uint16
		expN   int
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
		for i, v := range test.bitmap {
			a.bitmap[i] = v
		}
		b.runs = test.runs
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
	a := &container{}
	b := &container{}

	a.runs = []interval16{{start: 5, last: 10}}
	a.container_type = ContainerRun
	a.n = 6

	b.array = []uint16{1, 4, 5, 7, 10, 11, 12}
	b.container_type = ContainerArray
	b.n = 7
	res := union(a, b)
	if !reflect.DeepEqual(res.array, []uint16{1, 4, 5, 6, 7, 8, 9, 10, 11, 12}) {
		t.Fatalf("test #1 expected %v, but got %v", []uint16{1, 4, 5, 6, 7, 8, 9, 10, 11, 12}, res.array)
	}
	res = union(b, a)
	if !reflect.DeepEqual(res.array, []uint16{1, 4, 5, 6, 7, 8, 9, 10, 11, 12}) {
		t.Fatalf("test #2 expected %v, but got %v", []uint16{1, 4, 5, 6, 7, 8, 9, 10, 11, 12}, res.array)
	}

	res = union(a, a)
	if !reflect.DeepEqual(res.runs, []interval16{{start: 5, last: 10}}) {
		t.Fatalf("test #3 expected %v, but got %v", []interval16{{start: 5, last: 10}}, res.runs)
	}

	c := &container{bitmap: make([]uint64, bitmapN)}
	c.bitmap[0] = uint64(0x3)
	c.n = 2
	c.container_type = ContainerBitmap

	expected := make([]uint64, bitmapN)
	expected[0] = uint64(2019)

	res = union(c, a)
	if !reflect.DeepEqual(res.bitmap, expected) {
		t.Fatalf("test #4 expected %v, but got %v", []uint64{2019}, res.bitmap)
	}
	res = union(a, c)
	if !reflect.DeepEqual(res.bitmap, expected) {
		t.Fatalf("test #5 expected %v, but got %v", []uint64{2019}, res.bitmap)
	}
	res = union(b, c)
	res.bitmapToArray() //just added so test setup was easier
	if !reflect.DeepEqual(res.array, []uint16{0, 1, 4, 5, 7, 10, 11, 12}) {
		t.Fatalf("test #6 expected %v, but got %v", []uint16{0, 1, 4, 5, 7, 10, 11, 12}, res.array)
	}
	res = union(c, b)
	res.bitmapToArray() //just added so test setup was easier
	if !reflect.DeepEqual(res.array, []uint16{0, 1, 4, 5, 7, 10, 11, 12}) {
		t.Fatalf("test #6 expected %v, but got %v", []uint16{0, 1, 4, 5, 7, 10, 11, 12}, res.array)
	}

}
func TestIntersectMixed(t *testing.T) {
	a := &container{}
	b := &container{}
	c := &container{}

	a.runs = []interval16{{start: 5, last: 10}}
	a.n = 6
	a.container_type = ContainerRun
	b.array = []uint16{1, 4, 5, 7, 10, 11, 12}
	b.n = 7
	b.container_type = ContainerArray
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
	c.container_type = ContainerBitmap

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
	a := &container{}
	b := &container{}
	c := &container{}
	d := &container{}

	a.runs = []interval16{{start: 5, last: 10}}
	a.n = a.runCountRange(0, 100)
	a.container_type = ContainerRun

	b.array = []uint16{0, 2, 4, 6, 8, 10, 12}
	b.n = len(b.array)
	b.container_type = ContainerArray

	d.array = []uint16{1, 3, 5, 7, 9, 11, 12}
	d.n = len(d.array)
	d.container_type = ContainerArray

	res := difference(a, b)

	if !reflect.DeepEqual(res.array, []uint16{5, 7, 9}) {
		t.Fatalf("test #1 expected %v, but got %v", []uint16{5, 7, 9}, res.array)
	}

	res = difference(b, a)
	if !reflect.DeepEqual(res.array, []uint16{0, 2, 4, 12}) {
		t.Fatalf("test #2 expected %v, but got %v", []uint16{0, 2, 4, 12}, res.array)
	}

	res = difference(a, a)
	if !reflect.DeepEqual(res.runs, []interval16{}) {
		t.Fatalf("test #3 expected empty but got %v", res.runs)
	}

	c.bitmap = []uint64{0x64}
	c.n = c.countRange(0, 100)
	c.container_type = ContainerBitmap
	res = difference(c, a)
	if !reflect.DeepEqual(res.bitmap, []uint64{0x4}) {
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
	a := &container{}
	b := &container{}
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
		ret := unionRunRun(a, b)
		if !reflect.DeepEqual(ret.runs, test.exp) {
			t.Fatalf("test #%v expected %v, but got %v", i, test.exp, ret.runs)
		}
	}
}

func TestUnionArrayRun(t *testing.T) {
	a := &container{}
	b := &container{}
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
		ret := unionArrayRun(a, b)
		if !reflect.DeepEqual(ret.array, test.exp) {
			t.Fatalf("test #%v expected %v, but got %v", i, test.exp, ret.array)
		}
	}
}

func TestBitmapSetRange(t *testing.T) {
	c := &container{bitmap: make([]uint64, bitmapN)}
	tests := []struct {
		bitmap []uint64
		start  uint64
		last   uint64
		exp    []uint64
		expN   int
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
		for i, v := range test.bitmap {
			c.bitmap[i] = v
		}
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
	a := &container{}
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
		for i, v := range test.exp {
			exp[i] = v
		}

		a.array = test.array
		a.n = len(test.array)
		a.arrayToBitmap()
		if !reflect.DeepEqual(a.bitmap, exp) {
			t.Fatalf("test #%v expected %v, but got %v", i, exp, a.bitmap)
		}
	}
}

func TestBitmapToArray(t *testing.T) {
	a := &container{}
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
		n := 0
		for i, v := range test.bitmap {
			a.bitmap[i] = v
			n += int(popcount(v))
		}
		a.n = n

		a.bitmapToArray()
		if !reflect.DeepEqual(a.array, test.exp) {
			t.Fatalf("test #%v expected %v, but got %v", i, test.exp, a.array)
		}
	}
}

func TestRunToBitmap(t *testing.T) {
	a := &container{}
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
		a.n = n
		a.runToBitmap()
		if !reflect.DeepEqual(a.bitmap, exp) {
			t.Fatalf("test #%v expected %v, but got %v", i, exp, a.bitmap)
		}
	}
}

func getFullBitmap() []uint64 {
	x := make([]uint64, 1024, 1024)
	for i := range x {
		x[i] = uint64(0xFFFFFFFFFFFFFFFF)
	}
	return x

}

func TestBitmapToRun(t *testing.T) {
	a := &container{}
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
		a.n = n
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
	a := &container{}
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
		a.n = int(len(test.array))
		a.arrayToRun()
		if !reflect.DeepEqual(a.runs, test.exp) {
			t.Fatalf("test #%v expected %v, but got %v", i, test.exp, a.runs)
		}
	}
}

func TestRunToArray(t *testing.T) {
	a := &container{}
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
		a.n = len(test.exp)
		a.runToArray()
		if !reflect.DeepEqual(a.array, test.exp) {
			t.Fatalf("test #%v expected %v, but got %v", i, test.exp, a.array)
		}
	}
}

func TestBitmapZeroRange(t *testing.T) {
	c := &container{bitmap: make([]uint64, bitmapN)}
	tests := []struct {
		bitmap []uint64
		start  uint64
		last   uint64
		exp    []uint64
		expN   int
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
		for i, v := range test.bitmap {
			c.bitmap[i] = v
		}
		c.n = c.countRange(0, 65535)
		c.bitmapZeroRange(test.start, test.last+1)
		if !reflect.DeepEqual(c.bitmap[:len(test.exp)], test.exp) {
			t.Fatalf("test %#v expected %x, got %x", i, test.exp, c.bitmap[:len(test.bitmap)])
		}
		if test.expN != c.n {
			t.Fatalf("test #%v expected n to be %v, but got %v", i, test.expN, c.n)
		}
		for i, _ := range test.bitmap {
			c.bitmap[i] = 0
		}
	}

}

func TestUnionBitmapRun(t *testing.T) {
	a := &container{bitmap: make([]uint64, bitmapN)}
	b := &container{}
	tests := []struct {
		bitmap []uint64
		runs   []interval16
		exp    []uint64
		expN   int
	}{
		{
			bitmap: []uint64{2},
			runs:   []interval16{{start: 0, last: 0}, {start: 2, last: 5}, {start: 62, last: 71}, {start: 77, last: 78}},
			exp:    []uint64{0xC00000000000003F, 0x60FF},
			expN:   18,
		},
	}
	for i, test := range tests {
		for i, v := range test.bitmap {
			a.bitmap[i] = v
		}
		a.n = a.bitmapCountRange(0, 65535)
		b.runs = test.runs
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
		for i, _ := range test.bitmap {
			a.bitmap[i] = 0
		}
	}
}

func TestBitmapCountRuns(t *testing.T) {
	c := &container{bitmap: make([]uint64, bitmapN)}
	tests := []struct {
		bitmap []uint64
		exp    int
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
		for j, v := range test.bitmap {
			c.bitmap[j] = v
		}

		ret := c.bitmapCountRuns()
		if ret != test.exp {
			t.Fatalf("test #%v expected %v but got %v", i, test.exp, ret)
		}

		for j, _ := range test.bitmap {
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
	c := &container{}
	tests := []struct {
		array []uint16
		exp   int
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
	a := &container{}
	b := &container{}
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
		a.n = len(a.array)
		b.runs = test.runs
		b.n = b.runCountRange(0, 100)
		ret := differenceArrayRun(a, b)
		if !reflect.DeepEqual(ret.array, test.exp) {
			t.Fatalf("test #%v expected %v, but got %v", i, test.exp, ret.array)
		}
	}
}

func TestDifferenceRunArray(t *testing.T) {
	a := &container{}
	b := &container{}
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
	}
	for i, test := range tests {
		a.runs = test.runs
		a.n = a.runCountRange(0, 100)
		b.array = test.array
		b.n = len(b.array)
		ret := differenceRunArray(a, b)
		if !reflect.DeepEqual(ret.runs, test.exp) {
			t.Fatalf("test #%v expected %v, but got %v", i, test.exp, ret.runs)
		}
	}
}
func MakeBitmap(start []uint64) []uint64 {
	b := make([]uint64, bitmapN)
	for i, v := range start {
		b[i] = v

	}
	return b
}

func bitmap(positions ...uint64) []uint64 {
	ret := make([]uint64, bitmapN)
	for _, pos := range positions {
		ret[pos/64] |= 1 << (pos % 64)
	}
	return ret
}

func TestDifferenceRunBitmap(t *testing.T) {
	a := &container{}
	b := &container{bitmap: make([]uint64, bitmapN)}
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
	}
	for i, test := range tests {
		a.runs = test.runs
		a.n = a.runCountRange(0, 100)
		for i, v := range test.bitmap {
			b.bitmap[i] = v
		}
		b.n = b.bitmapCountRange(0, 100)
		ret := differenceRunBitmap(a, b)
		if !reflect.DeepEqual(ret.runs, test.exp) {
			t.Fatalf("test #%v expected %v, but got %v", i, test.exp, ret.runs)
		}
	}
}

func TestDifferenceRunBitmapOutputBitmap(t *testing.T) {
	a := &container{}
	b := &container{bitmap: make([]uint64, bitmapN)}
	tests := []struct {
		runs   []interval16
		bitmap []uint64
		exp    []uint64
	}{
		{
			runs:   []interval16{{start: 0, last: 65535}},
			bitmap: bitmapEvens(),
			exp:    bitmapOdds(),
		},
		{
			runs:   []interval16{{start: 0, last: 65535}},
			bitmap: bitmapOdds(),
			exp:    bitmapEvens(),
		},
	}
	for i, test := range tests {
		a.runs = test.runs
		a.n = a.runCountRange(0, 65535)
		b.bitmap = test.bitmap
		b.n = b.bitmapCountRange(0, 65535)
		ret := differenceRunBitmap(a, b)
		if !reflect.DeepEqual(ret.bitmap, test.exp) {
			t.Fatalf("test #%v expected %v, but got %#v", i, test.exp[:10], ret)
		}
	}
}

func TestDifferenceRunBitmapOutputArray(t *testing.T) {
	a := &container{container_type: ContainerRun}
	b := &container{container_type: ContainerBitmap}
	tests := []struct {
		runs   []interval16
		bitmap []uint64
		exp    []uint16
	}{
		{
			runs:   []interval16{{start: 10, last: 20}},
			bitmap: bitmap(2, 10, 12, 14, 16, 18, 22, 100),
			exp:    []uint16{11, 13, 15, 17, 19, 20},
		},
		{
			runs:   []interval16{{start: 17, last: 20}, {start: 62, last: 66}, {start: 98, last: 100}},
			bitmap: bitmap(2, 18, 22, 63, 65, 81, 100),
			exp:    []uint16{17, 19, 20, 62, 64, 66, 98, 99},
		},
	}
	for i, test := range tests {
		a.runs = test.runs
		a.n = a.runCountRange(0, 65535)
		b.bitmap = test.bitmap
		b.n = b.bitmapCountRange(0, 65535)
		ret := differenceRunBitmap(a, b)
		if !reflect.DeepEqual(ret.array, test.exp) {
			t.Fatalf("test #%v expected %v, but got %v", i, test.exp, ret.array)
		}
	}
}

func TestDifferenceBitmapRun(t *testing.T) {
	a := &container{bitmap: make([]uint64, bitmapN)}
	b := &container{}
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
	}
	for i, test := range tests {
		for i, v := range test.bitmap {
			a.bitmap[i] = v
		}
		a.n = a.bitmapCountRange(0, 100)
		b.runs = test.runs
		b.n = b.runCountRange(0, 100)
		ret := differenceBitmapRun(a, b)
		if !reflect.DeepEqual(ret.bitmap[:len(test.exp)], test.exp) {
			t.Fatalf("test #%v expected \n%X, but got \n%X", i, test.exp, ret.bitmap[:len(test.exp)])
		}
	}
}

func TestDifferenceBitmapArray(t *testing.T) {
	a := &container{bitmap: make([]uint64, bitmapN), container_type: ContainerBitmap}
	b := &container{container_type: ContainerArray}
	tests := []struct {
		bitmap []uint64
		array  []uint16
		exp    []uint16
	}{
		{
			bitmap: []uint64{0xFF0F},
			array:  []uint16{0, 1, 2, 3, 4, 5, 6, 7, 10},
			exp:    []uint16{8, 9, 11, 12, 13, 14, 15},
		},
	}
	for i, test := range tests {
		a.bitmap[0] = test.bitmap[0]
		a.n = a.count()
		b.array = test.array
		ret := differenceBitmapArray(a, b)
		if !reflect.DeepEqual(ret.array, test.exp) {
			t.Fatalf("test #%v expected \n%X, but got \n%X", i, test.exp, ret.array)
		}
	}
}
func TestDifferenceBitmapBitmap(t *testing.T) {
	a := &container{bitmap: make([]uint64, bitmapN), container_type: ContainerBitmap}
	b := &container{bitmap: make([]uint64, bitmapN), container_type: ContainerBitmap}
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
	a := &container{container_type: ContainerRun}
	b := &container{container_type: ContainerRun}
	tests := []struct {
		aruns []interval16
		bruns []interval16
		exp   []interval16
		expn  int
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
	ca := &container{array: []uint16{1, 10, 100, 1000}, n: 4, container_type: ContainerArray}
	ba := &Bitmap{keys: []uint64{0}, containers: []*container{ca}}
	ba2 := &Bitmap{}
	var buf bytes.Buffer
	_, err := ba.WriteTo(&buf)
	if err != nil {
		t.Fatalf("error writing: %v", err)
	}
	err = ba2.UnmarshalBinary(buf.Bytes())
	if err != nil {
		t.Fatalf("error unmarshaling: %v", err)
	}
	if !reflect.DeepEqual(ba2.containers[0].array, ca.array) {
		t.Fatalf("array test expected %x, but got %x", ca.array, ba2.containers[0].array)
	}
}

func TestWriteReadBitmap(t *testing.T) {
	// create bitmap containing > 4096 bits
	cb := &container{bitmap: make([]uint64, bitmapN), n: 129 * 32, container_type: ContainerBitmap}
	for i := 0; i < 129; i++ {
		cb.bitmap[i] = 0x5555555555555555
	}
	bb := &Bitmap{keys: []uint64{0}, containers: []*container{cb}}
	bb2 := &Bitmap{}
	var buf bytes.Buffer
	_, err := bb.WriteTo(&buf)
	if err != nil {
		t.Fatalf("error writing: %v", err)
	}
	err = bb2.UnmarshalBinary(buf.Bytes())
	if err != nil {
		t.Fatalf("error unmarshaling: %v", err)
	}
	if !reflect.DeepEqual(bb2.containers[0].bitmap, cb.bitmap) {
		t.Fatalf("bitmap test expected %x, but got %x", cb.bitmap, bb2.containers[0].bitmap)
	}
}

func TestWriteReadFullBitmap(t *testing.T) {
	// create bitmap containing > 4096 bits
	cb := &container{bitmap: make([]uint64, bitmapN), n: 65536, container_type: ContainerBitmap}
	for i := 0; i < bitmapN; i++ {
		cb.bitmap[i] = 0xffffffffffffffff
	}
	bb := &Bitmap{keys: []uint64{0}, containers: []*container{cb}}
	bb2 := &Bitmap{}
	var buf bytes.Buffer
	_, err := bb.WriteTo(&buf)
	if err != nil {
		t.Fatalf("error writing: %v", err)
	}
	err = bb2.UnmarshalBinary(buf.Bytes())
	if err != nil {
		t.Fatalf("error unmarshaling: %v", err)
	}
	if !reflect.DeepEqual(bb2.containers[0].bitmap, cb.bitmap) {
		t.Fatalf("bitmap test expected %x, but got %x", cb.bitmap, bb2.containers[0].bitmap)
	}

	if bb2.containers[0].n != cb.n {
		t.Fatalf("bitmap test expected count %x, but got %x", cb.n, bb2.containers[0].n)
	}
	if bb2.containers[0].count() != cb.count() {
		t.Fatalf("bitmap test expected count %x, but got %x", cb.n, bb2.containers[0].n)
	}
}

func TestWriteReadRun(t *testing.T) {
	cr := &container{runs: []interval16{{start: 3, last: 13}, {start: 100, last: 109}}, n: 21, container_type: ContainerRun}
	br := &Bitmap{keys: []uint64{0}, containers: []*container{cr}}
	br2 := &Bitmap{}
	var buf bytes.Buffer
	_, err := br.WriteTo(&buf)
	if err != nil {
		t.Fatalf("error writing: %v", err)
	}
	err = br2.UnmarshalBinary(buf.Bytes())
	if err != nil {
		t.Fatalf("error unmarshaling: %v", err)
	}
	if !reflect.DeepEqual(br2.containers[0].runs, cr.runs) {
		t.Fatalf("run test expected %x, but got %x", cr.runs, br2.containers[0].runs)
	}
}

func TestXorArrayRun(t *testing.T) {
	tests := []struct {
		a   *container
		b   *container
		exp *container
	}{
		{
			a:   &container{array: []uint16{1, 5, 10, 11, 12}, container_type: ContainerArray},
			b:   &container{runs: []interval16{{start: 2, last: 10}, {start: 12, last: 13}, {start: 15, last: 16}}, container_type: ContainerRun},
			exp: &container{array: []uint16{1, 2, 3, 4, 6, 7, 8, 9, 11, 13, 15, 16}, container_type: ContainerArray, n: 12},
		}, {
			a:   &container{array: []uint16{1, 5, 10, 11, 12, 13, 14}, container_type: ContainerArray},
			b:   &container{runs: []interval16{{start: 2, last: 10}, {start: 12, last: 13}, {start: 15, last: 16}}, container_type: ContainerRun},
			exp: &container{array: []uint16{1, 2, 3, 4, 6, 7, 8, 9, 11, 14, 15, 16}, container_type: ContainerArray, n: 12},
		}, {
			a:   &container{array: []uint16{65535}, container_type: ContainerArray},
			b:   &container{runs: []interval16{{start: 65534, last: 65535}}, container_type: ContainerRun},
			exp: &container{array: []uint16{65534}, container_type: ContainerArray, n: 1},
		}, {
			a:   &container{array: []uint16{65535}, container_type: ContainerArray},
			b:   &container{runs: []interval16{{start: 65535, last: 65535}}, container_type: ContainerRun},
			exp: &container{array: []uint16{}, container_type: ContainerArray, n: 0},
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
	a := &container{container_type: ContainerRun}
	b := &container{container_type: ContainerRun}
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
	a := &container{container_type: ContainerRun}
	b := &container{container_type: ContainerRun}
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

func TestBitmapFlip(t *testing.T) {
	c := &container{bitmap: make([]uint64, bitmapN), container_type: ContainerBitmap}

	ttable := []struct {
		original uint64
		flipped  uint64
	}{
		{0x0000000000000000, 0xFFFFFFFFFFFFFFFF},
		{0xFFFFFFFFFFFFFFFF, 0x0000000000000000},
		{0xFFFFFFFFFFFFFFF0, 0x000000000000000F},
		{0xFFFFFFEFFFFFFFFF, 0x0000001000000000},
		{0x0000001000000000, 0xFFFFFFEFFFFFFFFF},
	}

	expectedN := int(65536)
	for i, tt := range ttable {
		c.bitmap[i] = tt.original
		expectedN -= int(popcount(tt.original))
	}

	o := c.flipBitmap()

	for i, tt := range ttable {
		if o.bitmap[i] != tt.flipped {
			t.Fatalf("bitmapFlip calculation. expected %v, got %v", tt.flipped, o.bitmap[i])
		}
	}
	if o.n != expectedN {
		t.Fatalf("bitmapFlip calculation. expected count %v, got %v", expectedN, o.n)
	}
}

func TestBitmapXorRange(t *testing.T) {
	c := &container{bitmap: make([]uint64, bitmapN), container_type: ContainerBitmap}
	tests := []struct {
		bitmap []uint64
		start  uint64
		last   uint64
		exp    []uint64
		expN   int
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
		for i, v := range test.bitmap {
			c.bitmap[i] = v
		}
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
	a := &container{container_type: ContainerBitmap}
	b := &container{container_type: ContainerRun}
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
	b := NewBitmap(0, 1, 10, 100, 1000, 10000, 90000, 100000)
	if !b.containers[0].isArray() {
		t.Fatalf("wrong container type")
	}

	itr := b.Iterator()
	if !(itr.i == 0 && itr.j == -1) {
		t.Fatalf("iterator did not zero correctly: %v\n", itr)
	}

	itr.Seek(1000)
	if !(itr.i == 0 && itr.j == 3) {
		t.Fatalf("iterator did not seek correctly: %v\n", itr)
	}

	itr.Seek(10000)
	itr.Next()
	val, eof := itr.Next()
	if !(itr.i == 1 && itr.j == 0 && val == 90000 && !eof) {
		t.Fatalf("iterator did not next correctly across containers: %v\n", itr)
	}

	itr.Seek(80000)
	if !(itr.i == 1 && itr.j == -1) {
		t.Fatalf("iterator did not seek missing value correctly: %v\n", itr)
	}

	itr.Seek(100000)
	if !(itr.i == 1 && itr.j == 0) {
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
	b := NewBitmap()
	for i := uint64(61000); i < 71000; i++ {
		b.Add(i)
	}
	for i := uint64(75000); i < 75100; i++ {
		b.Add(i)
	}
	if !b.containers[0].isBitmap() {
		t.Fatalf("wrong container type")
	}

	itr := b.Iterator()
	if !(itr.i == 0 && itr.j == -1) {
		t.Fatalf("iterator did not zero correctly: %v\n", itr)
	}

	itr.Seek(65000)
	if !(itr.i == 0 && itr.j == 64999) {
		t.Fatalf("iterator did not seek correctly: %v\n", itr)
	}

	itr.Seek(65535)
	itr.Next()
	val, eof := itr.Next()
	if !(itr.i == 1 && itr.j == 0 && val == 65536 && !eof) {
		t.Fatalf("iterator did not next correctly across containers: %v\n", itr)
	}

	itr.Seek(74000)
	if !(itr.i == 1 && itr.j == 8463) {
		t.Fatalf("iterator did not seek missing value correctly: %v\n", itr)
	}

	itr.Seek(70999)
	if !(itr.i == 1 && itr.j == 5462) {
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
	b := NewBitmap(0, 1, 2, 3, 4, 5, 1000, 1001, 1002, 1003, 1004, 1005, 100000, 100001, 100002, 100003, 100004, 100005)
	b.Optimize()
	if !b.containers[0].isRun() {
		t.Fatalf("wrong container type")
	}

	itr := b.Iterator()
	if !(itr.i == 0 && itr.j == 0 && itr.k == -1) {
		t.Fatalf("iterator did not zero correctly: %v\n", itr)
	}

	itr.Seek(4)
	if !(itr.i == 0 && itr.j == 0 && itr.k == 3) {
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
	if !(itr.i == 0 && itr.j == 1 && itr.k == -1) {
		t.Fatalf("iterator did not seek missing value correctly: %v\n", itr)
	}

	itr.Seek(1004)
	if !(itr.i == 0 && itr.j == 1 && itr.k == 3) {
		t.Fatalf("iterator did not seek correctly in multiple runs: %v\n", itr)
	}

	itr.Seek(1005)
	if !(itr.i == 0 && itr.j == 1 && itr.k == 4) {
		t.Fatalf("iterator did not seek correctly to end of run: %v\n", itr)
	}

	itr.Seek(100005)
	if !(itr.i == 1 && itr.j == 0 && itr.k == 4) {
		t.Fatalf("iterator did not seek correctly in multiple containers: %v\n", itr)
	}

	val, eof = itr.Next()
	val, eof = itr.Next()
	if !(val == 0 && eof) {
		t.Fatalf("iterator did not eof correctly: %d, %v\n", val, eof)
	}
}

func TestRunBinSearchContains(t *testing.T) {
	tests := []struct {
		runs  []interval16
		index uint16
		exp   struct {
			index int
			found bool
		}
	}{
		{
			runs:  []interval16{{start: 0, last: 10}},
			index: uint16(3),
			exp: struct {
				index int
				found bool
			}{index: 0, found: true},
		},
		{
			runs:  []interval16{{start: 0, last: 10}},
			index: uint16(13),
			exp: struct {
				index int
				found bool
			}{index: 0, found: false},
		},
		{
			runs:  []interval16{{start: 0, last: 10}, {start: 20, last: 30}},
			index: uint16(13),
			exp: struct {
				index int
				found bool
			}{index: 0, found: false},
		},
		{
			runs:  []interval16{{start: 0, last: 10}, {start: 20, last: 30}},
			index: uint16(36),
			exp: struct {
				index int
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
		expi   int
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
	bm1 := NewBitmap(1<<16, 2<<16, 3<<16)
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
	bm1 := NewBitmap(1<<16, 2<<16, 3<<16)
	bm1.Remove(2 << 16)
	var buf bytes.Buffer
	if _, err := bm1.WriteTo(&buf); err != nil {
		t.Fatalf("Failure to write to bitmap buffer. ")
	}
	bm0 := NewBitmap()
	bm0.UnmarshalBinary(buf.Bytes())
	if bm0.countEmptyContainers() != 0 {
		t.Fatalf("Should be no empty containers ")
	}
	if bm0.Count() != bm1.Count() {
		t.Fatalf("Counts do not match after a marshal %d %d", bm0.Count(), bm1.Count())
	}
}

func Test_BufBitmapIterator_Next(t *testing.T) {
	b := NewBitmap()
	for i := uint64(0); i < 4097; i++ {
		b.Add(i)
	}
	if !b.containers[0].isBitmap() {
		t.Fatalf("wrong container type")
	}

	bin := []uint16{}

	itr := newBufBitmapIterator(newBitmapIterator(b.containers[0].bitmap))
	x := uint16(0)

	for i := 0; i < 10; i++ {
		x, _ = itr.next()
		bin = append(bin, x)
	}
	exp := []uint16{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	if !reflect.DeepEqual(bin, exp) {
		t.Fatalf("BufBitmapIterator expected (%v) but got (%v)", exp, bin)
	}

	// ensure that unread points next back one such that the last value is repeated
	itr.unread()
	x, _ = itr.next()
	bin = append(bin, x)
	exp = append(exp, uint16(9))
	if !reflect.DeepEqual(bin, exp) {
		t.Fatalf("BufBitmapIterator expected (%v) but got (%v)", exp, bin)
	}
}

func Test_BufBitmapIterator_UnreadPanic(t *testing.T) {

	defer func() {
		if r := recover(); r == nil {
			t.Errorf("BufBitmapIterator unread did not panic")
		}
	}()

	b := NewBitmap()
	for i := uint64(0); i < 4097; i++ {
		b.Add(i)
	}
	if !b.containers[0].isBitmap() {
		t.Fatalf("wrong container type")
	}

	itr := newBufBitmapIterator(newBitmapIterator(b.containers[0].bitmap))
	for i := 0; i < 10; i++ {
		itr.next()
	}

	// ensure that unreading back-to-back panics
	itr.unread()
	itr.unread()
}

func TestSearc64(t *testing.T) {
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
	a, b := &container{}, &container{
		bitmap: make([]uint64, bitmapN),
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
			bitmap: bitmapOdds(),
			exp:    []uint16{1, 63, 543, 639, 65535},
		},
		{
			array:  []uint16{0, 1, 63, 120, 543, 639, 12000, 65534, 65535},
			bitmap: bitmapEvens(),
			exp:    []uint16{0, 120, 12000, 65534},
		},
	}

	for i, test := range tests {
		a.array = test.array
		a.container_type = ContainerArray
		for i, bmval := range test.bitmap {
			b.bitmap[i] = bmval
		}
		b.container_type = ContainerBitmap
		ret1 := intersectArrayBitmapOld(a, b).array
		ret2 := intersectArrayBitmap(a, b).array
		if len(ret1) == 0 && len(ret2) == 0 && len(test.exp) == 0 {
			continue
		}
		if !reflect.DeepEqual(ret1, ret2) || !reflect.DeepEqual(ret2, test.exp) {
			t.Fatalf("test #%v intersectArrayBitmap fail orig: %v new: %v exp: %v", i, ret1, ret2, test.exp)
		}
	}
}

func bitmapOdds() []uint64 {
	bitmap := make([]uint64, bitmapN)
	for i := 0; i < bitmapN; i++ {
		bitmap[i] = 0xAAAAAAAAAAAAAAAA
	}
	return bitmap
}

func bitmapEvens() []uint64 {
	bitmap := make([]uint64, bitmapN)
	for i := 0; i < bitmapN; i++ {
		bitmap[i] = 0x5555555555555555
	}
	return bitmap
}
