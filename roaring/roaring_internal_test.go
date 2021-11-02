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
	"math/rand"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/molecula/featurebase/v2/generator"
	"github.com/pkg/errors"
)

// String produces a human viewable string of the contents.
func (iv Interval16) String() string {
	return fmt.Sprintf("[%d, %d]", iv.Start, iv.Last)
}

func GetRoaringIter(bitsToSet ...uint64) RoaringIterator {

	b := NewBitmap()
	changed := b.DirectAddN(bitsToSet...)
	n := len(bitsToSet)
	if changed != n {
		e := fmt.Sprintf("changed=%v but bitsToSet len = %v", changed, n)
		panic(e)
	}
	buf := bytes.NewBuffer(make([]byte, 0, 100000))
	_, er := b.WriteTo(buf)
	if er != nil {
		if er != nil {
			panic(er)
		}
	}
	itr, err := NewRoaringIterator(buf.Bytes())
	if err != nil {
		panic(err)
	}
	return itr
}

func TestRunAppendInterval(t *testing.T) {
	a := NewContainerRun(nil)
	tests := []struct {
		base []Interval16
		app  Interval16
		exp  int32
	}{
		{
			base: []Interval16{},
			app:  Interval16{Start: 22, Last: 25},
			exp:  4,
		},
		{
			base: []Interval16{{Start: 20, Last: 23}},
			app:  Interval16{Start: 22, Last: 25},
			exp:  2,
		},
		{
			base: []Interval16{{Start: 20, Last: 23}},
			app:  Interval16{Start: 21, Last: 22},
			exp:  0,
		},
		{
			base: []Interval16{{Start: 20, Last: 23}},
			app:  Interval16{Start: 19, Last: 25},
			exp:  2, // runAppendInterval explicitly does not support intervals whose start is < c.runs[-1].start
		},
	}

	for i, test := range tests {
		a.setRuns(test.base)
		if n := a.runAppendInterval(test.app); n != test.exp {
			t.Fatalf("test #%v expected %v, but got %v", i, test.exp, n)
		}
	}

}

func TestInterval16RunLen(t *testing.T) {
	iv := Interval16{Start: 7, Last: 9}
	if iv.runlen() != 3 {
		t.Fatalf("should be 3")
	}
	iv = Interval16{Start: 7, Last: 7}
	if iv.runlen() != 1 {
		t.Fatalf("should be 1")
	}
}

func TestContainerRunAdd(t *testing.T) {
	c := NewContainerRun(nil)
	tests := []struct {
		op  uint16
		exp []Interval16
	}{
		{1, []Interval16{{Start: 1, Last: 1}}},
		{2, []Interval16{{Start: 1, Last: 2}}},
		{4, []Interval16{{Start: 1, Last: 2}, {Start: 4, Last: 4}}},
		{3, []Interval16{{Start: 1, Last: 4}}},
		{10, []Interval16{{Start: 1, Last: 4}, {Start: 10, Last: 10}}},
		{7, []Interval16{{Start: 1, Last: 4}, {Start: 7, Last: 7}, {Start: 10, Last: 10}}},
		{6, []Interval16{{Start: 1, Last: 4}, {Start: 6, Last: 7}, {Start: 10, Last: 10}}},
		{0, []Interval16{{Start: 0, Last: 4}, {Start: 6, Last: 7}, {Start: 10, Last: 10}}},
		{8, []Interval16{{Start: 0, Last: 4}, {Start: 6, Last: 8}, {Start: 10, Last: 10}}},
	}
	var changed bool
	for _, test := range tests {
		c.setMapped(true)
		c, changed = c.add(test.op)
		if !changed {
			t.Fatalf("result of adding new bit should be true: %v", c.runs())
		}
		if !reflect.DeepEqual(c.runs(), test.exp) {
			t.Fatalf("Should have %v, but got %v after adding %v", test.exp, c.runs(), test.op)
		}
		if c.Mapped() {
			t.Fatalf("container should not be mapped after adding bit %v", test.op)
		}
	}
}

func TestContainerRunAdd2(t *testing.T) {
	c := NewContainerRun(nil)
	c, ret := c.add(0)
	if !ret {
		t.Fatalf("result of adding new bit should be true: %v", c.runs())
	}
	if !reflect.DeepEqual(c.runs(), []Interval16{{Start: 0, Last: 0}}) {
		t.Fatalf("should have 1 run of length 1, but have %v", c.runs())
	}
	c, ret = c.add(0)
	if ret {
		t.Fatalf("result of adding existing bit should be false: %v", c.runs())
	}
}

func TestRunCountRange(t *testing.T) {
	c := NewContainerRun(nil)
	cnt := RunCountRange(c.runs(), 2, 9)
	if cnt != 0 {
		t.Fatalf("should get 0 from empty container, but got: %v", cnt)
	}
	c, _ = c.add(5)
	c, _ = c.add(6)
	c, _ = c.add(7)

	cnt = RunCountRange(c.runs(), 2, 9)
	if cnt != 3 {
		t.Fatalf("should get 3 from interval within range, but got: %v", cnt)
	}

	c, _ = c.add(8)
	c, _ = c.add(9)
	c, _ = c.add(10)
	c, _ = c.add(11)

	cnt = RunCountRange(c.runs(), 4, 8)
	if cnt != 3 {
		t.Fatalf("should get 3 from range overlaps front of interval, but got: %v", cnt)
	}

	cnt = RunCountRange(c.runs(), 5, 8)
	if cnt != 3 {
		t.Fatalf("should get 3 from range within interval, but got: %v", cnt)
	}

	cnt = RunCountRange(c.runs(), 6, 8)
	if cnt != 2 {
		t.Fatalf("should get 2 from range within interval, but got: %v", cnt)
	}

	cnt = RunCountRange(c.runs(), 3, 9)
	if cnt != 4 {
		t.Fatalf("should get 4 from range overlaps front of interval, but got: %v", cnt)
	}

	cnt = RunCountRange(c.runs(), 9, 14)
	if cnt != 3 {
		t.Fatalf("should get 3 from range overlaps back of interval, but got: %v", cnt)
	}

	cnt = RunCountRange(c.runs(), 8, 10)
	if cnt != 2 {
		t.Fatalf("should get 2 from range within interval, but got: %v", cnt)
	}

	cnt = RunCountRange(c.runs(), 8, 11)
	if cnt != 3 {
		t.Fatalf("should get 3 from range within interval, but got: %v", cnt)
	}

	cnt = RunCountRange(c.runs(), 8, 12)
	if cnt != 4 {
		t.Fatalf("should get 4 from range overlaps back of interval, but got: %v", cnt)
	}

	cnt = RunCountRange(c.runs(), 5, 12)
	if cnt != 7 {
		t.Fatalf("should get 7 from interval within range, but got: %v", cnt)
	}

	cnt = RunCountRange(c.runs(), 5, 11)
	if cnt != 6 {
		t.Fatalf("should get 6 from interval equal to range, but got: %v", cnt)
	}

	c, _ = c.add(17)
	c, _ = c.add(19)
	c, _ = c.add(18)

	cnt = RunCountRange(c.runs(), 1, 22)
	if cnt != 10 {
		t.Fatalf("should get 10 from multiple ranges in interval, but got: %v", cnt)
	}

	c, _ = c.add(13)
	c, _ = c.add(14)

	cnt = RunCountRange(c.runs(), 6, 18)
	if cnt != 9 {
		t.Fatalf("should get 9 from multiple ranges overlapping both sides, but got: %v", cnt)
	}
	// verify that the disparate ops resulted in three separate runs
	cnt = c.countRuns()
	if cnt != 3 {
		t.Fatalf("should get 3 total runs, but got: %v [%v]", cnt, c.runs())
	}
}

func TestRunContains(t *testing.T) {
	c := NewContainerRun(nil)
	if c.runContains(5) {
		t.Fatalf("empty run container should not contain 5")
	}
	c, _ = c.add(5)
	if !c.runContains(5) {
		t.Fatalf("run container with 5 should contain 5")
	}

	c, _ = c.add(6)
	c, _ = c.add(7)

	c, _ = c.add(9)
	c, _ = c.add(10)
	c, _ = c.add(11)

	if !c.runContains(10) {
		t.Fatalf("run container with 10 in second run should contain 10")
	}
}

func TestBitmapCountRange(t *testing.T) {
	c := NewContainerBitmap(0, nil)
	tests := []struct {
		start  int32
		end    int32
		bitmap [bitmapN]uint64
		exp    int32
	}{
		{start: 0, end: 1, bitmap: [bitmapN]uint64{1}, exp: 1},
		{start: 2, end: 7, bitmap: [bitmapN]uint64{0xFFFFFFFFFFFFFF18}, exp: 2},
		{start: 67, end: 68, bitmap: [bitmapN]uint64{0, 0x8}, exp: 1},
		{start: 1, end: 68, bitmap: [bitmapN]uint64{0x3, 0x8, 0xF}, exp: 2},
		{start: 1, end: 258, bitmap: [bitmapN]uint64{0xF, 0x8, 0xA, 0x4, 0xFFFFFFFFFFFFFFFF}, exp: 9},
		{start: 66, end: 71, bitmap: [bitmapN]uint64{0xF, 0xFFFFFFFFFFFFFF18}, exp: 2},
		{start: 63, end: 64, bitmap: [bitmapN]uint64{0x8000000000000000}, exp: 1},
	}

	for i, test := range tests {
		c.setBitmap(test.bitmap[:])
		if ret := BitmapCountRange(c.bitmap(), test.start, test.end); ret != test.exp {
			t.Fatalf("test #%v count of %v from %v to %v should be %v but got %v", i, test.bitmap, test.start, test.end, test.exp, ret)
		}
	}
}

func TestIntersectionCountArrayBitmap3(t *testing.T) {
	a, b := NewContainerBitmapN(getFullBitmap(), MaxContainerVal+1), NewContainerBitmapN(getFullBitmap(), MaxContainerVal+1)

	res := intersectBitmapBitmap(a, b)
	if res.N() != res.count() || res.N() != MaxContainerVal+1 {
		t.Fatalf("test #1 intersectCountBitmapBitmap fail orig: %v new: %v exp: %v", res.N(), res.count(), MaxContainerVal+1)
	}

	a = a.bitmapToRun(0)
	res = intersectBitmapRun(b, a)
	if res.N() != res.count() || res.N() != MaxContainerVal+1 {
		t.Fatalf("test #2 intersectCountBitmapRun fail orig: %v new: %v exp: %v", res.N(), res.count(), MaxContainerVal+1)
	}
	b = b.bitmapToRun(0)
	res = intersectRunRun(a, b)
	n := intersectionCountRunRun(a, b)
	if res.N() != res.count() || res.N() != MaxContainerVal+1 || res.N() != int32(n) {
		t.Fatalf("test #3 intersectCountRunRun fail orig: %v new: %v exp: %v", res.N(), res.count(), MaxContainerVal+1)
	}
}

func TestIntersectionCountArrayBitmap2(t *testing.T) {
	a, b := NewContainerArray(nil), NewContainerBitmap(0, nil)
	tests := []struct {
		array  []uint16
		bitmap [bitmapN]uint64
		exp    int32
	}{
		{
			array:  []uint16{0},
			bitmap: [bitmapN]uint64{1},
			exp:    1,
		},
		{
			array:  []uint16{0, 1},
			bitmap: [bitmapN]uint64{3},
			exp:    2,
		},
		{
			array:  []uint16{64, 128, 129, 2000},
			bitmap: [bitmapN]uint64{932421, 2},
			exp:    0,
		},
		{
			array:  []uint16{0, 65, 130, 195},
			bitmap: [bitmapN]uint64{255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255},
			exp:    4,
		},
		{
			array:  []uint16{63, 120, 543, 639, 12000},
			bitmap: [bitmapN]uint64{0x8000000000000000, 0, 0, 0, 0, 0, 0, 0, 0, 0x8000000000000000},
			exp:    2,
		},
	}

	for i, test := range tests {
		a.setArray(test.array)
		b.setBitmap(test.bitmap[:])
		ret := intersectionCountArrayBitmap(a, b)
		if ret != test.exp {
			t.Fatalf("test #%v intersectCountArrayBitmap fail received: %v exp: %v", i, ret, test.exp)
		}
	}
}

func TestRunRemove(t *testing.T) {
	c := NewContainerRun([]Interval16{{Start: 2, Last: 10}, {Start: 12, Last: 13}, {Start: 15, Last: 16}})
	tests := []struct {
		op     uint16
		exp    []Interval16
		expRet bool
	}{
		{2, []Interval16{{Start: 3, Last: 10}, {Start: 12, Last: 13}, {Start: 15, Last: 16}}, true},
		{10, []Interval16{{Start: 3, Last: 9}, {Start: 12, Last: 13}, {Start: 15, Last: 16}}, true},
		{12, []Interval16{{Start: 3, Last: 9}, {Start: 13, Last: 13}, {Start: 15, Last: 16}}, true},
		{13, []Interval16{{Start: 3, Last: 9}, {Start: 15, Last: 16}}, true},
		{16, []Interval16{{Start: 3, Last: 9}, {Start: 15, Last: 15}}, true},
		{6, []Interval16{{Start: 3, Last: 5}, {Start: 7, Last: 9}, {Start: 15, Last: 15}}, true},
		{8, []Interval16{{Start: 3, Last: 5}, {Start: 7, Last: 7}, {Start: 9, Last: 9}, {Start: 15, Last: 15}}, true},
		{8, []Interval16{{Start: 3, Last: 5}, {Start: 7, Last: 7}, {Start: 9, Last: 9}, {Start: 15, Last: 15}}, false},
		{1, []Interval16{{Start: 3, Last: 5}, {Start: 7, Last: 7}, {Start: 9, Last: 9}, {Start: 15, Last: 15}}, false},
		{44, []Interval16{{Start: 3, Last: 5}, {Start: 7, Last: 7}, {Start: 9, Last: 9}, {Start: 15, Last: 15}}, false},
	}

	for i, test := range tests {
		c = c.Freeze()
		var ret bool
		c, ret = c.remove(test.op)
		if ret != test.expRet || !reflect.DeepEqual(c.runs(), test.exp) {
			t.Fatalf("test #%v Unexpected result removing %v from runs. Expected %v, got %v. Expected %v, got %v", i, test.op, test.expRet, ret, test.exp, c.runs())
		}
		if ret && c.frozen() {
			t.Fatalf("test #%v container was not unmapped although bit %v was removed", i, test.op)
		}
		if !ret && !c.frozen() {
			t.Fatalf("test #%v container was unmapped although bit %v was not removed", i, test.op)
		}
	}
}

func TestRunMax(t *testing.T) {
	c := NewContainerRun([]Interval16{{Start: 2, Last: 10}, {Start: 12, Last: 13}, {Start: 15, Last: 16}})
	max := c.max()
	if max != 16 {
		t.Fatalf("max for %v should be 16", c.runs())
	}

	c = NewContainerRun(nil)
	max = c.max()
	if max != 0 {
		t.Fatalf("max for %v should be 0", c.runs())
	}
}

func TestIntersectionCountArrayRun(t *testing.T) {
	a := NewContainerArray([]uint16{1, 5, 10, 11, 12})
	b := NewContainerRun([]Interval16{{Start: 2, Last: 10}, {Start: 12, Last: 13}, {Start: 15, Last: 16}})

	ret := intersectionCountArrayRun(a, b)
	if ret != 3 {
		t.Fatalf("count of %v with %v should be 3, but got %v", a.array(), b.runs(), ret)
	}
}

func TestIntersectionCountBitmapRun(t *testing.T) {
	ob := make([]uint64, bitmapN)
	ob[0] = 1 << 63
	a := NewContainerBitmap(1, ob)
	b := NewContainerRun([]Interval16{{Start: 63, Last: 64}})

	ret := intersectionCountBitmapRun(a, b)
	if ret != 1 {
		t.Fatalf("count of %v with %v should be 1, but got %v", a.bitmap(), b.runs(), ret)
	}

	a = NewContainerBitmap(-1, []uint64{0xF0000001, 0xFF00000000000000, 0xFF000000000000F0, 0x0F0000})
	b = NewContainerRun([]Interval16{{Start: 29, Last: 31}, {Start: 125, Last: 134}, {Start: 191, Last: 197}, {Start: 200, Last: 300}})

	ret = intersectionCountBitmapRun(a, b)
	if ret != 14 {
		t.Fatalf("count of %v with %v should be 14, but got %v", a.bitmap(), b.runs(), ret)
	}
}

func TestIntersectionCountRunRun(t *testing.T) {
	tests := []struct {
		aruns []Interval16
		bruns []Interval16
		exp   int32
	}{
		{
			aruns: []Interval16{},
			bruns: []Interval16{{Start: 3, Last: 8}}, exp: 0},
		{
			aruns: []Interval16{{Start: 2, Last: 10}},
			bruns: []Interval16{{Start: 3, Last: 8}}, exp: 6},
		{
			aruns: []Interval16{{Start: 2, Last: 10}},
			bruns: []Interval16{{Start: 1, Last: 11}}, exp: 9},
		{
			aruns: []Interval16{{Start: 2, Last: 10}},
			bruns: []Interval16{{Start: 0, Last: 2}}, exp: 1},
		{
			aruns: []Interval16{{Start: 2, Last: 10}},
			bruns: []Interval16{{Start: 1, Last: 10}}, exp: 9},
		{
			aruns: []Interval16{{Start: 2, Last: 10}},
			bruns: []Interval16{{Start: 5, Last: 12}}, exp: 6},
		{
			aruns: []Interval16{{Start: 2, Last: 10}},
			bruns: []Interval16{{Start: 10, Last: 99}}, exp: 1},
		{
			aruns: []Interval16{{Start: 2, Last: 10}, {Start: 44, Last: 99}},
			bruns: []Interval16{{Start: 12, Last: 14}}, exp: 0},
		{
			aruns: []Interval16{{Start: 2, Last: 10}, {Start: 12, Last: 13}},
			bruns: []Interval16{{Start: 2, Last: 10}, {Start: 12, Last: 13}}, exp: 11},
		{
			aruns: []Interval16{{Start: 8, Last: 12}, {Start: 15, Last: 19}},
			bruns: []Interval16{{Start: 9, Last: 9}, {Start: 11, Last: 17}}, exp: 6},
	}
	for i, test := range tests {
		a := NewContainerRun(test.aruns)
		b := NewContainerRun(test.bruns)
		ret := intersectionCountRunRun(a, b)
		if ret != test.exp {
			t.Fatalf("test #%v failed intersecting %v with %v should be %v, but got %v", i, test.aruns, test.bruns, test.exp, ret)
		}
	}
}

func TestIntersectArrayRun(t *testing.T) {
	a := NewContainerArray(nil)
	b := NewContainerRun(nil)
	tests := []struct {
		array []uint16
		runs  []Interval16
		exp   []uint16
	}{
		{
			array: []uint16{1, 4, 5, 7, 10, 11, 12},
			runs:  []Interval16{{Start: 5, Last: 10}},
			exp:   []uint16{5, 7, 10},
		},
		{
			array: []uint16{},
			runs:  []Interval16{{Start: 5, Last: 10}},
			exp:   []uint16(nil),
		},
		{
			array: []uint16{1, 4, 5, 7, 10, 11, 12},
			runs:  []Interval16{},
			exp:   []uint16(nil),
		},
		{
			array: []uint16{0, 1, 4, 5, 7, 10, 11, 12},
			runs:  []Interval16{{Start: 0, Last: 5}, {Start: 7, Last: 7}},
			exp:   []uint16{0, 1, 4, 5, 7},
		},
	}

	for i, test := range tests {
		a.setArray(test.array)
		b.setRuns(test.runs)
		ret := intersectArrayRun(a, b)
		if test.exp == nil {
			if len(ret.array()) != 0 {
				t.Fatalf("test #%v expected %v, but got %v", i, test.exp, ret.array())
			}
		} else if !reflect.DeepEqual(ret.array(), test.exp) {
			t.Fatalf("test #%v expected %v, but got %v", i, test.exp, ret.array())
		}
	}
}

func TestIntersectRunRun(t *testing.T) {
	a := NewContainerRun(nil)
	b := NewContainerRun(nil)
	tests := []struct {
		aruns []Interval16
		bruns []Interval16
		exp   []Interval16
		expN  int32
	}{
		{
			aruns: []Interval16{},
			bruns: []Interval16{{Start: 5, Last: 10}},
			exp:   []Interval16(nil),
			expN:  0,
		},
		{
			aruns: []Interval16{{Start: 5, Last: 12}},
			bruns: []Interval16{{Start: 5, Last: 10}},
			exp:   []Interval16{{Start: 5, Last: 10}},
			expN:  6,
		},
		{
			aruns: []Interval16{{Start: 1, Last: 3}, {Start: 5, Last: 5}, {Start: 7, Last: 8}, {Start: 9, Last: 12}},
			bruns: []Interval16{{Start: 5, Last: 10}},
			exp:   []Interval16{{Start: 5, Last: 5}, {Start: 7, Last: 10}},
			expN:  5,
		},
		{
			aruns: []Interval16{{Start: 20, Last: 30}},
			bruns: []Interval16{{Start: 5, Last: 10}, {Start: 19, Last: 21}},
			exp:   []Interval16{{Start: 20, Last: 21}},
			expN:  2,
		},
		{
			aruns: []Interval16{{Start: 5, Last: 10}},
			bruns: []Interval16{{Start: 7, Last: 12}},
			exp:   []Interval16{{Start: 7, Last: 10}},
			expN:  4,
		},
		{
			aruns: []Interval16{{Start: 5, Last: 12}},
			bruns: []Interval16{{Start: 7, Last: 10}},
			exp:   []Interval16{{Start: 7, Last: 10}},
			expN:  4,
		},
	}
	for i, test := range tests {
		a.setRuns(test.aruns)
		b.setRuns(test.bruns)
		ret := intersectRunRun(a, b)
		if ret.N() != test.expN {
			t.Fatalf("test #%v expected n to be %v, but got %v", i, test.expN, ret.N())
		}
		if test.exp != nil {
			if !reflect.DeepEqual(ret.runs(), test.exp) {
				t.Fatalf("test #%v expected %v, but got %v", i, test.exp, ret.runs())
			}
		} else if len(ret.runs()) != 0 {
			t.Fatalf("test #%v expected %v, but got %v", i, test.exp, ret.runs())
		}
	}
}

func TestIntersectBitmapRunBitmap(t *testing.T) {
	tests := []struct {
		bitmap []uint64
		runs   []Interval16
		exp    []uint64
		expN   int32
	}{
		{
			bitmap: []uint64{1},
			runs:   []Interval16{{Start: 0, Last: 0}, {Start: 2, Last: 5}, {Start: 62, Last: 71}, {Start: 77, Last: 4096}},
			exp:    []uint64{1},
			expN:   1,
		},
		{
			bitmap: []uint64{0xFFFFFFFFFFFFFFFF},
			runs:   []Interval16{{Start: 1, Last: 1}},
			exp:    []uint64{2},
			expN:   1,
		},
		{
			bitmap: []uint64{0xFFFFFFFFFFFFFFFF},
			runs:   []Interval16{{Start: 1, Last: 1}, {Start: 10, Last: 12}, {Start: 61, Last: 77}},
			exp:    []uint64{0xe000000000001C02},
			expN:   7,
		},
		{
			bitmap: []uint64{0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF},
			runs:   []Interval16{{Start: 1, Last: 1}, {Start: 61, Last: 77}},
			exp:    []uint64{0xE000000000000002, 0x00000000000003FFF},
			expN:   18,
		},
		{
			bitmap: []uint64{0xFFFFFFFFFFFFFFFF, 1, 1, 1, 0xA, 1, 1, 0, 1},
			runs:   []Interval16{{Start: 63, Last: 10000}},
			exp:    []uint64{0x8000000000000000, 1, 1, 1, 0xA, 1, 1, 0, 1},
			expN:   9,
		},
	}
	for i, test := range tests {
		exp := make([]uint64, bitmapN)
		copy(exp, test.exp)
		a := NewContainerBitmap(-1, test.bitmap)
		b := NewContainerRun(test.runs)
		b.setN(4097)
		ret := intersectBitmapRun(a, b)
		if ret.isArray() {
			ret = ret.arrayToBitmap()
		}
		if !reflect.DeepEqual(ret.bitmap(), exp) {
			t.Fatalf("test #%v expected %v, but got %v", i, exp, ret.bitmap())
		}
		if ret.N() != test.expN {
			t.Fatalf("test #%v expected n to be %v, but got %v", i, test.expN, ret.N())
		}
	}

}

func TestIntersectBitmapRunArray(t *testing.T) {
	a := NewContainerBitmap(0, nil)
	b := NewContainerRun(nil)
	tests := []struct {
		bitmap []uint64
		runs   []Interval16
		exp    []uint16
		expN   int32
	}{
		{
			bitmap: []uint64{1},
			runs:   []Interval16{{Start: 0, Last: 0}, {Start: 2, Last: 5}, {Start: 62, Last: 71}, {Start: 77, Last: 4096}},
			exp:    []uint16{0},
			expN:   1,
		},
		{
			bitmap: []uint64{0xFFFFFFFFFFFFFFFF},
			runs:   []Interval16{{Start: 1, Last: 1}},
			exp:    []uint16{1},
			expN:   1,
		},
		{
			bitmap: []uint64{0xFFFFFFFFFFFFFFFF},
			runs:   []Interval16{{Start: 1, Last: 1}, {Start: 10, Last: 12}, {Start: 61, Last: 77}},
			exp:    []uint16{1, 10, 11, 12, 61, 62, 63},
			expN:   7,
		},
		{
			bitmap: []uint64{0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF},
			runs:   []Interval16{{Start: 1, Last: 1}, {Start: 61, Last: 68}},
			exp:    []uint16{1, 61, 62, 63, 64, 65, 66, 67, 68},
			expN:   9,
		},
		{
			bitmap: []uint64{0xFFFFFFFFFFFFFFFF, 1, 1, 1, 0xA, 1, 1, 0, 1},
			runs:   []Interval16{{Start: 63, Last: 10000}, {Start: 65000, Last: 65535}},
			exp:    []uint16{63, 64, 128, 192, 257, 259, 320, 384, 512},
			expN:   9,
		},
	}
	for i, test := range tests {
		copy(a.bitmap(), test.bitmap)
		b.setRuns(test.runs)
		ret := intersectBitmapRun(a, b)
		if !reflect.DeepEqual(ret.array(), test.exp) {
			t.Fatalf("test #%v expected %v, but got %v", i, test.exp, ret.array())
		}
		if ret.N() != test.expN {
			t.Fatalf("test #%v expected n to be %v, but got %v", i, test.expN, ret.N())
		}
	}

}

func TestUnionMixed(t *testing.T) {

	// array container
	a := NewContainerArray([]uint16{1, 4, 5, 7, 10, 11, 12})

	// bitmap container
	b := NewContainerBitmap(2, []uint64{0x3})

	// run container
	r := NewContainerRun([]Interval16{{Start: 5, Last: 10}})

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
				res = res.bitmapToArray()
			} else if res.isRun() {
				res = res.runToArray()
			}
			if !reflect.DeepEqual(res.array(), tt.exp) {
				t.Fatalf("test %s expected %v, but got %v", tt.name, tt.exp, res.array())
			}
		}
	})
}

func TestUnionInterval16InPlace(t *testing.T) {
	tests := []struct {
		name      string
		a         []Interval16
		b         []Interval16
		expected  []Interval16
		expectedN int32
	}{
		{
			name:      "firstBitUnset lastBitSet",
			a:         []Interval16{{1, 10}},
			b:         []Interval16{{10, 10}},
			expected:  []Interval16{{1, 10}},
			expectedN: 10,
		},
		{
			name:      "single overlap",
			a:         []Interval16{{1, 10}, {21, 28}},
			b:         []Interval16{{8, 12}},
			expected:  []Interval16{{1, 12}, {21, 28}},
			expectedN: 20,
		},
		{
			name:      "nested intervals",
			a:         []Interval16{{3, 13}, {17, 20}},
			b:         []Interval16{{1, 4}, {6, 7}, {8, 9}, {10, 11}, {14, 17}},
			expected:  []Interval16{{1, 20}},
			expectedN: 20,
		},
		{
			name:      "no overlap",
			a:         []Interval16{{3, 4}, {7, 8}},
			b:         []Interval16{{1, 2}, {5, 6}, {9, 10}},
			expected:  []Interval16{{1, 10}},
			expectedN: 10,
		},
		{
			name:      "b in a",
			a:         []Interval16{{1, 10}},
			b:         []Interval16{{5, 7}},
			expected:  []Interval16{{1, 10}},
			expectedN: 10,
		},
		{
			name:      "a eq b",
			a:         []Interval16{{1, 10}},
			b:         []Interval16{{1, 10}},
			expected:  []Interval16{{1, 10}},
			expectedN: 10,
		},
		{
			name:      "a in b",
			a:         []Interval16{{5, 7}},
			b:         []Interval16{{1, 10}},
			expected:  []Interval16{{1, 10}},
			expectedN: 10,
		},
		{
			name:      "a ahead b",
			a:         []Interval16{{1, 2}, {3, 4}, {5, 7}},
			b:         []Interval16{{10, 11}, {12, 13}, {14, 15}},
			expected:  []Interval16{{1, 7}, {10, 15}},
			expectedN: 13,
		},
		{
			name:      "b ahead a",
			a:         []Interval16{{10, 11}, {12, 13}, {14, 15}},
			b:         []Interval16{{1, 2}, {3, 4}, {5, 7}},
			expected:  []Interval16{{1, 7}, {10, 15}},
			expectedN: 13,
		},
		{
			name:      "empty a and b",
			a:         []Interval16{},
			b:         []Interval16{},
			expected:  []Interval16{},
			expectedN: 0,
		},
		{
			name:      "empty a",
			a:         []Interval16{},
			b:         []Interval16{{1, 2}, {3, 4}, {5, 7}},
			expected:  []Interval16{{1, 7}},
			expectedN: 7,
		},
		{
			name:      "empty b",
			a:         []Interval16{{1, 2}, {3, 4}, {5, 7}},
			b:         []Interval16{},
			expected:  []Interval16{{1, 7}},
			expectedN: 7,
		},
		{
			name:      "single a",
			a:         []Interval16{{1, 2}},
			b:         []Interval16{},
			expected:  []Interval16{{1, 2}},
			expectedN: 2,
		},
		{
			name:      "single b",
			a:         []Interval16{},
			b:         []Interval16{{1, 2}},
			expected:  []Interval16{{1, 2}},
			expectedN: 2,
		},
		{
			name:      "single a single b",
			a:         []Interval16{{3, 4}},
			b:         []Interval16{{1, 2}},
			expected:  []Interval16{{1, 4}},
			expectedN: 4,
		},
		{
			name:      "oddBitsSet lastBitUnset",
			a:         []Interval16{{1, 1}, {3, 3}, {5, 5}},
			b:         []Interval16{{0, 4}},
			expected:  []Interval16{{0, 5}},
			expectedN: 6,
		},
		{
			name:      "all bits",
			a:         []Interval16{{1, 1}, {3, 3}, {5, 5}},
			b:         []Interval16{{0, 0}, {2, 2}, {4, 4}},
			expected:  []Interval16{{0, 5}},
			expectedN: 6,
		},
		{
			name:      "short a long b",
			a:         []Interval16{{5, 5}, {7, 7}, {9, 10}, {12, 12}, {15, 17}, {19, 20}},
			b:         []Interval16{{1, 10}, {12, 12}, {14, 18}},
			expected:  []Interval16{{1, 10}, {12, 12}, {14, 20}},
			expectedN: 18,
		},
		{
			name:      "common endings",
			a:         []Interval16{{1, 5}, {15, 20}, {25, 35}},
			b:         []Interval16{{1, 10}, {15, 20}, {30, 35}},
			expected:  []Interval16{{1, 10}, {15, 20}, {25, 35}},
			expectedN: 27,
		},
		{
			name:      "common endings and overlap",
			a:         []Interval16{{1, 5}, {10, 15}},
			b:         []Interval16{{5, 10}, {12, 17}},
			expected:  []Interval16{{1, 17}},
			expectedN: 17,
		},
		{
			name:      "no common endings and overlap",
			a:         []Interval16{{5, 10}, {12, 17}},
			b:         []Interval16{{0, 11}, {15, 20}},
			expected:  []Interval16{{0, 20}},
			expectedN: 21,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			bb := make([]Interval16, len(tc.b))
			copy(bb, tc.b)

			runs, n := unionInterval16InPlace(tc.a, tc.b)

			for i, v := range tc.expected {
				if runs[i] != v {
					t.Fatalf("runs expected: %+v, got: %+v", tc.expected, runs)
				}
			}
			if n != tc.expectedN {
				t.Fatalf("N expected: %d, got: %d", tc.expectedN, n)
			}

			for i, v := range bb {
				if tc.b[i] != v {
					t.Fatalf("b changed - runs expected: %+v, got: %+v", bb, tc.b)
				}
			}
		})
	}
}

func TestIntersectMixed(t *testing.T) {
	a := NewContainerRun([]Interval16{{Start: 5, Last: 10}})
	b := NewContainerArray([]uint16{1, 4, 5, 7, 10, 11, 12})
	c := NewContainerBitmap(2, []uint64{0x60})

	res := intersect(a, b)
	if !reflect.DeepEqual(res.array(), []uint16{5, 7, 10}) {
		t.Fatalf("test #1 expected %v, but got %v", []uint16{5, 7, 10}, res.array())
	}
	res = intersect(b, a)
	if !reflect.DeepEqual(res.array(), []uint16{5, 7, 10}) {
		t.Fatalf("test #1 expected %v, but got %v", []uint16{5, 7, 10}, res.array())
	}
	res = intersect(a, a)
	if !reflect.DeepEqual(res.runs(), []Interval16{{Start: 5, Last: 10}}) {
		t.Fatalf("test #3 expected %v, but got %v", []Interval16{{Start: 5, Last: 10}}, res.runs())
	}

	res = intersect(c, a)
	if !reflect.DeepEqual(res.array(), []uint16{5, 6}) {
		t.Fatalf("test #4 expected %v, but got %v", []uint16{6}, res.array())
	}

	res = intersect(a, c)
	if !reflect.DeepEqual(res.array(), []uint16{5, 6}) {
		t.Fatalf("test #5 expected %v, but got %v", []uint16{6}, res.array())
	}

	res = intersect(b, c)
	if !reflect.DeepEqual(res.array(), []uint16{5}) {
		t.Fatalf("test #6 expected %v, but got %v", []uint16{5}, res.array())
	}
	res = intersect(c, b)
	if !reflect.DeepEqual(res.array(), []uint16{5}) {
		t.Fatalf("test #7 expected %v, but got %v", []uint16{5}, res.array())
	}

}
func TestDifferenceMixed(t *testing.T) {
	a := NewContainerRun([]Interval16{{Start: 5, Last: 10}})

	b := NewContainerArray([]uint16{0, 2, 4, 6, 8, 10, 12})

	c := NewContainerBitmap(-1, MakeBitmap([]uint64{0x64}))

	d := NewContainerArray([]uint16{1, 3, 5, 7, 9, 11, 12})

	res := difference(a, b)

	if !reflect.DeepEqual(res.array(), []uint16{5, 7, 9}) {
		t.Fatalf("test #1 expected %v, but got %#v", []uint16{5, 7, 9}, res)
	}

	res = difference(b, a)
	if !reflect.DeepEqual(res.array(), []uint16{0, 2, 4, 12}) {
		t.Fatalf("test #2 expected %v, but got %v", []uint16{0, 2, 4, 12}, res.array())
	}

	res = difference(a, a)
	if !reflect.DeepEqual(res.runs(), []Interval16{}) {
		t.Fatalf("test #3 expected empty but got %v", res.runs())
	}

	res = difference(c, a)
	if !reflect.DeepEqual(res.bitmap(), MakeBitmap([]uint64{0x4})) {
		t.Fatalf("test #4 expected %v, but got %v", []uint16{4}, res.bitmap())
	}

	res = difference(a, c)
	if !reflect.DeepEqual(res.runs(), []Interval16{{Start: 7, Last: 10}}) {
		t.Fatalf("test #5 expected %v, but got %v", []Interval16{{Start: 7, Last: 10}}, res.runs())
	}

	res = difference(b, c)
	if !reflect.DeepEqual(res.array(), []uint16{0, 4, 8, 10, 12}) {
		t.Fatalf("test #6 expected %v, but got %v", []uint16{0, 4, 8, 10, 12}, res.array())
	}

	res = difference(c, b)
	if !reflect.DeepEqual(res.array(), []uint16{5}) {
		t.Fatalf("test #7 expected %v, but got %v", []uint16{5}, res.array())
	}

	res = difference(b, b)
	if res.N() != 0 {
		t.Fatalf("test #8 expected 0, but got %d", res.N())
	}

	res = difference(c, c)
	if res.N() != 0 {
		t.Fatalf("test #9 expected 0, but got %d", res.N())
	}

	res = difference(d, b)
	if !reflect.DeepEqual(res.array(), []uint16{1, 3, 5, 7, 9, 11}) {
		t.Fatalf("test #10 expected %v, but got %d", []uint16{1, 3, 5, 7, 9, 11}, res.array())
	}

	res = difference(b, d)
	if !reflect.DeepEqual(res.array(), []uint16{0, 2, 4, 6, 8, 10}) {
		t.Fatalf("test #11 expected %v, but got %d", []uint16{0, 2, 4, 6, 8, 10}, res.array())
	}

}

func TestUnionRunRun(t *testing.T) {
	a := NewContainerRun(nil)
	b := NewContainerRun(nil)
	tests := []struct {
		aruns []Interval16
		bruns []Interval16
		exp   []Interval16
	}{
		{
			aruns: []Interval16{},
			bruns: []Interval16{{Start: 5, Last: 10}},
			exp:   []Interval16{{Start: 5, Last: 10}},
		},
		{
			aruns: []Interval16{{Start: 5, Last: 12}},
			bruns: []Interval16{{Start: 5, Last: 10}},
			exp:   []Interval16{{Start: 5, Last: 12}},
		},
		{
			aruns: []Interval16{{Start: 1, Last: 3}, {Start: 5, Last: 5}, {Start: 7, Last: 8}, {Start: 9, Last: 12}},
			bruns: []Interval16{{Start: 5, Last: 10}},
			exp:   []Interval16{{Start: 1, Last: 3}, {Start: 5, Last: 12}},
		},
		{
			aruns: []Interval16{{Start: 1, Last: 3}, {Start: 5, Last: 5}, {Start: 7, Last: 8}, {Start: 9, Last: 12}},
			bruns: []Interval16{{Start: 2, Last: 65535}},
			exp:   []Interval16{{Start: 1, Last: 65535}},
		},
		{
			aruns: []Interval16{{Start: 2, Last: 65535}},
			bruns: []Interval16{{Start: 1, Last: 3}, {Start: 5, Last: 5}, {Start: 7, Last: 8}, {Start: 9, Last: 12}},
			exp:   []Interval16{{Start: 1, Last: 65535}},
		},
		{
			aruns: []Interval16{{Start: 1, Last: 3}, {Start: 5, Last: 5}, {Start: 7, Last: 8}, {Start: 9, Last: 12}},
			bruns: []Interval16{{Start: 0, Last: 65535}},
			exp:   []Interval16{{Start: 0, Last: 65535}},
		},
		{
			aruns: []Interval16{{Start: 0, Last: 65535}},
			bruns: []Interval16{{Start: 1, Last: 3}, {Start: 5, Last: 5}, {Start: 7, Last: 8}, {Start: 9, Last: 12}},
			exp:   []Interval16{{Start: 0, Last: 65535}},
		},
		{
			aruns: []Interval16{{Start: 1, Last: 3}, {Start: 5, Last: 5}, {Start: 7, Last: 9}, {Start: 12, Last: 22}},
			bruns: []Interval16{{Start: 2, Last: 8}, {Start: 16, Last: 27}, {Start: 33, Last: 34}},
			exp:   []Interval16{{Start: 1, Last: 9}, {Start: 12, Last: 27}, {Start: 33, Last: 34}},
		},
	}
	for i, test := range tests {
		a.setRuns(test.aruns)
		b.setRuns(test.bruns)
		ret := unionRunRun(a, b)
		if !reflect.DeepEqual(ret.runs(), test.exp) {
			t.Fatalf("test #%v expected %v, but got %v", i, test.exp, ret.runs())
		}
	}
}

func TestUnionArrayRun(t *testing.T) {
	a := NewContainerArray(nil)
	b := NewContainerRun(nil)
	tests := []struct {
		array []uint16
		runs  []Interval16
		exp   []uint16
	}{
		{
			array: []uint16{1, 4, 5, 7, 10, 11, 12},
			runs:  []Interval16{{Start: 5, Last: 10}},
			exp:   []uint16{1, 4, 5, 6, 7, 8, 9, 10, 11, 12},
		},
		{
			array: []uint16{},
			runs:  []Interval16{{Start: 5, Last: 10}},
			exp:   []uint16{5, 6, 7, 8, 9, 10},
		},
		{
			array: []uint16{1, 4, 5, 7, 10, 11, 12},
			runs:  []Interval16{},
			exp:   []uint16{1, 4, 5, 7, 10, 11, 12},
		},
		{
			array: []uint16{0, 1, 4, 5, 7, 10, 11, 12},
			runs:  []Interval16{{Start: 0, Last: 5}, {Start: 7, Last: 7}},
			exp:   []uint16{0, 1, 2, 3, 4, 5, 7, 10, 11, 12},
		},
	}

	for i, test := range tests {
		a.setArray(test.array)
		b.setRuns(test.runs)
		ret := unionArrayRun(a, b)
		if !reflect.DeepEqual(ret.array(), test.exp) {
			t.Fatalf("test #%v expected %v, but got %v", i, test.exp, ret.array())
		}
	}
}

func TestBitmapSetRange(t *testing.T) {
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
		c := NewContainerBitmap(-1, test.bitmap)
		c.bitmapSetRange(test.start, test.last+1)
		if !reflect.DeepEqual(c.bitmap()[:len(test.exp)], test.exp) {
			t.Fatalf("test %#v expected %x, got %x", i, test.exp, c.bitmap()[:len(test.bitmap)])
		}
		if test.expN != c.N() {
			t.Fatalf("test #%v expected n to be %v, but got %v", i, test.expN, c.N())
		}
	}
}

func TestArrayToBitmap(t *testing.T) {
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
		a := NewContainerArray(test.array)
		a = a.arrayToBitmap()
		if !reflect.DeepEqual(a.bitmap(), exp) {
			t.Fatalf("test #%v expected %v, but got %v", i, exp, a.bitmap())
		}
	}
}

func TestBitmapToArray(t *testing.T) {
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
		a := NewContainerBitmap(-1, test.bitmap)

		a = a.bitmapToArray()
		if !reflect.DeepEqual(a.array(), test.exp) {
			t.Fatalf("test #%v expected %#v, but got %#v", i, test.exp, a.array())
		}
	}
}

func TestRunToBitmap(t *testing.T) {
	tests := []struct {
		runs []Interval16
		exp  []uint64
	}{
		{
			runs: []Interval16{},
			exp:  []uint64{},
		},
		{
			runs: []Interval16{{Start: 0, Last: 0}},
			exp:  []uint64{1},
		},
		{
			runs: []Interval16{{Start: 0, Last: 4}},
			exp:  []uint64{31},
		},
		{
			runs: []Interval16{{Start: 2, Last: 2}, {Start: 5, Last: 7}, {Start: 13, Last: 14}, {Start: 17, Last: 17}},
			exp:  []uint64{155876},
		},
		{
			runs: []Interval16{{Start: 0, Last: 3}, {Start: 60, Last: 67}},
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
		a := NewContainerRun(test.runs)
		a = a.runToBitmap()
		if !reflect.DeepEqual(a.bitmap(), exp) {
			t.Fatalf("test #%v expected %v, but got %v", i, exp, a.bitmap())
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
	tests := []struct {
		bitmap []uint64
		exp    []Interval16
	}{
		{
			// empty run
			bitmap: []uint64{},
			exp:    []Interval16{},
		},
		{
			// single-bit run
			bitmap: []uint64{1},
			exp:    []Interval16{{Start: 0, Last: 0}},
		},
		{
			// single multi-bit run in one word
			bitmap: []uint64{31},
			exp:    []Interval16{{Start: 0, Last: 4}},
		},
		{
			// multiple runs in one word
			bitmap: []uint64{155876},
			exp:    []Interval16{{Start: 2, Last: 2}, {Start: 5, Last: 7}, {Start: 13, Last: 14}, {Start: 17, Last: 17}},
		},
		{
			// span two words, both mixed
			bitmap: []uint64{0xF00000000000000F, 0x000000000000000F},
			exp:    []Interval16{{Start: 0, Last: 3}, {Start: 60, Last: 67}},
		},
		{
			// span two words, first = maxBitmap
			bitmap: []uint64{0xFFFFFFFFFFFFFFFF, 0xF},
			exp:    []Interval16{{Start: 0, Last: 67}},
		},
		{
			// span two words, second = maxBitmap
			bitmap: []uint64{0xF000000000000000, 0xFFFFFFFFFFFFFFFF},
			exp:    []Interval16{{Start: 60, Last: 127}},
		},
		{
			// span three words
			bitmap: []uint64{0xF000000000000000, 0xFFFFFFFFFFFFFFFF, 0xF},
			exp:    []Interval16{{Start: 60, Last: 131}},
		},
		{
			bitmap: make([]uint64, bitmapN),
			exp:    []Interval16{{Start: 65408, Last: 65535}},
		},
		{
			bitmap: getFullBitmap(),
			exp:    []Interval16{{Start: 0, Last: 65535}},
		},
	}
	tests[8].bitmap[1022] = 0xFFFFFFFFFFFFFFFF
	tests[8].bitmap[1023] = 0xFFFFFFFFFFFFFFFF

	for i, test := range tests {
		a := NewContainerBitmap(-1, test.bitmap)
		x := a.bitmap()
		a = a.bitmapToRun(0)
		if !reflect.DeepEqual(a.runs(), test.exp) {
			t.Fatalf("test #%v expected %v, but got %v", i, test.exp, a.runs())
		}
		a = a.runToBitmap()
		if !reflect.DeepEqual(a.bitmap(), x) {
			t.Fatalf("test #%v expected %v, but got %v", i, a.bitmap(), x)
		}
	}
}

func TestArrayToRun(t *testing.T) {
	tests := []struct {
		array []uint16
		exp   []Interval16
	}{
		{
			array: []uint16{},
			exp:   []Interval16{},
		},
		{
			array: []uint16{0},
			exp:   []Interval16{{Start: 0, Last: 0}},
		},
		{
			array: []uint16{0, 1, 2, 3, 4},
			exp:   []Interval16{{Start: 0, Last: 4}},
		},
		{
			array: []uint16{2, 5, 6, 7, 13, 14, 17},
			exp:   []Interval16{{Start: 2, Last: 2}, {Start: 5, Last: 7}, {Start: 13, Last: 14}, {Start: 17, Last: 17}},
		},
	}

	for i, test := range tests {
		a := NewContainerArray(test.array)
		a = a.arrayToRun(0)
		if !reflect.DeepEqual(a.runs(), test.exp) {
			t.Fatalf("test #%v expected %v, but got %v", i, test.exp, a.runs())
		}
	}
}

func TestRunToArray(t *testing.T) {
	tests := []struct {
		runs []Interval16
		exp  []uint16
	}{
		{
			runs: []Interval16{},
			exp:  []uint16{},
		},
		{
			runs: []Interval16{{Start: 0, Last: 0}},
			exp:  []uint16{0},
		},
		{
			runs: []Interval16{{Start: 0, Last: 4}},
			exp:  []uint16{0, 1, 2, 3, 4},
		},
		{
			runs: []Interval16{{Start: 2, Last: 2}, {Start: 5, Last: 7}, {Start: 13, Last: 14}, {Start: 17, Last: 17}},
			exp:  []uint16{2, 5, 6, 7, 13, 14, 17},
		},
	}

	for i, test := range tests {
		a := NewContainerRun(test.runs)
		a = a.runToArray()
		if !reflect.DeepEqual(a.array(), test.exp) {
			t.Fatalf("test #%v expected %v, but got %v", i, test.exp, a.array())
		}
	}
}

func TestBitmapZeroRange(t *testing.T) {
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
		c := NewContainerBitmap(-1, test.bitmap)
		bitmap := c.bitmap()
		c.bitmapZeroRange(test.start, test.last+1)
		if !reflect.DeepEqual(bitmap[:len(test.exp)], test.exp) {
			t.Fatalf("test %#v expected %x, got %x", i, test.exp, bitmap[:len(test.bitmap)])
		}
		if test.expN != c.N() {
			t.Fatalf("test #%v expected n to be %v, but got %v", i, test.expN, c.N())
		}
		for i := range test.bitmap {
			bitmap[i] = 0
		}
	}

}

func TestUnionBitmapRun(t *testing.T) {
	tests := []struct {
		bitmap []uint64
		runs   []Interval16
		exp    []uint64
		expN   int32
	}{
		{
			bitmap: []uint64{2},
			runs:   []Interval16{{Start: 0, Last: 0}, {Start: 2, Last: 5}, {Start: 62, Last: 71}, {Start: 77, Last: 78}},
			exp:    []uint64{0xC00000000000003F, 0x60FF},
			expN:   18,
		},
	}
	for i, test := range tests {
		a := NewContainerBitmap(-1, test.bitmap)
		b := NewContainerRun(test.runs)
		ret := unionBitmapRun(a, b)
		if ret.isArray() {
			ret = ret.arrayToBitmap()
		}
		bitmap := ret.bitmap()
		if !reflect.DeepEqual(bitmap[:len(test.exp)], test.exp) {
			t.Fatalf("test #%v expected %x, but got %x", i, test.exp, bitmap[:len(test.exp)])
		}
		if ret.N() != test.expN {
			t.Fatalf("test #%v expected n to be %v, but got %v", i, test.expN, ret.N())
		}
		for i := range test.bitmap {
			a.bitmap()[i] = 0
		}
	}
}

func TestBitmapCountRuns(t *testing.T) {
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
	var c *Container

	for i, test := range tests {
		c = NewContainerBitmap(-1, test.bitmap)
		ret := c.bitmapCountRuns()
		if ret != test.exp {
			t.Fatalf("test #%v expected %v but got %v", i, test.exp, ret)
		}

		for j := range test.bitmap {
			c.bitmap()[j] = 0
		}
	}

	test := tests[3]
	for j, v := range test.bitmap {
		c.bitmap()[1024-len(test.bitmap)+j] = v

	}
	ret := c.bitmapCountRuns()
	if ret != test.exp {
		t.Fatalf("test at end expected %v but got %v", test.exp, ret)
	}
}

func TestArrayCountRuns(t *testing.T) {
	c := NewContainerArray(nil)
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
		c.setArray(test.array)
		ret := c.arrayCountRuns()
		if ret != test.exp {
			t.Fatalf("test #%v expected %v but got %v", i, test.exp, ret)
		}
	}
}

func TestDifferenceArrayRun(t *testing.T) {
	tests := []struct {
		array []uint16
		runs  []Interval16
		exp   []uint16
	}{
		{
			array: []uint16{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12},
			runs:  []Interval16{{Start: 5, Last: 10}},
			exp:   []uint16{0, 1, 2, 3, 4, 11, 12},
		},
	}
	for i, test := range tests {
		a := NewContainerArray(test.array)
		b := NewContainerRun(test.runs)
		ret := differenceArrayRun(a, b)
		if !reflect.DeepEqual(ret.array(), test.exp) {
			t.Fatalf("test #%v expected %v, but got %v", i, test.exp, ret.array())
		}
	}
}

func TestDifferenceRunArray(t *testing.T) {
	tests := []struct {
		runs  []Interval16
		array []uint16
		exp   []Interval16
	}{
		{
			runs:  []Interval16{{Start: 0, Last: 12}},
			array: []uint16{5, 6, 7, 8, 9, 10},
			exp:   []Interval16{{Start: 0, Last: 4}, {Start: 11, Last: 12}},
		},
		{
			runs:  []Interval16{{Start: 0, Last: 12}},
			array: []uint16{0, 1, 2, 3},
			exp:   []Interval16{{Start: 4, Last: 12}},
		},
		{
			runs:  []Interval16{{Start: 0, Last: 12}},
			array: []uint16{9, 10, 11, 12, 13},
			exp:   []Interval16{{Start: 0, Last: 8}},
		},
		{
			runs:  []Interval16{{Start: 1, Last: 12}},
			array: []uint16{0, 9, 10, 11, 12, 13},
			exp:   []Interval16{{Start: 1, Last: 8}},
		},
		{
			runs:  []Interval16{{Start: 1, Last: 12}, {Start: 14, Last: 14}, {Start: 18, Last: 18}},
			array: []uint16{0, 9, 10, 11, 12, 13, 14, 17},
			exp:   []Interval16{{Start: 1, Last: 8}, {Start: 18, Last: 18}},
		},
		{
			runs:  []Interval16{{Start: 1, Last: 12}, {Start: 14, Last: 14}, {Start: 18, Last: 18}},
			array: []uint16{0, 9, 10, 11, 12, 13, 14, 17, 19},
			exp:   []Interval16{{Start: 1, Last: 8}, {Start: 18, Last: 18}},
		},
		{
			runs:  []Interval16{{Start: 1, Last: 12}, {Start: 14, Last: 17}, {Start: 19, Last: 28}},
			array: []uint16{0, 9, 10, 11, 12, 13, 14, 17, 19, 25, 27},
			exp:   []Interval16{{Start: 1, Last: 8}, {Start: 15, Last: 16}, {Start: 20, Last: 24}, {Start: 26, Last: 26}, {Start: 28, Last: 28}},
		},
		{
			runs:  []Interval16{{Start: 0, Last: 20}, {Start: 65533, Last: 65535}},
			array: []uint16{65533, 65534, 65535},
			exp:   []Interval16{{Start: 0, Last: 20}},
		},
		{
			runs:  []Interval16{{Start: 0, Last: 20}, {Start: 65530, Last: 65535}},
			array: []uint16{37, 65535},
			exp:   []Interval16{{Start: 0, Last: 20}, {Start: 65530, Last: 65534}},
		},
	}
	for i, test := range tests {
		a := NewContainerRun(test.runs)
		b := NewContainerArray(test.array)
		ret := differenceRunArray(a, b)
		if !reflect.DeepEqual(ret.runs(), test.exp) {
			t.Fatalf("test #%v expected %v, but got %v", i, test.exp, ret.runs())
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
	c = c.arrayToBitmap()
	return c.bitmap()
}

func TestDifferenceRunBitmap(t *testing.T) {
	tests := []struct {
		runs   []Interval16
		bitmap []uint64
		exp    []Interval16
	}{
		{
			runs:   []Interval16{{Start: 0, Last: 63}},
			bitmap: MakeBitmap([]uint64{0x0000FFFF000000F0}),
			exp:    []Interval16{{Start: 0, Last: 3}, {Start: 8, Last: 31}, {Start: 48, Last: 63}},
		},
		{
			runs:   []Interval16{{Start: 0, Last: 63}},
			bitmap: MakeBitmap([]uint64{0x8000000000000000}),
			exp:    []Interval16{{Start: 0, Last: 62}},
		},
		{
			runs:   []Interval16{{Start: 0, Last: 63}},
			bitmap: MakeBitmap([]uint64{0x0000000000000001}),
			exp:    []Interval16{{Start: 1, Last: 63}},
		},
		{
			runs:   []Interval16{{Start: 0, Last: 63}},
			bitmap: MakeBitmap([]uint64{0x0, 0x0000000000000001}),
			exp:    []Interval16{{Start: 0, Last: 63}},
		},
		{
			runs:   []Interval16{{Start: 0, Last: 65}},
			bitmap: MakeBitmap([]uint64{0x0, 0x0000000000000001}),
			exp:    []Interval16{{Start: 0, Last: 63}, {Start: 65, Last: 65}},
		},
		{
			runs:   []Interval16{{Start: 0, Last: 65}},
			bitmap: MakeBitmap([]uint64{0x0, 0x8000000000000000}),
			exp:    []Interval16{{Start: 0, Last: 65}},
		},
		{
			runs:   []Interval16{{Start: 1, Last: 65535}},
			bitmap: MakeBitmap([]uint64{0x0000000000000001}),
			exp:    []Interval16{{Start: 1, Last: 65535}},
		},
		{
			runs:   []Interval16{{Start: 0, Last: 65533}, {Start: 65535, Last: 65535}},
			bitmap: MakeLastBitSet(),
			exp:    []Interval16{{Start: 0, Last: 65533}},
		},
	}
	for i, test := range tests {
		a := NewContainerRun(test.runs)
		b := NewContainerBitmap(-1, test.bitmap)
		ret := differenceRunBitmap(a, b)
		if !reflect.DeepEqual(ret.runs(), test.exp) {
			t.Fatalf("test #%v expected %v, but got %v", i, test.exp, ret.runs())
		}
	}
}

func TestDifferenceBitmapRun(t *testing.T) {
	tests := []struct {
		bitmap []uint64
		runs   []Interval16
		exp    []uint64
	}{
		{
			bitmap: []uint64{0xFFFFFFFFFFFFFFFF},
			runs:   []Interval16{{Start: 4, Last: 7}, {Start: 32, Last: 47}},
			exp:    []uint64{0xFFFF0000FFFFFF0F},
		},
		{
			bitmap: []uint64{0xFFFFFFFFFFFFFFBF},
			runs:   []Interval16{{Start: 0, Last: 5}, {Start: 7, Last: 63}},
			exp:    []uint64{0x0000000000000000},
		},
		{
			bitmap: []uint64{0xFFFFFFFFFFFFFFBF},
			runs:   []Interval16{{Start: 0, Last: 5}},
			exp:    []uint64{0xFFFFFFFFFFFFFF80},
		},
		{
			bitmap: []uint64{0xFFFFFFFFFFFFFFFF},
			runs:   []Interval16{{Start: 60, Last: 63}},
			exp:    []uint64{0x0FFFFFFFFFFFFFFF},
		},
		{
			bitmap: []uint64{0xFFFFFFFFFFFFFFFF},
			runs:   []Interval16{{Start: 60, Last: 65}},
			exp:    []uint64{0x0FFFFFFFFFFFFFFF},
		},
		{
			bitmap: []uint64{0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF},
			runs:   []Interval16{{Start: 60, Last: 65}, {Start: 67, Last: 72}, {Start: 126, Last: 130}},
			exp:    []uint64{0x0FFFFFFFFFFFFFFF, 0x3FFFFFFFFFFFFE04, 0xFFFFFFFFFFFFFFF8},
		},
		{
			bitmap: []uint64{0x0000000000000001},
			runs:   []Interval16{{Start: 0, Last: 0}},
			exp:    []uint64{0x0000000000000000},
		},
		{
			bitmap: []uint64{0x8000000000000000},
			runs:   []Interval16{{Start: 63, Last: 63}},
			exp:    []uint64{0x0000000000000000},
		},
		{
			bitmap: []uint64{0xC000000000000000, 0x0000000000000003},
			runs:   []Interval16{{Start: 63, Last: 64}},
			exp:    []uint64{0x4000000000000000, 0x0000000000000002},
		},
		{
			bitmap: []uint64{0x0000000000000000},
			runs:   []Interval16{{Start: 5, Last: 7}},
			exp:    []uint64{0x0000000000000000},
		}, {
			bitmap: bitmapLastBitSet(),
			runs:   []Interval16{{Start: 65535, Last: 65535}},
			exp:    bitmapEmpty(),
		},
		{
			bitmap: bitmapFull(),
			runs:   []Interval16{{Start: 0, Last: 65535}},
			exp:    bitmapEmpty(),
		},
	}
	for i, test := range tests {
		a := NewContainerBitmap(-1, test.bitmap)
		b := NewContainerRun(test.runs)
		ret := differenceBitmapRun(a, b)
		if !reflect.DeepEqual(ret.bitmap()[:len(test.exp)], test.exp) {
			t.Fatalf("test #%v expected \n%X, but got \n%X", i, test.exp, ret.bitmap()[:len(test.exp)])
		}
	}
}

func TestDifferenceBitmapArray(t *testing.T) {
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
		b := NewContainerBitmap(-1, test.bitmap[:1])
		a := NewContainerArray(test.array)
		ret := differenceBitmapArray(b, a)
		if !reflect.DeepEqual(ret.array(), test.exp) {
			t.Fatalf("test #%v expected %#v, but got %#v", i, test.exp, ret.array())
		}
	}
}

func TestDifferenceBitmapBitmap(t *testing.T) {
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
		a := NewContainerBitmap(-1, test.abitmap)
		b := NewContainerBitmap(-1, test.bbitmap)
		ret := differenceBitmapBitmap(a, b)
		if !reflect.DeepEqual(ret.array(), test.exp) {
			t.Fatalf("test #%v expected \n%X, but got \n%X", i, test.exp, ret.array())
		}
	}
}

func TestDifferenceRunRun(t *testing.T) {
	tests := []struct {
		aruns []Interval16
		bruns []Interval16
		exp   []Interval16
		expn  int32
	}{
		{
			// this tests all six overlap combinations
			// A                     [   ]                   [   ]                        [ ]              [     ]             [   ]                 [  ]
			// B                    [     ]                [   ]                     [ ]                     [  ]                [   ]                    [  ]
			aruns: []Interval16{{Start: 3, Last: 6}, {Start: 13, Last: 16}, {Start: 24, Last: 26}, {Start: 33, Last: 38}, {Start: 43, Last: 46}, {Start: 53, Last: 56}},
			bruns: []Interval16{{Start: 1, Last: 8}, {Start: 11, Last: 14}, {Start: 21, Last: 23}, {Start: 35, Last: 37}, {Start: 44, Last: 48}, {Start: 57, Last: 59}},
			exp:   []Interval16{{Start: 15, Last: 16}, {Start: 24, Last: 26}, {Start: 33, Last: 34}, {Start: 38, Last: 38}, {Start: 43, Last: 43}, {Start: 53, Last: 56}},
			expn:  13,
		},
	}
	for i, test := range tests {
		a := NewContainerRun(test.aruns)
		b := NewContainerRun(test.bruns)
		ret := differenceRunRun(a, b)
		if !reflect.DeepEqual(ret.runs(), test.exp) {
			t.Fatalf("test #%v expected %v, but got %v", i, test.exp, ret.runs())
		}
		if ret.N() != test.expn {
			t.Fatalf("test #%v expected n=%v, but got n=%v", i, test.expn, ret.N())
		}
	}
}

func TestWriteReadArray(t *testing.T) {
	ca := NewContainerArray([]uint16{1, 10, 100, 1000})
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
	if !reflect.DeepEqual(ba2.Containers.Get(0).array(), ca.array()) {
		t.Fatalf("array test expected %x, but got %x", ca.array(), ba2.Containers.Get(0).array())
	}
}

func TestWriteReadBitmap(t *testing.T) {
	// create bitmap containing > 4096 bits
	cb := NewContainerBitmapN(nil, 0)
	for i := 0; i < 129; i++ {
		cb.bitmap()[i] = 0x5555555555555555
		cb.n += 32
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
	if !reflect.DeepEqual(bb2.Containers.Get(0).bitmap(), cb.bitmap()) {
		t.Fatalf("bitmap test expected %x, but got %x", cb.bitmap(), bb2.Containers.Get(0).bitmap())
	}
}

func TestWriteReadFullBitmap(t *testing.T) {
	// create bitmap containing > 4096 bits
	cb := NewContainerBitmapN(nil, 0)
	for i := 0; i < bitmapN; i++ {
		cb.bitmap()[i] = 0xffffffffffffffff
		cb.n += 64
	}
	bb := NewFileBitmap()
	bb.Containers.Put(0, cb)
	bb2 := NewFileBitmap()
	var buf bytes.Buffer
	_, err := bb.writeToUnoptimized(&buf)
	if err != nil {
		t.Fatalf("error writing: %v", err)
	}
	if !cb.isBitmap() {
		t.Fatalf("how can i test a bitmap if it's not a bitmap")
	}
	err = bb2.UnmarshalBinary(buf.Bytes())
	if err != nil {
		t.Fatalf("error unmarshaling: %v", err)
	}
	if !reflect.DeepEqual(bb2.Containers.Get(0).bitmap(), cb.bitmap()) {
		t.Fatalf("bitmap test expected %x, but got %x", cb.bitmap(), bb2.Containers.Get(0).bitmap())
	}

	if bb2.Containers.Get(0).N() != cb.N() {
		t.Fatalf("bitmap test expected count %x, but got %x", cb.N(), bb2.Containers.Get(0).N())
	}
	if bb2.Containers.Get(0).count() != cb.count() {
		t.Fatalf("bitmap test expected count %x, but got %x", cb.N(), bb2.Containers.Get(0).N())
	}
}

func TestWriteReadRun(t *testing.T) {
	cr := NewContainerRun([]Interval16{{Start: 3, Last: 13}, {Start: 100, Last: 109}})
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
	if !reflect.DeepEqual(br2.Containers.Get(0).runs(), cr.runs()) {
		t.Fatalf("run test expected %x, but got %x", cr.runs(), br2.Containers.Get(0).runs())
	}
}

func TestXorArrayRun(t *testing.T) {
	tests := []struct {
		a   *Container
		b   *Container
		exp *Container
	}{
		{
			a:   NewContainerArray([]uint16{1, 5, 10, 11, 12}),
			b:   NewContainerRun([]Interval16{{Start: 2, Last: 10}, {Start: 12, Last: 13}, {Start: 15, Last: 16}}),
			exp: NewContainerArray([]uint16{1, 2, 3, 4, 6, 7, 8, 9, 11, 13, 15, 16}),
		}, {
			a:   NewContainerArray([]uint16{1, 5, 10, 11, 12, 13, 14}),
			b:   NewContainerRun([]Interval16{{Start: 2, Last: 10}, {Start: 12, Last: 13}, {Start: 15, Last: 16}}),
			exp: NewContainerArray([]uint16{1, 2, 3, 4, 6, 7, 8, 9, 11, 14, 15, 16}),
		}, {
			a:   NewContainerArray([]uint16{65535}),
			b:   NewContainerRun([]Interval16{{Start: 65534, Last: 65535}}),
			exp: NewContainerArray([]uint16{65534}),
		}, {
			a:   NewContainerArray([]uint16{65535}),
			b:   NewContainerRun([]Interval16{{Start: 65535, Last: 65535}}),
			exp: NewContainerArray([]uint16{}),
		},
	}

	for i, test := range tests {
		test.a.setN(test.a.count())
		test.b.setN(test.b.count())
		ret := xor(test.a, test.b)
		if !reflect.DeepEqual(ret.array(), test.exp.array()) {
			t.Fatalf("test #%v expected %#v, but got %#v", i, test.exp, ret)
		}
		ret = xor(test.b, test.a)
		if !reflect.DeepEqual(ret.array(), test.exp.array()) {
			t.Fatalf("test #%v.1 expected %#v, but got %#v", i, test.exp, ret)
		}
	}

}

//special case that didn't fit the xorrunrun table testing below.
func TestXorRunRun1(t *testing.T) {
	a := NewContainerRun([]Interval16{{Start: 4, Last: 10}})
	b := NewContainerRun([]Interval16{{Start: 5, Last: 10}})
	ret := xorRunRun(a, b)
	if !reflect.DeepEqual(ret.array(), []uint16{4}) {
		t.Fatalf("test #1 expected %v, but got %v", []uint16{4}, ret.array())
	}
	ret = xorRunRun(b, a)
	if !reflect.DeepEqual(ret.array(), []uint16{4}) {
		t.Fatalf("test #1 expected %v, but got %v", []uint16{4}, ret.array())
	}
}

func TestXorRunRun(t *testing.T) {
	a := NewContainerRun(nil)
	b := NewContainerRun(nil)
	tests := []struct {
		aruns []Interval16
		bruns []Interval16
		exp   []Interval16
	}{
		{
			aruns: []Interval16{},
			bruns: []Interval16{{Start: 5, Last: 10}},
			exp:   []Interval16{{Start: 5, Last: 10}},
		},
		{
			aruns: []Interval16{{Start: 0, Last: 4}},
			bruns: []Interval16{{Start: 6, Last: 10}},
			exp:   []Interval16{{Start: 0, Last: 4}, {Start: 6, Last: 10}},
		},
		{
			aruns: []Interval16{{Start: 0, Last: 6}},
			bruns: []Interval16{{Start: 4, Last: 10}},
			exp:   []Interval16{{Start: 0, Last: 3}, {Start: 7, Last: 10}},
		},
		{
			aruns: []Interval16{{Start: 4, Last: 10}},
			bruns: []Interval16{{Start: 0, Last: 6}},
			exp:   []Interval16{{Start: 0, Last: 3}, {Start: 7, Last: 10}},
		},
		{
			aruns: []Interval16{{Start: 0, Last: 10}},
			bruns: []Interval16{{Start: 0, Last: 6}},
			exp:   []Interval16{{Start: 7, Last: 10}},
		},
		{
			aruns: []Interval16{{Start: 0, Last: 6}},
			bruns: []Interval16{{Start: 0, Last: 10}},
			exp:   []Interval16{{Start: 7, Last: 10}},
		},
		{
			aruns: []Interval16{{Start: 0, Last: 6}},
			bruns: []Interval16{{Start: 0, Last: 10}},
			exp:   []Interval16{{Start: 7, Last: 10}},
		},
		{
			aruns: []Interval16{{Start: 5, Last: 12}},
			bruns: []Interval16{{Start: 5, Last: 10}},
			exp:   []Interval16{{Start: 11, Last: 12}},
		},
		{
			aruns: []Interval16{{Start: 1, Last: 3}, {Start: 5, Last: 5}, {Start: 7, Last: 12}},
			bruns: []Interval16{{Start: 5, Last: 10}},
			exp:   []Interval16{{Start: 1, Last: 3}, {Start: 6, Last: 6}, {Start: 11, Last: 12}},
		},
		{
			aruns: []Interval16{{Start: 1, Last: 3}, {Start: 5, Last: 5}, {Start: 7, Last: 12}},
			bruns: []Interval16{{Start: 2, Last: 65535}},
			exp:   []Interval16{{Start: 1, Last: 1}, {Start: 4, Last: 4}, {Start: 6, Last: 6}, {Start: 13, Last: 65535}},
		},
		{
			aruns: []Interval16{{Start: 2, Last: 65535}},
			bruns: []Interval16{{Start: 1, Last: 3}, {Start: 5, Last: 5}, {Start: 7, Last: 12}},
			exp:   []Interval16{{Start: 1, Last: 1}, {Start: 4, Last: 4}, {Start: 6, Last: 6}, {Start: 13, Last: 65535}},
		},
		{
			aruns: []Interval16{{Start: 1, Last: 3}, {Start: 5, Last: 5}, {Start: 7, Last: 12}},
			bruns: []Interval16{{Start: 0, Last: 65535}},
			exp:   []Interval16{{Start: 0, Last: 0}, {Start: 4, Last: 4}, {Start: 6, Last: 6}, {Start: 13, Last: 65535}},
		},
		{
			aruns: []Interval16{{Start: 0, Last: 65535}},
			bruns: []Interval16{{Start: 1, Last: 3}, {Start: 5, Last: 5}, {Start: 7, Last: 12}},
			exp:   []Interval16{{Start: 0, Last: 0}, {Start: 4, Last: 4}, {Start: 6, Last: 6}, {Start: 13, Last: 65535}},
		},
		{
			aruns: []Interval16{{Start: 1, Last: 3}, {Start: 5, Last: 5}, {Start: 7, Last: 9}, {Start: 12, Last: 22}},
			bruns: []Interval16{{Start: 2, Last: 8}, {Start: 16, Last: 27}, {Start: 33, Last: 34}},
			exp:   []Interval16{{Start: 1, Last: 1}, {Start: 4, Last: 4}, {Start: 6, Last: 6}, {Start: 9, Last: 9}, {Start: 12, Last: 15}, {Start: 23, Last: 27}, {Start: 33, Last: 34}},
		},
		{
			aruns: []Interval16{{Start: 65530, Last: 65535}},
			bruns: []Interval16{{Start: 65532, Last: 65535}},
			exp:   []Interval16{{Start: 65530, Last: 65531}},
		},
	}
	for i, test := range tests {
		a.setRuns(test.aruns)
		b.setRuns(test.bruns)
		ret := xorRunRun(a, b)
		if !reflect.DeepEqual(ret.runs(), test.exp) {
			t.Fatalf("test #%v expected %v, but got %v", i, test.exp, ret.runs())
		}
		ret = xorRunRun(b, a)
		if !reflect.DeepEqual(ret.runs(), test.exp) {
			t.Fatalf("test #%v.1 expected %v, but got %v", i, test.exp, ret.runs())
		}
	}
}

func TestBitmapXorRange(t *testing.T) {
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
		c := NewContainerBitmap(-1, test.bitmap)
		c.bitmapXorRange(test.start, test.last+1)
		if !reflect.DeepEqual(c.bitmap()[:len(test.exp)], test.exp) {
			t.Fatalf("test %#v expected %x, got %x", i, test.exp, c.bitmap()[:len(test.bitmap)])
		}
		if test.expN != c.N() {
			t.Fatalf("test #%v expected n to be %v, but got %v", i, test.expN, c.N())
		}
	}
}

func TestXorBitmapRun(t *testing.T) {
	tests := []struct {
		bitmap []uint64
		runs   []Interval16
		exp    []uint64
	}{
		{
			bitmap: []uint64{0x0, 0x0, 0x0},
			runs:   []Interval16{{Start: 129, Last: 131}},
			exp:    []uint64{0x0, 0x0, 0x00000000000000E},
		},
	}
	for i, test := range tests {
		a := NewContainerBitmap(-1, test.bitmap)
		e := NewContainerBitmap(-1, test.exp)
		b := NewContainerRun(test.runs)
		//xorBitmapRun
		ret := xor(a, b)
		if ret.isRun() {
			ret = ret.runToBitmap()
		}
		if !reflect.DeepEqual(ret.bitmap(), e.bitmap()) {
			t.Fatalf("test #%v expected %v, but got %v", i, e.bitmap(), ret.bitmap())
		}
		ret = xor(b, a)
		if ret.isRun() {
			ret = ret.runToBitmap()
		}
		if !reflect.DeepEqual(ret.bitmap(), e.bitmap()) {
			t.Fatalf("test #%v.1 expected %v, but got %v", i, e.bitmap(), ret.bitmap())
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

	itr.Seek(65535)
	if !(itr.key == 1 && itr.j == -1) {
		t.Fatalf("iterator did not seek missing value in previous container correctly: %v\n", itr)
	}
	val, eof = itr.Next()
	if !(val == 90000 && !eof) {
		t.Fatalf("iterator did not next from missing value in previous container correctly: %d, %v\n", val, eof)
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

	// Test for seeking value not in bitmap, where next container that the iterator should
	// go to has values with low bits smaller than the low bits of seek.
	b = NewBitmap(65537, 65538, 65539, 65541, 65542)
	itr = b.Iterator()
	// Both 65536+5-1 and 5 are not in b.
	itr.Seek(5)
	if !(itr.key == 1 && itr.j == -1) {
		t.Fatalf("iterator did not seek correctly in next container: %v\n", itr)
	}
	val, eof = itr.Next()
	if !(val == 65537 && !eof) {
		t.Fatalf("iterator did not next corrrectly to next container: %d, %v\n", val, eof)
	}
	val, eof = itr.Next()
	if !(val == 65538 && !eof) {
		t.Fatalf("iterator did not next correctly: %d, %v\n", val, eof)
	}
}

func TestIteratorBitmap(t *testing.T) {
	// use values that span two containers
	// this dataset will update to bitmap after enough Adds,
	// but won't update to RLE until Optimize() is called
	b := NewFileBitmap()
	for i := uint64(61000); i < 71000; i++ {
		if _, err := b.Add(i); err != nil {
			t.Fatalf("adding bit: %v", err)
		}
	}
	for i := uint64(75000); i < 75100; i++ {
		if _, err := b.Add(i); err != nil {
			t.Fatalf("adding bit: %v", err)
		}
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

	// Test for seeking value not in bitmap, where next container that the iterator should
	// go to has values with low bits smaller than the low bits of seek.
	for i := uint64(65536*3 + 2); i < 65536*3+4110; i++ {
		if i != 65536*3+5 && i != 65536*3+7 {
			if _, err := b.Add(i); err != nil {
				t.Fatalf("adding bit: %v", err)
			}
		}
	}

	// We expect this to be a bitmap container because more than
	// 4096 bits have been set, but Optimize() has not been called.
	if !b.Containers.Get(3).isBitmap() {
		t.Fatalf("wrong container type")
	}

	// Both 65536*2+5 and 65536*3+5 are not in b.
	itr.Seek(65536*2 + 5)
	if !(itr.key == 3 && itr.j == -1) {
		t.Fatalf("iterator did not seek correctly in next container: %v\n", itr)
	}
	val, eof = itr.Next()
	if !((val == 65536*3+2) && !eof) {
		t.Fatalf("iterator did not next correctly to next container: %d, %v\n", val, eof)
	}
	val, eof = itr.Next()
	if !((val == 65536*3+3) && !eof) {
		t.Fatalf("iterator did not next correctly to next container: %d, %v\n", val, eof)
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

	itr.Seek(1007)
	if !(itr.key == 1 && itr.j == -1 && itr.k == -1) {
		t.Fatalf("iterator did not seek correctly to end of run: %v\n", itr)
	}
	val, eof = itr.Next()
	if !(val == 100000 && !eof) {
		t.Fatalf("iterator did not next correctly across containers: %v, %v", val, itr)
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

	// Test for seeking value not in bitmap, where next container that the iterator should
	// go to has values with low bits smaller than the low bits of seek.
	for i := uint64(65536*3 + 1); i <= 65536*3+8; i++ {
		if _, err := b.Add(i); err != nil {
			t.Fatalf("adding bit: %v", err)
		}
	}
	for i := uint64(65536*3 + 10); i <= 65536*3+20; i++ {
		if _, err := b.Add(i); err != nil {
			t.Fatalf("adding bit: %v", err)
		}
	}
	b.Optimize()

	if !b.Containers.Get(3).isRun() {
		t.Fatalf("wrong container type")
	}

	// Both 65536*2+9 and 65536*3+9 are not in b.
	itr.Seek(65536*2 + 9)
	if !(itr.key == 3 && itr.j == 0 && itr.k == -1) {
		t.Fatalf("iterator did not seek correctly in next container: %v\n", itr)
	}
	val, eof = itr.Next()
	if !((val == 65536*3+1) && !eof) {
		t.Fatalf("iterator did not next correctly to next container: %d, %v\n", val, eof)
	}
	val, eof = itr.Next()
	if !((val == 65536*3+2) && !eof) {
		t.Fatalf("iterator did not next correctly to next container: %d, %v\n", val, eof)
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
		runs  []Interval16
		index uint16
		exp   struct {
			index int32
			found bool
		}
	}{
		{
			runs:  []Interval16{{Start: 0, Last: 10}},
			index: uint16(3),
			exp: struct {
				index int32
				found bool
			}{index: 0, found: true},
		},
		{
			runs:  []Interval16{{Start: 0, Last: 10}},
			index: uint16(13),
			exp: struct {
				index int32
				found bool
			}{index: 0, found: false},
		},
		{
			runs:  []Interval16{{Start: 0, Last: 10}, {Start: 20, Last: 30}},
			index: uint16(13),
			exp: struct {
				index int32
				found bool
			}{index: 0, found: false},
		},
		{
			runs:  []Interval16{{Start: 0, Last: 10}, {Start: 20, Last: 30}},
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
		idx, found := BinSearchRuns(index, runs)

		if test.exp.index != idx && test.exp.found != found {
			t.Fatalf("test #%v expected %v , but got %v %v", i, test.exp, idx, found)
		}
	}
}

func TestRunBinSearch(t *testing.T) {
	tests := []struct {
		runs   []Interval16
		search uint16
		exp    bool
		expi   int32
	}{
		{
			runs:   []Interval16{{2, 10}, {50, 60}, {80, 90}},
			search: 1,
			exp:    false,
			expi:   0,
		},
		{
			runs:   []Interval16{{2, 10}, {50, 60}, {80, 90}},
			search: 2,
			exp:    true,
			expi:   0,
		},
		{
			runs:   []Interval16{{2, 10}, {50, 60}, {80, 90}},
			search: 5,
			exp:    true,
			expi:   0,
		},
		{
			runs:   []Interval16{{2, 10}, {50, 60}, {80, 90}},
			search: 10,
			exp:    true,
			expi:   0,
		},
		{
			runs:   []Interval16{{2, 10}, {50, 60}, {80, 90}},
			search: 20,
			exp:    false,
			expi:   1,
		},
		{
			runs:   []Interval16{{2, 10}, {50, 60}, {80, 90}},
			search: 55,
			exp:    true,
			expi:   1,
		},
		{
			runs:   []Interval16{{2, 10}, {50, 60}, {80, 90}},
			search: 70,
			exp:    false,
			expi:   2,
		},
		{
			runs:   []Interval16{{2, 10}, {50, 60}, {80, 90}},
			search: 100,
			exp:    false,
			expi:   3,
		},
	}
	for i, test := range tests {
		idx, contains := BinSearchRuns(test.search, test.runs)
		if !(test.exp == contains && test.expi == idx) {
			t.Fatalf("test #%v expected (%v, %v) but got (%v, %v)", i, test.exp, test.expi, contains, idx)
		}
	}
}
func TestBitmap_RemoveEmptyContainers(t *testing.T) {
	bm1 := NewFileBitmap(1<<16, 2<<16, 3<<16)
	bm2 := NewFileBitmap(1<<16, 2<<16+1, 3<<16)
	bm3 := bm1.Intersect(bm2)
	if bm3.countNonEmptyContainers() != 2 {
		t.Fatalf("Should be 1 empty container ")
	}
	bm3.removeEmptyContainers()

	if bm3.countNonEmptyContainers() != bm3.Containers.Size() {
		t.Fatalf("Should be no empty containers ")
	}
}

func TestBitmap_BitmapWriteToWithEmpty(t *testing.T) {
	bm1 := NewFileBitmap(1<<16, 2<<16, 3<<16)
	if _, err := bm1.Remove(2 << 16); err != nil {
		t.Fatalf("removing a bit: %v", err)
	}
	var buf bytes.Buffer
	if _, err := bm1.WriteTo(&buf); err != nil {
		t.Fatalf("Failure to write to bitmap buffer. ")
	}
	bm0 := NewFileBitmap()
	if err := bm0.UnmarshalBinary(buf.Bytes()); err != nil {
		t.Fatalf("unmarshalling: %v", err)
	}
	if bm0.countNonEmptyContainers() != bm0.Containers.Size() {
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
		a := NewContainerArray(test.array)
		b := NewContainerBitmap(-1, test.bitmap)
		ret := intersectArrayBitmap(a, b).array()
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
		if _, err := b.Add(i); err != nil {
			t.Fatalf("adding bit: %v", err)
		}
	}
	c := b.Clone()
	if _, err := b.BitwiseEqual(c); err != nil {
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

func unionInPlaceWrapper(a, b *Container) *Container {
	ret := a.Clone().unionInPlace(b)
	ret.Repair()
	return ret
}

func differenceInPlaceWrapper(a, b *Container) *Container {
	a = a.Clone()
	a = a.differenceInPlace(b)
	return a
}

func intersectInPlaceWrapper(a, b *Container) *Container {
	return a.Clone().intersectInPlace(b)
}

func TestContainerBitwiseCompare(t *testing.T) {
	cts := setupContainerTests()

	for t1, containers := range cts {
		for name, c := range containers {
			for t2, other := range cts {
				for otherName, otherC := range other {
					err := c.BitwiseCompare(otherC)
					if err != nil {
						if otherName == name {
							t.Fatalf("container types %d/%d, contents %s: unexpected error %v",
								t1, t2, name, err)
						}
					} else {
						if name != otherName {
							t.Fatalf("container types %d/%d, unexpected %s == %s",
								t1, t2, name, otherName)
						}
					}
				}
			}
		}
	}
}

func TestContainerCombinations(t *testing.T) {

	cts := setupContainerTests()

	containerTypes := []byte{ContainerArray, ContainerBitmap, ContainerRun}

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

		// intersect in place
		{intersectInPlaceWrapper, "empty", "empty", "empty"},
		{intersectInPlaceWrapper, "empty", "full", "empty"},
		{intersectInPlaceWrapper, "empty", "firstBitSet", "empty"},
		{intersectInPlaceWrapper, "empty", "lastBitSet", "empty"},
		{intersectInPlaceWrapper, "empty", "firstBitUnset", "empty"},
		{intersectInPlaceWrapper, "empty", "lastBitUnset", "empty"},
		{intersectInPlaceWrapper, "empty", "innerBitsSet", "empty"},
		{intersectInPlaceWrapper, "empty", "outerBitsSet", "empty"},
		{intersectInPlaceWrapper, "empty", "oddBitsSet", "empty"},
		{intersectInPlaceWrapper, "empty", "evenBitsSet", "empty"},
		//
		{intersectInPlaceWrapper, "full", "empty", "empty"},
		{intersectInPlaceWrapper, "full", "full", "full"},
		{intersectInPlaceWrapper, "full", "firstBitSet", "firstBitSet"},
		{intersectInPlaceWrapper, "full", "lastBitSet", "lastBitSet"},
		{intersectInPlaceWrapper, "full", "firstBitUnset", "firstBitUnset"},
		{intersectInPlaceWrapper, "full", "lastBitUnset", "lastBitUnset"},
		{intersectInPlaceWrapper, "full", "innerBitsSet", "innerBitsSet"},
		{intersectInPlaceWrapper, "full", "outerBitsSet", "outerBitsSet"},
		{intersectInPlaceWrapper, "full", "oddBitsSet", "oddBitsSet"},
		{intersectInPlaceWrapper, "full", "evenBitsSet", "evenBitsSet"},
		//
		{intersectInPlaceWrapper, "firstBitSet", "empty", "empty"},
		{intersectInPlaceWrapper, "firstBitSet", "full", "firstBitSet"},
		{intersectInPlaceWrapper, "firstBitSet", "firstBitSet", "firstBitSet"},
		{intersectInPlaceWrapper, "firstBitSet", "lastBitSet", "empty"},
		{intersectInPlaceWrapper, "firstBitSet", "firstBitUnset", "empty"},
		{intersectInPlaceWrapper, "firstBitSet", "lastBitUnset", "firstBitSet"},
		{intersectInPlaceWrapper, "firstBitSet", "innerBitsSet", "empty"},
		{intersectInPlaceWrapper, "firstBitSet", "outerBitsSet", "firstBitSet"},
		{intersectInPlaceWrapper, "firstBitSet", "oddBitsSet", "empty"},
		{intersectInPlaceWrapper, "firstBitSet", "evenBitsSet", "firstBitSet"},
		//
		{intersectInPlaceWrapper, "lastBitSet", "empty", "empty"},
		{intersectInPlaceWrapper, "lastBitSet", "full", "lastBitSet"},
		{intersectInPlaceWrapper, "lastBitSet", "firstBitSet", "empty"},
		{intersectInPlaceWrapper, "lastBitSet", "lastBitSet", "lastBitSet"},
		{intersectInPlaceWrapper, "lastBitSet", "firstBitUnset", "lastBitSet"},
		{intersectInPlaceWrapper, "lastBitSet", "lastBitUnset", "empty"},
		{intersectInPlaceWrapper, "lastBitSet", "innerBitsSet", "empty"},
		{intersectInPlaceWrapper, "lastBitSet", "outerBitsSet", "lastBitSet"},
		{intersectInPlaceWrapper, "lastBitSet", "oddBitsSet", "lastBitSet"},
		{intersectInPlaceWrapper, "lastBitSet", "evenBitsSet", "empty"},
		//
		{intersectInPlaceWrapper, "firstBitUnset", "empty", "empty"},
		{intersectInPlaceWrapper, "firstBitUnset", "full", "firstBitUnset"},
		{intersectInPlaceWrapper, "firstBitUnset", "firstBitSet", "empty"},
		{intersectInPlaceWrapper, "firstBitUnset", "lastBitSet", "lastBitSet"},
		{intersectInPlaceWrapper, "firstBitUnset", "firstBitUnset", "firstBitUnset"},
		{intersectInPlaceWrapper, "firstBitUnset", "lastBitUnset", "innerBitsSet"},
		{intersectInPlaceWrapper, "firstBitUnset", "innerBitsSet", "innerBitsSet"},
		{intersectInPlaceWrapper, "firstBitUnset", "outerBitsSet", "lastBitSet"},
		{intersectInPlaceWrapper, "firstBitUnset", "oddBitsSet", "oddBitsSet"},
		//
		{intersectInPlaceWrapper, "lastBitUnset", "empty", "empty"},
		{intersectInPlaceWrapper, "lastBitUnset", "full", "lastBitUnset"},
		{intersectInPlaceWrapper, "lastBitUnset", "firstBitSet", "firstBitSet"},
		{intersectInPlaceWrapper, "lastBitUnset", "lastBitSet", "empty"},
		{intersectInPlaceWrapper, "lastBitUnset", "firstBitUnset", "innerBitsSet"},
		{intersectInPlaceWrapper, "lastBitUnset", "lastBitUnset", "lastBitUnset"},
		{intersectInPlaceWrapper, "lastBitUnset", "innerBitsSet", "innerBitsSet"},
		{intersectInPlaceWrapper, "lastBitUnset", "outerBitsSet", "firstBitSet"},
		{intersectInPlaceWrapper, "lastBitUnset", "evenBitsSet", "evenBitsSet"},
		//
		{intersectInPlaceWrapper, "innerBitsSet", "empty", "empty"},
		{intersectInPlaceWrapper, "innerBitsSet", "full", "innerBitsSet"},
		{intersectInPlaceWrapper, "innerBitsSet", "firstBitSet", "empty"},
		{intersectInPlaceWrapper, "innerBitsSet", "lastBitSet", "empty"},
		{intersectInPlaceWrapper, "innerBitsSet", "firstBitUnset", "innerBitsSet"},
		{intersectInPlaceWrapper, "innerBitsSet", "lastBitUnset", "innerBitsSet"},
		{intersectInPlaceWrapper, "innerBitsSet", "innerBitsSet", "innerBitsSet"},
		{intersectInPlaceWrapper, "innerBitsSet", "outerBitsSet", "empty"},
		//
		{intersectInPlaceWrapper, "outerBitsSet", "empty", "empty"},
		{intersectInPlaceWrapper, "outerBitsSet", "full", "outerBitsSet"},
		{intersectInPlaceWrapper, "outerBitsSet", "firstBitSet", "firstBitSet"},
		{intersectInPlaceWrapper, "outerBitsSet", "lastBitSet", "lastBitSet"},
		{intersectInPlaceWrapper, "outerBitsSet", "firstBitUnset", "lastBitSet"},
		{intersectInPlaceWrapper, "outerBitsSet", "lastBitUnset", "firstBitSet"},
		{intersectInPlaceWrapper, "outerBitsSet", "innerBitsSet", "empty"},
		{intersectInPlaceWrapper, "outerBitsSet", "outerBitsSet", "outerBitsSet"},
		{intersectInPlaceWrapper, "outerBitsSet", "oddBitsSet", "lastBitSet"},
		{intersectInPlaceWrapper, "outerBitsSet", "evenBitsSet", "firstBitSet"},
		//
		{intersectInPlaceWrapper, "oddBitsSet", "empty", "empty"},
		{intersectInPlaceWrapper, "oddBitsSet", "full", "oddBitsSet"},
		{intersectInPlaceWrapper, "oddBitsSet", "firstBitSet", "empty"},
		{intersectInPlaceWrapper, "oddBitsSet", "lastBitSet", "lastBitSet"},
		{intersectInPlaceWrapper, "oddBitsSet", "firstBitUnset", "oddBitsSet"},
		{intersectInPlaceWrapper, "oddBitsSet", "outerBitsSet", "lastBitSet"},
		{intersectInPlaceWrapper, "oddBitsSet", "oddBitsSet", "oddBitsSet"},
		{intersectInPlaceWrapper, "oddBitsSet", "evenBitsSet", "empty"},
		//
		{intersectInPlaceWrapper, "evenBitsSet", "empty", "empty"},
		{intersectInPlaceWrapper, "evenBitsSet", "full", "evenBitsSet"},
		{intersectInPlaceWrapper, "evenBitsSet", "firstBitSet", "firstBitSet"},
		{intersectInPlaceWrapper, "evenBitsSet", "lastBitSet", "empty"},
		{intersectInPlaceWrapper, "evenBitsSet", "lastBitUnset", "evenBitsSet"},
		{intersectInPlaceWrapper, "evenBitsSet", "outerBitsSet", "firstBitSet"},
		{intersectInPlaceWrapper, "evenBitsSet", "oddBitsSet", "empty"},
		{intersectInPlaceWrapper, "evenBitsSet", "evenBitsSet", "evenBitsSet"},

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

		// unionInPlaceWrapper
		{unionInPlaceWrapper, "empty", "empty", "empty"},
		{unionInPlaceWrapper, "empty", "full", "full"},
		{unionInPlaceWrapper, "empty", "firstBitSet", "firstBitSet"},
		{unionInPlaceWrapper, "empty", "lastBitSet", "lastBitSet"},
		{unionInPlaceWrapper, "empty", "firstBitUnset", "firstBitUnset"},
		{unionInPlaceWrapper, "empty", "lastBitUnset", "lastBitUnset"},
		{unionInPlaceWrapper, "empty", "innerBitsSet", "innerBitsSet"},
		{unionInPlaceWrapper, "empty", "outerBitsSet", "outerBitsSet"},
		{unionInPlaceWrapper, "empty", "oddBitsSet", "oddBitsSet"},
		{unionInPlaceWrapper, "empty", "evenBitsSet", "evenBitsSet"},
		//
		{unionInPlaceWrapper, "full", "empty", "full"},
		{unionInPlaceWrapper, "full", "full", "full"},
		{unionInPlaceWrapper, "full", "firstBitSet", "full"},
		{unionInPlaceWrapper, "full", "lastBitSet", "full"},
		{unionInPlaceWrapper, "full", "firstBitUnset", "full"},
		{unionInPlaceWrapper, "full", "lastBitUnset", "full"},
		{unionInPlaceWrapper, "full", "innerBitsSet", "full"},
		{unionInPlaceWrapper, "full", "outerBitsSet", "full"},
		{unionInPlaceWrapper, "full", "oddBitsSet", "full"},
		{unionInPlaceWrapper, "full", "evenBitsSet", "full"},
		//
		{unionInPlaceWrapper, "firstBitSet", "empty", "firstBitSet"},
		{unionInPlaceWrapper, "firstBitSet", "full", "full"},
		{unionInPlaceWrapper, "firstBitSet", "firstBitSet", "firstBitSet"},
		{unionInPlaceWrapper, "firstBitSet", "lastBitSet", "outerBitsSet"},
		{unionInPlaceWrapper, "firstBitSet", "firstBitUnset", "full"},
		{unionInPlaceWrapper, "firstBitSet", "lastBitUnset", "lastBitUnset"},
		{unionInPlaceWrapper, "firstBitSet", "innerBitsSet", "lastBitUnset"},
		{unionInPlaceWrapper, "firstBitSet", "outerBitsSet", "outerBitsSet"},
		//{unionInPlaceWrapper, "firstBitSet", "oddBitsSet", ""},
		{unionInPlaceWrapper, "firstBitSet", "evenBitsSet", "evenBitsSet"},
		//
		{unionInPlaceWrapper, "lastBitSet", "empty", "lastBitSet"},
		{unionInPlaceWrapper, "lastBitSet", "full", "full"},
		{unionInPlaceWrapper, "lastBitSet", "firstBitSet", "outerBitsSet"},
		{unionInPlaceWrapper, "lastBitSet", "lastBitSet", "lastBitSet"},
		{unionInPlaceWrapper, "lastBitSet", "firstBitUnset", "firstBitUnset"},
		{unionInPlaceWrapper, "lastBitSet", "lastBitUnset", "full"},
		{unionInPlaceWrapper, "lastBitSet", "innerBitsSet", "firstBitUnset"},
		{unionInPlaceWrapper, "lastBitSet", "outerBitsSet", "outerBitsSet"},
		{unionInPlaceWrapper, "lastBitSet", "oddBitsSet", "oddBitsSet"},
		//{unionInPlaceWrapper, "lastBitSet", "evenBitsSet", ""},
		//
		{unionInPlaceWrapper, "firstBitUnset", "empty", "firstBitUnset"},
		{unionInPlaceWrapper, "firstBitUnset", "full", "full"},
		{unionInPlaceWrapper, "firstBitUnset", "firstBitSet", "full"},
		{unionInPlaceWrapper, "firstBitUnset", "lastBitSet", "firstBitUnset"},
		{unionInPlaceWrapper, "firstBitUnset", "firstBitUnset", "firstBitUnset"},
		{unionInPlaceWrapper, "firstBitUnset", "lastBitUnset", "full"},
		{unionInPlaceWrapper, "firstBitUnset", "innerBitsSet", "firstBitUnset"},
		{unionInPlaceWrapper, "firstBitUnset", "outerBitsSet", "full"},
		{unionInPlaceWrapper, "firstBitUnset", "oddBitsSet", "firstBitUnset"},
		{unionInPlaceWrapper, "firstBitUnset", "evenBitsSet", "full"},
		//
		{unionInPlaceWrapper, "lastBitUnset", "empty", "lastBitUnset"},
		{unionInPlaceWrapper, "lastBitUnset", "full", "full"},
		{unionInPlaceWrapper, "lastBitUnset", "firstBitSet", "lastBitUnset"},
		{unionInPlaceWrapper, "lastBitUnset", "lastBitSet", "full"},
		{unionInPlaceWrapper, "lastBitUnset", "firstBitUnset", "full"},
		{unionInPlaceWrapper, "lastBitUnset", "lastBitUnset", "lastBitUnset"},
		{unionInPlaceWrapper, "lastBitUnset", "innerBitsSet", "lastBitUnset"},
		{unionInPlaceWrapper, "lastBitUnset", "outerBitsSet", "full"},
		{unionInPlaceWrapper, "lastBitUnset", "oddBitsSet", "full"},
		{unionInPlaceWrapper, "lastBitUnset", "evenBitsSet", "lastBitUnset"},
		//
		{unionInPlaceWrapper, "innerBitsSet", "empty", "innerBitsSet"},
		{unionInPlaceWrapper, "innerBitsSet", "full", "full"},
		{unionInPlaceWrapper, "innerBitsSet", "firstBitSet", "lastBitUnset"},
		{unionInPlaceWrapper, "innerBitsSet", "lastBitSet", "firstBitUnset"},
		{unionInPlaceWrapper, "innerBitsSet", "firstBitUnset", "firstBitUnset"},
		{unionInPlaceWrapper, "innerBitsSet", "lastBitUnset", "lastBitUnset"},
		{unionInPlaceWrapper, "innerBitsSet", "innerBitsSet", "innerBitsSet"},
		{unionInPlaceWrapper, "innerBitsSet", "outerBitsSet", "full"},
		{unionInPlaceWrapper, "innerBitsSet", "oddBitsSet", "firstBitUnset"},
		{unionInPlaceWrapper, "innerBitsSet", "evenBitsSet", "lastBitUnset"},
		//
		{unionInPlaceWrapper, "outerBitsSet", "empty", "outerBitsSet"},
		{unionInPlaceWrapper, "outerBitsSet", "full", "full"},
		{unionInPlaceWrapper, "outerBitsSet", "firstBitSet", "outerBitsSet"},
		{unionInPlaceWrapper, "outerBitsSet", "lastBitSet", "outerBitsSet"},
		{unionInPlaceWrapper, "outerBitsSet", "firstBitUnset", "full"},
		{unionInPlaceWrapper, "outerBitsSet", "lastBitUnset", "full"},
		{unionInPlaceWrapper, "outerBitsSet", "innerBitsSet", "full"},
		{unionInPlaceWrapper, "outerBitsSet", "outerBitsSet", "outerBitsSet"},
		//{unionInPlaceWrapper, "outerBitsSet", "oddBitsSet", ""},
		//{unionInPlaceWrapper, "outerBitsSet", "evenBitsSet", ""},
		//
		{unionInPlaceWrapper, "oddBitsSet", "empty", "oddBitsSet"},
		{unionInPlaceWrapper, "oddBitsSet", "full", "full"},
		//{unionInPlaceWrapper, "oddBitsSet", "firstBitSet", ""},
		{unionInPlaceWrapper, "oddBitsSet", "lastBitSet", "oddBitsSet"},
		{unionInPlaceWrapper, "oddBitsSet", "firstBitUnset", "firstBitUnset"},
		{unionInPlaceWrapper, "oddBitsSet", "lastBitUnset", "full"},
		{unionInPlaceWrapper, "oddBitsSet", "innerBitsSet", "firstBitUnset"},
		//{unionInPlaceWrapper, "oddBitsSet", "outerBitsSet", ""},
		{unionInPlaceWrapper, "oddBitsSet", "oddBitsSet", "oddBitsSet"},
		{unionInPlaceWrapper, "oddBitsSet", "evenBitsSet", "full"},
		//
		{unionInPlaceWrapper, "evenBitsSet", "empty", "evenBitsSet"},
		{unionInPlaceWrapper, "evenBitsSet", "full", "full"},
		{unionInPlaceWrapper, "evenBitsSet", "firstBitSet", "evenBitsSet"},
		//{unionInPlaceWrapper, "evenBitsSet", "lastBitSet", ""},
		{unionInPlaceWrapper, "evenBitsSet", "firstBitUnset", "full"},
		{unionInPlaceWrapper, "evenBitsSet", "lastBitUnset", "lastBitUnset"},
		{unionInPlaceWrapper, "evenBitsSet", "innerBitsSet", "lastBitUnset"},
		//{unionInPlaceWrapper, "evenBitsSet", "outerBitsSet", ""},
		{unionInPlaceWrapper, "evenBitsSet", "oddBitsSet", "full"},
		{unionInPlaceWrapper, "evenBitsSet", "evenBitsSet", "evenBitsSet"},

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

		// differenceInPlace
		{differenceInPlaceWrapper, "empty", "empty", "empty"},
		{differenceInPlaceWrapper, "empty", "full", "empty"},
		{differenceInPlaceWrapper, "empty", "firstBitSet", "empty"},
		{differenceInPlaceWrapper, "empty", "lastBitSet", "empty"},
		{differenceInPlaceWrapper, "empty", "firstBitUnset", "empty"},
		{differenceInPlaceWrapper, "empty", "lastBitUnset", "empty"},
		{differenceInPlaceWrapper, "empty", "innerBitsSet", "empty"},
		{differenceInPlaceWrapper, "empty", "outerBitsSet", "empty"},
		{differenceInPlaceWrapper, "empty", "oddBitsSet", "empty"},
		{differenceInPlaceWrapper, "empty", "evenBitsSet", "empty"},
		//
		{differenceInPlaceWrapper, "full", "empty", "full"},
		{differenceInPlaceWrapper, "full", "full", "empty"},
		{differenceInPlaceWrapper, "full", "firstBitSet", "firstBitUnset"},
		{differenceInPlaceWrapper, "full", "lastBitSet", "lastBitUnset"},
		{differenceInPlaceWrapper, "full", "firstBitUnset", "firstBitSet"},
		{differenceInPlaceWrapper, "full", "lastBitUnset", "lastBitSet"},
		{differenceInPlaceWrapper, "full", "innerBitsSet", "outerBitsSet"},
		{differenceInPlaceWrapper, "full", "outerBitsSet", "innerBitsSet"},
		{differenceInPlaceWrapper, "full", "oddBitsSet", "evenBitsSet"},
		{differenceInPlaceWrapper, "full", "evenBitsSet", "oddBitsSet"},
		//
		{differenceInPlaceWrapper, "firstBitSet", "empty", "firstBitSet"},
		{differenceInPlaceWrapper, "firstBitSet", "full", "empty"},
		{differenceInPlaceWrapper, "firstBitSet", "firstBitSet", "empty"},
		{differenceInPlaceWrapper, "firstBitSet", "lastBitSet", "firstBitSet"},
		{differenceInPlaceWrapper, "firstBitSet", "firstBitUnset", "firstBitSet"},
		{differenceInPlaceWrapper, "firstBitSet", "lastBitUnset", "empty"},
		{differenceInPlaceWrapper, "firstBitSet", "innerBitsSet", "firstBitSet"},
		{differenceInPlaceWrapper, "firstBitSet", "outerBitsSet", "empty"},
		{differenceInPlaceWrapper, "firstBitSet", "oddBitsSet", "firstBitSet"},
		{differenceInPlaceWrapper, "firstBitSet", "evenBitsSet", "empty"},
		//
		{differenceInPlaceWrapper, "lastBitSet", "empty", "lastBitSet"},
		{differenceInPlaceWrapper, "lastBitSet", "full", "empty"},
		{differenceInPlaceWrapper, "lastBitSet", "firstBitSet", "lastBitSet"},
		{differenceInPlaceWrapper, "lastBitSet", "lastBitSet", "empty"},
		{differenceInPlaceWrapper, "lastBitSet", "firstBitUnset", "empty"},
		{differenceInPlaceWrapper, "lastBitSet", "lastBitUnset", "lastBitSet"},
		{differenceInPlaceWrapper, "lastBitSet", "innerBitsSet", "lastBitSet"},
		{differenceInPlaceWrapper, "lastBitSet", "outerBitsSet", "empty"},
		{differenceInPlaceWrapper, "lastBitSet", "oddBitsSet", "empty"},
		{differenceInPlaceWrapper, "lastBitSet", "evenBitsSet", "lastBitSet"},
		//
		{differenceInPlaceWrapper, "firstBitUnset", "empty", "firstBitUnset"},
		{differenceInPlaceWrapper, "firstBitUnset", "full", "empty"},
		{differenceInPlaceWrapper, "firstBitUnset", "firstBitSet", "firstBitUnset"},
		{differenceInPlaceWrapper, "firstBitUnset", "lastBitSet", "innerBitsSet"},
		{differenceInPlaceWrapper, "firstBitUnset", "firstBitUnset", "empty"},
		{differenceInPlaceWrapper, "firstBitUnset", "lastBitUnset", "lastBitSet"},
		{differenceInPlaceWrapper, "firstBitUnset", "innerBitsSet", "lastBitSet"},
		{differenceInPlaceWrapper, "firstBitUnset", "outerBitsSet", "innerBitsSet"},
		//{differenceInPlaceWrapper, "firstBitUnset", "oddBitsSet", ""},
		{differenceInPlaceWrapper, "firstBitUnset", "evenBitsSet", "oddBitsSet"},
		//
		{differenceInPlaceWrapper, "lastBitUnset", "empty", "lastBitUnset"},
		{differenceInPlaceWrapper, "lastBitUnset", "full", "empty"},
		{differenceInPlaceWrapper, "lastBitUnset", "firstBitSet", "innerBitsSet"},
		{differenceInPlaceWrapper, "lastBitUnset", "lastBitSet", "lastBitUnset"},
		{differenceInPlaceWrapper, "lastBitUnset", "firstBitUnset", "firstBitSet"},
		{differenceInPlaceWrapper, "lastBitUnset", "lastBitUnset", "empty"},
		{differenceInPlaceWrapper, "lastBitUnset", "innerBitsSet", "firstBitSet"},
		{differenceInPlaceWrapper, "lastBitUnset", "outerBitsSet", "innerBitsSet"},
		{differenceInPlaceWrapper, "lastBitUnset", "oddBitsSet", "evenBitsSet"},
		//{differenceInPlaceWrapper, "lastBitUnset", "evenBitsSet", ""},
		//
		{differenceInPlaceWrapper, "innerBitsSet", "empty", "innerBitsSet"},
		{differenceInPlaceWrapper, "innerBitsSet", "full", "empty"},
		{differenceInPlaceWrapper, "innerBitsSet", "firstBitSet", "innerBitsSet"},
		{differenceInPlaceWrapper, "innerBitsSet", "lastBitSet", "innerBitsSet"},
		{differenceInPlaceWrapper, "innerBitsSet", "firstBitUnset", "empty"},
		{differenceInPlaceWrapper, "innerBitsSet", "lastBitUnset", "empty"},
		{differenceInPlaceWrapper, "innerBitsSet", "innerBitsSet", "empty"},
		{differenceInPlaceWrapper, "innerBitsSet", "outerBitsSet", "innerBitsSet"},
		//{differenceInPlaceWrapper, "innerBitsSet", "oddBitsSet", ""},
		//{differenceInPlaceWrapper, "innerBitsSet", "evenBitsSet", ""},
		//
		{differenceInPlaceWrapper, "outerBitsSet", "empty", "outerBitsSet"},
		{differenceInPlaceWrapper, "outerBitsSet", "full", "empty"},
		{differenceInPlaceWrapper, "outerBitsSet", "firstBitSet", "lastBitSet"},
		{differenceInPlaceWrapper, "outerBitsSet", "lastBitSet", "firstBitSet"},
		{differenceInPlaceWrapper, "outerBitsSet", "firstBitUnset", "firstBitSet"},
		{differenceInPlaceWrapper, "outerBitsSet", "lastBitUnset", "lastBitSet"},
		{differenceInPlaceWrapper, "outerBitsSet", "innerBitsSet", "outerBitsSet"},
		{differenceInPlaceWrapper, "outerBitsSet", "outerBitsSet", "empty"},
		{differenceInPlaceWrapper, "outerBitsSet", "oddBitsSet", "firstBitSet"},
		{differenceInPlaceWrapper, "outerBitsSet", "evenBitsSet", "lastBitSet"},
		//
		{differenceInPlaceWrapper, "oddBitsSet", "empty", "oddBitsSet"},
		{differenceInPlaceWrapper, "oddBitsSet", "full", "empty"},
		{differenceInPlaceWrapper, "oddBitsSet", "firstBitSet", "oddBitsSet"},
		//{differenceInPlaceWrapper, "oddBitsSet", "lastBitSet", ""},
		{differenceInPlaceWrapper, "oddBitsSet", "firstBitUnset", "empty"},
		{differenceInPlaceWrapper, "oddBitsSet", "lastBitUnset", "lastBitSet"},
		{differenceInPlaceWrapper, "oddBitsSet", "innerBitsSet", "lastBitSet"},
		//{differenceInPlaceWrapper, "oddBitsSet", "outerBitsSet", ""},
		{differenceInPlaceWrapper, "oddBitsSet", "oddBitsSet", "empty"},
		{differenceInPlaceWrapper, "oddBitsSet", "evenBitsSet", "oddBitsSet"},
		//
		{differenceInPlaceWrapper, "evenBitsSet", "empty", "evenBitsSet"},
		{differenceInPlaceWrapper, "evenBitsSet", "full", "empty"},
		//{differenceInPlaceWrapper, "evenBitsSet", "firstBitSet", ""},
		{differenceInPlaceWrapper, "evenBitsSet", "lastBitSet", "evenBitsSet"},
		{differenceInPlaceWrapper, "evenBitsSet", "firstBitUnset", "firstBitSet"},
		{differenceInPlaceWrapper, "evenBitsSet", "lastBitUnset", "empty"},
		{differenceInPlaceWrapper, "evenBitsSet", "innerBitsSet", "firstBitSet"},
		//{differenceInPlaceWrapper, "evenBitsSet", "outerBitsSet", ""},
		{differenceInPlaceWrapper, "evenBitsSet", "oddBitsSet", "evenBitsSet"},
		{differenceInPlaceWrapper, "evenBitsSet", "evenBitsSet", "empty"},
	}
	for _, testOp := range testOps {
		for _, x := range containerTypes {
			for _, y := range containerTypes {
				desc := fmt.Sprintf("%s(%s/%s, %s/%s)", getFunctionName(testOp.f), containerTypeNames[x], testOp.x, containerTypeNames[y], testOp.y)
				t.Run(desc, func(t *testing.T) {
					ret := runContainerFunc(testOp.f, cts[x][testOp.x], cts[y][testOp.y])
					exp := testOp.exp

					// Convert to all container types and check result.
					for _, ct := range containerTypes {
						if err := ret.BitwiseCompare(cts[ct][exp]); err != nil {
							t.Error(err)
						}
					}
				})
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

func TestUnmarshalRoaringWithNoErrors(t *testing.T) {
	testCases := []struct {
		roaringData     string
		roaringFileName string
		count           uint64
		expectedBits    string
	}{
		{ // generated serialize image from java(clojure) with arrays
			roaringData:  "3A300000020000000000020001000000180000001E0000000100020003000100",
			count:        4,
			expectedBits: "[1 2 3 65537]",
		},
		{ // generated serialize image from java(clojure) with a run and array
			roaringData:  "3B3001000100000900010000000100010009000100",
			count:        11,
			expectedBits: "[1 2 3 4 5 6 7 8 9 10 65537]",
		},
		{ // had to use an external file because emacs was barfing on the long line :()
			roaringFileName: "testdata/bitmapcontainer.roaringbitmap",
			count:           10000,
			expectedBits:    "X",
		},
	}
	var testContainer []byte
	var err error
	for _, testCase := range testCases {
		if testCase.roaringFileName == "" {
			testContainer, err = hex.DecodeString(testCase.roaringData)
			if err != nil {
				t.Fatalf("hex decode %s", err)
			}
		} else {
			testContainer, _ = ioutil.ReadFile(testCase.roaringFileName)
		}
		bm := NewBitmap()
		err = bm.UnmarshalBinary(testContainer)
		if err != nil {
			t.Fatalf("UnmarshalOfficialRoaring %s", err)
		}
		if bm.Count() != testCase.count {
			t.Fatalf("expecting %s got %d", testCase.expectedBits, bm.Count())
		}
	}
}

func TestUnmarshalRoaringWithErrors(t *testing.T) {
	//testing bitmaps with no containers
	noContainers := []struct {
		hexString     string
		expectedError string
	}{
		{ // Runs a bitmap without runs and no containers through the official roaring
			hexString:     "3A30000000000000",
			expectedError: "header: malformed bitmap, key-cardinality slice overruns buffer at 8",
		},
		{ // Runs a bitmap with runs and no containers through the official roaring
			hexString:     "3B30000000000000",
			expectedError: "header: malformed bitmap, key-cardinality slice overruns buffer at 9",
		},
		{ // Runs a bitmap in the Pilosa format through the Pilosa roaring
			hexString: "3C30000000000000",
		},
	}
	for _, loopContainers := range noContainers {
		zeroContainers, err := hex.DecodeString(loopContainers.hexString)
		if err != nil {
			t.Fatalf("hex decode %s", err)
		}
		bm := NewBitmap()
		err = bm.UnmarshalBinary(zeroContainers)
		if err != nil {
			if !strings.Contains(err.Error(), loopContainers.expectedError) {
				t.Fatalf("Expected: %s, Got: %s", loopContainers.expectedError, err)
			}
		}
	}
}

func BenchmarkUnionBitmapBitmapInPlace(b *testing.B) {
	b1 := newTestBitmapContainer()
	b2 := newTestBitmapContainer()
	for n := 0; n < b.N; n++ {
		b1 = unionBitmapBitmapInPlace(b1, b2)
	}
}

func BenchmarkBitmapRepair(b *testing.B) {
	b1 := newTestBitmapContainer()
	for n := 0; n < b.N; n++ {
		b1.bitmapRepair()
	}
}

func newTestBitmapContainer() *Container {
	return NewContainerBitmap(0, nil)
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
		a := NewContainerArray(test.array)
		ret1, _ := shift(a)      // test generic shift function
		ret2, _ := shiftArray(a) // test array-specific shift function
		// accept nil *Container as valid substitute for empty array
		if ret1 == nil {
			ret1 = NewContainerArray(nil)
		}
		if ret2 == nil {
			ret2 = NewContainerArray(nil)
		}
		if !reflect.DeepEqual(ret1.array(), test.exp) {
			t.Fatalf("test #%v shift() expected %v, but got %v", i, test.exp, ret1.array())
		} else if !reflect.DeepEqual(ret2.array(), test.exp) {
			t.Fatalf("test #%v shiftArray() expected %v, but got %v", i, test.exp, ret2.array())
		}
	}
}

func TestShiftBitmap(t *testing.T) {
	// note, bitmaps are provided for us by the ensuing tests
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
		a := NewContainerBitmap(-1, test.bitmap)
		ret1, _ := shift(a)       // test generic shift function
		ret2, _ := shiftBitmap(a) // test bitmap-specific shift function
		e := NewContainerBitmap(-1, test.exp)
		if !reflect.DeepEqual(ret1.bitmap(), e.bitmap()) {
			t.Fatalf("test #%v shift() expected %v, but got %v", i, e.bitmap(), ret1.bitmap())
		} else if !reflect.DeepEqual(ret2.bitmap(), test.exp) {
			t.Fatalf("test #%v shiftBitmap() expected %v, but got %v", i, e.bitmap(), ret2.bitmap())
		}
	}
}
func TestShiftRun(t *testing.T) {
	tests := []struct {
		runs  []Interval16
		n     int32
		en    int32
		exp   []Interval16
		carry bool
	}{
		{
			runs:  []Interval16{{Start: 5, Last: 10}},
			n:     5,
			en:    5,
			exp:   []Interval16{{Start: 6, Last: 11}},
			carry: false,
		},
		{
			runs:  []Interval16{{Start: 5, Last: 65535}},
			n:     65530,
			en:    65529,
			exp:   []Interval16{{Start: 6, Last: 65535}},
			carry: true,
		},
		{
			runs:  []Interval16{{Start: 65535, Last: 65535}},
			n:     1,
			en:    0,
			exp:   []Interval16{},
			carry: true,
		},
	}

	for i, test := range tests {
		a := NewContainerRun(test.runs)
		ret1, c1 := shift(a)    // test generic shift function
		ret2, c2 := shiftRun(a) // test run-specific shift function
		if !reflect.DeepEqual(ret1.runs(), test.exp) && c1 == test.carry && ret1.N() == test.en {
			t.Fatalf("test #%v shift() expected %v, but got %v %d", i, test.exp, ret1.runs(), ret1.N())
		} else if !reflect.DeepEqual(ret2.runs(), test.exp) && c2 == test.carry && ret2.N() == test.en {
			t.Fatalf("test #%v shiftRun() expected %v, but got %v %d", i, test.exp, ret2.runs(), ret2.N())
		}
	}
}

func TestOpLogWriteUnmarshal(t *testing.T) {
	tests := []*op{
		{
			typ:   opTypeAdd,
			value: 27,
		},
		{
			typ:   opTypeRemove,
			value: 28,
		},
		{
			typ:    opTypeAddBatch,
			values: []uint64{1, 2, 6, 19},
		},
		{
			typ:    opTypeRemoveBatch,
			values: []uint64{1, 2, 6, 19, 22, 44},
		},
		{
			typ:    opTypeAddBatch,
			values: []uint64{51234567890},
		},
		{
			typ:    opTypeRemoveBatch,
			values: []uint64{51234567890},
		},
		{
			typ:   opTypeAdd,
			value: 0,
		},
		{
			typ:   opTypeRemove,
			value: 0,
		},
		{
			typ:    opTypeAddBatch,
			values: []uint64{0},
		},
		{
			typ:    opTypeRemoveBatch,
			values: []uint64{0},
		},
		{
			typ:    opTypeAddBatch,
			values: []uint64{},
		},
		{
			typ:    opTypeRemoveBatch,
			values: []uint64{},
		},
	}

	// test each one separately
	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			buf := &bytes.Buffer{}
			if _, err := test.WriteTo(buf); err != nil {
				t.Errorf("writing op %v to buffer: %v", test, err)
			}

			op := &op{}
			if err := op.UnmarshalBinary(buf.Bytes()); err != nil {
				t.Fatalf("unmarshling op: %v", err)
			}

			if err := compareOps(test, op); err != nil {
				t.Errorf("mismatch: %v", err)
			}
		})
	}

	// now write them all to the same buffer and unmarshal one by one
	t.Run("writeAllOps", func(t *testing.T) {
		buf := &bytes.Buffer{}
		for _, test := range tests {
			_, err := test.WriteTo(buf)
			if err != nil {
				t.Fatalf("writing op to buffer: %v", err)
			}
		}

		data := buf.Bytes()
		offset := 0
		for i, test := range tests {
			op := &op{}

			if err := op.UnmarshalBinary(data[offset:]); err != nil {
				t.Fatalf("unmarshling op: %v", err)
			}
			if err := compareOps(test, op); err != nil {
				t.Errorf("mismatch at %d: %v", i, err)
			}
			offset += op.size()
		}
	})
}

func compareOps(op1, op2 *op) error {
	if op1.typ != op2.typ || op1.value != op2.value || len(op1.values) != len(op2.values) {
		return errors.Errorf("mismatched type, value, or length: %v, %v", op1, op2)
	}

	for i := 0; i < len(op1.values); i++ {
		if op1.values[i] != op2.values[i] {
			return errors.Errorf("mismatched values at %d: %d and %d", i, op1.values[i], op2.values[i])
		}
	}
	return nil
}

func TestDirectAddN(t *testing.T) {
	tests := []struct {
		call1     []uint64
		expn1     int
		call2     []uint64
		expn2     int
		exp       []uint64
		expcall2n []uint64
	}{
		{
			call1:     []uint64{0},
			expn1:     1,
			call2:     []uint64{0, 1},
			expn2:     1,
			exp:       []uint64{0, 1},
			expcall2n: []uint64{1},
		},
		{
			call1:     []uint64{0, 22, 55},
			expn1:     3,
			call2:     []uint64{0, 14, 22, 99, 55},
			expn2:     2,
			exp:       []uint64{0, 14, 22, 55, 99},
			expcall2n: []uint64{14, 99},
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			b := NewBitmap()
			n1 := b.DirectAddN(test.call1...)
			if n1 != test.expn1 {
				t.Errorf("mismatched n1 exp:%d got:%d", test.expn1, n1)
			}
			n2 := b.DirectAddN(test.call2...)
			if n2 != test.expn2 {
				t.Errorf("mismatched n2 exp:%d got:%d", test.expn2, n2)
			}
			if !reflect.DeepEqual(test.exp, b.Slice()) {
				t.Errorf("misatched results \n%v\n%v", test.exp, b.Slice())
			}
			if !reflect.DeepEqual(test.expcall2n, test.call2[:n2]) {
				t.Errorf("unexpected arg change \n%v\n%v", test.expcall2n, test.call2[:n2])
			}
		})
	}
}

func TestDirectAddNVsAdd(t *testing.T) {
	tests := [][]uint64{
		{},
		{0},
		{0, 1, 2, 3},
		{0, 1, 2, 101000, 9384932},
		{9384932, 101000, 2, 1, 0},
		{3489, 19230, 394, 0, 893982, 890283, 14, 7},
	}
	// Add some randomly created tests.
	rand := rand.New(rand.NewSource(1))
	for i := 0; i < 100; i++ {
		tests = append(tests, generator.Uint64Slice(1+rand.Intn(1000), 0, 10000000, i%2 == 0, rand))
	}
	testsCopy := make([][]uint64, len(tests))
	copy(testsCopy, tests)
	for i, test := range testsCopy {
		t.Run(fmt.Sprintf("Fresh%d", i), func(t *testing.T) {
			ba := NewBitmap()
			bd := NewBitmap()
			na, err := ba.Add(test...)
			if err != nil {
				t.Fatalf("adding bits: %v", err)
			}
			nd := bd.DirectAddN(test...)
			if na != (nd > 0) {
				t.Errorf("differing changed numbers %v, %d", na, nd)
			}
			if ba.Count() != bd.Count() {
				t.Errorf("different counts")
			}
			if !reflect.DeepEqual(ba.Slice(), bd.Slice()) {
				t.Errorf("unequal values\n%v\n%v", ba.Slice(), bd.Slice())
			}
		})
	}

	ba := NewBitmap()
	bd := NewBitmap()
	for i, test := range tests {
		t.Run(fmt.Sprintf("ContinuousAdd%d", i), func(t *testing.T) {
			na, err := ba.Add(test...)
			if err != nil {
				t.Fatalf("adding bits: %v", err)
			}
			nd := bd.DirectAddN(test...)
			if na != (nd > 0) {
				t.Errorf("differing changed numbers %v, %d", na, nd)
			}
			if ba.Count() != bd.Count() {
				t.Errorf("different counts")
			}
			if !reflect.DeepEqual(ba.Slice(), bd.Slice()) {
				t.Errorf("unequal values\n%v\n%v", ba.Slice(), bd.Slice())
			}
		})
	}

}

func BenchmarkUnionInPlaceRegression(b *testing.B) {
	initial := make([]uint64, 0, 10100)
	a1 := make([]uint64, 0, 10000)
	a2 := make([]uint64, 0, 10000)
	for i := uint64(0); i < 1<<30; i += 100000 {
		initial = append(initial, i)
		a1 = append(a1, i+67000)
		a2 = append(a2, i/2)
	}
	a1BM := NewBTreeBitmap(a1...)
	a2BM := NewBTreeBitmap(a2...)
	b.Run("Union1", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			bm := NewBTreeBitmap(initial...)
			_ = bm.Union(a1BM)
		}
	})
	b.Run("UnionInPlace1", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			bm := NewBTreeBitmap(initial...)
			bm.UnionInPlace(a1BM)
		}
	})

	b.Run("Union2", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			bm := NewBTreeBitmap(initial...)
			_ = bm.Union(a1BM, a2BM)
		}
	})
	b.Run("UnionInPlace2", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			bm := NewBTreeBitmap(initial...)
			bm.UnionInPlace(a1BM, a2BM)
		}
	})
}

func TestBitmapAny(t *testing.T) {
	bm := NewBTreeBitmap()
	if bm.Any() {
		t.Error("empty bitmap should have Any()==false")
	}
	_, err := bm.Add(1)
	if err != nil {
		t.Errorf("couldn't add a bit: %v", err)
	}
	if !bm.Any() {
		t.Error("bitmap with 1 bit should have Any()==true")
	}
	_, err = bm.Add(100000)
	if err != nil {
		t.Errorf("couldn't add a bit: %v", err)
	}
	if !bm.Any() {
		t.Error("bitmap with 2 bits should have Any()==true")
	}
	changed, err := bm.Remove(1)
	if err != nil {
		t.Errorf("couldn't remove a bit: %v", err)
	}
	if changed != true {
		t.Error("removing a set bit should have been a change")
	}
	if !bm.Any() {
		t.Error("bitmap with 1 bit left after removing 1 should have Any()==true")
	}
	_, err = bm.Add(1)
	if err != nil {
		t.Errorf("couldn't remove a bit: %v", err)
	}
	if changed != true {
		t.Error("re-addintg a previously set bit should have been a change")
	}
	bm = bm.Difference(NewBTreeBitmap(1))
	if !bm.Any() {
		t.Error("bitmap with 1 bit left after differencing 1 should have Any()==true")
	}
	_, err = bm.Remove(100000)
	if err != nil {
		t.Errorf("couldn't remove a bit: %v", err)
	}
	if bm.Any() {
		t.Error("shouldn't be any left")
	}
}

func TestDifferenceInPlace_N(t *testing.T) {
	a := doContainer(ContainerRun, runFull())
	b := doContainer(ContainerBitmap, bitmapFull())
	r := differenceInPlaceWrapper(a, b)
	if r.N() != 0 {
		t.Error("expected difference of containers to have n=0")
	}
}

func BenchmarkUnionRunRunInPlace(bm *testing.B) {
	bm.Skip("Skipping long running BenchmarkUnionRunRunInPlace")

	runs := []struct {
		name string
		fn   func() []Interval16
	}{
		{"FirstBitSet", runFirstBitSet},
		{"LastBitSet", runLastBitSet},
		{"FirstBitUnset", runFirstBitUnset},
		{"LastBitUnset", runLastBitUnset},
		{"InnerBitsSet", runInnerBitsSet},
		{"OuterBitsSet", runOuterBitsSet},
		{"OddBitsSet", runOddBitsSet},
		{"EvenBitsSet", runEvenBitsSet},
	}

	for _, ar := range runs {
		for _, br := range runs {
			bm.Run("RunToBitmapRun-"+ar.name+"_"+br.name, func(bm *testing.B) {
				for i := 0; i < bm.N; i++ {
					arun := doContainer(ContainerRun, ar.fn())
					brun := doContainer(ContainerRun, br.fn())

					abmp := arun.runToBitmap()
					_ = unionBitmapRunInPlace(abmp, brun)
				}
			})

			bm.Run("RunRun-"+ar.name+"_"+br.name, func(bm *testing.B) {
				for i := 0; i < bm.N; i++ {
					arun := doContainer(ContainerRun, ar.fn())
					brun := doContainer(ContainerRun, br.fn())

					_ = unionRunRunInPlace(arun, brun)
				}
			})
		}
	}
}

func TestUnionRunRunInPlaceBitwiseCompare(t *testing.T) {
	runs := []struct {
		name string
		fn   func() []Interval16
	}{
		{name: "FirstBitSet", fn: runFirstBitSet},
		{name: "LastBitSet", fn: runLastBitSet},
		{name: "FirstBitUnset", fn: runFirstBitUnset},
		{name: "LastBitUnset", fn: runLastBitUnset},
		{name: "InnerBitsSet", fn: runInnerBitsSet},
		{name: "OuterBitsSet", fn: runOuterBitsSet},
		{name: "OddBitsSet", fn: runOddBitsSet},
		{name: "EvenBitsSet", fn: runEvenBitsSet},
	}

	for _, a := range runs {
		for _, b := range runs {
			t.Run(a.name+"-"+b.name, func(t *testing.T) {
				arun := doContainer(ContainerRun, a.fn())
				abm := doContainer(ContainerRun, a.fn()).runToBitmap()
				brun := doContainer(ContainerRun, b.fn())

				out1 := unionBitmapRunInPlace(abm, brun)
				out2 := unionRunRunInPlace(arun, brun)
				out1.Repair()

				// out2 may no longer be a run container, so don't assume that.
				err := out1.BitwiseCompare(out2)
				if err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func TestCloneRoaringIterator(t *testing.T) {

	ca := NewContainerArray([]uint16{1, 10, 100, 1000})
	ba := NewFileBitmap()
	ba.Containers.Put(0, ca)
	ba.Containers.Put(10, ca)
	ba.Containers.Put(101, ca)
	ba.Containers.Put(10001, ca)
	var buf bytes.Buffer
	_, err := ba.WriteTo(&buf)
	if err != nil {
		t.Fatalf("error writing: %v", err)
	}

	itr, err := NewRoaringIterator(buf.Bytes())
	if err != nil {
		t.Fatalf("error NewRoaringIterator(buf.Bytes()): %v", err)
	}

	itr2 := itr.Clone()

	ikeys := itr2.ContainerKeys()
	if len(ikeys) == 0 {
		t.Fatalf("should not be empty")
	}
	if ikeys[0] != 0 {
		t.Fatalf("first ikeys should be 0")
	}
	if ikeys[len(ikeys)-1] != 10001 {
		t.Fatalf("last ikeys should be 10001")
	}

	var keys []uint64
	for itrKey, synthC := itr.NextContainer(); synthC != nil; itrKey, synthC = itr.NextContainer() {
		keys = append(keys, itrKey)
		_ = synthC
	}

	var keys2 []uint64
	for itrKey, synthC := itr2.NextContainer(); synthC != nil; itrKey, synthC = itr2.NextContainer() {
		keys2 = append(keys2, itrKey)
		_ = synthC
	}
	if !reflect.DeepEqual(keys, keys2) {
		t.Fatalf("keys != keys2. keys='%#v'; keys2='%#v'", keys, keys2)
	}
}

func TestRoaringIteratorContainerKeys(t *testing.T) {

	ca := NewContainerArray([]uint16{1, 10, 100, 1000})
	ba := NewFileBitmap()
	ba.Containers.Put(101, ca)
	ba.Containers.Put(10, ca)
	ba.Containers.Put(10001, ca)
	var buf bytes.Buffer
	_, err := ba.WriteTo(&buf)
	if err != nil {
		t.Fatalf("error writing: %v", err)
	}

	itr, err := NewRoaringIterator(buf.Bytes())
	if err != nil {
		t.Fatalf("error NewRoaringIterator(buf.Bytes()): %v", err)
	}

	ikeys := itr.ContainerKeys()
	if len(ikeys) == 0 {
		t.Fatalf("should not be empty")
	}
	if ikeys[0] != 10 {
		t.Fatalf("first ikeys should be 10")
	}
	if ikeys[len(ikeys)-1] != 10001 {
		t.Fatalf("last ikeys should be 10001")
	}

	// make and check empty bitmap

	baEmpty := NewFileBitmap()
	var bufEmpty bytes.Buffer
	_, err = baEmpty.WriteTo(&bufEmpty)
	if err != nil {
		t.Fatalf("error writing: %v", err)
	}

	itrEmpty, err := NewRoaringIterator(bufEmpty.Bytes())
	if err != nil {
		t.Fatalf("error NewRoaringIterator(bufEmpty.Bytes()): %v", err)
	}
	ikeys = itrEmpty.ContainerKeys()

	if len(ikeys) != 0 {
		t.Fatalf("should be empty")
	}
}

func TestRoaringIteratorSkip(t *testing.T) {

	ca := NewContainerArray([]uint16{1, 10, 100, 1000})
	ba := NewFileBitmap()
	ba.Containers.Put(101, ca)
	ba.Containers.Put(10, ca)
	ba.Containers.Put(10001, ca)
	var buf bytes.Buffer
	_, err := ba.WriteTo(&buf)
	if err != nil {
		t.Fatalf("error writing: %v", err)
	}

	itr, err := NewRoaringIterator(buf.Bytes())
	if err != nil {
		t.Fatalf("error NewRoaringIterator(buf.Bytes()): %v", err)
	}

	itr.Skip()
	ckey1, ct := itr.NextContainer()
	_ = ct
	if ckey1 != 101 {
		t.Fatalf("expected to skip 10 and get 101 but got: %v", ckey1)
	}

	// make and check empty bitmap

	baEmpty := NewFileBitmap()
	var bufEmpty bytes.Buffer
	_, err = baEmpty.WriteTo(&bufEmpty)
	if err != nil {
		t.Fatalf("error writing: %v", err)
	}

	itrEmpty, err := NewRoaringIterator(bufEmpty.Bytes())
	if err != nil {
		t.Fatalf("error NewRoaringIterator(bufEmpty.Bytes()): %v", err)
	}
	itrEmpty.Skip()
	// should not have panic-ed.
}

// we were seeing unionInterval16InPlace() returning too
// large an run container, which was causing problems when
// we write to the transactional backends. Verify that
// unionRunRunInPlace() converts to bitmap if its too large.
//
func TestContainer_unionRunRunInPlace_TwoBigRunArrays(t *testing.T) {

	a := NewContainerRun(nil)
	b := NewContainerRun(nil)

	for i := uint16(0); i < 8192; i++ {
		if i%3 == 0 {
			a, _ = a.runAdd(i)
		}
	}
	for i := uint16(0); i < 8192; i++ {
		if i%3 == 1 {
			b, _ = b.runAdd(i)
		}
	}

	c := unionRunRunInPlace(a, b)

	typ := ContainerType(c)
	if typ == ContainerRun {
		nr := len(c.runs())
		if nr > runMaxSize {
			panic(fmt.Sprintf("runs is over runMaxSize: %v", nr))
		}
	}
}

// we were seeing unionInterval16InPlace() returning too
// large an array container, which was causing problems when
// we write to the transactional backends. Verify that
// unionArrayArrayInPlace() converts to bitmap if its too large.
// Confirms that optimize() is done at the end of unionArrayArrayInPlace().
func TestContainer_unionArrayArrayInPlace_TwoBigArrayArrays(t *testing.T) {

	a := NewContainerArray(nil)
	b := NewContainerArray(nil)

	for i := uint16(0); i < 4096; i++ {
		a, _ = a.arrayAdd(i)
	}
	for i := uint16(4096); i < 8192; i++ {
		b, _ = b.arrayAdd(i)
	}

	c := unionArrayArrayInPlace(a, b)

	typ := ContainerType(c)
	if typ == ContainerArray {
		nr := len(c.array())
		if nr > ArrayMaxSize {
			panic(fmt.Sprintf("arrays is over arrayMaxSize: %v", nr))
		}
	}
}

// the love child of the above two tests.
func TestContainer_unionInPlace_ArrayUnionRun(t *testing.T) {

	for k := 0; k < 2; k++ {
		a := NewContainerArray(nil)
		for i := uint16(0); i < 8192; i += 2 {
			a, _ = a.arrayAdd(i)
		}

		b := NewContainerRun(nil)
		for i := uint16(8192); i < 8192*2; i++ {
			if i%3 == 1 {
				b, _ = b.runAdd(i)
			}
		}

		var c *Container
		if k == 0 {
			c = a.unionInPlace(b)
		} else {
			c = b.unionInPlace(a)
		}
		typ := ContainerType(c)
		if typ == ContainerArray {
			panic("should be impossible to have an array")
		}
	}
}

func TestContainerCallback(t *testing.T) {
	containers, err := InitContainerArchetypes()
	if err != nil {
		t.Fatalf("creating containers: %v", err)
	}
	got := make([]uint16, 65536)
	hit := func(u uint16) {
		got = append(got, u)
	}
	var expected []uint16
	// complain() wraps up some pretty-printing logic for this,
	// but note also the closure trapping expected/got so we can
	// just refer to them without passing them in.
	complain := func(t *testing.T, msg string, args ...interface{}) {
		l1 := len(expected)
		l2 := len(got)
		dotdot1 := ""
		dotdot2 := ""
		if l1 > 8 {
			expected = expected[:8]
			dotdot1 = "..."
		}
		if l2 > 8 {
			got = got[:8]
			dotdot2 = "..."
		}
		t.Fatalf("%s: expected %d%s, got %d%s", fmt.Sprintf(msg, args...), expected, dotdot1, got, dotdot2)
	}
	for t1, ci := range containers {
		t.Run(ContainerArchetypeNames[t1], func(t *testing.T) {
			for _, c1 := range ci {
				got = got[:0]
				expected = c1.Slice()
				containerCallback(c1, hit)
				if len(got) != len(expected) {
					complain(t, "wrong length (%d vs %d)", len(expected), len(got))
				}
				for i := range got {
					if got[i] != expected[i] {
						complain(t, "element %d differs: expected %d, got %d", i, expected[i], got[i])
					}
				}
			}
		})
	}
}

func TestIntersectionCallback(t *testing.T) {
	containers, err := InitContainerArchetypes()
	if err != nil {
		t.Fatalf("creating containers: %v", err)
	}
	got := make([]uint16, 65536)
	hit := func(u uint16) {
		got = append(got, u)
	}
	var expected []uint16
	// complain() wraps up some pretty-printing logic for this,
	// but note also the closure trapping expected/got so we can
	// just refer to them without passing them in.
	complain := func(t *testing.T, msg string, args ...interface{}) {
		l1 := len(expected)
		l2 := len(got)
		dotdot1 := ""
		dotdot2 := ""
		if l1 > 8 {
			expected = expected[:8]
			dotdot1 = "..."
		}
		if l2 > 8 {
			got = got[:8]
			dotdot2 = "..."
		}
		t.Fatalf("%s: expected %d%s, got %d%s", fmt.Sprintf(msg, args...), expected, dotdot1, got, dotdot2)
	}
	for t1, ci := range containers {
		for t2, cj := range containers {
			t.Run(fmt.Sprintf("%s-%s", ContainerArchetypeNames[t1], ContainerArchetypeNames[t2]), func(t *testing.T) {
				for _, c1 := range ci {
					for _, c2 := range cj {
						got = got[:0]
						expectedContainer := intersect(c1, c2)
						expected = expectedContainer.Slice()
						intersectionCallback(c1, c2, hit)
						if len(got) != len(expected) {
							complain(t, "wrong length (%d vs %d)", len(expected), len(got))
						}
						for i := range got {
							if got[i] != expected[i] {
								complain(t, "element %d differs: expected %d, got %d", i, expected[i], got[i])
							}
						}
					}
				}

			})
		}
	}
}
func TestImportBitmap(t *testing.T) {
	b := NewBitmap()
	i := GetRoaringIter(1, 3, 5)

	changed, _, err := b.ImportRoaringRawIterator(i, false, true, 16)
	if err != nil {
		t.Fatal("no error should happen changed")
	}
	if changed != 3 {
		t.Fatal("Should have changed")
	}
	i = GetRoaringIter(1, 3, 5)
	changed, _, err = b.ImportRoaringRawIterator(i, true, true, 16)
	if err != nil {
		t.Fatal("no error should happen changed")
	}
	if changed != 3 {
		t.Fatalf("Should have changed %v", changed)
	}
}
func TestVariousBitmap(t *testing.T) {
	b := NewBitmap(3)

	c, e := b.Add(8)
	if e != nil {
		t.Fatal("add:", e)
	}
	if c == false {
		t.Fatal("add: should have changed")
	}
	c, _ = b.Add(8)
	if c == true {

		t.Fatal("add: should not changed")
	}
	z, _ := b.AddN(9)
	if z != 1 {
		t.Fatal("add: should changed 1")
	}
	z, _ = b.RemoveN(9)
	if z != 1 {
		t.Fatal("add: should changed 1")
	}
	if b.Contains(100) {
		t.Fatal("should not contain 100")
	}
	if !b.Any() {
		t.Fatal("should have bits ")
	}
	if b.Size() == 0 {
		t.Fatal("should have storage")
	}
	if b.Count() == 0 {
		t.Fatal("should have bits")
	}
	if b.Max() == 0 {
		t.Fatal("should max >0")
	}
	if m, e := b.Min(); !(m == 3 && e) {
		t.Fatal("min should be 3 and containers exist", m, e)
	}
	b = nil
	x := b.Clone()
	if x != nil {

		t.Fatal("nil clone should be nil")
	}
	n := b.Freeze()
	if n != nil {

		t.Fatal("nil freeze should be nil")
	}
	r, _ := b.AddN()
	if r != 0 {

		t.Fatal("nil AddN should be 0")
	}
}
