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
	"reflect"
	"testing"
)

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
	c := container{runs: make([]interval16, 0)}
	tests := []struct {
		op  uint32
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
	c := container{runs: make([]interval16, 0)}
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
	c := container{runs: make([]interval16, 0)}
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
	c := container{runs: make([]interval16, 0)}
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
	c := container{}
	tests := []struct {
		start  uint32
		end    uint32
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

	c = container{bitmap: []uint64{0xF0000001, 0xFF000000, 0xFF00000000000000}}
	cnt = c.bitmapCountRange(62, 129)
	if cnt != 10 {
		t.Fatalf("count of %v from 62 to 129 should be 10, but got %v", c.bitmap, cnt)
	}
}

func TestRunRemove(t *testing.T) {
	c := container{runs: []interval16{{start: 2, last: 10}, {start: 12, last: 13}, {start: 15, last: 16}}}
	tests := []struct {
		op     uint32
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

	for _, test := range tests {
		c.mapped = true
		ret := c.remove(test.op)
		if ret != test.expRet || !reflect.DeepEqual(c.runs, test.exp) {
			t.Fatalf("Unexpected result removing %v from runs. Expected %v, got %v. Expected %v, got %v", test.op, test.expRet, ret, test.exp, c.runs)
		}
		if ret && c.mapped {
			t.Fatalf("container was not unmapped although bit %v was removed", test.op)
		}
		if !ret && !c.mapped {
			t.Fatalf("container was unmapped although bit %v was not removed", test.op)
		}
	}
}
