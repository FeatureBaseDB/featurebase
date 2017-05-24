package roaring

import "testing"

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
}

func TestIntersectionCountArrayBitmap2(t *testing.T) {
	a, b := &container{}, &container{}
	tests := []struct {
		array  []uint32
		bitmap []uint64
		exp    uint64
	}{
		{
			array:  []uint32{0},
			bitmap: []uint64{1},
			exp:    1,
		},
		{
			array:  []uint32{0, 1},
			bitmap: []uint64{3},
			exp:    2,
		},
		{
			array:  []uint32{64, 128, 129, 2000},
			bitmap: []uint64{932421, 2},
			exp:    0,
		},
		{
			array:  []uint32{0, 65, 130, 195},
			bitmap: []uint64{255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255},
			exp:    4,
		},
		{
			array:  []uint32{63, 120, 543, 639, 12000},
			bitmap: []uint64{0x8000000000000000, 0, 0, 0, 0, 0, 0, 0, 0, 0x8000000000000000},
			exp:    2,
		},
	}

	for i, test := range tests {
		a.array = test.array
		b.bitmap = test.bitmap
		ret1 := intersectionCountArrayBitmapOld(a, b)
		ret2 := intersectionCountArrayBitmap(a, b)
		if ret1 != ret2 || ret2 != test.exp {
			t.Fatalf("test #%v intersectCountArrayBitmap fail orig: %v new: %v exp: %v", i, ret1, ret2, test.exp)
		}
	}
}
