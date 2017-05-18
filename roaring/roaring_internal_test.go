package roaring

import (
	"testing"
)

func TestBitmapCountRange(t *testing.T) {
	c := container{bitmap: []uint64{1}}
	cnt := c.bitmapCountRange(63, 65)
	if cnt != 1 {
		t.Fatalf("count of %v from 63 to 65 should be 1, but got %v", c.bitmap, cnt)
	}

	c = container{bitmap: []uint64{0, 0x8000000000000000}}
	cnt = c.bitmapCountRange(65, 66)
	if cnt != 0 {
		t.Fatalf("count of %v from 65 to 66 should be 0, but got %v", c.bitmap, cnt)
	}

	c = container{bitmap: []uint64{0, 0xF000000000000000}}
	cnt = c.bitmapCountRange(65, 66)
	if cnt != 1 {
		t.Fatalf("count of %v from 65 to 66 should be 1, but got %v", c.bitmap, cnt)
	}

	c = container{bitmap: []uint64{0x1, 0xFF00000000000000}}
	cnt = c.bitmapCountRange(62, 66)
	if cnt != 3 {
		t.Fatalf("count of %v from 62 to 66 should be 3, but got %v", c.bitmap, cnt)
	}
}
