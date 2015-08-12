package index

import (
	"testing"
)

func TestRBBitmap_SetBit(t *testing.T) {
	bm := CreateRBBitmap()
	SetBit(bm, 0)
	ClearBit(bm, 0)
	if BitCount(bm) != 0 {
		t.Error("Should be 0")
	}
}
