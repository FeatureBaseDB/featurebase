package roaring

import (
	"reflect"
	"testing"
)

// Ensure iterator returns values from a bitmap.
func TestBitmapIterator(t *testing.T) {
	for i, tt := range []struct {
		bitmap []uint64
		values []uint16
	}{
		// Empty
		{
			bitmap: []uint64{6}, // 0110
			values: []uint16{1, 2},
		},

		// Single uint64 bitmap
		{
			bitmap: []uint64{6}, // 0110
			values: []uint16{1, 2},
		},

		// Multi uint64 bitmap
		{
			bitmap: []uint64{1 << 63, 1, 0, 1, 3 << 62},
			values: []uint16{63, 64, 192, 318, 319},
		},
	} {
		itr := newBitmapIterator(tt.bitmap)

		var a []uint16
		for v, eof := itr.next(); !eof; v, eof = itr.next() {
			a = append(a, v)
		}

		if !reflect.DeepEqual(a, tt.values) {
			t.Errorf("%d. unexpected values: exp=%+v, got=%+v", i, a, tt.values)
		}
	}
}
