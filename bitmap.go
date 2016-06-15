package pilosa

// #cgo  CFLAGS:-mpopcnt

import (
	"encoding/json"

	"github.com/umbel/pilosa/internal"
	"github.com/umbel/pilosa/roaring"
)

// Bitmap represents a set of bits.
type Bitmap struct {
	data roaring.Bitmap
	n    uint64

	// Attributes associated with the bitmap.
	Attrs map[string]interface{}
}

// NewBitmap returns a new instance of Bitmap.
func NewBitmap(bits ...uint64) *Bitmap {
	bm := &Bitmap{}
	for _, i := range bits {
		bm.SetBit(i)
	}
	return bm
}

// Merge adds chunks from other to b.
// Chunks in b are overwritten if they exist in other.
func (b *Bitmap) Merge(other *Bitmap) {
	itr := other.data.Iterator()
	for v, eof := itr.Next(); !eof; v, eof = itr.Next() {
		b.SetBit(v)
	}
}

// IntersectionCount returns the number of intersections between b and other.
func (b *Bitmap) IntersectionCount(other *Bitmap) uint64 {
	return b.data.IntersectionCount(&other.data)
}

// Intersect returns the itersection of b and other.
func (b *Bitmap) Intersect(other *Bitmap) *Bitmap {
	data := b.data.Intersect(&other.data)

	return &Bitmap{
		data: *data,
		n:    data.Count(),
	}
}

// Union returns the bitwise union of b and other.
func (b *Bitmap) Union(other *Bitmap) *Bitmap {
	// OPTIMIZE: Implement roaring.Bitmap.Union()

	itr0 := roaring.NewBufIterator(b.data.Iterator())
	itr1 := roaring.NewBufIterator(other.data.Iterator())

	output := NewBitmap()
	for {
		v0, eof0 := itr0.Next()
		v1, eof1 := itr1.Next()

		if eof0 && eof1 {
			break
		} else if eof0 {
			output.SetBit(v1)
		} else if eof1 {
			output.SetBit(v0)
		} else if v0 < v1 {
			output.SetBit(v0)
			itr1.Unread()
		} else if v0 > v1 {
			output.SetBit(v1)
			itr0.Unread()
		} else {
			output.SetBit(v0)
		}
	}
	return output
}

// Difference returns the diff of b and other.
func (b *Bitmap) Difference(other *Bitmap) *Bitmap {
	// OPTIMIZE: Implement roaring.Bitmap.Difference()

	itr0 := roaring.NewBufIterator(b.data.Iterator())
	itr1 := roaring.NewBufIterator(other.data.Iterator())

	output := NewBitmap()
	for {
		v0, eof0 := itr0.Next()
		v1, eof1 := itr1.Next()

		if eof0 {
			break
		} else if eof1 {
			output.SetBit(v0)
		} else if v0 < v1 {
			output.SetBit(v0)
			itr1.Unread()
		} else if v0 > v1 {
			itr0.Unread()
		}
	}
	return output
}

// MarshalJSON returns a JSON-encoded byte slice of b.
func (b *Bitmap) MarshalJSON() ([]byte, error) {
	var o struct {
		Attrs map[string]interface{} `json:"attrs"`
		Bits  []uint64               `json:"bits"`
	}
	o.Bits = b.Bits()

	o.Attrs = b.Attrs
	if o.Attrs == nil {
		o.Attrs = make(map[string]interface{})
	}

	return json.Marshal(&o)
}

// Bits returns the bits in b as a slice of ints.
func (b *Bitmap) Bits() []uint64 {
	a := make([]uint64, 0, b.Count())
	itr := b.data.Iterator()
	for v, eof := itr.Next(); !eof; v, eof = itr.Next() {
		a = append(a, v)
	}
	return a
}

// SetBit sets the i-th bit of the bitmap.
func (b *Bitmap) SetBit(i uint64) (changed bool) {
	changed, _ = b.data.Add(i)
	if changed {
		b.n++
	}
	return changed
}

// ClearBit clears the i-th bit of the bitmap.
func (b *Bitmap) ClearBit(i uint64) (changed bool) {
	changed, _ = b.data.Remove(i)
	if changed {
		b.n--
	}
	return changed
}

// InvalidateCount updates the cached count in the bitmap.
func (b *Bitmap) InvalidateCount() {
	itr, n := b.data.Iterator(), uint64(0)
	for _, eof := itr.Next(); !eof; _, eof = itr.Next() {
		n++
	}
	b.n = n
}

// Count returns the number of set bits in the bitmap.
func (b *Bitmap) Count() uint64 { return b.n }

// encodeBitmap converts b into its internal representation.
func encodeBitmap(b *Bitmap) *internal.Bitmap {
	if b == nil {
		return nil
	}

	return &internal.Bitmap{
		Bits:  b.Bits(),
		Attrs: encodeAttrs(b.Attrs),
	}
}

// decodeBitmap converts b from its internal representation.
func decodeBitmap(pb *internal.Bitmap) *Bitmap {
	if pb == nil {
		return nil
	}

	b := NewBitmap()
	b.Attrs = decodeAttrs(pb.GetAttrs())
	for _, v := range pb.GetBits() {
		b.SetBit(v)
	}
	return b
}

// Union performs a union on a slice of bitmaps.
func Union(bitmaps []*Bitmap) *Bitmap {
	other := bitmaps[0]
	for _, bm := range bitmaps[1:] {
		other = other.Union(bm)
	}
	return other
}
