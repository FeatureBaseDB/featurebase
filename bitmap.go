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

package pilosa

// #cgo  CFLAGS:-mpopcnt

import (
	"encoding/json"
	"sort"

	"github.com/pilosa/pilosa/internal"
	"github.com/pilosa/pilosa/roaring"
)

// Bitmap represents a set of bits.
type Bitmap struct {
	segments []BitmapSegment

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

// Merge merges data from other into b.
func (b *Bitmap) Merge(other *Bitmap) {
	var segments []BitmapSegment

	itr := newMergeSegmentIterator(b.segments, other.segments)
	for s0, s1 := itr.next(); s0 != nil || s1 != nil; s0, s1 = itr.next() {
		// Use the other bitmap's data if segment is missing.
		if s0 == nil {
			segments = append(segments, *s1)
			continue
		} else if s1 == nil {
			segments = append(segments, *s0)
			continue
		}

		// Otherwise merge.
		s0.Merge(s1)
		segments = append(segments, *s0)
	}

	b.segments = segments
	b.InvalidateCount()
}

// IntersectionCount returns the number of intersections between b and other.
func (b *Bitmap) IntersectionCount(other *Bitmap) uint64 {
	var n uint64

	itr := newMergeSegmentIterator(b.segments, other.segments)
	for s0, s1 := itr.next(); s0 != nil || s1 != nil; s0, s1 = itr.next() {
		// Ignore non-overlapping segments.
		if s0 == nil || s1 == nil {
			continue
		}

		n += s0.IntersectionCount(s1)
	}
	return n
}

// Intersect returns the itersection of b and other.
func (b *Bitmap) Intersect(other *Bitmap) *Bitmap {
	var segments []BitmapSegment

	itr := newMergeSegmentIterator(b.segments, other.segments)
	for s0, s1 := itr.next(); s0 != nil || s1 != nil; s0, s1 = itr.next() {
		// Ignore non-overlapping segments.
		if s0 == nil || s1 == nil {
			continue
		}
		segments = append(segments, *s0.Intersect(s1))
	}

	return &Bitmap{segments: segments}
}

// Xor returns the xor of b and other.
func (b *Bitmap) Xor(other *Bitmap) *Bitmap {
	var segments []BitmapSegment

	itr := newMergeSegmentIterator(b.segments, other.segments)
	for s0, s1 := itr.next(); s0 != nil || s1 != nil; s0, s1 = itr.next() {
		if s1 == nil {
			segments = append(segments, *s0)
			continue
		} else if s0 == nil {
			segments = append(segments, *s1)
			continue
		}

		segments = append(segments, *s0.Xor(s1))
	}

	return &Bitmap{segments: segments}
}

// Union returns the bitwise union of b and other.
func (b *Bitmap) Union(other *Bitmap) *Bitmap {
	var segments []BitmapSegment
	itr := newMergeSegmentIterator(b.segments, other.segments)
	for s0, s1 := itr.next(); s0 != nil || s1 != nil; s0, s1 = itr.next() {
		if s1 == nil {
			segments = append(segments, *s0)
			continue
		} else if s0 == nil {
			segments = append(segments, *s1)
			continue
		}
		segments = append(segments, *s0.Union(s1))
	}

	return &Bitmap{segments: segments}
}

// Difference returns the diff of b and other.
func (b *Bitmap) Difference(other *Bitmap) *Bitmap {
	var segments []BitmapSegment

	itr := newMergeSegmentIterator(b.segments, other.segments)
	for s0, s1 := itr.next(); s0 != nil || s1 != nil; s0, s1 = itr.next() {
		if s0 == nil {
			continue
		} else if s1 == nil {
			segments = append(segments, *s0)
			continue
		}
		segments = append(segments, *s0.Difference(s1))
	}

	return &Bitmap{segments: segments}
}

// SetBit sets the i-th bit of the bitmap.
func (b *Bitmap) SetBit(i uint64) (changed bool) {
	return b.createSegmentIfNotExists(i / SliceWidth).SetBit(i)
}

// ClearBit clears the i-th bit of the bitmap.
func (b *Bitmap) ClearBit(i uint64) (changed bool) {
	s := b.segment(i / SliceWidth)
	if s == nil {
		return false
	}
	return s.ClearBit(i)
}

// segment returns a segment for a given slice.
// Returns nil if segment does not exist.
func (b *Bitmap) segment(slice uint64) *BitmapSegment {
	if i := sort.Search(len(b.segments), func(i int) bool {
		return b.segments[i].slice >= slice
	}); i < len(b.segments) && b.segments[i].slice == slice {
		return &b.segments[i]
	}
	return nil
}

func (b *Bitmap) createSegmentIfNotExists(slice uint64) *BitmapSegment {
	i := sort.Search(len(b.segments), func(i int) bool {
		return b.segments[i].slice >= slice
	})

	// Return exact match.
	if i < len(b.segments) && b.segments[i].slice == slice {
		return &b.segments[i]
	}

	// Insert new segment.
	b.segments = append(b.segments, BitmapSegment{})
	if i < len(b.segments) {
		copy(b.segments[i+1:], b.segments[i:])
	}
	b.segments[i] = BitmapSegment{
		slice:    slice,
		writable: true,
	}

	return &b.segments[i]
}

// InvalidateCount updates the cached count in the bitmap.
func (b *Bitmap) InvalidateCount() {
	for i := range b.segments {
		b.segments[i].InvalidateCount()
	}
}

// IncrementCount increments the bitmap cached counter, note this is an optimization that assumes that the caller is aware the size increased.
func (b *Bitmap) IncrementCount(i uint64) {
	seg := b.segment(i / SliceWidth)
	if seg != nil {
		seg.n++
	}

}

// DecrementCount decrements the bitmap cached counter.
func (b *Bitmap) DecrementCount(i uint64) {
	seg := b.segment(i / SliceWidth)
	if seg != nil {
		if seg.n > 0 {
			seg.n--
		}
	}
}

// Count returns the number of set bits in the bitmap.
func (b *Bitmap) Count() uint64 {
	var n uint64
	for i := range b.segments {
		n += b.segments[i].Count()
	}
	return n
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
	for i := range b.segments {
		a = append(a, b.segments[i].Bits()...)
	}
	return a
}

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
	b.Attrs = decodeAttrs(pb.Attrs)
	for _, v := range pb.Bits {
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

// BitmapSegment holds a subset of a bitmap.
// This could point to a mmapped roaring bitmap or an in-memory bitmap. The
// width of the segment will always match the slice width.
type BitmapSegment struct {
	// Slice this segment belongs to
	slice uint64

	// Underlying raw bitmap implementation.
	// This is an mmapped bitmap if writable is false. Otherwise
	// it is a heap allocated bitmap which can be manipulated.
	data     roaring.Bitmap
	writable bool

	// Bit count
	n uint64
}

// Merge adds chunks from other to s.
// Chunks in s are overwritten if they exist in other.
func (s *BitmapSegment) Merge(other *BitmapSegment) {
	s.ensureWritable()

	itr := other.data.Iterator()
	for v, eof := itr.Next(); !eof; v, eof = itr.Next() {
		s.SetBit(v)
	}
}

// IntersectionCount returns the number of intersections between s and other.
func (s *BitmapSegment) IntersectionCount(other *BitmapSegment) uint64 {
	return s.data.IntersectionCount(&other.data)
}

// Intersect returns the itersection of s and other.
func (s *BitmapSegment) Intersect(other *BitmapSegment) *BitmapSegment {
	data := s.data.Intersect(&other.data)

	return &BitmapSegment{
		data:  *data,
		slice: s.slice,
		n:     data.Count(),
	}
}

// Union returns the bitwise union of s and other.
func (s *BitmapSegment) Union(other *BitmapSegment) *BitmapSegment {
	data := s.data.Union(&other.data)

	return &BitmapSegment{
		data:  *data,
		slice: s.slice,
		n:     data.Count(),
	}
}

// Difference returns the diff of s and other.
func (s *BitmapSegment) Difference(other *BitmapSegment) *BitmapSegment {
	data := s.data.Difference(&other.data)

	return &BitmapSegment{
		data:  *data,
		slice: s.slice,
		n:     data.Count(),
	}
}

// Xor returns the xor of s and other.
func (s *BitmapSegment) Xor(other *BitmapSegment) *BitmapSegment {
	data := s.data.Xor(&other.data)

	return &BitmapSegment{
		data:  *data,
		slice: s.slice,
		n:     data.Count(),
	}
}

// SetBit sets the i-th bit of the bitmap.
func (s *BitmapSegment) SetBit(i uint64) (changed bool) {
	s.ensureWritable()
	changed, _ = s.data.Add(i)
	if changed {
		s.n++
	}
	return changed
}

// ClearBit clears the i-th bit of the bitmap.
func (s *BitmapSegment) ClearBit(i uint64) (changed bool) {
	s.ensureWritable()

	changed, _ = s.data.Remove(i)
	if changed {
		s.n--
	}
	return changed
}

// InvalidateCount updates the cached count in the bitmap.
func (s *BitmapSegment) InvalidateCount() {
	s.n = s.data.Count()
}

// Bits returns a list of all bits set in the segment.
func (s *BitmapSegment) Bits() []uint64 {
	a := make([]uint64, 0, s.Count())
	itr := s.data.Iterator()
	for v, eof := itr.Next(); !eof; v, eof = itr.Next() {
		a = append(a, v)
	}
	return a
}

// Count returns the number of set bits in the bitmap.
func (s *BitmapSegment) Count() uint64 { return s.n }

// ensureWritable clones the segment if it is pointing to non-writable data.
func (s *BitmapSegment) ensureWritable() {
	if s.writable {
		return
	}

	s.data = *s.data.Clone()
	s.writable = true
}

// mergeSegmentIterator produces an iterator that loops through two sets of segments.
type mergeSegmentIterator struct {
	a0, a1 []BitmapSegment
}

// newMergeSegmentIterator returns a new instance of mergeSegmentIterator.
func newMergeSegmentIterator(a0, a1 []BitmapSegment) mergeSegmentIterator {
	return mergeSegmentIterator{a0: a0, a1: a1}
}

// next returns the next set of segments.
func (itr *mergeSegmentIterator) next() (s0, s1 *BitmapSegment) {
	// Find current segments.
	if len(itr.a0) > 0 {
		s0 = &itr.a0[0]
	}
	if len(itr.a1) > 0 {
		s1 = &itr.a1[0]
	}

	// Return if either or both are nil.
	if s0 == nil && s1 == nil {
		return
	} else if s0 == nil {
		itr.a1 = itr.a1[1:]
		return
	} else if s1 == nil {
		itr.a0 = itr.a0[1:]
		return
	}

	// Otherwise determine which is first.
	if s0.slice < s1.slice {
		itr.a0 = itr.a0[1:]
		return s0, nil
	} else if s0.slice > s1.slice {
		itr.a1 = itr.a1[1:]
		return s1, nil
	}

	// Return both if slices are equal.
	itr.a0, itr.a1 = itr.a0[1:], itr.a1[1:]
	return s0, s1
}
