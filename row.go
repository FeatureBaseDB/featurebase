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

import (
	"encoding/json"
	"sort"

	"github.com/pilosa/pilosa/internal"
	"github.com/pilosa/pilosa/roaring"
)

// Row is a set of integers (the associated columns), and attributes which are
// arbitrary key/value pairs storing metadata about what the row represents.
type Row struct {
	segments []RowSegment

	// String keys translated to/from segment columns.
	Keys []string

	// Attributes associated with the row.
	Attrs map[string]interface{}
}

// NewRow returns a new instance of Row.
func NewRow(columns ...uint64) *Row {
	r := &Row{}
	for _, i := range columns {
		r.SetBit(i)
	}
	return r
}

// Merge merges data from other into r.
func (r *Row) Merge(other *Row) {
	var segments []RowSegment

	itr := newMergeSegmentIterator(r.segments, other.segments)
	for s0, s1 := itr.next(); s0 != nil || s1 != nil; s0, s1 = itr.next() {
		// Use the other row's data if segment is missing.
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

	r.segments = segments
	r.InvalidateCount()
}

// IntersectionCount returns the number of intersections between r and other.
func (r *Row) IntersectionCount(other *Row) uint64 {
	var n uint64

	itr := newMergeSegmentIterator(r.segments, other.segments)
	for s0, s1 := itr.next(); s0 != nil || s1 != nil; s0, s1 = itr.next() {
		// Ignore non-overlapping segments.
		if s0 == nil || s1 == nil {
			continue
		}

		n += s0.IntersectionCount(s1)
	}
	return n
}

// Intersect returns the itersection of r and other.
func (r *Row) Intersect(other *Row) *Row {
	var segments []RowSegment

	itr := newMergeSegmentIterator(r.segments, other.segments)
	for s0, s1 := itr.next(); s0 != nil || s1 != nil; s0, s1 = itr.next() {
		// Ignore non-overlapping segments.
		if s0 == nil || s1 == nil {
			continue
		}
		segments = append(segments, *s0.Intersect(s1))
	}

	return &Row{segments: segments}
}

// Xor returns the xor of r and other.
func (r *Row) Xor(other *Row) *Row {
	var segments []RowSegment

	itr := newMergeSegmentIterator(r.segments, other.segments)
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

	return &Row{segments: segments}
}

// Union returns the bitwise union of r and other.
func (r *Row) Union(other *Row) *Row {
	var segments []RowSegment
	itr := newMergeSegmentIterator(r.segments, other.segments)
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

	return &Row{segments: segments}
}

// Difference returns the diff of r and other.
func (r *Row) Difference(other *Row) *Row {
	var segments []RowSegment

	itr := newMergeSegmentIterator(r.segments, other.segments)
	for s0, s1 := itr.next(); s0 != nil || s1 != nil; s0, s1 = itr.next() {
		if s0 == nil {
			continue
		} else if s1 == nil {
			segments = append(segments, *s0)
			continue
		}
		segments = append(segments, *s0.Difference(s1))
	}

	return &Row{segments: segments}
}

// SetBit sets the i-th column of the row.
func (r *Row) SetBit(i uint64) (changed bool) {
	return r.createSegmentIfNotExists(i / SliceWidth).SetBit(i)
}

// ClearBit clears the i-th column of the row.
func (r *Row) ClearBit(i uint64) (changed bool) {
	s := r.segment(i / SliceWidth)
	if s == nil {
		return false
	}
	return s.ClearBit(i)
}

// Segments returns a list of all segments in the row.
func (r *Row) Segments() []RowSegment {
	return r.segments
}

// segment returns a segment for a given slice.
// Returns nil if segment does not exist.
func (r *Row) segment(slice uint64) *RowSegment {
	if i := sort.Search(len(r.segments), func(i int) bool {
		return r.segments[i].slice >= slice
	}); i < len(r.segments) && r.segments[i].slice == slice {
		return &r.segments[i]
	}
	return nil
}

func (r *Row) createSegmentIfNotExists(slice uint64) *RowSegment {
	i := sort.Search(len(r.segments), func(i int) bool {
		return r.segments[i].slice >= slice
	})

	// Return exact match.
	if i < len(r.segments) && r.segments[i].slice == slice {
		return &r.segments[i]
	}

	// Insert new segment.
	r.segments = append(r.segments, RowSegment{data: *roaring.NewBitmap()})
	if i < len(r.segments) {
		copy(r.segments[i+1:], r.segments[i:])
	}
	r.segments[i] = RowSegment{
		data:     *roaring.NewBitmap(),
		slice:    slice,
		writable: true,
	}

	return &r.segments[i]
}

// InvalidateCount updates the cached count in the row.
func (r *Row) InvalidateCount() {
	for i := range r.segments {
		r.segments[i].InvalidateCount()
	}
}

// IncrementCount increments the row cached counter, note this is an optimization that assumes that the caller is aware the size increased.
func (r *Row) IncrementCount(i uint64) {
	seg := r.segment(i / SliceWidth)
	if seg != nil {
		seg.n++
	}

}

// DecrementCount decrements the row cached counter.
func (r *Row) DecrementCount(i uint64) {
	seg := r.segment(i / SliceWidth)
	if seg != nil {
		if seg.n > 0 {
			seg.n--
		}
	}
}

// Count returns the number of columns in the row.
func (r *Row) Count() uint64 {
	var n uint64
	for i := range r.segments {
		n += r.segments[i].Count()
	}
	return n
}

// MarshalJSON returns a JSON-encoded byte slice of r.
func (r *Row) MarshalJSON() ([]byte, error) {
	var o struct {
		Attrs   map[string]interface{} `json:"attrs"`
		Columns []uint64               `json:"columns"`
		Keys    []string               `json:"keys,omitempty"`
	}
	o.Columns = r.Columns()
	o.Keys = r.Keys

	o.Attrs = r.Attrs
	if o.Attrs == nil {
		o.Attrs = make(map[string]interface{})
	}

	return json.Marshal(&o)
}

// Columns returns the columns in r as a slice of ints.
func (r *Row) Columns() []uint64 {
	a := make([]uint64, 0, r.Count())
	for i := range r.segments {
		a = append(a, r.segments[i].Columns()...)
	}
	return a
}

// EncodeRow converts r into its internal representation.
func EncodeRow(r *Row) *internal.Row {
	if r == nil {
		return nil
	}

	return &internal.Row{
		Columns: r.Columns(),
		Attrs:   encodeAttrs(r.Attrs),
	}
}

// DecodeRow converts r from its internal representation.
func DecodeRow(pr *internal.Row) *Row {
	if pr == nil {
		return nil
	}

	r := NewRow()
	r.Attrs = decodeAttrs(pr.Attrs)
	for _, v := range pr.Columns {
		r.SetBit(v)
	}
	return r
}

// Union performs a union on a slice of rows.
func Union(rows []*Row) *Row {
	other := rows[0]
	for _, r := range rows[1:] {
		other = other.Union(r)
	}
	return other
}

// RowSegment holds a subset of a row.
// This could point to a mmapped roaring bitmap or an in-memory bitmap. The
// width of the segment will always match the slice width.
type RowSegment struct {
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
func (s *RowSegment) Merge(other *RowSegment) {
	s.ensureWritable()

	itr := other.data.Iterator()
	for v, eof := itr.Next(); !eof; v, eof = itr.Next() {
		s.SetBit(v)
	}
}

// IntersectionCount returns the number of intersections between s and other.
func (s *RowSegment) IntersectionCount(other *RowSegment) uint64 {
	return s.data.IntersectionCount(&other.data)
}

// Intersect returns the itersection of s and other.
func (s *RowSegment) Intersect(other *RowSegment) *RowSegment {
	data := s.data.Intersect(&other.data)

	return &RowSegment{
		data:  *data,
		slice: s.slice,
		n:     data.Count(),
	}
}

// Union returns the bitwise union of s and other.
func (s *RowSegment) Union(other *RowSegment) *RowSegment {
	data := s.data.Union(&other.data)

	return &RowSegment{
		data:  *data,
		slice: s.slice,
		n:     data.Count(),
	}
}

// Difference returns the diff of s and other.
func (s *RowSegment) Difference(other *RowSegment) *RowSegment {
	data := s.data.Difference(&other.data)

	return &RowSegment{
		data:  *data,
		slice: s.slice,
		n:     data.Count(),
	}
}

// Xor returns the xor of s and other.
func (s *RowSegment) Xor(other *RowSegment) *RowSegment {
	data := s.data.Xor(&other.data)

	return &RowSegment{
		data:  *data,
		slice: s.slice,
		n:     data.Count(),
	}
}

// SetBit sets the i-th column of the row.
func (s *RowSegment) SetBit(i uint64) (changed bool) {
	s.ensureWritable()
	changed, _ = s.data.Add(i)
	if changed {
		s.n++
	}
	return changed
}

// ClearBit clears the i-th column of the row.
func (s *RowSegment) ClearBit(i uint64) (changed bool) {
	s.ensureWritable()

	changed, _ = s.data.Remove(i)
	if changed {
		s.n--
	}
	return changed
}

// InvalidateCount updates the cached count in the row.
func (s *RowSegment) InvalidateCount() {
	s.n = s.data.Count()
}

// Columns returns a list of all columns set in the segment.
func (s *RowSegment) Columns() []uint64 {
	a := make([]uint64, 0, s.Count())
	itr := s.data.Iterator()
	for v, eof := itr.Next(); !eof; v, eof = itr.Next() {
		a = append(a, v)
	}
	return a
}

// Count returns the number of set columns in the row.
func (s *RowSegment) Count() uint64 { return s.n }

// ensureWritable clones the segment if it is pointing to non-writable data.
func (s *RowSegment) ensureWritable() {
	if s.writable {
		return
	}

	s.data = *s.data.Clone()
	s.writable = true
}

// mergeSegmentIterator produces an iterator that loops through two sets of segments.
type mergeSegmentIterator struct {
	a0, a1 []RowSegment
}

// newMergeSegmentIterator returns a new instance of mergeSegmentIterator.
func newMergeSegmentIterator(a0, a1 []RowSegment) mergeSegmentIterator {
	return mergeSegmentIterator{a0: a0, a1: a1}
}

// next returns the next set of segments.
func (itr *mergeSegmentIterator) next() (s0, s1 *RowSegment) {
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
