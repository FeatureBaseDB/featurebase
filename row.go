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

	"github.com/pilosa/pilosa/roaring"
)

// Row is a set of integers (the associated columns), and attributes which are
// arbitrary key/value pairs storing metadata about what the row represents.
type Row struct {
	segments []rowSegment

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
	var segments []rowSegment

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
	r.invalidateCount()
}

// intersectionCount returns the number of intersections between r and other.
func (r *Row) intersectionCount(other *Row) uint64 {
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

// xorCount returns the number of bits in the xor of r and other.
func (r *Row) xorCount(other *Row) uint64 {
	var n uint64

	itr := newMergeSegmentIterator(r.segments, other.segments)
	for s0, s1 := itr.next(); s0 != nil || s1 != nil; s0, s1 = itr.next() {
		// Ignore non-overlapping segments.
		if s0 == nil || s1 == nil {
			continue
		}

		n += s0.XorCount(s1)
	}
	return n
}

// Intersect returns the itersection of r and other.
func (r *Row) Intersect(other *Row) *Row {
	var segments []rowSegment

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
	var segments []rowSegment

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
	var segments []rowSegment
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
	var segments []rowSegment

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
	return r.createSegmentIfNotExists(i / ShardWidth).SetBit(i)
}

// clearBit clears the i-th column of the row.
func (r *Row) clearBit(i uint64) (changed bool) { // nolint: unparam
	s := r.segment(i / ShardWidth)
	if s == nil {
		return false
	}
	return s.ClearBit(i)
}

// Segments returns a list of all segments in the row.
func (r *Row) Segments() []rowSegment {
	return r.segments
}

// segment returns a segment for a given shard.
// Returns nil if segment does not exist.
func (r *Row) segment(shard uint64) *rowSegment {
	if i := sort.Search(len(r.segments), func(i int) bool {
		return r.segments[i].shard >= shard
	}); i < len(r.segments) && r.segments[i].shard == shard {
		return &r.segments[i]
	}
	return nil
}

func (r *Row) createSegmentIfNotExists(shard uint64) *rowSegment {
	i := sort.Search(len(r.segments), func(i int) bool {
		return r.segments[i].shard >= shard
	})

	// Return exact match.
	if i < len(r.segments) && r.segments[i].shard == shard {
		return &r.segments[i]
	}

	// Insert new segment.
	r.segments = append(r.segments, rowSegment{data: *roaring.NewBitmap()})
	if i < len(r.segments) {
		copy(r.segments[i+1:], r.segments[i:])
	}
	r.segments[i] = rowSegment{
		data:     *roaring.NewBitmap(),
		shard:    shard,
		writable: true,
	}

	return &r.segments[i]
}

// invalidateCount updates the cached count in the row.
func (r *Row) invalidateCount() {
	for i := range r.segments {
		r.segments[i].InvalidateCount()
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

// rowSegment holds a subset of a row.
// This could point to a mmapped roaring bitmap or an in-memory bitmap. The
// width of the segment will always match the shard width.
type rowSegment struct {
	// Shard this segment belongs to
	shard uint64

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
func (s *rowSegment) Merge(other *rowSegment) {
	s.ensureWritable()

	itr := other.data.Iterator()
	for v, eof := itr.Next(); !eof; v, eof = itr.Next() {
		s.SetBit(v)
	}
}

// IntersectionCount returns the number of intersections between s and other.
func (s *rowSegment) IntersectionCount(other *rowSegment) uint64 {
	return s.data.IntersectionCount(&other.data)
}

// XorCount returns the number of bits in the xor between s and other.
func (s *rowSegment) XorCount(other *rowSegment) uint64 {
	return s.data.XorCount(&other.data)
}

// Intersect returns the itersection of s and other.
func (s *rowSegment) Intersect(other *rowSegment) *rowSegment {
	data := s.data.Intersect(&other.data)

	return &rowSegment{
		data:  *data,
		shard: s.shard,
		n:     data.Count(),
	}
}

// Union returns the bitwise union of s and other.
func (s *rowSegment) Union(other *rowSegment) *rowSegment {
	data := s.data.Union(&other.data)

	return &rowSegment{
		data:  *data,
		shard: s.shard,
		n:     data.Count(),
	}
}

// Difference returns the diff of s and other.
func (s *rowSegment) Difference(other *rowSegment) *rowSegment {
	data := s.data.Difference(&other.data)

	return &rowSegment{
		data:  *data,
		shard: s.shard,
		n:     data.Count(),
	}
}

// Xor returns the xor of s and other.
func (s *rowSegment) Xor(other *rowSegment) *rowSegment {
	data := s.data.Xor(&other.data)

	return &rowSegment{
		data:  *data,
		shard: s.shard,
		n:     data.Count(),
	}
}

// SetBit sets the i-th column of the row.
func (s *rowSegment) SetBit(i uint64) (changed bool) {
	s.ensureWritable()
	changed, _ = s.data.Add(i)
	if changed {
		s.n++
	}
	return changed
}

// ClearBit clears the i-th column of the row.
func (s *rowSegment) ClearBit(i uint64) (changed bool) {
	s.ensureWritable()

	changed, _ = s.data.Remove(i)
	if changed {
		s.n--
	}
	return changed
}

// InvalidateCount updates the cached count in the row.
func (s *rowSegment) InvalidateCount() {
	s.n = s.data.Count()
}

// Columns returns a list of all columns set in the segment.
func (s *rowSegment) Columns() []uint64 {
	a := make([]uint64, 0, s.Count())
	itr := s.data.Iterator()
	for v, eof := itr.Next(); !eof; v, eof = itr.Next() {
		a = append(a, v)
	}
	return a
}

// Count returns the number of set columns in the row.
func (s *rowSegment) Count() uint64 { return s.n }

// ensureWritable clones the segment if it is pointing to non-writable data.
func (s *rowSegment) ensureWritable() {
	if s.writable {
		return
	}

	s.data = *s.data.Clone()
	s.writable = true
}

// mergeSegmentIterator produces an iterator that loops through two sets of segments.
type mergeSegmentIterator struct {
	a0, a1 []rowSegment
}

// newMergeSegmentIterator returns a new instance of mergeSegmentIterator.
func newMergeSegmentIterator(a0, a1 []rowSegment) mergeSegmentIterator {
	return mergeSegmentIterator{a0: a0, a1: a1}
}

// next returns the next set of segments.
func (itr *mergeSegmentIterator) next() (s0, s1 *rowSegment) {
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
	if s0.shard < s1.shard {
		itr.a0 = itr.a0[1:]
		return s0, nil
	} else if s0.shard > s1.shard {
		itr.a1 = itr.a1[1:]
		return s1, nil
	}

	// Return both if shards are equal.
	itr.a0, itr.a1 = itr.a0[1:], itr.a1[1:]
	return s0, s1
}
