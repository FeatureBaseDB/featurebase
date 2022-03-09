// Copyright 2021 Molecula Corp. All rights reserved.
package pilosa

import (
	"encoding/json"
	"sort"

	pb "github.com/molecula/featurebase/v3/proto"
	"github.com/molecula/featurebase/v3/roaring"
	"github.com/pkg/errors"
)

// Row is a set of integers (the associated columns).
type Row struct {
	Segments []RowSegment

	// String keys translated to/from segment columns.
	Keys []string

	// Index tells what index this row is from - needed for key translation.
	Index string

	// Field tells what field this row is from if it's a "vertical"
	// row. It may be the result of a Distinct query or Rows
	// query. Knowing the index and field, we can figure out how to
	// interpret the row data.
	Field string

	// NoSplit indicates that this row may not be split.
	// This is used for `Rows` calls in a GroupBy.
	NoSplit bool
}

// NewRow returns a new instance of Row.
func NewRow(columns ...uint64) *Row {
	r := &Row{}
	for _, i := range columns {
		r.SetBit(i)
	}
	return r
}

func (r *Row) Clone() (clone *Row) {
	if r == nil {
		return nil
	}
	var keyClone []string
	if len(r.Keys) > 0 {
		keyClone = make([]string, len(r.Keys))
		copy(keyClone, r.Keys)
	}

	clone = &Row{
		Keys:  keyClone,
		Index: r.Index,
		Field: r.Field,
	}

	for _, seg := range r.Segments {
		segClone := RowSegment{
			shard:    seg.shard,
			writable: true, // we know it is safe; it is a copy.
			n:        seg.n,
		}
		if seg.data != nil {
			segClone.data = seg.data.Clone() // *roaring.Bitmap
		}
		//segClone.InvalidateCount() // not needed?
		clone.Segments = append(clone.Segments, segClone)
	}
	return clone
}

// NewRowFromBitmap divides a bitmap into rows, which it now calls shards. This
// transposes; data that was in any shard for Row 0 is now considered shard 0,
// etcetera.
func NewRowFromBitmap(b *roaring.Bitmap) *Row {
	r := &Row{}
	if b == nil {
		return r
	}
	rowNum := uint64(0)
	for col, ok := b.MinAt(rowNum * ShardWidth); ok; col, ok = b.MinAt(rowNum * ShardWidth) {
		rowNum = col / ShardWidth
		seg := RowSegment{
			shard:    rowNum,
			data:     b.OffsetRange(rowNum*ShardWidth, rowNum*ShardWidth, (rowNum+1)*ShardWidth),
			writable: true,
		}
		seg.n = seg.data.Count()
		r.Segments = append(r.Segments, seg)
		rowNum++
	}
	return r
}

// NewRowFromRoaring parses a roaring data file as a row, dividing it into
// bitmaps and rowSegments based on shard width.
func NewRowFromRoaring(data []byte) *Row {
	bitmaps, shards := roaring.RoaringToBitmaps(data, ShardWidth)
	r := &Row{Segments: make([]RowSegment, len(bitmaps))}
	for i := range bitmaps {
		segment := RowSegment{
			shard:    shards[i],
			data:     bitmaps[i],
			writable: false,
			n:        bitmaps[i].Count(),
		}
		r.Segments[i] = segment
	}
	return r
}

// ToTable implements the ToTabler interface.
func (r *Row) ToTable() (*pb.TableResponse, error) {
	var n int
	if len(r.Keys) > 0 {
		n = len(r.Keys)
	} else {
		n = len(r.Columns())
	}
	return pb.RowsToTable(r, n)
}

// Hash calculate checksum code be useful in block hash join
func (r *Row) Hash() uint64 {
	hash := uint64(0)
	for i := range r.Segments {
		hash = r.Segments[i].data.Hash(hash)
	}
	return hash
}

// ToRows implements the ToRowser interface.
func (r *Row) ToRows(callback func(*pb.RowResponse) error) error {
	if len(r.Keys) > 0 {
		// Column keys
		ci := []*pb.ColumnInfo{
			{Name: "_id", Datatype: "string"},
		}
		for _, x := range r.Keys {
			if err := callback(&pb.RowResponse{
				Headers: ci,
				Columns: []*pb.ColumnResponse{
					{ColumnVal: &pb.ColumnResponse_StringVal{StringVal: x}},
				}}); err != nil {
				return errors.Wrap(err, "calling callback")
			}
			ci = nil //only send on the first
		}
	} else {
		// Column IDs
		ci := []*pb.ColumnInfo{
			{Name: "_id", Datatype: "uint64"},
		}
		for _, x := range r.Columns() {
			if err := callback(&pb.RowResponse{
				Headers: ci,
				Columns: []*pb.ColumnResponse{
					{ColumnVal: &pb.ColumnResponse_Uint64Val{Uint64Val: x}},
				}}); err != nil {
				return errors.Wrap(err, "calling callback")
			}
			ci = nil //only send on the first
		}
	}
	return nil
}

// Roaring returns the row treated as a unified roaring bitmap.
func (r *Row) Roaring() []byte {
	bitmaps := make([]*roaring.Bitmap, len(r.Segments))
	for i := range r.Segments {
		bitmaps[i] = r.Segments[i].data
	}
	return roaring.BitmapsToRoaring(bitmaps)
}

// IsEmpty returns true if the row doesn't contain any set bits.
func (r *Row) IsEmpty() bool {
	if len(r.Segments) == 0 {
		return true
	}
	for i := range r.Segments {
		if r.Segments[i].n > 0 {
			return false
		}

	}
	return true
}

func (r *Row) Freeze() {
	for _, s := range r.Segments {
		s.Freeze()
	}
}

// Merge merges data from other into r.
func (r *Row) Merge(other *Row) {
	var segments []RowSegment

	itr := newMergeSegmentIterator(r.Segments, other.Segments)
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

	r.Segments = segments
	r.invalidateCount()
}

// intersectionCount returns the number of intersections between r and other.
func (r *Row) intersectionCount(other *Row) uint64 {
	var n uint64

	itr := newMergeSegmentIterator(r.Segments, other.Segments)
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

	itr := newMergeSegmentIterator(r.Segments, other.Segments)
	for s0, s1 := itr.next(); s0 != nil || s1 != nil; s0, s1 = itr.next() {
		// Ignore non-overlapping segments.
		if s0 == nil || s1 == nil {
			continue
		}
		segments = append(segments, *s0.Intersect(s1))
	}

	return &Row{Segments: segments}
}

// Any returns true if row contains any bits.
func (r *Row) Any() bool {
	for _, s := range r.Segments {
		if s.data.Any() {
			return true
		}
	}
	return false
}

// Xor returns the xor of r and other.
func (r *Row) Xor(other *Row) *Row {
	var segments []RowSegment

	itr := newMergeSegmentIterator(r.Segments, other.Segments)
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

	return &Row{Segments: segments}
}

// Union returns the bitwise union of r and other.
func (r *Row) Union(others ...*Row) *Row {
	segments := make([][]RowSegment, 0, len(others)+1)
	if len(r.Segments) > 0 {
		segments = append(segments, r.Segments)
	}
	nextSegs := make([][]RowSegment, 0, len(others)+1)
	toProcess := make([]*RowSegment, 0, len(others)+1)
	var output []RowSegment
	for _, other := range others {
		if len(other.Segments) > 0 {
			segments = append(segments, other.Segments)
		}
	}
	for len(segments) > 0 {
		shard := segments[0][0].shard
		for _, segs := range segments {
			if segs[0].shard < shard {
				shard = segs[0].shard
			}
		}
		nextSegs = nextSegs[:0]
		toProcess = toProcess[:0]
		for _, segs := range segments {
			if segs[0].shard == shard {
				toProcess = append(toProcess, &segs[0])
				segs = segs[1:]
			}
			if len(segs) > 0 {
				nextSegs = append(nextSegs, segs)
			}
		}
		// at this point, "toProcess" is a list of all the segments
		// sharing the lowest ID, and nextSegs is a list of all the others.
		// Swap the segment lists (so we don't have to reallocate it)
		segments, nextSegs = nextSegs, segments
		if len(toProcess) == 1 {
			output = append(output, *toProcess[0])
		} else {
			output = append(output, *toProcess[0].Union(toProcess[1:]...))
		}
	}
	return &Row{Index: r.Index, Field: r.Field, Segments: output}
}

// Difference returns the diff of r and other.
func (r *Row) Difference(others ...*Row) *Row {
	var output []RowSegment
	o := make(map[uint64][]*RowSegment)

	for x := range others {
		for y := range others[x].Segments {
			segment := others[x].Segments[y]
			o[segment.shard] = append(o[segment.shard], &segment)
		}
	}
	for _, segment := range r.Segments {

		dest, ok := o[segment.shard]
		if ok {
			output = append(output, *segment.Difference(dest...))
		} else {
			output = append(output, segment)
		}
	}
	return &Row{Segments: output}
}

// Shift returns the bitwise shift of r by n bits.
// Currently only positive shift values are supported.
//
// NOTE: the Shift method is currently unsupported, and
// is considerred to be incorrect. Please DO NOT use it.
// We are leaving it here in case someone internally wants
// to use it with the understanding that the results may
// be incorrect.
//
// Why unsupported? For a full description, see:
// https://github.com/molecula/pilosa/issues/403.
// In short, the current implementation will shift a bit
// at the edge of a shard out of the shard and into a
// container which is assumed to be an invalid container
// for the shard. So for example, shifting the last bit
// of shard 0 (containers 0-15) will shift that bit out
// to container 16. While this "sort of" works, it
// breaks an assumption about containers, and might stop
// working in the future if that assumption is enforced.
func (r *Row) Shift(n int64) (*Row, error) {
	if n < 0 {
		return nil, errors.New("cannot shift by negative values")
	} else if n == 0 {
		return r, nil
	}

	work := r
	var segments []RowSegment
	for i := int64(0); i < n; i++ {
		segments = segments[:0]
		for _, segment := range work.Segments {
			shifted, err := segment.Shift()
			if err != nil {
				return nil, errors.Wrap(err, "shifting row segment")
			}
			segments = append(segments, *shifted)
		}
		work = &Row{Segments: segments}
	}

	return work, nil
}

// SetBit sets the i-th column of the row.
func (r *Row) SetBit(i uint64) (changed bool) {
	return r.createSegmentIfNotExists(i / ShardWidth).SetBit(i)
}

// segment returns a segment for a given shard.
// Returns nil if segment does not exist.
func (r *Row) segment(shard uint64) *RowSegment {
	if i := sort.Search(len(r.Segments), func(i int) bool {
		return r.Segments[i].shard >= shard
	}); i < len(r.Segments) && r.Segments[i].shard == shard {
		return &r.Segments[i]
	}
	return nil
}

func (r *Row) createSegmentIfNotExists(shard uint64) *RowSegment {
	i := sort.Search(len(r.Segments), func(i int) bool {
		return r.Segments[i].shard >= shard
	})

	// Return exact match.
	if i < len(r.Segments) && r.Segments[i].shard == shard {
		return &r.Segments[i]
	}

	// Insert new segment.
	r.Segments = append(r.Segments, RowSegment{data: roaring.NewSliceBitmap()})
	if i < len(r.Segments) {
		copy(r.Segments[i+1:], r.Segments[i:])
	}
	r.Segments[i] = RowSegment{
		data:     roaring.NewSliceBitmap(),
		shard:    shard,
		writable: true,
	}

	return &r.Segments[i]
}

// invalidateCount updates the cached count in the row.
func (r *Row) invalidateCount() {
	for i := range r.Segments {
		r.Segments[i].InvalidateCount()
	}
}

// Count returns the number of columns in the row.
func (r *Row) Count() uint64 {
	var n uint64
	if r == nil {
		// Count(Distinct()) on an empty field panics here
		return n
	}
	for i := range r.Segments {
		n += r.Segments[i].Count()
	}
	return n
}

// MarshalJSON returns a JSON-encoded byte slice of r.
func (r *Row) MarshalJSON() ([]byte, error) {
	var o struct {
		Columns []uint64 `json:"columns"`
		Keys    []string `json:"keys,omitempty"`
	}
	o.Columns = r.Columns()
	o.Keys = r.Keys

	return json.Marshal(&o)
}

// Columns returns the columns in r as a slice of ints.
func (r *Row) Columns() []uint64 {
	// We occasionally hit cases where we want to call Columns on something
	// that might not exist, but a nil slice would be fine.
	if r == nil {
		return nil
	}
	a := make([]uint64, 0, r.Count())
	for i := range r.Segments {
		a = append(a, r.Segments[i].Columns()...)
	}
	return a
}

// Includes returns true if the row contains the given column.
func (r *Row) Includes(col uint64) bool {
	shard := col / ShardWidth
	for i := range r.Segments {
		if r.Segments[i].shard == shard {
			return r.Segments[i].data.Contains(col)
		}
	}
	return false
}

// RowSegment holds a subset of a row.
// This could point to a mmapped roaring bitmap or an in-memory bitmap. The
// width of the segment will always match the shard width.
type RowSegment struct {
	// Shard this segment belongs to
	shard uint64

	// Underlying raw bitmap implementation.
	// This is an mmapped bitmap if writable is false. Otherwise
	// it is a heap allocated bitmap which can be manipulated.
	data     *roaring.Bitmap
	writable bool

	// Bit count
	n uint64
}

func (s *RowSegment) Shard() uint64 {
	return s.shard
}

func (s *RowSegment) Freeze() {
	s.data = s.data.Freeze()
}

/*
// Raw returns the row segment as a byte slice.
// It may be used by the gRPC server to deliver results
// as a roaring bitmap instead of a stream of RowResults.
func (s *rowSegment) Raw() (uint64, []byte) {
	var buf bytes.Buffer
	s.data.WriteTo(&buf)
	return s.shard, buf.Bytes()
}
*/

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
	return s.data.IntersectionCount(other.data)
}

// Intersect returns the itersection of s and other.
func (s *RowSegment) Intersect(other *RowSegment) *RowSegment {
	data := s.data.Intersect(other.data)

	return &RowSegment{
		data:  data,
		shard: s.shard,
		n:     data.Count(),
	}
}

// Union returns the bitwise union of s and other.
func (s *RowSegment) Union(others ...*RowSegment) *RowSegment {
	datas := make([]*roaring.Bitmap, len(others))
	for i, other := range others {
		datas[i] = other.data
	}
	data := s.data.Union(datas...)

	return &RowSegment{
		data:  data,
		shard: s.shard,
		n:     data.Count(),
	}
}

// Difference returns the diff of s and other.
func (s *RowSegment) Difference(others ...*RowSegment) *RowSegment {
	datas := make([]*roaring.Bitmap, len(others))
	for i, other := range others {
		datas[i] = other.data
	}
	data := s.data.Difference(datas...)

	return &RowSegment{
		data:  data,
		shard: s.shard,
		n:     data.Count(),
	}
}

// Xor returns the xor of s and other.
func (s *RowSegment) Xor(other *RowSegment) *RowSegment {
	data := s.data.Xor(other.data)

	return &RowSegment{
		data:  data,
		shard: s.shard,
		n:     data.Count(),
	}
}

// Shift returns s shifted by 1 bit.
func (s *RowSegment) Shift() (*RowSegment, error) {
	// TODO: deal with overflow
	// See issue: https://github.com/molecula/pilosa/issues/403
	data, err := s.data.Shift(1)
	if err != nil {
		return nil, errors.Wrap(err, "shifting roaring data")
	}

	return &RowSegment{
		data:  data,
		shard: s.shard,
		n:     data.Count(),
	}, nil
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

	// This doesn't actually clone all the containers, but does clone
	// the bitmap itself -- we get a new bitmap, but it just marks the
	// containers as frozen and shares them. It's now safe to write to
	// this bitmap, but the actual containers are copy-on-write.
	s.data = s.data.Freeze()
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
