// Copyright 2022 Molecula Corp. (DBA FeatureBase).
// SPDX-License-Identifier: Apache-2.0
// Package roaring implements roaring bitmaps with support for incremental changes.
package roaring

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"io"
	"math/bits"
	"sort"
	"unsafe"

	"github.com/pkg/errors"
)

const (
	// MagicNumber is an identifier, in bytes 0-1 of the file.
	MagicNumber = uint32(12348)

	// storageVersion indicates the storage version, in byte 2.
	storageVersion = uint32(0)

	// NOTE: byte 3 stores user-defined flags.

	// cookie is the first 3 bytes in a roaring bitmap file,
	// formed by joining MagicNumber and storageVersion
	cookie = MagicNumber + storageVersion<<16

	// headerBaseSize is the size in bytes of the cookie, flags, and key count
	// at the beginning of a file.
	headerBaseSize = 3 + 1 + 4

	// runCountHeaderSize is the size in bytes of the run count stored
	// at the beginning of every serialized run container.
	runCountHeaderSize = 2

	// interval16Size is the size of a single run in a container.runs.
	interval16Size = 4

	// bitmapN is the number of values in a container.bitmap.
	bitmapN = (1 << 16) / 64

	MaxContainerVal = 0xffff

	// maxContainerKey is the key representing the last container in a full row.
	// It is the full bitmap space (2^64) divided by container width (2^16).
	maxContainerKey = (1 << 48) - 1
)

const (
	ContainerNil    byte = iota // no container
	ContainerArray              // slice of bit position values
	ContainerBitmap             // slice of 1024 uint64s
	ContainerRun                // container of run-encoded bits
)

// map used for a more descriptive print
var containerTypeNames = map[byte]string{
	ContainerArray:  "array",
	ContainerBitmap: "bitmap",
	ContainerRun:    "run",
}

var fullContainer = NewContainerRun([]Interval16{{Start: 0, Last: MaxContainerVal}}).Freeze()

// AdvisoryError is used for the special case where we probably want to *report*
// an error reading a file, but don't want to actually count the file as not
// being read. For instance, a partial ops-log entry is *probably* harmless;
// we probably crashed while writing (?) and as such didn't report the write
// as successful. We hope.
type AdvisoryError interface {
	error
	AdvisoryOnly()
}

type advisoryError struct {
	e error
}

func (a advisoryError) Error() string {
	return a.e.Error()
}

// This marks the error as safe to ignore.
func (a advisoryError) AdvisoryOnly() {
}

type FileShouldBeTruncatedError interface {
	AdvisoryError
	SuggestedLength() int64
}

type fileShouldBeTruncatedError struct {
	advisoryError
	offset int64
}

func (f *fileShouldBeTruncatedError) SuggestedLength() int64 {
	return f.offset
}

func newFileShouldBeTruncatedError(err error, offset int64) *fileShouldBeTruncatedError {
	return &fileShouldBeTruncatedError{advisoryError: advisoryError{e: err}, offset: offset}
}

type Containers interface {
	// Get returns nil if the key does not exist.
	Get(key uint64) *Container

	// Put adds the container at key.
	Put(key uint64, c *Container)

	// Remove takes the container at key out.
	Remove(key uint64)

	// GetOrCreate returns the container at key, creating a new empty container if necessary.
	GetOrCreate(key uint64) *Container

	// Clone does a deep copy of Containers, including cloning all containers contained.
	Clone() Containers

	// Freeze creates a shallow copy of Containers, freezing all the containers
	// contained. The new copy is a distinct Containers, but the individual containers
	// are shared (but marked as frozen).
	Freeze() Containers

	// Last returns the highest key and associated container.
	Last() (key uint64, c *Container)

	// Size returns the number of containers stored.
	Size() int

	// Update calls fn (existing-container, existed), and expects
	// (new-container, write). If write is true, the container is used to
	// replace the given container.
	Update(key uint64, fn func(*Container, bool) (*Container, bool))

	// UpdateEvery calls fn (existing-container, existed), and expects
	// (new-container, write). If write is true, the container is used to
	// replace the given container.
	UpdateEvery(fn func(uint64, *Container, bool) (*Container, bool))

	// Iterator returns a ContainterIterator which after a call to Next(), a call to Value() will
	// return the first container at or after key. found will be true if a
	// container is found at key.
	Iterator(key uint64) (citer ContainerIterator, found bool)

	Count() uint64

	// Reset clears the containers collection to allow for recycling during snapshot
	Reset()
	// ResetN clears the collection but hints at a needed size.
	ResetN(int)

	// Repair will repair the cardinality of any containers whose cardinality were corrupted
	// due to optimized operations.
	Repair()
}

type ContainerIterator interface {
	Next() bool
	Value() (uint64, *Container)
	Close()
}

type nopContainerIterator struct{}

func (n nopContainerIterator) Next() bool                  { return false }
func (n nopContainerIterator) Value() (uint64, *Container) { return 0, nil }
func (n nopContainerIterator) Close()                      {}

type unionContainerIterator struct {
	iters []ContainerIterator
	curs  []containerWithKey
	cur   FilterKey
}

// NewUnionContainerIterator unions multiple container iterators to one.
func NewUnionContainerIterator(iters ...ContainerIterator) ContainerIterator {
	return &unionContainerIterator{
		iters: iters,
		curs:  make([]containerWithKey, len(iters)),
		cur:   0,
	}
}

func (u *unionContainerIterator) Next() bool {
	if u.cur == KEY_DONE {
		return false
	}
	// Next all iters that are at cur and save lowest
	lowest := uint64(KEY_DONE)
	for i, iter := range u.iters {
		if u.curs[i].key == u.cur {
			if iter.Next() {
				key, c := iter.Value()
				u.curs[i].key, u.curs[i].Container = FilterKey(key), c
				if key < lowest {
					lowest = key
				}
			} else {
				u.curs[i].key = KEY_DONE
				u.curs[i].Container = nil
			}
		}
	}
	u.cur = FilterKey(lowest)
	return u.cur != KEY_DONE
}

func (u *unionContainerIterator) Value() (uint64, *Container) {
	if u.cur == KEY_DONE {
		return uint64(KEY_DONE), nil
	}
	var ret *Container
	for _, cur := range u.curs {
		if cur.key == u.cur {
			ret = ret.UnionInPlace(cur.Container)
		}
	}
	ret.Repair()
	return uint64(u.cur), ret

}

func (u *unionContainerIterator) Close() {}

// Bitmap represents a roaring bitmap.
type Bitmap struct {
	Containers Containers

	// User-defined flags.
	Flags byte
	// should we try to keep things mapped?
	preferMapping bool

	// Number of bit change operations written to the writer. Some operations
	// contain multiple values, so "ops" represents the number of distinct
	// operations, while "opN" represents expected bit changes.
	ops int
	opN int

	// Writer where operations are appended to.
	OpWriter io.Writer
}

// NewBitmap returns a Bitmap with an initial set of values.
func NewBitmap(a ...uint64) *Bitmap {
	b := &Bitmap{
		Containers: newSliceContainers(),
	}
	// We have no way to report this. We aren't in a server context
	// so we haven't got a logger, nothing is checking for nil returns
	// from this.
	// Because we just created Bitmap, its OpWriter is nil, so there
	// is no code path which would cause AddN() to return an error.
	// Therefore, it's safe to swallow this error.
	_, _ = b.AddN(a...)
	return b
}

// NewBitMatrix is a convenience function which returns a new bitmap
// which is the concatenation of all the rows, with each row shifted
// by a shardwdith. For example, all values in the second row will
// have shardWidth added to them before being added to the bitmap.
// Modifies rows in place.
func NewBitMatrix(shardWidth uint64, rows ...[]uint64) *Bitmap {
	bm := NewBitmap()
	for rowNum, row := range rows {
		for i, col := range row {
			row[i] = uint64(rowNum)*shardWidth + col
		}
		bm.AddN(row...)
	}
	return bm
}

// NewSliceBitmap makes a new bitmap, explicitly selecting the slice containers
// type, which performs better in cases where we expect a contiguous block of
// containers added in ascending order, such as when extracting a range from
// another bitmap.
func NewSliceBitmap(a ...uint64) *Bitmap {
	b := &Bitmap{
		Containers: newSliceContainers(),
	}
	// We have no way to report this. We aren't in a server context
	// so we haven't got a logger, nothing is checking for nil returns
	// from this.
	// Because we just created Bitmap, its OpWriter is nil, so there
	// is no code path which would cause AddN() to return an error.
	// Therefore, it's safe to swallow this error.
	_, _ = b.AddN(a...)
	return b
}

// NewFileBitmap returns a Bitmap with an initial set of values, used for file storage.
var NewFileBitmap = NewBTreeBitmap

// Clone returns a heap allocated copy of the bitmap.
// Note: The OpWriter IS NOT copied to the new bitmap.
func (b *Bitmap) Clone() *Bitmap {

	if b == nil {
		return nil
	}

	// Create a copy of the bitmap structure.
	other := &Bitmap{
		Containers: b.Containers.Clone(),
	}

	return other
}

// Freeze returns a shallow copy of the bitmap. The new bitmap
// is a distinct bitmap, with a new Containers object, but the
// actual containers it holds are the same as the parent's
// containers, but have been frozen.
func (b *Bitmap) Freeze() *Bitmap {
	if b == nil {
		return nil
	}

	// Create a copy of the bitmap structure.
	other := &Bitmap{
		Containers: b.Containers.Freeze(),
	}

	return other
}

// Add adds values to the bitmap. TODO(2.0) deprecate - use the more general
// AddN (though be aware that it modifies 'a' in place).
func (b *Bitmap) Add(a ...uint64) (changed bool, err error) {
	changed = false
	for _, v := range a {
		// Create an add operation.
		op := &op{typ: opTypeAdd, value: v}

		// Write operation to op log.
		if err := b.writeOp(op); err != nil {
			return false, err
		}

		// Apply to the in-memory bitmap.
		if b.DirectAdd(v) {
			changed = true
		}
	}

	return changed, nil
}

// AddN adds values to the bitmap, appending them all to the op log in a batched
// write. It returns the number of changed bits.
// The input slice may be reordered, and the set of changed bits will end up in a[:changed].
func (b *Bitmap) AddN(a ...uint64) (changed int, err error) {
	if len(a) == 0 {
		return 0, nil
	}

	changed = b.DirectAddN(a...) // modifies a in-place

	if b.OpWriter != nil && changed > 0 {
		op := &op{
			typ:    opTypeAddBatch,
			values: a[:changed],
		}
		if err := b.writeOp(op); err != nil {
			b.DirectRemoveN(op.values...) // reset data since we're returning an error
			return 0, errors.Wrap(err, "writing to op log")
		}
	}

	return changed, nil
}

// DirectAddN sets multiple bits in the bitmap, returning how many changed. It
// modifies the slice 'a' in place such that once it's complete a[:changed] will
// be list of changed bits. It is more efficient than repeated calls to
// DirectAdd for semi-dense sorted data because it reuses the container from the
// previous value if the new value has the same highbits instead of looking it
// up each time. TODO: if Containers implementations cached the last few
// Container objects returned from calls like Get and GetOrCreate, this
// optimization would be less useful.
func (b *Bitmap) DirectAddN(a ...uint64) (changed int) {
	return b.directOpN((*Container).add, a...)
}

// DirectRemoveN behaves analgously to DirectAddN.
func (b *Bitmap) DirectRemoveN(a ...uint64) (changed int) {
	return b.directOpN((*Container).remove, a...)
}

// directOpN contains the logic for DirectAddN and DirectRemoveN. Theoretically,
// it could be used by anything that wanted to apply a boolean-returning
// container level operation across a list of values and return the number of
// trues while modifying the list of values in place to contain the
// true-returning values in order.
func (b *Bitmap) directOpN(op func(c *Container, v uint16) (*Container, bool), a ...uint64) (changed int) {
	hb := uint64(0xFFFFFFFFFFFFFFFF) // impossible sentinel value
	var cont *Container
	for _, v := range a {
		if newhb := highbits(v); newhb != hb {
			hb = newhb
			cont = b.Containers.GetOrCreate(hb)
		}
		newC, change := op(cont, lowbits(v))
		if change {
			a[changed] = v
			changed++
		}
		if newC != cont {
			b.Containers.Put(hb, newC)
			cont = newC
		}
	}
	return changed
}

// DirectAdd adds a value to the bitmap by bypassing the op log. TODO(2.0)
// deprecate in favor of DirectAddN.
func (b *Bitmap) DirectAdd(v uint64) bool {
	cont := b.Containers.GetOrCreate(highbits(v))
	newC, changed := cont.add(lowbits(v))
	if newC != cont {
		// avoid returning invalid container, and avoid a slow optimize call.
		switch newC.typeID {
		case ContainerArray:
			if len(newC.array()) > ArrayMaxSize {
				newC = newC.arrayToBitmap()
			}
		case ContainerRun:
			if len(newC.runs()) > runMaxSize {
				newC = newC.runToBitmap()
			}
		}
		b.Containers.Put(highbits(v), newC)
	}
	return changed
}

// Contains returns true if v is in the bitmap.
func (b *Bitmap) Contains(v uint64) bool {
	if b == nil {
		return false
	}
	c := b.Containers.Get(highbits(v))
	if c == nil {
		return false
	}
	return c.Contains(lowbits(v))
}

// Remove removes values from the bitmap (writing to the op log if available).
// TODO(2.0) deprecate - use the more general RemoveN (though be aware that it
// modifies 'a' in place).
func (b *Bitmap) Remove(a ...uint64) (changed bool, err error) {
	changed = false
	for _, v := range a {
		// Create an add operation.
		op := &op{typ: opTypeRemove, value: v}

		// Write operation to op log.
		if err := b.writeOp(op); err != nil {
			return false, err
		}

		// Apply operation to the bitmap.
		if op.apply(b) {
			changed = true
		}
	}
	return changed, nil
}

// RemoveN behaves analagously to AddN.
func (b *Bitmap) RemoveN(a ...uint64) (changed int, err error) {
	if len(a) == 0 {
		return 0, nil
	}

	changed = b.DirectRemoveN(a...) // modifies a in-place

	if b.OpWriter != nil && changed > 0 {
		op := &op{
			typ:    opTypeRemoveBatch,
			values: a[:changed],
		}
		if err := b.writeOp(op); err != nil {
			b.DirectAddN(op.values...) // reset data since we're returning an error
			return 0, errors.Wrap(err, "writing to op log")
		}
	}

	return changed, nil
}

func (b *Bitmap) remove(v uint64) bool {
	c := b.Containers.Get(highbits(v))
	newC, changed := c.remove(lowbits(v))
	if newC != c {
		if newC != nil {
			b.Containers.Put(highbits(v), newC)
		} else {
			b.Containers.Remove(highbits(v))
		}
	}
	return changed
}

// Min returns the lowest value in the bitmap.
// Second return value is true if containers exist in the bitmap.
func (b *Bitmap) Min() (uint64, bool) {
	v, eof := b.Iterator().Next()
	return v, !eof
}

// MinAt returns the lowest value in the bitmap at least equal to its argument.
// Second return value is true if containers exist in the bitmap.
func (b *Bitmap) MinAt(start uint64) (uint64, bool) {
	v, eof := b.IteratorAt(start).Next()
	return v, !eof
}

// Max returns the highest value in the bitmap.
// Returns zero if the bitmap is empty.
func (b *Bitmap) Max() uint64 {
	if b.Containers.Size() == 0 {
		return 0
	}

	hb, c := b.Containers.Last()
	lb := c.max()
	return hb<<16 | uint64(lb)
}

// Count returns the number of bits set in the bitmap.
func (b *Bitmap) Count() (n uint64) {
	return b.Containers.Count()
}

// Any checks whether there are any set bits within the bitmap.
func (b *Bitmap) Any() bool {
	iter, _ := b.Containers.Iterator(0)
	// TODO (jaffee) I'm not sure if it's possible/legal to have an empty
	// container, so this loop may be totally unnecessary. In theory, any empty
	// container should be removed from the bitmap though.
	for iter.Next() {
		_, c := iter.Value()
		if c.N() > 0 {
			return true
		}
	}
	return false
}

// Size returns the number of bytes required for the bitmap.
func (b *Bitmap) Size() int {
	numbytes := 0
	citer, _ := b.Containers.Iterator(0)
	for citer.Next() {
		_, c := citer.Value()
		numbytes += c.size()
	}
	return numbytes
}

// CountRange returns the number of bits set between [start, end).
func (b *Bitmap) CountRange(start, end uint64) (n uint64) {
	if roaringSentinel {
		if start > end {
			panic(fmt.Sprintf("counting in range but %v > %v", start, end))
		}
	}

	if b.Containers.Size() == 0 {
		return
	}

	skey := highbits(start)
	ekey := highbits(end)

	citer, found := b.Containers.Iterator(highbits(start))
	// If range is entirely in one container then just count that range.
	if found && skey == ekey {
		citer.Next()
		_, c := citer.Value()
		return uint64(c.countRange(int32(lowbits(start)), int32(lowbits(end))))
	}

	for citer.Next() {
		k, c := citer.Value()
		if k < skey {
			// TODO remove once we've validated this stuff works
			panic("should be impossible for k to be less than skey")
		}

		// k > ekey handles the case when start > end and where start and end
		// are in different containers. Same container case is already handled above.
		if k > ekey {
			break
		}
		if k == skey {
			n += uint64(c.countRange(int32(lowbits(start)), MaxContainerVal+1))
			continue
		}
		if k < ekey {
			n += uint64(c.N())
			continue
		}
		if k == ekey {
			n += uint64(c.countRange(0, int32(lowbits(end))))
			break
		}
	}
	return n
}

// Slice returns a slice of all integers in the bitmap.
func (b *Bitmap) Slice() []uint64 {
	var a []uint64
	itr := b.Iterator()
	itr.Seek(0)

	for v, eof := itr.Next(); !eof; v, eof = itr.Next() {
		a = append(a, v)
	}
	return a
}

// SliceRange returns a slice of integers between [start, end).
func (b *Bitmap) SliceRange(start, end uint64) []uint64 {
	if roaringSentinel {
		if start > end {
			panic(fmt.Sprintf("getting slice in range but %v > %v", start, end))
		}
	}
	var a []uint64
	itr := b.Iterator()
	itr.Seek(start)
	for v, eof := itr.Next(); !eof && v < end; v, eof = itr.Next() {
		a = append(a, v)
	}
	return a
}

// ForEach executes fn for each value in the bitmap.
func (b *Bitmap) ForEach(fn func(uint64) error) error {
	itr := b.Iterator()
	itr.Seek(0)
	for v, eof := itr.Next(); !eof; v, eof = itr.Next() {
		if err := fn(v); err != nil {
			return err
		}
	}
	return nil
}

// ForEachRange executes fn for each value in the bitmap between [start, end).
func (b *Bitmap) ForEachRange(start, end uint64, fn func(uint64) error) error {
	itr := b.Iterator()
	itr.Seek(start)
	for v, eof := itr.Next(); !eof && v < end; v, eof = itr.Next() {
		if err := fn(v); err != nil {
			return err
		}
	}
	return nil
}

// OffsetRange returns a new bitmap with a containers offset by start.
// The containers themselves are shared, so they get frozen so it will
// be safe to interact with them.
func (b *Bitmap) OffsetRange(offset, start, end uint64) *Bitmap {
	if lowbits(offset) != 0 {
		panic("offset must not contain low bits")
	}
	if lowbits(start) != 0 {
		panic("range start must not contain low bits")
	}
	if lowbits(end) != 0 {
		panic("range end must not contain low bits")
	}

	off := highbits(offset)
	hi0, hi1 := highbits(start), highbits(end)
	citer, _ := b.Containers.Iterator(hi0)
	other := NewSliceBitmap()
	for citer.Next() {
		k, c := citer.Value()
		if k >= hi1 {
			break
		}
		other.Containers.Put(off+(k-hi0), c.Freeze())
	}
	return other
}

// container returns the container with the given key.
func (b *Bitmap) container(key uint64) *Container {
	return b.Containers.Get(key)
}

// IntersectionCount returns the number of set bits that would result in an
// intersection between b and other. It is more efficient than actually
// intersecting the two and counting the result.
func (b *Bitmap) IntersectionCount(other *Bitmap) uint64 {
	var n uint64
	iiter, _ := b.Containers.Iterator(0)
	jiter, _ := other.Containers.Iterator(0)
	i, j := iiter.Next(), jiter.Next()
	ki, ci := iiter.Value()
	kj, cj := jiter.Value()
	for i && j {
		if ki < kj {
			i = iiter.Next()
			ki, ci = iiter.Value()
		} else if ki > kj {
			j = jiter.Next()
			kj, cj = jiter.Value()
		} else {
			n += uint64(intersectionCount(ci, cj))
			i, j = iiter.Next(), jiter.Next()
			ki, ci = iiter.Value()
			kj, cj = jiter.Value()
		}
	}
	return n
}

// Intersect returns the intersection of b and other.
func (b *Bitmap) Intersect(other *Bitmap) *Bitmap {
	output := NewBitmap()
	iiter, _ := b.Containers.Iterator(0)
	jiter, _ := other.Containers.Iterator(0)
	i, j := iiter.Next(), jiter.Next()
	ki, ci := iiter.Value()
	kj, cj := jiter.Value()
	for i && j {
		if ki < kj {
			i = iiter.Next()
			ki, ci = iiter.Value()
		} else if ki > kj {
			j = jiter.Next()
			kj, cj = jiter.Value()
		} else { // ki == kj
			newC := intersect(ci, cj)
			output.Containers.Put(ki, newC)
			i, j = iiter.Next(), jiter.Next()
			ki, ci = iiter.Value()
			kj, cj = jiter.Value()
		}
	}
	return output
}

func (b *Bitmap) Hash(hash uint64) uint64 {
	const (
		offset = 14695981039346656037
		prime  = 1099511628211
	)
	if hash == 0 {
		hash = uint64(offset)
	}

	it, _ := b.Containers.Iterator(0)
	for it.Next() {
		ki, _ := it.Value()
		hash ^= uint64(ki)
		hash *= prime
	}

	it, _ = b.Containers.Iterator(0)
	for it.Next() {
		_, ci := it.Value()
		hash ^= 0
		hash *= prime
		if ci.N() > 0 {
			var bytes []byte
			switch ci.typ() {

			case ContainerArray:
				bytes = fromArray16(ci.array())
			case ContainerBitmap:
				bytes = fromArray64(ci.bitmap())
			case ContainerRun:
				bytes = fromInterval16(ci.runs())
			}
			for _, b := range bytes {
				hash ^= uint64(b)
				hash *= prime
			}
		}
	}
	return hash
}

type mutableContainersIterator struct {
	c Containers

	cit ContainerIterator
	sit *sliceIterator
}

func newMutableContainersIterator(cs Containers, key uint64) *mutableContainersIterator {
	it := &mutableContainersIterator{c: cs}

	if sc, ok := cs.(*sliceContainers); ok {
		if i, found := sc.seek(key); found {
			it.sit = &sliceIterator{e: sc, i: i, index: i}
			return it
		}
	}

	it.cit, _ = cs.Iterator(key)
	return it
}

func (it *mutableContainersIterator) update(key uint64, newCont *Container) {
	if it.sit != nil {
		if key == it.sit.key {
			it.sit.e.containers[it.sit.index] = newCont
		}
	}

	it.c.Update(key, func(_ *Container, _ bool) (*Container, bool) {
		return newCont, true
	})
}

func (it *mutableContainersIterator) Next() bool {
	if it.sit != nil {
		return it.sit.Next()
	}

	return it.cit.Next()
}

func (it *mutableContainersIterator) Value() (uint64, *Container) {
	if it.sit != nil {
		return it.sit.Value()
	}

	return it.cit.Value()
}

func (it *mutableContainersIterator) Close() {}

// IntersectInPlace returns the bitwise intersection of b and others,
// modifying b in place.
func (b *Bitmap) IntersectInPlace(others ...*Bitmap) {
	var bSize int
	if bSize = b.Size(); bSize == 0 {
		// If b doesn't have any containers then return early.
		return
	}

	otherIters := make(handledIters, 0, len(others))
	for _, other := range others {
		it, _ := other.Containers.Iterator(0)
		if !it.Next() {
			// An empty bitmap - reset all
			b.Containers.Reset()
			return
		}

		otherIters = append(otherIters, handledIter{
			iter:    it,
			hasNext: true,
		})
	}

	bIter := newMutableContainersIterator(b.Containers, 0)
	for bIter.Next() {
		bKey, bCont := bIter.Value()
		if bCont.N() == 0 {
			// No point in intersecting things from an empty container.
			bIter.update(bKey, nil)
			continue
		}

		// Loop until every iters current value has been handled.
		for _, otherIter := range otherIters {
			if !otherIter.hasNext {
				continue
			}

			otherKey, otherCont := otherIter.iter.Value()
			for otherKey < bKey {
				otherIter.hasNext = otherIter.iter.Next()
				if !otherIter.hasNext {
					break
				}
				otherKey, otherCont = otherIter.iter.Value()
			}

			if bKey == otherKey {
				// Note: a nil container is valid, and has N == 0.
				if otherCont.N() != 0 {
					if bCont.frozen() {
						bCont = bCont.Clone()
						b.Containers.Put(bKey, bCont)
					}
					bCont = bCont.intersectInPlace(otherCont)
					bIter.update(bKey, bCont)
					if bCont == nil || bCont.N() == 0 {
						break
					}

					otherIter.hasNext = otherIter.iter.Next()
					continue
				}
			}

			bIter.update(bKey, nil)
			break
		}
	}

	b.Containers.Repair()
}

func (c *Container) intersectInPlace(other *Container) *Container {
	// short-circuit the trivial cases
	if c == nil || other == nil || c.N() == 0 || other.N() == 0 {
		c = nil
		return c
	}
	cFull, otherFull := (c.N() == MaxContainerVal+1), (other.N() == MaxContainerVal+1)
	if cFull && otherFull {
		return c
	}
	if cFull {
		return c.copyInPlace(other)
	}
	if otherFull {
		return c
	}

	switch c.typ() {
	case ContainerArray:
		switch other.typ() {
		case ContainerArray:
			return intersectArrayArrayInPlace(c, other)
		case ContainerBitmap:
			return intersectArrayBitmapInPlace(c, other)
		case ContainerRun:
			return intersectArrayRunInPlace(c, other)
		}

	case ContainerBitmap:
		switch other.typ() {
		case ContainerArray:
			return intersectBitmapArrayInPlace(c, other)
		case ContainerBitmap:
			return intersectBitmapBitmapInPlace(c, other)
		case ContainerRun:
			return intersectBitmapRunInPlace(c, other)
		}

	case ContainerRun:
		switch other.typ() {
		case ContainerArray:
			return intersectRunArrayInPlace(c, other)
		case ContainerBitmap:
			return intersectRunBitmapInPlace(c, other)
		case ContainerRun:
			return intersectRunRunInPlace(c, other)
		}
	}

	panic(fmt.Errorf("invalid intersect op: unknown types %d/%d", c.typ(), other.typ()))
}

func (c *Container) copyInPlace(other *Container) *Container {
	switch other.typ() {
	case ContainerArray:
		c.setTyp(ContainerArray)
		c.setArrayMaybeCopy(other.array(), true)

	case ContainerBitmap:
		c.setTyp(ContainerBitmap)
		c.setBitmapCopy(other.bitmap())
		c.setN(other.N())

	case ContainerRun:
		c.setTyp(ContainerRun)
		c.setRunsMaybeCopy(other.runs(), true)
		c.setN(other.N())

	default:
		panic(fmt.Errorf("invalid container type: %v", c.typ()))
	}

	return c
}

func intersectArrayArrayInPlace(a, b *Container) *Container {
	statsHit("intersectInPlace/ArrayArray")

	a = a.Thaw()
	aa, ba := a.array(), b.array()
	an, bn := len(aa), len(ba)
	n := 0
	for i, j := 0, 0; i < an && j < bn; {
		va, vb := aa[i], ba[j]

		if va < vb {
			i++
		} else if va > vb {
			j++
		} else {
			aa[n] = va
			n, i, j = n+1, i+1, j+1
		}
	}
	aa = aa[:n]
	a.setArray(aa)

	return a
}

func intersectArrayRunInPlace(a, b *Container) *Container {
	statsHit("intersectInPlace/ArrayRun")

	a = a.Thaw()
	aa, br := a.array(), b.runs()
	an, bn := len(aa), len(br)

	n := 0
	for i, j := 0, 0; i < an && j < bn; {
		va, vb := aa[i], br[j]
		if va < vb.Start {
			i++
		} else if va > vb.Last {
			j++
		} else {
			aa[n] = va
			n++
			i++
		}
	}

	aa = aa[:n]
	a.setArray(aa)

	return a
}

func intersectArrayBitmapInPlace(a, b *Container) *Container {
	statsHit("intersectInPlace/ArrayBitmap")

	a = a.Thaw()
	aa := a.array()
	bb := b.bitmap()

	n := 0
	for _, va := range aa {
		b := bb[va>>6]
		bidx := va % 64
		mask := uint64(1) << bidx
		if b&mask > 0 {
			aa[n] = va
			n++
		}
	}

	aa = aa[:n]
	a.setArray(aa)

	return a
}

func intersectBitmapBitmapInPlace(a, b *Container) *Container {
	statsHit("intersectInPlace/BitmapBitmap")

	a = a.Thaw()
	ab := a.bitmap()[:bitmapN]
	bb := b.bitmap()[:bitmapN]

	n := int32(0)
	for i := 0; i < bitmapN; i += 4 {
		// unrolling is still effective in go
		// TODO: the generated machine code is extremely bad here, because we are forcing the compiler to reload ab immediately after storing it
		// The body of the loop has a total of 8 branches: 4 bounds checks + 4 feature checks.
		// We could substantially improve this by converting the entire bitmap to [256][4]uint64.
		ptr := (*[4]uint64)(unsafe.Pointer(&bb[i]))
		ab[i] &= ptr[0]
		ab[i+1] &= ptr[1]
		ab[i+2] &= ptr[2]
		ab[i+3] &= ptr[3]

		n += int32(popcount(ab[i])) +
			int32(popcount(ab[i+1])) +
			int32(popcount(ab[i+2])) +
			int32(popcount(ab[i+3]))
	}
	a.setN(n)

	return a
}

func intersectBitmapArrayInPlace(a, b *Container) *Container {
	statsHit("intersectInPlace/BitmapArray")

	a = a.Thaw()
	ab := a.bitmap()
	ba := b.array()

	bn := len(ba)
	array := make([]uint16, bn)
	n := int32(0)
	for _, vb := range ba {
		i := vb >> 6
		mask := uint64(1) << uint(vb%64)
		if ab[i]&mask > 0 {
			array[n] = vb
			n++
		}
	}
	array = array[:n]
	a.setTyp(ContainerArray)
	a.setArray(array)

	return a
}

func intersectBitmapRunInPlace(a, b *Container) *Container {
	statsHit("intersectInPlace/BitmapRun")

	a = a.Thaw()
	ab := a.bitmap()
	br := b.runs()
	an := len(ab)
	bitmap := make([]uint64, an)

	n := int32(0)
	for _, vb := range br {
		i := vb.Start >> 6 // index into a

		vastart := i << 6
		valast := vastart + 63
		for valast >= vb.Start && vastart <= vb.Last && int(i) < an {
			if vastart >= vb.Start && valast <= vb.Last { // a within b
				bitmap[i] = ab[i]
				n += int32(popcount(ab[i]))
			} else if vb.Start >= vastart && vb.Last <= valast { // b within a
				var mask uint64 = ((1 << (vb.Last - vb.Start + 1)) - 1) << (vb.Start - vastart)
				bits := ab[i] & mask
				bitmap[i] |= bits
				n += int32(popcount(bits))
			} else if vastart < vb.Start { // a overlaps front of b
				offset := 64 - (1 + valast - vb.Start)
				bits := (ab[i] >> offset) << offset
				bitmap[i] |= bits
				n += int32(popcount(bits))
			} else if vb.Start < vastart { // b overlaps front of a
				offset := 64 - (1 + vb.Last - vastart)
				bits := (ab[i] << offset) >> offset
				bitmap[i] |= bits
				n += int32(popcount(bits))
			}

			i++
			vastart = i << 6
			valast = vastart + 63
		}
	}
	a.setBitmap(bitmap)
	a.setN(n)

	return a
}

func intersectRunRunInPlace(a, b *Container) *Container {
	statsHit("intersectInPlace/RunRun")

	a = a.Thaw()
	ar, br := a.runs(), b.runs()
	an, bn := len(ar), len(br)

	var runs []Interval16
	if an > bn {
		runs = make([]Interval16, 0, an)
	} else {
		runs = make([]Interval16, 0, bn)
	}

	n := int32(0)
	for i, j := 0, 0; i < an && j < bn; {
		va, vb := ar[i], br[j]

		if va.Last < vb.Start {
			// |--va--| |--vb--|
			i++
		} else if vb.Last < va.Start {
			// |--vb--| |--va--|
			j++
		} else if va.Last > vb.Last && va.Start >= vb.Start {
			// |--vb-|-|-va--|
			runs = append(runs, Interval16{Start: va.Start, Last: vb.Last})
			n += int32(vb.Last-va.Start) + 1
			j++
		} else if va.Last > vb.Last && va.Start < vb.Start {
			// |--va|--vb--|--|
			runs = append(runs, vb)
			n += int32(vb.Last-vb.Start) + 1
			j++
		} else if va.Last <= vb.Last && va.Start >= vb.Start {
			// |--vb|--va--|--|
			runs = append(runs, va)
			n += int32(va.Last-va.Start) + 1
			i++
		} else if va.Last <= vb.Last && va.Start < vb.Start {
			// |--va-|-|-vb--|
			runs = append(runs, Interval16{Start: vb.Start, Last: va.Last})
			n += int32(va.Last-vb.Start) + 1
			i++
		}
	}
	a.setRuns(runs)
	a.setN(n)

	return a
}

func intersectRunArrayInPlace(a, b *Container) *Container {
	statsHit("intersectInPlace/RunArray")

	a = a.Thaw()
	ar, ba := a.runs(), b.array()
	an, bn := len(ar), len(ba)

	array := make([]uint16, bn)
	n := 0
	for i, j := 0, 0; i < an && j < bn; {
		va, vb := ar[i], ba[j]
		if vb < va.Start {
			j++
		} else if vb > va.Last {
			i++
		} else {
			array[n] = vb
			n++
			j++
		}
	}

	array = array[:n]
	a.setTyp(ContainerArray)
	a.setArray(array)

	return a
}

func intersectRunBitmapInPlace(a, b *Container) *Container {
	statsHit("intersectInPlace/RunBitmap")

	// TODO(@kuba--):
	// Figure out how to efficiently intersect dense runs with bitmaps.
	// So far we convert run to bitmap and intersect bitmaps - it's much faster
	// than naive O(n^2) algorithm.
	a = a.runToBitmap()
	return intersectBitmapBitmapInPlace(a, b)
}

// Union returns the bitwise union of b and others as a new bitmap.
func (b *Bitmap) Union(others ...*Bitmap) *Bitmap {
	if len(others) == 1 {
		output := NewBitmap()
		b.unionIntoTargetSingle(output, others[0])
		return output
	}
	// It may seem counterintuitive to freeze this, but the result is
	// a new bitmap which can be safely modified, but postponing any
	// allocations until an actual write to any given container.
	output := b.Freeze()
	output.UnionInPlace(others...)
	return output
}

// UnionInPlace returns the bitwise union of b and others, modifying
// b in place.
func (b *Bitmap) UnionInPlace(others ...*Bitmap) {
	b.unionInPlace(others...)
}

func (b *Bitmap) unionIntoTargetSingle(target *Bitmap, other *Bitmap) {
	iiter, _ := b.Containers.Iterator(0)
	jiter, _ := other.Containers.Iterator(0)
	i, j := iiter.Next(), jiter.Next()
	ki, ci := iiter.Value()
	kj, cj := jiter.Value()
	for i || j {
		if i && (!j || ki < kj) {
			target.Containers.Put(ki, ci.Freeze())
			i = iiter.Next()
			ki, ci = iiter.Value()
		} else if j && (!i || ki > kj) {
			target.Containers.Put(kj, cj.Freeze())
			j = jiter.Next()
			kj, cj = jiter.Value()
		} else { // ki == kj
			newC := union(ci, cj)
			target.Containers.Put(ki, newC)
			i, j = iiter.Next(), jiter.Next()
			ki, ci = iiter.Value()
			kj, cj = jiter.Value()
		}
	}
}

// unionInPlace stores the union of b and others into b. The others will
// be left unchanged.
//
// This function performs an n-way union of n bitmaps. It performs this in an
// optimized manner looping through all the bitmaps and performing unions one
// container at a time. As a result, instead of generating many intermediary
// containers for each union operation for a given container key, only one
// new container needs to be allocated (or re-used) regardless of how many bitmaps
// participate in the union. This significantly reduces allocations. In addition,
// because we perform the unions one container at a time across all the bitmaps, we
// can calculate summary statistics that allow us to make more efficient decisions
// up front. For instance, if we have a non-bitmap target container, but we
// expect more than ArrayMaxSize bits, we can convert to bitmap preemptively.
// This will sometimes be wrong (we can't really tell how many bits we'll have
// after a union) but is probably close enough to be useful. This will save
// some reallocations for cases where several consecutive ops have array
// representations, and we expect to have to convert to a bitmap eventually;
// we don't allocate larger and larger array slices before doing that.
//
// An additional optimization that this function makes is that it recognizes that even when
// CPU support is present, performing the popcount() operation isn't free. Imagine a scenario
// where 10 bitset containers are being unioned together one after the next. If every
// bitset<->bitset union operation needs to keep the containers' cardinality up to date, then
// the algorithm will waste a lot of time performing intermediary popcount() operations that
// will immediately be invalidated by the next union operation. As a result, we allow the cardinality
// of containers to degrade when we perform the in-place union operations, and then when the algorithm
// completes we "repair" all the containers by performing the popcount() operation one time. This means
// that we only ever have to do O(1) popcount operations per container instead of O(n) where n is the
// number of containers with the same key that are being unioned together.
//
// The algorithm works by iterating through all of the containers in all of the bitmaps concurrently.
// At every "tick" of the outermost loop, we increment our pointer into the bitmaps list of containers
// by 1 (if we haven't reached the end of the containers for that bitmap.)
//
// We then loop through all of the "current" values(containers) for all of the bitmaps
// and for each container with a specific key that we encounter, we scan forward to see if any of the
// other bitmaps have a container for the same key. If so, we calculate some summary statistics and
// then use that information to make a decision about how to union all of the containers with the same
// key together, perform the union, mark the unioned containers as "handled" and then move on to the next
// batch of containers that share the same key.
//
// We repeat this process until every single bitmaps current container has been "handled". Then we start the
// outer loop over again and the process repeats until we've iterated through every container in every bitmap
// and unioned everything into a single target bitmap.
//
// The diagram below shows the iteration state of four different bitmaps as the algorithm progresses them.
// The diagrams should BE interpreted from left -> right, top -> bottom. The X's represent a container in
// the bitmap at a specific key,  ^ symbol represents the bitmaps current container iteration position,
// and the - symbol represents a container that is at the current iteration position, but has been marked as "handled".
//
//	----------------------------      |      ----------------------------      |      ----------------------------
//
// Bitmap 1 |___X____________X__________|     |      |___X____________X__________|     |      |___X____________X__________|
//
//	    ^                             |          _                             |
//	----------------------------      |      ----------------------------      |      ----------------------------
//
// Bitmap 2 |_______X________X______X___|     |      |_______X_______________X___|     |      |_______X_______________X___|
//
//	        ^                         |              ^                         |
//	----------------------------      |      ----------------------------      |      ----------------------------
//
// Bitmap 3 |_______X___________________|     |      |_______X___________________|     |      |_______X___________________|
//
//	        ^                         |              ^                         |
//	----------------------------      |      ----------------------------      |      ----------------------------
//
// Bitmap 4 |___X_______________________|     |      |___X_______________________|     |      |___X_______________________|
//
//	^                             |          _                             |
//
// ------------------------------------------------------------------------------------------------------------------------
//
//	----------------------------      |      ----------------------------      |      ----------------------------
//
// Bitmap 1 |___X____________X__________|     |      |___X____________X__________|     |      |___X____________X__________|
//
//	    _                             |                       ^                |                       _
//	----------------------------      |      ----------------------------      |      ----------------------------
//
// Bitmap 2 |_______X_______________X___|     |      |_______X_______________X___|     |      |_______X_______________X___|
//
//	        _                         |                              ^         |                              ^
//	----------------------------      |      ----------------------------      |      ----------------------------
//
// Bitmap 3 |_______X___________________|     |      |_______X___________________|     |      |_______X___________________|
//
//	        _                         |                                        |
//	----------------------------      |      ----------------------------      |      ----------------------------
//
// Bitmap 4 |___X_______________________|     |      |___X_______________________|     |      |___X_______________________|
//
//	_
func (b *Bitmap) unionInPlace(others ...*Bitmap) {
	const staticSize = 20
	var (
		requiredSliceSize = len(others)
		// To avoid having to allocate a slice every time, if the number of bitmaps
		// being unioned is small enough (i.e. smaller than staticSize), we can just
		// use this stack-allocated array.
		staticHandledIters = [staticSize]handledIter{}
		bitmapIters        handledIters
		target             = b
	)

	if requiredSliceSize <= staticSize {
		bitmapIters = staticHandledIters[:0]
	} else {
		bitmapIters = make(handledIters, 0, requiredSliceSize)
	}

	for _, other := range others {
		otherIter, _ := other.Containers.Iterator(0)
		if otherIter.Next() {
			bitmapIters = append(bitmapIters, handledIter{
				iter:    otherIter,
				hasNext: true,
				handled: false,
			})
		}
	}

	// Loop until we've exhausted every iter.
	hasNext := true
	for hasNext {
		// Loop until every iters current value has been handled.
		for i, iIter := range bitmapIters {
			if !iIter.hasNext || iIter.handled {
				// Either we've exhausted this iter (it has no more containers), or
				// we've already handled the current container by unioning it with
				// one of the containers we encountered earlier.
				continue
			}

			iKey, iContainer := iIter.iter.Value()
			expectedN := int64(0)

			// determine whether we have a target to union into.
			tContainer := target.Containers.Get(iKey)
			// if the target's full, short-circuit out.
			if tContainer != nil {
				tN, ok := tContainer.SafeN()
				if ok && tN == MaxContainerVal+1 {
					bitmapIters.markItersWithKeyAsHandled(i, iKey)
					continue
				}
				expectedN = int64(tN)
			}
			// Check i and later iters for any max-range containers, and
			// find out how many there are.
			summaryStats := bitmapIters[i:].calculateSummaryStats(iKey)
			if summaryStats.hasMaxRange {
				// One (or more) of the containers represented the maximum possible
				// range that a container can store, so instead of calculating a
				// union we can generate an RLE container that represents the entire
				// range.
				tContainer = fullContainer
				target.Containers.Put(iKey, tContainer)
				bitmapIters.markItersWithKeyAsHandled(i, iKey)
				continue
			}
			expectedN += summaryStats.n
			var itersToUnion handledIters
			// Overview: We know that we have at least one "other" container
			// to union in, and we may have a target container already. We want
			// to shortcut easy cases ("no target container, exactly one
			// other container").
			if tContainer == nil {
				// No existing target container.
				if summaryStats.c == 1 {
					// There's no target and we have only one container, we
					// can just reuse it instead of unioning.
					statsHit("unionInPlace/reuse")
					target.Containers.Put(iKey, iContainer.Freeze())
					bitmapIters[i].handled = true
					continue
				}
				// We have at least two other containers. We can union
				// everything together. We can union everything but
				// the first other container into a clone of the
				// first other container, but for some cases, that will
				// result in cloning a non-bitmap, then converting it
				// to a bitmap, and this will be expensive...
				if expectedN >= 512 && iContainer.typ() != ContainerBitmap {
					// copying the non-bitmap, then converting it,
					// is expensive.
					statsHit("unionInPlace/newBitmap")
					tContainer = NewContainerBitmapN(nil, 0)
					itersToUnion = bitmapIters[i:]
				} else {
					// either N will be small or iContainer is a
					// bitmap, so we can skip one union op by copying it.
					// And we can just freeze it, and the copy will
					// happen later if it's needed...
					statsHit("unionInPlace/clone")
					tContainer = iContainer.Freeze()
					itersToUnion = bitmapIters[i+1:]
				}
			} else {
				// we have an existing target container. If we're
				// going to end up wanting it to be a bitmap, we
				// convert it preemptively, because union into a
				// bitmap is nearly always faster.
				itersToUnion = bitmapIters[i:]
				if expectedN >= 512 && tContainer.typ() != ContainerBitmap {
					statsHit("unionInPlace/convertToBitmap")
					switch tContainer.typ() {
					case ContainerArray:
						tContainer = tContainer.arrayToBitmap()
					case ContainerRun:
						tContainer = tContainer.runToBitmap()
					}
				}
			}

			// Now we union all remaining containers with this key
			// together.
			for j, iter := range itersToUnion {
				jKey, jContainer := iter.iter.Value()

				if iKey == jKey {
					tContainer = tContainer.unionInPlace(jContainer)
					// "iter" is a local copy from the range
					// loop, not the actual slice member.
					itersToUnion[j].handled = true
				}
			}

			// Now that we've calculated a container that is a union of all the containers
			// with the same key across all the bitmaps, we store it in the list of containers
			// for the target.
			target.Containers.Put(iKey, tContainer)
		}

		hasNext = bitmapIters.next()
	}

	// Performing the popcount() operation with every union is wasteful because
	// its likely the value will be invalidated by the next union operation. As
	// a result, when we're performing all of our in-place unions we allow the value of
	// n (container cardinality) to fall out of sync, and then at the very end we perform
	// a "Repair" to recalculate all the container values. That way we never popcount()
	// an entire bitmap container more than once per bulk union operation.
	target.Containers.Repair()
}

// Difference returns the difference of b and other.
func (b *Bitmap) Difference(other ...*Bitmap) *Bitmap {
	output := b.singleDifference(other[0])
	if len(other) > 1 {
		output.DifferenceInPlace(other[1:]...)
	}
	return output
}

func (b *Bitmap) singleDifference(other *Bitmap) *Bitmap {
	output := NewBitmap()
	iiter, _ := b.Containers.Iterator(0)
	jiter, _ := other.Containers.Iterator(0)
	i, j := iiter.Next(), jiter.Next()
	ki, ci := iiter.Value()
	kj, cj := jiter.Value()
	for i || j {
		if i && (!j || ki < kj) {
			output.Containers.Put(ki, ci.Freeze())
			i = iiter.Next()
			ki, ci = iiter.Value()
		} else if j && (!i || ki > kj) {
			j = jiter.Next()
			kj, cj = jiter.Value()
		} else { // ki == kj
			output.Containers.Put(ki, difference(ci, cj))
			i, j = iiter.Next(), jiter.Next()
			ki, ci = iiter.Value()
			kj, cj = jiter.Value()
		}
	}
	return output
}

// Xor returns the bitwise exclusive or of b and other.
func (b *Bitmap) Xor(other *Bitmap) *Bitmap {
	output := NewBitmap()

	iiter, _ := b.Containers.Iterator(0)
	jiter, _ := other.Containers.Iterator(0)
	i, j := iiter.Next(), jiter.Next()
	ki, ci := iiter.Value()
	kj, cj := jiter.Value()
	for i || j {
		if i && (!j || ki < kj) {
			output.Containers.Put(ki, ci.Freeze())
			i = iiter.Next()
			ki, ci = iiter.Value()
		} else if j && (!i || ki > kj) {
			output.Containers.Put(kj, cj.Freeze())
			j = jiter.Next()
			kj, cj = jiter.Value()
		} else { // ki == kj
			output.Containers.Put(ki, xor(ci, cj))
			i, j = iiter.Next(), jiter.Next()
			ki, ci = iiter.Value()
			kj, cj = jiter.Value()
		}
	}
	return output
}

// Shift shifts the contents of b by 1.
//
// NOTE: This method is unsupported. See the `Shift()`
// method on `Row` in `row.go`.
func (b *Bitmap) Shift(n int) (*Bitmap, error) {
	if n != 1 {
		return nil, errors.New("cannot shift by a value other than 1")
	}
	output := NewBitmap()
	iiter, _ := b.Containers.Iterator(0)
	lastCarry := false
	lastKey := uint64(0)
	for iiter.Next() {
		ki, ci := iiter.Value()
		if lastCarry && ki > lastKey+1 {
			extra := NewContainerArray([]uint16{0})
			output.Containers.Put(lastKey+1, extra)
			lastCarry = false
		}
		o, carry := shift(ci)
		if lastCarry {
			o, _ = o.add(0)
		}
		if o.N() > 0 {
			output.Containers.Put(ki, o)
		}
		lastCarry = carry
		lastKey = ki
	}
	// As long as the carry wasn't from the max container,
	// append a new container and add the carried bit.
	if lastCarry && lastKey != maxContainerKey {
		extra := NewContainerArray([]uint16{0})
		output.Containers.Put(lastKey+1, extra)
	}

	return output, nil
}

// removeEmptyContainers deletes all containers that have a count of zero.
func (b *Bitmap) removeEmptyContainers() {
	citer, _ := b.Containers.Iterator(0)
	for citer.Next() {
		k, c := citer.Value()
		if c.N() == 0 {
			b.Containers.Remove(k)
		}
	}
}

func (b *Bitmap) countNonEmptyContainers() int {
	result := 0
	citer, _ := b.Containers.Iterator(0)
	for citer.Next() {
		_, c := citer.Value()
		if c.N() > 0 {
			result++
		}
	}
	return result
}

// Optimize converts array and bitmap containers to run containers as necessary.
func (b *Bitmap) Optimize() {
	b.Containers.UpdateEvery(func(key uint64, c *Container, existed bool) (*Container, bool) {
		return c.optimize(), true
	})
}

type errWriter struct {
	w   io.Writer
	err error
	n   int
}

func (ew *errWriter) WriteUint16(b []byte, v uint16) {
	if ew.err != nil {
		return
	}
	var n int
	binary.LittleEndian.PutUint16(b, v)
	n, ew.err = ew.w.Write(b)
	ew.n += n
}
func (ew *errWriter) WriteUint32(b []byte, v uint32) {
	if ew.err != nil {
		return
	}
	var n int
	binary.LittleEndian.PutUint32(b, v)
	n, ew.err = ew.w.Write(b)
	ew.n += n
}

func (ew *errWriter) WriteUint64(b []byte, v uint64) {
	if ew.err != nil {
		return
	}
	var n int
	binary.LittleEndian.PutUint64(b, v)
	n, ew.err = ew.w.Write(b)
	ew.n += n
}

// WriteTo writes b to w.
func (b *Bitmap) WriteTo(w io.Writer) (n int64, err error) {
	b.Optimize()
	return b.writeToUnoptimized(w)
}

// writeToUnoptimized is a WriteTo without the Optimize path. We need
// this because otherwise we can't do some of our marshal/unmarshal tests
// safely.
func (b *Bitmap) writeToUnoptimized(w io.Writer) (n int64, err error) {
	// Remove empty containers before persisting.
	//b.removeEmptyContainers()

	containerCount := b.countNonEmptyContainers()
	byte2 := make([]byte, 2)
	byte4 := make([]byte, 4)
	byte8 := make([]byte, 8)

	// Build header before writing individual container blocks.
	// Metadata for each container is 8+2+2+4 = sizeof(key) + sizeof(type)+sizeof(cardinality) + sizeof(file offset)
	// Type is stored as 2 bytes, even though it's only got values 1..3.
	// Cookie header section.
	ew := &errWriter{
		w: w,
		n: 0,
	}

	ew.WriteUint32(byte4, cookie|(uint32(b.Flags)<<24))
	ew.WriteUint32(byte4, uint32(containerCount))

	// Descriptive header section: encode keys and cardinality.
	// Key and cardinality are stored interleaved here, 12 bytes per container.
	citer, _ := b.Containers.Iterator(0)
	for citer.Next() {
		key, c := citer.Value()
		// Verify container count before writing.
		// TODO: instead of commenting this out, we need to make it a configuration option
		//count := c.count()
		//assert(c.count() == c.n, "cannot write container count, mismatch: count=%d, n=%d", count, c.n)
		if c.N() > 0 {
			ew.WriteUint64(byte8, key)
			ew.WriteUint16(byte2, uint16(c.typ()))
			ew.WriteUint16(byte2, uint16(c.N()-1))
		}
	}
	if ew.n != headerBaseSize+(containerCount*12) {
		return int64(ew.n), fmt.Errorf("after writing %d headers, wrote %d bytes, expected %d",
			containerCount, ew.n, headerBaseSize+(containerCount*12))
	}

	// Offset header section: write the offset for each container block.
	// 4 bytes per container. The actual data offsets will then be 4 bytes
	// further in per non-empty container, and now that we've scanned the
	// containers, we know how many we really have.
	offset := uint32(ew.n + (containerCount * 4))
	citer, _ = b.Containers.Iterator(0)
	for citer.Next() {
		_, c := citer.Value()
		if c.N() > 0 {
			ew.WriteUint32(byte4, offset)
			offset += uint32(c.size())
		}
	}
	if ew.err != nil {
		return int64(ew.n), ew.err
	}
	if ew.n != headerBaseSize+(containerCount*16) {
		return int64(ew.n), fmt.Errorf("after writing %d headers+offsets, wrote %d bytes, expected %d",
			containerCount, ew.n, headerBaseSize+(containerCount*16))
	}

	// We could compute the expected value, but a SliceContainers can contain
	// a nil *Container, reported by Size() but not returned by the iterator.
	n = int64(ew.n)

	// Container storage section: write each container block.
	citer, _ = b.Containers.Iterator(0)
	for citer.Next() {
		_, c := citer.Value()
		if c.N() > 0 {
			nn, err := c.WriteTo(w)
			n += nn
			if err != nil {
				return n, err
			}
		}
	}
	return n, nil
}

// NewContainerIterator takes a byte slice which is either standard
// roaring or pilosa roaring and returns a ContainerIterator.
func NewContainerIterator(data []byte) (ContainerIterator, error) {
	if len(data) == 0 {
		return nopContainerIterator{}, nil
	}
	ri, err := NewRoaringIterator(data)
	if err != nil {
		return nil, errors.Wrap(err, "getting roaring iterator")
	}
	return &containerIteratorRoaringIteratorWrapper{
		r: ri,
	}, nil
}

// containerIteratorRoaringIteratorWrapper wraps a RoaringIterator to
// make it play like a ContainerIterator.
type containerIteratorRoaringIteratorWrapper struct {
	r        RoaringIterator
	nextKey  uint64
	nextCont *Container
}

func (c *containerIteratorRoaringIteratorWrapper) Next() bool {
	c.nextKey, c.nextCont = c.r.NextContainer()
	return c.nextCont != nil
}

func (c *containerIteratorRoaringIteratorWrapper) Value() (uint64, *Container) {
	return c.nextKey, c.nextCont
}

func (c *containerIteratorRoaringIteratorWrapper) Close() {}

// RoaringIterator represents something which can iterate through a roaring
// bitmap and yield information about containers, including type, size, and
// the location of their data structures.
type RoaringIterator interface {
	// Len reports the number of containers total.
	Len() (count int64)
	// Next yields the information about the next container
	Next() (key uint64, cType byte, n int, length int, pointer *uint16, err error)
	// Remaining yields the bytes left over past the end of the roaring data,
	// which is typically an ops log in our case, and also its offset in case
	// we need to talk about truncation.
	Remaining() ([]byte, int64)

	// NextContainer is a helper that is used in place of Next(). It will
	// allocate a Container from the output of its internal call to Next(),
	// and return the key and container rc. If Next returns an error, then
	// NextContainer will return 0, nil.
	// TODO: have this reuse the *Container?
	NextContainer() (key uint64, rc *Container)

	// Data returns the underlying data, esp for the Ops log.
	Data() []byte

	// Clone copies the iterator, preserving it at this point in the iteration.
	// It may well share much underlying data.
	Clone() RoaringIterator

	// ContainerKeys provides all the
	// container keys that the iterator will return.
	// The current implementation requires that the underlying header
	// lists the keys in ascending order.
	// If there are no keys, then an empty slice will be returned.
	ContainerKeys() (slc []uint64)

	// Skip will move the iterator forward by 1 without
	// materializing the container.
	Skip()
}

// baseRoaringIterator holds values used by both Pilosa and official Roaring
// iterators.
type baseRoaringIterator struct {
	data              []byte
	keys              int64
	headers           []byte
	offsets           []byte
	currentKey        uint64
	currentIdx        int64
	currentType       byte
	currentN          int
	currentLen        int
	currentPointer    *uint16
	currentDataOffset uint64
	prevOffset32      uint32
	chunkOffset       uint64
	lastDataOffset    int64
	lastErr           error
}

// okay, then
func (b *baseRoaringIterator) SilenceLint() {
	// these are actually used in pilosaRoaringIterator or officialRoaringIterator
	// but structcheck doesn't know that
	_ = b.data
	_ = b.keys
	_ = b.offsets
	_ = b.headers
	_ = b.currentIdx
	_ = b.chunkOffset
	_ = b.prevOffset32
}

func (b *pilosaRoaringIterator) Clone() (clone RoaringIterator) {
	cp := *b
	return &cp
}

func (b *officialRoaringIterator) Clone() (clone RoaringIterator) {
	cp := *b
	return &cp
}

type pilosaRoaringIterator struct {
	baseRoaringIterator
}

type officialRoaringIterator struct {
	baseRoaringIterator
	containerTyper func(index uint, card int) byte
	haveRuns       bool
}

func newOfficialRoaringIterator(data []byte) (*officialRoaringIterator, error) {
	r := &officialRoaringIterator{}
	r.data = data

	// share code with the existing unmarshal code
	var offsetOffset, headerOffset int
	var err error
	var keys uint32

	// we ignore the flags, since we don't have to process them for anything.
	keys, r.containerTyper, headerOffset, offsetOffset, r.haveRuns, err = readOfficialHeader(data)
	if err != nil {
		return nil, fmt.Errorf("reading official header: %v", err)
	}
	if keys == 0 {
		// not an error, exactly. it's valid and well-formed, we just have nothing to do
		r.Done(io.EOF)
		return r, nil
	}
	r.keys = int64(keys)
	r.headers = data[headerOffset:offsetOffset]
	// note: offsets are only actually used with the no-run headers.
	if r.haveRuns {
		r.currentDataOffset = uint64(offsetOffset)
	} else {
		if len(r.data) < offsetOffset+int(r.keys*4) {
			return nil, fmt.Errorf("insufficient data for offsets (need %d bytes, found %d)",
				r.keys*4, len(r.data)-offsetOffset)
		}
		r.offsets = data[offsetOffset : offsetOffset+int(r.keys*4)]
		r.currentDataOffset = uint64(offsetOffset)
	}
	// set key to -1; user should call Next first.
	r.currentIdx = -1
	r.currentKey = ^uint64(0)
	r.lastErr = errors.New("tried to read iterator without calling Next first")
	return r, nil
}

func newPilosaRoaringIterator(data []byte) (*pilosaRoaringIterator, error) {
	fileVersion := uint32(data[2])
	if fileVersion != storageVersion {
		return nil, fmt.Errorf("wrong roaring version, file is v%d, server requires v%d", fileVersion, storageVersion)
	}
	r := &pilosaRoaringIterator{}
	r.data = data
	// Read key count in bytes sizeof(cookie)+sizeof(flag):(sizeof(cookie)+sizeof(uint32)).
	r.keys = int64(binary.LittleEndian.Uint32(data[3+1 : 8]))
	// it could happen
	if r.keys == 0 {
		// special case: what if we have zero containers, but a valid ops log after them?
		// set currentDataOffset so that Done will set lastDataOffset and Remaining() will
		// work.
		if len(data) > headerBaseSize {
			r.currentDataOffset = headerBaseSize
		}
		// not an error, exactly. it's valid and well-formed, we just have nothing to do
		r.Done(io.EOF)
		return r, nil
	}
	if int64(len(data)) < int64(headerBaseSize+(r.keys*16)) {
		return nil, fmt.Errorf("insufficient data for header + offsets: want %d bytes, got %d",
			headerBaseSize+(r.keys*16), len(data))
	}

	headerStart := int64(headerBaseSize)
	headerEnd := headerStart + (r.keys * 12)
	offsetStart := headerEnd
	offsetEnd := offsetStart + (r.keys * 4)
	r.headers = data[headerStart:headerEnd]
	r.offsets = data[offsetStart:offsetEnd]
	// if there's no containers, we want to act as though data started at the end
	// of the list of offsets, which was also empty, so we don't think the entire thing
	// is actually a malformed op
	r.prevOffset32 = uint32(offsetEnd)
	r.currentDataOffset = uint64(offsetEnd)
	// it's possible that there's so many headers that we're actually over
	// 4GB into the file already.
	r.chunkOffset = r.currentDataOffset &^ ((1 << 32) - 1)
	// set key to -1; user should call Next first.
	r.currentIdx = -1
	r.currentKey = ^uint64(0)
	r.lastErr = errors.New("tried to read iterator without calling Next first")
	return r, nil
}

func NewRoaringIterator(data []byte) (RoaringIterator, error) {
	if len(data) < headerBaseSize {
		return nil, errors.New("invalid data: not long enough to be a roaring header")
	}
	// Verify the first two bytes are a valid MagicNumber, and second two bytes match current storageVersion.
	fileMagic := uint32(binary.LittleEndian.Uint16(data[0:2]))
	switch fileMagic {
	case serialCookie, serialCookieNoRunContainer:
		return newOfficialRoaringIterator(data)
	case MagicNumber:
		return newPilosaRoaringIterator(data)
	}
	return nil, fmt.Errorf("unknown roaring magic number %d", fileMagic)
}

// Done marks the iterator as complete, recording err as the reason why
func (r *baseRoaringIterator) Done(err error) {
	r.lastErr = err
	r.currentKey = ^uint64(0)
	r.currentType = 0
	r.currentN = 0
	r.currentLen = 0
	r.currentPointer = nil
	r.lastDataOffset = int64(r.currentDataOffset)
	r.currentDataOffset = 0
}

func (r *pilosaRoaringIterator) ContainerKeys() (slc []uint64) {
	n := r.keys
	if n == 0 {
		return
	}
	for i := int64(0); i < n; i++ {
		beg := i * 12
		slc = append(slc, binary.LittleEndian.Uint64(r.headers[beg:beg+8]))
	}
	return
}

func (r *officialRoaringIterator) ContainerKeys() (slc []uint64) {
	n := r.keys
	if n == 0 {
		return
	}
	for i := int64(0); i < n; i++ {
		beg := i * 4
		slc = append(slc, uint64(binary.LittleEndian.Uint16(r.headers[beg:beg+2])))
	}
	return
}

func (r *baseRoaringIterator) Skip() {
	if r.currentIdx >= r.keys {
		// we're already done
		return
	}
	r.currentIdx++
	if r.currentIdx == r.keys {
		// this is the last key. transition state to the finalized state
		r.Done(io.EOF)
	}
}

// Len() indicates the total number of containers the iterator expects to have.
func (r *baseRoaringIterator) Len() int64 {
	return r.keys
}

func (r *baseRoaringIterator) Data() []byte {
	return r.data
}

func (r *baseRoaringIterator) Remaining() ([]byte, int64) {
	if r.lastDataOffset == 0 {
		return nil, 0
	}
	return r.data[r.lastDataOffset:], r.lastDataOffset
}

func (r *pilosaRoaringIterator) NextContainer() (key uint64, rc *Container) {
	itrKey, itrCType, itrN, itrLen, itrPointer, itrErr := r.Next()
	if itrErr != nil {
		return 0, nil
	}
	rc = &Container{}
	rc.typeID = itrCType
	rc.n = int32(itrN)
	rc.len = int32(itrLen)
	rc.cap = int32(itrLen)
	rc.pointer = itrPointer
	return itrKey, rc
}

func (r *pilosaRoaringIterator) Next() (key uint64, cType byte, n int, length int, pointer *uint16, err error) {
	if r.currentIdx >= r.keys {
		// we're already done
		return r.Current()
	}
	r.currentIdx++
	if r.currentIdx == r.keys {
		// this is the last key. transition state to the finalized state
		r.Done(io.EOF)
		return r.Current()
	}
	header := r.headers[r.currentIdx*12:]
	r.currentKey = binary.LittleEndian.Uint64(header[0:8])
	r.currentType = byte(binary.LittleEndian.Uint16(header[8:10]))
	r.currentN = int(binary.LittleEndian.Uint16(header[10:12])) + 1
	offset32 := binary.LittleEndian.Uint32(r.offsets[r.currentIdx*4:])
	if offset32 < r.prevOffset32 {
		r.chunkOffset += (1 << 32)
	}
	r.prevOffset32 = offset32
	r.currentDataOffset = r.chunkOffset + uint64(offset32)

	// a run container keeps its data after an initial 2 byte length header
	var runCount uint16
	if r.currentType == ContainerRun {
		runCount = binary.LittleEndian.Uint16(r.data[r.currentDataOffset : r.currentDataOffset+runCountHeaderSize])
		r.currentDataOffset += 2
	}
	if r.currentDataOffset > uint64(len(r.data)) || r.currentDataOffset < headerBaseSize {
		r.Done(fmt.Errorf("container %d/%d, key %d, had offset %d, maximum %d",
			r.currentIdx, r.keys, r.currentKey, r.currentDataOffset, len(r.data)))
		return r.Current()
	}
	r.currentPointer = (*uint16)(unsafe.Pointer(&r.data[r.currentDataOffset]))
	var size int
	switch r.currentType {
	case ContainerArray:
		r.currentLen = r.currentN
		size = r.currentLen * 2
	case ContainerBitmap:
		r.currentLen = 1024
		size = 8192
	case ContainerRun:
		r.currentLen = int(runCount)
		size = r.currentLen * 4
	}
	if int64(r.currentDataOffset)+int64(size) > int64(len(r.data)) {
		r.Done(fmt.Errorf("container %d/%d, key %d, had offset %d+%d size, maximum %d",
			r.currentIdx, r.keys, r.currentKey, r.currentDataOffset, size, len(r.data)))
		return r.Current()
	}
	r.currentDataOffset += uint64(size)
	r.lastErr = nil
	return r.Current()
}

func (r *officialRoaringIterator) NextContainer() (key uint64, rc *Container) {
	itrKey, itrCType, itrN, itrLen, itrPointer, itrErr := r.Next()
	if itrErr != nil {
		return 0, nil
	}
	rc = &Container{}
	rc.typeID = itrCType
	rc.n = int32(itrN)
	rc.len = int32(itrLen)
	rc.cap = int32(itrLen)
	rc.pointer = itrPointer
	return itrKey, rc
}

func (r *officialRoaringIterator) Next() (key uint64, cType byte, n int, length int, pointer *uint16, err error) {
	if r.currentIdx >= r.keys {
		// we're already done
		return r.Current()
	}
	r.currentIdx++
	if r.currentIdx == r.keys {
		// this is the last key. transition state to the finalized state
		r.Done(io.EOF)
		return r.Current()
	}
	header := r.headers[r.currentIdx*4:]
	r.currentKey = uint64(binary.LittleEndian.Uint16(header[0:2]))
	r.currentN = int(binary.LittleEndian.Uint16(header[2:4])) + 1
	r.currentType = r.containerTyper(uint(r.currentIdx), r.currentN)
	// with runs, we can't actually look up offsets; the format just stores
	// things sequentially. so we have to actually track the offset in that case.
	if !r.haveRuns {
		r.currentDataOffset = uint64(binary.LittleEndian.Uint32(r.offsets[r.currentIdx*4:]))
	}
	// a run container keeps its data after an initial 2 byte length header
	var runCount uint16
	if r.currentType == ContainerRun {
		if int(r.currentDataOffset)+2 > len(r.data) {
			r.Done(fmt.Errorf("insufficient data for offsets container %d/%d, expect run length at %d/%d bytes",
				r.currentIdx, r.keys, r.currentDataOffset, len(r.data)))
			return r.Current()
		}
		runCount = binary.LittleEndian.Uint16(r.data[r.currentDataOffset : r.currentDataOffset+runCountHeaderSize])
		r.currentDataOffset += 2
	}
	if r.currentDataOffset > uint64(len(r.data)) || r.currentDataOffset < headerBaseSize {
		r.Done(fmt.Errorf("container %d/%d, key %d, had offset %d, maximum %d",
			r.currentIdx, r.keys, r.currentKey, r.currentDataOffset, len(r.data)))
		return r.Current()
	}
	r.currentPointer = (*uint16)(unsafe.Pointer(&r.data[r.currentDataOffset]))
	var size int
	switch r.currentType {
	case ContainerArray:
		r.currentLen = r.currentN
		size = r.currentLen * 2
	case ContainerBitmap:
		r.currentLen = 1024
		size = 8192
	case ContainerRun:
		// official format stores runs as start/len, we want to convert, but since
		// they might be mmapped, we can't write to that memory
		newRuns := make([]Interval16, runCount)
		oldRuns := (*[65536]Interval16)(unsafe.Pointer(r.currentPointer))[:runCount:runCount]
		copy(newRuns, oldRuns)
		for i := range newRuns {
			newRuns[i].Last += newRuns[i].Start
		}
		r.currentPointer = (*uint16)(unsafe.Pointer(&newRuns[0]))
		r.currentLen = int(runCount)
		size = r.currentLen * 4
	}
	if int64(r.currentDataOffset)+int64(size) > int64(len(r.data)) {
		r.Done(fmt.Errorf("container %d/%d, key %d, had offset %d+%d size, maximum %d",
			r.currentIdx, r.keys, r.currentKey, r.currentDataOffset, size, len(r.data)))
		return r.Current()
	}
	r.currentDataOffset += uint64(size)
	r.lastErr = nil
	return r.Current()
}

func (r *baseRoaringIterator) Current() (key uint64, cType byte, n int, length int, pointer *uint16, err error) {
	return r.currentKey, r.currentType, r.currentN, r.currentLen, r.currentPointer, r.lastErr
}

// SanityCheckMapping is a debugging function which checks whether containers
// are *correctly* recorded as mapped or unmapped.
func (b *Bitmap) SanityCheckMapping(from, to uintptr) (mappedIn int64, mappedOut int64, unmappedIn int64, errs int, err error) {
	b.Containers.UpdateEvery(func(key uint64, c *Container, existed bool) (*Container, bool) {
		dptr := uintptr(unsafe.Pointer(c.pointer))
		if dptr >= from && dptr < to {
			if c.Mapped() {
				mappedIn++
			} else {
				err = fmt.Errorf("container key %d, addr %x, inside %x+%d",
					key, dptr, from, to-from)
				errs++
				unmappedIn++
			}
		} else {
			if c.Mapped() {
				err = fmt.Errorf("container key %d, addr %x, outside %x+%d, but mapped",
					key, dptr, from, to-from)
				errs++
				mappedOut++
			}
		}
		return c, false
	})
	return mappedIn, mappedOut, unmappedIn, errs, err
}

// RemapRoaringStorage tries to update all containers to refer to
// the roaring bitmap in the provided []byte. If any containers are
// marked as mapped, but do not match the provided storage, they will
// be unmapped. The boolean return indicates whether or not any
// containers were mapped to the given storage.
//
// Regardless, after this function runs, no containers have
// mapped storage which does not refer to data; either they got mapped
// to the new storage, or storage was allocated for them.
func (b *Bitmap) RemapRoaringStorage(data []byte) (mappedAny bool, returnErr error) {
	if b.Containers == nil {
		return false, nil
	}
	var itr RoaringIterator
	var err error
	var itrKey uint64
	var itrCType byte
	var itrN int
	var itrPointer *uint16
	var itrErr error

	// If we got no data, we don't want to do the actual mapping, just
	// the unmapping. If preferMapping is false, we also don't want to
	// map to the data. We still need to do the UpdateEvery loop, we
	// just won't have an iterator for it.
	if data != nil && b.preferMapping {
		itr, err = NewRoaringIterator(data)
	}
	// don't return early: we still have to do the unmapping
	if err != nil {
		returnErr = err
	}

	if itr != nil {
		itrKey, itrCType, itrN, _, itrPointer, itrErr = itr.Next()
	}
	if itrErr != nil {
		// iterator errored out, so we won't check it in the loop below
		itr = nil
	}

	b.Containers.UpdateEvery(func(key uint64, oldC *Container, existed bool) (newC *Container, write bool) {
		if itr != nil {
			for itrKey < key && itrErr == nil {
				itrKey, itrCType, itrN, _, itrPointer, itrErr = itr.Next()
			}
			if itrErr != nil {
				itr = nil
			}
			// container might be similar enough that we should trust it:
			if itrKey == key && itrCType == oldC.typ() && itrN == int(oldC.N()) {
				if oldC.frozen() {
					// we don't use Clone, because that would copy the
					// storage, and we don't need that.
					halfCopy := *oldC
					halfCopy.flags &^= flagFrozen
					newC = &halfCopy
				} else {
					newC = oldC
				}
				mappedAny = true
				newC.pointer = itrPointer
				newC.flags |= flagMapped
				return newC, true
			}
		}
		// if the container isn't mapped, we don't need to do anything
		if !oldC.Mapped() {
			return oldC, false
		}
		// forcibly unmap it, so the old mapping can be unmapped safely.
		newC = oldC.unmapOrClone()
		return newC, true
	})
	return mappedAny, returnErr
}

// ImportRoaringBits sets-or-clears bits based on a provided Roaring bitmap.
// This should be equivalent to unmarshalling the bitmap, then executing
// either `b = Union(b, newB)` or `b = Difference(b, newB)`, but with lower
// overhead. The log parameter controls whether to write to the op log; the
// answer should always be yes, except if you're calling using this to apply
// the op log.
//
// If rowSize is non-zero, we should return a map of rows we altered,
// where "rows" are sets of rowSize containers. Otherwise the map isn't used.
// (This allows ImportRoaring to update caches; see fragment.go.)
func (b *Bitmap) ImportRoaringBits(data []byte, clear bool, log bool, rowSize uint64) (changed int, rowSet map[uint64]int, err error) {
	if data == nil {
		return 0, nil, errors.New("no roaring bitmap provided")
	}
	var itr RoaringIterator

	itr, err = NewRoaringIterator(data)
	if err != nil {
		return 0, nil, err
	}
	return b.ImportRoaringRawIterator(itr, clear, log, rowSize)
}

// MergeRoaringRawIteratorIntoExists is a special merge iterator that flattens the results onto a
// single row which is used for determining existence. All row references are removed and only the column
// is considered.
func (b *Bitmap) MergeRoaringRawIteratorIntoExists(itr RoaringIterator, rowSize uint64) error {

	if itr == nil {
		return errors.New("nil RoaringIterator passed to MergeRoaringRawIteratorIntoExists")
	}
	var synthC Container
	importUpdater := func(oldC *Container, existed bool) (newC *Container, write bool) {
		existN := oldC.N()
		if existN == MaxContainerVal+1 {
			return oldC, false
		}
		if existN == 0 {
			newerC := synthC.Clone()
			return newerC, true
		}
		newC = oldC.unionInPlace(&synthC)
		if newC.typeID == ContainerBitmap {
			newC.Repair()
		}
		if newC.N() != existN {
			return newC, true
		}
		return oldC, false
	}
	itrKey, itrCType, itrN, itrLen, itrPointer, itrErr := itr.Next()
	for itrErr == nil {
		synthC.typeID = itrCType
		synthC.n = int32(itrN)
		synthC.len = int32(itrLen)
		synthC.cap = int32(itrLen)
		synthC.pointer = itrPointer
		b.Containers.Update(itrKey%rowSize, importUpdater)
		itrKey, itrCType, itrN, itrLen, itrPointer, itrErr = itr.Next()
	}
	// note: if we get a non-EOF err, it's possible that we made SOME
	// changes but didn't log them. I don't have a good solution to this.
	if itrErr != io.EOF {
		return itrErr
	}
	return nil
}

func (b *Bitmap) ImportRoaringRawIterator(itr RoaringIterator, clear bool, log bool, rowSize uint64) (changed int, rowSet map[uint64]int, err error) {
	var itrKey uint64
	var itrCType byte
	var itrN int
	var itrLen int
	var itrPointer *uint16
	var itrErr error

	if itr == nil {
		return 0, nil, errors.New("failed to create roaring iterator, but don't know why")
	}

	rowSet = make(map[uint64]int)

	var synthC Container
	var importUpdater func(*Container, bool) (*Container, bool)
	var currRow uint64
	if clear {
		importUpdater = func(oldC *Container, existed bool) (newC *Container, write bool) {
			existN := oldC.N()
			if existN == 0 || !existed {
				return nil, false
			}
			newC = difference(oldC, &synthC)
			if newC.N() != existN {
				changes := int(existN - newC.N())
				changed += changes
				rowSet[currRow] -= changes
				return newC, true
			}
			return oldC, false
		}
	} else {
		importUpdater = func(oldC *Container, existed bool) (newC *Container, write bool) {
			existN := oldC.N()
			if existN == MaxContainerVal+1 {
				return oldC, false
			}
			if existN == 0 {
				newerC := synthC.Clone()
				changed += int(newerC.N())
				rowSet[currRow] += int(newerC.N())
				return newerC, true
			}
			newC = oldC.unionInPlace(&synthC)
			if newC.typeID == ContainerBitmap {
				newC.Repair()
			}
			if newC.N() != existN {
				changes := int(newC.N() - existN)
				changed += changes
				rowSet[currRow] += changes
				return newC, true
			}
			return oldC, false
		}
	}
	itrKey, itrCType, itrN, itrLen, itrPointer, itrErr = itr.Next()
	for itrErr == nil {
		synthC.typeID = itrCType
		synthC.n = int32(itrN)
		synthC.len = int32(itrLen)
		synthC.cap = int32(itrLen)
		synthC.pointer = itrPointer
		if rowSize != 0 {
			currRow = itrKey / rowSize
		}
		b.Containers.Update(itrKey, importUpdater)
		itrKey, itrCType, itrN, itrLen, itrPointer, itrErr = itr.Next()
	}
	// note: if we get a non-EOF err, it's possible that we made SOME
	// changes but didn't log them. I don't have a good solution to this.
	if itrErr != io.EOF {
		return changed, rowSet, itrErr
	}
	err = nil
	if log && changed > 0 {
		o := op{opN: changed, roaring: itr.Data()}
		if clear {
			o.typ = opTypeRemoveRoaring
		} else {
			o.typ = opTypeAddRoaring
		}
		err = b.writeOp(&o)
	}
	return changed, rowSet, err
}

func (b *Bitmap) PreferMapping(preferred bool) {
	b.preferMapping = preferred
}

// writeOp writes op to the OpWriter, if available.
func (b *Bitmap) writeOp(op *op) error {
	if b.OpWriter == nil {
		return nil
	}

	if _, err := op.WriteTo(b.OpWriter); err != nil {
		return err
	}
	b.opN += op.count()
	b.ops++

	return nil
}

// Iterator returns a new iterator for the bitmap.
func (b *Bitmap) Iterator() *Iterator {
	itr := &Iterator{bitmap: b}
	itr.Seek(0)
	return itr
}

func (b *Bitmap) IteratorAt(start uint64) *Iterator {
	itr := &Iterator{bitmap: b}
	itr.Seek(start)
	return itr
}

// Ops returns the number of write ops the bitmap is aware of in its ops
// log, and their total bit count.
func (b *Bitmap) Ops() (ops int, opN int) {
	return b.ops, b.opN
}

// SetOps lets us reset the operation count in the weird case where we know
// we've changed an underlying file, without actually refreshing the bitmap.
func (b *Bitmap) SetOps(ops int, opN int) {
	b.ops, b.opN = ops, opN
}

// RoaringToBitmaps yields a series of bitmaps with specified shard
// keys, based on a single roaring file, with splits at multiples of
// shardWidth, which should be a multiple of container size.
func RoaringToBitmaps(data []byte, shardWidth uint64) ([]*Bitmap, []uint64) {
	if data == nil {
		return nil, nil
	}
	var itr RoaringIterator
	var itrKey uint64
	var itrCType byte
	var itrN int
	var itrLen int
	var itrPointer *uint16
	var itrErr error
	currentShard := ^uint64(0)
	var currentBitmap *Bitmap
	var bitmaps []*Bitmap
	var shards []uint64
	keysPerShard := shardWidth >> 16

	itr, err := NewRoaringIterator(data)
	if err != nil || itr == nil {
		return nil, nil
	}

	itrKey, itrCType, itrN, itrLen, itrPointer, itrErr = itr.Next()
	for itrErr == nil {
		newC := &Container{
			typeID:  itrCType,
			n:       int32(itrN),
			len:     int32(itrLen),
			cap:     int32(itrLen),
			pointer: itrPointer,
			flags:   flagMapped,
		}
		shard := itrKey / keysPerShard
		if shard != currentShard {
			if currentBitmap != nil {
				bitmaps = append(bitmaps, currentBitmap)
				shards = append(shards, currentShard)
			}
			currentBitmap = NewFileBitmap()
			currentShard = shard
		}
		currentBitmap.Containers.Put(itrKey, newC)
		itrKey, itrCType, itrN, itrLen, itrPointer, itrErr = itr.Next()
	}
	if currentBitmap != nil {
		bitmaps = append(bitmaps, currentBitmap)
		shards = append(shards, currentShard)
	}
	// we don't support ops logs for this
	return bitmaps, shards
}

// BitmapsToRoaring renders a series of non-overlapping bitmaps as a
// unified roaring file.
func BitmapsToRoaring(bitmaps []*Bitmap) []byte {
	count := int64(0)
	size := int64(0)
	for i, bm := range bitmaps {
		c, s := bm.roaringSize()
		// skip this bitmap during the next pass, since it's empty
		if c == 0 {
			bitmaps[i] = nil
			continue
		}
		count += c
		size += s
	}
	if count == 0 {
		return nil
	}
	// we have count headers, which need 12 bytes, plus a magic number,
	// plus offsets (4 bytes per container), plus size bytes of data to
	// write.
	out := make([]byte, headerBaseSize+(12*count)+(4*count)+size)
	binary.LittleEndian.PutUint16(out[0:2], uint16(MagicNumber))
	out[3] = byte(storageVersion)
	binary.LittleEndian.PutUint32(out[4:8], uint32(count))
	headerEnd := 8 + (12 * count)
	offsetEnd := headerEnd + (4 * count)
	headers := out[8:headerEnd]
	offsets := out[headerEnd:offsetEnd]
	data := out[offsetEnd:]
	headerOffset := 0
	offsetOffset := 0
	dataOffset := 0
	prevKey := uint64(0)
	for _, bm := range bitmaps {
		if bm == nil {
			continue
		}
		citer, _ := bm.Containers.Iterator(0)
		for citer.Next() {
			k, c := citer.Value()
			n := c.N()
			if n == 0 {
				continue
			}
			if roaringParanoia {
				if k < prevKey {
					panic("unsorted keys in multiple-bitmap roaring conversion")
				}
			}
			// place header at header offset, and data at data
			// offset
			header := headers[headerOffset : headerOffset+12]
			offset := offsets[offsetOffset : offsetOffset+4]
			headerOffset += 12
			offsetOffset += 4
			binary.LittleEndian.PutUint64(header[0:8], k)
			binary.LittleEndian.PutUint16(header[8:10], uint16(c.typeID))
			binary.LittleEndian.PutUint16(header[10:12], uint16(n-1))
			binary.LittleEndian.PutUint32(offset[0:4], uint32(dataOffset+int(offsetEnd)))
			nextData := data[dataOffset:]
			switch c.typeID { // TODO: make this work on big endian machines
			case ContainerArray:
				dataOffset += 2 * copy((*[1 << 16]uint16)(unsafe.Pointer(&nextData[0]))[:c.n:c.n], c.array())
			case ContainerBitmap:
				copy((*[1024]uint64)(unsafe.Pointer(&nextData[0]))[:], c.bitmap())
				dataOffset += 8192
			case ContainerRun:
				binary.LittleEndian.PutUint16(nextData[0:2], uint16(c.len))
				dataOffset += 2
				dataOffset += 4 * copy((*[1 << 15]Interval16)(unsafe.Pointer(&nextData[2]))[:c.len:c.len], c.runs())
			}
		}
	}
	return out
}

// roaringSize yields the count of non-empty containers, and the size
// of the storage *only* -- not the headers.
func (b *Bitmap) roaringSize() (int64, int64) {
	count := int64(0)
	size := int64(0)
	citer, _ := b.Containers.Iterator(0)
	for citer.Next() {
		_, c := citer.Value()
		if c.N() == 0 {
			continue
		}
		count++
		switch c.typeID {
		case ContainerArray:
			size += 2 * int64(c.N())
		case ContainerBitmap:
			size += 8192
		case ContainerRun:
			// 2 bytes for the count of runs, plus 4 bytes per run
			size += 2 + (4 * int64(c.len))
		}
	}
	return count, size
}

// Info returns stats for the bitmap.
func (b *Bitmap) Info(includeContainers bool) BitmapInfo {
	info := BitmapInfo{
		OpN:            b.opN,
		Ops:            b.ops,
		ContainerCount: b.Containers.Size(),
	}
	if includeContainers {
		info.Containers = make([]ContainerInfo, 0, info.ContainerCount)
	}
	citer, _ := b.Containers.Iterator(0)
	for citer.Next() {
		k, c := citer.Value()
		ci := c.info()
		ci.Key = k
		info.BitCount += uint64(c.N())
		if includeContainers {
			info.Containers = append(info.Containers, ci)
		}
	}
	return info
}

// Check performs a consistency check on the bitmap. Returns nil if consistent.
func (b *Bitmap) Check() error {
	var a ErrorList

	// Check each container.
	citer, _ := b.Containers.Iterator(0)
	for citer.Next() {
		k, c := citer.Value()
		if err := c.check(); err != nil {
			a.AppendWithPrefix(err, fmt.Sprintf("%d/", k))
		}
	}

	if len(a) == 0 {
		return nil
	}
	return a
}

// Flip performs a logical negate of the bits in the range [start,end].
func (b *Bitmap) Flip(start, end uint64) *Bitmap {
	if roaringSentinel {
		if start > end {
			panic(fmt.Sprintf("flipping in range but %v > %v", start, end))
		}
	}
	result := NewBitmap()
	itr := b.Iterator()
	v, eof := itr.Next()
	//copy over previous bits.
	for v < start && !eof {
		result.DirectAdd(v)
		v, eof = itr.Next()
	}
	//flip bits in range .
	for i := start; i <= end; i++ {
		if eof {
			result.DirectAdd(i)
		} else if v == i {
			v, eof = itr.Next()
		} else {
			result.DirectAdd(i)
		}
	}
	//add remaining.
	for !eof {
		result.DirectAdd(v)
		v, eof = itr.Next()
	}
	return result
}

// BitmapInfo represents a point-in-time snapshot of bitmap stats.
type BitmapInfo struct {
	OpN            int
	Ops            int
	OpDetails      []OpInfo `json:"OpDetails,omitempty"`
	BitCount       uint64
	ContainerCount int
	Containers     []ContainerInfo `json:"Containers,omitempty"`   // The containers found in the bitmap originally
	OpContainers   []ContainerInfo `json:"OpContainers,omitempty"` // The containers resulting from ops log changes.
	From, To       uintptr         // if set, indicates the address range used when unpacking
}

// Iterator represents an iterator over a Bitmap.
type Iterator struct {
	bitmap *Bitmap
	citer  ContainerIterator
	key    uint64
	c      *Container
	j, k   int32 // i: container; j: array index, bit index, or run index; k: offset within the run
}

// This exists because we used to support a backend which needed it, and I
// don't want to re-experience the joy of figuring out where close calls are needed.
func (itr *Iterator) Close() {}

// Seek moves to the first value equal to or greater than `seek`.
func (itr *Iterator) Seek(seek uint64) {
	// k should always be -1 unless we're seeking into a run container. Then the
	// "if c.isRun" section will take care of it.
	itr.k = -1

	// Move to the correct container.
	itr.citer, _ = itr.bitmap.Containers.Iterator(highbits(seek))
	if !itr.citer.Next() {
		itr.c = nil
		return // eof
	}
	itr.key, itr.c = itr.citer.Value()
	if roaringParanoia {
		if itr.c == nil {
			panic("seeking iterator got a nil container when Next() was true")
		}
	}

	// Move to the correct value index inside the container.
	lb, hb := lowbits(seek), highbits(seek)
	if itr.c.isArray() {
		// Seek is smaller than min(itr.c).
		if itr.key > hb {
			itr.j = -1
			return
		}

		// Find index in the container.
		itr.j = search32(itr.c.array(), lb)
		if itr.j < 0 {
			itr.j = -itr.j - 1
		}
		if itr.j < int32(len(itr.c.array())) {
			itr.j--
			return
		}

		// If it's at the end of the container then move to the next one.
		if !itr.citer.Next() {
			itr.c = nil
			return
		}
		itr.key, itr.c = itr.citer.Value()
		itr.j = -1
		return
	}

	if itr.c.isRun() {
		// Seek is smaller than min(itr.c).
		if itr.key > hb {
			itr.j = 0
			itr.k = -1
			return
		}

		j, contains := BinSearchRuns(lb, itr.c.runs())
		if contains {
			itr.j = j
			itr.k = int32(lb) - int32(itr.c.runs()[j].Start) - 1
			return
		}
		// If seek is larger than all elements, return.
		if j >= int32(len(itr.c.runs())) {
			if !itr.citer.Next() {
				itr.c = nil
				return
			}
			itr.key, itr.c = itr.citer.Value()
			itr.j = -1
			return
		}
		// Set iterator to next value in the Bitmap.
		itr.j = j
		itr.k = -1
		return
	}

	// If it's a bitmap container then move to index before the value.
	if itr.key > hb {
		itr.j = -1
		return
	}
	itr.j = int32(lb) - 1
}

// Next returns the next value in the bitmap.
// Returns eof as true if there are no values left in the iterator.
func (itr *Iterator) Next() (v uint64, eof bool) {
	if itr.c == nil {
		return 0, true
	}
	// Iterate over containers until we find the next value or EOF.
	for {
		if itr.c.isArray() {
			if itr.j >= itr.c.N()-1 {
				// Reached end of array, move to the next container.
				if !itr.citer.Next() {
					itr.c = nil
					return 0, true
				}
				itr.key, itr.c = itr.citer.Value()
				itr.j = -1
				continue
			}
			itr.j++
			return itr.peek(), false
		}

		if itr.c.isRun() {
			// Because itr.j for an array container defaults to -1
			// but defaults to 0 for a run container, we need to
			// standardize on treating -1 as our default value for itr.j.
			// Note that this is easier than changing the default to 0
			// because the array logic uses the negative number space
			// to represent offsets to an array position that isn't filled
			// (-1 being the first empty space in an array, or 0).
			if itr.j == -1 {
				itr.j++
			}

			// If the container is empty, move to the next container.
			if len(itr.c.runs()) == 0 {
				if !itr.citer.Next() {
					itr.c = nil
					return 0, true
				}
				itr.key, itr.c = itr.citer.Value()
				itr.j = -1
				continue
			}

			r := itr.c.runs()[itr.j]
			runLength := int32(r.Last - r.Start)

			if itr.k >= runLength {
				// Reached end of run, move to the next run.
				itr.j, itr.k = itr.j+1, -1
			}

			if itr.j >= int32(len(itr.c.runs())) {
				// Reached end of runs, move to the next container.
				if !itr.citer.Next() {
					itr.c = nil
					return 0, true
				}
				itr.key, itr.c = itr.citer.Value()
				itr.j = -1
				continue
			}

			itr.k++
			return itr.peek(), false
		}

		// Move to the next possible index in the bitmap container.
		itr.j++

		// Find first non-zero bit in current bitmap, if possible.
		hb := itr.j >> 6

		if hb >= int32(len(itr.c.bitmap())) {
			if !itr.citer.Next() {
				itr.c = nil
				return 0, true
			}
			itr.key, itr.c = itr.citer.Value()
			itr.j = -1
			continue
		}
		lb := itr.c.bitmap()[hb] >> (uint(itr.j) % 64)
		if lb != 0 {
			itr.j = itr.j + int32(trailingZeroN(lb))
			return itr.peek(), false
		}

		// Otherwise iterate through remaining bitmaps to find next bit.
		for hb++; hb < int32(len(itr.c.bitmap())); hb++ {
			if itr.c.bitmap()[hb] != 0 {
				itr.j = hb<<6 + int32(trailingZeroN(itr.c.bitmap()[hb]))
				return itr.peek(), false
			}
		}

		// If no bits found then move to the next container.
		if !itr.citer.Next() {
			itr.c = nil
			return 0, true
		}
		itr.key, itr.c = itr.citer.Value()
		itr.j = -1
	}
}

// peek returns the current value.
func (itr *Iterator) peek() uint64 {
	if itr.c == nil {
		return 0
	}
	if itr.c.isArray() {
		return itr.key<<16 | uint64(itr.c.array()[itr.j])
	}
	if itr.c.isRun() {
		return itr.key<<16 | uint64(itr.c.runs()[itr.j].Start+uint16(itr.k))
	}
	return itr.key<<16 | uint64(itr.j)
}

// ArrayMaxSize represents the maximum size of array containers.
const ArrayMaxSize = 4096

// runMaxSize represents the maximum size of run length encoded containers.
const runMaxSize = 2048

type Interval16 struct {
	Start uint16
	Last  uint16
}

// runlen returns the count of integers in the interval.
func (iv Interval16) runlen() int32 {
	return 1 + int32(iv.Last-iv.Start)
}

// count counts all bits in the container.
func (c *Container) count() (n int32) {
	return c.countRange(0, MaxContainerVal+1)
}

// countRange counts the number of bits set between [start, end).
func (c *Container) countRange(start, end int32) (n int32) {
	if roaringParanoia {
		if start > end {
			panic(fmt.Sprintf("counting in range but %v > %v", start, end))
		}
	}
	if c == nil {
		return 0
	}
	if c.isArray() {
		return ArrayCountRange(c.array(), start, end)
	} else if c.isRun() {
		return RunCountRange(c.runs(), start, end)
	}
	return BitmapCountRange(c.bitmap(), start, end)
}

func ArrayCountRange(array []uint16, start, end int32) (n int32) {
	if roaringParanoia {
		if start > end {
			panic(fmt.Sprintf("counting in range but %v > %v", start, end))
		}
	}
	i := int32(sort.Search(len(array), func(i int) bool { return int32(array[i]) >= start }))
	for ; i < int32(len(array)); i++ {
		v := int32(array[i])
		if v >= end {
			break
		}
		n++
	}
	return n
}

// BitmapCountRange counts bits set in [start,end).
func BitmapCountRange(bitmap []uint64, start, end int32) int32 {
	if roaringParanoia {
		if start > end {
			panic(fmt.Sprintf("counting in range but %v > %v", start, end))
		}
	}
	var n uint64
	i, j := start/64, end/64
	// Special case when start and end fall in the same word.
	if i == j {
		offi, offj := uint(start%64), uint(64-end%64)
		n += popcount((bitmap[i] >> offi) << (offj + offi))
		return int32(n)
	}

	// Count partial starting word.
	if off := uint(start) % 64; off != 0 {
		n += popcount(bitmap[i] >> off)
		i++
	}

	// Count words in between.
	for ; i < j; i++ {
		n += popcount(bitmap[i])
	}

	// Count partial ending word.
	if j < int32(len(bitmap)) {
		off := 64 - (uint(end) % 64)
		n += popcount(bitmap[j] << off)
	}

	return int32(n)
}

func callbackBits(w uint64, base uint16, fn func(uint16)) {
	for w != 0 {
		trail := uint16(bits.TrailingZeros64(w))
		fn(base + trail)
		base += trail + 1
		w >>= trail + 1
	}
}

// bitmapCallbackRange calls the provided function for every bit set in
// bitmap in the range [start,end).
func bitmapCallbackRange(bitmap []uint64, start, end int32, fn func(uint16)) {
	if roaringParanoia {
		if start > end {
			panic(fmt.Sprintf("counting in range but %v > %v", start, end))
		}
	}
	i, j := start/64, end/64
	// Special case when start and end fall in the same word.
	if i == j {
		// So, we want to know the offsets. For instance, if start and end
		// are 65 and 69, we might want i=1, offi=1, j=1, offj=5. Then we
		// compute masks from offi (masking out 0x1, or (1<<offi)-1), and
		// offj (masking out everything *but* (1<<offj)-1).
		//
		// But we can also use "x >> offi << offi" to trim the lowest offi
		// bits, and "x << (64-offj) >> (64-offj)" to trim all but the
		// lowest offj bits.
		//
		// We can then simplify slightly further: we use the inverted value
		// as offj, and compute (w << offi) >> (offi + offj) << offi.
		//
		// But wait, you ask. What if offi+offj is too large! Well, then
		// start and end were in the wrong order. We have 0 <= i <= j < 64.
		// If x+i > 64, then x > (64-i). Thus, if (64-j)+i > 64, it
		// follows that (64-j) > (64-i). So they'd have been in the wrong order.
		// In which case, we correctly yield a value of (0 << offi), or 0,
		// because nothing is between them.
		offi, offj := uint(start%64), uint(64-(end%64))
		w := (bitmap[i] << offj) >> (offi + offj) << offi
		if w != 0 {
			callbackBits(w, uint16(i)*64, fn)
		}
		return
	}

	// Count partial starting word.
	if off := uint(start) % 64; off != 0 {
		w := (bitmap[i] >> off) << off
		if w != 0 {
			callbackBits(w, (uint16(i) * 64), fn)
		}
		i++
	}

	// Count words in between.
	for ; i < j; i++ {
		if bitmap[i] != 0 {
			callbackBits(bitmap[i], uint16(i)*64, fn)
		}
	}

	// Count partial ending word.
	if j < int32(len(bitmap)) {
		off := 64 - (uint(end) % 64)
		w := (bitmap[j] << off) >> off
		if w != 0 {
			callbackBits(w, uint16(j)*64, fn)
		}
	}
}

// RunCountRange returns the ranged bit count for RLE pairs.
func RunCountRange(runs []Interval16, start, end int32) (n int32) {
	if roaringParanoia {
		if start > end {
			panic(fmt.Sprintf("counting in range but %v > %v", start, end))
		}
	}
	for _, iv := range runs {
		// iv is before range
		if int32(iv.Last) < start {
			continue
		}
		// iv is after range
		if end < int32(iv.Start) {
			break
		}
		// iv is superset of range
		if int32(iv.Start) <= start && int32(iv.Last) >= end {
			return end - start
		}
		// iv is subset of range
		if int32(iv.Start) >= start && int32(iv.Last) <= end {
			n += iv.runlen()
		}
		// iv overlaps beginning of range without being a subset
		if int32(iv.Start) < start && int32(iv.Last) < end {
			n += int32(iv.Last) - start + 1
		}
		// iv overlaps end of range without being a subset
		if int32(iv.Start) > start && int32(iv.Last) >= end {
			n += end - int32(iv.Start)
		}
	}
	return n
}

// add adds a value to the container.
func (c *Container) add(v uint16) (newC *Container, added bool) {
	if c == nil {
		return NewContainerArray([]uint16{v}), true
	}
	if c.isArray() {
		return c.arrayAdd(v)
	} else if c.isRun() {
		return c.runAdd(v)
	} else {
		return c.bitmapAdd(v)
	}
}

func (c *Container) arrayAdd(v uint16) (*Container, bool) {
	// Optimize appending to the end of an array container.
	array := c.array()
	if c.N() > 0 && c.N() < ArrayMaxSize && c.isArray() && array[c.N()-1] < v {
		statsHit("arrayAdd/append")
		c = c.Thaw()
		array = append(c.array(), v)
		c.setArray(array)
		return c, true
	}

	// Find index of the integer in the container. Exit if it already exists.
	i := search32(array, v)
	if i >= 0 {
		return c, false
	}

	// Convert to a bitmap container if too many values are in an array container.
	if c.N() >= ArrayMaxSize {
		statsHit("arrayAdd/arrayToBitmap")
		c = c.arrayToBitmap()
		return c.bitmapAdd(v)
	}

	// Otherwise insert into array.
	statsHit("arrayAdd/insert")
	c = c.Thaw()
	i = -i - 1
	array = append(c.array(), 0)
	copy(array[i+1:], array[i:])
	array[i] = v
	c.setArray(array)
	return c, true

}

func (c *Container) bitmapAdd(v uint16) (*Container, bool) {
	if c == nil {
		c = NewContainerBitmapN(nil, 1)
		c.bitmap()[v/64] |= (1 << uint64(v%64))
		return c, true
	}
	if c.bitmapContains(v) {
		return c, false
	}
	c = c.Thaw()
	c.bitmap()[v/64] |= (1 << uint64(v%64))
	c.setN(c.N() + 1)
	return c, true
}

func (c *Container) runAdd(v uint16) (*Container, bool) {
	runs := c.runs()

	if len(runs) == 0 {
		c = c.Thaw()
		c.setRuns([]Interval16{{Start: v, Last: v}})
		c.setN(1)
		return c, true
	}

	i := sort.Search(len(runs),
		func(i int) bool { return runs[i].Last >= v })

	if i == len(runs) {
		i--
	}

	iv := runs[i]
	if v >= iv.Start && iv.Last >= v {
		return c, false
	}

	c = c.Thaw()
	runs = c.runs()
	if iv.Last < v {
		if iv.Last == v-1 {
			runs[i].Last++
		} else {
			runs = append(runs, Interval16{Start: v, Last: v})
		}
	} else if v+1 == iv.Start {
		// combining two intervals
		if i > 0 && runs[i-1].Last == v-1 {
			runs[i-1].Last = iv.Last
			runs = append(runs[:i], runs[i+1:]...)
			c.setRuns(runs)
			c.setN(c.N() + 1)
			return c, true
		}
		// just before an interval
		runs[i].Start--
	} else if i > 0 && v-1 == runs[i-1].Last {
		// just after an interval
		runs[i-1].Last++
	} else {
		// alone
		newIv := Interval16{Start: v, Last: v}
		runs = append(runs[:i], append([]Interval16{newIv}, runs[i:]...)...)
	}
	c.setRuns(runs)
	c.setN(c.N() + 1)
	return c, true
}

// Contains returns true if v is in the container.
func (c *Container) Contains(v uint16) bool {
	if c == nil {
		return false
	}
	if c.isArray() {
		return c.arrayContains(v)
	} else if c.isRun() {
		return c.runContains(v)
	} else {
		return c.bitmapContains(v)
	}
}

func (c *Container) bitmapCountRuns() (r int32) {
	return bitmapCountRuns(c.bitmap())
}

func bitmapCountRuns(bitmap []uint64) (r int32) {
	for i := 0; i < 1023; i++ {
		v, v1 := bitmap[i], bitmap[i+1]
		r = r + int32(popcount((v<<1)&^v)+((v>>63)&^v1))
	}
	vl := bitmap[len(bitmap)-1]
	r = r + int32(popcount((vl<<1)&^vl)+vl>>63)
	return r
}

func arrayCountRuns(array []uint16) (r int32) {
	prev := int32(-2)
	for _, v := range array {
		if prev+1 != int32(v) {
			r++
		}
		prev = int32(v)
	}
	return r
}

func (c *Container) arrayCountRuns() (r int32) {
	return arrayCountRuns(c.array())
}

func (c *Container) countRuns() (r int32) {
	if c.isArray() {
		return c.arrayCountRuns()
	} else if c.isBitmap() {
		return c.bitmapCountRuns()
	} else if c.isRun() {
		return int32(len(c.runs()))
	}

	// sure hope this never happens
	return 0
}

// optimize converts the container to the type which will take up the least
// amount of space.
func (c *Container) optimize() *Container {
	if c.N() == 0 {
		statsHit("optimize/empty")
		return nil
	}
	runs := c.countRuns()

	var newType byte
	if runs <= runMaxSize && runs <= c.N()/2 {
		newType = ContainerRun
	} else if c.N() < ArrayMaxSize {
		newType = ContainerArray
	} else {
		newType = ContainerBitmap
	}

	// Then convert accordingly.
	if c.isArray() {
		if newType == ContainerBitmap {
			statsHit("optimize/arrayToBitmap")
			c = c.arrayToBitmap()
		} else if newType == ContainerRun {
			statsHit("optimize/arrayToRun")
			c = c.arrayToRun(runs)
		} else {
			statsHit("optimize/arrayUnchanged")
		}
	} else if c.isBitmap() {
		if newType == ContainerArray {
			statsHit("optimize/bitmapToArray")
			c = c.bitmapToArray()
		} else if newType == ContainerRun {
			statsHit("optimize/bitmapToRun")
			c = c.bitmapToRun(runs)
		} else {
			statsHit("optimize/bitmapUnchanged")
		}
	} else if c.isRun() {
		if newType == ContainerBitmap {
			statsHit("optimize/runToBitmap")
			c = c.runToBitmap()
		} else if newType == ContainerArray {
			statsHit("optimize/runToArray")
			c = c.runToArray()
		} else {
			statsHit("optimize/runUnchanged")
		}
	}
	return c
}

// unionInPlace does not necessarily preserve container's N; it's expected
// to be used when running a sequence of unions, after which you should
// call Repair(). (As of this writing, that only matters for bitmaps.)
//
// If called on a frozen container, or a container of the wrong sort,
// it is possible that the returned container will not actually be the
// original container; in-place is a suggestion.
func (c *Container) unionInPlace(other *Container) *Container {
	// short-circuit the trivial cases
	cN, cOk := c.SafeN()
	if cOk {
		if cN == MaxContainerVal+1 {
			return fullContainer
		}
		if cN == 0 {
			return other.Clone()
		}
	}
	oN, oOk := other.SafeN()
	if oOk {
		if oN == MaxContainerVal+1 {
			return fullContainer
		}
		if oN == 0 {
			return c
		}
	}
	switch c.typ() {
	case ContainerBitmap:
		switch other.typ() {
		case ContainerBitmap:
			return unionBitmapBitmapInPlace(c, other)
		case ContainerArray:
			return unionBitmapArrayInPlace(c, other)
		case ContainerRun:
			return unionBitmapRunInPlace(c, other)

		}
	case ContainerArray:
		switch other.typ() {
		case ContainerBitmap:
			c = c.arrayToBitmap()
			return unionBitmapBitmapInPlace(c, other)
		case ContainerArray:
			return unionArrayArrayInPlace(c, other)
		case ContainerRun:
			c = c.arrayToBitmap()
			return unionBitmapRunInPlace(c, other)
		}
	case ContainerRun:
		switch other.typ() {
		case ContainerBitmap:
			c = c.runToBitmap()
			return unionBitmapBitmapInPlace(c, other)
		case ContainerArray:
			c = c.runToBitmap()
			return unionBitmapArrayInPlace(c, other)
		case ContainerRun:
			return unionRunRunInPlace(c, other)
		}
	}
	panic(fmt.Errorf("invalid union op: unknown types %d/%d", c.typ(), other.typ()))
}

func (c *Container) arrayContains(v uint16) bool {
	return search32(c.array(), v) >= 0
}

func (c *Container) bitmapContains(v uint16) bool {
	return (c.bitmap()[v/64] & (1 << uint64(v%64))) != 0
}

// BinSearchRuns returns the index of the run containing v, and true, when v is contained;
// or the index of the next run starting after v, and false, when v is not contained.
func BinSearchRuns(v uint16, a []Interval16) (int32, bool) {
	i := int32(sort.Search(len(a),
		func(i int) bool {
			return a[i].Last >= v
		}))

	if i < int32(len(a)) {
		return i, (v >= a[i].Start) && (v <= a[i].Last)
	}

	return i, false
}

// runContains determines if v is in the container assuming c is a run
// container.
func (c *Container) runContains(v uint16) bool {
	_, found := BinSearchRuns(v, c.runs())
	return found
}

// remove removes a value from the container.
func (c *Container) remove(v uint16) (newC *Container, removed bool) {
	if c == nil {
		return nil, false
	}
	if c.isArray() {
		return c.arrayRemove(v)
	} else if c.isRun() {
		return c.runRemove(v)
	} else {
		return c.bitmapRemove(v)
	}
}

func (c *Container) arrayRemove(v uint16) (*Container, bool) {
	array := c.array()
	i := search32(array, v)
	if i < 0 {
		return c, false
	}
	// removing the last item? we can just return the empty container.
	if c.N() == 1 {
		return nil, true
	}
	c = c.Thaw()
	array = c.array()
	array = append(array[:i], array[i+1:]...)
	c.setArray(array)
	return c, true
}

func (c *Container) bitmapRemove(v uint16) (*Container, bool) {
	if !c.bitmapContains(v) {
		return c, false
	}

	// removing the last item? we can just return the empty container.
	if c.N() == 1 {
		return nil, true
	}
	c = c.Thaw()

	// Lower count and remove element.
	c.bitmap()[v/64] &^= (uint64(1) << uint(v%64))
	c.setN(c.N() - 1)

	// Convert to array if we go below the threshold.
	if c.N() == ArrayMaxSize {
		statsHit("bitmapRemove/bitmapToArray")
		c = c.bitmapToArray()
	}
	return c, true
}

// runRemove removes v from a run container, and returns true if v was removed.
func (c *Container) runRemove(v uint16) (*Container, bool) {
	runs := c.runs()
	i, contains := BinSearchRuns(v, runs)
	if !contains {
		return c, false
	}
	// removing the last item? we can just return the empty container.
	if c.N() == 1 {
		return nil, true
	}
	c = c.Thaw()
	runs = c.runs()
	if v == runs[i].Last && v == runs[i].Start {
		runs = append(runs[:i], runs[i+1:]...)
	} else if v == runs[i].Last {
		runs[i].Last--
	} else if v == runs[i].Start {
		runs[i].Start++
	} else if v > runs[i].Start {
		last := runs[i].Last
		runs[i].Last = v - 1
		runs = append(runs, Interval16{})
		copy(runs[i+2:], runs[i+1:])
		runs[i+1] = Interval16{Start: v + 1, Last: last}
		// runs = append(runs[:i+1], append([]interval16{{start: v + 1, last: last}}, runs[i+1:]...)...)
	}
	c.setN(c.N() - 1)
	c.setRuns(runs)
	return c, true
}

// max returns the maximum value in the container.
func (c *Container) max() uint16 {
	if c == nil || c.N() == 0 {
		// probably wrong, but prevents a crash elsewhere
		return 0
	}
	if c.isArray() {
		return c.arrayMax()
	} else if c.isRun() {
		return c.runMax()
	} else {
		return c.bitmapMax()
	}
}

func (c *Container) arrayMax() uint16 {
	array := c.array()
	return array[len(array)-1]
}

func (c *Container) bitmapMax() uint16 {
	// Search bitmap in reverse order.
	bitmap := c.bitmap()
	for i := len(bitmap); i > 0; i-- {
		// If value is zero then skip.
		v := bitmap[i-1]
		if v != 0 {
			r := bits.LeadingZeros64(v)
			return uint16((i-1)*64 + 63 - r)
		}

	}
	return 0
}

func (c *Container) runMax() uint16 {
	runs := c.runs()
	if len(runs) == 0 {
		return 0
	}
	return runs[len(runs)-1].Last
}

// bitmapToArray converts from bitmap format to array format.
func (c *Container) bitmapToArray() *Container {
	statsHit("bitmapToArray")
	if c == nil {
		if roaringParanoia {
			panic("nil container for bitmapToArray")
		}
		return nil
	}
	// If c is frozen, we'll be making a new array container. Otherwise,
	// we'll convert this container.
	if c.N() == 0 {
		if c.frozen() {
			return NewContainerArray(nil)
		}
		c.setTyp(ContainerArray)
		c.setArray(nil)
		return c
	}
	bitmap := c.bitmap()

	// FB-1247 adding an extra check just in case c.N proves to be unreliable
	// TODO  prove this has to be reliable
	makeArray := func(bm []uint64, ar []uint16) ([]uint16, bool, int32) {
		n := int32(0)
		for i, word := range bm {
			for word != 0 {
				t := word & -word
				if roaringParanoia {
					if n >= c.N() {
						panic("bitmap has more bits set than container.n")
					}
				}
				if n == int32(len(ar)) {
					return ar, true, n
				}
				ar[n] = uint16((i*64 + int(popcount(t-1))))
				n++
				word ^= t
			}
		}
		return ar, false, n
	}
	array, fail, n := makeArray(bitmap, make([]uint16, c.N()))
	if fail {
		// the onlyreason we are here is because N was incorrect
		// so we force a recount of N and try again
		c.bitmapRepair()
		array, fail, n = makeArray(bitmap, make([]uint16, c.N()))
		if fail {
			//this should not be able to happen under any circumstance
			panic("bitmapToArray failure")
		}

	}
	if roaringParanoia {
		if n != c.N() {
			panic("bitmap has fewer bits set than container.n")
		}
	}
	if c.frozen() {
		return NewContainerArray(array)
	}
	c.setTyp(ContainerArray)
	c.setMapped(false)
	c.setArray(array)
	return c
}

// arrayToBitmap converts from array format to bitmap format.
func (c *Container) arrayToBitmap() (out *Container) {
	statsHit("arrayToBitmap")
	if roaringParanoia {
		defer func() { out.CheckN() }()
	}
	if c == nil {
		if roaringParanoia {
			panic("nil container for arrayToBitmap")
		}
		return nil
	}

	// return early if empty
	if c.N() == 0 {
		if c.frozen() {
			return NewContainerBitmap(0, nil)
		}
		c.setTyp(ContainerBitmap)
		c.setBitmap(make([]uint64, bitmapN))
		return c
	}

	bitmap := make([]uint64, bitmapN)
	for _, v := range c.array() {
		bitmap[int(v)/64] |= (uint64(1) << uint(v%64))
	}
	if c.frozen() {
		return NewContainerBitmapN(bitmap, c.N())
	}
	c.setTyp(ContainerBitmap)
	c.setMapped(false)
	c.setBitmap(bitmap)
	return c
}

// runToBitmap converts from RLE format to bitmap format.
func (c *Container) runToBitmap() (out *Container) {
	statsHit("runToBitmap")
	if roaringParanoia {
		defer func() { c.CheckN() }()
	}
	if c == nil {
		if roaringParanoia {
			panic("nil container for runToBitmap")
		}
		return nil
	}
	if roaringParanoia {
		if c.N() > 65536 {
			panic(fmt.Sprintf("runToBitmap: container N %d", c.N()))
		}
	}

	// return early if empty
	if c.N() == 0 {
		if c.frozen() {
			return NewContainerBitmap(0, nil)
		}
		c.setTyp(ContainerBitmap)
		c.setBitmap(make([]uint64, bitmapN))
		return c
	}
	bitmap := make([]uint64, bitmapN)
	for _, iv := range c.runs() {
		w1, w2 := iv.Start/64, iv.Last/64
		b1, b2 := iv.Start&63, iv.Last&63
		// a mask for everything under bit X looks like
		// (1 << x) - 1. Say b1 is 4; our mask will want
		// to have the bottom 4 bits be zero, so we shift
		// left 4, getting 10000, then subtract 1, and
		// get 01111, which is the mask to *remove*.
		m1 := (uint64(1) << b1) - 1
		// inclusive mask: same thing, then shift left 1 and
		// or in 1. So for 4, we'd get 011111, which is the
		// mask to *keep*.
		m2 := (((uint64(1) << b2) - 1) << 1) | 1
		if w1 == w2 {
			// If we only had bit 4 in the range, this would
			// end up being 011111 &^ 01111, or 010000.
			bitmap[w1] |= (m2 &^ m1)
			continue
		}
		// for w2, the "To" field, we want to set the bottom N
		// bits. For w1, the "From" word, we want to set all *but*
		// the bottom N bits.
		bitmap[w2] |= m2
		bitmap[w1] |= ^m1
		words := bitmap[w1+1 : w2]
		// set every bit between them
		for i := range words {
			words[i] = ^uint64(0)
		}
	}
	if c.frozen() {
		return NewContainerBitmapN(bitmap, c.N())
	}
	c.setTyp(ContainerBitmap)
	c.setMapped(false)
	c.setBitmap(bitmap)
	return c
}

// bitmapToRun converts from bitmap format to RLE format.
func (c *Container) bitmapToRun(numRuns int32) *Container {
	statsHit("bitmapToRun")
	if c == nil {
		if roaringParanoia {
			panic("nil container for bitmapToRun")
		}
		return nil
	}

	// return early if empty
	if c.N() == 0 {
		if c.frozen() {
			return NewContainerRun(nil)
		}
		c.setTyp(ContainerRun)
		c.setRuns(nil)
		return c
	}

	bitmap := c.bitmap()
	if numRuns == 0 {
		numRuns = bitmapCountRuns(bitmap)
	}
	runs := make([]Interval16, 0, numRuns)

	current := bitmap[0]
	var i, start, last uint16
	for {
		// skip while empty
		for current == 0 && i < bitmapN-1 {
			i++
			current = bitmap[i]
		}

		if current == 0 {
			break
		}
		currentStart := uint16(trailingZeroN(current))
		start = 64*i + currentStart

		// pad LSBs with 1s
		current = current | (current - 1)

		// find next 0
		for current == maxBitmap && i < bitmapN-1 {
			i++
			current = bitmap[i]
		}

		if current == maxBitmap {

			// bitmap[1023] == maxBitmap
			runs = append(runs, Interval16{start, MaxContainerVal})
			break
		}
		currentLast := uint16(trailingZeroN(^current))
		last = 64*i + currentLast
		runs = append(runs, Interval16{start, last - 1})

		// pad LSBs with 0s
		current = current & (current + 1)
	}
	if c.frozen() {
		return NewContainerRunN(runs, c.N())
	}
	c.setTyp(ContainerRun)
	c.setRuns(runs)
	c.setMapped(false)
	return c
}

// arrayToRun converts from array format to RLE format.
func (c *Container) arrayToRun(numRuns int32) *Container {
	statsHit("arrayToRun")
	if c == nil {
		if roaringParanoia {
			panic("nil container for arrayToRun")
		}
		return nil
	}

	// return early if empty
	if c.N() == 0 {
		if c.frozen() {
			return NewContainerRun(nil)
		}
		c.setTyp(ContainerRun)
		c.setRuns(nil)
		return c
	}

	array := c.array()

	if numRuns == 0 {
		numRuns = arrayCountRuns(array)
	}

	runs := make([]Interval16, 0, numRuns)
	start := array[0]
	for i, v := range array[1:] {
		if v-array[i] > 1 {
			// if current-previous > 1, one run ends and another begins
			runs = append(runs, Interval16{start, array[i]})
			start = v
		}
	}
	// append final run
	runs = append(runs, Interval16{start, array[c.N()-1]})
	if c.frozen() {
		return NewContainerRunN(runs, c.N())
	}
	c.setTyp(ContainerRun)
	c.setMapped(false)
	c.setRuns(runs)
	return c
}

// runToArray converts from RLE format to array format.
func (c *Container) runToArray() *Container {
	statsHit("runToArray")
	if c == nil {
		if roaringParanoia {
			panic("nil container for runToArray")
		}
		return nil
	}

	// return early if empty
	if c.N() == 0 {
		if c.frozen() {
			return NewContainerArray(nil)
		}
		c.setTyp(ContainerArray)
		c.setArray(nil)
		return c
	}

	runs := c.runs()

	array := make([]uint16, c.N())
	n := int32(0)
	for _, r := range runs {
		for v := int(r.Start); v <= int(r.Last); v++ {
			array[n] = uint16(v)
			n++
		}
	}
	if roaringParanoia {
		if n != c.N() {
			panic("run has fewer bits set than container.n")
		}
	}
	if c.frozen() {
		return NewContainerArray(array)
	}
	c.setTyp(ContainerArray)
	c.setMapped(false)
	c.setArray(array)
	return c
}

// Clone returns a copy of c.
func (c *Container) Clone() (out *Container) {
	if roaringParanoia {
		defer func() { out.CheckN() }()
	}
	statsHit("Container/Clone")
	if c == nil {
		return nil
	}
	switch c.typ() {
	case ContainerArray:
		statsHit("Container/Clone/Array")
		out = NewContainerArrayCopy(c.array())
	case ContainerBitmap:
		statsHit("Container/Clone/Bitmap")
		other := NewContainerBitmapN(nil, 0)
		copy(other.bitmap(), c.bitmap())
		other.n = c.n
		out = other
	case ContainerRun:
		statsHit("Container/Clone/Run")
		out = NewContainerRunCopy(c.runs())
	default:
		panic(fmt.Sprintf("cloning a container of unknown type %d", c.typ()))
	}
	// this should probably never happen
	if roaringParanoia {
		if out.N() != out.count() {
			panic("cloned container has wrong n")
		}
	}
	return out
}

// WriteTo writes c to w.
func (c *Container) WriteTo(w io.Writer) (n int64, err error) {
	if c == nil {
		return 0, nil
	}
	if c.isArray() {
		return c.arrayWriteTo(w)
	} else if c.isRun() {
		return c.runWriteTo(w)
	} else {
		return c.bitmapWriteTo(w)
	}
}

func (c *Container) arrayWriteTo(w io.Writer) (n int64, err error) {
	statsHit("Container/arrayWriteTo")
	array := c.array()
	if len(array) == 0 {
		return 0, nil
	}

	// Verify all elements are valid.
	// TODO: instead of commenting this out, we need to make it a configuration option
	//	for _, v := range c.array {
	//	assert(lowbits(uint64(v)) == v, "cannot write array value out of range: %d", v)
	//}

	// Write sizeof(uint16) * cardinality bytes.
	nn, err := w.Write((*[0xFFFFFFF]byte)(unsafe.Pointer(&array[0]))[: 2*c.N() : 2*c.N()])
	return int64(nn), err
}

func (c *Container) bitmapWriteTo(w io.Writer) (n int64, err error) {
	statsHit("Container/bitmapWriteTo")
	bitmap := c.bitmap()
	// Write sizeof(uint64) * bitmapN bytes.
	nn, err := w.Write((*[0xFFFFFFF]byte)(unsafe.Pointer(&bitmap[0]))[:(8 * bitmapN):(8 * bitmapN)])
	return int64(nn), err
}

func (c *Container) runWriteTo(w io.Writer) (n int64, err error) {
	statsHit("Container/runWriteTo")
	runs := c.runs()
	if len(runs) == 0 {
		return 0, nil
	}
	var byte2 [2]byte
	binary.LittleEndian.PutUint16(byte2[:], uint16(len(runs)))
	_, err = w.Write(byte2[:])
	if err != nil {
		return 0, err
	}
	nn, err := w.Write((*[0xFFFFFFF]byte)(unsafe.Pointer(&runs[0]))[: interval16Size*len(runs) : interval16Size*len(runs)])
	return int64(runCountHeaderSize + nn), err
}

// size returns the encoded size of the container, in bytes.
func (c *Container) size() int {
	if c.isArray() {
		return len(c.array()) * 2 // sizeof(uint16)
	} else if c.isRun() {
		return len(c.runs())*interval16Size + runCountHeaderSize
	} else {
		return len(c.bitmap()) * 8 // sizeof(uint64)
	}
}

// info returns the current stats about the container.
func (c *Container) info() ContainerInfo {
	info := ContainerInfo{N: c.N(), Mapped: c.Mapped()}
	if c == nil {
		info.Type = "nil"
		info.Alloc = 0
		return info
	}
	info.Flags = c.flags.String()

	if c.isArray() {
		info.Type = "array"
		info.Alloc = len(c.array()) * 2 // sizeof(uint16)
	} else if c.isRun() {
		info.Type = "run"
		info.Alloc = len(c.runs())*interval16Size + runCountHeaderSize
	} else {
		info.Type = "bitmap"
		info.Alloc = len(c.bitmap()) * 8 // sizeof(uint64)
	}
	info.Pointer = uintptr(unsafe.Pointer(c.pointer))
	return info
}

// check performs a consistency check on the container.
func (c *Container) check() error {
	var a ErrorList

	if c == nil {
		return nil
	}
	if c.isArray() {
		array := c.array()
		if int32(len(array)) != c.N() {
			a.Append(fmt.Errorf("array count mismatch: count=%d, n=%d", len(array), c.N()))
		}
	} else if c.isRun() {
		n := RunCountRange(c.runs(), 0, MaxContainerVal+1)
		if n != c.N() {
			a.Append(fmt.Errorf("run count mismatch: count=%d, n=%d", n, c.N()))
		}
	} else if c.isBitmap() {
		if n := BitmapCountRange(c.bitmap(), 0, MaxContainerVal+1); n != c.N() {
			a.Append(fmt.Errorf("bitmap count mismatch: count=%d, n=%d", n, c.N()))
		}
	} else {
		a.Append(fmt.Errorf("empty container"))
		if c.N() != 0 {
			a.Append(fmt.Errorf("empty container with nonzero count: n=%d", c.N()))
		}
	}

	if a == nil {
		return nil
	}
	return a
}

// Repair repairs the cardinality of c if it has been corrupted by
// optimized operations.
func (c *Container) Repair() {
	// a frozen container can't have had n or contents changed, so we
	// don't need to recount it.
	if c.frozen() {
		return
	}
	if c.isBitmap() {
		c.bitmapRepair()
		c.setDirty(false)
	}
}

func (c *Container) bitmapRepair() {
	n := int32(0)
	// Manually unroll loop to make it a little faster.
	// TODO(rartoul): Can probably make this a few x faster using
	// SIMD instructions.
	bitmap := c.bitmap()[:bitmapN]
	for i := 0; i <= bitmapN-4; i += 4 {
		n += int32(popcount(bitmap[i]))
		n += int32(popcount(bitmap[i+1]))
		n += int32(popcount(bitmap[i+2]))
		n += int32(popcount(bitmap[i+3]))
	}
	c.setN(n)
}

// ContainerInfo represents a point-in-time snapshot of container stats.
type ContainerInfo struct {
	Key     uint64  // container key
	Type    string  // container type (array, bitmap, or run)
	Flags   string  // flag state
	N       int32   // number of bits
	Alloc   int     // memory used
	Pointer uintptr // address
	Mapped  bool    // whether this container thinks it is mmapped
}

// flip returns a new container containing the inverse of all
// bits in a.
func flip(a *Container) *Container { // nolint: deadcode
	if a.isArray() {
		return flipArray(a)
	} else if a.isRun() {
		return flipRun(a)
	} else {
		return flipBitmap(a)
	}
}

func flipArray(b *Container) *Container {
	statsHit("flipArray")
	// TODO: actually implement this
	x := b.Clone()
	x = x.arrayToBitmap()
	return flipBitmap(x)
}

func flipBitmap(b *Container) *Container {
	statsHit("flipBitmap")
	other := NewContainerBitmapN(nil, 0)
	bitmap := b.bitmap()
	otherBitmap := other.bitmap()
	for i, word := range bitmap {
		otherBitmap[i] = ^word
	}

	other.setN(other.count())
	return other
}

func flipRun(b *Container) *Container {
	statsHit("flipRun")
	// TODO: actually implement this
	x := b.Clone()
	x = x.runToBitmap()
	return flipBitmap(x)
}

// IntersectionAny checks whether two containers have any overlap without
// counting past the first bit found.
func IntersectionAny(a, b *Container) bool {
	return intersectionAny(a, b)
}

func intersectionAny(a, b *Container) bool {
	an := a.N()
	if an == 0 {
		return false
	}
	bn := b.N()
	if bn == 0 {
		return false
	}
	if an+bn > MaxContainerVal+1 {
		return true
	}
	if a.isArray() {
		if b.isArray() {
			return intersectionAnyArrayArray(a, b)
		} else if b.isRun() {
			return intersectionAnyArrayRun(a, b)
		} else {
			return intersectionAnyArrayBitmap(a, b)
		}
	} else if a.isRun() {
		if b.isArray() {
			return intersectionAnyArrayRun(b, a)
		} else if b.isRun() {
			return intersectionAnyRunRun(a, b)
		} else {
			return intersectionAnyRunBitmap(a, b)
		}
	} else {
		if b.isArray() {
			return intersectionAnyArrayBitmap(b, a)
		} else if b.isRun() {
			return intersectionAnyRunBitmap(b, a)
		} else {
			return intersectionAnyBitmapBitmap(a, b)
		}
	}
}

func intersectionAnyArrayArray(a, b *Container) bool {
	ca, cb := a.array(), b.array()
	nb := len(cb)
	j := 0
	for _, va := range ca {
		for cb[j] < va {
			j++
			if j >= nb {
				return false
			}
		}
		if cb[j] == va {
			return true
		}
	}
	return false
}

func intersectionAnyArrayRun(a, b *Container) bool {
	array, runs := a.array(), b.runs()
	na, nb := len(array), len(runs)
	for i, j := 0, 0; i < na && j < nb; {
		va, vb := array[i], runs[j]
		if va < vb.Start {
			i++
		} else if va >= vb.Start && va <= vb.Last {
			return true
		} else if va > vb.Last {
			j++
		}
	}
	return false
}

func intersectionAnyArrayBitmap(a, b *Container) bool {
	bitmap := b.bitmap()[:1024]
	for _, val := range a.array() {
		i := int(val >> 6)
		off := val % 64
		if (bitmap[i]>>off)&1 != 0 {
			return true
		}
	}
	return false
}

func intersectionAnyRunRun(a, b *Container) bool {
	ra, rb := a.runs(), b.runs()
	na, nb := len(ra), len(rb)
	for i, j := 0, 0; i < na && j < nb; {
		va, vb := ra[i], rb[j]
		if va.Last < vb.Start {
			// |--va--| |--vb--|
			i++
		} else if va.Start > vb.Last {
			// |--vb--| |--va--|
			j++
		} else {
			// va.Last >= vb.Start, and va.Start <= vb.Last,
			// means there must be overlap
			return true
		}
	}
	return false
}

func intersectionAnyRunBitmap(a, b *Container) bool {
	bb := b.bitmap()[:1024]
	runs := a.runs()
	for _, r := range runs {
		if r.Start/64 == r.Last/64 {
			mask := (^uint64(0) << (r.Start % 64)) &^
				(^uint64(0) << ((r.Last % 64) + 1))
			if mask&bb[r.Start/64] != 0 {
				return true
			}
			continue
		}

		firstWord, lastWord := r.Start/64, r.Last/64
		for i := firstWord + 1; i < lastWord; i++ {
			if bb[i] != 0 {
				return true
			}
		}

		firstMask := ^uint64(0) << (r.Start % 64)
		lastMask := ^(^uint64(0) << ((r.Last % 64) + 1))
		if (firstMask&bb[firstWord])|(lastMask&bb[lastWord]) != 0 {
			return true
		}
	}
	return false
}

func intersectionAnyBitmapBitmap(a, b *Container) bool {
	ba, bb := a.bitmap()[:1024], b.bitmap()[:1024]
	for i, v := range ba {
		if bb[i]&v != 0 {
			return true
		}
	}
	return false
}

func containerCallback(a *Container, fn func(uint16)) {
	if a.N() == 0 {
		return
	}
	switch {
	case a.isArray():
		values := a.array()
		for _, v := range values {
			fn(v)
		}
	case a.isBitmap():
		values := a.bitmap()
		for i, w := range values {
			if w == 0 {
				continue
			}
			callbackBits(w, uint16(i)*64, fn)
		}
	case a.isRun():
		values := a.runs()
		for _, r := range values {
			for i := int(r.Start); i <= int(r.Last); i++ {
				fn(uint16(i))
			}
		}
	}
}

func intersectionCallback(a, b *Container, fn func(uint16)) {
	if a.N() == MaxContainerVal+1 {
		containerCallback(b, fn)
		return
	}
	if b.N() == MaxContainerVal+1 {
		containerCallback(a, fn)
		return
	}
	if a.N() == 0 || b.N() == 0 {
		return
	}
	if a.isArray() {
		if b.isArray() {
			intersectionCallbackArrayArray(a, b, fn)
		} else if b.isRun() {
			intersectionCallbackArrayRun(a, b, fn)
		} else {
			intersectionCallbackArrayBitmap(a, b, fn)
		}
	} else if a.isRun() {
		if b.isArray() {
			intersectionCallbackArrayRun(b, a, fn)
		} else if b.isRun() {
			intersectionCallbackRunRun(a, b, fn)
		} else {
			intersectionCallbackBitmapRun(b, a, fn)
		}
	} else {
		if b.isArray() {
			intersectionCallbackArrayBitmap(b, a, fn)
		} else if b.isRun() {
			intersectionCallbackBitmapRun(a, b, fn)
		} else {
			intersectionCallbackBitmapBitmap(a, b, fn)
		}
	}
}

func intersectionCount(a, b *Container) int32 {
	if a.N() == MaxContainerVal+1 {
		return b.N()
	}
	if b.N() == MaxContainerVal+1 {
		return a.N()
	}
	if a.N() == 0 || b.N() == 0 {
		return 0
	}
	if a.isArray() {
		if b.isArray() {
			return intersectionCountArrayArray(a, b)
		} else if b.isRun() {
			return intersectionCountArrayRun(a, b)
		} else {
			return intersectionCountArrayBitmap(a, b)
		}
	} else if a.isRun() {
		if b.isArray() {
			return intersectionCountArrayRun(b, a)
		} else if b.isRun() {
			return intersectionCountRunRun(a, b)
		} else {
			return intersectionCountBitmapRun(b, a)
		}
	} else {
		if b.isArray() {
			return intersectionCountArrayBitmap(b, a)
		} else if b.isRun() {
			return intersectionCountBitmapRun(a, b)
		} else {
			return intersectionCountBitmapBitmap(a, b)
		}
	}
}

func intersectionCountArrayArray(a, b *Container) (n int32) {
	statsHit("intersectionCount/ArrayArray")
	ca, cb := a.array(), b.array()
	na, nb := len(ca), len(cb)
	if na > nb {
		ca, cb = cb, ca
		na, nb = nb, na // nolint: staticcheck, ineffassign
	}
	j := 0
	for _, va := range ca {
		for cb[j] < va {
			j++
			if j >= nb {
				return n
			}
		}
		if cb[j] == va {
			n++
		}
	}
	return n
}

func intersectionCountArrayRun(a, b *Container) (n int32) {
	statsHit("intersectionCount/ArrayRun")
	array, runs := a.array(), b.runs()
	na, nb := len(array), len(runs)
	for i, j := 0, 0; i < na && j < nb; {
		va, vb := array[i], runs[j]
		if va < vb.Start {
			i++
		} else if va >= vb.Start && va <= vb.Last {
			i++
			n++
		} else if va > vb.Last {
			j++
		}
	}
	return n
}

func intersectionCountRunRun(a, b *Container) (n int32) {
	statsHit("intersectionCount/RunRun")
	ra, rb := a.runs(), b.runs()
	na, nb := len(ra), len(rb)
	for i, j := 0, 0; i < na && j < nb; {
		va, vb := ra[i], rb[j]
		if va.Last < vb.Start {
			// |--va--| |--vb--|
			i++
		} else if va.Start > vb.Last {
			// |--vb--| |--va--|
			j++
		} else if va.Last > vb.Last && va.Start >= vb.Start {
			// |--vb-|-|-va--|
			n += 1 + int32(vb.Last-va.Start)
			j++
		} else if va.Last > vb.Last && va.Start < vb.Start {
			// |--va|--vb--|--|
			n += 1 + int32(vb.Last-vb.Start)
			j++
		} else if va.Last <= vb.Last && va.Start >= vb.Start {
			// |--vb|--va--|--|
			n += 1 + int32(va.Last-va.Start)
			i++
		} else if va.Last <= vb.Last && va.Start < vb.Start {
			// |--va-|-|-vb--|
			n += 1 + int32(va.Last-vb.Start)
			i++
		}
	}
	return n
}

func intersectionCountBitmapRun(a, b *Container) (n int32) {
	statsHit("intersectionCount/BitmapRun")
	for _, iv := range b.runs() {
		n += BitmapCountRange(a.bitmap(), int32(iv.Start), int32(iv.Last)+1)
	}
	return n
}

func intersectionCountArrayBitmap(a, b *Container) (n int32) {
	statsHit("intersectionCount/ArrayBitmap")
	bitmap := b.bitmap()
	ln := len(bitmap)
	for _, val := range a.array() {
		i := int(val >> 6)
		if i >= ln {
			break
		}
		off := val % 64
		n += int32(bitmap[i]>>off) & 1
	}
	return n
}

func intersectionCountBitmapBitmap(a, b *Container) (n int32) {
	statsHit("intersectionCount/BitmapBitmap")
	return int32(popcountAndSlice(a.bitmap(), b.bitmap()))
}

func intersectionCallbackArrayArray(a, b *Container, fn func(uint16)) {
	statsHit("intersectionCallback/ArrayArray")
	ca, cb := a.array(), b.array()
	na, nb := len(ca), len(cb)
	if na > nb {
		ca, cb = cb, ca
		na, nb = nb, na // nolint: staticcheck, ineffassign
	}
	if (na << 2) < nb {
		for _, va := range ca {
			if cb[0] < va {
				// try to skip ahead a bit faster
				for len(cb) > 7 && cb[7] < va {
					cb = cb[8:]
				}
				for len(cb) > 0 && cb[0] < va {
					cb = cb[1:]
				}
				if len(cb) == 0 {
					return
				}
			}
			if cb[0] == va {
				fn(va)
			}
		}
		return
	}
	j := 0
	for _, va := range ca {
		for cb[j] < va {
			j++
			if j >= nb {
				return
			}
		}
		if cb[j] == va {
			fn(va)
		}
	}
}

func intersectionCallbackArrayRun(a, b *Container, fn func(uint16)) {
	statsHit("intersectionCallback/ArrayRun")
	array, runs := a.array(), b.runs()
	na, nb := len(array), len(runs)
	for i, j := 0, 0; i < na && j < nb; {
		va, vb := array[i], runs[j]
		if va > vb.Last {
			j++
			continue
		}
		// If we got here, va is either before or in the current run,
		// so we're definitely done with this member of the array.
		i++
		if va >= vb.Start {
			fn(va)
		}
	}
}

func intersectionCallbackRunRun(a, b *Container, fn func(uint16)) {
	statsHit("intersectionCallback/RunRun")
	ra, rb := a.runs(), b.runs()
	na, nb := len(ra), len(rb)
	for i, j := 0, 0; i < na && j < nb; {
		va, vb := ra[i], rb[j]
		if va.Last < vb.Start {
			// |--va--| |--vb--|
			i++
		} else if va.Start > vb.Last {
			// |--vb--| |--va--|
			j++
		} else if va.Last > vb.Last && va.Start >= vb.Start {
			// |--vb-|-|-va--|
			for z := int(va.Start); z <= int(vb.Last); z++ {
				fn(uint16(z))
			}
			j++
		} else if va.Last > vb.Last && va.Start < vb.Start {
			// |--va|--vb--|--|
			for z := int(vb.Start); z <= int(vb.Last); z++ {
				fn(uint16(z))
			}
			j++
		} else if va.Last <= vb.Last && va.Start >= vb.Start {
			// |--vb|--va--|--|
			for z := int(va.Start); z <= int(va.Last); z++ {
				fn(uint16(z))
			}
			i++
		} else if va.Last <= vb.Last && va.Start < vb.Start {
			// |--va-|-|-vb--|
			for z := int(vb.Start); z <= int(va.Last); z++ {
				fn(uint16(z))
			}
			i++
		}
	}
}

func intersectionCallbackBitmapRun(a, b *Container, fn func(uint16)) {
	statsHit("intersectionCallback/BitmapRun")
	for _, iv := range b.runs() {
		bitmapCallbackRange(a.bitmap(), int32(iv.Start), int32(iv.Last)+1, fn)
	}
}

func intersectionCallbackArrayBitmap(a, b *Container, fn func(uint16)) {
	statsHit("intersectionCallback/ArrayBitmap")
	bitmap := b.bitmap()
	ln := len(bitmap)
	for _, val := range a.array() {
		i := int(val >> 6)
		if i >= ln {
			break
		}
		off := val % 64
		if (bitmap[i]>>off)&1 != 0 {
			fn(val)
		}
	}
}

func intersectionCallbackBitmapBitmap(a, b *Container, fn func(uint16)) {
	statsHit("intersectionCallback/BitmapBitmap")
	ab, bb := a.bitmap(), b.bitmap()
	for i := range ab {
		w := ab[i] & bb[i]
		if w == 0 {
			continue
		}
		base := uint16(i) * 64
		callbackBits(w, base, fn)
	}
}

func intersect(a, b *Container) (c *Container) {
	if roaringParanoia {
		defer func() { c.CheckN() }()
	}
	if a.N() == MaxContainerVal+1 {
		return b.Freeze()
	}
	if b.N() == MaxContainerVal+1 {
		return a.Freeze()
	}
	if a.N() == 0 || b.N() == 0 {
		return nil
	}
	if a.isArray() {
		if b.isArray() {
			return intersectArrayArray(a, b)
		} else if b.isRun() {
			return intersectArrayRun(a, b)
		} else {
			return intersectArrayBitmap(a, b)
		}
	} else if a.isRun() {
		if b.isArray() {
			return intersectArrayRun(b, a)
		} else if b.isRun() {
			return intersectRunRun(a, b)
		} else {
			return intersectBitmapRun(b, a)
		}
	} else {
		if b.isArray() {
			return intersectArrayBitmap(b, a)
		} else if b.isRun() {
			return intersectBitmapRun(a, b)
		} else {
			return intersectBitmapBitmap(a, b)
		}
	}
}

func intersectArrayArray(a, b *Container) *Container {
	statsHit("intersect/ArrayArray")
	aa, ab := a.array(), b.array()
	na, nb := len(aa), len(ab)
	output := make([]uint16, 0, na)
	for i, j := 0, 0; i < na && j < nb; {
		va, vb := aa[i], ab[j]
		if va < vb {
			i++
		} else if va > vb {
			j++
		} else {
			output = append(output, va)
			i, j = i+1, j+1
		}
	}
	return NewContainerArray(output)
}

// intersectArrayRun computes the intersect of an array container and a run
// container. The return is always an array container (since it's guaranteed to
// be low-cardinality)
func intersectArrayRun(a, b *Container) *Container {
	statsHit("intersect/ArrayRun")
	aa, rb := a.array(), b.runs()
	na, nb := len(aa), len(rb)
	var output []uint16
	for i, j := 0, 0; i < na && j < nb; {
		va, vb := aa[i], rb[j]
		if va < vb.Start {
			i++
		} else if va > vb.Last {
			j++
		} else {
			output = append(output, va)
			i++
		}
	}
	return NewContainerArray(output)
}

// intersectRunRun computes the intersect of two run containers.
func intersectRunRun(a, b *Container) *Container {
	statsHit("intersect/RunRun")
	output := NewContainerRun(nil)
	ra, rb := a.runs(), b.runs()
	na, nb := len(ra), len(rb)
	n := int32(0)
	for i, j := 0, 0; i < na && j < nb; {
		va, vb := ra[i], rb[j]
		if va.Last < vb.Start {
			// |--va--| |--vb--|
			i++
		} else if vb.Last < va.Start {
			// |--vb--| |--va--|
			j++
		} else if va.Last > vb.Last && va.Start >= vb.Start {
			// |--vb-|-|-va--|
			n += output.runAppendInterval(Interval16{Start: va.Start, Last: vb.Last})
			j++
		} else if va.Last > vb.Last && va.Start < vb.Start {
			// |--va|--vb--|--|
			n += output.runAppendInterval(vb)
			j++
		} else if va.Last <= vb.Last && va.Start >= vb.Start {
			// |--vb|--va--|--|
			n += output.runAppendInterval(va)
			i++
		} else if va.Last <= vb.Last && va.Start < vb.Start {
			// |--va-|-|-vb--|
			n += output.runAppendInterval(Interval16{Start: vb.Start, Last: va.Last})
			i++
		}
	}
	output.setN(n)
	runs := output.runs()
	if n < ArrayMaxSize && int32(len(runs)) > n/2 {
		output = output.runToArray()
	} else if len(runs) > runMaxSize {
		output = output.runToBitmap()
	}
	return output
}

// intersectBitmapRun returns an array container if either container's
// cardinality is <= ArrayMaxSize. Otherwise it returns a bitmap container.
func intersectBitmapRun(a, b *Container) *Container {
	statsHit("intersect/BitmapRun")
	var output *Container
	runs := b.runs()
	// Intersection will be array-sized for sure if either of the inputs
	// is array-sized.
	if b.N() <= ArrayMaxSize {
		var scratch [ArrayMaxSize]uint16
		n := 0
		for _, iv := range runs {
			for i := int(iv.Start); i <= int(iv.Last); i++ {
				if a.bitmapContains(uint16(i)) {
					scratch[n] = uint16(i)
					n++
				}
			}
		}
		// output is array container
		array := make([]uint16, n)
		copy(array, scratch[:])
		output = NewContainerArray(array)
	} else {
		// right now this iterates through the runs and sets integers in the
		// bitmap that are in the runs. alternately, we could zero out ranges in
		// the bitmap which are between runs.
		output = NewContainerBitmap(0, nil)
		bitmap := output.bitmap()
		aBitmap := a.bitmap()
		n := int32(0)
		for j := 0; j < len(runs); j++ {
			vb := runs[j]
			i := vb.Start >> 6 // index into a
			vastart := i << 6
			valast := vastart + 63
			for valast >= vb.Start && vastart <= vb.Last && i < bitmapN {
				if vastart >= vb.Start && valast <= vb.Last { // a within b
					bitmap[i] = aBitmap[i]
					n += int32(popcount(aBitmap[i]))
				} else if vb.Start >= vastart && vb.Last <= valast { // b within a
					var mask uint64 = ((1 << (vb.Last - vb.Start + 1)) - 1) << (vb.Start - vastart)
					bits := aBitmap[i] & mask
					bitmap[i] |= bits
					n += int32(popcount(bits))
				} else if vastart < vb.Start { // a overlaps front of b
					offset := 64 - (1 + valast - vb.Start)
					bits := (aBitmap[i] >> offset) << offset
					bitmap[i] |= bits
					n += int32(popcount(bits))
				} else if vb.Start < vastart { // b overlaps front of a
					offset := 64 - (1 + vb.Last - vastart)
					bits := (aBitmap[i] << offset) >> offset
					bitmap[i] |= bits
					n += int32(popcount(bits))
				}
				// update loop vars
				i++
				vastart = i << 6
				valast = vastart + 63
			}
		}
		output.setN(n)
	}
	return output
}

func intersectArrayBitmap(a, b *Container) *Container {
	statsHit("intersect/ArrayBitmap")
	array := make([]uint16, 0)
	bBitmap := b.bitmap()
	for _, va := range a.array() {
		bmidx := va / 64
		bidx := va % 64
		mask := uint64(1) << bidx
		b := bBitmap[bmidx]
		if b&mask > 0 {
			array = append(array, va)
		}
	}
	return NewContainerArray(array)
}

func intersectBitmapBitmap(a, b *Container) *Container {
	statsHit("intersect/BitmapBitmap")
	// local variables added to prevent BCE checks in loop
	// see https://go101.org/article/bounds-check-elimination.html
	var (
		ab = a.bitmask()
		bb = b.bitmask()
		ob = [1024]uint64{}
		n  int32
	)
	_, _ = &ab[0], &bb[0]
	for i := range ob {
		ob[i] = ab[i] & bb[i]
		n += int32(popcount(ob[i]))
	}

	output := NewContainerBitmapN(ob[:], n)
	return output
}

func union(a, b *Container) (c *Container) {
	if roaringParanoia {
		defer func() { c.CheckN() }()
	}
	if a.N() == MaxContainerVal+1 || b.N() == MaxContainerVal+1 {
		return fullContainer
	}
	if a.isArray() {
		if b.isArray() {
			return unionArrayArray(a, b)
		} else if b.isRun() {
			return unionArrayRun(a, b)
		} else {
			return unionArrayBitmap(a, b)
		}
	} else if a.isRun() {
		if b.isArray() {
			return unionArrayRun(b, a)
		} else if b.isRun() {
			return unionRunRun(a, b)
		} else {
			return unionBitmapRun(b, a)
		}
	} else {
		if b.isArray() {
			return unionArrayBitmap(b, a)
		} else if b.isRun() {
			return unionBitmapRun(a, b)
		} else {
			return unionBitmapBitmap(a, b)
		}
	}
}
func Merge(a, b []uint16) {

}
func unionArrayArray(a, b *Container) *Container {
	statsHit("union/ArrayArray")
	if a.N() == 0 {
		return b
	}
	if b.N() == 0 {
		return a
	}
	s1, s2 := a.array(), b.array()
	n1, n2 := len(s1), len(s2)
	output := make([]uint16, 0, n1+n2)
	i, j := 0, 0
	for {
		va, vb := s1[i], s2[j]
		if va < vb {
			output = append(output, va)
			i++
		} else if va > vb {
			output = append(output, vb)
			j++
		} else {
			output = append(output, va)
			i++
			j++
		}
		// It's possible we hit the ends at the same time,
		// in which case the append will copy 0 items. This
		// is cheaper than performing a separate conditional
		// check every time...
		if j >= n2 {
			output = append(output, s1[i:]...)
			break
		}
		if i >= n1 {
			output = append(output, s2[j:]...)
			break
		}
	}
	// note: len(output) CAN be > 4096
	return NewContainerArray(output)
}

// unionArrayArrayInPlace does what it sounds like -- tries to combine
// the two arrays in-place. It does not try to ensure that the result is
// of a good array size, so it could be up to twice that size, temporarily.
func unionArrayArrayInPlace(a, b *Container) *Container {
	statsHit("union/ArrayArrayInPlace")
	if a.N() == 0 {
		if b.N() != 0 {
			// for InPlace, we actually want to ensure that
			// we update a, as long as it's not frozen.
			a = a.Thaw()
			// ... but we also want to be sure we don't end up
			// copying in a mapped object into our not-mapped
			// object.
			a.setArrayMaybeCopy(b.array(), b.Mapped())
			return a.optimize()
		}
		return a
	}
	if b.N() == 0 {
		return a
	}
	s1, s2 := a.array(), b.array()
	n1, n2 := len(s1), len(s2)
	output := make([]uint16, 0, n1+n2)
	i, j := 0, 0
	for {
		va, vb := s1[i], s2[j]
		if va < vb {
			output = append(output, va)
			i++
		} else if va > vb {
			output = append(output, vb)
			j++
		} else {
			output = append(output, va)
			i++
			j++
		}
		// It's possible we hit the ends at the same time,
		// in which case the append will copy 0 items. This
		// is cheaper than performing a separate conditional
		// check every time...
		if j >= n2 {
			output = append(output, s1[i:]...)
			break
		}
		if i >= n1 {
			output = append(output, s2[j:]...)
			break
		}
	}
	// a union can't omit anything that was previously in a, so if
	// the output is the same length, nothing changed.
	if len(output) != int(a.N()) {
		a = a.Thaw()
		a.setArray(output)
	}
	return a.optimize()
}

// unionArrayRun optimistically assumes that the result will be a run container,
// and converts to a bitmap or array container afterwards if necessary.
func unionArrayRun(a, b *Container) *Container {
	statsHit("union/ArrayRun")
	output := NewContainerRun(nil)
	aa, rb := a.array(), b.runs()
	na, nb := len(aa), len(rb)
	var vb Interval16
	var va uint16
	n := int32(0)
	for i, j := 0, 0; i < na || j < nb; {
		if i < na {
			va = aa[i]
		}
		if j < nb {
			vb = rb[j]
		}
		if i < na && (j >= nb || va < vb.Start) {
			n += output.runAppendInterval(Interval16{Start: va, Last: va})
			i++
		} else {
			n += output.runAppendInterval(vb)
			j++
		}
	}
	output.setN(n)
	if n < ArrayMaxSize {
		output = output.runToArray()
	} else if len(output.runs()) > runMaxSize {
		output = output.runToBitmap()
	}
	return output
}

// runAppendInterval adds the given interval to the run container. It assumes
// that the interval comes at the end of the list of runs, and does not check
// that this is the case. It will not behave correctly if the start of the given
// interval is earlier than the start of the last interval in the list of runs.
// Its return value is the amount by which the cardinality of the container was
// increased.
func (c *Container) runAppendInterval(v Interval16) int32 {
	runs := c.runs()
	if len(runs) == 0 {
		runs = append(runs, v)
		c.setRuns(runs)
		return int32(v.Last-v.Start) + 1
	}

	last := runs[len(runs)-1]
	if last.Last == MaxContainerVal { //protect against overflow
		return 0
	}
	if last.Last+1 >= v.Start && v.Last > last.Last {
		runs[len(runs)-1].Last = v.Last
		c.setRuns(runs)
		return int32(v.Last - last.Last)
	} else if last.Last+1 < v.Start {
		runs = append(runs, v)
		c.setRuns(runs)
		return int32(v.Last-v.Start) + 1
	}
	return 0
}

func unionRunRun(a, b *Container) *Container {
	statsHit("union/RunRun")
	ra, rb := a.runs(), b.runs()
	na, nb := len(ra), len(rb)
	output := NewContainerRun(make([]Interval16, 0, na+nb))
	var va, vb Interval16
	n := int32(0)
	for i, j := 0, 0; i < na || j < nb; {
		if i < na {
			va = ra[i]
		}
		if j < nb {
			vb = rb[j]
		}
		if i < na && (j >= nb || va.Start < vb.Start) {
			n += output.runAppendInterval(va)
			i++
		} else {
			n += output.runAppendInterval(vb)
			j++
		}
	}
	output.setN(n)
	if len(output.runs()) > runMaxSize {
		output = output.runToBitmap()
	}
	return output
}

func unionBitmapRun(a, b *Container) *Container {
	statsHit("union/BitmapRun")
	output := a.Clone()
	for _, run := range b.runs() {
		output.bitmapSetRange(uint64(run.Start), uint64(run.Last)+1)
	}
	return output
}

// unions the run b into the bitmap a, mutating a in place. The n value of
// a will need to be repaired after the fact.
func unionBitmapRunInPlace(a, b *Container) *Container {
	a = a.Thaw()
	a.setDirty(true)
	bitmap := a.bitmap()
	statsHit("union/BitmapRun")
	for _, run := range b.runs() {
		bitmapSetRangeIgnoreN(bitmap, uint64(run.Start), uint64(run.Last)+1)
	}
	return a
}

const maxBitmap = 0xFFFFFFFFFFFFFFFF

// sets all bits in [i, j) (c must be a bitmap container, and bitmap must
// be its bitmap).
func (c *Container) bitmapSetRange(i, j uint64) {
	bitmap := c.bitmap()
	x := i >> 6
	y := (j - 1) >> 6
	var X uint64 = maxBitmap << (i % 64)
	var Y uint64 = maxBitmap >> (63 - ((j - 1) % 64))
	xcnt := popcount(X)
	ycnt := popcount(Y)
	n := int32(c.N())
	if x == y {
		n += int32((j - i) - popcount(bitmap[x]&(X&Y)))
		bitmap[x] |= (X & Y)
	} else {
		n += int32(xcnt - popcount(bitmap[x]&X))
		bitmap[x] |= X
		for i := x + 1; i < y; i++ {
			n += int32(64 - popcount(bitmap[i]))
			bitmap[i] = maxBitmap
		}
		n += int32(ycnt - popcount(bitmap[y]&Y))
		bitmap[y] |= Y
	}
	c.setN(n)
}

// sets all bits in [i, j) without updating any corresponding n value.
func bitmapSetRangeIgnoreN(bitmap []uint64, i, j uint64) {
	x := i >> 6
	y := (j - 1) >> 6
	var X uint64 = maxBitmap << (i % 64)
	var Y uint64 = maxBitmap >> (63 - ((j - 1) % 64))

	if x == y {
		bitmap[x] |= (X & Y)
	} else {
		bitmap[x] |= X
		for i := x + 1; i < y; i++ {
			bitmap[i] = maxBitmap
		}
		bitmap[y] |= Y
	}
}

// xor's all bits in [i, j) with all true (c must be a bitmap container).
func (c *Container) bitmapXorRange(i, j uint64) {
	x := i >> 6
	y := (j - 1) >> 6
	var X uint64 = maxBitmap << (i % 64)
	var Y uint64 = maxBitmap >> (63 - ((j - 1) % 64))
	bitmap := c.bitmap()
	n := c.N()
	if x == y {
		cnt := popcount(bitmap[x])
		bitmap[x] ^= (X & Y) //// flip
		n += int32(popcount(bitmap[x]) - cnt)
	} else {
		cnt := popcount(bitmap[x])
		bitmap[x] ^= X
		n += int32(popcount(bitmap[x]) - cnt)
		for i := x + 1; i < y; i++ {
			cnt = popcount(bitmap[i])
			bitmap[i] ^= maxBitmap
			n += int32(popcount(bitmap[i]) - cnt)
		}
		cnt = popcount(bitmap[y])
		bitmap[y] ^= Y
		n += int32(popcount(bitmap[y]) - cnt)
	}
	c.setN(n)
}

// zeroes all bits in [i, j) (c must be a bitmap container)
func (c *Container) bitmapZeroRange(i, j uint64) {
	x := i >> 6
	y := (j - 1) >> 6
	var X uint64 = maxBitmap << (i % 64)
	var Y uint64 = maxBitmap >> (63 - ((j - 1) % 64))
	bitmap := c.bitmap()
	n := c.N()
	if x == y {
		n -= int32(popcount(bitmap[x] & (X & Y)))
		bitmap[x] &= ^(X & Y)
	} else {
		n -= int32(popcount(bitmap[x] & X))
		bitmap[x] &= ^X
		for i := x + 1; i < y; i++ {
			n -= int32(popcount(bitmap[i]))
			bitmap[i] = 0
		}
		n -= int32(popcount(bitmap[y] & Y))
		bitmap[y] &= ^Y
	}
	c.setN(n)
}

func typePair(ct1, ct2 byte) int {
	return int((ct1 << 4) | ct2)
}

// compareArrayBitmap actually only verifies that everything in the array
// is in the bitmap. It's used only after comparing the N for the containers,
// so if there's anything in the bitmap that's not in the array, either there's
// something in the array that's not in the bitmap, or we didn't get here.
func compareArrayBitmap(a []uint16, b []uint64) error {
	for _, v := range a {
		w, bit := b[v>>6], v&63
		if w>>bit&1 == 0 {
			return fmt.Errorf("value %d missing", v)
		}
	}
	return nil
}

// compareArrayRuns determines whether an array matches a provided
// set of runs. As with compareArrayBitmap, it only verifies presence
// of the array's values in the run collection. the run collection
// can't be empty; if it were, N would have been 0, and we wouldn't
// have gotten here.
func compareArrayRuns(a []uint16, r []Interval16) error {
	ri := 0
	ru := r[ri]
	ri++
	for _, v := range a {
		if v < ru.Start {
			return fmt.Errorf("value %d missing", v)
		}
		if v > ru.Last {
			if ri >= len(r) {
				return fmt.Errorf("value %d missing", v)
			}
			ru = r[ri]
			ri++
			// if they're identical, the array value must be
			// the start of the next run.
			if v != ru.Start {
				return fmt.Errorf("value %d missing", v)
			}
		}
	}
	return nil
}

// compareArrayArray reports whether everything in a1 is equal to everything
// in a2.
func compareArrayArray(a1, a2 []uint16) error {
	if len(a1) != len(a2) {
		return fmt.Errorf("unexpected length mismatch, %d vs %d", len(a1), len(a2))
	}
	for i := range a1 {
		if a1[i] != a2[i] {
			return fmt.Errorf("item %d: %d vs %d", i, a1[i], a2[i])
		}
	}
	return nil
}

// BitwiseCompare reports whether two containers are equal. It returns
// an error describing any difference it finds. This is mostly intended
// for use in tests that expect equality.
func (c *Container) BitwiseCompare(c2 *Container) error {
	cn, c2n := c.N(), c2.N()
	if cn != c2n {
		return fmt.Errorf("containers are different lengths (%d vs %d)", cn, c2n)
	}
	if cn == 0 {
		return nil
	}
	switch typePair(c.typ(), c2.typ()) {
	case typePair(ContainerArray, ContainerArray):
		return compareArrayArray(c.array(), c2.array())
	case typePair(ContainerArray, ContainerBitmap):
		return compareArrayBitmap(c.array(), c2.bitmap())
	case typePair(ContainerBitmap, ContainerArray):
		return compareArrayBitmap(c2.array(), c.bitmap())
	case typePair(ContainerArray, ContainerRun):
		return compareArrayRuns(c.array(), c2.runs())
	case typePair(ContainerRun, ContainerArray):
		return compareArrayRuns(c2.array(), c.runs())
	default:
		c3 := xor(c, c2)
		if c3.N() != 0 {
			return fmt.Errorf("%d bits different between containers", c3.N())
		}
	}
	return nil
}

func unionArrayBitmap(a, b *Container) *Container {
	output := b.Clone()
	bitmap := output.bitmap()
	n := output.N()
	for _, v := range a.array() {
		if !output.bitmapContains(v) {
			bitmap[v/64] |= (1 << uint64(v%64))
			n++
		}
	}
	output.setN(n)
	return output
}

// unions array b into bitmap a, mutating a in place. The n value
// of a will need to be repaired after the fact.
func unionBitmapArrayInPlace(a, b *Container) *Container {
	a = a.Thaw()
	bitmap := a.bitmap()
	a.setDirty(true)
	for _, v := range b.array() {
		bitmap[v>>6] |= (uint64(1) << (v % 64))
	}
	return a
}

func unionBitmapBitmap(a, b *Container) *Container {
	// local variables added to prevent BCE checks in loop
	// see https://go101.org/article/bounds-check-elimination.html

	var (
		ab = a.bitmap()[:bitmapN]
		bb = b.bitmap()[:bitmapN]
		ob = make([]uint64, bitmapN)[:bitmapN]

		n int32
	)

	for i := 0; i < bitmapN; i++ {
		ob[i] = ab[i] | bb[i]
		n += int32(popcount(ob[i]))
	}

	output := NewContainerBitmapN(ob, n)
	return output
}

// unions bitmap b into bitmap a, mutating a in place. The n value of
// a will need to be repaired after the fact.
func unionBitmapBitmapInPlace(a, b *Container) *Container {
	a = a.Thaw()

	// local variables added to prevent BCE checks in loop
	// see https://go101.org/article/bounds-check-elimination.html
	var (
		ab = a.bitmap()[:bitmapN]
		bb = b.bitmap()[:bitmapN]
	)
	// Manually unroll loop to make it a little faster.
	// TODO(rartoul): Can probably make this a few x faster using
	// SIMD instructions.
	for i := 0; i < bitmapN; i += 4 {
		ab[i] |= bb[i]
		ab[i+1] |= bb[i+1]
		ab[i+2] |= bb[i+2]
		ab[i+3] |= bb[i+3]
	}
	a.setDirty(true)
	return a
}

// unions run b into run a, mutating a in place.
func unionRunRunInPlace(a, b *Container) *Container {
	statsHit("unionInPlace/RunRun")

	a = a.Thaw()
	runs, n := unionInterval16InPlace(a.runs(), b.runs())

	a.setRuns(runs)
	a.setN(n)

	if len(runs) > runMaxSize {
		a = a.runToBitmap()
	}
	return a

}

// unionInterval16InPlace merges two slice of intervals in place (in a).
// The main concept is to go value by value (instead of interval by interval)
// and count `.start` and `.last` points.
// If we get the `state == 0` it means we just built a new interval (`val`),
// and we can set it in `a` at the possition `off`
func unionInterval16InPlace(a, b []Interval16) ([]Interval16, int32) {
	n := int32(0)
	an, bn := len(a), len(b)

	var (
		// ai - index of a, aii - subindex (0: a[ai].start, 1: a[ai].last).
		ai, aii int = 0, 0

		// bi - index of b, bii - subindex (0: b[bi].start, 1: b[bi].last).
		bi, bii int = 0, 0

		// Offset of a - next available index to set.
		off int = 0
		// Value to set/append to a at off
		val Interval16

		// Current state - state equals 0 means we are clear (out of intervals)
		// When we start a new interval we add +1 when we get out of interval we add -1.
		state int

		// subindex (ii) to state mapping
		// .start: [0] -> 1
		// .last:  [1] -> -1
		iiMap = [2]int{1, -1}

		// If fromB is equal 2 it means that both val.start and val.last come from b,
		// so we need to extend a, first
		fromB int8

		// eval functions evaluates global state and value
		eval = func(arr [2]uint16, ii int, onlyB bool) {
			if state == 0 && ii == 0 {
				// we are clear and start a new interval
				val.Start = arr[ii]
				if onlyB {
					fromB++
				}
			}

			state += iiMap[ii]

			if state == 0 {
				// we just got out of interval
				// ii == 1
				val.Last = arr[ii]
				if onlyB {
					fromB++
				}
			}
		}
		// eval2 function is a special variant for eval function
		// it's only used when two interval endings are equal, e.g.:
		// a: ------------------|
		// b:        -----------|
		// the most important part is to change the global for both endings
		// before we check if we're getting out of interval and start the new one.
		eval2 = func(arr [2]uint16, i1, i2 int) {
			if state == 0 && (i1 == 0 || i2 == 0) {
				// we are clear and start a new interval
				val.Start = arr[i1]

			}

			state += iiMap[i1]
			state += iiMap[i2]

			if state == 0 {
				// (i1 == 1 || i2 == 1)
				// we just got out of interval
				val.Last = arr[i1]
			}
		}
	)

	for {
		// av, bv reflects a[ai] and b[bi] intervals as an array,
		// so we can internally iterate over values (points).
		var av, bv [2]uint16

		if ai < an && bi < bn {
			av[0], av[1] = a[ai].Start, a[ai].Last
			bv[0], bv[1] = b[bi].Start, b[bi].Last

			if av[aii] < bv[bii] {
				// a: |-------------------
				// b:           |-------------------

				eval(av, aii, false)
				aii++
			} else if av[aii] == bv[bii] {
				// a: |-------------------
				// b: |-------------------
				// or
				// a: ------------------|
				// b:                   |-------------------
				// or
				// a: ------------------|
				// b:      |------------|
				// ...

				eval2(av, aii, bii)
				aii++
				bii++
			} else { // bv[bii] < av[aii]
				// a:           |-------------------
				// b: |-------------------

				eval(bv, bii, true)
				bii++
			}
		} else if ai < an { // only a left
			av[0], av[1] = a[ai].Start, a[ai].Last
			eval(av, aii, false)
			aii++
		} else if bi < bn { // only b left
			bv[0], bv[1] = b[bi].Start, b[bi].Last
			eval(bv, bii, false)
			bii++
		} else {
			break
		}

		if state == 0 {
			if fromB == 2 {
				// val.start and val.last come from b, so we need to extend a, first
				a = append(a, Interval16{})
				copy(a[off+1:], a[off:])
				ai++
				an++
			}
			fromB = 0
			a, off = appendInterval16At(a, val, off)
			n += int32(val.Last) - int32(val.Start) + 1
		}

		if aii == 2 {
			// move to the next a's interval
			aii = 0
			ai++
		}

		if bii == 2 {
			// move to the next b's interval
			bii = 0
			bi++
		}
	}

	if len(a) > 0 {
		a = a[:off]
	}
	return a, n
}

// appendInterval16At appends or sets val in a at off position
// The function returns modified a ([]interval16) and new offset (off)
func appendInterval16At(a []Interval16, val Interval16, off int) ([]Interval16, int) {

	if off > 0 && int32(val.Start)-int32(a[off-1].Last) <= 1 {
		a[off-1].Last = val.Last
		return a, off
	}

	if off == len(a) {
		a = append(a, val)
		off++
		return a, off
	}

	a[off] = val
	off++

	return a, off
}

func difference(a, b *Container) (c *Container) {
	if roaringParanoia {
		defer func() { c.CheckN() }()
	}
	if a.N() == 0 || b.N() == MaxContainerVal+1 {
		return nil
	}
	if b.N() == 0 {
		return a.Freeze()
	}
	if a.isArray() {
		if b.isArray() {
			return differenceArrayArray(a, b)
		} else if b.isRun() {
			return differenceArrayRun(a, b)
		} else {
			return differenceArrayBitmap(a, b)
		}
	} else if a.isRun() {
		if b.isArray() {
			return differenceRunArray(a, b)
		} else if b.isRun() {
			return differenceRunRun(a, b)
		} else {
			return differenceRunBitmap(a, b)
		}
	} else {
		if b.isArray() {
			return differenceBitmapArray(a, b)
		} else if b.isRun() {
			return differenceBitmapRun(a, b)
		} else {
			return differenceBitmapBitmap(a, b)
		}
	}
}

// differenceArrayArray computes the difference bween two arrays.
func differenceArrayArray(a, b *Container) *Container {
	statsHit("difference/ArrayArray")
	output := NewContainerArray(nil)
	aa, ab := a.array(), b.array()
	na, nb := len(aa), len(ab)
	for i, j := 0, 0; i < na; {
		va := aa[i]
		if j >= nb {
			output, _ = output.add(va)
			i++
			continue
		}

		vb := ab[j]
		if va < vb {
			output, _ = output.add(va)
			i++
		} else if va > vb {
			j++
		} else {
			i, j = i+1, j+1
		}
	}
	return output
}

// differenceArrayRun computes the difference of an array from a run.
func differenceArrayRun(a, b *Container) *Container {
	statsHit("difference/ArrayRun")
	// func (ac *arrayContainer) iandNotRun16(rc *runContainer16) container {
	output := make([]uint16, 0, a.N())
	// cardinality upper bound: card(A)

	i := 0 // array index
	j := 0 // run index
	aa, rb := a.array(), b.runs()

	// handle overlap
	for i < len(aa) {

		// keep all array elements before beginning of runs
		if aa[i] < rb[j].Start {
			output = append(output, aa[i])
			i++
			continue
		}

		// if array element in run, skip it
		if aa[i] >= rb[j].Start && aa[i] <= rb[j].Last {
			i++
			continue
		}

		// if array element larger than current run, check next run
		if aa[i] > rb[j].Last {
			j++
			if j == len(rb) {
				break
			}
		}
	}

	if i < len(aa) {
		// keep all array elements after end of runs
		// It's possible that output was converted from array to bitmap in output.add()
		// so check container type before proceeding.
		output = append(output, aa[i:]...)
	}
	return NewContainerArray(output)
}

// differenceBitmapRun computes the difference of an bitmap from a run.
func differenceBitmapRun(a, b *Container) *Container {
	statsHit("difference/BitmapRun")
	output := a.Clone()
	for _, run := range b.runs() {
		output.bitmapZeroRange(uint64(run.Start), uint64(run.Last)+1)
	}
	return output
}

// differenceRunArray subtracts the bits in an array container from a run
// container.
func differenceRunArray(a, b *Container) *Container {
	statsHit("difference/RunArray")
	ra, ab := a.runs(), b.array()
	runs := make([]Interval16, 0, len(ra))

	bidx := 0
	vb := ab[bidx]

RUNLOOP:
	for _, run := range ra {
		start := run.Start
		for vb < run.Start {
			bidx++
			if bidx >= len(ab) {
				break
			}
			vb = ab[bidx]
		}
		for vb >= run.Start && vb <= run.Last {
			if vb == start {
				if vb == 65535 { // overflow
					break RUNLOOP
				}
				start++
				bidx++
				if bidx >= len(ab) {
					break
				}
				vb = ab[bidx]
				continue
			}
			runs = append(runs, Interval16{Start: start, Last: vb - 1})
			if vb == 65535 { // overflow
				break RUNLOOP
			}
			start = vb + 1
			bidx++
			if bidx >= len(ab) {
				break
			}
			vb = ab[bidx]
		}

		if start <= run.Last {
			runs = append(runs, Interval16{Start: start, Last: run.Last})
		}
	}
	output := NewContainerRun(runs)
	output = output.optimize()
	return output
}

// differenceRunBitmap computes the difference of an run from a bitmap.
func differenceRunBitmap(a, b *Container) *Container {
	statsHit("difference/RunBitmap")
	ra := a.runs()
	// If a is full, difference is the flip of b.
	if len(ra) > 0 && ra[0].Start == 0 && ra[0].Last == 65535 {
		return flipBitmap(b)
	}
	bb := b.bitmap()[:1024]
	runs := make([]Interval16, 0, len(ra))
	for _, inputRun := range ra {
		run := inputRun
		add := true
		for bit := inputRun.Start; bit <= inputRun.Last; bit++ {
			idx, exp := int(bit>>6), bit&63
			if (bb[idx]>>exp)&1 != 0 {
				if run.Start == bit {
					if bit == 65535 { //overflow
						add = false
					}

					run.Start++
				} else if bit == run.Last {
					run.Last--
				} else {
					run.Last = bit - 1
					if run.Last >= run.Start {
						if len(runs) >= runMaxSize {
							asBitmap := a.runToBitmap()
							return differenceBitmapBitmap(asBitmap, b)
						}
						runs = append(runs, run)
					}
					run.Start = bit + 1
					run.Last = inputRun.Last
				}
				if run.Start > run.Last {
					break
				}
			}

			if bit == 65535 { //overflow
				break
			}
		}
		if run.Start <= run.Last {
			if add {
				if len(runs) >= runMaxSize {
					asBitmap := a.runToBitmap()
					return differenceBitmapBitmap(asBitmap, b)
				}
				runs = append(runs, run)
			}
		}
	}

	output := NewContainerRun(runs)
	if output.N() < ArrayMaxSize && int32(len(runs)) > output.N()/2 {
		output = output.runToArray()
	} else if len(runs) > runMaxSize {
		output = output.runToBitmap()
	}
	return output
}

// differenceRunRun computes the difference of two runs.
func differenceRunRun(a, b *Container) *Container {
	statsHit("difference/RunRun")

	ra, rb := a.runs(), b.runs()
	apos := 0 // current a-run index
	bpos := 0 // current b-run index
	astart := ra[apos].Start
	alast := ra[apos].Last
	bstart := rb[bpos].Start
	blast := rb[bpos].Last
	alen := len(ra)
	blen := len(rb)

	runs := make([]Interval16, 0, alen+blen) // TODO allocate max then truncate? or something else
	// cardinality upper bound: sum of number of runs
	// each B-run could split an A-run in two, up to len(b.runs) times

	for apos < alen && bpos < blen {
		switch {
		case alast < bstart:
			// current A-run entirely precedes current B-run: keep full A-run, advance to next A-run
			runs = append(runs, Interval16{Start: astart, Last: alast})
			apos++
			if apos < alen {
				astart = ra[apos].Start
				alast = ra[apos].Last
			}
		case blast < astart:
			// current B-run entirely precedes current A-run: advance to next B-run
			bpos++
			if bpos < blen {
				bstart = rb[bpos].Start
				blast = rb[bpos].Last
			}
		default:
			// overlap
			if astart < bstart {
				runs = append(runs, Interval16{Start: astart, Last: bstart - 1})
			}
			if alast > blast {
				astart = blast + 1
			} else {
				apos++
				if apos < alen {
					astart = ra[apos].Start
					alast = ra[apos].Last
				}
			}
		}
	}
	if apos < alen {
		runs = append(runs, Interval16{Start: astart, Last: alast})
		apos++
		if apos < alen {
			runs = append(runs, ra[apos:]...)
		}
	}
	return NewContainerRun(runs)
}

func differenceArrayBitmap(a, b *Container) *Container {
	statsHit("difference/ArrayBitmap")
	output := make([]uint16, 0, a.N())
	bitmap := b.bitmap()
	for _, va := range a.array() {
		bmidx := va / 64
		bidx := va % 64
		mask := uint64(1) << bidx
		b := bitmap[bmidx]

		if mask&^b > 0 {
			output = append(output, va)
		}
	}
	return NewContainerArray(output)
}

func differenceBitmapArray(a, b *Container) *Container {
	statsHit("difference/BitmapArray")
	output := a.Clone()
	bitmap := output.bitmap()

	n := output.N()
	for _, v := range b.array() {
		if output.bitmapContains(v) {
			bitmap[v/64] &^= (uint64(1) << uint(v%64))
			n--
		}
	}
	output.setN(n)
	if n < ArrayMaxSize {
		output = output.bitmapToArray()
	}
	return output
}

func differenceBitmapBitmap(a, b *Container) *Container {
	statsHit("difference/BitmapBitmap")
	// local variables added to prevent BCE checks in loop
	// see https://go101.org/article/bounds-check-elimination.html

	var (
		ab = a.bitmap()[:bitmapN]
		bb = b.bitmap()[:bitmapN]
		ob = make([]uint64, bitmapN)[:bitmapN]

		n int32
	)

	for i := 0; i < bitmapN; i++ {
		ob[i] = ab[i] & (^bb[i])
		n += int32(popcount(ob[i]))
	}

	output := NewContainerBitmapN(ob, n)
	if output.N() < ArrayMaxSize {
		output = output.bitmapToArray()
	}
	return output
}

func xor(a, b *Container) (c *Container) {
	if roaringParanoia {
		defer func() { c.CheckN() }()
	}
	if a.N() == 0 {
		return b.Freeze()
	}
	if b.N() == 0 {
		return a.Freeze()
	}
	if a.isArray() {
		if b.isArray() {
			return xorArrayArray(a, b)
		} else if b.isRun() {
			return xorArrayRun(a, b)
		} else {
			return xorArrayBitmap(a, b)
		}
	} else if a.isRun() {
		if b.isArray() {
			return xorArrayRun(b, a)
		} else if b.isRun() {
			return xorRunRun(a, b)
		} else {
			return xorBitmapRun(b, a)
		}
	} else {
		if b.isArray() {
			return xorArrayBitmap(b, a)
		} else if b.isRun() {
			return xorBitmapRun(a, b)
		} else {
			return xorBitmapBitmap(a, b)
		}
	}
}

func xorArrayArray(a, b *Container) *Container {
	statsHit("xor/ArrayArray")
	aa, ab := a.array(), b.array()
	output := make([]uint16, len(aa)+len(ab))

	i, j, k := 0, 0, 0
	for i < len(aa) && j < len(ab) {
		va, vb := aa[i], ab[j]
		switch {
		case va < vb:
			// The a side is lower, so copy those first.
			for i < len(aa) && aa[i] < vb {
				output[k] = aa[i]
				i++
				k++
			}

		case va > vb:
			// The b side is lower, so copy those first.
			for j < len(ab) && ab[j] < va {
				output[k] = ab[j]
				j++
				k++
			}

		default:
			// Both are equal.
			// Skip them.
			i++
			j++
		}
	}
	switch {
	case i < len(aa):
		k += copy(output[k:], aa[i:])
	case j < len(ab):
		k += copy(output[k:], ab[j:])
	}

	if k == cap(output) {
		return NewContainerArray(output)
	}

	return NewContainerArrayCopy(output[:k])
}

func xorArrayBitmap(a, b *Container) *Container {
	statsHit("xor/ArrayBitmap")
	output := b.Clone()
	for _, v := range a.array() {
		if b.bitmapContains(v) {
			output, _ = output.remove(v)
		} else {
			output, _ = output.add(v)
		}
	}

	// It's possible that output was converted from bitmap to array in output.remove()
	// so we only do this conversion if output is still a bitmap container.
	if output.typ() == ContainerBitmap && output.count() < ArrayMaxSize {
		output = output.bitmapToArray()
	}

	return output
}

func xorBitmapBitmap(a, b *Container) *Container {
	statsHit("xor/BitmapBitmap")
	// local variables added to prevent BCE checks in loop
	// see https://go101.org/article/bounds-check-elimination.html

	var (
		ab = a.bitmask()
		bb = b.bitmask()
		ob = [1024]uint64{}

		n int32
	)

	_, _ = &ab[0], &bb[0]
	for i := range ob {
		ob[i] = ab[i] ^ bb[i]
		n += int32(popcount(ob[i]))
	}

	output := NewContainerBitmapN(ob[:], n)
	if n < ArrayMaxSize {
		output = output.bitmapToArray()
	}
	return output
}

// shift() shifts the contents of c by one. It returns
// the new container and a bool indicating whether a
// carry bit was shifted out.
func shift(c *Container) (*Container, bool) {
	if c.N() == 0 {
		return nil, false
	}
	if c.isArray() {
		return shiftArray(c)
	} else if c.isRun() {
		return shiftRun(c)
	}
	return shiftBitmap(c)
}

// shiftArray is an array-specific implementation of shift().
func shiftArray(a *Container) (*Container, bool) {
	statsHit("shift/Array")
	carry := false
	aa := a.array()
	output := make([]uint16, 0, len(aa))
	for _, v := range aa {
		if v+1 == 0 { // overflow
			carry = true
		} else {
			output = append(output, v+1)
		}
	}
	return NewContainerArray(output), carry
}

// shiftBitmap is a bitmap-specific implementation of shift().
func shiftBitmap(a *Container) (*Container, bool) {
	statsHit("shift/Bitmap")
	carry := uint64(0)
	output := NewContainerBitmapN(nil, 0)
	ba, bo := a.bitmap(), output.bitmap()
	lastCarry := uint64(0)
	for i, v := range ba {
		carry = v >> 63
		v = v<<1 | lastCarry
		bo[i] = v
		lastCarry = carry
	}
	output.setN(a.N() - int32(carry))
	return output, carry != 0
}

// shiftRun is a run-specific implementation of shift().
func shiftRun(a *Container) (*Container, bool) {
	statsHit("shift/Run")
	carry := false
	ra := a.runs()
	ro := make([]Interval16, 0, len(ra))

	for _, v := range ra {
		if v.Start+1 == 0 { // final run was 1 bit on container edge
			carry = true
			break
		} else if v.Last+1 == 0 { // final run ends on container edge
			v.Start++
			carry = true
		} else {
			v.Start++
			v.Last++
			carry = false
		}
		ro = append(ro, v)
	}

	return NewContainerRun(ro), carry
}

// opType represents a type of operation.
type opType uint8

const (
	opTypeAdd           = opType(0)
	opTypeRemove        = opType(1)
	opTypeAddBatch      = opType(2)
	opTypeRemoveBatch   = opType(3)
	opTypeAddRoaring    = opType(4)
	opTypeRemoveRoaring = opType(5)
)

var opTypes = []string{
	"add",
	"remove",
	"addN",
	"removeN",
	"addRoaring",
	"removeRoaring",
}

// op represents an operation on the bitmap.
type op struct {
	typ     opType
	opN     int
	value   uint64
	values  []uint64
	roaring []byte
}

// OpInfo is a description of an op.
type OpInfo struct {
	Type string
	OpN  int
	Size int
}

func (op *op) info() (info OpInfo) {
	if int(op.typ) < len(opTypes) {
		info.Type = opTypes[op.typ]
	} else {
		info.Type = fmt.Sprintf("unknown-type-%d", op.typ)
	}
	info.OpN = op.opN
	info.Size = op.size()
	return info
}

// apply executes the operation against a bitmap.
func (op *op) apply(b *Bitmap) (changed bool) {
	switch op.typ {
	case opTypeAdd:
		return b.DirectAdd(op.value)
	case opTypeRemove:
		return b.remove(op.value)
	case opTypeAddBatch:
		changed = b.DirectAddN(op.values...) > 0
	case opTypeRemoveBatch:
		changed = b.DirectRemoveN(op.values...) > 0
	case opTypeAddRoaring:
		changedN, _, _ := b.ImportRoaringBits(op.roaring, false, false, 0)
		changed = changedN != 0
	case opTypeRemoveRoaring:
		changedN, _, _ := b.ImportRoaringBits(op.roaring, true, false, 0)
		changed = changedN != 0
	default:
		panic(fmt.Sprintf("invalid op type: %d", op.typ))
	}
	return changed
}

// WriteTo writes op to the w.
func (op *op) WriteTo(w io.Writer) (n int64, err error) {
	buf := make([]byte, op.encodeSize())

	// Write type and value.
	buf[0] = byte(op.typ)
	switch op.typ {
	case opTypeAdd, opTypeRemove:
		binary.LittleEndian.PutUint64(buf[1:9], op.value)
	case opTypeAddBatch, opTypeRemoveBatch:
		binary.LittleEndian.PutUint64(buf[1:9], uint64(len(op.values)))
		p := 13 // start of values (skip 4 for checksum)
		for _, v := range op.values {
			binary.LittleEndian.PutUint64(buf[p:p+8], v)
			p += 8
		}
	case opTypeAddRoaring, opTypeRemoveRoaring:
		binary.LittleEndian.PutUint64(buf[1:9], uint64(len(op.roaring)))
		binary.LittleEndian.PutUint32(buf[13:17], uint32(op.opN))
	default:
		return 0, fmt.Errorf("can't marshal unknown op type %d", op.typ)
	}

	// Add checksum at the end.
	h := fnv.New32a()
	_, _ = h.Write(buf[0:9])
	_, _ = h.Write(buf[13:])
	if op.typ == 4 || op.typ == 5 {
		_, _ = h.Write(op.roaring)
	}
	binary.LittleEndian.PutUint32(buf[9:13], h.Sum32())

	// Write to writer.
	nn, err := w.Write(buf)
	if err != nil {
		return int64(nn), err
	}
	if op.typ == 4 || op.typ == 5 {
		var nn2 int
		// separate write so we don't have to copy the whole thing
		nn2, err = w.Write(op.roaring)
		nn += nn2
	}
	return int64(nn), err
}

var minOpSize = 13
var maxBatchSize = uint64(1 << 59)

// UnmarshalBinary decodes data into an op.
func (op *op) UnmarshalBinary(data []byte) error {
	if len(data) < minOpSize {
		return fmt.Errorf("op data out of bounds: len=%d", len(data))
	}
	statsHit("op/UnmarshalBinary")

	op.typ = opType(data[0])
	// op.value will actually contain the length of values for batch ops, or
	// length of the roaring bitmap for roaring bitmap ops
	op.value = binary.LittleEndian.Uint64(data[1:9])

	// Verify checksum.
	h := fnv.New32a()
	_, _ = h.Write(data[0:9])

	switch op.typ {
	case opTypeAdd, opTypeRemove:
		op.opN = 1
	case opTypeAddBatch, opTypeRemoveBatch:
		// This ensures that in doing 13+op.value*8, the max int won't be exceeded and a wrap around case
		// (resulting in a negative value) won't occur in the slice indexing while writing
		if op.value > maxBatchSize {
			return fmt.Errorf("maximum operation size exceeded")
		}
		if len(data) < int(13+op.value*8) {
			return fmt.Errorf("op data truncated - expected %d, got %d", 13+op.value*8, len(data))
		}
		_, _ = h.Write(data[13 : 13+op.value*8])
		op.opN = int(op.value)
		op.values = make([]uint64, op.opN)
		for i := range op.values {
			start := 13 + i*8
			op.values[i] = binary.LittleEndian.Uint64(data[start : start+8])
		}
		op.value = 0
	case opTypeAddRoaring, opTypeRemoveRoaring:
		if len(data) < int(13+4+op.value) {
			return fmt.Errorf("op data truncated - expected %d, got %d", 13+op.value, len(data))
		}
		op.opN = int(binary.LittleEndian.Uint32(data[13:17]))
		// gratuitous hack: treat any roaring write as having at least 1/8 of
		// its length in bits, even if it didn't actually change things.
		if op.opN < int(op.value/8) {
			op.opN = int(op.value / 8)
		}
		op.roaring = data[17 : 17+op.value]
		_, _ = h.Write(data[13 : 17+op.value])
		// op.value = 0
	default:
		return fmt.Errorf("unknown op type: %d", op.typ)
	}
	if chk := binary.LittleEndian.Uint32(data[9:13]); chk != h.Sum32() {
		return fmt.Errorf("checksum mismatch: type %d, exp=%08x, got=%08x", op.typ, h.Sum32(), chk)
	}

	return nil
}

// size returns the encoded size of the op, in bytes.
func (op *op) size() int {
	switch op.typ {
	case opTypeAdd, opTypeRemove:
		return 1 + 8 + 4
	case opTypeAddBatch, opTypeRemoveBatch:
		return 1 + 8 + 4 + len(op.values)*8

	case opTypeAddRoaring, opTypeRemoveRoaring:
		return 1 + 8 + 4 + 4 + len(op.roaring)
	}

	panic(fmt.Errorf("op size() called on unknown op type %d", op.typ))
}

// size returns the size needed to encode the op, in bytes. for
// roaring ops, this does not include the roaring data, which is
// already encoded.
func (op *op) encodeSize() int {
	switch op.typ {
	case opTypeAdd, opTypeRemove:
		return 1 + 8 + 4
	case opTypeAddBatch, opTypeRemoveBatch:
		return 1 + 8 + 4 + len(op.values)*8

	case opTypeAddRoaring, opTypeRemoveRoaring:
		return 1 + 8 + 4 + 4
	}

	panic(fmt.Errorf("op encodeSize() called on unknown op type %d", op.typ))
}

// count returns the number of bits the operation mutates.
func (op *op) count() int {
	switch op.typ {
	case 0, 1:
		return 1
	case 2, 3:
		return len(op.values)
	case 4, 5:
		return op.opN
	default:
		panic(fmt.Errorf("unknown operation type: %d", op.typ))
	}
}

func highbits(v uint64) uint64 { return v >> 16 }
func lowbits(v uint64) uint16  { return uint16(v & 0xFFFF) }

// search32 returns the index of value in a. If value is not found, it works the
// same way as search64.
func search32(a []uint16, value uint16) int32 {
	statsHit("search32")
	// Optimize for elements and the last element.
	n := int32(len(a))
	if n == 0 {
		return -1
	} else if a[n-1] == value {
		return n - 1
	}

	// Otherwise perform binary search for exact match.
	lo, hi := int32(0), n-1
	for lo+16 <= hi {
		i := int32(uint((lo + hi)) >> 1)
		v := a[i]

		if v < value {
			lo = i + 1
		} else if v > value {
			hi = i - 1
		} else {
			return i
		}
	}

	// If an exact match isn't found then return a negative index.
	for ; lo <= hi; lo++ {
		v := a[lo]
		if v == value {
			return lo
		} else if v > value {
			break
		}
	}
	return -(lo + 1)
}

// search64 returns the index of value in a. If value is not found, -1 * (1 +
// the index where v would be if it were inserted) is returned. This is done in
// order to both signal that value was not found (negative number), and also
// return information about where v would go if it were inserted. The +1 offset
// is necessary due to the case where v is not found, but would go at index 0.
// since negative 0 is no different from positive 0, we offset the returned
// negative indices by 1. See the test for this function for examples.
func search64(a []uint64, value uint64) int {
	statsHit("search64")
	// Optimize for elements and the last element.
	n := len(a)
	if n == 0 {
		return -1
	} else if a[n-1] == value {
		return n - 1
	}

	// Otherwise perform binary search for exact match.
	lo, hi := 0, n-1
	for lo+16 <= hi {
		i := int(uint((lo + hi)) >> 1)
		v := a[i]

		if v < value {
			lo = i + 1
		} else if v > value {
			hi = i - 1
		} else {
			return i
		}
	}

	// If an exact match isn't found then return a negative index.
	for ; lo <= hi; lo++ {
		v := a[lo]
		if v == value {
			return lo
		} else if v > value {
			break
		}
	}
	return -(lo + 1)
}

// trailingZeroN returns the number of trailing zeros in v.
// v must be greater than zero.
func trailingZeroN(v uint64) int {
	return bits.TrailingZeros64(v)
}

// ErrorList represents a list of errors.
type ErrorList []error

func (a ErrorList) Error() string {
	switch len(a) {
	case 0:
		return "no errors"
	case 1:
		return a[0].Error()
	}
	return fmt.Sprintf("%s (and %d more errors)", a[0], len(a)-1)
}

// Append appends an error to the list. If err is an ErrorList then all errors are appended.
func (a *ErrorList) Append(err error) {
	switch err := err.(type) {
	case ErrorList:
		*a = append(*a, err...)
	default:
		*a = append(*a, err)
	}
}

// AppendWithPrefix appends an error to the list and includes a prefix.
func (a *ErrorList) AppendWithPrefix(err error, prefix string) {
	switch err := err.(type) {
	case ErrorList:
		for i := range err {
			*a = append(*a, fmt.Errorf("%s%s", prefix, err[i]))
		}
	default:
		*a = append(*a, fmt.Errorf("%s%s", prefix, err))
	}
}

// xorArrayRun computes the exclusive or of an array and a run container.
func xorArrayRun(a, b *Container) *Container {
	statsHit("xor/ArrayRun")
	output := NewContainerRun(nil)
	aa, rb := a.array(), b.runs()
	na, nb := len(aa), len(rb)
	var vb Interval16
	var va uint16
	lastI, lastJ := -1, -1
	n := int32((0))
	for i, j := 0, 0; i < na || j < nb; {
		if i < na && i != lastI {
			va = aa[i]
		}
		if j < nb && j != lastJ {
			vb = rb[j]
		}
		lastI = i
		lastJ = j

		if i < na && (j >= nb || va < vb.Start) { //before
			n += output.runAppendInterval(Interval16{Start: va, Last: va})
			i++
		} else if j < nb && (i >= na || va > vb.Last) { //after
			n += output.runAppendInterval(vb)
			j++
		} else if va > vb.Start {
			if va < vb.Last {
				n += output.runAppendInterval(Interval16{Start: vb.Start, Last: va - 1})
				i++
				vb.Start = va + 1

				if vb.Start > vb.Last {
					j++
				}
			} else if va > vb.Last {
				n += output.runAppendInterval(vb)
				j++
			} else { // va == vb.last
				vb.Last--
				if vb.Start <= vb.Last {
					n += output.runAppendInterval(vb)
				}
				j++
				i++
			}

		} else { // we know va == vb.start
			if vb.Start == MaxContainerVal { // protect overflow
				j++
			} else {
				vb.Start++
				if vb.Start > vb.Last {
					j++
				}
			}
			i++
		}
	}
	output.setN(n)
	if n < ArrayMaxSize {
		output = output.runToArray()
	} else if len(output.runs()) > runMaxSize {
		output = output.runToBitmap()
	}
	return output
}

// xorCompare computes first exclusive run between two runs.
func xorCompare(x *xorstm) (r1 Interval16, hasData bool) {
	hasData = false
	if !x.vaValid || !x.vbValid {
		if x.vbValid {
			x.vbValid = false
			return x.vb, true
		}
		if x.vaValid {
			x.vaValid = false
			return x.va, true
		}
		return r1, false
	}

	if x.va.Last < x.vb.Start { //va  before
		x.vaValid = false
		r1 = x.va
		hasData = true
	} else if x.vb.Last < x.va.Start { //vb before
		x.vbValid = false
		r1 = x.vb
		hasData = true
	} else if x.va.Start == x.vb.Start && x.va.Last == x.vb.Last { // Equal
		x.vaValid = false
		x.vbValid = false
	} else if x.va.Start <= x.vb.Start && x.va.Last >= x.vb.Last { //vb inside
		x.vbValid = false
		if x.va.Start != x.vb.Start {
			r1 = Interval16{Start: x.va.Start, Last: x.vb.Start - 1}
			hasData = true
		}

		if x.vb.Last == MaxContainerVal { // Check for overflow
			x.vaValid = false

		} else {
			x.va.Start = x.vb.Last + 1
			if x.va.Start > x.va.Last {
				x.vaValid = false
			}
		}

	} else if x.vb.Start <= x.va.Start && x.vb.Last >= x.va.Last { //va inside
		x.vaValid = false
		if x.vb.Start != x.va.Start {
			r1 = Interval16{Start: x.vb.Start, Last: x.va.Start - 1}
			hasData = true
		}

		if x.va.Last == MaxContainerVal { //check for overflow
			x.vbValid = false
		} else {
			x.vb.Start = x.va.Last + 1
			if x.vb.Start > x.vb.Last {
				x.vbValid = false
			}
		}

	} else if x.va.Start < x.vb.Start && x.va.Last <= x.vb.Last { //va first overlap
		x.vaValid = false
		r1 = Interval16{Start: x.va.Start, Last: x.vb.Start - 1}
		hasData = true
		if x.va.Last == MaxContainerVal { // check for overflow
			x.vbValid = false
		} else {
			x.vb.Start = x.va.Last + 1
			if x.vb.Start > x.vb.Last {
				x.vbValid = false
			}
		}
	} else if x.vb.Start < x.va.Start && x.vb.Last <= x.va.Last { //vb first overlap
		x.vbValid = false
		r1 = Interval16{Start: x.vb.Start, Last: x.va.Start - 1}
		hasData = true

		if x.vb.Last == MaxContainerVal { // check for overflow
			x.vaValid = false
		} else {
			x.va.Start = x.vb.Last + 1
			if x.va.Start > x.va.Last {
				x.vaValid = false
			}
		}
	}
	return r1, hasData
}

// stm  is state machine used to "xor" iterate over runs.
type xorstm struct {
	vaValid, vbValid bool
	va, vb           Interval16
}

// xorRunRun computes the exclusive or of two run containers.
func xorRunRun(a, b *Container) *Container {
	statsHit("xor/RunRun")
	ra, rb := a.runs(), b.runs()
	na, nb := len(ra), len(rb)
	output := NewContainerRun(nil)

	lastI, lastJ := -1, -1

	state := &xorstm{}

	n := int32(0)
	for i, j := 0, 0; i < na || j < nb; {
		if i < na && lastI != i {
			state.va = ra[i]
			state.vaValid = true
		}

		if j < nb && lastJ != j {
			state.vb = rb[j]
			state.vbValid = true
		}
		lastI, lastJ = i, j

		r1, ok := xorCompare(state)
		if ok {
			n += output.runAppendInterval(r1)
		}
		if !state.vaValid {
			i++
		}
		if !state.vbValid {
			j++
		}

	}

	l := len(output.runs())
	output.setN(n)
	if n < ArrayMaxSize && int32(l) > n/2 {
		output = output.runToArray()
	} else if l > runMaxSize {
		output = output.runToBitmap()
	}
	return output
}

// xorRunRun computes the exclusive or of a bitmap and a run container.
func xorBitmapRun(a, b *Container) *Container {
	statsHit("xor/BitmapRun")
	output := a.Clone()

	for _, run := range b.runs() {
		output.bitmapXorRange(uint64(run.Start), uint64(run.Last)+1)
	}

	return output
}

// CompareBitmapSlice checks whether a bitmap has the same values in it
// that a provided slice does.
func CompareBitmapSlice(b *Bitmap, vals []uint64) (bool, error) {
	count := b.Count()
	if count != uint64(len(vals)) {
		return false, fmt.Errorf("length mismatch: bitmap has %d bits, slice has %d", count, len(vals))
	}
	for _, v := range vals {
		if !b.Contains(v) {
			return false, fmt.Errorf("bitmap lacks expected value %d", v)
		}
	}
	return true, nil
}

// CompareBitmapMap checks whether a bitmap has the same values in it
// that a provided map[uint64]struct{} has as keys.
func CompareBitmapMap(b *Bitmap, vals map[uint64]struct{}) (bool, error) {
	count := b.Count()
	if count != uint64(len(vals)) {
		return false, fmt.Errorf("length mismatch: bitmap has %d bits, map has %d", count, len(vals))
	}
	for v := range vals {
		if !b.Contains(v) {
			return false, fmt.Errorf("bitmap lacks expected value %d", v)
		}
	}
	return true, nil
}

// BitwiseEqual is used mostly in test cases to confirm that two bitmaps came
// out the same. It does not expect corresponding opN, or OpWriter, but expects
// identical bit contents. It does not expect identical representations; a bitmap
// container can be identical to an array container. It returns a boolean value,
// and also an explanation for a false value.
func (b *Bitmap) BitwiseEqual(c *Bitmap) (bool, error) {
	biter, _ := b.Containers.Iterator(0)
	citer, _ := c.Containers.Iterator(0)
	bn, cn := biter.Next(), citer.Next()
	var bk, ck uint64
	var bc, cc *Container
	bct, cct := 0, 0
	for bn && cn {
		bk, bc = biter.Value()
		ck, cc = citer.Value()
		// zero containers are allowed to match no-container
		if bk < ck {
			if bc.N() == 0 {
				bn = biter.Next()
				continue
			}
		}
		if ck < bk {
			if cc.N() == 0 {
				cn = citer.Next()
				continue
			}
		}
		bct++
		cct++
		if bk != ck {
			return false, fmt.Errorf("differing keys [%d vs %d]", bk, ck)
		}
		diff := xor(bc, cc)
		if diff.N() != 0 {
			return false, fmt.Errorf("differing containers for key %d: %v vs %v", bk, bc, cc)
		}
		bn, cn = biter.Next(), citer.Next()
	}
	// only one can have containers left. they should all be empty. so we
	// look at any remaining containers, break out of the loop if they're not
	// empty, and otherwise keep iterating.
	for bn {
		bn = biter.Next()
		bk, bc = biter.Value()
		if bc.N() != 0 {
			bct++
			break
		}
	}
	for cn {
		cn = citer.Next()
		ck, cc = citer.Value()
		if cc.N() != 0 {
			cct++
			break
		}
	}
	if bn {
		return false, fmt.Errorf("container mismatch: %d vs %d containers, first bitmap has extra container %d [%v bits]", bct, cct, bk, bc)
	}
	if cn {
		return false, fmt.Errorf("container mismatch: %d vs %d containers, second bitmap has extra container %d [%v bits]", bct, cct, ck, cc)
	}
	return true, nil
}

func popcount(x uint64) uint64 {
	return uint64(bits.OnesCount64(x))
}

func popcountAndSlice(s, m []uint64) uint64 {
	var (
		a = s[:bitmapN]
		b = m[:bitmapN]
	)

	cnt := uint64(0)
	for i := 0; i < bitmapN; i++ {
		cnt += popcount(a[i] & b[i])
	}
	return cnt
}

// constants from github.com/RoaringBitmap/roaring
// taken from  roaring/util.go
const (
	serialCookieNoRunContainer = 12346 // only arrays and bitmaps
	serialCookie               = 12347 // runs, arrays, and bitmaps
)

func readOfficialHeader(buf []byte) (size uint32, containerTyper func(index uint, card int) byte, header, pos int, haveRuns bool, err error) {
	statsHit("readOfficialHeader")
	if len(buf) < 8 {
		err = fmt.Errorf("buffer too small, expecting at least 8 bytes, was %d", len(buf))
		return size, containerTyper, header, pos, haveRuns, err
	}
	cf := func(index uint, card int) (newType byte) {
		newType = ContainerBitmap
		if card < ArrayMaxSize {
			newType = ContainerArray
		}
		return newType
	}
	containerTyper = cf
	cookie := binary.LittleEndian.Uint32(buf)
	pos += 4

	// cookie header
	if cookie == serialCookieNoRunContainer {
		size = binary.LittleEndian.Uint32(buf[pos:])
		pos += 4
	} else if cookie&0x0000FFFF == serialCookie {
		haveRuns = true
		size = uint32(uint16(cookie>>16) + 1) // number of containers

		// create is-run-container bitmap
		isRunBitmapSize := (int(size) + 7) / 8
		if pos+isRunBitmapSize > len(buf) {
			err = fmt.Errorf("malformed bitmap, is-run bitmap overruns buffer at %d", pos+isRunBitmapSize)
			return size, containerTyper, header, pos, haveRuns, err
		}

		isRunBitmap := buf[pos : pos+isRunBitmapSize]
		pos += isRunBitmapSize
		containerTyper = func(index uint, card int) byte {
			if isRunBitmap[index/8]&(1<<(index%8)) != 0 {
				return ContainerRun
			}
			return cf(index, card)
		}
	} else {
		err = fmt.Errorf("did not find expected serialCookie in header")
		return size, containerTyper, header, pos, haveRuns, err
	}

	header = pos
	if size > (1 << 16) {
		err = fmt.Errorf("it is logically impossible to have more than (1<<16) containers")
		return size, containerTyper, header, pos, haveRuns, err
	}

	// descriptive header
	if pos+2*2*int(size) >= len(buf) {
		err = fmt.Errorf("malformed bitmap, key-cardinality slice overruns buffer at %d", pos+2*2*int(size))
		return size, containerTyper, header, pos, haveRuns, err
	}
	pos += 2 * 2 * int(size) // moving pos past keycount
	return size, containerTyper, header, pos, haveRuns, err
}

// handledIter and handledIters are wrappers around Bitmap Container iterators
// and assist with the unionIntoTarget algorithm by abstracting away some tedious
// operations.
type handledIter struct {
	iter    ContainerIterator
	hasNext bool
	handled bool
}

type handledIters []handledIter

func (w handledIters) next() bool {
	hasNext := false

	for i, wrapped := range w {
		next := wrapped.iter.Next()
		w[i].hasNext = next
		w[i].handled = false
		if next {
			hasNext = true
		}
	}

	return hasNext
}

// Check all the iters from startIdx and up to see whether their next
// key is the given key; if it is, mark them as handled.
func (w handledIters) markItersWithKeyAsHandled(startIdx int, key uint64) {
	for i := startIdx; i < len(w); i++ {
		wrapped := w[i]
		currKey, _ := wrapped.iter.Value()
		if currKey == key {
			w[i].handled = true
		}
	}
}

func (w handledIters) calculateSummaryStats(key uint64) containerUnionSummaryStats {
	summary := containerUnionSummaryStats{}

	for _, iter := range w {
		// Calculate key-level statistics here
		currKey, currContainer := iter.iter.Value()

		if key == currKey {
			summary.c++
			summary.n += int64(currContainer.N())

			if currContainer.N() == MaxContainerVal+1 {
				summary.hasMaxRange = true
				summary.n = MaxContainerVal + 1
				return summary
			}
		}
	}

	return summary
}

// Summary statistics about all the containers in the other bitmaps
// that share the same key so we can make smarter union strategy
// decisions.
type containerUnionSummaryStats struct {
	// Estimated cardinality of the union of all containers with the same
	// key across all bitmaps. This calculation is very rough as we just sum
	// the cardinality of the container across the different bitmaps which could
	// result in very inflated values, but it allows us to avoid allocating
	// expensive bitmaps when unioning many low density containers.
	n int64
	// Containers found with this key. May be inaccurate if hasMaxRange is true.
	c int
	// Whether any of the containers with the specified keys are storing every possible
	// value that they can. If so, we can short-circuit all the unioning logic and use
	// a RLE container with a single value in it. This is an optimization to
	// avoid using an expensive bitmap container for bitmaps that have some
	// extremely dense containers.
	hasMaxRange bool
}

// DifferenceInPlace returns the bitwise difference of b and others, modifying
// b in place.
func (b *Bitmap) DifferenceInPlace(others ...*Bitmap) {
	bSize := b.Size()

	// If b doesn't have any containers then return early.
	if bSize == 0 {
		return
	}

	const staticSize = 20
	var (
		requiredSliceSize = len(others)
		// To avoid having to allocate a slice every time, if the number of bitmaps
		// being differenced is small enough (i.e. smaller than staticSize), we can
		// just use this stack-allocated array.
		staticHandledIters  = [staticSize]handledIter{}
		bitmapIters         handledIters
		target              = b
		removeContainerKeys = make([]uint64, 0, bSize)
	)

	if requiredSliceSize <= staticSize {
		bitmapIters = staticHandledIters[:0]
	} else {
		bitmapIters = make(handledIters, 0, requiredSliceSize)
	}

	for _, other := range others {
		otherIter, _ := other.Containers.Iterator(0)
		if otherIter.Next() {
			bitmapIters = append(bitmapIters, handledIter{
				iter:    otherIter,
				hasNext: true,
			})
		}
	}

	targetItr, _ := target.Containers.Iterator(0)
	// Go through all the containers and remove the other bits
	for targetItr.Next() {
		targetKey, curContainer := targetItr.Value()
		// no point in subtracting things from an empty container.
		if curContainer.N() == 0 {
			removeContainerKeys = append(removeContainerKeys, targetKey)
		}
		// Loop until every iters current value has been handled.
		for _, iIter := range bitmapIters {
			if !iIter.hasNext {
				continue
			}
			iKey, iContainer := iIter.iter.Value()
			for iKey < targetKey {
				iIter.hasNext = iIter.iter.Next()
				if iIter.hasNext {
					iKey, iContainer = iIter.iter.Value()
				} else {
					break
				}
			}
			if targetKey == iKey {
				// note: a nil container is valid, and has N == 0.
				if iContainer.N() != 0 {
					// Note: This Thaw() may be unnecessary, but some of the
					// differenceInPlace code may be assuming the container is
					// always writable.
					curContainer = curContainer.Thaw().DifferenceInPlace(iContainer)
					if curContainer.N() == 0 {
						removeContainerKeys = append(removeContainerKeys, targetKey)
						break
					} else {
						b.Containers.Put(targetKey, curContainer)
					}
				}
				iIter.hasNext = iIter.iter.Next()
			}
		}
	}

	for _, key := range removeContainerKeys {
		b.Containers.Remove(key)

	}
	target.Containers.Repair()
}

func (c *Container) DifferenceInPlace(other *Container) *Container {
	if other == nil {
		return c
	}
	if other.isArray() {
		if c.isArray() {
			return differenceArrayArrayInPlace(c, other)
		} else if c.isBitmap() {
			return differenceBitmapArrayInPlace(c, other)
		} else if c.isRun() {
			return differenceRunArrayInPlace(c, other)
		}
	} else if other.isBitmap() {
		if c.isArray() {
			return differenceArrayBitmapInPlace(c, other)
		} else if c.isBitmap() {
			return differenceBitmapBitmapInPlace(c, other)
		} else if c.isRun() {
			return differenceRunBitmapInPlace(c, other)
		}
	} else if other.isRun() {
		if c.isArray() {
			return differenceArrayRunInPlace(c, other)
		} else if c.isBitmap() {
			return differenceBitmapRunInPlace(c, other)
		} else if c.isRun() {
			return differenceRunRunInPlace(c, other)
		}
	}
	return c
}

func differenceArrayArrayInPlace(c, other *Container) *Container {
	statsHit("differenceInPlace/ArrayArray")
	aa, ab := c.array(), other.array()
	na, nb := len(aa), len(ab)
	if na == 0 || nb == 0 {
		return c
	}
	n := 0
	for i, j := 0, 0; i < na; {
		va := aa[i]
		if j >= nb {
			// nothing more to subtract; copy the remainder, bump n accordingly,
			// and be done.
			copy(aa[n:], aa[i:])
			n += len(aa) - i
			break
		}

		vb := ab[j]
		if va < vb {
			aa[n] = va
			n++
			i++
		} else if va > vb {
			j++
		} else {
			i, j = i+1, j+1
		}
	}
	aa = aa[:n]
	c.setArray(aa)
	return c
}

func differenceArrayBitmapInPlace(c, other *Container) *Container {
	statsHit("differenceInPlace/ArrayBitmap")
	aa := c.array()
	n := 0
	bitmap := other.bitmap()
	if len(aa) == 0 || len(bitmap) == 0 {
		return c
	}
	for _, va := range aa {
		bmidx := va / 64
		bidx := va % 64
		mask := uint64(1) << bidx
		b := bitmap[bmidx]

		if mask&^b > 0 {
			aa[n] = va
			n++
		}
	}
	aa = aa[:n]
	c.setArray(aa)
	return c
}

func differenceArrayRunInPlace(c, other *Container) *Container {
	statsHit("differenceInPlace/ArrayRun")

	i := 0 // array index
	j := 0 // run index
	aa, rb := c.array(), other.runs()
	if len(aa) == 0 || len(rb) == 0 {
		return c
	}
	n := 0

	// handle overlap
	for i < len(aa) {

		// keep all array elements before beginning of runs
		if aa[i] < rb[j].Start {
			aa[n] = aa[i]
			n++
			i++
			continue
		}

		// if array element in run, skip it
		if aa[i] >= rb[j].Start && aa[i] <= rb[j].Last {
			i++
			continue
		}

		// if array element larger than current run, check next run
		if aa[i] > rb[j].Last {
			j++
			if j == len(rb) {
				break
			}
		}
	}
	for ; i < len(aa); i++ {
		aa[n] = aa[i]
		n++
	}
	aa = aa[:n]
	c.setArray(aa)
	return c
}

func differenceBitmapArrayInPlace(c, other *Container) *Container {
	statsHit("differenceInPlace/BitmapArray")
	bitmap := c.bitmap()
	ab := other.array()
	if len(bitmap) == 0 || len(ab) == 0 {
		return c
	}

	n := c.N()
	for _, v := range ab {
		if c.bitmapContains(v) {
			bitmap[v/64] &^= (uint64(1) << uint(v%64))
			n--
		}
	}
	c.setN(n)
	if n < ArrayMaxSize {
		c = c.bitmapToArray() // With This Work
	}
	return c
}

func differenceBitmapBitmapInPlace(c, other *Container) *Container {
	statsHit("differenceInPlace/BitmapBitmap")
	// local variables added to prevent BCE checks in loop
	// see https://go101.org/article/bounds-check-elimination.html
	a := c.bitmap()
	b := other.bitmap()
	if len(a) == 0 || len(b) == 0 {
		return c
	}

	var (
		ab = a[:bitmapN]
		bb = b[:bitmapN]
		n  int32
	)

	for i := 0; i < bitmapN; i++ {
		ab[i] = ab[i] & (^bb[i])
		n += int32(popcount(ab[i]))
	}
	c.setN(n)
	if n < ArrayMaxSize {
		c = c.bitmapToArray()
	}
	return c
}

func differenceBitmapRunInPlace(c, other *Container) *Container {
	statsHit("differenceInPlace/BitmapRun")
	if len(c.bitmap()) == 0 {
		return c
	}
	for _, run := range other.runs() {
		c.bitmapZeroRange(uint64(run.Start), uint64(run.Last)+1)
	}
	return c
}

func differenceRunArrayInPlace(c, other *Container) *Container {
	statsHit("differenceInPlace/RunArray")
	ra, ab := c.runs(), other.array()
	if len(ra) == 0 || len(ab) == 0 {
		return c
	}
	runs := make([]Interval16, 0, len(ra))
	bidx := 0
	vb := ab[bidx]

RUNLOOP:
	for _, run := range ra {
		start := run.Start
		for vb < run.Start {
			bidx++
			if bidx >= len(ab) {
				break
			}
			vb = ab[bidx]
		}
		for vb >= run.Start && vb <= run.Last {
			if vb == start {
				if vb == 65535 { // overflow
					break RUNLOOP
				}
				start++
				bidx++
				if bidx >= len(ab) {
					break
				}
				vb = ab[bidx]
				continue
			}
			runs = append(runs, Interval16{Start: start, Last: vb - 1})
			if vb == 65535 { // overflow
				break RUNLOOP
			}
			start = vb + 1
			bidx++
			if bidx >= len(ab) {
				break
			}
			vb = ab[bidx]
		}

		if start <= run.Last {
			runs = append(runs, Interval16{Start: start, Last: run.Last})
		}
	}
	c.setRuns(runs)
	c.n = 0
	for _, run := range runs {
		c.n += int32(run.Last-run.Start) + 1
	}
	return c.optimize()
}

func differenceRunBitmapInPlace(c, other *Container) *Container {
	statsHit("differenceInPlace/RunBitmap")
	ra := c.runs()
	if len(ra) == 0 || len(other.bitmap()) == 0 {
		return c
	}
	// If a is full, difference is the flip of b.
	if len(ra) > 0 && ra[0].Start == 0 && ra[0].Last == 65535 {
		clone := other.Clone()
		bitmap := clone.bitmap()
		for i, word := range other.bitmap() {
			bitmap[i] = ^word
		}
		c.setTyp(ContainerBitmap)
		c.setMapped(false)
		c.setBitmap(bitmap)
		c.setN(c.count())
		return c
	}
	runs := make([]Interval16, 0, len(ra))
	for _, inputRun := range ra {
		run := inputRun
		add := true
		for bit := inputRun.Start; bit <= inputRun.Last; bit++ {
			if other.bitmapContains(bit) {
				if run.Start == bit {
					if bit == 65535 { //overflow
						add = false
					}

					run.Start++
				} else if bit == run.Last {
					run.Last--
				} else {
					run.Last = bit - 1
					if run.Last >= run.Start {
						runs = append(runs, run)
					}
					run.Start = bit + 1
					run.Last = inputRun.Last
				}
				if run.Start > run.Last {
					break
				}
			}

			if bit == 65535 { //overflow
				break
			}
		}
		if run.Start <= run.Last {
			if add {
				runs = append(runs, run)
			}
		}
	}

	c.setRuns(runs)
	c.n = 0
	for _, run := range runs {
		c.n += int32(run.Last-run.Start) + 1
	}
	if c.N() < ArrayMaxSize && int32(len(runs)) > c.N()/2 {
		c = c.runToArray()
	} else if len(runs) > runMaxSize {
		c = c.runToBitmap()
	}
	return c
}

func differenceRunRunInPlace(c, other *Container) *Container {
	statsHit("differenceInPlace/RunRun")

	ra, rb := c.runs(), other.runs()
	if len(ra) == 0 || len(rb) == 0 {
		return c
	}
	apos := 0 // current a-run index
	bpos := 0 // current b-run index
	astart := ra[apos].Start
	alast := ra[apos].Last
	bstart := rb[bpos].Start
	blast := rb[bpos].Last
	alen := len(ra)
	blen := len(rb)

	runs := make([]Interval16, 0, alen+blen) // TODO allocate max then truncate? or something else
	// cardinality upper bound: sum of number of runs
	// each B-run could split an A-run in two, up to len(b.runs) times

	for apos < alen && bpos < blen {
		switch {
		case alast < bstart:
			// current A-run entirely precedes current B-run: keep full A-run, advance to next A-run
			runs = append(runs, Interval16{Start: astart, Last: alast})
			apos++
			if apos < alen {
				astart = ra[apos].Start
				alast = ra[apos].Last
			}
		case blast < astart:
			// current B-run entirely precedes current A-run: advance to next B-run
			bpos++
			if bpos < blen {
				bstart = rb[bpos].Start
				blast = rb[bpos].Last
			}
		default:
			// overlap
			if astart < bstart {
				runs = append(runs, Interval16{Start: astart, Last: bstart - 1})
			}
			if alast > blast {
				astart = blast + 1
			} else {
				apos++
				if apos < alen {
					astart = ra[apos].Start
					alast = ra[apos].Last
				}
			}
		}
	}
	if apos < alen {
		runs = append(runs, Interval16{Start: astart, Last: alast})
		apos++
		if apos < alen {
			runs = append(runs, ra[apos:]...)
		}
	}
	c.setRuns(runs)
	c.n = 0
	for _, run := range runs {
		c.n += int32(run.Last-run.Start) + 1
	}
	return c
}

// Roaring encodes the bitmap in the Pilosa roaring
// format. Convenience wrapper around WriteTo.
func (b *Bitmap) Roaring() []byte {
	buf := &bytes.Buffer{}
	_, err := b.WriteTo(buf)
	if err != nil {
		panic(err) // I don't believe this can happen when writing to a bytes.Buffer
	}
	return buf.Bytes()
}

//RBF exports to be reconsidered as we progress

func (b *Bitmap) Put(key uint64, c *Container) {
	b.Containers.Put(key, c)
}

func AsBitmap(c *Container) []uint64 {
	return c.bitmap()
}
func AsArray(c *Container) []uint16 {
	return c.array()
}
func ContainerType(c *Container) byte {
	return c.typ()
}

func AsRuns(c *Container) []Interval16 {
	return c.runs()
}

func ConvertArrayToBitmap(c *Container) *Container {
	return c.arrayToBitmap()
}
func ConvertRunToBitmap(c *Container) *Container {
	return c.runToBitmap()
}

// Optimize yields a container with the same bits as c, but
// adjusted to the smallest-storage type by Roaring rules (thus,
// runs where that's smaller, otherwise arrays for N < 4096 and
// bitmaps for N >= 4096).
func Optimize(c *Container) *Container {
	return c.optimize()
}

func Union(a, b *Container) (c *Container) {
	c = union(a, b)
	// c can be have arrays that are too big, and need
	// to be optimized into raw bitmaps.
	return c.optimize()
}

func Difference(a, b *Container) *Container {
	return difference(a, b)
}

func Intersect(x, y *Container) *Container {
	return intersect(x, y)
}

func IntersectionCount(x, y *Container) int32 {
	return intersectionCount(x, y)
}

// Add yields a container identical to c, but with the given bit set; added
// is true if the bit wasn't previously set. It is unspecified whether
// the original container is modified.
func (c *Container) Add(v uint16) (newC *Container, added bool) {
	return c.add(v)
}

// Add yields a container identical to c, but with the given bit cleared;
// removed is true if the bit was previously set. It is unspecified whether
// the original container is modified.
func (c *Container) Remove(v uint16) (c2 *Container, removed bool) {
	return c.remove(v)
}

func (c *Container) Max() uint16 {
	return c.max()
}

func (c *Container) CountRange(start, end int32) (n int32) {
	return c.countRange(start, end)
}

// UnionInPlace yields a container containing all the bits set in
// either c or other. It may, or may not, modify c. The resulting
// container's count, as returned by c.N(), may be incorrect; see
// (*Container).Repair().  Do not freeze a container produced by this
// operation before repairing it.  We don't want this to call repair
// immediately because it can be faster for Bitmap.UnionInPlace to do
// it all at once after potentially many Container.UnionInPlace calls.
func (c *Container) UnionInPlace(other *Container) (r *Container) {
	return c.unionInPlace(other)
}

func (c *Container) Difference(other *Container) *Container {
	return difference(c, other)
}

// Slice returns an array of the values in the container as uint16.
// Do NOT modify the result; it could be the container's actual storage.
func (c *Container) Slice() (r []uint16) {
	if c == nil {
		return r
	}
	switch c.typ() {
	case ContainerArray:
		r = c.array()
	case ContainerBitmap:
		r = make([]uint16, c.N())
		n := int32(0)
		for i, word := range c.bitmap() {
			for word != 0 {
				t := word & -word
				if roaringParanoia {
					if n >= c.N() {
						panic("bitmap has more bits set than container.n")
					}
				}
				r[n] = uint16((i*64 + int(popcount(t-1))))
				n++
				word ^= t
			}
		}
	case ContainerRun:
		r = make([]uint16, c.N())
		n := 0
		for _, run := range c.runs() {
			for v := int(run.Start); v <= int(run.Last); v++ {
				r[n] = uint16(v)
				n++
			}
		}
	}
	return r
}

func fromArray16(a []uint16) []byte {
	return (*[8192]byte)(unsafe.Pointer(&a[0]))[: len(a)*2 : len(a)*2]
}
func fromArray64(a []uint64) []byte {
	return (*[8192]byte)(unsafe.Pointer(&a[0]))[:8192:8192]
}
func fromInterval16(a []Interval16) []byte {
	return (*[8192]byte)(unsafe.Pointer(&a[0]))[: len(a)*4 : len(a)*4]
}
