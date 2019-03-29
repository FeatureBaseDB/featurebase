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

// Package roaring implements roaring bitmaps with support for incremental changes.
package roaring

import (
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

	// storageVersion indicates the storage version, in bytes 2-3.
	storageVersion = uint32(0)

	// cookie is the first four bytes in a roaring bitmap file,
	// formed by joining MagicNumber and storageVersion
	cookie = MagicNumber + storageVersion<<16

	// headerBaseSize is the size in bytes of the cookie and key count at the
	// beginning of a file.
	headerBaseSize = 4 + 4

	// runCountHeaderSize is the size in bytes of the run count stored
	// at the beginning of every serialized run container.
	runCountHeaderSize = 2

	// interval16Size is the size of a single run in a container.runs.
	interval16Size = 4

	// bitmapN is the number of values in a container.bitmap.
	bitmapN = (1 << 16) / 64

	maxContainerVal = 0xffff

	// maxContainerKey is the key representing the last container in a full row.
	// It is the full bitmap space (2^64) divided by container width (2^16).
	maxContainerKey = (1 << 48) - 1
)

const (
	containerArray  byte = iota + 1 // slice of bit position values
	containerBitmap                 // slice of 1024 uint64s
	containerRun                    // container of run-encoded bits
)

// map used for a more descriptive print
var containerTypeNames = map[byte]string{
	containerArray:  "array",
	containerBitmap: "bitmap",
	containerRun:    "run",
}

type Containers interface {
	// Get returns nil if the key does not exist.
	Get(key uint64) *Container

	// Put adds the container at key.
	Put(key uint64, c *Container)

	// PutContainerValues updates an existing container at key.
	// If a container does not exist for key, a new one is allocated.
	// TODO(2.0) make n  int32
	PutContainerValues(key uint64, typ byte, n int, mapped bool)

	// Remove takes the container at key out.
	Remove(key uint64)

	// GetOrCreate returns the container at key, creating a new empty container if necessary.
	GetOrCreate(key uint64) *Container

	// Clone does a deep copy of Containers, including cloning all containers contained.
	Clone() Containers

	// Last returns the highest key and associated container.
	Last() (key uint64, c *Container)

	// Size returns the number of containers stored.
	Size() int

	// Iterator returns a Contiterator which after a call to Next(), a call to Value() will
	// return the first container at or after key. found will be true if a
	// container is found at key.
	Iterator(key uint64) (citer ContainerIterator, found bool)

	Count() uint64

	// Reset clears the containers collection to allow for recycling during snapshot
	Reset()

	// Repair will repair the cardinality of any containers whose cardinality were corrupted
	// due to optimized operations.
	Repair()
}

type ContainerIterator interface {
	Next() bool
	Value() (uint64, *Container)
}

// Bitmap represents a roaring bitmap.
type Bitmap struct {
	Containers Containers

	// Number of bit change operations written to the writer. Some operations
	// contain multiple values, each of those counts the number of values rather
	// than counting as one operation.
	opN int

	// Writer where operations are appended to.
	OpWriter io.Writer
}

// NewBitmap returns a Bitmap with an initial set of values.
func NewBitmap(a ...uint64) *Bitmap {
	b := &Bitmap{
		Containers: newSliceContainers(),
	}
	// TODO: We have no way to report this. We aren't in a server context
	// so we haven't got a logger, nothing is checking for nil returns
	// from this...
	_, _ = b.AddN(a...)
	return b
}

// NewFileBitmap returns a Bitmap with an initial set of values, used for file storage.
// By default, this is a copy of NewBitmap, but is replaced with B+Tree in server/enterprise.go
var NewFileBitmap func(a ...uint64) *Bitmap = NewBTreeBitmap

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
func (b *Bitmap) AddN(a ...uint64) (changed int, err error) {
	if len(a) == 0 {
		return 0, nil
	}

	changed = b.DirectAddN(a...) // modifies a in-place

	if b.OpWriter != nil {
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
func (b *Bitmap) directOpN(op func(c *Container, v uint16) bool, a ...uint64) (changed int) {
	hb := uint64(0xFFFFFFFFFFFFFFFF) // impossible sentinel value
	var cont *Container
	for _, v := range a {
		if newhb := highbits(v); newhb != hb {
			hb = newhb
			cont = b.Containers.GetOrCreate(hb)
		}
		if op(cont, lowbits(v)) {
			a[changed] = v
			changed++
		}
	}
	return changed
}

// DirectAdd adds a value to the bitmap by bypassing the op log. TODO(2.0)
// deprecate in favor of DirectAddN.
func (b *Bitmap) DirectAdd(v uint64) bool {
	cont := b.Containers.GetOrCreate(highbits(v))
	return cont.add(lowbits(v))
}

// Contains returns true if v is in the bitmap.
func (b *Bitmap) Contains(v uint64) bool {
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

	if b.OpWriter != nil {
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
	if c == nil {
		return false
	}
	// TODO - do nil check inside c.remove?
	return c.remove(lowbits(v))
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

// Any returns "b.Count() > 0"... but faster than doing that.
func (b *Bitmap) Any() bool {
	iter, _ := b.Containers.Iterator(0)
	// TODO (jaffee) I'm not sure if it's possible/legal to have an empty
	// container, so this loop may be totally unnecessary. In theory, any empty
	// container should be removed from the bitmap though.
	for iter.Next() {
		_, c := iter.Value()
		if c.n > 0 {
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
		if k == skey {
			n += uint64(c.countRange(int32(lowbits(start)), maxContainerVal+1))
			continue
		}
		if k < ekey {
			n += uint64(c.n)
			continue
		}
		if k == ekey {
			n += uint64(c.countRange(0, int32(lowbits(end))))
			break
		}
		if k > ekey {
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
	var a []uint64
	itr := b.Iterator()
	itr.Seek(start)
	for v, eof := itr.Next(); !eof && v < end; v, eof = itr.Next() {
		a = append(a, v)
	}
	return a
}

// ForEach executes fn for each value in the bitmap.
func (b *Bitmap) ForEach(fn func(uint64)) {
	itr := b.Iterator()
	itr.Seek(0)
	for v, eof := itr.Next(); !eof; v, eof = itr.Next() {
		fn(v)
	}
}

// ForEachRange executes fn for each value in the bitmap between [start, end).
func (b *Bitmap) ForEachRange(start, end uint64, fn func(uint64)) {
	itr := b.Iterator()
	itr.Seek(start)
	for v, eof := itr.Next(); !eof && v < end; v, eof = itr.Next() {
		fn(v)
	}
}

// OffsetRange returns a new bitmap with a containers offset by start.
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
	other := NewBitmap()
	for citer.Next() {
		k, c := citer.Value()
		if k >= hi1 {
			break
		}
		other.Containers.Put(off+(k-hi0), c)
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
			output.Containers.Put(ki, intersect(ci, cj))
			i, j = iiter.Next(), jiter.Next()
			ki, ci = iiter.Value()
			kj, cj = jiter.Value()
		}
	}
	return output
}

// Union returns the bitwise union of b and others as a new bitmap.
func (b *Bitmap) Union(others ...*Bitmap) *Bitmap {
	if len(others) == 1 {
		output := NewBitmap()
		b.unionIntoTargetSingle(output, others[0])
		return output
	}
	output := b.Clone()
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
			target.Containers.Put(ki, ci.Clone())
			i = iiter.Next()
			ki, ci = iiter.Value()
		} else if j && (!i || ki > kj) {
			target.Containers.Put(kj, cj.Clone())
			j = jiter.Next()
			kj, cj = jiter.Value()
		} else { // ki == kj
			target.Containers.Put(ki, union(ci, cj))
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
//          ----------------------------      |      ----------------------------      |      ----------------------------
// Bitmap 1 |___X____________X__________|     |      |___X____________X__________|     |      |___X____________X__________|
//              ^                             |          _                             |
//          ----------------------------      |      ----------------------------      |      ----------------------------
// Bitmap 2 |_______X________X______X___|     |      |_______X_______________X___|     |      |_______X_______________X___|
//                  ^                         |              ^                         |
//          ----------------------------      |      ----------------------------      |      ----------------------------
// Bitmap 3 |_______X___________________|     |      |_______X___________________|     |      |_______X___________________|
//                  ^                         |              ^                         |
//          ----------------------------      |      ----------------------------      |      ----------------------------
// Bitmap 4 |___X_______________________|     |      |___X_______________________|     |      |___X_______________________|
//              ^                             |          _                             |
// ------------------------------------------------------------------------------------------------------------------------
//          ----------------------------      |      ----------------------------      |      ----------------------------
// Bitmap 1 |___X____________X__________|     |      |___X____________X__________|     |      |___X____________X__________|
//              _                             |                       ^                |                       _
//          ----------------------------      |      ----------------------------      |      ----------------------------
// Bitmap 2 |_______X_______________X___|     |      |_______X_______________X___|     |      |_______X_______________X___|
//                  _                         |                              ^         |                              ^
//          ----------------------------      |      ----------------------------      |      ----------------------------
// Bitmap 3 |_______X___________________|     |      |_______X___________________|     |      |_______X___________________|
//                  _                         |                                        |
//          ----------------------------      |      ----------------------------      |      ----------------------------
// Bitmap 4 |___X_______________________|     |      |___X_______________________|     |      |___X_______________________|
//              _
func (b *Bitmap) unionInPlace(others ...*Bitmap) {
	var (
		requiredSliceSize = len(others)
		// To avoid having to allocate a slice everytime, if the number of bitmaps
		// being unioned is small enough we can just use this stack-allocated array.
		staticHandledIters = [20]handledIter{}
		bitmapIters        handledIters
		target             = b
	)

	if requiredSliceSize <= 20 {
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
				if tContainer.n == maxContainerVal+1 {
					bitmapIters.markItersWithKeyAsHandled(i, iKey)
					continue
				}
				expectedN = int64(tContainer.n)
			}
			// Check i and later iters for any max-range containers, and
			// find out how many there are.
			summaryStats := bitmapIters[i:].calculateSummaryStats(iKey)
			if summaryStats.hasMaxRange {
				// One (or more) of the containers represented the maximum possible
				// range that a container can store, so instead of calculating a
				// union we can generate an RLE container that represents the entire
				// range.
				tContainer = NewContainerRun([]interval16{{start: 0, last: maxContainerVal}})
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
					// can just clone it instead of unioning.
					statsHit("unionInPlace/reuse")
					target.Containers.Put(iKey, iContainer.Clone())
					bitmapIters[i].handled = true
					continue
				}
				// We have at least two other containers. We can union
				// everything together. We can union everything but
				// the first other container into a clone of the
				// first other container, but for some cases, that will
				// result in cloning a non-bitmap, then converting it
				// to a bitmap, and this will be expensive...
				if expectedN >= 512 && iContainer.typ != containerBitmap {
					// copying the non-bitmap, then converting it,
					// is expensive.
					statsHit("unionInPlace/newBitmap")
					tContainer = NewContainerBitmap(0, nil)
					itersToUnion = bitmapIters[i:]
				} else {
					// either N will be small or iContainer is a
					// bitmap, so we can skip one union op by copying it.
					statsHit("unionInPlace/clone")
					tContainer = iContainer.Clone()
					itersToUnion = bitmapIters[i+1:]
				}
			} else {
				// we have an existing target container. If we're
				// going to end up wanting it to be a bitmap, we
				// convert it preemptively, because union into a
				// bitmap is nearly always faster.
				itersToUnion = bitmapIters[i:]
				if expectedN >= 512 && tContainer.typ != containerBitmap {
					statsHit("unionInPlace/convertToBitmap")
					switch tContainer.typ {
					case containerArray:
						tContainer.arrayToBitmap()
					case containerRun:
						tContainer.runToBitmap()
					}
				}
			}

			// Now we union all remaining containers with this key
			// together.
			for j, iter := range itersToUnion {
				jKey, jContainer := iter.iter.Value()

				if iKey == jKey {
					tContainer.unionInPlace(jContainer)
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
func (b *Bitmap) Difference(other *Bitmap) *Bitmap {
	output := NewBitmap()

	iiter, _ := b.Containers.Iterator(0)
	jiter, _ := other.Containers.Iterator(0)
	i, j := iiter.Next(), jiter.Next()
	ki, ci := iiter.Value()
	kj, cj := jiter.Value()
	for i || j {
		if i && (!j || ki < kj) {
			output.Containers.Put(ki, ci.Clone())
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
			output.Containers.Put(ki, ci.Clone())
			i = iiter.Next()
			ki, ci = iiter.Value()
		} else if j && (!i || ki > kj) {
			output.Containers.Put(kj, cj.Clone())
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
		o, carry := shift(ci)
		if lastCarry {
			o.add(0)
		}
		if o.n > 0 {
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
		if c.n == 0 {
			b.Containers.Remove(k)
		}
	}
}
func (b *Bitmap) countEmptyContainers() int {
	result := 0
	citer, _ := b.Containers.Iterator(0)
	for citer.Next() {
		_, c := citer.Value()
		if c.n == 0 {
			result++
		}
	}
	return result
}

// Optimize converts array and bitmap containers to run containers as necessary.
func (b *Bitmap) Optimize() {
	citer, _ := b.Containers.Iterator(0)
	for citer.Next() {
		_, c := citer.Value()
		c.optimize()
	}
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

	containerCount := b.Containers.Size() - b.countEmptyContainers()
	headerSize := headerBaseSize
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

	ew.WriteUint32(byte4, cookie)
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
		if c.n > 0 {
			ew.WriteUint64(byte8, key)
			ew.WriteUint16(byte2, uint16(c.typ))
			ew.WriteUint16(byte2, uint16(c.n-1))
		}

	}

	// Offset header section: write the offset for each container block.
	// 4 bytes per container.
	offset := uint32(headerSize + (containerCount * (8 + 2 + 2 + 4)))
	citer, _ = b.Containers.Iterator(0)
	for citer.Next() {
		_, c := citer.Value()
		if c.n > 0 {
			ew.WriteUint32(byte4, offset)
			offset += uint32(c.size())
		}

	}
	if ew.err != nil {
		return int64(ew.n), ew.err
	}

	n = int64(headerSize + (containerCount * (8 + 2 + 2 + 4)))

	// Container storage section: write each container block.
	citer, _ = b.Containers.Iterator(0)
	for citer.Next() {
		_, c := citer.Value()
		if c.n > 0 {
			nn, err := c.WriteTo(w)
			n += nn
			if err != nil {
				return n, err
			}
		}
	}
	return n, nil
}

// unmarshalPilosaRoaring treats data as being encoded in Pilosa's 64 bit
// roaring format and decodes it into b.
func (b *Bitmap) unmarshalPilosaRoaring(data []byte) error {
	if len(data) < headerBaseSize {
		return errors.New("data too small")
	}

	// Verify the first two bytes are a valid MagicNumber, and second two bytes match current storageVersion.
	fileMagic := uint32(binary.LittleEndian.Uint16(data[0:2]))
	fileVersion := uint32(binary.LittleEndian.Uint16(data[2:4]))
	if fileMagic != MagicNumber {
		return fmt.Errorf("invalid roaring file, magic number %v is incorrect", fileMagic)
	}

	if fileVersion != storageVersion {
		return fmt.Errorf("wrong roaring version, file is v%d, server requires v%d", fileVersion, storageVersion)
	}

	// Read key count in bytes sizeof(cookie):(sizeof(cookie)+sizeof(uint32)).
	keyN := binary.LittleEndian.Uint32(data[4:8])

	headerSize := headerBaseSize
	b.Containers.Reset()
	// Descriptive header section: Read container keys and cardinalities.
	for i, buf := 0, data[headerSize:]; i < int(keyN); i, buf = i+1, buf[12:] {
		b.Containers.PutContainerValues(
			binary.LittleEndian.Uint64(buf[0:8]),
			byte(binary.LittleEndian.Uint16(buf[8:10])),
			int(binary.LittleEndian.Uint16(buf[10:12]))+1,
			true)
	}
	opsOffset := headerSize + int(keyN)*12

	// Read container offsets and attach data.
	citer, _ := b.Containers.Iterator(0)
	for i, buf := 0, data[opsOffset:]; i < int(keyN); i, buf = i+1, buf[4:] {
		offset := binary.LittleEndian.Uint32(buf[0:4])
		// Verify the offset is within the bounds of the input data.
		if int(offset) >= len(data) {
			return fmt.Errorf("offset out of bounds: off=%d, len=%d", offset, len(data))
		}

		// Map byte slice directly to the container data.
		citer.Next()
		_, c := citer.Value()
		switch c.typ {
		case containerRun:
			runCount := binary.LittleEndian.Uint16(data[offset : offset+runCountHeaderSize])
			c.setRuns((*[0xFFFFFFF]interval16)(unsafe.Pointer(&data[offset+runCountHeaderSize]))[:runCount:runCount])
			opsOffset = int(offset) + runCountHeaderSize + len(c.runs())*interval16Size
		case containerArray:
			c.setArray((*[0xFFFFFFF]uint16)(unsafe.Pointer(&data[offset]))[:c.n:c.n])
			opsOffset = int(offset) + len(c.array())*2 // sizeof(uint32)
		case containerBitmap:
			c.setBitmap((*[0xFFFFFFF]uint64)(unsafe.Pointer(&data[offset]))[:bitmapN:bitmapN])
			opsOffset = int(offset) + len(c.bitmap())*8 // sizeof(uint64)
		}
	}

	// Read ops log until the end of the file.
	buf := data[opsOffset:]
	for {
		// Exit when there are no more ops to parse.
		if len(buf) == 0 {
			break
		}

		// Unmarshal the op and apply it.
		var opr op
		if err := opr.UnmarshalBinary(buf); err != nil {
			// FIXME(benbjohnson): return error with position so file can be trimmed.
			return err
		}

		opr.apply(b)

		// Increase the op count.
		b.opN += opr.count()

		// Move the buffer forward.
		buf = buf[opr.size():]
	}

	return nil
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

	return nil
}

// Iterator returns a new iterator for the bitmap.
func (b *Bitmap) Iterator() *Iterator {
	itr := &Iterator{bitmap: b}
	itr.Seek(0)
	return itr
}

// Info returns stats for the bitmap.
func (b *Bitmap) Info() bitmapInfo {
	info := bitmapInfo{
		OpN:        b.opN,
		Containers: make([]containerInfo, 0, b.Containers.Size()),
	}

	citer, _ := b.Containers.Iterator(0)
	for citer.Next() {
		k, c := citer.Value()
		ci := c.info()
		ci.Key = k
		info.Containers = append(info.Containers, ci)
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

// bitmapInfo represents a point-in-time snapshot of bitmap stats.
type bitmapInfo struct {
	OpN        int
	Containers []containerInfo
}

// Iterator represents an iterator over a Bitmap.
type Iterator struct {
	bitmap *Bitmap
	citer  ContainerIterator
	key    uint64
	c      *Container
	j, k   int32 // i: container; j: array index, bit index, or run index; k: offset within the run
}

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

	// Move to the correct value index inside the container.
	lb := lowbits(seek)
	if itr.c.isArray() {
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
		if seek == 0 {
			itr.j, itr.k = 0, -1
		}

		j, contains := binSearchRuns(lb, itr.c.runs())
		if contains {
			itr.j = j
			itr.k = int32(lb) - int32(itr.c.runs()[j].start) - 1
		} else {
			// Set iterator to next value in the Bitmap.
			itr.j = j
			itr.k = -1
		}

		return
	}

	// If it's a bitmap container then move to index before the value and call next().
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
			if itr.j >= itr.c.n-1 {
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
			runLength := int32(r.last - r.start)

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
		return itr.key<<16 | uint64(itr.c.runs()[itr.j].start+uint16(itr.k))
	}
	return itr.key<<16 | uint64(itr.j)
}

// ArrayMaxSize represents the maximum size of array containers.
const ArrayMaxSize = 4096

// runMaxSize represents the maximum size of run length encoded containers.
const runMaxSize = 2048

type interval16 struct {
	start uint16
	last  uint16
}

// runlen returns the count of integers in the interval.
func (iv interval16) runlen() int32 {
	return 1 + int32(iv.last-iv.start)
}

// count counts all bits in the container.
func (c *Container) count() (n int32) {
	return c.countRange(0, maxContainerVal+1)
}

// countRange counts the number of bits set between [start, end).
func (c *Container) countRange(start, end int32) (n int32) {
	if c.isArray() {
		return c.arrayCountRange(start, end)
	} else if c.isRun() {
		return c.runCountRange(start, end)
	}
	return c.bitmapCountRange(start, end)
}

func (c *Container) arrayCountRange(start, end int32) (n int32) {
	array := c.array()
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

func (c *Container) bitmapCountRange(start, end int32) int32 {
	var n uint64
	i, j := start/64, end/64
	// Special case when start and end fall in the same word.
	bitmap := c.bitmap()
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

func (c *Container) runCountRange(start, end int32) (n int32) {
	runs := c.runs()
	for _, iv := range runs {
		// iv is before range
		if int32(iv.last) < start {
			continue
		}
		// iv is after range
		if end < int32(iv.start) {
			break
		}
		// iv is superset of range
		if int32(iv.start) < start && int32(iv.last) > end {
			return end - start
		}
		// iv is subset of range
		if int32(iv.start) >= start && int32(iv.last) < end {
			n += iv.runlen()
		}
		// iv overlaps beginning of range
		if int32(iv.start) < start && int32(iv.last) < end {
			n += int32(iv.last) - start + 1
		}
		// iv overlaps end of range
		if int32(iv.start) > start && int32(iv.last) >= end {
			n += end - int32(iv.start)
		}
	}
	return n
}

// add adds a value to the container.
func (c *Container) add(v uint16) (added bool) {

	if c.isArray() {
		added = c.arrayAdd(v)
	} else if c.isRun() {
		added = c.runAdd(v)
	} else {
		added = c.bitmapAdd(v)
	}
	if added {
		c.n++
	}
	return added
}

func (c *Container) arrayAdd(v uint16) bool {
	// Optimize appending to the end of an array container.
	array := c.array()
	if c.n > 0 && c.n < ArrayMaxSize && c.isArray() && array[c.n-1] < v {
		statsHit("arrayAdd/append")
		c.unmapArray()
		array = append(c.array(), v)
		c.setArray(array)
		return true
	}

	// Find index of the integer in the container. Exit if it already exists.
	i := search32(array, v)
	if i >= 0 {
		return false
	}

	// Convert to a bitmap container if too many values are in an array container.
	if c.n >= ArrayMaxSize {
		statsHit("arrayAdd/arrayToBitmap")
		c.arrayToBitmap()
		return c.bitmapAdd(v)
	}

	// Otherwise insert into array.
	statsHit("arrayAdd/insert")
	c.unmapArray()
	i = -i - 1
	array = append(c.array(), 0)
	copy(array[i+1:], array[i:])
	array[i] = v
	c.setArray(array)
	return true

}

func (c *Container) bitmapAdd(v uint16) bool {
	if c.bitmapContains(v) {
		return false
	}
	c.unmapBitmap()
	c.bitmap()[v/64] |= (1 << uint64(v%64))
	return true
}

func (c *Container) runAdd(v uint16) bool {
	runs := c.runs()

	if len(runs) == 0 {
		c.unmapRun()
		c.setRuns([]interval16{{start: v, last: v}})
		return true
	}

	i := sort.Search(len(runs),
		func(i int) bool { return runs[i].last >= v })

	if i == len(runs) {
		i--
	}

	iv := runs[i]
	if v >= iv.start && iv.last >= v {
		return false
	}

	c.unmapRun()
	runs = c.runs()
	if iv.last < v {
		if iv.last == v-1 {
			runs[i].last++
		} else {
			runs = append(runs, interval16{start: v, last: v})
		}
	} else if v+1 == iv.start {
		// combining two intervals
		if i > 0 && runs[i-1].last == v-1 {
			runs[i-1].last = iv.last
			runs = append(runs[:i], runs[i+1:]...)
			c.setRuns(runs)
			return true
		}
		// just before an interval
		runs[i].start--
	} else if i > 0 && v-1 == runs[i-1].last {
		// just after an interval
		runs[i-1].last++
	} else {
		// alone
		newIv := interval16{start: v, last: v}
		runs = append(runs[:i], append([]interval16{newIv}, runs[i:]...)...)
	}
	c.setRuns(runs)
	return true
}

// Contains returns true if v is in the container.
func (c *Container) Contains(v uint16) bool {
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
func (c *Container) optimize() {
	if c.n == 0 {
		statsHit("optimize/empty")
		return
	}
	runs := c.countRuns()

	var newType byte
	if runs <= runMaxSize && runs <= c.n/2 {
		newType = containerRun
	} else if c.n < ArrayMaxSize {
		newType = containerArray
	} else {
		newType = containerBitmap
	}

	// Then convert accordingly.
	if c.isArray() {
		if newType == containerBitmap {
			statsHit("optimize/arrayToBitmap")
			c.arrayToBitmap()
		} else if newType == containerRun {
			statsHit("optimize/arrayToRun")
			c.arrayToRun(runs)
		} else {
			statsHit("optimize/arrayUnchanged")
		}
	} else if c.isBitmap() {
		if newType == containerArray {
			statsHit("optimize/bitmapToArray")
			c.bitmapToArray()
		} else if newType == containerRun {
			statsHit("optimize/bitmapToRun")
			c.bitmapToRun(runs)
		} else {
			statsHit("optimize/bitmapUnchanged")
		}
	} else if c.isRun() {
		if newType == containerBitmap {
			statsHit("optimize/runToBitmap")
			c.runToBitmap()
		} else if newType == containerArray {
			statsHit("optimize/runToArray")
			c.runToArray()
		} else {
			statsHit("optimize/runUnchanged")
		}
	}
}

// unionInPlace does not necessarily preserve container's N; it's expected
// to be used when running a sequence of unions, after which you should
// call Repair(). (As of this writing, that only matters for bitmaps.)
func (c *Container) unionInPlace(other *Container) {
	switch c.typ {
	case containerBitmap:
		switch other.typ {
		case containerBitmap:
			unionBitmapBitmapInPlace(c, other)
		case containerArray:
			unionBitmapArrayInPlace(c, other)
		case containerRun:
			unionBitmapRunInPlace(c, other)

		}
	case containerArray:
		switch other.typ {
		case containerBitmap:
			c.arrayToBitmap()
			unionBitmapBitmapInPlace(c, other)
		case containerArray:
			unionArrayArrayInPlace(c, other)
		case containerRun:
			c.arrayToBitmap()
			unionBitmapRunInPlace(c, other)
		}
	case containerRun:
		switch other.typ {
		case containerBitmap:
			c.runToBitmap()
			unionBitmapBitmapInPlace(c, other)
		case containerArray:
			c.runToBitmap()
			unionBitmapArrayInPlace(c, other)
		case containerRun:
			c.runToBitmap()
			unionBitmapRunInPlace(c, other)
		}
	}
}

func (c *Container) arrayContains(v uint16) bool {
	return search32(c.array(), v) >= 0
}

func (c *Container) bitmapContains(v uint16) bool {
	return (c.bitmap()[v/64] & (1 << uint64(v%64))) != 0
}

// binSearchRuns returns the index of the run containing v, and true, when v is contained;
// or the index of the next run starting after v, and false, when v is not contained.
func binSearchRuns(v uint16, a []interval16) (int32, bool) {
	i := int32(sort.Search(len(a),
		func(i int) bool { return a[i].last >= v }))
	if i < int32(len(a)) {
		return i, (v >= a[i].start) && (v <= a[i].last)
	}

	return i, false
}

// runContains determines if v is in the container assuming c is a run
// container.
func (c *Container) runContains(v uint16) bool {
	_, found := binSearchRuns(v, c.runs())
	return found
}

// remove removes a value from the container.
func (c *Container) remove(v uint16) (removed bool) {
	if c.isArray() {
		removed = c.arrayRemove(v)
	} else if c.isRun() {
		removed = c.runRemove(v)
	} else {
		removed = c.bitmapRemove(v)
	}
	return removed
}

func (c *Container) arrayRemove(v uint16) bool {
	array := c.array()
	i := search32(array, v)
	if i < 0 {
		return false
	}
	c.unmapArray()
	array = c.array()

	array = append(array[:i], array[i+1:]...)
	c.n--
	c.setArray(array)
	return true
}

func (c *Container) bitmapRemove(v uint16) bool {
	if !c.bitmapContains(v) {
		return false
	}
	c.unmapBitmap()

	// Lower count and remove element.
	c.bitmap()[v/64] &^= (uint64(1) << uint(v%64))
	c.n--

	// Convert to array if we go below the threshold.
	if c.n == ArrayMaxSize {
		statsHit("bitmapRemove/bitmapToArray")
		c.bitmapToArray()
	}
	return true
}

// runRemove removes v from a run container, and returns true if v was removed.
func (c *Container) runRemove(v uint16) bool {
	runs := c.runs()
	i, contains := binSearchRuns(v, runs)
	if !contains {
		return false
	}
	c.unmapRun()
	runs = c.runs()
	if v == runs[i].last && v == runs[i].start {
		runs = append(runs[:i], runs[i+1:]...)
	} else if v == runs[i].last {
		runs[i].last--
	} else if v == runs[i].start {
		runs[i].start++
	} else if v > runs[i].start {
		last := runs[i].last
		runs[i].last = v - 1
		runs = append(runs, interval16{})
		copy(runs[i+2:], runs[i+1:])
		runs[i+1] = interval16{start: v + 1, last: last}
		// runs = append(runs[:i+1], append([]interval16{{start: v + 1, last: last}}, runs[i+1:]...)...)
	}
	c.n--
	c.setRuns(runs)
	return true
}

// max returns the maximum value in the container.
func (c *Container) max() uint16 {
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
	if len(array) == 0 {
		return 0 // probably hiding some ugly bug but it prevents a crash
	}
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
	return runs[len(runs)-1].last
}

// bitmapToArray converts from bitmap format to array format.
func (c *Container) bitmapToArray() {
	statsHit("bitmapToArray")
	bitmap := c.bitmap()
	c.setBitmap(nil)
	c.typ = containerArray
	c.mapped = false

	// return early if empty
	if c.n == 0 {
		c.setArray(nil)
		return
	}
	n := int32(0)

	array := make([]uint16, c.n)
	for i, word := range bitmap {
		for word != 0 {
			t := word & -word
			if roaringParanoia {
				if n >= c.n {
					panic("bitmap has more bits set than container.n")
				}
			}
			array[n] = uint16((i*64 + int(popcount(t-1))))
			n++
			word ^= t
		}
	}
	if roaringParanoia {
		if n != c.n {
			panic("bitmap has fewer bits set than container.n")
		}
	}
	c.setArray(array)
}

// arrayToBitmap converts from array format to bitmap format.
func (c *Container) arrayToBitmap() {
	statsHit("arrayToBitmap")
	array := c.array()
	c.typ = containerBitmap
	bitmap := make([]uint64, bitmapN)
	c.setBitmap(bitmap)
	c.mapped = false

	// return early if empty
	if c.n == 0 {
		return
	}

	for _, v := range array {
		bitmap[int(v)/64] |= (uint64(1) << uint(v%64))
	}
}

// runToBitmap converts from RLE format to bitmap format.
func (c *Container) runToBitmap() {
	statsHit("runToBitmap")
	runs := c.runs()
	bitmap := make([]uint64, bitmapN)
	c.typ = containerBitmap
	c.setBitmap(bitmap)

	c.mapped = false

	// return early if empty
	if c.n == 0 {
		return
	}

	for _, r := range runs {
		// TODO this can be ~64x faster for long runs by setting maxBitmap instead of single bits
		//note v must be int or will overflow
		for v := int(r.start); v <= int(r.last); v++ {
			bitmap[v/64] |= (uint64(1) << uint(v%64))
		}
	}
}

// bitmapToRun converts from bitmap format to RLE format.
func (c *Container) bitmapToRun(numRuns int32) {
	statsHit("bitmapToRun")
	bitmap := c.bitmap()
	c.mapped = false
	c.typ = containerRun
	// return early if empty
	if c.n == 0 {
		c.setRuns(nil)
		return
	}
	if numRuns == 0 {
		numRuns = bitmapCountRuns(bitmap)
	}

	runs := make([]interval16, 0, numRuns)

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
			runs = append(runs, interval16{start, maxContainerVal})
			break
		}
		currentLast := uint16(trailingZeroN(^current))
		last = 64*i + currentLast
		runs = append(runs, interval16{start, last - 1})

		// pad LSBs with 0s
		current = current & (current + 1)
	}
	c.setRuns(runs)
}

// arrayToRun converts from array format to RLE format.
func (c *Container) arrayToRun(numRuns int32) {
	statsHit("arrayToRun")
	array := c.array()
	c.typ = containerRun
	c.mapped = false
	// return early if empty
	if c.n == 0 {
		c.setRuns(nil)
		return
	}
	if numRuns == 0 {
		numRuns = arrayCountRuns(array)
	}

	runs := make([]interval16, 0, numRuns)
	start := array[0]
	for i, v := range array[1:] {
		if v-array[i] > 1 {
			// if current-previous > 1, one run ends and another begins
			runs = append(runs, interval16{start, array[i]})
			start = v
		}
	}
	// append final run
	runs = append(runs, interval16{start, array[c.n-1]})
	c.setRuns(runs)
}

// runToArray converts from RLE format to array format.
func (c *Container) runToArray() {
	statsHit("runToArray")
	runs := c.runs()
	c.typ = containerArray
	c.mapped = false

	// return early if empty
	if c.n == 0 {
		c.setArray(nil)
		return
	}

	array := make([]uint16, c.n)
	n := int32(0)
	for _, r := range runs {
		for v := int(r.start); v <= int(r.last); v++ {
			array[n] = uint16(v)
			n++
		}
	}
	if roaringParanoia {
		if n != c.n {
			panic("run has fewer bits set than container.n")
		}
	}
	c.setArray(array)
}

// Clone returns a copy of c.
func (c *Container) Clone() (out *Container) {
	statsHit("Container/Clone")
	switch c.typ {
	case containerArray:
		statsHit("Container/Clone/Array")
		cArray := c.array()
		array := make([]uint16, len(cArray))
		copy(array, cArray)
		out = NewContainerArray(array)
	case containerBitmap:
		statsHit("Container/Clone/Bitmap")
		other := NewContainerBitmap(c.n, nil)
		copy(other.bitmap(), c.bitmap())
		out = other
	case containerRun:
		statsHit("Container/Clone/Run")
		cRuns := c.runs()
		runs := make([]interval16, len(cRuns))
		copy(runs, cRuns)
		out = NewContainerRun(runs)
	}
	// this should probably never happen
	if roaringParanoia {
		if out.n != out.count() {
			panic("cloned container has wrong n")
		}
	}
	return out
}

// WriteTo writes c to w.
func (c *Container) WriteTo(w io.Writer) (n int64, err error) {
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
	nn, err := w.Write((*[0xFFFFFFF]byte)(unsafe.Pointer(&array[0]))[: 2*c.n : 2*c.n])
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
func (c *Container) info() containerInfo {
	info := containerInfo{N: c.n}

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

	if c.mapped {
		if c.isArray() {
			info.Pointer = unsafe.Pointer(&c.array()[0])
		} else if c.isRun() {
			info.Pointer = unsafe.Pointer(&c.runs()[0])
		} else {
			info.Pointer = unsafe.Pointer(&c.bitmap()[0])
		}
	}

	return info
}

// check performs a consistency check on the container.
func (c *Container) check() error {
	var a ErrorList

	if c.isArray() {
		array := c.array()
		if int32(len(array)) != c.n {
			a.Append(fmt.Errorf("array count mismatch: count=%d, n=%d", len(array), c.n))
		}
	} else if c.isRun() {
		n := c.runCountRange(0, maxContainerVal+1)
		if n != c.n {
			a.Append(fmt.Errorf("run count mismatch: count=%d, n=%d", n, c.n))
		}
	} else if c.isBitmap() {
		if n := c.bitmapCountRange(0, maxContainerVal+1); n != c.n {
			a.Append(fmt.Errorf("bitmap count mismatch: count=%d, n=%d", n, c.n))
		}
	} else {
		a.Append(fmt.Errorf("empty container"))
		if c.n != 0 {
			a.Append(fmt.Errorf("empty container with nonzero count: n=%d", c.n))
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
	if c.isBitmap() {
		c.bitmapRepair()
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
	c.n = n
}

// containerInfo represents a point-in-time snapshot of container stats.
type containerInfo struct {
	Key     uint64         // container key
	Type    string         // container type (array, bitmap, or run)
	N       int32          // number of bits
	Alloc   int            // memory used
	Pointer unsafe.Pointer // offset within the mmap
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
	x.arrayToBitmap()
	return flipBitmap(x)
}

func flipBitmap(b *Container) *Container {
	statsHit("flipBitmap")
	other := NewContainerBitmap(0, nil)
	bitmap := b.bitmap()
	otherBitmap := other.bitmap()
	for i, word := range bitmap {
		otherBitmap[i] = ^word
	}

	other.n = other.count()
	return other
}

func flipRun(b *Container) *Container {
	statsHit("flipRun")
	// TODO: actually implement this
	x := b.Clone()
	x.runToBitmap()
	return flipBitmap(x)
}

func intersectionCount(a, b *Container) int32 {
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
	if na == 0 || nb == 0 {
		return 0
	}
	if na > nb {
		ca, cb = cb, ca
		na, nb = nb, na // nolint: ineffassign
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
		if va < vb.start {
			i++
		} else if va >= vb.start && va <= vb.last {
			i++
			n++
		} else if va > vb.last {
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
		if va.last < vb.start {
			// |--va--| |--vb--|
			i++
		} else if va.start > vb.last {
			// |--vb--| |--va--|
			j++
		} else if va.last > vb.last && va.start >= vb.start {
			// |--vb-|-|-va--|
			n += 1 + int32(vb.last-va.start)
			j++
		} else if va.last > vb.last && va.start < vb.start {
			// |--va|--vb--|--|
			n += 1 + int32(vb.last-vb.start)
			j++
		} else if va.last <= vb.last && va.start >= vb.start {
			// |--vb|--va--|--|
			n += 1 + int32(va.last-va.start)
			i++
		} else if va.last <= vb.last && va.start < vb.start {
			// |--va-|-|-vb--|
			n += 1 + int32(va.last-vb.start)
			i++
		}
	}
	return n
}

func intersectionCountBitmapRun(a, b *Container) (n int32) {
	statsHit("intersectionCount/BitmapRun")
	for _, iv := range b.runs() {
		n += a.bitmapCountRange(int32(iv.start), int32(iv.last)+1)
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

func intersect(a, b *Container) *Container {
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
		if va < vb.start {
			i++
		} else if va > vb.last {
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
	for i, j := 0, 0; i < na && j < nb; {
		va, vb := ra[i], rb[j]
		if va.last < vb.start {
			// |--va--| |--vb--|
			i++
		} else if vb.last < va.start {
			// |--vb--| |--va--|
			j++
		} else if va.last > vb.last && va.start >= vb.start {
			// |--vb-|-|-va--|
			output.n += output.runAppendInterval(interval16{start: va.start, last: vb.last})
			j++
		} else if va.last > vb.last && va.start < vb.start {
			// |--va|--vb--|--|
			output.n += output.runAppendInterval(vb)
			j++
		} else if va.last <= vb.last && va.start >= vb.start {
			// |--vb|--va--|--|
			output.n += output.runAppendInterval(va)
			i++
		} else if va.last <= vb.last && va.start < vb.start {
			// |--va-|-|-vb--|
			output.n += output.runAppendInterval(interval16{start: vb.start, last: va.last})
			i++
		}
	}
	runs := output.runs()
	if output.n < ArrayMaxSize && int32(len(runs)) > output.n/2 {
		output.runToArray()
	} else if len(runs) > runMaxSize {
		output.runToBitmap()
	}
	return output
}

// intersectBitmapRun returns an array container if either container's
// cardinality is <= ArrayMaxSize. Otherwise it returns a bitmap container.
func intersectBitmapRun(a, b *Container) *Container {
	statsHit("intersect/BitmapRun")
	var output *Container
	runs := b.runs()
	if b.n <= ArrayMaxSize || a.n <= ArrayMaxSize {
		// output is array container
		array := make([]uint16, 0, b.n)
		for _, iv := range runs {
			for i := iv.start; i <= iv.last; i++ {
				if a.bitmapContains(i) {
					array = append(array, i)
				}
				// If the run ends the container, break to avoid an infinite loop.
				if i == 65535 {
					break
				}
			}
		}

		output = NewContainerArray(array)
	} else {
		// right now this iterates through the runs and sets integers in the
		// bitmap that are in the runs. alternately, we could zero out ranges in
		// the bitmap which are between runs.
		output = NewContainerBitmap(0, nil)
		bitmap := output.bitmap()
		aBitmap := a.bitmap()
		for j := 0; j < len(runs); j++ {
			vb := runs[j]
			i := vb.start >> 6 // index into a
			vastart := i << 6
			valast := vastart + 63
			for valast >= vb.start && vastart <= vb.last && i < bitmapN {
				if vastart >= vb.start && valast <= vb.last { // a within b
					bitmap[i] = aBitmap[i]
					output.n += int32(popcount(aBitmap[i]))
				} else if vb.start >= vastart && vb.last <= valast { // b within a
					var mask uint64 = ((1 << (vb.last - vb.start + 1)) - 1) << (vb.start - vastart)
					bits := aBitmap[i] & mask
					bitmap[i] |= bits
					output.n += int32(popcount(bits))
				} else if vastart < vb.start { // a overlaps front of b
					offset := 64 - (1 + valast - vb.start)
					bits := (aBitmap[i] >> offset) << offset
					bitmap[i] |= bits
					output.n += int32(popcount(bits))
				} else if vb.start < vastart { // b overlaps front of a
					offset := 64 - (1 + vb.last - vastart)
					bits := (aBitmap[i] << offset) >> offset
					bitmap[i] |= bits
					output.n += int32(popcount(bits))
				}
				// update loop vars
				i++
				vastart = i << 6
				valast = vastart + 63
			}
		}
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
		ab = a.bitmap()[:bitmapN]
		bb = b.bitmap()[:bitmapN]
		ob = make([]uint64, bitmapN)
		n  int32
	)
	for i := 0; i < bitmapN; i++ {
		ob[i] = ab[i] & bb[i]
		n += int32(popcount(ob[i]))
	}

	output := NewContainerBitmap(n, ob)
	return output
}

func union(a, b *Container) *Container {
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

func unionArrayArray(a, b *Container) *Container {
	statsHit("union/ArrayArray")
	aa, ab := a.array(), b.array()
	na, nb := len(aa), len(ab)
	output := make([]uint16, na+nb)
	n := 0
	for i, j := 0, 0; ; {
		if i >= na && j >= nb {
			break
		} else if i < na && j >= nb {
			output[n] = aa[i]
			n++
			i++
			continue
		} else if i >= na && j < nb {
			output[n] = ab[j]
			n++
			j++
			continue
		}

		va, vb := aa[i], ab[j]
		if va < vb {
			output[n] = va
			n++
			i++
		} else if va > vb {
			output[n] = vb
			n++
			j++
		} else {
			output[n] = va
			n++
			i, j = i+1, j+1
		}
	}
	return NewContainerArray(output[:n])
}

// unionArrayArrayInPlace does what it sounds like -- tries to combine
// the two arrays in-place. It does not try to ensure that the result is
// of a good array size, so it could be up to twice that size, temporarily.
func unionArrayArrayInPlace(a, b *Container) {
	statsHit("union/ArrayArrayInPlace")
	aa, ab := a.array(), b.array()
	na, nb := len(aa), len(ab)
	output := make([]uint16, na+nb)
	outN := 0
	for i, j := 0, 0; ; {
		if i >= na && j >= nb {
			break
		} else if i < na && j >= nb {
			copy(output[outN:], aa[i:])
			outN += na - i
			break
		} else if i >= na && j < nb {
			copy(output[outN:], ab[j:])
			outN += nb - j
			break
		}

		va, vb := aa[i], ab[j]
		if va < vb {
			output[outN] = va
			outN++
			i++
		} else if va > vb {
			output[outN] = vb
			outN++
			j++
		} else {
			output[outN] = va
			outN++
			i++
			j++
		}
	}
	a.setArray(output[:outN])
	a.n = int32(outN)
	if a.n > ArrayMaxSize {
		a.optimize()
	}
}

// unionArrayRun optimistically assumes that the result will be a run container,
// and converts to a bitmap or array container afterwards if necessary.
func unionArrayRun(a, b *Container) *Container {
	statsHit("union/ArrayRun")
	if b.n == maxContainerVal+1 {
		return b.Clone()
	}
	output := NewContainerRun(nil)
	aa, rb := a.array(), b.runs()
	na, nb := len(aa), len(rb)
	var vb interval16
	var va uint16
	for i, j := 0, 0; i < na || j < nb; {
		if i < na {
			va = aa[i]
		}
		if j < nb {
			vb = rb[j]
		}
		if i < na && (j >= nb || va < vb.start) {
			output.n += output.runAppendInterval(interval16{start: va, last: va})
			i++
		} else {
			output.n += output.runAppendInterval(vb)
			j++
		}
	}
	if output.n < ArrayMaxSize {
		output.runToArray()
	} else if len(output.runs()) > runMaxSize {
		output.runToBitmap()
	}
	return output
}

// runAppendInterval adds the given interval to the run container. It assumes
// that the interval comes at the end of the list of runs, and does not check
// that this is the case. It will not behave correctly if the start of the given
// interval is earlier than the start of the last interval in the list of runs.
// Its return value is the amount by which the cardinality of the container was
// increased.
func (c *Container) runAppendInterval(v interval16) int32 {
	runs := c.runs()
	if len(runs) == 0 {
		runs = append(runs, v)
		c.setRuns(runs)
		return int32(v.last-v.start) + 1
	}

	last := runs[len(runs)-1]
	if last.last == maxContainerVal { //protect against overflow
		return 0
	}
	if last.last+1 >= v.start && v.last > last.last {
		runs[len(runs)-1].last = v.last
		c.setRuns(runs)
		return int32(v.last - last.last)
	} else if last.last+1 < v.start {
		runs = append(runs, v)
		c.setRuns(runs)
		return int32(v.last-v.start) + 1
	}
	return 0
}

func unionRunRun(a, b *Container) *Container {
	statsHit("union/RunRun")
	if a.n == maxContainerVal+1 {
		return a.Clone()
	}
	if b.n == maxContainerVal+1 {
		return b.Clone()
	}
	ra, rb := a.runs(), b.runs()
	na, nb := len(ra), len(rb)
	output := NewContainerRun(make([]interval16, 0, na+nb))
	var va, vb interval16
	for i, j := 0, 0; i < na || j < nb; {
		if i < na {
			va = ra[i]
		}
		if j < nb {
			vb = rb[j]
		}
		if i < na && (j >= nb || va.start < vb.start) {
			output.n += output.runAppendInterval(va)
			i++
		} else {
			output.n += output.runAppendInterval(vb)
			j++
		}
	}
	if len(output.runs()) > runMaxSize {
		output.runToBitmap()
	}
	return output
}

func unionBitmapRun(a, b *Container) *Container {
	statsHit("union/BitmapRun")
	if b.n == maxContainerVal+1 {
		return b.Clone()
	}
	if a.n == maxContainerVal+1 {
		return a.Clone()
	}
	output := a.Clone()
	bitmap := output.bitmap()
	for _, run := range b.runs() {
		output.bitmapSetRange(bitmap, uint64(run.start), uint64(run.last)+1)
	}
	return output
}

// unions the run b into the bitmap a, mutating a in place. The n value of
// a will need to be repaired after the fact.
func unionBitmapRunInPlace(a, b *Container) {
	a.unmapBitmap()
	bitmap := a.bitmap()
	statsHit("union/BitmapRun")
	for _, run := range b.runs() {
		bitmapSetRangeIgnoreN(bitmap, uint64(run.start), uint64(run.last)+1)
	}
}

const maxBitmap = 0xFFFFFFFFFFFFFFFF

// sets all bits in [i, j) (c must be a bitmap container, and bitmap must
// be its bitmap).
func (c *Container) bitmapSetRange(bitmap []uint64, i, j uint64) {
	x := i >> 6
	y := (j - 1) >> 6
	var X uint64 = maxBitmap << (i % 64)
	var Y uint64 = maxBitmap >> (63 - ((j - 1) % 64))
	xcnt := popcount(X)
	ycnt := popcount(Y)
	if x == y {
		c.n += int32((j - i) - popcount(bitmap[x]&(X&Y)))
		bitmap[x] |= (X & Y)
	} else {
		c.n += int32(xcnt - popcount(bitmap[x]&X))
		bitmap[x] |= X
		for i := x + 1; i < y; i++ {
			c.n += int32(64 - popcount(bitmap[i]))
			bitmap[i] = maxBitmap
		}
		c.n += int32(ycnt - popcount(bitmap[y]&Y))
		bitmap[y] |= Y
	}
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
	if x == y {
		cnt := popcount(bitmap[x])
		bitmap[x] ^= (X & Y) //// flip
		c.n += int32(popcount(bitmap[x]) - cnt)
	} else {
		cnt := popcount(bitmap[x])
		bitmap[x] ^= X
		c.n += int32(popcount(bitmap[x]) - cnt)
		for i := x + 1; i < y; i++ {
			cnt = popcount(bitmap[i])
			bitmap[i] ^= maxBitmap
			c.n += int32(popcount(bitmap[i]) - cnt)
		}
		cnt = popcount(bitmap[y])
		bitmap[y] ^= Y
		c.n += int32(popcount(bitmap[y]) - cnt)
	}
}

// zeroes all bits in [i, j) (c must be a bitmap container)
func (c *Container) bitmapZeroRange(i, j uint64) {
	x := i >> 6
	y := (j - 1) >> 6
	var X uint64 = maxBitmap << (i % 64)
	var Y uint64 = maxBitmap >> (63 - ((j - 1) % 64))
	bitmap := c.bitmap()
	if x == y {
		c.n -= int32(popcount(bitmap[x] & (X & Y)))
		bitmap[x] &= ^(X & Y)
	} else {
		c.n -= int32(popcount(bitmap[x] & X))
		bitmap[x] &= ^X
		for i := x + 1; i < y; i++ {
			c.n -= int32(popcount(bitmap[i]))
			bitmap[i] = 0
		}
		c.n -= int32(popcount(bitmap[y] & Y))
		bitmap[y] &= ^Y
	}
}

func (c *Container) equals(c2 *Container) bool {
	if c.mapped != c2.mapped || c.typ != c2.typ || c.n != c2.n {
		return false
	}
	if c.typ == containerArray {
		ca, c2a := c.array(), c2.array()
		if len(ca) != len(c2a) {
			return false
		}
		for i := 0; i < len(ca); i++ {
			if ca[i] != c2a[i] {
				return false
			}
		}
	} else if c.typ == containerBitmap {
		cb, c2b := c.bitmap(), c2.bitmap()
		if len(cb) != len(c2b) {
			return false
		}
		for i := 0; i < len(cb); i++ {
			if cb[i] != c2b[i] {
				return false
			}
		}
	} else if c.typ == containerRun {
		cr, c2r := c.runs(), c2.runs()
		if len(cr) != len(c2r) {
			return false
		}
		for i := 0; i < len(cr); i++ {
			if cr[i] != c2r[i] {
				return false
			}
		}
	} else {
		panic(fmt.Sprintf("unknown container type: %v", c.typ))
	}
	return true
}

func unionArrayBitmap(a, b *Container) *Container {
	output := b.Clone()
	bitmap := output.bitmap()
	for _, v := range a.array() {
		if !output.bitmapContains(v) {
			bitmap[v/64] |= (1 << uint64(v%64))
			output.n++
		}
	}
	return output
}

// unions array b into bitmap a, mutating a in place. The n value
// of a will need to be repaired after the fact.
func unionBitmapArrayInPlace(a, b *Container) {
	a.unmapBitmap()
	bitmap := a.bitmap()
	for _, v := range b.array() {
		bitmap[v>>6] |= (uint64(1) << (v % 64))
	}
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

	output := NewContainerBitmap(n, ob)
	return output
}

// unions bitmap b into bitmap a, mutating a in place. The n value of
// a will need to be repaired after the fact.
func unionBitmapBitmapInPlace(a, b *Container) {

	a.unmapBitmap()

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
}

func difference(a, b *Container) *Container {
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
			output.add(va)
			i++
			continue
		}

		vb := ab[j]
		if va < vb {
			output.add(va)
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

	if a.n == 0 || b.n == 0 {
		return a.Clone()
	}

	output := NewContainerArray(make([]uint16, 0, a.n))
	// cardinality upper bound: card(A)

	i := 0 // array index
	j := 0 // run index
	aa, rb := a.array(), b.runs()

	// handle overlap
	for i < int(a.n) {

		// keep all array elements before beginning of runs
		if aa[i] < rb[j].start {
			output.add(aa[i])
			i++
			continue
		}

		// if array element in run, skip it
		if aa[i] >= rb[j].start && aa[i] <= rb[j].last {
			i++
			continue
		}

		// if array element larger than current run, check next run
		if aa[i] > rb[j].last {
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
		if output.typ == containerArray {
			array := output.array()
			array = append(array, aa[i:]...)
			output.setArray(array)
			// TODO: consider handling container.n mutations in one place
			// like we do with container.add().
			output.n += int32(len(aa[i:]))
		} else {
			for _, v := range aa[i:] {
				output.add(v)
			}
		}
	}
	return output
}

// differenceBitmapRun computes the difference of an bitmap from a run.
func differenceBitmapRun(a, b *Container) *Container {
	statsHit("difference/BitmapRun")
	if a.n == 0 || b.n == 0 {
		return a.Clone()
	}

	output := a.Clone()
	for _, run := range b.runs() {
		output.bitmapZeroRange(uint64(run.start), uint64(run.last)+1)
	}
	return output
}

// differenceRunArray subtracts the bits in an array container from a run
// container.
func differenceRunArray(a, b *Container) *Container {
	statsHit("difference/RunArray")
	if a.n == 0 || b.n == 0 {
		return a.Clone()
	}
	ra, ab := a.runs(), b.array()
	runs := make([]interval16, 0, len(ra))

	bidx := 0
	vb := ab[bidx]

RUNLOOP:
	for _, run := range ra {
		start := run.start
		for vb < run.start {
			bidx++
			if bidx >= len(ab) {
				break
			}
			vb = ab[bidx]
		}
		for vb >= run.start && vb <= run.last {
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
			runs = append(runs, interval16{start: start, last: vb - 1})
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

		if start <= run.last {
			runs = append(runs, interval16{start: start, last: run.last})
		}
	}
	output := NewContainerRun(runs)
	output.optimize()
	return output
}

// differenceRunBitmap computes the difference of an run from a bitmap.
func differenceRunBitmap(a, b *Container) *Container {
	statsHit("difference/RunBitmap")
	ra := a.runs()
	// If a is full, difference is the flip of b.
	if len(ra) > 0 && ra[0].start == 0 && ra[0].last == 65535 {
		return flipBitmap(b)
	}
	output := NewContainerRun(nil)
	runs := output.runs()
	if len(ra) == 0 {
		return NewContainerRun(nil)
	}
	output.n = a.n
	for _, inputRun := range ra {
		run := inputRun
		add := true
		for bit := inputRun.start; bit <= inputRun.last; bit++ {
			if b.bitmapContains(bit) {
				output.n--
				if run.start == bit {
					if bit == 65535 { //overflow
						add = false
					}

					run.start++
				} else if bit == run.last {
					run.last--
				} else {
					run.last = bit - 1
					if run.last >= run.start {
						runs = append(runs, run)
					}
					run.start = bit + 1
					run.last = inputRun.last
				}
				if run.start > run.last {
					break
				}
			}

			if bit == 65535 { //overflow
				break
			}
		}
		if run.start <= run.last {
			if add {
				runs = append(runs, run)
			}
		}
	}

	output.setRuns(runs)
	if output.n < ArrayMaxSize && int32(len(runs)) > output.n/2 {
		output.runToArray()
	} else if len(runs) > runMaxSize {
		output.runToBitmap()
	}
	return output
}

// differenceRunRun computes the difference of two runs.
func differenceRunRun(a, b *Container) *Container {
	statsHit("difference/RunRun")
	if a.n == 0 || b.n == 0 {
		return a.Clone()
	}

	ra, rb := a.runs(), b.runs()
	apos := 0 // current a-run index
	bpos := 0 // current b-run index
	astart := ra[apos].start
	alast := ra[apos].last
	bstart := rb[bpos].start
	blast := rb[bpos].last
	alen := len(ra)
	blen := len(rb)

	runs := make([]interval16, 0, alen+blen) // TODO allocate max then truncate? or something else
	// cardinality upper bound: sum of number of runs
	// each B-run could split an A-run in two, up to len(b.runs) times

	for apos < alen && bpos < blen {
		switch {
		case alast < bstart:
			// current A-run entirely precedes current B-run: keep full A-run, advance to next A-run
			runs = append(runs, interval16{start: astart, last: alast})
			apos++
			if apos < alen {
				astart = ra[apos].start
				alast = ra[apos].last
			}
		case blast < astart:
			// current B-run entirely precedes current A-run: advance to next B-run
			bpos++
			if bpos < blen {
				bstart = rb[bpos].start
				blast = rb[bpos].last
			}
		default:
			// overlap
			if astart < bstart {
				runs = append(runs, interval16{start: astart, last: bstart - 1})
			}
			if alast > blast {
				astart = blast + 1
			} else {
				apos++
				if apos < alen {
					astart = ra[apos].start
					alast = ra[apos].last
				}
			}
		}
	}
	if apos < alen {
		runs = append(runs, interval16{start: astart, last: alast})
		apos++
		if apos < alen {
			runs = append(runs, ra[apos:]...)
		}
	}
	return NewContainerRun(runs)
}

func differenceArrayBitmap(a, b *Container) *Container {
	statsHit("difference/ArrayBitmap")
	output := make([]uint16, 0, a.n)
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

	for _, v := range b.array() {
		if output.bitmapContains(v) {
			bitmap[v/64] &^= (uint64(1) << uint(v%64))
			output.n--
		}
	}
	if output.n < ArrayMaxSize {
		output.bitmapToArray()
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

	output := NewContainerBitmap(n, ob)
	if output.n < ArrayMaxSize {
		output.bitmapToArray()
	}
	return output
}

func xor(a, b *Container) *Container {
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
	output := NewContainerArray(nil)
	aa, ab := a.array(), b.array()
	na, nb := len(aa), len(ab)
	for i, j := 0, 0; i < na || j < nb; {
		if i < na && j >= nb {
			output.add(aa[i])
			i++
			continue
		} else if i >= na && j < nb {
			output.add(ab[j])
			j++
			continue
		}

		va, vb := aa[i], ab[j]
		if va < vb {
			output.add(va)
			i++
		} else if va > vb {
			output.add(vb)
			j++
		} else { //==
			i++
			j++
		}
	}
	return output
}

func xorArrayBitmap(a, b *Container) *Container {
	statsHit("xor/ArrayBitmap")
	output := b.Clone()
	for _, v := range a.array() {
		if b.bitmapContains(v) {
			output.remove(v)
		} else {
			output.add(v)
		}
	}

	// It's possible that output was converted from bitmap to array in output.remove()
	// so we only do this conversion if output is still a bitmap container.
	if output.typ == containerBitmap && output.count() < ArrayMaxSize {
		output.bitmapToArray()
	}

	return output
}

func xorBitmapBitmap(a, b *Container) *Container {
	statsHit("xor/BitmapBitmap")
	// local variables added to prevent BCE checks in loop
	// see https://go101.org/article/bounds-check-elimination.html

	var (
		ab = a.bitmap()[:bitmapN]
		bb = b.bitmap()[:bitmapN]
		ob = make([]uint64, bitmapN)[:bitmapN]

		n int32
	)

	for i := 0; i < bitmapN; i++ {
		ob[i] = ab[i] ^ bb[i]
		n += int32(popcount(ob[i]))
	}

	output := NewContainerBitmap(n, ob)
	if output.count() < ArrayMaxSize {
		output.bitmapToArray()
	}
	return output
}

// shift() shifts the contents of c by one. It returns
// the new container and a bool indicating whether a
// carry bit was shifted out.
func shift(c *Container) (*Container, bool) {
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
	carry := false
	output := NewContainerBitmap(a.n, nil)
	ba, bo := a.bitmap(), output.bitmap()
	lastCarry := false
	for i, v := range ba {
		carry = (v & (1 << 63)) != 0
		v = v << 1
		if lastCarry {
			v |= 1
		}
		bo[i] = v
		lastCarry = carry
	}
	if carry {
		output.n--
	}
	return output, carry
}

// shiftRun is a run-specific implementation of shift().
func shiftRun(a *Container) (*Container, bool) {
	statsHit("shift/Run")
	carry := false
	ra := a.runs()
	ro := make([]interval16, 0, len(ra))

	for _, v := range ra {
		if v.start+1 == 0 { // final run was 1 bit on container edge
			carry = true
			break
		} else if v.last+1 == 0 { // final run ends on container edge
			v.start++
			carry = true
		} else {
			v.start++
			v.last++
			carry = false
		}
		ro = append(ro, v)
	}

	return NewContainerRun(ro), carry
}

// opType represents a type of operation.
type opType uint8

const (
	opTypeAdd         = opType(0)
	opTypeRemove      = opType(1)
	opTypeAddBatch    = opType(2)
	opTypeRemoveBatch = opType(3)
)

// op represents an operation on the bitmap.
type op struct {
	typ    opType
	value  uint64
	values []uint64
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
	default:
		panic(fmt.Sprintf("invalid op type: %d", op.typ))
	}
	return changed
}

// WriteTo writes op to the w.
func (op *op) WriteTo(w io.Writer) (n int64, err error) {
	buf := make([]byte, op.size())

	// Write type and value.
	buf[0] = byte(op.typ)
	if op.typ <= 1 {
		binary.LittleEndian.PutUint64(buf[1:9], op.value)
	} else {
		binary.LittleEndian.PutUint64(buf[1:9], uint64(len(op.values)))
		p := 13 // start of values (skip 4 for checksum)
		for _, v := range op.values {
			binary.LittleEndian.PutUint64(buf[p:p+8], v)
			p += 8
		}
	}

	// Add checksum at the end.
	h := fnv.New32a()
	_, _ = h.Write(buf[0:9])
	_, _ = h.Write(buf[13:])
	binary.LittleEndian.PutUint32(buf[9:13], h.Sum32())

	// Write to writer.
	nn, err := w.Write(buf)
	return int64(nn), err
}

var minOpSize = 13

// UnmarshalBinary decodes data into an op.
func (op *op) UnmarshalBinary(data []byte) error {
	if len(data) < minOpSize {
		return fmt.Errorf("op data out of bounds: len=%d", len(data))
	}
	statsHit("op/UnmarshalBinary")

	op.typ = opType(data[0])
	// op.value will actually contain the length of values for batch ops
	op.value = binary.LittleEndian.Uint64(data[1:9])

	// Verify checksum.
	h := fnv.New32a()
	_, _ = h.Write(data[0:9])

	if op.typ > 1 {
		if len(data) < int(13+op.value*8) {
			return fmt.Errorf("op data truncated - expected %d, got %d", 13+op.value*8, len(data))
		}
		_, _ = h.Write(data[13 : 13+op.value*8])
		op.values = make([]uint64, op.value)
		for i := uint64(0); i < op.value; i++ {
			start := 13 + i*8
			op.values[i] = binary.LittleEndian.Uint64(data[start : start+8])
		}
		op.value = 0
	}
	if chk := binary.LittleEndian.Uint32(data[9:13]); chk != h.Sum32() {
		return fmt.Errorf("checksum mismatch: exp=%08x, got=%08x", h.Sum32(), chk)
	}

	return nil
}

// size returns the encoded size of the op, in bytes.
func (op *op) size() int {
	if op.typ == opTypeAdd || op.typ == opTypeRemove {
		return 1 + 8 + 4
	}
	return 1 + 8 + 4 + len(op.values)*8
}

// count returns the number of bits the operation mutates.
func (op *op) count() int {
	switch op.typ {
	case 0, 1:
		return 1
	case 2, 3:
		return len(op.values)
	default:
		panic(fmt.Sprintf("unknown operation type: %d", op.typ))
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
	var vb interval16
	var va uint16
	lastI, lastJ := -1, -1
	for i, j := 0, 0; i < na || j < nb; {
		if i < na && i != lastI {
			va = aa[i]
		}
		if j < nb && j != lastJ {
			vb = rb[j]
		}
		lastI = i
		lastJ = j

		if i < na && (j >= nb || va < vb.start) { //before
			output.n += output.runAppendInterval(interval16{start: va, last: va})
			i++
		} else if j < nb && (i >= na || va > vb.last) { //after
			output.n += output.runAppendInterval(vb)
			j++
		} else if va > vb.start {
			if va < vb.last {
				output.n += output.runAppendInterval(interval16{start: vb.start, last: va - 1})
				i++
				vb.start = va + 1

				if vb.start > vb.last {
					j++
				}
			} else if va > vb.last {
				output.n += output.runAppendInterval(vb)
				j++
			} else { // va == vb.last
				vb.last--
				if vb.start <= vb.last {
					output.n += output.runAppendInterval(vb)
				}
				j++
				i++
			}

		} else { // we know va == vb.start
			if vb.start == maxContainerVal { // protect overflow
				j++
			} else {
				vb.start++
				if vb.start > vb.last {
					j++
				}
			}
			i++
		}
	}
	if output.n < ArrayMaxSize {
		output.runToArray()
	} else if len(output.runs()) > runMaxSize {
		output.runToBitmap()
	}
	return output
}

// xorCompare computes first exclusive run between two runs.
func xorCompare(x *xorstm) (r1 interval16, hasData bool) {
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

	if x.va.last < x.vb.start { //va  before
		x.vaValid = false
		r1 = x.va
		hasData = true
	} else if x.vb.last < x.va.start { //vb before
		x.vbValid = false
		r1 = x.vb
		hasData = true
	} else if x.va.start == x.vb.start && x.va.last == x.vb.last { // Equal
		x.vaValid = false
		x.vbValid = false
	} else if x.va.start <= x.vb.start && x.va.last >= x.vb.last { //vb inside
		x.vbValid = false
		if x.va.start != x.vb.start {
			r1 = interval16{start: x.va.start, last: x.vb.start - 1}
			hasData = true
		}

		if x.vb.last == maxContainerVal { // Check for overflow
			x.vaValid = false

		} else {
			x.va.start = x.vb.last + 1
			if x.va.start > x.va.last {
				x.vaValid = false
			}
		}

	} else if x.vb.start <= x.va.start && x.vb.last >= x.va.last { //va inside
		x.vaValid = false
		if x.vb.start != x.va.start {
			r1 = interval16{start: x.vb.start, last: x.va.start - 1}
			hasData = true
		}

		if x.va.last == maxContainerVal { //check for overflow
			x.vbValid = false
		} else {
			x.vb.start = x.va.last + 1
			if x.vb.start > x.vb.last {
				x.vbValid = false
			}
		}

	} else if x.va.start < x.vb.start && x.va.last <= x.vb.last { //va first overlap
		x.vaValid = false
		r1 = interval16{start: x.va.start, last: x.vb.start - 1}
		hasData = true
		if x.va.last == maxContainerVal { // check for overflow
			x.vbValid = false
		} else {
			x.vb.start = x.va.last + 1
			if x.vb.start > x.vb.last {
				x.vbValid = false
			}
		}
	} else if x.vb.start < x.va.start && x.vb.last <= x.va.last { //vb first overlap
		x.vbValid = false
		r1 = interval16{start: x.vb.start, last: x.va.start - 1}
		hasData = true

		if x.vb.last == maxContainerVal { // check for overflow
			x.vaValid = false
		} else {
			x.va.start = x.vb.last + 1
			if x.va.start > x.va.last {
				x.vaValid = false
			}
		}
	}
	return r1, hasData
}

//stm  is state machine used to "xor" iterate over runs.
type xorstm struct {
	vaValid, vbValid bool
	va, vb           interval16
}

// xorRunRun computes the exclusive or of two run containers.
func xorRunRun(a, b *Container) *Container {
	statsHit("xor/RunRun")
	ra, rb := a.runs(), b.runs()
	na, nb := len(ra), len(rb)
	if na == 0 {
		return b.Clone()
	}
	if nb == 0 {
		return a.Clone()
	}
	output := NewContainerRun(nil)

	lastI, lastJ := -1, -1

	state := &xorstm{}

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
			output.n += output.runAppendInterval(r1)
		}
		if !state.vaValid {
			i++
		}
		if !state.vbValid {
			j++
		}

	}

	l := len(output.runs())
	if output.n < ArrayMaxSize && int32(l) > output.n/2 {
		output.runToArray()
	} else if l > runMaxSize {
		output.runToBitmap()
	}
	return output
}

// xorRunRun computes the exclusive or of a bitmap and a run container.
func xorBitmapRun(a, b *Container) *Container {
	statsHit("xor/BitmapRun")
	output := a.Clone()

	for _, run := range b.runs() {
		output.bitmapXorRange(uint64(run.start), uint64(run.last)+1)
	}

	return output
}

func bitmapsEqual(b, c *Bitmap) error { // nolint: deadcode
	statsHit("bitmapsEqual")
	if b.OpWriter != c.OpWriter {
		return errors.New("opWriters not equal")
	}
	if b.opN != c.opN {
		return errors.New("opNs not equal")
	}

	biter, _ := b.Containers.Iterator(0)
	citer, _ := c.Containers.Iterator(0)
	bn, cn := biter.Next(), citer.Next()
	for ; bn && cn; bn, cn = biter.Next(), citer.Next() {
		bk, bc := biter.Value()
		ck, cc := citer.Value()
		if bk != ck {
			return errors.New("keys not equal")
		}
		if !bc.equals(cc) {
			return errors.New("containers not equal")
		}
	}
	if bn && !cn || cn && !bn {
		return errors.New("different numbers of containers")
	}

	return nil
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
		newType = containerBitmap
		if card < ArrayMaxSize {
			newType = containerArray
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
				return containerRun
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
	if pos+2*2*int(size) > len(buf) {
		err = fmt.Errorf("malformed bitmap, key-cardinality slice overruns buffer at %d", pos+2*2*int(size))
		return size, containerTyper, header, pos, haveRuns, err
	}
	pos += 2 * 2 * int(size) // moving pos past keycount
	return size, containerTyper, header, pos, haveRuns, err
}

// UnmarshalBinary decodes b from a binary-encoded byte slice. data can be in
// either official roaring format or Pilosa's roaring format.
func (b *Bitmap) UnmarshalBinary(data []byte) error {
	if data == nil {
		// Nothing to unmarshal
		return nil
	}
	statsHit("Bitmap/UnmarshalBinary")
	b.opN = 0 // reset opN since we're reading new data.
	fileMagic := uint32(binary.LittleEndian.Uint16(data[0:2]))
	if fileMagic == MagicNumber { // if pilosa roaring
		return errors.Wrap(b.unmarshalPilosaRoaring(data), "unmarshaling as pilosa roaring")
	}

	keyN, containerTyper, header, pos, haveRuns, err := readOfficialHeader(data)
	if err != nil {
		return errors.Wrap(err, "reading roaring header")
	}

	b.Containers.Reset()
	// Descriptive header section: Read container keys and cardinalities.
	for i, buf := uint(0), data[header:]; i < uint(keyN); i, buf = i+1, buf[4:] {
		card := int(binary.LittleEndian.Uint16(buf[2:4])) + 1
		b.Containers.PutContainerValues(
			uint64(binary.LittleEndian.Uint16(buf[0:2])),
			containerTyper(i, card), /// container type voodo with isRunBitmap
			card,
			true)
	}

	// Read container offsets and attach data.
	if haveRuns {
		readWithRuns(b, data, pos, keyN)
	} else {
		err := readOffsets(b, data, pos, keyN)
		if err != nil {
			return errors.Wrap(err, "reading offsets from official roaring format")
		}
	}
	return nil
}

func readOffsets(b *Bitmap, data []byte, pos int, keyN uint32) error {

	citer, _ := b.Containers.Iterator(0)
	for i, buf := 0, data[pos:]; i < int(keyN); i, buf = i+1, buf[4:] {
		offset := binary.LittleEndian.Uint32(buf[0:4])
		// Verify the offset is within the bounds of the input data.
		if int(offset) >= len(data) {
			return fmt.Errorf("offset out of bounds: off=%d, len=%d", offset, len(data))
		}

		// Map byte slice directly to the container data.
		citer.Next()
		_, c := citer.Value()
		switch c.typ {
		case containerArray:
			c.setArray((*[0xFFFFFFF]uint16)(unsafe.Pointer(&data[offset]))[:c.n:c.n])
		case containerBitmap:
			c.setBitmap((*[0xFFFFFFF]uint64)(unsafe.Pointer(&data[offset]))[:bitmapN:bitmapN])
		default:
			return fmt.Errorf("unsupported container type %d", c.typ)
		}
	}
	return nil
}

func readWithRuns(b *Bitmap, data []byte, pos int, keyN uint32) {
	citer, _ := b.Containers.Iterator(0)
	for i := 0; i < int(keyN); i++ {
		citer.Next()
		_, c := citer.Value()
		switch c.typ {
		case containerRun:
			runCount := binary.LittleEndian.Uint16(data[pos : pos+runCountHeaderSize])
			c.setRuns((*[0xFFFFFFF]interval16)(unsafe.Pointer(&data[pos+runCountHeaderSize]))[:runCount:runCount])
			runs := c.runs()

			for o := range runs { // must convert from start:length to start:end :(
				runs[o].last = runs[o].start + runs[o].last
			}
			pos += int((runCount * interval16Size) + runCountHeaderSize)
		case containerArray:
			c.setArray((*[0xFFFFFFF]uint16)(unsafe.Pointer(&data[pos]))[:c.n:c.n])
			pos += int(c.n * 2)
		case containerBitmap:
			c.setBitmap((*[0xFFFFFFF]uint64)(unsafe.Pointer(&data[pos]))[:bitmapN:bitmapN])
			pos += bitmapN * 8
		}
	}
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
			summary.n += int64(currContainer.n)

			if currContainer.n == maxContainerVal+1 {
				summary.hasMaxRange = true
				summary.n = maxContainerVal + 1
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
