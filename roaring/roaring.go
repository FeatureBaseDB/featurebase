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

// package roaring implements roaring bitmaps with support for incremental changes.
package roaring

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"sort"
	"unsafe"
)

const (
	// cookie is the first four bytes in a roaring bitmap file.
	cookie = uint32(12346)

	// headerSize is the size of the cookie and key count at the beginning of a file.
	headerSize = 4 + 4

	// bitmapN is the number of values in a container.bitmap.
	bitmapN = (1 << 16) / 64

	// manual allocation size tuned to our average client data
	manualAlloc = 524288
)

// Bitmap represents a roaring bitmap.
type Bitmap struct {
	keys       []uint64     // keys for containers
	containers []*container // array and bitmap containers

	// Number of operations written to the writer.
	opN int

	// Writer where operations are appended to.
	OpWriter io.Writer
}

// NewBitmap returns a Bitmap with an initial set of values.
func NewBitmap(a ...uint64) *Bitmap {
	b := &Bitmap{}
	b.Add(a...)
	return b
}

// Clone returns a heap allocated copy of the bitmap.
// Note: The OpWriter IS NOT copied to the new bitmap.
func (b *Bitmap) Clone() *Bitmap {
	if b == nil {
		return nil
	}

	// Create a copy of the bitmap structure.
	other := &Bitmap{
		keys:       make([]uint64, len(b.keys)),
		containers: make([]*container, len(b.containers)),
	}

	// Copy keys & clone containers.
	copy(other.keys, b.keys)
	for i, c := range b.containers {
		other.containers[i] = c.clone()
	}

	return other
}

// Add adds values to the bitmap.
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
		if op.apply(b) {
			changed = true

		}
	}

	return changed, nil
}
func (b *Bitmap) add(v uint64) bool {
	hb := highbits(v)
	i := search64(b.keys, hb)

	// If index is negative then there's not an exact match
	// and a container needs to be added.
	if i < 0 {
		b.insertAt(hb, newContainer(), -i-1)
		i = -i - 1
	}

	return b.containers[i].add(lowbits(v))
}

// Contains returns true if v is in the bitmap.
func (b *Bitmap) Contains(v uint64) bool {
	c := b.container(highbits(v))
	if c == nil {
		return false
	}
	return c.contains(lowbits(v))
}

// Remove removes values from the bitmap.
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

func (b *Bitmap) remove(v uint64) bool {
	hb := highbits(v)
	i := search64(b.keys, hb)
	if i < 0 {
		return false
	}
	return b.containers[i].remove(lowbits(v))
}

// Max returns the highest value in the bitmap.
// Returns zero if the bitmap is empty.
func (b *Bitmap) Max() uint64 {
	if len(b.keys) == 0 {
		return 0
	}

	hb := b.keys[len(b.keys)-1]
	lb := b.containers[len(b.containers)-1].max()
	return uint64(hb)<<16 | uint64(lb)
}

// Count returns the number of bits set in the bitmap.
func (b *Bitmap) Count() (n uint64) {
	for _, container := range b.containers {
		n += uint64(container.n)
	}
	return n
}

// CountRange returns the number of bits set between [start, end).
func (b *Bitmap) CountRange(start, end uint64) (n uint64) {
	i := search64(b.keys, highbits(start))
	j := search64(b.keys, highbits(end))

	// If range is entirely in one container then just count that range.
	if i > 0 && i == j {
		return uint64(b.containers[i].countRange(lowbits(start), lowbits(end)))
	}

	// Count first partial container.
	if i < 0 {
		i = -i
	} else {
		n += uint64(b.containers[i].countRange(lowbits(start), (bitmapN*64)+1))
	}

	// Count last container.
	if j < 0 {
		j = -j
		if j > len(b.containers) {
			j = len(b.containers)
		}
	} else {
		n += uint64(b.containers[j].countRange(0, lowbits(end)))
	}

	// Count containers in between.
	for x := i + 1; x < j; x++ {
		n += uint64(b.containers[x].n)
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

	// Find starting container.
	n := len(b.containers)
	i := sort.Search(n, func(i int) bool { return b.keys[i] >= hi0 })

	var other Bitmap
	for ; i < n; i++ {
		key := b.keys[i]

		// If we've exceeded the upper bound then exit.
		if key >= hi1 {
			break
		}

		// Otherwise append container with offset key.
		other.keys = append(other.keys, off+(key-hi0))
		other.containers = append(other.containers, b.containers[i])
	}
	return &other
}

// container returns the container with the given key.
func (b *Bitmap) container(key uint64) *container {
	i := search64(b.keys, key)
	if i < 0 {
		return nil
	}
	return b.containers[i]
}

func insertU64(original []uint64, position int, value uint64) []uint64 {
	l := len(original)
	target := original
	if cap(original) == l {
		target = make([]uint64, l+1, l+manualAlloc)
		copy(target, original[:position])
	} else {
		target = append(target, 0)
	}
	copy(target[position+1:], original[position:])
	target[position] = value
	return target
}

func insertContainer(original []*container, position int, value *container) []*container {
	l := len(original)
	target := original
	if cap(original) == l {
		target = make([]*container, l+1, l+manualAlloc)
		copy(target, original[:position])
	} else {
		target = append(target, nil)
	}
	copy(target[position+1:], original[position:])
	target[position] = value
	return target
}
func (b *Bitmap) insertAt(key uint64, c *container, i int) {
	b.keys = insertU64(b.keys, i, key)
	b.containers = insertContainer(b.containers, i, c)
}

// IntersectionCount returns the number of intersections between b and other.
func (b *Bitmap) IntersectionCount(other *Bitmap) uint64 {
	var n uint64
	for i, j := 0, 0; i < len(b.containers) && j < len(other.containers); {
		ki, kj := b.keys[i], other.keys[j]
		if ki < kj {
			i++
		} else if ki > kj {
			j++
		} else {
			n += intersectionCount(b.containers[i], other.containers[j])
			i, j = i+1, j+1
		}
	}
	return n
}

// Intersect returns the intersection of b and other.
func (b *Bitmap) Intersect(other *Bitmap) *Bitmap {
	output := &Bitmap{}

	ki, ci := b.keys, b.containers
	kj, cj := other.keys, other.containers
	for {
		var key uint64
		var container *container

		ni, nj := len(ki), len(kj)
		if ni == 0 && nj == 0 { // eof(i,j)
			break
		} else if ni == 0 || (nj != 0 && ki[0] > kj[0]) { // eof(i) or i > j
			key, container = kj[0], cj[0].clone()
			kj, cj = kj[1:], cj[1:]
		} else if nj == 0 || (ki[0] < kj[0]) { // eof(j) or i < j
			key, container = ki[0], ci[0].clone()
			ki, ci = ki[1:], ci[1:]
		} else { // i == j
			key, container = ki[0], intersect(ci[0], cj[0])
			ki, ci = ki[1:], ci[1:]
			kj, cj = kj[1:], cj[1:]
			output.keys = append(output.keys, key)
			output.containers = append(output.containers, container)
		}

	}

	return output
}

// Union returns the bitwise union of b and other.
func (b *Bitmap) Union(other *Bitmap) *Bitmap {
	output := &Bitmap{}

	ki, ci := b.keys, b.containers
	kj, cj := other.keys, other.containers

	for {
		var key uint64
		var container *container

		ni, nj := len(ki), len(kj)
		if ni == 0 && nj == 0 { // eof(i,j)
			break
		} else if ni == 0 || (nj != 0 && ki[0] > kj[0]) { // eof(i) or i > j
			key, container = kj[0], cj[0].clone()
			kj, cj = kj[1:], cj[1:]
		} else if nj == 0 || (ki[0] < kj[0]) { // eof(j) or i < j
			key, container = ki[0], ci[0].clone()
			ki, ci = ki[1:], ci[1:]
		} else { // i == j
			key, container = ki[0], union(ci[0], cj[0])
			ki, ci = ki[1:], ci[1:]
			kj, cj = kj[1:], cj[1:]
		}

		output.keys = append(output.keys, key)
		output.containers = append(output.containers, container)
	}

	return output
}

// Difference returns the difference of b and other.
func (b *Bitmap) Difference(other *Bitmap) *Bitmap {
	output := &Bitmap{}

	ki, ci := b.keys, b.containers
	kj, cj := other.keys, other.containers

	for {
		var key uint64
		var container *container

		ni, nj := len(ki), len(kj)
		if ni == 0 { // eof(i)
			break
		} else if nj == 0 || ki[0] < kj[0] { // eof(j) or i < j
			key, container = ki[0], ci[0].clone()
			ki, ci = ki[1:], ci[1:]
			output.keys = append(output.keys, key)
			output.containers = append(output.containers, container)
		} else if nj > 0 && ki[0] > kj[0] { // i > j
			kj, cj = kj[1:], cj[1:]
		} else { // i == j
			key, container = ki[0], difference(ci[0], cj[0])
			ki, ci = ki[1:], ci[1:]
			kj, cj = kj[1:], cj[1:]
			output.keys = append(output.keys, key)
			output.containers = append(output.containers, container)
		}

	}

	return output
}

// Xor returns the bitwise exclusive or of b and other.
func (b *Bitmap) Xor(other *Bitmap) *Bitmap {
	output := &Bitmap{}

	ki, ci := b.keys, b.containers
	kj, cj := other.keys, other.containers

	for {
		var key uint64
		var container *container

		ni, nj := len(ki), len(kj)
		if ni == 0 && nj == 0 { // eof(i,j)
			break
		} else if ni == 0 || (nj != 0 && ki[0] > kj[0]) { // eof(i) or i > j
			key, container = kj[0], cj[0].clone()
			kj, cj = kj[1:], cj[1:]
		} else if nj == 0 || (ki[0] < kj[0]) { // eof(j) or i < j
			key, container = ki[0], ci[0].clone()
			ki, ci = ki[1:], ci[1:]
		} else { // i == j
			key, container = ki[0], xor(ci[0], cj[0])
			ki, ci = ki[1:], ci[1:]
			kj, cj = kj[1:], cj[1:]
		}

		output.keys = append(output.keys, key)
		output.containers = append(output.containers, container)
	}

	return output
}

// removeEmptyContainers deletes all containers that have a count of zero.
func (b *Bitmap) removeEmptyContainers() {
	for i := 0; i < len(b.containers); {
		c := b.containers[i]

		if c.n == 0 {
			b.keys = append(b.keys[:i], b.keys[i+1:]...)

			copy(b.containers[i:], b.containers[i+1:])
			b.containers[len(b.containers)-1] = nil
			b.containers = b.containers[:len(b.containers)-1]
			continue
		}

		i++
	}
}
func (b *Bitmap) countEmptyContainers() int {
	result := 0
	for i := 0; i < len(b.containers); {
		c := b.containers[i]

		if c.n == 0 {
			result++
		}
		i++
	}
	return result
}

// WriteTo writes b to w.
func (b *Bitmap) WriteTo(w io.Writer) (n int64, err error) {
	// Remove empty containers before persisting.
	//b.removeEmptyContainers()
	containerCount := len(b.keys) - b.countEmptyContainers()

	// Build header before writing individual container blocks.
	buf := make([]byte, headerSize+(containerCount*(4+8+4)))
	binary.LittleEndian.PutUint32(buf[0:], cookie)
	binary.LittleEndian.PutUint32(buf[4:], uint32(containerCount))
	empty := 0
	// Encode keys and cardinality.
	for i, key := range b.keys {
		c := b.containers[i]

		// Verify container count before writing.
		// TODO: instead of commenting this out, we need to make it a configuration option
		//count := c.count()
		//assert(c.count() == c.n, "cannot write container count, mismatch: count=%d, n=%d", count, c.n)
		if c.n > 0 {
			binary.LittleEndian.PutUint64(buf[headerSize+(i-empty)*12:], uint64(key))
			binary.LittleEndian.PutUint32(buf[headerSize+(i-empty)*12+8:], uint32(c.n-1))
		} else {
			empty++
		}
	}

	// Write the offset for each container block.
	offset := uint32(len(buf))
	empty = 0
	for i, c := range b.containers {

		if c.n > 0 {
			binary.LittleEndian.PutUint32(buf[headerSize+(containerCount*12)+((i-empty)*4):], uint32(offset))
		} else {
			empty++
		}
		offset += uint32(c.size())
	}

	// Write header.
	i, err := w.Write(buf)
	n += int64(i)
	if err != nil {
		return n, err
	}

	// Write each container block.
	for _, c := range b.containers {
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

// UnmarshalBinary decodes b from a binary-encoded byte slice.
func (b *Bitmap) UnmarshalBinary(data []byte) error {
	if len(data) < headerSize {
		return errors.New("data too small")
	}

	// Verify the first 4 bytes are the correct cookie.
	if v := binary.LittleEndian.Uint32(data[0:4]); v != cookie {
		return errors.New("invalid roaring file")
	}

	// Read key count.
	keyN := binary.LittleEndian.Uint32(data[4:8])
	b.keys = make([]uint64, keyN)
	b.containers = make([]*container, keyN)

	// Read container key headers.
	for i, buf := 0, data[8:]; i < int(keyN); i, buf = i+1, buf[12:] {
		b.keys[i] = binary.LittleEndian.Uint64(buf[0:8])
		b.containers[i] = &container{
			n:      int(binary.LittleEndian.Uint32(buf[8:12])) + 1,
			mapped: true,
		}
	}

	// Read container offsets and attach data.
	opsOffset := 8 + int(keyN)*12
	for i, buf := 0, data[opsOffset:]; i < int(keyN); i, buf = i+1, buf[4:] {
		offset := binary.LittleEndian.Uint32(buf[0:4])

		// Verify the offset is within the bounds of the input data.
		if int(offset) >= len(data) {
			return fmt.Errorf("offset out of bounds: off=%d, len=%d", offset, len(data))
		}

		// Map byte slice directly to the container data.
		c := b.containers[i]
		if c.n <= ArrayMaxSize {
			c.array = (*[0xFFFFFFF]uint32)(unsafe.Pointer(&data[offset]))[:c.n]
			// TODO: instead of commenting this out, we need to make it a configuration option
			//for _, v := range c.array {
			//    assert(lowbits(uint64(v)) == v, "array value out of range: %d", v)
			//}
			opsOffset = int(offset) + len(c.array)*4
		} else {
			c.bitmap = (*[0xFFFFFFF]uint64)(unsafe.Pointer(&data[offset]))[:bitmapN]
			opsOffset = int(offset) + len(c.bitmap)*8
		}

		// Verify container count on load.
		// TODO: instead of commenting this out, we need to make it a configuration option
		//count := c.count()
		//assert(c.count() == c.n, "container count mismatch: count=%d, n=%d", count, c.n)
	}

	// Read ops log until the end of the file.
	buf := data[opsOffset:]
	for {
		// Exit when there are no more ops to parse.
		if len(buf) == 0 {
			break
		}

		// Unmarshal the op and apply it.
		var op op
		if err := op.UnmarshalBinary(buf); err != nil {
			// FIXME(benbjohnson): return error with position so file can be trimmed.
			return err
		}
		op.apply(b)

		// Increase the op count.
		b.opN++

		// Move the buffer forward.
		buf = buf[op.size():]
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

	b.opN++
	return nil
}

// Iterator returns a new iterator for the bitmap.
func (b *Bitmap) Iterator() *Iterator {
	itr := &Iterator{bitmap: b}
	itr.Seek(0)
	return itr
}

// Info returns stats for the bitmap.
func (b *Bitmap) Info() BitmapInfo {
	info := BitmapInfo{
		OpN:        b.opN,
		Containers: make([]ContainerInfo, len(b.containers)),
	}

	for i, c := range b.containers {
		ci := c.info()
		ci.Key = b.keys[i]
		info.Containers[i] = ci
	}

	return info
}

// Check performs a consistency check on the bitmap. Returns nil if consistent.
func (b *Bitmap) Check() error {
	var a ErrorList

	// Check keys/containers match. Return immediately if this happens.
	if len(b.keys) != len(b.containers) {
		a.Append(fmt.Errorf("key/container count mismatch: %d != %d", len(b.keys), len(b.containers)))
		return a
	}

	// Check each container.
	for i, c := range b.containers {
		if err := c.check(); err != nil {
			a.AppendWithPrefix(err, fmt.Sprintf("%d/", b.keys[i]))
		}
	}

	if len(a) == 0 {
		return nil
	}
	return a
}

//Perform a logical negate of the bits in the range [start,end].
func (b *Bitmap) Flip(start, end uint64) *Bitmap {
	result := NewBitmap()
	itr := b.Iterator()
	v, eof := itr.Next()
	//copy over previous bits.
	for v < start && !eof {
		result.add(v)
		v, eof = itr.Next()
	}
	//flip bits in range .
	for i := start; i <= end; i++ {
		if eof {
			result.add(i)
		} else if v == i {
			v, eof = itr.Next()
		} else {
			result.add(i)
		}
	}
	//add remaining.
	for !eof {
		result.add(v)
		v, eof = itr.Next()
	}
	return result
}

// BitmapInfo represents a point-in-time snapshot of bitmap stats.
type BitmapInfo struct {
	OpN        int
	Containers []ContainerInfo
}

// Iterator represents an iterator over a Bitmap.
type Iterator struct {
	bitmap *Bitmap
	i, j   int
}

// eof returns true if the iterator is at the end of the bitmap.
func (itr *Iterator) eof() bool { return itr.i >= len(itr.bitmap.containers) }

// Seek moves to the first value equal to or greater than v.
func (itr *Iterator) Seek(seek uint64) {
	// Move to the correct container.
	itr.i = search64(itr.bitmap.keys, highbits(seek))
	if itr.i < 0 {
		itr.i = -itr.i - 1
	}
	if itr.eof() {
		return
	}

	// Move to the correct value index inside the array container.
	lb := lowbits(seek)
	if c := itr.bitmap.containers[itr.i]; c.isArray() {
		// Find index in the container.
		itr.j = search32(c.array, lb)
		if itr.j < 0 {
			itr.j = -itr.j - 1
		}
		if itr.j < len(c.array) {
			itr.j--
			return
		}

		// If it's at the end of the container then move to the next one.
		itr.i, itr.j = itr.i+1, -1
		return
	}

	// If it's a bitmap container then move to index before the value and call next().
	itr.j = int(lb) - 1
}

// Next returns the next value in the bitmap.
// Returns eof as true if there are no values left in the iterator.
func (itr *Iterator) Next() (v uint64, eof bool) {
	// Iterate over containers until we find the next value or EOF.
	for {
		if itr.eof() {
			return 0, true
		}

		// Move to the next item in the container if it's an array container.
		c := itr.bitmap.containers[itr.i]
		if c.isArray() {
			if itr.j >= c.n-1 {
				itr.i, itr.j = itr.i+1, -1
				continue
			}
			itr.j++
			return itr.peek(), false
		}
		// Move to the next possible index in the bitmap container.
		itr.j++

		// Find first non-zero bit in current bitmap, if possible.
		hb := int(itr.j / 64)

		if hb >= len(c.bitmap) {
			itr.i, itr.j = itr.i+1, -1
			continue
		}
		lb := c.bitmap[hb] >> (uint(itr.j) % 64)
		if lb != 0 {
			itr.j = int(itr.j) + trailingZeroN(lb)
			return itr.peek(), false
		}

		// Otherwise iterate through remaining bitmaps to find next bit.
		for hb++; hb < len(c.bitmap); hb++ {
			if c.bitmap[hb] != 0 {
				itr.j = int(hb*64) + trailingZeroN(c.bitmap[hb])
				return itr.peek(), false
			}
		}

		// If no bits found then move to the next container.
		itr.i, itr.j = itr.i+1, -1
	}
}

// peek returns the current value.
func (itr *Iterator) peek() uint64 {
	key := itr.bitmap.keys[itr.i]
	c := itr.bitmap.containers[itr.i]
	if c.isArray() {
		return uint64(key)<<16 | uint64(c.array[itr.j])
	}
	return uint64(key)<<16 | uint64(itr.j)
}

// BufIterator wraps an iterator to provide the ability to unread values.
type BufIterator struct {
	buf struct {
		v    uint64
		eof  bool
		full bool
	}
	itr *Iterator
}

// NewBufIterator returns a buffered iterator that wraps itr.
func NewBufIterator(itr *Iterator) *BufIterator {
	return &BufIterator{itr: itr}
}

// Seek moves to the first pair equal to or greater than pseek/bseek.
func (itr *BufIterator) Seek(v uint64) {
	itr.buf.full = false
	itr.itr.Seek(v)
}

// Next returns the next pair in the bitmap.
// If a value has been buffered then it is returned and the buffer is cleared.
func (itr *BufIterator) Next() (v uint64, eof bool) {
	if itr.buf.full {
		itr.buf.full = false
		return itr.buf.v, itr.buf.eof
	}

	// Read value onto buffer in case of unread.
	itr.buf.v, itr.buf.eof = itr.itr.Next()
	return itr.buf.v, itr.buf.eof
}

// Peek reads the next value but leaves it on the buffer.
func (itr *BufIterator) Peek() (v uint64, eof bool) {
	v, eof = itr.Next()
	itr.Unread()
	return
}

// Unread pushes previous pair on to the buffer.
// Panics if the buffer is already full.
func (itr *BufIterator) Unread() {
	if itr.buf.full {
		panic("roaring.BufIterator: buffer full")
	}
	itr.buf.full = true
}

// The maximum size of array containers.
const ArrayMaxSize = 4096

// container represents a container for uint32 integers.
//
// These are used for storing the low bits. Containers are separated into two
// types depending on cardinality. For containers with less than 4,096 values,
// an array container is used. For containers with more than 4,096 values,
// the values are encoded into bitmaps.
type container struct {
	n      int      // number of integers in container
	array  []uint32 // used for array containers
	bitmap []uint64 // used for bitmap containers
	mapped bool     // mapped directly to a byte slice when true
}

// newContainer returns a new instance of container.
func newContainer() *container {
	return &container{}
}

// isArray returns true if the container is an array container.
func (c *container) isArray() bool { return c.bitmap == nil }

// unmap creates copies of the containers data in the heap.
//
// This is performed when altering the container since its contents could be
// pointing at a read-only mmap.
func (c *container) unmap() {
	if !c.mapped {
		return
	}

	if c.array != nil {
		tmp := make([]uint32, len(c.array))
		copy(tmp, c.array)
		c.array = tmp
	}
	if c.bitmap != nil {
		tmp := make([]uint64, len(c.bitmap))
		copy(tmp, c.bitmap)
		c.bitmap = tmp
	}
	c.mapped = false
}

// count counts all bits in the container.
func (c *container) count() (n int) {
	return c.countRange(0, (bitmapN*64)+1)
}

// countRange counts the number of bits set between [start, end).
func (c *container) countRange(start, end uint32) (n int) {
	if c.isArray() {
		return c.arrayCountRange(start, end)
	}
	return c.bitmapCountRange(start, end)
}

func (c *container) arrayCountRange(start, end uint32) (n int) {
	i := sort.Search(len(c.array), func(i int) bool { return c.array[i] >= start })
	for ; i < len(c.array); i++ {
		v := c.array[i]
		if v >= end {
			break
		}
		n++
	}
	return n
}

func (c *container) bitmapCountRange(start, end uint32) int {
	var n uint64
	i, j := start/64, end/64

	// Special case when start and end fall in the same word.
	if i == j {
		offi, offj := start%64, 64-end%64
		n += popcount((c.bitmap[i] >> offi) << (offj + offi))
		return int(n)
	}

	// Count partial starting word.
	if off := start % 64; off != 0 {
		n += popcount(c.bitmap[i] >> off)
		i++
	}

	// Count words in between.
	for ; i < j; i++ {
		n += popcount(c.bitmap[i])
	}

	// Count partial ending word.
	if int(j) < len(c.bitmap) {
		off := 64 - (end % 64)
		n += popcount(c.bitmap[j] << off)
	}

	return int(n)
}

// add adds a value to the container.
func (c *container) add(v uint32) bool {
	if c.isArray() {
		return c.arrayAdd(v)
	}
	return c.bitmapAdd(v)
}

func (c *container) arrayAdd(v uint32) bool {
	// Optimize appending to the end of an array container.
	if c.n > 0 && c.n < ArrayMaxSize && c.isArray() && c.array[c.n-1] < v {
		c.unmap()
		c.array = append(c.array, v)
		c.n++
		return true
	}

	// Find index of the integer in the container. Exit if it already exists.
	i := search32(c.array, v)
	if i >= 0 {
		return false
	}

	// Convert to a bitmap container if too many values are in an array container.
	if c.n >= ArrayMaxSize {
		c.convertToBitmap()
		return c.bitmapAdd(v)
	}

	// Otherwise insert into array.
	c.unmap()
	i = -i - 1
	c.array = append(c.array, 0)
	copy(c.array[i+1:], c.array[i:])
	c.array[i] = v
	c.n++
	return true
}

func (c *container) bitmapAdd(v uint32) bool {
	if c.bitmapContains(v) {
		return false
	}
	c.unmap()
	c.bitmap[v/64] |= (1 << uint64(v%64))
	c.n++
	return true
}

// contains returns true if v is in the container.
func (c *container) contains(v uint32) bool {
	if c.isArray() {
		return c.arrayContains(v)
	}
	return c.bitmapContains(v)
}

func (c *container) arrayContains(v uint32) bool {
	return search32(c.array, v) >= 0
}

func (c *container) bitmapContains(v uint32) bool {
	return (c.bitmap[v/64] & (1 << uint64(v%64))) != 0
}

// remove adds a value to the container.
func (c *container) remove(v uint32) bool {
	if c.isArray() {
		return c.arrayRemove(v)
	}
	return c.bitmapRemove(v)
}

func (c *container) arrayRemove(v uint32) bool {
	i := search32(c.array, v)
	if i < 0 {
		return false
	}
	c.unmap()

	c.n--
	c.array = append(c.array[:i], c.array[i+1:]...)
	return true
}

func (c *container) bitmapRemove(v uint32) bool {
	if !c.bitmapContains(v) {
		return false
	}
	c.unmap()

	// Lower count and remove element.
	c.n--
	c.bitmap[v/64] &^= (uint64(1) << (v % 64))

	// Convert to array if we go below the threshold.
	if c.n == ArrayMaxSize {
		c.convertToArray()
	}
	return true
}

// max returns the maximum value in the container.
func (c *container) max() uint32 {
	if c.isArray() {
		return c.arrayMax()
	}
	return c.bitmapMax()
}

func (c *container) arrayMax() uint32 {
	if len(c.array) == 0 {
		return 0 //probably hiding some ugly bug but it prevents a crash
	}
	return c.array[len(c.array)-1]
}

func (c *container) bitmapMax() uint32 {
	// Search bitmap in reverse order.
	for i := len(c.bitmap) - 1; i >= 0; i-- {
		// If value is zero then skip.
		v := c.bitmap[i]
		if v == 0 {
			continue
		}

		// Find the highest set bit.
		for j := uint32(63); j >= 0; j-- {
			if v&(1<<j) != 0 {
				return uint32(i)*64 + j
			}
		}
	}
	return 0
}

// convertToArray converts the values in the bitmap to array values.
func (c *container) convertToArray() {
	c.array = make([]uint32, 0, c.n)
	for i, bitmap := range c.bitmap {
		for bitmap != 0 {
			t := bitmap & -bitmap
			c.array = append(c.array, uint32((i*64 + int(popcount(t-1)))))
			bitmap ^= t
		}
	}
	c.bitmap = nil
	c.mapped = false
}

// convertToBitmap converts the values in array to bitmap values.
func (c *container) convertToBitmap() {
	c.bitmap = make([]uint64, bitmapN)
	for _, v := range c.array {
		c.bitmap[int(v)/64] |= (uint64(1) << uint(v%64))
	}
	c.array = nil
	c.mapped = false
}

// clone returns a copy of c.
func (c *container) clone() *container {
	other := &container{n: c.n}

	if c.array != nil {
		other.array = make([]uint32, len(c.array))
		copy(other.array, c.array)
	}

	if c.bitmap != nil {
		other.bitmap = make([]uint64, len(c.bitmap))
		copy(other.bitmap, c.bitmap)
	}

	return other
}

// WriteTo writes c to w.
func (c *container) WriteTo(w io.Writer) (n int64, err error) {
	if c.isArray() {
		return c.arrayWriteTo(w)
	}
	return c.bitmapWriteTo(w)
}

func (c *container) arrayWriteTo(w io.Writer) (n int64, err error) {
	if len(c.array) == 0 {
		return 0, nil
	}

	// Verify all elements are valid.
	// TODO: instead of commenting this out, we need to make it a configuration option
	//for _, v := range c.array {
	//  	assert(lowbits(uint64(v)) == v, "cannot write array value out of range: %d", v)
	//}

	nn, err := w.Write((*[0xFFFFFFF]byte)(unsafe.Pointer(&c.array[0]))[:4*c.n])
	return int64(nn), err
}

func (c *container) bitmapWriteTo(w io.Writer) (n int64, err error) {
	nn, err := w.Write((*[0xFFFFFFF]byte)(unsafe.Pointer(&c.bitmap[0]))[:(8 * bitmapN)])
	return int64(nn), err
}

// size returns the encoded size of the container, in bytes.
func (c *container) size() int {
	if c.isArray() {
		return len(c.array) * 4
	}
	return len(c.bitmap) * 8
}

// info returns the current stats about the container.
func (c *container) info() ContainerInfo {
	info := ContainerInfo{N: c.n}

	if c.isArray() {
		info.Type = "array"
		info.Alloc = len(c.array) * 4
	} else {
		info.Type = "bitmap"
		info.Alloc = len(c.bitmap) * 8
	}

	if c.mapped {
		if c.isArray() {
			info.Pointer = unsafe.Pointer(&c.array[0])
		} else {
			info.Pointer = unsafe.Pointer(&c.bitmap[0])
		}
	}

	return info
}

// check performs a consistency check on the container.
func (c *container) check() error {
	var a ErrorList

	if c.n <= ArrayMaxSize {
		if len(c.array) != c.n {
			a.Append(fmt.Errorf("array count mismatch: count=%d, n=%d", len(c.array), c.n))
		}
	} else {
		if n := c.bitmapCountRange(0, uint32(len(c.bitmap)*64)); n != c.n {
			a.Append(fmt.Errorf("bitmap count mismatch: count=%d, n=%d", n, c.n))
		}
	}

	if a == nil {
		return nil
	}
	return a
}

// ContainerInfo represents a point-in-time snapshot of container stats.
type ContainerInfo struct {
	Key     uint64         // container key
	Type    string         // container type (array or bitmap)
	N       int            // number of bits
	Alloc   int            // memory used
	Pointer unsafe.Pointer // offset within the mmap
}

func intersectionCount(a, b *container) uint64 {
	if a.isArray() {
		if b.isArray() {
			return intersectionCountArrayArray(a, b)
		} else {
			return intersectionCountArrayBitmap(a, b)
		}
	} else {
		if b.isArray() {
			return intersectionCountArrayBitmap(b, a)
		} else {
			return intersectionCountBitmapBitmap(a, b)
		}
	}
}

func intersectionCountArrayArray(a, b *container) (n uint64) {
	na, nb := len(a.array), len(b.array)
	for i, j := 0, 0; i < na && j < nb; {
		va, vb := a.array[i], b.array[j]
		if va < vb {
			i++
		} else if va > vb {
			j++
		} else {
			n++
			i, j = i+1, j+1
		}
	}
	return n
}

func intersectionCountArrayBitmapOld(a, b *container) (n uint64) {
	// Copy array header so we can shrink it.
	array := a.array
	if len(array) == 0 {
		return 0
	}

	// Iterate over bitmap and find matching bits.
	for i, bn := uint32(0), uint32(len(b.bitmap)); i < bn; i++ {
		v := b.bitmap[i]

		// Ignore if bytes are empty or array is done.
		if v == 0 {
			continue
		}

		// Check each bit.
		for j := uint32(0); j < 64; j++ {
			if v&(1<<j) == 0 {
				continue
			}

			// Search array until match.
			bv := (i * 64) + j
			for {
				if len(array) == 0 {
					return n
				} else if array[0] < bv {
					array = array[1:]
				} else if array[0] == bv {
					n++
					break
				} else {
					break
				}
			}
		}
	}

	return n
}

func intersectionCountArrayBitmap(a, b *container) (n uint64) {
	for _, val := range a.array {
		i := val / 64
		if i >= uint32(len(b.bitmap)) {
			break
		}
		off := val % 64
		n += (b.bitmap[i] & (1 << off)) >> off
	}
	return n
}

func intersectionCountBitmapBitmap(a, b *container) (n uint64) {
	return popcntAndSlice(a.bitmap, b.bitmap)
}

func intersect(a, b *container) *container {
	if a.isArray() {
		if b.isArray() {
			return intersectArrayArray(a, b)
		} else {
			return intersectArrayBitmap(a, b)
		}
	} else {
		if b.isArray() {
			return intersectArrayBitmap(b, a)
		} else {
			return intersectBitmapBitmap(a, b)
		}
	}
}

func intersectArrayArray(a, b *container) *container {
	output := &container{}
	na, nb := len(a.array), len(b.array)
	for i, j := 0, 0; i < na && j < nb; {
		va, vb := a.array[i], b.array[j]
		if va < vb {
			i++
		} else if va > vb {
			j++
		} else {
			output.array = append(output.array, va)
			i, j = i+1, j+1
		}
	}
	output.n = len(output.array)
	return output
}

func intersectArrayBitmap(a, b *container) *container {
	output := &container{}
	itr := newBufIterator(newBitmapIterator(b.bitmap))
	for i := 0; i < len(a.array); {
		va := a.array[i]
		vb, eof := itr.next()
		if eof {
			break
		}

		if va < vb {
			i++
			itr.unread()
		} else if va > vb {
			// nop
		} else {
			output.add(va)
			i++
		}
	}
	return output
}

func intersectBitmapBitmap(a, b *container) *container {
	output := &container{}
	itr0 := newBufIterator(newBitmapIterator(a.bitmap))
	itr1 := newBufIterator(newBitmapIterator(b.bitmap))
	for {
		va, eof := itr0.next()
		if eof {
			break
		}

		vb, eof := itr1.next()
		if eof {
			break
		}

		if va < vb {
			itr1.unread()
		} else if va > vb {
			itr0.unread()
		} else {
			output.add(va)
		}
	}
	return output
}

func union(a, b *container) *container {
	if a.isArray() {
		if b.isArray() {
			return unionArrayArray(a, b)
		} else {
			return unionArrayBitmap(a, b)
		}
	} else {
		if b.isArray() {
			return unionArrayBitmap(b, a)
		} else {
			return unionBitmapBitmap(a, b)
		}
	}
}

func unionArrayArray(a, b *container) *container {
	output := &container{}
	na, nb := len(a.array), len(b.array)
	for i, j := 0, 0; ; {
		if i >= na && j >= nb {
			break
		} else if i < na && j >= nb {
			output.add(a.array[i])
			i++
			continue
		} else if i >= na && j < nb {
			output.add(b.array[j])
			j++
			continue
		}

		va, vb := a.array[i], b.array[j]
		if va < vb {
			output.add(va)
			i++
		} else if va > vb {
			output.add(vb)
			j++
		} else {
			output.add(va)
			i, j = i+1, j+1
		}
	}
	return output
}

func unionArrayBitmap(a, b *container) *container {
	output := &container{}
	itr := newBufIterator(newBitmapIterator(b.bitmap))
	for i := 0; ; {
		vb, eof := itr.next()
		if i >= len(a.array) && eof {
			break
		} else if i >= len(a.array) {
			output.add(vb)
			continue
		} else if eof {
			output.add(a.array[i])
			i++
			continue
		}

		va := a.array[i]
		if va < vb {
			output.add(va)
			i++
			itr.unread()
		} else if va > vb {
			output.add(vb)
		} else {
			output.add(va)
			i++
		}
	}
	return output
}

func unionBitmapBitmap(a, b *container) *container {
	output := &container{
		bitmap: make([]uint64, bitmapN),
	}

	for i := 0; i < bitmapN; i++ {
		v := a.bitmap[i] | b.bitmap[i]
		output.bitmap[i] = v
		output.n += int(popcnt(v))
	}

	return output
}

func difference(a, b *container) *container {
	if a.isArray() {
		if b.isArray() {
			return differenceArrayArray(a, b)
		} else {
			return differenceArrayBitmap(a, b)
		}
	} else {
		if b.isArray() {
			return differenceBitmapArray(a, b)
		} else {
			return differenceBitmapBitmap(a, b)
		}
	}
}

func differenceArrayArray(a, b *container) *container {
	output := &container{}
	na, nb := len(a.array), len(b.array)
	for i, j := 0, 0; i < na; {
		va := a.array[i]
		if j >= nb {
			output.add(va)
			i++
			continue
		}

		vb := b.array[j]
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

func differenceArrayBitmap(a, b *container) *container {
	output := &container{}
	itr := newBufIterator(newBitmapIterator(b.bitmap))
	for i := 0; i < len(a.array); {
		va := a.array[i]
		vb, eof := itr.next()
		if eof {
			output.add(va)
			i++
			continue
		}

		if va < vb {
			output.add(va)
			i++
			itr.unread()
		} else if va > vb {
			// nop
		} else {
			i++
		}
	}
	return output
}

func differenceBitmapArray(a, b *container) *container {
	output := &container{}
	itr := newBufIterator(newBitmapIterator(a.bitmap))
	array := b.array
	for {
		va, eof := itr.next()
		if eof {
			break
		}

		if len(array) == 0 {
			output.add(va)
			continue
		}

		vb := array[0]
		if va < vb {
			output.add(va)
		} else if va > vb {
			array = array[1:]
			itr.unread()
		} else {
			array = array[1:]
		}
	}
	return output
}

func differenceBitmapBitmap(a, b *container) *container {
	output := &container{}
	itr0 := newBufIterator(newBitmapIterator(a.bitmap))
	itr1 := newBufIterator(newBitmapIterator(b.bitmap))
	for {
		v0, eof0 := itr0.next()
		v1, eof1 := itr1.next()

		if eof0 {
			break
		} else if eof1 {
			output.add(v0)
		} else if v0 < v1 {
			output.add(v0)
			itr1.unread()
		} else if v0 > v1 {
			itr0.unread()
		}
	}
	return output
}

func xor(a, b *container) *container {
	if a.isArray() {
		if b.isArray() {
			return xorArrayArray(a, b)
		} else {
			return xorArrayBitmap(a, b)
		}
	} else {
		if b.isArray() {
			return xorArrayBitmap(b, a)
		} else {
			return xorBitmapBitmap(a, b)
		}
	}
}

func xorArrayArray(a, b *container) *container {
	output := &container{}
	na, nb := len(a.array), len(b.array)
	for i, j := 0, 0; i < na || j < nb; {
		if i < na && j >= nb {
			output.add(a.array[i])
			i++
			continue
		} else if i >= na && j < nb {
			output.add(b.array[j])
			j++
			continue
		}

		va, vb := a.array[i], b.array[j]
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

func xorArrayBitmap(a, b *container) *container {
	output := b.clone()
	for _, v := range a.array {
		if b.bitmapContains(v) {
			output.remove(v)
		} else {
			output.add(v)
		}
	}

	if output.count() < ArrayMaxSize {
		output.convertToArray()
	}

	return output
}

func xorBitmapBitmap(a, b *container) *container {
	output := &container{
		bitmap: make([]uint64, bitmapN),
	}

	for i := 0; i < bitmapN; i++ {
		v := a.bitmap[i] ^ b.bitmap[i]
		output.bitmap[i] = v
		output.n += int(popcnt(v))
	}

	if output.count() < ArrayMaxSize {
		output.convertToArray()
	}
	return output
}

// opType represents a type of operation.
type opType uint8

const (
	opTypeAdd    = opType(0)
	opTypeRemove = opType(1)
)

// op represents an operation on the bitmap.
type op struct {
	typ   opType
	value uint64
}

// apply executes the operation against a bitmap.
func (op *op) apply(b *Bitmap) bool {
	switch op.typ {
	case opTypeAdd:
		return b.add(op.value)
	case opTypeRemove:
		return b.remove(op.value)
	default:
		panic(fmt.Sprintf("invalid op type: %d", op.typ))
	}
	return false
}

// WriteTo writes op to the w.
func (op *op) WriteTo(w io.Writer) (n int64, err error) {
	buf := make([]byte, op.size())

	// Write type and value.
	buf[0] = byte(op.typ)
	binary.LittleEndian.PutUint64(buf[1:9], op.value)

	// Add checksum at the end.
	h := fnv.New32a()
	h.Write(buf[0:9])
	binary.LittleEndian.PutUint32(buf[9:13], h.Sum32())

	// Write to writer.
	nn, err := w.Write(buf)
	return int64(nn), err
}

// UnmarshalBinary decodes data into an op.
func (op *op) UnmarshalBinary(data []byte) error {
	if len(data) < op.size() {
		return fmt.Errorf("op data out of bounds: len=%d", len(data))
	}

	// Verify checksum.
	h := fnv.New32a()
	h.Write(data[0:9])
	if chk := binary.LittleEndian.Uint32(data[9:13]); chk != h.Sum32() {
		return fmt.Errorf("checksum mismatch: exp=%08x, got=%08x", h.Sum32(), chk)
	}

	// Read type and value.
	op.typ = opType(data[0])
	op.value = binary.LittleEndian.Uint64(data[1:9])

	return nil
}

// size returns the encoded size of the op, in bytes.
func (*op) size() int { return 1 + 8 + 4 }

func highbits(v uint64) uint64 { return uint64(v >> 16) }
func lowbits(v uint64) uint32  { return uint32(v & 0xFFFF) }

// search32 returns the index of v in a.
func search32(a []uint32, value uint32) int {
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

// search64 returns the index of v in a.
func search64(a []uint64, value uint64) int {
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
	n := int64(63)
	if y := v << 32; y != 0 {
		n, v = n-32, y
	}
	if y := v << 16; y != 0 {
		n, v = n-16, y
	}
	if y := v << 8; y != 0 {
		n, v = n-8, y
	}
	if y := v << 4; y != 0 {
		n, v = n-4, y
	}
	if y := v << 2; y != 0 {
		n, v = n-2, y
	}
	return int(n - int64(uint64(v<<1)>>63))
}

// bit population count, taken from
// https://code.google.com/p/go/issues/detail?id=4988#c11
// credit: https://code.google.com/u/arnehormann/
func popcount(x uint64) (n uint64) {
	x -= (x >> 1) & 0x5555555555555555
	x = (x>>2)&0x3333333333333333 + x&0x3333333333333333
	x += x >> 4
	x &= 0x0f0f0f0f0f0f0f0f
	x *= 0x0101010101010101
	return x >> 56
}

// bitmapIterator represents an iterator over container bitmap values.
type bitmapIterator struct {
	bitmap []uint64
	i      int
}

func newBitmapIterator(bitmap []uint64) *bitmapIterator {
	return &bitmapIterator{
		bitmap: bitmap,
		i:      -1,
	}
}

// next returns the next value in the bitmap.
// Returns eof as true if there are no values left in the iterator.
func (itr *bitmapIterator) next() (v uint32, eof bool) {
	if itr.i+1 >= len(itr.bitmap)*64 {
		return 0, true
	}
	itr.i++

	// Find first non-zero bit in current bitmap, if possible.
	hb := int(itr.i / 64)
	lb := itr.bitmap[hb] >> (uint(itr.i) % 64)
	if lb != 0 {
		itr.i = int(itr.i) + trailingZeroN(lb)
		return uint32(itr.i), false
	}

	// Otherwise iterate through remaining bitmaps to find next bit.
	for hb++; hb < len(itr.bitmap); hb++ {
		if itr.bitmap[hb] != 0 {
			itr.i = int(hb*64) + trailingZeroN(itr.bitmap[hb])
			return uint32(itr.i), false
		}
	}

	return 0, true
}

// bufBitmapIterator wraps an iterator to provide the ability to unread values.
type bufBitmapIterator struct {
	buf struct {
		v    uint32
		eof  bool
		full bool
	}
	itr *bitmapIterator
}

// newBufBitmapIterator returns a buffered iterator that wraps a bitmapIterator.
func newBufIterator(itr *bitmapIterator) *bufBitmapIterator {
	return &bufBitmapIterator{itr: itr}
}

// next returns the next pair in the bitmap.
// If a value has been buffered then it is returned and the buffer is cleared.
func (itr *bufBitmapIterator) next() (v uint32, eof bool) {
	if itr.buf.full {
		itr.buf.full = false
		return itr.buf.v, itr.buf.eof
	}

	// Read value onto buffer in case of unread.
	itr.buf.v, itr.buf.eof = itr.itr.next()
	return itr.buf.v, itr.buf.eof
}

// unread pushes previous pair on to the buffer. Panics if the buffer is already full.
func (itr *bufBitmapIterator) unread() {
	if itr.buf.full {
		panic("roaring.bufBitmapIterator: buffer full")
	}
	itr.buf.full = true
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

// assert panics with a formatted message if condition is false.
func assert(condition bool, format string, a ...interface{}) {
	if !condition {
		panic(fmt.Sprintf(format, a...))
	}
}
