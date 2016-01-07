// package roaring implements roaring bitmaps with support for incremental changes.
package roaring

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"unsafe"
)

const (
	// cookie is the first four bytes in a roaring bitmap file.
	cookie = uint32(12346)

	// headerSize is the size of the cookie and key count at the beginning of a file.
	headerSize = 4 + 4

	// bitmapN is the number of values in a container.bitmap.
	bitmapN = (1 << 16) / 64
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

// Add adds values to the bitmap.
func (b *Bitmap) Add(a ...uint64) error {
	for _, v := range a {
		// Create an add operation.
		op := &op{typ: opTypeAdd, value: v}

		// Write operation to op log.
		if err := b.writeOp(op); err != nil {
			return err
		}

		// Apply to the in-memory bitmap.
		op.apply(b)
	}

	return nil
}

func (b *Bitmap) add(v uint64) {
	hb := highbits(v)
	i := search64(b.keys, hb)

	// If index is negative then there's not an exact match
	// and a container needs to be added.
	if i < 0 {
		b.insertAt(hb, newContainer(), -i-1)
		i = -i - 1
	}

	b.containers[i].add(lowbits(v))
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
func (b *Bitmap) Remove(a ...uint64) error {
	for _, v := range a {
		// Create an add operation.
		op := &op{typ: opTypeRemove, value: v}

		// Write operation to op log.
		if err := b.writeOp(op); err != nil {
			return err
		}

		// Apply operation to the bitmap.
		op.apply(b)
	}
	return nil
}

func (b *Bitmap) remove(v uint64) {
	hb := highbits(v)
	i := search64(b.keys, hb)
	if i < 0 {
		return
	}
	b.containers[i].remove(lowbits(v))
}

// Slice returns a slice of all integers in the bitmap.
func (b *Bitmap) Slice() []uint64 {
	var a []uint64
	itr := b.iterator()
	for v := itr.Seek(0); !itr.EOF(); v = itr.Next() {
		a = append(a, v)
	}
	return a
}

// SliceRange returns a slice of integers between [start, end).
func (b *Bitmap) SliceRange(start, end uint64) []uint64 {
	var a []uint64
	itr := b.iterator()
	for v := itr.Seek(start); !itr.EOF() && v < end; v = itr.Next() {
		a = append(a, v)
	}
	return a
}

// ForEach executes fn for each value in the bitmap.
func (b *Bitmap) ForEach(fn func(uint64)) {
	itr := b.iterator()
	for v := itr.Seek(0); !itr.EOF(); v = itr.Next() {
		fn(v)
	}
}

// ForEachRange executes fn for each value in the bitmap between [start, end).
func (b *Bitmap) ForEachRange(start, end uint64, fn func(uint64)) {
	itr := b.iterator()
	for v := itr.Seek(start); !itr.EOF() && v < end; v = itr.Next() {
		fn(v)
	}
}

// container returns the container with the given key.
func (b *Bitmap) container(key uint64) *container {
	i := search64(b.keys, key)
	if i < 0 {
		return nil
	}
	return b.containers[i]
}

func (b *Bitmap) insertAt(key uint64, c *container, i int) {
	b.keys = append(b.keys, 0)
	copy(b.keys[i+1:], b.keys[i:])
	b.keys[i] = key

	b.containers = append(b.containers, nil)
	copy(b.containers[i+1:], b.containers[i:])
	b.containers[i] = c
}

// WriteTo writes b to w.
func (b *Bitmap) WriteTo(w io.Writer) (n int64, err error) {
	// Build header before writing individual container blocks.
	buf := make([]byte, headerSize+(len(b.keys)*(2+8+4)))
	binary.LittleEndian.PutUint32(buf[0:], cookie)
	binary.LittleEndian.PutUint32(buf[4:], uint32(len(b.keys)))

	// Encode keys and cardinality.
	for i, key := range b.keys {
		binary.LittleEndian.PutUint64(buf[headerSize+i*10:], uint64(key))
		binary.LittleEndian.PutUint16(buf[headerSize+i*10+8:], uint16(b.containers[i].n-1))
	}

	// Write the offset for each container block.
	offset := uint32(len(buf))
	for i, c := range b.containers {
		binary.LittleEndian.PutUint32(buf[headerSize+(len(b.keys)*10)+(i*4):], uint32(offset))
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
		nn, err := c.WriteTo(w)
		n += nn
		if err != nil {
			return n, err
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
	for i, buf := 0, data[8:]; i < int(keyN); i, buf = i+1, buf[10:] {
		b.keys[i] = binary.LittleEndian.Uint64(buf[0:8])
		b.containers[i] = &container{
			n:      int(binary.LittleEndian.Uint16(buf[8:10])) + 1,
			mapped: true,
		}
	}

	// Read container offsets and attach data.
	opsOffset := 8 + int(keyN)*10
	for i, buf := 0, data[opsOffset:]; i < int(keyN); i, buf = i+1, buf[4:] {
		offset := binary.LittleEndian.Uint32(buf[0:4])

		// Verify the offset is within the bounds of the input data.
		if int(offset) >= len(data) {
			return fmt.Errorf("offset out of bounds: off=%d, len=%d", offset, len(data))
		}

		// Map byte slice directly to the container data.
		c := b.containers[i]
		if c.n <= arrayMaxSize {
			c.array = (*[0xFFFFFFF]uint16)(unsafe.Pointer(&data[offset]))[:c.n]
			opsOffset = int(offset) + len(c.array)*2
		} else {
			c.bitmap = (*[0xFFFFFFF]uint64)(unsafe.Pointer(&data[offset]))[:bitmapN]
			opsOffset = int(offset) + len(c.bitmap)*8
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
		var op op
		if err := op.UnmarshalBinary(buf); err != nil {
			// FIXME(benbjohnson): return error with position so file can be trimmed.
			return err
		}
		op.apply(b)

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

// iterator returns an iterator for the bitmap.
func (b *Bitmap) iterator() *iterator { return &iterator{bitmap: b} }

// iterator represents an iterator over a Bitmap.
type iterator struct {
	bitmap *Bitmap
	i, j   int
}

// EOF returns true if the iterator is at the end of the bitmap.
func (itr *iterator) EOF() bool { return itr.i >= len(itr.bitmap.containers) }

// Seek moves to the first value equal to or greater than v.
func (itr *iterator) Seek(seek uint64) uint64 {
	// Move to the correct container.
	itr.i = search64(itr.bitmap.keys, highbits(seek))
	if itr.i < 0 {
		itr.i = -itr.i - 1
	}
	if itr.EOF() {
		return 0
	}

	// Move to the correct value index inside the array container.
	lb := lowbits(seek)
	if c := itr.bitmap.containers[itr.i]; c.isArray() {
		// Find index in the container.
		itr.j = search16(c.array, lb)
		if itr.j < 0 {
			itr.j = -itr.j - 1
		}
		if itr.j < len(c.array) {
			return itr.peek()
		}

		// If it's at the end of the container then move to the next one.
		itr.i, itr.j = itr.i+1, -1
		return itr.Next()
	}

	// If it's a bitmap container then move to index before the value and call next().
	itr.j = int(lb) - 1
	return itr.Next()
}

// Next returns the next value in the bitmap.
func (itr *iterator) Next() uint64 {
	// Iterate over containers until we find the next value or EOF.
	for {
		if itr.EOF() {
			return 0
		}

		// Move to the next item in the container if it's an array container.
		c := itr.bitmap.containers[itr.i]
		if c.isArray() {
			if itr.j >= c.n-1 {
				itr.i, itr.j = itr.i+1, -1
				continue
			}
			itr.j++
			return itr.peek()
		}

		// Move to the next possible index in the bitmap container.
		itr.j++

		// Find first non-zero bit in current bitmap, if possible.
		hb := int(itr.j / 64)
		lb := c.bitmap[hb] >> (uint(itr.j) % 64)
		if lb != 0 {
			itr.j = int(itr.j) + trailingZeroN(lb)
			return itr.peek()
		}

		// Otherwise iterate through remaining bitmaps to find next bit.
		for hb++; hb < len(c.bitmap); hb++ {
			if c.bitmap[hb] != 0 {
				itr.j = int(hb*64) + trailingZeroN(c.bitmap[hb])
				return itr.peek()
			}
		}

		// If no bits found then move to the next container.
		itr.i, itr.j = itr.i+1, -1
	}
}

// peek returns the current value.
func (itr *iterator) peek() uint64 {
	key := itr.bitmap.keys[itr.i]
	c := itr.bitmap.containers[itr.i]
	if c.isArray() {
		return uint64(key)<<16 | uint64(c.array[itr.j])
	}
	return uint64(key)<<16 | uint64(itr.j)
}

// The maximum size of array containers.
const arrayMaxSize = 4096

// container represents a container for uint16 integers.
//
// These are used for storing the low bits. Containers are separated into two
// types depending on cardinality. For containers with less than 4,096 values,
// an array container is used. For containers with more than 4,096 values,
// the values are encoded into bitmaps.
type container struct {
	n      int      // number of integers in container
	array  []uint16 // used for array containers
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
		tmp := make([]uint16, len(c.array))
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

// add adds a value to the container.
func (c *container) add(v uint16) {
	if c.isArray() {
		c.arrayAdd(v)
		return
	}
	c.bitmapAdd(v)
}

func (c *container) arrayAdd(v uint16) {
	// Optimize appending to the end of an array container.
	if c.n > 0 && c.isArray() && c.array[c.n-1] < v {
		c.unmap()
		c.array = append(c.array, v)
		c.n++
		return
	}

	// Find index of the integer in the container. Exit if it already exists.
	i := search16(c.array, v)
	if i >= 0 {
		return
	}

	// Convert to a bitmap container if too many values are in an array container.
	if c.n >= arrayMaxSize {
		c.convertToBitmap()
		c.bitmapAdd(v)
		return
	}

	// Otherwise insert into array.
	c.unmap()
	i = -i - 1
	c.array = append(c.array, 0)
	copy(c.array[i+1:], c.array[i:])
	c.array[i] = v
	c.n++
}

func (c *container) bitmapAdd(v uint16) {
	if c.bitmapContains(v) {
		return
	}
	c.unmap()
	c.bitmap[v/64] |= (1 << uint64(v%64))
	c.n++
}

// contains returns true if v is in the container.
func (c *container) contains(v uint16) bool {
	if c.isArray() {
		return c.arrayContains(v)
	}
	return c.bitmapContains(v)
}

func (c *container) arrayContains(v uint16) bool {
	return search16(c.array, v) >= 0
}

func (c *container) bitmapContains(v uint16) bool {
	return (c.bitmap[v/64] & (1 << uint64(v%64))) != 0
}

// remove adds a value to the container.
func (c *container) remove(v uint16) {
	if c.isArray() {
		c.arrayRemove(v)
		return
	}
	c.bitmapRemove(v)
}

func (c *container) arrayRemove(v uint16) {
	i := search16(c.array, v)
	if i < 0 {
		return
	}
	c.unmap()

	c.n--
	c.array = append(c.array[:i], c.array[i+1:]...)
}

func (c *container) bitmapRemove(v uint16) {
	if !c.bitmapContains(v) {
		return
	}
	c.unmap()

	// Lower count and remove element.
	c.n--
	c.bitmap[v/64] &^= (uint64(1) << (v % 64))

	// Convert to array if we go below the threshold.
	if c.n == arrayMaxSize {
		c.convertToArray()
	}
}

// convertToArray converts the values in the bitmap to array values.
func (c *container) convertToArray() {
	c.array = make([]uint16, 0, c.n)
	for i, bitmap := range c.bitmap {
		for bitmap != 0 {
			t := bitmap & -bitmap
			c.array = append(c.array, uint16((i*64 + int(popcount(t-1)))))
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

// WriteTo writes c to w.
func (c *container) WriteTo(w io.Writer) (n int64, err error) {
	if c.isArray() {
		return c.arrayWriteTo(w)
	}
	return c.bitmapWriteTo(w)
}

func (c *container) arrayWriteTo(w io.Writer) (n int64, err error) {
	nn, err := w.Write((*[0xFFFFFFF]byte)(unsafe.Pointer(&c.array[0]))[:2*c.n])
	return int64(nn), err
}

func (c *container) bitmapWriteTo(w io.Writer) (n int64, err error) {
	nn, err := w.Write((*[0xFFFFFFF]byte)(unsafe.Pointer(&c.bitmap[0]))[:(8 * bitmapN)])
	return int64(nn), err
}

// size returns the encoded size of the container, in bytes.
func (c *container) size() int {
	if c.isArray() {
		return len(c.array) * 2
	}
	return len(c.bitmap) * 8
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
func (op *op) apply(b *Bitmap) {
	switch op.typ {
	case opTypeAdd:
		b.add(op.value)
	case opTypeRemove:
		b.remove(op.value)
	default:
		panic(fmt.Sprintf("invalid op type: %d", op.typ))
	}
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
func lowbits(v uint64) uint16  { return uint16(v & 0xFFFF) }

// search16 returns the index of v in a.
func search16(a []uint16, value uint16) int {
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
