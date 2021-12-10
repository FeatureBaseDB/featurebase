package roaring

import (
	"fmt"
	"unsafe"
)

const (
	stashedArraySize = 5
	stashedRunSize   = (stashedArraySize / 2)
)

// Container represents a Container for uint16 integers.
//
// These are used for storing the low bits of numbers in larger sets of uint64.
// The high bits are stored in a Container's key which is tracked by a separate
// data structure. Integers in a Container can be encoded in one of three ways -
// the encoding used is usually whichever is most compact, though any Container
// type should be able to encode any set of integers safely. For containers with
// less than 4,096 values, an array is often used. Containers with long runs of
// integers would use run length encoding, and more random data usually uses
// bitmap encoding.
//
// The Container type has somewhat magical semantics. Containers can be marked
// as "frozen" by the Freeze method, after which, nothing should ever modify
// that specific container object again, no matter what. Because of this, but
// also sometimes for Even More Esoteric Reasons, *no* container method should
// ever be assumed to be genuinely modifying the container it was called on,
// and *every* container method that might modify a container should return
// the "modified" *Container, which *may point to a different object*. The
// caller should always use this resulting container, and if you're storing
// a *Container in a data structure, you need to update the data structure's
// pointer too.
//
// A nil *Container is a valid empty container.
//
// In general, operations on containers which produce new containers *may*
// yield new containers, and *may* yield their operands.
//
// The reason for all of this is to allow containers to have copy-on-write
// semantics, which allow us to reduce memory usage dramatically, and GC
// load even more dramatically.
type Container struct {
	pointer  *uint16                  // the data pointer
	len, cap int32                    // length and cap
	n        int32                    // number of integers in container
	flags    containerFlags           // internal flags
	typeID   byte                     // array, bitmap, or run
	data     [stashedArraySize]uint16 // immediate data for small arrays or runs
}

type containerFlags uint8

var containerFlagStrings = [...]string{
	"",
	"mapped",
	"frozen",
	"frozen/mapped",
	"pristine",
	"pristine/mapped",
	"pristine/frozen",
	"pristine/frozen/mapped",
	"dirty",
	"mapped/dirty",
	"frozen/dirty",
	"frozen/mapped/dirty",
	"pristine/dirty",
	"pristine/mapped/dirty",
	"pristine/frozen/dirty",
	"pristine/frozen/mapped/dirty",
}

func (f containerFlags) String() string {
	return containerFlagStrings[f&15]
}

const (
	flagMapped   = containerFlags(1 << iota) // using memory-mapped or otherwise external storage
	flagFrozen                               // not modifiable
	flagPristine                             // flagPristine is used for mmapped containers referring to storage
	flagDirty                                // flagDirty is used for containers which may have invalid N
)

func (c *Container) String() string {
	if c == nil {
		return "<nil container>"
	}
	var space, froze string
	if c.flags != 0 {
		space = " "
		froze = c.flags.String()
	}
	switch c.typeID {
	case ContainerArray:
		return fmt.Sprintf("<%s%sarray container, N=%d>", froze, space, c.N())
	case ContainerBitmap:
		return fmt.Sprintf("<%s%sbitmap container, N=%d>",
			froze, space, c.N())
	case ContainerRun:
		return fmt.Sprintf("<%s%srun container, N=%d, len %dx interval>",
			froze, space, c.N(), len(c.runs()))
	default:
		return fmt.Sprintf("<unknown %s%s%d container, N=%d>", froze, space, c.typeID, c.N())
	}
}

// NewContainer returns a new instance of container. This trivial function
// may later become more interesting.
func NewContainer() *Container {
	statsHit("NewContainer")
	return NewContainerArray(nil)
}

// RemakeContainerBitmap overwrites the contents of c, which must not be
// frozen, with a provided bitmap, and computes a correct N.
func RemakeContainerBitmap(c *Container, bitmap []uint64) *Container {
	*c = Container{typeID: ContainerBitmap}
	c.setBitmap(bitmap)
	c.bitmapRepair()
	return c
}

// RemakeContainerBitmapN uses the provided n instead of counting bits. The
// provided container must not be frozen.
func RemakeContainerBitmapN(c *Container, bitmap []uint64, n int32) *Container {
	*c = Container{typeID: ContainerBitmap}
	c.setBitmap(bitmap)
	c.n = n
	return c
}

// RemakeContainerArray populates c with an array container using the provided
// array. It must not be used on a frozen container.
func RemakeContainerArray(c *Container, array []uint16) *Container {
	*c = Container{typeID: ContainerArray}
	c.setArray(array)
	return c
}

// RemakeContainerRun repopulates c with the provided intervals. c must not
// be frozen.
func RemakeContainerRun(c *Container, intervals []Interval16) *Container {
	*c = Container{typeID: ContainerRun}
	c.setRuns(intervals)
	c.n = 0
	for _, r := range intervals {
		c.n += int32(r.Last - r.Start + 1)
	}
	return c
}

// RemakeContainerRunN repopulates c with the provided intervals, but
// assumes the provided n is accurate. c must not be frozen.
func RemakeContainerRunN(c *Container, intervals []Interval16, n int32) *Container {
	*c = Container{typeID: ContainerRun}
	c.setRuns(intervals)
	c.n = n
	return c
}

// NewContainerBitmap makes a bitmap container using the provided bitmap, or
// an empty one if provided bitmap is nil. If the provided bitmap is too short,
// it will be padded. This function's API is wrong; it should have been
// written as NewContainerBitmapN, and this should not take the n argument,
// but I did it wrong initially and now that would be a breaking change.
func NewContainerBitmap(n int, bitmap []uint64) *Container {
	if bitmap == nil {
		return NewContainerBitmapN(nil, 0)
	}
	c := &Container{typeID: ContainerBitmap}
	if len(bitmap) != bitmapN {
		// adjust to required length
		c.setBitmapCopy(bitmap)
	} else {
		c.setBitmap(bitmap)
	}
	// set n based on bitmap contents.
	if n < 0 {
		c.bitmapRepair()
	} else {
		c.setN(int32(n))
		if roaringParanoia {
			c.CheckN()
		}
	}
	return c
}

// NewContainerBitmapN makes a bitmap container using the provided bitmap, or
// an empty one if provided bitmap is nil. If the provided bitmap is too short,
// it will be padded. The container's count is specified directly.
func NewContainerBitmapN(bitmap []uint64, n int32) *Container {
	if bitmap == nil {
		bitmap = make([]uint64, bitmapN)
	}
	c := &Container{typeID: ContainerBitmap, n: n}
	if len(bitmap) != bitmapN {
		// adjust to required length
		c.setBitmapCopy(bitmap)
	} else {
		c.setBitmap(bitmap)
	}
	if roaringParanoia {
		c.CheckN()
	}
	return c
}

// NewContainerArray returns an array container using the provided set of
// values. It's okay if the slice is nil; that's a length of zero.
func NewContainerArray(set []uint16) *Container {
	c := &Container{typeID: ContainerArray}
	c.setArray(set)
	return c
}

// NewContainerArrayCopy returns an array container using the provided set of
// values. It's okay if the slice is nil; that's a length of zero. It copies
// the provided slice to new storage.
func NewContainerArrayCopy(set []uint16) *Container {
	c := &Container{typeID: ContainerArray}
	c.setArrayMaybeCopy(set, true)
	return c
}

// NewContainerArrayN returns an array container using the specified
// set of values, but overriding n.
// This is deprecated. It never worked in the first place.
// The provided value of n is ignored and instead derived from the set length.
func NewContainerArrayN(set []uint16, n int32) *Container {
	return NewContainerArray(set)
}

// NewContainerRun creates a new run container using a provided (possibly nil)
// slice of intervals.
func NewContainerRun(set []Interval16) *Container {
	c := &Container{typeID: ContainerRun}
	c.setRuns(set)
	for _, run := range set {
		c.n += int32(run.Last-run.Start) + 1
	}
	return c
}

// NewContainerRunCopy creates a new run container using a provided (possibly nil)
// slice of intervals. It copies the provided slice to new storage.
func NewContainerRunCopy(set []Interval16) *Container {
	c := &Container{typeID: ContainerRun}
	c.setRunsMaybeCopy(set, true)
	for _, run := range set {
		c.n += int32(run.Last-run.Start) + 1
	}
	return c
}

// NewContainerRunN creates a new run array using a provided (possibly nil)
// slice of intervals. It overrides n using the provided value.
func NewContainerRunN(set []Interval16, n int32) *Container {
	c := &Container{typeID: ContainerRun, n: n}
	c.setRuns(set)
	if roaringParanoia {
		c.CheckN()
	}
	return c
}

// Mapped returns the internal mapped field, which indicates whether the
// slice's backing store is believed to be associated with unwriteable
// mmapped space.
func (c *Container) Mapped() bool {
	if c == nil {
		return false
	}
	return (c.flags & flagMapped) != 0
}

// frozen() returns the internal frozen state. It isn't exported because
// nothing outside this package should be thinking about this.
func (c *Container) frozen() bool {
	if c == nil {
		return true
	}
	return (c.flags & flagFrozen) != 0
}

// SafeN returns N, true if it can, otherwise it returns 0, false. For
// instance, a container subject to in-place operations can not know its
// current N, and it's not meaningful or safe to query it until a repair,
// so you can use this to get N "if it's available".
func (c *Container) SafeN() (int32, bool) {
	if c == nil {
		return 0, true
	}
	if (c.flags & flagDirty) != 0 {
		return 0, false
	}
	return c.n, true
}

// N returns the 1-count of the container.
func (c *Container) N() int32 {
	if c == nil {
		return 0
	}
	if roaringParanoia {
		if c.flags&flagDirty != 0 {
			panic("trying to call N() on a dirty container")
		}
	}
	return c.n
}

func (c *Container) setN(n int32) {
	if c == nil {
		if roaringParanoia {
			panic("trying to setN on a nil container")
		}
		return
	}
	c.n = n
}

func (c *Container) typ() byte {
	if c == nil {
		return ContainerNil
	}
	return c.typeID
}

// setTyp should only be called if you already know that c is a
// non-nil, non-frozen, container.
func (c *Container) setTyp(newType byte) {
	if roaringParanoia {
		if c == nil || c.frozen() {
			panic("setTyp on nil or frozen container")
		}
	}
	c.typeID = newType
}

func (c *Container) setMapped(mapped bool) {
	if roaringParanoia {
		if c == nil || c.frozen() {
			panic("setMapped on nil or frozen container")
		}
	}
	if mapped {
		c.flags |= flagMapped
	} else {
		c.flags &^= flagMapped
	}
}

// SetMapped marks a container as "mapped"; do this if you're setting a
// container's storage to something that it shouldn't write to, like mmapped
// memory.
func (c *Container) SetMapped(mapped bool) {
	c.setMapped(mapped)
}

// setDirty marks a container as "dirty" -- we don't trust container's n.
// this should never happen except for bitmaps.
func (c *Container) setDirty(dirty bool) {
	if roaringParanoia {
		if c == nil || c.frozen() {
			panic("setDirty on nil or frozen container")
		}
	}
	if dirty {
		c.flags |= flagDirty
	} else {
		c.flags &^= flagDirty
	}
}

// Freeze returns an unmodifiable container identical to c. This might
// be c, now marked unmodifiable, or might be a new container. If c
// is currently marked as "mapped", referring to a backing store that's
// not a conventional Go pointer, the storage may (or may not) be copied.
// Do not call Freeze on a temporarily-corrupt container, such as one
// returned from UnionInPlace but on which you haven't since called Repair.
func (c *Container) Freeze() *Container {
	if c == nil {
		return nil
	}
	if c.flags&flagDirty != 0 {
		if roaringParanoia {
			panic("freezing dirty container")
		}
		// c.Repair won't work if this is already frozen, but in
		// theory that can't happen?
		c.Repair()
	}
	// don't need to freeze
	if c.flags&flagFrozen != 0 {
		return c
	}
	c.flags |= flagFrozen
	return c
}

// Thaw returns a modifiable container identical to c. This may be c, or it
// may be a new container with distinct backing store.
func (c *Container) Thaw() *Container {
	if c == nil {
		panic("trying to thaw a nil container")
	}
	if c.flags&(flagFrozen|flagMapped) == 0 {
		return c
	}
	return c.unmapOrClone()
}

func (c *Container) unmapOrClone() *Container {
	if c.flags&flagFrozen != 0 {
		// Can't modify this container, therefore, we have to make a
		// copy.
		return c.Clone()
	}
	c.flags &^= flagMapped
	c.flags &^= flagPristine
	// mapped: we want to unmap the storage.
	switch c.typeID {
	case ContainerArray:
		c.setArrayMaybeCopy(c.array(), true)
	case ContainerRun:
		c.setRunsMaybeCopy(c.runs(), true)
	case ContainerBitmap:
		c.setBitmapCopy(c.bitmap())
	default:
		panic(fmt.Sprintf("can't thaw invalid container, type %d", c.typeID))
	}
	return c
}

// array yields the data viewed as a slice of uint16 values.
func (c *Container) array() []uint16 {
	if c == nil {
		panic("attempt to read a nil container's array")
	}
	if roaringParanoia {
		if c.typeID != ContainerArray {
			panic("attempt to read non-array's array")
		}
	}
	return (*[1 << 16]uint16)(unsafe.Pointer(c.pointer))[:c.len:c.cap]
}

// setArrayMaybeCopy stores a set of uint16s as data. c must not be frozen.
// If doCopy is set, it will ensure that the data get copied (possibly to
// its internal stash.)
func (c *Container) setArrayMaybeCopy(array []uint16, doCopy bool) {
	if roaringParanoia {
		if c == nil || c.frozen() {
			panic("setArray on nil or frozen container")
		}
		if c.typeID != ContainerArray {
			panic("attempt to write non-array's array")
		}
	}
	if len(array) > 1<<16 {
		panic("impossibly large array")
	}
	c.flags &^= flagPristine
	// array we can fit in data store:
	if len(array) <= stashedArraySize {
		copy(c.data[:stashedArraySize], array)
		c.pointer, c.len, c.cap = &c.data[0], int32(len(array)), stashedArraySize
		c.n = c.len
		c.flags &^= flagMapped // this is no longer using a hypothetical mmapped input array
		return
	}
	if &array[0] == c.pointer && !doCopy {
		// nothing to do but update length
		c.len = int32(len(array))
		c.n = c.len
		return
	}
	// copy the array
	if doCopy {
		array = append([]uint16(nil), array...)
	}
	if cap(array) > 1<<16 {
		array = array[: len(array) : 1<<16]
	}
	c.pointer, c.len, c.cap = &array[0], int32(len(array)), int32(cap(array))
	c.n = c.len
}

// setArrayMaybeCopy stores a set of uint16s as data. c must not be frozen.
func (c *Container) setArray(array []uint16) {
	c.setArrayMaybeCopy(array, false)
}

// bitmap yields the data viewed as a slice of uint64s holding bits.
func (c *Container) bitmap() []uint64 {
	if c == nil {
		panic("attempt to read nil container's bitmap")
	}
	if roaringParanoia {
		if c.typeID != ContainerBitmap {
			panic("attempt to read non-bitmap's bitmap")
		}
	}
	return (*[1024]uint64)(unsafe.Pointer(c.pointer))[:]
}

func (c *Container) bitmask() *[1024]uint64 {
	if c == nil {
		panic("attempt to read nil container's bitmap")
	}
	if roaringParanoia {
		if c.typeID != ContainerBitmap {
			panic("attempt to read non-bitmap's bitmap")
		}
	}
	return (*[1024]uint64)(unsafe.Pointer(c.pointer))
}

// AsBitmap yields a 65k-bit bitmap, storing it in the target if a target
// is provided. The target should be zeroed, or this becomes an implicit
// union.
func (c *Container) AsBitmap(target []uint64) (out []uint64) {
	if c != nil && c.typeID == ContainerBitmap {
		return c.bitmap()
	}
	// Reminder: len(nil) == 0.
	if len(target) < 1024 {
		out = make([]uint64, 1024)
	} else {
		out = target
		for i := range out {
			out[i] = 0
		}
	}
	// A nil *Container is a valid empty container.
	if c == nil {
		return out
	}
	if c.typeID == ContainerArray {
		a := c.array()
		for _, v := range a {
			out[v/64] |= 1 << (v % 64)
		}
		return out
	}
	if c.typeID == ContainerRun {
		runs := c.runs()
		b := (*[1024]uint64)(unsafe.Pointer(&out[0]))
		for _, r := range runs {
			splatRun(b, r)
		}
		return out
	}
	// in theory this shouldn't happen?
	panic("unreachable")
}

// fillerBitmap is a bitmap full of filler.
var fillerBitmap = func() (a [1024]uint64) {
	for i := range a {
		a[i] = ^uint64(0)
	}
	return a
}()

func splatRun(into *[1024]uint64, from Interval16) {
	// TODO this can be ~64x faster for long runs by setting maxBitmap instead of single bits
	// note v must be int or will overflow
	// for v := int(from.Start); v <= int(from.Last); v++ {
	// 	into[v/64] |= (uint64(1) << uint(v%64))
	// }

	// Handle the case where the start and end fall within the same word.
	if from.Start/64 == from.Last/64 {
		highMask := ^uint64(0) >> (63 - (from.Last % 64))
		lowMask := ^uint64(0) << (from.Start % 64)
		into[from.Start/64] |= highMask & lowMask
		return
	}

	// Calculate preliminary bulk fill bounds.
	fillStart, fillEnd := from.Start/64, from.Last/64

	// Handle run start.
	if from.Start%64 != 0 {
		into[from.Start/64] |= ^uint64(0) << (from.Start % 64)
		fillStart++
	}

	// Handle run end.
	if from.Last%64 != 63 {
		into[from.Last/64] |= ^uint64(0) >> (63 - (from.Last % 64))
		fillEnd--
	}

	// Bulk fill everything inbetween.
	// Sufficiently large runs will use AVX under the hood.
	copy(into[fillStart:fillEnd+1], fillerBitmap[:])
}

// setBitmapCopy stores a copy of a bitmap as data.
func (c *Container) setBitmapCopy(bitmap []uint64) {
	var bitmapCopy [bitmapN]uint64
	copy(bitmapCopy[:], bitmap)
	c.setBitmap(bitmapCopy[:])
}

// setBitmap stores a set of uint64s as data.
func (c *Container) setBitmap(bitmap []uint64) {
	if c == nil || c.frozen() {
		panic("setBitmap on nil or frozen container")
	}
	if roaringParanoia {
		if c.typeID != ContainerBitmap {
			panic("attempt to write non-bitmap's bitmap")
		}
	}
	if len(bitmap) != 1024 {
		panic("illegal bitmap length")
	}
	c.pointer, c.len, c.cap = (*uint16)(unsafe.Pointer(&bitmap[0])), bitmapN, bitmapN
	c.flags &^= flagPristine
}

// runs yields the data viewed as a slice of intervals.
func (c *Container) runs() []Interval16 {
	if c == nil {
		return nil
	}
	if roaringParanoia {
		if c.typeID != ContainerRun {
			panic("attempt to read non-run's runs")
		}
	}
	return (*[1 << 15]Interval16)(unsafe.Pointer(c.pointer))[:c.len:c.cap]
}

// setRuns stores a set of intervals as data. c must not be frozen.
func (c *Container) setRuns(runs []Interval16) {
	c.setRunsMaybeCopy(runs, false)
}

// setRunsMaybeCopy stores a set of intervals as data. c must not be frozen.
// If doCopy is set, the values will be copied to different storage.
func (c *Container) setRunsMaybeCopy(runs []Interval16, doCopy bool) {
	if roaringParanoia {
		if c == nil || c.frozen() {
			panic("setRuns on nil or frozen container")
		}
		if c.typeID != ContainerRun {
			panic("attempt to write non-run's runs")
		}
	}
	if len(runs) > 1<<15 {
		panic("impossibly large run set")
	}
	c.flags &^= flagPristine
	// array we can fit in data store:
	if len(runs) <= stashedRunSize {
		newRuns := (*[stashedRunSize]Interval16)(unsafe.Pointer(&c.data))[:len(runs)]
		copy(newRuns, runs)
		c.pointer, c.len, c.cap = &c.data[0], int32(len(newRuns)), int32(cap(newRuns))
		c.flags &^= flagMapped // this is no longer using a hypothetical mmapped input array
		return
	}
	if &runs[0].Start == c.pointer && !doCopy {
		// nothing to do but update length
		c.len = int32(len(runs))
		return
	}
	if doCopy {
		runs = append([]Interval16(nil), runs...)
	}
	if cap(runs) > 1<<15 {
		runs = runs[: len(runs) : 1<<15]
	}
	c.pointer, c.len, c.cap = &runs[0].Start, int32(len(runs)), int32(cap(runs))
}

// isArray returns true if the container is an array container.
func (c *Container) isArray() bool {
	if c == nil {
		panic("calling isArray on nil container")
	}
	return c.typeID == ContainerArray
}

// isBitmap returns true if the container is a bitmap container.
func (c *Container) isBitmap() bool {
	if c == nil {
		panic("calling isBitmap on nil container")
	}
	return c.typeID == ContainerBitmap
}

// isRun returns true if the container is a run-length-encoded container.
func (c *Container) isRun() bool {
	if c == nil {
		panic("calling isRun on nil container")
	}
	return c.typeID == ContainerRun
}
